# Test plan

## 1. The problem being benchmarked

A batch job produces **one big table** (think: a data frame) that a SaaS application reads.

| Property | Production value | This benchmark (`20M` scale, measured in PostgreSQL 17) |
|---|---|---|
| Rows | ~20 million | 20,000,000 |
| Columns | 9: integers, floats, text | 3 `int`, 3 `float8`, 3 `text` |
| Table size | ~10 GB | 9.8 GB heap |
| Index set A | ~3 GB | `UNIQUE (tenant_id, item_key, period)` ≈ 3 GB |
| Index set B | ~3 GB | `(ref_path, period)` ≈ 3 GB |
| Total | ~16 GB | ≈ 16 GB |

Today the table is replaced by a **full refresh into PostgreSQL**. That is fine while the table is small,
but the refresh has to finish in **about 5 minutes**, clients keep growing, and the load is becoming
the bottleneck. The table is read by an application, is **not** written by the application, and needs to be
**maintained a couple of times per day** (not continuously).

Questions this repository answers with measurements:

1. **Speed** – how long does each platform / method need until the *new* table is fully queryable
   (data + both access paths + statistics)? How does it scale with row count?
2. **Cost** – what does the platform cost to run for this workload?
3. **Fit** – can the same platform serve the application (low-latency lookups, concurrency, staying
   available while the refresh runs), host the *other* tables of the product, and be maintained
   incrementally a few times a day instead of being rebuilt?

## 2. Dataset

Generated deterministically by `bench/datagen.py` (every value is a pure function of the row number; no RNG state).

```
id          BIGINT   surrogate row id, unique, not indexed
tenant_id   INT      SaaS client. 400 tenants, skewed: largest ≈ 5 % of rows, median ≈ 0.15 %
period      INT      integer time bucket 1001..1020 (20 periods per item)
quantity    FLOAT8
price       FLOAT8
score       FLOAT8
item_key    TEXT     ≈ 108 chars business key, one per item
ref_path    TEXT     ≈ 113 chars path-like secondary key, one per item
description TEXT     ≈ 200 chars free text, unique per row
```

* Text is built from a 2,000-word vocabulary so it compresses like real text (not random bytes, not constants).
* Row order in the source is by item; text keys are hash-derived, so the source is in **random order with
  respect to both indexes** (the hard, realistic case for B-tree maintenance).
* Source format: **Parquet** (zstd) split into N files — the stand-in for "the data frame the upstream job hands over".
* Scales: `100k`, `1M` (development), `5M`, `20M` (measured).
* Besides the base table the generator emits the maintenance inputs and the **expected checksums after each step**:
  * `delta_1pct`, `delta_10pct` – 75 % updates of existing business keys + 25 % new keys, scattered over the whole table;
  * `replace_tenant` – the complete recomputed data of the largest tenant (≈ 5 % of the table; ~10 % of its items disappear, new ones appear).

## 3. Protocol (identical for every solution — `bench/run.py`)

| Step | What happens | What is recorded |
|---|---|---|
| setup | engine started from an empty data directory | (untimed) |
| **initial_load** | `full_refresh()` into the empty engine | wall time, phase times, CPU %, peak extra disk |
| verify | `count(*)`, `sum(quantity)`, `sum(price)`, `max(id)` vs. generator manifest | pass / fail |
| storage | engine-reported sizes + `du` of the data directory | bytes |
| **queries** | 4 logical queries, warm-up then N timed runs; answers compared with DuckDB-on-Parquet reference | p50 / p95 / p99 |
| concurrency | 8 closed-loop clients, 50/50 point + series, 15 s | QPS, p50 / p99 |
| **refresh** ×(reps−1) | `full_refresh()` **over the live table while a reader thread issues point lookups every 20 ms** | wall time + reader errors, missing rows, longest stall |
| **upsert 1 %** / **upsert 10 %** | apply delta by business key, reader running | wall time, verified by checksum |
| **replace_tenant** | replace all rows of one tenant | wall time, verified by checksum |
| queries_after | point / series / tenant_agg again | latency drift after maintenance |
| storage_after | | bloat left behind |

The clock for a refresh starts before the first Parquet byte is read and stops when queries see the new data with
**both access paths and planner statistics in place**. Conversion (Parquet → wire format) is part of the measured time.

### Logical queries

| Name | SQL (logical) | Models |
|---|---|---|
| `point` | `WHERE tenant_id=? AND item_key=? AND period=?` → 1 row, all columns | detail page / API lookup via index A |
| `series` | `WHERE ref_path=? AND period BETWEEN ? AND ?  ORDER BY period` → ≤10 rows | secondary access path via index B |
| `tenant_agg` | `WHERE tenant_id=? GROUP BY period` (count, Σ quantity·price, avg score) | per-client dashboard (0.1–5 % of table) |
| `full_agg` | `GROUP BY tenant_id` top-20 over the whole table | back-office analytics (full scan) |

### Rules that keep the comparison fair

* Same machine, same Parquet input, one engine running at a time; runs at ≥5M rows hold a machine-wide lock.
* Every engine gets a *sensible production configuration for this box* (e.g. PostgreSQL `shared_buffers=4GB`), recorded in the result file. No `fsync=off`-style settings. Strategies that trade durability (e.g. `UNLOGGED`) are separate, explicitly labelled solutions.
* Engines that can read Parquet from object storage in production (ClickHouse, DuckDB) may read the local Parquet files directly; PostgreSQL-protocol engines must be fed over a client connection, as with any managed service.
* A solution claiming an *atomic swap* must show **0 reader errors and 0 missing rows** during refresh.
* Nothing counts unless the checksums verify and all four queries return the reference answers.

## 4. Solutions

Measured on this machine:

| # | Id prefix | Platform / method | Why it is on the list |
|---|---|---|---|
| 1 | `pg_naive_insert`, `pg_truncate_copy` | **PostgreSQL baseline** – batched `INSERT` / `TRUNCATE`+`COPY` into the indexed live table | what most pipelines (pandas `to_sql`, ORMs, simple ETL) do today |
| 2 | `pg_copy_swap`, `pg_parallel_swap`, `pg_unlogged_swap` | **PostgreSQL bulk-load pattern** – load a new table, build indexes afterwards, atomic rename; then parallel streams, tuned sessions, optionally no WAL | how far the existing platform goes with *method* changes only |
| 3 | `pg_partition_*` | **PostgreSQL partitioned by tenant** – parallel per-partition build, partition exchange for maintenance | turns "full refresh" into "refresh what changed" |
| 4 | `ts_*` | **TimescaleDB** – hypertable, with and without columnstore compression | Postgres-compatible time-series/columnar extension |
| 5 | `ch_*` | **ClickHouse** – MergeTree, Parquet ingestion, `EXCHANGE TABLES` | the reference fast-ingest columnar server |
| 6 | `duckdb_*` | **DuckDB + Parquet ("lakehouse")** – don't load at all: publish sorted Parquet / a DuckDB file and swap a pointer | lower bound for refresh time; proxy for S3 + Athena/DuckDB serving |
| 7 | `mysql_*` | **MySQL 8 (InnoDB)** – `LOAD DATA`, `RENAME TABLE` | the other mainstream OLTP engine |

Estimated from vendor documentation and price lists (cannot be run from this sandbox — no cloud accounts; each is labelled **estimate** everywhere it appears):

| # | Platform | Source of numbers |
|---|---|---|
| 8 | **Amazon Redshift** (Serverless / RA3) | AWS docs + pricing |
| 9 | **Snowflake** | Snowflake docs + pricing |
| 10 | **Google BigQuery** | Google docs + pricing |

Managed flavours of the measured engines (RDS/Aurora PostgreSQL, Tiger Cloud, ClickHouse Cloud, S3 + Athena, MotherDuck) appear in the cost model only.

## 5. Execution

1. Framework, generator and one reference solution written and validated end to end (`100k`, `1M`).
2. One sub-agent per engine family implements and tunes its solutions at `1M`, records every variant it tried (including the ones that lost) in `docs/notes/`, and validates once at `5M`.
3. A sixth agent researches cloud pricing and published load performance for the estimate-only platforms.
4. All solutions are reviewed, then the final matrix runs **sequentially**: every solution at `5M` and `20M`, ≥2 full loads each (1 initial + ≥1 refresh under read load).
5. `bench/report.py` turns `results/raw/*.json` into tables and charts; `README.md` holds the findings and the decision guide.

Progress is tracked in [`PROGRESS.md`](PROGRESS.md).

## 6. Known limits of this study

* One 4-vCPU / 15 GB / NVMe machine. Absolute times will be lower on bigger hardware; the **ratios between methods** and the **shape of the scaling** are what carry over. Hardware is recorded in every result file.
* Client and server share the machine (no network hop, but they also compete for the 4 cores).
* Single node only: no replication, no HA. Where replication changes the picture (WAL volume, `UNLOGGED`), it is called out in the findings rather than measured.
* Cloud warehouses are estimates, not measurements.
* Synthetic data. Text compressibility and key lengths were chosen to hit the stated sizes; a different text mix shifts columnar compression ratios.
