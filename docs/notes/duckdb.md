# DuckDB / Parquet "lakehouse" — engineering notes

DuckDB 2.0.1 embedded in the benchmark process; no server, no container.
Code: [`bench/solutions/duckdb_lake.py`](../../bench/solutions/duckdb_lake.py).
Numbers are from the final runs (`results/raw/duckdb_*__{5M,20M}__final.json`).

## The idea

Do not load into a database server. A refresh **publishes a new immutable snapshot** next to the old one and
switches a pointer file with an atomic rename; application processes embed DuckDB, poll the pointer and move to
the new snapshot between two queries. Files are fsync'ed before the pointer moves.

| | `duckdb_parquet` | `duckdb_native` |
|---|---|---|
| Snapshot | sorted zstd Parquet files + a JSON manifest holding each file's key range | one DuckDB database file |
| Production analogue | S3 + DuckDB in the app tier, or S3 + Athena / Trino | a file on shared / local disk, or MotherDuck |

There is **no B-tree anywhere**. Both access paths are served by sort order plus min/max pruning:

* **Index A** `(tenant_id, item_key, period)` — the table itself is sorted by it.
* **Index B** `(ref_path, period)` — a second, narrow, sorted copy holding
  `(ref_path, period, tenant_id, item_key, quantity, price, score)`: the columnar equivalent of a covering index.

Sorting is done in passes that fit in RAM (6M full-width rows, 10M narrow rows per pass) instead of one external
sort. The loader uses 4 threads and an 8 GB memory limit; the serving instance 4 threads and 2 GB.
ART indexes / primary keys on the native file are not used (zone maps on sorted data only).

## Measured, 20M rows

| | `duckdb_parquet` | `duckdb_native` |
|---|---|---|
| Full refresh over the live snapshot | **54 s** (sort+write table 38 s, projection 14 s) | **51 s** |
| Reader errors during refresh | 0 | 0 |
| Size | **2.0 GB** (table 1.6 + projection 0.4) | 2.8 GB |
| `point` p50 / p95 | 3.5 / 5.6 ms | 2.5 / 4.1 ms |
| `series` p50 / p95 | 2.7 / 3.3 ms | 2.1 / 3.7 ms |
| `tenant_agg` p50 | 4.8 ms | 2.5 ms |
| `full_agg` p50 | 499 ms | **42 ms** |
| QPS, 8 threads in one process | 936 | 1,396 |
| Upsert 1 % / 10 % | 44 s / 49 s (rewrite the affected files — at a scattered 1 % that is all of them) | 8.1 s / 13 s (in-place SQL on a copy of the file) |
| Replace largest tenant | 18 s | 8.6 s |

## What to know before choosing it

* **Fastest refresh and smallest footprint in the study, at the price of the serving model.** There is no
  database to connect to: every application process that reads the table links DuckDB and needs the files
  (local disk, a shared volume, or object storage). Anything that speaks only the PostgreSQL wire protocol —
  BI tools, other services, ad-hoc psql — cannot read it without something in between.
* **Lookups cost milliseconds, not microseconds** (2–4 ms vs 0.2 ms in PostgreSQL), and throughput is bounded
  by the cores of the *application* host; here 900–1,400 QPS from 8 Python threads in one process.
  With files on S3 instead of local NVMe, add network round trips per lookup — measure before relying on it.
* **Single writer.** One process publishes snapshots. That fits "a batch job refreshes the table a few times
  a day" exactly, and nothing else.
* **Scattered small changes are expensive on immutable files.** A 1 % upsert that touches every file rewrites
  every file (44 s — almost a full refresh). Tenant-aligned changes are cheap. The native file handles
  scattered upserts better (8 s) but must be copied before it is modified.
* **No transactions with the rest of the product's data, no constraints.** The other tables stay in PostgreSQL;
  joins across the two happen in the application or through DuckDB's postgres scanner.
* **Athena instead of embedded DuckDB** keeps the refresh path (it is just files) but turns every lookup into
  a seconds-class, per-query-billed request — see [`../cloud_estimates.md`](../cloud_estimates.md).
