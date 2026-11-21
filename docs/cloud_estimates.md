# Cloud platforms — ESTIMATES (not measured)

> **Nothing in this file was measured in this study.** Every duration, latency class and cost below is an
> **estimate** assembled on 2026-11-21 from vendor price lists, vendor documentation, vendor benchmark posts and
> a few third-party measurements. The self-hostable engines (PostgreSQL, TimescaleDB, ClickHouse, DuckDB/Parquet,
> MySQL) are measured elsewhere in this repository; this file covers only what could not be run locally.

**Workload assumed everywhere:** one table, 20 M rows × 9 columns, ~9.4 GB as CSV, ~1.7 GB as zstd Parquet,
~16 GB in PostgreSQL (10 GB heap + 2 × 3 GB B-tree). Full refresh 3×/day (≈ 91 refreshes/month), target < 5 min.
Read 24×7 at 10–50 lookups/s (= 26.3 M–131.4 M lookups/month) plus per-tenant and occasional full-table
aggregates. ~50 GB of ordinary OLTP tables exist next to it. Prices: us-east-1 / BigQuery `US` multi-region,
on-demand list, no commitments, 730 h/month, USD, taxes/support/egress excluded unless stated.

**How to read the evidence tags** (all accessed 2026-11-21):

| Tag | Meaning |
|---|---|
| **[P]** | Official page or official price-list API fetched directly today. |
| **[M]** | Official documentation read today from the vendor's public GitHub mirror (the docs site itself was not reachable); may lag the live site. |
| **[S]** | Official page could **not** be fetched from this sandbox (host blocked); figure taken from a search-engine extract of that page → *secondary retrieval*. |
| **[3]** | Third-party / secondary source. |
| **[NV]** | Not verified today; stated from general knowledge of the product. Treat as a pointer, not as evidence. |
| *inference* | My reasoning from the cited facts, not a statement by a vendor. |

Hosts that were blocked from the sandbox: `aws.amazon.com`, `docs.aws.amazon.com`, `www.snowflake.com`,
`docs.snowflake.com`, `docs.cloud.google.com`, `clickhouse.com`, `tigerdata.com`, `motherduck.com`. AWS prices come
from the official AWS Price List bulk API instead (reachable); Google prices from `cloud.google.com/bigquery/pricing`.

---

## 1. Summary

| Platform | Full-refresh path | Est. refresh time (20 M rows) | Point-lookup latency class | Hosts the ~50 GB OLTP tables? | Est. monthly cost, low – high | Confidence |
|---|---|---|---|---|---|---|
| **Amazon Redshift** (Serverless / RA3 / RG) | `COPY` Parquet from S3 → staging table → rename / `ALTER TABLE APPEND` | **unknown** (no credible published number); *inference* 1–5 min | hundreds of ms | **No** (PK/FK not enforced, columnar) | **$280 – $2,200** always-on (needed for direct serving); ≈ $25–90 if batch-only | cost high · time low |
| **Snowflake** (standard table) | `COPY INTO` from S3 stage → `ALTER TABLE … SWAP WITH` | ~1–4 min on S–M warehouse | hundreds of ms (standard); tens of ms claimed for Hybrid Tables | **Partially** (Hybrid Tables); **yes** via the separate Snowflake Postgres service | **$1,490 – $2,240+** XS warehouse always-on; ≈ $30–50 if batch-only | cost medium · time medium-low |
| **Google BigQuery** | Batch load job (free shared pool), `WRITE_TRUNCATE` | ~1–3 min + queueing, no SLO | hundreds of ms – seconds | **No** (PK/FK not enforced) | **≈ $0–25** batch-only; **$1,460 – $7,800** if the app queries it per lookup | cost high · time medium-low |
| **RDS for PostgreSQL** | client `COPY` / `aws_s3` import (CSV only) → build indexes → rename | = local PostgreSQL result, likely slower | single-digit ms | **Yes** | **$370 – $1,490** (includes the OLTP tables) | cost high · time: see measurements |
| **Aurora PostgreSQL** | same as RDS | = local PostgreSQL result, likely slower | single-digit ms | **Yes** | **$550 – $2,120** (includes the OLTP tables) | cost medium (I/O is inferred) |
| **Tiger Cloud** (Timescale) | PostgreSQL `COPY` (+ columnstore conversion) | = local TimescaleDB result | single-digit ms (rowstore) | **Yes** | **$640 – $1,880** — *prices from a third-party aggregator* | low |
| **ClickHouse Cloud** | `INSERT … SELECT FROM s3(…)` → `EXCHANGE TABLES` | ~0.5–2 min on ≥ 8 vCPU replicas; ~3–6 min on a 2 vCPU unit | tens of ms | **No** (a separate "Managed Postgres" product exists) | **$160 – $1,750** | cost medium-high · time medium |
| **S3 + Athena** (Parquet / Iceberg) | no load: write files, switch pointer / Iceberg commit | ≈ 0 + time to write Parquet | seconds | **No** | **< $10** batch-only; $1,300–6,600 if queried per lookup (don't) | cost high |
| **MotherDuck** | `CREATE OR REPLACE TABLE … AS SELECT FROM read_parquet('s3://…')` | **unknown** | tens – hundreds of ms (*inference*) | **No** | **≈ $250 – $2,000** — *sources conflict* | low |
| **DynamoDB import-from-S3** | import CSV/JSON into a **new** table, flip table name | **unknown**; AWS: "a few minutes to several hours" | single-digit ms | **No** (key-value, no FKs/joins) | **≈ $135 – $155** (lookups only, no aggregates) | cost high · time low |

Reading the cost column: wherever a platform bills for *running* compute (Redshift, Snowflake, ClickHouse Cloud,
PostgreSQL-family), a steady 10–50 lookups/s keeps it running 24×7, so "auto-suspend" saves nothing and the
always-on figure is the real one. The "batch-only" figures apply only if the application does **not** query the
platform directly (i.e. it sits beside PostgreSQL / a cache).

---

## 2. Amazon Redshift

**Prices [P, A1 — AWS Price List API, published 2026-09-11]:** Serverless **$0.375 / RPU-hour**; RA3 on-demand
`ra3.large` $0.543/h, `ra3.xlplus` $1.086/h, `ra3.4xlarge` $3.26/h; new Graviton RG nodes `rg.large` $0.3801/h,
`rg.xlarge` $0.7602/h, `rg.4xlarge` $3.04267/h; managed storage **$0.024 / GB-month**; concurrency scaling
$0.00015/node-second (`ra3.large`) to $0.0009 (`ra3.4xlarge`); Spectrum $5/TB.
**Billing rules [S, A6/A7]:** Serverless is metered per second in RPU-hours with a **60-second minimum charge**; an AWS
blog describes it as a 60-second minimum *per query* and warns that sub-second queries inflate cost. Default base
capacity 128 RPU, minimum **4 RPU** (for warehouses < 32 TB; 4 RPU option added June 2025). Provisioned clusters earn
up to 1 h/day of free concurrency-scaling credits [S, A10]. RG nodes GA 2026-05-12, `rg.large` single-node since
2026-09-03 [S, A12].

- **(a) Load path / duration.** `COPY … FORMAT PARQUET` from S3 into a staging table with `SORTKEY(tenant_id, item_key)`
  and a `DISTKEY`/`DISTSTYLE` choice; COPY sorts the data and updates statistics on load [NV]. Parquet files < 128 MB are not split, so
  parallelism = number of files [S, A13]. **Duration: unknown** — I found no credible published measurement at this scale.
  The only numbers: a 2026 blog claiming ~4 s per 1 GB Parquet in 8 files on 2 × `ra3.xlplus` (methodology not given)
  [3, A13], and an AWS re:Post article about a 36 TB / 334 B-row single COPY taking ~48 h (cluster size not in the extract)
  [S, A13]. *Inference:* 1–5 min for 1.7 GB Parquet on 8 RPU or 2 × `ra3.xlplus`; **confidence low — test before relying on it.**
- **(b) Serving.** Columnar MPP; point lookups rely on zone maps over the sort key, not an index. An AWS re:Post answer
  states Redshift is built for analytic scans, that small varied queries carry compile/dispatch overhead, and recommends
  Aurora/RDS for that pattern [S/3, A11]; a practitioner write-up targets 1–5 s (sub-second with good keys) for
  customer-facing use and notes ~50 concurrent query slots [3, A11]. **Class: hundreds of ms**, secondary-key (`ref_path`)
  lookups worse (only one sort order unless a second copy/materialized view is kept). Verdict: **put a cache or a
  serving database in front**; do not point a 10–50 QPS interactive API at it.
- **(c) Atomic refresh.** Load a staging table, then swap. `ALTER TABLE APPEND` moves blocks instead of copying but
  **cannot run inside a transaction and auto-commits** [S, A8]. Rename-swap inside one transaction and
  `DELETE`+`INSERT` in one transaction are the usual alternatives [NV]. `TRUNCATE` commits immediately [NV].
- **(d) Incremental.** `MERGE` is supported [M, A9 — archived 2023 doc mirror]. Updates are delete+insert in a columnar
  store → needs (auto-)vacuum/re-sort; a 1–10 % scattered delta is cheap in compute but degrades sort order over time (*inference*).
- **(e) Monthly cost.**
  - Serverless, direct serving (never idles because of the steady load): 4 RPU × $0.375 × 730 h = **$1,095**; 8 RPU = **$2,190**.
  - Provisioned always-on: 1 × `rg.large` = $0.3801 × 730 = **$277**; 1 × `ra3.large` = **$396**; 2 × `ra3.large` or 1 × `ra3.xlplus` = **$793**; 2 × `ra3.xlplus` = **$1,586**.
  - Batch-only (app does not query it): 91 refreshes × 5 min × 8 RPU × $0.375 = **$23**; at 32 RPU = **$91**.
  - Storage: a few GB compressed × $0.024 = **< $1**.
  - **Range: $280 – $2,200 always-on; $25 – $90 batch-only.**
- **(f) OLTP tables: no.** Primary/unique/foreign keys are "informational only; not enforced" [M, A9]; row-at-a-time writes are an anti-pattern.
- **(g) Caveats.** 60-s minimum makes Serverless economics bad for trickle queries; only one sort order per table;
  first execution of a new query shape pays compilation; RG node availability/track restrictions are new (2026) — check before sizing.

## 3. Snowflake

**Prices [S, S1 — search extract of Snowflake's pricing guide; host blocked]:** on-demand credit **$2.00 Standard / $3.00
Enterprise** (Business Critical $4.00 [3]) for AWS US East (N. Virginia); storage **$23 / TB-month** on-demand.
Gen1 warehouses: XS 1, S 2, M 4, L 8 credits/h; per-second billing with a **60-second minimum each time a warehouse
starts**; no credits while suspended [S, S3]. Gen2 warehouse rates: **not verified**.

- **(a) Load path / duration.** `COPY INTO` from an S3 external stage (Parquet), ideally ≥ 8 files of 100–250 MB.
  Evidence: Snowflake's own 10 TB test loaded snappy Parquet at **5.4 TB/h (uncompressed-equivalent) on a 2X-Large**
  (2018), and a 2025 re-run with the vectorized Parquet scanner cut the same load from 2,022 s to 626 s [S, S4]; load
  speed scales with warehouse size while credits per GB stay about constant [S, S4]. *Inference:* 5.4 TB/h on 32 credits/h
  ≈ 169 GB per credit-hour → 9.4 GB ≈ 0.06 credit-hours ≈ **1.7 min on Small, ~50 s on Medium** with the old scanner,
  faster with the vectorized one; add seconds for warehouse resume and optional `ORDER BY` for clustering.
  **Estimate 1–4 min; confidence medium-low** (vendor benchmark, different data, extrapolated 1,000× down).
  A competitor-run test reports 26 min for 1 B narrow rows of Parquet [3, S5] — consistent in order of magnitude.
- **(b) Serving.** Standard tables are micro-partitioned columnar: point lookups depend on pruning → **hundreds of ms**
  typical [NV, no vendor number found]. Two add-ons:
  - *Search Optimization Service (SOS)* — accelerates point lookups; Enterprise Edition feature [3, S8]; adds storage plus
    serverless maintenance compute that "is higher when there is high churn", estimates can be off by 50 % or more, and the
    credit rate lives in the Service Consumption Table (**rate not verified**; one third party says 2 credits/compute-hour)
    [S, S8]. A 3×/day *full* refresh is 100 % churn → worst case for SOS (*inference*).
  - *Hybrid Tables (Unistore)* — row store with enforced primary key and secondary indexes; Snowflake's guide claims
    double-digit-ms point lookups [S, S11]; quota ≈ 16,000 ops/s and **2 TB of hybrid storage per database**; **AWS
    commercial regions only** [S, S6]. Since 2026-03-01 hybrid-table *requests* are no longer billed separately; you pay
    warehouse compute plus a flat hybrid-storage rate per GB (EUR sheet of Jan 2026: €0.28/GB-month for AWS US East;
    **USD rate not verified**) [S, S6].
  Verdict: standard tables → cache/serving layer; Hybrid Tables can be queried directly but still need a running warehouse.
- **(c) Atomic refresh.** `ALTER TABLE a SWAP WITH b` "renames two tables in a single transaction" [S, S9];
  `CREATE OR REPLACE TABLE … AS` and zero-copy `CLONE` also apply [NV]. Readers see old or new, never empty.
  For Hybrid Tables: optimized bulk load exists for CTAS/COPY/INSERT, but throughput is unpublished (Snowflake cites an
  internal 50 GB test on a 2X-Large without absolute numbers) → **refresh time into a hybrid table: unknown** [S, S6].
- **(d) Incremental.** `MERGE` supported. Micro-partitions are immutable: touching one row rewrites its partition; a small
  delta scattered across many partitions rewrites most of the table [3, S10]. *Inference:* a 1–10 % random-key delta costs
  about as much compute as a full reload unless the table is clustered on the merge key.
- **(e) Monthly cost.**
  - Refresh: 91 × 5 min on Small (2 cr/h) = 15.2 credits → **$30 (Std) / $46 (Ent)**.
  - Direct serving: load never stops → XS always on = 730 credits → **$1,460 (Std) / $2,190 (Ent)**; Small = $2,920 / $4,380.
  - SOS maintenance or hybrid storage (16 GB+ × ~€0.28): **not quantified / small**; table storage ≈ 2–4 GB × $23/TB ≈ $0.10 (+ Time Travel copies of replaced data unless the table is transient [NV]).
  - **Range: ≈ $1,490 (Standard, XS always-on) – $2,240+ (Enterprise, XS always-on, before SOS); ≈ $30–50 batch-only.**
- **(f) OLTP tables: partially.** Standard tables do not enforce PRIMARY KEY/UNIQUE/FOREIGN KEY (only NOT NULL/CHECK)
  [S, S9]. Hybrid Tables require a primary key and support validated UNIQUE/FK constraints [S, S9], within the limits above.
  Separately, **Snowflake Postgres** (GA 2026-02-24, a managed PostgreSQL on a dedicated VM inside the Snowflake account)
  can host them as ordinary Postgres [S, S7]; its price list was **not verified**.
- **(g) Caveats.** The 60-s minimum plus auto-resume makes "suspend between lookups" impossible at 10 QPS; SOS and
  multi-cluster warehouses need Enterprise; hybrid tables are AWS-only and capped at 2 TB/database.

## 4. Google BigQuery

**Prices [P, G1 — cloud.google.com/bigquery/pricing, `US` multi-region unless noted]:** on-demand **$6.25 / TiB**,
first 1 TiB/month free, minimum **10 MB billed per table referenced and per query**; editions pay-as-you-go
**$0.04 (Standard) / $0.06 (Enterprise) / $0.10 (Enterprise Plus) per slot-hour**, per-second with a 1-minute minimum
(opt-in "fluid scaling" removes the minimum); storage active logical $0.000027397/GiB-h (**≈ $0.02/GiB-month**),
long-term ≈ $0.01, active physical ≈ $0.04, first 10 GiB free; **batch loading free** "using the shared slot pool";
Storage Write API **$0.025/GiB**, first 2 TiB/month free, and BI Engine **$0.0416/GiB-hour** (both read for
us-central1/us-east4; the `US` multi-region row could not be parsed separately); cross-cloud transfer from AWS
us-east-1 $0.09/GiB.

- **(a) Load path / duration.** Batch load job from Cloud Storage (Parquet), `WRITE_TRUNCATE`; free on the shared pool,
  which has no capacity guarantee (reservations are offered "for guaranteed capacity") [P, G1]. If the pipeline lives in
  AWS the 1.7 GB must cross clouds each time (1.7 GB × 91 × $0.09 ≈ $14/month on Google's side [P, G1], plus AWS egress).
  Evidence: a third-party pipeline benchmark loaded 6.44 GB of Parquet (8 files, TPC-H SF20) from GCS in **1 min 58 s**
  [3, G5]; Google's own load-performance post gives guidance (≤ 256 MB files, Parquet ≈ Avro, compressed Parquet slower)
  but only charts, measured with 1,000 reserved slots [P, G4]. **Estimate 1–3 min plus queueing; confidence medium-low.**
  The Storage Write API is an alternative for streaming/committed batches but offers nothing for a file-based full refresh.
- **(b) Serving.** Options: **clustering** on `(tenant_id, item_key)` (block pruning, not an index); **search indexes**
  (`SEARCH()` is designed for point lookups [S, G6]; `=`/`IN` on INT64/TIMESTAMP can use them too [P, G3]) — but an index on a table **< 10 GB is not populated**
  (`BASE_TABLE_TOO_SMALL`) [S, G6], and this table is ≈ 8.8 GiB logical (*inference* from column types), so it would not
  help at 20 M rows; **BI Engine** accelerates aggregates in memory. Published latencies: single-record query on a
  148 GB clustered table **0.8 s** (1.9 s unclustered) [P, G9]; a 6.5 MB query **0.5 s** with the advanced runtime's short-query
  path (> 1 s without; preview as of Aug 2025) [P, G2]; a search-index example at 725 ms [S, G6]. **Class: hundreds of ms
  to seconds.** On-demand projects share up to 2,000 slots [P, G1]. Verdict: **not a direct serving store** for an
  interactive API; export to / keep a serving database or cache.
- **(c) Atomic refresh.** Load with `WRITE_TRUNCATE` or `CREATE OR REPLACE TABLE … AS` replaces the table atomically [NV];
  there is a fixed per-table daily limit on load/replace operations (number not confirmed; 3/day is far below any plausible value) [S, G8].
- **(d) Incremental.** `MERGE` supported; on-demand bills the bytes scanned in the target (≤ ~9 GiB per run unless
  clustering prunes) → 91 runs ≈ 0.8 TiB/month ≈ inside the free tier / ≈ $5 (*inference*).
- **(e) Monthly cost.**
  - Batch-only: loads $0; storage 8.8 GiB < 10 GiB free tier → ≈ $0 (≈ $0.20 without the free tier); occasional aggregates within the free 1 TiB → **≈ $0–25** (incl. ~$14 cross-cloud transfer if fed from AWS).
  - Direct serving, on-demand: 26.3 M lookups × 10 MB minimum = 250.6 TiB → **$1,560**; at 50/s: 1,253 TiB → **$7,826** (floor; real scans may bill more).
  - Direct serving, editions: an always-busy 50-slot Standard reservation = 50 × $0.04 × 730 = **$1,460**; 100 slots = $2,920; Enterprise 50 slots = $2,190. Whether 50 slots carry 10–50 lookups/s is **unknown**.
  - **Range: ≈ $0–25 batch-only; $1,460 – $7,800 if queried per lookup.**
- **(f) OLTP tables: no.** "BigQuery doesn't enforce primary and foreign key constraints" [S, G7]; DML is batch-oriented.
- **(g) Caveats.** Per-query minimum billing makes high-QPS lookups the most expensive way to use it; shared-pool load
  latency is not guaranteed; a second cloud if everything else is on AWS.

## 5. Amazon RDS for PostgreSQL and Aurora PostgreSQL

**Prices [P, A2 — AWS Price List API, published 2026-11-20], us-east-1 on-demand:**

| Class (vCPU / GiB) | RDS PG Single-AZ | RDS PG Multi-AZ | Aurora PG Standard | Aurora PG I/O-Optimized |
|---|---|---|---|---|
| db.r6g.xlarge (4 / 32) | $0.450/h | $0.899/h | $0.519/h | $0.675/h |
| db.r7g.xlarge | $0.478/h | $0.956/h | $0.553/h | $0.719/h |
| db.r8g.xlarge | $0.478/h | $0.956/h | $0.552/h | $0.718/h |
| db.r6g.2xlarge (8 / 64) | $0.899/h | $1.798/h | $1.038/h | $1.349/h |
| db.r7g.2xlarge | $0.956/h | $1.913/h | $1.106/h | $1.437/h |
| db.r8g.2xlarge | $0.956/h | $1.912/h | $1.104/h | $1.436/h |

RDS storage: gp3 **$0.115/GB-month** (Multi-AZ $0.23), extra IOPS $0.02/IOPS-month (Multi-AZ $0.04), extra throughput
$0.08/MiBps-month (Multi-AZ $0.16), backup beyond free allocation $0.095/GB-month. Aurora: storage **$0.10/GB-month**
(Standard) or **$0.225** (I/O-Optimized); I/O **$0.20 per million requests** (Standard only); Serverless v2 $0.12/ACU-h.

- **(a) Load path / duration.** Identical SQL to the locally measured PostgreSQL solutions (COPY into a new table, build
  indexes, rename) — **use those measurements**; expect equal or slower because: data must arrive over the network via
  `COPY FROM STDIN`/`\copy` (server-side `COPY FROM 'file'` needs `pg_read_server_files`, which managed services do not
  grant) [S/3, A18]; the alternative `aws_s3.table_import_from_s3` reads S3 directly but only **text, CSV or binary COPY
  formats — not Parquet** [M, A14]; gp3 volumes below 400 GiB are capped at the 3,000 IOPS / 125 MiB/s baseline, while
  ≥ 400 GiB gives 12,000 IOPS / 500 MiB/s [S/3, A17 — AWS blog and re:Post, docs page not fetched]. *Inference:* writing
  ~16 GB of table+indexes plus a similar WAL volume in 5 min needs > 100 MiB/s sustained, so allocate ≥ 400 GiB gp3 or buy throughput.
- **(b) Serving.** Same engine as measured: B-tree lookups, **single-digit ms**, direct application access (with a pooler / RDS Proxy at $0.015 per vCPU-hour [P, A2]).
- **(c) Atomic refresh.** Same as self-hosted: transactional rename swap, or partition attach/detach.
- **(d) Incremental.** `INSERT … ON CONFLICT` / `MERGE` (PG ≥ 15) as measured locally; on Aurora Standard every changed page is billed I/O.
- **Not allowed / different.** No true superuser (`rds_superuser`) [S, A18]; `wal_level` is not set directly — the static
  parameter `rds.logical_replication` controls it and needs a reboot [S, A18], so `wal_level=minimal` tricks are out;
  Multi-AZ replicates synchronously to the standby and read replicas must replay the full WAL of every refresh [NV];
  on Aurora, **UNLOGGED tables exist only on the writer and are not replicated to readers** [S, A16]; Aurora bills write
  I/O per 4 KB of log (batched) [S, A15] and AWS positions I/O-Optimized for clusters whose I/O exceeds 25 % of spend [S, A15].
- **(e) Monthly cost** (the same instance also hosts the 50 GB of OLTP tables; ~100 GB used: 16 GB live + 16 GB shadow copy during swap + 50 GB + headroom).
  - RDS low: Single-AZ `db.r8g.xlarge` $0.478 × 730 = $349 + 200 GB gp3 $23 = **$372**.
  - RDS high: Multi-AZ `db.r8g.2xlarge` $1.912 × 730 = $1,396 + 400 GB gp3 Multi-AZ $92 = **$1,488**.
  - Aurora low: 1 × `db.r8g.xlarge` Standard $403 + 100 GB × $0.10 = $10 + I/O. *Inference (low confidence):* each refresh
    writes ≥ 16 GB ÷ 4 KB ≈ 4 M write I/Os (up to ~12 M) and re-reads up to ~2 M pages into a cold cache →
    91 × (4–12 M + 2 M) × $0.20/M ≈ **$110–$255** → **≈ $525–$670**. I/O-Optimized instead: $0.718 × 730 = $524 + 100 GB × $0.225 = $22.5 → **$547**, no I/O line — the safer choice for this pattern.
  - Aurora high: writer + reader, 2 × `db.r8g.2xlarge` I/O-Optimized = 2 × $1.436 × 730 = $2,097 + $22.5 = **$2,119**.
  - If an instance already exists for the OLTP tables, the **incremental** cost of the big table is ≈ 32 GB of storage (≈ $4–7) plus any instance up-size.
- **(f) OLTP tables: yes** — it is PostgreSQL.
- **(g) Caveats.** Refresh WAL volume hits replicas, backups/PITR and (Aurora Standard) the I/O bill; EBS throughput, not CPU, may be the limiter.

## 6. Tiger Cloud (Timescale)

- **Pricing model:** HA and read replicas are charged at the same rate as the primary; storage price is the same in
  all regions, compute varies by region; plans Performance / Scale / Enterprise [M, T1 — official docs via GitHub
  mirror]. Compute is billed hourly per service size (a started hour is billed in full) and storage is metered on the
  average GB actually stored, i.e. after compression [S, T1 — search extract of the same docs].
- **Rates [3, T2 — usagepricing.com rate-card copy; official page not reachable → treat as unverified]:** us-east-1
  Performance plan **4 CPU / 16 GB $0.8067/h**, **8 CPU / 32 GB $1.210/h** (Scale plan $0.968 / $1.452); storage
  **$0.00121/GB-hour ≈ $0.88/GB-month** of stored bytes (the advertised "$0.177/GB-month" assumes ~5× compression).
- **(a)–(d)** PostgreSQL semantics: load, swap and upsert behave like the locally measured `ts_*` solutions. Columnstore
  ("hypercore") compression is applied by a policy/job *after* the load, so "queryable with final layout" takes longer
  than the COPY itself; compressed chunks favour scans over arbitrary point lookups (see local measurements).
- **(e) Cost (low confidence):** low = 4 CPU/16 GB $0.8067 × 730 = $589 + 66 GB × $0.88 = $58 → **≈ $640**;
  high = 8 CPU/32 GB with one HA replica = 2 × ($883 + $58) → **≈ $1,880**.
- **(f) OLTP tables: yes** (PostgreSQL). **(g)** Storage per GB is ~8× gp3, so compression matters; verify the rate card before budgeting.

## 7. ClickHouse Cloud

**Prices [M, C1 — official billing doc (GitHub mirror), AWS us-east-1 examples; consistent with a search extract of the
pricing page, C2]:** one compute unit = 8 GiB RAM + 2 vCPU, metered per minute. Derived from the documented examples:
Basic **$0.2181/unit-hour** ($159.66 for one unit 24×7), Scale **$0.2985** ($436.95 for 2 replicas × 1 unit),
Enterprise **$0.3903**; storage **$25.30/TB-month** of compressed data, backups billed at the same rate; egress billed separately.

- **(a) Load path / duration.** `INSERT INTO new SELECT * FROM s3('…/*.parquet')` (or `s3Cluster`). ClickHouse's own S3
  performance guide measured, on ClickHouse Cloud nodes with 8 vCPU / 32 GiB, **59.82 M rows / 24 GB into MergeTree in
  192 s with defaults, 129 s tuned on one node, and 50–55 s across three nodes**; another example averages ~300,000
  rows/s [M, C3/C4]; a node with twice the resources ran "approximately twice as fast" [M, C3]. Row width there (~400 B) is close to this table (~470 B). *Inference:* 20 M rows ≈ **45–65 s on one
  8 vCPU replica**, ~20 s on three; S3 throughput improves "linearly with the number of cores" [M, C3], so a single 2 vCPU unit is ~3–6 min
  (low confidence). **Estimate 0.5–2 min on ≥ 8 vCPU; confidence medium** (vendor measurement, similar shape).
- **(b) Serving.** Sparse primary index on the sort key → **tens of ms** for `tenant_id + item_key` lookups; the second
  access path (`ref_path`) needs a projection or a second table; aggregates are its strength (see local `ch_*` results).
  Idling pauses compute after inactivity (minimum timeout 5 min) but ClickHouse says it "isn't recommended for services
  that power customer-facing features that are used frequently" [S, C5] → run always-on; direct app access is reasonable.
- **(c) Atomic refresh.** `EXCHANGE TABLES` (as measured locally) or `REPLACE PARTITION` [NV for Cloud's SharedMergeTree].
- **(d) Incremental.** No SQL `MERGE`; use ReplacingMergeTree, lightweight updates/deletes or partition replacement [NV]; see local measurements.
- **(e) Monthly cost.** Storage is negligible (≈ 3–5 GB compressed + backup ≈ $0.25).
  - Low: Basic, 1 replica × 1 unit, always-on = **$160** (single replica, no HA; refresh on 2 vCPU may exceed 5 min).
  - Mid: Scale, 2 replicas × 2 units (16 GiB / 4 vCPU each) = 4 × $0.2985 × 732 = **$874**.
  - High: Scale, 2 replicas × 4 units (32 GiB / 8 vCPU each) = **$1,748**.
- **(f) OLTP tables: no** for ClickHouse itself (no enforced FKs, no row-level transactional writes [NV]). ClickHouse now
  sells a separate **Managed Postgres** (GA on AWS per its 2026 announcement; pricing page still showed beta pricing) [S, C6] — that is PostgreSQL beside ClickHouse, not one engine.
- **(g) Caveats.** Unit prices above are derived from doc examples, not read off the live pricing page; Basic tier is capped (1 TB, single replica) [3, C2].

## 8. Amazon S3 + Athena, and MotherDuck

**Athena prices [P, A3/A5]:** **$5.00 per TB scanned**; capacity reservations **$0.30 per DPU-hour**; S3 Standard
$0.023/GB-month, GET $0.0004 per 1,000. Scans are rounded up with a **10 MB minimum per query** [S, A19]. Since Feb 2026
reservations start at **4 DPUs with 1-minute granularity** (previously 24 DPUs / 1 h) [S, A19].

- **(a) Refresh.** Nothing to load: the batch job writes sorted Parquet to a new prefix and the table pointer is switched
  (Glue location update or an Iceberg snapshot commit). Time ≈ time to write 1.7 GB of Parquet — see the local `duckdb_*` results.
- **(b) Serving.** Each query pays queueing + planning + S3 opens; AWS's own tuning example shows 0.7 s for a
  partition-pruned count and 4.8 s unpruned on a 74 GB table [S, A19]. **Class: seconds.** Not for per-request lookups:
  at the 10 MB minimum, 26.3 M lookups = 263 TB → **$1,314/month** (50/s → $6,570), before account-level concurrency quotas.
  Use it for the tenant/full-table aggregates and ad-hoc analysis only.
- **(c)** Atomic by construction (Iceberg commit / pointer swap). **(d)** `MERGE INTO` is supported for Iceberg tables
  (engine v3), merge-on-read only → periodic compaction needed [S, A19].
- **(e) Cost.** Storage: 3 retained versions × 1.7 GB × $0.023 = $0.12. Queries, batch-only example: 1,000 tenant
  aggregates/day × ~20 MB = 0.6 TB/month → $3. **< $10/month.** A permanently held 4-DPU reservation would be 4 × $0.30 × 730 = $876.
- **(f) OLTP tables: no.** **(g)** Latency variance and per-query minimum billing.

**MotherDuck (hosted DuckDB) — low confidence, sources conflict.** Search extracts of MotherDuck's fees addendum and
pricing page give compute instances ("ducklings") at **Pulse $0.60/h (metered per query in CU-seconds), Standard
$2.40/h, Jumbo $4.80/h, Mega $12/h, Giga $36/h** (one tracker says $24), **storage $0.04/GB-month**, Business plan
**$250/month** [S, M1]; MotherDuck's own docs pages in the same results still show older values ($0.08/GB-month,
≈ $0.25/CU-hour, $100 platform fee) [S, M1]. Standard and larger instances bill wall-clock time including a cooldown
after the last query [S, M1]. (a) `CREATE OR REPLACE TABLE … AS SELECT * FROM read_parquet('s3://…')` — **duration
unknown** (no published benchmark found; local DuckDB numbers are the proxy). (b) *Inference:* tens to hundreds of ms.
(e) Always-on Standard: $2.40 × 730 = $1,752 + $250 = **≈ $2,000**; per-query Pulse use cannot be estimated without
knowing CU consumption per lookup → lower bound ≈ the $250 plan fee. (f) **No** (analytical engine).

## 9. Amazon DynamoDB import-from-S3 (contrast only)

**Prices [P, A4 — AWS Price List API, published 2026-11-11]:** import from S3 **$0.15/GB**; the new Bulk Put Items
(load into an *existing* table, GA 2026-11-05) **$0.10/GB standard, $0.20/GB accelerated**; on-demand reads **$0.125
per million read request units**, writes $0.625 per million; storage $0.25/GB-month after 25 GB free.
**Behaviour [S, A20]:** import **always creates a new table** (secondary indexes are declared as part of the import [NV]), accepts **CSV, DynamoDB
JSON or Ion** (gzip/zstd) — **not Parquet** — and consumes no write capacity; AWS says an import takes "a few minutes
to several hours" depending on size and key distribution. For Bulk Put, AWS's illustrative figure is 10 M 1 KB items in
< 10 min and 100 M in < 1 h.

- **What it would cost.** Import: 9.4 GB CSV × $0.15 = $1.41 per refresh × 91 = **$129/month** [billing on uncompressed
  source size: NV]. Reads: 26.3 M–131.4 M strongly consistent 1-RRU gets = **$3.30–$16.40**. Storage: base table + one
  full-projection GSI ≈ 25–30 GB → ≈ $1–8. **≈ $135–$155/month.** A 1–10 % delta via Bulk Put ≈ $0.01–0.10 per run.
- **Why / when it fits.** Predictable **single-digit-ms** key lookups [NV — AWS's standard positioning], no capacity to
  manage, cheapest serving bill here. It fits when the read pattern is *only* keyed lookups and short key-range queries.
- **Why it does not fit this workload as stated.** No per-tenant or full-table aggregates (a second engine is still
  needed); the upstream job must emit CSV/JSON instead of Parquet; every full refresh is a new table the application must
  switch to; **import duration is not guaranteed and is unlikely to meet 5 minutes** (*inference*); no FKs/joins → cannot host the OLTP tables.

---

## 10. What changes at 100 M rows (5×: ~47 GB CSV, ~8.5 GB Parquet, ~80 GB in PostgreSQL)

*All inference.* Cost for the always-on platforms barely moves, because the bill is dominated by compute that must run
24×7 for the lookups, not by 80 GB of data: storage adds single-digit dollars on the warehouses and ≈ $10–75 on the
PostgreSQL family (two 80 GB copies during a swap; ≈ $140 on Tiger at ~$0.88/GB). What changes is the **refresh**:
the scale-out engines keep the 5-minute target by adding parallelism for the duration of the load at roughly constant
cost per GB (Snowflake: a Medium–Large warehouse for a few minutes; Redshift Serverless: more RPUs; BigQuery: still a
free load job, and the table now exceeds the 10 GB threshold so search indexes become usable; ClickHouse Cloud: the
documented 60 M rows in 129–192 s on one 8 vCPU node implies 3.5–5.5 min for 100 M, so three replicas or 16+ vCPU are
needed). The PostgreSQL family does not have that lever: load and index build are bounded by one writer, EBS
throughput and WAL, so a 5× table is ≥ 5× the measured time and a full refresh inside 5 minutes on an
r-class xlarge/2xlarge is not plausible — at that size the managed-Postgres options depend on **incremental
maintenance or per-tenant partition exchange** rather than full reloads, and on Aurora Standard the refresh I/O bill
grows to roughly $550–1,300/month (I/O-Optimized avoids it). DynamoDB import cost scales linearly to ≈ $640/month and
its duration grows; Athena/Parquet is unaffected apart from larger scans.

---

## 11. Sources (all accessed 2026-11-21)

**AWS**
- **A1** [P] AWS Price List bulk API, Amazon Redshift, us-east-1 (published 2026-09-11): https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRedshift/current/us-east-1/index.json
- **A2** [P] same API, Amazon RDS incl. Aurora (published 2026-11-20): https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-1/index.json
- **A3** [P] same API, Amazon Athena (published 2026-11-20): https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonAthena/current/us-east-1/index.json
- **A4** [P] same API, Amazon DynamoDB (published 2026-11-11): https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/us-east-1/index.json
- **A5** [P] same API, Amazon S3 (published 2026-11-21): https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json
- **A6** [S] Redshift Serverless billing and capacity: https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-billing.html · https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-capacity.html
- **A7** [S] AWS Big Data Blog, Redshift Serverless at 4 RPUs: https://aws.amazon.com/blogs/big-data/amazon-redshift-serverless-at-4-rpus-high-value-analytics-at-low-cost · https://aws.amazon.com/about-aws/whats-new/2025/06/amazon-redshift-serverless-4-rpu-capacity-option
- **A8** [S] Redshift `ALTER TABLE APPEND`: https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE_APPEND.html
- **A9** [M] Redshift constraints and MERGE (archived 2023 doc mirror): https://github.com/awsdocs/amazon-redshift-developer-guide/blob/master/doc_source/t_Defining_constraints.md · https://github.com/awsdocs/amazon-redshift-developer-guide/blob/master/doc_source/r_MERGE.md
- **A10** [S] Redshift concurrency scaling: https://aws.amazon.com/redshift/features/concurrency-scaling/
- **A11** [S/3] Short-query behaviour: https://www.repost.aws/questions/QU2XyXxf6kRkumDVseUj0Mnw/redshift-bad-short-query-performance · https://manhtai.github.io/posts/redshift-as-a-database-for-customer-facing-app · https://hackernoon.com/have-your-postgres-cake-with-amazon-redshift-and-eat-it-too-283b431ba5fa
- **A12** [S] Redshift RG instances: https://aws.amazon.com/about-aws/whats-new/2026/05/amazon-redshift-rg-instances-powered-by-graviton · https://aws.amazon.com/about-aws/whats-new/2026/09/redshift-rg-large-single-node
- **A13** [S/3] Redshift COPY: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html · https://www.repost.aws/articles/ARG4NMtpyiTuG_pBICYZe8lg/improve-copy-ingestion-performance-for-large-data-loads-on-amazon-redshift · https://oneuptime.com/blog/post/2026-02-12-load-data-redshift-s3/view
- **A14** [M] RDS `aws_s3` import formats (archived mirror; live page https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.FileFormats.html): https://github.com/awsdocs/amazon-rds-user-guide/blob/main/doc_source/USER_PostgreSQL.S3Import.md
- **A15** [S] Aurora FAQ (I/O accounting, I/O-Optimized): https://aws.amazon.com/rds/aurora/faqs/ · https://aws.amazon.com/blogs/database/planning-i-o-in-amazon-aurora
- **A16** [S] Aurora PostgreSQL unlogged tables: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-postgresql-unlogged-tables.html
- **A17** [S/3] RDS gp3 baselines: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Storage.html · https://repost.aws/questions/QUVfUKOwslRNWpHJYD44UDdA/confusing-info-on-rds-gp3-pricing · https://aws.amazon.com/blogs/database/best-storage-practices-for-running-production-workloads-on-hosted-databases-with-amazon-rds-or-amazon-ec2
- **A18** [S/3] RDS `\copy`, `rds_superuser`, logical replication: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Procedural.Importing.Copy.html · https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Roles.rds_superuser.html · https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_MultiAZDBCluster_LogicalRepl.html
- **A19** [S] Athena: https://aws.amazon.com/athena/pricing/ · https://aws.amazon.com/about-aws/whats-new/2026/02/amazon-athena-one-minute-capacity-reservations · https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-merge-into.html · https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena/
- **A20** [S] DynamoDB import and Bulk Put Items: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html · https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.Format.html · https://aws.amazon.com/blogs/database/introducing-bulk-put-items-for-amazon-dynamodb-load-data-from-amazon-s3-into-existing-tables-at-scale/

**Snowflake** (all [S] unless marked — both Snowflake hosts were blocked)
- **S1** Pricing guide: https://www.snowflake.com/pricing/pricing-guide/ · third-party confirmation [3]: https://www.eon.io/blog/snowflake-pricing
- **S2** Service Consumption Table (contents not read): https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf
- **S3** Warehouses and compute cost: https://docs.snowflake.com/en/user-guide/warehouses-overview · https://docs.snowflake.com/en/user-guide/cost-understanding-compute
- **S4** Load benchmarks: https://www.snowflake.com/en/blog/how-to-load-terabytes-into-snowflake-speeds-feeds-and-techniques/ (2018) · https://www.snowflake.com/en/blog/engineering/loading-terabytes-into-snowflake-speeds-feeds-techniques/ (2025)
- **S5** [3] Databend 1 B-row comparison: https://www.databend.com/blog/2024-01-05-1brows/
- **S6** Hybrid Tables: https://docs.snowflake.com/release-notes/2026/other/2026-03-02-hybrid-tables-pricing · https://docs.snowflake.com/en/user-guide/tables-hybrid-cost · https://docs.snowflake.com/en/user-guide/tables-hybrid-limitations · https://docs.snowflake.com/en/user-guide/tables-hybrid-best-practices · https://www.snowflake.com/en/blog/engineering/understanding-hybrid-table-bulk-insert/ · https://www.snowflake.com/legal-files/CreditConsumptionTableEUR.pdf
- **S7** Snowflake Postgres GA: https://docs.snowflake.com/release-notes/2026/other/2026-02-24-snowflake-postgres-ga
- **S8** Search optimization: https://docs.snowflake.com/en/user-guide/search-optimization/cost-estimation · [3] https://www.flexera.com/blog/finops/snowflake-search-optimization-part1/ · [3] https://www.eon.io/blog/snowflake-pricing
- **S9** `ALTER TABLE … SWAP WITH`, constraints: https://docs.snowflake.com/en/sql-reference/sql/alter-table · https://docs.snowflake.com/sql-reference/constraints-overview
- **S10** [3] MERGE and micro-partition rewrites: https://select.dev/posts/snowflake-merges
- **S11** Hybrid Tables as serving layer: https://www.snowflake.com/en/developers/guides/hybrid-tables-serving-layer/

**Google**
- **G1** [P] BigQuery pricing: https://cloud.google.com/bigquery/pricing
- **G2** [P] Short-query optimizations (2025-08-06): https://cloud.google.com/blog/products/data-analytics/short-query-optimizations-in-bigquery-advanced-runtime
- **G3** [P] Numeric search indexes (2024-05-23): https://cloud.google.com/blog/products/data-analytics/announcing-bigquery-numeric-search-indexes
- **G4** [P] Load performance considerations (2022-12-07): https://cloud.google.com/blog/products/data-analytics/performance-considerations-for-loading-data-into-bigquery
- **G5** [3] dltHub benchmark: https://dlthub.com/blog/benchmark-dlthub
- **G6** [S] Search indexes: https://docs.cloud.google.com/bigquery/docs/search-index · https://docs.cloud.google.com/bigquery/docs/search-intro · https://cloud.google.com/blog/products/data-analytics/pinpoint-unique-elements-with-bigquery-search-features
- **G7** [S] Primary/foreign keys: https://docs.cloud.google.com/bigquery/docs/primary-foreign-keys
- **G8** [S] Quotas and load-job optimization: https://docs.cloud.google.com/bigquery/quotas · https://docs.cloud.google.com/bigquery/docs/optimize-load-jobs
- **G9** [P] Clustered single-record lookup, 148 GB table (2024-01-19): https://cloud.google.com/blog/products/data-analytics/using-the-healpix-index-with-bigquery-for-astronomy-datasets

**ClickHouse, Tiger Data, MotherDuck**
- **C1** [M] ClickHouse Cloud billing overview (live: https://clickhouse.com/docs/cloud/manage/billing/overview): https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/cloud/reference/03_billing/01_billing_overview.md
- **C2** [S/3] https://clickhouse.com/pricing · https://www.usagepricing.com/blueprint/clickhouse-cloud
- **C3** [M] S3 insert/read performance guide: https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/s3/performance.md
- **C4** [M] Amazon reviews example (≈ 300 k rows/s): https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/getting-started/example-datasets/amazon-reviews.md
- **C5** [S] Idling: https://clickhouse.com/docs/cloud/features/autoscaling/idling
- **C6** [S] Managed Postgres: https://clickhouse.com/blog/managed-postgres-ga-xata-acquisition · https://clickhouse.com/docs/cloud/managed-postgres/pricing
- **T1** [M] Tiger Cloud pricing and account management (live: https://www.tigerdata.com/docs/about/latest/pricing-and-account-management): https://github.com/timescale/docs/blob/latest/about/pricing-and-account-management.md
- **T2** [3] Tiger Cloud rate-card copy: https://www.usagepricing.com/blueprint/timescale · official, not reachable: https://www.tigerdata.com/pricing
- **M1** [S] MotherDuck: https://motherduck.com/fees-addendum/ · https://motherduck.com/product/pricing/ · https://motherduck.com/docs/about-motherduck/billing/pricing/ · [3] https://pricingsaas.com/companies/motherduck
