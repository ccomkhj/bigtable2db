# Progress log

Environment: 4 vCPU Intel Xeon @ 2.1 GHz, 15 GB RAM, ext4 on virtual NVMe (≈940 MB/s sequential write, ≈4,100 fsync/s), Docker 29, Linux 6.18.

| When (UTC) | What |
|---|---|
| 2026-11-21 17:50 | Environment probed. Docker works and can pull images → self-hostable engines are measured for real. No cloud accounts → Redshift / Snowflake / BigQuery are estimates. |
| 2026-11-21 18:05 | Deterministic generator (`bench/datagen.py`), protocol runner (`bench/run.py`) and reference solution `pg_truncate_copy` working end to end with checksum + query verification. |
| 2026-11-21 18:08 | Size calibration at 1M rows in PostgreSQL 17: heap 490.6 MB, freshly built index ≈150 MB each → 9.8 GB + 2×3 GB at 20M, matching the production description. Indexes maintained *during* the load come out 37 % larger (206 MB per 1M rows) because of random-order page splits. |
| 2026-11-21 18:14 | Test plan written (`PLAN.md`). Datasets 5M / 20M generated (20M: 81 s, 2.0 GB zstd Parquet). Six sub-agents launched: Postgres bulk strategies, Postgres partitioning + incremental, TimescaleDB, ClickHouse, DuckDB/Parquet + MySQL, cloud cost research. |
| 2026-11-21 18:19 | Sequential matrix runner (`bench/matrix.py`) and report generator (`bench/report.py`) written. |
| 2026-11-21 18:31 | Cloud cost research delivered (`docs/cloud_estimates.md`, 10 platforms, every figure tagged by how it was sourced). Several vendor sites were unreachable from the sandbox; AWS prices come from the AWS Price List API, BigQuery from its pricing page, Snowflake / Tiger / MotherDuck prices are search extracts or third-party and are marked as such. |
| 2026-11-21 18:40–19:40 | Sub-agents implemented 15 solutions (Postgres ×9, TimescaleDB ×2, ClickHouse ×2, DuckDB ×2), each with variant switches for single-lever experiments. **All five engine agents were then cut off by an API spend limit** before validating at 5M or writing their notes; the machine was also restarted (data on disk survived). |
| 2026-11-21 20:00 | Lead took over directly instead of re-launching agents. Docker restarted, engine data wiped, every solution re-run sequentially at 1M: **15 / 15 pass** (checksums after load, refresh, both upserts, tenant replacement; all four queries match the reference; swap solutions show 0 reader errors). |
| 2026-11-21 20:10 | Gaps the agents never reached were filled: `ts_columnstore` (TimescaleDB 2.32 columnstore; load-then-convert beat direct-to-columnstore COPY 12.6 s vs 16.7 s at 1M) and `mysql_load_swap` (MySQL 8.4; clustering on `id` beat clustering on the business key: 840 MB vs 1,220 MB at 1M). Both pass at 1M. |
| 2026-11-21 20:13 | Final chain started, strictly sequential: 17 solutions at 5M → 20M matrix → single-lever labs (Postgres loader levers, upsert methods, partition layouts). |
