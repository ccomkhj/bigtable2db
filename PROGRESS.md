# Progress log

Environment: 4 vCPU Intel Xeon @ 2.1 GHz, 15 GB RAM, ext4 on virtual NVMe (≈940 MB/s sequential write, ≈4,100 fsync/s), Docker 29, Linux 6.18.

| When (UTC) | What |
|---|---|
| 2026-11-21 17:50 | Environment probed. Docker works and can pull images → self-hostable engines are measured for real. No cloud accounts → Redshift / Snowflake / BigQuery are estimates. |
| 2026-11-21 18:20 | Deterministic generator (`bench/datagen.py`), protocol runner (`bench/run.py`) and reference solution `pg_truncate_copy` working end to end with checksum + query verification. |
| 2026-11-21 18:30 | Size calibration at 1M rows in PostgreSQL 17: heap 490.6 MB, freshly built index ≈150 MB each → 9.8 GB + 2×3 GB at 20M, matching the production description. Indexes maintained *during* the load come out 37 % larger (206 MB per 1M rows) because of random-order page splits. |
| 2026-11-21 18:35 | Test plan written (`PLAN.md`). Datasets 5M / 20M generating. Sub-agents launched per engine family. |
