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
| 2026-11-21 21:05 | **5M matrix complete: 16 / 17 pass.** `ts_hypertable` failed its third load: `DROP TABLE` of the old hypertable inside the swap transaction deadlocked with a concurrent reader (DROP locks chunks before the parent, readers lock the parent first). The same runs show plain Postgres readers stalling ~1 s during the swap at 5M; the working hypothesis at this point was that dropping the 4 GB old table inside the swap transaction holds the lock too long (**disproved later**, see 01:35). |
| 2026-11-21 21:15 | Swap changed for all Postgres-protocol solutions (`pg_common.rename_swap`): the transaction only renames, the old table is dropped after the commit, parent locked first. Verified at 100k on six solutions. 20M runs use the new swap; the affected 5M runs are queued for re-measurement and the originals kept as `*__5M__v1swap.json`. |
| 2026-11-22 00:24 | **20M matrix complete: 14 / 15 pass.** `pg_unlogged_setlogged` loaded in 3 min 09 s but its refresh ran out of disk: `SET LOGGED` rewrites the table and both indexes through the WAL and needs a third copy (peak +41 GB). Headline: `pg_parallel_swap` 2 min 21 s, `pg_truncate_copy` 6 min 27 s with readers blocked, `pg_naive_insert` 13 min 45 s; ClickHouse 86–99 s; DuckDB 51–54 s. |
| 2026-11-22 00:53 | Labs complete: Postgres lever lab at 1M (`results/labs/pg_levers_1M.json`), four upsert methods and three hash-partition layouts at 5M. |
| 2026-11-22 01:24 | Ten Postgres-protocol solutions re-measured at 5M with the rename swap: all pass, including `ts_hypertable` (no deadlock). Times within 10 % of the first runs — and the ~1 s reader pause is still there, so the drop-inside-transaction hypothesis was wrong. |
| 2026-11-22 01:35 | **Cause of the 1 s pause found and verified** with `bench/labs/swap_stall.py`: autovacuum working on the live table. The swap's lock request waits `deadlock_timeout` (1 s) before the worker is cancelled, and readers queue behind it. With a worker on the table: 1,003 ms reader pause, 1 cancelled autovacuum task in the server log. With `autovacuum_enabled = false` on the table: worst read 162 ms, no lock wait. Recorded as a recommendation; the matrix numbers were measured without the fix. |
| 2026-11-22 01:45 | Engine notes (`docs/notes/`), generated tables and charts (`results/`), and `README.md` with findings and the decision guide written from the final numbers. Development-only result files removed; `results/raw/` holds the final matrix, the first-swap 5M runs (`v1swap`) and the lab runs. |

## Known gaps

* The engine sub-agents did not get to write their own notes or record the variants they tried while tuning; the notes were written by the lead from the final measurements and the lab runs, so they describe what was chosen and what it measures, not every dead end.
* MySQL has no refresh-under-read-load number at 20M (disk).
* `pg_parallel_swap` uses CSV `COPY`; the lever lab shows binary `COPY` (ADBC) would cut the COPY phase by about a quarter. Not applied to the final runs.
* The autovacuum fix for the swap pause is verified in isolation at 5M but not applied to the matrix runs, and its effect on partitioned tables and hypertables is inferred, not measured.
* ClickHouse was queried over HTTP + JSON from Python; a native-protocol client was not compared.
* Redshift, Snowflake, BigQuery and the managed services were not run; see `docs/cloud_estimates.md` for what could and could not be verified.
