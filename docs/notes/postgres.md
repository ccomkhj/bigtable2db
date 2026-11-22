# PostgreSQL bulk-load strategies — engineering notes

PostgreSQL 17.12 (`postgres:17`), `shared_buffers=4GB`, `max_wal_size=8GB`, `wal_compression=lz4`,
`maintenance_work_mem=1GB`, database collation `en_US.utf8`. Full server configuration is in every result file.
Code: [`bench/solutions/pg_baseline.py`](../../bench/solutions/pg_baseline.py),
[`bench/solutions/pg_strategies.py`](../../bench/solutions/pg_strategies.py).

## The ladder, 20M rows (10 GB heap + 2 × 3 GB indexes)

| Solution | What changes versus the row above | Initial load | Refresh over live table | Readers during refresh |
|---|---|--:|--:|---|
| `pg_naive_insert` | multi-row `INSERT`, 5,000 rows per statement, into the indexed live table | 13 min 45 s | – | blocked |
| `pg_truncate_copy` | `COPY` instead of `INSERT` | 6 min 07 s | 6 min 27 s | **blocked 6 min 19 s** |
| `pg_copy_swap` | load a *new* table, build indexes *after*, atomic rename | 3 min 18 s | 3 min 34 s | online |
| `pg_parallel_swap` | 4 COPY streams, both indexes at once, `COLLATE "C"` keys | 2 min 01 s | **2 min 21 s** | online |
| `pg_walminimal_swap` | `wal_level=minimal`: no WAL for the new table (self-hosted, no replicas) | 102 s | 106 s | online |
| `pg_unlogged_swap` | `UNLOGGED` table: no WAL at all, **not crash-safe, not replicated** | 63 s | 90 s | online |

Every step is a change of *method*; the server, the hardware and the table are the same.
`pg_parallel_swap` runs as an ordinary non-superuser role over client connections and is fully WAL-logged,
so it works unchanged on RDS, Aurora, Cloud SQL or any other managed PostgreSQL.

### Where the time goes at 20M

| | COPY | Indexes | Other |
|---|--:|--:|--:|
| `pg_truncate_copy` | 378 s (rows and both B-trees together) | – | 9 s |
| `pg_copy_swap` | 110 s | 96 s (one after the other) | 8 s |
| `pg_parallel_swap` | 64 s | 69 s (both at once) | 8 s |
| `pg_unlogged_swap` | 36 s | 48 s | 7 s |

Maintaining two B-trees on 110-character random keys while loading is what makes the baseline slow: every row
causes two random leaf-page writes, and the indexes end up 40 % larger (4.03 GB instead of 2.87 GB each)
because the pages split instead of being packed. A sorted bulk build afterwards is both faster and smaller.

## What each lever is worth

Measured in isolation at 1M rows on the otherwise idle machine, best of 3
([`results/labs/pg_levers_1M.json`](../../results/labs/pg_levers_1M.json)). At 1M everything fits in RAM, so
these are lower bounds for the effect of anything that reduces random I/O.

**Whole pipeline (COPY + 2 indexes + ANALYZE + swap)**

| Variant | Seconds | vs. `pg_copy_swap` |
|---|--:|--:|
| `pg_copy_swap` (1 stream, serial index builds, default collation) | 7.80 | – |
| + `COLLATE "C"` on the two text key columns only | 6.64 | −15 % |
| + 4 COPY streams only | 5.03 | −36 % |
| + build both indexes concurrently only | 6.70 | −14 % |
| `pg_parallel_swap` (all three) | 3.63 | **−53 %** |
| `pg_parallel_swap` with the default collation | 4.23 | −46 % |
| `pg_unlogged_swap` | 2.67 | −66 % |
| `pg_unlogged_setlogged` (`UNLOGGED`, then `ALTER TABLE … SET LOGGED`) | 6.80 | −13 % |

**COPY phase only**

| Variant | Seconds |
|---|--:|
| 1 / 2 / 3 / 4 / 6 / 8 CSV streams | 4.91 / 2.88 / 2.37 / 2.19 / 2.51 / 2.38 |
| 1 stream: pyarrow CSV / psycopg binary rows / ADBC binary | 4.68 / 3.28 / 2.71 |
| 4 streams: pyarrow CSV / psycopg binary rows / ADBC binary | 2.27 / 1.98 / **1.65** |
| 4 streams, `synchronous_commit` on / off | 2.35 / 2.29 |
| 4 streams into an `UNLOGGED` table | 1.55 |

**Index builds only** (both indexes)

| Variant | Default collation | `COLLATE "C"` |
|---|--:|--:|
| serial, 0 parallel workers | 3.25 | 2.18 |
| serial, 2 workers each (server default) | 2.25 | 1.46 |
| both at once, 1 worker each | 1.50 | **0.99** |
| both at once, 2 workers each | 1.61 | 0.91 |
| `maintenance_work_mem` 32 MB / 64 MB / 256 MB / 1 GB / 2 GB (both at once, "C") | | 1.33 / 1.06 / 1.01 / 1.02 / 0.87 |

**"What pipelines do today"** — loading 1M rows into the indexed table

| Method | Seconds | Projected to 20M (linear) |
|---|--:|--:|
| `pandas.to_sql(method="multi", chunksize=5000)` | **142** | ≈ 47 min |
| psycopg multi-row `INSERT`, 5,000 rows/statement (`pg_naive_insert`) | 33 | ≈ 11 min (measured: 13 min 45 s) |
| `pandas.to_sql(chunksize=5000)` (SQLAlchemy insertmanyvalues) | 23 | ≈ 8 min |
| psycopg `executemany`, pipeline mode | 19 | ≈ 6 min |
| single `COPY` (`pg_truncate_copy`) | 12 | ≈ 4 min (measured: 6 min 07 s) |

Reading the tables:

* **Parallel COPY is the biggest single lever and saturates at the core count** (4 streams on 4 vCPUs; 6 and 8
  are no better). On a bigger server, more streams keep helping until the WAL device is the limit.
* **`COLLATE "C"` on text keys is free speed** if the application only needs equality and byte-order range
  scans on those columns: index builds are 35 % faster because comparisons are `memcmp` instead of `strcoll`.
  It changes `ORDER BY` on those columns to byte order; all four benchmark queries return identical results.
* **Binary COPY beats CSV** (ADBC: −27 % on the COPY phase). The final `pg_parallel_swap` still uses CSV, so
  its 20M numbers leave that on the table — about a quarter of the 64 s COPY phase, if the 1M ratio holds.
* **`synchronous_commit=off` does nothing** for a handful of large transactions; `maintenance_work_mem` beyond
  64 MB does little once the sort no longer has to merge many runs.
* **`pandas.to_sql(method="multi")` is 12× slower than `COPY` into the same indexed table** and 39× slower
  than `pg_parallel_swap`. If the pipeline uses it today, that — not PostgreSQL — is the bottleneck.

## The no-WAL variants and what they cost

| | `pg_unlogged_swap` | `pg_unlogged_setlogged` | `pg_walminimal_swap` |
|---|---|---|---|
| Refresh at 20M | 90 s | initial load 3 min 09 s; **refresh failed: disk full** | 106 s |
| Survives a crash | **No** — the table is truncated to empty at crash recovery | yes | yes |
| Exists on replicas | **No** | yes | no replicas possible |
| Available on managed services | `UNLOGGED` yes (Aurora: writer only) | yes | no (`wal_level` is fixed) |

* **`UNLOGGED` then `SET LOGGED` does not pay off.** `SET LOGGED` rewrites the whole table and both indexes
  through the WAL: 125 s at 20M, more than the 66 s COPY it was meant to speed up, and it needs a third copy of
  the data. On this 70 GB disk the refresh ran out of space (old table + unlogged table + rewritten table + WAL).
* **`UNLOGGED` is a legitimate choice only when the table is a cache**: the source Parquet still exists, an
  empty table after a crash is acceptable until the next refresh, and nothing reads it from a replica.
* **`wal_level=minimal`** gets most of the gain while staying crash-safe (the new table is fsync'ed at commit
  instead of logged), but disables streaming replication and point-in-time recovery for the whole server.

## Production caveats for the swap pattern

* **Disk:** the old and the new table coexist. Peak extra space during a 20M refresh was 20 GB on top of the
  16 GB live table, including WAL. Plan for 2.5× the table size.
* **WAL volume:** a logged load writes about 0.5 GB of WAL per million rows here (≈ 10 GB at 20M), all of which
  streaming replicas must receive and replay; expect replica lag of the order of the load time. Loading into the
  indexed table writes twice as much (≈ 1 GB per million rows).
* **The rename takes an `ACCESS EXCLUSIVE` lock** for a few milliseconds, but it has to *wait* for every query
  still running on the old table, and new queries queue behind it. Set `lock_timeout` on the swap and retry,
  so that one long report cannot freeze the application.
* **Drop the old table after the rename commits, not inside the same transaction** — see the swap-stall
  diagnostic in the README. Foreign keys, views and grants that reference the table by OID must be re-created on
  the new table before the swap; a view over `big_table` keeps pointing at the *old* table after a rename.
* **Autovacuum / statistics:** `ANALYZE` on the new table is part of the measured time (about 1 s); skipping it
  leaves the planner without statistics until autovacuum notices.
