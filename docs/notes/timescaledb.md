# TimescaleDB — engineering notes

TimescaleDB 2.32.1 on PostgreSQL 17.12 (`timescale/timescaledb:latest-pg17`, Timescale License),
`timescaledb-tune` disabled, same server configuration as the vanilla PostgreSQL solutions plus
`shared_preload_libraries=timescaledb`. Code: [`bench/solutions/timescale.py`](../../bench/solutions/timescale.py).

The question: **does TimescaleDB help for this workload** — a tenant-keyed table that is bulk-replaced, not an
append-only time series?

## Three solutions

| | What it is |
|---|---|
| `ts_plain_control` | a plain PostgreSQL table inside the TimescaleDB image, same loader — the control |
| `ts_hypertable` | row-store hypertable, `by_range(period, 7)` → 3 chunks; 4 COPY streams; indexes after load |
| `ts_columnstore` | hypertable + columnstore: `segmentby tenant_id`, `orderby item_key, period`, bloom sparse index on `ref_path`; COPY, then convert the chunks in parallel |

## Measured

| | PostgreSQL `pg_parallel_swap` | `ts_hypertable` | `ts_columnstore` |
|---|--:|--:|--:|
| **Refresh, 20M** | **2 min 21 s** | 3 min 27 s | 3 min 58 s |
| – COPY | 64 s | 132 s | 125 s |
| – indexes / conversion | 69 s | 60 s | 106 s conversion + 5 s |
| Size, 20M | 15.3 GB | 15.3 GB | **4.1 GB** |
| `point` p50 | 0.21 ms | 0.15 ms | 2.3 ms |
| `series` p50 | 0.14 ms | 0.50 ms | **205 ms** |
| `tenant_agg` p50 | 45 ms | 60 ms | **4.3 ms** |
| `full_agg` p50 | 2.2 s | 2.2 s | **0.5 s** |
| QPS, 8 clients (point + series) | 4,832 | 4,170 | **35** |
| Upsert 1 % / 10 % | 32 s / 4 min 09 s | 35 s / 6 min 31 s | **6 min 46 s / 9 min 05 s** |
| Replace largest tenant | 31 s | 47 s | 64 s |
| Size after maintenance | 17.4 GB | 18.1 GB | **24.4 GB** |

At 5M the control (`ts_plain_control`, 30 s) is on par with `pg_incremental` (29 s), which uses an equally
simple loader, and 15 % behind the tuned `pg_parallel_swap` (26 s); the hypertable needs 48 s. The image itself
is not what costs the time — the hypertable and the columnstore are.

## Findings

* **The row-store hypertable only adds cost here.** COPY through the chunk router takes twice as long
  (132 s vs 64 s), the refresh is 47 % slower, and no query gets faster: nothing in this workload prunes by
  time, and `series` has to probe an index per chunk.
* **The columnstore delivers what it promises for analytics**: 3.7× smaller, per-tenant aggregates 10× faster,
  full-table aggregate 4× faster.
* **…and is unsuitable as the serving store for this table.**
  * The second access path has no index over compressed data. The bloom sparse index still has to visit the
    metadata of every batch: 205 ms per `ref_path` lookup at 20M, and it scales with table size
    (17 ms at 1M, 57 ms at 5M). Under 8 concurrent clients the box manages 35 queries per second.
  * Scattered upserts decompress every batch they touch. A 1 % upsert took 6 min 46 s — longer than a full
    refresh — and the three maintenance operations inflated the table from 4.1 GB to 24.4 GB, because the
    touched rows now sit in the row store *with* their B-tree entries until a policy job recompresses them.
  * Loading directly into the columnstore (`timescaledb.enable_direct_compress_copy`) was slower than
    COPY-then-convert at 1M (16.7 s vs 12.6 s, `results/raw/ts_columnstore__1M__lab_load_*.json`; one run each
    on a machine that was not idle) and gave slower secondary lookups (37 ms vs 17 ms), so it is not used.
* **Dropping a hypertable inside the swap transaction deadlocked with a reader** in the first 5M run: `DROP`
  locks the chunks before the parent, queries lock the parent first. Renaming inside the transaction and
  dropping afterwards (with an explicit `LOCK TABLE` on the parent first) fixed it; no deadlock in any later run.
  Readers still paused for about 6 s at the 20M swap. That is consistent with the autovacuum wait described in
  [`postgres.md`](postgres.md) (1 s per relation being vacuumed, here the parent and its chunks), but it was not
  isolated for hypertables.

## Verdict for this workload

TimescaleDB is the right tool when rows arrive in time order and old chunks become read-only. This table is
replaced wholesale and looked up by tenant and by a text key; for that, plain PostgreSQL with the bulk-load
pattern is faster to refresh and faster to serve, and partitioning by tenant (see
[`postgres_incremental.md`](postgres_incremental.md)) matches the access pattern better than partitioning by
time. The columnstore is worth a second look only if per-tenant analytics dominate and the table is rebuilt,
never upserted.

Availability: the columnstore is under the Timescale License (Tiger Cloud or self-hosted community edition);
it is not offered on Amazon RDS / Aurora.
