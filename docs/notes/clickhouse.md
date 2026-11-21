# ClickHouse — engineering notes

ClickHouse 26.10 (`clickhouse/clickhouse-server:latest`), single node, 8 GB server memory cap, default
durability settings. Code: [`bench/solutions/clickhouse.py`](../../bench/solutions/clickhouse.py).
All numbers below are from the final runs (`results/raw/ch_*__{5M,20M}__final.json`).

## Design

```sql
CREATE TABLE big_table (
    id Int64, tenant_id Int32, period Int32, quantity Float64, price Float64, score Float64,
    item_key String, ref_path String,            -- LZ4
    description String CODEC(ZSTD(1)),           -- 60 % of the bytes, never filtered on
    PROJECTION by_ref INDEX (ref_path, period) TYPE basic      -- ch_mergetree
    -- INDEX ix_ref ref_path TYPE bloom_filter(0.001) GRANULARITY 8   -- ch_replacing
) ENGINE = MergeTree                              -- or ReplacingMergeTree(_ver)
PARTITION BY tenant_id % 8
ORDER BY (tenant_id, item_key, period)
SETTINGS index_granularity = 256, min_compress_block_size = 8192;
```

* **Index A is the sort order.** The business key is the `ORDER BY`, so it costs no extra bytes.
* **`index_granularity = 256`** instead of the default 8192: a point lookup decompresses one small granule
  instead of ~4 MB of 500-byte rows. This is the main serving knob for wide rows.
* **8 partitions (`tenant_id % 8`)** exist for the loader, not for the queries: after the insert, each partition
  is merged into one part by its own background thread, so the final merge runs 8-wide instead of single-threaded,
  and a tenant replacement only has to rebuild 1/8 of the table.
* **Index B needs a decision.** `ch_mergetree` uses a *projection index* (a hidden sorted copy of
  `(ref_path, period)` with a pointer back to the row); `ch_replacing` uses a bloom-filter skip index because
  projections are not used by `FINAL` queries.
* **Loading** is one statement: `INSERT INTO new SELECT … FROM file('…parquet')` (in production: `s3(...)`), then
  `OPTIMIZE … FINAL` until every partition is one part, then `EXCHANGE TABLES new AND big_table` — atomic.
  The clock includes the merge, because query latency is only stable once the parts are merged.
* **Lookups run with `max_threads = 1`.**

## Two answers to "how do I change rows?"

| | `ch_mergetree` | `ch_replacing` |
|---|---|---|
| Idea | table is immutable; every change rebuilds merged parts off-line and swaps them in | `ReplacingMergeTree`: newest version of a key wins; every read uses `FINAL` |
| Upsert 1 % / 10 % at 20M | 88 s / 104 s (≈ a full refresh: rewrite everything not in the delta) | **1.1 s / 7.3 s** (plain `INSERT`) |
| Replace largest tenant | 27 s (rebuild one partition, `REPLACE PARTITION`) | **5.4 s** (`INSERT` + lightweight `DELETE` of rows not re-delivered) |
| `series` p50 before → after maintenance | 13 ms → 14 ms | 23 ms → **452 ms** until background merges catch up |
| `tenant_agg` p50 before → after | 4.1 ms → 4.8 ms | 3.9 ms → 14 ms |
| QPS, 8 clients | 314 | 200 |

`ReplacingMergeTree` makes writes nearly free and moves the cost to the readers: `FINAL` has to merge
versions at query time, and right after a maintenance run the secondary lookup is 20× slower. The
immutable design keeps reads flat but turns every small change into a rebuild. Pick by which side matters.

## Measured, 20M rows

| | `ch_mergetree` | `ch_replacing` |
|---|---|---|
| Full refresh over the live table | 99 s (insert 72 s, merge 19 s, swap 6 s) | 86 s |
| Reader errors during refresh | 0 | 0 |
| Size on disk | **2.2 GB** (9.1 GB uncompressed; index B = 0.10 GB) | 2.1 GB |
| `point` p50 / p95 | 4.2 / 6.5 ms | 3.7 / 6.0 ms |
| `series` p50 / p95 | 13 / 20 ms | 23 / 62 ms |
| `tenant_agg` p50 | 4.1 ms | 3.9 ms |
| `full_agg` p50 | 120 ms | 111 ms |

## What to know before choosing it

* **It is a second system, not a replacement for the OLTP database.** No multi-statement transactions,
  no foreign keys, no enforced uniqueness; row-level updates are either asynchronous mutations or
  `ReplacingMergeTree` semantics. The product's other tables stay in PostgreSQL.
* **Serving is the weak side.** Point lookups are ~4 ms (PostgreSQL: 0.2 ms) and concurrency is lower by an
  order of magnitude on this box: 200–300 QPS with 8 clients against ~4,800 for PostgreSQL. The client here is
  plain HTTP + JSON from Python; a native-protocol client would shave some of that, not a factor of ten.
  Fine for dashboards and for an API behind a cache; not a drop-in for a hot key-value path.
* **Aggregations are the strong side.** The per-tenant dashboard query is 10× faster than PostgreSQL at 20M
  (4 ms vs 45 ms) and the full-table aggregate is 19× faster (120 ms vs 2.2 s).
* **Disk during refresh:** the old table, the new unmerged parts and the merged parts coexist briefly
  (peak ≈ 8 GB extra at 20M, 3–4× the final size).
* **Replication** needs ClickHouse Keeper and `Replicated*MergeTree`; `EXCHANGE TABLES` is then issued
  `ON CLUSTER`. Not measured here. ClickHouse Cloud hides this (SharedMergeTree).
* `PROJECTION … INDEX … TYPE basic` is a recent feature; on older versions use a normal projection
  (stores the projected columns) or a second table.
