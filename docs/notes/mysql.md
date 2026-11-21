# MySQL 8.4 (InnoDB) — engineering notes

MySQL 8.4.13 (`mysql:8.4`), 4 GB buffer pool, 8 GB redo log, default durability
(`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`) and **binary logging on**, as in any replicated setup.
Code: [`bench/solutions/mysql.py`](../../bench/solutions/mysql.py). Client: `pymysql` (pure Python).

## Design

`mysql_load_swap` is the InnoDB version of the PostgreSQL bulk-load pattern:

1. `CREATE TABLE big_table_new (… PRIMARY KEY (id))` — no secondary indexes yet.
2. 4 connections, each `LOAD DATA LOCAL INFILE` from a named pipe fed with CSV converted from Parquet
   (`unique_checks=0`, `foreign_key_checks=0` in the loading sessions).
3. One `ALTER TABLE … ADD UNIQUE KEY (tenant_id, item_key, period), ADD KEY (ref_path, period), ALGORITHM=INPLACE`
   (sorted, parallel index build: `innodb_ddl_threads=4`, 1 GB DDL buffer).
4. `ANALYZE TABLE`, then `RENAME TABLE big_table TO big_table_old, big_table_new TO big_table` — atomic.

Text keys are `VARCHAR … CHARACTER SET ascii COLLATE ascii_bin`: byte comparisons, 1 byte per character.

### The clustering key decides everything

InnoDB stores the table *inside* the primary-key B-tree, so the choice of primary key is the choice of
physical row order.

| 1M rows | cluster on `id` (chosen) | cluster on the business key |
|---|---|---|
| Load + index | 15.3 s | 15.7 s |
| Size | **840 MB** | 1,220 MB |
| `point` p50 | 0.28 ms | 0.18 ms |
| `full_agg` p50 | 1,336 ms | 305 ms |

At 1M rows everything fits in the buffer pool and the two are equal on load time. Clustering on the business
key inserts every 500-byte row at a random position of the tree; once the table is larger than the buffer pool
that becomes random I/O, and it already costs 45 % more space from page splits. Clustering on `id` appends.
The price is one extra B-tree hop on point lookups and slower aggregates (they read through secondary indexes).

## Measured

| | 5M | 20M |
|---|---|---|
| Initial load | 66 s | **4 min 41 s** (load 159 s, indexes 122 s) |
| Refresh over the live table | 96 s | not run (see below) |
| Size (table + 2 indexes) | 4.1 GB | 16.3 GB (10.4 + 2.9 + 2.9) |
| `point` / `series` p50 | 0.42 / 0.38 ms | 0.43 / 0.39 ms |
| `tenant_agg` p50 | 19 ms | 187 ms |
| `full_agg` p50 | 6.6 s | **105 s** |
| QPS, 8 clients | 4,675 | 4,678 |
| Upsert 1 % / 10 % | 1.7 s / 17 s | 18 s / 2 min 53 s |
| Replace largest tenant | 14 s | 84 s |

The 20M run is a single load: two 16 GB table copies plus the binary log of the load plus the redo log do not
fit on this machine's disk, so there is no refresh-under-read-load number at 20M. The data directory was
33 GB after one load.

## What to know before choosing it

* **Nothing here argues for moving from PostgreSQL to MySQL.** With the same pattern MySQL needs 4 min 41 s
  where PostgreSQL needs 2 min 01 s for the same initial load, and analytical queries over the table are far slower (the full-table
  aggregate takes 105 s against 2.2 s).
* If MySQL is already the product's database, the pattern still works and stays inside 5 minutes at 20M on
  this hardware — barely. Point lookups and concurrency are on par with PostgreSQL.
* **The binary log doubles the write volume of a bulk load** and every replica has to replay it; on a replicated
  cluster expect replica lag of the same order as the load time.
* `RENAME TABLE a TO b, c TO a` is atomic and was observed to stall readers for less than 0.5 s at 5M.
