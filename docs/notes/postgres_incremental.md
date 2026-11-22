# PostgreSQL: maintain instead of rebuild — engineering notes

Code: [`bench/solutions/pg_partition.py`](../../bench/solutions/pg_partition.py) (`pg_incremental`,
`pg_partition_swap`). Default maintenance used by every other PostgreSQL solution:
[`bench/solutions/pg_common.py`](../../bench/solutions/pg_common.py).

The business question: **refresh everything a few times a day, or apply only what changed?**

## The answer depends on whether the table fits in memory

Upsert = `COPY` the delta into a temp table, then one
`INSERT … SELECT … ON CONFLICT (tenant_id, item_key, period) DO UPDATE`.
The delta is scattered over the whole table (the hard case): 75 % updates, 25 % new keys.

| Unpartitioned table (`pg_parallel_swap`) | 5M rows (3.8 GB, fits in RAM) | 20M rows (15.3 GB, does not) |
|---|--:|--:|
| Full refresh | 28 s | 2 min 21 s |
| Upsert 1 % | 2.0 s | **32 s** |
| Upsert 10 % | 15 s | **4 min 09 s** |
| Replace largest tenant (5 %): `DELETE` + `COPY` | 4.5 s | 31 s |
| Size before → after the three operations | 3.83 → 4.35 GB (+14 %) | 15.32 → 17.37 GB (+13 %) |

* At 5M, a 10 % upsert costs half a full refresh. **At 20M it costs almost twice a full refresh.**
  Row count went up 4×, upsert time 16×: every changed row needs a random page in the heap and in two indexes,
  and once those pages are not in memory each of them is a disk read.
* By linear interpolation between the two measured points, the break-even at 20M on this machine is at roughly
  **5 % of rows changed**; above that, rebuilding is faster. This is an estimate from two points, and it moves
  with RAM: with the whole table cached, the 5M ratio (break-even near 20 %) applies.
* **Updates bloat.** Each maintenance cycle left the table 13–14 % larger (dead row versions, split index
  pages). A full refresh resets that to zero; an incrementally maintained table needs autovacuum to keep up and
  will settle at a larger size.

### Upsert method does not matter much

5M rows, same table, same deltas (`results/raw/pg_incremental__5M__lab_upsert_*.json`):

| Method | Upsert 1 % | Upsert 10 % |
|---|--:|--:|
| `INSERT … ON CONFLICT DO UPDATE` | **1.9 s** | **16.3 s** |
| `DELETE … USING` + `COPY` | 2.1 s | 17.0 s |
| `MERGE` | 2.4 s | 19.2 s |
| `UPDATE … FROM` + `INSERT … WHERE NOT EXISTS` | 2.5 s | 21.1 s |

All four are within 30 %. The cost is the random I/O, not the SQL statement.

## Partitioning by tenant: `pg_partition_swap`

`PARTITION BY LIST (tenant_id)`: every tenant with at least 1 % of the rows gets its own partition, the rest
are spread over 8 buckets (15 partitions at 20M, the largest holds 2.2M rows). Index A stays `UNIQUE` because
it contains the partition key; index B becomes one local index per partition.

| 20M rows | Unpartitioned | Partitioned |
|---|--:|--:|
| Full refresh | 2 min 21 s | 2 min 07 s |
| **Replace largest tenant** | 31 s | **9.1 s** |
| Upsert 1 % / 10 % (scattered) | 32 s / 4 min 09 s | 34 s / 5 min 02 s |
| `point` p50 | 0.21 ms | 0.21 ms |
| **`series` p50** (lookup by `ref_path`, no tenant in the predicate) | 0.14 ms | **0.83 ms** |
| `tenant_agg` p50 | 45 ms | 42 ms |
| QPS, 8 clients | 4,832 | 3,740 |

* **Replacing one tenant becomes a partition exchange**: load the tenant's new data into a standalone table,
  index it, then `DETACH` the old partition and `ATTACH` the new one in a single transaction (a matching `CHECK`
  constraint avoids the validation scan). 9 s for a 1M-row tenant, no bloat, and nothing else in the table is
  touched. This is the cheapest way in the study to refresh "the client whose data was recomputed".
* **Scattered upserts gain nothing** from partitioning — they still touch every partition.
* **The price is on the secondary access path.** A query that does not contain `tenant_id` has to probe index B
  in every partition: `series` is 6× slower with 15 partitions, and throughput drops 23 %. With hash layouts the
  penalty grows with the partition count. If the application always knows the tenant, add `tenant_id` to that
  predicate and the penalty disappears.
* The full refresh is no slower than the unpartitioned one, even with the default collation: 15 small index
  builds sort faster than one big one.

## Production caveats

* `ATTACH PARTITION` takes `SHARE UPDATE EXCLUSIVE` on the parent; `DETACH` (non-concurrent) and the swap of the
  whole table take `ACCESS EXCLUSIVE` briefly. Use `lock_timeout` and retry.
* After a 10 % upsert, autovacuum has ~2M dead rows to clean on a 16 GB table; until it finishes, index-only
  scans degrade and the bloat stays.
* A 10 % upsert at 20M runs for four minutes and is fully WAL-logged; replicas lag accordingly.
* Maintenance jobs here are serialised with an advisory lock. Two concurrent upserts touching the same keys
  would otherwise deadlock or wait on each other row by row.
