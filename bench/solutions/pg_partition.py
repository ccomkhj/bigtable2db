"""PostgreSQL: avoid the full refresh.

Two solutions (design notes and every measured variant: docs/notes/postgres_incremental.md):

``pg_incremental``
    Unpartitioned table, same DDL as the baseline. Maintained with plain SQL
    (staging table + upsert / DELETE + COPY). Its full refresh is a minimal
    "new table, parallel COPY, indexes afterwards, atomic rename".

``pg_partition_swap``
    The table is declaratively partitioned by tenant. A full refresh builds a
    complete new partitioned table (parallel COPY through the parent, then
    per-partition index builds on one connection per worker) and swaps it in
    with one catalog-only transaction. ``replace_tenant`` rebuilds only the
    partition that holds the tenant as a standalone table and exchanges it
    (DETACH + ATTACH + DROP in one transaction, validation scan avoided with a
    CHECK constraint equal to the partition constraint).

Everything here works with a plain (non-superuser) role over a client
connection and is fully WAL-logged.

Variants are selected with environment variables so that the standard runner
can measure them; the defaults are the variants that won:

    B2D_PG_UPSERT        on_conflict | merge | update_insert | delete_copy | batch_insert
    B2D_PG_STAGE         temp | unlogged
    B2D_PG_SORT          none | server | client      (order the delta by business key)
    B2D_PG_FILLFACTOR    heap fillfactor (100 = default DDL)
    B2D_PG_REPLACE       delete_copy | merge_diff    (pg_incremental only)
    B2D_PG_PART_LAYOUT   list:<min_share_pct>:<tail_buckets> | hash:<n>
    B2D_PG_PLAN_CACHE    auto | force_custom_plan | force_generic_plan (reader sessions)
    B2D_PG_LOAD_WORKERS  parallel COPY / index-build connections
    B2D_PG_COLLATE_C     1 = text key columns COLLATE "C" (faster index builds; not the baseline DDL)
    B2D_PG_VACUUM_PROBE  path of a JSON file; if set, VACUUM is timed in teardown()
"""
from __future__ import annotations

import io
import json
import os
import queue
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, Sequence

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .. import spec
from . import register
from .base import Client, Dataset, Phases
from .pg_common import (PG_QUERIES, PgClient, PostgresBase, copy_parquet, create_table_sql, index_a_sql,
                        index_b_sql, rename_swap)

COLS = ", ".join(spec.COLUMN_NAMES)
KEY_COLS = list(spec.KEY)
KEYS = ", ".join(KEY_COLS)
NONKEY = [c for c in spec.COLUMN_NAMES if c not in spec.KEY]
MAINT_LOCK = 0x62326462  # advisory lock: maintenance jobs on big_table are serialized


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# --------------------------------------------------------------------------- parquet -> COPY helpers
def csv_chunks(files: Iterable[Path], columns: Sequence[str] | None = None, sort: bool = False,
               batch_rows: int = 50_000):
    """CSV byte chunks for COPY. `columns` projects, `sort` orders each file by the business key
    (binary order; close enough to the index order to give index-leaf locality)."""
    opts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
    for f in files:
        if sort:
            t = pq.read_table(f, columns=list(columns) if columns else None)
            t = t.take(pc.sort_indices(t, sort_keys=[(k, "ascending") for k in KEY_COLS]))
            batches = t.to_batches(max_chunksize=batch_rows)
        else:
            batches = pq.ParquetFile(f).iter_batches(batch_size=batch_rows,
                                                     columns=list(columns) if columns else None)
        for batch in batches:
            if columns:
                batch = batch.select(list(columns))
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, write_options=opts)
            yield buf.getvalue()


def copy_chunks(conn: psycopg.Connection, table: str, chunks: Iterable[bytes],
                columns: Sequence[str] = spec.COLUMN_NAMES, freeze: bool = False) -> None:
    opt = "FORMAT csv, FREEZE" if freeze else "FORMAT csv"
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN ({opt})") as cp:
            for chunk in chunks:
                cp.write(chunk)


def _feed(q: "queue.SimpleQueue"):
    while True:
        try:
            yield q.get_nowait()
        except queue.Empty:
            return


def run_parallel(n: int, fn, items: Iterable | None = None) -> None:
    """Run fn(worker_no) on n threads (or fn(item) over items) and re-raise the first error."""
    with ThreadPoolExecutor(n) as ex:
        futs = [ex.submit(fn, x) for x in (items if items is not None else range(n))]
        errs = [f.exception() for f in futs]
    for e in errs:
        if e is not None:
            raise e


# --------------------------------------------------------------------------- shared behaviour
class _MaintBase(PostgresBase):
    """Common pieces of both solutions: connection handling, parallel COPY,
    the plain-SQL upsert variants and the optional VACUUM probe."""

    port = 55434
    dbname = "postgres"            # experiments point this at cloned databases
    atomic_swap = True

    def __init__(self):
        self.cfg = {
            "upsert": _env("B2D_PG_UPSERT", self.default_upsert),
            "stage": _env("B2D_PG_STAGE", "temp"),
            "sort": _env("B2D_PG_SORT", "none"),
            "fillfactor": int(_env("B2D_PG_FILLFACTOR", "100")),
            "replace": _env("B2D_PG_REPLACE", "delete_copy"),
            "load_workers": int(_env("B2D_PG_LOAD_WORKERS", "4")),
            "plan_cache_mode": _env("B2D_PG_PLAN_CACHE", self.default_plan_cache),
        }
        self.collate_c = _env("B2D_PG_COLLATE_C", "0") == "1"     # text keys COLLATE "C" (not the baseline DDL)

    def _ddl(self, name: str, extra: str = "") -> str:
        return create_table_sql(name, collate_c=self.collate_c, extra=extra)

    default_upsert = "on_conflict"
    default_plan_cache = "auto"

    @property
    def dsn(self) -> str:
        return f"host=127.0.0.1 port={self.port} user=postgres password=bench dbname={self.dbname}"

    def info(self) -> dict:
        out = super().info()
        out["variant"] = dict(self.cfg)
        out["session_settings"] = {"load": self.LOAD_SESSION, "maintenance": self.MAINT_SESSION}
        return out

    def client(self) -> Client:
        mode = self.cfg["plan_cache_mode"]
        dsn = self.dsn if mode == "auto" else self.dsn + f" options='-c plan_cache_mode={mode}'"
        return PgClient(dsn, PG_QUERIES)

    # session-level settings (allowed for any role, also on managed services)
    LOAD_SESSION = ["SET synchronous_commit = off"]          # only affects the loader's own bulk statements
    MAINT_SESSION = ["SET work_mem = '256MB'"]

    def _heap_opts(self) -> str:
        ff = self.cfg["fillfactor"]
        return f"WITH (fillfactor = {ff})" if ff != 100 else ""

    def _session(self, c: psycopg.Connection, stmts: Sequence[str]) -> None:
        for s in stmts:
            c.execute(s)

    def parallel_copy(self, table: str, files: Sequence[Path]) -> None:
        """One COPY stream per worker connection; workers pull files from a shared queue.
        Parquet decode + CSV encode run in the worker threads (pyarrow releases the GIL)."""
        q: queue.SimpleQueue = queue.SimpleQueue()
        for f in files:
            q.put(f)

        def work(_):
            with self.connect() as c:
                self._session(c, self.LOAD_SESSION)
                copy_parquet(c, table, _feed(q))

        run_parallel(min(self.cfg["load_workers"], len(files)), work)

    # ---- upsert variants -----------------------------------------------------
    def _stage(self, c, delta: Path, phases: Phases, columns: Sequence[str] = spec.COLUMN_NAMES,
               name: str = "delta_stage") -> str:
        """Load the delta into a staging table inside the maintenance transaction."""
        with phases("stage_copy"):
            ddl = self._ddl(name)
            if list(columns) != spec.COLUMN_NAMES:
                ddl = f"CREATE TABLE {name} (tenant_id INTEGER NOT NULL, item_key TEXT NOT NULL, period INTEGER NOT NULL)"
            if self.cfg["stage"] == "temp":
                c.execute(ddl.replace("CREATE TABLE", "CREATE TEMP TABLE") + " ON COMMIT DROP")
            else:  # unlogged: shared buffers, visible to parallel workers, still no WAL for the rows
                c.execute(f"DROP TABLE IF EXISTS {name}")
                c.execute(ddl.replace("CREATE TABLE", "CREATE UNLOGGED TABLE"))
            copy_chunks(c, name, csv_chunks([delta], columns if list(columns) != spec.COLUMN_NAMES else None,
                                            sort=self.cfg["sort"] == "client"), columns)
        with phases("stage_analyze"):
            c.execute(f"ANALYZE {name}")       # autovacuum never sees a temp table; the join needs row counts
        return name

    def _unstage(self, c, name: str = "delta_stage") -> None:
        if self.cfg["stage"] != "temp":
            c.execute(f"DROP TABLE {name}")

    def _order(self) -> str:
        return f" ORDER BY {KEYS}" if self.cfg["sort"] == "server" else ""

    def _upsert_on_conflict(self, c, delta: Path, phases: Phases) -> None:
        st = self._stage(c, delta, phases)
        sets = ", ".join(f"{k} = EXCLUDED.{k}" for k in NONKEY)
        with phases("merge"):
            c.execute(f"INSERT INTO big_table ({COLS}) SELECT {COLS} FROM {st}{self._order()} "
                      f"ON CONFLICT ({KEYS}) DO UPDATE SET {sets}")
        self._unstage(c)

    def _upsert_merge(self, c, delta: Path, phases: Phases) -> None:
        st = self._stage(c, delta, phases)
        on = " AND ".join(f"t.{k} = s.{k}" for k in KEY_COLS)
        src = f"(SELECT * FROM {st}{self._order()})" if self._order() else st
        with phases("merge"):
            c.execute(f"MERGE INTO big_table t USING {src} s ON {on} "
                      f"WHEN MATCHED THEN UPDATE SET {', '.join(f'{k} = s.{k}' for k in NONKEY)} "
                      f"WHEN NOT MATCHED THEN INSERT ({COLS}) VALUES ({', '.join('s.' + k for k in spec.COLUMN_NAMES)})")
        self._unstage(c)

    def _upsert_update_insert(self, c, delta: Path, phases: Phases) -> None:
        st = self._stage(c, delta, phases)
        on = " AND ".join(f"t.{k} = s.{k}" for k in KEY_COLS)
        with phases("update"):
            c.execute(f"UPDATE big_table t SET {', '.join(f'{k} = s.{k}' for k in NONKEY)} FROM {st} s WHERE {on}")
        with phases("insert"):
            c.execute(f"INSERT INTO big_table ({COLS}) SELECT {COLS} FROM {st} s "
                      f"WHERE NOT EXISTS (SELECT 1 FROM big_table t WHERE {on})")
        self._unstage(c)

    def _upsert_delete_copy(self, c, delta: Path, phases: Phases) -> None:
        """Stage only the keys, DELETE the old versions, COPY the delta straight into the table."""
        st = self._stage(c, delta, phases, columns=KEY_COLS, name="delta_keys")
        on = " AND ".join(f"t.{k} = s.{k}" for k in KEY_COLS)
        with phases("delete"):
            c.execute(f"DELETE FROM big_table t USING {st} s WHERE {on}")
        with phases("copy"):
            copy_chunks(c, "big_table", csv_chunks([delta], sort=self.cfg["sort"] == "client"))
        self._unstage(c, "delta_keys")

    def _upsert_batch_insert(self, c, delta: Path, phases: Phases) -> None:
        """What an ORM / driver-level upsert does: no staging, multi-row INSERT .. ON CONFLICT batches."""
        sets = ", ".join(f"{k} = EXCLUDED.{k}" for k in NONKEY)
        sql = (f"INSERT INTO big_table ({COLS}) VALUES ({', '.join(['%s'] * len(spec.COLUMN_NAMES))}) "
               f"ON CONFLICT ({KEYS}) DO UPDATE SET {sets}")
        with phases("merge"):
            with c.cursor() as cur:
                for batch in pq.ParquetFile(delta).iter_batches(batch_size=5_000):
                    cols = [batch.column(n).to_pylist() for n in spec.COLUMN_NAMES]
                    cur.executemany(sql, list(zip(*cols)))

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        with self.connect(autocommit=False) as c:
            self._session(c, self.MAINT_SESSION)
            c.execute("SELECT pg_advisory_xact_lock(%s)", [MAINT_LOCK])
            getattr(self, "_upsert_" + self.cfg["upsert"])(c, delta, phases)
            with phases("commit"):
                c.commit()

    # ---- optional post-run probe: what does VACUUM cost after the maintenance steps? ----
    def _sizes(self, c) -> dict:
        s = self.storage()
        return {k: s.get(k) for k in ("table", "index_a", "index_b", "total")}

    def vacuum_probe(self) -> dict:
        with self.connect() as c:
            before = self._sizes(c)
            dead = c.execute("SELECT coalesce(sum(n_dead_tup), 0), coalesce(sum(n_live_tup), 0), "
                             "coalesce(sum(n_tup_upd), 0), coalesce(sum(n_tup_hot_upd), 0) "
                             "FROM pg_stat_user_tables WHERE relname = 'big_table' OR relname LIKE 'bt\\_%'").fetchone()
            lsn0 = c.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0]
            t0 = time.perf_counter()
            c.execute("VACUUM (ANALYZE) big_table")
            dt = time.perf_counter() - t0
            wal = c.execute("SELECT pg_current_wal_insert_lsn() - %s::pg_lsn", [lsn0]).fetchone()[0]
            return {"solution": self.name, "vacuum_analyze_s": round(dt, 3), "wal_bytes": int(wal),
                    "dead_tup_stat": int(dead[0]), "live_tup_stat": int(dead[1]), "n_tup_upd": int(dead[2]),
                    "n_tup_hot_upd": int(dead[3]), "before": before, "after": self._sizes(c)}

    def teardown(self) -> None:
        probe = os.environ.get("B2D_PG_VACUUM_PROBE")
        if probe:
            try:
                Path(probe).write_text(json.dumps(self.vacuum_probe(), indent=1))
            except Exception as e:  # noqa: BLE001
                print(f"[{self.name}] vacuum probe failed: {e!r}")
        super().teardown()


# =========================================================================== pg_incremental
@register
class PgIncremental(_MaintBase):
    """Unpartitioned big_table (baseline DDL), maintained incrementally with plain SQL."""

    name = "pg_incremental"
    strategy = ("unpartitioned; refresh = new table + 4 parallel COPY + indexes after + rename; "
                "upsert = temp staging + INSERT..ON CONFLICT; replace_tenant = DELETE + COPY")

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        new = "big_table_new"
        with self.connect() as c:
            with phases("create"):
                c.execute(f"DROP TABLE IF EXISTS {new}")
                c.execute(self._ddl(new, self._heap_opts()))
            with phases("copy"):
                self.parallel_copy(new, ds.base_files)

            def build(sql: str):
                with self.connect() as ci:
                    self._session(ci, self.LOAD_SESSION + ["SET maintenance_work_mem = '1GB'",
                                                           "SET max_parallel_maintenance_workers = 1"])
                    ci.execute(sql)

            with phases("index"):      # A and B concurrently, each leader + 1 parallel worker = 4 cores
                run_parallel(2, build, [index_a_sql(new), index_b_sql(new)])
            with phases("analyze"):
                c.execute(f"ANALYZE {new}")
            with phases("swap"):
                self.swap_in(c, new)

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        t = int(ds.replace_tenant_id)
        with self.connect(autocommit=False) as c:
            self._session(c, self.MAINT_SESSION)
            c.execute("SELECT pg_advisory_xact_lock(%s)", [MAINT_LOCK])
            if self.cfg["replace"] == "merge_diff":
                # PG17: MERGE through an auto-updatable view restricts the target to one tenant, and
                # WHEN NOT MATCHED BY SOURCE removes the keys that disappeared.
                st = self._stage(c, ds.replace_tenant_file, phases)
                with phases("merge"):
                    c.execute(f"CREATE TEMP VIEW bt_one_tenant AS SELECT * FROM big_table WHERE tenant_id = {t}")
                    c.execute(f"MERGE INTO bt_one_tenant t USING {st} s "
                              f"ON t.item_key = s.item_key AND t.period = s.period AND s.tenant_id = {t} "
                              f"WHEN MATCHED THEN UPDATE SET {', '.join(f'{k} = s.{k}' for k in NONKEY)} "
                              f"WHEN NOT MATCHED BY TARGET THEN INSERT ({COLS}) "
                              f"VALUES ({', '.join('s.' + k for k in spec.COLUMN_NAMES)}) "
                              f"WHEN NOT MATCHED BY SOURCE THEN DELETE")
                    c.execute("DROP VIEW bt_one_tenant")
                self._unstage(c)
            else:
                with phases("delete"):
                    c.execute("DELETE FROM big_table WHERE tenant_id = %s", [t])
                with phases("copy"):
                    copy_chunks(c, "big_table", csv_chunks([ds.replace_tenant_file],
                                                           sort=self.cfg["sort"] == "client"))
            with phases("commit"):
                c.commit()


# =========================================================================== pg_partition_swap
def tenant_counts(files: Sequence[Path]) -> dict[int, int]:
    """Rows per tenant, read from the tenant_id column only (a few hundred ms even at 20M rows)."""
    def one(f):
        vc = pc.value_counts(pq.read_table(f, columns=["tenant_id"]).column("tenant_id"))
        return dict(zip(vc.field(0).to_pylist(), vc.field(1).to_pylist()))

    out: dict[int, int] = {}
    with ThreadPoolExecutor(4) as ex:
        for d in ex.map(one, files):
            for k, v in d.items():
                out[k] = out.get(k, 0) + v
    return out


def list_layout(counts: dict[int, int], min_share_pct: float, tail_buckets: int) -> list[dict]:
    """LIST layout: one dedicated partition per tenant holding >= min_share_pct of the rows, the
    remaining tenants bin-packed (largest first, into the lightest bucket) into equally sized
    tail partitions. A DEFAULT partition (added by the caller) catches tenants unknown at build time."""
    total = sum(counts.values())
    big = sorted((t for t, n in counts.items() if n * 100.0 / total >= min_share_pct), key=lambda t: -counts[t])
    parts = [{"kind": "tenant", "tenants": [t], "rows": counts[t]} for t in big]
    buckets = [{"kind": "bucket", "tenants": [], "rows": 0} for _ in range(tail_buckets)]
    for t in sorted((t for t in counts if t not in set(big)), key=lambda t: -counts[t]):
        b = min(buckets, key=lambda b: b["rows"])
        b["tenants"].append(t)
        b["rows"] += counts[t]
    return parts + [b for b in buckets if b["tenants"]]


@register
class PgPartitionSwap(_MaintBase):
    """big_table partitioned by tenant; per-partition parallel build; partition exchange."""

    name = "pg_partition_swap"
    strategy = ("LIST-partitioned by tenant (dedicated partitions for big tenants + balanced tail buckets); "
                "refresh = parallel COPY + per-partition index builds + atomic swap; "
                "replace_tenant = rebuild one partition, DETACH/ATTACH in one transaction")
    default_layout = "list:1:8"

    def __init__(self):
        super().__init__()
        self.cfg["layout"] = _env("B2D_PG_PART_LAYOUT", self.default_layout)

    # ---- catalog helpers -----------------------------------------------------
    def _drop_stale(self, c) -> None:
        """Remove leftovers of aborted builds: bt_* tables that are not part of the live table."""
        rows = c.execute(
            "SELECT c.oid::regclass::text FROM pg_class c "
            "WHERE c.relkind IN ('r', 'p') AND c.relname LIKE 'bt\\_%' AND c.relnamespace = 'public'::regnamespace "
            "AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)").fetchall()
        for (name,) in rows:
            c.execute(f"DROP TABLE IF EXISTS {name} CASCADE")

    def _partitions(self, c, parent: str = "big_table") -> list[tuple[str, str, str]]:
        """(partition name, bound clause, partition constraint as SQL) for every partition."""
        return c.execute(
            "SELECT c.oid::regclass::text, pg_get_expr(c.relpartbound, c.oid), pg_get_partition_constraintdef(c.oid) "
            "FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = %s::regclass "
            "ORDER BY 1", [parent]).fetchall()

    def storage(self) -> dict:
        with self.connect() as c:
            table, total = c.execute(
                "SELECT coalesce(sum(pg_table_size(relid)), 0)::bigint, coalesce(sum(pg_total_relation_size(relid)), 0)::bigint "
                "FROM pg_partition_tree('big_table') WHERE isleaf").fetchone()
            idx = {}
            for n in ("big_table_a", "big_table_b"):
                idx[n] = c.execute("SELECT coalesce(sum(pg_relation_size(inhrelid)), 0)::bigint FROM pg_inherits "
                                   "WHERE inhparent = %s::regclass", [n]).fetchone()[0]
            nparts = c.execute("SELECT count(*) FROM pg_partition_tree('big_table') WHERE isleaf").fetchone()[0]
            db = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        return {"table": table, "index_a": idx["big_table_a"], "index_b": idx["big_table_b"], "total": total,
                "database": db, "partitions": nparts}

    # ---- full refresh --------------------------------------------------------
    def _create_partitions(self, c, parent: str, ds: Dataset, phases: Phases) -> list[tuple[str, int]]:
        """Create the empty partitioned table; returns [(partition, expected rows)]."""
        kind, *args = self.cfg["layout"].split(":")
        opts = self._heap_opts()
        parts: list[tuple[str, int]] = []
        if kind == "hash":
            n = int(args[0])
            c.execute(self._ddl(parent, "PARTITION BY HASH (tenant_id)"))
            for r in range(n):
                p = f"{parent}_p{r:02d}"
                c.execute(f"CREATE TABLE {p} PARTITION OF {parent} FOR VALUES WITH (MODULUS {n}, REMAINDER {r}) {opts}")
                parts.append((p, ds.rows // n))
            phases.note("layout", {"kind": "hash", "partitions": n})
        else:
            with phases("layout"):
                layout = list_layout(tenant_counts(ds.base_files), float(args[0]), int(args[1]))
            c.execute(self._ddl(parent, "PARTITION BY LIST (tenant_id)"))
            for i, part in enumerate(layout):
                p = f"{parent}_p{i:02d}"
                c.execute(f"CREATE TABLE {p} PARTITION OF {parent} "
                          f"FOR VALUES IN ({', '.join(map(str, part['tenants']))}) {opts}")
                parts.append((p, part["rows"]))
            p = f"{parent}_pdef"
            c.execute(f"CREATE TABLE {p} PARTITION OF {parent} DEFAULT {opts}")
            parts.append((p, 0))
            phases.note("layout", {"kind": "list", "partitions": len(parts),
                                   "dedicated": sum(1 for x in layout if x["kind"] == "tenant"),
                                   "tail_buckets": sum(1 for x in layout if x["kind"] == "bucket"),
                                   "largest_partition_rows": max(x["rows"] for x in layout)})
        return parts

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        gen = "bt_" + uuid.uuid4().hex[:8]
        workers = self.cfg["load_workers"]
        with self.connect() as c:
            with phases("create"):
                c.execute("SELECT pg_advisory_lock(%s)", [MAINT_LOCK])
                self._drop_stale(c)
                parts = self._create_partitions(c, gen, ds, phases)
            with phases("copy"):       # tuple routing by the server: every stream feeds all partitions
                self.parallel_copy(gen, ds.base_files)

            # per-partition index builds, largest first, one single-process build per connection
            tasks: queue.SimpleQueue = queue.SimpleQueue()
            for p, _ in sorted(parts, key=lambda x: -x[1]):
                tasks.put(index_a_sql(p))
                tasks.put(index_b_sql(p))
            par = 0 if len(parts) >= workers else 1

            def build(_):
                with self.connect() as ci:
                    self._session(ci, self.LOAD_SESSION + ["SET maintenance_work_mem = '768MB'",
                                                           f"SET max_parallel_maintenance_workers = {par}"])
                    for sql in _feed(tasks):
                        ci.execute(sql)

            def analyze():
                # PG17 has no ANALYZE ONLY: analysing the parent (inherited statistics, which autovacuum
                # never collects) also analyses every partition, one transaction each, single-threaded.
                # The data is complete after COPY, so this runs next to the index builds instead of after
                # them; it only ever waits for the one partition whose index is being built right now.
                with phases("analyze"), self.connect() as ca:
                    ca.execute(f"ANALYZE {gen}")

            with phases("index"):
                an = ThreadPoolExecutor(1)
                fut = an.submit(analyze)
                try:
                    run_parallel(workers, build)
                finally:
                    with phases("analyze_tail"):
                        an.shutdown(wait=True)
                fut.result()
            with phases("index_attach"):   # catalog only: the parent indexes adopt the partition indexes
                c.execute(index_a_sql(gen))
                c.execute(index_b_sql(gen))
            with phases("swap"):
                rename_swap(c, gen)   # renames only; the old generation is dropped after the commit
            c.execute("SELECT pg_advisory_unlock(%s)", [MAINT_LOCK])

    # ---- replace one tenant = exchange one partition ----------------------------
    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        t = int(ds.replace_tenant_id)
        with self.connect() as c:
            self._session(c, self.MAINT_SESSION)
            c.execute("SELECT pg_advisory_lock(%s)", [MAINT_LOCK])
            with phases("prepare"):
                old = bound = cdef = None
                for name, b, cd in self._partitions(c):
                    if c.execute(f"SELECT {cd} FROM (SELECT %s::int AS tenant_id) x", [t]).fetchone()[0]:
                        old, bound, cdef = name, b, cd
                        break
                if old is None:
                    raise RuntimeError(f"no partition accepts tenant {t}")
                dedicated = bound.replace(" ", "") == f"FORVALUESIN({t})"
                new = old.split("_r")[0] + "_r" + uuid.uuid4().hex[:6]
            with c.transaction():
                with phases("copy"):
                    # the CHECK equals the partition constraint, so ATTACH skips its validation scan
                    c.execute(self._ddl(new, self._heap_opts()))
                    c.execute(f"ALTER TABLE {new} ADD CONSTRAINT {new}_pck CHECK ({cdef})")
                    if not dedicated:   # shared bucket: carry the other tenants over
                        with phases("copy_other_tenants"):
                            c.execute(f"INSERT INTO {new} ({COLS}) SELECT {COLS} FROM {old} WHERE tenant_id <> %s", [t])
                    # created in this transaction => rows can be written frozen (no later hint-bit / freeze pass)
                    copy_chunks(c, new, csv_chunks([ds.replace_tenant_file]), freeze=True)

            def build(sql: str):
                with self.connect() as ci:
                    self._session(ci, ["SET maintenance_work_mem = '1GB'", "SET max_parallel_maintenance_workers = 1"])
                    ci.execute(sql)

            with phases("index"):
                run_parallel(2, build, [index_a_sql(new), index_b_sql(new)])
            with phases("analyze"):
                c.execute(f"ANALYZE {new}")
            with phases("swap"):
                # ACCESS EXCLUSIVE on the parent for the duration of three catalog updates; readers
                # queue behind it and then see the new partition - never a missing tenant.
                msgs: list[str] = []
                c.add_notice_handler(lambda d: msgs.append(d.message_primary or ""))
                with c.transaction():
                    c.execute("SET LOCAL lock_timeout = '30s'")
                    c.execute("SET LOCAL client_min_messages = debug1")   # to record that no scan happened
                    c.execute(f"ALTER TABLE big_table DETACH PARTITION {old}")
                    c.execute(f"ALTER TABLE big_table ATTACH PARTITION {new} {bound}")
                    c.execute(f"DROP TABLE {old}")
                    c.execute(f"ALTER TABLE {new} DROP CONSTRAINT {new}_pck")
            phases.note("partition", {"old": old, "new": new, "bound": bound[:80], "dedicated": dedicated,
                                      "attach_validation_scan_skipped":
                                          any("is implied by existing constraints" in m for m in msgs)})
            c.execute("SELECT pg_advisory_unlock(%s)", [MAINT_LOCK])
