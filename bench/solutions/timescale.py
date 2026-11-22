"""TimescaleDB solutions (image timescale/timescaledb:latest-pg17).

    ts_hypertable    row-store hypertable, parallel COPY, indexes after load, rename swap
    ts_columnstore   hypertable converted to the columnstore (compression), see class docstring
    ts_plain_control plain PostgreSQL table in the *same image* with the *same loader*
                     (isolates what the hypertable itself costs / buys)

Design notes, measurements and limitations: docs/notes/timescaledb.md.
"""
from __future__ import annotations

import io
import json
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import psycopg
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .. import dockerutil as du
from .. import spec
from . import register
from .base import Client, Dataset, Phases
from .pg_common import PG_BASE_CONF, PG_QUERIES, PgClient, PostgresBase, create_table_sql, rename_swap

# What TimescaleDB needs on top of PG_BASE_CONF. Nothing else differs from the
# vanilla PostgreSQL solutions (timescaledb-tune is disabled with NO_TS_TUNE).
TS_REQUIRED_CONF = {
    "shared_preload_libraries": "timescaledb",
    "timescaledb.telemetry_level": "off",
    # launcher + scheduler + at most one job must fit into max_worker_processes=8
    # next to the 4 parallel workers of PG_BASE_CONF
    "timescaledb.max_background_workers": "3",
}

COLS = ", ".join(spec.COLUMN_NAMES)


def _dev_opts() -> dict:
    """Development-only overrides: B2D_TS_OPTS='{"chunk_interval": 5}'."""
    raw = os.environ.get("B2D_TS_OPTS")
    return json.loads(raw) if raw else {}


# --------------------------------------------------------------------------- parallel COPY
def _row_group_units(files: Iterable[Path]) -> list[tuple[Path, int]]:
    units = []
    for f in files:
        md = pq.ParquetFile(f).metadata
        units += [(f, rg) for rg in range(md.num_row_groups)]
    return units


def parallel_copy(dsn: str, table: str, files: Iterable[Path], streams: int,
                  session_sql: Sequence[str] = (), batch_rows: int = 50_000,
                  copy_options: str = "FORMAT csv") -> int:
    """The Python equivalent of `timescaledb-parallel-copy`: `streams` connections,
    each running one COPY ... FROM STDIN, fed from a shared queue of Parquet row
    groups (decode + CSV encode happen in the worker threads, inside the clock).
    Each stream is one transaction. Returns the number of rows sent."""
    units: queue.SimpleQueue = queue.SimpleQueue()
    n_units = 0
    for u in _row_group_units(files):
        units.put(u)
        n_units += 1
    streams = max(1, min(streams, n_units))
    opts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
    sent = [0] * streams
    failed = threading.Event()

    def worker(k: int):
        open_files: dict[Path, pq.ParquetFile] = {}
        with psycopg.connect(dsn, autocommit=True) as conn:
            for s in session_sql:
                conn.execute(s)
            with conn.cursor() as cur, cur.copy(f"COPY {table} ({COLS}) FROM STDIN ({copy_options})") as cp:
                while not failed.is_set():
                    try:
                        f, rg = units.get_nowait()
                    except queue.Empty:
                        break
                    pf = open_files.get(f)
                    if pf is None:
                        pf = open_files[f] = pq.ParquetFile(f)
                    tbl = pf.read_row_group(rg, columns=spec.COLUMN_NAMES)
                    for batch in tbl.to_batches(max_chunksize=batch_rows):
                        buf = io.BytesIO()
                        pacsv.write_csv(batch, buf, write_options=opts)
                        cp.write(buf.getvalue())
                    sent[k] += tbl.num_rows

    def guarded(k: int):
        try:
            worker(k)
        except BaseException:
            failed.set()
            raise

    with ThreadPoolExecutor(streams) as ex:
        for fut in [ex.submit(guarded, k) for k in range(streams)]:
            fut.result()
    return sum(sent)


def run_parallel(dsn: str, jobs: Sequence[Sequence[str]], workers: int, session_sql: Sequence[str] = ()) -> None:
    """Run `jobs` (each a list of SQL statements, executed in order in autocommit
    mode) on up to `workers` connections."""
    jq: queue.SimpleQueue = queue.SimpleQueue()
    for j in jobs:
        jq.put(j)

    def worker():
        with psycopg.connect(dsn, autocommit=True) as conn:
            for s in session_sql:
                conn.execute(s)
            while True:
                try:
                    job = jq.get_nowait()
                except queue.Empty:
                    return
                for stmt in job:
                    conn.execute(stmt)

    n = max(1, min(workers, len(jobs)))
    with ThreadPoolExecutor(n) as ex:
        for fut in [ex.submit(worker) for _ in range(n)]:
            fut.result()


# --------------------------------------------------------------------------- shared base
class TimescaleBase(PostgresBase):
    engine = "timescaledb"
    image = "timescale/timescaledb:latest-pg17"
    port = 55433
    #: parallel COPY streams (client threads + server backends share the 4 vCPUs)
    streams = 4
    #: text keys use COLLATE "C" explicitly, so index build cost does not depend on
    #: the libc of the image (the image is Alpine/musl, where en_US.utf8 already
    #: sorts bytewise; vanilla postgres:17 is Debian/glibc)
    collate_c = True

    def conf(self) -> dict[str, str]:
        return {**PG_BASE_CONF, **TS_REQUIRED_CONF, **self.conf_overrides}

    def setup(self) -> None:
        d = du.data_dir(self.name)
        du.run(self.container, self.image, mounts={str(d): self.pgdata_in_container},
               env={"POSTGRES_PASSWORD": "bench", "NO_TS_TUNE": "true", "TIMESCALEDB_TELEMETRY": "off"},
               args=self.server_args())

        def ready():
            with psycopg.connect(self.dsn, connect_timeout=2) as c:
                return c.execute("SELECT 1").fetchone()[0] == 1

        du.wait_until(ready, timeout=120, what=self.container)
        time.sleep(2)   # the image restarts postgres once after initdb
        du.wait_until(ready, timeout=60, what=self.container)
        self.post_setup()

    def post_setup(self) -> None:
        with self.connect() as c:
            c.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    def info(self) -> dict:
        out = super().info()
        with self.connect() as c:
            out["timescaledb_version"] = c.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").fetchone()[0]
            out["timescaledb_license"] = c.execute("SHOW timescaledb.license").fetchone()[0]
            out["server_version_full"] = c.execute("SELECT version()").fetchone()[0]
            # everything that differs from the built-in defaults, as the server sees it
            out["effective_non_default_settings"] = dict(c.execute(
                "SELECT name, current_setting(name) FROM pg_settings "
                "WHERE source NOT IN ('default', 'override', 'client', 'session') ORDER BY name").fetchall())
        out["loader"] = {"streams": self.streams, "format": "csv (pyarrow) over COPY FROM STDIN",
                         "unit": "parquet row group"}
        return out

    # ---- helpers ------------------------------------------------------------
    def new_name(self, conn: psycopg.Connection) -> str:
        conn.execute("DROP TABLE IF EXISTS big_table_new")
        return "big_table_new"

    def swap_in(self, conn: psycopg.Connection, new: str) -> None:
        """Rename swap, old hypertable dropped after the commit (see pg_common.rename_swap).
        The first version of this benchmark dropped the old hypertable inside the swap
        transaction and deadlocked with a concurrent reader at 5M rows."""
        rename_swap(conn, new)


def index_a_sql(table: str) -> str:
    return f"CREATE UNIQUE INDEX {table}_a ON {table} (tenant_id, item_key, period)"


def index_b_sql(table: str) -> str:
    return f"CREATE INDEX {table}_b ON {table} (ref_path, period)"


# --------------------------------------------------------------------------- control
@register
class TsPlainControl(TimescaleBase):
    """Control: a plain (non-hyper) table in the TimescaleDB image, loaded with
    exactly the loader of ts_hypertable. Not a TimescaleDB feature; it exists so
    that 'hypertable vs plain table' can be compared with everything else equal
    (same binary, same libc, same client code)."""

    name = "ts_plain_control"
    strategy = "CONTROL: plain PG table in the TimescaleDB image; 4 parallel COPY streams, indexes after load, rename swap"
    atomic_swap = True

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        with self.connect() as c:
            new = self.new_name(c)
            c.execute(create_table_sql(new, collate_c=self.collate_c))
            with phases("copy"):
                parallel_copy(self.dsn, new, ds.base_files, self.streams)
            with phases("index"):
                run_parallel(self.dsn, [[index_a_sql(new)], [index_b_sql(new)]], 2)
            with phases("analyze"):
                c.execute(f"ANALYZE {new}")
            with phases("swap"):
                self.swap_in(c, new)


# --------------------------------------------------------------------------- row-store hypertable
@register
class TsHypertable(TimescaleBase):
    """Row-store hypertable partitioned on `period`."""

    name = "ts_hypertable"
    strategy = ("hypertable by_range(period), 4 parallel COPY streams into a new hypertable, "
                "indexes after load, rename swap")
    atomic_swap = True

    #: by_range(period, N). Chunks are aligned to multiples of N; periods are 1001..1020
    #: and 1001 = 7 * 143, so N=7 gives exactly 3 chunks (7 + 7 + 6 periods).
    chunk_interval = 7
    #: by_hash(tenant_id, N) space dimension; 0 = none
    space_partitions = 0
    #: "after" (CREATE INDEX on the loaded hypertable) or "before" (maintained during COPY)
    index_timing = "after"
    #: run CREATE INDEX a and b concurrently
    index_parallel = True

    def __init__(self):
        for k, v in _dev_opts().items():
            setattr(self, k, v)

    def info(self) -> dict:
        out = super().info()
        out["hypertable"] = {"partitioning": f"by_range(period, {self.chunk_interval})",
                             "space_partitions": self.space_partitions,
                             "index_timing": self.index_timing, "index_parallel": self.index_parallel}
        return out

    def create_hypertable(self, c: psycopg.Connection, name: str) -> None:
        c.execute(create_table_sql(name, collate_c=self.collate_c))
        c.execute(f"SELECT create_hypertable('{name}', by_range('period', {int(self.chunk_interval)}), "
                  f"create_default_indexes => false)")
        if self.space_partitions:
            c.execute(f"SELECT add_dimension('{name}', by_hash('tenant_id', {int(self.space_partitions)}))")

    def build_indexes(self, c: psycopg.Connection, name: str) -> None:
        if self.index_parallel:
            run_parallel(self.dsn, [[index_a_sql(name)], [index_b_sql(name)]], 2)
        else:
            c.execute(index_a_sql(name))
            c.execute(index_b_sql(name))

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        with self.connect() as c:
            new = self.new_name(c)
            self.create_hypertable(c, new)
            if self.index_timing == "before":
                self.build_indexes(c, new)
            with phases("copy"):
                parallel_copy(self.dsn, new, ds.base_files, self.streams)
            if self.index_timing == "after":
                with phases("index"):
                    self.build_indexes(c, new)
            with phases("analyze"):
                c.execute(f"ANALYZE {new}")
            with phases("swap"):
                self.swap_in(c, new)
            phases.note("chunks", c.execute("SELECT count(*) FROM show_chunks('big_table')").fetchone()[0])

    def storage(self) -> dict:
        with self.connect() as c:
            table, index, toast, total = c.execute(
                "SELECT table_bytes, index_bytes, toast_bytes, total_bytes FROM hypertable_detailed_size('big_table')"
            ).fetchone()
            a = c.execute("SELECT hypertable_index_size('big_table_a')").fetchone()[0]
            b = c.execute("SELECT hypertable_index_size('big_table_b')").fetchone()[0]
            db = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
            chunks = c.execute("SELECT count(*) FROM show_chunks('big_table')").fetchone()[0]
        return {"table": int(table) + int(toast or 0), "index_a": int(a), "index_b": int(b), "total": int(total),
                "database": db, "chunks": chunks}


# --------------------------------------------------------------------------- columnstore
@register
class TsColumnstore(TsHypertable):
    """Hypertable converted to the TimescaleDB columnstore (compression).

    Layout: segmentby tenant_id (every tenant's rows are stored together, the natural
    filter of a multi-tenant app), orderby (item_key, period) so the business-key lookup
    hits one ~1000-row batch through the min/max metadata, and a bloom-filter sparse index
    on ref_path for the secondary access path (there is no B-tree over compressed rows).

    Full refresh: COPY into the row-store chunks of a new hypertable, convert every chunk
    (one connection per chunk), then create the two B-tree indexes. They are created
    AFTER the conversion on purpose: they then only cover rows that arrive later
    (upserts land in the row-store part of a chunk), so building them is almost free, and
    index A is what makes INSERT ... ON CONFLICT possible against compressed chunks.

    Maintenance: plain SQL. An upsert decompresses the batches that contain conflicting
    keys; the changed rows stay in the row store until the chunk is converted again
    (in production: the columnstore policy job, not measured here)."""

    name = "ts_columnstore"
    port = 55435
    strategy = ("hypertable + columnstore: segmentby tenant_id, orderby (item_key, period), bloom sparse index on "
                "ref_path; 4 parallel COPY streams, convert chunks in parallel, rename swap")
    atomic_swap = True
    chunk_interval = 5          # 4 chunks -> 4 conversions in parallel on 4 vCPUs
    #: "post" = COPY to row store, then compress_chunk();  "direct" = COPY straight into the columnstore
    load_mode = "post"

    COLUMNSTORE = ("timescaledb.enable_columnstore, timescaledb.segmentby = 'tenant_id', "
                   "timescaledb.orderby = 'item_key, period', "
                   "timescaledb.sparse_index = 'bloom(ref_path), minmax(item_key), minmax(period)'")

    def info(self) -> dict:
        out = super().info()
        out["columnstore"] = {"settings": self.COLUMNSTORE, "load_mode": self.load_mode}
        return out

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        with self.connect() as c:
            new = self.new_name(c)
            self.create_hypertable(c, new)
            c.execute(f"ALTER TABLE {new} SET ({self.COLUMNSTORE})")
            if self.load_mode == "direct":
                with phases("copy_direct_columnstore"):
                    parallel_copy(self.dsn, new, ds.base_files, self.streams,
                                  session_sql=["SET timescaledb.enable_direct_compress_copy = on"])
            else:
                with phases("copy"):
                    parallel_copy(self.dsn, new, ds.base_files, self.streams)
                with phases("convert_to_columnstore"):
                    chunks = [r[0] for r in c.execute(f"SELECT show_chunks('{new}')::text").fetchall()]
                    run_parallel(self.dsn, [[f"SELECT compress_chunk('{ch}')"] for ch in chunks], self.streams)
            with phases("index"):
                c.execute(index_a_sql(new))
                c.execute(index_b_sql(new))
            with phases("analyze"):
                c.execute(f"ANALYZE {new}")
            with phases("swap"):
                self.swap_in(c, new)

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        sets = ", ".join(f"{col} = EXCLUDED.{col}" for col in spec.COLUMN_NAMES if col not in spec.KEY)
        with self.connect(autocommit=False) as c:
            c.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
            with phases("stage_copy"):
                c.execute(create_table_sql("delta_stage", collate_c=self.collate_c).replace(
                    "CREATE TABLE", "CREATE TEMP TABLE") + " ON COMMIT DROP")
                with c.cursor() as cur, cur.copy(f"COPY delta_stage ({COLS}) FROM STDIN (FORMAT csv)") as cp:
                    opts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
                    for batch in pq.ParquetFile(delta).iter_batches(batch_size=50_000):
                        buf = io.BytesIO()
                        pacsv.write_csv(batch, buf, write_options=opts)
                        cp.write(buf.getvalue())
            with phases("merge"):
                c.execute(f"INSERT INTO big_table SELECT * FROM delta_stage "
                          f"ON CONFLICT (tenant_id, item_key, period) DO UPDATE SET {sets}")
            with phases("commit"):
                c.commit()

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        with self.connect(autocommit=False) as c:
            c.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
            with phases("delete"):
                c.execute("DELETE FROM big_table WHERE tenant_id = %s", [ds.replace_tenant_id])
            with phases("copy"):
                with c.cursor() as cur, cur.copy(f"COPY big_table ({COLS}) FROM STDIN (FORMAT csv)") as cp:
                    opts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
                    for batch in pq.ParquetFile(ds.replace_tenant_file).iter_batches(batch_size=50_000):
                        buf = io.BytesIO()
                        pacsv.write_csv(batch, buf, write_options=opts)
                        cp.write(buf.getvalue())
            with phases("commit"):
                c.commit()
