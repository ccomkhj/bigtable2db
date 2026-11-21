"""PostgreSQL full-refresh strategies, from "what pipelines do today" to the
fastest method that is still crash-safe and replicable, plus the no-WAL variants.

    pg_naive_insert       batched multi-row INSERTs into the indexed live table
    pg_copy_swap          new table, ONE COPY stream, indexes afterwards, atomic rename
    pg_parallel_swap      same, with parallel COPY streams + tuned sessions; managed-service safe
    pg_unlogged_swap      same pipeline into an UNLOGGED table (no WAL, not crash-safe)
    pg_unlogged_setlogged pg_unlogged_swap + ALTER TABLE ... SET LOGGED before the swap
    pg_walminimal_swap    self-hosted only: wal_level=minimal, CREATE TABLE + COPY in one transaction

All of them share one loader (`load_new_table`) driven by a `LoadOptions`
record, so every lever can be measured in isolation:

    python -m bench.solutions.pg_strategies --scale 1M --suite all

runs the lever lab (see `SUITES`) and prints one line per variant.
Design notes and the measured lever table: docs/notes/postgres.md
"""
from __future__ import annotations

import argparse
import json
import queue
import statistics
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Iterable, Iterator

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .. import spec
from . import register
from .base import Dataset, Phases
from .pg_common import PostgresBase, copy_parquet, create_table_sql, index_a_sql, index_b_sql

STAGE = "big_table_new"
COLS = ", ".join(spec.COLUMN_NAMES)
PG_TYPES = ["int8", "int4", "int4", "float8", "float8", "float8", "text", "text", "text"]


# --------------------------------------------------------------------------- options
@dataclass(frozen=True)
class LoadOptions:
    """Everything that distinguishes one bulk-load variant from another."""

    #: number of concurrent COPY streams (= client connections = server backends)
    streams: int = 1
    #: csv = pyarrow CSV writer -> COPY (FORMAT csv)
    #: csv_ref = the reference encoder in pg_common.copy_parquet (single stream only)
    #: pgbinary = psycopg row-by-row binary COPY;  adbc = ADBC adbc_ingest (binary COPY built in C)
    encoder: str = "csv"
    #: threads | processes (who runs the client-side Parquet decode + encode)
    workers: str = "threads"
    #: rows per CSV chunk handed to libpq
    batch_rows: int = 50_000
    #: let pyarrow use its own thread pool inside each stream when decoding Parquet
    arrow_threads: bool = False
    #: CREATE UNLOGGED TABLE (no WAL for heap or indexes; truncated after a crash; absent on replicas)
    unlogged: bool = False
    #: CREATE TABLE + COPY in ONE transaction over ONE connection, fed by `streams` encoder threads.
    #: Required for COPY FREEZE and for the wal_level=minimal WAL skip.
    single_txn: bool = False
    #: COPY ... (FREEZE): rows are written frozen + all-visible (needs single_txn)
    freeze: bool = False
    #: SET synchronous_commit = off in the loading sessions
    sync_commit_off: bool = False
    #: session maintenance_work_mem for each CREATE INDEX (None = server setting)
    maintenance_work_mem: str | None = None
    #: session max_parallel_maintenance_workers for each CREATE INDEX (None = server setting)
    index_workers: int | None = None
    #: build index A and index B at the same time on two connections
    index_concurrent: bool = False
    #: B-tree fillfactor (None = default 90)
    index_fillfactor: int | None = None
    #: run ANALYZE on a third connection while... (not possible: ANALYZE conflicts with CREATE INDEX's SHARE lock)
    #: ALTER TABLE ... SET LOGGED after the build (only meaningful with unlogged=True)
    set_logged: bool = False


def _apply(conn: psycopg.Connection, settings: dict[str, str]) -> None:
    for k, v in settings.items():
        conn.execute("SELECT set_config(%s, %s, false)", [k, str(v)])


def copy_session(o: LoadOptions) -> dict[str, str]:
    return {"synchronous_commit": "off"} if o.sync_commit_off else {}


def index_session(o: LoadOptions) -> dict[str, str]:
    s = copy_session(o)
    if o.maintenance_work_mem is not None:
        s["maintenance_work_mem"] = o.maintenance_work_mem
    if o.index_workers is not None:
        s["max_parallel_maintenance_workers"] = str(o.index_workers)
    return s


# --------------------------------------------------------------------------- client-side encoders
def parquet_units(files: Iterable[Path]) -> list[tuple[str, int]]:
    """(file, row_group) work units; streams pull them from a shared queue so they stay balanced."""
    out = []
    for f in files:
        n = pq.read_metadata(f).num_row_groups
        out += [(str(f), rg) for rg in range(n)]
    return out


class _Shared:
    """Thread-safe iterator over the work units."""

    def __init__(self, items):
        self._it = iter(items)
        self._lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self._lock:
            return next(self._it)


def _tables(units: Iterable[tuple[str, int]], arrow_threads: bool) -> Iterator[pa.Table]:
    handles: dict[str, pq.ParquetFile] = {}
    for path, rg in units:
        pf = handles.get(path)
        if pf is None:
            pf = handles[path] = pq.ParquetFile(path)
        yield pf.read_row_group(rg, columns=spec.COLUMN_NAMES, use_threads=arrow_threads)


def _csv_chunks(units, batch_rows: int, arrow_threads: bool) -> Iterator[pa.Buffer]:
    """Parquet row groups -> CSV buffers. Decode and encode both run in Arrow C++ without the GIL."""
    wopts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
    for tbl in _tables(units, arrow_threads):
        for batch in tbl.to_batches(max_chunksize=batch_rows):
            sink = pa.BufferOutputStream()
            pacsv.write_csv(batch, sink, write_options=wopts)
            yield sink.getvalue()


def _stream(dsn: str, table: str, units, o: LoadOptions) -> int:
    """One COPY stream on its own connection. Returns rows sent."""
    if o.encoder == "adbc":
        return _stream_adbc(dsn, table, units, o)
    rows = 0
    with psycopg.connect(dsn, autocommit=True) as conn:
        _apply(conn, copy_session(o))
        with conn.cursor() as cur:
            if o.encoder == "csv":
                with cur.copy(f"COPY {table} ({COLS}) FROM STDIN (FORMAT csv)") as cp:
                    for chunk in _csv_chunks(units, o.batch_rows, o.arrow_threads):
                        cp.write(memoryview(chunk))
            elif o.encoder == "pgbinary":
                with cur.copy(f"COPY {table} ({COLS}) FROM STDIN (FORMAT binary)") as cp:
                    cp.set_types(PG_TYPES)
                    for tbl in _tables(units, o.arrow_threads):
                        for row in zip(*[c.to_pylist() for c in tbl.columns]):
                            cp.write_row(row)
            else:
                raise ValueError(o.encoder)
            rows = cur.rowcount
    return rows


def _uri(dsn: str) -> str:
    kv = dict(p.split("=", 1) for p in dsn.split())
    return f"postgresql://{kv['user']}:{kv['password']}@{kv['host']}:{kv['port']}/{kv['dbname']}"


def _stream_adbc(dsn: str, table: str, units, o: LoadOptions) -> int:
    import adbc_driver_postgresql.dbapi as adbc

    units = iter(units)
    first = next(_tables(units, o.arrow_threads), None)
    if first is None:
        return 0

    def batches():
        yield from first.to_batches(max_chunksize=o.batch_rows)
        for tbl in _tables(units, o.arrow_threads):
            yield from tbl.to_batches(max_chunksize=o.batch_rows)

    reader = pa.RecordBatchReader.from_batches(first.schema, batches())
    with adbc.connect(_uri(dsn)) as conn, conn.cursor() as cur:
        for k, v in copy_session(o).items():
            cur.execute(f"SET {k} = '{v}'")
        n = cur.adbc_ingest(table, reader, mode="append")
        conn.commit()
    return n


def _stream_proc(args) -> int:
    dsn, table, units, o = args
    return _stream(dsn, table, units, o)


def copy_parallel(dsn: str, table: str, files: Iterable[Path], o: LoadOptions) -> None:
    """`o.streams` independent COPY commands into the same table, each on its own connection."""
    units = parquet_units(files)
    n = max(1, o.streams)
    if o.workers == "processes" and n > 1:
        import multiprocessing as mp
        parts = [units[i::n] for i in range(n)]
        with ProcessPoolExecutor(n, mp_context=mp.get_context("spawn")) as ex:
            list(ex.map(_stream_proc, [(dsn, table, p, o) for p in parts]))
        return
    shared = _Shared(units)
    if n == 1:
        _stream(dsn, table, shared, o)
        return
    with ThreadPoolExecutor(n) as ex:
        futs = [ex.submit(_stream, dsn, table, shared, o) for _ in range(n)]
        for f in futs:
            f.result()


def copy_fan_in(conn: psycopg.Connection, table: str, files: Iterable[Path], o: LoadOptions) -> None:
    """ONE COPY stream (one server backend) fed by `o.streams` client-side encoder threads.
    Used where the COPY has to run in the transaction that created the table."""
    shared = _Shared(parquet_units(files))
    n = max(1, o.streams)
    q: queue.Queue = queue.Queue(maxsize=2 * n)
    done = object()

    def encode():
        try:
            for chunk in _csv_chunks(shared, o.batch_rows, o.arrow_threads):
                q.put(chunk)
            q.put(done)
        except BaseException as e:  # noqa: BLE001
            q.put(e)

    threads = [threading.Thread(target=encode, daemon=True) for _ in range(n)]
    for t in threads:
        t.start()
    opt = "FORMAT csv, FREEZE" if o.freeze else "FORMAT csv"
    live = n
    with conn.cursor() as cur, cur.copy(f"COPY {table} ({COLS}) FROM STDIN ({opt})") as cp:
        while live:
            item = q.get()
            if item is done:
                live -= 1
            elif isinstance(item, BaseException):
                raise item
            else:
                cp.write(memoryview(item))


# --------------------------------------------------------------------------- index builds
def build_indexes(dsn: str, table: str, o: LoadOptions, phases: Phases) -> None:
    ff = f" WITH (fillfactor = {o.index_fillfactor})" if o.index_fillfactor else ""
    sess = index_session(o)

    def one(which: str):
        sql = (index_a_sql if which == "a" else index_b_sql)(table) + ff
        with psycopg.connect(dsn, autocommit=True) as c:
            _apply(c, sess)
            with phases(f"index_{which}"):
                c.execute(sql)

    if o.index_concurrent:
        with ThreadPoolExecutor(2) as ex:
            futs = [ex.submit(one, w) for w in ("a", "b")]
            for f in futs:
                f.result()
    else:
        one("a")
        one("b")


# --------------------------------------------------------------------------- the loader
def load_new_table(dsn: str, files: list[Path], o: LoadOptions, phases: Phases, *, collate_c: bool,
                   table: str = STAGE) -> None:
    """Build `table` from the Parquet files: data, index A (unique), index B, statistics."""
    ddl = create_table_sql(table, unlogged=o.unlogged, collate_c=collate_c)
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(f"DROP TABLE IF EXISTS {table}")   # leftover of a failed run
        if not o.single_txn:
            c.execute(ddl)

    if o.single_txn:
        with psycopg.connect(dsn, autocommit=False) as c:
            _apply(c, copy_session(o))
            with phases("copy"):
                c.execute(ddl)
                copy_fan_in(c, table, files, o)
            with phases("copy_commit"):
                c.commit()
    elif o.encoder == "csv_ref":
        with psycopg.connect(dsn, autocommit=True) as c, phases("copy"):
            _apply(c, copy_session(o))
            copy_parquet(c, table, files)
    else:
        with phases("copy"):
            copy_parallel(dsn, table, files, o)

    with phases("index"):
        build_indexes(dsn, table, o, phases)
    with psycopg.connect(dsn, autocommit=True) as c:
        with phases("analyze"):
            c.execute(f"ANALYZE {table}")
        if o.set_logged:
            _apply(c, index_session(o))
            with phases("set_logged"):
                c.execute(f"ALTER TABLE {table} SET LOGGED")


class _SwapSolution(PostgresBase):
    """Load a new table next to the live one, then rename it into place."""

    atomic_swap = True
    opts = LoadOptions()

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        load_new_table(self.dsn, ds.base_files, self.opts, phases, collate_c=self.collate_c)
        with self.connect() as c, phases("swap"):
            self.swap_in(c, STAGE)

    def info(self) -> dict:
        d = super().info()
        d["load_options"] = asdict(self.opts)
        d["session_settings"] = {"copy": copy_session(self.opts), "index": index_session(self.opts)}
        d["conf_overrides"] = dict(self.conf_overrides)
        return d


# --------------------------------------------------------------------------- solutions
@register
class PgNaiveInsert(PostgresBase):
    """Baseline A: what `DataFrame.to_sql(method="multi", chunksize=5000)` or an ORM bulk insert sends:
    one transaction, TRUNCATE, then multi-row `INSERT ... VALUES (...), (...), ...` statements with
    bound parameters into the table that already carries both indexes."""

    name = "pg_naive_insert"
    strategy = "TRUNCATE + batched multi-row INSERT (5,000 rows/statement) into the indexed live table"
    atomic_swap = False
    batch_rows = 5_000     # 9 columns x 5,000 rows = 45,000 bind parameters (protocol limit 65,535)

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        with self.connect(autocommit=False) as c:
            if not self.table_exists(c):
                c.execute(create_table_sql("big_table"))
                c.execute(index_a_sql("big_table"))
                c.execute(index_b_sql("big_table"))
                c.commit()
            with phases("truncate"):
                c.execute("TRUNCATE big_table")
            with phases("insert_with_indexes"):
                naive_insert(c, "big_table", ds.base_files, self.batch_rows)
            with phases("commit"):
                c.commit()
        with self.connect() as c, phases("analyze"):
            c.execute("ANALYZE big_table")

    def info(self) -> dict:
        d = super().info()
        d["insert"] = {"rows_per_statement": self.batch_rows, "driver": f"psycopg {psycopg.__version__}",
                       "statement": "INSERT ... VALUES (%s, ...), ... with bound parameters, one transaction"}
        return d


def naive_insert(conn: psycopg.Connection, table: str, files: Iterable[Path], batch_rows: int) -> None:
    ncol = len(spec.COLUMN_NAMES)
    row_ph = "(" + ", ".join(["%s"] * ncol) + ")"

    def stmt(n: int) -> str:
        return f"INSERT INTO {table} ({COLS}) VALUES " + ", ".join([row_ph] * n)

    full = stmt(batch_rows)
    with conn.cursor() as cur:
        for f in files:
            for batch in pq.ParquetFile(f).iter_batches(batch_size=batch_rows, columns=spec.COLUMN_NAMES):
                cols = [c.to_pylist() for c in batch.columns]
                params = [v for row in zip(*cols) for v in row]
                n = batch.num_rows
                cur.execute(full if n == batch_rows else stmt(n), params)


@register
class PgCopySwap(_SwapSolution):
    """Step 1 of the bulk-load pattern, nothing else changed: the same single COPY stream and the
    same server configuration as pg_truncate_copy, but into a NEW table without indexes; both
    indexes are built afterwards (one sort each instead of 2 x N random B-tree inserts), ANALYZE,
    then an atomic rename. Readers keep using the old table until the rename."""

    name = "pg_copy_swap"
    strategy = "new table: single COPY, then CREATE INDEX x2, ANALYZE, atomic rename"
    opts = LoadOptions(streams=1, encoder="csv_ref")


@register
class PgParallelSwap(_SwapSolution):
    """The fastest crash-safe, WAL-logged, replicable variant. Uses only what a managed service
    allows: N client connections running COPY FROM STDIN, session-level settings, plain DDL, and
    the load runs as an ordinary non-superuser role that owns the table."""

    name = "pg_parallel_swap"
    strategy = ("new table: 4 parallel COPY streams, both indexes built concurrently, C collation keys, "
                "atomic rename (WAL-logged, managed-service safe)")
    collate_c = True
    opts = LoadOptions(streams=4, encoder="csv", sync_commit_off=True, index_concurrent=True,
                       index_workers=1, maintenance_work_mem="1GB")
    loader_role = "loader"
    _role_ready = False

    @property
    def dsn(self) -> str:
        user = self.loader_role if self._role_ready else "postgres"
        return f"host=127.0.0.1 port={self.port} user={user} password=bench dbname=postgres"

    def post_setup(self) -> None:
        # prove the "no superuser" claim: everything after this line runs as a plain role
        with psycopg.connect(self.dsn, autocommit=True) as c:
            c.execute(f"CREATE ROLE {self.loader_role} LOGIN PASSWORD 'bench'")
            c.execute(f"GRANT CREATE, USAGE ON SCHEMA public TO {self.loader_role}")
        self._role_ready = True

    def info(self) -> dict:
        d = super().info()
        with self.connect() as c:
            d["load_role"] = c.execute(
                "SELECT rolname || CASE WHEN rolsuper THEN ' (superuser)' ELSE ' (not superuser)' END "
                "FROM pg_roles WHERE rolname = current_user").fetchone()[0]
        return d


@register
class PgUnloggedSwap(_SwapSolution):
    """pg_parallel_swap with CREATE UNLOGGED TABLE: neither heap nor index pages are WAL-logged.
    The live table stays UNLOGGED: it is emptied by crash recovery and does not exist on physical
    replicas. Shown to quantify what WAL costs; see pg_unlogged_setlogged for the way back."""

    name = "pg_unlogged_swap"
    strategy = "UNLOGGED new table: 4 parallel COPY streams, concurrent index builds, atomic rename (no WAL, not crash-safe)"
    collate_c = True
    opts = replace(PgParallelSwap.opts, unlogged=True)


@register
class PgUnloggedSetLogged(_SwapSolution):
    """pg_unlogged_swap followed by ALTER TABLE ... SET LOGGED before the rename, i.e. the
    'load unlogged, then make it durable' idea. SET LOGGED rewrites the heap and rebuilds both
    indexes through WAL; the `set_logged` phase is its cost."""

    name = "pg_unlogged_setlogged"
    strategy = "pg_unlogged_swap + ALTER TABLE SET LOGGED before the rename (durable again; shows what SET LOGGED costs)"
    collate_c = True
    opts = replace(PgParallelSwap.opts, unlogged=True, set_logged=True)


@register
class PgWalMinimalSwap(_SwapSolution):
    """Self-hosted only: wal_level=minimal (no physical replicas, no PITR/WAL archiving). A table
    created and COPY-loaded in the same transaction is not WAL-logged but fsynced at commit, and
    index builds skip WAL as well, so the result is crash-safe without paying for WAL. The catch:
    the COPY must run in the creating transaction, i.e. over ONE connection; the client still
    decodes/encodes with 4 threads and funnels into that single stream."""

    name = "pg_walminimal_swap"
    strategy = ("wal_level=minimal: CREATE TABLE + one COPY FREEZE stream (4 encoder threads) in one transaction, "
                "concurrent index builds, atomic rename (crash-safe, no replication/PITR)")
    collate_c = True
    conf_overrides = {"wal_level": "minimal", "max_wal_senders": "0"}
    opts = replace(PgParallelSwap.opts, single_txn=True, freeze=True)


# --------------------------------------------------------------------------- lab-only loaders
_DUCKDB_SCRIPT = r"""
import sys, glob, os, duckdb
dsn, table, threads, files = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4:]
con = duckdb.connect()
con.execute(f"SET threads={threads}")
try:
    con.execute("LOAD postgres")
except Exception:
    import duckdb_extension_postgres_scanner as m          # PyPI repackaging of the signed extension
    ext = glob.glob(os.path.join(os.path.dirname(m.__file__), "extensions", "*", "*.duckdb_extension"))[0]
    con.execute(f"INSTALL '{ext}'")
    con.execute("LOAD postgres_scanner")
con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres)")
lst = ", ".join("'" + f + "'" for f in files)
con.execute(f"INSERT INTO pg.public.{table} SELECT * FROM read_parquet([{lst}])")
"""


def copy_duckdb(dsn: str, table: str, files: Iterable[Path], threads: int) -> None:
    """DuckDB's postgres extension: `INSERT INTO pg.tbl SELECT * FROM read_parquet(...)` (binary COPY
    under the hood). The extension cannot be downloaded in every environment, so the interpreter
    that has it is configurable: B2D_DUCKDB_PYTHON=/path/to/python."""
    import os
    import subprocess
    import sys
    py = os.environ.get("B2D_DUCKDB_PYTHON", sys.executable)
    subprocess.run([py, "-c", _DUCKDB_SCRIPT, dsn, table, str(threads), *map(str, files)], check=True)


def naive_insert_variant(dsn: str, table: str, files: list[Path], how: str, batch_rows: int = 5_000) -> None:
    """INSERT-based loaders as pipelines really issue them (lab suite `naive`)."""
    if how == "psycopg_multirow":
        with psycopg.connect(dsn, autocommit=False) as c:
            naive_insert(c, table, files, batch_rows)
            c.commit()
    elif how == "psycopg_executemany":
        sql = f"INSERT INTO {table} ({COLS}) VALUES (" + ", ".join(["%s"] * len(spec.COLUMN_NAMES)) + ")"
        with psycopg.connect(dsn, autocommit=False) as c, c.cursor() as cur:
            for f in files:
                for batch in pq.ParquetFile(f).iter_batches(batch_size=batch_rows, columns=spec.COLUMN_NAMES):
                    cur.executemany(sql, list(zip(*[col.to_pylist() for col in batch.columns])))
            c.commit()
    elif how in ("pandas_to_sql_multi", "pandas_to_sql_default"):
        import sqlalchemy as sa
        eng = sa.create_engine(_uri(dsn).replace("postgresql://", "postgresql+psycopg://"))
        kw = {"method": "multi", "chunksize": batch_rows} if how.endswith("multi") else {"chunksize": batch_rows}
        with eng.begin() as conn:
            for f in files:
                for batch in pq.ParquetFile(f).iter_batches(batch_size=100_000, columns=spec.COLUMN_NAMES):
                    batch.to_pandas().to_sql(table, conn, if_exists="append", index=False, **kw)
        eng.dispose()
    else:
        raise ValueError(how)


# --------------------------------------------------------------------------- lever lab
# Not part of the benchmark protocol. Measures one lever at a time against a kept-alive server so
# that docs/notes/postgres.md can say what each lever is worth:
#
#     python -m bench.solutions.pg_strategies --scale 1M --suite streams,encoders --repeat 3
#
# Variants of one suite are run round-robin (A B C A B C ...) so that noise from a shared machine
# hits all of them alike; the table reports min and median.
@dataclass(frozen=True)
class Variant:
    label: str
    opts: LoadOptions
    collate_c: bool = False
    #: reloadable server settings applied with ALTER SYSTEM before the run (reset afterwards)
    conf: tuple[tuple[str, str], ...] = ()


_P = LoadOptions(streams=4, encoder="csv")                      # parallel COPY baseline for the lab
_I = LoadOptions()                                              # index baseline: serial, server defaults


def _suites() -> dict[str, tuple[str, list[Variant]]]:
    """suite -> (mode, variants). mode: copy = COPY phase only, index = index builds on a loaded
    table, full = whole pipeline."""
    V = Variant
    return {
        "streams": ("copy", [V(f"csv x{n} streams", replace(_P, streams=n)) for n in (1, 2, 3, 4, 6, 8)]),
        "client": ("copy", [
            V("x4 threads (baseline)", _P),
            V("x4 processes", replace(_P, workers="processes")),
            V("x4 threads + arrow thread pool", replace(_P, arrow_threads=True)),
            V("x4 threads, 10k-row chunks", replace(_P, batch_rows=10_000)),
            V("x4 threads, 200k-row chunks", replace(_P, batch_rows=200_000)),
            V("x4 encoders -> ONE COPY stream", replace(_P, single_txn=True)),
            V("x4 encoders -> ONE COPY FREEZE stream", replace(_P, single_txn=True, freeze=True)),
        ]),
        "encoders": ("copy", [
            V("x1 pyarrow CSV (reference copy_parquet)", LoadOptions(encoder="csv_ref")),
            V("x1 pyarrow CSV (no-GIL sink)", LoadOptions(encoder="csv")),
            V("x1 psycopg binary write_row", LoadOptions(encoder="pgbinary")),
            V("x1 ADBC adbc_ingest (binary)", LoadOptions(encoder="adbc")),
            V("x4 pyarrow CSV", _P),
            V("x4 psycopg binary write_row", replace(_P, encoder="pgbinary")),
            V("x4 psycopg binary write_row, processes", replace(_P, encoder="pgbinary", workers="processes")),
            V("x4 ADBC adbc_ingest (binary)", replace(_P, encoder="adbc")),
        ]),
        "duckdb": ("copy", [
            V("x4 pyarrow CSV (baseline)", _P),
            V("DuckDB postgres extension, 4 threads", replace(_P, encoder="duckdb")),
            V("DuckDB postgres extension, 1 thread", replace(_P, encoder="duckdb", streams=1)),
        ]),
        "naive": ("naive", [
            V("COPY x1 into indexed table (pg_truncate_copy)", LoadOptions(encoder="csv_ref")),
            V("psycopg multi-row INSERT, 5k rows/stmt (pg_naive_insert)", LoadOptions(encoder="psycopg_multirow")),
            V("psycopg executemany (pipeline mode), 5k rows/batch", LoadOptions(encoder="psycopg_executemany")),
            V("pandas to_sql(method='multi', chunksize=5000)", LoadOptions(encoder="pandas_to_sql_multi")),
            V("pandas to_sql(chunksize=5000) (SQLAlchemy insertmanyvalues)", LoadOptions(encoder="pandas_to_sql_default")),
        ]),
        "session": ("copy", [
            V("x4 synchronous_commit=on", _P),
            V("x4 synchronous_commit=off", replace(_P, sync_commit_off=True)),
            V("x4 UNLOGGED", replace(_P, unlogged=True)),
        ]),
        "walconf": ("full", [
            V("wal_compression=lz4 (base)", PgParallelSwap.opts, True, (("wal_compression", "lz4"),)),
            V("wal_compression=off", PgParallelSwap.opts, True, (("wal_compression", "off"),)),
            V("wal_compression=zstd", PgParallelSwap.opts, True, (("wal_compression", "zstd"),)),
        ]),
        "index": ("index", [
            V("serial, 2 workers each (server default)", _I),
            V("serial, 0 workers", replace(_I, index_workers=0)),
            V("serial, 1 worker each", replace(_I, index_workers=1)),
            V("serial, 3 workers each", replace(_I, index_workers=3)),
            V("concurrent A||B, 0 workers each", replace(_I, index_concurrent=True, index_workers=0)),
            V("concurrent A||B, 1 worker each", replace(_I, index_concurrent=True, index_workers=1)),
            V("concurrent A||B, 2 workers each", replace(_I, index_concurrent=True, index_workers=2)),
        ]),
        "index_c": ("index", [
            V("serial, 2 workers each (server default)", _I, True),
            V("serial, 0 workers", replace(_I, index_workers=0), True),
            V("serial, 1 worker each", replace(_I, index_workers=1), True),
            V("serial, 3 workers each", replace(_I, index_workers=3), True),
            V("concurrent A||B, 0 workers each", replace(_I, index_concurrent=True, index_workers=0), True),
            V("concurrent A||B, 1 worker each", replace(_I, index_concurrent=True, index_workers=1), True),
            V("concurrent A||B, 2 workers each", replace(_I, index_concurrent=True, index_workers=2), True),
        ]),
        "index_mem": ("index", [
            V(f"concurrent, 1 worker, maintenance_work_mem={m}",
              replace(_I, index_concurrent=True, index_workers=1, maintenance_work_mem=m), True)
            for m in ("32MB", "64MB", "256MB", "1GB", "2GB")
        ] + [
            V("concurrent, 1 worker, 1GB, fillfactor=100",
              replace(_I, index_concurrent=True, index_workers=1, maintenance_work_mem="1GB",
                      index_fillfactor=100), True),
            V("concurrent, 1 worker, 1GB, UNLOGGED table",
              replace(_I, index_concurrent=True, index_workers=1, maintenance_work_mem="1GB", unlogged=True), True),
        ]),
        "index_walconf": ("index", [
            V(f"concurrent, 1 worker, wal_compression={w}",
              replace(_I, index_concurrent=True, index_workers=1), True, (("wal_compression", w),))
            for w in ("lz4", "off", "zstd")
        ]),
        "pipeline": ("full", [
            V("pg_copy_swap (1 stream, serial indexes, default collation)", PgCopySwap.opts, False),
            V("+ C collation only", PgCopySwap.opts, True),
            V("+ 4 COPY streams only", replace(PgCopySwap.opts, encoder="csv", streams=4), False),
            V("+ concurrent index builds only", replace(PgCopySwap.opts, index_concurrent=True, index_workers=1), False),
            V("pg_parallel_swap (all of the above)", PgParallelSwap.opts, True),
            V("pg_parallel_swap, default collation", PgParallelSwap.opts, False),
            V("pg_unlogged_swap", PgUnloggedSwap.opts, True),
            V("pg_unlogged_setlogged", PgUnloggedSetLogged.opts, True),
        ]),
        "minimal": ("full", [
            V("wal_level=minimal: 4 streams (WAL-logged heap), indexes skip WAL", PgParallelSwap.opts, True),
            V("wal_level=minimal: one-txn COPY FREEZE, 4 encoders", PgWalMinimalSwap.opts, True),
            V("wal_level=minimal: one-txn COPY, 4 encoders, no FREEZE", replace(PgWalMinimalSwap.opts, freeze=False), True),
            V("wal_level=minimal: UNLOGGED 4 streams", PgUnloggedSwap.opts, True),
        ]),
    }


class _Cpu:
    """CPU seconds used by this process (+children), by the server container, and by everybody
    else on the machine during one measurement. On a shared box wall time is noisy; CPU time is not."""

    def __init__(self, container: str):
        from ..dockerutil import sh
        cid = sh("docker", "inspect", "-f", "{{.Id}}", container)
        cands = [Path(f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage"),
                 Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat"),
                 Path(f"/sys/fs/cgroup/docker/{cid}/cpu.stat")]
        self.path = next((c for c in cands if c.exists()), None)

    def _server(self) -> float:
        if self.path is None:
            return float("nan")
        txt = self.path.read_text()
        if self.path.name == "cpuacct.usage":
            return int(txt) / 1e9
        return int(txt.split("usage_usec")[1].split()[0]) / 1e6

    @staticmethod
    def _client() -> float:
        import resource
        a, b = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
        return a.ru_utime + a.ru_stime + b.ru_utime + b.ru_stime

    @staticmethod
    def _machine() -> float:
        import psutil
        t = psutil.cpu_times()
        return sum(t) - t.idle - t.iowait

    def snap(self):
        return time.perf_counter(), self._client(), self._server(), self._machine()

    def since(self, s0) -> dict:
        t, c, s, m = (b - a for a, b in zip(s0, self.snap()))
        return {"client_cpu_s": round(c, 2), "server_cpu_s": round(s, 2),
                "foreign_cores": round(max(0.0, m - c - s) / t, 2)}

    def wait_quiet(self, max_foreign: float = 1.0, timeout: float = 6.0) -> None:
        """Wait (bounded) for the other tenants of the machine to leave some CPU."""
        t_end = time.time() + timeout
        while time.time() < t_end:
            s0 = self.snap()
            time.sleep(0.5)
            if self.since(s0)["foreign_cores"] <= max_foreign:
                return


def _lab_one(sol: PostgresBase, admin_dsn: str, files: list[Path], mode: str, v: Variant, state: dict,
             cpu: _Cpu) -> dict:
    table = "lab_t"
    with psycopg.connect(admin_dsn, autocommit=True) as a:
        for k, val in v.conf:
            a.execute(f"ALTER SYSTEM SET {k} = '{val}'")
        a.execute("SELECT pg_reload_conf()")
        time.sleep(0.3)
        if mode == "index":
            key = (v.collate_c, v.opts.unlogged)
            if state.get("loaded") != key:
                a.execute(f"DROP TABLE IF EXISTS {table}")
                a.execute(create_table_sql(table, unlogged=v.opts.unlogged, collate_c=v.collate_c))
                copy_parallel(admin_dsn, table, files, _P)
                a.execute(f"SELECT count(*) FROM {table}")      # set hint bits once, outside the measurement
                state["loaded"] = key
            a.execute(f"DROP INDEX IF EXISTS {table}_a, {table}_b")
        else:
            a.execute(f"DROP TABLE IF EXISTS {table}")
            state["loaded"] = None
        a.execute("CHECKPOINT")
        lsn0 = a.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0]

    cpu.wait_quiet()
    ph = Phases()
    c0 = cpu.snap()
    t0 = time.perf_counter()
    if mode == "index":
        build_indexes(admin_dsn, table, v.opts, ph)
    elif mode == "naive":
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            c.execute(create_table_sql(table, collate_c=v.collate_c))
            c.execute(index_a_sql(table))
            c.execute(index_b_sql(table))
            if v.opts.encoder == "csv_ref":
                copy_parquet(c, table, files)
        if v.opts.encoder != "csv_ref":
            naive_insert_variant(admin_dsn, table, files, v.opts.encoder)
    elif mode == "copy":
        o = v.opts
        ddl = create_table_sql(table, unlogged=o.unlogged, collate_c=v.collate_c)
        if o.encoder == "duckdb":
            with psycopg.connect(admin_dsn, autocommit=True) as c:
                c.execute(ddl)
            copy_duckdb(admin_dsn, table, files, o.streams)
        elif o.single_txn:
            with psycopg.connect(admin_dsn, autocommit=False) as c:
                _apply(c, copy_session(o))
                c.execute(ddl)
                copy_fan_in(c, table, files, o)
                c.commit()
        else:
            with psycopg.connect(admin_dsn, autocommit=True) as c:
                c.execute(ddl)
                if o.encoder == "csv_ref":
                    copy_parquet(c, table, files)
            if o.encoder != "csv_ref":
                copy_parallel(admin_dsn, table, files, o)
    else:
        load_new_table(admin_dsn, files, v.opts, ph, collate_c=v.collate_c, table=table)
    total = time.perf_counter() - t0
    used = cpu.since(c0)

    with psycopg.connect(admin_dsn, autocommit=True) as a:
        wal = a.execute("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), %s)", [lsn0]).fetchone()[0]
        sizes = a.execute(
            f"SELECT pg_table_size('{table}'), pg_indexes_size('{table}'), (SELECT count(*) FROM {table})").fetchone()
        for k, _ in v.conf:
            a.execute(f"ALTER SYSTEM RESET {k}")
        a.execute("SELECT pg_reload_conf()")
    return {"total_s": round(total, 3), "phases": ph.times, **used, "wal_mb": round(float(wal) / 2**20, 1),
            "heap_mb": round(sizes[0] / 2**20, 1), "index_mb": round(sizes[1] / 2**20, 1), "rows": sizes[2]}


def lab(scale: str, suites: list[str], repeat: int, out: Path | None, minimal: bool = False) -> None:
    from ..datagen import generate

    ds = Dataset(spec.dataset_dir(scale), generate(scale))
    sol = PgWalMinimalSwap() if minimal else PgParallelSwap()
    sol.setup()
    admin = f"host=127.0.0.1 port={sol.port} user=postgres password=bench dbname=postgres"
    results: dict = {"scale": scale, "rows": ds.rows, "conf": sol.conf(), "suites": {}}
    cpu = _Cpu(sol.container)
    try:
        all_suites = _suites()
        for name in suites:
            mode, variants = all_suites[name]
            runs: dict[str, list[dict]] = {v.label: [] for v in variants}
            state: dict = {}
            for _ in range(repeat):
                for v in variants:
                    r = _lab_one(sol, admin, ds.base_files, mode, v, state, cpu)
                    assert r["rows"] == ds.rows, (v.label, r)
                    runs[v.label].append(r)
            print(f"\n== {name} ({mode}, {scale}, {repeat} runs each) ==\n{'variant':62s} wall_min / median   "
                  f"cpu client+server (median)  foreign load at min", flush=True)
            rows = []
            for v in variants:
                rs = runs[v.label]
                tot = [r["total_s"] for r in rs]
                phs = {k: min(r["phases"].get(k, 0) for r in rs) for k in rs[0]["phases"]}
                best = min(rs, key=lambda r: r["total_s"])
                row = {"variant": v.label, "min_s": min(tot), "median_s": round(statistics.median(tot), 3),
                       "client_cpu_s": round(statistics.median(r["client_cpu_s"] for r in rs), 2),
                       "server_cpu_s": round(statistics.median(r["server_cpu_s"] for r in rs), 2),
                       "foreign_cores_at_min": best["foreign_cores"],
                       "phases_min": phs, "wal_mb": rs[0]["wal_mb"], "heap_mb": rs[0]["heap_mb"],
                       "index_mb": rs[0]["index_mb"], "runs": rs, "opts": asdict(v.opts),
                       "collate_c": v.collate_c, "conf": dict(v.conf)}
                rows.append(row)
                print(f"{v.label:62s} {row['min_s']:7.2f} / {row['median_s']:7.2f}   "
                      f"{row['client_cpu_s']:5.2f}+{row['server_cpu_s']:5.2f}={row['client_cpu_s'] + row['server_cpu_s']:6.2f}s  "
                      f"foreign={row['foreign_cores_at_min']:4.2f}  wal={row['wal_mb']:5.0f}MB  "
                      f"idx={row['index_mb']:4.0f}MB  {json.dumps(phs)}", flush=True)
            results["suites"][name] = {"mode": mode, "variants": rows}
            if out:
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(json.dumps(results, indent=1))
    finally:
        sol.teardown()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="PostgreSQL lever lab (see docs/notes/postgres.md)")
    ap.add_argument("--scale", default="1M", choices=["100k", "1M"])
    ap.add_argument("--suite", default="all")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--minimal", action="store_true", help="run against a wal_level=minimal server")
    a = ap.parse_args()
    import signal
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))   # always tear down
    names = [s for s in _suites() if s not in ("minimal", "duckdb")] if a.suite == "all" else a.suite.split(",")
    lab(a.scale, names, a.repeat, a.out, a.minimal)
