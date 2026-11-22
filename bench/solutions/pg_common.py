"""Shared building blocks for every PostgreSQL-protocol solution
(vanilla PostgreSQL, TimescaleDB, ...)."""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .. import dockerutil as du
from .. import spec
from .base import Client, Dataset, Phases, Solution

# A sensibly tuned production profile for a 4 vCPU / 16 GB box. This is the
# *server* configuration every Postgres solution starts from; loading
# strategies may add session-level settings on top and must say so in info().
PG_BASE_CONF = {
    "shared_buffers": "4GB",
    "effective_cache_size": "10GB",
    "work_mem": "64MB",
    "maintenance_work_mem": "1GB",
    "max_wal_size": "8GB",
    "min_wal_size": "1GB",
    "checkpoint_timeout": "15min",
    "checkpoint_completion_target": "0.9",
    "wal_compression": "lz4",
    "max_worker_processes": "8",
    "max_parallel_workers": "4",
    "max_parallel_workers_per_gather": "2",
    "max_parallel_maintenance_workers": "2",
    "random_page_cost": "1.1",
    "effective_io_concurrency": "200",
    "max_connections": "100",
    "listen_addresses": "127.0.0.1",
}

COLDEFS = """
    id          BIGINT           NOT NULL,
    tenant_id   INTEGER          NOT NULL,
    period      INTEGER          NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    score       DOUBLE PRECISION NOT NULL,
    item_key    TEXT {coll}      NOT NULL,
    ref_path    TEXT {coll}      NOT NULL,
    description TEXT             NOT NULL
"""

PG_QUERIES = {
    "point": "SELECT id, tenant_id, period, quantity, price, score, item_key, ref_path, description "
             "FROM big_table WHERE tenant_id = %s AND item_key = %s AND period = %s",
    "series": "SELECT period, quantity, price, score FROM big_table "
              "WHERE ref_path = %s AND period BETWEEN %s AND %s ORDER BY period",
    "tenant_agg": "SELECT period, count(*), sum(quantity * price), avg(score) "
                  "FROM big_table WHERE tenant_id = %s GROUP BY period ORDER BY period",
    "full_agg": "SELECT tenant_id, count(*) AS cnt, sum(quantity), avg(price) "
                "FROM big_table GROUP BY tenant_id ORDER BY cnt DESC, tenant_id ASC LIMIT 20",
}


def create_table_sql(name: str, *, unlogged: bool = False, collate_c: bool = False, extra: str = "") -> str:
    coll = 'COLLATE "C"' if collate_c else ""
    return f"CREATE {'UNLOGGED ' if unlogged else ''}TABLE {name} ({COLDEFS.format(coll=coll)}) {extra}"


def index_a_sql(table: str, name: str | None = None) -> str:
    return f"CREATE UNIQUE INDEX {name or table + '_a'} ON {table} (tenant_id, item_key, period)"


def index_b_sql(table: str, name: str | None = None) -> str:
    return f"CREATE INDEX {name or table + '_b'} ON {table} (ref_path, period)"


def parquet_csv_chunks(files: Iterable[Path], batch_rows: int = 50_000):
    """Yield CSV-encoded byte chunks (no header) for the given parquet files."""
    opts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
    for f in files:
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(batch_size=batch_rows):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, write_options=opts)
            yield buf.getvalue()


def copy_parquet(conn: psycopg.Connection, table: str, files: Iterable[Path]) -> None:
    """Client-side streaming COPY (works against any managed Postgres)."""
    cols = ", ".join(spec.COLUMN_NAMES)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({cols}) FROM STDIN (FORMAT csv)") as cp:
            for chunk in parquet_csv_chunks(files):
                cp.write(chunk)


def rename_swap(conn: psycopg.Connection, new: str) -> None:
    """Atomically make table `new` (with indexes new_a / new_b) the live big_table.

    The swap transaction only RENAMES (catalog updates, milliseconds); the old table is
    dropped after the commit, when no reader can reach it any more. On a TimescaleDB
    hypertable, DROP inside the swap transaction deadlocked with a reader (DROP locks
    chunks before the parent, readers lock the parent first); LOCK TABLE up front fixes
    the lock order in both steps. For a plain table it makes no measurable difference
    (PostgreSQL unlinks the files after releasing the locks either way).

    Readers can still pause for deadlock_timeout (1 s) here if autovacuum is working on
    the live table: the LOCK waits that long before the worker is cancelled. See
    bench/labs/swap_stall.py and the README."""
    conn.execute("DROP TABLE IF EXISTS big_table_old")        # leftover of a crashed run
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '30s'")
        if conn.execute("SELECT to_regclass('big_table')").fetchone()[0] is not None:
            conn.execute("LOCK TABLE big_table IN ACCESS EXCLUSIVE MODE")
            conn.execute("ALTER TABLE big_table RENAME TO big_table_old")
            conn.execute("ALTER INDEX IF EXISTS big_table_a RENAME TO big_table_old_a")
            conn.execute("ALTER INDEX IF EXISTS big_table_b RENAME TO big_table_old_b")
        conn.execute(f"ALTER TABLE {new} RENAME TO big_table")
        conn.execute(f"ALTER INDEX IF EXISTS {new}_a RENAME TO big_table_a")
        conn.execute(f"ALTER INDEX IF EXISTS {new}_b RENAME TO big_table_b")
    with conn.transaction():
        if conn.execute("SELECT to_regclass('big_table_old')").fetchone()[0] is not None:
            conn.execute("LOCK TABLE big_table_old IN ACCESS EXCLUSIVE MODE")
            conn.execute("DROP TABLE big_table_old")


class PgClient(Client):
    def __init__(self, dsn: str, queries: dict[str, str] | None = None):
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.q = queries or PG_QUERIES

    def query(self, name: str, params: Sequence[Any]) -> list[tuple]:
        return self.conn.execute(self.q[name], params, prepare=True).fetchall()

    def close(self):
        self.conn.close()


class PostgresBase(Solution):
    engine = "postgres"
    image = "postgres:17"
    port = 55432
    conf_overrides: dict[str, str] = {}
    collate_c = False
    pgdata_in_container = "/var/lib/postgresql/data"

    # ---- lifecycle ----------------------------------------------------------
    @property
    def container(self) -> str:
        return f"b2d-{self.name}"

    @property
    def dsn(self) -> str:
        return f"host=127.0.0.1 port={self.port} user=postgres password=bench dbname=postgres"

    def conf(self) -> dict[str, str]:
        return {**PG_BASE_CONF, **self.conf_overrides}

    def server_args(self) -> list[str]:
        args = ["-p", str(self.port)]
        for k, v in self.conf().items():
            args += ["-c", f"{k}={v}"]
        return args

    def setup(self) -> None:
        d = du.data_dir(self.name)
        du.run(self.container, self.image, mounts={str(d): self.pgdata_in_container},
               env={"POSTGRES_PASSWORD": "bench"}, args=self.server_args())

        def ready():
            with psycopg.connect(self.dsn, connect_timeout=2) as c:
                return c.execute("SELECT 1").fetchone()[0] == 1

        du.wait_until(ready, timeout=120, what=self.container)
        # the official image restarts postgres once after initdb; make sure we are on the final instance
        import time
        time.sleep(2)
        du.wait_until(ready, timeout=60, what=self.container)
        self.post_setup()

    def post_setup(self) -> None:
        pass

    def teardown(self) -> None:
        du.rm(self.container)
        du.data_dir(self.name, fresh=True).rmdir()

    def data_path(self) -> Path | None:
        return spec.WORK_ROOT / self.name

    def connect(self, autocommit: bool = True) -> psycopg.Connection:
        return psycopg.connect(self.dsn, autocommit=autocommit)

    # ---- reads --------------------------------------------------------------
    def client(self) -> Client:
        return PgClient(self.dsn)

    def verify_row(self) -> tuple:
        with self.connect() as c:
            return c.execute(spec.VERIFY_SQL).fetchone()

    def storage(self) -> dict:
        with self.connect() as c:
            table = c.execute("SELECT pg_table_size('big_table')").fetchone()[0]
            idx = dict(c.execute(
                "SELECT indexrelid::regclass::text, pg_relation_size(indexrelid) "
                "FROM pg_index WHERE indrelid = 'big_table'::regclass").fetchall())
            total = c.execute("SELECT pg_total_relation_size('big_table')").fetchone()[0]
            db = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        a = next((v for k, v in idx.items() if k.endswith("_a")), None)
        b = next((v for k, v in idx.items() if k.endswith("_b")), None)
        return {"table": table, "index_a": a, "index_b": b, "total": total, "database": db, "indexes": idx}

    def info(self) -> dict:
        with self.connect() as c:
            ver = c.execute("SHOW server_version").fetchone()[0]
            coll = c.execute("SELECT datcollate FROM pg_database WHERE datname = current_database()").fetchone()[0]
        return {"image": self.image, "server_version": ver, "db_collation": coll,
                "text_key_collation": "C" if self.collate_c else coll, "conf": self.conf()}

    # ---- helpers ------------------------------------------------------------
    def table_exists(self, conn: psycopg.Connection, name: str = "big_table") -> bool:
        return conn.execute("SELECT to_regclass(%s)", [name]).fetchone()[0] is not None

    def swap_in(self, conn: psycopg.Connection, new: str) -> None:
        rename_swap(conn, new)

    # ---- default incremental maintenance (plain SQL, works on any Postgres) --
    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in spec.COLUMN_NAMES if c not in spec.KEY)
        with self.connect(autocommit=False) as c:
            with phases("stage_copy"):
                c.execute(create_table_sql("delta_stage", collate_c=self.collate_c).replace(
                    "CREATE TABLE", "CREATE TEMP TABLE") + " ON COMMIT DROP")
                copy_parquet(c, "delta_stage", [delta])
            with phases("merge"):
                c.execute(f"INSERT INTO big_table SELECT * FROM delta_stage "
                          f"ON CONFLICT (tenant_id, item_key, period) DO UPDATE SET {sets}")
            with phases("commit"):
                c.commit()

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        with self.connect(autocommit=False) as c:
            with phases("delete"):
                c.execute("DELETE FROM big_table WHERE tenant_id = %s", [ds.replace_tenant_id])
            with phases("copy"):
                copy_parquet(c, "big_table", [ds.replace_tenant_file])
            with phases("commit"):
                c.commit()
