"""MySQL 8.4 (InnoDB): the other mainstream OLTP engine, using its own bulk-load pattern.

    mysql_load_swap   new table, parallel LOAD DATA LOCAL INFILE, secondary indexes
                      added afterwards in one ALTER, ANALYZE, atomic RENAME TABLE

Design notes: docs/notes/mysql.md
"""
from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pymysql

from .. import dockerutil as du
from .. import spec
from . import register
from .base import Client, Dataset, Phases, Solution

PORT = 53306
COLS = ", ".join(spec.COLUMN_NAMES)

# Sensible production settings for a 4 vCPU / 16 GB box; durability left at the
# defaults (innodb_flush_log_at_trx_commit=1, sync_binlog=1, binary log ON as in
# any replicated setup).
MYSQL_CONF = {
    "port": str(PORT),
    "bind-address": "127.0.0.1",
    "mysqlx": "OFF",
    "local-infile": "1",
    "innodb-buffer-pool-size": "4G",
    "innodb-redo-log-capacity": "8G",
    "innodb-io-capacity": "2000",
    "innodb-io-capacity-max": "4000",
    "innodb-ddl-threads": "4",
    "innodb-ddl-buffer-size": "1073741824",
    "innodb-parallel-read-threads": "4",
    "max-allowed-packet": "1G",
    "binlog-expire-logs-seconds": "600",
    "max-connections": "100",
}

# Text keys are ASCII with a binary collation: comparisons are memcmp (the
# equivalent of COLLATE "C" in PostgreSQL) and 1 byte per char keeps the composite
# keys far below InnoDB's 3072-byte index limit.
KEYTYPE = "VARCHAR(160) CHARACTER SET ascii COLLATE ascii_bin NOT NULL"
COLDEFS = f"""
    id          BIGINT NOT NULL,
    tenant_id   INT    NOT NULL,
    period      INT    NOT NULL,
    quantity    DOUBLE NOT NULL,
    price       DOUBLE NOT NULL,
    score       DOUBLE NOT NULL,
    item_key    {KEYTYPE},
    ref_path    {KEYTYPE},
    description VARCHAR(600) CHARACTER SET ascii COLLATE ascii_bin NOT NULL
"""

QUERIES = {
    "point": f"SELECT {COLS} FROM big_table WHERE tenant_id = %s AND item_key = %s AND period = %s",
    "series": "SELECT period, quantity, price, score FROM big_table "
              "WHERE ref_path = %s AND period BETWEEN %s AND %s ORDER BY period",
    "tenant_agg": "SELECT period, count(*), sum(quantity * price), avg(score) "
                  "FROM big_table WHERE tenant_id = %s GROUP BY period ORDER BY period",
    "full_agg": "SELECT tenant_id, count(*) AS cnt, sum(quantity), avg(price) "
                "FROM big_table GROUP BY tenant_id ORDER BY cnt DESC, tenant_id ASC LIMIT 20",
}

LOAD_SQL = ("LOAD DATA LOCAL INFILE %s INTO TABLE {table} CHARACTER SET ascii "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' LINES TERMINATED BY '\\n' "
            f"({COLS})")


def _connect(autocommit: bool = True) -> pymysql.connections.Connection:
    return pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="bench", database="bench",
                           autocommit=autocommit, local_infile=True, read_timeout=7200, write_timeout=7200)


def _exec(conn, sql: str, args=None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return list(cur.fetchall())


def _feed_fifo(fifo: str, units: Iterable[tuple[Path, int]]):
    """Parquet row groups -> CSV into a named pipe (nothing is materialised on disk)."""
    opts = pacsv.WriteOptions(include_header=False, quoting_style="needed")
    handles: dict[Path, pq.ParquetFile] = {}
    with open(fifo, "wb", buffering=1 << 20) as out:
        for f, rg in units:
            pf = handles.get(f) or handles.setdefault(f, pq.ParquetFile(f))
            pacsv.write_csv(pf.read_row_group(rg, columns=spec.COLUMN_NAMES), out, write_options=opts)


def load_parallel(table: str, files: Iterable[Path], streams: int, workdir: Path,
                  conn: pymysql.connections.Connection | None = None) -> None:
    """`streams` connections, each running one LOAD DATA LOCAL INFILE fed from its own FIFO.
    With `conn` given, a single stream runs on that connection (inside its transaction)."""
    units = []
    for f in files:
        units += [(f, rg) for rg in range(pq.ParquetFile(f).metadata.num_row_groups)]
    streams = 1 if conn is not None else max(1, min(streams, len(units)))
    shards = [units[k::streams] for k in range(streams)]

    def one(k: int):
        fifo = str(workdir / f"load-{os.getpid()}-{threading.get_ident()}-{k}.fifo")
        if os.path.exists(fifo):
            os.unlink(fifo)
        os.mkfifo(fifo)
        feeder = threading.Thread(target=_feed_fifo, args=(fifo, shards[k]), daemon=True)
        feeder.start()
        c = conn or _connect()
        try:
            if conn is None:
                _exec(c, "SET SESSION unique_checks = 0, foreign_key_checks = 0")
            _exec(c, LOAD_SQL.format(table=table), [fifo])
        finally:
            if conn is None:
                c.close()
            feeder.join(timeout=60)
            os.unlink(fifo)

    with ThreadPoolExecutor(streams) as ex:
        for fut in [ex.submit(one, k) for k in range(streams)]:
            fut.result()


class MyClient(Client):
    def __init__(self):
        self.conn = _connect()

    def query(self, name: str, params: Sequence[Any]) -> list[tuple]:
        return _exec(self.conn, QUERIES[name], list(params))

    def close(self):
        self.conn.close()


@register
class MysqlLoadSwap(Solution):
    """InnoDB tables are clustered on the PRIMARY KEY, so the key design decides the load
    cost. `pk = "id"` (default): cluster on the integer row id (the source arrives in id
    order -> append-only clustered index), business key and ref_path as secondary indexes
    built AFTER the load by one ALTER TABLE (sorted, parallel index build).
    `pk = "business"`: cluster on (tenant_id, item_key, period) -> every row is inserted at
    a random place in a 500-byte-per-row B-tree (measured as a losing variant, see notes)."""

    name = "mysql_load_swap"
    engine = "mysql"
    strategy = ("new InnoDB table clustered on id, 4 parallel LOAD DATA LOCAL INFILE streams, both secondary "
                "indexes added afterwards in one ALTER, ANALYZE, atomic RENAME TABLE (binlog on)")
    atomic_swap = True
    image = "mysql:8.4"
    streams = 4
    pk = os.environ.get("B2D_MYSQL_PK", "id")

    @property
    def container(self) -> str:
        return f"b2d-{self.name}"

    def data_path(self) -> Path:
        return spec.WORK_ROOT / self.name

    @property
    def fifo_dir(self) -> Path:
        d = spec.WORK_ROOT / f"{self.name}_fifo"
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ---- lifecycle ----------------------------------------------------------
    def setup(self) -> None:
        d = du.data_dir(self.name)
        du.run(self.container, self.image, mounts={str(d): "/var/lib/mysql"},
               env={"MYSQL_ROOT_PASSWORD": "bench", "MYSQL_DATABASE": "bench", "MYSQL_ROOT_HOST": "%"},
               args=[f"--{k}={v}" for k, v in MYSQL_CONF.items()])

        def ready():
            c = _connect()
            try:
                return _exec(c, "SELECT 1")[0][0] == 1
            finally:
                c.close()

        du.wait_until(ready, timeout=240, interval=1.0, what=self.container)

    def teardown(self) -> None:
        du.rm(self.container)
        du.data_dir(self.name, fresh=True).rmdir()
        du.sh("rm", "-rf", str(spec.WORK_ROOT / f"{self.name}_fifo"), check=False)

    def info(self) -> dict:
        c = _connect()
        try:
            ver = _exec(c, "SELECT VERSION()")[0][0]
            keys = ("log_bin", "sync_binlog", "innodb_flush_log_at_trx_commit", "innodb_buffer_pool_size",
                    "innodb_redo_log_capacity", "innodb_flush_method", "innodb_doublewrite", "binlog_format",
                    "innodb_ddl_threads", "innodb_ddl_buffer_size")
            var = {k: _exec(c, f"SELECT @@{k}")[0][0] for k in keys}
        finally:
            c.close()
        return {"image": self.image, "server_version": ver, "variables": var, "pk_design": self.pk,
                "streams": self.streams, "client": "pymysql (pure Python)"}

    # ---- DDL ----------------------------------------------------------------
    def _create(self, c, name: str, with_indexes: bool = False) -> None:
        pk = "PRIMARY KEY (id)" if self.pk == "id" else "PRIMARY KEY (tenant_id, item_key, period)"
        extra = ""
        if with_indexes:
            extra = ", " + ", ".join(self._index_defs())
        _exec(c, f"CREATE TABLE {name} ({COLDEFS}, {pk}{extra}) ENGINE=InnoDB")

    def _index_defs(self) -> list[str]:
        defs = ["KEY idx_b (ref_path, period)"]
        if self.pk == "id":
            defs.insert(0, "UNIQUE KEY idx_a (tenant_id, item_key, period)")
        return defs

    # ---- timed operations ---------------------------------------------------
    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        c = _connect()
        try:
            _exec(c, "DROP TABLE IF EXISTS big_table_new, big_table_old")
            self._create(c, "big_table_new")
            with phases("load_data"):
                load_parallel("big_table_new", ds.base_files, self.streams, self.fifo_dir)
            with phases("add_indexes"):
                _exec(c, "ALTER TABLE big_table_new " + ", ".join("ADD " + d for d in self._index_defs())
                      + ", ALGORITHM=INPLACE")
            with phases("analyze"):
                _exec(c, "ANALYZE TABLE big_table_new")
            with phases("swap"):
                if _exec(c, "SHOW TABLES LIKE 'big_table'"):
                    _exec(c, "RENAME TABLE big_table TO big_table_old, big_table_new TO big_table")
                    _exec(c, "DROP TABLE big_table_old")
                else:
                    _exec(c, "RENAME TABLE big_table_new TO big_table")
        finally:
            c.close()

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        sets = ", ".join(f"{col} = new.{col}" for col in spec.COLUMN_NAMES if col not in spec.KEY)
        c = _connect(autocommit=False)
        try:
            _exec(c, "DROP TEMPORARY TABLE IF EXISTS delta_stage")
            _exec(c, f"CREATE TEMPORARY TABLE delta_stage ({COLDEFS}) ENGINE=InnoDB")
            with phases("stage_load"):
                load_parallel("delta_stage", [delta], 1, self.fifo_dir, conn=c)
            with phases("merge"):
                _exec(c, f"INSERT INTO big_table ({COLS}) SELECT {COLS} FROM delta_stage AS new "
                         f"ON DUPLICATE KEY UPDATE {sets}")
            with phases("commit"):
                c.commit()
            _exec(c, "DROP TEMPORARY TABLE IF EXISTS delta_stage")
            c.commit()
        finally:
            c.close()

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        c = _connect(autocommit=False)
        try:
            with phases("delete"):
                _exec(c, "DELETE FROM big_table WHERE tenant_id = %s", [ds.replace_tenant_id])
            with phases("load_data"):
                load_parallel("big_table", [ds.replace_tenant_file], 1, self.fifo_dir, conn=c)
            with phases("commit"):
                c.commit()
        finally:
            c.close()

    # ---- reads --------------------------------------------------------------
    def client(self) -> Client:
        return MyClient()

    def verify_row(self) -> tuple:
        c = _connect()
        try:
            return _exec(c, spec.VERIFY_SQL)[0]
        finally:
            c.close()

    def storage(self) -> dict:
        c = _connect()
        try:
            _exec(c, "ANALYZE TABLE big_table")
            page = _exec(c, "SELECT @@innodb_page_size")[0][0]
            idx = dict(_exec(c, "SELECT index_name, stat_value * %s FROM mysql.innodb_index_stats "
                                "WHERE database_name = 'bench' AND table_name = 'big_table' AND stat_name = 'size'",
                             [page]))
            file_size = _exec(c, "SELECT FILE_SIZE FROM information_schema.INNODB_TABLESPACES "
                                 "WHERE NAME = 'bench/big_table'")
        finally:
            c.close()
        idx = {k: int(v) for k, v in idx.items()}
        return {"table": idx.get("PRIMARY"), "index_a": idx.get("idx_a"), "index_b": idx.get("idx_b"),
                "total": int(file_size[0][0]) if file_size else sum(idx.values()), "indexes": idx}
