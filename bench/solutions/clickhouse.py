"""ClickHouse solutions.

    ch_mergetree   immutable MergeTree, clustered on the business key, projection index for the
                   second access path. Every write (refresh, upsert, tenant replacement) builds
                   fully merged parts off-line and swaps them in atomically
                   (EXCHANGE TABLES / REPLACE PARTITION). Readers never need FINAL.
    ch_replacing   ReplacingMergeTree keyed on the business key, bloom-filter skip index for the
                   second access path. Refresh is the same build + EXCHANGE TABLES; maintenance is
                   a plain INSERT (plus a lightweight DELETE for rows that disappear) and every
                   read uses FINAL, so results are correct the moment the statement returns.

Both read the Parquet files server-side with file() (production analogue: s3()), bucket the
table by `tenant_id % 8` so the post-insert merges run in parallel, and only swap the new table
in once every bucket is a single merged part. Design notes and the final measurements:
docs/notes/clickhouse.md
"""
from __future__ import annotations

import http.client
import json
import urllib.parse
from pathlib import Path
from typing import Any, Sequence

from .. import dockerutil as du
from .. import spec
from . import register
from .base import Client, Dataset, Phases, Solution

CH_IMAGE = "clickhouse/clickhouse-server:latest"
HTTP_PORT = 58123
TCP_PORT = 59000
INTERSERVER_PORT = 59009

LIVE = spec.TABLE               # big_table
STAGE = spec.TABLE + "_new"     # full rebuild target, swapped in with EXCHANGE TABLES
PART_STAGE = spec.TABLE + "_part"   # single-bucket rebuild target, swapped in with REPLACE PARTITION

CH_TYPES = {"int64": "Int64", "int32": "Int32", "float64": "Float64", "text": "String"}
COLS = ", ".join(spec.COLUMN_NAMES)
PARQUET_STRUCTURE = ", ".join(f"{c} {CH_TYPES[t]}" for c, t in spec.COLUMNS)
KEY_TUPLE = "(" + ", ".join(spec.KEY) + ")"

# ---------------------------------------------------------------------------
# Server configuration (config.d). Everything not listed is the image default,
# in particular all durability-related MergeTree settings (see info()).
# ---------------------------------------------------------------------------
SERVER_CONF = {
    "max_server_memory_usage": 8 * 2**30,   # hard cap for the whole server on this 15 GB box
    "mark_cache_size": 1 * 2**30,           # default 5 GiB; marks of this table are < 20 MB
    "uncompressed_cache_size": 0,           # default; measured: no gain for these lookups
}

# system log tables that only add background writes / disk usage to the benchmark
# (query_log and part_log stay on, as in production)
_OFF_LOGS = ["trace_log", "text_log", "metric_log", "asynchronous_metric_log", "processors_profile_log",
             "opentelemetry_span_log", "query_views_log", "query_thread_log", "session_log", "error_log",
             "latency_log", "query_metric_log", "asynchronous_insert_log", "backup_log", "blob_storage_log",
             "crash_log", "s3queue_log", "zookeeper_log", "background_schedule_pool_log",
             "aggregated_zookeeper_log", "zookeeper_connection_log", "iceberg_metadata_log",
             "delta_lake_metadata_log", "transactions_info_log"]


def config_xml() -> str:
    logs = "\n".join(f'    <{name} remove="1"/>' for name in _OFF_LOGS)
    conf = "\n".join(f"    <{k}>{v}</{k}>" for k, v in SERVER_CONF.items())
    return f"""<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <http_port>{HTTP_PORT}</http_port>
    <tcp_port>{TCP_PORT}</tcp_port>
    <interserver_http_port>{INTERSERVER_PORT}</interserver_http_port>
    <mysql_port remove="1"/>
    <postgresql_port remove="1"/>
    <user_files_path>/bench_data/</user_files_path>
    <logger><level>warning</level><console>1</console></logger>
{conf}
{logs}
</clickhouse>
"""


# ---------------------------------------------------------------------------
# HTTP client. Plain http.client with keep-alive: measured against
# clickhouse-connect (HTTP) and clickhouse-driver (native TCP) the server-side
# cost per lookup is the same and this has the least client-side overhead.
# ---------------------------------------------------------------------------
class CHError(RuntimeError):
    pass


class CHHttp:
    def __init__(self, port: int = HTTP_PORT, settings: dict | None = None, timeout: float = 7200):
        self.port, self.timeout = port, timeout
        self.base = "/?" + urllib.parse.urlencode(settings or {})
        self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)

    def raw(self, sql: str | bytes, extra: str = "", retry: bool = True) -> bytes:
        body = sql.encode() if isinstance(sql, str) else sql
        for attempt in (0, 1):
            try:
                self.conn.request("POST", self.base + extra, body)
                r = self.conn.getresponse()
                data = r.read()
                break
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError,
                    http.client.CannotSendRequest, http.client.ResponseNotReady):
                # keep-alive connection closed by the server while idle: reconnect once
                self.conn.close()
                self.conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
                if attempt or not retry:
                    raise
        if r.status != 200:
            raise CHError(f"HTTP {r.status}: {data[:2000].decode(errors='replace')}\n-- {body[:600]!r}")
        return data

    def exec(self, sql: str, **settings) -> None:
        """Statement without a result set; errors always surface as HTTP status."""
        s = {"wait_end_of_query": 1, **settings}
        self.raw(sql, "&" + urllib.parse.urlencode(s), retry=False)

    def rows(self, sql: str, **settings) -> list[list]:
        s = {"output_format_json_quote_64bit_integers": 0, **settings}
        data = self.raw(sql + " FORMAT JSONCompactEachRow", "&" + urllib.parse.urlencode(s))
        return [json.loads(line) for line in data.splitlines() if line]

    def scalar(self, sql: str, **settings):
        return self.rows(sql, **settings)[0][0]

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def _param(v: Any) -> str:
    # query parameters are parsed in "escaped" (TSV) form on the server
    s = str(v).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
    return urllib.parse.quote(s, safe="")


class CHClient(Client):
    """One application connection: parameterised queries over HTTP keep-alive."""

    def __init__(self, queries: dict[str, tuple[str, dict]]):
        self.http = CHHttp(settings={"output_format_json_quote_64bit_integers": 0})
        self.q = {}
        for name, (sql, settings) in queries.items():
            extra = "".join(f"&{k}={v}" for k, v in settings.items())
            self.q[name] = ((sql + " FORMAT JSONCompactEachRow").encode(), extra)

    def query(self, name: str, params: Sequence[Any]) -> list[tuple]:
        body, extra = self.q[name]
        for i, v in enumerate(params):
            extra += f"&param_p{i}={_param(v)}"
        data = self.http.raw(body, extra)
        return [tuple(json.loads(line)) for line in data.splitlines()]

    def close(self):
        self.http.close()


# ---------------------------------------------------------------------------
# Common base
# ---------------------------------------------------------------------------
class ClickHouseBase(Solution):
    engine = "clickhouse"
    image = CH_IMAGE
    atomic_swap = True

    # ---- physical design (see docs/notes/clickhouse.md for the measurements) ----
    table_engine = "MergeTree"
    extra_columns: list[str] = []
    secondary = ""                      # second access path (ref_path, period)
    extra_table_settings: dict[str, Any] = {}
    #: tenant_id % BUCKETS partitions: 8 independent merges instead of one single-threaded
    #: 20M-row merge; point / tenant queries prune to one bucket.
    buckets = 8
    #: 256 rows per granule instead of 8192: a point lookup decompresses ~100 KB, not ~4 MB.
    index_granularity = 256
    default_codec = "LZ4"
    codecs = {"description": "ZSTD(1)"}  # 60 % of the bytes, never filtered on; costs no measurable load CPU
    use_final = False

    #: INSERT ... SELECT FROM file(): parallel Parquet decode and parallel part writers
    #: (both already default to the core count; pinned here to be explicit).
    insert_settings = {"max_threads": 4, "max_insert_threads": 4}
    #: settings for the two keyed lookups: one thread per query (no pipeline fan-out for 1 granule)
    lookup_settings: dict[str, Any] = {"max_threads": 1}

    # ---- lifecycle ----------------------------------------------------------
    @property
    def container(self) -> str:
        return f"b2d-{self.name}"

    def setup(self) -> None:
        d = du.data_dir(self.name)
        for sub in ("data", "conf", "log"):
            (d / sub).mkdir()
        (d / "conf" / "config.xml").write_text(config_xml())
        du.sh("chown", "-R", "101:101", str(d / "data"), str(d / "log"))
        du.run(self.container, self.image,
               mounts={str(d / "data"): "/var/lib/clickhouse",
                       str(d / "log"): "/var/log/clickhouse-server",
                       # replaces the image's docker_related_config.xml (which listens on 0.0.0.0)
                       str(d / "conf" / "config.xml"): "/etc/clickhouse-server/config.d/docker_related_config.xml:ro",
                       str(spec.DATA_ROOT): "/bench_data:ro"},
               env={"CLICKHOUSE_SKIP_USER_SETUP": "1", "CLICKHOUSE_DO_NOT_CHOWN": "1"})

        def ready():
            with CHHttp(timeout=3) as c:
                return c.rows("SELECT 1") == [[1]]

        try:
            du.wait_until(ready, timeout=120, what=self.container)
        except TimeoutError:
            print(du.logs(self.container))
            raise

    def teardown(self) -> None:
        du.rm(self.container)
        du.data_dir(self.name, fresh=True).rmdir()

    def data_path(self) -> Path | None:
        return spec.WORK_ROOT / self.name / "data"

    def admin(self) -> CHHttp:
        return CHHttp()

    # ---- DDL / SQL building blocks -------------------------------------------
    def table_settings(self) -> dict[str, Any]:
        return {"index_granularity": self.index_granularity,
                # cut compressed blocks at ~8 KiB so that one 256-row granule of the small columns
                # is not buried in a 64 KiB block
                "min_compress_block_size": 8192,
                **self.extra_table_settings}

    def ddl(self, name: str) -> str:
        cols = [f"{c} {CH_TYPES[t]} CODEC({self.codecs.get(c, self.default_codec)})" for c, t in spec.COLUMNS]
        cols += self.extra_columns
        if self.secondary:
            cols.append(self.secondary)
        settings = ", ".join(f"{k} = {v!r}" if isinstance(v, str) else f"{k} = {v}"
                             for k, v in self.table_settings().items())
        return (f"CREATE TABLE {name} (\n    " + ",\n    ".join(cols) + f"\n) ENGINE = {self.table_engine}\n"
                f"PARTITION BY tenant_id % {self.buckets}\n"
                f"ORDER BY ({', '.join(spec.KEY)})\n"
                f"SETTINGS {settings}")

    @staticmethod
    def src(files: Sequence[Path]) -> str:
        """file() table function over Parquet files below user_files_path (= spec.DATA_ROOT)."""
        rel = [str(Path(f).resolve().relative_to(spec.DATA_ROOT.resolve())) for f in files]
        glob = rel[0] if len(rel) == 1 else "{" + ",".join(rel) + "}"
        return f"file('{glob}', 'Parquet', '{PARQUET_STRUCTURE}')"

    def select_list(self, version: int | None = None) -> str:
        return COLS

    def table_exists(self, c: CHHttp, name: str = LIVE) -> bool:
        return c.scalar(f"EXISTS TABLE {name}") == 1

    def build(self, c: CHHttp, name: str, select_sql: str, phases: Phases, prefix: str = "") -> None:
        """Create `name`, fill it and bring it to its serving state: every bucket one merged part."""
        c.exec(f"DROP TABLE IF EXISTS {name} SYNC")
        c.exec(self.ddl(name))
        with phases(prefix + "insert"):
            c.exec(f"INSERT INTO {name} {select_sql}", **self.insert_settings)
        with phases(prefix + "optimize"):
            # one merge per bucket, run in parallel by the background pool; returns when all are done
            for _ in range(5):
                c.exec(f"OPTIMIZE TABLE {name} FINAL", alter_sync=2)
                n_parts, n_buckets = c.rows(
                    f"SELECT count(), uniqExact(partition_id) FROM system.parts "
                    f"WHERE database = currentDatabase() AND table = '{name}' AND active")[0]
                if n_parts == n_buckets:
                    break
            else:
                raise CHError(f"{name}: still {n_parts} parts in {n_buckets} buckets after OPTIMIZE FINAL")
            phases.note(prefix + "parts", n_parts)

    def warm(self, c: CHHttp, name: str) -> None:
        """Load primary indexes (lazy by default) of every new part before readers are switched."""
        c.rows(f"SELECT count() FROM {name} WHERE item_key = ''")
        c.rows(f"SELECT count() FROM {name} WHERE ref_path = ''", **self.series_settings())

    def swap_in(self, c: CHHttp, new: str) -> None:
        """Atomically make `new` the live table (Atomic database: readers that already started
        keep reading the old parts; new queries see the new table)."""
        if self.table_exists(c):
            c.exec(f"EXCHANGE TABLES {new} AND {LIVE}")
            c.exec(f"DROP TABLE {new} SYNC")
        else:
            c.exec(f"RENAME TABLE {new} TO {LIVE}")

    # ---- timed operations ------------------------------------------------------
    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        with self.admin() as c:
            self.build(c, STAGE, f"SELECT {self.select_list(1)} FROM {self.src(ds.base_files)}", phases)
            with phases("warm"):
                self.warm(c, STAGE)
            with phases("swap"):
                self.swap_in(c, STAGE)

    # ---- reads -------------------------------------------------------------------
    def series_settings(self) -> dict[str, Any]:
        return dict(self.lookup_settings)

    def queries(self) -> dict[str, tuple[str, dict]]:
        t = LIVE + (" FINAL" if self.use_final else "")
        agg = {"do_not_merge_across_partitions_select_final": 1} if self.use_final else {}
        look = {**self.lookup_settings, **agg}
        return {
            "point": (f"SELECT {COLS} FROM {t} WHERE tenant_id = {{p0:Int32}} AND item_key = {{p1:String}} "
                      f"AND period = {{p2:Int32}}", look),
            "series": (f"SELECT period, quantity, price, score FROM {t} WHERE ref_path = {{p0:String}} "
                       f"AND period BETWEEN {{p1:Int32}} AND {{p2:Int32}} ORDER BY period",
                       {**self.series_settings(), **agg}),
            "tenant_agg": (f"SELECT period, count() AS cnt, sum(quantity * price) AS revenue, avg(score) AS avg_score "
                           f"FROM {t} WHERE tenant_id = {{p0:Int32}} GROUP BY period ORDER BY period", agg),
            "full_agg": (f"SELECT tenant_id, count() AS cnt, sum(quantity) AS sum_qty, avg(price) AS avg_price "
                         f"FROM {t} GROUP BY tenant_id ORDER BY cnt DESC, tenant_id ASC LIMIT 20", agg),
        }

    def client(self) -> Client:
        return CHClient(self.queries())

    def verify_row(self) -> tuple:
        t = LIVE + (" FINAL" if self.use_final else "")
        with self.admin() as c:
            return tuple(c.rows(f"SELECT count(), sum(quantity), sum(price), max(id) FROM {t}")[0])

    def storage(self) -> dict:
        with self.admin() as c:
            (total, data, raw, marks, pk, skip, skip_marks, n_parts, n_rows, pk_mem) = c.rows(
                "SELECT sum(bytes_on_disk), sum(data_compressed_bytes), sum(data_uncompressed_bytes), "
                "sum(marks_bytes), sum(primary_key_size), sum(secondary_indices_compressed_bytes), "
                "sum(secondary_indices_marks_bytes), count(), sum(rows), sum(primary_key_bytes_in_memory) "
                f"FROM system.parts WHERE database = currentDatabase() AND table = '{LIVE}' AND active")[0]
            proj = c.scalar("SELECT sum(bytes_on_disk) FROM system.projection_parts "
                            f"WHERE database = currentDatabase() AND table = '{LIVE}' AND active")
            inactive = c.scalar("SELECT sum(bytes_on_disk) FROM system.parts "
                                "WHERE database = currentDatabase() AND NOT active")
            columns = dict(c.rows(
                "SELECT column, sum(column_data_compressed_bytes) FROM system.parts_columns "
                f"WHERE database = currentDatabase() AND table = '{LIVE}' AND active GROUP BY column"))
        # ClickHouse has no separate index for the business key: the table itself is sorted on it.
        # "index_a" is what the sparse primary index adds (primary.cidx + mark files of the base part),
        # "index_b" is the projection or skip index that serves (ref_path, period).
        index_a = pk + marks
        index_b = proj + skip + skip_marks
        return {"table": total - index_a - index_b, "index_a": index_a, "index_b": index_b, "total": total,
                "uncompressed": raw, "parts": n_parts, "rows_in_parts": n_rows,
                "primary_key_bytes_in_memory": pk_mem,
                # merged-away parts are kept for old_parts_lifetime (default 480 s) before deletion;
                # they show up in data_dir_bytes but are not part of the table
                "inactive_parts_bytes": inactive, "columns_compressed": columns}

    def info(self) -> dict:
        with self.admin() as c:
            ver = c.scalar("SELECT version()")
            durability = dict(c.rows(
                "SELECT name, value FROM system.merge_tree_settings WHERE name IN ('fsync_after_insert', "
                "'fsync_part_directory', 'min_rows_to_fsync_after_merge', "
                "'min_compressed_bytes_to_fsync_after_merge', 'old_parts_lifetime')"))
        return {"image": self.image, "image_digest": du.image_version(self.image), "server_version": ver,
                "server_conf": SERVER_CONF, "disabled_system_logs": _OFF_LOGS,
                "ports": {"http": HTTP_PORT, "native": TCP_PORT, "interserver": INTERSERVER_PORT},
                "ddl": self.ddl(LIVE), "insert_settings": self.insert_settings,
                "queries": {k: {"sql": s, "settings": st} for k, (s, st) in self.queries().items()},
                "client": "python http.client, HTTP keep-alive, JSONCompactEachRow",
                "durability_defaults_unchanged": durability,
                "statistics": "none needed: the sparse primary index is written with each part"}


# ---------------------------------------------------------------------------
# 1. Immutable MergeTree, rebuilt and swapped
# ---------------------------------------------------------------------------
@register
class ChMergeTree(ClickHouseBase):
    name = "ch_mergetree"
    strategy = ("MergeTree ORDER BY business key + projection index on (ref_path, period); "
                "INSERT SELECT FROM file(Parquet) -> OPTIMIZE FINAL -> EXCHANGE TABLES; "
                "maintenance = rebuild from table + delta and swap (no FINAL at read time)")

    # A projection that stores only (ref_path, period, _part_offset): the general-purpose
    # equivalent of a secondary B-tree (any column can be fetched through it).
    secondary = "PROJECTION by_ref INDEX (ref_path, period) TYPE basic"

    def series_settings(self) -> dict[str, Any]:
        # the planner only bothers with a projection index for parts >= 1M rows by default
        return {**self.lookup_settings, "min_table_rows_to_use_projection_index": 0}

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        """MergeTree has no in-place upsert. The delta is scattered over every bucket, so the
        cheapest way that keeps the table fully merged and readers consistent is to write
        (old rows whose key is not in the delta) + delta into a new table and swap."""
        d = self.src([delta])
        with self.admin() as c:
            self.build(c, STAGE,
                       f"SELECT {COLS} FROM {LIVE} WHERE {KEY_TUPLE} NOT IN (SELECT {', '.join(spec.KEY)} FROM {d}) "
                       f"UNION ALL SELECT {COLS} FROM {d}", phases)
            with phases("warm"):
                self.warm(c, STAGE)
            with phases("swap"):
                self.swap_in(c, STAGE)

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        """Rebuild only the bucket the tenant lives in (1/8 of the table) and swap that partition."""
        t = ds.replace_tenant_id
        b = t % self.buckets
        with self.admin() as c:
            self.build(c, PART_STAGE,
                       f"SELECT {COLS} FROM {LIVE} WHERE tenant_id % {self.buckets} = {b} AND tenant_id != {t} "
                       f"UNION ALL SELECT {COLS} FROM {self.src([ds.replace_tenant_file])} WHERE tenant_id = {t}",
                       phases)
            with phases("warm"):
                self.warm(c, PART_STAGE)
            with phases("swap"):
                c.exec(f"ALTER TABLE {LIVE} REPLACE PARTITION ID '{b}' FROM {PART_STAGE}")
                c.exec(f"DROP TABLE {PART_STAGE} SYNC")


# ---------------------------------------------------------------------------
# 2. ReplacingMergeTree + FINAL
# ---------------------------------------------------------------------------
@register
class ChReplacing(ClickHouseBase):
    name = "ch_replacing"
    strategy = ("ReplacingMergeTree(_ver) ORDER BY business key + bloom-filter skip index on ref_path; "
                "refresh = INSERT SELECT FROM file(Parquet) -> OPTIMIZE FINAL -> EXCHANGE TABLES; "
                "upsert = INSERT, tenant replace = INSERT + lightweight DELETE; all reads use FINAL")

    table_engine = "ReplacingMergeTree(_ver)"
    extra_columns = ["_ver UInt32 CODEC(LZ4)"]
    # projections are not used by FINAL queries (measured: full scan), a skip index is.
    secondary = "INDEX ix_ref ref_path TYPE bloom_filter(0.001) GRANULARITY 8"
    use_final = True

    def select_list(self, version: int | None = None) -> str:
        return f"{COLS}, {version}"

    def next_version(self, c: CHHttp) -> int:
        return int(c.scalar(f"SELECT max(_ver) FROM {LIVE}")) + 1

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        with self.admin() as c:
            ver = self.next_version(c)
            with phases("insert"):
                c.exec(f"INSERT INTO {LIVE} SELECT {self.select_list(ver)} FROM {self.src([delta])}",
                       **self.insert_settings)
            phases.note("version", ver)

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        t = ds.replace_tenant_id
        with self.admin() as c:
            ver = self.next_version(c)
            with phases("insert"):
                c.exec(f"INSERT INTO {LIVE} SELECT {self.select_list(ver)} "
                       f"FROM {self.src([ds.replace_tenant_file])} WHERE tenant_id = {t}", **self.insert_settings)
            with phases("delete_stale"):
                # rows of the tenant that were not re-delivered: mask them (lightweight delete,
                # synchronous by default: lightweight_deletes_sync = 2)
                c.exec(f"DELETE FROM {LIVE} IN PARTITION ID '{t % self.buckets}' "
                       f"WHERE tenant_id = {t} AND _ver < {ver}")
            phases.note("version", ver)
