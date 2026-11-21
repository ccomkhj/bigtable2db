"""ClickHouse solutions (work in progress)."""
from __future__ import annotations

import http.client
import json
import time
import urllib.parse
from pathlib import Path
from typing import Any, Sequence

from .. import dockerutil as du
from .. import spec
from . import register  # noqa: F401
from .base import Client, Dataset, Phases, Solution

CH_IMAGE = "clickhouse/clickhouse-server:latest"
HTTP_PORT = 58123
TCP_PORT = 59000
INTERSERVER_PORT = 59009

SERVER_CONF = {
    "max_server_memory_usage": 8 * 2**30,
    "mark_cache_size": 1 * 2**30,
    "uncompressed_cache_size": 1 * 2**30,
}

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


class CHError(RuntimeError):
    pass


class CHHttp:
    """Minimal keep-alive HTTP client for ClickHouse (no third-party driver in the hot path)."""

    def __init__(self, port: int = HTTP_PORT, settings: dict | None = None, timeout: float = 7200):
        self.port, self.timeout = port, timeout
        self.base = "/?" + urllib.parse.urlencode(settings or {})
        self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)

    def raw(self, sql: str, extra: str = "") -> bytes:
        body = sql.encode()
        for attempt in (0, 1):
            try:
                self.conn.request("POST", self.base + extra, body)
                r = self.conn.getresponse()
                data = r.read()
                break
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError,
                    http.client.CannotSendRequest):
                self.conn.close()
                self.conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
                if attempt:
                    raise
        if r.status != 200:
            raise CHError(f"HTTP {r.status}: {data[:2000].decode(errors='replace')}\n-- {sql[:500]}")
        return data

    def exec(self, sql: str, **settings) -> None:
        s = {"wait_end_of_query": 1, **settings}
        self.raw(sql, "&" + urllib.parse.urlencode(s))

    def rows(self, sql: str, **settings) -> list[list]:
        s = {"output_format_json_quote_64bit_integers": 0, **settings}
        data = self.raw(sql + " FORMAT JSONCompactEachRow", "&" + urllib.parse.urlencode(s))
        return [json.loads(line) for line in data.splitlines() if line]

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class ClickHouseBase(Solution):
    engine = "clickhouse"
    image = CH_IMAGE

    @property
    def container(self) -> str:
        return f"b2d-{self.name}"

    def setup(self) -> None:
        d = du.data_dir(self.name)
        (d / "data").mkdir()
        (d / "conf").mkdir()
        (d / "log").mkdir()
        (d / "conf" / "config.xml").write_text(config_xml())
        du.sh("chown", "-R", "101:101", str(d / "data"), str(d / "log"))
        du.run(self.container, self.image,
               mounts={str(d / "data"): "/var/lib/clickhouse",
                       str(d / "log"): "/var/log/clickhouse-server",
                       str(d / "conf" / "config.xml"): "/etc/clickhouse-server/config.d/docker_related_config.xml:ro",
                       str(spec.DATA_ROOT): "/bench_data:ro"},
               env={"CLICKHOUSE_SKIP_USER_SETUP": "1", "CLICKHOUSE_DO_NOT_CHOWN": "1"})

        def ready():
            with CHHttp(timeout=3) as c:
                return c.rows("SELECT 1") == [[1]]

        du.wait_until(ready, timeout=120, what=self.container)

    def teardown(self) -> None:
        du.rm(self.container)
        du.data_dir(self.name, fresh=True).rmdir()

    def data_path(self) -> Path | None:
        return spec.WORK_ROOT / self.name / "data"
