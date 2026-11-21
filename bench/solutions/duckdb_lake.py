"""DuckDB "lakehouse" solutions: do not load into a database server at all.

Two solutions, both server-less (no container) and both built on the same idea:
a refresh *publishes a new immutable snapshot* next to the old one and then
switches a pointer file atomically; application processes embed DuckDB, poll
the pointer and move to the new snapshot between two queries.

    duckdb_parquet   snapshot = sorted, zstd Parquet files + a JSON manifest
                     (the proxy for S3 + DuckDB-in-the-app-tier / Athena)
    duckdb_native    snapshot = one DuckDB database file

There is no B-tree anywhere. The two access paths are served by *sort order +
min/max pruning*:

    path A  (tenant_id, item_key, period)  the table itself is sorted by it
    path B  (ref_path, period)             a second, narrow, sorted projection
                                           (ref_path, period, tenant_id, item_key,
                                            quantity, price, score) - the columnar
                                           equivalent of a covering index whose
                                           "pointer" is the business key

See docs/notes/duckdb.md for the variants that were measured and rejected.
"""
from __future__ import annotations

import bisect
import json
import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Sequence

import duckdb

from .. import spec
from . import register
from .base import Client, Dataset, Phases, Solution

# --------------------------------------------------------------------------- configuration
LOAD_THREADS = 4
LOAD_MEMORY = "8GB"            # memory_limit of the (short-lived) loader connection
SERVE_THREADS = int(os.environ.get("B2D_DUCK_SERVE_THREADS", "4"))
SERVE_MEMORY = "2GB"           # memory_limit of the long-lived in-process serving instance

# Sorting is done in passes that fit comfortably in RAM instead of one external
# sort of the whole table: a pass re-reads the source with a pushed-down range
# filter, which is cheaper than spilling ~9 GB of rows to disk and back.
CHUNK_ROWS_A = int(os.environ.get("B2D_DUCK_CHUNK_A", 6_000_000))     # full-width rows per sort pass
CHUNK_ROWS_B = int(os.environ.get("B2D_DUCK_CHUNK_B", 10_000_000))    # narrow projection rows per sort pass

KEY_ORDER = "tenant_id, item_key, period"
B_COLS = "ref_path, period, tenant_id, item_key, quantity, price, score"
B_ORDER = "ref_path, period"
SELECT_COLS = ", ".join(spec.COLUMN_NAMES)

DDL_A = """CREATE TABLE {name} (
    id BIGINT NOT NULL, tenant_id INTEGER NOT NULL, period INTEGER NOT NULL,
    quantity DOUBLE NOT NULL, price DOUBLE NOT NULL, score DOUBLE NOT NULL,
    item_key VARCHAR NOT NULL, ref_path VARCHAR NOT NULL, description VARCHAR NOT NULL)"""
DDL_B = """CREATE TABLE {name} (
    ref_path VARCHAR NOT NULL, period INTEGER NOT NULL, tenant_id INTEGER NOT NULL, item_key VARCHAR NOT NULL,
    quantity DOUBLE NOT NULL, price DOUBLE NOT NULL, score DOUBLE NOT NULL)"""


def _lit(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _files_sql(files: Sequence[str]) -> str:
    return "[" + ", ".join(_lit(f) for f in files) + "]"


def _loader(tmp: Path, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database)
    con.execute(f"SET threads={LOAD_THREADS}; SET memory_limit='{LOAD_MEMORY}'; "
                f"SET temp_directory={_lit(tmp)}; SET preserve_insertion_order=true")
    return con


def _fsync_path(p: Path) -> None:
    fd = os.open(p, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write(path: Path, text: str) -> None:
    """Durable, atomic replace of a small file (the POSIX stand-in for an S3 PUT)."""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_path(path.parent)


def _tenant_counts(con, src: str) -> list[tuple[int, int]]:
    return con.execute(f"SELECT tenant_id, count(*) FROM {src} GROUP BY 1 ORDER BY 1").fetchall()


def _pack(counts: list[tuple[int, int]], max_rows: int) -> list[tuple[int, int, int]]:
    """Greedily pack consecutive tenants into ranges of at most max_rows rows
    (a single tenant larger than that gets a range of its own). -> [(lo, hi, rows)]"""
    out: list[list[int]] = []
    for t, c in counts:
        if out and out[-1][2] + c <= max_rows:
            out[-1][1] = t
            out[-1][2] += c
        else:
            out.append([t, t, c])
    return [tuple(x) for x in out]


def _string_cuts(con, rel: str, col: str, parts: int) -> list[str]:
    """Approximate quantile cut points of a string column (from a block sample)."""
    if parts <= 1:
        return []
    qs = ", ".join(f"{k / parts:.6f}" for k in range(1, parts))
    row = con.execute(f"SELECT quantile_disc({col}, [{qs}]) FROM "
                      f"(SELECT {col} FROM {rel} USING SAMPLE 200000 ROWS)").fetchone()[0]
    return sorted(set(row or []))


def _range_preds(col: str, cuts: list[str]) -> list[str]:
    if not cuts:
        return ["TRUE"]
    preds = [f"{col} <= {_lit(cuts[0])}"]
    preds += [f"{col} > {_lit(a)} AND {col} <= {_lit(b)}" for a, b in zip(cuts, cuts[1:])]
    preds.append(f"{col} > {_lit(cuts[-1])}")
    return preds


class _Pointer:
    """Cheap change detection of the CURRENT pointer file (one stat() per query)."""

    def __init__(self, path: Path):
        self.path = path

    def sig(self):
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            return None
        return (st.st_ino, st.st_mtime_ns)

    def read(self) -> str:
        return self.path.read_text().strip()


class LakeBase(Solution):
    engine = "duckdb"
    atomic_swap = True

    def __init__(self):
        self.root = spec.WORK_ROOT / self.name
        self._lock = threading.Lock()
        self._serve: duckdb.DuckDBPyConnection | None = None

    # ---- lifecycle ----------------------------------------------------------
    def setup(self) -> None:
        self._close_serving()
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True)
        self._open_serving()

    def teardown(self) -> None:
        self._close_serving()
        shutil.rmtree(self.root, ignore_errors=True)

    def data_path(self) -> Path | None:
        return self.root

    @property
    def pointer(self) -> _Pointer:
        return _Pointer(self.root / "CURRENT")

    def _open_serving(self) -> None:
        """The long-lived DuckDB instance of the application process. Every
        worker thread gets its own cursor (= connection) on it."""
        self._serve = duckdb.connect(":memory:")
        self._serve.execute(f"SET threads={SERVE_THREADS}; SET memory_limit='{SERVE_MEMORY}'; "
                            f"SET parquet_metadata_cache=true; SET temp_directory={_lit(self.root / 'tmp_serve')}")

    def _close_serving(self) -> None:
        if self._serve is not None:
            try:
                self._serve.close()
            except Exception:  # noqa: BLE001
                pass
            self._serve = None

    def _cursor(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            if self._serve is None:
                self._open_serving()
            return self._serve.cursor()

    def _next_gen(self) -> int:
        gens = [int(p.name.split("-")[1].split(".")[0]) for p in self.root.glob("gen-*")]
        return max(gens, default=0) + 1

    def verify_row(self) -> tuple:
        c = self.client()
        try:
            return c.verify()
        finally:
            c.close()

    def info(self) -> dict:
        return {"duckdb_version": duckdb.__version__, "server": "none (embedded, in-process)",
                "load": {"threads": LOAD_THREADS, "memory_limit": LOAD_MEMORY,
                         "sort_pass_rows_a": CHUNK_ROWS_A, "sort_pass_rows_b": CHUNK_ROWS_B},
                "serve": {"threads": SERVE_THREADS, "memory_limit": SERVE_MEMORY,
                          "model": "one DuckDB instance per process, one cursor per worker thread, "
                                   "CURRENT pointer checked with stat() before every query"},
                "durability": "new files are fsync'ed before the pointer is switched (rename)"}


# =========================================================================== duckdb_parquet
PQ_CODEC = os.environ.get("B2D_DUCK_CODEC", "zstd")
PQ_RG_A = int(os.environ.get("B2D_DUCK_RG_A", 2048))             # rows per row group, table files
PQ_RG_PER_FILE_A = int(os.environ.get("B2D_DUCK_RGPF_A", 32))    # row groups per table file
PQ_RG_B = int(os.environ.get("B2D_DUCK_RG_B", 8192))             # rows per row group, projection files
PQ_RG_PER_FILE_B = int(os.environ.get("B2D_DUCK_RGPF_B", 16))    # row groups per projection file
PQ_GROUPS = int(os.environ.get("B2D_DUCK_GROUPS", 64))           # target number of tenant groups (rewrite units)
PQ_MIN_GROUP_ROWS = 20_000
PQ_WRITERS = 4                                                   # concurrent COPY statements when cutting files


class ParquetSnapshot:
    """An immutable published state: which files make up the table and the
    projection, and the first key of every file (the manifest of a table
    format such as Iceberg/Delta carries the same information)."""

    def __init__(self, root: Path, manifest: dict):
        self.gen: int = manifest["gen"]
        self.group_lo: list[int] = [g["lo"] for g in manifest["groups"]]     # sorted tenant ranges
        self.g_files: list[list[str]] = [[str(root / f["path"]) for f in g["files"]] for g in manifest["groups"]]
        self.g_los: list[list[tuple]] = [[tuple(f["lo"]) for f in g["files"]] for g in manifest["groups"]]
        self.a_files = [f for fs in self.g_files for f in fs]
        self.b_lo: list[str] = [f["lo"] for f in manifest["b"]]              # sorted by first ref_path
        self.b_files = [str(root / f["path"]) for f in manifest["b"]]
        self._sql: dict[Any, str] = {}
        self.t_point = (f"SELECT {SELECT_COLS} FROM read_parquet(%s) "
                        "WHERE tenant_id = ? AND item_key = ? AND period = ?")
        self.t_series = ("SELECT period, quantity, price, score FROM read_parquet(%s) "
                         "WHERE ref_path = ? AND period BETWEEN ? AND ? ORDER BY period")
        self.t_tenant = ("SELECT period, count(*), sum(quantity * price), avg(score) FROM read_parquet(%s) "
                         "WHERE tenant_id = ? GROUP BY period ORDER BY period")
        self.full_agg = ("SELECT tenant_id, count(*) AS cnt, sum(quantity), avg(price) "
                         f"FROM read_parquet({_files_sql(self.a_files)}) "
                         "GROUP BY tenant_id ORDER BY cnt DESC, tenant_id ASC LIMIT 20")
        self.verify = (f"SELECT count(*), sum(quantity), sum(price), max(id) "
                       f"FROM read_parquet({_files_sql(self.a_files)})")

    def _cached(self, template: str, files: list[str]) -> str | None:
        if not files:
            return None
        key = (template, files[0], len(files))
        sql = self._sql.get(key)
        if sql is None:
            sql = self._sql[key] = template % _files_sql(files)
        return sql

    # ---- client-side pruning: tenant -> group -> file(s) ----------------------
    def point_sql(self, tenant_id: int, item_key: str, period: int) -> str | None:
        g = max(0, bisect.bisect_right(self.group_lo, tenant_id) - 1)
        # files are consecutive runs of one sort: file i holds keys [lo_i, lo_{i+1})
        i = bisect.bisect_right(self.g_los[g], (tenant_id, item_key, period)) - 1
        return self._cached(self.t_point, self.g_files[g][i:i + 1]) if i >= 0 else None

    def tenant_sql(self, tenant_id: int) -> str | None:
        g = max(0, bisect.bisect_right(self.group_lo, tenant_id) - 1)
        los = self.g_los[g]
        i0 = max(bisect.bisect_left(los, (tenant_id,)) - 1, 0)
        i1 = bisect.bisect_left(los, (tenant_id + 1,))
        return self._cached(self.t_tenant, self.g_files[g][i0:i1])

    def series_sql(self, ref_path: str) -> str | None:
        # the 20 periods of one ref_path may straddle a file boundary
        i0 = max(bisect.bisect_left(self.b_lo, ref_path) - 1, 0)
        i1 = bisect.bisect_right(self.b_lo, ref_path)
        return self._cached(self.t_series, self.b_files[i0:i1])


class ParquetClient(Client):
    def __init__(self, sol: "DuckdbParquet"):
        self.sol = sol
        self.con = sol._cursor()
        self.pointer = sol.pointer
        self.sig = None
        self.snap: ParquetSnapshot | None = None

    def _snapshot(self) -> ParquetSnapshot:
        sig = self.pointer.sig()
        if sig != self.sig or self.snap is None:
            self.snap = self.sol.load_snapshot()
            self.sig = sig
        return self.snap

    def query(self, name: str, params: Sequence[Any]) -> list[tuple]:
        s = self._snapshot()
        if name == "point":
            sql = s.point_sql(*params)
        elif name == "series":
            sql = s.series_sql(params[0])
        elif name == "tenant_agg":
            sql = s.tenant_sql(params[0])
        else:
            sql = s.full_agg
        if sql is None:
            return []
        return self.con.execute(sql, params).fetchall()

    def verify(self) -> tuple:
        return self.con.execute(self._snapshot().verify).fetchone()

    def close(self):
        self.con.close()


@register
class DuckdbParquet(LakeBase):
    name = "duckdb_parquet"
    strategy = ("publish immutable sorted zstd Parquet snapshot (table by business key + narrow projection "
                "by ref_path, small row groups, manifest with file key bounds), atomic pointer switch; "
                "DuckDB in-process")

    def __init__(self):
        super().__init__()
        self._snap_cache: tuple[Any, ParquetSnapshot] | None = None

    # ---- snapshot access ----------------------------------------------------
    def load_snapshot(self) -> ParquetSnapshot:
        with self._lock:
            sig = self.pointer.sig()
            if self._snap_cache is not None and self._snap_cache[0] == sig:
                return self._snap_cache[1]
            snap = ParquetSnapshot(self.root, json.loads((self.root / self.pointer.read()).read_text()))
            self._snap_cache = (sig, snap)
            return snap

    def _current_manifest(self) -> dict:
        return json.loads((self.root / self.pointer.read()).read_text())

    def client(self) -> Client:
        return ParquetClient(self)

    # ---- writers ------------------------------------------------------------
    def _new_gen_dir(self) -> tuple[int, Path]:
        gen = self._next_gen()
        gen_dir = self.root / "data" / f"g{gen:06d}"
        shutil.rmtree(gen_dir, ignore_errors=True)     # leftovers of a crashed, unpublished attempt
        (gen_dir / "a").mkdir(parents=True)
        (gen_dir / "b").mkdir(parents=True)
        return gen, gen_dir

    def _cut(self, con, table: str, order_cols: str, spans: list[tuple[int, int, str]], out_dir: Path,
             rg: int, file_rows: int) -> dict[str, list[dict]]:
        """Write row-id ranges of a sorted in-memory table as Parquet files of at
        most `file_rows` rows: one COPY per file, so file boundaries, order and
        first keys are exact. spans = [(first_rowid, last_rowid, name)].
        -> {name: [{"path", "lo", "rows", "bytes"}, ...]} in key order."""
        jobs = []
        for a, b, name in spans:
            n = b - a + 1
            pieces = -(-n // file_rows)
            size = -(-n // pieces)
            for i in range(pieces):
                s = a + i * size
                jobs.append((name, s, min(s + size - 1, b), out_dir / f"{name}-{i:04d}.parquet"))
        if not jobs:
            return {}
        starts = ", ".join(str(j[1]) for j in jobs)
        firsts = {r[0]: list(r[1:]) for r in con.execute(
            f"SELECT rowid, {order_cols} FROM {table} WHERE rowid IN ({starts})").fetchall()}
        opts = f"FORMAT parquet, COMPRESSION {PQ_CODEC}, ROW_GROUP_SIZE {rg}"

        def write(job):
            _, s, e, path = job
            cur = con.cursor()
            try:
                cur.execute(f"COPY (SELECT * FROM {table} WHERE rowid BETWEEN {s} AND {e}) TO {_lit(path)} ({opts})")
            finally:
                cur.close()

        with ThreadPoolExecutor(PQ_WRITERS) as ex:
            list(ex.map(write, jobs))
        out: dict[str, list[dict]] = {}
        for name, s, e, path in jobs:
            out.setdefault(name, []).append({"path": str(path.relative_to(self.root)), "lo": firsts[s],
                                             "rows": e - s + 1, "bytes": path.stat().st_size})
        return out

    def _write_pass_a(self, con, select_sql: str, group_lo: list[int], gen_dir: Path, tag: str) -> dict[int, list]:
        """Sort one memory-sized slice of the table and cut it into files, never
        letting a file cross a tenant-group boundary. -> {group: [file entries]}"""
        con.execute(f"CREATE OR REPLACE TABLE pass_a AS SELECT {SELECT_COLS} FROM ({select_sql}) "
                    f"ORDER BY {KEY_ORDER}")
        gspan: dict[int, list[int]] = {}
        for t, a, b in con.execute("SELECT tenant_id, min(rowid), max(rowid) FROM pass_a GROUP BY 1").fetchall():
            g = max(0, bisect.bisect_right(group_lo, t) - 1)
            s = gspan.setdefault(g, [a, b])
            s[0], s[1] = min(s[0], a), max(s[1], b)
        files = self._cut(con, "pass_a", KEY_ORDER, [(a, b, f"{tag}-grp{g:04d}") for g, (a, b) in sorted(gspan.items())],
                          gen_dir / "a", PQ_RG_A, PQ_RG_A * PQ_RG_PER_FILE_A)
        con.execute("DROP TABLE pass_a")
        return {int(name.rsplit("grp", 1)[1]): fs for name, fs in files.items()}

    def _write_b(self, con, a_files: list[str], rows: int, gen_dir: Path, phases: Phases) -> list[dict]:
        """(Re)build the projection sorted by (ref_path, period) from the table files."""
        if not a_files:
            return []
        src = f"read_parquet({_files_sql(a_files)})"
        with phases("b_plan"):
            # range boundaries of the sort passes: quantiles of an evenly spaced subset of files
            sample = a_files[::max(1, len(a_files) // 24)]
            cuts = _string_cuts(con, f"read_parquet({_files_sql(sample)})", "ref_path", -(-rows // CHUNK_ROWS_B))
        out: list[dict] = []
        for k, pred in enumerate(_range_preds("ref_path", cuts)):
            with phases("b_sort"):
                con.execute(f"CREATE OR REPLACE TABLE pass_b AS SELECT {B_COLS} FROM {src} WHERE {pred} "
                            f"ORDER BY {B_ORDER}")
                n = con.execute("SELECT count(*) FROM pass_b").fetchone()[0]
            with phases("b_write"):
                if n:
                    fs = self._cut(con, "pass_b", "ref_path", [(0, n - 1, f"r{k:02d}")], gen_dir / "b",
                                   PQ_RG_B, PQ_RG_B * PQ_RG_PER_FILE_B)
                    out += [{**f, "lo": f["lo"][0]} for f in fs[f"r{k:02d}"]]
                con.execute("DROP TABLE pass_b")
        phases.note("b_sort_passes", len(cuts) + 1)
        phases.note("b_files", len(out))
        return out

    def _publish(self, gen: int, groups: list[dict], b: list[dict], new_dir: Path, phases: Phases) -> None:
        manifest = {"gen": gen, "rows": sum(g["rows"] for g in groups), "groups": groups, "b": b,
                    "layout": {"codec": PQ_CODEC, "row_group_a": PQ_RG_A, "row_group_b": PQ_RG_B}}
        with phases("fsync"):
            for d, _, fs in os.walk(new_dir):
                for f in fs:
                    _fsync_path(Path(d) / f)
                _fsync_path(Path(d))
        with phases("publish"):
            name = f"gen-{gen:06d}.manifest.json"
            _atomic_write(self.root / name, json.dumps(manifest))
            prev = self.pointer.read() if self.pointer.sig() is not None else None
            _atomic_write(self.root / "CURRENT", name)          # <- the atomic switch
        with phases("gc"):
            self._gc(keep={name, prev})

    def _gc(self, keep: set) -> None:
        """Delete everything not referenced by the current and the previous
        manifest (the previous one is kept for queries that are still running)."""
        live: set[str] = set()
        for m in self.root.glob("gen-*.manifest.json"):
            if m.name in keep:
                man = json.loads(m.read_text())
                live |= {f["path"] for g in man["groups"] for f in g["files"]} | {f["path"] for f in man["b"]}
            else:
                m.unlink()
        data = self.root / "data"
        for d, _, fs in os.walk(data, topdown=False):
            for f in fs:
                p = Path(d) / f
                if str(p.relative_to(self.root)) not in live:
                    p.unlink()
            if not os.listdir(d) and Path(d) != data:
                os.rmdir(d)

    # ---- timed operations ---------------------------------------------------
    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        gen, gen_dir = self._new_gen_dir()
        src = f"read_parquet({_files_sql([str(f) for f in ds.base_files])})"
        con = _loader(self.root / "tmp")
        try:
            with phases("plan"):
                counts = _tenant_counts(con, src)
                rows = sum(c for _, c in counts)
                ranges = _pack(counts, max(PQ_MIN_GROUP_ROWS, rows // PQ_GROUPS))
                # tenant groups = units of later copy-on-write; every tenant id (also one
                # that does not exist yet) maps to exactly one group
                groups = [{"lo": (lo if g else -2**31), "rows": n, "files": []} for g, (lo, _, n) in enumerate(ranges)]
                group_lo = [g["lo"] for g in groups]
                passes = _pack([(g, grp["rows"]) for g, grp in enumerate(groups)], CHUNK_ROWS_A)
            with phases("a_sort_write"):
                for k, (g0, g1, _) in enumerate(passes):
                    where = "" if len(passes) == 1 else \
                        f" WHERE tenant_id BETWEEN {ranges[g0][0]} AND {ranges[g1][1]}"
                    for g, files in self._write_pass_a(con, f"SELECT * FROM {src}{where}", group_lo, gen_dir,
                                                       f"p{k:02d}").items():
                        groups[g]["files"] = files
            phases.note("a_sort_passes", len(passes))
            phases.note("a_files", sum(len(g["files"]) for g in groups))
            b = self._write_b(con, [str(self.root / f["path"]) for g in groups for f in g["files"]], rows,
                              gen_dir, phases)
        finally:
            con.close()
        self._publish(gen, groups, b, gen_dir, phases)

    def _maintain(self, affected: list[int], old_filter: str, new_rows_sql: str, con, phases: Phases) -> None:
        """Copy-on-write: rewrite the tenant groups in `affected` from their old
        rows that pass `old_filter` plus `new_rows_sql` (restricted to the group's
        tenant range); all other groups keep their files. The projection has a
        different sort order, so it is rebuilt completely."""
        man = self._current_manifest()
        gen, gen_dir = self._new_gen_dir()
        groups = [dict(g) for g in man["groups"]]
        group_lo = [g["lo"] for g in groups]
        hi = group_lo[1:] + [2**31]
        with phases("a_rewrite"):
            for k, (g0, g1, _) in enumerate(_pack([(g, groups[g]["rows"]) for g in affected], CHUNK_ROWS_A)):
                batch = [g for g in affected if g0 <= g <= g1]
                in_batch = " OR ".join(f"(tenant_id >= {group_lo[g]} AND tenant_id < {hi[g]})" for g in batch)
                parts = [f"SELECT {SELECT_COLS} FROM ({new_rows_sql}) n WHERE {in_batch}"]
                old = [str(self.root / f["path"]) for g in batch for f in groups[g]["files"]]
                if old:
                    parts.append(f"SELECT {SELECT_COLS} FROM read_parquet({_files_sql(old)}) o WHERE {old_filter}")
                written = self._write_pass_a(con, " UNION ALL ".join(parts), group_lo, gen_dir, f"p{k:02d}")
                for g in batch:
                    groups[g]["files"] = written.get(g, [])
                    groups[g]["rows"] = sum(f["rows"] for f in groups[g]["files"])
        phases.note("groups_rewritten", len(affected))
        phases.note("groups_total", len(groups))
        phases.note("a_files_rewritten", sum(len(groups[g]["files"]) for g in affected))
        phases.note("a_files_total", sum(len(g["files"]) for g in groups))
        b = self._write_b(con, [str(self.root / f["path"]) for g in groups for f in g["files"]],
                          sum(g["rows"] for g in groups), gen_dir, phases)
        self._publish(gen, groups, b, gen_dir, phases)

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        group_lo = [g["lo"] for g in self._current_manifest()["groups"]]
        con = _loader(self.root / "tmp")
        try:
            with phases("stage"):
                con.execute(f"CREATE TABLE delta AS SELECT {SELECT_COLS} FROM read_parquet({_lit(delta)})")
                tenants = [t for (t,) in con.execute("SELECT DISTINCT tenant_id FROM delta").fetchall()]
            affected = sorted({max(0, bisect.bisect_right(group_lo, t) - 1) for t in tenants})
            keep_old = ("NOT EXISTS (SELECT 1 FROM delta d WHERE d.tenant_id = o.tenant_id "
                        "AND d.item_key = o.item_key AND d.period = o.period)")
            self._maintain(affected, keep_old, "SELECT * FROM delta", con, phases)
        finally:
            con.close()

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        group_lo = [g["lo"] for g in self._current_manifest()["groups"]]
        t = int(ds.replace_tenant_id)
        con = _loader(self.root / "tmp")
        try:
            self._maintain([max(0, bisect.bisect_right(group_lo, t) - 1)], f"o.tenant_id <> {t}",
                           f"SELECT * FROM read_parquet({_lit(ds.replace_tenant_file)})", con, phases)
        finally:
            con.close()

    # ---- metadata -----------------------------------------------------------
    def storage(self) -> dict:
        man = self._current_manifest()
        a = sum(f["bytes"] for g in man["groups"] for f in g["files"])
        b = sum(f["bytes"] for f in man["b"])
        return {"table": a, "index_a": 0, "index_b": b, "total": a + b,
                "files_a": sum(len(g["files"]) for g in man["groups"]), "files_b": len(man["b"]),
                "tenant_groups": len(man["groups"]),
                "note": "index_a = sort order of the table files (no extra bytes); index_b = sorted projection; "
                        "data_dir_bytes also contains the previous snapshot, kept until the next publish"}

    def info(self) -> dict:
        i = super().info()
        i["parquet"] = {"codec": PQ_CODEC, "row_group_rows_a": PQ_RG_A, "row_groups_per_file_a": PQ_RG_PER_FILE_A,
                        "row_group_rows_b": PQ_RG_B, "row_groups_per_file_b": PQ_RG_PER_FILE_B,
                        "target_tenant_groups": PQ_GROUPS, "projection_b_columns": B_COLS,
                        "parquet_metadata_cache": True}
        return i


# =========================================================================== duckdb_native
class NativeClient(Client):
    def __init__(self, sol: "DuckdbNative"):
        self.sol = sol
        self.con = sol._cursor()
        self.pointer = sol.pointer
        self.sig = None
        self.q: dict[str, str] = {}

    def _queries(self) -> dict[str, str]:
        sig = self.pointer.sig()
        if sig != self.sig or not self.q:
            alias = self.sol.attach_current()
            a, b = f"{alias}.big_table", f"{alias}.big_table_b"
            self.q = {
                "point": f"SELECT {SELECT_COLS} FROM {a} WHERE tenant_id = ? AND item_key = ? AND period = ?",
                "series": f"SELECT period, quantity, price, score FROM {b} "
                          "WHERE ref_path = ? AND period BETWEEN ? AND ? ORDER BY period",
                "tenant_agg": f"SELECT period, count(*), sum(quantity * price), avg(score) FROM {a} "
                              "WHERE tenant_id = ? GROUP BY period ORDER BY period",
                "full_agg": f"SELECT tenant_id, count(*) AS cnt, sum(quantity), avg(price) FROM {a} "
                            "GROUP BY tenant_id ORDER BY cnt DESC, tenant_id ASC LIMIT 20",
                "verify": f"SELECT count(*), sum(quantity), sum(price), max(id) FROM {a}",
            }
            self.sig = sig
        return self.q

    def query(self, name: str, params: Sequence[Any]) -> list[tuple]:
        return self.con.execute(self._queries()[name], params).fetchall()

    def verify(self) -> tuple:
        return self.con.execute(self._queries()["verify"]).fetchone()

    def close(self):
        self.con.close()


@register
class DuckdbNative(LakeBase):
    name = "duckdb_native"
    strategy = ("build a new DuckDB database file (table sorted by business key + projection sorted by "
                "ref_path, zone maps only, no ART), switch pointer atomically; read-only attach in-process")

    def __init__(self):
        super().__init__()
        self._attached: dict[str, str] = {}     # file name -> alias
        self._current: tuple[Any, str] | None = None

    def setup(self) -> None:
        self._attached, self._current = {}, None
        super().setup()

    def client(self) -> Client:
        return NativeClient(self)

    def attach_current(self) -> str:
        """Attach the database file CURRENT points to (read-only) and return its
        alias. The previous generation stays attached until the one after it
        arrives, so queries that are still running on it finish normally."""
        with self._lock:
            sig = self.pointer.sig()
            if self._current is not None and self._current[0] == sig:
                return self._current[1]
            fname = self.pointer.read()
            alias = self._attached.get(fname)
            if alias is None:
                alias = "g" + fname.split("-")[1].split(".")[0]
                cur = self._serve.cursor()
                cur.execute(f"ATTACH {_lit(self.root / fname)} AS {alias} (READ_ONLY)")
                for old_name, old_alias in list(self._attached.items()):
                    if self._current is None or old_alias != self._current[1]:
                        try:
                            cur.execute(f"DETACH {old_alias}")
                        except Exception:  # noqa: BLE001  (still in use: retry at the next switch)
                            continue
                        del self._attached[old_name]
                cur.close()
                self._attached[fname] = alias
            self._current = (sig, alias)
            return alias

    def _publish(self, building: Path, gen: int, phases: Phases) -> None:
        with phases("publish"):
            name = f"gen-{gen:06d}.duckdb"
            _fsync_path(building)
            os.replace(building, self.root / name)
            prev = self.pointer.read() if self.pointer.sig() is not None else None
            _atomic_write(self.root / "CURRENT", name)          # <- the atomic switch
            for p in self.root.glob("gen-*.duckdb*"):
                if p.name not in (name, prev):
                    p.unlink()                                   # open readers keep their inode

    def _building(self, gen: int) -> Path:
        p = self.root / f"build-{gen:06d}.duckdb"
        for f in self.root.glob("build-*"):
            f.unlink()
        return p

    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        gen = self._next_gen()
        path = self._building(gen)
        src = f"read_parquet({_files_sql([str(f) for f in ds.base_files])})"
        con = _loader(self.root / "tmp", str(path))
        try:
            with phases("plan"):
                counts = _tenant_counts(con, src)
                rows = sum(c for _, c in counts)
                passes = _pack(counts, CHUNK_ROWS_A)
            with phases("a_sort_insert"):
                con.execute(DDL_A.format(name="big_table"))
                for lo, hi, _ in passes:
                    con.execute(f"INSERT INTO big_table SELECT {SELECT_COLS} FROM {src} "
                                f"WHERE tenant_id BETWEEN {lo} AND {hi} ORDER BY {KEY_ORDER}")
            phases.note("a_sort_passes", len(passes))
            with phases("b_plan"):
                cuts = _string_cuts(con, "big_table", "ref_path", -(-rows // CHUNK_ROWS_B))
            with phases("b_sort_insert"):
                con.execute(DDL_B.format(name="big_table_b"))
                for pred in _range_preds("ref_path", cuts):
                    con.execute(f"INSERT INTO big_table_b SELECT {B_COLS} FROM big_table WHERE {pred} "
                                f"ORDER BY {B_ORDER}")
            phases.note("b_sort_passes", len(cuts) + 1)
            with phases("checkpoint"):
                con.execute("CHECKPOINT")
        finally:
            con.close()
        self._publish(path, gen, phases)

    def _copy_current(self, phases: Phases) -> tuple[int, Path]:
        """DuckDB files are single-writer and the serving processes hold the
        current file open read-only, so maintenance runs on a private copy."""
        gen = self._next_gen()
        path = self._building(gen)
        with phases("copy_file"):
            shutil.copyfile(self.root / self.pointer.read(), path)
        return gen, path

    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        gen, path = self._copy_current(phases)
        con = _loader(self.root / "tmp", str(path))
        on = "t.tenant_id = d.tenant_id AND t.item_key = d.item_key AND t.period = d.period"
        try:
            with phases("stage"):
                con.execute(f"CREATE TEMP TABLE delta AS SELECT {SELECT_COLS} FROM read_parquet({_lit(delta)})")
            con.execute("BEGIN")
            with phases("merge_a"):
                con.execute(f"DELETE FROM big_table t USING delta d WHERE {on}")
                con.execute(f"INSERT INTO big_table SELECT * FROM delta ORDER BY {KEY_ORDER}")
            with phases("merge_b"):
                con.execute(f"DELETE FROM big_table_b t USING delta d WHERE {on}")
                con.execute(f"INSERT INTO big_table_b SELECT {B_COLS} FROM delta ORDER BY {B_ORDER}")
            with phases("commit_checkpoint"):
                con.execute("COMMIT")
                con.execute("CHECKPOINT")
        finally:
            con.close()
        self._publish(path, gen, phases)

    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        gen, path = self._copy_current(phases)
        con = _loader(self.root / "tmp", str(path))
        t = int(ds.replace_tenant_id)
        src = f"read_parquet({_lit(ds.replace_tenant_file)})"
        try:
            con.execute("BEGIN")
            with phases("replace_a"):
                con.execute(f"DELETE FROM big_table WHERE tenant_id = {t}")
                con.execute(f"INSERT INTO big_table SELECT {SELECT_COLS} FROM {src} ORDER BY {KEY_ORDER}")
            with phases("replace_b"):
                con.execute(f"DELETE FROM big_table_b WHERE tenant_id = {t}")
                con.execute(f"INSERT INTO big_table_b SELECT {B_COLS} FROM {src} ORDER BY {B_ORDER}")
            with phases("commit_checkpoint"):
                con.execute("COMMIT")
                con.execute("CHECKPOINT")
        finally:
            con.close()
        self._publish(path, gen, phases)

    def storage(self) -> dict:
        fname = self.pointer.read()
        total = (self.root / fname).stat().st_size
        alias = self.attach_current()
        cur = self._cursor()
        try:
            blk = dict(cur.execute(
                f"SELECT table_name, count(DISTINCT block_id) FROM pragma_storage_info('{alias}.big_table') "
                f"WHERE block_id IS NOT NULL GROUP BY 1 UNION ALL "
                f"SELECT table_name, count(DISTINCT block_id) FROM pragma_storage_info('{alias}.big_table_b') "
                f"WHERE block_id IS NOT NULL GROUP BY 1").fetchall()) if False else {}
            bs, used, free = cur.execute(
                f"SELECT block_size, used_blocks, free_blocks FROM pragma_database_size() "
                f"WHERE database_name = '{alias}'").fetchone()
        finally:
            cur.close()
        return {"total": total, "used_bytes": bs * used, "free_bytes": bs * free, "index_a": 0, **blk,
                "note": "one database file holding big_table (sorted) and big_table_b (projection); "
                        "data_dir_bytes also contains the previous snapshot, kept until the next publish"}

    def info(self) -> dict:
        i = super().info()
        i["native"] = {"indexes": "none (zone maps on sorted data); ART measured and rejected, see notes",
                       "projection_b_columns": B_COLS, "maintenance": "copy file, SQL in the copy, switch pointer"}
        return i
