"""Benchmark runner: drives one solution through the full protocol at one scale.

    python -m bench.run --list
    python -m bench.run --solution pg_truncate_copy --scale 1M
    python -m bench.run --solution pg_truncate_copy --scale 20M --reps 3 --tag final

Protocol (identical for every solution, see PLAN.md):

    setup            start engine from an empty data dir                       (untimed)
    initial_load     full_refresh into the empty engine                        (timed)
    verify           count / checksums must equal the generator's manifest
    storage          engine-reported and on-disk bytes
    queries          4 logical queries, latency percentiles, answers verified
    concurrency      8 closed-loop clients, point+series mix, QPS
    refresh x(reps-1) full_refresh over the LIVE table while a reader polls it  (timed, availability)
    upsert 1%        75% updates + 25% inserts, scattered                      (timed, verified)
    upsert 10%       same                                                      (timed, verified)
    replace_tenant   swap the complete data of the largest tenant (~5%)        (timed, verified)
    queries_after    point/series latency after the maintenance operations
    storage_after    bloat left behind by maintenance
    teardown

Runs at scale >= 5M take a machine-wide lock so that timings never overlap.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import platform
import shutil
import statistics
import threading
import time
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psutil

from . import datagen, spec
from .solutions import load_all
from .solutions.base import Dataset, Phases, Solution

ALL_STEPS = ["initial_load", "queries", "concurrency", "refresh", "incremental", "queries_after"]
HEAVY_ROWS = 5_000_000
LOCK_FILE = spec.WORK_ROOT.parent / ".heavy.lock"


# --------------------------------------------------------------------------- utils
def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = min(len(s) - 1, max(0, math.ceil(p / 100 * len(s)) - 1))
    return s[k]


def lat_summary(ms: list[float]) -> dict:
    if not ms:
        return {"n": 0}
    return {"n": len(ms), "p50_ms": round(pct(ms, 50), 3), "p95_ms": round(pct(ms, 95), 3),
            "p99_ms": round(pct(ms, 99), 3), "max_ms": round(max(ms), 3),
            "mean_ms": round(statistics.fmean(ms), 3)}


def host_info() -> dict:
    model = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    return {"cpus": os.cpu_count(), "cpu_model": model,
            "mem_gb": round(psutil.virtual_memory().total / 2**30, 1),
            "kernel": platform.release(), "python": platform.python_version()}


class Monitor:
    """Samples system CPU, memory and filesystem usage while an operation runs."""

    def __init__(self, interval: float = 0.5):
        self.interval = interval
        self._stop = threading.Event()
        self.cpu: list[float] = []
        self.mem_peak = 0
        self.disk0 = shutil.disk_usage(spec.WORK_ROOT).used
        self.disk_peak = self.disk0
        self._t = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        psutil.cpu_percent(None)
        while not self._stop.wait(self.interval):
            self.cpu.append(psutil.cpu_percent(None))
            vm = psutil.virtual_memory()
            self.mem_peak = max(self.mem_peak, vm.total - vm.available)
            self.disk_peak = max(self.disk_peak, shutil.disk_usage(spec.WORK_ROOT).used)

    def start(self):
        self._t.start()
        return self

    def stop(self) -> dict:
        self._stop.set()
        self._t.join()
        return {"cpu_avg_pct": round(statistics.fmean(self.cpu), 1) if self.cpu else None,
                "cpu_max_pct": max(self.cpu) if self.cpu else None,
                "mem_peak_gb": round(self.mem_peak / 2**30, 2),
                "disk_peak_delta_gb": round((self.disk_peak - self.disk0) / 2**30, 2)}


class Reader:
    """An application reader polling the live table with point lookups while a
    refresh runs. A lookup must always return exactly one row."""

    def __init__(self, sol: Solution, params: list, pause: float = 0.02):
        self.sol, self.params, self.pause = sol, params, pause
        self._stop = threading.Event()
        self.lat: list[float] = []
        self.errors = 0
        self.missing = 0
        self.first_error = None
        self.max_stall = 0.0
        self._t = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        client = None
        last_ok = time.perf_counter()
        i = 0
        while not self._stop.is_set():
            p = self.params[i % len(self.params)]
            i += 1
            t0 = time.perf_counter()
            try:
                if client is None:
                    client = self.sol.client()
                rows = client.query("point", p)
                now = time.perf_counter()
                if len(rows) == 1:
                    self.lat.append((now - t0) * 1000)
                    self.max_stall = max(self.max_stall, now - last_ok)
                    last_ok = now
                else:
                    self.missing += 1
            except Exception as e:  # noqa: BLE001
                self.errors += 1
                if self.first_error is None:
                    self.first_error = repr(e)[:300]
                try:
                    if client is not None:
                        client.close()
                except Exception:  # noqa: BLE001
                    pass
                client = None
                time.sleep(0.1)
            time.sleep(self.pause)
        self.max_stall = max(self.max_stall, time.perf_counter() - last_ok) if self.lat else float("inf")
        if client is not None:
            client.close()

    def start(self):
        self._t.start()
        return self

    def stop(self) -> dict:
        self._stop.set()
        self._t.join(timeout=600)
        out = lat_summary(self.lat)
        out.update({"errors": self.errors, "missing_rows": self.missing,
                    "max_stall_s": round(self.max_stall, 3), "first_error": self.first_error})
        return out


def timed(fn, *args) -> dict:
    phases = Phases()
    mon = Monitor().start()
    t0 = time.perf_counter()
    err = None
    try:
        fn(*args, phases)
    except Exception:  # noqa: BLE001
        err = traceback.format_exc()
    total = time.perf_counter() - t0
    out = {"total_s": round(total, 3), "phases": phases.times, "notes": phases.notes, **mon.stop()}
    if err:
        out["error"] = err
        print(err)
    return out


def _norm(v):
    if isinstance(v, Decimal):
        return float(v)
    return v


def rows_equal(got, want, rel: float = 1e-6) -> bool:
    if len(got) != len(want):
        return False
    for g, w in zip(got, want):
        if len(g) != len(w):
            return False
        for a, b in zip(g, w):
            a, b = _norm(a), _norm(b)
            if isinstance(a, float) or isinstance(b, float):
                if not math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-9):
                    return False
            elif isinstance(a, (int,)) and isinstance(b, (int,)):
                if a != b:
                    return False
            elif str(a) != str(b):
                return False
    return True


def verify(sol: Solution, expected: dict) -> dict:
    try:
        c, sq, sp, mx = (_norm(x) for x in sol.verify_row())
        ok = (int(c) == expected["count"] and int(mx) == expected["max_id"]
              and math.isclose(float(sq), expected["sum_quantity"], rel_tol=1e-7)
              and math.isclose(float(sp), expected["sum_price"], rel_tol=1e-7))
        return {"ok": ok, "got": [int(c), float(sq), float(sp), int(mx)],
                "want": [expected["count"], expected["sum_quantity"], expected["sum_price"], expected["max_id"]]}
    except Exception:  # noqa: BLE001
        return {"ok": False, "error": traceback.format_exc()}


def bench_queries(sol: Solution, manifest: dict, iters: dict, check: bool) -> dict:
    out = {}
    client = sol.client()
    try:
        for q in spec.QUERIES:
            if q not in iters:
                continue
            params = [tuple(p) for p in manifest["params"][q]]
            entry: dict = {}
            try:
                if check:
                    oks = []
                    for p, want in zip(params[:3], manifest["reference"][q]):
                        oks.append(rows_equal(client.query(q, p), [tuple(r) for r in want]))
                    entry["verified"] = all(oks)
                for i in range(spec.QUERY_WARMUP[q]):
                    client.query(q, params[(1000 + i) % len(params)])
                ms = []
                for i in range(iters[q]):
                    p = params[i % len(params)]
                    t0 = time.perf_counter()
                    client.query(q, p)
                    ms.append((time.perf_counter() - t0) * 1000)
                entry.update(lat_summary(ms))
            except Exception:  # noqa: BLE001
                entry["error"] = traceback.format_exc()
                print(entry["error"])
            out[q] = entry
    finally:
        client.close()
    return out


def bench_concurrency(sol: Solution, manifest: dict, threads: int = 8, seconds: float = 15.0) -> dict:
    pp = [tuple(p) for p in manifest["params"]["point"]]
    sp = [tuple(p) for p in manifest["params"]["series"]]
    lats: list[list[float]] = [[] for _ in range(threads)]
    errors = [0] * threads
    start = threading.Barrier(threads + 1)
    deadline = [0.0]

    def worker(k: int):
        c = sol.client()
        start.wait()
        i = k * 97
        while time.perf_counter() < deadline[0]:
            i += 1
            t0 = time.perf_counter()
            try:
                if i % 2:
                    c.query("point", pp[i % len(pp)])
                else:
                    c.query("series", sp[i % len(sp)])
                lats[k].append((time.perf_counter() - t0) * 1000)
            except Exception:  # noqa: BLE001
                errors[k] += 1
        c.close()

    ts = [threading.Thread(target=worker, args=(k,), daemon=True) for k in range(threads)]
    for t in ts:
        t.start()
    deadline[0] = time.perf_counter() + seconds + 1e9   # placeholder until all are connected
    start.wait()
    t0 = time.perf_counter()
    deadline[0] = t0 + seconds
    for t in ts:
        t.join()
    el = time.perf_counter() - t0
    allms = [x for l in lats for x in l]
    out = lat_summary(allms)
    out.update({"threads": threads, "seconds": round(el, 2), "qps": round(len(allms) / el, 1),
                "errors": sum(errors)})
    return out


def storage(sol: Solution) -> dict:
    try:
        s = dict(sol.storage())
    except Exception:  # noqa: BLE001
        s = {"error": traceback.format_exc()}
    dp = sol.data_path()
    if dp is not None:
        from .dockerutil import du_bytes
        s["data_dir_bytes"] = du_bytes(dp)
    return s


# --------------------------------------------------------------------------- main protocol
def run(solution: str, scale: str, reps: int = 2, steps: list[str] | None = None, tag: str | None = None,
        keep: bool = False) -> dict:
    steps = steps or ALL_STEPS
    sols = load_all()
    if solution not in sols:
        raise SystemExit(f"unknown solution {solution!r}; have: {', '.join(sorted(sols))}")
    manifest = datagen.generate(scale)
    ds = Dataset(spec.dataset_dir(scale), manifest)
    sol: Solution = sols[solution]()
    res: dict = {
        "solution": sol.name, "engine": sol.engine, "strategy": sol.strategy, "atomic_swap": sol.atomic_swap,
        "scale": scale, "rows": ds.rows, "reps": reps, "tag": tag,
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": host_info(), "steps": {}, "ok": False,
    }
    S = res["steps"]
    spec.WORK_ROOT.mkdir(parents=True, exist_ok=True)
    lock = None
    if ds.rows >= HEAVY_ROWS:
        lock = open(LOCK_FILE, "w")
        print(f"[run] waiting for heavy-run lock {LOCK_FILE} ...", flush=True)
        fcntl.flock(lock, fcntl.LOCK_EX)
    t_all = time.time()

    def log(msg):
        print(f"[run {sol.name} {scale} +{time.time()-t_all:6.0f}s] {msg}", flush=True)

    try:
        log("setup")
        sol.setup()
        res["info"] = sol.info()

        log("initial_load")
        S["initial_load"] = timed(sol.full_refresh, ds)
        S["initial_load"]["verify"] = verify(sol, manifest["expected"]["base"])
        S["storage"] = storage(sol)
        log(f"initial_load {S['initial_load']['total_s']}s verify={S['initial_load']['verify'].get('ok')}")
        if "error" in S["initial_load"]:
            raise RuntimeError("initial load failed")

        if "queries" in steps:
            S["queries"] = bench_queries(sol, manifest, spec.QUERY_ITERS, check=True)
            log("queries " + json.dumps({k: (v.get("p50_ms"), v.get("verified")) for k, v in S["queries"].items()}))
        if "concurrency" in steps:
            S["concurrency"] = bench_concurrency(sol, manifest)
            log(f"concurrency qps={S['concurrency'].get('qps')}")

        if "refresh" in steps:
            S["refresh"] = []
            for r in range(max(0, reps - 1)):
                reader = Reader(sol, [tuple(p) for p in manifest["params"]["point"]]).start()
                time.sleep(1.0)
                out = timed(sol.full_refresh, ds)
                time.sleep(1.0)
                out["reader"] = reader.stop()
                out["verify"] = verify(sol, manifest["expected"]["base"])
                S["refresh"].append(out)
                log(f"refresh#{r+1} {out['total_s']}s reader errors={out['reader']['errors']} "
                    f"missing={out['reader']['missing_rows']} stall={out['reader']['max_stall_s']}s "
                    f"verify={out['verify'].get('ok')}")

        if "incremental" in steps:
            S["incremental"] = {}
            for key, path in (("delta_1pct", ds.delta_1pct), ("delta_10pct", ds.delta_10pct)):
                reader = Reader(sol, [tuple(p) for p in manifest["params"]["point"]]).start()
                out = timed(sol.upsert, ds, path)
                out["reader"] = reader.stop()
                out["rows"] = manifest[f"rows_{key}"]
                out["verify"] = verify(sol, manifest["expected"][key])
                S["incremental"][f"upsert_{key.split('_')[1]}"] = out
                log(f"upsert {key} {out['total_s']}s verify={out['verify'].get('ok')}")
            out = timed(sol.replace_tenant, ds)
            out["rows"] = manifest["rows_replace_tenant"]
            out["verify"] = verify(sol, manifest["expected"]["replace_tenant"])
            S["incremental"]["replace_tenant"] = out
            log(f"replace_tenant {out['total_s']}s verify={out['verify'].get('ok')}")
            S["storage_after"] = storage(sol)

        if "queries_after" in steps and "incremental" in steps:
            S["queries_after"] = bench_queries(sol, manifest, {"point": 300, "series": 300, "tenant_agg": 20},
                                               check=False)
            log("queries_after " + json.dumps({k: v.get("p50_ms") for k, v in S["queries_after"].items()}))

        def _all_ok() -> bool:
            checks = [S["initial_load"]["verify"].get("ok")]
            checks += [r["verify"].get("ok") and "error" not in r for r in S.get("refresh", [])]
            checks += [v["verify"].get("ok") and "error" not in v for v in S.get("incremental", {}).values()]
            checks += [v.get("verified", False) for v in S.get("queries", {}).values()]
            return all(checks)

        res["ok"] = _all_ok()
    except Exception:  # noqa: BLE001
        res["fatal"] = traceback.format_exc()
        print(res["fatal"])
    finally:
        res["wall_s"] = round(time.time() - t_all, 1)
        spec.RESULTS.mkdir(parents=True, exist_ok=True)
        stamp = tag or datetime.now().strftime("%Y%m%d-%H%M%S")
        path = spec.RESULTS / f"{sol.name}__{scale}__{stamp}.json"
        path.write_text(json.dumps(res, indent=1, default=str))
        log(f"result -> {path}  ok={res['ok']}")
        if not keep:
            try:
                sol.teardown()
            except Exception:  # noqa: BLE001
                traceback.print_exc()
        if lock is not None:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--solution")
    ap.add_argument("--scale", default="1M", choices=list(spec.SCALES))
    ap.add_argument("--reps", type=int, default=2, help="total full loads: 1 initial + (reps-1) refreshes under read load")
    ap.add_argument("--steps", default=",".join(ALL_STEPS))
    ap.add_argument("--tag")
    ap.add_argument("--keep", action="store_true", help="leave the engine running afterwards (debugging)")
    a = ap.parse_args()
    if a.list:
        for n, c in sorted(load_all().items()):
            print(f"{n:28s} {c.engine:12s} {c.strategy}")
        return
    if not a.solution:
        ap.error("--solution required")
    r = run(a.solution, a.scale, a.reps, a.steps.split(","), a.tag, a.keep)
    raise SystemExit(0 if r["ok"] else 1)


if __name__ == "__main__":
    main()
