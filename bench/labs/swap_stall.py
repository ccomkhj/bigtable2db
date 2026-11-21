"""Diagnostic: what does an application reader wait on while big_table is swapped?

    python -m bench.labs.swap_stall --solution pg_parallel_swap --scale 5M [--mode rename_drop]

Loads the table, starts a reader (point lookups every 20 ms, server-side prepared
statement like the benchmark client), then performs one refresh. A third connection
samples pg_stat_activity + pg_locks every 25 ms and records what the reader's backend
is waiting for whenever one of its queries has been running for more than 100 ms.
"""
from __future__ import annotations

import argparse
import collections
import json
import threading
import time

import psycopg

from .. import datagen, spec
from ..solutions import load_all
from ..solutions.base import Dataset, Phases


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--solution", default="pg_parallel_swap")
    ap.add_argument("--scale", default="5M")
    ap.add_argument("--prepare", default="1", help="1 = reader uses server-side prepared statements")
    a = ap.parse_args()
    ds = Dataset(spec.dataset_dir(a.scale), datagen.generate(a.scale))
    sol = load_all()[a.solution]()
    sol.setup()
    try:
        sol.full_refresh(ds, Phases())
        params = [tuple(p) for p in ds.manifest["params"]["point"]]
        stop = threading.Event()
        lat: list[tuple[float, float]] = []
        pid = [None]
        from ..solutions.pg_common import PG_QUERIES

        def reader():
            c = psycopg.connect(sol.dsn, autocommit=True)
            pid[0] = c.info.backend_pid
            i = 0
            while not stop.is_set():
                t0 = time.perf_counter()
                c.execute(PG_QUERIES["point"], params[i % len(params)], prepare=a.prepare == "1").fetchall()
                lat.append((time.time(), (time.perf_counter() - t0) * 1000))
                i += 1
                time.sleep(0.02)

        waits: collections.Counter = collections.Counter()
        samples: list = []

        def sampler():
            c = psycopg.connect(sol.dsn, autocommit=True)
            while not stop.is_set():
                if pid[0]:
                    r = c.execute(
                        "SELECT a.wait_event_type, a.wait_event, a.state, "
                        "       extract(epoch FROM clock_timestamp() - a.query_start) AS running_s, "
                        "       (SELECT string_agg(DISTINCT l.locktype || ':' || coalesce(l.relation::regclass::text, '') "
                        "               || ':' || l.mode, ', ') FROM pg_locks l WHERE l.pid = a.pid AND NOT l.granted) AS waiting_for, "
                        "       (SELECT string_agg(DISTINCT left(b.query, 60), ' | ') FROM pg_stat_activity b "
                        "         WHERE b.pid = ANY(pg_blocking_pids(a.pid))) AS blocked_by "
                        "FROM pg_stat_activity a WHERE a.pid = %s", [pid[0]]).fetchone()
                    if r and r[2] == "active" and r[3] and float(r[3]) > 0.1:
                        key = (r[0], r[1], r[4], r[5])
                        waits[key] += 1
                        samples.append((round(float(r[3]), 2),) + key)
                time.sleep(0.025)

        ts = [threading.Thread(target=reader, daemon=True), threading.Thread(target=sampler, daemon=True)]
        for t in ts:
            t.start()
        time.sleep(2)
        ph = Phases()
        t0 = time.time()
        sol.full_refresh(ds, ph)
        t1 = time.time()
        time.sleep(2)
        stop.set()
        for t in ts:
            t.join(timeout=30)
        slow = [(round(ts_ - t0, 2), round(ms)) for ts_, ms in lat if ms > 100]
        print(json.dumps({"solution": a.solution, "scale": a.scale, "prepare": a.prepare,
                          "refresh_s": round(t1 - t0, 1), "phases": ph.times,
                          "reader_queries": len(lat), "reader_max_ms": round(max(ms for _, ms in lat)),
                          "slow_queries_[t_since_refresh_start_s, ms]": slow,
                          "what_the_reader_waited_on": [{"samples": n, "wait_event_type": k[0], "wait_event": k[1],
                                                         "ungranted_lock": k[2], "blocked_by": k[3]}
                                                        for k, n in waits.most_common(8)]}, indent=1))
    finally:
        sol.teardown()


if __name__ == "__main__":
    main()
