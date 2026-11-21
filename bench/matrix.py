"""Run the final measurement matrix: every solution x scale, strictly one after another.

    python -m bench.matrix --scales 5M,20M --tag final
    python -m bench.matrix --scales 20M --tag final --solutions pg_parallel_swap,ch_mergetree

Each run is its own process (clean client state), has a hard timeout, and is
skipped if a result file with the same tag already exists (so the matrix can be
resumed). Logs go to $B2D_WORK/../logs/.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from . import spec
from .solutions import load_all

# Total full loads per run (1 initial + N-1 refreshes under read load).
DEFAULT_REPS = 3
# Slow solutions get fewer repetitions; their variance is irrelevant next to their runtime.
REPS = {"pg_naive_insert": 1, "pg_truncate_copy": 2, "pg_copy_swap": 2, "mysql_load_swap": 2}
# Per-scale exceptions, each explained in README.md ("How to read the numbers").
#   * load-only: these differ from pg_parallel_swap only in HOW the table is loaded; the resulting
#     table is the same, so queries / maintenance are not re-measured at 20M.
#   * skipped at 20M: controls and duplicates that exist to isolate one variable at 5M.
#   * mysql at 20M: one load only. Two table copies + binlog + redo do not fit on this disk.
LOAD_ONLY_20M = {"pg_naive_insert", "pg_copy_swap", "pg_unlogged_swap", "pg_unlogged_setlogged",
                 "pg_walminimal_swap"}
SKIP_20M = {"pg_incremental", "ts_plain_control"}
REPS_20M = {"mysql_load_swap": 1}
TIMEOUT_S = {"5M": 60 * 60, "20M": 150 * 60}
LOG_DIR = spec.WORK_ROOT.parent / "logs"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default="5M,20M")
    ap.add_argument("--tag", default="final")
    ap.add_argument("--solutions", default="")
    ap.add_argument("--reps", type=int, default=0)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    names = [s for s in a.solutions.split(",") if s] or sorted(load_all())
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for scale in a.scales.split(","):
        for name in names:
            out = spec.RESULTS / f"{name}__{scale}__{a.tag}.json"
            if out.exists() and not a.force:
                print(f"[matrix] skip {name} {scale} (exists)", flush=True)
                continue
            reps = a.reps or REPS.get(name, DEFAULT_REPS)
            steps = None
            if scale == "20M":
                if name in SKIP_20M and not a.solutions:
                    print(f"[matrix] skip {name} {scale} (not part of the 20M matrix)", flush=True)
                    continue
                reps = a.reps or REPS_20M.get(name, reps)
                if name in LOAD_ONLY_20M:
                    steps = "initial_load,refresh"
            log = LOG_DIR / f"{name}__{scale}__{a.tag}.log"
            t0 = time.time()
            print(f"[matrix] start {name} {scale} reps={reps}", flush=True)
            with open(log, "w") as lf:
                try:
                    rc = subprocess.run(
                        [sys.executable, "-m", "bench.run", "--solution", name, "--scale", scale,
                         "--reps", str(reps), "--tag", a.tag] + (["--steps", steps] if steps else []),
                        stdout=lf, stderr=subprocess.STDOUT, timeout=TIMEOUT_S.get(scale, 3600)).returncode
                except subprocess.TimeoutExpired:
                    rc = "timeout"
                    subprocess.run(["docker", "rm", "-f", "-v", f"b2d-{name}"], capture_output=True)
                    subprocess.run(["rm", "-rf", str(spec.WORK_ROOT / name)], capture_output=True)
            ok = None
            if out.exists():
                ok = json.loads(out.read_text()).get("ok")
            print(f"[matrix] done  {name} {scale} rc={rc} ok={ok} wall={time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
