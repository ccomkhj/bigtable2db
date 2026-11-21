"""Tiny docker helpers. All engines run with --network host (no userland proxy
in the data path) and keep their data on a bind mount under spec.WORK_ROOT
(no overlayfs in the data path)."""
from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

from . import spec


def sh(*args: str, check: bool = True, capture: bool = True) -> str:
    r = subprocess.run(list(args), check=False, capture_output=capture, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return (r.stdout or "").strip()


def rm(name: str):
    sh("docker", "rm", "-f", "-v", name, check=False)


def data_dir(name: str, fresh: bool = True) -> Path:
    d = spec.WORK_ROOT / name
    if fresh and d.exists():
        # files are owned by container users; remove through a container-independent root rm
        shutil.rmtree(d, ignore_errors=True)
        sh("rm", "-rf", str(d), check=False)
    d.mkdir(parents=True, exist_ok=True)
    return d


def run(name: str, image: str, *, mounts: dict[str, str] | None = None, env: dict[str, str] | None = None,
        args: list[str] | None = None, shm: str = "4g", extra: list[str] | None = None) -> str:
    rm(name)
    cmd = ["docker", "run", "-d", "--name", name, "--network", "host", f"--shm-size={shm}"]
    for host, cont in (mounts or {}).items():
        cmd += ["-v", f"{host}:{cont}"]
    for k, v in (env or {}).items():
        cmd += ["-e", f"{k}={v}"]
    cmd += (extra or [])
    cmd.append(image)
    cmd += (args or [])
    return sh(*cmd)


def wait_until(fn, timeout: float = 120, interval: float = 0.5, what: str = "service"):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            if fn():
                return
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(interval)
    raise TimeoutError(f"{what} not ready after {timeout}s: {last}")


def logs(name: str, tail: int = 50) -> str:
    return sh("docker", "logs", "--tail", str(tail), name, check=False)


def du_bytes(path: Path | str) -> int:
    out = sh("du", "-sb", str(path), check=False)
    try:
        return int(out.split()[0])
    except Exception:  # noqa: BLE001
        return -1


def image_version(image: str) -> str:
    return sh("docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image, check=False)
