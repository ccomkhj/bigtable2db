"""The contract every benchmarked solution implements.

A *solution* = one engine + one loading strategy. The runner (bench/run.py)
drives all solutions through exactly the same steps and does all the timing,
verification and bookkeeping, so solution code only contains engine logic.
"""
from __future__ import annotations

import abc
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Sequence


class Phases:
    """Named wall-clock phases inside one timed operation.

        with phases("copy"): ...
        with phases("index_a"): ...

    Phases may overlap (e.g. two index builds in parallel threads); the
    operation total is always measured separately by the runner.
    """

    def __init__(self):
        self.times: dict[str, float] = {}
        self.notes: dict[str, Any] = {}

    @contextmanager
    def __call__(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.times[name] = round(self.times.get(name, 0.0) + time.perf_counter() - t0, 3)

    def note(self, key: str, value: Any):
        self.notes[key] = value


class Dataset:
    """Paths of one generated scale (see bench/datagen.py)."""

    def __init__(self, root: Path, manifest: dict):
        self.root = root
        self.manifest = manifest
        self.scale: str = manifest["scale"]
        self.rows: int = manifest["rows"]
        self.base_files: list[Path] = sorted((root / "base").glob("*.parquet"))
        self.delta_1pct: Path = root / "delta_1pct.parquet"
        self.delta_10pct: Path = root / "delta_10pct.parquet"
        self.replace_tenant_file: Path = root / "replace_tenant.parquet"
        self.replace_tenant_id: int = manifest["replace_tenant"]


class Client(abc.ABC):
    """One application connection. Each benchmark thread owns its own Client."""

    @abc.abstractmethod
    def query(self, name: str, params: Sequence[Any]) -> list[tuple]:
        """Run logical query `name` (see spec.QUERIES) and return rows as tuples."""

    def close(self):  # noqa: B027
        pass


class Solution(abc.ABC):
    #: unique id, also used for container name / data dir / result file
    name: str = ""
    #: engine family, e.g. "postgres", "timescaledb", "clickhouse"
    engine: str = ""
    #: one-line human description of the loading strategy
    strategy: str = ""
    #: True if readers keep seeing the complete old table until the new one is ready
    atomic_swap: bool = False

    # ---- lifecycle (untimed) ------------------------------------------------
    @abc.abstractmethod
    def setup(self) -> None:
        """Start the engine from an empty data dir and wait until it accepts connections."""

    @abc.abstractmethod
    def teardown(self) -> None:
        """Stop the engine and delete its data."""

    # ---- timed operations ---------------------------------------------------
    @abc.abstractmethod
    def full_refresh(self, ds: Dataset, phases: Phases) -> None:
        """Make ds.base_files the complete content of the live table, with both
        access paths (INDEX_A unique, INDEX_B) built and planner statistics ready.

        Must work when the live table does not exist yet AND when it already
        exists and is being read. The clock covers everything from reading the
        Parquet files to the moment queries see the new data."""

    @abc.abstractmethod
    def upsert(self, ds: Dataset, delta: Path, phases: Phases) -> None:
        """Apply `delta` (same schema): rows whose business key exists replace
        the old row, other rows are inserted."""

    @abc.abstractmethod
    def replace_tenant(self, ds: Dataset, phases: Phases) -> None:
        """Replace ALL rows of ds.replace_tenant_id by ds.replace_tenant_file."""

    # ---- reads --------------------------------------------------------------
    @abc.abstractmethod
    def client(self) -> Client:
        ...

    @abc.abstractmethod
    def verify_row(self) -> tuple:
        """Result of spec.VERIFY_SQL: (count, sum(quantity), sum(price), max(id))."""

    @abc.abstractmethod
    def storage(self) -> dict:
        """Engine-reported bytes: {"table": .., "index_a": .., "index_b": .., "total": ..}
        (use what applies; "total" is mandatory)."""

    # ---- metadata -----------------------------------------------------------
    def info(self) -> dict:
        """Engine version and the configuration that matters (free form)."""
        return {}

    def data_path(self) -> Path | None:
        """Host directory holding the engine's data (for du)."""
        return None
