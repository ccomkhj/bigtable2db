"""Solution registry. Each module registers its Solution classes in SOLUTIONS."""
from __future__ import annotations

import importlib
import pkgutil

from .base import Solution

SOLUTIONS: dict[str, type[Solution]] = {}


def register(cls: type[Solution]) -> type[Solution]:
    assert cls.name and cls.name not in SOLUTIONS, f"duplicate/empty solution name: {cls.name!r}"
    SOLUTIONS[cls.name] = cls
    return cls


def load_all() -> dict[str, type[Solution]]:
    for m in pkgutil.iter_modules(__path__):
        if m.name not in ("base",) and not m.name.startswith("_"):
            importlib.import_module(f"{__name__}.{m.name}")
    return SOLUTIONS
