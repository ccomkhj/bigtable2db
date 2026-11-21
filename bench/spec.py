"""Single source of truth for the benchmark workload.

Every solution loads the same logical table and answers the same logical
queries. Nothing engine specific lives here.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("B2D_DATA", "/var/lib/bench/data"))      # generated parquet
WORK_ROOT = Path(os.environ.get("B2D_WORK", "/var/lib/bench/engines"))   # engine data dirs
RESULTS = REPO / "results" / "raw"

TABLE = "big_table"

# name, logical type  (3 ints, 3 floats, 3 texts = 9 columns)
COLUMNS = [
    ("id", "int64"),          # surrogate row id, unique, NOT indexed
    ("tenant_id", "int32"),   # SaaS client, 400 tenants, skewed (largest ~5% of rows)
    ("period", "int32"),      # integer time bucket, 1001..1020
    ("quantity", "float64"),
    ("price", "float64"),
    ("score", "float64"),
    ("item_key", "text"),     # ~108 chars business key, one per item
    ("ref_path", "text"),     # ~112 chars path-like secondary key, one per item
    ("description", "text"),  # ~200 chars free text, unique per row
]
COLUMN_NAMES = [c for c, _ in COLUMNS]

# The two "index sets" the application depends on.
#   idx_a  UNIQUE (tenant_id, item_key, period)   -> business key, point lookups, upsert key
#   idx_b         (ref_path, period)              -> secondary access path, range scans
KEY = ("tenant_id", "item_key", "period")
INDEX_A = ("tenant_id", "item_key", "period")
INDEX_B = ("ref_path", "period")

PERIODS = 20
PERIOD_BASE = 1001
N_TENANTS = 400
REPLACE_TENANT = 1            # the largest tenant (~5% of rows) is fully recomputed

SCALES = {
    "100k": 100_000,
    "1M": 1_000_000,
    "5M": 5_000_000,
    "20M": 20_000_000,
}

# ---------------------------------------------------------------------------
# Logical queries. Each solution implements these four in its own dialect and
# must return rows as tuples in exactly the column order given here.
# ---------------------------------------------------------------------------
QUERIES = {
    # 1 row; all 9 columns in COLUMN_NAMES order. params: (tenant_id, item_key, period)
    "point": "SELECT * FROM big_table WHERE tenant_id = ? AND item_key = ? AND period = ?",
    # <= 10 rows (period, quantity, price, score) ordered by period. params: (ref_path, period_lo, period_hi)
    "series": "SELECT period, quantity, price, score FROM big_table "
              "WHERE ref_path = ? AND period BETWEEN ? AND ? ORDER BY period",
    # 20 rows (period, cnt, revenue, avg_score) ordered by period. params: (tenant_id,)
    "tenant_agg": "SELECT period, count(*) AS cnt, sum(quantity * price) AS revenue, avg(score) AS avg_score "
                  "FROM big_table WHERE tenant_id = ? GROUP BY period ORDER BY period",
    # 20 rows (tenant_id, cnt, sum_qty, avg_price). no params. full scan.
    "full_agg": "SELECT tenant_id, count(*) AS cnt, sum(quantity) AS sum_qty, avg(price) AS avg_price "
                "FROM big_table GROUP BY tenant_id ORDER BY cnt DESC, tenant_id ASC LIMIT 20",
}

# how many measured executions per query (after warm-up)
QUERY_ITERS = {"point": 500, "series": 500, "tenant_agg": 40, "full_agg": 5}
QUERY_WARMUP = {"point": 50, "series": 50, "tenant_agg": 5, "full_agg": 1}

# Reference SQL used to verify table content after each mutation.
VERIFY_SQL = "SELECT count(*), sum(quantity), sum(price), max(id) FROM big_table"


def dataset_dir(scale: str) -> Path:
    return DATA_ROOT / scale
