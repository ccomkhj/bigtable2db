"""Deterministic synthetic dataset generator (DuckDB -> Parquet).

    python -m bench.datagen --scale 1M

Produces, under $B2D_DATA/<scale>/ :

    base/part-XXX.parquet      the full table (the "dataframe" an upstream job hands us)
    delta_1pct.parquet         1% of rows:  75% updates of existing keys, 25% brand-new keys
    delta_10pct.parquet        10% of rows: same mix, applied AFTER delta_1pct
    replace_tenant.parquet     complete recomputed data for tenant REPLACE_TENANT
                               (applied AFTER both deltas; drops ~10% of its items, adds new ones)
    manifest.json              row counts, expected checksums after every step,
                               sampled query parameters and reference query answers

Every value is a pure function of the row number, so any scale is reproducible
bit-for-bit and no random state is involved.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

from . import spec

VOCAB_SIZE = 2000
# knobs calibrated so that PostgreSQL stores ~500 B/row heap and ~150 B/row per index
ITEM_KEY_WORDS = 7
ITEM_KEY_HEX = 22
REF_PATH_WORDS = 9
DESCRIPTION_WORDS = 24

STRIDE_1 = 1_000_003      # primes, coprime with every scale -> distinct, well scattered row ids
STRIDE_10 = 7_368_787


def _vocab() -> list[str]:
    """2000 deterministic pseudo-words, 4-9 chars, so text compresses like real text (~3x)."""
    cons = "b c d f g h j k l m n p r s t v w z ch sh th st tr pl".split()
    vow = "a e i o u ai ea ou".split()
    words, seen, k = [], set(), 0
    while len(words) < VOCAB_SIZE:
        k += 1
        h = hashlib.md5(f"vocab-{k}".encode()).digest()   # stable across platforms / versions
        n_syl = 2 + h[0] % 3
        w = "".join(cons[h[1 + 2 * s] % len(cons)] + vow[h[2 + 2 * s] % len(vow)] for s in range(n_syl))[:9]
        if len(w) >= 4 and w not in seen:
            seen.add(w)
            words.append(w)
    return words


def connect(threads: int = 4, memory: str = "6GB") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={threads}; SET memory_limit='{memory}'; SET preserve_insertion_order=true;")
    con.execute(f"""
    CREATE MACRO u(x, salt) AS (hash(x, salt) % 1000000007)::DOUBLE / 1000000007.0;
    CREATE MACRO item_tenant(it) AS (1 + floor({spec.N_TENANTS} * pow(u(it, 7), 2)))::INTEGER;
    """)
    return con


# All text is assembled from plain scalar expressions (substr of one constant
# string holding the fixed-width vocabulary) so DuckDB can vectorise it.
_VOCAB_STR = "".join(w.ljust(9) for w in _vocab())


def _word(x: str, salt: int) -> str:
    return f"rtrim(substr('{_VOCAB_STR}', 1 + 9 * (hash({x}, {salt}) % {VOCAB_SIZE})::BIGINT, 9))"


def _words(x: str, salt: int, n: int, sep: str) -> str:
    return "concat_ws('%s', %s)" % (sep, ", ".join(_word(x, salt + k) for k in range(n)))


def _item_key(it: str) -> str:
    return (f"concat(upper({_word(it, 11)}), '-', lpad((hash({it}, 12) % 10000)::VARCHAR, 4, '0'), '/', "
            f"{_words(it, 100, ITEM_KEY_WORDS, '/')}, '/SKU-', lpad(({it})::VARCHAR, 10, '0'), '-', "
            f"substr(md5(concat('k', ({it})::VARCHAR)), 1, {ITEM_KEY_HEX}))")


def _ref_path(it: str) -> str:
    return (f"concat('/', {_words(it, 200, REF_PATH_WORDS, '/')}, '/', "
            f"md5(concat('r', ({it})::VARCHAR)), '.html')")


def _copy(con, select_sql: str, path: Path):
    con.execute(
        f"COPY ({select_sql}) TO '{path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)"
    )


def _rows_sql(id_sql: str, version: int, tenant_override: str = "NULL") -> str:
    """SELECT producing full rows for the ids yielded by `id_sql` (a query with one column `i`).

    i = row id; item = i // PERIODS; `version` changes only the non-key columns.
    """
    P = spec.PERIODS
    it = f"(i // {P})"
    return f"""
        SELECT i::BIGINT AS id,
               coalesce({tenant_override}, item_tenant({it}))::INTEGER AS tenant_id,
               ({spec.PERIOD_BASE} + i % {P})::INTEGER AS period,
               round(u(i, {1000 + version}) * 1000, 3) AS quantity,
               round(1 + u(i, {2000 + version}) * 499, 2) AS price,
               u(i, {3000 + version}) AS score,
               {_item_key(it)} AS item_key,
               {_ref_path(it)} AS ref_path,
               {_words('i', 4000 + 100 * version, DESCRIPTION_WORDS, ' ')} AS description
        FROM ({id_sql}) ids(i)
    """


def generate(scale: str, parts: int | None = None, force: bool = False) -> dict:
    n = spec.SCALES[scale]
    P = spec.PERIODS
    out = spec.dataset_dir(scale)
    manifest_path = out / "manifest.json"
    if manifest_path.exists() and not force:
        return json.loads(manifest_path.read_text())
    (out / "base").mkdir(parents=True, exist_ok=True)
    parts = parts or max(4, math.ceil(n / 1_250_000))   # 20M -> 16 files
    t0 = time.time()
    con = connect()

    # ---- base table: `parts` files, generated 4 at a time -------------------
    per = n // parts
    assert per % P == 0, "part boundary must not split an item"

    def gen_part(k: int):
        lo, hi = k * per, (n if k == parts - 1 else (k + 1) * per)
        cur = con.cursor()
        _copy(cur, _rows_sql(f"SELECT range FROM range({lo}, {hi})", 0), out / "base" / f"part-{k:03d}.parquet")

    with ThreadPoolExecutor(4) as ex:
        list(ex.map(gen_part, range(parts)))
    print(f"[datagen] base {n:,} rows in {time.time()-t0:.1f}s")

    # ---- deltas -------------------------------------------------------------
    def delta(frac: float, stride: int, offset: int, version: int, new_lo: int) -> tuple[str, int, int]:
        m = int(n * frac)
        m_new = (int(m * 0.25) // P) * P
        m_upd = m - m_new
        upd = f"SELECT (j * {stride} + {offset}) % {n} FROM range({m_upd}) t(j)"
        new = f"SELECT range FROM range({new_lo}, {new_lo + m_new})"
        return f"{_rows_sql(upd, version)} UNION ALL {_rows_sql(new, version)}", m, new_lo + m_new

    sql1, m1, hi1 = delta(0.01, STRIDE_1, 17, 1, n)
    _copy(con, sql1, out / "delta_1pct.parquet")
    sql10, m10, hi10 = delta(0.10, STRIDE_10, 4242, 2, hi1)
    _copy(con, sql10, out / "delta_10pct.parquet")

    # ---- tenant replacement: recomputed full data for one tenant ------------
    T = spec.REPLACE_TENANT
    keep = (f"SELECT range FROM range({hi10}) t(range) "
            f"WHERE item_tenant(range // {P}) = {T} AND hash(range // {P}, 99) % 10 <> 0")
    n_new_items = max(1, int(n * 0.05 * 0.10) // P)
    new = f"SELECT range FROM range({hi10}, {hi10 + n_new_items * P})"
    _copy(con, f"{_rows_sql(keep, 3)} UNION ALL {_rows_sql(new, 3, str(T))}", out / "replace_tenant.parquet")
    print(f"[datagen] deltas done at {time.time()-t0:.1f}s")

    # ---- expected state after each step (id is unique per business key) -----
    base = f"read_parquet('{out}/base/*.parquet')"
    d1 = f"read_parquet('{out}/delta_1pct.parquet')"
    d10 = f"read_parquet('{out}/delta_10pct.parquet')"
    rt = f"read_parquet('{out}/replace_tenant.parquet')"
    cols = "id, tenant_id, quantity, price"
    con.execute(f"""
        CREATE TEMP TABLE s0 AS SELECT {cols} FROM {base};
        CREATE TEMP TABLE s1 AS SELECT * FROM s0 WHERE id NOT IN (SELECT id FROM {d1})
                                UNION ALL SELECT {cols} FROM {d1};
        CREATE TEMP TABLE s2 AS SELECT * FROM s1 WHERE id NOT IN (SELECT id FROM {d10})
                                UNION ALL SELECT {cols} FROM {d10};
        CREATE TEMP TABLE s3 AS SELECT * FROM s2 WHERE tenant_id <> {T}
                                UNION ALL SELECT {cols} FROM {rt};
    """)

    def chk(t):
        c, sq, sp, mx = con.execute(f"SELECT count(*), sum(quantity), sum(price), max(id) FROM {t}").fetchone()
        return {"count": c, "sum_quantity": sq, "sum_price": sp, "max_id": mx}

    expected = {"base": chk("s0"), "delta_1pct": chk("s1"), "delta_10pct": chk("s2"), "replace_tenant": chk("s3")}

    # ---- query parameters + reference answers (against the base state) ------
    step = max(1, n // 2000)
    point = con.execute(
        f"SELECT tenant_id, item_key, period FROM {base} WHERE id % {step} = {step // 3} ORDER BY hash(id) LIMIT 2000"
    ).fetchall()
    series = con.execute(
        f"SELECT ref_path, {spec.PERIOD_BASE} + (id % 11)::INTEGER, {spec.PERIOD_BASE} + (id % 11)::INTEGER + 9 "
        f"FROM {base} WHERE id % {step} = {step // 2} ORDER BY hash(id, 5) LIMIT 2000"
    ).fetchall()
    tenants = [(1 + (k * 37) % spec.N_TENANTS,) for k in range(40)]
    params = {"point": point, "series": series, "tenant_agg": tenants, "full_agg": [()]}

    con.execute(f"CREATE VIEW big_table AS SELECT * FROM {base}")
    reference = {}
    for q, sql in spec.QUERIES.items():
        reference[q] = [con.execute(sql, list(p)).fetchall() for p in params[q][:3]]

    stats = con.execute(
        f"SELECT avg(length(item_key)), avg(length(ref_path)), avg(length(description)), "
        f"count(DISTINCT tenant_id), max(c)::DOUBLE / sum(c) FROM "
        f"(SELECT tenant_id, any_value(item_key) item_key, any_value(ref_path) ref_path, "
        f" any_value(description) description, count(*) c FROM {base} GROUP BY tenant_id)"
    ).fetchone()

    manifest = {
        "scale": scale,
        "rows": n,
        "parts": parts,
        "rows_delta_1pct": m1,
        "rows_delta_10pct": m10,
        "rows_replace_tenant": con.execute(f"SELECT count(*) FROM {rt}").fetchone()[0],
        "replace_tenant": T,
        "parquet_bytes": sum(f.stat().st_size for f in (out / "base").glob("*.parquet")),
        "avg_len": {"item_key": stats[0], "ref_path": stats[1], "description": stats[2]},
        "tenants": stats[3],
        "largest_tenant_share": stats[4],
        "expected": expected,
        "params": params,
        "reference": reference,
        "generated_in_s": round(time.time() - t0, 1),
    }
    manifest_path.write_text(json.dumps(manifest, default=str))
    print(f"[datagen] {scale} complete in {time.time()-t0:.1f}s -> {out}")
    return manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", required=True, choices=list(spec.SCALES))
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    m = generate(a.scale, force=a.force)
    print(json.dumps({k: v for k, v in m.items() if k not in ("params", "reference")}, indent=2))


if __name__ == "__main__":
    main()
