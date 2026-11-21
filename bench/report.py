"""Turn results/raw/*__<tag>.json into tables and charts.

    python -m bench.report --tag final

Writes:
    results/summary.csv          one row per (solution, scale)
    results/RESULTS.md           generated tables (all scales)
    results/charts/*.png         charts used by README.md
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path

from . import spec

OUT = spec.REPO / "results"
CHARTS = OUT / "charts"

FAMILY_ORDER = ["postgres", "timescaledb", "clickhouse", "duckdb", "mysql"]
# fixed categorical order (validated palette, light surface); colour follows the engine, never the rank
FAMILY_COLOR = {"postgres": "#2a78d6", "timescaledb": "#eb6834", "clickhouse": "#1baf7a",
                "duckdb": "#eda100", "mysql": "#e87ba4"}
FAMILY_LABEL = {"postgres": "PostgreSQL", "timescaledb": "TimescaleDB", "clickhouse": "ClickHouse",
                "duckdb": "DuckDB / Parquet", "mysql": "MySQL"}
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
TARGET_S = 300


def load(tag: str) -> list[dict]:
    rows = []
    for f in sorted(spec.RESULTS.glob(f"*__{tag}.json")):
        r = json.loads(f.read_text())
        S = r.get("steps", {})
        il = S.get("initial_load") or {}
        if "total_s" not in il or "error" in il:
            rows.append({"solution": r["solution"], "engine": r["engine"], "scale": r["scale"], "rows": r["rows"],
                         "ok": False, "failed": True, "strategy": r.get("strategy", "")})
            continue
        refreshes = [x for x in S.get("refresh", []) if "error" not in x]
        rt = [x["total_s"] for x in refreshes]
        loads = [il["total_s"]] + rt
        inc = S.get("incremental", {})
        q = S.get("queries", {})
        qa = S.get("queries_after", {})
        st = S.get("storage", {})
        sta = S.get("storage_after", {})
        conc = S.get("concurrency", {})

        def t(d, k):
            v = d.get(k, {})
            return v.get("total_s") if v.get("verify", {}).get("ok") and "error" not in v else None

        row = {
            "solution": r["solution"], "engine": r["engine"], "strategy": r.get("strategy", ""),
            "atomic_swap": r.get("atomic_swap"), "scale": r["scale"], "rows": r["rows"], "ok": r.get("ok"),
            "initial_load_s": il["total_s"],
            "refresh_median_s": round(statistics.median(rt), 1) if rt else None,
            "refresh_min_s": min(rt) if rt else None, "refresh_max_s": max(rt) if rt else None,
            "n_refresh": len(rt),
            "load_median_s": round(statistics.median(loads), 1),
            "headline_s": round(statistics.median(rt), 1) if rt else il["total_s"],
            "rows_per_s": None,
            "cpu_avg_pct": il.get("cpu_avg_pct"),
            "disk_peak_refresh_gb": max((x.get("disk_peak_delta_gb", 0) for x in refreshes), default=None),
            "reader_errors": sum(x["reader"]["errors"] for x in refreshes) if refreshes else None,
            "reader_missing": sum(x["reader"]["missing_rows"] for x in refreshes) if refreshes else None,
            "reader_max_stall_s": max((x["reader"]["max_stall_s"] for x in refreshes), default=None),
            "reader_p99_ms": max((x["reader"].get("p99_ms", 0) for x in refreshes), default=None),
            "upsert_1pct_s": t(inc, "upsert_1pct"), "upsert_10pct_s": t(inc, "upsert_10pct"),
            "replace_tenant_s": t(inc, "replace_tenant"),
            "storage_gb": round(st.get("total", 0) / 2**30, 2) if st.get("total") else None,
            "storage_table_gb": round(st["table"] / 2**30, 2) if st.get("table") else None,
            "storage_after_gb": round(sta.get("total", 0) / 2**30, 2) if sta.get("total") else None,
            "data_dir_gb": round(st.get("data_dir_bytes", 0) / 2**30, 2) if st.get("data_dir_bytes") else None,
            "qps_8c": conc.get("qps"), "conc_p99_ms": conc.get("p99_ms"),
            "phases": il.get("phases", {}),
            "refresh_phases": refreshes[-1].get("phases", {}) if refreshes else {},
            "info": r.get("info", {}),
        }
        row["rows_per_s"] = round(r["rows"] / row["headline_s"])
        for name in spec.QUERIES:
            row[f"{name}_p50_ms"] = q.get(name, {}).get("p50_ms")
            row[f"{name}_p95_ms"] = q.get(name, {}).get("p95_ms")
            row[f"{name}_ok"] = q.get(name, {}).get("verified")
            row[f"{name}_after_p50_ms"] = qa.get(name, {}).get("p50_ms")
        rows.append(row)
    return rows


# --------------------------------------------------------------------------- formatting
def fmt_t(s) -> str:
    if s is None:
        return "–"
    if s < 10:
        return f"{s:.1f} s"
    if s < 120:
        return f"{s:.0f} s"
    return f"{int(s // 60)} min {int(round(s % 60)):02d} s"


def fmt_ms(v) -> str:
    if v is None:
        return "–"
    return f"{v:.2f}" if v < 10 else f"{v:.0f}"


def fmt(v, nd=1) -> str:
    return "–" if v is None else f"{v:,.{nd}f}"


def md_table(header: list[str], body: list[list[str]], align: str | None = None) -> str:
    align = align or "l" + "r" * (len(header) - 1)
    sep = [":--" if a == "l" else "--:" for a in align]
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(sep) + " |"]
    lines += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(lines)


def order(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (r.get("headline_s") is None, r.get("headline_s") or 0))


def tables_for_scale(rows: list[dict], scale: str) -> str:
    rs = order([r for r in rows if r["scale"] == scale and not r.get("failed")])
    if not rs:
        return ""
    out = [f"## {scale} rows\n"]
    out.append("### Full refresh\n")
    out.append(md_table(
        ["Solution", "Initial load", "Refresh over live table (median)", "min – max", "rows/s", "≤ 5 min",
         "Reader errors / missing", "Longest reader stall", "Peak extra disk"],
        [[f"`{r['solution']}`", fmt_t(r["initial_load_s"]), "**" + fmt_t(r["refresh_median_s"]) + "**",
          f"{fmt_t(r['refresh_min_s'])} – {fmt_t(r['refresh_max_s'])}" if r["n_refresh"] > 1 else "–",
          f"{r['rows_per_s']:,}", "yes" if r["headline_s"] <= TARGET_S else "no",
          "–" if r["reader_errors"] is None else f"{r['reader_errors']} / {r['reader_missing']}",
          "–" if r["reader_max_stall_s"] is None else fmt_t(r["reader_max_stall_s"]),
          "–" if r["disk_peak_refresh_gb"] is None else f"{r['disk_peak_refresh_gb']:.1f} GB"] for r in rs]))
    out.append("\n### Maintenance instead of refresh\n")
    out.append(md_table(
        ["Solution", "Upsert 1 %", "Upsert 10 %", "Replace largest tenant (~5 %)", "Storage after load",
         "Storage after maintenance"],
        [[f"`{r['solution']}`", fmt_t(r["upsert_1pct_s"]), fmt_t(r["upsert_10pct_s"]), fmt_t(r["replace_tenant_s"]),
          f"{fmt(r['storage_gb'], 2)} GB", f"{fmt(r['storage_after_gb'], 2)} GB"] for r in rs]))
    out.append("\n### Serving (p50 / p95 in ms, single client; QPS with 8 closed-loop clients)\n")
    out.append(md_table(
        ["Solution", "point", "series", "tenant_agg", "full_agg", "QPS (8 clients)", "point after maintenance (p50)"],
        [[f"`{r['solution']}`"] + [f"{fmt_ms(r[q + '_p50_ms'])} / {fmt_ms(r[q + '_p95_ms'])}" for q in spec.QUERIES]
         + [fmt(r["qps_8c"], 0), fmt_ms(r["point_after_p50_ms"])] for r in rs]))
    out.append("\n### Where the time goes (phases of the last refresh, seconds; phases can overlap when run in parallel)\n")
    for r in rs:
        ph = r["refresh_phases"] or r["phases"]
        out.append(f"* `{r['solution']}` — " + ", ".join(f"{k} {v:.1f}" for k, v in ph.items()))
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- charts
def _style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK2, length=0, labelsize=9)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _legend(ax, fams, **kw):
    from matplotlib.lines import Line2D
    hs = [Line2D([0], [0], marker="o", linestyle="", markersize=7, markerfacecolor=FAMILY_COLOR[f],
                 markeredgecolor=SURFACE, label=FAMILY_LABEL[f]) for f in FAMILY_ORDER if f in fams]
    leg = ax.legend(handles=hs, frameon=True, facecolor=SURFACE, edgecolor=SURFACE, framealpha=1, fontsize=9,
                    labelcolor=INK2, **kw)
    return leg


def chart_refresh(rows: list[dict], scale: str, path: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    rs = order([r for r in rows if r["scale"] == scale and not r.get("failed")])
    if not rs:
        return
    fig, ax = plt.subplots(figsize=(9, 0.42 * len(rs) + 1.6), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)
    ys = list(range(len(rs)))[::-1]
    for y, r in zip(ys, rs):
        c = FAMILY_COLOR.get(r["engine"], INK2)
        lo, hi = min(r["initial_load_s"], r["refresh_min_s"] or 1e9), max(r["initial_load_s"], r["refresh_max_s"] or 0)
        ax.plot([lo, hi], [y, y], color=c, linewidth=2, alpha=0.45, solid_capstyle="round")
        ax.plot([r["headline_s"]], [y], "o", color=c, markersize=8, markeredgecolor=SURFACE, markeredgewidth=1.5)
        ax.text(hi * 1.12, y, fmt_t(r["headline_s"]), va="center", ha="left", fontsize=9, color=INK)
    ax.set_yticks(ys)
    ax.set_yticklabels([r["solution"] for r in rs], fontsize=9, color=INK)
    ax.set_xscale("log")
    xmax = max(max(r["initial_load_s"], r["refresh_max_s"] or 0) for r in rs)
    xmin = min(min(r["initial_load_s"], r["refresh_min_s"] or 1e9) for r in rs)
    ax.set_xlim(xmin / 1.6, max(xmax * 2.6, TARGET_S * 2.2))
    ax.set_ylim(-0.7, len(rs) - 0.3)
    ax.axvline(TARGET_S, color=INK2, linewidth=1, linestyle=(0, (4, 3)))
    ax.text(TARGET_S / 1.07, -0.45, "5-minute target", fontsize=8.5, color=INK2, ha="right", va="center")
    ticks = [t for t in (5, 10, 30, 60, 120, 300, 600, 1800, 3600) if ax.get_xlim()[0] <= t <= ax.get_xlim()[1]]
    ax.set_xticks(ticks)
    ax.set_xticklabels([fmt_t(t).replace(" 00 s", "") for t in ticks])
    ax.minorticks_off()
    ax.set_title(f"Full refresh of the {scale}-row table until it is queryable (log scale)\n"
                 "dot = median refresh over the live table; line = range of all loads",
                 fontsize=10.5, color=INK, loc="left")
    _legend(ax, {r["engine"] for r in rs}, loc="upper right")
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def chart_maintenance(rows: list[dict], scale: str, path: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    rs = order([r for r in rows if r["scale"] == scale and not r.get("failed") and r.get("upsert_1pct_s")])
    if not rs:
        return
    ops = [("upsert_1pct_s", "Upsert 1 % of rows"), ("upsert_10pct_s", "Upsert 10 % of rows"),
           ("replace_tenant_s", "Replace largest tenant (~5 %)"), ("headline_s", "Full refresh")]
    fig, axes = plt.subplots(1, 4, figsize=(12, 0.36 * len(rs) + 1.5), dpi=200, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    ys = list(range(len(rs)))[::-1]
    vals = [r[k] for r in rs for k, _ in ops if r.get(k)]
    lo, hi = min(vals) / 1.8, max(vals) * 6
    for ax, (k, title) in zip(axes, ops):
        _style(ax)
        for y, r in zip(ys, rs):
            v = r.get(k)
            if v is None:
                ax.text(lo * 1.3, y, "failed", fontsize=8, color=INK2, va="center")
                continue
            c = FAMILY_COLOR.get(r["engine"], INK2)
            ax.plot([v], [y], "o", color=c, markersize=7, markeredgecolor=SURFACE, markeredgewidth=1.2)
            ax.text(v * 1.35, y, fmt_t(v), va="center", ha="left", fontsize=8, color=INK)
        ax.set_xscale("log")
        ax.set_xlim(lo, hi)
        ax.minorticks_off()
        ticks = [t for t in (0.1, 1, 10, 60, 600, 3600) if lo <= t <= hi]
        ax.set_xticks(ticks)
        ax.set_xticklabels([fmt_t(t).replace(" 00 s", "").replace(".0 s", " s") for t in ticks], fontsize=8)
        ax.set_title(title, fontsize=9.5, color=INK, loc="left")
    axes[0].set_yticks(ys)
    axes[0].set_yticklabels([r["solution"] for r in rs], fontsize=9, color=INK)
    fig.suptitle(f"Maintaining the {scale}-row table instead of rebuilding it (same log scale in every panel)",
                 fontsize=10.5, color=INK, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def chart_storage(rows: list[dict], scale: str, path: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    rs = [r for r in rows if r["scale"] == scale and not r.get("failed") and r.get("storage_gb")]
    rs = sorted(rs, key=lambda r: r["storage_gb"])
    if not rs:
        return
    fig, ax = plt.subplots(figsize=(9, 0.36 * len(rs) + 1.3), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)
    ys = list(range(len(rs)))[::-1]
    for y, r in zip(ys, rs):
        c = FAMILY_COLOR.get(r["engine"], INK2)
        ax.barh(y, r["storage_gb"], height=0.55, color=c)
        ax.text(r["storage_gb"] + 0.15, y, f"{r['storage_gb']:.1f} GB", va="center", fontsize=9, color=INK)
    ax.set_yticks(ys)
    ax.set_yticklabels([r["solution"] for r in rs], fontsize=9, color=INK)
    ax.set_xlim(0, max(r["storage_gb"] for r in rs) * 1.15)
    ax.set_xlabel("GB on disk after the load (engine-reported, table + access paths)", fontsize=9, color=INK2)
    ax.set_title(f"Storage footprint of the {scale}-row table", fontsize=10.5, color=INK, loc="left")
    _legend(ax, {r["engine"] for r in rs}, loc="lower right")
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def chart_scaling(rows: list[dict], path: Path):
    """Seconds vs rows for every solution measured at >= 2 scales."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    by: dict[str, list[dict]] = {}
    for r in rows:
        if not r.get("failed"):
            by.setdefault(r["solution"], []).append(r)
    by = {k: sorted(v, key=lambda r: r["rows"]) for k, v in by.items() if len(v) >= 2}
    if not by:
        return
    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ends = []
    for name, v in by.items():
        c = FAMILY_COLOR.get(v[0]["engine"], INK2)
        xs, ys = [r["rows"] / 1e6 for r in v], [r["headline_s"] for r in v]
        ax.plot(xs, ys, "-o", color=c, linewidth=2, markersize=5, markeredgecolor=SURFACE)
        ends.append((ys[-1], xs[-1], name))
    # de-collide end labels on the log axis
    ends.sort()
    last = 0
    for y, x, name in ends:
        yy = max(y, last * 1.17)
        ax.text(x * 1.04, yy, name, fontsize=8, color=INK, va="center")
        last = yy
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.axhline(TARGET_S, color=INK2, linewidth=1, linestyle=(0, (4, 3)))
    ax.text(ax.get_xlim()[0] * 1.02, TARGET_S * 1.08, "5-minute target", fontsize=8.5, color=INK2)
    xs_all = sorted({r["rows"] / 1e6 for v in by.values() for r in v})
    ax.set_xticks(xs_all)
    ax.set_xticklabels([f"{x:g}M" for x in xs_all])
    ax.set_xlim(xs_all[0] * 0.85, xs_all[-1] * 2.4)
    ax.minorticks_off()
    yt = [t for t in (1, 5, 10, 30, 60, 300, 600, 1800, 3600) if ax.get_ylim()[0] <= t <= ax.get_ylim()[1]]
    ax.set_yticks(yt)
    ax.set_yticklabels([fmt_t(t).replace(" 00 s", "").replace(".0 s", " s") for t in yt])
    ax.set_xlabel("rows", fontsize=9, color=INK2)
    ax.set_title("Refresh time vs table size (log-log; a straight 45° line = linear scaling)", fontsize=10.5,
                 color=INK, loc="left")
    _legend(ax, {v[0]["engine"] for v in by.values()}, loc="upper left")
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="final")
    a = ap.parse_args()
    rows = load(a.tag)
    if not rows:
        raise SystemExit(f"no results with tag {a.tag}")
    CHARTS.mkdir(parents=True, exist_ok=True)
    flat = [{k: v for k, v in r.items() if k not in ("phases", "refresh_phases", "info")} for r in rows]
    keys = sorted({k for r in flat for k in r}, key=lambda k: (k not in ("solution", "scale"), k))
    with open(OUT / "summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(flat)
    scales = sorted({r["scale"] for r in rows}, key=lambda s: -spec.SCALES[s])
    md = ["# Generated result tables\n",
          f"Generated by `python -m bench.report --tag {a.tag}` from `results/raw/*__{a.tag}.json`. "
          "Do not edit by hand.\n"]
    failed = [r for r in rows if r.get("failed") or not r.get("ok")]
    if failed:
        md.append("**Runs with a failed verification or a fatal error:** "
                  + ", ".join(f"`{r['solution']}` @ {r['scale']}" for r in failed) + "\n")
    for s in scales:
        md.append(tables_for_scale(rows, s))
        chart_refresh(rows, s, CHARTS / f"refresh_{s}.png")
        chart_maintenance(rows, s, CHARTS / f"maintenance_{s}.png")
        chart_storage(rows, s, CHARTS / f"storage_{s}.png")
    chart_scaling(rows, CHARTS / "scaling.png")
    (OUT / "RESULTS.md").write_text("\n".join(md))
    print(f"wrote {OUT/'summary.csv'}, {OUT/'RESULTS.md'}, charts for {scales}")


if __name__ == "__main__":
    main()
