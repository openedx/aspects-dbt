"""
Compare two bench.py result files as a markdown table.

Usage: python scripts/bench/compare_runs.py <before.json> <after.json>
"""

import json
import sys


def _load(path):
    with open(path) as f:
        data = json.load(f)
    return data["meta"], {c["chart"]: c for c in data["charts"]}, {b["tab"]: b for b in data["bursts"]}


def _fmt_ms(v):
    return "error" if v is None else f"{v / 1000:.2f} s" if v >= 1000 else f"{v:.0f} ms"


def _ratio(before, after):
    if not before or not after:
        return ""
    return f"{before / after:.1f}x faster" if after < before else f"{after / before:.1f}x slower"


def main():
    """Print the comparison."""
    meta_a, charts_a, bursts_a = _load(sys.argv[1])
    meta_b, charts_b, bursts_b = _load(sys.argv[2])
    print(f"**Before:** {meta_a['label']} ({meta_a['ref']}, rls={meta_a['rls']})  ")
    print(f"**After:** {meta_b['label']} ({meta_b['ref']}, rls={meta_b['rls']})\n")
    print("| Chart | Warm before | Warm after | Change | Rows read before | after | Peak MiB before | after |")
    print("|---|---|---|---|---|---|---|---|")
    for name in sorted(set(charts_a) | set(charts_b)):
        a, b = charts_a.get(name, {}), charts_b.get(name, {})
        wa = None if a.get("error") else a.get("warm_ms_median") or a.get("cold_ms")
        wb = None if b.get("error") else b.get("warm_ms_median") or b.get("cold_ms")
        print(
            f"| {name} | {_fmt_ms(wa)} | {_fmt_ms(wb)} | {_ratio(wa, wb)} "
            f"| {a.get('read_rows', 0):,} | {b.get('read_rows', 0):,} "
            f"| {a.get('peak_memory', 0) / 2**20:,.0f} | {b.get('peak_memory', 0) / 2**20:,.0f} |"
        )
    if bursts_a or bursts_b:
        print("\n| Tab burst (all charts at once) | Wall before | after | Errors before | after | Sum of peak MiB before | after |")
        print("|---|---|---|---|---|---|---|")
        for tab in sorted(set(bursts_a) | set(bursts_b)):
            a, b = bursts_a.get(tab, {}), bursts_b.get(tab, {})
            print(
                f"| {tab} | {_fmt_ms(a.get('wall_ms'))} | {_fmt_ms(b.get('wall_ms'))} "
                f"| {a.get('errors', '')} | {b.get('errors', '')} "
                f"| {a.get('peak_memory_sum', 0) / 2**20:,.0f} | {b.get('peak_memory_sum', 0) / 2**20:,.0f} |"
            )


if __name__ == "__main__":
    main()
