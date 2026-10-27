"""
Benchmark the ClickHouse queries behind the Aspects engagement dashboards.

The SQL is rendered the way Aspects builds it: the tutor-contrib-aspects dataset templates are
rendered with the tutor settings, then with Superset's Jinja (filter_values / where_in), and
wrapped in the outer query Superset generates for each chart from its saved query_context. The
row-level-security clause can be added as Superset does for course staff (``course_key in (...)``)
or left off as for admins.

Results come from system.query_log, so they reflect ClickHouse's own accounting.

Usage:
  uv run --with jinja2 --with pyyaml scripts/bench/bench.py \\
      --aspects ../tutor-contrib-aspects --course-key <key> [--learner <username>] \\
      --label <name> [--runs 5] [--rls staff|admin] [--burst] [--charts substring]

Connection settings come from CH_URL (default http://localhost:18123), CH_USER, CH_PASSWORD.
"""

import argparse
import base64
import concurrent.futures
import json
import os
import pathlib
import re
import statistics
import sys
import time
import urllib.parse
import urllib.request
import uuid

import jinja2
import yaml

# Which dashboard charts to benchmark: (dashboard file, tab path prefixes).
DASHBOARDS = {
    "course": ("Course_Dashboard.yaml", ("Engagement/",)),
    "learner": ("Individual_Learner.yaml", ("Pages", "Problems", "Videos")),
}

TUTOR_SETTINGS = {
    "DBT_PROFILE_TARGET_DATABASE": "reporting",
    "ASPECTS_EVENT_SINK_DATABASE": "event_sink",
    "ASPECTS_XAPI_DATABASE": "openedx",
}

TIME_GRAINS = {
    "PT1H": "toStartOfHour(toDateTime({}))",
    "P1D": "toStartOfDay(toDateTime({}))",
    "P1W": "toMonday(toDateTime({}))",
    "P1M": "toStartOfMonth(toDateTime({}))",
    "P1Y": "toStartOfYear(toDateTime({}))",
}


# ---- ClickHouse ----------------------------------------------------------------------------


def ch(sql: str, settings: dict | None = None, query_id: str | None = None) -> str:
    """Run a query over HTTP and return the raw response body."""
    params = {"query_id": query_id} if query_id else {}
    params.update(settings or {})
    url = os.environ.get("CH_URL", "http://localhost:18123") + "/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, data=sql.encode(), method="POST")
    auth = f"{os.environ.get('CH_USER', 'ch_admin')}:{os.environ['CH_PASSWORD']}"
    req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=600) as resp:
        return resp.read().decode()


# ---- rendering -------------------------------------------------------------------------------


class Renderer:
    """Builds the SQL Superset sends for a chart."""

    def __init__(self, aspects: pathlib.Path):
        self.templates = aspects / "tutoraspects" / "templates"
        self.assets = self.templates / "aspects/build/aspects-superset/openedx-assets/assets"
        self.tutor_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(self.templates)), keep_trailing_newline=True
        )
        self.datasets = {}
        for f in (self.assets / "datasets").glob("*.yaml"):
            d = yaml.safe_load(f.read_text())
            self.datasets[d["uuid"]] = d
        self.charts = {}
        for f in (self.assets / "charts").glob("*.yaml"):
            d = yaml.safe_load(f.read_text())
            self.charts[d["uuid"]] = d

    def dashboard_charts(self, which: str):
        """Yield (tab, chart) for the benchmarked tabs of a dashboard."""
        file, prefixes = DASHBOARDS[which]
        dash = yaml.safe_load((self.assets / "dashboards" / file).read_text())
        pos = dash["position"]

        def tab_path(key):
            parts = []
            while key in pos:
                node = pos[key]
                if node.get("type") == "TAB":
                    parts.append(node["meta"].get("text"))
                parents = node.get("parents") or []
                key = parents[-1] if parents else None
            return "/".join(reversed(parts))

        for key, node in pos.items():
            if isinstance(node, dict) and node.get("type") == "CHART":
                tab = tab_path(key)
                if tab.startswith(prefixes):
                    yield tab, self.charts[node["meta"]["uuid"]]

    def superset_render(self, sql: str, filters: dict) -> str:
        """Render Superset's Jinja: filter values, where_in, and the Aspects column macros."""
        env = jinja2.Environment()
        env.filters["where_in"] = lambda vals: "(" + ", ".join(
            "'" + str(v).replace("'", "\\'") + "'" for v in vals
        ) + ")"
        return env.from_string(sql).render(
            filter_values=lambda col: list(filters.get(col, [])),
            # English output of the Aspects translation macros.
            translate_column=lambda col: col,
            translate_column_bool=lambda col: (
                f"CASE WHEN {col} = true THEN 'Yes' ELSE 'No' END"
            ),
        )

    def dataset_sql(self, dataset: dict, filters: dict) -> str:
        """Render a dataset's SQL: tutor templates first, then Superset's Jinja."""
        tutor_sql = self.tutor_env.from_string(dataset["sql"]).render(**TUTOR_SETTINGS)
        return self.superset_render(tutor_sql, filters)

    def chart_sql(self, chart: dict, filters: dict, rls: str | None) -> str:  # pylint: disable=too-many-locals,too-many-branches
        """Return the outer query Superset generates for the chart's first query."""
        dataset = self.datasets[chart["dataset_uuid"]]
        qc = chart["query_context"]
        qc = json.loads(qc) if isinstance(qc, str) else qc
        q = qc["queries"][0]
        metrics_def = {m["metric_name"]: m["expression"] for m in dataset.get("metrics", [])}
        columns_def = {
            c["column_name"]: c["expression"]
            for c in dataset.get("columns", [])
            if c.get("expression")
        }

        def col_expr(c):
            if isinstance(c, dict):
                expr = c.get("sqlExpression") or c.get("column_name")
                label = c.get("label") or expr
                grain = c.get("timeGrain")
            else:
                expr, label, grain = c, c, None
            expr = self.superset_render(
                self.tutor_env.from_string(columns_def.get(expr, expr)).render(**TUTOR_SETTINGS),
                filters,
            )
            if grain in TIME_GRAINS:
                expr = TIME_GRAINS[grain].format(expr)
            return expr, label

        def metric_expr(m):
            if isinstance(m, str):
                return metrics_def[m], m
            if m.get("expressionType") == "SQL":
                return m["sqlExpression"], m.get("label")
            col = m["column"]["column_name"]
            agg = m["aggregate"]
            inner = f"DISTINCT {col}" if agg == "COUNT_DISTINCT" else col
            func = "COUNT" if agg == "COUNT_DISTINCT" else agg
            return f"{func}({inner})", m.get("label")

        cols = [col_expr(c) for c in q.get("columns", [])]
        mets = [metric_expr(m) for m in q.get("metrics") or []]
        select = [f"{e} AS `{label}`" for e, label in cols] + [
            f"{e} AS `{label}`" for e, label in mets
        ]

        where = []
        for f in q.get("filters", []):
            op, col, val = f["op"], f["col"], f.get("val")
            if op == "TEMPORAL_RANGE":
                continue
            col = columns_def.get(col, col) if isinstance(col, str) else col["sqlExpression"]
            if op in ("IN", "NOT IN"):
                vals = ", ".join(_lit(v) for v in val)
                where.append(f"{col} {op} ({vals})")
            elif op in ("IS NULL", "IS NOT NULL"):
                where.append(f"{col} {op}")
            else:
                where.append(f"{col} {op} {_lit(val)}")
        extras = q.get("extras", {})
        if extras.get("where"):
            where.append(f"({extras['where']})")
        if rls:
            where.append(f"({rls})")

        sql = "SELECT " + ",\n       ".join(select or ["*"])
        sql += f"\nFROM (\n{self.dataset_sql(dataset, filters)}\n) AS virtual_table"
        if where:
            sql += "\nWHERE " + " AND ".join(where)
        if mets and cols:
            sql += "\nGROUP BY " + ", ".join(f"`{label}`" for _, label in cols)
        if extras.get("having"):
            sql += f"\nHAVING {extras['having']}"
        orderby = q.get("orderby") or []
        if orderby and mets:
            parts = []
            for m, asc in orderby:
                expr = metric_expr(m)[0]
                parts.append(f"{expr} {'ASC' if asc else 'DESC'}")
            sql += "\nORDER BY " + ", ".join(parts)
        sql += f"\nLIMIT {q.get('row_limit') or 10000}"
        return sql


def _lit(v) -> str:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    return "'" + str(v).replace("'", "\\'") + "'"


# ---- running ---------------------------------------------------------------------------------


def drop_caches() -> None:
    """Drop ClickHouse's caches (the OS page cache stays warm)."""
    for c in ("MARK CACHE", "UNCOMPRESSED CACHE", "QUERY CACHE", "MMAP CACHE"):
        ch(f"SYSTEM DROP {c}")


def run_one(name: str, sql: str, tag: str) -> dict:
    """Run a query, discarding its rows; return its id and any error."""
    qid = f"{tag}-{uuid.uuid4()}"
    start = time.monotonic()
    error = None
    try:
        ch(sql + "\nFORMAT Null", {"log_comment": tag}, qid)
    except urllib.error.HTTPError as e:  # pylint: disable=no-member
        error = e.read().decode()[:300]
    return {"chart": name, "query_id": qid, "wall_ms": (time.monotonic() - start) * 1000,
            "error": error}


def query_log(ids: list[str]) -> dict:
    """Return query_log stats keyed by query id."""
    ch("SYSTEM FLUSH LOGS")
    id_list = ", ".join(_lit(i) for i in ids)
    out = ch(
        f"""SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage,
                   exception_code
            FROM system.query_log
            WHERE query_id IN ({id_list}) AND type != 'QueryStart'
            FORMAT JSONEachRow"""
    )
    return {r["query_id"]: r for r in map(json.loads, out.splitlines())}


def main():  # pylint: disable=too-many-locals,too-many-statements
    """Render and run the benchmark, printing a summary and writing a JSON result file."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--aspects", required=True, type=pathlib.Path)
    ap.add_argument("--course-key", required=True)
    ap.add_argument("--learner", help="username for the learner dashboard")
    ap.add_argument("--label", required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--rls", choices=("staff", "admin"), default="staff")
    ap.add_argument("--burst", action="store_true", help="also run each tab's charts at once")
    ap.add_argument("--charts", help="only charts whose name contains this")
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("scripts/bench/results"))
    ap.add_argument("--render-only", action="store_true")
    args = ap.parse_args()

    r = Renderer(args.aspects)
    course_name = ch(
        f"SELECT course_name FROM event_sink.dim_course_names "
        f"WHERE course_key = {_lit(args.course_key)} FORMAT TSVRaw"
    ).strip()
    rls = f"course_key in ({_lit(args.course_key)})" if args.rls == "staff" else None

    scenarios = [("course", {"course_name": [course_name]})]
    if args.learner:
        scenarios.append(("learner", {"course_name": [course_name], "username": [args.learner]}))

    queries = []
    for dash, filters in scenarios:
        for tab, chart in r.dashboard_charts(dash):
            name = f"{dash}/{tab}/{chart['slice_name']} [{chart['uuid'][:4]}]"
            if args.charts and args.charts not in name:
                continue
            queries.append((dash, tab, name, r.chart_sql(chart, filters, rls)))

    args.out.mkdir(parents=True, exist_ok=True)
    sql_dir = args.out / f"{args.label}.sql"
    sql_dir.mkdir(exist_ok=True)
    for _, _, name, sql in queries:
        (sql_dir / (re.sub(r"[^A-Za-z0-9]+", "_", name) + ".sql")).write_text(sql + "\n")
    if args.render_only:
        print(f"Rendered {len(queries)} queries to {sql_dir}")
        return

    results = []
    for _, _, name, sql in queries:
        drop_caches()
        cold = run_one(name, sql, f"bench-{args.label}-cold")
        warm = [run_one(name, sql, f"bench-{args.label}-warm") for _ in range(args.runs)]
        results.append({"chart": name, "cold": cold, "warm": warm})
        print(f"  {name}: cold {cold['wall_ms']:.0f} ms"
              f"{' ERROR ' + cold['error'][:80] if cold['error'] else ''}", file=sys.stderr)

    bursts = []
    if args.burst:
        for dash, tab in sorted({(d, t) for d, t, _, _ in queries}):
            tab_queries = [(n, s) for d, t, n, s in queries if (d, t) == (dash, tab)]
            drop_caches()
            start = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(len(tab_queries)) as pool:
                runs = list(pool.map(
                    lambda q: run_one(q[0], q[1], f"bench-{args.label}-burst"), tab_queries
                ))
            bursts.append({"tab": f"{dash}/{tab}", "wall_ms": (time.monotonic() - start) * 1000,
                           "runs": runs})

    ids = [x["query_id"] for res in results for x in [res["cold"], *res["warm"]]]
    ids += [x["query_id"] for b in bursts for x in b["runs"]]
    log = query_log(ids)

    def stats(run):
        entry = log.get(run["query_id"], {})
        return {**run, **{k: int(v) for k, v in entry.items() if k != "query_id"}}

    summary = []
    for res in results:
        cold = stats(res["cold"])
        warm = [stats(w) for w in res["warm"]]
        summary.append({
            "chart": res["chart"],
            "cold_ms": cold.get("query_duration_ms"),
            "warm_ms_median": statistics.median([w.get("query_duration_ms", 0) for w in warm] or [0]),
            "read_rows": cold.get("read_rows"),
            "read_bytes": cold.get("read_bytes"),
            "peak_memory": max(w.get("memory_usage", 0) for w in [cold, *warm]),
            "error": cold.get("error") or next((w["error"] for w in warm if w["error"]), None),
        })
    burst_summary = [
        {"tab": b["tab"], "wall_ms": round(b["wall_ms"]),
         "errors": sum(1 for x in b["runs"] if x["error"]),
         "peak_memory_sum": sum(stats(x).get("memory_usage", 0) for x in b["runs"])}
        for b in bursts
    ]

    meta = {"label": args.label, "course_key": args.course_key, "learner": args.learner,
            "rls": args.rls, "runs": args.runs,
            "ref": os.popen("git -C .private/bench/aspects-dbt-run log --oneline -1 2>/dev/null").read().strip(),
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (args.out / f"{args.label}.json").write_text(
        json.dumps({"meta": meta, "charts": summary, "bursts": burst_summary}, indent=1)
    )

    print(f"\n{args.label}: {meta}")
    print(f"{'chart':70s} {'cold ms':>8s} {'warm ms':>8s} {'read rows':>12s} {'MiB read':>9s} {'peak MiB':>9s}")
    for s in summary:
        print(f"{s['chart'][:70]:70s} {s['cold_ms'] or 0:8d} {s['warm_ms_median']:8.0f} "
              f"{s['read_rows'] or 0:12d} {(s['read_bytes'] or 0) / 2**20:9.0f} "
              f"{s['peak_memory'] / 2**20:9.0f}{'  ERROR' if s['error'] else ''}")
    for b in burst_summary:
        print(f"burst {b['tab']:30s} wall {b['wall_ms']:7d} ms  errors {b['errors']}  "
              f"sum of peak memory {b['peak_memory_sum'] / 2**20:.0f} MiB")


if __name__ == "__main__":
    main()
