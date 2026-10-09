"""
Check that filtering a model by course only reads that course from the large tables beneath it.

Superset filters every Aspects dashboard query by course, either with a literal list (course
staff, through row-level security) or with a subquery on dim_course_names. If a model change stops
that filter from reaching the event-sized tables underneath, the query still works and returns the
same rows, but reads every course; on a big instance that's the difference between milliseconds
and a timeout. Timing that in CI is too noisy, so this checks the query plan instead: every read of
a table sorted by course must use course_key in its primary key condition.

Usage: python scripts/ci/check_course_pruning.py [--models a,b,...]

Connects with CLICKHOUSE_URL (default http://localhost:8123), CLICKHOUSE_USER and
CLICKHOUSE_PASSWORD. By default it checks every view and table with a course_key column, in every
database except the system ones.
"""

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request

URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
USER = os.environ.get("CLICKHOUSE_USER", "default")
PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
SKIP_DATABASES = ("system", "information_schema", "INFORMATION_SCHEMA", "default")


def query(sql):
    """Run a query over HTTP and return the response body."""
    request = urllib.request.Request(URL, data=sql.encode())
    token = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request) as response:
            return response.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{e.read().decode()[:500]}\nin: {sql[:300]}") from None


def course_sorted_tables():
    """Tables (not dimensions) whose sorting key starts with org / course_key."""
    rows = query(
        "select database || '.' || name, sorting_key from system.tables "
        f"where database not in {SKIP_DATABASES} and engine like '%MergeTree' "
        "and name not like 'dim_%' and name not like '.inner%' format TSV"
    )
    tables = set()
    for line in rows.splitlines():
        name, key = line.split("\t")
        if "course_key" in [k.strip() for k in key.split(",")[:2]]:
            tables.add(name)
    return tables


def models_with_course_key():
    """Every view or table in the checked databases that has a course_key column."""
    rows = query(
        "select distinct database || '.' || table from system.columns "
        f"where database not in {SKIP_DATABASES} and name = 'course_key' format TSV"
    )
    return sorted(rows.split())


def reads(plan):
    """Yield (table, primary key index) for each ReadFromMergeTree step in an EXPLAIN plan."""
    node = plan.get("Plan", plan)
    if node.get("Node Type") == "ReadFromMergeTree":
        index = next((i for i in node.get("Indexes", []) if i.get("Type") == "PrimaryKey"), None)
        yield node.get("Description"), index
    for child in node.get("Plans", []):
        yield from reads(child)


def check(model, course_filter, guarded):
    """Return the guarded tables `model` reads without using course_key."""
    plan = json.loads(
        query(
            f"explain indexes = 1, json = 1 select * from {model} where {course_filter} "
            "format TSVRaw"
        )
    )
    problems = set()
    for step in plan:
        for table, index in reads(step):
            # No index analysis means the optimizer dropped the read (e.g. a filter that is
            # always false for that branch); a full read shows condition "true".
            if table in guarded and index is not None and "course_key" not in (index.get("Keys") or []):
                problems.add(table)
    return problems


def main():
    """Check the models and exit non-zero on any unfiltered read."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--models", help="comma-separated db.model list; default: all with course_key")
    args = parser.parse_args()

    guarded = course_sorted_tables()
    models = args.models.split(",") if args.models else models_with_course_key()
    names = query(
        "select database || '.' || name from system.tables where name = 'dim_course_names' "
        "format TSV"
    ).split()[0]
    course, name = query(
        f"select course_key, course_name from {names} limit 1 format TSV"
    ).strip().split("\t")
    name = name.replace("'", "\\'")
    # The two ways Superset filters by course: row-level security for course staff, and the
    # course name filter.
    filters = {
        "literal": f"course_key in ('{course}')",
        "subquery": f"course_key in (select course_key from {names} where course_name in ('{name}'))",
    }
    failed = False
    for model in models:
        for kind, course_filter in filters.items():
            try:
                problems = check(model, course_filter, guarded)
            except RuntimeError as e:
                print(f"ERROR {model} ({kind}): {e}")
                failed = True
                continue
            if problems:
                failed = True
                print(f"FAIL  {model} ({kind} course filter) reads every course of: "
                      + ", ".join(sorted(problems)))
    print(f"Checked {len(models)} models against {len(guarded)} course-sorted tables.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
