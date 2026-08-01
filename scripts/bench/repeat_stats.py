"""One definition of "how repeated benchmark attempts collapse into a result".

Four runners now time the same 22 queries — `tpch_cluster_run` (distributed),
`tpch_spark_run` (external distributed), `tpch_embedded_run` (Krishiv
in-process) and `tpch_duckdb_run` (single-process reference). A comparison
across them is only meaningful if they all reduce repeats the same way, and
three copies of this logic is three chances for one of them to quietly pick the
best run instead of the median.

# The two decisions, and why they are what they are

**Median, not minimum.** Reporting best-of-N is how a benchmark becomes an
advertisement. The median is written back to `elapsed_s`, so every existing
consumer (`vs_spark.py`, the regression gate) keeps working unchanged and
silently gets the more honest number.

**Fail closed on flakiness.** If *any* attempt of a query failed, the query
reports as failed even when others passed, tagged INTERMITTENT with the count.
An intermittent failure is a defect; averaging it away is how it survives to a
release. Single-run sweeps could not see this at all.

# Why repeats are not optional

A single sample cannot be compared to anything, and the noise here is large:
q16 — which no declared primary key can affect — moved 48.63 s to 57.60 s
between two otherwise identical runs (18.4%), and q11 spanned 20.43 s to
39.33 s (92.5%) across two passes of one binary. Several per-query "findings"
during the 2026-07-31 session sat inside that band and were withdrawn.
"""

from __future__ import annotations

import statistics


def summarize(runs: list[dict], repeat: int) -> list[dict]:
    """Collapse repeated attempts into one representative row per query.

    `runs` is every attempt in execution order, each carrying at least `id`,
    `status` and `elapsed_s`. Returns one row per query in first-seen order.
    """
    order: list[str] = []
    by_id: dict[str, list[dict]] = {}
    for run in runs:
        by_id.setdefault(run["id"], []).append(run)
        if run["id"] not in order:
            order.append(run["id"])

    summary: list[dict] = []
    for query_id in order:
        attempts = by_id[query_id]
        failures = [a for a in attempts if a.get("status") != "ok"]
        if failures:
            row = dict(failures[0])
            row["repeat_count"] = len(attempts)
            row["failed_attempts"] = len(failures)
            if len(failures) < len(attempts):
                row["error"] = (
                    f"INTERMITTENT ({len(failures)}/{len(attempts)} attempts failed): "
                    f"{row.get('error', '')}"
                )
            summary.append(row)
            continue

        times = sorted(a["elapsed_s"] for a in attempts)
        median = statistics.median(times)
        # The representative is a REAL attempt whose time is nearest the median,
        # so its stage/task counts describe a run that actually happened rather
        # than a synthesised average.
        row = dict(min(attempts, key=lambda a: abs(a["elapsed_s"] - median)))
        row["repeat_count"] = len(attempts)
        row["elapsed_s"] = median
        row["elapsed_min_s"] = times[0]
        row["elapsed_median_s"] = median
        row["elapsed_max_s"] = times[-1]
        row["spread_pct"] = (
            0.0 if times[0] <= 0 else 100.0 * (times[-1] - times[0]) / times[0]
        )
        row["elapsed_all_s"] = times
        summary.append(row)

    if repeat == 1:
        # Keep a single-run record byte-comparable with every earlier sweep: no
        # repeat bookkeeping on a run that had nothing to repeat.
        for row in summary:
            for key in ("repeat_count", "elapsed_all_s"):
                row.pop(key, None)
    return summary


def print_spread(results: list[dict]) -> None:
    """The spread table. Printed only when repeats exist to spread over."""
    print(f"\n{'query':<5} {'n':>2} {'min':>9} {'median':>9} {'max':>9} {'spread':>8}")
    for row in results:
        if row.get("status") != "ok":
            print(f"{row['id']:<5} {'':>2} {row.get('status')}")
            continue
        print(
            f"{row['id']:<5} {row['repeat_count']:>2} {row['elapsed_min_s']:>9.2f} "
            f"{row['elapsed_median_s']:>9.2f} {row['elapsed_max_s']:>9.2f} "
            f"{row['spread_pct']:>7.1f}%"
        )
    print(
        "\nA delta smaller than a query's own spread is not a result. "
        "Compare medians, and treat anything inside the spread as noise."
    )
