#!/usr/bin/env python3
"""Benchmark regression gate (Phase 66).

Engine-side port of the platform repo's Phase 29 gate (same budget
semantics, reused per the Phase 66 plan doc). Reads benchmarks/budgets.json
and benchmarks/results.jsonl (one JSON object per benchmark run: {"path",
"value_ms", "commit", "date", "env"}), and:

- flags any path whose LATEST result exceeds its budget by >20%
- escalates to FAIL only when the breach is sustained (the previous run of
  that path also breached) — single spikes on shared hardware warn, two
  consecutive nights fail (the noise rule from the platform's Phase 29 doc)
- prints the comparison table with provenance

Exit codes: 0 ok/warn, 1 sustained breach, 2 usage error.

Self-test: `scripts/bench_gate.py --self-test` exercises the parser and the
threshold/sustained logic with synthetic data (the phase's unit test gate).
"""

import json
import sys
from pathlib import Path

BREACH_FACTOR = 1.20


def load_budgets(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text())
    return {b["path"]: b for b in data["budgets"]}


def load_results(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def evaluate(budgets: dict[str, dict], results: list[dict]) -> tuple[list[str], list[str]]:
    """Return (warnings, failures). A failure is a sustained (2-run) breach."""
    warnings, failures = [], []
    by_path: dict[str, list[dict]] = {}
    for r in results:
        by_path.setdefault(r["path"], []).append(r)
    for path, budget in budgets.items():
        runs = by_path.get(path, [])
        if not runs:
            continue  # unmeasured budgets are gaps, not breaches
        latest = runs[-1]
        limit = budget["budget_ms"] * BREACH_FACTOR
        if latest["value_ms"] <= limit:
            continue
        msg = (
            f"{path}: {latest['value_ms']:.0f}ms vs budget {budget['budget_ms']}ms "
            f"(+{(latest['value_ms'] / budget['budget_ms'] - 1) * 100:.0f}%) "
            f"[commit {latest.get('commit', '?')[:9]} {latest.get('date', '?')} "
            f"env={latest.get('env', '?')}]"
        )
        prev_breached = len(runs) >= 2 and runs[-2]["value_ms"] > limit
        if prev_breached:
            failures.append("SUSTAINED " + msg)
        else:
            warnings.append("spike " + msg)
    return warnings, failures


def self_test() -> int:
    budgets = {
        "q": {"path": "q", "budget_ms": 100, "benchmark": "x"},
        "unmeasured": {"path": "unmeasured", "budget_ms": 10, "benchmark": "y"},
    }
    # Within budget -> clean.
    w, f = evaluate(budgets, [{"path": "q", "value_ms": 110}])
    assert not w and not f, "<=20% over budget must pass"
    # Single breach -> warning only.
    w, f = evaluate(budgets, [{"path": "q", "value_ms": 121}])
    assert len(w) == 1 and not f, "single spike warns"
    # Sustained breach -> failure.
    w, f = evaluate(
        budgets,
        [{"path": "q", "value_ms": 130}, {"path": "q", "value_ms": 125}],
    )
    assert not w and len(f) == 1, "two consecutive breaches fail"
    # Recovery resets the streak.
    w, f = evaluate(
        budgets,
        [
            {"path": "q", "value_ms": 130},
            {"path": "q", "value_ms": 90},
            {"path": "q", "value_ms": 130},
        ],
    )
    assert len(w) == 1 and not f, "a clean run in between resets to spike"
    # Unmeasured budget is a gap, never a breach.
    w, f = evaluate(budgets, [])
    assert not w and not f
    print("bench_gate self-test PASSED")
    return 0


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    # --require-fresh N: a budget with no measurement in the last N days is a
    # gate FAILURE, not a silent gap. The nightly tier passes this so "gate
    # ok" can never mean "nothing was measured" (verification-gate honesty).
    require_fresh_days = None
    if "--require-fresh" in sys.argv:
        require_fresh_days = int(sys.argv[sys.argv.index("--require-fresh") + 1])
    root = Path(__file__).resolve().parent.parent
    budgets = load_budgets(root / "benchmarks" / "budgets.json")
    results = load_results(root / "benchmarks" / "results.jsonl")
    warnings, failures = evaluate(budgets, results)
    measured = {r["path"] for r in results}
    for path, b in budgets.items():
        status = "measured" if path in measured else "NO DATA YET"
        print(f"{path:<32} budget {b['budget_ms']}ms  [{status}]")
    if require_fresh_days is not None:
        import datetime as _dt
        cutoff = _dt.date.today() - _dt.timedelta(days=require_fresh_days)
        for path in budgets:
            dates = [
                _dt.date.fromisoformat(r["date"])
                for r in results
                if r["path"] == path and r.get("date")
            ]
            if not dates or max(dates) < cutoff:
                failures.append(
                    f"STALE {path}: no measurement in the last {require_fresh_days} days "
                    f"(latest: {max(dates).isoformat() if dates else 'never'}) — "
                    "the nightly tier is not actually measuring this budget"
                )
    for w in warnings:
        print(f"WARN {w}")
    for f in failures:
        print(f"FAIL {f}")
    if failures:
        print("\nregression gate FAILED (sustained breach or stale budget)")
        return 1
    print("\nregression gate ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
