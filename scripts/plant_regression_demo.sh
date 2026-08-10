#!/usr/bin/env bash
# Planted-regression proof for the benchmark gate (Phase 66).
#
# A regression gate that has never been seen to fail is an assumption, not a
# gate. This script proves scripts/bench_gate.py catches a real, measured
# slowdown, end to end and entirely locally — no CI involvement, and nothing
# under benchmarks/ is touched (the gate reads budgets.json/results.jsonl
# relative to its own location, so it runs against a scratch copy of that
# layout in a temp dir):
#
#   1. runs the smallest gated bench (streaming_latency_embedded — the
#      1ms-budget nightly-tier path, ~140µs per iteration when healthy)
#      untouched -> the "night 0" baseline row, which must PASS the gate;
#   2. re-runs it twice with KRISHIV_BENCH_PLANT_REGRESSION_MS=$PLANT_MS —
#      the bench harness's own gate-self-test hook (see
#      crates/krishiv-bench/benches/streaming_latency.rs) that sleeps that
#      long inside every timed iteration — planting a known ~25x regression
#      as two more "nights" of history (two runs because the gate's noise
#      rule only FAILS on a sustained, 2-consecutive-runs breach; one
#      planted run alone must and does only warn);
#   3. runs the real bench_gate.py over that 3-row history and EXPECTS it
#      to exit 1 with a SUSTAINED breach of the planted path.
#
# Exits 0 only when the gate correctly flags the plant. Any other outcome
# exits nonzero — including a baseline that already breaches (host too
# noisy to demonstrate anything; rerun on a quieter box) and a planted
# median below the planted sleep (hook not reaching the timed region).
#
# Usage: scripts/plant_regression_demo.sh
#        (a few minutes once krishiv-bench's bench profile is in target/)
# Env:   PLANT_MS -> planted per-iteration sleep in ms, default 25
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLANT_MS="${PLANT_MS:-25}"
BENCH_PATH="streaming_latency_embedded_p50"
CRITERION_ID="streaming_latency_embedded/embedded_1k_row_batch_steady_state"
COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
DATE="$(date -u +%F)"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Scratch gate: same scripts/ + benchmarks/ shape the real gate resolves its
# files through, seeded with the verbatim gate code and the real committed
# budget for the planted path (and only that one, so the output is crisp).
mkdir -p "$WORK/scripts" "$WORK/benchmarks"
cp "$ROOT/scripts/bench_gate.py" "$WORK/scripts/bench_gate.py"
python3 - "$ROOT/benchmarks/budgets.json" "$WORK/benchmarks/budgets.json" "$BENCH_PATH" <<'EOF'
import json, sys
src, dst, path = sys.argv[1], sys.argv[2], sys.argv[3]
budgets = [b for b in json.load(open(src))["budgets"] if b["path"] == path]
assert budgets, f"{path} is not declared in {src}"
json.dump({"budgets": budgets}, open(dst, "w"))
EOF
BUDGET_MS="$(python3 -c "
import json, sys
print(json.load(open(sys.argv[1]))['budgets'][0]['budget_ms'])
" "$WORK/benchmarks/budgets.json")"

criterion_median_ms() {
  python3 -c "
import json, sys
print(json.load(open(sys.argv[1]))['median']['point_estimate'] / 1e6)
" "$ROOT/target/criterion/$CRITERION_ID/new/estimates.json"
}

record() { # value_ms
  printf '{"path": "%s", "value_ms": %s, "commit": "%s", "date": "%s", "env": "gate-self-test"}\n' \
    "$BENCH_PATH" "$1" "$COMMIT" "$DATE" >>"$WORK/benchmarks/results.jsonl"
}

run_bench() {
  cargo bench -p krishiv-bench --bench streaming_latency -- streaming_latency_embedded
}

echo "==> night 0: baseline (no plant)"
run_bench
BASELINE_MS="$(criterion_median_ms)"
record "$BASELINE_MS"
echo "    baseline median: ${BASELINE_MS} ms (budget: ${BUDGET_MS} ms)"

echo "==> gate on the baseline-only history (must pass)"
if ! python3 "$WORK/scripts/bench_gate.py"; then
  echo "FAIL: the gate rejects the un-planted baseline — either this host is too" >&2
  echo "      noisy to demonstrate anything (rerun on a quieter box) or the" >&2
  echo "      budget is genuinely breached today (investigate that first)." >&2
  exit 1
fi

for night in 1 2; do
  echo "==> night $night: planted run (KRISHIV_BENCH_PLANT_REGRESSION_MS=${PLANT_MS})"
  KRISHIV_BENCH_PLANT_REGRESSION_MS="$PLANT_MS" run_bench
  PLANTED_MS="$(criterion_median_ms)"
  record "$PLANTED_MS"
  echo "    planted median: ${PLANTED_MS} ms"
  if ! python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)" \
    "$PLANTED_MS" "$PLANT_MS"; then
    echo "FAIL: planted median ${PLANTED_MS} ms < planted sleep ${PLANT_MS} ms — the" >&2
    echo "      hook never reached the timed region; the demo proves nothing." >&2
    exit 1
  fi
done

echo "==> gate on the planted 3-night history (must FAIL with a SUSTAINED breach)"
set +e
GATE_OUT="$(python3 "$WORK/scripts/bench_gate.py" 2>&1)"
GATE_RC=$?
set -e
printf '%s\n' "$GATE_OUT"

if [ "$GATE_RC" -ne 1 ]; then
  echo "FAIL: expected the gate to exit 1 on the planted regression, got rc=${GATE_RC}" >&2
  exit 1
fi
if ! printf '%s\n' "$GATE_OUT" | grep -q "SUSTAINED ${BENCH_PATH}"; then
  echo "FAIL: gate exited 1, but not with a SUSTAINED breach of ${BENCH_PATH}" >&2
  exit 1
fi

echo
echo "PROOF OK: bench_gate.py flagged the planted ${PLANT_MS}ms/iteration regression as a"
echo "SUSTAINED breach of ${BENCH_PATH} (baseline ${BASELINE_MS} ms -> planted"
echo "${PLANTED_MS} ms against the ${BUDGET_MS} ms budget) and exited 1 — exactly what"
echo "the nightly job would do. Nothing under benchmarks/ was modified."
