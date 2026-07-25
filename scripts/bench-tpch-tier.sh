#!/usr/bin/env bash
# TPC-H benchmark tier (Phase 62 "public benchmark suite + history").
#
# `scripts/bench-tier.sh` deliberately skips TPC-H because CI does not provision
# the multi-GB dataset, which left TPC-H numbers living only as one-off dated
# entries in BENCHMARKING.md — not in the committed, gate-checked history the
# other benchmarks use. This script closes that on a machine that HAS the data:
# same criterion → results.jsonl → bench_gate.py pipeline, same provenance
# fields, so a TPC-H regression is caught the same way a streaming one is.
#
# Env:
#   KRISHIV_TPCH_DATA_DIR_SF1  / _SF10 / _SF100  -> dataset roots (at least one)
#   BENCH_ENV                                     -> honest environment label
#
# Usage:
#   KRISHIV_TPCH_DATA_DIR_SF10=/home/krishiv-bench-data/tpch/sf10 \
#   BENCH_ENV=dev-box scripts/bench-tpch-tier.sh
#
# Run it on an otherwise-idle machine. A number measured against a box that is
# also compiling is not a benchmark, it is noise with a timestamp.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/benchmarks/results.jsonl"
COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
DATE="$(date -u +%F)"
ENV_LABEL="${BENCH_ENV:?set BENCH_ENV to an honest environment label (e.g. dev-box)}"

if [ -z "${KRISHIV_TPCH_DATA_DIR_SF1:-}${KRISHIV_TPCH_DATA_DIR_SF10:-}${KRISHIV_TPCH_DATA_DIR_SF100:-}${KRISHIV_TPCH_DATA_DIR:-}" ]; then
  echo "FAIL: no KRISHIV_TPCH_DATA_DIR_* set — refusing to 'pass' having measured nothing" >&2
  exit 2
fi

MEASURED=0

criterion_median_ms() {
  local f="$ROOT/target/criterion/$1/new/estimates.json"
  [ -f "$f" ] || return 1
  python3 -c "
import json, sys
print(json.load(open(sys.argv[1]))['median']['point_estimate'] / 1e6)
" "$f"
}

record() { # path value_ms
  printf '{"path": "%s", "value_ms": %s, "commit": "%s", "date": "%s", "env": "%s"}\n' \
    "$1" "$2" "$COMMIT" "$DATE" "$ENV_LABEL" >>"$RESULTS"
  echo "recorded $1 = $2 ms"
  MEASURED=$((MEASURED + 1))
}

echo "==> tpch_sf10 (embedded SqlEngine over the parquet dataset)"
cargo bench -p krishiv-bench --bench tpch_sf10

# group/bench-id pairs as constructed in benches/tpch_sf10.rs:
#   c.benchmark_group("tpch_q<N>") + BenchmarkId::new("<bench_name>", <sf>)
for scale in sf1 sf10 sf100; do
  for pair in \
    "tpch_q1/q1_pricing_summary" \
    "tpch_q3/q3_shipping_priority" \
    "tpch_q5/q5_local_supplier_volume" \
    "tpch_q6/q6_forecasting_revenue" \
    "tpch_q10/q10_returned_item_reporting" \
    "tpch_q18/q18_large_volume_customer"
  do
    query="${pair%%/*}"
    if v=$(criterion_median_ms "$pair/$scale"); then
      record "${query}_${scale}_p50" "$v"
    fi
  done
done

if [ "$MEASURED" -eq 0 ]; then
  echo "FAIL: cargo bench produced no criterion output — the dataset env vars are" >&2
  echo "      set but the benches skipped (missing <table>.parquet files?)." >&2
  exit 1
fi

echo "==> recorded $MEASURED TPC-H measurements into benchmarks/results.jsonl"

# bench_gate.py evaluates BUDGETS, not results: a recorded path with no budget
# declared in benchmarks/budgets.json is history, not a gate. That is deliberate
# for the first run — a budget invented before any measurement exists is a
# number pulled from the air. After this run, add budgets for the paths above at
# a defensible headroom over the recorded medians, and only then is TPC-H
# actually gated.
echo "==> regression gate (TPC-H paths are ungated until budgets.json declares them)"
python3 "$ROOT/scripts/bench_gate.py"
