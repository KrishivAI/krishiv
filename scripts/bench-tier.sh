#!/usr/bin/env bash
# Nightly benchmark tier (Phase 66). Runs the krishiv-bench targets that are
# fully self-contained (no external dataset), reads each tracked criterion
# result from target/criterion/<group>/<id>/new/estimates.json, and appends
# one JSONL row per measurement to benchmarks/results.jsonl (with
# commit/date/env provenance) before running the regression gate.
#
# TPC-H (tpch_sf10/tpch_distributed/tpch_overhead) and TPC-DS (tpcds_smoke)
# are deliberately NOT run here: they need KRISHIV_TPCH_DATA_DIR_*/
# KRISHIV_TPCDS_DATA_DIR pointing at pre-generated multi-GB data that CI does
# not provision, and self-skip with a stderr notice when unset — silently
# "passing" a budget nothing measured is worse than not declaring the budget
# (see benchmarks/budgets.json's _doc and BENCHMARKING.md). Run those with
# `just bench-tpch` / `scripts/bench-tpcds-gate.sh` on a machine that has the
# datasets.
#
# Env:
#   BENCH_ENV                    -> environment label (required)
#   KRISHIV_BENCH_IVM_MAX_ROWS   -> optional cap if the runner can't afford
#                                    the 10M-row point (~2GB free RAM)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/benchmarks/results.jsonl"
COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
DATE="$(date -u +%F)"
ENV_LABEL="${BENCH_ENV:?set BENCH_ENV to an honest environment label (e.g. ci-shared, dev-box)}"

MEASURED=0

# criterion_median_ms <group>/<id> -> prints the median point estimate in
# milliseconds, or returns nonzero if that bench didn't produce output
# (e.g. group skipped).
criterion_median_ms() {
  local f="$ROOT/target/criterion/$1/new/estimates.json"
  [ -f "$f" ] || return 1
  python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))
print(d['median']['point_estimate'] / 1e6)
" "$f"
}

record() { # path value_ms
  printf '{"path": "%s", "value_ms": %s, "commit": "%s", "date": "%s", "env": "%s"}\n' \
    "$1" "$2" "$COMMIT" "$DATE" "$ENV_LABEL" >>"$RESULTS"
  echo "recorded $1 = $2 ms"
  MEASURED=$((MEASURED + 1))
}

echo "==> streaming_latency"
cargo bench -p krishiv-bench --bench streaming_latency
if v=$(criterion_median_ms "streaming_latency_embedded/embedded_1k_row_batch_steady_state"); then
  record streaming_latency_embedded_p50 "$v"
else
  echo "SKIP streaming_latency_embedded_p50 (no criterion output)"
fi
if v=$(criterion_median_ms "streaming_latency_single_node/single_node_1k_row_batch_steady_state"); then
  record streaming_latency_single_node_p50 "$v"
else
  echo "SKIP streaming_latency_single_node_p50 (no criterion output)"
fi

echo "==> ivm_vs_full_recompute"
cargo bench -p krishiv-bench --bench ivm_vs_full_recompute
if v=$(criterion_median_ms "ivm_incremental_feed/10000000"); then
  record ivm_tick_p50_at_10m_rows "$v"
else
  echo "SKIP ivm_tick_p50_at_10m_rows (no criterion output — check KRISHIV_BENCH_IVM_MAX_ROWS)"
fi

echo "==> nexmark"
cargo bench -p krishiv-bench --bench nexmark
for q in q1_currency_conversion_100k q2_auction_filter_100k q5_auction_category_100k q8_person_region_100k; do
  if v=$(criterion_median_ms "nexmark_sql/$q"); then
    record "nexmark_${q}_p50" "$v"
  else
    echo "SKIP nexmark_${q}_p50 (no criterion output)"
  fi
done

if [ "$MEASURED" = 0 ]; then
  echo "FAIL: every benchmark was skipped — the nightly tier measured nothing." >&2
  exit 2
fi

echo "==> regression gate (budgets must have fresh measurements)"
python3 "$ROOT/scripts/bench_gate.py" --require-fresh 8
