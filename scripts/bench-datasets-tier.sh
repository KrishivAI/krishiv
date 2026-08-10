#!/usr/bin/env bash
# Nightly dataset tier (Phase 66): tpch_distributed + tpcds_smoke.
#
# `scripts/bench-tier.sh` runs only the dataset-free targets. This tier covers
# the two bench targets that need generated data but are small enough for CI
# to provision itself at SF1: `tpch_distributed` (the InProcessCluster
# submission path) over TPC-H SF1, and `tpcds_smoke` (embedded SqlEngine,
# star/snowflake plan shapes) over TPC-DS SF1. The `dataset-tier` job in
# .github/workflows/bench.yml generates both datasets (tpchgen-cli and
# scripts/bench/gen_tpcds_sf1.py respectively — deterministic per generator
# version, cached between nights) and runs this script nightly. Same
# criterion -> results.jsonl -> bench_gate.py pipeline and provenance fields
# as the other tiers, so a regression in the cluster-submission path or the
# TPC-DS planner shapes is caught the same way a streaming one is.
#
# This tier is SF1 by definition: point KRISHIV_TPCDS_DATA_DIR at SF1 data or
# the recorded tpcds_*_sf1_p50 history rows will silently mix scales.
# (tpch_distributed carries the scale in its criterion id, so extra
# KRISHIV_TPCH_DATA_DIR_SF10/_SF100 dirs on a data-rich host just add
# budget-less history rows — recorded, not gated.)
#
# Env:
#   KRISHIV_TPCH_DATA_DIR_SF1  -> TPC-H SF1 parquet dir (required)
#   KRISHIV_TPCDS_DATA_DIR     -> TPC-DS SF1 parquet dir (required)
#   BENCH_ENV                  -> honest environment label (required)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/benchmarks/results.jsonl"
COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
DATE="$(date -u +%F)"
ENV_LABEL="${BENCH_ENV:?set BENCH_ENV to an honest environment label (e.g. ci-shared, dev-box)}"

if [ ! -d "${KRISHIV_TPCH_DATA_DIR_SF1:-}" ]; then
  echo "FAIL: KRISHIV_TPCH_DATA_DIR_SF1 is not a directory — refusing to 'pass' having measured nothing" >&2
  exit 2
fi
if [ ! -d "${KRISHIV_TPCDS_DATA_DIR:-}" ]; then
  echo "FAIL: KRISHIV_TPCDS_DATA_DIR is not a directory — refusing to 'pass' having measured nothing" >&2
  exit 2
fi

# Both benches early-return from the timed closure when a table's parquet is
# missing (`tables_exist`), which would record a near-zero "measurement" of an
# empty iteration. Fail here instead of gating on a number nothing measured.
for t in customer lineitem nation orders region supplier; do
  if [ ! -f "$KRISHIV_TPCH_DATA_DIR_SF1/$t.parquet" ]; then
    echo "FAIL: $KRISHIV_TPCH_DATA_DIR_SF1/$t.parquet missing — tpch_distributed would time empty iterations" >&2
    exit 2
  fi
done
for t in customer customer_address catalog_sales date_dim web_sales store_sales store item; do
  if [ ! -f "$KRISHIV_TPCDS_DATA_DIR/$t.parquet" ]; then
    echo "FAIL: $KRISHIV_TPCDS_DATA_DIR/$t.parquet missing — tpcds_smoke would skip that query" >&2
    exit 2
  fi
done

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

echo "==> tpch_distributed (InProcessCluster submission path over the parquet dataset)"
cargo bench -p krishiv-bench --bench tpch_distributed

# group/bench-id pairs as constructed in benches/tpch_distributed.rs:
#   c.benchmark_group("tpch_distributed_q<N>") + BenchmarkId::new("<bench_name>", <sf>)
# Budgets are declared for sf1 only; sf10/sf100 rows (from a data-rich host)
# are history, not a gate.
for scale in sf1 sf10 sf100; do
  for pair in \
    "tpch_distributed_q1/q1_pricing_summary" \
    "tpch_distributed_q3/q3_shipping_priority" \
    "tpch_distributed_q5/q5_local_supplier_volume" \
    "tpch_distributed_q6/q6_forecasting_revenue" \
    "tpch_distributed_q10/q10_returned_item_reporting" \
    "tpch_distributed_q18/q18_large_volume_customer"
  do
    query="${pair%%/*}"
    if v=$(criterion_median_ms "$pair/$scale"); then
      record "${query}_${scale}_p50" "$v"
    fi
  done
done

echo "==> tpcds_smoke (embedded SqlEngine over the parquet dataset)"
cargo bench -p krishiv-bench --bench tpcds_smoke

# group/bench-id pairs as constructed in benches/tpcds_smoke.rs:
#   c.benchmark_group("tpcds_smoke") + BenchmarkId::new("query", <name>)
for q in q1 q3 q6 q12 q27; do
  if v=$(criterion_median_ms "tpcds_smoke/query/$q"); then
    record "tpcds_${q}_sf1_p50" "$v"
  else
    echo "SKIP tpcds_${q}_sf1_p50 (no criterion output)"
  fi
done

if [ "$MEASURED" -eq 0 ]; then
  echo "FAIL: every benchmark was skipped — the dataset tier measured nothing." >&2
  exit 2
fi

echo "==> recorded $MEASURED dataset-tier measurements into benchmarks/results.jsonl"

echo "==> regression gate (datasets budgets must have fresh measurements)"
python3 "$ROOT/scripts/bench_gate.py" --tier datasets --require-fresh 8
