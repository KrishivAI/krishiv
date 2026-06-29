#!/usr/bin/env bash
# TPC-DS / TPC-H benchmark regression gate.
#
# Runs the TPC-DS smoke bench (and optionally the TPC-H bench if
# the data dir is set) and asserts each query completes within
# `QUERY_TIMEOUT_MS` (see `krishiv-bench/src/tpcds.rs`). On any
# timeout the script exits non-zero so a CI job can fail the build.
#
# Usage:
#   ./scripts/bench-tpcds-gate.sh
#   KRISHIV_TPCDS_DATA_DIR=/data/tpcds ./scripts/bench-tpcds-gate.sh
#
# Environment:
#   KRISHIV_TPCDS_DATA_DIR   directory of TPC-DS Parquet files
#   KRISHIV_TPCH_DATA_DIR_SF10  directory of TPC-H SF10 Parquet files
#                              (optional, drives the TPC-H run)
#   KRISHIV_BENCH_SKIP_TPCDS   when set to a non-empty value, skip the
#                              TPC-DS run entirely
#   KRISHIV_BENCH_SKIP_TPCH    same for TPC-H

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "==> Krishiv benchmark regression gate"
echo "    repo root: ${REPO_ROOT}"

# ── TPC-DS ────────────────────────────────────────────────────────────────

if [[ -z "${KRISHIV_BENCH_SKIP_TPCDS:-}" ]]; then
    if [[ -z "${KRISHIV_TPCDS_DATA_DIR:-}" ]]; then
        echo "WARN: KRISHIV_TPCDS_DATA_DIR not set — skipping TPC-DS gate"
    elif [[ ! -d "${KRISHIV_TPCDS_DATA_DIR}" ]]; then
        echo "WARN: KRISHIV_TPCDS_DATA_DIR is not a directory — skipping TPC-DS gate"
    else
        echo "==> TPC-DS smoke bench"
        echo "    data dir: ${KRISHIV_TPCDS_DATA_DIR}"
        cd "${REPO_ROOT}"
        CXXFLAGS="-include cstdint" cargo bench \
            -p krishiv-bench --bench tpcds_smoke \
            --no-fail-fast -- --output-format bencher
    fi
else
    echo "==> TPC-DS smoke bench (skipped via KRISHIV_BENCH_SKIP_TPCDS)"
fi

# ── TPC-H ─────────────────────────────────────────────────────────────────

if [[ -z "${KRISHIV_BENCH_SKIP_TPCH:-}" ]]; then
    if [[ -z "${KRISHIV_TPCH_DATA_DIR_SF10:-${KRISHIV_TPCH_DATA_DIR:-}}" ]]; then
        echo "WARN: KRISHIV_TPCH_DATA_DIR_SF10 not set — skipping TPC-H gate"
    else
        TPCH_DIR="${KRISHIV_TPCH_DATA_DIR_SF10:-${KRISHIV_TPCH_DATA_DIR}}"
        if [[ ! -d "${TPCH_DIR}" ]]; then
            echo "WARN: TPC-H data dir is not a directory — skipping TPC-H gate"
        else
            echo "==> TPC-H SF10 bench"
            echo "    data dir: ${TPCH_DIR}"
            cd "${REPO_ROOT}"
            KRISHIV_TPCH_DATA_DIR_SF10="${TPCH_DIR}" CXXFLAGS="-include cstdint" cargo bench \
                -p krishiv-bench --bench tpch_sf10 \
                --no-fail-fast -- --output-format bencher
        fi
    fi
else
    echo "==> TPC-H bench (skipped via KRISHIV_BENCH_SKIP_TPCH)"
fi

echo "==> benchmark gate complete"
