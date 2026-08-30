#!/usr/bin/env bash
# Run the TPC-H corpus on Spark in LOCAL mode, as the single-box baseline for
# `tpch_batch_sweep.sh`.
#
# `spark_submit_tpch.sh` and its single-node twin submit to Kubernetes and read
# from MinIO over s3a. That is the right shape for comparing cluster
# topologies, and the wrong shape for comparing engines on one machine: it
# measures the object store at least as much as the engine. This runs Spark
# against the SAME local Parquet directories the Krishiv sweep reads, on the
# same box, so the storage path is identical on both sides.
#
# # Matching the resources, which is the whole point
#
# In local mode the driver IS the executor, so `--driver-memory` is the entire
# memory budget. Left at its default it is **1 GiB**, and there is no
# spark-defaults.conf here to change that. A 600M-row aggregate in a 1 GiB heap
# spends its life in GC: an early run of q1 at SF100 took 290 s that way
# against Krishiv's 23 s, which measures the default, not the engine. So the
# budget is pinned to the same 24 GiB pool the Krishiv sweep is given.
#
# Nothing else is tuned. No AQE knobs beyond the 3.5 defaults, no broadcast
# thresholds, no statistics, no query rewrites — the same policy
# `tpch_spark_run.py` states, for the same reason: tuning one side and not the
# other is how baselines become marketing.
#
# `spark.local.dir` is pointed at real disk for the same reason the Krishiv
# sweep sets KRISHIV_QUERY_SPILL_DIR: /tmp is a tmpfs on this box, and letting
# either engine spill into RAM would flatter it and then kill it.
#
# Usage: spark_local_tpch.sh <data-dir> <out-json> [only-query-ids]
set -euo pipefail

DATA="${1:?usage: spark_local_tpch.sh <data-dir> <out-json> [only]}"
OUT="${2:?usage: spark_local_tpch.sh <data-dir> <out-json> [only]}"
ONLY="${3:-}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TOOLCHAIN="${KRISHIV_SPARK_TOOLCHAIN:-/home/gopal/krishiv-bench-data/spark-toolchain}"

JAVA_HOME="$(echo "$TOOLCHAIN"/jdk-*)"
export JAVA_HOME
VENV="$TOOLCHAIN/venv"
export PYSPARK_PYTHON="$VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"

CORES="${KRISHIV_SPARK_CORES:-$(nproc)}"
MEM="${KRISHIV_SPARK_DRIVER_MEM:-24g}"
SCRATCH="${KRISHIV_SPARK_SCRATCH:-/home/gopal/krishiv-bench-data/spark-scratch}"
mkdir -p "$SCRATCH" "$(dirname "$OUT")"

CORPUS="${KRISHIV_SPARK_CORPUS:?set KRISHIV_SPARK_CORPUS to the matching corpus JSON}"

echo "# spark local[$CORES], driver memory $MEM"
echo "# data    $DATA"
echo "# scratch $SCRATCH"

args=(--data "$DATA" --corpus-json "$CORPUS" --out "$OUT" --label "spark-local")
[ -n "$ONLY" ] && args+=(--only "$ONLY")

exec "$VENV/bin/spark-submit" \
  --master "local[$CORES]" \
  --driver-memory "$MEM" \
  --conf spark.local.dir="$SCRATCH" \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.ui.enabled=false \
  "$ROOT/scripts/bench/tpch_spark_run.py" "${args[@]}"
