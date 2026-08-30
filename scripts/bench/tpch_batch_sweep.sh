#!/usr/bin/env bash
# Run the TPC-H corpus through the embedded BATCH SQL path at one scale factor.
#
# Wraps `tpch_embedded_run.py` with the two settings a large-scale batch run on
# a bare-metal box gets wrong by default. Both are silent failures, which is
# why they are pinned here rather than left to the caller's memory:
#
# 1. **The memory pool is unbounded unless asked.** `query_memory_limit_from_env`
#    falls back to the *cgroup* limit, and bare metal has none — so with no
#    `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` the FairSpillPool is never built and
#    nothing spills. That is fine at SF1 and is an OOM kill at SF1000.
#
# 2. **Spill location and ceiling.** `KRISHIV_QUERY_SPILL_DIR` moves spill
#    files off `/tmp`, which on this box is a *tmpfs* — spilling to a
#    RAM-backed filesystem to relieve memory pressure adds memory pressure.
#    TMPDIR is exported alongside it so any non-DataFusion scratch follows.
#    The 100 GiB ceiling that failed q3 here is now derived from the spill
#    filesystem's free space, so it needs no override; set
#    `KRISHIV_QUERY_SPILL_MAX_DISK_BYTES` only to cap a run deliberately.
#
# The corpus JSON is regenerated per run at the matching scale factor, because
# q11's `0.0001 / SF` threshold makes the SQL itself scale-dependent: reusing
# an SF100 corpus at SF1000 measures a different query and still looks clean.
#
# Usage: tpch_batch_sweep.sh <scale-factor> <data-dir> <out-json> [timeout-s]
set -euo pipefail

SF="${1:?usage: tpch_batch_sweep.sh <sf> <data-dir> <out-json> [timeout-s]}"
DATA="${2:?usage: tpch_batch_sweep.sh <sf> <data-dir> <out-json> [timeout-s]}"
OUT="${3:?usage: tpch_batch_sweep.sh <sf> <data-dir> <out-json> [timeout-s]}"
TIMEOUT="${4:-1800}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BINARY="${KRISHIV_BINARY:-$ROOT/target/release/krishiv}"

# Spill to real disk, not the tmpfs /tmp.
SPILL_DIR="${KRISHIV_BENCH_SPILL_DIR:-/home/gopal/krishiv-bench-data/spill}"
mkdir -p "$SPILL_DIR"
export KRISHIV_QUERY_SPILL_DIR="$SPILL_DIR"
export TMPDIR="$SPILL_DIR"

# Default to half of physical RAM. The pool is not the process's whole
# footprint — Arrow buffers, the parquet reader's decompression scratch and the
# page cache all live outside it — so claiming most of RAM for the pool is how
# a "bounded" run still gets OOM-killed.
if [ -z "${KRISHIV_QUERY_MEMORY_LIMIT_BYTES:-}" ]; then
  total_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
  KRISHIV_QUERY_MEMORY_LIMIT_BYTES=$(( total_kb * 1024 / 2 ))
fi
export KRISHIV_QUERY_MEMORY_LIMIT_BYTES

[ -x "$BINARY" ] || { echo "FAIL: no krishiv binary at $BINARY" >&2; exit 2; }
[ -d "$DATA" ]   || { echo "FAIL: no dataset at $DATA" >&2; exit 2; }

CORPUS="$(dirname "$OUT")/tpch_corpus_sf${SF}.json"
mkdir -p "$(dirname "$OUT")"
"$ROOT/target/release/tpch_corpus" --scale-factor "$SF" > "$CORPUS"

echo "# scale factor   $SF"
echo "# data           $DATA"
echo "# binary         $BINARY ($(git -C "$ROOT" rev-parse --short HEAD))"
echo "# query pool     $((KRISHIV_QUERY_MEMORY_LIMIT_BYTES / 1024 / 1024 / 1024)) GiB"
echo "# spill (TMPDIR) $TMPDIR"
echo "# per-query cap  ${TIMEOUT}s"

exec python3 "$ROOT/scripts/bench/tpch_embedded_run.py" \
  --binary "$BINARY" \
  --data "$DATA" \
  --corpus-json "$CORPUS" \
  --scale "$SF" \
  --label "batch-sf${SF}" \
  --timeout "$TIMEOUT" \
  --out "$OUT"
