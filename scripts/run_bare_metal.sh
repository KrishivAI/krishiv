#!/usr/bin/env bash
# Start a local bare-metal Krishiv cluster: coordinator + flight server + one executor.
# Builds with release-k8s profile (thin LTO, fast link) unless SKIP_BUILD=1.
#
# Usage:
#   bash scripts/run_bare_metal.sh
#   SLOTS=8 bash scripts/run_bare_metal.sh
#   SKIP_BUILD=1 bash scripts/run_bare_metal.sh   # reuse existing binary

set -euo pipefail

SLOTS=${SLOTS:-4}
PROFILE=${PROFILE:-release-k8s}
BINDIR="target/$PROFILE"
RUNDIR="/tmp/krishiv-bare-metal"

mkdir -p "$RUNDIR"

# ── Build ──────────────────────────────────────────────────────────────────────
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    echo "Building bare-metal binary (profile: $PROFILE)..."
    cargo build -p krishiv \
        --no-default-features --features bare-metal \
        --profile "$PROFILE"
fi

KRISHIV="$BINDIR/krishiv"
if [[ ! -x "$KRISHIV" ]]; then
    echo "ERROR: $KRISHIV not found. Run without SKIP_BUILD=1 to build first." >&2
    exit 1
fi

# ── Coordinator ────────────────────────────────────────────────────────────────
export KRISHIV_COORDINATOR_HTTP="http://127.0.0.1:18081"
export KRISHIV_FLIGHT_ADDR="127.0.0.1:50052"
export KRISHIV_COORDINATOR_URL="http://127.0.0.1:50052"

echo "Starting coordinator (gRPC :9091, HTTP :18081)..."
"$KRISHIV" coordinator \
    --grpc-addr 0.0.0.0:9091 \
    --http-addr 0.0.0.0:18081 \
    --metadata-backend json \
    --metadata-path "$RUNDIR/meta.json" \
    --insecure \
    >"$RUNDIR/coordinator.log" 2>&1 &
COORD_PID=$!
echo "  PID $COORD_PID — $RUNDIR/coordinator.log"
sleep 1

# ── Flight server ──────────────────────────────────────────────────────────────
echo "Starting Flight server (:50052)..."
"$KRISHIV" flight-server >"$RUNDIR/flight.log" 2>&1 &
FLIGHT_PID=$!
echo "  PID $FLIGHT_PID — $RUNDIR/flight.log"

# ── Executor ───────────────────────────────────────────────────────────────────
sleep 1
echo "Starting executor ($SLOTS slots)..."
"$KRISHIV" executor \
    --connect \
    --coordinator http://127.0.0.1:9091 \
    --slots "$SLOTS" \
    --task-grpc-addr 127.0.0.1:50057 \
    --barrier-grpc-addr 127.0.0.1:50058 \
    >"$RUNDIR/executor.log" 2>&1 &
EXEC_PID=$!
echo "  PID $EXEC_PID — $RUNDIR/executor.log"

echo ""
echo "Cluster running. Press Ctrl-C to stop."
echo "  Coordinator gRPC : 127.0.0.1:9091"
echo "  Flight endpoint  : 127.0.0.1:50052"

cleanup() {
    echo ""; echo "Stopping..."
    kill "$COORD_PID" "$FLIGHT_PID" "$EXEC_PID" 2>/dev/null || true
    wait "$COORD_PID" "$FLIGHT_PID" "$EXEC_PID" 2>/dev/null || true
    echo "Done."
}
trap cleanup INT TERM
wait
