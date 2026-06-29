#!/usr/bin/env bash
# IVM scenario tests against the coordinator HTTP API.
# Usage: ./scripts/test_ivm_scenarios.sh [base_url]
# Default base_url: http://localhost:30002
set -euo pipefail

BASE="${1:-http://localhost:30002}"
PASS=0
FAIL=0

pass() { echo "PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

ipc_b64() {
    # Build Arrow IPC base64 for a batch with schema {amount: Float64, _weight: Int64}
    python3 - <<'PYEOF'
import pyarrow as pa, base64, sys
schema = pa.schema([pa.field('amount', pa.float64()), pa.field('_weight', pa.int64())])
batch = pa.record_batch({'amount': [100.0, 200.0, 50.0], '_weight': [1, 1, 1]}, schema=schema)
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, schema) as w:
    w.write_batch(batch)
print(base64.b64encode(sink.getvalue().to_pybytes()).decode(), end='')
PYEOF
}

decode_snap() {
    local b64="$1"
    python3 - "$b64" <<'PYEOF'
import sys, base64, pyarrow as pa
raw = base64.b64decode(sys.argv[1])
# Strip 8-byte magic if present
data = raw[8:] if raw[:8] != b'\x00\x00\x00\x00\x00\x00\x00\x00' else raw[8:]
data = raw[8:]
try:
    reader = pa.ipc.open_stream(pa.py_buffer(data))
    batches = [b for b in reader]
    if batches:
        print(batches[0].to_pydict())
    else:
        print("(empty)")
except Exception as e:
    print(f"decode error: {e}")
PYEOF
}

cleanup_job() {
    local job="$1"
    curl -s -X DELETE "$BASE/api/v1/ivm/jobs/$job" > /dev/null 2>&1 || true
}

echo "============================================================"
echo " IVM Scenario Tests — $BASE"
echo "============================================================"

# ── Scenario A: SUM no GROUP BY, no executors ────────────────────────────────
echo ""
echo "--- Scenario A: SUM no GROUP BY, local step ---"
cleanup_job "test-a"

curl -sf -X POST "$BASE/api/v1/ivm/jobs" \
    -H 'Content-Type: application/json' \
    -d '{"job_id":"test-a"}' > /dev/null

curl -sf -X POST "$BASE/api/v1/ivm/jobs/test-a/views" \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "total_sales",
        "body_sql": "SELECT SUM(amount) AS total FROM sales",
        "output_schema": {
            "fields": [{"name":"total","data_type":"Float64","nullable":true}]
        },
        "is_materialized": true
    }' > /dev/null

FEED_B64=$(ipc_b64)
curl -sf -X POST "$BASE/api/v1/ivm/jobs/test-a/sources/sales/feed" \
    -H 'Content-Type: application/json' \
    -d "{\"delta_ipc_b64\": \"$FEED_B64\"}" > /dev/null

STEP=$(curl -sf -X POST "$BASE/api/v1/ivm/jobs/test-a/step")
ACTIVE=$(echo "$STEP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['active_views'])")
ROWS=$(echo "$STEP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['total_output_rows'])")

if [ "$ACTIVE" = "1" ] && [ "$ROWS" = "1" ]; then
    pass "Scenario A: step active_views=1 total_output_rows=1"
else
    fail "Scenario A: step returned active_views=$ACTIVE total_output_rows=$ROWS (expected 1 1)"
fi

SNAP=$(curl -sf "$BASE/api/v1/ivm/jobs/test-a/views/total_sales/snap")
NUM_ROWS=$(echo "$SNAP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['num_rows'])")
IPC_B64=$(echo "$SNAP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('snapshot_ipc_b64',''))")

if [ "$NUM_ROWS" = "1" ] && [ -n "$IPC_B64" ]; then
    pass "Scenario A: snapshot has 1 row"
    DECODED=$(decode_snap "$IPC_B64")
    echo "  Snapshot data: $DECODED"
else
    fail "Scenario A: snapshot null or 0 rows (num_rows=$NUM_ROWS, b64=${IPC_B64:0:10}...)"
fi

# Debug info endpoint
DEBUG=$(curl -sf "$BASE/api/v1/ivm/jobs/test-a/views/total_sales/debug-info" 2>/dev/null || echo '{}')
echo "  Debug info: $DEBUG"

cleanup_job "test-a"

# ── Scenario B: GROUP BY, single shard ───────────────────────────────────────
echo ""
echo "--- Scenario B: GROUP BY region, local step ---"
cleanup_job "test-b"

python3 - <<'PYEOF' > /tmp/ivm_region_feed.txt
import pyarrow as pa, base64
schema = pa.schema([
    pa.field('region', pa.utf8()),
    pa.field('amount', pa.float64()),
    pa.field('_weight', pa.int64())
])
batch = pa.record_batch({
    'region': ['east', 'west', 'east'],
    'amount': [100.0, 200.0, 50.0],
    '_weight': [1, 1, 1]
}, schema=schema)
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, schema) as w:
    w.write_batch(batch)
print(base64.b64encode(sink.getvalue().to_pybytes()).decode(), end='')
PYEOF

curl -sf -X POST "$BASE/api/v1/ivm/jobs" \
    -H 'Content-Type: application/json' \
    -d '{"job_id":"test-b"}' > /dev/null

# Use single shard to force Single job (avoid auto-partitioning)
curl -sf -X POST "$BASE/api/v1/ivm/jobs/test-b/views" \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "region_sales",
        "body_sql": "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
        "output_schema": {
            "fields": [
                {"name":"region","data_type":"Utf8","nullable":true},
                {"name":"total","data_type":"Float64","nullable":true}
            ]
        },
        "is_materialized": true
    }' > /dev/null

REGION_B64=$(cat /tmp/ivm_region_feed.txt)
curl -sf -X POST "$BASE/api/v1/ivm/jobs/test-b/sources/orders/feed" \
    -H 'Content-Type: application/json' \
    -d "{\"delta_ipc_b64\": \"$REGION_B64\"}" > /dev/null

STEP_B=$(curl -sf -X POST "$BASE/api/v1/ivm/jobs/test-b/step")
ACTIVE_B=$(echo "$STEP_B" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['active_views'])")
ROWS_B=$(echo "$STEP_B" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['total_output_rows'])")

if [ "$ACTIVE_B" = "1" ] && [ "$ROWS_B" = "2" ]; then
    pass "Scenario B: step active_views=1 total_output_rows=2 (east+west)"
else
    fail "Scenario B: step returned active_views=$ACTIVE_B total_output_rows=$ROWS_B (expected 1 2)"
fi

SNAP_B=$(curl -sf "$BASE/api/v1/ivm/jobs/test-b/views/region_sales/snap")
NUM_B=$(echo "$SNAP_B" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['num_rows'])")
IPC_B=$(echo "$SNAP_B" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('snapshot_ipc_b64',''))")

if [ "$NUM_B" = "2" ] && [ -n "$IPC_B" ]; then
    pass "Scenario B: snapshot has 2 rows (one per region)"
    DECODED_B=$(decode_snap "$IPC_B")
    echo "  Snapshot data: $DECODED_B"
else
    fail "Scenario B: snapshot null or wrong rows (num_rows=$NUM_B)"
fi

DEBUG_B=$(curl -sf "$BASE/api/v1/ivm/jobs/test-b/views/region_sales/debug-info" 2>/dev/null || echo '{}')
echo "  Debug info: $DEBUG_B"

cleanup_job "test-b"

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " Results: PASS=$PASS FAIL=$FAIL"
echo "============================================================"
[ "$FAIL" -eq 0 ]
