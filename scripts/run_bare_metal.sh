#!/bin/bash
set -e

# Setup Bare Metal Cluster on Custom Ports
mkdir -p /tmp/bare-metal-run
export KRISHIV_COORDINATOR_HTTP="http://127.0.0.1:18081"
export KRISHIV_FLIGHT_ADDR="127.0.0.1:50052"

echo "Starting Coordinator..."
cargo run -p krishiv -- coordinator --grpc-addr 0.0.0.0:9091 --http-addr 0.0.0.0:18081 --metadata-backend json --metadata-path /tmp/bare-metal-run/meta.json --insecure > /tmp/bare-metal-run/coord.log 2>&1 &
COORD_PID=$!
sleep 2

echo "Starting Flight Server..."
cargo run -p krishiv -- flight-server > /tmp/bare-metal-run/flight.log 2>&1 &
FLIGHT_PID=$!

echo "Starting Executor..."
cargo run -p krishiv -- executor --connect --coordinator http://127.0.0.1:9091 --slots 4 --task-grpc-addr 127.0.0.1:50057 --barrier-grpc-addr 127.0.0.1:50058 > /tmp/bare-metal-run/exec.log 2>&1 &
EXEC_PID=$!

sleep 3

export KRISHIV_COORDINATOR_URL="http://127.0.0.1:50052"

echo "=== Running Python Examples ==="
for ex in crates/krishiv-python/examples/*.py; do
    echo "--- $ex ---"
    /home/code/krishiv/.venv/bin/python "$ex" || true
done

echo "=== Running Rust Examples ==="
for ex in examples/*.rs; do
    ex_name=$(basename "$ex" .rs)
    echo "--- $ex_name ---"
    cargo run -p krishiv-api --example "$ex_name" || true
done
for ex in crates/krishiv/examples/*.rs; do
    ex_name=$(basename "$ex" .rs)
    echo "--- $ex_name ---"
    cargo run -p krishiv --example "$ex_name" || true
done

# Cleanup
kill $COORD_PID $FLIGHT_PID $EXEC_PID
wait $COORD_PID $FLIGHT_PID $EXEC_PID 2>/dev/null || true
echo "Tests complete."
