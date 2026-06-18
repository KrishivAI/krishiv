#!/usr/bin/env bash
# Example 14: SQL CLI — Time-travel audit via the krishiv CLI.
#
# Creates a Delta table with multiple versions, then queries each version
# using the CLI --version flag.
#
# Run: bash examples/delta-batch/sql/14_cli_time_travel.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

DELTA_DIR=$(mktemp -d)
echo "Delta table directory: $DELTA_DIR"

# Build the project
cd "$REPO_ROOT"
cargo build -p krishiv --release 2>/dev/null || cargo build -p krishiv
KRISHIV_BIN="$REPO_ROOT/target/debug/krishiv"

# Helper to write Delta data
HELPER_DIR=$(mktemp -d)
cat > "$HELPER_DIR/Cargo.toml" <<'EOF'
[package]
name = "delta-audit-helper"
version = "0.1.0"
edition = "2021"

[dependencies]
arrow = "58.3.0"
krishiv-connectors = { path = "../../../crates/krishiv-connectors", features = ["lakehouse", "delta"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
EOF

mkdir -p "$HELPER_DIR/src"
cat > "$HELPER_DIR/src/main.rs" <<RUST
use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_connectors::lakehouse::{write_delta, DeltaWriteMode};

#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("reading_hour", DataType::Int64, false),
    ]));

    // v0: morning readings
    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["S1", "S2", "S3"])),
            Arc::new(arrow::array::Float64Array::from(vec![22.0, 21.5, 23.1])),
            Arc::new(Int64Array::from(vec![6, 6, 6])),
        ],
    ).unwrap();
    write_delta("${DELTA_DIR}", vec![batch0], DeltaWriteMode::Overwrite, false).await.unwrap();

    // v1: midday readings
    let batch1 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["S1", "S2", "S3", "S4"])),
            Arc::new(arrow::array::Float64Array::from(vec![28.5, 27.0, 29.2, 26.8])),
            Arc::new(Int64Array::from(vec![12, 12, 12, 12])),
        ],
    ).unwrap();
    write_delta("${DELTA_DIR}", vec![batch1], DeltaWriteMode::Append, false).await.unwrap();
    println!("Written 2 versions of sensor data");
}
RUST

echo "=== Building helper ==="
cd "$HELPER_DIR"
cargo build --release 2>/dev/null || cargo build
"$HELPER_DIR/target/debug/delta-audit-helper"

echo ""
echo "=== Query latest version ==="
"$KRISHIV_BIN" table read --path "$DELTA_DIR" --format delta

echo ""
echo "=== Time-travel to v0 (morning only) ==="
"$KRISHIV_BIN" table read --path "$DELTA_DIR" --format delta --version 0

echo ""
echo "=== Time-travel to v1 (morning + midday) ==="
"$KRISHIV_BIN" table read --path "$DELTA_DIR" --format delta --version 1

echo ""
echo "CLI time-travel audit example completed!"
rm -rf "$HELPER_DIR" "$DELTA_DIR"
