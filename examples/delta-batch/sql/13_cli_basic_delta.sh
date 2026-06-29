#!/usr/bin/env bash
# Example 13: SQL CLI — Basic Delta table operations via the krishiv CLI.
#
# Demonstrates creating a Delta table with the Rust API, then querying it
# via the CLI `krishiv table read` command.
#
# Run: bash examples/delta-batch/sql/13_cli_basic_delta.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

DELTA_DIR=$(mktemp -d)
echo "Delta table directory: $DELTA_DIR"

# Build the project first
echo "=== Building krishiv CLI ==="
cd "$REPO_ROOT"
cargo build -p krishiv --release 2>/dev/null || cargo build -p krishiv
KRISHIV_BIN="$REPO_ROOT/target/debug/krishiv"

# We need a small Rust program to write the Delta table since the CLI
# only has `table read`. Let's write a helper.
HELPER_DIR=$(mktemp -d)
cat > "$HELPER_DIR/Cargo.toml" <<'EOF'
[package]
name = "delta-helper"
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
        Field::new("id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Widget", "Gadget", "Doohickey"])),
            Arc::new(arrow::array::Float64Array::from(vec![29.99, 49.99, 19.99])),
        ],
    ).unwrap();
    write_delta("${DELTA_DIR}", vec![batch], DeltaWriteMode::Overwrite, false).await.unwrap();
    println!("Delta table written to ${DELTA_DIR}");
}
RUST

echo "=== Building Delta helper ==="
cd "$HELPER_DIR"
cargo build --release 2>/dev/null || cargo build
"$HELPER_DIR/target/debug/delta-helper"

echo ""
echo "=== Reading Delta table via CLI ==="
"$KRISHIV_BIN" table read --path "$DELTA_DIR" --format delta

echo ""
echo "=== Reading specific version ==="
"$KRISHIV_BIN" table read --path "$DELTA_DIR" --format delta --version 0

echo ""
echo "CLI basic Delta example completed!"
rm -rf "$HELPER_DIR" "$DELTA_DIR"
