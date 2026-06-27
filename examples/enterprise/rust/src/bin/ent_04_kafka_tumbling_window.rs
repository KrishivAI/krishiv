//! Enterprise 04 · Kafka → 10-second tumbling window → console — embedded mode
//!
//! Assigns order events (with an Int64 epoch-ms timestamp) to 10-second
//! tumbling windows, computing count + sum(amount) per (window, customer).
//!
//! Uses `execute_windowed_stream` from `krishiv` (re-exported from
//! `krishiv_runtime`) with `LocalWindowExecutionSpec`.
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_04_kafka_tumbling_window

use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{AggExpr, AggFunction};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind, execute_windowed_stream};

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 04: Tumbling Window (embedded) ===");

    // Events spanning three 10-second windows (base_ms, base+10s, base+20s).
    let base: i64 = 1_716_200_000_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
        Field::new("ts",       DataType::Int64,   false),
    ]));

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec![
            "alice", "bob",   "carol", "alice", "dave",
            "bob",   "eve",   "alice", "carol", "frank",
        ])),
        Arc::new(Float64Array::from(vec![
            120.0, 45.0, 999.0, 340.0, 77.0,
            210.0, 55.0, 130.0, 820.0, 60.0,
        ])),
        Arc::new(Int64Array::from(vec![
            base,           base + 1_000,
            base + 3_000,   base + 5_000,
            base + 8_000,   // window 0 (0..10s)
            base + 12_000,  base + 13_000,
            base + 15_000,  base + 18_000, // window 1 (10..20s)
            base + 22_000,                 // window 2 (20..30s)
        ])),
    ])?;

    let spec = LocalWindowExecutionSpec {
        key_column:      "customer".into(),
        key_column_type: "utf8".into(),
        event_time_column: "ts".into(),
        watermark_lag_ms: 2_000,
        window_kind:     LocalWindowKind::Tumbling,
        window_size_ms:  10_000,
        agg_exprs: vec![
            AggExpr {
                function: AggFunction::Count,
                input_column: String::new(),
                output_column: "order_count".into(),
            },
            AggExpr {
                function: AggFunction::Sum,
                input_column: "amount".into(),
                output_column: "total_amount".into(),
            },
        ],
        state_ttl_ms: None,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };

    let output = execute_windowed_stream(vec![batch], &spec)?;

    println!("  {} result batches", output.len());
    let total_rows: usize = output.iter().map(|b: &RecordBatch| b.num_rows()).sum();

    if !output.is_empty() {
        let session = krishiv::Session::builder().build()?;
        session.register_record_batches("windows", output)?;
        let df = session.sql(
            "SELECT window_start_ms, window_end_ms, customer, order_count, total_amount \
             FROM windows ORDER BY window_start_ms, total_amount DESC"
        )?;
        println!("\n--- Tumbling window results (10s buckets) ---");
        println!("{}", df.collect()?.pretty()?);
    }

    println!("\n✓ {} window rows produced", total_rows);
    Ok(())
}
