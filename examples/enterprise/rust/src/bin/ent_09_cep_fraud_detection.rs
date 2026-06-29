//! Enterprise 09 · CEP fraud detection — embedded mode
//!
//! Applies a Complex Event Processing pattern using `PartitionedCepMatcher<String>`:
//!
//!   login → purchase → large_txn (amount > 5000) within 90 seconds
//!
//! Each row in the input is routed to the matcher by its `event` field name,
//! which maps directly to CEP stage names. Amount filtering for `large_txn`
//! is applied before calling `process_event` (PatternStage has no predicates).
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_09_cep_fraud_detection

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_plan::cep::{PartitionedCepMatcher, Pattern};

fn single_row_batch(
    schema: Arc<Schema>,
    user: &str,
    event: &str,
    amount: f64,
    ts_ms: i64,
) -> RecordBatch {
    RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec![user])),
        Arc::new(StringArray::from(vec![event])),
        Arc::new(Float64Array::from(vec![amount])),
        Arc::new(Int64Array::from(vec![ts_ms])),
    ])
    .unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 09: CEP Fraud Detection (embedded) ===");
    println!("  pattern: login → purchase → large_txn(>5000) within 90s");

    // Build the CEP pattern using the fluent API.
    let pattern = Pattern::begin("login")
        .followed_by("purchase")
        .followed_by("large_txn")
        .within(Duration::from_secs(90))
        .compile()
        .expect("pattern compile");

    let mut matcher = PartitionedCepMatcher::<String>::new(pattern);

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8,    false),
        Field::new("event",   DataType::Utf8,    false),
        Field::new("amount",  DataType::Float64, false),
        Field::new("ts_ms",   DataType::Int64,   false),
    ]));

    // Input events — (user, event_type, amount, ts_ms).
    let events: &[(&str, &str, f64, i64)] = &[
        // u1: complete fraud sequence → ALERT
        ("u1", "login",     0.0,    1_000),
        ("u1", "purchase",  50.0,   2_000),
        ("u1", "large_txn", 9500.0, 3_000),
        // u2: login + purchase but no large_txn → no alert
        ("u2", "login",     0.0,    1_000),
        ("u2", "purchase",  120.0,  2_000),
        // u3: large_txn without prior login → no alert (sequence check)
        ("u3", "large_txn", 8000.0, 1_000),
        // u4: tries large_txn but amount too small → skip
        ("u4", "login",     0.0,    1_000),
        ("u4", "purchase",  30.0,   2_000),
        ("u4", "large_txn", 200.0,  3_000), // amount < 5000 → not forwarded to CEP
        // u1 again: second fraud sequence (pattern was consumed, starts fresh)
        ("u1", "login",     0.0,    10_000),
        ("u1", "purchase",  75.0,   11_000),
        ("u1", "large_txn", 6000.0, 12_000),
    ];

    let mut alert_count = 0usize;

    for &(user_id, event_type, amount, ts_ms) in events {
        // Pre-filter: only route large_txn to CEP if amount > 5000.
        let cep_stage = if event_type == "large_txn" && amount <= 5000.0 {
            println!("  skip  user={} event=large_txn amount={:.2} (below threshold)", user_id, amount);
            continue;
        } else {
            event_type
        };

        let batch = single_row_batch(schema.clone(), user_id, event_type, amount, ts_ms);
        let matches = matcher.process_event(user_id.to_string(), cep_stage, batch, ts_ms);

        if matches.is_empty() {
            println!("  event user={} stage={} amount={:.2} ts={}  (no match yet)", user_id, event_type, amount, ts_ms);
        } else {
            for sequence in matches {
                alert_count += 1;
                let total_events: usize = sequence.iter().map(|b| b.num_rows()).sum();
                println!("\n  🚨 FRAUD ALERT #{}", alert_count);
                println!("     user_id     = {}", user_id);
                println!("     stage_count = {}", sequence.len());
                println!("     total_rows  = {}", total_events);

                // Show each matched event batch.
                for (i, stage_batch) in sequence.iter().enumerate() {
                    let ev = stage_batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                    let am = stage_batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
                    let ts = stage_batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
                    println!("       step {}  event={}  amount={:.2}  ts={}", i, ev.value(0), am.value(0), ts.value(0));
                }
            }
        }
    }

    println!("\n--- CEP stats ---");
    println!("  active partitions : {}", matcher.partition_count());
    println!("  alerts fired      : {}", alert_count);
    println!("\n✓ CEP pipeline complete");

    Ok(())
}
