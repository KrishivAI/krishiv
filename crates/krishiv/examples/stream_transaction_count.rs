//! Real-time transaction count using a Tumbling Event-Time Window.
//! Tested locally using in-memory streams without requiring a Kafka broker.
//! Run with: `cargo run -p krishiv --example stream_transaction_count`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{
    DataType, ExecutionMode, Field, QueryResult, RecordBatch, Schema, Session, StreamBatch,
    WatermarkSpec,
};

fn main() -> Result<(), Box<dyn Error>> {
    // 1. Build an embedded in-process session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // 2. Prepare streaming mock transaction batches (timestamp, user_id)
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("user_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000, 2000, 61000, 62000])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Alice", "Alice"])),
        ],
    )?;

    // 3. Register as a bounded memory stream (sequence=0)
    let stream_batch = StreamBatch::new(0, batch);
    let stream = session
        .memory_stream("transactions", vec![stream_batch])
        .unwrap();

    // 4. Declare event-time windowing via the fluent Rust API
    let windowed = stream
        .key_by("user_id")
        .with_event_time("timestamp")
        .watermark(WatermarkSpec::fixed_lag_ms(5000)) // 5s allowed lateness
        .tumbling_window(60000); // 1-minute tumbling window size

    // 5. Execute in-process and collect output stream batches
    let results = windowed.collect()?;

    // Extract record batches and print formatted query result
    let batches = results.into_iter().map(|b| b.batch().clone()).collect();
    println!("{}", QueryResult::new(batches).pretty()?);

    Ok(())
}
