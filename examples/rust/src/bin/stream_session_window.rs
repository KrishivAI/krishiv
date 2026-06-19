//! Real-time user session grouping using Session Windows.
//! Tested locally using in-memory streams without requiring a Kafka broker.
//! Run with: `cargo run -p krishiv-rust-examples --bin stream_session_window`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{
    DataType, ExecutionMode, Field, QueryResult, RecordBatch, Schema, Session, StreamBatch,
    WatermarkSpec,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;

    // Alice has a 12-second gap between the third and fourth interactions (triggers session split)
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("user_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000, 5000, 8000, 20000])),
            Arc::new(StringArray::from(vec!["Alice", "Alice", "Alice", "Alice"])),
        ],
    )?;

    let stream = session
        .memory_stream("clicks", vec![StreamBatch::new(0, batch)])
        .unwrap();

    let session_windowed = stream
        .key_by("user_id")
        .with_event_time("timestamp")
        .watermark(WatermarkSpec::fixed_lag_ms(2000))
        .session_window(10000); // 10-second inactivity gap

    let results = session_windowed.collect()?;

    let batches = results.into_iter().map(|b| b.batch().clone()).collect();
    println!("{}", QueryResult::new(batches).pretty()?);

    Ok(())
}
