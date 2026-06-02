//! Stateful streaming with event-time State TTL eviction.
//! Tested locally using in-memory streams without requiring a Kafka broker.
//! Run with: `cargo run -p krishiv-rust-examples --bin stream_state_ttl`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{
    DataType, ExecutionMode, Field, QueryResult, RecordBatch, Schema, Session, StateTtlConfig,
    StreamBatch, WatermarkSpec,
};

fn main() -> Result<(), Box<dyn Error>> {
    // 1. Configure state TTL of 5 seconds
    let ttl_config = StateTtlConfig::new(5000);

    // 2. Build session with the TTL config
    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.with_state_ttl(ttl_config).build()?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("user_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000, 15000])),
            Arc::new(StringArray::from(vec!["Alice", "Alice"])),
        ],
    )?;

    // 3. Associate the TTL directly with the memory stream
    let stream = session
        .memory_stream("user_txs", vec![StreamBatch::new(0, batch)])
        .unwrap()
        .with_state_ttl(5000);

    // 4. Define window
    let windowed = stream
        .key_by("user_id")
        .with_event_time("timestamp")
        .watermark(WatermarkSpec::fixed_lag_ms(1000))
        .tumbling_window(2000);

    // 5. Collect outputs (executes with state TTL backend eviction automatically enabled)
    let results = windowed.collect()?;

    let batches = results.into_iter().map(|b| b.batch().clone()).collect();
    println!("{}", QueryResult::new(batches).pretty()?);

    Ok(())
}
