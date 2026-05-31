//! Real-time sliding window with Multi-Source Watermark synchronization.
//! Tested locally using in-memory streams without requiring a Kafka broker.
//! Run with: `cargo run -p krishiv --example stream_multi_source`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{
    DataType, ExecutionMode, Field, MultiSourceWatermarkSpec, QueryResult, RecordBatch, Schema,
    Session, StreamBatch, WatermarkSpec,
};

fn main() -> Result<(), Box<dyn Error>> {
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("device_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000, 2000, 3000, 8000])),
            Arc::new(StringArray::from(vec![
                "device-1", "device-2", "device-1", "device-2",
            ])),
        ],
    )?;

    let stream = session
        .memory_stream("sensor_stream", vec![StreamBatch::new(0, batch)])
        .unwrap();

    // Reconcile watermarks dynamically across both device identifiers
    let multi_source_wm = MultiSourceWatermarkSpec::new()
        .with_source_id_column("device_id")
        .source("device-1", WatermarkSpec::fixed_lag_ms(1000))
        .source("device-2", WatermarkSpec::fixed_lag_ms(2000));

    let windowed = stream
        .key_by("device_id")
        .with_event_time("timestamp")
        .with_multi_source_watermark(multi_source_wm)
        .sliding_window(10000, 5000); // 10s window size, sliding every 5s

    let results = windowed.collect()?;

    let batches = results.into_iter().map(|b| b.batch().clone()).collect();
    println!("{}", QueryResult::new(batches).pretty()?);

    Ok(())
}
