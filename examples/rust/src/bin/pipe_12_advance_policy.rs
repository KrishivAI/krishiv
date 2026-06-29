//! Pipeline 12 · Advance policy: coalesce input by row count (OnChange vs EveryRows).
//! Run: cargo run -p krishiv-rust-examples --bin pipe_12_advance_policy
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::pipeline::CdcChange;
use krishiv_api::{PipelineMode, RunPolicy};

fn row(amount: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![amount]))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    // Step after every change (lowest latency) instead of once at the end.
    session.pipeline("stream_like")
        .source_cdc("events", vec![
            CdcChange::insert(row(10)),
            CdcChange::insert(row(20)),
            CdcChange::insert(row(30)),
        ])
        .view("running_total", "SELECT SUM(amount) AS total FROM events", true)
        .sink_memory("running_total", sink.clone())
        .mode(PipelineMode::Stream)
        .run(RunPolicy::OnChange).await?;
    let total = sink.lock().unwrap()[0].column(0).as_any()
        .downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    println!("[pipe_12] running total (advance=on_change) = {total}");
    assert_eq!(total, 60.0);
    Ok(())
}
