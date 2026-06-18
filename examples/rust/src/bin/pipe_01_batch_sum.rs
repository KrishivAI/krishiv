//! Pipeline 01 · Batch: in-memory source → SUM view → memory sink.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_01_batch_sum
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::{PipelineMode, RunPolicy};

fn batch(amounts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(amounts.to_vec()))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("batch_sum")
        .source_memory("orders", vec![batch(&[100, 50, 25, 75])])
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink_memory("revenue", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    let out = sink.lock().unwrap();
    let total = out[0].column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    println!("[pipe_01] total revenue = {total}");
    assert_eq!(total, 250.0);
    Ok(())
}
