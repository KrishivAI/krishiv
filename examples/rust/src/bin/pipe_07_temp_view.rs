//! Pipeline 07 · Temporary view: intermediate transform not exposed to a sink.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_07_temp_view
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::{PipelineMode, RunPolicy};

fn amounts(v: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(v.to_vec()))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("tv")
        .source_memory("raw", vec![amounts(&[100, 50, 300, 20])])
        .temp_view("big", "SELECT amount FROM raw WHERE amount >= 100")
        .view("big_count", "SELECT COUNT(*) AS n FROM big", true)
        .sink_memory("big_count", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    let n = sink.lock().unwrap()[0].column(0).as_any()
        .downcast_ref::<Int64Array>().unwrap().value(0);
    println!("[pipe_07] big orders via temp view = {n}");
    assert_eq!(n, 2);
    Ok(())
}
