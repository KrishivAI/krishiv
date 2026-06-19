//! Pipeline 10 · Persistent incremental runs + full refresh.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_10_persistent_refresh
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
fn sum(sink: &Arc<Mutex<Vec<RecordBatch>>>) -> f64 {
    sink.lock().unwrap()[0].column(0).as_any()
        .downcast_ref::<arrow::array::Float64Array>().unwrap().value(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let mk = || -> Arc<Mutex<Vec<RecordBatch>>> { Arc::new(Mutex::new(Vec::new())) };

    let s1 = mk();
    session.pipeline("acc").source_memory("raw", vec![amounts(&[10])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", s1.clone()).mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    println!("[pipe_10] run 1 total = {}", sum(&s1));

    let s2 = mk();
    session.pipeline("acc").source_memory("raw", vec![amounts(&[5])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", s2.clone()).mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    println!("[pipe_10] run 2 total (incremental) = {}", sum(&s2));

    let s3 = mk();
    session.pipeline("acc").source_memory("raw", vec![amounts(&[100])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", s3.clone()).mode(PipelineMode::Ivm)
        .refresh(RunPolicy::Once).await?;
    println!("[pipe_10] run 3 total (after refresh) = {}", sum(&s3));

    assert_eq!((sum(&s1), sum(&s2), sum(&s3)), (10.0, 15.0, 100.0));
    Ok(())
}
