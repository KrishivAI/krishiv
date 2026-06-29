//! Pipeline 09 · Dry-run validation (catch a bad pipeline before running).
//! Run: cargo run -p krishiv-rust-examples --bin pipe_09_validate
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::PipelineMode;

fn amounts(v: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(v.to_vec()))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;

    // Well-formed pipeline validates.
    let ok = session.pipeline("good")
        .source_memory("raw", vec![amounts(&[1, 2, 3])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("total", Arc::new(Mutex::new(Vec::new())))
        .mode(PipelineMode::Ivm)
        .build();
    ok.validate().await?;
    println!("[pipe_09] well-formed pipeline: VALID");

    // Sink references an undefined view → validation fails.
    let bad = session.pipeline("bad")
        .source_memory("raw", vec![amounts(&[1])])
        .view("total", "SELECT SUM(amount) AS s FROM raw", true)
        .sink_memory("missing", Arc::new(Mutex::new(Vec::new())))
        .mode(PipelineMode::Ivm)
        .build();
    let err = bad.validate().await;
    println!("[pipe_09] bad pipeline rejected: {}", err.is_err());
    assert!(err.is_err());
    Ok(())
}
