//! Pipeline 05 · Data-quality expectations (DROP + FAIL).
//! Run: cargo run -p krishiv-rust-examples --bin pipe_05_expectations
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::{OnViolation, PipelineMode, RunPolicy};

fn amounts(v: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(v.to_vec()))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;

    // DROP: the -5 row is filtered out before the sink.
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("dq_drop")
        .source_memory("raw", vec![amounts(&[10, -5, 20])])
        .view("clean", "SELECT amount FROM raw", true)
        .expect("clean", "positive", "amount > 0", OnViolation::Drop)
        .sink_memory("clean", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    let kept: usize = sink.lock().unwrap().iter().map(|b| b.num_rows()).sum();
    println!("[pipe_05] DROP kept {kept} of 3 rows");
    assert_eq!(kept, 2);

    // FAIL: a violation aborts the run.
    let err = session.pipeline("dq_fail")
        .source_memory("raw", vec![amounts(&[10, -5])])
        .view("clean", "SELECT amount FROM raw", true)
        .expect("clean", "positive", "amount > 0", OnViolation::Fail)
        .sink_memory("clean", Arc::new(Mutex::new(Vec::new())))
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await;
    println!("[pipe_05] FAIL expectation errored as expected: {}", err.is_err());
    assert!(err.is_err());
    Ok(())
}
