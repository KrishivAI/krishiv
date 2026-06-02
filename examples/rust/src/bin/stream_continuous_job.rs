//! Continuous unbounded streaming job execution.
//! Registering, submitting, pushing inputs, and polling/draining output window results.
//! Run with: `cargo run -p krishiv-rust-examples --bin stream_continuous_job`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, QueryResult, RecordBatch, Schema, Session};
use krishiv_runtime::LocalWindowExecutionSpec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;

    // 1. Submit an unbounded streaming pipeline job with count aggregation
    let spec = LocalWindowExecutionSpec::new_test_tumbling("user_id", "timestamp", 10000);
    let job_id = session.submit_stream_job("alerts_stream", spec)?;
    println!("Submitted continuous stream job ID: {}", job_id);

    // 2. Prepare and dynamically push a real-time record batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("user_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000, 2000])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;

    println!("Pushing live input batch...");
    session.push_stream_job_input(&job_id, vec![batch])?;

    // 3. Poll/drain any emitted window results asynchronously
    println!("Polling emitted window results...");
    let results = session.poll_stream_job(&job_id).await?;
    println!("Polled {} batches from continuous pipeline.", results.len());

    if !results.is_empty() {
        println!("{}", QueryResult::new(results).pretty()?);
    }

    Ok(())
}
