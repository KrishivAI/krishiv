//! Pipeline 06 · Fan-in: multiple sources appended into one view (flows).
//! Run: cargo run -p krishiv-rust-examples --bin pipe_06_flows_fanin
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::{PipelineMode, RunPolicy};

fn ids(v: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(v.to_vec()))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("fanin")
        .source_memory("topic_a", vec![ids(&[1, 2])])
        .source_memory("topic_b", vec![ids(&[3, 4, 5])])
        .flow("all_events", "SELECT id FROM topic_a")
        .flow("all_events", "SELECT id FROM topic_b")
        .sink_memory("all_events", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    let n: usize = sink.lock().unwrap().iter().map(|b| b.num_rows()).sum();
    println!("[pipe_06] fan-in total rows = {n}");
    assert_eq!(n, 5);
    Ok(())
}
