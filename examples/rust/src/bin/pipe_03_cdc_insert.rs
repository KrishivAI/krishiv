//! Pipeline 03 · IVM/CDC: insert change events → incremental SUM.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_03_cdc_insert
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::pipeline::CdcChange;
use krishiv_api::{PipelineMode, RunPolicy};

fn order(amount: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![amount]))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("cdc_rev")
        .source_cdc("orders", vec![
            CdcChange::insert(order(100)),
            CdcChange::insert(order(50)),
            CdcChange::insert(order(25)),
        ])
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink_memory("revenue", sink.clone())
        .run(RunPolicy::Once).await?;
    let total = sink.lock().unwrap()[0].column(0).as_any()
        .downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    println!("[pipe_03] CDC-inserted revenue = {total}");
    assert_eq!(total, 175.0);
    let _ = PipelineMode::Ivm;
    Ok(())
}
