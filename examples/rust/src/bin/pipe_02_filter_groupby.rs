//! Pipeline 02 · Batch: filter + group-by revenue per region.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_02_filter_groupby
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::{PipelineMode, RunPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let rb = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["US","EU","US","EU","APAC"])),
        Arc::new(Int64Array::from(vec![100, 50, 25, 75, 5])),
    ])?;
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("by_region")
        .source_memory("orders", vec![rb])
        .view("region_rev",
            "SELECT region, SUM(amount) AS total FROM orders WHERE amount >= 10 GROUP BY region", true)
        .sink_memory("region_rev", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    let n = sink.lock().unwrap().iter().map(|b| b.num_rows()).sum::<usize>();
    println!("[pipe_02] regions with revenue (amount>=10) = {n}");
    assert_eq!(n, 3);
    Ok(())
}
