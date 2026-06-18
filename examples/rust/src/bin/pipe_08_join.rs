//! Pipeline 08 · Multi-source incremental join (orders ⋈ customers).
//! Run: cargo run -p krishiv-rust-examples --bin pipe_08_join
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::{PipelineMode, RunPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let orders = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
            Arc::new(Int64Array::from(vec![100, 50, 25])),
        ],
    )?;
    let customers = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    session.pipeline("joined")
        .source_memory("orders", vec![orders])
        .source_memory("customers", vec![customers])
        .view("enriched",
            "SELECT o.order_id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.customer_id", true)
        .sink_memory("enriched", sink.clone())
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;
    let n: usize = sink.lock().unwrap().iter().map(|b| b.num_rows()).sum();
    println!("[pipe_08] joined rows = {n}");
    assert_eq!(n, 3);
    Ok(())
}
