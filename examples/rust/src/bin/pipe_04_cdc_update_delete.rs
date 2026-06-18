//! Pipeline 04 · IVM/CDC: updates + deletes with Z-set retraction.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_04_cdc_update_delete
#![forbid(unsafe_code)]
use std::sync::{Arc, Mutex};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_api::pipeline::CdcChange;
use krishiv_api::RunPolicy;

fn row(id: i64, amount: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![Arc::new(Int64Array::from(vec![id])), Arc::new(Int64Array::from(vec![amount]))],
    ).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::builder().build()?;
    let sink: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    // insert(100) + insert(50) = 150 ; update id2 50→200 (+150) ; delete id1 (-100) → 250.
    session.pipeline("cdc_mut")
        .source_cdc("orders", vec![
            CdcChange::insert(row(1, 100)),
            CdcChange::insert(row(2, 50)),
            CdcChange::update(row(2, 50), row(2, 200)),
            CdcChange::delete(row(1, 100)),
        ])
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink_memory("revenue", sink.clone())
        .run(RunPolicy::Once).await?;
    let total = sink.lock().unwrap()[0].column(0).as_any()
        .downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    println!("[pipe_04] revenue after insert/update/delete = {total}");
    assert_eq!(total, 200.0);
    Ok(())
}
