//! Pipeline 11 · Connector source + sink: Parquet in, Parquet out.
//! Run: cargo run -p krishiv-rust-examples --bin pipe_11_parquet_connector
#![forbid(unsafe_code)]
use std::sync::Arc;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_connectors::parquet::{ParquetSink, ParquetSource};
use krishiv_api::pipeline::{Egress, Ingest};
use krishiv_api::{PipelineMode, RunPolicy};
use parquet::arrow::ArrowWriter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let in_path = dir.path().join("orders.parquet");
    let out_path = dir.path().join("revenue.parquet");

    // Write input parquet: amount = [100, 50, 25].
    let schema = Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)]));
    let rb = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![100, 50, 25]))])?;
    { let mut w = ArrowWriter::try_new(std::fs::File::create(&in_path)?, schema, None)?; w.write(&rb)?; w.close()?; }

    let session = Session::builder().build()?;
    session.pipeline("parq")
        .source("orders", Ingest::Connector(Box::new(ParquetSource::open(&in_path)?)))
        .view("revenue", "SELECT SUM(amount) AS total FROM orders", true)
        .sink("revenue", Egress::Connector(Box::new(ParquetSink::create(&out_path)?)))
        .mode(PipelineMode::Ivm)
        .run(RunPolicy::Once).await?;

    // Read the output parquet back.
    let result = session.read_parquet(&out_path)?.collect()?;
    let total = result.into_batches()[0].column(0).as_any()
        .downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
    println!("[pipe_11] parquet→pipeline→parquet total = {total}");
    assert_eq!(total, 175.0);
    Ok(())
}
