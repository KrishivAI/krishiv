//! Application log analytics and SLA error rate calculation using SQL.
//! Run with: `cargo run -p krishiv --example batch_log_analytics`

#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let parquet_path = temp.path().join("app_logs.parquet");
    write_logs_parquet(&parquet_path)?;

    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;

    session.register_parquet("app_logs", &parquet_path)?;

    // Calculate request and error counts, and error percentage per service
    let df = session.sql(
        "SELECT service_name, \
                COUNT(*) as total_requests, \
                SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) as server_errors, \
                (SUM(CASE WHEN status_code >= 500 THEN 1.0 ELSE 0.0 END) / COUNT(*)) * 100.0 as error_rate_pct \
         FROM app_logs \
         GROUP BY service_name \
         ORDER BY error_rate_pct DESC"
    )?;

    let result = df.collect()?;
    println!("{}", result.pretty()?);

    Ok(())
}

fn write_logs_parquet(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("status_code", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "auth-service",
                "payment-service",
                "auth-service",
                "payment-service",
                "catalog-service",
                "auth-service",
            ])),
            Arc::new(Int64Array::from(vec![200, 500, 200, 200, 200, 503])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
