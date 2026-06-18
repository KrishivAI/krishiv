//! Example 12: Schema Evolution — Delta table with changing schemas across versions.
//!
//! Demonstrates writing data with evolving schemas (adding columns) to Delta Lake,
//! and querying across versions with different schemas.
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_schema_evolution`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // v0: minimal schema
    let schema_v0 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch_v0 = RecordBatch::try_new(
        schema_v0,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )?;
    write_delta(&delta_path, vec![batch_v0], DeltaWriteMode::Overwrite, false).await?;
    println!("v0: Basic schema (id, name) — 3 records");

    // v1: add 'age' column
    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let batch_v1 = RecordBatch::try_new(
        schema_v1,
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["Dave", "Eve"])),
            Arc::new(Int32Array::from(vec![28, 35])),
        ],
    )?;
    write_delta(&delta_path, vec![batch_v1], DeltaWriteMode::Append, false).await?;
    println!("v1: Added 'age' column — 2 new records");

    // v2: add 'salary' column
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
    ]));
    let batch_v2 = RecordBatch::try_new(
        schema_v2,
        vec![
            Arc::new(Int64Array::from(vec![6, 7])),
            Arc::new(StringArray::from(vec!["Frank", "Grace"])),
            Arc::new(Int32Array::from(vec![42, 31])),
            Arc::new(Float64Array::from(vec![120000.0, 95000.0])),
        ],
    )?;
    write_delta(&delta_path, vec![batch_v2], DeltaWriteMode::Append, false).await?;
    println!("v2: Added 'salary' column — 2 new records");

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read v0 (only id, name)
    let df_v0 = session.read_delta_async(&delta_path, Some(0)).await?;
    println!("\n--- v0 data (id, name only) ---");
    println!("{}", df_v0.collect_async().await?.pretty()?);

    // Read v1 (id, name, age)
    let df_v1 = session.read_delta_async(&delta_path, Some(1)).await?;
    println!("\n--- v1 data (id, name, age) ---");
    println!("{}", df_v1.collect_async().await?.pretty()?);

    // Read latest (id, name, age, salary)
    let df_latest = session.read_delta_async(&delta_path, None).await?;
    println!("\n--- Latest data (all columns) ---");
    println!("{}", df_latest.collect_async().await?.pretty()?);

    println!("\nSchema evolution example completed successfully!");
    Ok(())
}
