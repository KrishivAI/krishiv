//! Delta Lake time-travel query and audit batch example.
//! Run with: `cargo run -p krishiv --example batch_delta_audit`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // 1. Create a Delta Lake table and write Version 0
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch_v0 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;
    krishiv_lakehouse::write_delta(
        &delta_path,
        vec![batch_v0],
        krishiv_lakehouse::DeltaWriteMode::Overwrite,
        false,
    )
    .await?;

    // 2. Append to the table to create Version 1
    let batch_v1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["Charlie"])),
        ],
    )?;
    krishiv_lakehouse::write_delta(
        &delta_path,
        vec![batch_v1],
        krishiv_lakehouse::DeltaWriteMode::Append,
        false,
    )
    .await?;

    // 3. Build the embedded session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // 4. Query the latest version (Version 1)
    println!("--- Current Version (Latest) ---");
    let current_df = session.read_delta_async(&delta_path, None).await?;
    println!("{}", current_df.collect_async().await?.pretty()?);

    // 5. Query the historical version 0 (Time Travel!)
    println!("--- Historical Version 0 ---");
    let historical_df = session.read_delta_async(&delta_path, Some(0)).await?;
    println!("{}", historical_df.collect_async().await?.pretty()?);

    Ok(())
}
