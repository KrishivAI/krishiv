//! Example 10: CDC Ingestion — Change Data Capture into Delta Lake.
//!
//! Simulates CDC events (inserts, updates, deletes) arriving in batches.
//! Each batch is written as a Delta version. Queries show the evolution
//! of the dataset across versions.
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_cdc_ingestion`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

fn users_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // CDC Batch 1: initial inserts
    let cdc1 = RecordBatch::try_new(
        users_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            Arc::new(StringArray::from(vec![
                "alice@example.com",
                "bob@example.com",
                "carol@example.com",
            ])),
            Arc::new(StringArray::from(vec!["active", "active", "active"])),
        ],
    )?;
    write_delta(&delta_path, vec![cdc1], DeltaWriteMode::Overwrite, false).await?;
    println!("CDC Batch 1: 3 users inserted");

    // CDC Batch 2: update bob's email, insert dave
    let cdc2 = RecordBatch::try_new(
        users_schema(),
        vec![
            Arc::new(Int64Array::from(vec![2, 4])),
            Arc::new(StringArray::from(vec!["bob", "dave"])),
            Arc::new(StringArray::from(vec![
                "bob.new@example.com",
                "dave@example.com",
            ])),
            Arc::new(StringArray::from(vec!["active", "active"])),
        ],
    )?;
    write_delta(&delta_path, vec![cdc2], DeltaWriteMode::Append, false).await?;
    println!("CDC Batch 2: 1 update (bob), 1 insert (dave)");

    // CDC Batch 3: deactivate carol
    let cdc3 = RecordBatch::try_new(
        users_schema(),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["carol"])),
            Arc::new(StringArray::from(vec!["carol@example.com"])),
            Arc::new(StringArray::from(vec!["inactive"])),
        ],
    )?;
    write_delta(&delta_path, vec![cdc3], DeltaWriteMode::Append, false).await?;
    println!("CDC Batch 3: 1 update (carol deactivated)");

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read latest and register
    let df_latest = session.read_delta_async(&delta_path, None).await?;
    let result = df_latest.collect_async().await?;
    session.register_record_batches("users", result.into_batches())?;

    // Current state
    let df = session.sql("SELECT * FROM users ORDER BY user_id")?;
    println!("\n--- Current user state (latest) ---");
    println!("{}", df.collect()?.pretty()?);

    // Time-travel: original state
    let df_v0 = session.read_delta_async(&delta_path, Some(0)).await?;
    println!("\n--- Original state (v0) ---");
    println!("{}", df_v0.collect_async().await?.pretty()?);

    // Time-travel: after first update
    let df_v1 = session.read_delta_async(&delta_path, Some(1)).await?;
    println!("\n--- After first CDC batch (v1) ---");
    println!("{}", df_v1.collect_async().await?.pretty()?);

    // Status distribution
    let df_status = session.sql(
        "SELECT status, COUNT(*) as count FROM users GROUP BY status",
    )?;
    println!("\n--- User Status Distribution ---");
    println!("{}", df_status.collect()?.pretty()?);

    println!("\nCDC ingestion example completed successfully!");
    Ok(())
}
