//! Example 11: Merge/Upsert — Delta MERGE operations for slowly changing dimensions.
//!
//! Demonstrates Delta Lake merge (upsert) operations: inserting new records
//! and updating existing ones based on a key column.
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_merge_upsert`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, merge_delta, write_delta};
use tempfile::tempdir;

fn catalog_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("stock", DataType::Int64, false),
    ]))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // Initial catalog
    let initial = RecordBatch::try_new(
        catalog_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Widget", "Gadget", "Doohickey"])),
            Arc::new(Float64Array::from(vec![29.99, 49.99, 19.99])),
            Arc::new(Int64Array::from(vec![100, 50, 200])),
        ],
    )?;
    write_delta(&delta_path, vec![initial], DeltaWriteMode::Overwrite, false).await?;
    println!("Initial catalog: 3 products");

    // Incoming batch: update Widget price/stock, insert new Gizmo
    let incoming = RecordBatch::try_new(
        catalog_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 4])),
            Arc::new(StringArray::from(vec!["Widget", "Gizmo"])),
            Arc::new(Float64Array::from(vec![34.99, 99.99])),
            Arc::new(Int64Array::from(vec![85, 30])),
        ],
    )?;

    // Merge: update existing + insert new
    let result = merge_delta(
        &delta_path,
        vec![incoming],
        "product_id",
        true,  // when_matched_update
        true,  // when_not_matched_insert
    )
    .await?;
    println!(
        "Merge result: {} inserted, {} updated",
        result.rows_inserted, result.rows_updated
    );

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read latest and register
    let df_latest = session.read_delta_async(&delta_path, None).await?;
    let result = df_latest.collect_async().await?;
    session.register_record_batches("catalog", result.into_batches())?;

    // Current catalog
    let df = session.sql("SELECT * FROM catalog ORDER BY product_id")?;
    println!("\n--- Current catalog (after merge) ---");
    println!("{}", df.collect()?.pretty()?);

    // Time-travel: original catalog
    let df_v0 = session.read_delta_async(&delta_path, Some(0)).await?;
    println!("\n--- Original catalog (v0) ---");
    println!("{}", df_v0.collect_async().await?.pretty()?);

    // Another merge
    let incoming2 = RecordBatch::try_new(
        catalog_schema(),
        vec![
            Arc::new(Int64Array::from(vec![2, 5])),
            Arc::new(StringArray::from(vec!["Gadget Pro", "Widget Mini"])),
            Arc::new(Float64Array::from(vec![79.99, 14.99])),
            Arc::new(Int64Array::from(vec![25, 150])),
        ],
    )?;
    let result2 = merge_delta(
        &delta_path,
        vec![incoming2],
        "product_id",
        true,
        true,
    )
    .await?;
    println!(
        "\nSecond merge: {} inserted, {} updated",
        result2.rows_inserted, result2.rows_updated
    );

    // Final catalog
    let df_final = session.sql("SELECT * FROM catalog ORDER BY product_id")?;
    println!("\n--- Final catalog (after 2 merges) ---");
    println!("{}", df_final.collect()?.pretty()?);

    println!("\nMerge/upsert example completed successfully!");
    Ok(())
}
