//! Example 7: Inventory Management — Delta table with overwrite for stock snapshots.
//!
//! Simulates a warehouse management system that tracks inventory levels.
//! Each snapshot overwrites the previous, but time-travel provides history.
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_inventory_management`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

fn inventory_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sku", DataType::Utf8, false),
        Field::new("product_name", DataType::Utf8, false),
        Field::new("warehouse", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("reorder_point", DataType::Int64, false),
    ]))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // Morning snapshot
    let morning = RecordBatch::try_new(
        inventory_schema(),
        vec![
            Arc::new(StringArray::from(vec!["SKU-001", "SKU-002", "SKU-003", "SKU-004"])),
            Arc::new(StringArray::from(vec![
                "Widget A",
                "Widget B",
                "Gadget X",
                "Gadget Y",
            ])),
            Arc::new(StringArray::from(vec![
                "Warehouse-1",
                "Warehouse-1",
                "Warehouse-2",
                "Warehouse-2",
            ])),
            Arc::new(Int64Array::from(vec![500, 200, 50, 10])),
            Arc::new(Int64Array::from(vec![100, 50, 20, 15])),
        ],
    )?;
    write_delta(
        &delta_path,
        vec![morning],
        DeltaWriteMode::Overwrite,
        false,
    )
    .await?;
    println!("Morning snapshot: 4 SKUs across 2 warehouses");

    // Afternoon: sales depleted stock
    let afternoon = RecordBatch::try_new(
        inventory_schema(),
        vec![
            Arc::new(StringArray::from(vec!["SKU-001", "SKU-002", "SKU-003", "SKU-004"])),
            Arc::new(StringArray::from(vec![
                "Widget A",
                "Widget B",
                "Gadget X",
                "Gadget Y",
            ])),
            Arc::new(StringArray::from(vec![
                "Warehouse-1",
                "Warehouse-1",
                "Warehouse-2",
                "Warehouse-2",
            ])),
            Arc::new(Int64Array::from(vec![320, 45, 15, 8])),
            Arc::new(Int64Array::from(vec![100, 50, 20, 15])),
        ],
    )?;
    write_delta(
        &delta_path,
        vec![afternoon],
        DeltaWriteMode::Overwrite,
        false,
    )
    .await?;
    println!("Afternoon snapshot: stock depleted after sales");

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read latest and register for SQL
    let df_latest = session.read_delta_async(&delta_path, None).await?;
    let result = df_latest.collect_async().await?;
    session.register_record_batches("inventory", result.into_batches())?;

    // Find items below reorder point
    let df = session.sql(
        "SELECT sku, product_name, quantity, reorder_point, (reorder_point - quantity) as deficit FROM inventory WHERE quantity < reorder_point ORDER BY deficit DESC",
    )?;
    println!("\n--- Items Below Reorder Point ---");
    println!("{}", df.collect()?.pretty()?);

    // Warehouse summary
    let df2 = session.sql(
        "SELECT warehouse, COUNT(*) as sku_count, SUM(quantity) as total_units FROM inventory GROUP BY warehouse",
    )?;
    println!("\n--- Warehouse Summary ---");
    println!("{}", df2.collect()?.pretty()?);

    // Time-travel to morning snapshot
    let df_morning = session.read_delta_async(&delta_path, Some(0)).await?;
    println!("\n--- Morning snapshot (v0) ---");
    println!("{}", df_morning.collect_async().await?.pretty()?);

    println!("\nInventory management example completed successfully!");
    Ok(())
}
