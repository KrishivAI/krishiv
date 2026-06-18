//! Example 6: E-commerce Orders — Delta table with append and SQL queries.
//!
//! Creates an orders Delta table, appends new orders, then runs analytical
//! SQL queries against the Delta data.
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_ecommerce_orders`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("total_amount", DataType::Float64, false),
        Field::new("region", DataType::Utf8, false),
    ]))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // Version 0: initial orders
    let batch_v0 = RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1001, 1002, 1003])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(StringArray::from(vec!["Widget", "Gadget", "Widget Pro"])),
            Arc::new(Int64Array::from(vec![5, 1, 2])),
            Arc::new(Float64Array::from(vec![49.95, 129.99, 199.98])),
            Arc::new(StringArray::from(vec!["US", "EU", "US"])),
        ],
    )?;
    write_delta(&delta_path, vec![batch_v0], DeltaWriteMode::Overwrite, false).await?;
    println!("Version 0: 3 initial orders written");

    // Version 1: new batch of orders
    let batch_v1 = RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1004, 1005, 1006, 1007])),
            Arc::new(Int64Array::from(vec![3, 4, 2, 5])),
            Arc::new(StringArray::from(vec![
                "Laptop",
                "Mouse",
                "Gadget",
                "Keyboard",
            ])),
            Arc::new(Int64Array::from(vec![1, 3, 2, 1])),
            Arc::new(Float64Array::from(vec![999.99, 59.97, 259.98, 79.99])),
            Arc::new(StringArray::from(vec!["US", "EU", "APAC", "US"])),
        ],
    )?;
    write_delta(&delta_path, vec![batch_v1], DeltaWriteMode::Append, false).await?;
    println!("Version 1: 4 orders appended");

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read latest Delta data and register for SQL
    let df_latest = session.read_delta_async(&delta_path, None).await?;
    let result = df_latest.collect_async().await?;
    session.register_record_batches("orders", result.into_batches())?;

    // Analytical query: revenue by region
    let df = session.sql(
        "SELECT region, COUNT(*) as order_count, SUM(total_amount) as revenue FROM orders GROUP BY region ORDER BY revenue DESC",
    )?;
    println!("\n--- Revenue by Region ---");
    println!("{}", df.collect()?.pretty()?);

    // Query: top customers by spend
    let df2 = session.sql(
        "SELECT customer_id, COUNT(*) as orders, SUM(total_amount) as total_spent FROM orders GROUP BY customer_id ORDER BY total_spent DESC",
    )?;
    println!("\n--- Top Customers ---");
    println!("{}", df2.collect()?.pretty()?);

    // Time-travel: only original orders
    let df_v0 = session.read_delta_async(&delta_path, Some(0)).await?;
    println!("\n--- Original orders (v0) ---");
    println!("{}", df_v0.collect_async().await?.pretty()?);

    println!("\nE-commerce orders example completed successfully!");
    Ok(())
}
