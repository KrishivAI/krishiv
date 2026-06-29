//! Example 9: Multi-Table Join — Delta tables with SQL JOIN queries.
//!
//! Creates two Delta tables (customers and orders) and runs analytical
//! JOIN queries across them.
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_multi_table_join`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

fn customers_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("tier", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
    ]))
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let customers_path = temp.path().join("customers").to_string_lossy().to_string();
    let orders_path = temp.path().join("orders").to_string_lossy().to_string();

    // Write customers
    let customers = RecordBatch::try_new(
        customers_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol", "Dave", "Eve",
            ])),
            Arc::new(StringArray::from(vec![
                "Gold", "Silver", "Gold", "Bronze", "Silver",
            ])),
            Arc::new(StringArray::from(vec![
                "New York", "London", "Tokyo", "Paris", "Sydney",
            ])),
        ],
    )?;
    write_delta(&customers_path, vec![customers], DeltaWriteMode::Overwrite, false).await?;
    println!("Wrote 5 customers");

    // Write orders
    let orders = RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104, 105, 106, 107])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam", "USB Hub",
            ])),
            Arc::new(Float64Array::from(vec![
                999.99, 29.99, 79.99, 349.99, 149.99, 89.99, 45.00,
            ])),
        ],
    )?;
    write_delta(&orders_path, vec![orders], DeltaWriteMode::Overwrite, false).await?;
    println!("Wrote 7 orders");

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read both Delta tables and register
    let cust_df = session.read_delta_async(&customers_path, None).await?;
    let cust_result = cust_df.collect_async().await?;
    session.register_record_batches("customers", cust_result.into_batches())?;

    let ord_df = session.read_delta_async(&orders_path, None).await?;
    let ord_result = ord_df.collect_async().await?;
    session.register_record_batches("orders", ord_result.into_batches())?;

    // JOIN: customer spending
    let df = session.sql(
        r#"
        SELECT c.name, c.tier, c.city,
               COUNT(o.order_id) as order_count,
               SUM(o.amount) as total_spent
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.name, c.tier, c.city
        ORDER BY total_spent DESC
        "#,
    )?;
    println!("\n--- Customer Spending Summary ---");
    println!("{}", df.collect()?.pretty()?);

    // LEFT JOIN: all customers including those with no orders
    let df2 = session.sql(
        r#"
        SELECT c.name, c.tier,
               COALESCE(SUM(o.amount), 0) as total_spent,
               COUNT(o.order_id) as order_count
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.name, c.tier
        ORDER BY total_spent DESC
        "#,
    )?;
    println!("\n--- All Customers (including no orders) ---");
    println!("{}", df2.collect()?.pretty()?);

    // Tier analysis
    let df3 = session.sql(
        r#"
        SELECT c.tier,
               COUNT(DISTINCT c.customer_id) as customers,
               SUM(o.amount) as total_revenue,
               AVG(o.amount) as avg_order_value
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.tier
        ORDER BY total_revenue DESC
        "#,
    )?;
    println!("\n--- Tier Analysis ---");
    println!("{}", df3.collect()?.pretty()?);

    println!("\nMulti-table join example completed successfully!");
    Ok(())
}
