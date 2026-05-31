//! E-commerce SQL Join and aggregate batch execution on local Parquet files.
//! Run with: `cargo run -p krishiv --example batch_ecommerce`

#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let orders_path = temp.path().join("orders.parquet");
    let customers_path = temp.path().join("customers.parquet");

    write_orders_parquet(&orders_path)?;
    write_customers_parquet(&customers_path)?;

    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    session.register_parquet("orders", &orders_path)?;
    session.register_parquet("customers", &customers_path)?;

    // Join customers and orders to calculate revenue by segment
    let df = session.sql(
        "SELECT c.segment, \
                COUNT(o.order_id) as total_orders, \
                SUM(o.amount) as total_revenue \
         FROM orders o \
         JOIN customers c ON o.customer_id = c.customer_id \
         WHERE o.status = 'COMPLETED' \
         GROUP BY c.segment \
         ORDER BY total_revenue DESC",
    )?;

    let result = df.collect()?;
    println!("{}", result.pretty()?);

    Ok(())
}

fn write_orders_parquet(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 3])),
            Arc::new(Float64Array::from(vec![150.0, 45.5, 99.9, 1200.0])),
            Arc::new(StringArray::from(vec![
                "COMPLETED",
                "COMPLETED",
                "COMPLETED",
                "PENDING",
            ])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_customers_parquet(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("segment", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["VIP", "Standard", "VIP"])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
