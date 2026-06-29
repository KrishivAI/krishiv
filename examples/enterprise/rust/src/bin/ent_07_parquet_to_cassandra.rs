//! Enterprise 07 · Parquet → Cassandra (batch CQL writes) — embedded mode
//!
//! Loads an orders dataset, filters via SQL, then shows the CQL INSERT
//! statements that `CassandraSink` would execute. The actual Cassandra
//! connection is skipped in embedded mode.
//!
//! In production, replace the print loop with:
//!   ```rust
//!   let cfg = CassandraConfig::new("localhost", "krishiv", "orders");
//!   let sink = CassandraSink::connect(cfg).await?;
//!   sink.write_batch(&batch).await?;
//!   ```
//! (requires `features = ["cassandra"]` in Cargo.toml and a live cluster)
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_07_parquet_to_cassandra

use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 07: Parquet → Cassandra (embedded demo) ===");

    let dir = tempdir()?;
    let orders_path = dir.path().join("orders.parquet");

    // Write synthetic orders to Parquet.
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id",  DataType::Int64,   false),
            Field::new("customer",  DataType::Utf8,    false),
            Field::new("product",   DataType::Utf8,    false),
            Field::new("amount",    DataType::Float64, false),
            Field::new("status",    DataType::Utf8,    false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104, 105, 106, 107, 108])),
            Arc::new(StringArray::from(vec!["alice","bob","carol","dave","eve","frank","grace","hank"])),
            Arc::new(StringArray::from(vec!["Laptop","Mouse","Chair","Monitor","USB Hub","Keyboard","Webcam","Desk"])),
            Arc::new(Float64Array::from(vec![1299.99, 29.99, 349.99, 499.99, 39.99, 129.99, 89.99, 699.99])),
            Arc::new(StringArray::from(vec!["shipped","pending","delivered","shipped","cancelled","pending","delivered","pending"])),
        ])?;
        let file = std::fs::File::create(&orders_path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        w.write(&batch)?;
        w.close()?;
    }

    // SQL: filter to active orders.
    let session = Session::builder().build()?;
    session.register_parquet("orders", &orders_path)?;

    let df = session.sql(
        "SELECT order_id, customer, product, amount, status \
         FROM orders WHERE status IN ('shipped', 'delivered') \
         ORDER BY amount DESC",
    )?;

    let result = df.collect()?;
    let batches = result.into_batches();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("  {} active orders (shipped + delivered)", total);

    // Generate CQL INSERT statements (what CassandraSink would execute).
    let ks = "krishiv";
    let tbl = "orders";
    println!("\n--- CQL INSERTs (unlogged batch) ---");
    println!("BEGIN UNLOGGED BATCH");

    for batch in &batches {
        // Cast Utf8View → Utf8 to handle DataFusion's default string output type.
        let cust_arr   = arrow::compute::cast(batch.column_by_name("customer").unwrap(), &DataType::Utf8).unwrap();
        let prod_arr   = arrow::compute::cast(batch.column_by_name("product").unwrap(), &DataType::Utf8).unwrap();
        let status_arr = arrow::compute::cast(batch.column_by_name("status").unwrap(), &DataType::Utf8).unwrap();

        let id_col  = batch.column_by_name("order_id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let cust    = cust_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let prod    = prod_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let amt     = batch.column_by_name("amount").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        let status  = status_arr.as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..batch.num_rows() {
            println!(
                "  INSERT INTO {}.{} (order_id, customer, product, amount, status) \
                 VALUES ({}, '{}', '{}', {:.2}, '{}');",
                ks, tbl,
                id_col.value(i),
                cust.value(i),
                prod.value(i),
                amt.value(i),
                status.value(i),
            );
        }
    }
    println!("APPLY BATCH;");

    // Summary.
    let session2 = Session::builder().build()?;
    session2.register_record_batches("active", batches)?;
    let summary = session2.sql(
        "SELECT status, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total \
         FROM active GROUP BY status ORDER BY total DESC"
    )?;
    println!("\n--- Active order summary ---");
    println!("{}", summary.collect()?.pretty()?);

    println!("\n✓ {} rows ready for Cassandra {}.{}", total, ks, tbl);
    println!("  (connect a CassandraSink for live writes)");

    Ok(())
}
