//! Enterprise 08 · Multi-source join: Kafka events + Parquet lookup — embedded mode
//!
//! Demonstrates stream-table join via Krishiv SQL:
//!   1. Load a static product catalog from Parquet (the "table" side)
//!   2. Simulate order events from an InMemoryKafkaSource (the "stream" side)
//!   3. Collect both sides into the Session, run a JOIN query
//!   4. Write enriched output to Parquet
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_08_multi_source_join

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_connectors::kafka::InMemoryKafkaSource;
use krishiv_connectors::Source;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 08: Multi-source join (embedded) ===");

    let dir = tempdir()?;

    // ── Product catalog (Parquet — table side) ─────────────────────────────
    let catalog_path = dir.path().join("catalog.parquet");
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("product_id",  DataType::Int64,   false),
            Field::new("name",        DataType::Utf8,    false),
            Field::new("category",    DataType::Utf8,    false),
            Field::new("unit_price",  DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Laptop Pro","Mouse","Desk Chair","Monitor 4K","USB Hub"])),
            Arc::new(StringArray::from(vec!["electronics","electronics","furniture","electronics","electronics"])),
            Arc::new(Float64Array::from(vec![1299.99, 29.99, 349.99, 499.99, 39.99])),
        ])?;
        let file = std::fs::File::create(&catalog_path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        w.write(&batch)?;
        w.close()?;
    }

    // ── Order events (InMemoryKafkaSource — stream side) ───────────────────
    let event_schema = Arc::new(Schema::new(vec![
        Field::new("order_id",   DataType::Int64, false),
        Field::new("product_id", DataType::Int64, false),
        Field::new("customer",   DataType::Utf8,  false),
        Field::new("qty",        DataType::Int64, false),
    ]));

    let event_batch = RecordBatch::try_new(event_schema, vec![
        Arc::new(Int64Array::from(vec![1001, 1002, 1003, 1004, 1005, 1006])),
        Arc::new(Int64Array::from(vec![1, 2, 3, 1, 5, 4])),
        Arc::new(StringArray::from(vec!["alice","bob","carol","dave","eve","frank"])),
        Arc::new(Int64Array::from(vec![1, 3, 1, 2, 5, 1])),
    ])?;

    let mut source = InMemoryKafkaSource::new("orders", 0, 0, vec![event_batch]);
    let mut event_batches: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = source.read_batch().await.context("read_batch")? {
        println!("  read {} events from InMemoryKafkaSource", batch.num_rows());
        event_batches.push(batch);
    }

    // ── SQL JOIN ───────────────────────────────────────────────────────────
    let session = Session::builder().build()?;
    session.register_parquet("product_catalog", &catalog_path)?;
    session.register_record_batches("order_events", event_batches)?;

    let enriched_df = session.sql(
        "SELECT
             o.order_id,
             o.customer,
             p.name         AS product_name,
             p.category,
             p.unit_price,
             o.qty,
             CAST(o.qty AS DOUBLE) * p.unit_price AS line_total
         FROM order_events o
         JOIN product_catalog p ON o.product_id = p.product_id
         ORDER BY line_total DESC",
    )?;

    let result = enriched_df.collect()?;
    let batches = result.into_batches();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    println!("\n--- Enriched orders ---");
    let view_session = Session::builder().build()?;
    view_session.register_record_batches("enriched", batches.clone())?;
    println!("{}", view_session.sql("SELECT * FROM enriched")?.collect()?.pretty()?);

    // ── Category revenue summary ───────────────────────────────────────────
    let summary_session = Session::builder().build()?;
    summary_session.register_record_batches("enriched", batches.clone())?;
    let summary = summary_session.sql(
        "SELECT category, COUNT(*) AS orders, ROUND(SUM(line_total), 2) AS revenue \
         FROM enriched GROUP BY category ORDER BY revenue DESC"
    )?;
    println!("\n--- Revenue by category ---");
    println!("{}", summary.collect()?.pretty()?);

    // ── Write enriched output to Parquet ──────────────────────────────────
    let out_path = dir.path().join("enriched_orders.parquet");
    if let Some(first) = batches.first() {
        let schema = first.schema();
        let file = std::fs::File::create(&out_path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        for b in &batches { w.write(b)?; }
        w.close()?;
    }

    println!("\n✓ {} enriched rows written to {}", rows, out_path.display());

    Ok(())
}
