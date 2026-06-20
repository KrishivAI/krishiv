//! Enterprise 06 · Parquet → Elasticsearch (bulk index) — embedded mode
//!
//! Loads a product catalog from Parquet, runs SQL enrichment to add
//! `inventory_value` and `price_tier` columns, then serialises each row to the
//! Elasticsearch `_bulk` JSON format using `serde_json`.
//!
//! In production, pass the serialised docs to `ElasticsearchSink::write_batch`
//! (requires the `elasticsearch` feature in Cargo.toml and a live cluster).
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_06_parquet_to_elasticsearch

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
    println!("=== Enterprise 06: Parquet → Elasticsearch (embedded demo) ===");

    let dir = tempdir()?;
    let catalog_path = dir.path().join("products.parquet");

    // Write the product catalog to Parquet.
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("product_id",  DataType::Int64,   false),
            Field::new("name",        DataType::Utf8,    false),
            Field::new("category",    DataType::Utf8,    false),
            Field::new("unit_price",  DataType::Float64, false),
            Field::new("stock",       DataType::Int64,   false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                "Laptop Pro 15", "Wireless Mouse", "Desk Chair Ergo",
                "Monitor 27 4K", "USB-C Hub 7-in-1", "Mechanical Keyboard",
                "Webcam 4K",     "Standing Desk",
            ])),
            Arc::new(StringArray::from(vec![
                "electronics", "electronics", "furniture",
                "electronics", "electronics", "electronics",
                "electronics", "furniture",
            ])),
            Arc::new(Float64Array::from(vec![
                1299.99, 29.99, 349.99, 499.99, 39.99, 129.99, 89.99, 699.99,
            ])),
            Arc::new(Int64Array::from(vec![42, 150, 30, 68, 200, 85, 120, 15])),
        ])?;
        let file = std::fs::File::create(&catalog_path)?;
        let mut w = ArrowWriter::try_new(file, schema, None)?;
        w.write(&batch)?;
        w.close()?;
    }

    // SQL enrichment: add inventory_value + price_tier.
    let session = Session::builder().build()?;
    session.register_parquet("products", &catalog_path)?;

    let enriched = session.sql(
        "SELECT product_id, name, category, unit_price, stock,
                CAST(unit_price * stock AS DOUBLE) AS inventory_value,
                CASE
                    WHEN unit_price > 500  THEN 'premium'
                    WHEN unit_price > 100  THEN 'mid-range'
                    ELSE 'budget'
                END AS price_tier
         FROM products
         ORDER BY inventory_value DESC",
    )?;

    let result = enriched.collect()?;
    let batches = result.into_batches();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("  enriched {} products", total);

    // Serialize to Elasticsearch _bulk format (action + source lines).
    let index = "krishiv-products";
    let mut bulk_lines: Vec<String> = Vec::new();

    for batch in &batches {
        // Cast string columns to Utf8 to handle DataFusion's Utf8View output.
        let name_arr  = arrow::compute::cast(batch.column_by_name("name").unwrap(), &DataType::Utf8).unwrap();
        let cat_arr   = arrow::compute::cast(batch.column_by_name("category").unwrap(), &DataType::Utf8).unwrap();
        let tier_arr  = arrow::compute::cast(batch.column_by_name("price_tier").unwrap(), &DataType::Utf8).unwrap();

        let id_col    = batch.column_by_name("product_id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col  = name_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let cat_col   = cat_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let price_col = batch.column_by_name("unit_price").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        let stock_col = batch.column_by_name("stock").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let inv_col   = batch.column_by_name("inventory_value").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        let tier_col  = tier_arr.as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..batch.num_rows() {
            let action = serde_json::json!({
                "index": { "_index": index, "_id": id_col.value(i) }
            });
            let doc = serde_json::json!({
                "product_id":      id_col.value(i),
                "name":            name_col.value(i),
                "category":        cat_col.value(i),
                "unit_price":      price_col.value(i),
                "stock":           stock_col.value(i),
                "inventory_value": inv_col.value(i),
                "price_tier":      tier_col.value(i),
            });
            bulk_lines.push(action.to_string());
            bulk_lines.push(doc.to_string());
        }
    }

    println!("\n--- _bulk payload ({} action+doc pairs) ---", bulk_lines.len() / 2);
    for pair in bulk_lines.chunks(2).take(3) {
        println!("  {}", pair[0]);
        println!("  {}", pair[1]);
    }
    if bulk_lines.len() / 2 > 3 {
        println!("  … ({} more pairs)", bulk_lines.len() / 2 - 3);
    }

    // Category summary.
    let session2 = Session::builder().build()?;
    session2.register_record_batches("enriched", batches)?;
    let summary = session2.sql(
        "SELECT price_tier, COUNT(*) AS products, \
                ROUND(SUM(inventory_value), 2) AS inventory_value \
         FROM enriched GROUP BY price_tier ORDER BY inventory_value DESC"
    )?;
    println!("\n--- Inventory by price tier ---");
    println!("{}", summary.collect()?.pretty()?);

    println!("\n✓ {} documents ready for Elasticsearch index '{}'", total, index);
    println!("  (connect an ElasticsearchSink to push to a live cluster)");

    Ok(())
}
