//! Basic SQL example: register an in-memory table and run SQL queries against it.
//!
//! Run with:
//!   cargo run -p krishiv --example basic_sql
//!
//! This demonstrates the minimal embedded-mode usage pattern.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_api::session::{Session, SessionBuilder};
use krishiv_api::types::{ExecutionMode, QueryResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build an embedded session ─────────────────────────────────────────
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // ── Create an in-memory table ─────────────────────────────────────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "Dave", "Eve"])),
            Arc::new(Int64Array::from(vec![85, 92, 78, 95, 88])),
        ],
    )?;

    session
        .register_record_batches("scores", vec![batch])
        .await?;

    // ── Run a simple SELECT ───────────────────────────────────────────────
    println!("--- Top scorers ---");
    let result = session.sql("SELECT name, score FROM scores ORDER BY score DESC LIMIT 3")?;
    print_result(result);

    // ── Run an aggregate ─────────────────────────────────────────────────
    println!("\n--- Statistics ---");
    let result = session.sql(
        "SELECT COUNT(*) AS n, AVG(score) AS avg_score, MAX(score) AS top_score FROM scores",
    )?;
    print_result(result);

    // ── Run a filtered query ──────────────────────────────────────────────
    println!("\n--- Scores above 90 ---");
    let result = session.sql("SELECT name FROM scores WHERE score > 90 ORDER BY name")?;
    print_result(result);

    Ok(())
}

fn print_result(result: QueryResult) {
    match result {
        QueryResult::Batch(batches) => {
            for batch in &batches {
                let schema = batch.schema();
                // Print header
                let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
                println!("{}", headers.join(" | "));
                println!("{}", "─".repeat(headers.join(" | ").len()));
                // Print rows
                for row in 0..batch.num_rows() {
                    let vals: Vec<String> = (0..batch.num_columns())
                        .map(|col| {
                            let arr = batch.column(col);
                            if arr.is_null(row) {
                                "NULL".to_string()
                            } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                                a.value(row).to_string()
                            } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                                a.value(row).to_string()
                            } else {
                                "?".to_string()
                            }
                        })
                        .collect();
                    println!("{}", vals.join(" | "));
                }
            }
        }
        other => println!("{other:?}"),
    }
}
