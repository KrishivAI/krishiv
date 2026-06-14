//! Streaming word count example using the Krishiv streaming API.
//!
//! Run with:
//!   cargo run -p krishiv --example streaming_word_count
//!
//! This demonstrates the streaming DataFrame builder pattern: read from an
//! in-memory bounded stream, apply transformations, and collect results.

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

    // ── Register a "document" table (simulates a stream source) ──────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("word", DataType::Utf8, false),
    ]));

    let words = vec![
        "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
        "the", "fox", "the", "dog",
    ];
    let doc_ids: Vec<i64> = (0..words.len() as i64).map(|i| i % 3).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(doc_ids)),
            Arc::new(StringArray::from(words.clone())),
        ],
    )?;

    session
        .register_record_batches("word_stream", vec![batch])
        .await?;

    // ── Word count: GROUP BY word, COUNT(*) ───────────────────────────────
    println!("--- Word frequencies (top 5) ---");
    let result = session.sql(
        "SELECT word, COUNT(*) AS freq \
         FROM word_stream \
         GROUP BY word \
         ORDER BY freq DESC, word \
         LIMIT 5",
    )?;
    print_result(result);

    // ── Per-document word count ───────────────────────────────────────────
    println!("\n--- Words per document ---");
    let result = session.sql(
        "SELECT doc_id, COUNT(*) AS word_count \
         FROM word_stream \
         GROUP BY doc_id \
         ORDER BY doc_id",
    )?;
    print_result(result);

    // ── Unique words ──────────────────────────────────────────────────────
    println!("\n--- Unique word count ---");
    let result = session.sql("SELECT COUNT(DISTINCT word) AS unique_words FROM word_stream")?;
    print_result(result);

    Ok(())
}

fn print_result(result: QueryResult) {
    match result {
        QueryResult::Batch(batches) => {
            for batch in &batches {
                let schema = batch.schema();
                let headers: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                println!("{}", headers.join("\t| "));
                println!("{}", "─".repeat(30));
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
                    println!("{}", vals.join("\t| "));
                }
            }
        }
        other => println!("{other:?}"),
    }
}
