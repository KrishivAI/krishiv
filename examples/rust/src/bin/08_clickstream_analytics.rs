//! Example 8: Clickstream Analytics — complex SQL on Delta data.
//!
//! Simulates a web analytics pipeline that ingests clickstream events
//! into Delta and runs complex analytical queries (sessionization, funnel analysis).
//!
//! Run: `cargo run -p krishiv-rust-examples --bin delta_clickstream_analytics`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

fn clickstream_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("page", DataType::Utf8, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("time_on_page_sec", DataType::Float64, false),
        Field::new("session_ts", DataType::Utf8, false),
    ]))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let delta_path = temp.path().to_string_lossy().to_string();

    // Page view events
    let batch1 = RecordBatch::try_new(
        clickstream_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(Int64Array::from(vec![101, 101, 101, 102, 102, 103, 103, 103])),
            Arc::new(StringArray::from(vec![
                "/home",
                "/products",
                "/cart",
                "/home",
                "/products",
                "/home",
                "/pricing",
                "/signup",
            ])),
            Arc::new(StringArray::from(vec![
                "view", "view", "add_to_cart", "view", "view", "view", "view", "click",
            ])),
            Arc::new(Float64Array::from(vec![
                15.0, 45.0, 5.0, 10.0, 60.0, 8.0, 120.0, 2.0,
            ])),
            Arc::new(StringArray::from(vec![
                "2025-01-15T10:00:00",
                "2025-01-15T10:00:15",
                "2025-01-15T10:01:00",
                "2025-01-15T10:05:00",
                "2025-01-15T10:05:10",
                "2025-01-15T10:10:00",
                "2025-01-15T10:10:30",
                "2025-01-15T10:12:30",
            ])),
        ],
    )?;
    write_delta(&delta_path, vec![batch1], DeltaWriteMode::Overwrite, false).await?;
    println!("Wrote 8 clickstream events");

    // More events
    let batch2 = RecordBatch::try_new(
        clickstream_schema(),
        vec![
            Arc::new(Int64Array::from(vec![9, 10, 11, 12])),
            Arc::new(Int64Array::from(vec![101, 104, 104, 102])),
            Arc::new(StringArray::from(vec![
                "/checkout",
                "/home",
                "/products",
                "/cart",
            ])),
            Arc::new(StringArray::from(vec![
                "purchase", "view", "view", "remove",
            ])),
            Arc::new(Float64Array::from(vec![30.0, 20.0, 35.0, 3.0])),
            Arc::new(StringArray::from(vec![
                "2025-01-15T10:15:00",
                "2025-01-15T10:20:00",
                "2025-01-15T10:20:35",
                "2025-01-15T10:25:00",
            ])),
        ],
    )?;
    write_delta(&delta_path, vec![batch2], DeltaWriteMode::Append, false).await?;
    println!("Appended 4 more events");

    // Build session
    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Read latest and register for SQL
    let df_latest = session.read_delta_async(&delta_path, None).await?;
    let result = df_latest.collect_async().await?;
    session.register_record_batches("clickstream", result.into_batches())?;

    // Page popularity
    let df = session.sql(
        "SELECT page, COUNT(*) as visits, AVG(time_on_page_sec) as avg_time FROM clickstream GROUP BY page ORDER BY visits DESC",
    )?;
    println!("\n--- Page Popularity ---");
    println!("{}", df.collect()?.pretty()?);

    // User engagement
    let df2 = session.sql(
        "SELECT user_id, COUNT(*) as events, SUM(time_on_page_sec) as total_time, COUNT(DISTINCT page) as unique_pages FROM clickstream GROUP BY user_id ORDER BY total_time DESC",
    )?;
    println!("\n--- User Engagement ---");
    println!("{}", df2.collect()?.pretty()?);

    // Conversion funnel
    let df3 = session.sql(
        r#"SELECT
            COUNT(DISTINCT CASE WHEN page = '/home' THEN user_id END) as home_visitors,
            COUNT(DISTINCT CASE WHEN page = '/products' THEN user_id END) as product_viewers,
            COUNT(DISTINCT CASE WHEN page = '/cart' THEN user_id END) as cart_users,
            COUNT(DISTINCT CASE WHEN action = 'purchase' THEN user_id END) as purchasers
        FROM clickstream"#,
    )?;
    println!("\n--- Conversion Funnel ---");
    println!("{}", df3.collect()?.pretty()?);

    println!("\nClickstream analytics example completed successfully!");
    Ok(())
}
