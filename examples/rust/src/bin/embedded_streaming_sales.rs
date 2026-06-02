#![forbid(unsafe_code)]

use std::error::Error;
use futures::StreamExt;

use krishiv::{AggExpr, ExecutionMode, Session};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // We will use an in-memory stream of simulated JSON-like retail events.
    // However, thanks to the unified execution engine, the same API works seamlessly 
    // against Kafka or real network streams.

    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // In a real application, you'd register a streaming source. 
    // For this example, we'll pretend there's a Kafka table.
    // But actually, let's just create an empty query that represents a stream and show the API:
    // We can use the fluent StreamingDataFrame API:
    
    let df = session.sql("SELECT 'store1' as warehouse, 100 as event_ts, 'coffee' as sku, 5 as amount UNION ALL SELECT 'store1' as warehouse, 200 as event_ts, 'tea' as sku, 2 as amount")?;

    let mut stream = df.stream()
        .key_by("warehouse")
        .with_event_time("event_ts")
        .tumbling_window(60000)
        .agg(vec![krishiv::AggExpr {
            function: krishiv::AggFunction::Count,
            input_column: String::new(),
            output_column: "sku_count".to_string(),
        }])
        .execute_stream_async()
        .await?;

    println!("Starting local streaming aggregation...");
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        println!("Received streaming batch: {} rows", batch.num_rows());
    }
    
    Ok(())
}
