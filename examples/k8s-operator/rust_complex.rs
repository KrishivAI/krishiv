use krishiv_api::session::SessionBuilder;
use krishiv_common::durability::DurabilityProfile;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Complex Rust Batch Job (with Shuffle) ---");
    let start = Instant::now();

    // In k8s operator mode, this connects to the Job Coordinator Pod (JCP)
    // We explicitly set Tiered shuffle durability to leverage our new feature!
    let session = SessionBuilder::new()
        .durability(DurabilityProfile::Tiered)
        .connect_from_env()
        .await?;

    // Execute a complex shuffle: Distributed Hash Aggregate
    let df = session.sql(r#"
        SELECT 
            value % 50 as cohort, 
            COUNT(value) as cohort_size,
            AVG(value) as cohort_avg
        FROM generate_series(1, 5000000)
        GROUP BY value % 50
        ORDER BY cohort_size DESC
        LIMIT 5
    "#).await?;

    // Trigger execution
    let result = df.collect().await?;
    let duration = start.elapsed();

    krishiv_api::util::print_batches(&result)?;
    println!("Rust Execution Time: {:?}", duration);

    println!("\n--- Starting Complex Streaming Job ---");
    let stream_start = Instant::now();

    // Using the same Session, register a mock parquet file or direct SQL stream
    // Since we are in an example, we can use a direct SQL stream from an external source or just demonstrate syntax
    session.register_parquet_stream("k8s_stream_data", std::path::Path::new("/tmp/dummy.parquet"));
    let stream_q = r#"
        SELECT cohort, COUNT(*) as events
        FROM (SELECT value % 50 as cohort FROM k8s_stream_data)
        GROUP BY cohort
    "#;

    println!("Submitting streaming query...");
    let mut stream = session.execute_local_async(stream_q).await?.execute_stream_async().await?;

    use futures::StreamExt;
    // We only wait for the first micro-batch in this example
    if let Some(batch) = stream.next().await {
        println!("Received streaming micro-batch! Rows: {}", batch?.num_rows());
    }

    println!("Rust Streaming Setup Time: {:?}", stream_start.elapsed());

    Ok(())
}
