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
    let df = session
        .sql(
            r#"
        SELECT 
            mod(id, 50) as cohort, 
            COUNT(id) as cohort_size,
            AVG(id) as cohort_avg
        FROM generate_series(1, 5000000)
        GROUP BY mod(id, 50)
        ORDER BY cohort_size DESC
        LIMIT 5
    "#,
        )
        .await?;

    // Trigger execution
    let result = df.collect().await?;
    let duration = start.elapsed();

    krishiv_api::util::print_batches(&result)?;
    println!("Rust Execution Time: {:?}", duration);

    Ok(())
}
