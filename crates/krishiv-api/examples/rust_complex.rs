use krishiv_api::session::SessionBuilder;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Starting Complex Rust Batch Job (with Shuffle) ---");
    let start = Instant::now();

    let session = SessionBuilder::from_env()?.build()?;

    let df = session.sql(
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
    )?;

    let result = df.collect()?;
    let duration = start.elapsed();

    println!("{}", result.pretty()?);
    println!("Rust Execution Time: {:?}", duration);

    Ok(())
}
