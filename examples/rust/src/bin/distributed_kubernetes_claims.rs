#![forbid(unsafe_code)]

use std::error::Error;
use std::io;
use std::path::PathBuf;

use krishiv::Session;

fn main() -> Result<(), Box<dyn Error>> {
    let flight_url = std::env::var("KRISHIV_K8S_FLIGHT_URL")
        .unwrap_or_else(|_| String::from("http://127.0.0.1:50051"));
    let claims_path = std::env::var("CLAIMS_PARQUET")
        .map(PathBuf::from)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "set CLAIMS_PARQUET to a Parquet path visible to Krishiv Kubernetes pods",
            )
        })?;

    let session = Session::builder()
        .with_coordinator(flight_url)
        .with_remote_execution(true)
        .build()?;
    session.register_parquet("claims", &claims_path)?;

    let result = session
        .execute_remote(
            "select payer, diagnosis_group, count(*) as claims, sum(allowed_amount_cents) as allowed_cents \
             from claims \
             where service_year = 2026 \
             group by payer, diagnosis_group \
             order by allowed_cents desc",
        )?
        .collect()?;

    println!("Distributed Kubernetes batch: healthcare claims spend by payer and diagnosis group");
    println!("{}", result.pretty()?);
    Ok(())
}
