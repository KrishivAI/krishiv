#![forbid(unsafe_code)]

//! Standalone Arrow Flight SQL server for local and distributed Krishiv clusters.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    krishiv_flight_sql::run_flight_server_from_env().await
}
