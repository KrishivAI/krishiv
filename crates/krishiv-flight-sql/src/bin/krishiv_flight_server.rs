//! Standalone Arrow Flight SQL server for local and distributed Krishiv clusters.

use std::net::SocketAddr;

use krishiv_flight_sql::make_flight_sql_server;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("KRISHIV_FLIGHT_ADDR")
        .unwrap_or_else(|_| String::from("127.0.0.1:50051"))
        .parse()?;

    eprintln!("krishiv-flight-server listening on http://{addr}");
    Server::builder()
        .add_service(make_flight_sql_server())
        .serve(addr)
        .await?;
    Ok(())
}
