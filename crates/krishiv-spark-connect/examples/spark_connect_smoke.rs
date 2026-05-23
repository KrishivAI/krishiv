//! Standalone Spark Connect smoke server for Python integration tests.

use krishiv_spark_connect::{serve_spark_connect, SparkConnectServiceImpl};
use krishiv_sql::SqlEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("KRISHIV_SPARK_CONNECT_ADDR").unwrap_or_else(|_| "127.0.0.1:17070".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("Spark Connect listening on {addr}");
    let svc = SparkConnectServiceImpl::new(SqlEngine::new());
    serve_spark_connect(listener, svc).await?;
    Ok(())
}
