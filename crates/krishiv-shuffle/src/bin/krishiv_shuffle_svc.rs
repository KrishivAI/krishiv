#![forbid(unsafe_code)]

//! Optional shuffle service — serves local disk partitions over HTTP (WS-6.7).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    krishiv_shuffle::shuffle_svc::run_shuffle_svc_from_env().await
}
