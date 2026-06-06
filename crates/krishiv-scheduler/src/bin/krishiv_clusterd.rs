#![forbid(unsafe_code)]

//! Cluster control plane daemon (`krishiv-clusterd` / `krishiv clusterd`).

use std::env;
use std::error::Error;

use krishiv_scheduler::{
    coordinator_daemon_help, parse_coordinator_daemon_config, run_clusterd_daemon,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_coordinator_daemon_config(env::args().skip(1))?;
    if config.help {
        print!("{}", coordinator_daemon_help());
        return Ok(());
    }
    run_clusterd_daemon(config, None).await
}
