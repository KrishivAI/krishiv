#![forbid(unsafe_code)]

//! Standalone `krishiv-coordinator` binary (alias for `krishiv coordinator`).

use std::env;
use std::error::Error;

use krishiv_scheduler::{
    coordinator_daemon_help, parse_coordinator_daemon_config, run_standalone_coordinator,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_coordinator_daemon_config(env::args().skip(1))?;
    if config.help {
        print!("{}", coordinator_daemon_help());
        return Ok(());
    }
    run_standalone_coordinator(config, None).await
}
