#![forbid(unsafe_code)]

//! Per-job coordinator process (JCP) for bare-metal / shared-metadata deployments.
//!
//! Shares a durable metadata store with the cluster control plane and runs
//! orchestration loops scoped to a single [`krishiv_proto::JobId`].
//!
//! Invoked as `krishiv-job-coordinator` or `krishiv job-coordinator`.

use std::env;
use std::error::Error;

use krishiv_scheduler::{
    job_coordinator_daemon_help, parse_job_coordinator_daemon_config, run_job_coordinator_daemon,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_job_coordinator_daemon_config(env::args().skip(1))?;
    if config.help {
        print!("{}", job_coordinator_daemon_help());
        return Ok(());
    }
    run_job_coordinator_daemon(config).await
}
