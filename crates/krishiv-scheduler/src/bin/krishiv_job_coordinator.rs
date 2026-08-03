#![forbid(unsafe_code)]
// A daemon entry point writes `--help` to stdout and, as its last act, why it
// could not start to stderr — and at that point there is no logger to route
// either through. `krishiv_clusterd` already says exactly this; these two
// still used `tracing::error!`, so a config-parse failure (which happens
// before any logging is initialised) exited 2 with no output at all.
#![allow(clippy::print_stderr, clippy::print_stdout)]

//! Per-job coordinator process (JCP) for bare-metal / shared-metadata deployments.
//!
//! Shares a durable metadata store with the cluster control plane and runs
//! orchestration loops scoped to a single [`krishiv_proto::JobId`].
//!
//! Invoked as `krishiv-job-coordinator` or `krishiv job-coordinator`.

use std::env;
use std::error::Error;
use std::process;

use krishiv_scheduler::{
    job_coordinator_daemon_help, parse_job_coordinator_daemon_config, run_job_coordinator_daemon,
};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .thread_name("krishiv-jcp")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .map_err(|e| format!("could not build tokio runtime: {e}"))?;

    let result = rt.block_on(async {
        let config = parse_job_coordinator_daemon_config(env::args().skip(1))?;
        if config.help {
            print!("{}", job_coordinator_daemon_help());
            return Ok(());
        }
        run_job_coordinator_daemon(config).await
    });

    rt.shutdown_timeout(std::time::Duration::from_secs(5));

    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!("{error}");
            process::exit(2);
        }
    }
}
