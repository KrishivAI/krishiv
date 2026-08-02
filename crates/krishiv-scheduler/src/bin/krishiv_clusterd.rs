#![forbid(unsafe_code)]
// A daemon entry point writes `--help` to stdout and, as its last act, why it
// could not start to stderr — and at that point there is no logger to route
// either through. Same allows as `krishiv_flight_server`,
// `krishiv_shuffle_svc` and `krishiv_job_coordinator`, which do the same.
//
// It compiles only under `--features etcd`, which `just lint` never passed, so
// this was not a rule being broken — it was a file CI never looked at.
#![allow(clippy::print_stderr, clippy::print_stdout)]

//! Cluster control plane daemon (`krishiv-clusterd` / `krishiv clusterd`).

use std::env;
use std::error::Error;
use std::process;

use krishiv_scheduler::{
    coordinator_daemon_help, parse_coordinator_daemon_config, run_clusterd_daemon,
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
        .thread_name("krishiv-clusterd")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .map_err(|e| format!("could not build tokio runtime: {e}"))?;

    let result = rt.block_on(async {
        let config = parse_coordinator_daemon_config(env::args().skip(1))?;
        if config.help {
            print!("{}", coordinator_daemon_help());
            return Ok(());
        }
        run_clusterd_daemon(config, None, vec![]).await
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
