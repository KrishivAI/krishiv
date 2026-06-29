#![forbid(unsafe_code)]
// CLI binary intentionally writes help text to stdout.
#![allow(clippy::print_stdout)]

//! Standalone `krishiv-coordinator` binary (alias for `krishiv coordinator`).

use std::env;
use std::error::Error;
use std::process;

use krishiv_scheduler::{
    coordinator_daemon_help, parse_coordinator_daemon_config, run_standalone_coordinator,
};

// Use jemalloc for lower allocator contention on the coordinator's
// multi-threaded gRPC dispatch loop and heartbeat bookkeeping.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), Box<dyn Error>> {
    // Coordinator is I/O-bound: many small gRPC messages, heartbeat timers, and
    // etcd watch loops. Cap worker threads at 8 to avoid unnecessary context
    // switches on beefy machines; the coordinator is never CPU-saturated.
    //
    // thread_stack_size 2 MiB: coordinator has no recursive plan visitors, so
    // the default 2 MiB is plenty. Named explicitly for observability.
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .thread_name("krishiv-coord")
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
        run_standalone_coordinator(config, None, vec![]).await
    });

    rt.shutdown_timeout(std::time::Duration::from_secs(5));

    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::error!(error = %error, "coordinator exited with error");
            process::exit(2);
        }
    }
}
