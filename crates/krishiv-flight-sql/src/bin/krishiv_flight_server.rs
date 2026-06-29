#![forbid(unsafe_code)]
//! Standalone Arrow Flight SQL server for local and distributed Krishiv clusters.
//!
//! The `eprintln!` calls below are in the fatal startup path that runs *before*
//! `tracing` is initialized, so they cannot be replaced with `tracing::error!`.
#![allow(clippy::print_stderr)]

use std::process;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // Flight SQL server is I/O-bound: Arrow IPC serialisation + gRPC network.
    // Cap at 8 workers; CPU saturation is never the bottleneck here.
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .thread_name("krishiv-flight")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            eprintln!("fatal: could not build tokio runtime: {e}");
            process::exit(1);
        });

    let result = rt.block_on(krishiv_flight_sql::run_flight_server_from_env());
    rt.shutdown_timeout(std::time::Duration::from_secs(5));

    if let Err(error) = result {
        eprintln!("{error}");
        process::exit(2);
    }
}
