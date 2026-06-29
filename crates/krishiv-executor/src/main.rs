#![forbid(unsafe_code)]
// CLI entry point intentionally writes errors to stderr.
#![allow(clippy::print_stderr)]

use std::env;
use std::process;

// Use jemalloc for lower allocator contention and reduced RSS on
// multi-threaded executor workloads (Arrow buffer churn + RocksDB).
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // Build a tuned multi-threaded runtime rather than using the #[tokio::main]
    // default. Key choices:
    //
    // - worker_threads: one per logical CPU — executor is CPU-bound for batch
    //   (DataFusion physical plan execution) and I/O-bound for streaming
    //   (waiting on sources/sinks). Using all cores gives the best utilisation
    //   for both workload shapes.
    //
    // - thread_stack_size 4 MiB: DataFusion's recursive plan visitor and the
    //   SQL query planner can overflow the default 2 MiB stack on deeply nested
    //   queries. 4 MiB gives headroom without a significant RSS cost (stack
    //   pages are committed lazily by the OS).
    //
    // - max_blocking_threads 512: RocksDB state reads and Parquet writes go
    //   through spawn_blocking. The default cap (512 on Linux) is fine, but
    //   naming it explicitly makes the setting visible to operators.
    //
    // - thread_name: shows up in profilers (perf, py-spy, flamegraph) and
    //   in /proc/{pid}/task/{tid}/comm for process-level observability.
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(parallelism)
        .thread_name("krishiv-exec")
        .thread_stack_size(4 * 1024 * 1024)
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            eprintln!("fatal: could not build tokio runtime: {e}");
            process::exit(1);
        });

    let exit = rt.block_on(async {
        match krishiv_executor::cli::run_executor_cli(env::args().skip(1)).await {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("{error}");
                2
            }
        }
    });

    // Shut down the runtime before exiting so background tasks (metrics flush,
    // graceful gRPC drain) have a chance to complete.
    rt.shutdown_timeout(std::time::Duration::from_secs(5));
    process::exit(exit);
}
