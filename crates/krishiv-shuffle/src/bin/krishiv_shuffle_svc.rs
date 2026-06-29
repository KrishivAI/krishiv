#![forbid(unsafe_code)]
// Fatal startup/exit messages must go to stderr before the async runtime or
// tracing subscriber are initialised, so eprintln!/print_stderr are correct here.
#![allow(clippy::print_stderr)]

//! Optional shuffle service — serves local disk partitions over HTTP (WS-6.7).

use std::process;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // Shuffle service is network I/O-bound: reading Arrow IPC partitions from
    // disk and writing them over HTTP. Capping at 8 workers avoids unnecessary
    // context switches; we never saturate all CPUs here.
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .thread_name("krishiv-shuffle")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            eprintln!("fatal: could not build tokio runtime: {e}");
            process::exit(1);
        });

    let result = rt.block_on(krishiv_shuffle::shuffle_svc::run_shuffle_svc_from_env());
    rt.shutdown_timeout(std::time::Duration::from_secs(5));

    if let Err(error) = result {
        eprintln!("{error}");
        process::exit(2);
    }
}
