#![deny(unsafe_code)]

// Use jemalloc instead of the system allocator when the feature is enabled.
// jemalloc reduces allocator contention 2-4x on multi-threaded workloads and
// cuts peak RSS 10-20% for data-engine use patterns (many short-lived Arrow
// buffers + long-lived RocksDB block caches). The `unprefixed_malloc_on_*`
// feature also replaces malloc/free globally so native deps benefit too.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cli;
mod cluster_cmd;
mod daemon_cmd;
mod doctor_cmd;
mod ivm_cmd;
mod local_cluster;
mod pipeline_cmd;
mod process_util;
mod query_cli;
mod remote_client;
mod stream_cmd;
mod table_cmd;

use std::env;
use std::path::Path;
use std::process;

fn main() {
    // Load .env file — optional, silently ignored if absent.
    if let Err(e) = dotenvy::dotenv()
        && !e.not_found()
    {
        eprintln!("warn: failed to load .env: {e}");
    }

    // Initialise telemetry — opt-in via OTEL_EXPORTER_OTLP_ENDPOINT.
    let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
    let _metrics = krishiv_metrics::init(krishiv_metrics::MetricsConfig {
        service_name: "krishiv".into(),
        otlp_endpoint,
        ..Default::default()
    })
    .unwrap_or_else(|e| {
        eprintln!("warn: metrics init failed: {e}");
        krishiv_metrics::MetricsHandle::noop()
    });

    // Multi-call binary dispatch (BusyBox pattern): when invoked via a
    // symlink (krishiv-coordinator, krishiv-executor, …), translate argv[0]
    // into the equivalent subcommand. This lets a single `krishiv` binary
    // serve all daemon entrypoints, eliminating 6 redundant binaries that
    // would each statically link the full DataFusion/Arrow/tokio/tonic stack.
    let mut args: Vec<String> = env::args().skip(1).collect();
    if let Some(sub) = multipass_subcommand() {
        args.insert(0, sub.to_string());
    }

    if let Some(code) = daemon_cmd::try_run_daemon(&args) {
        process::exit(code);
    }
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let response = cli::dispatch(&arg_refs);

    if !response.stdout.is_empty() {
        print!("{}", response.stdout);
    }
    if !response.stderr.is_empty() {
        eprint!("{}", response.stderr);
    }
    process::exit(response.exit_code);
}

/// Detect symlink invocation and return the equivalent `krishiv` subcommand.
///
/// Enables the multi-call binary pattern: deploy-time symlinks like
/// `krishiv-coordinator → krishiv` cause the binary to dispatch as
/// `krishiv coordinator` with zero runtime overhead.
fn multipass_subcommand() -> Option<&'static str> {
    let prog = env::args().next()?;
    let name = Path::new(&prog).file_name()?.to_str()?;
    match name {
        "krishiv-coordinator" => Some("coordinator"),
        "krishiv-clusterd" => Some("clusterd"),
        "krishiv-executor" => Some("executor"),
        "krishiv-job-coordinator" => Some("job-coordinator"),
        "krishiv-flight-server" => Some("flight-server"),
        "krishiv-shuffle-svc" => Some("shuffle-svc"),
        _ => None,
    }
}
