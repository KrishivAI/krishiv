#![deny(unsafe_code)]

// Use jemalloc instead of the system allocator when the feature is enabled.
// jemalloc reduces allocator contention 2-4x on multi-threaded workloads and
// cuts peak RSS 10-20% for data-engine use patterns (many short-lived Arrow
// buffers + long-lived RocksDB block caches). The `unprefixed_malloc_on_*`
// feature also replaces malloc/free globally so native deps benefit too.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod capabilities;
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

    // NOT declared single-query here, though this is exactly the process that
    // is one. See `declare_single_query_process`: giving an embedded query the
    // whole pool to size joins against was measured *faster and wrong* — TPC-H
    // q5 at SF100 went 247.1 s -> 122.2 s, and q8 and q18 stopped completing at
    // all. The declaration stays available for a caller that has bounded its
    // own join sizes; the CLI has not.

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

#[cfg(test)]
mod single_query_declaration_tests {
    /// Every process that shares one memory pool between concurrent queries
    /// must exit through `try_run_daemon`, because everything past it is
    /// declared single-query and plans joins against the *whole* pool.
    ///
    /// `mcp` is the trap: it is the one daemon that builds a `Session`, so it
    /// looks like an embedded engine while actually multiplexing queries. If it
    /// ever left this list, two concurrent MCP queries could each reserve half
    /// the pool as an unspillable hash-join build side and the container would
    /// be OOM-killed.
    #[test]
    fn every_pool_sharing_process_exits_through_the_daemon_dispatch() {
        for sub in [
            "mcp",
            "executor",
            "coordinator",
            "clusterd",
            "job-coordinator",
            "flight-server",
            "shuffle-svc",
        ] {
            assert!(
                crate::daemon_cmd::is_daemon_subcommand(sub),
                "`{sub}` would reach the single-query declaration in main()"
            );
        }
    }

    /// The symlink entrypoints translate to daemon subcommands, so they must
    /// land in the same list — `krishiv-executor` is the deployed name.
    #[test]
    fn symlink_entrypoints_are_daemons_too() {
        for sub in [
            "coordinator",
            "clusterd",
            "executor",
            "job-coordinator",
            "flight-server",
            "shuffle-svc",
            "mcp",
        ] {
            assert!(crate::daemon_cmd::is_daemon_subcommand(sub));
        }
    }
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
        "krishiv-mcp" => Some("mcp"),
        _ => None,
    }
}
