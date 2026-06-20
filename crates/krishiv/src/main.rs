#![deny(unsafe_code)]

mod cli;
mod cluster_cmd;
mod daemon_cmd;
mod local_cluster;
mod pipeline_cmd;
mod process_util;
mod query_cli;
mod remote_client;
mod stream_cmd;
mod table_cmd;

use std::env;
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

    let args: Vec<String> = env::args().skip(1).collect();
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
