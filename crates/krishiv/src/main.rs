#![forbid(unsafe_code)]

use std::env;
use std::process;

fn main() {
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
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let response = krishiv::cli::dispatch(&arg_refs);

    if !response.stdout.is_empty() {
        print!("{}", response.stdout);
    }
    if !response.stderr.is_empty() {
        eprint!("{}", response.stderr);
    }
    process::exit(response.exit_code);
}
