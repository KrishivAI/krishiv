#![forbid(unsafe_code)]

use std::env;
use std::process;

fn main() {
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
    let metrics_config = krishiv_metrics::MetricsConfig {
        service_name: "krishiv-cli".into(),
        otlp_endpoint,
        ..Default::default()
    };
    let _metrics = krishiv_metrics::init(metrics_config).unwrap_or_else(|e| {
        eprintln!("warn: metrics init failed: {e}");
        krishiv_metrics::MetricsHandle::noop()
    });

    let args: Vec<String> = env::args().skip(1).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let response = krishiv_cli::dispatch(&arg_refs);

    if !response.stdout.is_empty() {
        print!("{}", response.stdout);
    }

    if !response.stderr.is_empty() {
        eprint!("{}", response.stderr);
    }

    process::exit(response.exit_code);
}
