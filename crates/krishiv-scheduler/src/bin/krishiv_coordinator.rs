#![forbid(unsafe_code)]

//! Standalone `krishiv-coordinator` binary for bare-metal / VM deployments.
//!
//! For Kubernetes deployments use `krishiv-operator`, which embeds the
//! coordinator alongside the CRD reconciler. This binary is for bare metal
//! or VM environments where Kubernetes is not available.
//!
//! Usage:
//!   krishiv-coordinator [--coordinator-id <ID>] [--grpc-addr <HOST:PORT>]
//!
//! Options:
//!   --coordinator-id <ID>     Coordinator id, defaults to KRISHIV_COORDINATOR_ID or coord-local
//!   --grpc-addr <HOST:PORT>   gRPC listen address, defaults to KRISHIV_GRPC_ADDR or 0.0.0.0:9090
//!   -h, --help                Show help

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use krishiv_proto::{CoordinatorId, CoordinatorState};
use krishiv_scheduler::{
    Coordinator, SharedCoordinator, StabilityMetrics,
    serve_coordinator_executor_grpc_with_listener,
};
use krishiv_shuffle::{LocalDiskShuffleStore, ShuffleStore as _};
use tokio::net::TcpListener;
use tokio::time::{Duration, interval};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = CoordinatorCliConfig::parse(env::args().skip(1))?;
    if config.help {
        print!("{}", CoordinatorCliConfig::help());
        return Ok(());
    }

    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    let coordinator = SharedCoordinator::new(Coordinator::active(coordinator_id));

    // If a shuffle directory is configured, run a background GC loop that
    // drains terminal-job shuffle partitions from the local disk store.
    if let Some(shuffle_dir) = &config.shuffle_dir {
        let store: Arc<LocalDiskShuffleStore> =
            Arc::new(LocalDiskShuffleStore::new(shuffle_dir).map_err(|e| {
                format!("failed to open shuffle store at '{}': {e}", shuffle_dir.display())
            })?);
        let gc_coordinator = coordinator.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let job_ids = gc_coordinator
                    .write()
                    .map(|mut c| c.take_gc_ready_jobs())
                    .unwrap_or_default();
                for job_id in job_ids {
                    if let Err(e) = store.delete_job_partitions(job_id.as_str()).await {
                        eprintln!("shuffle GC failed for job {job_id}: {e}");
                    }
                }
            }
        });
    }

    // Start optional HTTP health/metrics server.
    if let Some(http_addr) = config.http_addr {
        let http_coordinator = coordinator.clone();
        let http_listener = TcpListener::bind(http_addr).await?;
        println!(
            "Krishiv coordinator {} HTTP listening on {}",
            config.coordinator_id,
            http_listener.local_addr()?
        );
        tokio::spawn(async move {
            let router = coordinator_http_router(http_coordinator);
            let _ = axum::serve(http_listener, router).await;
        });
    }

    let listener = TcpListener::bind(config.grpc_addr).await?;
    println!(
        "Krishiv coordinator {} gRPC listening on {}",
        config.coordinator_id,
        listener.local_addr()?
    );

    serve_coordinator_executor_grpc_with_listener(listener, coordinator).await?;
    Ok(())
}

fn coordinator_http_router(coordinator: SharedCoordinator) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(coordinator)
}

async fn readyz(
    State(coordinator): State<SharedCoordinator>,
) -> Result<&'static str, (axum::http::StatusCode, String)> {
    match coordinator.read() {
        Ok(c) if c.state() == CoordinatorState::Active => Ok("ready\n"),
        Ok(_) => Err((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "coordinator is not active\n".to_owned(),
        )),
        Err(_) => Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "coordinator lock poisoned\n".to_owned(),
        )),
    }
}

async fn metrics(State(coordinator): State<SharedCoordinator>) -> impl IntoResponse {
    let m = coordinator
        .read()
        .map(|c| c.stability_metrics())
        .unwrap_or_else(|_| StabilityMetrics::empty());
    let max_hb_age = m
        .heartbeat_ages()
        .iter()
        .map(|a| a.age_ticks())
        .max()
        .unwrap_or(0);
    let body = format!(
        "\
# HELP krishiv_running_tasks Currently running task count
# TYPE krishiv_running_tasks gauge
krishiv_running_tasks {running}
# HELP krishiv_task_retries_total Total stage-level retries scheduled
# TYPE krishiv_task_retries_total counter
krishiv_task_retries_total {retries}
# HELP krishiv_failed_assignments_total Total failed task assignments
# TYPE krishiv_failed_assignments_total counter
krishiv_failed_assignments_total {failed}
# HELP krishiv_max_executor_heartbeat_age_ticks Max executor heartbeat age in scheduler ticks
# TYPE krishiv_max_executor_heartbeat_age_ticks gauge
krishiv_max_executor_heartbeat_age_ticks {hb_age}
# HELP krishiv_shuffle_partitions_available Shuffle partitions available across active jobs
# TYPE krishiv_shuffle_partitions_available gauge
krishiv_shuffle_partitions_available {shuffle_partitions}
# HELP krishiv_shuffle_bytes_written_total Total bytes written to shuffle store
# TYPE krishiv_shuffle_bytes_written_total counter
krishiv_shuffle_bytes_written_total {shuffle_bytes}
",
        running = m.running_task_count(),
        retries = m.retry_count(),
        failed = m.failed_assignments(),
        hb_age = max_hb_age,
        shuffle_partitions = m.shuffle_partitions_available,
        shuffle_bytes = m.shuffle_bytes_written,
    );
    ([(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoordinatorCliConfig {
    coordinator_id: String,
    grpc_addr: SocketAddr,
    http_addr: Option<SocketAddr>,
    shuffle_dir: Option<PathBuf>,
    help: bool,
}

impl CoordinatorCliConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut config = Self {
            coordinator_id: env::var("KRISHIV_COORDINATOR_ID")
                .unwrap_or_else(|_| String::from("coord-local")),
            grpc_addr: env::var("KRISHIV_GRPC_ADDR")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| "0.0.0.0:9090".parse().unwrap()),
            http_addr: env::var("KRISHIV_HTTP_ADDR")
                .ok()
                .and_then(|value| value.parse().ok()),
            shuffle_dir: env::var("KRISHIV_SHUFFLE_DIR").ok().map(PathBuf::from),
            help: false,
        };
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--coordinator-id" => {
                    config.coordinator_id = next_arg(&mut args, "--coordinator-id")?;
                }
                "--grpc-addr" => {
                    let value = next_arg(&mut args, "--grpc-addr")?;
                    config.grpc_addr = value
                        .parse()
                        .map_err(|_| format!("invalid socket address for --grpc-addr: {value}"))?;
                }
                "--shuffle-dir" => {
                    let value = next_arg(&mut args, "--shuffle-dir")?;
                    config.shuffle_dir = Some(PathBuf::from(value));
                }
                "--http-addr" => {
                    let value = next_arg(&mut args, "--http-addr")?;
                    config.http_addr = Some(
                        value
                            .parse()
                            .map_err(|_| format!("invalid socket address for --http-addr: {value}"))?,
                    );
                }
                "--help" | "-h" => config.help = true,
                unknown => {
                    return Err(format!("unknown option: {unknown}\n\n{}", Self::help()).into());
                }
            }
        }

        if config.coordinator_id.trim().is_empty() {
            return Err("coordinator id cannot be empty".into());
        }

        Ok(config)
    }

    fn help() -> &'static str {
        "Run the Krishiv coordinator for bare-metal / VM deployments.\n\
         \n\
         Usage:\n\
           krishiv-coordinator [OPTIONS]\n\
         \n\
         Options:\n\
           --coordinator-id <ID>     Coordinator id, defaults to KRISHIV_COORDINATOR_ID or coord-local\n\
           --grpc-addr <HOST:PORT>   gRPC listen address, defaults to KRISHIV_GRPC_ADDR or 0.0.0.0:9090\n\
           --http-addr <HOST:PORT>   HTTP listen address for /healthz /readyz /metrics (optional)\n\
           --shuffle-dir <PATH>      Local shuffle store dir, defaults to KRISHIV_SHUFFLE_DIR (optional)\n\
           -h, --help                Show help\n"
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, Box<dyn Error>> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}").into())
}

#[cfg(test)]
mod tests {
    use super::CoordinatorCliConfig;

    #[test]
    fn parses_defaults() {
        let config = CoordinatorCliConfig::parse([]).unwrap();
        assert_eq!(config.coordinator_id, "coord-local");
        assert_eq!(config.grpc_addr.port(), 9090);
        assert!(!config.help);
    }

    #[test]
    fn parses_explicit_flags() {
        let config = CoordinatorCliConfig::parse([
            String::from("--coordinator-id"),
            String::from("coord-prod"),
            String::from("--grpc-addr"),
            String::from("127.0.0.1:19090"),
        ])
        .unwrap();

        assert_eq!(config.coordinator_id, "coord-prod");
        assert_eq!(config.grpc_addr.to_string(), "127.0.0.1:19090");
    }

    #[test]
    fn parses_help_flag() {
        let config = CoordinatorCliConfig::parse([String::from("--help")]).unwrap();
        assert!(config.help);
    }

    #[test]
    fn rejects_unknown_flag() {
        let error = CoordinatorCliConfig::parse([String::from("--wat")]).unwrap_err();
        assert!(error.to_string().contains("unknown option"));
    }

    #[test]
    fn rejects_invalid_grpc_addr() {
        let error = CoordinatorCliConfig::parse([
            String::from("--grpc-addr"),
            String::from("not-a-socket-addr"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("invalid socket address"));
    }
}
