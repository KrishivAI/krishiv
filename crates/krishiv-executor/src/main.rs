#![forbid(unsafe_code)]

use std::env;
use std::net::SocketAddr;
use std::process;
use std::time::Duration;

use axum::Router;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use krishiv_executor::{
    ExecutorAssignmentInbox, ExecutorConfig, ExecutorRuntime, ExecutorTaskRunner,
    GrpcCoordinatorService, serve_executor_task_grpc_with_listener,
};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() {
    match run(env::args().skip(1)).await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("{error}");
            process::exit(2);
        }
    }
}

async fn run(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let config = ExecutorCliConfig::parse(args)?;
    if config.help {
        print!("{}", ExecutorCliConfig::help());
        return Ok(());
    }

    let mode = config.mode;
    let heartbeat_interval_secs = config.heartbeat_interval_secs;
    let http_addr = config.http_addr;
    let task_grpc_addr = config.task_grpc_addr;
    let runtime = ExecutorRuntime::new(config.into_executor_config()?);

    // Start optional HTTP health server (/healthz, /readyz, /metrics).
    if let Some(addr) = http_addr {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("failed to bind HTTP addr {addr}: {e}"))?;
        println!(
            "Krishiv executor HTTP listening on {}",
            listener.local_addr().unwrap()
        );
        tokio::spawn(async move {
            let router = executor_http_router();
            let _ = axum::serve(listener, router).await;
        });
    }

    match mode {
        ExecutorMode::DryRun => print_contract_summary(&runtime),
        ExecutorMode::RegisterOnce => register_once(&runtime).await.map(|_| ()),
        ExecutorMode::Connect => {
            heartbeat_loop(&runtime, heartbeat_interval_secs, task_grpc_addr).await
        }
    }
}

fn executor_http_router() -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(|| async { "ready\n" }))
        .route("/metrics", get(executor_metrics))
}

async fn executor_metrics() -> impl IntoResponse {
    let body = "\
# HELP krishiv_executor_up Executor process is running
# TYPE krishiv_executor_up gauge
krishiv_executor_up 1
"
    .to_owned();
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

fn print_contract_summary(runtime: &ExecutorRuntime) -> Result<(), String> {
    let registration = runtime.registration_request();
    let heartbeat = runtime.heartbeat_request();

    println!("{}", runtime.startup_summary());
    println!(
        "registration version={} executor={} host={} slots={}",
        registration.version(),
        registration.descriptor().executor_id(),
        registration.descriptor().host(),
        registration.descriptor().slots()
    );
    println!(
        "heartbeat version={} executor={} lease_generation={} state={} running_attempts={}",
        heartbeat.version(),
        heartbeat.executor_id(),
        heartbeat.lease_generation(),
        heartbeat.state(),
        heartbeat.running_attempts().len()
    );
    Ok(())
}

async fn register_once(runtime: &ExecutorRuntime) -> Result<(), String> {
    println!("{}", runtime.startup_summary());
    let (registration, heartbeat) = runtime
        .register_and_heartbeat_once()
        .await
        .map_err(|error| error.to_string())?;

    println!(
        "registration response version={} executor={} lease_generation={} disposition={} message={}",
        registration.version(),
        registration.executor_id(),
        registration.lease_generation(),
        registration.disposition(),
        registration.message().unwrap_or("")
    );
    println!(
        "heartbeat response version={} lease_generation={} disposition={} message={}",
        heartbeat.version(),
        heartbeat.lease_generation(),
        heartbeat.disposition(),
        heartbeat.message().unwrap_or("")
    );
    Ok(())
}

async fn heartbeat_loop(
    runtime: &ExecutorRuntime,
    heartbeat_interval_secs: u64,
    task_grpc_addr: Option<SocketAddr>,
) -> Result<(), String> {
    register_once(runtime).await?;

    // GAP-CP-09: Start the executor task gRPC server so the coordinator can push
    // task assignments without polling.  The inbox is shared between the gRPC
    // service and the task runner loop below.
    let inbox = ExecutorAssignmentInbox::new();
    if let Some(addr) = task_grpc_addr {
        let task_listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("failed to bind task gRPC addr {addr}: {e}"))?;
        let bound_addr = task_listener.local_addr().unwrap();
        println!("Krishiv executor task gRPC listening on {bound_addr}");
        let server_inbox = inbox.clone();
        tokio::spawn(async move {
            let _ = serve_executor_task_grpc_with_listener(task_listener, server_inbox).await;
        });
    }

    // Spawn the task runner loop: pop assignments from the inbox and run them,
    // reporting status to the coordinator endpoint.
    let runner_inbox = inbox.clone();
    let runner_endpoint = runtime.config().coordinator_endpoint().to_owned();
    tokio::spawn(async move {
        let runner = ExecutorTaskRunner::new(runner_inbox);
        loop {
            let coord = GrpcCoordinatorService::new(runner_endpoint.clone());
            match runner.run_next_with(&coord).await {
                Ok(Some(_report)) => {}
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    eprintln!("task runner error: {e}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });

    let mut sigterm = signal(SignalKind::terminate()).map_err(|error| error.to_string())?;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(heartbeat_interval_secs)) => {
                let heartbeat = runtime
                    .heartbeat_with_grpc_endpoint()
                    .await
                    .map_err(|error| error.to_string())?;
                println!(
                    "heartbeat response version={} lease_generation={} disposition={} message={}",
                    heartbeat.version(),
                    heartbeat.lease_generation(),
                    heartbeat.disposition(),
                    heartbeat.message().unwrap_or("")
                );
            }
            _ = sigterm.recv() => {
                println!("SIGTERM received — deregistering and shutting down");
                let _ = runtime.deregister_with_grpc_endpoint().await;
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutorCliConfig {
    executor_id: String,
    host: String,
    slots: usize,
    coordinator_endpoint: String,
    mode: ExecutorMode,
    heartbeat_interval_secs: u64,
    http_addr: Option<SocketAddr>,
    /// GAP-CP-09: Address for the executor task gRPC server.
    task_grpc_addr: Option<SocketAddr>,
    help: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutorMode {
    DryRun,
    RegisterOnce,
    Connect,
}

impl ExecutorCliConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            executor_id: env::var("KRISHIV_EXECUTOR_ID")
                .unwrap_or_else(|_| String::from("exec-local")),
            host: env::var("HOSTNAME").unwrap_or_else(|_| String::from("localhost")),
            slots: env::var("KRISHIV_TASK_SLOTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            coordinator_endpoint: env::var("KRISHIV_COORDINATOR_ENDPOINT")
                .unwrap_or_else(|_| String::from("http://127.0.0.1:8080")),
            mode: ExecutorMode::DryRun,
            heartbeat_interval_secs: env::var("KRISHIV_HEARTBEAT_INTERVAL_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            http_addr: env::var("KRISHIV_HTTP_ADDR")
                .ok()
                .and_then(|value| value.parse().ok()),
            task_grpc_addr: env::var("KRISHIV_TASK_GRPC_ADDR")
                .ok()
                .and_then(|value| value.parse().ok())
                .or_else(|| "0.0.0.0:50055".parse().ok()),
            help: false,
        };
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--executor-id" => config.executor_id = next_arg(&mut args, "--executor-id")?,
                "--host" => config.host = next_arg(&mut args, "--host")?,
                "--slots" => {
                    let value = next_arg(&mut args, "--slots")?;
                    config.slots = value
                        .parse()
                        .map_err(|_| String::from("--slots must be a positive integer"))?;
                }
                "--coordinator" => {
                    config.coordinator_endpoint = next_arg(&mut args, "--coordinator")?;
                }
                "--register-once" => {
                    config.set_mode(ExecutorMode::RegisterOnce)?;
                }
                "--connect" => {
                    config.set_mode(ExecutorMode::Connect)?;
                }
                "--http-addr" => {
                    let value = next_arg(&mut args, "--http-addr")?;
                    config.http_addr =
                        Some(value.parse().map_err(|_| {
                            format!("invalid socket address for --http-addr: {value}")
                        })?);
                }
                "--task-grpc-addr" => {
                    let value = next_arg(&mut args, "--task-grpc-addr")?;
                    if value == "off" {
                        config.task_grpc_addr = None;
                    } else {
                        config.task_grpc_addr = Some(value.parse().map_err(|_| {
                            format!("invalid socket address for --task-grpc-addr: {value}")
                        })?);
                    }
                }
                "--heartbeat-interval-secs" => {
                    let value = next_arg(&mut args, "--heartbeat-interval-secs")?;
                    config.heartbeat_interval_secs = value.parse().map_err(|_| {
                        String::from("--heartbeat-interval-secs must be a positive integer")
                    })?;
                    if config.heartbeat_interval_secs == 0 {
                        return Err(String::from(
                            "--heartbeat-interval-secs must be greater than zero",
                        ));
                    }
                }
                "--help" | "-h" => config.help = true,
                unknown => return Err(format!("unknown option: {unknown}\n\n{}", Self::help())),
            }
        }

        Ok(config)
    }

    fn into_executor_config(self) -> Result<ExecutorConfig, String> {
        ExecutorConfig::new(
            self.executor_id,
            self.host,
            self.slots,
            self.coordinator_endpoint,
        )
        .map_err(|error| error.to_string())
    }

    fn set_mode(&mut self, mode: ExecutorMode) -> Result<(), String> {
        if self.mode != ExecutorMode::DryRun && self.mode != mode {
            return Err(String::from(
                "--register-once and --connect are mutually exclusive",
            ));
        }
        self.mode = mode;
        Ok(())
    }

    fn help() -> &'static str {
        "Run the Krishiv executor.\n\
         \n\
         Usage:\n\
           krishiv-executor [OPTIONS]\n\
         \n\
         Options:\n\
           --executor-id <ID>           Executor id, defaults to KRISHIV_EXECUTOR_ID or exec-local\n\
           --host <HOST>                Host or pod name, defaults to HOSTNAME or localhost\n\
           --slots <N>                  Task slots, defaults to KRISHIV_TASK_SLOTS or 1\n\
           --coordinator <URL>          Coordinator endpoint, defaults to KRISHIV_COORDINATOR_ENDPOINT or http://127.0.0.1:8080\n\
           --register-once              Register with the coordinator, send one heartbeat, then exit\n\
           --connect                    Register with the coordinator and continue heartbeating\n\
           --heartbeat-interval-secs <N> Heartbeat interval for --connect, defaults to KRISHIV_HEARTBEAT_INTERVAL_SECS or 10\n\
           --task-grpc-addr <ADDR>      Task gRPC server address (default: KRISHIV_TASK_GRPC_ADDR or 0.0.0.0:50055; use 'off' to disable)\n\
           -h, --help                   Show help\n"
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}"))
}

#[cfg(test)]
mod tests {
    use super::{ExecutorCliConfig, ExecutorMode};

    #[test]
    fn parses_explicit_config() {
        let config = ExecutorCliConfig::parse([
            String::from("--executor-id"),
            String::from("exec-1"),
            String::from("--host"),
            String::from("pod-a"),
            String::from("--slots"),
            String::from("2"),
            String::from("--coordinator"),
            String::from("http://coordinator"),
        ])
        .unwrap();

        assert_eq!(config.executor_id, "exec-1");
        assert_eq!(config.host, "pod-a");
        assert_eq!(config.slots, 2);
        assert_eq!(config.coordinator_endpoint, "http://coordinator");
        assert_eq!(config.mode, ExecutorMode::DryRun);
    }

    #[test]
    fn rejects_unknown_option() {
        let error = ExecutorCliConfig::parse([String::from("--wat")]).unwrap_err();

        assert!(error.contains("unknown option"));
    }

    #[test]
    fn parses_network_modes() {
        let register = ExecutorCliConfig::parse([String::from("--register-once")]).unwrap();
        let connect = ExecutorCliConfig::parse([
            String::from("--connect"),
            String::from("--heartbeat-interval-secs"),
            String::from("3"),
        ])
        .unwrap();

        assert_eq!(register.mode, ExecutorMode::RegisterOnce);
        assert_eq!(connect.mode, ExecutorMode::Connect);
        assert_eq!(connect.heartbeat_interval_secs, 3);
    }

    #[test]
    fn rejects_conflicting_network_modes() {
        let error =
            ExecutorCliConfig::parse([String::from("--connect"), String::from("--register-once")])
                .unwrap_err();

        assert!(error.contains("mutually exclusive"));
    }
}
