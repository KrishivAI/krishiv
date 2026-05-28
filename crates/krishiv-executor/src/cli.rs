//! Executor process CLI (`krishiv executor` / `krishiv-executor`).

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::grpc_client::SharedLeaseGeneration;
use crate::{
    ExecutorAssignmentInbox, ExecutorBarrierService, ExecutorConfig, ExecutorRuntime,
    ExecutorTaskRunner, GrpcCoordinatorService, SharedBarrierInjector, ShuffleContext,
    executor_barrier_grpc_server, serve_executor_task_grpc_with_listener,
};
use axum::Router;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use krishiv_checkpoint::{CheckpointStorage, open_checkpoint_storage_from_uri};
use dashmap::DashMap;
use krishiv_proto::{InitiateCheckpointRequest, JobId, TaskAttemptRef};
use krishiv_shuffle::{InMemoryShuffleStore, LocalDiskShuffleStore};
use krishiv_state::RedbStateBackend;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tonic::transport::Server;

/// Run the executor CLI (blocking async runtime).
pub async fn run_executor_cli(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let config = ExecutorCliConfig::parse(args)?;
    if config.help {
        print!("{}", ExecutorCliConfig::help());
        return Ok(());
    }

    let mode = config.mode;
    let heartbeat_interval_secs = config.heartbeat_interval_secs;
    let http_addr = config.http_addr;
    let task_grpc_addr = config.task_grpc_addr;
    let barrier_grpc_addr = config.barrier_grpc_addr;
    let shuffle_dir = config.shuffle_dir.clone();
    let checkpoint_uri = config.checkpoint_uri.clone();
    let slots = config.slots;
    let mut runtime = ExecutorRuntime::new(config.into_executor_config()?);

    // Start optional HTTP health server (/healthz, /readyz, /metrics).
    if let Some(addr) = http_addr {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("failed to bind HTTP addr {addr}: {e}"))?;
        println!(
            "Krishiv executor HTTP listening on {}",
            listener.local_addr().unwrap()
        );
        let http_executor_id = runtime.config().executor_id().as_str().to_owned();
        let http_slots = slots;
        tokio::spawn(async move {
            let router = executor_http_router(http_executor_id, http_slots);
            let _ = axum::serve(listener, router).await;
        });
    }

    match mode {
        ExecutorMode::DryRun => print_contract_summary(&runtime),
        ExecutorMode::RegisterOnce => register_once(&mut runtime).await,
        ExecutorMode::Connect => {
            heartbeat_loop(
                &mut runtime,
                heartbeat_interval_secs,
                task_grpc_addr,
                barrier_grpc_addr,
                shuffle_dir,
                checkpoint_uri,
                slots,
            )
            .await
        }
    }
}

fn executor_http_router(executor_id: String, slots: usize) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(|| async { "ready\n" }))
        .route(
            "/metrics",
            get(move || executor_metrics(executor_id.clone(), slots)),
        )
}

async fn executor_metrics(executor_id: String, slots: usize) -> impl IntoResponse {
    let body = format!(
        "\
# HELP krishiv_executor_up Executor process is running
# TYPE krishiv_executor_up gauge
krishiv_executor_up{{executor_id=\"{executor_id}\"}} 1
# HELP krishiv_executor_slots_total Total task slots configured
# TYPE krishiv_executor_slots_total gauge
krishiv_executor_slots_total{{executor_id=\"{executor_id}\"}} {slots}
"
    );
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

async fn register_once(runtime: &mut ExecutorRuntime) -> Result<(), String> {
    println!("{}", runtime.startup_summary());
    let (registration, heartbeat) = runtime
        .register_and_heartbeat_once()
        .await
        .map_err(|error| error.to_string())?;
    runtime.apply_lease_generation(registration.lease_generation());

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

#[allow(clippy::too_many_arguments)]
async fn heartbeat_loop(
    runtime: &mut ExecutorRuntime,
    heartbeat_interval_secs: u64,
    task_grpc_addr: Option<SocketAddr>,
    barrier_grpc_addr: Option<SocketAddr>,
    shuffle_dir: Option<std::path::PathBuf>,
    checkpoint_uri: String,
    slots: usize,
) -> Result<(), String> {
    // Bind task/barrier listeners FIRST so the *first* register advertises real
    // endpoints — avoids the double-register race that previously bumped the
    // lease before the loop could observe it (B8).
    let inbox = ExecutorAssignmentInbox::new();
    let task_listener = if let Some(addr) = task_grpc_addr {
        Some(
            TcpListener::bind(addr)
                .await
                .map_err(|e| format!("failed to bind task gRPC addr {addr}: {e}"))?,
        )
    } else {
        None
    };
    if let Some(listener) = &task_listener {
        let bound_addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{bound_addr}");
        runtime.set_advertised_endpoints(Some(endpoint.clone()), None);
        println!("Krishiv executor task gRPC listening on {bound_addr}");
    }
    let barrier_listener = if let Some(addr) = barrier_grpc_addr {
        Some(
            TcpListener::bind(addr)
                .await
                .map_err(|e| format!("failed to bind barrier gRPC addr {addr}: {e}"))?,
        )
    } else {
        None
    };
    if let Some(listener) = &barrier_listener {
        let bound_addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{bound_addr}");
        runtime.set_advertised_endpoints(None, Some(endpoint.clone()));
        println!("Krishiv executor barrier gRPC listening on {bound_addr}");
    }

    // First register (now with task/barrier endpoints already populated).
    register_once(runtime).await?;
    let initial_lease = runtime.config().lease_generation();
    let shared_lease = SharedLeaseGeneration::new(initial_lease);
    let coordinator_endpoint = runtime.config().coordinator_endpoint().to_owned();
    let coord_service = Arc::new(GrpcCoordinatorService::with_shared_lease(
        coordinator_endpoint.clone(),
        shared_lease.clone(),
    ));

    // Now spawn the task and barrier servers.  No more re-registers required.
    if let Some(listener) = task_listener {
        let server_inbox = inbox.clone();
        tokio::spawn(async move {
            let _ = serve_executor_task_grpc_with_listener(listener, server_inbox).await;
        });
    }
    let barrier_injector: SharedBarrierInjector = Default::default();
    if let Some(listener) = barrier_listener {
        let barrier_service = ExecutorBarrierService::new(
            barrier_injector.clone(),
            runtime.config().executor_id().as_str(),
        );
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(tonic::service::interceptor(
                    executor_barrier_grpc_server(barrier_service),
                    krishiv_metrics::grpc::extract_trace_context,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
    }

    // Checkpoint storage and state backend.  Both honor explicit URIs from the CLI/env.
    let checkpoint_storage: Arc<dyn CheckpointStorage> =
        open_checkpoint_storage_from_uri(&checkpoint_uri)
            .map_err(|e| format!("checkpoint storage at {checkpoint_uri}: {e}"))?;
    let state_backend =
        Arc::new(RedbStateBackend::ephemeral().map_err(|e| format!("state backend: {e}"))?);

    // Shuffle store: required for `shuffle-write:` fragments and for streaming
    // operators that exchange partitions between executors (B5).  When no
    // `--shuffle-dir` is provided we still create an in-memory shuffle store so
    // R4a typed shuffle write/read tasks succeed for tests.
    let running_attempts: Arc<DashMap<String, TaskAttemptRef>> = Arc::new(DashMap::new());
    runtime.set_running_attempts(running_attempts.clone());
    let mut runner_builder = ExecutorTaskRunner::new(inbox.clone())
        .with_live_lease(shared_lease.clone())
        .with_barrier_injector(barrier_injector)
        .with_running_attempts(running_attempts);
    if let Some(dir) = &shuffle_dir {
        let disk = Arc::new(
            LocalDiskShuffleStore::new(dir)
                .map_err(|e| format!("local shuffle store at {}: {e}", dir.display()))?,
        );
        // Start the shuffle Flight server on a kernel-chosen port and advertise it.
        let bind: SocketAddr = "0.0.0.0:0"
            .parse()
            .expect("0.0.0.0:0 is a valid socket address");
        let (local_addr, _server_handle) = krishiv_shuffle::flight::serve(bind, Arc::clone(&disk))
            .await
            .map_err(|e| format!("shuffle flight server: {e}"))?;
        let endpoint = local_addr.to_string();
        println!("Krishiv executor shuffle flight listening on {endpoint}");
        runner_builder = runner_builder.with_shuffle(ShuffleContext {
            store: disk,
            flight_endpoint: endpoint,
        });
    }
    let inmem_shuffle = Arc::new(InMemoryShuffleStore::new());
    runner_builder = runner_builder.with_inmem_shuffle(inmem_shuffle);

    let runner = Arc::new(runner_builder);

    // Spawn `slots` concurrent runner tasks all reading from the same inbox
    // (B6): without this the executor processes one task at a time regardless
    // of the advertised slot count.
    let shutdown = Arc::new(AtomicBool::new(false));
    let effective_slots = slots.max(1);
    let storage_for_tasks = Arc::clone(&checkpoint_storage);
    let backend_for_tasks = Arc::clone(&state_backend);
    for slot_idx in 0..effective_slots {
        let runner_loop = Arc::clone(&runner);
        let coord = Arc::clone(&coord_service);
        let storage = Arc::clone(&storage_for_tasks);
        let backend = Arc::clone(&backend_for_tasks);
        let shutdown_flag = Arc::clone(&shutdown);
        tokio::spawn(async move {
            loop {
                if shutdown_flag.load(Ordering::Relaxed) {
                    tracing::info!(slot = slot_idx, "runner shutting down");
                    break;
                }

                // Drain any pending barriers from the gRPC injector before
                // picking up the next task assignment.
                runner_loop
                    .drain_pending_barriers(backend.as_ref(), storage.as_ref(), coord.as_ref())
                    .await;

                match runner_loop.run_next_with(coord.as_ref()).await {
                    Ok(Some(_report)) => {}
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => {
                        tracing::warn!(slot = slot_idx, error = %e, "task runner error");
                        // Invalidate the channel so the next iteration reconnects.
                        coord.invalidate_channel().await;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });
    }

    let mut sigterm = signal(SignalKind::terminate()).map_err(|error| error.to_string())?;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(heartbeat_interval_secs)) => {
                match runtime.heartbeat_with_grpc_endpoint().await {
                    Ok(heartbeat) => {
                        use krishiv_proto::TransportDisposition;
                        // Propagate to the shared atomic so runner RPCs see the new lease.
                        shared_lease.set(heartbeat.lease_generation());
                        println!(
                            "heartbeat response version={} lease_generation={} disposition={} message={}",
                            heartbeat.version(),
                            heartbeat.lease_generation(),
                            heartbeat.disposition(),
                            heartbeat.message().unwrap_or("")
                        );
                        // F1: if the coordinator reports our lease is stale (or we are
                        // unknown to it), re-register.  This is the steady-state
                        // recovery path that allows an executor to survive a
                        // coordinator restart or a transient lease bump.
                        if matches!(
                            heartbeat.disposition(),
                            TransportDisposition::StaleLease
                                | TransportDisposition::UnknownExecutor
                        ) {
                            tracing::warn!(
                                disposition = %heartbeat.disposition(),
                                "heartbeat reported lease problem; re-registering"
                            );
                            match runtime.register_with_grpc_endpoint().await {
                                Ok(response) => {
                                    runtime.apply_lease_generation(response.lease_generation());
                                    shared_lease.set(response.lease_generation());
                                    coord_service.invalidate_channel().await;
                                }
                                Err(error) => {
                                    tracing::error!(error = %error, "re-register failed");
                                }
                            }
                            continue;
                        }
                        for cmd in heartbeat.checkpoint_commands() {
                            if let Ok(job_id) = JobId::try_new(cmd.job_id.as_str()) {
                                let req = InitiateCheckpointRequest {
                                    job_id,
                                    epoch: cmd.epoch,
                                    fencing_token: cmd.fencing_token,
                                };
                                let _ = runner
                                    .initiate_checkpoint_for_job(
                                        &req,
                                        state_backend.as_ref(),
                                        checkpoint_storage.as_ref(),
                                        coord_service.as_ref(),
                                    )
                                    .await;
                            }
                        }
                    }
                    Err(error) => {
                        let text = error.to_string();
                        tracing::warn!(error = %text, "heartbeat rpc failed; will retry");
                        // Drop the cached channel so the next iteration reconnects.
                        coord_service.invalidate_channel().await;
                    }
                }
            }
            _ = sigterm.recv() => {
                println!("SIGTERM received — deregistering and shutting down");
                shutdown.store(true, Ordering::Relaxed);
                // Give in-flight tasks a brief window to observe the shutdown
                // flag before we tear down the gRPC channel.
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = runtime.deregister_with_grpc_endpoint().await;
                return Ok(());
            }
        }
    }
}

// (LeaseGeneration is referenced via the heartbeat callback above.)

/// Executor CLI help text.
pub fn executor_cli_help() -> &'static str {
    ExecutorCliConfig::help()
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
    /// BarrierService gRPC listen address (WS-4).
    barrier_grpc_addr: Option<SocketAddr>,
    /// Local on-disk shuffle store directory; if set, the shuffle Flight
    /// server is started and the runner is wired for `shuffle-write:` fragments (B5).
    shuffle_dir: Option<std::path::PathBuf>,
    /// Checkpoint storage URI (filesystem path or `s3://`, `memory://`, …).
    /// Defaults to `KRISHIV_CHECKPOINT_STORAGE` then `file:///tmp/krishiv-checkpoints`.
    checkpoint_uri: String,
    help: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutorMode {
    DryRun,
    RegisterOnce,
    Connect,
}

impl ExecutorCliConfig {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
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
            barrier_grpc_addr: env::var("KRISHIV_BARRIER_GRPC_ADDR")
                .ok()
                .and_then(|value| value.parse().ok())
                .or_else(|| "0.0.0.0:50056".parse().ok()),
            shuffle_dir: env::var("KRISHIV_SHUFFLE_DIR")
                .ok()
                .map(std::path::PathBuf::from),
            checkpoint_uri: env::var("KRISHIV_CHECKPOINT_STORAGE")
                .unwrap_or_else(|_| String::from("file:///tmp/krishiv-checkpoints")),
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
                "--barrier-grpc-addr" => {
                    let value = next_arg(&mut args, "--barrier-grpc-addr")?;
                    if value == "off" {
                        config.barrier_grpc_addr = None;
                    } else {
                        config.barrier_grpc_addr = Some(value.parse().map_err(|_| {
                            format!("invalid socket address for --barrier-grpc-addr: {value}")
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
                "--shuffle-dir" => {
                    let value = next_arg(&mut args, "--shuffle-dir")?;
                    config.shuffle_dir = if value.is_empty() {
                        None
                    } else {
                        Some(std::path::PathBuf::from(value))
                    };
                }
                "--checkpoint-uri" => {
                    config.checkpoint_uri = next_arg(&mut args, "--checkpoint-uri")?;
                }
                "--help" | "-h" => config.help = true,
                unknown => return Err(format!("unknown option: {unknown}\n\n{}", Self::help())),
            }
        }

        Ok(config)
    }

    fn into_executor_config(self) -> Result<ExecutorConfig, String> {
        // Pre-populate task and barrier endpoints so that the FIRST register
        // call advertises real endpoints; the binary will rewrite them after
        // binding listeners (which use kernel-chosen ports if 0).  This avoids
        // the lease-bumping double-register race documented in B8.
        let mut cfg = ExecutorConfig::new(
            self.executor_id,
            self.host,
            self.slots,
            self.coordinator_endpoint,
        )
        .map_err(|error| error.to_string())?;
        if let Some(addr) = self.task_grpc_addr {
            cfg = cfg.with_task_endpoint(format!("http://{addr}"));
        }
        if let Some(addr) = self.barrier_grpc_addr {
            cfg = cfg.with_barrier_endpoint(format!("http://{addr}"));
        }
        Ok(cfg)
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

    pub fn help() -> &'static str {
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
           --barrier-grpc-addr <ADDR>   Barrier gRPC server address (default: KRISHIV_BARRIER_GRPC_ADDR or 0.0.0.0:50056; use 'off' to disable)\n\
           --shuffle-dir <DIR>          On-disk shuffle store directory (also KRISHIV_SHUFFLE_DIR)\n\
           --checkpoint-uri <URI>       Checkpoint storage URI: file://path, s3://bucket/prefix, memory:// (default: KRISHIV_CHECKPOINT_STORAGE or file:///tmp/krishiv-checkpoints)\n\
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
