//! Executor process CLI (`krishiv executor` / `krishiv-executor`).

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::grpc_client::SharedLeaseGeneration;
use crate::{
    ExecutorAssignmentInbox, ExecutorBarrierService, ExecutorConfig, ExecutorRuntime,
    ExecutorTaskAuthConfig, ExecutorTaskRunner, GrpcCoordinatorService, SharedBarrierAckRegistry,
    SharedBarrierInjector, SharedKeyGroupRanges, ShuffleContext, executor_barrier_grpc_server,
};
use axum::Router;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use dashmap::DashMap;
use krishiv_common::durability::DurabilityProfile;
use krishiv_proto::{InitiateCheckpointRequest, JobId, TaskAttemptRef};
use krishiv_shuffle::{
    InMemoryShuffleStore, LocalDiskShuffleStore, ShuffleBackend, open_shuffle_backend_from_uri,
};
use krishiv_state::FjallStateBackend;
use krishiv_state::checkpoint::{CheckpointStorage, open_checkpoint_storage_from_uri};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tonic::transport::Server;

/// Run the executor CLI (blocking async runtime).
pub async fn run_executor_cli(args: impl IntoIterator<Item = String>) -> crate::ExecutorResult<()> {
    let config = ExecutorCliConfig::parse(args)?;
    if config.help {
        print!("{}", ExecutorCliConfig::help());
        return Ok(());
    }
    config.validate_task_auth_startup(&ExecutorTaskAuthConfig::from_env())?;
    config.validate_durable_startup()?;

    let mode = config.mode;
    let heartbeat_interval_secs = config.heartbeat_interval_secs;
    let http_addr = config.http_addr;
    let task_grpc_addr = config.task_grpc_addr;
    let barrier_grpc_addr = config.barrier_grpc_addr;
    let durability_profile = config.durability_profile;

    // Apply profile-driven backend defaults when explicit flags were not given.
    // The profile acts as a policy; explicit flags always win.
    let (shuffle_dir, shuffle_uri) = apply_shuffle_defaults(
        config.shuffle_dir.clone(),
        config.shuffle_uri.clone(),
        durability_profile,
    )?;
    let state_dir = apply_state_default(config.state_dir.clone(), durability_profile)?;
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
                shuffle_uri,
                state_dir,
                checkpoint_uri,
                slots,
                durability_profile,
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
    // Sanitize the label value: Prometheus text format uses `"` as the label
    // value delimiter, so any embedded quote or backslash must be escaped.
    // A malicious executor_id like `"} 0\nexec{x="` would otherwise inject
    // extra metric lines into the scrape output.
    let safe_id = executor_id.replace('\\', "\\\\").replace('"', "\\\"");
    let body = format!(
        "\
# HELP krishiv_executor_up Executor process is running
# TYPE krishiv_executor_up gauge
krishiv_executor_up{{executor_id=\"{safe_id}\"}} 1
# HELP krishiv_executor_slots_total Total task slots configured
# TYPE krishiv_executor_slots_total gauge
krishiv_executor_slots_total{{executor_id=\"{safe_id}\"}} {slots}
"
    );
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

fn print_contract_summary(runtime: &ExecutorRuntime) -> crate::ExecutorResult<()> {
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

async fn register_once(runtime: &mut ExecutorRuntime) -> crate::ExecutorResult<()> {
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

fn apply_non_stale_heartbeat_lease(
    runtime: &mut ExecutorRuntime,
    shared_lease: &SharedLeaseGeneration,
    heartbeat: &krishiv_proto::ExecutorHeartbeatResponse,
) -> bool {
    use krishiv_proto::TransportDisposition;
    if matches!(
        heartbeat.disposition(),
        TransportDisposition::StaleLease | TransportDisposition::UnknownExecutor
    ) {
        return false;
    }
    runtime.apply_lease_generation(heartbeat.lease_generation());
    shared_lease.set(heartbeat.lease_generation());
    true
}

fn apply_successful_reregister_lease(
    runtime: &mut ExecutorRuntime,
    shared_lease: &SharedLeaseGeneration,
    response: &krishiv_proto::RegisterExecutorResponse,
) {
    runtime.apply_lease_generation(response.lease_generation());
    shared_lease.set(response.lease_generation());
}

#[allow(clippy::too_many_arguments)]
async fn heartbeat_loop(
    runtime: &mut ExecutorRuntime,
    heartbeat_interval_secs: u64,
    task_grpc_addr: Option<SocketAddr>,
    barrier_grpc_addr: Option<SocketAddr>,
    shuffle_dir: Option<std::path::PathBuf>,
    shuffle_uri: Option<String>,
    state_dir: Option<std::path::PathBuf>,
    checkpoint_uri: String,
    slots: usize,
    durability_profile: DurabilityProfile,
) -> crate::ExecutorResult<()> {
    // Bind task/barrier listeners FIRST so the *first* register advertises real
    // endpoints — avoids the double-register race that previously bumped the
    // lease before the loop could observe it.
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
        // When bound to 0.0.0.0, use the configured host (HOSTNAME / KRISHIV_HOST)
        // so the coordinator can reach us via a routable address.
        let advertised_host = if bound_addr.ip().is_unspecified() {
            runtime.config().host().to_owned()
        } else {
            bound_addr.ip().to_string()
        };
        let endpoint = format!("http://{}:{}", advertised_host, bound_addr.port());
        runtime.set_advertised_endpoints(Some(endpoint.clone()), None);
        println!("Krishiv executor task gRPC listening on {bound_addr} (advertised {endpoint})");
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
        let advertised_host = if bound_addr.ip().is_unspecified() {
            runtime.config().host().to_owned()
        } else {
            bound_addr.ip().to_string()
        };
        let endpoint = format!("http://{}:{}", advertised_host, bound_addr.port());
        runtime.set_advertised_endpoints(None, Some(endpoint.clone()));
        println!("Krishiv executor barrier gRPC listening on {bound_addr} (advertised {endpoint})");
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

    // Create shared continuous-streaming state upfront so that both the gRPC
    // task server and the task runner operate on the same loop_executors and
    // continuous_inputs maps (distributed push_continuous_input path).
    let shared_loop_executors = Arc::new(DashMap::new());
    let shared_continuous_inputs: Arc<DashMap<String, Vec<arrow::record_batch::RecordBatch>>> =
        Arc::new(DashMap::new());

    // Now spawn the task and barrier servers.  No more re-registers required.
    if let Some(listener) = task_listener {
        let server_inbox = inbox.clone();
        let grpc_loop_executors = Arc::clone(&shared_loop_executors);
        let grpc_continuous_inputs = Arc::clone(&shared_continuous_inputs);
        tokio::spawn(async move {
            use crate::transport::serve_executor_task_grpc_with_listener_and_continuous;
            let _ = serve_executor_task_grpc_with_listener_and_continuous(
                listener,
                server_inbox,
                grpc_loop_executors,
                grpc_continuous_inputs,
            )
            .await;
        });
    }
    let barrier_injector: SharedBarrierInjector = Default::default();
    let barrier_ack_registry = SharedBarrierAckRegistry::new();
    let key_group_ranges = SharedKeyGroupRanges::new();
    let task_auth = ExecutorTaskAuthConfig::from_env();
    if let Some(listener) = barrier_listener {
        let barrier_service = ExecutorBarrierService::new(
            barrier_injector.clone(),
            runtime.config().executor_id().as_str(),
        )
        .with_state_backend_kind("fjall")
        .with_key_group_ranges(key_group_ranges.clone())
        .with_ack_registry(barrier_ack_registry.clone())
        .with_auth_config(task_auth);
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(tonic::service::interceptor::InterceptedService::new(
                    executor_barrier_grpc_server(barrier_service),
                    krishiv_metrics::grpc::extract_trace_context,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
    }

    // Checkpoint storage and state backend.
    // The per-task state backend (used by checkpoint RPCs) is always ephemeral; durable
    // window-operator state is managed per-job via `runner.state_dir` (see streaming.rs).
    let checkpoint_storage: Arc<dyn CheckpointStorage> =
        open_checkpoint_storage_from_uri(&checkpoint_uri)
            .map_err(|e| format!("checkpoint storage at {checkpoint_uri}: {e}"))?;
    let state_backend = Arc::new(
        FjallStateBackend::open_for_profile(durability_profile, state_dir.as_deref())
            .map_err(|e| format!("state backend: {e}"))?,
    );

    // Shuffle store: required for `shuffle-write:` fragments and for streaming
    // operators that exchange partitions between executors.
    let running_attempts: Arc<DashMap<String, TaskAttemptRef>> = Arc::new(DashMap::new());
    runtime.set_running_attempts(running_attempts.clone());
    let mut runner_builder = ExecutorTaskRunner::new(inbox.clone())
        .with_live_lease(shared_lease.clone())
        .with_barrier_injector(barrier_injector)
        .with_barrier_ack_registry(barrier_ack_registry)
        .with_key_group_ranges(key_group_ranges)
        .with_running_attempts(running_attempts)
        .with_executor_id(runtime.config().executor_id().clone())
        .with_udf_limits(krishiv_plan::udf::ResourceLimits::default())
        .with_shared_loop_executors(shared_loop_executors)
        .with_shared_continuous_inputs(shared_continuous_inputs);
    // Wire durable state dir so stream:loop: window operators use file-backed state.
    if let Some(ref dir) = state_dir {
        runner_builder = runner_builder.with_state_dir(dir.clone());
    }
    if let Some(uri) = shuffle_uri {
        let backend = open_shuffle_backend_from_uri(&uri, durability_profile)
            .map_err(|e| format!("shuffle URI {uri}: {e}"))?;
        match backend.as_ref() {
            ShuffleBackend::Local(disk) => {
                let bind: SocketAddr = "0.0.0.0:0"
                    .parse()
                    .map_err(|e| format!("failed to parse shuffle bind address: {e}"))?;
                let (local_addr, _server_handle) =
                    krishiv_shuffle::flight::serve(bind, Arc::clone(disk))
                        .await
                        .map_err(|e| format!("shuffle flight server: {e}"))?;
                let endpoint = local_addr.to_string();
                println!("Krishiv executor shuffle flight listening on {endpoint}");
                let local_dir = shuffle_dir.clone().unwrap_or_else(|| {
                    std::path::PathBuf::from(if uri.starts_with("file://") {
                        uri.strip_prefix("file://").unwrap_or(&uri)
                    } else {
                        "/tmp/krishiv-shuffle"
                    })
                });
                runner_builder = runner_builder
                    .with_shuffle(ShuffleContext {
                        store: Arc::clone(&backend),
                        local_dir,
                        flight_endpoint: endpoint,
                    })
                    .with_inmem_shuffle(backend);
            }
            ShuffleBackend::Object(_) | ShuffleBackend::Tiered(_) => {
                runner_builder = runner_builder
                    .with_shuffle(ShuffleContext {
                        store: Arc::clone(&backend),
                        local_dir: shuffle_dir.clone().unwrap_or_default(),
                        flight_endpoint: String::new(),
                    })
                    .with_inmem_shuffle(backend);
            }
            ShuffleBackend::InMemory(_) => {
                runner_builder = runner_builder.with_inmem_shuffle(backend);
            }
        }
    } else if let Some(dir) = &shuffle_dir {
        let disk = Arc::new(
            LocalDiskShuffleStore::new(dir)
                .map_err(|e| format!("local shuffle store at {}: {e}", dir.display()))?,
        );
        // Start the shuffle Flight server on a kernel-chosen port and advertise it.
        let bind: SocketAddr = "0.0.0.0:0"
            .parse()
            .map_err(|e| format!("failed to parse shuffle bind address: {e}"))?;
        let (local_addr, _server_handle) = krishiv_shuffle::flight::serve(bind, Arc::clone(&disk))
            .await
            .map_err(|e| format!("shuffle flight server: {e}"))?;
        let endpoint = local_addr.to_string();
        println!("Krishiv executor shuffle flight listening on {endpoint}");
        runner_builder = runner_builder.with_shuffle(ShuffleContext {
            store: Arc::new(krishiv_shuffle::ShuffleBackend::Local(disk)),
            local_dir: dir.clone(),
            flight_endpoint: endpoint,
        });
    }
    if krishiv_common::allows_unbounded_shuffle_store(durability_profile) {
        let inmem_shuffle = Arc::new(krishiv_shuffle::ShuffleBackend::InMemory(Arc::new(
            InMemoryShuffleStore::new(),
        )));
        runner_builder = runner_builder.with_inmem_shuffle(inmem_shuffle);
    }

    // Streaming progress buffer (GAP-OB-04): shared between runner tasks
    // (writers) and the heartbeat loop (reader).  Keyed by "job_id:task_id".
    let progress_buffer: Arc<dashmap::DashMap<String, krishiv_proto::StreamingProgressReport>> =
        Arc::new(dashmap::DashMap::new());
    let progress_cb: std::sync::Arc<dyn crate::runner::StreamingProgressCallback> =
        std::sync::Arc::new(ProgressBufferCallback {
            buffer: Arc::clone(&progress_buffer),
        });
    runner_builder = runner_builder.with_progress_callback(progress_cb);

    // Wire the progress buffer into the executor transport config so
    // heartbeat_request() drains and reports snapshots to the coordinator.
    runtime
        .config_mut()
        .set_progress_buffer(Arc::clone(&progress_buffer));

    let runner = Arc::new(runner_builder);

    // Spawn `slots` concurrent runner tasks all reading from the same inbox;
    // without this the executor processes one task at a time regardless
    // of the advertised slot count.
    let shutdown = Arc::new(AtomicBool::new(false));
    let effective_slots = slots.max(1);
    let storage_for_tasks = Arc::clone(&checkpoint_storage);
    let backend_for_tasks = Arc::clone(&state_backend);
    // Share a single wakeup notifier across all slots so any push wakes exactly
    // one waiting slot (notify_one) without requiring per-slot channels.
    let slot_wakeup = Arc::clone(runner.inbox().wakeup());
    for slot_idx in 0..effective_slots {
        let runner_loop = Arc::clone(&runner);
        let coord = Arc::clone(&coord_service);
        let storage = Arc::clone(&storage_for_tasks);
        let backend = Arc::clone(&backend_for_tasks);
        let shutdown_flag = Arc::clone(&shutdown);
        let wakeup = Arc::clone(&slot_wakeup);
        tokio::spawn(async move {
            loop {
                if shutdown_flag.load(Ordering::Acquire) {
                    tracing::info!(slot = slot_idx, "runner shutting down");
                    break;
                }

                // Drain any pending barriers from the gRPC injector before
                // picking up the next task assignment.
                runner_loop
                    .drain_pending_barriers(
                        Arc::clone(&backend) as Arc<dyn krishiv_state::StateBackend>,
                        Arc::clone(&storage)
                            as Arc<dyn krishiv_state::checkpoint::CheckpointStorage>,
                        coord.as_ref().clone(),
                    )
                    .await;

                match runner_loop.run_next_with(coord.as_ref()).await {
                    Ok(Some(_report)) => {}
                    Ok(None) => {
                        // Wait for a push notification or a 1-second fallback so
                        // the loop can detect shutdown or stale state without
                        // burning CPU on a 50 ms unconditional sleep.
                        tokio::select! {
                            _ = wakeup.notified() => {}
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
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

    let base_backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);
    let mut current_backoff = base_backoff;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(heartbeat_interval_secs)) => {
                match runtime.heartbeat_with_grpc_endpoint().await {
                    Ok(heartbeat) => {
                        use krishiv_proto::TransportDisposition;
                        // Reset backoff on successful heartbeat.
                        current_backoff = base_backoff;
                        println!(
                            "heartbeat response version={} lease_generation={} disposition={} message={}",
                            heartbeat.version(),
                            heartbeat.lease_generation(),
                            heartbeat.disposition(),
                            heartbeat.message().unwrap_or("")
                        );
                        // If the coordinator reports our lease is stale (or we are
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
                                    apply_successful_reregister_lease(
                                        runtime,
                                        &shared_lease,
                                        &response,
                                    );
                                    coord_service.invalidate_channel().await;
                                }
                                Err(error) => {
                                    tracing::error!(error = %error, "re-register failed");
                                }
                            }
                            continue;
                        }
                        // Only update the shared lease after confirming the
                        // heartbeat disposition is not stale (fix: lease-generation race).
                        apply_non_stale_heartbeat_lease(runtime, &shared_lease, &heartbeat);
                        for cmd in heartbeat.checkpoint_commands() {
                            if let Ok(job_id) = JobId::try_new(cmd.job_id.as_str()) {
                                let req = InitiateCheckpointRequest {
                                    job_id,
                                    epoch: cmd.epoch,
                                    fencing_token: cmd.fencing_token,
                                };
                                if let Err(error) = runner
                                    .initiate_checkpoint_for_job(
                                        &req,
                                        Arc::clone(&state_backend) as Arc<dyn krishiv_state::StateBackend>,
                                        Arc::clone(&checkpoint_storage) as Arc<dyn krishiv_state::checkpoint::CheckpointStorage>,
                                        coord_service.as_ref().clone(),
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        job_id = %cmd.job_id,
                                        epoch = cmd.epoch,
                                        error = %error,
                                        "checkpoint command failed"
                                    );
                                }
                            }
                        }
                        // R7.2: Apply source throttle limits from the coordinator heartbeat
                        // response.  The `SourceThrottleTable` is shared between the heartbeat
                        // loop (writer) and all runner task slots (readers) via an Arc<DashMap>,
                        // so no additional locking is required here.
                        for tc in heartbeat.throttle_commands() {
                            runner
                                .source_throttle_limits
                                .apply(&tc.source_id, tc.rows_per_second);
                        }
                    }
                    Err(error) => {
                        let text = error.to_string();
                        tracing::warn!(
                            error = %text,
                            backoff_secs = current_backoff.as_secs(),
                            "heartbeat rpc failed; will retry with backoff"
                        );
                        // Drop the cached channel so the next iteration reconnects.
                        coord_service.invalidate_channel().await;
                        tokio::time::sleep(current_backoff).await;
                        current_backoff = (current_backoff * 2).min(max_backoff);
                    }
                }
            }
            _ = sigterm.recv() => {
                println!("SIGTERM received — deregistering and shutting down");
                shutdown.store(true, Ordering::Release);
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
    /// Address for the executor task gRPC server.
    task_grpc_addr: Option<SocketAddr>,
    /// BarrierService gRPC listen address.
    barrier_grpc_addr: Option<SocketAddr>,
    /// Local on-disk shuffle store directory; if set, the shuffle Flight
    /// server is started and the runner is wired for `shuffle-write:` fragments.
    shuffle_dir: Option<std::path::PathBuf>,
    /// Shuffle storage URI (`file://`, `s3://`, `memory://`). Takes precedence over `--shuffle-dir`.
    /// Reads `KRISHIV_SHUFFLE_URI`.
    shuffle_uri: Option<String>,
    /// Root directory for durable window operator state.
    /// When set, continuous window operators use file-backed Fjall state instead
    /// of ephemeral (in-memory) state, surviving executor restarts.
    /// Reads `KRISHIV_STATE_DIR` env var; set automatically for durable profiles.
    state_dir: Option<std::path::PathBuf>,
    /// Checkpoint storage URI (filesystem path or `s3://`, `memory://`, …).
    /// Defaults to `KRISHIV_CHECKPOINT_STORAGE` then `file:///tmp/krishiv-checkpoints`.
    checkpoint_uri: String,
    /// Durability profile — controls which backends are required/auto-selected.
    /// Reads `KRISHIV_DURABILITY_PROFILE` env var; default is `dev-local`.
    durability_profile: DurabilityProfile,
    help: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutorMode {
    DryRun,
    RegisterOnce,
    Connect,
}

impl ExecutorCliConfig {
    pub fn parse(args: impl IntoIterator<Item = String>) -> crate::ExecutorResult<Self> {
        let mut config = Self {
            executor_id: env::var("KRISHIV_EXECUTOR_ID")
                .unwrap_or_else(|_| String::from("exec-local")),
            // POD_IP is injected via the Kubernetes downward API and is the
            // correct routable address for coordinator→executor gRPC callbacks.
            // Fall back to HOSTNAME for non-Kubernetes deployments.
            host: env::var("POD_IP")
                .or_else(|_| env::var("HOSTNAME"))
                .unwrap_or_else(|_| String::from("localhost")),
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
            shuffle_uri: env::var("KRISHIV_SHUFFLE_URI").ok(),
            state_dir: env::var("KRISHIV_STATE_DIR")
                .ok()
                .map(std::path::PathBuf::from),
            checkpoint_uri: env::var("KRISHIV_CHECKPOINT_STORAGE")
                .unwrap_or_else(|_| String::from("file:///tmp/krishiv-checkpoints")),
            durability_profile: env::var("KRISHIV_DURABILITY_PROFILE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
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
                        return Err(crate::ExecutorError::LocalExecution {
                            message: String::from(
                                "--heartbeat-interval-secs must be greater than zero",
                            ),
                        });
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
                "--shuffle-uri" => {
                    let value = next_arg(&mut args, "--shuffle-uri")?;
                    config.shuffle_uri = if value.is_empty() { None } else { Some(value) };
                }
                "--checkpoint-uri" => {
                    config.checkpoint_uri = next_arg(&mut args, "--checkpoint-uri")?;
                }
                "--state-dir" => {
                    let value = next_arg(&mut args, "--state-dir")?;
                    config.state_dir = if value.is_empty() {
                        None
                    } else {
                        Some(std::path::PathBuf::from(value))
                    };
                }
                "--durability-profile" => {
                    let value = next_arg(&mut args, "--durability-profile")?;
                    config.durability_profile =
                        value
                            .parse()
                            .map_err(|_| crate::ExecutorError::LocalExecution {
                                message: format!(
                                    "unknown --durability-profile '{value}'; supported: dev-local, \
                                 single-node-durable, distributed-durable"
                                ),
                            })?;
                }
                "--help" | "-h" => config.help = true,
                unknown => {
                    return Err(crate::ExecutorError::LocalExecution {
                        message: format!("unknown option: {unknown}\n\n{}", Self::help()),
                    });
                }
            }
        }

        Ok(config)
    }

    fn into_executor_config(self) -> crate::ExecutorResult<ExecutorConfig> {
        // Pre-populate task and barrier endpoints so that the FIRST register
        // call advertises real endpoints; the binary will rewrite them after
        // binding listeners (which use kernel-chosen ports if 0).  This avoids
        // the lease-bumping double-register race.
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

    fn validate_task_auth_startup(
        &self,
        auth: &ExecutorTaskAuthConfig,
    ) -> crate::ExecutorResult<()> {
        let network_control = self.task_grpc_addr.is_some() || self.barrier_grpc_addr.is_some();
        let durable = matches!(
            self.durability_profile,
            DurabilityProfile::SingleNodeDurable | DurabilityProfile::DistributedDurable
        ) || krishiv_common::is_production_mode();
        if durable && network_control && auth.bearer_token().is_none() {
            return Err(crate::ExecutorError::LocalExecution {
                message: format!(
                    "durability profile '{}' requires non-empty KRISHIV_EXECUTOR_TASK_BEARER_TOKEN \
                     when task or barrier gRPC is enabled",
                    self.durability_profile
                ),
            });
        }
        if self.task_grpc_addr.is_some() {
            auth.validate_required()?;
        }
        Ok(())
    }

    fn validate_durable_startup(&self) -> crate::ExecutorResult<()> {
        if !krishiv_common::allows_memory_checkpoint_uri(self.durability_profile)
            && self.checkpoint_uri.trim().starts_with("memory://")
        {
            return Err(crate::ExecutorError::LocalExecution {
                message: format!(
                    "checkpoint URI '{}' is forbidden for durability profile '{}'; \
                     use a file:// or s3:// URI",
                    self.checkpoint_uri, self.durability_profile
                ),
            });
        }
        Ok(())
    }

    fn set_mode(&mut self, mode: ExecutorMode) -> crate::ExecutorResult<()> {
        if self.mode != ExecutorMode::DryRun && self.mode != mode {
            return Err(crate::ExecutorError::LocalExecution {
                message: String::from("--register-once and --connect are mutually exclusive"),
            });
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
           --shuffle-uri <URI>          Shuffle storage URI: file://path, s3://bucket/prefix (KRISHIV_SHUFFLE_URI)\n\
           --checkpoint-uri <URI>       Checkpoint storage URI: file://path, s3://bucket/prefix, memory:// (default: KRISHIV_CHECKPOINT_STORAGE or file:///tmp/krishiv-checkpoints)\n\
           \n\
         Security:\n\
           Set KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH=true and non-empty KRISHIV_EXECUTOR_TASK_BEARER_TOKEN for distributed task-control gRPC.\n\
           -h, --help                   Show help\n"
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}"))
}

/// Apply shuffle backend defaults driven by the durability profile.
///
/// When `shuffle_uri` is set it takes precedence over directory defaults.
fn apply_shuffle_defaults(
    explicit_dir: Option<std::path::PathBuf>,
    shuffle_uri: Option<String>,
    profile: DurabilityProfile,
) -> crate::ExecutorResult<(Option<std::path::PathBuf>, Option<String>)> {
    if let Some(uri) = shuffle_uri.filter(|u| !u.trim().is_empty()) {
        return Ok((explicit_dir, Some(uri)));
    }
    match (explicit_dir, profile) {
        (Some(dir), _) => Ok((Some(dir), None)),
        (None, DurabilityProfile::DevLocal) => Ok((None, None)),
        (None, DurabilityProfile::SingleNodeDurable) => {
            let dir = std::path::PathBuf::from("/tmp/krishiv-shuffle");
            tracing::info!(
                path = %dir.display(),
                "single-node-durable: auto-selecting shuffle dir (set --shuffle-dir to override)"
            );
            Ok((Some(dir), None))
        }
        (None, DurabilityProfile::DistributedDurable) => {
            Err(crate::ExecutorError::LocalExecution {
                message: String::from(
                    "durability-profile=distributed-durable requires --shuffle-uri or --shuffle-dir \
                     (set KRISHIV_SHUFFLE_URI, KRISHIV_SHUFFLE_DIR, or pass explicit flags)",
                ),
            })
        }
    }
}

/// Apply state-dir defaults driven by the durability profile.
///
/// | Profile             | Explicit flag | Result                          |
/// |---------------------|---------------|---------------------------------|
/// | DevLocal            | any           | use as-is (None = ephemeral OK) |
/// | SingleNodeDurable   | None          | auto: `/tmp/krishiv-state`      |
/// | DistributedDurable  | None          | **error** — must be explicit    |
fn apply_state_default(
    explicit: Option<std::path::PathBuf>,
    profile: DurabilityProfile,
) -> crate::ExecutorResult<Option<std::path::PathBuf>> {
    match (explicit, profile) {
        (Some(dir), _) => Ok(Some(dir)),
        (None, DurabilityProfile::DevLocal) => Ok(None),
        (None, DurabilityProfile::SingleNodeDurable) => {
            let dir = std::path::PathBuf::from("/tmp/krishiv-state");
            tracing::info!(
                path = %dir.display(),
                "single-node-durable: auto-selecting state dir (set --state-dir to override)"
            );
            Ok(Some(dir))
        }
        (None, DurabilityProfile::DistributedDurable) => {
            Err(crate::ExecutorError::LocalExecution {
                message: String::from(
                    "durability-profile=distributed-durable requires --state-dir \
                     (set KRISHIV_STATE_DIR or pass --state-dir <path>)",
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutorCliConfig, ExecutorMode, apply_non_stale_heartbeat_lease,
        apply_successful_reregister_lease,
    };
    use crate::grpc_client::SharedLeaseGeneration;
    use crate::{ExecutorConfig, ExecutorRuntime, ExecutorTaskAuthConfig};
    use krishiv_proto::{
        ExecutorHeartbeatResponse, ExecutorId, LeaseGeneration, RegisterExecutorResponse,
        TransportDisposition,
    };

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
    fn stale_heartbeat_does_not_advance_runtime_or_shared_lease() {
        let mut runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-lease", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let shared_lease = SharedLeaseGeneration::new(LeaseGeneration::initial());
        let stale_lease = LeaseGeneration::initial().next();
        let stale = ExecutorHeartbeatResponse::new(stale_lease, TransportDisposition::StaleLease);

        assert!(!apply_non_stale_heartbeat_lease(
            &mut runtime,
            &shared_lease,
            &stale
        ));
        assert_eq!(
            runtime.config().lease_generation(),
            LeaseGeneration::initial()
        );
        assert_eq!(shared_lease.get(), LeaseGeneration::initial());
    }

    #[test]
    fn successful_reregister_advances_runtime_and_shared_lease() {
        let mut runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-lease", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let shared_lease = SharedLeaseGeneration::new(LeaseGeneration::initial());
        let next_lease = LeaseGeneration::initial().next();
        let response = RegisterExecutorResponse::new(
            ExecutorId::try_new("exec-lease").unwrap(),
            next_lease,
            TransportDisposition::Accepted,
        );

        apply_successful_reregister_lease(&mut runtime, &shared_lease, &response);

        assert_eq!(runtime.config().lease_generation(), next_lease);
        assert_eq!(shared_lease.get(), next_lease);
    }

    #[test]
    fn rejects_unknown_option() {
        let error = ExecutorCliConfig::parse([String::from("--wat")]).unwrap_err();

        assert!(error.to_string().contains("unknown option"));
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

        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn parses_help_flag() {
        let config = ExecutorCliConfig::parse([String::from("--help")]).unwrap();
        assert!(config.help);
    }

    #[test]
    fn parses_short_help_flag() {
        let config = ExecutorCliConfig::parse([String::from("-h")]).unwrap();
        assert!(config.help);
    }

    #[test]
    fn default_config_from_empty_args() {
        let config = ExecutorCliConfig::parse(std::iter::empty::<String>()).unwrap();
        let expected_host = std::env::var("POD_IP")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| String::from("localhost"));
        assert_eq!(config.executor_id, "exec-local");
        assert_eq!(config.host, expected_host);
        assert_eq!(config.slots, 1);
        assert_eq!(config.coordinator_endpoint, "http://127.0.0.1:8080");
        assert_eq!(config.mode, ExecutorMode::DryRun);
        assert_eq!(config.heartbeat_interval_secs, 10);
        assert!(!config.help);
    }

    #[test]
    fn parses_custom_slots() {
        let config =
            ExecutorCliConfig::parse([String::from("--slots"), String::from("8")]).unwrap();
        assert_eq!(config.slots, 8);
    }

    #[test]
    fn rejects_non_numeric_slots() {
        let err =
            ExecutorCliConfig::parse([String::from("--slots"), String::from("abc")]).unwrap_err();
        assert!(
            err.to_string()
                .contains("--slots must be a positive integer")
        );
    }

    #[test]
    fn parses_http_addr() {
        let config =
            ExecutorCliConfig::parse([String::from("--http-addr"), String::from("127.0.0.1:9090")])
                .unwrap();
        assert_eq!(config.http_addr, Some("127.0.0.1:9090".parse().unwrap()));
    }

    #[test]
    fn rejects_invalid_http_addr() {
        let err =
            ExecutorCliConfig::parse([String::from("--http-addr"), String::from("not-a-port")])
                .unwrap_err();
        assert!(err.to_string().contains("invalid socket address"));
    }

    #[test]
    fn parses_task_grpc_addr() {
        let config = ExecutorCliConfig::parse([
            String::from("--task-grpc-addr"),
            String::from("0.0.0.0:50099"),
        ])
        .unwrap();
        assert_eq!(
            config.task_grpc_addr,
            Some("0.0.0.0:50099".parse().unwrap())
        );
    }

    #[test]
    fn task_grpc_addr_off_disables() {
        let config =
            ExecutorCliConfig::parse([String::from("--task-grpc-addr"), String::from("off")])
                .unwrap();
        assert!(config.task_grpc_addr.is_none());
    }

    #[test]
    fn durable_profile_rejects_memory_checkpoint_uri() {
        let config = ExecutorCliConfig::parse([
            String::from("--durability-profile"),
            String::from("single-node-durable"),
            String::from("--checkpoint-uri"),
            String::from("memory://test"),
        ])
        .unwrap();
        let err = config.validate_durable_startup().unwrap_err();
        assert!(err.to_string().contains("memory://"));
    }

    #[test]
    fn required_task_auth_rejects_exposed_task_grpc_without_token() {
        let config = ExecutorCliConfig::parse([
            String::from("--connect"),
            String::from("--task-grpc-addr"),
            String::from("0.0.0.0:50055"),
        ])
        .unwrap();
        let auth = ExecutorTaskAuthConfig::new(true, None);

        let err = config.validate_task_auth_startup(&auth).unwrap_err();

        assert!(
            err.to_string()
                .contains("KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH")
        );
        assert!(
            err.to_string()
                .contains("KRISHIV_EXECUTOR_TASK_BEARER_TOKEN")
        );
    }

    #[test]
    fn required_task_auth_accepts_exposed_task_grpc_with_token() {
        let config = ExecutorCliConfig::parse([
            String::from("--connect"),
            String::from("--task-grpc-addr"),
            String::from("0.0.0.0:50055"),
        ])
        .unwrap();
        let auth = ExecutorTaskAuthConfig::new(true, Some(String::from("task-secret")));

        config.validate_task_auth_startup(&auth).unwrap();
    }

    #[test]
    fn required_task_auth_allows_disabled_task_grpc_without_token() {
        let config =
            ExecutorCliConfig::parse([String::from("--task-grpc-addr"), String::from("off")])
                .unwrap();
        let auth = ExecutorTaskAuthConfig::new(true, None);

        config.validate_task_auth_startup(&auth).unwrap();
    }

    #[test]
    fn parses_barrier_grpc_addr() {
        let config = ExecutorCliConfig::parse([
            String::from("--barrier-grpc-addr"),
            String::from("0.0.0.0:50098"),
        ])
        .unwrap();
        assert_eq!(
            config.barrier_grpc_addr,
            Some("0.0.0.0:50098".parse().unwrap())
        );
    }

    #[test]
    fn barrier_grpc_addr_off_disables() {
        let config =
            ExecutorCliConfig::parse([String::from("--barrier-grpc-addr"), String::from("off")])
                .unwrap();
        assert!(config.barrier_grpc_addr.is_none());
    }

    #[test]
    fn parses_heartbeat_interval() {
        let config = ExecutorCliConfig::parse([
            String::from("--heartbeat-interval-secs"),
            String::from("5"),
        ])
        .unwrap();
        assert_eq!(config.heartbeat_interval_secs, 5);
    }

    #[test]
    fn rejects_zero_heartbeat_interval() {
        let err = ExecutorCliConfig::parse([
            String::from("--heartbeat-interval-secs"),
            String::from("0"),
        ])
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("--heartbeat-interval-secs must be greater than zero")
        );
    }

    #[test]
    fn rejects_non_numeric_heartbeat_interval() {
        let err = ExecutorCliConfig::parse([
            String::from("--heartbeat-interval-secs"),
            String::from("xyz"),
        ])
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("--heartbeat-interval-secs must be a positive integer")
        );
    }

    #[test]
    fn parses_shuffle_dir() {
        let config =
            ExecutorCliConfig::parse([String::from("--shuffle-dir"), String::from("/tmp/shuffle")])
                .unwrap();
        assert_eq!(
            config.shuffle_dir,
            Some(std::path::PathBuf::from("/tmp/shuffle"))
        );
    }

    #[test]
    fn shuffle_dir_empty_string_errors() {
        // Empty string after --shuffle-dir is treated as missing value
        let err = ExecutorCliConfig::parse([String::from("--shuffle-dir"), String::from("")])
            .unwrap_err();
        assert!(err.to_string().contains("missing value for --shuffle-dir"));
    }

    #[test]
    fn parses_checkpoint_uri() {
        let config = ExecutorCliConfig::parse([
            String::from("--checkpoint-uri"),
            String::from("s3://bucket/prefix"),
        ])
        .unwrap();
        assert_eq!(config.checkpoint_uri, "s3://bucket/prefix");
    }

    #[test]
    fn default_checkpoint_uri() {
        let config = ExecutorCliConfig::parse(std::iter::empty::<String>()).unwrap();
        assert_eq!(config.checkpoint_uri, "file:///tmp/krishiv-checkpoints");
    }

    #[test]
    fn executor_cli_help_is_nonempty() {
        let help = ExecutorCliConfig::help();
        assert!(!help.is_empty());
        assert!(help.contains("--executor-id"));
        assert!(help.contains("--slots"));
        assert!(help.contains("--connect"));
    }

    #[test]
    fn rejects_missing_value_for_flag() {
        let err = ExecutorCliConfig::parse([String::from("--executor-id")]).unwrap_err();
        assert!(err.to_string().contains("missing value for"));
    }

    #[test]
    fn rejects_extra_positional_argument() {
        let err = ExecutorCliConfig::parse([
            String::from("--executor-id"),
            String::from("e1"),
            String::from("extra-positional"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("unknown option"));
    }
}

/// Bridges streaming progress snapshots from runner tasks to the heartbeat loop
/// via a shared DashMap. Runner tasks write progress; the heartbeat loop drains
/// and attaches reports to the next `ExecutorHeartbeat`.
struct ProgressBufferCallback {
    buffer: Arc<dashmap::DashMap<String, krishiv_proto::StreamingProgressReport>>,
}

impl crate::runner::StreamingProgressCallback for ProgressBufferCallback {
    fn on_progress(&self, snapshot: &crate::runner::StreamingProgressSnapshot) {
        let key = format!("{}:{}", snapshot.job_id, snapshot.task_id);
        let (Ok(job_id), Ok(task_id)) = (
            krishiv_proto::JobId::try_new(snapshot.job_id.clone()),
            krishiv_proto::TaskId::try_new(snapshot.task_id.clone()),
        ) else {
            tracing::warn!(
                job_id = %snapshot.job_id,
                task_id = %snapshot.task_id,
                "skipping streaming progress report with invalid job_id/task_id"
            );
            return;
        };
        let report = krishiv_proto::StreamingProgressReport::new(job_id, task_id)
            .with_watermark_ms(snapshot.watermark_ms)
            .with_rows_emitted(snapshot.rows_emitted)
            .with_batches_emitted(snapshot.batches_emitted)
            .with_state_bytes(snapshot.state_bytes)
            .with_source_offset(snapshot.source_offset.clone().unwrap_or_default())
            .with_timestamp_ms(snapshot.timestamp_ms);
        self.buffer.insert(key, report);
    }
}
