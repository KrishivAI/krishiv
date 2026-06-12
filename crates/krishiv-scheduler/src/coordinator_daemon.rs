//! Shared coordinator / clusterd startup (bare metal + VM).

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use krishiv_common::durability::DurabilityProfile;
use krishiv_proto::{CoordinatorId, CoordinatorState};
use krishiv_shuffle::{LocalDiskShuffleStore, ShuffleStore as _};
use tokio::net::TcpListener;
use tokio::time::{Duration, interval};

use crate::InMemoryMetadataStore;
use crate::auth::configured_coordinator_bearer_token;
use crate::rpc_drain::InFlightTracker;
use crate::store::MetadataStore;
use crate::{
    ClusterControlPlane, Coordinator, LeaderElection, SharedCoordinator, SingleNodeLeader,
    scheduler_metrics, serve_coordinator_executor_grpc_with_listener_and_tracker,
    server_tls_config_from_env,
};

use crate::RocksDbMetadataStore;

#[cfg(feature = "etcd")]
use crate::{EtcdLeaseElection, EtcdMetadataStore};

const EXECUTOR_TASK_BEARER_TOKEN_ENV: &str = "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN";
const COORDINATOR_BEARER_TOKEN_ENV: &str = crate::auth::COORDINATOR_BEARER_TOKEN_ENV;
const COORDINATOR_BEARER_TOKENS_ENV: &str = crate::auth::COORDINATOR_BEARER_TOKENS_ENV;

/// Callback invoked by `spawn_coordinator_sidecars` to start co-located services
/// that depend on both the coordinator and crates not available in krishiv-scheduler.
///
/// The future is spawned as a Tokio task so it runs concurrently with the coordinator.
/// Example: start a Flight SQL server co-located with the coordinator.
pub type CoordinatorSidecarFn = Box<
    dyn FnOnce(SharedCoordinator) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send,
>;

/// CLI configuration for coordinator-family binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDaemonConfig {
    pub coordinator_id: String,
    pub grpc_addr: SocketAddr,
    pub http_addr: Option<SocketAddr>,
    pub shuffle_dir: Option<PathBuf>,
    pub durability_profile: DurabilityProfile,
    pub metadata_backend: Option<String>,
    pub metadata_path: Option<PathBuf>,
    /// `single` (default) or `etcd` (requires `feature = "etcd"` on clusterd builds).
    pub leader_backend: String,
    /// etcd gRPC endpoints (e.g. `http://127.0.0.1:2379`).
    pub etcd_endpoints: Vec<String>,
    /// Coordination key for CCP leader lease (default `/krishiv/ccp/leader`).
    pub etcd_lease_key: String,
    /// etcd lease TTL in seconds (default 15).
    pub leader_lease_duration_s: u64,
    /// Allow anonymous gRPC (dev only; default: false).
    pub insecure: bool,
    /// If set, start a co-located Arrow Flight SQL server on this address.
    /// The Flight server is wired directly to the coordinator — no HTTP proxy.
    /// Set via `--flight-addr <HOST:PORT>` or `KRISHIV_FLIGHT_ADDR`.
    pub flight_addr: Option<SocketAddr>,
    pub help: bool,
}

impl CoordinatorDaemonConfig {
    /// Minimal config for HTTP router construction when full daemon flags are unavailable.
    pub fn http_sidecar(profile: DurabilityProfile) -> Self {
        Self {
            coordinator_id: String::from("coord-http"),
            grpc_addr: "127.0.0.1:0".parse().expect("valid addr"),
            http_addr: Some("127.0.0.1:0".parse().expect("valid addr")),
            shuffle_dir: None,
            durability_profile: profile,
            metadata_backend: None,
            metadata_path: None,
            leader_backend: String::from("single"),
            etcd_endpoints: Vec::new(),
            etcd_lease_key: String::from("/krishiv/ccp/leader"),
            leader_lease_duration_s: 15,
            insecure: false,
            flight_addr: None,
            help: false,
        }
    }
}

/// Build the leader-election backend for clusterd from daemon flags.
pub async fn build_leader_election(
    config: &CoordinatorDaemonConfig,
) -> Result<Arc<dyn LeaderElection + Send + Sync>, Box<dyn Error>> {
    match config.leader_backend.as_str() {
        "single" => Ok(Arc::new(SingleNodeLeader::new())),
        "etcd" => build_etcd_leader_election(config).await,
        other => {
            Err(format!("unknown --leader-backend '{other}' (supported: single, etcd)").into())
        }
    }
}

async fn build_etcd_leader_election(
    config: &CoordinatorDaemonConfig,
) -> Result<Arc<dyn LeaderElection + Send + Sync>, Box<dyn Error>> {
    #[cfg(feature = "etcd")]
    {
        if config.etcd_endpoints.is_empty() {
            return Err(
                "--leader-backend etcd requires at least one --etcd-endpoints value (or KRISHIV_ETCD_ENDPOINTS)".into(),
            );
        }
        let election = EtcdLeaseElection::connect(
            config.etcd_endpoints.clone(),
            config.etcd_lease_key.clone(),
            config.coordinator_id.clone(),
            config.leader_lease_duration_s,
        )
        .await?;
        Ok(Arc::new(election))
    }
    #[cfg(not(feature = "etcd"))]
    {
        let _ = config;
        Err("etcd leader election requires building krishiv-scheduler with feature `etcd`".into())
    }
}

/// Build a shared coordinator from daemon configuration.
pub fn build_shared_coordinator(
    config: &CoordinatorDaemonConfig,
) -> Result<SharedCoordinator, Box<dyn Error>> {
    build_shared_coordinator_sync(config)
}

/// Synchronous entry used by binaries; etcd metadata uses a blocking connect.
/// Attach metadata store with durability-aware fail-closed write semantics.
fn attach_metadata_store(
    coord: Coordinator,
    store: impl MetadataStore + 'static,
    config: &CoordinatorDaemonConfig,
) -> Coordinator {
    let fail_closed =
        krishiv_common::profile_requires_fail_closed_metadata(config.durability_profile);
    coord
        .with_durability_profile(config.durability_profile)
        .with_store_fail_closed(store, fail_closed)
}

pub fn build_shared_coordinator_sync(
    config: &CoordinatorDaemonConfig,
) -> Result<SharedCoordinator, Box<dyn Error>> {
    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    // `mut` is required when the `redb` or `etcd` features are enabled so the
    // `recover_from_store` arm can take `&mut self`; in the default in-memory
    // build the binding is read-only.
    #[allow(unused_mut)]
    let mut coord = Coordinator::active(coordinator_id);
    let coordinator = match (config.metadata_backend.as_deref(), &config.metadata_path) {
        (Some("memory"), _) | (None, None) => {
            // InMemoryMetadataStore is the correct default for embedded/test use.
            SharedCoordinator::new(attach_metadata_store(
                coord,
                InMemoryMetadataStore::default(),
                config,
            ))
        }
        #[cfg(feature = "etcd")]
        (Some("etcd"), _) => {
            if config.etcd_endpoints.is_empty() {
                return Err(
                    "--metadata-backend etcd requires --etcd-endpoints (or KRISHIV_ETCD_ENDPOINTS)"
                        .into(),
                );
            }
            let store = krishiv_common::async_util::block_on(EtcdMetadataStore::connect(
                config.etcd_endpoints.clone(),
            ))
            .map_err(|e| format!("etcd metadata store: {e}"))?;
            coord
                .recover_from_store(&store)
                .map_err(|e| format!("coordinator recovery failed: {e}"))?;
            SharedCoordinator::new(attach_metadata_store(coord, store, config))
        }
        #[cfg(not(feature = "etcd"))]
        (Some("etcd"), _) => {
            return Err(
                "etcd metadata requires building krishiv-scheduler with feature `etcd`".into(),
            );
        }
        // "rocksdb" is the canonical name; "redb" is accepted as a legacy alias.
        (Some("rocksdb" | "redb"), Some(path)) => {
            let store = RocksDbMetadataStore::open(path.as_path())
                .map_err(|e| format!("rocksdb store '{}': {e}", path.display()))?;
            coord
                .recover_from_store(&store)
                .map_err(|e| format!("coordinator recovery failed: {e}"))?;
            SharedCoordinator::new(attach_metadata_store(coord, store, config))
        }
        (Some("rocksdb" | "redb"), None) => {
            return Err("--metadata-backend rocksdb requires --metadata-path".into());
        }
        (backend, Some(path)) => {
            let backend = backend.unwrap_or("(none)");
            return Err(format!(
                "cannot use --metadata-path '{path}' with backend '{backend}'; \
                 for durable single-node storage use: \
                 --metadata-backend rocksdb. \
                 For distributed HA use: --metadata-backend etcd (requires --features etcd).",
                path = path.display(),
            )
            .into());
        }
        (Some(unknown), _) => {
            return Err(format!(
                "unknown --metadata-backend '{unknown}'; supported: memory, rocksdb, etcd"
            )
            .into());
        }
    };
    Ok(coordinator)
}

/// Run cluster control plane loops and block on the gRPC server.
pub async fn run_cluster_control_plane(
    ccp: Arc<ClusterControlPlane>,
    listener: TcpListener,
) -> Result<(), Box<dyn Error>> {
    let ccp_loop = Arc::clone(&ccp);
    let leader_task = tokio::spawn(async move {
        ccp_loop.run_leader_loop().await;
    });

    let coordinator = ccp.shared_coordinator().clone();
    let in_flight = InFlightTracker::new();
    let tls_config = server_tls_config_from_env()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let grpc_serve = serve_coordinator_executor_grpc_with_listener_and_tracker(
        listener,
        coordinator.clone(),
        in_flight.clone(),
        tls_config,
    );

    tokio::select! {
        result = grpc_serve => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received; initiating coordinator graceful shutdown");
        }
        _ = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sig) => { sig.recv().await; }
                Err(_) => std::future::pending::<()>().await,
            }
        } => {
            tracing::info!("SIGTERM received; initiating coordinator graceful shutdown");
        }
    }

    leader_task.abort();
    let _ = leader_task.await;

    // Drain in-flight gRPC handlers (heartbeats, task updates) before
    // demoting so the new leader doesn't observe stale state mid-write
    // (R11). Bounded by a timeout fallback in case a handler is wedged —
    // unlike the old fixed 2-second sleep, this returns as soon as the
    // server is actually idle and still makes forward progress if not.
    if !in_flight.drain(Duration::from_secs(10)).await {
        tracing::warn!(
            active_calls = in_flight.active_count(),
            "RPC drain timed out with calls still in flight; proceeding with demotion"
        );
    }

    // Demote coordinator and release leadership.
    {
        let mut coord = coordinator.write().await;
        coord.demote_to_standby();
        tracing::info!("coordinator demoted to standby on shutdown");
    }
    ccp.leader().release().await;

    Ok(())
}

/// Spawn shuffle GC, HTTP/metrics, and any additional co-located services.
///
/// `extra_sidecars` are additional futures to run alongside the coordinator.
/// Each factory receives a `SharedCoordinator` clone and returns a future that
/// is spawned as a Tokio task. Use this to start co-located services (e.g., a
/// Flight SQL server) that depend on both the coordinator and crates not
/// available in `krishiv-scheduler`.
pub async fn spawn_coordinator_sidecars(
    coordinator: &SharedCoordinator,
    config: &CoordinatorDaemonConfig,
    extra_http_factory: Option<Box<dyn FnOnce(SharedCoordinator) -> Router + Send>>,
    extra_sidecars: Vec<CoordinatorSidecarFn>,
) -> Result<(), Box<dyn Error>> {
    if let Some(shuffle_dir) = &config.shuffle_dir {
        let store: Arc<LocalDiskShuffleStore> =
            Arc::new(LocalDiskShuffleStore::new(shuffle_dir).map_err(|e| {
                format!(
                    "failed to open shuffle store at '{}': {e}",
                    shuffle_dir.display()
                )
            })?);
        let gc_coordinator = coordinator.clone();
        let orphan_store = Arc::clone(&store);
        let orphan_shuffle_dir = shuffle_dir.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            let mut orphan_tick_count: u64 = 0;
            loop {
                ticker.tick().await;
                let job_ids = gc_coordinator.write().await.take_gc_ready_jobs();
                for job_id in job_ids {
                    if let Err(e) = store.delete_job_partitions(job_id.as_str()).await {
                        tracing::error!(job_id = %job_id, error = %e, "shuffle GC failed");
                    }
                }
                // Orphan scan every 60 s (every 12th 5-second tick) — removes
                // partition files for jobs that crashed before reaching terminal
                // state and were never added to gc_ready_jobs (C4).
                orphan_tick_count += 1;
                if orphan_tick_count.is_multiple_of(12) {
                    let active = gc_coordinator.read().await.active_job_ids();
                    let dir = orphan_shuffle_dir.clone();
                    let store2 = Arc::clone(&orphan_store);
                    tokio::task::spawn_blocking(move || {
                        match krishiv_shuffle::orphan::cleanup_orphans(&dir, &active) {
                            Ok(n) if n > 0 => {
                                tracing::info!(
                                    removed = n,
                                    "shuffle orphan GC: removed orphaned partition files"
                                );
                            }
                            Ok(_) => {}
                            Err(e) => tracing::error!(error = %e, "shuffle orphan GC failed"),
                        }
                        drop(store2); // keep Arc alive
                    });
                }
            }
        });
    }

    if let Some(http_addr) = config.http_addr {
        let http_coordinator = coordinator.clone();
        let http_listener = TcpListener::bind(http_addr).await?;
        tracing::info!(addr = %http_listener.local_addr()?, "Krishiv coordinator HTTP listening");
        let http_config = config.clone();
        tokio::spawn(async move {
            let router = coordinator_http_router(http_coordinator.clone(), &http_config);
            let router = if let Some(factory) = extra_http_factory {
                router.merge(factory(http_coordinator))
            } else {
                router
            };
            let _ = axum::serve(http_listener, router).await;
        });
    }

    // Spawn any additional co-located services (e.g., Flight SQL server).
    for sidecar_fn in extra_sidecars {
        let coordinator_clone = coordinator.clone();
        tokio::spawn(sidecar_fn(coordinator_clone));
    }

    // Orchestration loops (heartbeat, task launch, barrier dispatch) are now spawned
    // by run_standalone_coordinator / run_cluster_control_plane via
    // spawn_orchestration_loops(). No separate coordinator_tick loop here.

    Ok(())
}

pub fn coordinator_http_router(
    coordinator: SharedCoordinator,
    config: &CoordinatorDaemonConfig,
) -> Router {
    use crate::http_auth::{require_coordinator_bearer, resolve_http_bearer_tokens};
    use axum::middleware;

    let public = Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics));

    let protected = Router::new()
        .route("/api/v1/jobs", get(api_jobs))
        .route("/api/v1/executors", get(api_executors))
        .route(
            "/api/v1/executors/{executor_id}/reset",
            post(api_executor_reset),
        )
        .route(
            "/api/v1/batch-sql/submit",
            post(crate::batch_sql_http::api_batch_sql_submit),
        )
        .route(
            "/api/v1/batch-sql/{job_id}",
            get(crate::batch_sql_http::api_batch_sql_poll),
        )
        .route(
            "/api/v1/bounded-window",
            post(crate::bounded_window_http::api_bounded_window),
        )
        .route(
            "/api/v1/continuous-register",
            post(crate::continuous_stream_http::api_continuous_register),
        )
        .route(
            "/api/v1/continuous-push",
            post(crate::continuous_stream_http::api_continuous_push),
        )
        .route(
            "/api/v1/continuous-drain",
            post(crate::continuous_stream_http::api_continuous_drain),
        )
        .route("/api/v1/jobs/{job_id}/diagnose", get(api_job_diagnose))
        .with_state(coordinator.clone());

    let protected = if http_auth_required(config) {
        let tokens = resolve_http_bearer_tokens();
        protected.layer(middleware::from_fn(move |req, next| {
            let tokens = tokens.clone();
            async move { require_coordinator_bearer(req, next, &tokens).await }
        }))
    } else {
        protected
    };

    Router::new()
        .merge(public)
        .merge(protected)
        .with_state(coordinator)
}

fn http_auth_required(config: &CoordinatorDaemonConfig) -> bool {
    if krishiv_common::allow_anonymous_http_override() {
        return false;
    }
    krishiv_common::requires_http_auth(config.durability_profile)
}

#[derive(Debug, Clone, serde::Serialize)]
struct LiveJobsResponse {
    jobs: Vec<LiveJobView>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct LiveJobView {
    job_id: String,
    kind: String,
    state: String,
    stage_count: usize,
    task_count: usize,
    assigned_task_count: usize,
    running_task_count: usize,
    succeeded_task_count: usize,
    failed_task_count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
struct LiveExecutorsResponse {
    executors: Vec<LiveExecutorView>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct LiveExecutorView {
    executor_id: String,
    host: String,
    slots: usize,
    state: String,
    lease_generation: u64,
    running_task_count: usize,
    last_heartbeat_tick: u64,
    /// Consecutive task failure count (circuit breaker input). Resets to 0 on success.
    consecutive_task_failures: u32,
}

async fn api_jobs(State(coordinator): State<SharedCoordinator>) -> impl IntoResponse {
    let jobs = {
        let coord = coordinator.read().await;
        coord
            .job_snapshots()
            .into_iter()
            .map(|job| LiveJobView {
                job_id: job.job_id().to_string(),
                kind: format!("{:?}", job.kind()),
                state: format!("{:?}", job.state()),
                stage_count: job.stage_count(),
                task_count: job.task_count(),
                assigned_task_count: job.assigned_task_count(),
                running_task_count: job.running_task_count(),
                succeeded_task_count: job.succeeded_task_count(),
                failed_task_count: job.failed_task_count(),
            })
            .collect::<Vec<_>>()
    };
    Json(LiveJobsResponse { jobs })
}

async fn api_executors(State(coordinator): State<SharedCoordinator>) -> impl IntoResponse {
    let executors = {
        let coord = coordinator.read().await;
        coord
            .executor_snapshots()
            .into_iter()
            .map(|record| {
                let descriptor = record.descriptor();
                LiveExecutorView {
                    executor_id: record.executor_id().to_string(),
                    host: descriptor.host().to_string(),
                    slots: descriptor.slots(),
                    state: format!("{:?}", record.state()),
                    lease_generation: record.lease_generation().as_u64(),
                    running_task_count: record.running_tasks().len(),
                    last_heartbeat_tick: record.last_heartbeat_tick(),
                    consecutive_task_failures: record.consecutive_task_failures,
                }
            })
            .collect::<Vec<_>>()
    };
    Json(LiveExecutorsResponse { executors })
}

/// Reset the circuit-breaker failure counter for one executor.
///
/// Call this after confirming an executor is healthy again so the coordinator
/// resumes assigning tasks to it without waiting for a successful task cycle.
async fn api_executor_reset(
    State(coordinator): State<SharedCoordinator>,
    axum::extract::Path(executor_id_str): axum::extract::Path<String>,
) -> impl IntoResponse {
    use krishiv_proto::ExecutorId;
    let executor_id = match ExecutorId::try_new(&executor_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid executor id"})),
            );
        }
    };
    coordinator
        .write()
        .await
        .executors
        .reset_task_failures(&executor_id);
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({"reset": true, "executor_id": executor_id_str})),
    )
}

/// Structured observability report for production diagnosis (GAP-OB-07).
async fn api_job_diagnose(
    State(coordinator): State<SharedCoordinator>,
    axum::extract::Path(job_id_str): axum::extract::Path<String>,
) -> impl IntoResponse {
    use axum::http::StatusCode;
    use krishiv_proto::JobId;

    let job_id = match JobId::try_new(&job_id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid job id"})),
            )
                .into_response();
        }
    };

    let report = {
        let coord = coordinator.read().await;
        match crate::coordinator::observability::build_observability_report(&coord, &job_id) {
            Ok(report) => report,
            Err(e) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    };

    Json(report).into_response()
}

async fn readyz(
    State(coordinator): State<SharedCoordinator>,
) -> Result<&'static str, (axum::http::StatusCode, String)> {
    let c = coordinator.read().await;
    if c.state() != CoordinatorState::Active {
        return Err((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "coordinator is not active\n".to_owned(),
        ));
    }
    let healthy_executors = c
        .executors()
        .executors
        .values()
        .filter(|e| e.state.can_accept_work())
        .count();
    if healthy_executors == 0 {
        return Err((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "no healthy executors registered\n".to_owned(),
        ));
    }
    Ok("ready\n")
}

async fn metrics(State(coordinator): State<SharedCoordinator>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        render_metrics_body(&coordinator).await,
    )
}

async fn render_metrics_body(coordinator: &SharedCoordinator) -> String {
    let m = coordinator.read().await.stability_metrics();
    let max_hb_age = m
        .heartbeat_ages()
        .iter()
        .map(|a| a.age_ticks())
        .max()
        .unwrap_or(0);
    let mut body = format!(
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
",
        running = m.running_task_count(),
        retries = m.retry_count(),
        failed = m.failed_assignments(),
        hb_age = max_hb_age,
    );
    body.push('\n');
    let scheduler = scheduler_metrics();
    let scheduler_body = format!(
        "\
# HELP krishiv_scheduler_jobs_submitted_total Total jobs submitted to the coordinator
# TYPE krishiv_scheduler_jobs_submitted_total counter
krishiv_scheduler_jobs_submitted_total {jobs}
# HELP krishiv_scheduler_checkpoint_epochs_total Total checkpoint epochs initiated
# TYPE krishiv_scheduler_checkpoint_epochs_total counter
krishiv_scheduler_checkpoint_epochs_total {epochs}
# HELP krishiv_scheduler_tasks_assigned_total Total task assignments launched
# TYPE krishiv_scheduler_tasks_assigned_total counter
krishiv_scheduler_tasks_assigned_total {tasks}
",
        jobs = scheduler.jobs_submitted_total,
        epochs = scheduler.checkpoint_epochs_total,
        tasks = scheduler.tasks_assigned_total,
    );
    body.push_str(&scheduler_body);
    body.push_str(&krishiv_metrics::global_metrics().render_prometheus());
    body
}

/// Parse coordinator-family daemon flags (`krishiv-coordinator`, `krishiv-clusterd`, `krishiv clusterd`, …).
pub fn parse_coordinator_daemon_config(
    args: impl IntoIterator<Item = String>,
) -> Result<CoordinatorDaemonConfig, Box<dyn Error>> {
    let mut config = CoordinatorDaemonConfig {
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
        durability_profile: env::var("KRISHIV_DURABILITY_PROFILE")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or_default(),
        metadata_backend: env::var("KRISHIV_METADATA_BACKEND").ok(),
        metadata_path: env::var("KRISHIV_METADATA_PATH").ok().map(PathBuf::from),
        leader_backend: env::var("KRISHIV_LEADER_BACKEND")
            .unwrap_or_else(|_| String::from("single")),
        etcd_endpoints: parse_etcd_endpoints_env(),
        etcd_lease_key: env::var("KRISHIV_ETCD_LEADER_KEY")
            .unwrap_or_else(|_| String::from("/krishiv/ccp/leader")),
        leader_lease_duration_s: env::var("KRISHIV_LEADER_LEASE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15),
        insecure: env::var("KRISHIV_ALLOW_ANONYMOUS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false),
        flight_addr: env::var("KRISHIV_FLIGHT_ADDR")
            .ok()
            .and_then(|value| value.parse().ok()),
        help: false,
    };
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--coordinator-id" => {
                config.coordinator_id = next_daemon_arg(&mut args, "--coordinator-id")?;
            }
            "--grpc-addr" => {
                let value = next_daemon_arg(&mut args, "--grpc-addr")?;
                config.grpc_addr = value
                    .parse()
                    .map_err(|_| format!("invalid socket address for --grpc-addr: {value}"))?;
            }
            "--http-addr" => {
                let value = next_daemon_arg(&mut args, "--http-addr")?;
                config.http_addr = Some(
                    value
                        .parse()
                        .map_err(|_| format!("invalid socket address for --http-addr: {value}"))?,
                );
            }
            "--shuffle-dir" => {
                config.shuffle_dir =
                    Some(PathBuf::from(next_daemon_arg(&mut args, "--shuffle-dir")?));
            }
            "--durability-profile" => {
                config.durability_profile =
                    next_daemon_arg(&mut args, "--durability-profile")?.parse()?;
            }
            "--metadata-backend" => {
                config.metadata_backend = Some(next_daemon_arg(&mut args, "--metadata-backend")?);
            }
            "--metadata-path" => {
                config.metadata_path = Some(PathBuf::from(next_daemon_arg(
                    &mut args,
                    "--metadata-path",
                )?));
            }
            "--leader-backend" => {
                config.leader_backend = next_daemon_arg(&mut args, "--leader-backend")?;
            }
            "--etcd-endpoints" => {
                config.etcd_endpoints =
                    parse_etcd_endpoints_list(&next_daemon_arg(&mut args, "--etcd-endpoints")?);
            }
            "--etcd-lease-key" => {
                config.etcd_lease_key = next_daemon_arg(&mut args, "--etcd-lease-key")?;
            }
            "--leader-lease-secs" => {
                let value = next_daemon_arg(&mut args, "--leader-lease-secs")?;
                config.leader_lease_duration_s = value
                    .parse()
                    .map_err(|_| format!("invalid integer for --leader-lease-secs: {value}"))?;
            }
            "--insecure" => config.insecure = true,
            "--flight-addr" => {
                let value = next_daemon_arg(&mut args, "--flight-addr")?;
                config.flight_addr = Some(
                    value
                        .parse()
                        .map_err(|_| format!("invalid socket address for --flight-addr: {value}"))?,
                );
            }
            "--help" | "-h" => config.help = true,
            unknown => {
                return Err(
                    format!("unknown option: {unknown}\n\n{}", coordinator_daemon_help()).into(),
                );
            }
        }
    }
    if config.coordinator_id.trim().is_empty() {
        return Err("coordinator id cannot be empty".into());
    }
    if config.leader_backend == "etcd" && config.etcd_endpoints.is_empty() {
        return Err(
            "--leader-backend etcd requires --etcd-endpoints or KRISHIV_ETCD_ENDPOINTS".into(),
        );
    }
    if config.leader_lease_duration_s == 0 {
        return Err("--leader-lease-secs must be greater than zero".into());
    }
    validate_durability_profile_config(&config)?;
    Ok(config)
}

fn validate_durability_profile_config(
    config: &CoordinatorDaemonConfig,
) -> Result<(), Box<dyn Error>> {
    match config.durability_profile {
        DurabilityProfile::DevLocal => Ok(()),
        DurabilityProfile::SingleNodeDurable => {
            // Only rocksdb gives real crash-recovery for coordinator metadata.
            match config.metadata_backend.as_deref() {
                Some("rocksdb" | "redb") => {}
                Some(other) => {
                    return Err(format!(
                        "single-node-durable requires --metadata-backend redb; got '{other}'"
                    )
                    .into());
                }
                None => {
                    return Err(
                        "single-node-durable requires --metadata-backend redb and --metadata-path <path>"
                            .into(),
                    );
                }
            }
            if config.metadata_path.is_none() {
                return Err("single-node-durable requires --metadata-path".into());
            }
            if config.shuffle_dir.is_none() {
                return Err("single-node-durable requires --shuffle-dir".into());
            }
            if config.leader_backend != "single" {
                return Err("single-node-durable requires --leader-backend single".into());
            }
            Ok(())
        }
        DurabilityProfile::DistributedDurable => {
            if config.metadata_backend.as_deref() != Some("etcd") {
                return Err("distributed-durable requires --metadata-backend etcd".into());
            }
            if config.leader_backend != "etcd" {
                return Err("distributed-durable requires --leader-backend etcd".into());
            }
            if config.etcd_endpoints.is_empty() {
                return Err(
                    "distributed-durable requires --etcd-endpoints or KRISHIV_ETCD_ENDPOINTS"
                        .into(),
                );
            }
            Ok(())
        }
    }
}

fn validate_runtime_security_config(
    config: &CoordinatorDaemonConfig,
    executor_task_bearer_token_configured: bool,
    coordinator_bearer_token_configured: bool,
) -> Result<(), Box<dyn Error>> {
    let durable = matches!(
        config.durability_profile,
        DurabilityProfile::SingleNodeDurable | DurabilityProfile::DistributedDurable
    );
    if durable {
        if config.insecure {
            return Err(format!(
                "{} rejects --insecure coordinator gRPC",
                config.durability_profile
            )
            .into());
        }
        if let Err(error) = crate::auth::validate_coordinator_bearer_token_sources() {
            return Err(
                format!("failed to read coordinator bearer token configuration: {error}").into(),
            );
        }
        if !coordinator_bearer_token_configured {
            return Err(format!(
                "{} requires non-empty {COORDINATOR_BEARER_TOKEN_ENV} \
                 or {COORDINATOR_BEARER_TOKENS_ENV} so coordinator gRPC is authenticated",
                config.durability_profile
            )
            .into());
        }
    }
    if config.durability_profile == DurabilityProfile::DistributedDurable
        && !executor_task_bearer_token_configured
    {
        return Err(format!(
            "distributed-durable requires non-empty {EXECUTOR_TASK_BEARER_TOKEN_ENV} so executor task-control gRPC is authenticated"
        )
        .into());
    }
    if config.http_addr.is_some()
        && http_auth_required(config)
        && !coordinator_bearer_token_configured
        && !krishiv_common::allow_anonymous_http_override()
    {
        return Err(format!(
            "coordinator HTTP is enabled but no bearer tokens are configured; \
             set {COORDINATOR_BEARER_TOKEN_ENV} or {COORDINATOR_BEARER_TOKENS_ENV} \
             (or {} for dev-only bypass)",
            krishiv_common::ALLOW_ANONYMOUS_HTTP_ENV,
        )
        .into());
    }
    Ok(())
}

fn executor_task_bearer_token_configured() -> bool {
    env::var(EXECUTOR_TASK_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| !token.trim().is_empty())
        .unwrap_or(false)
}

fn coordinator_bearer_token_configured() -> bool {
    crate::auth::coordinator_bearer_auth_configured()
}

fn configure_coordinator_grpc_auth(config: &CoordinatorDaemonConfig) -> bool {
    if config.insecure {
        if let Err(error) = crate::auth::set_allow_anonymous() {
            tracing::error!("{error}");
            return false;
        }
        return false;
    }
    // OIDC/JWKS JWT auth takes precedence over static bearer tokens.
    if crate::auth::configure_jwt_auth_provider_from_env() {
        return true;
    }
    crate::auth::configure_grpc_auth_provider_from_env()
}

fn parse_etcd_endpoints_env() -> Vec<String> {
    env::var("KRISHIV_ETCD_ENDPOINTS")
        .ok()
        .map(|raw| parse_etcd_endpoints_list(&raw))
        .unwrap_or_default()
}

fn parse_etcd_endpoints_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

fn next_daemon_arg(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<String, Box<dyn Error>> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}").into())
}

/// Help text for coordinator-family daemons.
pub fn coordinator_daemon_help() -> &'static str {
    "Run a Krishiv coordinator or cluster control plane.\n\
     \n\
     Usage:\n\
       krishiv coordinator [OPTIONS]\n\
       krishiv clusterd [OPTIONS]\n\
     \n\
     Options:\n\
       --coordinator-id <ID>     Coordinator id (KRISHIV_COORDINATOR_ID, default coord-local)\n\
       --grpc-addr <HOST:PORT>     gRPC listen address (KRISHIV_GRPC_ADDR, default 0.0.0.0:9090)\n\
       --http-addr <HOST:PORT>     HTTP for /healthz /readyz /metrics /federation (optional)\n\
       --shuffle-dir <PATH>        Local shuffle store directory (optional)\n\
       --durability-profile <NAME> dev-local | single-node-durable | distributed-durable\n\
       --metadata-backend <TYPE>   memory | rocksdb | etcd\n\
       --metadata-path <PATH>      Durable metadata path (required for rocksdb)\n\
       --leader-backend <TYPE>     single (default) | etcd (clusterd HA; feature etcd)\n\
       --etcd-endpoints <HOSTS>    Comma-separated etcd URLs (KRISHIV_ETCD_ENDPOINTS)\n\
        --etcd-lease-key <KEY>      Leader key (default /krishiv/ccp/leader)\n\
        --leader-lease-secs <N>     etcd lease TTL seconds (default 15)\n\
        --insecure                  Allow anonymous gRPC (dev only; default: false)\n\
        --flight-addr <HOST:PORT>   Co-locate Arrow Flight SQL on this address (KRISHIV_FLIGHT_ADDR)\n\
        -h, --help                  Show help\n"
}

/// Standalone active coordinator (bare metal / VM).
///
/// `extra_sidecars` are additional co-located services to spawn alongside the
/// coordinator (e.g., a Flight SQL server). Each factory receives a
/// `SharedCoordinator` clone and returns a future that is spawned as a Tokio task.
pub async fn run_standalone_coordinator(
    config: CoordinatorDaemonConfig,
    extra_http_factory: Option<Box<dyn FnOnce(SharedCoordinator) -> Router + Send>>,
    extra_sidecars: Vec<CoordinatorSidecarFn>,
) -> Result<(), Box<dyn Error>> {
    let grpc_auth_configured = configure_coordinator_grpc_auth(&config);
    validate_runtime_security_config(
        &config,
        executor_task_bearer_token_configured(),
        coordinator_bearer_token_configured(),
    )?;
    let _auth_reload_task = if grpc_auth_configured {
        crate::auth::spawn_grpc_auth_reload_task_from_env()
    } else {
        None
    };
    let coordinator = build_shared_coordinator(&config)?;
    if config.durability_profile != DurabilityProfile::DevLocal {
        // Standalone coordinators must start with a monotonic fencing token.
        // A fresh SingleNodeLeader begins at 1; bump once so the token
        // advances across restarts and checkpoints from a prior run are not
        // rejected by validate_fencing_token_for_restore (A8).
        let leader = SingleNodeLeader::new();
        let token = leader.bump_fencing_token();
        coordinator.sync_leader_fencing_token(token);
    }
    spawn_coordinator_sidecars(&coordinator, &config, extra_http_factory, extra_sidecars).await?;
    // Standalone must spawn orchestration loops for task dispatch and heartbeat management.
    let _handles = coordinator.spawn_orchestration_loops();
    let listener = TcpListener::bind(config.grpc_addr).await?;
    tracing::info!(coordinator_id = %config.coordinator_id, addr = %listener.local_addr()?, "Krishiv coordinator gRPC listening");

    let in_flight = InFlightTracker::new();
    let tls_config = server_tls_config_from_env()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let grpc_serve = serve_coordinator_executor_grpc_with_listener_and_tracker(
        listener,
        coordinator.clone(),
        in_flight.clone(),
        tls_config,
    );

    tokio::select! {
        result = grpc_serve => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received; initiating standalone coordinator graceful shutdown");
        }
        _ = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sig) => { sig.recv().await; }
                Err(_) => std::future::pending::<()>().await,
            }
        } => {
            tracing::info!("SIGTERM received; initiating standalone coordinator graceful shutdown");
        }
    }

    // See the leader-mode shutdown path above (R11): drain in-flight RPCs
    // instead of sleeping a fixed window.
    if !in_flight.drain(Duration::from_secs(10)).await {
        tracing::warn!(
            active_calls = in_flight.active_count(),
            "RPC drain timed out with calls still in flight; proceeding with demotion"
        );
    }

    {
        let mut coord = coordinator.write().await;
        coord.demote_to_standby();
        tracing::info!("standalone coordinator demoted to standby on shutdown");
    }

    Ok(())
}

/// Cluster control plane daemon (`krishiv-clusterd`).
///
/// When `extra_http_factory` is provided, the returned routes are merged
/// into the coordinator's HTTP server.
///
/// `extra_sidecars` are additional co-located services to spawn alongside the
/// coordinator (e.g., a Flight SQL server).
pub async fn run_clusterd_daemon(
    config: CoordinatorDaemonConfig,
    extra_http_factory: Option<Box<dyn FnOnce(SharedCoordinator) -> Router + Send>>,
    extra_sidecars: Vec<CoordinatorSidecarFn>,
) -> Result<(), Box<dyn Error>> {
    let grpc_auth_configured = configure_coordinator_grpc_auth(&config);
    validate_runtime_security_config(
        &config,
        executor_task_bearer_token_configured(),
        coordinator_bearer_token_configured(),
    )?;
    let _auth_reload_task = if grpc_auth_configured {
        crate::auth::spawn_grpc_auth_reload_task_from_env()
    } else {
        None
    };
    let shared = build_shared_coordinator(&config)?;
    let leader = build_leader_election(&config).await?;
    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    let ccp = Arc::new(ClusterControlPlane::from_shared_with_leader(
        coordinator_id,
        shared.clone(),
        leader,
    ));
    spawn_coordinator_sidecars(&shared, &config, extra_http_factory, extra_sidecars).await?;
    let listener = TcpListener::bind(config.grpc_addr).await?;
    tracing::info!(coordinator_id = %config.coordinator_id, addr = %listener.local_addr()?, leader_backend = %config.leader_backend, "Krishiv clusterd (CCP) gRPC listening");
    run_cluster_control_plane(ccp, listener).await
}

/// Per-job coordinator process configuration.
///
/// The standalone JCP daemon is an HTTP **client** of the cluster control
/// plane.  It does NOT run independent orchestration loops or own a separate
/// `Coordinator` — A3 in the audit demonstrated that pattern produced a stuck
/// process because executors register with the CCP, not the JCP, so the JCP's
/// view of the world was always empty.
///
/// Instead, the JCP:
///   1. Submits the job to the CCP (if not already present) via the federation
///      HTTP endpoint.
///   2. Polls job status until it reaches a terminal state.
///   3. Exits with code 0 (Succeeded) / 1 (Failed) / 2 (Cancelled).
///
/// For Kubernetes `dedicatedCoordinator: true` deployments the per-job loops
/// continue to run inside the operator process via
/// [`crate::JobCoordinator::spawn_job_orchestration_loops`] which DOES share
/// the operator's `SharedCoordinator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCoordinatorDaemonConfig {
    pub job_id: String,
    /// Cluster control-plane HTTP base URL, e.g. `http://krishiv-clusterd:18080`.
    pub coordinator_http: String,
    /// How often to poll the CCP for job status.
    pub poll_interval: std::time::Duration,
    pub help: bool,
}

/// Parse `krishiv job-coordinator` flags.
pub fn parse_job_coordinator_daemon_config(
    args: impl IntoIterator<Item = String>,
) -> Result<JobCoordinatorDaemonConfig, Box<dyn Error>> {
    let mut config = JobCoordinatorDaemonConfig {
        job_id: env::var("KRISHIV_JOB_ID").unwrap_or_default(),
        coordinator_http: env::var("KRISHIV_COORDINATOR_HTTP")
            .unwrap_or_else(|_| String::from("http://127.0.0.1:18080")),
        poll_interval: std::time::Duration::from_secs(
            env::var("KRISHIV_JCP_POLL_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        ),
        help: false,
    };
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--job-id" => config.job_id = next_daemon_arg(&mut args, "--job-id")?,
            "--coordinator-http" => {
                config.coordinator_http = next_daemon_arg(&mut args, "--coordinator-http")?;
            }
            "--poll-interval-secs" => {
                let v = next_daemon_arg(&mut args, "--poll-interval-secs")?;
                let secs: u64 = v.parse().map_err(|_| "--poll-interval-secs must be u64")?;
                config.poll_interval = std::time::Duration::from_secs(secs.max(1));
            }
            "--help" | "-h" => config.help = true,
            unknown => {
                return Err(format!(
                    "unknown option: {unknown}\n\n{}",
                    job_coordinator_daemon_help()
                )
                .into());
            }
        }
    }
    Ok(config)
}

pub fn job_coordinator_daemon_help() -> &'static str {
    "Run a per-job coordinator (JCP) as a CCP client process.\n\
     \n\
     Usage:\n\
       krishiv job-coordinator --job-id <ID> [--coordinator-http <URL>]\n\
     \n\
     Options:\n\
       --job-id <ID>              Job id to watch (also KRISHIV_JOB_ID)\n\
       --coordinator-http <URL>   CCP federation HTTP endpoint (also KRISHIV_COORDINATOR_HTTP, default http://127.0.0.1:18080)\n\
       --poll-interval-secs <N>   Status poll interval (also KRISHIV_JCP_POLL_INTERVAL_SECS, default 2)\n\
     \n\
     Optional env KRISHIV_JOB_SPEC_JSON to submit the job on first connect.\n"
}

#[derive(serde::Deserialize)]
struct JcpJobStatusResponse {
    #[serde(default)]
    pub state: String,
}

/// Run the per-job coordinator loop as a CCP client (A3).
pub async fn run_job_coordinator_daemon(
    jcp_config: JobCoordinatorDaemonConfig,
) -> Result<(), Box<dyn Error>> {
    if jcp_config.job_id.is_empty() {
        return Err("--job-id is required".into());
    }
    if jcp_config.coordinator_http.is_empty() {
        return Err("--coordinator-http is required".into());
    }
    let job_id = jcp_config.job_id.clone();
    let base = jcp_config
        .coordinator_http
        .trim_end_matches('/')
        .to_string();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // First-time submit: if KRISHIV_JOB_SPEC_JSON is provided, submit through
    // the federation endpoint.  If the CCP already knows the job, the endpoint
    // returns BAD_REQUEST (DuplicateJob) which is fine.
    if let Ok(spec_json) = env::var("KRISHIV_JOB_SPEC_JSON") {
        let body = serde_json::json!({
            "job_id": job_id,
            "spec_json": spec_json,
        });
        let url = format!("{base}/federation/v1/jobs");
        let mut submit = client.post(&url).json(&body);
        if let Some(token) = configured_coordinator_bearer_token() {
            submit = submit.header("Authorization", format!("Bearer {token}"));
        }
        match submit.send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(job_id = %job_id, base = %base, "Krishiv JCP: submitted job");
            }
            Ok(resp) => {
                tracing::warn!(
                    status = %resp.status(),
                    "JCP submit returned non-success (already-submitted is typical)"
                );
            }
            Err(e) => {
                return Err(format!("submit job to {url}: {e}").into());
            }
        }
    }

    tracing::info!(job_id = %job_id, base = %base, poll_interval = ?jcp_config.poll_interval, "Krishiv JCP watching job");

    let status_url = format!("{base}/federation/v1/jobs/{}", urlencoding::encode(&job_id));
    loop {
        let mut status_req = client.get(&status_url);
        if let Some(token) = configured_coordinator_bearer_token() {
            status_req = status_req.header("Authorization", format!("Bearer {token}"));
        }
        match status_req.send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<JcpJobStatusResponse>().await {
                    Ok(status) => {
                        tracing::info!(job_id = %job_id, state = %status.state, "Krishiv JCP: job state");
                        let terminal =
                            matches!(status.state.as_str(), "Succeeded" | "Failed" | "Cancelled");
                        if terminal {
                            return match status.state.as_str() {
                                "Succeeded" => Ok(()),
                                "Cancelled" => Err("job cancelled".into()),
                                _ => Err("job failed".into()),
                            };
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "JCP failed to decode status payload");
                    }
                }
            }
            Ok(resp) => {
                tracing::warn!(status = %resp.status(), "JCP status RPC returned non-success");
            }
            Err(e) => {
                tracing::warn!(error = %e, "JCP status RPC failed; will retry");
            }
        }
        tokio::time::sleep(jcp_config.poll_interval).await;
    }
}

#[cfg(test)]
mod parse_tests {
    use super::{
        coordinator_daemon_help, parse_coordinator_daemon_config, render_metrics_body,
        validate_runtime_security_config,
    };
    use crate::{Coordinator, SharedCoordinator};
    use krishiv_common::durability::DurabilityProfile;
    use krishiv_proto::CoordinatorId;

    #[test]
    fn parses_defaults() {
        let config = parse_coordinator_daemon_config(std::iter::empty::<String>()).unwrap();
        assert_eq!(config.coordinator_id, "coord-local");
        assert_eq!(config.grpc_addr.port(), 9090);
        assert_eq!(config.durability_profile, DurabilityProfile::DevLocal);
        assert!(!config.help);
    }

    #[test]
    fn parses_help_flag() {
        let config = parse_coordinator_daemon_config([String::from("--help")]).unwrap();
        assert!(config.help);
    }

    #[test]
    fn parses_etcd_leader_flags() {
        let config = parse_coordinator_daemon_config([
            String::from("--leader-backend"),
            String::from("etcd"),
            String::from("--etcd-endpoints"),
            String::from("http://127.0.0.1:2379,http://127.0.0.2:2379"),
            String::from("--etcd-lease-key"),
            String::from("/krishiv/test/leader"),
            String::from("--leader-lease-secs"),
            String::from("30"),
        ])
        .unwrap();
        assert_eq!(config.leader_backend, "etcd");
        assert_eq!(config.etcd_endpoints.len(), 2);
        assert_eq!(config.etcd_lease_key, "/krishiv/test/leader");
        assert_eq!(config.leader_lease_duration_s, 30);
    }

    #[test]
    fn rejects_etcd_backend_without_endpoints() {
        let err = parse_coordinator_daemon_config([
            String::from("--leader-backend"),
            String::from("etcd"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("etcd-endpoints"));
    }

    #[test]
    fn parses_single_node_durable_profile_with_required_local_storage() {
        // single-node-durable requires redb (not json) for crash-recovery.
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("single-node-durable"),
            String::from("--metadata-backend"),
            String::from("redb"),
            String::from("--metadata-path"),
            String::from("/tmp/krishiv-metadata.redb"),
            String::from("--shuffle-dir"),
            String::from("/tmp/krishiv-shuffle"),
        ])
        .unwrap();

        assert_eq!(
            config.durability_profile,
            DurabilityProfile::SingleNodeDurable
        );
        assert_eq!(config.metadata_backend.as_deref(), Some("redb"));
        assert!(config.metadata_path.is_some());
        assert!(config.shuffle_dir.is_some());
    }

    #[test]
    fn rejects_single_node_durable_with_json_metadata_backend() {
        // json was removed because it silently loses state on restart.
        let err = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("single-node-durable"),
            String::from("--metadata-backend"),
            String::from("json"),
            String::from("--metadata-path"),
            String::from("/tmp/krishiv-metadata.json"),
            String::from("--shuffle-dir"),
            String::from("/tmp/krishiv-shuffle"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("redb"), "error: {err}");
    }

    #[test]
    fn rejects_single_node_durable_without_metadata_path() {
        let err = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("single-node-durable"),
            String::from("--metadata-backend"),
            String::from("redb"),
            String::from("--shuffle-dir"),
            String::from("/tmp/krishiv-shuffle"),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("metadata-path"), "error: {err}");
    }

    #[test]
    fn parses_distributed_durable_profile_with_etcd_fencing() {
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("distributed-durable"),
            String::from("--metadata-backend"),
            String::from("etcd"),
            String::from("--leader-backend"),
            String::from("etcd"),
            String::from("--etcd-endpoints"),
            String::from("http://127.0.0.1:2379"),
        ])
        .unwrap();

        assert_eq!(
            config.durability_profile,
            DurabilityProfile::DistributedDurable
        );
        assert_eq!(config.metadata_backend.as_deref(), Some("etcd"));
        assert_eq!(config.leader_backend, "etcd");
        assert_eq!(config.etcd_endpoints, vec!["http://127.0.0.1:2379"]);
    }

    #[test]
    fn single_node_durable_runtime_requires_coordinator_token() {
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("single-node-durable"),
            String::from("--metadata-backend"),
            String::from("redb"),
            String::from("--metadata-path"),
            String::from("/tmp/krishiv-meta"),
            String::from("--shuffle-dir"),
            String::from("/tmp/krishiv-shuffle"),
        ])
        .unwrap();

        let error = validate_runtime_security_config(&config, false, false).unwrap_err();

        assert!(error.to_string().contains("single-node-durable"));
        assert!(
            error
                .to_string()
                .contains("KRISHIV_COORDINATOR_BEARER_TOKEN")
        );
    }

    #[test]
    fn distributed_durable_runtime_requires_coordinator_token() {
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("distributed-durable"),
            String::from("--metadata-backend"),
            String::from("etcd"),
            String::from("--leader-backend"),
            String::from("etcd"),
            String::from("--etcd-endpoints"),
            String::from("http://127.0.0.1:2379"),
        ])
        .unwrap();

        let error = validate_runtime_security_config(&config, true, false).unwrap_err();

        assert!(error.to_string().contains("distributed-durable"));
        assert!(
            error
                .to_string()
                .contains("KRISHIV_COORDINATOR_BEARER_TOKEN")
        );
    }

    #[test]
    fn distributed_durable_runtime_requires_executor_task_token() {
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("distributed-durable"),
            String::from("--metadata-backend"),
            String::from("etcd"),
            String::from("--leader-backend"),
            String::from("etcd"),
            String::from("--etcd-endpoints"),
            String::from("http://127.0.0.1:2379"),
        ])
        .unwrap();

        let error = validate_runtime_security_config(&config, false, true).unwrap_err();

        assert!(error.to_string().contains("distributed-durable"));
        assert!(
            error
                .to_string()
                .contains("KRISHIV_EXECUTOR_TASK_BEARER_TOKEN")
        );
    }

    #[test]
    fn distributed_durable_runtime_rejects_insecure_grpc() {
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("distributed-durable"),
            String::from("--metadata-backend"),
            String::from("etcd"),
            String::from("--leader-backend"),
            String::from("etcd"),
            String::from("--etcd-endpoints"),
            String::from("http://127.0.0.1:2379"),
            String::from("--insecure"),
        ])
        .unwrap();

        let error = validate_runtime_security_config(&config, true, true).unwrap_err();

        assert!(error.to_string().contains("--insecure"));
    }

    #[test]
    fn distributed_durable_runtime_accepts_required_tokens() {
        let config = parse_coordinator_daemon_config([
            String::from("--durability-profile"),
            String::from("distributed-durable"),
            String::from("--metadata-backend"),
            String::from("etcd"),
            String::from("--leader-backend"),
            String::from("etcd"),
            String::from("--etcd-endpoints"),
            String::from("http://127.0.0.1:2379"),
        ])
        .unwrap();

        validate_runtime_security_config(&config, true, true).unwrap();
    }

    #[test]
    fn daemon_help_lists_rocksdb_metadata_backend() {
        let help = coordinator_daemon_help();

        assert!(help.contains("memory | rocksdb | etcd"));
        assert!(!help.contains("memory | redb | etcd"));
        assert!(!help.contains("sqlite | etcd"));
    }

    #[tokio::test]
    async fn metrics_body_includes_scheduler_counters() {
        let coordinator = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-metrics").unwrap(),
        ));
        let body = render_metrics_body(&coordinator).await;
        assert!(body.contains("krishiv_scheduler_jobs_submitted_total"));
        assert!(body.contains("krishiv_scheduler_checkpoint_epochs_total"));
        assert!(body.contains("krishiv_scheduler_tasks_assigned_total"));
    }

    #[tokio::test]
    async fn circuit_breaker_reset_endpoint_returns_ok() {
        use crate::CoordinatorDaemonConfig;
        use crate::coordinator_http_router;
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use krishiv_proto::{ExecutorDescriptor, ExecutorId};
        use tower::ServiceExt;

        let _ = crate::auth::set_allow_anonymous();

        let coordinator = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-cb-reset").unwrap(),
        ));

        let exec_id = ExecutorId::try_new("exec-cb-reset").unwrap();
        let descriptor = ExecutorDescriptor::new(exec_id.clone(), "localhost", 4)
            .with_task_endpoint(crate::IN_PROCESS_TASK_ENDPOINT);
        coordinator
            .write()
            .await
            .register_executor(descriptor)
            .unwrap();

        // Simulate consecutive task failures so the circuit breaker has state to reset.
        let threshold = coordinator
            .read()
            .await
            .config()
            .circuit_breaker_failure_threshold();
        for _ in 0..threshold {
            coordinator
                .write()
                .await
                .executors
                .record_task_failure(&exec_id, threshold);
        }

        let config = CoordinatorDaemonConfig::http_sidecar(DurabilityProfile::DevLocal);
        let router = coordinator_http_router(coordinator.clone(), &config);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/executors/{exec_id}/reset"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["reset"], serde_json::json!(true));
        assert_eq!(json["executor_id"], serde_json::json!(exec_id.to_string()));

        // Verify the failure counter was actually reset.
        let failures = coordinator
            .read()
            .await
            .executor_snapshots()
            .into_iter()
            .find(|s| s.executor_id() == &exec_id)
            .map(|s| s.consecutive_task_failures)
            .unwrap_or(1);
        assert_eq!(
            failures, 0,
            "circuit breaker counter must be zero after reset"
        );
    }

    /// Regression (Wave 4 — Observability & Shutdown): `readyz` must return
    /// 503 when the coordinator is `Active` but has no executor that
    /// `can_accept_work` (it previously only checked coordinator state,
    /// reporting "ready" even though no work could actually be scheduled),
    /// and 200 once a healthy executor is registered.
    #[tokio::test]
    async fn readyz_requires_a_healthy_executor() {
        use crate::CoordinatorDaemonConfig;
        use crate::coordinator_http_router;
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use krishiv_proto::{ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState};
        use tower::ServiceExt;

        let _ = crate::auth::set_allow_anonymous();

        let coordinator = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-readyz").unwrap(),
        ));
        let config = CoordinatorDaemonConfig::http_sidecar(DurabilityProfile::DevLocal);

        // No executors registered yet — coordinator is Active but cannot
        // accept work.
        let router = coordinator_http_router(coordinator.clone(), &config);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "readyz must report unavailable when no healthy executors are registered"
        );

        // Register and heartbeat an executor into a healthy state.
        let exec_id = ExecutorId::try_new("exec-readyz").unwrap();
        {
            let mut c = coordinator.write().await;
            c.register_executor(ExecutorDescriptor::new(exec_id.clone(), "localhost", 4))
                .unwrap();
            c.executor_heartbeat(ExecutorHeartbeat::new(exec_id, ExecutorState::Healthy))
                .unwrap();
        }

        let router = coordinator_http_router(coordinator.clone(), &config);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "readyz must report ready once a healthy executor can accept work"
        );
    }
}
