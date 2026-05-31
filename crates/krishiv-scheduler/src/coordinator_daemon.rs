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
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use krishiv_proto::{CoordinatorId, CoordinatorState};
use krishiv_shuffle::{LocalDiskShuffleStore, ShuffleStore as _};
use tokio::net::TcpListener;
use tokio::time::{Duration, interval};

#[cfg(feature = "sqlite")]
use crate::SqliteMetadataStore;
use crate::{
    ClusterControlPlane, Coordinator, InMemoryMetadataStore, JsonFileMetadataStore, LeaderElection,
    SharedCoordinator, SingleNodeLeader, scheduler_metrics,
    serve_coordinator_executor_grpc_with_listener,
};

#[cfg(feature = "etcd")]
use crate::{EtcdLeaseElection, EtcdMetadataStore};

/// CLI configuration for coordinator-family binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDaemonConfig {
    pub coordinator_id: String,
    pub grpc_addr: SocketAddr,
    pub http_addr: Option<SocketAddr>,
    pub shuffle_dir: Option<PathBuf>,
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
    pub help: bool,
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
pub fn build_shared_coordinator_sync(
    config: &CoordinatorDaemonConfig,
) -> Result<SharedCoordinator, Box<dyn Error>> {
    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    let mut coord = Coordinator::active(coordinator_id);
    let coordinator = match (config.metadata_backend.as_deref(), &config.metadata_path) {
        (Some("memory"), _) | (None, None) => {
            SharedCoordinator::new(coord.with_store(InMemoryMetadataStore::default()))
        }
        #[cfg(feature = "etcd")]
        (Some("etcd"), _) => {
            if config.etcd_endpoints.is_empty() {
                return Err(
                    "--metadata-backend etcd requires --etcd-endpoints (or KRISHIV_ETCD_ENDPOINTS)"
                        .into(),
                );
            }
            let store = krishiv_async_util::block_on(EtcdMetadataStore::connect(
                config.etcd_endpoints.clone(),
            ))
            .map_err(|e| format!("etcd metadata store: {e}"))?;
            coord
                .recover_from_store(&store)
                .map_err(|e| format!("coordinator recovery failed: {e}"))?;
            SharedCoordinator::new(coord.with_store(store))
        }
        #[cfg(not(feature = "etcd"))]
        (Some("etcd"), _) => {
            return Err(
                "etcd metadata requires building krishiv-scheduler with feature `etcd`".into(),
            );
        }
        (backend, Some(path)) => {
            let path = path.to_string_lossy();
            match backend.unwrap_or("json") {
                #[cfg(feature = "sqlite")]
                "sqlite" => {
                    let store = SqliteMetadataStore::open(path.as_ref())
                        .map_err(|e| format!("sqlite store '{path}': {e}"))?;
                    coord
                        .recover_from_store(&store)
                        .map_err(|e| format!("coordinator recovery failed: {e}"))?;
                    SharedCoordinator::new(coord.with_store(store))
                }
                _ => {
                    let store = JsonFileMetadataStore::open(path.as_ref())
                        .map_err(|e| format!("json store '{path}': {e}"))?;
                    coord
                        .recover_from_store(&store)
                        .map_err(|e| format!("coordinator recovery failed: {e}"))?;
                    SharedCoordinator::new(coord.with_store(store))
                }
            }
        }
        (Some("sqlite"), None) => {
            return Err("--metadata-backend sqlite requires --metadata-path".into());
        }
        (Some("json"), None) => {
            return Err("--metadata-backend json requires --metadata-path".into());
        }
        (Some(unknown), _) => {
            return Err(format!(
                "unknown --metadata-backend '{unknown}'; supported: memory, json, sqlite, etcd"
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
    // Store handles so the orchestration loops live until this function returns.
    let _orchestrator_handles = ccp.spawn_orchestration_loops();
    let leader = Arc::clone(ccp.leader());
    let ccp_loop = Arc::clone(&ccp);
    tokio::spawn(async move {
        ccp_loop.run_leader_loop().await;
    });
    let _ = leader;

    let coordinator = ccp.shared_coordinator().clone();
    let grpc_serve = serve_coordinator_executor_grpc_with_listener(listener, coordinator.clone());

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

    // Demote coordinator and release leadership.
    {
        let mut coord = coordinator.write().await;
        coord.demote_to_standby();
        tracing::info!("coordinator demoted to standby on shutdown");
    }

    Ok(())
}

/// Spawn shuffle GC and HTTP/metrics when configured.
pub async fn spawn_coordinator_sidecars(
    coordinator: &SharedCoordinator,
    config: &CoordinatorDaemonConfig,
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
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let job_ids = gc_coordinator.write().await.take_gc_ready_jobs();
                for job_id in job_ids {
                    if let Err(e) = store.delete_job_partitions(job_id.as_str()).await {
                        eprintln!("shuffle GC failed for job {job_id}: {e}");
                    }
                }
            }
        });
    }

    if let Some(http_addr) = config.http_addr {
        let http_coordinator = coordinator.clone();
        let http_listener = TcpListener::bind(http_addr).await?;
        println!(
            "Krishiv coordinator HTTP listening on {}",
            http_listener.local_addr()?
        );
        tokio::spawn(async move {
            let router = coordinator_http_router(http_coordinator);
            let _ = axum::serve(http_listener, router).await;
        });
    }

    let tick_coordinator = coordinator.clone();
    let tick_period_ms = {
        let coord = tick_coordinator.read().await;
        coord.config().tick_period_ms()
    };
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(tick_period_ms));
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = tick_coordinator.wait_for_change() => {}
            }

            let mut coord = tick_coordinator.write().await;
            if let Err(e) = coord.coordinator_tick() {
                tracing::warn!(error = %e, "coordinator tick failed");
            }
        }
    });

    Ok(())
}

pub fn coordinator_http_router(coordinator: SharedCoordinator) -> Router {
    use crate::batch_sql_http::api_batch_sql;
    use crate::federation_http::{
        federation_cancel_job, federation_job_status, federation_submit_job,
    };
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route(
            "/",
            get(|| async { axum::response::Redirect::temporary("/ui") }),
        )
        .route("/ui", get(live_ui))
        .route("/api/v1/jobs", get(api_jobs))
        .route("/api/v1/executors", get(api_executors))
        .route("/api/v1/batch-sql", post(api_batch_sql))
        .route("/federation/v1/jobs", post(federation_submit_job))
        .route("/federation/v1/jobs/{job_id}", get(federation_job_status))
        .route(
            "/federation/v1/jobs/{job_id}/cancel",
            post(federation_cancel_job),
        )
        .with_state(coordinator)
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
                }
            })
            .collect::<Vec<_>>()
    };
    Json(LiveExecutorsResponse { executors })
}

async fn live_ui(State(coordinator): State<SharedCoordinator>) -> impl IntoResponse {
    let coord = coordinator.read().await;
    let (state, jobs, executors) = (
        format!("{:?}", coord.state()),
        coord.job_snapshots(),
        coord.executor_snapshots(),
    );

    let mut body = String::from(
        "<!doctype html><html><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>Krishiv Cluster</title>\
         <style>body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:32px;color:#17202a}\
         table{border-collapse:collapse;width:100%;margin:16px 0 28px}th,td{border-bottom:1px solid #d8dee4;padding:8px;text-align:left}\
         th{background:#f6f8fa}.meta{color:#57606a}.ok{color:#116329;font-weight:600}</style></head><body>",
    );
    body.push_str("<h1>Krishiv Cluster</h1>");
    body.push_str(&format!(
        "<p class=\"meta\">Coordinator state: <span class=\"ok\">{state}</span></p>"
    ));
    body.push_str("<h2>Executors</h2><table><thead><tr><th>Executor</th><th>Host</th><th>Slots</th><th>State</th><th>Running Tasks</th><th>Lease</th><th>Last Heartbeat Tick</th></tr></thead><tbody>");
    if executors.is_empty() {
        body.push_str("<tr><td colspan=\"7\">No executors registered.</td></tr>");
    } else {
        for record in executors {
            let descriptor = record.descriptor();
            body.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                record.executor_id(),
                descriptor.host(),
                descriptor.slots(),
                record.state(),
                record.running_tasks().len(),
                record.lease_generation().as_u64(),
                record.last_heartbeat_tick(),
            ));
        }
    }
    body.push_str("</tbody></table>");
    body.push_str("<h2>Jobs</h2><table><thead><tr><th>Job</th><th>Kind</th><th>State</th><th>Stages</th><th>Tasks</th><th>Assigned</th><th>Running</th><th>Succeeded</th><th>Failed</th></tr></thead><tbody>");
    if jobs.is_empty() {
        body.push_str("<tr><td colspan=\"9\">No jobs submitted yet.</td></tr>");
    } else {
        for job in jobs {
            body.push_str(&format!(
                "<tr><td>{}</td><td>{:?}</td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                job.job_id(),
                job.kind(),
                job.state(),
                job.stage_count(),
                job.task_count(),
                job.assigned_task_count(),
                job.running_task_count(),
                job.succeeded_task_count(),
                job.failed_task_count(),
            ));
        }
    }
    body.push_str("</tbody></table><p class=\"meta\">JSON: <a href=\"/api/v1/jobs\">jobs</a> · <a href=\"/api/v1/executors\">executors</a> · <a href=\"/metrics\">metrics</a></p></body></html>");
    Html(body)
}

async fn readyz(
    State(coordinator): State<SharedCoordinator>,
) -> Result<&'static str, (axum::http::StatusCode, String)> {
    let c = coordinator.read().await;
    if c.state() == CoordinatorState::Active {
        Ok("ready\n")
    } else {
        Err((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "coordinator is not active\n".to_owned(),
        ))
    }
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
",
        running = m.running_task_count(),
        retries = m.retry_count(),
        failed = m.failed_assignments(),
        hb_age = max_hb_age,
    );
    let mut body = body;
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
    Ok(config)
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
       --metadata-backend <TYPE>   memory | json | sqlite | etcd\n\
       --metadata-path <PATH>      Durable metadata path (required for json/sqlite)\n\
       --leader-backend <TYPE>     single (default) | etcd (clusterd HA; feature etcd)\n\
       --etcd-endpoints <HOSTS>    Comma-separated etcd URLs (KRISHIV_ETCD_ENDPOINTS)\n\
        --etcd-lease-key <KEY>      Leader key (default /krishiv/ccp/leader)\n\
        --leader-lease-secs <N>     etcd lease TTL seconds (default 15)\n\
        --insecure                  Allow anonymous gRPC (dev only; default: false)\n\
        -h, --help                  Show help\n"
}

/// Standalone active coordinator (bare metal / VM).
pub async fn run_standalone_coordinator(
    config: CoordinatorDaemonConfig,
) -> Result<(), Box<dyn Error>> {
    if config.insecure {
        crate::auth::set_allow_anonymous();
    }
    let coordinator = build_shared_coordinator(&config)?;
    spawn_coordinator_sidecars(&coordinator, &config).await?;
    let listener = TcpListener::bind(config.grpc_addr).await?;
    println!(
        "Krishiv coordinator {} gRPC listening on {}",
        config.coordinator_id,
        listener.local_addr()?
    );

    let grpc_serve = serve_coordinator_executor_grpc_with_listener(listener, coordinator.clone());

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

    {
        let mut coord = coordinator.write().await;
        coord.demote_to_standby();
        tracing::info!("standalone coordinator demoted to standby on shutdown");
    }

    Ok(())
}

/// Cluster control plane daemon (`krishiv-clusterd`).
pub async fn run_clusterd_daemon(config: CoordinatorDaemonConfig) -> Result<(), Box<dyn Error>> {
    if config.insecure {
        crate::auth::set_allow_anonymous();
    }
    let shared = build_shared_coordinator(&config)?;
    let leader = build_leader_election(&config).await?;
    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    let ccp = Arc::new(ClusterControlPlane::from_shared_with_leader(
        coordinator_id,
        shared.clone(),
        leader,
    ));
    spawn_coordinator_sidecars(&shared, &config).await?;
    let listener = TcpListener::bind(config.grpc_addr).await?;
    println!(
        "Krishiv clusterd (CCP) {} gRPC listening on {} (leader-backend={})",
        config.coordinator_id,
        listener.local_addr()?,
        config.leader_backend,
    );
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
        match client.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                println!("Krishiv JCP: submitted job {job_id} to {base}");
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

    println!(
        "Krishiv JCP watching job {job_id} on {base} (poll every {:?})",
        jcp_config.poll_interval
    );

    let status_url = format!("{base}/federation/v1/jobs/{job_id}");
    loop {
        match client.get(&status_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<JcpJobStatusResponse>().await {
                    Ok(status) => {
                        println!("Krishiv JCP: job {job_id} state={}", status.state);
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
    use super::{parse_coordinator_daemon_config, render_metrics_body};
    use crate::{Coordinator, SharedCoordinator};
    use krishiv_proto::CoordinatorId;

    #[test]
    fn parses_defaults() {
        let config = parse_coordinator_daemon_config(std::iter::empty::<String>()).unwrap();
        assert_eq!(config.coordinator_id, "coord-local");
        assert_eq!(config.grpc_addr.port(), 9090);
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
}
