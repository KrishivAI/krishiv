//! Shared coordinator / clusterd startup (bare metal + VM).

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
use krishiv_shuffle::{LocalDiskShuffleStore, ShuffleStore as _};
use tokio::net::TcpListener;
use tokio::time::{Duration, interval};

use crate::{
    ClusterControlPlane, Coordinator, InMemoryMetadataStore, JsonFileMetadataStore,
    SharedCoordinator, StabilityMetrics, serve_coordinator_executor_grpc_with_listener,
};
#[cfg(feature = "sqlite")]
use crate::SqliteMetadataStore;

/// CLI configuration for coordinator-family binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDaemonConfig {
    pub coordinator_id: String,
    pub grpc_addr: SocketAddr,
    pub http_addr: Option<SocketAddr>,
    pub shuffle_dir: Option<PathBuf>,
    pub metadata_backend: Option<String>,
    pub metadata_path: Option<PathBuf>,
    pub help: bool,
}

/// Build a shared coordinator from daemon configuration.
pub fn build_shared_coordinator(
    config: &CoordinatorDaemonConfig,
) -> Result<SharedCoordinator, Box<dyn Error>> {
    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    let mut coord = Coordinator::active(coordinator_id);
    let coordinator = match (config.metadata_backend.as_deref(), &config.metadata_path) {
        (Some("memory"), _) | (None, None) => {
            SharedCoordinator::new(coord.with_store(InMemoryMetadataStore::default()))
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
                "unknown --metadata-backend '{unknown}'; supported: memory, json, sqlite"
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
    ccp.spawn_orchestration_loops();
    let leader = Arc::clone(ccp.leader());
    let ccp_loop = Arc::clone(&ccp);
    tokio::spawn(async move {
        ccp_loop.run_leader_loop().await;
    });
    let _ = leader;
    serve_coordinator_executor_grpc_with_listener(listener, ccp.shared_coordinator().clone()).await?;
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
    let tick_period_ms = tick_coordinator
        .read()
        .map(|c| c.config().tick_period_ms())
        .unwrap_or(1_000);
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(tick_period_ms));
        loop {
            ticker.tick().await;
            if let Ok(mut coord) = tick_coordinator.write() {
                if let Err(e) = coord.coordinator_tick() {
                    tracing::warn!(error = %e, "coordinator tick failed");
                }
            }
        }
    });

    Ok(())
}

pub fn coordinator_http_router(coordinator: SharedCoordinator) -> Router {
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
",
        running = m.running_task_count(),
        retries = m.retry_count(),
        failed = m.failed_assignments(),
        hb_age = max_hb_age,
    );
    let mut body = body;
    body.push('\n');
    body.push_str(&krishiv_metrics::global_metrics().render_prometheus());
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}
