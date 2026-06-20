//! Optional HTTP shuffle service (`krishiv shuffle-svc`).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::{LocalDiskShuffleStore, PartitionId, ShuffleCompression, ShuffleStore};
use axum::Router;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use constant_time_eq::constant_time_eq;
use tokio::net::TcpListener;

#[derive(Clone)]
pub(crate) struct ShuffleSvcState {
    pub(crate) store: Arc<LocalDiskShuffleStore>,
    pub(crate) token: Option<String>,
}

/// Run the shuffle HTTP service (env `KRISHIV_SHUFFLE_DIR`, `KRISHIV_SHUFFLE_ADDR`).
pub async fn run_shuffle_svc_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_dir = std::env::var("KRISHIV_SHUFFLE_DIR")
        .unwrap_or_else(|_| String::from("/tmp/krishiv-shuffle"));
    let addr: SocketAddr = std::env::var("KRISHIV_SHUFFLE_ADDR")
        .unwrap_or_else(|_| String::from("0.0.0.0:2004"))
        .parse()?;
    run_shuffle_svc(&base_dir, addr).await
}

/// Run the shuffle HTTP service on `addr` with data under `base_dir`.
pub async fn run_shuffle_svc(
    base_dir: impl AsRef<Path>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store = Arc::new(
        LocalDiskShuffleStore::new(base_dir.as_ref())?.with_compression(ShuffleCompression::Lz4),
    );
    let token = std::env::var("KRISHIV_SHUFFLE_TOKEN").ok();
    let state = ShuffleSvcState { store, token };
    let app = Router::new()
        .route(
            "/shuffle/{job_id}/{stage_id}/{partition}",
            get(read_partition),
        )
        .route("/healthz", get(|| async { "ok\n" }))
        .with_state(state);
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        addr = %listener.local_addr()?,
        dir = %base_dir.as_ref().display(),
        "krishiv-shuffle-svc listening"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

pub(crate) async fn read_partition(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath((job_id, stage_id, partition)): AxumPath<(String, String, u32)>,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(token) = &state.token {
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let expected = format!("Bearer {token}");
        if !constant_time_eq(auth_header.as_bytes(), expected.as_bytes()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    let id = PartitionId {
        job_id,
        stage_id,
        partition,
    };
    let part = state
        .store
        .read_partition(&id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "read_partition failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    let rows: usize = part.batches.iter().map(|b| b.num_rows()).sum();
    Ok(format!("partition {} rows={rows}\n", id.partition))
}
