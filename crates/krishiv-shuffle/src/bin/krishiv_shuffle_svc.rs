//! Optional shuffle service — serves local disk partitions over HTTP (WS-6.7).

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use krishiv_shuffle::{LocalDiskShuffleStore, PartitionId, ShuffleCompression, ShuffleStore};
use tokio::net::TcpListener;

#[derive(Clone)]
struct ShuffleSvcState {
    store: Arc<LocalDiskShuffleStore>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = env::var("KRISHIV_SHUFFLE_DIR").unwrap_or_else(|_| "/tmp/krishiv-shuffle".into());
    let addr: SocketAddr = env::var("KRISHIV_SHUFFLE_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:7072".into())
        .parse()?;
    let store = Arc::new(
        LocalDiskShuffleStore::new(&base_dir)?
            .with_compression(ShuffleCompression::Lz4),
    );
    let state = ShuffleSvcState { store };
    let app = Router::new()
        .route(
            "/shuffle/{job_id}/{stage_id}/{partition}",
            get(read_partition),
        )
        .route("/healthz", get(|| async { "ok\n" }))
        .with_state(state);
    let listener = TcpListener::bind(addr).await?;
    println!(
        "krishiv-shuffle-svc listening on {} (dir={})",
        listener.local_addr()?,
        base_dir
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn read_partition(
    State(state): State<ShuffleSvcState>,
    Path((job_id, stage_id, partition)): Path<(String, String, u32)>,
) -> Result<impl IntoResponse, StatusCode> {
    let id = PartitionId {
        job_id,
        stage_id,
        partition,
    };
    let part = state
        .store
        .read_partition(&id)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let rows: usize = part.batches.iter().map(|b| b.num_rows()).sum();
    Ok(format!("partition {} rows={rows}\n", id.partition))
}
