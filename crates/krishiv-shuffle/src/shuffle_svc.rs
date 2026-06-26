//! Optional HTTP shuffle service (`krishiv shuffle-svc`).
//!
//! Three independent serving paths:
//!
//! * `/shuffle/<job>/<stage>/<p>` — classic per-partition Arrow IPC, backed by
//!   any `ShuffleStore` implementation.
//!
//! * `/ess/<job>/<stage>/<p>` — External Shuffle Service (ESS) path that reads
//!   directly from the index+data files produced by [`SortShuffleWriter`]. The
//!   ESS index is a shared [`SortShuffleIndex`] populated by the executor after
//!   a sort-shuffle write task completes.
//!
//! * `/ess/push/<job>/<stage>/<task>/<p>` (POST) — T12 push-based shuffle:
//!   map tasks push their per-partition Arrow IPC payloads directly to the ESS.
//!   `/ess/merged/<job>/<stage>/<p>` (GET) returns the concatenated stream from
//!   all tasks that pushed for that partition, eliminating per-task connections
//!   in the reduce fetch phase.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use constant_time_eq::constant_time_eq;
use dashmap::DashMap;
use tokio::net::TcpListener;

use crate::push_shuffle::PushShuffleStore;
use crate::sort_shuffle_writer::SortShuffleFiles;
use crate::{LocalDiskShuffleStore, PartitionId, ShuffleCompression, ShuffleStore};

// ── ESS index ────────────────────────────────────────────────────────────────

/// Thread-safe registry mapping `(job_id, stage_id)` to their
/// [`SortShuffleFiles`].  Shared between the executor write path and the ESS
/// HTTP handlers so no inter-process registration RPC is needed on a single
/// node.
#[derive(Default, Clone)]
pub struct SortShuffleIndex {
    inner: Arc<DashMap<(String, String), SortShuffleFiles>>,
}

impl SortShuffleIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register sort-shuffle output files for `(job_id, stage_id)`.
    pub fn register(&self, job_id: &str, stage_id: &str, files: SortShuffleFiles) {
        self.inner
            .insert((job_id.to_owned(), stage_id.to_owned()), files);
    }

    /// Look up files for `(job_id, stage_id)`.
    pub fn get(&self, job_id: &str, stage_id: &str) -> Option<SortShuffleFiles> {
        self.inner
            .get(&(job_id.to_owned(), stage_id.to_owned()))
            .map(|e| e.clone())
    }

    /// Remove all entries for a completed or cancelled job.
    pub fn remove_job(&self, job_id: &str) {
        self.inner.retain(|(jid, _), _| jid.as_str() != job_id);
    }
}

// ── HTTP service state ────────────────────────────────────────────────────────

// A3: Use a trait object so the HTTP shuffle service can be backed by any
// ShuffleStore implementation, not just LocalDiskShuffleStore.
#[derive(Clone)]
pub(crate) struct ShuffleSvcState {
    pub(crate) store: Arc<dyn ShuffleStore + Send + Sync>,
    pub(crate) ess_index: SortShuffleIndex,
    /// T12: push-based shuffle store — accumulates per-partition IPC payloads
    /// pushed by map tasks, served merged via `/ess/merged/…`.
    pub(crate) push_store: PushShuffleStore,
    pub(crate) token: Option<String>,
}

// ── Service launch ────────────────────────────────────────────────────────────

/// Run the shuffle HTTP service (env `KRISHIV_SHUFFLE_DIR`, `KRISHIV_SHUFFLE_ADDR`).
pub async fn run_shuffle_svc_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_dir = std::env::var("KRISHIV_SHUFFLE_DIR")
        .unwrap_or_else(|_| String::from("/tmp/krishiv-shuffle"));
    let addr: SocketAddr = std::env::var("KRISHIV_SHUFFLE_ADDR")
        .unwrap_or_else(|_| String::from("0.0.0.0:7072"))
        .parse()?;
    run_shuffle_svc(&base_dir, addr).await
}

/// Run the shuffle HTTP service on `addr` with data under `base_dir`.
///
/// Returns the [`SortShuffleIndex`] shared with ESS endpoints so callers
/// (the executor) can register sort-shuffle files without an extra RPC.
pub async fn run_shuffle_svc(
    base_dir: impl AsRef<Path>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store: Arc<dyn ShuffleStore + Send + Sync> = Arc::new(
        LocalDiskShuffleStore::new(base_dir.as_ref())?.with_compression(ShuffleCompression::Lz4),
    );
    let token = std::env::var("KRISHIV_SHUFFLE_TOKEN").ok();
    let ess_index = SortShuffleIndex::new();
    let state = ShuffleSvcState {
        store,
        ess_index,
        push_store: PushShuffleStore::new(),
        token,
    };
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        addr = %listener.local_addr()?,
        dir = %base_dir.as_ref().display(),
        "krishiv-shuffle-svc listening"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Build the Axum router (extracted for testability).
pub(crate) fn build_router(state: ShuffleSvcState) -> Router {
    Router::new()
        .route(
            "/shuffle/{job_id}/{stage_id}/{partition}",
            get(read_partition),
        )
        // ESS: serve a partition from the sort-shuffle index+data files.
        .route(
            "/ess/{job_id}/{stage_id}/{partition}",
            get(ess_read_partition),
        )
        // ESS: remove all index entries for a completed job (GC).
        .route("/ess/gc/{job_id}", post(ess_gc_job))
        // T12 push-based shuffle: map tasks POST their IPC payloads here.
        .route(
            "/ess/push/{job_id}/{stage_id}/{task_id}/{partition}",
            post(ess_push_partition),
        )
        // T12 push-based shuffle: reduce tasks GET the merged IPC stream here.
        .route(
            "/ess/merged/{job_id}/{stage_id}/{partition}",
            get(ess_merged_read),
        )
        // T12 push-based shuffle: GC push store for a job.
        .route("/ess/push-gc/{job_id}", post(ess_push_gc))
        .route("/healthz", get(|| async { "ok\n" }))
        .with_state(state)
}

// ── Classic shuffle handler ───────────────────────────────────────────────────

pub(crate) async fn read_partition(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath((job_id, stage_id, partition)): AxumPath<(String, String, u32)>,
) -> Result<impl IntoResponse, StatusCode> {
    check_bearer_token(&headers, &state.token)?;

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

    // B1: Serialize batches to Arrow IPC stream bytes and return them with
    // the correct Content-Type header, instead of a text summary.
    use arrow::ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &part.schema).map_err(|e| {
            tracing::error!(error = %e, "IPC StreamWriter creation failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        for batch in &part.batches {
            writer.write(batch).map_err(|e| {
                tracing::error!(error = %e, "IPC batch write failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }
        writer.finish().map_err(|e| {
            tracing::error!(error = %e, "IPC StreamWriter finish failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    }

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apache.arrow.stream",
        )],
        buf,
    ))
}

// ── ESS handlers ─────────────────────────────────────────────────────────────

/// `GET /ess/{job_id}/{stage_id}/{partition}` — serve a single partition's
/// Arrow IPC bytes by seeking into the sort-shuffle data file via the index.
async fn ess_read_partition(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath((job_id, stage_id, partition)): AxumPath<(String, String, u32)>,
) -> Result<impl IntoResponse, StatusCode> {
    check_bearer_token(&headers, &state.token)?;

    let files = state
        .ess_index
        .get(&job_id, &stage_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    if partition >= files.partition_count {
        return Err(StatusCode::NOT_FOUND);
    }

    let offsets = files.read_offsets().map_err(|e| {
        tracing::error!(error = %e, "ESS read index failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let start = offsets[partition as usize];
    let end = offsets[partition as usize + 1];

    if start == end {
        // Empty partition: return an empty body with the correct content type.
        return Ok((
            [(
                axum::http::header::CONTENT_TYPE,
                "application/vnd.apache.arrow.stream",
            )],
            Vec::new(),
        ));
    }

    // Read just the partition's byte slice from the data file.
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(&files.data_path).map_err(|e| {
        tracing::error!(error = %e, "ESS open data file failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    f.seek(SeekFrom::Start(start)).map_err(|e| {
        tracing::error!(error = %e, "ESS seek failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let len = (end - start) as usize;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf).map_err(|e| {
        tracing::error!(error = %e, "ESS read_exact failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apache.arrow.stream",
        )],
        buf,
    ))
}

/// `POST /ess/gc/{job_id}` — remove all ESS index entries for a job.
/// Called by the coordinator (or executor) after job completion so index memory
/// is reclaimed without restarting the service.
async fn ess_gc_job(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath(job_id): AxumPath<String>,
) -> StatusCode {
    if let Err(status) = check_bearer_token(&headers, &state.token) {
        return status;
    }
    state.ess_index.remove_job(&job_id);
    StatusCode::NO_CONTENT
}

// ── T12 push-shuffle handlers ────────────────────────────────────────────────

/// `POST /ess/push/{job_id}/{stage_id}/{task_id}/{partition}` — map task pushes
/// its Arrow IPC payload for `partition` to the push store.
async fn ess_push_partition(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath((job_id, stage_id, _task_id, partition)): AxumPath<(String, String, String, u32)>,
    body: axum::body::Bytes,
) -> StatusCode {
    if let Err(status) = check_bearer_token(&headers, &state.token) {
        return status;
    }
    state
        .push_store
        .push(&job_id, &stage_id, partition, body.to_vec());
    StatusCode::NO_CONTENT
}

/// `GET /ess/merged/{job_id}/{stage_id}/{partition}` — returns the concatenated
/// IPC stream from all map tasks that pushed data for this partition.
async fn ess_merged_read(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath((job_id, stage_id, partition)): AxumPath<(String, String, u32)>,
) -> Result<impl IntoResponse, StatusCode> {
    check_bearer_token(&headers, &state.token)?;

    let merged = state
        .push_store
        .merge_read(&job_id, &stage_id, partition)
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apache.arrow.stream",
        )],
        merged,
    ))
}

/// `POST /ess/push-gc/{job_id}` — remove all push-store data for `job_id`.
async fn ess_push_gc(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath(job_id): AxumPath<String>,
) -> StatusCode {
    if let Err(status) = check_bearer_token(&headers, &state.token) {
        return status;
    }
    state.push_store.gc_job(&job_id);
    StatusCode::NO_CONTENT
}

// ── Auth helper ───────────────────────────────────────────────────────────────

fn check_bearer_token(
    headers: &axum::http::HeaderMap,
    token: &Option<String>,
) -> Result<(), StatusCode> {
    if let Some(tok) = token {
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let expected = format!("Bearer {tok}");
        if !constant_time_eq(auth_header.as_bytes(), expected.as_bytes()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}
