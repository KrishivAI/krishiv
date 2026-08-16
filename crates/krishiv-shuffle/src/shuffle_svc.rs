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
    /// DIST-6: Token stored behind a RwLock so it can be reloaded at runtime.
    pub(crate) token: Arc<std::sync::RwLock<Option<String>>>,
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
    // DIST-6: Store token in a RwLock for runtime reload.
    let token_val = std::env::var("KRISHIV_SHUFFLE_TOKEN").ok();
    // Also read from file if set.
    let token_val = if token_val.is_none() {
        if let Ok(token_file) = std::env::var("KRISHIV_SHUFFLE_TOKEN_FILE") {
            tokio::fs::read_to_string(&token_file)
                .await
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        } else {
            None
        }
    } else {
        token_val
    };
    // SEC-3 (Phase 63): the shuffle data plane carries real user data
    // (intermediate query results) between executors. Under a durable/production
    // profile, refuse to start an unauthenticated service — a missing token is a
    // fail-closed startup error, mirroring the executor task-auth startup guard,
    // not a silently-open endpoint that `check_bearer_token` would wave through.
    crate::token_auth::require_shuffle_token_or_fail(
        token_val.is_some(),
        krishiv_common::resolve_durability_profile(),
    )?;
    let token = Arc::new(std::sync::RwLock::new(token_val));
    let ess_index = SortShuffleIndex::new();
    // The push store's ceiling comes from the same container budget that sizes
    // the query pool. It used to take its own 2 GiB default, which on a 4500Mi
    // executor put total claims 0.45 GiB over the container and on a 2500Mi
    // executor 1.22 GiB over — the executors were OOM-killed mid-sweep while
    // the query pool, which could only see its own reservations, reported
    // headroom throughout. `None` (no cgroup limit) keeps the previous
    // unbounded behaviour, which is correct off a container.
    let capacity = krishiv_common::ExecutorCapacity::detect();
    let push_store = match capacity.shuffle_store_bytes {
        Some(bytes) => {
            PushShuffleStore::new().with_memory_limit(usize::try_from(bytes).unwrap_or(usize::MAX))
        }
        None => PushShuffleStore::new(),
    };
    tracing::info!(
        shuffle_store_bytes = ?capacity.shuffle_store_bytes,
        container_bytes = ?capacity.memory_limit_bytes,
        "push-shuffle store sized from the container budget"
    );
    let state = ShuffleSvcState {
        store,
        ess_index,
        push_store,
        token,
    };
    let app = build_router(state.clone());

    // DIST-6: Spawn a background reload task if a token file is configured.
    if let Ok(token_file) = std::env::var("KRISHIV_SHUFFLE_TOKEN_FILE") {
        let reload_token = state.token.clone();
        let reload_secs = std::env::var("KRISHIV_SHUFFLE_TOKEN_RELOAD_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(reload_secs));
            loop {
                interval.tick().await;
                if let Ok(content) = tokio::fs::read_to_string(&token_file).await {
                    let trimmed = content.trim().to_string();
                    if !trimmed.is_empty()
                        && let Ok(mut guard) = reload_token.write()
                    {
                        *guard = Some(trimmed);
                    }
                }
            }
        });
    }
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
        // DIST-4: Set expected map-task push count for a partition so
        // merged reads wait until all expected pushes have arrived.
        .route(
            "/ess/expect/{job_id}/{stage_id}/{partition}",
            post(ess_set_expected_pushes),
        )
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

    // DIST-3: read_offsets opens the index file synchronously — offload
    // to spawn_blocking so the async handler doesn't block the reactor.
    let files_clone = files.clone();
    let offsets = tokio::task::spawn_blocking(move || files_clone.read_offsets())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|e| {
            tracing::error!(error = %e, "ESS read index failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let start = offsets.get(partition as usize).copied().ok_or_else(|| {
        tracing::error!(partition, "partition index out of range for offsets");
        StatusCode::BAD_REQUEST
    })?;
    let end = offsets
        .get(partition as usize + 1)
        .copied()
        .ok_or_else(|| {
            tracing::error!(partition, "partition+1 index out of range for offsets");
            StatusCode::BAD_REQUEST
        })?;

    // The index file decides how many bytes this handler allocates, so it has
    // to be checked against the data file rather than trusted.
    //
    // `let len = (end - start) as usize` on a non-monotonic index underflows:
    // a debug build panics inside the handler, and a release build wraps to a
    // number near `u64::MAX` and hands it straight to `vec![0u8; len]`, which
    // aborts the process on allocation failure. An index truncated by a crash
    // mid-write, or one whose last offset outlives a re-written data file, is
    // enough — no malice required. Both are refused here as client errors,
    // which is what they are.
    if end < start {
        tracing::error!(
            partition,
            start,
            end,
            "ESS index is not monotonic; refusing to size a read from it"
        );
        return Err(StatusCode::BAD_REQUEST);
    }
    let data_len = tokio::fs::metadata(&files.data_path)
        .await
        .map(|m| m.len())
        .map_err(|e| {
            tracing::error!(error = %e, "ESS stat data file failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    if end > data_len {
        tracing::error!(
            partition,
            end,
            data_len,
            "ESS index points past the end of the data file"
        );
        return Err(StatusCode::BAD_REQUEST);
    }

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

    // DIST-3: Move synchronous filesystem I/O to spawn_blocking so the async
    // handler doesn't stall the Tokio reactor for large partition reads.
    let data_path = files.data_path.clone();
    let read_result = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(&data_path).map_err(|e| {
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
        Ok::<Vec<u8>, StatusCode>(buf)
    })
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "ESS spawn_blocking join failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })??;

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apache.arrow.stream",
        )],
        read_result,
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
    if let Err(e) = state
        .push_store
        .push(&job_id, &stage_id, partition, body.to_vec())
    {
        tracing::warn!(error = %e, "shuffle push_store.push returned error");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
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

/// DIST-4: `POST /ess/expect/{job_id}/{stage_id}/{partition}?count=N`
///
/// Sets the expected number of map-task pushes for a partition so that
/// merge_read returns None until all expected pushes have arrived.
/// Called by the coordinator when assigning shuffle-stage tasks.
async fn ess_set_expected_pushes(
    headers: axum::http::HeaderMap,
    State(state): State<ShuffleSvcState>,
    AxumPath((job_id, stage_id, partition)): AxumPath<(String, String, u32)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    if let Err(status) = check_bearer_token(&headers, &state.token) {
        return status;
    }
    let count: usize = params
        .get("count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if count > 0 {
        state
            .push_store
            .set_expected_pushes(&job_id, &stage_id, partition, count);
        StatusCode::NO_CONTENT
    } else {
        StatusCode::BAD_REQUEST
    }
}

// ── Auth helper ───────────────────────────────────────────────────────────────

fn check_bearer_token(
    headers: &axum::http::HeaderMap,
    token: &Arc<std::sync::RwLock<Option<String>>>,
) -> Result<(), StatusCode> {
    // DIST-6: Read through the RwLock so the token can be reloaded at runtime.
    let guard = token
        .read()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(tok) = guard.as_ref() {
        let presented = krishiv_common::bearer_token(
            headers.get("authorization").and_then(|v| v.to_str().ok()),
        )
        .unwrap_or("");
        if !constant_time_eq(presented.as_bytes(), tok.as_bytes()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}

// SEC-3 (Phase 63) startup fail-closed guard now lives in [`crate::token_auth`]
// and is shared with the Flight shuffle server; its unit tests live there.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::ShufflePartition;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

    const TOKEN: &str = "s3cr3t-shuffle-token";

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn state_with(dir: &Path) -> ShuffleSvcState {
        ShuffleSvcState {
            store: Arc::new(LocalDiskShuffleStore::new(dir).unwrap()),
            ess_index: SortShuffleIndex::new(),
            push_store: PushShuffleStore::new(),
            token: Arc::new(std::sync::RwLock::new(Some(TOKEN.to_string()))),
        }
    }

    /// Issue one request against a fresh router built from `state`.
    async fn call(
        state: &ShuffleSvcState,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: Body,
    ) -> (StatusCode, Vec<u8>) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let response = build_router(state.clone())
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap();
        (status, bytes.to_vec())
    }

    /// Write an ESS index file with the given offsets and register it.
    fn register_ess(state: &ShuffleSvcState, dir: &Path, data: &[u8], offsets: &[u64]) {
        let data_path = dir.join("ess.data");
        let index_path = dir.join("ess.index");
        std::fs::write(&data_path, data).unwrap();
        let mut raw = Vec::with_capacity(offsets.len() * 8);
        for o in offsets {
            raw.extend_from_slice(&o.to_le_bytes());
        }
        std::fs::write(&index_path, raw).unwrap();
        state.ess_index.register(
            "job",
            "stage",
            SortShuffleFiles {
                data_path,
                index_path,
                // The index carries `partition_count + 1` offsets.
                partition_count: (offsets.len() - 1) as u32,
            },
        );
    }

    /// Every route that touches shuffle data must reject a wrong token.
    ///
    /// This file had **6.61% coverage** while serving intermediate query data
    /// between executors, and the auth check is a per-handler call rather than
    /// middleware — so a new handler that simply forgets to call
    /// `check_bearer_token` is unauthenticated and nothing notices. Enumerating
    /// the routes here is the only thing that makes that visible.
    #[tokio::test]
    async fn every_data_route_refuses_a_wrong_token() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        let routes = [
            ("GET", "/shuffle/job/stage/0"),
            ("GET", "/ess/job/stage/0"),
            ("POST", "/ess/gc/job"),
            ("POST", "/ess/push/job/stage/task/0"),
            ("GET", "/ess/merged/job/stage/0"),
            ("POST", "/ess/push-gc/job"),
            ("POST", "/ess/expect/job/stage/0?count=1"),
        ];
        for (method, uri) in routes {
            let (status, _) = call(&state, method, uri, Some("wrong"), Body::empty()).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} accepted a wrong bearer token"
            );
            let (status, _) = call(&state, method, uri, None, Body::empty()).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} accepted a request with no bearer token"
            );
        }
    }

    /// `/healthz` is deliberately open — it carries no data and a liveness
    /// probe has no credentials.
    #[tokio::test]
    async fn healthz_is_reachable_without_a_token() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        let (status, body) = call(&state, "GET", "/healthz", None, Body::empty()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"ok\n");
    }

    #[tokio::test]
    async fn a_written_partition_is_served_as_arrow_ipc() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
        store
            .write_partition(
                ShufflePartition {
                    id: PartitionId {
                        job_id: "job".into(),
                        stage_id: "stage".into(),
                        partition: 0,
                    },
                    schema: batch(&[1, 2, 3]).schema(),
                    batches: vec![batch(&[1, 2, 3])],
                },
                1,
            )
            .await
            .unwrap();

        let (status, body) = call(
            &state,
            "GET",
            "/shuffle/job/stage/0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
        let rows: Vec<i64> = reader
            .flat_map(|b| {
                b.unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(rows, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_partition_that_was_never_written_is_404() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        let (status, _) = call(
            &state,
            "GET",
            "/shuffle/job/stage/7",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn an_ess_partition_is_served_from_its_byte_range() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        register_ess(&state, dir.path(), b"AAAABBBBCC", &[0, 4, 8, 10]);
        for (partition, want) in [(0u32, &b"AAAA"[..]), (1, b"BBBB"), (2, b"CC")] {
            let (status, body) = call(
                &state,
                "GET",
                &format!("/ess/job/stage/{partition}"),
                Some(TOKEN),
                Body::empty(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "partition {partition}");
            assert_eq!(body, want, "partition {partition} served the wrong range");
        }
    }

    /// A non-monotonic index used to reach `let len = (end - start) as usize`.
    ///
    /// Debug builds panicked inside the handler; release builds wrapped to a
    /// number near `u64::MAX` and passed it to `vec![0u8; len]`, aborting the
    /// process on allocation failure. A crash mid-index-write is enough to
    /// produce one — this is a robustness bug, not a security scenario, and it
    /// takes down the whole shuffle service rather than one request.
    #[tokio::test]
    async fn a_non_monotonic_ess_index_is_refused_instead_of_sizing_an_allocation_from_it() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        // Partition 0 claims to run from byte 8 back to byte 0.
        register_ess(&state, dir.path(), b"AAAABBBB", &[8, 0, 8]);
        let (status, _) = call(
            &state,
            "GET",
            "/ess/job/stage/0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a descending offset pair must be refused, not turned into a length"
        );
    }

    /// An index whose last offset outlives a re-written (shorter) data file
    /// would `read_exact` past EOF. That surfaces as a 500 either way, but the
    /// allocation happens first — so it is refused before the `vec![0u8; len]`.
    #[tokio::test]
    async fn an_ess_index_pointing_past_the_data_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        register_ess(&state, dir.path(), b"AAAA", &[0, 4_000_000_000]);
        let (status, _) = call(
            &state,
            "GET",
            "/ess/job/stage/0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "an offset past EOF must be refused before it sizes a buffer"
        );
    }

    #[tokio::test]
    async fn an_ess_partition_beyond_the_partition_count_is_404() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        register_ess(&state, dir.path(), b"AAAA", &[0, 4]);
        let (status, _) = call(
            &state,
            "GET",
            "/ess/job/stage/9",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ess_gc_drops_the_index_entry_for_that_job_only() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        register_ess(&state, dir.path(), b"AAAA", &[0, 4]);
        state.ess_index.register(
            "other-job",
            "stage",
            SortShuffleFiles {
                data_path: dir.path().join("ess.data"),
                index_path: dir.path().join("ess.index"),
                partition_count: 1,
            },
        );

        let (status, _) = call(&state, "POST", "/ess/gc/job", Some(TOKEN), Body::empty()).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(state.ess_index.get("job", "stage").is_none());
        assert!(
            state.ess_index.get("other-job", "stage").is_some(),
            "GC must be scoped to the job it was asked about"
        );
    }

    /// The push path end to end: expected-count gating, then push, then merge.
    ///
    /// `merge_read` returning `None` before every expected push has arrived is
    /// what stops a reduce task reading a partial partition, so the test
    /// asserts the 404 *before* asserting the success.
    #[tokio::test]
    async fn pushed_partitions_merge_only_once_every_expected_push_arrives() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());

        let (status, _) = call(
            &state,
            "POST",
            "/ess/expect/job/stage/0?count=2",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, _) = call(
            &state,
            "POST",
            "/ess/push/job/stage/task-a/0",
            Some(TOKEN),
            Body::from(b"AAAA".to_vec()),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, _) = call(
            &state,
            "GET",
            "/ess/merged/job/stage/0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "a merged read must not serve a partition that is still missing a push"
        );

        let (status, _) = call(
            &state,
            "POST",
            "/ess/push/job/stage/task-b/0",
            Some(TOKEN),
            Body::from(b"BBBB".to_vec()),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, body) = call(
            &state,
            "GET",
            "/ess/merged/job/stage/0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.len(),
            8,
            "both pushes must appear in the merged stream"
        );

        let (status, _) = call(
            &state,
            "POST",
            "/ess/push-gc/job",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(
            &state,
            "GET",
            "/ess/merged/job/stage/0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "GC must drop pushed data");
    }

    /// `?count=0` is rejected: it would mean "this partition expects nothing",
    /// which `merge_read` cannot distinguish from "not configured".
    #[tokio::test]
    async fn an_expected_push_count_of_zero_is_a_bad_request() {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(dir.path());
        let (status, _) = call(
            &state,
            "POST",
            "/ess/expect/job/stage/0?count=0",
            Some(TOKEN),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// With no token configured the service is open by design — the fail-closed
    /// decision is made once at startup by
    /// `token_auth::require_shuffle_token_or_fail`, not per request. Pinning
    /// this means a change that made `check_bearer_token` fail closed would
    /// surface here rather than as a dev-mode outage.
    #[tokio::test]
    async fn with_no_token_configured_requests_are_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = state_with(dir.path());
        state.token = Arc::new(std::sync::RwLock::new(None));
        let (status, _) = call(&state, "GET", "/shuffle/job/stage/0", None, Body::empty()).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "an unauthenticated service must reach the handler (404 = no such partition)"
        );
    }
}
