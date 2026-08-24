#![forbid(unsafe_code)]

//! HTTP handlers for the IVM (DeltaBatch) API.
//!
//! # Protocol overview
//!
//! | Method | Path                                            | Description                       |
//! |--------|-------------------------------------------------|-----------------------------------|
//! | POST   | `/api/v1/ivm/jobs`                              | Create a new IVM job              |
//! | GET    | `/api/v1/ivm/jobs`                              | List all IVM job IDs              |
//! | DELETE | `/api/v1/ivm/jobs/{job_id}`                     | Delete an IVM job                 |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/views`               | Register or update a view         |
//! | DELETE | `/api/v1/ivm/jobs/{job_id}/views/{view_name}`   | Drop a view                       |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/sources/{src}/feed`  | Feed a DeltaBatch (Arrow IPC b64) |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/step`                | Run one IVM tick                  |
//! | GET    | `/api/v1/ivm/jobs/{job_id}/views/{view}/snap`   | Current snapshot (Arrow IPC b64)  |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/checkpoint`          | Serialize state to bytes (b64)    |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/restore`             | Restore state from bytes (b64)    |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/vector-views`        | Register a vector view (preview)  |
//! | GET    | `/api/v1/ivm/jobs/{job_id}/vector-views`        | List vector views + sink health   |
//! | DELETE | `/api/v1/ivm/jobs/{job_id}/vector-views/{view}` | Stop a vector view                |
//!
//! Vector views are an **HTTP-only preview** (IVM-AUD-INT-F17): the only
//! supported `sink_type` is `in_memory`, and there is no CLI, Python, MCP or
//! SQL surface for them. See `krishiv_ivm::vector_sink` for the full statement.

use axum::Json;
use axum::extract::{FromRef, Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::time::Duration;

use krishiv_ivm::{
    DeltaBatch, IncrementalFlow, IncrementalViewSpec, coalesce_pending, deserialize_delta_batch,
    serialize_delta_batch,
};
use krishiv_proto::{JobId, JobKind, JobSpec, JobState, StageId, StageSpec, TaskId, TaskSpec};

use crate::SharedCoordinator;
use crate::ivm::{RegisteredVectorView, SharedIvmJobRegistry};

// ── combined router state ─────────────────────────────────────────────────────

/// Router state for IVM endpoints: job registry + coordinator reference.
///
/// Carrying the coordinator enables the step handler to check executor
/// availability and log distributed-compute context (future: offload heavy
/// IVM computation to registered executors rather than always running on the
/// coordinator).
#[derive(Clone)]
pub struct IvmRouterState {
    pub registry: SharedIvmJobRegistry,
    pub coordinator: SharedCoordinator,
}

impl FromRef<IvmRouterState> for SharedIvmJobRegistry {
    fn from_ref(state: &IvmRouterState) -> Self {
        state.registry.clone()
    }
}

impl FromRef<IvmRouterState> for SharedCoordinator {
    fn from_ref(state: &IvmRouterState) -> Self {
        state.coordinator.clone()
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn ivm_err(msg: impl std::fmt::Display) -> StatusCode {
    tracing::warn!("IVM error: {msg}");
    StatusCode::BAD_REQUEST
}

fn ivm_not_found(job_id: &str) -> StatusCode {
    tracing::warn!("IVM job not found: {job_id}");
    StatusCode::NOT_FOUND
}

/// Resolve a job, transparently rehydrating it from the coordinator's durable
/// snapshot when this process has never seen it.
///
/// **Every handler that names a job must go through here.** The registry is
/// process-local and nothing repopulates it at startup — `restore_durable_snapshot`
/// is reachable only from this function and `api_ivm_create_job`. So after a
/// coordinator restart or a failover to a standby, a job whose state is sitting
/// in the metadata store is simply absent until some handler rehydrates it.
///
/// Ten handlers used `registry.get` directly and 404'd instead. The read side
/// was the visible half: `/stats` exists to be polled "every few seconds" by
/// the platform freshness sampler, and it answered 404 — a live table reported
/// as missing — until an unrelated `/feed` or `/step` happened to resurrect the
/// job. `/checkpoint`, the backup path, failed the same way, and `/restore`
/// refused to restore into a job that demonstrably existed.
/// Reject a mutating IVM request unless this coordinator is the active leader.
///
/// IVM-AUD-DIST-E1: there was no leader check on any IVM endpoint. A demoted
/// coordinator served feed/step/restore/delete at 200, rehydrated the job from
/// the store, advanced its own copy of the flow and persisted it back over the
/// new leader's snapshot — split-brain, with both halves answering `success:
/// true`. Reads are deliberately left unfenced (a stale read is not a
/// divergence); everything that writes goes through here.
async fn ensure_ivm_leader(coordinator: &SharedCoordinator) -> Result<(), StatusCode> {
    coordinator.ensure_active_leader().await.map_err(|e| {
        tracing::warn!("IVM request refused: {e}");
        StatusCode::SERVICE_UNAVAILABLE
    })
}

async fn ensure_ivm_job(
    registry: &SharedIvmJobRegistry,
    coordinator: &SharedCoordinator,
    job_id: &str,
) -> Result<crate::ivm::IvmJob, StatusCode> {
    if let Some(job) = registry.get(job_id) {
        return Ok(job);
    }
    let snapshot = coordinator
        .load_ivm_snapshot(job_id)
        .await
        .ok_or_else(|| ivm_not_found(job_id))?;
    registry
        .restore_durable_snapshot(job_id, &snapshot)
        .map_err(ivm_err)?;
    registry.get(job_id).ok_or_else(|| ivm_not_found(job_id))
}

async fn persist_ivm_job(
    registry: &SharedIvmJobRegistry,
    coordinator: &SharedCoordinator,
    job_id: &str,
) -> Result<(), StatusCode> {
    let snapshot = registry.durable_snapshot(job_id).map_err(ivm_err)?;
    coordinator
        .save_ivm_snapshot(job_id, snapshot)
        .await
        .map_err(|error| {
            tracing::error!(job_id, %error, "persisting IVM snapshot failed");
            StatusCode::SERVICE_UNAVAILABLE
        })
}

/// Whether this coordinator can actually make IVM state durable, warning once
/// per process when it cannot.
///
/// IVM-AUD-DIST-C5: with no metadata store configured,
/// `SharedCoordinator::save_ivm_snapshot` returns `Ok(())` without writing
/// anything. Every IVM handler then reported success for a write that never
/// happened, `/restore` answered `{"success": true}` for a rewind that existed
/// only in this process's memory, and a restart lost the lot — silently, with
/// no line in the log and no field in any response. Handlers that claim
/// durability now report this back, so "not durable" is a fact the caller can
/// read rather than something it discovers after a restart.
async fn ivm_writes_are_durable(coordinator: &SharedCoordinator) -> bool {
    let durable = coordinator.has_metadata_store().await;
    if !durable {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                "this coordinator has no metadata store configured: IVM job state is held in \
                 memory only, every persist is a no-op, and a restart loses all of it. IVM \
                 responses report `durable: false` while this is the case."
            );
        });
    }
    durable
}

/// Refuse a feed when the job's un-stepped backlog is already at the cap.
///
/// IVM-AUD-INT-F11: nothing anywhere applied backpressure. `pending` is an
/// unbounded `Vec` per source, the body limit is 512 MiB per request and there
/// is no concurrency limiter, so a producer feeding faster than the stepper
/// drains grew the coordinator's heap until the process died — with every
/// `/feed` answering `success: true` right up to the end. `429 Too Many
/// Requests` is the answer that lets a client back off instead.
///
/// The check is deliberately coarse: it reads the backlog *before* admitting
/// this delta, so N concurrent feeds can each pass and overshoot the cap by up
/// to N bodies. That bounds the overshoot by the concurrency, which is the
/// property that was missing; making it exact would need the admission to be
/// atomic with the feed, and the feed is the thing being protected.
fn ensure_pending_headroom(
    registry: &SharedIvmJobRegistry,
    flow: &crate::ivm::IvmJob,
    job_id: &str,
) -> Result<(), StatusCode> {
    let cap = registry.max_pending_bytes();
    if cap == 0 {
        return Ok(());
    }
    let pending = flow.pending_bytes().map_err(ivm_err)? as u64;
    if pending >= cap {
        tracing::warn!(
            job_id,
            pending_bytes = pending,
            cap_bytes = cap,
            "IVM feed refused: the job's un-stepped backlog is at the cap (step it, or raise \
             KRISHIV_IVM_MAX_PENDING_BYTES)"
        );
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    Ok(())
}

// ── schema JSON ───────────────────────────────────────────────────────────────

/// A field of the **legacy** type-name schema wire.
///
/// This form can only express the types [`parse_schema`] whitelists; anything
/// else is a 400. New clients send `output_schema_ipc_b64` instead (see
/// [`RegisterViewRequest::output_schema_ipc_b64`], IVM-AUD-API-A1) and this
/// remains only so an older client keeps working.
#[derive(Debug, Deserialize)]
pub struct SchemaFieldJson {
    pub name: String,
    /// Arrow DataType as a string: "Int32", "Int64", "Float32", "Float64",
    /// "Utf8", "LargeUtf8", "Boolean", "Binary", "TimestampMs".
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Deserialize)]
pub struct SchemaJson {
    pub fields: Vec<SchemaFieldJson>,
}

fn parse_schema(s: &SchemaJson) -> Option<arrow::datatypes::SchemaRef> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let fields: Option<Vec<Field>> = s
        .fields
        .iter()
        .map(|f| {
            let dt = match f.data_type.as_str() {
                "Int8" => Some(DataType::Int8),
                "Int16" => Some(DataType::Int16),
                "Int32" => Some(DataType::Int32),
                "Int64" => Some(DataType::Int64),
                "UInt8" => Some(DataType::UInt8),
                "UInt16" => Some(DataType::UInt16),
                "UInt32" => Some(DataType::UInt32),
                "UInt64" => Some(DataType::UInt64),
                "Float32" => Some(DataType::Float32),
                "Float64" => Some(DataType::Float64),
                "Utf8" => Some(DataType::Utf8),
                // DataFusion 54 emits Utf8View as the default string representation
                // (e.g. CAST(x AS VARCHAR), string GROUP BY keys), so an
                // IncrementalDataFrame's inferred output schema serializes string
                // columns as "Utf8View". Accept it (and LargeUtf8) as a string type.
                "Utf8View" => Some(DataType::Utf8View),
                "LargeUtf8" => Some(DataType::LargeUtf8),
                "Boolean" => Some(DataType::Boolean),
                "Binary" => Some(DataType::Binary),
                "TimestampMs" => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
                "TimestampUs" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
                "Date32" => Some(DataType::Date32),
                "Date64" => Some(DataType::Date64),
                _ => None,
            }?;
            Some(Field::new(f.name.clone(), dt, f.nullable))
        })
        .collect();
    Some(std::sync::Arc::new(Schema::new(fields?)))
}

/// Decode an Arrow IPC **schema** message (base64 of a stream whose only
/// required content is the schema header) back into a `SchemaRef`.
///
/// IVM-AUD-API-A1: the type-name wire above is a closed whitelist, so a
/// DataFrame whose output schema contains `Decimal128`, a timezoned
/// `Timestamp`, `Time32`/`Time64`, `List`, `Struct`, `Interval`, `Null` or
/// `FixedSizeBinary` could be registered embedded and 400'd distributed. Arrow's
/// own encoding carries every type — including field metadata, nested children
/// and dictionary ids — so shipping it removes the whole class instead of
/// widening the match arm by arm.
fn parse_schema_ipc(b64: &str) -> Result<arrow::datatypes::SchemaRef, String> {
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
        .map_err(|e| format!("output_schema_ipc_b64 base64 decode: {e}"))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("output_schema_ipc_b64 is not an Arrow IPC stream: {e}"))?;
    Ok(reader.schema())
}

// ── POST /api/v1/ivm/jobs ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    /// Optional explicit job ID. If absent, a UUID v4 is generated.
    pub job_id: Option<String>,
    /// When `Some(false)`, the job is pinned to a single (non-partitioned) flow
    /// so it can host a view-DAG (a derived view reading the base view's full
    /// output). Absent / `Some(true)` keeps the default auto-partitioning.
    #[serde(default)]
    pub partitioned: Option<bool>,
    /// Accumulate every fed delta so `POST .../checkpoint-delta` returns a real
    /// incremental backup.
    ///
    /// IVM-AUD-DIST-C1: nothing in the HTTP API switched accumulation on and
    /// there was no field to ask for it, so `/checkpoint-delta` answered a
    /// well-formed **count = 0** frame forever and `/restore-delta` composed
    /// nothing — a caller taking incremental backups got success responses and
    /// backups containing none of the input since the last full checkpoint.
    ///
    /// Off by default, because it is not free: every accepted delta is retained
    /// in memory until a `/checkpoint-delta` call drains it, so a job nobody
    /// backs up would grow without bound.
    ///
    /// This route is idempotent (an existing job is rehydrated, not replaced),
    /// so posting it again with `delta_checkpoints: true` is also how
    /// accumulation is switched on for a job that already exists. There is no
    /// way to switch it back off: the flow has no disable, and pretending
    /// otherwise is the bug this field fixes.
    #[serde(default)]
    pub delta_checkpoints: bool,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub job_id: String,
    /// Whether this coordinator can persist the job at all (IVM-AUD-DIST-C5).
    /// `false` means the job is memory-only and a coordinator restart loses it,
    /// which is otherwise indistinguishable from a durable create.
    pub durable: bool,
}

/// Bring `job_id` into the live registry and make it durable.
///
/// Every entry point that creates an IVM job must go through this. The order
/// matters and is not obvious: a job absent from the registry may still have a
/// durable snapshot (coordinator restart, standby promotion, eviction), so
/// **rehydrate before creating**. Creating first yields an empty job under a
/// live id, which the next `/step` or `/restore` then persists straight over
/// the real state — the read handlers' own `ensure_ivm_job` rehydration cannot
/// save it either, because the id *is* present, just empty.
///
/// `partitioned == Some(false)` pins the job to a single flow so it can host a
/// view-DAG; absent / `Some(true)` keeps the default auto-partitioning. The
/// shape is chosen only when the job is genuinely new — a job that already
/// exists (live or rehydrated) keeps the shape it has.
///
/// # A shape request that cannot be honoured is a 409, not a success
///
/// IVM-AUD-INT-F16: `partitioned` used to be consulted *only* on the create
/// branch, so asking for an unpartitioned job under a name that already held a
/// partitioned one returned `200 {job_id}` and a job that was still
/// partitioned. That is exactly the `Session::ivm` (auto-partitioning) vs
/// `DataFrame::to_incremental` (pinned single) collision under one name — and
/// the caller who asked for single did so because a partitioned flow never
/// cascades a base view's output to derived views, so their view-DAG would sit
/// empty forever with nothing said. The request is now refused.
pub(crate) async fn create_or_rehydrate_ivm_job(
    registry: &SharedIvmJobRegistry,
    coordinator: &SharedCoordinator,
    job_id: &str,
    partitioned: Option<bool>,
    delta_checkpoints: bool,
) -> Result<(), StatusCode> {
    if registry.get(job_id).is_none() {
        if let Some(snapshot) = coordinator.load_ivm_snapshot(job_id).await {
            registry
                .restore_durable_snapshot(job_id, &snapshot)
                .map_err(ivm_err)?;
        } else if partitioned == Some(false) {
            registry
                .create_unpartitioned(job_id.to_owned())
                .map_err(ivm_err)?;
        } else {
            registry.create(job_id.to_owned()).map_err(ivm_err)?;
        }
    }
    // IVM-AUD-INT-F16. Checked after the branch above so it covers all three
    // arrivals — already live, just rehydrated, just created — and a freshly
    // created unpartitioned job passes it by construction.
    if partitioned == Some(false) && registry.get(job_id).is_some_and(|job| job.is_partitioned()) {
        tracing::warn!(
            job_id,
            "IVM create refused: an unpartitioned job was requested under a name that already \
             holds a key-partitioned one"
        );
        return Err(StatusCode::CONFLICT);
    }
    // Applied to a rehydrated job too, unlike `partitioned`: accumulation is
    // monotone and cheap to re-assert, so this route doubles as the "turn it on
    // for a job that already exists" surface (IVM-AUD-DIST-C1).
    if delta_checkpoints {
        registry.enable_delta_checkpoints(job_id).map_err(ivm_err)?;
    }
    persist_ivm_job(registry, coordinator, job_id).await
}

pub async fn api_ivm_create_job(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let job_id = body
        .job_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    create_or_rehydrate_ivm_job(
        &registry,
        &coordinator,
        &job_id,
        body.partitioned,
        body.delta_checkpoints,
    )
    .await?;
    let durable = ivm_writes_are_durable(&coordinator).await;
    Ok(Json(CreateJobResponse { job_id, durable }))
}

// ── GET /api/v1/ivm/jobs ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ListJobsResponse {
    pub job_ids: Vec<String>,
    /// Per-job detail (additive, #console): view names let a client fetch
    /// per-view stats without guessing names, which the id list alone
    /// forced. Snapshot-only jobs (present durably but not live in the
    /// registry) report no views and live=false.
    pub jobs: Vec<IvmJobSummary>,
}

#[derive(Debug, Serialize)]
pub struct IvmJobSummary {
    pub job_id: String,
    pub view_names: Vec<String>,
    pub partitioned: bool,
    /// Whether the job is live in the registry (vs. snapshot-only).
    pub live: bool,
}

pub async fn api_ivm_list_jobs(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
) -> Json<ListJobsResponse> {
    // IVM-AUD-DIST-C4: `list_ivm_snapshots` hands back the snapshot bytes and
    // this used to drop them with `|(job_id, _)|`, then fabricate
    // `partitioned: false` for every job it had not rehydrated. For the
    // canonical IVM workload — a single-column `GROUP BY` first view, which
    // auto-partitions — that is simply the wrong answer, about the one property
    // of a job a client can never change afterwards. Read the persisted shape.
    let mut summaries: HashMap<String, crate::ivm::IvmSnapshotSummary> = HashMap::new();
    let mut job_ids = registry.job_ids();
    for (job_id, snapshot) in coordinator.list_ivm_snapshots().await {
        if let Some(summary) = crate::ivm::read_ivm_snapshot_summary(&snapshot) {
            summaries.insert(job_id.clone(), summary);
        }
        job_ids.push(job_id);
    }
    job_ids.sort();
    job_ids.dedup();
    let jobs = job_ids
        .iter()
        .map(|id| match registry.get(id) {
            Some(job) => IvmJobSummary {
                job_id: id.clone(),
                view_names: job.view_names(),
                partitioned: job.is_partitioned(),
                live: true,
            },
            // Snapshot-only. A snapshot this build cannot parse leaves no
            // summary, and then the honest answer is still the old empty one.
            None => {
                let summary = summaries.get(id);
                IvmJobSummary {
                    job_id: id.clone(),
                    view_names: summary.map(|s| s.view_names.clone()).unwrap_or_default(),
                    partitioned: summary.is_some_and(|s| s.partitioned),
                    live: false,
                }
            }
        })
        .collect();
    Json(ListJobsResponse { job_ids, jobs })
}

// ── DELETE /api/v1/ivm/jobs/{job_id} ─────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct DeleteJobResponse {
    pub deleted: bool,
}

pub async fn api_ivm_delete_job(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<DeleteJobResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    // Serialize against any in-flight `/step` for this job by holding the same
    // per-job step lock a tick holds (#224 C). Without this, deletion races a
    // concurrent tick: the tick reads its snapshot, we remove it here, then the
    // tick's trailing `persist_ivm_job` writes the snapshot back — resurrecting
    // a deleted job on disk. Taking the lock makes deletion either win outright
    // (the waiting tick then sees the job gone and 404s without computing —
    // IVM-AUD-DIST-H6) or wait for an in-flight tick to finish first. The wait
    // is bounded by one tick's timeout now that both the resident and central
    // step paths are time-bounded (#224 B) — and that bound is
    // `KRISHIV_IVM_DISPATCH_TIMEOUT_SECS` (IVM-AUD-DIST-H7), so an operator who
    // needs deletion to give up sooner has a lever.
    let _step_guard = registry.step_lock(&job_id).lock_owned().await;

    // Best-effort detach of the resident executor flow (Phase 57): fire the
    // detach fragment in the background so job deletion never blocks on an
    // executor round trip. If it fails, the orphaned flow is bounded by the
    // executor process lifetime and a re-created same-id job re-attaches
    // (replacing the entry) anyway.
    if registry.dispatch_state(&job_id).attached {
        let coordinator = coordinator.clone();
        let detach = krishiv_ivm::encode_ivm_detach_fragment(&job_id);
        let job = job_id.clone();
        tokio::spawn(async move {
            if let Err(e) = run_ivm_fragment_job(&coordinator, detach, "ivm-detach").await {
                tracing::warn!(job_id = %job, error = %e, "resident IVM detach failed");
            }
        });
    }
    if let Err(error) = coordinator.remove_ivm_snapshot(&job_id).await {
        tracing::error!(job_id, %error, "removing IVM snapshot failed");
    }
    Ok(Json(DeleteJobResponse {
        deleted: registry.delete(&job_id),
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/views ─────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RegisterViewRequest {
    pub name: String,
    pub body_sql: String,
    /// Legacy type-name schema. Still required on the wire so that a client
    /// which predates `output_schema_ipc_b64` is unaffected; ignored whenever
    /// that field is present.
    pub output_schema: SchemaJson,
    /// IVM-AUD-API-A1. Base64 of an Arrow IPC stream whose schema header is the
    /// view's output schema — the authoritative form when present.
    ///
    /// The type-name form above cannot express `Decimal128`, a timezoned
    /// `Timestamp`, `Time32`/`Time64`, `List`, `Struct`, `Interval`, `Null` or
    /// `FixedSizeBinary`, so `DataFrame::to_incremental` on a query returning
    /// any of them worked embedded and answered 400 distributed — the same
    /// DataFrame, a different answer per mode. `#[serde(default)]` keeps an
    /// older client working: it omits the field and gets exactly the whitelist
    /// behaviour it already had.
    #[serde(default)]
    pub output_schema_ipc_b64: Option<String>,
    #[serde(default)]
    pub is_materialized: bool,
    #[serde(default)]
    pub is_recursive: bool,
    /// IVM-AUD-DDL-B1. Absent before, and the handler hardcoded an empty
    /// vector, so a view's LATENESS bound was silently dropped in Distributed
    /// mode: the same SQL meant different retention semantics per mode.
    /// `#[serde(default)]` keeps an older client (which omits the field)
    /// working — it just gets the old no-lateness behaviour it already had.
    #[serde(default)]
    pub lateness: Vec<LatenessJson>,
}

/// Wire form of `krishiv_delta::LatenessSpec`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatenessJson {
    pub column: String,
    pub lateness_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct RegisterViewResponse {
    pub success: bool,
}

pub async fn api_ivm_register_view(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<RegisterViewRequest>,
) -> Result<Json<RegisterViewResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    // Existence is enforced by the registry (which also decides, on the first
    // view, whether to auto-partition the job by a single-column GROUP BY key).
    ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    // Prefer the Arrow IPC schema when the client sent one (IVM-AUD-API-A1);
    // fall back to the type-name whitelist for older clients that cannot.
    let output_schema = match &body.output_schema_ipc_b64 {
        Some(b64) => parse_schema_ipc(b64).map_err(ivm_err)?,
        None => {
            parse_schema(&body.output_schema).ok_or_else(|| ivm_err("invalid output_schema"))?
        }
    };
    let spec = IncrementalViewSpec {
        name: body.name,
        body_sql: body.body_sql,
        output_schema,
        is_materialized: body.is_materialized,
        is_recursive: body.is_recursive,
        lateness: body
            .lateness
            .into_iter()
            .map(|l| krishiv_ivm::LatenessSpec::new(l.column, l.lateness_ms))
            .collect(),
    };
    registry.register_view(&job_id, spec).map_err(ivm_err)?;
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(RegisterViewResponse { success: true }))
}

// ── DELETE /api/v1/ivm/jobs/{job_id}/views/{view_name} ───────────────────────

#[derive(Debug, Serialize)]
pub struct DropViewResponse {
    pub dropped: bool,
}

pub async fn api_ivm_drop_view(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<DropViewResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let dropped = flow.drop_view(&view_name).map_err(ivm_err)?;
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(DropViewResponse { dropped }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/sources/{src}/feed ────────────────────────

#[derive(Debug, Deserialize)]
pub struct FeedSourceRequest {
    /// Base64-encoded Arrow IPC bytes of a serialized `DeltaBatch`.
    pub delta_ipc_b64: String,
}

#[derive(Debug, Serialize)]
pub struct FeedSourceResponse {
    pub success: bool,
}

pub async fn api_ivm_feed_source(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<FeedSourceRequest>,
) -> Result<Json<FeedSourceResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    ensure_pending_headroom(&registry, &flow, &job_id)?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.delta_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // G7: drop zero-weight rows on ingress so downstream operators never see them.
    let delta = deserialize_delta_batch(&ipc_bytes)
        .map_err(ivm_err)?
        .drop_zeros()
        .map_err(ivm_err)?;
    flow.feed(&source_name, delta).map_err(ivm_err)?;
    Ok(Json(FeedSourceResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/sources/{src}/stream-delta ────────────────
//
// Fast path for producers that already emit pre-computed ±1 DeltaBatches
// (CDC-native connectors, Debezium readers) and do not need the snapshot-diff
// overhead of the /stream-bridge endpoint.

#[derive(Debug, Deserialize)]
pub struct FeedStreamDeltaRequest {
    /// Base64-encoded Arrow IPC bytes of a pre-computed `DeltaBatch`.
    pub delta_ipc_b64: String,
}

#[derive(Debug, Serialize)]
pub struct FeedStreamDeltaResponse {
    pub success: bool,
}

pub async fn api_ivm_feed_stream_delta(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<FeedStreamDeltaRequest>,
) -> Result<Json<FeedStreamDeltaResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    ensure_pending_headroom(&registry, &flow, &job_id)?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.delta_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    let delta = deserialize_delta_batch(&ipc_bytes)
        .map_err(ivm_err)?
        .drop_zeros()
        .map_err(ivm_err)?;
    // Pre-computed delta: feed directly (same as /feed; the distinct route is
    // kept for coordinator API/wire compatibility with CDC-native producers).
    flow.feed(&source_name, delta).map_err(ivm_err)?;
    Ok(Json(FeedStreamDeltaResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/step ──────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct StepResponse {
    pub active_views: usize,
    pub total_output_rows: usize,
    pub tick: u64,
    /// IVM-AUD-API-A5. Per-view health for this tick.
    ///
    /// A view whose SQL or operator fails does **not** fail the tick: the flow
    /// skips it, keeps going and reports it here. Before this field existed the
    /// response carried counters only, so a distributed caller had no view-level
    /// failure signal at all — `krishiv_api::StepReport` filled both of its
    /// vectors with `Vec::new()` and a broken view was byte-identical to a
    /// healthy one.
    ///
    /// Serialized as a nested object (rather than two sibling arrays) so that a
    /// client can tell "the coordinator reported health and nothing failed"
    /// from "this coordinator does not report health": an older coordinator
    /// omits the object entirely, and the client decodes that as `None`.
    pub view_health: ViewHealthJson,
}

/// Per-view health for one tick. See [`StepResponse::view_health`].
///
/// `reported` exists because health is not always available. A central tick
/// computes the views here and always has a real report. A **resident** tick is
/// computed on an executor, and whether it has one depends on that executor's
/// tick wire: since IVM-AUD-A5-RESIDENT the v2 result (`IVMD2`) carries the
/// executor's real `degraded_views`/`errored_views`, but an executor still on
/// v1 answers with output deltas only — and during a rolling upgrade that is
/// exactly what a coordinator meets. `reported: false` means "nobody looked",
/// never "nothing failed"; serializing empty vectors as a report would collapse
/// the two.
#[derive(Debug, Serialize, Deserialize)]
pub struct ViewHealthJson {
    /// Whether the engine that ran this tick reported per-view health at all.
    /// When false the two vectors below are empty for lack of a signal, not
    /// because every view is healthy.
    pub reported: bool,
    /// Why health is unavailable, when `reported` is false. Empty otherwise.
    #[serde(default)]
    pub unreported_reason: String,
    /// Views that ran on the O(state) DiffBased path this tick.
    pub degraded_views: Vec<String>,
    /// Views that failed and were skipped this tick.
    pub errored_views: Vec<ViewErrorJson>,
    /// Degraded views the executor's health frame dropped to keep the tick wire
    /// bounded (`krishiv_ivm::MAX_HEALTH_ENTRIES`). Non-zero means
    /// `degraded_views` is a prefix, not the whole list. Always 0 on a tick this
    /// coordinator computed itself.
    #[serde(default)]
    pub degraded_omitted: u32,
    /// Errored views dropped by the same cap.
    #[serde(default)]
    pub errored_omitted: u32,
}

impl ViewHealthJson {
    /// A real report from a tick the coordinator computed itself.
    fn reported(summary: &krishiv_ivm::StepSummary) -> Self {
        Self {
            reported: true,
            unreported_reason: String::new(),
            degraded_views: summary.degraded_views.clone(),
            errored_views: summary
                .errored_views
                .iter()
                .map(|e| ViewErrorJson {
                    view: e.view.clone(),
                    kind: view_error_kind_name(&e.kind).to_owned(),
                    message: e.message.clone(),
                })
                .collect(),
            degraded_omitted: 0,
            errored_omitted: 0,
        }
    }

    /// A real report — one that carries information, as opposed to an absence
    /// of signal.
    ///
    /// Usually relayed from the executor that ran a resident tick. It is *not*
    /// always a relay: `submit_resident_ivm_step`'s empty-input early return
    /// also produces a (default, empty) report, because no view was evaluated
    /// and "nothing failed" is then the coordinator's own knowledge rather
    /// than something it heard. So `reported: true` can flip to `false` on the
    /// next tick of the same job against a v1 executor — the difference is
    /// whether the tick had input, not whether the peer got worse.
    ///
    /// The kinds arrive as snake-case strings from
    /// [`krishiv_ivm::view_error_kind_name`] and are passed through verbatim —
    /// an executor on a newer build may name a kind this coordinator has never
    /// heard of, and mapping it onto a known one would relabel a new failure
    /// mode as an old one.
    fn from_tick_health(health: &krishiv_ivm::TickHealth) -> Self {
        Self {
            reported: true,
            unreported_reason: String::new(),
            degraded_views: health.degraded_views.clone(),
            errored_views: health
                .errored_views
                .iter()
                .map(|e| ViewErrorJson {
                    view: e.view.clone(),
                    kind: e.kind.clone(),
                    message: e.message.clone(),
                })
                .collect(),
            degraded_omitted: health.degraded_omitted,
            errored_omitted: health.errored_omitted,
        }
    }

    /// No signal, and why.
    fn unreported(reason: &str) -> Self {
        Self {
            reported: false,
            unreported_reason: reason.to_owned(),
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
            degraded_omitted: 0,
            errored_omitted: 0,
        }
    }
}

/// Health for a tick that ran on a resident executor.
///
/// `Some` → a real report: either the executor's, over the v2 tick wire, or a
/// locally-known empty one for a tick that evaluated no view (see
/// [`ViewHealthJson::from_tick_health`]).
/// `None` → it does not, and the honest answer names the wire version rather
/// than blaming the protocol as a whole (it was the protocol's fault only until
/// IVM-AUD-A5-RESIDENT).
///
/// A free function so the mapping can be unit-tested directly. It is NOT
/// unreachable through the handler: an earlier version of this comment claimed
/// `test_deps_with_shards` yields `executor_count == 0` so no test could reach
/// the resident arm, and used that to justify unit-level proof only. That was
/// false — `executor_count` comes from `coordinator.executor_snapshots()`, and
/// `coordinator_with_one_executor` has driven this arm end to end since
/// IVM-AUD-DIST-A3. See `a_v2_resident_tick_relays_real_health_through_the_handler`.
///
/// Note also that what selects `Some` vs `None` here is the *tick result's*
/// magic (`IVMD2` carries health, `IVMD1` does not), not the attach-time
/// capability echo — a fact established by revert-proving that test.
fn health_for_resident(health: Option<&krishiv_ivm::TickHealth>) -> ViewHealthJson {
    match health {
        Some(h) => ViewHealthJson::from_tick_health(h),
        None => ViewHealthJson::unreported(
            "this tick ran on a resident executor whose tick wire predates per-view health (pre-IVMD2); its result carries output deltas only",
        ),
    }
}

/// Wire form of `krishiv_ivm::ViewError`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ViewErrorJson {
    pub view: String,
    /// Snake-case name of `krishiv_ivm::ViewErrorKind` — see
    /// [`view_error_kind_name`]. A client that does not recognise the name must
    /// say so rather than substituting a kind it does know.
    pub kind: String,
    pub message: String,
}

/// Wire name for a view-failure kind.
///
/// One definition, in `krishiv-ivm`, shared with the resident tick encoder: a
/// second exhaustive match here would be free to drift, and then the same
/// failure would reach a caller under two different names depending on which
/// route computed the tick.
use krishiv_ivm::view_error_kind_name;

pub async fn api_ivm_step(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<StepResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;

    // Serialize concurrent steps for this job so two simultaneous ticks cannot
    // drain each other's pending or double-advance the tick counter. Per-job,
    // so independent jobs still step in parallel.
    let step_lock = registry.step_lock(&job_id);
    let _guard = step_lock.lock().await;

    // IVM-AUD-DIST-H6: `ensure_ivm_job` ran before the lock, so a `DELETE` that
    // won the race has already removed both the registry entry and the durable
    // snapshot by the time we get here — while `flow` above is still a live
    // `Arc` to the orphaned flow. Without this re-check the handler computed a
    // whole tick on that orphan and only then failed, in `persist_ivm_job`,
    // with a bare 400 (`durable_snapshot` cannot find the job) — expensive,
    // and the wrong status for "this job no longer exists".
    if registry.get(&job_id).is_none() {
        return Err(ivm_not_found(&job_id));
    }

    let executor_count = coordinator
        .read()
        .await
        .executor_snapshots()
        .into_iter()
        .filter(|e| e.state().can_accept_work())
        .count();

    // Phase 57 (AUD-6): single-flow jobs with live executors run RESIDENT —
    // state lives on the executor, the wire carries deltas + a fence only.
    // Partitioned jobs always compute centrally (their shards already run in
    // parallel in-process). Every route is recorded as a queryable dispatch
    // decision; nothing falls back silently.
    //
    // The route also decides where this tick's per-view health comes from
    // (IVM-AUD-API-A5): a central tick computes the views here and its
    // `StepSummary` is the report; a resident tick relays the executor's own
    // health off the v2 tick wire (IVM-AUD-A5-RESIDENT), and says so when it
    // met an executor still on v1.
    let (summary, health) = if executor_count > 0 && matches!(flow, crate::ivm::IvmJob::Single(_)) {
        let crate::ivm::IvmJob::Single(inner_flow) = &flow else {
            unreachable!("matched above")
        };
        match submit_resident_ivm_step(&coordinator, &registry, inner_flow, &job_id).await {
            Ok((sum, tick_health)) => {
                let health = health_for_resident(tick_health.as_ref());
                (sum, health)
            }
            Err(step_err) => {
                // Recorded central fallback: submit_resident_ivm_step re-feeds
                // pending before failing, so this tick observes the same input.
                // The resident flow (if any) is now considered detached — the
                // next step re-attaches from the coordinator's state mirror.
                tracing::warn!(
                    job_id = %job_id,
                    error = %step_err,
                    "IVM resident dispatch failed; computing this tick centrally \
                     (recorded; job will re-attach)"
                );
                let tick = flow.tick().unwrap_or(0);
                registry.update_dispatch(&job_id, |d| {
                    d.attached = false;
                    d.last = Some(crate::ivm::IvmDispatchRecord {
                        tick,
                        mode: "central-fallback".to_owned(),
                        reason: step_err.clone(),
                        at_unix_ms: krishiv_common::async_util::unix_now_ms(),
                    });
                });
                let sum = central_step_with_timeout(&flow, &job_id).await?;
                let health = ViewHealthJson::reported(&sum);
                (sum, health)
            }
        }
    } else {
        let mode = if matches!(flow, crate::ivm::IvmJob::Partitioned(_)) {
            "central-partitioned"
        } else {
            "central-no-executors"
        };
        let tick = flow.tick().unwrap_or(0);
        registry.update_dispatch(&job_id, |d| {
            d.last = Some(crate::ivm::IvmDispatchRecord {
                tick,
                mode: mode.to_owned(),
                reason: String::new(),
                at_unix_ms: krishiv_common::async_util::unix_now_ms(),
            });
        });
        let sum = central_step_with_timeout(&flow, &job_id).await?;
        let health = ViewHealthJson::reported(&sum);
        (sum, health)
    };

    let tick = flow.tick().unwrap_or(0);
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(StepResponse {
        active_views: summary.active_views,
        total_output_rows: summary.total_output_rows,
        tick,
        view_health: health,
    }))
}

/// Default timeout for a dispatched IVM fragment before falling back to
/// central compute. Override with `KRISHIV_IVM_DISPATCH_TIMEOUT_SECS`.
const DEFAULT_IVM_DISPATCH_TIMEOUT_SECS: u64 = 300;

/// Pure policy for [`ivm_dispatch_timeout_secs`], split out so it is testable
/// without touching the process environment.
///
/// A value that is absent, unparseable or zero falls back to the default: zero
/// would make every tick time out instantly, which is a worse failure than the
/// misconfiguration it came from.
fn resolve_ivm_dispatch_timeout_secs(env_override: Option<&str>) -> u64 {
    env_override
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_IVM_DISPATCH_TIMEOUT_SECS)
}

/// How long one IVM tick may run before it is abandoned.
///
/// IVM-AUD-DIST-H7: this was a hardcoded 300 s with no way to change it, and
/// it bounds more than the tick — `DELETE /jobs/{id}` takes the same per-job
/// step lock, so a job whose tick is wedged cannot be deleted for up to this
/// long either. An operator with a workload that legitimately needs longer (or
/// a control plane that wants deletion to give up sooner) had no lever at all.
fn ivm_dispatch_timeout_secs() -> u64 {
    resolve_ivm_dispatch_timeout_secs(
        std::env::var("KRISHIV_IVM_DISPATCH_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

/// Run one **central** (in-coordinator) IVM tick under the same safety timeout
/// the resident-dispatch path already enforces (#224 B).
///
/// The central path is the fallback taken when no executor can accept work or a
/// resident dispatch failed. Before this it ran unbounded, so a pathologically
/// large delta could block the HTTP handler — and, worse, hold the per-job step
/// lock — indefinitely, wedging every subsequent tick and deletion for that job.
/// The bound matches [`IVM_DISPATCH_TIMEOUT_SECS`] so both step paths behave
/// identically. A timeout surfaces as `503 Service Unavailable` (retryable),
/// never a silent hang.
async fn central_step_with_timeout(
    flow: &crate::ivm::IvmJob,
    job_id: &str,
) -> Result<krishiv_ivm::StepSummary, StatusCode> {
    let timeout_secs = ivm_dispatch_timeout_secs();
    match tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        flow.step_datafusion(),
    )
    .await
    {
        Ok(Ok(summary)) => Ok(summary),
        Ok(Err(e)) => {
            tracing::error!(job_id, error = %e, "IVM central step failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
        Err(_elapsed) => {
            tracing::error!(job_id, timeout_secs, "IVM central step timed out");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

/// Submit one IVM fragment as a scheduler batch job, await its terminal state,
/// and return the inline result blob (if any).
///
/// The fragment is wrapped in the Phase-52 typed task-fragment envelope
/// (`ExecutionKind::DeltaBatch`) so durable profiles accept it.
async fn run_ivm_fragment_job(
    coordinator: &SharedCoordinator,
    fragment_body: String,
    label: &str,
) -> Result<Option<Vec<u8>>, String> {
    let fragment = krishiv_plan::task_fragment::TypedTaskFragment::new(
        krishiv_plan::ExecutionKind::DeltaBatch,
        fragment_body,
    )
    .encode()
    .map_err(|e| format!("encode typed fragment: {e}"))?;

    let sched_job_id = JobId::try_new(format!(
        "{label}-{}",
        krishiv_common::async_util::unix_now_ms()
    ))
    .map_err(|e| e.to_string())?;
    let task = TaskSpec::new(
        TaskId::try_new("task-ivm").map_err(|e| e.to_string())?,
        fragment,
    );
    let stage = StageSpec::new(
        StageId::try_new("stage-ivm").map_err(|e| e.to_string())?,
        label,
    )
    .with_task(task);
    let spec = JobSpec::new(sched_job_id.clone(), label, JobKind::Batch).with_stage(stage);

    let notify = {
        let mut coord = coordinator.write().await;
        coord.submit_job(spec).map_err(|e| e.to_string())?;
        coord.notify().clone()
    };

    // Poll until terminal (bounded by `ivm_dispatch_timeout_secs`). The recheck
    // right before sleeping closes the missed-Notify gap (H-20).
    let timeout_secs = ivm_dispatch_timeout_secs();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    let succeeded = loop {
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                job_id = %sched_job_id,
                timeout_secs,
                "IVM dispatch job timed out"
            );
            break false;
        }
        let state = {
            let coord = coordinator.read().await;
            coord
                .job_snapshot(&sched_job_id)
                .map(|s| s.state())
                .unwrap_or(JobState::Failed)
        };
        match state {
            JobState::Succeeded => break true,
            JobState::Failed | JobState::Cancelled => break false,
            _ => {
                let recheck = {
                    let coord = coordinator.read().await;
                    coord
                        .job_snapshot(&sched_job_id)
                        .map(|s| s.state())
                        .unwrap_or(JobState::Failed)
                };
                if !matches!(
                    recheck,
                    JobState::Queued | JobState::Accepted | JobState::Planning | JobState::Running
                ) {
                    continue;
                }
                let state_changed = notify.notified();
                tokio::select! {
                    _ = state_changed => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        }
    };

    if !succeeded {
        let _ = coordinator.write().await.cancel_job(&sched_job_id);
        return Err(format!("{label} job {sched_job_id} did not succeed"));
    }

    let blob = {
        let mut coord = coordinator.write().await;
        coord
            .take_job_inline_results(&sched_job_id)
            .and_then(|mut v| v.pop())
    };
    Ok(blob)
}

/// Phase 57 (AUD-6): dispatch one IVM tick to a **resident** executor flow.
///
/// State ships to the executor ONCE, at attach; every tick afterwards the
/// wire carries only the input deltas plus a fence, and the executor returns
/// per-view **output deltas** — never full snapshots. The old 16 MiB
/// `MAX_IVM_OFFLOAD_STATE_BYTES` cliff is gone: large state is exactly what
/// residency is for.
///
/// The coordinator stays authoritative by *mirroring* the tick: it applies
/// the same input deltas to its source snapshots and the returned output
/// deltas to its view state (`apply_remote_tick`), so central fallback and
/// re-attach (both from this mirror) are always correct. The fence makes
/// placement drift self-healing: a tick that lands on an executor without
/// the flow (or replays after a retry) errors instead of corrupting state,
/// and the caller re-attaches.
async fn submit_resident_ivm_step(
    coordinator: &SharedCoordinator,
    registry: &SharedIvmJobRegistry,
    flow: &std::sync::Arc<IncrementalFlow>,
    ivm_job_id: &str,
) -> Result<(krishiv_ivm::StepSummary, Option<krishiv_ivm::TickHealth>), String> {
    // 1. Drain pending locally — never lost: re-fed on any failure below.
    let local_pending = flow.take_pending().map_err(|e| e.to_string())?;
    let dispatch_deltas = coalesce_pending(local_pending.clone()).map_err(|e| e.to_string())?;

    // Nothing to compute: advance the tick structurally and return. No view is
    // evaluated, so "nothing failed" is a fact rather than an absence of signal
    // — the same answer a central tick with no input gives.
    if dispatch_deltas.is_empty() {
        flow.step_with(|_| Ok(HashMap::new()))
            .map_err(|e| e.to_string())?;
        return Ok((
            krishiv_ivm::StepSummary::default(),
            Some(krishiv_ivm::TickHealth::default()),
        ));
    }

    let refeed = |e: String| -> String {
        let _ = flow.re_feed(local_pending.clone());
        e
    };

    // 2. Attach if needed: ship the full state mirror once.
    let mut disp = registry.dispatch_state(ivm_job_id);
    if !disp.attached {
        let state_bytes = flow
            .checkpoint_full()
            .map_err(|e| refeed(format!("checkpoint_full: {e}")))?;
        let specs = flow.view_specs().map_err(|e| refeed(e.to_string()))?;
        let attach =
            krishiv_ivm::encode_ivm_attach_fragment(ivm_job_id, &specs, &state_bytes, disp.fence)
                .map_err(|e| refeed(e.to_string()))?;
        let echo = run_ivm_fragment_job(coordinator, attach, "ivm-attach")
            .await
            .map_err(refeed)?;
        // IVM-AUD-INT-F19: the attach reply is the wire negotiation. No blob
        // (an executor predating the echo) decodes fail-closed to the legacy
        // JSON tick + v1 result, which every executor understands.
        let negotiated = krishiv_ivm::decode_attach_echo(echo.as_deref());
        // The executor's flow owns the live accumulators from here on; the
        // coordinator's cached plans are stale and must never apply another
        // delta (a later central fallback rebuilds + reseeds from the mirror).
        // IVM-AUD-INT-F10: this used to drop the drained deltas on the floor —
        // `refeed` was in scope and simply not applied, so a failure here lost
        // every delta this tick had taken custody of.
        flow.invalidate_view_plans()
            .map_err(|e| refeed(format!("invalidate_view_plans: {e}")))?;
        registry.update_dispatch(ivm_job_id, |d| {
            d.attached = true;
            d.wire = negotiated;
        });
        disp.attached = true;
        disp.wire = negotiated;
        tracing::info!(
            job_id = %ivm_job_id,
            state_bytes = state_bytes.len(),
            fence = disp.fence,
            binary_deltas = negotiated.binary_input_deltas,
            tick_health = negotiated.tick_health,
            "IVM job attached to resident executor flow"
        );
    }

    // 3. Tick: deltas + fence only (O(Δ) wire, both directions).
    //
    // The payload dialect is whatever the attach echo said this executor reads,
    // unless an operator has forced the legacy wire. Sending binary to an
    // executor that cannot read it fails the tick, detaches the job and costs a
    // full `checkpoint_full` re-attach, so the default when we know nothing is
    // the one every executor understands.
    let binary_deltas = disp.wire.binary_input_deltas && !legacy_tick_wire_forced();
    let fence = disp.fence + 1;
    let tick_fragment =
        krishiv_ivm::encode_ivm_tick_fragment(ivm_job_id, &dispatch_deltas, fence, binary_deltas)
            .map_err(|e| refeed(e.to_string()))?;
    let blob = run_ivm_fragment_job(coordinator, tick_fragment, "ivm-tick")
        .await
        .map_err(refeed)?
        .ok_or_else(|| refeed("ivm-tick produced no inline result blob".to_owned()))?;
    let result = krishiv_ivm::decode_tick_result(&blob)
        .map_err(|e| refeed(format!("decode tick result: {e}")))?;
    let view_deltas = result.view_deltas;
    // A tick negotiated as health-reporting that answers without health means
    // the pod behind this job was replaced between the attach and the tick
    // (IVM-AUD-DIST-A2: there is no placement pin). Correct — the answer is
    // still a valid v1 result — but worth saying out loud, because the symptom
    // downstream is `reported: false` on a cluster the operator believes is
    // fully upgraded.
    if disp.wire.tick_health && binary_deltas && result.health.is_none() {
        tracing::warn!(
            job_id = %ivm_job_id,
            "resident tick answered on the v1 wire though this job negotiated v2 at \
             attach; the tick likely landed on a different executor than the attach"
        );
    }

    // 4. Mirror the tick on the coordinator's authoritative state.
    //
    // IVM-AUD-INT-F10: this path skipped `refeed` too, and the consequence was
    // worse than a lost delta — the executor had applied the tick and the
    // coordinator had not, so the two diverged and the next tick's fence check
    // was the first thing to notice. `apply_remote_tick` is now all-or-nothing
    // (see its docs), which is what makes re-feeding here safe rather than a
    // double-apply.
    let summary = flow
        .apply_remote_tick(local_pending.clone(), view_deltas)
        .map_err(|e| refeed(format!("apply_remote_tick: {e}")))?;
    // `summary` is the coordinator MIRROR's view of the tick — it applied
    // deltas, it did not evaluate any view SQL, so its health vectors are empty
    // by construction and mean nothing. The health that means something is the
    // executor's, and it rides in the tick result.
    let tick = flow.tick().unwrap_or(0);
    registry.update_dispatch(ivm_job_id, |d| {
        d.fence = fence;
        d.last = Some(crate::ivm::IvmDispatchRecord {
            tick,
            mode: "resident".to_owned(),
            reason: String::new(),
            at_unix_ms: krishiv_common::async_util::unix_now_ms(),
        });
    });
    Ok((summary, result.health))
}

/// `KRISHIV_IVM_LEGACY_TICK_WIRE=1` forces the pre-IVMD2 JSON tick payload even
/// when the executor said it reads binary.
///
/// The operator escape hatch for a wire change: a coordinator can be put back
/// on the dialect every executor has always understood without a rollback.
/// Costs the 25% wire saving and the per-view health (a JSON tick is answered
/// in v1), which is the point — it is the old behaviour, exactly.
fn legacy_tick_wire_forced() -> bool {
    krishiv_common::env_registry::truthy_env("KRISHIV_IVM_LEGACY_TICK_WIRE")
}

// ── GET /api/v1/ivm/jobs/{job_id}/dispatch ───────────────────────────────────

/// Queryable dispatch decision for a job (Phase 57 quality gate: no silent
/// fallbacks — the last route every tick took is recorded here).
#[derive(Debug, Serialize)]
pub struct DispatchStateResponse {
    pub attached: bool,
    pub fence: u64,
    pub last: Option<crate::ivm::IvmDispatchRecord>,
}

pub async fn api_ivm_dispatch_state(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<DispatchStateResponse>, StatusCode> {
    // Rehydrates like every other job-scoped handler. The *record* it returns
    // is process-local by nature — `attached` and `fence` describe a resident
    // flow this coordinator attached — so after a restart the honest answer is
    // a freshly-defaulted "not attached", which is exactly what rehydration
    // produces. 404 would instead claim the job does not exist.
    ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let d = registry.dispatch_state(&job_id);
    Ok(Json(DispatchStateResponse {
        attached: d.attached,
        fence: d.fence,
        last: d.last,
    }))
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/snap ─────────────────────

#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    /// Base64-encoded Arrow IPC bytes of a `DeltaBatch` (all +1 weights).
    pub snapshot_ipc_b64: Option<String>,
    pub num_rows: usize,
    /// Whether the view was registered with `is_materialized`.
    ///
    /// Without this, "you never asked for materialization" and "materialized
    /// but currently empty" were byte-identical responses
    /// (`{"snapshot_ipc_b64": null, "num_rows": 0}`), because
    /// `RegisterViewRequest::is_materialized` is `#[serde(default)]` = false.
    /// A caller hitting the default reads a correct engine as a broken one —
    /// which is exactly what happened while building the Phase 62 soak.
    pub materialized: bool,
}

pub async fn api_ivm_snapshot(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, StatusCode> {
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let materialized = flow.view_is_materialized(&view_name);
    let rb_opt = flow.snapshot(&view_name).map_err(ivm_err)?;
    match rb_opt {
        None => Ok(Json(SnapshotResponse {
            snapshot_ipc_b64: None,
            num_rows: 0,
            materialized,
        })),
        Some(rb) => {
            let num_rows = rb.num_rows();
            let delta = DeltaBatch::from_inserts(rb).map_err(ivm_err)?;
            let ipc = serialize_delta_batch(&delta).map_err(ivm_err)?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(SnapshotResponse {
                snapshot_ipc_b64: Some(b64),
                num_rows,
                materialized,
            }))
        }
    }
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/output ───────────────────

/// Query string of `GET .../views/{view}/output`.
#[derive(Debug, Deserialize, Default)]
pub struct ViewOutputQuery {
    /// Serve the held delta only if it was published *after* this tick.
    ///
    /// IVM-AUD-INT-F5. Without it, this endpoint is a non-consuming peek at a
    /// coalescing watch: polling twice between ticks hands back the same delta
    /// twice, and a consumer with no way to tell them apart double-applies it.
    /// Pass the `tick` from the previous response and a repeat read answers
    /// `delta_ipc_b64: null` instead.
    #[serde(default)]
    pub since_tick: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ViewOutputResponse {
    /// Base64-encoded Arrow IPC of the held delta. `None` when the view has
    /// published nothing yet, or when `since_tick` says the caller already has
    /// this one.
    pub delta_ipc_b64: Option<String>,
    pub num_rows: usize,
    /// The flow tick this delta was published at; `None` when there is none.
    ///
    /// IVM-AUD-PART-11: this endpoint served a partitioned job's merged
    /// per-shard deltas as "the latest delta" with nothing saying which tick
    /// they belonged to — and the shards' coalescing watches meant they need
    /// not have belonged to the same one. The merge now only combines shards
    /// that published at the same tick, and this reports which.
    ///
    /// Reported even when `delta_ipc_b64` is `None` because of `since_tick`, so
    /// the caller can carry its cursor forward.
    pub tick: Option<u64>,
    /// Total rows this view has ever published (inserts + retractions), summed
    /// across shards.
    ///
    /// IVM-AUD-INT-F5, the detection half. The channel behind this endpoint is
    /// a **coalescing watch**: it holds one value per view, so a consumer
    /// polling slower than `/step` loses every delta but the newest, and used
    /// to lose them with no trace. This counter makes the loss measurable:
    /// remember it alongside the delta you were served, and on the next poll
    ///
    /// ```text
    /// rows_lost = published_rows_total - previous_published_rows_total - num_rows
    /// ```
    ///
    /// is the number of published rows that passed through the watch and were
    /// overwritten. Zero means nothing was missed. **It does not recover them**
    /// — this endpoint is still lossy by construction, and making it lossless
    /// needs the broadcast stream (`IncrementalFlow::view_output_stream`)
    /// exposed over HTTP, which is not done.
    ///
    /// Two things it is not: (1) monotone across a restore — `restore`/
    /// `restore_full` reset the counters (IVM-AUD-CORE-27), so a consumer must
    /// treat a *decrease* as "cursor invalid, resync from `/snap`", not as
    /// negative loss; (2) exact for a partitioned job — the served delta merges
    /// only the shards that published at `tick`, while this counts every
    /// shard, so a job whose shards publish at different ticks reports
    /// nonzero loss for rows that are merely still in flight.
    pub published_rows_total: u64,
}

pub async fn api_ivm_view_output(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, view_name)): Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<ViewOutputQuery>,
) -> Result<Json<ViewOutputResponse>, StatusCode> {
    let job = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let published_rows_total = job
        .view_delta_stats(&view_name)
        .map_err(ivm_err)?
        .map(|s| s.rows_inserted_total + s.rows_retracted_total)
        .unwrap_or(0);
    // Peek the latest output delta (for a partitioned job, the shards that
    // published at the newest tick — see `view_output_peek_at_tick`).
    match job.view_output_peek_at_tick(&view_name).map_err(ivm_err)? {
        None => Ok(Json(ViewOutputResponse {
            delta_ipc_b64: None,
            num_rows: 0,
            tick: None,
            published_rows_total,
        })),
        // Already delivered: the watch is non-consuming, so without this the
        // same delta comes back on every poll (IVM-AUD-INT-F5).
        Some((tick, _)) if query.since_tick.is_some_and(|since| tick <= since) => {
            Ok(Json(ViewOutputResponse {
                delta_ipc_b64: None,
                num_rows: 0,
                tick: Some(tick),
                published_rows_total,
            }))
        }
        Some((tick, delta)) => {
            let num_rows = delta.num_rows();
            let ipc = serialize_delta_batch(&delta).map_err(ivm_err)?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(ViewOutputResponse {
                delta_ipc_b64: Some(b64),
                num_rows,
                tick: Some(tick),
                published_rows_total,
            }))
        }
    }
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/stats ───────────────────

/// Lightweight per-view maintenance stats (#94): row count plus cumulative
/// and last-tick insert/retract counters. Unlike `/snap` this never
/// serializes the snapshot, so pollers (the platform freshness sampler) can
/// hit it every few seconds regardless of table size. Counters are logical
/// multiset changes and reset on process restart — a poller derives rates by
/// diffing consecutive reads and must tolerate the counters going backwards.
#[derive(Debug, Serialize)]
pub struct ViewStatsResponse {
    pub num_rows: usize,
    pub rows_inserted_total: u64,
    pub rows_retracted_total: u64,
    pub last_tick_inserts: u64,
    pub last_tick_retracts: u64,
}

pub async fn api_ivm_view_stats(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<ViewStatsResponse>, StatusCode> {
    let job = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    // 404 for a view that isn't registered (matches /debug-info semantics).
    job.view_spec(&view_name)
        .map_err(ivm_err)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let num_rows = job
        .snapshot(&view_name)
        .map_err(ivm_err)?
        .map(|rb| rb.num_rows())
        .unwrap_or(0);
    let stats = job
        .view_delta_stats(&view_name)
        .map_err(ivm_err)?
        .unwrap_or_default();
    Ok(Json(ViewStatsResponse {
        num_rows,
        rows_inserted_total: stats.rows_inserted_total,
        rows_retracted_total: stats.rows_retracted_total,
        last_tick_inserts: stats.last_tick_inserts,
        last_tick_retracts: stats.last_tick_retracts,
    }))
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/debug-info ──────────────

#[derive(Debug, Serialize)]
pub struct ViewDebugInfo {
    pub is_materialized: bool,
    pub has_snapshot: bool,
    pub snapshot_num_rows: usize,
    pub has_last_output: bool,
    pub last_output_num_rows: usize,
    /// AUD-9 (loud degradation): `true` when the view executes O(Δ) incrementally,
    /// `false` when it fell back to full recompute (or has not been planned yet).
    pub plan_incremental: bool,
    /// Human-readable explanation of the plan choice — makes a silent
    /// full-recompute fallback visible and actionable.
    pub plan_reason: String,
}

pub async fn api_ivm_view_debug_info(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<ViewDebugInfo>, StatusCode> {
    let job = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    // is_materialized from spec
    let is_materialized = job
        .view_spec(&view_name)
        .map_err(ivm_err)?
        .ok_or_else(|| ivm_err(format!("view {view_name} not found")))?
        .is_materialized;
    let snapshot = job.snapshot(&view_name).map_err(ivm_err)?;
    let has_snapshot = snapshot.is_some();
    let snapshot_num_rows = snapshot.map(|s| s.num_rows()).unwrap_or(0);
    let last_output = job.view_output_peek(&view_name).map_err(ivm_err)?;
    let has_last_output = last_output.is_some();
    let last_output_num_rows = last_output.map(|d| d.num_rows()).unwrap_or(0);
    let (plan_incremental, plan_reason) = job
        .view_plan_classification(&view_name)
        .map_err(ivm_err)?
        .unwrap_or((false, "view not registered".to_string()));
    Ok(Json(ViewDebugInfo {
        is_materialized,
        has_snapshot,
        snapshot_num_rows,
        has_last_output,
        last_output_num_rows,
        plan_incremental,
        plan_reason,
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/checkpoint ────────────────────────────────

#[derive(Debug, Serialize)]
pub struct CheckpointResponse {
    /// Base64-encoded checkpoint bytes (Arrow IPC length-prefix format).
    pub checkpoint_b64: String,
}

pub async fn api_ivm_checkpoint(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<CheckpointResponse>, StatusCode> {
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    // Full checkpoint (sources + view baselines): the source-only `checkpoint`
    // loses view state across a restart, which broke IVM recovery (G6/F4).
    let bytes = flow.checkpoint_full().map_err(ivm_err)?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
    Ok(Json(CheckpointResponse {
        checkpoint_b64: b64,
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/restore ───────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RestoreRequest {
    pub checkpoint_b64: String,
}

#[derive(Debug, Serialize)]
pub struct RestoreResponse {
    pub success: bool,
    /// Whether the restored state was written anywhere it survives this
    /// process (IVM-AUD-DIST-C5). `success: true, durable: false` means the
    /// rewind happened in memory and the next restart undoes it.
    pub durable: bool,
}

pub async fn api_ivm_restore(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<RestoreRequest>,
) -> Result<Json<RestoreResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // IVM-AUD-DIST-B1: a rewind must not interleave with a tick. Only `/step`
    // and `DELETE` took this lock, so a restore landing mid-tick let the
    // in-flight tick apply its input deltas and the executor's output deltas
    // (`apply_remote_tick`) on top of the state we had just replaced — half of
    // the rewind silently undone, answered `{"success": true}`. Take the same
    // per-job lock a tick takes, so the two orders are the only two outcomes.
    let _step_guard = registry.step_lock(&job_id).lock_owned().await;
    // Matches `api_ivm_checkpoint`'s full checkpoint (sources + view baselines).
    flow.restore_full(&bytes).map_err(ivm_err)?;
    // Persist, like every other handler that changes authoritative state
    // (`register_view`, `drop_view`, `step`). Without this a restore lived only
    // in memory: answer `{"success": true}`, restart before the next `/step`,
    // and `ensure_ivm_job` rehydrates the *pre-restore* snapshot — silently
    // undoing the rewind the operator was told had happened.
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(RestoreResponse {
        success: true,
        durable: ivm_writes_are_durable(&coordinator).await,
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/checkpoint-delta ──────────────────────────

#[derive(Debug, Serialize)]
pub struct CheckpointDeltaResponse {
    /// Base64-encoded delta checkpoint bytes.
    pub checkpoint_delta_b64: String,
}

pub async fn api_ivm_checkpoint_delta(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<CheckpointDeltaResponse>, StatusCode> {
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let bytes = flow.checkpoint_delta().map_err(ivm_err)?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
    Ok(Json(CheckpointDeltaResponse {
        checkpoint_delta_b64: b64,
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/restore-delta ─────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RestoreDeltaRequest {
    pub checkpoint_delta_b64: String,
}

#[derive(Debug, Serialize)]
pub struct RestoreDeltaResponse {
    pub success: bool,
    /// See [`RestoreResponse::durable`] (IVM-AUD-DIST-C5).
    pub durable: bool,
}

pub async fn api_ivm_restore_delta(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<RestoreDeltaRequest>,
) -> Result<Json<RestoreDeltaResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_delta_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // Same reason as `api_ivm_restore` (IVM-AUD-DIST-B1).
    let _step_guard = registry.step_lock(&job_id).lock_owned().await;
    flow.restore_delta(&bytes).map_err(ivm_err)?;
    // Same reason as `api_ivm_restore`.
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(RestoreDeltaResponse {
        success: true,
        durable: ivm_writes_are_durable(&coordinator).await,
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-bridge ───────

#[derive(Debug, Deserialize)]
pub struct StreamBridgeRequest {
    /// Base64-encoded Arrow IPC bytes for one or more RecordBatches (full snapshot).
    pub snapshot_ipc_b64: String,
}

#[derive(Debug, Serialize)]
pub struct StreamBridgeResponse {
    pub success: bool,
}

pub async fn api_ivm_stream_bridge(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<StreamBridgeRequest>,
) -> Result<Json<StreamBridgeResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    ensure_pending_headroom(&registry, &flow, &job_id)?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.snapshot_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // Decode Arrow IPC stream to RecordBatches.
    let batches = {
        use arrow::ipc::reader::StreamReader;
        let cursor = std::io::Cursor::new(&ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| ivm_err(format!("IPC stream open: {e}")))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ivm_err(format!("IPC stream read: {e}")))?
    };
    flow.feed_snapshot(&source_name, &batches)
        .map_err(ivm_err)?;
    Ok(Json(StreamBridgeResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/vector-views ───────────────────────────────

use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct RegisterVectorViewRequest {
    pub view_name: String,
    pub id_column: String,
    pub vector_column: String,
    /// Sink type: currently only "in_memory" is supported via HTTP.
    #[serde(default = "default_sink_type")]
    pub sink_type: String,
}

fn default_sink_type() -> String {
    "in_memory".to_string()
}

#[derive(Debug, Serialize)]
pub struct RegisterVectorViewResponse {
    pub success: bool,
    pub view_name: String,
    /// Number of maintenance tasks started (one per shard).
    pub shards: usize,
}

/// Register a vector view on an IVM job.
///
/// IVM-AUD-DIST-H3: this used to build an `InMemoryVectorSink`, hand it to
/// detached per-shard tasks and drop the only `Arc` before returning — so
/// nothing could ever read what was written (`IvmVectorSink` is write-only),
/// calling it N times with the same view name left N×shards permanent tasks
/// with no way to stop any of them, and deleting the job stopped none. The
/// sink and the task handles now live in the job registry, which makes the
/// contents readable through `GET .../vector-views`, makes a duplicate name a
/// 400 instead of a silent leak, and makes `DELETE /jobs/{id}` stop the tasks.
pub async fn api_ivm_register_vector_view(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<RegisterVectorViewRequest>,
) -> Result<Json<RegisterVectorViewResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    use krishiv_ivm::VectorViewSpec;

    let job = ensure_ivm_job(&registry, &coordinator, &job_id).await?;

    if body.sink_type != "in_memory" {
        return Err(ivm_err(format!(
            "unsupported sink_type '{}'; only 'in_memory' is supported via HTTP",
            body.sink_type
        )));
    }

    let sink = krishiv_ivm::InMemoryVectorSink::new();
    let spec = VectorViewSpec {
        view_name: body.view_name.clone(),
        id_column: body.id_column.clone(),
        vector_column: body.vector_column.clone(),
        sink: Arc::clone(&sink) as Arc<dyn krishiv_ivm::IvmVectorSink>,
    };

    // One maintenance task per shard, all writing the shared sink.
    let handles = job.spawn_vector_views(spec).map_err(ivm_err)?;
    let shards = handles.len();
    registry
        .register_vector_view(
            &job_id,
            RegisteredVectorView::new(
                body.view_name.clone(),
                body.id_column,
                body.vector_column,
                body.sink_type,
                sink,
                handles,
            ),
        )
        .map_err(ivm_err)?;

    Ok(Json(RegisterVectorViewResponse {
        success: true,
        view_name: body.view_name,
        shards,
    }))
}

// ── GET /api/v1/ivm/jobs/{job_id}/vector-views ────────────────────────────────

/// One registered vector view, with the health of its maintenance tasks.
#[derive(Debug, Serialize)]
pub struct VectorViewSummary {
    pub view_name: String,
    pub id_column: String,
    pub vector_column: String,
    pub sink_type: String,
    /// Maintenance tasks (one per shard).
    pub shards: usize,
    /// Points currently in the sink.
    pub points: usize,
    /// True when any shard's index is known to no longer match the view — a
    /// lost delta or a lagging subscriber. The index must be rebuilt.
    pub diverged: bool,
    /// Per-shard counters: applied / upserted / deleted / errors / missed, plus
    /// the last error and the reason a stopped task stopped.
    pub shard_status: Vec<krishiv_ivm::VectorViewStatus>,
}

#[derive(Debug, Serialize)]
pub struct ListVectorViewsResponse {
    pub job_id: String,
    pub vector_views: Vec<VectorViewSummary>,
}

/// List a job's vector views and the health of their maintenance tasks.
///
/// This is the operator-visible surface IVM-AUD-PART-19 / PART-20 / DIST-H3
/// were missing: before it, a sink failure was a log line, a lagging
/// subscriber was a log line, and the index contents were unreachable.
pub async fn api_ivm_list_vector_views(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
) -> Result<Json<ListVectorViewsResponse>, StatusCode> {
    if registry.get(&job_id).is_none() {
        return Err(ivm_not_found(&job_id));
    }
    let vector_views = registry.map_vector_views(&job_id, |v| VectorViewSummary {
        view_name: v.view_name.clone(),
        id_column: v.id_column.clone(),
        vector_column: v.vector_column.clone(),
        sink_type: v.sink_type.clone(),
        shards: v.shards(),
        points: v.points(),
        diverged: v.diverged(),
        shard_status: v.shard_status(),
    });
    Ok(Json(ListVectorViewsResponse {
        job_id,
        vector_views,
    }))
}

// ── DELETE /api/v1/ivm/jobs/{job_id}/vector-views/{view_name} ─────────────────

#[derive(Debug, Serialize)]
pub struct DeleteVectorViewResponse {
    pub deleted: bool,
}

/// Stop a vector view's maintenance tasks and forget its sink.
pub async fn api_ivm_delete_vector_view(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<DeleteVectorViewResponse>, StatusCode> {
    ensure_ivm_leader(&coordinator).await?;
    Ok(Json(DeleteVectorViewResponse {
        deleted: registry.delete_vector_view(&job_id, &view_name),
    }))
}

// ── Router builder ────────────────────────────────────────────────────────────

use axum::Router;
use axum::routing::{delete, get, post};

/// Build the IVM sub-router with all endpoints wired up.
///
/// The returned `Router<()>` has combined `IvmRouterState` baked in and can
/// be merged into the main coordinator router.
pub fn ivm_router(state: IvmRouterState) -> Router<()> {
    Router::new()
        // Unified submit endpoint — dispatches by `kind` field.
        .route(
            "/api/v1/jobs",
            post(crate::unified_jobs_http::api_unified_submit),
        )
        .route(
            "/api/v1/ivm/jobs",
            post(api_ivm_create_job).get(api_ivm_list_jobs),
        )
        .route("/api/v1/ivm/jobs/{job_id}", delete(api_ivm_delete_job))
        .route(
            "/api/v1/ivm/jobs/{job_id}/views",
            post(api_ivm_register_view),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}",
            delete(api_ivm_drop_view),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/feed",
            post(api_ivm_feed_source),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-bridge",
            post(api_ivm_stream_bridge),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-delta",
            post(api_ivm_feed_stream_delta),
        )
        .route("/api/v1/ivm/jobs/{job_id}/step", post(api_ivm_step))
        .route(
            "/api/v1/ivm/jobs/{job_id}/dispatch",
            get(api_ivm_dispatch_state),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}/snap",
            get(api_ivm_snapshot),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}/output",
            get(api_ivm_view_output),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}/stats",
            get(api_ivm_view_stats),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}/debug-info",
            get(api_ivm_view_debug_info),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/checkpoint",
            post(api_ivm_checkpoint),
        )
        .route("/api/v1/ivm/jobs/{job_id}/restore", post(api_ivm_restore))
        .route(
            "/api/v1/ivm/jobs/{job_id}/checkpoint-delta",
            post(api_ivm_checkpoint_delta),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/restore-delta",
            post(api_ivm_restore_delta),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/vector-views",
            post(api_ivm_register_vector_view).get(api_ivm_list_vector_views),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/vector-views/{view_name}",
            delete(api_ivm_delete_vector_view),
        )
        // IVM feed / checkpoint / restore / snapshot carry Arrow IPC batches of
        // real user data (base64), which routinely exceed axum's 2 MiB default
        // request-body cap — a modest 500k-row delta already trips it with
        // "413 Payload Too Large". Raise the cap to 512 MiB so realistic
        // incremental workloads and state checkpoints go through; this is a
        // data-plane router, not a control endpoint.
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::coordinator_daemon::PROTECTED_HTTP_BODY_LIMIT_BYTES,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    /// T11 (IVM-AUD-A5-RESIDENT). A resident tick whose executor sent health
    /// must be reported as a real report — the whole point of widening the
    /// wire. Extracted as a pure function so the mapping can be tested
    /// directly — not because the handler is unreachable, which an earlier
    /// version of this comment wrongly asserted. The handler-level cover is
    /// `a_v2_resident_tick_relays_real_health_through_the_handler`.
    #[test]
    fn health_for_resident_reports_when_the_executor_sent_health() {
        let health = krishiv_ivm::TickHealth {
            degraded_views: vec!["slow".into()],
            errored_views: vec![krishiv_ivm::WireViewError {
                view: "broken".into(),
                kind: "view_sql".into(),
                message: "column not found".into(),
            }],
            degraded_omitted: 0,
            errored_omitted: 0,
        };
        let h = super::health_for_resident(Some(&health));
        assert!(h.reported, "the executor did report; say so");
        assert!(h.unreported_reason.is_empty());
        assert_eq!(h.degraded_views, vec!["slow".to_string()]);
        let e = h.errored_views.iter().find(|e| e.view == "broken").unwrap();
        assert_eq!(e.kind, "view_sql");
        assert_eq!(e.message, "column not found");
    }

    /// T12. The other direction, and the honesty half: an executor still on the
    /// v1 tick wire produces `reported: false` with a reason that names the
    /// wire version rather than blaming the protocol (which stopped being at
    /// fault when A5-RESIDENT was fixed).
    #[test]
    fn health_for_resident_stays_unreported_on_a_v1_executor() {
        let h = super::health_for_resident(None);
        assert!(!h.reported);
        assert!(h.degraded_views.is_empty() && h.errored_views.is_empty());
        assert!(
            h.unreported_reason.contains("IVMD2"),
            "the reason must name the wire version the executor lacks: {}",
            h.unreported_reason
        );
    }

    /// The console fetches per-view stats by name; the list must carry the
    /// names. Revert-proof: drop the `jobs` field mapping and this fails.
    #[tokio::test]
    async fn list_jobs_carries_view_names() {
        let registry = std::sync::Arc::new(crate::ivm::IvmJobRegistry::new());
        let coordinator = crate::SharedCoordinator::new(crate::Coordinator::active(
            krishiv_proto::CoordinatorId::try_new("coord-ivm-list").unwrap(),
        ));
        create_or_rehydrate_ivm_job(
            &registry,
            &coordinator,
            "job-list-views",
            Some(false),
            false,
        )
        .await
        .unwrap();
        let job = registry.get("job-list-views").unwrap();
        job.register_view(krishiv_ivm::IncrementalViewSpec {
            name: "v_total".to_string(),
            body_sql: "SELECT k, SUM(v) AS total FROM t GROUP BY k".to_string(),
            output_schema: std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("total", arrow::datatypes::DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: Vec::new(),
        })
        .unwrap();

        let resp = api_ivm_list_jobs(
            axum::extract::State(registry),
            axum::extract::State(coordinator),
        )
        .await;
        let json = serde_json::to_value(&resp.0).unwrap();
        let entry = json["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|j| j["job_id"] == "job-list-views")
            .expect("job present");
        assert_eq!(entry["view_names"], serde_json::json!(["v_total"]));
        assert_eq!(entry["live"], serde_json::json!(true));
    }

    use super::*;
    use crate::Coordinator;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_proto::CoordinatorId;

    fn test_deps() -> (SharedIvmJobRegistry, SharedCoordinator) {
        (
            std::sync::Arc::new(crate::ivm::IvmJobRegistry::new()),
            SharedCoordinator::new(Coordinator::active(
                CoordinatorId::try_new("test-coord").unwrap(),
            )),
        )
    }

    /// Deterministic deps: the partition decision depends on the shard count
    /// (`IvmJobRegistry::new()` derives it from the environment), so handler
    /// tests pin it explicitly — 1 = always Single, >1 = GROUP BY views
    /// auto-partition.
    fn test_deps_with_shards(shards: usize) -> (SharedIvmJobRegistry, SharedCoordinator) {
        (
            std::sync::Arc::new(crate::ivm::IvmJobRegistry::with_default_shards(shards)),
            SharedCoordinator::new(Coordinator::active(
                CoordinatorId::try_new("test-coord").unwrap(),
            )),
        )
    }

    /// A coordinator that lost leadership — the split-brain half.
    fn standby_deps() -> (SharedIvmJobRegistry, SharedCoordinator) {
        (
            std::sync::Arc::new(crate::ivm::IvmJobRegistry::with_default_shards(1)),
            SharedCoordinator::new(Coordinator::standby(
                CoordinatorId::try_new("demoted-coord").unwrap(),
            )),
        )
    }

    fn orders(regions: &[&str], amounts: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(regions.to_vec())),
                Arc::new(Int64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn delta_b64(rb: RecordBatch) -> String {
        let delta = DeltaBatch::from_inserts(rb).unwrap();
        let ipc = serialize_delta_batch(&delta).unwrap();
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, ipc)
    }

    fn ipc_stream_b64(rb: &RecordBatch) -> String {
        let mut buf = Vec::new();
        {
            let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &rb.schema()).unwrap();
            w.write(rb).unwrap();
            w.finish().unwrap();
        }
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, buf)
    }

    fn revenue_view_request() -> RegisterViewRequest {
        RegisterViewRequest {
            name: "revenue".into(),
            body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
            output_schema: SchemaJson {
                fields: vec![
                    SchemaFieldJson {
                        name: "region".into(),
                        data_type: "Utf8".into(),
                        nullable: true,
                    },
                    SchemaFieldJson {
                        name: "total".into(),
                        data_type: "Float64".into(),
                        nullable: true,
                    },
                ],
            },
            output_schema_ipc_b64: None,
            is_materialized: true,
            is_recursive: false,
            lateness: Vec::new(),
        }
    }

    /// Create a job + revenue view through the HTTP handlers themselves.
    async fn create_revenue_job(
        registry: &SharedIvmJobRegistry,
        coordinator: &SharedCoordinator,
        job_id: &str,
    ) {
        let _ = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some(job_id.to_owned()),
                partitioned: None,
                delta_checkpoints: false,
            }),
        )
        .await
        .expect("create job");
        let _ = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(job_id.to_owned()),
            Json(revenue_view_request()),
        )
        .await
        .expect("register view");
    }

    /// Decode a snapshot/output payload back into (region → value) pairs.
    fn decode_delta_rows(b64: &str) -> Vec<(String, f64)> {
        let ipc = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).unwrap();
        let delta = deserialize_delta_batch(&ipc).unwrap();
        let data = delta.data_batch();
        let regions = data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = data
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut rows: Vec<(String, f64)> = (0..data.num_rows())
            .map(|i| (regions.value(i).to_owned(), totals.value(i)))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    // ── job lifecycle ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_job_generates_an_id_and_lists_it() {
        let (registry, coordinator) = test_deps_with_shards(1);
        let resp = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: None,
                partitioned: None,
                delta_checkpoints: false,
            }),
        )
        .await
        .expect("create");
        assert!(!resp.job_id.is_empty(), "generated id must be non-empty");

        let listed = api_ivm_list_jobs(State(registry.clone()), State(coordinator.clone())).await;
        assert!(listed.job_ids.contains(&resp.job_id));
    }

    #[tokio::test]
    async fn create_job_with_explicit_id_is_idempotent() {
        let (registry, coordinator) = test_deps_with_shards(1);
        for _ in 0..2 {
            let resp = api_ivm_create_job(
                State(registry.clone()),
                State(coordinator.clone()),
                Json(CreateJobRequest {
                    job_id: Some("job-a".into()),
                    partitioned: None,
                    delta_checkpoints: false,
                }),
            )
            .await
            .expect("create");
            assert_eq!(resp.job_id, "job-a");
        }
        let listed = api_ivm_list_jobs(State(registry.clone()), State(coordinator.clone())).await;
        assert_eq!(
            listed.job_ids.iter().filter(|j| *j == "job-a").count(),
            1,
            "duplicate create must not duplicate the listing"
        );
    }

    #[tokio::test]
    async fn delete_reports_deleted_then_false_for_missing() {
        let (registry, coordinator) = test_deps_with_shards(1);
        registry.create("gone".into()).unwrap();

        let first = api_ivm_delete_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("gone".into()),
        )
        .await;
        assert!(first.expect("leader delete must succeed").deleted);

        let second = api_ivm_delete_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("gone".into()),
        )
        .await;
        assert!(
            !second.expect("leader delete must succeed").deleted,
            "second delete of the same job is a no-op"
        );
        assert!(registry.get("gone").is_none());
    }

    // ── view registration ─────────────────────────────────────────────────────

    /// IVM-AUD-INT-F16. `partitioned` was consulted only on the create branch,
    /// so asking for an unpartitioned job under a name that already held a
    /// partitioned one answered `200 {job_id}` and left the job partitioned.
    /// The caller asked for single because a partitioned flow never cascades a
    /// base view's output to derived views, so their view-DAG would sit empty
    /// forever with nothing said.
    #[tokio::test]
    async fn pinning_an_existing_partitioned_job_is_a_conflict_not_a_success() {
        let (registry, coordinator) = test_deps_with_shards(3);
        // Default (auto-partitioning) create, then a GROUP BY first view: the
        // registry shards the job on registration.
        create_revenue_job(&registry, &coordinator, "agg").await;
        assert!(
            registry.get("agg").unwrap().is_partitioned(),
            "precondition: the job must really be partitioned"
        );

        let status = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some("agg".to_owned()),
                partitioned: Some(false),
                delta_checkpoints: false,
            }),
        )
        .await;
        let status = match status {
            Ok(_) => panic!("asking for single under a partitioned name must not succeed"),
            Err(status) => status,
        };
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            registry.get("agg").unwrap().is_partitioned(),
            "the refusal must leave the existing job alone"
        );
    }

    /// The same request against a name nobody holds still creates the pinned
    /// job — the conflict check must not have turned the pin into an error.
    #[tokio::test]
    async fn pinning_a_fresh_name_still_creates_an_unpartitioned_job() {
        let (registry, coordinator) = test_deps_with_shards(3);
        let _ = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some("fresh".to_owned()),
                partitioned: Some(false),
                delta_checkpoints: false,
            }),
        )
        .await
        .expect("a fresh pinned create must succeed");
        let _ = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("fresh".to_owned()),
            Json(revenue_view_request()),
        )
        .await
        .expect("register view");
        assert!(
            !registry.get("fresh").unwrap().is_partitioned(),
            "a GROUP BY first view must not shard a pinned job"
        );
    }

    #[tokio::test]
    async fn register_view_404s_on_missing_job() {
        let (registry, coordinator) = test_deps_with_shards(1);
        let err = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("nope".into()),
            Json(revenue_view_request()),
        )
        .await
        .expect_err("must fail");
        assert_eq!(err, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn register_view_rejects_an_unknown_schema_type() {
        let (registry, coordinator) = test_deps_with_shards(1);
        registry.create("j".into()).unwrap();
        let mut req = revenue_view_request();
        req.output_schema.fields[1].data_type = "Decimal999".into();
        let err = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(req),
        )
        .await
        .expect_err("must fail");
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn register_view_accepts_utf8view_schema() {
        // DataFusion 54 emits Utf8View for string columns, so an
        // IncrementalDataFrame's inferred output schema serializes them as
        // "Utf8View". The coordinator must accept it (regression for the
        // distributed df.to_incremental() "invalid output_schema" 400).
        let (registry, coordinator) = test_deps_with_shards(1);
        registry.create("j".into()).unwrap();
        let mut req = revenue_view_request();
        req.output_schema.fields[0].data_type = "Utf8View".into();
        let resp = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(req),
        )
        .await
        .expect("Utf8View output schema must be accepted");
        assert!(resp.0.success);
    }

    /// IVM-AUD-API-A1. The type-name schema wire is a closed whitelist, so a
    /// view whose output carries `Decimal128`, a timezoned `Timestamp`,
    /// `Time32`/`Time64`, `List`, `Struct`, `Interval`, `Null` or
    /// `FixedSizeBinary` was a 400 here and a success embedded. The request now
    /// also accepts the schema as Arrow IPC, which carries all of them.
    ///
    /// The request is built from raw JSON on purpose: the field NAME is the
    /// contract with `krishiv-runtime`'s client, and a struct literal would not
    /// pin it. Every one of these types is asserted to arrive intact via
    /// `view_spec`, so a fix that accepted the field and dropped its contents
    /// would still fail.
    #[tokio::test]
    async fn register_view_accepts_an_arrow_ipc_output_schema() {
        use arrow::datatypes::{Fields, IntervalUnit, TimeUnit};

        let wide: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Decimal128(20, 4), false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("t32", DataType::Time32(TimeUnit::Second), true),
            Field::new("t64", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "addr",
                DataType::Struct(Fields::from(vec![Field::new("zip", DataType::Int32, true)])),
                true,
            ),
            Field::new("gap", DataType::Interval(IntervalUnit::MonthDayNano), true),
            Field::new("nothing", DataType::Null, true),
            Field::new("hash", DataType::FixedSizeBinary(16), true),
        ]));
        let mut buf = Vec::new();
        {
            let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &wide).unwrap();
            w.finish().unwrap();
        }
        let ipc_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &buf);

        // Sanity: the legacy wire genuinely cannot express this schema, so the
        // acceptance below is caused by the IPC field and nothing else.
        let legacy = SchemaJson {
            fields: wide
                .fields()
                .iter()
                .map(|f| SchemaFieldJson {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect(),
        };
        assert!(
            parse_schema(&legacy).is_none(),
            "the type-name whitelist must still reject these types"
        );

        let raw = serde_json::json!({
            "name": "wide",
            "body_sql": "SELECT * FROM t",
            "output_schema": { "fields": [] },
            "output_schema_ipc_b64": ipc_b64,
            "is_materialized": true,
        });
        let req: RegisterViewRequest = serde_json::from_value(raw).expect("request decodes");

        let (registry, coordinator) = test_deps_with_shards(1);
        registry.create("j".into()).unwrap();
        let resp = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(req),
        )
        .await
        .expect("an Arrow IPC output schema must be accepted");
        assert!(resp.0.success);

        let spec = registry
            .get("j")
            .unwrap()
            .view_spec("wide")
            .unwrap()
            .expect("view registered");
        assert_eq!(
            spec.output_schema.as_ref(),
            wide.as_ref(),
            "every field must reach the flow unchanged"
        );
    }

    /// A client that predates `output_schema_ipc_b64` omits it, and must get
    /// exactly the whitelist behaviour it already had — acceptance for a listed
    /// type, 400 for an unlisted one (the sibling test above).
    #[tokio::test]
    async fn register_view_without_the_ipc_field_still_uses_the_type_names() {
        let (registry, coordinator) = test_deps_with_shards(1);
        registry.create("j".into()).unwrap();
        let raw = serde_json::json!({
            "name": "revenue",
            "body_sql": "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
            "output_schema": { "fields": [
                { "name": "region", "data_type": "Utf8", "nullable": true },
                { "name": "total", "data_type": "Float64", "nullable": true },
            ]},
            "is_materialized": true,
        });
        let req: RegisterViewRequest = serde_json::from_value(raw).expect("request decodes");
        assert!(req.output_schema_ipc_b64.is_none());
        let resp = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(req),
        )
        .await
        .expect("the legacy body must keep working");
        assert!(resp.0.success);
        let spec = registry
            .get("j")
            .unwrap()
            .view_spec("revenue")
            .unwrap()
            .expect("view registered");
        assert_eq!(spec.output_schema.field(1).data_type(), &DataType::Float64);
    }

    /// IVM-AUD-DDL-B1. The request struct had no `lateness` field and this
    /// handler hardcoded `lateness: vec![]`, so `CREATE INCREMENTAL VIEW …
    /// LATENESS ts INTERVAL '5' MINUTE` kept its late-record dropping and
    /// join-trace GC in embedded mode and lost both in Distributed mode —
    /// silently, with no error and no log. Same SQL, different retention.
    #[tokio::test]
    async fn register_view_carries_the_lateness_bound_into_the_flow() {
        let (registry, coordinator) = test_deps_with_shards(1);
        registry.create("j".into()).unwrap();
        let mut req = revenue_view_request();
        req.lateness = vec![LatenessJson {
            column: "event_ts".into(),
            lateness_ms: 300_000,
        }];
        let resp = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(req),
        )
        .await
        .expect("register must succeed");
        assert!(resp.0.success);

        let declared = registry
            .get("j")
            .expect("job must exist")
            .declared_lateness()
            .expect("flow must expose its declared lateness");
        assert_eq!(
            declared.len(),
            1,
            "the LATENESS bound must reach the flow, not be dropped at the wire"
        );
        assert_eq!(declared[0].column, "event_ts");
        assert_eq!(declared[0].lateness_ms, 300_000);
    }

    /// IVM-AUD-DIST-E1. No IVM endpoint had a leader check, so a demoted
    /// coordinator answered every mutating call at 200: it rehydrated the job
    /// from the store, advanced its own copy of the flow, and persisted that
    /// back over the new leader's snapshot. Both halves reported success and
    /// the two diverged silently.
    #[tokio::test]
    async fn a_demoted_coordinator_refuses_every_mutating_ivm_call() {
        let (registry, coordinator) = standby_deps();
        registry.create("j".into()).unwrap();

        let created = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some("j2".into()),
                partitioned: None,
                delta_checkpoints: false,
            }),
        )
        .await;
        assert_eq!(
            created.err(),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "create must be refused by a non-leader"
        );

        let registered = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(revenue_view_request()),
        )
        .await;
        assert_eq!(
            registered.err(),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "register-view must be refused by a non-leader"
        );

        let stepped = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await;
        assert_eq!(
            stepped.err(),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "step must be refused by a non-leader"
        );

        let deleted = api_ivm_delete_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await;
        assert_eq!(
            deleted.err(),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "delete must be refused by a non-leader"
        );
        assert!(
            registry.get("j").is_some(),
            "a refused delete must leave the job alone"
        );
    }

    #[tokio::test]
    async fn drop_view_reports_dropped_then_false() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let dropped = api_ivm_drop_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("drop");
        assert!(dropped.dropped);

        let again = api_ivm_drop_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("second drop still 200s");
        assert!(!again.dropped);
    }

    // ── feed / step / read-back ───────────────────────────────────────────────

    #[tokio::test]
    async fn feed_rejects_bad_base64_and_garbage_ipc() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let err = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: "not/base64!!".into(),
            }),
        )
        .await
        .expect_err("bad base64");
        assert_eq!(err, StatusCode::BAD_REQUEST);

        let err = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    b"not arrow ipc",
                ),
            }),
        )
        .await
        .expect_err("garbage ipc");
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn feed_step_and_snapshot_end_to_end() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(
                    &["US", "EU", "US", "APAC", "EU", "US"],
                    &[100, 50, 25, 10, 75, 5],
                )),
            }),
        )
        .await
        .expect("feed");

        let step = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");
        assert_eq!(step.active_views, 1);
        assert_eq!(step.tick, 1);
        assert!(step.total_output_rows > 0);

        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(snap.num_rows, 3, "one aggregate row per region");
        let rows = decode_delta_rows(snap.snapshot_ipc_b64.as_deref().unwrap());
        assert_eq!(
            rows,
            vec![
                ("APAC".to_owned(), 10.0),
                ("EU".to_owned(), 125.0),
                ("US".to_owned(), 130.0),
            ]
        );
    }

    #[tokio::test]
    async fn stream_delta_endpoint_feeds_a_precomputed_delta() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let _ = api_ivm_feed_stream_delta(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedStreamDeltaRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[42])),
            }),
        )
        .await
        .expect("stream-delta feed");

        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");
        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(
            decode_delta_rows(snap.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("US".to_owned(), 42.0)]
        );
    }

    #[tokio::test]
    async fn stream_bridge_accepts_a_full_ipc_snapshot() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let batch = orders(&["US", "EU"], &[7, 3]);
        let _ = api_ivm_stream_bridge(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(StreamBridgeRequest {
                snapshot_ipc_b64: ipc_stream_b64(&batch),
            }),
        )
        .await
        .expect("stream-bridge");

        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");
        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(
            decode_delta_rows(snap.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("EU".to_owned(), 3.0), ("US".to_owned(), 7.0)]
        );
    }

    #[tokio::test]
    async fn stream_bridge_rejects_garbage_ipc() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let err = api_ivm_stream_bridge(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(StreamBridgeRequest {
                snapshot_ipc_b64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    b"junk",
                ),
            }),
        )
        .await
        .expect_err("garbage ipc");
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    // ── dispatch decision visibility ──────────────────────────────────────────

    /// IVM-AUD-API-A5. `/step` answered with counters only, and those counters
    /// are identical whether every view evaluated or one of them blew up — a
    /// failing view does not fail the tick. So a distributed caller had no
    /// view-level failure signal at all: `krishiv_api::IvmJob::step` filled
    /// `degraded_views`/`errored_views` with `Vec::new()` because there was
    /// nothing on the wire to fill them from.
    #[tokio::test]
    async fn step_reports_a_failed_view_through_view_health() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        // A second view on the same job that cannot evaluate.
        let mut broken = revenue_view_request();
        broken.name = "broken".into();
        broken.body_sql = "SELECT region, no_such_column FROM orders".into();
        let _ = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".to_owned()),
            Json(broken),
        )
        .await
        .expect("register broken view");

        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".to_owned(), "orders".to_owned())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[10])),
            }),
        )
        .await
        .expect("feed");

        let resp = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".to_owned()),
        )
        .await
        .expect("step succeeds even though a view failed");

        let health = &resp.0.view_health;
        assert!(
            health.reported,
            "a tick this coordinator computed itself has real health to report"
        );
        assert!(health.unreported_reason.is_empty());
        assert!(
            health.errored_views.iter().any(|e| e.view == "broken"),
            "the failed view must be named on the wire: {:?}",
            health.errored_views
        );
        let e = health
            .errored_views
            .iter()
            .find(|e| e.view == "broken")
            .unwrap();
        assert_eq!(
            e.kind, "view_sql",
            "the failure kind must travel as a stable snake-case name"
        );
        assert!(!e.message.is_empty(), "the failure must carry its message");

        // The counters this endpoint used to return alone: identical in shape
        // to a healthy tick, which is exactly why they were not enough.
        assert_eq!(resp.0.tick, 1);
    }

    #[tokio::test]
    async fn step_records_the_central_dispatch_decision() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");

        let disp = api_ivm_dispatch_state(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("dispatch state")
        .0;
        assert!(!disp.attached);
        let last = disp.last.expect("a dispatch record must be recorded");
        assert_eq!(last.mode, "central-no-executors");
    }

    #[tokio::test]
    async fn partitioned_job_steps_centrally_and_records_it() {
        let (registry, coordinator) = test_deps_with_shards(3);
        create_revenue_job(&registry, &coordinator, "j").await;
        assert!(
            registry.get("j").unwrap().is_partitioned(),
            "GROUP BY view must auto-partition at 3 shards"
        );

        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US", "EU", "APAC"], &[1, 2, 3])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");

        let disp = api_ivm_dispatch_state(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("dispatch state")
        .0;
        assert_eq!(disp.last.unwrap().mode, "central-partitioned");
    }

    #[tokio::test]
    async fn dispatch_state_404s_on_missing_job() {
        let (registry, coordinator) = test_deps_with_shards(1);
        let err = api_ivm_dispatch_state(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("nope".into()),
        )
        .await
        .expect_err("must 404");
        assert_eq!(err, StatusCode::NOT_FOUND);
    }

    // ── read-only view endpoints ──────────────────────────────────────────────

    /// The `materialized` flag distinguishes "you never asked for
    /// materialization" from "materialized but empty" — previously both
    /// returned `{"snapshot_ipc_b64": null, "num_rows": 0}` and a caller who hit
    /// the `#[serde(default)]` false could only conclude the engine was broken.
    #[tokio::test]
    async fn snapshot_reports_whether_the_view_is_materialized() {
        let (registry, coordinator) = test_deps_with_shards(1);
        let _ = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some("mat-flag".into()),
                partitioned: Some(false),
                delta_checkpoints: false,
            }),
        )
        .await
        .unwrap();

        // Registered WITHOUT is_materialized (the request default).
        let mut plain = revenue_view_request();
        plain.name = "plain".into();
        plain.is_materialized = false;
        let _ = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("mat-flag".to_string()),
            Json(plain),
        )
        .await
        .unwrap();

        let mut materialized = revenue_view_request();
        materialized.name = "mat".into();
        materialized.is_materialized = true;
        let _ = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("mat-flag".to_string()),
            Json(materialized),
        )
        .await
        .unwrap();

        let plain_snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("mat-flag".to_string(), "plain".to_string())),
        )
        .await
        .unwrap();
        let mat_snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("mat-flag".to_string(), "mat".to_string())),
        )
        .await
        .unwrap();

        // Both are empty right now — the flag is the only thing telling them
        // apart, which is the whole point.
        assert_eq!(plain_snap.num_rows, 0);
        assert_eq!(mat_snap.num_rows, 0);
        assert!(
            !plain_snap.materialized,
            "non-materialized view must say so"
        );
        assert!(mat_snap.materialized, "materialized view must say so");
    }

    #[tokio::test]
    async fn snapshot_and_output_are_empty_before_any_step() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(snap.num_rows, 0);
        assert!(snap.snapshot_ipc_b64.is_none());

        let out = api_ivm_view_output(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
            axum::extract::Query(ViewOutputQuery::default()),
        )
        .await
        .expect("output");
        assert_eq!(out.num_rows, 0);
        assert!(out.delta_ipc_b64.is_none());
    }

    #[tokio::test]
    async fn view_output_returns_the_last_tick_delta() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[9])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");

        let out = api_ivm_view_output(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
            axum::extract::Query(ViewOutputQuery::default()),
        )
        .await
        .expect("output");
        assert!(out.delta_ipc_b64.is_some());
        assert!(out.num_rows > 0);
    }

    /// IVM-AUD-INT-F5. `/output` is a non-consuming peek at a **coalescing**
    /// watch, so it had two silent failures: polling twice between ticks handed
    /// back the same delta twice (a consumer with no way to tell them apart
    /// double-applies), and a consumer polling slower than `/step` lost every
    /// delta but the newest with nothing recording that it happened.
    ///
    /// `since_tick` closes the first. `published_rows_total` measures the
    /// second — it does not recover the lost deltas, and this test asserts the
    /// arithmetic that makes the loss visible, not that nothing was lost.
    #[tokio::test]
    async fn view_output_carries_a_cursor_and_makes_coalesced_loss_measurable() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let feed_and_step = async |regions: &'static [&'static str], amounts: &'static [i64]| {
            let _ = api_ivm_feed_source(
                State(registry.clone()),
                State(coordinator.clone()),
                Path(("j".to_owned(), "orders".to_owned())),
                Json(FeedSourceRequest {
                    delta_ipc_b64: delta_b64(orders(regions, amounts)),
                }),
            )
            .await
            .expect("feed");
            let _ = api_ivm_step(
                State(registry.clone()),
                State(coordinator.clone()),
                Path("j".to_owned()),
            )
            .await
            .expect("step");
        };
        let read = async |since: Option<u64>| {
            api_ivm_view_output(
                State(registry.clone()),
                State(coordinator.clone()),
                Path(("j".to_owned(), "revenue".to_owned())),
                axum::extract::Query(ViewOutputQuery { since_tick: since }),
            )
            .await
            .expect("output")
            .0
        };

        feed_and_step(&["US"], &[10]).await;
        let first = read(None).await;
        assert!(first.delta_ipc_b64.is_some());
        let cursor = first.tick.expect("a published delta has a tick");
        let seen_rows = first.published_rows_total;
        assert_eq!(
            seen_rows, first.num_rows as u64,
            "the first publication accounts for every published row so far"
        );

        // Same tick, polled again with the cursor: no delta, but the tick is
        // still reported so the caller can carry the cursor forward.
        let again = read(Some(cursor)).await;
        assert!(
            again.delta_ipc_b64.is_none(),
            "a delta already delivered must not come back a second time"
        );
        assert_eq!(again.tick, Some(cursor));
        // …while a cursorless poll still re-serves it (the old behaviour, kept
        // for callers that have no cursor).
        assert!(read(None).await.delta_ipc_b64.is_some());

        // Two more ticks with no poll in between: the watch coalesces, so the
        // middle delta is GONE. It is not recovered here — it is counted.
        feed_and_step(&["EU"], &[5]).await;
        feed_and_step(&["APAC"], &[7]).await;

        let latest = read(Some(cursor)).await;
        assert!(latest.delta_ipc_b64.is_some(), "the newest delta is served");
        assert!(
            latest.tick.expect("tick") > cursor + 1,
            "at least one tick published between the cursor and this value"
        );
        let lost = latest.published_rows_total - seen_rows - latest.num_rows as u64;
        assert!(
            lost > 0,
            "the coalesced tick's rows must be reported as lost: total={} seen={} served={}",
            latest.published_rows_total,
            seen_rows,
            latest.num_rows
        );
    }

    #[tokio::test]
    async fn view_stats_404_for_unregistered_view_and_count_inserts() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let err = api_ivm_view_stats(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "no-such-view".into())),
        )
        .await
        .expect_err("unregistered view must 404");
        assert_eq!(err, StatusCode::NOT_FOUND);

        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US", "EU"], &[1, 2])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");

        let stats = api_ivm_view_stats(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("stats");
        assert_eq!(stats.num_rows, 2);
        assert!(stats.rows_inserted_total >= 2);
        assert!(stats.last_tick_inserts >= 2);
        assert_eq!(stats.rows_retracted_total, 0);
    }

    #[tokio::test]
    async fn view_debug_info_reports_materialization_and_plan_choice() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[1])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");

        let info = api_ivm_view_debug_info(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("debug info");
        assert!(info.is_materialized);
        assert!(info.has_snapshot);
        assert_eq!(info.snapshot_num_rows, 1);
        assert!(info.has_last_output);
        assert!(
            !info.plan_reason.is_empty(),
            "plan choice must always carry a reason (AUD-9 loud degradation)"
        );

        let err = api_ivm_view_debug_info(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "ghost".into())),
        )
        .await
        .expect_err("unknown view");
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    // ── checkpoint / restore ──────────────────────────────────────────────────

    #[tokio::test]
    async fn full_checkpoint_restores_earlier_view_state() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[100])),
            }),
        )
        .await
        .expect("feed 1");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step 1");

        let ckpt = api_ivm_checkpoint(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint")
        .0;
        assert!(!ckpt.checkpoint_b64.is_empty());

        // Advance the state past the checkpoint…
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[900])),
            }),
        )
        .await
        .expect("feed 2");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step 2");
        let advanced = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(
            decode_delta_rows(advanced.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("US".to_owned(), 1000.0)]
        );

        // …then restore back to the checkpointed state.
        let _ = api_ivm_restore(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RestoreRequest {
                checkpoint_b64: ckpt.checkpoint_b64,
            }),
        )
        .await
        .expect("restore");
        let restored = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(
            decode_delta_rows(restored.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("US".to_owned(), 100.0)],
            "restore must rewind the materialized view to the checkpoint"
        );
    }

    #[tokio::test]
    async fn delta_checkpoint_round_trips() {
        // IVM-AUD-DIST-C1 / IVM-AUD-DIST-H1. This test used to call
        // checkpoint-delta then restore-delta and assert *nothing*, so it
        // passed on the 4-byte `count = 0` frame that was the only thing
        // /checkpoint-delta could produce: no handler ever switched
        // accumulation on. It now asserts the frame carries the source and
        // that composing full + delta actually replays the input.
        let (registry, coordinator) = test_deps_with_shards(1);
        let _ = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some("j".to_owned()),
                partitioned: None,
                delta_checkpoints: true,
            }),
        )
        .await
        .expect("create job");
        let _ = api_ivm_register_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".to_owned()),
            Json(revenue_view_request()),
        )
        .await
        .expect("register view");

        // A full checkpoint of the empty job: the base the delta rides on.
        let base = api_ivm_checkpoint(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint")
        .0
        .checkpoint_b64;

        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[5])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");

        let delta_ckpt = api_ivm_checkpoint_delta(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint-delta")
        .0
        .checkpoint_delta_b64;

        // The frame is `u32 count` then one length-prefixed entry per source.
        // Pre-fix this decoded to exactly `[0,0,0,0]`.
        let raw = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &delta_ckpt)
            .unwrap();
        let count = u32::from_le_bytes(raw[0..4].try_into().unwrap());
        assert_eq!(count, 1, "delta checkpoint must carry the fed source");
        assert!(
            String::from_utf8_lossy(&raw).contains("orders"),
            "delta checkpoint must name the source it accumulated"
        );

        // Rewind to the empty base, then compose the delta back on top.
        let _ = api_ivm_restore(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RestoreRequest {
                checkpoint_b64: base,
            }),
        )
        .await
        .expect("restore full");
        let _ = api_ivm_restore_delta(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RestoreDeltaRequest {
                checkpoint_delta_b64: delta_ckpt,
            }),
        )
        .await
        .expect("restore-delta");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step after restore");

        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snap")
        .0;
        assert_eq!(
            decode_delta_rows(&snap.snapshot_ipc_b64.expect("snapshot present")),
            vec![("US".to_owned(), 5.0)],
            "full + delta must reproduce the input the delta accumulated"
        );
    }

    #[tokio::test]
    async fn restore_rejects_bad_base64_and_garbage_bytes() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let err = api_ivm_restore(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RestoreRequest {
                checkpoint_b64: "!!!".into(),
            }),
        )
        .await
        .expect_err("bad base64");
        assert_eq!(err, StatusCode::BAD_REQUEST);

        let err = api_ivm_restore(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RestoreRequest {
                checkpoint_b64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    b"not a checkpoint",
                ),
            }),
        )
        .await
        .expect_err("garbage bytes");
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    // ── durable snapshot round trip through a store-backed coordinator ────────

    #[tokio::test]
    async fn evicted_job_is_restored_from_the_durable_snapshot() {
        let (registry, _) = test_deps_with_shards(1);
        let coordinator = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("test-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        create_revenue_job(&registry, &coordinator, "j").await;
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[100])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step persists the snapshot");

        // Simulate an in-memory eviction (process restart): the registry loses
        // the job but the coordinator's store still holds the durable snapshot.
        assert!(registry.delete("j"));
        assert!(registry.get("j").is_none());

        // list still surfaces the durably-persisted job…
        let listed = api_ivm_list_jobs(State(registry.clone()), State(coordinator.clone())).await;
        assert!(listed.job_ids.contains(&"j".to_owned()));

        // …and a state-reading handler that goes through ensure_ivm_job
        // transparently restores it.
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["EU"], &[50])),
            }),
        )
        .await
        .expect("feed after restore");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step after restore");
        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(
            decode_delta_rows(snap.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("EU".to_owned(), 50.0), ("US".to_owned(), 100.0)],
            "restored job must keep its pre-eviction materialized state"
        );
    }

    /// Build a store-backed coordinator + a job with 100 US revenue already
    /// stepped and persisted, then evict the registry entry to model a
    /// coordinator restart / standby failover: the durable snapshot survives,
    /// the in-memory registry does not.
    async fn evicted_but_durable_job() -> (SharedIvmJobRegistry, SharedCoordinator) {
        let (registry, _) = test_deps_with_shards(1);
        let coordinator = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("test-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        create_revenue_job(&registry, &coordinator, "j").await;
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[100])),
            }),
        )
        .await
        .expect("feed");
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step persists the snapshot");
        assert!(registry.delete("j"), "evict the in-memory entry");
        (registry, coordinator)
    }

    /// `POST /api/v1/jobs {"kind":"ivm"}` is a second front door to the same
    /// resource, and it used to call `registry.create` directly rather than
    /// the create path above. For an id whose state is durable but whose
    /// registry entry is gone — restart, promotion, eviction — that produced a
    /// live *empty* job under the same id, which the next step or restore
    /// would then persist straight over the real state. The read handlers'
    /// own rehydration cannot catch it either: the id is present, just empty.
    #[tokio::test]
    async fn unified_ivm_submission_rehydrates_an_evicted_job_rather_than_recreating_it_empty() {
        let (registry, coordinator) = evicted_but_durable_job().await;
        let state = crate::ivm_http::IvmRouterState {
            registry: registry.clone(),
            coordinator: coordinator.clone(),
        };

        let _ = crate::unified_jobs_http::api_unified_submit(
            State(state),
            Json(
                serde_json::from_value(serde_json::json!({"kind": "ivm", "job_id": "j"}))
                    .expect("well-formed unified request"),
            ),
        )
        .await
        .expect("unified submit must succeed");

        let job = registry.get("j").expect("the job must be live again");
        let snapshot = job
            .snapshot("revenue")
            .expect("snapshot lookup must succeed")
            .expect("the rehydrated job must still have its view");
        assert_eq!(
            snapshot.num_rows(),
            1,
            "resubmitting an existing id must restore its state, not blank it"
        );
    }

    /// Reads must rehydrate too. Nothing repopulates the registry at startup,
    /// so a coordinator restart left `/snap`, `/output`, `/stats`,
    /// `/debug-info` and `/checkpoint` answering 404 for a job whose state was
    /// sitting in the store — a live table reported as missing — until an
    /// unrelated `/feed` or `/step` happened to resurrect it. `/stats` is the
    /// one the platform freshness sampler polls every few seconds.
    #[tokio::test]
    async fn read_handlers_rehydrate_an_evicted_job_instead_of_404ing() {
        let (registry, coordinator) = evicted_but_durable_job().await;

        let stats = api_ivm_view_stats(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("/stats must rehydrate, not 404");
        assert_eq!(stats.num_rows, 1);

        // Evict again so each handler is proven to rehydrate on its own rather
        // than riding on the previous one's restore.
        assert!(registry.delete("j"));
        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("/snap must rehydrate, not 404");
        assert_eq!(
            decode_delta_rows(snap.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("US".to_owned(), 100.0)],
            "the rehydrated view must carry its pre-eviction state"
        );

        assert!(registry.delete("j"));
        let ckpt = api_ivm_checkpoint(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("/checkpoint — the backup path — must rehydrate, not 404");
        assert!(!ckpt.checkpoint_b64.is_empty());

        assert!(registry.delete("j"));
        let info = api_ivm_view_debug_info(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("/debug-info must rehydrate, not 404");
        assert!(info.has_snapshot);

        assert!(registry.delete("j"));
        let out = api_ivm_view_output(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
            axum::extract::Query(ViewOutputQuery::default()),
        )
        .await
        .expect("/output must rehydrate, not 404");
        // 200 with a null delta, not 404: the durable snapshot carries source
        // and view state, not the last tick's emitted delta, so after a restart
        // there genuinely is no last output to report. "No delta since the
        // restart" and "no such job" are different answers and the caller must
        // be able to tell them apart.
        assert!(out.delta_ipc_b64.is_none());
        assert_eq!(out.num_rows, 0);
    }

    /// A restore that is not persisted is a restore that did not happen. The
    /// handler changes authoritative state exactly as `register_view`,
    /// `drop_view` and `step` do, all of which persist; this one answered
    /// `{"success": true}` and left the store holding the pre-restore
    /// snapshot, so the next rehydration silently undid the rewind.
    #[tokio::test]
    async fn restore_is_durable_without_waiting_for_the_next_step() {
        let (registry, _) = test_deps_with_shards(1);
        let coordinator = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("test-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        create_revenue_job(&registry, &coordinator, "j").await;

        let feed_and_step = async |amount: i64| {
            let _ = api_ivm_feed_source(
                State(registry.clone()),
                State(coordinator.clone()),
                Path(("j".into(), "orders".into())),
                Json(FeedSourceRequest {
                    delta_ipc_b64: delta_b64(orders(&["US"], &[amount])),
                }),
            )
            .await
            .expect("feed");
            let _ = api_ivm_step(
                State(registry.clone()),
                State(coordinator.clone()),
                Path("j".into()),
            )
            .await
            .expect("step");
        };

        feed_and_step(100).await;
        let ckpt = api_ivm_checkpoint(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint")
        .0;

        // Advance past the checkpoint and persist that advanced state.
        feed_and_step(900).await;

        let _ = api_ivm_restore(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RestoreRequest {
                checkpoint_b64: ckpt.checkpoint_b64,
            }),
        )
        .await
        .expect("restore");

        // Restart before any further step. The durable snapshot must already
        // be the restored one.
        assert!(registry.delete("j"), "evict the in-memory entry");
        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snapshot");
        assert_eq!(
            decode_delta_rows(snap.snapshot_ipc_b64.as_deref().unwrap()),
            vec![("US".to_owned(), 100.0)],
            "a restore reported successful must survive a restart; the store \
             still held the pre-restore (advanced) snapshot"
        );
    }

    // ── vector views ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn vector_view_rejects_unsupported_sink_and_missing_job() {
        let (registry, coordinator) = test_deps_with_shards(1);

        let err = api_ivm_register_vector_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("nope".into()),
            Json(RegisterVectorViewRequest {
                view_name: "v".into(),
                id_column: "id".into(),
                vector_column: "vec".into(),
                sink_type: "in_memory".into(),
            }),
        )
        .await
        .expect_err("missing job");
        assert_eq!(err, StatusCode::NOT_FOUND);

        create_revenue_job(&registry, &coordinator, "j").await;
        let err = api_ivm_register_vector_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RegisterVectorViewRequest {
                view_name: "v".into(),
                id_column: "id".into(),
                vector_column: "vec".into(),
                sink_type: "qdrant".into(),
            }),
        )
        .await
        .expect_err("unsupported sink");
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    /// IVM-AUD-DIST-H3: the handler used to build an `InMemoryVectorSink`, hand
    /// it to detached tasks and drop the only `Arc` on the way out, so nothing
    /// could read what was written and there was no record the view existed.
    /// Registration must survive the handler and be listable, with health.
    #[tokio::test]
    async fn a_registered_vector_view_outlives_the_handler_and_is_listable() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;

        let resp = api_ivm_register_vector_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RegisterVectorViewRequest {
                view_name: "revenue".into(),
                id_column: "region".into(),
                vector_column: "vec".into(),
                sink_type: "in_memory".into(),
            }),
        )
        .await
        .expect("register vector view");
        assert_eq!(resp.shards, 1);

        let listed = api_ivm_list_vector_views(State(registry.clone()), Path("j".into()))
            .await
            .expect("list vector views");
        assert_eq!(
            listed.vector_views.len(),
            1,
            "the registration must outlive the handler that made it"
        );
        let v = &listed.vector_views[0];
        assert_eq!(v.view_name, "revenue");
        assert_eq!(v.sink_type, "in_memory");
        assert_eq!(v.shards, 1);
        assert_eq!(
            v.shard_status.len(),
            1,
            "each shard's maintenance task must report health"
        );
        assert!(!v.diverged);

        // Same name twice is refused rather than silently spawning a second
        // set of tasks against a second unreachable sink.
        let err = api_ivm_register_vector_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
            Json(RegisterVectorViewRequest {
                view_name: "revenue".into(),
                id_column: "region".into(),
                vector_column: "vec".into(),
                sink_type: "in_memory".into(),
            }),
        )
        .await
        .expect_err("duplicate vector view name");
        assert_eq!(err, StatusCode::BAD_REQUEST);

        // And it can be stopped.
        let del = api_ivm_delete_vector_view(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("delete vector view");
        assert!(del.deleted);
        let listed = api_ivm_list_vector_views(State(registry.clone()), Path("j".into()))
            .await
            .expect("list vector views");
        assert!(listed.vector_views.is_empty());
    }

    // ── schema JSON parsing ───────────────────────────────────────────────────

    #[test]
    fn parse_schema_supports_every_documented_type_and_rejects_unknown() {
        let all = [
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float32",
            "Float64",
            "Utf8",
            "LargeUtf8",
            "Boolean",
            "Binary",
            "TimestampMs",
            "TimestampUs",
            "Date32",
            "Date64",
        ];
        let schema = parse_schema(&SchemaJson {
            fields: all
                .iter()
                .map(|t| SchemaFieldJson {
                    name: format!("f_{t}"),
                    data_type: (*t).to_owned(),
                    nullable: true,
                })
                .collect(),
        })
        .expect("all documented types must parse");
        assert_eq!(schema.fields().len(), all.len());

        assert!(
            parse_schema(&SchemaJson {
                fields: vec![SchemaFieldJson {
                    name: "x".into(),
                    data_type: "Struct".into(),
                    nullable: false,
                }],
            })
            .is_none(),
            "unknown type must reject the whole schema"
        );
    }

    /// IVM-AUD-INT-F11: a producer that outruns the stepper must be told to
    /// slow down, not allowed to grow the coordinator's heap until it dies.
    /// Revert-proof: drop the `ensure_pending_headroom` call from
    /// `api_ivm_feed_source` and the second feed succeeds.
    #[tokio::test]
    async fn a_feed_is_refused_once_the_backlog_hits_the_cap() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        // A cap of one byte: any accepted delta puts the backlog over it.
        registry.set_max_pending_bytes(1);

        // The first feed sees an empty backlog and is admitted.
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[5])),
            }),
        )
        .await
        .expect("first feed is admitted");
        assert!(
            registry.get("j").unwrap().pending_bytes().unwrap() > 0,
            "precondition: the first feed left a backlog"
        );

        // The second sees the backlog and is refused, on every feed route.
        let status = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[6])),
            }),
        )
        .await
        .expect_err("feed past the cap must be refused");
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

        let status = api_ivm_feed_stream_delta(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedStreamDeltaRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[6])),
            }),
        )
        .await
        .expect_err("stream-delta past the cap must be refused");
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

        let status = api_ivm_stream_bridge(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(StreamBridgeRequest {
                snapshot_ipc_b64: ipc_stream_b64(&orders(&["US"], &[6])),
            }),
        )
        .await
        .expect_err("stream-bridge past the cap must be refused");
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

        // Stepping drains the backlog, which is what re-opens the gate — the
        // point of backpressure is that it lifts.
        let _ = api_ivm_step(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("step");
        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[6])),
            }),
        )
        .await
        .expect("a drained backlog re-opens the gate");
    }

    /// IVM-AUD-INT-F11: a typo in the environment must not silently disable
    /// the cap; an explicit `0` must.
    #[test]
    fn the_backlog_cap_is_configurable_and_fails_closed_on_nonsense() {
        use crate::ivm::resolve_max_pending_bytes;
        assert_eq!(
            resolve_max_pending_bytes(None),
            crate::ivm::DEFAULT_IVM_MAX_PENDING_BYTES
        );
        assert_eq!(resolve_max_pending_bytes(Some(" 4096 ")), 4096);
        assert_eq!(
            resolve_max_pending_bytes(Some("0")),
            0,
            "an explicit 0 is the documented opt-out"
        );
        assert_eq!(
            resolve_max_pending_bytes(Some("1GiB")),
            crate::ivm::DEFAULT_IVM_MAX_PENDING_BYTES,
            "an unparseable value must fall back to the default, not to unlimited"
        );
    }

    // ── resident dispatch harness (IVM-AUD-DIST-A3) ──────────────────────────

    /// Play the executor for the resident-IVM dispatch protocol.
    ///
    /// IVM-AUD-DIST-A3: nothing exercised `submit_resident_ivm_step` through
    /// `api_ivm_step`, because that path submits `ivm-attach` / `ivm-tick`
    /// scheduler jobs and then *waits for an executor* to drive them to a
    /// terminal state — so with no executor in the test the handler could only
    /// sit there until the dispatch timeout. This stands in for one: it polls
    /// the coordinator for those jobs, launches them and terminates each,
    /// letting `responder` decide per job id whether the job succeeds
    /// (`Ok(blob)`) or is cancelled (`Err(())`) — the fork the fence protocol
    /// and the re-feed-on-failure guard hang off.
    ///
    /// It deliberately does not decode the fragment: a fake executor that
    /// re-implemented the wire would be testing itself. It keys off the job id
    /// prefix the coordinator chose (`ivm-attach-…` / `ivm-tick-…`) and returns
    /// whatever blob the test wants that step to observe.
    fn spawn_fake_ivm_executor(
        coordinator: SharedCoordinator,
        executor_id: krishiv_proto::ExecutorId,
        lease: krishiv_proto::LeaseGeneration,
        responder: impl Fn(&str) -> Result<Option<Vec<u8>>, ()> + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        use krishiv_proto::{TaskOutputMetadata, TaskState, TaskStatusUpdate};
        tokio::spawn(async move {
            let mut handled: std::collections::HashSet<String> = std::collections::HashSet::new();
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let fresh: Vec<JobId> = {
                    let coord = coordinator.read().await;
                    coord
                        .job_snapshots()
                        .into_iter()
                        .filter(|j| j.job_id().as_str().starts_with("ivm-"))
                        .filter(|j| !handled.contains(j.job_id().as_str()))
                        .map(|j| j.job_id().clone())
                        .collect()
                };
                for job_id in fresh {
                    handled.insert(job_id.as_str().to_owned());
                    let reply = responder(job_id.as_str());
                    let mut coord = coordinator.write().await;
                    if let Err(()) = reply {
                        let _ = coord.cancel_job(&job_id);
                        continue;
                    }
                    let Ok(blob) = reply else { continue };
                    let Ok(mut assignments) = coord.launch_assigned_task_assignments(&job_id)
                    else {
                        continue;
                    };
                    if assignments.is_empty() {
                        handled.remove(job_id.as_str());
                        continue;
                    }
                    let assignment = assignments.remove(0);
                    let meta = TaskOutputMetadata::new("ivm", 0, 0, 0)
                        .with_inline_record_batch_ipc(blob.into_iter().collect());
                    let update = TaskStatusUpdate::new(
                        job_id,
                        assignment.stage_id().clone(),
                        assignment.task_id().clone(),
                        executor_id.clone(),
                        TaskState::Succeeded,
                        assignment.attempt_id().as_u32(),
                    )
                    .with_lease_generation(lease)
                    .with_output_metadata(meta);
                    let _ = coord.apply_task_update(update);
                    let _ = coord.take_pending_sink_finalize();
                }
            }
        })
    }

    /// A coordinator with one live executor, plus the lease to report under.
    async fn coordinator_with_one_executor() -> (
        SharedCoordinator,
        krishiv_proto::ExecutorId,
        krishiv_proto::LeaseGeneration,
    ) {
        let coordinator = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("test-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        let executor_id = krishiv_proto::ExecutorId::try_new("exec-ivm").unwrap();
        let lease = coordinator
            .write()
            .await
            .register_executor(krishiv_proto::ExecutorDescriptor::new(
                executor_id.clone(),
                "pod-ivm",
                4,
            ))
            .expect("register executor");
        (coordinator, executor_id, lease)
    }

    /// The per-view output-delta blob a resident executor returns for a tick.
    fn revenue_output_blob(region: &str, total: f64) -> Vec<u8> {
        let rb = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("total", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![region])),
                Arc::new(Float64Array::from(vec![total])),
            ],
        )
        .unwrap();
        let mut map = HashMap::new();
        map.insert("revenue".to_owned(), DeltaBatch::from_inserts(rb).unwrap());
        krishiv_ivm::encode_delta_map(&map).unwrap()
    }

    /// A v2 tick result carrying real per-view health, driven all the way
    /// through `api_ivm_step`.
    ///
    /// IVM-AUD-A5-RESIDENT. The register, this module and the T11 test doc all
    /// claimed "`test_deps_with_shards` yields `executor_count == 0`, so no
    /// test can reach the resident arm of `api_ivm_step` through the handler",
    /// and used that to justify closing a HIGH row on unit-level proof alone.
    /// The premise was false: `executor_count` comes from
    /// `coordinator.executor_snapshots()`, and `coordinator_with_one_executor`
    /// has driven the resident arm through this handler since DIST-A3. What
    /// was genuinely missing is the path this test covers — the executor
    /// echoing v2 capability at attach, the coordinator then sending a binary
    /// tick, and a real `TickHealth` arriving as `reported: true` with its
    /// contents intact.
    #[tokio::test]
    async fn a_v2_resident_tick_relays_real_health_through_the_handler() {
        use krishiv_ivm::{TickHealth, WireCapabilities, WireViewError};

        let (registry, _) = test_deps_with_shards(1);
        let (coordinator, executor_id, lease) = coordinator_with_one_executor().await;
        create_revenue_job(&registry, &coordinator, "j").await;

        let fake = spawn_fake_ivm_executor(coordinator.clone(), executor_id, lease, |job_id| {
            if job_id.starts_with("ivm-attach") {
                // Speak v2 at attach — the half the old test left uncovered by
                // answering `Ok(None)`, which fail-closes to the legacy wire.
                Ok(Some(krishiv_ivm::encode_attach_echo(WireCapabilities {
                    binary_input_deltas: true,
                    tick_health: true,
                })))
            } else {
                let rb = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("region", DataType::Utf8, true),
                        Field::new("total", DataType::Float64, true),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["US"])),
                        Arc::new(Float64Array::from(vec![5.0])),
                    ],
                )
                .unwrap();
                let mut map = HashMap::new();
                map.insert("revenue".to_owned(), DeltaBatch::from_inserts(rb).unwrap());
                let health = TickHealth {
                    degraded_views: vec!["slow_view".to_owned()],
                    errored_views: vec![WireViewError {
                        view: "broken_view".to_owned(),
                        kind: "view_sql".to_owned(),
                        message: "planning failed on the executor".to_owned(),
                    }],
                    degraded_omitted: 0,
                    errored_omitted: 0,
                };
                Ok(Some(
                    krishiv_ivm::encode_tick_result(&map, &health).unwrap(),
                ))
            }
        });

        let fed = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[5])),
            }),
        )
        .await
        .expect("feed");
        assert!(fed.0.success);
        let step = tokio::time::timeout(
            Duration::from_secs(20),
            api_ivm_step(
                State(registry.clone()),
                State(coordinator.clone()),
                Path("j".into()),
            ),
        )
        .await
        .expect("step did not finish")
        .expect("step");
        fake.abort();

        assert_eq!(
            registry
                .dispatch_state("j")
                .last
                .as_ref()
                .map(|d| d.mode.as_str()),
            Some("resident"),
            "precondition: the tick must actually have gone to the resident executor"
        );

        let health = &step.0.view_health;
        assert!(
            health.reported,
            "a v2 executor's health must arrive as reported, not as unreported: {health:?}"
        );
        assert_eq!(
            health.degraded_views,
            vec!["slow_view".to_owned()],
            "the executor's degraded list must survive the wire"
        );
        assert_eq!(
            health.errored_views.len(),
            1,
            "the executor's errored list must survive the wire: {health:?}"
        );
        assert_eq!(health.errored_views[0].view, "broken_view");
        assert!(
            health.errored_views[0]
                .message
                .contains("planning failed on the executor"),
            "the message must cross verbatim: {health:?}"
        );
    }

    /// IVM-AUD-DIST-A3: the first test to drive `submit_resident_ivm_step`
    /// through `api_ivm_step` — attach, fenced tick, and the coordinator-side
    /// mirror of the executor's output deltas.
    #[tokio::test]
    async fn a_resident_tick_attaches_fences_and_mirrors_the_output_delta() {
        let (registry, _) = test_deps_with_shards(1);
        let (coordinator, executor_id, lease) = coordinator_with_one_executor().await;
        create_revenue_job(&registry, &coordinator, "j").await;

        let fake = spawn_fake_ivm_executor(coordinator.clone(), executor_id, lease, |job_id| {
            if job_id.starts_with("ivm-attach") {
                Ok(None)
            } else {
                Ok(Some(revenue_output_blob("US", 5.0)))
            }
        });

        let _ = api_ivm_feed_source(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "orders".into())),
            Json(FeedSourceRequest {
                delta_ipc_b64: delta_b64(orders(&["US"], &[5])),
            }),
        )
        .await
        .expect("feed");
        let step = tokio::time::timeout(
            Duration::from_secs(20),
            api_ivm_step(
                State(registry.clone()),
                State(coordinator.clone()),
                Path("j".into()),
            ),
        )
        .await
        .expect("step did not finish")
        .expect("step");
        fake.abort();

        let dispatch = registry.dispatch_state("j");
        assert_eq!(
            dispatch.last.as_ref().map(|d| d.mode.as_str()),
            Some("resident"),
            "the tick must have been dispatched to the resident executor, not computed centrally"
        );
        assert!(dispatch.attached, "the job must be recorded as attached");
        assert_eq!(dispatch.fence, 1, "the first tick fence is 1");
        assert_eq!(step.0.total_output_rows, 1);

        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snap")
        .0;
        assert_eq!(
            decode_delta_rows(&snap.snapshot_ipc_b64.expect("snapshot present")),
            vec![("US".to_owned(), 5.0)],
            "the coordinator mirror must carry the executor's output delta"
        );
    }

    /// IVM-AUD-INT-F10: when mirroring the resident tick fails, the deltas that
    /// tick drained must go back to `pending` so the central fallback computes
    /// them. Pre-fix `apply_remote_tick`'s error path skipped the `refeed`
    /// guard entirely and the deltas were simply gone — the fallback then ran
    /// on nothing and the view silently under-counted.
    ///
    /// The distinguishing assertion is the total: the fallback exists in both
    /// versions, so "the answer is right" only separates them because without
    /// the re-feed the fallback has no input at all.
    #[tokio::test]
    async fn a_failed_remote_mirror_returns_its_deltas_to_the_central_fallback() {
        let (registry, _) = test_deps_with_shards(1);
        let (coordinator, executor_id, lease) = coordinator_with_one_executor().await;
        create_revenue_job(&registry, &coordinator, "j").await;

        // Tick 1 succeeds, establishing the view baseline (region, total).
        // Tick 2 returns a delta whose schema cannot be applied to it, so the
        // coordinator's mirror fails after the executor has already applied it.
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fake = spawn_fake_ivm_executor(coordinator.clone(), executor_id, lease, {
            let ticks = Arc::clone(&ticks);
            move |job_id| {
                if job_id.starts_with("ivm-attach") {
                    return Ok(None);
                }
                let n = ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    Ok(Some(revenue_output_blob("US", 5.0)))
                } else {
                    let rb = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new(
                            "not_the_view_schema",
                            DataType::Int64,
                            true,
                        )])),
                        vec![Arc::new(Int64Array::from(vec![1i64]))],
                    )
                    .unwrap();
                    let mut map = HashMap::new();
                    map.insert("revenue".to_owned(), DeltaBatch::from_inserts(rb).unwrap());
                    Ok(Some(krishiv_ivm::encode_delta_map(&map).unwrap()))
                }
            }
        });

        for amount in [5i64, 7i64] {
            let _ = api_ivm_feed_source(
                State(registry.clone()),
                State(coordinator.clone()),
                Path(("j".into(), "orders".into())),
                Json(FeedSourceRequest {
                    delta_ipc_b64: delta_b64(orders(&["US"], &[amount])),
                }),
            )
            .await
            .expect("feed");
            let _ = tokio::time::timeout(
                Duration::from_secs(20),
                api_ivm_step(
                    State(registry.clone()),
                    State(coordinator.clone()),
                    Path("j".into()),
                ),
            )
            .await
            .expect("step did not finish")
            .expect("step");
        }
        fake.abort();

        // Precondition, not the proof: the second tick did take the fallback.
        assert_eq!(
            registry
                .dispatch_state("j")
                .last
                .as_ref()
                .map(|d| d.mode.as_str()),
            Some("central-fallback"),
            "the mirror failure must have been recorded as a central fallback"
        );

        let snap = api_ivm_snapshot(
            State(registry.clone()),
            State(coordinator.clone()),
            Path(("j".into(), "revenue".into())),
        )
        .await
        .expect("snap")
        .0;
        assert_eq!(
            decode_delta_rows(&snap.snapshot_ipc_b64.expect("snapshot present")),
            vec![("US".to_owned(), 12.0)],
            "the deltas the failed mirror drained must have been re-fed to the fallback"
        );
    }

    /// IVM-AUD-DIST-B1: a `/restore` must serialize against an in-flight tick.
    /// Only `/step` and `DELETE` took the per-job step lock, so a rewind could
    /// land mid-tick and be half-undone by the tick's own writes while the
    /// caller was told `{"success": true}`. Same shape as
    /// `delete_waits_for_the_per_job_step_lock`: hold the lock a tick holds and
    /// prove the handler waits. Revert-proof: drop the `_step_guard` line in
    /// `api_ivm_restore` and this fails at the `!restore.is_finished()` assert.
    #[tokio::test]
    async fn restore_waits_for_the_per_job_step_lock() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let checkpoint = api_ivm_checkpoint(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint")
        .0
        .checkpoint_b64;

        let held = registry.step_lock("j").lock_owned().await;
        let restore = tokio::spawn({
            let (registry, coordinator) = (registry.clone(), coordinator.clone());
            async move {
                api_ivm_restore(
                    State(registry),
                    State(coordinator),
                    Path("j".into()),
                    Json(RestoreRequest {
                        checkpoint_b64: checkpoint,
                    }),
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !restore.is_finished(),
            "restore ran straight through a held step lock — it can interleave with a tick"
        );

        drop(held);
        let resp = tokio::time::timeout(Duration::from_secs(2), restore)
            .await
            .expect("restore did not finish within 2s of lock release")
            .expect("restore task panicked");
        assert!(resp.expect("restore must succeed").success);
    }

    /// IVM-AUD-DIST-B1, the delta half. Revert-proof: drop the `_step_guard`
    /// line in `api_ivm_restore_delta`.
    #[tokio::test]
    async fn restore_delta_waits_for_the_per_job_step_lock() {
        let (registry, coordinator) = test_deps_with_shards(1);
        create_revenue_job(&registry, &coordinator, "j").await;
        let delta_ckpt = api_ivm_checkpoint_delta(
            State(registry.clone()),
            State(coordinator.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint-delta")
        .0
        .checkpoint_delta_b64;

        let held = registry.step_lock("j").lock_owned().await;
        let restore = tokio::spawn({
            let (registry, coordinator) = (registry.clone(), coordinator.clone());
            async move {
                api_ivm_restore_delta(
                    State(registry),
                    State(coordinator),
                    Path("j".into()),
                    Json(RestoreDeltaRequest {
                        checkpoint_delta_b64: delta_ckpt,
                    }),
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !restore.is_finished(),
            "restore-delta ran straight through a held step lock"
        );

        drop(held);
        let resp = tokio::time::timeout(Duration::from_secs(2), restore)
            .await
            .expect("restore-delta did not finish within 2s of lock release")
            .expect("restore-delta task panicked");
        assert!(resp.expect("restore-delta must succeed").success);
    }

    /// IVM-AUD-DIST-H6: a `/step` that loses the race with `DELETE` must not
    /// compute a tick it can never persist. Pre-fix it computed the whole tick
    /// on the orphaned flow and then answered 400 from `persist_ivm_job`; the
    /// tick assertion is the one that cannot pass both ways, because a bare
    /// status change could be produced by any number of edits.
    #[tokio::test]
    async fn a_step_that_loses_the_race_with_delete_burns_no_tick() {
        let (registry, _) = test_deps_with_shards(1);
        let coordinator = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("test-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        create_revenue_job(&registry, &coordinator, "j").await;
        // Hold the handle so the flow (and its tick counter) outlives deletion.
        let flow = registry.get("j").expect("job live");
        assert_eq!(flow.tick().unwrap(), 0);

        // A tick is in flight: it has already passed `ensure_ivm_job` and is
        // waiting on the step lock.
        let held = registry.step_lock("j").lock_owned().await;
        let step = tokio::spawn({
            let (registry, coordinator) = (registry.clone(), coordinator.clone());
            async move { api_ivm_step(State(registry), State(coordinator), Path("j".into())).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // DELETE wins: registry entry and durable snapshot both gone.
        registry.delete("j");
        coordinator.remove_ivm_snapshot("j").await.unwrap();
        drop(held);

        let status = tokio::time::timeout(Duration::from_secs(2), step)
            .await
            .expect("step did not finish")
            .expect("step task panicked")
            .expect_err("a step on a deleted job must not succeed");
        // The tick assertion first: it is the behavioural claim. A status-only
        // assertion could be satisfied by any edit that changes the error path.
        assert_eq!(
            flow.tick().unwrap(),
            0,
            "the step must not have burned a tick on the orphaned flow"
        );
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "a step on a job that was deleted under it is a 404, not a 400"
        );
    }

    /// IVM-AUD-DIST-C5: a coordinator with no metadata store persists nothing,
    /// and every handler used to report success anyway. Revert-proof: hardcode
    /// `durable: true` in either handler and the store-less half fails.
    #[tokio::test]
    async fn responses_say_whether_the_write_was_actually_durable() {
        let (registry, coordinator) = test_deps_with_shards(1);
        let created = api_ivm_create_job(
            State(registry.clone()),
            State(coordinator.clone()),
            Json(CreateJobRequest {
                job_id: Some("nostore".into()),
                partitioned: None,
                delta_checkpoints: false,
            }),
        )
        .await
        .expect("create")
        .0;
        assert!(
            !created.durable,
            "a coordinator with no metadata store must not claim a durable create"
        );

        let (registry, _) = test_deps_with_shards(1);
        let stored = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("stored-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        create_revenue_job(&registry, &stored, "j").await;
        let checkpoint = api_ivm_checkpoint(
            State(registry.clone()),
            State(stored.clone()),
            Path("j".into()),
        )
        .await
        .expect("checkpoint")
        .0
        .checkpoint_b64;
        let restored = api_ivm_restore(
            State(registry.clone()),
            State(stored.clone()),
            Path("j".into()),
            Json(RestoreRequest {
                checkpoint_b64: checkpoint,
            }),
        )
        .await
        .expect("restore")
        .0;
        assert!(
            restored.durable,
            "a store-backed coordinator must report the restore as durable"
        );
    }

    /// IVM-AUD-DIST-C4: a job present only as a durable snapshot was listed
    /// with a fabricated `partitioned: false` and no views. Revert-proof: put
    /// `partitioned: false, view_names: Vec::new()` back in the `None` arm.
    #[tokio::test]
    async fn a_snapshot_only_job_is_listed_with_its_persisted_shape() {
        // 3 shards so the GROUP BY view really does auto-partition.
        let registry = std::sync::Arc::new(crate::ivm::IvmJobRegistry::with_default_shards(3));
        let coordinator = SharedCoordinator::new(
            Coordinator::active(CoordinatorId::try_new("test-coord").unwrap())
                .with_store(crate::store::InMemoryMetadataStore::default()),
        );
        create_revenue_job(&registry, &coordinator, "j").await;
        assert!(
            registry.get("j").expect("live").is_partitioned(),
            "precondition: the revenue view must auto-partition at 3 shards"
        );

        // Evict it from this process, leaving only the durable snapshot.
        registry.delete("j");

        let listed = api_ivm_list_jobs(State(registry.clone()), State(coordinator.clone()))
            .await
            .0;
        let entry = listed
            .jobs
            .iter()
            .find(|j| j.job_id == "j")
            .expect("snapshot-only job listed");
        assert!(!entry.live, "precondition: the job is snapshot-only");
        assert!(
            entry.partitioned,
            "a snapshot-only job must report the shape it was persisted with"
        );
        assert_eq!(entry.view_names, vec!["revenue".to_owned()]);
    }

    /// IVM-AUD-DIST-H7: the dispatch/delete bound is configurable, and a
    /// misconfiguration must not turn every tick into an instant timeout.
    #[test]
    fn the_dispatch_timeout_is_configurable_and_rejects_nonsense() {
        assert_eq!(
            resolve_ivm_dispatch_timeout_secs(None),
            DEFAULT_IVM_DISPATCH_TIMEOUT_SECS
        );
        assert_eq!(resolve_ivm_dispatch_timeout_secs(Some(" 45 ")), 45);
        assert_eq!(
            resolve_ivm_dispatch_timeout_secs(Some("0")),
            DEFAULT_IVM_DISPATCH_TIMEOUT_SECS,
            "0 would time out every tick instantly; fall back to the default"
        );
        assert_eq!(
            resolve_ivm_dispatch_timeout_secs(Some("soon")),
            DEFAULT_IVM_DISPATCH_TIMEOUT_SECS
        );
    }

    /// #224 (C): job deletion must serialize against an in-flight `/step` via
    /// the per-job step lock, so a concurrent tick cannot re-persist (resurrect)
    /// the snapshot after deletion removed it. This proves the handler *waits*
    /// on a held step lock rather than racing past it. Without the fix in
    /// `api_ivm_delete_job`, the handler never touches the lock and finishes
    /// immediately even while a tick holds it.
    #[tokio::test]
    async fn delete_waits_for_the_per_job_step_lock() {
        let (registry, coordinator) = test_deps();
        let job_id = "resurrect-me".to_owned();
        registry.create(job_id.clone()).unwrap();

        // Simulate an in-flight /step by holding the same per-job step lock.
        let held = registry.step_lock(&job_id).lock_owned().await;

        let delete = tokio::spawn({
            let (registry, coordinator, job_id) =
                (registry.clone(), coordinator.clone(), job_id.clone());
            async move {
                api_ivm_delete_job(
                    State(registry.clone()),
                    State(coordinator.clone()),
                    Path(job_id),
                )
                .await
            }
        });

        // While the lock is held, deletion must not complete.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !delete.is_finished(),
            "delete completed without waiting on the step lock (resurrection race open)"
        );

        // Releasing the lock lets deletion proceed to completion.
        drop(held);
        let resp = tokio::time::timeout(Duration::from_secs(2), delete)
            .await
            .expect("delete did not finish within 2s of lock release")
            .expect("delete task panicked");
        assert!(
            resp.expect("leader delete must succeed").deleted,
            "job should have been reported deleted"
        );
    }
}
