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
use crate::ivm::SharedIvmJobRegistry;

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

// ── schema JSON ───────────────────────────────────────────────────────────────

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

// ── POST /api/v1/ivm/jobs ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    /// Optional explicit job ID. If absent, a UUID v4 is generated.
    pub job_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub job_id: String,
}

pub async fn api_ivm_create_job(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>, StatusCode> {
    let job_id = body
        .job_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if registry.get(&job_id).is_none() {
        if let Some(snapshot) = coordinator.load_ivm_snapshot(&job_id).await {
            registry
                .restore_durable_snapshot(&job_id, &snapshot)
                .map_err(ivm_err)?;
        } else {
            registry.create(job_id.clone()).map_err(ivm_err)?;
        }
    }
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(CreateJobResponse { job_id }))
}

// ── GET /api/v1/ivm/jobs ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ListJobsResponse {
    pub job_ids: Vec<String>,
}

pub async fn api_ivm_list_jobs(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
) -> Json<ListJobsResponse> {
    let mut job_ids = registry.job_ids();
    job_ids.extend(
        coordinator
            .list_ivm_snapshots()
            .await
            .into_iter()
            .map(|(job_id, _)| job_id),
    );
    job_ids.sort();
    job_ids.dedup();
    Json(ListJobsResponse { job_ids })
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
) -> Json<DeleteJobResponse> {
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
    Json(DeleteJobResponse {
        deleted: registry.delete(&job_id),
    })
}

// ── POST /api/v1/ivm/jobs/{job_id}/views ─────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RegisterViewRequest {
    pub name: String,
    pub body_sql: String,
    pub output_schema: SchemaJson,
    #[serde(default)]
    pub is_materialized: bool,
    #[serde(default)]
    pub is_recursive: bool,
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
    // Existence is enforced by the registry (which also decides, on the first
    // view, whether to auto-partition the job by a single-column GROUP BY key).
    ensure_ivm_job(&registry, &coordinator, &job_id).await?;
    let output_schema =
        parse_schema(&body.output_schema).ok_or_else(|| ivm_err("invalid output_schema"))?;
    let spec = IncrementalViewSpec {
        name: body.name,
        body_sql: body.body_sql,
        output_schema,
        is_materialized: body.is_materialized,
        is_recursive: body.is_recursive,
        lateness: vec![],
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
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
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
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;
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
}

pub async fn api_ivm_step(
    State(registry): State<SharedIvmJobRegistry>,
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<StepResponse>, StatusCode> {
    let flow = ensure_ivm_job(&registry, &coordinator, &job_id).await?;

    // Serialize concurrent steps for this job so two simultaneous ticks cannot
    // drain each other's pending or double-advance the tick counter. Per-job,
    // so independent jobs still step in parallel.
    let step_lock = registry.step_lock(&job_id);
    let _guard = step_lock.lock().await;

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
    let summary = if executor_count > 0 && matches!(flow, crate::ivm::IvmJob::Single(_)) {
        let crate::ivm::IvmJob::Single(inner_flow) = &flow else {
            unreachable!("matched above")
        };
        match submit_resident_ivm_step(&coordinator, &registry, inner_flow, &job_id).await {
            Ok(sum) => sum,
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
                flow.step_datafusion().await.map_err(|e| {
                    tracing::error!("IVM central fallback error for job {job_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?
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
        flow.step_datafusion().await.map_err(|e| {
            tracing::error!("IVM step error for job {job_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    };

    let tick = flow.tick().unwrap_or(0);
    persist_ivm_job(&registry, &coordinator, &job_id).await?;
    Ok(Json(StepResponse {
        active_views: summary.active_views,
        total_output_rows: summary.total_output_rows,
        tick,
    }))
}

/// Timeout for a dispatched IVM fragment before falling back to central compute.
const IVM_DISPATCH_TIMEOUT_SECS: u64 = 300;

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

    // Poll until terminal (bounded by IVM_DISPATCH_TIMEOUT_SECS). The recheck
    // right before sleeping closes the missed-Notify gap (H-20).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(IVM_DISPATCH_TIMEOUT_SECS);
    let succeeded = loop {
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                job_id = %sched_job_id,
                timeout_secs = IVM_DISPATCH_TIMEOUT_SECS,
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
) -> Result<krishiv_ivm::StepSummary, String> {
    // 1. Drain pending locally — never lost: re-fed on any failure below.
    let local_pending = flow.take_pending().map_err(|e| e.to_string())?;
    let dispatch_deltas = coalesce_pending(local_pending.clone()).map_err(|e| e.to_string())?;

    // Nothing to compute: advance the tick structurally and return.
    if dispatch_deltas.is_empty() {
        flow.step_with(|_| Ok(HashMap::new()))
            .map_err(|e| e.to_string())?;
        return Ok(krishiv_ivm::StepSummary::default());
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
        run_ivm_fragment_job(coordinator, attach, "ivm-attach")
            .await
            .map_err(refeed)?;
        // The executor's flow owns the live accumulators from here on; the
        // coordinator's cached plans are stale and must never apply another
        // delta (a later central fallback rebuilds + reseeds from the mirror).
        flow.invalidate_view_plans().map_err(|e| e.to_string())?;
        registry.update_dispatch(ivm_job_id, |d| d.attached = true);
        disp.attached = true;
        tracing::info!(
            job_id = %ivm_job_id,
            state_bytes = state_bytes.len(),
            fence = disp.fence,
            "IVM job attached to resident executor flow"
        );
    }

    // 3. Tick: deltas + fence only (O(Δ) wire, both directions).
    let fence = disp.fence + 1;
    let tick_fragment = krishiv_ivm::encode_ivm_tick_fragment(ivm_job_id, &dispatch_deltas, fence)
        .map_err(|e| refeed(e.to_string()))?;
    let blob = run_ivm_fragment_job(coordinator, tick_fragment, "ivm-tick")
        .await
        .map_err(refeed)?
        .ok_or_else(|| refeed("ivm-tick produced no inline result blob".to_owned()))?;
    let view_deltas = krishiv_ivm::decode_delta_map(&blob)
        .map_err(|e| refeed(format!("decode delta map: {e}")))?;

    // 4. Mirror the tick on the coordinator's authoritative state.
    let summary = flow
        .apply_remote_tick(local_pending, view_deltas)
        .map_err(|e| e.to_string())?;
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
    Ok(summary)
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
    Path(job_id): Path<String>,
) -> Result<Json<DispatchStateResponse>, StatusCode> {
    registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
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
}

pub async fn api_ivm_snapshot(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let rb_opt = flow.snapshot(&view_name).map_err(ivm_err)?;
    match rb_opt {
        None => Ok(Json(SnapshotResponse {
            snapshot_ipc_b64: None,
            num_rows: 0,
        })),
        Some(rb) => {
            let num_rows = rb.num_rows();
            let delta = DeltaBatch::from_inserts(rb).map_err(ivm_err)?;
            let ipc = serialize_delta_batch(&delta).map_err(ivm_err)?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(SnapshotResponse {
                snapshot_ipc_b64: Some(b64),
                num_rows,
            }))
        }
    }
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/output ───────────────────

#[derive(Debug, Serialize)]
pub struct ViewOutputResponse {
    /// Base64-encoded Arrow IPC of the latest delta (may be None if no output yet).
    pub delta_ipc_b64: Option<String>,
    pub num_rows: usize,
}

pub async fn api_ivm_view_output(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<ViewOutputResponse>, StatusCode> {
    let job = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    // Peek the latest output delta (merged across shards for partitioned jobs).
    match job.view_output_peek(&view_name).map_err(ivm_err)? {
        None => Ok(Json(ViewOutputResponse {
            delta_ipc_b64: None,
            num_rows: 0,
        })),
        Some(delta) => {
            let num_rows = delta.num_rows();
            let ipc = serialize_delta_batch(&delta).map_err(ivm_err)?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(ViewOutputResponse {
                delta_ipc_b64: Some(b64),
                num_rows,
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
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<ViewStatsResponse>, StatusCode> {
    let job = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
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
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<ViewDebugInfo>, StatusCode> {
    let job = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
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
    Path(job_id): Path<String>,
) -> Result<Json<CheckpointResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
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
}

pub async fn api_ivm_restore(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RestoreRequest>,
) -> Result<Json<RestoreResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // Matches `api_ivm_checkpoint`'s full checkpoint (sources + view baselines).
    flow.restore_full(&bytes).map_err(ivm_err)?;
    Ok(Json(RestoreResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/checkpoint-delta ──────────────────────────

#[derive(Debug, Serialize)]
pub struct CheckpointDeltaResponse {
    /// Base64-encoded delta checkpoint bytes.
    pub checkpoint_delta_b64: String,
}

pub async fn api_ivm_checkpoint_delta(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
) -> Result<Json<CheckpointDeltaResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
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
}

pub async fn api_ivm_restore_delta(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RestoreDeltaRequest>,
) -> Result<Json<RestoreDeltaResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_delta_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    flow.restore_delta(&bytes).map_err(ivm_err)?;
    Ok(Json(RestoreDeltaResponse { success: true }))
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
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<StreamBridgeRequest>,
) -> Result<Json<StreamBridgeResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
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
}

pub async fn api_ivm_register_vector_view(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RegisterVectorViewRequest>,
) -> Result<Json<RegisterVectorViewResponse>, StatusCode> {
    use krishiv_ivm::VectorViewSpec;

    let job = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;

    if body.sink_type != "in_memory" {
        return Err(ivm_err(format!(
            "unsupported sink_type '{}'; only 'in_memory' is supported via HTTP",
            body.sink_type
        )));
    }

    let sink: Arc<dyn krishiv_ivm::IvmVectorSink> = krishiv_ivm::InMemoryVectorSink::new();
    let spec = VectorViewSpec {
        view_name: body.view_name.clone(),
        id_column: body.id_column.clone(),
        vector_column: body.vector_column.clone(),
        sink,
    };

    // Spawn and detach; one task per shard (partitioned jobs write a shared sink).
    // Tasks run until the flow is dropped.
    job.spawn_vector_views(spec).map_err(ivm_err)?;

    Ok(Json(RegisterVectorViewResponse {
        success: true,
        view_name: body.view_name,
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
            post(api_ivm_register_vector_view),
        )
        .with_state(state)
}
