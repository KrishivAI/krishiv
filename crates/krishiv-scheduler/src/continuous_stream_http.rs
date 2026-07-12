//! HTTP handlers for continuous streaming queries.
//!
//! All three endpoints (register / push / drain) are coordinator-mediated:
//! push stores batches as InlineIpc input partitions in the coordinator's job
//! state; drain returns results from the coordinator's inline result store.
//! This removes the direct executor gRPC path that bypassed the coordinator,
//! enforcing the same single-owner scheduling and task-delivery path as other
//! jobs. Cycle input and output buffers remain coordinator-memory state and do
//! not establish an exactly-once recovery guarantee.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use krishiv_plan::TypedTaskFragment;
use krishiv_plan::window::{WindowExecutionSpec, decode_window_execution_spec};
use krishiv_proto::{InputPartition, InputPartitionDescriptor, JobId, JobKind};
use serde::{Deserialize, Serialize};

use crate::{Coordinator, SchedulerError, SharedCoordinator};

fn scheduler_status(error: &SchedulerError) -> StatusCode {
    match error {
        SchedulerError::DuplicateJob { .. } => StatusCode::CONFLICT,
        SchedulerError::UnknownJob { .. } => StatusCode::NOT_FOUND,
        SchedulerError::InvalidJob { .. } => StatusCode::CONFLICT,
        SchedulerError::InactiveCoordinator { .. } => StatusCode::SERVICE_UNAVAILABLE,
        SchedulerError::NoExecutors | SchedulerError::ExecutorUnavailable { .. } => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Debug, Deserialize)]
pub struct ContinuousRegisterRequest {
    pub job_id: String,
    pub spec: WindowExecutionSpec,
    /// Optional streaming Iceberg sink (G7): cycle output is staged under
    /// checkpoint epochs and committed by the checkpoint lifecycle.
    #[serde(default)]
    pub sink: Option<ContinuousSinkSpec>,
    /// Phase 55: number of run-loop subtasks. `1` (default) with the default
    /// mode keeps the certified cycle-push model; values > 1 require (and
    /// imply) the run-loop model.
    #[serde(default)]
    pub parallelism: Option<u32>,
    /// Phase 55 execution model: `"cycle"` (default — coordinator-fenced
    /// cycle-push, the G8-certified path) or `"run-loop"` (promoted
    /// long-lived barrier-loop tasks).
    #[serde(default)]
    pub mode: Option<String>,
    /// Phase 55: registry connector sources the run-loop subtasks own
    /// directly (kind + table + connector config). Ignored for cycle mode.
    #[serde(default)]
    pub sources: Vec<ContinuousRegistrySource>,
    /// Phase 55: barrier checkpoint interval for run-loop jobs (ms). Enables
    /// the coordinator-driven barrier pipeline; requires
    /// `checkpoint_storage_path`.
    #[serde(default)]
    pub checkpoint_interval_ms: Option<u64>,
    /// Checkpoint storage path (file: URI or directory) for run-loop jobs.
    #[serde(default)]
    pub checkpoint_storage_path: Option<String>,
}

/// One registry connector source owned by run-loop subtasks (Phase 55).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousRegistrySource {
    /// Connector kind (e.g. `kafka`, `parquet-dir`).
    pub kind: String,
    /// Logical table/topic name.
    pub table: String,
    /// Connector properties (broker addresses, topic, paths, …).
    #[serde(default)]
    pub config: std::collections::BTreeMap<String, String>,
}

/// Phase 55 execution model for a continuous job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuousJobMode {
    /// Coordinator-fenced cycle-push (the G8-certified escape hatch).
    Cycle,
    /// Promoted long-lived run-loop tasks (`stream:rloop:`).
    RunLoop,
}

impl ContinuousJobMode {
    fn parse(mode: Option<&str>, parallelism: u32) -> Result<Self, String> {
        match mode.map(str::trim) {
            None | Some("") | Some("cycle") | Some("cycle-push") => {
                if parallelism > 1 {
                    Err(format!(
                        "parallelism {parallelism} requires mode \"run-loop\"; \
                         the cycle model is single-subtask by contract"
                    ))
                } else {
                    Ok(Self::Cycle)
                }
            }
            Some("run-loop") | Some("barrier-loop") | Some("rloop") => Ok(Self::RunLoop),
            Some(other) => Err(format!(
                "unknown continuous mode '{other}' (expected \"cycle\" or \"run-loop\")"
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cycle => "cycle-push",
            Self::RunLoop => "run-loop",
        }
    }
}

/// Streaming Iceberg sink target for a continuous job (G7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousSinkSpec {
    /// Local table root directory on the executor host.
    pub root: String,
    /// Iceberg table name inside the root.
    pub table: String,
    /// `append` (default) or `upsert`.
    #[serde(default = "default_sink_mode")]
    pub mode: String,
    /// Key columns identifying a logical row (required for upsert).
    #[serde(default)]
    pub key_columns: Vec<String>,
    /// Optional column carrying per-row ops (`upsert`/`delete`).
    #[serde(default)]
    pub op_column: Option<String>,
}

fn default_sink_mode() -> String {
    String::from("append")
}

impl ContinuousSinkSpec {
    /// Build the validated string sink contract
    /// (`iceberg-sink:<root>|<table>|mode=...`) carried on the task spec.
    fn contract_string(&self) -> crate::SchedulerResult<String> {
        let mut contract = format!(
            "{}{}|{}|mode={}",
            krishiv_proto::ICEBERG_SINK_PREFIX,
            self.root,
            self.table,
            self.mode
        );
        if !self.key_columns.is_empty() {
            contract.push_str(&format!("|keys={}", self.key_columns.join(",")));
        }
        if let Some(op) = &self.op_column {
            contract.push_str(&format!("|op={op}"));
        }
        // Validate through the shared parser so a malformed spec is rejected
        // at registration instead of failing every cycle on the executor.
        match krishiv_proto::OutputContractDescriptor::parse_iceberg_sink(&contract) {
            Some(Ok(_)) => Ok(contract),
            Some(Err(message)) => Err(SchedulerError::InvalidJob { message }),
            None => Err(SchedulerError::InvalidJob {
                message: "iceberg sink contract failed to round-trip".into(),
            }),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ContinuousRegisterResponse {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousJobView {
    pub job_id: String,
    pub state: String,
    pub task_count: usize,
    pub assigned_task_count: usize,
    pub running_task_count: usize,
    pub succeeded_task_count: usize,
    pub failed_task_count: usize,
    pub last_watermark_ms: Option<i64>,
    pub persisted_watermark_ms: Option<i64>,
    pub snapshot_available: bool,
    pub cycle_in_flight: bool,
    /// Delivery-guarantee metadata derived from the job's sink contract and
    /// the connector capability registry (#92) — the platform surfaces this
    /// as delivery-guarantee labels instead of hardcoding claims.
    pub delivery: ContinuousDeliveryView,
    pub spec: WindowExecutionSpec,
}

/// Delivery-guarantee metadata for one continuous job.
///
/// `effective` is the end-to-end label: the weakest guarantee across the
/// checkpointed push source, the sink, and whether the source offsets ride in
/// the sink's commit transaction. It intentionally reports capabilities the
/// coordinator can actually see — never an aspirational claim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousDeliveryView {
    /// Phase 55 execution model: `"cycle-push"` (coordinator-fenced cycles)
    /// or `"run-loop"` (promoted long-lived barrier-loop tasks). Registry
    /// delivery metadata labels the model per the honesty rule.
    #[serde(default = "default_delivery_model")]
    pub model: String,
    /// Number of run-loop subtasks (1 for cycle-push jobs).
    #[serde(default = "default_delivery_parallelism")]
    pub parallelism: u32,
    /// Sink kind (`"iceberg"`) when the job writes through a two-phase sink;
    /// absent when results are only drained from coordinator memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    /// Strongest guarantee the sink's capabilities support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_guarantee: Option<String>,
    /// Whether source offsets are committed atomically with the sink epoch
    /// (they are staged into every checkpoint whenever a sink is attached).
    pub source_offsets_in_sink_transaction: bool,
    /// Effective end-to-end delivery guarantee label:
    /// `best-effort | at-least-once | effectively-once | exactly-once`.
    pub effective: String,
}

fn default_delivery_model() -> String {
    String::from("cycle-push")
}

fn default_delivery_parallelism() -> u32 {
    1
}

fn continuous_delivery_view(record: &crate::JobRecord) -> ContinuousDeliveryView {
    use krishiv_connectors::{DeliveryGuarantee, iceberg_streaming_sink_capabilities};
    let shape = decode_continuous_job_shape(record).ok();
    let (model, parallelism) = shape
        .as_ref()
        .map(|s| (s.mode.as_str().to_owned(), s.parallelism))
        .unwrap_or_else(|| (default_delivery_model(), 1));
    let kafka_sink = record
        .spec
        .stages()
        .first()
        .and_then(|stage| stage.tasks().first())
        .and_then(|task| task.sink_contract())
        .and_then(|contract| {
            match krishiv_proto::OutputContractDescriptor::parse_kafka_sink(contract) {
                Some(Ok(descriptor)) => Some(descriptor),
                _ => None,
            }
        });
    let iceberg_sink = record
        .spec
        .stages()
        .first()
        .and_then(|stage| stage.tasks().first())
        .and_then(|task| task.sink_contract())
        .and_then(|contract| {
            match krishiv_proto::OutputContractDescriptor::parse_iceberg_sink(contract) {
                Some(Ok(descriptor)) => Some(descriptor),
                // A malformed contract would already fail the task on the
                // executor; report it as "no sink" rather than guessing.
                _ => None,
            }
        });
    if iceberg_sink.is_some() {
        let guarantee = iceberg_streaming_sink_capabilities().delivery_guarantee();
        ContinuousDeliveryView {
            model,
            parallelism,
            sink: Some("iceberg".into()),
            sink_guarantee: Some(guarantee.as_str().into()),
            source_offsets_in_sink_transaction: true,
            effective: guarantee.as_str().into(),
        }
    } else if kafka_sink.is_some() {
        // Transactional Kafka sink under the epoch/2PC contract: committed
        // output is exactly-once for `read_committed` consumers; source
        // offsets do NOT ride in the Kafka transaction (they live in the
        // checkpoint), so the honest end-to-end label is effectively-once.
        ContinuousDeliveryView {
            model,
            parallelism,
            sink: Some("kafka".into()),
            sink_guarantee: Some("exactly-once".into()),
            source_offsets_in_sink_transaction: false,
            effective: "effectively-once".into(),
        }
    } else {
        ContinuousDeliveryView {
            model,
            parallelism,
            sink: None,
            sink_guarantee: None,
            source_offsets_in_sink_transaction: false,
            // Checkpointed replay can re-emit a drained cycle after restore;
            // without a transactional sink the honest label stops here.
            effective: DeliveryGuarantee::AtLeastOnce.as_str().into(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ContinuousListResponse {
    pub streams: Vec<ContinuousJobView>,
}

#[derive(Debug, Serialize)]
pub struct ContinuousCheckpointResponse {
    pub job_id: String,
    pub snapshot_b64: Option<String>,
    pub watermark_ms: Option<i64>,
    pub snapshot_available: bool,
    pub spec: WindowExecutionSpec,
}

#[derive(Debug, Deserialize)]
pub struct ContinuousRestoreRequest {
    pub snapshot_b64: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousRestoreResponse {
    pub job_id: String,
    pub restored: bool,
    pub watermark_ms: i64,
}

fn invalid_continuous_job(job_id: &JobId, message: impl Into<String>) -> SchedulerError {
    SchedulerError::InvalidJob {
        message: format!("continuous job {} {}", job_id.as_str(), message.into()),
    }
}

fn decode_continuous_job_spec(
    record: &crate::JobRecord,
) -> crate::SchedulerResult<WindowExecutionSpec> {
    decode_continuous_job_shape(record).map(|shape| shape.spec)
}

/// Decoded identity of a continuous job: its window spec plus the Phase 55
/// execution model and parallelism.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ContinuousJobShape {
    pub spec: WindowExecutionSpec,
    pub mode: ContinuousJobMode,
    pub parallelism: u32,
}

fn decode_continuous_job_shape(
    record: &crate::JobRecord,
) -> crate::SchedulerResult<ContinuousJobShape> {
    let job_id = record.job_id();
    let fragment = record
        .spec
        .stages()
        .first()
        .and_then(|stage| stage.tasks().first())
        .map(|task| task.description())
        .ok_or_else(|| invalid_continuous_job(job_id, "has no continuous task fragment"))?;
    let typed = TypedTaskFragment::decode(fragment)
        .ok_or_else(|| invalid_continuous_job(job_id, "typed fragment decode failed"))?;
    let cycle_prefix = format!("stream:loop:{}|", job_id.as_str());
    let rloop_prefix = format!("stream:rloop:{}|", job_id.as_str());
    if let Some(encoded) = typed.body.strip_prefix(&cycle_prefix) {
        let spec = decode_window_execution_spec(encoded).map_err(|error| {
            invalid_continuous_job(job_id, format!("window spec decode failed: {error}"))
        })?;
        return Ok(ContinuousJobShape {
            spec,
            mode: ContinuousJobMode::Cycle,
            parallelism: 1,
        });
    }
    if let Some(rest) = typed.body.strip_prefix(&rloop_prefix) {
        // `<subtask>/<parallelism>|<window_spec>`
        let (subtask_segment, encoded) = rest.split_once('|').ok_or_else(|| {
            invalid_continuous_job(job_id, "run-loop fragment missing subtask segment")
        })?;
        let parallelism = subtask_segment
            .split_once('/')
            .and_then(|(_, p)| p.trim().parse::<u32>().ok())
            .ok_or_else(|| {
                invalid_continuous_job(job_id, "run-loop fragment has a malformed subtask segment")
            })?;
        let spec = decode_window_execution_spec(encoded).map_err(|error| {
            invalid_continuous_job(job_id, format!("window spec decode failed: {error}"))
        })?;
        return Ok(ContinuousJobShape {
            spec,
            mode: ContinuousJobMode::RunLoop,
            parallelism,
        });
    }
    Err(invalid_continuous_job(
        job_id,
        "does not use a stream:loop or stream:rloop fragment",
    ))
}

fn continuous_job_view(
    coordinator: &Coordinator,
    job_id: &JobId,
) -> crate::SchedulerResult<ContinuousJobView> {
    let job = coordinator
        .job_coordinator(job_id)
        .ok_or_else(|| SchedulerError::UnknownJob {
            job_id: job_id.clone(),
        })?;
    let record = job.read_record();
    if record.spec.kind() != JobKind::Streaming {
        return Err(invalid_continuous_job(job_id, "is not a streaming job"));
    }
    let spec = decode_continuous_job_spec(&record)?;
    let detail = record.detail_snapshot();
    let shape = decode_continuous_job_shape(&record).ok();
    let subtask_watermarks = detail
        .stages()
        .iter()
        .flat_map(|stage| stage.tasks().iter())
        .filter_map(|task| task.last_watermark_ms());
    // Watermarks v2 (Phase 55): a parallel run-loop job's global watermark is
    // the MIN across its subtasks — a max would let one fast subtask drag the
    // watermark past a lagging sibling and late-drop its rows. Subtasks that
    // have never reported are skipped (source idleness is handled per-split
    // inside each subtask). Cycle jobs keep max (single task; historical
    // behavior).
    let last_watermark_ms = if shape
        .as_ref()
        .is_some_and(|s| s.mode == ContinuousJobMode::RunLoop)
    {
        subtask_watermarks.min()
    } else {
        subtask_watermarks.max()
    };
    let persisted = coordinator.load_continuous_snapshot(job_id.as_str());
    Ok(ContinuousJobView {
        job_id: job_id.to_string(),
        state: format!("{:?}", detail.job().state()),
        task_count: detail.job().task_count(),
        assigned_task_count: detail.job().assigned_task_count(),
        running_task_count: detail.job().running_task_count(),
        succeeded_task_count: detail.job().succeeded_task_count(),
        failed_task_count: detail.job().failed_task_count(),
        last_watermark_ms,
        persisted_watermark_ms: persisted.as_ref().map(|snapshot| snapshot.watermark_ms),
        snapshot_available: persisted.is_some(),
        cycle_in_flight: coordinator.continuous_input_cycles.contains(job_id),
        delivery: continuous_delivery_view(&record),
        spec,
    })
}

pub async fn api_continuous_register(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterRequest>,
) -> Result<Json<ContinuousRegisterResponse>, StatusCode> {
    // Encode/spec errors are a client fault -> 400 (unlike the SQL entrypoint,
    // whose caller already compiled a valid spec).
    if JobId::try_new(&body.job_id).is_err()
        || krishiv_plan::window::encode_window_execution_spec(&body.spec).is_err()
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let options = ContinuousRegistrationOptions {
        sink: body.sink.clone(),
        parallelism: body.parallelism,
        mode: body.mode.clone(),
        sources: body.sources.clone(),
        checkpoint_interval_ms: body.checkpoint_interval_ms,
        checkpoint_storage_path: body.checkpoint_storage_path.clone(),
    };
    register_continuous_stream_with_options(&coordinator, &body.job_id, &body.spec, &options)
        .await
        .map_err(|error| match error {
            ContinuousStreamError::Scheduler(e) => scheduler_status(&e),
            ContinuousStreamError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    Ok(Json(ContinuousRegisterResponse { success: true }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousRegisterSqlRequest {
    pub job_id: String,
    /// A windowed streaming query:
    /// `SELECT key, AGG(col) FROM TUMBLE(TABLE src, DESCRIPTOR(ts), <ms>) GROUP BY key`.
    pub sql: String,
    /// Optional streaming Iceberg sink (G7).
    #[serde(default)]
    pub sink: Option<ContinuousSinkSpec>,
    /// Phase 55: run-loop subtask count (see [`ContinuousRegisterRequest`]).
    #[serde(default)]
    pub parallelism: Option<u32>,
    /// Phase 55: `"cycle"` (default) or `"run-loop"`.
    #[serde(default)]
    pub mode: Option<String>,
    /// Phase 55: registry connector sources for run-loop subtasks.
    #[serde(default)]
    pub sources: Vec<ContinuousRegistrySource>,
    #[serde(default)]
    pub checkpoint_interval_ms: Option<u64>,
    #[serde(default)]
    pub checkpoint_storage_path: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ContinuousRegisterSqlResponse {
    pub success: bool,
    /// The source table the window reads from (feed pushes target it).
    pub source: String,
}

/// Register a continuous streaming job from **SQL**: the coordinator compiles
/// the windowed query to a [`WindowExecutionSpec`] itself
/// (`krishiv_sql::streaming_window_plan`), so callers (the platform pipeline
/// reconciler) pass SQL and stay decoupled from the operator spec type.
pub async fn api_continuous_register_sql(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterSqlRequest>,
) -> Result<Json<ContinuousRegisterSqlResponse>, StatusCode> {
    let plan = krishiv_sql::streaming_window_plan::compile_streaming_window_sql(&body.sql)
        .map_err(|error| {
            tracing::warn!(error = %error, "continuous-register-sql: compile failed");
            StatusCode::BAD_REQUEST
        })?;
    let options = ContinuousRegistrationOptions {
        sink: body.sink.clone(),
        parallelism: body.parallelism,
        mode: body.mode.clone(),
        sources: body.sources.clone(),
        checkpoint_interval_ms: body.checkpoint_interval_ms,
        checkpoint_storage_path: body.checkpoint_storage_path.clone(),
    };
    register_continuous_stream_with_options(&coordinator, &body.job_id, &plan.spec, &options)
        .await
        .map_err(|error| match error {
            ContinuousStreamError::Scheduler(e) => scheduler_status(&e),
            ContinuousStreamError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    Ok(Json(ContinuousRegisterSqlResponse {
        success: true,
        source: plan.source,
    }))
}

pub async fn api_continuous_list(
    State(coordinator): State<SharedCoordinator>,
) -> Result<Json<ContinuousListResponse>, StatusCode> {
    let streams = {
        let coord = coordinator.read().await;
        let mut streams = coord
            .job_snapshots()
            .into_iter()
            .filter(|job| job.kind() == JobKind::Streaming)
            .filter_map(|job| continuous_job_view(&coord, job.job_id()).ok())
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        streams
    };
    Ok(Json(ContinuousListResponse { streams }))
}

pub async fn api_continuous_get(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousJobView>, StatusCode> {
    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let view = {
        let coord = coordinator.read().await;
        continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?
    };
    Ok(Json(view))
}

#[derive(Debug, Serialize)]
pub struct ContinuousDeregisterResponse {
    pub cancelled: bool,
}

/// Tear down a continuous streaming job: cancel it (stops the loop and pushes
/// the cancel RPC to the executor), then evict it from the registry so its
/// `job_id` is freed for re-registration — cancel alone leaves a terminal
/// tombstone that would make a later register of the same id conflict. This is
/// the teardown leg the pipeline reconciler drives when a windowed streaming
/// table is dropped or replaced. Verifies the job is a streaming job before
/// cancelling, so an errant DELETE cannot cancel a batch/IVM job.
pub async fn api_continuous_deregister(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousDeregisterResponse>, StatusCode> {
    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut coord = coordinator.write().await;
    // Confirm it exists and is a streaming job (404 if unknown, 409 otherwise).
    continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?;
    // push_cancel_job (not plain cancel_job): the assigned executor must hear
    // about the teardown so it retires the job identity — drops the stateful
    // `stream:loop` executor and the inbox dedupe entries. Without the RPC, a
    // recreated job reusing the same deterministic ids has its first cycle
    // silently swallowed as an at-least-once duplicate.
    coord
        .push_cancel_job(&job_id)
        .await
        .map_err(|error| scheduler_status(&error))?;
    // Cancel is terminal → evict removes it from `job_coordinators`, freeing the id.
    coord.evict_completed_job(&job_id);
    // A job id can be reused (a fresh `continuous-register-sql` with the same
    // id after deregister is a normal, supported pattern). Clear the retired
    // job's persisted checkpoint so the next job with this id starts clean
    // instead of silently inheriting a stale watermark/state.
    coord.remove_continuous_snapshot(job_id.as_str());
    Ok(Json(ContinuousDeregisterResponse { cancelled: true }))
}

pub async fn api_continuous_checkpoint(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousCheckpointResponse>, StatusCode> {
    use base64::Engine as _;

    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let response = {
        let coord = coordinator.read().await;
        let view =
            continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?;
        let persisted = coord.load_continuous_snapshot(job_id.as_str());
        ContinuousCheckpointResponse {
            job_id: view.job_id,
            snapshot_b64: persisted.as_ref().map(|snapshot| {
                base64::engine::general_purpose::STANDARD.encode(&snapshot.snapshot_bytes)
            }),
            watermark_ms: persisted.as_ref().map(|snapshot| snapshot.watermark_ms),
            snapshot_available: persisted.is_some(),
            spec: view.spec,
        }
    };
    Ok(Json(response))
}

pub async fn api_continuous_restore(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<ContinuousRestoreRequest>,
) -> Result<Json<ContinuousRestoreResponse>, StatusCode> {
    use base64::Engine as _;

    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let snapshot_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.snapshot_b64.as_bytes())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if snapshot_bytes.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let watermark_ms = {
        let mut coord = coordinator.write().await;
        let view =
            continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?;
        let watermark_ms = view
            .persisted_watermark_ms
            .or(view.last_watermark_ms)
            .unwrap_or(i64::MIN);
        let snapshot = crate::ContinuousSnapshot {
            snapshot_bytes,
            watermark_ms,
        };
        coord
            .pending_continuous_restores
            .insert(job_id.clone(), snapshot.clone());
        coord.save_continuous_snapshot(job_id.as_str(), snapshot);
        // Keep the existing streaming job active; the restore is applied on the
        // next fenced cycle assignment, not out-of-band.
        watermark_ms
    };
    Ok(Json(ContinuousRestoreResponse {
        job_id: job_id.to_string(),
        restored: true,
        watermark_ms,
    }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousPushRequest {
    pub job_id: String,
    pub input_batches_b64: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousPushResponse {
    pub success: bool,
}

/// Ingest/egress targets of a run-loop job: `(task_id, endpoint)` per
/// subtask. `None` when the job is not a run-loop job.
fn run_loop_targets(
    coord: &Coordinator,
    job_id: &JobId,
) -> crate::SchedulerResult<Option<Vec<(String, String)>>> {
    let Some(jc) = coord.job_coordinator(job_id) else {
        return Err(crate::SchedulerError::UnknownJob {
            job_id: job_id.clone(),
        });
    };
    let record = jc.read_record();
    if record.spec.kind() != JobKind::Streaming {
        return Ok(None);
    }
    let shape = decode_continuous_job_shape(&record)?;
    if shape.mode != ContinuousJobMode::RunLoop {
        return Ok(None);
    }
    let mut targets = Vec::new();
    for stage in record.stages() {
        for task in stage.tasks() {
            let Some(executor_id) = task.assigned_executor() else {
                continue;
            };
            if let Some(endpoint) = coord.find_executor_endpoint(executor_id) {
                targets.push((task.task_id().as_str().to_owned(), endpoint));
            }
        }
    }
    Ok(Some(targets))
}

/// Monotonic round-robin cursor for external pushes into run-loop jobs.
static RUN_LOOP_PUSH_CURSOR: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Push external input into a run-loop job: the bytes go straight to ONE
/// subtask executor over `push_continuous_input` (round-robin); the keyed
/// exchange re-routes rows to their owning subtasks. The coordinator never
/// buffers the data — control-plane-only (Phase 55).
async fn push_run_loop_input(
    coordinator: &SharedCoordinator,
    job_id: &JobId,
    targets: Vec<(String, String)>,
    ipc_bytes: Vec<u8>,
) -> Result<(), ContinuousStreamError> {
    use krishiv_proto::{TaskId, TransportVersion, wire};
    if targets.is_empty() {
        return Err(ContinuousStreamError::Unavailable(format!(
            "run-loop job {job_id} has no launched subtasks to push to"
        )));
    }
    let cursor =
        RUN_LOOP_PUSH_CURSOR.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % targets.len();
    let Some((task_id, endpoint)) = targets.get(cursor).cloned() else {
        return Err(ContinuousStreamError::Unavailable(String::from(
            "run-loop push target selection failed",
        )));
    };
    if crate::is_in_process_task_endpoint(&endpoint) {
        return Err(ContinuousStreamError::Unavailable(String::from(
            "run-loop push cannot reach an in-process-only executor over gRPC",
        )));
    }
    let channels = coordinator.read().await.executor_channels.clone();
    let channel = Coordinator::get_or_connect_channel_on_map(&channels, &endpoint)
        .await
        .map_err(ContinuousStreamError::Scheduler)?;
    let max = krishiv_proto::max_grpc_message_bytes();
    let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
        channel,
        crate::coordinator::task_assignment::inject_executor_task_request_context
            as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
    )
    .max_decoding_message_size(max)
    .max_encoding_message_size(max);
    let request = krishiv_proto::task::PushContinuousInputRequest {
        version: TransportVersion::CURRENT,
        job_id: job_id.clone(),
        task_id: TaskId::try_new(&task_id).map_err(|e| invalid_registration(e.to_string()))?,
        ipc_bytes,
    };
    client
        .push_continuous_input(wire::push_continuous_input_request_to_wire(request))
        .await
        .map_err(|status| {
            ContinuousStreamError::Unavailable(format!(
                "run-loop push to {endpoint} failed: {status}"
            ))
        })?;
    Ok(())
}

/// Drain a run-loop job's egress: fan `drain_continuous_output` out to each
/// distinct executor hosting a subtask and concatenate the IPC payloads.
async fn drain_run_loop_output(
    coordinator: &SharedCoordinator,
    job_id: &JobId,
    targets: Vec<(String, String)>,
) -> Result<Vec<Vec<u8>>, ContinuousStreamError> {
    use krishiv_proto::{TaskId, TransportVersion, wire};
    let channels = coordinator.read().await.executor_channels.clone();
    let mut seen_endpoints = std::collections::BTreeSet::new();
    let mut payloads = Vec::new();
    for (task_id, endpoint) in targets {
        if !seen_endpoints.insert(endpoint.clone()) {
            continue;
        }
        if crate::is_in_process_task_endpoint(&endpoint) {
            continue;
        }
        let channel = Coordinator::get_or_connect_channel_on_map(&channels, &endpoint)
            .await
            .map_err(ContinuousStreamError::Scheduler)?;
        let max = krishiv_proto::max_grpc_message_bytes();
        let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
            channel,
            crate::coordinator::task_assignment::inject_executor_task_request_context
                as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        )
        .max_decoding_message_size(max)
        .max_encoding_message_size(max);
        let request = krishiv_proto::task::DrainContinuousOutputRequest {
            version: TransportVersion::CURRENT,
            job_id: job_id.clone(),
            task_id: TaskId::try_new(&task_id)
                .map_err(|e| invalid_registration(e.to_string()))?,
        };
        let response = client
            .drain_continuous_output(wire::drain_continuous_output_request_to_wire(request))
            .await
            .map_err(|status| {
                ContinuousStreamError::Unavailable(format!(
                    "run-loop drain from {endpoint} failed: {status}"
                ))
            })?
            .into_inner();
        let decoded = wire::drain_continuous_output_response_from_wire(response)
            .map_err(|e| invalid_registration(e.to_string()))?;
        if !decoded.ipc_bytes.is_empty() {
            payloads.push(decoded.ipc_bytes);
        }
    }
    Ok(payloads)
}

#[derive(Debug, Serialize)]
pub struct ContinuousStopWithSavepointResponse {
    pub job_id: String,
    /// Savepoint epoch the barrier carries; the job stops once it commits.
    pub savepoint_epoch: u64,
}

/// Phase 55 Leg H: stop a continuous job with a savepoint — the rescale cut.
///
/// Triggers `stop_job_with_savepoint`: a savepoint barrier flows through the
/// job like a normal checkpoint; when every task acks and the epoch commits
/// (copied into the immutable savepoints area), the coordinator cancels the
/// job. Changing parallelism = stop-with-savepoint → re-register → restore
/// (the key-group redistribution mechanism lands in Phase 56).
pub async fn api_continuous_stop_with_savepoint(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousStopWithSavepointResponse>, StatusCode> {
    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut coord = coordinator.write().await;
    // 404 unknown / 409 non-streaming, matching the other continuous routes.
    continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?;
    let epoch = coord
        .stop_job_with_savepoint(&job_id, Some(String::from("continuous-stop")))
        .map_err(|error| scheduler_status(&error))?;
    Ok(Json(ContinuousStopWithSavepointResponse {
        job_id: job_id.to_string(),
        savepoint_epoch: epoch,
    }))
}

/// Dispatch one serialized input cycle through the job's retained window state.
///
/// The coordinator fences concurrent pushes, attaches the input as an InlineIpc
/// partition, and delivers a normal task assignment to the job's active
/// executor. The executor reports cycle output through the existing task-result
/// path.
pub async fn api_continuous_push(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousPushRequest>,
) -> Result<Json<ContinuousPushResponse>, StatusCode> {
    use base64::Engine as _;
    let ipc_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.input_batches_b64.as_bytes())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if ipc_bytes.is_empty()
        || crate::batch_sql::decode_inline_record_batches(std::slice::from_ref(&ipc_bytes))
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .is_empty()
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    let job_id =
        krishiv_proto::JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Phase 55: run-loop jobs receive pushes directly on their executors —
    // no coordinator fencing, no coordinator-buffered data (control-plane-
    // only invariant). The push is ingest API, never the execution driver.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id).map_err(|error| scheduler_status(&error))?
    };
    if let Some(targets) = run_loop {
        push_run_loop_input(&coordinator, &job_id, targets, ipc_bytes)
            .await
            .map_err(|error| match error {
                ContinuousStreamError::Scheduler(e) => scheduler_status(&e),
                _ => StatusCode::SERVICE_UNAVAILABLE,
            })?;
        return Ok(Json(ContinuousPushResponse { success: true }));
    }

    let partition = InputPartition::typed(
        "continuous-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: String::from("input"),
            ipc_bytes,
        },
    );

    let (targets, channels, target_count) = {
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id, vec![partition])
            .map_err(|error| scheduler_status(&error))?;
        let assignments = match coord.launch_assigned_task_assignments(&job_id) {
            Ok(assignments) if !assignments.is_empty() => assignments,
            Ok(_) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(scheduler_status(&error));
            }
        };
        let targets = match coord.resolve_assignment_targets(assignments) {
            Ok(targets) => targets,
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(scheduler_status(&error));
            }
        };
        if targets
            .iter()
            .any(|(endpoint, _)| crate::is_in_process_task_endpoint(endpoint))
        {
            coord.abort_continuous_input_cycle(&job_id);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let target_count = targets.len();
        (targets, coord.executor_channels.clone(), target_count)
    };

    let responses =
        match Coordinator::deliver_assignment_targets_with_channels(channels, targets).await {
            Ok(responses) => responses,
            Err(_) => {
                coordinator
                    .write()
                    .await
                    .abort_continuous_input_cycle(&job_id);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };
    let mut coord = coordinator.write().await;
    if !coord.continuous_input_cycles.contains(&job_id) {
        return Err(StatusCode::CONFLICT);
    }
    let accepted = coord.apply_assignment_dispatch_responses(&job_id, &responses);
    if accepted != target_count {
        coord.abort_continuous_input_cycle(&job_id);
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(ContinuousPushResponse { success: true }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousDrainRequest {
    pub job_id: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousDrainResponse {
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

/// Return newly emitted window batches from the coordinator's inline result store.
///
/// Results are written by the executor after processing a fenced `stream:loop`
/// cycle and are consumed once from the coordinator's in-memory result store.
///
/// **Delivery guarantee (DUR-5): best-effort, not durable.** The inline result
/// store is coordinator RAM; a restart before the client drains loses those
/// windows permanently (input already consumed). This holds even under a
/// durable profile — see [`drain_continuous_stream_coordinated`] for the full
/// note and the durable alternatives (transactional sink / queryable state).
pub async fn api_continuous_drain(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousDrainRequest>,
) -> Result<Json<ContinuousDrainResponse>, StatusCode> {
    let job_id =
        krishiv_proto::JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Phase 55: run-loop jobs serve their egress buffers from the executors.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id).map_err(|error| scheduler_status(&error))?
    };
    if let Some(targets) = run_loop {
        let payloads = drain_run_loop_output(&coordinator, &job_id, targets)
            .await
            .map_err(|error| match error {
                ContinuousStreamError::Scheduler(e) => scheduler_status(&e),
                _ => StatusCode::SERVICE_UNAVAILABLE,
            })?;
        return Ok(Json(ContinuousDrainResponse {
            inline_record_batch_ipc: payloads,
        }));
    }

    let batches = {
        let mut coord = coordinator.write().await;
        let snapshot = coord
            .job_snapshot(&job_id)
            .map_err(|error| scheduler_status(&error))?;
        if snapshot.kind() != krishiv_proto::JobKind::Streaming {
            return Err(StatusCode::CONFLICT);
        }
        coord.take_job_inline_results(&job_id).unwrap_or_default()
    };

    Ok(Json(ContinuousDrainResponse {
        inline_record_batch_ipc: batches,
    }))
}

// -------------------------------------------------------------------------
// Public programmatic API — no HTTP types.
// Used by co-located services (e.g., Flight SQL sidecar) that call the
// coordinator directly without an HTTP round-trip.
// -------------------------------------------------------------------------

/// Error returned by the programmatic continuous-stream helpers.
#[derive(Debug)]
pub enum ContinuousStreamError {
    /// A `SchedulerError` wrapped for external callers.
    Scheduler(crate::SchedulerError),
    /// The push cycle was aborted (e.g., no executor available).
    Unavailable(String),
    /// A cycle was aborted because it conflicted with the current state.
    Aborted(String),
}

impl std::fmt::Display for ContinuousStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheduler(e) => write!(f, "scheduler error: {e}"),
            Self::Unavailable(msg) => write!(f, "unavailable: {msg}"),
            Self::Aborted(msg) => write!(f, "aborted: {msg}"),
        }
    }
}

impl std::error::Error for ContinuousStreamError {}

impl From<crate::SchedulerError> for ContinuousStreamError {
    fn from(e: crate::SchedulerError) -> Self {
        Self::Scheduler(e)
    }
}

/// Register a new continuous streaming job with the coordinator.
///
/// This is the programmatic equivalent of `api_continuous_register` — it calls
/// the same coordinator methods without serialising to HTTP.
///
/// The job is identified by `job_id` and parameterised by `spec`.
pub async fn register_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
) -> Result<(), ContinuousStreamError> {
    register_continuous_stream_with_sink(coordinator, job_id, spec, None).await
}

/// Register a continuous streaming job, optionally attaching a streaming
/// Iceberg sink contract (G7) so cycle output lands in an Iceberg table
/// under checkpoint-aligned two-phase commit.
pub async fn register_continuous_stream_with_sink(
    coordinator: &SharedCoordinator,
    job_id: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
    sink: Option<&ContinuousSinkSpec>,
) -> Result<(), ContinuousStreamError> {
    let options = ContinuousRegistrationOptions {
        sink: sink.cloned(),
        ..Default::default()
    };
    register_continuous_stream_with_options(coordinator, job_id, spec, &options).await
}

/// Full registration options for a continuous streaming job (Phase 55).
#[derive(Debug, Clone, Default)]
pub struct ContinuousRegistrationOptions {
    /// Optional streaming Iceberg sink (G7 cycle model / barrier model).
    pub sink: Option<ContinuousSinkSpec>,
    /// Run-loop subtask count (defaults to 1).
    pub parallelism: Option<u32>,
    /// `"cycle"` (default) or `"run-loop"`.
    pub mode: Option<String>,
    /// Registry connector sources owned by run-loop subtasks.
    pub sources: Vec<ContinuousRegistrySource>,
    /// Barrier checkpoint interval (run-loop jobs).
    pub checkpoint_interval_ms: Option<u64>,
    /// Checkpoint storage path (run-loop jobs).
    pub checkpoint_storage_path: Option<String>,
}

fn invalid_registration(message: impl Into<String>) -> ContinuousStreamError {
    ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
        message: message.into(),
    })
}

/// Build the JobSpec for a continuous job: one `stream:loop:` task in cycle
/// mode, or N `stream:rloop:` subtasks (`task-streaming-<i>`) in run-loop
/// mode. Subtask index order == task order in the stage, so the launch path's
/// `key_group_range_for_task(task_index, stage_parallelism)` stamps exactly
/// the range the run-loop's exchange routes by.
fn build_continuous_job_spec(
    job_id: &krishiv_proto::JobId,
    spec: &WindowExecutionSpec,
    mode: ContinuousJobMode,
    parallelism: u32,
    options: &ContinuousRegistrationOptions,
) -> Result<krishiv_proto::JobSpec, ContinuousStreamError> {
    use krishiv_plan::ExecutionKind;
    use krishiv_plan::window::encode_window_execution_spec;
    use krishiv_proto::{JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};

    let stage_id = StageId::try_new("stage-streaming")
        .map_err(|e| invalid_registration(e.to_string()))?;
    let encoded_spec = encode_window_execution_spec(spec)
        .map_err(|e| invalid_registration(e.to_string()))?;
    let sink_contract = match &options.sink {
        Some(sink) => Some(
            sink.contract_string()
                .map_err(ContinuousStreamError::Scheduler)?,
        ),
        None => None,
    };

    let mut stage = StageSpec::new(stage_id, "continuous-streaming");
    match mode {
        ContinuousJobMode::Cycle => {
            let task_id = TaskId::try_new("task-streaming")
                .map_err(|e| invalid_registration(e.to_string()))?;
            let body = format!("stream:loop:{}|{encoded_spec}", job_id.as_str());
            let fragment = TypedTaskFragment::new(ExecutionKind::Streaming, body)
                .encode()
                .map_err(|e| invalid_registration(e.to_string()))?;
            let mut task = TaskSpec::new(task_id, fragment);
            if let Some(contract) = &sink_contract {
                task = task.with_sink_contract(contract.clone());
            }
            stage = stage.with_task(task);
        }
        ContinuousJobMode::RunLoop => {
            for subtask in 0..parallelism {
                let task_id = TaskId::try_new(format!("task-streaming-{subtask}"))
                    .map_err(|e| invalid_registration(e.to_string()))?;
                let body = format!(
                    "stream:rloop:{}|{subtask}/{parallelism}|{encoded_spec}",
                    job_id.as_str()
                );
                let fragment = TypedTaskFragment::new(ExecutionKind::Streaming, body)
                    .encode()
                    .map_err(|e| invalid_registration(e.to_string()))?;
                let mut task = TaskSpec::new(task_id, fragment);
                if let Some(contract) = &sink_contract {
                    task = task.with_sink_contract(contract.clone());
                }
                stage = stage.with_task(task);
            }
        }
    }

    let mut job_spec = JobSpec::new(job_id.clone(), "continuous-streaming", JobKind::Streaming)
        .with_stage(stage);
    if let (Some(interval), Some(path)) = (
        options.checkpoint_interval_ms,
        options.checkpoint_storage_path.as_deref(),
    ) {
        job_spec = job_spec.with_checkpoint(interval, path);
    }
    Ok(job_spec)
}

/// Register a continuous streaming job with full Phase 55 options. Run-loop
/// jobs are additionally assigned + launched here — the tasks start once and
/// run until stopped (the coordinator stays control-plane-only afterwards).
pub async fn register_continuous_stream_with_options(
    coordinator: &SharedCoordinator,
    job_id: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
    options: &ContinuousRegistrationOptions,
) -> Result<(), ContinuousStreamError> {
    use krishiv_proto::JobId;

    let parallelism = options.parallelism.unwrap_or(1).max(1);
    let mode = ContinuousJobMode::parse(options.mode.as_deref(), parallelism)
        .map_err(invalid_registration)?;
    if mode == ContinuousJobMode::RunLoop
        && options.checkpoint_interval_ms.is_some() != options.checkpoint_storage_path.is_some()
    {
        return Err(invalid_registration(
            "run-loop checkpointing requires BOTH checkpoint_interval_ms and              checkpoint_storage_path (or neither)",
        ));
    }
    let job_id_typed =
        JobId::try_new(job_id).map_err(|e| invalid_registration(e.to_string()))?;
    let job_spec = build_continuous_job_spec(&job_id_typed, spec, mode, parallelism, options)?;

    let freshly_submitted = {
        let mut coord = coordinator.write().await;
        coord
            .ensure_active()
            .map_err(ContinuousStreamError::Scheduler)?;
        upsert_continuous_streaming_job(
            &mut coord,
            &job_id_typed,
            spec,
            mode,
            parallelism,
            job_spec,
        )
        .await
        .map_err(ContinuousStreamError::Scheduler)?
    };

    if mode == ContinuousJobMode::RunLoop && freshly_submitted {
        launch_run_loop_job(coordinator, &job_id_typed, &options.sources).await?;
    }
    Ok(())
}

/// Assign, wire, and launch a freshly registered run-loop job's subtasks.
///
/// Each subtask's input partitions carry (a) the peer table for the keyed
/// exchange (`stream-peers:` — subtask index, task id, executor endpoint) and
/// (b) every registry source descriptor (the subtask filters to the splits it
/// owns). The tasks launch once; from here on the coordinator is
/// control-plane-only for this job.
async fn launch_run_loop_job(
    coordinator: &SharedCoordinator,
    job_id: &krishiv_proto::JobId,
    sources: &[ContinuousRegistrySource],
) -> Result<(), ContinuousStreamError> {
    let (targets, channels, target_count) = {
        let mut coord = coordinator.write().await;
        coord
            .assign_pending_tasks(job_id)
            .map_err(ContinuousStreamError::Scheduler)?;

        // Peer table: every stream:rloop task must be assigned with a
        // resolvable endpoint before launch — the exchange fails closed
        // otherwise.
        let mut peers: Vec<(usize, String, String)> = Vec::new();
        {
            let jc = coord.job_coordinator(job_id).ok_or_else(|| {
                ContinuousStreamError::Scheduler(crate::SchedulerError::UnknownJob {
                    job_id: job_id.clone(),
                })
            })?;
            let job = jc.read_record();
            for stage in job.spec.stages() {
                for (index, task) in stage.tasks().iter().enumerate() {
                    let typed = TypedTaskFragment::decode(task.description());
                    let is_rloop = typed
                        .as_ref()
                        .is_some_and(|t| t.body.starts_with("stream:rloop:"));
                    if !is_rloop {
                        continue;
                    }
                    let assigned = job
                        .stages
                        .iter()
                        .flat_map(|s| s.tasks())
                        .find(|t| t.task_id() == task.task_id())
                        .and_then(|t| t.assigned_executor().cloned());
                    let Some(executor_id) = assigned else {
                        return Err(ContinuousStreamError::Unavailable(format!(
                            "run-loop job {job_id} subtask {index} has no executor                              (register more executors and retry)"
                        )));
                    };
                    let endpoint = coord
                        .find_executor_endpoint(&executor_id)
                        .ok_or_else(|| {
                            ContinuousStreamError::Unavailable(format!(
                                "run-loop job {job_id}: executor {executor_id} has no                                  task endpoint"
                            ))
                        })?;
                    peers.push((index, task.task_id().as_str().to_owned(), endpoint));
                }
            }
        }
        if peers.is_empty() {
            return Err(invalid_registration(format!(
                "job {job_id} has no stream:rloop tasks to launch"
            )));
        }

        let peer_entries: Vec<String> = peers
            .iter()
            .map(|(subtask, task_id, endpoint)| format!("{subtask}={task_id}@{endpoint}"))
            .collect();
        let peers_partition = krishiv_proto::InputPartition::new(
            "stream-peers",
            format!("stream-peers:{}", peer_entries.join(";")),
        );
        let mut source_partitions: Vec<krishiv_proto::InputPartition> = Vec::new();
        for (index, source) in sources.iter().enumerate() {
            let config_json = serde_json::to_string(&source.config).map_err(|e| {
                invalid_registration(format!("source config for '{}': {e}", source.table))
            })?;
            source_partitions.push(krishiv_proto::InputPartition::new(
                format!("registry-src-{index}"),
                format!(
                    "registry-connector:{}:{}:{config_json}",
                    source.kind.trim(),
                    source.table.trim()
                ),
            ));
        }

        let mut per_task: std::collections::HashMap<
            krishiv_proto::TaskId,
            Vec<krishiv_proto::InputPartition>,
        > = std::collections::HashMap::new();
        for (_, task_id, _) in &peers {
            let task_id = krishiv_proto::TaskId::try_new(task_id)
                .map_err(|e| invalid_registration(e.to_string()))?;
            let mut partitions = vec![peers_partition.clone()];
            partitions.extend(source_partitions.iter().cloned());
            per_task.insert(task_id, partitions);
        }
        coord
            .job_task_input_partitions
            .insert(job_id.clone(), per_task);

        let assignments = coord
            .launch_assigned_task_assignments(job_id)
            .map_err(ContinuousStreamError::Scheduler)?;
        if assignments.is_empty() {
            return Err(ContinuousStreamError::Unavailable(format!(
                "run-loop job {job_id} produced no launchable assignments"
            )));
        }
        let targets = coord
            .resolve_assignment_targets(assignments)
            .map_err(ContinuousStreamError::Scheduler)?;
        let count = targets.len();
        (targets, coord.executor_channels.clone(), count)
    };

    let responses = Coordinator::deliver_assignment_targets_with_channels(channels, targets)
        .await
        .map_err(ContinuousStreamError::Scheduler)?;
    let mut coord = coordinator.write().await;
    let accepted = coord.apply_assignment_dispatch_responses(job_id, &responses);
    // In-process targets are filtered before delivery (tests drive their
    // inboxes directly), so only remote responses are counted here.
    if accepted < responses.len() {
        return Err(ContinuousStreamError::Unavailable(format!(
            "run-loop job {job_id}: {accepted}/{target_count} subtask launches accepted"
        )));
    }
    Ok(())
}

/// Convergent (upsert) submission of a continuous streaming job.
///
/// A continuous streaming job is a declarative, desired-state object keyed by
/// `job_id`: the pipeline reconciler re-drives registration to make the running
/// job match `desired_spec`. Registration is therefore an UPSERT, not an insert —
/// unlike generic `submit_job`, which (correctly) rejects a duplicate batch/delta
/// job with `DuplicateJob`.
///
///   - same id, same spec, healthy (non-terminal + decodable) -> idempotent
///     no-op. This preserves streaming continuity; a steady-state reconcile must
///     NOT tear a healthy stream down and recreate it (that would reset window
///     state + watermarks).
///   - same id, but terminal / undecodable (limbo) / different spec -> retire the
///     old job and submit fresh. This heals a wedged entry and applies a genuine
///     spec change.
///   - same id, non-streaming job -> genuine id collision -> `DuplicateJob`.
///
/// `job_spec` is the already-built `JobSpec` for `desired_spec` (both call sites
/// construct it, differing only in how they surface encode errors).
async fn upsert_continuous_streaming_job(
    coord: &mut Coordinator,
    job_id: &JobId,
    desired_spec: &WindowExecutionSpec,
    desired_mode: ContinuousJobMode,
    desired_parallelism: u32,
    job_spec: krishiv_proto::JobSpec,
) -> crate::SchedulerResult<bool> {
    let existing = coord.job_coordinator(job_id).map(|jc| {
        let record = jc.read_record();
        let is_streaming = record.spec.kind() == JobKind::Streaming;
        let terminal = record.state().is_terminal();
        let decoded = decode_continuous_job_shape(&record).ok();
        (is_streaming, terminal, decoded)
    });
    if let Some((is_streaming, terminal, decoded)) = existing {
        if !is_streaming {
            return Err(crate::SchedulerError::DuplicateJob {
                job_id: job_id.clone(),
            });
        }
        let healthy = !terminal && decoded.is_some();
        let desired_shape = ContinuousJobShape {
            spec: desired_spec.clone(),
            mode: desired_mode,
            parallelism: desired_parallelism,
        };
        if healthy && decoded.as_ref() == Some(&desired_shape) {
            // Already running the desired spec/mode/parallelism — nothing to
            // do; a steady-state reconcile must not reset window state.
            return Ok(false);
        }
        // Terminal, limbo, or spec changed: retire the old incarnation so the id
        // is free for a clean re-submit.
        //   1. push_cancel_job best-effort notifies the executor to retire the
        //      stateful stream:loop identity (and cancels in scheduler state).
        //   2. cancel_job unconditionally marks the job terminal — push_cancel_job
        //      can bail during target collection (e.g. a limbo task with no valid
        //      cancel attempt) BEFORE it cancels, which would otherwise leave the
        //      job non-terminal and evict a no-op.
        //   3. evict frees the registry slot; snapshot is cleared so the fresh job
        //      starts clean instead of inheriting a stale watermark/state.
        let _ = coord.push_cancel_job(job_id).await;
        let _ = coord.cancel_job(job_id);
        coord.evict_completed_job(job_id);
        coord.remove_continuous_snapshot(job_id.as_str());
    }

    coord.submit_job(job_spec)?;
    Ok(true)
}

/// Push one cycle of IPC bytes as input for a continuous streaming job.
///
/// This is the programmatic equivalent of `api_continuous_push` — it calls the
/// same coordinator methods without serialising to HTTP.
///
/// `ipc_bytes` must be a valid Arrow IPC stream (non-empty).
pub async fn push_continuous_input_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    ipc_bytes: Vec<u8>,
) -> Result<(), ContinuousStreamError> {
    use krishiv_proto::{InputPartition, InputPartitionDescriptor, JobId};

    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;

    // Phase 55: run-loop jobs receive pushes directly on their executors.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id_typed).map_err(ContinuousStreamError::Scheduler)?
    };
    if let Some(targets) = run_loop {
        return push_run_loop_input(coordinator, &job_id_typed, targets, ipc_bytes).await;
    }

    let partition = InputPartition::typed(
        "continuous-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: String::from("input"),
            ipc_bytes,
        },
    );

    let (targets, channels, target_count) = {
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id_typed, vec![partition])
            .map_err(ContinuousStreamError::Scheduler)?;
        let assignments = match coord.launch_assigned_task_assignments(&job_id_typed) {
            Ok(assignments) if !assignments.is_empty() => assignments,
            Ok(_) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Unavailable(String::from(
                    "no executor available for continuous cycle",
                )));
            }
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Scheduler(error));
            }
        };
        let targets = match coord.resolve_assignment_targets(assignments) {
            Ok(targets) => targets,
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Scheduler(error));
            }
        };
        if targets
            .iter()
            .any(|(endpoint, _)| crate::is_in_process_task_endpoint(endpoint))
        {
            coord.abort_continuous_input_cycle(&job_id_typed);
            return Err(ContinuousStreamError::Unavailable(String::from(
                "continuous push cannot reach in-process-only executor via co-located Flight SQL",
            )));
        }
        let target_count = targets.len();
        (targets, coord.executor_channels.clone(), target_count)
    };

    let responses =
        match Coordinator::deliver_assignment_targets_with_channels(channels, targets).await {
            Ok(responses) => responses,
            Err(_) => {
                coordinator
                    .write()
                    .await
                    .abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Unavailable(String::from(
                    "assignment delivery failed",
                )));
            }
        };

    let mut coord = coordinator.write().await;
    if !coord.continuous_input_cycles.contains(&job_id_typed) {
        return Err(ContinuousStreamError::Aborted(String::from(
            "continuous cycle was aborted concurrently",
        )));
    }
    let accepted = coord.apply_assignment_dispatch_responses(&job_id_typed, &responses);
    if accepted != target_count {
        coord.abort_continuous_input_cycle(&job_id_typed);
        return Err(ContinuousStreamError::Unavailable(String::from(
            "not all assignment targets accepted the cycle",
        )));
    }
    Ok(())
}

/// Drain completed results from a continuous streaming job.
///
/// This is the programmatic equivalent of `api_continuous_drain` — it calls the
/// same coordinator methods without serialising to HTTP.
///
/// Returns IPC byte payloads (one per completed window), or an empty vec if no
/// results are available yet.
///
/// # Delivery guarantee (DUR-5): best-effort, NOT durable
///
/// Undrained windows live only in coordinator RAM (`job_inline_results`). A
/// coordinator restart between cycle completion and drain loses those windows
/// permanently — the input was already consumed, so they are not regenerated.
/// **This path is best-effort even under a durable profile.** A durable profile
/// does not imply drained output survives a restart. For at-least-once /
/// exactly-once delivery that survives coordinator loss, consume via the
/// transactional Iceberg sink or queryable-state snapshots (both durable), not
/// this drain endpoint. (The Phase 55 streamed-results work is the structural
/// retirement of this in-RAM path.)
pub async fn drain_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
) -> Result<Vec<Vec<u8>>, ContinuousStreamError> {
    use krishiv_proto::JobId;

    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;

    // Phase 55: run-loop jobs serve their egress buffers from the executors.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id_typed).map_err(ContinuousStreamError::Scheduler)?
    };
    if let Some(targets) = run_loop {
        return drain_run_loop_output(coordinator, &job_id_typed, targets).await;
    }

    let mut coord = coordinator.write().await;
    let snapshot = coord
        .job_snapshot(&job_id_typed)
        .map_err(ContinuousStreamError::Scheduler)?;
    if snapshot.kind() != krishiv_proto::JobKind::Streaming {
        return Err(ContinuousStreamError::Scheduler(
            crate::SchedulerError::InvalidJob {
                message: format!("job {job_id} is not a streaming job"),
            },
        ));
    }
    Ok(coord
        .take_job_inline_results(&job_id_typed)
        .unwrap_or_default())
}

/// Stage a one-shot continuous-stream restore snapshot for the next cycle.
pub async fn restore_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    snapshot_bytes: Vec<u8>,
) -> Result<(), ContinuousStreamError> {
    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    if snapshot_bytes.is_empty() {
        return Err(ContinuousStreamError::Scheduler(
            crate::SchedulerError::InvalidJob {
                message: format!("continuous job {job_id} restore snapshot must not be empty"),
            },
        ));
    }
    let mut coord = coordinator.write().await;
    let watermark_ms = continuous_job_view(&coord, &job_id_typed)
        .ok()
        .and_then(|view| view.persisted_watermark_ms.or(view.last_watermark_ms))
        .unwrap_or(i64::MIN);
    let snapshot = crate::ContinuousSnapshot {
        snapshot_bytes,
        watermark_ms,
    };
    coord
        .pending_continuous_restores
        .insert(job_id_typed.clone(), snapshot.clone());
    coord.save_continuous_snapshot(job_id, snapshot);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use axum::Json;
    use axum::extract::State;
    use krishiv_plan::window::{WindowExecutionSpec, WindowKind, decode_window_execution_spec};
    use krishiv_proto::{
        CoordinatorId, ExecutorTaskAssignment, TaskStatusResponse, TransportDisposition,
    };

    use crate::{Coordinator, SharedCoordinator};

    async fn make_coordinator_with_executor(suffix: &str) -> SharedCoordinator {
        make_coordinator_with_executor_hb(suffix, None).await
    }

    /// Build a coordinator + one in-process executor, optionally pinning the
    /// heartbeat timeout. Eviction-timing tests MUST pin it: the production
    /// default (`CoordinatorConfig::default()`) was deliberately raised to 9
    /// ticks by the heartbeat/lease reliability audit so a healthy executor
    /// survives a delayed heartbeat, and tests that hardcode a tick budget must
    /// not silently rot when that default moves.
    async fn make_coordinator_with_executor_hb(
        suffix: &str,
        heartbeat_timeout_ticks: Option<u64>,
    ) -> SharedCoordinator {
        use krishiv_proto::{ExecutorDescriptor, ExecutorId};
        let coord_id = CoordinatorId::try_new(format!("coord-cs-{suffix}")).unwrap();
        let coordinator = match heartbeat_timeout_ticks {
            Some(ticks) => {
                let config = crate::CoordinatorConfig::new(1, ticks);
                SharedCoordinator::new(Coordinator::active_with_config(coord_id, config))
            }
            None => SharedCoordinator::new(Coordinator::active(coord_id)),
        };
        let exec_id = ExecutorId::try_new(format!("exec-cs-{suffix}")).unwrap();
        let desc = ExecutorDescriptor::new(exec_id, "localhost", 4)
            .with_task_endpoint(crate::IN_PROCESS_TASK_ENDPOINT);
        coordinator.write().await.register_executor(desc).unwrap();
        coordinator
    }

    fn tumbling_spec() -> WindowExecutionSpec {
        WindowExecutionSpec {
            key_column: "user_id".to_string(),
            key_column_type: String::from("utf8"),
            event_time_column: "ts".to_string(),
            watermark_lag_ms: 0,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 10_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: WindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        }
    }

    fn encoded_input() -> String {
        use base64::Engine as _;

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])) as _,
                Arc::new(Int64Array::from(vec![100_i64, 12_000_i64])) as _,
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        base64::engine::general_purpose::STANDARD.encode(ipc)
    }

    fn input_partition() -> InputPartition {
        use base64::Engine as _;

        InputPartition::typed(
            "continuous-input",
            InputPartitionDescriptor::InlineIpc {
                table_name: String::from("input"),
                ipc_bytes: base64::engine::general_purpose::STANDARD
                    .decode(encoded_input())
                    .unwrap(),
            },
        )
    }

    async fn prepare_cycle(
        coordinator: &SharedCoordinator,
        job_id: &str,
    ) -> ExecutorTaskAssignment {
        let job_id = krishiv_proto::JobId::try_new(job_id).unwrap();
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id, vec![input_partition()])
            .unwrap();
        let mut assignments = coord.launch_assigned_task_assignments(&job_id).unwrap();
        assert_eq!(assignments.len(), 1);
        assignments.remove(0)
    }

    #[tokio::test]
    async fn continuous_mode_parse_rejects_parallel_cycle() {
        assert!(ContinuousJobMode::parse(None, 1).unwrap() == ContinuousJobMode::Cycle);
        assert!(
            ContinuousJobMode::parse(Some("run-loop"), 3).unwrap() == ContinuousJobMode::RunLoop
        );
        assert!(ContinuousJobMode::parse(None, 3).is_err());
        assert!(ContinuousJobMode::parse(Some("bogus"), 1).is_err());
    }

    /// Phase 55: run-loop registration produces N `stream:rloop:` subtasks
    /// whose fragment identity round-trips through the shape decoder, and the
    /// delivery metadata labels the model honestly.
    #[tokio::test]
    async fn run_loop_registration_builds_parallel_subtasks() {
        let coordinator = make_coordinator_with_executor("rloop-reg").await;
        let options = ContinuousRegistrationOptions {
            parallelism: Some(3),
            mode: Some(String::from("run-loop")),
            ..Default::default()
        };
        register_continuous_stream_with_options(
            &coordinator,
            "rloop-reg-job",
            &tumbling_spec(),
            &options,
        )
        .await
        .expect("run-loop registration must succeed");

        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("rloop-reg-job").unwrap();
        let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
        let tasks: Vec<_> = record
            .spec
            .stages()
            .iter()
            .flat_map(|stage| stage.tasks())
            .collect();
        assert_eq!(tasks.len(), 3, "parallelism 3 registers three subtasks");
        for (index, task) in tasks.iter().enumerate() {
            let body = TypedTaskFragment::decode(task.description()).unwrap().body;
            assert!(
                body.starts_with(&format!("stream:rloop:rloop-reg-job|{index}/3|")),
                "subtask {index} fragment carries its identity: {body}"
            );
        }
        let shape = decode_continuous_job_shape(&record).unwrap();
        assert_eq!(shape.mode, ContinuousJobMode::RunLoop);
        assert_eq!(shape.parallelism, 3);
        assert_eq!(shape.spec, tumbling_spec());

        let view = continuous_job_view(&coord, &job_id).unwrap();
        assert_eq!(view.delivery.model, "run-loop");
        assert_eq!(view.delivery.parallelism, 3);
        assert_eq!(view.task_count, 3);
    }

    /// Phase 55: re-registering the same shape is an idempotent no-op, while
    /// a parallelism change retires the old incarnation and resubmits.
    #[tokio::test]
    async fn run_loop_reregistration_is_convergent() {
        let coordinator = make_coordinator_with_executor("rloop-upsert").await;
        let options = ContinuousRegistrationOptions {
            parallelism: Some(2),
            mode: Some(String::from("run-loop")),
            ..Default::default()
        };
        register_continuous_stream_with_options(
            &coordinator,
            "rloop-upsert-job",
            &tumbling_spec(),
            &options,
        )
        .await
        .unwrap();
        // Same shape → no-op (must not error, must keep 2 tasks).
        register_continuous_stream_with_options(
            &coordinator,
            "rloop-upsert-job",
            &tumbling_spec(),
            &options,
        )
        .await
        .unwrap();
        {
            let coord = coordinator.read().await;
            let job_id = krishiv_proto::JobId::try_new("rloop-upsert-job").unwrap();
            let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
            assert_eq!(record.spec.task_count(), 2);
        }
        // Parallelism change → retire + fresh submit at the new parallelism.
        let rescaled = ContinuousRegistrationOptions {
            parallelism: Some(3),
            mode: Some(String::from("run-loop")),
            ..Default::default()
        };
        register_continuous_stream_with_options(
            &coordinator,
            "rloop-upsert-job",
            &tumbling_spec(),
            &rescaled,
        )
        .await
        .unwrap();
        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("rloop-upsert-job").unwrap();
        let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
        assert_eq!(record.spec.task_count(), 3, "rescale re-registers at 3");
    }

    /// Phase 55: cycle-mode registration is bit-for-bit unchanged (the G8
    /// path) — one stream:loop task, delivery model "cycle-push".
    #[tokio::test]
    async fn cycle_registration_shape_is_unchanged() {
        let coordinator = make_coordinator_with_executor("cycle-shape").await;
        register_continuous_stream_coordinated(&coordinator, "cycle-shape-job", &tumbling_spec())
            .await
            .unwrap();
        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cycle-shape-job").unwrap();
        let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
        let shape = decode_continuous_job_shape(&record).unwrap();
        assert_eq!(shape.mode, ContinuousJobMode::Cycle);
        assert_eq!(shape.parallelism, 1);
        let view = continuous_job_view(&coord, &job_id).unwrap();
        assert_eq!(view.delivery.model, "cycle-push");
        assert_eq!(view.task_count, 1);
    }

    #[tokio::test]
    async fn register_succeeds_and_drain_returns_empty() {
        let coordinator = make_coordinator_with_executor("reg-drain").await;

        let register_req = ContinuousRegisterRequest {
            job_id: "cs-test-job".to_string(),
            spec: tumbling_spec(),
            sink: None,
            parallelism: None,
            mode: None,
            sources: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        };
        let response = api_continuous_register(State(coordinator.clone()), Json(register_req))
            .await
            .unwrap();
        assert!(response.0.success, "register must succeed");
        {
            let coord = coordinator.read().await;
            let job_id = krishiv_proto::JobId::try_new("cs-test-job").unwrap();
            let job = coord.job_coordinator(&job_id).expect("registered job");
            let record = job.read_record();
            let fragment = record.spec.stages()[0].tasks()[0].description();
            let body = TypedTaskFragment::decode(fragment)
                .expect("continuous job must use a typed fragment")
                .body;
            let encoded_spec = body
                .strip_prefix("stream:loop:cs-test-job|")
                .expect("continuous task must retain its job id");
            assert_eq!(
                decode_window_execution_spec(encoded_spec).unwrap(),
                tumbling_spec()
            );
        }

        // Drain before any push — should return empty, not error.
        let drain_req = ContinuousDrainRequest {
            job_id: "cs-test-job".to_string(),
        };
        let drain_resp = api_continuous_drain(State(coordinator.clone()), Json(drain_req))
            .await
            .unwrap();
        assert!(
            drain_resp.0.inline_record_batch_ipc.is_empty(),
            "drain before push must return empty results"
        );
    }

    #[tokio::test]
    async fn list_get_and_checkpoint_expose_continuous_job_metadata() {
        use base64::Engine as _;

        let coordinator = make_coordinator_with_executor("list-checkpoint").await;
        coordinator
            .write()
            .await
            .attach_store(crate::InMemoryMetadataStore::default());

        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-list-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();

        coordinator.write().await.save_continuous_snapshot(
            "cs-list-job",
            crate::ContinuousSnapshot {
                snapshot_bytes: b"checkpoint".to_vec(),
                watermark_ms: 12_345,
            },
        );
        for _ in 0..50 {
            if coordinator
                .read()
                .await
                .load_continuous_snapshot("cs-list-job")
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let list = api_continuous_list(State(coordinator.clone()))
            .await
            .unwrap();
        assert_eq!(list.0.streams.len(), 1);
        assert_eq!(list.0.streams[0].job_id, "cs-list-job");
        assert!(list.0.streams[0].snapshot_available);
        assert_eq!(list.0.streams[0].persisted_watermark_ms, Some(12_345));

        let get = api_continuous_get(
            State(coordinator.clone()),
            Path(String::from("cs-list-job")),
        )
        .await
        .unwrap();
        assert_eq!(get.0.job_id, "cs-list-job");
        assert_eq!(get.0.spec, tumbling_spec());

        let checkpoint =
            api_continuous_checkpoint(State(coordinator), Path(String::from("cs-list-job")))
                .await
                .unwrap();
        assert_eq!(checkpoint.0.job_id, "cs-list-job");
        assert_eq!(checkpoint.0.watermark_ms, Some(12_345));
        assert_eq!(
            checkpoint.0.snapshot_b64,
            Some(base64::engine::general_purpose::STANDARD.encode("checkpoint"))
        );
    }

    /// #92: the view's delivery block is derived from the sink contract and
    /// connector capability metadata — never hardcoded platform-side.
    #[tokio::test]
    async fn delivery_view_reflects_sink_capability_metadata() {
        let coordinator = make_coordinator_with_executor("delivery").await;

        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-delivery-drain".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-delivery-iceberg".into(),
                spec: tumbling_spec(),
                sink: Some(ContinuousSinkSpec {
                    root: "/tmp/warehouse".into(),
                    table: "cycles".into(),
                    mode: "append".into(),
                    key_columns: Vec::new(),
                    op_column: None,
                }),
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();

        let drain_only = api_continuous_get(
            State(coordinator.clone()),
            Path(String::from("cs-delivery-drain")),
        )
        .await
        .unwrap();
        assert_eq!(drain_only.0.delivery.sink, None);
        assert_eq!(drain_only.0.delivery.effective, "at-least-once");
        assert!(!drain_only.0.delivery.source_offsets_in_sink_transaction);

        let with_sink = api_continuous_get(
            State(coordinator),
            Path(String::from("cs-delivery-iceberg")),
        )
        .await
        .unwrap();
        assert_eq!(with_sink.0.delivery.sink.as_deref(), Some("iceberg"));
        assert_eq!(
            with_sink.0.delivery.sink_guarantee.as_deref(),
            Some("exactly-once")
        );
        assert_eq!(with_sink.0.delivery.effective, "exactly-once");
        assert!(with_sink.0.delivery.source_offsets_in_sink_transaction);
    }

    #[tokio::test]
    async fn coordinator_prepares_one_fenced_executor_cycle() {
        let coordinator = make_coordinator_with_executor("push").await;

        // Register the job first.
        let register_req = ContinuousRegisterRequest {
            job_id: "cs-push-job".to_string(),
            spec: tumbling_spec(),
            sink: None,
            parallelism: None,
            mode: None,
            sources: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        };
        let _ = api_continuous_register(State(coordinator.clone()), Json(register_req))
            .await
            .unwrap();

        let assignment = prepare_cycle(&coordinator, "cs-push-job").await;
        assert!(assignment.requires_reattach());

        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cs-push-job").unwrap();
        assert!(coord.continuous_input_cycles.contains(&job_id));
        assert_eq!(coord.job_input_partitions[&job_id].len(), 1);
    }

    #[tokio::test]
    async fn restore_stages_snapshot_for_next_continuous_cycle() {
        use base64::Engine as _;

        let coordinator = make_coordinator_with_executor("restore").await;
        coordinator
            .write()
            .await
            .attach_store(crate::InMemoryMetadataStore::default());
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-restore-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();

        coordinator.write().await.save_continuous_snapshot(
            "cs-restore-job",
            crate::ContinuousSnapshot {
                snapshot_bytes: b"old-checkpoint".to_vec(),
                watermark_ms: 777,
            },
        );
        for _ in 0..50 {
            if coordinator
                .read()
                .await
                .load_continuous_snapshot("cs-restore-job")
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let restore = api_continuous_restore(
            State(coordinator.clone()),
            Path(String::from("cs-restore-job")),
            Json(ContinuousRestoreRequest {
                snapshot_b64: base64::engine::general_purpose::STANDARD.encode("new-checkpoint"),
            }),
        )
        .await
        .unwrap();
        assert!(restore.0.restored);
        assert_eq!(restore.0.watermark_ms, 777);

        let assignment = prepare_cycle(&coordinator, "cs-restore-job").await;
        assert_eq!(assignment.input_partitions().len(), 2);
        match assignment.input_partitions()[0].descriptor() {
            Some(InputPartitionDescriptor::ContinuousRestore {
                snapshot_bytes,
                watermark_ms,
            }) => {
                assert_eq!(snapshot_bytes.as_slice(), b"new-checkpoint");
                assert_eq!(*watermark_ms, 777);
            }
            other => panic!("expected restore descriptor, got {other:?}"),
        }

        let job_id = krishiv_proto::JobId::try_new("cs-restore-job").unwrap();
        {
            let coord = coordinator.read().await;
            assert!(coord.pending_continuous_restores.contains_key(&job_id));
        }
        {
            let mut coord = coordinator.write().await;
            let accepted = coord.apply_assignment_dispatch_responses(
                &job_id,
                &[(
                    assignment,
                    TaskStatusResponse::new(TransportDisposition::Accepted),
                )],
            );
            assert_eq!(accepted, 1);
            assert!(!coord.pending_continuous_restores.contains_key(&job_id));
        }
    }

    #[tokio::test]
    async fn push_rejects_undeliverable_in_process_target_and_rolls_back() {
        let coordinator = make_coordinator_with_executor("in-process-push").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-in-process-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();

        let error = api_continuous_push(
            State(coordinator.clone()),
            Json(ContinuousPushRequest {
                job_id: "cs-in-process-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("HTTP push must not pretend an in-process target was delivered");
        assert_eq!(error, StatusCode::SERVICE_UNAVAILABLE);

        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cs-in-process-job").unwrap();
        assert!(!coord.continuous_input_cycles.contains(&job_id));
        assert!(!coord.job_input_partitions.contains_key(&job_id));
    }

    #[tokio::test]
    async fn register_with_invalid_job_id_returns_bad_request() {
        let coordinator = make_coordinator_with_executor("invalid").await;

        let req = ContinuousRegisterRequest {
            job_id: "".to_string(), // empty id is invalid
            spec: tumbling_spec(),
            sink: None,
            parallelism: None,
            mode: None,
            sources: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        };
        let result = api_continuous_register(State(coordinator.clone()), Json(req)).await;
        assert!(result.is_err(), "empty job_id must be rejected");
    }

    #[tokio::test]
    async fn register_rejects_invalid_window_spec_before_job_creation() {
        let coordinator = make_coordinator_with_executor("invalid-window").await;
        let mut spec = tumbling_spec();
        spec.window_size_ms = 0;

        let error = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-invalid-window".into(),
                spec,
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .expect_err("invalid window spec must fail registration");

        assert_eq!(error, StatusCode::BAD_REQUEST);
        let job_id = krishiv_proto::JobId::try_new("cs-invalid-window").unwrap();
        assert!(matches!(
            coordinator.read().await.job_snapshot(&job_id),
            Err(SchedulerError::UnknownJob { .. })
        ));
    }

    /// A continuous stream is a declarative desired-state object: re-registering
    /// the SAME id with the SAME spec is an idempotent no-op (success), not a
    /// conflict. This is what a steady-state pipeline reconcile does, and it must
    /// NOT tear the running job down (which would reset window state).
    #[tokio::test]
    async fn reregister_same_spec_is_idempotent() {
        let coordinator = make_coordinator_with_executor("idempotent").await;
        let request = || ContinuousRegisterRequest {
            job_id: "cs-idempotent-job".to_string(),
            spec: tumbling_spec(),
            sink: None,
            parallelism: None,
            mode: None,
            sources: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        };
        let first = api_continuous_register(State(coordinator.clone()), Json(request()))
            .await
            .expect("first register succeeds");
        assert!(first.0.success);
        let second = api_continuous_register(State(coordinator.clone()), Json(request()))
            .await
            .expect("re-register with same spec is idempotent, not a conflict");
        assert!(second.0.success);

        // Exactly one streaming job with this id remains registered.
        let coord = coordinator.read().await;
        let streaming = coord
            .job_snapshots()
            .into_iter()
            .filter(|job| {
                job.kind() == JobKind::Streaming && job.job_id().as_str() == "cs-idempotent-job"
            })
            .count();
        assert_eq!(streaming, 1, "re-register must not create a duplicate job");
    }

    /// Re-registering the same id with a CHANGED spec converges: the old job is
    /// torn down and a fresh one created carrying the new window spec.
    #[tokio::test]
    async fn reregister_with_changed_spec_replaces_job() {
        let coordinator = make_coordinator_with_executor("replace").await;
        let first = ContinuousRegisterRequest {
            job_id: "cs-replace-job".to_string(),
            spec: tumbling_spec(),
            sink: None,
            parallelism: None,
            mode: None,
            sources: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        };
        let _ = api_continuous_register(State(coordinator.clone()), Json(first))
            .await
            .expect("first register succeeds");

        let mut changed = tumbling_spec();
        changed.window_size_ms = 30_000; // different desired spec
        let second = ContinuousRegisterRequest {
            job_id: "cs-replace-job".to_string(),
            spec: changed.clone(),
            sink: None,
            parallelism: None,
            mode: None,
            sources: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        };
        let resp = api_continuous_register(State(coordinator.clone()), Json(second))
            .await
            .expect("changed-spec re-register converges");
        assert!(resp.0.success);

        // The registered job now carries the NEW spec, and there is still exactly one.
        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cs-replace-job").unwrap();
        let view = continuous_job_view(&coord, &job_id).expect("job present and renderable");
        assert_eq!(
            view.spec, changed,
            "replaced job must carry the new window spec"
        );
    }

    /// A non-streaming job holding the same id is a genuine collision -> 409.
    #[tokio::test]
    async fn register_over_non_streaming_id_conflicts() {
        use krishiv_proto::{JobSpec, StageSpec, TaskId, TaskSpec};
        let coordinator = make_coordinator_with_executor("collision").await;
        // Submit a plain batch job under the target id.
        {
            let mut coord = coordinator.write().await;
            let stage =
                StageSpec::new(krishiv_proto::StageId::try_new("s1").unwrap(), "batch").with_task(
                    TaskSpec::new(TaskId::try_new("t1").unwrap(), "batch-task-body"),
                );
            let spec = JobSpec::new(
                krishiv_proto::JobId::try_new("cs-collision-id").unwrap(),
                "batch-job",
                JobKind::Batch,
            )
            .with_stage(stage);
            coord.submit_job(spec).expect("batch submit");
        }
        let error = api_continuous_register(
            State(coordinator),
            Json(ContinuousRegisterRequest {
                job_id: "cs-collision-id".to_string(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .expect_err("continuous register over a batch id must conflict");
        assert_eq!(error, StatusCode::CONFLICT);
    }

    /// Deregistering a registered-but-never-pushed streaming job must free the
    /// id. Its task is at attempt 0 (no cycle ever ran); push_cancel_job used to
    /// `?`-fail on `AttemptId::try_new(0)` → 409 → the job could never be torn
    /// down (a teardown-leg limbo). Regression guard for that fix.
    #[tokio::test]
    async fn deregister_never_pushed_streaming_job_frees_id() {
        let coordinator = make_coordinator_with_executor("dereg-fresh").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-dereg-fresh".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();
        // Deregister immediately, before any push (task attempt is still 0).
        let resp = api_continuous_deregister(
            State(coordinator.clone()),
            Path("cs-dereg-fresh".to_string()),
        )
        .await
        .expect("deregister of a never-pushed streaming job must succeed, not 409");
        assert!(resp.0.cancelled);
        // The id is freed from the registry, so it can be reused.
        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cs-dereg-fresh").unwrap();
        assert!(
            coord.job_coordinator(&job_id).is_none(),
            "deregister must free the id from the registry"
        );
    }

    #[tokio::test]
    async fn push_and_drain_unknown_job_return_not_found() {
        let coordinator = make_coordinator_with_executor("unknown").await;
        let push = api_continuous_push(
            State(coordinator.clone()),
            Json(ContinuousPushRequest {
                job_id: "missing-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("unknown push must fail");
        assert_eq!(push, StatusCode::NOT_FOUND);

        let drain = api_continuous_drain(
            State(coordinator),
            Json(ContinuousDrainRequest {
                job_id: "missing-job".into(),
            }),
        )
        .await
        .expect_err("unknown drain must fail");
        assert_eq!(drain, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn concurrent_push_is_rejected_while_cycle_is_in_flight() {
        let coordinator = make_coordinator_with_executor("busy").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-busy-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();
        let _ = prepare_cycle(&coordinator, "cs-busy-job").await;
        let error = api_continuous_push(
            State(coordinator),
            Json(ContinuousPushRequest {
                job_id: "cs-busy-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("second concurrent cycle must be fenced");
        assert_eq!(error, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn successful_cycle_publishes_output_and_returns_task_to_idle() {
        use base64::Engine as _;
        use krishiv_proto::{TaskOutputMetadata, TaskState, TaskStatusUpdate};

        let coordinator = make_coordinator_with_executor("cycle").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-cycle-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();
        let assignment = prepare_cycle(&coordinator, "cs-cycle-job").await;

        let job_id = krishiv_proto::JobId::try_new("cs-cycle-job").unwrap();
        let running = TaskStatusUpdate::new(
            job_id.clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.executor_id().clone(),
            TaskState::Running,
            assignment.attempt_id().as_u32(),
        )
        .with_lease_generation(assignment.lease_generation());
        coordinator
            .write()
            .await
            .apply_task_update(running)
            .unwrap();

        let output_ipc = base64::engine::general_purpose::STANDARD
            .decode(encoded_input())
            .unwrap();
        let succeeded = TaskStatusUpdate::new(
            job_id.clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.executor_id().clone(),
            TaskState::Succeeded,
            assignment.attempt_id().as_u32(),
        )
        .with_lease_generation(assignment.lease_generation())
        .with_output_metadata(
            TaskOutputMetadata::new("streaming_window", 1, 1, 2)
                .with_inline_record_batch_ipc(vec![output_ipc.clone()]),
        );
        coordinator
            .write()
            .await
            .apply_task_update(succeeded.clone())
            .unwrap();
        assert_eq!(
            coordinator
                .write()
                .await
                .apply_task_update(succeeded)
                .unwrap(),
            crate::TaskUpdateOutcome::Duplicate
        );

        let blocked_push = api_continuous_push(
            State(coordinator.clone()),
            Json(ContinuousPushRequest {
                job_id: "cs-cycle-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("undrained output must backpressure the next cycle");
        assert_eq!(blocked_push, StatusCode::CONFLICT);

        let mut coord = coordinator.write().await;
        let detail = coord.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.job().state(), krishiv_proto::JobState::Running);
        assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Succeeded);
        assert!(!coord.continuous_input_cycles.contains(&job_id));
        assert!(!coord.job_input_partitions.contains_key(&job_id));
        assert_eq!(
            coord.take_job_inline_results(&job_id),
            Some(vec![output_ipc])
        );
    }

    /// G5 follow-up (found live via the Phase-20 executor fault loop): if the
    /// executor holding a continuous job's task is lost *mid-cycle* — the
    /// task never reports a terminal status, so `apply_task_update`'s
    /// Succeeded/Failed/Cancelled cleanup of `continuous_input_cycles` never
    /// runs — `advance_heartbeat_tick` must release the fence itself, or
    /// every future push 409s forever. Advances the deterministic heartbeat
    /// clock past the default timeout (`CoordinatorConfig::default()` = 3
    /// ticks) without ever re-heartbeating the sole executor, so it is
    /// evicted while the cycle it was assigned is still open.
    #[tokio::test]
    async fn heartbeat_tick_releases_input_cycle_fence_after_executor_lost_mid_cycle() {
        // Pin the timeout to 3 ticks so the fixed tick budget below is
        // deterministic and independent of the production default.
        let coordinator = make_coordinator_with_executor_hb("lost-mid-cycle", Some(3)).await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-lost-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();
        // Assigns the task and inserts the job into `continuous_input_cycles`
        // (task_assignment.rs::prepare_continuous_input_cycle) — a cycle is
        // now "in flight" exactly as it is between a live push and its
        // eventual Succeeded/Failed/Cancelled status update.
        let _assignment = prepare_cycle(&coordinator, "cs-lost-job").await;

        let job_id = krishiv_proto::JobId::try_new("cs-lost-job").unwrap();
        assert!(
            coordinator
                .read()
                .await
                .continuous_input_cycles
                .contains(&job_id),
            "prepare_cycle must mark the cycle in flight"
        );

        // Never heartbeat the executor again; advance past the default
        // timeout so the next tick evicts it as lost.
        for _ in 0..5 {
            coordinator.advance_heartbeat_tick().await.unwrap();
        }

        let coord = coordinator.read().await;
        assert!(
            !coord.continuous_input_cycles.contains(&job_id),
            "the input-cycle fence must be released when the executor \
             holding the task is lost mid-cycle, or every future push 409s"
        );
        assert!(!coord.job_input_partitions.contains_key(&job_id));
    }

    /// Real-world root cause (found live via the Phase-20 executor fault
    /// loop, distinct from the fence bug above): placement onto a healthy
    /// executor (`assign_pending_tasks_for_schedulable_jobs`) is otherwise
    /// only ever triggered by a NEW executor *registering*. A completed
    /// cycle's task keeps its `assigned_executor` set (by design — sticky
    /// placement across cycles) until the heartbeat clock evicts that
    /// executor and resets the task to `Pending`. If a replacement executor
    /// already registered *before* that eviction tick fires — the ordinary
    /// case, since eviction takes `heartbeat_timeout_ticks` ticks while a
    /// k8s replacement pod registers within seconds — that registration
    /// event is already in the past, and nothing else ever re-attempts
    /// placement: the task sits `Pending`/unassigned forever, and
    /// `prepare_continuous_input_cycle` permanently rejects every future
    /// push ("not idle and ready for input"). Fixed by extending
    /// `reset_running_tasks_for_lost_executor`'s state match to include a
    /// continuous task's idle `Succeeded` state, so the existing per-job
    /// reassignment sweep picks it up immediately.
    #[tokio::test]
    async fn heartbeat_tick_reassigns_task_to_already_registered_executor_after_loss() {
        use krishiv_proto::{ExecutorDescriptor, ExecutorId, TaskState};

        // Pin the timeout to 3 ticks: the relative-timing math below (original
        // evicted while the replacement survives) assumes a 3-tick window.
        let coordinator = make_coordinator_with_executor_hb("reassign", Some(3)).await;

        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-reassign-job".into(),
                spec: tumbling_spec(),
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .unwrap();
        let job_id = krishiv_proto::JobId::try_new("cs-reassign-job").unwrap();

        // Run one cycle to completion (Succeeded) on the fixture's sole
        // executor — the task's `assigned_executor` stays set to it
        // afterward (sticky placement), matching real behavior.
        let assignment = prepare_cycle(&coordinator, "cs-reassign-job").await;
        let original_executor = assignment.executor_id().clone();
        let succeeded = krishiv_proto::TaskStatusUpdate::new(
            job_id.clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.executor_id().clone(),
            TaskState::Succeeded,
            assignment.attempt_id().as_u32(),
        )
        .with_lease_generation(assignment.lease_generation());
        coordinator
            .write()
            .await
            .apply_task_update(succeeded)
            .unwrap();

        // Advance 2 ticks (default `heartbeat_timeout_ticks` is 3, so
        // `original_executor` is not yet stale), *then* register the
        // replacement — giving it a fresher heartbeat baseline, exactly like
        // a k8s replacement pod registering only after the old one has
        // already gone quiet for a while. One more tick pushes
        // `original_executor` past the threshold (3 - 0 >= 3) while
        // `replacement_id` stays comfortably under it (3 - 2 < 3).
        coordinator.advance_heartbeat_tick().await.unwrap();
        coordinator.advance_heartbeat_tick().await.unwrap();

        let replacement_id = ExecutorId::try_new("exec-cs-reassign-replacement").unwrap();
        let replacement_desc = ExecutorDescriptor::new(replacement_id.clone(), "localhost", 4)
            .with_task_endpoint(crate::IN_PROCESS_TASK_ENDPOINT);
        coordinator
            .write()
            .await
            .register_executor(replacement_desc)
            .unwrap();

        let evicted = coordinator.advance_heartbeat_tick().await.unwrap();
        assert!(
            evicted.contains(&original_executor),
            "this tick must be the one that evicts the original executor"
        );

        let coord = coordinator.read().await;
        let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
        let task = record
            .stages()
            .iter()
            .flat_map(|s| s.tasks())
            .find(|t| t.task_id() == assignment.task_id())
            .unwrap();
        assert_ne!(
            task.assigned_executor(),
            Some(&original_executor),
            "the lost executor must not still be the assignment"
        );
        assert_eq!(
            task.assigned_executor(),
            Some(&replacement_id),
            "the task must be reassigned to the already-registered healthy \
             executor immediately on eviction, not left unassigned forever \
             waiting for a registration event that already happened"
        );
    }
}
