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

/// Bound for the run-loop data-plane push/drain RPCs (Phase 58 #180).
///
/// Neither `push_run_loop_input` nor `drain_run_loop_output` holds any
/// coordinator lock during the RPC (both clone `executor_channels` and drop
/// the read guard immediately), so a stuck call here does not wedge the
/// coordinator the way the unbounded `push_cancel_job` dispatch did. But
/// without an explicit deadline these single-attempt, latency-sensitive
/// calls fall back to the channel's implicit ~35s keepalive detection
/// (`get_or_connect_channel_on_map`'s `keep_alive_timeout` +
/// `http2_keep_alive_interval`) to notice a dead/partitioned executor —
/// a much slower and less precise failure signal than an explicit bound.
const RUN_LOOP_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Map a scheduler error to its HTTP status **and carry its message**.
///
/// `scheduler_status` alone throws the message away, so every failure on this
/// surface reached the caller as a bare status code with an empty body. The
/// cycle model's "push while a cycle is undrained" rejection is the sharpest
/// example: the caller sees `409` and nothing else, with no hint that a drain
/// is what unwedges it. That is a correct contract reported opaquely — it cost
/// a full bisection to identify while building the Phase 62 soak.
///
/// Axum renders `(StatusCode, String)` as a plain-text body, so handlers that
/// return this keep their status codes and gain an explanation.
pub(crate) fn scheduler_error_response(error: &SchedulerError) -> (StatusCode, String) {
    (scheduler_status(error), error.to_string())
}

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
    /// Legacy windowed spec. Exactly ONE of `spec` / `stream_spec` must be
    /// present; both or neither is refused rather than guessed at.
    #[serde(default)]
    pub spec: Option<WindowExecutionSpec>,
    /// Task #147: class-tagged spec for the non-window streaming classes
    /// (and, optionally, windows). New clients send this; old coordinators
    /// that do not know the field fail the request on the missing `spec`,
    /// which is fail-closed rather than silently window-planning a join.
    #[serde(default)]
    pub stream_spec: Option<krishiv_plan::stream_task::StreamingTaskSpec>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Resolve a wire `mode` string + `parallelism` into the model that will
    /// actually run.
    ///
    /// Public because remote clients verify the coordinator's echo against what
    /// they asked for, and that comparison must use *this* function rather than
    /// a second copy of the alias list — two parsers that disagree would make
    /// the verification itself the thing that lies.
    pub fn parse(mode: Option<&str>, parallelism: u32) -> Result<Self, String> {
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

/// Streaming sink target for a continuous job.
///
/// Two shapes, distinguished by whether `connector` is set:
/// - **Iceberg (G7, default)** — `root`/`table`/`mode`/`key_columns`/`op_column`
///   build the checkpoint-aligned two-phase-commit `iceberg-sink:` contract.
/// - **Any registered connector (#197)** — `connector` names a sink kind and
///   `options` carries its driver properties, building a `registry-sink:`
///   contract the executor opens through the connector registry. Delivery is
///   at-least-once (flushed per cycle, replayed cycles re-deliver), and the
///   driver must declare `resumable_flush` or the executor rejects it at open.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousSinkSpec {
    /// Registered connector kind (`csv`, `elasticsearch`, `jdbc-sink`, …).
    /// When set, this is a registry-dispatched sink and the Iceberg fields are
    /// ignored.
    #[serde(default)]
    pub connector: Option<String>,
    /// Driver properties for `connector` (`path`, `url`, `index`, …).
    #[serde(default)]
    pub options: std::collections::BTreeMap<String, String>,
    /// Local table root directory on the executor host (Iceberg sinks).
    #[serde(default)]
    pub root: String,
    /// Iceberg table name inside the root.
    #[serde(default)]
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
    /// G7: when set, the sink commits through the platform's governed REST
    /// catalog of this name (the catalog the query path resolves) instead of a
    /// local table root. `namespace` then holds the governed table's namespace
    /// (the pipeline schema) and `root` the warehouse it lives under. The
    /// executor connects to the catalog from its `KRISHIV_ICEBERG_REST_*`
    /// environment; `None` keeps the legacy local-root filesystem sink.
    #[serde(default)]
    pub catalog: Option<String>,
    /// Governed-table namespace (pipeline schema) — required when `catalog` is
    /// set, ignored otherwise.
    #[serde(default)]
    pub namespace: Option<String>,
}

fn default_sink_mode() -> String {
    String::from("append")
}

impl ContinuousSinkSpec {
    /// Build the validated string sink contract carried on the task spec —
    /// `registry-sink:<kind>|<base64-json>` when `connector` is set, otherwise
    /// `iceberg-sink:<root>|<table>|mode=...`.
    fn contract_string(&self) -> crate::SchedulerResult<String> {
        if let Some(kind) = self
            .connector
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty())
        {
            return self.registry_contract_string(kind);
        }
        let governed = self
            .catalog
            .as_deref()
            .map(str::trim)
            .is_some_and(|c| !c.is_empty());
        if self.table.trim().is_empty() {
            return Err(SchedulerError::InvalidJob {
                message: String::from(
                    "continuous sink requires either `connector` (registry sink) or \
                     `table` (Iceberg sink)",
                ),
            });
        }
        // A local-root Iceberg sink needs a `root`; a governed (catalog) sink
        // has none — the catalog assigns the table location.
        if !governed && self.root.trim().is_empty() {
            return Err(SchedulerError::InvalidJob {
                message: String::from(
                    "continuous Iceberg sink requires `root` (or set `catalog` for a governed sink)",
                ),
            });
        }
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
        if let Some(catalog) = self
            .catalog
            .as_deref()
            .map(str::trim)
            .filter(|c| !c.is_empty())
        {
            contract.push_str(&format!("|catalog={catalog}"));
        }
        if let Some(namespace) = self
            .namespace
            .as_deref()
            .map(str::trim)
            .filter(|n| !n.is_empty())
        {
            contract.push_str(&format!("|namespace={namespace}"));
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

    /// Encode a registry-dispatched sink as `registry-sink:<kind>|<base64-json>`
    /// (#197). Base64 so property values containing `|`/`:` cannot corrupt the
    /// contract framing — the same encoding the batch export uses.
    fn registry_contract_string(&self, kind: &str) -> crate::SchedulerResult<String> {
        use base64::Engine as _;

        let properties: serde_json::Map<String, serde_json::Value> = self
            .options
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect();
        let payload = serde_json::to_vec(&serde_json::json!({
            "name": format!("continuous-{kind}"),
            "properties": properties,
        }))
        .map_err(|error| SchedulerError::InvalidJob {
            message: format!("registry sink options are not encodable: {error}"),
        })?;
        Ok(format!(
            "registry-sink:{kind}|{}",
            base64::engine::general_purpose::STANDARD.encode(payload)
        ))
    }
}

#[derive(Debug, Serialize)]
pub struct ContinuousRegisterResponse {
    /// Echo of the registered class (task #147): a client that sent a
    /// non-window class and gets no echo is talking to a coordinator that
    /// silently dropped the field — verify_ack turns that into a hard error.
    #[serde(default)]
    pub class: String,
    pub success: bool,
    /// The execution model actually registered (`"cycle-push"` / `"run-loop"`).
    ///
    /// A bare `success: true` cannot tell a caller whether the coordinator
    /// honoured `mode`/`parallelism` or fell back to the single-subtask cycle
    /// model — an older coordinator ignores unknown request fields and answers
    /// success either way. These three fields are the applied shape, so a
    /// client can compare what it asked for against what runs.
    pub mode: String,
    /// The subtask count actually registered.
    pub parallelism: u32,
    /// Whether barrier checkpointing was armed.
    pub checkpointing: bool,
    /// How many registry connector sources the subtasks took ownership of. A
    /// coordinator that drops the `sources` field registers a run-loop job
    /// whose subtasks read nothing at all — which looks identical, from the
    /// outside, to a healthy job that simply has not seen input yet.
    pub sources: usize,
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
    /// Streaming class of the job (task #147): window / join / pipeline /
    /// stateless.
    pub class: String,
    /// The window spec — present for window-class jobs only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<WindowExecutionSpec>,
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
    /// The window spec — present for window-class jobs only (task #147).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<WindowExecutionSpec>,
    /// Execution model this job runs (`"cycle"` or `"run-loop"`).
    ///
    /// This endpoint reads the **cycle** model's coordinator-side snapshot
    /// store. A run-loop job checkpoints through the barrier pipeline into its
    /// `checkpoint_storage_path` instead, so it returns
    /// `snapshot_available: false` here no matter how many barrier checkpoints
    /// have committed. Without this field a caller cannot tell "no checkpoint
    /// has been taken yet" from "this endpoint does not serve your job's
    /// execution model" — the two look identical, and the second is not a
    /// state that will ever change by waiting.
    pub model: String,
    /// Set for run-loop jobs: what the caller should use instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_source: Option<String>,
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

fn decode_continuous_job_task(
    record: &crate::JobRecord,
) -> crate::SchedulerResult<krishiv_plan::stream_task::StreamingTaskSpec> {
    decode_continuous_job_shape(record).map(|shape| shape.task)
}

/// Decoded identity of a continuous job: its window spec plus the Phase 55
/// execution model and parallelism.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ContinuousJobShape {
    pub task: krishiv_plan::stream_task::StreamingTaskSpec,
    pub mode: ContinuousJobMode,
    pub parallelism: u32,
}

impl ContinuousJobShape {
    /// The window spec, when this is a window job; `None` means "not a
    /// window job", never an error to hide.
    pub(crate) fn window_spec(&self) -> Option<&WindowExecutionSpec> {
        match &self.task {
            krishiv_plan::stream_task::StreamingTaskSpec::Window(w) => Some(w),
            _ => None,
        }
    }
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
            task: krishiv_plan::stream_task::StreamingTaskSpec::Window(Box::new(spec)),
            mode: ContinuousJobMode::Cycle,
            parallelism: 1,
        });
    }
    // Task #147 classed run-loop fragments: `<prefix><job>|<sub>/<par>|<json>`.
    for (prefix, class) in [
        ("stream:rjoin:", "join"),
        ("stream:rpipe:", "pipeline"),
        ("stream:rbatch:", "stateless"),
    ] {
        let classed_prefix = format!("{prefix}{}|", job_id.as_str());
        if let Some(rest) = typed.body.strip_prefix(&classed_prefix) {
            let (subtask_segment, json) = rest.split_once('|').ok_or_else(|| {
                invalid_continuous_job(job_id, format!("{class} fragment missing subtask segment"))
            })?;
            let parallelism = subtask_segment
                .split_once('/')
                .and_then(|(_, p)| p.trim().parse::<u32>().ok())
                .ok_or_else(|| {
                    invalid_continuous_job(
                        job_id,
                        format!("{class} fragment has a malformed subtask segment"),
                    )
                })?;
            let task = match class {
                "join" => krishiv_plan::stream_task::StreamingTaskSpec::Join(
                    serde_json::from_str(json).map_err(|e| {
                        invalid_continuous_job(job_id, format!("join spec decode failed: {e}"))
                    })?,
                ),
                "pipeline" => krishiv_plan::stream_task::StreamingTaskSpec::Pipeline(
                    serde_json::from_str(json).map_err(|e| {
                        invalid_continuous_job(job_id, format!("pipeline spec decode failed: {e}"))
                    })?,
                ),
                _ => krishiv_plan::stream_task::StreamingTaskSpec::Stateless(
                    serde_json::from_str(json).map_err(|e| {
                        invalid_continuous_job(job_id, format!("stateless spec decode failed: {e}"))
                    })?,
                ),
            };
            return Ok(ContinuousJobShape {
                task,
                mode: ContinuousJobMode::RunLoop,
                parallelism,
            });
        }
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
            task: krishiv_plan::stream_task::StreamingTaskSpec::Window(Box::new(spec)),
            mode: ContinuousJobMode::RunLoop,
            parallelism,
        });
    }
    Err(invalid_continuous_job(
        job_id,
        "does not use a stream:loop / stream:rloop / stream:rjoin / stream:rpipe / \
         stream:rbatch fragment",
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
    let task = decode_continuous_job_task(&record)?;
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
        class: task.class_name().to_string(),
        spec: ContinuousJobShape {
            task,
            mode: ContinuousJobMode::Cycle,
            parallelism: 1,
        }
        .window_spec()
        .cloned(),
    })
}

pub async fn api_continuous_register(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterRequest>,
) -> Result<Json<ContinuousRegisterResponse>, (StatusCode, String)> {
    // Encode/spec errors are a client fault -> 400 (unlike the SQL entrypoint,
    // whose caller already compiled a valid spec).
    if let Err(error) = JobId::try_new(&body.job_id) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid job_id '{}': {error}", body.job_id),
        ));
    }
    let task = match (&body.spec, &body.stream_spec) {
        (Some(w), None) => {
            if let Err(error) = krishiv_plan::window::encode_window_execution_spec(w) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("window spec is not encodable: {error}"),
                ));
            }
            krishiv_plan::stream_task::StreamingTaskSpec::Window(Box::new(w.clone()))
        }
        (None, Some(t)) => t.clone(),
        (Some(_), Some(_)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "send exactly one of `spec` (legacy window) or `stream_spec` (classed), \
                 not both — two specs is an ambiguity, not a fallback"
                    .into(),
            ));
        }
        (None, None) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "send exactly one of `spec` or `stream_spec`".into(),
            ));
        }
    };
    let options = ContinuousRegistrationOptions {
        sink: body.sink.clone(),
        parallelism: body.parallelism,
        mode: body.mode.clone(),
        sources: body.sources.clone(),
        checkpoint_interval_ms: body.checkpoint_interval_ms,
        checkpoint_storage_path: body.checkpoint_storage_path.clone(),
    };
    let applied =
        register_continuous_task_with_options(&coordinator, &body.job_id, &task, &options)
            .await
            .map_err(|error| match error {
                ContinuousStreamError::Scheduler(e) => scheduler_error_response(&e),
                other @ ContinuousStreamError::Unavailable(_) => {
                    (StatusCode::SERVICE_UNAVAILABLE, other.to_string())
                }
                other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;
    Ok(Json(ContinuousRegisterResponse {
        class: task.class_name().to_string(),
        success: true,
        mode: applied.mode.as_str().to_string(),
        parallelism: applied.parallelism,
        checkpointing: applied.checkpointing,
        sources: applied.sources,
    }))
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
    /// The execution model actually registered — see
    /// [`ContinuousRegisterResponse::mode`].
    pub mode: String,
    /// The subtask count actually registered.
    pub parallelism: u32,
    /// Whether barrier checkpointing was armed.
    pub checkpointing: bool,
    /// How many registry connector sources the subtasks took ownership of. A
    /// coordinator that drops the `sources` field registers a run-loop job
    /// whose subtasks read nothing at all — which looks identical, from the
    /// outside, to a healthy job that simply has not seen input yet.
    pub sources: usize,
}

/// Register a continuous streaming job from **SQL**: the coordinator compiles
/// the windowed query to a [`WindowExecutionSpec`] itself
/// (`krishiv_sql::streaming_window_plan`), so callers (the platform pipeline
/// reconciler) pass SQL and stay decoupled from the operator spec type.
pub async fn api_continuous_register_sql(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterSqlRequest>,
) -> Result<Json<ContinuousRegisterSqlResponse>, (StatusCode, String)> {
    let plan = krishiv_sql::streaming_window_plan::compile_streaming_window_sql(&body.sql)
        .map_err(|error| {
            tracing::warn!(error = %error, "continuous-register-sql: compile failed");
            (
                StatusCode::BAD_REQUEST,
                format!("streaming window SQL failed to compile: {error}"),
            )
        })?;
    let options = ContinuousRegistrationOptions {
        sink: body.sink.clone(),
        parallelism: body.parallelism,
        mode: body.mode.clone(),
        sources: body.sources.clone(),
        checkpoint_interval_ms: body.checkpoint_interval_ms,
        checkpoint_storage_path: body.checkpoint_storage_path.clone(),
    };
    let applied =
        register_continuous_stream_with_options(&coordinator, &body.job_id, &plan.spec, &options)
            .await
            .map_err(|error| match error {
                ContinuousStreamError::Scheduler(e) => scheduler_error_response(&e),
                other @ ContinuousStreamError::Unavailable(_) => {
                    (StatusCode::SERVICE_UNAVAILABLE, other.to_string())
                }
                other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
            })?;
    Ok(Json(ContinuousRegisterSqlResponse {
        success: true,
        source: plan.source,
        mode: applied.mode.as_str().to_string(),
        parallelism: applied.parallelism,
        checkpointing: applied.checkpointing,
        sources: applied.sources,
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
) -> Result<Json<ContinuousJobView>, (StatusCode, String)> {
    let job_id = JobId::try_new(&job_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid job id: {error}")))?;
    let view = {
        let coord = coordinator.read().await;
        continuous_job_view(&coord, &job_id).map_err(|error| scheduler_error_response(&error))?
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
) -> Result<Json<ContinuousDeregisterResponse>, (StatusCode, String)> {
    let job_id = JobId::try_new(&job_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid job id: {error}")))?;
    let mut coord = coordinator.write().await;
    // Confirm it exists and is a streaming job (404 if unknown, 409 otherwise).
    continuous_job_view(&coord, &job_id).map_err(|error| scheduler_error_response(&error))?;
    // push_cancel_job (not plain cancel_job): the assigned executor must hear
    // about the teardown so it retires the job identity — drops the stateful
    // `stream:loop` executor and the inbox dedupe entries. Without the RPC, a
    // recreated job reusing the same deterministic ids has its first cycle
    // silently swallowed as an at-least-once duplicate.
    coord
        .push_cancel_job(&job_id)
        .await
        .map_err(|error| scheduler_error_response(&error))?;
    // Cancel is terminal → evict removes it from `job_coordinators`, freeing the id.
    coord.evict_completed_job(&job_id);
    // A job id can be reused (a fresh `continuous-register-sql` with the same
    // id after deregister is a normal, supported pattern). Clear the retired
    // job's persisted checkpoint so the next job with this id starts clean
    // instead of silently inheriting a stale watermark/state.
    coord.remove_continuous_snapshot(job_id.as_str());
    Ok(Json(ContinuousDeregisterResponse { cancelled: true }))
}

/// POST /api/v1/continuous-flush — relay a bounded producer's end-of-stream
/// declaration. Cycle jobs run one final `stream-eos:` cycle and return its
/// inline payloads; run-loop jobs flush every subtask's open window state
/// into their egress buffers (drain to collect it) and return none.
pub async fn api_continuous_flush(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousFlushRequest>,
) -> Result<Json<ContinuousFlushResponse>, (StatusCode, String)> {
    let payloads = flush_continuous_stream_coordinated(&coordinator, &body.job_id)
        .await
        .map_err(|error| match error {
            ContinuousStreamError::Scheduler(e) => scheduler_error_response(&e),
            other => (StatusCode::SERVICE_UNAVAILABLE, other.to_string()),
        })?;
    use base64::Engine as _;
    Ok(Json(ContinuousFlushResponse {
        success: true,
        inline_record_batch_ipc_b64: payloads
            .into_iter()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes))
            .collect(),
    }))
}

#[derive(serde::Deserialize)]
pub struct ContinuousFlushRequest {
    pub job_id: String,
}

#[derive(serde::Serialize)]
pub struct ContinuousFlushResponse {
    pub success: bool,
    /// Cycle-mode flush payloads (base64 Arrow IPC); empty for run-loop jobs,
    /// whose flushed output is collected by the next drain.
    pub inline_record_batch_ipc_b64: Vec<String>,
}

pub async fn api_continuous_checkpoint(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousCheckpointResponse>, (StatusCode, String)> {
    use base64::Engine as _;

    let job_id = JobId::try_new(&job_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid job id: {error}")))?;
    let response = {
        let coord = coordinator.read().await;
        let view = continuous_job_view(&coord, &job_id)
            .map_err(|error| scheduler_error_response(&error))?;
        let persisted = coord.load_continuous_snapshot(job_id.as_str());
        // Name the execution model so a run-loop caller is not left staring at
        // `snapshot_available: false` wondering whether to keep polling. This
        // endpoint reads the cycle store; run-loop snapshots live in the job's
        // checkpoint storage, written by the barrier pipeline.
        let is_run_loop = run_loop_targets(&coord, &job_id).ok().flatten().is_some();
        ContinuousCheckpointResponse {
            job_id: view.job_id,
            snapshot_b64: persisted.as_ref().map(|snapshot| {
                base64::engine::general_purpose::STANDARD.encode(&snapshot.snapshot_bytes)
            }),
            watermark_ms: persisted.as_ref().map(|snapshot| snapshot.watermark_ms),
            snapshot_available: persisted.is_some(),
            spec: view.spec,
            model: if is_run_loop { "run-loop" } else { "cycle" }.to_owned(),
            snapshot_source: is_run_loop.then(|| {
                String::from(
                    "run-loop jobs checkpoint through the barrier pipeline into                      checkpoint_storage_path; this endpoint reads the cycle model's                      coordinator snapshot store and will always report                      snapshot_available=false for them",
                )
            }),
        }
    };
    Ok(Json(response))
}

pub async fn api_continuous_restore(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<ContinuousRestoreRequest>,
) -> Result<Json<ContinuousRestoreResponse>, (StatusCode, String)> {
    use base64::Engine as _;

    let job_id = JobId::try_new(&job_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid job id: {error}")))?;
    let snapshot_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.snapshot_b64.as_bytes())
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("snapshot_b64 is not valid base64: {error}"),
            )
        })?;
    if snapshot_bytes.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            String::from("restore snapshot must not be empty"),
        ));
    }
    let watermark_ms = {
        let mut coord = coordinator.write().await;
        let view = continuous_job_view(&coord, &job_id)
            .map_err(|error| scheduler_error_response(&error))?;
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
    /// Task #147: `"L"` / `"R"` targets one side of a two-source run-loop
    /// job. Absent for window/stateless jobs.
    #[serde(default)]
    pub side: Option<String>,
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
    tokio::time::timeout(
        RUN_LOOP_RPC_TIMEOUT,
        client.push_continuous_input(wire::push_continuous_input_request_to_wire(request)),
    )
    .await
    .map_err(|_| {
        ContinuousStreamError::Unavailable(format!(
            "run-loop push to {endpoint} timed out after {}s",
            RUN_LOOP_RPC_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|status| {
        if status.code() == tonic::Code::ResourceExhausted {
            ContinuousStreamError::Backpressure(format!(
                "run-loop push to {endpoint} rejected: {}",
                status.message()
            ))
        } else {
            ContinuousStreamError::Unavailable(format!(
                "run-loop push to {endpoint} failed: {status}"
            ))
        }
    })?;
    Ok(())
}

/// Relay the end-of-stream directive to one executor hosting run-loop
/// subtasks of `job_id`: a `push_continuous_input` with the reserved
/// `stream-eos` task id and an empty payload. The executor flushes every
/// local operator of the job into its egress buffer (see the executor's
/// `RUN_LOOP_EOS_TASK_ID` handler).
async fn push_run_loop_eos(
    channels: &crate::coordinator::task_assignment::ExecutorChannelMap,
    job_id: &krishiv_proto::JobId,
    endpoint: &str,
) -> Result<(), ContinuousStreamError> {
    use krishiv_proto::{TaskId, TransportVersion, wire};
    if crate::is_in_process_task_endpoint(endpoint) {
        return Err(ContinuousStreamError::Unavailable(String::from(
            "run-loop EOS flush cannot reach an in-process-only executor over gRPC",
        )));
    }
    let channel = Coordinator::get_or_connect_channel_on_map(channels, endpoint)
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
        task_id: TaskId::try_new("stream-eos").map_err(|e| invalid_registration(e.to_string()))?,
        ipc_bytes: Vec::new(),
    };
    tokio::time::timeout(
        RUN_LOOP_RPC_TIMEOUT,
        client.push_continuous_input(wire::push_continuous_input_request_to_wire(request)),
    )
    .await
    .map_err(|_| {
        ContinuousStreamError::Unavailable(format!(
            "run-loop EOS flush to {endpoint} timed out after {}s",
            RUN_LOOP_RPC_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|status| {
        ContinuousStreamError::Unavailable(format!(
            "run-loop EOS flush to {endpoint} failed: {status}"
        ))
    })?;
    Ok(())
}

/// Drain a run-loop job's egress: fan `drain_continuous_output` out to each
/// distinct executor hosting a subtask and concatenate the IPC payloads.
///
/// **Partial success is success.** `drain_continuous_output` CLEARS the
/// executor's egress buffer as it reads it, so a payload collected from
/// executor N no longer exists anywhere else. Failing the whole call because
/// executor N+1 timed out therefore does not retry N -- it destroys N's output.
/// This used to `?` on each RPC and drop everything already collected.
///
/// So: return what was collected and log the endpoints that failed; the caller
/// drains again and picks up the rest. Only a drain that collected *nothing*
/// surfaces the error, since there is no data to lose and the caller needs to
/// know the cluster is unreachable.
async fn drain_run_loop_output(
    coordinator: &SharedCoordinator,
    job_id: &JobId,
    targets: Vec<(String, String)>,
    wait_ms: u64,
) -> Result<Vec<Vec<u8>>, ContinuousStreamError> {
    use krishiv_proto::{TaskId, TransportVersion, wire};
    let channels = coordinator.read().await.executor_channels.clone();
    let mut seen_endpoints = std::collections::BTreeSet::new();
    let mut payloads = Vec::new();
    let mut first_error: Option<ContinuousStreamError> = None;
    let mut failed_endpoints: Vec<String> = Vec::new();
    for (task_id, endpoint) in targets {
        if !seen_endpoints.insert(endpoint.clone()) {
            continue;
        }
        if crate::is_in_process_task_endpoint(&endpoint) {
            continue;
        }
        let channel = match Coordinator::get_or_connect_channel_on_map(&channels, &endpoint).await {
            Ok(channel) => channel,
            Err(error) => {
                first_error.get_or_insert(ContinuousStreamError::Scheduler(error));
                failed_endpoints.push(endpoint.clone());
                continue;
            }
        };
        let max = krishiv_proto::max_grpc_message_bytes();
        let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
            channel,
            crate::coordinator::task_assignment::inject_executor_task_request_context
                as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        )
        .max_decoding_message_size(max)
        .max_encoding_message_size(max);
        // The long-poll budget goes to the FIRST executor only (task #149
        // fix 12): the fan-out is sequential, so paying it per executor would
        // multiply worst-case latency by the executor count; once the first
        // wait returns, the remaining executors are checked immediately.
        let this_wait =
            if payloads.is_empty() && failed_endpoints.is_empty() && seen_endpoints.len() == 1 {
                wait_ms
            } else {
                0
            };
        let request = krishiv_proto::task::DrainContinuousOutputRequest {
            version: TransportVersion::CURRENT,
            job_id: job_id.clone(),
            task_id: TaskId::try_new(&task_id).map_err(|e| invalid_registration(e.to_string()))?,
            wait_ms: this_wait,
        };
        let rpc = tokio::time::timeout(
            RUN_LOOP_RPC_TIMEOUT + std::time::Duration::from_millis(this_wait),
            client.drain_continuous_output(wire::drain_continuous_output_request_to_wire(request)),
        )
        .await
        .map_err(|_| {
            ContinuousStreamError::Unavailable(format!(
                "run-loop drain from {endpoint} timed out after {}s",
                RUN_LOOP_RPC_TIMEOUT.as_secs()
            ))
        })
        .and_then(|result| {
            result.map_err(|status| {
                ContinuousStreamError::Unavailable(format!(
                    "run-loop drain from {endpoint} failed: {status}"
                ))
            })
        })
        .and_then(|response| {
            wire::drain_continuous_output_response_from_wire(response.into_inner())
                .map_err(|e| invalid_registration(e.to_string()))
        });
        match rpc {
            Ok(decoded) => {
                if !decoded.ipc_bytes.is_empty() {
                    payloads.push(decoded.ipc_bytes);
                }
            }
            Err(error) => {
                first_error.get_or_insert(error);
                failed_endpoints.push(endpoint.clone());
            }
        }
    }
    if let Some(error) = first_error {
        if payloads.is_empty() {
            return Err(error);
        }
        // Data in hand outranks the error: these payloads exist nowhere else.
        tracing::warn!(
            job_id = %job_id,
            failed_endpoints = ?failed_endpoints,
            collected = payloads.len(),
            %error,
            "run-loop drain was partial; returning what was collected because the \
             executors' egress buffers were already cleared. Re-drain for the rest."
        );
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
) -> Result<Json<ContinuousStopWithSavepointResponse>, (StatusCode, String)> {
    let job_id = JobId::try_new(&job_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid job id: {error}")))?;
    let mut coord = coordinator.write().await;
    // 404 unknown / 409 non-streaming, matching the other continuous routes.
    continuous_job_view(&coord, &job_id).map_err(|error| scheduler_error_response(&error))?;
    let epoch = coord
        .stop_job_with_savepoint(&job_id, Some(String::from("continuous-stop")))
        .map_err(|error| scheduler_error_response(&error))?;
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
) -> Result<Json<ContinuousPushResponse>, (StatusCode, String)> {
    use base64::Engine as _;
    // Every rejection on this path now carries why. The push surface is the one
    // callers drive in a loop, so an unexplained status code here is the most
    // expensive kind of silence.
    let bad = |message: &str| (StatusCode::BAD_REQUEST, message.to_owned());
    let ipc_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.input_batches_b64.as_bytes())
        .map_err(|error| bad(&format!("input_batches_b64 is not valid base64: {error}")))?;
    if ipc_bytes.is_empty()
        || crate::batch_sql::decode_inline_record_batches(std::slice::from_ref(&ipc_bytes))
            .map_err(|error| {
                bad(&format!(
                    "input_batches_b64 is not a valid Arrow IPC stream: {error}"
                ))
            })?
            .is_empty()
    {
        return Err(bad("continuous push carried no record batches"));
    }

    let job_id = krishiv_proto::JobId::try_new(&body.job_id)
        .map_err(|error| bad(&format!("invalid job_id '{}': {error}", body.job_id)))?;

    // Leader-fence before the existence check (see
    // `push_continuous_input_coordinated`): a push to a non-active replica
    // during a leadership transition must be a retryable 503, not a 404
    // "unknown job" for a job that is durably registered elsewhere.
    {
        let coord = coordinator.read().await;
        if let Err(e) = coord.ensure_active() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "continuous push not served here: {e}; retry (routes to the active leader)"
                ),
            ));
        }
    }

    // Phase 55: run-loop jobs receive pushes directly on their executors —
    // no coordinator fencing, no coordinator-buffered data (control-plane-
    // only invariant). The push is ingest API, never the execution driver.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id).map_err(|error| scheduler_error_response(&error))?
    };
    if let Some(targets) = run_loop {
        let targets = match body.side.as_deref() {
            None => targets,
            Some(side @ ("L" | "R")) => targets
                .into_iter()
                .map(|(task, endpoint)| (format!("{task}#{side}"), endpoint))
                .collect(),
            Some(other) => {
                return Err(bad(&format!(
                    "push side must be \"L\" or \"R\", got '{other}'"
                )));
            }
        };
        push_run_loop_input(&coordinator, &job_id, targets, ipc_bytes)
            .await
            .map_err(|error| match error {
                ContinuousStreamError::Scheduler(e) => scheduler_error_response(&e),
                backpressure @ ContinuousStreamError::Backpressure(_) => {
                    (StatusCode::TOO_MANY_REQUESTS, backpressure.to_string())
                }
                other => (StatusCode::SERVICE_UNAVAILABLE, other.to_string()),
            })?;
        return Ok(Json(ContinuousPushResponse { success: true }));
    }
    if body.side.is_some() {
        return Err(bad(
            "side-tagged pushes are only meaningful for run-loop two-source jobs",
        ));
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
            .map_err(|error| scheduler_error_response(&error))?;
        let assignments = match coord.launch_assigned_task_assignments(&job_id) {
            Ok(assignments) if !assignments.is_empty() => assignments,
            Ok(_) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    String::from("no task assignments were launched for this cycle"),
                ));
            }
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(scheduler_error_response(&error));
            }
        };
        let targets = match coord.resolve_assignment_targets(assignments) {
            Ok(targets) => targets,
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(scheduler_error_response(&error));
            }
        };
        if targets
            .iter()
            .any(|(endpoint, _)| crate::is_in_process_task_endpoint(endpoint))
        {
            coord.abort_continuous_input_cycle(&job_id);
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                String::from("continuous push rejected: service unavailable"),
            ));
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
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    String::from("continuous push rejected: service unavailable"),
                ));
            }
        };
    let mut coord = coordinator.write().await;
    if !coord.continuous_input_cycles.contains(&job_id) {
        return Err((
            StatusCode::CONFLICT,
            String::from("continuous push rejected: conflict"),
        ));
    }
    let accepted = coord.apply_assignment_dispatch_responses(&job_id, &responses);
    if accepted != target_count {
        coord.abort_continuous_input_cycle(&job_id);
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            String::from("continuous push rejected: service unavailable"),
        ));
    }

    Ok(Json(ContinuousPushResponse { success: true }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousDrainRequest {
    pub job_id: String,
    /// Long-poll budget in milliseconds (task #149 fix 12): when the job's
    /// egress is empty, the drain parks up to this long for output instead
    /// of returning empty immediately. Absent/0 keeps the immediate return.
    #[serde(default)]
    pub wait_ms: u64,
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
) -> Result<Json<ContinuousDrainResponse>, (StatusCode, String)> {
    let job_id = krishiv_proto::JobId::try_new(&body.job_id).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid job_id '{}': {error}", body.job_id),
        )
    })?;

    // Phase 55: run-loop jobs serve their egress buffers from the executors.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id).map_err(|error| scheduler_error_response(&error))?
    };
    if let Some(targets) = run_loop {
        let payloads = drain_run_loop_output(&coordinator, &job_id, targets, body.wait_ms)
            .await
            .map_err(|error| match error {
                ContinuousStreamError::Scheduler(e) => scheduler_error_response(&e),
                other => (StatusCode::SERVICE_UNAVAILABLE, other.to_string()),
            })?;
        return Ok(Json(ContinuousDrainResponse {
            inline_record_batch_ipc: payloads,
        }));
    }

    let batches = {
        let mut coord = coordinator.write().await;
        let snapshot = coord
            .job_snapshot(&job_id)
            .map_err(|error| scheduler_error_response(&error))?;
        if snapshot.kind() != krishiv_proto::JobKind::Streaming {
            return Err((
                StatusCode::CONFLICT,
                format!("job {job_id} is not a streaming job"),
            ));
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
    /// The target executor's continuous input buffer is full: flow control,
    /// not failure. Maps to HTTP 429 so producers back off and retry instead
    /// of treating the push as dead (a plain 503 is indistinguishable from
    /// "no executors" and turns ordinary backpressure into a hard error).
    Backpressure(String),
    /// A cycle was aborted because it conflicted with the current state.
    Aborted(String),
}

impl std::fmt::Display for ContinuousStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheduler(e) => write!(f, "scheduler error: {e}"),
            Self::Unavailable(msg) => write!(f, "unavailable: {msg}"),
            Self::Backpressure(msg) => write!(f, "backpressure: {msg}"),
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
    register_continuous_stream_with_options(coordinator, job_id, spec, &options)
        .await
        .map(|_| ())
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
    task: &krishiv_plan::stream_task::StreamingTaskSpec,
    mode: ContinuousJobMode,
    parallelism: u32,
    options: &ContinuousRegistrationOptions,
) -> Result<krishiv_proto::JobSpec, ContinuousStreamError> {
    use krishiv_plan::ExecutionKind;
    use krishiv_plan::stream_task::StreamingTaskSpec;
    use krishiv_plan::window::encode_window_execution_spec;
    use krishiv_proto::{JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};

    let stage_id =
        StageId::try_new("stage-streaming").map_err(|e| invalid_registration(e.to_string()))?;
    // Per-class run-loop fragment payload: windows keep the compact codec
    // (wire compatibility); the other classes are raw JSON, matching the
    // executor's parsers. `(prefix, payload)`.
    let (rl_prefix, encoded_spec) = match task {
        StreamingTaskSpec::Window(w) => (
            "stream:rloop:",
            encode_window_execution_spec(w).map_err(|e| invalid_registration(e.to_string()))?,
        ),
        StreamingTaskSpec::Join(j) => (
            "stream:rjoin:",
            serde_json::to_string(j).map_err(|e| invalid_registration(e.to_string()))?,
        ),
        StreamingTaskSpec::Pipeline(pl) => (
            "stream:rpipe:",
            serde_json::to_string(pl).map_err(|e| invalid_registration(e.to_string()))?,
        ),
        StreamingTaskSpec::Stateless(st) => (
            "stream:rbatch:",
            serde_json::to_string(st).map_err(|e| invalid_registration(e.to_string()))?,
        ),
    };
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
            if !matches!(task, StreamingTaskSpec::Window(_)) {
                return Err(invalid_registration(
                    "non-window classes cannot build a cycle fragment (guarded at \
                     registration; reaching this is a routing bug)",
                ));
            }
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
                    "{rl_prefix}{}|{subtask}/{parallelism}|{encoded_spec}",
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

    let mut job_spec =
        JobSpec::new(job_id.clone(), "continuous-streaming", JobKind::Streaming).with_stage(stage);
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
) -> Result<AppliedContinuousRegistration, ContinuousStreamError> {
    register_continuous_task_with_options(
        coordinator,
        job_id,
        &krishiv_plan::stream_task::StreamingTaskSpec::Window(Box::new(spec.clone())),
        options,
    )
    .await
}

/// Class-routed registration (task #147): windows keep every existing rule;
/// the join/pipeline/stateless classes are RUN-LOOP ONLY (a cycle task
/// exists for one push-triggered invocation — a two-sided join or a pipeline
/// cannot express its input discipline there, and refusing names the mode
/// that works), and pipelines are parallelism-1 (stage re-keying; see the
/// executor's refusal).
pub async fn register_continuous_task_with_options(
    coordinator: &SharedCoordinator,
    job_id: &str,
    task: &krishiv_plan::stream_task::StreamingTaskSpec,
    options: &ContinuousRegistrationOptions,
) -> Result<AppliedContinuousRegistration, ContinuousStreamError> {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    use krishiv_proto::JobId;

    task.validate()
        .map_err(|e| invalid_registration(e.to_string()))?;
    let parallelism = options.parallelism.unwrap_or(1).max(1);
    let mode = ContinuousJobMode::parse(options.mode.as_deref(), parallelism)
        .map_err(invalid_registration)?;

    if !matches!(task, StreamingTaskSpec::Window(_)) && mode == ContinuousJobMode::Cycle {
        return Err(invalid_registration(format!(
            "the '{}' streaming class is run-loop only: a cycle task exists for one \
             push-triggered invocation and cannot own a join's two-sided input or a \
             pipeline's stage chain. Register with mode: \"run-loop\"",
            task.class_name()
        )));
    }
    if matches!(task, StreamingTaskSpec::Pipeline(_)) && parallelism != 1 {
        return Err(invalid_registration(format!(
            "pipeline parallelism {parallelism} is not supported: stages re-key between \
             stages, so parallel subtask-local pipelines silently compute wrong per-key \
             answers; register with parallelism 1 (the inter-stage exchange is the \
             tracked follow-up)"
        )));
    }

    // A cycle task exists for exactly one push-triggered invocation, so between
    // pushes no live thread owns wall clock for it — that is why
    // `StreamingLoop::Cycle` declares `IdleTick::None`, and why declaring
    // otherwise fails the build.
    //
    // A session window closes on inactivity, and inactivity is by definition
    // the absence of the events that would advance the watermark. So a session
    // job in cycle mode can never close its windows: it is accepted, it runs,
    // it reports healthy, and it emits nothing until some later push happens to
    // carry an event past the gap. Refusing it at registration turns a silent
    // wrong answer into a message naming the mode that works.
    //
    // This is the DEFAULT path, not an edge case: `ContinuousJobMode::parse`
    // maps `None | "" | "cycle"` to Cycle, so a caller who never thought about
    // execution mode lands here.
    if let StreamingTaskSpec::Window(w) = task
        && mode == ContinuousJobMode::Cycle
        && krishiv_plan::window::requires_wall_clock(&w.window_kind)
    {
        return Err(invalid_registration(
            "session windows close on inactivity, which needs a wall clock; a cycle task \
             exists only for one push-triggered invocation and owns no thread between \
             pushes, so this job would accept events and never emit a session. Register \
             with mode: \"run-loop\", which is long-lived and ticks on elapsed time.",
        ));
    }

    if mode == ContinuousJobMode::RunLoop
        && options.checkpoint_interval_ms.is_some() != options.checkpoint_storage_path.is_some()
    {
        return Err(invalid_registration(
            "run-loop checkpointing requires BOTH checkpoint_interval_ms and checkpoint_storage_path (or neither)",
        ));
    }
    // A run-loop job without checkpointing has no CheckpointCoordinator, so it
    // has no savepoint — and therefore NO non-lossy stop at all. Its teardown
    // does bookkeeping only: the open window and everything accumulated since
    // the job started are discarded, and a restart begins from empty. That is a
    // legitimate choice for a test or a job whose real output is its sink, but
    // it is a choice, and registration is the moment it is made. Warning here
    // rather than erroring, because the cycle model has always worked this way
    // and turning it into a hard failure would break callers who know.
    if mode == ContinuousJobMode::RunLoop && options.checkpoint_interval_ms.is_none() {
        tracing::warn!(
            job_id,
            parallelism,
            "run-loop job registered WITHOUT checkpointing: it has no savepoint and therefore no \
             non-lossy stop — a stop, cancel, or executor loss discards all window state since \
             the job started, and a restart resumes from empty. Pass checkpoint_interval_ms + \
             checkpoint_storage_path to make the state recoverable."
        );
    }
    let job_id_typed = JobId::try_new(job_id).map_err(|e| invalid_registration(e.to_string()))?;
    let job_spec = build_continuous_job_spec(&job_id_typed, task, mode, parallelism, options)?;

    let freshly_submitted = {
        let mut coord = coordinator.write().await;
        coord
            .ensure_active()
            .map_err(ContinuousStreamError::Scheduler)?;
        upsert_continuous_streaming_job(
            &mut coord,
            &job_id_typed,
            task,
            mode,
            parallelism,
            job_spec,
        )
        .await
        .map_err(ContinuousStreamError::Scheduler)?
    };

    // KNOWN GAP (still open, but no longer a guess — both obvious fixes were
    // built and measured, 2026-08-17):
    //
    // Every failure path in `launch_run_loop_job` runs *after* the job spec was
    // upserted, so a failed launch leaves the job registered with its subtasks
    // assigned to executors that never received them. Re-registering an
    // identical shape is a deliberate no-op (not `freshly_submitted`), so a
    // retry cannot launch it either — the id stays wedged until an explicit
    // deregister.
    //
    // Candidate 1, key the launch on "has this job actually been LAUNCHED"
    // rather than on `freshly_submitted`. Necessary, and insufficient: the
    // retry does re-enter the launch, then fails differently with "produced no
    // launchable assignments", because the first attempt already transitioned
    // its tasks out of Assigned before dispatch failed. Retryability needs a
    // clean slate, not permission to try.
    //
    // Candidate 2, roll the registration back on failure (cancel + evict +
    // drop snapshot). This works, and it broke FOUR existing tests — including
    // `run_loop_registration_builds_parallel_subtasks` and
    // `run_loop_reregistration_is_convergent`, which are this crate's only
    // coverage of run-loop shape-building and convergence. They inspect the
    // job record *after* a launch that necessarily fails, because
    // `make_coordinator_with_executor` registers `IN_PROCESS_TASK_ENDPOINT`
    // and a launch against it can never succeed. Rollback deletes the record
    // they read, so it trades a rare recorded wedge for permanently losing
    // that coverage.
    //
    // What this actually needs is one of: a coordinator primitive that returns
    // a job's tasks to Assigned so a retry can re-dispatch them (candidate 1
    // then works and the record survives), or a test fixture with a
    // dispatchable executor so a launch can succeed and the coverage stops
    // depending on the failure path. Both are real work; neither is a patch to
    // this function.
    if mode == ContinuousJobMode::RunLoop && freshly_submitted {
        launch_run_loop_job(coordinator, &job_id_typed, &options.sources).await?;
    }
    Ok(AppliedContinuousRegistration {
        mode,
        parallelism,
        sources: options.sources.len(),
        checkpointing: options.checkpoint_interval_ms.is_some()
            && options.checkpoint_storage_path.is_some(),
    })
}

/// What a registration **actually** applied, as decided by
/// [`register_continuous_stream_with_options`].
///
/// This exists so remote callers can be told the truth. The registration
/// request is a set of *options*; the shape that ends up running is derived
/// here — `parallelism` is clamped, `mode` is parsed from a string, and
/// checkpointing is armed only when both knobs are present. A caller that
/// asked for run-loop parallelism 8 and reached a coordinator that ignored the
/// request (because it predates the field, or because it took a different
/// branch) would otherwise see a bare success and believe it got what it asked
/// for. Every seam that can be driven from another process echoes this back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedContinuousRegistration {
    /// The execution model actually registered.
    pub mode: ContinuousJobMode,
    /// The subtask count actually registered (post-clamp).
    pub parallelism: u32,
    /// How many registry connector sources the subtasks took ownership of.
    pub sources: usize,
    /// Whether barrier checkpointing was armed (needs BOTH knobs).
    pub checkpointing: bool,
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
                    // The whole run-loop FAMILY launches the same way (task
                    // #147): windows, joins, pipelines, stateless.
                    let is_rloop = typed.as_ref().is_some_and(|t| {
                        [
                            "stream:rloop:",
                            "stream:rjoin:",
                            "stream:rpipe:",
                            "stream:rbatch:",
                        ]
                        .iter()
                        .any(|p| t.body.starts_with(p))
                    });
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
                    let endpoint = coord.find_executor_endpoint(&executor_id).ok_or_else(|| {
                        ContinuousStreamError::Unavailable(format!(
                            "run-loop job {job_id}: executor {executor_id} has no task endpoint"
                        ))
                    })?;
                    peers.push((index, task.task_id().as_str().to_owned(), endpoint));
                }
            }
        }
        if peers.is_empty() {
            return Err(invalid_registration(format!(
                "job {job_id} has no run-loop-family tasks to launch"
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
        // An in-process endpoint cannot host a run-loop subtask: the push and
        // drain paths both fail closed on one, so a job launched here would be
        // permanently unreachable. Reject at launch, exactly as the cycle-push
        // path does before dispatching its own targets.
        if targets
            .iter()
            .any(|(endpoint, _)| crate::is_in_process_task_endpoint(endpoint))
        {
            return Err(ContinuousStreamError::Unavailable(format!(
                "run-loop job {job_id} was assigned an in-process executor endpoint, \
                 which cannot receive continuous push or serve drain; register a \
                 remote executor"
            )));
        }
        let count = targets.len();
        (targets, coord.executor_channels.clone(), count)
    };

    let responses = Coordinator::deliver_assignment_targets_with_channels(channels, targets)
        .await
        .map_err(ContinuousStreamError::Scheduler)?;
    let mut coord = coordinator.write().await;
    let accepted = coord.apply_assignment_dispatch_responses(job_id, &responses);
    // Compare against the number of targets we RESOLVED, not the number of
    // responses we got back. `deliver_assignment_targets_with_channels`
    // partitions undeliverable targets out before producing responses, so
    // `accepted < responses.len()` compared two numbers that shrink together:
    // with every target undeliverable it read `0 < 0`, and registration
    // returned Ok having launched no subtasks at all. The two sibling call
    // sites in this file (`:1174`, `:1778`) both use `!= target_count`.
    if accepted != target_count {
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
    desired_task: &krishiv_plan::stream_task::StreamingTaskSpec,
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
            task: desired_task.clone(),
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
/// Side-tagged run-loop push (task #147): the two-source classes read input
/// from `{job}#{task}#L` / `#R` buffers, so a push for one join side targets
/// the subtask's side-suffixed task id. Window/stateless jobs use the
/// untagged [`push_continuous_input_coordinated`].
pub async fn push_continuous_input_side_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    side: &str,
    ipc_bytes: Vec<u8>,
) -> Result<(), ContinuousStreamError> {
    use krishiv_proto::JobId;
    if side != "L" && side != "R" {
        return Err(invalid_registration(format!(
            "push side must be \"L\" or \"R\", got '{side}'"
        )));
    }
    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    {
        let coord = coordinator.read().await;
        if let Err(e) = coord.ensure_active() {
            return Err(ContinuousStreamError::Unavailable(format!(
                "continuous push not served here: {e}; retry (routes to the active leader)"
            )));
        }
    }
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id_typed).map_err(ContinuousStreamError::Scheduler)?
    };
    let Some(targets) = run_loop else {
        return Err(invalid_registration(
            "side-tagged pushes are only meaningful for run-loop two-source jobs",
        ));
    };
    let targets: Vec<(String, String)> = targets
        .into_iter()
        .map(|(task, endpoint)| (format!("{task}#{side}"), endpoint))
        .collect();
    push_run_loop_input(coordinator, &job_id_typed, targets, ipc_bytes).await
}

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

    // Leader-fence BEFORE the existence check, mirroring register
    // (`ensure_active` up front). Registration is durable and recovered on
    // promotion, so a job unknown to the ACTIVE leader is a genuine error;
    // but a push landing on a demoted/standby/not-yet-recovered replica
    // during a leadership transition would otherwise fall straight into
    // `run_loop_targets`' `UnknownJob` and surface as a hard, non-retryable
    // `scheduler error: unknown job` — the phase58 gate's intermittent
    // streaming failure. Fenced, that transient becomes a retryable
    // `Unavailable` (the client's Service routing then reaches the real
    // leader), and `UnknownJob` past this point means what it says.
    {
        let coord = coordinator.read().await;
        if let Err(e) = coord.ensure_active() {
            return Err(ContinuousStreamError::Unavailable(format!(
                "continuous push not served here: {e}; retry (routes to the active leader)"
            )));
        }
    }

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
/// Close every window a coordinator-backed continuous job still holds open.
///
/// # Why this needs its own machinery
///
/// A drain returns what the watermark already closed, so a bounded source whose
/// final events land inside a window nothing later closes leaves that window
/// unemitted. On a single node the fix is one call to the operator. In a
/// coordinator-backed cluster there is no registry entry and no local operator
/// at all — the operator lives inside the executor, built at
/// `fragment/streaming.rs`. The only way to reach it is to schedule work.
///
/// So this schedules one final cycle carrying a `stream-eos:` input partition,
/// a direct sibling of `stream-peers:`. `execute_streaming_fragment` reads it
/// and calls `StreamDriver::on_stop(CoordinatorDirective)`, which is legitimate
/// precisely because the control plane is the only party that can know a cycle
/// task's stream is over — a cycle exists for one invocation and cannot observe
/// its own source running out.
///
/// # Errors
///
/// Returns an error if the job is unknown, is not a streaming job, runs in
/// run-loop mode (which does not flush on stop — its source is never exhausted),
/// or if the final cycle cannot be launched or does not complete in time.
pub async fn flush_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
) -> Result<Vec<Vec<u8>>, ContinuousStreamError> {
    use krishiv_proto::JobId;

    let invalid = |message: String| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob { message })
    };

    let job_id_typed =
        JobId::try_new(job_id).map_err(|e| invalid(format!("invalid job_id: {e}")))?;

    // A run-loop job's SOURCE never exhausts itself — but a push-fed job's
    // PRODUCER is exactly the party that can know the stream is over, the
    // same principle behind cycle mode's `stream-eos:` partition below. So
    // for run-loop jobs this relays the producer's end-of-stream declaration
    // to every executor hosting a subtask (the reserved `stream-eos` task id
    // on the push RPC); each executor flushes its local window/pipeline
    // state into the job's egress buffer, and the caller drains it next.
    // Returns no inline payloads — run-loop output travels through drain.
    let run_loop = {
        let coord = coordinator.read().await;
        run_loop_targets(&coord, &job_id_typed).map_err(ContinuousStreamError::Scheduler)?
    };
    if let Some(targets) = run_loop {
        let mut endpoints: Vec<String> =
            targets.into_iter().map(|(_, endpoint)| endpoint).collect();
        endpoints.sort_unstable();
        endpoints.dedup();
        if endpoints.is_empty() {
            return Err(ContinuousStreamError::Unavailable(format!(
                "run-loop job {job_id} has no launched subtasks to flush"
            )));
        }
        let channels = coordinator.read().await.executor_channels.clone();
        for endpoint in endpoints {
            push_run_loop_eos(&channels, &job_id_typed, &endpoint).await?;
        }
        return Ok(Vec::new());
    }

    {
        let coord = coordinator.read().await;
        let snapshot = coord
            .job_snapshot(&job_id_typed)
            .map_err(ContinuousStreamError::Scheduler)?;
        if snapshot.kind() != krishiv_proto::JobKind::Streaming {
            return Err(invalid(format!("job {job_id} is not a streaming job")));
        }
    }

    let partition = InputPartition::new("stream-eos", END_OF_STREAM_PARTITION);

    let (targets, channels, target_count) = {
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id_typed, vec![partition])
            .map_err(ContinuousStreamError::Scheduler)?;
        let assignments = match coord.launch_assigned_task_assignments(&job_id_typed) {
            Ok(assignments) if !assignments.is_empty() => assignments,
            Ok(_) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(invalid(format!(
                    "no task assignments were launched for job {job_id}'s end-of-stream cycle"
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
            return Err(invalid(format!(
                "job {job_id}'s end-of-stream cycle resolved to an in-process task \
                 endpoint, which cannot be dispatched"
            )));
        }
        let target_count = targets.len();
        (targets, coord.executor_channels.clone(), target_count)
    };

    let responses =
        match Coordinator::deliver_assignment_targets_with_channels(channels, targets).await {
            Ok(responses) => responses,
            Err(error) => {
                coordinator
                    .write()
                    .await
                    .abort_continuous_input_cycle(&job_id_typed);
                return Err(invalid(format!(
                    "job {job_id}'s end-of-stream cycle could not be delivered: {error}"
                )));
            }
        };
    {
        let mut coord = coordinator.write().await;
        let accepted = coord.apply_assignment_dispatch_responses(&job_id_typed, &responses);
        if accepted != target_count {
            coord.abort_continuous_input_cycle(&job_id_typed);
            return Err(invalid(format!(
                "job {job_id}'s end-of-stream cycle was accepted by {accepted}/{target_count} \
                 executors; its trailing windows would be incomplete"
            )));
        }
    }

    // Unlike a push, this call must not return before the cycle's output is
    // available: its caller is a bounded run about to close its sinks, and a
    // flush whose rows arrive after that is a flush that did nothing. The cycle
    // fence clears when the task result lands, so that is the completion
    // signal.
    let deadline = std::time::Instant::now() + END_OF_STREAM_CYCLE_TIMEOUT;
    loop {
        {
            let coord = coordinator.read().await;
            if !coord.continuous_input_cycles.contains(&job_id_typed) {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            coordinator
                .write()
                .await
                .abort_continuous_input_cycle(&job_id_typed);
            return Err(invalid(format!(
                "job {job_id}'s end-of-stream cycle did not complete within \
                 {END_OF_STREAM_CYCLE_TIMEOUT:?}; its trailing windows are not in the output"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let mut coord = coordinator.write().await;
    Ok(coord
        .take_job_inline_results(&job_id_typed)
        .unwrap_or_default())
}

/// The `stream-eos:` partition description the executor matches on.
///
/// Kept as a constant next to its only producer so the string the coordinator
/// writes and the prefix `read_end_of_stream_directive` looks for cannot drift
/// apart silently — they live in different crates.
const END_OF_STREAM_PARTITION: &str = "stream-eos:1";

/// How long a final end-of-stream cycle may take before the flush is reported
/// as failed.
///
/// Generous: this runs once per bounded job, and the alternative to waiting is
/// returning an incomplete answer that looks complete.
const END_OF_STREAM_CYCLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

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
        return drain_run_loop_output(coordinator, &job_id_typed, targets, 0).await;
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

/// Return previously drained continuous-stream payloads to the FRONT of the
/// job's inline result store.
///
/// The companion of [`drain_continuous_stream_coordinated`] for takers that
/// discover — after the consume-once take — that they cannot deliver what
/// they took (e.g. the Flight `ContinuousDrain` action's response-size cap).
/// Returning the payloads lets the caller surface a retryable error while the
/// client's streaming fallback finds the data still there, instead of the
/// silent 0-row loss this replaced.
pub async fn return_continuous_stream_payloads(
    coordinator: &SharedCoordinator,
    job_id: &str,
    payloads: Vec<Vec<u8>>,
) -> Result<(), ContinuousStreamError> {
    let job_id_typed = krishiv_proto::JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    let mut coord = coordinator.write().await;
    // Refuse for run-loop jobs rather than write somewhere nothing reads.
    //
    // `drain_continuous_stream_coordinated` returns early for a run-loop job
    // and serves its executors' egress buffers; `job_inline_results` is the
    // *cycle* model's store. This function had no mode check, so a run-loop
    // put-back landed in a map no drain of that job would ever consult — the
    // exact consume-once loss the unshift machinery exists to prevent, plus a
    // map that grows until job teardown. Failing loudly is the honest answer:
    // there is no put-back RPC to executor egress, so the payloads genuinely
    // cannot be returned, and pretending otherwise loses them silently.
    if run_loop_targets(&coord, &job_id_typed)
        .map_err(ContinuousStreamError::Scheduler)?
        .is_some()
    {
        return Err(ContinuousStreamError::Unavailable(format!(
            "continuous job {job_id} runs the run-loop model, whose output is served from \
             executor egress rather than the coordinator's inline result store; drained \
             payloads cannot be returned to it. Re-drain instead."
        )));
    }
    coord.unshift_job_inline_results(&job_id_typed, payloads);
    Ok(())
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

    /// A taker that cannot deliver what it took must be able to hand the
    /// payloads back — AHEAD of any output that landed in between — so the
    /// retry drains the same data in the same order. Without this, the
    /// Flight `ContinuousDrain` action's oversized-response rejection
    /// consumed a whole cycle's output and the client's fallback re-drained
    /// an empty store (87k windowed rows reported as "0 rows", 2026-08-10).
    #[test]
    fn unshift_puts_payloads_back_ahead_of_newer_output() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("unshift-coord").unwrap());
        let job_id = krishiv_proto::JobId::try_new("unshift-job").unwrap();

        // Newer cycle output arrived between the take and the return.
        coordinator
            .job_inline_results
            .insert(job_id.clone(), vec![vec![3u8]]);
        coordinator.unshift_job_inline_results(&job_id, vec![vec![1u8], vec![2u8]]);
        assert_eq!(
            coordinator.take_job_inline_results(&job_id),
            Some(vec![vec![1u8], vec![2u8], vec![3u8]]),
            "returned payloads must drain first, in their original order"
        );

        // Returning to a fully drained store must recreate the entry.
        coordinator.unshift_job_inline_results(&job_id, vec![vec![9u8]]);
        assert_eq!(
            coordinator.take_job_inline_results(&job_id),
            Some(vec![vec![9u8]]),
        );

        // An empty return is a no-op, not an empty entry that would trip the
        // push fence's undrained-output check.
        coordinator.unshift_job_inline_results(&job_id, Vec::new());
        assert_eq!(coordinator.take_job_inline_results(&job_id), None);
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

    /// A session-window spec: the one kind that cannot close without a clock.
    fn session_spec() -> WindowExecutionSpec {
        WindowExecutionSpec {
            window_kind: WindowKind::Session,
            session_gap_ms: Some(30_000),
            ..tumbling_spec()
        }
    }

    /// Cycle mode must refuse a session window instead of accepting one it can
    /// never close.
    ///
    /// Cycle is the DEFAULT — `ContinuousJobMode::parse` maps `None`, `""` and
    /// `"cycle"` to it — so this is the path a caller who never thought about
    /// execution mode takes. Before the guard, such a job registered cleanly,
    /// ran, reported healthy, and emitted nothing.
    ///
    /// The message must name the mode that works. A refusal that says only
    /// "unsupported" converts a silent wrong answer into a dead end.
    #[tokio::test]
    async fn cycle_mode_refuses_a_session_window_it_could_never_close() {
        for mode in [None, Some(String::new()), Some(String::from("cycle"))] {
            let coordinator = make_coordinator_with_executor("cycle-session").await;
            let options = ContinuousRegistrationOptions {
                mode: mode.clone(),
                ..Default::default()
            };
            let error = register_continuous_stream_with_options(
                &coordinator,
                "cycle-session-job",
                &session_spec(),
                &options,
            )
            .await
            .expect_err("a session window in cycle mode must be refused");

            let text = error.to_string();
            assert!(
                text.contains("run-loop"),
                "the refusal must name the mode that works, or the caller has no move: \
                 {text}"
            );
            assert!(
                text.contains("wall clock") || text.contains("inactivity"),
                "the refusal must say WHY, so it reads as a design limit rather than an \
                 arbitrary block: {text}"
            );
        }
    }

    /// The guard is scoped to session windows and to cycle mode.
    ///
    /// Without this, `requires_wall_clock` could return `true` for everything
    /// and the test above would still pass while the guard refused every job in
    /// the default mode — a far worse outcome than the defect it fixes.
    #[tokio::test]
    async fn the_session_guard_refuses_nothing_else() {
        let coordinator = make_coordinator_with_executor("cycle-tumbling").await;
        register_continuous_stream_with_options(
            &coordinator,
            "cycle-tumbling-job",
            &tumbling_spec(),
            &ContinuousRegistrationOptions::default(),
        )
        .await
        .expect("a tumbling window in cycle mode closes on data and must be accepted");

        // And a session window is fine on the loop that owns a wall clock.
        let coordinator = make_coordinator_with_executor("rloop-session").await;
        let options = ContinuousRegistrationOptions {
            mode: Some(String::from("run-loop")),
            ..Default::default()
        };
        let outcome = register_continuous_stream_with_options(
            &coordinator,
            "rloop-session-job",
            &session_spec(),
            &options,
        )
        .await;
        assert!(
            !matches!(
                outcome.as_ref().err(),
                Some(ContinuousStreamError::Scheduler(
                    crate::SchedulerError::InvalidJob { .. }
                ))
            ),
            "run-loop mode owns a wall clock, so a session window must not be refused \
             as invalid there; got {outcome:?}"
        );
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
            key_parts: Vec::new(),
            derived_columns: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            processing_time: false,
            window_timezone: None,
            row_filter: None,
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
        .expect_err(
            "the only executor here has an in-process task endpoint, which cannot host a \
             run-loop subtask; launch must refuse rather than report success",
        );

        // The job spec is recorded by `upsert_continuous_streaming_job` before
        // launch is attempted, so the shape assertions below still hold — and
        // they now describe a job that provably did NOT start, which is what
        // this coordinator can actually produce.
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
        assert_eq!(shape.window_spec().cloned(), Some(tumbling_spec()));

        let view = continuous_job_view(&coord, &job_id).unwrap();
        assert_eq!(view.delivery.model, "run-loop");
        assert_eq!(view.delivery.parallelism, 3);
        assert_eq!(view.task_count, 3);
    }

    /// A run-loop launch that dispatched nothing must not report success.
    ///
    /// The guard read `accepted < responses.len()`, comparing two numbers that
    /// shrink together: undeliverable targets are partitioned out *before*
    /// responses are produced, so a launch where every target was undeliverable
    /// evaluated `0 < 0` and returned `Ok(())` having started no subtasks. Every
    /// run-loop test on such a coordinator was green while launching nothing.
    ///
    /// An all-in-process executor set is exactly that case: every target is
    /// partitioned out before delivery, so `responses` is empty and the old
    /// comparison could not fire. It is also the fixture every run-loop test in
    /// this file uses, which is why none of them noticed.
    #[tokio::test]
    async fn run_loop_launch_rejects_when_no_subtask_was_actually_dispatched() {
        let coordinator = make_coordinator_with_executor("rloop-nolaunch").await;
        let options = ContinuousRegistrationOptions {
            parallelism: Some(3),
            mode: Some(String::from("run-loop")),
            ..Default::default()
        };
        let error = register_continuous_stream_with_options(
            &coordinator,
            "rloop-nolaunch-job",
            &tumbling_spec(),
            &options,
        )
        .await
        .expect_err("a launch that dispatched no subtask must not return Ok");
        assert!(
            matches!(error, ContinuousStreamError::Unavailable(_)),
            "expected Unavailable, got {error:?}"
        );

        // Pin the known gap rather than leaving it undescribed: the refused
        // launch leaves the job registered with its subtasks still assigned.
        // That is a wedge (an identical re-registration is a no-op, so it can
        // never launch on retry) and is recorded as follow-up work at the
        // `launch_run_loop_job` call site. If a later change adds rollback,
        // this assertion is the one to update — deliberately, not silently.
        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("rloop-nolaunch-job").unwrap();
        let jc = coord
            .job_coordinator(&job_id)
            .expect("the job record outlives the failed launch (known gap)");
        let record = jc.read_record();
        assert_eq!(
            record.spec.task_count(),
            3,
            "the spec was recorded before launch was attempted"
        );
    }

    /// Assert a registration got as far as recording its job spec and then
    /// refused to launch, because this test coordinator's only executor has an
    /// in-process endpoint that cannot host a run-loop subtask.
    ///
    /// Spelled out rather than swallowed with `let _ =`: the launch outcome is
    /// incidental to what these upsert tests are about, but silently discarding
    /// it would hide a real regression in the launch guard.
    fn assert_launch_refused_for_in_process(
        result: Result<AppliedContinuousRegistration, ContinuousStreamError>,
    ) {
        match result {
            Err(ContinuousStreamError::Unavailable(message)) => assert!(
                message.contains("in-process executor endpoint"),
                "expected the in-process launch refusal, got: {message}"
            ),
            other => panic!("expected an in-process launch refusal, got {other:?}"),
        }
    }

    /// A run-loop job's drained payloads must never be "returned" into the
    /// cycle model's inline result store.
    ///
    /// `drain_continuous_stream_coordinated` serves run-loop output from
    /// executor egress and never reads `job_inline_results`, but
    /// `return_continuous_stream_payloads` unshifted into that map with no mode
    /// check. A put-back for a run-loop job therefore wrote data into a
    /// structure no drain of that job would ever consult: the consume-once loss
    /// the unshift machinery exists to prevent, plus a map that grows until
    /// teardown. Refusing is the honest answer — there is no put-back RPC to
    /// executor egress, so the payloads genuinely cannot be returned.
    #[tokio::test]
    async fn run_loop_payloads_are_not_returned_into_the_cycle_inline_store() {
        let coordinator = make_coordinator_with_executor("rloop-putback").await;
        let options = ContinuousRegistrationOptions {
            parallelism: Some(2),
            mode: Some(String::from("run-loop")),
            ..Default::default()
        };
        assert_launch_refused_for_in_process(
            register_continuous_stream_with_options(
                &coordinator,
                "rloop-putback-job",
                &tumbling_spec(),
                &options,
            )
            .await,
        );

        let error = return_continuous_stream_payloads(
            &coordinator,
            "rloop-putback-job",
            vec![b"some-drained-ipc".to_vec()],
        )
        .await
        .expect_err("a run-loop put-back must be refused, not silently misfiled");
        assert!(
            matches!(error, ContinuousStreamError::Unavailable(_)),
            "expected Unavailable, got {error:?}"
        );

        // And nothing was written: the store the cycle model drains stays empty.
        let job_id = krishiv_proto::JobId::try_new("rloop-putback-job").unwrap();
        let mut coord = coordinator.write().await;
        assert!(
            coord
                .take_job_inline_results(&job_id)
                .unwrap_or_default()
                .is_empty(),
            "the refused put-back must leave the inline result store untouched"
        );
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
        // Fresh submit → launch attempted → refused (in-process endpoint). The
        // job spec is recorded first, so the convergence assertions still hold.
        assert_launch_refused_for_in_process(
            register_continuous_stream_with_options(
                &coordinator,
                "rloop-upsert-job",
                &tumbling_spec(),
                &options,
            )
            .await,
        );
        // Same shape → no-op: not freshly submitted, so no launch is attempted
        // and this must succeed outright.
        register_continuous_stream_with_options(
            &coordinator,
            "rloop-upsert-job",
            &tumbling_spec(),
            &options,
        )
        .await
        .expect("re-registering an identical shape is a no-op and cannot fail");
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
        // Rescale retires the old incarnation and submits fresh, so launch is
        // attempted again and refused again for the same reason.
        assert_launch_refused_for_in_process(
            register_continuous_stream_with_options(
                &coordinator,
                "rloop-upsert-job",
                &tumbling_spec(),
                &rescaled,
            )
            .await,
        );
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
            spec: Some(tumbling_spec()),
            stream_spec: None,
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
            wait_ms: 0,
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
        assert_eq!(get.0.spec, Some(tumbling_spec()));

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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
        use base64::Engine as _;

        // #197: a continuous job can name any registered connector as its sink.
        // The contract must be the registry form, not the Iceberg one — that is
        // what routes the executor to stage_rloop_connector.
        {
            let spec = ContinuousSinkSpec {
                connector: Some("csv".into()),
                options: [("path".to_string(), "/tmp/stream|out.csv".to_string())]
                    .into_iter()
                    .collect(),
                root: String::new(),
                table: String::new(),
                mode: default_sink_mode(),
                key_columns: Vec::new(),
                op_column: None,
                catalog: None,
                namespace: None,
            };
            let contract = spec.contract_string().expect("registry contract");
            assert!(contract.starts_with("registry-sink:csv|"), "{contract}");
            // Base64 body: a property value containing `|` cannot corrupt the framing.
            let encoded = contract.split_once('|').unwrap().1;
            let decoded: serde_json::Value = serde_json::from_slice(
                &base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .expect("base64"),
            )
            .expect("json");
            assert_eq!(decoded["properties"]["path"], "/tmp/stream|out.csv");
        }
        // A sink spec naming neither a connector nor an Iceberg table is
        // rejected at registration, not at the first cycle on an executor.
        {
            let empty = ContinuousSinkSpec {
                connector: None,
                options: Default::default(),
                root: String::new(),
                table: String::new(),
                mode: default_sink_mode(),
                key_columns: Vec::new(),
                op_column: None,
                catalog: None,
                namespace: None,
            };
            let error = empty.contract_string().expect_err("must be rejected");
            assert!(error.to_string().contains("connector"), "{error}");
        }

        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-delivery-iceberg".into(),
                spec: Some(tumbling_spec()),
                stream_spec: None,
                sink: Some(ContinuousSinkSpec {
                    connector: None,
                    options: Default::default(),
                    root: "/tmp/warehouse".into(),
                    table: "cycles".into(),
                    mode: "append".into(),
                    key_columns: Vec::new(),
                    op_column: None,
                    catalog: None,
                    namespace: None,
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
            spec: Some(tumbling_spec()),
            stream_spec: None,
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
                side: None,
            }),
        )
        .await
        .expect_err("HTTP push must not pretend an in-process target was delivered");
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);

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
            spec: Some(tumbling_spec()),
            stream_spec: None,
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
                spec: Some(spec),
                stream_spec: None,
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

        assert_eq!(error.0, StatusCode::BAD_REQUEST);
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
            spec: Some(tumbling_spec()),
            stream_spec: None,
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
            spec: Some(tumbling_spec()),
            stream_spec: None,
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
            spec: Some(changed.clone()),
            stream_spec: None,
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
            view.spec,
            Some(changed),
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
        assert_eq!(error.0, StatusCode::CONFLICT);
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
                side: None,
            }),
        )
        .await
        .expect_err("unknown push must fail");
        assert_eq!(push.0, StatusCode::NOT_FOUND);

        let drain = api_continuous_drain(
            State(coordinator),
            Json(ContinuousDrainRequest {
                job_id: "missing-job".into(),
                wait_ms: 0,
            }),
        )
        .await
        .expect_err("unknown drain must fail");
        assert_eq!(drain.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn concurrent_push_is_rejected_while_cycle_is_in_flight() {
        let coordinator = make_coordinator_with_executor("busy").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-busy-job".into(),
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
                side: None,
            }),
        )
        .await
        .expect_err("second concurrent cycle must be fenced");
        assert_eq!(error.0, StatusCode::CONFLICT);
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
                side: None,
            }),
        )
        .await
        .expect_err("undrained output must backpressure the next cycle");
        // The status is the contract (back-pressure, deliberate). The MESSAGE is
        // the fix: this used to be a bare 409 with an empty body, so a caller
        // driving pushes in a loop learned nothing about why it was refused or
        // that a drain unwedges it. Assert both, so the explanation cannot be
        // silently dropped again.
        assert_eq!(blocked_push.0, StatusCode::CONFLICT);
        assert!(
            !blocked_push.1.trim().is_empty(),
            "a 409 with an empty body tells the caller nothing"
        );

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

    /// Sibling of the #9 fix in `apply_assignments` (Phase 58 #180): recycling
    /// a continuous job's task for its *next* push cycle must stamp a fresh
    /// `assigned_at_ms`, not clear it to `None`. `reset_stuck_assigned_tasks`
    /// silently skips any `Assigned` task with `assigned_at_ms: None` forever
    /// — if the recycled task's next launch never lands (its executor dies in
    /// the same fault window that triggered the push, or the launch RPC is
    /// simply lost), it becomes permanently invisible to the reaper. Live
    /// found in the Phase 58 chaos gate: a streaming job's task sat `Assigned`
    /// with `assigned_at_ms: None` for 20+ minutes after a successful cycle
    /// recycled it, surviving two full clean 50-cell gate runs undetected.
    #[tokio::test]
    async fn recycled_cycle_task_gets_a_fresh_assigned_at_ms_not_none() {
        use base64::Engine as _;
        use krishiv_proto::{TaskOutputMetadata, TaskState, TaskStatusUpdate};

        let coordinator = make_coordinator_with_executor("recycle-stamp").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-recycle-stamp-job".into(),
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
        let job_id = krishiv_proto::JobId::try_new("cs-recycle-stamp-job").unwrap();

        // First cycle: drive it to Succeeded, exactly like a real push.
        let first = prepare_cycle(&coordinator, "cs-recycle-stamp-job").await;
        let running = TaskStatusUpdate::new(
            job_id.clone(),
            first.stage_id().clone(),
            first.task_id().clone(),
            first.executor_id().clone(),
            TaskState::Running,
            first.attempt_id().as_u32(),
        )
        .with_lease_generation(first.lease_generation());
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
            first.stage_id().clone(),
            first.task_id().clone(),
            first.executor_id().clone(),
            TaskState::Succeeded,
            first.attempt_id().as_u32(),
        )
        .with_lease_generation(first.lease_generation())
        .with_output_metadata(
            TaskOutputMetadata::new("streaming_window", 1, 1, 2)
                .with_inline_record_batch_ipc(vec![output_ipc]),
        );
        coordinator
            .write()
            .await
            .apply_task_update(succeeded)
            .unwrap();
        // Drain so the second cycle's undrained-output guard doesn't fire.
        coordinator.write().await.take_job_inline_results(&job_id);

        // Second cycle: this is the recycle path — the task transitions
        // Succeeded -> Assigned again to prepare for the next push.
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id, vec![input_partition()])
            .unwrap();
        let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
        let task = &record.stages()[0].tasks()[0];
        assert_eq!(
            task.state(),
            TaskState::Assigned,
            "recycle must move the task back to Assigned"
        );
        assert!(
            task.assigned_at_ms.is_some(),
            "a recycled Assigned task must carry a fresh assigned_at_ms or it \
             becomes permanently invisible to reset_stuck_assigned_tasks if \
             its next launch never lands"
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
                spec: Some(tumbling_spec()),
                stream_spec: None,
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

    /// Every rejection on this surface must say why, not just how.
    ///
    /// `scheduler_error_response` was introduced because a bare status code
    /// with an empty body "cost a full bisection" during the Phase 62 soak —
    /// but only `api_continuous_push` was converted. Its siblings kept
    /// returning `StatusCode` alone, so a caller driving register/get/drain
    /// against a mistyped id, a non-streaming job, or a bad snapshot got a
    /// number and nothing else. `drain` is the sharpest case: it is driven in
    /// the same loop as `push`, whose message was already fixed.
    ///
    /// Asserts the status (the contract) AND a non-empty body (the
    /// explanation) so the explanation cannot be dropped again one handler at
    /// a time.
    #[tokio::test]
    async fn every_continuous_rejection_explains_itself() {
        let coordinator = make_coordinator_with_executor("explains").await;

        // Unknown job: drain, get, checkpoint, deregister, stop-with-savepoint.
        let drain = api_continuous_drain(
            State(coordinator.clone()),
            Json(ContinuousDrainRequest {
                job_id: "cs-explains-missing".into(),
                wait_ms: 0,
            }),
        )
        .await
        .expect_err("unknown drain must fail");
        assert_eq!(drain.0, StatusCode::NOT_FOUND);
        assert!(!drain.1.trim().is_empty(), "drain 404 must say which job");

        for (label, result) in [
            (
                "get",
                api_continuous_get(
                    State(coordinator.clone()),
                    Path(String::from("cs-explains-missing")),
                )
                .await
                .err()
                .map(|e| (e.0, e.1)),
            ),
            (
                "checkpoint",
                api_continuous_checkpoint(
                    State(coordinator.clone()),
                    Path(String::from("cs-explains-missing")),
                )
                .await
                .err()
                .map(|e| (e.0, e.1)),
            ),
            (
                "deregister",
                api_continuous_deregister(
                    State(coordinator.clone()),
                    Path(String::from("cs-explains-missing")),
                )
                .await
                .err()
                .map(|e| (e.0, e.1)),
            ),
            (
                "stop-with-savepoint",
                api_continuous_stop_with_savepoint(
                    State(coordinator.clone()),
                    Path(String::from("cs-explains-missing")),
                )
                .await
                .err()
                .map(|e| (e.0, e.1)),
            ),
        ] {
            let (status, message) = result.unwrap_or_else(|| panic!("{label} must reject"));
            assert_eq!(status, StatusCode::NOT_FOUND, "{label} status");
            assert!(!message.trim().is_empty(), "{label} must explain itself");
        }

        // Client faults on register: bad id, unencodable spec, and a restore
        // whose snapshot is not base64.
        let bad_id = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: String::new(),
                spec: Some(tumbling_spec()),
                stream_spec: None,
                sink: None,
                parallelism: None,
                mode: None,
                sources: Vec::new(),
                checkpoint_interval_ms: None,
                checkpoint_storage_path: None,
            }),
        )
        .await
        .expect_err("empty job id must be rejected");
        assert_eq!(bad_id.0, StatusCode::BAD_REQUEST);
        assert!(
            bad_id.1.contains("job_id"),
            "the 400 must name the offending field, got {:?}",
            bad_id.1
        );

        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-explains-job".into(),
                spec: Some(tumbling_spec()),
                stream_spec: None,
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
        let bad_snapshot = api_continuous_restore(
            State(coordinator),
            Path(String::from("cs-explains-job")),
            Json(ContinuousRestoreRequest {
                snapshot_b64: String::from("!!! not base64 !!!"),
            }),
        )
        .await
        .expect_err("malformed snapshot must be rejected");
        assert_eq!(bad_snapshot.0, StatusCode::BAD_REQUEST);
        assert!(
            bad_snapshot.1.contains("base64"),
            "the 400 must say the snapshot was not decodable, got {:?}",
            bad_snapshot.1
        );
    }

    /// A continuous push to a NON-active coordinator must be a retryable
    /// `Unavailable`, never `UnknownJob`. Registration is durable and
    /// recovered on promotion, so a push that lands on a standby during a
    /// leadership transition is a routing blip the client should retry (its
    /// Service reaches the real leader) — not a hard "unknown job" the
    /// phase58 gate's `retry_engine` treats as a permanent failure. Without
    /// the leader fence the standby falls straight into the existence check
    /// and returns `Scheduler(UnknownJob)`, the intermittent gate failure.
    #[tokio::test]
    async fn continuous_push_to_a_standby_is_retryable_not_unknown_job() {
        let coord_id = CoordinatorId::try_new("coord-cs-standby-push").unwrap();
        let coordinator = SharedCoordinator::new(Coordinator::standby(coord_id));

        let err = push_continuous_input_coordinated(&coordinator, "any-job", vec![1, 2, 3])
            .await
            .expect_err("a standby must not serve a continuous push");

        match err {
            ContinuousStreamError::Unavailable(msg) => {
                assert!(
                    msg.contains("active leader"),
                    "the 503 must tell the caller to retry against the leader, got {msg:?}"
                );
            }
            other => panic!(
                "a standby push must be retryable Unavailable, not {other:?} \
                 (an UnknownJob here is the unfenced bug)"
            ),
        }
    }

    /// Task #147: a JOIN registered through `stream_spec` builds a
    /// stream:rjoin: run-loop fragment whose decode round-trips the class —
    /// the tripwire that catches a coordinator silently window-planning a
    /// join. Cycle mode for a non-window class is refused by name.
    #[tokio::test]
    async fn classed_join_registration_round_trips_and_cycle_is_refused() {
        use krishiv_plan::stream_join::StreamingJoinSpec;
        use krishiv_plan::stream_task::StreamingTaskSpec;

        let join = StreamingJoinSpec {
            left_source: "bid".into(),
            right_source: "auction".into(),
            time_column: "ts".into(),
            left_key_column: "auction".into(),
            right_key_column: "id".into(),
            window_ms: 10_000,
        };
        let task = StreamingTaskSpec::Join(Box::new(join));
        let coordinator = make_coordinator_with_executor("classed-join").await;

        // Cycle (the default mode) is refused BY NAME for non-window classes.
        let cycle_err = register_continuous_task_with_options(
            &coordinator,
            "classed-join-cycle",
            &task,
            &ContinuousRegistrationOptions::default(),
        )
        .await
        .expect_err("cycle must be refused");
        assert!(
            cycle_err.to_string().contains("run-loop only"),
            "{cycle_err}"
        );

        let options = ContinuousRegistrationOptions {
            mode: Some("run-loop".into()),
            parallelism: Some(2),
            ..Default::default()
        };
        // The in-process fixture cannot host run-loop pushes; reaching THAT
        // refusal proves routing, validation, fragment build, submission and
        // launch-target discovery all succeeded for the classed job — the
        // same dependence the rloop registration tests document.
        let err =
            register_continuous_task_with_options(&coordinator, "classed-join", &task, &options)
                .await
                .expect_err("in-process endpoints cannot serve run-loops");
        assert!(
            err.to_string().contains("in-process executor endpoint"),
            "{err}"
        );

        // The submitted record is decodable and class-faithful even though
        // launch was refused.
        let coord = coordinator.read().await;
        let job_id = JobId::try_new("classed-join").unwrap();
        let jc = coord.job_coordinator(&job_id).unwrap();
        let record = jc.read_record();
        let shape = decode_continuous_job_shape(&record).expect("decodable");
        assert_eq!(shape.task.class_name(), "join", "class survives the wire");
        assert_eq!(shape.parallelism, 2);
        assert_eq!(shape.mode, ContinuousJobMode::RunLoop);
    }
}
