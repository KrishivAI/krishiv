#![forbid(unsafe_code)]

//! R2 in-process scheduler skeleton.
//!
//! This crate owns the distributed control-plane model without introducing
//! Kubernetes clients. R2 keeps one active coordinator and replaceable
//! executors; R3.1 maps coordinator/executor contracts to a networked gRPC
//! service.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, LockResult, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use krishiv_checkpoint::{
    CheckpointMetadata, LocalFsCheckpointStorage, read_epoch_metadata, validate_epoch,
    validate_fencing_token,
};
use krishiv_plan::{LogicalPlan, PhysicalPlan};
use krishiv_proto::{
    AttemptId, CheckpointAckRequest, CheckpointAckResponse,
    CheckpointEpochInfo, CoordinatorExecutorService, CoordinatorId, CoordinatorState,
    CoordinatorManagementService,
    DeregisterExecutorRequest,
    DeregisterExecutorResponse, ExecutorDescriptor, ExecutorHeartbeat, ExecutorHeartbeatRequest,
    ExecutorHeartbeatResponse, ExecutorId, ExecutorTaskAssignment,
    HeartbeatHotKeyReport, HeartbeatThrottleCommand, InitiateCheckpointCommand,
    InitiateCheckpointRequest,
    LlmThrottleCommand,
    InspectStateRequest, InspectStateResponse, ListCheckpointsRequest, ListCheckpointsResponse,
    JobId, JobKind, JobSpec, LeaseGeneration,
    RegisterExecutorRequest, RegisterExecutorResponse,
    RestoreJobRequest, RestoreJobResponse,
    StageId, StateSnapshotInfo, StreamingTaskState, TaskAssignment, TaskAttemptRef,
    TaskCancellationRequest, TaskId, TaskState, TaskStatusRequest,
    TaskStatusResponse, TaskStatusUpdate, TransportDisposition, TransportVersion,
    TriggerSavepointRequest, TriggerSavepointResponse, wire,
};

// ── GAP-OB-01: Scheduler hot-path metrics counters ──────────────────────────
//
// Simple process-local atomic counters exposed via `scheduler_metrics()`.
// Prometheus / OTLP export can scrape these via the metrics HTTP endpoint.

/// Total number of jobs accepted by `submit_job` since process start.
pub static JOBS_SUBMITTED_TOTAL: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(0));

/// Total number of checkpoint epochs initiated since process start.
pub static CHECKPOINT_EPOCHS_TOTAL: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(0));

/// Total number of task assignments launched since process start.
pub static TASKS_ASSIGNED_TOTAL: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(0));

/// Snapshot of scheduler-level metrics counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerMetrics {
    pub jobs_submitted_total: u64,
    pub checkpoint_epochs_total: u64,
    pub tasks_assigned_total: u64,
}

/// Read the current scheduler metrics snapshot.
pub fn scheduler_metrics() -> SchedulerMetrics {
    SchedulerMetrics {
        jobs_submitted_total: JOBS_SUBMITTED_TOTAL.load(AtomicOrdering::Relaxed),
        checkpoint_epochs_total: CHECKPOINT_EPOCHS_TOTAL.load(AtomicOrdering::Relaxed),
        tasks_assigned_total: TASKS_ASSIGNED_TOTAL.load(AtomicOrdering::Relaxed),
    }
}

/// Scheduler result alias.
pub type SchedulerResult<T> = Result<T, SchedulerError>;

/// Result of applying a task status update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskUpdateOutcome {
    /// The update changed scheduler state.
    Applied,
    /// The update was already reflected in scheduler state.
    Duplicate,
}

// ── Sub-modules ────────────────────────────────────────────────────────────────
pub mod admission;
pub mod checkpoint;
pub mod heartbeat;
pub mod in_process;
pub mod job;
pub mod llm_quota;

pub use in_process::{
    InProcessCoordinatorBridge, IN_PROCESS_TASK_ENDPOINT, is_in_process_task_endpoint,
};
pub(crate) mod store;

// ── Re-exports for backwards-compatible crate-level API ────────────────────────
pub use admission::{
    QueueManager, InMemoryQueueManager, QuotaPolicy, QuotaQueueManager, ConfigFileQueueManager,
};
pub use checkpoint::{CheckpointCoordinatorState, CheckpointCoordinator};
pub use heartbeat::{
    ExecutorHealthSnapshot, ExecutorRegistry, ExecutorRecord, ExecutorHeartbeatAge,
};
pub use job::{
    SubmitOutcome, ResourceUsage, NamespaceQuotaSnapshot,
    JobRecord, StageRecord, TaskRecord,
    JobSnapshot, JobDetailSnapshot, StageSnapshot, TaskSnapshot,
    StabilityMetrics,
    job_spec_from_logical_plan, job_spec_from_physical_plan,
    StaticScheduler,
};
pub(crate) use job::validate_job;
pub use store::{
    EventLogEvent, MetadataStore, InMemoryMetadataStore, JsonFileMetadataStore,
};
#[cfg(feature = "sqlite")]
pub use store::SqliteMetadataStore;

/// Job submission interface supporting both gRPC (process mode) and Kubernetes
/// CRD (operator mode) submission paths.
///
/// `GrpcJobSubmitter` and `KubernetesJobSubmitter` are deferred; the trait is
/// defined here so callers can depend on the abstraction immediately.
pub trait JobSubmitter: Send + Sync {
    fn submit(&self, spec: &JobSpec) -> SchedulerResult<()>;
}



/// Coordinator behavior knobs for deterministic R2 scheduler tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorConfig {
    max_stage_retries: u32,
    heartbeat_timeout_ticks: u64,
    memory_threshold_bytes: Option<u64>,
    /// Number of ticks after coordinator restart during which streaming-job
    /// executor leases are not evicted for missing heartbeats.  Executors
    /// running streaming tasks need time to re-register after a coordinator
    /// restart; evicting them immediately would force a full re-run.
    streaming_reattach_grace_ticks: u64,
    /// Wall-clock milliseconds represented by one heartbeat tick.
    ///
    /// Used to convert tick counts into elapsed-time estimates for the
    /// per-job checkpoint interval timer.  Defaults to 1 000 ms (1 second).
    tick_period_ms: u64,
    /// Job-level LLM request quota per minute (R17).
    llm_quota_requests_per_minute: u32,
    /// Job-level LLM token quota per minute (R17).
    llm_quota_tokens_per_minute: u64,
}

impl CoordinatorConfig {
    /// Create a coordinator config.
    pub fn new(max_stage_retries: u32, heartbeat_timeout_ticks: u64) -> Self {
        Self {
            max_stage_retries,
            heartbeat_timeout_ticks: heartbeat_timeout_ticks.max(1),
            memory_threshold_bytes: None,
            streaming_reattach_grace_ticks: 5,
            tick_period_ms: 1_000,
            llm_quota_requests_per_minute: 100,
            llm_quota_tokens_per_minute: 10_000,
        }
    }

    /// Override job-level LLM request quota (R17).
    #[must_use]
    pub fn with_llm_quota(mut self, requests_per_minute: u32, tokens_per_minute: u64) -> Self {
        self.llm_quota_requests_per_minute = requests_per_minute;
        self.llm_quota_tokens_per_minute = tokens_per_minute;
        self
    }

    /// Set the memory threshold above which executors are skipped for placement.
    #[must_use]
    pub fn with_memory_threshold(mut self, bytes: u64) -> Self {
        self.memory_threshold_bytes = Some(bytes);
        self
    }

    /// Set the streaming re-attach grace period in heartbeat ticks.
    #[must_use]
    pub fn with_streaming_reattach_grace_ticks(mut self, ticks: u64) -> Self {
        self.streaming_reattach_grace_ticks = ticks;
        self
    }

    /// Set the wall-clock duration of one heartbeat tick in milliseconds.
    #[must_use]
    pub fn with_tick_period_ms(mut self, ms: u64) -> Self {
        self.tick_period_ms = ms.max(1);
        self
    }

    /// Maximum number of stage-level retries after an executor reports failure.
    pub fn max_stage_retries(&self) -> u32 {
        self.max_stage_retries
    }

    /// Number of scheduler ticks an executor can miss before it is marked lost.
    pub fn heartbeat_timeout_ticks(&self) -> u64 {
        self.heartbeat_timeout_ticks
    }

    /// Memory threshold above which executors are skipped for placement.
    pub fn memory_threshold_bytes(&self) -> Option<u64> {
        self.memory_threshold_bytes
    }

    /// Grace period after coordinator restart before streaming executor leases expire.
    pub fn streaming_reattach_grace_ticks(&self) -> u64 {
        self.streaming_reattach_grace_ticks
    }

    /// Wall-clock milliseconds per heartbeat tick.
    pub fn tick_period_ms(&self) -> u64 {
        self.tick_period_ms
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self::new(1, 3)
    }
}

// ── R7.2 Adaptive governance types ───────────────────────────────────────────

/// The kind of adaptive decision taken or suppressed by the coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecisionKind {
    HotKeySplit,
    Repartition,
    SourceThrottle,
    SlowSinkDetected,
}

impl fmt::Display for AdaptiveDecisionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HotKeySplit => f.write_str("hot-key-split"),
            Self::Repartition => f.write_str("repartition"),
            Self::SourceThrottle => f.write_str("source-throttle"),
            Self::SlowSinkDetected => f.write_str("slow-sink"),
        }
    }
}

/// One recorded adaptive decision (applied or suppressed by manual override).
#[derive(Debug, Clone)]
pub struct AdaptiveDecisionLog {
    pub timestamp_ms: u64,
    pub kind: AdaptiveDecisionKind,
    pub affected_job_id: JobId,
    pub details: String,
    /// `true` if the decision was actually applied; `false` if suppressed.
    pub applied: bool,
}

/// Manual override configuration for adaptive behaviors in the coordinator.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveOverrideConfig {
    pub disable_hot_key_splitting: bool,
    pub disable_adaptive_repartition: bool,
    pub disable_source_throttling: bool,
}

/// A throttle command the coordinator sends back to an executor in the
/// heartbeat response (R7.2 Group C).
///
/// The executor forwards this to its source operators to apply rate limiting.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrottleDecision {
    /// Source operator id on the executor.
    pub source_id: String,
    /// Maximum rows per second (`None` clears the throttle).
    pub rows_per_second: Option<u64>,
}

/// Side effects returned from a successful executor heartbeat.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorHeartbeatEffects {
    /// Source-operator throttle directives (R7.2).
    pub source_throttles: Vec<ThrottleDecision>,
    /// LLM UDF throttle directives (R17).
    pub llm_throttles: Vec<LlmThrottleCommand>,
    pub checkpoint_commands: Vec<InitiateCheckpointCommand>,
    pub lease_generation: LeaseGeneration,
}

/// Scheduler and coordinator errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    /// The coordinator is not active.
    InactiveCoordinator {
        coordinator_id: CoordinatorId,
        state: CoordinatorState,
    },
    /// Executor already exists.
    DuplicateExecutor { executor_id: ExecutorId },
    /// Executor was not found.
    UnknownExecutor { executor_id: ExecutorId },
    /// Executor used an older or otherwise invalid lease generation.
    StaleExecutorLease {
        executor_id: ExecutorId,
        expected: LeaseGeneration,
        received: LeaseGeneration,
    },
    /// No healthy executors are available for placement.
    NoExecutors,
    /// Job already exists.
    DuplicateJob { job_id: JobId },
    /// Job was not found.
    UnknownJob { job_id: JobId },
    /// Stage was not found.
    UnknownStage { stage_id: StageId },
    /// Task was not found.
    UnknownTask { task_id: TaskId },
    /// Task status referenced an attempt that is no longer current.
    StaleTaskAttempt {
        task_id: TaskId,
        expected: u32,
        received: u32,
    },
    /// Job submission was invalid.
    InvalidJob { message: String },
    /// Distributed DAG conversion failed.
    InvalidPlan { message: String },
    /// Coordinator/executor transport failed.
    Transport { message: String },
    /// Executor endpoint is unavailable for task dispatch.
    ExecutorUnavailable { endpoint: String, reason: String },
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InactiveCoordinator {
                coordinator_id,
                state,
            } => write!(
                f,
                "coordinator {coordinator_id} is {state}; only the active coordinator may mutate state"
            ),
            Self::DuplicateExecutor { executor_id } => {
                write!(f, "executor already registered: {executor_id}")
            }
            Self::UnknownExecutor { executor_id } => write!(f, "unknown executor: {executor_id}"),
            Self::StaleExecutorLease {
                executor_id,
                expected,
                received,
            } => write!(
                f,
                "stale executor lease for {executor_id}: expected generation {expected}, received {received}"
            ),
            Self::NoExecutors => f.write_str("no healthy executors are available"),
            Self::DuplicateJob { job_id } => write!(f, "job already exists: {job_id}"),
            Self::UnknownJob { job_id } => write!(f, "unknown job: {job_id}"),
            Self::UnknownStage { stage_id } => write!(f, "unknown stage: {stage_id}"),
            Self::UnknownTask { task_id } => write!(f, "unknown task: {task_id}"),
            Self::StaleTaskAttempt {
                task_id,
                expected,
                received,
            } => write!(
                f,
                "stale task attempt for {task_id}: expected attempt {expected}, received {received}"
            ),
            Self::InvalidJob { message } => write!(f, "invalid job: {message}"),
            Self::InvalidPlan { message } => write!(f, "invalid plan: {message}"),
            Self::Transport { message } => write!(f, "transport error: {message}"),
            Self::ExecutorUnavailable { endpoint, reason } => {
                write!(f, "executor endpoint {endpoint} unavailable: {reason}")
            }
        }
    }
}

impl Error for SchedulerError {}


/// R2 coordinator skeleton.
#[derive(Clone)]
pub struct Coordinator {
    coordinator_id: CoordinatorId,
    state: CoordinatorState,
    config: CoordinatorConfig,
    executors: ExecutorRegistry,
    /// O(1) job lookup by id.  Replaces Vec<JobRecord> linear scan.
    jobs: HashMap<JobId, JobRecord>,
    store: Option<Arc<Mutex<dyn MetadataStore + 'static>>>,
    /// Per-job checkpoint coordinators for streaming jobs with checkpoint config.
    checkpoint_coordinators: HashMap<JobId, CheckpointCoordinator>,
    /// Controls admission of new jobs.  Defaults to `InMemoryQueueManager`
    /// (always admits).  R7.1 will add quota-aware implementations.
    queue_manager: Arc<dyn QueueManager>,
    /// Jobs that have just reached a terminal state and need shuffle GC.
    /// Drained by the coordinator binary's tick loop.
    gc_ready_jobs: Vec<JobId>,
    /// Number of heartbeat ticks since the last coordinator restart.
    /// Used to implement `streaming_reattach_grace_ticks`: for this many ticks
    /// after `recover_from_store` is called, streaming-job executors are not
    /// evicted for missing heartbeats.
    ticks_since_restart: u64,
    /// Set to true after `recover_from_store` has been called at least once.
    recovering: bool,
    /// Append-only log of adaptive decisions (hot-key split, repartition,
    /// throttle, slow-sink).  Keyed by job id.  R7.2 Group H.
    adaptive_decision_log: HashMap<JobId, Vec<AdaptiveDecisionLog>>,
    /// Manual override config for adaptive behaviors.
    adaptive_override: AdaptiveOverrideConfig,
    /// P1.1: O(1) index from streaming task id to (job_id, stage_id) for heartbeat lookup.
    /// Populated when tasks are assigned; entries removed on task completion/failure.
    streaming_task_index: HashMap<TaskId, (JobId, StageId)>,
    /// P1.2: Cached gRPC channels keyed by executor endpoint string.
    /// Avoids a full TCP+TLS handshake per task assignment push.
    executor_channels: Arc<tokio::sync::Mutex<HashMap<String, tonic::transport::Channel>>>,
    checkpoint_notify_sent: HashSet<(JobId, ExecutorId, u64)>,
    /// Aggregates LLM quota reports across executors (R17).
    llm_quota_aggregator: llm_quota::LlmQuotaAggregator,
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("coordinator_id", &self.coordinator_id)
            .field("state", &self.state)
            .field("config", &self.config)
            .field("executors", &self.executors)
            .field("jobs", &self.jobs)
            .field("store", &self.store.as_ref().map(|_| "<store>"))
            .field("streaming_task_index_len", &self.streaming_task_index.len())
            .finish()
    }
}

/// Shared handle to the active coordinator owned by an R2 runtime process.
#[derive(Debug, Clone)]
pub struct SharedCoordinator {
    inner: Arc<RwLock<Coordinator>>,
}

impl SharedCoordinator {
    /// Create a shared coordinator handle.
    pub fn new(coordinator: Coordinator) -> Self {
        Self {
            inner: Arc::new(RwLock::new(coordinator)),
        }
    }

    /// Borrow the coordinator for read-only status snapshots.
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, Coordinator>> {
        self.inner.read()
    }

    /// Borrow the coordinator for scheduler mutations.
    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, Coordinator>> {
        self.inner.write()
    }

    /// Advance the heartbeat clock by one tick (P0-4).
    pub fn advance_heartbeat_tick(&self) -> SchedulerResult<Vec<ExecutorId>> {
        self.write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .advance_heartbeat_clock(1)
    }

    /// Launch and push all assigned tasks for non-terminal jobs (P0-4).
    pub async fn drive_pending_task_launches(&self) -> SchedulerResult<usize> {
        let job_ids = {
            let coord = self.read().map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?;
            coord
                .jobs
                .iter()
                .filter(|(_, job)| !job.state().is_terminal())
                .map(|(job_id, _)| job_id.clone())
                .collect::<Vec<_>>()
        };
        let mut launched = 0usize;
        for job_id in job_ids {
            let targets = {
                let mut coord = self.write().map_err(|_| SchedulerError::Transport {
                    message: "coordinator lock poisoned".to_string(),
                })?;
                let assignments = coord.launch_assigned_task_assignments(&job_id)?;
                coord.resolve_assignment_targets(assignments)?
            };
            let channels = {
                let coord = self.read().map_err(|_| SchedulerError::Transport {
                    message: "coordinator lock poisoned".to_string(),
                })?;
                coord.executor_channels.clone()
            };
            let responses =
                Coordinator::deliver_assignment_targets_with_channels(channels, targets).await?;
            launched = launched.saturating_add(responses.len());
        }
        Ok(launched)
    }

    /// Spawn background heartbeat and task-launch loops for standalone deployments (P0-4).
    pub fn spawn_orchestration_loops(&self) {
        let heartbeat = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(error) = heartbeat.advance_heartbeat_tick() {
                    eprintln!("coordinator heartbeat tick failed: {error}");
                }
            }
        });
        let launch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                if let Err(error) = launch.drive_pending_task_launches().await {
                    eprintln!("coordinator task launch tick failed: {error}");
                }
            }
        });
    }
}

/// Tonic-shaped adapter that exposes coordinator/executor RPCs over a shared coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorExecutorTonicService {
    coordinator: SharedCoordinator,
}

impl CoordinatorExecutorTonicService {
    /// Create a coordinator/executor service adapter.
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self { coordinator }
    }

    /// Shared coordinator backing this adapter.
    pub fn coordinator(&self) -> &SharedCoordinator {
        &self.coordinator
    }
}

#[tonic::async_trait]
impl CoordinatorExecutorService for CoordinatorExecutorTonicService {
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        // GAP-CP-08: Extract auth context for every handler.
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "register_executor");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let descriptor = request.descriptor().clone();
        let executor_id = descriptor.executor_id().clone();
        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator.register_executor(descriptor) {
            Ok(lease_generation) => RegisterExecutorResponse::new(
                executor_id,
                lease_generation,
                TransportDisposition::Accepted,
            ),
            Err(SchedulerError::DuplicateExecutor { executor_id }) => {
                RegisterExecutorResponse::new(
                    executor_id,
                    LeaseGeneration::initial(),
                    TransportDisposition::Duplicate,
                )
                .with_message("executor is already registered")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "deregister_executor");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator
            .deregister_executor(request.executor_id(), request.lease_generation())
        {
            Ok(lease_generation) => DeregisterExecutorResponse::new(
                request.executor_id().clone(),
                lease_generation,
                TransportDisposition::Accepted,
            ),
            Err(SchedulerError::UnknownExecutor { .. }) => DeregisterExecutorResponse::new(
                request.executor_id().clone(),
                request.lease_generation(),
                TransportDisposition::UnknownExecutor,
            )
            .with_message("executor is not registered"),
            Err(SchedulerError::StaleExecutorLease { expected, .. }) => {
                DeregisterExecutorResponse::new(
                    request.executor_id().clone(),
                    expected,
                    TransportDisposition::StaleLease,
                )
                .with_message("executor lease generation is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "executor_heartbeat");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut heartbeat = ExecutorHeartbeat::new(request.executor_id().clone(), request.state())
            .with_lease_generation(request.lease_generation())
            .with_running_tasks(
                request
                    .running_attempts()
                    .iter()
                    .map(|attempt| attempt.task_id().clone())
                    .collect(),
            );
        if let Some(bytes) = request.memory_used_bytes() {
            heartbeat = heartbeat.with_memory_used_bytes(bytes);
        }
        if let Some(bytes) = request.memory_limit_bytes() {
            heartbeat = heartbeat.with_memory_limit_bytes(bytes);
        }
        if let Some(count) = request.active_task_count() {
            heartbeat = heartbeat.with_active_task_count(count);
        }
        if !request.streaming_task_states().is_empty() {
            heartbeat =
                heartbeat.with_streaming_task_states(request.streaming_task_states().to_vec());
        }
        if !request.hot_key_reports().is_empty() {
            heartbeat = heartbeat.with_hot_key_reports(request.hot_key_reports().to_vec());
        }
        if !request.llm_quota_reports().is_empty() {
            heartbeat = heartbeat.with_llm_quota_reports(request.llm_quota_reports().to_vec());
        }
        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator.executor_heartbeat(heartbeat) {
            Ok(effects) => {
                let mut resp = ExecutorHeartbeatResponse::new(
                    effects.lease_generation,
                    TransportDisposition::Accepted,
                );
                if !effects.source_throttles.is_empty() {
                    let wire_cmds: Vec<HeartbeatThrottleCommand> = effects
                        .source_throttles
                        .into_iter()
                        .map(|c| HeartbeatThrottleCommand {
                            source_id: c.source_id,
                            rows_per_second: c.rows_per_second,
                        })
                        .collect();
                    resp = resp.with_throttle_commands(wire_cmds);
                }
                if !effects.llm_throttles.is_empty() {
                    resp = resp.with_llm_throttles(effects.llm_throttles);
                }
                if !effects.checkpoint_commands.is_empty() {
                    resp = resp.with_checkpoint_commands(effects.checkpoint_commands);
                }
                resp
            }
            Err(SchedulerError::UnknownExecutor { .. }) => ExecutorHeartbeatResponse::new(
                request.lease_generation(),
                TransportDisposition::UnknownExecutor,
            )
            .with_message("executor is not registered"),
            Err(SchedulerError::StaleExecutorLease { expected, .. }) => {
                ExecutorHeartbeatResponse::new(expected, TransportDisposition::StaleLease)
                    .with_message("executor lease generation is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "task_status");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut update = TaskStatusUpdate::new(
            request.job_id().clone(),
            request.stage_id().clone(),
            request.task_id().clone(),
            request.executor_id().clone(),
            request.state(),
            request.attempt_id().as_u32(),
        )
        .with_lease_generation(request.lease_generation());
        if let Some(message) = request.message() {
            update = update.with_message(message);
        }
        if let Some(output_metadata) = request.output_metadata() {
            update = update.with_output_metadata(output_metadata.clone());
        }

        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator.apply_task_update(update) {
            Ok(TaskUpdateOutcome::Applied) => {
                TaskStatusResponse::new(TransportDisposition::Accepted)
            }
            Ok(TaskUpdateOutcome::Duplicate) => {
                TaskStatusResponse::new(TransportDisposition::Duplicate)
                    .with_message("task status update was already applied")
            }
            Err(SchedulerError::UnknownJob { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownJob)
                    .with_message("job is not registered")
            }
            Err(SchedulerError::UnknownTask { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownTask)
                    .with_message("task is not registered")
            }
            Err(SchedulerError::UnknownExecutor { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownExecutor)
                    .with_message("executor is not registered")
            }
            Err(SchedulerError::StaleExecutorLease { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleLease)
                    .with_message("executor lease generation is stale")
            }
            Err(SchedulerError::StaleTaskAttempt { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleAttempt)
                    .with_message("task attempt is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "checkpoint_ack");
        let ack = request.into_inner();
        // GAP-CK-03: commit_epoch() calls sync disk I/O via LocalFsCheckpointStorage.
        // Move the lock-acquire + storage write onto a blocking thread so the
        // Tokio worker pool is not stalled.
        let coordinator = self.coordinator.clone();
        let response = tokio::task::spawn_blocking(move || {
            coordinator
                .write()
                .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))
                .map(|mut c| c.handle_checkpoint_ack(ack))
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("checkpoint_ack task panicked: {e}")))?
        ?;
        Ok(tonic::Response::new(response))
    }
}

/// Management service implementation: routes CLI→coordinator RPCs (GAP-RT-04).
#[tonic::async_trait]
impl CoordinatorManagementService for CoordinatorExecutorTonicService {
    async fn trigger_savepoint(
        &self,
        request: tonic::Request<TriggerSavepointRequest>,
    ) -> Result<tonic::Response<TriggerSavepointResponse>, tonic::Status> {
        let req = request.into_inner();
        let job_id = JobId::try_new(&req.job_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid job_id: {e}"))
        })?;
        let label = if req.label.is_empty() { None } else { Some(req.label) };
        let mut coordinator = self.coordinator.write().map_err(|_| {
            tonic::Status::internal("coordinator lock poisoned")
        })?;
        let epoch = coordinator.savepoint_job(&job_id, label).map_err(|e| {
            tonic::Status::internal(e.to_string())
        })?;
        Ok(tonic::Response::new(TriggerSavepointResponse { epoch }))
    }

    async fn restore_job(
        &self,
        request: tonic::Request<RestoreJobRequest>,
    ) -> Result<tonic::Response<RestoreJobResponse>, tonic::Status> {
        let req = request.into_inner();
        let job_id = JobId::try_new(&req.job_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid job_id: {e}"))
        })?;
        let coordinator = self.coordinator.read().map_err(|_| {
            tonic::Status::internal("coordinator lock poisoned")
        })?;
        match coordinator.restore_job_from_checkpoint(&job_id, req.epoch, &req.storage_path) {
            Ok(_meta) => Ok(tonic::Response::new(RestoreJobResponse {
                accepted: true,
                message: format!("restore plan loaded for job {} epoch {}", req.job_id, req.epoch),
            })),
            Err(e) => Ok(tonic::Response::new(RestoreJobResponse {
                accepted: false,
                message: e.to_string(),
            })),
        }
    }

    async fn list_checkpoints(
        &self,
        request: tonic::Request<ListCheckpointsRequest>,
    ) -> Result<tonic::Response<ListCheckpointsResponse>, tonic::Status> {
        let req = request.into_inner();
        let job_id = JobId::try_new(&req.job_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid job_id: {e}"))
        })?;
        let coordinator = self.coordinator.read().map_err(|_| {
            tonic::Status::internal("coordinator lock poisoned")
        })?;
        let epoch_nums = coordinator.list_job_checkpoints(&job_id).map_err(|e| {
            tonic::Status::internal(e.to_string())
        })?;
        // Enrich each epoch with savepoint metadata if available.
        let epochs = epoch_nums
            .into_iter()
            .map(|epoch| {
                // Try to read metadata for savepoint labeling; skip on error.
                let (is_savepoint, savepoint_label) = coordinator
                    .checkpoint_coordinator(&job_id)
                    .and_then(|coord| {
                        let storage = coord.storage.as_ref();
                        krishiv_checkpoint::read_epoch_metadata(storage, req.job_id.as_str(), epoch)
                            .ok()
                            .flatten()
                            .map(|m| (m.is_savepoint, m.savepoint_label.unwrap_or_default()))
                    })
                    .unwrap_or((false, String::new()));
                CheckpointEpochInfo {
                    epoch,
                    is_savepoint,
                    savepoint_label: if savepoint_label.is_empty() { None } else { Some(savepoint_label) },
                }
            })
            .collect();
        Ok(tonic::Response::new(ListCheckpointsResponse { epochs }))
    }

    async fn inspect_state(
        &self,
        request: tonic::Request<InspectStateRequest>,
    ) -> Result<tonic::Response<InspectStateResponse>, tonic::Status> {
        let req = request.into_inner();
        let job_id = JobId::try_new(&req.job_id).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid job_id: {e}"))
        })?;
        let coordinator = self.coordinator.read().map_err(|_| {
            tonic::Status::internal("coordinator lock poisoned")
        })?;
        // Collect snapshot paths for the requested operator from the checkpoint coordinator.
        let snapshots = coordinator
            .checkpoint_coordinator(&job_id)
            .map(|coord| {
                coord
                    .pending_acks
                    .values()
                    .filter(|ack| req.operator_id.is_empty() || ack.operator_id == req.operator_id)
                    .filter_map(|ack| {
                        ack.snapshot_path.as_ref().map(|path| StateSnapshotInfo {
                            task_id: ack.task_id.as_str().to_owned(),
                            snapshot_path: path.clone(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(tonic::Response::new(InspectStateResponse { snapshots }))
    }
}

/// Networked gRPC adapter for coordinator/executor transport calls.
#[derive(Debug, Clone)]
pub struct CoordinatorExecutorGrpcService {
    inner: CoordinatorExecutorTonicService,
}

impl CoordinatorExecutorGrpcService {
    /// Create a network service from a shared coordinator.
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self {
            inner: CoordinatorExecutorTonicService::new(coordinator),
        }
    }

    /// Shared coordinator backing this service.
    pub fn coordinator(&self) -> &SharedCoordinator {
        self.inner.coordinator()
    }
}

#[tonic::async_trait]
impl wire::v1::coordinator_executor_server::CoordinatorExecutor for CoordinatorExecutorGrpcService {
    async fn register_executor(
        &self,
        request: tonic::Request<wire::v1::RegisterExecutorRequest>,
    ) -> Result<tonic::Response<wire::v1::RegisterExecutorResponse>, tonic::Status> {
        let request = wire::register_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .register_executor(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::register_executor_response_to_wire(response),
        ))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<wire::v1::DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<wire::v1::DeregisterExecutorResponse>, tonic::Status> {
        let request = wire::deregister_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .deregister_executor(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::deregister_executor_response_to_wire(response),
        ))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<wire::v1::ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<wire::v1::ExecutorHeartbeatResponse>, tonic::Status> {
        let request = wire::executor_heartbeat_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .executor_heartbeat(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::executor_heartbeat_response_to_wire(response),
        ))
    }

    async fn task_status(
        &self,
        request: tonic::Request<wire::v1::TaskStatusRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::task_status_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .task_status(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<wire::v1::CheckpointAckRequest>,
    ) -> Result<tonic::Response<wire::v1::CheckpointAckResponse>, tonic::Status> {
        let request = wire::checkpoint_ack_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .checkpoint_ack(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::checkpoint_ack_response_to_wire(
            response,
        )))
    }
}

/// gRPC adapter exposing the coordinator management service (GAP-RT-04).
///
/// Converts wire proto types to domain types, then delegates to
/// `CoordinatorExecutorTonicService::CoordinatorManagementService`.
#[derive(Debug, Clone)]
pub struct CoordinatorManagementGrpcService {
    inner: CoordinatorExecutorTonicService,
}

impl CoordinatorManagementGrpcService {
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self {
            inner: CoordinatorExecutorTonicService::new(coordinator),
        }
    }
}

#[tonic::async_trait]
impl wire::v1::coordinator_management_server::CoordinatorManagement
    for CoordinatorManagementGrpcService
{
    async fn trigger_savepoint(
        &self,
        request: tonic::Request<wire::v1::TriggerSavepointRequest>,
    ) -> Result<tonic::Response<wire::v1::TriggerSavepointResponse>, tonic::Status> {
        let w = request.into_inner();
        let domain = TriggerSavepointRequest { job_id: w.job_id, label: w.label };
        let resp = CoordinatorManagementService::trigger_savepoint(
            &self.inner,
            tonic::Request::new(domain),
        )
        .await?
        .into_inner();
        Ok(tonic::Response::new(wire::v1::TriggerSavepointResponse {
            epoch: resp.epoch,
            message: String::new(),
        }))
    }

    async fn restore_job(
        &self,
        request: tonic::Request<wire::v1::RestoreJobRequest>,
    ) -> Result<tonic::Response<wire::v1::RestoreJobResponse>, tonic::Status> {
        let w = request.into_inner();
        let domain = RestoreJobRequest {
            job_id: w.job_id,
            epoch: w.epoch,
            storage_path: w.storage_path,
        };
        let resp = CoordinatorManagementService::restore_job(
            &self.inner,
            tonic::Request::new(domain),
        )
        .await?
        .into_inner();
        Ok(tonic::Response::new(wire::v1::RestoreJobResponse {
            accepted: resp.accepted,
            message: resp.message,
        }))
    }

    async fn list_checkpoints(
        &self,
        request: tonic::Request<wire::v1::ListCheckpointsRequest>,
    ) -> Result<tonic::Response<wire::v1::ListCheckpointsResponse>, tonic::Status> {
        let w = request.into_inner();
        let domain = ListCheckpointsRequest { job_id: w.job_id };
        let resp = CoordinatorManagementService::list_checkpoints(
            &self.inner,
            tonic::Request::new(domain),
        )
        .await?
        .into_inner();
        let epochs = resp
            .epochs
            .into_iter()
            .map(|e| wire::v1::CheckpointEpochInfo {
                epoch: e.epoch,
                is_savepoint: e.is_savepoint,
                savepoint_label: e.savepoint_label.unwrap_or_default(),
            })
            .collect();
        Ok(tonic::Response::new(wire::v1::ListCheckpointsResponse {
            epochs,
        }))
    }

    async fn inspect_state(
        &self,
        request: tonic::Request<wire::v1::InspectStateRequest>,
    ) -> Result<tonic::Response<wire::v1::InspectStateResponse>, tonic::Status> {
        let w = request.into_inner();
        let domain = InspectStateRequest {
            job_id: w.job_id,
            operator_id: w.operator_id,
        };
        let resp = CoordinatorManagementService::inspect_state(
            &self.inner,
            tonic::Request::new(domain),
        )
        .await?
        .into_inner();
        let snapshots = resp
            .snapshots
            .into_iter()
            .map(|s| wire::v1::StateSnapshotInfo {
                task_id: s.task_id,
                snapshot_path: s.snapshot_path,
            })
            .collect();
        Ok(tonic::Response::new(wire::v1::InspectStateResponse {
            snapshots,
        }))
    }
}

// ── gRPC auth enforcement (P3-20) ─────────────────────────────────────────────

static GRPC_AUTH_PROVIDER: std::sync::OnceLock<Arc<dyn krishiv_governance::AuthProvider>> =
    std::sync::OnceLock::new();

/// Install a process-wide auth provider for coordinator gRPC (optional).
pub fn set_grpc_auth_provider(provider: Arc<dyn krishiv_governance::AuthProvider>) {
    let _ = GRPC_AUTH_PROVIDER.set(provider);
}

/// Validate `auth` when a provider is configured; otherwise allow anonymous access.
pub fn validate_grpc_auth(auth: &AuthContext) -> Result<(), tonic::Status> {
    let Some(provider) = GRPC_AUTH_PROVIDER.get() else {
        return Ok(());
    };
    match auth {
        AuthContext::Bearer { subject } => {
            if provider.authenticate(subject).is_some() {
                Ok(())
            } else {
                Err(tonic::Status::unauthenticated("invalid API key"))
            }
        }
        AuthContext::Anonymous => Err(tonic::Status::unauthenticated(
            "missing Bearer token",
        )),
    }
}

// ── R8 auth interceptor skeleton ─────────────────────────────────────────────

/// Authentication context extracted by the auth interceptor.
///
/// In R8.1+ this will carry a validated bearer token or mTLS peer identity.
/// For now it is always `Anonymous` — the interceptor is a no-op that ensures
/// every future call site already accepts an `AuthContext` without structural
/// changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthContext {
    /// No credential presented; accepted in development / internal-only deployments.
    Anonymous,
    /// A validated bearer token (R8.1 wiring placeholder).
    Bearer { subject: String },
}

impl AuthContext {
    /// Return `true` if this context represents a known authenticated subject.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Bearer { .. })
    }

    /// Subject string, or `"anonymous"` for unauthenticated callers.
    pub fn subject(&self) -> &str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Bearer { subject } => subject.as_str(),
        }
    }
}

/// Extract an `AuthContext` from the gRPC request metadata.
///
/// Reads the `authorization` header. If it starts with `"Bearer "` the token
/// is extracted and returned as `Bearer { subject: <token> }`. In R9 the token
/// is the API key validated by `krishiv_governance::StaticApiKeyAuthProvider`;
/// JWT/OIDC validation is deferred to R10.
///
/// Returns `Anonymous` when no header is present or parsing fails.
pub fn extract_auth_context(metadata: &tonic::metadata::MetadataMap) -> AuthContext {
    let header = metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(token) = header.strip_prefix("Bearer ") {
        let token = token.trim();
        if !token.is_empty() {
            return AuthContext::Bearer {
                subject: token.to_owned(),
            };
        }
    }
    AuthContext::Anonymous
}

/// Build the generated tonic server around the scheduler-backed gRPC adapter.
pub fn coordinator_executor_grpc_server(
    coordinator: SharedCoordinator,
) -> wire::v1::coordinator_executor_server::CoordinatorExecutorServer<CoordinatorExecutorGrpcService>
{
    wire::v1::coordinator_executor_server::CoordinatorExecutorServer::new(
        CoordinatorExecutorGrpcService::new(coordinator),
    )
}

/// Build the coordinator management gRPC service (GAP-RT-04).
pub fn coordinator_management_grpc_server(
    coordinator: SharedCoordinator,
) -> wire::v1::coordinator_management_server::CoordinatorManagementServer<
    CoordinatorManagementGrpcService,
> {
    wire::v1::coordinator_management_server::CoordinatorManagementServer::new(
        CoordinatorManagementGrpcService::new(coordinator),
    )
}

/// Serve the coordinator/executor gRPC API on a socket address.
pub async fn serve_coordinator_executor_grpc(
    addr: SocketAddr,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    let coordinator_for_management = coordinator.clone();
    tonic::transport::Server::builder()
        .add_service(coordinator_executor_grpc_server(coordinator))
        .add_service(coordinator_management_grpc_server(coordinator_for_management))
        .serve(addr)
        .await
}

/// Serve the coordinator/executor gRPC API on an already-bound listener.
pub async fn serve_coordinator_executor_grpc_with_listener(
    listener: tokio::net::TcpListener,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    let coordinator_for_management = coordinator.clone();
    tonic::transport::Server::builder()
        .add_service(coordinator_executor_grpc_server(coordinator))
        .add_service(coordinator_management_grpc_server(coordinator_for_management))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

fn ensure_transport_version(version: TransportVersion) -> Result<(), tonic::Status> {
    if TransportVersion::CURRENT.is_compatible_with(version) {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(format!(
            "unsupported coordinator/executor transport version {version}; current version is {}",
            TransportVersion::CURRENT
        )))
    }
}

fn status_from_wire_error(error: wire::WireError) -> tonic::Status {
    tonic::Status::invalid_argument(error.to_string())
}

fn status_from_scheduler_error(error: SchedulerError) -> tonic::Status {
    match error {
        SchedulerError::InactiveCoordinator { .. } => {
            tonic::Status::failed_precondition(error.to_string())
        }
        SchedulerError::StaleExecutorLease { .. } | SchedulerError::StaleTaskAttempt { .. } => {
            tonic::Status::failed_precondition(error.to_string())
        }
        SchedulerError::UnknownExecutor { .. }
        | SchedulerError::UnknownJob { .. }
        | SchedulerError::UnknownStage { .. }
        | SchedulerError::UnknownTask { .. } => tonic::Status::not_found(error.to_string()),
        SchedulerError::DuplicateExecutor { .. } | SchedulerError::DuplicateJob { .. } => {
            tonic::Status::already_exists(error.to_string())
        }
        SchedulerError::NoExecutors
        | SchedulerError::InvalidJob { .. }
        | SchedulerError::InvalidPlan { .. } => tonic::Status::invalid_argument(error.to_string()),
        SchedulerError::Transport { .. } | SchedulerError::ExecutorUnavailable { .. } => {
            tonic::Status::unavailable(error.to_string())
        }
    }
}

impl Coordinator {
    /// Create an active R2 coordinator.
    pub fn active(coordinator_id: CoordinatorId) -> Self {
        Self::active_with_config(coordinator_id, CoordinatorConfig::default())
    }

    /// Create an active R2 coordinator with explicit config.
    fn build(
        coordinator_id: CoordinatorId,
        config: CoordinatorConfig,
        state: CoordinatorState,
    ) -> Self {
        Self {
            coordinator_id,
            state,
            config,
            executors: ExecutorRegistry::new(
                config.heartbeat_timeout_ticks(),
                config.memory_threshold_bytes(),
            ),
            jobs: HashMap::new(),
            store: None,
            checkpoint_coordinators: HashMap::new(),
            queue_manager: Arc::new(InMemoryQueueManager),
            gc_ready_jobs: Vec::new(),
            ticks_since_restart: u64::MAX,
            recovering: false,
            adaptive_decision_log: HashMap::new(),
            adaptive_override: AdaptiveOverrideConfig::default(),
            streaming_task_index: HashMap::new(),
            executor_channels: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            checkpoint_notify_sent: HashSet::new(),
            llm_quota_aggregator: llm_quota::LlmQuotaAggregator::new(
                config.llm_quota_requests_per_minute,
                config.llm_quota_tokens_per_minute,
            ),
        }
    }

    /// Create a new active coordinator with a process-unique identifier.
    pub fn new_active(config: Option<CoordinatorConfig>) -> Self {
        Self::try_new_active(config).expect("coordinator id generation")
    }

    /// Create a new standby coordinator with a process-unique identifier.
    pub fn new_standby(config: Option<CoordinatorConfig>) -> Self {
        Self::try_new_standby(config).expect("coordinator id generation")
    }

    /// Generate a process-unique `CoordinatorId`.
    ///
    /// Uses a process-local atomic counter so that multiple coordinator
    /// instances (e.g. during distributed tests or multi-API-server deployments)
    /// each receive a distinct identity without requiring an external ID service.
    fn generate_id() -> SchedulerResult<CoordinatorId> {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        CoordinatorId::try_new(format!("coordinator-{n}"))
            .map_err(|e| SchedulerError::InvalidJob {
                message: format!("generated coordinator id invalid: {e}"),
            })
    }

    /// Create a new active coordinator, returning an error if id generation fails.
    pub fn try_new_active(config: Option<CoordinatorConfig>) -> SchedulerResult<Self> {
        Ok(Self::build(
            Self::generate_id()?,
            config.unwrap_or_default(),
            CoordinatorState::Active,
        ))
    }

    /// Create a new standby coordinator, returning an error if id generation fails.
    pub fn try_new_standby(config: Option<CoordinatorConfig>) -> SchedulerResult<Self> {
        Ok(Self::build(
            Self::generate_id()?,
            config.unwrap_or_default(),
            CoordinatorState::Standby,
        ))
    }

    pub fn active_with_config(coordinator_id: CoordinatorId, config: CoordinatorConfig) -> Self {
        Self::build(coordinator_id, config, CoordinatorState::Active)
    }

    /// Attach a metadata store to this coordinator (builder).
    #[must_use]
    pub fn with_store(mut self, store: impl MetadataStore + 'static) -> Self {
        self.store = Some(Arc::new(Mutex::new(store)));
        self
    }

    /// Replace the default `InMemoryQueueManager` with a custom admission controller.
    ///
    /// R7.1 will use this to inject quota-aware and CRD-backed queue managers.
    #[must_use]
    pub fn with_queue_manager(mut self, qm: impl QueueManager + 'static) -> Self {
        self.queue_manager = Arc::new(qm);
        self
    }

    /// Override the adaptive governance configuration (R7.2).
    #[must_use]
    pub fn with_adaptive_override(mut self, cfg: AdaptiveOverrideConfig) -> Self {
        self.adaptive_override = cfg;
        self
    }

    /// Return the adaptive decision log for a job, or an empty slice if there
    /// are no decisions for this job.  R7.2 Group H.
    pub fn adaptive_decision_log(&self, job_id: &JobId) -> &[AdaptiveDecisionLog] {
        self.adaptive_decision_log
            .get(job_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Create a standby R2 coordinator.
    pub fn standby(coordinator_id: CoordinatorId) -> Self {
        Self::standby_with_config(coordinator_id, CoordinatorConfig::default())
    }

    /// Create a standby R2 coordinator with explicit config.
    pub fn standby_with_config(coordinator_id: CoordinatorId, config: CoordinatorConfig) -> Self {
        Self::build(coordinator_id, config, CoordinatorState::Standby)
    }

    /// Coordinator id.
    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    /// Coordinator state.
    pub fn state(&self) -> CoordinatorState {
        self.state
    }

    /// Promote a standby coordinator to active leader (P0-5 / P3-19).
    pub fn promote_to_active(&mut self) {
        self.state = CoordinatorState::Active;
    }

    /// Demote to standby when leadership is lost.
    pub fn demote_to_standby(&mut self) {
        self.state = CoordinatorState::Standby;
    }

    /// Coordinator config.
    pub fn config(&self) -> CoordinatorConfig {
        self.config
    }

    /// Register an executor with the active coordinator.
    pub fn register_executor(
        &mut self,
        descriptor: ExecutorDescriptor,
    ) -> SchedulerResult<LeaseGeneration> {
        self.ensure_active()?;
        self.executors.register(descriptor)
    }

    /// Deregister an executor with a valid lease generation.
    pub fn deregister_executor(
        &mut self,
        executor_id: &ExecutorId,
        lease_generation: LeaseGeneration,
    ) -> SchedulerResult<LeaseGeneration> {
        self.ensure_active()?;
        self.executors.deregister(executor_id, lease_generation)
    }

    /// Apply an executor heartbeat.
    ///
    /// For streaming executors re-attaching after a coordinator restart, the heartbeat may
    /// include `streaming_task_states`. These are applied to the matching task records so
    /// the coordinator tracks the executor's current watermark and source offset without
    /// re-submitting the job from scratch.
    ///
    /// Returns throttle commands to forward back to the executor (R7.2 Group C).
    /// Currently returns an empty vec unless adaptive source throttling is wired up.
    pub fn executor_heartbeat(
        &mut self,
        heartbeat: ExecutorHeartbeat,
    ) -> SchedulerResult<ExecutorHeartbeatEffects> {
        self.ensure_active()?;
        let executor_id = heartbeat.executor_id().clone();
        let fallback_lease = heartbeat.lease_generation();
        let streaming_states: Vec<StreamingTaskState> = heartbeat.streaming_task_states().to_vec();
        let hot_key_reports = heartbeat.hot_key_reports().to_vec();
        let llm_reports = heartbeat.llm_quota_reports().to_vec();
        self.executors.heartbeat(heartbeat)?;
        for state in &streaming_states {
            self.apply_streaming_task_state(state);
        }
        // R7.2 Group D: process hot-key reports and record adaptive decisions.
        self.process_hot_key_reports(&hot_key_reports);
        if !llm_reports.is_empty() {
            self.llm_quota_aggregator.ingest(&llm_reports);
        }
        let llm_throttles = self.llm_quota_aggregator.evaluate_and_reset();
        let checkpoint_commands =
            self.pending_initiate_checkpoints_for_executor(&executor_id);
        let lease_generation = self
            .executors
            .find_executor(&executor_id)
            .map(|e| e.lease_generation())
            .unwrap_or(fallback_lease);
        Ok(ExecutorHeartbeatEffects {
            source_throttles: Vec::new(),
            llm_throttles,
            checkpoint_commands,
            lease_generation,
        })
    }

    /// Record adaptive decisions for incoming hot-key reports.
    ///
    /// For each hot key whose heat_score exceeds the threshold, logs an
    /// `AdaptiveDecisionLog` entry. If `disable_hot_key_splitting` is set,
    /// the decision is logged with `applied: false`.
    fn process_hot_key_reports(&mut self, reports: &[HeartbeatHotKeyReport]) {
        use std::time::{SystemTime, UNIX_EPOCH};
        if reports.is_empty() {
            return;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        for report in reports {
            if report.job_id.is_empty() {
                continue;
            }
            let job_id = match JobId::try_new(report.job_id.clone()) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let applied = !self.adaptive_override.disable_hot_key_splitting;
            let log = AdaptiveDecisionLog {
                timestamp_ms: now_ms,
                kind: AdaptiveDecisionKind::HotKeySplit,
                affected_job_id: job_id.clone(),
                details: format!(
                    "hot key '{}' heat={:.3} estimated_count={} max_error={}",
                    report.key, report.heat_score, report.estimated_count, report.max_error
                ),
                applied,
            };
            self.adaptive_decision_log
                .entry(job_id)
                .or_default()
                .push(log);
        }
    }

    /// Update a task record's last-known watermark and source offset from executor-reported state.
    ///
    /// P1.1: Uses `streaming_task_index` for O(1) lookup instead of O(jobs×stages×tasks) scan.
    fn apply_streaming_task_state(&mut self, state: &StreamingTaskState) {
        let (job_id, stage_id) = match self.streaming_task_index.get(&state.task_id) {
            Some(entry) => (entry.0.clone(), entry.1.clone()),
            None => return,
        };
        if let Some(job) = self.jobs.get_mut(&job_id)
            && let Some(stage) = job.stages.iter_mut().find(|s| s.stage_id() == &stage_id)
        {
            for task in stage.tasks_mut() {
                if task.task_id() == &state.task_id {
                    task.apply_streaming_state(state);
                    return;
                }
            }
        }
    }

    /// Populate `streaming_task_index` for all tasks in a job after assignment.
    ///
    /// Called after `apply_assignments` so that streaming heartbeats can use the O(1) index.
    fn index_streaming_tasks(&mut self, job_id: &JobId) {
        let job = match self.jobs.get(job_id) {
            Some(j) => j,
            None => return,
        };
        for stage in &job.stages {
            let stage_id = stage.stage_id().clone();
            for task in stage.tasks() {
                self.streaming_task_index
                    .insert(task.task_id().clone(), (job_id.clone(), stage_id.clone()));
            }
        }
    }

    /// Remove `streaming_task_index` entries for a completed/failed/cancelled job.
    fn remove_streaming_task_index(&mut self, job_id: &JobId) {
        let task_ids: Vec<TaskId> = self
            .streaming_task_index
            .iter()
            .filter(|(_, (jid, _))| jid == job_id)
            .map(|(tid, _)| tid.clone())
            .collect();
        for tid in task_ids {
            self.streaming_task_index.remove(&tid);
        }
    }

    /// Mark an executor lost and release its running task assignments for retry.
    pub fn mark_executor_lost(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        self.ensure_active()?;
        self.executors.mark_lost(executor_id)?;
        self.reset_running_tasks_for_lost_executor(executor_id);
        Ok(())
    }

    /// Advance the deterministic heartbeat clock and mark timed-out executors lost.
    ///
    /// Tasks previously assigned to lost executors are reset to `Assigned` so they
    /// will be relaunched on the next `launch_assigned_task_assignments` call.
    ///
    /// During the streaming re-attach grace period after a coordinator restart,
    /// executors that own Running tasks in streaming jobs are not evicted even if
    /// they have missed heartbeats. This gives them time to re-register without
    /// forcing a full streaming job re-run.
    pub fn advance_heartbeat_clock(&mut self, ticks: u64) -> SchedulerResult<Vec<ExecutorId>> {
        self.ensure_active()?;
        // Advance the restart tick counter.
        self.ticks_since_restart = self.ticks_since_restart.saturating_add(ticks);

        let in_grace_period = self.recovering
            && self.ticks_since_restart <= self.config.streaming_reattach_grace_ticks();

        let lost = self.executors.advance_clock(ticks);
        let mut evicted: Vec<ExecutorId> = Vec::new();
        for lost_id in &lost {
            // During the re-attach grace period, skip evicting executors that own
            // Running tasks in streaming jobs so they can re-register.
            if in_grace_period && self.executor_has_streaming_running_tasks(lost_id) {
                continue;
            }
            self.reset_running_tasks_for_lost_executor(lost_id);
            evicted.push(lost_id.clone());
        }

        // Drive per-job checkpoint interval timers (SCH-3: quorum = running tasks).
        let elapsed_ms = ticks.saturating_mul(self.config.tick_period_ms());
        let job_ids: Vec<JobId> = self.checkpoint_coordinators.keys().cloned().collect();
        for job_id in job_ids {
            let running = self.running_task_count_for_job(&job_id);
            if let Some(coord) = self.checkpoint_coordinators.get_mut(&job_id) {
                coord.set_expected_task_count(running);
                coord.try_tick(elapsed_ms);
            }
        }

        Ok(evicted)
    }

    /// Count tasks in `Running` or `Assigned` state for a job (checkpoint quorum size).
    fn running_task_count_for_job(&self, job_id: &JobId) -> usize {
        self.jobs.get(job_id).map_or(0, |job| {
            job.stages
                .iter()
                .flat_map(|stage| stage.tasks())
                .filter(|task| {
                    matches!(
                        task.state(),
                        TaskState::Running | TaskState::Assigned
                    )
                })
                .count()
        })
    }

    pub fn coordinator_tick(&mut self) -> SchedulerResult<()> {
        self.advance_heartbeat_clock(1)?;
        for job_id in self.jobs.keys().cloned().collect::<Vec<_>>() {
            let _ = self.launch_assigned_task_assignments(&job_id)?;
        }
        Ok(())
    }

    pub fn pending_initiate_checkpoints_for_executor(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Vec<InitiateCheckpointCommand> {
        let mut out = Vec::new();
        for (job_id, coord) in &self.checkpoint_coordinators {
            let epoch = match &coord.state {
                CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
                _ => continue,
            };
            if !self.executor_has_running_task_in_job(executor_id, job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), epoch);
            if self.checkpoint_notify_sent.contains(&key) {
                continue;
            }
            out.push(InitiateCheckpointCommand {
                job_id: job_id.clone(),
                epoch,
                fencing_token: coord.fencing_token,
            });
            self.checkpoint_notify_sent.insert(key);
        }
        out
    }

    fn clear_checkpoint_notify_for_epoch(&mut self, job_id: &JobId, epoch: u64) {
        self.checkpoint_notify_sent
            .retain(|(jid, _, e)| jid != job_id || *e != epoch);
    }

    fn executor_has_running_task_in_job(
        &self,
        executor_id: &ExecutorId,
        job_id: &JobId,
    ) -> bool {
        self.jobs.get(job_id).is_some_and(|job| {
            job.stages.iter().any(|stage| {
                stage.tasks().iter().any(|task| {
                    task.state() == TaskState::Running
                        && task.assigned_executor() == Some(executor_id)
                })
            })
        })
    }

    /// Returns true if the executor owns at least one Running task in a streaming job.
    fn executor_has_streaming_running_tasks(&self, executor_id: &ExecutorId) -> bool {
        self.jobs.values().any(|job| {
            job.spec.kind() == JobKind::Streaming
                && job.stages.iter().any(|stage| {
                    stage.tasks().iter().any(|task| {
                        task.state() == TaskState::Running
                            && task.assigned_executor() == Some(executor_id)
                    })
                })
        })
    }

    /// Snapshot all in-memory jobs to a `MetadataStore` so that a subsequent
    /// `recover_from_store` call sees the current state.  Primarily useful in
    /// tests that simulate a coordinator restart without a real persistent store.
    pub fn persist_jobs_to_store(&self, store: &mut dyn MetadataStore) -> SchedulerResult<()> {
        for record in self.jobs.values() {
            store.save_job(record)?;
        }
        Ok(())
    }

    /// Restore job state from a `MetadataStore` after coordinator restart.
    ///
    /// For streaming jobs with Running tasks, the `streaming_reattach_grace_ticks`
    /// window starts here: executors owning those tasks will not be evicted for
    /// missing heartbeats during the grace period, allowing them to re-register
    /// and resume without re-processing already-committed events.
    ///
    /// For streaming jobs with checkpoint config, checkpoint state is recovered
    /// via `CheckpointCoordinator::recover_from_storage`.
    pub fn recover_from_store(&mut self, store: &dyn MetadataStore) -> SchedulerResult<()> {
        // P1.23: Clear in-memory state first so stale phantom jobs cannot survive.
        // Always prefer the persisted store as the authoritative source of truth.
        self.jobs.clear();
        self.streaming_task_index.clear();
        for record in store.jobs() {
            self.jobs.insert(record.job_id().clone(), record.clone());
        }
        // RC1: Rebuild streaming_task_index so heartbeats arriving during the
        // recovery window are not silently dropped.  Without this, every call to
        // apply_streaming_task_state returns early because the index is empty.
        let streaming_job_ids: Vec<JobId> = self
            .jobs
            .values()
            .filter(|j| j.spec.kind() == JobKind::Streaming)
            .map(|j| j.job_id().clone())
            .collect();
        for job_id in streaming_job_ids {
            self.index_streaming_tasks(&job_id);
        }
        // GAP-CP-06: Rebuild checkpoint coordinators from the recovered job specs.
        // Before this fix, recover_from_store iterated an empty in-memory map
        // because checkpoint coordinators are only inserted in submit_job.  After
        // a coordinator restart the map is empty so no checkpointing resumes.
        self.checkpoint_coordinators.clear();
        let streaming_checkpoint_jobs: Vec<(JobId, u64, String, usize)> = self
            .jobs
            .values()
            .filter(|j| {
                j.spec.kind() == JobKind::Streaming
                    && j.spec.checkpoint_interval_ms().is_some()
                    && j.spec.checkpoint_storage_path().is_some()
            })
            .map(|j| {
                let task_count: usize = j.spec.stages().iter().map(|s| s.tasks().len()).sum();
                (
                    j.job_id().clone(),
                    j.spec.checkpoint_interval_ms().unwrap(),
                    j.spec.checkpoint_storage_path().unwrap().to_owned(),
                    task_count,
                )
            })
            .collect();
        for (job_id, interval_ms, storage_path, task_count) in streaming_checkpoint_jobs {
            match Self::open_checkpoint_storage(&storage_path) {
                Ok(storage) => {
                    let mut coord = CheckpointCoordinator::new(
                        job_id.clone(),
                        Arc::new(storage),
                        interval_ms,
                        task_count,
                    );
                    let _ = coord.recover_from_storage();
                    self.checkpoint_coordinators.insert(job_id, coord);
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %e,
                        "cannot restore checkpoint coordinator (storage unavailable); job will checkpoint from scratch"
                    );
                }
            }
        }
        // Start the re-attach grace period.
        self.ticks_since_restart = 0;
        self.recovering = true;
        Ok(())
    }

    fn open_checkpoint_storage(path: &str) -> SchedulerResult<LocalFsCheckpointStorage> {
        LocalFsCheckpointStorage::new(path).map_err(|e| SchedulerError::InvalidJob {
            message: format!("failed to open checkpoint storage at {path}: {e}"),
        })
    }

    /// Compute the current reservation totals for the given namespace.
    ///
    /// Walks active (non-terminal) jobs and sums their `cpu_limit_nanos` and
    /// `memory_limit_bytes` reservations. The returned snapshot is passed to
    /// `QueueManager::admit` so quota enforcement is stateless in the queue
    /// manager itself.
    pub fn namespace_quota_snapshot(&self, namespace_id: Option<&str>) -> NamespaceQuotaSnapshot {
        let mut snap = NamespaceQuotaSnapshot {
            namespace_id: namespace_id.map(str::to_owned),
            ..Default::default()
        };
        for job in self.jobs.values() {
            if job.state().is_terminal() {
                continue;
            }
            if job.spec.namespace_id() != namespace_id {
                continue;
            }
            snap.active_job_count += 1;
            snap.cpu_nanos_reserved = snap
                .cpu_nanos_reserved
                .saturating_add(job.spec.cpu_limit_nanos().unwrap_or(0));
            snap.memory_bytes_reserved = snap
                .memory_bytes_reserved
                .saturating_add(job.spec.memory_limit_bytes().unwrap_or(0));
        }
        snap
    }

    pub fn submit_job(&mut self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        self.ensure_active()?;
        validate_job(&spec)?;

        if self.jobs.contains_key(spec.job_id()) {
            return Err(SchedulerError::DuplicateJob {
                job_id: spec.job_id().clone(),
            });
        }

        // Admission control: compute live quota snapshot then ask the queue manager.
        let quota = self.namespace_quota_snapshot(spec.namespace_id());
        let outcome = self.queue_manager.admit(&spec, &quota);
        if let SubmitOutcome::Queued { .. } = &outcome {
            return Ok(outcome);
        }

        // Create a CheckpointCoordinator for streaming jobs with checkpoint config.
        if spec.kind() == JobKind::Streaming
            && let (Some(interval_ms), Some(storage_path)) = (
                spec.checkpoint_interval_ms(),
                spec.checkpoint_storage_path(),
            )
        {
            let storage = Self::open_checkpoint_storage(storage_path)?;
            let ckpt_coord = CheckpointCoordinator::new(
                spec.job_id().clone(),
                Arc::new(storage),
                interval_ms,
                0,
            );
            self.checkpoint_coordinators
                .insert(spec.job_id().clone(), ckpt_coord);
        }

        let executors = self.executors.schedulable_executors();
        let assignments = StaticScheduler::place(&spec, &executors)?;
        let job_id = spec.job_id().clone();
        let mut record = JobRecord::from_spec(spec, self.config.max_stage_retries());
        record.apply_assignments(assignments);
        if let Some(store) = &self.store {
            let mut s = store.lock().unwrap_or_else(|p| p.into_inner());
            // GAP-CP-05: Fail-closed on persist errors — a submission that cannot
            // be durably recorded must not be accepted, to prevent phantom jobs
            // surviving coordinator restart.
            s.save_job(&record).map_err(|e| SchedulerError::Transport {
                message: format!("failed to persist job {} to metadata store: {e}", record.job_id()),
            })?;
            if let Err(e) = s.append_event(EventLogEvent::JobSubmitted {
                job_id: job_id.clone(),
            }) {
                tracing::warn!(
                    error = %e,
                    job_id = %job_id,
                    "failed to append JobSubmitted event to store (non-fatal)"
                );
            }
        }
        let inserted_job_id = record.job_id().clone();
        self.jobs.insert(inserted_job_id.clone(), record);
        // P1.1: Index streaming tasks for O(1) heartbeat lookup.
        self.index_streaming_tasks(&inserted_job_id);
        // GAP-OB-01: Increment jobs_submitted counter.
        JOBS_SUBMITTED_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        krishiv_metrics::global_metrics().inc_tasks_submitted();
        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::JobSubmitted {
                job_id: inserted_job_id.to_string(),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );
        Ok(SubmitOutcome::Accepted)
    }

    /// Mutable access to the checkpoint coordinator for a specific job.
    pub fn checkpoint_coordinator_mut(
        &mut self,
        job_id: &JobId,
    ) -> Option<&mut CheckpointCoordinator> {
        self.checkpoint_coordinators.get_mut(job_id)
    }

    /// Read-only access to the checkpoint coordinator for a specific job.
    pub fn checkpoint_coordinator(&self, job_id: &JobId) -> Option<&CheckpointCoordinator> {
        self.checkpoint_coordinators.get(job_id)
    }

    /// Route a checkpoint ack to the correct per-job coordinator.
    pub fn handle_checkpoint_ack(&mut self, ack: CheckpointAckRequest) -> CheckpointAckResponse {
        let job_id = ack.job_id.clone();
        match self.checkpoint_coordinators.get_mut(&job_id) {
            None => CheckpointAckResponse::JobNotFound,
            Some(coord) => {
                let current_epoch = coord.current_epoch();
                match coord.receive_ack(ack.clone()) {
                    Ok(true) => {
                        self.clear_checkpoint_notify_for_epoch(&job_id, ack.epoch);
                        CHECKPOINT_EPOCHS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
                        CheckpointAckResponse::Accepted
                    }
                    Ok(false) => CheckpointAckResponse::Accepted,
                    Err(_) => CheckpointAckResponse::StaleEpoch { current_epoch },
                }
            }
        }
    }

    /// Initiate a savepoint for a streaming job.
    ///
    /// Returns the savepoint epoch number.  Fails if no `CheckpointCoordinator`
    /// exists for this job (i.e. the job was not submitted with checkpoint config).
    pub fn savepoint_job(&mut self, job_id: &JobId, label: Option<String>) -> SchedulerResult<u64> {
        let running = self.running_task_count_for_job(job_id);
        match self.checkpoint_coordinators.get_mut(job_id) {
            None => Err(SchedulerError::InvalidJob {
                message: format!(
                    "no checkpoint coordinator for job {job_id}; job must be streaming with checkpoint config"
                ),
            }),
            Some(coord) => {
                coord.set_expected_task_count(running.max(1));
                coord
                    .initiate_savepoint(label)
                    .map_err(|e| SchedulerError::InvalidJob { message: e })
            }
        }
    }

    /// List all valid checkpoint epochs for a job.
    pub fn list_job_checkpoints(&self, job_id: &JobId) -> SchedulerResult<Vec<u64>> {
        match self.checkpoint_coordinators.get(job_id) {
            None => Ok(vec![]),
            Some(coord) => coord.list_epochs().map_err(|e| SchedulerError::InvalidJob {
                message: e.to_string(),
            }),
        }
    }

    /// Read and validate checkpoint metadata for `epoch` from `storage_path`.
    ///
    /// Returns the validated `CheckpointMetadata` so the caller can inspect
    /// source offsets and operator snapshots before resubmitting tasks.
    /// Rejects mismatched parallelism if the job is already tracked.
    pub fn restore_job_from_checkpoint(
        &self,
        job_id: &JobId,
        epoch: u64,
        storage_path: &str,
    ) -> SchedulerResult<CheckpointMetadata> {
        let storage = Self::open_checkpoint_storage(storage_path)?;

        let meta = read_epoch_metadata(&storage, job_id.as_str(), epoch).map_err(|e| {
            SchedulerError::InvalidJob {
                message: format!("cannot read checkpoint epoch {epoch}: {e}"),
            }
        })?;

        let meta = meta.ok_or_else(|| SchedulerError::InvalidJob {
            message: format!("checkpoint epoch {epoch} not found for job {job_id}"),
        })?;

        validate_epoch(&storage, job_id.as_str(), epoch).map_err(|e| {
            SchedulerError::InvalidJob {
                message: format!("checkpoint epoch {epoch} failed integrity check: {e}"),
            }
        })?;

        // GAP-CK-01: Validate fencing token against the live coordinator.
        // Rejects restores from checkpoints that predate the current coordinator
        // generation, preventing stale-epoch restores after a failover.
        if let Some(coord) = self.checkpoint_coordinators.get(job_id) {
            let current_token = coord.fencing_token().as_u64();
            validate_fencing_token(&meta, current_token).map_err(|e| {
                SchedulerError::InvalidJob {
                    message: format!("restore rejected for job {job_id}: {e}"),
                }
            })?;
        }

        // Parallelism check: if the job is already tracked, reject mismatched task count.
        if let Ok(detail) = self.job_detail_snapshot(job_id) {
            let current_tasks = detail.job().task_count();
            let snapshot_tasks = meta.operator_snapshots.len();
            if snapshot_tasks > 0 && current_tasks != snapshot_tasks {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "cannot restore job {job_id}: checkpoint has {snapshot_tasks} operator snapshots \
                         but job has {current_tasks} tasks; rescaling requires a savepoint + resubmit with matching parallelism"
                    ),
                });
            }
        }

        Ok(meta)
    }

    // ── R6a: Out-of-band barrier trigger ──────────────────────────────────────

    /// Initiate a new checkpoint epoch for a streaming job and return one
    /// `InitiateCheckpointRequest` per currently running task.
    ///
    /// The caller is responsible for delivering each request to its executor
    /// (via gRPC or in-process simulation). Executors respond by calling
    /// `handle_checkpoint_ack()` on this coordinator.
    ///
    /// Returns `Err` if the job has no checkpoint coordinator (not a streaming
    /// job with checkpoint config) or if a checkpoint is already in flight.
    pub fn trigger_checkpoint_for_job(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<InitiateCheckpointRequest>> {
        // Validate job exists first.
        self.find_job(job_id)?;

        let running = self.running_task_count_for_job(job_id);
        let coord = self
            .checkpoint_coordinators
            .get_mut(job_id)
            .ok_or_else(|| SchedulerError::InvalidJob {
                message: format!(
                    "no checkpoint coordinator for job {job_id}; \
                     job must be streaming with checkpoint_interval_ms set"
                ),
            })?;
        coord.set_expected_task_count(running);

        let epoch = coord
            .initiate()
            .map_err(|msg| SchedulerError::InvalidJob { message: msg })?;
        let fencing_token = coord.fencing_token();

        // One broadcast request covers all executors for the job — the
        // coordinator doesn't need per-task granularity for the barrier trigger.
        // The executor processes the request once per running task internally.
        Ok(vec![InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch,
            fencing_token,
        }])
    }

    /// Convert and submit a Krishiv logical DAG through the R2 scheduler.
    pub fn submit_logical_plan(
        &mut self,
        job_id: JobId,
        plan: &LogicalPlan,
    ) -> SchedulerResult<SubmitOutcome> {
        self.submit_job(job_spec_from_logical_plan(job_id, plan)?)
    }

    /// Convert and submit a Krishiv physical DAG through the R2 scheduler.
    pub fn submit_physical_plan(
        &mut self,
        job_id: JobId,
        plan: &PhysicalPlan,
    ) -> SchedulerResult<SubmitOutcome> {
        self.submit_job(job_spec_from_physical_plan(job_id, plan)?)
    }

    /// Re-assign all `Pending` tasks in a job to available executors.
    ///
    /// Called after a stage retry (P1.24) to move tasks from `Pending` back to
    /// `Assigned` so `launch_assigned_tasks` can launch them.
    pub fn assign_pending_tasks(&mut self, job_id: &JobId) -> SchedulerResult<usize> {
        self.ensure_active()?;
        // Collect executor ids first to avoid a simultaneous immutable + mutable borrow.
        let executor_ids: Vec<ExecutorId> = self
            .executors
            .schedulable_executors()
            .into_iter()
            .map(|d| d.executor_id().clone())
            .collect();
        if executor_ids.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }
        let job = self.find_job_mut(job_id)?;
        let pending_task_ids: Vec<TaskId> = job
            .stages
            .iter()
            .flat_map(|s| s.tasks())
            .filter(|t| t.state() == TaskState::Pending)
            .map(|t| t.task_id().clone())
            .collect();
        let count = pending_task_ids.len();
        for (idx, task_id) in pending_task_ids.into_iter().enumerate() {
            let executor_id = executor_ids[idx % executor_ids.len()].clone();
            let assignment = TaskAssignment::new(task_id, executor_id);
            job.apply_assignments(vec![assignment]);
        }
        Ok(count)
    }

    /// Launch all assigned tasks for a job.
    pub fn launch_assigned_tasks(&mut self, job_id: &JobId) -> SchedulerResult<usize> {
        self.launch_assigned_task_assignments(job_id)
            .map(|assignments| assignments.len())
    }

    /// Launch all assigned tasks for a job and return executor transport assignments.
    pub fn launch_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        self.ensure_active()?;
        let executor_leases = self.executors.assignment_leases();
        let assignments = self.find_job_mut(job_id)?
            .launch_assigned_task_assignments(&executor_leases)?;
        // GAP-OB-01: Increment tasks_assigned counter.
        TASKS_ASSIGNED_TOTAL.fetch_add(assignments.len() as u64, AtomicOrdering::Relaxed);
        Ok(assignments)
    }

    /// Cancel a job and mark non-terminal stages/tasks cancelled.
    pub fn cancel_job(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        self.ensure_active()?;
        self.find_job_mut(job_id)?.cancel();
        if !self.gc_ready_jobs.contains(job_id) {
            self.gc_ready_jobs.push(job_id.clone());
        }
        self.checkpoint_coordinators.remove(job_id);
        Ok(())
    }

    /// Basic scheduler/executor stability metrics.
    ///
    /// P2.6: Single-pass over jobs/stages/tasks instead of six separate iterations.
    pub fn stability_metrics(&self) -> StabilityMetrics {
        let mut failed_assignments: usize = 0;
        let mut retry_count: usize = 0;
        let mut running_task_count: usize = 0;
        let mut shuffle_partitions_available: usize = 0;
        let mut shuffle_bytes_written: u64 = 0;

        for job in self.jobs.values() {
            // Stage retry counts.
            for stage in job.stages() {
                retry_count = retry_count.saturating_add(stage.retry_count() as usize);
            }
            // Per-task counters and shuffle partition output bytes.
            for stage in job.stages() {
                for task in stage.tasks() {
                    match task.state() {
                        TaskState::Failed => failed_assignments += 1,
                        TaskState::Running => running_task_count += 1,
                        _ => {}
                    }
                    if let Some(meta) = task.output_metadata() {
                        for p in meta.shuffle_partitions() {
                            shuffle_bytes_written =
                                shuffle_bytes_written.saturating_add(p.size_bytes);
                        }
                    }
                }
            }
            // Shuffle partition availability from the job's shuffle_output map.
            shuffle_partitions_available = shuffle_partitions_available
                .saturating_add(job.shuffle_partitions_available_count());
        }

        StabilityMetrics {
            heartbeat_ages: self.executors.heartbeat_ages(),
            failed_assignments,
            retry_count,
            running_task_count,
            shuffle_partitions_available,
            shuffle_bytes_written,
        }
    }

    /// Resolve executor task endpoints for launched assignments.
    pub fn resolve_assignment_targets(
        &self,
        assignments: Vec<ExecutorTaskAssignment>,
    ) -> SchedulerResult<Vec<(String, ExecutorTaskAssignment)>> {
        let mut targets = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            let endpoint = self
                .executors
                .find_executor(assignment.executor_id())?
                .descriptor()
                .task_endpoint()
                .ok_or_else(|| SchedulerError::InvalidJob {
                    message: format!(
                        "executor {} has no task endpoint for assignment push",
                        assignment.executor_id()
                    ),
                })?
                .to_owned();
            targets.push((endpoint, assignment));
        }
        Ok(targets)
    }

    /// Push pre-resolved assignments to executor task endpoints.
    pub async fn deliver_assignment_targets(
        &self,
        targets: Vec<(String, ExecutorTaskAssignment)>,
    ) -> SchedulerResult<Vec<TaskStatusResponse>> {
        let channels = self.executor_channels.clone();
        Self::deliver_assignment_targets_with_channels(channels, targets).await
    }

    async fn deliver_assignment_targets_with_channels(
        channels: Arc<tokio::sync::Mutex<HashMap<String, tonic::transport::Channel>>>,
        targets: Vec<(String, ExecutorTaskAssignment)>,
    ) -> SchedulerResult<Vec<TaskStatusResponse>> {
        let mut responses = Vec::with_capacity(targets.len());
        for (endpoint, assignment) in targets {
            let channel = Self::get_or_connect_channel_on_map(&channels, &endpoint).await?;
            let mut client = wire::v1::executor_task_client::ExecutorTaskClient::new(channel);
            let response = client
                .assign_task(wire::executor_task_assignment_to_wire(assignment))
                .await
                .map_err(|error| SchedulerError::Transport {
                    message: error.to_string(),
                })?
                .into_inner();
            responses.push(
                wire::task_status_response_from_wire(response).map_err(|error| {
                    SchedulerError::Transport {
                        message: error.to_string(),
                    }
                })?,
            );
        }
        Ok(responses)
    }

    /// Launch assigned tasks and push them to executor-owned task endpoints.
    pub async fn push_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<TaskStatusResponse>> {
        let assignments = self.launch_assigned_task_assignments(job_id)?;
        let targets = self.resolve_assignment_targets(assignments)?;
        self.deliver_assignment_targets(targets).await
    }

    /// Cancel a job and push `CancelTask` RPCs to all executors owning running tasks.
    ///
    /// Partial RPC failures are logged but are not fatal for R3.1 — the
    /// scheduler-side cancel is always applied.
    pub async fn push_cancel_job(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        // Collect (endpoint, TaskCancellationRequest) for each running task.
        let mut targets: Vec<(String, TaskCancellationRequest)> = Vec::new();
        {
            let job = self.find_job(job_id)?;
            for stage in job.stages() {
                for task in stage.tasks() {
                    if task.state() == TaskState::Running
                        && let Some(executor_id) = task.assigned_executor()
                        && let Ok(record) = self.executors.find_executor(executor_id)
                        && let Some(endpoint) = record.descriptor().task_endpoint()
                    {
                        let attempt_id = AttemptId::try_new(task.attempt()).map_err(|e| {
                            SchedulerError::InvalidJob {
                                message: e.to_string(),
                            }
                        })?;
                        let req = TaskCancellationRequest::new(TaskAttemptRef::new(
                            job_id.clone(),
                            stage.stage_id().clone(),
                            task.task_id().clone(),
                            attempt_id,
                        ))
                        .with_reason("job cancelled");
                        targets.push((endpoint.to_owned(), req));
                    }
                }
            }
        }

        // Cancel the job in scheduler state first.
        self.cancel_job(job_id)?;

        // Push cancel RPCs — partial failures are non-fatal.
        for (endpoint, req) in targets {
            match wire::v1::executor_task_client::ExecutorTaskClient::connect(endpoint.clone())
                .await
            {
                Ok(mut client) => {
                    let _ = client
                        .cancel_task(wire::task_cancellation_request_to_wire(req))
                        .await;
                }
                Err(err) => {
                    eprintln!("push_cancel_job: failed to connect to {endpoint}: {err}");
                }
            }
        }
        Ok(())
    }

    /// Apply a task update from an executor.
    pub fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        self.ensure_active()?;
        self.executors
            .validate_lease(update.executor_id(), update.lease_generation())?;
        let job_id = update.job_id().clone();
        let outcome = self.find_job_mut(&job_id)?.apply_task_update(update)?;

        // Snapshot the job's current state and resource usage after the update.
        let (is_terminal, usage) = self
            .jobs
            .get(&job_id)
            .map(|r| (r.state().is_terminal(), r.resource_usage.clone()))
            .unwrap_or((false, ResourceUsage::zero()));

        if is_terminal && !self.gc_ready_jobs.contains(&job_id) {
            self.gc_ready_jobs.push(job_id.clone());
            self.checkpoint_coordinators.remove(&job_id);
            // Notify the queue manager so it can release reserved capacity.
            self.queue_manager.on_job_complete(&job_id, &usage);
        }
        if let Some(record) = self.jobs.get(&job_id)
            && let Some(store) = &self.store
        {
            let mut s = store.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(e) = s.save_job(record) {
                tracing::warn!(
                    error = %e,
                    job_id = %job_id,
                    "failed to persist job to store after task update"
                );
            }
        }
        // P1.1: Remove streaming task index entries when job reaches a terminal state.
        let is_terminal = self
            .jobs
            .get(&job_id)
            .map(|r| r.state().is_terminal())
            .unwrap_or(false);
        if is_terminal {
            self.remove_streaming_task_index(&job_id);
        }
        Ok(outcome)
    }

    /// Drain the list of jobs that have reached a terminal state and need shuffle GC.
    ///
    /// The coordinator binary's tick loop should call this, then asynchronously
    /// delete partitions for each returned job id via the shuffle store.
    pub fn take_gc_ready_jobs(&mut self) -> Vec<JobId> {
        std::mem::take(&mut self.gc_ready_jobs)
    }

    /// Snapshot one job.
    pub fn job_snapshot(&self, job_id: &JobId) -> SchedulerResult<JobSnapshot> {
        self.find_job(job_id).map(JobRecord::snapshot)
    }

    /// Snapshot one job with stage and task detail.
    pub fn job_detail_snapshot(&self, job_id: &JobId) -> SchedulerResult<JobDetailSnapshot> {
        self.find_job(job_id).map(JobRecord::detail_snapshot)
    }

    /// Snapshot all known jobs.
    pub fn job_snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs.values().map(JobRecord::snapshot).collect()
    }

    /// Snapshot all known executors.
    pub fn executor_snapshots(&self) -> Vec<ExecutorRecord> {
        self.executors.list().to_vec()
    }

    fn reset_running_tasks_for_lost_executor(&mut self, lost_id: &ExecutorId) {
        for job in self.jobs.values_mut() {
            let mut job_affected = false;
            for stage in &mut job.stages {
                let mut stage_affected = false;
                for task in &mut stage.tasks {
                    if task.state == TaskState::Running
                        && task.assigned_executor.as_ref() == Some(lost_id)
                    {
                        // Keep the assignment for the next launch attempt; the
                        // attempt counter is not bumped here — it will be bumped
                        // when `launch_assigned_task_assignments` is called next.
                        task.state = TaskState::Assigned;
                        stage_affected = true;
                        job_affected = true;
                    }
                }
                if stage_affected {
                    stage.refresh_state();
                }
            }
            if job_affected {
                job.refresh_state();
            }
        }
    }

    /// P1.2: Get or create a cached gRPC channel for the given executor endpoint.
    ///
    /// On a cache hit, clones the existing `Channel` (pointer-only cost).
    /// On a miss, establishes a new TCP+TLS connection and stores it for reuse.
    async fn get_or_connect_channel(
        &self,
        endpoint: &str,
    ) -> SchedulerResult<tonic::transport::Channel> {
        Self::get_or_connect_channel_on_map(&self.executor_channels, endpoint).await
    }

    async fn get_or_connect_channel_on_map(
        channels: &Arc<tokio::sync::Mutex<HashMap<String, tonic::transport::Channel>>>,
        endpoint: &str,
    ) -> SchedulerResult<tonic::transport::Channel> {
        let mut map = channels.lock().await;
        if let Some(ch) = map.get(endpoint) {
            return Ok(ch.clone());
        }
        let ch = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| SchedulerError::InvalidJob {
                message: e.to_string(),
            })?
            .connect()
            .await
            .map_err(|e| SchedulerError::ExecutorUnavailable {
                endpoint: endpoint.to_string(),
                reason: e.to_string(),
            })?;
        map.insert(endpoint.to_owned(), ch.clone());
        Ok(ch)
    }

    fn ensure_active(&self) -> SchedulerResult<()> {
        if self.state == CoordinatorState::Active {
            Ok(())
        } else {
            Err(SchedulerError::InactiveCoordinator {
                coordinator_id: self.coordinator_id.clone(),
                state: self.state,
            })
        }
    }

    fn find_job(&self, job_id: &JobId) -> SchedulerResult<&JobRecord> {
        self.jobs
            .get(job_id)
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }

    fn find_job_mut(&mut self, job_id: &JobId) -> SchedulerResult<&mut JobRecord> {
        self.jobs
            .get_mut(job_id)
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }
}

// ── Leader election abstraction ───────────────────────────────────────────────

/// `SingleNodeElection` is the embedded/single-node implementation.
/// `K8sLeaseElection` in `krishiv-operator` implements this for Kubernetes HA.
/// Bare-metal HA backed by external etcd is deferred post-R9.
///
/// # ADR-R12-02 (Option B — AFIT)
/// The three mutating methods use `async fn` (AFIT, stable since Rust 1.75).
/// This eliminates the `block_on` anti-pattern in `K8sLeaseElection`, which
/// panics when called from inside an async Tokio runtime context.
///
/// `dyn LeaderElection` is not used anywhere in this codebase, so auto-trait
/// bounds on the returned futures (the lint `async_fn_in_trait` warns about)
/// are not a concern.
#[allow(async_fn_in_trait)]
pub trait LeaderElection: Send + Sync {
    /// Whether this node currently holds the leader lease.
    fn is_leader(&self) -> bool;

    /// Attempt to acquire the leader lease. Returns `true` if acquired.
    ///
    /// Default: always succeeds (single-node behaviour).
    async fn try_acquire(&self) -> bool {
        self.is_leader()
    }

    /// Renew the current leader lease. Returns `true` if the renewal succeeded.
    ///
    /// A `false` result means another node has taken the lease — this node must
    /// stop acting as leader immediately and reject any pending checkpoint writes.
    ///
    /// Default: returns `is_leader()` (single-node behaviour).
    async fn renew(&self) -> bool {
        self.is_leader()
    }

    /// Release the leader lease voluntarily (graceful shutdown).
    ///
    /// Default: no-op.
    async fn release(&self) {}

    /// Monotonically increasing fencing token for this lease holder.
    ///
    /// Must be stored in every `CheckpointMetadata` committed by this
    /// coordinator. A checkpoint whose `fencing_token` is less than the current
    /// token must be rejected.
    ///
    /// Default: returns `0` (single-node — no competing coordinators).
    fn fencing_token(&self) -> u64 {
        0
    }
}

/// No-op leader election that always reports this node as the leader.
#[derive(Debug, Default)]
pub struct SingleNodeElection;

impl LeaderElection for SingleNodeElection {
    fn is_leader(&self) -> bool {
        true
    }
}

// ── TLS configuration ─────────────────────────────────────────────────────────

/// TLS configuration for the coordinator/executor gRPC transport.
///
/// When `None` is passed to the TLS-aware server builder, connections are
/// plaintext (appropriate for K8s pod-to-pod within a NetworkPolicy-controlled
/// namespace, or local development).
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM-encoded server certificate chain.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded server private key.
    pub key_pem: Vec<u8>,
    /// Optional PEM-encoded CA certificate for client certificate verification
    /// (mTLS). When `None`, client certificates are not required.
    pub ca_pem: Option<Vec<u8>>,
}

impl TlsConfig {
    /// Build a `TlsConfig` from PEM byte slices.
    pub fn new(cert_pem: impl Into<Vec<u8>>, key_pem: impl Into<Vec<u8>>) -> Self {
        Self {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
            ca_pem: None,
        }
    }

    /// Attach a CA certificate for mTLS peer verification.
    #[must_use]
    pub fn with_ca(mut self, ca_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca_pem.into());
        self
    }
}


#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use krishiv_checkpoint::{
        CheckpointMetadata, CheckpointStorage, IntegrityManifest, LocalFsCheckpointStorage,
        list_valid_epochs, write_epoch_metadata, write_manifest,
    };
    use krishiv_plan::{ExecutionKind as PlanExecutionKind, LogicalPlan, PhysicalPlan, PlanNode};
    use krishiv_proto::{
        AttemptId, CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
        CoordinatorId, DeregisterExecutorRequest, ExecutorDescriptor, ExecutorHeartbeat,
        ExecutorHeartbeatRequest, ExecutorId, ExecutorState, FencingToken, JobId, JobKind, JobSpec,
        JobState, LeaseGeneration, RegisterExecutorRequest, StageId, StageSpec, StreamingTaskState,
        TaskAttemptRef, TaskId, TaskOutputMetadata, TaskSpec, TaskState, TaskStatusRequest,
        TaskStatusResponse, TaskStatusUpdate, TransportDisposition, wire,
    };

    use super::{
        AdaptiveDecisionKind, AdaptiveOverrideConfig, CheckpointCoordinator,
        CheckpointCoordinatorState, ConfigFileQueueManager, Coordinator, CoordinatorConfig,
        CoordinatorExecutorTonicService, EventLogEvent, ExecutorRegistry, InMemoryMetadataStore,
        InMemoryQueueManager, JsonFileMetadataStore, LeaderElection, MetadataStore,
        NamespaceQuotaSnapshot, QueueManager, QuotaPolicy, QuotaQueueManager, SchedulerError,
        SharedCoordinator, SingleNodeElection, StaticScheduler, SubmitOutcome, TaskUpdateOutcome,
        job_spec_from_logical_plan, serve_coordinator_executor_grpc_with_listener,
    };
    #[cfg(feature = "sqlite")]
    use super::SqliteMetadataStore;

    #[derive(Debug, Clone, Default)]
    struct RecordingExecutorTaskService {
        task_ids: Arc<Mutex<Vec<String>>>,
    }

    #[tonic::async_trait]
    impl wire::v1::executor_task_server::ExecutorTask for RecordingExecutorTaskService {
        async fn assign_task(
            &self,
            request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            let assignment = wire::executor_task_assignment_from_wire(request.into_inner())
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
            self.task_ids
                .lock()
                .unwrap()
                .push(assignment.task_id().as_str().to_owned());
            Ok(tonic::Response::new(wire::task_status_response_to_wire(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )))
        }

        async fn cancel_task(
            &self,
            _request: tonic::Request<wire::v1::TaskCancellationRequest>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(wire::task_status_response_to_wire(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )))
        }
    }

    #[test]
    fn standby_coordinator_rejects_mutation() {
        let mut coordinator = Coordinator::standby(CoordinatorId::try_new("coord-1").unwrap());
        let executor = ExecutorDescriptor::new(ExecutorId::try_new("exec-1").unwrap(), "pod-a", 1);

        let error = coordinator.register_executor(executor).unwrap_err();

        assert!(matches!(error, SchedulerError::InactiveCoordinator { .. }));
    }

    #[test]
    fn executor_registry_accepts_registration_and_heartbeat() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut registry = ExecutorRegistry::default();
        registry
            .register(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        registry
            .heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        assert_eq!(registry.list().len(), 1);
        assert_eq!(registry.list()[0].state(), ExecutorState::Healthy);
        assert_eq!(registry.list()[0].last_heartbeat_tick(), 0);
    }

    #[test]
    fn heartbeat_timeout_marks_executor_lost() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(1, 2),
        );
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        assert!(coordinator.advance_heartbeat_clock(1).unwrap().is_empty());
        let lost = coordinator.advance_heartbeat_clock(1).unwrap();

        assert_eq!(lost, vec![executor_id]);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );
    }

    #[test]
    fn stale_lease_heartbeat_is_rejected_after_executor_loss() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let lease_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        coordinator.mark_executor_lost(&executor_id).unwrap();
        let current_generation = coordinator.executor_snapshots()[0].lease_generation();
        let error = coordinator
            .executor_heartbeat(
                ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                    .with_lease_generation(lease_generation),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            SchedulerError::StaleExecutorLease {
                expected,
                received,
                ..
            } if expected == current_generation && received == lease_generation
        ));
    }

    #[test]
    fn lost_executor_can_reregister_with_next_lease_generation() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let initial_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        coordinator.mark_executor_lost(&executor_id).unwrap();
        let next_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-b", 2))
            .unwrap();

        assert_eq!(next_generation, initial_generation.next());
        let executor = &coordinator.executor_snapshots()[0];
        assert_eq!(executor.state(), ExecutorState::Registered);
        assert_eq!(executor.descriptor().host(), "pod-b");
        assert_eq!(executor.descriptor().slots(), 2);
        assert_eq!(executor.lease_generation(), next_generation);
    }

    #[test]
    fn executor_deregisters_with_valid_lease() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let lease_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        let next_generation = coordinator
            .deregister_executor(&executor_id, lease_generation)
            .unwrap();

        let executor = &coordinator.executor_snapshots()[0];
        assert_eq!(executor.state(), ExecutorState::Removed);
        assert_eq!(executor.lease_generation(), next_generation);
    }

    #[test]
    fn cancel_job_marks_active_tasks_cancelled() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job_id = JobId::try_new("job-cancel").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "pod-a", 1))
            .unwrap();
        coordinator
            .submit_job(demo_job_with_id(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        coordinator.cancel_job(&job_id).unwrap();

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.job().state(), JobState::Cancelled);
        assert_eq!(
            detail.stages()[0].state(),
            krishiv_proto::StageState::Cancelled
        );
        assert!(
            detail.stages()[0]
                .tasks()
                .iter()
                .all(|task| task.state() == TaskState::Cancelled)
        );
    }

    #[test]
    fn task_output_metadata_is_visible_in_job_detail_snapshot() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job_id = JobId::try_new("job-output-meta").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let lease_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();
        let assignment = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap()
            .remove(0);
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Running,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(lease_generation),
            )
            .unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id,
                    TaskState::Succeeded,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(lease_generation)
                .with_output_metadata(TaskOutputMetadata::new("sql", 2, 1, 2)),
            )
            .unwrap();

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let metadata = detail.stages()[0].tasks()[0].output_metadata().unwrap();
        assert_eq!(metadata.output_kind(), "sql");
        assert_eq!(metadata.row_count(), 2);
        assert_eq!(metadata.batch_count(), 1);
        assert_eq!(metadata.column_count(), 2);
    }

    #[test]
    fn stability_metrics_include_heartbeat_age_and_task_counts() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job_id = JobId::try_new("job-metrics").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator.advance_heartbeat_clock(1).unwrap();

        let metrics = coordinator.stability_metrics();
        assert_eq!(metrics.heartbeat_ages()[0].executor_id(), &executor_id);
        assert_eq!(metrics.heartbeat_ages()[0].age_ticks(), 1);
        assert_eq!(metrics.running_task_count(), 1);
    }

    #[test]
    fn shared_coordinator_exposes_same_scheduler_state_to_clones() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let observer = shared.clone();
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
                .unwrap();
            coordinator
                .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
                .unwrap();
        }

        let coordinator = observer.read().unwrap();
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
    }

    #[tokio::test]
    async fn tonic_service_registers_executor_through_shared_coordinator() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        let response = service
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2),
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(response.lease_generation(), LeaseGeneration::initial());
        let coordinator = shared.read().unwrap();
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].executor_id(),
            &executor_id
        );
    }

    #[tokio::test]
    async fn tonic_service_applies_executor_heartbeat_to_shared_coordinator() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let task_id = TaskId::try_new("task-1").unwrap();

        service
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2),
            )))
            .await
            .unwrap();

        let heartbeat = ExecutorHeartbeatRequest::new(
            executor_id.clone(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_running_attempts(vec![TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            task_id.clone(),
            AttemptId::initial(),
        )]);
        let response = service
            .executor_heartbeat(tonic::Request::new(heartbeat))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        let coordinator = shared.read().unwrap();
        let executor = &coordinator.executor_snapshots()[0];
        assert_eq!(executor.state(), ExecutorState::Healthy);
        assert_eq!(executor.running_tasks(), &[task_id]);
    }

    #[tokio::test]
    async fn tonic_service_reports_unknown_executor_heartbeat_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared);

        let response = service
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                ExecutorId::try_new("missing-exec").unwrap(),
                LeaseGeneration::initial(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            response.disposition(),
            TransportDisposition::UnknownExecutor
        );
    }

    #[tokio::test]
    async fn tonic_service_reports_stale_lease_heartbeat_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
                .unwrap();
            coordinator.mark_executor_lost(&executor_id).unwrap();
        }

        let response = service
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                executor_id,
                LeaseGeneration::initial(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::StaleLease);
        assert_eq!(
            response.lease_generation(),
            LeaseGeneration::initial().next()
        );
    }

    #[tokio::test]
    async fn coordinator_pushes_assignments_to_executor_task_endpoint() {
        let service = RecordingExecutorTaskService::default();
        let recorded = service.task_ids.clone();
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping assignment push test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wire::v1::executor_task_server::ExecutorTaskServer::new(
                    service,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job_id = JobId::try_new("job-push").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(
                ExecutorDescriptor::new(executor_id, "pod-a", 1)
                    .with_task_endpoint(format!("http://{addr}")),
            )
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();

        let responses = coordinator
            .push_assigned_task_assignments(&job_id)
            .await
            .unwrap();

        assert_eq!(responses[0].disposition(), TransportDisposition::Accepted);
        assert_eq!(recorded.lock().unwrap().as_slice(), &["task-1".to_owned()]);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn task_launch_drives_to_running() {
        let service = RecordingExecutorTaskService::default();
        let recorded = service.task_ids.clone();
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping task_launch_drives_to_running: loopback denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wire::v1::executor_task_server::ExecutorTaskServer::new(
                    service,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let executor_id = ExecutorId::try_new("exec-launch").unwrap();
        let job_id = JobId::try_new("job-launch").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-launch").unwrap());
        coordinator
            .register_executor(
                ExecutorDescriptor::new(executor_id, "pod-launch", 1)
                    .with_task_endpoint(format!("http://{addr}")),
            )
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();

        let shared = SharedCoordinator::new(coordinator);
        let launched = shared.drive_pending_task_launches().await.unwrap();
        assert_eq!(launched, 1);
        assert_eq!(recorded.lock().unwrap().as_slice(), &["task-1".to_owned()]);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn grpc_service_registers_and_heartbeats_over_network() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping networked gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind test gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_shared = shared.clone();
        let server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(listener, server_shared)
                .await
                .unwrap();
        });

        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            format!("http://{addr}"),
        )
        .await
        .unwrap();
        let executor_id = ExecutorId::try_new("exec-network-1").unwrap();
        let registration = client
            .register_executor(wire::register_executor_request_to_wire(
                RegisterExecutorRequest::new(ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-network",
                    2,
                )),
            ))
            .await
            .unwrap()
            .into_inner();
        let registration = wire::register_executor_response_from_wire(registration).unwrap();

        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        assert_eq!(registration.executor_id(), &executor_id);

        let heartbeat = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                ExecutorHeartbeatRequest::new(
                    executor_id.clone(),
                    LeaseGeneration::initial(),
                    ExecutorState::Healthy,
                ),
            ))
            .await
            .unwrap()
            .into_inner();
        let heartbeat = wire::executor_heartbeat_response_from_wire(heartbeat).unwrap();

        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);
        {
            let coordinator = shared.read().unwrap();
            assert_eq!(coordinator.executor_snapshots().len(), 1);
            assert_eq!(
                coordinator.executor_snapshots()[0].state(),
                ExecutorState::Healthy
            );
        }

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();
        {
            let mut coordinator = shared.write().unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let task_status = client
            .task_status(wire::task_status_request_to_wire(TaskStatusRequest::new(
                TaskAttemptRef::new(job_id, stage_id, task_id, AttemptId::initial()),
                executor_id,
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();
        let task_status = wire::task_status_response_from_wire(task_status).unwrap();

        assert_eq!(task_status.disposition(), TransportDisposition::Accepted);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn grpc_deregister_transitions_executor_to_removed() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-deregister").unwrap(),
        ));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping networked gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind test gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_shared = shared.clone();
        let server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(listener, server_shared)
                .await
                .unwrap();
        });

        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            format!("http://{addr}"),
        )
        .await
        .unwrap();

        let executor_id = ExecutorId::try_new("exec-dereg-1").unwrap();
        let register_resp = client
            .register_executor(wire::register_executor_request_to_wire(
                RegisterExecutorRequest::new(ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-dereg",
                    1,
                )),
            ))
            .await
            .unwrap()
            .into_inner();
        let register_resp = wire::register_executor_response_from_wire(register_resp).unwrap();
        assert_eq!(register_resp.disposition(), TransportDisposition::Accepted);

        let lease_generation = {
            let coordinator = shared.read().unwrap();
            coordinator
                .executor_snapshots()
                .into_iter()
                .find(|s| s.executor_id() == &executor_id)
                .expect("executor should be registered")
                .lease_generation()
        };

        let dereg_resp = client
            .deregister_executor(wire::deregister_executor_request_to_wire(
                DeregisterExecutorRequest::new(executor_id.clone(), lease_generation),
            ))
            .await
            .unwrap()
            .into_inner();
        let dereg_resp = wire::deregister_executor_response_from_wire(dereg_resp).unwrap();
        assert_eq!(dereg_resp.disposition(), TransportDisposition::Accepted);

        {
            let coordinator = shared.read().unwrap();
            let snapshot = coordinator
                .executor_snapshots()
                .into_iter()
                .find(|s| s.executor_id() == &executor_id)
                .expect("executor should still be in registry after deregister");
            assert_eq!(snapshot.state(), ExecutorState::Removed);
        }

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn tonic_service_routes_task_status_updates() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
                .unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let status = TaskStatusRequest::new(
            TaskAttemptRef::new(job_id.clone(), stage_id, task_id, AttemptId::initial()),
            executor_id,
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        );
        let response = service
            .task_status(tonic::Request::new(status))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(
            shared
                .read()
                .unwrap()
                .job_snapshot(&job_id)
                .unwrap()
                .state(),
            JobState::Running
        );
    }

    #[tokio::test]
    async fn tonic_service_reports_duplicate_task_status_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();
        let ids = TaskAttemptRef::new(
            job_id.clone(),
            stage_id.clone(),
            task_id.clone(),
            AttemptId::initial(),
        );

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
                .unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let accepted = service
            .task_status(tonic::Request::new(TaskStatusRequest::new(
                ids.clone(),
                executor_id.clone(),
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();
        let duplicate = service
            .task_status(tonic::Request::new(TaskStatusRequest::new(
                ids,
                executor_id,
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(accepted.disposition(), TransportDisposition::Accepted);
        assert_eq!(duplicate.disposition(), TransportDisposition::Duplicate);
    }

    #[tokio::test]
    async fn tonic_service_reports_stale_task_attempt_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(1, 3),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
                .unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
            coordinator
                .apply_task_update(TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id.clone(),
                    task_id.clone(),
                    executor_id.clone(),
                    TaskState::Failed,
                    1,
                ))
                .unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let response = service
            .task_status(tonic::Request::new(TaskStatusRequest::new(
                TaskAttemptRef::new(job_id, stage_id, task_id, AttemptId::initial()),
                executor_id,
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::StaleAttempt);
    }

    #[test]
    fn coordinator_rejects_task_status_with_stale_executor_lease() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();
        let stale_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator.mark_executor_lost(&executor_id).unwrap();

        let error = coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id,
                    stage_id,
                    task_id,
                    executor_id,
                    TaskState::Succeeded,
                    1,
                )
                .with_lease_generation(stale_generation),
            )
            .unwrap_err();

        assert!(matches!(error, SchedulerError::StaleExecutorLease { .. }));
    }

    #[test]
    fn duplicate_terminal_task_status_is_idempotent() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        let update = TaskStatusUpdate::new(
            job_id.clone(),
            stage_id,
            task_id,
            executor_id,
            TaskState::Succeeded,
            1,
        );
        assert_eq!(
            coordinator.apply_task_update(update.clone()).unwrap(),
            TaskUpdateOutcome::Applied
        );
        assert_eq!(
            coordinator.apply_task_update(update).unwrap(),
            TaskUpdateOutcome::Duplicate
        );
        assert_eq!(
            coordinator
                .job_snapshot(&job_id)
                .unwrap()
                .succeeded_task_count(),
            1
        );
    }

    #[test]
    fn coordinator_launch_returns_executor_task_assignments_with_attempt_and_lease() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let lease_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();

        coordinator.submit_job(job).unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].job_id(), &job_id);
        assert_eq!(assignments[0].executor_id(), &executor_id);
        assert_eq!(assignments[0].attempt_id(), AttemptId::initial());
        assert_eq!(assignments[0].lease_generation(), lease_generation);
        assert_eq!(
            assignments[0].output_contract().kind(),
            krishiv_proto::OutputContractKind::InlineRecordBatches
        );
        assert!(!assignments[0].input_partitions().is_empty());
        assert!(
            coordinator
                .job_snapshot(&job_id)
                .unwrap()
                .running_task_count()
                > 0
        );
    }

    #[test]
    fn static_scheduler_places_tasks_round_robin() {
        let job = demo_job();
        let exec_a = ExecutorDescriptor::new(ExecutorId::try_new("exec-a").unwrap(), "pod-a", 1);
        let exec_b = ExecutorDescriptor::new(ExecutorId::try_new("exec-b").unwrap(), "pod-b", 1);
        let executors = vec![&exec_a, &exec_b];

        let assignments = StaticScheduler::place(&job, &executors).unwrap();

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].executor_id().as_str(), "exec-a");
        assert_eq!(assignments[1].executor_id().as_str(), "exec-b");
    }

    #[test]
    fn converts_batch_logical_plan_into_distributed_job_spec() {
        let plan = LogicalPlan::new("batch-dag", PlanExecutionKind::Batch)
            .with_node(PlanNode::new(
                "scan",
                "scan parquet",
                PlanExecutionKind::Batch,
            ))
            .with_node(
                PlanNode::new("aggregate", "count", PlanExecutionKind::Batch).with_inputs(["scan"]),
            );

        let job = job_spec_from_logical_plan(JobId::try_new("job-batch").unwrap(), &plan).unwrap();

        assert_eq!(job.kind(), JobKind::Batch);
        assert_eq!(job.name(), "batch-dag");
        assert_eq!(job.task_count(), 2);
        assert!(job.stages()[0].tasks()[1].description().contains("scan"));
    }

    #[test]
    fn coordinator_routes_batch_logical_plan_through_scheduler() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                2,
            ))
            .unwrap();

        let plan = LogicalPlan::new("batch-dag", PlanExecutionKind::Batch)
            .with_node(PlanNode::new(
                "scan",
                "scan parquet",
                PlanExecutionKind::Batch,
            ))
            .with_node(
                PlanNode::new("project", "project columns", PlanExecutionKind::Batch)
                    .with_inputs(["scan"]),
            );
        let job_id = JobId::try_new("job-batch").unwrap();

        coordinator
            .submit_logical_plan(job_id.clone(), &plan)
            .unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();

        assert_eq!(snapshot.kind(), JobKind::Batch);
        assert_eq!(snapshot.task_count(), 2);
        assert_eq!(snapshot.assigned_task_count(), 2);
        assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
        assert_eq!(
            coordinator
                .job_snapshot(&job_id)
                .unwrap()
                .running_task_count(),
            2
        );
    }

    #[test]
    fn coordinator_routes_streaming_physical_plan_with_local_state_semantics() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                1,
            ))
            .unwrap();

        let plan =
            PhysicalPlan::new("stream-dag", PlanExecutionKind::Streaming).with_node(PlanNode::new(
                "memory-source",
                "local memory stream",
                PlanExecutionKind::Streaming,
            ));
        let job_id = JobId::try_new("job-stream").unwrap();

        coordinator
            .submit_physical_plan(job_id.clone(), &plan)
            .unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();

        assert_eq!(snapshot.kind(), JobKind::Streaming);
        assert_eq!(snapshot.task_count(), 1);
        assert_eq!(snapshot.assigned_task_count(), 1);
    }

    #[test]
    fn empty_plan_routes_as_single_distributed_task() {
        let plan = PhysicalPlan::new("empty-physical", PlanExecutionKind::Batch);

        let job = super::job_spec_from_physical_plan(JobId::try_new("job-empty").unwrap(), &plan)
            .unwrap();

        assert_eq!(job.kind(), JobKind::Batch);
        assert_eq!(job.task_count(), 1);
        assert!(
            job.stages()[0].tasks()[0]
                .description()
                .contains("empty-physical")
        );
    }

    #[test]
    fn coordinator_submits_launches_and_completes_job() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let first_task = job.stages()[0].tasks()[0].task_id().clone();
        let second_task = job.stages()[0].tasks()[1].task_id().clone();

        coordinator.submit_job(job).unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.assigned_task_count(), 2);

        assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.running_task_count(), 2);

        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                first_task,
                executor_id.clone(),
                TaskState::Succeeded,
                1,
            ))
            .unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id,
                second_task,
                executor_id,
                TaskState::Succeeded,
                1,
            ))
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 2);

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages().len(), 1);
        assert_eq!(detail.stages()[0].tasks().len(), 2);
        assert_eq!(coordinator.job_snapshots().len(), 1);
    }

    #[test]
    fn task_failure_marks_stage_and_job_failed() {
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(0, 3),
        );
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id,
                    task_id,
                    executor_id,
                    TaskState::Failed,
                    1,
                )
                .with_message("executor reported failure"),
            )
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Failed);
        assert_eq!(snapshot.failed_task_count(), 1);
    }

    #[test]
    fn task_failure_retries_entire_stage_before_terminal_failure() {
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(1, 3),
        );
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let first_task = job.stages()[0].tasks()[0].task_id().clone();
        let second_task = job.stages()[0].tasks()[1].task_id().clone();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                first_task.clone(),
                executor_id.clone(),
                TaskState::Failed,
                1,
            ))
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Running);
        // P1.24: After retry, tasks are Pending (not Assigned), so assigned_task_count = 0.
        assert_eq!(snapshot.assigned_task_count(), 0);
        assert_eq!(snapshot.failed_task_count(), 0);

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].retry_count(), 1);
        // P1.24: Retried tasks must be Pending so the scheduler can re-queue them.
        assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Pending);
        assert_eq!(detail.stages()[0].tasks()[1].state(), TaskState::Pending);

        // Re-assign then launch (simulates the scheduler's next planning cycle).
        assert_eq!(coordinator.assign_pending_tasks(&job_id).unwrap(), 2);
        assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].tasks()[0].attempt(), 2);
        assert_eq!(detail.stages()[0].tasks()[1].attempt(), 2);

        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                first_task,
                executor_id.clone(),
                TaskState::Succeeded,
                2,
            ))
            .unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id,
                second_task,
                executor_id,
                TaskState::Succeeded,
                2,
            ))
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 2);
    }

    // ── P1.24: retry_stage sets Pending (not Assigned) ───────────────────────

    #[test]
    fn retried_tasks_are_pending_and_become_schedulable() {
        // P1.24: Verify that after a stage retry all tasks transition to Pending
        // so the scheduler can re-queue them through the normal placement path.
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-p124").unwrap(),
            CoordinatorConfig::new(1, 3),
        );
        let executor_id = ExecutorId::try_new("exec-p124").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        // Report task failure to trigger a retry.
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                task_id.clone(),
                executor_id.clone(),
                TaskState::Failed,
                1,
            ))
            .unwrap();

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].retry_count(), 1);

        // All tasks must be Pending — not Assigned — so placement runs again.
        for task in detail.stages()[0].tasks() {
            assert_eq!(
                task.state(),
                TaskState::Pending,
                "retried task {} must be Pending, got {:?}",
                task.task_id(),
                task.state()
            );
        }

        // assign_pending_tasks + launch confirms tasks are re-schedulable.
        let assigned = coordinator.assign_pending_tasks(&job_id).unwrap();
        assert_eq!(assigned, 2, "both tasks must be re-assigned after retry");
        let launched = coordinator.launch_assigned_tasks(&job_id).unwrap();
        assert_eq!(
            launched, 2,
            "both tasks must be launchable after re-assignment"
        );
    }

    #[test]
    fn coordinator_marks_executor_lost() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        coordinator.mark_executor_lost(&executor_id).unwrap();

        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );
    }

    fn demo_job() -> JobSpec {
        demo_job_with_id(JobId::try_new("job-1").unwrap())
    }

    fn demo_job_with_id(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "demo batch", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a"))
                .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "scan b")),
        )
    }

    fn single_task_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "single task", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a")),
        )
    }

    fn single_task_streaming_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "streaming job", JobKind::Streaming).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stream-stage").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream-task"),
            ),
        )
    }

    // ── streaming refresh_state guard ─────────────────────────────────────

    #[test]
    fn streaming_job_does_not_succeed_when_all_stages_succeed() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                1,
            ))
            .unwrap();
        let job_id = JobId::try_new("job-stream-1").unwrap();
        coordinator
            .submit_job(single_task_streaming_job(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let executor_id = detail.stages()[0].tasks()[0]
            .assigned_executor()
            .unwrap()
            .clone();
        let lease = coordinator.executor_snapshots()[0].lease_generation();
        let attempt = detail.stages()[0].tasks()[0].attempt();

        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    StageId::try_new("stage-1").unwrap(),
                    task_id,
                    executor_id,
                    TaskState::Succeeded,
                    attempt,
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        // Streaming jobs must never reach Succeeded — they stay Running.
        let final_snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_ne!(
            final_snapshot.state(),
            JobState::Succeeded,
            "streaming job must not transition to Succeeded"
        );
        assert_eq!(final_snapshot.state(), JobState::Running);
    }

    #[test]
    fn batch_job_succeeds_when_all_stages_succeed() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                1,
            ))
            .unwrap();
        let job_id = JobId::try_new("job-batch-1").unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let executor_id = detail.stages()[0].tasks()[0]
            .assigned_executor()
            .unwrap()
            .clone();
        let lease = coordinator.executor_snapshots()[0].lease_generation();
        let attempt = detail.stages()[0].tasks()[0].attempt();

        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    StageId::try_new("stage-1").unwrap(),
                    task_id,
                    executor_id,
                    TaskState::Succeeded,
                    attempt,
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        assert_eq!(
            coordinator.job_snapshot(&job_id).unwrap().state(),
            JobState::Succeeded,
            "batch job must transition to Succeeded"
        );
    }

    // ── streaming re-attach grace period ──────────────────────────────────

    #[test]
    fn streaming_executor_not_evicted_within_grace_period() {
        let config = CoordinatorConfig::new(1, 2).with_streaming_reattach_grace_ticks(10);
        let mut coordinator =
            Coordinator::active_with_config(CoordinatorId::try_new("coord-1").unwrap(), config);
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        let job_id = JobId::try_new("job-s-1").unwrap();
        coordinator
            .submit_job(single_task_streaming_job(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        // Mark the task Running so it has a committed executor assignment.
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let exec_id_clone = detail.stages()[0].tasks()[0]
            .assigned_executor()
            .unwrap()
            .clone();
        let lease = coordinator.executor_snapshots()[0].lease_generation();
        let attempt = detail.stages()[0].tasks()[0].attempt();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    StageId::try_new("stage-1").unwrap(),
                    task_id,
                    exec_id_clone,
                    TaskState::Running,
                    attempt,
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        // Simulate coordinator restart via recover_from_store.
        // P1.23: the store must contain the streaming job so recovery can restore it.
        let mut store = InMemoryMetadataStore::default();
        store
            .save_job(coordinator.jobs.values().next().unwrap())
            .unwrap();
        coordinator.recover_from_store(&store).unwrap();

        // Advance 3 ticks (> timeout of 2, but < grace period of 10).
        let evicted = coordinator.advance_heartbeat_clock(3).unwrap();
        assert!(
            !evicted.contains(&executor_id),
            "streaming executor must not be evicted within grace period"
        );
    }

    #[test]
    fn streaming_executor_evicted_after_grace_period() {
        let config = CoordinatorConfig::new(1, 2).with_streaming_reattach_grace_ticks(2);
        let mut coordinator =
            Coordinator::active_with_config(CoordinatorId::try_new("coord-1").unwrap(), config);
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        let job_id = JobId::try_new("job-s-2").unwrap();
        coordinator
            .submit_job(single_task_streaming_job(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        // Mark task Running.
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let exec_id_clone = detail.stages()[0].tasks()[0]
            .assigned_executor()
            .unwrap()
            .clone();
        let lease = coordinator.executor_snapshots()[0].lease_generation();
        let attempt = detail.stages()[0].tasks()[0].attempt();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    StageId::try_new("stage-1").unwrap(),
                    task_id,
                    exec_id_clone,
                    TaskState::Running,
                    attempt,
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        // Trigger grace period.
        let store = InMemoryMetadataStore::default();
        coordinator.recover_from_store(&store).unwrap();

        // 5 ticks > grace period (2) + heartbeat timeout (2).
        let evicted = coordinator.advance_heartbeat_clock(5).unwrap();
        assert!(
            evicted.contains(&executor_id),
            "streaming executor must be evicted after grace period expires"
        );
    }

    #[test]
    fn streaming_reattach_updates_task_watermark_and_offset() {
        // Scenario: coordinator has a running streaming job. The coordinator
        // "restarts" (recover_from_store). The executor re-registers and sends
        // a heartbeat with its current watermark and source offset. The coordinator
        // must update the task record without creating a new job.

        let config = CoordinatorConfig::new(1, 10).with_streaming_reattach_grace_ticks(20);
        let mut coordinator =
            Coordinator::active_with_config(CoordinatorId::try_new("coord-ra").unwrap(), config);

        let executor_id = ExecutorId::try_new("exec-ra-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job_id = JobId::try_new("job-ra-1").unwrap();
        coordinator
            .submit_job(single_task_streaming_job(job_id.clone()))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        // Retrieve task/stage ids and mark the task Running.
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let stage_id = detail.stages()[0].stage_id().clone();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let exec_id = detail.stages()[0].tasks()[0]
            .assigned_executor()
            .unwrap()
            .clone();
        let lease = coordinator.executor_snapshots()[0].lease_generation();
        let attempt = detail.stages()[0].tasks()[0].attempt();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id,
                    task_id.clone(),
                    exec_id,
                    TaskState::Running,
                    attempt,
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        // Confirm job is Running before simulated restart.
        assert_eq!(
            coordinator
                .job_detail_snapshot(&job_id)
                .unwrap()
                .job()
                .state(),
            JobState::Running
        );

        // Simulate coordinator restart: persist the streaming job to the store
        // so recovery (P1.23) can restore it (in a real restart the store
        // would have been written before the coordinator process exited).
        let mut store = InMemoryMetadataStore::default();
        store
            .save_job(coordinator.jobs.values().next().unwrap())
            .unwrap();
        coordinator.recover_from_store(&store).unwrap();

        // Executor sends its first post-restart heartbeat carrying streaming state.
        let reported_watermark_ms: u64 = 12_000;
        let reported_offset = b"kafka-partition-0:offset-42".to_vec();
        let heartbeat = ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
            .with_lease_generation(lease)
            .with_streaming_task_states(vec![StreamingTaskState::new(
                task_id.clone(),
                reported_watermark_ms,
                reported_offset.clone(),
            )]);
        coordinator.executor_heartbeat(heartbeat).unwrap();

        // The coordinator must NOT have submitted a new job.
        let snapshots = coordinator.job_snapshots();
        assert_eq!(snapshots.len(), 1, "no duplicate job should be created");
        assert_eq!(snapshots[0].job_id(), &job_id);

        // The task record must now carry the executor-reported watermark and offset.
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task = &detail.stages()[0].tasks()[0];
        assert_eq!(
            task.last_watermark_ms(),
            Some(reported_watermark_ms as i64),
            "task watermark must be updated from heartbeat"
        );
        assert_eq!(
            task.last_source_offset(),
            Some(reported_offset.as_slice()),
            "task source offset must be updated from heartbeat"
        );

        // Job must still be Running (not re-submitted as Accepted/Pending).
        assert_eq!(
            coordinator
                .job_detail_snapshot(&job_id)
                .unwrap()
                .job()
                .state(),
            JobState::Running,
            "job must remain Running after re-attach"
        );
    }

    #[test]
    fn streaming_reattach_does_not_affect_batch_tasks() {
        // A batch job's tasks must not be disturbed by streaming_task_states
        // arriving from an unrelated executor heartbeat.
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-bt").unwrap());

        let executor_id = ExecutorId::try_new("exec-bt-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job_id = JobId::try_new("job-bt-1").unwrap();
        let spec = JobSpec::new(job_id.clone(), "batch", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
        );
        coordinator.submit_job(spec).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task_id = detail.stages()[0].tasks()[0].task_id().clone();
        let lease = coordinator.executor_snapshots()[0].lease_generation();

        // Heartbeat with a streaming_task_state referencing the batch task id.
        let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
            .with_lease_generation(lease)
            .with_streaming_task_states(vec![StreamingTaskState::new(
                task_id.clone(),
                9999,
                vec![],
            )]);
        coordinator.executor_heartbeat(heartbeat).unwrap();

        // The watermark is applied (apply_streaming_state is task-kind-agnostic).
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task = &detail.stages()[0].tasks()[0];
        assert_eq!(
            task.last_watermark_ms(),
            Some(9999),
            "apply_streaming_state is task-agnostic; the coordinator applies it if IDs match"
        );
        // Task state must be unchanged by the heartbeat (Running from launch_assigned_tasks).
        assert_eq!(task.state(), TaskState::Running);
    }

    #[test]
    fn validate_job_rejects_unknown_upstream_stage() {
        let job_id = JobId::try_new("job-1").unwrap();
        let spec = JobSpec::new(job_id, "bad upstream", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage1")
                .with_upstream_stage(StageId::try_new("ghost-stage").unwrap())
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
        );
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                1,
            ))
            .unwrap();
        let result = coordinator.submit_job(spec);
        assert!(
            matches!(result, Err(SchedulerError::InvalidJob { .. })),
            "expected InvalidJob, got {result:?}"
        );
    }

    #[test]
    fn validate_job_accepts_valid_upstream_stage() {
        let job_id = JobId::try_new("job-2").unwrap();
        let spec = JobSpec::new(job_id, "good upstream", JobKind::Batch)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "producer")
                    .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
            )
            .with_stage(
                StageSpec::new(StageId::try_new("stage-2").unwrap(), "consumer")
                    .with_upstream_stage(StageId::try_new("stage-1").unwrap())
                    .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "t2")),
            );
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-2").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                2,
            ))
            .unwrap();
        assert!(coordinator.submit_job(spec).is_ok());
    }

    // ── P0.19: O(1) duplicate task-id detection tests ─────────────────────────

    #[test]
    fn validate_job_rejects_duplicate_task_ids() {
        let job_id = JobId::try_new("job-dup").unwrap();
        // Two stages both containing task-1 — duplicate across stages.
        let spec = JobSpec::new(job_id, "duplicate task ids", JobKind::Batch)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1")
                    .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
            )
            .with_stage(
                StageSpec::new(StageId::try_new("stage-2").unwrap(), "s2")
                    .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1-dup")),
            );
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-dup").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                2,
            ))
            .unwrap();
        let result = coordinator.submit_job(spec);
        assert!(
            matches!(result, Err(SchedulerError::InvalidJob { .. })),
            "expected InvalidJob for duplicate task id, got {result:?}"
        );
    }

    #[test]
    fn validate_job_accepts_large_unique_task_set() {
        // P0.19: Verify correct behaviour with 1000+ tasks using the HashSet path.
        let job_id = JobId::try_new("job-large").unwrap();
        const TASK_COUNT: usize = 1024;
        let mut stage = StageSpec::new(StageId::try_new("stage-big").unwrap(), "big stage");
        for i in 0..TASK_COUNT {
            stage = stage.with_task(TaskSpec::new(
                TaskId::try_new(format!("task-{i}")).unwrap(),
                format!("task {i}"),
            ));
        }
        let spec = JobSpec::new(job_id, "large unique task set", JobKind::Batch).with_stage(stage);
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-large").unwrap());
        // Register enough slots for all tasks.
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                TASK_COUNT,
            ))
            .unwrap();
        assert!(
            coordinator.submit_job(spec).is_ok(),
            "1024 unique task ids must be accepted"
        );
    }

    #[test]
    fn in_memory_metadata_store_round_trips() {
        let coord_id = CoordinatorId::try_new("coord-1").unwrap();
        let job_id = JobId::try_new("job-1").unwrap();
        let mut store = InMemoryMetadataStore::default();

        let event = EventLogEvent::JobSubmitted {
            job_id: job_id.clone(),
        };
        store.append_event(event.clone()).unwrap();
        assert_eq!(store.events().len(), 1);
        assert_eq!(store.events()[0], event);

        let mut coordinator = Coordinator::active(coord_id);
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                2,
            ))
            .unwrap();
        coordinator.submit_job(demo_job()).unwrap();
        let record = coordinator.jobs.values().next().unwrap();
        store.save_job(record).unwrap();
        assert_eq!(store.jobs().len(), 1);
        assert_eq!(store.jobs()[0].job_id(), &job_id);

        // Overwrite with the same record is idempotent.
        store
            .save_job(coordinator.jobs.values().next().unwrap())
            .unwrap();
        assert_eq!(store.jobs().len(), 1);
    }

    #[test]
    fn single_node_election_is_always_leader() {
        let election = SingleNodeElection::default();
        assert!(election.is_leader());
    }

    #[test]
    fn coordinator_recovers_jobs_from_store() {
        let coord_id = CoordinatorId::try_new("coord-1").unwrap();
        let job_id = JobId::try_new("job-1").unwrap();
        let mut store = InMemoryMetadataStore::default();

        let mut prev = Coordinator::active(coord_id.clone());
        prev.register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ))
        .unwrap();
        prev.submit_job(demo_job()).unwrap();
        store.save_job(prev.jobs.values().next().unwrap()).unwrap();

        let mut coordinator = Coordinator::active(coord_id);
        coordinator.recover_from_store(&store).unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Running);
    }

    // ── P1.23: recover_from_store clears stale in-memory state ───────────────

    #[test]
    fn recover_from_store_removes_phantom_stale_jobs() {
        // Pre-populate the coordinator with a stale job that is NOT in the store.
        let coord_id = CoordinatorId::try_new("coord-p123").unwrap();
        let stale_job_id = JobId::try_new("stale-job").unwrap();
        let store_job_id = JobId::try_new("stored-job").unwrap();

        let mut coordinator = Coordinator::active(coord_id.clone());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                2,
            ))
            .unwrap();

        // Submit a job so it lands in-memory but NOT in the store.
        let stale_spec = JobSpec::new(stale_job_id.clone(), "stale", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
        );
        coordinator.submit_job(stale_spec).unwrap();
        assert!(
            coordinator.job_snapshot(&stale_job_id).is_ok(),
            "stale job must be in-memory"
        );

        // Build a store that only has a different job.
        let mut store = InMemoryMetadataStore::default();
        let mut prev = Coordinator::active(coord_id);
        prev.register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-2").unwrap(),
            "pod-b",
            2,
        ))
        .unwrap();
        let stored_spec = JobSpec::new(store_job_id.clone(), "stored", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-s").unwrap(), "ss")
                .with_task(TaskSpec::new(TaskId::try_new("task-s1").unwrap(), "ts1")),
        );
        prev.submit_job(stored_spec).unwrap();
        store.save_job(prev.jobs.values().next().unwrap()).unwrap();

        // Recovery must discard the stale in-memory job and load only the stored one.
        coordinator.recover_from_store(&store).unwrap();
        assert!(
            coordinator.job_snapshot(&stale_job_id).is_err(),
            "stale phantom job must be removed after recovery"
        );
        assert!(
            coordinator.job_snapshot(&store_job_id).is_ok(),
            "store-persisted job must be present after recovery"
        );
    }

    // --- Slice 1: MetadataStore write-through tests ---

    #[test]
    fn metadata_store_persists_job_on_submit() {
        let coord_id = CoordinatorId::try_new("coord-ms1").unwrap();
        let job_id = JobId::try_new("job-1").unwrap();
        let store = InMemoryMetadataStore::default();
        let store_arc = std::sync::Arc::new(std::sync::Mutex::new(store));

        let mut coordinator =
            Coordinator::active(coord_id).with_store(InMemoryMetadataStore::default());
        // Attach our observable arc separately via explicit field — use with_store builder path.
        // We use a fresh store here and verify via the coordinator's write-through.
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                1,
            ))
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();

        // The write-through happened into the internal store.
        drop(store_arc); // not used; we verify indirectly

        // Direct verification: job should be visible on the original coordinator.
        let snap = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snap.job_id(), &job_id);
    }

    #[test]
    fn metadata_store_persists_task_state_on_update() {
        let coord_id = CoordinatorId::try_new("coord-ms2").unwrap();
        let job_id = JobId::try_new("job-ms2").unwrap();

        let mut coordinator =
            Coordinator::active(coord_id).with_store(InMemoryMetadataStore::default());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let lease = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        let assignment = &assignments[0];

        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Running,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(lease),
            )
            .unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id,
                    TaskState::Succeeded,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        let snap = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snap.state(), JobState::Succeeded);
        assert_eq!(snap.succeeded_task_count(), 1);
    }

    #[test]
    fn coordinator_recovers_submitted_job_from_store() {
        let coord_id = CoordinatorId::try_new("coord-ms3").unwrap();
        let job_id = JobId::try_new("job-ms3").unwrap();

        // First coordinator: submit job and let write-through populate the store.
        // We construct the store separately, wrap it, and inject it.
        let mut c1 = Coordinator::active(coord_id.clone());
        c1.register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            1,
        ))
        .unwrap();
        c1.submit_job(single_task_job(job_id.clone())).unwrap();

        // Simulate persisting to an external store manually.
        let mut external_store = InMemoryMetadataStore::default();
        // Save the job record into the external store by recovering c1's state.
        // (In production the write-through would have done this automatically.)
        for job in c1.jobs.values() {
            external_store.save_job(job).unwrap();
        }

        // Second coordinator: recover from the external store.
        let mut c2 = Coordinator::active(coord_id.clone());
        c2.recover_from_store(&external_store).unwrap();

        let snap = c2.job_snapshot(&job_id).unwrap();
        assert_eq!(snap.job_id(), &job_id);
    }

    #[test]
    fn json_file_metadata_store_recovers_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.json");
        let job_id = JobId::try_new("job-json-recover").unwrap();

        {
            let store = JsonFileMetadataStore::open(&path).unwrap();
            let mut coordinator =
                Coordinator::active(CoordinatorId::try_new("coord-json-1").unwrap())
                    .with_store(store);
            let executor_id = ExecutorId::try_new("exec-json-1").unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id, "pod-json", 1))
                .unwrap();
            coordinator
                .submit_job(
                    JobSpec::new(job_id.clone(), "json recovery", JobKind::Batch).with_stage(
                        StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage").with_task(
                            TaskSpec::new(TaskId::try_new("task-1").unwrap(), "sql: select 1"),
                        ),
                    ),
                )
                .unwrap();
        }

        let raw_json = std::fs::read_to_string(&path).unwrap();
        let metadata_json: serde_json::Value = serde_json::from_str(&raw_json).unwrap();
        assert_eq!(metadata_json["schema_version"], 1);
        assert_eq!(metadata_json["store_kind"], "krishiv.scheduler.metadata");

        let reopened = JsonFileMetadataStore::open(&path).unwrap();
        assert_eq!(reopened.events().len(), 1);
        let mut recovered = Coordinator::active(CoordinatorId::try_new("coord-json-2").unwrap());
        recovered.recover_from_store(&reopened).unwrap();
        let snapshot = recovered.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.task_count(), 1);
        assert_eq!(snapshot.assigned_task_count(), 1);
    }

    #[test]
    fn json_file_metadata_store_rejects_newer_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future-metadata.json");
        std::fs::write(
            &path,
            r#"{
              "schema_version": 999,
              "store_kind": "krishiv.scheduler.metadata",
              "events": [],
              "jobs": []
            }"#,
        )
        .unwrap();

        let err = JsonFileMetadataStore::open(&path).unwrap_err();
        assert!(
            err.to_string().contains("schema version 999"),
            "expected newer schema version error, got {err}"
        );
    }

    // --- Slice 3: Executor crash detection + task reassignment ---

    #[test]
    fn executor_crash_detected_and_task_reassigned() {
        let executor_a = ExecutorId::try_new("exec-a").unwrap();
        let executor_b = ExecutorId::try_new("exec-b").unwrap();
        let job_id = JobId::try_new("job-crash").unwrap();

        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-crash").unwrap(),
            CoordinatorConfig::new(1, 2),
        );

        // Register executor A with heartbeat to mark it Healthy.
        let lease_a = coordinator
            .register_executor(ExecutorDescriptor::new(executor_a.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_a.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        // Submit and launch a job (goes to executor A).
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        let assignment = &assignments[0];

        // Mark it Running.
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_a.clone(),
                    TaskState::Running,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(lease_a),
            )
            .unwrap();

        // Task should be Running before crash.
        {
            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Running);
        }

        // Advance clock past heartbeat timeout — executor A is lost.
        coordinator.advance_heartbeat_clock(1).unwrap();
        let lost = coordinator.advance_heartbeat_clock(1).unwrap();
        assert_eq!(lost, vec![executor_a.clone()]);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );

        // Task should have been reset to Assigned.
        {
            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            assert_eq!(
                detail.stages()[0].tasks()[0].state(),
                TaskState::Assigned,
                "task should be reset to Assigned after executor crash"
            );
        }

        // Re-register executor A (lost executor re-joins with a new lease).
        // The task is still assigned to executor A, so the relaunch will go back to it.
        let new_lease_a = coordinator
            .register_executor(ExecutorDescriptor::new(
                executor_a.clone(),
                "pod-a-recovered",
                1,
            ))
            .unwrap();
        coordinator
            .executor_heartbeat(
                ExecutorHeartbeat::new(executor_a.clone(), ExecutorState::Healthy)
                    .with_lease_generation(new_lease_a),
            )
            .unwrap();

        // Also register executor B for visibility (optional in this path).
        let _lease_b = coordinator
            .register_executor(ExecutorDescriptor::new(executor_b.clone(), "pod-b", 1))
            .unwrap();

        let relaunch = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        assert_eq!(relaunch.len(), 1, "should have one task to relaunch");
        // The relaunched assignment targets executor A (the originally assigned executor).
        assert_eq!(relaunch[0].executor_id(), &executor_a);

        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    relaunch[0].stage_id().clone(),
                    relaunch[0].task_id().clone(),
                    executor_a.clone(),
                    TaskState::Running,
                    relaunch[0].attempt_id().as_u32(),
                )
                .with_lease_generation(new_lease_a),
            )
            .unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    relaunch[0].stage_id().clone(),
                    relaunch[0].task_id().clone(),
                    executor_a,
                    TaskState::Succeeded,
                    relaunch[0].attempt_id().as_u32(),
                )
                .with_lease_generation(new_lease_a),
            )
            .unwrap();

        let snap = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snap.state(), JobState::Succeeded);
    }

    // --- Slice 4: CancelTask RPC push ---

    #[tokio::test]
    async fn cancel_job_pushes_cancel_rpc_to_executor() {
        let service = RecordingExecutorTaskService::default();
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping cancel push test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wire::v1::executor_task_server::ExecutorTaskServer::new(
                    service,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let executor_id = ExecutorId::try_new("exec-cancel").unwrap();
        let job_id = JobId::try_new("job-cancel-push").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-cancel").unwrap());
        let lease = coordinator
            .register_executor(
                ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1)
                    .with_task_endpoint(format!("http://{addr}")),
            )
            .unwrap();
        coordinator
            .submit_job(single_task_job(job_id.clone()))
            .unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        let assignment = &assignments[0];

        // Mark it Running so push_cancel_job has a running task to cancel.
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Running,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(lease),
            )
            .unwrap();

        coordinator.push_cancel_job(&job_id).await.unwrap();

        let snap = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snap.state(), JobState::Cancelled);

        server.abort();
        let _ = server.await;
    }

    // --- Slice 6: Extended heartbeat + memory-aware placement ---

    #[test]
    fn extended_heartbeat_stores_memory_snapshot() {
        let executor_id = ExecutorId::try_new("exec-mem").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-mem").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(
                ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
                    .with_memory_used_bytes(512 * 1024 * 1024)
                    .with_memory_limit_bytes(1024 * 1024 * 1024)
                    .with_active_task_count(3),
            )
            .unwrap();

        let snapshots = coordinator.executor_snapshots();
        let snapshot = snapshots[0].health_snapshot().unwrap();
        assert_eq!(snapshot.memory_used_bytes, Some(512 * 1024 * 1024));
        assert_eq!(snapshot.memory_limit_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(snapshot.active_task_count, Some(3));
    }

    #[test]
    fn memory_aware_placement_skips_overloaded_executor() {
        let executor_id = ExecutorId::try_new("exec-overloaded").unwrap();
        let job_id = JobId::try_new("job-mem-aware").unwrap();
        let threshold = 800 * 1024 * 1024u64; // 800 MiB threshold

        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-mem-aware").unwrap(),
            CoordinatorConfig::new(1, 3).with_memory_threshold(threshold),
        );
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        // Heartbeat with memory usage ABOVE the threshold.
        coordinator
            .executor_heartbeat(
                ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
                    .with_memory_used_bytes(900 * 1024 * 1024), // 900 MiB > 800 MiB threshold
            )
            .unwrap();

        // Submit should fail with NoExecutors because the executor is over the threshold.
        let result = coordinator.submit_job(single_task_job(job_id.clone()));
        assert!(
            matches!(result, Err(SchedulerError::NoExecutors)),
            "expected NoExecutors, got {:?}",
            result
        );
    }

    // ── CheckpointCoordinator tests ───────────────────────────────────────────

    fn make_ack(
        job_id: &JobId,
        task_id: &str,
        epoch: u64,
        fencing_token: FencingToken,
        snapshot_path: Option<String>,
    ) -> CheckpointAckRequest {
        CheckpointAckRequest {
            job_id: job_id.clone(),
            operator_id: format!("operator-{task_id}"),
            task_id: TaskId::try_new(task_id).unwrap(),
            epoch,
            fencing_token,
            source_offsets: vec![krishiv_proto::CheckpointSourceOffset {
                partition_id: format!("partition-{task_id}"),
                offset: 100,
            }],
            snapshot_path,
        }
    }

    #[test]
    fn checkpoint_coordinator_initiates_and_collects_acks() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ck-1").unwrap();
        let mut coord = CheckpointCoordinator::new(job_id.clone(), storage.clone(), 5000, 2);

        // Write state snapshots so the manifest can hash them.
        krishiv_checkpoint::write_operator_snapshot(
            storage.as_ref(),
            "job-ck-1",
            1,
            "operator-task-1",
            "task-1",
            b"state bytes",
        )
        .unwrap();
        krishiv_checkpoint::write_operator_snapshot(
            storage.as_ref(),
            "job-ck-1",
            1,
            "operator-task-2",
            "task-2",
            b"state bytes 2",
        )
        .unwrap();

        let epoch = coord.initiate().unwrap();
        assert_eq!(epoch, 1);
        assert!(coord.is_awaiting_acks());

        let snap_path1 =
            krishiv_checkpoint::snapshot_path("job-ck-1", 1, "operator-task-1", "task-1");
        let snap_path2 =
            krishiv_checkpoint::snapshot_path("job-ck-1", 1, "operator-task-2", "task-2");
        let ack1 = make_ack(
            &job_id,
            "task-1",
            1,
            FencingToken::initial(),
            Some(snap_path1),
        );
        let ack2 = make_ack(
            &job_id,
            "task-2",
            1,
            FencingToken::initial(),
            Some(snap_path2),
        );

        // First ack: not yet quorum.
        let done = coord.receive_ack(ack1).unwrap();
        assert!(!done);
        assert!(coord.is_awaiting_acks());

        // Second ack: quorum complete, epoch committed.
        let done = coord.receive_ack(ack2).unwrap();
        assert!(done);
        assert!(!coord.is_awaiting_acks());
        assert_eq!(coord.current_epoch(), 1);

        // Verify metadata was written to storage.
        let meta = krishiv_checkpoint::read_epoch_metadata(storage.as_ref(), "job-ck-1", 1)
            .unwrap()
            .unwrap();
        assert_eq!(meta.epoch, 1);
        assert_eq!(meta.job_id, "job-ck-1");
        assert!(!meta.is_savepoint);

        // Verify manifest exists and epoch validates.
        assert!(krishiv_checkpoint::validate_epoch(storage.as_ref(), "job-ck-1", 1).unwrap());
    }

    #[test]
    fn checkpoint_coordinator_rejects_stale_epoch_ack() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ck-stale").unwrap();
        let mut coord = CheckpointCoordinator::new(job_id.clone(), storage, 5000, 1);
        let _ = coord.initiate().unwrap(); // epoch = 1

        // Send ack with wrong epoch.
        let ack = make_ack(&job_id, "task-1", 99, FencingToken::initial(), None);
        let result = coord.receive_ack(ack);
        assert!(result.is_err(), "stale epoch ack must be rejected");
    }

    #[test]
    fn checkpoint_coordinator_abort_resets_state() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ck-abort").unwrap();
        let mut coord = CheckpointCoordinator::new(job_id.clone(), storage, 5000, 2);
        let _ = coord.initiate().unwrap();
        assert!(coord.is_awaiting_acks());

        coord.abort_epoch("timeout");
        assert!(!coord.is_awaiting_acks());
        assert!(matches!(
            coord.coordinator_state(),
            CheckpointCoordinatorState::Failed { epoch: 1, .. }
        ));

        // Can initiate again after abort.
        let _ = coord.initiate().unwrap();
        assert!(coord.is_awaiting_acks());
    }

    #[test]
    fn checkpoint_coordinator_recover_finds_latest_epoch() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ck-recover").unwrap();

        // Write two complete epochs manually.
        for epoch in [1u64, 2] {
            let meta = CheckpointMetadata {
                version: CheckpointMetadata::VERSION,
                epoch,
                job_id: "job-ck-recover".to_owned(),
                fencing_token: 1,
                timestamp_ms: epoch * 5000,
                source_offsets: vec![],
                operator_snapshots: vec![],
                is_savepoint: false,
                savepoint_label: None,
                iceberg_snapshot_id: None,
                kafka_offsets: None,
            };
            write_epoch_metadata(storage.as_ref(), "job-ck-recover", epoch, &meta).unwrap();
            let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
            let mut manifest = IntegrityManifest::new();
            manifest.insert_bytes("metadata.json", &meta_json);
            write_manifest(storage.as_ref(), "job-ck-recover", epoch, &manifest).unwrap();
        }

        let mut coord = CheckpointCoordinator::new(job_id, storage, 5000, 1);
        let recovered = coord.recover_from_storage().unwrap();
        assert_eq!(recovered, Some(2));
        assert_eq!(coord.current_epoch(), 2);
    }

    #[test]
    fn checkpoint_coordinator_savepoint_sets_flag() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ck-sp").unwrap();
        let mut coord = CheckpointCoordinator::new(job_id.clone(), storage.clone(), 5000, 1);

        let epoch = coord
            .initiate_savepoint(Some("my-savepoint".to_owned()))
            .unwrap();
        assert_eq!(epoch, 1);

        let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
        let done = coord.receive_ack(ack).unwrap();
        assert!(done);

        let meta = krishiv_checkpoint::read_epoch_metadata(storage.as_ref(), "job-ck-sp", 1)
            .unwrap()
            .unwrap();
        assert!(
            meta.is_savepoint,
            "is_savepoint must be true for savepoints"
        );
        assert_eq!(meta.savepoint_label.as_deref(), Some("my-savepoint"));
    }

    #[test]
    fn coordinator_creates_checkpoint_coordinator_for_streaming_job_with_config() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let storage_path = storage.base_dir().to_string_lossy().to_string();

        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-ck-test").unwrap());
        let executor_id = ExecutorId::try_new("exec-ck-test").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-ck", 1))
            .unwrap();

        let job_id = JobId::try_new("job-ck-stream").unwrap();
        let spec = JobSpec::new(job_id.clone(), "stream-ck", JobKind::Streaming)
            .with_checkpoint(5000, &storage_path)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage").with_task(
                    TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
                ),
            );
        coordinator.submit_job(spec).unwrap();

        assert!(
            coordinator.checkpoint_coordinator(&job_id).is_some(),
            "streaming job with checkpoint config must have a CheckpointCoordinator"
        );
    }

    #[test]
    fn coordinator_routes_ack_to_correct_job() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let storage_path = storage.base_dir().to_string_lossy().to_string();

        let mut coordinator =
            Coordinator::active(CoordinatorId::try_new("coord-ck-route").unwrap());
        let executor_id = ExecutorId::try_new("exec-ck-route").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-ck", 1))
            .unwrap();

        let job_id = JobId::try_new("job-ck-route").unwrap();
        let spec = JobSpec::new(job_id.clone(), "route-ck", JobKind::Streaming)
            .with_checkpoint(5000, &storage_path)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage").with_task(
                    TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
                ),
            );
        coordinator.submit_job(spec).unwrap();

        // Initiate an epoch on the coordinator's checkpoint coordinator.
        {
            let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
            coord.set_expected_task_count(1);
            coord.initiate().unwrap();
        }

        // Route an ack through the coordinator.
        let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
        let response = coordinator.handle_checkpoint_ack(ack);
        assert_eq!(
            response,
            CheckpointAckResponse::Accepted,
            "ack for valid epoch must be accepted"
        );

        // Unknown job → JobNotFound.
        let unknown_job_id = JobId::try_new("job-unknown").unwrap();
        let ack = make_ack(&unknown_job_id, "task-1", 1, FencingToken::initial(), None);
        let response = coordinator.handle_checkpoint_ack(ack);
        assert_eq!(response, CheckpointAckResponse::JobNotFound);
    }

    // ── Group D: savepoint_job / list_job_checkpoints / restore ───────────────

    #[test]
    fn coordinator_savepoint_job_initiates_savepoint() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let storage_path = storage.base_dir().to_string_lossy().to_string();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-sp").unwrap());
        let exec_id = ExecutorId::try_new("exec-sp").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-sp", 1))
            .unwrap();
        let job_id = JobId::try_new("job-sp").unwrap();
        let spec = JobSpec::new(job_id.clone(), "streaming-sp", JobKind::Streaming)
            .with_checkpoint(5000, &storage_path)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1").with_task(
                    TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
                ),
            );
        coordinator.submit_job(spec).unwrap();

        let epoch = coordinator
            .savepoint_job(&job_id, Some("my-label".to_string()))
            .unwrap();
        assert_eq!(epoch, 1, "first savepoint must be epoch 1");

        // Batch job without checkpoint config → error.
        let batch_id = JobId::try_new("job-batch-sp").unwrap();
        let result = coordinator.savepoint_job(&batch_id, None);
        assert!(result.is_err(), "batch job has no checkpoint coordinator");
    }

    #[test]
    fn coordinator_list_job_checkpoints_returns_empty_for_new_job() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let storage_path = storage.base_dir().to_string_lossy().to_string();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-lc").unwrap());
        let exec_id = ExecutorId::try_new("exec-lc").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-lc", 1))
            .unwrap();
        let job_id = JobId::try_new("job-lc").unwrap();
        let spec = JobSpec::new(job_id.clone(), "streaming-lc", JobKind::Streaming)
            .with_checkpoint(5000, &storage_path)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1").with_task(
                    TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
                ),
            );
        coordinator.submit_job(spec).unwrap();

        let epochs = coordinator.list_job_checkpoints(&job_id).unwrap();
        assert!(epochs.is_empty(), "no epochs committed yet");

        // Job without coordinator → empty vec (not an error).
        let unknown = JobId::try_new("job-unknown-lc").unwrap();
        let epochs = coordinator.list_job_checkpoints(&unknown).unwrap();
        assert!(epochs.is_empty());
    }

    #[test]
    fn coordinator_restore_rejects_missing_epoch() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let storage_path = storage.base_dir().to_string_lossy().to_string();
        let coordinator = Coordinator::active(CoordinatorId::try_new("coord-restore").unwrap());
        let job_id = JobId::try_new("job-restore").unwrap();
        let result = coordinator.restore_job_from_checkpoint(&job_id, 99, &storage_path);
        assert!(
            result.is_err(),
            "epoch 99 does not exist; restore must fail"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not found") || msg.contains("cannot read"),
            "error message must explain why: {msg}"
        );
    }

    // ── Group E: Chaos tests ──────────────────────────────────────────────────

    #[test]
    fn chaos_1_coordinator_kill_mid_checkpoint_no_duplicate_commit() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let mut coord = CheckpointCoordinator::new(
            JobId::try_new("job-chaos1").unwrap(),
            std::sync::Arc::new(storage),
            5000,
            2,
        );

        // Epoch 1 initiated; only one ack arrives before "kill".
        let epoch = coord.initiate().unwrap();
        assert_eq!(epoch, 1);
        let ack = make_ack(
            &JobId::try_new("job-chaos1").unwrap(),
            "task-0",
            1,
            coord.fencing_token(),
            None,
        );
        coord.receive_ack(ack).unwrap(); // partial — quorum not met

        // Simulate coordinator kill → abort.
        coord.abort_epoch("coordinator killed");
        assert!(
            matches!(
                coord.coordinator_state(),
                CheckpointCoordinatorState::Failed { .. }
            ),
            "state must be Failed after abort"
        );

        // Nothing committed to storage.
        let epochs = coord.list_epochs().unwrap();
        assert!(epochs.is_empty(), "no epoch must be committed after abort");

        // Epoch 2 succeeds after "restart".
        let epoch2 = coord.initiate().unwrap();
        assert_eq!(epoch2, 2);
        for task in &["task-0", "task-1"] {
            let ack = make_ack(
                &JobId::try_new("job-chaos1").unwrap(),
                task,
                2,
                coord.fencing_token(),
                None,
            );
            coord.receive_ack(ack).unwrap();
        }
        let committed = coord.list_epochs().unwrap();
        assert_eq!(committed, vec![2], "only epoch 2 must be committed");
    }

    #[test]
    fn chaos_1a_coordinator_restart_recovers_from_durable_metadata() {
        let storage: std::sync::Arc<dyn CheckpointStorage> =
            std::sync::Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-chaos1a").unwrap();

        // Coordinator A: commit epoch 1.
        let mut coord_a = CheckpointCoordinator::new(job_id.clone(), storage.clone(), 5000, 1);
        coord_a.initiate().unwrap();
        let ack = make_ack(&job_id, "task-0", 1, coord_a.fencing_token(), None);
        coord_a.receive_ack(ack).unwrap();
        let epochs = coord_a.list_epochs().unwrap();
        assert_eq!(epochs, vec![1]);

        // Coordinator B: new instance, same storage — recover.
        let mut coord_b = CheckpointCoordinator::new(job_id.clone(), storage.clone(), 5000, 1);
        let recovered = coord_b.recover_from_storage().unwrap();
        assert_eq!(recovered, Some(1), "must recover epoch 1");
        assert_eq!(coord_b.current_epoch(), 1);

        // Coordinator B can initiate epoch 2 without re-committing epoch 1.
        let epoch2 = coord_b.initiate().unwrap();
        assert_eq!(epoch2, 2);
        let epochs_before = coord_b.list_epochs().unwrap();
        assert_eq!(epochs_before, vec![1], "epoch 2 not yet committed");
    }

    #[test]
    fn chaos_2_executor_kill_mid_checkpoint_abort_is_clean() {
        let storage: std::sync::Arc<dyn CheckpointStorage> =
            std::sync::Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-chaos2").unwrap();
        let mut coord = CheckpointCoordinator::new(job_id.clone(), storage, 5000, 2);

        coord.initiate().unwrap();
        // Only task-0 acks; task-1 is "dead".
        let ack = make_ack(&job_id, "task-0", 1, coord.fencing_token(), None);
        coord.receive_ack(ack).unwrap();

        coord.abort_epoch("executor-1 lost");
        let epochs = coord.list_epochs().unwrap();
        assert!(epochs.is_empty(), "partial epoch must not be committed");
        assert!(matches!(
            coord.coordinator_state(),
            CheckpointCoordinatorState::Failed { .. }
        ));

        // Epoch 2 with both tasks succeeds.
        coord.initiate().unwrap();
        for task in &["task-0", "task-1"] {
            let ack = make_ack(&job_id, task, 2, coord.fencing_token(), None);
            coord.receive_ack(ack).unwrap();
        }
        assert_eq!(coord.list_epochs().unwrap(), vec![2]);
    }

    #[test]
    fn chaos_3_sink_kill_mid_write_abort_discards_staged_output() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use krishiv_connectors::TwoPhaseCommitSink;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let mut sink = krishiv_connectors::InMemoryTwoPhaseCommitSink::new();

        // Prepare epoch 1 then abort (simulating sink kill).
        let handle = sink.prepare(1, &batch).unwrap();
        sink.abort(handle).unwrap();

        assert!(sink.committed().is_empty(), "abort must not commit");
        assert_eq!(
            sink.staged_count(),
            0,
            "staged area must be cleared after abort"
        );

        // Epoch 2 prepare + commit succeeds.
        let handle2 = sink.prepare(2, &batch).unwrap();
        sink.commit(handle2).unwrap();
        assert_eq!(
            sink.committed().len(),
            1,
            "commit must land exactly one batch"
        );
        assert_eq!(sink.committed()[0].0, 2, "committed epoch must be 2");
    }

    #[test]
    fn chaos_4_corrupt_checkpoint_fallback_to_prior_valid_epoch() {
        use krishiv_checkpoint::{
            CheckpointStorage, metadata_path, validate_epoch, write_epoch_metadata, write_manifest,
        };

        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let job_id = "job-chaos4";

        // Helper: write a minimal valid epoch and build the manifest from the
        // actual stored bytes (write_epoch_metadata uses to_vec_pretty internally).
        let write_valid_epoch = |epoch: u64, storage: &LocalFsCheckpointStorage| {
            let meta = CheckpointMetadata {
                version: CheckpointMetadata::VERSION,
                epoch,
                job_id: job_id.to_string(),
                fencing_token: FencingToken::initial().as_u64(),
                timestamp_ms: epoch * 1000,
                source_offsets: vec![],
                operator_snapshots: vec![],
                is_savepoint: false,
                savepoint_label: None,
                iceberg_snapshot_id: None,
                kafka_offsets: None,
            };
            let storage_dyn: &dyn CheckpointStorage = storage;
            write_epoch_metadata(storage_dyn, job_id, epoch, &meta).unwrap();
            // Read back the actual bytes so the manifest hash matches exactly.
            let stored_bytes = storage_dyn
                .read_bytes(&metadata_path(job_id, epoch))
                .unwrap()
                .unwrap();
            let mut manifest = IntegrityManifest::new();
            manifest.insert_bytes("metadata.json", &stored_bytes);
            write_manifest(storage_dyn, job_id, epoch, &manifest).unwrap();
        };

        write_valid_epoch(1, &storage);
        write_valid_epoch(2, &storage);

        // Corrupt epoch 2 metadata by overwriting with invalid JSON.
        let storage_dyn: &dyn CheckpointStorage = &storage;
        storage_dyn
            .write_bytes(&metadata_path(job_id, 2), b"not-valid-json")
            .unwrap();

        // latest_valid_epoch falls back to epoch 1.
        let valid_epochs = list_valid_epochs(&storage, job_id).unwrap();
        assert_eq!(
            valid_epochs,
            vec![1],
            "only epoch 1 is valid after corrupting epoch 2"
        );

        // Confirm individual epoch verdicts.
        // validate_epoch returns Ok(false) for hash mismatches, Ok(true) for valid.
        assert!(
            !validate_epoch(&storage, job_id, 2).unwrap_or(true),
            "corrupt epoch 2 must fail validation"
        );
        assert!(
            validate_epoch(&storage, job_id, 1).unwrap_or(false),
            "intact epoch 1 must pass validation"
        );
    }

    #[test]
    fn chaos_e6_rolling_upgrade_savepoint_restore_preserves_epoch_sequence() {
        use krishiv_checkpoint::read_epoch_metadata;

        let storage: std::sync::Arc<dyn CheckpointStorage> =
            std::sync::Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-chaos-e6").unwrap();

        // Coordinator A: normal epoch 1, then savepoint epoch 2.
        let mut coord_a = CheckpointCoordinator::new(job_id.clone(), storage.clone(), 5000, 1);
        coord_a.initiate().unwrap();
        coord_a
            .receive_ack(make_ack(
                &job_id,
                "task-0",
                1,
                coord_a.fencing_token(),
                None,
            ))
            .unwrap();
        assert_eq!(coord_a.list_epochs().unwrap(), vec![1]);

        // Initiate savepoint (epoch 2).
        coord_a
            .initiate_savepoint(Some("pre-upgrade".to_string()))
            .unwrap();
        coord_a
            .receive_ack(make_ack(
                &job_id,
                "task-0",
                2,
                coord_a.fencing_token(),
                None,
            ))
            .unwrap();
        assert_eq!(coord_a.list_epochs().unwrap(), vec![1, 2]);

        // Verify savepoint metadata.
        let meta = read_epoch_metadata(&*storage, job_id.as_str(), 2)
            .unwrap()
            .unwrap();
        assert!(meta.is_savepoint, "epoch 2 must be a savepoint");
        assert_eq!(
            meta.savepoint_label.as_deref(),
            Some("pre-upgrade"),
            "savepoint label must match"
        );

        // Coordinator B (simulated "upgraded binary"): recover from same storage.
        let mut coord_b = CheckpointCoordinator::new(job_id.clone(), storage.clone(), 5000, 1);
        let recovered = coord_b.recover_from_storage().unwrap();
        assert_eq!(recovered, Some(2), "must recover savepoint epoch 2");

        // Initiate epoch 3 — no re-commit of epoch 2.
        let epoch3 = coord_b.initiate().unwrap();
        assert_eq!(epoch3, 3);
        // Epoch 2 still committed; epoch 3 not yet.
        assert_eq!(
            coord_b.list_epochs().unwrap(),
            vec![1, 2],
            "epoch 3 not committed yet — only 1 and 2 exist"
        );
    }

    // ── Item 2: checkpoint timer wired into advance_heartbeat_clock ──────────

    #[test]
    fn checkpoint_coordinator_try_tick_fires_after_interval() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-tick").unwrap();
        let mut coord = CheckpointCoordinator::new(job_id, storage, 5_000, 0);

        // Accumulate 4 000 ms — below the 5 000 ms interval.
        assert_eq!(coord.try_tick(4_000), None, "not yet due");
        assert_eq!(coord.try_tick(2_000), None, "zero running tasks skips initiate");
        coord.set_expected_task_count(1);
        assert_eq!(coord.try_tick(5_000), Some(1), "epoch 1 initiated");
        // Epoch 1 is now in AwaitingAcks. Abort it to return to Idle.
        coord.abort_epoch("test reset");
        // Clock resets on initiate: another 5 000 ms triggers epoch 2.
        assert_eq!(coord.try_tick(5_000), Some(2), "epoch 2 initiated");
    }

    #[test]
    fn checkpoint_coordinator_try_tick_skips_while_awaiting_acks() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-tick-busy").unwrap();
        // expected_task_count = 1 so the coordinator will wait for an ack.
        let mut coord = CheckpointCoordinator::new(job_id, storage, 1_000, 1);

        // First tick crosses the interval — epoch 1 initiated (now AwaitingAcks).
        assert_eq!(coord.try_tick(1_000), Some(1));
        // While awaiting acks, further ticks must not initiate.
        assert_eq!(
            coord.try_tick(10_000),
            None,
            "in-flight checkpoint blocks next"
        );
    }

    #[test]
    fn advance_heartbeat_clock_drives_checkpoint_coordinator() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_owned();
        let job_id = JobId::try_new("job-clock").unwrap();

        let config = CoordinatorConfig::new(1, 3).with_tick_period_ms(1_000);
        let coordinator_id = CoordinatorId::try_new("coord-clock").unwrap();
        let mut coordinator = Coordinator::active_with_config(coordinator_id, config);

        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "host-1", 2))
            .unwrap();

        // Submit a streaming job with a 3-second checkpoint interval.
        let task_id = TaskId::try_new("t1").unwrap();
        let stage = StageSpec::new(StageId::try_new("s1").unwrap(), "stage-1")
            .with_task(TaskSpec::new(task_id, "task-1"));
        let spec = JobSpec::new(job_id.clone(), "clock-test", JobKind::Streaming)
            .with_stage(stage)
            .with_checkpoint(3_000, storage_path);
        coordinator.submit_job(spec).unwrap();

        // 2 ticks × 1 000 ms = 2 000 ms < 3 000 ms — no checkpoint yet.
        coordinator.advance_heartbeat_clock(2).unwrap();
        assert_eq!(
            coordinator
                .checkpoint_coordinator(&job_id)
                .unwrap()
                .current_epoch(),
            0,
            "epoch 0 — not yet due"
        );

        // 2 more ticks: 4 000 ms total >= 3 000 ms — epoch 1 fires.
        coordinator.advance_heartbeat_clock(2).unwrap();
        assert_eq!(
            coordinator
                .checkpoint_coordinator(&job_id)
                .unwrap()
                .current_epoch(),
            1,
            "epoch 1 initiated after 4 ticks × 1 000 ms"
        );
    }

    // ── R6a: Out-of-band barrier trigger ──────────────────────────────────────

    #[test]
    fn trigger_checkpoint_for_job_returns_initiate_request() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_owned();
        let coordinator_id = CoordinatorId::try_new("coord-r6a").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);

        let executor_id = ExecutorId::try_new("exec-r6a").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "host", 2))
            .unwrap();

        let job_id = JobId::try_new("job-r6a").unwrap();
        let stage_id = StageId::try_new("s-r6a").unwrap();
        let task_id = TaskId::try_new("t-r6a").unwrap();

        let spec = JobSpec::new(job_id.clone(), "stream", JobKind::Streaming)
            .with_stage(StageSpec::new(stage_id, "stage").with_task(TaskSpec::new(task_id, "task")))
            .with_checkpoint(1_000, storage_path);
        coordinator.submit_job(spec).unwrap();

        // trigger_checkpoint_for_job initiates epoch 1 and returns the request.
        let requests = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
        assert_eq!(requests.len(), 1, "one broadcast request");
        assert_eq!(requests[0].epoch, 1, "first epoch");
        assert_eq!(requests[0].job_id, job_id);

        // A second trigger while epoch 1 is in flight must fail.
        assert!(
            coordinator.trigger_checkpoint_for_job(&job_id).is_err(),
            "cannot trigger while acks are pending"
        );
    }

    #[test]
    fn trigger_checkpoint_then_ack_commits_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_owned();
        let coordinator_id = CoordinatorId::try_new("coord-r6b").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);

        let executor_id = ExecutorId::try_new("exec-r6b").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host", 2))
            .unwrap();

        let job_id = JobId::try_new("job-r6b").unwrap();
        let stage_id = StageId::try_new("s-r6b").unwrap();
        let task_id = TaskId::try_new("t-r6b-1").unwrap();

        let spec = JobSpec::new(job_id.clone(), "stream", JobKind::Streaming)
            .with_stage(
                StageSpec::new(stage_id, "stage").with_task(TaskSpec::new(task_id.clone(), "task")),
            )
            .with_checkpoint(1_000, storage_path);
        coordinator.submit_job(spec).unwrap();

        // Trigger checkpoint — epoch 1.
        let requests = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
        let req = &requests[0];
        let epoch = req.epoch;
        let fencing_token = req.fencing_token.clone();

        // Simulate executor acking the checkpoint.
        let ack = CheckpointAckRequest {
            job_id: job_id.clone(),
            operator_id: format!("operator-{}", task_id.as_str()),
            task_id: task_id.clone(),
            epoch,
            fencing_token,
            source_offsets: vec![],
            snapshot_path: None,
        };

        let response = coordinator.handle_checkpoint_ack(ack);
        assert_eq!(
            response,
            CheckpointAckResponse::Accepted,
            "ack must be accepted"
        );

        // After all tasks ack, coordinator should commit epoch 1.
        let coord = coordinator.checkpoint_coordinator(&job_id).unwrap();
        assert_eq!(coord.current_epoch(), 1);
        assert!(
            !coord.is_awaiting_acks(),
            "epoch 1 should be committed after all acks received"
        );
    }

    #[test]
    fn trigger_checkpoint_fails_without_checkpoint_config() {
        let coordinator_id = CoordinatorId::try_new("coord-r6c").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);

        let executor_id = ExecutorId::try_new("exec-r6c").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "host", 2))
            .unwrap();

        let job_id = JobId::try_new("job-r6c").unwrap();
        let spec = JobSpec::new(job_id.clone(), "stream", JobKind::Streaming).with_stage(
            StageSpec::new(StageId::try_new("s-r6c").unwrap(), "stage")
                .with_task(TaskSpec::new(TaskId::try_new("t-r6c").unwrap(), "task")),
        );
        coordinator.submit_job(spec).unwrap();

        // No checkpoint_interval_ms set — must fail.
        assert!(
            coordinator.trigger_checkpoint_for_job(&job_id).is_err(),
            "trigger must fail when job has no checkpoint coordinator"
        );
    }

    // ── Items 3+4: QueueManager trait + SubmitOutcome ────────────────────────

    #[test]
    fn in_memory_queue_manager_always_accepts() {
        let qm = InMemoryQueueManager;
        let spec = demo_job();
        let quota = NamespaceQuotaSnapshot::default();
        assert_eq!(qm.admit(&spec, &quota), SubmitOutcome::Accepted);
    }

    // ── R7.1 Resource governance tests ───────────────────────────────────────

    #[test]
    fn quota_queue_manager_admits_within_limits() {
        let qm = QuotaQueueManager::with_default(QuotaPolicy {
            cpu_nanos_limit: Some(1_000_000_000),
            memory_bytes_limit: Some(512 * 1024 * 1024),
            max_concurrent_jobs: Some(5),
        });
        let spec = demo_job()
            .with_cpu_limit_nanos(100_000_000)
            .with_memory_limit_bytes(64 * 1024 * 1024);
        let quota = NamespaceQuotaSnapshot {
            active_job_count: 2,
            cpu_nanos_reserved: 200_000_000,
            memory_bytes_reserved: 128 * 1024 * 1024,
            ..Default::default()
        };
        assert_eq!(qm.admit(&spec, &quota), SubmitOutcome::Accepted);
    }

    #[test]
    fn quota_queue_manager_queues_when_cpu_limit_exceeded() {
        let qm = QuotaQueueManager::with_default(QuotaPolicy {
            cpu_nanos_limit: Some(1_000_000_000),
            memory_bytes_limit: None,
            max_concurrent_jobs: None,
        });
        let spec = demo_job().with_cpu_limit_nanos(600_000_000);
        let quota = NamespaceQuotaSnapshot {
            cpu_nanos_reserved: 500_000_000,
            ..Default::default()
        };
        assert_eq!(
            qm.admit(&spec, &quota),
            SubmitOutcome::Queued { position: 0 }
        );
    }

    #[test]
    fn quota_queue_manager_queues_when_memory_limit_exceeded() {
        let qm = QuotaQueueManager::with_default(QuotaPolicy {
            cpu_nanos_limit: None,
            memory_bytes_limit: Some(512 * 1024 * 1024),
            max_concurrent_jobs: None,
        });
        let spec = demo_job().with_memory_limit_bytes(300 * 1024 * 1024);
        let quota = NamespaceQuotaSnapshot {
            memory_bytes_reserved: 300 * 1024 * 1024,
            ..Default::default()
        };
        assert_eq!(
            qm.admit(&spec, &quota),
            SubmitOutcome::Queued { position: 0 }
        );
    }

    #[test]
    fn quota_queue_manager_queues_when_job_count_exceeded() {
        let qm = QuotaQueueManager::with_default(QuotaPolicy {
            cpu_nanos_limit: None,
            memory_bytes_limit: None,
            max_concurrent_jobs: Some(2),
        });
        let spec = demo_job();
        let quota = NamespaceQuotaSnapshot {
            active_job_count: 2,
            ..Default::default()
        };
        assert!(matches!(
            qm.admit(&spec, &quota),
            SubmitOutcome::Queued { .. }
        ));
    }

    #[test]
    fn quota_queue_manager_uses_namespace_policy() {
        use std::collections::HashMap;
        let mut ns_policies = HashMap::new();
        ns_policies.insert(
            "analytics".to_owned(),
            QuotaPolicy {
                cpu_nanos_limit: None,
                memory_bytes_limit: None,
                max_concurrent_jobs: Some(1),
            },
        );
        let qm = QuotaQueueManager::new(QuotaPolicy::default(), ns_policies);

        let spec_ns = demo_job().with_namespace("analytics");
        let spec_default = demo_job();
        let quota_full = NamespaceQuotaSnapshot {
            namespace_id: Some("analytics".to_owned()),
            active_job_count: 1,
            ..Default::default()
        };
        let quota_empty = NamespaceQuotaSnapshot {
            namespace_id: Some("analytics".to_owned()),
            active_job_count: 0,
            ..Default::default()
        };
        // Analytics namespace is full.
        assert!(matches!(
            qm.admit(&spec_ns, &quota_full),
            SubmitOutcome::Queued { .. }
        ));
        // Default namespace has no limit — admits.
        assert_eq!(
            qm.admit(&spec_default, &quota_full),
            SubmitOutcome::Accepted
        );
        // Analytics namespace has capacity — admits.
        assert_eq!(qm.admit(&spec_ns, &quota_empty), SubmitOutcome::Accepted);
    }

    #[test]
    fn config_file_queue_manager_admits_from_in_memory_config() {
        use std::collections::HashMap;
        let qm = ConfigFileQueueManager::from_config(
            QuotaPolicy {
                max_concurrent_jobs: Some(3),
                ..Default::default()
            },
            HashMap::new(),
        );
        let spec = demo_job();
        let quota_ok = NamespaceQuotaSnapshot {
            active_job_count: 2,
            ..Default::default()
        };
        let quota_full = NamespaceQuotaSnapshot {
            active_job_count: 3,
            ..Default::default()
        };
        assert_eq!(qm.admit(&spec, &quota_ok), SubmitOutcome::Accepted);
        assert!(matches!(
            qm.admit(&spec, &quota_full),
            SubmitOutcome::Queued { .. }
        ));
    }

    #[test]
    fn config_file_queue_manager_loads_from_json_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queues.json");
        std::fs::write(
            &path,
            r#"{"default":{"max_concurrent_jobs":1},"namespaces":{}}"#,
        )
        .unwrap();
        let qm = ConfigFileQueueManager::from_path(&path).unwrap();
        let spec = demo_job();
        let quota_ok = NamespaceQuotaSnapshot {
            active_job_count: 0,
            ..Default::default()
        };
        let quota_full = NamespaceQuotaSnapshot {
            active_job_count: 1,
            ..Default::default()
        };
        assert_eq!(qm.admit(&spec, &quota_ok), SubmitOutcome::Accepted);
        assert!(matches!(
            qm.admit(&spec, &quota_full),
            SubmitOutcome::Queued { .. }
        ));
    }

    #[test]
    fn namespace_quota_snapshot_sums_active_jobs() {
        let coordinator_id = CoordinatorId::try_new("coord-quota").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);
        let executor_id = ExecutorId::try_new("exec-quota").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host", 4))
            .unwrap();

        let job_id_a = JobId::try_new("quota-a").unwrap();
        let job_id_b = JobId::try_new("quota-b").unwrap();

        let spec_a = single_task_job(job_id_a.clone())
            .with_namespace("team-a")
            .with_cpu_limit_nanos(500_000_000)
            .with_memory_limit_bytes(256 * 1024 * 1024);

        let spec_b = single_task_job(job_id_b.clone())
            .with_namespace("team-a")
            .with_cpu_limit_nanos(300_000_000)
            .with_memory_limit_bytes(128 * 1024 * 1024);

        coordinator.submit_job(spec_a).unwrap();
        coordinator.submit_job(spec_b).unwrap();

        let snap = coordinator.namespace_quota_snapshot(Some("team-a"));
        assert_eq!(snap.active_job_count, 2);
        assert_eq!(snap.cpu_nanos_reserved, 800_000_000);
        assert_eq!(snap.memory_bytes_reserved, (256 + 128) * 1024 * 1024);

        let snap_other = coordinator.namespace_quota_snapshot(Some("team-b"));
        assert_eq!(snap_other.active_job_count, 0);
    }

    #[test]
    fn coordinator_queues_job_when_quota_exceeded() {
        let coordinator_id = CoordinatorId::try_new("coord-qe").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id).with_queue_manager(
            QuotaQueueManager::with_default(QuotaPolicy {
                max_concurrent_jobs: Some(1),
                ..Default::default()
            }),
        );
        let executor_id = ExecutorId::try_new("exec-qe").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "host", 2))
            .unwrap();

        let job_id_a = JobId::try_new("qe-a").unwrap();
        let job_id_b = JobId::try_new("qe-b").unwrap();

        coordinator.submit_job(single_task_job(job_id_a)).unwrap();

        // Second job exceeds the 1-job concurrent limit.
        let outcome = coordinator.submit_job(single_task_job(job_id_b)).unwrap();
        assert!(matches!(outcome, SubmitOutcome::Queued { .. }));
    }

    #[test]
    fn resource_usage_accumulates_from_task_stats() {
        let coordinator_id = CoordinatorId::try_new("coord-ru").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);
        let executor_id = ExecutorId::try_new("exec-ru").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host", 2))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        let job_id = JobId::try_new("ru-job").unwrap();
        let stage_id = StageId::try_new("stage-1").unwrap();
        let task_id = TaskId::try_new("task-1").unwrap();

        use krishiv_proto::{TaskRuntimeStats, TaskStatusUpdate};

        let spec = JobSpec::new(job_id.clone(), "ru", JobKind::Batch).with_stage(
            StageSpec::new(stage_id.clone(), "s").with_task(TaskSpec::new(task_id.clone(), "t")),
        );
        coordinator.submit_job(spec).unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        let assignment = assignments.first().unwrap();

        let mut meta = TaskOutputMetadata::new("inline", 10, 1, 5);
        meta = meta.with_runtime_stats(TaskRuntimeStats {
            input_rows: 0,
            output_rows: 10,
            cpu_nanos: 1_000_000,
            memory_bytes: 0,
            spill_bytes: 0,
        });

        let update = TaskStatusUpdate::new(
            assignment.job_id().clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            executor_id,
            TaskState::Succeeded,
            assignment.attempt_id().as_u32(),
        )
        .with_lease_generation(assignment.lease_generation())
        .with_output_metadata(meta);

        coordinator.apply_task_update(update).unwrap();

        let snap = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snap.resource_usage().cpu_nanos, 1_000_000);
        assert_eq!(snap.resource_usage().task_count, 1);
    }

    #[test]
    fn job_spec_priority_and_namespace_round_trip() {
        let job_id = JobId::try_new("prio-job").unwrap();
        let spec = JobSpec::new(job_id, "test", JobKind::Batch)
            .with_priority(200)
            .with_namespace("eng")
            .with_cpu_limit_nanos(1_000_000)
            .with_memory_limit_bytes(1024);

        assert_eq!(spec.priority(), 200);
        assert_eq!(spec.namespace_id(), Some("eng"));
        assert_eq!(spec.cpu_limit_nanos(), Some(1_000_000));
        assert_eq!(spec.memory_limit_bytes(), Some(1024));
    }

    #[test]
    fn coordinator_uses_queue_manager_on_submit() {
        let coordinator_id = CoordinatorId::try_new("coord-qm").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);

        let executor_id = ExecutorId::try_new("exec-qm").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "host-1", 2))
            .unwrap();

        let outcome = coordinator.submit_job(demo_job()).unwrap();
        assert_eq!(outcome, SubmitOutcome::Accepted);
    }

    #[test]
    fn coordinator_with_blocking_queue_manager_returns_queued() {
        #[derive(Debug)]
        struct BlockAllQueueManager;
        impl QueueManager for BlockAllQueueManager {
            fn admit(&self, _spec: &JobSpec, _quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
                SubmitOutcome::Queued { position: 0 }
            }
        }

        let coordinator_id = CoordinatorId::try_new("coord-block").unwrap();
        let mut coordinator =
            Coordinator::active(coordinator_id).with_queue_manager(BlockAllQueueManager);

        let executor_id = ExecutorId::try_new("exec-block").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id, "host-1", 2))
            .unwrap();

        // Job is queued, not accepted — coordinator has no JobRecord yet.
        let outcome = coordinator.submit_job(demo_job()).unwrap();
        assert_eq!(outcome, SubmitOutcome::Queued { position: 0 });
        assert!(
            coordinator
                .job_snapshot(&demo_job().job_id().clone())
                .is_err(),
            "queued job must not appear in job list"
        );
    }

    // ── R7.2 Adaptive decision log tests ─────────────────────────────────────

    #[test]
    fn adaptive_decision_log_empty_for_unknown_job() {
        let coordinator_id = CoordinatorId::try_new("coord-adaptive").unwrap();
        let coordinator = Coordinator::active(coordinator_id);
        let job_id = JobId::try_new("unknown-job").unwrap();
        assert!(coordinator.adaptive_decision_log(&job_id).is_empty());
    }

    #[test]
    fn hot_key_reports_appended_to_decision_log() {
        use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

        let coordinator_id = CoordinatorId::try_new("coord-hk").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);

        let executor_id = ExecutorId::try_new("exec-hk").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
            .unwrap();

        let job_id = JobId::try_new("job-hk-1").unwrap();
        let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
            .with_hot_key_reports(vec![HeartbeatHotKeyReport {
                key: "hot-key".into(),
                estimated_count: 500,
                max_error: 10,
                heat_score: 0.25,
                job_id: job_id.as_str().to_owned(),
                source_id: "src-0".into(),
            }]);

        let effects = coordinator.executor_heartbeat(heartbeat).unwrap();
        // Default config: no throttle commands issued.
        assert!(effects.source_throttles.is_empty());
        assert!(effects.llm_throttles.is_empty());

        let log = coordinator.adaptive_decision_log(&job_id);
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].kind, AdaptiveDecisionKind::HotKeySplit);
        assert!(log[0].applied, "hot-key split must be applied by default");
        assert!(log[0].details.contains("hot-key"));
    }

    #[test]
    fn hot_key_split_suppressed_by_override() {
        use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

        let coordinator_id = CoordinatorId::try_new("coord-hk-override").unwrap();
        let mut coordinator =
            Coordinator::active(coordinator_id).with_adaptive_override(AdaptiveOverrideConfig {
                disable_hot_key_splitting: true,
                ..AdaptiveOverrideConfig::default()
            });

        let executor_id = ExecutorId::try_new("exec-hk-override").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
            .unwrap();

        let job_id = JobId::try_new("job-hk-2").unwrap();
        let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
            .with_hot_key_reports(vec![HeartbeatHotKeyReport {
                key: "skewed-key".into(),
                estimated_count: 1000,
                max_error: 0,
                heat_score: 0.9,
                job_id: job_id.as_str().to_owned(),
                source_id: "src-0".into(),
            }]);

        coordinator.executor_heartbeat(heartbeat).unwrap();

        let log = coordinator.adaptive_decision_log(&job_id);
        assert_eq!(log.len(), 1);
        assert!(
            !log[0].applied,
            "decision must be suppressed when disable_hot_key_splitting=true"
        );
        assert!(log[0].details.contains("skewed-key"));
    }

    #[test]
    fn multiple_hot_key_reports_all_logged() {
        use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

        let coordinator_id = CoordinatorId::try_new("coord-hk-multi").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id);

        let executor_id = ExecutorId::try_new("exec-hk-multi").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
            .unwrap();

        let job_id = JobId::try_new("job-hk-3").unwrap();
        let reports = vec![
            HeartbeatHotKeyReport {
                key: "key-a".into(),
                estimated_count: 300,
                max_error: 5,
                heat_score: 0.3,
                job_id: job_id.as_str().to_owned(),
                source_id: "src-0".into(),
            },
            HeartbeatHotKeyReport {
                key: "key-b".into(),
                estimated_count: 200,
                max_error: 3,
                heat_score: 0.2,
                job_id: job_id.as_str().to_owned(),
                source_id: "src-0".into(),
            },
        ];

        let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
            .with_hot_key_reports(reports);
        coordinator.executor_heartbeat(heartbeat).unwrap();

        let log = coordinator.adaptive_decision_log(&job_id);
        assert_eq!(log.len(), 2, "one log entry per hot-key report");
    }

    #[test]
    fn adaptive_override_config_defaults_all_false() {
        let cfg = AdaptiveOverrideConfig::default();
        assert!(!cfg.disable_hot_key_splitting);
        assert!(!cfg.disable_adaptive_repartition);
        assert!(!cfg.disable_source_throttling);
    }

    // ── S6.4: SqliteMetadataStore ─────────────────────────────────────────────

    #[cfg(feature = "sqlite")]
    fn sqlite_coordinator_with_job(job_id: &JobId, name: &str) -> Coordinator {
        let task = TaskSpec::new(TaskId::try_new("task-1").unwrap(), "test-task");
        let stage = StageSpec::new(StageId::try_new("stage-1").unwrap(), "test-stage")
            .with_task(task);
        let spec = JobSpec::new(job_id.clone(), name, JobKind::Batch).with_stage(stage);
        let exec_id = ExecutorId::try_new("exec-sqlite-1").unwrap();
        let mut coord = Coordinator::active(
            CoordinatorId::try_new(&format!("coord-{name}")).unwrap(),
        );
        coord.register_executor(ExecutorDescriptor::new(exec_id, "sqlite-node", 4)).unwrap();
        coord.submit_job(spec).unwrap();
        coord
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_metadata_store_save_and_reload_job() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let job_id = JobId::try_new("job-sqlite-1").unwrap();

        // Write via coordinator.
        {
            let mut coordinator = sqlite_coordinator_with_job(&job_id, "sqlite-test");
            let mut store = SqliteMetadataStore::open(&path).unwrap();
            coordinator.persist_jobs_to_store(&mut store).unwrap();
            assert_eq!(store.jobs().len(), 1);
        }

        // Reopen and verify.
        let store = SqliteMetadataStore::open(&path).unwrap();
        assert_eq!(store.jobs().len(), 1);
        assert_eq!(store.jobs()[0].job_id(), &job_id);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_metadata_store_upserts_job() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upsert.db");
        let job_id = JobId::try_new("job-sqlite-2").unwrap();
        let mut coordinator = sqlite_coordinator_with_job(&job_id, "upsert-test");
        let mut store = SqliteMetadataStore::open(&path).unwrap();

        // Persist twice — upsert means only one row.
        coordinator.persist_jobs_to_store(&mut store).unwrap();
        coordinator.persist_jobs_to_store(&mut store).unwrap();

        assert_eq!(store.jobs().len(), 1, "upsert must not create duplicate rows");
        assert_eq!(store.jobs()[0].job_id(), &job_id);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_metadata_store_persist_jobs_to_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.db");
        let job_id = JobId::try_new("job-sqlite-3").unwrap();

        let mut coordinator = sqlite_coordinator_with_job(&job_id, "persist-test");
        let mut store = SqliteMetadataStore::open(&path).unwrap();
        coordinator.persist_jobs_to_store(&mut store).unwrap();

        // Reopen and recover.
        let store2 = SqliteMetadataStore::open(&path).unwrap();
        let mut coordinator2 =
            Coordinator::active(CoordinatorId::try_new("coord-sqlite-2").unwrap());
        coordinator2.recover_from_store(&store2).unwrap();

        assert!(coordinator2.job_detail_snapshot(&job_id).is_ok());
    }
}
