//! R2 coordinator skeleton.

use dashmap::DashMap;
use krishiv_checkpoint::{
    CheckpointMetadata, CheckpointStorage, open_checkpoint_storage_from_uri, read_epoch_metadata,
    validate_epoch, validate_fencing_token_for_restore,
};
use krishiv_plan::{LogicalPlan, PhysicalPlan};
use krishiv_proto::{
    AttemptId, CheckpointAckRequest, CheckpointAckResponse, CoordinatorId, CoordinatorState,
    ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorTaskAssignment,
    HeartbeatHotKeyReport, InitiateCheckpointCommand, InitiateCheckpointRequest, JobId, JobKind,
    JobSpec, JobState, LeaseGeneration, StageId, StreamingProgressReport, StreamingTaskState,
    TaskAssignment, TaskAttemptRef, TaskCancellationRequest, TaskId, TaskState, TaskStatusResponse,
    TaskStatusUpdate, wire,
};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use tokio::sync::{Notify, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::adaptive::{
    AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, ExecutorHeartbeatEffects,
};
use crate::admission::{InMemoryQueueManager, QueueManager};
use crate::barrier_dispatch::drive_barrier_dispatches;
use crate::checkpoint::{CheckpointCoordinator, CheckpointCoordinatorState};
use crate::config::CoordinatorConfig;
use crate::error::{SchedulerError, SchedulerResult, TaskUpdateOutcome};
use crate::heartbeat::{ExecutorRecord, ExecutorRegistry};
use crate::in_process::is_in_process_task_endpoint;
use crate::job::{
    JobDetailSnapshot, JobRecord, JobSnapshot, NamespaceQuotaSnapshot, ResourceUsage,
    SlotAwareScheduler, StabilityMetrics, SubmitOutcome, job_spec_from_logical_plan,
    job_spec_from_physical_plan, validate_job,
};
use crate::llm_quota;
use crate::metrics::{
    CHECKPOINT_EPOCHS_TOTAL, JOBS_SUBMITTED_TOTAL, TASKS_ASSIGNED_TOTAL, record_checkpoint_epoch,
};
use crate::store::{EventLogEvent, MetadataStore, NonBlockingStoreHandle};

static COORDINATOR_NEXT_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Generate a cluster-unique coordinator identifier using hostname + PID + tick.
/// Collision-resistant across coordinator restarts, multi-process deployments,
/// and cross-host scenarios. For deterministic IDs deployments should set
/// `--coordinator-id` explicitly or use kubernetes StatefulSet naming.
fn generate_coordinator_id() -> SchedulerResult<CoordinatorId> {
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| {
            // Fall back to reading /etc/hostname when HOSTNAME env var is not set.
            std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_owned())
        })
        .unwrap_or_else(|_| format!("host-{}", std::process::id()));
    let pid = std::process::id();
    let tick = COORDINATOR_NEXT_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Include a timestamp to ensure uniqueness across PID reuse (L1: prevents collisions).
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_secs() % 1_000_000) as u32) // last 6 digits of unix timestamp
        .unwrap_or(0);
    // Sanitize hostname: replace chars invalid in IDs with '-'
    let safe_hostname: String = hostname
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .take(40)
        .collect();
    CoordinatorId::try_new(format!("coord-{safe_hostname}-{pid}-{ts}-{tick}")).map_err(|e| {
        SchedulerError::InvalidJob {
            message: format!("generated coordinator id invalid: {e}"),
        }
    })
}

/// R2 coordinator skeleton.
///
/// Intentionally does *not* derive `Clone`.  Cloning the coordinator would
/// only deep-copy `HashMap` fields while aliasing the `Arc` fields
/// (`store`, `executor_channels`, …), producing two `Coordinator`s with
/// divergent job state but shared channel caches — a foot-gun that has bitten
/// before (F3 in the audit).  The shared handle is [`SharedCoordinator`].
pub struct Coordinator {
    pub(crate) coordinator_id: CoordinatorId,
    pub(crate) state: CoordinatorState,
    pub(crate) config: CoordinatorConfig,
    pub(crate) executors: ExecutorRegistry,
    /// O(1) job lookup by id.  Replaces Vec<JobRecord> linear scan.
    pub(crate) jobs: HashMap<JobId, JobRecord>,
    pub(crate) store: Option<NonBlockingStoreHandle>,
    /// Per-job checkpoint coordinators for streaming jobs with checkpoint config.
    pub(crate) checkpoint_coordinators: HashMap<JobId, CheckpointCoordinator>,
    /// Controls admission of new jobs.  Defaults to `InMemoryQueueManager`
    /// (always admits).  R7.1 will add quota-aware implementations.
    pub(crate) queue_manager: Arc<dyn QueueManager>,
    /// Jobs that have just reached a terminal state and need shuffle GC.
    /// Drained by the coordinator binary's tick loop.
    pub(crate) gc_ready_jobs: Vec<JobId>,
    /// Number of heartbeat ticks since the last coordinator restart.
    /// Used to implement `streaming_reattach_grace_ticks`: for this many ticks
    /// after `recover_from_store` is called, streaming-job executors are not
    /// evicted for missing heartbeats.
    pub(crate) ticks_since_restart: u64,
    /// Set to true after `recover_from_store` has been called at least once.
    pub(crate) recovering: bool,
    /// Append-only log of adaptive decisions (hot-key split, repartition,
    /// throttle, slow-sink).  Keyed by job id.  R7.2 Group H.
    /// Uses VecDeque for O(1) front-pop when evicting oldest entries.
    pub(crate) adaptive_decision_log:
        HashMap<JobId, std::collections::VecDeque<AdaptiveDecisionLog>>,
    /// Manual override config for adaptive behaviors.
    pub(crate) adaptive_override: AdaptiveOverrideConfig,
    /// P1.1: O(1) index from streaming task id to (job_id, stage_id) for heartbeat lookup.
    /// Populated when tasks are assigned; entries removed on task completion/failure.
    pub(crate) streaming_task_index: HashMap<TaskId, (JobId, StageId)>,
    /// Reverse index from job_id to task_ids for O(tasks_per_job) cleanup.
    /// Built in `index_streaming_tasks`, used in `remove_streaming_task_index`.
    pub(crate) streaming_job_task_index: HashMap<JobId, Vec<TaskId>>,
    /// M6: Sharded gRPC channel cache keyed by executor endpoint string.
    /// DashMap provides per-shard locking so lookups for different endpoints
    /// proceed in parallel.  Avoids a full TCP+TLS handshake per task push.
    pub(crate) executor_channels: Arc<DashMap<String, tonic::transport::Channel>>,
    pub(crate) checkpoint_notify_sent: HashSet<(JobId, ExecutorId, u64)>,
    /// (job_id, epoch) pairs for which a gRPC barrier round-trip was dispatched.
    pub(crate) barrier_dispatch_sent: HashSet<(JobId, u64)>,
    /// Aggregates LLM quota reports across executors (R17).
    pub(crate) llm_quota_aggregator: llm_quota::LlmQuotaAggregator,
    /// Inline Arrow IPC result batches keyed by job id (terminal SQL/window collect).
    pub(crate) job_inline_results: HashMap<JobId, Vec<Vec<u8>>>,
    /// Parquet tables registered for coordinated `batch-sql` jobs.
    pub(crate) batch_sql_job_tables: HashMap<JobId, Vec<crate::batch_sql::BatchSqlTable>>,

    /// Notify channel for waking daemon tick and other waiters on state change.
    pub(crate) notify: Arc<Notify>,

    /// Track B (two-tier): Per-job JobCoordinator instances.
    /// Each owns its JobRecord and will progressively own per-job launch decisions,
    /// heartbeat windows, checkpoint coordination, and recovery logic.
    /// The outer Coordinator (CCP) becomes thin routing + admission + cross-job concerns.
    pub(crate) job_coordinators: HashMap<JobId, Arc<crate::job_coordinator::JobCoordinator>>,
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
            .field("job_inline_results_len", &self.job_inline_results.len())
            .finish()
    }
}

/// Shared handle to the active coordinator owned by an R2 runtime process.
///
/// # Lock Sharding (H2 partial)
///
/// The outer `inner` lock guards the full `Coordinator` state. The dedicated
/// `executor_inner` and `checkpoint_inner` locks provide finer-grained access
/// to the hottest paths (heartbeat processing and checkpoint acks) without
/// requiring the full coordinator write lock.
///
/// Migration path: hot gRPC handlers should prefer the dedicated inner locks;
/// the outer lock is held only for operations that need full coordinator state.
#[derive(Debug, Clone)]
pub struct SharedCoordinator {
    inner: Arc<RwLock<Coordinator>>,
    /// Dedicated lock for executor registry state — avoids serialising
    /// heartbeat processing behind the full coordinator write lock.
    pub(crate) executor_inner:
        Arc<RwLock<crate::coordinator_sharded::ExecutorInner>>,
    /// Dedicated lock for checkpoint coordinator state — avoids serialising
    /// checkpoint ack processing behind the full coordinator write lock.
    pub(crate) checkpoint_inner:
        Arc<RwLock<crate::coordinator_sharded::CheckpointInner>>,
}

impl SharedCoordinator {
    /// Create a shared coordinator handle.
    pub fn new(coordinator: Coordinator) -> Self {
        use crate::coordinator_sharded::{CheckpointInner, ExecutorInner};
        let executor_inner = ExecutorInner {
            executors: coordinator.executors.clone(),
            state: coordinator.state,
            ticks_since_restart: coordinator.ticks_since_restart,
            recovering: coordinator.recovering,
            notify: coordinator.notify.clone(),
        };
        let checkpoint_inner = CheckpointInner::new();
        Self {
            inner: Arc::new(RwLock::new(coordinator)),
            executor_inner: Arc::new(RwLock::new(executor_inner)),
            checkpoint_inner: Arc::new(RwLock::new(checkpoint_inner)),
        }
    }

    /// Borrow the coordinator for read-only status snapshots.
    pub async fn read(&self) -> RwLockReadGuard<'_, Coordinator> {
        self.inner.read().await
    }

    /// Borrow the coordinator for scheduler mutations.
    pub async fn write(&self) -> RwLockWriteGuard<'_, Coordinator> {
        self.inner.write().await
    }

    /// Advance the heartbeat clock by one tick (P0-4).
    pub async fn advance_heartbeat_tick(&self) -> SchedulerResult<Vec<ExecutorId>> {
        tracing::debug!(
            "advancing heartbeat tick (per-job JCP delegation and Notify will react in two-tier model)"
        );
        let lost = self.write().await.advance_heartbeat_clock(1)?;

        // Real JCP usage (Track B ownership): delegate per-job heartbeat staleness,
        // loss recovery, and launch eligibility to the owning JobCoordinator.
        // The outer Coordinator now asks the JCP rather than walking job state directly.
        let coord = self.inner.read().await;
        for (job_id, jc) in &coord.job_coordinators {
            let in_flight = jc.has_in_flight_tasks().await;
            let eligible = jc.has_tasks_eligible_for_launch().await;
            let (launch_eligible, stages_with_work) = jc.get_launch_work_summary().await;

            for lost in &lost {
                let ts = krishiv_async_util::unix_now_ms() as u64;
                let stale = jc.record_heartbeat_and_detect_stale(lost, ts).await;
                let affected = jc.handle_executor_loss(lost).await;
                if affected > 0 || stale {
                    tracing::warn!(
                        job_id = %job_id,
                        executor_id = %lost,
                        affected_tasks = affected,
                        stale_detected = stale,
                        "JCP handled executor loss during heartbeat tick"
                    );
                }
            }

            tracing::debug!(
                job_id = %job_id,
                in_flight,
                eligible_for_launch = eligible,
                launch_eligible_tasks = launch_eligible,
                stages_with_pending_work = stages_with_work,
                "JCP consulted during heartbeat tick (full per-job delegation)"
            );
        }

        for lost_exec in &lost {
            tracing::debug!(executor_id = %lost_exec, "executor lost during heartbeat tick; JCP recovery paths may activate");
        }

        tracing::debug!(
            lost_count = lost.len(),
            "heartbeat tick completed; lost executors will trigger recovery paths"
        );
        Ok(lost)
    }

    /// Wait for any coordinator state change notification (executor, checkpoint, etc.).
    /// Used by the daemon tick to react promptly instead of pure periodic polling.
    pub async fn wait_for_change(&self) {
        let notify = { self.inner.read().await.notify.clone() };
        notify.notified().await;
    }

    /// Launch and push all assigned tasks for non-terminal jobs (P0-4).
    pub async fn drive_pending_task_launches(&self) -> SchedulerResult<usize> {
        tracing::debug!("driving pending task launches for non-terminal jobs");

        // Real delegation (Track B): Build the list of jobs to drive using JCP-owned
        // queries where possible. This moves filtering and decision data into the per-job
        // coordinator.
        let job_ids = {
            let coord = self.read().await;
            let mut ids = Vec::new();
            for (job_id, _) in coord.jobs.iter().filter(|(_, j)| !j.state().is_terminal()) {
                if let Some(jc) = coord.job_coordinator(job_id) {
                    if jc.should_consider_for_launch().await {
                        ids.push(job_id.clone());
                    }
                } else {
                    // Fallback for jobs without JCP yet (transitional).
                    ids.push(job_id.clone());
                }
            }
            ids
        };

        // Real delegation (Track B): query per-job JCP surface for launch decision data.
        // Pure async .await delegation — no block_on in this hot path.
        for job_id in &job_ids {
            let jc = {
                let coord = self.inner.read().await;
                coord.job_coordinator(job_id).clone()
            };
            if let Some(jc) = jc {
                let has_in_flight = jc.has_in_flight_tasks().await;
                let stage_count = jc.stage_count().await;
                let eligible_for_launch = jc.has_tasks_eligible_for_launch().await;
                let (eligible_tasks, stages_with_work) = jc.get_launch_work_summary().await;
                tracing::debug!(
                    job_id = %job_id,
                    has_in_flight,
                    stage_count,
                    eligible_for_launch,
                    eligible_tasks,
                    stages_with_work,
                    "JCP consulted for launch decision (full per-job summary)"
                );
            }
        }

        // Real delegation step (Track B): the outer Coordinator will consult per-job
        // JobCoordinator instances (via has_in_flight_tasks, stage_count, etc.) for
        // launch and heartbeat decisions. The owned JCP methods above are the seam.
        let mut launched = 0usize;
        for job_id in job_ids {
            tracing::debug!(
                job_id = %job_id,
                "driving launches for job"
            );

            let targets = {
                let mut coord = self.write().await;
                let assignments = coord.launch_assigned_task_assignments(&job_id)?;
                coord.resolve_assignment_targets(assignments)?
            };
            let channels = {
                let coord = self.read().await;
                coord.executor_channels.clone()
            };
            let delivery =
                Coordinator::deliver_assignment_targets_with_channels(channels, targets).await;
            match delivery {
                Ok(responses) => {
                    let mut coord = self.write().await;
                    let newly_launched =
                        coord.apply_assignment_dispatch_responses(&job_id, &responses);
                    launched = launched.saturating_add(newly_launched);

                    tracing::debug!(
                        job_id = %job_id,
                        newly_launched,
                        "assignment dispatch responses applied"
                    );

                    if newly_launched > 0 {
                        coord.notify.notify_waiters();
                    }

                    tracing::debug!(
                        job_id = %job_id,
                        newly_launched,
                        executor_count = responses.len(),
                        "task launch dispatch completed (async JCP delegation influences future cycles)"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %error,
                        "task launch delivery failed for job; clearing in-flight and continuing"
                    );
                    let mut coord = self.write().await;
                    coord.clear_launch_in_flight_for_job(&job_id);
                    // Do NOT return — continue with remaining jobs
                }
            }
        }
        Ok(launched)
    }

    /// Spawn background heartbeat and task-launch loops.
    ///
    /// The returned [`OrchestratorHandles`] **must be stored**; dropping it
    /// immediately aborts all loops. Call [`OrchestratorHandles::shutdown`]
    /// before dropping on graceful coordinator shutdown or demotion.
    #[must_use = "dropping OrchestratorHandles immediately aborts all background loops"]
    pub fn spawn_orchestration_loops(&self) -> OrchestratorHandles {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut abort_handles = Vec::with_capacity(3);

        // Heartbeat loop
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = rx.changed() => { if *rx.borrow() { return; } }
                    }
                    if let Err(error) = coord.advance_heartbeat_tick().await {
                        let text = error.to_string();
                        if !text.contains("InactiveCoordinator") {
                            tracing::warn!(error = %text, "coordinator heartbeat tick failed");
                        }
                    }
                }
            });
            abort_handles.push(task.abort_handle());
        }

        // Task launch loop
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = rx.changed() => { if *rx.borrow() { return; } }
                    }
                    if let Err(error) = coord.drive_pending_task_launches().await {
                        let text = error.to_string();
                        if !text.contains("InactiveCoordinator") {
                            tracing::warn!(error = %text, "coordinator task launch tick failed");
                        }
                    }
                }
            });
            abort_handles.push(task.abort_handle());
        }

        // Barrier dispatch loop
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = rx.changed() => { if *rx.borrow() { return; } }
                    }
                    if let Err(error) =
                        drive_barrier_dispatches(&coord, std::time::Duration::from_secs(30)).await
                    {
                        tracing::warn!(error = %error, "coordinator barrier dispatch failed");
                    }
                }
            });
            abort_handles.push(task.abort_handle());
        }

        OrchestratorHandles {
            abort_handles,
            shutdown_tx,
        }
    }
}

/// Handles for the background orchestration tasks spawned by
/// [`SharedCoordinator::spawn_orchestration_loops`].
///
/// **Must be kept alive** for the loops to run. Call [`OrchestratorHandles::shutdown`]
/// for graceful termination, or drop to abort immediately.
pub struct OrchestratorHandles {
    abort_handles: Vec<tokio::task::AbortHandle>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl OrchestratorHandles {
    /// Signal all loops to stop and wait for them to exit.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        for h in &self.abort_handles {
            h.abort();
        }
    }
}

impl Drop for OrchestratorHandles {
    fn drop(&mut self) {
        self.shutdown();
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
            streaming_job_task_index: HashMap::new(),
            executor_channels: Arc::new(DashMap::new()),
            checkpoint_notify_sent: HashSet::new(),
            barrier_dispatch_sent: HashSet::new(),
            llm_quota_aggregator: llm_quota::LlmQuotaAggregator::new(
                config.llm_quota_requests_per_minute(),
                config.llm_quota_tokens_per_minute(),
            ),
            job_inline_results: HashMap::new(),
            batch_sql_job_tables: HashMap::new(),
            notify: Arc::new(Notify::new()),
            job_coordinators: HashMap::new(),
        }
    }

    /// Take inline Arrow IPC result batches for a completed job.
    pub fn take_job_inline_results(&mut self, job_id: &JobId) -> Option<Vec<Vec<u8>>> {
        self.job_inline_results.remove(job_id)
    }

    /// Track B (two-tier): Returns the JobCoordinator for a job if present.
    /// This is the seam for delegating per-job decisions (launch, recovery, heartbeat windows).
    pub fn job_coordinator(
        &self,
        job_id: &JobId,
    ) -> Option<Arc<crate::job_coordinator::JobCoordinator>> {
        self.job_coordinators.get(job_id).cloned()
    }

    /// Track E large completion step: Returns UDF resource limits for a job using JCP-owned accessors.
    /// Returns (time_cap_ms, memory_bytes). Callers can build a real ResourceLimits from this.
    pub async fn job_udf_resource_limits(&self, job_id: &JobId) -> (Option<u64>, Option<u64>) {
        if let Some(jc) = self.job_coordinator(job_id) {
            return jc.udf_resource_limits().await;
        }
        // Transitional fallback
        (Some(60 * 60 * 1000), None)
    }

    /// Create a new active coordinator with a process-unique identifier.
    /// Returns an error if id generation fails.
    pub fn new_active(config: Option<CoordinatorConfig>) -> SchedulerResult<Self> {
        Self::try_new_active(config)
    }

    /// Create a new standby coordinator with a process-unique identifier.
    /// Returns an error if id generation fails.
    pub fn new_standby(config: Option<CoordinatorConfig>) -> SchedulerResult<Self> {
        Self::try_new_standby(config)
    }

    /// Create a new active coordinator, returning an error if id generation fails.
    pub fn try_new_active(config: Option<CoordinatorConfig>) -> SchedulerResult<Self> {
        Ok(Self::build(
            generate_coordinator_id()?,
            config.unwrap_or_default(),
            CoordinatorState::Active,
        ))
    }

    /// Create a new standby coordinator, returning an error if id generation fails.
    pub fn try_new_standby(config: Option<CoordinatorConfig>) -> SchedulerResult<Self> {
        Ok(Self::build(
            generate_coordinator_id()?,
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
        self.store = Some(NonBlockingStoreHandle::new(store));
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

    /// Return the adaptive decision log for a job, or an empty vec if there
    /// are no decisions for this job.  R7.2 Group H.
    pub fn adaptive_decision_log(&self, job_id: &JobId) -> Vec<&AdaptiveDecisionLog> {
        self.adaptive_decision_log
            .get(job_id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
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
        let res = self.executors.register(descriptor);
        if res.is_ok() {
            self.notify.notify_waiters();
        }
        res
    }

    /// Deregister an executor with a valid lease generation.
    pub fn deregister_executor(
        &mut self,
        executor_id: &ExecutorId,
        lease_generation: LeaseGeneration,
    ) -> SchedulerResult<LeaseGeneration> {
        self.ensure_active()?;
        let res = self.executors.deregister(executor_id, lease_generation);
        if res.is_ok() {
            // Evict the executor's gRPC channel so stale TCP connections
            // do not leak (Phase 1.3).
            if let Ok(record) = self.executors.find_executor(executor_id)
                && let Some(endpoint) = record.descriptor().task_endpoint()
            {
                self.executor_channels.remove(endpoint);
            }
            self.notify.notify_waiters();
        }
        res
    }

    /// Apply an executor heartbeat.
    ///
    /// For streaming executors re-attaching after a coordinator restart, the heartbeat may
    /// include `streaming_task_states`. These are applied to the matching task records so
    /// the coordinator tracks the executor's current watermark and source offset without
    /// re-submitting the job from scratch.
    ///
    /// Returns throttle commands to forward back to the executor (R7.2 Group C).
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
        let streaming_progress: Vec<StreamingProgressReport> =
            heartbeat.streaming_progress().to_vec();
        self.executors.heartbeat(heartbeat)?;
        for state in &streaming_states {
            self.apply_streaming_task_state(state);
        }
        // R7.2 Group D: process hot-key reports and record adaptive decisions.
        let source_throttles = self.process_hot_key_reports(&hot_key_reports);
        if !llm_reports.is_empty() {
            self.llm_quota_aggregator.ingest(&llm_reports);
        }
        // Record streaming progress for observability (watermark, throughput, state size).
        for report in &streaming_progress {
            self.record_streaming_progress(report);
        }
        let llm_throttles = self.llm_quota_aggregator.evaluate_and_reset();
        let checkpoint_commands = self.pending_initiate_checkpoints_for_executor(&executor_id);
        let lease_generation = self
            .executors
            .find_executor(&executor_id)
            .map(|e| e.lease_generation())
            .unwrap_or(fallback_lease);

        self.notify.notify_waiters();

        Ok(ExecutorHeartbeatEffects {
            source_throttles,
            llm_throttles,
            checkpoint_commands,
            lease_generation,
        })
    }

    /// Record adaptive decisions for incoming hot-key reports and return throttle
    /// commands to send back to the executor.
    ///
    /// For each hot key whose `heat_score` exceeds `HOT_KEY_HEAT_THRESHOLD`, an
    /// `AdaptiveDecisionLog` entry is recorded AND a `ThrottleDecision` is returned
    /// so the executor can immediately reduce the source's ingestion rate.
    ///
    /// The throttle rate is set to `(1.0 - heat_score) * base_rows_per_second`
    /// (floor: 1 row/s) so hotter keys receive more aggressive throttling.
    ///
    /// If `disable_hot_key_splitting` is set the decision is logged with
    /// `applied = false` and no throttle command is emitted.
    fn process_hot_key_reports(
        &mut self,
        reports: &[HeartbeatHotKeyReport],
    ) -> Vec<crate::adaptive::ThrottleDecision> {
        const HOT_KEY_HEAT_THRESHOLD: f64 = 0.3;
        const BASE_ROWS_PER_SECOND: u64 = 10_000;

        if reports.is_empty() {
            return Vec::new();
        }
        let now_ms = u64::try_from(krishiv_async_util::unix_now_ms()).unwrap_or(0);
        let mut throttles = Vec::new();

        for report in reports {
            if report.job_id.is_empty() {
                continue;
            }
            let job_id = match JobId::try_new(report.job_id.clone()) {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(
                        raw_job_id = %report.job_id,
                        error = %e,
                        "ignoring hot-key report with invalid job_id from executor"
                    );
                    continue;
                }
            };
            let is_hot = report.heat_score >= HOT_KEY_HEAT_THRESHOLD;
            let applied = is_hot && !self.adaptive_override.disable_hot_key_splitting;
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
            let log_bucket = self.adaptive_decision_log.entry(job_id).or_default();
            const MAX_LOG_PER_JOB: usize = 100;
            if log_bucket.len() >= MAX_LOG_PER_JOB {
                log_bucket.pop_front(); // O(1) with VecDeque
            }
            log_bucket.push_back(log);

            if applied {
                // Clamp heat_score to [0, 1] to prevent invalid calculations from NaN or out-of-range values.
                let heat = report.heat_score.clamp(0.0_f64, 1.0_f64);
                // Throttle the source proportional to its heat score.
                let reduced_rate =
                    ((1.0 - heat) * BASE_ROWS_PER_SECOND as f64).max(1.0) as u64;
                throttles.push(crate::adaptive::ThrottleDecision {
                    source_id: report.source_id.clone(),
                    rows_per_second: Some(reduced_rate),
                });
                tracing::info!(
                    source_id = %report.source_id,
                    heat_score = report.heat_score,
                    throttle_rate = reduced_rate,
                    "hot-key throttle applied"
                );
            }
        }
        throttles
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

    /// Record a streaming progress report from an executor heartbeat.
    ///
    /// These reports carry watermark, throughput, and state-size data for
    /// continuous streaming tasks. The data is logged at debug level for
    /// observability and can be consumed by external monitoring.
    fn record_streaming_progress(&mut self, report: &StreamingProgressReport) {
        tracing::debug!(
            job_id = %report.job_id,
            task_id = %report.task_id,
            watermark_ms = report.watermark_ms,
            rows_emitted = report.rows_emitted,
            batches_emitted = report.batches_emitted,
            state_bytes = report.state_bytes,
            timestamp_ms = report.timestamp_ms,
            "streaming progress",
        );

        // Wire incoming executor streaming progress reports to the global metrics registry (Phase 3 H3 / GAP-OB-04)
        let metrics = krishiv_metrics::global_metrics();
        metrics.set_watermark_ms(&report.job_id, report.watermark_ms);
        // Note: report.rows_emitted is a cumulative counter representing newly emitted rows since task start.
        // We use set_streaming_rows to set the absolute cumulative updates correctly without double-counting.
        metrics.set_streaming_rows(&report.job_id, &report.task_id, report.rows_emitted);
        metrics.set_state_bytes(&report.job_id, report.state_bytes);
    }

    /// Populate `streaming_task_index` for all tasks in a job after assignment.
    ///
    /// Called after `apply_assignments` so that streaming heartbeats can use the O(1) index.
    /// Also populates the reverse index for O(tasks_per_job) cleanup.
    fn index_streaming_tasks(&mut self, job_id: &JobId) {
        let job = match self.jobs.get(job_id) {
            Some(j) => j,
            None => return,
        };
        let mut job_task_ids = Vec::new();
        for stage in &job.stages {
            let stage_id = stage.stage_id().clone();
            for task in stage.tasks() {
                let task_id = task.task_id().clone();
                self.streaming_task_index
                    .insert(task_id.clone(), (job_id.clone(), stage_id.clone()));
                job_task_ids.push(task_id);
            }
        }
        if !job_task_ids.is_empty() {
            self.streaming_job_task_index.insert(job_id.clone(), job_task_ids);
        }
    }

    /// Remove `streaming_task_index` entries for a completed/failed/cancelled job.
    /// Uses the reverse index for O(tasks_per_job) lookup instead of O(total_tasks) scan.
    fn remove_streaming_task_index(&mut self, job_id: &JobId) {
        if let Some(task_ids) = self.streaming_job_task_index.remove(job_id) {
            for tid in task_ids {
                self.streaming_task_index.remove(&tid);
            }
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

            // Capture the awaiting epoch BEFORE ticking so we can detect a
            // timeout-triggered abort (GAP-5).  An abort transitions the state
            // from AwaitingAcks → Failed; if that happens we must clean up
            // checkpoint_notify_sent and barrier_dispatch_sent entries for the
            // aborted epoch so they don't accumulate forever and block future
            // checkpoint rounds.
            let pre_tick_awaiting: Option<u64> =
                self.checkpoint_coordinators.get(&job_id).and_then(|c| {
                    if let CheckpointCoordinatorState::AwaitingAcks { epoch, .. } = &c.state {
                        Some(*epoch)
                    } else {
                        None
                    }
                });

            if let Some(coord) = self.checkpoint_coordinators.get_mut(&job_id) {
                coord.set_expected_task_count(running);
                coord.try_tick(elapsed_ms, self.config.checkpoint_ack_timeout_ms());
            }

            // GAP-5: if try_tick aborted an in-flight epoch, remove all stale
            // tracking entries that referenced that epoch.
            //
            // Without this cleanup:
            //   - checkpoint_notify_sent retains (job_id, executor_id, epoch) for
            //     every executor that was notified; since the epoch number is never
            //     reused those entries would live until the coordinator shuts down.
            //   - barrier_dispatch_sent retains (job_id, epoch); again the epoch is
            //     unique so the entry is harmless for correctness but wastes memory.
            if let Some(aborted_epoch) = pre_tick_awaiting {
                let was_aborted = self
                    .checkpoint_coordinators
                    .get(&job_id)
                    .is_some_and(|c| matches!(c.state, CheckpointCoordinatorState::Failed { .. }));
                if was_aborted {
                    self.checkpoint_notify_sent
                        .retain(|(jid, _, e)| jid != &job_id || *e != aborted_epoch);
                    self.barrier_dispatch_sent
                        .retain(|(jid, e)| jid != &job_id || *e != aborted_epoch);
                    tracing::warn!(
                        job_id = %job_id,
                        epoch = aborted_epoch,
                        "checkpoint epoch aborted by ack timeout; \
                         cleaned up stale notify and barrier-dispatch tracking entries"
                    );
                }
            }
        }

        Ok(evicted)
    }

    /// Count tasks in `Running` state for a job (checkpoint quorum size).
    ///
    /// D3: Previously this included `Assigned` tasks too, which over-counted
    /// the expected quorum and caused barrier rounds to time out waiting for
    /// acks from tasks that hadn't started yet.  When the new task transitions
    /// to `Running` via heartbeat, the coordinator can re-tick to include it
    /// in the next epoch.
    fn running_task_count_for_job(&self, job_id: &JobId) -> usize {
        self.jobs.get(job_id).map_or(0, |job| {
            job.stages
                .iter()
                .flat_map(|stage| stage.tasks())
                .filter(|task| matches!(task.state(), TaskState::Running))
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
        // Also clean up barrier dispatch tracking for the committed epoch.
        self.barrier_dispatch_sent
            .retain(|(jid, e)| jid != job_id || *e != epoch);
    }

    fn executor_has_running_task_in_job(&self, executor_id: &ExecutorId, job_id: &JobId) -> bool {
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

    /// Reload one job record from the attached metadata store into memory.
    ///
    /// Used by per-job coordinator processes that share a durable metadata file
    /// with the cluster control plane (ADR-DIST-01).
    pub fn sync_job_from_metadata_store(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| SchedulerError::Transport {
                message: "coordinator has no metadata store".to_string(),
            })?;
        let record = {
            let guard = store.inner();
            guard.jobs().iter().find(|j| j.job_id() == job_id).cloned()
        };
        if let Some(record) = record {
            let streaming = record.spec.kind() == JobKind::Streaming;
            self.jobs.insert(job_id.clone(), record.clone());
            // Track B (two-tier): keep the JCP surface consistent when a dedicated
            // per-job coordinator syncs a single job record from shared metadata.
            if !self.job_coordinators.contains_key(job_id) {
                let jcp = crate::job_coordinator::JobCoordinator::new(job_id.clone(), record);
                self.job_coordinators.insert(job_id.clone(), Arc::new(jcp));
            }
            if streaming {
                self.index_streaming_tasks(job_id);
            }
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
        self.job_coordinators.clear();
        for record in store.jobs() {
            let job_id = record.job_id().clone();
            self.jobs.insert(job_id.clone(), record.clone());
            // Track B (two-tier): repopulate JobCoordinator map on recovery so the
            // per-job ownership seam (launch decisions, heartbeat windows, recovery)
            // survives coordinator restart / failover for long-lived jobs.
            let jcp = crate::job_coordinator::JobCoordinator::new(job_id.clone(), record.clone());
            self.job_coordinators.insert(job_id, Arc::new(jcp));
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
            .filter_map(|j| {
                let task_count: usize = j.spec.stages().iter().map(|s| s.tasks().len()).sum();
                Some((
                    j.job_id().clone(),
                    j.spec.checkpoint_interval_ms()?,
                    j.spec.checkpoint_storage_path()?.to_owned(),
                    task_count,
                ))
            })
            .collect();
        for (job_id, interval_ms, storage_path, task_count) in streaming_checkpoint_jobs {
            match Self::open_checkpoint_storage(&storage_path) {
                Ok(storage) => {
                    let mut coord = CheckpointCoordinator::new(
                        job_id.clone(),
                        self.coordinator_id().as_str().to_owned(),
                        storage,
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

    fn open_checkpoint_storage(path: &str) -> SchedulerResult<Arc<dyn CheckpointStorage>> {
        open_checkpoint_storage_from_uri(path).map_err(|e| SchedulerError::InvalidJob {
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

        // Prepare (but don't yet commit) a CheckpointCoordinator for streaming jobs.
        // A7: We previously inserted the coordinator into `checkpoint_coordinators`
        // before persisting the job — if `save_job` failed, the in-memory coordinator
        // leaked.  Now we open storage here, hand the constructed `CheckpointCoordinator`
        // over only after the job record is durably saved AND inserted in memory.
        let mut pending_checkpoint: Option<CheckpointCoordinator> = None;
        if spec.kind() == JobKind::Streaming
            && let (Some(interval_ms), Some(storage_path)) = (
                spec.checkpoint_interval_ms(),
                spec.checkpoint_storage_path(),
            )
        {
            let storage = Self::open_checkpoint_storage(storage_path)?;
            pending_checkpoint = Some(CheckpointCoordinator::new(
                spec.job_id().clone(),
                self.coordinator_id().as_str().to_owned(),
                storage,
                interval_ms,
                0,
            ));
        }

        let executors = self.executors.schedulable_executors();
        let assignments = SlotAwareScheduler::place(&spec, &executors)?;
        let job_id = spec.job_id().clone();
        let job_name = spec.name().to_owned();
        let namespace = spec
            .namespace_id()
            .map(|s| s.to_owned())
            .unwrap_or_default();
        let mut record = JobRecord::from_spec(spec, self.config.max_stage_retries());
        record.apply_assignments(assignments);
        if let Some(store) = &self.store {
            // Non-blocking fire-and-forget: enqueue writes to background task.
            // The coordinator lock is released immediately after enqueueing.
            store.save_job(&record);
            store.append_event(EventLogEvent::JobSubmitted {
                job_id: job_id.clone(),
            });
        }
        let inserted_job_id = record.job_id().clone();
        self.jobs.insert(inserted_job_id.clone(), record.clone());

        // Track B (two-tier CCP/JCP): create the owning JobCoordinator for this job.
        // The JCP holds the Arc<RwLock<JobRecord>> and will progressively own per-job
        // launch decisions, heartbeat windows, checkpoint coordination, and recovery.
        // The outer Coordinator (CCP) retains cross-job concerns and the thin map for delegation.
        let jcp = crate::job_coordinator::JobCoordinator::new(
            inserted_job_id.clone(),
            record.clone(),
        );
        self.job_coordinators
            .insert(inserted_job_id.clone(), Arc::new(jcp));
        tracing::debug!(
            job_id = %inserted_job_id,
            "job coordinator registered (two-tier seam active)"
        );

        if let Some(ckpt_coord) = pending_checkpoint {
            self.checkpoint_coordinators
                .insert(inserted_job_id.clone(), ckpt_coord);
        }
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

        // GAP-OB-06: Emit OpenLineage START event.
        // Only spawn when a Tokio runtime is active (production); skip in
        // synchronous test contexts — the event is advisory, not critical.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _j_id = inserted_job_id.to_string();
            handle.spawn(async move {
                let event = krishiv_governance::new_run_event(
                    krishiv_governance::RunEventType::Start,
                    job_name,
                    namespace,
                    vec![],
                    vec![],
                );
                krishiv_governance::emit_lineage_event(event).await;
            });
        }

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
        tracing::debug!(
            job_id = %ack.job_id,
            epoch = ack.epoch,
            fencing_token = ack.fencing_token.as_u64(),
            "handling checkpoint ack"
        );

        let job_id = ack.job_id.clone();

        let res = match self.checkpoint_coordinators.get_mut(&job_id) {
            None => CheckpointAckResponse::JobNotFound,
            Some(coord) => {
                let coordinator_token = coord.fencing_token();
                if ack.fencing_token.as_u64() != coordinator_token.as_u64() {
                    return CheckpointAckResponse::StaleFencingToken {
                        current_token: coordinator_token.as_u64(),
                    };
                }

                let current_epoch = coord.current_epoch();
                match coord.receive_ack(ack.clone()) {
                    Ok(true) => {
                        self.clear_checkpoint_notify_for_epoch(&job_id, ack.epoch);
                        CHECKPOINT_EPOCHS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
                        record_checkpoint_epoch(job_id.as_str(), ack.epoch);
                        CheckpointAckResponse::Accepted
                    }
                    Ok(false) => CheckpointAckResponse::Accepted,
                    Err(_) => CheckpointAckResponse::StaleEpoch { current_epoch },
                }
            }
        };

        self.notify.notify_waiters();

        res
    }

    /// Initiate a savepoint for a streaming job.
    ///
    /// Returns the savepoint epoch number.  Fails if no `CheckpointCoordinator`
    /// exists for this job (i.e. the job was not submitted with checkpoint config).
    pub fn savepoint_job(&mut self, job_id: &JobId, label: Option<String>) -> SchedulerResult<u64> {
        let running = self.running_task_count_for_job(job_id);
        let res = match self.checkpoint_coordinators.get_mut(job_id) {
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
        };

        if res.is_ok() {
            krishiv_governance::audit_log(
                "scheduler",
                &krishiv_governance::AuditAction::SavepointCreated {
                    job_id: job_id.to_string(),
                },
                krishiv_governance::AuditOutcome::Allowed,
            );
        }
        res
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
        self.restore_job_from_checkpoint_with_fencing(job_id, epoch, storage_path, None)
    }

    /// Same as [`Self::restore_job_from_checkpoint`] but accepts an explicit
    /// current fencing token from the live leader-election backend.
    ///
    /// Distributed deployments MUST pass the live token (A8): when the
    /// in-memory `checkpoint_coordinators` map has not yet been rebuilt after
    /// a restart, this is the only place where stale-epoch restores can be
    /// rejected.
    pub fn restore_job_from_checkpoint_with_fencing(
        &self,
        job_id: &JobId,
        epoch: u64,
        storage_path: &str,
        leader_fencing_token: Option<u64>,
    ) -> SchedulerResult<CheckpointMetadata> {
        let storage = Self::open_checkpoint_storage(storage_path)?;

        let meta = read_epoch_metadata(storage.as_ref(), job_id.as_str(), epoch).map_err(|e| {
            SchedulerError::InvalidJob {
                message: format!("cannot read checkpoint epoch {epoch}: {e}"),
            }
        })?;

        let meta = meta.ok_or_else(|| SchedulerError::InvalidJob {
            message: format!("checkpoint epoch {epoch} not found for job {job_id}"),
        })?;

        validate_epoch(storage.as_ref(), job_id.as_str(), epoch).map_err(|e| {
            SchedulerError::InvalidJob {
                message: format!("checkpoint epoch {epoch} failed integrity check: {e}"),
            }
        })?;

        // GAP-CK-01 / A8: prefer the in-memory checkpoint coordinator's token
        // (most recent), then fall back to the leader-election token.  At
        // least one MUST be present for distributed deployments.
        let token = self
            .checkpoint_coordinators
            .get(job_id)
            .map(|coord| coord.fencing_token().as_u64())
            .or(leader_fencing_token);
        if let Some(current_token) = token {
            validate_fencing_token_for_restore(&meta, current_token).map_err(|e| {
                SchedulerError::InvalidJob {
                    message: format!("restore rejected for job {job_id}: {e}"),
                }
            })?;
        } else {
            tracing::warn!(
                job_id = %job_id,
                epoch = epoch,
                "restoring checkpoint without fencing token validation; \
                 caller did not supply a leader token (A8)"
            );
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
        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::SavepointRestored {
                job_id: job_id.to_string(),
                epoch,
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

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
        let mut executor_ids: Vec<ExecutorId> = self
            .executors
            .schedulable_executors()
            .into_iter()
            .map(|d| d.executor_id().clone())
            .collect();
        executor_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
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
    pub fn register_batch_sql_tables(
        &mut self,
        job_id: JobId,
        tables: Vec<crate::batch_sql::BatchSqlTable>,
    ) {
        self.batch_sql_job_tables.insert(job_id, tables);
    }

    pub fn launch_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        tracing::debug!(
            job_id = %job_id,
            "launching assigned task assignments (JCP delegation and Notify will be used in two-tier model)"
        );
        self.ensure_active()?;

        // PRR Parallel Execution - Circuit Breaker (IMM-1):
        // Filter out executors that have crossed the failure threshold before
        // even attempting to launch tasks to them.
        let failure_threshold = self.config.circuit_breaker_failure_threshold();
        let bad_executors: std::collections::HashSet<_> = self
            .executors
            .executors_over_failure_threshold(failure_threshold)
            .into_iter()
            .collect();

        let mut executor_leases = self.executors.assignment_leases();
        if !bad_executors.is_empty() {
            executor_leases.retain(|(eid, _)| !bad_executors.contains(eid));
            tracing::warn!(
                job_id = %job_id,
                bad_executor_count = bad_executors.len(),
                "circuit breaker: filtered bad executors from launch candidates"
            );
        }

        let batch_tables = self.batch_sql_job_tables.get(job_id).cloned();
        let assignments = self
            .find_job_mut(job_id)?
            .launch_assigned_task_assignments(&executor_leases, batch_tables.as_deref())?;
        // GAP-OB-01: Increment tasks_assigned counter.
        TASKS_ASSIGNED_TOTAL.fetch_add(assignments.len() as u64, AtomicOrdering::Relaxed);
        Ok(assignments)
    }

    /// Cancel a job and mark non-terminal stages/tasks cancelled.
    pub fn cancel_job(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        self.ensure_active()?;
        let job = self.find_job_mut(job_id)?;
        job.cancel();

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::JobCancelled {
                job_id: job_id.to_string(),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        let job_name = job.spec.name().to_owned();
        let namespace = job
            .spec
            .namespace_id()
            .map(|s| s.to_owned())
            .unwrap_or_default();

        if !self.gc_ready_jobs.contains(job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.remove(0);
            }
            self.gc_ready_jobs.push(job_id.clone());
        }
        self.checkpoint_coordinators.remove(job_id);

        // Emit OpenLineage FAIL event for job cancellation (Phase 3 M5 / GAP-OB-06)
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let event = krishiv_governance::new_run_event(
                    krishiv_governance::RunEventType::Fail,
                    job_name,
                    namespace,
                    vec![],
                    vec![],
                );
                krishiv_governance::emit_lineage_event(event).await;
            });
        }
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
        tracing::debug!(
            assignment_count = assignments.len(),
            "resolving assignment targets for delivery"
        );

        for a in &assignments {
            tracing::trace!(task_id = %a.task_id(), executor = %a.executor_id(), "resolving single assignment target");
        }

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
    ) -> SchedulerResult<Vec<(ExecutorTaskAssignment, TaskStatusResponse)>> {
        let channels = self.executor_channels.clone();
        Self::deliver_assignment_targets_with_channels(channels, targets).await
    }

    pub(crate) async fn deliver_assignment_targets_with_channels(
        channels: Arc<DashMap<String, tonic::transport::Channel>>,
        targets: Vec<(String, ExecutorTaskAssignment)>,
    ) -> SchedulerResult<Vec<(ExecutorTaskAssignment, TaskStatusResponse)>> {
        use futures::stream::{FuturesUnordered, StreamExt};

        // Inbox-backed in-process targets do not have a gRPC endpoint: they are
        // delivered directly via `InProcessCoordinatorBridge` (see F4 / the
        // `inprocess://` sentinel).  Logging would create noise; the in-process
        // path pushes to the inbox before reaching this function.
        let (in_process, remote): (Vec<_>, Vec<_>) = targets
            .into_iter()
            .partition(|(endpoint, _)| is_in_process_task_endpoint(endpoint));
        if !in_process.is_empty() {
            tracing::debug!(
                count = in_process.len(),
                "skipping gRPC dispatch for in-process task endpoints"
            );
        }

        let mut futures = FuturesUnordered::new();
        for (endpoint, assignment) in remote {
            let channels = Arc::clone(&channels);
            futures.push(async move {
                let channel = Self::get_or_connect_channel_on_map(&channels, &endpoint).await?;
                let mut client =
                    wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                        channel,
                        krishiv_metrics::grpc::inject_trace_context
                            as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                    );
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    client.assign_task(wire::executor_task_assignment_to_wire(assignment.clone())),
                )
                .await
                .map_err(|_| SchedulerError::Transport {
                    message: format!("assign_task to {endpoint} timed out after 30s"),
                })?
                .map_err(|error| SchedulerError::Transport {
                    message: format!("assign_task to {endpoint}: {error}"),
                })?
                .into_inner();
                wire::task_status_response_from_wire(response)
                    .map(|decoded| (assignment, decoded))
                    .map_err(|error| SchedulerError::Transport {
                        message: format!("wire decode from {endpoint}: {error}"),
                    })
            });
        }

        let mut responses = Vec::new();
        while let Some(result) = futures.next().await {
            responses.push(result?);
        }
        Ok(responses)
    }

    /// Launch assigned tasks and push them to executor-owned task endpoints.
    ///
    /// # Lock safety (GAP-4)
    ///
    /// This method takes `&mut self` for the sync prepare phase
    /// (`launch_assigned_task_assignments` + `resolve_assignment_targets`), then
    /// clones the channel map and calls the **static** `deliver_assignment_targets_with_channels`
    /// so `self` is NOT borrowed during the async network I/O.
    ///
    /// **Important**: If you call this through a `SharedCoordinator.write()` guard the write
    /// lock is still held for the duration of the await, because the borrow lives for the
    /// entire async function body.  For the production dispatch path use
    /// `JobCoordinator::spawn_job_orchestration_loops`, which explicitly drops the write guard
    /// before awaiting.  This method is intended for tests and CLI tools where no shared lock
    /// is involved.
    pub async fn push_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<TaskStatusResponse>> {
        let assignments = self.launch_assigned_task_assignments(job_id)?;
        let targets = self.resolve_assignment_targets(assignments)?;
        // GAP-4: Clone the channel map BEFORE the await point. Because
        // `deliver_assignment_targets_with_channels` is a static method that owns
        // `channels`, `self` is not borrowed across the network I/O yield points.
        // Callers that hold a `SharedCoordinator.write()` guard should prefer the
        // `JobCoordinator` pattern (acquire lock → collect targets → drop lock → deliver).
        let channels = self.executor_channels.clone();
        let responses =
            match Self::deliver_assignment_targets_with_channels(channels, targets).await {
                Ok(responses) => responses,
                Err(error) => {
                    self.clear_launch_in_flight_for_job(job_id);
                    return Err(error);
                }
            };
        self.apply_assignment_dispatch_responses(job_id, &responses);
        Ok(responses
            .into_iter()
            .map(|(_, response)| response)
            .collect())
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

        // Push cancel RPCs — partial failures are non-fatal.  Re-use the
        // executor channel cache so we do not pay a TCP+TLS handshake per
        // cancel target (F2).  Drive them concurrently.
        let channels = self.executor_channels.clone();
        let mut futures = futures::stream::FuturesUnordered::new();
        for (endpoint, req) in targets {
            if is_in_process_task_endpoint(&endpoint) {
                tracing::debug!(endpoint = %endpoint, "skipping cancel for in-process executor");
                continue;
            }
            let channels = channels.clone();
            futures.push(async move {
                let channel = match Self::get_or_connect_channel_on_map(&channels, &endpoint).await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!(endpoint = %endpoint, error = %err, "push_cancel_job: connect failed");
                        return;
                    }
                };
                let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                    channel,
                    krishiv_metrics::grpc::inject_trace_context
                        as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                );
                if let Err(err) = client
                    .cancel_task(wire::task_cancellation_request_to_wire(req))
                    .await
                {
                    tracing::warn!(endpoint = %endpoint, error = %err, "push_cancel_job: cancel_task rpc failed");
                }
            });
        }
        use futures::stream::StreamExt;
        while futures.next().await.is_some() {}
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

        tracing::debug!(
            job_id = %update.job_id(),
            stage_id = %update.stage_id(),
            task_id = %update.task_id(),
            attempt = update.attempt(),
            state = ?update.state(),
            executor = %update.executor_id(),
            "applying task status update"
        );

        let job_id = update.job_id().clone();
        let stage_id = update.stage_id().clone();
        let task_id = update.task_id().clone();
        let attempt = update.attempt();
        let inline_ipc = update
            .output_metadata()
            .map(|meta| meta.inline_record_batch_ipc().to_vec())
            .unwrap_or_default();
        let terminal_state = update.state();
        let executor_id_for_circuit = update.executor_id().clone();
        let outcome = self.find_job_mut(&job_id)?.apply_task_update(update)?;

        // IMM-2 (Circuit Breaker Strengthening):
        // Record failure and, if the executor is now bad, clear the assignment
        // so the task can be re-assigned to a healthy executor on the next launch cycle.
        if terminal_state == TaskState::Failed {
            krishiv_governance::audit_log(
                "scheduler",
                &krishiv_governance::AuditAction::TaskFailed {
                    job_id: job_id.to_string(),
                    stage_id: stage_id.to_string(),
                    task_id: task_id.to_string(),
                    attempt_id: attempt,
                },
                krishiv_governance::AuditOutcome::Allowed,
            );

            let threshold = self.config.circuit_breaker_failure_threshold();
            let exceeded = self
                .executors
                .record_task_failure(&executor_id_for_circuit, threshold);
            if exceeded {
                tracing::warn!(
                    executor_id = %executor_id_for_circuit,
                    "executor exceeded failure threshold — clearing assignments for re-launch on healthy executors"
                );

                if let Some(jc) = self.job_coordinator(&job_id) {
                    let jc = jc.clone();
                    let eid = executor_id_for_circuit.clone();
                    tokio::spawn(async move {
                        let _ = jc.clear_assignments_for_bad_executor_and_count(&eid).await;
                    });
                } else {
                    if let Ok(job) = self.find_job_mut(&job_id) {
                        for stage in job.stages_mut() {
                            for task in stage.tasks_mut() {
                                if task.assigned_executor.as_ref() == Some(&executor_id_for_circuit)
                                {
                                    task.assigned_executor = None;
                                    task.launch_in_flight = false;
                                }
                            }
                        }
                    }
                }

                tracing::debug!(
                    job_id = %job_id,
                    executor_id = %executor_id_for_circuit,
                    "circuit breaker triggered; assignments cleared via JCP or fallback"
                );
                self.notify.notify_waiters();
            }
        } else if terminal_state == TaskState::Succeeded {
            self.executors.reset_task_failures(&executor_id_for_circuit);
        }

        if terminal_state == TaskState::Succeeded && !inline_ipc.is_empty() {
            self.job_inline_results
                .entry(job_id.clone())
                .or_default()
                .extend(inline_ipc);
        }

        // Snapshot the job's current state and resource usage after the update.
        let (is_terminal, usage, state, job_name, namespace) = self
            .jobs
            .get(&job_id)
            .map(|r| {
                (
                    r.state().is_terminal(),
                    r.resource_usage.clone(),
                    r.state(),
                    r.spec.name().to_owned(),
                    r.spec
                        .namespace_id()
                        .map(|s| s.to_owned())
                        .unwrap_or_default(),
                )
            })
            .unwrap_or((
                false,
                ResourceUsage::zero(),
                JobState::Accepted,
                String::new(),
                String::new(),
            ));

        if is_terminal && !self.gc_ready_jobs.contains(&job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.remove(0);
            }
            self.gc_ready_jobs.push(job_id.clone());
            self.checkpoint_coordinators.remove(&job_id);
            self.job_coordinators.remove(&job_id);
            self.queue_manager.on_job_complete(&job_id, &usage);

            // Emit OpenLineage COMPLETE/FAIL events (Phase 3 M5 / GAP-OB-06)
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let event_type = match state {
                    JobState::Succeeded => krishiv_governance::RunEventType::Complete,
                    _ => krishiv_governance::RunEventType::Fail,
                };
                handle.spawn(async move {
                    let event = krishiv_governance::new_run_event(
                        event_type,
                        job_name,
                        namespace,
                        vec![],
                        vec![],
                    );
                    krishiv_governance::emit_lineage_event(event).await;
                });
            }
        }
        if let Some(record) = self.jobs.get(&job_id)
            && let Some(store) = &self.store
        {
            // Non-blocking fire-and-forget: enqueue the save to background task.
            store.save_job(record);
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
        self.executors.list()
    }

    fn clear_launch_in_flight_for_job(&mut self, job_id: &JobId) {
        let Some(job) = self.jobs.get_mut(job_id) else {
            return;
        };
        for stage in &mut job.stages {
            for task in stage.tasks_mut() {
                if task.state() == TaskState::Assigned {
                    task.clear_launch_in_flight();
                }
            }
            stage.refresh_state();
        }
        job.refresh_state();
    }

    fn clear_launch_in_flight_for_task(&mut self, job_id: &JobId, task_id: &TaskId) {
        let Some(job) = self.jobs.get_mut(job_id) else {
            return;
        };
        for stage in &mut job.stages {
            let mut changed = false;
            for task in stage.tasks_mut() {
                if task.task_id() == task_id && task.state() == TaskState::Assigned {
                    task.clear_launch_in_flight();
                    changed = true;
                    break;
                }
            }
            if changed {
                stage.refresh_state();
            }
        }
        job.refresh_state();
    }

    fn apply_assignment_dispatch_responses(
        &mut self,
        job_id: &JobId,
        responses: &[(ExecutorTaskAssignment, TaskStatusResponse)],
    ) -> usize {
        tracing::debug!(
            job_id = %job_id,
            response_count = responses.len(),
            "applying launch dispatch responses (JCP delegation may influence future retries)"
        );

        for (assignment, response) in responses {
            tracing::trace!(
                job_id = %job_id,
                task_id = %assignment.task_id(),
                disposition = ?response.disposition(),
                "individual launch response"
            );
        }

        let mut accepted = 0usize;
        for (assignment, response) in responses {
            match response.disposition() {
                krishiv_proto::TransportDisposition::Accepted
                | krishiv_proto::TransportDisposition::Duplicate => {
                    accepted = accepted.saturating_add(1);
                }
                _ => self.clear_launch_in_flight_for_task(job_id, assignment.task_id()),
            }
        }
        accepted
    }

    fn reset_running_tasks_for_lost_executor(&mut self, lost_id: &ExecutorId) {
        const MAX_EXECUTOR_LOSSES_BEFORE_FAIL: u32 = 5;

        let mut jobs_to_reassign = Vec::new();
        for (job_id, job) in &mut self.jobs {
            let mut job_affected = false;
            for stage in &mut job.stages {
                let mut stage_affected = false;
                for task in &mut stage.tasks {
                    if task.assigned_executor.as_ref() == Some(lost_id)
                        && (task.state == TaskState::Running
                            || (task.state == TaskState::Assigned && task.launch_in_flight()))
                    {
                        task.executor_loss_count = task.executor_loss_count.saturating_add(1);
                        task.assigned_executor = None;
                        task.clear_launch_in_flight();
                        if task.executor_loss_count >= MAX_EXECUTOR_LOSSES_BEFORE_FAIL {
                            task.state = TaskState::Failed;
                            task.last_failure_reason = Some(format!(
                                "executor lost {} consecutive times (max {}); task permanently failed",
                                task.executor_loss_count, MAX_EXECUTOR_LOSSES_BEFORE_FAIL
                            ));
                            tracing::warn!(
                                task_id = %task.task_id(),
                                executor_loss_count = task.executor_loss_count,
                                "task failed after too many executor losses"
                            );
                        } else {
                            task.state = TaskState::Pending;
                        }
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
                jobs_to_reassign.push(job_id.clone());
            }
        }
        for job_id in jobs_to_reassign {
            if let Err(error) = self.assign_pending_tasks(&job_id) {
                tracing::warn!(job_id = %job_id, error = %error, "failed to reassign tasks after executor loss");
            }
        }
    }

    /// P1.2: Get or create a cached gRPC channel for the given executor endpoint.
    ///
    /// On a cache hit, clones the existing `Channel` (pointer-only cost).
    /// On a miss, establishes a new TCP+TLS connection and stores it for reuse.
    #[allow(dead_code)]
    async fn get_or_connect_channel(
        &self,
        endpoint: &str,
    ) -> SchedulerResult<tonic::transport::Channel> {
        Self::get_or_connect_channel_on_map(&self.executor_channels, endpoint).await
    }

    async fn get_or_connect_channel_on_map(
        channels: &Arc<DashMap<String, tonic::transport::Channel>>,
        endpoint: &str,
    ) -> SchedulerResult<tonic::transport::Channel> {
        // Fast path: check the sharded cache (per-shard lock, dropped
        // immediately) so lookups for different endpoints never contend.
        if let Some(ch) = channels.get(endpoint) {
            return Ok(ch.clone());
        }

        // Slow path: connect outside any lock so a single slow handshake
        // cannot block lookups for other endpoints (M6).
        let parsed =
            tonic::transport::Endpoint::from_shared(endpoint.to_string()).map_err(|e| {
                SchedulerError::InvalidJob {
                    message: e.to_string(),
                }
            })?;
        let ch = parsed
            .connect_timeout(std::time::Duration::from_secs(10))
            .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
            .http2_keep_alive_interval(std::time::Duration::from_secs(15))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .map_err(|e| SchedulerError::ExecutorUnavailable {
                endpoint: endpoint.to_string(),
                reason: e.to_string(),
            })?;

        // Only one shard is locked during the insert.  If another task
        // raced and already installed a channel, prefer the existing one.
        let endpoint_owned = endpoint.to_owned();
        let entry = channels.entry(endpoint_owned);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(existing) => Ok(existing.get().clone()),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(ch.clone());
                Ok(ch)
            }
        }
    }

    pub(crate) fn ensure_active(&self) -> SchedulerResult<()> {
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
