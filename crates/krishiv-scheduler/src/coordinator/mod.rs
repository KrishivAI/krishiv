//! Coordinator state machine and shared handle.

use dashmap::DashMap;
use krishiv_common::DurabilityProfile;
use krishiv_plan::{LogicalPlan, PhysicalPlan};
use krishiv_proto::{
    AttemptId, CheckpointAckRequest, CheckpointAckResponse, CoordinatorId, CoordinatorState,
    ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorTaskAssignment,
    HeartbeatHotKeyReport, InitiateCheckpointCommand, InitiateCheckpointRequest, JobId, JobKind,
    JobSpec, JobState, LeaseGeneration, StageId, StageState, StreamingProgressReport,
    StreamingTaskState, TaskAssignment, TaskAttemptRef, TaskCancellationRequest, TaskId, TaskState,
    TaskStatusResponse, TaskStatusUpdate, wire,
};
use krishiv_state::checkpoint::{
    CheckpointMetadata, CheckpointStorage, open_checkpoint_storage_from_uri, read_epoch_metadata,
    validate_epoch, validate_fencing_token_for_restore,
};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
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

/// Directive instructing executors of a job to reload state and source
/// offsets from a committed checkpoint epoch (global rollback).
///
/// Set on explicit restore activation and on executor loss for checkpointed
/// streaming jobs: surviving executors must roll back too, otherwise their
/// post-checkpoint state would double-count the source data that rewound
/// sources re-deliver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreDirective {
    pub epoch: u64,
    pub fencing_token: u64,
}

/// Active coordinator.
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
    /// Active durability profile for fail-closed admission and restore paths.
    pub(crate) durability_profile: DurabilityProfile,
    pub(crate) executors: ExecutorRegistry,
    pub(crate) store: Option<NonBlockingStoreHandle>,
    /// Per-job checkpoint coordinators for streaming jobs with checkpoint config.
    pub(crate) checkpoint_coordinators: HashMap<JobId, CheckpointCoordinator>,
    /// Controls admission of new jobs.  Defaults to `InMemoryQueueManager`
    /// (always admits).  R7.1 will add quota-aware implementations.
    pub(crate) queue_manager: Arc<dyn QueueManager>,
    /// Jobs that have just reached a terminal state and need shuffle GC.
    /// Drained by the coordinator binary's tick loop.
    pub(crate) gc_ready_jobs: VecDeque<JobId>,
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
    /// Sharded gRPC channel cache keyed by executor endpoint string.
    /// DashMap avoids a full TCP+TLS handshake per task push.
    pub(crate) executor_channels: Arc<DashMap<String, tonic::transport::Channel>>,
    pub(crate) checkpoint_notify_sent: indexmap::IndexSet<(JobId, ExecutorId, u64)>,
    /// (job_id, executor_id, epoch) triples for which a checkpoint-complete
    /// notification (transactional-sink commit signal) was already delivered.
    pub(crate) checkpoint_complete_sent: indexmap::IndexSet<(JobId, ExecutorId, u64)>,
    /// Active restore directives per job: every executor with tasks in the job
    /// must reload state/offsets from the directive's epoch (global rollback).
    pub(crate) restore_directives: HashMap<JobId, RestoreDirective>,
    /// (job_id, executor_id, epoch) triples for which a restore directive was
    /// already delivered.
    pub(crate) restore_notify_sent: indexmap::IndexSet<(JobId, ExecutorId, u64)>,
    /// Jobs that must be cancelled once their savepoint epoch commits
    /// (stop-with-savepoint protocol).  Maps job → savepoint epoch.
    pub(crate) pending_stop_after_savepoint: HashMap<JobId, u64>,
    /// (job_id, epoch) pairs for which a gRPC barrier round-trip was dispatched.
    pub(crate) barrier_dispatch_sent: HashSet<(JobId, u64)>,
    /// Inline Arrow IPC result batches keyed by job id (terminal SQL/window collect).
    pub(crate) job_inline_results: HashMap<JobId, Vec<Vec<u8>>>,
    /// Parquet tables registered for coordinated `batch-sql` jobs.
    pub(crate) batch_sql_job_tables: HashMap<JobId, Vec<crate::batch_sql::BatchSqlTable>>,
    /// Inline input partitions registered for coordinated batch-sql and bounded-window jobs.
    pub(crate) job_input_partitions: HashMap<JobId, Vec<krishiv_proto::InputPartition>>,
    /// Task-scoped inline inputs for coordinator-partitioned jobs.
    pub(crate) job_task_input_partitions:
        HashMap<JobId, HashMap<TaskId, Vec<krishiv_proto::InputPartition>>>,
    /// Continuous jobs with one coordinator-dispatched input cycle currently
    /// assigned or executing. This fences concurrent pushes for the same job.
    pub(crate) continuous_input_cycles: HashSet<JobId>,

    /// S1: Skew-aware repartitioning overrides. When a hot-key report exceeds
    /// the threshold, the affected job's stage is added here with a RoundRobin
    /// bucket count. `launch_assigned_task_assignments` uses this to override
    /// Hash partitioning with RoundRobin for the next task batch, distributing
    /// hot-key data evenly across available executors.
    /// Entry is removed after the next task launch to allow normal partitioning
    /// to resume (adaptive: only applies to the immediate next batch).
    pub(crate) skew_repartition_overrides: HashMap<JobId, u32>,

    /// EMA-derived advisory partition counts for streaming jobs.
    ///
    /// Populated by `record_streaming_advisory_buckets` when a streaming task
    /// output carries `advisory_buckets` from `StreamingPartitionAdvisor`.
    /// The coordinator uses these values to scale the number of tasks it
    /// assigns for the next streaming cycle. Entries are removed with
    /// `evict_completed_job`.
    pub(crate) streaming_advisory_partitions: HashMap<JobId, u32>,

    /// Notify channel for waking daemon tick and other waiters on state change.
    pub(crate) notify: Arc<Notify>,

    /// Per-job coordinators. Each owns its JobRecord and per-job launch decisions.
    pub(crate) job_coordinators: HashMap<JobId, Arc<crate::job_coordinator::JobCoordinator>>,

    /// AQE coalesce hints produced by stage-boundary re-optimization (Phase 2.9).
    ///
    /// Keyed by (job_id, completed_stage_id).  Populated after a shuffle stage
    /// completes and the CoalesceRule fires.  Consumed by `launch_assigned_task_assignments`
    /// for the downstream stage to right-size reduce-side parallelism.
    pub(crate) aqe_coalesce_hints: HashMap<(JobId, StageId), usize>,
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("coordinator_id", &self.coordinator_id)
            .field("state", &self.state)
            .field("config", &self.config)
            .field("executors", &self.executors)
            .field("job_coordinators_len", &self.job_coordinators.len())
            .field("store", &self.store.as_ref().map(|_| "<store>"))
            .field("streaming_task_index_len", &self.streaming_task_index.len())
            .field("job_inline_results_len", &self.job_inline_results.len())
            .field(
                "job_task_input_partitions_len",
                &self.job_task_input_partitions.len(),
            )
            .finish()
    }
}

/// Shared handle to the active coordinator.
///
/// The outer `inner` lock guards the full `Coordinator` state. The dedicated
/// `executor_inner` and `checkpoint_inner` locks provide finer-grained access
/// to the hottest paths (heartbeat processing and checkpoint acks) without
/// requiring the full coordinator write lock.
#[derive(Debug, Clone)]
pub struct SharedCoordinator {
    inner: Arc<RwLock<Coordinator>>,
    /// Dedicated lock for executor registry state — avoids serialising
    /// heartbeat processing behind the full coordinator write lock.
    pub executor_inner: Arc<RwLock<crate::coordinator_sharded::ExecutorInner>>,
    /// Dedicated lock for checkpoint coordinator state — avoids serialising
    /// checkpoint ack processing behind the full coordinator write lock.
    pub checkpoint_inner: Arc<RwLock<crate::coordinator_sharded::CheckpointInner>>,
    /// Process-wide durability profile (from daemon config or env).
    pub durability_profile: DurabilityProfile,
    /// Live leader-election fencing token mirrored from the CCP leader backend.
    pub leader_fencing_token: Arc<std::sync::atomic::AtomicU64>,
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
        let checkpoint_inner = CheckpointInner::from_parts(
            coordinator.checkpoint_coordinators.clone(),
            coordinator.checkpoint_notify_sent.clone(),
            coordinator.barrier_dispatch_sent.clone(),
        );
        let durability_profile = coordinator.durability_profile;
        Self {
            inner: Arc::new(RwLock::new(coordinator)),
            executor_inner: Arc::new(RwLock::new(executor_inner)),
            checkpoint_inner: Arc::new(RwLock::new(checkpoint_inner)),
            durability_profile,
            leader_fencing_token: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Attach the daemon durability profile to this shared handle.
    #[must_use]
    pub fn with_durability_profile(mut self, profile: DurabilityProfile) -> Self {
        self.durability_profile = profile;
        self
    }

    /// Mirror the live leader-election fencing token for restore validation (A8).
    pub fn sync_leader_fencing_token(&self, token: u64) {
        self.leader_fencing_token
            .store(token, std::sync::atomic::Ordering::SeqCst);
    }

    /// Borrow the coordinator for read-only status snapshots.
    pub async fn read(&self) -> RwLockReadGuard<'_, Coordinator> {
        self.inner.read().await
    }

    /// Borrow the coordinator for scheduler mutations.
    pub async fn write(&self) -> RwLockWriteGuard<'_, Coordinator> {
        self.inner.write().await
    }

    /// Submit a job through the shared coordinator and refresh sharded checkpoint state.
    pub async fn submit_job(&self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        let (outcome, checkpoint_coordinators, checkpoint_notify_sent, barrier_dispatch_sent) = {
            let mut coord = self.inner.write().await;
            let outcome = coord.submit_job(spec)?;
            (
                outcome,
                coord.checkpoint_coordinators.clone(),
                coord.checkpoint_notify_sent.clone(),
                coord.barrier_dispatch_sent.clone(),
            )
        };

        let mut checkpoint_inner = self.checkpoint_inner.write().await;
        crate::coordinator_sharded::sync_checkpoint_to_inner(
            &checkpoint_coordinators,
            &checkpoint_notify_sent,
            &barrier_dispatch_sent,
            &mut checkpoint_inner,
        );

        Ok(outcome)
    }

    /// Return executor snapshots using the sharded `ExecutorInner` read lock,
    /// avoiding the full coordinator read lock for high-frequency observability
    /// queries (dashboards, health checks, metrics scrapes).
    ///
    /// The returned snapshots reflect the executor registry as maintained by the
    /// sharded inner state, which is kept in sync with the main coordinator on
    /// every heartbeat tick and task assignment. Use this in preference to
    /// `coordinator.read().await.executor_snapshots()` when the coordinator lock
    /// is a contention point.
    pub async fn executor_snapshots_fast(&self) -> Vec<crate::coordinator::ExecutorRecord> {
        let inner = self.executor_inner.read().await;
        inner.executors.list()
    }

    /// Advance the heartbeat clock by one tick.
    pub async fn advance_heartbeat_tick(&self) -> SchedulerResult<Vec<ExecutorId>> {
        tracing::debug!("advancing heartbeat tick");
        let lost = {
            let mut coord = self.inner.write().await;
            let lost = coord.advance_heartbeat_clock(1)?;
            // Sync coordinator → inner locks after mutation to prevent dual-state
            // drift (G3).  The inner locks are read by in-process hot paths and
            // must see the updated executor registry and checkpoint state.
            let mut exec_inner = self.executor_inner.write().await;
            let mut ckpt_inner = self.checkpoint_inner.write().await;
            crate::coordinator_sharded::sync_executor_to_inner(
                &coord.executors,
                coord.state,
                coord.executors.current_tick,
                coord.recovering,
                &mut exec_inner,
            );
            crate::coordinator_sharded::sync_checkpoint_to_inner(
                &coord.checkpoint_coordinators,
                &coord.checkpoint_notify_sent,
                &coord.barrier_dispatch_sent,
                &mut ckpt_inner,
            );
            lost
        };

        // Clone JCP Arcs outside the read guard so .await calls do not hold the lock.
        let jc_snapshots: Vec<(JobId, Arc<crate::job_coordinator::JobCoordinator>)> = {
            let coord = self.inner.read().await;
            coord
                .job_coordinators
                .iter()
                .map(|(job_id, jc)| (job_id.clone(), Arc::clone(jc)))
                .collect()
        };
        for (job_id, jc) in jc_snapshots {
            let in_flight = jc.has_in_flight_tasks().await;
            let eligible = jc.has_tasks_eligible_for_launch().await;
            let (launch_eligible, stages_with_work) = jc.get_launch_work_summary().await;

            for lost in &lost {
                let ts = krishiv_common::async_util::unix_now_ms() as u64;
                let stale = jc.record_heartbeat_and_detect_stale(lost, ts);
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

    /// Launch and push all assigned tasks for non-terminal jobs.
    pub async fn drive_pending_task_launches(&self) -> SchedulerResult<usize> {
        tracing::debug!("driving pending task launches for non-terminal jobs");

        // Build the list of jobs to drive, sorted by priority descending so
        // higher-priority jobs consume executor slots first.
        let job_ids = {
            let coord = self.read().await;
            let mut id_pairs: Vec<(u8, JobId)> = Vec::new();
            for (job_id, jc) in coord.job_coordinators.iter() {
                let (is_terminal, priority) = {
                    let record = jc.read_record();
                    (record.state().is_terminal(), record.spec.priority())
                };
                if is_terminal {
                    continue;
                }
                if let Some(_jc) = coord.job_coordinator(job_id) {
                    if jc.should_consider_for_launch().await {
                        id_pairs.push((priority, job_id.clone()));
                    }
                } else {
                    id_pairs.push((priority, job_id.clone()));
                }
            }
            id_pairs.sort_by_key(|k| Reverse(k.0));
            id_pairs.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
        };

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

        // Phase 1: collect all assignment targets and channels.
        struct JobLaunch {
            job_id: JobId,
            targets: Vec<(String, ExecutorTaskAssignment)>,
        }

        let channels = {
            let coord = self.read().await;
            coord.executor_channels.clone()
        };

        // Process jobs in small batches under short-lived write locks so
        // that readers (heartbeats, API queries) are not blocked while
        // resolving assignments for many jobs.
        let mut launches: Vec<JobLaunch> = Vec::new();
        const LAUNCH_BATCH_SIZE: usize = 20;
        for batch in job_ids.chunks(LAUNCH_BATCH_SIZE) {
            let mut coord = self.write().await;
            for job_id in batch {
                match coord.launch_assigned_task_assignments(job_id) {
                    Ok(assignments) => {
                        let targets = coord
                            .resolve_assignment_targets(assignments)
                            .unwrap_or_default();
                        if !targets.is_empty() {
                            launches.push(JobLaunch {
                                job_id: job_id.clone(),
                                targets,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "failed to launch assignments for job; skipping"
                        );
                    }
                }
            }
        }

        // Phase 2: deliver all assignments concurrently.
        let channels = Arc::new(channels);
        let delivery_futures: Vec<_> = launches
            .iter()
            .map(|jl| {
                let channels = Arc::clone(&channels);
                let job_id = jl.job_id.clone();
                let targets = jl.targets.clone();
                tokio::spawn(async move {
                    let delivery = Coordinator::deliver_assignment_targets_with_channels(
                        (*channels).clone(),
                        targets,
                    )
                    .await;
                    (job_id, delivery)
                })
            })
            .collect();

        let delivery_results = futures::future::join_all(delivery_futures)
            .await
            .into_iter()
            .filter_map(|r| r.ok());

        // Phase 3: apply all results under a single write lock.
        let mut launched = 0usize;
        {
            let mut coord = self.write().await;
            for (job_id, delivery) in delivery_results {
                match delivery {
                    Ok(responses) => {
                        let newly_launched =
                            coord.apply_assignment_dispatch_responses(&job_id, &responses);
                        launched = launched.saturating_add(newly_launched);

                        tracing::debug!(
                            job_id = %job_id,
                            newly_launched,
                            executor_count = responses.len(),
                            "task launch dispatch completed"
                        );

                        if newly_launched > 0 {
                            coord.notify.notify_waiters();
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %error,
                            "task launch delivery failed for job; clearing in-flight and continuing"
                        );
                        coord.clear_launch_in_flight_for_job(&job_id);
                    }
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

        // Task launch loop — wakes immediately on any state-change notification
        // (job submit, task completion, executor registration, etc.) with a 500 ms
        // fallback interval so missed notifications never stall the queue.
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let task = tokio::spawn(async move {
                // Clone the Arc<Notify> once before the loop to avoid taking the
                // coordinator read-lock on every iteration.
                let notify = coord.inner.read().await.notify.clone();
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = notify.notified() => {}
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
    /// Create an active coordinator.
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
            durability_profile: DurabilityProfile::DevLocal,
            executors: ExecutorRegistry::new(
                config.heartbeat_timeout_ticks(),
                config.memory_threshold_bytes(),
            ),
            store: None,
            checkpoint_coordinators: HashMap::new(),
            queue_manager: Arc::new(InMemoryQueueManager),
            gc_ready_jobs: VecDeque::new(),
            ticks_since_restart: u64::MAX,
            recovering: false,
            adaptive_decision_log: HashMap::new(),
            adaptive_override: AdaptiveOverrideConfig::default(),
            streaming_task_index: HashMap::new(),
            streaming_job_task_index: HashMap::new(),
            executor_channels: Arc::new(DashMap::new()),
            checkpoint_notify_sent: indexmap::IndexSet::new(),
            checkpoint_complete_sent: indexmap::IndexSet::new(),
            restore_directives: HashMap::new(),
            restore_notify_sent: indexmap::IndexSet::new(),
            pending_stop_after_savepoint: HashMap::new(),
            barrier_dispatch_sent: HashSet::new(),
            job_inline_results: HashMap::new(),
            batch_sql_job_tables: HashMap::new(),
            job_input_partitions: HashMap::new(),
            job_task_input_partitions: HashMap::new(),
            continuous_input_cycles: HashSet::new(),
            skew_repartition_overrides: HashMap::new(),
            streaming_advisory_partitions: HashMap::new(),
            notify: Arc::new(Notify::new()),
            job_coordinators: HashMap::new(),
            aqe_coalesce_hints: HashMap::new(),
        }
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

    /// Override the durability profile used for fail-closed admission/restore.
    #[must_use]
    pub fn with_durability_profile(mut self, profile: DurabilityProfile) -> Self {
        self.durability_profile = profile;
        self
    }

    /// Attach a metadata store to this coordinator (builder).
    #[must_use]
    pub fn with_store(mut self, store: impl MetadataStore + 'static) -> Self {
        self.store = Some(NonBlockingStoreHandle::new(store));
        self
    }

    /// Attach a metadata store with explicit fail-closed write semantics.
    #[must_use]
    pub fn with_store_fail_closed(
        mut self,
        store: impl MetadataStore + 'static,
        fail_closed_writes: bool,
    ) -> Self {
        self.store =
            Some(NonBlockingStoreHandle::new(store).with_fail_closed_writes(fail_closed_writes));
        self
    }

    /// Attach a metadata store to an already-constructed coordinator.
    ///
    /// Replaces any previously attached store. Used when the coordinator is
    /// already wrapped in `Arc<Mutex<>>` and a builder chain is not possible
    /// (e.g. in-process cluster tests and embedded runtimes).
    pub fn attach_store(&mut self, store: impl MetadataStore + 'static) {
        self.store = Some(NonBlockingStoreHandle::new(store));
    }

    /// Persist a continuous job snapshot via the attached metadata store.
    ///
    /// No-op when no store is configured (store = None). The call is
    /// fire-and-forget: the snapshot is queued on the background write channel
    /// and will not block the caller. Failures are logged by the background task.
    pub fn save_continuous_snapshot(
        &self,
        job_id: &str,
        snapshot: crate::store::ContinuousSnapshot,
    ) {
        if let Some(store) = &self.store {
            store.save_continuous_snapshot(job_id, snapshot);
        }
    }

    /// Load a previously persisted continuous job snapshot from the store.
    ///
    /// Returns `None` when no store is configured or when no snapshot exists
    /// for `job_id`. The load is synchronous — it reads through to the
    /// in-memory view maintained by `NonBlockingStoreHandle`.
    pub fn load_continuous_snapshot(
        &self,
        job_id: &str,
    ) -> Option<crate::store::ContinuousSnapshot> {
        self.store.as_ref()?.load_continuous_snapshot(job_id)
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

    /// Executor registry (used to populate sharded inner locks).
    pub fn executors(&self) -> &ExecutorRegistry {
        &self.executors
    }

    /// Heartbeat ticks since coordinator restart.
    pub fn ticks_since_restart(&self) -> u64 {
        self.executors.current_tick
    }

    /// Whether the coordinator is in recovery mode.
    pub fn recovering(&self) -> bool {
        self.recovering
    }

    /// Notify handle for wake-on-state-change.
    pub fn notify(&self) -> &Arc<Notify> {
        &self.notify
    }

    /// Promote a standby coordinator to active leader.
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

    /// Return all currently tracked job ids (active and recently-terminal).
    /// Used by the shuffle orphan-cleanup loop to determine which job directories
    /// on disk are legitimately owned vs. abandoned (C4).
    pub fn active_job_ids(&self) -> std::collections::HashSet<String> {
        self.job_coordinators
            .keys()
            .map(|jid| jid.as_str().to_string())
            .collect()
    }

    pub fn coordinator_tick(&mut self) -> SchedulerResult<()> {
        self.advance_heartbeat_clock(1)?;
        let job_ids: Vec<JobId> = self.job_coordinators.keys().cloned().collect();
        for job_id in &job_ids {
            let _ = self.launch_assigned_task_assignments(job_id)?;
        }
        // R5: Stall detection — reset Running tasks whose executor is still
        // alive (heartbeating) but the task itself has not progressed past
        // TaskState::Running for longer than the stall timeout. This catches
        // deadlocked operators that block a thread without crashing the executor.
        self.detect_and_reset_stalled_tasks();
        Ok(())
    }

    /// R5: Scan all Running tasks and reset those that have been in-flight
    /// longer than `TASK_STALL_TIMEOUT_MS`. The task is marked Failed so the
    /// coordinator retries it on a different executor slot.
    fn detect_and_reset_stalled_tasks(&mut self) {
        const TASK_STALL_TIMEOUT_MS: u64 = 30 * 60 * 1_000; // 30 minutes
        let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
        for jc in self.job_coordinators.values() {
            let mut record = jc.write_record();
            for stage in record.stages_mut() {
                for task in stage.tasks_mut() {
                    if task.state() != krishiv_proto::TaskState::Running {
                        continue;
                    }
                    if let Some(assigned_ms) = task.assigned_at_ms
                        && now_ms.saturating_sub(assigned_ms) > TASK_STALL_TIMEOUT_MS
                    {
                        tracing::warn!(
                            task_id = %task.task_id(),
                            stall_secs = now_ms.saturating_sub(assigned_ms) / 1000,
                            "resetting stalled task (no progress for >30 min)"
                        );
                        task.state = krishiv_proto::TaskState::Failed;
                        task.last_failure_reason = Some(format!(
                            "task stalled: no progress for {} min",
                            now_ms.saturating_sub(assigned_ms) / 60_000
                        ));
                        task.launch_in_flight = false;
                        task.assigned_at_ms = None;
                    }
                }
            }
        }
    }

    /// Finalize the staged sink outputs of a job that just reached a terminal
    /// state (Phase 2.3 distributed write commit).
    ///
    /// - `Succeeded`: atomically publish staged part files into the
    ///   destination (rename, with copy+delete fallback) and remove staging.
    ///   A publish failure demotes the job to `Failed` so callers never
    ///   observe a succeeded job whose output was not made visible; the
    ///   staging directory is left in place for a later retry or GC.
    /// - `Failed` / `Cancelled`: remove staged files (tolerates already
    ///   missing staging directories).
    ///
    /// Both publish and cleanup are idempotent, so re-entering this method on
    /// duplicate terminal updates converges.
    pub(crate) fn finalize_staged_sink_outputs(&mut self, job_id: &JobId) {
        use krishiv_common::write_commit::{
            SinkWriteSpec, cleanup_staged_outputs, publish_staged_outputs,
        };

        const SINK_PREFIX: &str = "object-parquet-sink:";

        let (state, contracts): (JobState, Vec<String>) = {
            let Some(jc) = self.job_coordinators.get(job_id) else {
                return;
            };
            let record = jc.read_record();
            let contracts = record
                .spec
                .stages()
                .iter()
                .flat_map(|stage| stage.tasks())
                .filter_map(|task| task.sink_contract())
                .filter_map(|contract| {
                    contract
                        .trim()
                        .strip_prefix(SINK_PREFIX)
                        .map(str::to_owned)
                })
                .collect();
            (record.state(), contracts)
        };
        if contracts.is_empty() || !state.is_terminal() {
            return;
        }

        let mut publish_failed = false;
        for payload in contracts {
            let spec = match SinkWriteSpec::parse(&payload) {
                Ok(spec) => spec,
                Err(error) => {
                    // An unparseable contract on a terminal job is a launch-time
                    // bug; nothing was staged under a path we can derive.
                    tracing::error!(job_id = %job_id, error = %error, "invalid sink contract during finalize");
                    continue;
                }
            };
            if !spec.staged {
                // Legacy direct-write contracts commit inside the task itself.
                continue;
            }
            match state {
                JobState::Succeeded => {
                    match publish_staged_outputs(&spec, job_id.as_str()) {
                        Ok(outcome) => {
                            tracing::info!(
                                job_id = %job_id,
                                dest = %spec.dest_path,
                                published = outcome.published.len(),
                                skipped_existing = outcome.skipped_existing,
                                ignored = outcome.ignored,
                                "published staged sink outputs"
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                job_id = %job_id,
                                dest = %spec.dest_path,
                                error = %error,
                                "failed to publish staged sink outputs; failing job"
                            );
                            publish_failed = true;
                        }
                    }
                }
                JobState::Failed | JobState::Cancelled => {
                    if let Err(error) = cleanup_staged_outputs(&spec, job_id.as_str()) {
                        tracing::warn!(
                            job_id = %job_id,
                            dest = %spec.dest_path,
                            error = %error,
                            "failed to clean up staged sink outputs"
                        );
                    }
                }
                JobState::Accepted | JobState::Planning | JobState::Running => {}
            }
        }

        if publish_failed && let Ok(mut job) = self.find_job_mut(job_id) {
            job.state = JobState::Failed;
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

    fn find_job(
        &self,
        job_id: &JobId,
    ) -> SchedulerResult<std::sync::RwLockReadGuard<'_, JobRecord>> {
        self.job_coordinators
            .get(job_id)
            .map(|jc| jc.read_record())
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }

    fn find_job_mut(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<std::sync::RwLockWriteGuard<'_, JobRecord>> {
        self.job_coordinators
            .get(job_id)
            .map(|jc| jc.write_record())
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }
}

mod checkpoint_ops;
mod executor_ops;
mod job_lifecycle;
pub mod observability;
mod recovery;
mod snapshots;
mod streaming;
mod task_assignment;
