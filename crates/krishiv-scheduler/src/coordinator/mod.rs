//! Coordinator state machine and shared handle.

use dashmap::DashMap;
use krishiv_common::DurabilityProfile;
use krishiv_plan::{LogicalPlan, PhysicalPlan};
use krishiv_proto::{
    AttemptId, CheckpointAckRequest, CheckpointAckResponse, CheckpointAlignment, CoordinatorId,
    CoordinatorState, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorTaskAssignment,
    HeartbeatHotKeyReport, InitiateCheckpointCommand, InitiateCheckpointRequest, JobId, JobKind,
    JobSpec, JobState, LeaseGeneration, StageId, StageState, StreamingProgressReport,
    StreamingTaskState, TaskAttemptRef, TaskCancellationRequest, TaskId, TaskState,
    TaskStatusResponse, TaskStatusUpdate, wire,
};
use krishiv_state::QueryableStateStore;
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
use tracing::Instrument as _;

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
    ShuffleRegenOutcome, SlotAwareScheduler, StabilityMetrics, SubmitOutcome,
    job_spec_from_logical_plan, job_spec_from_physical_plan, validate_job,
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
    /// SC14: optional cluster manager for dynamic allocation. The
    /// default is [`NoopClusterManager`] (a no-op for bare-metal and
    /// `clusterd` modes); Kubernetes mode wires this to the
    /// operator CRD API.
    pub(crate) cluster_manager: Arc<dyn crate::cluster_control::ClusterManager>,
    /// Active durability profile for fail-closed admission and restore paths.
    pub(crate) durability_profile: DurabilityProfile,
    /// Executor-registry state (owned; mirrored into SharedCoordinator::executor_inner).
    pub(crate) exec: crate::coordinator_sharded::ExecutorInner,
    pub(crate) store: Option<NonBlockingStoreHandle>,
    /// Checkpoint coordinator state (owned; mirrored into SharedCoordinator::checkpoint_inner).
    pub(crate) ckpt: crate::coordinator_sharded::CheckpointInner,
    /// Controls admission of new jobs.  Defaults to `InMemoryQueueManager`
    /// (always admits).  R7.1 will add quota-aware implementations.
    pub(crate) queue_manager: Arc<dyn QueueManager>,
    /// Jobs that have just reached a terminal state and need shuffle GC.
    /// Drained by the coordinator binary's tick loop.
    pub(crate) gc_ready_jobs: VecDeque<JobId>,
    /// When each `gc_ready_jobs` entry became terminal. The GC tick only evicts
    /// a job once it has been terminal for at least the GC grace window
    /// (TTL-after-finished), so a slow consumer — e.g. a batch-SQL poll delayed
    /// by write-lock contention — still observes the terminal outcome and takes
    /// its result before the job is reaped (otherwise the poll sees `UnknownJob`
    /// and the result is silently lost).
    pub(crate) gc_ready_at: std::collections::HashMap<JobId, std::time::Instant>,
    /// Append-only log of adaptive decisions (hot-key split, repartition,
    /// throttle, slow-sink).  Keyed by job id.  R7.2 Group H.
    /// Uses VecDeque for O(1) front-pop when evicting oldest entries.
    pub(crate) adaptive_decision_log:
        HashMap<JobId, std::collections::VecDeque<AdaptiveDecisionLog>>,
    /// Manual override config for adaptive behaviors.
    pub(crate) adaptive_override: AdaptiveOverrideConfig,
    /// Phase 53: jobs known to still have unassigned `Pending` tasks after
    /// their last assignment round (capacity backlog). Drained when slots
    /// free (task completion, executor registration) instead of scanning
    /// every job.
    pub(crate) pending_backlog_jobs: std::collections::HashSet<JobId>,
    /// Phase 53: jobs with launch-ready work since the last drive tick.
    /// `drive_pending_task_launches` consumes this set so the 500 ms launch
    /// tick is O(dirty jobs), not O(all jobs); a periodic full sweep guards
    /// against missed marks.
    pub(crate) launch_dirty_jobs: std::collections::HashSet<JobId>,
    /// Phase 53: monotonically increasing drive-tick counter used to pace
    /// the full-sweep fallback of the dirty-job launch path.
    pub(crate) launch_sweep_counter: std::sync::atomic::AtomicU64,
    /// Phase 53: scheduler pool specs (weight/min-share) keyed by pool name.
    /// Empty = every namespace is its own pool with default weight 1.
    pub(crate) scheduler_pools: HashMap<String, crate::PoolSpec>,
    /// Phase 53: namespace → pool mapping. Unmapped namespaces use the
    /// namespace id itself as the pool name ("" for the default namespace).
    pub(crate) namespace_pools: HashMap<String, String>,
    /// P1.1: O(1) index from streaming task id to (job_id, stage_id) for heartbeat lookup.
    /// Populated when tasks are assigned; entries removed on task completion/failure.
    pub(crate) streaming_task_index: HashMap<TaskId, (JobId, StageId)>,
    /// Reverse index from job_id to task_ids for O(tasks_per_job) cleanup.
    /// Built in `index_streaming_tasks`, used in `remove_streaming_task_index`.
    pub(crate) streaming_job_task_index: HashMap<JobId, Vec<TaskId>>,
    /// Sharded gRPC channel cache keyed by executor endpoint string.
    /// DashMap avoids a full TCP+TLS handshake per task push.
    ///
    /// Each entry is an `Arc<OnceCell<Channel>>` so that, under concurrent
    /// callers for the same endpoint, exactly one TCP+TLS connection is
    /// established and every other caller awaits and reuses it (#43/#44). The
    /// map lock is held only long enough to get-or-insert the (empty) cell; the
    /// connect runs through `OnceCell::get_or_try_init` with no map lock held.
    pub(crate) executor_channels:
        Arc<DashMap<String, Arc<tokio::sync::OnceCell<tonic::transport::Channel>>>>,
    /// Inline Arrow IPC result batches keyed by job id (terminal SQL/window collect).
    pub(crate) job_inline_results: HashMap<JobId, Vec<Vec<u8>>>,
    /// Spooled task results received via `PushTaskResult` and not yet claimed
    /// by their task's terminal status report.
    pub(crate) pending_task_result_spools:
        HashMap<crate::result_spool::TaskResultKey, crate::result_spool::TaskResultSpool>,
    /// Disk-backed result spools keyed by job id (large terminal SQL results).
    pub(crate) job_result_spools: HashMap<JobId, Vec<crate::result_spool::TaskResultSpool>>,
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
    /// One-shot continuous stream restore snapshots to inject into the next
    /// coordinator-dispatched cycle for a job.
    pub(crate) pending_continuous_restores: HashMap<JobId, crate::ContinuousSnapshot>,

    /// S1: Skew-aware repartitioning overrides. When a hot-key report exceeds
    /// the threshold, the affected job's stage is added here with a RoundRobin
    /// bucket count. `launch_assigned_task_assignments` uses this to override
    /// Hash partitioning with RoundRobin for the next task batch, distributing
    /// hot-key data evenly across available executors.
    /// Entry is removed after the next task launch to allow normal partitioning
    /// to resume (adaptive: only applies to the immediate next batch).
    pub(crate) skew_repartition_overrides: HashMap<JobId, u32>,
    /// Throttle commands queued from task-status hot-key reports; drained on the
    /// executor's next heartbeat (task status responses do not carry throttles).
    pub(crate) pending_source_throttles:
        HashMap<ExecutorId, Vec<crate::adaptive::ThrottleDecision>>,

    /// EMA-derived advisory partition counts for streaming jobs.
    ///
    /// Populated by `record_streaming_advisory_buckets` when a streaming task
    /// output carries `advisory_buckets` from `StreamingPartitionAdvisor`.
    /// The coordinator uses these values to scale the number of tasks it
    /// assigns for the next streaming cycle. Entries are removed with
    /// `evict_completed_job`.
    pub(crate) streaming_advisory_partitions: HashMap<JobId, u32>,

    /// Per-job coordinators. Each owns its JobRecord and per-job launch decisions.
    pub(crate) job_coordinators: HashMap<JobId, Arc<crate::job_coordinator::JobCoordinator>>,

    /// AQE coalesce hints produced by stage-boundary re-optimization (Phase 2.9).
    ///
    /// Keyed by (job_id, completed_stage_id).  Populated after a shuffle stage
    /// completes and the CoalesceRule fires.  Consumed by `launch_assigned_task_assignments`
    /// for the downstream stage to right-size reduce-side parallelism.
    pub(crate) aqe_coalesce_hints: HashMap<(JobId, StageId), usize>,

    /// Deferred sink-finalization work queued by `finalize_staged_sink_outputs`.
    ///
    /// Callers in async contexts (gRPC handler) drain this after releasing the
    /// coordinator write lock and execute the blocking filesystem operations via
    /// `tokio::task::spawn_blocking`, so the write lock is not held during I/O.
    /// Callers in sync contexts use `block_in_place` to drain.
    pub(crate) pending_sink_finalize: Vec<SinkFinalizeWork>,

    /// Per-executor, per-job event-time watermarks (ms since epoch) from streaming progress reports.
    /// Used to compute the global minimum watermark per job (F5: distributed watermark).
    pub(crate) executor_job_watermarks: HashMap<ExecutorId, HashMap<JobId, i64>>,

    // ── SC11: cascade circuit breaker state ───────────────────────────────
    /// Ring buffer of wall-clock timestamps (ms) at which executor losses were
    /// recorded.  Entries older than `config.cascade_window_ms` are pruned each
    /// time a new loss is recorded.
    pub(crate) cascade_loss_timestamps: std::collections::VecDeque<u64>,

    /// Wall-clock timestamp at which the cascade circuit breaker was last tripped.
    /// `None` means the breaker is closed (normal operation).
    pub(crate) cascade_tripped_at_ms: Option<u64>,

    /// Phase 59 (observability gap-a): monotonic submit instant per job, used to
    /// observe whole-query wall-clock latency at job terminal. Set in
    /// `submit_job`, read+removed in `on_job_terminal` (batch jobs only —
    /// streaming submit→terminal is job lifetime, not query latency). Bounded by
    /// the number of concurrently in-flight jobs; also cleared on eviction so a
    /// job that never reaches `on_job_terminal` cannot leak an entry.
    pub(crate) job_submit_instants: HashMap<JobId, std::time::Instant>,
}

/// Describes a stalled task that must be cancelled and reset.
///
/// Produced by [`Coordinator::collect_stall_cancel_work`]; consumed by
/// [`SharedCoordinator::detect_and_cancel_stalled_tasks`].
#[derive(Debug, Clone)]
pub struct StallCancelWork {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt: u32,
    /// gRPC endpoint of the executor holding the task, or `None` for in-process.
    pub executor_endpoint: Option<String>,
    pub stall_secs: u64,
}

/// A straggler task eligible for speculative preemption.
///
/// Produced by [`Coordinator::collect_speculation_work`]; consumed by
/// [`Coordinator::apply_speculation_preempts`].
#[derive(Debug, Clone)]
pub struct SpeculativeWork {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt: u32,
    /// gRPC endpoint of the current executor, for CancelTask RPC.
    pub executor_endpoint: Option<String>,
    /// How long the task has been running (ms).
    pub running_ms: u64,
    /// Median completed task duration for the stage (ms).
    pub median_ms: u64,
}

/// Deferred work for Phase 2.3 distributed write commit.
///
/// Produced by [`Coordinator::finalize_staged_sink_outputs`] and consumed by
/// the gRPC/in-process task-status handler outside the coordinator write lock.
#[derive(Debug)]
pub struct SinkFinalizeWork {
    pub job_id: JobId,
    pub state: JobState,
    pub specs: Vec<krishiv_common::write_commit::SinkWriteSpec>,
}

impl SinkFinalizeWork {
    /// Execute the blocking filesystem operations for this finalize work.
    /// Returns `true` if all publish operations succeeded, `false` if any failed
    /// (failure should cause the caller to mark the job as `Failed`).
    pub fn execute(self) -> bool {
        use krishiv_common::write_commit::{cleanup_staged_outputs, publish_staged_outputs};
        let mut all_ok = true;
        for spec in &self.specs {
            match self.state {
                JobState::Succeeded => match publish_staged_outputs(spec, self.job_id.as_str()) {
                    Ok(outcome) => {
                        tracing::info!(
                            job_id = %self.job_id,
                            dest = %spec.dest_path,
                            published = outcome.published.len(),
                            skipped_existing = outcome.skipped_existing,
                            ignored = outcome.ignored,
                            "published staged sink outputs"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            job_id = %self.job_id,
                            dest = %spec.dest_path,
                            error = %error,
                            "failed to publish staged sink outputs; failing job"
                        );
                        all_ok = false;
                    }
                },
                JobState::Failed | JobState::Cancelled => {
                    if let Err(error) = cleanup_staged_outputs(spec, self.job_id.as_str()) {
                        tracing::warn!(
                            job_id = %self.job_id,
                            dest = %spec.dest_path,
                            error = %error,
                            "failed to clean up staged sink outputs"
                        );
                    }
                }
                // `Committing` is never carried in the work item itself (the
                // work records the publish *intent* — `Succeeded` → publish);
                // the job record holds `Committing` while this runs.
                JobState::Queued
                | JobState::Accepted
                | JobState::Planning
                | JobState::Running
                | JobState::Committing => {}
            }
        }
        all_ok
    }
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("coordinator_id", &self.coordinator_id)
            .field("state", &self.state)
            .field("config", &self.config)
            .field("executors", &self.exec.executors)
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
///
/// # Lock acquisition order
///
/// To prevent deadlock, always acquire locks in the following order:
///
/// 1. `SharedCoordinator::inner` (Tokio async `RwLock`) — outermost, guards all `Coordinator` state.
/// 2. `SharedCoordinator::executor_inner` / `checkpoint_inner` (Tokio async `RwLock`) — shard locks;
///    never hold while trying to re-acquire `inner`.
/// 3. `JobCoordinator::inner` (std `RwLock<JobRecord>`) — per-job record; acquired via
///    `jc.read_record()` / `jc.write_record()`.
/// 4. `JobCoordinator::heartbeat_timestamps` (std `Mutex`) — always the innermost lock; never
///    acquire (3) while holding (4).
///
/// Any code that acquires locks in a different order risks a deadlock.  If you add a new
/// synchronization primitive, place it in this ordering and update this comment.
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
    /// E4.4: Live operator state registry for queryable-state REST API.
    /// Operator backends register here; deregistered on job eviction.
    pub queryable_state: Arc<QueryableStateStore>,
}

impl SharedCoordinator {
    /// Create a shared coordinator handle.
    pub fn new(coordinator: Coordinator) -> Self {
        // Clone the embedded inner state into the sharded locks so the fast
        // paths can read executor / checkpoint state without the full write lock.
        let executor_inner = coordinator.exec.clone();
        let checkpoint_inner = coordinator.ckpt.clone();
        let durability_profile = coordinator.durability_profile;
        Self {
            inner: Arc::new(RwLock::new(coordinator)),
            executor_inner: Arc::new(RwLock::new(executor_inner)),
            checkpoint_inner: Arc::new(RwLock::new(checkpoint_inner)),
            durability_profile,
            leader_fencing_token: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            queryable_state: Arc::new(QueryableStateStore::new()),
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

    /// Override per-job checkpoint coordinator fencing tokens with the current
    /// leader token.
    ///
    /// After recovery from checkpoint storage, each `CheckpointCoordinator`
    /// restores its fencing token from the last committed checkpoint metadata —
    /// a value written by the *previous* coordinator instance.  When a new
    /// leader instance starts (standalone restart or cluster leader election),
    /// it holds a different fencing token.  Calling this method ensures that
    /// subsequent `trigger_checkpoint_for_job` calls use the current leader
    /// token, preventing executors from rejecting checkpoint acks as stale (C8).
    pub async fn sync_checkpoint_fencing_tokens(&self, token: u64) {
        let new_token = match krishiv_proto::FencingToken::try_new(token) {
            Ok(t) => t,
            Err(_) => return,
        };
        // Update the outer (canonical) copy.
        let mut coord = self.inner.write().await;
        for c in coord.ckpt.coordinators.values_mut() {
            c.fencing_token = new_token;
        }
        // Drop the outer lock before acquiring inner — the gRPC checkpoint_ack
        // handler and drive_barrier_dispatches read from the inner lock's
        // coordinators, so every ack would be rejected as StaleFencingToken
        // until the next epoch advances if we don't propagate here.
        drop(coord);
        let mut ckpt_inner = self.checkpoint_inner.write().await;
        for c in ckpt_inner.coordinators.values_mut() {
            c.fencing_token = new_token;
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

    /// Submit a job through the shared coordinator and refresh sharded checkpoint state.
    pub async fn submit_job(&self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        let outcome = {
            let mut coord = self.inner.write().await;
            coord.submit_job(spec)?
        };

        // Add the newly submitted job's checkpoint coordinator to the inner lock
        // without clobbering any *other* job's in-flight epoch (C1 residual 1).
        // apply_monotonic_from also propagates the 4 delivery-tracking fields.
        let coord = self.inner.read().await;
        let mut ckpt_inner = self.checkpoint_inner.write().await;
        ckpt_inner.apply_monotonic_from(&coord.ckpt);

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
    #[tracing::instrument(skip(self), name = "advance_heartbeat_tick")]
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
            exec_inner.clone_from(&coord.exec);
            // C1 residual 1: periodic tick must not clobber an inner checkpoint
            // coordinator a concurrent ack already advanced further.
            // apply_monotonic_from: monotonic for coordinators, full replace for
            // the 4 delivery-tracking fields.
            ckpt_inner.apply_monotonic_from(&coord.ckpt);
            lost
        };

        // The per-job JCP consultation below is only load-bearing when an
        // executor was lost this tick; the launch-work summaries feed nothing
        // but the debug log. Skip the whole per-job walk (N jobs × several
        // async lock acquisitions each, every tick) on the common quiet path.
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        if lost.is_empty() && !debug_enabled {
            return Ok(lost);
        }

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

            // G5 follow-up (found live via the Phase-20 executor fault loop):
            // a continuous job's per-cycle input fence
            // (`continuous_input_cycles`) is normally cleared when its task
            // reports a terminal status (Succeeded/Failed/Cancelled) back to
            // `apply_task_update`. An executor that is hard-killed *while* it
            // holds that task never sends one, so the fence is never
            // released that way — and every future `continuous-push` 409s
            // forever. `reset_running_tasks_for_lost_executor` (called
            // earlier this tick, inside `advance_heartbeat_clock`) already
            // cleared the task's `assigned_executor` for us — by the time we
            // reach this loop it typically reads `None` again (which is also
            // why `jc.handle_executor_loss`'s own `affected` count above is
            // frequently 0 here: the same clearing already happened once).
            // Check that resulting state directly rather than depend on the
            // (redundant, order-sensitive) `affected` counter: whenever this
            // tick evicted at least one executor and a streaming job's cycle
            // fence is still set while its task now has no assigned
            // executor, the cycle can never complete on its own — release
            // the fence so the next explicit push can proceed.
            if !lost.is_empty() {
                let is_streaming_with_unassigned_task = {
                    let record = jc.read_record();
                    record.spec.kind() == JobKind::Streaming
                        && record
                            .stages()
                            .iter()
                            .flat_map(|s| s.tasks())
                            .any(|t| t.assigned_executor().is_none())
                };
                if is_streaming_with_unassigned_task {
                    let mut coord = self.inner.write().await;
                    if coord.continuous_input_cycles.remove(&job_id) {
                        coord.job_input_partitions.remove(&job_id);
                        tracing::warn!(
                            job_id = %job_id,
                            "continuous input-cycle fence released after executor loss \
                             left its task unassigned mid-cycle (was stuck without \
                             this — every push would 409)"
                        );
                    }
                }
            }

            if debug_enabled {
                let in_flight = jc.has_in_flight_tasks().await;
                let eligible = jc.has_tasks_eligible_for_launch().await;
                let (launch_eligible, stages_with_work) = jc.get_launch_work_summary().await;
                tracing::debug!(
                    job_id = %job_id,
                    in_flight,
                    eligible_for_launch = eligible,
                    launch_eligible_tasks = launch_eligible,
                    stages_with_pending_work = stages_with_work,
                    "JCP consulted during heartbeat tick (full per-job delegation)"
                );
            }
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

    /// R5 (production path): detect stalled tasks, send CancelTask RPCs, then
    /// reset them.
    ///
    /// The three-phase pattern mirrors `push_cancel_job`:
    /// 1. Read stall info under read lock — no mutation.
    /// 2. Send CancelTask RPCs concurrently outside any lock.
    /// 3. Apply state resets under write lock.
    ///
    /// This ensures the coordinator write lock is not held while waiting for
    /// gRPC round-trips, and the zombie executor receives a cancel signal before
    /// the coordinator re-queues the task for another executor.
    pub async fn detect_and_cancel_stalled_tasks(&self) {
        use crate::coordinator::task_assignment::inject_executor_task_request_context;
        use crate::in_process::is_in_process_task_endpoint;

        let work: Vec<StallCancelWork> = {
            let coord = self.inner.read().await;
            coord.collect_stall_cancel_work()
        };
        if work.is_empty() {
            return;
        }

        // Send CancelTask RPCs concurrently — outside any lock.
        let channels = {
            let coord = self.inner.read().await;
            coord.executor_channels.clone()
        };
        let mut cancel_futures = futures::stream::FuturesUnordered::new();
        for item in &work {
            let Some(ref endpoint) = item.executor_endpoint else {
                continue;
            };
            if is_in_process_task_endpoint(endpoint) {
                continue;
            }
            let endpoint = endpoint.clone();
            let channels = channels.clone();
            let attempt_id = match AttemptId::try_new(item.attempt) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let req = TaskCancellationRequest::new(TaskAttemptRef::new(
                item.job_id.clone(),
                item.stage_id.clone(),
                item.task_id.clone(),
                attempt_id,
            ))
            .with_reason("task stalled: no progress for >30 min");
            cancel_futures.push(async move {
                let channel =
                    match Coordinator::get_or_connect_channel_on_map(&channels, &endpoint).await {
                        Ok(c) => c,
                        Err(err) => {
                            tracing::warn!(endpoint = %endpoint, error = %err, "stall-cancel: connect failed");
                            return;
                        }
                    };
                let max = krishiv_proto::max_grpc_message_bytes();
                let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                    channel,
                    inject_executor_task_request_context
                        as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                )
                .max_decoding_message_size(max)
                .max_encoding_message_size(max);
                if let Err(err) = client
                    .cancel_task(wire::task_cancellation_request_to_wire(req))
                    .await
                {
                    tracing::warn!(endpoint = %endpoint, error = %err, "stall-cancel: cancel_task rpc failed");
                }
            });
        }
        use futures::stream::StreamExt as _;
        while cancel_futures.next().await.is_some() {}

        // Apply resets under write lock after RPCs complete.
        let mut coord = self.inner.write().await;
        coord.apply_stall_resets(&work);
    }

    /// Run one speculative-execution tick.
    ///
    /// Phase 53 (audit §3b) — the correct straggler protocol on the stall
    /// machinery: the running original receives a `CancelTask` RPC **before**
    /// the coordinator resets the task to `Pending`; the relaunch then bumps
    /// the attempt id, so no two executors ever run the same task/attempt
    /// concurrently and late status updates from the cancelled original are
    /// fenced as `StaleTaskAttempt`. First completion wins: the preempt
    /// re-checks `Running` at the observed attempt under the write lock, so a
    /// straggler that finished while the cancel was in flight keeps its
    /// result and is not re-run. Residual: a cancel RPC that never reaches a
    /// wedged executor leaves it burning a slot until stall detection or
    /// lease expiry reaps it — its results remain fenced either way.
    ///
    /// No-op when `speculative_execution_enabled` is `false`.  Called alongside
    /// `detect_and_cancel_stalled_tasks` in the daemon heartbeat loop.
    pub async fn run_speculative_execution(&self) {
        use crate::coordinator::task_assignment::inject_executor_task_request_context;
        use crate::in_process::is_in_process_task_endpoint;

        let work: Vec<SpeculativeWork> = {
            let coord = self.inner.read().await;
            coord.collect_speculation_work()
        };
        if work.is_empty() {
            return;
        }
        crate::metrics::SPECULATION_DETECTED_TOTAL
            .fetch_add(work.len() as u64, AtomicOrdering::Relaxed);
        for item in &work {
            tracing::info!(
                job_id = %item.job_id,
                stage_id = %item.stage_id,
                task_id = %item.task_id,
                running_ms = item.running_ms,
                median_ms = item.median_ms,
                "speculation: straggler detected — cancelling original before re-queue"
            );
        }

        // Send CancelTask RPCs to the straggler originals — outside any lock,
        // mirroring the stall-cancel three-phase pattern.
        let channels = {
            let coord = self.inner.read().await;
            coord.executor_channels.clone()
        };
        let mut cancel_futures = futures::stream::FuturesUnordered::new();
        for item in &work {
            let Some(ref endpoint) = item.executor_endpoint else {
                continue;
            };
            if is_in_process_task_endpoint(endpoint) {
                continue;
            }
            let endpoint = endpoint.clone();
            let channels = channels.clone();
            let attempt_id = match AttemptId::try_new(item.attempt) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let req = TaskCancellationRequest::new(TaskAttemptRef::new(
                item.job_id.clone(),
                item.stage_id.clone(),
                item.task_id.clone(),
                attempt_id,
            ))
            .with_reason("speculative preemption: straggler re-queued to another executor");
            cancel_futures.push(async move {
                let channel =
                    match Coordinator::get_or_connect_channel_on_map(&channels, &endpoint).await {
                        Ok(c) => c,
                        Err(err) => {
                            tracing::warn!(endpoint = %endpoint, error = %err, "speculation-cancel: connect failed");
                            return;
                        }
                    };
                let max = krishiv_proto::max_grpc_message_bytes();
                let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                    channel,
                    inject_executor_task_request_context
                        as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                )
                .max_decoding_message_size(max)
                .max_encoding_message_size(max);
                if let Err(err) = client
                    .cancel_task(wire::task_cancellation_request_to_wire(req))
                    .await
                {
                    tracing::warn!(endpoint = %endpoint, error = %err, "speculation-cancel: cancel_task rpc failed");
                }
            });
        }
        use futures::stream::StreamExt as _;
        while cancel_futures.next().await.is_some() {}

        // Apply preempt-resets under write lock after the cancels: the reset
        // re-checks Running-at-attempt, so an original that completed while
        // the cancel was in flight wins (its Succeeded update was applied
        // before the write lock was taken).
        let mut coord = self.inner.write().await;
        coord.apply_speculation_preempts(&work);
    }

    /// Wait for any coordinator state change notification (executor, checkpoint, etc.).
    /// Used by the daemon tick to react promptly instead of pure periodic polling.
    pub async fn wait_for_change(&self) {
        let notify = { self.inner.read().await.exec.notify.clone() };
        notify.notified().await;
    }

    /// Launch and push all assigned tasks for non-terminal jobs.
    pub async fn drive_pending_task_launches(&self) -> SchedulerResult<usize> {
        tracing::debug!("driving pending task launches for non-terminal jobs");

        // Phase 53: consume the dirty-job set so the 500 ms launch tick is
        // O(dirty jobs); every 8th tick falls back to a full sweep as a
        // safety net against missed dirty marks.
        let sweep = {
            let coord = self.read().await;
            coord
                .launch_sweep_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % 8
                == 0
        };
        let dirty: Option<std::collections::HashSet<JobId>> = if sweep {
            let mut coord = self.write().await;
            coord.launch_dirty_jobs.clear();
            None
        } else {
            let mut coord = self.write().await;
            let taken = std::mem::take(&mut coord.launch_dirty_jobs);
            Some(taken)
        };
        if let Some(d) = &dirty
            && d.is_empty()
        {
            return Ok(0);
        }

        // Build the list of jobs to drive, sorted by priority descending so
        // higher-priority jobs consume executor slots first.
        let job_ids = {
            let coord = self.read().await;
            let mut id_pairs: Vec<(u8, JobId)> = Vec::new();
            for (job_id, jc) in coord.job_coordinators.iter() {
                if let Some(d) = &dirty
                    && !d.contains(job_id)
                {
                    continue;
                }
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

        // Purely observability: several async JCP lock acquisitions per job
        // per 500 ms launch tick — only pay for them when debug logging is on.
        if tracing::enabled!(tracing::Level::DEBUG) {
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
                    Ok(assignments) => match coord.resolve_assignment_targets(assignments) {
                        Ok(targets) => {
                            if !targets.is_empty() {
                                launches.push(JobLaunch {
                                    job_id: job_id.clone(),
                                    targets,
                                });
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                job_id = %job_id,
                                error = %error,
                                "failed to resolve assignment targets; clearing in-flight launch state"
                            );
                            coord.clear_launch_in_flight_for_job(job_id);
                        }
                    },
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
                tokio::spawn(
                    async move {
                        let delivery = Coordinator::deliver_assignment_targets_with_channels(
                            (*channels).clone(),
                            targets,
                        )
                        .await;
                        (job_id, delivery)
                    }
                    .instrument(tracing::Span::current()),
                )
            })
            .collect();

        let delivery_results = futures::future::join_all(delivery_futures)
            .await
            .into_iter()
            .filter_map(|r| match r {
                Ok(result) => Some(result),
                Err(e) => {
                    if e.is_panic() {
                        tracing::error!("assignment delivery task panicked");
                    }
                    None
                }
            });

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
                            coord.exec.notify.notify_waiters();
                        }
                    }
                    Err(error @ SchedulerError::AssignmentRejected { .. }) => {
                        // Permanent rejection: the task payload is malformed, so
                        // re-delivery can never succeed. Fail the job terminally
                        // instead of clearing in-flight and retrying forever.
                        tracing::error!(
                            job_id = %job_id,
                            error = %error,
                            "task launch permanently rejected by executor; failing job"
                        );
                        coord.clear_launch_in_flight_for_job(&job_id);
                        if let Err(cancel_err) = coord.cancel_job(&job_id) {
                            tracing::warn!(
                                job_id = %job_id,
                                error = %cancel_err,
                                "failed to cancel job after permanent assignment rejection"
                            );
                        }
                    }
                    Err(SchedulerError::ExecutorUnavailable { endpoint, reason }) => {
                        // The executor was unreachable after the delivery retry
                        // budget (connection refused / repeated timeout = the
                        // pod is gone). Proactively mark it lost so its assigned
                        // tasks are reset and reassigned to a healthy executor
                        // on the next launch tick, instead of re-dispatching to
                        // the dead endpoint every 500ms until the heartbeat
                        // timeout eventually detects the loss (#206 — reproduced
                        // as transient query failures under executor churn on
                        // the k3s chaos cert). Reuses the existing, tested
                        // `mark_executor_lost` recovery path, just on an earlier
                        // signal.
                        tracing::warn!(
                            job_id = %job_id,
                            endpoint = %endpoint,
                            reason = %reason,
                            "task launch delivery failed; marking unreachable executor lost for fast reassignment"
                        );
                        coord.clear_launch_in_flight_for_job(&job_id);
                        if let Some(executor_id) = coord.executor_id_for_task_endpoint(&endpoint) {
                            if let Err(e) = coord.mark_executor_lost(&executor_id) {
                                tracing::debug!(
                                    executor = %executor_id,
                                    error = %e,
                                    "mark_executor_lost after delivery failure was a no-op (already lost)"
                                );
                            }
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
    /// for graceful termination before dropping.
    #[must_use = "dropping OrchestratorHandles immediately aborts all background loops"]
    pub fn spawn_orchestration_loops(&self) -> OrchestratorHandles {
        self.spawn_orchestration_loops_inner()
    }
}

impl SharedCoordinator {
    fn spawn_orchestration_loops_inner(&self) -> OrchestratorHandles {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut join_handles: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(3);

        // Heartbeat loop
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let span = tracing::info_span!("coordinator.heartbeat_loop");
            join_handles.push(tokio::spawn(
                async move {
                    // One loop iteration == one heartbeat tick, so the loop
                    // period MUST be `config.tick_period_ms()` — checkpoint
                    // interval timers and ack timeouts convert ticks to
                    // elapsed time with that constant. A hard-coded 5 s here
                    // against the old 1 000 ms default made every checkpoint
                    // interval and ack timeout run 5× slower than configured.
                    let period_ms = { coord.inner.read().await.config.tick_period_ms() };
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_millis(period_ms));
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
                        coord.detect_and_cancel_stalled_tasks().await;
                        coord.run_speculative_execution().await;
                    }
                }
                .instrument(span),
            ));
        }

        // Task launch loop
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let span = tracing::info_span!("coordinator.task_launch_loop");
            join_handles.push(tokio::spawn(
                async move {
                    let notify = coord.inner.read().await.exec.notify.clone();
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_millis(500));
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
                }
                .instrument(span),
            ));
        }

        // Barrier dispatch loop
        {
            let coord = self.clone();
            let mut rx = shutdown_rx.clone();
            let span = tracing::info_span!("coordinator.barrier_dispatch_loop");
            join_handles.push(tokio::spawn(
                async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {}
                            _ = rx.changed() => { if *rx.borrow() { return; } }
                        }
                        if let Err(error) =
                            drive_barrier_dispatches(&coord, std::time::Duration::from_secs(30))
                                .await
                        {
                            tracing::warn!(error = %error, "coordinator barrier dispatch failed");
                        }
                    }
                }
                .instrument(span),
            ));
        }

        // System metrics refresh loop — updates process and system gauges
        // (RSS, CPU, memory, threads) every 10 seconds for the Prometheus
        // and UI metrics endpoints.
        {
            let mut rx = shutdown_rx.clone();
            let span = tracing::info_span!("coordinator.system_metrics_loop");
            join_handles.push(tokio::spawn(
                async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {}
                            _ = rx.changed() => { if *rx.borrow() { return; } }
                        }
                        krishiv_metrics::system::system_metrics().refresh();
                    }
                }
                .instrument(span),
            ));
        }

        OrchestratorHandles {
            join_handles,
            shutdown_tx,
        }
    }
}

/// Handles for the background orchestration tasks spawned by
/// [`SharedCoordinator::spawn_orchestration_loops`].
///
/// **Must be kept alive** for the loops to run. Call [`OrchestratorHandles::shutdown`]
/// for graceful termination, or drop to signal-and-abort immediately.
pub struct OrchestratorHandles {
    join_handles: Vec<tokio::task::JoinHandle<()>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl OrchestratorHandles {
    /// Signal shutdown and wait for all loops to exit (with 5-second timeout).
    ///
    /// Loops check the shutdown watch on every tick; with a 2-second maximum
    /// tick interval all loops should exit well within 5 seconds. If they do
    /// not, they are aborted.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        let timeout = std::time::Duration::from_secs(5);
        for handle in self.join_handles.drain(..) {
            match tokio::time::timeout(timeout, handle).await {
                Ok(_) => {}
                Err(_) => {
                    tracing::warn!("orchestration loop did not exit within 5s; aborting");
                }
            }
        }
    }

    /// Abort all loops immediately (called from Drop; does not wait for exit).
    fn abort_all(&mut self) {
        let _ = self.shutdown_tx.send(true);
        for handle in &self.join_handles {
            handle.abort();
        }
    }
}

impl Drop for OrchestratorHandles {
    fn drop(&mut self) {
        self.abort_all();
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
        use crate::coordinator_sharded::{CheckpointInner, ExecutorInner};
        let notify = Arc::new(Notify::new());
        let exec = ExecutorInner {
            executors: ExecutorRegistry::new(
                config.heartbeat_timeout_ticks(),
                config.memory_threshold_bytes(),
            ),
            state,
            ticks_since_restart: u64::MAX,
            recovering: false,
            notify: notify.clone(),
        };
        let mut ckpt = CheckpointInner::new();
        ckpt.notify = notify.clone();
        Self {
            coordinator_id,
            state,
            config,
            cluster_manager: Arc::new(crate::cluster_control::NoopClusterManager),
            durability_profile: DurabilityProfile::DevLocal,
            exec,
            store: None,
            ckpt,
            queue_manager: Arc::new(InMemoryQueueManager),
            gc_ready_jobs: VecDeque::new(),
            gc_ready_at: std::collections::HashMap::new(),
            adaptive_decision_log: HashMap::new(),
            adaptive_override: AdaptiveOverrideConfig::default(),
            pending_backlog_jobs: HashSet::new(),
            launch_dirty_jobs: HashSet::new(),
            launch_sweep_counter: std::sync::atomic::AtomicU64::new(0),
            scheduler_pools: HashMap::new(),
            namespace_pools: HashMap::new(),
            streaming_task_index: HashMap::new(),
            streaming_job_task_index: HashMap::new(),
            executor_channels: Arc::new(DashMap::new()),
            job_inline_results: HashMap::new(),
            pending_task_result_spools: HashMap::new(),
            job_result_spools: HashMap::new(),
            batch_sql_job_tables: HashMap::new(),
            job_input_partitions: HashMap::new(),
            job_task_input_partitions: HashMap::new(),
            continuous_input_cycles: HashSet::new(),
            pending_continuous_restores: HashMap::new(),
            skew_repartition_overrides: HashMap::new(),
            pending_source_throttles: HashMap::new(),
            streaming_advisory_partitions: HashMap::new(),
            job_coordinators: HashMap::new(),
            aqe_coalesce_hints: HashMap::new(),
            pending_sink_finalize: Vec::new(),
            executor_job_watermarks: HashMap::new(),
            cascade_loss_timestamps: std::collections::VecDeque::new(),
            cascade_tripped_at_ms: None,
            job_submit_instants: HashMap::new(),
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

    /// Phase 53: configure scheduler pools (weight/min-share) and the
    /// namespace → pool mapping. Unmapped namespaces pool by namespace id.
    pub fn set_scheduler_pools(
        &mut self,
        pools: HashMap<String, crate::PoolSpec>,
        namespace_pools: HashMap<String, String>,
    ) {
        self.scheduler_pools = pools;
        self.namespace_pools = namespace_pools;
    }

    /// Phase 53: resolve the pool a namespace belongs to.
    pub(crate) fn pool_for_namespace(&self, namespace: Option<&str>) -> String {
        let ns = namespace.unwrap_or_default();
        self.namespace_pools
            .get(ns)
            .cloned()
            .unwrap_or_else(|| ns.to_owned())
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

    /// SC14: attach a custom [`crate::cluster_control::ClusterManager`].
    ///
    /// The default is [`crate::cluster_control::NoopClusterManager`]
    /// (a no-op for bare-metal and `clusterd` modes). Kubernetes mode
    /// passes an implementation that talks to the operator CRD API.
    #[must_use]
    pub fn with_cluster_manager(
        mut self,
        manager: Arc<dyn crate::cluster_control::ClusterManager>,
    ) -> Self {
        self.cluster_manager = manager;
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

    /// Remove a persisted continuous job snapshot. No-op when no store is
    /// configured. Called on deregister so a later job registered with the
    /// same id starts from a clean slate instead of silently inheriting a
    /// stale watermark/state left over from the retired job.
    pub fn remove_continuous_snapshot(&self, job_id: &str) {
        if let Some(store) = &self.store {
            store.remove_continuous_snapshot(job_id);
        }
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
        &self.exec.executors
    }

    /// Heartbeat ticks since coordinator restart.
    pub fn ticks_since_restart(&self) -> u64 {
        self.exec.executors.current_tick
    }

    /// Whether the coordinator is in recovery mode.
    pub fn recovering(&self) -> bool {
        self.exec.recovering
    }

    /// Notify handle for wake-on-state-change.
    pub fn notify(&self) -> &Arc<Notify> {
        &self.exec.notify
    }

    /// Executor-facing inner state snapshot (for constructing sharded inner locks).
    pub fn exec_inner_snapshot(&self) -> crate::coordinator_sharded::ExecutorInner {
        self.exec.clone()
    }

    /// Checkpoint-control inner state snapshot (for constructing sharded inner locks).
    pub fn checkpoint_inner_snapshot(&self) -> crate::coordinator_sharded::CheckpointInner {
        self.ckpt.clone()
    }

    /// Promote a standby coordinator to active leader.
    pub fn promote_to_active(&mut self) {
        self.state = CoordinatorState::Active;
        self.exec.state = CoordinatorState::Active;
    }

    /// Demote to standby when leadership is lost.
    pub fn demote_to_standby(&mut self) {
        self.state = CoordinatorState::Standby;
        self.exec.state = CoordinatorState::Standby;
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

    /// Advance the sync coordinator state machine one tick.
    ///
    /// Launches newly-eligible assigned tasks (e.g. stages whose upstream
    /// shuffle dependencies completed since the last tick) and returns those
    /// assignments: the sync tick has no transport, so the *caller* owns
    /// dispatch. The launched tasks are marked in-flight and will not be
    /// re-launched — dropping the return value loses them permanently
    /// (upstream-gated stages then hang in `Scheduling` forever).
    pub fn coordinator_tick(
        &mut self,
    ) -> SchedulerResult<Vec<krishiv_proto::ExecutorTaskAssignment>> {
        let lost = self.advance_heartbeat_clock(1)?;
        // H-5: Mirror JCP executor-loss handling (same logic as the async
        // `SharedCoordinator::advance_heartbeat_tick`). Without this the JCP's
        // copy of job state retains stale executor assignments after a loss.
        for lost_id in &lost {
            for jc in self.job_coordinators.values() {
                jc.handle_executor_loss_sync(lost_id);
            }
        }
        let job_ids: Vec<JobId> = self.job_coordinators.keys().cloned().collect();
        let mut launched = Vec::new();
        for job_id in &job_ids {
            launched.extend(self.launch_assigned_task_assignments(job_id)?);
        }
        // R5: Stall detection — reset Running tasks whose executor is still
        // alive (heartbeating) but the task itself has not progressed past
        // TaskState::Running for longer than the stall timeout. This catches
        // deadlocked operators that block a thread without crashing the executor.
        self.detect_and_reset_stalled_tasks();
        Ok(launched)
    }

    /// R5: Collect stalled Running tasks that have exceeded the configured
    /// `task_stall_timeout_ms` (default: 30 minutes).
    ///
    /// Pure read — does not mutate any state. Callers send `CancelTask` RPCs
    /// from the returned targets, then call [`Self::apply_stall_resets`].
    pub(crate) fn collect_stall_cancel_work(&self) -> Vec<StallCancelWork> {
        let stall_timeout_ms = self.config.task_stall_timeout_ms();
        let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
        let mut work = Vec::new();
        for jc in self.job_coordinators.values() {
            let record = jc.read_record();
            for stage in record.stages() {
                for task in stage.tasks() {
                    if task.state() != krishiv_proto::TaskState::Running {
                        continue;
                    }
                    if let Some(assigned_ms) = task.assigned_at_ms
                        && now_ms.saturating_sub(task.last_progress_ms.unwrap_or(assigned_ms))
                            > stall_timeout_ms
                    {
                        let executor_endpoint = task
                            .assigned_executor()
                            .and_then(|eid| self.exec.executors.find_executor(eid).ok())
                            .and_then(|rec| rec.descriptor().task_endpoint().map(str::to_owned));
                        work.push(StallCancelWork {
                            job_id: record.job_id().clone(),
                            stage_id: stage.stage_id().clone(),
                            task_id: task.task_id().clone(),
                            attempt: task.attempt(),
                            executor_endpoint,
                            stall_secs: now_ms.saturating_sub(assigned_ms) / 1000,
                        });
                    }
                }
            }
        }
        work
    }

    /// Apply stall resets: mark each task in `work` as Failed and clear
    /// in-flight state. Must be called after cancel RPCs are sent.
    pub(crate) fn apply_stall_resets(&mut self, work: &[StallCancelWork]) {
        for item in work {
            let Some(jc) = self.job_coordinators.get(&item.job_id) else {
                continue;
            };
            let mut record = jc.write_record();
            for stage in record.stages_mut() {
                if stage.stage_id() != &item.stage_id {
                    continue;
                }
                let mut stage_affected = false;
                for task in stage.tasks_mut() {
                    if task.task_id() != &item.task_id || task.attempt() != item.attempt {
                        continue;
                    }
                    if task.state() != krishiv_proto::TaskState::Running {
                        continue;
                    }
                    tracing::warn!(
                        task_id = %item.task_id,
                        stall_secs = item.stall_secs,
                        "resetting stalled task (no progress for >30 min)"
                    );
                    task.state = krishiv_proto::TaskState::Failed;
                    task.last_failure_reason = Some(format!(
                        "task stalled: no progress for {} min",
                        item.stall_secs / 60,
                    ));
                    task.launch_in_flight = false;
                    task.assigned_at_ms = None;
                    task.last_progress_ms = None;
                    stage_affected = true;
                }
                if stage_affected {
                    stage.refresh_state();
                }
            }
            record.refresh_state();
        }
    }

    /// Legacy sync path used by tests. Sends no cancel RPCs (test executors
    /// are in-process and don't need them); prefer the async
    /// [`SharedCoordinator::detect_and_cancel_stalled_tasks`] in production.
    fn detect_and_reset_stalled_tasks(&mut self) {
        let work = self.collect_stall_cancel_work();
        self.apply_stall_resets(&work);
    }

    /// Collect straggler tasks eligible for speculative preemption.
    ///
    /// Returns `Vec<SpeculativeWork>` for each Running task in a Running
    /// `ShuffleMap` stage where:
    ///   - at least `config.speculative_min_completed_tasks` tasks in the stage
    ///     have Succeeded (so we have a stable median to compare against), and
    ///   - the task has been Running for more than
    ///     `median_completed_ms * config.speculative_slowdown_factor`.
    ///
    /// Only fires when `config.speculative_execution_enabled()` is `true`.
    /// Never speculates on streaming jobs (straggler semantics don't apply).
    pub(crate) fn collect_speculation_work(&self) -> Vec<SpeculativeWork> {
        let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
        self.collect_speculation_work_at(now_ms)
    }

    /// Like [`collect_speculation_work`] but accepts an explicit `now_ms` so
    /// tests can inject deterministic wall-clock values.
    pub(crate) fn collect_speculation_work_at(&self, now_ms: u64) -> Vec<SpeculativeWork> {
        if !self.config.speculative_execution_enabled() {
            return Vec::new();
        }
        let factor = self.config.speculative_slowdown_factor();
        let min_completed = self.config.speculative_min_completed_tasks();
        let mut work = Vec::new();

        for jc in self.job_coordinators.values() {
            let record = jc.read_record();
            // Skip streaming jobs — speculative execution is batch-only.
            if record.spec.kind() == krishiv_proto::JobKind::Streaming {
                continue;
            }
            for stage in record.stages() {
                // Only speculate on Running ShuffleMap stages.
                if stage.state() != krishiv_proto::StageState::Running {
                    continue;
                }
                if stage.spec.kind() != krishiv_proto::StageKind::ShuffleMap {
                    continue;
                }

                // Compute median duration of Succeeded tasks in this stage.
                let mut durations_ms: Vec<u64> = stage
                    .tasks()
                    .iter()
                    .filter(|t| t.state() == krishiv_proto::TaskState::Succeeded)
                    .filter_map(|t| t.completed_duration_ms)
                    .collect();

                if durations_ms.len() < min_completed {
                    continue;
                }

                durations_ms.sort_unstable();
                let median_ms = durations_ms
                    .get(durations_ms.len() / 2)
                    .copied()
                    .unwrap_or(0);
                if median_ms == 0 {
                    continue;
                }
                let threshold_ms = (median_ms as f64 * factor) as u64;

                for task in stage.tasks() {
                    if task.state() != krishiv_proto::TaskState::Running {
                        continue;
                    }
                    // Sink gate (Phase 53): never speculate a task that owns a
                    // side-effecting sink contract. ShuffleMap stages should
                    // not carry sinks, but the check makes the precondition
                    // explicit rather than conventional.
                    if task.spec.sink_contract().is_some() {
                        continue;
                    }
                    let Some(started_ms) = task.assigned_at_ms else {
                        continue;
                    };
                    let running_ms = now_ms.saturating_sub(started_ms);
                    if running_ms <= threshold_ms {
                        continue;
                    }
                    let executor_endpoint = task
                        .assigned_executor()
                        .and_then(|eid| self.exec.executors.find_executor(eid).ok())
                        .and_then(|rec| rec.descriptor().task_endpoint().map(str::to_owned));
                    work.push(SpeculativeWork {
                        job_id: record.job_id().clone(),
                        stage_id: stage.stage_id().clone(),
                        task_id: task.task_id().clone(),
                        attempt: task.attempt(),
                        executor_endpoint,
                        running_ms,
                        median_ms,
                    });
                }
            }
        }
        work
    }

    /// Preemptively re-schedule straggler tasks identified by
    /// [`collect_speculation_work`].
    ///
    /// Each task is reset to `Pending` *without* consuming a retry attempt —
    /// this is a preemptive preemption, not a failure-driven retry.  The task
    /// will be re-assigned to a (possibly different) executor on the next
    /// `launch_assigned_task_assignments` call.
    pub(crate) fn apply_speculation_preempts(&mut self, work: &[SpeculativeWork]) {
        for item in work {
            let Some(jc) = self.job_coordinators.get(&item.job_id) else {
                continue;
            };
            let mut record = jc.write_record();
            let mut stage_affected = false;
            for stage in record.stages_mut() {
                if stage.stage_id() != &item.stage_id {
                    continue;
                }
                for task in stage.tasks_mut() {
                    if task.task_id() != &item.task_id || task.attempt() != item.attempt {
                        continue;
                    }
                    if task.state() != krishiv_proto::TaskState::Running {
                        continue;
                    }
                    tracing::info!(
                        task_id = %item.task_id,
                        running_ms = item.running_ms,
                        median_ms = item.median_ms,
                        "speculative preemption: re-scheduling straggler task"
                    );
                    crate::metrics::SPECULATION_PREEMPTED_TOTAL
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    // Reset to Pending without incrementing failure_count so
                    // the per-task retry budget is not consumed. The relaunch
                    // bumps the attempt id, fencing late updates from the
                    // cancelled original.
                    task.state = krishiv_proto::TaskState::Pending;
                    task.assigned_executor = None;
                    task.launch_in_flight = false;
                    task.assigned_at_ms = None;
                    task.last_progress_ms = None;
                    stage_affected = true;
                }
                if stage_affected {
                    stage.refresh_state();
                }
            }
            if stage_affected {
                record.refresh_state();
            }
        }
    }

    // ── SC11: cascade circuit breaker ─────────────────────────────────────

    /// Record one executor loss and potentially trip the cascade circuit breaker.
    ///
    /// `now_ms` is the wall-clock time at which the loss occurred.  Old entries
    /// outside the `cascade_window_ms` are pruned before the count is evaluated.
    pub(crate) fn record_cascade_loss(&mut self, now_ms: u64) {
        let window = self.config.cascade_window_ms();
        let cutoff = now_ms.saturating_sub(window);
        while self
            .cascade_loss_timestamps
            .front()
            .copied()
            .unwrap_or(u64::MAX)
            < cutoff
        {
            self.cascade_loss_timestamps.pop_front();
        }
        self.cascade_loss_timestamps.push_back(now_ms);

        let threshold = self.config.cascade_failure_threshold();
        if self.cascade_loss_timestamps.len() >= threshold && self.cascade_tripped_at_ms.is_none() {
            self.cascade_tripped_at_ms = Some(now_ms);
            tracing::warn!(
                losses_in_window = self.cascade_loss_timestamps.len(),
                window_ms = window,
                cooldown_ms = self.config.cascade_cooldown_ms(),
                "SC11: cascade circuit breaker OPEN — suspending task assignments"
            );
        }
    }

    /// Returns `true` when the cascade circuit breaker is open (assignments
    /// are paused) at `now_ms`.  Automatically resets the breaker if the
    /// cooldown period has elapsed.
    pub(crate) fn cascade_breaker_open(&mut self, now_ms: u64) -> bool {
        let Some(tripped_at) = self.cascade_tripped_at_ms else {
            return false;
        };
        let cooldown = self.config.cascade_cooldown_ms();
        if now_ms.saturating_sub(tripped_at) >= cooldown {
            self.cascade_tripped_at_ms = None;
            tracing::info!(
                tripped_at_ms = tripped_at,
                cooldown_ms = cooldown,
                "SC11: cascade circuit breaker CLOSED — resuming task assignments"
            );
            return false;
        }
        true
    }

    /// Collect staged sink finalization work for a job that just reached a
    /// terminal state (Phase 2.3 distributed write commit).
    ///
    /// This method only reads state; it does **not** perform any filesystem I/O.
    /// The returned [`SinkFinalizeWork`] is pushed to `self.pending_sink_finalize`
    /// so the caller can drain it after releasing the coordinator write lock and
    /// run the blocking operations via `tokio::task::spawn_blocking`.
    ///
    /// - `Succeeded`: marks parts for atomic publish (rename / copy+delete fallback).
    /// - `Failed` / `Cancelled`: marks staged files for cleanup.
    ///
    /// Both operations are idempotent; re-entry on duplicate terminal updates
    /// converges.
    pub(crate) fn finalize_staged_sink_outputs(&mut self, job_id: &JobId) {
        let state = match self.job_coordinators.get(job_id) {
            Some(jc) => jc.read_record().state(),
            None => return,
        };
        if !state.is_terminal() {
            return;
        }
        let specs = self.staged_sink_specs(job_id);
        if specs.is_empty() {
            return;
        }

        // DUR-1: a `Succeeded` job with staged output must not be persisted
        // `Succeeded` until the publish durably completes. Demote it to the
        // non-terminal `Committing` state now — *before* the caller persists
        // this record in `apply_task_update` — so a crash in the publish window
        // recovers as `Committing` and re-drives the (idempotent) publish rather
        // than trusting a false success. The caller resolves it via
        // `mark_sink_publish_committed` (→ `Succeeded`) or
        // `mark_sink_publish_failed` (→ `Failed`). `Failed`/`Cancelled` jobs
        // only clean up staging and stay terminal, so they are not demoted.
        if state == JobState::Succeeded
            && let Ok(mut record) = self.find_job_mut(job_id)
        {
            record.state = JobState::Committing;
        }
        self.pending_sink_finalize.push(SinkFinalizeWork {
            job_id: job_id.clone(),
            // Publish *intent*, not the job's live state: `Succeeded` → publish,
            // `Failed`/`Cancelled` → cleanup. The record now holds `Committing`.
            state,
            specs,
        });
    }

    /// Parse the staged object-sink specs declared by a job's tasks. Shared by
    /// the live finalize path and the `Committing` re-drive on recovery so both
    /// reconstruct exactly the same publish set from the persisted job spec.
    pub(crate) fn staged_sink_specs(
        &self,
        job_id: &JobId,
    ) -> Vec<krishiv_common::write_commit::SinkWriteSpec> {
        use krishiv_common::write_commit::SinkWriteSpec;
        const SINK_PREFIX: &str = "object-parquet-sink:";

        let Some(jc) = self.job_coordinators.get(job_id) else {
            return Vec::new();
        };
        let record = jc.read_record();
        let mut specs = Vec::new();
        for payload in record
            .spec
            .stages()
            .iter()
            .flat_map(|stage| stage.tasks())
            .filter_map(|task| task.sink_contract())
            .filter_map(|contract| contract.trim().strip_prefix(SINK_PREFIX).map(str::to_owned))
        {
            match SinkWriteSpec::parse(&payload) {
                Ok(spec) if spec.staged => specs.push(spec),
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(job_id = %job_id, error = %error, "invalid sink contract during finalize");
                }
            }
        }
        specs
    }

    /// Drain all pending sink finalization work. Callers release the coordinator
    /// write lock before executing the returned work via `spawn_blocking`.
    pub fn take_pending_sink_finalize(&mut self) -> Vec<SinkFinalizeWork> {
        std::mem::take(&mut self.pending_sink_finalize)
    }

    /// Promote a `Committing` job to `Succeeded` after its staged sink output
    /// has durably published (DUR-1). Persists the terminal state
    /// **synchronously** — the durable `Succeeded` write happens only now, after
    /// the publish — then runs the terminal bookkeeping deferred at
    /// `Committing` time. No-op if the job is not `Committing` (e.g. a duplicate
    /// finalize, or a job that never staged output).
    pub(crate) fn mark_sink_publish_committed(&mut self, job_id: &JobId) {
        {
            let Ok(mut job) = self.find_job_mut(job_id) else {
                return;
            };
            if job.state() != JobState::Committing {
                return;
            }
            job.state = JobState::Succeeded;
        }
        if let Err(e) = self.persist_job_record(job_id, true) {
            tracing::error!(
                job_id = %job_id,
                error = %e,
                "failed to persist Succeeded after sink publish; job remains recoverable as Committing"
            );
        }
        self.on_job_terminal(job_id);
    }

    /// Mark a job `Failed` after a publish step failed outside the write lock
    /// (DUR-1). Persists the terminal state synchronously and runs terminal
    /// bookkeeping. No-op if the job is already terminal.
    pub(crate) fn mark_sink_publish_failed(&mut self, job_id: &JobId) {
        {
            let Ok(mut job) = self.find_job_mut(job_id) else {
                return;
            };
            if job.state().is_terminal() {
                return;
            }
            job.state = JobState::Failed;
        }
        if let Err(e) = self.persist_job_record(job_id, true) {
            tracing::error!(
                job_id = %job_id,
                error = %e,
                "failed to persist Failed after sink publish failure"
            );
        }
        self.on_job_terminal(job_id);
    }

    /// Re-drive jobs found in the non-terminal `Committing` state after a
    /// coordinator restart (DUR-1). Their staged output may or may not have been
    /// published before the crash; `publish_staged_outputs` is idempotent (it
    /// skips already-present destination files), so re-running it is safe. Each
    /// job is then resolved to a terminal state and persisted via the recovery
    /// `store` (which is not yet installed as `self.store` at recovery time).
    ///
    /// Runs synchronously at startup, before the coordinator serves traffic, so
    /// the blocking filesystem publish is acceptable here.
    pub(crate) fn redrive_committing_jobs(&mut self, store: &mut dyn MetadataStore) {
        use krishiv_common::write_commit::publish_staged_outputs;

        let committing: Vec<JobId> = self
            .job_coordinators
            .iter()
            .filter(|(_, jc)| jc.read_record().state() == JobState::Committing)
            .map(|(id, _)| id.clone())
            .collect();

        for job_id in committing {
            let specs = self.staged_sink_specs(&job_id);
            let mut all_ok = true;
            for spec in &specs {
                if let Err(error) = publish_staged_outputs(spec, job_id.as_str()) {
                    tracing::error!(
                        job_id = %job_id,
                        dest = %spec.dest_path,
                        error = %error,
                        "DUR-1 recovery: failed to re-publish staged sink output; failing job"
                    );
                    all_ok = false;
                }
            }
            let resolved = if all_ok {
                JobState::Succeeded
            } else {
                JobState::Failed
            };
            if let Ok(mut job) = self.find_job_mut(&job_id) {
                job.state = resolved;
            }
            // Persist the resolved terminal state via the recovery store.
            if let Some(jc) = self.job_coordinators.get(&job_id) {
                let record = jc.read_record();
                if let Err(e) = store.save_job(&record) {
                    tracing::error!(
                        job_id = %job_id,
                        error = %e,
                        "DUR-1 recovery: failed to persist resolved state for Committing job"
                    );
                }
            }
            self.on_job_terminal(&job_id);
            tracing::warn!(
                job_id = %job_id,
                resolved = %resolved,
                "DUR-1 recovery: re-drove Committing job to terminal state after restart"
            );
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

pub(crate) mod aqe;
mod checkpoint_ops;
mod executor_ops;
mod heartbeat_mapping;
pub(crate) use heartbeat_mapping::{
    executor_heartbeat_from_request, executor_heartbeat_response_from_effects,
};
mod job_lifecycle;
pub mod observability;
mod recovery;
mod snapshots;
mod streaming;
pub(crate) mod task_assignment;
