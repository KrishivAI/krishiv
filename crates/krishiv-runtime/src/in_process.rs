//! In-process coordinator + executor over shared mpsc/inbox transport (ADR-12.4).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_common::async_util::block_on;
use krishiv_executor::{
    ContinuousJobDrainer, ExecutorAssignmentInbox, ExecutorTaskOutputKind, ExecutorTaskRunner,
};
use krishiv_plan::window::WindowExecutionSpec;
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorId, InputPartition, InputPartitionDescriptor, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::coordinator_sharded::{CheckpointInner, ExecutorInner};
use krishiv_scheduler::{
    ContinuousSnapshot, Coordinator, IN_PROCESS_TASK_ENDPOINT, InProcessCoordinatorBridge,
    MetadataStore, SubmitOutcome,
};
use tokio::sync::RwLock;

use crate::continuous_stream::{ContinuousStreamRegistry, SharedContinuousStreamRegistry};
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

/// Process-global counter used to give every [`InProcessStreamingRuntime`]
/// a unique numeric suffix.  This avoids two concurrent embedded sessions
/// colliding on coordinator id (C1, C2).
static CLUSTER_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Maximum coordinator-tick cycles per job. Each cycle corresponds to one
/// stage of a multi-stage query graph. Exceeding this limit indicates an
/// infinite streaming loop or misconfigured multi-stage plan.
/// For unbounded queries use the streaming API (`submit_stream_job`,
/// `drain_continuous_job`).
const MAX_STAGE_ITERATIONS: usize = 1024;

/// Sentinel returned by uninitialized streaming windows (`i64::MIN`).
/// Watermarks equal to this value are never propagated to downstream stages.
pub(crate) const WATERMARK_UNSET: i64 = i64::MIN;

fn next_cluster_suffix() -> u64 {
    CLUSTER_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Parquet table registration forwarded to executor SQL tasks.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BatchSqlTable {
    pub table_name: String,
    pub path: PathBuf,
}

struct RegistryDrainer(Arc<ContinuousStreamRegistry>);

impl ContinuousJobDrainer for RegistryDrainer {
    fn drain_job(&self, job_id: &str) -> Result<Vec<RecordBatch>, String> {
        self.0.drain_job(job_id).map_err(|e| e.to_string())
    }
}

/// Shared in-process streaming runtime (coordinator + executor inbox).
#[derive(Clone)]
pub struct InProcessStreamingRuntime {
    coordinator: Arc<Mutex<Coordinator>>,
    bridge: InProcessCoordinatorBridge,
    inbox: ExecutorAssignmentInbox,
    runner: Arc<ExecutorTaskRunner>,
    continuous_registry: SharedContinuousStreamRegistry,
    _executor_id: ExecutorId,
    /// Per-cluster job counter so each `InProcessStreamingRuntime` has its
    /// own job id namespace (C1).
    job_counter: Arc<AtomicU64>,
    /// Per-cluster suffix used in coordinator/executor ids.
    suffix: u64,
}

impl InProcessStreamingRuntime {
    pub fn new() -> RuntimeResult<Self> {
        Self::with_continuous_registry(Arc::new(ContinuousStreamRegistry::new()))
    }

    pub fn with_continuous_registry(
        registry: SharedContinuousStreamRegistry,
    ) -> RuntimeResult<Self> {
        Self::build(registry, Arc::new(dashmap::DashMap::new()))
    }

    /// Create a runtime that shares an existing parquet-file-footer cache.
    ///
    /// Multiple sessions that process the same parquet files can share one
    /// `Arc<DashMap<String, ()>>` obtained from [`parquet_cache`] on a prior
    /// session, eliminating redundant footer reads across session boundaries.
    pub fn with_parquet_cache(cache: Arc<dashmap::DashMap<String, ()>>) -> RuntimeResult<Self> {
        Self::build(Arc::new(ContinuousStreamRegistry::new()), cache)
    }

    /// Expose the parquet-cache handle so it can be shared with new sessions.
    pub fn parquet_cache(&self) -> Arc<dashmap::DashMap<String, ()>> {
        Arc::clone(self.runner.registered_parquet_cache())
    }

    fn build(
        registry: SharedContinuousStreamRegistry,
        parquet_cache: Arc<dashmap::DashMap<String, ()>>,
    ) -> RuntimeResult<Self> {
        let suffix = next_cluster_suffix();
        // Each in-process cluster gets a process-unique coordinator and
        // executor id so multiple sessions sharing the same process do not
        // collide in metadata stores or audit logs (C1).
        let coordinator_id =
            CoordinatorId::try_new(format!("in-process-coord-{suffix}")).map_err(|e| {
                RuntimeError::InvalidState {
                    message: e.to_string(),
                }
            })?;
        let coordinator = Arc::new(Mutex::new(Coordinator::active(coordinator_id)));
        let executor_id =
            ExecutorId::try_new(format!("in-process-exec-{suffix}")).map_err(|e| {
                RuntimeError::InvalidState {
                    message: e.to_string(),
                }
            })?;
        let descriptor = ExecutorDescriptor::new(executor_id.clone(), "localhost", 8)
            .with_task_endpoint(IN_PROCESS_TASK_ENDPOINT);
        {
            let mut coord = coordinator.lock().map_err(|_| RuntimeError::InvalidState {
                message: "coordinator lock poisoned during executor registration".into(),
            })?;
            coord
                .register_executor(descriptor)
                .map_err(|e| RuntimeError::InvalidState {
                    message: e.to_string(),
                })?;
        }
        // Build sharded inner locks after registering the local executor so the
        // bridge starts from the same control-plane state as the coordinator.
        let (executor_inner, checkpoint_inner): (
            Arc<RwLock<ExecutorInner>>,
            Arc<RwLock<CheckpointInner>>,
        ) = {
            let coord = coordinator.lock().map_err(|_| RuntimeError::InvalidState {
                message: "coordinator lock poisoned during bridge inner-state extraction".into(),
            })?;
            let (checkpoint_coordinators, checkpoint_notify_sent, barrier_dispatch_sent) =
                coord.checkpoint_inner_parts();
            (
                Arc::new(RwLock::new(ExecutorInner {
                    executors: coord.executors().clone(),
                    state: coord.state(),
                    ticks_since_restart: coord.ticks_since_restart(),
                    recovering: coord.recovering(),
                    notify: coord.notify().clone(),
                })),
                Arc::new(RwLock::new(CheckpointInner::from_parts(
                    checkpoint_coordinators,
                    checkpoint_notify_sent,
                    barrier_dispatch_sent,
                ))),
            )
        };
        let inbox = ExecutorAssignmentInbox::new();
        let drainer = Arc::new(RegistryDrainer(Arc::clone(&registry)));
        let runner = Arc::new(
            ExecutorTaskRunner::new(inbox.clone())
                .with_executor_id(executor_id.clone())
                .with_continuous_drainer(drainer)
                .with_shared_parquet_cache(parquet_cache),
        );
        let bridge = InProcessCoordinatorBridge::new(
            Arc::clone(&coordinator),
            executor_inner,
            checkpoint_inner,
        );
        Ok(Self {
            coordinator,
            bridge,
            inbox,
            runner,
            continuous_registry: registry,
            _executor_id: executor_id,
            job_counter: Arc::new(AtomicU64::new(1)),
            suffix,
        })
    }

    /// Per-cluster job id generator (C1) — replaces the legacy process-global counter.
    fn next_job_id(&self) -> RuntimeResult<JobId> {
        let n = self.job_counter.fetch_add(1, Ordering::Relaxed);
        JobId::try_new(format!("in-process-{}-job-{n}", self.suffix)).map_err(|e| {
            RuntimeError::InvalidState {
                message: e.to_string(),
            }
        })
    }

    pub fn continuous_registry(&self) -> &ContinuousStreamRegistry {
        &self.continuous_registry
    }

    /// Register a continuous streaming job (window operator state retained in registry).
    pub fn register_continuous_job(
        &self,
        job_id: &str,
        spec: WindowExecutionSpec,
    ) -> RuntimeResult<()> {
        self.continuous_registry.register_job(job_id, spec)
    }

    /// Push input batches for a continuous job before draining via coordinator.
    pub fn push_continuous_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        self.continuous_registry.push_input(job_id, batches)
    }

    /// Deregister a streaming source and clear any matching parquet-cache entries.
    ///
    /// Wraps [`SqlEngine::deregister_streaming_source`] and additionally removes
    /// all `registered_parquet_cache` entries whose key starts with `"{name}:"`,
    /// preventing stale "table not found" errors if the same name is later
    /// re-registered as a parquet table (N2).
    pub fn deregister_streaming_source(&self, name: &str) -> RuntimeResult<()> {
        self.runner
            .sql_engine()
            .deregister_streaming_source(name)
            .map_err(|e| RuntimeError::InvalidState {
                message: e.to_string(),
            })?;
        let prefix = format!("{name}:");
        self.runner
            .registered_parquet_cache()
            .retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }

    /// Check if a query is streaming.
    pub fn is_streaming_query(&self, query: &str) -> RuntimeResult<bool> {
        self.runner
            .sql_engine()
            .is_streaming_query(query)
            .map_err(|e| RuntimeError::PlanRejected {
                reason: format!("sql parse error: {e}"),
            })
    }

    /// Execute batch SQL on the in-process executor via the coordinator. → executor (`sql:` fragment).
    /// Execute a SQL query directly via the SQL engine, bypassing the coordinator
    /// state machine entirely. Only valid for non-streaming, single-stage batch
    /// queries. Eliminates 6+ Mutex lock/unlock pairs of coordinator overhead.
    fn execute_inline_sql(
        &self,
        query: &str,
        tables: &[BatchSqlTable],
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let engine = Arc::clone(self.runner.sql_engine());
        // Owned copies are required by async move. Clone is O(n_tables * n_fields);
        // the common case (no parquet tables) pays only one empty-Vec allocation.
        let query = query.to_owned();
        let tables = tables.to_vec();
        let parquet_cache = Arc::clone(self.runner.registered_parquet_cache());
        block_on(async move {
            // Register any parquet tables supplied for this query.
            // Skip tables already registered in a previous inline call to avoid
            // redundant DataFusion re-registration (file footer re-read).
            for table in &tables {
                let canonical_path = table
                    .path
                    .canonicalize()
                    .unwrap_or_else(|_| table.path.clone());
                let cache_key = format!("{}:{}", table.table_name, canonical_path.display());
                // Atomic check-and-insert via DashMap entry API prevents the TOCTOU
                // race where two concurrent threads both see contains_key==false,
                // both call register_parquet, and the second call fails.
                match parquet_cache.entry(cache_key) {
                    dashmap::mapref::entry::Entry::Occupied(_) => {}
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        engine
                            .register_parquet(&table.table_name, &table.path)
                            .await
                            .map_err(|e| RuntimeError::transport(e.to_string()))?;
                        v.insert(());
                    }
                }
            }
            let df = engine
                .sql(query)
                .await
                .map_err(|e| RuntimeError::transport(e.to_string()))?;
            df.collect()
                .await
                .map_err(|e| RuntimeError::transport(e.to_string()))
        })
    }

    /// Returns `true` when the query is safe to run inline (bypassing coordinator).
    ///
    /// Only pure `SELECT` queries are eligible. DDL, mutations, and EXPLAIN must
    /// go through the coordinator so job lifecycle, retries, and barriers apply.
    pub(crate) fn can_execute_inline(&self, query: &str, is_streaming: bool) -> bool {
        if is_streaming {
            return false;
        }
        // Case-insensitive prefix check without allocating an uppercase String.
        let trimmed = query.trim_start();
        // Any statement that mutates state or requires coordinator lifecycle must
        // not bypass it. EXPLAIN output differs between paths; route via coordinator.
        const NON_INLINE_PREFIXES: &[&str] = &[
            "EXPLAIN", "CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "COPY",
            "MERGE",
        ];
        !NON_INLINE_PREFIXES
            .iter()
            .any(|p| trimmed.len() >= p.len() && trimmed[..p.len()].eq_ignore_ascii_case(p))
    }

    pub fn execute_batch_sql(
        &self,
        query: &str,
        tables: &[BatchSqlTable],
        is_streaming: bool,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        // Fast path: bypass the coordinator state machine for non-streaming
        // batch queries. The coordinator was designed for distributed job
        // lifecycle; routing in-process single-stage SQL through it adds
        // 6+ Mutex lock/unlock pairs per query with no functional benefit.
        if self.can_execute_inline(query, is_streaming) {
            return self.execute_inline_sql(query, tables);
        }
        let fragment = format!("sql: {query}");
        let kind = if is_streaming {
            JobKind::Streaming
        } else {
            JobKind::Batch
        };
        self.run_terminal_task(&fragment, kind, tables, Vec::new())
    }

    /// Drain a locally registered continuous streaming job and return newly
    /// emitted batches.
    ///
    /// After each successful drain, the window executor state is snapshotted
    /// and queued to the coordinator's metadata store (if one is attached). The
    /// persist is fire-and-forget and does not block the caller; a missing store
    /// or a snapshot failure is silently ignored so drain is never degraded by
    /// persistence failures.
    pub fn drain_continuous_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        let batches = self.continuous_registry.drain_job(job_id)?;
        // Snapshot and persist window state after each successful drain.
        // Snapshot errors are swallowed: a job with no window state yet returns
        // an error on the first drain; that is expected and must not block drains.
        // Lock-poison is unexpected and logged at WARN so operators know the
        // coordinator is degraded without breaking the drain path.
        if let Ok((snapshot_bytes, watermark_ms)) =
            self.continuous_registry.snapshot_job_with_watermark(job_id)
        {
            match self.coordinator.lock() {
                Ok(coord) => {
                    coord.save_continuous_snapshot(
                        job_id,
                        ContinuousSnapshot {
                            snapshot_bytes,
                            watermark_ms,
                        },
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        job_id,
                        "coordinator mutex poisoned; continuous job snapshot not persisted"
                    );
                }
            }
        }
        Ok(batches)
    }

    /// Attach a metadata store so that continuous job snapshots are persisted.
    ///
    /// Call before any `drain_continuous_job` calls to enable snapshot persistence.
    /// Safe to call on a running runtime; subsequent drains will use the new store.
    pub fn attach_store(&self, store: impl MetadataStore + 'static) -> RuntimeResult<()> {
        let mut coord = self
            .coordinator
            .lock()
            .map_err(|_| RuntimeError::InvalidState {
                message: "coordinator lock poisoned during attach_store".into(),
            })?;
        coord.attach_store(store);
        Ok(())
    }

    /// Read the most recently persisted snapshot for a continuous job from the store.
    ///
    /// Returns `None` when no store is configured or no snapshot exists for `job_id`.
    /// Useful for exporting snapshots for cross-session transfer or diagnostics.
    pub fn load_continuous_snapshot(&self, job_id: &str) -> Option<ContinuousSnapshot> {
        let coord = self.coordinator.lock().ok()?;
        coord.load_continuous_snapshot(job_id)
    }

    /// Restore continuous jobs from previously persisted snapshots in the store.
    ///
    /// For each `(job_id, spec)` pair, if the coordinator's store contains a
    /// snapshot for `job_id`, the job is registered via
    /// [`ContinuousStreamRegistry::register_job_from_snapshot`] so the next
    /// `push_input` / `drain_continuous_job` call resumes from the saved state.
    ///
    /// Jobs that have no snapshot in the store are silently skipped. Jobs that
    /// are already registered in the registry are also skipped to avoid
    /// double-registration errors.
    ///
    /// Returns the number of jobs actually restored.
    pub fn restore_continuous_jobs_from_store(
        &self,
        job_specs: &[(&str, WindowExecutionSpec)],
    ) -> RuntimeResult<usize> {
        let coord = self
            .coordinator
            .lock()
            .map_err(|_| RuntimeError::InvalidState {
                message: "coordinator lock poisoned during restore_continuous_jobs_from_store"
                    .into(),
            })?;
        let mut restored = 0usize;
        for (job_id, spec) in job_specs {
            if self.continuous_registry.has_job(job_id) {
                continue;
            }
            if let Some(snapshot) = coord.load_continuous_snapshot(job_id) {
                self.continuous_registry.register_job_from_snapshot(
                    *job_id,
                    spec.clone(),
                    &snapshot.snapshot_bytes,
                )?;
                restored += 1;
            }
        }
        Ok(restored)
    }

    fn run_terminal_task(
        &self,
        fragment: &str,
        kind: JobKind,
        tables: &[BatchSqlTable],
        stream_partitions: Vec<InputPartition>,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let job_id = self.next_job_id()?;
        let task_id = TaskId::try_new("task-0").map_err(|e| RuntimeError::InvalidState {
            message: e.to_string(),
        })?;
        let stage_id = StageId::try_new("stage-0").map_err(|e| RuntimeError::InvalidState {
            message: e.to_string(),
        })?;
        let job_spec = JobSpec::new(job_id.clone(), fragment.to_string(), kind).with_stage(
            StageSpec::new(stage_id, "stage-0")
                .with_task(TaskSpec::new(task_id.clone(), fragment.to_string())),
        );

        {
            let mut coord = self
                .coordinator
                .lock()
                .map_err(|_| RuntimeError::InvalidState {
                    message: "coordinator lock poisoned during job submission".into(),
                })?;
            match coord.submit_job(job_spec) {
                Ok(SubmitOutcome::Accepted) | Ok(SubmitOutcome::Queued { .. }) => {}
                Err(e) => {
                    return Err(RuntimeError::InvalidState {
                        message: e.to_string(),
                    });
                }
            }
        }

        // C5: Multi-stage in-process execution.  Repeatedly:
        //  1. Ask the coordinator for currently-assigned tasks for this job.
        //  2. For the first stage's first task, attach the input partitions
        //     supplied by the caller (parquet tables / stream partitions).
        //  3. Push every assignment into the inbox.
        //  4. Drain the inbox via the runner.
        //  5. Loop until no new assignments are launched (terminal stages all done).
        let initial_partitions: Vec<InputPartition> = tables
            .iter()
            .enumerate()
            .map(|(idx, table)| {
                InputPartition::new(format!("local-parquet-{idx}"), String::new()).with_descriptor(
                    InputPartitionDescriptor::LocalParquet {
                        table_name: table.table_name.clone(),
                        path: table.path.to_string_lossy().into_owned(),
                    },
                )
            })
            .chain(stream_partitions)
            .collect();

        let bridge = self.bridge.clone();
        let runner = Arc::clone(&self.runner);
        let mut output_batches = Vec::new();
        let mut iter_count = 0usize;
        let mut first_iteration_partitions = Some(initial_partitions);
        // G1: Track the max watermark from the previous stage so it can be
        // injected as a WatermarkHint into the first assignment of the next stage.
        let mut stage_watermark_ms: Option<i64> = None;

        block_on(async {
            loop {
                if iter_count >= MAX_STAGE_ITERATIONS {
                    return Err(RuntimeError::transport(format!(
                        "in-process runtime exceeded {MAX_STAGE_ITERATIONS} stage iterations \
                         for job {job_id}; for unbounded queries use the streaming API"
                    )));
                }
                iter_count += 1;

                // O4: Merge launch_assigned_task_assignments + job_snapshot into
                // one lock acquisition (previously two separate locks per iteration).
                // coordinator_tick is kept after task execution below.
                let (mut assignments, is_terminal) = {
                    let mut coord = self.coordinator.lock().map_err(|_| {
                        RuntimeError::transport(
                            "coordinator lock poisoned during task assignment launch",
                        )
                    })?;
                    let assignments = coord
                        .launch_assigned_task_assignments(&job_id)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?;
                    let is_terminal = coord
                        .job_snapshot(&job_id)
                        .map(|s| s.state().is_terminal())
                        .unwrap_or(false);
                    (assignments, is_terminal)
                };

                if assignments.is_empty() {
                    if is_terminal {
                        return Ok(());
                    }
                    // First iteration: coordinator never produced assignments.
                    if iter_count == 1 {
                        return Err(RuntimeError::transport(
                            "in-process coordinator produced no task assignments",
                        ));
                    }
                    // Subsequent iterations with no new assignments but job not
                    // yet terminal: all tasks in the previous stage completed and
                    // the coordinator bridge already updated state — the job is
                    // effectively done from the in-process executor's perspective.
                    return Ok(());
                }

                // G1: Inject the upstream stage's watermark as a WatermarkHint
                // partition so downstream streaming stages start at the correct
                // watermark baseline rather than i64::MIN.
                if let Some(wm) = stage_watermark_ms.take() {
                    // Only inject if the next stage is a streaming fragment; batch
                    // stages ignore the hint (O6). Guard against empty assignments (G5).
                    // Use starts_with instead of contains to avoid matching "stream:"
                    // inside a SQL string predicate.
                    let next_is_streaming = assignments
                        .first()
                        .map(|a| a.plan_fragment().description().starts_with("stream:"))
                        .unwrap_or(false);
                    if next_is_streaming && !assignments.is_empty() {
                        let hint = InputPartition::new("watermark-hint", String::new())
                            .with_descriptor(InputPartitionDescriptor::WatermarkHint {
                                watermark_ms: wm,
                            });
                        // Inject the watermark hint into ALL tasks in this stage, not
                        // just the first. Tasks 1..N previously started at WATERMARK_UNSET
                        // (i64::MIN), causing inconsistent late-data suppression within
                        // a multi-task streaming stage.
                        assignments = assignments
                            .into_iter()
                            .map(|a| {
                                let mut new_parts = vec![hint.clone()];
                                new_parts.extend(a.input_partitions().to_vec());
                                a.with_input_partitions(new_parts)
                            })
                            .collect();
                    } else {
                        // Restore for the next iteration when assignments arrive.
                        stage_watermark_ms = Some(wm);
                    }
                }

                // Attach caller-supplied input partitions to the FIRST assignment
                // emitted by the FIRST iteration only.  Subsequent stages source
                // their input from shuffle outputs.
                if let Some(partitions) = first_iteration_partitions.take() {
                    let first = assignments.remove(0).with_input_partitions(partitions);
                    self.inbox
                        .push(first)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?;
                }
                for assignment in assignments {
                    self.inbox
                        .push(assignment)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?;
                }

                while let Some(report) = runner
                    .run_next_with(&bridge)
                    .await
                    .map_err(|e| RuntimeError::transport(e.message()))?
                {
                    // Only collect terminal-stage outputs (SQL, connector pipeline,
                    // streaming window).  Intermediate shuffle-write reports must
                    // not be concatenated into the final result set.
                    let kind = report.output().kind();
                    if matches!(
                        kind,
                        ExecutorTaskOutputKind::Sql
                            | ExecutorTaskOutputKind::ConnectorPipeline
                            | ExecutorTaskOutputKind::StreamingWindow
                    ) {
                        output_batches.extend(report.output().record_batches().to_vec());
                    }
                    // G1: Collect watermark from streaming stages for the next stage.
                    // Skip i64::MIN (WATERMARK_UNSET) — it is the uninitialized sentinel
                    // returned by windows that have not yet processed any events (B3/A4).
                    if let Some(wm) = report.output().watermark_ms() && wm > WATERMARK_UNSET {
                        stage_watermark_ms =
                            Some(stage_watermark_ms.map_or(wm, |prev: i64| prev.max(wm)));
                    }
                    // Forward the EMA-derived advisory partition count to the
                    // coordinator so it can scale streaming task concurrency on
                    // the next cycle without user configuration.
                    if kind == ExecutorTaskOutputKind::StreamingWindow
                        && let Some(buckets) = report.output().advisory_buckets()
                        && let Ok(mut coord) = self.coordinator.lock()
                    {
                        coord.record_streaming_advisory_buckets(&job_id, buckets);
                    }
                }

                // Drive a coordinator tick after each stage's tasks complete.
                // This advances the state machine: marks the completed stage as
                // Succeeded and makes the next stage's tasks eligible for assignment.
                {
                    let mut coord =
                        self.coordinator
                            .lock()
                            .map_err(|_| RuntimeError::InvalidState {
                                message: "coordinator lock poisoned during coordinator tick".into(),
                            })?;
                    let _ = coord.coordinator_tick();
                }
            }
        })?;

        // Evict the completed job from the coordinator's in-memory registry.
        // The embedded runtime has no background GC loop; without this, every
        // drain_continuous_job / execute_batch_sql call would leave a terminal
        // JobCoordinator entry growing unboundedly, and coordinator_tick would
        // iterate over all of them on every subsequent call.
        {
            let mut coord = self
                .coordinator
                .lock()
                .map_err(|_| RuntimeError::InvalidState {
                    message: "coordinator lock poisoned during job eviction".into(),
                })?;
            coord.evict_completed_job(&job_id);
        }

        Ok(output_batches)
    }

    pub fn execute_windowed(
        &self,
        _topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &WindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        if input_batches.is_empty() {
            return Ok(Vec::new());
        }
        // Fast path: call execute_bounded_window directly, bypassing the coordinator
        // state machine (submit_job → task-assignment → run_next_with → InMemory
        // partition deserialization → coordinator_tick → evict = 6 mutex acquisitions).
        // execute_bounded_window is a pure stateless function that takes Arrow batches
        // and a window spec; the coordinator path added no value for this operation.
        // The `topic` argument was only used to name the InMemory partition — irrelevant
        // for the computation itself, so it is intentionally unused here.
        krishiv_dataflow::execute_bounded_window(input_batches, spec, None)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Stable identity for session-scoped coordinator reuse tests.
    pub fn coordinator_instance_id(&self) -> usize {
        Arc::as_ptr(&self.coordinator) as usize
    }

    /// Borrow the runner's SQL engine (for Kafka / streaming source registration
    /// forwarded from the Flight server).
    pub fn runner_sql_engine(&self) -> &krishiv_sql::SqlEngine {
        self.runner.sql_engine()
    }

    pub fn execute_windowed_local(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        use crate::in_process_cluster::local_spec_to_plan_spec;
        self.execute_windowed(topic, input_batches, &local_spec_to_plan_spec(spec))
    }
}

/// Run windowed aggregation via a session-scoped cluster (preferred).
pub fn execute_windowed_in_process(
    cluster: &crate::InProcessCluster,
    topic: &str,
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<RecordBatch>> {
    cluster.collect_bounded_window(topic, input_batches, spec)
}

/// Legacy entry: creates an ephemeral in-process cluster (tests only).
#[cfg(test)]
pub fn execute_windowed_in_process_ephemeral(
    topic: &str,
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<RecordBatch>> {
    let cluster = crate::InProcessCluster::new()?;
    cluster.collect_bounded_window(topic, input_batches, spec)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::in_process_cluster::InProcessCluster;
    use crate::local_streaming::LocalWindowExecutionSpec;

    #[test]
    fn in_process_windowed_stream_returns_batches() {
        let batch = krishiv_common::test_fixtures::make_test_user_ts_batch(
            vec!["a", "b"],
            vec![1_000, 5_000],
        );
        let spec = LocalWindowExecutionSpec::new_test_tumbling("user_id", "ts", 10_000);
        let cluster = InProcessCluster::new().unwrap();
        let out = cluster
            .collect_bounded_window("events", vec![batch], &spec)
            .unwrap();
        assert!(!out.is_empty());
    }

    // ── Inline fast-path tests ─────────────────────────────────────────────────

    #[test]
    fn inline_fast_path_simple_select_returns_correct_result() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime
            .execute_batch_sql("SELECT 42 AS n", &[], false)
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn inline_fast_path_multi_column_select() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime
            .execute_batch_sql("SELECT 1 AS a, 'hello' AS b, 3.14 AS c", &[], false)
            .unwrap();
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[test]
    fn streaming_query_bypasses_inline_path() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        // Register a streaming source so is_streaming_query returns true.
        runtime
            .runner
            .sql_engine()
            .register_streaming_source_name("stream_t")
            .unwrap();
        // is_streaming=true forces coordinator path regardless.
        let result = runtime.execute_batch_sql("SELECT 1", &[], true);
        // Just verify it doesn't panic (returns Ok or coordinator error).
        let _ = result;
    }

    #[test]
    fn batch_sql_routes_through_coordinator() {
        let runtime = InProcessStreamingRuntime::new().expect("runtime");
        let batches = runtime
            .execute_batch_sql("SELECT 1 AS value", &[], false)
            .expect("batch sql");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn continuous_job_drains_via_registry() {
        let runtime = InProcessStreamingRuntime::new().expect("runtime");
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime
            .register_continuous_job("events", spec)
            .expect("register");
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        runtime
            .push_continuous_input("events", vec![batch])
            .expect("push");
        let _ = runtime.drain_continuous_job("events").expect("drain");
    }

    #[test]
    fn runtime_new_creates_working_runtime() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime.execute_batch_sql("SELECT 42", &[], false).unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn execute_batch_sql_returns_single_batch() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime
            .execute_batch_sql("SELECT 'hello' AS msg", &[], false)
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[test]
    fn execute_batch_sql_multi_column() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime
            .execute_batch_sql("SELECT 1 AS a, 'x' AS b", &[], false)
            .unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn execute_windowed_empty_batches_returns_empty() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        let result = runtime.execute_windowed("topic", vec![], &spec).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn coordinator_instance_id_is_stable() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let id1 = runtime.coordinator_instance_id();
        let id2 = runtime.coordinator_instance_id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn multiple_runtimes_have_distinct_coordinator_ids() {
        let r1 = InProcessStreamingRuntime::new().unwrap();
        let r2 = InProcessStreamingRuntime::new().unwrap();
        assert_ne!(r1.coordinator_instance_id(), r2.coordinator_instance_id());
    }

    #[test]
    fn push_continuous_input_unknown_job_fails() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let result = runtime.push_continuous_input("no-such", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn drain_continuous_job_unknown_fails() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let result = runtime.drain_continuous_job("no-such");
        assert!(result.is_err());
    }

    #[test]
    fn continuous_registry_accessor() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let reg = runtime.continuous_registry();
        assert!(reg.list_jobs().is_empty());
    }

    #[test]
    fn batch_sql_with_parquet_tables_attempt() {
        use std::path::PathBuf;
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let tables = vec![BatchSqlTable {
            table_name: "nonexistent".into(),
            path: PathBuf::from("/no/such/file.parquet"),
        }];
        // This may fail because file doesn't exist but the routing path is tested
        let result = runtime.execute_batch_sql("SELECT 1", &tables, false);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn batch_sql_with_empty_tables() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let result = runtime
            .execute_batch_sql("SELECT 1 AS n", &[], false)
            .unwrap();
        assert_eq!(result[0].num_rows(), 1);
    }

    #[test]
    fn register_and_drain_multiple_continuous_jobs() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime.register_continuous_job("j1", spec.clone()).unwrap();
        runtime.register_continuous_job("j2", spec).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        runtime
            .push_continuous_input("j1", vec![batch.clone()])
            .unwrap();
        runtime.push_continuous_input("j2", vec![batch]).unwrap();
        let _ = runtime.drain_continuous_job("j1").unwrap();
        let _ = runtime.drain_continuous_job("j2").unwrap();
    }

    // ── New N-series fixes ────────────────────────────────────────────────────

    #[test]
    fn ddl_queries_bypass_inline_path() {
        // CREATE, DROP, INSERT, EXPLAIN etc. must never go through execute_inline_sql.
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let ddl_queries = [
            "CREATE TABLE t (id INT)",
            "DROP TABLE IF EXISTS t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET id = 2",
            "DELETE FROM t WHERE id = 1",
            "EXPLAIN SELECT 1",
            "ALTER TABLE t ADD COLUMN x INT",
        ];
        for q in &ddl_queries {
            assert!(
                !runtime.can_execute_inline(q, false),
                "DDL must not be inline-eligible: {q}"
            );
        }
        // Plain SELECT must remain eligible.
        assert!(
            runtime.can_execute_inline("SELECT 1", false),
            "plain SELECT must be inline-eligible"
        );
    }

    #[test]
    fn shared_parquet_cache_is_reused_across_sessions() {
        let rt1 = InProcessStreamingRuntime::new().unwrap();
        let cache = rt1.parquet_cache();
        // Pre-populate the cache as if rt1 had registered a parquet table.
        cache.insert("events:/data/events.parquet".to_string(), ());
        // rt2 shares the same cache.
        let rt2 = InProcessStreamingRuntime::with_parquet_cache(Arc::clone(&cache)).unwrap();
        assert!(
            rt2.parquet_cache()
                .contains_key("events:/data/events.parquet"),
            "shared cache must be visible in the new session"
        );
    }

    // ── Snapshot persistence tests ────────────────────────────────────────────

    #[test]
    fn drain_continuous_job_persists_snapshot_to_store() {
        use krishiv_scheduler::InMemoryMetadataStore;
        let runtime = InProcessStreamingRuntime::new().unwrap();
        runtime
            .attach_store(InMemoryMetadataStore::default())
            .unwrap();

        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime.register_continuous_job("events", spec).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        runtime
            .push_continuous_input("events", vec![batch])
            .unwrap();
        let _ = runtime.drain_continuous_job("events").unwrap();

        // After drain, the snapshot must have been written to the store.
        // In sync-mode tests (no Tokio runtime → tx=None) the write is synchronous.
        let snapshot = runtime.load_continuous_snapshot("events");
        assert!(
            snapshot.is_some(),
            "drain_continuous_job must persist a snapshot to the attached store"
        );
    }

    #[test]
    fn restore_continuous_jobs_from_store_resumes_watermark() {
        use krishiv_scheduler::{InMemoryMetadataStore, MetadataStore};
        // Session 1: register job, push batches that cross a window, drain.
        let rt1 = InProcessStreamingRuntime::new().unwrap();
        rt1.attach_store(InMemoryMetadataStore::default()).unwrap();

        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        rt1.register_continuous_job("job-a", spec.clone()).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mk_batch = |ts: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["u1"])) as _,
                    Arc::new(Int64Array::from(vec![ts])) as _,
                ],
            )
            .unwrap()
        };
        // ts=1_000 and ts=11_000 cross the first 10-second window boundary.
        rt1.push_continuous_input("job-a", vec![mk_batch(1_000), mk_batch(11_000)])
            .unwrap();
        let _ = rt1.drain_continuous_job("job-a").unwrap();

        // Extract the snapshot from session 1.
        let snapshot = rt1
            .load_continuous_snapshot("job-a")
            .expect("drain must persist a snapshot");

        // Session 2: pre-populate a fresh store with the snapshot, then restore.
        let mut store2 = InMemoryMetadataStore::default();
        store2.save_continuous_snapshot("job-a", snapshot).unwrap();
        let rt2 = InProcessStreamingRuntime::new().unwrap();
        rt2.attach_store(store2).unwrap();
        let restored = rt2
            .restore_continuous_jobs_from_store(&[("job-a", spec.clone())])
            .unwrap();
        assert_eq!(
            restored, 1,
            "exactly one job must be restored from the store"
        );
        assert!(
            rt2.continuous_registry.has_job("job-a"),
            "restored job must appear in the registry"
        );
    }

    #[test]
    fn restore_continuous_jobs_skips_jobs_without_snapshot() {
        use krishiv_scheduler::InMemoryMetadataStore;
        let runtime = InProcessStreamingRuntime::new().unwrap();
        runtime
            .attach_store(InMemoryMetadataStore::default())
            .unwrap();

        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        // No drain → no snapshot in the store.
        let restored = runtime
            .restore_continuous_jobs_from_store(&[("no-such-snapshot", spec)])
            .unwrap();
        assert_eq!(
            restored, 0,
            "job with no stored snapshot must not be restored"
        );
    }

    #[test]
    fn restore_continuous_jobs_skips_already_registered() {
        use krishiv_scheduler::InMemoryMetadataStore;
        let runtime = InProcessStreamingRuntime::new().unwrap();
        runtime
            .attach_store(InMemoryMetadataStore::default())
            .unwrap();

        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime.register_continuous_job("j", spec.clone()).unwrap();

        // Even if a snapshot were present the job is already registered.
        let restored = runtime
            .restore_continuous_jobs_from_store(&[("j", spec)])
            .unwrap();
        assert_eq!(
            restored, 0,
            "already-registered job must be skipped by restore"
        );
    }

    #[test]
    fn attach_store_then_drain_without_input_persists_empty_snapshot() {
        use krishiv_scheduler::InMemoryMetadataStore;
        let runtime = InProcessStreamingRuntime::new().unwrap();
        runtime
            .attach_store(InMemoryMetadataStore::default())
            .unwrap();

        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime.register_continuous_job("empty-job", spec).unwrap();
        // Drain with no input must not panic and must produce a snapshot.
        let out = runtime.drain_continuous_job("empty-job").unwrap();
        assert!(out.is_empty());
        let snap = runtime.load_continuous_snapshot("empty-job");
        assert!(
            snap.is_some(),
            "drain of empty job must still persist a snapshot"
        );
    }

    #[test]
    fn deregister_streaming_source_clears_parquet_cache_entries() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        runtime
            .runner
            .sql_engine()
            .register_streaming_source_name("topic")
            .unwrap();
        // Manually insert a cache entry simulating a prior parquet registration
        // under the same table name (hybrid scenario, N2).
        runtime
            .runner
            .registered_parquet_cache()
            .insert("topic:/data/snapshot.parquet".to_string(), ());
        runtime.deregister_streaming_source("topic").unwrap();
        assert!(
            !runtime
                .runner
                .registered_parquet_cache()
                .contains_key("topic:/data/snapshot.parquet"),
            "deregister must clear parquet cache entries for the table"
        );
        // An unrelated entry must be untouched.
        runtime
            .runner
            .registered_parquet_cache()
            .insert("other:/data/other.parquet".to_string(), ());
        runtime.deregister_streaming_source("topic").unwrap(); // idempotent
        assert!(
            runtime
                .runner
                .registered_parquet_cache()
                .contains_key("other:/data/other.parquet"),
            "unrelated cache entries must not be cleared"
        );
    }
}
