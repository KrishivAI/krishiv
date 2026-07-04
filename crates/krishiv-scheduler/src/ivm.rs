#![forbid(unsafe_code)]

//! IVM job registry for the coordinator.
//!
//! Each IVM job is a long-lived flow held in-process. A job is either a single
//! [`IncrementalFlow`] or, when its first view is a key-shardable aggregate, an
//! auto-partitioned [`PartitionedIncrementalFlow`] — decided transparently at
//! view-registration time (see [`IvmJobRegistry::register_view`]).
//!
//! The coordinator's flow is the **single source of truth for every mode**
//! (embedded, single-node, distributed), which keeps executors replaceable.
//! For distributed mode with live executors, single-flow ticks are offloaded to
//! an executor: the coordinator drains pending locally, ships a full state
//! snapshot (`checkpoint_full`), and applies the returned view outputs via
//! `apply_computed_tick`; on any failure it re-feeds pending and computes
//! centrally. Partitioned jobs always compute centrally (shards already run in
//! parallel in-process).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_ivm::{
    DeltaBatch, IncrementalFlow, IncrementalViewSpec, IvmError, IvmResult,
    PartitionedIncrementalFlow, StepSummary, partition_key_from_sql,
};

/// A coordinator-hosted IVM job: a single flow, or one auto-partitioned by key.
///
/// Both variants hold `Arc`s, so cloning is cheap and the handle can be passed
/// to async HTTP handlers. The enum exposes the full flow surface the IVM HTTP
/// API needs; `match self` dispatches to the right backing flow.
#[derive(Clone)]
pub enum IvmJob {
    /// Unpartitioned — the default, and the only shape for non-shardable views.
    Single(Arc<IncrementalFlow>),
    /// Key-partitioned across shards (single-column `GROUP BY` aggregates).
    Partitioned(Arc<PartitionedIncrementalFlow>),
}

impl IvmJob {
    /// True when this job is auto-partitioned.
    pub fn is_partitioned(&self) -> bool {
        matches!(self, IvmJob::Partitioned(_))
    }

    /// Register a view on the job. (Partitioning is decided by the registry
    /// *before* the job reaches this variant; here we just register.)
    pub fn register_view(&self, spec: IncrementalViewSpec) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.register_view(spec),
            IvmJob::Partitioned(p) => p.register_view(spec),
        }
    }

    /// Drop a view. Returns `true` if it existed.
    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        match self {
            IvmJob::Single(f) => f.drop_view(name),
            IvmJob::Partitioned(p) => p.drop_view(name),
        }
    }

    /// Feed a `DeltaBatch` for a source (routed to its shard when partitioned).
    pub fn feed(&self, source: &str, delta: DeltaBatch) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.feed(source, delta),
            IvmJob::Partitioned(p) => p.feed(source, delta),
        }
    }

    /// Feed a full streaming snapshot, differentiated against the previous one.
    pub fn feed_snapshot(&self, source: &str, batches: &[RecordBatch]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.feed_snapshot(source, batches),
            IvmJob::Partitioned(p) => p.feed_snapshot(source, batches),
        }
    }

    /// Advance one tick (shards step in parallel when partitioned).
    pub async fn step_datafusion(&self) -> IvmResult<StepSummary> {
        match self {
            IvmJob::Single(f) => f.step_datafusion().await,
            IvmJob::Partitioned(p) => p.step_datafusion().await,
        }
    }

    /// Current tick count.
    pub fn tick(&self) -> IvmResult<u64> {
        match self {
            IvmJob::Single(f) => f.tick(),
            IvmJob::Partitioned(p) => p.tick(),
        }
    }

    /// Read a source/view snapshot from the per-source map (the `/snap` surface).
    pub fn source_snapshot(&self, name: &str) -> IvmResult<Option<RecordBatch>> {
        match self {
            IvmJob::Single(f) => f.source_snapshot(name),
            IvmJob::Partitioned(p) => p.source_snapshot(name),
        }
    }

    /// Read a view's materialized snapshot (concatenated across shards).
    pub fn snapshot(&self, view: &str) -> IvmResult<Option<RecordBatch>> {
        match self {
            IvmJob::Single(f) => f.snapshot(view),
            IvmJob::Partitioned(p) => p.snapshot(view),
        }
    }

    /// Return the spec for a named view (`None` if not registered).
    pub fn view_spec(&self, view: &str) -> IvmResult<Option<IncrementalViewSpec>> {
        match self {
            IvmJob::Single(f) => f.view_spec(view),
            IvmJob::Partitioned(p) => p.view_spec(view),
        }
    }

    /// Enable delta-checkpoint accumulation (every shard when partitioned).
    pub fn enable_delta_checkpoints(&self) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.enable_delta_checkpoints(),
            IvmJob::Partitioned(p) => p.enable_delta_checkpoints(),
        }
    }

    /// Enable content-addressed input dedup (every shard when partitioned).
    pub fn enable_input_dedup(&self) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.enable_input_dedup(),
            IvmJob::Partitioned(p) => p.enable_input_dedup(),
        }
    }

    /// Peek a view's latest output delta (merged across shards when partitioned).
    pub fn view_output_peek(&self, view: &str) -> IvmResult<Option<DeltaBatch>> {
        match self {
            IvmJob::Single(f) => f.view_output_peek(view),
            IvmJob::Partitioned(p) => p.view_output_peek(view),
        }
    }

    /// Spawn a vector-view background task (one per shard when partitioned, all
    /// writing the shared sink). Returns the join handles.
    pub fn spawn_vector_views(
        &self,
        spec: krishiv_ivm::VectorViewSpec,
    ) -> IvmResult<Vec<tokio::task::JoinHandle<()>>> {
        match self {
            IvmJob::Single(f) => Ok(vec![krishiv_ivm::spawn_vector_view(f, spec)?]),
            IvmJob::Partitioned(p) => p.spawn_vector_views(spec),
        }
    }

    /// Full checkpoint (per-shard framed when partitioned).
    pub fn checkpoint(&self) -> IvmResult<Vec<u8>> {
        match self {
            IvmJob::Single(f) => f.checkpoint(),
            IvmJob::Partitioned(p) => p.checkpoint(),
        }
    }

    /// Restore a full checkpoint.
    pub fn restore(&self, bytes: &[u8]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.restore(bytes),
            IvmJob::Partitioned(p) => p.restore(bytes),
        }
    }

    /// Full checkpoint: sources **and view state** (snapshot + baseline), so a
    /// restore converges maintained views after restart (G6). Prefer this over
    /// [`checkpoint`](Self::checkpoint), which captures sources only.
    pub fn checkpoint_full(&self) -> IvmResult<Vec<u8>> {
        match self {
            IvmJob::Single(f) => f.checkpoint_full(),
            IvmJob::Partitioned(p) => p.checkpoint_full(),
        }
    }

    /// Restore a full checkpoint (see [`checkpoint_full`](Self::checkpoint_full)).
    pub fn restore_full(&self, bytes: &[u8]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.restore_full(bytes),
            IvmJob::Partitioned(p) => p.restore_full(bytes),
        }
    }

    /// Delta checkpoint (per-shard framed when partitioned).
    pub fn checkpoint_delta(&self) -> IvmResult<Vec<u8>> {
        match self {
            IvmJob::Single(f) => f.checkpoint_delta(),
            IvmJob::Partitioned(p) => p.checkpoint_delta(),
        }
    }

    /// Restore a delta checkpoint.
    pub fn restore_delta(&self, bytes: &[u8]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.restore_delta(bytes),
            IvmJob::Partitioned(p) => p.restore_delta(bytes),
        }
    }
}

/// Hard cap on auto-derived IVM shard fan-out (keeps tiny jobs from spawning a
/// flow per core on large machines).
const MAX_AUTO_IVM_SHARDS: usize = 8;

/// Pure shard-count policy: honour a valid `KRISHIV_IVM_SHARDS` override
/// (N≥1; `1` disables partitioning), else derive from `parallelism` capped at
/// [`MAX_AUTO_IVM_SHARDS`]. Split out from environment/CPU lookup for testing.
fn resolve_ivm_shards(env_override: Option<&str>, parallelism: usize) -> usize {
    if let Some(raw) = env_override
        && let Ok(n) = raw.trim().parse::<usize>()
        && n >= 1
    {
        return n;
    }
    parallelism.clamp(1, MAX_AUTO_IVM_SHARDS)
}

/// Default partition fan-out for a shardable IVM job.
///
/// Escape hatch: `KRISHIV_IVM_SHARDS=N` pins the fan-out (N≥1; `1` disables
/// partitioning entirely, e.g. for debugging). Absent or invalid, it derives
/// from available parallelism, capped at [`MAX_AUTO_IVM_SHARDS`] — one shard per
/// core removes the single-core ceiling on keyed incremental views.
fn default_ivm_shards() -> usize {
    let env = std::env::var("KRISHIV_IVM_SHARDS").ok();
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    resolve_ivm_shards(env.as_deref(), parallelism)
}

/// Registry of IVM jobs hosted on this coordinator process.
#[derive(Debug)]
pub struct IvmJobRegistry {
    jobs: Mutex<HashMap<String, IvmJob>>,
    /// Shard count used when a job's first view is auto-partitioned.
    default_shards: usize,
    /// Per-job async step locks. Serialize concurrent `step` calls so two
    /// simultaneous ticks cannot drain each other's pending or double-advance
    /// the tick counter. Each job gets its own lock (created lazily, removed on
    /// `delete`) so unrelated jobs never contend.
    step_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl std::fmt::Debug for IvmJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IvmJob::Single(_) => f.write_str("IvmJob::Single"),
            IvmJob::Partitioned(p) => write!(f, "IvmJob::Partitioned({} shards)", p.num_shards()),
        }
    }
}

impl Default for IvmJobRegistry {
    fn default() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            default_shards: default_ivm_shards(),
            step_locks: Mutex::new(HashMap::new()),
        }
    }
}

impl IvmJobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a registry with an explicit auto-partition fan-out (for tests).
    pub fn with_default_shards(default_shards: usize) -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            default_shards: default_shards.max(1),
            step_locks: Mutex::new(HashMap::new()),
        }
    }

    /// Return the per-job async step lock (creating it if absent).
    ///
    /// The lock serializes concurrent `step`/dispatch calls for one job. It is
    /// intentionally per-job so independent jobs step in parallel.
    pub fn step_lock(&self, job_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self
            .step_locks
            .lock()
            .ok()
            .and_then(|m| m.get(job_id).cloned())
        {
            return lock;
        }
        let mut locks = match self.step_locks.lock() {
            Ok(l) => l,
            Err(p) => p.into_inner(),
        };
        locks
            .entry(job_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Create a new IVM job. Idempotent: returns `Ok` if the job already exists.
    pub fn create(&self, job_id: String) -> Result<(), IvmError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| IvmError::execution("registry lock poisoned"))?;
        jobs.entry(job_id)
            .or_insert_with(|| IvmJob::Single(Arc::new(IncrementalFlow::new())));
        Ok(())
    }

    /// Register (or update) a view on a job, auto-partitioning when eligible.
    ///
    /// The partition decision is made here, on the **first** view of a job: if
    /// the job is still a fresh single flow (no views yet) and the view is a
    /// single-column `GROUP BY` aggregate, the job is upgraded in place to a
    /// [`PartitionedIncrementalFlow`] keyed on that column, sized by
    /// [`default_ivm_shards`]. All subsequent views register on the chosen
    /// flow. Non-shardable first views leave the job single.
    pub fn register_view(&self, job_id: &str, spec: IncrementalViewSpec) -> Result<(), IvmError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| IvmError::execution("registry lock poisoned"))?;
        let job = jobs
            .get(job_id)
            .ok_or_else(|| IvmError::execution(format!("IVM job not found: {job_id}")))?
            .clone();

        // Only a fresh, unpartitioned, view-less job is a candidate for upgrade.
        if let IvmJob::Single(flow) = &job
            && flow.view_names().map(|v| v.is_empty()).unwrap_or(false)
            && self.default_shards > 1
            && let Some(key) = partition_key_from_sql(&spec.body_sql)
        {
            let part = PartitionedIncrementalFlow::new(self.default_shards, key);
            part.register_view(spec)?;
            jobs.insert(job_id.to_string(), IvmJob::Partitioned(Arc::new(part)));
            return Ok(());
        }

        job.register_view(spec)
    }

    /// Look up a job. Returns `None` if not found.
    pub fn get(&self, job_id: &str) -> Option<IvmJob> {
        self.jobs.lock().ok()?.get(job_id).cloned()
    }

    /// Delete a job. Returns `true` if the job existed.
    pub fn delete(&self, job_id: &str) -> bool {
        let removed = self
            .jobs
            .lock()
            .map(|mut j| j.remove(job_id).is_some())
            .unwrap_or(false);
        // Drop the per-job step lock so a recreated same-id job gets a fresh one.
        let _ = self.step_locks.lock().map(|mut l| l.remove(job_id));
        removed
    }

    /// List all job IDs.
    pub fn job_ids(&self) -> Vec<String> {
        self.jobs
            .lock()
            .map(|j| j.keys().cloned().collect())
            .unwrap_or_default()
    }
}

/// Shared, reference-counted handle to the IVM job registry.
pub type SharedIvmJobRegistry = Arc<IvmJobRegistry>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

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

    fn revenue_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "revenue".into(),
            body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    fn passthrough_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "passthrough".into(),
            body_sql: "SELECT region, amount FROM orders".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    /// A GROUP BY view auto-partitions the job; a pass-through view does not.
    #[test]
    fn register_view_auto_partitions_group_by() {
        let reg = IvmJobRegistry::with_default_shards(3);

        reg.create("agg".into()).unwrap();
        reg.register_view("agg", revenue_spec()).unwrap();
        assert!(reg.get("agg").unwrap().is_partitioned());

        reg.create("flat".into()).unwrap();
        reg.register_view("flat", passthrough_spec()).unwrap();
        assert!(!reg.get("flat").unwrap().is_partitioned());
    }

    /// With a single configured shard, even a GROUP BY view stays single.
    #[test]
    fn single_shard_registry_never_partitions() {
        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create("agg".into()).unwrap();
        reg.register_view("agg", revenue_spec()).unwrap();
        assert!(!reg.get("agg").unwrap().is_partitioned());
    }

    /// End-to-end through the coordinator `IvmJob` surface: an auto-partitioned
    /// job feeds, steps, and snapshots to the same grand total as a single flow.
    #[tokio::test]
    async fn partitioned_job_matches_single_job_end_to_end() {
        let data = orders(
            &["US", "EU", "US", "APAC", "EU", "US"],
            &[100, 50, 25, 10, 75, 5],
        );
        let grand = |b: &RecordBatch| -> f64 {
            b.column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or(0.0))
                .sum()
        };

        // Partitioned (3 shards).
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        assert!(job.is_partitioned());
        job.feed("orders", DeltaBatch::from_inserts(data.clone()).unwrap())
            .unwrap();
        job.step_datafusion().await.unwrap();
        let part = job.snapshot_revenue().await;

        // Single (1 shard).
        let reg1 = IvmJobRegistry::with_default_shards(1);
        reg1.create("j".into()).unwrap();
        reg1.register_view("j", revenue_spec()).unwrap();
        let job1 = reg1.get("j").unwrap();
        assert!(!job1.is_partitioned());
        job1.feed("orders", DeltaBatch::from_inserts(data).unwrap())
            .unwrap();
        job1.step_datafusion().await.unwrap();
        let single = job1.snapshot_revenue().await;

        assert_eq!(grand(&part), 265.0);
        assert_eq!(grand(&part), grand(&single));
    }

    impl IvmJob {
        /// Test helper: read the `revenue` view's materialized snapshot.
        async fn snapshot_revenue(&self) -> RecordBatch {
            match self {
                IvmJob::Single(f) => f.snapshot("revenue").unwrap().unwrap(),
                IvmJob::Partitioned(p) => p.snapshot("revenue").unwrap().unwrap(),
            }
        }
    }

    /// Checkpoint/restore round-trips a partitioned job through the registry.
    #[tokio::test]
    async fn partitioned_job_checkpoint_restore() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        job.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US", "APAC"], &[100, 50, 25, 10]))
                .unwrap(),
        )
        .unwrap();
        job.step_datafusion().await.unwrap();
        let before = job.source_snapshot("orders").unwrap().unwrap();
        let bytes = job.checkpoint().unwrap();

        // New registry/job of the same shape restores the source state.
        let reg2 = IvmJobRegistry::with_default_shards(3);
        reg2.create("j".into()).unwrap();
        reg2.register_view("j", revenue_spec()).unwrap();
        let job2 = reg2.get("j").unwrap();
        job2.restore(&bytes).unwrap();
        let after = job2.source_snapshot("orders").unwrap().unwrap();

        assert_eq!(before.num_rows(), after.num_rows());
    }

    // ── shard-count policy (escape hatch) ─────────────────────────────────────

    #[test]
    fn resolve_ivm_shards_honours_env_and_caps() {
        // Valid override wins, including 1 (= disable partitioning).
        assert_eq!(resolve_ivm_shards(Some("4"), 16), 4);
        assert_eq!(resolve_ivm_shards(Some("1"), 16), 1);
        assert_eq!(resolve_ivm_shards(Some(" 6 "), 2), 6); // trimmed
        // Invalid / zero / empty override → fall back to capped parallelism.
        assert_eq!(resolve_ivm_shards(Some("0"), 4), 4);
        assert_eq!(resolve_ivm_shards(Some("abc"), 4), 4);
        assert_eq!(resolve_ivm_shards(Some(""), 4), 4);
        assert_eq!(resolve_ivm_shards(None, 4), 4);
        // Parallelism is clamped to [1, MAX_AUTO_IVM_SHARDS].
        assert_eq!(resolve_ivm_shards(None, 0), 1);
        assert_eq!(resolve_ivm_shards(None, 100), MAX_AUTO_IVM_SHARDS);
    }

    // ── registry lifecycle edge cases ─────────────────────────────────────────

    #[test]
    fn register_view_on_missing_job_errors() {
        let reg = IvmJobRegistry::with_default_shards(3);
        assert!(reg.register_view("ghost", revenue_spec()).is_err());
    }

    #[test]
    fn create_is_idempotent_and_preserves_partitioning() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
        // A second create must not clobber the existing (partitioned) job.
        reg.create("j".into()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
    }

    #[test]
    fn delete_and_list_jobs() {
        let reg = IvmJobRegistry::with_default_shards(2);
        reg.create("a".into()).unwrap();
        reg.create("b".into()).unwrap();
        let mut ids = reg.job_ids();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
        assert!(reg.delete("a"));
        assert!(!reg.delete("a")); // already gone
        assert_eq!(reg.job_ids(), vec!["b".to_string()]);
    }

    #[test]
    fn only_first_view_drives_partition_decision() {
        let reg = IvmJobRegistry::with_default_shards(3);
        // First view is non-shardable → job stays single...
        reg.create("j".into()).unwrap();
        reg.register_view("j", passthrough_spec()).unwrap();
        assert!(!reg.get("j").unwrap().is_partitioned());
        // ...and a later GROUP BY view does NOT retroactively partition it.
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(!reg.get("j").unwrap().is_partitioned());
    }

    #[test]
    fn second_view_registers_on_already_partitioned_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
        // A second GROUP BY view on the partitioned job registers without error.
        let spec2 = IncrementalViewSpec {
            name: "revenue2".into(),
            body_sql: "SELECT region, COUNT(*) AS n FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("n", DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        reg.register_view("j", spec2).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
    }

    #[test]
    fn enable_flags_propagate_through_ivm_job() {
        // Both variants accept the enable_* config without error.
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("p".into()).unwrap();
        reg.register_view("p", revenue_spec()).unwrap();
        let part = reg.get("p").unwrap();
        assert!(part.is_partitioned());
        part.enable_delta_checkpoints().unwrap();
        part.enable_input_dedup().unwrap();

        let reg1 = IvmJobRegistry::with_default_shards(1);
        reg1.create("s".into()).unwrap();
        reg1.register_view("s", revenue_spec()).unwrap();
        let single = reg1.get("s").unwrap();
        single.enable_delta_checkpoints().unwrap();
        single.enable_input_dedup().unwrap();
    }

    /// Stream-bridge (`feed_snapshot`) works through the partitioned registry job.
    #[tokio::test]
    async fn feed_snapshot_through_partitioned_registry_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        assert!(job.is_partitioned());
        job.feed_snapshot("orders", &[orders(&["US", "EU", "US"], &[10, 20, 30])])
            .unwrap();
        job.step_datafusion().await.unwrap();
        assert_eq!(job.snapshot_revenue().await.num_rows(), 2);
    }

    /// `view_output_peek` works through a partitioned registry job (merged delta).
    #[tokio::test]
    async fn view_output_peek_through_partitioned_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        assert!(job.is_partitioned());
        assert!(job.view_output_peek("revenue").unwrap().is_none()); // before any step
        job.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[1, 2, 3])).unwrap(),
        )
        .unwrap();
        job.step_datafusion().await.unwrap();
        let peek = job.view_output_peek("revenue").unwrap().unwrap();
        assert_eq!(peek.num_rows(), 2); // US, EU merged across shards
    }

    /// Vector-view fan-out: one task per shard (partitioned) vs. one (single).
    /// Regression: Single-job registry must expose a non-null snapshot after step
    /// when `is_materialized = true`.
    #[tokio::test]
    async fn single_job_snapshot_non_null_after_step() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_ivm::DeltaBatch;

        let sales_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]));
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Float64,
            true,
        )]));
        let spec = IncrementalViewSpec {
            name: "total_sales".into(),
            body_sql: "SELECT SUM(amount) AS total FROM sales".into(),
            output_schema: view_schema,
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };

        // Use default_shards=1 so no auto-partition (stays Single).
        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create("job-a".into()).unwrap();
        reg.register_view("job-a", spec).unwrap();

        let sales_batch = RecordBatch::try_new(
            sales_schema,
            vec![Arc::new(Float64Array::from(vec![100.0_f64, 200.0, 50.0]))],
        )
        .unwrap();
        let job = reg.get("job-a").unwrap();
        job.feed("sales", DeltaBatch::from_inserts(sales_batch).unwrap())
            .unwrap();
        let summary = job.step_datafusion().await.unwrap();
        assert_eq!(summary.active_views, 1, "expected 1 active view");
        assert_eq!(summary.total_output_rows, 1, "expected 1 output row");

        let snap = job
            .snapshot("total_sales")
            .expect("snapshot() failed")
            .expect("snapshot is None for materialized view after step");
        assert_eq!(snap.num_rows(), 1);
        let total = snap
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((total - 350.0).abs() < 1e-9, "expected 350.0, got {total}");
    }

    #[tokio::test]
    async fn spawn_vector_views_fans_out_per_shard() {
        use krishiv_ivm::{InMemoryVectorSink, VectorViewSpec};

        let make_spec = || VectorViewSpec {
            view_name: "revenue".into(),
            id_column: "region".into(),
            vector_column: "v".into(),
            sink: InMemoryVectorSink::new(),
        };

        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("p".into()).unwrap();
        reg.register_view("p", revenue_spec()).unwrap();
        let handles = reg
            .get("p")
            .unwrap()
            .spawn_vector_views(make_spec())
            .unwrap();
        assert_eq!(handles.len(), 3);
        for h in handles {
            h.abort();
        }

        let reg1 = IvmJobRegistry::with_default_shards(1);
        reg1.create("s".into()).unwrap();
        reg1.register_view("s", revenue_spec()).unwrap();
        let handles = reg1
            .get("s")
            .unwrap()
            .spawn_vector_views(make_spec())
            .unwrap();
        assert_eq!(handles.len(), 1);
        for h in handles {
            h.abort();
        }
    }

    // ── per-job step lock ─────────────────────────────────────────────────────

    /// The step lock is per-job: same job → same lock, different jobs → different
    /// locks. Deleting a job drops its lock so a recreated same-id job gets a
    /// fresh one.
    #[test]
    fn step_lock_is_per_job_and_lifecycle_aware() {
        let reg = IvmJobRegistry::with_default_shards(1);
        let a1 = reg.step_lock("job-a");
        let a2 = reg.step_lock("job-a");
        let b = reg.step_lock("job-b");
        // Same job → same lock Arc.
        assert!(
            Arc::ptr_eq(&a1, &a2),
            "repeated step_lock must return the same Arc"
        );
        // Different job → different lock.
        assert!(
            !Arc::ptr_eq(&a1, &b),
            "different jobs must have different locks"
        );

        // Delete + recreate → fresh lock (old one not resurrected).
        reg.delete("job-a");
        let a3 = reg.step_lock("job-a");
        assert!(
            !Arc::ptr_eq(&a1, &a3),
            "deleted job must get a fresh lock on recreate"
        );
    }

    /// The step lock actually serializes: a held lock blocks a second acquirer
    /// until the first is released.
    #[tokio::test]
    async fn step_lock_serializes_concurrent_acquirers() {
        let reg = IvmJobRegistry::with_default_shards(1);
        let lock = reg.step_lock("job-s");

        let g1 = lock.lock().await;
        // While g1 is held, a second acquire should not complete immediately.
        let try_second =
            tokio::time::timeout(std::time::Duration::from_millis(50), lock.lock()).await;
        assert!(
            try_second.is_err(),
            "second acquire must block while first is held"
        );

        drop(g1);
        // Now the second acquire succeeds.
        let _g2 = lock.lock().await;
    }
}
