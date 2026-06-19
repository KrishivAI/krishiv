//! IVM job backends for remote and embedded modes.
//!
//! These are the two leaf implementations that the unified `krishiv_api::IvmJob`
//! handle dispatches to. Both expose the same surface — the single `feed`
//! primitive plus lifecycle/checkpoint methods — so the api-level enum can
//! delegate without reaching into private state.
//!
//! # Remote (distributed coordinator)
//!
//! ```rust,ignore
//! let job = RemoteIvmJob::create(&coordinator_url, Some("revenue")).await?;
//! job.register_view(&spec).await?;
//! job.feed("orders", &DeltaBatch::from_inserts(batch)?).await?;
//! let summary = job.step().await?;
//! ```
//!
//! # Embedded (in-process)
//!
//! ```rust,ignore
//! let registry = SharedIvmJobRegistry::default();
//! let job = EmbeddedIvmJob::create(&registry, "revenue")?;
//! job.register_view(spec)?;
//! job.feed("orders", DeltaBatch::from_inserts(batch)?)?;
//! job.step().await?;
//! ```

use arrow::record_batch::RecordBatch;
use krishiv_ivm::{DeltaBatch, IncrementalViewSpec, StepSummary};
use krishiv_scheduler::{IvmJob, SharedIvmJobRegistry};

use crate::{
    RemoteStepSummary, RuntimeError, RuntimeResult, execute_coordinator_ivm_checkpoint,
    execute_coordinator_ivm_checkpoint_delta, execute_coordinator_ivm_create_job,
    execute_coordinator_ivm_feed_source, execute_coordinator_ivm_register_view,
    execute_coordinator_ivm_restore, execute_coordinator_ivm_restore_delta,
    execute_coordinator_ivm_snapshot, execute_coordinator_ivm_step,
    execute_coordinator_ivm_stream_bridge,
};

// ── RemoteIvmJob ──────────────────────────────────────────────────────────────

/// Handle to an IVM job managed by a remote coordinator.
///
/// Obtained via [`RemoteIvmJob::create`]. All methods are async and issue
/// HTTP requests to the coordinator.
#[derive(Debug, Clone)]
pub struct RemoteIvmJob {
    coordinator_http: String,
    job_id: String,
}

impl RemoteIvmJob {
    /// Create a new IVM job on the coordinator and return a handle.
    ///
    /// `job_name` is used as the job ID when supplied; the coordinator assigns
    /// one automatically if `None`.
    pub async fn create(coordinator_http: &str, job_name: Option<&str>) -> RuntimeResult<Self> {
        let job_id = execute_coordinator_ivm_create_job(coordinator_http, job_name).await?;
        Ok(Self {
            coordinator_http: coordinator_http.to_owned(),
            job_id,
        })
    }

    /// Wrap an existing job ID without creating a new one.
    pub fn from_job_id(coordinator_http: impl Into<String>, job_id: impl Into<String>) -> Self {
        Self {
            coordinator_http: coordinator_http.into(),
            job_id: job_id.into(),
        }
    }

    /// The assigned job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Register or update an incremental view on this job.
    pub async fn register_view(&self, spec: &IncrementalViewSpec) -> RuntimeResult<()> {
        execute_coordinator_ivm_register_view(&self.coordinator_http, &self.job_id, spec).await
    }

    /// Feed a [`DeltaBatch`] as input for a named source (the one feed primitive).
    ///
    /// Build the delta with `DeltaBatch::from_inserts` / `from_deletes` /
    /// `from_cdc` before calling. The delta is buffered until the next
    /// [`step`](Self::step) call.
    pub async fn feed(&self, source_name: &str, delta: &DeltaBatch) -> RuntimeResult<()> {
        execute_coordinator_ivm_feed_source(
            &self.coordinator_http,
            &self.job_id,
            source_name,
            delta,
        )
        .await
    }

    /// Feed a full streaming snapshot; the coordinator differentiates against
    /// the previous snapshot for this source.
    pub async fn feed_snapshot(
        &self,
        source_name: &str,
        batches: &[RecordBatch],
    ) -> RuntimeResult<()> {
        execute_coordinator_ivm_stream_bridge(
            &self.coordinator_http,
            &self.job_id,
            source_name,
            batches,
        )
        .await
    }

    /// Read the current materialized snapshot of a view (`None` if not yet produced).
    pub async fn snapshot(&self, view_name: &str) -> RuntimeResult<Option<RecordBatch>> {
        execute_coordinator_ivm_snapshot(&self.coordinator_http, &self.job_id, view_name).await
    }

    /// Advance one clock tick on the coordinator.
    pub async fn step(&self) -> RuntimeResult<RemoteStepSummary> {
        execute_coordinator_ivm_step(&self.coordinator_http, &self.job_id).await
    }

    /// Retrieve a serialized checkpoint from the coordinator.
    pub async fn checkpoint(&self) -> RuntimeResult<Vec<u8>> {
        execute_coordinator_ivm_checkpoint(&self.coordinator_http, &self.job_id).await
    }

    /// Restore this job from previously captured checkpoint bytes.
    pub async fn restore(&self, bytes: &[u8]) -> RuntimeResult<()> {
        execute_coordinator_ivm_restore(&self.coordinator_http, &self.job_id, bytes).await
    }

    /// Retrieve a delta checkpoint (incremental deltas since last call).
    pub async fn checkpoint_delta(&self) -> RuntimeResult<Vec<u8>> {
        execute_coordinator_ivm_checkpoint_delta(&self.coordinator_http, &self.job_id).await
    }

    /// Apply delta checkpoint bytes on top of restored state.
    pub async fn restore_delta(&self, bytes: &[u8]) -> RuntimeResult<()> {
        execute_coordinator_ivm_restore_delta(&self.coordinator_http, &self.job_id, bytes).await
    }
}

// ── EmbeddedIvmJob ────────────────────────────────────────────────────────────

/// Handle to an IVM job running in-process (embedded / single-node mode).
///
/// Holds a [`SharedIvmJobRegistry`] + job id rather than a raw flow, so view
/// registration goes through [`IvmJobRegistry::register_view`] and the job
/// **auto-partitions** when its first view is a key-shardable aggregate — the
/// same path the distributed coordinator uses. Each operation fetches the
/// current [`IvmJob`] (a cheap `Arc` clone) and dispatches to it.
#[derive(Clone)]
pub struct EmbeddedIvmJob {
    registry: SharedIvmJobRegistry,
    job_id: String,
}

impl EmbeddedIvmJob {
    /// Create (or get existing) an in-process IVM job in `registry`.
    pub fn create(
        registry: &SharedIvmJobRegistry,
        job_id: impl Into<String>,
    ) -> RuntimeResult<Self> {
        let job_id = job_id.into();
        registry
            .create(job_id.clone())
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))?;
        if registry.get(&job_id).is_none() {
            return Err(RuntimeError::plan_rejected(format!(
                "ivm job '{job_id}' not found after create"
            )));
        }
        Ok(Self {
            registry: registry.clone(),
            job_id,
        })
    }

    /// The job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// `true` if this job auto-partitioned (its first view was shardable).
    pub fn is_partitioned(&self) -> RuntimeResult<bool> {
        Ok(self.job()?.is_partitioned())
    }

    /// Fetch the current backing job from the registry.
    fn job(&self) -> RuntimeResult<IvmJob> {
        self.registry.get(&self.job_id).ok_or_else(|| {
            RuntimeError::transport(format!("ivm job '{}' no longer exists", self.job_id))
        })
    }

    /// Register or update a view — auto-partitioning the job when eligible.
    pub fn register_view(&self, spec: IncrementalViewSpec) -> RuntimeResult<()> {
        self.registry
            .register_view(&self.job_id, spec)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Feed a [`DeltaBatch`] to a local source (the one feed primitive).
    pub fn feed(&self, source_name: impl Into<String>, delta: DeltaBatch) -> RuntimeResult<()> {
        let source = source_name.into();
        self.job()?
            .feed(&source, delta)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Feed a full streaming snapshot; differentiated against the previous one.
    pub fn feed_snapshot(
        &self,
        source_name: impl Into<String>,
        batches: &[RecordBatch],
    ) -> RuntimeResult<()> {
        let source = source_name.into();
        self.job()?
            .feed_snapshot(&source, batches)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Read the current materialized snapshot of a view (`None` if not yet produced).
    pub fn snapshot(&self, view_name: &str) -> RuntimeResult<Option<RecordBatch>> {
        self.job()?
            .snapshot(view_name)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Run one local IVM tick asynchronously (DataFusion-backed).
    pub async fn step(&self) -> RuntimeResult<StepSummary> {
        self.job()?
            .step_datafusion()
            .await
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Current tick count.
    pub fn tick(&self) -> RuntimeResult<u64> {
        self.job()?
            .tick()
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Enable delta checkpoint accumulation.
    pub fn enable_delta_checkpoints(&self) -> RuntimeResult<()> {
        self.job()?
            .enable_delta_checkpoints()
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Enable content-addressed input dedup.
    pub fn enable_input_dedup(&self) -> RuntimeResult<()> {
        self.job()?
            .enable_input_dedup()
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Serialize a full checkpoint of source snapshots.
    pub fn checkpoint(&self) -> RuntimeResult<Vec<u8>> {
        self.job()?
            .checkpoint()
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Restore source snapshots from checkpoint bytes.
    pub fn restore(&self, bytes: &[u8]) -> RuntimeResult<()> {
        self.job()?
            .restore(bytes)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Retrieve a delta checkpoint (deltas accumulated since last call).
    pub fn checkpoint_delta(&self) -> RuntimeResult<Vec<u8>> {
        self.job()?
            .checkpoint_delta()
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Apply delta checkpoint bytes on top of an existing restored state.
    pub fn restore_delta(&self, bytes: &[u8]) -> RuntimeResult<()> {
        self.job()?
            .restore_delta(bytes)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }
}

impl std::fmt::Debug for EmbeddedIvmJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedIvmJob")
            .field("job_id", &self.job_id)
            .finish()
    }
}

#[cfg(test)]
mod embedded_tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_scheduler::IvmJobRegistry;

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
            name: "flat".into(),
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

    /// Gap #1 fix: an embedded job whose first view is a GROUP BY aggregate
    /// auto-partitions — the embedded path now matches the distributed one.
    #[test]
    fn embedded_group_by_view_auto_partitions() {
        let reg = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job = EmbeddedIvmJob::create(&reg, "agg").unwrap();
        job.register_view(revenue_spec()).unwrap();
        assert!(job.is_partitioned().unwrap());
    }

    #[test]
    fn embedded_passthrough_view_stays_single() {
        let reg = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job = EmbeddedIvmJob::create(&reg, "flat").unwrap();
        job.register_view(passthrough_spec()).unwrap();
        assert!(!job.is_partitioned().unwrap());
    }

    #[test]
    fn embedded_single_shard_registry_never_partitions() {
        let reg = Arc::new(IvmJobRegistry::with_default_shards(1));
        let job = EmbeddedIvmJob::create(&reg, "agg").unwrap();
        job.register_view(revenue_spec()).unwrap();
        assert!(!job.is_partitioned().unwrap());
    }

    /// An auto-partitioned embedded job produces the same result as a single one.
    #[tokio::test]
    async fn embedded_partitioned_feed_step_snapshot_matches_single() {
        let data = orders(&["US", "EU", "US", "APAC", "EU"], &[100, 50, 25, 10, 75]);
        let grand = |b: &RecordBatch| -> f64 {
            b.column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or(0.0))
                .sum()
        };

        let reg = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job = EmbeddedIvmJob::create(&reg, "p").unwrap();
        job.register_view(revenue_spec()).unwrap();
        assert!(job.is_partitioned().unwrap());
        job.feed("orders", DeltaBatch::from_inserts(data.clone()).unwrap())
            .unwrap();
        job.step().await.unwrap();
        let part = job.snapshot("revenue").unwrap().unwrap();

        let reg1 = Arc::new(IvmJobRegistry::with_default_shards(1));
        let job1 = EmbeddedIvmJob::create(&reg1, "s").unwrap();
        job1.register_view(revenue_spec()).unwrap();
        job1.feed("orders", DeltaBatch::from_inserts(data).unwrap())
            .unwrap();
        job1.step().await.unwrap();
        let single = job1.snapshot("revenue").unwrap().unwrap();

        assert_eq!(part.num_rows(), single.num_rows());
        assert_eq!(grand(&part), grand(&single));
        assert_eq!(grand(&part), 260.0);
    }

    #[tokio::test]
    async fn embedded_partitioned_checkpoint_restore() {
        let reg = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job = EmbeddedIvmJob::create(&reg, "j").unwrap();
        job.register_view(revenue_spec()).unwrap();
        job.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[1, 2, 3])).unwrap(),
        )
        .unwrap();
        job.step().await.unwrap();
        let bytes = job.checkpoint().unwrap();

        let reg2 = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job2 = EmbeddedIvmJob::create(&reg2, "j").unwrap();
        job2.register_view(revenue_spec()).unwrap();
        job2.restore(&bytes).unwrap();
        // Restored source state is intact.
        assert!(job2.tick().is_ok());
    }

    /// Operations on a job deleted out from under the handle error, not panic.
    #[test]
    fn embedded_job_deleted_from_registry_errors() {
        let reg = Arc::new(IvmJobRegistry::with_default_shards(2));
        let job = EmbeddedIvmJob::create(&reg, "doomed").unwrap();
        job.register_view(revenue_spec()).unwrap();
        assert!(reg.delete("doomed"));
        assert!(job.tick().is_err());
        assert!(
            job.feed(
                "orders",
                DeltaBatch::from_inserts(orders(&["US"], &[1])).unwrap()
            )
            .is_err()
        );
    }
}
