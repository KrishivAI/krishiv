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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_ivm::{DeltaBatch, IncrementalFlow, IncrementalViewSpec, StepSummary};
use krishiv_scheduler::SharedIvmJobRegistry;

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
/// Wraps an [`IncrementalFlow`] from a shared [`SharedIvmJobRegistry`].
#[derive(Clone)]
pub struct EmbeddedIvmJob {
    flow: Arc<IncrementalFlow>,
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
        let flow = registry.get(&job_id).ok_or_else(|| {
            RuntimeError::plan_rejected(format!("ivm job '{job_id}' not found after create"))
        })?;
        Ok(Self { flow, job_id })
    }

    /// The job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Direct access to the underlying [`IncrementalFlow`] (advanced use).
    pub fn flow(&self) -> &IncrementalFlow {
        &self.flow
    }

    /// Register or update a view on the local flow.
    pub fn register_view(&self, spec: IncrementalViewSpec) -> RuntimeResult<()> {
        self.flow
            .register_view(spec)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Feed a [`DeltaBatch`] to a local source (the one feed primitive).
    pub fn feed(&self, source_name: impl Into<String>, delta: DeltaBatch) -> RuntimeResult<()> {
        self.flow
            .feed(source_name, delta)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Feed a full streaming snapshot; differentiated against the previous one.
    pub fn feed_snapshot(
        &self,
        source_name: impl Into<String>,
        batches: &[RecordBatch],
    ) -> RuntimeResult<()> {
        self.flow
            .feed_snapshot(source_name, batches)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Read the current materialized snapshot of a view (`None` if not yet produced).
    pub fn snapshot(&self, view_name: &str) -> RuntimeResult<Option<RecordBatch>> {
        self.flow
            .snapshot(view_name)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Run one local IVM tick asynchronously (DataFusion-backed).
    pub async fn step(&self) -> RuntimeResult<StepSummary> {
        self.flow
            .step_datafusion()
            .await
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Current tick count.
    pub fn tick(&self) -> RuntimeResult<u64> {
        self.flow
            .tick()
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Enable delta checkpoint accumulation.
    pub fn enable_delta_checkpoints(&self) -> RuntimeResult<()> {
        self.flow
            .enable_delta_checkpoints()
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Enable content-addressed input dedup.
    pub fn enable_input_dedup(&self) -> RuntimeResult<()> {
        self.flow
            .enable_input_dedup()
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Serialize a full checkpoint of source snapshots.
    pub fn checkpoint(&self) -> RuntimeResult<Vec<u8>> {
        self.flow
            .checkpoint()
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Restore source snapshots from checkpoint bytes.
    pub fn restore(&self, bytes: &[u8]) -> RuntimeResult<()> {
        self.flow
            .restore(bytes)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Retrieve a delta checkpoint (deltas accumulated since last call).
    pub fn checkpoint_delta(&self) -> RuntimeResult<Vec<u8>> {
        self.flow
            .checkpoint_delta()
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Apply delta checkpoint bytes on top of an existing restored state.
    pub fn restore_delta(&self, bytes: &[u8]) -> RuntimeResult<()> {
        self.flow
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
