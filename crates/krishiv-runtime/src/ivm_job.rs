//! Ergonomic IVM job handles for remote and embedded modes.
//!
//! # Remote (distributed coordinator)
//!
//! ```rust,ignore
//! let job = RemoteIvmJob::create(&coordinator_url, "revenue").await?;
//! job.register_view(&spec).await?;
//! job.feed_source("orders", &delta).await?;
//! let summary = job.step().await?;
//! ```
//!
//! # Embedded (in-process)
//!
//! ```rust,ignore
//! let registry = SharedIvmJobRegistry::default();
//! let job = EmbeddedIvmJob::create(registry, "revenue")?;
//! job.flow().register_view(spec)?;
//! job.flow().feed_source("orders", delta)?;
//! job.flow().step_datafusion().await?;
//! ```

use std::sync::Arc;

use krishiv_ivm::{DeltaBatch, IncrementalFlow, IncrementalViewSpec, StepSummary};
use krishiv_scheduler::SharedIvmJobRegistry;

use crate::{
    RemoteStepSummary, RuntimeError, RuntimeResult, execute_coordinator_ivm_checkpoint,
    execute_coordinator_ivm_create_job, execute_coordinator_ivm_feed_source,
    execute_coordinator_ivm_register_view, execute_coordinator_ivm_restore,
    execute_coordinator_ivm_step,
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

    /// Push a [`DeltaBatch`] as input delta for a named source.
    ///
    /// The delta is buffered until the next [`step`](Self::step) call.
    pub async fn feed_source(&self, source_name: &str, delta: &DeltaBatch) -> RuntimeResult<()> {
        execute_coordinator_ivm_feed_source(
            &self.coordinator_http,
            &self.job_id,
            source_name,
            delta,
        )
        .await
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

    /// Direct access to the underlying [`IncrementalFlow`].
    pub fn flow(&self) -> &IncrementalFlow {
        &self.flow
    }

    /// Register or update a view on the local flow.
    pub fn register_view(&self, spec: IncrementalViewSpec) -> RuntimeResult<()> {
        self.flow
            .register_view(spec)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Feed a delta batch to a local source.
    pub fn feed_source(
        &self,
        source_name: impl Into<String>,
        delta: DeltaBatch,
    ) -> RuntimeResult<()> {
        self.flow
            .feed_source(source_name, delta)
            .map_err(|e| RuntimeError::plan_rejected(e.to_string()))
    }

    /// Run one local IVM tick asynchronously (DataFusion-backed).
    pub async fn step(&self) -> RuntimeResult<StepSummary> {
        self.flow
            .step_datafusion()
            .await
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

// ── IvmJobHandle — unified enum ───────────────────────────────────────────────

/// Mode-agnostic IVM job handle.
///
/// Returned by [`KrishivSession::ivm_job`] so callers write the same code
/// regardless of whether the session is embedded or distributed.
#[derive(Debug, Clone)]
pub enum IvmJobHandle {
    /// In-process execution (embedded / single-node).
    Embedded(EmbeddedIvmJob),
    /// Remote execution via coordinator HTTP.
    Remote(RemoteIvmJob),
}

impl IvmJobHandle {
    /// The job ID.
    pub fn job_id(&self) -> &str {
        match self {
            Self::Embedded(j) => j.job_id(),
            Self::Remote(j) => j.job_id(),
        }
    }

    /// Feed a delta batch. In embedded mode the call is synchronous (wrapped).
    pub async fn feed_source(&self, source_name: &str, delta: &DeltaBatch) -> RuntimeResult<()> {
        match self {
            Self::Embedded(j) => j.feed_source(source_name, delta.clone()),
            Self::Remote(j) => j.feed_source(source_name, delta).await,
        }
    }

    /// Advance one tick. Returns `(active_views, tick)`.
    pub async fn step(&self) -> RuntimeResult<(usize, u64)> {
        match self {
            Self::Embedded(j) => {
                let summary = j.step().await?;
                Ok((summary.active_views, 0))
            }
            Self::Remote(j) => {
                let s = j.step().await?;
                Ok((s.active_views, s.tick))
            }
        }
    }
}
