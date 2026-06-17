//! Unified session entry point — same API across embedded and distributed modes.
//!
//! # Quick start
//!
//! **Embedded (in-process):**
//! ```rust,ignore
//! let session = KrishivSession::embedded();
//! let job = session.ivm_job("revenue").await?;
//! job.feed_source("orders", &delta).await?;
//! let (active, tick) = job.step().await?;
//! ```
//!
//! **Distributed (coordinator):**
//! ```rust,ignore
//! let session = KrishivSession::distributed("http://coordinator:8080");
//! let job = session.ivm_job("revenue").await?;  // same call
//! ```
//!
//! **Batch SQL:**
//! ```rust,ignore
//! let rows = session.batch_sql("SELECT 1 + 1 AS n", &[]).await?;
//! ```

use arrow::record_batch::RecordBatch;
use krishiv_scheduler::SharedIvmJobRegistry;

use crate::ivm_job::{EmbeddedIvmJob, IvmJobHandle, RemoteIvmJob};
use crate::streaming_job::RemoteStreamingJob;
use crate::{RuntimeResult, execute_coordinator_batch_sql};

// ── session mode ──────────────────────────────────────────────────────────────

/// Mode-agnostic compute session.
///
/// Constructed via [`KrishivSession::embedded`] or
/// [`KrishivSession::distributed`] and used as the single entry point for
/// batch, IVM, and streaming operations.
#[derive(Debug, Clone)]
pub struct KrishivSession {
    mode: SessionMode,
}

#[derive(Debug, Clone)]
enum SessionMode {
    Embedded { registry: SharedIvmJobRegistry },
    Distributed { coordinator_http: String },
}

impl KrishivSession {
    /// Create a session that executes locally (in-process).
    pub fn embedded() -> Self {
        Self {
            mode: SessionMode::Embedded {
                registry: std::sync::Arc::new(krishiv_scheduler::IvmJobRegistry::new()),
            },
        }
    }

    /// Create a session backed by a remote coordinator.
    ///
    /// `coordinator_http` is the base URL of the coordinator HTTP API,
    /// e.g. `"http://localhost:8080"`.
    pub fn distributed(coordinator_http: impl Into<String>) -> Self {
        Self {
            mode: SessionMode::Distributed {
                coordinator_http: coordinator_http.into(),
            },
        }
    }

    /// Whether this session is in embedded (in-process) mode.
    pub fn is_embedded(&self) -> bool {
        matches!(self.mode, SessionMode::Embedded { .. })
    }

    /// Whether this session is in distributed (remote coordinator) mode.
    pub fn is_distributed(&self) -> bool {
        matches!(self.mode, SessionMode::Distributed { .. })
    }

    // ── IVM ──────────────────────────────────────────────────────────────────

    /// Create or retrieve an IVM job with the given name.
    ///
    /// Returns a unified [`IvmJobHandle`] that works identically in both
    /// embedded and distributed modes.
    pub async fn ivm_job(&self, job_name: &str) -> RuntimeResult<IvmJobHandle> {
        match &self.mode {
            SessionMode::Embedded { registry } => {
                let job = EmbeddedIvmJob::create(registry, job_name)?;
                Ok(IvmJobHandle::Embedded(job))
            }
            SessionMode::Distributed { coordinator_http } => {
                let job = RemoteIvmJob::create(coordinator_http, Some(job_name)).await?;
                Ok(IvmJobHandle::Remote(job))
            }
        }
    }

    // ── Batch SQL ────────────────────────────────────────────────────────────

    /// Execute a batch SQL query.
    ///
    /// In distributed mode the query is submitted to the coordinator.
    /// In embedded mode this returns an error (use `krishiv_api::Session::sql` directly).
    pub async fn batch_sql(
        &self,
        query: &str,
        tables: &[crate::in_process::BatchSqlTable],
    ) -> RuntimeResult<Vec<RecordBatch>> {
        match &self.mode {
            SessionMode::Embedded { .. } => Err(crate::RuntimeError::unsupported(
                "batch_sql on KrishivSession::embedded — use krishiv_api::Session::sql instead",
            )),
            SessionMode::Distributed { coordinator_http } => {
                execute_coordinator_batch_sql(coordinator_http, query, tables, false).await
            }
        }
    }

    // ── Continuous Streaming ──────────────────────────────────────────────────

    /// Get a handle to an existing remote streaming job (distributed mode only).
    ///
    /// Use `RemoteStreamingJob::create` to register a new job first.
    pub fn streaming_job(&self, job_id: &str) -> RuntimeResult<RemoteStreamingJob> {
        match &self.mode {
            SessionMode::Embedded { .. } => Err(crate::RuntimeError::unsupported(
                "streaming_job on KrishivSession::embedded — use InProcessStreamingRuntime instead",
            )),
            SessionMode::Distributed { coordinator_http } => Ok(RemoteStreamingJob::from_job_id(
                coordinator_http.clone(),
                job_id,
            )),
        }
    }
}
