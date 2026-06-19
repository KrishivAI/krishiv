//! Ergonomic streaming job handle for the remote (coordinator HTTP) mode.
//!
//! # Example
//!
//! ```rust,ignore
//! let job = RemoteStreamingJob::create(&coordinator_url, &spec, "etl-job").await?;
//! job.push(&input_batches).await?;
//! let output = job.drain().await?;
//! ```

use arrow::record_batch::RecordBatch;
use krishiv_plan::window::WindowExecutionSpec;

use crate::{
    RuntimeResult, execute_coordinator_continuous_drain, execute_coordinator_continuous_push,
    execute_coordinator_continuous_register,
};

/// Handle to a continuous streaming job registered on the coordinator.
///
/// Obtained via [`RemoteStreamingJob::create`]. All methods are async.
#[derive(Debug, Clone)]
pub struct RemoteStreamingJob {
    coordinator_http: String,
    job_id: String,
}

impl RemoteStreamingJob {
    /// Register a new continuous streaming job on the coordinator.
    pub async fn create(
        coordinator_http: &str,
        spec: &WindowExecutionSpec,
        job_id: &str,
    ) -> RuntimeResult<Self> {
        execute_coordinator_continuous_register(coordinator_http, job_id, spec).await?;
        Ok(Self {
            coordinator_http: coordinator_http.to_owned(),
            job_id: job_id.to_owned(),
        })
    }

    /// Wrap an existing job ID without re-registering it.
    pub fn from_job_id(coordinator_http: impl Into<String>, job_id: impl Into<String>) -> Self {
        Self {
            coordinator_http: coordinator_http.into(),
            job_id: job_id.into(),
        }
    }

    /// The job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Push input batches to the streaming job.
    pub async fn push(&self, batches: &[RecordBatch]) -> RuntimeResult<()> {
        execute_coordinator_continuous_push(&self.coordinator_http, &self.job_id, batches).await
    }

    /// Drain accumulated output batches from the job.
    pub async fn drain(&self) -> RuntimeResult<Vec<RecordBatch>> {
        execute_coordinator_continuous_drain(&self.coordinator_http, &self.job_id).await
    }
}
