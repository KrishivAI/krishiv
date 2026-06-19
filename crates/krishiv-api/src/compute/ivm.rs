//! Mode-agnostic IVM job handle.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use krishiv_delta::DeltaBatch;
use krishiv_ivm::IncrementalViewSpec;
use krishiv_runtime::{EmbeddedIvmJob, RemoteIvmJob, SharedIvmJobRegistry};

use super::job::{Checkpointable, FeedableJob, Job, JobKind, StepReport};
use crate::Result;

/// A handle to an incremental-view-maintenance job.
///
/// Returned by [`Session::ivm`](crate::Session::ivm). The same handle works in
/// both embedded (in-process) and distributed (coordinator) modes — the session
/// picks the variant from its execution mode, so callers write identical code.
#[derive(Debug, Clone)]
pub enum IvmJob {
    /// In-process execution.
    Embedded(EmbeddedIvmJob),
    /// Remote execution via coordinator HTTP.
    Remote(RemoteIvmJob),
}

impl IvmJob {
    /// Create (or attach to) an embedded IVM job in `registry`.
    pub fn embedded(registry: &SharedIvmJobRegistry, name: &str) -> Result<Self> {
        Ok(Self::Embedded(EmbeddedIvmJob::create(registry, name)?))
    }

    /// Create a remote IVM job on the coordinator at `coordinator_http`.
    pub async fn remote(coordinator_http: &str, name: &str) -> Result<Self> {
        Ok(Self::Remote(
            RemoteIvmJob::create(coordinator_http, Some(name)).await?,
        ))
    }

    /// Register or update an incremental view on this job.
    pub async fn register_view(&self, spec: IncrementalViewSpec) -> Result<()> {
        match self {
            Self::Embedded(j) => j.register_view(spec)?,
            Self::Remote(j) => j.register_view(&spec).await?,
        }
        Ok(())
    }

    /// Enable delta-checkpoint accumulation (embedded only; remote always on).
    pub fn enable_delta_checkpoints(&self) -> Result<()> {
        match self {
            Self::Embedded(j) => j.enable_delta_checkpoints()?,
            Self::Remote(_) => {}
        }
        Ok(())
    }

    /// Enable content-addressed input dedup (embedded only).
    pub fn enable_input_dedup(&self) -> Result<()> {
        match self {
            Self::Embedded(j) => j.enable_input_dedup()?,
            Self::Remote(_) => {}
        }
        Ok(())
    }
}

impl Job for IvmJob {
    fn job_id(&self) -> &str {
        match self {
            Self::Embedded(j) => j.job_id(),
            Self::Remote(j) => j.job_id(),
        }
    }

    fn kind(&self) -> JobKind {
        JobKind::Ivm
    }
}

#[async_trait]
impl FeedableJob for IvmJob {
    async fn feed(&self, source: &str, delta: &DeltaBatch) -> Result<()> {
        match self {
            Self::Embedded(j) => j.feed(source, delta.clone())?,
            Self::Remote(j) => j.feed(source, delta).await?,
        }
        Ok(())
    }

    async fn feed_snapshot(&self, source: &str, batches: &[RecordBatch]) -> Result<()> {
        match self {
            Self::Embedded(j) => j.feed_snapshot(source, batches)?,
            Self::Remote(j) => j.feed_snapshot(source, batches).await?,
        }
        Ok(())
    }

    async fn step(&self) -> Result<StepReport> {
        Ok(match self {
            Self::Embedded(j) => {
                let summary = j.step().await?;
                StepReport {
                    active_views: summary.active_views,
                    total_output_rows: summary.total_output_rows,
                    tick: j.tick()?,
                }
            }
            Self::Remote(j) => {
                let s = j.step().await?;
                StepReport {
                    active_views: s.active_views,
                    total_output_rows: s.total_output_rows,
                    tick: s.tick,
                }
            }
        })
    }

    async fn snapshot(&self, view: &str) -> Result<Option<RecordBatch>> {
        Ok(match self {
            Self::Embedded(j) => j.snapshot(view)?,
            Self::Remote(j) => j.snapshot(view).await?,
        })
    }
}

#[async_trait]
impl Checkpointable for IvmJob {
    async fn checkpoint(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Embedded(j) => j.checkpoint()?,
            Self::Remote(j) => j.checkpoint().await?,
        })
    }

    async fn restore(&self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Embedded(j) => j.restore(bytes)?,
            Self::Remote(j) => j.restore(bytes).await?,
        }
        Ok(())
    }

    async fn checkpoint_delta(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Embedded(j) => j.checkpoint_delta()?,
            Self::Remote(j) => j.checkpoint_delta().await?,
        })
    }

    async fn restore_delta(&self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Embedded(j) => j.restore_delta(bytes)?,
            Self::Remote(j) => j.restore_delta(bytes).await?,
        }
        Ok(())
    }
}
