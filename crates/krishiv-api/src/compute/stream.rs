//! Mode-agnostic streaming job handle.
//!
//! Symmetric with [`IvmJob`](super::ivm::IvmJob): `session.stream(...)` returns a
//! [`StreamJob`] that pushes input and drains output identically in embedded and
//! distributed modes.

use arrow::record_batch::RecordBatch;
use krishiv_runtime::RemoteStreamingJob;

use super::job::{Job, JobKind};
use crate::{Result, Session};

/// An embedded (in-process) streaming job, wrapping the session's continuous
/// stream registry. This makes `session.stream()` symmetric with the remote
/// path — callers push input and drain output through one handle.
#[derive(Clone)]
pub struct EmbeddedStreamJob {
    session: Session,
    job_id: String,
}

impl EmbeddedStreamJob {
    pub(crate) fn new(session: Session, job_id: String) -> Self {
        Self { session, job_id }
    }

    /// The job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Push input batches to the streaming job.
    pub fn push(&self, batches: Vec<RecordBatch>) -> Result<()> {
        self.session.push_stream_job_input(&self.job_id, batches)
    }

    /// Drain newly emitted output batches.
    pub async fn drain(&self) -> Result<Vec<RecordBatch>> {
        self.session.poll_stream_job(&self.job_id).await
    }
}

impl std::fmt::Debug for EmbeddedStreamJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedStreamJob")
            .field("job_id", &self.job_id)
            .finish_non_exhaustive()
    }
}

/// A handle to a continuous windowed streaming job.
///
/// Returned by [`Session::stream`](crate::Session::stream). Works in both
/// embedded and distributed modes.
#[derive(Clone)]
pub enum StreamJob {
    /// In-process execution. Boxed because an embedded handle carries a full
    /// [`Session`], which is much larger than the remote variant.
    Embedded(Box<EmbeddedStreamJob>),
    /// Remote execution via coordinator HTTP.
    Remote(RemoteStreamingJob),
}

impl StreamJob {
    /// Push input batches to the streaming job.
    pub async fn push(&self, batches: Vec<RecordBatch>) -> Result<()> {
        match self {
            Self::Embedded(j) => j.push(batches),
            Self::Remote(j) => j.push(&batches).await.map_err(Into::into),
        }
    }

    /// Drain newly emitted output batches.
    pub async fn drain(&self) -> Result<Vec<RecordBatch>> {
        match self {
            Self::Embedded(j) => j.drain().await,
            Self::Remote(j) => j.drain().await.map_err(Into::into),
        }
    }
}

impl Job for StreamJob {
    fn job_id(&self) -> &str {
        match self {
            Self::Embedded(j) => j.job_id(),
            Self::Remote(j) => j.job_id(),
        }
    }

    fn kind(&self) -> JobKind {
        JobKind::Stream
    }
}

impl std::fmt::Debug for StreamJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Embedded(j) => f.debug_tuple("StreamJob::Embedded").field(j).finish(),
            Self::Remote(j) => f.debug_tuple("StreamJob::Remote").field(j).finish(),
        }
    }
}
