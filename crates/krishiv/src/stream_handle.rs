use std::sync::{Arc, Mutex};

use krishiv_api::KrishivError;
use krishiv_api::Session;

/// Handle to a submitted streaming job.
///
/// Returned by [`Relation::sink_to`] for unbounded streams and by bounded sink
/// operations (where `job_id` is `"completed"`).
pub struct StreamHandle {
    job_id: String,
    session: Session,
    cancelled: Arc<Mutex<bool>>,
}

impl StreamHandle {
    pub(crate) fn new(job_id: String, session: Session) -> Self {
        Self {
            job_id,
            session,
            cancelled: Arc::new(Mutex::new(false)),
        }
    }

    pub(crate) fn completed() -> Self {
        // For bounded sink_to — job finished synchronously
        let session = krishiv_api::SessionBuilder::new()
            .build()
            .expect("completed handle session");
        Self::new("completed".into(), session)
    }

    /// Unique identifier for this streaming job.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Signal the background thread to stop processing.
    pub fn cancel(&self) -> crate::Result<()> {
        *self.cancelled.lock().unwrap() = true;
        Ok(())
    }

    /// Returns `true` if [`cancel`] has been called.
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.lock().unwrap()
    }

    /// Drain newly emitted output batches from a continuous streaming job.
    pub fn poll_output(&self) -> crate::Result<Vec<arrow::record_batch::RecordBatch>> {
        krishiv_async_util::block_on(self.session.poll_stream_job(&self.job_id))
            .map_err(Into::into)
    }

    /// Expose the cancel flag `Arc` so it can be shared with background threads.
    pub(crate) fn cancelled_flag(&self) -> Arc<Mutex<bool>> {
        Arc::clone(&self.cancelled)
    }
}

impl std::fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamHandle")
            .field("job_id", &self.job_id)
            .field("is_cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}
