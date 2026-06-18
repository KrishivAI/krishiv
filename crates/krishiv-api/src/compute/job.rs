//! Unified job traits shared by IVM and streaming handles.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use krishiv_delta::DeltaBatch;

use crate::Result;

/// Which kind of long-lived job a handle drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    /// Incremental view maintenance (DeltaBatch / Z-set).
    Ivm,
    /// Continuous windowed streaming.
    Stream,
}

/// Result of advancing a feedable job by one tick.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StepReport {
    /// Views (or operators) that produced non-empty output this tick.
    pub active_views: usize,
    /// Total output rows emitted across all outputs this tick.
    pub total_output_rows: usize,
    /// The tick counter after this step.
    pub tick: u64,
}

/// Identity common to every long-lived job. Batch is not a `Job` (it is
/// one-shot and returns a `DataFrame`).
pub trait Job {
    /// The job's stable identifier.
    fn job_id(&self) -> &str;
    /// Which execution model this job uses.
    fn kind(&self) -> JobKind;
}

/// A job that accepts input deltas and advances a logical clock.
///
/// This is where the **single `feed` primitive** lives. Build the `DeltaBatch`
/// with the appropriate constructor first (`DeltaBatch::from_inserts`,
/// `from_deletes`, `from_cdc`), then feed it.
#[async_trait]
pub trait FeedableJob: Job {
    /// Feed a `DeltaBatch` as input for a named source; buffered until `step`.
    async fn feed(&self, source: &str, delta: &DeltaBatch) -> Result<()>;

    /// Feed a full snapshot, differentiated against the previous one for this
    /// source (the streaming bridge). Stateful inside the job.
    async fn feed_snapshot(&self, source: &str, batches: &[RecordBatch]) -> Result<()>;

    /// Advance one tick.
    async fn step(&self) -> Result<StepReport>;

    /// Read the current materialized snapshot of a view (`None` if not yet produced).
    async fn snapshot(&self, view: &str) -> Result<Option<RecordBatch>>;
}

/// A job whose state can be checkpointed and restored.
#[async_trait]
pub trait Checkpointable: Job {
    /// Serialize a full checkpoint.
    async fn checkpoint(&self) -> Result<Vec<u8>>;
    /// Restore from a full checkpoint.
    async fn restore(&self, bytes: &[u8]) -> Result<()>;
    /// Serialize only the deltas accumulated since the last call.
    async fn checkpoint_delta(&self) -> Result<Vec<u8>>;
    /// Apply delta-checkpoint bytes on top of restored state.
    async fn restore_delta(&self, bytes: &[u8]) -> Result<()>;
}
