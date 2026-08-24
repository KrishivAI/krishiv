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
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StepReport {
    /// Views (or operators) that produced non-empty output this tick.
    pub active_views: usize,
    /// Total output rows emitted across all outputs this tick.
    pub total_output_rows: usize,
    /// The tick counter after this step.
    pub tick: u64,
    /// View names that ran on the O(state) DiffBased path during this step
    /// (either forced by `force_diff_based` or because no incremental plan was
    /// built — e.g. unsupported join types). Useful for operators to surface
    /// the join-type degradations called out in the IVM plan code.
    pub degraded_views: Vec<String>,
    /// Per-view errors that caused a view to be skipped during this step.
    /// Step did not panic; subsequent ticks re-evaluate. Each entry is a
    /// `(view_name, kind, message)` triple.
    pub errored_views: Vec<ViewError>,
    /// Whether the two vectors above are a *report* at all.
    ///
    /// They are the only view-level failure channel there is — a failing view
    /// does not make `step` return `Err` — so "empty" has to mean one thing.
    /// It did not: distributed ticks filled both with `Vec::new()` because the
    /// coordinator's step response carried counters only, making a broken view
    /// indistinguishable from a healthy one (IVM-AUD-API-A5). Check this before
    /// concluding anything from an empty `errored_views`.
    pub view_health: ViewHealth,
}

/// Provenance of [`StepReport::degraded_views`] / [`StepReport::errored_views`].
///
/// Use [`ViewHealth::is_reported`] for the common "did anything report" check;
/// match on the variant when you need to know whether the lists are complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewHealth {
    /// The engine that ran the tick reported per-view health. Empty vectors
    /// mean "no view degraded / failed".
    ///
    /// The counts say how many entries the wire dropped: the resident tick
    /// frame caps each vector at 256, and before IVM-AUD-A5-RESIDENT-b those
    /// counts stopped at the HTTP layer, so a Rust caller read a truncated
    /// list as a whole one. They are carried here rather than as loose fields
    /// on `StepReport` so that "is this a report" and "is it complete" cannot
    /// be answered separately. Both are 0 unless a single tick failed or
    /// degraded more than 256 views.
    Reported {
        degraded_omitted: u32,
        errored_omitted: u32,
    },
    /// Nothing reported per-view health for this tick, so the vectors are empty
    /// for lack of a signal — **not** because every view is healthy. The string
    /// says which link in the chain has no signal to give.
    Unreported(String),
}

impl ViewHealth {
    /// Whether the report's health vectors can be trusted as a statement about
    /// the views. True even if the lists were truncated — see
    /// [`is_complete`](Self::is_complete).
    pub fn is_reported(&self) -> bool {
        matches!(self, Self::Reported { .. })
    }

    /// Whether a report arrived **and** carried every entry. False when the
    /// resident tick wire dropped entries past its 256-per-vector cap.
    pub fn is_complete(&self) -> bool {
        matches!(
            self,
            Self::Reported {
                degraded_omitted: 0,
                errored_omitted: 0
            }
        )
    }
}

/// A default-constructed `StepReport` describes no tick, so it can make no
/// claim about any view's health.
impl Default for ViewHealth {
    fn default() -> Self {
        Self::Unreported("no step was reported".to_owned())
    }
}

/// One view's failure during a `step`. Carried in [`StepReport::errored_views`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewError {
    pub view: String,
    pub kind: ViewErrorKind,
    pub message: String,
}

/// Category of failure for a view during a `step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewErrorKind {
    /// The incremental operator (`apply`) returned an error (trace capacity,
    /// schema mismatch, type coercion, etc.).
    OperatorApply,
    /// The view's SQL body failed to execute (column not found, type mismatch).
    ViewSql,
    /// The view's published output failed (downstream backpressure, etc.).
    Publish,
    /// A recursive view's body did not reach a fixed point within the engine's
    /// iteration cap, so the tick has no value for it and its previous value
    /// stands (IVM-AUD-CORE-12).
    FixpointNotConverged,
    /// The engine that ran the tick named a failure kind this binary does not
    /// know — a coordinator newer than this client. The view really did fail;
    /// only its category is unknown, and the reported name is preserved at the
    /// front of [`ViewError::message`]. Never produced by an embedded tick.
    Unrecognized,
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

    /// Feed a DeltaBatch and step in one call. Equivalent to `feed` + `step`.
    /// Returns the step summary.
    async fn feed_and_step(&self, source: &str, delta: &DeltaBatch) -> Result<StepReport> {
        self.feed(source, delta).await?;
        self.step().await
    }

    /// Feed a plain RecordBatch as insertions and step in one call.
    /// Convenience wrapper: creates a `DeltaBatch::from_inserts` automatically.
    async fn feed_inserts_and_step(&self, source: &str, batch: &RecordBatch) -> Result<StepReport> {
        let delta = DeltaBatch::from_inserts(batch.clone()).map_err(|e| {
            crate::error::KrishivError::Runtime {
                message: e.to_string(),
            }
        })?;
        self.feed_and_step(source, &delta).await
    }
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
