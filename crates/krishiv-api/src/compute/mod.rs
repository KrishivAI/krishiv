//! Unified compute handles — one mode-agnostic model across embedded and
//! distributed execution.
//!
//! A [`Session`](crate::Session) is the single entry point; it hands out job
//! handles that behave identically regardless of where they run:
//!
//! - [`Session::batch`](crate::Session::batch) → a one-shot `DataFrame` (collect).
//! - [`Session::ivm`](crate::Session::ivm) → an [`IvmJob`] (feed / step / snapshot).
//! - [`Session::stream`](crate::Session::stream) → the unified [`crate::StreamingJob`].
//!
//! Jobs share a small trait hierarchy: [`Job`] (identity), [`FeedableJob`]
//! (the one `feed` + `step`/`snapshot`), and [`Checkpointable`] (durable state).
//! Batch is deliberately *not* a `Job` — it is one-shot and returns a `DataFrame`.

mod incremental_df;
mod ivm;
pub mod job;

pub use incremental_df::IncrementalDataFrame;
pub use ivm::IvmJob;
pub use job::{Checkpointable, FeedableJob, Job, JobKind, StepReport, ViewError, ViewErrorKind};
