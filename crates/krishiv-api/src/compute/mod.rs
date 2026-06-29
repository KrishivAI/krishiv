//! Unified compute handles — one mode-agnostic model across embedded and
//! distributed execution.
//!
//! A [`Session`](crate::Session) is the single entry point; it hands out job
//! handles that behave identically regardless of where they run:
//!
//! - [`Session::batch`](crate::Session::batch) → a one-shot `DataFrame` (collect).
//! - [`Session::ivm`](crate::Session::ivm) → an [`IvmJob`] (feed / step / snapshot).
//! - [`Session::stream`](crate::Session::stream) → a [`StreamJob`] (push / drain).
//!
//! Jobs share a small trait hierarchy: [`Job`] (identity), [`FeedableJob`]
//! (the one `feed` + `step`/`snapshot`), and [`Checkpointable`] (durable state).
//! Batch is deliberately *not* a `Job` — it is one-shot and returns a `DataFrame`.

mod ivm;
mod job;
mod stream;

pub use ivm::IvmJob;
pub use job::{Checkpointable, FeedableJob, Job, JobKind, StepReport};
pub use stream::{EmbeddedStreamJob, StreamJob};
