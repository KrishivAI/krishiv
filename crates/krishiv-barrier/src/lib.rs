#![forbid(unsafe_code)]

//! Checkpoint barrier dispatch types and acknowledgment tracking (R16).

mod client;
mod dispatch;
mod tracker;

pub use client::inject_barrier;
pub use dispatch::{BarrierDispatchPlan, BarrierDispatchTarget};
pub use tracker::CheckpointBarrierTracker;
