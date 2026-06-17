#![forbid(unsafe_code)]

//! Incremental View Maintenance (IVM) engine for Krishiv.
//!
//! `IncrementalFlow` is the primary API: register views, feed source deltas,
//! call `step_datafusion()` each tick, and subscribe to output `DeltaBatch`es.

pub mod error;
pub mod flow;

pub use error::{IvmError, IvmResult};
pub use flow::{IncrementalFlow, StepSummary};

// Re-export the key delta types so callers need only `krishiv-ivm`.
pub use krishiv_delta::{
    DeltaBatch, IncrementalViewRegistry, IncrementalViewSpec,
    apply_delta, differentiate,
    serialize_delta_batch, deserialize_delta_batch,
};
