#![forbid(unsafe_code)]

//! Incremental View Maintenance (IVM) engine for Krishiv.
//!
//! `IncrementalFlow` is the primary API: register views, feed source deltas,
//! call `step_datafusion()` each tick, and subscribe to output `DeltaBatch`es.

pub mod error;
pub mod flow;
pub mod partitioned;
pub mod plan;
pub mod provenance;
pub mod vector_sink;

pub use error::{IvmError, IvmResult};
pub use flow::{
    IncrementalFlow, StepSummary, ViewError, ViewErrorKind, coalesce_pending, decode_batch_map,
    encode_batch_map, encode_ivm_step_fragment,
};
pub use partitioned::PartitionedIncrementalFlow;
pub use plan::{
    ViewPlan, ViewPlanKind, build_view_plan, partition_key_for_view, partition_key_from_sql,
};
pub use provenance::{ProvenanceIndex, hash_all_rows, hash_batch_row};
pub use vector_sink::testing::InMemoryVectorSink;
pub use vector_sink::{IvmVectorSink, VectorFuture, VectorViewSpec, spawn_vector_view};

// Re-export the key delta types so callers need only `krishiv-ivm`.
pub use krishiv_delta::{
    DeltaBatch, IncrementalViewRegistry, IncrementalViewSpec, apply_delta, deserialize_delta_batch,
    differentiate, serialize_delta_batch,
};
