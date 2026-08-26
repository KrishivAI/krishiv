#![forbid(unsafe_code)]

//! Incremental View Maintenance (IVM) engine for Krishiv.
//!
//! `IncrementalFlow` is the primary API: register views, feed source deltas,
//! call `step_datafusion()` each tick, and subscribe to output `DeltaBatch`es.

pub mod decompose;
pub mod error;
pub mod flow;
pub mod partitioned;
pub mod plan;
pub mod provenance;
pub mod spill;
pub mod vector_sink;
pub mod window_rewrite;

pub use decompose::{Hop, decompose};
pub use error::{IvmError, IvmResult};
pub use flow::{
    ATTACH_ECHO_MAGIC, IVM_TICK_WIRE_VERSION, IncrementalFlow, RetainedState, StepSummary,
    TickHealth, TickResult, ViewDeltaStats, ViewError, ViewErrorKind, ViewExecution,
    WireCapabilities, WireViewError, coalesce_pending, decode_attach_echo, decode_delta_map,
    decode_tick_result, encode_attach_echo, encode_delta_map, encode_ivm_attach_fragment,
    encode_ivm_detach_fragment, encode_ivm_tick_fragment, encode_tick_result, view_error_kind_name,
};
pub use partitioned::PartitionedIncrementalFlow;
pub use plan::{ViewPlan, ViewPlanKind, build_view_plan, partition_key_from_sql};
pub use provenance::{ProvenanceIndex, hash_all_rows, hash_batch_row};
pub use spill::{
    ivm_memory_limit_bytes, shard_memory_limit_bytes, spill_session_context,
    spill_session_context_with_limit,
};
pub use vector_sink::testing::InMemoryVectorSink;
pub use vector_sink::{
    IvmVectorSink, VectorFuture, VectorViewHandle, VectorViewHealth, VectorViewSpec,
    VectorViewStatus, spawn_vector_view,
};

// Re-export the key delta types so callers need only `krishiv-ivm`.
pub use krishiv_delta::{
    DeltaBatch, IncrementalViewRegistry, IncrementalViewSpec, LatenessSpec, apply_delta,
    deserialize_delta_batch, differentiate, serialize_delta_batch,
};
