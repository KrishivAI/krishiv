#![forbid(unsafe_code)]

//! Public facade for `krishiv-api`.
//!
//! High-level client API for constructing local, batch SQL, and streaming pipelines.

pub mod dataframe;
pub mod error;
pub mod session;
pub mod stream;
pub mod streaming_dataframe;
pub mod types;
pub mod window;

#[cfg(test)]
mod tests;

// Re-export the public API at the crate root for perfect source compatibility.
pub use dataframe::{DataFrame, ExecutionResult};
pub use error::{KrishivError, Result};
pub use session::{Session, SessionBuilder};
pub use stream::{KeyedStream, Stream};
pub use streaming_dataframe::{
    KrishivStream, NamedSideOutputStream, StreamingDataFrame, StreamingOutputStreams,
};
pub use types::{DeploymentTarget, ExecutionMode, QueryResult, StreamBatch, StreamMode};
pub use window::{
    MultiSourceWatermarkSpec, SessionWindowedStream, SlidingWindowedStream, StateTtlConfig,
    WatermarkSpec, WindowedStream,
};

// Re-export Arrow, plan, and runtime types used by public APIs.
pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub use arrow::record_batch::RecordBatch;
pub use krishiv_dataflow::{AggExpr, AggFunction};
pub use krishiv_plan::{LogicalPlan as KrishivLogicalPlan, PhysicalPlan as KrishivPhysicalPlan};
pub use krishiv_runtime::{
    ClusterEndpoints, InProcessCluster, InProcessStreamingRuntime, JobStatus, LocalJobRegistry,
    LocalWindowExecutionSpec, LocalWindowKind, execute_streaming_window, execute_windowed_stream,
    is_streaming_plan,
};
pub use krishiv_state::TtlConfig;
pub use krishiv_plan::udf::{ScalarUdf, UdfError, UdfRegistry};

// Governance hook/auth interfaces
pub use krishiv_plan::governance::{AuthProvider, PolicyHook};
