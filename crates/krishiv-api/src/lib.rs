#![forbid(unsafe_code)]

//! Public facade for `krishiv-api`.
//!
//! High-level client API for constructing local, batch SQL, and streaming pipelines.

pub mod blocking;
pub mod catalog;
pub mod dataframe;
pub mod error;
pub mod expression;
pub mod io;
pub mod prepared;
pub mod process;
pub mod query;
pub mod session;
pub mod stream;
pub mod streaming_builder;
pub mod streaming_dataframe;
pub mod types;
pub mod window;

#[cfg(test)]
mod tests;

// Re-export the public API at the crate root for perfect source compatibility.
pub use catalog::{
    FunctionIdentifier, FunctionMetadata, Identifier, Namespace, TableIdentifier, TableMetadata,
    ViewIdentifier,
};
pub use blocking::BlockingSession;
pub use dataframe::{
    Boundedness, DataFrame, ExecutionResult, ExplainMode, GroupedDataFrame, GroupingSpec, JoinType,
    PivotValue, QueryExecutionStats,
};
pub use query::{QueryCompletion, QueryHandle, QueryId, QueryProgress, QueryStatus};
pub use error::{KrishivError, Result};
pub use expression::{
    AggregateFunction as ExprAggregateFunction, BinaryOperator as ExprBinaryOperator,
    EXPRESSION_FORMAT_VERSION, Expr, ExprDataType, ExprField, IntervalUnit, Literal, NullOrdering,
    ScalarValue, SortDirection, TimeUnit, avg, col, count, count_all, function, lit, max, min, sum,
};
pub use io::{
    CsvReadOptions, DataFormat, DataFrameReader, DataFrameWriter, FileReadOptions,
    FileWriteOptions, JsonReadOptions, MalformedRecordPolicy, ParquetReadOptions,
};
pub use krishiv_connectors::{
    DatabaseIoOptions, FileLayout, FileSortDirection, KafkaIoOptions, SchemaEvolutionMode,
    SortField, WriteDistribution, WriteMode,
};
pub use prepared::PreparedStatement;
pub use process::{apply_async_io, apply_process_function};
pub use session::{Session, SessionBuilder};
pub use stream::{KeyedStream, Stream};
pub use streaming_builder::{
    DataStreamReader, DataStreamWriter, ForeachBatchFn, StreamingOutputMode, StreamingQuery,
    StreamingQueryProgress, StreamingTrigger,
};
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
pub use krishiv_dataflow::{
    AggExpr, AggFunction,
    BroadcastContext, BroadcastProcessExecutor, BroadcastProcessFunction, BroadcastStateDescriptor,
    CoProcessExecutor, CoProcessFunction, ConnectedStreams,
    ListState, MapState, OperatorConfig, OperatorUid, ProcessContext, ProcessFunction,
    ProcessFunctionExecutor, ReducingState, StateError, StateValue, TimerEntry, TimerKind,
    ValueState,
};
pub use krishiv_plan::udf::{ScalarUdf, UdfError, UdfRegistry};
pub use krishiv_plan::{LogicalPlan as KrishivLogicalPlan, PhysicalPlan as KrishivPhysicalPlan};
pub use krishiv_runtime::{
    ClusterEndpoints, InProcessCluster, InProcessStreamingRuntime, JobStatus, LocalJobRegistry,
    LocalWindowExecutionSpec, LocalWindowKind, execute_streaming_window, execute_windowed_stream,
    is_streaming_plan,
};
pub use krishiv_state::TtlConfig;

// Governance hook/auth interfaces
pub use krishiv_plan::governance::{AuthProvider, PolicyHook};
