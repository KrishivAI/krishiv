#![forbid(unsafe_code)]

//! Public facade for `krishiv-api`.
//!
//! High-level client API for constructing local, batch SQL, and streaming pipelines.

pub mod blocking;
pub mod catalog;
pub mod compute;
pub mod connector_runtime;
pub mod dataframe;
pub mod engines;
pub mod error;
pub mod expression;
pub mod incremental_flow;
pub mod io;
/// P11: Materialized Table API — materialized tables with managed refresh lifecycle.
pub mod materialized_table;
pub mod pipeline;
pub mod prepared;
pub mod process;
pub mod query;
pub mod session;
pub mod sql_job;
pub mod stream;
pub mod streaming_builder;
pub mod streaming_dataframe;
pub mod timers;
pub mod types;
pub mod window;

#[cfg(test)]
mod conformance;
#[cfg(test)]
mod delivery_cert;
#[cfg(test)]
mod mode_conformance;
#[cfg(test)]
mod tests;

// Re-export the public API at the crate root for perfect source compatibility.
pub use blocking::BlockingSession;
pub use catalog::{
    FunctionIdentifier, FunctionMetadata, Identifier, Namespace, TableIdentifier, TableMetadata,
    ViewIdentifier,
};
pub use compute::{
    Checkpointable, EmbeddedStreamJob, FeedableJob, IvmJob, Job, JobKind, StepReport, StreamJob,
    ViewError, ViewErrorKind,
};
pub use connector_runtime::{
    ConnectorSinkProvider, ConnectorSourceProvider, DebeziumCdcSourceProvider,
    RuntimeQueryExecutor, durable_engine_runtime, embedded_connector_runtime,
    embedded_consolidating_runtime, runtime_backed_engine_runtime,
};
pub use engines::{
    BatchEngine, IncrementalEngine, RunningJob, StreamingEngine, run_job, spawn_streaming_job,
};
// The shared engine vocabulary — the same `EngineKind`/`CompiledJob` are used by
// the SQL, Python, and Rust front-ends so engine selection never forks per API.
pub use dataframe::{
    Boundedness, DataFrame, ExecutionResult, ExplainMode, GroupedDataFrame, GroupingSpec, JoinType,
    PivotValue, QueryExecutionStats,
};
pub use error::{KrishivError, Result};
pub use expression::{
    AggregateFunction as ExprAggregateFunction, BinaryOperator as ExprBinaryOperator,
    EXPRESSION_FORMAT_VERSION, Expr, ExprDataType, ExprField, IntervalUnit, Literal, NullOrdering,
    ScalarValue, SortDirection, TimeUnit, WindowFrame, WindowFrameBound, WindowFrameUnits, avg,
    col, count, count_all, cume_dist, dense_rank, first_value, function, lag, last_value, lead,
    lit, max, min, nth_value, ntile, percent_rank, rank, row_number, sum,
};
pub use incremental_flow::{IncrementalFlow, StepSummary};
pub use io::{
    CsvReadOptions, DataFormat, DataFrameReader, DataFrameWriter, FileReadOptions,
    FileWriteOptions, JsonReadOptions, MalformedRecordPolicy, ParquetReadOptions,
};
pub use krishiv_connectors::{
    DatabaseIoOptions, FileLayout, FileSortDirection, KafkaIoOptions, SchemaEvolutionMode,
    SortField, WriteDistribution, WriteMode,
};
pub use krishiv_engine_core::{
    ChangelogBatch, CompiledJob, ComputeEngine, DeliveryContract, EngineError, EngineKind,
    EngineRuntime, JobHandle, Placement, QueryExecutor, RowKind, SinkSpec, SourceSpec, StatePolicy,
};
// Note: `krishiv_engine_core::JobStatus` is intentionally not re-exported at the
// crate root — `krishiv_runtime::JobStatus` already occupies that name. Engine
// job status is reached via [`JobHandle::status`].
pub use pipeline::{
    CdcChange, Egress, Expectation, Ingest, OnViolation, Pipeline, PipelineBuilder, PipelineMode,
    RunPolicy, ViewDef,
};
pub use prepared::PreparedStatement;
pub use process::{apply_async_io, apply_process_function};
pub use query::{QueryCompletion, QueryHandle, QueryId, QueryProgress, QueryStatus};
pub use session::{
    CompiledContinuousStreamJob, ContinuousStreamCheckpoint, ContinuousStreamStatus,
    RegisteredContinuousStreamJob, Session, SessionBuilder, SubmittedSqlJobState,
    SubmittedSqlJobStatus,
};
pub use sql_job::compile_sql_job;
pub use stream::{KeyedStream, Stream};
pub use streaming_builder::{
    DataStreamReader, DataStreamWriter, ForeachBatchFn, KafkaTransactionalConfig,
    StreamingOutputMode, StreamingQuery, StreamingQueryProgress, StreamingTrigger,
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
    AggExpr, AggFunction, BroadcastContext, BroadcastProcessExecutor, BroadcastProcessFunction,
    BroadcastStateDescriptor, CoProcessExecutor, CoProcessFunction, ConnectedStreams, ListState,
    MapState, OperatorConfig, OperatorUid, ProcessContext, ProcessFunction,
    ProcessFunctionExecutor, ReducingState, StateError, StateValue, TimerEntry, TimerKind,
    ValueState,
};
pub use krishiv_plan::udf::{ScalarUdf, UdfError, UdfRegistry};
pub use krishiv_plan::{LogicalPlan as KrishivLogicalPlan, PhysicalPlan as KrishivPhysicalPlan};
pub use krishiv_runtime::{
    ClusterEndpoints, CoordinatorBatchSqlJobResult, InProcessCluster, InProcessStreamingRuntime,
    JobStatus, LocalJobRegistry, LocalWindowExecutionSpec, LocalWindowKind,
    execute_streaming_window, execute_windowed_stream, is_streaming_plan,
};
pub use krishiv_state::TtlConfig;

// Governance hook/auth interfaces
pub use krishiv_plan::governance::{AuthProvider, PolicyHook};
