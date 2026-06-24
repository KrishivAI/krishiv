#![forbid(unsafe_code)]

//! Arrow-native physical execution operators for Krishiv.

pub use krishiv_plan::lower_to_physical;

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors that can occur during physical execution.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecError {
    /// An Arrow error occurred.
    #[error("arrow error: {0}")]
    Arrow(String),
    /// A required column was not found in the schema.
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    /// A data type is not supported for this operation.
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
    /// An input batch contains values that violate an operator contract.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// An upstream stream failed before the operator could process its input.
    #[error("upstream stream error: {0}")]
    Upstream(String),
    /// A window operator was constructed with an invalid configuration.
    #[error("invalid window config: {0}")]
    InvalidWindowConfig(String),
    /// Incoming batch schema cannot be evolved to the target schema.
    #[error("incompatible schema evolution: {0}")]
    IncompatibleSchemaEvolution(String),
    /// A CEP pattern matching error occurred.
    #[error("cep error: {0}")]
    Cep(String),
    /// A memory budget was exceeded — caller should abort the operator.
    #[error("oom: {0}")]
    Oom(String),
}

impl From<arrow::error::ArrowError> for ExecError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e.to_string())
    }
}

/// Convenience alias for `Result<T, ExecError>`.
pub type ExecResult<T> = Result<T, ExecError>;

// ── JoinType ──────────────────────────────────────────────────────────────────

pub use krishiv_plan::JoinType;

// ── Sub-modules ───────────────────────────────────────────────────────────────

pub mod adaptive;
pub mod aggregate;
pub mod broadcast_state;
pub mod cep;
pub mod connected_streams;
pub mod continuous;
pub mod dedup_operator;
pub mod interval_join;
pub mod join;
pub mod live_table;
pub mod memo;
pub mod operator_config;
pub mod operator_runtime;
pub mod process_fn;
pub mod queue;
pub mod schema_normalize;
pub mod side_output;
pub mod state_descriptor;
pub mod temporal_join;
#[cfg(test)]
mod watermark_e2e;
pub mod watermark_util;
pub mod window;

pub use adaptive::{
    AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, HeavyHittersTracker,
    HotKeyReport, RateLimiter, SinkLatencyTracker, StreamingPartitionAdvisor, ThrottleCommand,
};
pub use aggregate::{AggExpr, AggFunction};
pub use broadcast_state::{
    BroadcastContext, BroadcastProcessExecutor, BroadcastProcessFunction, BroadcastStateDescriptor,
};
pub use connected_streams::{CoProcessExecutor, CoProcessFunction, ConnectedStreams};
pub use continuous::ContinuousWindowExecutor;
pub use operator_config::{OperatorConfig, OperatorUid};
pub use operator_runtime::{execute_bounded_window, execute_streaming_window};
pub use process_fn::{
    ProcessContext, ProcessFunction, ProcessFunctionExecutor, TimerEntry, TimerKind,
};
pub use queue::{
    OperatorMessage, OperatorQueueError, OperatorQueueMetrics, OperatorQueueReceiver,
    OperatorQueueSender, operator_queue,
};
pub use schema_normalize::{ColumnRenameMap, SchemaNormalizeOperator};
pub use state_descriptor::{
    ListState, MapState, ReducingState, StateError, StateValue, ValueState,
};
pub use window::{
    CountWindowOperator, CountWindowSpec, MultiSourceWatermarkState, SessionWindowOperator,
    SessionWindowSpec, SlidingWindowOperator, SlidingWindowSpec, StateBackedSessionWindowOperator,
    StateBackedSlidingWindowOperator, StateBackedTumblingWindowOperator, TumblingWindowOperator,
    TumblingWindowSpec, WatermarkState,
};
#[cfg(test)]
mod lib_tests;
