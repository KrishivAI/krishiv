#![forbid(unsafe_code)]

//! Public facade for `krishiv-executor`.
//!
//! Executor runtime: receives task assignments, executes DataFusion queries,
//! and reports results back to the job coordinator.

// Root-level modules containing domain implementations.
pub mod assignment_inbox;
pub mod barrier;
pub mod barrier_grpc;
pub mod barrier_transport;
pub mod cli;
pub mod error;
pub mod execution_model;
pub(crate) mod fragment;
pub mod grpc;
pub mod grpc_client;
pub mod llm_throttle;
pub mod runner;
pub mod source_throttle;
pub mod transport;

#[cfg(test)]
mod tests;

// Re-export the public API at the crate root for source compatibility.
pub use assignment_inbox::{AssignmentPushOutcome, ExecutorAssignmentInbox};
pub use error::{ExecutorError, ExecutorResult, ExecutorTransportResult};
pub use execution_model::ExecutionModel;

// Re-exports of barrier, runner, transport, and grpc types.
pub use barrier::{BarrierSimulator, BarrierSnapshot};
pub use barrier_grpc::{ExecutorBarrierService, executor_barrier_grpc_server};
pub use barrier_transport::{
    BarrierInjector, SharedBarrierInjector, SharedKeyGroupRanges, make_checkpoint_barrier,
};
pub use grpc::{
    ExecutorTaskAuthConfig, ExecutorTaskGrpcService, ExecutorTaskInboxService,
    executor_task_grpc_server,
};
pub use runner::{
    ContinuousJobDrainer, ExecutorTaskOutput, ExecutorTaskOutputKind, ExecutorTaskRunReport,
    ExecutorTaskRunner, ShuffleContext, TaskRunner,
};
pub use source_throttle::SourceThrottleTable;
pub use transport::{
    ExecutorConfig, ExecutorRuntime, ExecutorTransportError, GrpcCoordinatorService,
    serve_executor_task_grpc, serve_executor_task_grpc_with_listener,
};
