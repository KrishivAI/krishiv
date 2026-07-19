#![forbid(unsafe_code)]

//! Public facade for `krishiv-executor`.
//!
//! Executor runtime: receives task assignments, executes DataFusion queries,
//! and reports results back to the job coordinator.

// Root-level modules containing domain implementations.
pub mod aligned_join;
pub mod assignment_inbox;
pub mod barrier;
pub mod barrier_grpc;
pub mod barrier_transport;
pub mod cli;
pub mod error;
pub mod ess_client;
pub mod execution_model;
pub(crate) mod fragment;
pub mod grpc;
pub mod grpc_client;
pub mod runner;
pub mod source_throttle;
pub mod stream_exchange;
pub mod transactions;
pub mod transport;

#[cfg(test)]
mod tests;

/// Type-erase a future behind `Pin<Box<dyn Future + Send>>`.
///
/// Compile-time load-bearing, not a style choice: the executor's deep async
/// call graph (runner → fragment → staging/exchange helpers) otherwise inlines
/// every callee's generator into the caller's, and rustc's trait solver then
/// proves `Send` over one enormous nested type per spawn/boxing point —
/// measured at 95% of a 20h+ compile (`ObligationForest::process_obligations`).
/// Boxing at cross-function await seams makes each proof local and small.
pub(crate) fn erased<'a, T>(
    fut: impl std::future::Future<Output = T> + Send + 'a,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>> {
    Box::pin(fut)
}

// Re-export the public API at the crate root for source compatibility.
pub use assignment_inbox::{AssignmentPushOutcome, ExecutorAssignmentInbox};
pub use error::{ExecutorError, ExecutorResult, ExecutorTransportResult};
pub use execution_model::ExecutionModel;

// Re-exports of barrier, runner, transport, and grpc types.
pub use barrier_grpc::{ExecutorBarrierService, executor_barrier_grpc_server};
pub use barrier_transport::{
    BarrierInjector, BarrierSource, SharedBarrierAckRegistry, SharedBarrierInjector,
    SharedKeyGroupRanges, make_checkpoint_barrier,
};
pub use grpc::{
    ExecutorTaskAuthConfig, ExecutorTaskGrpcService, ExecutorTaskInboxService,
    executor_task_grpc_server,
};
pub use runner::{
    CheckpointStateHandle, ContinuousJobDrainer, ExecutorTaskOutput, ExecutorTaskOutputKind,
    ExecutorTaskRunReport, ExecutorTaskRunner, ShuffleContext, TaskRunner,
    set_inline_result_max_bytes_for_tests,
};
pub use source_throttle::SourceThrottleTable;
pub use transactions::{SharedSinkParticipant, TwoPhaseSinkRegistry};
pub use transport::{
    ExecutorConfig, ExecutorRuntime, ExecutorTransportError, GrpcCoordinatorService,
    serve_executor_task_grpc, serve_executor_task_grpc_with_listener,
};
