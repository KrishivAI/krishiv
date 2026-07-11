#![forbid(unsafe_code)]

//! Public facade for `krishiv-proto`.

macro_rules! impl_with_version {
    ($name:ty) => {
        impl $name {
            /// Override transport version.
            #[must_use]
            pub fn with_version(mut self, version: $crate::TransportVersion) -> Self {
                self.version = version;
                self
            }
        }
    };
}

pub mod checkpoint;
pub mod executor;
pub mod ids;
pub mod io;
pub mod job;
pub mod lifecycle;
pub mod management;
pub mod services;
pub mod task;
pub mod wire;

#[cfg(test)]
mod tests;

pub use checkpoint::{
    CheckpointAckRequest, CheckpointAckResponse, CheckpointAlignment, CheckpointSourceOffset,
    InitiateCheckpointRequest, SinkTransactionRef, UnalignedBufferRef,
};
pub use executor::{
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor, ExecutorHeartbeat,
    HeartbeatHotKeyReport, HeartbeatThrottleCommand, LlmQuotaReport, LlmThrottleCommand,
    ShufflePartitionOutput, StreamingProgressReport, StreamingTaskState, TaskOutputMetadata,
    TaskRuntimeStats, TraceContext,
};
pub use ids::{
    AttemptId, CoordinatorId, ExecutorId, FencingToken, IdError, JobId, LeaseGeneration,
    OperatorId, PartitionId, ProtoResult, StageId, TaskId, TransportVersion,
};
pub use io::{
    ConnectorCapabilityFlags, ResourceProfile, ShuffleReadConfig, ShuffleWriteConfig, TaskSpec,
};
pub use job::{JobSpec, OutputBufferPolicy, StageKind, StageSpec, StreamingExecutionProfile};
pub use lifecycle::{CoordinatorState, ExecutorState, JobKind, JobState, StageState, TaskState};
pub use management::CoordinatorManagementService;
pub use management::{
    CheckpointEpochInfo, InspectStateRequest, InspectStateResponse, ListCheckpointsRequest,
    ListCheckpointsResponse, RestoreJobRequest, RestoreJobResponse, StateSnapshotInfo,
    TriggerSavepointRequest, TriggerSavepointResponse,
};
pub use services::{CoordinatorExecutorService, ExecutorTaskService};

/// Maximum gRPC message size (bytes) for the coordinator↔executor task
/// transport, applied as both the encode and decode limit on every generated
/// client and server in that path.
///
/// Tonic's default decode limit is 4 MiB — far below a batch task's collected
/// result. A distributed join over a real Iceberg table (a 10M-row NYC-taxi
/// join ships ~350 MB back as one message) otherwise fails the task with
/// `decoded message length too large: found N bytes, the limit is 4194304`.
/// Override with `KRISHIV_GRPC_MAX_MESSAGE_BYTES`; defaults to 1 GiB.
pub fn max_grpc_message_bytes() -> usize {
    const DEFAULT: usize = 1024 * 1024 * 1024; // 1 GiB
    std::env::var("KRISHIV_GRPC_MAX_MESSAGE_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT)
}
pub use task::{
    CheckpointCompleteCommand, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
    ExecutorTaskAssignment, ICEBERG_SINK_PREFIX, IcebergSinkMode, InitiateCheckpointCommand,
    InputPartition, InputPartitionDescriptor, KeyGroupRange, MemoryKafkaRecord,
    MissingShufflePartition, OutputContract, OutputContractDescriptor, OutputContractKind,
    PlanFragment, PushTaskResultResponse, RegisterExecutorRequest, RegisterExecutorResponse,
    RestoreFromCheckpointCommand, TaskAssignment, TaskAttemptRef, TaskCancellationRequest,
    TaskResultChunk, TaskStatusRequest, TaskStatusResponse, TaskStatusUpdate, TransportDisposition,
};
