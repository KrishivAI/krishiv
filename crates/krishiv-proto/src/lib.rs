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
    AbortCheckpointRequest, CheckpointAckRequest, CheckpointAckResponse,
    CheckpointInitiateResponse, CheckpointSourceOffset, InitiateCheckpointRequest,
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
pub use io::{ConnectorCapabilityFlags, ShuffleReadConfig, ShuffleWriteConfig, TaskSpec};
pub use job::{JobSpec, StageSpec};
pub use lifecycle::{CoordinatorState, ExecutorState, JobKind, JobState, StageState, TaskState};
pub use management::CoordinatorManagementService;
pub use management::{
    CheckpointEpochInfo, InspectStateRequest, InspectStateResponse, ListCheckpointsRequest,
    ListCheckpointsResponse, RestoreJobRequest, RestoreJobResponse, StateSnapshotInfo,
    TriggerSavepointRequest, TriggerSavepointResponse,
};
pub use services::{CoordinatorExecutorService, ExecutorTaskService};
pub use task::{
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorTaskAssignment,
    InitiateCheckpointCommand, InputPartition, InputPartitionDescriptor, KeyGroupRange,
    MemoryKafkaRecord, MissingShufflePartition, OutputContract, OutputContractDescriptor,
    OutputContractKind, PlanFragment, RegisterExecutorRequest, RegisterExecutorResponse,
    TaskAssignment, TaskAttemptRef, TaskCancellationRequest, TaskStatusRequest, TaskStatusResponse,
    TaskStatusUpdate, TransportDisposition,
};
