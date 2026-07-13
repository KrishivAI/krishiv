//! Task runner types: `TaskRunner`, `ExecutorTaskRunner`, `ExecutorTaskRunReport`,
//! `ExecutorTaskOutput`, `ExecutorTaskOutputKind`, `ShuffleContext`, `LocalParquetPartition`.

mod executor_task_runner;
mod partition;
pub(crate) mod result_spool;
mod task_output;
pub(crate) mod task_runner;

#[cfg(test)]
mod runner_tests;

pub use executor_task_runner::{
    ExecutorTaskRunner, RunLoopBarrierContext, SharedContinuousNotify, SharedContinuousOutputs,
    SharedCoordinatorClient, TaskStateBinding,
};
pub use result_spool::{
    INLINE_RESULT_MAX_BYTES_ENV, SpooledTaskResult, set_inline_result_max_bytes_for_tests,
};
pub use task_output::{
    CheckpointStateHandle, ExecutorTaskOutput, ExecutorTaskOutputKind, ExecutorTaskRunReport,
    RestoredJobCheckpoint, RestoredSourceOffset, ShuffleContext, kafka_offsets_from_source_records,
    restored_source_offsets_from_records,
};
pub use task_runner::{
    ContinuousJobDrainer, SharedProgressCallback, StreamingProgressCallback,
    StreamingProgressSnapshot, TaskRunner,
};

pub(crate) use partition::{
    CONNECTOR_PARQUET_PARTITION_PREFIX, LocalParquetPartition, OBJECT_PARQUET_PARTITION_PREFIX,
    OBJECT_PARQUET_SINK_PREFIX, REGISTRY_CONNECTOR_PARTITION_PREFIX, SHUFFLE_WRITE_PREFIX,
    encode_missing_shuffle, parse_local_parquet_partitions,
};
#[cfg(feature = "kafka")]
pub(crate) use partition::{
    KAFKA_TO_PARQUET_FRAGMENT, MEMORY_KAFKA_PARTITION_PREFIX, PARQUET_SINK_PREFIX,
};
