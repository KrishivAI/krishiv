//! Task runner types: `TaskRunner`, `ExecutorTaskRunner`, `ExecutorTaskRunReport`,
//! `ExecutorTaskOutput`, `ExecutorTaskOutputKind`, `ShuffleContext`, `LocalParquetPartition`.

mod executor_task_runner;
mod partition;
mod task_output;
mod task_runner;

#[cfg(test)]
mod runner_tests;

pub use executor_task_runner::ExecutorTaskRunner;
pub use task_output::{
    CheckpointStateHandle, ExecutorTaskOutput, ExecutorTaskOutputKind, ExecutorTaskRunReport,
    RestoredJobCheckpoint, ShuffleContext, kafka_offsets_from_source_records,
};
pub use task_runner::{
    ContinuousJobDrainer, SharedProgressCallback, StreamingProgressCallback,
    StreamingProgressSnapshot, TaskRunner,
};

pub(crate) use partition::{
    CONNECTOR_PARQUET_PARTITION_PREFIX, DEFAULT_BATCH_TASK_TIMEOUT_SECS,
    DEFAULT_STREAMING_TASK_TIMEOUT_SECS, LOCAL_PARQUET_PARTITION_PREFIX,
    OBJECT_PARQUET_PARTITION_PREFIX, OBJECT_PARQUET_SINK_PREFIX, SHUFFLE_WRITE_PREFIX,
    TASK_FAILURE_MESSAGE_MAX_BYTES, LocalParquetPartition, format_failure_message,
    parse_local_parquet_partitions,
};
#[cfg(feature = "kafka")]
pub(crate) use partition::{
    KAFKA_TO_PARQUET_FRAGMENT, MEMORY_KAFKA_PARTITION_PREFIX, PARQUET_SINK_PREFIX,
};

pub(crate) use task_output::apply_snapshots_to_state;
