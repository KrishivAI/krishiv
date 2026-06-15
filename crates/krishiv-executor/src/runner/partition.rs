use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use krishiv_proto::{InputPartition, InputPartitionDescriptor, MissingShufflePartition, StageId};

use crate::{ExecutorError, ExecutorResult};

/// Default batch task timeout in seconds (1 hour). Applied when the job spec
/// does not explicitly set `task_timeout_secs`. Prevents hung tasks from
/// blocking the stage indefinitely.
pub(crate) const DEFAULT_BATCH_TASK_TIMEOUT_SECS: u64 = 3600;

/// Default streaming task safety timeout in seconds (5 minutes).
/// Streaming fragments run continuously, but a deadlocked window operator
/// would block forever without this guard (R6). Operators that legitimately
/// need longer windows should set `task_timeout_secs` explicitly.
pub(crate) const DEFAULT_STREAMING_TASK_TIMEOUT_SECS: u64 = 300;
pub(crate) const MAX_CHECKPOINT_ACK_RETRIES: u8 = 3;

/// Maximum bytes used in the failure message sent to the coordinator.  Larger
/// messages are truncated with `…` so they cannot blow past gRPC payload limits.
pub(crate) const TASK_FAILURE_MESSAGE_MAX_BYTES: usize = 4096;

/// Format an executor-side failure into a coordinator-visible message that
/// includes the fragment description and the underlying error text.  Truncates
/// at [`TASK_FAILURE_MESSAGE_MAX_BYTES`] so we cannot ship arbitrarily large
/// strings through `task_status` RPCs.
pub(crate) fn format_failure_message(fragment: &str, error: &str) -> String {
    let mut buf = String::with_capacity(fragment.len() + error.len() + 32);
    buf.push_str("executor failed fragment '");
    buf.push_str(fragment.trim());
    buf.push_str("': ");
    buf.push_str(error.trim());
    if buf.len() > TASK_FAILURE_MESSAGE_MAX_BYTES {
        let mut end = TASK_FAILURE_MESSAGE_MAX_BYTES.saturating_sub(1);
        while !buf.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        buf.truncate(end);
        buf.push('…');
    }
    buf
}

/// Extract missing shuffle partition references from a task execution error.
///
/// When `read_shuffle_flight_partitions` encounters a `NotFound` gRPC status on an
/// Arrow Flight call, it returns `ExecutorError::ShufflePartitionMissing`.  This
/// helper converts that into the wire-level `MissingShufflePartition` list so the
/// coordinator can re-schedule the producing task.
pub(crate) fn collect_missing_shuffle_partitions(error: &ExecutorError) -> Vec<MissingShufflePartition> {
    match error {
        ExecutorError::ShufflePartitionMissing {
            stage_id,
            partition_id,
            ..
        } => {
            if let Ok(sid) = StageId::try_new(stage_id.clone()) {
                vec![MissingShufflePartition::new(sid, *partition_id)]
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

pub(crate) const LOCAL_PARQUET_PARTITION_PREFIX: &str = "local-parquet:";
pub(crate) const CONNECTOR_PARQUET_PARTITION_PREFIX: &str = "connector-parquet:";
pub(crate) const OBJECT_PARQUET_PARTITION_PREFIX: &str = "object-parquet:";
pub(crate) const OBJECT_PARQUET_SINK_PREFIX: &str = "object-parquet-sink:";
#[cfg(feature = "kafka")]
pub(crate) const MEMORY_KAFKA_PARTITION_PREFIX: &str = "memory-kafka:";
#[cfg(feature = "kafka")]
pub(crate) const PARQUET_SINK_PREFIX: &str = "parquet-sink:";
#[cfg(feature = "kafka")]
pub(crate) const KAFKA_TO_PARQUET_FRAGMENT: &str = "connector-pipeline:kafka-to-parquet";
pub(crate) const SHUFFLE_WRITE_PREFIX: &str = "shuffle-write:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalParquetPartition {
    pub(crate) table_name: String,
    pub(crate) path: PathBuf,
}

impl LocalParquetPartition {
    pub(crate) fn parse(partition: &krishiv_proto::InputPartition) -> ExecutorResult<Option<Self>> {
        let (table_name, path) = match partition.descriptor() {
            Some(InputPartitionDescriptor::LocalParquet { table_name, path }) => {
                (table_name.as_str(), path.as_str())
            }
            Some(_) => return Ok(None),
            None => {
                let descriptor = partition.description().trim();
                let Some(payload) = descriptor.strip_prefix(LOCAL_PARQUET_PARTITION_PREFIX) else {
                    return Ok(None);
                };
                payload
                    .split_once(':')
                    .ok_or_else(|| ExecutorError::InvalidAssignment {
                        message: format!(
                            "input partition {} must use local-parquet:<table>:<path>",
                            partition.partition_id()
                        ),
                    })?
            }
        };
        let table_name = table_name.trim();
        let path = path.trim();
        if table_name.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} has an empty local Parquet table name",
                    partition.partition_id()
                ),
            });
        }
        if path.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} has an empty local Parquet path",
                    partition.partition_id()
                ),
            });
        }

        Ok(Some(Self {
            table_name: table_name.to_owned(),
            path: PathBuf::from(path),
        }))
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

pub(crate) fn parse_local_parquet_partitions(
    partitions: &[InputPartition],
) -> crate::ExecutorResult<Vec<LocalParquetPartition>> {
    let mut table_names = BTreeSet::new();
    let mut parsed = Vec::new();
    for partition in partitions {
        let Some(local_partition) = LocalParquetPartition::parse(partition)? else {
            continue;
        };
        if !table_names.insert(local_partition.table_name().to_owned()) {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "duplicate local Parquet table name {} in assigned input partitions",
                    local_partition.table_name()
                ),
            });
        }
        parsed.push(local_partition);
    }
    Ok(parsed)
}
