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
/// need longer windows should set `task_timeout_secs` explicitly in the task
/// spec, or override the cluster-wide default via the
/// `KRISHIV_STREAMING_TASK_TIMEOUT_SECS` environment variable.
pub(crate) const DEFAULT_STREAMING_TASK_TIMEOUT_SECS: u64 = 300;

/// Return the effective streaming task timeout, checking
/// `KRISHIV_STREAMING_TASK_TIMEOUT_SECS` before the compiled-in default.
/// Per-task `task_timeout_secs` still takes precedence over both.
pub(crate) fn default_streaming_task_timeout_secs() -> u64 {
    std::env::var("KRISHIV_STREAMING_TASK_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_STREAMING_TASK_TIMEOUT_SECS)
}

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

/// Sentinel embedded in a dfplan shuffle-read error string when the upstream
/// producer's partition is unfetchable (executor gone).
///
/// The dfplan `ShufflePartitionReader` trait is `Result<_, String>`-typed, so —
/// unlike the legacy `shuffle-write:` path which returns a structured
/// [`ExecutorError::ShufflePartitionMissing`] — a missing partition can only be
/// signalled by embedding this marker in the error text. It survives the
/// DataFusion → stream → `ExecutorError` wrapping (each layer does `"...: {e}"`)
/// so [`collect_missing_shuffle_partitions`] can recover it downstream.
// Deliberately does NOT start with the `KRISHIV_` prefix: the env-flag registry
// test scans sources for `KRISHIV_*` literals and would otherwise mistake this
// protocol marker for an undeclared environment flag.
pub(crate) const MISSING_SHUFFLE_MARKER: &str = "KRV_SHUFFLE_MISSING";

/// Encode the missing-partition marker for a dfplan shuffle-read failure.
/// `stage_id` is the `shuffle_stage_key` (`sN.mM`) form, which contains no `,`
/// or `)` so the payload parses unambiguously.
pub(crate) fn encode_missing_shuffle(stage_id: &str, partition_id: u32) -> String {
    format!("{MISSING_SHUFFLE_MARKER}(stage={stage_id},partition={partition_id})")
}

/// Recover `(stage_id, partition_id)` from a marker embedded anywhere in `text`.
fn parse_missing_shuffle_marker(text: &str) -> Option<(String, u32)> {
    let start = text.find(MISSING_SHUFFLE_MARKER)?;
    let after = &text[start + MISSING_SHUFFLE_MARKER.len()..];
    let inner = after.strip_prefix('(')?;
    let end = inner.find(')')?;
    let body = &inner[..end]; // "stage=sN.mM,partition=P"
    let (stage_part, partition_part) = body.split_once(",partition=")?;
    let stage_id = stage_part.strip_prefix("stage=")?;
    let partition_id = partition_part.parse::<u32>().ok()?;
    Some((stage_id.to_owned(), partition_id))
}

/// Extract missing shuffle partition references from a task execution error.
///
/// Two shuffle-read paths report a lost producer:
///  * the legacy `shuffle-write:` path returns a structured
///    [`ExecutorError::ShufflePartitionMissing`], and
///  * the dfplan (Phase 52 staged-batch) path, whose `Result<_, String>`
///    reader can only embed the [`MISSING_SHUFFLE_MARKER`] in the error text.
///
/// Either way this converts it into the wire-level `MissingShufflePartition`
/// list so the coordinator can re-schedule the producing task.
pub(crate) fn collect_missing_shuffle_partitions(
    error: &ExecutorError,
) -> Vec<MissingShufflePartition> {
    if let ExecutorError::ShufflePartitionMissing {
        stage_id,
        partition_id,
        ..
    } = error
    {
        if let Ok(sid) = StageId::try_new(stage_id.clone()) {
            return vec![MissingShufflePartition::new(sid, *partition_id)];
        }
        return Vec::new();
    }
    // dfplan path: the marker rides inside the stringified error.
    if let Some((stage_id, partition_id)) = parse_missing_shuffle_marker(&error.to_string())
        && let Ok(sid) = StageId::try_new(stage_id)
    {
        return vec![MissingShufflePartition::new(sid, partition_id)];
    }
    Vec::new()
}

pub(crate) const LOCAL_PARQUET_PARTITION_PREFIX: &str = "local-parquet:";
pub(crate) const CONNECTOR_PARQUET_PARTITION_PREFIX: &str = "connector-parquet:";
pub(crate) const OBJECT_PARQUET_PARTITION_PREFIX: &str = "object-parquet:";
pub(crate) const OBJECT_PARQUET_SINK_PREFIX: &str = "object-parquet-sink:";
/// Batch-export sink contract dispatched through the connector registry
/// (#197 / Phase 67 export leg). Format:
/// `registry-sink:<kind>|<base64(config-json)>` where config-json is
/// `{"name": "...", "properties": {"k": "v", …}}`. The executor opens the
/// registered sink driver for `<kind>` and streams the task's result batches
/// into it — one generic path for every registry sink (s3-files, jdbc-sink,
/// elasticsearch, …), availability decided by which drivers are registered.
pub(crate) const REGISTRY_SINK_PREFIX: &str = "registry-sink:";
#[cfg(feature = "kafka")]
pub(crate) const MEMORY_KAFKA_PARTITION_PREFIX: &str = "memory-kafka:";
#[cfg(feature = "kafka")]
pub(crate) const PARQUET_SINK_PREFIX: &str = "parquet-sink:";
#[cfg(feature = "kafka")]
pub(crate) const KAFKA_TO_PARQUET_FRAGMENT: &str = "connector-pipeline:kafka-to-parquet";
pub(crate) const SHUFFLE_WRITE_PREFIX: &str = "shuffle-write:";
/// Registry-driven source partition prefix: `registry-connector:<kind>:<table>:<config_json>`.
pub(crate) const REGISTRY_CONNECTOR_PARTITION_PREFIX: &str = "registry-connector:";

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

#[cfg(test)]
mod missing_shuffle_tests {
    use super::*;

    #[test]
    fn structured_shuffle_partition_missing_is_collected() {
        let err = ExecutorError::ShufflePartitionMissing {
            stage_id: "s0.m2".to_owned(),
            partition_id: 3,
            message: "gone".to_owned(),
        };
        let got = collect_missing_shuffle_partitions(&err);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].stage_id().as_str(), "s0.m2");
        assert_eq!(got[0].partition_id(), 3);
    }

    #[test]
    fn dfplan_marker_survives_wrapping_and_is_collected() {
        // Mirror the real wrapping: read_partition embeds the marker, the
        // ShuffleReadExec wraps it in a DataFusionError string, and the task
        // runner surfaces it as a LocalExecution error. The marker must still
        // be recoverable from the fully-wrapped `to_string()`.
        let marker = encode_missing_shuffle("s1.m4", 7);
        let wrapped = ExecutorError::LocalExecution {
            message: format!(
                "shuffle read (stage 1, map 4, partition 7): {marker}: \
                 shuffle partition job/s1.m4/7 unreachable after 5 attempts \
                 (producer executor gone): connection refused"
            ),
        };
        let got = collect_missing_shuffle_partitions(&wrapped);
        assert_eq!(got.len(), 1, "marker must be recovered from wrapped error");
        assert_eq!(got[0].stage_id().as_str(), "s1.m4");
        assert_eq!(got[0].partition_id(), 7);
    }

    #[test]
    fn ordinary_error_yields_no_missing_partitions() {
        let err = ExecutorError::LocalExecution {
            message: "dfplan shuffle-flight fetch failed (endpoint=... ): decode error".to_owned(),
        };
        assert!(collect_missing_shuffle_partitions(&err).is_empty());
    }

    #[test]
    fn encode_parse_round_trips() {
        let s = encode_missing_shuffle("s10.m0", 42);
        let (stage, part) = parse_missing_shuffle_marker(&s).expect("parses");
        assert_eq!(stage, "s10.m0");
        assert_eq!(part, 42);
    }
}
