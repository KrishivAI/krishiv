//! Shared helper functions used by multiple fragment execution modules.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use krishiv_proto::{
    InputPartitionDescriptor, OutputContract, OutputContractDescriptor, TaskState,
    TransportDisposition,
};

use crate::runner::{
    CONNECTOR_PARQUET_PARTITION_PREFIX, LocalParquetPartition, OBJECT_PARQUET_PARTITION_PREFIX,
    OBJECT_PARQUET_SINK_PREFIX,
};
use crate::{ExecutorError, ExecutorResult};

/// Recognised SQL fragment prefixes.
pub(crate) const SQL_FRAGMENT_PREFIX: &str = "sql:";

/// Extract the SQL query from a `sql:<query>` fragment string.
///
/// The prefix must appear at the **start** of the fragment. Earlier versions
/// used `split_once("sql:")` which mis-parsed SQL whose body contained the
/// literal substring `sql:` (e.g. a string literal `'sql:abc'`); the prefix
/// is now anchored to position 0 to make routing deterministic.
pub(crate) fn sql_query_from_fragment(fragment: &str) -> Option<&str> {
    let rest = fragment.strip_prefix(SQL_FRAGMENT_PREFIX)?;
    let query = rest.trim();
    (!query.is_empty()).then_some(query)
}

pub(crate) fn task_fragment_body(fragment: &str) -> ExecutorResult<String> {
    krishiv_plan::task_body_for_profile(fragment, krishiv_common::resolve_durability_profile())
        .map_err(|error| ExecutorError::InvalidAssignment {
            message: error.to_string(),
        })
}

pub(crate) fn ensure_status_accepted_or_duplicate(
    disposition: TransportDisposition,
    state: TaskState,
) -> Result<(), tonic::Status> {
    match disposition {
        TransportDisposition::Accepted | TransportDisposition::Duplicate => Ok(()),
        _ => Err(tonic::Status::failed_precondition(format!(
            "coordinator returned {disposition} for {state} status"
        ))),
    }
}

pub(crate) fn parse_local_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<LocalParquetPartition>> {
    crate::runner::parse_local_parquet_partitions(partitions)
}

pub(crate) fn parse_object_parquet_descriptor(
    partition_id: &str,
    payload: &str,
    expected: &str,
) -> ExecutorResult<(String, PathBuf, String)> {
    let parts: Vec<&str> = payload.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(ExecutorError::InvalidAssignment {
            message: format!("input partition {partition_id} must use {expected}"),
        });
    }
    let table_name = parts[0].trim();
    let base_dir = parts[1].trim();
    let object_path = parts[2].trim();
    if table_name.is_empty() || base_dir.is_empty() || object_path.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: format!("input partition {partition_id} has an empty object-parquet field"),
        });
    }
    Ok((
        table_name.to_owned(),
        PathBuf::from(base_dir),
        object_path.to_owned(),
    ))
}

/// Read all batches from `connector-parquet:<path>` input partitions via `ParquetSource`.
///
/// Returns a list of `(table_name, batches)` pairs — one per `connector-parquet:` partition.
/// The table name is derived from the path's filename stem (without extension).
/// Partitions that do not start with the `connector-parquet:` prefix are skipped.
pub(crate) async fn read_connector_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use krishiv_connectors::{Source, parquet::ParquetSource};

    let mut result = Vec::new();
    for partition in partitions {
        let (path_str, explicit_table_name) = match partition.descriptor() {
            Some(InputPartitionDescriptor::ConnectorParquet { table_name, path }) => {
                (path.as_str(), table_name.as_deref())
            }
            Some(_) => continue,
            None => {
                let desc = partition.description().trim();
                match desc.strip_prefix(CONNECTOR_PARQUET_PARTITION_PREFIX) {
                    Some(p) => (p.trim(), None),
                    None => continue,
                }
            }
        };
        if path_str.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} has an empty path in connector-parquet descriptor",
                    partition.partition_id()
                ),
            });
        }
        let path = std::path::Path::new(path_str);
        // Derive a table name from the filename stem unless the typed descriptor supplied one.
        let table_name = explicit_table_name
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("connector_table")
                    .to_owned()
            });

        let mut source = ParquetSource::open(path).map_err(|e| ExecutorError::LocalExecution {
            message: format!("connector-parquet open failed for '{path_str}': {e}"),
        })?;
        let mut batches = Vec::new();
        while let Some(batch) =
            source
                .read_batch()
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("connector-parquet read failed: {e}"),
                })?
        {
            batches.push(batch);
        }
        result.push((table_name, batches));
    }
    Ok(result)
}

/// Read all batches from `object-parquet:<table>:<base_dir>:<object_path>` partitions.
///
/// This is the deterministic S3-compatible executor path for R3: tests use
/// `object_store::local::LocalFileSystem`, while production object-store
/// credentials and provider-specific URLs remain behind the connector boundary.
pub(crate) async fn read_object_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use std::sync::Arc;

    use krishiv_connectors::{Source, s3::S3Source};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;

    let mut result = Vec::new();
    for partition in partitions {
        let (table_name, base_dir, object_path) = match partition.descriptor() {
            Some(InputPartitionDescriptor::ObjectParquet {
                table_name,
                base_dir,
                object_path,
            }) => (
                table_name.clone(),
                PathBuf::from(base_dir),
                object_path.clone(),
            ),
            Some(_) => continue,
            None => {
                let desc = partition.description().trim();
                let Some(payload) = desc.strip_prefix(OBJECT_PARQUET_PARTITION_PREFIX) else {
                    continue;
                };
                parse_object_parquet_descriptor(
                    partition.partition_id(),
                    payload,
                    "object-parquet:<table>:<base_dir>:<object_path>",
                )?
            }
        };
        let store = Arc::new(
            LocalFileSystem::new_with_prefix(&base_dir).map_err(|error| {
                ExecutorError::LocalExecution {
                    message: format!(
                        "failed to open object store prefix '{}': {error}",
                        base_dir.display()
                    ),
                }
            })?,
        );
        let mut source = S3Source::open(store, ObjectPath::from(object_path.clone()))
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("object-parquet open failed for '{object_path}': {error}"),
            })?;
        let mut batches = Vec::new();
        while let Some(batch) =
            source
                .read_batch()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("object-parquet read failed: {error}"),
                })?
        {
            batches.push(batch);
        }
        result.push((table_name, batches));
    }
    Ok(result)
}

/// Parse the sink contract on `contract` into a [`SinkWriteSpec`].
///
/// Accepts both the typed `ObjectParquetSink` descriptor (always legacy
/// direct-write semantics) and the string payload form, which may carry the
/// Phase 2.3 staged-commit tokens (`mode=`, `partition_by=`).
pub(crate) fn parse_object_parquet_sink_spec(
    contract: &OutputContract,
) -> ExecutorResult<krishiv_common::write_commit::SinkWriteSpec> {
    use krishiv_common::write_commit::SinkWriteSpec;

    let spec = match contract.descriptor() {
        Some(OutputContractDescriptor::ObjectParquetSink {
            base_dir,
            object_path,
        }) => SinkWriteSpec::parse(&format!("{}:{}", base_dir.trim(), object_path.trim())),
        _ => {
            let payload = contract
                .description()
                .trim()
                .strip_prefix(OBJECT_PARQUET_SINK_PREFIX)
                .ok_or_else(|| ExecutorError::InvalidAssignment {
                    message: format!(
                        "object sink must use {OBJECT_PARQUET_SINK_PREFIX}<base_dir>:<dest>\
                         [:mode=<m>][:partition_by=<cols>]"
                    ),
                })?;
            SinkWriteSpec::parse(payload)
        }
    };
    spec.map_err(|error| ExecutorError::InvalidAssignment {
        message: error.to_string(),
    })
}

/// Write `batches` as a single Parquet object at `object_path` under the
/// `base_dir` object-store prefix. Overwrites any existing object at the same
/// path (idempotent re-run of the same task attempt).
async fn write_parquet_object(
    base_dir: &str,
    object_path: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> ExecutorResult<()> {
    use std::sync::Arc;

    use krishiv_connectors::{Sink, s3::S3Sink};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;

    let store = Arc::new(LocalFileSystem::new_with_prefix(base_dir).map_err(|error| {
        ExecutorError::LocalExecution {
            message: format!("failed to open object store prefix '{base_dir}': {error}"),
        }
    })?);
    let mut sink = S3Sink::new(store, ObjectPath::from(object_path));
    for batch in batches {
        sink.write_batch(batch.clone())
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("object-parquet sink write failed: {error}"),
            })?;
    }
    sink.flush()
        .await
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("object-parquet sink flush failed: {error}"),
        })
}

/// Legacy direct object-parquet sink write (no staging, no commit protocol).
pub(crate) async fn write_object_parquet_sink(
    contract: &OutputContract,
    batches: &[arrow::record_batch::RecordBatch],
) -> ExecutorResult<()> {
    let spec = parse_object_parquet_sink_spec(contract)?;
    write_parquet_object(&spec.base_dir, &spec.dest_path, batches).await
}

/// Execute an object-parquet sink write for a task, dispatching between the
/// legacy direct-write path and the Phase 2.3 staged commit protocol.
///
/// Staged contracts (any `mode=` / `partition_by=` token present):
/// - output is split into Hive partition slices when `partition_by` is set;
/// - each slice is written to
///   `<dest>/_staging/<job_id>/[<hive>/]<task_id>-<attempt>.parquet`;
/// - the staged relative paths are returned so they can be reported in
///   [`krishiv_proto::TaskOutputMetadata::sink_staged_files`]. Publication into
///   the destination happens at the job level (coordinator) on job success.
///
/// Legacy contracts write the final object directly and return an empty list.
/// Re-running the same task attempt overwrites its own files in both paths.
pub(crate) async fn write_object_parquet_sink_for_task(
    assignment: &krishiv_proto::ExecutorTaskAssignment,
    batches: &[arrow::record_batch::RecordBatch],
) -> ExecutorResult<Vec<String>> {
    use krishiv_common::write_commit::split_batches_by_partition_columns;

    let spec = parse_object_parquet_sink_spec(assignment.output_contract())?;
    if !spec.staged {
        write_object_parquet_sink(assignment.output_contract(), batches).await?;
        return Ok(Vec::new());
    }

    // Ensure the object-store prefix exists before opening it: the staged
    // destination directory may not have been created yet on this executor.
    std::fs::create_dir_all(&spec.base_dir).map_err(|error| ExecutorError::LocalExecution {
        message: format!(
            "failed to create sink base directory '{}': {error}",
            spec.base_dir
        ),
    })?;

    let slices =
        split_batches_by_partition_columns(batches, &spec.partition_by).map_err(|error| {
            ExecutorError::LocalExecution {
                message: format!("staged sink partition split failed: {error}"),
            }
        })?;

    let job_id = assignment.job_id().as_str();
    let task_id = assignment.task_id().as_str();
    let attempt = assignment.attempt_id().as_u32();

    let mut staged_paths = Vec::new();
    for slice in slices {
        if slice.batches.is_empty() {
            continue;
        }
        let rel = spec.staged_file_rel(job_id, &slice.hive_path, task_id, attempt);
        write_parquet_object(&spec.base_dir, &rel, &slice.batches).await?;
        staged_paths.push(rel);
    }
    Ok(staged_paths)
}

/// Fetch all `shuffle-flight:` input partitions via Arrow IPC over TCP and return
/// `(table_name, batches)` pairs ready for registration with the SQL engine.
///
/// Executor-wide semaphore that caps concurrent Arrow Flight shuffle fetches.
///
/// Default is 8 simultaneous in-flight requests per executor. Configurable via
/// `KRISHIV_SHUFFLE_FETCH_CONCURRENCY`. Shared across all tasks running on the
/// same executor process to prevent thundering-herd on shuffle services.
static SHUFFLE_FETCH_SEMAPHORE: LazyLock<Arc<tokio::sync::Semaphore>> = LazyLock::new(|| {
    let concurrency = std::env::var("KRISHIV_SHUFFLE_FETCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(8);
    Arc::new(tokio::sync::Semaphore::new(concurrency))
});

/// Multiple partitions sharing the same table name are merged so the engine sees
/// one logical table regardless of how many physical shuffle partitions were read.
/// All partitions are fetched concurrently up to `KRISHIV_SHUFFLE_FETCH_CONCURRENCY`
/// (default 8) simultaneous requests; the first error aborts the remaining fetches.
pub(crate) async fn read_shuffle_flight_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use std::collections::BTreeMap;

    use futures::StreamExt as _;
    use futures::stream::FuturesUnordered;
    use krishiv_shuffle::flight::{FetchRetryPolicy, FlightShuffleClient};

    // Transient transport failures are retried with backoff; a missing
    // partition (NotFound) fails immediately so the scheduler can react.
    let retry_policy = FetchRetryPolicy::from_env();

    // Collect the flight-fetch futures for all shuffle-flight partitions.
    let fetches: FuturesUnordered<_> = partitions
        .iter()
        .filter_map(|partition| match partition.descriptor() {
            Some(InputPartitionDescriptor::ShuffleFlight {
                table_name,
                flight_endpoint,
                job_id,
                upstream_stage_id,
                partition_id,
            }) => Some((
                table_name.clone(),
                flight_endpoint.clone(),
                job_id.clone(),
                upstream_stage_id.clone(),
                *partition_id,
            )),
            Some(_) | None => None,
        })
        .map(
            |(table_name, flight_endpoint, job_id, upstream_stage_id, partition_id)| async move {
                // Acquire a concurrency permit before touching the network.
                // The permit is released when it drops at the end of this block.
                let _permit = SHUFFLE_FETCH_SEMAPHORE
                    .acquire()
                    .await
                    .expect("shuffle fetch semaphore closed");
                let batches = FlightShuffleClient::fetch_with_retry(
                    &flight_endpoint,
                    job_id.as_str(),
                    upstream_stage_id.as_str(),
                    partition_id,
                    retry_policy,
                )
                .await
                .map_err(|e| {
                    if e.kind() == io::ErrorKind::NotFound {
                        ExecutorError::ShufflePartitionMissing {
                            stage_id: upstream_stage_id.as_str().to_owned(),
                            partition_id,
                            message: e.to_string(),
                        }
                    } else {
                        ExecutorError::LocalExecution {
                            message: format!(
                                "shuffle-flight fetch failed (endpoint={flight_endpoint} \
                                 job={job_id} stage={upstream_stage_id} partition={partition_id}): {e}"
                            ),
                        }
                    }
                })?;
                Ok::<_, ExecutorError>((table_name, batches))
            },
        )
        .collect();

    // Drive all fetches concurrently and merge results by table name.
    let mut table_batches: BTreeMap<String, Vec<arrow::record_batch::RecordBatch>> =
        BTreeMap::new();
    let mut stream = fetches;
    while let Some(result) = stream.next().await {
        let (table_name, batches) = result?;
        table_batches.entry(table_name).or_default().extend(batches);
    }

    Ok(table_batches.into_iter().collect())
}

/// Translate a task's [`MemoryBudget`] limit into a DataFusion engine memory
/// limit for the per-task `SqlEngine`. Tasks without an explicit limit fall
/// back to the executor-wide `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` default.
pub(crate) fn task_engine_memory_limit(
    memory_budget: &krishiv_common::MemoryBudget,
) -> Option<usize> {
    memory_budget
        .limit()
        .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX))
        .or_else(krishiv_sql::query_memory_limit_from_env)
}

/// Environment variable: whole-process memory budget for this executor,
/// shared across all concurrent task slots.
pub const EXECUTOR_MEMORY_LIMIT_ENV: &str = "KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES";

/// Minimum engine memory granted to a task even when the process budget is
/// exhausted.  Keeps tasks progressing under heavy spill rather than failing;
/// bounds over-commit at `concurrent_slots × 32 MiB`.
const MIN_TASK_ENGINE_MEMORY_BYTES: u64 = 32 * 1024 * 1024;

/// Process-wide memory budget shared by every task slot in this executor.
///
/// `KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES` unset or unparseable → unlimited
/// (per-task limits still apply individually).
static EXECUTOR_PROCESS_BUDGET: std::sync::LazyLock<Arc<krishiv_common::MemoryBudget>> =
    std::sync::LazyLock::new(|| {
        let limit = std::env::var(EXECUTOR_MEMORY_LIMIT_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0);
        krishiv_common::MemoryBudget::from_limit(limit)
    });

/// Process-wide executor memory budget (test and metrics inspection).
#[allow(dead_code)]
pub(crate) fn executor_process_budget() -> &'static Arc<krishiv_common::MemoryBudget> {
    &EXECUTOR_PROCESS_BUDGET
}

/// RAII guard for a task's share of the executor process memory budget.
///
/// Releases the reservation when the task finishes (drop), so concurrent
/// slots see freed capacity immediately.
pub(crate) struct ProcessMemoryReservation {
    bytes: u64,
}

impl Drop for ProcessMemoryReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            EXECUTOR_PROCESS_BUDGET.release(self.bytes);
        }
    }
}

/// Compute a task's effective engine memory limit under the shared
/// executor process budget, reserving the granted amount for the task's
/// lifetime.
///
/// Behaviour:
/// - No process limit configured → the per-task limit (or env default)
///   passes through unchanged with no reservation.
/// - Process limit configured → the task's desired limit (per-task limit,
///   env default, or the full process limit when neither is set) is reserved
///   against the process budget. When the full amount is unavailable the
///   task is granted the remaining capacity instead, and when the budget is
///   fully exhausted a minimum grant of 32 MiB keeps the task progressing
///   with aggressive spilling (bounded over-commit, logged).
///
/// The returned guard must be held for the duration of task execution.
pub(crate) fn reserve_task_engine_memory(
    memory_budget: &krishiv_common::MemoryBudget,
) -> (Option<usize>, Option<ProcessMemoryReservation>) {
    let desired = task_engine_memory_limit(memory_budget);
    let process = &*EXECUTOR_PROCESS_BUDGET;
    let Some(process_limit) = process.limit() else {
        return (desired, None);
    };

    let want = desired
        .map(|d| u64::try_from(d).unwrap_or(u64::MAX))
        .unwrap_or(process_limit)
        .min(process_limit);

    if process.try_reserve(want) {
        let granted = usize::try_from(want).unwrap_or(usize::MAX);
        return (
            Some(granted),
            Some(ProcessMemoryReservation { bytes: want }),
        );
    }

    // Full amount unavailable: take whatever remains, if meaningful.
    let remaining = process.remaining().unwrap_or(0);
    if remaining >= MIN_TASK_ENGINE_MEMORY_BYTES && process.try_reserve(remaining) {
        tracing::warn!(
            requested = want,
            granted = remaining,
            "executor process memory budget under pressure; task granted reduced engine limit"
        );
        let granted = usize::try_from(remaining).unwrap_or(usize::MAX);
        return (
            Some(granted),
            Some(ProcessMemoryReservation { bytes: remaining }),
        );
    }

    // Budget exhausted: minimum grant without reservation (bounded over-commit)
    // so the task spills aggressively instead of failing outright.
    tracing::warn!(
        requested = want,
        granted = MIN_TASK_ENGINE_MEMORY_BYTES,
        "executor process memory budget exhausted; task granted minimum engine limit"
    );
    (
        Some(usize::try_from(MIN_TASK_ENGINE_MEMORY_BYTES).unwrap_or(usize::MAX)),
        None,
    )
}

/// Extract the upstream watermark hint from task input partitions if present.
/// Returns the highest watermark_ms value found (in case multiple hints exist).
pub(crate) fn read_watermark_hint(partitions: &[krishiv_proto::InputPartition]) -> Option<i64> {
    partitions
        .iter()
        .filter_map(|p| {
            if let Some(InputPartitionDescriptor::WatermarkHint { watermark_ms }) = p.descriptor() {
                Some(*watermark_ms)
            } else {
                None
            }
        })
        .max()
}

pub(crate) fn read_inline_ipc_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use arrow::ipc::reader::StreamReader;

    let mut result = Vec::new();
    for partition in partitions {
        // Handle zero-copy InMemory partitions directly — no IPC decode needed.
        if let Some(InputPartitionDescriptor::InMemory {
            table_name,
            batches,
        }) = partition.descriptor()
        {
            let owned: Vec<_> = batches.iter().map(|b| (**b).clone()).collect();
            result.push((table_name.clone(), owned));
            continue;
        }

        let (table_name, ipc_bytes) = match partition.descriptor() {
            Some(InputPartitionDescriptor::InlineIpc {
                table_name,
                ipc_bytes,
            }) => (table_name.clone(), ipc_bytes.clone()),
            Some(_) | None => continue,
        };
        if ipc_bytes.is_empty() {
            result.push((table_name, vec![]));
            continue;
        }
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None).map_err(|e| {
            ExecutorError::InvalidAssignment {
                message: format!(
                    "inline-ipc decode failed for partition '{}': {e}",
                    partition.partition_id()
                ),
            }
        })?;
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
            ExecutorError::InvalidAssignment {
                message: format!(
                    "inline-ipc read failed for partition '{}': {e}",
                    partition.partition_id()
                ),
            }
        })?;
        result.push((table_name, batches));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use krishiv_common::MemoryBudget;

    #[test]
    fn task_engine_memory_limit_uses_explicit_task_budget() {
        let budget = MemoryBudget::limited(256 * 1024 * 1024);
        assert_eq!(
            super::task_engine_memory_limit(&budget),
            Some(256 * 1024 * 1024)
        );
    }

    #[test]
    fn task_engine_memory_limit_unlimited_budget_falls_back_to_env_default() {
        // The env var is not set in unit tests, so an unlimited budget yields
        // an unbounded engine.
        let budget = MemoryBudget::unlimited();
        assert_eq!(
            super::task_engine_memory_limit(&budget),
            krishiv_sql::query_memory_limit_from_env()
        );
    }

    #[test]
    fn reserve_task_engine_memory_passthrough_without_process_limit() {
        // KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES is not set in unit tests, so the
        // process budget is unlimited: the per-task limit passes through and
        // no reservation guard is created.
        let budget = MemoryBudget::limited(64 * 1024 * 1024);
        let (limit, guard) = super::reserve_task_engine_memory(&budget);
        assert_eq!(limit, Some(64 * 1024 * 1024));
        assert!(guard.is_none());
    }

    #[test]
    fn process_memory_reservation_releases_on_drop() {
        // Exercise the guard against a locally constructed budget through the
        // same release path the global budget uses.
        let process = MemoryBudget::limited(100);
        assert!(process.try_reserve(80));
        process.release(80);
        assert_eq!(process.used_bytes(), 0);
        // The Drop impl on ProcessMemoryReservation calls release on the
        // global EXECUTOR_PROCESS_BUDGET, which is unlimited in tests; verify
        // dropping a guard with zero bytes is a no-op and does not panic.
        drop(super::ProcessMemoryReservation { bytes: 0 });
    }

    // ── Phase 2.3 distributed write commit protocol ─────────────────────────

    mod write_commit {
        use std::path::Path;
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use krishiv_common::write_commit::{
            SinkWriteSpec, WriteMode, cleanup_staged_outputs, publish_staged_outputs,
            split_batches_by_partition_columns,
        };
        use krishiv_proto::{
            AttemptId, ExecutorId, ExecutorTaskAssignment, JobId, LeaseGeneration, OutputContract,
            OutputContractKind, PlanFragment, StageId, TaskAttemptRef, TaskId,
        };
        use tempfile::tempdir;

        fn stage_file(base: &Path, rel: &str, contents: &str) {
            let path = base.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }

        fn staged_spec(base: &Path, mode: WriteMode) -> SinkWriteSpec {
            SinkWriteSpec::staged(base.to_string_lossy().into_owned(), "out", mode, Vec::new())
                .unwrap()
        }

        fn people_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("country", DataType::Utf8, true),
                Field::new("year", DataType::Int64, true),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(StringArray::from(vec![
                        Some("US"),
                        Some("IN"),
                        Some("US"),
                        None,
                    ])),
                    Arc::new(Int64Array::from(vec![
                        Some(2024),
                        Some(2024),
                        Some(2025),
                        Some(2025),
                    ])),
                ],
            )
            .unwrap()
        }

        #[test]
        fn sink_contract_mode_parsing() {
            assert_eq!(WriteMode::parse("append").unwrap(), WriteMode::Append);
            assert_eq!(WriteMode::parse("Overwrite").unwrap(), WriteMode::Overwrite);
            assert_eq!(
                WriteMode::parse("errorifexists").unwrap(),
                WriteMode::ErrorIfExists
            );
            assert_eq!(WriteMode::parse("ignore").unwrap(), WriteMode::Ignore);
            assert!(WriteMode::parse("replace").is_err());

            // Missing mode token = Append (backwards compatible default).
            let spec = SinkWriteSpec::parse("/base:out:partition_by=c").unwrap();
            assert_eq!(spec.mode, WriteMode::Append);
            assert!(spec.staged);
            // Token-less payload keeps legacy direct-write semantics.
            let legacy = SinkWriteSpec::parse("/base:out/file.parquet").unwrap();
            assert!(!legacy.staged);
        }

        #[test]
        fn staging_path_construction() {
            let spec = SinkWriteSpec::staged("/base", "out", WriteMode::Append, vec![]).unwrap();
            assert_eq!(spec.staging_dir_rel("job-9"), "out/_staging/job-9");
            assert_eq!(
                spec.staged_file_rel("job-9", "", "task-0", 1),
                "out/_staging/job-9/task-0-1.parquet"
            );
            assert_eq!(
                spec.staged_file_rel("job-9", "country=US/year=2024", "task-2", 3),
                "out/_staging/job-9/country=US/year=2024/task-2-3.parquet"
            );
            // Round-trip the contract through the executor-side parser.
            let contract = OutputContract::new(
                OutputContractKind::Sink,
                format!("object-parquet-sink:{}", spec.contract_payload()),
            );
            let parsed = crate::fragment::common::parse_object_parquet_sink_spec(&contract)
                .expect("contract must parse");
            assert_eq!(parsed, spec);
        }

        #[test]
        fn publish_renames_staged_files_into_destination() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-1/task-0-1.parquet", "zero");
            stage_file(base, "out/_staging/job-1/task-1-1.parquet", "one");

            let spec = staged_spec(base, WriteMode::Append);
            let outcome = publish_staged_outputs(&spec, "job-1").unwrap();
            assert_eq!(outcome.published.len(), 2);
            assert_eq!(outcome.skipped_existing, 0);
            assert!(!outcome.ignored);
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-0-job-1.parquet")).unwrap(),
                "zero"
            );
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-1-job-1.parquet")).unwrap(),
                "one"
            );
            assert!(
                !base.join("out/_staging").exists(),
                "staging must be removed"
            );
        }

        #[test]
        fn publish_keeps_only_highest_attempt() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-2/task-0-1.parquet", "attempt-1");
            stage_file(base, "out/_staging/job-2/task-0-2.parquet", "attempt-2");

            let spec = staged_spec(base, WriteMode::Append);
            let outcome = publish_staged_outputs(&spec, "job-2").unwrap();
            assert_eq!(outcome.published.len(), 1);
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-0-job-2.parquet")).unwrap(),
                "attempt-2"
            );
        }

        #[test]
        fn publish_error_if_exists_semantics() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-3/task-0-1.parquet", "data");
            stage_file(base, "out/preexisting.parquet", "foreign");

            let spec = staged_spec(base, WriteMode::ErrorIfExists);
            let error = publish_staged_outputs(&spec, "job-3").unwrap_err();
            assert!(error.to_string().contains("error_if_exists"));
            // Staging is preserved so a retry under a different mode can publish.
            assert!(base.join("out/_staging/job-3/task-0-1.parquet").exists());

            // With no foreign content the same mode publishes normally.
            std::fs::remove_file(base.join("out/preexisting.parquet")).unwrap();
            let outcome = publish_staged_outputs(&spec, "job-3").unwrap();
            assert_eq!(outcome.published.len(), 1);
        }

        #[test]
        fn publish_ignore_semantics() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-4/task-0-1.parquet", "data");
            stage_file(base, "out/preexisting.parquet", "foreign");

            let spec = staged_spec(base, WriteMode::Ignore);
            let outcome = publish_staged_outputs(&spec, "job-4").unwrap();
            assert!(outcome.ignored);
            assert!(outcome.published.is_empty());
            assert!(!base.join("out/part-0-job-4.parquet").exists());
            assert!(!base.join("out/_staging").exists(), "staging cleaned up");
            assert_eq!(
                std::fs::read_to_string(base.join("out/preexisting.parquet")).unwrap(),
                "foreign"
            );
        }

        #[test]
        fn publish_overwrite_semantics() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-5/task-0-1.parquet", "fresh");
            stage_file(base, "out/part-0-old-job.parquet", "stale");
            stage_file(
                base,
                "out/country=US/part-0-old-job.parquet",
                "stale-nested",
            );

            let spec = staged_spec(base, WriteMode::Overwrite);
            let outcome = publish_staged_outputs(&spec, "job-5").unwrap();
            assert_eq!(outcome.published.len(), 1);
            assert!(!base.join("out/part-0-old-job.parquet").exists());
            assert!(!base.join("out/country=US/part-0-old-job.parquet").exists());
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-0-job-5.parquet")).unwrap(),
                "fresh"
            );
        }

        #[test]
        fn publish_is_idempotent_and_converges_after_partial_publish() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-6/task-0-1.parquet", "zero");
            stage_file(base, "out/_staging/job-6/task-1-1.parquet", "one");

            // Simulate a crash after publishing only task-0: its staged file
            // was renamed to the final name and removed from staging.
            std::fs::create_dir_all(base.join("out")).unwrap();
            std::fs::rename(
                base.join("out/_staging/job-6/task-0-1.parquet"),
                base.join("out/part-0-job-6.parquet"),
            )
            .unwrap();

            // Re-publish (ErrorIfExists must not trip over our own part file).
            let spec = staged_spec(base, WriteMode::ErrorIfExists);
            let outcome = publish_staged_outputs(&spec, "job-6").unwrap();
            assert_eq!(outcome.published.len(), 1);
            assert_eq!(outcome.skipped_existing, 0);
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-1-job-6.parquet")).unwrap(),
                "one"
            );

            // Publishing again after completion is a no-op.
            let outcome = publish_staged_outputs(&spec, "job-6").unwrap();
            assert!(outcome.published.is_empty());
            assert_eq!(outcome.skipped_existing, 0);
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-0-job-6.parquet")).unwrap(),
                "zero"
            );
        }

        #[test]
        fn publish_skips_existing_final_files() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            stage_file(base, "out/_staging/job-7/task-0-1.parquet", "staged");
            stage_file(base, "out/part-0-job-7.parquet", "already-published");

            let spec = staged_spec(base, WriteMode::Append);
            let outcome = publish_staged_outputs(&spec, "job-7").unwrap();
            assert!(outcome.published.is_empty());
            assert_eq!(outcome.skipped_existing, 1);
            // The already-published copy wins; the duplicate staged file is dropped.
            assert_eq!(
                std::fs::read_to_string(base.join("out/part-0-job-7.parquet")).unwrap(),
                "already-published"
            );
            assert!(!base.join("out/_staging").exists());
        }

        #[test]
        fn cleanup_tolerates_missing_staging() {
            let temp = tempdir().unwrap();
            let base = temp.path();
            let spec = staged_spec(base, WriteMode::Append);
            // Nothing staged at all: cleanup must succeed.
            cleanup_staged_outputs(&spec, "job-8").unwrap();

            stage_file(base, "out/_staging/job-8/task-0-1.parquet", "data");
            cleanup_staged_outputs(&spec, "job-8").unwrap();
            assert!(!base.join("out/_staging").exists());
            // And again, after removal.
            cleanup_staged_outputs(&spec, "job-8").unwrap();
        }

        #[test]
        fn partitioned_split_multi_column_with_nulls() {
            let batch = people_batch();
            let slices = split_batches_by_partition_columns(
                std::slice::from_ref(&batch),
                &[String::from("country"), String::from("year")],
            )
            .unwrap();
            let paths: Vec<&str> = slices.iter().map(|s| s.hive_path.as_str()).collect();
            assert_eq!(
                paths,
                vec![
                    "country=IN/year=2024",
                    "country=US/year=2024",
                    "country=US/year=2025",
                    "country=__HIVE_DEFAULT_PARTITION__/year=2025",
                ]
            );
            let total_rows: usize = slices
                .iter()
                .flat_map(|s| s.batches.iter())
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(total_rows, batch.num_rows());
            // Partition columns are retained in the data files.
            assert_eq!(slices[0].batches[0].num_columns(), 3);

            // Missing partition column is a hard error.
            let error = split_batches_by_partition_columns(
                std::slice::from_ref(&batch),
                &[String::from("missing")],
            )
            .unwrap_err();
            assert!(error.to_string().contains("missing"));

            // No partition columns: single slice, empty hive path.
            let plain =
                split_batches_by_partition_columns(std::slice::from_ref(&batch), &[]).unwrap();
            assert_eq!(plain.len(), 1);
            assert!(plain[0].hive_path.is_empty());
        }

        fn sink_assignment(contract: String, attempt: u32) -> ExecutorTaskAssignment {
            ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    JobId::try_new("job-sink-1").unwrap(),
                    StageId::try_new("stage-0").unwrap(),
                    TaskId::try_new("task-0").unwrap(),
                    AttemptId::try_new(attempt).unwrap(),
                ),
                ExecutorId::try_new("exec-sink-1").unwrap(),
                LeaseGeneration::initial(),
                PlanFragment::new("sql: select 1"),
                OutputContract::new(OutputContractKind::Sink, contract),
            )
        }

        #[tokio::test]
        async fn staged_sink_task_write_then_publish_round_trip() {
            let temp = tempdir().unwrap();
            let base = temp.path().to_string_lossy().into_owned();
            let spec = SinkWriteSpec::staged(
                base.clone(),
                "out",
                WriteMode::Append,
                vec![String::from("country")],
            )
            .unwrap();
            let contract = format!("object-parquet-sink:{}", spec.contract_payload());

            let batch = people_batch();
            let staged = crate::fragment::common::write_object_parquet_sink_for_task(
                &sink_assignment(contract, 1),
                std::slice::from_ref(&batch),
            )
            .await
            .unwrap();
            assert_eq!(staged.len(), 3, "US, IN, and null-country partitions");
            for rel in &staged {
                assert!(
                    rel.starts_with("out/_staging/job-sink-1/"),
                    "staged path '{rel}' must live under the job staging dir"
                );
                assert!(temp.path().join(rel).is_file());
            }
            // Nothing visible at the destination before publish.
            assert!(!temp.path().join("out/country=US").exists());

            let outcome = publish_staged_outputs(&spec, "job-sink-1").unwrap();
            assert_eq!(outcome.published.len(), 3);
            assert!(!temp.path().join("out/_staging").exists());

            // The published partition file is a readable Parquet object.
            let us_file = temp.path().join("out/country=US/part-0-job-sink-1.parquet");
            let file = std::fs::File::open(&us_file).unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap();
            let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
            assert_eq!(rows, 2, "two US rows");
        }

        #[tokio::test]
        async fn staged_sink_task_rerun_overwrites_its_own_staging_file() {
            let temp = tempdir().unwrap();
            let base = temp.path().to_string_lossy().into_owned();
            let spec =
                SinkWriteSpec::staged(base.clone(), "out", WriteMode::Append, vec![]).unwrap();
            let contract = format!("object-parquet-sink:{}", spec.contract_payload());

            let batch = people_batch();
            let first = crate::fragment::common::write_object_parquet_sink_for_task(
                &sink_assignment(contract.clone(), 1),
                std::slice::from_ref(&batch),
            )
            .await
            .unwrap();
            let second = crate::fragment::common::write_object_parquet_sink_for_task(
                &sink_assignment(contract, 1),
                std::slice::from_ref(&batch),
            )
            .await
            .unwrap();
            assert_eq!(first, second, "same attempt stages the same path");

            let staging = temp.path().join("out/_staging/job-sink-1");
            let entries: Vec<_> = std::fs::read_dir(&staging).unwrap().collect();
            assert_eq!(entries.len(), 1, "re-run must not duplicate staging files");
        }

        #[tokio::test]
        async fn legacy_sink_contract_writes_directly_without_staging() {
            let temp = tempdir().unwrap();
            let base = temp.path().to_string_lossy().into_owned();
            let contract = format!("object-parquet-sink:{base}:out/direct.parquet");

            let batch = people_batch();
            let staged = crate::fragment::common::write_object_parquet_sink_for_task(
                &sink_assignment(contract, 1),
                std::slice::from_ref(&batch),
            )
            .await
            .unwrap();
            assert!(staged.is_empty(), "legacy contracts report no staged files");
            assert!(temp.path().join("out/direct.parquet").is_file());
            assert!(!temp.path().join("out/_staging").exists());
        }
    }
}
