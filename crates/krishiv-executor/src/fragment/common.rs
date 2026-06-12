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

pub(crate) async fn write_object_parquet_sink(
    contract: &OutputContract,
    batches: &[arrow::record_batch::RecordBatch],
) -> ExecutorResult<()> {
    use std::sync::Arc;

    use krishiv_connectors::{Sink, s3::S3Sink};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;

    let (base_dir, object_path) = match contract.descriptor() {
        Some(OutputContractDescriptor::ObjectParquetSink {
            base_dir,
            object_path,
        }) => (base_dir.as_str(), object_path.as_str()),
        _ => {
            let payload = contract
                .description()
                .trim()
                .strip_prefix(OBJECT_PARQUET_SINK_PREFIX)
                .ok_or_else(|| ExecutorError::InvalidAssignment {
                    message: format!(
                        "object sink must use {OBJECT_PARQUET_SINK_PREFIX}<base_dir>:<object_path>"
                    ),
                })?;
            payload
                .split_once(':')
                .ok_or_else(|| ExecutorError::InvalidAssignment {
                    message: format!(
                        "object sink must use {OBJECT_PARQUET_SINK_PREFIX}<base_dir>:<object_path>"
                    ),
                })?
        }
    };
    let base_dir = base_dir.trim();
    let object_path = object_path.trim();
    if base_dir.is_empty() || object_path.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("object sink base_dir and object_path cannot be empty"),
        });
    }

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
}
