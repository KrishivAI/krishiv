//! Shared helper functions used by multiple fragment execution modules.

use std::path::PathBuf;

use krishiv_proto::{
    InputPartitionDescriptor, OutputContract, OutputContractDescriptor, TaskState,
    TransportDisposition,
};

use crate::{ExecutorError, ExecutorResult};
use crate::runner::{
    LocalParquetPartition, CONNECTOR_PARQUET_PARTITION_PREFIX, OBJECT_PARQUET_PARTITION_PREFIX,
    OBJECT_PARQUET_SINK_PREFIX,
};

pub(crate) fn sql_query_from_fragment(fragment: &str) -> Option<&str> {
    let (_, query) = fragment.split_once("sql:")?;
    let query = query.trim();
    (!query.is_empty()).then_some(query)
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
/// Multiple partitions sharing the same table name are merged so the engine sees
/// one logical table regardless of how many physical shuffle partitions were read.
pub(crate) async fn read_shuffle_flight_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use std::collections::BTreeMap;

    use krishiv_shuffle::flight::FlightShuffleClient;

    let mut table_batches: BTreeMap<String, Vec<arrow::record_batch::RecordBatch>> =
        BTreeMap::new();

    for partition in partitions {
        let (table_name, flight_endpoint, job_id, upstream_stage_id, partition_id) =
            match partition.descriptor() {
                Some(InputPartitionDescriptor::ShuffleFlight {
                    table_name,
                    flight_endpoint,
                    job_id,
                    upstream_stage_id,
                    partition_id,
                }) => (
                    table_name.clone(),
                    flight_endpoint.clone(),
                    job_id.clone(),
                    upstream_stage_id.clone(),
                    *partition_id,
                ),
                Some(_) | None => continue,
            };

        let batches = FlightShuffleClient::fetch(
            &flight_endpoint,
            &job_id,
            &upstream_stage_id,
            partition_id,
        )
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: format!(
                "shuffle-flight fetch failed (endpoint={flight_endpoint} job={job_id} \
                 stage={upstream_stage_id} partition={partition_id}): {e}"
            ),
        })?;
        table_batches.entry(table_name).or_default().extend(batches);
    }

    Ok(table_batches.into_iter().collect())
}
