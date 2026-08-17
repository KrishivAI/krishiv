//! Shared helper functions used by multiple fragment execution modules.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::DataType;
use krishiv_dataflow::adaptive::HeavyHittersTracker;
use krishiv_proto::{
    CheckpointSourceOffset, HeartbeatHotKeyReport, InputPartitionDescriptor, JobId, OutputContract,
    OutputContractDescriptor, TaskState, TransportDisposition,
};

use crate::runner::{
    CONNECTOR_PARQUET_PARTITION_PREFIX, LocalParquetPartition, OBJECT_PARQUET_PARTITION_PREFIX,
    OBJECT_PARQUET_SINK_PREFIX, REGISTRY_CONNECTOR_PARTITION_PREFIX, RestoredSourceOffset,
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
    let table_name = parts.first().copied().unwrap_or("").trim();
    let base_dir = parts.get(1).copied().unwrap_or("").trim();
    let object_path = parts.get(2).copied().unwrap_or("").trim();
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

#[derive(Debug, Clone)]
pub(crate) struct RegistryPartitionSpec {
    pub partition_id: String,
    pub raw_descriptor: String,
    pub kind: String,
    pub table_name: String,
    pub connector_config: krishiv_connectors::ConnectorConfig,
}

impl RegistryPartitionSpec {
    pub fn continuous_source_key(&self, job_id: &str) -> String {
        format!("{job_id}|{}|{}", self.partition_id, self.raw_descriptor)
    }

    pub fn restored_offset<'a>(
        &self,
        restored_offsets: Option<&'a [RestoredSourceOffset]>,
    ) -> Option<&'a RestoredSourceOffset> {
        restored_offsets
            .unwrap_or(&[])
            .iter()
            .find(|offset| offset.matches_source(&self.partition_id, &self.table_name))
    }
}

pub(crate) fn parse_registry_partition_specs(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<RegistryPartitionSpec>> {
    use krishiv_connectors::ConnectorConfig;

    let mut specs = Vec::new();
    for partition in partitions {
        let desc = partition.description().trim();
        let Some(payload) = desc.strip_prefix(REGISTRY_CONNECTOR_PARTITION_PREFIX) else {
            continue;
        };
        let mut parts = payload.splitn(3, ':');
        let kind_str = parts.next().unwrap_or("").trim();
        let table_name = parts.next().unwrap_or("").trim().to_owned();
        let config_json = parts.next().unwrap_or("{}").trim();

        if kind_str.is_empty() || table_name.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "registry-connector partition '{}' must have the form \
                     registry-connector:<kind>:<table>:<config_json>",
                    partition.partition_id()
                ),
            });
        }

        let props: std::collections::BTreeMap<String, String> =
            serde_json::from_str(config_json).map_err(|e| ExecutorError::InvalidAssignment {
                message: format!(
                    "registry-connector partition '{}': invalid config JSON '{config_json}': {e}",
                    partition.partition_id()
                ),
            })?;
        let mut connector_config = ConnectorConfig::new(kind_str, kind_str);
        for (k, v) in props {
            connector_config = connector_config.with_property(k, v);
        }

        specs.push(RegistryPartitionSpec {
            partition_id: partition.partition_id().to_owned(),
            raw_descriptor: desc.to_owned(),
            kind: kind_str.to_owned(),
            table_name,
            connector_config,
        });
    }
    Ok(specs)
}

/// Coerce a source-read batch to the column types the windowed operator
/// requires. Kafka JSON sources (`registry-connector:kafka`) decode every field
/// as `Utf8` (see `RdkafkaKafkaSource::payload_to_batch`), but the windowed
/// executor requires an `Int64`/`Timestamp` event-time column, a
/// `key_column_type`-typed key, and numeric aggregate-input columns — a raw
/// all-`Utf8` batch fails the operator's type checks and its watermark never
/// advances. This casts exactly those columns, and only when the current type is
/// incompatible, so it is a cheap no-op for already-typed inputs (e.g. batches
/// fed through the coordinator push path). Casts are strict (`safe = false`): a
/// value that cannot be parsed to the required type fails loudly rather than
/// silently nulling and dropping the row.
pub(crate) fn coerce_batch_for_window(
    batch: &arrow::record_batch::RecordBatch,
    spec: &krishiv_plan::window::WindowExecutionSpec,
) -> ExecutorResult<arrow::record_batch::RecordBatch> {
    krishiv_dataflow::stream_driver::coerce_batch_for_window(batch, spec).map_err(|error| {
        ExecutorError::LocalExecution {
            message: error.to_string(),
        }
    })
}

/// Read input partitions of the form `registry-connector:<kind>:<table_name>:<config_json>`
/// using a pluggable [`ConnectorRegistry`], preserving any connector-encoded
/// checkpoint offset observed after the read.
///
/// The `config_json` segment is a JSON object whose keys map to
/// [`ConnectorConfig`][krishiv_connectors::ConnectorConfig] properties.
/// Example:
/// ```text
/// registry-connector:parquet-directory:my_table:{"path":"/data/year=2024","recursive":"true"}
/// ```
///
/// This is the CO5 execution path that allows any registered connector (including
/// the new `ParquetDirectorySource` and `S3PrefixSource`) to be used as an input
/// partition source from fragment assignments.
///
/// # Offsets are read, not written, here
///
/// This path deliberately does **not** ask the source for a checkpoint offset
/// after reading. It used to: an inner function computed one per partition and
/// its only caller discarded it with `let _ = &read.source_offset;`. That cost
/// a real connector call per partition for a value nobody consumed — and worse,
/// the call is fallible and propagated with `?`, so a batch fragment could fail
/// on a checkpoint-encoding error from a feature it does not use.
///
/// The streaming loop, which genuinely needs offsets, captures them itself from
/// the source it owns (`run_loop.rs`, via [`checkpoint_offset_from_dyn_source`])
/// — a batch fragment is one-shot and has nowhere to put one.
pub(crate) async fn read_registry_partitions(
    registry: &krishiv_connectors::ConnectorRegistry,
    partitions: &[krishiv_proto::InputPartition],
    restored_offsets: Option<&[RestoredSourceOffset]>,
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    let mut result = Vec::new();
    for spec in parse_registry_partition_specs(partitions)? {
        let mut source = registry
            .open_source(&spec.connector_config)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!(
                    "registry-connector open failed for kind '{}' \
                     (partition '{}'): {e}",
                    spec.kind, spec.partition_id
                ),
            })?;

        if let Some(restored_offset) = spec.restored_offset(restored_offsets) {
            source
                .restore_encoded_checkpoint_offset_dyn(&restored_offset.encoded_offset)
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!(
                        "registry-connector restore failed for kind '{}' \
                         table '{}' partition '{}': {e}",
                        spec.kind, spec.table_name, spec.partition_id
                    ),
                })?;
        }

        let mut batches = Vec::new();
        while let Some(batch) =
            source
                .read_batch_dyn()
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!(
                        "registry-connector read failed for kind '{}': {e}",
                        spec.kind
                    ),
                })?
        {
            batches.push(batch);
        }
        result.push((spec.table_name, batches));
    }
    Ok(result)
}

pub(crate) fn checkpoint_offset_from_dyn_source(
    spec: &RegistryPartitionSpec,
    source: &dyn krishiv_connectors::DynSource,
) -> ExecutorResult<Option<CheckpointSourceOffset>> {
    source
        .encoded_checkpoint_offset_dyn()
        .map_err(|e| ExecutorError::LocalExecution {
            message: format!(
                "registry-connector checkpoint offset failed for kind '{}' \
                 table '{}' partition '{}': {e}",
                spec.kind, spec.table_name, spec.partition_id
            ),
        })?
        .map(|encoded_offset| {
            Ok::<_, ExecutorError>(CheckpointSourceOffset {
                partition_id: krishiv_proto::PartitionId::try_new(spec.partition_id.clone())
                    .map_err(|_| ExecutorError::InvalidAssignment {
                        message: format!(
                            "registry-connector partition '{}' has invalid checkpoint id",
                            spec.partition_id
                        ),
                    })?,
                offset: legacy_offset_from_dyn_source(source),
                encoded_offset,
            })
        })
        .transpose()
}

pub(crate) fn legacy_offset_from_dyn_source(source: &dyn krishiv_connectors::DynSource) -> i64 {
    let Some(offset) = source.current_offset_dyn() else {
        return 0;
    };
    let any = offset.as_ref();
    if let Some(value) = any.downcast_ref::<i64>() {
        *value
    } else if let Some(value) = any.downcast_ref::<i32>() {
        i64::from(*value)
    } else if let Some(value) = any.downcast_ref::<u64>() {
        i64::try_from(*value).unwrap_or(i64::MAX)
    } else if let Some(value) = any.downcast_ref::<u32>() {
        i64::from(*value)
    } else if let Some(value) = any.downcast_ref::<usize>() {
        i64::try_from(*value).unwrap_or(i64::MAX)
    } else if let Some(value) = any.downcast_ref::<isize>() {
        i64::try_from(*value).unwrap_or_else(|_| {
            if value.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        })
    } else {
        0
    }
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

/// Extract the Iceberg streaming-sink descriptor from `contract`, if any.
///
/// Accepts the typed `IcebergSink` descriptor and the legacy string form
/// (`iceberg-sink:<root>|<table>|mode=...`) that reaches assignments via
/// `TaskSpec::with_sink_contract`. Returns `Ok(None)` when the contract does
/// not target an Iceberg sink; a present-but-malformed contract fails the
/// task rather than silently discarding cycle output.
pub(crate) fn iceberg_sink_descriptor(
    contract: &OutputContract,
) -> ExecutorResult<Option<OutputContractDescriptor>> {
    if let Some(descriptor @ OutputContractDescriptor::IcebergSink { .. }) = contract.descriptor() {
        return Ok(Some(descriptor.clone()));
    }
    match OutputContractDescriptor::parse_iceberg_sink(contract.description()) {
        None => Ok(None),
        Some(Ok(descriptor)) => Ok(Some(descriptor)),
        Some(Err(message)) => Err(ExecutorError::InvalidAssignment { message }),
    }
}

/// Streaming object-parquet sink writer (Phase 52 #194, zero-materialization).
///
/// Consumes result batches one at a time, routing each through the Hive
/// partition split and appending to an incremental async parquet writer per
/// output object. Sink tasks therefore never hold the full result set: the
/// previous path collected every batch, then buffered the entire parquet
/// encoding in memory before one atomic put — and failed outright past the
/// buffer's 256 MiB pending cap. Memory here is bounded by the writers' open
/// row groups instead of the result size.
///
/// Writers upload via the object store's multipart protocol, so a task that
/// fails mid-write can leave a partial object. Staged contracts are immune
/// (only job-success publication moves staged files into the destination);
/// legacy direct writes trade the old all-or-nothing put for unbounded
/// result sizes, and a retried attempt overwrites its own object anyway.
pub(crate) struct ObjectParquetSinkStream {
    spec: krishiv_common::write_commit::SinkWriteSpec,
    store: Arc<dyn object_store::ObjectStore>,
    /// One incremental writer per output object, keyed by the object's
    /// relative path. `BTreeMap` keeps deterministic staged-path ordering.
    writers: std::collections::BTreeMap<
        String,
        parquet::arrow::AsyncArrowWriter<parquet::arrow::async_writer::ParquetObjectWriter>,
    >,
    /// `Some` when the staged commit protocol applies: `(job_id, task_id, attempt)`
    /// name the staged part files. `None` = legacy direct write to `dest_path`.
    staged_identity: Option<(String, String, u32)>,
    row_count: usize,
    batch_count: usize,
    column_count: usize,
}

impl ObjectParquetSinkStream {
    /// Open a sink stream for `assignment`'s object-parquet output contract.
    pub(crate) fn open(assignment: &krishiv_proto::ExecutorTaskAssignment) -> ExecutorResult<Self> {
        let spec = parse_object_parquet_sink_spec(assignment.output_contract())?;
        let staged_identity = spec.staged.then(|| {
            (
                assignment.job_id().as_str().to_owned(),
                assignment.task_id().as_str().to_owned(),
                assignment.attempt_id().as_u32(),
            )
        });
        Self::open_with_spec(spec, staged_identity)
    }

    fn open_with_spec(
        spec: krishiv_common::write_commit::SinkWriteSpec,
        staged_identity: Option<(String, String, u32)>,
    ) -> ExecutorResult<Self> {
        // Ensure the object-store prefix exists before opening it: the
        // destination directory may not have been created yet on this executor.
        std::fs::create_dir_all(&spec.base_dir).map_err(|error| ExecutorError::LocalExecution {
            message: format!(
                "failed to create sink base directory '{}': {error}",
                spec.base_dir
            ),
        })?;
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&spec.base_dir).map_err(
                |error| ExecutorError::LocalExecution {
                    message: format!(
                        "failed to open object store prefix '{}': {error}",
                        spec.base_dir
                    ),
                },
            )?,
        );
        Ok(Self {
            spec,
            store,
            writers: std::collections::BTreeMap::new(),
            staged_identity,
            row_count: 0,
            batch_count: 0,
            column_count: 0,
        })
    }

    /// The object path a slice with `hive_path` writes to.
    fn object_rel_path(&self, hive_path: &str) -> String {
        match &self.staged_identity {
            Some((job_id, task_id, attempt)) => self
                .spec
                .staged_file_rel(job_id, hive_path, task_id, *attempt),
            None => self.spec.dest_path.clone(),
        }
    }

    /// Route one result batch into the per-object writers.
    pub(crate) async fn write(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> ExecutorResult<()> {
        use krishiv_common::write_commit::split_batches_by_partition_columns;

        // Zero-row batches still flow through: they carry the schema, and the
        // pre-streaming path wrote a schema-only parquet object for them.
        self.row_count += batch.num_rows();
        self.batch_count += 1;
        self.column_count = self.column_count.max(batch.num_columns());

        let partition_by = if self.staged_identity.is_some() {
            self.spec.partition_by.clone()
        } else {
            // Legacy direct writes never split: everything lands in dest_path.
            Vec::new()
        };
        let slices =
            split_batches_by_partition_columns(std::slice::from_ref(&batch), &partition_by)
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("staged sink partition split failed: {error}"),
                })?;
        for slice in slices {
            let rel = self.object_rel_path(&slice.hive_path);
            for slice_batch in slice.batches {
                let writer = match self.writers.entry(rel.clone()) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let object_writer = parquet::arrow::async_writer::ParquetObjectWriter::new(
                            Arc::clone(&self.store),
                            object_store::path::Path::from(rel.as_str()),
                        );
                        let writer = parquet::arrow::AsyncArrowWriter::try_new(
                            object_writer,
                            slice_batch.schema(),
                            None,
                        )
                        .map_err(|error| {
                            ExecutorError::LocalExecution {
                                message: format!(
                                    "failed to create parquet sink writer '{rel}': {error}"
                                ),
                            }
                        })?;
                        entry.insert(writer)
                    }
                };
                writer.write(&slice_batch).await.map_err(|error| {
                    ExecutorError::LocalExecution {
                        message: format!("object-parquet sink write failed ('{rel}'): {error}"),
                    }
                })?;
            }
        }
        Ok(())
    }

    /// Close every writer and return the staged relative paths (empty for
    /// legacy direct writes) plus the accumulated result shape
    /// `(rows, batches, columns)`.
    pub(crate) async fn finish(self) -> ExecutorResult<(Vec<String>, (usize, usize, usize))> {
        let staged = self.staged_identity.is_some();
        let mut staged_paths = Vec::new();
        for (rel, writer) in self.writers {
            writer
                .close()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("object-parquet sink flush failed ('{rel}'): {error}"),
                })?;
            if staged {
                staged_paths.push(rel);
            }
        }
        Ok((
            staged_paths,
            (self.row_count, self.batch_count, self.column_count),
        ))
    }
}

/// Legacy direct object-parquet sink write (no staging, no commit protocol).
pub(crate) async fn write_object_parquet_sink(
    contract: &OutputContract,
    batches: &[arrow::record_batch::RecordBatch],
) -> ExecutorResult<()> {
    let spec = parse_object_parquet_sink_spec(contract)?;
    let mut sink = ObjectParquetSinkStream::open_with_spec(spec, None)?;
    for batch in batches {
        sink.write(batch.clone()).await?;
    }
    sink.finish().await.map(|_| ())
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
    let mut sink = ObjectParquetSinkStream::open(assignment)?;
    for batch in batches {
        sink.write(batch.clone()).await?;
    }
    sink.finish()
        .await
        .map(|(staged_paths, _shape)| staged_paths)
}

/// Fetch all `shuffle-flight:` input partitions via Arrow IPC over TCP and return
/// `(table_name, batches)` pairs ready for registration with the SQL engine.
///
/// Executor-wide semaphore that caps concurrent Arrow Flight shuffle fetches.
///
/// Default is 8 simultaneous in-flight requests per executor. Configurable via
/// `KRISHIV_SHUFFLE_FETCH_CONCURRENCY`. Shared across all tasks running on the
/// same executor process to prevent thundering-herd on shuffle services.
pub(crate) static SHUFFLE_FETCH_SEMAPHORE: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| {
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
                let _permit = SHUFFLE_FETCH_SEMAPHORE.acquire().await.map_err(|_| {
                    ExecutorError::LocalExecution {
                        message: "shuffle fetch semaphore closed".to_string(),
                    }
                })?;
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

    // Say how much was materialised.
    //
    // This function holds a task's ENTIRE shuffle input resident, and does it
    // outside every budget: the batches go on to `register_record_batches`,
    // which builds a `MemTable` that lives for the whole fragment. The map side
    // (`execute_shuffle_write_fragment`) streams and costs a batch; this path —
    // the final stage of every distributed query — costs the fragment.
    //
    // It is not simply an oversight. A `MemTable` can be scanned repeatedly and
    // a stream cannot, and a fragment's SQL may reference the same shuffle table
    // more than once, so converting needs either a proof of single use or a
    // spillable buffering provider. Until then the least this can do is not be
    // invisible: an unaccounted allocation that nothing reports is how the
    // executor OOM investigation lost weeks to shuffle page cache
    // (`executor_capacity`'s module docs).
    let bytes: usize = table_batches
        .values()
        .flat_map(|batches| batches.iter())
        .map(arrow::record_batch::RecordBatch::get_array_memory_size)
        .sum();
    let rows: usize = table_batches
        .values()
        .flat_map(|batches| batches.iter())
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    if bytes > 0 {
        tracing::info!(
            tables = table_batches.len(),
            rows,
            bytes,
            mib = bytes / (1024 * 1024),
            "shuffle read: whole fragment materialised, outside any memory budget"
        );
    }

    Ok(table_batches.into_iter().collect())
}

/// This executor process's capacity, derived once from its real CPU and
/// memory (see [`krishiv_common::ExecutorCapacity`]). Every per-task sizing
/// decision reads it, so slots, the query pool, and per-task parallelism
/// cannot drift apart the way three independent env lookups did.
pub(crate) fn executor_capacity() -> &'static krishiv_common::ExecutorCapacity {
    static CAPACITY: std::sync::LazyLock<krishiv_common::ExecutorCapacity> =
        std::sync::LazyLock::new(krishiv_common::ExecutorCapacity::detect);
    &CAPACITY
}

/// Translate a task's [`MemoryBudget`] limit into a DataFusion engine memory
/// limit for the per-task `SqlEngine`. `None` means the task has no private
/// limit and should draw on the shared executor pool instead.
pub(crate) fn task_engine_memory_limit(
    memory_budget: &krishiv_common::MemoryBudget,
) -> Option<usize> {
    memory_budget
        .limit()
        .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX))
}

/// Tasks currently executing on this process — armed by the runner's
/// `RunningAttemptGuard` (incremented when a task starts, decremented when
/// its guard drops), read by [`task_engine_parallelism`] to lend idle slots'
/// parallelism to the tasks that are actually running (Phase 65 elastic
/// DF-share).
pub(crate) static OCCUPIED_TASK_SLOTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Elastic share: `cores / occupied`, floored at the static per-slot share
/// and capped at the core count. Pure so it is directly testable; occupancy
/// is sampled once at engine construction — a task keeps the width it
/// planned with, so a burst arriving mid-flight transiently oversubscribes
/// (DataFusion partitions are cooperative Tokio tasks, not pinned threads)
/// and drains back to the static share as construction-time occupancy rises.
fn elastic_parallelism(
    base: std::num::NonZeroUsize,
    cores: std::num::NonZeroUsize,
    occupied: usize,
) -> std::num::NonZeroUsize {
    let lent = (cores.get() / occupied.max(1))
        .max(base.get())
        .min(cores.get());
    std::num::NonZeroUsize::new(lent).unwrap_or(base)
}

/// Elastic DF-share is on by default; `KRISHIV_ELASTIC_DF_SHARE=0` (or
/// `false`) disables it, and an explicit `KRISHIV_TARGET_PARALLELISM` pin
/// wins outright — a pinned width must never be widened behind the
/// operator's back (the all-slots-busy bench depends on exactly this).
fn elastic_df_share_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        if std::env::var("KRISHIV_TARGET_PARALLELISM").is_ok() {
            return false;
        }
        !matches!(
            std::env::var("KRISHIV_ELASTIC_DF_SHARE").as_deref(),
            Ok("0") | Ok("false")
        )
    })
}

/// DataFusion `target_partitions` for engines created inside a task slot.
///
/// [`krishiv_sql::SqlEngine::new`] defaults to the machine's full CPU
/// parallelism — right for the embedded placement, but an executor runs up to
/// `slots` fragments concurrently, so each per-task engine gets at least its
/// per-slot share: `max(1, cores / slots)`.
///
/// The share is taken from the resolved [`executor_capacity`], not from
/// `KRISHIV_TASK_SLOTS` directly. Reading the environment variable here was a
/// bug: `--slots N` on the command line sets the executor's real slot count
/// without setting the variable, so the share was computed against the
/// *default* slot count. On a 4-core executor started with `--slots 1` the
/// single task got 1 partition instead of 4 and used a quarter of the CPU.
///
/// Phase 65 elastic DF-share: a task starting while most slots are IDLE gets
/// the idle slots' shares lent to it (`cores / running-tasks`), so a lone
/// batch task on a 12-core/12-slot executor plans 12-wide instead of 1-wide.
/// Under full occupancy this is exactly the old static share.
pub(crate) fn task_engine_parallelism() -> std::num::NonZeroUsize {
    let capacity = executor_capacity();
    let base = capacity.task_parallelism;
    if !elastic_df_share_enabled() {
        return base;
    }
    let occupied = OCCUPIED_TASK_SLOTS.load(std::sync::atomic::Ordering::Relaxed);
    elastic_parallelism(base, capacity.cores, occupied)
}

/// Build the per-task SQL engine: its memory source, its job's UDF resource
/// limits, and the per-slot parallelism share.
pub(crate) fn task_sql_engine(
    engine_memory: krishiv_sql::EngineMemory,
    udf_limits: krishiv_plan::udf::ResourceLimits,
) -> krishiv_sql::SqlEngine {
    krishiv_sql::SqlEngine::new_with_engine_memory(engine_memory)
        .with_target_parallelism(task_engine_parallelism())
        .with_udf_limits(udf_limits)
        // Give the task engine a UDF registry so Python UDFs shipped in the
        // fragment (register_python_udf) have somewhere to land.
        .with_udf_registry(std::sync::Arc::new(std::sync::RwLock::new(
            krishiv_plan::udf::UdfRegistry::new(),
        )))
}

/// Environment variable: whole-process memory budget for this executor,
/// shared across all concurrent task slots.
pub const EXECUTOR_MEMORY_LIMIT_ENV: &str = "KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES";

/// Minimum engine memory granted to a task even when the process budget is
/// exhausted.  Keeps tasks progressing under heavy spill rather than failing;
/// bounds over-commit at `concurrent_slots × 32 MiB`.
const MIN_TASK_ENGINE_MEMORY_BYTES: u64 = 32 * 1024 * 1024;

/// The whole-process memory limit, parsed once.
///
/// One static rather than two. The unified arbiter and the (now removed) flat
/// process budget each parsed `KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES` with the
/// *same* predicate, so they were always both present or both absent — which
/// made the flat budget's entire reservation ladder unreachable: the unified
/// branch above it returned in every case that reached either. Deriving both
/// from one parse removes the possibility of them disagreeing at all.
static EXECUTOR_MEMORY_LIMIT: std::sync::LazyLock<Option<u64>> = std::sync::LazyLock::new(|| {
    std::env::var(EXECUTOR_MEMORY_LIMIT_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
});

/// Phase 56 (SH7): the executor-wide unified memory arbiter — one pool with
/// Shuffle/Execution/State soft regions (Spark's model) instead of
/// per-subsystem budget islands that overcommit against each other. Present
/// only when `KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES` bounds the process; the
/// unbounded dev default keeps the historical pass-through behavior.
static EXECUTOR_UNIFIED_MEMORY: std::sync::LazyLock<
    Option<Arc<krishiv_common::UnifiedMemoryManager>>,
> = std::sync::LazyLock::new(|| {
    (*EXECUTOR_MEMORY_LIMIT).map(krishiv_common::UnifiedMemoryManager::with_total)
});

/// The executor-wide unified memory arbiter, when the process is bounded.
pub(crate) fn executor_unified_memory() -> Option<&'static Arc<krishiv_common::UnifiedMemoryManager>>
{
    EXECUTOR_UNIFIED_MEMORY.as_ref()
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
        if self.bytes > 0
            && let Some(manager) = executor_unified_memory()
        {
            manager.release(krishiv_common::MemoryRegion::Execution, self.bytes);
        }
    }
}

/// Decide where a task's DataFusion execution memory comes from, reserving
/// against the explicit process budget when one is configured.
///
/// Three cases, in precedence order:
/// - **The job set a per-task limit** → a private pool of exactly that size.
///   An explicit request is honoured exactly.
/// - **`KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES` is set** → the operator asked for
///   hard per-task partitioning of a fixed process budget, so the reservation
///   ladder below runs and the task gets a private pool of its grant.
/// - **Neither** (the default) → the shared executor pool. Nothing is
///   reserved and nothing is divided up front: `FairSpillPool` apportions
///   live, so a task running alone gets the whole budget.
///
/// The returned guard must be held for the duration of task execution.
pub(crate) fn reserve_task_engine_memory(
    memory_budget: &krishiv_common::MemoryBudget,
) -> (krishiv_sql::EngineMemory, Option<ProcessMemoryReservation>) {
    use krishiv_common::MemoryRegion;

    let desired = task_engine_memory_limit(memory_budget);
    let (Some(process_limit), Some(manager)) = (*EXECUTOR_MEMORY_LIMIT, executor_unified_memory())
    else {
        // No explicit process partitioning: a per-task limit stays private,
        // everything else shares the executor pool.
        return (
            desired.map_or_else(
                krishiv_sql::EngineMemory::for_this_process,
                krishiv_sql::EngineMemory::Private,
            ),
            None,
        );
    };

    let want = desired
        .map(|d| u64::try_from(d).unwrap_or(u64::MAX))
        .unwrap_or(process_limit)
        .min(process_limit);

    // Phase 56 (SH7): task engine pools draw from the unified arbiter's
    // Execution region — shuffle buffers created inside task execution ride
    // this reservation; state backends reserve from the State region at
    // checkpoint time. `try_reserve` failure drives a shrink-then-minimum-grant
    // ladder.
    if manager.try_reserve(MemoryRegion::Execution, want) {
        let granted = usize::try_from(want).unwrap_or(usize::MAX);
        return (
            krishiv_sql::EngineMemory::Private(granted),
            Some(ProcessMemoryReservation { bytes: want }),
        );
    }

    // Full amount unavailable: take whatever remains, if meaningful.
    let remaining = manager
        .available_for_region(MemoryRegion::Execution)
        .min(want);
    if remaining >= MIN_TASK_ENGINE_MEMORY_BYTES
        && manager.try_reserve(MemoryRegion::Execution, remaining)
    {
        tracing::warn!(
            requested = want,
            granted = remaining,
            "unified executor memory under pressure; task granted reduced engine limit"
        );
        let granted = usize::try_from(remaining).unwrap_or(usize::MAX);
        return (
            krishiv_sql::EngineMemory::Private(granted),
            Some(ProcessMemoryReservation { bytes: remaining }),
        );
    }

    // Exhausted: minimum grant without reservation (bounded over-commit) so the
    // task spills aggressively instead of failing outright.
    tracing::warn!(
        requested = want,
        granted = MIN_TASK_ENGINE_MEMORY_BYTES,
        "unified executor memory exhausted; task granted minimum engine limit"
    );
    (
        krishiv_sql::EngineMemory::Private(
            usize::try_from(MIN_TASK_ENGINE_MEMORY_BYTES).unwrap_or(usize::MAX),
        ),
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

/// Extract a one-shot continuous-stream restore snapshot from task inputs.
pub(crate) fn read_continuous_restore_hint(
    partitions: &[krishiv_proto::InputPartition],
) -> Option<(Vec<u8>, i64)> {
    partitions.iter().find_map(|p| {
        if let Some(InputPartitionDescriptor::ContinuousRestore {
            snapshot_bytes,
            watermark_ms,
        }) = p.descriptor()
        {
            Some((snapshot_bytes.clone(), *watermark_ms))
        } else {
            None
        }
    })
}

/// Input-partition prefix marking "the stream has ended".
///
/// A direct sibling of `stream-peers:`. A cycle task exists for exactly one
/// invocation, so it cannot observe its own source running out — the control
/// plane is the only thing that can know, and this is how it says so.
pub(crate) const STREAM_EOS_PARTITION_PREFIX: &str = "stream-eos:";

/// Did the coordinator mark this invocation as the end of the stream?
///
/// When true, the cycle must close every window it still holds open. Without
/// this, a bounded job on a coordinator-backed cluster emits only what the
/// watermark closed on its own and reports success — a silently short answer.
pub(crate) fn read_end_of_stream_directive(partitions: &[krishiv_proto::InputPartition]) -> bool {
    partitions.iter().any(|partition| {
        partition
            .description()
            .trim()
            .starts_with(STREAM_EOS_PARTITION_PREFIX)
    })
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

/// Streaming-friendly hot-key accumulator: feed batches one at a time, finalize
/// into reports. This avoids retaining the full source `Vec<RecordBatch>` in
/// shuffle-write paths that stream SQL output batch-by-batch.
pub(crate) struct HotKeyAccumulator {
    tracker: HeavyHittersTracker,
    key_idx: Option<usize>,
}

impl HotKeyAccumulator {
    pub(crate) fn new() -> Self {
        Self {
            tracker: HeavyHittersTracker::new(64),
            key_idx: None,
        }
    }

    pub(crate) fn observe_batch(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        key_column: &str,
    ) {
        if self.key_idx.is_none() {
            self.key_idx = batch.schema().index_of(key_column).ok();
        }
        let Some(kidx) = self.key_idx else { return };
        let col = batch.column(kidx);
        match col.data_type() {
            DataType::Utf8 => {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.tracker.observe(arr.value(i).to_owned());
                        }
                    }
                }
            }
            DataType::LargeUtf8 => {
                if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.tracker.observe(arr.value(i).to_owned());
                        }
                    }
                }
            }
            DataType::Utf8View => {
                if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.tracker.observe(arr.value(i).to_owned());
                        }
                    }
                }
            }
            DataType::Int32 => {
                if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.tracker.observe(arr.value(i).to_string());
                        }
                    }
                }
            }
            DataType::Int64 => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.tracker.observe(arr.value(i).to_string());
                        }
                    }
                }
            }
            DataType::Boolean => {
                if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            self.tracker.observe(arr.value(i).to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub(crate) fn into_reports(
        self,
        job_id: &JobId,
        source_id: &str,
    ) -> Vec<HeartbeatHotKeyReport> {
        self.tracker
            .hot_keys(0.10)
            .into_iter()
            .map(|report| HeartbeatHotKeyReport {
                key: report.key,
                estimated_count: report.estimated_count,
                max_error: report.max_error,
                heat_score: report.heat_score,
                job_id: job_id.clone(),
                source_id: source_id.to_string(),
            })
            .collect()
    }
}

/// Track key-column heavy hitters across a slice of `RecordBatch`es and return
/// the set of keys that each carry ≥10% of the total row count.
///
/// Used by both batch shuffle-write and streaming window fragments so that hot-key
/// detection logic is not duplicated between them.  `source_id` is whatever
/// identifier the coordinator uses to correlate the report back to the originating
/// stage/partition (stage_id for batch, stage_id.to_string() for streaming).
pub(crate) fn build_hot_key_reports(
    batches: &[arrow::record_batch::RecordBatch],
    key_column: &str,
    job_id: &JobId,
    source_id: &str,
) -> Vec<HeartbeatHotKeyReport> {
    let mut acc = HotKeyAccumulator::new();
    for batch in batches {
        acc.observe_batch(batch, key_column);
    }
    acc.into_reports(job_id, source_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use krishiv_common::MemoryBudget;
    use krishiv_connectors::{
        ConnectorCapabilities, ConnectorConfig, ConnectorResult, DynSource, Source,
        registry::{
            ConnectorDescriptor, ConnectorKind, ConnectorRegistry, ConnectorRole, OpenSourceFuture,
            SourceDriver,
        },
    };
    use krishiv_proto::InputPartition;

    use crate::runner::RestoredSourceOffset;

    #[test]
    fn coerce_batch_for_window_casts_utf8_kafka_columns_to_window_types() {
        use arrow::array::{Array, Float64Array, StringArray};
        use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec};

        // A Kafka JSON source decodes every field as Utf8 (sorted by name):
        // key (string), ts (epoch-ms), val (numeric).
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("ts", DataType::Utf8, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(StringArray::from(vec!["1720000000000", "1720000060000"])) as _,
                Arc::new(StringArray::from(vec!["10", "32"])) as _,
            ],
        )
        .unwrap();

        let mut spec = WindowExecutionSpec::tumbling("key", "ts", 60_000);
        spec.agg_exprs = vec![WindowAgg {
            kind: WindowAggKind::Sum,
            input_column: "val".into(),
            output_column: "total".into(),
            filter: None,
        }];

        let coerced = super::coerce_batch_for_window(&batch, &spec).unwrap();
        let s = coerced.schema();
        // Key stays Utf8 (the default key_column_type), event time becomes Int64,
        // the aggregate input becomes Float64.
        assert_eq!(
            s.field_with_name("key").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            s.field_with_name("ts").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            s.field_with_name("val").unwrap().data_type(),
            &DataType::Float64
        );
        // Values round-trip through the strict cast.
        let ts = coerced
            .column(s.index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 1_720_000_000_000);
        let val = coerced
            .column(s.index_of("val").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(val.value(1), 32.0);

        // An already-typed batch (push path) is returned unchanged.
        let typed_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("val", DataType::Float64, true),
        ]));
        let typed = RecordBatch::try_new(
            typed_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_720_000_000_000_i64])) as _,
                Arc::new(Float64Array::from(vec![5.0])) as _,
            ],
        )
        .unwrap();
        let unchanged = super::coerce_batch_for_window(&typed, &spec).unwrap();
        assert_eq!(unchanged.schema(), typed_schema);
    }

    #[test]
    fn task_engine_memory_limit_uses_explicit_task_budget() {
        let budget = MemoryBudget::limited(256 * 1024 * 1024);
        assert_eq!(
            super::task_engine_memory_limit(&budget),
            Some(256 * 1024 * 1024)
        );
    }

    #[test]
    fn task_parallelism_is_the_resolved_capacitys_per_slot_share() {
        // The share comes from the resolved capacity rather than a second,
        // independent reading of KRISHIV_TASK_SLOTS — that divergence is what
        // made `--slots N` a no-op for per-task parallelism. The arithmetic
        // itself is covered by krishiv_common::executor_capacity's tests.
        //
        // With the Phase 65 elastic share the exact value depends on the
        // process-global occupancy gauge (other tests run tasks
        // concurrently), so the wired function is bounded, not pinned:
        // never below the static per-slot share, never above the cores.
        // The exact lending arithmetic is pinned by the elastic_parallelism
        // tests below.
        let capacity = super::executor_capacity();
        let got = super::task_engine_parallelism();
        assert!(
            got >= capacity.task_parallelism
                && got <= capacity.cores.max(capacity.task_parallelism),
            "elastic share {got} must stay within [per-slot share {}, cores {}]",
            capacity.task_parallelism,
            capacity.cores,
        );
        assert!(
            capacity.task_parallelism.get() * capacity.slots.get() <= capacity.cores.get().max(1),
            "per-slot shares must not oversubscribe the cores they divide"
        );
    }

    #[test]
    fn elastic_parallelism_lends_idle_slots_to_a_lone_task() {
        let nz = |n: usize| std::num::NonZeroUsize::new(n).unwrap();
        // 12 cores / 12 slots: static share is 1. A lone task (occupancy 1)
        // gets the whole machine; a gauge that has not been armed yet
        // (occupancy 0) reads the same as a lone task.
        assert_eq!(super::elastic_parallelism(nz(1), nz(12), 0), nz(12));
        assert_eq!(super::elastic_parallelism(nz(1), nz(12), 1), nz(12));
        // Half occupied: idle shares split among the runners.
        assert_eq!(super::elastic_parallelism(nz(1), nz(12), 6), nz(2));
        // Fully occupied: exactly the old static per-slot share.
        assert_eq!(super::elastic_parallelism(nz(1), nz(12), 12), nz(1));
        // Overloaded (more tasks than cores): floored at the static share.
        assert_eq!(super::elastic_parallelism(nz(1), nz(12), 24), nz(1));
        // Non-trivial base (4 cores / 2 slots = 2): lone task gets 4,
        // full occupancy the base 2, and the base always floors the result.
        assert_eq!(super::elastic_parallelism(nz(2), nz(4), 1), nz(4));
        assert_eq!(super::elastic_parallelism(nz(2), nz(4), 2), nz(2));
        assert_eq!(super::elastic_parallelism(nz(2), nz(4), 4), nz(2));
    }

    #[test]
    fn task_engine_memory_limit_is_none_without_an_explicit_task_budget() {
        // `None` now means "draw on the shared executor pool", not "fall back
        // to a private pool at the env default" — a per-task private pool is
        // exactly the per-slot overcommit the shared pool exists to remove.
        assert_eq!(
            super::task_engine_memory_limit(&MemoryBudget::unlimited()),
            None
        );
    }

    #[test]
    fn an_explicit_task_budget_gets_a_private_pool() {
        // KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES is not set in unit tests, so the
        // process budget is unlimited; the job's own limit is still honoured
        // exactly and needs no reservation guard.
        let budget = MemoryBudget::limited(64 * 1024 * 1024);
        let (memory, guard) = super::reserve_task_engine_memory(&budget);
        assert!(
            matches!(memory, krishiv_sql::EngineMemory::Private(bytes) if bytes == 64 * 1024 * 1024),
            "expected a private 64 MiB pool, got {memory:?}"
        );
        assert!(guard.is_none());
    }

    /// A per-slot *share* must never become a per-task *cap*.
    ///
    /// The two are opposites and were conflated: `min_task_memory_share_bytes`
    /// is the arithmetic share a task sees when every slot is busy, and it is
    /// documented as sizing spill reservations only. Feeding it into a task's
    /// `MemoryBudget` instead turns it into a hard ceiling — the task gets a
    /// *private* pool of that size and can never touch the rest of the shared
    /// pool, even running alone. TPC-H q10 at SF100 died asking for 6.9 MB
    /// against a 797.6 MB private pool while the executor's shared pool held
    /// 2392 MiB, ~1.6 GB of it idle in the same process.
    ///
    /// Asserting on the *pool identity* is what makes this a real test: a
    /// budgeted task gets `Private`, an unbudgeted one gets `Shared`, and only
    /// an explicit quota may produce the former.
    #[test]
    fn a_fair_share_must_not_become_a_private_pool() {
        let share = krishiv_common::executor_capacity::ExecutorCapacity::detect_cached()
            .min_task_memory_share_bytes();
        let Some(share) = share else {
            // No cgroup limit here: there is no share to mistake for a cap.
            return;
        };
        // What the regression did: budget = the fair share.
        let (as_budget, _g1) = super::reserve_task_engine_memory(&MemoryBudget::limited(share));
        assert!(
            matches!(as_budget, krishiv_sql::EngineMemory::Private(_)),
            "a limited budget must produce a private pool — which is exactly why \
             the per-slot share must not be used as one"
        );
        // What an unbudgeted batch task must get instead.
        let (unbudgeted, _g2) = super::reserve_task_engine_memory(&MemoryBudget::unlimited());
        assert!(
            matches!(
                unbudgeted,
                krishiv_sql::EngineMemory::Shared { .. } | krishiv_sql::EngineMemory::Unbounded
            ),
            "a task with no explicit quota must draw on the shared pool, so it can \
             use the whole budget when no other slot is competing"
        );
    }

    #[test]
    fn tasks_without_a_budget_share_one_pool_rather_than_each_taking_one() {
        // The property that makes executor memory safe: whatever the slot
        // count, unbudgeted tasks all point at the *same* pool object, so the
        // process cannot claim `slots × pool_size`.
        let (first, _guard_a) = super::reserve_task_engine_memory(&MemoryBudget::unlimited());
        let (second, _guard_b) = super::reserve_task_engine_memory(&MemoryBudget::unlimited());
        match (first, second) {
            (
                krishiv_sql::EngineMemory::Shared { pool: a, .. },
                krishiv_sql::EngineMemory::Shared { pool: b, .. },
            ) => assert!(
                Arc::ptr_eq(&a, &b),
                "concurrent tasks must share one pool, not two of the same size"
            ),
            // No cgroup limit in the test environment: unbounded is correct,
            // and is still one shared decision rather than per-task pools.
            (krishiv_sql::EngineMemory::Unbounded, krishiv_sql::EngineMemory::Unbounded) => {
                assert!(krishiv_sql::process_query_pool().is_none());
            }
            (a, b) => panic!("unbudgeted tasks got mismatched memory sources: {a:?} / {b:?}"),
        }
    }

    #[test]
    fn process_memory_reservation_releases_on_drop() {
        // Exercise the guard against a locally constructed budget through the
        // same release path the arbiter uses.
        let process = MemoryBudget::limited(100);
        assert!(process.try_reserve(80));
        process.release(80);
        assert_eq!(process.used_bytes(), 0);
        // The Drop impl releases into the unified arbiter, which is absent in
        // tests; verify dropping a zero-byte guard is a no-op and does not panic.
        drop(super::ProcessMemoryReservation { bytes: 0 });
    }

    /// The process limit and the arbiter must be the same decision.
    ///
    /// They were two `LazyLock`s parsing the same env var with the same
    /// predicate, which made them always agree — and therefore made the flat
    /// budget's whole reservation ladder unreachable, because the unified
    /// branch ahead of it returned in every case that got that far. Deriving
    /// the arbiter from the parsed limit is what makes "present together or
    /// absent together" a property of the code rather than a coincidence of two
    /// copies of one expression.
    #[test]
    fn the_memory_limit_and_the_arbiter_are_present_together_or_not_at_all() {
        assert_eq!(
            super::EXECUTOR_MEMORY_LIMIT.is_some(),
            super::executor_unified_memory().is_some(),
            "a bounded process must have an arbiter, and an unbounded one must not"
        );
    }

    #[derive(Clone)]
    struct RestoringSourceDriver {
        batches: Arc<Vec<RecordBatch>>,
    }

    impl SourceDriver for RestoringSourceDriver {
        fn descriptor(&self) -> ConnectorDescriptor {
            ConnectorDescriptor::new(
                ConnectorKind::Csv,
                ConnectorRole::Source,
                ConnectorCapabilities::new()
                    .with_bounded()
                    .with_rewindable()
                    .with_checkpoint(),
            )
        }

        fn validate(&self, _config: &ConnectorConfig) -> ConnectorResult<()> {
            Ok(())
        }

        fn open<'a>(&'a self, _config: &'a ConnectorConfig) -> OpenSourceFuture<'a> {
            let batches = Arc::clone(&self.batches);
            Box::pin(async move {
                Ok(Box::new(RestoringSource {
                    schema: batches
                        .first()
                        .map(|batch| batch.schema())
                        .unwrap_or_else(|| Arc::new(Schema::empty())),
                    batches,
                    cursor: 0,
                }) as Box<dyn DynSource>)
            })
        }
    }

    struct RestoringSource {
        schema: SchemaRef,
        batches: Arc<Vec<RecordBatch>>,
        cursor: usize,
    }

    impl Source for RestoringSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint()
        }

        fn source_schema(&self) -> Option<SchemaRef> {
            Some(self.schema.clone())
        }

        async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
            let batch = self.batches.get(self.cursor).cloned();
            if batch.is_some() {
                self.cursor = self.cursor.saturating_add(1);
            }
            Ok(batch)
        }

        fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
            Some(Box::new(self.cursor))
        }

        fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
            Ok(Some((self.cursor as u64).to_le_bytes().to_vec()))
        }

        fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
            if encoded.len() != 8 {
                return Err(krishiv_connectors::ConnectorError::Offset {
                    message: format!("expected 8 encoded cursor bytes, got {}", encoded.len()),
                });
            }
            let mut raw = [0_u8; 8];
            raw.copy_from_slice(encoded);
            self.cursor = usize::try_from(u64::from_le_bytes(raw)).map_err(|_| {
                krishiv_connectors::ConnectorError::Offset {
                    message: "encoded cursor exceeds usize".into(),
                }
            })?;
            Ok(())
        }
    }

    fn value_batch(value: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
    }

    #[tokio::test]
    async fn read_registry_partitions_restores_encoded_source_offset() {
        let mut registry = ConnectorRegistry::new();
        registry.register_source(Arc::new(RestoringSourceDriver {
            batches: Arc::new(vec![value_batch(10), value_batch(20)]),
        }));
        let partition =
            InputPartition::new("orders-partition-0", "registry-connector:csv:orders:{}");
        let restored = RestoredSourceOffset {
            partition_id: "orders-partition-0".into(),
            source_id: Some("orders".into()),
            legacy_offset: 1,
            encoded_offset: 1_u64.to_le_bytes().to_vec(),
        };

        let tables = super::read_registry_partitions(&registry, &[partition], Some(&[restored]))
            .await
            .expect("registry partition read");

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].0, "orders");
        assert_eq!(tables[0].1.len(), 1);
        let values = tables[0].1[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 20);
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
