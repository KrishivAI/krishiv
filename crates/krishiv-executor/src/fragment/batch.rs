//! Batch fragment execution: `execute_batch_fragment` and its helpers.

#[cfg(feature = "kafka")]
use std::path::PathBuf;
use std::sync::Arc;

use krishiv_proto::{ExecutorTaskAssignment, TaskRuntimeStats};
#[cfg(feature = "kafka")]
use krishiv_proto::{InputPartitionDescriptor, OutputContract, OutputContractDescriptor};
use krishiv_sql::SqlEngine;

use super::common::{
    parse_local_parquet_partitions, read_connector_parquet_partitions,
    read_inline_ipc_partitions, read_object_parquet_partitions, read_shuffle_flight_partitions,
    sql_query_from_fragment, task_fragment_body, write_object_parquet_sink,
};
use crate::runner::{
    ExecutorTaskOutput, ExecutorTaskRunner, OBJECT_PARQUET_SINK_PREFIX, SHUFFLE_WRITE_PREFIX,
};
#[cfg(feature = "kafka")]
use crate::runner::{
    KAFKA_TO_PARQUET_FRAGMENT, MEMORY_KAFKA_PARTITION_PREFIX, PARQUET_SINK_PREFIX,
};
use crate::{ExecutorError, ExecutorResult};

const WINDOW_PREFIX: &str = "window:";

/// Execute a batch (terminal) stage fragment.
pub(crate) async fn execute_batch_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
) -> ExecutorResult<ExecutorTaskOutput> {
    let fragment_body = task_fragment_body(assignment.plan_fragment().description());
    let fragment = fragment_body.as_str();
    if fragment.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("plan fragment description cannot be empty"),
        });
    }
    if assignment.output_contract().description().trim().is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("output contract description cannot be empty"),
        });
    }

    // R4a typed shuffle read: read from the in-memory store and return batches directly.
    if let Some(read_cfg) = assignment.shuffle_read() {
        if let Some(store) = &runner.inmem_shuffle {
            return execute_inmem_shuffle_read(assignment, read_cfg, store).await;
        } else {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from(
                    "shuffle_read config requires an in-memory shuffle store but none is configured",
                ),
            });
        }
    }

    #[cfg(feature = "kafka")]
    if fragment == KAFKA_TO_PARQUET_FRAGMENT {
        return execute_source_to_sink_pipeline(runner, assignment).await;
    }

    if let Some(shuffle_spec) = fragment.strip_prefix(SHUFFLE_WRITE_PREFIX) {
        if let Some(ctx) = &runner.shuffle {
            return execute_shuffle_write_fragment(
                Arc::clone(&runner.sql_engine),
                assignment,
                shuffle_spec,
                ctx,
            )
            .await;
        } else {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from(
                    "shuffle-write fragment requires a shuffle context but none is configured",
                ),
            });
        }
    }

    // R4a typed shuffle write: hash-partition SQL output and write to the in-memory store.
    if let Some(write_cfg) = assignment.shuffle_write() {
        if let Some(store) = &runner.inmem_shuffle {
            return execute_inmem_shuffle_write(
                Arc::clone(&runner.sql_engine),
                assignment,
                write_cfg,
                store,
            )
            .await;
        } else {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from(
                    "shuffle_write config requires an in-memory shuffle store but none is configured",
                ),
            });
        }
    }

    if let Some(query) = sql_query_from_fragment(fragment) {
        let engine = Arc::clone(&runner.sql_engine);
        for partition in parse_local_parquet_partitions(assignment.input_partitions())? {
            engine
                .register_parquet(partition.table_name(), partition.path())
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }
        for (table_name, batches) in
            read_connector_parquet_partitions(assignment.input_partitions()).await?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }
        for (table_name, batches) in
            read_object_parquet_partitions(assignment.input_partitions()).await?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }
        for (table_name, batches) in
            read_shuffle_flight_partitions(assignment.input_partitions()).await?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }
        // InlineIpc: Arrow IPC bytes delivered in-band with the task assignment.
        for (table_name, batches) in
            read_inline_ipc_partitions(assignment.input_partitions())?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }

        let dataframe = engine
            .sql(query)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: error.to_string(),
            })?;
        let (batches, sql_stats) = dataframe.collect_with_stats().await.map_err(|error| {
            ExecutorError::LocalExecution {
                message: error.to_string(),
            }
        })?;
        if assignment.output_contract().kind() == krishiv_proto::OutputContractKind::Sink
            && assignment
                .output_contract()
                .description()
                .trim()
                .starts_with(OBJECT_PARQUET_SINK_PREFIX)
        {
            write_object_parquet_sink(assignment.output_contract(), &batches).await?;
        }
        let row_count = batches.iter().map(|batch| batch.num_rows()).sum();
        let column_count = batches.first().map_or(0, |batch| batch.num_columns());
        let runtime_stats = TaskRuntimeStats {
            input_rows: 0,
            output_rows: sql_stats.output_rows,
            cpu_nanos: sql_stats.cpu_nanos,
            memory_bytes: 0,
            spill_bytes: 0,
        };
        return Ok(
            ExecutorTaskOutput::sql(row_count, batches.len(), column_count)
                .with_record_batches(batches)
                .with_runtime_stats(runtime_stats),
        );
    }

    if let Some(rest) = fragment.strip_prefix(WINDOW_PREFIX) {
        return execute_window_fragment(rest, assignment).await;
    }

    Err(ExecutorError::InvalidAssignment {
        message: format!("unsupported batch fragment type: {}", fragment),
    })
}

/// Execute a `window:<topic>:<spec_b64>` fragment.
///
/// Input batches are delivered as `InlineIpc` input partitions on the task
/// assignment — they never travel inside the fragment description string.
/// Results are returned as inline IPC via `OutputContractKind::InlineRecordBatches`.
async fn execute_window_fragment(
    rest: &str,
    assignment: &ExecutorTaskAssignment,
) -> ExecutorResult<ExecutorTaskOutput> {
    use base64::Engine as _;

    // Format: <topic>:<spec_b64>
    let mut parts = rest.splitn(2, ':');
    let _topic = parts.next().ok_or_else(|| ExecutorError::InvalidAssignment {
        message: format!("window fragment missing topic: {rest}"),
    })?;
    let spec_b64 = parts.next().ok_or_else(|| ExecutorError::InvalidAssignment {
        message: format!("window fragment missing spec_b64: {rest}"),
    })?;

    let spec_json = base64::engine::general_purpose::STANDARD
        .decode(spec_b64.as_bytes())
        .map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("window spec b64 decode: {e}"),
        })?;
    let plan_spec: krishiv_plan::window::WindowExecutionSpec =
        serde_json::from_slice(&spec_json).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("window spec json decode: {e}"),
        })?;

    // Read input batches from InlineIpc partitions (not from the fragment string).
    let inline_tables = read_inline_ipc_partitions(assignment.input_partitions())?;
    let input_batches: Vec<_> = inline_tables.into_iter().flat_map(|(_, b)| b).collect();

    let output_batches = tokio::task::spawn_blocking(move || {
        krishiv_exec::execute_bounded_window(input_batches, &plan_spec)
    })
    .await
    .map_err(|e| ExecutorError::LocalExecution {
        message: format!("window blocking task: {e}"),
    })?
    .map_err(|e| ExecutorError::LocalExecution {
        message: format!("window execution: {e}"),
    })?;

    let row_count = output_batches.iter().map(|b| b.num_rows()).sum();
    let col_count = output_batches.first().map_or(0, |b| b.num_columns());
    Ok(
        ExecutorTaskOutput::sql(row_count, output_batches.len(), col_count)
            .with_record_batches(output_batches),
    )
}

/// Execute a `shuffle-write:hash:<key_column>:<num_partitions>` fragment.
async fn execute_shuffle_write_fragment(
    engine: Arc<SqlEngine>,
    assignment: &ExecutorTaskAssignment,
    spec: &str,
    ctx: &crate::runner::ShuffleContext,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    // Parse "hash:<key_column>:<num_partitions>"
    let parts: Vec<&str> = spec.splitn(3, ':').collect();
    if parts.len() != 3 || parts[0] != "hash" {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "shuffle-write spec must be 'hash:<key_column>:<num_partitions>', got '{spec}'"
            ),
        });
    }
    let key_column = parts[1].trim();
    let num_partitions: u32 =
        parts[2]
            .trim()
            .parse()
            .map_err(|_| ExecutorError::InvalidAssignment {
                message: format!(
                    "shuffle-write num_partitions is not a valid u32: '{}'",
                    parts[2]
                ),
            })?;
    if key_column.is_empty() || num_partitions == 0 {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("shuffle-write key_column and num_partitions must be non-empty"),
        });
    }

    let query = assignment
        .output_contract()
        .description()
        .trim()
        .strip_prefix("sql:")
        .map(str::trim)
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: String::from(
                "shuffle-write output contract must start with 'sql:' followed by the query",
            ),
        })?;

    for partition in parse_local_parquet_partitions(assignment.input_partitions())? {
        engine
            .register_parquet(partition.table_name(), partition.path())
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in
        read_connector_parquet_partitions(assignment.input_partitions()).await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in
        read_object_parquet_partitions(assignment.input_partitions()).await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in
        read_shuffle_flight_partitions(assignment.input_partitions()).await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }

    let dataframe = engine
        .sql(query)
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;
    let batches = dataframe
        .collect()
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;

    let partitioner = HashPartitioner::new(key_column, num_partitions);
    let job_id = assignment.job_id().as_str();
    let stage_id = assignment.stage_id().as_str();
    let lease_token = assignment.lease_generation().as_u64();

    for p in 0..num_partitions {
        let id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id: stage_id.to_owned(),
            partition: p,
        };
        ctx.store
            .register_partition_lease(id, lease_token)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("shuffle lease registration failed: {e}"),
            })?;
    }

    let mut partition_batches: Vec<Vec<arrow::record_batch::RecordBatch>> =
        vec![Vec::new(); num_partitions as usize];
    let mut total_rows: usize = 0;

    for batch in &batches {
        total_rows += batch.num_rows();
        let buckets = partitioner
            .partition(batch)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("hash partition failed: {e}"),
            })?;
        for (bucket_idx, bucket_batch) in buckets.into_iter().enumerate() {
            if bucket_batch.num_rows() > 0 {
                partition_batches[bucket_idx].push(bucket_batch);
            }
        }
    }

    let output_schema: arrow::datatypes::SchemaRef = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));

    let mut outputs: Vec<krishiv_proto::ShufflePartitionOutput> =
        Vec::with_capacity(num_partitions as usize);

    for (p, part_batches) in partition_batches.into_iter().enumerate() {
        let p = p as u32;
        let id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id: stage_id.to_owned(),
            partition: p,
        };
        let schema = part_batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| output_schema.clone());
        let size_bytes: u64 = part_batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        let partition = ShufflePartition {
            id,
            schema,
            batches: part_batches,
        };
        ctx.store
            .write_partition(partition, lease_token)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("shuffle write failed for partition {p}: {e}"),
            })?;
        outputs.push(krishiv_proto::ShufflePartitionOutput::new(
            p,
            size_bytes,
            ctx.flight_endpoint.clone(),
        ));
    }

    Ok(ExecutorTaskOutput::shuffle_write(total_rows, outputs))
}

/// Execute a typed R4a shuffle-write task backed by `InMemoryShuffleStore`.
async fn execute_inmem_shuffle_write(
    engine: Arc<SqlEngine>,
    assignment: &ExecutorTaskAssignment,
    write_cfg: &krishiv_proto::ShuffleWriteConfig,
    store: &std::sync::Arc<krishiv_shuffle::InMemoryShuffleStore>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    let fragment_body = task_fragment_body(assignment.plan_fragment().description());
    let batches = if let Some(query) = sql_query_from_fragment(&fragment_body) {
        for partition in parse_local_parquet_partitions(assignment.input_partitions())? {
            engine
                .register_parquet(partition.table_name(), partition.path())
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;
        }
        for (table_name, tbl_batches) in
            read_connector_parquet_partitions(assignment.input_partitions()).await?
        {
            engine
                .register_record_batches(&table_name, tbl_batches)
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;
        }
        for (table_name, tbl_batches) in
            read_object_parquet_partitions(assignment.input_partitions()).await?
        {
            engine
                .register_record_batches(&table_name, tbl_batches)
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;
        }
        for (table_name, tbl_batches) in
            read_shuffle_flight_partitions(assignment.input_partitions()).await?
        {
            engine
                .register_record_batches(&table_name, tbl_batches)
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;
        }
        let dataframe = engine
            .sql(query)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
        dataframe
            .collect()
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?
    } else {
        Vec::new()
    };

    let num_partitions = write_cfg.num_partitions as u32;
    let lease_token = write_cfg.lease_token;
    let job_id = assignment.job_id().as_str();
    let stage_id = write_cfg.stage_id.as_str();
    let key_column = write_cfg.key_columns.first().map(String::as_str);

    let output_schema: arrow::datatypes::SchemaRef = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));

    let mut partition_batches: Vec<Vec<arrow::record_batch::RecordBatch>> =
        vec![Vec::new(); num_partitions as usize];
    let mut total_rows: usize = 0;

    for batch in &batches {
        total_rows += batch.num_rows();
        if num_partitions == 0 || batch.num_rows() == 0 {
            continue;
        }
        if let Some(key_col) = key_column {
            let partitioner = HashPartitioner::new(key_col, num_partitions);
            let buckets =
                partitioner
                    .partition(batch)
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!("R4a hash partition failed: {e}"),
                    })?;
            for (bucket_idx, bucket_batch) in buckets.into_iter().enumerate() {
                if bucket_batch.num_rows() > 0 {
                    partition_batches[bucket_idx].push(bucket_batch);
                }
            }
        } else {
            partition_batches[0].push(batch.clone());
        }
    }

    let mut outputs: Vec<krishiv_proto::ShufflePartitionOutput> =
        Vec::with_capacity(num_partitions as usize);

    for (p, part_batches) in partition_batches.into_iter().enumerate() {
        let p = p as u32;
        let id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id: stage_id.to_owned(),
            partition: p,
        };
        let schema = part_batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| output_schema.clone());
        let size_bytes: u64 = part_batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        let partition = ShufflePartition {
            id,
            schema,
            batches: part_batches,
        };
        store
            .write_partition(partition, lease_token)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("R4a in-memory shuffle write failed for partition {p}: {e}"),
            })?;
        outputs.push(krishiv_proto::ShufflePartitionOutput::inline(p, size_bytes));
    }

    Ok(ExecutorTaskOutput::shuffle_write(total_rows, outputs))
}

/// Execute a typed R4a shuffle-read task backed by `InMemoryShuffleStore`.
async fn execute_inmem_shuffle_read(
    assignment: &ExecutorTaskAssignment,
    read_cfg: &krishiv_proto::ShuffleReadConfig,
    store: &std::sync::Arc<krishiv_shuffle::InMemoryShuffleStore>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{PartitionId, ShuffleStore as _};

    let id = PartitionId {
        job_id: assignment.job_id().as_str().to_owned(),
        stage_id: read_cfg.stage_id.as_str().to_owned(),
        partition: read_cfg.partition_id as u32,
    };

    let partition = store
        .read_partition(&id)
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: format!(
                "R4a in-memory shuffle read failed for partition {}: {e}",
                read_cfg.partition_id
            ),
        })?;

    let batches = partition.map(|p| p.batches).unwrap_or_default();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let batch_count = batches.len();
    let column_count = batches.first().map_or(0, |b| b.num_columns());

    Ok(ExecutorTaskOutput::sql(
        row_count,
        batch_count,
        column_count,
    ))
}

#[cfg(feature = "kafka")]
async fn execute_source_to_sink_pipeline(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_connectors::kafka::{
        InMemoryKafkaOffsetCommitter, InMemoryKafkaSource, KafkaOffset,
    };
    use krishiv_connectors::parquet::ParquetSink;
    use krishiv_connectors::{PostWriteOffsetCommitProtocol, Source};

    let (topic, partition, start_offset, batch) =
        parse_memory_kafka_partition(assignment.input_partitions())?;
    let sink_path = parse_parquet_sink_path(assignment.output_contract())?;
    // Build the source_id used to look up any coordinator-issued throttle limit.
    // Format mirrors the assignment partition descriptor: "<topic>/<partition>".
    let source_id = format!("{topic}/{partition}");
    let mut source = InMemoryKafkaSource::new(topic, partition, start_offset, vec![batch]);
    let mut sink =
        ParquetSink::create(&sink_path).map_err(|error| ExecutorError::LocalExecution {
            message: format!(
                "parquet sink create failed for '{}': {error}",
                sink_path.display()
            ),
        })?;
    let mut committer = InMemoryKafkaOffsetCommitter::new();

    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    let mut column_count = 0usize;
    while let Some(batch) =
        source
            .read_batch()
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("memory Kafka source read failed: {error}"),
            })?
    {
        // R7.2: Log any active throttle limit for this source before emitting.
        // Real token-bucket enforcement is a follow-on task; the log makes the
        // wiring visible in traces so operators can confirm the limit arrived.
        runner.source_throttle_limits.check_and_log(&source_id);
        row_count += batch.num_rows();
        batch_count += 1;
        column_count = batch.num_columns();
        let offset = source
            .current_offset()
            .and_then(|offset| offset.downcast::<KafkaOffset>().ok())
            .map(|offset| *offset)
            .ok_or_else(|| ExecutorError::LocalExecution {
                message: String::from("memory Kafka source did not expose a KafkaOffset"),
            })?;

        PostWriteOffsetCommitProtocol::write_flush_commit(&mut sink, &mut committer, batch, offset)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("Kafka-to-Parquet post-write commit failed: {error}"),
            })?;
    }

    if committer.committed_offsets().is_empty() && row_count > 0 {
        return Err(ExecutorError::LocalExecution {
            message: String::from("Kafka-to-Parquet pipeline wrote rows without committing offset"),
        });
    }

    Ok(ExecutorTaskOutput::connector_pipeline(
        row_count,
        batch_count,
        column_count,
    ))
}

#[cfg(feature = "kafka")]
fn parse_parquet_sink_path(contract: &OutputContract) -> ExecutorResult<PathBuf> {
    let path = match contract.descriptor() {
        Some(OutputContractDescriptor::ParquetSink { path }) => path.as_str(),
        _ => contract
            .description()
            .trim()
            .strip_prefix(PARQUET_SINK_PREFIX)
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!(
                    "Kafka-to-Parquet output contract must use {PARQUET_SINK_PREFIX}<path>"
                ),
            })?,
    }
    .trim();
    if path.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("Kafka-to-Parquet output path cannot be empty"),
        });
    }
    Ok(PathBuf::from(path))
}

#[cfg(feature = "kafka")]
fn parse_memory_kafka_partition(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<(String, i32, i64, arrow::record_batch::RecordBatch)> {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let mut parsed = None;
    for partition in partitions {
        if let Some(descriptor) = partition.descriptor() {
            let InputPartitionDescriptor::MemoryKafka {
                topic,
                partition: kafka_partition,
                start_offset,
                records,
            } = descriptor
            else {
                continue;
            };
            if parsed.is_some() {
                return Err(ExecutorError::InvalidAssignment {
                    message: String::from(
                        "Kafka-to-Parquet pipeline accepts exactly one memory-kafka partition",
                    ),
                });
            }
            if topic.trim().is_empty() || records.is_empty() {
                return Err(ExecutorError::InvalidAssignment {
                    message: String::from("typed memory-kafka topic and records cannot be empty"),
                });
            }
            let ids = records.iter().map(|record| record.id).collect::<Vec<_>>();
            let values = records
                .iter()
                .map(|record| record.value.as_str())
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(values)),
                ],
            )
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("failed to build typed memory-kafka record batch: {error}"),
            })?;
            parsed = Some((topic.clone(), *kafka_partition, *start_offset, batch));
            continue;
        }

        let desc = partition.description().trim();
        let Some(payload) = desc.strip_prefix(MEMORY_KAFKA_PARTITION_PREFIX) else {
            continue;
        };
        if parsed.is_some() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from(
                    "Kafka-to-Parquet pipeline accepts exactly one memory-kafka partition",
                ),
            });
        }
        let parts: Vec<&str> = payload.splitn(4, ':').collect();
        if parts.len() != 4 {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} must use memory-kafka:<topic>:<partition>:<start_offset>:<id=value,...>",
                    partition.partition_id()
                ),
            });
        }
        let topic = parts[0].trim();
        if topic.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("memory-kafka topic cannot be empty"),
            });
        }
        let kafka_partition =
            parts[1]
                .trim()
                .parse::<i32>()
                .map_err(|error| ExecutorError::InvalidAssignment {
                    message: format!("invalid memory-kafka partition id: {error}"),
                })?;
        let start_offset =
            parts[2]
                .trim()
                .parse::<i64>()
                .map_err(|error| ExecutorError::InvalidAssignment {
                    message: format!("invalid memory-kafka start offset: {error}"),
                })?;
        let records = parts[3].trim();
        if records.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("memory-kafka records cannot be empty"),
            });
        }

        let mut ids = Vec::new();
        let mut values = Vec::new();
        for record in records.split(',') {
            let (id, value) =
                record
                    .trim()
                    .split_once('=')
                    .ok_or_else(|| ExecutorError::InvalidAssignment {
                        message: format!(
                            "invalid memory-kafka record '{record}', expected id=value"
                        ),
                    })?;
            ids.push(id.trim().parse::<i64>().map_err(|error| {
                ExecutorError::InvalidAssignment {
                    message: format!("invalid memory-kafka record id '{id}': {error}"),
                }
            })?);
            values.push(value.trim().to_owned());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let value_refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(value_refs)),
            ],
        )
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("failed to build memory-kafka record batch: {error}"),
        })?;
        parsed = Some((topic.to_owned(), kafka_partition, start_offset, batch));
    }

    parsed.ok_or_else(|| ExecutorError::InvalidAssignment {
        message: format!(
            "Kafka-to-Parquet pipeline requires one {MEMORY_KAFKA_PARTITION_PREFIX}<topic>:<partition>:<start_offset>:<records> input partition"
        ),
    })
}
