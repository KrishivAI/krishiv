//! Batch fragment execution: `execute_batch_fragment` and its helpers.

#[cfg(feature = "kafka")]
use std::path::PathBuf;
use std::sync::Arc;

use krishiv_common::MemoryBudget;
use krishiv_plan::udf::ResourceLimits;
use krishiv_proto::{ExecutorTaskAssignment, TaskRuntimeStats};
#[cfg(feature = "kafka")]
use krishiv_proto::{InputPartitionDescriptor, OutputContract, OutputContractDescriptor};

use super::common::{
    parse_local_parquet_partitions, read_connector_parquet_partitions, read_inline_ipc_partitions,
    read_object_parquet_partitions, read_shuffle_flight_partitions, sql_query_from_fragment,
    task_fragment_body, write_object_parquet_sink_for_task,
};
use crate::runner::{
    ExecutorTaskOutput, ExecutorTaskRunner, OBJECT_PARQUET_SINK_PREFIX, SHUFFLE_WRITE_PREFIX,
};

/// Register all input partitions from an assignment onto a SQL engine.
async fn load_input_tables(
    engine: &Arc<krishiv_sql::SqlEngine>,
    assignment: &krishiv_proto::ExecutorTaskAssignment,
) -> crate::ExecutorResult<()> {
    for partition in parse_local_parquet_partitions(assignment.input_partitions())? {
        engine
            .register_parquet(partition.table_name(), partition.path())
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in
        read_connector_parquet_partitions(assignment.input_partitions()).await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in
        read_object_parquet_partitions(assignment.input_partitions()).await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in
        read_shuffle_flight_partitions(assignment.input_partitions()).await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    Ok(())
}
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
    udf_limits: ResourceLimits,
    memory_budget: Arc<MemoryBudget>,
) -> ExecutorResult<ExecutorTaskOutput> {
    // Reserve this task's share of the executor process memory budget for the
    // duration of the fragment; the guard releases the share on return.
    let (engine_memory_limit, _process_memory_reservation) =
        crate::fragment::common::reserve_task_engine_memory(&memory_budget);
    let fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
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
                assignment,
                shuffle_spec,
                ctx,
                udf_limits.clone(),
                engine_memory_limit,
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
                assignment,
                write_cfg,
                store,
                udf_limits.clone(),
                engine_memory_limit,
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
        // Create a new SQL engine with UDF limits and the task's memory limit
        // for this task execution. The memory limit bounds DataFusion's pool
        // so sorts/joins/aggregations spill instead of growing unbounded.
        let engine = Arc::new(
            krishiv_sql::SqlEngine::new_with_memory_limit(engine_memory_limit)
                .with_udf_limits(udf_limits),
        );
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
        for (table_name, batches) in read_inline_ipc_partitions(assignment.input_partitions())? {
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
        let mut sink_staged_files = Vec::new();
        if assignment.output_contract().kind() == krishiv_proto::OutputContractKind::Sink
            && assignment
                .output_contract()
                .description()
                .trim()
                .starts_with(OBJECT_PARQUET_SINK_PREFIX)
        {
            sink_staged_files = write_object_parquet_sink_for_task(assignment, &batches).await?;
        }
        let row_count = batches.iter().map(|batch| batch.num_rows()).sum();
        let column_count = batches.first().map_or(0, |batch| batch.num_columns());
        if sql_stats.spill_bytes > 0 {
            krishiv_metrics::global_metrics()
                .record_spill(sql_stats.spill_bytes, sql_stats.spill_count);
        }
        let runtime_stats = TaskRuntimeStats {
            input_rows: 0,
            output_rows: sql_stats.output_rows,
            cpu_nanos: sql_stats.cpu_nanos,
            memory_bytes: 0,
            spill_bytes: sql_stats.spill_bytes,
            serialized_bytes: 0,
        };
        return Ok(
            ExecutorTaskOutput::sql(row_count, batches.len(), column_count)
                .with_record_batches(batches)
                .with_runtime_stats(runtime_stats)
                .with_sink_staged_files(sink_staged_files),
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
    let topic = parts
        .next()
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!("window fragment missing topic: {rest}"),
        })?;
    if !krishiv_common::validate::is_safe_identifier(topic) {
        return Err(ExecutorError::InvalidAssignment {
            message: format!("window fragment contains invalid topic '{topic}'"),
        });
    }
    let spec_b64 = parts
        .next()
        .ok_or_else(|| ExecutorError::InvalidAssignment {
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
    let mut inline_tables = read_inline_ipc_partitions(assignment.input_partitions())?;
    if inline_tables.len() != 1 {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "bounded window task requires exactly one inline input table; found {}",
                inline_tables.len()
            ),
        });
    }
    let (input_topic, input_batches) =
        inline_tables
            .pop()
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: "bounded window task is missing its inline input table".into(),
            })?;
    if input_topic != topic {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "bounded window input table '{input_topic}' does not match fragment topic '{topic}'"
            ),
        });
    }

    let output_batches = tokio::task::spawn_blocking(move || {
        // Bounded tasks replay their complete InlineIpc input after failure.
        // Reopening partial persistent state would double-apply rows on retry.
        krishiv_dataflow::execute_bounded_window(input_batches, &plan_spec, None)
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
    assignment: &ExecutorTaskAssignment,
    spec: &str,
    ctx: &crate::runner::ShuffleContext,
    udf_limits: ResourceLimits,
    engine_memory_limit: Option<usize>,
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

    // Create a new SQL engine with UDF limits and the task's memory limit.
    let limited_engine = Arc::new(
        krishiv_sql::SqlEngine::new_with_memory_limit(engine_memory_limit)
            .with_udf_limits(udf_limits),
    );
    load_input_tables(&limited_engine, assignment).await?;

    let dataframe = limited_engine
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

    let job_id = assignment.job_id().as_str();
    let stage_id = assignment.stage_id().as_str();
    let lease_token = assignment.lease_generation().as_u64();
    let partitioner = HashPartitioner::new(key_column, num_partitions)
        .with_seed(shuffle_seed_from_job_id(job_id));

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

    // Track heavy hitters (hot keys) during this shuffle write.
    let hot_key_reports =
        build_hot_key_reports(&batches, key_column, assignment.job_id(), stage_id);

    let mut output = ExecutorTaskOutput::shuffle_write(total_rows, outputs);
    output.hot_key_reports = hot_key_reports;
    Ok(output)
}

/// Execute a typed R4a shuffle-write task backed by `InMemoryShuffleStore`.
async fn execute_inmem_shuffle_write(
    assignment: &ExecutorTaskAssignment,
    write_cfg: &krishiv_proto::ShuffleWriteConfig,
    store: &std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
    udf_limits: ResourceLimits,
    engine_memory_limit: Option<usize>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    let fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
    // Create a new SQL engine with UDF limits and the task's memory limit.
    let limited_engine = Arc::new(
        krishiv_sql::SqlEngine::new_with_memory_limit(engine_memory_limit)
            .with_udf_limits(udf_limits),
    );
    let batches = if let Some(query) = sql_query_from_fragment(&fragment_body) {
        load_input_tables(&limited_engine, assignment).await?;
        let dataframe =
            limited_engine
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
            let partitioner = HashPartitioner::new(key_col, num_partitions)
                .with_seed(shuffle_seed_from_job_id(job_id));
            let buckets =
                partitioner
                    .partition(batch)
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!("hash partition failed: {e}"),
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
                message: format!("in-memory shuffle write failed for partition {p}: {e}"),
            })?;
        outputs.push(krishiv_proto::ShufflePartitionOutput::inline(p, size_bytes));
    }

    // Track heavy hitters (hot keys) during this in-memory shuffle write.
    let hot_key_reports = build_hot_key_reports(
        &batches,
        key_column.unwrap_or(""),
        assignment.job_id(),
        stage_id,
    );

    let mut output = ExecutorTaskOutput::shuffle_write(total_rows, outputs);
    output.hot_key_reports = hot_key_reports;
    Ok(output)
}

/// Execute a typed R4a shuffle-read task backed by `InMemoryShuffleStore`.
async fn execute_inmem_shuffle_read(
    assignment: &ExecutorTaskAssignment,
    read_cfg: &krishiv_proto::ShuffleReadConfig,
    store: &std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
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

    Ok(ExecutorTaskOutput::sql(row_count, batch_count, column_count).with_record_batches(batches))
}

#[cfg(feature = "kafka")]
async fn execute_source_to_sink_pipeline(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
) -> ExecutorResult<ExecutorTaskOutput> {
    let profile = krishiv_common::resolve_durability_profile();
    if krishiv_common::forbids_simulation_connectors(profile) {
        return execute_broker_kafka_to_parquet(runner, assignment, profile).await;
    }
    execute_memory_kafka_to_parquet(runner, assignment).await
}

#[cfg(feature = "kafka")]
async fn wait_for_throttle(runner: &ExecutorTaskRunner, source_id: &str, rows: u64) {
    while runner.source_throttle_limits.try_consume(source_id, rows) < rows {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(feature = "kafka")]
async fn execute_memory_kafka_to_parquet(
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
        let rows = batch.num_rows() as u64;
        wait_for_throttle(runner, &source_id, rows).await;
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

        PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            batch,
            offset.clone(),
        )
        .await
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("Kafka-to-Parquet post-write commit failed: {error}"),
        })?;

        // Record the live offset so checkpoint barrier acks carry it into
        // checkpoint metadata (mirrors the broker pipeline).
        let task_id = assignment.task_id().clone();
        runner
            .checkpoint_runners
            .entry(task_id.clone())
            .or_insert_with(|| crate::runner::TaskRunner::new(task_id))
            .kafka_source_offsets = vec![offset];
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
async fn execute_broker_kafka_to_parquet(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    profile: krishiv_common::DurabilityProfile,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_connectors::CheckpointSource;
    use krishiv_connectors::kafka::{MultiKafkaOffset, RdkafkaKafkaSource};

    let (topic, partition, _, _) = parse_memory_kafka_partition(assignment.input_partitions())?;
    let sink_path = parse_parquet_sink_path(assignment.output_contract())?;
    let source_id = format!("{topic}/{partition}");
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").map_err(|_| ExecutorError::LocalExecution {
            message: String::from(
                "durable Kafka pipeline requires KAFKA_BOOTSTRAP_SERVERS to be set",
            ),
        })?;
    let group_id = format!("krishiv-{}", assignment.job_id());
    let manual_commit = krishiv_common::requires_manual_kafka_commit(profile);
    let auto_commit = if manual_commit { None } else { Some(5_000) };
    let mut source = RdkafkaKafkaSource::new(bootstrap, group_id, topic.clone(), auto_commit, None)
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("rdkafka source for topic '{topic}': {error}"),
        })?;

    // Checkpoint restore: seek the consumer to the offsets recorded by the
    // restored checkpoint, bypassing group-managed positions.
    let job_id_str = assignment.job_id().as_str().to_owned();
    if let Some((_, restored)) = runner.kafka_restore_offsets.remove(&job_id_str) {
        let for_topic: Vec<_> = restored
            .iter()
            .filter(|ko| ko.topic == topic)
            .cloned()
            .collect();
        if !for_topic.is_empty() {
            let multi = MultiKafkaOffset::new(for_topic);
            source
                .restore_offset(&multi)
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("Kafka offset restore for topic '{topic}' failed: {error}"),
                })?;
            tracing::info!(
                job_id = %assignment.job_id(),
                topic = %topic,
                partitions = multi.offsets.len(),
                "Kafka source seeked to restored checkpoint offsets"
            );
        }
        // Offsets for other topics belong to other source tasks of this job:
        // put them back for those pipelines to consume.
        let remaining: Vec<_> = restored
            .into_iter()
            .filter(|ko| ko.topic != topic)
            .collect();
        if !remaining.is_empty() {
            runner
                .kafka_restore_offsets
                .insert(job_id_str.clone(), remaining);
        }
    }

    if manual_commit {
        execute_broker_kafka_two_phase(runner, assignment, source, &sink_path, &source_id, &topic)
            .await
    } else {
        execute_broker_kafka_at_least_once(runner, assignment, source, &sink_path, &source_id).await
    }
}

/// Exactly-once Kafka→Parquet for durable profiles.
///
/// Output is staged through a per-job `EpochTransactionLog` over a
/// `LocalParquetTwoPhaseCommitSink`: batches accumulate in the open
/// transaction, the checkpoint barrier prepares them as `.parquet.tmp` files,
/// and the coordinator's `CheckpointCompleteCommand` renames them into place.
/// Live source offsets are recorded in the task's checkpoint runner so the
/// barrier ack carries them into checkpoint metadata — the checkpoint, not
/// the broker's group offsets, is the recovery authority.
#[cfg(feature = "kafka")]
async fn execute_broker_kafka_two_phase(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    mut source: krishiv_connectors::kafka::RdkafkaKafkaSource,
    sink_path: &std::path::Path,
    source_id: &str,
    topic: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_connectors::{
        CheckpointSource, EpochTransactionLog, LocalParquetTwoPhaseCommitSink, Source,
        TransactionalSinkParticipant as _,
    };

    // The configured sink path is the transactional output directory: each
    // committed file is `<epoch>-<n>.parquet`, staged as `.parquet.tmp`.
    std::fs::create_dir_all(sink_path).map_err(|error| ExecutorError::LocalExecution {
        message: format!(
            "cannot create transactional parquet output dir '{}': {error}",
            sink_path.display()
        ),
    })?;
    let job_id_str = assignment.job_id().as_str().to_owned();
    let sink_dir = sink_path.to_path_buf();
    let participant = runner
        .transaction_log
        .get_or_register(&job_id_str, move || {
            Ok(EpochTransactionLog::new(
                LocalParquetTwoPhaseCommitSink::new(sink_dir),
            ))
        })
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("transactional parquet sink init failed: {error}"),
        })?;

    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    let mut column_count = 0usize;
    loop {
        let Some(batch) =
            source
                .read_batch()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("broker Kafka source read failed: {error}"),
                })?
        else {
            break;
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = batch.num_rows() as u64;
        wait_for_throttle(runner, source_id, rows).await;
        row_count += batch.num_rows();
        batch_count += 1;
        column_count = batch.num_columns();

        participant
            .lock()
            .map_err(|_| ExecutorError::LocalExecution {
                message: format!(
                    "transactional sink lock poisoned for job {job_id_str}; restart the job"
                ),
            })?
            .stage(&batch)
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("Kafka-to-Parquet transactional stage failed: {error}"),
            })?;

        // Record live offsets so the next checkpoint barrier's ack carries
        // them into the checkpoint metadata.
        let offsets =
            source
                .checkpoint_offset()
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("Kafka checkpoint offset read failed: {error}"),
                })?;
        let task_id = assignment.task_id().clone();
        runner
            .checkpoint_runners
            .entry(task_id.clone())
            .or_insert_with(|| crate::runner::TaskRunner::new(task_id))
            .kafka_source_offsets = offsets.offsets;
    }

    if row_count > 0 {
        let staged = participant
            .lock()
            .map_err(|_| ExecutorError::LocalExecution {
                message: format!("transactional sink lock poisoned for job {job_id_str}"),
            })?
            .open_rows();
        tracing::debug!(
            job_id = %assignment.job_id(),
            topic,
            rows = row_count,
            staged_open_rows = staged,
            "Kafka-to-Parquet cycle staged rows; visibility awaits checkpoint commit"
        );
    }

    Ok(ExecutorTaskOutput::connector_pipeline(
        row_count,
        batch_count,
        column_count,
    ))
}

/// At-least-once Kafka→Parquet for non-durable profiles (broker auto-commit).
#[cfg(feature = "kafka")]
async fn execute_broker_kafka_at_least_once(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    mut source: krishiv_connectors::kafka::RdkafkaKafkaSource,
    sink_path: &std::path::Path,
    source_id: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_connectors::kafka::KafkaOffset;
    use krishiv_connectors::parquet::ParquetSink;
    use krishiv_connectors::{Sink, Source};

    let mut sink =
        ParquetSink::create(sink_path).map_err(|error| ExecutorError::LocalExecution {
            message: format!(
                "parquet sink create failed for '{}': {error}",
                sink_path.display()
            ),
        })?;

    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    let mut column_count = 0usize;
    let mut commits = 0usize;
    loop {
        let Some(batch) =
            source
                .read_batch()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("broker Kafka source read failed: {error}"),
                })?
        else {
            break;
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = batch.num_rows() as u64;
        wait_for_throttle(runner, source_id, rows).await;
        row_count += batch.num_rows();
        batch_count += 1;
        column_count = batch.num_columns();
        sink.write_batch(batch)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("Kafka-to-Parquet write failed: {error}"),
            })?;
        sink.flush()
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("Kafka-to-Parquet flush failed: {error}"),
            })?;
        source.commit_offsets();
        commits += 1;
        let _ = source
            .current_offset()
            .and_then(|offset| offset.downcast::<KafkaOffset>().ok());
    }

    if row_count > 0 && commits == 0 {
        return Err(ExecutorError::LocalExecution {
            message: String::from("broker Kafka pipeline wrote rows without committing offsets"),
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

/// Derive a stable u64 shuffle seed from a job ID string.
///
/// Using a per-job seed on `HashPartitioner` prevents adversarial or
/// pathological key distributions from concentrating rows into one bucket
/// across all jobs. The seed is deterministic for the same job ID so
/// retried tasks produce identical partition assignments.
fn shuffle_seed_from_job_id(job_id: &str) -> u64 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(job_id.as_bytes());
    hasher.finish()
}

fn build_hot_key_reports(
    batches: &[arrow::record_batch::RecordBatch],
    key_column: &str,
    job_id: &krishiv_proto::JobId,
    stage_id: &str,
) -> Vec<krishiv_proto::HeartbeatHotKeyReport> {
    use arrow::array::{
        Array, BooleanArray, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::datatypes::DataType;
    use krishiv_dataflow::adaptive::HeavyHittersTracker;

    let mut tracker = HeavyHittersTracker::new(64);
    let key_idx = batches
        .first()
        .and_then(|b| b.schema().index_of(key_column).ok());
    if let Some(kidx) = key_idx {
        for batch in batches {
            let col = batch.column(kidx);
            match col.data_type() {
                DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        for i in 0..arr.len() {
                            if arr.is_valid(i) {
                                tracker.observe(arr.value(i).to_owned());
                            }
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
                        for i in 0..arr.len() {
                            if arr.is_valid(i) {
                                tracker.observe(arr.value(i).to_owned());
                            }
                        }
                    }
                }
                DataType::Utf8View => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                        for i in 0..arr.len() {
                            if arr.is_valid(i) {
                                tracker.observe(arr.value(i).to_owned());
                            }
                        }
                    }
                }
                DataType::Int32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                        for i in 0..arr.len() {
                            if arr.is_valid(i) {
                                tracker.observe(arr.value(i).to_string());
                            }
                        }
                    }
                }
                DataType::Int64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        for i in 0..arr.len() {
                            if arr.is_valid(i) {
                                tracker.observe(arr.value(i).to_string());
                            }
                        }
                    }
                }
                DataType::Boolean => {
                    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        for i in 0..arr.len() {
                            if arr.is_valid(i) {
                                tracker.observe(arr.value(i).to_string());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    tracker
        .hot_keys(0.10)
        .into_iter()
        .map(|report| krishiv_proto::HeartbeatHotKeyReport {
            key: report.key,
            estimated_count: report.estimated_count,
            max_error: report.max_error,
            heat_score: report.heat_score,
            job_id: job_id.clone(),
            source_id: stage_id.to_string(),
        })
        .collect()
}
