//! Batch fragment execution: `execute_batch_fragment` and its helpers.

#[cfg(feature = "kafka")]
use std::path::PathBuf;
use std::sync::Arc;

use krishiv_common::MemoryBudget;
use krishiv_plan::udf::ResourceLimits;
use krishiv_proto::{ExecutorTaskAssignment, TaskRuntimeStats};
#[cfg(feature = "kafka")]
use krishiv_proto::{InputPartitionDescriptor, OutputContract, OutputContractDescriptor};

use futures::StreamExt as _;

use super::common::{
    HotKeyAccumulator, parse_local_parquet_partitions, read_connector_parquet_partitions,
    read_inline_ipc_partitions, read_object_parquet_partitions, read_registry_partitions,
    read_shuffle_flight_partitions, sql_query_from_fragment, task_fragment_body,
};
use crate::runner::{
    ExecutorTaskOutput, ExecutorTaskRunner, OBJECT_PARQUET_SINK_PREFIX, RestoredSourceOffset,
    SHUFFLE_WRITE_PREFIX,
};

/// Register all input partitions from an assignment onto a SQL engine.
///
/// When `registry` is supplied, `registry-connector:` partitions are also
/// resolved through it (CO5 — pluggable connector path).
async fn load_input_tables(
    engine: &Arc<krishiv_sql::SqlEngine>,
    assignment: &krishiv_proto::ExecutorTaskAssignment,
    registry: Option<&krishiv_connectors::ConnectorRegistry>,
    restored_source_offsets: Option<&[RestoredSourceOffset]>,
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
    if let Some(reg) = registry {
        for (table_name, batches) in
            read_registry_partitions(reg, assignment.input_partitions(), restored_source_offsets)
                .await?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|e| crate::ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;
        }
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
    let restored_source_offsets = runner
        .source_restore_offsets
        .get(assignment.job_id().as_str())
        .map(|entry| entry.clone())
        .unwrap_or_default();
    let restored_source_offsets =
        (!restored_source_offsets.is_empty()).then_some(restored_source_offsets.as_slice());
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

    // Phase 52 (ADR-0003): proto-encoded physical-plan stage fragment. Must
    // be dispatched before the generic `shuffle_write` config branch below —
    // dfplan map tasks carry a ShuffleWriteConfig too, but their body is a
    // plan partition, not a `sql:` query.
    if krishiv_sql::distributed_plan::is_dfplan_body(fragment) {
        return execute_dfplan_fragment(
            runner,
            assignment,
            fragment,
            udf_limits.clone(),
            engine_memory_limit,
        )
        .await;
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
                Some(runner.connector_registry.as_ref()),
                restored_source_offsets,
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
                Some(runner.connector_registry.as_ref()),
                restored_source_offsets,
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
        let engine = Arc::new(crate::fragment::common::task_sql_engine(
            engine_memory_limit,
            udf_limits,
        ));
        // Resolve governed `catalog.namespace.table` references (coordinator-mode
        // catalog support): register the platform Iceberg REST catalog from
        // KRISHIV_ICEBERG_REST_* if configured. Non-fatal — a query that does not
        // reference the catalog still runs if the catalog is unreachable.
        if let Err(error) = engine.register_iceberg_rest_catalog_from_env().await {
            tracing::warn!(%error, "iceberg REST catalog registration from env failed");
        }
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
        // CO5: registry-driven connector partitions (e.g. parquet-directory, s3-prefix).
        for (table_name, batches) in read_registry_partitions(
            runner.connector_registry.as_ref(),
            assignment.input_partitions(),
            restored_source_offsets,
        )
        .await?
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

        let is_object_sink = assignment.output_contract().kind()
            == krishiv_proto::OutputContractKind::Sink
            && assignment
                .output_contract()
                .description()
                .trim()
                .starts_with(OBJECT_PARQUET_SINK_PREFIX);

        if is_object_sink {
            // Zero-materialization sink path (#194): stream result batches
            // straight into per-partition incremental parquet writers instead
            // of collecting the full result first. Sink jobs deliver rows
            // through the sink contract, not inline (execute_batch_sql_sink
            // discards report batches), so none ride the task report.
            let (mut stream, stats_handle) =
                dataframe
                    .execute_stream_with_stats()
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: error.to_string(),
                    })?;
            let mut sink = crate::fragment::common::ObjectParquetSinkStream::open(assignment)?;
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
                sink.write(batch).await?;
            }
            let (sink_staged_files, (row_count, batch_count, column_count)) = sink.finish().await?;
            let sql_stats = stats_handle.stats();
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
                ExecutorTaskOutput::sql(row_count, batch_count, column_count)
                    .with_runtime_stats(runtime_stats)
                    .with_sink_staged_files(sink_staged_files),
            );
        }

        // Inline results stream through the spool decision (Phase 2.10):
        // small results stay in memory; large ones overflow to disk and are
        // delivered to the coordinator in bounded PushTaskResult chunks, so
        // executor memory never holds the whole result.
        let (stream, stats_handle) =
            dataframe
                .execute_stream_with_stats()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        let (drained, shape) = crate::runner::result_spool::drain_stream_with_spool(
            stream,
            crate::runner::result_spool::inline_result_max_bytes(),
        )
        .await?;
        let sql_stats = stats_handle.stats();
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
        let output =
            ExecutorTaskOutput::sql(shape.row_count, shape.batch_count, shape.column_count)
                .with_runtime_stats(runtime_stats);
        return Ok(match drained {
            crate::runner::result_spool::DrainedResult::Inline(batches) => {
                output.with_record_batches(batches)
            }
            crate::runner::result_spool::DrainedResult::Spooled(spool) => {
                tracing::info!(
                    total_bytes = spool.total_bytes(),
                    rows = shape.row_count,
                    "task result exceeded inline threshold; spooled to disk"
                );
                output.with_spooled_result(std::sync::Arc::new(spool))
            }
        });
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
    registry: Option<&krishiv_connectors::ConnectorRegistry>,
    restored_source_offsets: Option<&[RestoredSourceOffset]>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    // Parse "hash:<key_column>:<num_partitions>"
    let parts: Vec<&str> = spec.splitn(3, ':').collect();
    if parts.len() != 3 || parts.first().copied() != Some("hash") {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "shuffle-write spec must be 'hash:<key_column>:<num_partitions>', got '{spec}'"
            ),
        });
    }
    let key_column = parts.get(1).copied().unwrap_or("").trim();
    let part2 = parts.get(2).copied().unwrap_or("");
    let num_partitions: u32 =
        part2
            .trim()
            .parse()
            .map_err(|_| ExecutorError::InvalidAssignment {
                message: format!("shuffle-write num_partitions is not a valid u32: '{part2}'"),
            })?;
    if key_column.is_empty() || num_partitions == 0 {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("shuffle-write key_column and num_partitions must be non-empty"),
        });
    }
    if num_partitions > 10_000 {
        return Err(ExecutorError::InvalidAssignment {
            message: format!(
                "shuffle-write num_partitions {num_partitions} exceeds maximum of 10,000"
            ),
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
    let limited_engine = Arc::new(crate::fragment::common::task_sql_engine(
        engine_memory_limit,
        udf_limits,
    ));
    // Coordinator-mode catalog support: register the platform Iceberg REST
    // catalog from KRISHIV_ICEBERG_REST_* so governed tables resolve. Non-fatal.
    if let Err(error) = limited_engine
        .register_iceberg_rest_catalog_from_env()
        .await
    {
        tracing::warn!(%error, "iceberg REST catalog registration from env failed");
    }
    load_input_tables(
        &limited_engine,
        assignment,
        registry,
        restored_source_offsets,
    )
    .await?;

    let dataframe = limited_engine
        .sql(query)
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;
    let mut sql_stream =
        dataframe
            .execute_stream()
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
    let mut output_schema: arrow::datatypes::SchemaRef =
        Arc::new(arrow::datatypes::Schema::empty());
    let mut hot_key_acc = HotKeyAccumulator::new();
    let mut ess_writer: Option<krishiv_shuffle::SortShuffleWriter> = if ctx.ess_index.is_some() {
        Some(
            krishiv_shuffle::SortShuffleWriter::new(
                job_id,
                stage_id,
                key_column,
                num_partitions,
                &ctx.local_dir,
            )
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("ESS sort-shuffle writer init failed: {e}"),
            })?,
        )
    } else {
        None
    };

    while let Some(result) = sql_stream.next().await {
        let batch = result.map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;
        if batch.num_rows() == 0 {
            continue;
        }
        if output_schema.fields().is_empty() {
            output_schema = batch.schema();
        }
        total_rows += batch.num_rows();
        hot_key_acc.observe_batch(&batch, key_column);
        if let Some(w) = &mut ess_writer {
            w.push(batch.clone())
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("ESS sort-shuffle push failed: {e}"),
                })?;
        }
        let buckets = partitioner
            .partition(&batch)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("hash partition failed: {e}"),
            })?;
        for (bucket_idx, bucket_batch) in buckets.into_iter().enumerate() {
            if bucket_batch.num_rows() > 0
                && let Some(v) = partition_batches.get_mut(bucket_idx)
            {
                v.push(bucket_batch);
            }
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
        let rows_written: u64 = part_batches.iter().map(|b| b.num_rows() as u64).sum();

        // DB-3: coalesce K sub-batches (one per source batch) into one well-sized
        // batch before writing to the shuffle store. Improves downstream columnar
        // throughput and reduces per-batch overhead in the store.
        let part_batches = if part_batches.len() > 1 {
            arrow::compute::concat_batches(&schema, &part_batches)
                .map(|b| vec![b])
                .unwrap_or(part_batches)
        } else {
            part_batches
        };

        // T12: if a push-shuffle store is wired, serialise partition to IPC
        // before transferring ownership to write_partition.
        if let Some(ps) = ctx.push_store.as_ref() {
            use arrow::ipc::writer::StreamWriter;
            let mut ipc_bytes: Vec<u8> = Vec::new();
            if !part_batches.is_empty() {
                let mut w = StreamWriter::try_new(&mut ipc_bytes, &schema).map_err(|e| {
                    ExecutorError::LocalExecution {
                        message: format!("push-shuffle ipc writer init failed: {e}"),
                    }
                })?;
                for batch in &part_batches {
                    w.write(batch).map_err(|e| ExecutorError::LocalExecution {
                        message: format!("push-shuffle ipc write failed: {e}"),
                    })?;
                }
                w.finish().map_err(|e| ExecutorError::LocalExecution {
                    message: format!("push-shuffle ipc finish failed: {e}"),
                })?;
            }
            if !ipc_bytes.is_empty()
                && let Err(e) = ps.push(job_id, stage_id, p, ipc_bytes)
            {
                tracing::warn!(error = %e, "shuffle push_store.push returned error");
            }
        }

        let partition = ShufflePartition {
            id,
            schema,
            batches: part_batches,
        };

        // T19: time the shuffle write and increment the bytes / rows /
        // time counters. The `write_partition` call is async; we measure
        // wall-clock around it so the metric reflects end-to-end write
        // time (serialise + IO).
        let write_started = std::time::Instant::now();
        ctx.store
            .write_partition(partition, lease_token)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("shuffle write failed for partition {p}: {e}"),
            })?;
        let write_elapsed_us = write_started.elapsed().as_micros() as u64;
        krishiv_metrics::global_metrics().add_shuffle_bytes_written(size_bytes);
        krishiv_metrics::global_metrics().add_shuffle_records_written(rows_written);
        krishiv_metrics::global_metrics().add_shuffle_write_time_us(write_elapsed_us);
        outputs.push(krishiv_proto::ShufflePartitionOutput::new(
            p,
            size_bytes,
            ctx.flight_endpoint.clone(),
        ));
    }

    // ESS: flush the sort-writer that was fed inline during the streaming pass.
    // AQE T7: patch outputs with real on-disk byte sizes.
    if let (Some(ess_index), Some(sort_writer)) = (&ctx.ess_index, ess_writer) {
        let files = sort_writer
            .flush()
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("ESS sort-shuffle flush failed: {e}"),
            })?;
        if let Ok(offsets) = files.read_offsets() {
            for (p, output_entry) in outputs.iter_mut().enumerate() {
                if let (Some(&off_p), Some(&off_p1)) = (offsets.get(p), offsets.get(p + 1)) {
                    let real_bytes = off_p1.saturating_sub(off_p);
                    let endpoint = output_entry.flight_endpoint.clone();
                    *output_entry =
                        krishiv_proto::ShufflePartitionOutput::new(p as u32, real_bytes, endpoint);
                }
            }
        }
        ess_index.register(job_id, stage_id, files);
    }

    let hot_key_reports = hot_key_acc.into_reports(assignment.job_id(), stage_id);

    let mut output = ExecutorTaskOutput::shuffle_write(total_rows, outputs);
    output.hot_key_reports = hot_key_reports;
    Ok(output)
}

/// Shuffle reads for decoded dfplan fragments, keyed by the
/// `shuffle_stage_key(stage, map_task)` wire contract shared with the
/// coordinator's staged-job builder.
///
/// Partitions written on this executor are read from the local store (a
/// local miss reads as empty — map tasks write every partition, including
/// empty ones). Partitions whose map task ran on another executor are
/// fetched over Arrow Flight from the locations the coordinator attached to
/// the assignment (`InputPartitionDescriptor::ShuffleFlight`); there a
/// missing partition is an error, never silently empty.
struct InmemDfplanShuffleReader {
    store: std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
    job_id: String,
    /// `(sub-stage key, partition) → flight endpoint` for remote partitions.
    remote_endpoints: std::collections::HashMap<(String, u32), String>,
}

// Manual impl: `ShuffleBackend` itself does not derive Debug.
impl std::fmt::Debug for InmemDfplanShuffleReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InmemDfplanShuffleReader")
            .field("job_id", &self.job_id)
            .finish_non_exhaustive()
    }
}

impl krishiv_sql::distributed_plan::ShufflePartitionReader for InmemDfplanShuffleReader {
    fn read_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<Vec<arrow::record_batch::RecordBatch>, String>>
    {
        use krishiv_shuffle::{PartitionId, ShuffleStore as _};
        let partition = match u32::try_from(partition) {
            Ok(p) => p,
            Err(_) => {
                return Box::pin(async move {
                    Err(format!("shuffle partition index {partition} exceeds u32"))
                });
            }
        };
        let stage_key =
            krishiv_sql::distributed_plan::shuffle_stage_key(upstream_stage_index, map_task_index);

        if let Some(endpoint) = self.remote_endpoints.get(&(stage_key.clone(), partition)) {
            let endpoint = endpoint.clone();
            let job_id = self.job_id.clone();
            return Box::pin(async move {
                let _permit = super::common::SHUFFLE_FETCH_SEMAPHORE
                    .acquire()
                    .await
                    .map_err(|_| String::from("shuffle fetch semaphore closed"))?;
                krishiv_shuffle::flight::FlightShuffleClient::fetch_with_retry(
                    &endpoint,
                    &job_id,
                    &stage_key,
                    partition,
                    krishiv_shuffle::flight::FetchRetryPolicy::from_env(),
                )
                .await
                .map_err(|e| {
                    format!(
                        "dfplan shuffle-flight fetch failed (endpoint={endpoint} \
                         stage={stage_key} partition={partition}): {e}"
                    )
                })
            });
        }

        let id = PartitionId {
            job_id: self.job_id.clone(),
            stage_id: stage_key,
            partition,
        };
        let store = std::sync::Arc::clone(&self.store);
        Box::pin(async move {
            store
                .read_partition(&id)
                .await
                .map(|found| found.map(|p| p.batches).unwrap_or_default())
                .map_err(|e| e.to_string())
        })
    }
}

/// Execute a Phase 52 `dfplan:v1:` fragment: one output partition of a
/// proto-encoded physical-plan stage subtree (ADR-0003).
///
/// Map tasks (the assignment carries a `ShuffleWriteConfig`) hash-partition
/// the partition's output into the in-memory shuffle store under the task's
/// sub-stage key; Result tasks stream through the inline/spool decision
/// exactly like `sql:` results. No SQL is parsed or planned here — the plan
/// arrives fully optimized from the coordinator.
async fn execute_dfplan_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
    udf_limits: ResourceLimits,
    engine_memory_limit: Option<usize>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    let store = runner
        .inmem_shuffle
        .clone()
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: String::from(
                "dfplan fragment requires an in-memory shuffle store but none is configured",
            ),
        })?;
    // The engine supplies the runtime environment (memory pool, spill) the
    // decoded plan executes under; no tables are registered on it.
    let engine = Arc::new(crate::fragment::common::task_sql_engine(
        engine_memory_limit,
        udf_limits,
    ));
    let job_id = assignment.job_id().as_str();
    // This executor's advertised shuffle endpoint: partitions recorded under
    // it (or under no endpoint) are local; everything else is fetched.
    let own_endpoint = runner
        .shuffle
        .as_ref()
        .map(|c| c.flight_endpoint.clone())
        .unwrap_or_default();
    let remote_endpoints: std::collections::HashMap<(String, u32), String> = assignment
        .input_partitions()
        .iter()
        .filter_map(|p| match p.descriptor() {
            Some(krishiv_proto::InputPartitionDescriptor::ShuffleFlight {
                flight_endpoint,
                upstream_stage_id,
                partition_id,
                ..
            }) if !flight_endpoint.is_empty() && *flight_endpoint != own_endpoint => Some((
                (upstream_stage_id.as_str().to_owned(), *partition_id),
                flight_endpoint.clone(),
            )),
            _ => None,
        })
        .collect();
    let reader: Arc<dyn krishiv_sql::distributed_plan::ShufflePartitionReader> =
        Arc::new(InmemDfplanShuffleReader {
            store: Arc::clone(&store),
            job_id: job_id.to_owned(),
            remote_endpoints,
        });
    let (schema, mut stream) = krishiv_sql::distributed_plan::execute_dfplan_body(
        fragment,
        engine.session_context(),
        Some(reader),
    )
    .map_err(|e| ExecutorError::InvalidAssignment {
        message: format!("dfplan fragment: {e}"),
    })?;

    if let Some(write_cfg) = assignment.shuffle_write() {
        // Map task: hash-partition the stream into the shuffle store under
        // this task's sub-stage key (mirrors `execute_inmem_shuffle_write`,
        // which owns the `sql:`-body variant of the same protocol).
        let num_partitions = write_cfg.num_partitions.max(1) as u32;
        let key_column = write_cfg.key_columns.first().map(String::as_str);
        let partitioner = key_column.map(|col| {
            HashPartitioner::new(col, num_partitions).with_seed(shuffle_seed_from_job_id(job_id))
        });
        let mut partition_batches: Vec<Vec<arrow::record_batch::RecordBatch>> =
            vec![Vec::new(); num_partitions as usize];
        let mut total_rows = 0usize;
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            total_rows += batch.num_rows();
            if let Some(p) = &partitioner {
                let buckets = p
                    .partition(&batch)
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!("dfplan hash partition failed: {e}"),
                    })?;
                for (bucket_idx, bucket_batch) in buckets.into_iter().enumerate() {
                    if bucket_batch.num_rows() > 0
                        && let Some(v) = partition_batches.get_mut(bucket_idx)
                    {
                        v.push(bucket_batch);
                    }
                }
            } else if let Some(v) = partition_batches.first_mut() {
                v.push(batch);
            }
        }

        let mut outputs: Vec<krishiv_proto::ShufflePartitionOutput> =
            Vec::with_capacity(num_partitions as usize);
        for (p, part_batches) in partition_batches.into_iter().enumerate() {
            let p = p as u32;
            let part_schema = part_batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::clone(&schema));
            let size_bytes: u64 = part_batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .sum();
            let part_batches = if part_batches.len() > 1 {
                arrow::compute::concat_batches(&part_schema, &part_batches)
                    .map(|b| vec![b])
                    .unwrap_or(part_batches)
            } else {
                part_batches
            };
            store
                .write_partition(
                    ShufflePartition {
                        id: PartitionId {
                            job_id: job_id.to_owned(),
                            stage_id: write_cfg.stage_id.as_str().to_owned(),
                            partition: p,
                        },
                        schema: part_schema,
                        batches: part_batches,
                    },
                    write_cfg.lease_token,
                )
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("dfplan shuffle write failed for partition {p}: {e}"),
                })?;
            // Advertise this executor's shuffle flight endpoint so the
            // coordinator can route downstream tasks on other executors
            // here ("" = in-process only, local store reads).
            outputs.push(krishiv_proto::ShufflePartitionOutput::new(
                p,
                size_bytes,
                own_endpoint.as_str(),
            ));
        }
        return Ok(ExecutorTaskOutput::shuffle_write(total_rows, outputs));
    }

    // Result task: stream through the inline/spool decision (Phase 2.10).
    let (drained, shape) = crate::runner::result_spool::drain_stream_with_spool(
        stream,
        crate::runner::result_spool::inline_result_max_bytes(),
    )
    .await?;
    let output = ExecutorTaskOutput::sql(shape.row_count, shape.batch_count, shape.column_count);
    Ok(match drained {
        crate::runner::result_spool::DrainedResult::Inline(batches) => {
            output.with_record_batches(batches)
        }
        crate::runner::result_spool::DrainedResult::Spooled(spool) => {
            tracing::info!(
                total_bytes = spool.total_bytes(),
                rows = shape.row_count,
                "dfplan result exceeded inline threshold; spooled to disk"
            );
            output.with_spooled_result(std::sync::Arc::new(spool))
        }
    })
}

/// Execute a typed R4a shuffle-write task backed by `InMemoryShuffleStore`.
async fn execute_inmem_shuffle_write(
    assignment: &ExecutorTaskAssignment,
    write_cfg: &krishiv_proto::ShuffleWriteConfig,
    store: &std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
    udf_limits: ResourceLimits,
    engine_memory_limit: Option<usize>,
    registry: Option<&krishiv_connectors::ConnectorRegistry>,
    restored_source_offsets: Option<&[RestoredSourceOffset]>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    let fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
    // Create a new SQL engine with UDF limits and the task's memory limit.
    let limited_engine = Arc::new(crate::fragment::common::task_sql_engine(
        engine_memory_limit,
        udf_limits,
    ));
    // Coordinator-mode catalog support: register the platform Iceberg REST
    // catalog from KRISHIV_ICEBERG_REST_* so governed tables resolve. Non-fatal.
    if let Err(error) = limited_engine
        .register_iceberg_rest_catalog_from_env()
        .await
    {
        tracing::warn!(%error, "iceberg REST catalog registration from env failed");
    }
    let num_partitions = write_cfg.num_partitions as u32;
    let lease_token = write_cfg.lease_token;
    let job_id = assignment.job_id().as_str();
    let stage_id = write_cfg.stage_id.as_str();
    let key_column = write_cfg.key_columns.first().map(String::as_str);

    let mut partition_batches: Vec<Vec<arrow::record_batch::RecordBatch>> =
        vec![Vec::new(); num_partitions as usize];
    let mut total_rows: usize = 0;
    let mut output_schema: arrow::datatypes::SchemaRef =
        std::sync::Arc::new(arrow::datatypes::Schema::empty());
    let mut hot_key_acc = HotKeyAccumulator::new();

    if let Some(query) = sql_query_from_fragment(&fragment_body) {
        load_input_tables(
            &limited_engine,
            assignment,
            registry,
            restored_source_offsets,
        )
        .await?;
        let dataframe =
            limited_engine
                .sql(query)
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;
        let mut sql_stream =
            dataframe
                .execute_stream()
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: e.to_string(),
                })?;

        let partitioner = key_column.map(|col| {
            HashPartitioner::new(col, num_partitions).with_seed(shuffle_seed_from_job_id(job_id))
        });

        while let Some(result) = sql_stream.next().await {
            let batch = result.map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
            total_rows += batch.num_rows();
            if num_partitions == 0 || batch.num_rows() == 0 {
                continue;
            }
            if output_schema.fields().is_empty() {
                output_schema = batch.schema();
            }
            hot_key_acc.observe_batch(&batch, key_column.unwrap_or(""));
            if let Some(p) = &partitioner {
                let buckets = p
                    .partition(&batch)
                    .map_err(|e| ExecutorError::LocalExecution {
                        message: format!("hash partition failed: {e}"),
                    })?;
                for (bucket_idx, bucket_batch) in buckets.into_iter().enumerate() {
                    if bucket_batch.num_rows() > 0
                        && let Some(v) = partition_batches.get_mut(bucket_idx)
                    {
                        v.push(bucket_batch);
                    }
                }
            } else if let Some(v) = partition_batches.first_mut() {
                v.push(batch);
            }
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
        let part_batches = if part_batches.len() > 1 {
            arrow::compute::concat_batches(&schema, &part_batches)
                .map(|b| vec![b])
                .unwrap_or(part_batches)
        } else {
            part_batches
        };
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

    let hot_key_reports = hot_key_acc.into_reports(assignment.job_id(), stage_id);

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

    // T19: time the shuffle read and increment the bytes / rows / time
    // counters. Local reads (`store.read_partition`) are intra-process;
    // we count them as `local_blocks_fetched`.
    let read_started = std::time::Instant::now();
    let fetch_started = std::time::Instant::now();
    let partition = store
        .read_partition(&id)
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: format!(
                "R4a in-memory shuffle read failed for partition {}: {e}",
                read_cfg.partition_id
            ),
        })?;
    let fetch_wait_us = fetch_started.elapsed().as_micros() as u64;

    let batches = partition.map(|p| p.batches).unwrap_or_default();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let bytes_read: u64 = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let read_elapsed_us = read_started.elapsed().as_micros() as u64;
    krishiv_metrics::global_metrics().add_shuffle_read_bytes(bytes_read);
    krishiv_metrics::global_metrics().add_shuffle_read_records(row_count as u64);
    krishiv_metrics::global_metrics().add_shuffle_read_time_us(read_elapsed_us);
    krishiv_metrics::global_metrics().add_shuffle_fetch_wait_time_us(fetch_wait_us);
    krishiv_metrics::global_metrics().add_shuffle_local_blocks_fetched(1);
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
            .or_insert_with(|| {
                std::sync::Arc::new(std::sync::Mutex::new(crate::runner::TaskRunner::new(
                    task_id,
                )))
            })
            .lock()
            .unwrap_or_else(|p| p.into_inner())
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
    tokio::fs::create_dir_all(sink_path)
        .await
        .map_err(|error| ExecutorError::LocalExecution {
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
            .or_insert_with(|| {
                std::sync::Arc::new(std::sync::Mutex::new(crate::runner::TaskRunner::new(
                    task_id,
                )))
            })
            .lock()
            .unwrap_or_else(|p| p.into_inner())
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_shuffle::{
        InMemoryShuffleStore, LocalDiskShuffleStore, PartitionId, ShuffleBackend, ShufflePartition,
        ShuffleStore as _,
    };
    use krishiv_sql::distributed_plan::{ShufflePartitionReader as _, shuffle_stage_key};

    fn shuffle_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 8, 9]))]).unwrap()
    }

    /// Leg 3 residual: partitions whose map task ran on another executor
    /// must arrive over Arrow Flight from the coordinator-attached endpoint
    /// — a local read would silently return empty and corrupt results.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn dfplan_reader_fetches_remote_partition_over_flight() {
        let stage_key = shuffle_stage_key(0, 0);

        // "Remote" executor: a disk shuffle store served over Flight.
        let dir = tempfile::tempdir().unwrap();
        let remote_store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let batch = shuffle_batch();
        let id = PartitionId {
            job_id: "job-dfplan-flight".to_owned(),
            stage_id: stage_key.clone(),
            partition: 3,
        };
        remote_store
            .register_partition_lease(id.clone(), 1)
            .await
            .unwrap();
        remote_store
            .write_partition(
                ShufflePartition {
                    id,
                    schema: batch.schema(),
                    batches: vec![batch],
                },
                1,
            )
            .await
            .unwrap();
        let (addr, server) = krishiv_shuffle::flight::serve(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&remote_store),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // "Local" executor: empty local store; the coordinator attached the
        // remote location for (sub-stage, partition).
        let reader = InmemDfplanShuffleReader {
            store: Arc::new(ShuffleBackend::InMemory(Arc::new(
                InMemoryShuffleStore::new(),
            ))),
            job_id: "job-dfplan-flight".to_owned(),
            remote_endpoints: std::collections::HashMap::from([(
                (stage_key, 3u32),
                addr.to_string(),
            )]),
        };
        let batches = reader.read_partition(0, 0, 3).await.unwrap();
        server.abort();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    /// Partitions with no remote location read from the local store, where a
    /// miss is empty by contract (map tasks write every partition, including
    /// empty ones).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn dfplan_reader_local_miss_reads_empty() {
        let reader = InmemDfplanShuffleReader {
            store: Arc::new(ShuffleBackend::InMemory(Arc::new(
                InMemoryShuffleStore::new(),
            ))),
            job_id: "job-dfplan-local".to_owned(),
            remote_endpoints: std::collections::HashMap::new(),
        };
        let batches = reader.read_partition(0, 0, 0).await.unwrap();
        assert!(batches.is_empty());
    }
}
