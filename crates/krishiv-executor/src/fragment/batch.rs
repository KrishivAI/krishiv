//! Batch fragment execution: `execute_batch_fragment` and its helpers.

#[cfg(feature = "kafka")]
use std::path::PathBuf;
use std::sync::Arc;

use krishiv_common::MemoryBudget;

// All three map-side shuffle-write paths in this file
// (`execute_shuffle_write_fragment`, `execute_dfplan_fragment`,
// `execute_inmem_shuffle_write`) buffer their hash-partitioned output through
// `crate::fragment::shuffle_write_buffer::ShuffleWriteBuffer`. They are wired
// to the same type deliberately: the previous guard here was open-coded three
// times, an earlier fix patched only one copy, and the executors kept being
// OOM-killed through the other two with the new error never appearing in their
// logs because the code that raised it was not the code that ran.
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
use krishiv_sql::distributed_plan::ShuffleFragmentStream;

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
    for (table_name, batches) in crate::erased(read_connector_parquet_partitions(
        assignment.input_partitions(),
    ))
    .await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in crate::erased(read_object_parquet_partitions(
        assignment.input_partitions(),
    ))
    .await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    for (table_name, batches) in crate::erased(read_shuffle_flight_partitions(
        assignment.input_partitions(),
    ))
    .await?
    {
        engine
            .register_record_batches(&table_name, batches)
            .await
            .map_err(|e| crate::ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
    }
    if let Some(reg) = registry {
        for (table_name, batches) in crate::erased(read_registry_partitions(
            reg,
            assignment.input_partitions(),
            restored_source_offsets,
        ))
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
/// Split leading `/* krishiv-register-python-udf(a)f:… */` directive comment(s)
/// off the front of a fragment body. Returns the joined directives and the
/// remaining body. Staged Python-UDF fragments prepend these ahead of their
/// `dfplan:` body so the executor can register the worker-backed UDF before
/// decoding the plan; every other fragment has no leading comment and comes back
/// unchanged (the single-task `sql:` body keeps its directive after `sql:`).
fn split_leading_python_udf_directives(body: &str) -> (String, String) {
    const CLOSE: &str = " */";
    let mut directives: Vec<&str> = Vec::new();
    let mut rest = body.trim_start();
    loop {
        let is_directive = rest.starts_with("/* krishiv-register-python-udf:")
            || rest.starts_with("/* krishiv-register-python-udaf:");
        if !is_directive {
            break;
        }
        let Some(end) = rest.find(CLOSE) else { break };
        directives.push(&rest[..end + CLOSE.len()]);
        rest = rest[end + CLOSE.len()..].trim_start();
    }
    (directives.join("\n"), rest.to_string())
}

pub(crate) async fn execute_batch_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    udf_limits: ResourceLimits,
    memory_budget: Arc<MemoryBudget>,
) -> ExecutorResult<ExecutorTaskOutput> {
    // Reserve this task's share of the executor process memory budget for the
    // duration of the fragment; the guard releases the share on return.
    let (engine_memory, _process_memory_reservation) =
        crate::fragment::common::reserve_task_engine_memory(&memory_budget);
    let raw_fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
    // A staged Python-UDF fragment carries the `/* krishiv-register-python-udf */`
    // directive(s) ahead of its `dfplan:` body so this executor can reconstruct
    // the worker-backed UDF before decoding the plan. Split them off here; the
    // single-task `sql:` path keeps its directive inside the SQL body (handled by
    // register_python_udfs_from_sql) and is unaffected (no leading comment).
    let (python_udf_directives, fragment_body) =
        split_leading_python_udf_directives(&raw_fragment_body);
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
            return crate::erased(execute_inmem_shuffle_read(assignment, read_cfg, store)).await;
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
        return crate::erased(execute_dfplan_fragment(
            runner,
            assignment,
            fragment,
            &python_udf_directives,
            udf_limits.clone(),
            engine_memory,
        ))
        .await;
    }

    #[cfg(feature = "kafka")]
    if fragment == KAFKA_TO_PARQUET_FRAGMENT {
        return crate::erased(execute_source_to_sink_pipeline(runner, assignment)).await;
    }

    if let Some(shuffle_spec) = fragment.strip_prefix(SHUFFLE_WRITE_PREFIX) {
        if let Some(ctx) = &runner.shuffle {
            return crate::erased(execute_shuffle_write_fragment(
                assignment,
                shuffle_spec,
                ctx,
                udf_limits.clone(),
                engine_memory,
                Some(runner.connector_registry.as_ref()),
                restored_source_offsets,
            ))
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
            return crate::erased(execute_inmem_shuffle_write(
                assignment,
                write_cfg,
                store,
                udf_limits.clone(),
                engine_memory,
                Some(runner.connector_registry.as_ref()),
                restored_source_offsets,
            ))
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
            engine_memory,
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
        for (table_name, batches) in crate::erased(read_connector_parquet_partitions(
            assignment.input_partitions(),
        ))
        .await?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }
        for (table_name, batches) in crate::erased(read_object_parquet_partitions(
            assignment.input_partitions(),
        ))
        .await?
        {
            engine
                .register_record_batches(&table_name, batches)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
        }
        for (table_name, batches) in crate::erased(read_shuffle_flight_partitions(
            assignment.input_partitions(),
        ))
        .await?
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

        // Register any Python UDFs shipped in the fragment SQL before planning.
        let query = engine
            .register_python_udfs_from_sql(query)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: error.to_string(),
            })?;
        let dataframe =
            engine
                .sql(&query)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;

        // #197 / Phase 67 export leg: a `registry-sink:` contract streams the
        // task's result batches into any registered connector sink (s3-files,
        // jdbc-sink, elasticsearch, …) through the one connector registry —
        // the batch-export counterpart to the streaming Iceberg/Kafka sinks.
        let output_description = assignment.output_contract().description().trim().to_owned();
        let is_registry_sink = assignment.output_contract().kind()
            == krishiv_proto::OutputContractKind::Sink
            && output_description.starts_with(crate::runner::REGISTRY_SINK_PREFIX);
        if is_registry_sink {
            return execute_registry_sink(runner, &dataframe, &output_description).await;
        }

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
        return crate::erased(execute_window_fragment(rest, assignment)).await;
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
    engine_memory: krishiv_sql::EngineMemory,
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
        engine_memory,
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
    crate::erased(load_input_tables(
        &limited_engine,
        assignment,
        registry,
        restored_source_offsets,
    ))
    .await?;

    // Register any Python UDFs shipped in the fragment before planning.
    let query = limited_engine
        .register_python_udfs_from_sql(query)
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;
    let dataframe = crate::erased(limited_engine.sql(&query))
        .await
        .map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;
    let (physical_output_schema, mut sql_stream) =
        crate::erased(dataframe.execute_stream_with_schema())
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

    // Bounded, pool-accounted, spilling map-side buffer — see
    // `shuffle_write_buffer` for why holding the whole map output in a plain
    // `Vec<Vec<RecordBatch>>` OOM-killed executors at SF100.
    let mut buffer = crate::fragment::shuffle_write_buffer::ShuffleWriteBuffer::for_task(
        num_partitions as usize,
        &limited_engine,
        Some(ctx.local_dir.clone()),
    );
    let mut total_rows: usize = 0;
    // A2 (review 2026-07-27): this was `Schema::empty()`, replaced only by the
    // first NON-EMPTY batch. A map task whose entire input is filtered out
    // therefore wrote every one of its partitions with a zero-column schema,
    // while the reduce side declares the coordinator's schema regardless
    // (`RecordBatchStreamAdapter::new` does not validate that yielded batches
    // match it). Downstream operators then received batches whose schema
    // disagreed with the plan.
    //
    // Take it from the *stream*, not from the DataFrame. The first version of
    // this fix used `DataFrame::schema()`, which is the plan's **logical**
    // schema, and the two disagree wherever physical planning re-types an
    // expression: TPC-H q17's `avg(l_quantity)` is `Decimal128(15, 2)`
    // logically and `Decimal128(30, 15)` in the batches the stream actually
    // yields, so every partition was written with a schema its own rows
    // violated — "column types must match schema types". The stream's schema
    // is the physical one, is known before the first batch, and is correct
    // when there are no batches at all, which is the case A2 exists for.
    let mut output_schema: arrow::datatypes::SchemaRef =
        physical_output_schema;
    let mut hot_key_acc = HotKeyAccumulator::new();
    let mut ess_writer: Option<krishiv_shuffle::SortShuffleWriter> = if ctx.ess_index.is_some() {
        Some(
            // The SAME partitioner the store path below uses. These two writers
            // see identical batches and must agree on where each row goes; when
            // the ESS writer built its own it used seed 0 against this path's
            // job-derived seed, so the ESS index and the store placed the same
            // row in different partitions.
            krishiv_shuffle::SortShuffleWriter::new(
                job_id,
                stage_id,
                partitioner.clone(),
                key_column,
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
            buffer.push(bucket_idx, bucket_batch).await?;
        }
    }

    let mut outputs: Vec<krishiv_proto::ShufflePartitionOutput> =
        Vec::with_capacity(num_partitions as usize);

    // One schema for this task's whole output, latched from the first batch
    // that carried data — the same rule `drain_into_store` follows, and for
    // the same reason. Deciding per partition made a partition's schema depend
    // on whether it happened to receive rows: full ones took the *produced*
    // schema, empty ones took the plan's *declared* one, and physical
    // execution re-types expressions, so the two disagree (q17 declares
    // `Decimal128(15, 2)` for `avg(l_quantity)` and produces
    // `Decimal128(30, 15)`). The reduce side concatenates across map tasks and
    // rejects the mixture.
    let observed_schema = buffer.pushed_schema();
    super::shuffle_write_buffer::warn_on_schema_divergence(observed_schema.as_ref(), &output_schema);
    for p in 0..num_partitions {
        // `_reservation` must outlive `part_batches` — it is the pool's view
        // of this partition while it is concatenated and serialised.
        let (part_batches, _reservation) = buffer.drain_partition(p as usize).await?.into_parts();
        let id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id: stage_id.to_owned(),
            partition: p,
        };
        let schema = observed_schema
            .clone()
            .unwrap_or_else(|| output_schema.clone());
        // Not `get_array_memory_size()`: `take` on a Utf8View column leaves
        // every partition pointing at the SAME shared data buffers, so that
        // charges each one the whole buffer — measured 38.32x over 47 buckets.
        // AQE sizes reduce parallelism from this number.
        let size_bytes: u64 = krishiv_shuffle::logical_partition_bytes(&part_batches);
        let rows_written: u64 = part_batches.iter().map(|b| b.num_rows() as u64).sum();

        // DB-3: coalesce sub-batches into well-sized batches before writing to
        // the shuffle store, bounded by a byte target rather than swallowing
        // the whole partition — see `coalesce_shuffle_batches`.
        let part_batches =
            super::shuffle_write_buffer::coalesce_shuffle_batches(part_batches, &schema);

        // T12: if a push-shuffle store is wired, serialise partition to IPC
        // before transferring ownership to write_partition.
        if let Some(ps) = ctx.push_store.as_ref() {
            use arrow::ipc::writer::StreamWriter;
            // Sized up front, and deliberately.
            //
            // This buffer is a *second* full copy of the partition, living
            // alongside the Arrow batches until `ps.push` takes it — so this
            // loop's true peak is roughly twice a partition, and only the Arrow
            // half is covered by `_reservation`. A default `Vec` reaches its
            // final size by repeated doubling, and each growth copies, so the
            // unaccounted half briefly costs more than the copy itself.
            //
            // `size_bytes` is the partition's logical size, which is the right
            // order of magnitude for its IPC encoding: this removes the realloc
            // churn without pretending to be exact. Capped so a bad estimate
            // cannot turn a hint into a huge speculative allocation.
            const IPC_RESERVE_CAP_BYTES: u64 = 256 * 1024 * 1024;
            let mut ipc_bytes: Vec<u8> =
                Vec::with_capacity(usize::try_from(size_bytes.min(IPC_RESERVE_CAP_BYTES)).unwrap_or(0));
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
            // The one moment both copies are live, reported because nothing
            // else can see it: `_reservation` covers the Arrow batches, and an
            // IPC `Vec<u8>` is invisible to the pool.
            if !ipc_bytes.is_empty() {
                tracing::debug!(
                    partition = p,
                    arrow_bytes = size_bytes,
                    ipc_bytes = ipc_bytes.len(),
                    "shuffle write: partition held as Arrow and IPC simultaneously; \
                     only the Arrow half is pool-accounted"
                );
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
/// Partitions whose map task ran on another executor are fetched over Arrow
/// Flight from the locations the coordinator attached to the assignment
/// (`InputPartitionDescriptor::ShuffleFlight`). Everything else is read from
/// the local store.
///
/// A1: a local miss is an ERROR naming the partition, never an empty read.
/// The reduce side has three cases and only two used to be distinguishable —
/// "written here and genuinely empty", "written here and lost", and "written
/// on another executor and the coordinator never told me" all produced zero
/// rows and `Ok`. The third is silent data loss on a query that reports
/// success, which only a digest comparison would ever catch, and it is
/// reachable whenever a producer reports an empty endpoint (an executor that
/// came up before its shuffle Flight listener was configured, a restarted
/// container that lost `--shuffle-addr`). It is not the same as "genuinely
/// empty": map tasks publish every partition including the empty ones, so a
/// legitimately-empty partition reads back as `Some(0 batches)`, and only a
/// partition that was never written — or was deleted underneath us — reads
/// back as `None`.
///
/// The error carries the `KRV_SHUFFLE_MISSING` marker so the coordinator
/// regenerates the producer rather than failing outright; C1/C2 bound that
/// loop and diagnose it when it does not converge.
struct InmemDfplanShuffleReader {
    store: std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
    job_id: String,
    /// `(sub-stage key, partition) → flight endpoint` for remote partitions.
    remote_endpoints: std::collections::HashMap<(String, u32), String>,
    /// `(sub-stage key, partition)` the coordinator attached to THIS executor's
    /// endpoint — positive evidence that a local read is the right branch,
    /// rather than the absence of evidence for any other one.
    local_partitions: std::collections::HashSet<(String, u32)>,
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
    fn open_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<ShuffleFragmentStream, String>> {
        use futures::StreamExt as _;
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
                // The permit covers the *open*, and is released when this future
                // returns — deliberately NOT held for the stream's lifetime.
                //
                // Holding it for the stream deadlocks. `ShuffleReadExec` opens
                // several fragments ahead (`buffered`) and consumes them in
                // order, so an open-but-unconsumed stream would keep its permit
                // until downstream consumption reached it. A plan with two
                // shuffle reads feeding an operator that interleaves its inputs
                // then hangs: side A opens enough fragments to take every permit,
                // side B blocks forever waiting for one, and A's permits are only
                // released by consumption that is now blocked on B. The
                // collecting fetch this replaced could not deadlock, because its
                // permit scope ended when the fetch returned regardless of what
                // the consumer did.
                //
                // The looser bound is also sufficient now. What the semaphore was
                // protecting against was concurrent *materialisation* — each
                // fetch used to build a whole fragment in memory. A streaming
                // fetch's open-but-unconsumed cost is an HTTP/2 flow-control
                // window, tens of kilobytes, so resident bytes no longer scale
                // with the number of open streams. A bound that can deadlock is
                // worse than a bound that is merely loose.
                let permit = super::common::SHUFFLE_FETCH_SEMAPHORE
                    .acquire()
                    .await
                    .map_err(|_| String::from("shuffle fetch semaphore closed"))?;
                let stream = krishiv_shuffle::flight::FlightShuffleClient::open_with_retry(
                    &endpoint,
                    &job_id,
                    &stage_key,
                    partition,
                    krishiv_shuffle::flight::FetchRetryPolicy::from_env(),
                )
                .await
                .map_err(|e| {
                    // A NotFound here means the producer executor is gone (see
                    // FlightShuffleClient::open_with_retry, which maps an
                    // exhausted transport retry to NotFound). The reader trait
                    // is String-typed, so embed the structured missing-partition
                    // marker the task runner recovers via
                    // collect_missing_shuffle_partitions — the coordinator then
                    // regenerates this producer instead of the job failing.
                    if e.kind() == std::io::ErrorKind::NotFound {
                        format!(
                            "{}: {e}",
                            crate::runner::encode_missing_shuffle(&stage_key, partition)
                        )
                    } else {
                        format!(
                            "dfplan shuffle-flight fetch failed (endpoint={endpoint} \
                             stage={stage_key} partition={partition}): {e}"
                        )
                    }
                })?;
                drop(permit);
                Ok(Box::pin(stream.map(|b| b.map_err(|e| e.to_string())))
                    as ShuffleFragmentStream)
            });
        }

        // A1: say which of the three cases this is, in the error, before the
        // read — after it the distinction is gone.
        let attached_locally = self
            .local_partitions
            .contains(&(stage_key.clone(), partition));
        let attached_for_stage = self
            .remote_endpoints
            .keys()
            .chain(self.local_partitions.iter())
            .any(|(key, _)| key == &stage_key);
        let id = PartitionId {
            job_id: self.job_id.clone(),
            stage_id: stage_key.clone(),
            partition,
        };
        let store = std::sync::Arc::clone(&self.store);
        Box::pin(async move {
            // A local read streams too: the file is on this node's disk, so
            // there is no reason to build the whole fragment in anonymous
            // memory before the first batch can be joined.
            let found = store.stream_partition(&id).await.map_err(|e| e.to_string())?;
            match found {
                Some(p) => Ok(Box::pin(p.batches.map(|b| b.map_err(|e| e.to_string())))
                    as ShuffleFragmentStream),
                None => {
                    let provenance = if attached_locally {
                        "the coordinator located it on THIS executor, so the local store lost it"
                    } else if attached_for_stage {
                        "the coordinator attached locations for this stage key but none for this \
                         partition — the producer reported no endpoint for it (A1)"
                    } else {
                        "the coordinator attached no location for this stage key at all — either \
                         the producer advertised no shuffle endpoint, or it ran here and its \
                         output is gone (A1)"
                    };
                    Err(format!(
                        "{}: shuffle partition job={} stage={} partition={} is not in the local \
                         store and has no attached remote location; {provenance}. Reading it as \
                         empty would silently drop rows from a query that reports success.",
                        crate::runner::encode_missing_shuffle(&stage_key, partition),
                        id.job_id,
                        stage_key,
                        partition,
                    ))
                }
            }
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
    python_udf_directives: &str,
    udf_limits: ResourceLimits,
    engine_memory: krishiv_sql::EngineMemory,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId};

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
        engine_memory,
        udf_limits,
    ));
    // Register any Python scalar UDF the staged fragment carries BEFORE decoding
    // the plan: the serialized dfplan references the UDF by name, so it must
    // exist in this engine's function registry for decode to resolve it.
    if !python_udf_directives.is_empty() {
        engine
            .register_python_udfs_from_sql(python_udf_directives)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("register staged python UDF: {e}"),
            })?;
    }
    let job_id = assignment.job_id().as_str();
    // This executor's advertised shuffle endpoint: partitions recorded under
    // it (or under no endpoint) are local; everything else is fetched.
    let own_endpoint = runner
        .shuffle
        .as_ref()
        .map(|c| c.flight_endpoint.clone())
        .unwrap_or_default();
    let mut remote_endpoints: std::collections::HashMap<(String, u32), String> =
        std::collections::HashMap::new();
    let mut local_partitions: std::collections::HashSet<(String, u32)> =
        std::collections::HashSet::new();
    for input in assignment.input_partitions() {
        let Some(krishiv_proto::InputPartitionDescriptor::ShuffleFlight {
            flight_endpoint,
            upstream_stage_id,
            partition_id,
            ..
        }) = input.descriptor()
        else {
            continue;
        };
        let key = (upstream_stage_id.as_str().to_owned(), *partition_id);
        // A1: an endpoint equal to ours is positive evidence that the partition
        // is here, not merely the absence of evidence that it is elsewhere.
        // Recording it separately is what lets a local miss say which of the
        // three reduce-side cases it is.
        if flight_endpoint.is_empty() || *flight_endpoint == own_endpoint {
            local_partitions.insert(key);
        } else {
            remote_endpoints.insert(key, flight_endpoint.clone());
        }
    }
    let reader: Arc<dyn krishiv_sql::distributed_plan::ShufflePartitionReader> =
        Arc::new(InmemDfplanShuffleReader {
            store: Arc::clone(&store),
            job_id: job_id.to_owned(),
            remote_endpoints,
            local_partitions,
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
        // Every declared key column, in order — not just the first. Hashing
        // `key_columns[0]` alone is co-location-correct but collapses a
        // composite key onto its leading column's cardinality; see
        // `HashPartitioner`.
        let partitioner = (!write_cfg.key_columns.is_empty()).then(|| {
            HashPartitioner::new_multi(write_cfg.key_columns.clone(), num_partitions)
                .with_seed(shuffle_seed_from_job_id(job_id))
        });
        // Bounded, pool-accounted, spilling map-side buffer. The
        // `Vec<Vec<RecordBatch>>` this replaces held the task's ENTIRE map
        // output — `write_partition` takes a whole partition, so there was
        // nowhere to flush part-way through — and was invisible to the
        // DataFusion pool. At SF100 the `dist-s4` fragment of q8/q9 (the raw
        // lineitem scan, hash-partitioned by `l_partkey`, no join to prune it)
        // put ~2 GiB of Arrow data in that buffer per task, RSS walked past
        // the cgroup limit while the pool still reported headroom, and the
        // kernel SIGKILLed the executor. See `shuffle_write_buffer` for the
        // full evidence.
        let mut buffer = crate::fragment::shuffle_write_buffer::ShuffleWriteBuffer::for_task(
            num_partitions as usize,
            &engine,
            runner.shuffle.as_ref().map(|c| c.local_dir.clone()),
        );
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
                    buffer.push(bucket_idx, bucket_batch).await?;
                }
            } else {
                buffer.push(0, batch).await?;
            }
        }

        // Publishes the WHOLE partition space, empties included — see
        // `drain_into_store` for why a skipped empty partition strands its
        // remote consumer until the job's regeneration budget runs out.
        let stage_id = write_cfg.stage_id.as_str().to_owned();
        let stats = crate::fragment::shuffle_write_buffer::drain_into_store(
            &mut buffer,
            store.as_ref(),
            move |partition| PartitionId {
                job_id: job_id.to_owned(),
                stage_id: stage_id.clone(),
                partition,
            },
            &schema,
            write_cfg.lease_token,
            |_, _| Ok(()),
        )
        .await?;
        // Advertise this executor's shuffle flight endpoint so the coordinator
        // can route downstream tasks on other executors here ("" = in-process
        // only, local store reads).
        let outputs: Vec<krishiv_proto::ShufflePartitionOutput> = stats
            .iter()
            .map(|s| {
                krishiv_proto::ShufflePartitionOutput::new(
                    s.partition,
                    s.size_bytes,
                    own_endpoint.as_str(),
                )
            })
            .collect();
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
    engine_memory: krishiv_sql::EngineMemory,
    registry: Option<&krishiv_connectors::ConnectorRegistry>,
    restored_source_offsets: Option<&[RestoredSourceOffset]>,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_shuffle::{HashPartitioner, PartitionId, ShufflePartition, ShuffleStore as _};

    let fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
    // Create a new SQL engine with UDF limits and the task's memory limit.
    let limited_engine = Arc::new(crate::fragment::common::task_sql_engine(
        engine_memory,
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

    // Bounded, pool-accounted, spilling map-side buffer — see
    // `shuffle_write_buffer`. `inmem_shuffle` is a misnomer on a real
    // deployment: the executor wires the SAME disk-backed store here, so the
    // whole-output buffer this replaces was the executor's peak heap on this
    // path too.
    let mut buffer = crate::fragment::shuffle_write_buffer::ShuffleWriteBuffer::for_task(
        num_partitions as usize,
        &limited_engine,
        None,
    );
    let mut total_rows: usize = 0;
    let mut output_schema: arrow::datatypes::SchemaRef =
        std::sync::Arc::new(arrow::datatypes::Schema::empty());
    let mut hot_key_acc = HotKeyAccumulator::new();

    if let Some(query) = sql_query_from_fragment(&fragment_body) {
        crate::erased(load_input_tables(
            &limited_engine,
            assignment,
            registry,
            restored_source_offsets,
        ))
        .await?;
        let query = limited_engine
            .register_python_udfs_from_sql(query)
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: e.to_string(),
            })?;
        let dataframe = crate::erased(limited_engine.sql(&query))
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

        // All declared key columns, in order — see `HashPartitioner`. The
        // hot-key accumulator below still observes only the leading column: it
        // is a skew *detector*, and a single column is what it can summarise.
        let partitioner = (!write_cfg.key_columns.is_empty()).then(|| {
            HashPartitioner::new_multi(write_cfg.key_columns.clone(), num_partitions)
                .with_seed(shuffle_seed_from_job_id(job_id))
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
                    buffer.push(bucket_idx, bucket_batch).await?;
                }
            } else {
                buffer.push(0, batch).await?;
            }
        }
    }

    let mut outputs: Vec<krishiv_proto::ShufflePartitionOutput> =
        Vec::with_capacity(num_partitions as usize);

    // One schema for this task's whole output — see the identical latch in the
    // shuffle-write path above and in `drain_into_store`.
    let observed_schema = buffer.pushed_schema();
    super::shuffle_write_buffer::warn_on_schema_divergence(observed_schema.as_ref(), &output_schema);
    for p in 0..num_partitions {
        // `_reservation` must outlive `part_batches` — it is the pool's view
        // of this partition while it is concatenated and serialised.
        let (part_batches, _reservation) = buffer.drain_partition(p as usize).await?.into_parts();
        let id = PartitionId {
            job_id: job_id.to_owned(),
            stage_id: stage_id.to_owned(),
            partition: p,
        };
        let schema = observed_schema
            .clone()
            .unwrap_or_else(|| output_schema.clone());
        // Not `get_array_memory_size()`: `take` on a Utf8View column leaves
        // every partition pointing at the SAME shared data buffers, so that
        // charges each one the whole buffer — measured 38.32x over 47 buckets.
        // AQE sizes reduce parallelism from this number.
        let size_bytes: u64 = krishiv_shuffle::logical_partition_bytes(&part_batches);
        let part_batches =
            super::shuffle_write_buffer::coalesce_shuffle_batches(part_batches, &schema);
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
        return crate::erased(execute_broker_kafka_to_parquet(runner, assignment, profile)).await;
    }
    crate::erased(execute_memory_kafka_to_parquet(runner, assignment)).await
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
        crate::erased(execute_broker_kafka_two_phase(
            runner, assignment, source, &sink_path, &source_id, &topic,
        ))
        .await
    } else {
        crate::erased(execute_broker_kafka_at_least_once(
            runner, assignment, source, &sink_path, &source_id,
        ))
        .await
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

/// Parse a `registry-sink:<kind>|<base64(config-json)>` output contract into a
/// [`ConnectorConfig`] the connector registry can open (#197 / Phase 67).
///
/// The config JSON is `{"name": "...", "properties": {"k": "v", …}}`. It is
/// base64-encoded on the wire so arbitrary property values (paths, URLs,
/// containing `|`/`:`) cannot corrupt the contract framing. Property values are
/// coerced to strings; a non-string JSON value is serialized back to its JSON
/// text so nothing is silently dropped.
pub(crate) fn parse_registry_sink_contract(
    description: &str,
) -> Result<krishiv_connectors::config::ConnectorConfig, String> {
    use base64::Engine as _;
    use krishiv_connectors::config::ConnectorConfig;

    let payload = description
        .trim()
        .strip_prefix(crate::runner::REGISTRY_SINK_PREFIX)
        .ok_or("registry-sink contract missing prefix")?;
    let (kind, encoded) = payload
        .split_once('|')
        .ok_or("registry-sink contract must be <kind>|<base64-json>")?;
    let kind = kind.trim();
    if kind.is_empty() {
        return Err("registry-sink contract missing connector kind".into());
    }
    let json_bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|e| format!("registry-sink config base64: {e}"))?;
    let value: serde_json::Value = serde_json::from_slice(&json_bytes)
        .map_err(|e| format!("registry-sink config json: {e}"))?;
    let name = value
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("registry-sink-export");
    let mut config = ConnectorConfig::new(name, kind);
    if let Some(props) = value.get("properties").and_then(|v| v.as_object()) {
        for (key, val) in props {
            let val_str = match val.as_str() {
                Some(s) => s.to_owned(),
                None => val.to_string(),
            };
            config = config.with_property(key, val_str);
        }
    }
    Ok(config)
}

/// Stream a batch SQL result into a registry-dispatched connector sink and
/// report the row count (#197 / Phase 67 batch export). The sink is opened
/// through the executor's connector registry, so availability is decided by
/// which drivers are registered — an unregistered kind fails the task cleanly
/// rather than silently discarding output.
async fn execute_registry_sink(
    runner: &ExecutorTaskRunner,
    dataframe: &krishiv_sql::SqlDataFrame,
    output_description: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    let config = parse_registry_sink_contract(output_description)
        .map_err(|message| ExecutorError::InvalidAssignment { message })?;

    let mut sink = runner
        .connector_registry
        .open_sink(&config)
        .await
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("registry-sink open ({}): {error}", config.kind),
        })?;

    let (mut stream, stats_handle) =
        dataframe
            .execute_stream_with_stats()
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: error.to_string(),
            })?;

    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    let mut column_count = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| ExecutorError::LocalExecution {
            message: error.to_string(),
        })?;
        row_count += batch.num_rows();
        batch_count += 1;
        column_count = batch.num_columns();
        sink.write_batch_dyn(batch)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("registry-sink write ({}): {error}", config.kind),
            })?;
    }
    // Flush is mandatory before the task reports success: it is what makes the
    // output durable, so a write/flush failure fails the task (post-write
    // offset-commit contract) rather than acknowledging unwritten output.
    sink.flush_dyn()
        .await
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("registry-sink flush ({}): {error}", config.kind),
        })?;

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
    Ok(
        ExecutorTaskOutput::sql(row_count, batch_count, column_count)
            .with_runtime_stats(runtime_stats),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    // ── #197 / Phase 67: registry-sink batch export ──────────────────────────

    /// Build a `registry-sink:<kind>|<base64-json>` contract string the same way
    /// a coordinator/platform would emit one.
    fn registry_sink_contract(kind: &str, name: &str, props: &[(&str, &str)]) -> String {
        use base64::Engine as _;
        let properties: serde_json::Map<String, serde_json::Value> = props
            .iter()
            .map(|(k, v)| ((*k).to_owned(), serde_json::Value::String((*v).to_owned())))
            .collect();
        let json = serde_json::json!({ "name": name, "properties": properties });
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&json).unwrap());
        format!("{}{kind}|{encoded}", crate::runner::REGISTRY_SINK_PREFIX)
    }

    #[test]
    fn parse_registry_sink_contract_round_trips_kind_and_properties() {
        let contract = registry_sink_contract(
            "s3",
            "orders-export",
            &[("path", "s3://bucket/x|y"), ("format", "parquet")],
        );
        let config = parse_registry_sink_contract(&contract).expect("parse");
        assert_eq!(config.kind, "s3");
        assert_eq!(config.name, "orders-export");
        // A value containing the framing char survives because the JSON is b64'd.
        assert_eq!(config.get("path"), Some("s3://bucket/x|y"));
        assert_eq!(config.get("format"), Some("parquet"));
    }

    #[test]
    fn parse_registry_sink_contract_rejects_malformed() {
        assert!(parse_registry_sink_contract("not-a-registry-sink").is_err());
        assert!(parse_registry_sink_contract("registry-sink:s3").is_err()); // no |payload
        assert!(parse_registry_sink_contract("registry-sink:|abcd").is_err()); // empty kind
        assert!(parse_registry_sink_contract("registry-sink:s3|not-base64!!").is_err());
    }

    /// A collecting sink + driver so the export path can be exercised without an
    /// external system: every written batch is captured for assertion.
    #[derive(Clone, Default)]
    struct CollectingSink {
        batches: std::sync::Arc<std::sync::Mutex<Vec<RecordBatch>>>,
        flushed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl krishiv_connectors::sink::Sink for CollectingSink {
        fn capabilities(&self) -> krishiv_connectors::ConnectorCapabilities {
            krishiv_connectors::ConnectorCapabilities::new().with_idempotent()
        }
        fn write_batch(
            &mut self,
            batch: RecordBatch,
        ) -> impl std::future::Future<Output = krishiv_connectors::error::ConnectorResult<()>> + Send
        {
            let batches = self.batches.clone();
            async move {
                batches.lock().unwrap().push(batch);
                Ok(())
            }
        }
        fn flush(
            &mut self,
        ) -> impl std::future::Future<Output = krishiv_connectors::error::ConnectorResult<()>> + Send
        {
            let flushed = self.flushed.clone();
            async move {
                flushed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }
    }

    struct CollectingSinkDriver {
        sink: CollectingSink,
    }

    impl krishiv_connectors::registry::SinkDriver for CollectingSinkDriver {
        fn descriptor(&self) -> krishiv_connectors::registry::ConnectorDescriptor {
            krishiv_connectors::registry::ConnectorDescriptor::new(
                krishiv_connectors::registry::ConnectorKind::S3,
                krishiv_connectors::registry::ConnectorRole::Sink,
                krishiv_connectors::ConnectorCapabilities::new().with_idempotent(),
            )
        }
        fn validate(
            &self,
            _config: &krishiv_connectors::config::ConnectorConfig,
        ) -> krishiv_connectors::error::ConnectorResult<()> {
            Ok(())
        }
        fn open<'a>(
            &'a self,
            _config: &'a krishiv_connectors::config::ConnectorConfig,
        ) -> krishiv_connectors::registry::OpenSinkFuture<'a> {
            let sink = self.sink.clone();
            Box::pin(
                async move { Ok(Box::new(sink) as Box<dyn krishiv_connectors::sink::DynSink>) },
            )
        }
    }

    /// End-to-end: a batch SQL fragment with a `registry-sink:` output contract
    /// opens the registered sink and streams its result rows into it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_sink_batch_export_writes_result_rows_to_the_sink() {
        use krishiv_proto::{
            AttemptId, ExecutorId, JobId, LeaseGeneration, OutputContract, OutputContractKind,
            PlanFragment, StageId, TaskAttemptRef, TaskId,
        };

        let sink = CollectingSink::default();
        let mut registry = krishiv_connectors::ConnectorRegistry::new();
        registry.register_sink(std::sync::Arc::new(CollectingSinkDriver {
            sink: sink.clone(),
        }));
        let runner = crate::runner::ExecutorTaskRunner::new(crate::ExecutorAssignmentInbox::new())
            .with_connector_registry(registry);

        let fragment = krishiv_plan::TypedTaskFragment::new(
            krishiv_plan::ExecutionKind::Batch,
            "sql: SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a",
        )
        .encode()
        .unwrap();
        let contract = registry_sink_contract("s3", "export", &[("path", "s3://b/out")]);
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-registry-sink").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(fragment),
            OutputContract::new(OutputContractKind::Sink, contract),
        );

        let output = runner.execute_batch_fragment(&assignment).await.unwrap();
        assert_eq!(output.row_count(), 3, "all result rows reported");

        let written: usize = sink
            .batches
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(written, 3, "all result rows streamed into the sink");
        assert!(
            sink.flushed.load(std::sync::atomic::Ordering::SeqCst),
            "sink must be flushed before the task reports success"
        );
    }

    /// An unregistered sink kind fails the task cleanly instead of silently
    /// discarding output.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_sink_unregistered_kind_fails_the_task() {
        use krishiv_proto::{
            AttemptId, ExecutorId, JobId, LeaseGeneration, OutputContract, OutputContractKind,
            PlanFragment, StageId, TaskAttemptRef, TaskId,
        };
        // Empty registry: no driver registered for "s3".
        let runner = crate::runner::ExecutorTaskRunner::new(crate::ExecutorAssignmentInbox::new())
            .with_connector_registry(krishiv_connectors::ConnectorRegistry::new());
        let fragment = krishiv_plan::TypedTaskFragment::new(
            krishiv_plan::ExecutionKind::Batch,
            "sql: SELECT 1 AS a",
        )
        .encode()
        .unwrap();
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-registry-sink-missing").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(fragment),
            OutputContract::new(
                OutputContractKind::Sink,
                registry_sink_contract("s3", "export", &[]),
            ),
        );
        let result = runner.execute_batch_fragment(&assignment).await;
        assert!(result.is_err(), "unregistered sink kind must fail the task");
    }
    use krishiv_shuffle::{
        InMemoryShuffleStore, LocalDiskShuffleStore, PartitionId, ShuffleBackend, ShufflePartition,
        ShuffleStore as _,
    };
    use krishiv_sql::distributed_plan::{ShufflePartitionReader as _, shuffle_stage_key};

    /// Open a fragment and drain it, so tests can keep asserting on a
    /// `Vec<RecordBatch>` now that the reader streams.
    ///
    /// Collecting here is exactly what the production path must NOT do, which is
    /// the point: the test wants every batch in hand to assert on, and does not
    /// care about residency.
    async fn read_fragment(
        reader: &InmemDfplanShuffleReader,
        stage: usize,
        map_task: usize,
        partition: usize,
    ) -> Result<Vec<RecordBatch>, String> {
        use futures::TryStreamExt as _;
        reader
            .open_partition(stage, map_task, partition)
            .await?
            .try_collect()
            .await
    }

    fn shuffle_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 8, 9]))]).unwrap()
    }

    /// More fragments may be OPEN at once than the executor-wide fetch
    /// semaphore has permits.
    ///
    /// This pins the fix for a deadlock introduced when the read path started
    /// streaming. `ShuffleReadExec` opens several fragments ahead and consumes
    /// them in order, so if a permit were held for the *stream's* lifetime an
    /// open-but-unconsumed fragment would keep it until downstream consumption
    /// reached it. A plan with two shuffle reads feeding an operator that
    /// interleaves its inputs then hangs forever: side A takes every permit,
    /// side B blocks waiting for one, and A's permits are only released by
    /// consumption that is now blocked on B.
    ///
    /// The semaphore here is deliberately smaller than the number of fragments
    /// opened, which is the condition that would hang. If this test ever times
    /// out, someone has re-tied the permit to the stream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn more_fragments_can_be_open_than_the_fetch_semaphore_has_permits() {
        use futures::TryStreamExt as _;
        let dir = tempfile::tempdir().unwrap();
        let remote_store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let batch = shuffle_batch();
        // Four map tasks, each with partition 0 written.
        let map_tasks = 4usize;
        for map_task in 0..map_tasks {
            let id = PartitionId {
                job_id: "job-permit".to_owned(),
                stage_id: shuffle_stage_key(0, map_task),
                partition: 0,
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
                        batches: vec![batch.clone()],
                    },
                    1,
                )
                .await
                .unwrap();
        }
        let (addr, server) = krishiv_shuffle::flight::serve(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&remote_store),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let endpoint = format!("127.0.0.1:{}", addr.port());
        let reader = InmemDfplanShuffleReader {
            store: Arc::new(ShuffleBackend::InMemory(Arc::new(
                InMemoryShuffleStore::new(),
            ))),
            job_id: "job-permit".to_owned(),
            remote_endpoints: (0..map_tasks)
                .map(|m| ((shuffle_stage_key(0, m), 0u32), endpoint.clone()))
                .collect(),
            local_partitions: std::collections::HashSet::new(),
        };

        // Open every fragment BEFORE consuming any of them. With a permit held
        // for the stream's lifetime this is the deadlock: the semaphore's
        // default is 8, but the shape is what matters, so hold all four open at
        // once and only then drain them.
        let mut opened = Vec::new();
        for map_task in 0..map_tasks {
            opened.push(
                tokio::time::timeout(
                    std::time::Duration::from_secs(20),
                    reader.open_partition(0, map_task, 0),
                )
                .await
                .expect("opening a fragment must not block on unconsumed streams")
                .unwrap(),
            );
        }
        let mut rows = 0usize;
        for stream in opened {
            let batches: Vec<RecordBatch> = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                stream.try_collect(),
            )
            .await
            .expect("draining an opened fragment must not block")
            .unwrap();
            rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(rows, map_tasks * 3, "every opened fragment must deliver its rows");
        server.abort();
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
            local_partitions: std::collections::HashSet::new(),
        };
        let batches = read_fragment(&reader, 0, 0, 3).await.unwrap();
        server.abort();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    /// A1: a partition with no attached location and nothing in the local
    /// store is an ERROR naming it, never a silent empty read.
    ///
    /// This test used to assert the opposite — that a local miss reads empty —
    /// which made "written on another executor and the coordinator never told
    /// me" indistinguishable from "written here and genuinely empty". Both
    /// produced zero rows and `Ok`, so the query reported success with rows
    /// missing and only a digest comparison would ever have noticed. A
    /// legitimately-empty partition is not affected: map tasks publish every
    /// partition, so it reads back as `Some(0 batches)`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn dfplan_reader_local_miss_is_an_error_naming_the_partition() {
        let reader = InmemDfplanShuffleReader {
            store: Arc::new(ShuffleBackend::InMemory(Arc::new(
                InMemoryShuffleStore::new(),
            ))),
            job_id: "job-dfplan-local".to_owned(),
            remote_endpoints: std::collections::HashMap::new(),
            local_partitions: std::collections::HashSet::new(),
        };
        let error = read_fragment(&reader, 0, 0, 0)
            .await
            .expect_err("an unlocatable partition must not read as empty");
        for needle in [
            "KRV_SHUFFLE_MISSING",
            &shuffle_stage_key(0, 0),
            "job-dfplan-local",
            "partition=0",
        ] {
            assert!(
                error.contains(needle),
                "the error must name {needle}; got: {error}"
            );
        }
    }

    /// The half of A1 that must NOT change: a partition the map task wrote on
    /// this executor still reads locally, and an empty one still reads as zero
    /// rows rather than erroring. Written through the real disk store so the
    /// "published but empty" case is genuinely on disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn dfplan_reader_reads_a_published_empty_partition_locally() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let backend = Arc::new(ShuffleBackend::Local(Arc::clone(&disk)));
        let stage_key = shuffle_stage_key(0, 0);

        // Partition 0 receives no rows; the map path publishes it anyway.
        map_write_through_production_path(
            &backend,
            "job-dfplan-empty-local",
            &stage_key,
            2,
            64 * 1024 * 1024,
            dir.path().to_path_buf(),
            &[0, 64],
        )
        .await;

        let reader = InmemDfplanShuffleReader {
            store: backend,
            job_id: "job-dfplan-empty-local".to_owned(),
            remote_endpoints: std::collections::HashMap::new(),
            local_partitions: std::collections::HashSet::from([(stage_key, 0u32)]),
        };
        let batches = read_fragment(&reader, 0, 0, 0)
            .await
            .expect("a published-but-empty partition must still read locally");
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            0,
            "a genuinely empty partition reads as zero rows, not as an error"
        );
    }

    /// Build the map-side buffer, feed it, and drain it into `store` through
    /// the same `drain_into_store` the three production map-write paths use.
    ///
    /// Taking the production function (rather than re-typing its loop) is what
    /// keeps these tests from passing vacuously against a drain that skips
    /// partitions.
    #[allow(clippy::unwrap_used)]
    async fn map_write_through_production_path(
        store: &Arc<ShuffleBackend>,
        job_id: &str,
        stage_key: &str,
        num_partitions: usize,
        soft_limit_bytes: u64,
        spill_dir: std::path::PathBuf,
        rows_per_partition: &[usize],
    ) -> (
        Vec<crate::fragment::shuffle_write_buffer::PartitionWriteStat>,
        usize,
    ) {
        use crate::fragment::shuffle_write_buffer::{ShuffleWriteBuffer, drain_into_store};

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mut buffer = ShuffleWriteBuffer::new(
            num_partitions,
            Some(krishiv_sql::EngineMemory::shared_pool(64 * 1024 * 1024)),
            soft_limit_bytes,
            spill_dir,
        );
        for (partition, rows) in rows_per_partition.iter().enumerate() {
            // Several batches per partition so the spill variant has something
            // to split across runs.
            for chunk in 0..4usize {
                if *rows == 0 {
                    continue;
                }
                let values: Vec<i64> = (0..*rows as i64)
                    .map(|i| (partition as i64) * 1_000_000 + (chunk as i64) * 1_000 + i)
                    .collect();
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .unwrap();
                buffer.push(partition, batch).await.unwrap();
            }
        }
        let spills = buffer.spill_count();
        let job = job_id.to_owned();
        let stage = stage_key.to_owned();
        let stats = drain_into_store(
            &mut buffer,
            store,
            move |partition| PartitionId {
                job_id: job.clone(),
                stage_id: stage.clone(),
                partition,
            },
            &schema,
            1,
            |_, _| Ok(()),
        )
        .await
        .unwrap();
        (stats, spills)
    }

    /// Serve `store` over Flight and return a reader that fetches every
    /// partition of `stage_key` REMOTELY — the case where a partition the map
    /// task never wrote is an error rather than an empty read.
    #[allow(clippy::unwrap_used)]
    async fn remote_reader_for(
        store: &Arc<LocalDiskShuffleStore>,
        job_id: &str,
        stage_key: &str,
        num_partitions: u32,
    ) -> (InmemDfplanShuffleReader, tokio::task::JoinHandle<()>) {
        let (addr, server) =
            krishiv_shuffle::flight::serve("127.0.0.1:0".parse().unwrap(), Arc::clone(store))
                .await
                .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let remote_endpoints = (0..num_partitions)
            .map(|p| ((stage_key.to_owned(), p), addr.to_string()))
            .collect();
        (
            InmemDfplanShuffleReader {
                store: Arc::new(ShuffleBackend::InMemory(Arc::new(
                    InMemoryShuffleStore::new(),
                ))),
                job_id: job_id.to_owned(),
                remote_endpoints,
                local_partitions: std::collections::HashSet::new(),
            },
            server,
        )
    }

    /// TPC-H q3's live failure, as a test.
    ///
    /// A map task whose hash layout leaves a partition with zero rows must
    /// still publish that partition. Its remote consumer fetches over Flight,
    /// where "never written" is an error, not an empty read — so a skipped
    /// empty partition makes the consumer report a missing upstream, the
    /// coordinator regenerate the producer, and the producer reproduce the
    /// same gap until the regeneration budget is exhausted and the job fails.
    ///
    /// The assertion is on the REMOTE read specifically: a local read returns
    /// empty on a miss and would pass whether or not the partition exists,
    /// which is exactly the vacuous version of this test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn map_write_publishes_partitions_that_received_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let backend = Arc::new(ShuffleBackend::Local(Arc::clone(&disk)));
        let stage_key = shuffle_stage_key(0, 0);

        // Partitions 1 and 3 get rows; 0 and 2 get none — the q3 shape.
        let (stats, spills) = map_write_through_production_path(
            &backend,
            "job-empty-partition",
            &stage_key,
            4,
            64 * 1024 * 1024,
            dir.path().to_path_buf(),
            &[0, 128, 0, 128],
        )
        .await;
        assert_eq!(spills, 0, "this case must not spill; it isolates emptiness");
        assert_eq!(stats.len(), 4, "every partition must be published");

        let (reader, server) = remote_reader_for(&disk, "job-empty-partition", &stage_key, 4).await;
        let mut results = Vec::new();
        for partition in 0..4usize {
            results.push(read_fragment(&reader, 0, 0, partition).await);
        }
        server.abort();

        for (partition, result) in results.iter().enumerate() {
            let batches = result.as_ref().unwrap_or_else(|e| {
                panic!(
                    "partition {partition} must be fetchable remotely, got missing/error: {e}. \
                     A map task that skips its empty partitions strands its consumer forever."
                )
            });
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let expected = if partition % 2 == 0 { 0 } else { 128 * 4 };
            assert_eq!(rows, expected, "partition {partition} row count");
        }
    }

    /// The same contract once the buffer has actually spilled: partitions that
    /// went to disk must come back whole, and partitions that were empty must
    /// still be published rather than lost among the spilled ones.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::unwrap_used)]
    async fn spilled_map_write_lands_every_partition_in_the_served_store() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let backend = Arc::new(ShuffleBackend::Local(Arc::clone(&disk)));
        let stage_key = shuffle_stage_key(1, 2);

        // A 4 KiB ceiling against ~48 KiB of payload forces repeated spills.
        let (stats, spills) = map_write_through_production_path(
            &backend,
            "job-spilled-partition",
            &stage_key,
            4,
            4 * 1024,
            dir.path().to_path_buf(),
            &[512, 0, 512, 512],
        )
        .await;
        assert!(spills > 0, "test must actually exercise the spill path");
        assert_eq!(stats.len(), 4, "every partition must be published");

        let (reader, server) =
            remote_reader_for(&disk, "job-spilled-partition", &stage_key, 4).await;
        let mut results = Vec::new();
        for partition in 0..4usize {
            results.push(read_fragment(&reader, 1, 2, partition).await);
        }
        server.abort();

        for (partition, result) in results.iter().enumerate() {
            let batches = result
                .as_ref()
                .unwrap_or_else(|e| panic!("partition {partition} missing after spill: {e}"));
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let expected = if partition == 1 { 0 } else { 512 * 4 };
            assert_eq!(
                rows, expected,
                "partition {partition} lost rows across the spill boundary"
            );
        }
    }
}
