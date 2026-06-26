//! Streaming fragment execution: unified bounded window path (all window kinds).

use std::sync::{Arc, Mutex};

use krishiv_common::MemoryBudget;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_plan::udf::ResourceLimits;

use crate::fragment::common::{build_hot_key_reports, task_fragment_body};
use crate::runner::{ExecutorTaskOutput, ExecutorTaskRunner};
use crate::{ExecutorError, ExecutorResult};
use krishiv_dataflow::execute_bounded_window;
use krishiv_plan::window::{WindowAggKind, WindowExecutionSpec, decode_window_execution_spec};
use krishiv_proto::ExecutorTaskAssignment;

const STREAM_KAFKA_PARTITION_PREFIX: &str = "stream-kafka:";

/// Fragment prefix for CEP sequential pattern execution — re-exported from `krishiv_plan`.
const STREAM_CEP_PREFIX: &str = krishiv_plan::cep::STREAM_CEP_PREFIX;

/// Fragment prefix for continuous window loop execution (GAP-6).
///
/// Format: `stream:loop:<job_id>|<window_fragment>` where `<window_fragment>`
/// is a full encoded window spec as produced by
/// `krishiv_plan::window::encode_stream_fragment` (e.g.
/// `stream:tw:key=user_id:time=ts:win=10000:lag=1000:agg=count`).
///
/// On each invocation the executor:
///  1. Looks up (or creates) a per-job `ContinuousWindowExecutor` stored in
///     `runner.loop_executors`.  State is retained across calls so partial
///     windows accumulate correctly.
///  2. Reads newly arrived input from the local continuous drainer or from
///     coordinator-delivered inline IPC partitions.
///  3. Passes the batches through `ContinuousWindowExecutor::drain()`.
///  4. Returns any newly emitted (closed) window batches.
const STREAM_LOOP_PREFIX: &str = "stream:loop:";

/// Fragment prefix for ST8 stream-to-stream watermark join.
///
/// Format: `window-join:<json>` where `<json>` is a serialised
/// [`WatermarkWindowJoinSpec`][krishiv_dataflow::WatermarkWindowJoinSpec].
const WINDOW_JOIN_PREFIX: &str = "window-join:";

/// Parse `stream-kafka:` partitions into batches with schema `(key, ts, val)`.
fn parse_stream_kafka_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<arrow::record_batch::RecordBatch>> {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let mut batches = Vec::new();

    for partition in partitions {
        let desc = partition.description().trim();
        let Some(payload) = desc.strip_prefix(STREAM_KAFKA_PARTITION_PREFIX) else {
            continue;
        };

        let parts: Vec<&str> = payload.splitn(4, ':').collect();
        if parts.len() != 4 {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "stream-kafka partition {} must use \
                     stream-kafka:<topic>:<partition>:<start_offset>:<records>",
                    partition.partition_id()
                ),
            });
        }

        let records_str = parts.get(3).copied().unwrap_or("").trim();
        let mut keys: Vec<String> = Vec::new();
        let mut timestamps: Vec<i64> = Vec::new();
        let mut values: Vec<i64> = Vec::new();

        for record in records_str.split('|') {
            let record = record.trim();
            if record.is_empty() {
                continue;
            }

            let mut key: Option<String> = None;
            let mut ts: Option<i64> = None;
            let mut val: Option<i64> = None;

            for kv in record.split(',') {
                let kv = kv.trim();
                let (k, v) =
                    kv.split_once('=')
                        .ok_or_else(|| ExecutorError::InvalidAssignment {
                            message: format!("invalid stream-kafka field '{kv}', expected k=v"),
                        })?;
                match k.trim() {
                    "key" => key = Some(v.trim().to_owned()),
                    "ts" => {
                        ts = Some(v.trim().parse::<i64>().map_err(|e| {
                            ExecutorError::InvalidAssignment {
                                message: format!("invalid ts '{v}': {e}"),
                            }
                        })?)
                    }
                    "val" => {
                        val = Some(v.trim().parse::<i64>().map_err(|e| {
                            ExecutorError::InvalidAssignment {
                                message: format!("invalid val '{v}': {e}"),
                            }
                        })?)
                    }
                    other => {
                        return Err(ExecutorError::InvalidAssignment {
                            message: format!("unknown stream-kafka record field '{other}'"),
                        });
                    }
                }
            }

            keys.push(key.ok_or_else(|| ExecutorError::InvalidAssignment {
                message: String::from("stream-kafka record missing 'key' field"),
            })?);
            timestamps.push(ts.ok_or_else(|| ExecutorError::InvalidAssignment {
                message: String::from("stream-kafka record missing 'ts' field"),
            })?);
            values.push(val.ok_or_else(|| ExecutorError::InvalidAssignment {
                message: String::from("stream-kafka record missing 'val' field"),
            })?);
        }

        if keys.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "stream-kafka partition {} contains no records",
                    partition.partition_id()
                ),
            });
        }

        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(key_refs)) as _,
                Arc::new(Int64Array::from(timestamps)) as _,
                Arc::new(Int64Array::from(values)) as _,
            ],
        )
        .map_err(|e| ExecutorError::LocalExecution {
            message: format!("failed to build stream-kafka RecordBatch: {e}"),
        })?;

        batches.push(batch);
    }

    Ok(batches)
}

/// Wire format for a `stream:cep:` task fragment.
#[derive(serde::Deserialize)]
struct CepFragmentSpec {
    key_column: String,
    event_time_column: String,
    stage_column: String,
    pattern: krishiv_plan::cep::CompiledPattern,
}

/// Execute a `stream:cep:` fragment using [`PartitionedCepMatcher`] (GAP-10).
///
/// Reads input batches from the assignment, routes each row to the appropriate
/// per-key matcher using the stage identified by `stage_column`, then collects
/// and returns all completed pattern matches as concatenated `RecordBatch`es.
fn execute_cep_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    use arrow::array::{Array, Int64Array, StringArray};
    use krishiv_plan::cep::PartitionedCepMatcher;

    let payload = fragment.strip_prefix(STREAM_CEP_PREFIX).ok_or_else(|| {
        ExecutorError::InvalidAssignment {
            message: format!(
                "execute_cep_fragment called with wrong prefix; expected '{STREAM_CEP_PREFIX}', \
                 got: {fragment}"
            ),
        }
    })?;

    let spec: CepFragmentSpec =
        serde_json::from_str(payload).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream:cep invalid JSON spec: {e}"),
        })?;

    let job_id = assignment.job_id().as_str();

    // Collect input batches using the same priority order as the loop path.
    let input_batches: Vec<arrow::record_batch::RecordBatch> =
        if let Some(drainer) = runner.continuous_drainer.as_ref() {
            drainer
                .drain_job(job_id)
                .map_err(|message| ExecutorError::LocalExecution { message })?
        } else if let Some((_, pushed)) = runner.continuous_inputs.remove(job_id) {
            pushed
        } else {
            let inmem = read_inmem_stream_batches(assignment.input_partitions());
            if !inmem.is_empty() {
                inmem
            } else {
                crate::fragment::common::read_inline_ipc_partitions(assignment.input_partitions())?
                    .into_iter()
                    .flat_map(|(_, batches)| batches)
                    .collect()
            }
        };

    let mut matcher = PartitionedCepMatcher::<String>::new(spec.pattern);
    let mut matched_batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    // H2: track max event time seen so the output carries a watermark.
    let mut max_event_time_ms: i64 = i64::MIN;

    for batch in &input_batches {
        let schema = batch.schema();

        let key_idx =
            schema
                .index_of(&spec.key_column)
                .map_err(|_| ExecutorError::InvalidAssignment {
                    message: format!(
                        "stream:cep key_column '{}' not found in schema",
                        spec.key_column
                    ),
                })?;
        let time_idx = schema.index_of(&spec.event_time_column).map_err(|_| {
            ExecutorError::InvalidAssignment {
                message: format!(
                    "stream:cep event_time_column '{}' not found in schema",
                    spec.event_time_column
                ),
            }
        })?;
        let stage_idx =
            schema
                .index_of(&spec.stage_column)
                .map_err(|_| ExecutorError::InvalidAssignment {
                    message: format!(
                        "stream:cep stage_column '{}' not found in schema",
                        spec.stage_column
                    ),
                })?;

        let key_col = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!("stream:cep key_column '{}' must be Utf8", spec.key_column),
            })?;
        let time_col = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!(
                    "stream:cep event_time_column '{}' must be Int64",
                    spec.event_time_column
                ),
            })?;
        let stage_col = batch
            .column(stage_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!(
                    "stream:cep stage_column '{}' must be Utf8",
                    spec.stage_column
                ),
            })?;

        for row in 0..batch.num_rows() {
            if key_col.is_null(row) || time_col.is_null(row) || stage_col.is_null(row) {
                continue;
            }
            let key = key_col.value(row).to_owned();
            let event_time_ms = time_col.value(row);
            let stage_name = stage_col.value(row).to_owned();

            if event_time_ms > max_event_time_ms {
                max_event_time_ms = event_time_ms;
            }

            let row_batch = batch.slice(row, 1);
            let matches = matcher.process_event(key, &stage_name, row_batch, event_time_ms);

            for stage_batches in matches {
                // stage_batches is Vec<RecordBatch>, one per matched stage.
                // Concatenate into a single batch representing one full match.
                if stage_batches.is_empty() {
                    continue;
                }
                let merged = arrow::compute::concat_batches(
                    &stage_batches
                        .first()
                        .ok_or_else(|| ExecutorError::LocalExecution {
                            message: "empty stage batches".into(),
                        })?
                        .schema(),
                    &stage_batches,
                )
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("stream:cep concat match stages: {e}"),
                })?;
                matched_batches.push(merged);
            }
        }
    }

    let total_rows: usize = matched_batches.iter().map(|b| b.num_rows()).sum();
    let total_batches = matched_batches.len();
    let column_count = matched_batches
        .first()
        .map(|b| b.num_columns())
        .unwrap_or(0);

    krishiv_metrics::global_metrics().set_streaming_rows(
        job_id,
        assignment.task_id().as_str(),
        total_rows as u64,
    );

    let mut output = ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
        matched_batches,
    );
    // H2: propagate watermark so coordinator can advance global low-watermark.
    if max_event_time_ms > i64::MIN {
        output = output.with_watermark_ms(max_event_time_ms);
    }
    Ok(output)
}

/// ST8: Execute a `window-join:` fragment using [`WatermarkWindowJoinOperator`].
///
/// Fragment format: `window-join:<json>` where `<json>` is a serialised
/// `WatermarkWindowJoinSpec`.  Input partitions with even indices are treated
/// as the left stream; odd indices as the right stream.
fn execute_window_join_fragment(
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    let json = fragment.strip_prefix(WINDOW_JOIN_PREFIX).ok_or_else(|| {
        ExecutorError::InvalidAssignment {
            message: format!(
                "execute_window_join_fragment: wrong prefix; expected '{WINDOW_JOIN_PREFIX}'"
            ),
        }
    })?;

    let spec: krishiv_dataflow::WatermarkWindowJoinSpec =
        serde_json::from_str(json).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("window-join: invalid spec JSON: {e}"),
        })?;

    // Partition input into left (even indices) and right (odd indices).
    let all_partitions =
        crate::fragment::common::read_inline_ipc_partitions(assignment.input_partitions())
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("window-join: failed to decode input partitions: {e}"),
            })?;

    let (left_batches, right_batches): (Vec<_>, Vec<_>) = all_partitions
        .into_iter()
        .enumerate()
        .flat_map(|(i, (_src, batches))| batches.into_iter().map(move |b| (i, b)))
        .partition(|(i, _)| i % 2 == 0);

    let left: Vec<_> = left_batches.into_iter().map(|(_, b)| b).collect();
    let right: Vec<_> = right_batches.into_iter().map(|(_, b)| b).collect();

    let out =
        krishiv_dataflow::execute_window_join(&left, &right, spec, i64::MAX).map_err(|e| {
            ExecutorError::LocalExecution {
                message: format!("window-join: execution error: {e}"),
            }
        })?;

    let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
    let total_batches = out.len();
    let column_count = out.first().map(|b| b.num_columns()).unwrap_or(0);
    Ok(ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
        out,
    ))
}

/// Execute a `stream:loop:` fragment using `ContinuousWindowExecutor` (GAP-6).
///
/// Creates or reuses a per-job stateful executor stored in
/// `runner.loop_executors`.  Drains pending batches via `continuous_drainer`,
/// passes them through the window operator, and returns emitted window batches.
fn execute_loop_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    fragment: &str,
) -> ExecutorResult<ExecutorTaskOutput> {
    let payload = fragment.strip_prefix(STREAM_LOOP_PREFIX).ok_or_else(|| {
        ExecutorError::InvalidAssignment {
            message: format!(
                "execute_loop_fragment called with wrong prefix; expected '{}', got: {fragment}",
                STREAM_LOOP_PREFIX
            ),
        }
    })?;

    // Format: <job_id>|<window_fragment>
    let (job_id, window_spec_str) =
        payload
            .split_once('|')
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!(
                    "stream:loop fragment must be \
                     stream:loop:<job_id>|<window_spec>; got: {fragment}"
                ),
            })?;
    let job_id = job_id.trim();
    let window_spec_str = window_spec_str.trim();

    if job_id.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("stream:loop fragment requires a non-empty job_id"),
        });
    }

    // Fetch or create the stateful executor for this job.
    let executor_entry = runner
        .loop_executors
        .entry(job_id.to_owned())
        .or_try_insert_with(|| {
            let plan_spec = decode_window_execution_spec(window_spec_str).map_err(|e| {
                ExecutorError::InvalidAssignment {
                    message: format!("stream:loop invalid window spec '{window_spec_str}': {e}"),
                }
            })?;
            // Use durable state dir when the runner is configured for
            // single-node-durable or distributed-durable profiles.
            let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(job_id));
            let mut exec =
                ContinuousWindowExecutor::new_with_state_dir(plan_spec, job_state_dir.as_deref())
                    .map_err(|e| ExecutorError::InvalidAssignment {
                    message: format!("stream:loop failed to create window executor: {e}"),
                })?;
            // A restore directive that arrived before this job's first cycle
            // seeds the freshly created executor with the checkpoint state.
            if let Some((_, restored)) = runner.pending_restores.remove(job_id) {
                let mut non_empty = restored.snapshots.iter().filter(|b| !b.is_empty());
                if let Some(first) = non_empty.next() {
                    exec.restore_from_snapshot(first).map_err(|e| {
                        ExecutorError::LocalExecution {
                            message: format!(
                                "stream:loop restore from checkpoint epoch {} failed: {e}",
                                restored.epoch
                            ),
                        }
                    })?;
                    for rest in non_empty {
                        exec.merge_snapshot(rest)
                            .map_err(|e| ExecutorError::LocalExecution {
                                message: format!(
                                    "stream:loop merge restore from checkpoint epoch {} \
                                     failed: {e}",
                                    restored.epoch
                                ),
                            })?;
                    }
                }
                tracing::info!(
                    job_id,
                    epoch = restored.epoch,
                    snapshots = restored.snapshots.len(),
                    "seeded new continuous window executor from restored checkpoint"
                );
            }
            Ok::<_, ExecutorError>(Arc::new(Mutex::new(exec)))
        })?;
    let executor_arc = executor_entry.value().clone();
    drop(executor_entry); // release dashmap lock

    // Embedded execution drains the shared session registry. Distributed
    // execution receives the cycle's batches as coordinator-owned InlineIpc
    // partitions on the assignment.
    let input_batches = if let Some(drainer) = runner.continuous_drainer.as_ref() {
        drainer
            .drain_job(job_id)
            .map_err(|message| ExecutorError::LocalExecution { message })?
    } else if let Some((_, pushed)) = runner.continuous_inputs.remove(job_id) {
        // Distributed path: consume batches that arrived via push_continuous_input gRPC.
        pushed
    } else {
        crate::fragment::common::read_inline_ipc_partitions(assignment.input_partitions())?
            .into_iter()
            .flat_map(|(_, batches)| batches)
            .collect()
    };

    // Process through the stateful window executor.
    let (output_batches, loop_watermark_ms) = {
        let mut exec = executor_arc
            .lock()
            .map_err(|_| ExecutorError::LocalExecution {
                message: format!(
                    "stream:loop job '{job_id}' executor lock poisoned; \
                     window state is inconsistent — restart the job",
                ),
            })?;
        let batches = exec
            .drain(input_batches)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("stream:loop drain error: {e}"),
            })?;
        // H2: propagate watermark so the coordinator can advance the global
        // streaming watermark and trigger late-data handling downstream.
        let wm = exec.last_watermark_ms();
        (batches, wm)
    };

    // Fix #2: after each drain cycle, push a read-only snapshot into the
    // queryable-state store so REST point-lookups can serve the latest state
    // without contending with the hot processing path.
    if let Some(qs) = runner.queryable_state.as_ref() {
        use krishiv_state::StateBackend as _;
        // Use peek_snapshot_bytes (no checkpoint() side-effect) so the normal
        // checkpoint lifecycle is not disturbed by queryable-state observation.
        executor_arc
            .lock()
            .ok()
            .and_then(|exec| exec.peek_snapshot_bytes().ok())
            .and_then(|bytes| {
                let mut backend = krishiv_state::RocksDbStateBackend::ephemeral().ok()?;
                backend.load_snapshot(&bytes).ok()?;
                qs.register(job_id, "window-exec", std::sync::Arc::new(backend));
                Some(())
            });
    }

    let total_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
    let total_batches = output_batches.len();
    let column_count = output_batches.first().map(|b| b.num_columns()).unwrap_or(0);
    // Scan the window grouping key in output batches so the coordinator can
    // detect skewed partitions in continuous streaming jobs.
    let key_column = decode_window_execution_spec(window_spec_str)
        .ok()
        .map(|s| s.key_column)
        .unwrap_or_default();
    let hot_key_reports = build_hot_key_reports(
        &output_batches,
        &key_column,
        assignment.job_id(),
        assignment.stage_id().as_str(),
    );
    let mut output = ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
        output_batches,
    );
    output.hot_key_reports = hot_key_reports;
    if loop_watermark_ms > i64::MIN {
        output = output.with_watermark_ms(loop_watermark_ms);
    }
    Ok(output)
}

/// Execute a bounded streaming window fragment (tumbling, sliding, or session).
pub(crate) async fn execute_streaming_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    udf_limits: ResourceLimits,
    memory_budget: Arc<MemoryBudget>,
) -> ExecutorResult<ExecutorTaskOutput> {
    let fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
    let fragment = fragment_body.as_str();

    // Fragment dispatch priority (first match wins):
    //   1. SQL query    — detected via sql_query_from_fragment()
    //   2. stream:loop: — stateful ContinuousWindowExecutor (long-lived)
    //   3. <default>    — bounded window (tumbling / sliding / session)
    if let Some(query) = crate::fragment::common::sql_query_from_fragment(fragment) {
        // Create a new SQL engine with UDF limits and the task's memory limit
        // for this task execution. The reservation guard holds this task's
        // share of the executor process budget until the fragment returns.
        let (engine_memory_limit, _process_memory_reservation) =
            crate::fragment::common::reserve_task_engine_memory(&memory_budget);
        let engine = Arc::new(
            krishiv_sql::SqlEngine::new_with_memory_limit(engine_memory_limit)
                .with_udf_limits(udf_limits),
        );

        // Continuous SQL queries must use execute_stream to avoid blocking and buffering forever.
        let dataframe = engine
            .sql(query)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: error.to_string(),
            })?;

        let mut stream =
            dataframe
                .execute_stream()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;

        use tokio_stream::StreamExt;
        let mut total_rows = 0;
        let mut total_batches = 0;
        let mut column_count = 0;

        let is_object_parquet_sink = assignment.output_contract().kind()
            == krishiv_proto::OutputContractKind::Sink
            && assignment
                .output_contract()
                .description()
                .trim()
                .starts_with(crate::runner::OBJECT_PARQUET_SINK_PREFIX);
        // Staged sink contracts buffer the bounded stream output and run one
        // staged write at the end (the commit protocol needs whole-task
        // output); legacy direct contracts keep their per-batch write.
        let is_staged_sink = is_object_parquet_sink
            && crate::fragment::common::parse_object_parquet_sink_spec(
                assignment.output_contract(),
            )?
            .staged;
        let mut staged_buffer: Vec<arrow::record_batch::RecordBatch> = Vec::new();

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|error| ExecutorError::LocalExecution {
                message: error.to_string(),
            })?;

            column_count = batch.num_columns();

            if is_staged_sink {
                staged_buffer.push(batch.clone());
            } else if is_object_parquet_sink {
                crate::fragment::common::write_object_parquet_sink(
                    assignment.output_contract(),
                    std::slice::from_ref(&batch),
                )
                .await?;
            }

            total_rows += batch.num_rows();
            total_batches += 1;
        }

        let sink_staged_files = if is_staged_sink {
            crate::fragment::common::write_object_parquet_sink_for_task(assignment, &staged_buffer)
                .await?
        } else {
            Vec::new()
        };

        return Ok(ExecutorTaskOutput::streaming_window(
            total_rows,
            total_batches,
            column_count,
            vec![],
        )
        .with_sink_staged_files(sink_staged_files));
    }

    // GAP-10: stream:cep: fragments use PartitionedCepMatcher for sequential
    // pattern matching over keyed event streams.
    if fragment.starts_with(STREAM_CEP_PREFIX) {
        return execute_cep_fragment(runner, assignment, fragment);
    }

    // GAP-6: stream:loop: fragments use a stateful ContinuousWindowExecutor
    // shared across drain cycles via runner.loop_executors.
    if fragment.starts_with(STREAM_LOOP_PREFIX) {
        return execute_loop_fragment(runner, assignment, fragment);
    }

    // ST8: watermark-bounded stream-to-stream join.
    if fragment.starts_with(WINDOW_JOIN_PREFIX) {
        return execute_window_join_fragment(assignment, fragment);
    }

    let mut plan_spec =
        decode_window_execution_spec(fragment).map_err(|e| ExecutorError::InvalidAssignment {
            message: e.to_string(),
        })?;

    // GAP-WATERMARK: Apply the upstream stage's output watermark as the initial
    // prev_watermark_ms for this stage's window operators. Without this, stage 2
    // starts from i64::MIN and incorrectly treats all stage-1 output events as
    // in-order even when the actual watermark is much higher, causing false
    // "no late events" reports.
    // The WatermarkHint partition is injected by the coordinator when emitting
    // task assignments for downstream stages.
    if let Some(upstream_wm) =
        crate::fragment::common::read_watermark_hint(assignment.input_partitions())
    {
        tracing::debug!(
            upstream_watermark_ms = upstream_wm,
            "applied upstream watermark hint to downstream stage window spec"
        );
    }

    // G2/G3: InMemory partitions (embedded mode fast path — no ASCII round-trip).
    // Produced by InProcessStreamingRuntime::execute_windowed using direct Arrow
    // RecordBatches rather than stream-kafka ASCII strings. Preserves all columns
    // so multi-aggregation windows work correctly.
    let inmem_batches = read_inmem_stream_batches(assignment.input_partitions());
    let job_id = assignment.job_id().as_str();
    if !inmem_batches.is_empty() {
        return execute_streaming_with_batches(
            runner,
            assignment.job_id(),
            assignment.stage_id(),
            inmem_batches,
            plan_spec,
        )
        .await;
    }

    let batches = parse_stream_kafka_partitions(assignment.input_partitions())?;

    // Only override column names for stream-kafka partitions.  Overriding
    // unconditionally would clobber user-specified column names for non-kafka
    // streaming fragments (e.g. in-process or file-backed streams).
    if !batches.is_empty() {
        plan_spec.key_column = String::from("key");
        plan_spec.event_time_column = String::from("ts");
        if plan_spec.agg_exprs.first().is_some_and(|a| a.kind == WindowAggKind::Sum)
            && plan_spec.agg_exprs.first().is_some_and(|a| a.input_column.is_empty())
            && let Some(agg) = plan_spec.agg_exprs.first_mut()
        {
            agg.input_column = String::from("val");
        }
    }
    // GAP-2: compute the observed event-time watermark from input batches BEFORE
    // executing the window so we can attach it to the output and let the coordinator
    // track global low-watermark across all executor tasks.
    let observed_watermark_ms = compute_input_watermark(&batches, &plan_spec);
    let advisory_buckets = advise_streaming_buckets(runner, job_id, &batches);

    let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(job_id));

    let collected_batches = execute_bounded_window(batches, &plan_spec, job_state_dir.as_deref())
        .map_err(|e| ExecutorError::LocalExecution {
        message: e.to_string(),
    })?;

    let total_rows: usize = collected_batches.iter().map(|b| b.num_rows()).sum();
    let total_batches = collected_batches.len();
    let column_count = collected_batches
        .first()
        .map(|b| b.num_columns())
        .unwrap_or(0);

    krishiv_metrics::global_metrics().set_streaming_rows(
        job_id,
        assignment.task_id().as_str(),
        total_rows as u64,
    );

    let hot_key_reports = build_hot_key_reports(
        &collected_batches,
        &plan_spec.key_column,
        assignment.job_id(),
        assignment.stage_id().as_str(),
    );
    let mut output = ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
        collected_batches,
    );
    if let Some(wm) = observed_watermark_ms {
        output = output.with_watermark_ms(wm);
    }
    output.hot_key_reports = hot_key_reports;
    output = output.with_advisory_buckets(advisory_buckets);
    Ok(output)
}

/// Collect all InMemory partition batches into a flat Vec.
///
/// Returns empty if no InMemory partitions are present so callers can fall
/// through to the stream-kafka ASCII path.
fn read_inmem_stream_batches(
    partitions: &[krishiv_proto::InputPartition],
) -> Vec<arrow::record_batch::RecordBatch> {
    use krishiv_proto::InputPartitionDescriptor;
    let mut out = Vec::new();
    for p in partitions {
        if let Some(InputPartitionDescriptor::InMemory { batches, .. }) = p.descriptor() {
            for b in batches {
                out.push((**b).clone());
            }
        }
    }
    out
}

/// Execute a bounded streaming window over pre-decoded in-memory batches.
///
/// Used by the InMemory partition fast path to skip stream-kafka ASCII parsing.
async fn execute_streaming_with_batches(
    runner: &ExecutorTaskRunner,
    job_id: &krishiv_proto::JobId,
    stage_id: &krishiv_proto::StageId,
    batches: Vec<arrow::record_batch::RecordBatch>,
    spec: WindowExecutionSpec,
) -> ExecutorResult<ExecutorTaskOutput> {
    let observed_watermark_ms = compute_input_watermark(&batches, &spec);

    // Observe total input bytes in the per-job StreamingPartitionAdvisor so
    // the EMA tracks actual data volume across cycles.
    let advisory_buckets = advise_streaming_buckets(runner, job_id.as_str(), &batches);

    let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(job_id.as_str()));
    let collected =
        execute_bounded_window(batches, &spec, job_state_dir.as_deref()).map_err(|e| {
            ExecutorError::LocalExecution {
                message: e.to_string(),
            }
        })?;
    let total_rows: usize = collected.iter().map(|b| b.num_rows()).sum();
    let total_batches = collected.len();
    let column_count = collected.first().map(|b| b.num_columns()).unwrap_or(0);
    let hot_key_reports =
        build_hot_key_reports(&collected, &spec.key_column, job_id, stage_id.as_str());
    let mut output =
        ExecutorTaskOutput::streaming_window(total_rows, total_batches, column_count, collected);
    if let Some(wm) = observed_watermark_ms {
        output = output.with_watermark_ms(wm);
    }
    output.hot_key_reports = hot_key_reports;
    output = output.with_advisory_buckets(advisory_buckets);
    Ok(output)
}

/// Observe `batches` in the per-job `StreamingPartitionAdvisor` and return the
/// current EMA-derived bucket recommendation.
///
/// Creates the advisor on first call for a given job (initial=2, min=1, max=128).
/// All runner clones share the same advisor instance so the EMA accumulates
/// correctly across task cycles for the same job.
fn advise_streaming_buckets(
    runner: &ExecutorTaskRunner,
    job_id: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> u32 {
    use krishiv_dataflow::StreamingPartitionAdvisor;
    let entry = runner
        .streaming_advisors
        .entry(job_id.to_owned())
        .or_insert_with(|| {
            Arc::new(std::sync::Mutex::new(StreamingPartitionAdvisor::new(
                2, 1, 128,
            )))
        });
    let advisor_arc = entry.value().clone();
    drop(entry);

    let total_bytes: u64 = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    match advisor_arc.lock() {
        Ok(mut advisor) => advisor.observe_batch_bytes(total_bytes),
        Err(_) => {
            tracing::warn!(job_id = %job_id, "streaming partition advisor mutex poisoned; defaulting to 1 bucket");
            1
        }
    }
}

/// Compute the event-time watermark from input batches.
///
/// Watermark = max(event_time_column) − watermark_lag_ms.
/// Returns `None` if the event-time column is not found or the batches are empty.
/// Supports Int64 (milliseconds) and Timestamp columns (all TimeUnit variants, converted to ms).
fn compute_input_watermark(
    batches: &[arrow::record_batch::RecordBatch],
    spec: &WindowExecutionSpec,
) -> Option<i64> {
    use arrow::array::{
        Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    let mut max_ts: Option<i64> = None;
    for batch in batches {
        let col_idx = batch.schema().index_of(&spec.event_time_column).ok()?;
        let col = batch.column(col_idx);
        let batch_max = match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>()?;
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .reduce(i64::max)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let arr = col.as_any().downcast_ref::<TimestampSecondArray>()?;
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i).saturating_mul(1_000))
                    .reduce(i64::max)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = col.as_any().downcast_ref::<TimestampMillisecondArray>()?;
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i))
                    .reduce(i64::max)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i) / 1_000)
                    .reduce(i64::max)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .map(|i| arr.value(i) / 1_000_000)
                    .reduce(i64::max)
            }
            _ => return None,
        };
        if let Some(ts) = batch_max {
            max_ts = Some(match max_ts {
                Some(prev) => prev.max(ts),
                None => ts,
            });
        }
    }
    max_ts.map(|ts| ts.saturating_sub(spec.watermark_lag_ms as i64))
}
