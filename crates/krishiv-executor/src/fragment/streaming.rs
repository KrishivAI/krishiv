//! Streaming fragment execution: unified bounded window path (all window kinds).

use std::sync::{Arc, Mutex};

use krishiv_exec::ContinuousWindowExecutor;
use krishiv_udf::ResourceLimits;

use crate::fragment::common::task_fragment_body;
use crate::runner::{ExecutorTaskOutput, ExecutorTaskRunner};
use crate::{ExecutorError, ExecutorResult};
use krishiv_exec::execute_bounded_window;
use krishiv_plan::window::{WindowAggKind, WindowExecutionSpec, WindowKind, parse_stream_fragment};
use krishiv_proto::ExecutorTaskAssignment;

const STREAM_KAFKA_PARTITION_PREFIX: &str = "stream-kafka:";
const STREAM_CONTINUOUS_PREFIX: &str = "stream:continuous:";

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
///  2. Calls `runner.continuous_drainer.drain_job(job_id)` to fetch newly
///     arrived input batches.
///  3. Passes the batches through `ContinuousWindowExecutor::drain()`.
///  4. Returns any newly emitted (closed) window batches.
const STREAM_LOOP_PREFIX: &str = "stream:loop:";

fn parsed_to_plan_spec(parsed: krishiv_plan::window::ParsedStreamFragment) -> WindowExecutionSpec {
    let (slide_ms, session_gap_ms) = match parsed.window_kind {
        WindowKind::Tumbling => (None, None),
        WindowKind::Sliding => (parsed.slide_ms, None),
        WindowKind::Session => (None, parsed.session_gap_ms),
    };
    WindowExecutionSpec {
        key_column: parsed.key_col,
        event_time_column: parsed.time_col,
        watermark_lag_ms: parsed.lag_ms,
        window_kind: parsed.window_kind,
        window_size_ms: parsed.window_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs: vec![parsed.agg],
        state_ttl_ms: parsed.ttl_ms,
        source_watermark_lags: parsed.source_watermark_lags,
        source_id_column: parsed.source_id_column,
    }
}

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

        let records_str = parts[3].trim();
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
            values.push(val.unwrap_or(0));
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

/// Execute a `stream:loop:` fragment using `ContinuousWindowExecutor` (GAP-6).
///
/// Creates or reuses a per-job stateful executor stored in
/// `runner.loop_executors`.  Drains pending batches via `continuous_drainer`,
/// passes them through the window operator, and returns emitted window batches.
fn execute_loop_fragment(
    runner: &ExecutorTaskRunner,
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
            let parsed = parse_stream_fragment(window_spec_str).map_err(|e| {
                ExecutorError::InvalidAssignment {
                    message: format!("stream:loop invalid window spec '{window_spec_str}': {e}"),
                }
            })?;
            let mut plan_spec = parsed_to_plan_spec(parsed);
            // Normalise column names for Kafka-style data (same as bounded path).
            plan_spec.key_column = String::from("key");
            plan_spec.event_time_column = String::from("ts");
            // Use durable state dir when the runner is configured for
            // single-node-durable or distributed-durable profiles.
            let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(job_id));
            let exec =
                ContinuousWindowExecutor::new_with_state_dir(plan_spec, job_state_dir.as_deref())
                    .map_err(|e| ExecutorError::InvalidAssignment {
                    message: format!("stream:loop failed to create window executor: {e}"),
                })?;
            Ok::<_, ExecutorError>(Arc::new(Mutex::new(exec)))
        })?;
    let executor_arc = executor_entry.value().clone();
    drop(executor_entry); // release dashmap lock

    // Get new input batches from the drainer.
    let drainer =
        runner
            .continuous_drainer
            .as_ref()
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: String::from(
                    "stream:loop fragment requires a continuous_drainer on the executor runner",
                ),
            })?;
    let input_batches = drainer
        .drain_job(job_id)
        .map_err(|message| ExecutorError::LocalExecution { message })?;

    // Process through the stateful window executor.
    let output_batches = {
        let mut exec = executor_arc
            .lock()
            .map_err(|_| ExecutorError::LocalExecution {
                message: format!(
                    "stream:loop job '{job_id}' executor lock poisoned; \
                     window state is inconsistent — restart the job",
                ),
            })?;
        exec.drain(input_batches)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("stream:loop drain error: {e}"),
            })?
    };

    let total_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
    let total_batches = output_batches.len();
    let column_count = output_batches.first().map(|b| b.num_columns()).unwrap_or(0);
    Ok(ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
        output_batches,
    ))
}

/// Execute a bounded streaming window fragment (tumbling, sliding, or session).
pub(crate) async fn execute_streaming_fragment(
    runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
    udf_limits: ResourceLimits,
) -> ExecutorResult<ExecutorTaskOutput> {
    let fragment_body = task_fragment_body(assignment.plan_fragment().description())?;
    let fragment = fragment_body.as_str();

    // Fragment dispatch priority (first match wins):
    //   1. SQL query          — detected via sql_query_from_fragment()
    //   2. stream:loop:       — stateful ContinuousWindowExecutor (long-lived)
    //   3. stream:continuous: — ContinuousStreamRegistry drain (session-scoped)
    //   4. <default>          — bounded window (tumbling / sliding / session)
    if let Some(query) = crate::fragment::common::sql_query_from_fragment(fragment) {
        // Create a new SQL engine with UDF limits for this task execution.
        let engine = Arc::new(krishiv_sql::SqlEngine::new().with_udf_limits(udf_limits));

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

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.map_err(|error| ExecutorError::LocalExecution {
                message: error.to_string(),
            })?;

            column_count = batch.num_columns();

            if assignment.output_contract().kind() == krishiv_proto::OutputContractKind::Sink
                && assignment
                    .output_contract()
                    .description()
                    .trim()
                    .starts_with(crate::runner::OBJECT_PARQUET_SINK_PREFIX)
            {
                crate::fragment::common::write_object_parquet_sink(
                    assignment.output_contract(),
                    &[batch.clone()],
                )
                .await?;
            }

            total_rows += batch.num_rows();
            total_batches += 1;
        }

        return Ok(ExecutorTaskOutput::streaming_window(
            total_rows,
            total_batches,
            column_count,
            vec![],
        ));
    }

    // GAP-6: stream:loop: fragments use a stateful ContinuousWindowExecutor
    // shared across drain cycles via runner.loop_executors.
    if fragment.starts_with(STREAM_LOOP_PREFIX) {
        return execute_loop_fragment(runner, fragment);
    }

    if let Some(job_id) = fragment.strip_prefix(STREAM_CONTINUOUS_PREFIX) {
        let job_id = job_id.trim();
        if job_id.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("stream:continuous fragment requires a job id"),
            });
        }
        // In-process mode: drain from the shared ContinuousStreamRegistry.
        // Distributed mode: read InlineIpc partitions delivered in the task
        // assignment — pushed there by api_continuous_push via the coordinator.
        let collected_batches = if let Some(drainer) = runner.continuous_drainer.as_ref() {
            drainer
                .drain_job(job_id)
                .map_err(|message| ExecutorError::LocalExecution { message })?
        } else {
            // Flatten all InlineIpc partitions into a single batch list.
            crate::fragment::common::read_inline_ipc_partitions(assignment.input_partitions())?
                .into_iter()
                .flat_map(|(_, batches)| batches)
                .collect()
        };
        let total_rows: usize = collected_batches.iter().map(|b| b.num_rows()).sum();
        let total_batches = collected_batches.len();
        let column_count = collected_batches
            .first()
            .map(|b| b.num_columns())
            .unwrap_or(0);
        return Ok(ExecutorTaskOutput::streaming_window(
            total_rows,
            total_batches,
            column_count,
            collected_batches,
        ));
    }

    let parsed = parse_stream_fragment(fragment).map_err(|e| ExecutorError::InvalidAssignment {
        message: e.to_string(),
    })?;
    let mut plan_spec = parsed_to_plan_spec(parsed);

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
        // Propagate as the initial watermark_lag baseline so the first batch of
        // this stage uses the correct late-event threshold.
        if plan_spec.watermark_lag_ms == 0 {
            plan_spec.watermark_lag_ms = upstream_wm.unsigned_abs();
        }
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
        return execute_streaming_with_batches(runner, job_id, inmem_batches, plan_spec).await;
    }

    let batches = parse_stream_kafka_partitions(assignment.input_partitions())?;

    // Only override column names for stream-kafka partitions.  Overriding
    // unconditionally would clobber user-specified column names for non-kafka
    // streaming fragments (e.g. in-process or file-backed streams).
    if !batches.is_empty() {
        plan_spec.key_column = String::from("key");
        plan_spec.event_time_column = String::from("ts");
        if plan_spec
            .agg_exprs
            .first()
            .is_some_and(|a| a.kind == WindowAggKind::Sum)
            && plan_spec.agg_exprs[0].input_column.is_empty()
        {
            plan_spec.agg_exprs[0].input_column = String::from("val");
        }
    }
    // GAP-2: compute the observed event-time watermark from input batches BEFORE
    // executing the window so we can attach it to the output and let the coordinator
    // track global low-watermark across all executor tasks.
    let observed_watermark_ms = compute_input_watermark(&batches, &plan_spec);

    let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(job_id));

    let collected_batches =
        execute_bounded_window(batches, &plan_spec, job_state_dir.as_deref()).map_err(|e| {
        ExecutorError::LocalExecution {
            message: e.to_string(),
        }
    })?;

    let total_rows: usize = collected_batches.iter().map(|b| b.num_rows()).sum();
    let total_batches = collected_batches.len();
    let column_count = collected_batches
        .first()
        .map(|b| b.num_columns())
        .unwrap_or(0);

    let mut output = ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
        collected_batches,
    );
    if let Some(wm) = observed_watermark_ms {
        output = output.with_watermark_ms(wm);
    }
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
    job_id: &str,
    batches: Vec<arrow::record_batch::RecordBatch>,
    spec: WindowExecutionSpec,
) -> ExecutorResult<ExecutorTaskOutput> {
    let observed_watermark_ms = compute_input_watermark(&batches, &spec);
    let job_state_dir = runner.state_dir.as_ref().map(|d| d.join(job_id));
    let collected = execute_bounded_window(batches, &spec, job_state_dir.as_deref()).map_err(|e| {
        ExecutorError::LocalExecution {
            message: e.to_string(),
        }
    })?;
    let total_rows: usize = collected.iter().map(|b| b.num_rows()).sum();
    let total_batches = collected.len();
    let column_count = collected.first().map(|b| b.num_columns()).unwrap_or(0);
    let mut output =
        ExecutorTaskOutput::streaming_window(total_rows, total_batches, column_count, collected);
    if let Some(wm) = observed_watermark_ms {
        output = output.with_watermark_ms(wm);
    }
    Ok(output)
}

/// Compute the event-time watermark from input batches.
///
/// Watermark = max(event_time_column) − watermark_lag_ms.
/// Returns `None` if the event-time column is not found or the batches are empty.
fn compute_input_watermark(
    batches: &[arrow::record_batch::RecordBatch],
    spec: &WindowExecutionSpec,
) -> Option<i64> {
    use arrow::array::{Array, Int64Array};

    let mut max_ts: Option<i64> = None;
    for batch in batches {
        let col_idx = batch.schema().index_of(&spec.event_time_column).ok()?;
        let col = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()?;
        for i in 0..col.len() {
            if !col.is_null(i) {
                let ts = col.value(i);
                max_ts = Some(match max_ts {
                    Some(prev) => prev.max(ts),
                    None => ts,
                });
            }
        }
    }
    max_ts.map(|ts| ts.saturating_sub(spec.watermark_lag_ms as i64))
}
