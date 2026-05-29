//! Streaming fragment execution: unified bounded window path (all window kinds).

use std::sync::{Arc, Mutex};

use krishiv_exec::ContinuousWindowExecutor;

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
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
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
    let payload = fragment
        .strip_prefix(STREAM_LOOP_PREFIX)
        .expect("called with wrong prefix");

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
            let exec = ContinuousWindowExecutor::new(plan_spec).map_err(|e| {
                ExecutorError::InvalidAssignment {
                    message: format!("stream:loop failed to create window executor: {e}"),
                }
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
                message: String::from(
                    "stream:loop executor lock poisoned; executor state is inconsistent",
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
) -> ExecutorResult<ExecutorTaskOutput> {
    let fragment = assignment.plan_fragment().description().trim();

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
        let drainer = runner.continuous_drainer.as_ref().ok_or_else(|| {
            ExecutorError::InvalidAssignment {
                message: String::from(
                    "stream:continuous fragment requires a continuous drainer on the executor runner",
                ),
            }
        })?;
        let collected_batches = drainer
            .drain_job(job_id)
            .map_err(|message| ExecutorError::LocalExecution { message })?;
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

    let collected_batches =
        execute_bounded_window(batches, &plan_spec).map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
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
