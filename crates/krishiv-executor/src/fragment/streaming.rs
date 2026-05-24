//! Streaming fragment execution: `execute_streaming_fragment` and its helpers.

use krishiv_proto::ExecutorTaskAssignment;

use crate::{ExecutorError, ExecutorResult};
use crate::runner::{ExecutorTaskOutput, ExecutorTaskRunner};

// ── Streaming fragment parser ─────────────────────────────────────────────────

const STREAM_KAFKA_PARTITION_PREFIX: &str = "stream-kafka:";

/// Aggregate function for a streaming window.
enum StreamingAgg {
    Count,
    Sum { col: String },
}

/// Parsed configuration from a `stream:tw:...` fragment string.
struct StreamingWindowSpec {
    key_col: String,
    time_col: String,
    window_ms: u64,
    lag_ms: u64,
    agg: StreamingAgg,
}

/// Parse `stream:tw:key=<col>:time=<col>:win=<ms>:lag=<ms>[:agg=count|sum:col=<col>]`
fn parse_streaming_window_spec(fragment: &str) -> ExecutorResult<StreamingWindowSpec> {
    let payload =
        fragment
            .strip_prefix("stream:tw:")
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!(
                    "streaming fragment must start with 'stream:tw:'; got: {fragment}"
                ),
            })?;

    let mut key_col = None;
    let mut time_col = None;
    let mut window_ms = None;
    let mut lag_ms = None;
    let mut agg_kind: Option<String> = None;
    let mut agg_col: Option<String> = None;

    for part in payload.split(':') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!("streaming fragment field must be k=v; got '{part}'"),
            })?;
        match k.trim() {
            "key" => key_col = Some(v.trim().to_owned()),
            "time" => time_col = Some(v.trim().to_owned()),
            "win" => {
                window_ms =
                    Some(
                        v.trim()
                            .parse::<u64>()
                            .map_err(|e| ExecutorError::InvalidAssignment {
                                message: format!("invalid win value '{v}': {e}"),
                            })?,
                    )
            }
            "lag" => {
                lag_ms =
                    Some(
                        v.trim()
                            .parse::<u64>()
                            .map_err(|e| ExecutorError::InvalidAssignment {
                                message: format!("invalid lag value '{v}': {e}"),
                            })?,
                    )
            }
            "agg" => agg_kind = Some(v.trim().to_owned()),
            "col" => agg_col = Some(v.trim().to_owned()),
            _ => {} // forward-compatible: ignore unknown keys
        }
    }

    let agg = match agg_kind.as_deref() {
        None | Some("count") => StreamingAgg::Count,
        Some("sum") => StreamingAgg::Sum {
            col: agg_col.ok_or_else(|| ExecutorError::InvalidAssignment {
                message: String::from("stream:tw: with agg=sum requires col=<column>"),
            })?,
        },
        Some(other) => {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "unknown streaming aggregate '{other}', expected 'count' or 'sum'"
                ),
            });
        }
    };

    Ok(StreamingWindowSpec {
        key_col: key_col.ok_or_else(|| ExecutorError::InvalidAssignment {
            message: String::from("stream:tw: fragment missing key=<col>"),
        })?,
        time_col: time_col.ok_or_else(|| ExecutorError::InvalidAssignment {
            message: String::from("stream:tw: fragment missing time=<col>"),
        })?,
        window_ms: window_ms.ok_or_else(|| ExecutorError::InvalidAssignment {
            message: String::from("stream:tw: fragment missing win=<ms>"),
        })?,
        lag_ms: lag_ms.unwrap_or(0),
        agg,
    })
}

/// Parse `stream-kafka:` input partitions into `RecordBatch`es.
///
/// Format: `stream-kafka:<topic>:<partition>:<start_offset>:<records>`
/// where `<records>` is `key=<k>,ts=<t>,val=<v>|key=<k2>,...`
///
/// Output schema: `(key: Utf8, ts: Int64, val: Int64)`.
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

        // Format: <topic>:<partition>:<start_offset>:<records>
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

/// Execute a streaming (continuous) stage fragment.
///
/// R5.1 certified path: `stream:tw:key=<col>:time=<col>:win=<ms>:lag=<ms>`
/// with `stream-kafka:` input partition descriptors.
///
/// The loop processes all available input batches in order, advancing the
/// watermark from each batch's events, flushing closed windows, and
/// collecting output metadata.  For the R5.1 deterministic replay
/// acceptance gate, the same input sequence must produce identical output.
///
/// Continuous (unbounded) Kafka sources arrive in R5.2; for R5.1 the
/// source is the finite `stream-kafka:` in-memory batch set.
pub(crate) async fn execute_streaming_fragment(
    _runner: &ExecutorTaskRunner,
    assignment: &ExecutorTaskAssignment,
) -> ExecutorResult<ExecutorTaskOutput> {
    use arrow::array::Int64Array;
    use krishiv_exec::{
        AggExpr, AggFunction, OperatorMessage, TumblingWindowOperator, TumblingWindowSpec,
        WatermarkState, operator_queue,
    };

    let fragment = assignment.plan_fragment().description().trim();
    let spec = parse_streaming_window_spec(fragment)?;
    let batches = parse_stream_kafka_partitions(assignment.input_partitions())?;

    let agg_exprs = match &spec.agg {
        StreamingAgg::Count => vec![AggExpr {
            function: AggFunction::Count,
            input_column: String::new(),
            output_column: String::from("count"),
        }],
        StreamingAgg::Sum { col } => vec![AggExpr {
            function: AggFunction::Sum,
            input_column: col.clone(),
            output_column: format!("sum_{col}"),
        }],
    };

    let tw_spec = TumblingWindowSpec {
        key_column: spec.key_col.clone(),
        event_time_column: spec.time_col.clone(),
        window_size_ms: spec.window_ms,
        agg_exprs,
    };

    let mut watermark = WatermarkState::new(spec.lag_ms);
    let mut window_op = TumblingWindowOperator::new(tw_spec);
    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;
    let mut column_count: usize = 0;

    // Wire batches through the bounded backpressure OperatorQueue (R7.2 Group B).
    // The producer task feeds all parsed batches into the queue then drops the
    // sender so the receiver loop terminates naturally.
    let (tx, mut rx) = operator_queue(64);
    tokio::spawn(async move {
        for batch in batches {
            // Ignore send errors: the receiver may have dropped on cancellation.
            let _ = tx.send_data(batch).await;
        }
        // tx drops here, closing the data channel.
    });

    while let Some(msg) = rx.recv().await {
        match msg {
            OperatorMessage::Data(batch) => {
                let time_idx = batch.schema().index_of(&spec.time_col).map_err(|_| {
                    ExecutorError::LocalExecution {
                        message: format!(
                            "event_time column '{}' not found in stream-kafka batch",
                            spec.time_col
                        ),
                    }
                })?;
                let arr = batch
                    .column(time_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| ExecutorError::LocalExecution {
                        message: format!("event_time column '{}' must be Int64", spec.time_col),
                    })?;
                // EXE-3: advance watermark per event so lateness is evaluated in-order.
                for row in 0..arr.len() {
                    let event_time_ms = arr.value(row);
                    if watermark.is_late(event_time_ms) {
                        continue;
                    }
                    watermark.advance(event_time_ms);
                    let new_wm = watermark.current_watermark_ms();
                    let row_batch = batch.slice(row, 1);
                    let output = window_op.process_batch(&row_batch, new_wm).map_err(|e| {
                        ExecutorError::LocalExecution {
                            message: format!("streaming window process_batch failed: {e}"),
                        }
                    })?;
                    for ob in &output {
                        total_rows += ob.num_rows();
                        total_batches += 1;
                        column_count = ob.num_columns();
                    }
                }
            }
            OperatorMessage::Barrier { epoch } => {
                // Barrier received: checkpoint handling is deferred to R8.
                let _ = epoch;
            }
        }
    }

    // Final flush: push watermark past all buffered data to close
    // remaining open windows before the task reports completion.
    let final_wm = watermark
        .current_watermark_ms()
        .saturating_add(i64::MAX / 4);
    let final_output = window_op.flush_closed_windows(final_wm).map_err(|e| {
        ExecutorError::LocalExecution {
            message: format!("streaming final window flush failed: {e}"),
        }
    })?;
    for ob in &final_output {
        total_rows += ob.num_rows();
        total_batches += 1;
        column_count = ob.num_columns();
    }

    Ok(ExecutorTaskOutput::streaming_window(
        total_rows,
        total_batches,
        column_count,
    ))
}
