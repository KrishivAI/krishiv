//! In-process streaming execution for embedded and single-node modes (ADR-12.4/12.5).

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use krishiv_exec::{
    AggExpr, AggFunction, SessionWindowOperator, SessionWindowSpec, SlidingWindowOperator,
    SlidingWindowSpec, StateBackedTumblingWindowOperator, TumblingWindowOperator,
    TumblingWindowSpec, WatermarkState,
};
use krishiv_state::{InMemoryStateBackend, StateBackend, TtlConfig, TtlStateBackend};

use crate::RuntimeError;

/// Window operator kind for local execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalWindowKind {
    Tumbling,
    Sliding { slide_ms: u64 },
    Session { gap_ms: u64 },
}

/// Specification for executing a keyed, windowed stream in-process.
#[derive(Debug, Clone)]
pub struct LocalWindowExecutionSpec {
    pub key_column: String,
    pub event_time_column: String,
    pub watermark_lag_ms: u64,
    pub window_kind: LocalWindowKind,
    pub window_size_ms: u64,
    pub agg_exprs: Vec<AggExpr>,
    /// When set, tumbling windows persist accumulators through a TTL-wrapped state backend.
    pub state_ttl_ms: Option<u64>,
}

impl LocalWindowExecutionSpec {
    /// Default count aggregation for window output.
    pub fn default_count_agg() -> Vec<AggExpr> {
        vec![AggExpr {
            function: AggFunction::Count,
            input_column: String::new(),
            output_column: String::from("count"),
        }]
    }
}

fn exec_err(e: krishiv_exec::ExecError) -> RuntimeError {
    RuntimeError::transport(e.to_string())
}

fn max_event_time_ms(batch: &RecordBatch, column: &str) -> Result<i64, RuntimeError> {
    let idx = batch
        .schema()
        .index_of(column)
        .map_err(|_| RuntimeError::transport(format!("event_time column '{column}' not found")))?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            RuntimeError::transport(format!("event_time column '{column}' must be Int64"))
        })?;
    let mut max = i64::MIN;
    for row in 0..arr.len() {
        let v = arr.value(row);
        if v > max {
            max = v;
        }
    }
    Ok(max)
}

/// Run windowed aggregation over bounded input batches.
pub fn execute_windowed_stream(
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    if input_batches.is_empty() {
        return Ok(Vec::new());
    }
    if spec.agg_exprs.is_empty() {
        return Err(RuntimeError::transport(
            "windowed stream execution requires at least one aggregate expression",
        ));
    }

    let mut watermark = WatermarkState::new(spec.watermark_lag_ms);
    let mut output = Vec::new();

    match &spec.window_kind {
        LocalWindowKind::Tumbling => {
            let tw_spec = TumblingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                agg_exprs: spec.agg_exprs.clone(),
            };
            if let Some(ttl_ms) = spec.state_ttl_ms {
                let inner = InMemoryStateBackend::new();
                let state: Box<dyn StateBackend> =
                    Box::new(TtlStateBackend::new(inner, TtlConfig::new(ttl_ms)));
                let mut op = StateBackedTumblingWindowOperator::new(
                    tw_spec,
                    state,
                    "local-stream",
                    "tumbling",
                )
                .map_err(|e| RuntimeError::transport(e.to_string()))?;
                for batch in &input_batches {
                    let max_ts = max_event_time_ms(batch, &spec.event_time_column)?;
                    if max_ts > i64::MIN {
                        watermark.advance(max_ts);
                    }
                    let wm = watermark.current_watermark_ms();
                    output.extend(op.process_batch(batch, wm).map_err(exec_err)?);
                }
                output.extend(op.flush_closed_windows(i64::MAX).map_err(exec_err)?);
            } else {
                let mut op = TumblingWindowOperator::new(tw_spec);
                for batch in &input_batches {
                    let max_ts = max_event_time_ms(batch, &spec.event_time_column)?;
                    if max_ts > i64::MIN {
                        watermark.advance(max_ts);
                    }
                    let wm = watermark.current_watermark_ms();
                    output.extend(op.process_batch(batch, wm).map_err(exec_err)?);
                }
                output.extend(op.flush_closed_windows(i64::MAX).map_err(exec_err)?);
            }
        }
        LocalWindowKind::Sliding { slide_ms } => {
            let sw_spec = SlidingWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                window_size_ms: spec.window_size_ms,
                slide_ms: *slide_ms,
                agg_exprs: spec.agg_exprs.clone(),
            };
            let mut op = SlidingWindowOperator::new(sw_spec).map_err(exec_err)?;
            for batch in &input_batches {
                let max_ts = max_event_time_ms(batch, &spec.event_time_column)?;
                if max_ts > i64::MIN {
                    watermark.advance(max_ts);
                }
                let wm = watermark.current_watermark_ms();
                output.extend(op.process_batch(batch, wm).map_err(exec_err)?);
            }
            output.extend(
                op.flush_closed_windows(i64::MAX)
                    .map_err(exec_err)?,
            );
        }
        LocalWindowKind::Session { gap_ms } => {
            let sess_spec = SessionWindowSpec {
                key_column: spec.key_column.clone(),
                event_time_column: spec.event_time_column.clone(),
                session_gap_ms: *gap_ms,
                agg_exprs: spec.agg_exprs.clone(),
            };
            let mut op = SessionWindowOperator::new(sess_spec);
            for batch in &input_batches {
                let max_ts = max_event_time_ms(batch, &spec.event_time_column)?;
                if max_ts > i64::MIN {
                    watermark.advance(max_ts);
                }
                let wm = watermark.current_watermark_ms();
                output.extend(op.process_batch(batch, wm).map_err(exec_err)?);
            }
            output.extend(
                op.flush_closed_sessions(i64::MAX)
                    .map_err(exec_err)?,
            );
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn events_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000, 2_000])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn tumbling_window_produces_closed_buckets() {
        let spec = LocalWindowExecutionSpec {
            key_column: String::from("user_id"),
            event_time_column: String::from("ts"),
            watermark_lag_ms: 0,
            window_kind: LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
        };
        let out =
            execute_windowed_stream(vec![events_batch()], &spec).expect("execute_windowed_stream");
        assert!(!out.is_empty());
        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 2);
    }
}
