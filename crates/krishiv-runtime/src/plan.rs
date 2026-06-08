//! Physical plan classification helpers (ADR-12.5).

use krishiv_dataflow::{AggExpr, AggFunction};
use krishiv_plan::window::{WindowAggKind, WindowKind};
use krishiv_plan::{ExecutionKind, NodeOp, PhysicalPlan};

use crate::RuntimeError;
use crate::local_streaming::{LocalWindowExecutionSpec, LocalWindowKind};

/// Returns true when the plan must run through the single-node streaming runtime
/// rather than DataFusion batch execution.
///
/// Classification is based on the plan's [`ExecutionKind`] — not on string
/// prefix matching.  Prior versions used name-based heuristics which could
/// misclassify user SQL containing the literal text "stream:" or "krishiv-stream".
/// ADR-12.5 established that `ExecutionKind::Streaming` is the sole discriminant.
pub fn is_streaming_plan(plan: &PhysicalPlan) -> bool {
    plan.kind() == ExecutionKind::Streaming
}

/// Derive a local window execution spec from typed streaming plan nodes.
pub fn streaming_spec_from_plan(
    plan: &PhysicalPlan,
) -> Result<LocalWindowExecutionSpec, RuntimeError> {
    plan.validate()
        .map_err(|error| RuntimeError::plan_rejected(error.to_string()))?;
    if plan.kind() != ExecutionKind::Streaming {
        return Err(RuntimeError::plan_rejected(
            "streaming_spec_from_plan requires ExecutionKind::Streaming",
        ));
    }

    let mut key_column = String::new();
    let mut key_column_type = String::from("utf8");
    let mut event_time_column = String::new();
    let mut watermark_lag_ms = 0u64;
    let mut window_kind = None;
    let mut window_size_ms = 0u64;
    let mut agg_exprs = LocalWindowExecutionSpec::default_count_agg();
    let mut state_ttl_ms = None;
    let mut source_watermark_lags = std::collections::HashMap::new();
    let mut source_id_column = None;

    for node in plan.nodes() {
        let Some(op) = node.op() else {
            continue;
        };
        match op {
            NodeOp::KeyBy { key_column: key } => key_column = key.clone(),
            NodeOp::Watermark {
                event_time_column: time_col,
                lag_ms,
            } => {
                event_time_column = time_col.clone();
                watermark_lag_ms = *lag_ms;
            }
            NodeOp::Window { spec } => {
                key_column = spec.key_column.clone();
                key_column_type = spec.key_column_type.clone();
                event_time_column = spec.event_time_column.clone();
                watermark_lag_ms = spec.watermark_lag_ms;
                window_size_ms = spec.window_size_ms;
                agg_exprs = window_aggs_to_exec(&spec.agg_exprs);
                source_watermark_lags = spec.source_watermark_lags.clone();
                source_id_column = spec.source_id_column.clone();
                window_kind = Some(match spec.window_kind {
                    WindowKind::Tumbling => LocalWindowKind::Tumbling,
                    WindowKind::Sliding => LocalWindowKind::Sliding {
                        slide_ms: spec.slide_ms.unwrap_or(spec.window_size_ms),
                    },
                    WindowKind::Session => LocalWindowKind::Session {
                        gap_ms: spec.session_gap_ms.unwrap_or(spec.window_size_ms),
                    },
                });
            }
            NodeOp::StateTtl { ttl_ms } => state_ttl_ms = Some(*ttl_ms),
            _ => {}
        }
    }

    let window_kind = match window_kind {
        Some(wk) => wk,
        None => {
            return Err(RuntimeError::plan_rejected(
                "streaming plan has no window operator node",
            ));
        }
    };
    if key_column.is_empty() || event_time_column.is_empty() {
        return Err(RuntimeError::plan_rejected(
            "streaming plan missing key or event-time column",
        ));
    }

    Ok(LocalWindowExecutionSpec {
        key_column,
        key_column_type,
        event_time_column,
        watermark_lag_ms,
        window_kind,
        window_size_ms,
        agg_exprs,
        state_ttl_ms,
        source_watermark_lags,
        source_id_column,
    })
}

fn window_aggs_to_exec(aggs: &[krishiv_plan::window::WindowAgg]) -> Vec<AggExpr> {
    if aggs.is_empty() {
        return LocalWindowExecutionSpec::default_count_agg();
    }
    aggs.iter()
        .map(|agg| {
            let function = match agg.kind {
                WindowAggKind::Count => AggFunction::Count,
                WindowAggKind::Sum => AggFunction::Sum,
                WindowAggKind::Min => AggFunction::Min,
                WindowAggKind::Max => AggFunction::Max,
                WindowAggKind::Avg => AggFunction::Avg,
            };
            AggExpr {
                function,
                input_column: agg.input_column.clone(),
                output_column: agg.output_column.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use krishiv_plan::window::{WindowAgg, WindowExecutionSpec};
    use krishiv_plan::{ExecutionKind, NodeOp, PhysicalPlan, PlanNode};

    use super::*;

    #[test]
    fn streaming_kind_is_streaming_plan() {
        let plan = PhysicalPlan::new("events", ExecutionKind::Streaming);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_kind_with_stream_prefix_is_not_streaming() {
        let plan = PhysicalPlan::new("stream:tw:key=u", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn ordinary_batch_sql_is_not_streaming() {
        let plan = PhysicalPlan::new("sql-query", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_stream_in_name_is_not_streaming() {
        let plan = PhysicalPlan::new("krishiv-stream:events", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_stream_kafka_is_not_streaming() {
        let plan = PhysicalPlan::new("stream-kafka:topic:0:0:records", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_partial_stream_name_not_streaming() {
        let plan = PhysicalPlan::new("my-stream-data", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn empty_name_batch_not_streaming() {
        let plan = PhysicalPlan::new("", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn streaming_with_any_name() {
        let plan = PhysicalPlan::new("anything-at-all", ExecutionKind::Streaming);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_name_stream_colon_is_not_streaming() {
        let plan = PhysicalPlan::new("stream:", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_name_krishiv_stream_is_not_streaming() {
        let plan = PhysicalPlan::new("prefix-krishiv-stream-suffix", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn streaming_spec_from_window_node() {
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 60_000);
        let plan = PhysicalPlan::new("events", ExecutionKind::Streaming).with_node(
            PlanNode::new("w", "win", ExecutionKind::Streaming)
                .with_op(NodeOp::Window { spec: Box::new(spec) }),
        );
        let local_spec = streaming_spec_from_plan(&plan).expect("spec");
        assert_eq!(local_spec.key_column, "user_id");
        assert_eq!(local_spec.event_time_column, "ts");
        assert_eq!(local_spec.window_size_ms, 60_000);
        assert!(matches!(local_spec.window_kind, LocalWindowKind::Tumbling));
    }
}
