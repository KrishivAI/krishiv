//! Physical plan lowering: typed [`NodeOp`] → executor task fragments (ADR-DIST-04).

use crate::window::{encode_stream_fragment, WindowAgg, WindowExecutionSpec, WindowKind};
use crate::{NodeOp, PlanNode};

const PLAN_OP_PREFIX: &str = "planop:";

/// Encode a plan node as the executor task fragment description.
pub fn encode_task_fragment(node: &PlanNode) -> String {
    if let Some(fragment) = node.op().and_then(node_op_to_fragment) {
        return fragment;
    }
    legacy_node_description(node)
}

/// Decode a `planop:` fragment back to a node op when present.
pub fn decode_task_fragment(fragment: &str) -> Option<NodeOp> {
    let payload = fragment.strip_prefix(PLAN_OP_PREFIX)?;
    serde_json::from_str(payload).ok()
}

fn node_op_to_fragment(op: &NodeOp) -> Option<String> {
    match op {
        NodeOp::TumblingWindow {
            window_size_ms,
            aggs,
        } => Some(encode_stream_fragment(&window_spec(
            WindowKind::Tumbling,
            *window_size_ms,
            None,
            None,
            aggs,
        ))),
        NodeOp::SlidingWindow {
            window_size_ms,
            slide_ms,
            aggs,
        } => Some(encode_stream_fragment(&window_spec(
            WindowKind::Sliding,
            *window_size_ms,
            Some(*slide_ms),
            None,
            aggs,
        ))),
        NodeOp::SessionWindow {
            session_gap_ms,
            aggs,
        } => Some(encode_stream_fragment(&window_spec(
            WindowKind::Session,
            0,
            None,
            Some(*session_gap_ms),
            aggs,
        ))),
        NodeOp::Scan { table } => Some(format!("sql:SELECT * FROM {table}")),
        NodeOp::Filter | NodeOp::Project { .. } | NodeOp::Aggregate { .. } => {
            serde_json::to_string(op)
                .ok()
                .map(|json| format!("{PLAN_OP_PREFIX}{json}"))
        }
        NodeOp::StreamSource { source_id, .. } => Some(format!("stream-source:{source_id}")),
        NodeOp::StateTtl { ttl_ms } => Some(format!("stream-ttl:{ttl_ms}")),
        other => serde_json::to_string(other)
            .ok()
            .map(|json| format!("{PLAN_OP_PREFIX}{json}")),
    }
}

fn window_spec(
    window_kind: WindowKind,
    window_size_ms: u64,
    slide_ms: Option<u64>,
    session_gap_ms: Option<u64>,
    aggs: &[WindowAgg],
) -> WindowExecutionSpec {
    WindowExecutionSpec {
        key_column: String::from("key"),
        event_time_column: String::from("ts"),
        watermark_lag_ms: 0,
        window_kind,
        window_size_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs: if aggs.is_empty() {
            WindowExecutionSpec::default_count_agg()
        } else {
            aggs.to_vec()
        },
        state_ttl_ms: None,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
    }
}

fn legacy_node_description(node: &PlanNode) -> String {
    if node.inputs().is_empty() {
        format!("{} [{}] {}", node.id(), node.kind(), node.label())
    } else {
        format!(
            "{} [{}] {} <- {}",
            node.id(),
            node.kind(),
            node.label(),
            node.inputs().join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlanNode;

    #[test]
    fn tumbling_node_op_lowers_to_stream_fragment() {
        use crate::ExecutionKind;
        let node = PlanNode::new("w1", "window", ExecutionKind::Streaming).with_op(
            NodeOp::TumblingWindow {
                window_size_ms: 5_000,
                aggs: vec![WindowAgg::count("count")],
            },
        );
        let frag = encode_task_fragment(&node);
        assert!(frag.starts_with("stream:tw:"));
    }

    #[test]
    fn planop_roundtrip_for_filter() {
        let op = NodeOp::Filter;
        let frag = format!("{PLAN_OP_PREFIX}{}", serde_json::to_string(&op).unwrap());
        assert_eq!(decode_task_fragment(&frag), Some(op));
    }
}
