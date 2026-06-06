//! Physical plan lowering: typed [`NodeOp`] → executor task fragments (ADR-DIST-04).

use crate::window::{WindowAgg, WindowExecutionSpec, WindowKind, encode_stream_fragment};
use crate::{NodeOp, PlanNode};
use krishiv_common::validate::{is_safe_identifier, validate_safe_id};

const PLAN_OP_PREFIX: &str = "planop:";

/// Encode a plan node as the executor task fragment description.
///
/// S2: When `node.broadcast_eligible()` is `true` and the node carries a
/// `Hash` Exchange, the partitioning is promoted to `Broadcast` so the
/// physical plan builder honour the flag set by the optimizer / user.
pub fn encode_task_fragment(node: &PlanNode) -> String {
    // S2: Honour broadcast_eligible flag — override Hash Exchange → Broadcast.
    if node.broadcast_eligible() {
        if let Some(crate::NodeOp::Exchange {
            partitioning: crate::Partitioning::Hash { .. } | crate::Partitioning::RoundRobin { .. },
        }) = node.op()
        {
            let broadcast_op = crate::NodeOp::Exchange {
                partitioning: crate::Partitioning::Broadcast,
            };
            if let Ok(json) = serde_json::to_string(&broadcast_op) {
                return format!("{PLAN_OP_PREFIX}{json}");
            }
        }
    }
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
            key_column,
            event_time_column,
            window_size_ms,
            aggs,
        } => Some(encode_stream_fragment(&window_spec(
            WindowKind::Tumbling,
            key_column.clone(),
            event_time_column.clone(),
            *window_size_ms,
            None,
            None,
            aggs,
        ))),
        NodeOp::SlidingWindow {
            key_column,
            event_time_column,
            window_size_ms,
            slide_ms,
            aggs,
        } => Some(encode_stream_fragment(&window_spec(
            WindowKind::Sliding,
            key_column.clone(),
            event_time_column.clone(),
            *window_size_ms,
            Some(*slide_ms),
            None,
            aggs,
        ))),
        NodeOp::SessionWindow {
            key_column,
            event_time_column,
            session_gap_ms,
            aggs,
        } => Some(encode_stream_fragment(&window_spec(
            WindowKind::Session,
            key_column.clone(),
            event_time_column.clone(),
            0,
            None,
            Some(*session_gap_ms),
            aggs,
        ))),
        NodeOp::Scan { table, .. } => {
            // Validate and quote the table identifier to prevent SQL injection
            // through the fragment string. Double-quoted identifiers are the
            // SQL- standard escape mechanism; embedded quotes are doubled.
            validate_safe_id(table, "scan table").ok()?;
            let quoted = format!("\"{}\"", table.replace('"', "\"\""));
            Some(format!("sql:SELECT * FROM {quoted}"))
        }
        NodeOp::Filter { .. } | NodeOp::Project { .. } | NodeOp::Aggregate { .. } => {
            serde_json::to_string(op)
                .ok()
                .map(|json| format!("{PLAN_OP_PREFIX}{json}"))
        }
        NodeOp::StreamSource { source_id, .. } => {
            if !is_safe_identifier(source_id) {
                return None;
            }
            Some(format!("stream-source:{source_id}"))
        }
        NodeOp::StateTtl { ttl_ms } => Some(format!("stream-ttl:{ttl_ms}")),
        other => serde_json::to_string(other)
            .ok()
            .map(|json| format!("{PLAN_OP_PREFIX}{json}")),
    }
}

fn window_spec(
    window_kind: WindowKind,
    key_column: String,
    event_time_column: String,
    window_size_ms: u64,
    slide_ms: Option<u64>,
    session_gap_ms: Option<u64>,
    aggs: &[WindowAgg],
) -> WindowExecutionSpec {
    WindowExecutionSpec {
        key_column,
        key_column_type: String::from("utf8"),
        event_time_column,
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
                key_column: String::new(),
                event_time_column: String::new(),
                window_size_ms: 5_000,
                aggs: vec![WindowAgg::count("count")],
            },
        );
        let frag = encode_task_fragment(&node);
        assert!(frag.starts_with("stream:tw:"));
    }

    #[test]
    fn planop_roundtrip_for_filter() {
        let op = NodeOp::Filter {
            predicate: String::new(),
        };
        let frag = format!("{PLAN_OP_PREFIX}{}", serde_json::to_string(&op).unwrap());
        assert_eq!(decode_task_fragment(&frag), Some(op));
    }

    use crate::ExecutionKind;

    #[test]
    fn scan_table_is_double_quoted_in_fragment() {
        let node = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("orders"),
            filters: vec![],
        });
        let frag = encode_task_fragment(&node);
        assert_eq!(frag, "sql:SELECT * FROM \"orders\"");
    }

    #[test]
    fn scan_table_escapes_embedded_quotes() {
        let node = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("o\"rders"),
            filters: vec![],
        });
        let frag = encode_task_fragment(&node);
        assert_eq!(frag, "sql:SELECT * FROM \"o\"\"rders\"");
    }

    #[test]
    fn scan_table_with_path_traversal_rejected() {
        let node = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("../etc/passwd"),
            filters: vec![],
        });
        let frag = encode_task_fragment(&node);
        // Falls through to legacy_node_description because validation fails.
        assert!(frag.starts_with("scan [batch]"));
    }

    #[test]
    fn stream_source_with_safe_id_encoded() {
        let node = PlanNode::new("src", "source", ExecutionKind::Streaming).with_op(
            NodeOp::StreamSource {
                source_id: String::from("kafka-orders"),
                bounded: false,
            },
        );
        let frag = encode_task_fragment(&node);
        assert_eq!(frag, "stream-source:kafka-orders");
    }

    #[test]
    fn stream_source_with_unsafe_id_rejected() {
        let node = PlanNode::new("src", "source", ExecutionKind::Streaming).with_op(
            NodeOp::StreamSource {
                source_id: String::from("source/id"),
                bounded: false,
            },
        );
        let frag = encode_task_fragment(&node);
        // Falls through to legacy_node_description because validation fails.
        assert!(frag.starts_with("src [streaming]"));
    }
}
