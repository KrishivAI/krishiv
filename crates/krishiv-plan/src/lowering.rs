//! Physical plan lowering: typed [`NodeOp`] → executor task fragments (ADR-DIST-04).

use crate::window::encode_window_execution_spec;
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
    if node.broadcast_eligible()
        && let Some(crate::NodeOp::Exchange {
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
        NodeOp::Window { spec } => encode_window_execution_spec(spec).ok(),
        NodeOp::Scan { table, filters } => {
            validate_safe_id(table, "scan table").ok()?;
            let quoted = format!("\"{}\"", table.replace('"', "\"\""));
            let sql = if filters.is_empty() {
                format!("SELECT * FROM {quoted}")
            } else {
                let where_clause = filters.join(" AND ");
                format!("SELECT * FROM {quoted} WHERE {where_clause}")
            };
            Some(format!("sql:{sql}"))
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
    fn window_node_op_lowers_to_lossless_stream_fragment() {
        use crate::ExecutionKind;
        use crate::window::{WindowAgg, WindowExecutionSpec};
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 5_000);
        let node =
            PlanNode::new("w1", "window", ExecutionKind::Streaming).with_op(NodeOp::Window {
                spec: Box::new(spec),
            });
        let frag = encode_task_fragment(&node);
        assert!(
            frag.starts_with("stream:spec:v1:"),
            "expected lossless format, got: {frag}"
        );
        let _ = WindowAgg::count("count");
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
    fn scan_includes_pushed_down_filters() {
        let node = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("orders"),
            filters: vec![
                String::from("amount > 100"),
                String::from("status = 'active'"),
            ],
        });
        let frag = encode_task_fragment(&node);
        assert_eq!(
            frag,
            "sql:SELECT * FROM \"orders\" WHERE amount > 100 AND status = 'active'"
        );
    }

    #[test]
    fn scan_with_single_filter() {
        let node = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("users"),
            filters: vec![String::from("age >= 18")],
        });
        let frag = encode_task_fragment(&node);
        assert_eq!(frag, "sql:SELECT * FROM \"users\" WHERE age >= 18");
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
