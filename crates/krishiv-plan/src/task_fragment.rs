//! Typed task fragments with explicit execution kind (unified batch/streaming).

use crate::{ExecutionKind, NodeOp, PlanNode};

const FRAGMENT_PREFIX: &str = "krishiv-fragment:";

/// Wire- and log-stable task fragment envelope.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TypedTaskFragment {
    pub execution_kind: ExecutionKind,
    pub body: String,
}

impl TypedTaskFragment {
    pub fn new(execution_kind: ExecutionKind, body: impl Into<String>) -> Self {
        Self {
            execution_kind,
            body: body.into(),
        }
    }

    pub fn encode(&self) -> String {
        let json = serde_json::to_string(self).expect("fragment json");
        format!("{FRAGMENT_PREFIX}{json}")
    }

    pub fn decode(fragment: &str) -> Option<Self> {
        let payload = fragment.strip_prefix(FRAGMENT_PREFIX)?;
        serde_json::from_str(payload).ok()
    }

    pub fn execution_kind_from_legacy(fragment: &str) -> ExecutionKind {
        if fragment.starts_with("stream:") {
            return ExecutionKind::Streaming;
        }
        if let Some(op) = crate::decode_task_fragment(fragment) {
            return match op {
                NodeOp::TumblingWindow { .. }
                | NodeOp::SlidingWindow { .. }
                | NodeOp::SessionWindow { .. }
                | NodeOp::StreamSource { .. }
                | NodeOp::Watermark { .. } => ExecutionKind::Streaming,
                _ => ExecutionKind::Batch,
            };
        }
        ExecutionKind::Batch
    }

    pub fn decode_or_legacy(fragment: &str) -> Self {
        Self::decode(fragment).unwrap_or_else(|| {
            Self::new(
                Self::execution_kind_from_legacy(fragment),
                fragment.to_string(),
            )
        })
    }
}

/// Encode a plan node as a typed task fragment string for the scheduler/executor.
pub fn encode_typed_task_fragment(node: &PlanNode) -> String {
    let body = crate::encode_task_fragment(node);
    TypedTaskFragment::new(node.kind(), body).encode()
}

/// Decode execution kind from any fragment representation.
pub fn execution_kind_from_fragment(fragment: &str) -> ExecutionKind {
    TypedTaskFragment::decode_or_legacy(fragment).execution_kind
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeOp, PlanNode};

    #[test]
    fn round_trip_typed_fragment() {
        let node =
            PlanNode::new("w", "win", ExecutionKind::Streaming).with_op(NodeOp::TumblingWindow {
                key_column: String::new(),
                event_time_column: String::new(),
                window_size_ms: 1_000,
                aggs: vec![],
            });
        let encoded = encode_typed_task_fragment(&node);
        let decoded = TypedTaskFragment::decode_or_legacy(&encoded);
        assert_eq!(decoded.execution_kind, ExecutionKind::Streaming);
        assert!(decoded.body.starts_with("stream:"));
    }

    #[test]
    fn legacy_stream_prefix_is_streaming() {
        assert_eq!(
            execution_kind_from_fragment("stream:tw:key=u"),
            ExecutionKind::Streaming
        );
    }
}
