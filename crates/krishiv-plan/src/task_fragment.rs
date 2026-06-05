//! Typed task fragments with explicit execution kind (unified batch/streaming).

use crate::{ExecutionKind, NodeOp, PlanError, PlanNode};

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

    pub fn encode(&self) -> Result<String, PlanError> {
        let json = serde_json::to_string(self)
            .map_err(|e| PlanError::Encode(format!("fragment json: {e}")))?;
        Ok(format!("{FRAGMENT_PREFIX}{json}"))
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

    /// Decode a fragment, rejecting legacy untyped strings in durable/production profiles.
    pub fn decode_for_profile(
        fragment: &str,
        profile: krishiv_common::DurabilityProfile,
    ) -> Result<Self, PlanError> {
        if let Some(decoded) = Self::decode(fragment) {
            return Ok(decoded);
        }
        if krishiv_common::allow_legacy_task_fragments(profile) {
            return Ok(Self::decode_or_legacy(fragment));
        }
        Err(PlanError::Validation(format!(
            "legacy untyped task fragment rejected for profile '{}': {}",
            profile,
            fragment.chars().take(120).collect::<String>()
        )))
    }
}

/// Encode a plan node as a typed task fragment string for the scheduler/executor.
pub fn encode_typed_task_fragment(node: &PlanNode) -> Result<String, PlanError> {
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
        let encoded = encode_typed_task_fragment(&node).expect("encode");
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

    #[test]
    fn durable_profile_rejects_legacy_fragment() {
        use krishiv_common::DurabilityProfile;
        let err = TypedTaskFragment::decode_for_profile(
            "stream:tw:key=u",
            DurabilityProfile::SingleNodeDurable,
        )
        .unwrap_err();
        assert!(err.to_string().contains("legacy untyped"));
    }
}

