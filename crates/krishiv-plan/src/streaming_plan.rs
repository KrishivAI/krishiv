//! Build streaming [`PhysicalPlan`] values from window configuration.

use crate::window::{WindowExecutionSpec, validate_window_execution_spec};
use crate::{ExecutionKind, LogicalPlan, PhysicalPlan, PlanError, PlanNode, lower_to_physical};

/// Build a logical streaming plan for a bounded windowed collect.
pub fn logical_plan_for_window(
    name: impl Into<String>,
    spec: &WindowExecutionSpec,
) -> Result<LogicalPlan, PlanError> {
    validate_window_execution_spec(spec)?;
    let name = name.into();
    let mut plan = LogicalPlan::new(name.clone(), ExecutionKind::Streaming);
    plan.add_node(PlanNode::new(
        "source",
        format!("stream-source:{name}"),
        ExecutionKind::Streaming,
    ));
    plan.add_node(
        PlanNode::new(
            "keyby",
            format!("key-by:{}", spec.key_column),
            ExecutionKind::Streaming,
        )
        .with_op(crate::NodeOp::KeyBy {
            key_column: spec.key_column.clone(),
        })
        .with_inputs(["source".to_string()]),
    );
    plan.add_node(
        PlanNode::new("window", "window".to_string(), ExecutionKind::Streaming)
            .with_op(crate::NodeOp::Window {
                spec: Box::new(spec.clone()),
            })
            .with_inputs(["keyby".to_string()]),
    );
    if let Some(ttl_ms) = spec.state_ttl_ms {
        plan.add_node(
            PlanNode::new(
                "state-ttl",
                format!("ttl:{ttl_ms}"),
                ExecutionKind::Streaming,
            )
            .with_op(crate::NodeOp::StateTtl { ttl_ms })
            .with_inputs(["window".to_string()]),
        );
    }
    Ok(plan)
}

/// Lower a window spec to a physical plan (copies logical nodes with ops).
pub fn physical_plan_for_window(
    name: impl Into<String>,
    spec: &WindowExecutionSpec,
) -> Result<PhysicalPlan, PlanError> {
    let logical = logical_plan_for_window(name, spec)?;
    lower_to_physical(&logical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::WindowExecutionSpec;

    #[test]
    fn physical_plan_carries_window_nodes() {
        let spec = WindowExecutionSpec::tumbling("k", "ts", 1000);
        let physical = physical_plan_for_window("events", &spec).expect("physical plan");
        assert_eq!(physical.kind(), ExecutionKind::Streaming);
        assert!(physical.nodes().len() >= 3);
        physical.validate().expect("valid physical graph");
    }

    #[test]
    fn logical_plan_for_window_validates_spec() {
        use crate::window::{WindowExecutionSpec, WindowKind};
        let mut spec = WindowExecutionSpec::tumbling("k", "ts", 1000);
        spec.window_kind = WindowKind::Sliding;
        // slide_ms is None — must fail after strict validation
        let err = logical_plan_for_window("test", &spec).expect_err("should require slide_ms");
        assert!(err.to_string().contains("slide_ms"), "unexpected: {err}");
    }
}
