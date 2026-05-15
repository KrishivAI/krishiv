#![forbid(unsafe_code)]

//! Physical execution stubs for Krishiv.
//!
//! This crate will own Arrow physical operators. R1 bootstrap only defines the
//! lowering seam from Krishiv logical plans into Krishiv physical plans.

use krishiv_plan::{LogicalPlan, PhysicalPlan, PlanNode};

/// Bootstrap physical operator categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKind {
    /// Source operator.
    Source,
    /// Projection operator.
    Projection,
    /// Filter operator.
    Filter,
    /// Aggregate operator.
    Aggregate,
    /// Sink operator.
    Sink,
    /// Placeholder for operators not classified in the bootstrap slice.
    Unknown,
}

/// Minimal physical operator descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalOperator {
    name: String,
    kind: OperatorKind,
}

impl PhysicalOperator {
    /// Create an operator descriptor.
    pub fn new(name: impl Into<String>, kind: OperatorKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }

    /// Operator name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Operator kind.
    pub fn kind(&self) -> OperatorKind {
        self.kind
    }
}

/// Lower a logical plan into a physical plan placeholder.
///
/// This is intentionally not a real optimizer or execution engine. It gives R1
/// callers a stable seam to test while DataFusion-backed execution is added.
pub fn lower_to_physical(logical: &LogicalPlan) -> PhysicalPlan {
    let mut physical = PhysicalPlan::new(logical.name(), logical.kind());

    for node in logical.nodes() {
        physical.add_node(
            PlanNode::new(
                format!("physical:{}", node.id()),
                format!("physical {}", node.label()),
                node.kind(),
            )
            .with_inputs(node.inputs().iter().cloned()),
        );
    }

    physical
}

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

    use super::lower_to_physical;

    #[test]
    fn lowers_logical_nodes_to_physical_nodes() {
        let logical = LogicalPlan::new("demo", ExecutionKind::Batch).with_node(PlanNode::new(
            "scan",
            "scan parquet",
            ExecutionKind::Batch,
        ));

        let physical = lower_to_physical(&logical);

        assert_eq!(physical.name(), "demo");
        assert_eq!(physical.nodes().len(), 1);
        assert_eq!(physical.nodes()[0].id(), "physical:scan");
    }
}
