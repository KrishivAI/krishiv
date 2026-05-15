#![forbid(unsafe_code)]

//! Logical and physical plan types for Krishiv.
//!
//! R1 bootstrap keeps these types deliberately small. Later R1 work will bridge
//! them to DataFusion logical and physical plans without exposing DataFusion as
//! the long-term public Krishiv API.

use std::fmt;

/// Whether a plan represents bounded batch work or unbounded streaming work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionKind {
    /// Bounded work that eventually completes.
    Batch,
    /// Unbounded work that runs until cancelled.
    Streaming,
}

impl fmt::Display for ExecutionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch => f.write_str("batch"),
            Self::Streaming => f.write_str("streaming"),
        }
    }
}

/// A small bootstrap plan node used by both logical and physical plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanNode {
    id: String,
    label: String,
    kind: ExecutionKind,
    inputs: Vec<String>,
}

impl PlanNode {
    /// Create a node with no inputs.
    pub fn new(id: impl Into<String>, label: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            kind,
            inputs: Vec::new(),
        }
    }

    /// Attach input node ids to this node.
    #[must_use]
    pub fn with_inputs(mut self, inputs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.inputs = inputs.into_iter().map(Into::into).collect();
        self
    }

    /// Stable node id inside a plan.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Human-readable node label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Execution kind for this node.
    pub fn kind(&self) -> ExecutionKind {
        self.kind
    }

    /// Input node ids.
    pub fn inputs(&self) -> &[String] {
        &self.inputs
    }
}

/// Krishiv logical plan wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalPlan {
    name: String,
    kind: ExecutionKind,
    nodes: Vec<PlanNode>,
}

impl LogicalPlan {
    /// Create an empty logical plan.
    pub fn new(name: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nodes: Vec::new(),
        }
    }

    /// Add a node to the plan.
    pub fn add_node(&mut self, node: PlanNode) {
        self.nodes.push(node);
    }

    /// Add a node and return the updated plan.
    #[must_use]
    pub fn with_node(mut self, node: PlanNode) -> Self {
        self.add_node(node);
        self
    }

    /// Plan name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Plan execution kind.
    pub fn kind(&self) -> ExecutionKind {
        self.kind
    }

    /// Plan nodes.
    pub fn nodes(&self) -> &[PlanNode] {
        &self.nodes
    }

    /// Compact textual description for early `EXPLAIN` output.
    pub fn describe(&self) -> String {
        describe_plan("logical", &self.name, self.kind, &self.nodes)
    }
}

/// Krishiv physical plan wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalPlan {
    name: String,
    kind: ExecutionKind,
    nodes: Vec<PlanNode>,
}

impl PhysicalPlan {
    /// Create an empty physical plan.
    pub fn new(name: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nodes: Vec::new(),
        }
    }

    /// Add a node to the plan.
    pub fn add_node(&mut self, node: PlanNode) {
        self.nodes.push(node);
    }

    /// Add a node and return the updated plan.
    #[must_use]
    pub fn with_node(mut self, node: PlanNode) -> Self {
        self.add_node(node);
        self
    }

    /// Plan name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Plan execution kind.
    pub fn kind(&self) -> ExecutionKind {
        self.kind
    }

    /// Plan nodes.
    pub fn nodes(&self) -> &[PlanNode] {
        &self.nodes
    }

    /// Compact textual description for early `EXPLAIN` output.
    pub fn describe(&self) -> String {
        describe_plan("physical", &self.name, self.kind, &self.nodes)
    }
}

fn describe_plan(plan_type: &str, name: &str, kind: ExecutionKind, nodes: &[PlanNode]) -> String {
    let mut output = format!("{plan_type} plan: {name}\nkind: {kind}\nnodes:");
    if nodes.is_empty() {
        output.push_str(" <empty>");
        return output;
    }

    for node in nodes {
        output.push_str(&format!(
            "\n- {} [{}] {}",
            node.id(),
            node.kind(),
            node.label()
        ));
        if !node.inputs().is_empty() {
            output.push_str(&format!(" <- {}", node.inputs().join(", ")));
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::{ExecutionKind, LogicalPlan, PlanNode};

    #[test]
    fn describes_logical_plan_with_nodes() {
        let plan = LogicalPlan::new("demo", ExecutionKind::Batch).with_node(PlanNode::new(
            "scan",
            "scan parquet",
            ExecutionKind::Batch,
        ));

        let description = plan.describe();

        assert!(description.contains("logical plan: demo"));
        assert!(description.contains("scan parquet"));
    }
}
