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

/// Partitioning strategy for a plan node's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partitioning {
    /// No partitioning — data is not distributed across partitions.
    Unpartitioned,
    /// Hash-based partitioning on named key columns.
    Hash {
        /// Column names used as hash keys.
        keys: Vec<String>,
        /// Number of output buckets.
        buckets: u32,
    },
    /// Round-robin distribution across N buckets.
    RoundRobin {
        /// Number of output buckets.
        buckets: u32,
    },
    /// Broadcast — replicate to all downstream partitions.
    Broadcast,
}

impl fmt::Display for Partitioning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unpartitioned => f.write_str("unpartitioned"),
            Self::Hash { keys, buckets } => {
                write!(f, "hash({}, buckets={})", keys.join(", "), buckets)
            }
            Self::RoundRobin { buckets } => write!(f, "round-robin(buckets={})", buckets),
            Self::Broadcast => f.write_str("broadcast"),
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
    /// Output partitioning strategy for this node.
    partitioning: Partitioning,
    /// Whether this node is eligible for broadcast join optimisation.
    broadcast_eligible: bool,
    /// Estimated output row count, if known.
    estimated_rows: Option<u64>,
}

impl PlanNode {
    /// Create a node with no inputs and default annotations.
    pub fn new(id: impl Into<String>, label: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            kind,
            inputs: Vec::new(),
            partitioning: Partitioning::Unpartitioned,
            broadcast_eligible: false,
            estimated_rows: None,
        }
    }

    /// Attach input node ids to this node.
    #[must_use]
    pub fn with_inputs(mut self, inputs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.inputs = inputs.into_iter().map(Into::into).collect();
        self
    }

    /// Set the output partitioning strategy for this node.
    #[must_use]
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.partitioning = partitioning;
        self
    }

    /// Set whether this node is eligible for broadcast join optimisation.
    #[must_use]
    pub fn with_broadcast_eligible(mut self, broadcast_eligible: bool) -> Self {
        self.broadcast_eligible = broadcast_eligible;
        self
    }

    /// Set the estimated output row count for this node.
    #[must_use]
    pub fn with_estimated_rows(mut self, estimated_rows: Option<u64>) -> Self {
        self.estimated_rows = estimated_rows;
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

    /// Output partitioning strategy.
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    /// Whether this node is eligible for broadcast join optimisation.
    pub fn broadcast_eligible(&self) -> bool {
        self.broadcast_eligible
    }

    /// Estimated output row count.
    pub fn estimated_rows(&self) -> Option<u64> {
        self.estimated_rows
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
        if node.partitioning() != &Partitioning::Unpartitioned {
            output.push_str(&format!(" [partitioning: {}]", node.partitioning()));
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::{ExecutionKind, LogicalPlan, Partitioning, PhysicalPlan, PlanNode};

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

    #[test]
    fn plan_node_default_annotations() {
        let node = PlanNode::new("n1", "label", ExecutionKind::Batch);
        assert_eq!(node.partitioning(), &Partitioning::Unpartitioned);
        assert!(!node.broadcast_eligible());
        assert_eq!(node.estimated_rows(), None);
    }

    #[test]
    fn plan_node_builder_methods() {
        let node = PlanNode::new("n1", "label", ExecutionKind::Batch)
            .with_partitioning(Partitioning::Hash {
                keys: vec!["region".to_string()],
                buckets: 8,
            })
            .with_broadcast_eligible(true)
            .with_estimated_rows(Some(1_000));

        assert_eq!(
            node.partitioning(),
            &Partitioning::Hash {
                keys: vec!["region".to_string()],
                buckets: 8,
            }
        );
        assert!(node.broadcast_eligible());
        assert_eq!(node.estimated_rows(), Some(1_000));
    }

    #[test]
    fn plan_node_round_robin_partitioning() {
        let node = PlanNode::new("n1", "label", ExecutionKind::Batch)
            .with_partitioning(Partitioning::RoundRobin { buckets: 4 });
        assert_eq!(
            node.partitioning(),
            &Partitioning::RoundRobin { buckets: 4 }
        );
    }

    #[test]
    fn plan_node_broadcast_partitioning() {
        let node = PlanNode::new("n1", "label", ExecutionKind::Batch)
            .with_partitioning(Partitioning::Broadcast);
        assert_eq!(node.partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn describe_shows_partitioning_when_not_unpartitioned() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch).with_node(
            PlanNode::new("agg", "aggregate", ExecutionKind::Batch).with_partitioning(
                Partitioning::Hash {
                    keys: vec!["city".to_string()],
                    buckets: 16,
                },
            ),
        );
        let desc = plan.describe();
        assert!(desc.contains("partitioning: hash(city, buckets=16)"));
    }

    #[test]
    fn describe_does_not_show_partitioning_when_unpartitioned() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch).with_node(PlanNode::new(
            "scan",
            "scan",
            ExecutionKind::Batch,
        ));
        let desc = plan.describe();
        assert!(!desc.contains("partitioning:"));
    }

    #[test]
    fn physical_plan_with_broadcast_node() {
        let plan = PhysicalPlan::new("p", ExecutionKind::Batch).with_node(
            PlanNode::new("dim", "dim scan", ExecutionKind::Batch)
                .with_partitioning(Partitioning::Broadcast)
                .with_broadcast_eligible(true)
                .with_estimated_rows(Some(500)),
        );
        let node = &plan.nodes()[0];
        assert_eq!(node.partitioning(), &Partitioning::Broadcast);
        assert!(node.broadcast_eligible());
        assert_eq!(node.estimated_rows(), Some(500));

        let desc = plan.describe();
        assert!(desc.contains("broadcast"));
    }

    #[test]
    fn partitioning_display() {
        assert_eq!(Partitioning::Unpartitioned.to_string(), "unpartitioned");
        assert_eq!(
            Partitioning::Hash {
                keys: vec!["a".to_string(), "b".to_string()],
                buckets: 4
            }
            .to_string(),
            "hash(a, b, buckets=4)"
        );
        assert_eq!(
            Partitioning::RoundRobin { buckets: 2 }.to_string(),
            "round-robin(buckets=2)"
        );
        assert_eq!(Partitioning::Broadcast.to_string(), "broadcast");
    }
}
