#![forbid(unsafe_code)]

//! Logical and physical plan types for Krishiv.
//!
//! R1 bootstrap keeps these types deliberately small. Later R1 work will bridge
//! them to DataFusion logical and physical plans without exposing DataFusion as
//! the long-term public Krishiv API.

use std::fmt;

/// Data type for a plan schema field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Boolean,
    Int32,
    Int64,
    Float64,
    Utf8,
    Binary,
    Timestamp,
}

/// One field in a plan schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    name: String,
    field_type: FieldType,
    nullable: bool,
}

impl SchemaField {
    /// Create a non-nullable schema field.
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            nullable: false,
        }
    }

    /// Set nullability.
    #[must_use]
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Field type.
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Whether this field is nullable.
    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Output schema for a plan node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlanSchema {
    fields: Vec<SchemaField>,
}

impl PlanSchema {
    /// Create a schema from a list of fields.
    pub fn new(fields: Vec<SchemaField>) -> Self {
        Self { fields }
    }

    /// Schema fields.
    pub fn fields(&self) -> &[SchemaField] {
        &self.fields
    }

    /// Whether this schema has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Join variant used in `NodeOp::Join`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// Typed operator classification for a plan node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeOp {
    /// Table or file scan.
    Scan { table: String },
    /// Row filter.
    Filter,
    /// Column projection.
    Project { columns: Vec<String> },
    /// Aggregation with optional group keys.
    Aggregate { group_keys: Vec<String> },
    /// Join of two inputs.
    Join { join_type: JoinType },
    /// Data exchange / shuffle between partitions.
    Exchange { partitioning: Partitioning },
    /// Output sink.
    Sink { format: String },
    /// Operator not covered by the above variants.
    Other { description: String },
}

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
    /// Typed operator classification.
    op: Option<NodeOp>,
    /// Output schema produced by this node.
    output_schema: PlanSchema,
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
            op: None,
            output_schema: PlanSchema::default(),
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

    /// Set the typed operator classification for this node.
    #[must_use]
    pub fn with_op(mut self, op: NodeOp) -> Self {
        self.op = Some(op);
        self
    }

    /// Set the output schema for this node.
    #[must_use]
    pub fn with_output_schema(mut self, schema: PlanSchema) -> Self {
        self.output_schema = schema;
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

    /// Typed operator classification, if set.
    pub fn op(&self) -> Option<&NodeOp> {
        self.op.as_ref()
    }

    /// Output schema for this node.
    pub fn output_schema(&self) -> &PlanSchema {
        &self.output_schema
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
        if node.broadcast_eligible() {
            output.push_str(" [broadcast-eligible]");
        }
        if let Some(rows) = node.estimated_rows() {
            output.push_str(&format!(" [est-rows: {rows}]"));
        }
    }

    output
}

// ── Plan diffing ──────────────────────────────────────────────────────────────

/// Summary of structural differences between two physical plans.
///
/// Used by operators to understand what changed between two versions of the same
/// job's physical plan (e.g. after an adaptive repartitioning decision in R7/R9).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanDiff {
    /// Node ids present in `after` but not in `before`.
    pub added: Vec<String>,
    /// Node ids present in `before` but not in `after`.
    pub removed: Vec<String>,
    /// Node ids present in both plans but with different labels or operators.
    pub changed: Vec<String>,
}

impl PlanDiff {
    /// Whether the two plans are structurally identical.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Compute the structural diff between two physical plans.
///
/// Nodes are matched by id. A node is "changed" if its label or operator type
/// differs between `before` and `after`.
pub fn diff_plans(before: &PhysicalPlan, after: &PhysicalPlan) -> PlanDiff {
    use std::collections::HashMap;

    let before_map: HashMap<&str, &PlanNode> =
        before.nodes().iter().map(|n| (n.id(), n)).collect();
    let after_map: HashMap<&str, &PlanNode> =
        after.nodes().iter().map(|n| (n.id(), n)).collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for (id, after_node) in &after_map {
        match before_map.get(id) {
            None => added.push((*id).to_owned()),
            Some(before_node) => {
                if before_node.label() != after_node.label()
                    || before_node.op() != after_node.op()
                {
                    changed.push((*id).to_owned());
                }
            }
        }
    }
    for id in before_map.keys() {
        if !after_map.contains_key(id) {
            removed.push((*id).to_owned());
        }
    }

    added.sort();
    removed.sort();
    changed.sort();

    PlanDiff { added, removed, changed }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionKind, FieldType, JoinType, LogicalPlan, NodeOp, Partitioning, PhysicalPlan,
        PlanNode, PlanSchema, SchemaField,
    };

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
    fn plan_node_with_typed_op() {
        let node =
            PlanNode::new("scan", "scan parquet", ExecutionKind::Batch).with_op(NodeOp::Scan {
                table: String::from("orders"),
            });
        assert!(matches!(node.op(), Some(NodeOp::Scan { table }) if table == "orders"));
    }

    #[test]
    fn plan_node_schema_propagation() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("id", FieldType::Int64),
            SchemaField::new("name", FieldType::Utf8).with_nullable(true),
        ]);
        let node = PlanNode::new("proj", "project", ExecutionKind::Batch)
            .with_op(NodeOp::Project {
                columns: vec![String::from("id"), String::from("name")],
            })
            .with_output_schema(schema);
        assert_eq!(node.output_schema().fields().len(), 2);
        assert_eq!(node.output_schema().fields()[0].name(), "id");
        assert_eq!(
            node.output_schema().fields()[0].field_type(),
            &FieldType::Int64
        );
        assert!(!node.output_schema().fields()[0].nullable());
        assert!(node.output_schema().fields()[1].nullable());
    }

    #[test]
    fn plan_schema_empty_by_default() {
        let node = PlanNode::new("n1", "label", ExecutionKind::Batch);
        assert!(node.output_schema().is_empty());
    }

    #[test]
    fn node_op_variants_round_trip() {
        let ops: Vec<NodeOp> = vec![
            NodeOp::Scan {
                table: String::from("t1"),
            },
            NodeOp::Filter,
            NodeOp::Project {
                columns: vec![String::from("a")],
            },
            NodeOp::Aggregate {
                group_keys: vec![String::from("region")],
            },
            NodeOp::Join {
                join_type: JoinType::Inner,
            },
            NodeOp::Exchange {
                partitioning: Partitioning::Broadcast,
            },
            NodeOp::Sink {
                format: String::from("parquet"),
            },
            NodeOp::Other {
                description: String::from("custom"),
            },
        ];
        for op in &ops {
            let cloned = op.clone();
            assert_eq!(&cloned, op);
            // Verify Debug works.
            let _ = format!("{cloned:?}");
        }
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

    // ── PlanDiff ──────────────────────────────────────────────────────────

    fn make_plan(nodes: &[(&str, &str)]) -> PhysicalPlan {
        let mut plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        for (id, label) in nodes {
            plan.add_node(PlanNode::new(*id, *label, ExecutionKind::Batch));
        }
        plan
    }

    #[test]
    fn diff_plans_identical_is_empty() {
        let p = make_plan(&[("scan", "Scan"), ("agg", "Aggregate")]);
        let diff = super::diff_plans(&p, &p);
        assert!(diff.is_empty());
    }

    #[test]
    fn diff_plans_added_node() {
        let before = make_plan(&[("scan", "Scan")]);
        let after = make_plan(&[("scan", "Scan"), ("filter", "Filter")]);
        let diff = super::diff_plans(&before, &after);
        assert_eq!(diff.added, vec!["filter"]);
        assert!(diff.removed.is_empty());
        assert!(diff.changed.is_empty());
    }

    #[test]
    fn diff_plans_removed_node() {
        let before = make_plan(&[("scan", "Scan"), ("filter", "Filter")]);
        let after = make_plan(&[("scan", "Scan")]);
        let diff = super::diff_plans(&before, &after);
        assert!(diff.added.is_empty());
        assert_eq!(diff.removed, vec!["filter"]);
        assert!(diff.changed.is_empty());
    }

    #[test]
    fn diff_plans_changed_label() {
        let before = make_plan(&[("n1", "OldLabel")]);
        let after = make_plan(&[("n1", "NewLabel")]);
        let diff = super::diff_plans(&before, &after);
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.changed, vec!["n1"]);
    }
}
