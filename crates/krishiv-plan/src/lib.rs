#![forbid(unsafe_code)]

//! Logical and physical plan types for Krishiv.
//!
//! R1 bootstrap keeps these types deliberately small. Later R1 work will bridge
//! them to DataFusion logical and physical plans without exposing DataFusion as
//! the long-term public Krishiv API.

use std::fmt;

pub mod cep;
pub mod expression;
pub mod governance;
mod graph;
mod lowering;
pub mod optimizer;
pub mod task_fragment;
pub mod udf;
pub mod window;
pub use expression::{
    AggregateFunction as ExprAggregateFunction, BinaryOperator as ExprBinaryOperator,
    EXPRESSION_FORMAT_VERSION, Expr, ExprDataType, ExprField, IntervalUnit, NullOrdering,
    ScalarValue, SortDirection, TimeUnit,
};
pub use graph::lower_to_physical;
pub use task_fragment::{
    TASK_FRAGMENT_VERSION, TypedTaskFragment, encode_typed_task_fragment,
    execution_kind_from_fragment, task_body_for_profile, validate_job_fragments,
};

/// Errors returned by plan encoding, decoding, and validation operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// Failed to parse a plan fragment or expression.
    #[error("plan parse error: {0}")]
    Parse(String),
    /// Failed to encode a plan fragment to wire format.
    #[error("plan encode error: {0}")]
    Encode(String),
    /// Plan validation failed (e.g. missing required fields).
    #[error("plan validation error: {0}")]
    Validation(String),
}

/// Data type for a plan schema field.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    /// Cartesian product — no join predicate (E2.3).
    Cross,
    /// Nested-loop join; used for non-equi predicates (E2.3).
    NestedLoop,
}

/// Typed operator classification for a plan node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeOp {
    /// Table or file scan, with optional pushed-down filter predicates.
    Scan { table: String, filters: Vec<String> },
    /// Row filter with a predicate expression string.
    Filter { predicate: String },
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
    /// AQE coalesce: merge many small partitions into fewer larger ones.
    ///
    /// Inserted by the AQE `CoalesceRule` when runtime statistics show that
    /// partition count can be reduced to improve downstream task efficiency.
    CoalescePartitions {
        /// Number of output partitions after coalescing.
        target_partitions: usize,
    },
    /// Create a live table backed by a streaming query.
    CreateLiveTable { name: String, query: String },
    /// Refresh materialized state for a live table.
    RefreshLiveTable { name: String },
    /// Drop a live table.
    DropLiveTable { name: String },
    /// Key stream by column before windowing.
    KeyBy { key_column: String },
    /// Event-time watermark on a keyed stream.
    Watermark {
        event_time_column: String,
        lag_ms: u64,
    },
    /// Windowed streaming operator (tumbling, sliding, or session window).
    Window {
        spec: Box<window::WindowExecutionSpec>,
    },
    /// Bounded or unbounded stream source.
    StreamSource { source_id: String, bounded: bool },
    /// Operator state TTL for streaming nodes.
    StateTtl { ttl_ms: u64 },
    /// E2.2: Globally-sorted output produced by a three-stage pipeline:
    /// local sort → range-partition shuffle → merge-sort.  The executor
    /// treats this as a batch pipeline that produces a single sorted partition.
    GlobalSort {
        /// Ordered list of `(column, ascending)` sort keys.
        keys: Vec<(String, bool)>,
    },
    /// E2.2 / E2.4: Sort-merge join using pre-sorted, range-partitioned inputs.
    SortMergeJoin {
        join_type: JoinType,
        /// Column names used as equi-join keys (must match sort order).
        left_keys: Vec<String>,
        right_keys: Vec<String>,
    },
    /// E3.2: Time-windowed join: buffer both streams in the window interval,
    /// emit matched pairs when the window closes.
    WindowJoin {
        join_type: JoinType,
        /// Column names used as equi-join keys.
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        /// Event-time column used to determine window membership.
        time_column: String,
        /// Window duration in milliseconds.
        window_ms: u64,
    },
    /// E5.2: Expand an array-typed column into one row per element.
    ///
    /// Equivalent to `UNNEST(array_column)` in SQL or a LATERAL join over an
    /// array.  The `output_column` name is used for the expanded element.
    /// If `with_ordinality` is `true` an extra `ordinality` column (`u64`) is
    /// appended with the 1-based position of each element.
    Unnest {
        array_column: String,
        output_column: String,
        with_ordinality: bool,
    },
    /// E5.3: Recursive CTE — iterative fixpoint execution.
    ///
    /// Executes `base_query` once to seed the accumulator, then repeatedly
    /// executes `recursive_query` (which may reference the CTE name) and unions
    /// the new rows into the accumulator until either no new rows are produced
    /// (fixpoint) or `max_iterations` is reached.
    RecursiveCte {
        /// CTE name visible inside `recursive_query`.
        name: String,
        /// The non-recursive seed query.
        base_query: String,
        /// The recursive query that may reference `name`.
        recursive_query: String,
        /// Hard cap on iterations to prevent infinite loops.
        max_iterations: u32,
    },
    /// CEP sequential pattern match on a keyed stream.
    ///
    /// `stage_column` names the column whose string value identifies which
    /// pattern stage each row belongs to.  The executor groups rows by
    /// `key_column`, routes each row to `PartitionedCepMatcher::process_event`
    /// with the row's stage name, and emits concatenated match batches.
    Cep {
        key_column: String,
        event_time_column: String,
        stage_column: String,
    },
    /// Operator not covered by the above variants.
    Other { description: String },
}

/// Whether a plan represents bounded batch work or unbounded streaming work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// E2.4: Range-based partitioning using sampled sort key boundaries.
    ///
    /// Rows whose sort key falls in `[boundaries[i-1], boundaries[i])` go to
    /// partition `i`.  Used by `GlobalSort` / `SortMergeJoin` pipelines.
    Range {
        /// Sort key columns (each `(column, ascending)`).
        keys: Vec<(String, bool)>,
        /// Sampled boundary values (serialised as JSON strings).
        /// There are `buckets - 1` boundaries for `buckets` output partitions.
        boundaries: Vec<String>,
        /// Number of output partitions.
        buckets: u32,
    },
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
            Self::Range { keys, buckets, .. } => {
                let key_str = keys
                    .iter()
                    .map(|(c, asc)| format!("{} {}", c, if *asc { "ASC" } else { "DESC" }))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "range({key_str}, buckets={buckets})")
            }
        }
    }
}

/// A small bootstrap plan node used by both logical and physical plans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

    /// Replace the human-readable node label.
    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
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

    /// Attach an exchange (repartition) node to the plan.
    ///
    /// This is a convenience wrapper around `with_partitioning` that creates a
    /// `Hash` partitioning on the given key columns with `num_partitions`
    /// buckets.  Used by the `DataFrame::repartition()` API.
    #[must_use]
    pub fn with_exchange(
        self,
        key_columns: impl IntoIterator<Item = impl Into<String>>,
        num_partitions: u32,
    ) -> Self {
        self.with_partitioning(Partitioning::Hash {
            keys: key_columns.into_iter().map(Into::into).collect(),
            buckets: num_partitions,
        })
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

    /// Mutate the output partitioning strategy in-place.
    pub fn set_partitioning(&mut self, partitioning: Partitioning) {
        self.partitioning = partitioning;
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

/// Maximum number of nodes allowed in a single plan.
///
/// Prevents adversarial or accidental plans from causing stack overflows in
/// recursive plan walkers or excessive memory allocation (S7).
pub const MAX_PLAN_NODES: usize = 10_000;

/// Shared core fields for logical and physical plans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PlanCore {
    pub(crate) name: String,
    pub(crate) kind: ExecutionKind,
    pub(crate) nodes: Vec<PlanNode>,
    /// Override for shuffle partition count (`SET shuffle.partitions = N`).
    /// When `Some`, `AutoPartitionRule` uses this as the target bucket count
    /// instead of computing from data size.
    shuffle_partitions: Option<u32>,
}

impl PlanCore {
    fn new(name: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nodes: Vec::new(),
            shuffle_partitions: None,
        }
    }

    fn add_node(&mut self, node: PlanNode) {
        self.nodes.push(node);
    }

    fn with_node(mut self, node: PlanNode) -> Self {
        self.add_node(node);
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> ExecutionKind {
        self.kind
    }

    fn nodes(&self) -> &[PlanNode] {
        &self.nodes
    }

    fn nodes_mut(&mut self) -> &mut [PlanNode] {
        &mut self.nodes
    }

    fn shuffle_partitions(&self) -> Option<u32> {
        self.shuffle_partitions
    }

    fn with_shuffle_partitions(mut self, n: Option<u32>) -> Self {
        self.shuffle_partitions = n;
        self
    }
}

/// Krishiv logical plan wrapper.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogicalPlan {
    pub(crate) core: PlanCore,
}

impl LogicalPlan {
    /// Create an empty logical plan.
    pub fn new(name: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            core: PlanCore::new(name, kind),
        }
    }

    /// Add a node to the plan.
    pub fn add_node(&mut self, node: PlanNode) {
        self.core.add_node(node);
    }

    /// Add a node and return the updated plan.
    #[must_use]
    pub fn with_node(mut self, node: PlanNode) -> Self {
        self.core = self.core.with_node(node);
        self
    }

    /// Plan name.
    pub fn name(&self) -> &str {
        self.core.name()
    }

    /// Plan execution kind.
    pub fn kind(&self) -> ExecutionKind {
        self.core.kind()
    }

    /// Plan nodes.
    pub fn nodes(&self) -> &[PlanNode] {
        self.core.nodes()
    }

    /// Validate node identifiers, input references, and graph acyclicity.
    pub fn validate(&self) -> Result<(), PlanError> {
        graph::validate_plan("logical", self.name(), self.nodes())
    }

    /// Compact textual description for early `EXPLAIN` output.
    pub fn describe(&self) -> String {
        describe_plan(
            "logical",
            self.core.name(),
            self.core.kind(),
            self.core.nodes(),
        )
    }

    /// Return the shuffle partition override, if set.
    pub fn shuffle_partitions(&self) -> Option<u32> {
        self.core.shuffle_partitions()
    }

    /// Set the shuffle partition override for this plan.
    #[must_use]
    pub fn with_shuffle_partitions(mut self, n: Option<u32>) -> Self {
        self.core = self.core.with_shuffle_partitions(n);
        self
    }
}

/// Krishiv physical plan wrapper.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalPlan {
    pub(crate) core: PlanCore,
    /// Post-AQE coalesced partition count set by `CoalesceRule::apply`.
    /// `None` means coalescing has not been applied.
    coalesced_partition_count: Option<usize>,
}

impl PhysicalPlan {
    /// Create an empty physical plan.
    pub fn new(name: impl Into<String>, kind: ExecutionKind) -> Self {
        Self {
            core: PlanCore::new(name, kind),
            coalesced_partition_count: None,
        }
    }

    /// Return the post-AQE coalesced partition count, if set by `CoalesceRule`.
    pub fn coalesced_partition_count(&self) -> Option<usize> {
        self.coalesced_partition_count
    }

    /// Set the coalesced partition count (called by `CoalesceRule::apply`).
    #[must_use]
    pub fn with_coalesced_partition_count(mut self, count: usize) -> Self {
        self.coalesced_partition_count = Some(count);
        self
    }

    /// Add a node to the plan.
    pub fn add_node(&mut self, node: PlanNode) {
        self.core.add_node(node);
    }

    /// Add a node and return the updated plan.
    #[must_use]
    pub fn with_node(mut self, node: PlanNode) -> Self {
        self.core = self.core.with_node(node);
        self
    }

    /// Plan name.
    pub fn name(&self) -> &str {
        self.core.name()
    }

    /// Plan execution kind.
    pub fn kind(&self) -> ExecutionKind {
        self.core.kind()
    }

    /// Plan nodes (read-only access).
    pub fn nodes(&self) -> &[PlanNode] {
        self.core.nodes()
    }

    /// Plan nodes (mutable access).
    ///
    /// Used by AQE rules such as `AutoPartitionRule` to adjust partition counts
    /// on `Exchange` nodes without rebuilding the entire plan graph.
    pub fn nodes_mut(&mut self) -> &mut [PlanNode] {
        self.core.nodes_mut()
    }

    /// Return the shuffle partition override, if set.
    pub fn shuffle_partitions(&self) -> Option<u32> {
        self.core.shuffle_partitions()
    }

    /// Set the shuffle partition override for this plan.
    #[must_use]
    pub fn with_shuffle_partitions(mut self, n: Option<u32>) -> Self {
        self.core = self.core.with_shuffle_partitions(n);
        self
    }

    /// Validate node identifiers, input references, and graph acyclicity.
    pub fn validate(&self) -> Result<(), PlanError> {
        graph::validate_plan("physical", self.name(), self.nodes())
    }

    /// Compact textual description for early `EXPLAIN` output.
    pub fn describe(&self) -> String {
        describe_plan(
            "physical",
            self.core.name(),
            self.core.kind(),
            self.core.nodes(),
        )
    }
}

fn describe_plan(plan_type: &str, name: &str, kind: ExecutionKind, nodes: &[PlanNode]) -> String {
    use std::fmt::Write;
    let mut output = format!("{plan_type} plan: {name}\nkind: {kind}\nnodes:");
    if nodes.is_empty() {
        output.push_str(" <empty>");
        return output;
    }

    for node in nodes {
        write!(
            output,
            "\n- {} [{}] {}",
            node.id(),
            node.kind(),
            node.label()
        )
        .unwrap();
        if !node.inputs().is_empty() {
            write!(output, " <- {}", node.inputs().join(", ")).unwrap();
        }
        if node.partitioning() != &Partitioning::Unpartitioned {
            write!(output, " [partitioning: {}]", node.partitioning()).unwrap();
        }
        if node.broadcast_eligible() {
            output.push_str(" [broadcast-eligible]");
        }
        if let Some(rows) = node.estimated_rows() {
            write!(output, " [est-rows: {rows}]").unwrap();
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
/// Nodes are matched by id. A node is "changed" if any of its label, operator,
/// inputs, partitioning, estimated row count, or output schema differs between
/// `before` and `after`.
#[must_use]
pub fn diff_plans(before: &PhysicalPlan, after: &PhysicalPlan) -> PlanDiff {
    use std::collections::HashMap;

    let before_map: HashMap<&str, &PlanNode> = before.nodes().iter().map(|n| (n.id(), n)).collect();
    let after_map: HashMap<&str, &PlanNode> = after.nodes().iter().map(|n| (n.id(), n)).collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for (id, after_node) in &after_map {
        match before_map.get(id) {
            None => added.push((*id).to_owned()),
            Some(before_node) => {
                let structurally_different = before_node.label() != after_node.label()
                    || before_node.op() != after_node.op()
                    || before_node.inputs() != after_node.inputs()
                    || before_node.partitioning() != after_node.partitioning()
                    || before_node.estimated_rows() != after_node.estimated_rows()
                    || before_node.output_schema() != after_node.output_schema();
                if structurally_different {
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

    PlanDiff {
        added,
        removed,
        changed,
    }
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
                filters: vec![],
            });
        assert!(matches!(node.op(), Some(NodeOp::Scan { table, .. }) if table == "orders"));
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
                filters: vec![],
            },
            NodeOp::Filter {
                predicate: String::new(),
            },
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
            NodeOp::CoalescePartitions {
                target_partitions: 4,
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

    #[test]
    fn diff_plans_detects_changed_partitioning() {
        let mut before = PhysicalPlan::new("test", ExecutionKind::Batch);
        before.add_node(
            PlanNode::new("n1", "label", ExecutionKind::Batch)
                .with_partitioning(Partitioning::Unpartitioned),
        );
        let mut after = PhysicalPlan::new("test", ExecutionKind::Batch);
        after.add_node(
            PlanNode::new("n1", "label", ExecutionKind::Batch)
                .with_partitioning(Partitioning::Broadcast),
        );
        let diff = super::diff_plans(&before, &after);
        assert_eq!(diff.changed, vec!["n1"]);
    }

    #[test]
    fn diff_plans_detects_changed_estimated_rows() {
        let mut before = PhysicalPlan::new("test", ExecutionKind::Batch);
        before.add_node(
            PlanNode::new("n1", "label", ExecutionKind::Batch).with_estimated_rows(Some(100)),
        );
        let mut after = PhysicalPlan::new("test", ExecutionKind::Batch);
        after.add_node(
            PlanNode::new("n1", "label", ExecutionKind::Batch).with_estimated_rows(Some(200)),
        );
        let diff = super::diff_plans(&before, &after);
        assert_eq!(diff.changed, vec!["n1"]);
    }

    #[test]
    fn diff_plans_detects_changed_inputs() {
        let mut before = PhysicalPlan::new("test", ExecutionKind::Batch);
        before.add_node(PlanNode::new("src", "source", ExecutionKind::Batch));
        before.add_node(PlanNode::new("n1", "label", ExecutionKind::Batch).with_inputs(["src"]));
        let mut after = PhysicalPlan::new("test", ExecutionKind::Batch);
        after.add_node(PlanNode::new("src", "source", ExecutionKind::Batch));
        after.add_node(PlanNode::new("n1", "label", ExecutionKind::Batch)); // no inputs
        let diff = super::diff_plans(&before, &after);
        assert_eq!(diff.changed, vec!["n1"]);
    }

    #[test]
    fn graph_rejects_duplicate_input_edges() {
        let plan = LogicalPlan::new("dup-edges", ExecutionKind::Batch)
            .with_node(PlanNode::new("src", "source", ExecutionKind::Batch))
            .with_node(
                PlanNode::new("n1", "node", ExecutionKind::Batch).with_inputs(["src", "src"]),
            );
        let err = plan.validate().expect_err("duplicate inputs must fail");
        assert!(
            err.to_string().contains("duplicate input"),
            "unexpected: {err}"
        );
    }
}
