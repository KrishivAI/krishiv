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
pub mod stream_join;
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
    /// Semi-structured JSON-like data (Spark VARIANT equivalent).
    Variant,
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
///
/// T2 (Spark parity): `LeftSemi`/`RightSemi`/`LeftAnti`/`RightAnti` were
/// previously collapsed to `Inner` when the public `krishiv_api::JoinType`
/// was lowered to the plan layer. They are now first-class variants.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    /// Left semi-join — rows from the left input that have at least one match
    /// in the right input.
    Semi,
    /// Anti-join — rows from the left input that have no match in the right.
    /// (Originally symmetric; preserved for back-compat.)
    Anti,
    /// Left semi-join variant (Spark parity). Equivalent to `Semi` for the
    /// left input; distinguished to match the public API and DataFusion's
    /// 7-variant join enum.
    LeftSemi,
    /// Right semi-join variant (Spark parity). Mirror of `LeftSemi`.
    RightSemi,
    /// Left anti-join variant (Spark parity).
    LeftAnti,
    /// Right anti-join variant (Spark parity).
    RightAnti,
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
    /// AQE skew mitigation: split a hot partition into N sub-partitions by
    /// appending a `salt` column to the join key. The build side is
    /// replicated `factor` times so that each salted sub-partition of the
    /// probe side joins against the full build side in parallel. The
    /// `unsalt` node strips the salt column from the post-join output.
    ///
    /// Equivalent to Spark AQE's `OptimizeSkewedJoin` rule.
    SkewJoin {
        /// The join key columns on both sides (must match).
        keys: Vec<String>,
        /// Number of sub-partitions the hot side is split into.
        factor: u32,
        /// Original join type — kept so the executor can dispatch correctly.
        join_type: JoinType,
    },
    /// Operator not covered by the above variants.
    Other { description: String },
}

/// Whether a plan represents bounded batch work, unbounded streaming, or IVM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecutionKind {
    /// Bounded work that eventually completes.
    Batch,
    /// Unbounded work that runs until cancelled.
    Streaming,
    /// Tick-driven incremental view maintenance (DeltaBatch mode).
    ///
    /// Plans of this kind are managed through the IVM HTTP API on the
    /// coordinator. Each tick consumes source deltas, runs SQL views, and
    /// publishes incremental `DeltaBatch` outputs.
    DeltaBatch,
}

impl fmt::Display for ExecutionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch => f.write_str("batch"),
            Self::Streaming => f.write_str("streaming"),
            Self::DeltaBatch => f.write_str("delta-batch"),
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

#[cfg(test)]
mod gap_tests;

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

    /// T2: every public API `JoinType` must have a first-class plan counterpart
    /// (T2 — was previously collapsed to `Inner` for the four semi/anti
    /// variants).
    #[test]
    fn join_type_variants_are_distinct() {
        let all = [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Semi,
            JoinType::Anti,
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Cross,
            JoinType::NestedLoop,
        ];
        // The four variants added in T2 must be distinct from `Inner`,
        // `Semi`, and `Anti` so a JSON round-trip preserves the join kind.
        assert_ne!(JoinType::LeftSemi, JoinType::Inner);
        assert_ne!(JoinType::RightSemi, JoinType::Inner);
        assert_ne!(JoinType::LeftAnti, JoinType::Inner);
        assert_ne!(JoinType::RightAnti, JoinType::Inner);
        assert_ne!(JoinType::LeftSemi, JoinType::Semi);
        assert_ne!(JoinType::LeftAnti, JoinType::Anti);
        // All variants must be unique among themselves.
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b, "{a:?} and {b:?} must be distinct");
            }
        }
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
            // A real round trip: serialise through serde_json and back, which
            // is how `NodeOp` actually crosses the wire (task fragments are
            // JSON-encoded). The previous body only did `op.clone()` +
            // `assert_eq!`, which exercises the derived `Clone`/`PartialEq`
            // alone — a value is always equal to its own clone, so no
            // production serialization line could be deleted to make it fail.
            let json = serde_json::to_string(op).expect("NodeOp serialises");
            let back: NodeOp = serde_json::from_str(&json).expect("NodeOp round-trips");
            assert_eq!(
                &back, op,
                "NodeOp did not survive a serde round trip: {json}"
            );
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
