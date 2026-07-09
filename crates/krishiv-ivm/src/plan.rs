#![forbid(unsafe_code)]

//! Incremental execution plan for IVM views.
//!
//! `build_view_plan` parses a view's SQL via DataFusion and attempts to
//! pattern-match an O(Δ) execution strategy. Falls back to `ViewPlan::DiffBased`
//! for any SQL pattern that cannot be lowered to a known incremental operator.
//!
//! # Supported patterns (O(Δ))
//! - Single-source GROUP BY aggregate → `IncrementalAggOp`
//! - Two-source INNER JOIN → `IncrementalJoinOp` (bilinear probe)
//! - Single-source DISTINCT → `IncrementalDistinctOp`
//!
//! # DiffBased fallback
//! Subqueries, multi-way joins, window functions, OUTER joins, and other
//! complex patterns fall through to full SQL re-execution + diff.

use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::BooleanArray;
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::TreeNode;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Aggregate, Expr, Join, JoinType, LogicalPlan, Projection, Window};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::prelude::SessionContext;

use arrow::record_batch::RecordBatch;
use krishiv_delta::{
    Aggregation, DeltaBatch, DeltaError, DeltaResult, IncrJoinType, IncrementalAggOp,
    IncrementalDistinctOp, IncrementalJoinOp,
};

// ── ViewPlan enum ─────────────────────────────────────────────────────────────

/// Execution plan for one incremental view.
///
/// Variants other than `DiffBased` are O(Δ): they operate only on the
/// incoming delta and maintain state across ticks.
#[allow(clippy::large_enum_variant)]
pub enum ViewPlan {
    /// Stateful group-by aggregate over one source (or upstream view).
    Aggregate {
        source: String,
        op: IncrementalAggOp,
        /// `WHERE` predicate applied to the source delta before aggregation.
        filter: Option<SourceFilter>,
    },
    /// Bilinear inner join: `ΔA ⋈ B_trace + A_trace ⋈ ΔB`.
    Join {
        left_source: String,
        right_source: String,
        op: IncrementalJoinOp,
        /// Predicate applied to the left source delta before probing.
        left_filter: Option<SourceFilter>,
        /// Predicate applied to the right source delta before probing.
        right_filter: Option<SourceFilter>,
    },
    /// Threshold-tracking DISTINCT: emits ±1 only at crossing the 0-threshold.
    Distinct {
        source: String,
        op: IncrementalDistinctOp,
        /// `WHERE` predicate applied to the source delta before de-duplication.
        filter: Option<SourceFilter>,
    },
    /// Fallback: full SQL re-execution + diff against previous output (O(state)).
    DiffBased,
}

/// A compiled `WHERE` predicate applied to a source's delta before it reaches
/// an incremental operator.
///
/// Filter is *linear* (`filter(ΔA) = Δ(filter(A))`), so it composes with any
/// O(Δ) operator with no state of its own: apply the predicate to the incoming
/// delta (and to the snapshot replayed during seeding) and the operator sees
/// exactly the rows the view's `WHERE` admits.
///
/// AUD-1: before this, `source_of_plan` peeled `Filter` nodes transparently and
/// the raw *unfiltered* delta was fed to the operator, so any filtered
/// single-source aggregate returned silently wrong results.
#[derive(Clone)]
pub struct SourceFilter {
    predicate: Arc<dyn PhysicalExpr>,
}

impl SourceFilter {
    /// Keep only the delta rows for which the predicate evaluates to `true`.
    pub fn apply(&self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let predicate = self.predicate.clone();
        krishiv_delta::operators::filter::filter_batch(delta, move |batch| {
            let n = batch.num_rows();
            let value = predicate
                .evaluate(batch)
                .map_err(|e| DeltaError::Operator(format!("filter predicate eval: {e}")))?;
            let array = value
                .into_array(n)
                .map_err(|e| DeltaError::Operator(format!("filter predicate to_array: {e}")))?;
            let mask = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DeltaError::Operator("filter predicate did not evaluate to Boolean".into())
                })?;
            Ok(mask.clone())
        })
    }
}

/// Apply an optional source filter to an optional delta (helper for both the
/// live apply path and snapshot seeding).
pub fn apply_side_filter(
    filter: &Option<SourceFilter>,
    delta: Option<DeltaBatch>,
) -> DeltaResult<Option<DeltaBatch>> {
    match (filter, delta) {
        (Some(f), Some(d)) => Ok(Some(f.apply(d)?)),
        (_, d) => Ok(d),
    }
}

/// Lightweight discriminant for inter-phase communication without borrowing the
/// operator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewPlanKind {
    Incremental,
    DiffBased,
}

impl ViewPlan {
    pub fn kind(&self) -> ViewPlanKind {
        match self {
            ViewPlan::DiffBased => ViewPlanKind::DiffBased,
            _ => ViewPlanKind::Incremental,
        }
    }

    /// Serialize the operator's internal accumulator state, or `None` when the
    /// operator has no losslessly-serializable state (`Join`, whose traces carry
    /// Arrow data, and `DiffBased`, which is stateless). A caller that gets
    /// `None` falls back to [`seed_from_snapshots`](Self::seed_from_snapshots).
    ///
    /// This is what makes an incremental view survive a coordinator restart
    /// *losslessly*, including sources with genuinely duplicate rows: the
    /// materialized source snapshot is a set (multiplicity dropped by
    /// `filter_positive`), so the accumulator cannot be rebuilt from it — only
    /// the operator itself holds the ground truth (G6/F4).
    pub fn checkpoint_state(&self) -> Option<Vec<u8>> {
        match self {
            ViewPlan::Aggregate { op, .. } => Some(op.state_bytes()),
            ViewPlan::Distinct { op, .. } => Some(op.state_bytes()),
            ViewPlan::Join { .. } | ViewPlan::DiffBased => None,
        }
    }

    /// Restore operator state produced by [`checkpoint_state`]. Returns `false`
    /// when this plan variant does not carry restorable state (caller should
    /// seed instead); `true` when the state was applied.
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<bool> {
        match self {
            ViewPlan::Aggregate { op, .. } => {
                op.restore_state_bytes(bytes)?;
                Ok(true)
            }
            ViewPlan::Distinct { op, .. } => {
                op.restore_state_bytes(bytes)?;
                Ok(true)
            }
            ViewPlan::Join { .. } | ViewPlan::DiffBased => Ok(false),
        }
    }

    /// Seed a freshly built incremental operator's internal state from the
    /// current full snapshot(s) of its source(s).
    ///
    /// A checkpoint restore rebuilds the flow's operators **empty** — the
    /// per-group accumulator / join traces / distinct multiplicities live only
    /// in the operator and are not serialized by `checkpoint_full`. Without
    /// seeding, the first delta after a restore is applied against empty state,
    /// so the operator emits an *insertion* for a group that already exists in
    /// the restored view snapshot (no matching retraction), corrupting the
    /// materialized output on the next restore cycle (G6/F4 recreate path).
    ///
    /// `lookup(source)` returns the restored full snapshot of a base source or
    /// upstream view (pre-tick, i.e. before this tick's delta). Replaying it as
    /// an insert-only delta reconstructs the exact operator state the original
    /// flow held; the emitted output is discarded (the view snapshot + baseline
    /// were restored separately, in lockstep). A no-op when the source snapshot
    /// is absent or empty — the normal first-build case, where data has not yet
    /// arrived and the operator *should* start empty.
    pub fn seed_from_snapshots(
        &mut self,
        lookup: impl Fn(&str) -> Option<RecordBatch>,
    ) -> DeltaResult<()> {
        let seed_delta = |name: &str| -> DeltaResult<Option<DeltaBatch>> {
            match lookup(name) {
                Some(snap) if snap.num_rows() > 0 => Ok(Some(DeltaBatch::from_inserts(snap)?)),
                _ => Ok(None),
            }
        };
        match self {
            ViewPlan::Aggregate { source, op, filter } => {
                // AUD-1: the replayed snapshot must pass the same WHERE filter,
                // otherwise the seeded state includes rows the view excludes.
                if let Some(delta) = apply_side_filter(filter, seed_delta(source)?)? {
                    let _ = op.apply(delta)?;
                }
            }
            ViewPlan::Distinct { source, op, filter } => {
                if let Some(delta) = apply_side_filter(filter, seed_delta(source)?)? {
                    let _ = op.apply(delta)?;
                }
            }
            ViewPlan::Join {
                left_source,
                right_source,
                op,
                left_filter,
                right_filter,
            } => {
                let left = apply_side_filter(left_filter, seed_delta(left_source)?)?;
                let right = apply_side_filter(right_filter, seed_delta(right_source)?)?;
                if left.is_some() || right.is_some() {
                    let _ = op.apply(left, right)?;
                }
            }
            ViewPlan::DiffBased => {}
        }
        Ok(())
    }

    /// GC trace state for join operators.
    ///
    /// Each `ViewPlan::Join` is GC'd at the minimum watermark of its own two
    /// sources, not the global minimum across all sources. Using the global
    /// minimum would prevent GC whenever any slow/unwatermarked source exists.
    pub fn gc_watermark(
        &mut self,
        watermarks: &AHashMap<String, i64>,
    ) -> krishiv_delta::DeltaResult<usize> {
        match self {
            ViewPlan::Join {
                left_source,
                right_source,
                op,
                ..
            } => {
                let wm_left = watermarks
                    .get(left_source.as_str())
                    .copied()
                    .unwrap_or(i64::MIN);
                let wm_right = watermarks
                    .get(right_source.as_str())
                    .copied()
                    .unwrap_or(i64::MIN);
                let wm = wm_left.min(wm_right);
                if wm > i64::MIN {
                    op.gc_traces(wm)
                } else {
                    Ok(0)
                }
            }
            ViewPlan::Aggregate { source, op, .. } => {
                let wm = watermarks.get(source.as_str()).copied().unwrap_or(i64::MIN);
                if wm > i64::MIN {
                    op.gc_watermark(wm)
                } else {
                    Ok(0)
                }
            }
            ViewPlan::Distinct { source, op, .. } => {
                let wm = watermarks.get(source.as_str()).copied().unwrap_or(i64::MIN);
                if wm > i64::MIN {
                    op.gc_watermark(wm)
                } else {
                    Ok(0)
                }
            }
            ViewPlan::DiffBased => Ok(0),
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Try to build an O(Δ) `ViewPlan` for a view, falling back to `DiffBased`.
///
/// `available_schemas` maps each known source / upstream view name to its data
/// schema (no `_weight` column). This is needed to construct operators.
pub async fn build_view_plan(
    ctx: &SessionContext,
    body_sql: &str,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> ViewPlan {
    let df = match ctx.sql(body_sql).await {
        Ok(d) => d,
        Err(_) => return ViewPlan::DiffBased,
    };
    let plan = df.logical_plan().clone();
    try_build_from_logical(&plan, output_schema, available_schemas).unwrap_or(ViewPlan::DiffBased)
}

// ── Auto-partition key inference ──────────────────────────────────────────────

/// Inspect a view's SQL and report the single column it can be safely sharded
/// by, or `None` if no safe single-key sharding exists.
///
/// A view is shardable when its output for any key value depends only on input
/// rows carrying that key value. The conservative, provably-correct shape is a
/// **single-column `GROUP BY` aggregate** over one source: every group lives
/// entirely within one shard, so per-shard results concatenate with no
/// cross-shard merge. Multi-column `GROUP BY`, joins (two sources keyed
/// independently), and diff-based views return `None` and run on a single flow.
///
/// This is the "auto" half of unified partitioning for IVM: the engine, not the
/// user, decides whether and how to shard a keyed incremental view.
pub async fn partition_key_for_view(ctx: &SessionContext, body_sql: &str) -> Option<String> {
    let df = ctx.sql(body_sql).await.ok()?;
    let plan = df.logical_plan().clone();
    partition_key_from_logical(&plan)
}

/// Schema-free variant of [`partition_key_for_view`] that inspects the SQL text
/// directly (no `SessionContext`, no source schemas needed).
///
/// The coordinator registers views **before** any data arrives, so source
/// schemas are not yet known and `ctx.sql` cannot plan. This parses the SQL to
/// an AST and applies the same conservative rule: a single top-level `SELECT`
/// with exactly one plain-column `GROUP BY` expression returns that column;
/// anything else (multi-column GROUP BY, joins, set ops, subqueries in the
/// outer position) returns `None`.
pub fn partition_key_from_sql(sql: &str) -> Option<String> {
    use sqlparser::ast::{Expr as SqlExpr, GroupByExpr, SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(query) = stmts.first()? else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let GroupByExpr::Expressions(exprs, modifiers) = &select.group_by else {
        return None;
    };
    if exprs.len() != 1 || !modifiers.is_empty() {
        return None;
    }
    match exprs.first()? {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

fn partition_key_from_logical(plan: &LogicalPlan) -> Option<String> {
    match plan {
        // Peel top-level projections transparently (same as the plan walker).
        LogicalPlan::Projection(Projection { input, .. }) => partition_key_from_logical(input),
        LogicalPlan::Aggregate(agg) => {
            // Exactly one GROUP BY expression, resolvable to a base column.
            if agg.group_expr.len() != 1 {
                return None;
            }
            expr_col_name(agg.group_expr.first()?)
        }
        _ => None,
    }
}

// ── Plan walker ───────────────────────────────────────────────────────────────

fn try_build_from_logical(
    plan: &LogicalPlan,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    match plan {
        // Peel top-level projections transparently.
        LogicalPlan::Projection(Projection { input, .. }) => {
            try_build_from_logical(input, output_schema, available_schemas)
        }
        LogicalPlan::Aggregate(agg) => build_agg_plan(agg, output_schema, available_schemas),
        LogicalPlan::Join(join) => {
            // Only 2-source joins (source_of_plan returns None for multi-way joins
            // where one side is itself a Join node with 2 inputs).
            build_join_plan(join, available_schemas)
        }
        // DISTINCT — the inner plan is the first (and only) input.
        LogicalPlan::Distinct(_) => {
            let inputs = plan.inputs();
            let inner_plan = inputs.first().copied()?;
            let source = source_of_plan(inner_plan)?;
            Some(ViewPlan::Distinct {
                source,
                op: IncrementalDistinctOp::new(),
                // AUD-1: a filtered DISTINCT falls back to DiffBased because
                // `source_of_plan` now refuses to peel `Filter` nodes (returns
                // None → DiffBased). O(Δ) filtered DISTINCT is future work.
                filter: None,
            })
        }
        // Window functions (ROW_NUMBER, RANK, rolling aggregates) cannot be
        // computed O(Δ) in general. Fall through to DiffBased explicitly.
        LogicalPlan::Window(Window { .. }) => None,
        // All other patterns (subqueries, set operations, multi-way joins, etc.)
        // fall back to DiffBased full SQL re-execution.
        _ => None,
    }
}

// ── Aggregate plan builder ────────────────────────────────────────────────────

fn build_agg_plan(
    agg: &Aggregate,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    // AUD-1: resolve the source *and* any WHERE predicate between the aggregate
    // and it. A clean `Aggregate → [Filter…] → [SubqueryAlias] → Scan` chain
    // keeps O(Δ) with the predicate applied to each delta; a compile failure
    // bails to DiffBased (never silently drops the predicate). Chains the strict
    // resolver can't read (e.g. a projection with computed columns) fall through
    // to `source_of_plan`, which now refuses to peel `Filter` — so a dropped
    // WHERE can never slip through as a plain aggregate.
    let (source, filter) = match resolve_source_with_filters(&agg.input) {
        Some((source, preds)) => {
            let schema = available_schemas.get(&source)?;
            let filter = compile_source_filter(&preds, &source, schema).ok()?;
            (source, filter)
        }
        None => (source_of_plan(&agg.input)?, None),
    };
    let input_schema = available_schemas.get(&source)?;

    // Extract GROUP BY column names.
    let group_by: Vec<String> = agg.group_expr.iter().filter_map(expr_col_name).collect();

    // Aggregate output columns = output_schema columns that are NOT in group_by.
    let agg_output_cols: Vec<String> = output_schema
        .fields()
        .iter()
        .filter(|f| !group_by.contains(f.name()))
        .map(|f| f.name().clone())
        .collect();

    if agg.aggr_expr.len() != agg_output_cols.len() {
        return None;
    }

    let mut aggregations: Vec<Aggregation> = Vec::new();
    for (expr, out_col) in agg.aggr_expr.iter().zip(agg_output_cols.iter()) {
        aggregations.push(expr_to_aggregation(expr, out_col)?);
    }

    // AUD-3: honor the view's declared output column types (SUM(Int64)→Int64
    // unless the view declares otherwise) so the incremental snapshot matches
    // the registered contract.
    let op =
        IncrementalAggOp::new_with_output_schema(input_schema, group_by, aggregations, output_schema)
            .ok()?;
    Some(ViewPlan::Aggregate { source, op, filter })
}

// ── Join plan builder ─────────────────────────────────────────────────────────

fn build_join_plan(
    join: &Join,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    let incr_join_type = match join.join_type {
        JoinType::Inner => IncrJoinType::Inner,
        JoinType::Left => IncrJoinType::LeftOuter,
        other => {
            tracing::warn!(
                join_type = ?other,
                "IVM plan degraded to O(state) DiffBased: {:?} join is not \
                 supported by the incremental join operator; only INNER and \
                 LEFT OUTER run in O(Δ) mode",
                other
            );
            return None;
        }
    };

    // AUD-1: resolve each side's source plus any WHERE predicate on that side
    // (e.g. a filtered subquery join input). A predicate that fails to compile
    // bails the whole join to DiffBased rather than dropping the filter.
    let (left_source, left_filter, right_source, right_filter) = {
        let (ls, lf) = resolve_side_with_filter(&join.left, available_schemas)?;
        let (rs, rf) = resolve_side_with_filter(&join.right, available_schemas)?;
        (ls, lf, rs, rf)
    };
    let left_schema = available_schemas.get(&left_source)?;
    let right_schema = available_schemas.get(&right_source)?;

    let mut left_key_cols: Vec<String> = Vec::new();
    let mut right_key_cols: Vec<String> = Vec::new();

    for (left_expr, right_expr) in &join.on {
        left_key_cols.push(expr_col_name(left_expr)?);
        right_key_cols.push(expr_col_name(right_expr)?);
    }

    if left_key_cols.is_empty() {
        return None;
    }

    let op = IncrementalJoinOp::new(
        left_schema.clone(),
        right_schema.clone(),
        left_key_cols,
        right_key_cols,
        incr_join_type,
    )
    .ok()?;

    Some(ViewPlan::Join {
        left_source,
        right_source,
        op,
        left_filter,
        right_filter,
    })
}

/// Resolve one join side to `(source, optional filter)`, mirroring the
/// aggregate resolution: strict `Filter`/`SubqueryAlias`/`Scan` chains keep the
/// predicate O(Δ); otherwise fall back to `source_of_plan` (which refuses to
/// peel `Filter`, so a filtered side that isn't a clean chain → DiffBased).
fn resolve_side_with_filter(
    plan: &LogicalPlan,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<(String, Option<SourceFilter>)> {
    match resolve_source_with_filters(plan) {
        Some((source, preds)) => {
            let schema = available_schemas.get(&source)?;
            let filter = compile_source_filter(&preds, &source, schema).ok()?;
            Some((source, filter))
        }
        None => Some((source_of_plan(plan)?, None)),
    }
}

// ── Source resolution ─────────────────────────────────────────────────────────

/// Walk a plan tree to find the single base table scan, returning its name.
/// Returns `None` for multi-input plans (joins, unions) or unsupported nodes.
///
/// AUD-1: this **refuses to peel `Filter` nodes** (and a `TableScan` carrying
/// pushed-down `filters`). Previously it peeled any single-input node including
/// `Filter`, so the operator was built against a source whose `WHERE` was
/// silently discarded. The filter-aware `resolve_source_with_filters` handles
/// the clean-chain case in O(Δ); anything that reaches a `Filter` here returns
/// `None`, correctly degrading the view to DiffBased full recompute.
fn source_of_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(ts) if ts.filters.is_empty() => {
            Some(ts.table_name.table().to_string())
        }
        // A scan with pushed-down predicates or a Filter node would mean a
        // dropped WHERE — never resolve through it.
        LogicalPlan::TableScan(_) | LogicalPlan::Filter(_) => None,
        LogicalPlan::SubqueryAlias(sa) => source_of_plan(&sa.input),
        _ => {
            let inputs = plan.inputs();
            if inputs.len() == 1 {
                source_of_plan(inputs.first()?)
            } else {
                None
            }
        }
    }
}

/// Resolve the single base source under `plan`, collecting the `Filter`
/// predicates between the operator and that source. Only `SubqueryAlias` and
/// `Filter` nodes are peeled; a clean `Scan` (with no pushed-down filters) ends
/// the walk. Any other node (a projection with computed columns, sort, limit,
/// nested aggregate, multi-input) returns `None`, so the caller falls back to
/// `source_of_plan` or DiffBased.
fn resolve_source_with_filters(plan: &LogicalPlan) -> Option<(String, Vec<Expr>)> {
    match plan {
        LogicalPlan::TableScan(ts) if ts.filters.is_empty() => {
            Some((ts.table_name.table().to_string(), Vec::new()))
        }
        LogicalPlan::SubqueryAlias(sa) => resolve_source_with_filters(&sa.input),
        LogicalPlan::Filter(f) => {
            let (src, mut preds) = resolve_source_with_filters(&f.input)?;
            preds.push(f.predicate.clone());
            Some((src, preds))
        }
        _ => None,
    }
}

/// Compile collected predicates (AND-combined) into a [`SourceFilter`] against
/// the source's data schema.
///
/// - `Ok(None)`  — no predicates, no filtering needed.
/// - `Ok(Some)`  — compiled successfully.
/// - `Err(())`   — the predicate could not be compiled; the caller must fall
///   back to DiffBased rather than silently drop it.
fn compile_source_filter(
    preds: &[Expr],
    source: &str,
    source_schema: &SchemaRef,
) -> Result<Option<SourceFilter>, ()> {
    if preds.is_empty() {
        return Ok(None);
    }
    let combined = preds.iter().cloned().reduce(|a, b| a.and(b)).ok_or(())?;
    // Qualify the schema with the source name so predicate column references of
    // either `source.col` or bare `col` resolve to the right column index.
    let df_schema =
        DFSchema::try_from_qualified_schema(source, source_schema.as_ref()).map_err(|_| ())?;
    // The unoptimized logical predicate is not type-coerced, so a `Float64 >
    // Int64` literal comparison would fail the Arrow comparison kernel at eval.
    // Run type coercion against the source schema to insert the needed casts
    // before lowering to a physical expression.
    let mut coercion = TypeCoercionRewriter::new(&df_schema);
    let coerced = combined.rewrite(&mut coercion).map_err(|_| ())?.data;
    let props = ExecutionProps::new();
    let predicate = create_physical_expr(&coerced, &df_schema, &props).map_err(|_| ())?;
    Ok(Some(SourceFilter { predicate }))
}

// ── Expr helpers ─────────────────────────────────────────────────────────────

fn expr_col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::Alias(alias) => expr_col_name(&alias.expr),
        _ => None,
    }
}

fn expr_to_aggregation(expr: &Expr, output_col: &str) -> Option<Aggregation> {
    match expr {
        Expr::Alias(alias) => expr_to_aggregation(&alias.expr, output_col),
        Expr::AggregateFunction(agg_fn) => {
            let func_name = agg_fn.func.name().to_lowercase();
            match func_name.as_str() {
                "sum" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Sum {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                "count" => {
                    // IVM-6: COUNT(col) excludes nulls; COUNT(*) counts all rows.
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name);
                    Some(Aggregation::Count {
                        output_col: output_col.to_string(),
                        input_col,
                    })
                }
                "avg" | "mean" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Avg {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                "min" | "min_by" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Min {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                "max" | "max_by" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Max {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}
