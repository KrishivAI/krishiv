#![forbid(unsafe_code)]

//! Incremental execution plan for IVM views.
//!
//! `build_view_plan` parses a view's SQL via DataFusion and attempts to
//! pattern-match an O(Δ) execution strategy. Falls back to `ViewPlan::DiffBased`
//! for any SQL pattern that cannot be lowered to a known incremental operator.
//!
//! # Supported patterns (O(Δ))
//! - Single-source GROUP BY aggregate → `IncrementalAggOp`
//! - Two-source INNER / LEFT OUTER equi-JOIN → `IncrementalJoinOp` (bilinear
//!   probe), including a `WHERE` above the join whose conjuncts each touch
//!   only one side (pushed onto that side's delta; right-side pushdown is
//!   inner-join only — under LEFT OUTER it would change null-padding)
//! - Single-source DISTINCT → `IncrementalDistinctOp`
//!
//! # DiffBased fallback
//! Subqueries, multi-way joins, window functions, non-equi or cross-side
//! join predicates, RIGHT/FULL OUTER joins, and other complex patterns fall
//! through to full SQL re-execution + diff.

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

    /// AUD-9 (loud degradation): a short human description of how this view
    /// executes, surfaced on the `debug-info` endpoint so an operator can see —
    /// and act on — a view silently running full-recompute instead of O(Δ).
    pub fn describe(&self) -> &'static str {
        match self {
            ViewPlan::Aggregate { .. } => {
                "incremental aggregate — retract/insert only the changed groups per delta"
            }
            ViewPlan::Distinct { .. } => "incremental DISTINCT — multiset add/remove per delta",
            ViewPlan::Join { .. } => {
                "incremental equi-join — symmetric hash trace; probes only the delta rows"
            }
            ViewPlan::DiffBased => {
                "full recompute (DiffBased) — no O(Δ) plan matched this view shape (needs a \
                 single-source GROUP BY aggregate, DISTINCT, or equi-join with supported \
                 per-side filters); the tick re-runs the whole view SQL and diffs the result"
            }
        }
    }

    /// Serialize the operator's internal accumulator state, or `None` when the
    /// operator has none (`DiffBased` is stateless). A caller that gets `None`
    /// falls back to [`seed_from_snapshots`](Self::seed_from_snapshots).
    ///
    /// This is what makes an incremental view survive a coordinator restart
    /// *losslessly*, including sources with genuinely duplicate rows: the
    /// materialized source snapshot is a set (multiplicity dropped by
    /// `filter_positive`), so the accumulator cannot be rebuilt from it — only
    /// the operator itself holds the ground truth (G6/F4). Join traces
    /// serialize their Z-sets via Arrow IPC (#160), which also spares the
    /// distributed `delta:step:` path from rebuilding join hash state from
    /// full source snapshots on every offloaded tick.
    pub fn checkpoint_state(&self) -> Option<Vec<u8>> {
        match self {
            ViewPlan::Aggregate { op, .. } => Some(op.state_bytes()),
            ViewPlan::Distinct { op, .. } => Some(op.state_bytes()),
            // Trace serialization is fallible (IPC); on failure fall back to
            // snapshot seeding rather than failing the whole checkpoint.
            ViewPlan::Join { op, .. } => match op.state_bytes() {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "join trace checkpoint failed; restore will re-seed \
                         from source snapshots (multiplicity-lossy)"
                    );
                    None
                }
            },
            ViewPlan::DiffBased => None,
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
            ViewPlan::Join { op, .. } => {
                op.restore_state_bytes(bytes)?;
                Ok(true)
            }
            ViewPlan::DiffBased => Ok(false),
        }
    }

    /// Seed a freshly built incremental operator's internal state from the
    /// current full snapshot(s) of its source(s).
    ///
    /// This is the **fallback** path: `checkpoint_full` serializes operator
    /// state (aggregates/distinct accumulators and, since #160, join traces),
    /// and restore prefers those bytes. Seeding covers checkpoints written
    /// before join-state serialization existed, a failed state decode, and the
    /// normal first-build case. Without either, the first delta after a
    /// restore is applied against empty state, so the operator emits an
    /// *insertion* for a group that already exists in the restored view
    /// snapshot (no matching retraction), corrupting the materialized output
    /// on the next restore cycle (G6/F4 recreate path). Note seeding replays
    /// the *materialized* snapshot — a set — so duplicate-row multiplicity is
    /// not recoverable on this path; the checkpointed bytes are.
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
///
/// Planning runs against an **ephemeral schema-only context**: the plan is
/// determined by the SQL's structure, never by which sources happen to hold
/// rows this tick. Planning against the tick's data context made an
/// empty/emptied source fail `ctx.sql` and pin the view to DiffBased — fatal
/// after a checkpoint restore, which rebuilds plans lazily (#160).
pub async fn build_view_plan(
    body_sql: &str,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
) -> ViewPlan {
    use datafusion::datasource::MemTable;
    let ctx = SessionContext::new();
    for (name, schema) in available_schemas {
        let empty = RecordBatch::new_empty(schema.clone());
        if let Ok(table) = MemTable::try_new(schema.clone(), vec![vec![empty]]) {
            let _ = ctx.register_table(name.as_str(), Arc::new(table));
        }
    }
    let df = match ctx.sql(body_sql).await {
        Ok(d) => d,
        Err(_) => return ViewPlan::DiffBased,
    };
    let plan = df.logical_plan().clone();
    try_build_from_logical(&plan, output_schema, available_schemas, lateness)
        .unwrap_or(ViewPlan::DiffBased)
}

// ── Auto-partition key inference ──────────────────────────────────────────────

/// Inspect a view's SQL and report the single column it can be safely sharded
/// by, or `None` if no safe single-key sharding exists.
///
/// # The rule this enforces
///
/// A view is shardable when, for every key value `k`, the view's output rows
/// for `k` depend only on input rows carrying `k` **and** the whole view's
/// output is the concatenation of those per-key results. The only shape this
/// function is willing to prove is a **single-column `GROUP BY` aggregate over
/// exactly one plain table**, with nothing above or beside the aggregation
/// that can see across groups.
///
/// # What it refuses, and why (IVM-AUD-PART-6)
///
/// The predecessor of this function looked at `GROUP BY` and nothing else, so
/// it declared these shardable and got each of them wrong:
///
/// * `LIMIT n` / `FETCH` — the limit is applied *inside every shard*, so an
///   N-shard job returns up to `n × N` rows and, with `ORDER BY`, a top-N over
///   the wrong candidate set.
/// * `ORDER BY` — per-shard results are concatenated, which destroys the
///   ordering the query asked for.
/// * A join, or any second table in `FROM` — sharding by the group key
///   co-locates rows by the *group* key, not the join key, so matching pairs
///   land in different shards and silently disappear (or the dimension source
///   hard-errors at feed time for lacking the key column at all).
/// * Any subquery — a FROM-clause or scalar subquery is evaluated once per
///   shard over that shard's rows, so a `(SELECT SUM(x) FROM t)` denominator
///   becomes the shard's sum instead of the table's.
/// * A projection alias that shadows the key (`SELECT other AS region … GROUP
///   BY region`) — rows are routed by the *input* column named `region`, which
///   is not the column the query groups on.
/// * Window functions, `DISTINCT`, `QUALIFY`, `CLUSTER/DISTRIBUTE/SORT BY`,
///   set operations, CTEs and multi-statement input — each can see across
///   groups or across shards.
///
/// `WHERE`, `HAVING`, and any aggregate function are accepted: they are
/// per-row or per-group, and a group lives entirely inside one shard.
///
/// # Why the SQL text and not a `LogicalPlan`
///
/// The coordinator registers views **before** any data arrives, so source
/// schemas are not yet known and `SessionContext::sql` cannot plan. This
/// parses to a `sqlparser` AST instead. There used to be a second,
/// logical-plan-based detector here that disagreed with this one in both
/// directions (it caught `LIMIT`, because a `Limit` node sits above the
/// `Aggregate`, but happily sharded a join, because it read the `Aggregate`'s
/// group expression without ever looking at its input) — and it was reachable
/// only from a function with no production callers. It is gone: one detector,
/// used everywhere (IVM-AUD-PART-7).
pub fn partition_key_from_sql(sql: &str) -> Option<String> {
    use sqlparser::ast::{Expr as SqlExpr, GroupByExpr, Query, SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(query) = stmts.first()? else {
        return None;
    };

    // One `Query` node in the whole tree. Anything that introduces a second —
    // a CTE, a derived table in FROM, a scalar/IN/EXISTS subquery anywhere —
    // would be evaluated per shard over that shard's rows only.
    if count_query_nodes(query.as_ref()) != 1 {
        return None;
    }

    // Clauses that operate on the *result set* rather than on a group. Each of
    // these would be applied independently inside every shard.
    let Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query.as_ref();
    if with.is_some()
        || order_by.is_some()
        || limit_clause.is_some()
        || fetch.is_some()
        || !locks.is_empty()
        || for_clause.is_some()
        || settings.is_some()
        || format_clause.is_some()
        || !pipe_operators.is_empty()
    {
        return None;
    }

    let SetExpr::Select(select) = body.as_ref() else {
        return None;
    };

    // Select-level modifiers that can see across groups or across shards.
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.exclude.is_some()
        || select.select_modifiers.is_some()
        || !select.optimizer_hints.is_empty()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return None;
    }

    // Exactly one plain table in FROM, no joins, no table-valued function.
    let (table_idents, _) = single_plain_table(select)?;

    // Exactly one plain-column GROUP BY expression, qualified (if at all) by
    // that table or its alias.
    let GroupByExpr::Expressions(exprs, modifiers) = &select.group_by else {
        return None;
    };
    if exprs.len() != 1 || !modifiers.is_empty() {
        return None;
    }
    let key = match exprs.first()? {
        SqlExpr::Identifier(ident) => ident.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => {
            if parts.len() != 2 {
                return None;
            }
            let qualifier = parts.first()?.value.as_str();
            if !table_idents
                .iter()
                .any(|name| name.eq_ignore_ascii_case(qualifier))
            {
                return None;
            }
            parts.last()?.value.clone()
        }
        _ => return None,
    };

    // A window function anywhere in the projection ranks over its own
    // partition, which need not be the shard key.
    if projection_has_window_function(select) {
        return None;
    }

    // A projection alias equal to the key routes rows by an input column the
    // query never groups on.
    if projection_alias_shadows(select, &key) {
        return None;
    }

    Some(key)
}

/// Number of `Query` nodes in the tree, including the root.
fn count_query_nodes(query: &sqlparser::ast::Query) -> usize {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Query, Visit, Visitor};

    struct Counter(usize);
    impl Visitor for Counter {
        type Break = ();
        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.0 += 1;
            ControlFlow::Continue(())
        }
    }
    let mut counter = Counter(0);
    let _ = query.visit(&mut counter);
    counter.0
}

/// The `FROM` clause reduced to the one plain table it must be: returns the
/// names that may qualify a column of it (the table's last identifier and its
/// alias, if any) plus that table's bare name.
fn single_plain_table(select: &sqlparser::ast::Select) -> Option<(Vec<String>, String)> {
    use sqlparser::ast::TableFactor;

    if select.from.len() != 1 {
        return None;
    }
    let from = select.from.first()?;
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        partitions,
        json_path,
        sample,
        ..
    } = &from.relation
    else {
        return None;
    };
    // A table-valued function, a MSSQL hint, a partition selector, a PartiQL
    // path or a TABLESAMPLE all change what "the rows of this table" means.
    if args.is_some()
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
    {
        return None;
    }
    let bare = name.0.last()?.as_ident()?.value.clone();
    let mut idents = vec![bare.clone()];
    if let Some(alias) = alias {
        idents.push(alias.name.value.clone());
    }
    Some((idents, bare))
}

/// Whether any projected expression is a window function (`… OVER (…)`).
fn projection_has_window_function(select: &sqlparser::ast::Select) -> bool {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Expr as SqlExpr, Visit, Visitor};

    struct WindowFinder(bool);
    impl Visitor for WindowFinder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<()> {
            if let SqlExpr::Function(func) = expr
                && func.over.is_some()
            {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut finder = WindowFinder(false);
    let _ = select.projection.visit(&mut finder);
    finder.0
}

/// Whether a projection aliases some *other* expression to the key's name.
///
/// `SELECT customer AS region, SUM(amount) FROM orders GROUP BY region` groups
/// on the alias — i.e. on `customer` — while the router would shard the input
/// on the column literally named `region`. Two different columns, one name.
fn projection_alias_shadows(select: &sqlparser::ast::Select, key: &str) -> bool {
    use sqlparser::ast::{Expr as SqlExpr, SelectItem};

    let names_the_key = |expr: &SqlExpr| match expr {
        SqlExpr::Identifier(ident) => ident.value.eq_ignore_ascii_case(key),
        SqlExpr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|p| p.value.eq_ignore_ascii_case(key)),
        _ => false,
    };
    select.projection.iter().any(|item| match item {
        SelectItem::ExprWithAlias { expr, alias } => {
            alias.value.eq_ignore_ascii_case(key) && !names_the_key(expr)
        }
        SelectItem::ExprWithAliases { expr, aliases } => {
            aliases.iter().any(|a| a.value.eq_ignore_ascii_case(key)) && !names_the_key(expr)
        }
        _ => false,
    })
}

// ── Plan walker ───────────────────────────────────────────────────────────────

fn try_build_from_logical(
    plan: &LogicalPlan,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
) -> Option<ViewPlan> {
    match plan {
        // Peel top-level projections transparently.
        LogicalPlan::Projection(Projection { input, expr, .. }) => {
            // IVM-AUD-CORE-23: a SELECT's aggregate aliases live in this
            // projection, not in the Aggregate below it — the Aggregate's own
            // schema names them `sum(sales.amount)` / `count(*)`. Peeling the
            // projection therefore threw away the only thing that says which
            // aggregate feeds which declared output column, leaving the
            // planner to pair them positionally.
            if let LogicalPlan::Aggregate(agg) = input.as_ref() {
                let aliases = aggregate_output_aliases(expr);
                return build_agg_plan(agg, output_schema, available_schemas, &aliases);
            }
            try_build_from_logical(input, output_schema, available_schemas, lateness)
        }
        LogicalPlan::Aggregate(agg) => {
            build_agg_plan(agg, output_schema, available_schemas, &AHashMap::new())
        }
        LogicalPlan::Join(join) => {
            // Only 2-source joins (source_of_plan returns None for multi-way joins
            // where one side is itself a Join node with 2 inputs).
            build_join_plan(join, None, available_schemas, lateness)
        }
        // #160: `WHERE` above a join (`SELECT … FROM a JOIN b ON … WHERE …`)
        // plans as `Filter → Join`. Filter is linear, so conjuncts that touch
        // only one side push onto that side's delta filter; anything
        // cross-side (or right-side under LEFT OUTER, where pushdown changes
        // null-padding semantics) bails to DiffBased inside the builder.
        // Non-join inputs keep the previous behavior (single-source WHERE
        // shapes are resolved inside the aggregate/distinct builders; a bare
        // filtered scan stays DiffBased).
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::Join(join) => {
                build_join_plan(join, Some(&f.predicate), available_schemas, lateness)
            }
            _ => None,
        },
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

/// Map each projected column's *internal* name to the name the SELECT gives it.
///
/// `SELECT region, SUM(amount) AS total` projects `sum(sales.amount) AS total`,
/// so this yields `{"sum(sales.amount)" -> "total"}`. Un-aliased columns map to
/// themselves, which is what a `SELECT region, …` needs.
fn aggregate_output_aliases(exprs: &[Expr]) -> AHashMap<String, String> {
    let mut out = AHashMap::new();
    for e in exprs {
        // Alias chains nest: `COUNT(*) AS cnt` projects
        // `Alias(Alias(Column("count(Int64(1))"), "count(*)"), "cnt")`, so the
        // outermost name is the user's and the innermost column is the
        // aggregate's internal name. Peel to the base column, keep the outer
        // name.
        let (mut inner, output_name) = match e {
            Expr::Alias(alias) => (alias.expr.as_ref(), alias.name.clone()),
            Expr::Column(col) => (e, col.name.clone()),
            _ => continue,
        };
        while let Expr::Alias(next) = inner {
            inner = next.expr.as_ref();
        }
        if let Expr::Column(col) = inner {
            out.insert(col.name.clone(), output_name);
        }
    }
    out
}

fn build_agg_plan(
    agg: &Aggregate,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    output_aliases: &AHashMap<String, String>,
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

    // IVM-AUD-CORE-23: pair each aggregate with its declared output column by
    // NAME. This used to zip the two lists positionally — `aggr_expr` in SELECT
    // order against the declared schema's non-group columns in schema order —
    // so a view whose declared schema listed its aggregate columns in a
    // different order than the SELECT list transposed the aggregations
    // (`SUM` computed into the `cnt` column and vice versa) while the arity
    // check above still passed.
    //
    // The plan's own schema is [group fields…, aggregate fields…], so the
    // aggregate at index i is named by field `group_expr.len() + i`.
    let plan_agg_names: Vec<String> = (0..agg.aggr_expr.len())
        .map(|i| {
            let internal = agg.schema.field(agg.group_expr.len() + i).name();
            output_aliases
                .get(internal)
                .cloned()
                .unwrap_or_else(|| internal.to_string())
        })
        .collect();

    let pair_by_name = plan_agg_names
        .iter()
        .all(|n| agg_output_cols.iter().any(|c| c.eq_ignore_ascii_case(n)));

    let mut aggregations: Vec<Aggregation> = Vec::new();
    if pair_by_name {
        for (expr, plan_name) in agg.aggr_expr.iter().zip(plan_agg_names.iter()) {
            let out_col = agg_output_cols
                .iter()
                .find(|c| c.eq_ignore_ascii_case(plan_name))?;
            aggregations.push(expr_to_aggregation(expr, out_col)?);
        }
    } else if agg.aggr_expr.len() == 1 {
        // One aggregate renamed by the view's declared schema: the mapping is
        // unambiguous even though the names differ.
        let out_col = agg_output_cols.first()?;
        let expr = agg.aggr_expr.first()?;
        aggregations.push(expr_to_aggregation(expr, out_col)?);
    } else {
        // Several aggregates whose names do not match the declared schema:
        // there is no way to know which column each one feeds. Degrade to
        // DiffBased (full recompute, right answer) rather than guess.
        return None;
    }

    // AUD-3: honor the view's declared output column types (SUM(Int64)→Int64
    // unless the view declares otherwise) so the incremental snapshot matches
    // the registered contract.
    let op = IncrementalAggOp::new_with_output_schema(
        input_schema,
        group_by,
        aggregations,
        output_schema,
    )
    .ok()?;
    Some(ViewPlan::Aggregate { source, op, filter })
}

// ── Join plan builder ─────────────────────────────────────────────────────────

fn build_join_plan(
    join: &Join,
    outer_filter: Option<&Expr>,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
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
    let (left_source, left_side_preds) = resolve_source_with_filters(&join.left)
        .or_else(|| source_of_plan(&join.left).map(|s| (s, Vec::new())))?;
    let (right_source, right_side_preds) = resolve_source_with_filters(&join.right)
        .or_else(|| source_of_plan(&join.right).map(|s| (s, Vec::new())))?;
    let left_schema = available_schemas.get(&left_source)?;
    let right_schema = available_schemas.get(&right_source)?;

    let mut left_key_cols: Vec<String> = Vec::new();
    let mut right_key_cols: Vec<String> = Vec::new();

    for (left_expr, right_expr) in &join.on {
        left_key_cols.push(expr_col_name(left_expr)?);
        right_key_cols.push(expr_col_name(right_expr)?);
    }

    // #160: the SQL planner leaves the ON condition in `join.filter` (the
    // optimizer pass that lifts equi-pairs into `join.on` never runs on the
    // unoptimized plan inspected here) — so before this, every SQL-registered
    // join silently degraded to DiffBased. Accept a conjunction of plain
    // column equalities, classifying each side by the join input schemas; any
    // other shape (non-equi residual, expressions over keys) bails.
    if let Some(filter) = &join.filter {
        for conjunct in datafusion::logical_expr::utils::split_conjunction(filter) {
            let Expr::BinaryExpr(be) = strip_alias(conjunct) else {
                return None;
            };
            if be.op != datafusion::logical_expr::Operator::Eq {
                return None;
            }
            let (Expr::Column(a), Expr::Column(b)) =
                (strip_alias(&be.left), strip_alias(&be.right))
            else {
                return None;
            };
            let a_left = join.left.schema().index_of_column(a).is_ok();
            let b_left = join.left.schema().index_of_column(b).is_ok();
            match (a_left, b_left) {
                (true, false) => {
                    left_key_cols.push(a.name.clone());
                    right_key_cols.push(b.name.clone());
                }
                (false, true) => {
                    left_key_cols.push(b.name.clone());
                    right_key_cols.push(a.name.clone());
                }
                // Same-side equality or unresolvable column: not an equi-join
                // pair this operator can key on.
                _ => return None,
            }
        }
    }

    if left_key_cols.is_empty() {
        return None;
    }

    // #160: decompose a `WHERE` above the join by side. Filter is linear, so
    // a conjunct over one side's columns filters that side's delta before the
    // probe. Cross-side conjuncts cannot be pushed; under LEFT OUTER a
    // right-side conjunct would change null-padding semantics (it makes the
    // join effectively inner) — both bail to DiffBased.
    let mut left_preds = left_side_preds;
    let mut right_preds = right_side_preds;
    if let Some(filter) = outer_filter {
        for conjunct in datafusion::logical_expr::utils::split_conjunction(filter) {
            let cols = conjunct.column_refs();
            if cols.is_empty() {
                return None;
            }
            let all_left = cols
                .iter()
                .all(|c| join.left.schema().index_of_column(c).is_ok());
            let all_right = cols
                .iter()
                .all(|c| join.right.schema().index_of_column(c).is_ok());
            if all_left {
                left_preds.push((*conjunct).clone());
            } else if all_right && incr_join_type == IncrJoinType::Inner {
                right_preds.push((*conjunct).clone());
            } else {
                return None;
            }
        }
    }
    // Outer-filter columns are qualified by the join-side relation (a table
    // alias, e.g. `t.dist`), which the source-schema compile below cannot
    // resolve — strip qualifiers so they bind by bare name.
    let left_filter =
        compile_source_filter(&unqualify_columns(&left_preds)?, &left_source, left_schema).ok()?;
    let right_filter = compile_source_filter(
        &unqualify_columns(&right_preds)?,
        &right_source,
        right_schema,
    )
    .ok()?;

    // IVM-AUD-CORE-5: build the traces WITH the lateness column when the view
    // declares one that both sides carry. Without it `Trace::gc_below_watermark`
    // early-returns on `lateness_col_idx == None`, so the entire per-tick
    // watermark-GC loop was a no-op and join traces grew unbounded — the exact
    // failure `join.rs` documents ("without calling `with_lateness_column`,
    // `gc_below_watermark` is a universal no-op"). Both sides must carry the
    // column: a trace GC'd on one side only would drop rows that the other
    // side can still legitimately match.
    let lateness_col = lateness
        .iter()
        .map(|l| l.column.as_str())
        .find(|col| left_schema.index_of(col).is_ok() && right_schema.index_of(col).is_ok());
    if lateness_col.is_none() && !lateness.is_empty() {
        tracing::debug!(
            "LATENESS declared but no column is present on both join sides; \
             join traces will not be watermark-GC'd"
        );
    }
    let op = IncrementalJoinOp::new_with_lateness(
        left_schema.clone(),
        right_schema.clone(),
        left_key_cols,
        right_key_cols,
        incr_join_type,
        lateness_col,
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

/// Peel `Alias` wrappers off an expression (planners wrap freely).
fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(&alias.expr),
        other => other,
    }
}

/// Rewrite every column reference to its bare (unqualified) name so a
/// predicate lifted from above the join compiles against the source's data
/// schema regardless of the SQL-side table alias.
fn unqualify_columns(preds: &[Expr]) -> Option<Vec<Expr>> {
    use datafusion::common::Column;
    use datafusion::common::tree_node::{Transformed, TreeNode as _};
    preds
        .iter()
        .map(|p| {
            p.clone()
                .transform(|e| {
                    Ok(match e {
                        Expr::Column(c) => {
                            Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))
                        }
                        other => Transformed::no(other),
                    })
                })
                .map(|t| t.data)
                .ok()
        })
        .collect()
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
            // IVM-AUD-CORE-22: `AggregateFunctionParams` carries `distinct`,
            // `filter`, `order_by` and `null_treatment`, and NONE of them was
            // read — so `COUNT(DISTINCT user_id)` lowered to a plain
            // incremental COUNT and `SUM(x) FILTER (WHERE y > 0)` to an
            // unfiltered SUM, both silently wrong. This is exactly the
            // MIN_BY/MAX_BY class the code below already guards against; the
            // guard was written for that one case and never generalized.
            //
            // Refusing to build an incremental plan degrades the view to
            // DiffBased (full recompute + diff), which is slower and CORRECT.
            // A wrong answer computed quickly is not a trade worth making.
            if agg_fn.params.distinct {
                tracing::warn!(
                    output_col,
                    "IVM plan degraded to O(state) DiffBased: DISTINCT inside an \
                     aggregate has no incremental operator (a Z-set retraction \
                     cannot tell whether the last copy of a value was removed \
                     without holding per-value multiplicity)"
                );
                return None;
            }
            if agg_fn.params.filter.is_some() {
                tracing::warn!(
                    output_col,
                    "IVM plan degraded to O(state) DiffBased: FILTER (WHERE …) on \
                     an aggregate is not applied by the incremental operators"
                );
                return None;
            }
            if !agg_fn.params.order_by.is_empty() {
                tracing::warn!(
                    output_col,
                    "IVM plan degraded to O(state) DiffBased: ORDER BY inside an \
                     aggregate is order-sensitive and the incremental operators \
                     are not"
                );
                return None;
            }
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
                // NOT min_by/max_by: those return the value of arg0 at the
                // extremum of arg1, which plain Min/Max over arg0 silently
                // mis-computes — they must degrade to DiffBased.
                "min" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Min {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                "max" => {
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    fn join_ctx_and_schemas() -> (SessionContext, AHashMap<String, SchemaRef>, SchemaRef) {
        let orders_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]));
        let customers_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let orders = RecordBatch::try_new(
            orders_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let customers = RecordBatch::try_new(
            customers_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "orders",
            Arc::new(MemTable::try_new(orders_schema.clone(), vec![vec![orders]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "customers",
            Arc::new(MemTable::try_new(customers_schema.clone(), vec![vec![customers]]).unwrap()),
        )
        .unwrap();
        let mut schemas = AHashMap::new();
        schemas.insert("orders".to_string(), orders_schema);
        schemas.insert("customers".to_string(), customers_schema);
        let out_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        (ctx, schemas, out_schema)
    }

    async fn plan_for(sql: &str) -> ViewPlan {
        let (_ctx, schemas, out_schema) = join_ctx_and_schemas();
        build_view_plan(sql, &out_schema, &schemas, &[]).await
    }

    /// #160 regression pin: the SQL planner leaves the ON condition in
    /// `join.filter`, so this shape must still lower to the incremental
    /// operator. Before the fix every SQL join view silently ran DiffBased.
    #[tokio::test]
    async fn sql_inner_join_lowers_to_incremental() {
        let plan = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id",
        )
        .await;
        assert_eq!(plan.kind(), ViewPlanKind::Incremental);
        let ViewPlan::Join {
            left_source,
            right_source,
            left_filter,
            right_filter,
            ..
        } = plan
        else {
            panic!("expected a join plan");
        };
        assert_eq!(
            (left_source.as_str(), right_source.as_str()),
            ("orders", "customers")
        );
        assert!(left_filter.is_none() && right_filter.is_none());
    }

    /// A WHERE above the join whose conjuncts each touch one side pushes onto
    /// that side's delta filter (O(Δ) preserved).
    #[tokio::test]
    async fn where_above_join_pushes_per_side_filters() {
        let plan = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             WHERE orders.order_id > 10 AND customers.name = 'Alice'",
        )
        .await;
        let ViewPlan::Join {
            left_filter,
            right_filter,
            ..
        } = plan
        else {
            panic!("expected a join plan, got DiffBased");
        };
        assert!(left_filter.is_some(), "left-side WHERE conjunct pushed");
        assert!(right_filter.is_some(), "right-side WHERE conjunct pushed");
    }

    /// Right-side WHERE above a LEFT OUTER join changes null-padding
    /// semantics — must degrade, never push.
    #[tokio::test]
    async fn left_outer_with_right_side_where_degrades() {
        let plan = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders LEFT JOIN customers ON orders.customer_id = customers.customer_id \
             WHERE customers.name = 'Alice'",
        )
        .await;
        assert_eq!(plan.kind(), ViewPlanKind::DiffBased);
    }

    /// Non-equi and cross-side predicates cannot be keyed — degrade.
    #[tokio::test]
    async fn non_equi_and_cross_side_predicates_degrade() {
        let non_equi = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id < customers.customer_id",
        )
        .await;
        assert_eq!(non_equi.kind(), ViewPlanKind::DiffBased);
        let cross_side = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             WHERE orders.order_id > customers.customer_id",
        )
        .await;
        assert_eq!(cross_side.kind(), ViewPlanKind::DiffBased);
    }

    /// Regression (crate-12 audit, A-class): MIN_BY/MAX_BY return the value of
    /// arg0 at the extremum of arg1 — the previous mapping to plain Min/Max of
    /// arg0 silently computed the wrong answer on the O(Δ) path. They must
    /// degrade to DiffBased.
    #[tokio::test]
    async fn min_by_max_by_degrade_to_diff_based() {
        let (_ctx, schemas, _) = join_ctx_and_schemas();
        let out_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, true),
            Field::new("first_order", DataType::Int32, true),
        ]));
        let plan = build_view_plan(
            "SELECT customer_id, MIN_BY(order_id, order_id) AS first_order \
             FROM orders GROUP BY customer_id",
            &out_schema,
            &schemas,
            &[],
        )
        .await;
        assert_eq!(
            plan.kind(),
            ViewPlanKind::DiffBased,
            "MIN_BY must not lower to the incremental Min operator"
        );
        let plan = build_view_plan(
            "SELECT customer_id, MAX_BY(order_id, order_id) AS last_order \
             FROM orders GROUP BY customer_id",
            &out_schema,
            &schemas,
            &[],
        )
        .await;
        assert_eq!(plan.kind(), ViewPlanKind::DiffBased);
    }
}
