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

use ahash::AHashMap;
use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Aggregate, Expr, Join, JoinType, LogicalPlan, Projection};
use datafusion::prelude::SessionContext;

use krishiv_delta::{
    Aggregation, IncrJoinType, IncrementalAggOp, IncrementalDistinctOp, IncrementalJoinOp,
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
    },
    /// Bilinear inner join: `ΔA ⋈ B_trace + A_trace ⋈ ΔB`.
    Join {
        left_source: String,
        right_source: String,
        op: IncrementalJoinOp,
    },
    /// Threshold-tracking DISTINCT: emits ±1 only at crossing the 0-threshold.
    Distinct {
        source: String,
        op: IncrementalDistinctOp,
    },
    /// Fallback: full SQL re-execution + diff against previous output (O(state)).
    DiffBased,
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

    /// GC state older than `watermark_ms` (only meaningful for Join traces).
    pub fn gc_watermark(&mut self, watermark_ms: i64) -> krishiv_delta::DeltaResult<usize> {
        match self {
            ViewPlan::Join { op, .. } => op.gc_traces(watermark_ms),
            _ => Ok(0),
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
    let Statement::Query(query) = &stmts[0] else {
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
    match &exprs[0] {
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
            expr_col_name(&agg.group_expr[0])
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
        LogicalPlan::Join(join) => build_join_plan(join, available_schemas),
        // DISTINCT — the inner plan is the first (and only) input.
        LogicalPlan::Distinct(_) => {
            let inputs = plan.inputs();
            let inner_plan = inputs.first().copied()?;
            let source = source_of_plan(inner_plan)?;
            Some(ViewPlan::Distinct {
                source,
                op: IncrementalDistinctOp::new(),
            })
        }
        _ => None,
    }
}

// ── Aggregate plan builder ────────────────────────────────────────────────────

fn build_agg_plan(
    agg: &Aggregate,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    let source = source_of_plan(&agg.input)?;
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

    let op = IncrementalAggOp::new(input_schema, group_by, aggregations).ok()?;
    Some(ViewPlan::Aggregate { source, op })
}

// ── Join plan builder ─────────────────────────────────────────────────────────

fn build_join_plan(
    join: &Join,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    if join.join_type != JoinType::Inner {
        return None;
    }

    let left_source = source_of_plan(&join.left)?;
    let right_source = source_of_plan(&join.right)?;
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
        IncrJoinType::Inner,
    )
    .ok()?;

    Some(ViewPlan::Join {
        left_source,
        right_source,
        op,
    })
}

// ── Source resolution ─────────────────────────────────────────────────────────

/// Walk a plan tree to find the single base table scan, returning its name.
/// Returns `None` for multi-input plans (joins, unions) or unsupported nodes.
fn source_of_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(ts) => Some(ts.table_name.table().to_string()),
        LogicalPlan::SubqueryAlias(sa) => source_of_plan(&sa.input),
        _ => {
            let inputs = plan.inputs();
            if inputs.len() == 1 {
                source_of_plan(inputs[0])
            } else {
                None
            }
        }
    }
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
                "count" => Some(Aggregation::Count {
                    output_col: output_col.to_string(),
                }),
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
