//! Greedy reordering of inner-join chains by estimated join output.
//!
//! # The gap this closes
//!
//! DataFusion 54 has **no join reordering rule at all**. Its logical rule list
//! contains `EliminateCrossJoin`, which turns a cross join plus a predicate
//! into an inner join *in place*, and nothing else that touches join order. So
//! the shape of the join tree is exactly the order the relations appear in the
//! `FROM` clause, whatever their sizes.
//!
//! For a star-schema query written fact-first that is usually fine. For one
//! that names a second large relation early it is not. TPC-DS q72 begins
//!
//! ```text
//!   FROM catalog_sales JOIN inventory ON (cs_item_sk = inv_item_sk)
//!        JOIN warehouse … JOIN item … JOIN customer_demographics …
//!        JOIN household_demographics … JOIN date_dim d1 …
//! ```
//!
//! and `cs_item_sk = inv_item_sk` is a join between two *facts* on a column
//! that is a key of neither — 368.6 K surviving `catalog_sales` rows against
//! 11.74 M `inventory` rows produce a **15.29 M row** intermediate, which the
//! five joins above it then whittle down to 380.9 K. Measured on TPC-DS SF1,
//! embedded, that one join is 6.15 s of the query's 15.2 s of join CPU, and
//! every operator above it carries the 15.29 M rows.
//!
//! Reordering the `FROM` clause by hand, changing nothing else:
//!
//! ```text
//!   q72 as written            2655 ms
//!   q72 hand-reordered         280 ms   byte-identical result
//!   q72 size-greedy order      294 ms   byte-identical result
//!   DuckDB                     307 ms
//! ```
//!
//! The third line is what the rule first implemented: picking the smallest
//! connected relation next, by base-table row count alone.
//!
//! # Why size alone was not enough
//!
//! A relation's size says nothing about what joining it does. TPC-H q5 is
//! written `FROM customer, orders, lineitem, supplier, nation, region`, and
//! `supplier` (1 M rows at SF100) connects to `customer` (15 M) through
//! `c_nationkey = s_nationkey` — a key with 25 values. The size greedy took
//! `supplier` before `orders` because it is smaller, and that join produces
//! 15 M × 1 M / 25 = 600 billion rows. q5 went from 21 s to more than 22
//! minutes and 38 GB before it was stopped.
//!
//! So the greedy now ranks candidates by the **estimated output** of joining
//! them, `|placed| × |candidate| / max(ndv(placed key), ndv(candidate key))`,
//! the textbook estimate under key containment. Distinct-value counts come
//! from [`TableKeyNdv`], an upper bound read from the same Parquet footer
//! statistics as the row counts: an integer column whose values span
//! `[min, max]` has at most `max - min + 1` of them, and never more than the
//! table has rows. Where every candidate joins on a key (q72's dimensions),
//! every estimate equals the placed size and the size tie-break reproduces the
//! old order; where one multiplies (q5's `nationkey`), it is placed last.
//!
//! # Why row counts are available here when they were not before
//!
//! [`semi_join_reduction::SEMI_JOIN_DIMENSION_ENV`] documents at length that a
//! logical rule cannot size a relation, because `TableSource` exposes no
//! `statistics()`. That is still true of `TableSource`. It is not true of this
//! engine: `SqlEngine` keeps a `table_row_counts` registry, populated at
//! registration from the Parquet footers, and this rule is constructed over it
//! exactly as `ann_rewrite::AnnTopKPrefilter` is constructed over the vector
//! index cache. A rule built over an empty registry — the staged planner — is
//! inert, because [`Self::rewrite`] declines unless *every* relation in the
//! chain has a known size and every join key a known distinct-value bound.
//!
//! # Why it is safe
//!
//! - **Inner joins only, and only `JoinConstraint::On`.** Inner join is
//!   associative and commutative, so any order over the same relations with the
//!   same predicates produces the same multiset of rows. `USING` merges columns
//!   and is declined rather than reasoned about.
//! - **Every predicate is replaced, none dropped.** Each equijoin pair and each
//!   non-equi filter is placed at the first point in the new order where all the
//!   relations it names are present. If any predicate cannot be placed, the
//!   rewrite is abandoned and the plan is returned untouched.
//! - **The output schema is preserved exactly.** Reordering permutes the column
//!   order of the join's schema, so the rebuilt chain is wrapped in a projection
//!   that restores the original schema's columns in the original order. Parents
//!   resolve by qualified name and cannot tell the difference.
//! - **No cross joins are introduced.** Each step picks a relation that shares
//!   an equijoin edge with what is already placed; if none does, the rewrite is
//!   abandoned.
//! - **Only a clearly better order replaces the written one.** The written and
//!   the greedy order are costed with the same estimate — the sum of their
//!   intermediate results — and the plan is rewritten only when the greedy
//!   order costs at most half as much. An estimate is not a measurement, and a
//!   marginal predicted gain is not worth overriding the author's order for.
//! - **Idempotent.** If the greedy order is the order already in the plan, the
//!   rule reports no transform, so the optimizer reaches a fixed point.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchemaRef, Result, ScalarValue, Statistics};
use datafusion::logical_expr::{
    Expr, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder, TableScan,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Row counts for registered base tables, keyed by table name.
pub type TableRowCounts = Arc<std::sync::RwLock<HashMap<String, u64>>>;

/// Upper bounds on the distinct values of registered tables' columns, keyed by
/// table name.
pub type TableKeyNdv = Arc<std::sync::RwLock<HashMap<String, KeyNdv>>>;

/// One table's distinct-value bounds, and the row count they were taken at.
///
/// The row count is a staleness check. `INSERT` and `TRUNCATE` update the
/// row-count registry in place without re-reading statistics, so a bound taken
/// before them may no longer hold; the rule uses a table's bounds only while
/// its registered row count is still the one recorded here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyNdv {
    pub rows: u64,
    pub columns: HashMap<String, u64>,
}

/// Record `table`'s column bounds, replacing any earlier ones.
pub fn record_key_ndv(
    registry: &TableKeyNdv,
    table: &str,
    rows: u64,
    columns: HashMap<String, u64>,
) {
    if let Ok(mut registry) = registry.write() {
        registry.insert(table.to_owned(), KeyNdv { rows, columns });
    }
}

/// An integer-valued scalar, for the `[min, max]` range bound.
fn integral(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) | ScalarValue::Date32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) | ScalarValue::Date64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        _ => None,
    }
}

/// The distinct-value bound `[min, max]` gives an integer column, capped by
/// the row count.
fn range_bound(min: i128, max: i128, rows: u64) -> Option<u64> {
    let span = max.checked_sub(min)?.checked_add(1)?;
    let span = u64::try_from(span.max(1)).unwrap_or(u64::MAX);
    Some(span.min(rows.max(1)))
}

/// Upper bounds on each column's distinct values, from scan statistics.
///
/// A recorded distinct count is used as is. Otherwise an integer or date
/// column with a known minimum and maximum has at most `max - min + 1`
/// distinct values. Every bound is capped by `rows`. A column with neither —
/// a string key, a column with no footer statistics — is left out, which makes
/// the join-reorder rule decline any chain joining on it.
#[must_use]
pub fn column_ndv_bounds(schema: &Schema, stats: &Statistics, rows: u64) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    for (field, column) in schema.fields().iter().zip(&stats.column_statistics) {
        let bound = match column.distinct_count {
            Precision::Exact(n) | Precision::Inexact(n) => Some((n as u64).clamp(1, rows.max(1))),
            Precision::Absent => match (column.min_value.get_value(), column.max_value.get_value())
            {
                (Some(min), Some(max)) => integral(min)
                    .zip(integral(max))
                    .and_then(|(min, max)| range_bound(min, max, rows)),
                _ => None,
            },
        };
        if let Some(bound) = bound {
            out.insert(field.name().clone(), bound);
        }
    }
    out
}

/// [`column_ndv_bounds`] for an in-memory table, read from the data itself.
#[must_use]
pub fn ndv_bounds_from_batches(batches: &[RecordBatch], rows: u64) -> HashMap<String, u64> {
    use datafusion::arrow::array::Int64Array;
    let mut out = HashMap::new();
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return out;
    };
    for (index, field) in schema.fields().iter().enumerate() {
        if !(field.data_type().is_integer() || matches!(field.data_type(), DataType::Date32)) {
            continue;
        }
        let mut range: Option<(i64, i64)> = None;
        for batch in batches {
            let Some(column) = batch.columns().get(index) else {
                continue;
            };
            let Ok(cast) = datafusion::arrow::compute::cast(column, &DataType::Int64) else {
                range = None;
                break;
            };
            let Some(values) = cast.as_any().downcast_ref::<Int64Array>() else {
                range = None;
                break;
            };
            let (Some(min), Some(max)) = (
                datafusion::arrow::compute::min(values),
                datafusion::arrow::compute::max(values),
            ) else {
                continue;
            };
            range = Some(match range {
                Some((lo, hi)) => (lo.min(min), hi.max(max)),
                None => (min, max),
            });
        }
        if let Some(bound) =
            range.and_then(|(min, max)| range_bound(i128::from(min), i128::from(max), rows))
        {
            out.insert(field.name().clone(), bound);
        }
    }
    out
}

/// Environment switch for greedy join reordering.
///
/// # On by default, and the sweep that made it so
///
/// A reordering rule changes the plan of every multi-way inner join, so the
/// only measurement that counts is one across a whole benchmark — not the query
/// it was written for. `KRISHIV_SEMI_JOIN_DIMENSION` shipped on after a
/// three-query A/B and cost q10 18.1x; the regression set was chosen from the
/// previous incident, which is to say from the queries already known about.
///
/// So this rule was measured on all 99 TPC-DS queries at SF1, embedded, warm,
/// best of three, paired and interleaved, with results hashed per query. The
/// first version — the greedy alone, no inversion guard — read:
///
/// ```text
///   suite      18972 ms -> 17385 ms   (+8.4%)   99/99 rows identical
///   wins >10%  14        losses >10%  15
///   q72         2717 ms ->   265 ms   10.2x
///   q24          205 ms ->   805 ms    4.0x SLOWER
/// ```
///
/// A net win that is *entirely* q72: on the other 98 queries it lost 865 ms.
/// The regressions are all the same shape — `store_sales ⋈ store_returns`, a
/// near-1:1 fact-to-fact join that reduces — and base-table size cannot tell it
/// from q72's fact-to-fact join that multiplies. Hence the guard in
/// [`JoinReorder::rewrite`] that only reorders a chain whose written order is
/// inverted. With it:
///
/// ```text
///   suite      18935 ms -> 16467 ms   (+15.0%)  99/99 rows identical
///   wins >10%   7        losses >10%   4        neutral 88
///   q72         2680 ms ->   263 ms   10.2x
///   worst loss    80 ms ->   104 ms   (q6, 24 ms)
///   excluding q72            16255 ms -> 16204 ms  — neutral
/// ```
///
/// # SF100, where size-only ranking fell over
///
/// That sweep was SF1, embedded, and it is what shipped the rule on. The
/// 2026-10-01 TPC-H SF100 run (12 cores, 61 GB) measured the same size-only
/// greedy against `KRISHIV_JOIN_REORDER=off`, one process per query:
///
/// ```text
///   q5     21 s / 1.9 GB  ->  >22 min / 38 GB  (killed)
///   q10   14.9 s          ->  31.1 s           2.1x slower
///   q8    18.8 s          ->  36.2 s           1.9x slower
///   q9    34.0 s          ->  46.6 s           1.4x slower, 11 GB
///   q7    31.0 s          ->  28.9 s           neutral
///   q21   43.3 s          ->  40.5 s           neutral
/// ```
///
/// Every loss is the module-doc story: a small relation placed first because
/// it is small, joined on a key whose distinct count is tiny (`nationkey`,
/// 25 values), multiplying the probe side — and the inversion guard cannot
/// see it, because TPC-H chains are written fact-first anyway. So the greedy
/// now ranks by estimated join output, declines any chain with a key it has
/// no distinct-value bound for, and keeps the written order unless the
/// greedy one is estimated at least 2x cheaper (`REQUIRED_ESTIMATED_GAIN`);
/// the inversion guard stays. With all three the rule changes **no** TPC-H plan at SF100
/// and, of the 14 TPC-DS SF1 plans the old rule rewrote (6, 18, 19, 26, 37,
/// 53, 63, 72, 77, 82, 84, 85, 89, 91), only q72's — the one that was ever a
/// measured win.
///
/// Measured against the size-only rule, both on, release build, 2026-10-01:
///
/// ```text
///   TPC-H SF100 (one process per query, 300 s cap)
///   q5     killed at 22 min / 38 GB  ->  21.0 s    q10   31.1 s -> 15.0 s
///   q8     36.2 s -> 20.8 s                       q9    46.6 s -> 34.9 s
///   q7     28.9 s -> 32.7 s   q21  40.5 s -> 45.3 s   (both within the off
///                                                      run's spread)
///   TPC-DS SF1, 99 queries, paired and interleaved, best of 3
///   suite      13114 ms -> 13174 ms   (-0.5%)   99/99 results identical
///   median per-query ratio 1.00; q72 233 ms -> 218 ms
///   q6  151 ms ->  79 ms, q26 100 ms -> 71 ms   (no longer reordered)
///   12 "losses" and 9 "wins" over 10%, all on 50-500 ms queries whose
///   plans are identical under both rules — noise, not the rule.
/// ```
///
/// # What is NOT measured
///
/// The distributed path. A bad join order there becomes a bad shuffle, and
/// no sweep has been run against a cluster. `KRISHIV_JOIN_REORDER=off`
/// disables the rule.
pub const JOIN_REORDER_ENV: &str = "KRISHIV_JOIN_REORDER";

/// Longest chain this rule will reorder.
///
/// The greedy is O(n²) in the number of relations and the rebuild walks every
/// predicate per step, so a bound keeps planning time bounded on the pathological
/// hand-written joins that appear in generated SQL. Ten covers every TPC-H and
/// TPC-DS query; q72, the longest, is nine.
const MAX_RELATIONS: usize = 10;

/// Whether greedy join reordering is enabled (default: **yes**).
///
/// Opt-*out* parsing, matching `semi_join_reduction::enabled_from`: anything but
/// an explicit no leaves the rule on.
pub fn join_reorder_enabled() -> bool {
    !matches!(
        std::env::var(JOIN_REORDER_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false" | "no"
    )
}

/// Greedy left-deep reordering of inner-join chains.
#[derive(Debug)]
pub struct JoinReorder {
    row_counts: TableRowCounts,
    key_ndv: TableKeyNdv,
    /// Bypass the env gate and always apply — see
    /// [`crate::semi_join_reduction::SemiJoinPushdownThroughInnerJoin`] for why
    /// the rules in this crate carry one.
    forced: bool,
}

impl JoinReorder {
    /// The rule over `row_counts` and `key_ndv`, gated on [`JOIN_REORDER_ENV`].
    #[must_use]
    pub fn new(row_counts: TableRowCounts, key_ndv: TableKeyNdv) -> Self {
        Self {
            row_counts,
            key_ndv,
            forced: false,
        }
    }

    /// The rule with its env gate bypassed, for tests and explicit opt-in.
    #[must_use]
    pub fn forced(row_counts: TableRowCounts, key_ndv: TableKeyNdv) -> Self {
        Self {
            row_counts,
            key_ndv,
            forced: true,
        }
    }

    /// The distinct-value bound for `column` of `relation`.
    ///
    /// Only for a relation that is a base table seen through renaming and
    /// row-removing nodes, where a column of the relation is the table's column
    /// of the same name. Anything that computes columns — an aggregate, a
    /// projection with expressions, a join the chain did not flatten — has no
    /// bound here, and the caller declines.
    fn ndv_of(&self, relation: &LogicalPlan, column: &Column) -> Option<u64> {
        let scan = base_scan(relation)?;
        let full = scan.table_name.to_string();
        let bare = scan.table_name.table();
        // Two separate reads, never nested: registration takes these locks in
        // the order counts-then-ndv.
        let rows = {
            let counts = self.row_counts.read().ok()?;
            counts.get(&full).or_else(|| counts.get(bare)).copied()?
        };
        let registry = self.key_ndv.read().ok()?;
        let entry = registry.get(&full).or_else(|| registry.get(bare))?;
        if entry.rows != rows {
            return None;
        }
        entry.columns.get(&column.name).copied()
    }

    /// Rows in `plan`, when it bottoms out in exactly one known base table.
    ///
    /// A subtree with no scan (a values list) or more than one (a join this rule
    /// declined to flatten, a union) has no single base size, and returning
    /// `None` makes the caller decline the whole chain. Guessing a size for one
    /// relation is how a reordering rule moves a 150 M row table to the bottom.
    fn size_of(&self, plan: &LogicalPlan) -> Option<u64> {
        let mut found: Option<u64> = None;
        let mut stack = vec![plan];
        while let Some(node) = stack.pop() {
            if let LogicalPlan::TableScan(scan) = node {
                let counts = self.row_counts.read().ok()?;
                let rows = counts
                    .get(&scan.table_name.to_string())
                    .or_else(|| counts.get(scan.table_name.table()))
                    .copied()?;
                if found.is_some() {
                    return None;
                }
                found = Some(rows);
            }
            stack.extend(node.inputs());
        }
        found
    }
}

/// The table scan under `plan`, through nodes that keep a column's meaning:
/// aliases, filters and column-pruning projections.
fn base_scan(mut plan: &LogicalPlan) -> Option<&TableScan> {
    loop {
        plan = match plan {
            LogicalPlan::TableScan(scan) => return Some(scan),
            LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
            LogicalPlan::Filter(filter) => filter.input.as_ref(),
            LogicalPlan::Projection(projection) if is_column_pruning(projection) => {
                projection.input.as_ref()
            }
            _ => return None,
        };
    }
}

/// An equijoin edge between two relations of the chain, with the
/// distinct-value bound of each side's key column.
#[derive(Debug, Clone, Copy)]
struct Edge {
    a: usize,
    b: usize,
    ndv_a: f64,
    ndv_b: f64,
}

/// One level's worth of a flattened inner-join chain.
struct Chain {
    /// Leaves, deepest-left first: the order the `FROM` clause put them in.
    relations: Vec<LogicalPlan>,
    /// Every equijoin pair from every level of the chain.
    on: Vec<(Expr, Expr)>,
    /// Every non-equi join filter from every level of the chain.
    filters: Vec<Expr>,
    /// Preserved from the chain; the rewrite declines if levels disagree.
    null_equality: datafusion::common::NullEquality,
}

/// Is this a projection that only *prunes* columns?
///
/// `OptimizeProjections` runs before this rule in every pass and inserts one of
/// these between the levels of a join chain, which is why flattening has to see
/// through them — a chain of three relations otherwise looks like two and is
/// declined. Only bare `Expr::Column` lists qualify: an alias renames a column
/// and a computed expression adds one, and dropping either would change what
/// the columns above resolve to. Pruning alone is safe to drop because the
/// rebuilt chain is re-projected to the original schema and
/// `OptimizeProjections` re-inserts the pruning on the next pass.
fn is_column_pruning(projection: &datafusion::logical_expr::Projection) -> bool {
    projection
        .expr
        .iter()
        .all(|expr| matches!(expr, Expr::Column(_)))
}

/// Descend past pruning projections to the node beneath them.
fn skip_pruning(mut plan: &LogicalPlan) -> &LogicalPlan {
    while let LogicalPlan::Projection(projection) = plan {
        if !is_column_pruning(projection) {
            break;
        }
        plan = projection.input.as_ref();
    }
    plan
}

/// Flatten a left-deep run of inner joins into its leaves and predicates.
///
/// Only the *left* spine is followed. A join nested on the right is a leaf: it
/// was written as a parenthesised join and reordering across it would change
/// which relations the user grouped, for no evidence that it helps.
fn flatten(plan: &LogicalPlan) -> Option<Chain> {
    let LogicalPlan::Join(top) = plan else {
        return None;
    };
    if top.join_type != JoinType::Inner || top.join_constraint != JoinConstraint::On {
        return None;
    }
    let mut relations = Vec::new();
    let mut on = Vec::new();
    let mut filters = Vec::new();
    let null_equality = top.null_equality;
    let mut node = plan;
    while let LogicalPlan::Join(join) = node {
        if join.join_type != JoinType::Inner
            || join.join_constraint != JoinConstraint::On
            || join.null_equality != null_equality
            || join.on.is_empty()
        {
            break;
        }
        on.extend(join.on.iter().cloned());
        if let Some(filter) = &join.filter {
            filters.push(filter.clone());
        }
        relations.push(skip_pruning(join.right.as_ref()).clone());
        node = skip_pruning(join.left.as_ref());
    }
    if relations.is_empty() {
        return None;
    }
    relations.push(node.clone());
    relations.reverse();
    Some(Chain {
        relations,
        on,
        filters,
        null_equality,
    })
}

/// Which relation each column belongs to, by index into `relations`.
fn column_owners(relations: &[LogicalPlan]) -> HashMap<Column, usize> {
    let mut owners = HashMap::new();
    for (index, relation) in relations.iter().enumerate() {
        for column in relation.schema().columns() {
            owners.insert(column, index);
        }
    }
    owners
}

/// The relations an expression names, or `None` if it names an unknown column.
fn referenced(expr: &Expr, owners: &HashMap<Column, usize>) -> Option<HashSet<usize>> {
    let mut out = HashSet::new();
    for column in expr.column_refs() {
        out.insert(*owners.get(column)?);
    }
    Some(out)
}

/// Estimated rows from joining `candidate` to the relations in `placed`, whose
/// join so far is estimated at `rows`. `None` if no edge connects them.
///
/// `rows × |candidate| / max(ndv(placed side), ndv(candidate side))`. With
/// several edges between the two — a composite key — each side's distinct
/// count is the product of its columns' bounds, capped by that side's rows: a
/// composite key cannot have more distinct values than there are rows.
fn join_estimate(
    rows: f64,
    candidate: usize,
    placed: &[usize],
    sizes: &[u64],
    edges: &[Edge],
) -> Option<f64> {
    let size = *sizes.get(candidate)? as f64;
    let mut placed_ndv = 1.0_f64;
    let mut candidate_ndv = 1.0_f64;
    let mut connected = false;
    for edge in edges {
        let (theirs, ours) = if edge.a == candidate && placed.contains(&edge.b) {
            (edge.ndv_b, edge.ndv_a)
        } else if edge.b == candidate && placed.contains(&edge.a) {
            (edge.ndv_a, edge.ndv_b)
        } else {
            continue;
        };
        connected = true;
        placed_ndv *= theirs;
        candidate_ndv *= ours;
    }
    if !connected {
        return None;
    }
    let denominator = placed_ndv.min(rows).max(candidate_ndv.min(size)).max(1.0);
    Some((rows * size / denominator).max(1.0))
}

/// Estimated cost of joining in `order`: the sum of its intermediate results.
/// `None` if some step would have no edge to what precedes it.
///
/// The last join's output is left out. It is the query's result, the same
/// rows whatever the order, so counting it only adds the same large number to
/// both sides of the comparison and hides the difference between them.
fn order_cost(order: &[usize], sizes: &[u64], edges: &[Edge]) -> Option<f64> {
    let first = *order.first()?;
    let mut rows = *sizes.get(first)? as f64;
    let mut placed = vec![first];
    let mut cost = 0.0;
    let last = order.len().saturating_sub(1);
    for (step, next) in order.iter().copied().enumerate().skip(1) {
        rows = join_estimate(rows, next, &placed, sizes, edges)?;
        if step < last {
            cost += rows;
        }
        placed.push(next);
    }
    Some(cost)
}

/// Greedy order: keep the first relation, then repeatedly take the connected
/// relation whose join is estimated to produce the fewest rows — the smaller
/// relation on a tie, which is every candidate when each joins on a key.
///
/// The anchor is deliberately *not* chosen by size. It is the relation the query
/// named first, which in a star-schema query is the fact table and is what every
/// dimension reduces; re-anchoring on the smallest dimension would rebuild the
/// same chain upside down for no measured gain, and would deviate from the
/// author's written order in every query rather than only where sizes demand it.
fn greedy_order(sizes: &[u64], edges: &[Edge], count: usize) -> Option<Vec<usize>> {
    let mut placed = vec![0usize];
    let mut rows = *sizes.first()? as f64;
    let mut remaining: Vec<usize> = (1..count).collect();
    while !remaining.is_empty() {
        let mut best: Option<(f64, u64, usize)> = None;
        for candidate in remaining.iter().copied() {
            let Some(estimate) = join_estimate(rows, candidate, &placed, sizes, edges) else {
                continue;
            };
            let key = (
                estimate,
                sizes.get(candidate).copied().unwrap_or(u64::MAX),
                candidate,
            );
            if best.is_none_or(|current| (key.0, key.1, key.2) < current) {
                best = Some(key);
            }
        }
        // Nothing left is reachable by an equijoin: finishing the chain would
        // mean inventing a cross join. Leave the plan alone.
        let (estimate, _, next) = best?;
        remaining.retain(|index| *index != next);
        placed.push(next);
        rows = estimate;
    }
    Some(placed)
}

/// How much cheaper, by estimate, the greedy order must be than the written
/// one before the rule replaces it. See the module docs.
const REQUIRED_ESTIMATED_GAIN: f64 = 2.0;

impl OptimizerRule for JoinReorder {
    fn name(&self) -> &str {
        "join_reorder"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        // Top-down, so the deepest join of a chain is reached through its root
        // and the whole chain is flattened once rather than once per level.
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !self.forced && !join_reorder_enabled() {
            return Ok(Transformed::no(plan));
        }
        let Some(chain) = flatten(&plan) else {
            return Ok(Transformed::no(plan));
        };
        let count = chain.relations.len();
        if !(3..=MAX_RELATIONS).contains(&count) {
            return Ok(Transformed::no(plan));
        }
        let Some(sizes) = chain
            .relations
            .iter()
            .map(|relation| self.size_of(relation))
            .collect::<Option<Vec<u64>>>()
        else {
            return Ok(Transformed::no(plan));
        };

        let owners = column_owners(&chain.relations);
        let mut edges = Vec::with_capacity(chain.on.len());
        for (left, right) in &chain.on {
            let (Some(l), Some(r)) = (referenced(left, &owners), referenced(right, &owners)) else {
                return Ok(Transformed::no(plan));
            };
            // An equijoin side that spans relations is not an edge this greedy
            // models; declining keeps the rewrite honest about what it placed.
            if l.len() != 1 || r.len() != 1 {
                return Ok(Transformed::no(plan));
            }
            let (Some(l), Some(r)) = (l.into_iter().next(), r.into_iter().next()) else {
                return Ok(Transformed::no(plan));
            };
            // A key whose distinct values are unknown makes every estimate
            // through it a guess, and guessing is how a 25-value key was taken
            // for a selective one. Decline, as for an unsized relation.
            let (Expr::Column(left_column), Expr::Column(right_column)) = (left, right) else {
                return Ok(Transformed::no(plan));
            };
            let (Some(left_relation), Some(right_relation)) =
                (chain.relations.get(l), chain.relations.get(r))
            else {
                return Ok(Transformed::no(plan));
            };
            let (Some(ndv_a), Some(ndv_b)) = (
                self.ndv_of(left_relation, left_column),
                self.ndv_of(right_relation, right_column),
            ) else {
                return Ok(Transformed::no(plan));
            };
            edges.push(Edge {
                a: l,
                b: r,
                ndv_a: ndv_a as f64,
                ndv_b: ndv_b as f64,
            });
        }

        // Only chains whose written order is actually inverted are reordered.
        //
        // A relation larger than the anchor is one the `FROM` clause asked to be
        // joined before things that could have shrunk it — q72's `inventory`,
        // 11.74 M rows against a 1.44 M row fact. Where every relation is
        // smaller than the anchor, the written order is already fact-first and
        // moving anything is a bet on selectivity this rule cannot estimate:
        // reordering those chains regressed q24 4.0x (205 ms -> 805 ms), q50
        // 2.0x and q36 1.4x on TPC-DS SF1, because `store_sales ⋈ store_returns`
        // is a near-1:1 fact-to-fact join that *reduces*, and size alone cannot
        // tell it from q72's fact-to-fact join that multiplies.
        let Some(anchor) = sizes.first().copied() else {
            return Ok(Transformed::no(plan));
        };
        if !sizes.iter().skip(1).any(|size| *size > anchor) {
            return Ok(Transformed::no(plan));
        }
        let Some(order) = greedy_order(&sizes, &edges, count) else {
            return Ok(Transformed::no(plan));
        };
        let written: Vec<usize> = (0..count).collect();
        if order == written {
            return Ok(Transformed::no(plan));
        }
        // The written order is the default; the greedy one has to be clearly
        // cheaper by the same estimate to replace it. A written order with an
        // unconnected step (impossible for a chain DataFusion built, but not
        // assumed) has no cost and loses to any connected one.
        let Some(greedy_cost) = order_cost(&order, &sizes, &edges) else {
            return Ok(Transformed::no(plan));
        };
        if let Some(written_cost) = order_cost(&written, &sizes, &edges)
            && greedy_cost * REQUIRED_ESTIMATED_GAIN > written_cost
        {
            return Ok(Transformed::no(plan));
        }

        let original_schema: DFSchemaRef = Arc::clone(plan.schema());
        let pairs: Vec<(usize, usize)> = edges.iter().map(|edge| (edge.a, edge.b)).collect();
        match rebuild(&plan, &chain, &owners, &pairs, &order, &original_schema) {
            Some(rebuilt) => Ok(Transformed::yes(rebuilt)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// Rebuild the chain left-deep in `order`, placing every predicate exactly once.
///
/// Returns `None` — meaning "leave the plan alone" — if any predicate cannot be
/// placed, rather than building a plan that has quietly dropped one.
fn rebuild(
    plan: &LogicalPlan,
    chain: &Chain,
    owners: &HashMap<Column, usize>,
    edges: &[(usize, usize)],
    order: &[usize],
    original_schema: &DFSchemaRef,
) -> Option<LogicalPlan> {
    let mut placed: HashSet<usize> = HashSet::new();
    let first = *order.first()?;
    placed.insert(first);
    let mut builder = LogicalPlanBuilder::from(chain.relations.get(first)?.clone());
    let mut used_on = vec![false; chain.on.len()];
    let mut used_filter = vec![false; chain.filters.len()];

    for step in order.iter().skip(1) {
        let next = *step;
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        for (index, (a, b)) in edges.iter().enumerate() {
            if used_on.get(index).copied()? {
                continue;
            }
            let (left, right) = chain.on.get(index)?;
            // Orient each pair so the already-placed side is on the left of the
            // new join, whichever side of the original pair it came from.
            if *a == next && placed.contains(b) {
                left_keys.push(right.clone());
                right_keys.push(left.clone());
            } else if *b == next && placed.contains(a) {
                left_keys.push(left.clone());
                right_keys.push(right.clone());
            } else {
                continue;
            }
            *used_on.get_mut(index)? = true;
        }
        if left_keys.is_empty() {
            return None;
        }
        placed.insert(next);

        // A non-equi filter belongs at the first level where every relation it
        // names is present.
        let mut ready = Vec::new();
        for (index, filter) in chain.filters.iter().enumerate() {
            if used_filter.get(index).copied()? {
                continue;
            }
            let names = referenced(filter, owners)?;
            if names.iter().all(|relation| placed.contains(relation)) {
                ready.push(filter.clone());
                *used_filter.get_mut(index)? = true;
            }
        }
        let filter = ready.into_iter().reduce(Expr::and);

        // `Join::try_new` rather than `LogicalPlanBuilder::join_on`: the builder's
        // expression form parks equalities in the join's `filter`, nothing later
        // hoists them into `on`, and the physical planner then picks a nested
        // loop — the mechanism that once made q2 eighteen times slower (see
        // `semi_join_reduction`). Keys must land in `on` to stay a hash join.
        let joined = Join::try_new(
            Arc::new(builder.build().ok()?),
            Arc::new(chain.relations.get(next)?.clone()),
            left_keys.into_iter().zip(right_keys).collect(),
            filter,
            JoinType::Inner,
            JoinConstraint::On,
            chain.null_equality,
            false,
        )
        .ok()?;
        builder = LogicalPlanBuilder::from(LogicalPlan::Join(joined));
    }

    // Every predicate must have been placed; a leftover means the rewrite would
    // have changed the answer.
    if used_on.iter().any(|used| !used) || used_filter.iter().any(|used| !used) {
        return None;
    }

    // Reordering permutes the schema, so restore the original column order.
    let projection: Vec<Expr> = original_schema
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let rebuilt = builder.project(projection).ok()?.build().ok()?;
    if rebuilt.schema().as_ref() != original_schema.as_ref() {
        return None;
    }
    debug_assert!(matches!(plan, LogicalPlan::Join(_)));
    Some(rebuilt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    /// A one-column table; the rule reads sizes from the registry, never from
    /// the data, so the rows here exist only to make the join runnable.
    fn table(column: &str, values: &[i64]) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("batch");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"))
    }

    /// A two-column fact table joining out to both dimensions.
    fn fact() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cs_item_sk", DataType::Int64, false),
            Field::new("cs_demo_sk", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
            ],
        )
        .expect("batch");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"))
    }

    /// Registries for `tables`: each table's row count, and its columns'
    /// distinct-value bounds recorded at that row count.
    /// A table's name, row count, and `(column, distinct-value bound)` pairs.
    type TableFixture<'a> = (&'a str, u64, &'a [(&'a str, u64)]);

    fn registries(tables: &[TableFixture<'_>]) -> (TableRowCounts, TableKeyNdv) {
        let counts: TableRowCounts = Arc::default();
        let ndv: TableKeyNdv = Arc::default();
        for (table, rows, columns) in tables {
            counts.write().unwrap().insert((*table).to_owned(), *rows);
            let columns = columns
                .iter()
                .map(|(column, bound)| ((*column).to_owned(), *bound))
                .collect();
            record_key_ndv(&ndv, table, *rows, columns);
        }
        (counts, ndv)
    }

    /// q72's sizes: a fact, an inventory an order of magnitude larger, and a
    /// demographics dimension four orders smaller. Both joins are on keys of
    /// the dimension side (18 K items, 7.2 K demographics).
    fn sizes() -> (TableRowCounts, TableKeyNdv) {
        registries(&[
            (
                "catalog_sales",
                1_441_548,
                &[("cs_item_sk", 18_000), ("cs_demo_sk", 7_200)],
            ),
            ("inventory", 11_745_000, &[("inv_item_sk", 18_000)]),
            ("household_demographics", 7_200, &[("hd_demo_sk", 7_200)]),
        ])
    }

    fn context(registries: Option<(TableRowCounts, TableKeyNdv)>) -> SessionContext {
        let mut builder = SessionStateBuilder::new().with_default_features();
        if let Some((counts, ndv)) = registries {
            builder = builder.with_optimizer_rule(Arc::new(JoinReorder::forced(counts, ndv)));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("catalog_sales", fact()).expect("fact");
        ctx.register_table("inventory", table("inv_item_sk", &[1, 2, 3]))
            .expect("inventory");
        ctx.register_table("household_demographics", table("hd_demo_sk", &[1, 2]))
            .expect("demographics");
        ctx
    }

    /// q72's FROM order: the largest relation named immediately after the fact.
    const Q72_SHAPE: &str = "SELECT count(*) FROM catalog_sales \
        JOIN inventory ON (cs_item_sk = inv_item_sk) \
        JOIN household_demographics ON (cs_demo_sk = hd_demo_sk)";

    async fn plan_of(ctx: &SessionContext, sql: &str) -> String {
        format!(
            "{}",
            ctx.sql(sql)
                .await
                .expect("plan")
                .into_optimized_plan()
                .expect("optimize")
                .display_indent()
        )
    }

    /// Index of the first join line naming `key`, deepest-last in a printed tree.
    fn join_line(plan: &str, key: &str) -> usize {
        plan.lines()
            .position(|line| line.contains("Inner Join") && line.contains(key))
            .unwrap_or_else(|| panic!("no inner join on {key} in:\n{plan}"))
    }

    /// The big relation must end up ABOVE the small one.
    ///
    /// This is the whole rule. Asserting merely that the plan changed would pass
    /// on a reordering that moved `inventory` deeper, which is the plan q72
    /// already had and the one that costs 2655 ms.
    #[tokio::test]
    async fn the_largest_relation_is_joined_last() {
        let plan = plan_of(&context(Some(sizes())), Q72_SHAPE).await;
        let inventory = join_line(&plan, "inv_item_sk");
        let demographics = join_line(&plan, "hd_demo_sk");
        assert!(
            inventory < demographics,
            "the 11.7M-row inventory join must sit above the 7.2K-row \
             demographics join, so the small one filters the fact stream first:\n{plan}"
        );
    }

    /// Without the rule the plan is left-deep in FROM order — the shape the
    /// assertion above rejects. Pins what is being changed.
    #[tokio::test]
    async fn from_clause_order_is_what_datafusion_leaves_behind() {
        let plan = plan_of(&context(None), Q72_SHAPE).await;
        assert!(
            join_line(&plan, "inv_item_sk") > join_line(&plan, "hd_demo_sk"),
            "DataFusion 54 has no join reordering; the chain should still be in \
             FROM order:\n{plan}"
        );
    }

    /// A relation the registry cannot size must abandon the whole rewrite.
    ///
    /// Reordering around a guessed size is how a rule moves the largest table to
    /// the bottom, so "unknown" has to mean "decline", not "assume small".
    #[tokio::test]
    async fn an_unsized_relation_declines_the_whole_chain() {
        let partial = registries(&[("inventory", 11_745_000, &[("inv_item_sk", 18_000)])]);
        let plan = plan_of(&context(Some(partial)), Q72_SHAPE).await;
        assert!(
            join_line(&plan, "inv_item_sk") > join_line(&plan, "hd_demo_sk"),
            "with `catalog_sales` and `household_demographics` unsized the rule \
             must leave the plan alone:\n{plan}"
        );
    }

    /// A chain already written fact-first must be left alone.
    ///
    /// q24's `store_sales ⋈ store_returns` is a near-1:1 fact-to-fact join that
    /// *reduces*; q72's `catalog_sales ⋈ inventory` is one that multiplies.
    /// Base-table size cannot tell them apart, so the rule only reorders chains
    /// where a relation is larger than the anchor — the case where the written
    /// order is demonstrably inverted. Without this guard q24 ran 4.0x slower
    /// (205 ms -> 805 ms on TPC-DS SF1).
    #[tokio::test]
    async fn a_chain_already_written_largest_first_is_left_alone() {
        let counts = registries(&[
            (
                "catalog_sales",
                2_880_404,
                &[("cs_item_sk", 18_000), ("cs_demo_sk", 7_200)],
            ),
            ("inventory", 287_514, &[("inv_item_sk", 18_000)]),
            ("household_demographics", 7_200, &[("hd_demo_sk", 7_200)]),
        ]);
        let plan = plan_of(&context(Some(counts)), Q72_SHAPE).await;
        assert!(
            join_line(&plan, "inv_item_sk") > join_line(&plan, "hd_demo_sk"),
            "every relation is smaller than the anchor, so the written order \
             stands and the rule must not reorder:\n{plan}"
        );
    }

    /// A key whose distinct values are unknown declines the rewrite, as an
    /// unsized relation does: an estimate through it would be a guess.
    #[tokio::test]
    async fn a_join_key_without_a_distinct_value_bound_declines_the_chain() {
        let (counts, ndv) = sizes();
        ndv.write()
            .unwrap()
            .get_mut("household_demographics")
            .unwrap()
            .columns
            .clear();
        let plan = plan_of(&context(Some((counts, ndv))), Q72_SHAPE).await;
        assert!(
            join_line(&plan, "inv_item_sk") > join_line(&plan, "hd_demo_sk"),
            "with no bound on hd_demo_sk the rule must leave the plan alone:\n{plan}"
        );
    }

    /// A bound recorded at another row count is stale and is not used.
    ///
    /// `INSERT` and `TRUNCATE` move the row-count registry without re-reading
    /// statistics, so a bound taken before them may no longer hold.
    #[tokio::test]
    async fn a_bound_taken_at_another_row_count_is_not_used() {
        let (counts, ndv) = sizes();
        counts
            .write()
            .unwrap()
            .insert("household_demographics".to_owned(), 9_000);
        let plan = plan_of(&context(Some((counts, ndv))), Q72_SHAPE).await;
        assert!(
            join_line(&plan, "inv_item_sk") > join_line(&plan, "hd_demo_sk"),
            "a stale bound must decline the chain:\n{plan}"
        );
    }

    /// TPC-H q5's shape at SF100: the smallest connected relation is joined
    /// on a 25-value key, and must not be moved ahead of the key joins.
    ///
    /// The size greedy moved `supplier` (1 M rows) ahead of `orders` (150 M)
    /// because it is smaller, and `c_nationkey = s_nationkey` then produced
    /// 15 M × 1 M / 25 rows. q5 ran past 22 minutes and 38 GB instead of 21 s.
    #[tokio::test]
    async fn a_small_relation_joined_on_a_low_cardinality_key_is_not_moved_first() {
        fn two(left: &str, right: &str, rows: &[(i64, i64)]) -> Arc<MemTable> {
            let schema = Arc::new(Schema::new(vec![
                Field::new(left, DataType::Int64, false),
                Field::new(right, DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch");
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table"))
        }
        let (counts, ndv) = registries(&[
            (
                "customer",
                15_000_000,
                &[("c_custkey", 15_000_000), ("c_nationkey", 25)],
            ),
            ("orders", 150_000_000, &[("o_custkey", 10_000_000)]),
            ("supplier", 1_000_000, &[("s_nationkey", 25)]),
        ]);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(JoinReorder::forced(counts, ndv)))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "customer",
            two("c_custkey", "c_nationkey", &[(1, 1), (2, 2)]),
        )
        .expect("customer");
        ctx.register_table(
            "orders",
            two("o_custkey", "o_orderkey", &[(1, 10), (2, 20)]),
        )
        .expect("orders");
        ctx.register_table("supplier", table("s_nationkey", &[1, 2]))
            .expect("supplier");
        let plan = plan_of(
            &ctx,
            "SELECT count(*) FROM customer \
             JOIN orders ON (c_custkey = o_custkey) \
             JOIN supplier ON (c_nationkey = s_nationkey)",
        )
        .await;
        assert!(
            join_line(&plan, "s_nationkey") < join_line(&plan, "o_custkey"),
            "the nationkey join multiplies and must stay above the custkey join:\n{plan}"
        );
    }

    #[test]
    fn join_estimates_divide_by_the_larger_distinct_count() {
        let sizes = [15_000_000, 150_000_000, 1_000_000];
        let edges = [
            Edge {
                a: 0,
                b: 1,
                ndv_a: 15_000_000.0,
                ndv_b: 10_000_000.0,
            },
            Edge {
                a: 0,
                b: 2,
                ndv_a: 25.0,
                ndv_b: 25.0,
            },
        ];
        let orders = join_estimate(15e6, 1, &[0], &sizes, &edges).unwrap();
        let supplier = join_estimate(15e6, 2, &[0], &sizes, &edges).unwrap();
        assert_eq!(orders, 150e6, "a key join keeps the many side's rows");
        assert_eq!(supplier, 15e6 * 1e6 / 25.0, "a 25-value key multiplies");
        assert_eq!(greedy_order(&sizes, &edges, 3), Some(vec![0, 1, 2]));
        // Not connected: no estimate.
        assert_eq!(join_estimate(15e6, 2, &[1], &sizes, &edges), None);
    }

    /// A composite key cannot have more distinct values than either side has
    /// rows, so multiplying per-column bounds is capped.
    #[test]
    fn a_composite_key_distinct_count_is_capped_by_rows() {
        let sizes = [600_000_000, 80_000_000];
        let edges = [
            Edge {
                a: 0,
                b: 1,
                ndv_a: 20_000_000.0,
                ndv_b: 20_000_000.0,
            },
            Edge {
                a: 0,
                b: 1,
                ndv_a: 1_000_000.0,
                ndv_b: 1_000_000.0,
            },
        ];
        let estimate = join_estimate(600e6, 1, &[0], &sizes, &edges).unwrap();
        assert_eq!(estimate, 80e6, "600M x 80M / min(2e13, 600M) = 80M");
    }

    #[test]
    fn distinct_value_bounds_come_from_statistics_and_data() {
        use datafusion::common::stats::ColumnStatistics;
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("counted", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("wide", DataType::Int64, false),
        ]);
        let column = |min: ScalarValue, max: ScalarValue| ColumnStatistics {
            min_value: Precision::Exact(min),
            max_value: Precision::Exact(max),
            ..ColumnStatistics::new_unknown()
        };
        let stats = Statistics {
            num_rows: Precision::Exact(1_000),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                column(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(24))),
                ColumnStatistics {
                    distinct_count: Precision::Inexact(7),
                    ..ColumnStatistics::new_unknown()
                },
                column(ScalarValue::from("a"), ScalarValue::from("z")),
                column(
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(1 << 40)),
                ),
            ],
        };
        let bounds = column_ndv_bounds(&schema, &stats, 1_000);
        assert_eq!(bounds.get("key"), Some(&25), "the [min, max] span");
        assert_eq!(bounds.get("counted"), Some(&7), "a recorded distinct count");
        assert_eq!(bounds.get("name"), None, "strings have no range bound");
        assert_eq!(bounds.get("wide"), Some(&1_000), "capped by the row count");

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)])),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                3, 7, 5,
            ]))],
        )
        .unwrap();
        assert_eq!(ndv_bounds_from_batches(&[batch], 3).get("k"), Some(&3));
    }

    /// Reordering permutes the join schema; the answer must not move with it.
    #[tokio::test]
    async fn the_reordered_plan_returns_the_same_rows() {
        const SHAPE: &str = "SELECT cs_item_sk, cs_demo_sk, inv_item_sk, hd_demo_sk \
            FROM catalog_sales \
            JOIN inventory ON (cs_item_sk = inv_item_sk) \
            JOIN household_demographics ON (cs_demo_sk = hd_demo_sk) \
            ORDER BY cs_item_sk, cs_demo_sk";
        let reordered = rows(&context(Some(sizes())), SHAPE).await;
        let baseline = rows(&context(None), SHAPE).await;
        assert_eq!(
            reordered, baseline,
            "reordering an inner-join chain must not change the answer"
        );
        assert!(!baseline.is_empty(), "the fixture must actually join");
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let mut out = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let mut cells = Vec::new();
                for column in 0..batch.num_columns() {
                    let values = datafusion::common::cast::as_int64_array(batch.column(column))
                        .expect("int64");
                    cells.push(values.value(row).to_string());
                }
                out.push(cells.join("|"));
            }
        }
        out
    }
}
