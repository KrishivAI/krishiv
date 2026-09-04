//! Greedy reordering of inner-join chains by base-table size.
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
//! The third line is the one this rule implements, and it is why the rule is a
//! *size* greedy and not something cleverer: picking the smallest connected
//! relation next, using base-table row counts alone, already reproduces the
//! hand-tuned plan.
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
//! chain has a known size.
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
//! - **Idempotent.** If the greedy order is the order already in the plan, the
//!   rule reports no transform, so the optimizer reaches a fixed point.

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchemaRef, Result};
use datafusion::logical_expr::{
    Expr, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Row counts for registered base tables, keyed by table name.
pub type TableRowCounts = Arc<std::sync::RwLock<HashMap<String, u64>>>;

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
/// # What is NOT measured
///
/// SF1, embedded, one machine. **The distributed path and SF100 are unmeasured**,
/// and that is where a bad join order becomes a bad shuffle — the mechanism
/// behind every large regression this crate has recorded. The rule is on because
/// the full sweep supports it and its worst observed cost is 24 ms; re-run the
/// sweep at scale before trusting it there. `KRISHIV_JOIN_REORDER=off` disables it.
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
    /// Bypass the env gate and always apply — see
    /// [`crate::semi_join_reduction::SemiJoinPushdownThroughInnerJoin`] for why
    /// the rules in this crate carry one.
    forced: bool,
}

impl JoinReorder {
    /// The rule over `row_counts`, gated on [`JOIN_REORDER_ENV`].
    #[must_use]
    pub fn new(row_counts: TableRowCounts) -> Self {
        Self {
            row_counts,
            forced: false,
        }
    }

    /// The rule with its env gate bypassed, for tests and explicit opt-in.
    #[must_use]
    pub fn forced(row_counts: TableRowCounts) -> Self {
        Self {
            row_counts,
            forced: true,
        }
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

/// Greedy order: keep the first relation, then repeatedly take the smallest
/// relation that shares an equijoin edge with what is already placed.
///
/// The anchor is deliberately *not* chosen by size. It is the relation the query
/// named first, which in a star-schema query is the fact table and is what every
/// dimension reduces; re-anchoring on the smallest dimension would rebuild the
/// same chain upside down for no measured gain, and would deviate from the
/// author's written order in every query rather than only where sizes demand it.
fn greedy_order(sizes: &[u64], edges: &[(usize, usize)], count: usize) -> Option<Vec<usize>> {
    let mut placed = vec![0usize];
    let mut remaining: HashSet<usize> = (1..count).collect();
    while !remaining.is_empty() {
        let connected: Vec<usize> = remaining
            .iter()
            .copied()
            .filter(|candidate| {
                edges.iter().any(|(a, b)| {
                    (a == candidate && placed.contains(b)) || (b == candidate && placed.contains(a))
                })
            })
            .collect();
        // Nothing left is reachable by an equijoin: finishing the chain would
        // mean inventing a cross join. Leave the plan alone.
        let next = connected
            .iter()
            .copied()
            .min_by_key(|index| (sizes.get(*index).copied().unwrap_or(u64::MAX), *index))?;
        remaining.remove(&next);
        placed.push(next);
    }
    Some(placed)
}

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
            edges.push((l, r));
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
        if order == (0..count).collect::<Vec<_>>() {
            return Ok(Transformed::no(plan));
        }

        let original_schema: DFSchemaRef = Arc::clone(plan.schema());
        match rebuild(&plan, &chain, &owners, &edges, &order, &original_schema) {
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

    /// q72's sizes: a fact, an inventory an order of magnitude larger, and a
    /// demographics dimension four orders smaller.
    fn sizes() -> TableRowCounts {
        let counts: HashMap<String, u64> = [
            ("catalog_sales".to_owned(), 1_441_548),
            ("inventory".to_owned(), 11_745_000),
            ("household_demographics".to_owned(), 7_200),
        ]
        .into_iter()
        .collect();
        Arc::new(std::sync::RwLock::new(counts))
    }

    fn context(row_counts: Option<TableRowCounts>) -> SessionContext {
        let mut builder = SessionStateBuilder::new().with_default_features();
        if let Some(counts) = row_counts {
            builder = builder.with_optimizer_rule(Arc::new(JoinReorder::forced(counts)));
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
        let partial: TableRowCounts = Arc::new(std::sync::RwLock::new(
            [("inventory".to_owned(), 11_745_000u64)]
                .into_iter()
                .collect(),
        ));
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
        let counts: TableRowCounts = Arc::new(std::sync::RwLock::new(
            [
                ("catalog_sales".to_owned(), 2_880_404u64),
                ("inventory".to_owned(), 287_514),
                ("household_demographics".to_owned(), 7_200),
            ]
            .into_iter()
            .collect(),
        ));
        let plan = plan_of(&context(Some(counts)), Q72_SHAPE).await;
        assert!(
            join_line(&plan, "inv_item_sk") > join_line(&plan, "hd_demo_sk"),
            "every relation is smaller than the anchor, so the written order \
             stands and the rule must not reorder:\n{plan}"
        );
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
