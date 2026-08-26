//! Cut a multi-operator query into a chain of single-operator views.
//!
//! # Why
//!
//! [`crate::plan`] matches **one operator per view**. Every TPC-H query
//! composes several — filter, derived measures, grouped aggregate — so handed
//! over whole, not one of the twenty-two gets an O(delta) plan. Cut by hand
//! into hops they all plan `Incremental`, including
//! `sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))` over
//! `DECIMAL(15,2)`. The operators were never the gap; this is.
//!
//! # Scope, and why it is this narrow
//!
//! **Linear chains over a single source.** A `Join` makes the plan a DAG with
//! its own naming and lifecycle questions, and is refused. That is not
//! caution for its own sake: one non-incremental hop mid-chain forces its
//! upstream to full-recompute every tick, so a *partially* cut query is
//! slower than an uncut one. Half of this feature is worse than none of it,
//! which is why the whole decomposition is discarded if any hop fails.
//!
//! # The two mechanics that make a hop a real relation
//!
//! **Hops are re-rooted structurally, never textually.** A hop's SQL is
//! produced by replacing the node's input with a scan of the hop below and
//! unparsing the result — not by editing `FROM` in generated text, which
//! breaks on a `FROM` inside a string literal and is the sort of mechanism
//! that yields silently wrong answers.
//!
//! **The hop below is scanned under the original table's name.** A node's
//! expressions carry qualified references (`lineitem.l_quantity`); pointing
//! them at `__ivm_v_h0` would leave them unresolvable. Aliasing the hop scan
//! back to `lineitem` keeps every reference valid without rewriting a single
//! expression — which matters, because rewriting expressions is where a
//! decomposer would start quietly changing what the query means.
//!
//! **Every hop carries an explicit projection.** A bare `Filter` node has no
//! select list of its own and unparses to `SELECT FROM lineitem WHERE ...`,
//! which re-plans to zero columns (measured across the TPC-H corpus in
//! `krishiv-bench/tests/ivm_hop_round_trip.rs`: 165/220 bare, 220/220
//! projected).

use std::sync::Arc;

use ahash::AHashMap;
use arrow::datatypes::SchemaRef;
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, Filter as LogicalFilter, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;

use crate::plan::{ViewPlan, ViewPlanKind, build_view_plan_single};

/// One hop: a single-operator query over the hop beneath it.
#[derive(Debug, Clone)]
pub struct Hop {
    /// Generated for intermediate hops; the caller's own name for the last.
    pub name: String,
    pub body_sql: String,
    /// What this hop emits, and therefore what the hop above may reference.
    pub schema: SchemaRef,
}

/// Name an intermediate hop: a deterministic function of the view name and
/// position, never a counter or map-iteration order. This name *is* the
/// identity of the hop's checkpointed operator state (IVM-AUD-STALE-1 is what
/// happens when state and identity disagree), and the `__ivm_` prefix keeps
/// generated names out of the space a user view can occupy.
fn hop_name(view: &str, index: usize) -> String {
    format!("__ivm_{}_h{index}", view.to_lowercase())
}

/// Nodes that carry no operator: peeled through, never cut.
fn is_passthrough(node: &LogicalPlan) -> bool {
    matches!(
        node,
        LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_)
    )
}

/// Nodes that are read-time properties of the relation rather than operators.
/// IVM-AUD-ORDER-1 applies ordering at snapshot time and IVM-AUD-TOPN-1 owns
/// `LIMIT`, so these ride on the final hop instead of becoming hops.
fn is_read_time(node: &LogicalPlan) -> bool {
    matches!(node, LogicalPlan::Sort(_) | LogicalPlan::Limit(_))
}

/// The chain from leaf scan to root, leaf first, or `None` if not linear.
fn linear_chain(plan: &LogicalPlan) -> Option<Vec<LogicalPlan>> {
    let mut chain = Vec::new();
    let mut node = plan.clone();
    loop {
        match &node {
            LogicalPlan::TableScan(_) => {
                chain.push(node);
                chain.reverse();
                return Some(chain);
            }
            // DECOMP-4/MJOIN-1: a Join joins the chain. A LEFT-DEEP join
            // tree — the shape every comma join plans to — descends along the
            // left spine, each Join becoming one chain node whose RIGHT side
            // must be a plain source (a bushy right side fails the hop
            // planner and refuses the chain). The bottom-most Join, whose
            // left side is also plain, is the leaf.
            LogicalPlan::Join(join) => {
                let left_is_join = matches!(join.left.as_ref(), LogicalPlan::Join(_));
                chain.push(node.clone());
                if left_is_join {
                    let LogicalPlan::Join(join) = node else {
                        return None;
                    };
                    node = join.left.as_ref().clone();
                    continue;
                }
                chain.reverse();
                return Some(chain);
            }
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_) => {
                let inputs = node.inputs();
                if inputs.len() != 1 {
                    return None;
                }
                let next = (*inputs.first()?).clone();
                chain.push(node);
                node = next;
            }
            // Joins, unions, windows, set operations, recursion.
            _ => return None,
        }
    }
}

/// What a linear chain reads at the bottom: one table, or one two-source join.
enum Leaf {
    /// Scan of a single table — hops above are re-rooted onto a scan of the
    /// hop below WEARING this name, so their qualified references resolve.
    Table(String),
    /// A two-source join — its emitted relation carries flat bare names (the
    /// decomposer refuses collisions), so hops above are re-rooted onto an
    /// UNALIASED scan with their references unqualified, which is the sound
    /// transform ALIAS-1 established for a single relation.
    Join,
}

/// REORDER-1: rebuild a pure comma-join run in a CONNECTED order of its join
/// graph, so predicate distribution keys every level. Returns `None` when the
/// current order is already fine, when the graph is disconnected (a true
/// cross join — the caller keeps the original run and the keyless level
/// refuses downstream), or when anything fails to rebuild.
fn relinearize_join_run(join_run: &[LogicalPlan], predicate: &Expr) -> Option<Vec<LogicalPlan>> {
    // The sides, FROM order: leaf-left, then each level's right.
    let mut sides: Vec<LogicalPlan> = Vec::new();
    if let Some(LogicalPlan::Join(j0)) = join_run.first() {
        sides.push(j0.left.as_ref().clone());
    }
    for jn in join_run {
        let LogicalPlan::Join(j) = jn else {
            return None;
        };
        sides.push(j.right.as_ref().clone());
    }
    // Adjacency from cross-side plain-column equalities.
    let side_of = |c: &datafusion::common::Column| -> Option<usize> {
        sides
            .iter()
            .position(|sp| sp.schema().index_of_column(c).is_ok())
    };
    let mut edges: Vec<(usize, usize)> = Vec::new();
    for conjunct in datafusion::logical_expr::utils::split_conjunction(predicate) {
        if let Expr::BinaryExpr(be) = conjunct
            && be.op == datafusion::logical_expr::Operator::Eq
            && let (Expr::Column(a), Expr::Column(b)) = (be.left.as_ref(), be.right.as_ref())
            && let (Some(sa), Some(sb)) = (side_of(a), side_of(b))
            && sa != sb
        {
            edges.push((sa, sb));
        }
    }
    let connected_to = |set: &[usize], v: usize| {
        edges
            .iter()
            .any(|(a, b)| (*a == v && set.contains(b)) || (*b == v && set.contains(a)))
    };
    // Already connected level by level? Keep the original order.
    let mut prefix: Vec<usize> = vec![0];
    let already_fine = (1..sides.len()).all(|k| {
        let ok = connected_to(&prefix, k);
        prefix.push(k);
        ok
    });
    if already_fine {
        return None;
    }
    // Greedy connected order from side 0.
    let mut order: Vec<usize> = vec![0];
    while order.len() < sides.len() {
        let next = (0..sides.len()).find(|v| !order.contains(v) && connected_to(&order, *v))?;
        order.push(next);
    }
    // Rebuild the left-deep run in that order.
    let mut builder = LogicalPlanBuilder::from(sides.get(*order.first()?)?.clone());
    for idx in order.iter().skip(1) {
        builder = builder.cross_join(sides.get(*idx)?.clone()).ok()?;
    }
    let rebuilt = builder.build().ok()?;
    // Collect the new run leaf-first, mirroring `linear_chain`.
    let mut run_rev: Vec<LogicalPlan> = Vec::new();
    let mut node = rebuilt;
    loop {
        match node {
            LogicalPlan::Join(ref j) => {
                let left = j.left.as_ref().clone();
                run_rev.push(node);
                if matches!(left, LogicalPlan::Join(_)) {
                    node = left;
                } else {
                    break;
                }
            }
            _ => return None,
        }
    }
    run_rev.reverse();
    Some(run_rev)
}

fn leaf_of(chain: &[LogicalPlan]) -> Option<Leaf> {
    match chain.first()? {
        LogicalPlan::TableScan(ts) => Some(Leaf::Table(ts.table_name.table().to_string())),
        LogicalPlan::Join(_) => Some(Leaf::Join),
        _ => None,
    }
}

/// True for nodes that have no select list of their own and so unparse to
/// `SELECT FROM t WHERE ...` — an empty projection that re-plans to zero
/// columns. A `Projection` or `Aggregate` already carries one; wrapping those
/// again nests a derived table, and a projection over a projection is exactly
/// the shape IVM-AUD-RESOLVE-1 taught the planner to refuse.
fn needs_projection(node: &LogicalPlan) -> bool {
    matches!(node, LogicalPlan::Filter(_) | LogicalPlan::Join(_))
}

/// Project every column a node exposes, under its own name.
///
/// Names are preserved rather than positionally aliased because a linear chain
/// reads one source and therefore cannot expose the same qualified name twice
/// — the collision that forces positional aliases is a self-join, which this
/// module refuses anyway. Keeping names is what lets the hop above be
/// re-rooted without rewriting any expression.
fn explicit_projection(node: &LogicalPlan) -> Option<LogicalPlan> {
    let exprs: Vec<Expr> = node
        .schema()
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect();
    LogicalPlanBuilder::from(node.clone())
        .project(exprs)
        .ok()?
        .build()
        .ok()
}

/// A plain scan of `hop` — for hops above a join leaf, whose relation carries
/// flat bare names that qualified references are unqualified against.
fn bare_hop_scan(hop: &str, schema: &SchemaRef) -> Option<LogicalPlan> {
    let empty = arrow::array::RecordBatch::new_empty(schema.clone());
    let table = MemTable::try_new(schema.clone(), vec![vec![empty]]).ok()?;
    LogicalPlanBuilder::scan(
        hop,
        datafusion::datasource::provider_as_source(Arc::new(table)),
        None,
    )
    .ok()?
    .build()
    .ok()
}

/// Strip qualifiers from every column reference in `exprs` — sound over one
/// relation with unique bare names (see `Leaf::Join`).
fn unqualify_exprs(exprs: &[Expr]) -> Option<Vec<Expr>> {
    use datafusion::common::Column;
    exprs
        .iter()
        .map(|e| {
            e.clone()
                .transform(|node| {
                    Ok(match node {
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

/// A scan of `hop`, wearing `alias` so the caller's qualified column
/// references still resolve against it.
fn aliased_hop_scan(hop: &str, schema: &SchemaRef, alias: &str) -> Option<LogicalPlan> {
    let empty = arrow::array::RecordBatch::new_empty(schema.clone());
    let table = MemTable::try_new(schema.clone(), vec![vec![empty]]).ok()?;
    LogicalPlanBuilder::scan(
        hop,
        datafusion::datasource::provider_as_source(Arc::new(table)),
        None,
    )
    .ok()?
    .alias(alias)
    .ok()?
    .build()
    .ok()
}

/// An aggregate whose argument or group key is an expression needs a hop
/// beneath it that materialises the expression as a column.
///
/// `SUM(l_extendedprice * l_discount)` is not a shape the incremental
/// aggregate accepts — its arguments and group keys must be plain columns, so
/// that a delta row's contribution can be read straight out of the batch. The
/// product has to exist as a column first, which means synthesising a hop the
/// query never mentioned: project everything the aggregate's input exposes,
/// plus each computed sub-expression under a generated name, then rewrite the
/// aggregate to reference those names.
///
/// The rewrite substitutes whole expressions, never parts of them, so what the
/// aggregate computes is unchanged — it is the same expression, read from a
/// column instead of recomputed. Returns `(hoist projection, rewritten
/// aggregate exprs)`, or `None` if there is nothing to hoist.
fn hoist_computed_inputs(node: &LogicalPlan) -> Option<(LogicalPlan, Vec<Expr>)> {
    let LogicalPlan::Aggregate(agg) = node else {
        return None;
    };
    // Every computed expression the aggregate depends on, in a stable order.
    let mut computed: Vec<Expr> = Vec::new();
    let mut note = |e: &Expr| {
        if !matches!(e, Expr::Column(_) | Expr::Literal(_, _)) && !computed.contains(e) {
            computed.push(e.clone());
        }
    };
    for g in &agg.group_expr {
        note(g);
    }
    for a in &agg.aggr_expr {
        if let Expr::AggregateFunction(f) = a {
            for arg in &f.params.args {
                note(arg);
            }
        }
    }
    if computed.is_empty() {
        return None;
    }

    // The hop: pass every input column through, then add the computed ones.
    let mut proj: Vec<Expr> = agg
        .input
        .schema()
        .iter()
        .map(|(q, f)| Expr::Column(Column::new(q.cloned(), f.name())))
        .collect();
    let names: Vec<String> = (0..computed.len()).map(|i| format!("__ivm_e{i}")).collect();
    for (e, n) in computed.iter().zip(names.iter()) {
        proj.push(e.clone().alias(n.clone()));
    }
    let hop = LogicalPlanBuilder::from(agg.input.as_ref().clone())
        .project(proj)
        .ok()?
        .build()
        .ok()?;

    // Rewrite the aggregate's expressions to read the materialised columns.
    // Returns `(expr, changed)` so the caller can tell a rewrite from a
    // pass-through.
    let swap = |e: Expr| -> Option<(Expr, bool)> {
        // Top-down, not bottom-up: substitution must replace WHOLE recorded
        // expressions. TPC-H q1 carries both `a * (1-d)` and
        // `a * (1-d) * (1+t)` — bottom-up rewrites the inner product first,
        // the outer expression then no longer equals its recorded form, and
        // the aggregate keeps a computed argument the hop planner refuses.
        e.transform_down(|node| {
            for (c, n) in computed.iter().zip(names.iter()) {
                if &node == c {
                    return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(n))));
                }
            }
            Ok(Transformed::no(node))
        })
        .ok()
        .map(|t| (t.data, t.transformed))
    };
    // The hoist must be invisible above the aggregate: whatever sits on top
    // still references the aggregate's ORIGINAL output names — `sum(a * b)`,
    // not `sum(__ivm_e0)` — so every rewritten expression is aliased back to
    // the name the original node's schema gave it (group fields first, then
    // aggregate fields, which is exactly the aggregate's schema order).
    // Without the alias, re-rooting the node above fails with "no field named
    // sum(a * b)" and the whole chain is refused.
    let original_names: Vec<String> = agg
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if original_names.len() != agg.group_expr.len() + agg.aggr_expr.len() {
        return None;
    }
    let mut rewritten = Vec::new();
    for (e, name) in agg
        .group_expr
        .iter()
        .chain(agg.aggr_expr.iter())
        .zip(original_names.iter())
    {
        let (swapped, changed) = swap(e.clone())?;
        rewritten.push(if changed {
            swapped.alias(name.clone())
        } else {
            swapped
        });
    }
    Some((hop, rewritten))
}

/// Cut `body_sql` into hops, or return `None` to leave the view alone.
///
/// The last hop keeps the caller's `view_name` and `declared` schema, so the
/// registered view keeps its identity and its contract; only what sits beneath
/// it is new.
pub async fn decompose(
    view_name: &str,
    body_sql: &str,
    declared: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<Vec<Hop>> {
    decompose_core(view_name, body_sql, declared, available_schemas)
        .await
        .map(|(_, hops, _)| hops)
}

/// Cut `body_sql` into a single [`ViewPlan::Chain`] the flow can run in place
/// — the wiring that makes decomposition an engine capability instead of a
/// library call (DECOMP-2). The hops stay internal to the one registered view:
/// no generated view names, no separate checkpoint identities, no distributed
/// attach questions — the chain checkpoints, restores and seeds through the
/// view's own `ViewPlan` surface like every other plan.
/// Returns a boxed, type-erased `Send` future rather than being an `async fn`:
/// the call graph is recursive (plan builder → decompose → per-hop plan
/// builder), and with concrete future types the compiler's `Send` proof
/// becomes self-referential — every async caller up to the HTTP handlers then
/// fails with "implementation of `Send` is not general enough". Erasing at
/// the signature keeps the concrete recursive type out of every caller's
/// state.
pub(crate) fn decompose_into_chain<'a>(
    body_sql: &'a str,
    declared: &'a SchemaRef,
    available_schemas: &'a AHashMap<String, SchemaRef>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<ViewPlan>> + Send + 'a>> {
    Box::pin(async move {
        let (source, _, plans) =
            decompose_core("chain", body_sql, declared, available_schemas).await?;
        Some(ViewPlan::Chain {
            source,
            hops: plans,
        })
    })
}

async fn decompose_core(
    view_name: &str,
    body_sql: &str,
    declared: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<(String, Vec<Hop>, Vec<ViewPlan>)> {
    let ctx = SessionContext::new();
    for (name, schema) in available_schemas {
        let empty = arrow::array::RecordBatch::new_empty(schema.clone());
        if let Ok(t) = MemTable::try_new(schema.clone(), vec![vec![empty]]) {
            let _ = ctx.register_table(name.as_str(), Arc::new(t));
        }
    }
    let plan = ctx.sql(body_sql).await.ok()?.logical_plan().clone();

    let chain = linear_chain(&plan)?;
    let leaf = leaf_of(&chain)?;

    // DECOMP-4: a join leaf emits a flat relation whose field names are the
    // bare column names of both sides. A collision would make every reference
    // above it ambiguous, so it is refused before any cutting happens.
    if matches!(leaf, Leaf::Join) {
        // MJOIN-1: the leaf join's LEFT side and every join level's RIGHT
        // side must be plain sources (the walk already descended the left
        // spine, so a non-source here means a bushy tree — refused before
        // any unparse: replanning a doomed multi-way tree recurses
        // DataFusion's planner deep enough to overflow a default thread
        // stack, found on q2's five-way comma join).
        let mut top_join: Option<&LogicalPlan> = None;
        for n in &chain {
            if let LogicalPlan::Join(j) = n {
                if top_join.is_none() && !crate::plan::side_resolves_to_source(&j.left) {
                    return None;
                }
                if !crate::plan::side_resolves_to_source(&j.right) {
                    return None;
                }
                top_join = Some(n);
            }
        }
        // Every bare column name across the ACCUMULATED relation must be
        // unique — hops above the joins reference them unqualified, and the
        // topmost join's schema carries all of them.
        let mut seen = std::collections::HashSet::new();
        for (_, f) in top_join?.schema().iter() {
            if !seen.insert(f.name().clone()) {
                return None;
            }
        }
    }

    // Operators to cut, bottom-up. Read-time nodes ride on the final hop.
    // An aggregate over computed inputs expands into two: the hop that
    // materialises them, then the aggregate itself. A join leaf becomes cut 0
    // — except when a Filter sits directly above it, which MERGES into the
    // join cut instead of becoming its own hop: a comma join carries its equi
    // keys in that WHERE, and a filter hop above a keyless join hop would
    // leave the join a cross join the planner refuses. The Filter node's own
    // input IS the join subtree, so using it unchanged as cut 0 is the merge.
    // Plain indexed loops, no closure-based iterator: a `filter(..)` closure
    // held in this future's state trips rustc's higher-ranked `FnOnce` proof
    // once the future must be `Send` (the boxed recursive edge requires it).
    let mut operator_nodes: Vec<&LogicalPlan> = Vec::new();
    for n in &chain {
        if !is_passthrough(n) && !is_read_time(n) {
            operator_nodes.push(n);
        }
    }
    let mut cuts: Vec<LogicalPlan> = Vec::new();
    let mut next = 0usize;
    if matches!(leaf, Leaf::Join) {
        // MJOIN-1: the run of join levels at the bottom of the chain, leaf
        // first. A single WHERE above the run (the comma-join idiom) is
        // DISTRIBUTED: each conjunct attaches to the LOWEST join level whose
        // accumulated schema covers its columns, so `c_custkey = o_custkey`
        // keys level one and `n_regionkey = r_regionkey` keys level five —
        // handing the whole WHERE to any single level would leave the others
        // keyless cross joins the planner refuses. Conjuncts no level covers
        // become a filter hop above the run.
        let mut join_run: Vec<LogicalPlan> = Vec::new();
        while let Some(n) = operator_nodes.get(next) {
            if matches!(n, LogicalPlan::Join(_)) {
                join_run.push((*n).clone());
                next += 1;
            } else {
                break;
            }
        }
        // REORDER-1: the FROM order is arbitrary, and a level whose side
        // relates to the accumulated set only THROUGH a later table is a
        // keyless cross join the planner refuses — q9 lists `part, supplier`
        // first, but they meet only via `lineitem`. When the run is a pure
        // comma join (no per-level ON), linearize the JOIN GRAPH instead:
        // adjacency from the WHERE's cross-side equalities, keep the original
        // order if every level is already connected to its prefix, otherwise
        // greedily append any side connected to the visited set — which keys
        // every level, because ANY connected order of a connected graph does.
        // A disconnected graph is a true cross join and refuses. Correctness
        // does not depend on WHICH connected order is chosen; cost does, and
        // choosing better than "first connected" is recorded future work.
        if join_run
            .iter()
            .all(|n| matches!(n, LogicalPlan::Join(j) if j.on.is_empty() && j.filter.is_none()))
            && let Some(LogicalPlan::Filter(f)) = operator_nodes.get(next)
        {
            join_run = relinearize_join_run(&join_run, &f.predicate).unwrap_or(join_run);
        }
        let mut level_preds: Vec<Vec<Expr>> = vec![Vec::new(); join_run.len()];
        let mut above_preds: Vec<Expr> = Vec::new();
        if let Some(LogicalPlan::Filter(f)) = operator_nodes.get(next) {
            for conjunct in datafusion::logical_expr::utils::split_conjunction(&f.predicate) {
                let cols = conjunct.column_refs();
                let level = join_run.iter().position(|jn| {
                    !cols.is_empty() && cols.iter().all(|c| jn.schema().index_of_column(c).is_ok())
                });
                match level {
                    Some(k) => level_preds.get_mut(k)?.push((*conjunct).clone()),
                    None => above_preds.push((*conjunct).clone()),
                }
            }
            next += 1; // the WHERE is consumed by the distribution
        }
        for (k, jn) in join_run.iter().enumerate() {
            let preds = level_preds.get(k)?;
            let cut = if preds.is_empty() {
                jn.clone()
            } else {
                let combined = preds.iter().cloned().reduce(|a, b| a.and(b))?;
                LogicalPlan::Filter(LogicalFilter::try_new(combined, Arc::new((*jn).clone())).ok()?)
            };
            cuts.push(cut);
        }
        if !above_preds.is_empty() {
            let combined = above_preds.iter().cloned().reduce(|a, b| a.and(b))?;
            let top = join_run.last()?.clone();
            cuts.push(LogicalPlan::Filter(
                LogicalFilter::try_new(combined, Arc::new(top)).ok()?,
            ));
        }
    }
    for n in operator_nodes.iter().skip(next) {
        match hoist_computed_inputs(n) {
            Some((hoist, rewritten)) => {
                let agg = n.with_new_exprs(rewritten, vec![hoist.clone()]).ok()?;
                cuts.push(hoist);
                cuts.push(agg);
            }
            None => cuts.push((*n).clone()),
        }
    }
    // One operator is what the planner already handles; cutting buys nothing.
    if cuts.len() < 2 {
        return None;
    }
    // A cut carrying a subquery can never plan as a hop — no operator compiles
    // an EXISTS / IN / scalar subquery — so the chain is doomed and is refused
    // BEFORE the unparse + replan round trip. This is not only economy: q20's
    // two-level nested IN-subqueries recurse DataFusion's planner deep enough
    // on the round trip to overflow a default-size thread stack.
    for cut in &cuts {
        for e in cut.expressions() {
            let mut has_subquery = false;
            let _ = e.apply(|node| {
                if matches!(
                    node,
                    Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
                ) {
                    has_subquery = true;
                    return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            });
            if has_subquery {
                return None;
            }
        }
    }
    // Whatever sits above the topmost operator (ORDER BY / LIMIT), in order.
    let root = chain.last()?;
    let above: Vec<LogicalPlan> = chain
        .iter()
        .skip_while(|n| !std::ptr::eq(*n, root) && !is_read_time(n))
        .filter(|n| is_read_time(n))
        .cloned()
        .collect();

    let mut hops: Vec<Hop> = Vec::new();
    let mut plans: Vec<ViewPlan> = Vec::new();
    let mut visible = available_schemas.clone();
    let last = cuts.len() - 1;
    // TOPN-2: a bare `Sort` is a read-time property (ORDER-1) and rides the
    // final hop; a `Sort` under a `Limit` is a TOP-N — it changes which rows
    // are in the relation — so the read-time run becomes its OWN final hop
    // over the last cut, where a projection's renames (`sum(…) AS revenue`)
    // are already plain columns the top-N matcher accepts.
    let topn_final = above.iter().any(|n| matches!(n, LogicalPlan::Limit(_)));

    for (i, node) in cuts.iter().enumerate() {
        // Re-root onto the hop below, structurally.
        let rooted = if i == 0 {
            node.clone()
        } else {
            let prev = hops.last()?;
            match &leaf {
                Leaf::Table(source) => {
                    let scan = aliased_hop_scan(&prev.name, &prev.schema, source)?;
                    node.with_new_exprs(node.expressions(), vec![scan]).ok()?
                }
                // The hop below carries flat bare names (collision-refused
                // above), so references unqualify — the accumulated relation
                // plus the level's right table have pairwise-unique bare
                // names, so an unqualified reference is unambiguous (the
                // ALIAS-1 rule). A mid-chain JOIN cut (MJOIN-1) re-roots its
                // LEFT input onto the hop below and keeps its right table; a
                // Filter(Join) cut rebuilds both layers.
                Leaf::Join => {
                    let scan = bare_hop_scan(&prev.name, &prev.schema)?;
                    match node {
                        LogicalPlan::Join(j) => {
                            let exprs = unqualify_exprs(&node.expressions())?;
                            node.with_new_exprs(exprs, vec![scan, j.right.as_ref().clone()])
                                .ok()?
                        }
                        LogicalPlan::Filter(f)
                            if matches!(f.input.as_ref(), LogicalPlan::Join(_)) =>
                        {
                            let LogicalPlan::Join(j) = f.input.as_ref() else {
                                return None;
                            };
                            let join_exprs = unqualify_exprs(&f.input.expressions())?;
                            let new_join = f
                                .input
                                .as_ref()
                                .clone()
                                .with_new_exprs(join_exprs, vec![scan, j.right.as_ref().clone()])
                                .ok()?;
                            let pred =
                                unqualify_exprs(std::slice::from_ref(&f.predicate))?.pop()?;
                            LogicalPlan::Filter(
                                LogicalFilter::try_new(pred, Arc::new(new_join)).ok()?,
                            )
                        }
                        _ => {
                            let mut exprs = unqualify_exprs(&node.expressions())?;
                            // Unqualifying changes DERIVED names — the
                            // aggregate `sum(profit.amount)` re-roots as
                            // `sum(amount)` — and the hop above references
                            // the ORIGINAL. Alias every expression back to
                            // the original node's schema name (the hoist's
                            // alias-back rule, applied to re-rooting): output
                            // names are part of a hop's contract. Projection
                            // and Aggregate only — a Filter's expression is a
                            // predicate and a Sort's is an ordering; neither
                            // names an output column.
                            if matches!(
                                node,
                                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_)
                            ) && node.schema().fields().len() == exprs.len()
                            {
                                exprs = exprs
                                    .into_iter()
                                    .zip(node.schema().fields().iter())
                                    .map(|(e, f)| e.alias(f.name().clone()))
                                    .collect();
                            }
                            node.with_new_exprs(exprs, vec![scan]).ok()?
                        }
                    }
                }
            }
        };

        let (name, target) = if i == last && !topn_final {
            // Re-apply ORDER BY / LIMIT so the final view keeps them. Above a
            // join leaf the relation is bare-named, so the read-time exprs
            // unqualify like every other re-rooted reference (a Sort keyed on
            // `profit.nation` reads plain `nation` from the hop relation).
            let mut top = rooted;
            for r in &above {
                let exprs = match &leaf {
                    Leaf::Join => unqualify_exprs(&r.expressions())?,
                    Leaf::Table(_) => r.expressions(),
                };
                top = r.with_new_exprs(exprs, vec![top]).ok()?;
            }
            (view_name.to_string(), top)
        } else {
            let body = if needs_projection(&rooted) {
                explicit_projection(&rooted)?
            } else {
                rooted
            };
            (hop_name(view_name, i), body)
        };

        let sql = datafusion::sql::unparser::plan_to_sql(&target)
            .ok()?
            .to_string();
        let schema: SchemaRef = if i == last && !topn_final {
            declared.clone()
        } else {
            Arc::new(target.schema().as_arrow().clone())
        };

        // Per-hop verification against the schemas the flow will actually have.
        // Anything short of a real O(delta) plan discards the whole chain —
        // and the verified plan IS the hop's operator, kept rather than
        // rebuilt, so what was checked is what runs.
        let plan = build_view_plan_single(&sql, &schema, &visible, &[]).await;
        if plan.kind() != ViewPlanKind::Incremental {
            return None;
        }
        plans.push(plan);
        visible.insert(name.clone(), schema.clone());
        hops.push(Hop {
            name,
            body_sql: sql,
            schema,
        });
    }

    // TOPN-2: the synthesized final top-N hop over the last intermediate hop.
    if topn_final {
        let prev = hops.last()?;
        let scan = match &leaf {
            Leaf::Table(source) => aliased_hop_scan(&prev.name, &prev.schema, source)?,
            Leaf::Join => bare_hop_scan(&prev.name, &prev.schema)?,
        };
        let mut top = scan;
        for r in &above {
            let exprs = match &leaf {
                Leaf::Join => unqualify_exprs(&r.expressions())?,
                Leaf::Table(_) => r.expressions(),
            };
            top = r.with_new_exprs(exprs, vec![top]).ok()?;
        }
        let sql = datafusion::sql::unparser::plan_to_sql(&top)
            .ok()?
            .to_string();
        let plan = build_view_plan_single(&sql, declared, &visible, &[]).await;
        if plan.kind() != ViewPlanKind::Incremental {
            return None;
        }
        plans.push(plan);
        hops.push(Hop {
            name: view_name.to_string(),
            body_sql: sql,
            schema: declared.clone(),
        });
    }

    // The chain's nominal source: the scan-leaf table, or the join hop's own
    // left source (used only as a fallback name — the flow reads a join hop's
    // sources from the hop plan itself).
    let source = match (&leaf, plans.first()) {
        (Leaf::Table(t), _) => t.clone(),
        (Leaf::Join, Some(ViewPlan::Join { left_source, .. })) => left_source.clone(),
        (Leaf::Join, _) => return None,
    };
    Some((source, hops, plans))
}
