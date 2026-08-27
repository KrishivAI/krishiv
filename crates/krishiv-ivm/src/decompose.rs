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
//! **Hops are re-rooted structurally, never textually.** A hop is produced
//! by replacing the node's input with a scan of the hop below — never by
//! editing `FROM` in generated text, which breaks on a `FROM` inside a
//! string literal and is the sort of mechanism that yields silently wrong
//! answers. The re-rooted plan is verified DIRECTLY by the single-operator
//! matchers, and the verified plan is the operator that runs (PLANHOP-1).
//! Verification used to round-trip each hop through `plan_to_sql` + replan +
//! re-decorrelation instead — three lossy transformations between the plan
//! the cutting logic reasoned about and the operator that ran, the exact
//! seam behind q9's rename-leak saga (REORDER-1) — and, measured before the
//! switch, byte-equivalent in outcome across the whole corpus (the exact
//! coverage gates moved by nothing). A hop's SQL rendering is best-effort
//! output for callers that register hops as standalone views; admission
//! never depends on it.
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

use crate::plan::{ChainSide, ViewPlan, ViewPlanKind, build_view_plan_from_logical};

/// What the cutting engine returns: the chain's nominal source, the public
/// hop records, the spine's verified plans, and any side sub-chains.
type CutPlan = (String, Vec<Hop>, Vec<ViewPlan>, Vec<ChainSide>);

/// One hop: a single-operator query over the hop beneath it.
#[derive(Debug, Clone)]
pub struct Hop {
    /// Generated for intermediate hops; the caller's own name for the last.
    pub name: String,
    /// Best-effort SQL rendering of the hop, for callers that register hops
    /// as standalone views. `None` when DataFusion's unparser cannot render
    /// the hop's plan — no current corpus hop hits this (the unparser renders
    /// even a semi/anti join back to EXISTS / NOT EXISTS), but chain
    /// admission must never depend on unparser coverage, so the type says so
    /// (PLANHOP-1). The chain itself carries the verified plan and never
    /// round-trips through SQL.
    pub body_sql: Option<String>,
    /// What this hop emits, and therefore what the hop above may reference.
    pub schema: SchemaRef,
}

/// Name an intermediate hop: a deterministic function of the view name and
/// position, never a counter or map-iteration order. This name *is* the
/// identity of the hop's checkpointed operator state (IVM-AUD-STALE-1 is what
/// happens when state and identity disagree), and the `__ivm_` prefix keeps
/// generated names out of the space a user view can occupy.
fn hop_name(view: &str, index: usize) -> String {
    // A SIDE's hops recurse through the same machinery under an already-
    // prefixed name (`__ivm_v_s0`); stripping the prefix before re-adding it
    // keeps `__ivm_v_s0_h0` instead of `__ivm___ivm_v_s0_h0`. User view
    // names never carry the prefix, so their hop names are unchanged.
    format!(
        "__ivm_{}_h{index}",
        view.trim_start_matches("__ivm_").to_lowercase()
    )
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

/// SIDE-2: [`explicit_projection`] for a cut whose join right side is a SIDE
/// scan. The side's equi-KEY columns are skipped: the operator never emits
/// them (JOIN-2 drops right keys), and a side key routinely shares its bare
/// name with a spine column (q17's side `l_partkey` vs lineitem's), so
/// projecting it would hand every hop above an ambiguous relation. Narrowed
/// to side joins so no pre-existing chain's hop schema changes shape.
fn explicit_projection_side_aware(
    node: &LogicalPlan,
    side_names: &ahash::AHashSet<String>,
) -> Option<LogicalPlan> {
    let join = match node {
        LogicalPlan::Join(j) => Some(j),
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::Join(j) => Some(j),
            _ => None,
        },
        _ => None,
    };
    let mut skip: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    if let Some(j) = join {
        let right_quals: std::collections::HashSet<String> = j
            .right
            .schema()
            .iter()
            .filter_map(|(q, _)| q.map(|t| t.to_string()))
            .collect();
        // LEFTAGG-1: a LEFT OUTER cut must ALSO skip its right keys — the
        // operator drops them, and the reference-rewrite that re-derives a
        // dropped right key from its left pair is exact only for INNER (an
        // unmatched padded row has a NULL right key against a non-NULL left
        // one), so projecting the key would refuse the hop outright (q13).
        let left_outer = j.join_type == datafusion::logical_expr::JoinType::Left;
        if left_outer || right_quals.iter().any(|q| side_names.contains(q)) {
            let mut keys: std::collections::HashSet<String> = std::collections::HashSet::new();
            for (_, r) in &j.on {
                if let Expr::Column(c) = r {
                    keys.insert(c.name.clone());
                }
            }
            if let Some(f) = &j.filter {
                for conjunct in datafusion::logical_expr::utils::split_conjunction(f) {
                    if let Expr::BinaryExpr(be) = conjunct
                        && be.op == datafusion::logical_expr::Operator::Eq
                        && let (Expr::Column(a), Expr::Column(b)) =
                            (be.left.as_ref(), be.right.as_ref())
                    {
                        let a_left = j.left.schema().index_of_column(a).is_ok();
                        let b_left = j.left.schema().index_of_column(b).is_ok();
                        match (a_left, b_left) {
                            (true, false) => {
                                keys.insert(b.name.clone());
                            }
                            (false, true) => {
                                keys.insert(a.name.clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
            for (q, f) in j.right.schema().iter() {
                if let Some(q) = q
                    && (left_outer || side_names.contains(&q.to_string()))
                    && keys.contains(f.name())
                {
                    skip.insert((q.to_string(), f.name().clone()));
                }
            }
        }
    }
    let exprs: Vec<Expr> = node
        .schema()
        .iter()
        .filter(|(q, f)| {
            !q.as_ref()
                .is_some_and(|q| skip.contains(&(q.to_string(), f.name().clone())))
        })
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
/// relation with unique bare names (see `Leaf::Join`). Qualifiers named in
/// `keep` survive: a SIDE-1 join condition references the side's relation by
/// its internal hop name (`__ivm_v_s0.pid`), and stripping that would make
/// the reference resolve against the SPINE relation whenever the bare name
/// exists on both — a silently wrong join key.
fn unqualify_exprs_keeping(exprs: &[Expr], keep: &ahash::AHashSet<String>) -> Option<Vec<Expr>> {
    use datafusion::common::Column;
    exprs
        .iter()
        .map(|e| {
            e.clone()
                .transform(|node| {
                    Ok(match node {
                        Expr::Column(c)
                            if !c
                                .relation
                                .as_ref()
                                .is_some_and(|r| keep.contains(&r.to_string())) =>
                        {
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

/// SIDE-1/SIDE-3: is this join side a candidate for its own sub-chain? A
/// side may be a linear fold over one table or, since SIDE-3, a join run of
/// its own (q2's `min(ps_supplycost)` reads four tables) — the recursion
/// through [`decompose_plan`] decides the rest. The refusals here mirror the
/// spine's: no read-time nodes (a `LIMIT` inside a side would change WHICH
/// rows the side holds — refusing is the only honest answer), no subquery
/// expressions anywhere in the chain's own nodes.
fn side_chain_shape(plan: &LogicalPlan) -> Option<()> {
    let chain = linear_chain(plan)?;
    leaf_of(&chain)?;
    if chain.iter().any(is_read_time) {
        return None;
    }
    let mut has_operator = false;
    for n in &chain {
        if !is_passthrough(n) {
            has_operator = true;
        }
        for e in n.expressions() {
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
    if !has_operator {
        return None;
    }
    Some(())
}

/// SIDE-3: cut a join side into its own sub-chain by RECURSING through the
/// spine's own cutting engine — guard, predicate distribution,
/// relinearization, re-rooting and per-hop verification are all
/// [`decompose_plan`]'s, under the side's name prefix. A side may therefore
/// be a join run of its own (q2's four-table `min` side). What a side may
/// NOT have, in v1, is sides of ITS OWN (q20's nested scalar) — the
/// recursion refuses them wholesale, recorded rather than half-taken. Side
/// hop names are deterministic (`__ivm_<view>_s<k>[_h<i>]`) because they are
/// checkpoint identity, and every side hop's (name, schema) is published
/// into the caller's `visible` so the spine join's rewritten right side
/// verifies against it.
fn decompose_side(
    view_name: &str,
    k: usize,
    side: &LogicalPlan,
    visible: &mut AHashMap<String, SchemaRef>,
) -> Option<(ChainSide, Vec<Hop>)> {
    side_chain_shape(side)?;
    let base = format!(
        "__ivm_{}_s{k}",
        view_name.trim_start_matches("__ivm_").to_lowercase()
    );
    let declared: SchemaRef = Arc::new(side.schema().as_arrow().clone());
    let (source, records, plans, sub_sides) = decompose_plan(&base, side, &declared, visible, 1)?;
    if !sub_sides.is_empty() {
        return None;
    }
    for r in &records {
        visible.insert(r.name.clone(), r.schema.clone());
    }
    Some((
        ChainSide {
            name: base,
            source,
            hops: plans,
        },
        records,
    ))
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
        .map(|(_, hops, _, _)| hops)
}

/// Cut `body_sql` into a single [`ViewPlan::Chain`] the flow can run in place
/// — the wiring that makes decomposition an engine capability instead of a
/// library call (DECOMP-2). The hops stay internal to the one registered view:
/// no generated view names, no separate checkpoint identities, no distributed
/// attach questions — the chain checkpoints, restores and seeds through the
/// view's own `ViewPlan` surface like every other plan.
/// A plain `async fn` since PLANHOP-1: hop verification is
/// [`build_view_plan_from_logical`], which is synchronous and never re-enters
/// the plan builder, so the call graph is no longer recursive and the boxed
/// type-erased future this used to return (the recursive concrete type made
/// the compiler's `Send` proof self-referential all the way up to the HTTP
/// handlers) has nothing left to erase.
pub(crate) async fn decompose_into_chain(
    body_sql: &str,
    declared: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    let (source, _, plans, sides) =
        decompose_core("chain", body_sql, declared, available_schemas).await?;
    Some(ViewPlan::Chain {
        source,
        hops: plans,
        sides,
    })
}

async fn decompose_core(
    view_name: &str,
    body_sql: &str,
    declared: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<CutPlan> {
    let ctx = SessionContext::new();
    for (name, schema) in available_schemas {
        let empty = arrow::array::RecordBatch::new_empty(schema.clone());
        if let Ok(t) = MemTable::try_new(schema.clone(), vec![vec![empty]]) {
            let _ = ctx.register_table(name.as_str(), Arc::new(t));
        }
    }
    let plan = crate::plan::maybe_decorrelate(ctx.sql(body_sql).await.ok()?.logical_plan().clone());
    // Two cuts minimum for a registered view: one operator is what the
    // planner already handles, so cutting buys nothing.
    //
    // The cutting engine runs on its OWN thread with a deep stack: per-hop
    // verification recurses through DataFusion's type coercion and physical
    // planning for every cut, and a wide predicate (q19's three-arm
    // disjunction over a dozen conjuncts each) blows the default worker
    // stack at the depth the flow's tick path calls this from — measured as
    // a coordinator-killing SIGABRT, not a refusal. Plan-time only, once
    // per chain build, and scoped so the borrowed inputs need no cloning.
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("ivm-decompose".into())
            .stack_size(64 * 1024 * 1024)
            .spawn_scoped(scope, || {
                decompose_plan(view_name, &plan, declared, available_schemas, 2)
            })
            .ok()?
            .join()
            .ok()?
    })
}

/// The synchronous cutting engine (SIDE-3): everything after SQL planning.
/// A SIDE that is itself a join run recurses through this same function —
/// its guard, predicate distribution, relinearization, re-rooting and
/// verification are the spine's own, under the side's name prefix. The
/// recursion is bounded by plan depth and entirely synchronous (PLANHOP-1
/// made hop verification sync), so there is no boxed-future story here.
fn decompose_plan(
    view_name: &str,
    plan: &LogicalPlan,
    declared: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    min_cuts: usize,
) -> Option<CutPlan> {
    let chain = linear_chain(plan)?;
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
                // SEMI-2: a semi/anti level's right side is a MEMBERSHIP
                // relation — never emitted, so it may be a filtered projection
                // over one source (the shape DataFusion's decorrelator
                // produces for EXISTS / IN, SEMI-1) rather than a plain scan.
                // The same resolver the join builder uses decides, so what
                // this guard admits is exactly what the hop planner accepts.
                let membership = matches!(
                    j.join_type,
                    datafusion::logical_expr::JoinType::LeftSemi
                        | datafusion::logical_expr::JoinType::LeftAnti
                );
                let right_ok = if membership {
                    crate::plan::resolve_semi_side_with_filters(&j.right).is_some()
                        || side_chain_shape(&j.right).is_some()
                } else if j.join_type == datafusion::logical_expr::JoinType::Inner {
                    // SIDE-2: an INNER level's side may also be its own
                    // sub-chain — the shape a decorrelated SCALAR aggregate
                    // takes after OUTER-1 proves the padding away. Its
                    // columns are EMITTED (minus the equi keys the operator
                    // drops), unlike a membership side's.
                    crate::plan::side_resolves_to_source(&j.right)
                        || side_chain_shape(&j.right).is_some()
                } else {
                    crate::plan::side_resolves_to_source(&j.right)
                };
                if !right_ok {
                    return None;
                }
                top_join = Some(n);
            }
        }
        // Every bare column name across the ACCUMULATED EMITTED relation
        // must be unique — hops above the joins reference them unqualified.
        // The EMITTED relation is what matters: the join operator drops each
        // level's equi-key RIGHT columns (JOIN-2 rewrites references to the
        // paired left key), so a side whose key shares a bare name with a
        // spine column (q17's side `l_partkey` vs lineitem's) is not a
        // collision — the duplicate never exists downstream. Keys named in a
        // level's ON/filter are visible here; comma-join keys live in the
        // WHERE and are not subtracted, which only leaves the check exactly
        // as strict as it was for those shapes. A membership level (semi/
        // anti) emits nothing of its right side at all.
        let _ = top_join;
        let mut seen = std::collections::HashSet::new();
        let mut first_join = true;
        for n in &chain {
            let LogicalPlan::Join(j) = n else { continue };
            if first_join {
                for (_, f) in j.left.schema().iter() {
                    if !seen.insert(f.name().clone()) {
                        return None;
                    }
                }
                first_join = false;
            }
            if matches!(
                j.join_type,
                datafusion::logical_expr::JoinType::LeftSemi
                    | datafusion::logical_expr::JoinType::LeftAnti
            ) {
                continue;
            }
            let mut right_keys: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for (_, r) in &j.on {
                if let Expr::Column(c) = r {
                    right_keys.insert(c.name.clone());
                }
            }
            if let Some(f) = &j.filter {
                for conjunct in datafusion::logical_expr::utils::split_conjunction(f) {
                    if let Expr::BinaryExpr(be) = conjunct
                        && be.op == datafusion::logical_expr::Operator::Eq
                        && let (Expr::Column(a), Expr::Column(b)) =
                            (be.left.as_ref(), be.right.as_ref())
                    {
                        let a_left = j.left.schema().index_of_column(a).is_ok();
                        let b_left = j.left.schema().index_of_column(b).is_ok();
                        match (a_left, b_left) {
                            (true, false) => {
                                right_keys.insert(b.name.clone());
                            }
                            (false, true) => {
                                right_keys.insert(a.name.clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
            for (_, f) in j.right.schema().iter() {
                if right_keys.contains(f.name()) {
                    continue;
                }
                if !seen.insert(f.name().clone()) {
                    return None;
                }
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
    // A `SubqueryAlias` between two operators RENAMES the relation the upper
    // one references (q15's `revenue0`, a `FROM auction a2`): the alias is a
    // passthrough — never a cut — but the hop scan the upper operator
    // re-roots onto must WEAR it, or every qualified reference above goes
    // unresolvable. Track, per operator, the alias that took effect below it.
    let mut op_alias_below: Vec<Option<String>> = Vec::new();
    let mut pending_alias: Option<String> = None;
    for n in &chain {
        if let LogicalPlan::SubqueryAlias(sa) = n {
            pending_alias = Some(sa.alias.table().to_string());
        }
        if !is_passthrough(n) && !is_read_time(n) {
            operator_nodes.push(n);
            op_alias_below.push(pending_alias.take());
        }
    }
    let mut cuts: Vec<LogicalPlan> = Vec::new();
    let mut cut_alias_below: Vec<Option<String>> = Vec::new();
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
        // SIDE-3 widened this from all-or-nothing to the run's maximal
        // PREFIX of pure comma levels: a scalar side join sits ATOP the run
        // with its correlation key in its own filter (q2), and gating
        // relinearization on the WHOLE run being pure left `part × supplier`
        // as a keyless cross join at the leaf — exactly the shape REORDER-1
        // exists to fix. Levels past the prefix keep their position; their
        // embedded left subtrees are stale after the splice, which is fine
        // because re-rooting replaces every level's left with a hop scan and
        // only the leaf level's own tree is ever planned as-is.
        if let Some(LogicalPlan::Filter(f)) = operator_nodes.get(next) {
            let prefix_len = join_run
                .iter()
                .take_while(
                    |n| matches!(n, LogicalPlan::Join(j) if j.on.is_empty() && j.filter.is_none()),
                )
                .count();
            if prefix_len >= 2
                && let Some(new_prefix) =
                    relinearize_join_run(join_run.get(..prefix_len)?, &f.predicate)
            {
                join_run.splice(..prefix_len, new_prefix);
            }
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
            cut_alias_below.push(None);
        }
        if !above_preds.is_empty() {
            let combined = above_preds.iter().cloned().reduce(|a, b| a.and(b))?;
            let top = join_run.last()?.clone();
            cuts.push(LogicalPlan::Filter(
                LogicalFilter::try_new(combined, Arc::new(top)).ok()?,
            ));
            cut_alias_below.push(None);
        }
    }
    // SIDE-2: fold literal group keys (the decorrelator's `Boolean(true) AS
    // __always_true` marker) out of Aggregate cuts BEFORE the computed-input
    // hoist can bury them in a projection column; later references substitute
    // the literal itself — exact, because the column IS that constant on
    // every row the aggregate emits.
    let mut lit_groups: AHashMap<String, Expr> = AHashMap::new();
    for (op_idx, n) in operator_nodes.iter().enumerate().skip(next) {
        let alias_below = op_alias_below.get(op_idx).cloned().flatten();
        let n_owned: LogicalPlan = if lit_groups.is_empty() {
            (*n).clone()
        } else {
            let exprs: Option<Vec<Expr>> = n
                .expressions()
                .iter()
                .map(|e| {
                    e.clone()
                        .transform(|node| {
                            Ok(match &node {
                                Expr::Column(c) => match lit_groups.get(&c.name) {
                                    Some(lit) => Transformed::yes(lit.clone()),
                                    None => Transformed::no(node),
                                },
                                _ => Transformed::no(node),
                            })
                        })
                        .map(|t| t.data)
                        .ok()
                })
                .collect();
            let inputs: Vec<LogicalPlan> = n.inputs().into_iter().cloned().collect();
            n.with_new_exprs(exprs?, inputs).ok()?
        };
        let n_owned = if let LogicalPlan::Aggregate(agg) = &n_owned {
            let mut kept: Vec<Expr> = Vec::new();
            for g in &agg.group_expr {
                let (inner, name) = match g {
                    Expr::Alias(a) => (a.expr.as_ref(), a.name.clone()),
                    other => (other, other.schema_name().to_string()),
                };
                if matches!(inner, Expr::Literal(_, _)) {
                    lit_groups.insert(name.clone(), inner.clone().alias(name));
                } else {
                    kept.push(g.clone());
                }
            }
            if kept.len() == agg.group_expr.len() {
                n_owned
            } else {
                LogicalPlan::Aggregate(
                    datafusion::logical_expr::Aggregate::try_new(
                        agg.input.clone(),
                        kept,
                        agg.aggr_expr.clone(),
                    )
                    .ok()?,
                )
            }
        } else {
            n_owned
        };
        match hoist_computed_inputs(&n_owned) {
            Some((hoist, rewritten)) => {
                let agg = n_owned
                    .with_new_exprs(rewritten, vec![hoist.clone()])
                    .ok()?;
                cuts.push(hoist);
                cut_alias_below.push(alias_below);
                cuts.push(agg);
                cut_alias_below.push(None);
            }
            None => {
                cuts.push(n_owned);
                cut_alias_below.push(alias_below);
            }
        }
    }
    // For a registered view (min_cuts = 2) one operator is what the planner
    // already handles; a SIDE (min_cuts = 1) always needs its own fold,
    // because its output feeds a join rather than a view.
    if cuts.len() < min_cuts {
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
    // The alias a `Leaf::Table` hop scan wears is the qualifier the hop
    // above actually references — `FROM lineitem` qualifies by the table
    // name, `FROM auction a2` by the alias — read from the previous hop's
    // own schema (its first qualified field), falling back to the source
    // name for fully-unqualified relations. Existing chains' references
    // qualify by the source name, so their hop shapes are unchanged.
    let mut prev_alias = match &leaf {
        Leaf::Table(t) => t.clone(),
        Leaf::Join => String::new(),
    };
    debug_assert_eq!(cuts.len(), cut_alias_below.len());
    let last = cuts.len() - 1;
    // TOPN-2: a bare `Sort` is a read-time property (ORDER-1) and rides the
    // final hop; a `Sort` under a `Limit` is a TOP-N — it changes which rows
    // are in the relation — so the read-time run becomes its OWN final hop
    // over the last cut, where a projection's renames (`sum(…) AS revenue`)
    // are already plain columns the top-N matcher accepts.
    let topn_final = above.iter().any(|n| matches!(n, LogicalPlan::Limit(_)));

    // SIDE-1 pre-pass: a MEMBERSHIP level (semi/anti) whose right side is not
    // a plain filtered source becomes a SIDE sub-chain — the decorrelated
    // aggregate + HAVING membership set maintained as its own fold. The join
    // cut is rewritten IN PLACE: its right input becomes a scan of the side's
    // final hop, and every reference into the old right subplan requalifies
    // to the side's name — kept qualified, because the side's bare column
    // names (`l_orderkey`) routinely also exist on the spine relation, and an
    // unqualified reference would silently bind to the wrong side of the
    // join. Any failure refuses the whole chain (the wholesale rule).
    let mut sides: Vec<ChainSide> = Vec::new();
    let mut side_records: Vec<Hop> = Vec::new();
    let mut side_names: ahash::AHashSet<String> = ahash::AHashSet::new();
    let mut visible = available_schemas.clone();
    for idx in 0..cuts.len() {
        let (pred, join) = match cuts.get(idx)? {
            LogicalPlan::Join(j) => (None, j.clone()),
            LogicalPlan::Filter(f) => match f.input.as_ref() {
                LogicalPlan::Join(j) => (Some(f.predicate.clone()), j.clone()),
                _ => continue,
            },
            _ => continue,
        };
        let membership = matches!(
            join.join_type,
            datafusion::logical_expr::JoinType::LeftSemi
                | datafusion::logical_expr::JoinType::LeftAnti
        );
        let needs_side = if membership {
            crate::plan::resolve_semi_side_with_filters(&join.right).is_none()
        } else if join.join_type == datafusion::logical_expr::JoinType::Inner {
            // SIDE-2: an emitted inner side that is not a plain source.
            !crate::plan::side_resolves_to_source(&join.right)
                && side_chain_shape(&join.right).is_some()
        } else {
            false
        };
        if !needs_side {
            continue;
        }
        // KEYLESS-1 policy: a level with no cross-side equality ANYWHERE (no
        // ON pairs, no equi in the join filter, none in its distributed
        // WHERE pred) joins its side as a single group. That is admitted
        // only when the side is a GLOBAL aggregate — one row by
        // construction, so the "cross product" is left × 1 (an uncorrelated
        // scalar comparison, UNCORR-1) — and refused for any multi-row side,
        // which would be the O(N×M) blowup the planner refuses everywhere
        // else.
        if !membership {
            let mut keyed = !join.on.is_empty();
            let scan_equi = |e: &Expr| {
                for conjunct in datafusion::logical_expr::utils::split_conjunction(e) {
                    if let Expr::BinaryExpr(be) = conjunct
                        && be.op == datafusion::logical_expr::Operator::Eq
                        && let (Expr::Column(a), Expr::Column(b)) =
                            (be.left.as_ref(), be.right.as_ref())
                    {
                        let a_left = join.left.schema().index_of_column(a).is_ok();
                        let b_left = join.left.schema().index_of_column(b).is_ok();
                        if a_left != b_left {
                            return true;
                        }
                    }
                }
                false
            };
            if let Some(f) = &join.filter {
                keyed = keyed || scan_equi(f);
            }
            if let Some(pdr) = &pred {
                keyed = keyed || scan_equi(pdr);
            }
            if !keyed && !crate::plan::is_global_aggregate(&join.right) {
                return None;
            }
        }
        let (side, records) = decompose_side(view_name, sides.len(), &join.right, &mut visible)?;
        let side_schema = visible.get(&side.name)?.clone();
        let scan = bare_hop_scan(&side.name, &side_schema)?;
        let right_quals: ahash::AHashSet<String> = join
            .right
            .schema()
            .iter()
            .filter_map(|(q, _)| q.map(|t| t.to_string()))
            .collect();
        let side_name = side.name.clone();
        let requalify = |exprs: &[Expr]| -> Option<Vec<Expr>> {
            exprs
                .iter()
                .map(|e| {
                    e.clone()
                        .transform(|node| {
                            Ok(match node {
                                Expr::Column(c)
                                    if c.relation
                                        .as_ref()
                                        .is_some_and(|r| right_quals.contains(&r.to_string())) =>
                                {
                                    Transformed::yes(Expr::Column(datafusion::common::Column::new(
                                        Some(side_name.clone()),
                                        c.name,
                                    )))
                                }
                                other => Transformed::no(other),
                            })
                        })
                        .map(|t| t.data)
                        .ok()
                })
                .collect()
        };
        let left = join.left.as_ref().clone();
        let join_plan = LogicalPlan::Join(join);
        let exprs = requalify(&join_plan.expressions())?;
        let rebuilt = join_plan.with_new_exprs(exprs, vec![left, scan]).ok()?;
        let new_cut = match pred {
            // The level's own predicate may reference the side too (q17's
            // `l_quantity < 0.2 * avg(…)` attaches HERE by the WHERE
            // distribution) — requalified the same way as the join exprs.
            Some(pdr) => {
                let pdr = requalify(std::slice::from_ref(&pdr))?.pop()?;
                LogicalPlan::Filter(LogicalFilter::try_new(pdr, Arc::new(rebuilt)).ok()?)
            }
            None => rebuilt,
        };
        *cuts.get_mut(idx)? = new_cut;
        side_names.insert(side.name.clone());
        sides.push(side);
        side_records.extend(records);
    }

    for (i, node) in cuts.iter().enumerate() {
        if let Some(a) = cut_alias_below.get(i).cloned().flatten() {
            prev_alias = a;
        }
        // Re-root onto the hop below, structurally.
        let rooted = if i == 0 {
            node.clone()
        } else {
            let prev = hops.last()?;
            match &leaf {
                Leaf::Table(_) => {
                    let scan = aliased_hop_scan(&prev.name, &prev.schema, &prev_alias)?;
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
                            let exprs = unqualify_exprs_keeping(&node.expressions(), &side_names)?;
                            node.with_new_exprs(exprs, vec![scan, j.right.as_ref().clone()])
                                .ok()?
                        }
                        LogicalPlan::Filter(f)
                            if matches!(f.input.as_ref(), LogicalPlan::Join(_)) =>
                        {
                            let LogicalPlan::Join(j) = f.input.as_ref() else {
                                return None;
                            };
                            let join_exprs =
                                unqualify_exprs_keeping(&f.input.expressions(), &side_names)?;
                            let new_join = f
                                .input
                                .as_ref()
                                .clone()
                                .with_new_exprs(join_exprs, vec![scan, j.right.as_ref().clone()])
                                .ok()?;
                            let pred = unqualify_exprs_keeping(
                                std::slice::from_ref(&f.predicate),
                                &side_names,
                            )?
                            .pop()?;
                            LogicalPlan::Filter(
                                LogicalFilter::try_new(pred, Arc::new(new_join)).ok()?,
                            )
                        }
                        _ => {
                            let mut exprs =
                                unqualify_exprs_keeping(&node.expressions(), &side_names)?;
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
                    Leaf::Join => unqualify_exprs_keeping(&r.expressions(), &side_names)?,
                    Leaf::Table(_) => r.expressions(),
                };
                top = r.with_new_exprs(exprs, vec![top]).ok()?;
            }
            (view_name.to_string(), top)
        } else {
            let body = if needs_projection(&rooted) {
                explicit_projection_side_aware(&rooted, &side_names)?
            } else {
                rooted
            };
            (hop_name(view_name, i), body)
        };

        let schema: SchemaRef = if i == last && !topn_final {
            declared.clone()
        } else {
            Arc::new(target.schema().as_arrow().clone())
        };
        prev_alias = target
            .schema()
            .iter()
            .find_map(|(q, _)| q.map(|t| t.table().to_string()))
            .unwrap_or(prev_alias);

        // Per-hop verification against the schemas the flow will actually
        // have, on the re-rooted plan ITSELF (PLANHOP-1) — not on an unparse
        // + replan + re-decorrelate round trip, which reconstructs a
        // different object than the one the cutting logic reasoned about
        // (for a semi hop the old path only worked because the unparser
        // renders it as EXISTS and the decorrelator then ran a SECOND time
        // inside verification). Anything short of a real O(delta) plan
        // discards the whole chain — and the verified plan IS the hop's
        // operator, kept rather than rebuilt, so what was checked is what
        // runs.
        let plan = build_view_plan_from_logical(&target, &schema, &visible, &[]);
        if plan.kind() != ViewPlanKind::Incremental {
            return None;
        }
        let sql = datafusion::sql::unparser::plan_to_sql(&target)
            .ok()
            .map(|s| s.to_string());
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
                Leaf::Join => unqualify_exprs_keeping(&r.expressions(), &side_names)?,
                Leaf::Table(_) => r.expressions(),
            };
            top = r.with_new_exprs(exprs, vec![top]).ok()?;
        }
        let plan = build_view_plan_from_logical(&top, declared, &visible, &[]);
        if plan.kind() != ViewPlanKind::Incremental {
            return None;
        }
        let sql = datafusion::sql::unparser::plan_to_sql(&top)
            .ok()
            .map(|s| s.to_string());
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
    // Side hops precede the spine in the public hop list — topological order,
    // so a caller registering hops as standalone views registers each side
    // before the join that reads it.
    let mut all_hops = side_records;
    all_hops.extend(hops);
    Some((source, all_hops, plans, sides))
}
