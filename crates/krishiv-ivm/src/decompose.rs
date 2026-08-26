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
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
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
            // DECOMP-4: a two-source Join ends the walk as the chain's leaf.
            // Whether it is actually maintainable (equi keys, INNER, both
            // sides plain sources) is the hop planner's decision, made when
            // the leaf hop is verified like every other hop.
            LogicalPlan::Join(_) => {
                chain.push(node);
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
    if matches!(leaf, Leaf::Join)
        && let Some(join_node) = chain.first()
    {
        // Both sides must be plain sources — the hop planner's own acceptance
        // rule, checked HERE so a multi-way join (a side that is itself a
        // join) refuses before the giant unparse + replan that would only be
        // refused afterwards anyway (and whose planner recursion overflows a
        // default thread stack on a five-way comma join).
        if let LogicalPlan::Join(j) = join_node
            && !(crate::plan::side_resolves_to_source(&j.left)
                && crate::plan::side_resolves_to_source(&j.right))
        {
            return None;
        }
        let mut seen = std::collections::HashSet::new();
        for (_, f) in join_node.schema().iter() {
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
        let join_node = *operator_nodes.first()?;
        next = 1;
        match operator_nodes.get(1) {
            Some(LogicalPlan::Filter(_)) => {
                cuts.push((*operator_nodes.get(1)?).clone());
                next = 2;
            }
            _ => cuts.push(join_node.clone()),
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
                // above), so references unqualify — one relation, so a bare
                // name is unambiguous (the ALIAS-1 rule).
                Leaf::Join => {
                    let scan = bare_hop_scan(&prev.name, &prev.schema)?;
                    let exprs = unqualify_exprs(&node.expressions())?;
                    node.with_new_exprs(exprs, vec![scan]).ok()?
                }
            }
        };

        let (name, target) = if i == last {
            // Re-apply ORDER BY / LIMIT so the final view keeps them.
            let mut top = rooted;
            for r in &above {
                top = r.with_new_exprs(r.expressions(), vec![top]).ok()?;
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
        let schema: SchemaRef = if i == last {
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
