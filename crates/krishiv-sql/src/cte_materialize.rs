//! Materialise a CTE that is referenced more than once.
//!
//! # The gap this closes
//!
//! DataFusion **inlines** every CTE. A `WITH x AS (…)` referenced three times is
//! planned as three copies of its body and executed three times. DuckDB and
//! PostgreSQL materialise a CTE referenced more than once, which is why the
//! same query text costs one scan there and N here.
//!
//! It dominates what is left of the TPC-DS tail. Counting `DataSourceExec`
//! nodes per table in `EXPLAIN ANALYZE` at SF1, after the join-reorder fix:
//!
//! ```text
//!   q23  store_sales x6   5.45 s of 7.25 s scan CPU is duplicate
//!   q14  store_sales x5, catalog_sales x5, web_sales x5      3.30 s duplicate
//!   q27  store_sales x3                                      2.86 s duplicate
//!   q64  store_sales x2, catalog_sales x2                    2.17 s duplicate
//!   q4   store_sales x2, web_sales x2                        1.66 s duplicate
//!   q47  store_sales x3                                      1.47 s duplicate
//! ```
//!
//! Materialising by hand — lifting the CTE body into `CREATE TABLE … AS` and
//! pointing the query at the table — with byte-identical results:
//!
//! ```text
//!   q27  414 ms -> 195 ms   2.12x   (DuckDB 78 ms)
//!   q23  631 ms -> 353 ms   1.79x   (DuckDB 253 ms)
//! ```
//!
//! # On by default, and the four sweeps behind that
//!
//! All 99 TPC-DS queries at SF1, warm, best of three, paired and interleaved,
//! results hashed per query. **99/99 rows identical in every sweep.**
//!
//! ```text
//!   1. any repeated alias           16177 -> 18224 ms   0.888x    5 wins  18 losses
//!   2. + body must reduce input     31561 -> 33365 ms   0.946x   11 wins  20 losses
//!   3. + consumers must not filter,
//!      + schema check fixed         16448 -> 16036 ms   1.026x   11 wins   8 losses
//!   4. + subquery expressions,
//!      + filters traced to the body 16628 -> 14388 ms   1.156x   13 wins  10 losses
//! ```
//!
//! (Sweep 2 ran on a machine loaded by an unrelated build, hence the doubled
//! absolutes; the arms interleave, so the ratio holds.) In sweep 4, on a quiet
//! machine, the largest loss is 32 ms (q88, 0.88x) and every loss is under 15%;
//! the wins are q95 4.11x, q14 3.57x, q57 2.19x, q36 2.05x, q27 1.92x, q23
//! 1.79x, q47 1.75x. Against DuckDB the suite went 16498 -> 14891 ms and q95
//! joined q72 as faster than it.
//!
//! What each step found:
//!
//! - **Caching a bare scan forfeits pushdown.** Sweep 1 cached anything
//!   repeated, including `SubqueryAlias: ss1` over an unfiltered `store_sales`
//!   in q44 — 2.88 M rows, all columns, where each consumer would otherwise have
//!   pushed `ss_store_sk = 4` into the scan. 111 -> 428 ms. [`reduces_its_input`].
//!
//! - **Consumers that filter the alias want their own copies.** `WITH cs AS (…)
//!   … WHERE cs1.syear = 2000 AND cs2.syear = 2001` gives each inlined copy its
//!   own year through `PushDownFilter`; the shared copy computes both. q64 0.63x,
//!   q39 0.38x, q2 0.58x in sweep 2, all gone in sweep 3. [`consumers_filter`].
//!
//! - **The schema check was declining most real CTEs silently.** The rewrite
//!   compared the replacement's `DFSchema` to the candidate's for equality, and
//!   `DFSchema` equality includes functional dependencies — which a `GROUP BY`
//!   body has and a `MemTable` scan does not. Every aggregate-bearing CTE was
//!   declined without a trace; q27 fired in sweeps 1–2 only because its body has
//!   no `GROUP BY`. Sweeps 1 and 2 therefore measured a rule that mostly did not
//!   run. Found by a probe that printed `cached=0` for an unfiltered, repeated,
//!   aggregate-bearing CTE; fixed by comparing fields and qualifiers.
//!
//! What sweep 4 added, each from a query the rule had declined or mishandled:
//! references inside `EXISTS`/`IN`/scalar subqueries are found and rewritten
//! (`apply_with_subqueries`; q95, q14); a predicate is traced through alias
//! bodies to the candidate rather than matched one alias up (q39 had cached the
//! derived table *inside* the CTE its consumers filtered, 0.51x); and a conjunct
//! counts only when every column it names traces to the candidate through one
//! alias and is pushable there — so a correlation, a join predicate, a self-join
//! and a filter above a window no longer block (q95, q47). See
//! [`consumers_filter`].
//!
//! Still unmodelled: a filter that reaches the alias through a join with a
//! filtered dimension is invisible to [`consumers_filter`]; sweep 4 says what is
//! left of that is inside noise at SF1. **SF100 and the distributed path are
//! unmeasured** — the rule declines outside a single-query process regardless.
//! `KRISHIV_CTE_MATERIALIZE=off` disables it.
//!
//! # Why this runs before the optimizer, and must
//!
//! The N copies are identical in the **unoptimized** plan and stop being
//! identical the moment `OptimizeProjections` runs, because it prunes each copy
//! to the columns *its own* consumer needs:
//!
//! ```text
//!   unoptimized            optimized
//!   SubqueryAlias: r       SubqueryAlias: r          SubqueryAlias: r
//!     Projection: a, b       Filter: b > 5             Projection:
//!       Filter: b > 5          TableScan [b]             Filter: b > 5
//!         TableScan                                        TableScan [b]
//! ```
//!
//! Matching on the optimized plan finds nothing. So this operates on the plan
//! `SessionContext::sql` hands back, before optimization.
//!
//! The cost of that ordering is the honest trade every materialising engine
//! makes: the cached copy carries every column any consumer needs, so per-copy
//! projection pruning is lost. Both measurements above already include it.
//!
//! # What it will not touch
//!
//! - **Anything but a single-query process.** A custom-shaped plan that reaches
//!   the coordinator has to encode, and the scheduler's response to a stage plan
//!   it cannot encode is to run the query as one task — the mechanism
//!   `spillable_join` documents. The rewrite emits only standard nodes over a
//!   `MemTable`, but the eager execution it performs has no place in a process
//!   that is planning stages rather than running them.
//! - **Engines with streaming sources.** Collecting an unbounded input does not
//!   terminate.
//! - **A body that does not reduce its input.** See [`reduces_its_input`].
//! - **An alias reachable only through a subquery expression.** The search walks
//!   `LogicalPlan::inputs`, which does not descend into scalar or `EXISTS`
//!   subqueries, so repeats inside them are not found. Conservative: it does
//!   less, never something wrong.
//! - **Anything over the cap.** Collection stops at
//!   [`MAX_MATERIALIZED_ROWS`]/[`MAX_MATERIALIZED_BYTES`] and the original plan
//!   runs unchanged, so a CTE too large to hold degrades to today's behaviour
//!   rather than to an out-of-memory.

use crate::{SqlError, SqlResult};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{LogicalPlan, SubqueryAlias};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Environment switch for CTE materialisation. See the module docs for the
/// three sweeps behind the default.
///
/// The rewrite executes part of the query eagerly inside a call whose contract
/// is lazy. That is confined to a single-query process — the CLI and the
/// embedded engine, where `sql()` is followed by a collect — and never reaches
/// the coordinator or an executor.
pub const CTE_MATERIALIZE_ENV: &str = "KRISHIV_CTE_MATERIALIZE";

/// Rows past which a CTE is left inlined.
pub const MAX_MATERIALIZED_ROWS: usize = 20_000_000;

/// Bytes past which a CTE is left inlined.
pub const MAX_MATERIALIZED_BYTES: usize = 2 << 30;

/// Most CTEs materialised for one statement.
///
/// Each pass materialises the largest remaining repeat, so nested and sibling
/// CTEs need more than one. TPC-DS q23, the deepest in the suite, has three.
const MAX_PASSES: usize = 4;

/// Prefix for the temporary tables this module registers.
const TEMP_PREFIX: &str = "__krishiv_cte_";

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// Whether CTE materialisation is enabled (default: **yes**). Opt-out parsing,
/// matching `join_reorder_enabled`.
pub fn cte_materialize_enabled() -> bool {
    !matches!(
        std::env::var(CTE_MATERIALIZE_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false" | "no"
    )
}

/// Replace every repeated `SubqueryAlias` in `dataframe`'s plan with a scan of
/// its collected result.
///
/// Returns the dataframe unchanged whenever the rewrite does not apply, so a
/// caller can wrap this around the plan unconditionally.
pub async fn materialize_repeated_ctes(
    context: &SessionContext,
    dataframe: DataFrame,
) -> SqlResult<DataFrame> {
    if !cte_materialize_enabled() || !krishiv_common::executor_capacity::is_single_query_process() {
        return Ok(dataframe);
    }
    materialize_repeated_ctes_forced(context, dataframe).await
}

/// As [`materialize_repeated_ctes`], with the env gate and the process check
/// bypassed — for tests and explicit opt-in, matching the `forced()`
/// constructors on this crate's optimizer rules. Reading the switch inside the
/// rule would make every test that exercises it race every other test in the
/// process over one environment variable.
pub async fn materialize_repeated_ctes_forced(
    context: &SessionContext,
    dataframe: DataFrame,
) -> SqlResult<DataFrame> {
    // Grouping sets first: the rewrite emits the finest aggregate once per set
    // as one repeated alias, which is exactly what the loop below caches. See
    // `rollup_rewrite` for why the two are coupled.
    let (mut plan, mut materialized) =
        match crate::rollup_rewrite::rewrite_grouping_sets(dataframe.logical_plan())? {
            Some(rewritten) => (rewritten, true),
            None => (dataframe.logical_plan().clone(), false),
        };
    for _ in 0..MAX_PASSES {
        let Some(candidate) = largest_repeated_alias(&plan) else {
            break;
        };
        let Some(rewritten) = materialize_one(context, &plan, &candidate).await? else {
            break;
        };
        plan = rewritten;
        materialized = true;
    }
    if !materialized {
        return Ok(dataframe);
    }
    context
        .execute_logical_plan(plan)
        .await
        .map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: {error}"),
        })
}

/// The repeated `SubqueryAlias` with the most nodes beneath it.
///
/// Largest first because materialising the outermost repeat subsumes every
/// repeat inside it; picking the smallest would cache a leaf and leave the
/// expensive joins above it duplicated.
fn largest_repeated_alias(plan: &LogicalPlan) -> Option<SubqueryAlias> {
    // `apply_with_subqueries`, not `inputs()`: a CTE referenced from an
    // `EXISTS` or a scalar subquery lives in a *expression*, and `inputs()`
    // never sees it. TPC-DS q95's `ws_wh` is joined once and probed by two
    // `EXISTS`; q14's `avg_sales` is a scalar subquery in three `HAVING`s.
    let mut counts: HashMap<SubqueryAlias, usize> = HashMap::new();
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node {
            *counts.entry(alias.clone()).or_insert(0) += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    counts
        .into_iter()
        .filter(|(alias, count)| {
            *count >= 2
                && reduces_its_input(&alias.input)
                && !already_materialized(&alias.input)
                && !consumers_filter(plan, alias)
        })
        .max_by_key(|(alias, _)| node_count(&alias.input))
        .map(|(alias, _)| alias)
}

/// Does any consumer filter the alias in a way its own inlined copy could
/// have pushed into the body?
///
/// This is the q64 signature: `WITH cs AS (…) … WHERE cs1.syear = 2000 AND
/// cs2.syear = 2001`. Each inlined copy receives its predicate through
/// `PushDownFilter` and computes only that year; the shared copy has to compute
/// both. When the predicates are selective, N cheap copies beat one expensive
/// one — q64 0.63x, q39 0.38x, q2 0.58x with this check absent. Where no
/// predicate names the alias, pushdown had nothing to give the copies and the
/// shared copy is pure saving — q27 2.1x.
///
/// A conjunct counts only when **every column it references traces to the
/// candidate through one alias**, and at least one of them could be pushed
/// beneath the body's top operator. That excludes, deliberately:
///
/// - **A correlation.** `EXISTS (SELECT 1 FROM r WHERE r.k = outer_ref(s.k))`
///   becomes a join key when decorrelated; the copy still computes all of `r`.
/// - **A join predicate.** `wr_order_number = ws_wh.ws_order_number` names the
///   alias and another relation; the copy is one side of a join and computes
///   all of itself. q95's `ws_wh` is consumed exactly so, three times — 3.55x
///   by hand, declined by a guard that counted this.
/// - **A self-join.** `inv1.i_item_sk = inv2.i_item_sk` names the candidate
///   twice through two aliases; neither copy computes less for it.
/// - **One the copy could not push.** `WITH v1 AS (… rank() OVER (…) …) …
///   WHERE v1.d_year = 1999`: no pushdown through a window except on partition
///   columns, nor through an aggregate except on group keys, so each copy
///   computes the whole body regardless. q47, 2.04x by hand.
///
/// Columns are **traced through alias bodies**, not matched by name one level
/// up. q39's `WITH inv AS (SELECT … FROM (SELECT … GROUP BY …, d_moy) foo
/// WHERE …) … FROM inv inv1, inv inv2 WHERE inv1.d_moy = 1`: the candidate the
/// rule reaches is `foo` — `inv` is declined for this very predicate — and
/// `inv1.d_moy` is `foo.d_moy` two projections down, a group key the copy
/// pushes into. Matching one level up let `foo` be cached whole, all twelve
/// months for consumers that each wanted one: q39 0.51x.
fn consumers_filter(plan: &LogicalPlan, candidate: &SubqueryAlias) -> bool {
    use datafusion::logical_expr::utils::split_conjunction;
    let mut aliases: HashMap<String, SubqueryAlias> = HashMap::new();
    let mut predicates: Vec<Expr> = Vec::new();
    let _ = plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::SubqueryAlias(alias) => {
                aliases
                    .entry(alias.alias.table().to_owned())
                    .or_insert_with(|| alias.clone());
            }
            LogicalPlan::Filter(filter) => predicates.push(filter.predicate.clone()),
            LogicalPlan::Join(join) => predicates.extend(join.filter.iter().cloned()),
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
    predicates
        .iter()
        .flat_map(|expr| split_conjunction(expr))
        .filter(|conjunct| !conjunct.contains_outer())
        .any(|conjunct| {
            let refs = conjunct.column_refs();
            if refs.is_empty() {
                return false;
            }
            let mut through: Option<&str> = None;
            let mut pushable = false;
            for column in refs {
                let Some(relation) = column.relation.as_ref() else {
                    return false;
                };
                let Some(alias) = aliases.get(relation.table()) else {
                    return false;
                };
                let Some(traced) = trace_to(alias, candidate, &column.name) else {
                    return false;
                };
                match through {
                    None => through = Some(relation.table()),
                    Some(name) if name == relation.table() => {}
                    Some(_) => return false,
                }
                pushable |= pushable_into(&candidate.input, &traced);
            }
            pushable
        })
}

/// Follow output column `name` of `alias` down through row-preserving nodes
/// and projections until it reaches `candidate`; the column's name there.
///
/// `None` when the column is computed on the way (an aggregate, a window,
/// arithmetic), when a node that changes the row set is crossed before the
/// candidate is reached, or when the chain bottoms out elsewhere.
fn trace_to(alias: &SubqueryAlias, candidate: &SubqueryAlias, name: &str) -> Option<String> {
    if alias == candidate {
        return Some(name.to_owned());
    }
    let mut node = alias.input.as_ref();
    let mut name = name.to_owned();
    loop {
        match node {
            LogicalPlan::SubqueryAlias(inner) => {
                if inner == candidate {
                    return Some(name);
                }
                node = inner.input.as_ref();
            }
            LogicalPlan::Projection(projection) => {
                let expr = projection.expr.iter().find(|expr| match expr {
                    Expr::Column(column) => column.name == name,
                    Expr::Alias(alias) => alias.name == name,
                    other => other.schema_name().to_string() == name,
                })?;
                name = match expr {
                    Expr::Column(column) => column.name.clone(),
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        Expr::Column(column) => column.name.clone(),
                        _ => return None,
                    },
                    _ => return None,
                };
                node = projection.input.as_ref();
            }
            LogicalPlan::Filter(filter) => node = filter.input.as_ref(),
            LogicalPlan::Sort(sort) => node = sort.input.as_ref(),
            LogicalPlan::Limit(limit) => node = limit.input.as_ref(),
            LogicalPlan::Distinct(distinct) => node = distinct.input(),
            _ => return None,
        }
    }
}

/// Could a predicate on the body's output column `name` be pushed beneath the
/// body's top operator, so that an inlined copy computes less?
///
/// Through a projection the column is followed to what produced it: a bare
/// column keeps its name, anything computed (an aggregate, a window, an
/// arithmetic) is not pushable. Beneath, a `Window` admits only its partition
/// columns and an `Aggregate` only its group keys — the same rule
/// `PushDownFilter` applies. Everything else lets the predicate through.
fn pushable_into(body: &LogicalPlan, name: &str) -> bool {
    match body {
        LogicalPlan::Projection(projection) => {
            let Some(expr) = projection.expr.iter().find(|expr| match expr {
                Expr::Column(column) => column.name == name,
                Expr::Alias(alias) => alias.name == name,
                other => other.schema_name().to_string() == name,
            }) else {
                return false;
            };
            match expr {
                Expr::Column(column) => pushable_into(&projection.input, &column.name),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::Column(column) => pushable_into(&projection.input, &column.name),
                    _ => false,
                },
                _ => false,
            }
        }
        LogicalPlan::SubqueryAlias(alias) => pushable_into(&alias.input, name),
        LogicalPlan::Aggregate(aggregate) => aggregate
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::Column(c) if c.name == name)),
        LogicalPlan::Window(window) => window.window_expr.iter().all(|expr| {
            let Expr::WindowFunction(function) = expr else {
                return false;
            };
            function
                .params
                .partition_by
                .iter()
                .any(|expr| matches!(expr, Expr::Column(c) if c.name == name))
        }),
        _ => true,
    }
}

/// Does this body actually shrink what it reads?
///
/// A body that is only scans, projections and aliases is the worst thing to
/// cache. Re-reading it is nearly free — the pages are already warm — while
/// caching it *forfeits* what makes each inlined copy cheap: the consumer's
/// predicate can no longer be pushed into the scan, and the columns the
/// consumer does not want can no longer be pruned from it.
///
/// TPC-DS q44 is the case that proved it. It has no `WITH` clause at all; the
/// repeated alias is `ss1` over a bare `store_sales` scan in two branches that
/// each filter `ss_store_sk = 4`. Materialising it cached **2.88 M rows**, all
/// columns, and ran 3.9x slower (111 ms -> 428 ms). Requiring a reducing node
/// is what separates that from q27's aggregate-bearing body, which is 1.9x
/// faster cached.
fn reduces_its_input(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Window(_) => true,
        // Row-preserving wrappers: ask what is underneath.
        LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Union(_) => plan.inputs().iter().any(|child| reduces_its_input(child)),
        // A bare scan, and anything unmodelled, is not worth caching.
        _ => false,
    }
}

/// Has this alias already been rewritten by an earlier pass?
///
/// Without this the loop would re-materialise its own temporary table on every
/// pass, forever, because the rewritten node is still a repeated `SubqueryAlias`
/// over a `TableScan`.
fn already_materialized(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::TableScan(scan)
        if scan.table_name.table().starts_with(TEMP_PREFIX))
}

fn node_count(plan: &LogicalPlan) -> usize {
    1 + plan
        .inputs()
        .iter()
        .map(|child| node_count(child))
        .sum::<usize>()
}

/// Collect `candidate`'s body, register it, and point every occurrence at it.
///
/// `Ok(None)` means "leave the plan alone": the body exceeded the cap, or the
/// rewrite would not have reproduced the original schema.
async fn materialize_one(
    context: &SessionContext,
    plan: &LogicalPlan,
    candidate: &SubqueryAlias,
) -> SqlResult<Option<LogicalPlan>> {
    let body = DataFrame::new(context.state(), candidate.input.as_ref().clone());
    let schema = Arc::new(body.schema().as_arrow().clone());
    let Some(batches) =
        collect_within_cap(body, MAX_MATERIALIZED_ROWS, MAX_MATERIALIZED_BYTES).await?
    else {
        return Ok(None);
    };
    // Spread the batches over `target_partitions` partitions.
    //
    // A `MemTable` built as `vec![batches]` has ONE partition, and every
    // operator above it then runs single-threaded — which is why the first
    // version of this rule was 12.6% *slower* across the 99 queries (q44 0.26x,
    // q39 0.33x, q64 0.60x) while the same materialisation done by hand through
    // `CREATE TABLE AS` was 1.8x faster. Caching the rows is the win; caching
    // them into one partition gave the win back and more.
    let table =
        MemTable::try_new(Arc::clone(&schema), partition(batches, context)).map_err(|error| {
            SqlError::DataFusion {
                message: format!("CTE materialisation: {error}"),
            }
        })?;
    let name = format!("{TEMP_PREFIX}{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
    context
        .register_table(&name, Arc::new(table))
        .map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: register '{name}': {error}"),
        })?;

    // The replacement keeps the original alias, so every column above it
    // resolves by the same qualified name it did before.
    let scan = context
        .table(&name)
        .await
        .map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: read '{name}': {error}"),
        })?
        .into_unoptimized_plan();
    let replacement = SubqueryAlias::try_new(Arc::new(scan), candidate.alias.clone())
        .map(LogicalPlan::SubqueryAlias)
        .map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: alias: {error}"),
        })?;
    // Field-level equality, not `DFSchema` equality. A body with a `GROUP BY`
    // carries a functional dependency in its schema that a `MemTable` scan does
    // not, and whole-schema equality declined every such CTE silently — which
    // is most of them. Losing the dependency costs the optimizer above an
    // opportunity, never a row.
    if replacement.schema().fields() != candidate.schema.fields()
        || replacement
            .schema()
            .iter()
            .map(|(qualifier, _)| qualifier)
            .ne(candidate.schema.iter().map(|(qualifier, _)| qualifier))
    {
        return Ok(None);
    }

    let rewritten = plan
        .clone()
        .transform_down_with_subqueries(|node| {
            Ok(match &node {
                LogicalPlan::SubqueryAlias(alias) if alias == candidate => {
                    Transformed::yes(replacement.clone())
                }
                _ => Transformed::no(node),
            })
        })
        .map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: rewrite: {error}"),
        })?;
    Ok(Some(rewritten.data))
}

/// Deal `batches` round-robin into as many partitions as the session will use.
///
/// Round-robin rather than contiguous chunks so an ordered input does not put
/// every large batch in one partition; the cached relation carries no ordering
/// guarantee, so the deal is free.
fn partition(
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
    context: &SessionContext,
) -> Vec<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    let target = context
        .copied_config()
        .options()
        .execution
        .target_partitions
        .max(1)
        .min(batches.len().max(1));
    let mut out = vec![Vec::new(); target];
    for (index, batch) in batches.into_iter().enumerate() {
        if let Some(slot) = out.get_mut(index % target) {
            slot.push(batch);
        }
    }
    out
}

/// Collect every batch, or `None` if the result outgrows the cap.
///
/// Streamed rather than `DataFrame::collect` so a CTE far larger than the cap
/// is abandoned after the cap's worth of memory rather than after all of it.
async fn collect_within_cap(
    frame: DataFrame,
    max_rows: usize,
    max_bytes: usize,
) -> SqlResult<Option<Vec<datafusion::arrow::record_batch::RecordBatch>>> {
    let mut stream = frame
        .execute_stream()
        .await
        .map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: {error}"),
        })?;
    let mut batches = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| SqlError::DataFusion {
            message: format!("CTE materialisation: {error}"),
        })?;
        rows += batch.num_rows();
        bytes += batch.get_array_memory_size();
        if rows > max_rows || bytes > max_bytes {
            return Ok(None);
        }
        batches.push(batch);
    }
    Ok(Some(batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    /// q27's shape: one CTE over a fact table, consumed by two `UNION ALL`
    /// branches that need different columns of it.
    const TWICE: &str = "WITH results AS \
        (SELECT ss_item_sk, ss_quantity FROM store_sales WHERE ss_quantity > 5) \
        SELECT * FROM (SELECT sum(ss_quantity) s FROM results \
                       UNION ALL SELECT count(*) s FROM results) t";

    fn context() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ss_item_sk", DataType::Int64, false),
            Field::new("ss_quantity", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![1, 10, 20, 30])),
            ],
        )
        .expect("batch");
        let ctx = SessionContext::new();
        ctx.register_table(
            "store_sales",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table")),
        )
        .expect("register");
        ctx
    }

    /// Cache reads in a rendered plan — scans only, not the qualified column
    /// references the optimizer prints above them.
    fn cached_scans(plan: &str) -> usize {
        plan.matches(&format!("TableScan: {TEMP_PREFIX}")).count()
    }

    async fn optimized(ctx: &SessionContext, frame: DataFrame) -> String {
        let _ = ctx;
        format!(
            "{}",
            frame
                .into_optimized_plan()
                .expect("optimize")
                .display_indent()
        )
    }

    /// The fact table must not be scanned by the returned plan at all.
    ///
    /// The body is collected once, eagerly, so the plan that remains reads the
    /// cache from both references and `store_sales` is gone from it. Both halves
    /// are asserted: a plan with no `store_sales` but only one cache reference
    /// would mean the rewrite had dropped a branch, and a plan with two cache
    /// references but a surviving `store_sales` would mean it had materialised
    /// one reference and left the other inlined.
    #[tokio::test]
    async fn a_cte_referenced_twice_reads_a_cache_from_both_references() {
        let ctx = context();
        let frame = ctx.sql(TWICE).await.expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            plan.matches("TableScan: store_sales").count(),
            0,
            "the CTE body is collected eagerly, so no scan of it should remain:\n{plan}"
        );
        assert_eq!(
            cached_scans(&plan),
            2,
            "both references must read the cache:\n{plan}"
        );
    }

    /// Without the rewrite DataFusion inlines the CTE. Pins what is changed.
    #[tokio::test]
    async fn datafusion_inlines_a_cte_once_per_reference() {
        let ctx = context();
        let frame = ctx.sql(TWICE).await.expect("plan");
        let plan = optimized(&ctx, frame).await;
        assert_eq!(
            plan.matches("TableScan: store_sales").count(),
            2,
            "DataFusion 54 inlines every CTE reference:\n{plan}"
        );
    }

    /// Materialising must not change the answer.
    #[tokio::test]
    async fn the_materialized_plan_returns_the_same_rows() {
        let ctx = context();
        let baseline = rows(ctx.sql(TWICE).await.expect("plan")).await;
        let ctx = context();
        let frame = ctx.sql(TWICE).await.expect("plan");
        let rewritten = rows(
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(rewritten, baseline, "materialising changed the answer");
        assert!(!baseline.is_empty(), "the fixture must produce rows");
    }

    /// A repeated alias over a bare scan must be left inlined.
    ///
    /// TPC-DS q44's repeated alias is `ss1` over an unfiltered `store_sales`,
    /// consumed by two branches that each filter `ss_store_sk = 4`. Caching it
    /// held 2.88 M rows and forfeited the pushdown that made each inlined copy
    /// cheap: 111 ms -> 428 ms, 3.9x slower.
    #[tokio::test]
    async fn a_repeated_alias_over_a_bare_scan_is_left_alone() {
        let ctx = context();
        let frame = ctx
            .sql(
                "SELECT a.n, b.n FROM \
                   (SELECT count(*) n FROM (SELECT * FROM store_sales) ss1 \
                    WHERE ss1.ss_item_sk = 1) a, \
                   (SELECT count(*) n FROM (SELECT * FROM store_sales) ss1 \
                    WHERE ss1.ss_item_sk = 2) b",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            0,
            "an unfiltered scan must not be cached — the consumers' predicates \
             push into it and the cache would forfeit that:\n{plan}"
        );
    }

    /// A CTE whose body aggregates must be cached like any other.
    ///
    /// The first version of this module compared schemas with `DFSchema`
    /// equality, which includes the functional dependency a `GROUP BY` body
    /// carries and a `MemTable` scan does not — so every aggregate-bearing CTE
    /// was declined silently, and the positive fixture above, having no
    /// `GROUP BY`, could not tell. This one can.
    #[tokio::test]
    async fn a_cte_whose_body_aggregates_is_still_cached() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH r AS (SELECT ss_item_sk, sum(ss_quantity) q FROM store_sales GROUP BY ss_item_sk) \
                 SELECT a.q, b.q FROM r a, r b",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            2,
            "a GROUP BY body carries a functional dependency the cache lacks; \
             that must not decline the rewrite:\n{plan}"
        );
    }

    /// A CTE probed from `EXISTS` subqueries must be found and cached.
    ///
    /// q95's `ws_wh` is joined once and probed by two `EXISTS`; the references
    /// live in filter *expressions*, which `LogicalPlan::inputs` never visits,
    /// and their `r.k = outer_ref(…)` correlation is a join key after
    /// decorrelation, not a filter the copy could push. 3.55x by hand.
    #[tokio::test]
    async fn a_cte_probed_from_exists_subqueries_is_cached() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH r AS (SELECT ss_item_sk k, sum(ss_quantity) q FROM store_sales GROUP BY ss_item_sk) \
                 SELECT count(*) FROM store_sales s \
                 WHERE EXISTS (SELECT 1 FROM r WHERE r.k = s.ss_item_sk) \
                   AND NOT EXISTS (SELECT 1 FROM r WHERE r.q = s.ss_quantity)",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            2,
            "both EXISTS probes must read the cache; the correlation is not a \
             consumer filter:\n{plan}"
        );
    }

    /// A consumer filter that could not push through the body does not block.
    ///
    /// q47's `v1` ends in `rank() OVER (PARTITION BY i_category …)` and its
    /// consumers filter `v1.d_year = 1999`; `d_year` is not a partition column,
    /// so each inlined copy computes the whole window anyway. 2.04x by hand.
    #[tokio::test]
    async fn a_filter_the_copy_could_not_push_does_not_block_caching() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH r AS (SELECT ss_item_sk, ss_quantity, \
                            rank() OVER (PARTITION BY ss_item_sk ORDER BY ss_quantity) rn \
                            FROM store_sales) \
                 SELECT a.rn, b.rn FROM r a, r b WHERE a.ss_quantity = 10 AND b.ss_quantity = 20",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            2,
            "ss_quantity is not a partition column, so the filter cannot push \
             below the window and the copies gain nothing from it:\n{plan}"
        );
    }

    /// A filter that reaches the candidate through an enclosing alias counts.
    ///
    /// q39: the rule reaches `foo`, the derived table inside `inv`, because
    /// `inv` itself is declined for `inv1.d_moy = 1`. That same predicate is
    /// `foo.d_moy` two projections down — a group key the copy pushes into —
    /// and matching one level up cached all twelve months for consumers that
    /// each wanted one: 0.51x.
    #[tokio::test]
    async fn a_filter_reaching_the_candidate_through_an_outer_alias_counts() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH inv AS (SELECT k, q, q * 2 AS q2 FROM \
                    (SELECT ss_item_sk k, sum(ss_quantity) q FROM store_sales GROUP BY ss_item_sk) foo \
                  WHERE q > 0) \
                 SELECT a.q2, b.q2 FROM inv a, inv b WHERE a.k = 1 AND b.k = 2 AND a.q = b.q",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            0,
            "`a.k = 1` is `foo.k` beneath `inv`, a group key each copy pushes \
             into; neither `inv` nor `foo` may be cached:\n{plan}"
        );
    }

    /// A join predicate naming the alias is not a consumer filter.
    ///
    /// q95 consumes `ws_wh` through `IN (SELECT wr_order_number FROM web_returns,
    /// ws_wh WHERE wr_order_number = ws_wh.ws_order_number)`; the copy is one
    /// side of that join and computes all of itself. 3.55x by hand.
    #[tokio::test]
    async fn a_join_predicate_on_the_alias_does_not_block_caching() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH r AS (SELECT ss_item_sk k, sum(ss_quantity) q FROM store_sales GROUP BY ss_item_sk) \
                 SELECT count(*) FROM store_sales s \
                 WHERE s.ss_item_sk IN (SELECT k FROM r) \
                   AND s.ss_quantity IN (SELECT t.ss_quantity FROM store_sales t, r WHERE t.ss_item_sk = r.k)",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            2,
            "`t.ss_item_sk = r.k` is a join, not a filter the copy could push:\n{plan}"
        );
    }

    /// A self-join between two references is not a consumer filter.
    ///
    /// `inv1.i_item_sk = inv2.i_item_sk` names the candidate twice through two
    /// aliases; neither copy computes less for it, and the shared copy serves
    /// both sides of the join.
    #[tokio::test]
    async fn a_self_join_between_two_references_does_not_block_caching() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH r AS (SELECT ss_item_sk k, sum(ss_quantity) q FROM store_sales GROUP BY ss_item_sk) \
                 SELECT a.q, b.q FROM r a, r b WHERE a.k = b.k",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            2,
            "a join between two references of the same CTE pushes nothing into \
             either copy:\n{plan}"
        );
    }

    /// A CTE whose consumers filter it must stay inlined.
    ///
    /// q64's `WHERE cs1.syear = 2000 AND cs2.syear = 2001`: each inlined copy
    /// gets its own year through `PushDownFilter`; the shared copy has to compute
    /// both. 0.63x when this was materialised.
    #[tokio::test]
    async fn a_cte_filtered_by_its_consumers_is_left_alone() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH r AS (SELECT ss_item_sk, sum(ss_quantity) q FROM store_sales GROUP BY ss_item_sk) \
                 SELECT a.q, b.q FROM (SELECT q FROM r WHERE r.ss_item_sk = 1) a, \
                                      (SELECT q FROM r WHERE r.ss_item_sk = 2) b",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            cached_scans(&plan),
            0,
            "consumers push predicates into their own copies; caching would \
             compute both and serve neither cheaply:\n{plan}"
        );
    }

    /// A CTE past the cap is left inlined rather than held in memory.
    ///
    /// The cap is the difference between "slower than DuckDB" and "out of
    /// memory", so it is tested against the real collector, not asserted from
    /// reading the constant.
    #[tokio::test]
    async fn a_body_over_the_cap_is_abandoned() {
        let ctx = context();
        let body = ctx.sql("SELECT * FROM store_sales").await.expect("plan");
        let capped = collect_within_cap(body, 1, usize::MAX)
            .await
            .expect("collect");
        assert!(
            capped.is_none(),
            "a body past the row cap must abandon, not collect"
        );
        let body = ctx.sql("SELECT * FROM store_sales").await.expect("plan");
        let under = collect_within_cap(body, usize::MAX, usize::MAX)
            .await
            .expect("collect");
        assert!(under.is_some(), "a body under the cap must collect");
    }

    /// A CTE referenced once must not be materialised: the round trip through a
    /// `MemTable` would cost a copy and save nothing.
    #[tokio::test]
    async fn a_cte_referenced_once_is_left_alone() {
        let ctx = context();
        let frame = ctx
            .sql(
                "WITH results AS (SELECT ss_quantity FROM store_sales) \
                  SELECT sum(ss_quantity) FROM results",
            )
            .await
            .expect("plan");
        let plan = optimized(
            &ctx,
            materialize_repeated_ctes_forced(&ctx, frame)
                .await
                .expect("materialize"),
        )
        .await;
        assert_eq!(
            plan.matches("__krishiv_cte_").count(),
            0,
            "a single-reference CTE must stay inlined:\n{plan}"
        );
    }

    async fn rows(frame: DataFrame) -> Vec<String> {
        let batches = frame.collect().await.expect("collect");
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
        out.sort();
        out
    }
}
