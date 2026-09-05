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
//! # OFF by default, and the two sweeps that made it so
//!
//! This rule reproduces the q27 hand result exactly — 426 ms -> 191 ms, rows
//! identical — and is still a **net loss across the suite**. Measured on all 99
//! TPC-DS queries at SF1, warm, best of three, paired and interleaved, results
//! hashed per query:
//!
//! ```text
//!   first version    16177 ms -> 18224 ms   0.888x    5 wins >10%, 18 losses
//!   with the guard   31561 ms -> 33365 ms   0.946x   11 wins >10%, 20 losses
//! ```
//!
//! (The second sweep ran against a loaded machine, which is why its absolute
//! numbers are near double; the arms are interleaved, so the ratio holds.)
//! **99/99 rows identical in both.** The rewrite is correct. It is not a win.
//!
//! Two mechanisms, one fixed and one not:
//!
//! - **Fixed.** The first version cached anything repeated, including
//!   `SubqueryAlias: ss1` over a bare `store_sales` scan — 2.88 M rows, all
//!   columns, in q44, which each consumer would otherwise have filtered to
//!   `ss_store_sk = 4` in the scan itself. q44 ran 3.9x slower. See
//!   [`reduces_its_input`].
//!
//! - **Not fixed, and the reason this stays off.** What remains is
//!   `WITH cs AS (…) … WHERE cs1.syear = 2000 … cs2.syear = 2001` — q64, q39,
//!   q2, q75, q59. Each *inlined* copy receives its consumer's predicate through
//!   `PushDownFilter` and computes only the rows that consumer wants;
//!   materialising computes the union of what every consumer wants, once. When
//!   the consumers' predicates are selective and disjoint, N cheap copies beat
//!   one expensive shared one, and no structural property of the CTE body can
//!   tell that from q27, where the consumers filter nothing and one shared copy
//!   wins 2.1x. The missing input is a **cost comparison** between "the body,
//!   whole" and "the body, N times, each with its consumer's predicate pushed
//!   in" — an estimate this engine does not have. It is not guessed at here.
//!
//! So: off by default, correct when enabled, and worth enabling for a workload
//! whose repeated CTEs are consumed unfiltered.
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
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{LogicalPlan, SubqueryAlias};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Environment switch for CTE materialisation.
///
/// Opt-*in*. The rewrite executes part of the query eagerly inside a call whose
/// contract is lazy, which is a change of *when* work happens for every caller
/// of `SqlEngine::sql`, not only for the ones it speeds up. It stays behind a
/// switch until a full 99-query sweep says otherwise.
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

/// Whether CTE materialisation is enabled (default: **no**).
pub fn cte_materialize_enabled() -> bool {
    matches!(
        std::env::var(CTE_MATERIALIZE_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "on" | "true" | "yes"
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
    let mut plan = dataframe.logical_plan().clone();
    let mut materialized = false;
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
    let mut counts: HashMap<&SubqueryAlias, usize> = HashMap::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if let LogicalPlan::SubqueryAlias(alias) = node {
            *counts.entry(alias).or_insert(0) += 1;
        }
        stack.extend(node.inputs());
    }
    counts
        .into_iter()
        .filter(|(alias, count)| {
            *count >= 2 && reduces_its_input(&alias.input) && !already_materialized(&alias.input)
        })
        .max_by_key(|(alias, _)| node_count(&alias.input))
        .map(|(alias, _)| alias.clone())
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
    if *replacement.schema() != candidate.schema {
        return Ok(None);
    }

    let rewritten = plan
        .clone()
        .transform_down(|node| {
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
            plan.matches(TEMP_PREFIX).count(),
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
            plan.matches(TEMP_PREFIX).count(),
            0,
            "an unfiltered scan must not be cached — the consumers' predicates \
             push into it and the cache would forfeit that:\n{plan}"
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
