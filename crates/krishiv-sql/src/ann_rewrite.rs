#![forbid(unsafe_code)]
//! Plain-SQL kNN acceleration: `ORDER BY <distance>(col, literal) LIMIT k`
//! → a provably-safe distance pre-filter — Phase 36 G19, the query half of
//! the transparent-acceleration wire-in.
//!
//! # Why a pre-filter and not an "index scan"
//!
//! A user's `ORDER BY … LIMIT k` is **exact SQL**; silently swapping it
//! for an approximate probe would change answers. This rule never does
//! that. When the engine holds a vector index for the scanned table
//! ([`crate::vector_search::TableVectorIndex`], populated by
//! `build_vector_index`, `register_parquet_with_vector_index`, or a prior
//! `ann_search`), the rule computes the **exact** k-th smallest distance τ
//! from the cached rows at plan time and injects
//!
//! ```text
//!   Sort: dist(col, q) ASC, fetch=k        (untouched)
//!     Filter: dist(col, q) <= τ + slack    (injected)
//!       <original input>
//! ```
//!
//! The Sort — same expression, same fetch — still produces the final
//! order, so the plan's answer is the brute-force answer by construction:
//! the filter only removes rows whose distance provably exceeds the k-th
//! best, and the slack (see `tau_slack`) over-covers f32-vs-f64
//! representation differences, erring toward keeping rows. τ itself is
//! exact, computed by a triangle-inequality-pruned traversal of the IVF
//! cells (Euclidean) or a flat pass (cosine) over the cached rows.
//!
//! # When the rule declines (all of these must hold to fire)
//!
//! - first sort key is `cosine_distance`/`array_cosine_distance`/
//!   `array_distance` over a column and a literal vector, ASC, NULLS LAST;
//! - `fetch` is present and the cache holds ≥ k defined-distance rows
//!   (fewer, and NULL-distance rows could reach the tail of the top-k —
//!   the filter would drop them, so the rule must not fire);
//! - the input chain down to the `TableScan` is row-preserving
//!   (projections/aliases only, no `WHERE`, join, or aggregate — τ over
//!   the full table says nothing about a filtered subset's k-th best);
//! - the scan carries no pushed-down filters or fetch of its own;
//! - a cached index exists for exactly that (table, column) under the
//!   matching metric and dimension.
//!
//! Cache invalidation on every DML / re-registration path keeps a firing
//! rule honest; `KRISHIV_ANN_AUTO_REWRITE=off` kills the rewrite entirely.
//! The distributed planner does not register this rule — its stages plan
//! against `BatchSqlTable` schemas without engine-resident row data.

use std::sync::Arc;

use datafusion::common::Column;
use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{Expr, Filter, LogicalPlan, TableScan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;

use crate::vector_search::{SqlDistanceFn, VectorIndexCache, vector_cache_key};

/// Environment kill-switch. Default on: the rewrite is results-identical
/// by construction, so it earns the same default as any other optimizer
/// rule; set to `0`/`off`/`false`/`no` to disable.
pub const ANN_AUTO_REWRITE_ENV: &str = "KRISHIV_ANN_AUTO_REWRITE";

fn enabled_from(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "off" | "false" | "no"
    )
}

/// The rule. Holds a clone of the engine's vector-index cache; the rule
/// and `SqlEngine` see one map, so an index built through any entry point
/// accelerates plain SQL immediately.
#[derive(Debug)]
pub struct AnnTopKPrefilter {
    cache: VectorIndexCache,
    enabled: bool,
}

impl AnnTopKPrefilter {
    pub(crate) fn new(cache: VectorIndexCache) -> Self {
        let enabled = enabled_from(&std::env::var(ANN_AUTO_REWRITE_ENV).unwrap_or_default());
        Self { cache, enabled }
    }

    /// A disabled instance, for tests: mutating process environment is
    /// unsound under a multi-threaded runner, so the switch is tested by
    /// construction instead.
    #[cfg(test)]
    pub(crate) fn disabled(cache: VectorIndexCache) -> Self {
        Self {
            cache,
            enabled: false,
        }
    }
}

impl OptimizerRule for AnnTopKPrefilter {
    fn name(&self) -> &str {
        "ann_topk_prefilter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !self.enabled {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Sort(sort) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(k) = sort.fetch.filter(|k| *k > 0) else {
            return Ok(Transformed::no(plan));
        };
        // First key must be the distance ascending with NULLs last; extra
        // keys are tie-breaks over the same retained candidate pool and
        // stay safe.
        let Some(first) = sort.expr.first() else {
            return Ok(Transformed::no(plan));
        };
        if !first.asc || first.nulls_first {
            return Ok(Transformed::no(plan));
        }
        let Some((distance_fn, column, query)) = match_distance_call(&first.expr) else {
            return Ok(Transformed::no(plan));
        };
        let Some(scan) = row_preserving_scan(&sort.input) else {
            return Ok(Transformed::no(plan));
        };
        // The distance column must belong to the scanned table when it says
        // which relation it is from.
        if let Some(rel) = &column.relation
            && !rel.table().eq_ignore_ascii_case(scan.table_name.table())
        {
            return Ok(Transformed::no(plan));
        }
        let key = vector_cache_key(scan.table_name.table(), &column.name);
        let Some(entry) = self.cache.read().ok().and_then(|c| c.get(&key).cloned()) else {
            return Ok(Transformed::no(plan));
        };
        if entry.index.metric != distance_fn.index_metric() || entry.dim != query.len() {
            return Ok(Transformed::no(plan));
        }
        let Some(kth) = entry.exact_kth_sql_distance(&query, k, distance_fn) else {
            return Ok(Transformed::no(plan));
        };
        let tau = kth + entry.tau_slack(distance_fn) + kth.abs() * 1e-9;

        let predicate = first.expr.clone().lt_eq(lit(tau));
        let filtered = LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(&sort.input))?);
        Ok(Transformed::yes(LogicalPlan::Sort(
            datafusion::logical_expr::Sort {
                expr: sort.expr.clone(),
                input: Arc::new(filtered),
                fetch: sort.fetch,
            },
        )))
    }
}

/// Strip semantic no-ops: aliases (the planner names sort keys
/// `expr AS <display>`), and the type-coercion casts the analyzer wraps
/// around `FixedSizeList(Float32)` columns and array literals to reach the
/// UDF's `List(Float64)` signature.
fn strip_casts(mut expr: &Expr) -> &Expr {
    loop {
        match expr {
            Expr::Alias(alias) => expr = &alias.expr,
            Expr::Cast(cast) => expr = &cast.expr,
            Expr::TryCast(cast) => expr = &cast.expr,
            other => return other,
        }
    }
}

/// Match `<distance_fn>(column, literal-vector)` in either argument order
/// (both supported functions are symmetric).
fn match_distance_call(expr: &Expr) -> Option<(SqlDistanceFn, &Column, Vec<f32>)> {
    let Expr::ScalarFunction(call) = strip_casts(expr) else {
        return None;
    };
    let distance_fn = match call.func.name() {
        "cosine_distance" | "array_cosine_distance" => SqlDistanceFn::CosineDistance,
        "array_distance" => SqlDistanceFn::Euclidean,
        _ => return None,
    };
    let [a, b] = call.args.as_slice() else {
        return None;
    };
    let matched = match (as_column(a), as_query_vec(b)) {
        (Some(column), Some(query)) => (column, query),
        _ => match (as_column(b), as_query_vec(a)) {
            (Some(column), Some(query)) => (column, query),
            _ => return None,
        },
    };
    Some((distance_fn, matched.0, matched.1))
}

fn as_column(expr: &Expr) -> Option<&Column> {
    match strip_casts(expr) {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}

/// A literal query vector: either a folded list scalar or a `make_array`
/// call over numeric literals (the pre-folding spelling).
fn as_query_vec(expr: &Expr) -> Option<Vec<f32>> {
    match strip_casts(expr) {
        Expr::Literal(scalar, _) => scalar_list_to_f32(scalar),
        Expr::ScalarFunction(call) if call.func.name() == "make_array" => call
            .args
            .iter()
            .map(|arg| match strip_casts(arg) {
                Expr::Literal(scalar, _) => scalar_to_f32(scalar),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

fn scalar_to_f32(scalar: &ScalarValue) -> Option<f32> {
    match scalar.cast_to(&arrow::datatypes::DataType::Float64).ok()? {
        ScalarValue::Float64(Some(v)) => Some(v as f32),
        _ => None,
    }
}

fn scalar_list_to_f32(scalar: &ScalarValue) -> Option<Vec<f32>> {
    use arrow::array::{Array, Float64Array};
    let elements = match scalar {
        ScalarValue::List(list) if list.len() == 1 && list.is_valid(0) => list.value(0),
        ScalarValue::LargeList(list) if list.len() == 1 && list.is_valid(0) => list.value(0),
        ScalarValue::FixedSizeList(list) if list.len() == 1 && list.is_valid(0) => list.value(0),
        _ => return None,
    };
    let widened = arrow::compute::cast(&elements, &arrow::datatypes::DataType::Float64).ok()?;
    let floats = widened.as_any().downcast_ref::<Float64Array>()?;
    if floats.null_count() > 0 {
        return None;
    }
    Some(floats.values().iter().map(|v| *v as f32).collect())
}

/// Walk to the `TableScan` through row-preserving nodes only. A node that
/// can drop or multiply rows (filter, join, aggregate, limit — anything
/// unrecognized) makes the rule decline: τ over the whole table bounds
/// nothing about a subset. The scan itself must carry no pushed filters
/// and no fetch for the same reason.
fn row_preserving_scan(mut node: &LogicalPlan) -> Option<&TableScan> {
    loop {
        match node {
            LogicalPlan::Projection(projection) => node = &projection.input,
            LogicalPlan::SubqueryAlias(alias) => node = &alias.input,
            LogicalPlan::TableScan(scan)
                if scan.filters.is_empty() && scan.fetch.is_none() =>
            {
                return Some(scan);
            }
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_metric::VectorMetric;
    use arrow::array::{Array, StringArray};

    /// Seed `n` rows through plain SQL so the table exists via the same
    /// paths a user would take, then return the engine.
    async fn seeded_engine(n: usize) -> crate::SqlEngine {
        let engine = crate::SqlEngine::new();
        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            let base = (i % 8) as f64 * 10.0 + (i / 8) as f64 * 0.1;
            values.push(format!(
                "('row-{i:03}', [{}, {}, {}, {}])",
                base,
                base + 1.0,
                base - 1.0,
                base + 0.5
            ));
        }
        let sql = format!(
            "CREATE TABLE docs AS SELECT * FROM (VALUES {}) v(id, emb)",
            values.join(", ")
        );
        engine
            .sql(&sql)
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        engine
    }

    async fn ids(engine: &crate::SqlEngine, sql: &str) -> Vec<String> {
        let batches = engine
            .sql(sql)
            .await
            .expect("query")
            .collect()
            .await
            .expect("collect");
        let mut out = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string ids");
            for i in 0..batch.num_rows() {
                out.push(column.value(i).to_string());
            }
        }
        out
    }

    /// The DataFusion optimized logical plan — where an injected τ filter
    /// is visible. (The engine's `EXPLAIN` prints krishiv's own plan
    /// summary, not this.)
    async fn explain(engine: &crate::SqlEngine, sql: &str) -> String {
        let plan = engine
            .session_context()
            .sql(sql)
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("optimize");
        format!("{}", plan.display_indent())
    }

    const KNN: &str =
        "SELECT id FROM docs ORDER BY cosine_distance(emb, [20.0, 21.0, 19.0, 20.5]) LIMIT 5";

    #[tokio::test]
    async fn rewrite_fires_and_answers_stay_identical_cosine() {
        let engine = seeded_engine(120).await;
        let baseline = ids(&engine, KNN).await; // no index yet — brute force
        assert!(
            !explain(&engine, KNN).await.contains("<="),
            "no index, no injected filter"
        );

        engine
            .build_vector_index("docs", "emb", VectorMetric::Cosine, 0)
            .await
            .expect("index builds");
        let plan = explain(&engine, KNN).await;
        assert!(
            plan.contains("cosine_distance") && plan.contains("<="),
            "the τ pre-filter must appear in the optimized plan:\n{plan}"
        );
        let accelerated = ids(&engine, KNN).await;
        assert_eq!(accelerated, baseline, "the rewrite may never move an answer");
    }

    #[tokio::test]
    async fn rewrite_answers_stay_identical_euclidean() {
        let engine = seeded_engine(120).await;
        let knn =
            "SELECT id FROM docs ORDER BY array_distance(emb, [40.0, 41.0, 39.0, 40.5]) LIMIT 7";
        let baseline = ids(&engine, knn).await;
        engine
            .build_vector_index("docs", "emb", VectorMetric::L2, 0)
            .await
            .expect("index builds");
        let plan = explain(&engine, knn).await;
        assert!(
            plan.contains("array_distance") && plan.contains("<="),
            "τ pre-filter under array_distance:\n{plan}"
        );
        assert_eq!(ids(&engine, knn).await, baseline);
    }

    #[tokio::test]
    async fn metric_mismatch_declines() {
        let engine = seeded_engine(60).await;
        engine
            .build_vector_index("docs", "emb", VectorMetric::L2, 0)
            .await
            .expect("index builds");
        // Cosine query over an L2 index: cells mean nothing — must decline.
        assert!(
            !explain(&engine, KNN).await.contains("<="),
            "wrong-metric index must not fire"
        );
        // And the answer is still just the brute-force answer.
        let fresh = seeded_engine(60).await;
        assert_eq!(ids(&engine, KNN).await, ids(&fresh, KNN).await);
    }

    #[tokio::test]
    async fn where_clause_declines() {
        // τ over the full table bounds nothing about a filtered subset's
        // k-th best; firing here could DROP correct rows.
        let engine = seeded_engine(120).await;
        engine
            .build_vector_index("docs", "emb", VectorMetric::Cosine, 0)
            .await
            .expect("index builds");
        let filtered = "SELECT id FROM docs WHERE id > 'row-090' \
             ORDER BY cosine_distance(emb, [20.0, 21.0, 19.0, 20.5]) LIMIT 5";
        let plan = explain(&engine, filtered).await;
        // The only <= in the plan would be ours; the WHERE uses >.
        assert!(
            !plan.contains("<="),
            "row-dropping input must decline:\n{plan}"
        );
        let fresh = seeded_engine(120).await;
        assert_eq!(ids(&engine, filtered).await, ids(&fresh, filtered).await);
    }

    #[tokio::test]
    async fn table_replacement_invalidates_and_stays_exact() {
        let engine = seeded_engine(120).await;
        engine
            .build_vector_index("docs", "emb", VectorMetric::Cosine, 0)
            .await
            .expect("index builds");
        let _ = ids(&engine, KNN).await; // rewrite active
        // Replace the table wholesale: different rows, same name.
        engine
            .sql(
                "CREATE OR REPLACE TABLE docs AS SELECT * FROM (VALUES \
                 ('n-1', [100.0, 101.0, 99.0, 100.5]), \
                 ('n-2', [20.0, 21.0, 19.0, 20.5]), \
                 ('n-3', [0.0, 1.0, -1.0, 0.5])) v(id, emb)",
            )
            .await
            .expect("replace")
            .collect()
            .await
            .expect("collect");
        let after = ids(
            &engine,
            "SELECT id FROM docs ORDER BY cosine_distance(emb, [20.0, 21.0, 19.0, 20.5]) LIMIT 2",
        )
        .await;
        assert_eq!(
            after,
            vec!["n-2".to_string(), "n-1".to_string()],
            "a stale τ from the replaced table must not drop the new rows"
        );
    }

    #[tokio::test]
    async fn near_tie_boundary_keeps_the_exact_top_k() {
        // Rows packed within ~1e-7 of the k-th distance: the slack must
        // err toward keeping candidates, and the Sort settles the order.
        let engine = crate::SqlEngine::new();
        let mut values = Vec::new();
        for i in 0..40 {
            let eps = i as f64 * 1e-7;
            values.push(format!("('t-{i:02}', [1.0, {}, 0.0, 0.0])", 1.0 + eps));
        }
        engine
            .sql(format!(
                "CREATE TABLE ties AS SELECT * FROM (VALUES {}) v(id, emb)",
                values.join(", ")
            ))
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let knn = "SELECT id FROM ties ORDER BY cosine_distance(emb, [1.0, 1.0, 0.0, 0.0]) LIMIT 6";
        let baseline = ids(&engine, knn).await;
        engine
            .build_vector_index("ties", "emb", VectorMetric::Cosine, 4)
            .await
            .expect("index builds");
        assert_eq!(ids(&engine, knn).await, baseline, "boundary ties survive");
    }

    #[tokio::test]
    async fn disabled_rule_rewrites_nothing() {
        use datafusion::optimizer::OptimizerContext;
        let cache: VectorIndexCache = Arc::default();
        let rule = AnnTopKPrefilter::disabled(cache);
        // Any plan at all: a disabled rule must return it untransformed.
        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
            .build()
            .expect("plan");
        let out = rule
            .rewrite(plan.clone(), &OptimizerContext::new())
            .expect("rewrite runs");
        assert!(!out.transformed);
        assert_eq!(out.data, plan);
    }
}
