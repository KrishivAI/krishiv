//! Differential oracle: the O(Δ) path and the full-recompute path must agree.
//!
//! The engine can already compute every view both ways — `force_diff_based`
//! makes a flow re-run the whole view SQL and diff it, which is the trusted
//! answer by construction. Before this file, that switch had exactly one caller
//! in the whole repo (a proptest), so the two paths were never compared against
//! each other for any real view shape.
//!
//! That is the gap IVM-AUD-SCHEMA-1 fell through. A projected `DISTINCT` and a
//! projected join each published the operator's own wider relation while
//! reporting `Incremental` with empty health, and no test could see it, because
//! every test asserted the incremental path against *itself*. Comparing against
//! the recompute answer catches that class automatically, for any shape, without
//! anyone having to predict which operator will be wrong next.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn orders(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Sort rows into a comparable canonical form — neither path promises an order.
fn canonical(batch: &RecordBatch) -> Vec<Vec<i64>> {
    let cols: Vec<&Int64Array> = (0..batch.num_columns())
        .map(|c| {
            batch
                .column(c)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
        })
        .collect();
    let mut rows: Vec<Vec<i64>> = (0..batch.num_rows())
        .map(|r| cols.iter().map(|c| c.value(r)).collect())
        .collect();
    rows.sort();
    rows
}

/// Feed the same deltas to an incremental flow and a forced-recompute flow,
/// and return `(incremental_snapshot, recompute_snapshot, was_incremental)`.
async fn both_ways(
    sql: &str,
    out: SchemaRef,
    batches: &[Vec<(i64, i64, i64)>],
) -> (RecordBatch, RecordBatch, bool) {
    let spec = |name: &str| IncrementalViewSpec {
        name: name.into(),
        body_sql: sql.into(),
        output_schema: out.clone(),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    };

    let incr = IncrementalFlow::new();
    incr.register_view(spec("v")).unwrap();
    let full = IncrementalFlow::new();
    full.register_view(spec("v")).unwrap();
    full.force_diff_based().unwrap();

    for rows in batches {
        let d = DeltaBatch::from_inserts(orders(rows)).unwrap();
        incr.feed("orders", d.clone()).unwrap();
        full.feed("orders", d).unwrap();
        incr.step_datafusion().await.unwrap();
        full.step_datafusion().await.unwrap();
    }

    let was_incremental = incr
        .view_plan_classification("v")
        .unwrap()
        .expect("registered")
        .0;
    (
        incr.snapshot("v").unwrap().expect("incremental published"),
        full.snapshot("v").unwrap().expect("recompute published"),
        was_incremental,
    )
}

fn i64_schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ))
}

/// Two deltas, so the second exercises maintenance rather than first-build.
fn two_batches() -> Vec<Vec<(i64, i64, i64)>> {
    vec![
        vec![(1, 10, 100), (2, 10, 200), (3, 20, 300)],
        vec![(1, 10, 50), (4, 20, 400), (2, 10, 25)],
    ]
}

#[tokio::test]
async fn grouped_sum_agrees_with_full_recompute() {
    let (a, b, incremental) = both_ways(
        "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
        i64_schema(&["region", "total"]),
        &two_batches(),
    )
    .await;
    assert!(incremental, "this shape must take the O(delta) path");
    assert_eq!(
        canonical(&a),
        canonical(&b),
        "O(delta) disagreed with recompute"
    );
}

#[tokio::test]
async fn grouped_min_max_agrees_with_full_recompute() {
    let (a, b, incremental) = both_ways(
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM orders GROUP BY region",
        i64_schema(&["region", "lo", "hi"]),
        &two_batches(),
    )
    .await;
    assert!(incremental, "this shape must take the O(delta) path");
    assert_eq!(canonical(&a), canonical(&b));
}

#[tokio::test]
async fn global_aggregate_agrees_with_full_recompute() {
    let (a, b, incremental) = both_ways(
        "SELECT SUM(amount) AS total, COUNT(*) AS n FROM orders",
        i64_schema(&["total", "n"]),
        &two_batches(),
    )
    .await;
    assert!(incremental, "this shape must take the O(delta) path");
    assert_eq!(canonical(&a), canonical(&b));
}

#[tokio::test]
async fn distinct_star_agrees_with_full_recompute() {
    let out = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let mut batches = two_batches();
    batches[1].push((1, 10, 100)); // exact duplicate of a row from batch 0
    let (a, b, incremental) = both_ways("SELECT DISTINCT * FROM orders", out, &batches).await;
    assert!(incremental, "this shape must take the O(delta) path");
    assert_eq!(canonical(&a), canonical(&b));
}

/// The shape SCHEMA-1 was about. It is DiffBased now, so both flows take the
/// same path and this cannot fail on a disagreement — its job is to pin that
/// the *answer* is the projected one, which is what the defect got wrong.
#[tokio::test]
async fn projected_distinct_agrees_with_full_recompute() {
    let (a, b, incremental) = both_ways(
        "SELECT DISTINCT region FROM orders",
        i64_schema(&["region"]),
        &two_batches(),
    )
    .await;
    // Value comparison FIRST, deliberately. Asserting the plan kind before the
    // answer would let the classification short-circuit the disagreement, and
    // then this test would only ever prove which plan was chosen — not that the
    // oracle can catch a wrong answer, which is the whole point of it.
    assert_eq!(
        a.num_columns(),
        1,
        "published {} columns against a 1-column declared output — the operator's \
         relation, not the view's (IVM-AUD-SCHEMA-1)",
        a.num_columns()
    );
    assert_eq!(
        canonical(&a),
        canonical(&b),
        "O(delta) disagreed with full recompute"
    );
    assert_eq!(canonical(&a), vec![vec![10], vec![20]]);
    assert!(
        !incremental,
        "a projected DISTINCT has no O(delta) plan (IVM-AUD-SCHEMA-1); if this \
         starts passing incrementally, the oracle above must be extended to prove \
         the new operator emits the projected relation"
    );
}

#[tokio::test]
async fn filtered_aggregate_agrees_with_full_recompute() {
    let (a, b, incremental) = both_ways(
        "SELECT region, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY region",
        i64_schema(&["region", "total"]),
        &two_batches(),
    )
    .await;
    assert!(incremental, "a WHERE pushed onto the delta stays O(delta)");
    assert_eq!(canonical(&a), canonical(&b));
}
