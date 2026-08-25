//! IVM-ORDER-1: `ORDER BY` on an incremental view.
//!
//! Ordering is a read-time property of a Z-set, not a maintenance concern, so
//! the `Sort` node is peeled and the inner plan maintained exactly as it would
//! be without the clause. `LIMIT` is a different thing and must NOT be peeled —
//! a top-N's *answer* depends on the order, so dropping the LIMIT would publish
//! more rows than the view promises.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn spec(n: &str, sql: &str, out: SchemaRef) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: n.into(),
        body_sql: sql.into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}
fn src() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
    ]))
}
fn i64s(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ))
}
fn feed(flow: &IncrementalFlow, ks: Vec<i64>, as_: Vec<i64>) {
    let b = RecordBatch::try_new(
        src(),
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(as_)),
        ],
    )
    .unwrap();
    flow.feed("s", DeltaBatch::from_inserts(b).unwrap())
        .unwrap();
}
fn col(b: &RecordBatch, name: &str) -> Vec<i64> {
    let a = b
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..a.len()).map(|i| a.value(i)).collect()
}

#[tokio::test]
async fn an_ordered_aggregate_stays_incremental_and_reads_back_sorted() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "o",
        "SELECT k, SUM(a) AS total FROM s GROUP BY k ORDER BY total DESC",
        i64s(&["k", "total"]),
    ))
    .unwrap();
    feed(&flow, vec![1, 2, 3], vec![10, 30, 20]);
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow.view_plan_classification("o").unwrap().unwrap();
    assert!(
        incremental,
        "ORDER BY must not cost the view its O(delta) plan: {reason}"
    );
    assert!(summary.degraded_views.is_empty());
    assert_eq!(
        col(&flow.snapshot("o").unwrap().unwrap(), "total"),
        vec![30, 20, 10],
        "snapshot must read back in the declared order"
    );
}

/// The order must hold after maintenance, not just on the first tick — a sort
/// applied once at build time would pass the test above and fail this one.
#[tokio::test]
async fn the_order_holds_after_a_later_delta_reshuffles_it() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "o",
        "SELECT k, SUM(a) AS total FROM s GROUP BY k ORDER BY total ASC",
        i64s(&["k", "total"]),
    ))
    .unwrap();
    feed(&flow, vec![1, 2], vec![10, 20]);
    flow.step_datafusion().await.unwrap();
    assert_eq!(
        col(&flow.snapshot("o").unwrap().unwrap(), "total"),
        vec![10, 20]
    );

    // Push k=1 past k=2, which reverses the presentation order.
    feed(&flow, vec![1], vec![100]);
    flow.step_datafusion().await.unwrap();
    // Assert the PLAN, not just the answer: DiffBased re-runs the view SQL and
    // produces the same sorted rows, so an answer-only assertion here passes
    // whether or not ORDER BY costs the view its incremental plan.
    let (incremental, reason) = flow.view_plan_classification("o").unwrap().unwrap();
    assert!(incremental, "still O(delta) after maintenance: {reason}");
    assert_eq!(
        col(&flow.snapshot("o").unwrap().unwrap(), "total"),
        vec![20, 110],
        "the order is applied on read, so it tracks the maintained values"
    );
}

#[tokio::test]
async fn ordering_by_several_columns_works() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "o",
        "SELECT k, a FROM s ORDER BY k ASC, a DESC",
        i64s(&["k", "a"]),
    ))
    .unwrap();
    feed(&flow, vec![2, 1, 1], vec![5, 7, 9]);
    flow.step_datafusion().await.unwrap();
    let (incremental, reason) = flow.view_plan_classification("o").unwrap().unwrap();
    assert!(
        incremental,
        "a multi-column ORDER BY must stay O(delta): {reason}"
    );
    let snap = flow.snapshot("o").unwrap().unwrap();
    assert_eq!(col(&snap, "k"), vec![1, 1, 2]);
    assert_eq!(col(&snap, "a"), vec![9, 7, 5]);
}

/// A top-N whose input is an **aggregate** falls back, because the operator
/// reads a source or upstream view and `resolve_source_with_filters` refuses to
/// peel an `Aggregate`. Express it as a two-hop DAG (aggregate view, then a
/// top-N view over it), the same discipline the map follows.
///
/// The load-bearing half is the second assertion: the fallback must still
/// honour the LIMIT. Peeling a `Sort` that carries `fetch` would drop it and
/// publish every row — a wrong answer wearing a working view's clothes.
#[tokio::test]
async fn a_top_n_over_an_aggregate_in_one_view_falls_back() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "t",
        "SELECT k, SUM(a) AS total FROM s GROUP BY k ORDER BY total DESC LIMIT 2",
        i64s(&["k", "total"]),
    ))
    .unwrap();
    feed(&flow, vec![1, 2, 3], vec![10, 30, 20]);
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, _) = flow.view_plan_classification("t").unwrap().unwrap();
    assert!(
        !incremental,
        "a top-N has no O(delta) plan; peeling its LIMIT would be a wrong answer"
    );
    assert!(summary.degraded_views.iter().any(|v| v == "t"));
    // And the fallback must honour the LIMIT.
    assert_eq!(
        flow.snapshot("t").unwrap().unwrap().num_rows(),
        2,
        "the DiffBased answer must still be the top 2"
    );
}

/// Top-N directly over a source is now O(Δ) (IVM-TOPN-1): an ordered index over
/// the relation, emitting only the change to the window.
#[tokio::test]
async fn a_top_n_over_a_source_is_incremental() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec("t", "SELECT * FROM s ORDER BY a DESC LIMIT 2", src()))
        .unwrap();
    feed(&flow, vec![1, 2, 3], vec![10, 30, 20]);
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow.view_plan_classification("t").unwrap().unwrap();
    assert!(
        incremental,
        "top-N over a source must be O(delta): {reason}"
    );
    assert!(summary.degraded_views.is_empty());
    let snap = flow.snapshot("t").unwrap().unwrap();
    assert_eq!(snap.num_rows(), 2, "LIMIT 2");
    assert_eq!(
        col(&snap, "a"),
        vec![30, 20],
        "top 2 by a DESC, read back in order"
    );

    // A retraction inside the window promotes the row below it.
    let del = RecordBatch::try_new(
        src(),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .unwrap();
    flow.feed("s", DeltaBatch::from_deletes(del).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();
    let snap = flow.snapshot("t").unwrap().unwrap();
    assert_eq!(
        col(&snap, "a"),
        vec![20, 10],
        "retracting the leader promotes a(10) into the window"
    );
}

/// A top-N view must read back **sorted**, not merely hold the right k rows.
///
/// This is the defect the first cut shipped: `extract_output_order` matched only
/// a bare `Sort`, while `ORDER BY … LIMIT` roots at `Limit`, so a top-N recorded
/// no order at all. It survived `a_top_n_over_a_source_is_incremental` because
/// the operator's `BTreeMap` emits in sort order, making early snapshots look
/// sorted by accident. Reading back after several maintenance ticks — each one
/// reshuffling which rows are in the window — is what exposes it.
#[tokio::test]
async fn a_top_n_reads_back_sorted_after_repeated_maintenance() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec("t", "SELECT * FROM s ORDER BY a DESC LIMIT 3", src()))
        .unwrap();

    // Arrive deliberately out of order, one tick at a time, so no single tick's
    // emission order can accidentally be the answer.
    for (k, a) in [(1, 50), (2, 10), (3, 90), (4, 30), (5, 70)] {
        feed(&flow, vec![k], vec![a]);
        flow.step_datafusion().await.unwrap();
    }

    let (incremental, reason) = flow.view_plan_classification("t").unwrap().unwrap();
    assert!(incremental, "{reason}");
    let snap = flow.snapshot("t").unwrap().unwrap();
    assert_eq!(
        col(&snap, "a"),
        vec![90, 70, 50],
        "top 3 by a DESC, and read back in that order"
    );
}
