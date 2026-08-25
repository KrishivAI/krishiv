//! IVM-MAP-1: the stateless map/projection operator.
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
fn rows(ks: Vec<i64>, as_: Vec<i64>) -> DeltaBatch {
    DeltaBatch::from_inserts(
        RecordBatch::try_new(
            src(),
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Int64Array::from(as_)),
            ],
        )
        .unwrap(),
    )
    .unwrap()
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

/// A narrowing projection now has an O(Δ) plan. Before IVM-MAP-1 there were
/// only three variants — Aggregate, Join, unprojected Distinct — so this fell
/// to DiffBased full re-execution.
#[tokio::test]
async fn a_projection_is_incremental() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec("p", "SELECT k FROM s", i64s(&["k"])))
        .unwrap();
    flow.feed("s", rows(vec![1, 2], vec![10, 20])).unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow.view_plan_classification("p").unwrap().unwrap();
    assert!(
        incremental,
        "a projection must get an O(delta) plan: {reason}"
    );
    assert!(summary.degraded_views.is_empty());
    let snap = flow.snapshot("p").unwrap().unwrap();
    assert_eq!(snap.num_columns(), 1);
    assert_eq!(col(&snap, "k"), vec![1, 2]);
}

/// A derived column — the shape every TPC-H measure needs.
#[tokio::test]
async fn a_derived_column_is_incremental() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "d",
        "SELECT k, a * 2 AS dbl FROM s",
        i64s(&["k", "dbl"]),
    ))
    .unwrap();
    flow.feed("s", rows(vec![1, 2], vec![10, 20])).unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow.view_plan_classification("d").unwrap().unwrap();
    assert!(
        incremental,
        "a derived column must get an O(delta) plan: {reason}"
    );
    assert!(summary.degraded_views.is_empty());
    assert_eq!(
        col(&flow.snapshot("d").unwrap().unwrap(), "dbl"),
        vec![20, 40]
    );
}

/// A bare filtered scan. `plan.rs` documented this as DiffBased; filter is
/// linear, so there was never a reason it had to be.
#[tokio::test]
async fn a_filtered_scan_is_incremental() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec("f", "SELECT * FROM s WHERE a > 15", src()))
        .unwrap();
    flow.feed("s", rows(vec![1, 2], vec![10, 20])).unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow.view_plan_classification("f").unwrap().unwrap();
    assert!(
        incremental,
        "a filtered scan must get an O(delta) plan: {reason}"
    );
    assert!(summary.degraded_views.is_empty());
    let snap = flow.snapshot("f").unwrap().unwrap();
    assert_eq!(snap.num_rows(), 1, "only a=20 passes a > 15");
    assert_eq!(col(&snap, "a"), vec![20]);
}

/// **The point of the whole operator.** `SUM(a*2)` degrades — an expression
/// under an aggregate has no O(Δ) plan — and that single gate blocks every
/// TPC-H derived measure. Split into map -> aggregate and BOTH hops are
/// incremental, which is the route that makes the corpus reachable.
#[tokio::test]
async fn a_derived_measure_aggregates_incrementally_as_a_two_hop_dag() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "m",
        "SELECT k, a * 2 AS dbl FROM s",
        i64s(&["k", "dbl"]),
    ))
    .unwrap();
    flow.register_view(spec(
        "agg",
        "SELECT k, SUM(dbl) AS total FROM m GROUP BY k",
        i64s(&["k", "total"]),
    ))
    .unwrap();
    flow.feed("s", rows(vec![1, 1, 2], vec![10, 20, 5]))
        .unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    for v in ["m", "agg"] {
        let (incremental, reason) = flow.view_plan_classification(v).unwrap().unwrap();
        assert!(incremental, "hop '{v}' fell back: {reason}");
    }
    assert!(
        summary.degraded_views.is_empty(),
        "{:?}",
        summary.degraded_views
    );
    assert!(
        summary.errored_views.is_empty(),
        "{:?}",
        summary.errored_views
    );

    let snap = flow.snapshot("agg").unwrap().unwrap();
    let mut got: Vec<(i64, i64)> = col(&snap, "k")
        .into_iter()
        .zip(col(&snap, "total"))
        .collect();
    got.sort();
    // k=1: (10+20)*2 = 60 ; k=2: 5*2 = 10
    assert_eq!(got, vec![(1, 60), (2, 10)]);
}

/// Map is linear, so a retraction must flow straight through it. If the
/// operator ever grew state, this is what would break first.
///
/// **Correctness test, not a proof of the operator**: it stays green with the
/// map plan reverted, because DiffBased computes the same answer. That is the
/// fallback mask — the plan-kind assertions in the tests above are what
/// distinguish fixed from broken.
#[tokio::test]
async fn a_retraction_flows_through_the_map() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "m",
        "SELECT k, a * 2 AS dbl FROM s",
        i64s(&["k", "dbl"]),
    ))
    .unwrap();
    flow.register_view(spec(
        "agg",
        "SELECT k, SUM(dbl) AS total FROM m GROUP BY k",
        i64s(&["k", "total"]),
    ))
    .unwrap();
    flow.feed("s", rows(vec![1, 1], vec![10, 20])).unwrap();
    flow.step_datafusion().await.unwrap();

    // Retract the a=20 row.
    let del = DeltaBatch::from_deletes(
        RecordBatch::try_new(
            src(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![20])),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    flow.feed("s", del).unwrap();
    let summary = flow.step_datafusion().await.unwrap();
    assert!(
        summary.errored_views.is_empty(),
        "{:?}",
        summary.errored_views
    );

    let snap = flow.snapshot("agg").unwrap().unwrap();
    assert_eq!(
        col(&snap, "total"),
        vec![20],
        "60 - 40 = 20 after the retraction"
    );
}

/// The map must not claim a shape whose relation is not the declared one —
/// the IVM-AUD-SCHEMA-1 rule, which applies to this operator too.
///
/// **Wrong-fix guard, not a defect proof**: it passes with the map plan
/// reverted too (everything fell back then). Its job is to catch a future map
/// that projects whatever it likes and calls the result the view.
#[tokio::test]
async fn a_map_whose_output_does_not_match_the_declaration_falls_back() {
    let flow = IncrementalFlow::new();
    // Declares one column; the SQL projects two.
    flow.register_view(spec("bad", "SELECT k, a FROM s", i64s(&["k"])))
        .unwrap();
    flow.feed("s", rows(vec![1], vec![10])).unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, _) = flow.view_plan_classification("bad").unwrap().unwrap();
    assert!(
        !incremental,
        "must not claim a relation the view did not declare"
    );
    assert!(summary.degraded_views.iter().any(|v| v == "bad"));
}

/// A map over a source carrying a CORE-2 deficit must publish the relation, not
/// the input stream.
///
/// `DELETE 42` before `INSERT 42`: the delete cancels nothing, so it goes to the
/// source's deficit and never reaches the materialized relation; the later
/// insert cancels it. The relation is empty throughout, so the view is empty.
///
/// Forwarding the *raw* delta instead publishes that un-cancellable `-1`, the
/// view's materialization clamps it to zero, and the later `+1` then makes the
/// row appear — one row where there should be none. A stateful operator's
/// accumulator nets this out internally, which is why the defect was latent
/// until a stateless operator existed to expose it. The fix feeds the map
/// `raw + old_deficit - new_deficit`.
#[tokio::test]
async fn a_map_over_a_deficit_carrying_source_publishes_the_relation() {
    let flow = IncrementalFlow::new();
    flow.register_view(spec("p", "SELECT k FROM s", i64s(&["k"])))
        .unwrap();

    let one = |k: i64, a: i64| {
        RecordBatch::try_new(
            src(),
            vec![
                Arc::new(Int64Array::from(vec![k])),
                Arc::new(Int64Array::from(vec![a])),
            ],
        )
        .unwrap()
    };

    // Retraction of a row that was never inserted.
    flow.feed("s", DeltaBatch::from_deletes(one(42, 1)).unwrap())
        .unwrap();
    let s1 = flow.step_datafusion().await.unwrap();
    assert!(s1.errored_views.is_empty(), "{:?}", s1.errored_views);
    assert_eq!(
        flow.snapshot("p").unwrap().map_or(0, |b| b.num_rows()),
        0,
        "a retraction that cancels nothing never enters the relation"
    );

    // The matching insertion cancels it. The relation is still empty.
    flow.feed("s", DeltaBatch::from_inserts(one(42, 1)).unwrap())
        .unwrap();
    let s2 = flow.step_datafusion().await.unwrap();
    assert!(s2.errored_views.is_empty(), "{:?}", s2.errored_views);
    assert_eq!(
        flow.snapshot("p").unwrap().map_or(0, |b| b.num_rows()),
        0,
        "the insertion cancels the remembered retraction, so the view stays empty"
    );

    // And an ordinary insertion afterwards still shows up.
    flow.feed("s", DeltaBatch::from_inserts(one(7, 1)).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();
    let snap = flow.snapshot("p").unwrap().unwrap();
    assert_eq!(
        col(&snap, "k"),
        vec![7],
        "the map is still live after the cancellation"
    );
}
