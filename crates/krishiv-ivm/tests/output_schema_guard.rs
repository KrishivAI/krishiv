//! IVM-AUD-SCHEMA-1: an incremental plan must never publish a relation other
//! than the one its view declared.
//!
//! Every incremental operator emits its own *natural* relation — `DISTINCT`
//! emits whole source rows, the join emits left ++ right-non-key columns — and
//! a `Projection` above it in the logical plan is not part of the operator.
//! `source_of_plan` peels projections, so a projected view resolved to a source
//! and then published the operator's wider relation while reporting
//! `Incremental` with an empty `degraded_views`. A silent wrong answer.
//!
//! The first two tests pin the fix. The last two exist because the obvious
//! wrong fix — refuse every DISTINCT, refuse every join — also makes the first
//! two pass, while deleting the capability.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn spec(name: &str, sql: &str, out: SchemaRef) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: name.into(),
        body_sql: sql.into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn names(schema: &SchemaRef) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

async fn feed_orders(flow: &IncrementalFlow, ids: Vec<i64>, amounts: Vec<i64>) {
    let b = RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(amounts)),
        ],
    )
    .unwrap();
    flow.feed("orders", DeltaBatch::from_inserts(b).unwrap())
        .unwrap();
}

#[tokio::test]
async fn a_projected_distinct_publishes_the_declared_columns_not_the_source() {
    let out = Arc::new(Schema::new(vec![Field::new(
        "customer_id",
        DataType::Int64,
        true,
    )]));
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "d",
        "SELECT DISTINCT customer_id FROM orders",
        out.clone(),
    ))
    .unwrap();
    // Same customer twice, different amounts. Correct answer: one row, one column.
    feed_orders(&flow, vec![7, 7], vec![100, 200]).await;
    let summary = flow.step_datafusion().await.unwrap();

    let snap = flow.snapshot("d").unwrap().expect("view published");
    assert_eq!(
        names(&snap.schema()),
        vec!["customer_id".to_string()],
        "published the operator's source relation instead of the declared columns"
    );
    assert_eq!(
        snap.num_rows(),
        1,
        "DISTINCT customer_id over {{7,7}} is one row"
    );
    // Since DECOMP-2 this shape no longer degrades at all: it decomposes into
    // a map + DISTINCT chain, and the assertions above prove the chain emits
    // the DECLARED relation — which is the contract this file guards. If it
    // ever degrades again the fallback must be visible, so the absence of the
    // silent-wrong-answer is asserted from both directions.
    assert!(
        !summary.degraded_views.iter().any(|v| v == "d"),
        "a projected DISTINCT decomposes (DECOMP-2); reporting it degraded means \
         the chain was lost"
    );
    let (inc, why) = flow
        .view_plan_classification("d")
        .unwrap()
        .expect("registered");
    assert!(inc && why.contains("chain"), "expected a chain plan: {why}");
    assert!(summary.errored_views.is_empty());
}

#[tokio::test]
async fn a_projected_join_publishes_the_declared_columns_not_the_join_relation() {
    let cust = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
    ]));
    let out = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Int64, true),
        Field::new("region", DataType::Int64, true),
    ]));
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "j",
        "SELECT o.amount, c.region FROM orders o JOIN cust c ON o.customer_id = c.customer_id",
        out,
    ))
    .unwrap();
    feed_orders(&flow, vec![1], vec![10]).await;
    flow.feed(
        "cust",
        DeltaBatch::from_inserts(
            RecordBatch::try_new(
                cust,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![5])),
                ],
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    let snap = flow.snapshot("j").unwrap().expect("view published");
    assert_eq!(
        names(&snap.schema()),
        vec!["amount".to_string(), "region".to_string()],
        "published the join's natural relation (incl. the key) instead of the declared columns"
    );
    assert!(
        summary.degraded_views.iter().any(|v| v == "j"),
        "got {:?}",
        summary.degraded_views
    );
    assert!(summary.errored_views.is_empty());
}

#[tokio::test]
async fn distinct_star_stays_incremental() {
    let out = orders_schema();
    let flow = IncrementalFlow::new();
    flow.register_view(spec("ds", "SELECT DISTINCT * FROM orders", out))
        .unwrap();
    feed_orders(&flow, vec![7, 7], vec![100, 100]).await;
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow
        .view_plan_classification("ds")
        .unwrap()
        .expect("registered");
    assert!(
        incremental,
        "DISTINCT * emits exactly the source relation, so it must keep its O(delta) plan: {reason}"
    );
    assert!(summary.degraded_views.is_empty());
    let snap = flow.snapshot("ds").unwrap().expect("published");
    assert_eq!(snap.num_rows(), 1, "two identical rows dedup to one");
    assert_eq!(snap.num_columns(), 2);
}

#[tokio::test]
async fn an_unprojected_join_stays_incremental() {
    let cust = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
    ]));
    // The join operator emits all left columns plus the right's non-key columns.
    let out = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
        Field::new("region", DataType::Int64, true),
    ]));
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "js",
        "SELECT * FROM orders o JOIN cust c ON o.customer_id = c.customer_id",
        out,
    ))
    .unwrap();
    feed_orders(&flow, vec![1], vec![10]).await;
    flow.feed(
        "cust",
        DeltaBatch::from_inserts(
            RecordBatch::try_new(
                cust,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![5])),
                ],
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    let (incremental, reason) = flow
        .view_plan_classification("js")
        .unwrap()
        .expect("registered");
    assert!(
        incremental,
        "an unprojected equi-join emits exactly what the operator produces, so it \
         must keep its O(delta) plan: {reason}"
    );
    assert!(summary.degraded_views.is_empty());
}

/// A view may declare `Utf8View` for a column the source physically holds as
/// `Utf8` — that is what DataFusion 54 plans and what the coordinator ships in
/// an attach fragment — while the operator emits the source's encoding. Those
/// are the same logical column and the guard must not reject them.
///
/// The first cut of the guard compared data types byte-for-byte and broke
/// `krishiv-executor`'s `resident_group_by_aggregate_first_tick_emits_delta`:
/// a correct `GROUP BY` was reported `OutputSchemaMismatch` with the message
/// `["region","total"] but the view declares ["region","total"]`, because only
/// the encoding differed. Pinned here, in the crate that owns the comparison,
/// rather than relying on a test one crate away to notice.
#[tokio::test]
async fn a_utf8view_declaration_over_a_utf8_source_stays_incremental() {
    use arrow::array::StringArray;

    let src = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let out = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8View, true),
        Field::new("total", DataType::Int64, true),
    ]));
    let flow = IncrementalFlow::new();
    flow.register_view(spec(
        "rev",
        "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
        out,
    ))
    .unwrap();
    let batch = RecordBatch::try_new(
        src,
        vec![
            Arc::new(StringArray::from(vec!["us", "eu", "us"])),
            Arc::new(Int64Array::from(vec![100, 50, 25])),
        ],
    )
    .unwrap();
    flow.feed("orders", DeltaBatch::from_inserts(batch).unwrap())
        .unwrap();
    let summary = flow.step_datafusion().await.unwrap();

    assert!(
        summary.errored_views.is_empty(),
        "a Utf8View declaration over a Utf8 source is an encoding difference, not \
         a wrong relation; got {:?}",
        summary.errored_views
    );
    let (incremental, reason) = flow
        .view_plan_classification("rev")
        .unwrap()
        .expect("registered");
    assert!(incremental, "must keep its O(delta) plan: {reason}");
    assert_eq!(
        flow.snapshot("rev").unwrap().expect("published").num_rows(),
        2
    );
}
