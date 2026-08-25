//! IVM-AUD-DEC-1 against the oracle: exact decimal aggregates must agree with
//! DataFusion, not merely with themselves.
//!
//! The operator-level tests in `krishiv-delta` pin the arithmetic this engine
//! *intends*. They cannot tell whether that intent matches SQL — the result
//! type of `SUM(Decimal128(p,s))`, and whether `AVG` rounds or truncates, are
//! DataFusion's decisions, and an incremental view that disagrees with them is
//! wrong no matter how exact its accumulator is. `force_diff_based` re-runs the
//! view SQL through DataFusion, so these compare against the real answer.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Decimal128Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

const P: u8 = 20;
const S: i8 = 2;

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(P, S), true),
    ]))
}

/// `rows` carry **unscaled** amounts at scale `S`.
fn sales(rows: &[(i64, i128)]) -> RecordBatch {
    let dec = Decimal128Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
        .with_precision_and_scale(P, S)
        .unwrap();
    RecordBatch::try_new(
        sales_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(dec),
        ],
    )
    .unwrap()
}

/// Render rows as text and sort — neither path promises an order, and the
/// rendering carries the decimal's scale, so a same-magnitude-wrong-scale
/// answer is a visible difference rather than a silent pass.
fn canonical(batch: &RecordBatch) -> Vec<Vec<String>> {
    let opts = FormatOptions::default();
    let fmts: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
        .collect();
    let mut rows: Vec<Vec<String>> = (0..batch.num_rows())
        .map(|r| fmts.iter().map(|f| f.value(r).to_string()).collect())
        .collect();
    rows.sort();
    rows
}

async fn both_ways(
    sql: &str,
    out: SchemaRef,
    batches: &[Vec<(i64, i128)>],
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
        let d = DeltaBatch::from_inserts(sales(rows)).unwrap();
        incr.feed("sales", d.clone()).unwrap();
        full.feed("sales", d).unwrap();
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

/// Amounts chosen so the exact total needs more than f64's 53 bits: each is
/// ~1.2e16 unscaled, and the sum's last digits are the whole point.
fn two_batches() -> Vec<Vec<(i64, i128)>> {
    vec![
        vec![
            (10, 12_345_678_901_234_567),
            (10, 76_543_210_987_654_321),
            (20, 1),
        ],
        vec![
            (10, 11_111_111_111_111_111),
            (20, 99_999_999_999_999_999),
            (20, 3),
        ],
    ]
}

#[tokio::test]
async fn decimal_grouped_sum_agrees_with_full_recompute() {
    let out = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, true),
        // SUM widens precision by 10 and keeps the scale.
        Field::new("total", DataType::Decimal128(30, 2), true),
    ]));
    let (a, b, incremental) = both_ways(
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        out,
        &two_batches(),
    )
    .await;
    // Answer first: asserting the plan kind before the values would let the
    // classification short-circuit a disagreement.
    assert_eq!(
        canonical(&a),
        canonical(&b),
        "exact decimal SUM disagreed with DataFusion"
    );
    assert!(incremental, "this shape must take the O(delta) path");
}

#[tokio::test]
async fn decimal_avg_agrees_with_full_recompute() {
    let out = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, true),
        // AVG adds 4 to both precision and scale.
        Field::new("mean", DataType::Decimal128(24, 6), true),
    ]));
    let (a, b, incremental) = both_ways(
        "SELECT region, AVG(amount) AS mean FROM sales GROUP BY region",
        out,
        &two_batches(),
    )
    .await;
    // None of these divide evenly, so this compares the *rounding rule* —
    // truncation toward zero — and not just the magnitude.
    assert_eq!(
        canonical(&a),
        canonical(&b),
        "decimal AVG disagreed with DataFusion's rounding"
    );
    assert!(incremental, "this shape must take the O(delta) path");
}

#[tokio::test]
async fn decimal_min_max_agrees_with_full_recompute() {
    let out = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, true),
        Field::new("lo", DataType::Decimal128(P, S), true),
        Field::new("hi", DataType::Decimal128(P, S), true),
    ]));
    let (a, b, incremental) = both_ways(
        "SELECT region, MIN(amount) AS lo, MAX(amount) AS hi FROM sales GROUP BY region",
        out,
        &two_batches(),
    )
    .await;
    assert_eq!(canonical(&a), canonical(&b));
    assert!(incremental, "this shape must take the O(delta) path");
}

#[tokio::test]
async fn decimal_sum_survives_retraction_against_the_oracle() {
    let out = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, true),
        Field::new("total", DataType::Decimal128(30, 2), true),
    ]));
    let sql = "SELECT region, SUM(amount) AS total FROM sales GROUP BY region";
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

    let inserted = sales(&[(10, 12_345_678_901_234_567), (10, 55_555_555_555_555_555)]);
    for d in [
        DeltaBatch::from_inserts(inserted.clone()).unwrap(),
        // Retract the first row only — the surviving total must be exact, which
        // is where an f64 accumulator's error would show up as a residue.
        DeltaBatch::from_deletes(sales(&[(10, 12_345_678_901_234_567)])).unwrap(),
    ] {
        incr.feed("sales", d.clone()).unwrap();
        full.feed("sales", d).unwrap();
        incr.step_datafusion().await.unwrap();
        full.step_datafusion().await.unwrap();
    }

    let a = incr.snapshot("v").unwrap().expect("published");
    let b = full.snapshot("v").unwrap().expect("published");
    assert_eq!(canonical(&a), canonical(&b));
    assert_eq!(
        canonical(&a),
        vec![vec!["10".to_string(), "555555555555555.55".to_string()]]
    );
    // Without this, the test passes when the decimal aggregate is refused: both
    // flows fall back to DiffBased, agree with each other, and prove nothing
    // about the incremental path. The value assertions above guard the
    // arithmetic; this one guards that the arithmetic is the one being used.
    assert!(
        incr.view_plan_classification("v")
            .unwrap()
            .expect("registered")
            .0,
        "retraction must be maintained incrementally, not recomputed"
    );
}
