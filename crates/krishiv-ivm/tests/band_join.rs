//! BAND-1: joins whose ON clause carries non-equi conjuncts (a band), and
//! joins whose SELECT projects the joined relation — both O(Δ) now, both
//! compared against `force_diff_based` full recompute, which is the trusted
//! answer by construction.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Array as _, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn auctions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}
fn persons_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pid", DataType::Int64, false),
        Field::new("city", DataType::Int64, false),
        Field::new("pts", DataType::Int64, false),
    ]))
}
fn auctions(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        auctions_schema(),
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
fn persons(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        persons_schema(),
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

fn spec(sql: &str, out: SchemaRef) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: sql.into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

fn canonical(batch: &RecordBatch) -> Vec<Vec<Option<i64>>> {
    let cols: Vec<&Int64Array> = (0..batch.num_columns())
        .map(|c| {
            batch
                .column(c)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
        })
        .collect();
    let mut rows: Vec<Vec<Option<i64>>> = (0..batch.num_rows())
        .map(|r| {
            cols.iter()
                .map(|c| (!c.is_null(r)).then(|| c.value(r)))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// Feed the same auction/person deltas to both flows and compare. The person
/// rows are chosen so every auction matches a person BY KEY, but only some
/// fall inside the ±100 band — the residual is what separates right from
/// wrong, and DiffBased-vs-DiffBased trivial agreement is excluded by the
/// plan-kind assertion.
async fn band_both_ways(
    sql: &str,
    out: SchemaRef,
    ticks: &[(Option<RecordBatch>, Option<RecordBatch>, bool)],
) -> (RecordBatch, RecordBatch) {
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    for (a, p, retract) in ticks {
        let mk = |b: &RecordBatch| {
            if *retract {
                DeltaBatch::from_deletes(b.clone()).unwrap()
            } else {
                DeltaBatch::from_inserts(b.clone()).unwrap()
            }
        };
        if let Some(a) = a {
            subject.feed("auction", mk(a)).unwrap();
            oracle.feed("auction", mk(a)).unwrap();
        }
        if let Some(p) = p {
            subject.feed("person", mk(p)).unwrap();
            oracle.feed("person", mk(p)).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);
    }

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "a band equi-join must take the O(delta) path: {why}");
    (
        subject.snapshot("v").unwrap().expect("subject published"),
        oracle.snapshot("v").unwrap().expect("oracle published"),
    )
}

/// Equi key + band residual, projected output — the NEXMark q3/q8/q20 shape.
/// Auction 1 matches person 7 inside the band; auction 2 matches person 8 BY
/// KEY ONLY (350 apart) and must not join; auction 3 has no key match at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_band_equi_join_agrees_with_recompute_and_filters_by_the_band() {
    let (got, want) = band_both_ways(
        "SELECT p.city, a.id FROM auction a JOIN person p \
         ON a.seller = p.pid AND a.ts BETWEEN p.pts - 100 AND p.pts + 100",
        Arc::new(Schema::new(vec![
            Field::new("city", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ])),
        &[
            (
                Some(auctions(&[(1, 7, 1000), (2, 8, 2000), (3, 9, 3000)])),
                Some(persons(&[(7, 100, 1050), (8, 200, 2350)])),
                false,
            ),
            // A later person INSIDE auction 2's band — the trace must still
            // hold the auction from tick 1 to produce this join.
            (None, Some(persons(&[(8, 300, 1990)])), false),
        ],
    )
    .await;
    assert_eq!(
        canonical(&got),
        canonical(&want),
        "O(delta) band join disagreed with recompute"
    );
    assert_eq!(
        canonical(&got),
        vec![vec![Some(100), Some(1)], vec![Some(300), Some(2)],],
        "exactly the in-band pairs"
    );
}

/// Retract a row that had joined inside the band: the pair must leave.
#[tokio::test(flavor = "multi_thread")]
async fn retracting_a_banded_row_retracts_the_join_pair() {
    let a1 = auctions(&[(1, 7, 1000)]);
    let (got, want) = band_both_ways(
        "SELECT p.city, a.id FROM auction a JOIN person p \
         ON a.seller = p.pid AND a.ts BETWEEN p.pts - 100 AND p.pts + 100",
        Arc::new(Schema::new(vec![
            Field::new("city", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ])),
        &[
            (Some(a1.clone()), Some(persons(&[(7, 100, 1050)])), false),
            (Some(a1), None, true),
        ],
    )
    .await;
    assert_eq!(canonical(&got), canonical(&want));
    assert_eq!(
        canonical(&got),
        Vec::<Vec<Option<i64>>>::new(),
        "pair retracted"
    );
}
