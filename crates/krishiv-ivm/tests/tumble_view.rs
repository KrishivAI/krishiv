//! WINDOW-1: a `TUMBLE(TABLE …)` view registered VERBATIM maintains O(Δ) —
//! the TVF rewrites at registration into a derived table computing
//! `window_start`/`window_end`, and the chain machinery does the rest.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Array as _, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn bid_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}
fn bids(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        bid_schema(),
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

/// Windowed count per auction, `window_start` selected so the windows are
/// visible in the output. Rows land in two distinct 10s windows; a late row
/// for the FIRST window arrives on tick 2 and must update that window's
/// count — the materialized-view semantics of a tumble.
#[tokio::test(flavor = "multi_thread")]
async fn a_tumble_view_registered_verbatim_maintains_incrementally() {
    let sql = "SELECT auction, window_start, COUNT(*) AS c \
               FROM TUMBLE(TABLE bid, DESCRIPTOR(ts), 10000) \
               GROUP BY auction, window_start, window_end";
    let out = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("window_start", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]));

    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    let ticks = [
        bids(&[
            (1, 10, 1_000),
            (1, 20, 2_000),
            (2, 30, 3_000),
            (1, 40, 15_000),
        ]),
        // Late row for the first window: its count must go 2 -> 3.
        bids(&[(1, 50, 9_999)]),
    ];
    for batch in &ticks {
        let d = DeltaBatch::from_inserts(batch.clone()).unwrap();
        subject.feed("bid", d.clone()).unwrap();
        oracle.feed("bid", d).unwrap();
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);
    }

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "a tumble view must take the O(delta) path: {why}");
    assert!(why.contains("chain"), "expected the chain: {why}");

    let got = subject.snapshot("v").unwrap().expect("subject published");
    let want = oracle.snapshot("v").unwrap().expect("oracle published");
    assert_eq!(
        canonical(&got),
        canonical(&want),
        "disagreed with recompute"
    );
    assert_eq!(
        canonical(&got),
        vec![
            vec![Some(1), Some(0), Some(3)],
            vec![Some(1), Some(10000), Some(1)],
            vec![Some(2), Some(0), Some(1)],
        ],
        "per-window counts, late row included in its window"
    );
}

/// UINT-1 + WINDOW-1, the NEXMark q7 shape: a global MAX over an **unsigned**
/// column per window. DataFusion types MAX(UInt64) as UInt64, which the
/// aggregate's declared-type gate refused wholesale — every unsigned NEXMark
/// aggregate fell to DiffBased for the type alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_tumble_max_over_an_unsigned_column_maintains_incrementally() {
    use arrow::array::UInt64Array;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("price", DataType::UInt64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let rows = |v: &[(u64, i64)]| -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(v.iter().map(|r| r.0).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(v.iter().map(|r| r.1).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    };
    let sql = "SELECT window_start, MAX(price) AS final \
               FROM TUMBLE(TABLE bid, DESCRIPTOR(ts), 10000) \
               GROUP BY window_start, window_end";
    let out = Arc::new(Schema::new(vec![
        Field::new("window_start", DataType::Int64, true),
        Field::new("final", DataType::UInt64, true),
    ]));

    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    for batch in [
        rows(&[(100, 1_000), (250, 2_000), (75, 15_000)]),
        rows(&[(300, 9_000)]),
    ] {
        let d = DeltaBatch::from_inserts(batch).unwrap();
        subject.feed("bid", d.clone()).unwrap();
        oracle.feed("bid", d).unwrap();
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);
    }

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc && why.contains("chain"), "expected the chain: {why}");

    let got = subject.snapshot("v").unwrap().expect("published");
    let want = oracle.snapshot("v").unwrap().expect("oracle published");
    // Text canonical — the columns are mixed Int64/UInt64.
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let text = |b: &RecordBatch| -> Vec<Vec<String>> {
        let opts = FormatOptions::default();
        let fmts: Vec<ArrayFormatter> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
            .collect();
        let mut rows: Vec<Vec<String>> = (0..b.num_rows())
            .map(|r| fmts.iter().map(|f| f.value(r).to_string()).collect())
            .collect();
        rows.sort();
        rows
    };
    assert_eq!(text(&got), text(&want), "disagreed with recompute");
    assert_eq!(
        text(&got),
        vec![
            vec!["0".to_string(), "300".to_string()],
            vec!["10000".to_string(), "75".to_string()],
        ]
    );
}
