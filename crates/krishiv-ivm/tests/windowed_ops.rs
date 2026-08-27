//! HOP-1 + TOPNK-1 miniatures: the hopping-window fan-out and the keyed
//! top-N, each compared against `force_diff_based` full recompute — the
//! trusted answer by construction, because the oracle executes the SAME
//! registration-rewritten standard SQL through DataFusion whole.
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

fn spec(sql: &str, out: SchemaRef) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: "v".into(),
        body_sql: sql.into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: Vec::new(),
    }
}

fn canonical(batch: &RecordBatch) -> Vec<Vec<Option<i64>>> {
    let mut rows: Vec<Vec<Option<i64>>> = (0..batch.num_rows())
        .map(|r| {
            batch
                .columns()
                .iter()
                .map(|c| {
                    c.as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|a| (!a.is_null(r)).then(|| a.value(r)))
                })
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

struct Pair {
    subject: IncrementalFlow,
    oracle: IncrementalFlow,
}

impl Pair {
    fn new(sql: &str, out: SchemaRef) -> Self {
        let subject = IncrementalFlow::new();
        subject.register_view(spec(sql, out.clone())).unwrap();
        let oracle = IncrementalFlow::new();
        oracle.register_view(spec(sql, out)).unwrap();
        oracle.force_diff_based().unwrap();
        Self { subject, oracle }
    }

    fn feed(&self, batch: RecordBatch, retract: bool) {
        let d = if retract {
            DeltaBatch::from_deletes(batch).unwrap()
        } else {
            DeltaBatch::from_inserts(batch).unwrap()
        };
        self.subject.feed("auction", d.clone()).unwrap();
        self.oracle.feed("auction", d).unwrap();
    }

    async fn tick(&self, label: &str, expect: &[Vec<Option<i64>>]) {
        let s = self.subject.step_datafusion().await.unwrap();
        self.oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{label}: {:?}", s.errored_views);
        let got = canonical(&self.subject.snapshot("v").unwrap().expect("published"));
        let want = canonical(&self.oracle.snapshot("v").unwrap().expect("published"));
        assert_eq!(got, want, "{label}: disagreed with recompute");
        assert_eq!(got, expect.to_vec(), "{label}: exact expectation");
    }

    fn assert_incremental(&self, needle: &str) {
        let (inc, why) = self
            .subject
            .view_plan_classification("v")
            .unwrap()
            .expect("registered");
        assert!(inc, "fell back to DiffBased: {why}");
        assert!(why.contains(needle), "wanted '{needle}' in: {why}");
    }
}

/// HOP-1: `HOP(slide 2, size 4)` fans every row into TWO windows. Tick 2
/// lands a second row sharing one of them (the counts differ per window);
/// tick 3 retracts the first row and its lone window VANISHES while the
/// shared one only decrements.
#[tokio::test(flavor = "multi_thread")]
async fn a_hopping_window_count_maintains_through_the_fanout() {
    let sql = "SELECT seller, window_start, COUNT(*) AS c \
               FROM HOP(TABLE auction, DESCRIPTOR(ts), 2, 4) \
               GROUP BY seller, window_start, window_end";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("window_start", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let p = Pair::new(sql, out);

    // ts=5 lives in [2,6) and [4,8).
    p.feed(auctions(&[(1, 7, 5)]), false);
    p.tick(
        "tick 1",
        &[
            vec![Some(7), Some(2), Some(1)],
            vec![Some(7), Some(4), Some(1)],
        ],
    )
    .await;
    // ts=6 lives in [4,8) and [6,10): window 4 is SHARED, count 2.
    p.feed(auctions(&[(2, 7, 6)]), false);
    p.tick(
        "tick 2",
        &[
            vec![Some(7), Some(2), Some(1)],
            vec![Some(7), Some(4), Some(2)],
            vec![Some(7), Some(6), Some(1)],
        ],
    )
    .await;
    // Retract ts=5: window 2 vanishes, window 4 decrements to 1.
    p.feed(auctions(&[(1, 7, 5)]), true);
    p.tick(
        "tick 3",
        &[
            vec![Some(7), Some(4), Some(1)],
            vec![Some(7), Some(6), Some(1)],
        ],
    )
    .await;
    p.assert_incremental("chain");
}

/// TOPNK-1 (q19's shape): per (seller, window) top-2 by ts. Tick 2 retracts
/// the leader — the row OUTSIDE the published window is promoted, the exact
/// case that forces the operator to hold whole partitions. Tick 3 inserts a
/// new leader and the smallest of the top-2 falls out. The second seller's
/// partition never re-emits.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_top2_promotes_on_retraction_within_the_partition() {
    let sql = "SELECT seller, id, ts FROM TUMBLE(TABLE auction, DESCRIPTOR(ts), 100) \
               GROUP BY seller, window_start, window_end ORDER BY ts DESC LIMIT 2";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let p = Pair::new(sql, out);

    p.feed(
        auctions(&[(1, 7, 5), (2, 7, 3), (3, 7, 1), (4, 8, 2)]),
        false,
    );
    p.tick(
        "tick 1",
        &[
            vec![Some(7), Some(1), Some(5)],
            vec![Some(7), Some(2), Some(3)],
            vec![Some(8), Some(4), Some(2)],
        ],
    )
    .await;
    p.feed(auctions(&[(1, 7, 5)]), true);
    p.tick(
        "tick 2 (promotion)",
        &[
            vec![Some(7), Some(2), Some(3)],
            vec![Some(7), Some(3), Some(1)],
            vec![Some(8), Some(4), Some(2)],
        ],
    )
    .await;
    p.feed(auctions(&[(5, 7, 9)]), false);
    p.tick(
        "tick 3 (new leader)",
        &[
            vec![Some(7), Some(2), Some(3)],
            vec![Some(7), Some(5), Some(9)],
            vec![Some(8), Some(4), Some(2)],
        ],
    )
    .await;
    p.assert_incremental("keyed top-N");
}

/// TOPNK-1 (q18's shape): keep-LAST dedup as top-1 by event time per
/// (seller, window). A newer event DISPLACES the published row — retraction
/// and insertion in one tick — and retracting the newest falls back to the
/// older one.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_top1_keeps_the_latest_and_falls_back_on_retraction() {
    let sql = "SELECT seller, id FROM TUMBLE(TABLE auction, DESCRIPTOR(ts), 100) \
               GROUP BY seller, window_start, window_end ORDER BY ts DESC LIMIT 1";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let p = Pair::new(sql, out);

    p.feed(auctions(&[(1, 7, 5)]), false);
    p.tick("tick 1", &[vec![Some(7), Some(1)]]).await;
    p.feed(auctions(&[(2, 7, 8)]), false);
    p.tick("tick 2 (displaced)", &[vec![Some(7), Some(2)]])
        .await;
    p.feed(auctions(&[(2, 7, 8)]), true);
    p.tick("tick 3 (fallback)", &[vec![Some(7), Some(1)]]).await;
    p.assert_incremental("keyed top-N");
}

/// SESSION-1: gap sessions per seller with COUNT — q11's shape in
/// miniature (gap 5). Tick 2 drops a BRIDGE event between two sessions and
/// they MERGE into one row; tick 3 retracts the bridge and the sessions
/// SPLIT back — the exact inverse. The oracle computes sessions through
/// the LAG cascade whole, so agreement here is agreement on the session
/// semantics, not just on plumbing.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_window_count_merges_and_splits() {
    let sql = "SELECT seller, window_start, window_end, COUNT(*) AS c \
               FROM SESSION(TABLE auction, DESCRIPTOR(ts), 5) \
               GROUP BY seller, window_start, window_end";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let p = Pair::new(sql, out);

    // Sessions: seller 7 events 1,2 → [1,7); event 8 → [8,13); seller 8: [50,55).
    p.feed(
        auctions(&[(1, 7, 1), (2, 7, 2), (3, 7, 8), (4, 8, 50)]),
        false,
    );
    p.tick(
        "tick 1",
        &[
            vec![Some(7), Some(1), Some(7), Some(2)],
            vec![Some(7), Some(8), Some(13), Some(1)],
            vec![Some(8), Some(50), Some(55), Some(1)],
        ],
    )
    .await;
    // ts=4 bridges (2→4 and 4→8 both < 5): ONE session [1,13) of 4 events.
    p.feed(auctions(&[(5, 7, 4)]), false);
    p.tick(
        "tick 2 (merge)",
        &[
            vec![Some(7), Some(1), Some(13), Some(4)],
            vec![Some(8), Some(50), Some(55), Some(1)],
        ],
    )
    .await;
    // Retracting the bridge splits them back.
    p.feed(auctions(&[(5, 7, 4)]), true);
    p.tick(
        "tick 3 (split)",
        &[
            vec![Some(7), Some(1), Some(7), Some(2)],
            vec![Some(7), Some(8), Some(13), Some(1)],
            vec![Some(8), Some(50), Some(55), Some(1)],
        ],
    )
    .await;
    p.assert_incremental("chain");
}
