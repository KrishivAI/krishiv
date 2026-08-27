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

/// JOIN-2: a comma join is an equi-join spelled in the WHERE — the TPC-H
/// idiom (`FROM orders, lineitem WHERE o_orderkey = l_orderkey`). The
/// cross-side equality becomes the trace key and the cross-side band becomes
/// the residual, exactly as if both were in an ON clause.
#[tokio::test(flavor = "multi_thread")]
async fn a_comma_join_with_a_where_band_agrees_with_recompute() {
    let (got, want) = band_both_ways(
        "SELECT p.city, a.id FROM auction a, person p \
         WHERE a.seller = p.pid AND a.ts BETWEEN p.pts - 100 AND p.pts + 100",
        Arc::new(Schema::new(vec![
            Field::new("city", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ])),
        &[(
            Some(auctions(&[(1, 7, 1000), (2, 8, 2000)])),
            Some(persons(&[(7, 100, 1050), (8, 200, 2350)])),
            false,
        )],
    )
    .await;
    assert_eq!(canonical(&got), canonical(&want));
    assert_eq!(
        canonical(&got),
        vec![vec![Some(100), Some(1)]],
        "only the in-band pair joins; the key-only match is excluded"
    );
}

/// DECORR-1 + SEMI-1: a correlated EXISTS decorrelates (via DataFusion's own
/// rule) to a LeftSemi join the incremental operator maintains. The person
/// arriving a tick late is the crossing: the auction must ENTER the relation
/// when its first match appears, and LEAVE when the last one retracts.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_exists_maintains_as_a_semi_join() {
    let sql = "SELECT id, ts FROM auction a WHERE EXISTS \
               (SELECT 1 FROM person p WHERE p.pid = a.seller)";
    let out = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
    ]));
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    let p7 = persons(&[(7, 100, 0)]);
    for (a, p, retract) in [
        // Auctions for sellers 7 and 8; person 7 exists: auction 1 is in.
        (
            Some(auctions(&[(1, 7, 10), (2, 8, 20)])),
            Some(p7.clone()),
            false,
        ),
        // Person 7 leaves: auction 1 leaves (crossing down).
        (None, Some(p7.clone()), true),
        // Person 7 returns: auction 1 re-enters (crossing up).
        (None, Some(p7), false),
    ] {
        let mk = |b: &RecordBatch| {
            if retract {
                DeltaBatch::from_deletes(b.clone()).unwrap()
            } else {
                DeltaBatch::from_inserts(b.clone()).unwrap()
            }
        };
        if let Some(a) = a {
            subject.feed("auction", mk(&a)).unwrap();
            oracle.feed("auction", mk(&a)).unwrap();
        }
        if let Some(p) = p {
            subject.feed("person", mk(&p)).unwrap();
            oracle.feed("person", mk(&p)).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);

        let got = subject.snapshot("v").unwrap().expect("published");
        let want = oracle.snapshot("v").unwrap().expect("published");
        assert_eq!(
            canonical(&got),
            canonical(&want),
            "semi join disagreed with recompute"
        );
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc,
        "a decorrelated EXISTS must take the O(delta) path: {why}"
    );
    assert_eq!(
        canonical(&subject.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(1), Some(10)]],
        "auction 1 re-entered on the crossing up; auction 2 never matched"
    );
}

/// The NOT EXISTS mirror: a LeftAnti join.
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_not_exists_maintains_as_an_anti_join() {
    let sql = "SELECT id, ts FROM auction a WHERE NOT EXISTS \
               (SELECT 1 FROM person p WHERE p.pid = a.seller)";
    let out = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
    ]));
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    for (a, p) in [
        (
            Some(auctions(&[(1, 7, 10), (2, 8, 20)])),
            Some(persons(&[(7, 100, 0)])),
        ),
        // Person 8 arrives later: auction 2 leaves the anti relation.
        (None, Some(persons(&[(8, 200, 0)]))),
    ] {
        if let Some(a) = a {
            let d = DeltaBatch::from_inserts(a).unwrap();
            subject.feed("auction", d.clone()).unwrap();
            oracle.feed("auction", d).unwrap();
        }
        if let Some(p) = p {
            let d = DeltaBatch::from_inserts(p).unwrap();
            subject.feed("person", d.clone()).unwrap();
            oracle.feed("person", d).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);
        let got = subject.snapshot("v").unwrap().expect("published");
        let want = oracle.snapshot("v").unwrap().expect("published");
        assert_eq!(canonical(&got), canonical(&want));
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc,
        "a decorrelated NOT EXISTS must take the O(delta) path: {why}"
    );
    assert_eq!(
        canonical(&subject.snapshot("v").unwrap().unwrap()),
        Vec::<Vec<Option<i64>>>::new(),
        "every auction gained a match; the anti relation is empty"
    );
}

/// SEMI-2: an aggregate ABOVE a correlated EXISTS — TPC-H q4's shape in
/// miniature. The chain's leaf is a LeftSemi join whose right side is a
/// membership relation, admitted by the decomposer's guard through the same
/// resolver the join builder uses. The person retract must pull seller 7's
/// auctions out of the COUNT through the chain (crossing down), and the
/// reinsert must restore them (crossing up) — each tick compared against
/// full recompute.
#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_above_exists_maintains_as_a_chain() {
    let sql = "SELECT seller, COUNT(*) AS n FROM auction a WHERE EXISTS \
               (SELECT 1 FROM person p WHERE p.pid = a.seller) GROUP BY seller";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    let p7 = persons(&[(7, 100, 0)]);
    for (a, p, retract) in [
        // Two auctions by seller 7, one by seller 8; only person 7 exists.
        (
            Some(auctions(&[(1, 7, 10), (2, 7, 11), (3, 8, 20)])),
            Some(p7.clone()),
            false,
        ),
        // Person 7 leaves: both of seller 7's auctions leave the COUNT.
        (None, Some(p7.clone()), true),
        // Person 7 returns: the group comes back at n = 2.
        (None, Some(p7), false),
    ] {
        let mk = |b: &RecordBatch| {
            if retract {
                DeltaBatch::from_deletes(b.clone()).unwrap()
            } else {
                DeltaBatch::from_inserts(b.clone()).unwrap()
            }
        };
        if let Some(a) = a {
            subject.feed("auction", mk(&a)).unwrap();
            oracle.feed("auction", mk(&a)).unwrap();
        }
        if let Some(p) = p {
            subject.feed("person", mk(&p)).unwrap();
            oracle.feed("person", mk(&p)).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);

        let got = subject.snapshot("v").unwrap().expect("published");
        let want = oracle.snapshot("v").unwrap().expect("published");
        assert_eq!(
            canonical(&got),
            canonical(&want),
            "the chain over a semi join disagreed with recompute"
        );
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc,
        "an aggregate above EXISTS must take the O(delta) path: {why}"
    );
    assert!(
        why.contains("chain"),
        "incremental but not via the chain: {why}"
    );
    assert_eq!(
        canonical(&subject.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(7), Some(2)]],
        "seller 7 re-entered at n = 2 on the crossing up; seller 8 never matched"
    );
}

/// SIDE-1: an aggregate above `IN (SELECT … GROUP BY … HAVING …)` — TPC-H
/// q18's shape in miniature. The membership side is itself an AGGREGATE,
/// maintained as the chain's SIDE fold: a seller is a member while they hold
/// at least two person rows, and the side aggregate's retract+insert pairs
/// net to a single signed row exactly when a pid crosses the HAVING
/// threshold — which is precisely a semi-join membership crossing. Tick 2
/// retracts one of pid 7's two rows (count 2 → 1, leaves the set, seller 7's
/// auctions leave the COUNT); tick 3 restores it. Every tick is compared
/// against full recompute.
#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_above_a_having_membership_side_maintains_as_a_chain() {
    let sql = "SELECT seller, COUNT(*) AS n FROM auction a WHERE seller IN \
               (SELECT pid FROM person GROUP BY pid HAVING COUNT(*) > 1) GROUP BY seller";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    let second_p7 = persons(&[(7, 200, 1)]);
    for (a, p, retract) in [
        // Sellers 7 (two auctions) and 8 (one); pid 7 has TWO person rows
        // (a member), pid 8 has one (not a member).
        (
            Some(auctions(&[(1, 7, 10), (2, 7, 11), (3, 8, 20)])),
            Some(persons(&[(7, 100, 0), (7, 200, 1), (8, 300, 0)])),
            false,
        ),
        // pid 7 drops to one row: count crosses 2 -> 1, membership lost.
        (None, Some(second_p7.clone()), true),
        // And back: membership regained, the group returns at n = 2.
        (None, Some(second_p7), false),
    ] {
        let mk = |b: &RecordBatch| {
            if retract {
                DeltaBatch::from_deletes(b.clone()).unwrap()
            } else {
                DeltaBatch::from_inserts(b.clone()).unwrap()
            }
        };
        if let Some(a) = a {
            subject.feed("auction", mk(&a)).unwrap();
            oracle.feed("auction", mk(&a)).unwrap();
        }
        if let Some(p) = p {
            subject.feed("person", mk(&p)).unwrap();
            oracle.feed("person", mk(&p)).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);

        let got = subject.snapshot("v").unwrap().expect("published");
        let want = oracle.snapshot("v").unwrap().expect("published");
        assert_eq!(
            canonical(&got),
            canonical(&want),
            "the chain with a HAVING membership side disagreed with recompute"
        );
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc,
        "an aggregate above a HAVING membership must take the O(delta) path: {why}"
    );
    assert!(
        why.contains("chain"),
        "incremental but not via the chain: {why}"
    );
    assert_eq!(
        canonical(&subject.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(7), Some(2)]],
        "seller 7 re-entered at n = 2; seller 8's pid never held two rows"
    );
}

/// SIDE-1's checkpoint framing (CHN2): a side-bearing chain must restore its
/// SIDE hop state losslessly. The side source carries a genuinely duplicate
/// person row — pid 7 is a member only because its COUNT(*) is 2 via
/// duplicates, which the materialized source snapshot (a set) cannot
/// represent. After restore, retracting ONE copy must cross membership down
/// and empty the view; a restore that fell back to snapshot seeding holds
/// count 1, sees no crossing, and leaves the restored snapshot published —
/// a wrong answer wearing a healthy view's clothes.
#[tokio::test(flavor = "multi_thread")]
async fn a_side_bearing_chain_checkpoints_and_restores_losslessly() {
    let sql = "SELECT seller, COUNT(*) AS n FROM auction a WHERE seller IN \
               (SELECT pid FROM person GROUP BY pid HAVING COUNT(*) > 1) GROUP BY seller";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let mk_spec = || spec(sql, out.clone());

    let flow = IncrementalFlow::new();
    flow.register_view(mk_spec()).unwrap();
    flow.feed(
        "auction",
        DeltaBatch::from_inserts(auctions(&[(1, 7, 10), (2, 7, 11)])).unwrap(),
    )
    .unwrap();
    // The SAME person row twice — the duplicate is the point.
    flow.feed(
        "person",
        DeltaBatch::from_inserts(persons(&[(7, 100, 0), (7, 100, 0)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    let (inc, why) = flow
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc && why.contains("chain"), "not a chain: {why}");
    assert_eq!(
        canonical(&flow.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(7), Some(2)]],
        "pid 7 is a member through its duplicate rows"
    );

    let blob = flow.checkpoint_full().unwrap();
    let restored = IncrementalFlow::new();
    restored.register_view(mk_spec()).unwrap();
    restored.restore_full(&blob).unwrap();

    // One copy retracts: the TRUE count crosses 2 -> 1, membership is lost,
    // and the group must leave the view. Only the CHN2-framed side aggregate
    // state knows the count was 2.
    for f in [&restored, &flow] {
        f.feed(
            "person",
            DeltaBatch::from_deletes(persons(&[(7, 100, 0)])).unwrap(),
        )
        .unwrap();
        f.step_datafusion().await.unwrap();
    }
    let restored_rows = canonical(&restored.snapshot("v").unwrap().unwrap());
    let continuous_rows = canonical(&flow.snapshot("v").unwrap().unwrap());
    assert_eq!(
        restored_rows,
        Vec::<Vec<Option<i64>>>::new(),
        "membership crossed down through the restored side state"
    );
    assert_eq!(
        restored_rows, continuous_rows,
        "restore changed nothing but the process"
    );
}

/// SIDE-1's seed path: when restored operator state cannot be adopted
/// (IVM-AUD-STALE-1 — here forced by re-registering the view with
/// cosmetically different SQL, which changes the logic fingerprint while
/// planning identically), the chain re-seeds from source snapshots — and the
/// SIDE must seed from ITS source's snapshot, then feed the join's right
/// trace. pid 7's membership comes from two DISTINCT person rows (a set
/// snapshot represents them faithfully), so the seeded side aggregate holds
/// count 2; the first post-restore tick retracts one row, membership crosses
/// down, and the view must empty. A seed that never wires the side into the
/// join leaves the restored snapshot published unchanged — frozen wrong.
#[tokio::test(flavor = "multi_thread")]
async fn a_side_bearing_chain_reseeds_from_snapshots_when_state_is_stale() {
    let sql = "SELECT seller, COUNT(*) AS n FROM auction a WHERE seller IN \
               (SELECT pid FROM person GROUP BY pid HAVING COUNT(*) > 1) GROUP BY seller";
    // Same query, one extra space: identical plan, different fingerprint.
    let sql_variant = "SELECT seller,  COUNT(*) AS n FROM auction a WHERE seller IN \
               (SELECT pid FROM person GROUP BY pid HAVING COUNT(*) > 1) GROUP BY seller";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));

    let flow = IncrementalFlow::new();
    flow.register_view(spec(sql, out.clone())).unwrap();
    flow.feed(
        "auction",
        DeltaBatch::from_inserts(auctions(&[(1, 7, 10), (2, 7, 11)])).unwrap(),
    )
    .unwrap();
    // Two DISTINCT rows for pid 7 — membership survives the set snapshot.
    flow.feed(
        "person",
        DeltaBatch::from_inserts(persons(&[(7, 100, 0), (7, 200, 1)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(
        canonical(&flow.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(7), Some(2)]],
    );

    let blob = flow.checkpoint_full().unwrap();
    let restored = IncrementalFlow::new();
    restored.register_view(spec(sql_variant, out)).unwrap();
    restored.restore_full(&blob).unwrap();

    // The mismatched fingerprint discarded state AND snapshot, so the first
    // post-restore tick runs against operators seeded from the restored
    // SOURCE snapshots alone. Feeding a NEW auction for seller 7 makes the
    // seeding observable POSITIVELY: the auction joins only if the side's
    // seeded aggregate (count 2 > 1) put pid 7 in the join's membership
    // trace — an unseeded side leaves the view empty instead.
    for f in [&restored, &flow] {
        f.feed(
            "auction",
            DeltaBatch::from_inserts(auctions(&[(4, 7, 12)])).unwrap(),
        )
        .unwrap();
        let s = f.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);
    }
    let (inc, why) = restored
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc && why.contains("chain"),
        "not a chain after reseed: {why}"
    );
    assert_eq!(
        canonical(&restored.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(7), Some(3)]],
        "the new auction joined through the SEEDED side membership"
    );
    assert_eq!(
        canonical(&restored.snapshot("v").unwrap().unwrap()),
        canonical(&flow.snapshot("v").unwrap().unwrap()),
        "reseeded and continuous flows agree"
    );

    // And the crossing still works on the reseeded state: one person copy
    // retracts, count crosses 2 -> 1, the whole group leaves.
    for f in [&restored, &flow] {
        f.feed(
            "person",
            DeltaBatch::from_deletes(persons(&[(7, 100, 0)])).unwrap(),
        )
        .unwrap();
        f.step_datafusion().await.unwrap();
    }
    assert_eq!(
        canonical(&restored.snapshot("v").unwrap().unwrap()),
        Vec::<Vec<Option<i64>>>::new(),
        "membership crossed down through the reseeded side state"
    );
    assert_eq!(
        canonical(&restored.snapshot("v").unwrap().unwrap()),
        canonical(&flow.snapshot("v").unwrap().unwrap()),
    );
}

/// SIDE-2 + OUTER-1: an aggregate above a correlated SCALAR subquery — TPC-H
/// q17's shape in miniature. Decorrelation produces a LEFT OUTER join against
/// an avg-per-seller side whose value is EMITTED and compared (`ts < avg`);
/// the comparison rejects the padding, DataFusion's own elimination proves
/// the join INNER, and the side maintains as the chain's side fold. Tick 2
/// shifts seller 7's average UP so two previously-excluded auctions enter
/// the COUNT (a side VALUE update — retract+insert of the side row fans out
/// through the join); tick 3 retracts the shifting row and they leave again.
/// Every tick is compared against full recompute.
#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_above_a_scalar_avg_side_maintains_as_a_chain() {
    let sql = "SELECT seller, COUNT(*) AS n FROM auction a WHERE ts < \
               (SELECT AVG(ts) FROM auction a2 WHERE a2.seller = a.seller) GROUP BY seller";
    let out = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    let shifting = auctions(&[(5, 7, 100)]);
    let expectations: [&[(i64, i64)]; 3] = [
        // avg(7) = 20 → only ts=10 is below; avg(8) = 5 → 5 < 5 is false.
        &[(7, 1)],
        // +ts=100: avg(7) = 40 → ts 10, 20, 30 all below (crossing IN).
        &[(7, 3)],
        // retracted: back to avg 20 (crossing OUT).
        &[(7, 1)],
    ];
    for (i, (a, retract)) in [
        (
            Some(auctions(&[(1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 8, 5)])),
            false,
        ),
        (Some(shifting.clone()), false),
        (Some(shifting), true),
    ]
    .into_iter()
    .enumerate()
    {
        if let Some(a) = a {
            let d = if retract {
                DeltaBatch::from_deletes(a).unwrap()
            } else {
                DeltaBatch::from_inserts(a).unwrap()
            };
            subject.feed("auction", d.clone()).unwrap();
            oracle.feed("auction", d).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);

        let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
        let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
        assert_eq!(
            got, want,
            "tick {i}: scalar-side chain disagreed with recompute"
        );
        let expect: Vec<Vec<Option<i64>>> = expectations[i]
            .iter()
            .map(|(s, n)| vec![Some(*s), Some(*n)])
            .collect();
        assert_eq!(
            got, expect,
            "tick {i}: crossing did not land where the math says"
        );
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc,
        "a scalar-aggregate side must take the O(delta) path: {why}"
    );
    assert!(
        why.contains("chain"),
        "incremental but not via the chain: {why}"
    );
}

/// SIDE-3: a scalar side that is itself a JOIN RUN — q2's shape in
/// miniature. The side computes `MIN(a2.ts)` per seller over auction ⋈
/// person (only sellers present in person have a group at all), and the
/// spine keys on BOTH the correlation (seller) and the scalar equality
/// itself (`a.ts = min`). Tick 2 inserts a lower-ts auction: the side's min
/// SHIFTS, so the old minimum's auction leaves the view and the new one
/// enters — one side value update fanning both directions. Tick 3 retracts
/// it and the original returns. Every tick against full recompute, with
/// exact expected values.
#[tokio::test(flavor = "multi_thread")]
async fn a_scalar_side_that_is_a_join_run_maintains_as_a_chain() {
    let sql = "SELECT a.id, a.ts FROM auction a WHERE a.ts = \
               (SELECT MIN(a2.ts) FROM auction a2, person p \
                WHERE a2.seller = p.pid AND a2.seller = a.seller)";
    let out = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let subject = IncrementalFlow::new();
    subject.register_view(spec(sql, out.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(sql, out)).unwrap();
    oracle.force_diff_based().unwrap();

    let lower = auctions(&[(3, 7, 5)]);
    let expectations: [&[(i64, i64)]; 3] = [
        // min(seller 7) = 10 → auction 1; seller 8 has no person row, so the
        // side holds no group for it and auction 4 never matches.
        &[(1, 10)],
        // min shifts to 5: auction 3 in, auction 1 out.
        &[(3, 5)],
        // retracted: min back to 10, auction 1 returns.
        &[(1, 10)],
    ];
    for (i, (a, p, retract)) in [
        (
            Some(auctions(&[(1, 7, 10), (2, 7, 20), (4, 8, 1)])),
            Some(persons(&[(7, 100, 0)])),
            false,
        ),
        (Some(lower.clone()), None, false),
        (Some(lower), None, true),
    ]
    .into_iter()
    .enumerate()
    {
        let mk = |b: &RecordBatch| {
            if retract {
                DeltaBatch::from_deletes(b.clone()).unwrap()
            } else {
                DeltaBatch::from_inserts(b.clone()).unwrap()
            }
        };
        if let Some(a) = a {
            subject.feed("auction", mk(&a)).unwrap();
            oracle.feed("auction", mk(&a)).unwrap();
        }
        if let Some(p) = p {
            subject.feed("person", mk(&p)).unwrap();
            oracle.feed("person", mk(&p)).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);

        let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
        let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
        assert_eq!(
            got, want,
            "tick {i}: join-run side disagreed with recompute"
        );
        let expect: Vec<Vec<Option<i64>>> = expectations[i]
            .iter()
            .map(|(id, ts)| vec![Some(*id), Some(*ts)])
            .collect();
        assert_eq!(got, expect, "tick {i}: the shifting min did not land");
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "a join-run side must take the O(delta) path: {why}");
    assert!(
        why.contains("chain"),
        "incremental but not via the chain: {why}"
    );
}

/// SIDE-3's seed path: a JOIN-bearing side must seed BOTH its join's traces
/// from source snapshots. Forced through the STALE-1 reseed (fingerprint
/// mismatch discards state and snapshot); the post-restore tick feeds a new
/// auction at exactly the seeded minimum — it joins only if the side's
/// aggregate AND its join traces were seeded, a positive observable.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_run_side_chain_reseeds_from_snapshots() {
    let sql = "SELECT a.id, a.ts FROM auction a WHERE a.ts = \
               (SELECT MIN(a2.ts) FROM auction a2, person p \
                WHERE a2.seller = p.pid AND a2.seller = a.seller)";
    let sql_variant = "SELECT a.id, a.ts  FROM auction a WHERE a.ts = \
               (SELECT MIN(a2.ts) FROM auction a2, person p \
                WHERE a2.seller = p.pid AND a2.seller = a.seller)";
    let out = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));

    let flow = IncrementalFlow::new();
    flow.register_view(spec(sql, out.clone())).unwrap();
    flow.feed(
        "auction",
        DeltaBatch::from_inserts(auctions(&[(1, 7, 10), (2, 7, 20)])).unwrap(),
    )
    .unwrap();
    flow.feed(
        "person",
        DeltaBatch::from_inserts(persons(&[(7, 100, 0)])).unwrap(),
    )
    .unwrap();
    flow.step_datafusion().await.unwrap();
    assert_eq!(
        canonical(&flow.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(1), Some(10)]],
    );

    let blob = flow.checkpoint_full().unwrap();
    let restored = IncrementalFlow::new();
    restored.register_view(spec(sql_variant, out)).unwrap();
    restored.restore_full(&blob).unwrap();

    for f in [&restored, &flow] {
        f.feed(
            "auction",
            DeltaBatch::from_inserts(auctions(&[(5, 7, 10)])).unwrap(),
        )
        .unwrap();
        let s = f.step_datafusion().await.unwrap();
        assert!(s.errored_views.is_empty(), "{:?}", s.errored_views);
    }
    let (inc, why) = restored
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(
        inc && why.contains("chain"),
        "not a chain after reseed: {why}"
    );
    assert_eq!(
        canonical(&restored.snapshot("v").unwrap().unwrap()),
        vec![vec![Some(1), Some(10)], vec![Some(5), Some(10)]],
        "the new auction joined through the SEEDED side (aggregate + traces)"
    );
    assert_eq!(
        canonical(&restored.snapshot("v").unwrap().unwrap()),
        canonical(&flow.snapshot("v").unwrap().unwrap()),
        "reseeded and continuous flows agree"
    );
}
