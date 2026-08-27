//! HOP-1 + TOPNK-1 at corpus scale: NEXMark q5, q18 and q19 registered
//! VERBATIM (the streaming TVF + ranking dialect the corpus actually ships),
//! maintained across ticks with generated bid batches and a retraction, and
//! compared against `force_diff_based` recompute — the oracle runs the SAME
//! registration-rewritten standard SQL whole through DataFusion, so subject
//! and oracle answer one question and only the maintenance differs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::prelude::SessionContext;
use krishiv_bench::nexmark::{NexmarkGenerator, SUPPORTED_QUERIES};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

fn corpus_sql(name: &str) -> String {
    SUPPORTED_QUERIES
        .iter()
        .find(|q| q.name == name)
        .unwrap_or_else(|| panic!("{name} not in corpus"))
        .sql
        .to_owned()
}

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

/// Register `name`'s corpus SQL verbatim on a subject and a DiffBased oracle,
/// feed three generated bid batches across three ticks — the third also
/// RETRACTS half of the first batch, the case that exercises promotion,
/// window-count decrement and group vanishing — comparing snapshots each tick.
async fn corpus_verbatim_matches_recompute(name: &str, needle: &str) {
    let sql = corpus_sql(name);
    let mut g = NexmarkGenerator::new(42, 1_000, 0, 0);
    let batches: Vec<RecordBatch> = (0..3).map(|_| g.next_bid_batch(200).unwrap()).collect();
    let bid_schema: SchemaRef = batches[0].schema();

    // The declared contract is DataFusion's own logical schema for the
    // REGISTRATION-REWRITTEN SQL (the raw dialect plans nowhere).
    let mut rewritten = sql.clone();
    for rewrite in [
        krishiv_ivm::window_rewrite::rewrite_tumble_tvfs,
        krishiv_ivm::window_rewrite::rewrite_hop_tvfs,
        krishiv_ivm::window_rewrite::rewrite_streaming_topn,
    ] {
        if let Some(r) = rewrite(&rewritten) {
            rewritten = r;
        }
    }
    let ctx = SessionContext::new();
    ctx.register_batch("bid", RecordBatch::new_empty(bid_schema.clone()))
        .unwrap();
    let declared: SchemaRef = Arc::new(
        ctx.sql(&rewritten)
            .await
            .unwrap()
            .schema()
            .as_arrow()
            .clone(),
    );

    let spec = |out: SchemaRef| IncrementalViewSpec {
        name: "v".into(),
        // VERBATIM: the dialect SQL, exactly as the corpus ships it — the
        // registration rewrite is the engine's own surface, not the test's.
        body_sql: sql.clone(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: Vec::new(),
    };
    let subject = IncrementalFlow::new();
    subject.register_view(spec(declared.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(declared)).unwrap();
    oracle.force_diff_based().unwrap();

    for (i, batch) in batches.iter().enumerate() {
        let d = DeltaBatch::from_inserts(batch.clone()).unwrap();
        subject.feed("bid", d.clone()).unwrap();
        oracle.feed("bid", d).unwrap();
        if i == 2 {
            let half = batches[0].slice(0, batches[0].num_rows() / 2);
            let d = DeltaBatch::from_deletes(half).unwrap();
            subject.feed("bid", d.clone()).unwrap();
            oracle.feed("bid", d).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(
            s.errored_views.is_empty(),
            "{name} tick {i}: {:?}",
            s.errored_views
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
        );
        let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
        let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
        assert_eq!(got, want, "{name} tick {i}: disagreed with recompute");
        assert!(!got.is_empty(), "{name} tick {i}: fixture produced rows");
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "{name}: fell back to DiffBased: {why}");
    assert!(why.contains(needle), "{name}: wanted '{needle}' in: {why}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nexmark_q5_registered_verbatim_maintains_incrementally() {
    corpus_verbatim_matches_recompute("q5_hot_items", "chain").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nexmark_q18_registered_verbatim_maintains_incrementally() {
    corpus_verbatim_matches_recompute("q18_last_bid_dedup", "keyed top-N").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nexmark_q19_registered_verbatim_maintains_incrementally() {
    corpus_verbatim_matches_recompute("q19_auction_top10", "keyed top-N").await;
}

/// KEYEXPR-1 at corpus scale: q13's side-input join — `b.auction % 1000 =
/// s.k` with `auction` UInt64, so the hoisted key is `TRY_CAST(… AS
/// UInt64)` (the exact-for-integers type story) and the side table is
/// registered as the bounded SOURCE it is, fed once before the stream.
/// Ticks feed bids, then retract half of the first batch; every tick
/// compares against the DiffBased oracle.
#[tokio::test(flavor = "multi_thread")]
async fn nexmark_q13_registered_verbatim_maintains_incrementally() {
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    let sql = corpus_sql("q13_side_input_join");
    let mut g = NexmarkGenerator::new(42, 1_000, 0, 0);
    let batches: Vec<RecordBatch> = (0..3).map(|_| g.next_bid_batch(200).unwrap()).collect();
    let keys: Vec<u64> = (0..1000).collect();
    let labels: Vec<String> = keys.iter().map(|k| format!("cat-{k}")).collect();
    let side = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt64, false),
            Field::new("label", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(keys)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("bid", RecordBatch::new_empty(batches[0].schema()))
        .unwrap();
    ctx.register_batch("side", RecordBatch::new_empty(side.schema()))
        .unwrap();
    let declared: SchemaRef = Arc::new(ctx.sql(&sql).await.unwrap().schema().as_arrow().clone());

    let spec = |out: SchemaRef| IncrementalViewSpec {
        name: "v".into(),
        body_sql: sql.clone(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: Vec::new(),
    };
    let subject = IncrementalFlow::new();
    subject.register_view(spec(declared.clone())).unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec(declared)).unwrap();
    oracle.force_diff_based().unwrap();

    let d = DeltaBatch::from_inserts(side).unwrap();
    subject.feed("side", d.clone()).unwrap();
    oracle.feed("side", d).unwrap();
    for (i, batch) in batches.iter().enumerate() {
        let d = DeltaBatch::from_inserts(batch.clone()).unwrap();
        subject.feed("bid", d.clone()).unwrap();
        oracle.feed("bid", d).unwrap();
        if i == 2 {
            let half = batches[0].slice(0, batches[0].num_rows() / 2);
            let d = DeltaBatch::from_deletes(half).unwrap();
            subject.feed("bid", d.clone()).unwrap();
            oracle.feed("bid", d).unwrap();
        }
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(
            s.errored_views.is_empty(),
            "q13 tick {i}: {:?}",
            s.errored_views
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
        );
        let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
        let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
        assert_eq!(got, want, "q13 tick {i}: disagreed with recompute");
        assert!(!got.is_empty(), "q13 tick {i}: rows expected");
    }
    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "q13: fell back to DiffBased: {why}");
    assert!(why.contains("chain"), "q13: not via the chain: {why}");
}
