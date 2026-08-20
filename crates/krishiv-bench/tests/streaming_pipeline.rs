//! Join → windowed aggregation pipelines, SQL text to emitted rows (task
//! #146): NEXMark Q9 (winning bids = join + per-auction top-1) and Q4
//! (average winning price per category = join + MAX stage + AVG stage).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::pipeline::JoinAggPipeline;
use krishiv_sql::streaming_pipeline_plan::{
    compile_streaming_pipeline_sql, looks_like_streaming_pipeline,
};

const Q9: &str = "WITH joined AS (SELECT b.auction, b.bidder, b.price FROM bid b \
                  JOIN auction a ON b.auction = a.id \
                  AND b.\"dateTime\" BETWEEN a.\"dateTime\" - 10000 AND a.\"dateTime\" + 10000) \
                  SELECT auction, bidder, price \
                  FROM TUMBLE(TABLE joined, DESCRIPTOR(left_dateTime), 10000) \
                  GROUP BY auction, window_start, window_end \
                  ORDER BY price DESC LIMIT 1";

const Q4: &str = "WITH joined AS (SELECT b.auction, b.price, a.category FROM bid b \
                  JOIN auction a ON b.auction = a.id \
                  AND b.\"dateTime\" BETWEEN a.\"dateTime\" - 10000 AND a.\"dateTime\" + 10000), \
                  winning AS (SELECT auction, category, MAX(price) AS final \
                  FROM TUMBLE(TABLE joined, DESCRIPTOR(left_dateTime), 10000) \
                  GROUP BY auction, category, window_start, window_end) \
                  SELECT category, AVG(final) AS avg_final \
                  FROM TUMBLE(TABLE winning, DESCRIPTOR(window_start_ms), 10000) \
                  GROUP BY category, window_start, window_end";

fn bids(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (auction, bidder, price, dateTime)
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("bids")
}

fn auctions(rows: &[(i64, i64, i64)]) -> RecordBatch {
    // (id, category, dateTime)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("auctions")
}

fn column_i64(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch.schema().index_of(name).expect(name);
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    (0..batch.num_rows()).map(|i| arr.value(i)).collect()
}

/// Q9: per auction, the HIGHEST bid survives — join then top-1.
#[test]
fn q9_pipeline_emits_the_winning_bid_per_auction() {
    let plan = compile_streaming_pipeline_sql(Q9).expect("Q9 pipeline must compile");
    let mut pipe = JoinAggPipeline::new(&plan.spec).expect("pipeline");

    let mut out = pipe
        .on_right(&auctions(&[(1, 10, 1_000), (2, 11, 1_000)]))
        .expect("auctions");
    out.extend(
        pipe.on_left(&bids(&[
            (1, 91, 100, 1_000),
            (1, 92, 900, 1_100),
            (1, 93, 500, 1_200),
            (2, 94, 50, 1_100),
        ]))
        .expect("bids"),
    );
    out.extend(pipe.flush_all().expect("flush"));

    let mut winners: Vec<(i64, i64, i64)> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| {
            let a = column_i64(b, "auction");
            let bidders = column_i64(b, "bidder");
            let p = column_i64(b, "price");
            a.into_iter()
                .zip(bidders)
                .zip(p)
                .map(|((a, bd), p)| (a, bd, p))
                .collect::<Vec<_>>()
        })
        .collect();
    winners.sort_unstable();
    assert_eq!(
        winners,
        vec![(1, 92, 900), (2, 94, 50)],
        "each auction's single highest bid, with its bidder"
    );
}

/// Q4: winning bid per auction (stage 1), then AVERAGE of winning bids per
/// category (stage 2). Two auctions in category 10 with winners 900 and 400
/// average to 650; category 11's lone winner stays 50. Collapsing to one
/// stage (MAX or AVG of raw bids per category) gives different numbers, so
/// this fixture distinguishes true two-stage composition.
#[test]
fn q4_pipeline_averages_winning_bids_per_category() {
    let plan = compile_streaming_pipeline_sql(Q4).expect("Q4 pipeline must compile");
    let mut pipe = JoinAggPipeline::new(&plan.spec).expect("pipeline");

    let mut out = pipe
        .on_right(&auctions(&[(1, 10, 1_000), (2, 10, 1_000), (3, 11, 1_000)]))
        .expect("auctions");
    out.extend(
        pipe.on_left(&bids(&[
            (1, 91, 100, 1_000),
            (1, 92, 900, 1_100),
            (2, 93, 400, 1_150),
            (2, 94, 100, 1_160),
            (3, 95, 50, 1_100),
        ]))
        .expect("bids"),
    );
    out.extend(pipe.flush_all().expect("flush"));

    let mut avgs: Vec<(i64, f64)> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| {
            let cats = column_i64(b, "category");
            let idx = b.schema().index_of("avg_final").expect("avg_final");
            let arr = b
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 avg");
            cats.into_iter()
                .zip((0..b.num_rows()).map(|i| arr.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    avgs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        avgs,
        vec![(10, 650.0), (11, 50.0)],
        "AVG of WINNING bids: (900+400)/2 = 650 — raw-bid AVG would be 375, \
         raw-bid MAX would be 900"
    );
}

/// The chain is validated: a stage reading the wrong source is refused
/// naming BOTH names.
#[test]
fn stage_reading_the_wrong_source_is_refused_by_name() {
    let bad = "WITH joined AS (SELECT b.auction, b.price FROM bid b \
               JOIN auction a ON b.auction = a.id \
               AND b.\"dateTime\" BETWEEN a.\"dateTime\" - 10000 AND a.\"dateTime\" + 10000) \
               SELECT auction, MAX(price) AS mx \
               FROM TUMBLE(TABLE elsewhere, DESCRIPTOR(dateTime), 10000) \
               GROUP BY auction, window_start, window_end";
    assert!(looks_like_streaming_pipeline(bad), "shape is claimed");
    let err = compile_streaming_pipeline_sql(bad).expect_err("must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("elsewhere") && msg.contains("joined"),
        "the refusal must name both the wrong and the required source: {msg}"
    );
}

/// The stage-0 lateness bound is TWICE the band. A bid whose own time trails
/// the watermark by 1.5 bands can still legitimately match an auction that
/// survived eviction (0.6 bands behind); with only 1x lag the stage drops
/// that match as late and one winner silently vanishes.
#[test]
fn a_match_deep_in_the_band_survives_the_stage() {
    // Band 10_000. Watermark will reach 20_000 via the late auction batch.
    let plan = compile_streaming_pipeline_sql(Q9).expect("compiles");
    assert!(
        plan.spec.stages[0].watermark_lag_ms >= 20_000,
        "premise: stage 0 lag must be at least 2x the 10s band"
    );
    let mut pipe = JoinAggPipeline::new(&plan.spec).expect("pipeline");

    let mut out = Vec::new();
    // Auction at 14_000 (0.6 bands behind the eventual 20_000 watermark).
    out.extend(pipe.on_right(&auctions(&[(1, 10, 14_000)])).expect("a1"));
    // Advance the join watermark to 20_000 with an unrelated pair.
    out.extend(pipe.on_right(&auctions(&[(9, 10, 20_000)])).expect("a2"));
    out.extend(pipe.on_left(&bids(&[(9, 90, 1, 20_000)])).expect("b-adv"));
    pipe.advance_watermark(20_000);
    // The deep-in-band bid: own time 5_000 = wm - 1.5 bands, matching the
    // auction at 14_000 (|5_000 - 14_000| = 9_000 < 10_000, in band).
    out.extend(
        pipe.on_left(&bids(&[(1, 91, 700, 5_000)]))
            .expect("deep bid"),
    );
    out.extend(pipe.flush_all().expect("flush"));

    let winners: Vec<i64> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| column_i64(b, "price"))
        .collect();
    assert!(
        winners.contains(&700),
        "the deep-in-band match must reach the stage, not be dropped as late: {winners:?}"
    );
}
