//! Per-row memory attribution for the IVM state structures (task #166).
//!
//! # Why this exists
//!
//! A single-node scale ladder put the ceiling at ~10M seeded rows on a 61 GB
//! box: 12.17 GB resident for 10M rows, or **~1.2 KB per seeded row** against a
//! NEXMark bid row of 50-100 bytes. That 10-20x blowup, not the hardware, is
//! what caps every large run — extrapolating to ~120 GB at 100M and ~1.2 TB at
//! 1B. Task #166 lists three suspects and says, in capitals, do not fix before
//! attributing.
//!
//! This measures each suspect ALONE at a fixed row count, so the per-row cost
//! of each is a number rather than an argument. It is deliberately NOT an
//! assertion: a budget here would either be so loose it proves nothing or so
//! tight it fails on an allocator change. The output is the deliverable.
//!
//! `#[ignore]`d because it allocates gigabytes and takes real time; run it
//! explicitly:
//!
//! ```text
//! cargo test -p krishiv-delta --release --test memory_attribution -- --ignored --nocapture
//! ```
//!
//! **Read RSS, not the allocator's own counters.** RSS is what the OOM killer
//! counts, and it is what the ladder measured; a `#[global_allocator]` shim
//! would report requested bytes and miss both allocator slack and the
//! fragmentation that actually filled the 14 GB cap.

#![allow(clippy::unwrap_used, clippy::print_stdout)]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use krishiv_delta::DeltaBatch;
use krishiv_delta::snapshot_index::SnapshotIndex;
use krishiv_delta::trace::Trace;

/// Resident set size in bytes, from `/proc/self/statm` field 2 (resident
/// pages). Linux-only, which this benchmark host is.
fn rss_bytes() -> usize {
    let raw = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: usize = raw
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    pages * 4096
}

/// A NEXMark-bid-shaped row: two integer columns and one short string. The
/// payload is what a source row actually costs, and it is the denominator the
/// 1.2 KB figure is measured against.
fn bid_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("bidder", DataType::Utf8, false),
    ]))
}

fn bid_batch(n: usize) -> RecordBatch {
    let auctions: Vec<i64> = (0..n as i64).collect();
    let prices: Vec<i64> = (0..n as i64).map(|i| i % 1_000).collect();
    let bidders: Vec<String> = (0..n).map(|i| format!("b{}", i % 10_000)).collect();
    RecordBatch::try_new(
        bid_schema(),
        vec![
            Arc::new(Int64Array::from(auctions)),
            Arc::new(Int64Array::from(prices)),
            Arc::new(StringArray::from(bidders)),
        ],
    )
    .unwrap()
}

fn report(label: &str, before: usize, after: usize, rows: usize) {
    let delta = after.saturating_sub(before);
    println!(
        "{label:<34} {:>10.1} MB   {:>8.1} bytes/row",
        delta as f64 / (1024.0 * 1024.0),
        delta as f64 / rows as f64
    );
}

#[test]
#[ignore = "allocates gigabytes; run explicitly for task #166 attribution"]
fn per_row_memory_of_each_ivm_state_structure() {
    const ROWS: usize = 2_000_000;
    println!("\nIVM per-row memory attribution — {ROWS} rows, RSS deltas\n");
    println!("{:<34} {:>13}   {:>18}", "structure", "resident", "per row");
    println!("{}", "-".repeat(70));

    // ── 1. The payload itself: an Arrow batch of source rows. ────────────
    let base = rss_bytes();
    let batch = bid_batch(ROWS);
    let after_batch = rss_bytes();
    report("arrow source batch (payload)", base, after_batch, ROWS);

    // ── 2. Trace: the accumulated Z-set an operator maintains. ───────────
    let before_trace = rss_bytes();
    let mut trace = Trace::new(bid_schema(), &["auction"]).unwrap();
    trace.insert(DeltaBatch::from_inserts(batch.clone()).unwrap());
    // Probe once so the PERF-6 key index is built — a trace that is only
    // checkpointed never pays for one, so measuring without a probe would
    // under-report what a JOINING trace costs.
    let _ = trace.probe_by_keys(&bid_batch(1)).unwrap();
    let after_trace = rss_bytes();
    report(
        "  + Trace (indexed, probed)",
        before_trace,
        after_trace,
        ROWS,
    );

    // ── 3. SnapshotIndex: the materialized-view row multiset. ────────────
    // #166's prime suspect: AHashMap<Vec<u8>, (i64, u64)> is one heap Vec per
    // DISTINCT row, with the encoded key bytes duplicated out of the batch.
    let before_index = rss_bytes();
    let mut index = SnapshotIndex::new(bid_schema()).unwrap();
    index
        .apply(&DeltaBatch::from_inserts(batch.clone()).unwrap())
        .unwrap();
    let after_index = rss_bytes();
    report(
        "  + SnapshotIndex (counts map)",
        before_index,
        after_index,
        ROWS,
    );

    // ── 4. Its materialization cache, which apply() invalidates and
    //       batch() rebuilds — a second full copy of the snapshot. ────────
    let before_cache = rss_bytes();
    let _materialized = index.batch().unwrap();
    let after_cache = rss_bytes();
    report(
        "  + SnapshotIndex::batch() cache",
        before_cache,
        after_cache,
        ROWS,
    );

    println!("{}", "-".repeat(70));
    report("TOTAL resident", base, rss_bytes(), ROWS);
    println!(
        "\nLadder reference: 10M seeded rows -> 12.17 GB resident = ~1218 bytes/row.\n\
         Anything here that is not a large share of that is NOT the barrier,\n\
         however plausible it looked in #166's suspect list.\n"
    );
    // Keep every structure alive to the end: dropping one early would hand its
    // pages back and silently deflate every later reading.
    drop((batch, trace, index, _materialized));
}
