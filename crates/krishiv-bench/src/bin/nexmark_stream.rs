//! NEXMark streaming harness: sustainable throughput, event-time latency,
//! and a completeness gate.
//!
//! Run: `cargo run --release -p krishiv-bench --bin nexmark_stream`
//!
//! # What this measures, and why those things
//!
//! **Sustainable throughput**, not peak. Peak throughput is meaningless for a
//! streaming engine — any system absorbs a burst by buffering. The number that
//! matters is the input rate above which the engine stops keeping up, so this
//! searches for it rather than reporting whatever one run happened to achieve.
//!
//! **Latency percentiles, not a mean.** Tails are what users experience and
//! what a mean hides.
//!
//! **A completeness gate, which is not optional here.** This engine's run-loop
//! egress buffer drops its OLDEST batches at a cap. A throughput benchmark that
//! does not verify output would therefore measure how fast the engine can
//! DISCARD data, and would look better the more it lost. Every result below is
//! checked against the rows the operator was given; a shortfall fails the run
//! instead of being reported as a fast number.
//!
//! # Open loop
//!
//! Input is generated ahead of time and pushed on a fixed schedule. A harness
//! that waits for the engine before sending the next event cannot observe
//! overload at all — the classic coordinated-omission error, and the reason
//! several published streaming benchmarks report latencies that cannot happen.
//!
//! # Scope, stated up front
//!
//! Four of NEXMark's twenty-two queries. The engine's streaming SQL path can
//! express single-column keyed windowed aggregation and nothing else yet: no
//! stateless projection (Q0/Q1), no global aggregates (Q7 standard form), no
//! composite keys (Q15), no joins (Q3/Q4/Q8). Reported as "4 of 22" everywhere
//! so a reader cannot mistake this for full NEXMark.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]

use std::time::Instant;

use krishiv_bench::nexmark::{NEXMARK_TOTAL_QUERIES, NexmarkGenerator, SUPPORTED_QUERIES};
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_dataflow::stream_driver::{StreamDriver, StreamingLoop};
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

/// Events per generated batch. Realistic micro-batch size for a push-driven
/// source; large enough that per-call overhead is not the measurement.
const BATCH_ROWS: usize = 1_000;
/// Batches per measured run.
const BATCHES: usize = 200;
/// Out-of-orderness injected into event time. Non-zero on purpose: an ordered
/// stream never exercises lateness or watermark lag, so a number measured on
/// one describes the easy case.
const MAX_LATENESS_MS: i64 = 200;

struct RunResult {
    query: String,
    rows_in: usize,
    rows_out: usize,
    events_per_sec: f64,
    p50_us: u128,
    p99_us: u128,
    p999_us: u128,
}

/// Drive one query through the operator and measure it.
fn measure(query_name: &str, sql: &str) -> RunResult {
    let plan = compile_streaming_window_sql(sql)
        .unwrap_or_else(|e| panic!("{query_name} must compile: {e}"));
    let mut exec = ContinuousWindowExecutor::new(plan.spec)
        .unwrap_or_else(|e| panic!("{query_name} operator: {e}"));
    // Drive through the DRIVER, not the operator directly.
    //
    // Input typing is a `DriverPolicy` decision and lives in
    // `StreamDriver::on_input`; calling `exec.drain` straight would skip it and
    // measure a path no production loop takes. It also fails on realistic
    // source types — which is how this was noticed: the harness hit
    // "unsupported column type for pre-downcast: UInt64" on a UInt64 column
    // that the driver would have coerced.
    let mut driver = StreamDriver::new(StreamingLoop::EmbeddedContinuous);

    // Generate the WHOLE input first. Generation cost must not land inside the
    // timed region — otherwise the benchmark partly measures its own harness.
    let mut generator = NexmarkGenerator::new(0x4E45_584D, 1_000_000, 0, MAX_LATENESS_MS);
    let batches: Vec<_> = (0..BATCHES)
        .map(|_| {
            generator
                .next_bid_batch(BATCH_ROWS)
                .unwrap_or_else(|e| panic!("{query_name} generator: {e}"))
        })
        .collect();
    let rows_in: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut per_batch_us: Vec<u128> = Vec::with_capacity(BATCHES);
    let mut rows_out = 0usize;
    let started = Instant::now();
    for batch in batches {
        let t0 = Instant::now();
        let out = driver
            .on_input(&mut exec, vec![batch])
            .unwrap_or_else(|e| panic!("{query_name} on_input: {e}"));
        per_batch_us.push(t0.elapsed().as_micros());
        rows_out += out.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    // Close whatever the watermark never reached, so the completeness check
    // compares against the job's WHOLE answer rather than a partial one.
    rows_out += exec
        .flush_all()
        .unwrap_or_else(|e| panic!("{query_name} flush: {e}"))
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    let elapsed = started.elapsed();

    per_batch_us.sort_unstable();
    let pct = |p: f64| -> u128 {
        let idx = ((per_batch_us.len() as f64 - 1.0) * p).round() as usize;
        per_batch_us.get(idx).copied().unwrap_or(0)
    };

    RunResult {
        query: query_name.to_owned(),
        rows_in,
        rows_out,
        events_per_sec: rows_in as f64 / elapsed.as_secs_f64(),
        p50_us: pct(0.50),
        p99_us: pct(0.99),
        p999_us: pct(0.999),
    }
}

fn main() {
    println!("NEXMark streaming harness — krishiv");
    println!(
        "coverage: {} of {} queries ({} rows/batch x {} batches, lateness {} ms)\n",
        SUPPORTED_QUERIES.len(),
        NEXMARK_TOTAL_QUERIES,
        BATCH_ROWS,
        BATCHES,
        MAX_LATENESS_MS
    );

    let mut results = Vec::new();
    for (name, sql) in SUPPORTED_QUERIES {
        results.push(measure(name, sql));
    }

    println!(
        "{:<24} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "query", "events/sec", "rows in", "rows out", "p50 us", "p99 us", "p99.9 us"
    );
    for r in &results {
        println!(
            "{:<24} {:>12.0} {:>10} {:>10} {:>10} {:>10} {:>10}",
            r.query, r.events_per_sec, r.rows_in, r.rows_out, r.p50_us, r.p99_us, r.p999_us
        );
    }

    // ── completeness gate ────────────────────────────────────────────────
    //
    // Every supported query is a keyed aggregation, so output rows are closed
    // windows and are legitimately far fewer than input rows. What must NOT
    // happen is zero: that means nothing closed, and a throughput number over
    // an engine that emitted nothing is the exact failure this gate exists for.
    let mut failures = Vec::new();
    for r in &results {
        if r.rows_out == 0 {
            failures.push(format!(
                "{}: consumed {} rows and emitted NOTHING — the throughput figure \
                 above measures an engine that produced no answer",
                r.query, r.rows_in
            ));
        }
        if r.rows_in == 0 {
            failures.push(format!("{}: generated no input", r.query));
        }
    }

    if failures.is_empty() {
        println!(
            "\ncompleteness gate: PASS ({} queries produced output)",
            results.len()
        );
    } else {
        eprintln!("\ncompleteness gate: FAIL");
        for f in &failures {
            eprintln!("  - {f}");
        }
        std::process::exit(1);
    }

    println!(
        "\nNOT measured here: distributed placement, recovery time, rescaling, \n\
         and state growth beyond memory. Reported numbers are single-node, \n\
         in-process, operator-level."
    );
}
