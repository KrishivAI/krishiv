//! Phase 57 exit-gate soak: long-run memory flat under an insert-only
//! source with lateness configured (RSS bounded over a ≥24h synthetic run).
//!
//! A continuous tumbling-window job (10s windows, 60s allowed lateness)
//! ingests a steady synthetic stream in which ~10% of events are LATE
//! within the lateness bound (must be aggregated) and ~5% are late BEYOND
//! the bound (must be dropped — retained beyond-bound state is exactly the
//! leak this soak exists to catch). Every minute the driver samples its
//! own `VmRSS` from `/proc/self/status` and appends one JSONL line; on
//! exit it prints a verdict comparing the median RSS of the first
//! post-warmup hour against the last hour.
//!
//! Run: KRISHIV_SOAK_SECONDS=86400 cargo run --release -p krishiv-bench \
//!        --bin lateness_soak -- /path/to/soak.jsonl
//! Any wall-clock length works (default 24h); the verdict rule is the
//! same. Warm-up (first 10 minutes) is excluded from the verdict.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)]

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_plan::window::WindowExecutionSpec;
use krishiv_runtime::InProcessStreamingRuntime;

const WINDOW_MS: u64 = 10_000;
const LATENESS_MS: u64 = 60_000;
const KEYS: i64 = 100;
const EVENTS_PER_TICK: i64 = 500;
const TICK: Duration = Duration::from_millis(100);
const SAMPLE_EVERY: Duration = Duration::from_secs(60);
const WARMUP: Duration = Duration::from_secs(600);
/// Verdict tolerance: last-hour median RSS may exceed the first
/// post-warmup hour's median by at most this factor.
const FLAT_TOLERANCE: f64 = 1.10;

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

fn batch(schema: &Arc<Schema>, rows: &[(String, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(k, _, _)| k.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("soak batch build")
}

fn main() {
    let out_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "lateness-soak.jsonl".into());
    let total = Duration::from_secs(
        std::env::var("KRISHIV_SOAK_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(86_400),
    );
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .expect("open soak log");

    let runtime = InProcessStreamingRuntime::new().expect("runtime");
    let mut spec = WindowExecutionSpec::tumbling("k", "ts", WINDOW_MS);
    spec.allowed_lateness_ms = Some(LATENESS_MS);
    runtime
        .register_continuous_job("lateness-soak", spec)
        .expect("register job");

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let start = Instant::now();
    // Synthetic event time advances with wall time (1:1), starting high
    // enough that "very late" subtraction never goes negative.
    let ts0: i64 = 10 * 60 * 1000;
    let mut pushed: u64 = 0;
    let mut emitted: u64 = 0;
    let mut last_sample = Instant::now() - SAMPLE_EVERY; // sample immediately
    let mut seq: i64 = 0;

    eprintln!(
        "lateness_soak: window={WINDOW_MS}ms lateness={LATENESS_MS}ms keys={KEYS} \
         events/tick={EVENTS_PER_TICK} tick={TICK:?} total={total:?} log={out_path}"
    );

    while start.elapsed() < total {
        let now_ms = ts0 + start.elapsed().as_millis() as i64;
        let mut rows = Vec::with_capacity(EVENTS_PER_TICK as usize);
        for i in 0..EVENTS_PER_TICK {
            seq += 1;
            let key = format!("user-{:03}", (seq * 7) % KEYS);
            // 85% on-time, 10% late-within-bound (30s behind), 5% late
            // beyond the bound (5 min behind — must be dropped, and must
            // not leave state behind).
            let ts = match i % 20 {
                0 => now_ms - 300_000,
                1 | 2 => now_ms - 30_000,
                _ => now_ms,
            };
            rows.push((key, ts, 1));
        }
        runtime
            .push_continuous_input("lateness-soak", vec![batch(&schema, &rows)])
            .expect("push");
        pushed += EVENTS_PER_TICK as u64;

        let drained = runtime
            .drain_continuous_job("lateness-soak")
            .expect("drain");
        emitted += drained.iter().map(|b| b.num_rows() as u64).sum::<u64>();

        if last_sample.elapsed() >= SAMPLE_EVERY {
            last_sample = Instant::now();
            let line = serde_json::json!({
                "elapsed_s": start.elapsed().as_secs(),
                "rss_kib": rss_kib(),
                "events_pushed": pushed,
                "windows_emitted": emitted,
            });
            writeln!(out, "{line}").expect("write sample");
            out.flush().ok();
            println!("SOAK {line}");
        }
        std::thread::sleep(TICK);
    }

    // Verdict: median RSS of the first post-warmup hour vs the last hour.
    let samples: Vec<(u64, u64)> = std::fs::read_to_string(&out_path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| Some((v.get("elapsed_s")?.as_u64()?, v.get("rss_kib")?.as_u64()?)))
        .collect();
    let median = |xs: &mut Vec<u64>| -> u64 {
        xs.sort_unstable();
        xs.get(xs.len() / 2).copied().unwrap_or(0)
    };
    let warm = WARMUP.as_secs();
    let total_s = total.as_secs();
    let mut first: Vec<u64> = samples
        .iter()
        .filter(|(t, _)| *t >= warm && *t < warm + 3600)
        .map(|(_, r)| *r)
        .collect();
    let mut last: Vec<u64> = samples
        .iter()
        .filter(|(t, _)| *t + 3600 >= total_s)
        .map(|(_, r)| *r)
        .collect();
    if first.is_empty() || last.is_empty() {
        println!(
            "SOAK VERDICT: INCONCLUSIVE — run shorter than warmup + comparison windows \
             (need > {}s); events={pushed} windows={emitted}",
            warm + 2 * 3600
        );
        std::process::exit(2);
    }
    let (first_med, last_med) = (median(&mut first), median(&mut last));
    let flat = first_med > 0 && (last_med as f64) <= (first_med as f64) * FLAT_TOLERANCE;
    println!(
        "SOAK VERDICT: {} — first-hour median RSS {first_med} KiB, last-hour median \
         {last_med} KiB (tolerance {FLAT_TOLERANCE}x); events={pushed} windows={emitted}",
        if flat {
            "FLAT (PASS)"
        } else {
            "GROWING (FAIL)"
        },
    );
    std::process::exit(if flat { 0 } else { 1 });
}
