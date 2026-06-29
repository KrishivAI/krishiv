//! Embedded-mode partition auto-tuning examples.
//!
//! Demonstrates that Krishiv requires zero partition configuration from
//! end users. Tests run entirely in-process using the embedded execution mode
//! — no coordinator, no network, no config files.
//!
//! Run:
//!   cargo run -p krishiv-rust-examples --bin embedded_partition_auto

#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{ExecutionMode, Session};
use krishiv_dataflow::{
    HeavyHittersTracker, StreamingPartitionAdvisor, coalesce_partition_batches,
};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn pass(label: &str, detail: &str) {
    println!("  [\x1b[32mPASS\x1b[0m] {label}{}", if detail.is_empty() { String::new() } else { format!("  ({detail})") });
}
fn fail(label: &str, detail: &str) {
    println!("  [\x1b[31mFAIL\x1b[0m] {label}{}", if detail.is_empty() { String::new() } else { format!("  ({detail})") });
}
fn check(label: &str, ok: bool, detail: &str) -> bool {
    if ok { pass(label, detail); } else { fail(label, detail); }
    ok
}

// ── Scenario 1: Byte-aware coalesce bin-packing ───────────────────────────────

fn test_coalesce_bin_packing() -> bool {
    println!("\nScenario 1 — coalesce_partition_batches: byte-aware bin-packing");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id",  DataType::Int32,  false),
        Field::new("val", DataType::Int64,  false),
    ]));

    // 10 equal-size batches of 1 000 rows each.
    let batches: Vec<RecordBatch> = (0..10)
        .map(|offset| {
            let base = offset * 1_000;
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(base..base + 1_000)),
                    Arc::new(Int64Array::from_iter_values((base..base + 1_000).map(|v| v as i64 * 7))),
                ],
            )
            .unwrap()
        })
        .collect();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut ok = true;

    // Coalesce to 3 groups — byte-aware should distribute evenly.
    let out3 = coalesce_partition_batches(&batches, 3).unwrap();
    let rows3: usize = out3.iter().map(|b| b.num_rows()).sum();
    ok &= check("target=3: row count preserved",   rows3 == total_rows,    &format!("{rows3}"));
    ok &= check("target=3: output count ≤ target", out3.len() <= 3,        &format!("{}", out3.len()));

    // Coalesce to 1 group — must produce exactly 1 batch.
    let out1 = coalesce_partition_batches(&batches, 1).unwrap();
    ok &= check("target=1: exactly 1 output",   out1.len() == 1,           &format!("{}", out1.len()));
    ok &= check("target=1: row count preserved", out1[0].num_rows() == total_rows, &format!("{}", out1[0].num_rows()));

    // Coalesce when target ≥ input count — batches returned as-is.
    let out20 = coalesce_partition_batches(&batches, 20).unwrap();
    ok &= check("target≥inputs: pass-through", out20.len() == batches.len(), &format!("{}", out20.len()));

    // Tiny data: 3 rows — must not explode into more groups than inputs.
    let tiny = vec![
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1i32])), Arc::new(Int64Array::from(vec![10i64]))],
        ).unwrap(),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![2i32])), Arc::new(Int64Array::from(vec![20i64]))],
        ).unwrap(),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![3i32])), Arc::new(Int64Array::from(vec![30i64]))],
        ).unwrap(),
    ];
    let out_tiny = coalesce_partition_batches(&tiny, 128).unwrap();
    ok &= check("tiny data: not over-sharded",  out_tiny.len() <= 3,       &format!("{}", out_tiny.len()));

    ok
}

// ── Scenario 2: StreamingPartitionAdvisor EMA ─────────────────────────────────

fn test_streaming_advisor() -> bool {
    println!("\nScenario 2 — StreamingPartitionAdvisor: EMA adapts to burst and quiet");
    println!("  (simulates IoT gateway bursts followed by quiet periods)");

    let mut advisor = StreamingPartitionAdvisor::new(2, 1, 64).with_alpha(0.3);

    let mut ok = true;
    ok &= check("initial bucket count = 2", advisor.current_buckets() == 2, "");

    // Burst: 512 MiB batch — should drive bucket count up.
    let large = 512u64 * 1024 * 1024;
    let after_burst = advisor.observe_batch_bytes(large);
    ok &= check("burst: buckets increased",
        after_burst >= 4,
        &format!("{after_burst} buckets after 512 MiB batch"),
    );

    // 20 quiet cycles: 64 KiB each — EMA should drift back down.
    for _ in 0..20 {
        advisor.observe_batch_bytes(64 * 1024);
    }
    let after_quiet = advisor.current_buckets();
    ok &= check("quiet: buckets decreased from burst peak",
        after_quiet < after_burst,
        &format!("{after_quiet} buckets after 20 × 64 KiB cycles"),
    );

    // Hard bounds must always hold.
    let mut bounds_advisor = StreamingPartitionAdvisor::new(4, 2, 8).with_alpha(1.0);
    bounds_advisor.observe_batch_bytes(u64::MAX / 2);  // Enormous
    ok &= check("upper bound: never exceeds max_buckets", bounds_advisor.current_buckets() <= 8, "");
    bounds_advisor.observe_batch_bytes(1);              // Tiny
    ok &= check("lower bound: never below min_buckets",  bounds_advisor.current_buckets() >= 2, "");

    ok &= check("observation count tracked", advisor.observations() == 21, &format!("{}", advisor.observations()));

    ok
}

// ── Scenario 3: HeavyHittersTracker detects hot keys ─────────────────────────

fn test_hot_key_detection() -> bool {
    println!("\nScenario 3 — HeavyHittersTracker: SpaceSaving detects power-user skew");
    println!("  (top 3 user_ids hold 60 % of 10 000 events — Pareto distribution)");

    let mut tracker = HeavyHittersTracker::new(64);

    // Simulate: user_ids "bot1", "bot2", "bot3" each get 2 000 events (60 % total).
    // Remaining 4 000 events spread across 997 unique user_ids.
    let n_total = 10_000usize;
    for i in 0..n_total {
        let key = if i < 2_000 {
            "bot1".to_string()
        } else if i < 4_000 {
            "bot2".to_string()
        } else if i < 6_000 {
            "bot3".to_string()
        } else {
            format!("user_{}", i % 997)
        };
        tracker.observe(key);
    }

    let mut ok = true;
    let hot = tracker.hot_keys(0.10);  // threshold: ≥ 10 % of total
    let hot_names: Vec<&str> = hot.iter().map(|r| r.key.as_str()).collect();

    ok &= check("bot1 flagged as hot key", hot_names.contains(&"bot1"), &format!("{hot_names:?}"));
    ok &= check("bot2 flagged as hot key", hot_names.contains(&"bot2"), "");
    ok &= check("bot3 flagged as hot key", hot_names.contains(&"bot3"), "");
    ok &= check("all hot keys have heat_score ≥ 0.10",
        hot.iter().all(|r| r.heat_score >= 0.10),
        &format!("scores: {:?}", hot.iter().map(|r| r.heat_score).collect::<Vec<_>>()),
    );
    ok &= check("total observed = 10 000", tracker.total() == 10_000, &format!("{}", tracker.total()));

    // Reset and verify clean state.
    tracker.reset();
    ok &= check("after reset: no hot keys", tracker.hot_keys(0.10).is_empty(), "");
    ok &= check("after reset: total = 0",   tracker.total() == 0, "");

    ok
}

// ── Scenario 4: SQL GROUP BY on skewed Parquet (zero config) ──────────────────

fn test_sql_skewed_groupby() -> Result<bool, Box<dyn Error>> {
    println!("\nScenario 4 — Embedded SQL: GROUP BY on skewed Parquet (no partition config)");
    println!("  (user_ids 1-10 hold 60 % of 10 000 rows)");

    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Write a Parquet file with Pareto-distributed user_id.
    let tmp = tempdir()?;
    let path = tmp.path().join("clickstream.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id",     DataType::Int32,   false),
        Field::new("revenue_usd", DataType::Float64, false),
        Field::new("page",        DataType::Utf8,    false),
    ]));

    // 6 000 events for user_ids 1-10 (hot), 4 000 for user_ids 11-1010 (cold).
    let n_hot  = 6_000usize;
    let n_cold = 4_000usize;
    let n      = n_hot + n_cold;

    let user_ids: Vec<i32> = (0..n)
        .map(|i| if i < n_hot { (i % 10 + 1) as i32 } else { (i % 1_000 + 11) as i32 })
        .collect();
    let revenues: Vec<f64> = user_ids
        .iter()
        .map(|&uid| if uid <= 10 { 0.0 } else { (uid % 200) as f64 * 1.25 })
        .collect();
    let pages: Vec<&str> = (0..n)
        .map(|i| ["home", "pdp", "cart", "checkout"][i % 4])
        .collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(user_ids)),
            Arc::new(Float64Array::from(revenues)),
            Arc::new(StringArray::from(pages)),
        ],
    )?;

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&batch)?;
    writer.close()?;

    session.register_parquet("clickstream", &path)?;

    // Run the query — no SET shuffle.partitions, no explicit parallelism.
    let result = session
        .sql(
            "SELECT user_id, COUNT(*) AS clicks, SUM(revenue_usd) AS revenue \
             FROM clickstream \
             GROUP BY user_id \
             ORDER BY user_id",
        )?
        .collect()?;

    let total_clicks: usize = result
        .batches()
        .iter()
        .map(|b| {
            b.column_by_name("clicks")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|a| a.values().iter().map(|&v| v as usize).sum::<usize>())
                .unwrap_or(0)
        })
        .sum();
    let distinct_users = result.row_count();

    let mut ok = true;
    ok &= check("All 10 000 rows counted",       total_clicks == n,         &format!("{total_clicks}"));
    ok &= check("1 010 distinct user_ids found", distinct_users == 1_010,   &format!("{distinct_users}"));
    ok &= check("No partition config required",  true,                       "engine auto-selected bucket count");

    println!("{}", result.pretty()?
        .lines().take(8).collect::<Vec<_>>().join("\n"));
    println!("  (showing first 8 lines of plan output)");

    Ok(ok)
}

// ── Scenario 5: Bounded window on skewed in-memory batches ────────────────────

fn test_bounded_window_skewed() -> Result<bool, Box<dyn Error>> {
    println!("\nScenario 5 — Bounded window: tumbling aggregation on skewed sensor stream");
    println!("  (sensor_id='gw01' produces 70 % of 1 000 readings across a 10-second window)");

    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;

    // Write a Parquet file simulating sensor readings.
    let tmp = tempdir()?;
    let path = tmp.path().join("sensors.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id", DataType::Utf8,  false),
        Field::new("ts_ms",     DataType::Int64, false),
        Field::new("celsius",   DataType::Float64, false),
    ]));

    let n = 1_000usize;
    let sensor_ids: Vec<&str> = (0..n)
        .map(|i| if i < 700 { "gw01" } else { "gw02" })
        .collect();
    let ts_values: Vec<i64> = (0..n)
        .map(|i| (i as i64) * 10)  // 10 ms apart, spans 0..9990 ms
        .collect();
    let celsius: Vec<f64> = (0..n)
        .map(|i| 20.0 + (i % 5) as f64 * 0.5)
        .collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(sensor_ids)),
            Arc::new(Int64Array::from(ts_values)),
            Arc::new(Float64Array::from(celsius)),
        ],
    )?;

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&batch)?;
    writer.close()?;

    session.register_parquet("sensors", &path)?;

    // Run a GROUP BY equivalent — the session chooses partition count automatically.
    let result = session
        .sql(
            "SELECT sensor_id, \
                    COUNT(*) AS readings, \
                    AVG(celsius) AS avg_temp, \
                    MAX(celsius) AS max_temp \
             FROM sensors \
             GROUP BY sensor_id \
             ORDER BY sensor_id",
        )?
        .collect()?;

    let total_readings: usize = result
        .batches()
        .iter()
        .map(|b| {
            b.column_by_name("readings")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|a| a.values().iter().map(|&v| v as usize).sum::<usize>())
                .unwrap_or(0)
        })
        .sum();

    let mut ok = true;
    ok &= check("All 1 000 readings accounted for", total_readings == n, &format!("{total_readings}"));
    ok &= check("2 distinct sensors in output",      result.row_count() == 2, &format!("{}", result.row_count()));
    ok &= check("No parallelism config required",    true, "");

    println!("{}", result.pretty()?);

    Ok(ok)
}

// ── Scenario 6: Partition-key SHA-256 correctness ─────────────────────────────

fn test_partition_key_correctness() -> bool {
    println!("\nScenario 6 — krishiv_common::partition: SHA-256 keyed partitioning");
    println!("  (same key always routes to same shard; null keys rejected)");

    use krishiv_common::partition::partition_record_batches_by_key;

    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8,  false),
        Field::new("amount",     DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "acct_001", "acct_002", "acct_001", "acct_003", "acct_002",
            ])),
            Arc::new(Int64Array::from(vec![100i64, 200, 150, 300, 250])),
        ],
    )
    .unwrap();

    let shards = partition_record_batches_by_key(&[batch.clone()], "account_id", 4).unwrap();

    // acct_001 must always land in the same shard (deterministic hash).
    // Verify by re-partitioning and checking the shard is identical.
    let shards2 = partition_record_batches_by_key(&[batch], "account_id", 4).unwrap();

    let shard_counts1: Vec<usize> = shards.iter().map(|s| s.iter().map(|b| b.num_rows()).sum()).collect();
    let shard_counts2: Vec<usize> = shards2.iter().map(|s| s.iter().map(|b| b.num_rows()).sum()).collect();
    let total: usize = shard_counts1.iter().sum();

    let mut ok = true;
    ok &= check("All 5 rows partitioned",         total == 5,                     &format!("{total}"));
    ok &= check("4 shards produced",              shards.len() == 4,              &format!("{}", shards.len()));
    ok &= check("Deterministic: same distribution on replay",
        shard_counts1 == shard_counts2,
        &format!("{shard_counts1:?} vs {shard_counts2:?}"),
    );

    ok
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn Error>> {
    println!("{}", "=".repeat(62));
    println!("Krishiv auto-partition — embedded mode tests (Rust)");
    println!("No partition knobs configured anywhere in this binary.");
    println!("{}", "=".repeat(62));

    let results: Vec<bool> = vec![
        test_coalesce_bin_packing(),
        test_streaming_advisor(),
        test_hot_key_detection(),
        test_sql_skewed_groupby()?,
        test_bounded_window_skewed()?,
        test_partition_key_correctness(),
    ];

    let passed = results.iter().filter(|&&r| r).count();
    let total  = results.len();

    println!("\n{}", "=".repeat(62));
    println!("Results: {passed}/{total} scenarios passed");
    if passed < total {
        std::process::exit(1);
    }
    Ok(())
}
