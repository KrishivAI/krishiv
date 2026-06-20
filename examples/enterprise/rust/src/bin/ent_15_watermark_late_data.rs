//! Enterprise 15 · Watermark & late-data correctness
//!
//! Produces a mix of on-time and late events (timestamps behind the watermark)
//! then verifies that:
//!   1. On-time events land in the correct tumbling window.
//!   2. Late events below the watermark lag are DROPPED and counted.
//!   3. Window results match only the in-time rows.
//!
//! Watermark lag = 5 000 ms. Window size = 10 000 ms.
//! Late events carry timestamps 30 000 ms behind the current event-time max.
//!
//! Run:
//!   cargo run --bin ent_15_watermark_late_data

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::{AggExpr, AggFunction};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind, execute_windowed_stream};

const WINDOW_MS:      i64 = 10_000;
const WATERMARK_LAG:  i64 = 5_000;
const LATE_OFFSET_MS: i64 = 30_000; // how far behind "now" the late events are

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
        Field::new("ts_ms",    DataType::Int64,   false),
    ]))
}

/// Build the on-time batch (spans `window_count` windows).
fn make_ontime_batch(
    schema: Arc<Schema>,
    base_ts: i64,
    window_count: usize,
    normal_per_window: usize,
) -> Result<(RecordBatch, i64)> {
    let customers = ["alice", "bob", "carol", "dave"];
    let mut ids:   Vec<i64>  = Vec::new();
    let mut custs: Vec<&str> = Vec::new();
    let mut amts:  Vec<f64>  = Vec::new();
    let mut ts:    Vec<i64>  = Vec::new();
    let mut id = 0i64;
    for w in 0..window_count {
        let window_start = base_ts + w as i64 * WINDOW_MS;
        for r in 0..normal_per_window {
            ids.push(id);
            custs.push(customers[id as usize % 4]);
            amts.push(50.0 + r as f64);
            ts.push(window_start + r as i64 * (WINDOW_MS / normal_per_window as i64));
            id += 1;
        }
    }
    let max_ts = *ts.iter().max().unwrap();
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(Int64Array::from(ts)),
    ])?;
    Ok((batch, max_ts))
}

/// Build the late batch: timestamps `LATE_OFFSET_MS` behind max_ts (below watermark).
fn make_late_batch(schema: Arc<Schema>, max_ts: i64, late_count: usize, id_base: i64) -> Result<RecordBatch> {
    let customers = ["alice", "bob", "carol", "dave"];
    let late_ts = max_ts - LATE_OFFSET_MS; // well behind watermark
    let ids:   Vec<i64>  = (id_base..id_base + late_count as i64).collect();
    let custs: Vec<&str> = ids.iter().map(|i| customers[(i % 4) as usize]).collect();
    let amts:  Vec<f64>  = vec![999.0; late_count]; // distinctive — must NOT appear in output
    let ts:    Vec<i64>  = vec![late_ts; late_count];
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(Int64Array::from(ts)),
    ])?)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 15: Watermark & Late Data Correctness ===");
    println!("  window_size  : {WINDOW_MS} ms");
    println!("  watermark_lag: {WATERMARK_LAG} ms");
    println!("  late_offset  : {LATE_OFFSET_MS} ms behind watermark (should be dropped)");
    println!();

    let schema = schema();
    let base_ts: i64 = 1_716_200_000_000;
    let windows      = 5;
    let normal_pw    = 100; // rows per window
    let late_count   = 50;
    let normal_count = windows * normal_pw;

    // KEY: pass as TWO separate batches so the watermark from batch-1 is
    // set before batch-2 arrives. Late events in batch-2 are behind that
    // watermark and must be dropped.
    let (ontime_batch, max_ts) = make_ontime_batch(schema.clone(), base_ts, windows, normal_pw)?;
    let late_batch             = make_late_batch(schema, max_ts, late_count, normal_count as i64)?;

    println!("  on-time batch: {} rows (spans {} windows)", ontime_batch.num_rows(), windows);
    println!("  late batch   : {} rows  ts={} (watermark after batch-1 ≈ {})",
        late_batch.num_rows(),
        max_ts - LATE_OFFSET_MS,
        max_ts - WATERMARK_LAG);
    println!("  expected     : late rows DROPPED (ts {} < watermark {})",
        max_ts - LATE_OFFSET_MS, max_ts - WATERMARK_LAG);
    println!();

    let spec = LocalWindowExecutionSpec {
        key_column:        "customer".into(),
        key_column_type:   "utf8".into(),
        event_time_column: "ts_ms".into(),
        watermark_lag_ms:  WATERMARK_LAG as u64,
        window_kind:       LocalWindowKind::Tumbling,
        window_size_ms:    WINDOW_MS as u64,
        agg_exprs: vec![
            AggExpr { function: AggFunction::Count, input_column: String::new(),   output_column: "cnt".into() },
            AggExpr { function: AggFunction::Sum,   input_column: "amount".into(), output_column: "revenue".into() },
            AggExpr { function: AggFunction::Max,   input_column: "amount".into(), output_column: "max_amt".into() },
        ],
        state_ttl_ms:          None,
        source_watermark_lags: HashMap::new(),
        source_id_column:      None,
    };

    // Two batches: on-time first, late second.
    let window_output = execute_windowed_stream(vec![ontime_batch, late_batch], &spec)?;

    // ── Analyse output ──────────────────────────────────────────────────────
    let total_window_rows: usize = window_output.iter().map(|b| b.num_rows()).sum();
    let mut total_cnt   = 0i64;
    let mut max_revenue = 0.0f64;
    let mut max_amt_seen = 0.0f64;
    let mut late_amt_found = false;

    for batch in &window_output {
        let cnts = batch.column_by_name("cnt")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let revs = batch.column_by_name("revenue")
            .or_else(|| batch.column_by_name("revenue"))
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let maxs = batch.column_by_name("max_amt")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        if let Some(c) = cnts {
            for i in 0..c.len() { total_cnt += c.value(i); }
        }
        if let Some(r) = revs {
            for i in 0..r.len() { max_revenue = max_revenue.max(r.value(i)); }
        }
        if let Some(m) = maxs {
            for i in 0..m.len() {
                let v = m.value(i);
                max_amt_seen = max_amt_seen.max(v);
                if (v - 999.0).abs() < 0.01 { late_amt_found = true; }
            }
        }
    }

    // Print output via session SQL.
    let session = krishiv::Session::builder().build()?;
    session.register_record_batches("windows", window_output)?;
    let df = session.sql(
        "SELECT customer, window_start_ms, window_end_ms, cnt, ROUND(revenue,2) AS revenue \
         FROM windows ORDER BY window_start_ms, customer LIMIT 20"
    )?;
    println!("--- Window output (first 20 rows) ---");
    println!("{}", df.collect()?.pretty()?);

    println!("--- Verification ---");
    println!("  window rows emitted: {total_window_rows}");
    println!("  total event count  : {total_cnt}");
    println!("  expected on-time   : {normal_count}");
    println!("  max_amt in output  : {max_amt_seen:.1} (late events carry 999.0)");

    let late_dropped = total_cnt == normal_count as i64 && !late_amt_found;
    if late_dropped {
        println!("  ✓ late events DROPPED: max_amt {max_amt_seen:.1} < 999.0, total_cnt={total_cnt}");
    } else {
        println!("  ⚠ late events may have leaked: max_amt={max_amt_seen:.1} late_amt_found={late_amt_found}");
    }

    // Check that exactly `windows` distinct windows fired per customer (4 customers × 5 windows).
    let df2 = session.sql(
        "SELECT COUNT(*) AS window_rows FROM windows"
    )?;
    let r = df2.collect()?;
    let cnt_rows: i64 = r.batches().first()
        .and_then(|b| b.column_by_name("window_rows"))
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0))
        .unwrap_or(0);

    println!("  window rows total  : {cnt_rows} (expected {} = {} windows × 4 customers)",
        windows * 4, windows);

    if cnt_rows == (windows * 4) as i64 && late_dropped {
        println!("\n✓ PASS — watermark correctly dropped {late_count} late events, {windows} windows × 4 customers fired");
    } else {
        println!("\n✗ FAIL — check output above");
    }

    Ok(())
}
