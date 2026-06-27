//! Enterprise 11 · Kafka High-Load Pipeline — real broker, Arrow IPC batches
//!
//! Measures end-to-end throughput of a produce → consume → window-aggregate
//! pipeline against a live Kafka broker using Arrow IPC for batch serialisation
//! (entire RecordBatch per message, orders of magnitude more efficient than
//! row-per-JSON-message).
//!
//! Pipeline:
//!   Producer  →  Kafka topic "orders-load-test" (4 partitions)
//!   Consumer  →  KafkaSource (rdkafka StreamConsumer)
//!   Window    →  execute_windowed_stream (tumbling 10-second windows)
//!   Output    →  throughput metrics printed to stdout
//!
//! Configuration via environment variables (all optional):
//!   KAFKA_BROKERS   broker list (default: localhost:9092)
//!   LOAD_ROWS       total rows to produce (default: 1_000_000)
//!   BATCH_SIZE      rows per Kafka message (default: 10_000)
//!   CONSUMER_TIMEOUT_MS  idle timeout before consumer stops (default: 3_000)
//!
//! Run:
//!   cargo run --bin ent_11_kafka_high_load
//!   LOAD_ROWS=100000 cargo run --bin ent_11_kafka_high_load

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use krishiv::{AggExpr, AggFunction};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind, execute_windowed_stream};
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

const TOPIC: &str = "orders-load-test";
const CUSTOMERS: &[&str] = &["alice", "bob", "carol", "dave", "eve", "frank", "grace", "henry"];
const PRODUCTS:  &[&str] = &["Laptop", "Mouse", "Chair", "Monitor", "Hub", "Keyboard", "Cam", "Desk"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id",  DataType::Int64,   false),
        Field::new("customer",  DataType::Utf8,    false),
        Field::new("product",   DataType::Utf8,    false),
        Field::new("amount",    DataType::Float64, false),
        Field::new("ts_ms",     DataType::Int64,   false),
    ]))
}

/// Build a synthetic batch of `size` rows starting at `base_id`.
/// Timestamps cycle through 10-second windows so the window operator fires.
fn make_batch(schema: Arc<Schema>, base_id: i64, size: usize) -> Result<RecordBatch> {
    let base_ts: i64 = 1_716_200_000_000; // fixed epoch for reproducibility
    let window_ms: i64 = 10_000;

    let order_ids: Vec<i64>  = (base_id..base_id + size as i64).collect();
    let customers: Vec<&str> = order_ids.iter().map(|i| CUSTOMERS[(i % CUSTOMERS.len() as i64) as usize]).collect();
    let products:  Vec<&str> = order_ids.iter().map(|i| PRODUCTS[(i % PRODUCTS.len() as i64) as usize]).collect();
    let amounts:   Vec<f64>  = order_ids.iter().map(|i| 10.0 + (i % 1000) as f64 * 0.5).collect();
    // Spread timestamps across windows; each batch falls into one window.
    let window_idx = base_id / size as i64;
    let ts: Vec<i64> = order_ids.iter().map(|i| {
        base_ts + window_idx * window_ms + (i % window_ms)
    }).collect();

    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(order_ids)),
        Arc::new(StringArray::from(customers)),
        Arc::new(StringArray::from(products)),
        Arc::new(Float64Array::from(amounts)),
        Arc::new(Int64Array::from(ts)),
    ])?)
}

/// Serialise a RecordBatch to Arrow IPC streaming bytes.
fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Deserialise Arrow IPC streaming bytes back to a RecordBatch.
fn ipc_to_batch(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    reader.map(|r| r.context("ipc read")).collect()
}

// ---------------------------------------------------------------------------
// Phase 1 — Produce
// ---------------------------------------------------------------------------

async fn produce(
    schema: Arc<Schema>,
    total_rows: usize,
    batch_size: usize,
) -> Result<(usize, f64)> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers())
        .set("message.timeout.ms", "10000")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.ms", "50")
        .set("batch.num.messages", "1000")
        .set("compression.type", "lz4")
        .create()
        .context("create producer")?;

    let mut base_id: i64 = 0;
    let mut total_bytes = 0usize;
    let num_batches = total_rows.div_ceil(batch_size);
    let t0 = Instant::now();

    let mut futures = Vec::with_capacity(64);

    for b in 0..num_batches {
        let rows = batch_size.min(total_rows - b * batch_size);
        let batch = make_batch(schema.clone(), base_id, rows)?;
        let ipc   = batch_to_ipc(&batch)?;
        base_id  += rows as i64;
        total_bytes += ipc.len();

        // Use partition key = batch index mod 4 for even distribution.
        let key = (b % 4).to_string();
        let record: FutureRecord<str, Vec<u8>> = FutureRecord::to(TOPIC)
            .key(key.as_str())
            .payload(&ipc);

        // Pipeline: keep up to 64 in-flight sends, drain when full.
        let f = producer.send_result(record)
            .map_err(|(e, _)| anyhow::anyhow!("send_result failed: {e}"))?;
        futures.push(f);

        if futures.len() >= 64 {
            for fut in futures.drain(..) {
                fut.await
                    .map_err(|e| anyhow::anyhow!("delivery: {e}"))?
                    .map_err(|(e, _)| anyhow::anyhow!("broker error: {e}"))?;
            }
        }

        if b % 50 == 0 {
            let elapsed = t0.elapsed().as_secs_f64();
            let rows_done = b * batch_size;
            let mb = total_bytes as f64 / 1_048_576.0;
            eprint!("\r  produce: {rows_done:>8} / {total_rows} rows  {mb:.1} MB  {:.0} msg/s   ",
                if elapsed > 0.0 { (b + 1) as f64 / elapsed } else { 0.0 });
        }
    }

    // Drain remaining futures.
    for fut in futures.drain(..) {
        fut.await
            .map_err(|e| anyhow::anyhow!("delivery: {e}"))?
            .map_err(|(e, _)| anyhow::anyhow!("broker error: {e}"))?;
    }

    producer.flush(Timeout::After(Duration::from_secs(10))).context("flush")?;
    let elapsed = t0.elapsed().as_secs_f64();
    eprintln!(); // newline after progress
    Ok((total_bytes, elapsed))
}

// ---------------------------------------------------------------------------
// Phase 2 — Consume + window
// ---------------------------------------------------------------------------

async fn consume_and_window(
    total_rows: usize,
    idle_timeout_ms: u64,
) -> Result<(usize, usize, f64, Vec<RecordBatch>)> {
    // Fresh consumer group per run so we always start from offset 0.
    let group_id = format!("krishiv-load-{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers())
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "65536")
        .set("fetch.wait.max.ms", "200")
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "3000")
        .set("max.poll.interval.ms", "300000")
        .set("socket.keepalive.enable", "true")
        .set("metadata.max.age.ms", "30000")
        .create()
        .context("create consumer")?;

    consumer.subscribe(&[TOPIC]).context("subscribe")?;

    // Allow time for the initial group rebalance to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let spec = LocalWindowExecutionSpec {
        key_column:        "customer".into(),
        key_column_type:   "utf8".into(),
        event_time_column: "ts_ms".into(),
        watermark_lag_ms:  0,
        window_kind:       LocalWindowKind::Tumbling,
        window_size_ms:    10_000,
        agg_exprs: vec![
            AggExpr { function: AggFunction::Count, input_column: String::new(),    output_column: "cnt".into() },
            AggExpr { function: AggFunction::Sum,   input_column: "amount".into(),  output_column: "revenue".into() },
            AggExpr { function: AggFunction::Min,   input_column: "amount".into(),  output_column: "min_amt".into() },
            AggExpr { function: AggFunction::Max,   input_column: "amount".into(),  output_column: "max_amt".into() },
        ],
        state_ttl_ms:          None,
        source_watermark_lags: HashMap::new(),
        source_id_column:      None,
        window_timezone:       None,
    };

    let mut consumed_rows  = 0usize;
    let mut consumed_bytes = 0usize;
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let t0 = Instant::now();

    loop {
        let msg = tokio::time::timeout(
            Duration::from_millis(idle_timeout_ms),
            consumer.recv(),
        ).await;

        match msg {
            Err(_) => {
                // Idle timeout — stop if we've seen all rows or made progress.
                if consumed_rows >= total_rows {
                    break;
                }
                // Extend wait if we're still making progress.
                if consumed_rows == 0 {
                    continue; // haven't started yet, keep waiting
                }
                break; // received some rows then went idle — done
            }
            Ok(Err(e)) => {
                // Transport glitches during rebalance are transient.
                eprintln!("  warn: {e} (retrying)");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(Ok(msg)) => {
                let payload = msg.payload().unwrap_or(&[]);
                consumed_bytes += payload.len();
                match ipc_to_batch(payload) {
                    Ok(batches) => {
                        for b in batches {
                            consumed_rows += b.num_rows();
                            all_batches.push(b);
                        }
                    }
                    Err(e) => eprintln!("  warn: IPC decode failed: {e}"),
                }
                if consumed_rows % 100_000 == 0 {
                    let elapsed = t0.elapsed().as_secs_f64();
                    eprint!("\r  consume: {consumed_rows:>8} rows  {:.0} rows/s   ",
                        if elapsed > 0.0 { consumed_rows as f64 / elapsed } else { 0.0 });
                }
            }
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();
    eprintln!();

    // Run windowed aggregation over all consumed batches.
    let window_output = execute_windowed_stream(all_batches, &spec)
        .map_err(|e| anyhow::anyhow!("window: {e}"))?;

    Ok((consumed_rows, consumed_bytes, elapsed, window_output))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows:   usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let batch_size:   usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let idle_timeout: u64   = std::env::var("CONSUMER_TIMEOUT_MS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(3_000);

    println!("=== Enterprise 11: Kafka High-Load Pipeline ===");
    println!("  broker        : {}", brokers());
    println!("  topic         : {TOPIC}");
    println!("  total rows    : {total_rows:>10}");
    println!("  batch size    : {batch_size:>10} rows/msg");
    println!("  messages      : {:>10}", total_rows.div_ceil(batch_size));
    println!("  serialisation : Arrow IPC + lz4 compression");
    println!();

    // ── Phase 1: Produce ──────────────────────────────────────────────────
    println!("▶ Phase 1 — Produce");
    let schema = schema();
    let (prod_bytes, prod_secs) = produce(schema, total_rows, batch_size).await?;

    let prod_rows_per_sec = total_rows as f64 / prod_secs;
    let prod_mb_per_sec   = prod_bytes as f64 / 1_048_576.0 / prod_secs;
    println!("  ✓ produced {total_rows:>10} rows  in {prod_secs:.2}s");
    println!("    throughput : {prod_rows_per_sec:>10.0} rows/s   {prod_mb_per_sec:.1} MB/s");
    println!("    payload    : {:.1} MB total ({:.0} bytes/msg avg)",
        prod_bytes as f64 / 1_048_576.0,
        prod_bytes as f64 / total_rows.div_ceil(batch_size) as f64);
    println!();

    // ── Phase 2: Consume + window ─────────────────────────────────────────
    println!("▶ Phase 2 — Consume + windowed aggregation (10s tumbling)");
    let (cons_rows, cons_bytes, cons_secs, window_out) =
        consume_and_window(total_rows, idle_timeout).await?;

    let cons_rows_per_sec = cons_rows as f64 / cons_secs;
    let cons_mb_per_sec   = cons_bytes as f64 / 1_048_576.0 / cons_secs;
    let window_rows: usize = window_out.iter().map(|b| b.num_rows()).sum();

    println!("  ✓ consumed  {cons_rows:>10} rows  in {cons_secs:.2}s");
    println!("    throughput : {cons_rows_per_sec:>10.0} rows/s   {cons_mb_per_sec:.1} MB/s");
    println!("    windows    : {window_rows:>10} window-rows emitted");
    println!();

    // ── Summary ───────────────────────────────────────────────────────────
    let e2e_secs = prod_secs + cons_secs;
    println!("▶ End-to-end summary");
    println!("  total rows   : {total_rows}");
    println!("  e2e time     : {e2e_secs:.2}s");
    println!("  e2e rows/s   : {:.0}", total_rows as f64 / e2e_secs);
    println!("  e2e MB/s     : {:.1}", prod_bytes as f64 / 1_048_576.0 / e2e_secs);
    println!();

    // ── Sample window output ──────────────────────────────────────────────
    if !window_out.is_empty() {
        let session = krishiv::Session::builder().build()?;
        session.register_record_batches("windows", window_out)?;
        let df = session.sql(
            "SELECT customer, COUNT(*) AS windows, \
                    SUM(cnt) AS total_orders, ROUND(SUM(revenue), 2) AS total_revenue \
             FROM windows \
             GROUP BY customer ORDER BY total_revenue DESC LIMIT 8"
        )?;
        println!("--- Top customers across all windows ---");
        println!("{}", df.collect()?.pretty()?);
    }

    Ok(())
}
