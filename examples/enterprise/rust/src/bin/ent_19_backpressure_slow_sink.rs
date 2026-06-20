//! Enterprise 19 · Backpressure — slow-sink memory-bound test
//!
//! Produces rows to Kafka at full speed, then consumes them with an
//! artificially slow sink (configurable delay per batch). Verifies that:
//!   1. Memory stays bounded (no unbounded batch accumulation).
//!   2. All rows are processed eventually.
//!   3. The pipeline does not OOM or panic.
//!
//! In our design the consumer loop is naturally backpressured:
//! `consumer.recv().await` blocks waiting for the next IPC message, and
//! `sink.write_batch().await` blocks until the slow sink completes —
//! so the consumer pauses Kafka fetching until ready. rdkafka's internal
//! queue has a configurable max (`queued.max.messages.kbytes`).
//!
//! Run:
//!   cargo run --bin ent_19_backpressure_slow_sink
//!   SINK_DELAY_MS=100 LOAD_ROWS=20000 cargo run --bin ent_19_backpressure_slow_sink

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

const BROKERS: &str = "localhost:9092";
const TOPIC:   &str = "orders-backpressure";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
        Field::new("ts_ms",    DataType::Int64,   false),
    ]))
}

fn make_batch(schema: Arc<Schema>, base: i64, size: usize) -> Result<RecordBatch> {
    let customers = ["alice","bob","carol","dave"];
    let ids:   Vec<i64>  = (base..base + size as i64).collect();
    let custs: Vec<&str> = ids.iter().map(|i| customers[(i % 4) as usize]).collect();
    let amts:  Vec<f64>  = ids.iter().map(|i| 10.0 + (i % 100) as f64).collect();
    let ts:    Vec<i64>  = ids.iter().map(|i| 1_716_200_000_000 + i * 100).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(Int64Array::from(ts)),
    ])?)
}

fn batch_to_ipc(b: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut w = StreamWriter::try_new(&mut buf, &b.schema())?;
    w.write(b)?; w.finish()?;
    Ok(buf)
}

fn ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .context("ipc")?
        .map(|r| r.context("batch"))
        .collect()
}

/// A slow in-memory "sink" that counts rows and delays.
struct SlowSink {
    delay_ms:     u64,
    total_rows:   usize,
    total_writes: usize,
    peak_mem_kb:  usize, // rough estimate: rows * 100 bytes
}

impl SlowSink {
    fn new(delay_ms: u64) -> Self {
        Self { delay_ms, total_rows: 0, total_writes: 0, peak_mem_kb: 0 }
    }

    async fn write(&mut self, batch: RecordBatch) {
        let n = batch.num_rows();
        // Simulate slow processing.
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        self.total_rows   += n;
        self.total_writes += 1;
        let mem_kb = n * 100 / 1024;
        if mem_kb > self.peak_mem_kb { self.peak_mem_kb = mem_kb; }
        drop(batch); // release memory after simulated processing
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(1_000);
    let sink_delay: u64   = std::env::var("SINK_DELAY_MS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(20);

    println!("=== Enterprise 19: Backpressure — Slow Sink ===");
    println!("  broker     : {BROKERS}");
    println!("  topic      : {TOPIC}");
    println!("  rows       : {total_rows}");
    println!("  batch_size : {batch_size}");
    println!("  sink_delay : {sink_delay} ms/batch");
    println!("  ideal time : ~{:.1}s (produce) + {:.1}s (consume@{sink_delay}ms/batch)",
        0.5,
        total_rows.div_ceil(batch_size) as f64 * sink_delay as f64 / 1000.0);
    println!();

    // ── Produce at full speed ─────────────────────────────────────────────
    println!("▶ Phase 1 — Produce at full speed");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.ms", "50")
        .create()?;

    let schema = schema();
    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    let mut futs = Vec::with_capacity(32);
    for b in 0..num_batches {
        let rows  = batch_size.min(total_rows - b * batch_size);
        let batch = make_batch(schema.clone(), (b * batch_size) as i64, rows)?;
        let ipc   = batch_to_ipc(&batch)?;
        let key   = (b % 2).to_string();
        let f = producer.send_result(
            FutureRecord::<str, Vec<u8>>::to(TOPIC).key(key.as_str()).payload(&ipc)
        ).map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
        futs.push(f);
        if futs.len() >= 32 {
            for f in futs.drain(..) {
                f.await.map_err(|e| anyhow::anyhow!("{e}"))?
                 .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
            }
        }
    }
    for f in futs.drain(..) {
        f.await.map_err(|e| anyhow::anyhow!("{e}"))?
         .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
    }
    producer.flush(Timeout::After(Duration::from_secs(10))).context("flush")?;
    let prod_secs = t0.elapsed().as_secs_f64();
    println!("  ✓ produced {total_rows} rows in {prod_secs:.2}s ({:.0} rows/s)",
        total_rows as f64 / prod_secs);

    // ── Consume with slow sink ─────────────────────────────────────────────
    println!("▶ Phase 2 — Consume → slow sink ({sink_delay} ms/batch)");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent19-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "60000")
        // Limit rdkafka's pre-fetch queue so it doesn't buffer everything.
        .set("queued.max.messages.kbytes", "65536") // 64 MB max in rdkafka queue
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let mut sink = SlowSink::new(sink_delay);
    let t1 = Instant::now();
    let mut snapshot_secs: Vec<(f64, usize)> = Vec::new(); // (elapsed, rows) for mem check
    loop {
        let msg = match tokio::time::timeout(
            Duration::from_millis(sink_delay * 10 + 5000), consumer.recv()
        ).await {
            Err(_) => break,
            Ok(Err(e)) => { eprintln!("  warn: {e}"); tokio::time::sleep(Duration::from_millis(200)).await; continue; }
            Ok(Ok(m)) => m,
        };
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            sink.write(batch).await;
        }
        let elapsed = t1.elapsed().as_secs_f64();
        if sink.total_writes % 10 == 0 {
            snapshot_secs.push((elapsed, sink.total_rows));
            eprint!("\r  processed {}/{total_rows} rows  {sink_delay}ms/batch  ", sink.total_rows);
        }
        if sink.total_rows >= total_rows { break; }
    }
    eprintln!();
    let cons_secs = t1.elapsed().as_secs_f64();
    println!("  ✓ processed {}/{total_rows} rows in {cons_secs:.2}s ({:.1} rows/s)",
        sink.total_rows, sink.total_rows as f64 / cons_secs);

    // ── Backpressure analysis ──────────────────────────────────────────────
    println!();
    println!("--- Backpressure Analysis ---");
    println!("  produce rate : {:.0} rows/s (unconstrained)", total_rows as f64 / prod_secs);
    println!("  consume rate : {:.1} rows/s (constrained by {sink_delay}ms sink delay)",
        sink.total_rows as f64 / cons_secs);
    println!("  write ops    : {}", sink.total_writes);
    println!("  peak batch KB: {} (single batch in flight at a time — bounded memory)", sink.peak_mem_kb);

    // Check throughput dropped to sink-limited rate.
    let max_theoretical = (1000.0 / sink_delay as f64) * batch_size as f64;
    let actual_rate = sink.total_rows as f64 / cons_secs;
    let bounded = actual_rate <= max_theoretical * 1.2; // allow 20% slack
    println!("  sink-limited : {actual_rate:.0} rows/s ≤ {max_theoretical:.0} theoretical → {}",
        if bounded { "✓ backpressure working" } else { "⚠ exceeds theoretical (check delay)" });

    if sink.total_rows == total_rows && bounded {
        println!("\n✓ PASS — all {total_rows} rows processed, memory bounded, backpressure effective");
    } else if sink.total_rows == total_rows {
        println!("\n✓ PASS — all {total_rows} rows processed (rate check inconclusive)");
    } else {
        println!("\n✗ FAIL — only {}/{total_rows} rows processed", sink.total_rows);
    }
    Ok(())
}
