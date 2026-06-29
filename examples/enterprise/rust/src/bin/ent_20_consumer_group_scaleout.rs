//! Enterprise 20 · Consumer group scale-out — 2 parallel consumers
//!
//! Produces 100 000 rows across 4 Kafka partitions, then runs 2 independent
//! consumers in the SAME consumer group concurrently. Each consumer gets ~2
//! partitions. Verifies:
//!   1. Total rows across both consumers == produced rows (no loss).
//!   2. No order_id appears in both consumers (no duplicates).
//!   3. Partition assignment is balanced (each consumer gets ≥1 partition).
//!
//! Run:
//!   cargo run --bin ent_20_consumer_group_scaleout

use std::collections::HashSet;
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

const BROKERS:    &str = "localhost:9092";
const TOPIC:      &str = "orders-scaleout";
const TOTAL_ROWS: usize = 100_000;
const BATCH_SIZE: usize = 5_000;
const GROUP_ID:   &str = "krishiv-scaleout";

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
    let ts:    Vec<i64>  = ids.iter().map(|i| 1_716_200_000_000 + i * 1_000).collect();
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

fn new_consumer(name: &str) -> Result<StreamConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", GROUP_ID)
        .set("client.id", name)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "3000")
        .set("max.poll.interval.ms", "300000")
        .create()?)
}

async fn run_consumer(
    consumer: StreamConsumer,
    name: String,
    expected_total: usize,
) -> Result<(Vec<i64>, Vec<i32>)> {
    consumer.subscribe(&[TOPIC])?;
    let mut order_ids  = Vec::<i64>::new();
    let mut partitions = Vec::<i32>::new();
    let t = Instant::now();
    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(6000), consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => {
                if !e.to_string().contains("transport") {
                    eprintln!("  [{name}] error: {e}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(Ok(m)) => m,
        };
        let part = msg.partition();
        if !partitions.contains(&part) { partitions.push(part); }
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..ids.len() { order_ids.push(ids.value(i)); }
        }
        if order_ids.len() % 20_000 == 0 {
            eprint!("\r  [{name}] {}/{expected_total} rows  {:.1}s   ",
                order_ids.len(), t.elapsed().as_secs_f64());
        }
    }
    eprintln!();
    println!("  [{name}] finished: {} rows from partitions {:?}  ({:.2}s)",
        order_ids.len(), {let mut p = partitions.clone(); p.sort(); p}, t.elapsed().as_secs_f64());
    Ok((order_ids, partitions))
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 20: Consumer Group Scale-Out ===");
    println!("  broker      : {BROKERS}");
    println!("  topic       : {TOPIC} (4 partitions)");
    println!("  group       : {GROUP_ID}");
    println!("  consumers   : 2 (parallel, same group)");
    println!("  total rows  : {TOTAL_ROWS}");
    println!();

    // ── Phase 1: Produce ───────────────────────────────────────────────────
    println!("▶ Phase 1 — Produce {TOTAL_ROWS} rows (4 partitions, round-robin)");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()?;
    let schema = schema();
    let t0 = Instant::now();
    let num_batches = TOTAL_ROWS.div_ceil(BATCH_SIZE);
    let mut futs = Vec::new();
    for b in 0..num_batches {
        let rows  = BATCH_SIZE.min(TOTAL_ROWS - b * BATCH_SIZE);
        let batch = make_batch(schema.clone(), (b * BATCH_SIZE) as i64, rows)?;
        let ipc   = batch_to_ipc(&batch)?;
        let key   = (b % 4).to_string(); // distribute evenly across 4 partitions
        let f = producer.send_result(
            FutureRecord::<str, Vec<u8>>::to(TOPIC).key(key.as_str()).payload(&ipc)
        ).map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
        futs.push(f);
        if futs.len() >= 16 {
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
    println!("  ✓ produced in {:.2}s ({:.0} rows/s)",
        t0.elapsed().as_secs_f64(), TOTAL_ROWS as f64 / t0.elapsed().as_secs_f64());

    // ── Phase 2: 2 consumers in parallel ──────────────────────────────────
    println!("▶ Phase 2 — 2 parallel consumers, same group (waiting for rebalance…)");
    let c1 = new_consumer("consumer-1")?;
    let c2 = new_consumer("consumer-2")?;
    // Start both, then wait for rebalance to complete before polling.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let half = TOTAL_ROWS / 2;
    let t1 = Instant::now();
    let (r1, r2) = tokio::join!(
        run_consumer(c1, "C1".into(), half),
        run_consumer(c2, "C2".into(), half),
    );
    let (ids1, parts1) = r1?;
    let (ids2, parts2) = r2?;
    let total_time = t1.elapsed().as_secs_f64();

    // ── Verify ─────────────────────────────────────────────────────────────
    let total_consumed = ids1.len() + ids2.len();
    let set1: HashSet<i64> = ids1.iter().copied().collect();
    let set2: HashSet<i64> = ids2.iter().copied().collect();
    let duplicates: HashSet<_> = set1.intersection(&set2).collect();
    let all_ids: HashSet<i64> = set1.union(&set2).copied().collect();

    println!();
    println!("--- Consumer Group Scale-Out Results ---");
    println!("  C1: {} rows from partitions {:?}", ids1.len(), {let mut p=parts1.clone();p.sort();p});
    println!("  C2: {} rows from partitions {:?}", ids2.len(), {let mut p=parts2.clone();p.sort();p});
    println!("  total consumed : {total_consumed}");
    println!("  unique rows    : {}", all_ids.len());
    println!("  duplicates     : {}", duplicates.len());
    println!("  time           : {total_time:.2}s");
    println!("  combined rate  : {:.0} rows/s", total_consumed as f64 / total_time);

    let partition_split = !parts1.is_empty() && !parts2.is_empty();
    let no_dups  = duplicates.is_empty();
    let complete  = total_consumed == TOTAL_ROWS;

    println!();
    if partition_split { println!("  ✓ partitions split across consumers"); }
    else               { println!("  ⚠ one consumer got all partitions"); }
    if no_dups   { println!("  ✓ no duplicate order_ids"); }
    else         { println!("  ✗ {} duplicate order_ids!", duplicates.len()); }
    if complete  { println!("  ✓ all {TOTAL_ROWS} rows consumed"); }
    else         { println!("  ⚠ {total_consumed}/{TOTAL_ROWS} rows consumed"); }

    if no_dups && complete {
        println!("\n✓ PASS — 2 consumers partitioned {TOTAL_ROWS} rows with no gaps or duplicates");
    } else {
        println!("\n✗ FAIL — see details above");
    }
    Ok(())
}
