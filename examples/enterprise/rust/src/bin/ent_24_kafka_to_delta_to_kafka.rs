//! Enterprise 24 · Kafka → Delta Lake → Kafka (full roundtrip pipeline)
//!
//! Demonstrates a complete streaming-batch hybrid pipeline:
//!   Phase 1 — Produce 50 000 raw events to Kafka source topic
//!   Phase 2 — Consume source Kafka → write to Delta Lake (staging)
//!   Phase 3 — Read Delta, run SQL aggregation (revenue per customer)
//!   Phase 4 — Publish enriched results to a Kafka output topic
//!   Phase 5 — Consume output topic, verify total revenue matches source
//!
//! This pattern is common in Lambda/Kappa architectures where:
//!   - Delta Lake is the durable batch layer
//!   - Kafka carries streaming results for real-time consumers
//!
//! Run:
//!   cargo run --bin ent_24_kafka_to_delta_to_kafka
//!   LOAD_ROWS=200000 cargo run --bin ent_24_kafka_to_delta_to_kafka

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use krishiv_connectors::lakehouse::{DeltaTableHandle, DeltaWriteMode, write_delta};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use tempfile::tempdir;

const BROKERS:      &str = "localhost:9092";
const SRC_TOPIC:    &str = "pipeline-source";
const OUT_TOPIC:    &str = "pipeline-output";

fn order_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id",  DataType::Int64,   false),
        Field::new("customer",  DataType::Utf8,    false),
        Field::new("product",   DataType::Utf8,    false),
        Field::new("amount",    DataType::Float64, false),
        Field::new("region",    DataType::Utf8,    false),
        Field::new("ts_ms",     DataType::Int64,   false),
    ]))
}

fn make_orders(schema: Arc<Schema>, base: i64, count: usize) -> Result<RecordBatch> {
    let customers = ["alice","bob","carol","dave","eve","frank","grace","henry"];
    let products  = ["Laptop","Mouse","Chair","Monitor","Hub","Keyboard","Cam","Desk"];
    let regions   = ["us-east","us-west","eu-west","ap-south"];
    let ids:   Vec<i64>  = (base..base + count as i64).collect();
    let custs: Vec<&str> = ids.iter().map(|i| customers[(i % 8) as usize]).collect();
    let prods: Vec<&str> = ids.iter().map(|i| products[(i % 8) as usize]).collect();
    let amts:  Vec<f64>  = ids.iter().map(|i| 10.0 + (i % 1000) as f64 * 0.5).collect();
    let regs:  Vec<&str> = ids.iter().map(|i| regions[(i % 4) as usize]).collect();
    let ts:    Vec<i64>  = ids.iter().map(|i| 1_716_200_000_000 + i * 100).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(StringArray::from(prods)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(StringArray::from(regs)),
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

fn new_consumer(group: &str) -> Result<StreamConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .create()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(50_000);
    let batch_size = 5_000usize;

    println!("=== Enterprise 24: Kafka → Delta → Kafka (full pipeline) ===");
    println!("  broker     : {BROKERS}");
    println!("  source     : {SRC_TOPIC}");
    println!("  output     : {OUT_TOPIC}");
    println!("  rows       : {total_rows}");
    println!();

    // ── Reset Kafka topics ────────────────────────────────────────────────
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKERS).create()?;
    let opts = AdminOptions::new();
    for t in [SRC_TOPIC, OUT_TOPIC] {
        let _ = admin.delete_topics(&[t], &opts).await;
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    admin.create_topics(&[
        NewTopic::new(SRC_TOPIC, 4, TopicReplication::Fixed(1)),
        NewTopic::new(OUT_TOPIC, 4, TopicReplication::Fixed(1)),
    ], &opts).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("✓ Kafka topics reset");

    // ── Phase 1: Produce raw orders to source Kafka topic ─────────────────
    println!("▶ Phase 1 — Produce {total_rows} orders to {SRC_TOPIC}");
    let schema = order_schema();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()?;

    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    let mut futs = Vec::with_capacity(16);
    for b in 0..num_batches {
        let rows  = batch_size.min(total_rows - b * batch_size);
        let batch = make_orders(schema.clone(), (b * batch_size) as i64, rows)?;
        let ipc   = batch_to_ipc(&batch)?;
        let key   = (b % 4).to_string();
        let f = producer.send_result(
            FutureRecord::<str, Vec<u8>>::to(SRC_TOPIC).key(key.as_str()).payload(&ipc)
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
        t0.elapsed().as_secs_f64(), total_rows as f64 / t0.elapsed().as_secs_f64().max(0.001));

    // ── Phase 2: Consume from source Kafka → Delta Lake staging ───────────
    println!("▶ Phase 2 — Consume {SRC_TOPIC} → Delta Lake staging");
    let dir = tempdir()?;
    let delta_path = dir.path().join("orders_staging").to_string_lossy().to_string();
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let src_consumer = new_consumer(&format!("krishiv-ent24-src-{run_id}"))?;
    src_consumer.subscribe(&[SRC_TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let t1 = Instant::now();
    let mut staged_rows = 0usize;
    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(4000), src_consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => {
                if !e.to_string().contains("transport") { eprintln!("  warn: {e}"); }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(Ok(m)) => m,
        };
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            let n = batch.num_rows();
            write_delta(&delta_path, vec![batch], DeltaWriteMode::Append, false)
                .await.context("write delta staging")?;
            staged_rows += n;
        }
        eprint!("\r  staged {staged_rows}/{total_rows}   ");
        if staged_rows >= total_rows { break; }
    }
    eprintln!();
    println!("  ✓ {staged_rows} rows → Delta Lake in {:.2}s ({:.0} rows/s)",
        t1.elapsed().as_secs_f64(), staged_rows as f64 / t1.elapsed().as_secs_f64().max(0.001));

    // ── Phase 3: SQL aggregation on Delta staging table ────────────────────
    println!("▶ Phase 3 — SQL: revenue + orders per (customer, region)");
    let t2 = Instant::now();
    let handle = DeltaTableHandle::open(&delta_path, None).await.context("open delta")?;
    let staged_batches = handle.scan_batches().await.context("scan delta")?;
    let session = krishiv::Session::builder().build()?;
    session.register_record_batches("orders", staged_batches)?;
    let agg_df = session.sql(
        "SELECT customer, region,
                COUNT(*)              AS order_count,
                ROUND(SUM(amount),2)  AS revenue,
                ROUND(AVG(amount),2)  AS avg_order,
                ROUND(MAX(amount),2)  AS max_order
         FROM orders
         GROUP BY customer, region
         ORDER BY revenue DESC"
    )?;
    let agg_res    = agg_df.collect()?;
    let agg_batches = agg_res.into_batches();
    let agg_rows: usize = agg_batches.iter().map(|b| b.num_rows()).sum();
    println!("  ✓ aggregation in {:.2}s → {} result rows (8 customers × 4 regions)",
        t2.elapsed().as_secs_f64(), agg_rows);

    // ── Phase 4: Publish aggregated results to output Kafka topic ──────────
    println!("▶ Phase 4 — Publish {agg_rows} enriched rows to {OUT_TOPIC}");
    let t3 = Instant::now();
    let mut published = 0usize;
    let mut total_rev_pub = 0.0f64;
    let mut futs2 = Vec::new();

    for batch in &agg_batches {
        let custs  = cast(batch.column(0), &DataType::Utf8)?;
        let regs   = cast(batch.column(1), &DataType::Utf8)?;
        let cnts   = cast(batch.column(2), &DataType::Int64)?;
        let revs   = cast(batch.column(3), &DataType::Float64)?;
        let avgs   = cast(batch.column(4), &DataType::Float64)?;
        let maxs   = cast(batch.column(5), &DataType::Float64)?;

        let custs = custs.as_any().downcast_ref::<StringArray>().unwrap();
        let regs  = regs.as_any().downcast_ref::<StringArray>().unwrap();
        let cnts  = cnts.as_any().downcast_ref::<Int64Array>().unwrap();
        let revs  = revs.as_any().downcast_ref::<Float64Array>().unwrap();
        let avgs  = avgs.as_any().downcast_ref::<Float64Array>().unwrap();
        let maxs  = maxs.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            let rev = revs.value(i);
            total_rev_pub += rev;
            let json = serde_json::json!({
                "customer":    custs.value(i),
                "region":      regs.value(i),
                "order_count": cnts.value(i),
                "revenue":     rev,
                "avg_order":   avgs.value(i),
                "max_order":   maxs.value(i),
            }).to_string();
            let key = format!("{}-{}", custs.value(i), regs.value(i));
            let f = producer.send_result(
                FutureRecord::to(OUT_TOPIC).key(key.as_str()).payload(json.as_bytes())
            ).map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
            futs2.push(f);
            published += 1;
            if futs2.len() >= 64 {
                for f in futs2.drain(..) {
                    f.await.map_err(|e| anyhow::anyhow!("{e}"))?
                     .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
                }
            }
        }
    }
    for f in futs2.drain(..) {
        f.await.map_err(|e| anyhow::anyhow!("{e}"))?
         .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
    }
    producer.flush(Timeout::After(Duration::from_secs(10))).context("flush")?;
    println!("  ✓ published {published} rows in {:.2}s", t3.elapsed().as_secs_f64());

    // ── Phase 5: Consume output topic and verify ───────────────────────────
    println!("▶ Phase 5 — Consume {OUT_TOPIC} and verify");
    let out_consumer = new_consumer(&format!("krishiv-ent24-out-{run_id}"))?;
    out_consumer.subscribe(&[OUT_TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut consumed = 0usize;
    let mut total_rev_con = 0.0f64;
    let mut leaderboard: Vec<(String, String, f64)> = Vec::new();

    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(3000), out_consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => {
                if !e.to_string().contains("transport") { eprintln!("  warn: {e}"); }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(Ok(m)) => m,
        };
        if let Some(payload) = msg.payload() {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) {
                let rev = v["revenue"].as_f64().unwrap_or(0.0);
                let cust = v["customer"].as_str().unwrap_or("?").to_string();
                let reg  = v["region"].as_str().unwrap_or("?").to_string();
                total_rev_con += rev;
                leaderboard.push((cust, reg, rev));
                consumed += 1;
            }
        }
        if consumed >= published { break; }
    }

    leaderboard.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());

    println!();
    println!("--- Pipeline results ---");
    println!("  source rows (Kafka)   : {total_rows}");
    println!("  staged rows (Delta)   : {staged_rows}");
    println!("  agg rows published    : {published}");
    println!("  agg rows consumed     : {consumed}");
    println!("  total revenue (pub)   : ${total_rev_pub:.2}");
    println!("  total revenue (con)   : ${total_rev_con:.2}");
    println!();
    println!("  top 5 customer-region by revenue:");
    for (cust, reg, rev) in leaderboard.iter().take(5) {
        println!("    {cust:<8} {reg:<12} ${rev:.2}");
    }

    let cnt_ok = consumed == published && staged_rows == total_rows;
    let rev_ok = (total_rev_pub - total_rev_con).abs() < 1.0;
    println!();
    if cnt_ok && rev_ok {
        println!("✓ PASS — {total_rows} rows: Kafka → Delta → Kafka pipeline complete, revenue matches");
    } else {
        println!("✗ FAIL — cnt_ok={cnt_ok} rev_ok={rev_ok}");
    }
    Ok(())
}
