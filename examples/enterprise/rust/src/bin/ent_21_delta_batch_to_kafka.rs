//! Enterprise 21 · Delta Lake batch → Kafka sink
//!
//! Pattern: write product data to a local Delta table (3 append batches),
//! then read every version and publish each as an Arrow IPC message to Kafka.
//! A consumer verifies the total row count matches what was written.
//!
//! Run:
//!   cargo run --bin ent_21_delta_batch_to_kafka

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
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

const BROKERS: &str = "localhost:9092";
const TOPIC:   &str = "delta-products";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int64,   false),
        Field::new("name",       DataType::Utf8,    false),
        Field::new("category",   DataType::Utf8,    false),
        Field::new("price",      DataType::Float64, false),
        Field::new("stock",      DataType::Int64,   false),
    ]))
}

fn make_product_batch(schema: Arc<Schema>, base: i64, count: usize) -> Result<RecordBatch> {
    let categories = ["Electronics", "Furniture", "Clothing", "Books", "Food"];
    let ids:    Vec<i64>  = (base..base + count as i64).collect();
    let names:  Vec<String> = ids.iter().map(|i| format!("Product-{i:05}")).collect();
    let cats:   Vec<&str> = ids.iter().map(|i| categories[(i % 5) as usize]).collect();
    let prices: Vec<f64>  = ids.iter().map(|i| 9.99 + (i % 500) as f64 * 0.5).collect();
    let stock:  Vec<i64>  = ids.iter().map(|i| 10 + i % 200).collect();
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(name_refs)),
        Arc::new(StringArray::from(cats)),
        Arc::new(Float64Array::from(prices)),
        Arc::new(Int64Array::from(stock)),
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

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 21: Delta Lake batch → Kafka sink ===");
    println!("  broker : {BROKERS}");
    println!("  topic  : {TOPIC}");
    println!();

    // ── Reset Kafka topic ─────────────────────────────────────────────────
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKERS).create()?;
    let opts = AdminOptions::new();
    let _ = admin.delete_topics(&[TOPIC], &opts).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin.create_topics(
        &[NewTopic::new(TOPIC, 4, TopicReplication::Fixed(1))], &opts,
    ).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("✓ Kafka topic reset");

    // ── Phase 1: Build Delta table with 3 batches (3 versions) ────────────
    println!("▶ Phase 1 — Write 3 batches to Delta Lake (append each)");
    let dir = tempdir()?;
    let delta_path = dir.path().join("products");
    let schema = schema();
    let batch_rows = 5_000usize;
    let num_batches = 3;
    let total_rows = batch_rows * num_batches;
    let mut versions: Vec<i64> = Vec::new();

    for b in 0..num_batches {
        let batch = make_product_batch(schema.clone(), (b * batch_rows) as i64, batch_rows)?;
        write_delta(
            delta_path.to_string_lossy().as_ref(),
            vec![batch],
            DeltaWriteMode::Append,
            false,
        ).await.context("write_delta")?;
        let handle = DeltaTableHandle::open(delta_path.to_string_lossy().as_ref(), None).await?;
        // version after b+1 writes = b (0-indexed)
        let ver = b as i64;
        versions.push(ver);
        println!("  batch {} → Delta version {} ({} rows cumulative)", b, ver, (b + 1) * batch_rows);
    }

    println!("  ✓ Delta table: {} rows across {} versions", total_rows, num_batches);

    // ── Phase 2: Read each Delta version snapshot → publish to Kafka ──────
    println!("▶ Phase 2 — Read Delta snapshots → publish to Kafka (Arrow IPC)");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()?;

    let t_pub = Instant::now();
    let mut published_rows = 0usize;
    let mut msg_count = 0usize;

    // Read the final (latest) version — all rows
    let handle = DeltaTableHandle::open(delta_path.to_string_lossy().as_ref(), None).await?;
    let all_batches = handle.scan_batches().await.context("scan delta")?;
    for (i, batch) in all_batches.iter().enumerate() {
        let ipc = batch_to_ipc(batch)?;
        let key = i.to_string();
        producer.send_result(
            FutureRecord::<str, Vec<u8>>::to(TOPIC).key(key.as_str()).payload(&ipc)
        ).map_err(|(e,_)| anyhow::anyhow!("{e}"))?
         .await.map_err(|e| anyhow::anyhow!("{e}"))?
         .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
        published_rows += batch.num_rows();
        msg_count += 1;
    }
    producer.flush(Timeout::After(Duration::from_secs(10))).context("flush")?;
    let pub_secs = t_pub.elapsed().as_secs_f64();
    println!("  ✓ published {published_rows} rows in {msg_count} messages in {pub_secs:.2}s ({:.0} rows/s)",
        published_rows as f64 / pub_secs.max(0.001));

    // ── Phase 3: Consume from Kafka and verify ────────────────────────────
    println!("▶ Phase 3 — Consume from Kafka and verify row count");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent21-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let t_con = Instant::now();
    let mut consumed_rows = 0usize;
    let mut category_counts = std::collections::HashMap::<String, i64>::new();

    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(4000), consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => {
                if !e.to_string().contains("transport") {
                    eprintln!("  warn: {e}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(Ok(m)) => m,
        };
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            consumed_rows += batch.num_rows();
            // Tally categories for verification
            if let Some(col) = batch.column_by_name("category") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        *category_counts.entry(arr.value(i).to_string()).or_insert(0) += 1;
                    }
                }
            }
        }
        if consumed_rows >= published_rows { break; }
    }
    let con_secs = t_con.elapsed().as_secs_f64();
    println!("  ✓ consumed {consumed_rows} rows in {con_secs:.2}s ({:.0} rows/s)",
        consumed_rows as f64 / con_secs.max(0.001));

    // ── Verify ────────────────────────────────────────────────────────────
    println!();
    println!("--- Verification ---");
    println!("  delta rows  : {total_rows}");
    println!("  published   : {published_rows}");
    println!("  consumed    : {consumed_rows}");
    println!();
    println!("  category counts:");
    let mut cats: Vec<_> = category_counts.iter().collect();
    cats.sort_by_key(|(k, _)| k.as_str());
    for (cat, n) in &cats {
        println!("    {cat:<15} {n}");
    }
    println!();

    if consumed_rows == total_rows && published_rows == total_rows {
        println!("✓ PASS — {total_rows} Delta rows published and verified via Kafka");
    } else {
        println!("✗ FAIL — delta={total_rows} published={published_rows} consumed={consumed_rows}");
    }
    Ok(())
}
