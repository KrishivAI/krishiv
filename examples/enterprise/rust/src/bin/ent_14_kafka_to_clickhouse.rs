//! Enterprise 14 · Kafka → ClickHouse (high-throughput OLAP sink)
//!
//! Reads Arrow IPC batches from Kafka and bulk-inserts them into ClickHouse
//! via its HTTP interface (JSONEachRow format). Tests OLAP ingestion throughput.
//!
//! Run:
//!   docker run -d --name clickhouse -p 8123:8123 clickhouse/clickhouse-server:24
//!   cargo run --bin ent_14_kafka_to_clickhouse
//!   LOAD_ROWS=1000000 cargo run --bin ent_14_kafka_to_clickhouse

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
const TOPIC:   &str = "orders-clickhouse";
const CH_URL:  &str = "http://localhost:8123";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id",  DataType::Int64,   false),
        Field::new("customer",  DataType::Utf8,    false),
        Field::new("product",   DataType::Utf8,    false),
        Field::new("amount",    DataType::Float64, false),
        Field::new("ts_ms",     DataType::Int64,   false),
    ]))
}

fn make_batch(schema: Arc<Schema>, base: i64, size: usize) -> Result<RecordBatch> {
    let customers = ["alice","bob","carol","dave","eve","frank","grace","henry"];
    let products  = ["Laptop","Mouse","Chair","Monitor","Hub","Keyboard","Cam","Desk"];
    let ids:   Vec<i64>  = (base..base + size as i64).collect();
    let custs: Vec<&str> = ids.iter().map(|i| customers[(i % 8) as usize]).collect();
    let prods: Vec<&str> = ids.iter().map(|i| products[(i % 8) as usize]).collect();
    let amts:  Vec<f64>  = ids.iter().map(|i| 10.0 + (i % 1000) as f64 * 0.5).collect();
    let ts:    Vec<i64>  = ids.iter().map(|i| 1_716_200_000_000 + i * 100).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(StringArray::from(prods)),
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
        .context("ipc reader")?
        .map(|r| r.context("ipc batch"))
        .collect()
}

/// Serialise a RecordBatch to ClickHouse JSONEachRow format.
fn batch_to_json_each_row(batch: &RecordBatch) -> String {
    let ids    = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let custs  = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let prods  = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    let amts   = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
    let ts_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out = String::with_capacity(batch.num_rows() * 80);
    for r in 0..batch.num_rows() {
        out.push_str(&format!(
            "{{\"order_id\":{},\"customer\":\"{}\",\"product\":\"{}\",\"amount\":{},\"ts_ms\":{}}}\n",
            ids.value(r), custs.value(r), prods.value(r), amts.value(r), ts_col.value(r)
        ));
    }
    out
}

async fn ch_query(client: &reqwest::Client, sql: &str) -> Result<String> {
    client.post(CH_URL)
        .body(sql.to_string())
        .send().await?
        .text().await
        .context("ch query")
}

async fn setup_clickhouse(client: &reqwest::Client) -> Result<()> {
    ch_query(client, "
        CREATE TABLE IF NOT EXISTS orders (
            order_id  Int64,
            customer  String,
            product   String,
            amount    Float64,
            ts_ms     Int64
        ) ENGINE = MergeTree()
        ORDER BY (ts_ms, order_id)
    ").await?;
    ch_query(client, "TRUNCATE TABLE IF EXISTS orders").await?;
    Ok(())
}

async fn insert_batch(client: &reqwest::Client, body: String) -> Result<()> {
    let resp = client.post(format!("{CH_URL}/?query=INSERT+INTO+orders+FORMAT+JSONEachRow"))
        .body(body)
        .send().await.context("ch insert")?;
    if !resp.status().is_success() {
        let err = resp.text().await.unwrap_or_default();
        anyhow::bail!("ClickHouse insert error: {err}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(500_000);
    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(10_000);

    println!("=== Enterprise 14: Kafka → ClickHouse ===");
    println!("  broker  : {BROKERS}");
    println!("  topic   : {TOPIC}");
    println!("  ch_url  : {CH_URL}");
    println!("  rows    : {total_rows}");
    println!();

    let http = reqwest::Client::new();
    setup_clickhouse(&http).await.context("clickhouse setup")?;
    println!("✓ ClickHouse ready");

    // ── Phase 1: Produce ───────────────────────────────────────────────────
    println!("▶ Phase 1 — Produce {total_rows} rows to Kafka (Arrow IPC + lz4)");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()?;

    let schema = schema();
    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    let mut futures = Vec::with_capacity(32);
    for b in 0..num_batches {
        let rows = batch_size.min(total_rows - b * batch_size);
        let batch = make_batch(schema.clone(), (b * batch_size) as i64, rows)?;
        let ipc = batch_to_ipc(&batch)?;
        let key = (b % 4).to_string();
        let f = producer.send_result(
            FutureRecord::<str, Vec<u8>>::to(TOPIC).key(key.as_str()).payload(&ipc)
        ).map_err(|(e, _)| anyhow::anyhow!("{e}"))?;
        futures.push(f);
        if futures.len() >= 32 {
            for fut in futures.drain(..) {
                fut.await.map_err(|e| anyhow::anyhow!("{e}"))?
                   .map_err(|(e, _)| anyhow::anyhow!("{e}"))?;
            }
        }
    }
    for fut in futures.drain(..) {
        fut.await.map_err(|e| anyhow::anyhow!("{e}"))?
           .map_err(|(e, _)| anyhow::anyhow!("{e}"))?;
    }
    producer.flush(Timeout::After(Duration::from_secs(10))).context("flush")?;
    let prod_secs = t0.elapsed().as_secs_f64();
    println!("  ✓ produced in {prod_secs:.2}s ({:.0} rows/s)", total_rows as f64 / prod_secs);

    // ── Phase 2: Consume → ClickHouse ─────────────────────────────────────
    println!("▶ Phase 2 — Consume → ClickHouse (JSONEachRow bulk insert)");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent14-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let t1 = Instant::now();
    let mut consumed = 0usize;
    let mut inserts  = 0usize;
    loop {
        let msg = match tokio::time::timeout(
            Duration::from_millis(4000), consumer.recv()
        ).await {
            Err(_) => break,
            Ok(Err(e)) => { eprintln!("  warn: {e}"); tokio::time::sleep(Duration::from_millis(200)).await; continue; }
            Ok(Ok(m)) => m,
        };
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            let rows = batch.num_rows();
            let body = batch_to_json_each_row(&batch);
            insert_batch(&http, body).await?;
            consumed += rows;
            inserts  += 1;
        }
        if consumed % 100_000 == 0 {
            eprint!("\r  consumed {consumed}/{total_rows}  {inserts} inserts   ");
        }
        if consumed >= total_rows { break; }
    }
    eprintln!();
    let cons_secs = t1.elapsed().as_secs_f64();
    println!("  ✓ {consumed} rows → ClickHouse in {cons_secs:.2}s ({:.0} rows/s, {inserts} batches)",
        consumed as f64 / cons_secs);

    // ── Verify ─────────────────────────────────────────────────────────────
    tokio::time::sleep(Duration::from_millis(500)).await; // allow CH to merge
    let count_resp = ch_query(&http, "SELECT COUNT(*) FROM orders FORMAT TabSeparated").await?;
    let count: u64 = count_resp.trim().parse().unwrap_or(0);
    let rev_resp   = ch_query(&http, "SELECT ROUND(SUM(amount),2) FROM orders FORMAT TabSeparated").await?;
    let top_resp   = ch_query(&http,
        "SELECT customer, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS rev \
         FROM orders GROUP BY customer ORDER BY rev DESC LIMIT 8 FORMAT TabSeparated"
    ).await?;

    println!();
    println!("--- ClickHouse verification ---");
    println!("  total rows   : {count}");
    println!("  total revenue: {}", rev_resp.trim());
    println!();
    println!("  customer | count  | revenue");
    println!("  ---------|--------|--------");
    for line in top_resp.trim().lines() {
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() >= 3 {
            println!("  {:<8} | {:>6} | {}", cols[0], cols[1], cols[2]);
        }
    }

    // e2e stats
    let e2e = prod_secs + cons_secs;
    println!();
    println!("▶ End-to-end: {:.0} rows/s in {e2e:.2}s", total_rows as f64 / e2e);

    if count == total_rows as u64 {
        println!("✓ row count correct: {count} == {total_rows}");
    } else {
        println!("⚠ row count mismatch: got {count}, expected {total_rows}");
    }
    Ok(())
}
