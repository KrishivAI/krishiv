//! Enterprise 13 · Kafka → PostgreSQL (transactional batch insert)
//!
//! Reads Arrow IPC batches from Kafka and inserts them into PostgreSQL using
//! array unnest for efficient bulk inserts inside a transaction. An offset
//! table tracks the last committed Kafka position per partition.
//!
//! Run:
//!   docker run -d --name pg -e POSTGRES_PASSWORD=pass -e POSTGRES_DB=krishiv \
//!     -p 5432:5432 postgres:16-alpine
//!   cargo run --bin ent_13_kafka_to_postgres
//!   LOAD_ROWS=100000 cargo run --bin ent_13_kafka_to_postgres

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
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use tokio_postgres::NoTls;

const BROKERS: &str = "localhost:9092";
const TOPIC:   &str = "orders-postgres";
const PG_URL:  &str = "host=localhost dbname=krishiv user=postgres password=pass";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("product",  DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
        Field::new("ts_ms",    DataType::Int64,   false),
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

async fn setup_postgres(client: &tokio_postgres::Client) -> Result<()> {
    client.batch_execute("
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGINT      PRIMARY KEY,
            customer TEXT        NOT NULL,
            product  TEXT        NOT NULL,
            amount   FLOAT8      NOT NULL,
            ts_ms    BIGINT      NOT NULL
        );
        CREATE TABLE IF NOT EXISTS kafka_offsets (
            part_id INT    PRIMARY KEY,
            next_offset BIGINT NOT NULL
        );
        TRUNCATE orders, kafka_offsets;
    ").await.context("setup postgres")
}

/// Bulk-insert a RecordBatch using unnest — fast and transactional.
async fn write_batch_pg(
    client:    &mut tokio_postgres::Client,
    batch:     &RecordBatch,
    partition: i32,
    offset:    i64,
) -> Result<()> {
    let ids    = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let custs  = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let prods  = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    let amts   = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
    let ts_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();

    let id_vec:   Vec<i64>  = (0..batch.num_rows()).map(|r| ids.value(r)).collect();
    let cust_vec: Vec<&str> = (0..batch.num_rows()).map(|r| custs.value(r)).collect();
    let prod_vec: Vec<&str> = (0..batch.num_rows()).map(|r| prods.value(r)).collect();
    let amt_vec:  Vec<f64>  = (0..batch.num_rows()).map(|r| amts.value(r)).collect();
    let ts_vec:   Vec<i64>  = (0..batch.num_rows()).map(|r| ts_col.value(r)).collect();

    let tx = client.transaction().await?;
    tx.execute(
        "INSERT INTO orders (order_id, customer, product, amount, ts_ms)
         SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[], $4::float8[], $5::bigint[])
         ON CONFLICT (order_id) DO NOTHING",
        &[&id_vec, &cust_vec, &prod_vec, &amt_vec, &ts_vec],
    ).await.context("unnest insert")?;

    tx.execute(
        "INSERT INTO kafka_offsets (part_id, next_offset)
         VALUES ($1, $2)
         ON CONFLICT (part_id) DO UPDATE SET next_offset = EXCLUDED.next_offset",
        &[&partition, &offset],
    ).await.context("offset commit")?;

    tx.commit().await.context("commit")
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(50_000);
    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(5_000);

    println!("=== Enterprise 13: Kafka → PostgreSQL ===");
    println!("  broker : {BROKERS}");
    println!("  topic  : {TOPIC}");
    println!("  pg_url : {PG_URL}");
    println!("  rows   : {total_rows}");
    println!();

    // ── Reset Kafka topic (delete + recreate for clean state) ─────────────
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .create()?;
    let opts = AdminOptions::new();
    let _ = admin.delete_topics(&[TOPIC], &opts).await; // ignore error if not exists
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin.create_topics(
        &[NewTopic::new(TOPIC, 4, TopicReplication::Fixed(1))],
        &opts,
    ).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("✓ Kafka topic reset");

    // ── Connect to PostgreSQL ───────────────────────────────────────────────
    let (mut pg, conn) = tokio_postgres::connect(PG_URL, NoTls)
        .await.context("connect postgres")?;
    tokio::spawn(async move { conn.await.expect("pg conn dropped") });
    setup_postgres(&pg).await?;
    println!("✓ PostgreSQL ready");

    // ── Phase 1: Produce ───────────────────────────────────────────────────
    println!("▶ Phase 1 — Produce {total_rows} rows");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()?;
    let schema = schema();
    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    let mut futs = Vec::with_capacity(16);
    for b in 0..num_batches {
        let rows  = batch_size.min(total_rows - b * batch_size);
        let batch = make_batch(schema.clone(), (b * batch_size) as i64, rows)?;
        let ipc   = batch_to_ipc(&batch)?;
        let key   = (b % 4).to_string();
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
    let prod_secs = t0.elapsed().as_secs_f64();
    println!("  ✓ produced in {prod_secs:.2}s ({:.0} rows/s)", total_rows as f64 / prod_secs);

    // ── Phase 2: Consume → PostgreSQL ─────────────────────────────────────
    println!("▶ Phase 2 — Consume → PostgreSQL (unnest batch insert + offset commit)");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent13-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let t1 = Instant::now();
    let mut consumed = 0usize;
    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(5000), consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => { eprintln!("  warn: {e}"); tokio::time::sleep(Duration::from_millis(200)).await; continue; }
            Ok(Ok(m)) => m,
        };
        let part   = msg.partition();
        let offset = msg.offset();
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            write_batch_pg(&mut pg, &batch, part, offset).await
                .with_context(|| format!("pg write part={part} off={offset}"))?;
            consumed += batch.num_rows();
        }
        if consumed % 10_000 == 0 {
            eprint!("\r  consumed {consumed}/{total_rows}   ");
        }
        if consumed >= total_rows { break; }
    }
    eprintln!();
    let cons_secs = t1.elapsed().as_secs_f64();
    println!("  ✓ {consumed} rows → PostgreSQL in {cons_secs:.2}s ({:.0} rows/s)",
        consumed as f64 / cons_secs);

    // ── Verify ─────────────────────────────────────────────────────────────
    let row = pg.query_one("SELECT COUNT(*) FROM orders", &[]).await?;
    let n: i64 = row.get(0);
    let top = pg.query(
        "SELECT customer, COUNT(*) AS cnt, ROUND(SUM(amount)::numeric,2)::float8 AS rev \
         FROM orders GROUP BY customer ORDER BY rev DESC",
        &[],
    ).await?;
    let offsets = pg.query("SELECT part_id, next_offset FROM kafka_offsets ORDER BY part_id", &[]).await?;

    println!();
    println!("--- PostgreSQL verification ---");
    println!("  total rows: {n}");
    println!();
    println!("  customer | orders | revenue");
    println!("  ---------|--------|--------");
    for r in &top {
        let c: &str = r.get(0);
        let cnt: i64 = r.get(1);
        let rev: f64 = r.get(2);
        println!("  {c:<8} | {cnt:>6} | {rev:.2}");
    }
    println!();
    println!("  committed Kafka offsets:");
    for r in &offsets {
        let part: i32 = r.get(0);
        let off:  i64 = r.get(1);
        println!("    partition {part} → offset {off}");
    }
    let e2e = prod_secs + cons_secs;
    println!();
    println!("▶ e2e: {:.0} rows/s in {e2e:.2}s", total_rows as f64 / e2e);
    if n == total_rows as i64 {
        println!("✓ row count correct: {n} == {total_rows}");
    } else {
        println!("⚠ row count mismatch: got {n}, expected {total_rows}");
    }
    Ok(())
}
