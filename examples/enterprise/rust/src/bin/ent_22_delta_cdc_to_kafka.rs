//! Enterprise 22 · Delta Lake CDC diff → Kafka (time-travel change feed)
//!
//! Pattern: detect row-level changes between two Delta versions using
//! time-travel, encode them as CDC events (op=INSERT/UPDATE/DELETE),
//! and publish to Kafka as newline-delimited JSON.
//!
//! Stages:
//!   1. Write V0: initial orders snapshot (100 rows)
//!   2. Write V1: partial update + new inserts + deletes (simulate day-2 ops)
//!   3. Diff V0 → V1 using order_id as primary key
//!   4. Publish CDC events to Kafka (INSERT / UPDATE / DELETE)
//!   5. Consume and verify change counts
//!
//! Run:
//!   cargo run --bin ent_22_delta_cdc_to_kafka

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
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
const TOPIC:   &str = "delta-cdc";

#[derive(Debug, serde::Serialize)]
struct CdcEvent {
    op:         String, // INSERT / UPDATE / DELETE
    order_id:   i64,
    customer:   String,
    amount:     f64,
    status:     String,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
        Field::new("status",   DataType::Utf8,    false),
    ]))
}

fn orders_batch(schema: Arc<Schema>, base: i64, count: usize, status: &str) -> Result<RecordBatch> {
    let customers = ["alice","bob","carol","dave","eve"];
    let ids:    Vec<i64>  = (base..base + count as i64).collect();
    let custs:  Vec<&str> = ids.iter().map(|i| customers[(i % 5) as usize]).collect();
    let amts:   Vec<f64>  = ids.iter().map(|i| 50.0 + (i % 200) as f64 * 2.5).collect();
    let stats:  Vec<&str> = vec![status; count];
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(StringArray::from(stats)),
    ])?)
}

/// Extract all rows from a RecordBatch into a map keyed by order_id.
fn to_row_map(batch: &RecordBatch) -> Result<HashMap<i64, CdcEvent>> {
    let ids   = cast(batch.column(0), &DataType::Int64)?;
    let custs = cast(batch.column(1), &DataType::Utf8)?;
    let amts  = cast(batch.column(2), &DataType::Float64)?;
    let stats = cast(batch.column(3), &DataType::Utf8)?;

    let ids   = ids.as_any().downcast_ref::<Int64Array>().unwrap();
    let custs = custs.as_any().downcast_ref::<StringArray>().unwrap();
    let amts  = amts.as_any().downcast_ref::<Float64Array>().unwrap();
    let stats = stats.as_any().downcast_ref::<StringArray>().unwrap();

    let mut map = HashMap::new();
    for i in 0..batch.num_rows() {
        let id = ids.value(i);
        map.insert(id, CdcEvent {
            op: String::new(),
            order_id: id,
            customer: custs.value(i).to_string(),
            amount:   amts.value(i),
            status:   stats.value(i).to_string(),
        });
    }
    Ok(map)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 22: Delta CDC diff → Kafka ===");
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

    let dir = tempdir()?;
    let path = dir.path().join("orders_delta");
    let delta_path = path.to_string_lossy().to_string();
    let schema = schema();

    // ── Phase 1: V0 — initial 100 orders (pending) ────────────────────────
    println!("▶ Phase 1 — Write V0: 100 orders (status=pending)");
    let v0_batch = orders_batch(schema.clone(), 0, 100, "pending")?;
    write_delta(&delta_path, vec![v0_batch], DeltaWriteMode::Append, false)
        .await.context("write v0")?;

    // ── Phase 2: V1 — overwrite: update 80 to shipped, delete 10, add 20 ─
    println!("▶ Phase 2 — Write V1: 80 updated (shipped) + 20 new orders");
    let updated  = orders_batch(schema.clone(), 0,   80, "shipped")?; // orders 0-79 updated
    let new_ones = orders_batch(schema.clone(), 100, 20, "pending")?; // orders 100-119 new
    // Merge updated + new into one batch for V1
    let combined = arrow::compute::concat_batches(&schema, &[updated, new_ones])?;
    // Write V1 as overwrite (simulates full snapshot replacement)
    write_delta(&delta_path, vec![combined], DeltaWriteMode::Overwrite, false)
        .await.context("write v1")?;

    println!("  V0: 100 rows (orders 0-99)");
    println!("  V1: 100 rows (orders 0-79 shipped, orders 100-119 pending)");
    println!("  Expected changes: 80 UPDATEs, 20 DELETEs (80-99), 20 INSERTs (100-119)");

    // ── Phase 3: Diff V0 → V1 ─────────────────────────────────────────────
    println!("▶ Phase 3 — Diff V0 → V1 via Delta time-travel");
    let v0_handle = DeltaTableHandle::open(&delta_path, Some(0)).await?;
    let v1_handle = DeltaTableHandle::open(&delta_path, None).await?;

    let v0_batches = v0_handle.scan_batches().await.context("scan v0")?;
    let v1_batches = v1_handle.scan_batches().await.context("scan v1")?;

    let v0_rows: HashMap<i64, CdcEvent> = v0_batches.iter()
        .map(|b| to_row_map(b))
        .collect::<Result<Vec<_>>>()?
        .into_iter().flatten().collect();
    let v1_rows: HashMap<i64, CdcEvent> = v1_batches.iter()
        .map(|b| to_row_map(b))
        .collect::<Result<Vec<_>>>()?
        .into_iter().flatten().collect();

    let mut events: Vec<CdcEvent> = Vec::new();
    // INSERTs: in V1 but not in V0
    for (_id, row) in v1_rows.iter().filter(|(id,_)| !v0_rows.contains_key(id)) {
        events.push(CdcEvent { op: "INSERT".into(), order_id: row.order_id,
            customer: row.customer.clone(), amount: row.amount, status: row.status.clone() });
    }
    // DELETEs: in V0 but not in V1
    for (_id, row) in v0_rows.iter().filter(|(id,_)| !v1_rows.contains_key(id)) {
        events.push(CdcEvent { op: "DELETE".into(), order_id: row.order_id,
            customer: row.customer.clone(), amount: row.amount, status: row.status.clone() });
    }
    // UPDATEs: in both but different status
    for (id, v1_row) in &v1_rows {
        if let Some(v0_row) = v0_rows.get(id) {
            if v0_row.status != v1_row.status {
                events.push(CdcEvent { op: "UPDATE".into(), order_id: v1_row.order_id,
                    customer: v1_row.customer.clone(), amount: v1_row.amount, status: v1_row.status.clone() });
            }
        }
    }

    let inserts = events.iter().filter(|e| e.op == "INSERT").count();
    let updates = events.iter().filter(|e| e.op == "UPDATE").count();
    let deletes = events.iter().filter(|e| e.op == "DELETE").count();
    println!("  diff complete: {} INSERTs, {} UPDATEs, {} DELETEs", inserts, updates, deletes);

    // ── Phase 4: Publish CDC events to Kafka (JSON lines) ─────────────────
    println!("▶ Phase 4 — Publish {} CDC events to Kafka (JSON)", events.len());
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .create()?;

    let t_pub = Instant::now();
    let mut futs = Vec::new();
    for event in &events {
        let json = serde_json::to_string(event)?;
        let key  = event.order_id.to_string();
        let f = producer.send_result(
            FutureRecord::to(TOPIC).key(key.as_str()).payload(json.as_bytes())
        ).map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
        futs.push(f);
        if futs.len() >= 64 {
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
    println!("  ✓ published in {:.2}s ({:.0} events/s)",
        t_pub.elapsed().as_secs_f64(),
        events.len() as f64 / t_pub.elapsed().as_secs_f64().max(0.001));

    // ── Phase 5: Consume and verify ───────────────────────────────────────
    println!("▶ Phase 5 — Consume from Kafka and verify");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent22-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut seen_ops = HashMap::<String, usize>::new();
    let mut total_consumed = 0usize;
    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(3000), consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => {
                if !e.to_string().contains("transport") { eprintln!("  warn: {e}"); }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(Ok(m)) => m,
        };
        if let Some(payload) = msg.payload() {
            if let Ok(ev) = serde_json::from_slice::<serde_json::Value>(payload) {
                let op = ev["op"].as_str().unwrap_or("?").to_string();
                *seen_ops.entry(op).or_insert(0) += 1;
                total_consumed += 1;
            }
        }
        if total_consumed >= events.len() { break; }
    }

    println!();
    println!("--- CDC change feed results ---");
    println!("  total events published : {}", events.len());
    println!("  total events consumed  : {total_consumed}");
    println!();
    println!("  op       | sent | received");
    println!("  ---------|------|----------");
    for op in ["INSERT","UPDATE","DELETE"] {
        let sent = events.iter().filter(|e| e.op == op).count();
        let recv = seen_ops.get(op).copied().unwrap_or(0);
        let ok = if sent == recv { "✓" } else { "✗" };
        println!("  {op:<8} | {sent:>4} | {recv:>4}  {ok}");
    }

    let ok = total_consumed == events.len();
    println!();
    if ok {
        println!("✓ PASS — all {} CDC events published and consumed correctly", events.len());
    } else {
        println!("✗ FAIL — published={} consumed={}", events.len(), total_consumed);
    }
    Ok(())
}
