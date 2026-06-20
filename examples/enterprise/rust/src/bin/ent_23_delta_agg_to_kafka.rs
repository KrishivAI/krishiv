//! Enterprise 23 · Delta Lake SQL aggregation → Kafka results topic
//!
//! Pattern: ingest raw events into a Delta table, run SQL window aggregations
//! using Krishiv's embedded session, then publish the compact aggregated
//! results to a Kafka topic for downstream consumers.
//!
//! Stages:
//!   1. Write 100 000 raw sales events to Delta (5 batches of 20 000)
//!   2. Run SQL: revenue + order count per (category, month) window
//!   3. Publish aggregated rows to Kafka as JSON
//!   4. Consume and verify total revenue matches Delta source
//!
//! Run:
//!   cargo run --bin ent_23_delta_agg_to_kafka
//!   LOAD_ROWS=500000 cargo run --bin ent_23_delta_agg_to_kafka

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

const BROKERS:     &str = "localhost:9092";
const TOPIC:       &str = "delta-agg-results";

fn raw_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sale_id",   DataType::Int64,   false),
        Field::new("category",  DataType::Utf8,    false),
        Field::new("product",   DataType::Utf8,    false),
        Field::new("amount",    DataType::Float64, false),
        Field::new("month",     DataType::Int64,   false), // 1-12
        Field::new("ts_ms",     DataType::Int64,   false),
    ]))
}

fn make_sales_batch(schema: Arc<Schema>, base: i64, count: usize) -> Result<RecordBatch> {
    let categories = ["Electronics","Furniture","Clothing","Books","Food"];
    let products   = ["Laptop","Chair","Shirt","Novel","Coffee","TV","Desk","Jeans","Guide","Tea"];
    let ids:   Vec<i64>  = (base..base + count as i64).collect();
    let cats:  Vec<&str> = ids.iter().map(|i| categories[(i % 5) as usize]).collect();
    let prods: Vec<&str> = ids.iter().map(|i| products[(i % 10) as usize]).collect();
    let amts:  Vec<f64>  = ids.iter().map(|i| 5.0 + (i % 500) as f64 * 1.99).collect();
    let mnths: Vec<i64>  = ids.iter().map(|i| 1 + i % 12).collect();
    let ts:    Vec<i64>  = ids.iter().map(|i| 1_700_000_000_000 + i * 60_000).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(cats)),
        Arc::new(StringArray::from(prods)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(Int64Array::from(mnths)),
        Arc::new(Int64Array::from(ts)),
    ])?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let batch_size = 20_000usize;

    println!("=== Enterprise 23: Delta SQL agg → Kafka ===");
    println!("  broker    : {BROKERS}");
    println!("  topic     : {TOPIC}");
    println!("  raw rows  : {total_rows}");
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

    // ── Phase 1: Write raw sales events to Delta ──────────────────────────
    println!("▶ Phase 1 — Write {total_rows} raw sales events to Delta Lake");
    let dir = tempdir()?;
    let delta_path = dir.path().join("sales_raw").to_string_lossy().to_string();
    let schema = raw_schema();
    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    for b in 0..num_batches {
        let rows  = batch_size.min(total_rows - b * batch_size);
        let batch = make_sales_batch(schema.clone(), (b * batch_size) as i64, rows)?;
        write_delta(&delta_path, vec![batch], DeltaWriteMode::Append, false)
            .await.context("write_delta")?;
    }
    println!("  ✓ wrote {total_rows} rows in {:.2}s ({:.0} rows/s)",
        t0.elapsed().as_secs_f64(), total_rows as f64 / t0.elapsed().as_secs_f64().max(0.001));

    // ── Phase 2: SQL aggregation via Krishiv session ───────────────────────
    println!("▶ Phase 2 — SQL: GROUP BY category, month → revenue + orders");
    let t1 = Instant::now();
    let handle = DeltaTableHandle::open(&delta_path, None).await?;
    let raw_batches = handle.scan_batches().await.context("scan delta")?;
    let session = krishiv::Session::builder().build()?;
    session.register_record_batches("sales", raw_batches)?;
    let agg_df = session.sql(
        "SELECT category, month,
                COUNT(*)         AS order_count,
                ROUND(SUM(amount),2) AS revenue,
                ROUND(AVG(amount),2) AS avg_order
         FROM sales
         GROUP BY category, month
         ORDER BY category, month"
    )?;
    let agg_result = agg_df.collect()?;
    let agg_batches = agg_result.into_batches();
    let agg_rows: usize = agg_batches.iter().map(|b| b.num_rows()).sum();
    println!("  ✓ aggregation done in {:.2}s → {} result rows (5 categories × 12 months)",
        t1.elapsed().as_secs_f64(), agg_rows);

    // ── Phase 3: Publish agg results to Kafka (JSON) ──────────────────────
    println!("▶ Phase 3 — Publish {agg_rows} aggregated rows to Kafka");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .create()?;

    let t2 = Instant::now();
    let mut published = 0usize;
    let mut _total_revenue_published = 0.0f64;
    let mut futs = Vec::new();

    for batch in &agg_batches {
        let cats   = cast(batch.column(0), &DataType::Utf8)?;
        let months = cast(batch.column(1), &DataType::Int64)?;
        let cnts   = cast(batch.column(2), &DataType::Int64)?;
        let revs   = cast(batch.column(3), &DataType::Float64)?;
        let avgs   = cast(batch.column(4), &DataType::Float64)?;

        let cats  = cats.as_any().downcast_ref::<StringArray>().unwrap();
        let mnths = months.as_any().downcast_ref::<Int64Array>().unwrap();
        let cnts  = cnts.as_any().downcast_ref::<Int64Array>().unwrap();
        let revs  = revs.as_any().downcast_ref::<Float64Array>().unwrap();
        let avgs  = avgs.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            let rev = revs.value(i);
            _total_revenue_published += rev;
            let json = serde_json::json!({
                "category":    cats.value(i),
                "month":       mnths.value(i),
                "order_count": cnts.value(i),
                "revenue":     rev,
                "avg_order":   avgs.value(i),
            }).to_string();
            let key = format!("{}-{}", cats.value(i), mnths.value(i));
            let f = producer.send_result(
                FutureRecord::to(TOPIC).key(key.as_str()).payload(json.as_bytes())
            ).map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
            futs.push(f);
            published += 1;
            if futs.len() >= 64 {
                for f in futs.drain(..) {
                    f.await.map_err(|e| anyhow::anyhow!("{e}"))?
                     .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
                }
            }
        }
    }
    for f in futs.drain(..) {
        f.await.map_err(|e| anyhow::anyhow!("{e}"))?
         .map_err(|(e,_)| anyhow::anyhow!("{e}"))?;
    }
    producer.flush(Timeout::After(Duration::from_secs(10))).context("flush")?;
    println!("  ✓ published {published} rows in {:.2}s", t2.elapsed().as_secs_f64());

    // ── Phase 4: Consume and verify ───────────────────────────────────────
    println!("▶ Phase 4 — Consume and verify total revenue");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent23-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let mut consumed = 0usize;
    let mut total_revenue_consumed = 0.0f64;
    let mut cat_revenue: std::collections::HashMap<String, f64> = Default::default();

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
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) {
                let rev = v["revenue"].as_f64().unwrap_or(0.0);
                let cat = v["category"].as_str().unwrap_or("?").to_string();
                total_revenue_consumed += rev;
                *cat_revenue.entry(cat).or_insert(0.0) += rev;
                consumed += 1;
            }
        }
        if consumed >= published { break; }
    }

    // Cross-check: compute expected total revenue directly from source data
    let session2 = krishiv::Session::builder().build()?;
    let handle2  = DeltaTableHandle::open(&delta_path, None).await?;
    let raw2     = handle2.scan_batches().await?;
    session2.register_record_batches("sales2", raw2)?;
    let total_df = session2.sql("SELECT ROUND(SUM(amount),2) AS total FROM sales2")?;
    let total_res = total_df.collect()?;
    let total_batches = total_res.into_batches();
    let expected_rev: f64 = total_batches.iter()
        .flat_map(|b| {
            let col = b.column(0);
            let arr = cast(col, &DataType::Float64).ok()?;
            let arr = arr.as_any().downcast_ref::<Float64Array>()?;
            Some(arr.value(0))
        })
        .next().unwrap_or(0.0);

    println!();
    println!("--- Aggregation → Kafka results ---");
    println!("  raw rows in Delta   : {total_rows}");
    println!("  agg rows published  : {published}  (5 categories × 12 months)");
    println!("  agg rows consumed   : {consumed}");
    println!("  total revenue (Delta): ${expected_rev:.2}");
    println!("  total revenue (Kafka): ${total_revenue_consumed:.2}");
    println!();
    println!("  revenue by category:");
    let mut cats: Vec<_> = cat_revenue.iter().collect();
    cats.sort_by(|a,b| b.1.partial_cmp(a.1).unwrap());
    for (cat, rev) in &cats {
        println!("    {cat:<15} ${rev:.2}");
    }

    let rev_ok = (expected_rev - total_revenue_consumed).abs() < 1.0;
    let cnt_ok = consumed == published;
    println!();
    if rev_ok && cnt_ok {
        println!("✓ PASS — {total_rows} raw → {published} agg rows → Kafka; revenue matches");
    } else {
        println!("✗ FAIL — cnt_ok={cnt_ok} rev_ok={rev_ok}");
    }
    Ok(())
}
