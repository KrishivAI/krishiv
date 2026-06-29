//! Enterprise 12 · Kafka → Parquet (at-least-once, real broker)
//!
//! Demonstrates the `KafkaSink` / `KafkaSource` connector API against a live
//! Kafka broker. Each `write_batch` on `KafkaSink` waits for broker delivery
//! acknowledgement before returning — giving at-least-once per batch.
//!
//! Pipeline:
//!   KafkaSink  → produces each row as a JSON message, waits for ack
//!   KafkaSource → consumes JSON messages, one RecordBatch per poll
//!   ParquetSink → accumulates all batches into one output file
//!
//! Run:
//!   cargo run --bin ent_12_kafka_real_at_least_once
//!   LOAD_ROWS=10000 cargo run --bin ent_12_kafka_real_at_least_once

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_connectors::kafka::{KafkaConfig, KafkaSink, KafkaSource};
use krishiv_connectors::parquet::ParquetSink;
use krishiv_connectors::{ConnectorConfig, Sink, Source};
use tempfile::tempdir;

const TOPIC: &str = "orders-at-least-once";
const BROKERS: &str = "localhost:9092";

fn make_batch(schema: Arc<Schema>, start_id: i64, count: usize) -> Result<RecordBatch> {
    let ids: Vec<i64>    = (start_id..start_id + count as i64).collect();
    let customers        = ["alice", "bob", "carol", "dave"];
    let custs: Vec<&str> = ids.iter().map(|i| customers[(i % 4) as usize]).collect();
    let amounts: Vec<f64>= ids.iter().map(|i| 10.0 + (i % 100) as f64).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(Float64Array::from(amounts)),
    ])?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(2_000);
    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(200);

    println!("=== Enterprise 12: Kafka at-least-once (real broker) ===");
    println!("  broker     : {BROKERS}");
    println!("  topic      : {TOPIC}");
    println!("  total rows : {total_rows}");
    println!("  batch size : {batch_size} rows/batch");
    println!();

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
    ]));

    // ── Phase 1: Produce ──────────────────────────────────────────────────
    println!("▶ Phase 1 — Produce via KafkaSink (at-least-once, JSON row per message)");
    let sink_cfg = KafkaConfig::from_config(
        &ConnectorConfig::new("kafka-sink", "kafka")
            .with_property("bootstrap.servers", BROKERS)
            .with_property("topic", TOPIC)
            .with_property("group.id", "krishiv-ent12-sink")
            .with_property("message.timeout.ms", "30000"),
    ).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut sink = KafkaSink::new(sink_cfg).map_err(|e| anyhow::anyhow!("{e}"))?;

    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    for b in 0..num_batches {
        let rows  = batch_size.min(total_rows - b * batch_size);
        let batch = make_batch(schema.clone(), (b * batch_size) as i64, rows)?;
        sink.write_batch(batch).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        if b % 5 == 0 {
            eprint!("\r  produced {}/{} rows   ", (b + 1) * batch_size, total_rows);
        }
    }
    sink.flush().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    eprintln!();

    let prod_secs = t0.elapsed().as_secs_f64();
    println!("  ✓ {total_rows} rows in {prod_secs:.2}s  ({:.0} rows/s)",
        total_rows as f64 / prod_secs);
    println!();

    // ── Phase 2: Consume + Parquet ─────────────────────────────────────────
    println!("▶ Phase 2 — Consume via KafkaSource → ParquetSink");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let src_cfg = KafkaConfig::from_config(
        &ConnectorConfig::new("kafka-source", "kafka")
            .with_property("bootstrap.servers", BROKERS)
            .with_property("topic", TOPIC)
            .with_property("group.id", format!("krishiv-ent12-{run_id}"))
            .with_property("session.timeout.ms", "30000")
            .with_property("heartbeat.interval.ms", "3000"),
    ).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut source = KafkaSource::new(src_cfg).map_err(|e| anyhow::anyhow!("{e}"))?;

    let dir = tempdir()?;
    let out_path = dir.path().join("output.parquet");
    let mut parquet = ParquetSink::create(&out_path).context("create ParquetSink")?;

    // Allow time for initial group rebalance.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let t1 = Instant::now();
    let idle_timeout = std::time::Duration::from_millis(8000);
    let mut consumed_rows = 0usize;
    loop {
        let result = tokio::time::timeout(idle_timeout, source.read_batch()).await;
        let batch = match result {
            Err(_) => break, // idle — done
            Ok(Ok(Some(b))) => b,
            Ok(Ok(None)) => continue,
            Ok(Err(e)) => {
                eprintln!("  warn: {e} (retrying)");
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                continue;
            }
        };
        if batch.num_rows() == 0 { continue; }

        consumed_rows += batch.num_rows();
        parquet.write_batch(batch).await.map_err(|e| anyhow::anyhow!("{e}"))?;

        if consumed_rows % (batch_size * 10).max(100) == 0 {
            eprint!("\r  consumed {consumed_rows}/{total_rows} rows   ");
        }
        if consumed_rows >= total_rows { break; }
    }
    eprintln!();
    parquet.flush().await.map_err(|e| anyhow::anyhow!("{e}"))?;

    let cons_secs = t1.elapsed().as_secs_f64();
    println!("  ✓ {consumed_rows} rows → output.parquet in {cons_secs:.2}s  ({:.0} rows/s)",
        consumed_rows as f64 / cons_secs);
    println!();

    // ── Verify via SQL ─────────────────────────────────────────────────────
    let session = krishiv::Session::builder().build()?;
    let all_batches = session.read_parquet(&out_path)?.collect()?.into_batches();
    session.register_record_batches("orders", all_batches)?;
    // KafkaSink serialises rows as JSON; KafkaSource reads all fields back as
    // Utf8 strings (no schema preservation). Cast numeric columns explicitly.
    let df = session.sql(
        "SELECT customer, COUNT(*) AS orders, \
                ROUND(SUM(CAST(amount AS DOUBLE)), 2) AS revenue \
         FROM orders GROUP BY customer ORDER BY revenue DESC"
    )?;

    println!("--- Verification: revenue per customer ---");
    println!("{}", df.collect()?.pretty()?);

    if consumed_rows == total_rows {
        println!("✓ row count correct: {consumed_rows} == {total_rows}");
    } else {
        println!("⚠ row count mismatch: got {consumed_rows}, expected {total_rows}");
    }

    Ok(())
}
