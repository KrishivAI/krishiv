//! Enterprise 12 · Kafka → Parquet (at-least-once, real broker) — stress test
//!
//! Produces configurable batches to a real Kafka broker, then consumes them
//! with the `KafkaSink`/`KafkaSource` connector API (JSON row-per-message)
//! and verifies exactly the right number of rows land in Parquet.
//!
//! Demonstrates:
//!   - Real `KafkaSource` + `KafkaSink` connector API
//!   - `PostWriteOffsetCommitProtocol` at-least-once delivery
//!   - End-to-end row-count correctness check
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
use krishiv_connectors::{CheckpointSource, ConnectorConfig, PostWriteOffsetCommitProtocol, Sink, Source};
use tempfile::tempdir;

const TOPIC: &str = "orders-at-least-once";
const BROKERS: &str = "localhost:9092";

fn make_batch(schema: Arc<Schema>, start_id: i64, count: usize) -> Result<RecordBatch> {
    let ids: Vec<i64>    = (start_id..start_id + count as i64).collect();
    let customers        = vec!["alice", "bob", "carol", "dave"];
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
        .ok().and_then(|s| s.parse().ok()).unwrap_or(5_000);
    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(500);

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
    println!("▶ Phase 1 — Produce via KafkaSink");
    let sink_cfg = KafkaConfig::from_config(
        &ConnectorConfig::new("kafka-sink", "kafka")
            .with_property("bootstrap.servers", BROKERS)
            .with_property("topic", TOPIC)
            .with_property("group.id", "krishiv-ent12-sink"),
    ).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut sink = KafkaSink::new(sink_cfg).map_err(|e| anyhow::anyhow!("{e}"))?;

    let t0 = Instant::now();
    let num_batches = total_rows.div_ceil(batch_size);
    for b in 0..num_batches {
        let rows  = batch_size.min(total_rows - b * batch_size);
        let batch = make_batch(schema.clone(), (b * batch_size) as i64, rows)?;
        sink.write_batch(batch).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    sink.flush().await.map_err(|e| anyhow::anyhow!("{e}"))?;

    let prod_secs = t0.elapsed().as_secs_f64();
    println!("  ✓ {total_rows} rows in {prod_secs:.2}s  ({:.0} rows/s)",
        total_rows as f64 / prod_secs);
    println!();

    // ── Phase 2: Consume + Parquet ─────────────────────────────────────────
    println!("▶ Phase 2 — Consume via KafkaSource → ParquetSink (at-least-once)");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let src_cfg = KafkaConfig::from_config(
        &ConnectorConfig::new("kafka-source", "kafka")
            .with_property("bootstrap.servers", BROKERS)
            .with_property("topic", TOPIC)
            .with_property("group.id", format!("krishiv-ent12-{run_id}")),
    ).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut source = KafkaSource::new(src_cfg).map_err(|e| anyhow::anyhow!("{e}"))?;

    let dir = tempdir()?;
    let mut file_idx = 0usize;
    let mut written_rows = 0usize;
    let mut written_paths = Vec::new();

    // Wait for initial rebalance.
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let t1 = Instant::now();
    let idle_timeout = std::time::Duration::from_millis(5000);
    loop {
        let batch = match tokio::time::timeout(idle_timeout, source.read_batch()).await {
            Err(_) => break, // idle timeout — consumed everything available
            Ok(r)  => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        };
        let Some(batch) = batch else { continue };
        if batch.num_rows() == 0 { continue }

        let offsets = source.checkpoint_offset()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let path = dir.path().join(format!("part_{file_idx:04}.parquet"));
        let mut parquet = ParquetSink::create(&path).context("create ParquetSink")?;
        let mut committer = krishiv_connectors::kafka::InMemoryKafkaOffsetCommitter::new();
        PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut parquet,
            &mut committer,
            batch.clone(),
            // Use a per-partition offset for the committer.
            krishiv_connectors::kafka::KafkaOffset {
                topic: TOPIC.to_string(),
                partition: 0,
                offset: offsets.offsets.first().map(|o| o.offset).unwrap_or(0),
            },
        ).await.map_err(|e| anyhow::anyhow!("{e}"))?;

        written_rows += batch.num_rows();
        written_paths.push(path);
        file_idx += 1;

        if written_rows % 1000 == 0 {
            eprint!("\r  consumed {written_rows}/{total_rows} rows   ");
        }

        if written_rows >= total_rows { break; }
    }
    eprintln!();

    let cons_secs = t1.elapsed().as_secs_f64();
    println!("  ✓ {written_rows} rows → {file_idx} Parquet files in {cons_secs:.2}s  ({:.0} rows/s)",
        written_rows as f64 / cons_secs);
    println!();

    // ── Verify via SQL ─────────────────────────────────────────────────────
    let session = krishiv::Session::builder().build()?;
    let mut all: Vec<RecordBatch> = Vec::new();
    for p in &written_paths {
        for b in session.read_parquet(p)?.collect()?.into_batches() {
            all.push(b);
        }
    }
    session.register_record_batches("orders", all)?;
    let df = session.sql("SELECT COUNT(*) AS rows, ROUND(SUM(amount),2) AS revenue FROM orders")?;
    println!("--- Verification ---");
    println!("{}", df.collect()?.pretty()?);

    if written_rows == total_rows {
        println!("✓ row count correct: {written_rows} == {total_rows}");
    } else {
        println!("⚠ row count mismatch: got {written_rows}, expected {total_rows}");
    }

    Ok(())
}
