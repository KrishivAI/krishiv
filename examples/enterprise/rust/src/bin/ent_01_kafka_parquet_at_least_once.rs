//! Enterprise 01 · Kafka → Parquet (at-least-once, rolling files) — embedded mode
//!
//! Uses `InMemoryKafkaSource` (no broker needed) and `ParquetSink` to
//! demonstrate the at-least-once delivery contract:
//!
//!   for each Kafka batch:
//!     open a new Parquet file  →  write  →  flush  →  commit offset
//!
//! One Parquet file is produced per micro-batch poll (rolling files pattern).
//! `ParquetSink` finalises the file on `flush()`, so each batch needs its own
//! sink instance. The offset is committed only after the flush succeeds, so a
//! crash between flush and commit leaves the consumer at the previous committed
//! position and the batch replays (at-least-once, possible duplicate file).
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_01_kafka_parquet_at_least_once

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_connectors::kafka::{InMemoryKafkaOffsetCommitter, InMemoryKafkaSource};
use krishiv_connectors::parquet::ParquetSink;
use krishiv_connectors::{CheckpointSource, PostWriteOffsetCommitProtocol, Source};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 01: Kafka → Parquet (at-least-once, rolling files) ===");

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("product",  DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
    ]));

    let batch_a = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        Arc::new(StringArray::from(vec!["Laptop", "Mouse", "Chair"])),
        Arc::new(Float64Array::from(vec![1299.99, 29.99, 349.99])),
    ])?;

    let batch_b = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![4, 5])),
        Arc::new(StringArray::from(vec!["dave", "eve"])),
        Arc::new(StringArray::from(vec!["Monitor", "USB Hub"])),
        Arc::new(Float64Array::from(vec![499.99, 39.99])),
    ])?;

    let mut source = InMemoryKafkaSource::new("orders", 0, 0, vec![batch_a, batch_b]);
    let mut committer = InMemoryKafkaOffsetCommitter::new();

    let dir = tempdir()?;
    let mut total_rows = 0usize;
    let mut file_idx = 0usize;
    let mut written_paths = Vec::new();

    // Rolling-file loop: one Parquet file per micro-batch poll.
    while let Some(batch) = source.read_batch().await.context("read_batch")? {
        let n = batch.num_rows();
        let offset = source.checkpoint_offset().context("checkpoint_offset")?;

        // Open a fresh sink for this batch (ParquetSink closes after flush).
        let path = dir.path().join(format!("orders_part_{file_idx:04}.parquet"));
        let mut sink = ParquetSink::create(&path).context("create ParquetSink")?;

        println!("  poll {} → {} rows  offset={}", file_idx, n, offset.offset);

        PostWriteOffsetCommitProtocol::write_flush_commit(&mut sink, &mut committer, batch, offset)
            .await
            .context("write_flush_commit")?;

        println!("    committed offset={}  file={}",
            committer.last_committed_offset().unwrap().offset,
            path.file_name().unwrap().to_string_lossy());

        written_paths.push(path);
        total_rows += n;
        file_idx += 1;
    }

    println!("\n  committed offsets: {:?}", committer.committed_offsets());
    println!("  rolling files written: {}", written_paths.len());

    // Read all parts back via Krishiv SQL.
    let session = krishiv::Session::builder().build()?;
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for path in &written_paths {
        let df = session.read_parquet(path)?;
        for b in df.collect()?.into_batches() {
            all_batches.push(b);
        }
    }
    session.register_record_batches("orders", all_batches)?;
    let df = session.sql("SELECT * FROM orders ORDER BY order_id")?;

    println!("\n--- All {} rows (from {} files) ---", total_rows, file_idx);
    println!("{}", df.collect()?.pretty()?);

    println!("✓ at-least-once: {} rows across {} Parquet files", total_rows, file_idx);

    Ok(())
}
