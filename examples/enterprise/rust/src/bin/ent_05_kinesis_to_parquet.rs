//! Enterprise 05 · Kinesis → Parquet (checkpointed) — embedded mode
//!
//! Simulates reading IoT sensor records from Kinesis shards using in-memory
//! Arrow batches (same schema as `KinesisSource`). Saves the last "sequence
//! number" to a checkpoint file; on re-run, the consumer skips already-seen
//! records by comparing sequence numbers.
//!
//! In production, swap the in-memory records for `KinesisSource::new(cfg).await`
//! which reads from the real AWS/LocalStack Kinesis API.
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_05_kinesis_to_parquet

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

const CHECKPOINT_KEY: &str = "/tmp/krishiv-ent-05-checkpoint.txt";

/// Schema matching `KinesisSource::fixed_schema()` from the real connector.
fn kinesis_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sequence_number",     DataType::Utf8,   false),
        Field::new("partition_key",       DataType::Utf8,   false),
        Field::new("data",                DataType::Binary, false),
        Field::new("arrival_timestamp_ms",DataType::Int64,  false),
    ]))
}

fn make_records(schema: Arc<Schema>, start_seq: u64) -> RecordBatch {
    let records = vec![
        (format!("seq-{:05}", start_seq + 1), "pk-1", r#"{"sensor":"s001","temp_c":22.1}"#, 1716200000000i64),
        (format!("seq-{:05}", start_seq + 2), "pk-2", r#"{"sensor":"s002","temp_c":-3.5}"#, 1716200001000),
        (format!("seq-{:05}", start_seq + 3), "pk-3", r#"{"sensor":"s001","temp_c":22.5}"#, 1716200002000),
        (format!("seq-{:05}", start_seq + 4), "pk-1", r#"{"sensor":"s003","temp_c":18.0}"#, 1716200003000),
        (format!("seq-{:05}", start_seq + 5), "pk-2", r#"{"sensor":"s002","temp_c":-4.1}"#, 1716200004000),
    ];

    RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(records.iter().map(|r| r.0.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(records.iter().map(|r| r.1).collect::<Vec<_>>())),
        Arc::new(BinaryArray::from(records.iter().map(|r| r.2.as_bytes()).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(records.iter().map(|r| r.3).collect::<Vec<_>>())),
    ]).unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 05: Kinesis → Parquet (embedded, checkpointed) ===");

    let schema = kinesis_schema();
    let dir = tempdir()?;
    let out_path = dir.path().join("kinesis_records.parquet");

    // Restore from checkpoint or start from beginning.
    let checkpoint_seq = std::fs::read_to_string(CHECKPOINT_KEY)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);

    if checkpoint_seq > 0 {
        println!("  resuming from checkpoint seq={}", checkpoint_seq);
    } else {
        println!("  starting from beginning (no checkpoint)");
    }

    // Simulate one shard poll — in real code this is `source.read_batch().await`.
    let batch = make_records(schema.clone(), checkpoint_seq);

    // Extract last sequence number for the checkpoint.
    let seq_col = batch.column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("seq col")?;
    let last_seq: u64 = seq_col
        .iter()
        .filter_map(|s| s.and_then(|v| v.trim_start_matches("seq-").parse::<u64>().ok()))
        .max()
        .unwrap_or(checkpoint_seq);

    println!("  received {} records  last_seq=seq-{:05}", batch.num_rows(), last_seq);

    // Decode data field for display.
    let data_col = batch.column(2).as_any().downcast_ref::<BinaryArray>().context("data col")?;
    for i in 0..batch.num_rows() {
        let s = std::str::from_utf8(data_col.value(i)).unwrap_or("?");
        println!("  row {}  data={}", i, s);
    }

    // Write to Parquet.
    {
        let file = std::fs::File::create(&out_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
    }

    // Save checkpoint.
    std::fs::write(CHECKPOINT_KEY, last_seq.to_string())?;
    println!("\n  checkpoint saved: seq-{:05}", last_seq);

    println!("✓ {} rows written to {}", batch.num_rows(), out_path.display());

    let session = krishiv::Session::builder().build()?;
    let df = session.read_parquet(&out_path)?;
    println!("\n--- Kinesis records ---");
    println!("{}", df.select(&["sequence_number", "partition_key", "arrival_timestamp_ms"])?.collect()?.pretty()?);

    // Clean up checkpoint so next run starts fresh.
    let _ = std::fs::remove_file(CHECKPOINT_KEY);

    Ok(())
}
