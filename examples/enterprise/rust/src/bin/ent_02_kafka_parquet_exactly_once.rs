//! Enterprise 02 · Kafka → Parquet (exactly-once via 2PC) — embedded mode
//!
//! Demonstrates `EpochTransactionLog<LocalParquetTwoPhaseCommitSink>`:
//!
//!   1. stage(batch)           — buffer the batch in the open log
//!   2. pre_commit(epoch)      — write each buffered batch as `<epoch>-N.parquet.tmp`
//!   3. commit_through(epoch)  — atomic rename `.tmp → .parquet`
//!
//! A crash between steps 2 and 3 leaves `.tmp` files that can be
//! re-committed idempotently on restart (POSIX rename is atomic).
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_02_kafka_parquet_exactly_once

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_connectors::kafka::InMemoryKafkaSource;
use krishiv_connectors::two_phase::{
    EpochTransactionLog, LocalParquetTwoPhaseCommitSink, TransactionalSinkParticipant,
};
use krishiv_connectors::Source;
use tempfile::tempdir;

const BARRIER_EVERY: usize = 2;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 02: Kafka → Parquet (exactly-once 2PC, embedded) ===");

    let dir = tempdir()?;
    println!("  staging dir : {}", dir.path().display());

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
    ]));

    let mk = |ids: Vec<i64>, customers: Vec<&str>, amounts: Vec<f64>| {
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(customers)),
            Arc::new(Float64Array::from(amounts)),
        ])
        .unwrap()
    };

    let batches = vec![
        mk(vec![1, 2], vec!["alice", "bob"],   vec![1299.99, 29.99]),
        mk(vec![3, 4], vec!["carol", "dave"],  vec![349.99, 499.99]),
        mk(vec![5],    vec!["eve"],             vec![39.99]),
        mk(vec![6, 7], vec!["frank", "grace"], vec![129.99, 89.99]),
    ];

    let mut source = InMemoryKafkaSource::new("orders", 0, 0, batches);
    let sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
    let mut log = EpochTransactionLog::new(sink);

    let mut epoch = 1u64;
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;

    while let Some(batch) = source.read_batch().await.context("read_batch")? {
        total_rows += batch.num_rows();
        println!("  batch {}  rows={}  open_rows={}", batch_count, batch.num_rows(), log.open_rows());
        log.stage(&batch).context("stage")?;
        batch_count += 1;

        if batch_count % BARRIER_EVERY == 0 {
            log.pre_commit(epoch).context("pre_commit")?;
            println!("  ↑ pre_commit epoch={}  prepared={:?}", epoch, log.prepared_epochs());
            let n = log.commit_through(epoch).context("commit_through")?;
            println!("  ✓ commit_through epoch={}  files={}", epoch, n);
            epoch += 1;
        }
    }

    if log.open_rows() > 0 {
        log.pre_commit(epoch).context("final pre_commit")?;
        let n = log.commit_through(epoch).context("final commit_through")?;
        println!("  ✓ final barrier epoch={}  files={}", epoch, n);
    }

    let files: Vec<_> = std::fs::read_dir(dir.path())?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".parquet"))
        .collect();

    println!("\n✓ total_rows={}  parquet_files={:?}", total_rows, files);

    let session = krishiv::Session::builder().build()?;
    for f in &files {
        let path = dir.path().join(f);
        let df = session.read_parquet(&path)?;
        println!("\n--- {} ---", f);
        println!("{}", df.collect()?.pretty()?);
    }

    Ok(())
}
