//! Enterprise 10 · S3 ETL pipeline — embedded mode (LocalFileSystem)
//!
//! Demonstrates `S3Source` + `S3Sink` with `object_store::local::LocalFileSystem`
//! as a drop-in for real S3/MinIO:
//!
//!   1. Write synthetic IoT sensor data via ArrowWriter (simulating an upload)
//!   2. Open it back with `S3Source` (streaming Parquet reader)
//!   3. Register batches in Krishiv, run SQL transform (filter + enrich)
//!   4. Write the curated result to a new location via `S3Sink`
//!
//! In production, swap `LocalFileSystem::new(dir)` for an `AmazonS3` or
//! `MicrosoftAzure` object store — the connector code is identical.
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_10_s3_etl_pipeline

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::Session;
use krishiv_connectors::s3::{S3Sink, S3Source};
use krishiv_connectors::Source;
use object_store::{ObjectStoreExt as _, PutPayload, local::LocalFileSystem, path::Path as StorePath};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 10: S3 ETL Pipeline (embedded / LocalFileSystem) ===");

    let dir = tempdir()?;
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);

    let input_path  = StorePath::from("input/sensors.parquet");
    let output_path = StorePath::from("output/curated.parquet");

    // ── Step 1: write synthetic sensor data (simulates an S3 upload) ──────
    println!("  writing synthetic sensors to {}", input_path);
    let sensor_schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id",     DataType::Utf8,    false),
        Field::new("ts_ms",         DataType::Int64,   false),
        Field::new("temp_c",        DataType::Float64, false),
        Field::new("humidity_pct",  DataType::Float64, false),
        Field::new("location",      DataType::Utf8,    false),
    ]));

    let base: i64 = 1_716_200_000_000;
    let sensor_batch = RecordBatch::try_new(sensor_schema.clone(), vec![
        Arc::new(StringArray::from(vec![
            "s001","s001","s001","s001",
            "s002","s002","s002","s002",
            "s003","s003","s003",
        ])),
        Arc::new(Int64Array::from(vec![
            base, base+60_000, base+120_000, base+180_000,
            base+10_000, base+70_000, base+130_000, base+190_000,
            base+20_000, base+80_000, base+140_000,
        ])),
        Arc::new(Float64Array::from(vec![
            22.1, 22.5, 23.0, 22.8,       // s001 — comfortable
            -3.5, -2.0, -4.1, 39.5,       // s002 — cold-storage (39.5 = anomaly)
            18.0, 18.2, 18.5,             // s003 — office
        ])),
        Arc::new(Float64Array::from(vec![
            45.0, 44.5, 46.0, 45.5,
            80.0, 78.0, 82.0, 101.0,     // 101% = invalid humidity
            55.0, 54.0, 56.0,
        ])),
        Arc::new(StringArray::from(vec![
            "warehouse-a","warehouse-a","warehouse-a","warehouse-a",
            "cold-storage","cold-storage","cold-storage","cold-storage",
            "office","office","office",
        ])),
    ])?;

    // Write Parquet bytes to a temp file, then upload to the object store.
    let tmp_input = dir.path().join("_sensors_tmp.parquet");
    {
        let file = std::fs::File::create(&tmp_input)?;
        let mut w = ArrowWriter::try_new(file, sensor_schema.clone(), None)?;
        w.write(&sensor_batch)?;
        w.close()?;
    }
    let raw = std::fs::read(&tmp_input)?;
    store.put(&input_path, PutPayload::from(raw)).await.context("put input")?;

    // ── Step 2: read back via S3Source ────────────────────────────────────
    println!("  reading via S3Source from {}", input_path);
    let mut source = S3Source::open(Arc::clone(&store), input_path.clone())
        .await
        .context("S3Source::open")?;

    let mut raw_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = source.read_batch().await.context("read_batch")? {
        raw_batches.push(batch);
    }
    let raw_rows: usize = raw_batches.iter().map(|b| b.num_rows()).sum();
    println!("  read {} raw rows", raw_rows);

    // ── Step 3: SQL transform ─────────────────────────────────────────────
    let session = Session::builder().build()?;
    session.register_record_batches("raw_sensors", raw_batches)?;

    let df = session.sql(
        "SELECT
             sensor_id,
             ts_ms,
             temp_c,
             CAST(temp_c * 9.0 / 5.0 + 32.0 AS DOUBLE) AS temp_f,
             humidity_pct,
             location,
             CASE
                 WHEN temp_c < 0  THEN 'cold'
                 WHEN temp_c < 25 THEN 'comfortable'
                 ELSE 'hot'
             END AS temp_bucket,
             CAST((temp_c > 38 OR temp_c < -5) AS BOOLEAN) AS is_anomaly
         FROM raw_sensors
         WHERE humidity_pct BETWEEN 10 AND 95
         ORDER BY ts_ms",
    )?;

    let curated_result = df.collect()?;
    let curated_batches = curated_result.into_batches();
    let curated_rows: usize = curated_batches.iter().map(|b| b.num_rows()).sum();
    println!("  {} curated rows after filter (removed invalid humidity)", curated_rows);

    // ── Step 4: write to S3Sink ───────────────────────────────────────────
    let mut sink = S3Sink::new(Arc::clone(&store), output_path.clone());

    use krishiv_connectors::Sink;
    for batch in &curated_batches {
        sink.write_batch(batch.clone()).await.context("write_batch")?;
    }
    sink.flush().await.context("flush")?;

    println!("  curated parquet written to {}", output_path);

    // ── Verify: read curated back via S3Source ────────────────────────────
    let mut read_back = S3Source::open(Arc::clone(&store), output_path)
        .await
        .context("open curated")?;
    let mut verify: Vec<RecordBatch> = Vec::new();
    while let Some(b) = read_back.read_batch().await? {
        verify.push(b);
    }

    let verify_rows: usize = verify.iter().map(|b| b.num_rows()).sum();
    println!("\n✓ ETL complete  raw={} → curated={} rows", raw_rows, verify_rows);

    let session2 = Session::builder().build()?;
    session2.register_record_batches("curated", verify)?;
    let summary = session2.sql(
        "SELECT temp_bucket, COUNT(*) AS count, ROUND(AVG(temp_c), 2) AS avg_temp, \
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies \
         FROM curated GROUP BY temp_bucket ORDER BY avg_temp"
    )?;
    println!("\n--- Curated summary by temp bucket ---");
    println!("{}", summary.collect()?.pretty()?);

    Ok(())
}
