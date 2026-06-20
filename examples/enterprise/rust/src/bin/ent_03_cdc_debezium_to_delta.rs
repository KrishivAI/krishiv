//! Enterprise 03 · CDC (Debezium) → Delta Lake — embedded mode
//!
//! Uses `InMemoryCdcEventSource` with synthetic Debezium 2.x JSON payloads.
//! The pipeline parses Insert/Update/Delete events, builds an Arrow batch
//! (with `_op`, `_lsn`, `_ts_ms` metadata columns), and writes it to a
//! local Delta table via `write_delta`.
//!
//! In production, swap `InMemoryCdcEventSource` for `RdkafkaCdcEventSource`
//! pointed at your Debezium Kafka topic.
//!
//! Run:
//!   cargo run -p krishiv-enterprise-examples --bin ent_03_cdc_debezium_to_delta

use anyhow::{Context, Result};
use krishiv_connectors::cdc::{
    CdcToLakehousePipeline, InMemoryCdcEventSource, build_batch_from_events,
    parse_debezium_envelope,
};
use krishiv_connectors::lakehouse::{DeltaWriteMode, write_delta};
use tempfile::tempdir;

const EVENTS: &[&str] = &[
    r#"{"op":"c","source":{"lsn":100,"ts_ms":1716201600000,"table":"orders"},"after":{"order_id":1,"customer":"alice","product":"Laptop Pro","amount":"1299.99","status":"pending"}}"#,
    r#"{"op":"c","source":{"lsn":110,"ts_ms":1716201601000,"table":"orders"},"after":{"order_id":2,"customer":"bob","product":"Mouse","amount":"29.99","status":"pending"}}"#,
    r#"{"op":"u","source":{"lsn":120,"ts_ms":1716201602000,"table":"orders"},"before":{"order_id":1},"after":{"order_id":1,"customer":"alice","product":"Laptop Pro","amount":"1299.99","status":"shipped"}}"#,
    // schema evolution: "notes" column appears on order 3
    r#"{"op":"c","source":{"lsn":130,"ts_ms":1716201603000,"table":"orders"},"after":{"order_id":3,"customer":"carol","product":"Monitor 4K","amount":"499.99","status":"pending","notes":"gift"}}"#,
    r#"{"op":"d","source":{"lsn":140,"ts_ms":1716201604000,"table":"orders"},"before":{"order_id":2},"after":null}"#,
];

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 03: CDC (Debezium) → Delta Lake (embedded) ===");

    let dir = tempdir()?;
    let delta_path = dir.path().join("orders_delta");
    println!("  delta path : {}", delta_path.display());

    // Parse all events.
    let mut events = Vec::new();
    for (i, json) in EVENTS.iter().enumerate() {
        let event = parse_debezium_envelope(json, 0, i as i64)
            .map_err(|e| anyhow::anyhow!("parse error on event {}: {}", i, e))?;
        println!("  event {}  op={:?}  table={}", i, event.op, event.table);
        events.push(event);
    }

    // Build single Arrow batch from all CDC events.
    let batch = build_batch_from_events(&events)
        .map_err(|e| anyhow::anyhow!("build_batch_from_events: {}", e.0))?;

    println!(
        "\n  merged batch: {} rows  columns: {:?}",
        batch.num_rows(),
        batch.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>()
    );

    // Write to Delta Lake.
    write_delta(
        delta_path.to_string_lossy().to_string(),
        vec![batch],
        DeltaWriteMode::Append,
        false,
    )
    .await
    .context("write_delta")?;

    println!("✓ Delta table written");

    // Read back and show results.
    let session = krishiv::Session::builder().build()?;
    let df = session.read_delta_async(delta_path.to_string_lossy().as_ref(), None).await?;
    println!("\n--- CDC output (all ops) ---");
    println!("{}", df.collect()?.pretty()?);

    // Operation distribution.
    let session2 = krishiv::Session::builder().build()?;
    let df2 = session2.read_delta_async(delta_path.to_string_lossy().as_ref(), None).await?;
    let result = df2.collect()?;
    session2.register_record_batches("cdc_out", result.into_batches())?;
    let summary = session2.sql("SELECT _op, COUNT(*) AS n FROM cdc_out GROUP BY _op ORDER BY n DESC")?;
    println!("\n--- Op summary ---");
    println!("{}", summary.collect()?.pretty()?);

    // Also demonstrate the pipeline approach with InMemoryCdcEventSource.
    println!("\n--- InMemoryCdcEventSource pipeline ---");
    let source = InMemoryCdcEventSource::new(EVENTS.iter().copied());
    let pipeline = CdcToLakehousePipeline::new(
        "orders.cdc",
        vec!["broker:9092".into()],
        "my_catalog",
        "warehouse.orders",
        vec!["order_id".into()],
    );
    pipeline.validate().context("pipeline validate")?;
    println!("  pipeline config valid ✓");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut pipeline_batches = Vec::new();
    pipeline
        .run_with_source(
            source,
            |b| { pipeline_batches.push(b); Ok(()) },
            shutdown_rx,
        )
        .await
        .context("run_with_source")?;
    drop(shutdown_tx);

    let total: usize = pipeline_batches.iter().map(|b| b.num_rows()).sum();
    println!("  pipeline produced {} rows across {} batches", total, pipeline_batches.len());

    Ok(())
}
