//! Enterprise 18 · Kafka → InfluxDB (time-series / IoT ingestion)
//!
//! Reads sensor readings from Kafka (Arrow IPC) and writes them to InfluxDB v2
//! using the line protocol over HTTP. Demonstrates the IoT / metrics pattern.
//!
//! InfluxDB setup:
//!   docker run -d --name influxdb -p 8086:8086 \
//!     -e DOCKER_INFLUXDB_INIT_MODE=setup \
//!     -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
//!     -e DOCKER_INFLUXDB_INIT_PASSWORD=password123 \
//!     -e DOCKER_INFLUXDB_INIT_ORG=krishiv \
//!     -e DOCKER_INFLUXDB_INIT_BUCKET=sensors \
//!     -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=krishiv-token-123 \
//!     influxdb:2
//!
//! Run:
//!   cargo run --bin ent_18_kafka_to_influxdb
//!   LOAD_ROWS=50000 cargo run --bin ent_18_kafka_to_influxdb

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

const BROKERS:     &str = "localhost:9092";
const TOPIC:       &str = "sensors-influx";
const INFLUX_URL:  &str = "http://localhost:8086";
const INFLUX_ORG:  &str = "krishiv";
const INFLUX_BUCK: &str = "sensors";
const INFLUX_TOK:  &str = "krishiv-token-123";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("device_id",   DataType::Utf8,    false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("humidity",    DataType::Float64, false),
        Field::new("pressure",    DataType::Float64, false),
        Field::new("ts_ns",       DataType::Int64,   false), // nanoseconds for InfluxDB
    ]))
}

fn make_batch(schema: Arc<Schema>, base: i64, size: usize) -> Result<RecordBatch> {
    let devices = ["sensor-01","sensor-02","sensor-03","sensor-04",
                   "sensor-05","sensor-06","sensor-07","sensor-08"];
    let ids: Vec<i64> = (base..base + size as i64).collect();
    let devs: Vec<&str> = ids.iter().map(|i| devices[(i % 8) as usize]).collect();
    let temps: Vec<f64>  = ids.iter().map(|i| 20.0 + (i % 30) as f64 * 0.5).collect();
    let humid: Vec<f64>  = ids.iter().map(|i| 40.0 + (i % 40) as f64 * 0.3).collect();
    let press: Vec<f64>  = ids.iter().map(|i| 1010.0 + (i % 20) as f64 * 0.1).collect();
    // Use nanosecond timestamps (InfluxDB line protocol requires ns by default).
    let ts_ns: Vec<i64>  = ids.iter().map(|i| 1_716_200_000_000_000_000 + i * 1_000_000).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(devs)),
        Arc::new(Float64Array::from(temps)),
        Arc::new(Float64Array::from(humid)),
        Arc::new(Float64Array::from(press)),
        Arc::new(Int64Array::from(ts_ns)),
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
        .context("ipc")?
        .map(|r| r.context("batch"))
        .collect()
}

/// Convert a RecordBatch to InfluxDB line protocol.
/// Measurement: sensors, tags: device_id, fields: temperature, humidity, pressure.
fn batch_to_line_protocol(batch: &RecordBatch) -> String {
    let devs  = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let temps = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    let humid = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    let press = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
    let ts_ns = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out = String::with_capacity(batch.num_rows() * 100);
    for r in 0..batch.num_rows() {
        out.push_str(&format!(
            "sensors,device_id={} temperature={:.2},humidity={:.2},pressure={:.2} {}\n",
            devs.value(r), temps.value(r), humid.value(r), press.value(r), ts_ns.value(r)
        ));
    }
    out
}

async fn influx_write(client: &reqwest::Client, body: String) -> Result<()> {
    let resp = client
        .post(format!("{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCK}&precision=ns"))
        .header("Authorization", format!("Token {INFLUX_TOK}"))
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(body)
        .send().await.context("influx write")?;
    if !resp.status().is_success() {
        let e = resp.text().await.unwrap_or_default();
        anyhow::bail!("InfluxDB write error: {e}");
    }
    Ok(())
}

async fn influx_query(client: &reqwest::Client, flux: &str) -> Result<String> {
    let resp = client
        .post(format!("{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}"))
        .header("Authorization", format!("Token {INFLUX_TOK}"))
        .header("Content-Type", "application/vnd.flux")
        .header("Accept", "application/csv")
        .body(flux.to_string())
        .send().await.context("influx query")?;
    resp.text().await.context("influx query text")
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows: usize = std::env::var("LOAD_ROWS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(2_000);

    println!("=== Enterprise 18: Kafka → InfluxDB (time-series) ===");
    println!("  broker     : {BROKERS}");
    println!("  topic      : {TOPIC}");
    println!("  influx_url : {INFLUX_URL}");
    println!("  rows       : {total_rows}");
    println!();

    let http = reqwest::Client::new();

    // Verify InfluxDB is up.
    let health = http.get(format!("{INFLUX_URL}/health"))
        .send().await.context("influxdb health check")?
        .text().await?;
    if !health.contains("\"pass\"") {
        anyhow::bail!("InfluxDB not healthy: {health}");
    }
    println!("✓ InfluxDB ready");

    // ── Phase 1: Produce sensor readings to Kafka ──────────────────────────
    println!("▶ Phase 1 — Produce {total_rows} sensor readings to Kafka");
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
        let key   = (b % 2).to_string();
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

    // ── Phase 2: Consume → InfluxDB ───────────────────────────────────────
    println!("▶ Phase 2 — Consume → InfluxDB line protocol");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent18-{run_id}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .create()?;
    consumer.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let t1 = Instant::now();
    let mut consumed  = 0usize;
    let mut write_ops = 0usize;
    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(5000), consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => { eprintln!("  warn: {e}"); tokio::time::sleep(Duration::from_millis(300)).await; continue; }
            Ok(Ok(m)) => m,
        };
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            let rows  = batch.num_rows();
            let lines = batch_to_line_protocol(&batch);
            influx_write(&http, lines).await?;
            consumed  += rows;
            write_ops += 1;
        }
        if consumed % 5_000 == 0 {
            eprint!("\r  consumed {consumed}/{total_rows} ({write_ops} writes)   ");
        }
        if consumed >= total_rows { break; }
    }
    eprintln!();
    let cons_secs = t1.elapsed().as_secs_f64();
    println!("  ✓ {consumed} readings → InfluxDB in {cons_secs:.2}s ({:.0} rows/s, {write_ops} batches)",
        consumed as f64 / cons_secs);

    // ── Verify via Flux query ──────────────────────────────────────────────
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let flux = format!(
        r#"from(bucket: "{INFLUX_BUCK}")
          |> range(start: 2024-05-20T00:00:00Z, stop: 2030-01-01T00:00:00Z)
          |> filter(fn: (r) => r._measurement == "sensors")
          |> filter(fn: (r) => r._field == "temperature")
          |> group()
          |> count()"#
    );
    let result = influx_query(&http, &flux).await?;
    // Parse the CSV — data rows start with "," and last column is _value.
    let count: u64 = result.lines()
        .filter(|l| l.starts_with(',') && !l.contains("_value"))
        .filter_map(|l| l.split(',').last().and_then(|v| v.trim().parse::<u64>().ok()))
        .sum();

    println!();
    println!("--- InfluxDB verification ---");
    println!("  temperature point count : {count}");
    println!("  expected                : {total_rows}");

    // Query avg temperature per device.
    let flux2 = format!(
        r#"from(bucket: "{INFLUX_BUCK}")
          |> range(start: 2024-05-20T00:00:00Z, stop: 2030-01-01T00:00:00Z)
          |> filter(fn: (r) => r._measurement == "sensors" and r._field == "temperature")
          |> group(columns: ["device_id"])
          |> mean(column: "_value")"#
    );
    let avg_result = influx_query(&http, &flux2).await?;
    println!();
    println!("  avg temperature per device:");
    for line in avg_result.lines().filter(|l| !l.starts_with('#') && !l.is_empty()).skip(1) {
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() >= 8 {
            let device = cols.iter().find(|c| c.contains("sensor-")).copied().unwrap_or("?");
            let val    = cols.last().unwrap_or(&"?").trim();
            println!("    {device:<12} → {val} °C");
        }
    }

    if count == total_rows as u64 {
        println!("\n✓ row count correct: {count} == {total_rows}");
    } else {
        println!("\n⚠ row count: got {count}, expected {total_rows}");
    }
    Ok(())
}
