//! Native Rust Kafka streaming SQL runner for Krishiv + Redpanda.
//!
//! Each scenario uses `SqlEngine::register_kafka_source` — the native Rust API
//! added to `krishiv-sql` — to wire a live Kafka topic into DataFusion.
//! A micro-batch is then collected from the streaming table, re-registered as an
//! in-memory batch, and SQL analytics run against it.  No Python bridge needed.
//!
//! The final scenario demonstrates `SqlEngine::sql_to_kafka` writing results
//! back to a new Kafka topic.
//!
//! Usage:
//!   BOOTSTRAP=<host:port> cargo run --bin kafka_streaming_sql
//!
//! Defaults to localhost:9092 (suitable when port-forwarding from k8s Redpanda).
//! Inside the cluster: BOOTSTRAP=redpanda-0.redpanda-service.default.svc.cluster.local:9092

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;

// ── bootstrap ─────────────────────────────────────────────────────────────────

fn bootstrap() -> String {
    std::env::var("BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// Unique consumer group per binary run so repeat executions start from earliest
/// without seeing stale committed offsets from a previous run.
fn unique_group(base: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{base}-{ts}")
}

// ── produce ───────────────────────────────────────────────────────────────────

fn make_producer(bs: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bs)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creation failed")
}

async fn produce(producer: &FutureProducer, topic: &str, msgs: &[Value]) {
    for msg in msgs {
        let payload = msg.to_string();
        let rec: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(&payload);
        let _ = producer.send(rec, Duration::from_secs(5)).await;
    }
    let _ = producer.flush(Duration::from_secs(3));
}

// ── collect micro-batch from a streaming table ────────────────────────────────

/// Execute `SELECT * FROM <table>` on `engine` (which has a Kafka streaming table
/// registered under `table`) and collect up to `max_rows` rows, waiting at most
/// `timeout`.  Returns the collected `RecordBatch`es.
async fn collect_micro_batch(
    engine: &krishiv_sql::SqlEngine,
    table: &str,
    max_rows: usize,
    timeout: Duration,
) -> Vec<RecordBatch> {
    let sql = format!("SELECT * FROM {table}");
    let df = match engine.sql(&sql).await {
        Ok(df) => df,
        Err(e) => {
            eprintln!("  collect_micro_batch sql error: {e}");
            return vec![];
        }
    };
    let mut stream = match df.execute_stream().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  collect_micro_batch stream error: {e}");
            return vec![];
        }
    };

    let deadline = Instant::now() + timeout;
    let mut batches = Vec::new();
    let mut total = 0usize;

    while total < max_rows && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(b))) if b.num_rows() > 0 => {
                total += b.num_rows();
                batches.push(b);
            }
            Ok(Some(Ok(_))) => {} // empty batch — keep polling
            _ => break,
        }
    }
    batches
}

// ── schema helper ─────────────────────────────────────────────────────────────

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, dt)| Field::new(*n, dt.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

// ── one scenario ──────────────────────────────────────────────────────────────

/// Full pipeline for one scenario:
/// 1. Register Kafka topic as streaming table via `register_kafka_source`.
/// 2. Produce `messages` to the topic.
/// 3. Collect a micro-batch from the streaming table (SELECT *).
/// 4. Re-register the collected data as an in-memory table.
/// 5. Run `agg_sql` (GROUP BY / aggregation) on the in-memory table.
/// 6. Print results.
async fn run_scenario(
    label: &str,
    producer: &FutureProducer,
    topic: &str,
    table_schema: SchemaRef,
    messages: &[Value],
    agg_sql: &str,
    bs: &str,
) {
    println!("\n--- {label} ---");

    // ── 1. Register Kafka source via the native Rust API ───────────────────────
    let engine = krishiv_sql::SqlEngine::new();
    let table_name = topic.strip_prefix("krishiv-").unwrap_or(topic);
    let group = unique_group(table_name);

    if let Err(e) = engine.register_kafka_source(
        table_name,
        table_schema.clone(),
        bs,
        topic,
        &group,
    ) {
        println!("  register_kafka_source error: {e}");
        return;
    }

    // ── 2. Produce events (after subscription is established) ──────────────────
    // Small delay lets the consumer group finish its rebalance before messages land.
    tokio::time::sleep(Duration::from_millis(600)).await;
    produce(producer, topic, messages).await;

    // ── 3. Collect micro-batch from the streaming table ────────────────────────
    let batches = collect_micro_batch(&engine, table_name, messages.len(), Duration::from_secs(5)).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("  collected {total_rows} / {} rows from '{topic}'", messages.len());

    if total_rows == 0 {
        println!("  (no data received)");
        return;
    }

    // ── 4. Build an Arrow batch for any rows not yet typed correctly ───────────
    // The streaming table already projects via the declared schema, so the
    // collected batches match `table_schema`.  We re-register them in a fresh
    // engine for the aggregation query.
    let agg_engine = krishiv_sql::SqlEngine::new();
    if let Err(e) = agg_engine.register_record_batches(table_name, batches).await {
        println!("  register batch error: {e}");
        return;
    }

    // ── 5. Run the aggregation SQL ─────────────────────────────────────────────
    match agg_engine.sql(agg_sql).await {
        Ok(df) => match df.collect().await {
            Ok(results) => {
                for b in &results {
                    print_batch(b);
                }
                if results.iter().all(|b| b.num_rows() == 0) {
                    println!("  (empty result)");
                }
            }
            Err(e) => println!("  collect error: {e}"),
        },
        Err(e) => println!("  sql error: {e}"),
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bs = bootstrap();
    println!("=== Krishiv Native Kafka Streaming SQL ===");
    println!("Bootstrap : {bs}");
    println!("Engine    : register_kafka_source (rdkafka + DataFusion) — zero Python");
    println!("Pattern   : produce → streaming table → micro-batch → aggregation SQL");

    let producer = make_producer(&bs);

    // ── 1. Fraud Detection ────────────────────────────────────────────────────
    run_scenario(
        "1. Fraud Detection — high-value transaction aggregation",
        &producer, "krishiv-sales",
        schema(&[("user_id", DataType::Utf8), ("amount", DataType::Float64),
                 ("category", DataType::Utf8), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"user_id":"U1","amount":5200.0,"category":"electronics","ts":1000}),
            serde_json::json!({"user_id":"U1","amount":6100.0,"category":"electronics","ts":1010}),
            serde_json::json!({"user_id":"U2","amount":150.0,"category":"grocery","ts":1020}),
            serde_json::json!({"user_id":"U3","amount":8900.0,"category":"luxury","ts":1030}),
            serde_json::json!({"user_id":"U1","amount":4800.0,"category":"electronics","ts":1040}),
        ],
        "SELECT user_id, SUM(amount) as total, COUNT(*) as txn_count \
         FROM sales GROUP BY user_id ORDER BY total DESC",
        &bs,
    ).await;

    // ── 2. IoT Anomaly ────────────────────────────────────────────────────────
    run_scenario(
        "2. IoT Telemetry — anomaly detection",
        &producer, "krishiv-iot",
        schema(&[("device_id", DataType::Utf8), ("temp", DataType::Float64),
                 ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"device_id":"D1","temp":22.5,"ts":2000}),
            serde_json::json!({"device_id":"D1","temp":23.1,"ts":2010}),
            serde_json::json!({"device_id":"D2","temp":98.7,"ts":2020}),
            serde_json::json!({"device_id":"D1","temp":22.8,"ts":2030}),
            serde_json::json!({"device_id":"D3","temp":101.2,"ts":2040}),
        ],
        "SELECT device_id, AVG(temp) as avg_temp, MAX(temp) as peak_temp \
         FROM iot GROUP BY device_id ORDER BY peak_temp DESC",
        &bs,
    ).await;

    // ── 3. Clickstream ────────────────────────────────────────────────────────
    run_scenario(
        "3. Clickstream Analytics — page view counts",
        &producer, "krishiv-clicks",
        schema(&[("session_id", DataType::Utf8), ("action", DataType::Utf8),
                 ("page", DataType::Utf8), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"session_id":"S1","action":"view","page":"/home","ts":3000}),
            serde_json::json!({"session_id":"S2","action":"click","page":"/products","ts":3010}),
            serde_json::json!({"session_id":"S1","action":"click","page":"/cart","ts":3020}),
            serde_json::json!({"session_id":"S3","action":"view","page":"/home","ts":3030}),
            serde_json::json!({"session_id":"S2","action":"purchase","page":"/checkout","ts":3040}),
        ],
        "SELECT page, COUNT(*) as hits, COUNT(DISTINCT session_id) as unique_sessions \
         FROM clicks GROUP BY page ORDER BY hits DESC",
        &bs,
    ).await;

    // ── 4. Ride-Share Pricing ─────────────────────────────────────────────────
    run_scenario(
        "4. Ride-Share — demand & pricing by zone",
        &producer, "krishiv-rides",
        schema(&[("zone", DataType::Utf8), ("driver_id", DataType::Utf8),
                 ("fare", DataType::Float64), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"zone":"downtown","driver_id":"D1","fare":12.5,"ts":4000}),
            serde_json::json!({"zone":"airport","driver_id":"D2","fare":35.0,"ts":4010}),
            serde_json::json!({"zone":"downtown","driver_id":"D3","fare":18.0,"ts":4020}),
            serde_json::json!({"zone":"suburb","driver_id":"D4","fare":8.0,"ts":4030}),
            serde_json::json!({"zone":"airport","driver_id":"D5","fare":40.0,"ts":4040}),
        ],
        "SELECT zone, COUNT(*) as ride_count, ROUND(AVG(fare), 2) as avg_fare \
         FROM rides GROUP BY zone ORDER BY ride_count DESC",
        &bs,
    ).await;

    // ── 5. Log Error Rate ─────────────────────────────────────────────────────
    run_scenario(
        "5. Log Monitoring — 5xx error rate per service",
        &producer, "krishiv-logs",
        schema(&[("service", DataType::Utf8), ("level", DataType::Utf8),
                 ("status_code", DataType::Int64), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"service":"api","level":"ERROR","status_code":500,"ts":5000}),
            serde_json::json!({"service":"auth","level":"INFO","status_code":200,"ts":5010}),
            serde_json::json!({"service":"api","level":"ERROR","status_code":503,"ts":5020}),
            serde_json::json!({"service":"db","level":"ERROR","status_code":500,"ts":5030}),
            serde_json::json!({"service":"api","level":"INFO","status_code":200,"ts":5040}),
        ],
        "SELECT service, COUNT(*) as error_count \
         FROM logs WHERE level = 'ERROR' GROUP BY service ORDER BY error_count DESC",
        &bs,
    ).await;

    // ── 6. Supply Chain GPS ───────────────────────────────────────────────────
    run_scenario(
        "6. Supply Chain — last GPS position per truck",
        &producer, "krishiv-supply",
        schema(&[("truck_id", DataType::Utf8), ("lat", DataType::Float64),
                 ("lon", DataType::Float64), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"truck_id":"T1","lat":37.77,"lon":-122.41,"ts":6000}),
            serde_json::json!({"truck_id":"T2","lat":34.05,"lon":-118.24,"ts":6010}),
            serde_json::json!({"truck_id":"T1","lat":37.79,"lon":-122.43,"ts":6020}),
            serde_json::json!({"truck_id":"T3","lat":40.71,"lon":-74.00,"ts":6030}),
        ],
        "SELECT truck_id, MAX(lat) as last_lat, MAX(lon) as last_lon \
         FROM supply GROUP BY truck_id ORDER BY truck_id",
        &bs,
    ).await;

    // ── 7. VWAP Trading ───────────────────────────────────────────────────────
    run_scenario(
        "7. Trading — VWAP per ticker",
        &producer, "krishiv-trades",
        schema(&[("ticker", DataType::Utf8), ("price", DataType::Float64),
                 ("volume", DataType::Int64), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"ticker":"AAPL","price":183.5,"volume":1000,"ts":7000}),
            serde_json::json!({"ticker":"AAPL","price":184.0,"volume":500,"ts":7010}),
            serde_json::json!({"ticker":"MSFT","price":415.2,"volume":800,"ts":7020}),
            serde_json::json!({"ticker":"AAPL","price":183.8,"volume":750,"ts":7030}),
            serde_json::json!({"ticker":"MSFT","price":416.0,"volume":600,"ts":7040}),
        ],
        "SELECT ticker, \
           ROUND(SUM(price * CAST(volume AS DOUBLE)) / CAST(SUM(volume) AS DOUBLE), 4) as vwap, \
           SUM(volume) as total_volume \
         FROM trades GROUP BY ticker ORDER BY ticker",
        &bs,
    ).await;

    // ── 8. Social Media Trends ────────────────────────────────────────────────
    run_scenario(
        "8. Social Media — trending hashtags",
        &producer, "krishiv-social",
        schema(&[("hashtag", DataType::Utf8), ("platform", DataType::Utf8),
                 ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"hashtag":"#rust","platform":"twitter","ts":8000}),
            serde_json::json!({"hashtag":"#kafka","platform":"linkedin","ts":8010}),
            serde_json::json!({"hashtag":"#rust","platform":"twitter","ts":8020}),
            serde_json::json!({"hashtag":"#streaming","platform":"twitter","ts":8030}),
            serde_json::json!({"hashtag":"#rust","platform":"reddit","ts":8040}),
            serde_json::json!({"hashtag":"#kafka","platform":"twitter","ts":8050}),
        ],
        "SELECT hashtag, COUNT(*) as mentions, COUNT(DISTINCT platform) as platform_reach \
         FROM social GROUP BY hashtag ORDER BY mentions DESC",
        &bs,
    ).await;

    // ── 9. Gaming Leaderboard ─────────────────────────────────────────────────
    run_scenario(
        "9. Gaming — leaderboard by total score",
        &producer, "krishiv-gaming",
        schema(&[("player_id", DataType::Utf8), ("score", DataType::Int64),
                 ("level", DataType::Int64), ("ts", DataType::Int64)]),
        &[
            serde_json::json!({"player_id":"Alice","score":1500,"level":10,"ts":9000}),
            serde_json::json!({"player_id":"Bob","score":2300,"level":12,"ts":9010}),
            serde_json::json!({"player_id":"Alice","score":1800,"level":11,"ts":9020}),
            serde_json::json!({"player_id":"Charlie","score":3100,"level":15,"ts":9030}),
            serde_json::json!({"player_id":"Bob","score":2100,"level":12,"ts":9040}),
        ],
        "SELECT player_id, SUM(score) as total_score, MAX(level) as max_level \
         FROM gaming GROUP BY player_id ORDER BY total_score DESC",
        &bs,
    ).await;

    // ── 10. Retail + sql_to_kafka write-back ──────────────────────────────────
    println!("\n--- 10. Retail — replenishment signals + sql_to_kafka write-back ---");

    let retail_schema = schema(&[("sku", DataType::Utf8), ("warehouse", DataType::Utf8),
                                  ("units_sold", DataType::Int64), ("ts", DataType::Int64)]);

    let retail_engine = krishiv_sql::SqlEngine::new();
    let retail_group = unique_group("retail");
    if let Err(e) = retail_engine.register_kafka_source(
        "retail", retail_schema.clone(), &bs, "krishiv-retail", &retail_group,
    ) {
        println!("  register error: {e}");
    } else {
        tokio::time::sleep(Duration::from_millis(600)).await;
        produce(&producer, "krishiv-retail", &[
            serde_json::json!({"sku":"COFFEE-001","warehouse":"WH-A","units_sold":45,"ts":10000}),
            serde_json::json!({"sku":"TEA-002","warehouse":"WH-A","units_sold":30,"ts":10010}),
            serde_json::json!({"sku":"COFFEE-001","warehouse":"WH-B","units_sold":60,"ts":10020}),
            serde_json::json!({"sku":"MILK-003","warehouse":"WH-A","units_sold":120,"ts":10030}),
            serde_json::json!({"sku":"TEA-002","warehouse":"WH-B","units_sold":25,"ts":10040}),
        ]).await;

        let batches = collect_micro_batch(&retail_engine, "retail", 5, Duration::from_secs(5)).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("  collected {total} rows from 'krishiv-retail'");

        if total > 0 {
            let agg_engine = krishiv_sql::SqlEngine::new();
            agg_engine.register_record_batches("retail", batches).await.unwrap();

            // Show replenishment query result.
            let agg_sql = "SELECT sku, SUM(units_sold) as total_sold, \
                            COUNT(DISTINCT warehouse) as warehouses \
                           FROM retail GROUP BY sku ORDER BY total_sold DESC";
            if let Ok(df) = agg_engine.sql(agg_sql).await {
                if let Ok(results) = df.collect().await {
                    for b in &results { print_batch(b); }
                }
            }

            // Write the aggregated results back to Kafka (sql_to_kafka).
            let write_engine = krishiv_sql::SqlEngine::new();
            write_engine
                .register_record_batches("retail", {
                    // re-fetch since we consumed `batches` above
                    let retail_group2 = unique_group("retail-write");
                    let src = krishiv_sql::SqlEngine::new();
                    src.register_kafka_source(
                        "retail_raw", retail_schema, &bs, "krishiv-retail", &retail_group2,
                    ).unwrap();
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    collect_micro_batch(&src, "retail_raw", 5, Duration::from_secs(3)).await
                })
                .await
                .unwrap();

            match write_engine.sql_to_kafka(agg_sql, &bs, "krishiv-fraud-alerts").await {
                Ok(n) => println!("  sql_to_kafka: wrote {n} rows → 'krishiv-fraud-alerts'"),
                Err(e) => println!("  sql_to_kafka error: {e}"),
            }
        }
    }

    println!("\n=== All 10 scenarios complete ===");
    println!("    register_kafka_source + sql_to_kafka — zero Python");
    Ok(())
}

// ── display ───────────────────────────────────────────────────────────────────

fn print_batch(batch: &RecordBatch) {
    use arrow::array::{Float64Array, Int64Array, StringArray};

    let schema = batch.schema();
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    println!("  {}", headers.join("  |  "));
    println!("  {}", "-".repeat(headers.join("  |  ").len()));
    for row in 0..batch.num_rows() {
        let vals: Vec<String> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let col = batch.column(i);
                if col.is_null(row) {
                    return "null".to_string();
                }
                match f.data_type() {
                    DataType::Utf8 => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| a.value(row).to_string())
                        .unwrap_or_else(|| "?".into()),
                    DataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.value(row).to_string())
                        .unwrap_or_else(|| "?".into()),
                    DataType::Float64 => col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .map(|a| format!("{:.4}", a.value(row)))
                        .unwrap_or_else(|| "?".into()),
                    _ => arrow::util::display::array_value_to_string(col.as_ref(), row)
                        .unwrap_or_else(|_| "?".into()),
                }
            })
            .collect();
        println!("  {}", vals.join("  |  "));
    }
}
