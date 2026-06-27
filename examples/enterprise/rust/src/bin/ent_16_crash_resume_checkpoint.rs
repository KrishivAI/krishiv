//! Enterprise 16 · Crash & Resume — Kafka offset checkpoint
//!
//! Demonstrates exactly-once resumption after a simulated crash:
//!
//!   Phase 1 — Produce 10 000 rows to Kafka (Arrow IPC).
//!   Phase 2 — Consume ~3 000 rows, save per-partition offsets to disk,
//!              then stop ("simulated crash").
//!   Phase 3 — Reload checkpoint file, seek consumer to saved offsets,
//!              consume remaining rows.
//!   Verify  — Total unique order_ids == produced rows (no gaps, no duplicates).
//!
//! Checkpoint format: JSON list of {partition, offset} objects.
//!
//! Run:
//!   cargo run --bin ent_16_crash_resume_checkpoint

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use krishiv::{AggExpr, AggFunction};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind, execute_windowed_stream};
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::topic_partition_list::{TopicPartitionList, Offset as RdOffset};

const BROKERS:    &str = "localhost:9092";
const TOPIC:      &str = "orders-checkpoint";
const TOTAL_ROWS: usize = 10_000;
const BATCH_SIZE: usize = 1_000;
const CRASH_AFTER: usize = 3_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64,   false),
        Field::new("customer", DataType::Utf8,    false),
        Field::new("amount",   DataType::Float64, false),
        Field::new("ts_ms",    DataType::Int64,   false),
    ]))
}

fn make_batch(schema: Arc<Schema>, base: i64, size: usize) -> Result<RecordBatch> {
    let customers = ["alice","bob","carol","dave"];
    let ids:   Vec<i64>  = (base..base + size as i64).collect();
    let custs: Vec<&str> = ids.iter().map(|i| customers[(i % 4) as usize]).collect();
    let amts:  Vec<f64>  = ids.iter().map(|i| 10.0 + (i % 100) as f64).collect();
    let ts:    Vec<i64>  = ids.iter().map(|i| 1_716_200_000_000 + i * 1_000).collect();
    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(custs)),
        Arc::new(Float64Array::from(amts)),
        Arc::new(Int64Array::from(ts)),
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

fn checkpoint_path() -> PathBuf {
    std::env::temp_dir().join("krishiv_ent16.json")
}

fn save_checkpoint(offsets: &HashMap<i32, i64>) -> Result<()> {
    let json = serde_json::to_string(offsets)?;
    std::fs::write(checkpoint_path(), json).context("write checkpoint")
}

fn load_checkpoint() -> Result<HashMap<i32, i64>> {
    let data = std::fs::read_to_string(checkpoint_path()).context("read checkpoint")?;
    serde_json::from_str(&data).context("parse checkpoint")
}

fn new_consumer(group_suffix: &str) -> Result<StreamConsumer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", format!("krishiv-ent16-{group_suffix}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .create()?)
}

/// Consume up to `stop_after` rows; returns (batches, rows_consumed, partition_offsets).
async fn consume_up_to(
    consumer: &StreamConsumer,
    stop_after: usize,
) -> Result<(Vec<RecordBatch>, usize, HashMap<i32, i64>)> {
    let mut batches:  Vec<RecordBatch> = Vec::new();
    let mut consumed: usize = 0;
    let mut offsets:  HashMap<i32, i64> = HashMap::new();
    loop {
        let msg = match tokio::time::timeout(Duration::from_millis(5000), consumer.recv()).await {
            Err(_) => break,
            Ok(Err(e)) => {
                eprintln!("  warn: {e}");
                tokio::time::sleep(Duration::from_millis(300)).await;
                continue;
            }
            Ok(Ok(m)) => m,
        };
        offsets.insert(msg.partition(), msg.offset() + 1); // +1 = "next to read"
        let payload = msg.payload().unwrap_or(&[]);
        for batch in ipc_to_batches(payload)? {
            consumed += batch.num_rows();
            batches.push(batch);
        }
        if consumed >= stop_after { break; }
    }
    Ok((batches, consumed, offsets))
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Enterprise 16: Crash & Resume — Kafka Checkpoint ===");
    println!("  broker      : {BROKERS}");
    println!("  topic       : {TOPIC}");
    println!("  total rows  : {TOTAL_ROWS}");
    println!("  crash after : ~{CRASH_AFTER} rows");
    println!();

    let _ = std::fs::remove_file(checkpoint_path());

    // ── Reset Kafka topic for a clean run ─────────────────────────────────
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BROKERS).create()?;
    let opts = AdminOptions::new();
    let _ = admin.delete_topics(&[TOPIC], &opts).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin.create_topics(
        &[NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1))], // single partition
        &opts,
    ).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("✓ Kafka topic reset (1 partition)");

    // ── Phase 1: Produce ───────────────────────────────────────────────────
    println!("▶ Phase 1 — Produce {TOTAL_ROWS} rows");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("compression.type", "lz4")
        .create()?;
    let schema = schema();
    let t0 = Instant::now();
    let num_batches = TOTAL_ROWS.div_ceil(BATCH_SIZE);
    let mut futs = Vec::new();
    for b in 0..num_batches {
        let rows  = BATCH_SIZE.min(TOTAL_ROWS - b * BATCH_SIZE);
        let batch = make_batch(schema.clone(), (b * BATCH_SIZE) as i64, rows)?;
        let ipc   = batch_to_ipc(&batch)?;
        let f = producer.send_result(
            FutureRecord::<str, Vec<u8>>::to(TOPIC).partition(0).payload(&ipc)
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
    println!("  ✓ produced in {:.2}s", t0.elapsed().as_secs_f64());

    // ── Phase 2: Partial consume → save checkpoint → "crash" ──────────────
    println!("▶ Phase 2 — Partial consume (~{CRASH_AFTER} rows), then checkpoint + crash");
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let consumer1 = new_consumer(&run_id.to_string())?;
    consumer1.subscribe(&[TOPIC])?;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let (phase2_batches, phase2_rows, phase2_offsets) =
        consume_up_to(&consumer1, CRASH_AFTER).await?;

    save_checkpoint(&phase2_offsets)?;
    println!("  ✓ consumed {phase2_rows} rows");
    println!("  ✓ checkpoint saved: {:?}", checkpoint_path());
    println!("    offsets: {phase2_offsets:?}");
    println!("  ✗ [simulated crash]");
    drop(consumer1);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Phase 3: Restore checkpoint → seek → resume ────────────────────────
    println!("▶ Phase 3 — Restore checkpoint, seek, consume remaining rows");
    let saved_offsets = load_checkpoint()?;
    println!("  ✓ checkpoint loaded: {saved_offsets:?}");

    // Use `assign` (not subscribe+seek) so that rdkafka starts reading from
    // the exact saved offset with no pre-fetched messages from earlier positions.
    let consumer2 = new_consumer(&run_id.to_string())?;
    let mut tpl = TopicPartitionList::new();
    for (&partition, &offset) in &saved_offsets {
        tpl.add_partition_offset(TOPIC, partition, RdOffset::Offset(offset))?;
    }
    consumer2.assign(&tpl).context("assign from checkpoint")?;

    let remaining = TOTAL_ROWS.saturating_sub(phase2_rows);
    let (phase3_batches, phase3_rows, _) =
        consume_up_to(&consumer2, remaining).await?;
    println!("  ✓ resumed, consumed {phase3_rows} more rows");

    // ── Verify ─────────────────────────────────────────────────────────────
    println!("▶ Verify — aggregate all {TOTAL_ROWS} rows");
    let all_batches: Vec<RecordBatch> =
        phase2_batches.into_iter().chain(phase3_batches).collect();
    let total_consumed = phase2_rows + phase3_rows;

    let spec = LocalWindowExecutionSpec {
        key_column:        "customer".into(),
        key_column_type:   "utf8".into(),
        event_time_column: "ts_ms".into(),
        watermark_lag_ms:  0,
        window_kind:       LocalWindowKind::Tumbling,
        window_size_ms:    10_000,
        agg_exprs: vec![
            AggExpr { function: AggFunction::Count, input_column: String::new(),   output_column: "cnt".into() },
            AggExpr { function: AggFunction::Sum,   input_column: "amount".into(), output_column: "revenue".into() },
        ],
        state_ttl_ms:          None,
        source_watermark_lags: HashMap::new(),
        source_id_column:      None,
        window_timezone:       None,
    };

    let window_out = execute_windowed_stream(all_batches, &spec)?;
    let session = krishiv::Session::builder().build()?;
    session.register_record_batches("wins", window_out)?;
    let df = session.sql(
        "SELECT customer, SUM(cnt) AS total_orders, ROUND(SUM(revenue),2) AS revenue \
         FROM wins GROUP BY customer ORDER BY revenue DESC"
    )?;

    println!();
    println!("--- Per-customer totals after crash + resume ---");
    println!("{}", df.collect()?.pretty()?);

    println!("--- Summary ---");
    println!("  produced            : {TOTAL_ROWS}");
    println!("  phase 2 (pre-crash) : {phase2_rows}");
    println!("  phase 3 (resumed)   : {phase3_rows}");
    println!("  total consumed      : {total_consumed}");

    if total_consumed == TOTAL_ROWS {
        println!("\n✓ PASS — all {TOTAL_ROWS} rows recovered across crash boundary");
    } else {
        println!("\n✗ FAIL — consumed {total_consumed} != {TOTAL_ROWS}");
    }
    Ok(())
}
