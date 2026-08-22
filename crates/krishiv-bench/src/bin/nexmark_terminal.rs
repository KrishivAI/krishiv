//! NEXMark through the CONVERGED streaming surface (task #151): every job is
//! built as a `StreamingDataFrame`, started through `write()`, and driven by
//! the unified `StreamingJob` handle — the exact API users hold — against a
//! live coordinator (single-node or the k3s rig), not the raw registration
//! harness. Measures registration, push, execution, EOS flush, and drain end
//! to end, plus one update-mode and one complete-mode case to exercise the
//! mitigated output modes on the wire.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use krishiv_api::{Session, StreamingJob};
use krishiv_bench::nexmark::{NexmarkGenerator, RowsOut, SUPPORTED_QUERIES};
use krishiv_plan::stream_task::StreamingTaskSpec;

const BATCH_ROWS: usize = 1_000;
const BATCHES: usize = 100;
const REPS: usize = 3;
const MAX_LATENESS_MS: i64 = 5_000;

struct Case {
    name: &'static str,
    output_mode: &'static str,
    window_ms: u64,
    key: &'static str,
}

const MODE_CASES: &[Case] = &[
    Case {
        name: "t4_update_mode",
        output_mode: "update",
        window_ms: 10_000,
        key: "auction",
    },
    Case {
        name: "t5_complete_mode",
        output_mode: "complete",
        window_ms: 10_000,
        key: "bidder",
    },
];

/// The q13 reference table (auction id % 1000 -> label), identical to the
/// wire-native harness's side registration.
fn side_table_spec() -> krishiv_plan::stream_task::SideTableSpec {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use base64::Engine as _;
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let keys: Vec<i64> = (0..1000).collect();
    let labels: Vec<String> = keys.iter().map(|k| format!("label-{k}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(Int64Array::from(keys)),
            std::sync::Arc::new(StringArray::from(labels)),
        ],
    )
    .expect("side batch");
    let mut sink = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut sink, &schema).expect("ipc writer");
        writer.write(&batch).expect("write side batch");
        writer.finish().expect("finish side ipc");
    }
    krishiv_plan::stream_task::SideTableSpec {
        name: String::from("side"),
        ipc_base64: base64::engine::general_purpose::STANDARD.encode(sink),
    }
}

fn source_batches(source: &str, batches: usize) -> Vec<RecordBatch> {
    let mut generator = NexmarkGenerator::new(0x4E45_584D, 1_000_000, 0, MAX_LATENESS_MS);
    (0..batches)
        .map(|_| match source {
            "bid" => generator.next_bid_batch(BATCH_ROWS),
            "auction" => generator.next_auction_batch(BATCH_ROWS),
            "person" => generator.next_person_batch(BATCH_ROWS),
            other => panic!("no generator for source '{other}'"),
        })
        .map(|r| r.expect("generate"))
        .collect()
}

fn env_parallelism() -> u32 {
    std::env::var("KRISHIV_BENCH_PARALLELISM")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

fn checkpointed(
    mut writer: krishiv_api::stream_write::StreamWriter,
) -> krishiv_api::stream_write::StreamWriter {
    if let (Ok(interval), Ok(path)) = (
        std::env::var("KRISHIV_BENCH_CHECKPOINT_INTERVAL_MS"),
        std::env::var("KRISHIV_BENCH_CHECKPOINT_PATH"),
    ) && let Ok(ms) = interval.parse::<u64>()
        && ms > 0
    {
        writer = writer.checkpoint(ms, path);
    }
    writer
}

/// One rep of a corpus query THROUGH the terminal: stream_sql -> write() ->
/// StreamingJob, with side-tagged pushes for the two-source classes.
async fn run_corpus_rep(
    session: &Session,
    q: &krishiv_bench::nexmark::BenchQuery,
    rep: usize,
) -> (usize, usize, f64) {
    let nonce = std::process::id();
    let job_name = format!("nexterm-{}-{rep}-{nonce}", q.name);
    let sides = if q.sql.contains(" side ") || q.sql.contains(" side\n") {
        vec![side_table_spec()]
    } else {
        Vec::new()
    };
    let df = session
        .stream_sql_with_sides(q.sql, sides)
        .unwrap_or_else(|e| panic!("{}: compile: {e}", q.name));
    let (two_source, parallelism) = match df.task_override().expect("stream_sql sets the task") {
        StreamingTaskSpec::Join(j) => (
            Some((j.left_source.clone(), j.right_source.clone())),
            env_parallelism(),
        ),
        StreamingTaskSpec::Pipeline(p) => (
            Some((p.join.left_source.clone(), p.join.right_source.clone())),
            if p.parallel_unsafe_reason().is_some() {
                1
            } else {
                env_parallelism()
            },
        ),
        _ => (None, env_parallelism()),
    };
    let writer = checkpointed(
        df.write()
            .output_mode("append")
            .trigger("available_now", 0)
            .parallelism(parallelism),
    );
    let job = writer
        .start(session, &job_name)
        .unwrap_or_else(|e| panic!("{}: start: {e}", q.name));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    let mut rows_in = 0usize;
    match &two_source {
        Some((left_source, right_source)) => {
            let left = source_batches(left_source, BATCHES);
            let right = source_batches(right_source, BATCHES);
            rows_in += left
                .iter()
                .chain(right.iter())
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            for (l, r) in left.chunks(4).zip(right.chunks(4)) {
                job.push_side("R", r)
                    .await
                    .unwrap_or_else(|e| panic!("{}: push R: {e}", q.name));
                job.push_side("L", l)
                    .await
                    .unwrap_or_else(|e| panic!("{}: push L: {e}", q.name));
            }
        }
        None => {
            let batches = source_batches("bid", BATCHES);
            rows_in += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            for chunk in batches.chunks(4) {
                job.push(chunk.to_vec())
                    .await
                    .unwrap_or_else(|e| panic!("{}: push: {e}", q.name));
            }
        }
    }
    let flushed = job
        .flush()
        .await
        .unwrap_or_else(|e| panic!("{}: flush: {e}", q.name));
    let mut rows_out: usize = flushed.iter().map(RecordBatch::num_rows).sum();
    rows_out += drain_until_stable(&job, false).await;
    let elapsed = started.elapsed();
    job.stop()
        .await
        .unwrap_or_else(|e| panic!("{}: stop: {e}", q.name));
    (rows_in, rows_out, rows_in as f64 / elapsed.as_secs_f64())
}

fn bid_batches(batches: usize) -> Vec<RecordBatch> {
    let mut generator = NexmarkGenerator::new(0x4E45_584D, 1_000_000, 0, MAX_LATENESS_MS);
    (0..batches)
        .map(|_| generator.next_bid_batch(BATCH_ROWS).expect("generate"))
        .collect()
}

async fn drain_until_stable(job: &StreamingJob, complete: bool) -> usize {
    // Complete mode folds every delta into a view and returns the FULL table
    // on every drain, so "a drain returned no rows" never happens once the
    // first window fires — the delta criterion below loops forever there
    // (attempt22/23's hang). Stable for complete = the table size unchanged
    // across 4 consecutive drains; rows out = that final table size.
    if complete {
        let mut last = usize::MAX;
        let mut quiet = 0;
        while quiet < 4 {
            let out = job.drain().await.expect("drain");
            let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
            if rows == last {
                quiet += 1;
            } else {
                quiet = 0;
                last = rows;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        return last;
    }
    let mut total = 0usize;
    let mut quiet = 0;
    while quiet < 4 {
        let out = job.drain().await.expect("drain");
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        if rows == 0 {
            quiet += 1;
            tokio::time::sleep(Duration::from_millis(150)).await;
        } else {
            quiet = 0;
            total += rows;
        }
    }
    total
}

async fn run_case(session: &Session, case: &Case, rep: usize) -> (usize, usize, f64) {
    // A per-process nonce keeps job ids unique across harness runs: a prior
    // run killed mid-case leaves its continuous job live, and re-registering
    // the same id collides with that incarnation (now a loud registration
    // error; before 2026-08-22 it was a silent two-hour wedge).
    let nonce = std::process::id();
    let job_name = format!("nexterm-{}-{rep}-{nonce}", case.name);
    let mut writer = session
        .sql(
            "SELECT CAST(0 AS BIGINT) AS auction, CAST(0 AS BIGINT) AS bidder, \
              CAST(0 AS BIGINT) AS price, CAST(0 AS BIGINT) AS \"dateTime\"",
        )
        .expect("seed df")
        .stream()
        .with_event_time("dateTime")
        .key_by(case.key)
        .tumbling_window(case.window_ms)
        .write()
        .output_mode(case.output_mode)
        .trigger("available_now", 0);
    if let (Ok(interval), Ok(path)) = (
        std::env::var("KRISHIV_BENCH_CHECKPOINT_INTERVAL_MS"),
        std::env::var("KRISHIV_BENCH_CHECKPOINT_PATH"),
    ) && let Ok(ms) = interval.parse::<u64>()
        && ms > 0
    {
        writer = writer.checkpoint(ms, path);
    }
    let job = writer
        .start(session, &job_name)
        .unwrap_or_else(|e| panic!("{}: start: {e}", case.name));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let batches = bid_batches(BATCHES);
    let rows_in: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let started = Instant::now();
    for chunk in batches.chunks(4) {
        job.push(chunk.to_vec())
            .await
            .unwrap_or_else(|e| panic!("{}: push: {e}", case.name));
    }
    let flushed = job
        .flush()
        .await
        .unwrap_or_else(|e| panic!("{}: flush: {e}", case.name));
    let mut rows_out: usize = flushed.iter().map(RecordBatch::num_rows).sum();
    let drained = drain_until_stable(&job, case.output_mode == "complete").await;
    if case.output_mode == "complete" {
        // The last full-table snapshot IS the answer.
        rows_out = drained.max(rows_out);
    } else {
        rows_out += drained;
    }
    let elapsed = started.elapsed();
    job.stop()
        .await
        .unwrap_or_else(|e| panic!("{}: stop: {e}", case.name));
    let evs = rows_in as f64 / elapsed.as_secs_f64();
    (rows_in, rows_out, evs)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let http = std::env::var("KRISHIV_COORDINATOR_URL")
        .unwrap_or_else(|_| String::from("http://127.0.0.1:27072"));
    let flight = std::env::var("KRISHIV_FLIGHT_URL")
        .unwrap_or_else(|_| String::from("http://127.0.0.1:27075"));
    let session = Session::builder()
        .with_execution_mode(krishiv_api::types::ExecutionMode::Distributed)
        .with_local_cluster(flight.clone())
        .with_coordinator_http(http.clone())
        .build()
        .expect("distributed session");

    println!("NEXMark TERMINAL harness — StreamingDataFrame.write() end to end");
    println!(
        "coordinator: {http}  flight: {flight}  ({} corpus queries + {} mode cases, \
         {BATCH_ROWS} rows/batch x {BATCHES} batches, 1 warm-up + {REPS} reps, median)",
        SUPPORTED_QUERIES.len(),
        MODE_CASES.len()
    );
    println!(
        "{:<26} {:>12} {:>10} {:>10}",
        "query", "ev/sec med", "rows in", "rows out"
    );

    let mut failures: Vec<String> = Vec::new();

    for q in SUPPORTED_QUERIES {
        let _ = run_corpus_rep(&session, q, 1000).await;
        let mut evs: Vec<f64> = Vec::new();
        let mut last = (0usize, 0usize);
        for rep in 0..REPS {
            let (rin, rout, e) = run_corpus_rep(&session, q, rep).await;
            evs.push(e);
            last = (rin, rout);
        }
        evs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = evs.get(evs.len() / 2).copied().unwrap_or_default();
        println!("{:<26} {:>12.0} {:>10} {:>10}", q.name, med, last.0, last.1);
        match q.expect {
            RowsOut::NonZero if last.1 == 0 => failures.push(format!(
                "{}: consumed {} rows and emitted NOTHING",
                q.name, last.0
            )),
            RowsOut::ExactInput if last.1 != last.0 => failures.push(format!(
                "{}: {} rows in but {} out — a passthrough must emit every row",
                q.name, last.0, last.1
            )),
            _ => {}
        }
    }

    for case in MODE_CASES {
        let mut evs: Vec<f64> = Vec::new();
        let mut last = (0usize, 0usize);
        for rep in 0..REPS {
            let (rin, rout, e) = run_case(&session, case, rep).await;
            evs.push(e);
            last = (rin, rout);
        }
        evs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = evs.get(evs.len() / 2).copied().unwrap_or_default();
        println!(
            "{:<26} {:>12.0} {:>10} {:>10}",
            case.name, med, last.0, last.1
        );
        if last.1 == 0 {
            failures.push(format!(
                "{}: consumed {} rows and emitted NOTHING",
                case.name, last.0
            ));
        }
    }

    if failures.is_empty() {
        println!(
            "\ncompleteness gate: PASS ({} corpus + {} mode cases)",
            SUPPORTED_QUERIES.len(),
            MODE_CASES.len()
        );
    } else {
        println!("\ncompleteness gate: FAIL");
        for f in &failures {
            println!("  - {f}");
        }
        std::process::exit(1);
    }
}
