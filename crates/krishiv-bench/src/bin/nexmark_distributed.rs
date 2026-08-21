//! NEXMark against a LIVE distributed cluster (task #147).
//!
//! Every one of the 22 queries is compiled with the same routing ladder the
//! engine uses (pipeline → join → window → stateless), registered on the
//! coordinator named by `KRISHIV_COORDINATOR_URL` through the class-routed
//! wire, fed by HTTP pushes (side-tagged for two-source classes), and
//! drained through the run-loop egress. Throughput is wall-clock from first
//! push to a STABLE drained row count — network, registration, exchange and
//! egress all included, which is the point.
//!
//! Method: 1 warm-up + 3 measured reps (each rep a FRESH job id — run-loop
//! reregistration is convergent, and reusing an id would carry state across
//! reps), median reported, completeness gated per query contract. The
//! determinism assertion from the embedded harness is deliberately absent:
//! cross-network arrival order is not deterministic, and pretending it is
//! would make the gate lie.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]

use std::time::{Duration, Instant};

use krishiv_bench::nexmark::{NEXMARK_TOTAL_QUERIES, NexmarkGenerator, RowsOut, SUPPORTED_QUERIES};
use krishiv_plan::stream_task::{StatelessQuerySpec, StreamingTaskSpec};

const BATCH_ROWS: usize = 1_000;
const BATCHES: usize = 100;
const MAX_LATENESS_MS: i64 = 200;
const WARMUP_REPS: usize = 1;
const MEASURED_REPS: usize = 3;
const DRAIN_STABLE_POLLS: usize = 3;
const DRAIN_POLL: Duration = Duration::from_millis(100);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

fn task_for(sql: &str, source_hint: &str) -> StreamingTaskSpec {
    if krishiv_sql::streaming_pipeline_plan::looks_like_streaming_pipeline(sql) {
        StreamingTaskSpec::Pipeline(Box::new(
            krishiv_sql::streaming_pipeline_plan::compile_streaming_pipeline_sql(sql)
                .expect("pipeline compiles")
                .spec,
        ))
    } else if krishiv_sql::streaming_join_plan::looks_like_streaming_join(sql) {
        StreamingTaskSpec::Join(Box::new(
            krishiv_sql::streaming_join_plan::compile_streaming_join_sql(sql)
                .expect("join compiles")
                .spec,
        ))
    } else if krishiv_sql::streaming_tvf::find_window_tvf(sql).is_some() {
        StreamingTaskSpec::Window(Box::new(
            krishiv_sql::streaming_window_plan::compile_streaming_window_sql(sql)
                .expect("window compiles")
                .spec,
        ))
    } else {
        // Queries that JOIN the bounded reference table carry it in the spec
        // — the wire-native form of what the embedded harness does by
        // registering "side" on the executor before the stream starts.
        let side_tables = if sql.contains(" side ") || sql.contains(" side\n") {
            vec![side_table_spec()]
        } else {
            vec![]
        };
        StreamingTaskSpec::Stateless(Box::new(StatelessQuerySpec {
            sql: sql.to_owned(),
            source: source_hint.to_owned(),
            side_tables,
        }))
    }
}

/// The q13 reference table (auction id % 1000 → label), wire-encoded. Same
/// shape the embedded harness registers as "side".
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
    let batch = arrow::record_batch::RecordBatch::try_new(
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

fn side_batches(source: &str, batches: usize) -> Vec<arrow::record_batch::RecordBatch> {
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

struct RepResult {
    rows_in: usize,
    rows_out: usize,
    events_per_sec: f64,
}

async fn drain_until_stable(url: &str, job: &str) -> usize {
    let deadline = Instant::now() + DRAIN_TIMEOUT;
    let mut total = 0usize;
    let mut stable = 0usize;
    loop {
        let drained = krishiv_runtime::execute_coordinator_continuous_drain(url, job)
            .await
            .unwrap_or_default();
        let got: usize = drained.iter().map(|b| b.num_rows()).sum();
        if got == 0 {
            stable += 1;
        } else {
            total += got;
            stable = 0;
        }
        if (total > 0 && stable >= DRAIN_STABLE_POLLS) || Instant::now() >= deadline {
            return total;
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }
}

async fn measure_rep(url: &str, name: &str, sql: &str, rep: usize) -> RepResult {
    let job = format!("nexd-{name}-{rep}");
    let task = task_for(sql, "bid");
    let parallelism: u32 = match &task {
        StreamingTaskSpec::Pipeline(_) => 1,
        _ => std::env::var("KRISHIV_BENCH_PARALLELISM")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3),
    };
    let mut options = krishiv_runtime::ContinuousRegisterOptions::run_loop(parallelism);
    options.mode = Some(String::from("run-loop"));
    krishiv_runtime::execute_coordinator_continuous_register_task(url, &job, &task, &options)
        .await
        .unwrap_or_else(|e| panic!("{name}: register: {e}"));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    let mut rows_in = 0usize;
    match &task {
        StreamingTaskSpec::Join(j) => {
            let left = side_batches(&j.left_source, BATCHES);
            let right = side_batches(&j.right_source, BATCHES);
            rows_in += left
                .iter()
                .chain(right.iter())
                .map(|b| b.num_rows())
                .sum::<usize>();
            for (l, r) in left.chunks(4).zip(right.chunks(4)) {
                krishiv_runtime::execute_coordinator_continuous_push_side(url, &job, "R", r)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: push R: {e}"));
                krishiv_runtime::execute_coordinator_continuous_push_side(url, &job, "L", l)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: push L: {e}"));
            }
        }
        StreamingTaskSpec::Pipeline(p) => {
            let left = side_batches(&p.join.left_source, BATCHES);
            let right = side_batches(&p.join.right_source, BATCHES);
            rows_in += left
                .iter()
                .chain(right.iter())
                .map(|b| b.num_rows())
                .sum::<usize>();
            for (l, r) in left.chunks(4).zip(right.chunks(4)) {
                krishiv_runtime::execute_coordinator_continuous_push_side(url, &job, "R", r)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: push R: {e}"));
                krishiv_runtime::execute_coordinator_continuous_push_side(url, &job, "L", l)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: push L: {e}"));
            }
        }
        _ => {
            let batches = side_batches("bid", BATCHES);
            rows_in += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            for chunk in batches.chunks(4) {
                krishiv_runtime::execute_coordinator_continuous_push(url, &job, chunk)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: push: {e}"));
            }
        }
    }
    // The workload is bounded; the run-loop surface is not. Declare
    // end-of-stream so subtasks flush their open windows into egress —
    // without this, a window the data's own event times never close (the
    // whole stream can span less than one window) is silently unemitted and
    // the drain below reports a pipeline that "produced nothing".
    krishiv_runtime::execute_coordinator_continuous_flush(url, &job)
        .await
        .unwrap_or_else(|e| panic!("{name}: flush: {e}"));
    let rows_out = drain_until_stable(url, &job).await;
    let elapsed = started.elapsed();
    // Tear the job down before the next rep registers: a registered run-loop
    // job holds its executor slots until deregistered, and 3 leftover jobs at
    // parallelism 3 exhaust a 3x3-slot cluster — the next rep's pushes then
    // fail with "no launched subtasks to push to".
    krishiv_runtime::execute_coordinator_continuous_deregister(url, &job)
        .await
        .unwrap_or_else(|e| panic!("{name}: deregister: {e}"));
    RepResult {
        rows_in,
        rows_out,
        events_per_sec: rows_in as f64 / elapsed.as_secs_f64(),
    }
}

#[tokio::main]
async fn main() {
    let url = std::env::var("KRISHIV_COORDINATOR_URL")
        .expect("KRISHIV_COORDINATOR_URL must point at the live coordinator");
    println!("NEXMark DISTRIBUTED harness — krishiv");
    println!(
        "coordinator: {url}  ({} of {} queries, {} rows/batch x {} batches, \
         {} warm-up + {} reps, median)",
        SUPPORTED_QUERIES.len(),
        NEXMARK_TOTAL_QUERIES,
        BATCH_ROWS,
        BATCHES,
        WARMUP_REPS,
        MEASURED_REPS
    );

    let mut failures: Vec<String> = Vec::new();
    println!(
        "{:<24} {:>12} {:>21} {:>9} {:>9}",
        "query", "ev/sec med", "ev/sec min-max", "rows in", "rows out"
    );
    for q in SUPPORTED_QUERIES {
        for w in 0..WARMUP_REPS {
            let _ = measure_rep(&url, q.name, q.sql, 1000 + w).await;
        }
        let mut reps = Vec::new();
        for rep in 0..MEASURED_REPS {
            reps.push(measure_rep(&url, q.name, q.sql, rep).await);
        }
        let mut rates: Vec<f64> = reps.iter().map(|r| r.events_per_sec).collect();
        rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = rates.get(rates.len() / 2).copied().unwrap_or(0.0);
        let last = reps.last().expect("reps");
        println!(
            "{:<24} {:>12.0} {:>9.0}-{:>9.0} {:>9} {:>9}",
            q.name,
            median,
            rates.first().copied().unwrap_or(0.0),
            rates.last().copied().unwrap_or(0.0),
            last.rows_in,
            last.rows_out
        );
        match q.expect {
            RowsOut::NonZero if last.rows_out == 0 => failures.push(format!(
                "{}: consumed {} rows and emitted NOTHING",
                q.name, last.rows_in
            )),
            RowsOut::ExactInput if last.rows_out != last.rows_in => failures.push(format!(
                "{}: {} rows in but {} out — a passthrough must emit every row",
                q.name, last.rows_in, last.rows_out
            )),
            _ => {}
        }
    }

    if failures.is_empty() {
        println!(
            "\ncompleteness gate: PASS ({} queries)",
            SUPPORTED_QUERIES.len()
        );
    } else {
        eprintln!("\ncompleteness gate: FAIL");
        for f in &failures {
            eprintln!("  - {f}");
        }
        std::process::exit(1);
    }
    println!(
        "\nMeasured END TO END across the cluster: registration, HTTP push, keyed \n\
         exchange, operator execution, and egress drain. Not measured: recovery, \n\
         rescale, state growth beyond memory."
    );
}
