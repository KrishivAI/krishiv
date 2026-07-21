//! Streaming latency benchmarks for Krishiv.
//!
//! Measures the per-batch end-to-end latency of the three placement modes
//! (embedded, single-node, distributed) on a tumbling-window aggregation,
//! the canonical streaming workload. The targets are documented in
//! `docs/implementation/phase-1-engine-contract.md`:
//!
//! - Embedded P99 < 1 ms per batch
//! - Single-node P99 < 5 ms per batch
//! - Distributed P99 < 50 ms per batch
//!
//! The embedded/single-node benchmarks drive `ContinuousWindowExecutor::drain`
//! directly rather than going through `run_job`: job dispatch, checkpoint-
//! service construction, and (for single-node) the RocksDB state-backend open
//! are one-time-per-job costs in production — a continuous job builds its
//! executor once and processes many batches over its lifetime — not a
//! per-batch cost. They belong in the untimed setup closure, not the timed
//! region.
//!
//! Each timed closure processes exactly one batch, with nine prior batches
//! already drained (untimed, in setup) into the same still-open tumbling
//! window. This measures the steady-state cost of an ordinary batch that
//! updates already-known per-key state — not one-time job-startup cost, and
//! not a window's close/emit cost (the timed batch's watermark stays short
//! of the window's end, so no window closes during the timed call). Timing a
//! whole multi-batch sequence in one criterion sample instead would report
//! cumulative multi-batch latency as if it were a single batch's — comparing
//! that against a per-batch target would be just as misleading as including
//! setup cost. Window-close/emit cost is a distinct, currently unmeasured
//! cost (it lands on whichever batch happens to cross a window boundary);
//! this file only measures the common case.
//!
//! To run: cargo bench -p krishiv-bench --bench streaming_latency
//!
//! NOTE: `krishiv-bench` is excluded from the default workspace build
//! (per `docs/README.md:50`); this benchmark only compiles when the
//! workspace `cargo bench` invocation is used.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_plan::window::WindowExecutionSpec;

/// `n` rows spaced 1ms apart starting at `ts_base`, cycling through 100
/// distinct keys (`u0`..`u99`). The 1ms stride keeps an `n`-row batch's span
/// under `n` milliseconds, so several batches can tile one tumbling window
/// without overlapping or overrunning it.
fn make_events_batch(n: usize, ts_base: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let user_ids: Vec<String> = (0..n).map(|i| format!("u{}", i % 100)).collect();
    let user_id_refs: Vec<&str> = user_ids.iter().map(|s| s.as_str()).collect();
    let ts_values: Vec<i64> = (0..n as i64).map(|i| ts_base + i).collect();
    let v_values: Vec<i64> = (0..n as i64).map(|i| i).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(user_id_refs)) as ArrayRef,
            Arc::new(Int64Array::from(ts_values)) as ArrayRef,
            Arc::new(Int64Array::from(v_values)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// The window under test is `[0, 10_000)` (tumbling, size 10_000ms). Nine
/// 1k-row, 1000ms-wide slices tile `[0, 9_000)` — each batch's max event time
/// stays below the window's end, so none of them closes it. Drained (untimed)
/// in benchmark setup to warm up per-key state before the timed batch below.
fn warm_up_batches() -> Vec<RecordBatch> {
    (0..9).map(|i| make_events_batch(1_000, i * 1_000)).collect()
}

/// The timed batch: one more 1k-row, 1000ms-wide slice at `[9_000, 10_000)`,
/// still inside the same open window as the nine warm-up batches (max event
/// time 9999 stays below the window's 10_000ms end, so this batch doesn't
/// close it either). All 100 keys already have aggregate state from the
/// warm-up batches, so this is a pure existing-state update — the ordinary,
/// representative per-batch cost in a long-running job.
fn steady_state_batch() -> RecordBatch {
    make_events_batch(1_000, 9_000)
}

/// Embedded placement benchmark: in-process data plane, in-memory state,
/// no fsync. The P99 floor here is the per-batch cost of:
/// 1. draining one source batch
/// 2. looking up each row's window
/// 3. updating the per-key aggregate
/// 4. emitting any closed windows (none, for this batch)
fn bench_embedded_tumbling(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_latency_embedded");
    let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
    group.bench_function("embedded_1k_row_batch_steady_state", |b| {
        b.iter_batched_ref(
            || {
                let mut executor =
                    ContinuousWindowExecutor::new(spec.clone()).expect("create executor");
                for batch in warm_up_batches() {
                    let _ = executor.drain(vec![batch]);
                }
                (executor, steady_state_batch())
            },
            |(executor, batch)| {
                let _ = executor.drain(vec![batch.clone()]);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Single-node placement benchmark: same executor, but file-backed RocksDB
/// state under a fresh temp dir. Measured only microseconds above embedded,
/// not milliseconds — the state backend batches its WAL and defers `sync()`
/// to once per checkpoint epoch (see `operator_runtime.rs`'s
/// `open_state_backend`), so an ordinary drain here pays RocksDB's
/// in-process API overhead but no synchronous disk flush. Checkpoint cost
/// is a separate, unmeasured cost this benchmark's repeated
/// drain-without-checkpoint loop doesn't exercise.
///
/// The executor is constructed and warmed up (nine untimed drains, which
/// also forces the lazy RocksDB-backend open — `ContinuousWindowExecutor`
/// only builds the backend once it sees a first batch) in the untimed setup
/// closure, matching a real continuous job paying that open cost once at
/// start rather than on every batch.
fn bench_single_node_tumbling(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_latency_single_node");
    let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
    group.bench_function("single_node_1k_row_batch_steady_state", |b| {
        b.iter_batched_ref(
            || {
                let ckpt_dir = tempfile::tempdir().expect("tempdir");
                let state_dir = ckpt_dir.path().join("window-state");
                std::fs::create_dir_all(&state_dir).expect("mkdir state dir");
                let mut executor =
                    ContinuousWindowExecutor::new_with_state_dir(spec.clone(), Some(&state_dir))
                        .expect("create executor");
                for batch in warm_up_batches() {
                    let _ = executor.drain(vec![batch]);
                }
                // `ckpt_dir` must outlive the timed closure: the executor
                // holds an open RocksDB handle into it for the whole
                // iteration.
                (ckpt_dir, executor, steady_state_batch())
            },
            |(_ckpt_dir, executor, batch)| {
                let _ = executor.drain(vec![batch.clone()]);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Distributed-path component: the Arrow-IPC shuffle codec cost.
///
/// A distributed placement moves each shuffle partition across the network by
/// serializing the `RecordBatch` to Arrow IPC bytes and reconstructing it on the
/// receiver (`ShuffleService::encode_partition`/`decode_partition`). This is the
/// per-partition serialization cost that sits on the distributed latency path —
/// the embedded/single-node cells above never pay it (they pass `Arc<RecordBatch>`).
/// Measuring the round-trip isolates the network-serialization overhead from the
/// compute, so a regression in the codec shows up independently of a cluster.
fn bench_shuffle_ipc_roundtrip(c: &mut Criterion) {
    use krishiv_engine_core::{decode_batch_ipc, encode_batch_ipc};
    let mut group = c.benchmark_group("streaming_latency_shuffle_ipc");
    let batch = make_events_batch(10_000, 0);
    group.bench_function("ipc_roundtrip_10k_rows", |b| {
        b.iter(|| {
            let bytes = encode_batch_ipc(&batch).expect("encode");
            let decoded = decode_batch_ipc(&bytes).expect("decode");
            assert_eq!(decoded.num_rows(), 10_000);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_embedded_tumbling,
    bench_single_node_tumbling,
    bench_shuffle_ipc_roundtrip
);
criterion_main!(benches);
