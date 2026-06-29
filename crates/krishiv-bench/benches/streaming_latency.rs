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
//! To run: cargo bench -p krishiv-bench --bench streaming_latency
//!
//! NOTE: `krishiv-bench` is excluded from the default workspace build
//! (per `docs/README.md:50`); this benchmark only compiles when the
//! workspace `cargo bench` invocation is used.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use krishiv_api::{
    CompiledJob, EngineKind, EngineRuntime, Placement, Session, SinkSpec, SourceSpec,
    connector_runtime::{durable_engine_runtime, embedded_connector_runtime},
    run_job,
};
use krishiv_engine_core::mem::{InMemorySinkProvider, InMemorySourceProvider};

fn make_events_batch(n: usize, ts_base: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let user_ids: Vec<String> = (0..n).map(|i| format!("u{}", i % 100)).collect();
    let user_id_refs: Vec<&str> = user_ids.iter().map(|s| s.as_str()).collect();
    let ts_values: Vec<i64> = (0..n as i64).map(|i| ts_base + i * 10).collect();
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

/// Embedded placement benchmark: in-process data plane, in-memory state,
/// no fsync. The P99 floor here is the per-batch cost of:
/// 1. draining one source batch
/// 2. looking up the row's window
/// 3. updating the per-key aggregate
/// 4. emitting closed windows to the in-memory sink
///
/// Each iteration: 10 batches of 1k rows each (10k events), then a single
/// boundary-closing batch at event-time past the first window's end so the
/// drain emits a closed window.
fn bench_embedded_tumbling(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_latency_embedded");
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    group.bench_function("embedded_10k_rows_10k_window", |b| {
        b.iter_batched_ref(
            || {
                let sources = InMemorySourceProvider::new();
                let mut batches = Vec::new();
                for i in 0..10 {
                    batches.push(make_events_batch(1_000, i as i64 * 100_000));
                }
                // Boundary-closing batch: event-time jumps past the
                // 10_000 ms window end.
                batches.push(make_events_batch(1, 1_000_000));
                sources.insert("t", batches);
                let collected = InMemorySinkProvider::new();
                (sources, collected)
            },
            |(sources, collected)| {
                let runtime = embedded_connector_runtime();
                let query = "SELECT user_id, SUM(v) AS total \
                             FROM TUMBLE(TABLE t, DESCRIPTOR(ts), 10000) \
                             GROUP BY user_id, window_start, window_end";
                let job = CompiledJob::new(
                    "bench-stream-lat",
                    query,
                    vec![SourceSpec::unbounded("t", "memory", "")],
                    vec![SinkSpec::new("out", "memory", "")],
                    true,
                )
                .with_engine(EngineKind::Streaming);
                rt.block_on(async {
                    let _ = run_job(job, runtime).await;
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Single-node placement benchmark: same code path as embedded, but
/// `durable_engine_runtime` selects `Placement::SingleNode` and wires in
/// a file-backed RocksDB state. The P99 floor here should be a few
/// milliseconds higher than embedded (file I/O on the state backend).
fn bench_single_node_tumbling(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_latency_single_node");
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    group.bench_function("single_node_10k_rows_10k_window", |b| {
        b.iter_batched_ref(
            || {
                let sources = InMemorySourceProvider::new();
                let mut batches = Vec::new();
                for i in 0..10 {
                    batches.push(make_events_batch(1_000, i as i64 * 100_000));
                }
                batches.push(make_events_batch(1, 1_000_000));
                sources.insert("t", batches);
                let collected = InMemorySinkProvider::new();
                let ckpt_dir = tempfile::tempdir().expect("tempdir");
                (sources, collected, ckpt_dir)
            },
            |(sources, collected, ckpt_dir)| {
                let runtime = rt.block_on(async {
                    durable_engine_runtime(Placement::SingleNode, ckpt_dir.path(), false)
                        .expect("durable runtime")
                });
                let query = "SELECT user_id, SUM(v) AS total \
                             FROM TUMBLE(TABLE t, DESCRIPTOR(ts), 10000) \
                             GROUP BY user_id, window_start, window_end";
                let job = CompiledJob::new(
                    "bench-stream-lat-sn",
                    query,
                    vec![SourceSpec::unbounded("t", "memory", "")],
                    vec![SinkSpec::new("out", "memory", "")],
                    true,
                )
                .with_engine(EngineKind::Streaming);
                // Manually wire the sources/sinks into the runtime, since
                // `durable_engine_runtime` is a thin wrapper around
                // `EngineRuntime` and the test fixtures are passed separately.
                // For benchmark purposes we replace the runtime's source/sink
                // providers with the test ones.
                let runtime = EngineRuntime {
                    sources: Arc::new(sources.clone()),
                    sinks: Arc::new(collected.clone()),
                    ..runtime
                };
                rt.block_on(async {
                    let _ = run_job(job, runtime).await;
                });
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
