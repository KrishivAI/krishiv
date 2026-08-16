//! Phase 65 oversubscription-contract proof harness: every task slot busy at
//! once, with the process-global Rayon compute pool at its default size.
//!
//! # The contract being proved
//!
//! `krishiv_common::compute_pool` adds one process-global Rayon pool for the
//! engine's serial CPU kernels, sized so that under full slot occupancy
//! `slots × DF-partitions × pool-threads` does not blow past the core count
//! (the default reserves one core for the Tokio reactor). The claim that this
//! sizing does not oversubscribe the machine is only a claim until it is
//! measured with every slot occupied — which is exactly the situation this
//! bench constructs:
//!
//! - `KRISHIV_TASK_SLOTS` is pinned to the machine's core count (via
//!   `set_slots_override`, the sanctioned in-process route: `env::set_var` is
//!   `unsafe` under edition 2024 and this workspace forbids unsafe).
//! - `KRISHIV_TARGET_PARALLELISM=1` pins each query's DataFusion
//!   `target_partitions` to its per-slot share (`cores / slots`), so
//!   `slots × DF-partitions = cores` exactly.
//! - N = `available_parallelism()` identical TPC-H q1 queries run
//!   concurrently through one shared `InProcessCluster`, submitted through
//!   the coordinator job path (`collect_batch_sql_via_coordinator`) so the
//!   slot machinery — not the inline fast path — is what gets saturated.
//!
//! The contract number is the **per-query p99 wall time**: with the default
//! compute pool (`max(1, cores-1)` threads) it must not regress against a
//! pool pinned to one thread (`KRISHIV_COMPUTE_THREADS=1`).
//!
//! # Why two separate runs instead of two bench functions
//!
//! Both the compute pool and `ExecutorCapacity` live behind process-global
//! `OnceLock`s, and `KRISHIV_COMPUTE_THREADS` is read once when the pool is
//! first built — a second in-process mode could never observe a different
//! value. So the mode is declared per process via `ALL_SLOTS_BUSY_MODE` and
//! the bench is run twice:
//!
//! ```text
//! export KRISHIV_TPCH_DATA_DIR_SF1=…/tpch-sf1   # tpchgen-cli parquet -s 1
//! export KRISHIV_TARGET_PARALLELISM=1
//!
//! ALL_SLOTS_BUSY_MODE=serial KRISHIV_COMPUTE_THREADS=1 \
//!     cargo bench -p krishiv-bench --bench all_slots_busy
//! ALL_SLOTS_BUSY_MODE=pooled \
//!     cargo bench -p krishiv-bench --bench all_slots_busy
//! ```
//!
//! Each run prints a machine-readable summary on stderr:
//!
//! ```text
//! all_slots_busy mode=<serial|pooled> n=<n> p50=<ms> p95=<ms> p99=<ms>
//! ```
//!
//! Compare the two p99 values; pooled ≤ serial (within noise) is the pass.
//! Missing data or an inconsistent environment skips with a notice rather
//! than failing, so `just bench` still runs on unconfigured machines.

// Benchmark harness: a panic is the failure signal, and clippy.toml's
// `allow-*-in-tests` does not cover bench targets.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]

use criterion::{BenchmarkId, Criterion, SamplingMode};
use krishiv_bench::tpch;
use krishiv_runtime::{BatchSqlTable, InProcessCluster};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Barrier, Mutex};
use std::time::{Duration, Instant};

/// Declares which compute-pool configuration this process was started with;
/// validated against `KRISHIV_COMPUTE_THREADS` so the printed label is honest.
const MODE_ENV: &str = "ALL_SLOTS_BUSY_MODE";

/// Rounds Criterion is asked to measure (its warm-up adds at least one more;
/// every round executed under Criterion contributes N per-query samples).
const ROUNDS: usize = 25;

/// Unrecorded warm rounds before Criterion starts, so cold-cache effects do
/// not masquerade as tail latency in either mode.
const WARM_ROUNDS: usize = 2;

fn cores() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

/// Nearest-rank percentile over an already-sorted slice (`NaN` when empty).
fn percentile(sorted_ms: &[f64], q: f64) -> f64 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let rank = ((sorted_ms.len() as f64) * q).ceil() as usize;
    let idx = rank.min(sorted_ms.len()).max(1).saturating_sub(1);
    sorted_ms.get(idx).copied().unwrap_or(f64::NAN)
}

/// Resolve and validate the declared mode against the actual environment.
///
/// Returns `None` (with a notice) when the environment contradicts the label,
/// because a summary line whose `mode=` field lies is worse than no run.
fn resolve_mode() -> Option<&'static str> {
    let declared = std::env::var(MODE_ENV).unwrap_or_else(|_| String::from("pooled"));
    let compute = std::env::var(krishiv_common::compute_pool::COMPUTE_THREADS_ENV).ok();
    match declared.as_str() {
        "serial" => {
            if compute.as_deref() == Some("1") {
                Some("serial")
            } else {
                eprintln!(
                    "skipping all_slots_busy: {MODE_ENV}=serial requires \
                     KRISHIV_COMPUTE_THREADS=1 in the environment (found {compute:?}); \
                     the pool is built once per process, so the variable must be set \
                     before the process starts"
                );
                None
            }
        }
        "pooled" => {
            if compute.is_none() || compute.as_deref() == Some("0") {
                Some("pooled")
            } else {
                eprintln!(
                    "skipping all_slots_busy: {MODE_ENV}=pooled requires \
                     KRISHIV_COMPUTE_THREADS to be unset (or 0) so the default \
                     auto-sizing is what gets measured (found {compute:?})"
                );
                None
            }
        }
        other => {
            eprintln!("skipping all_slots_busy: unknown {MODE_ENV}={other} (serial|pooled)");
            None
        }
    }
}

/// One all-slots-busy round: N identical queries released together, each on
/// its own OS thread, through the shared cluster's coordinator job path.
/// Returns the round's wall time; per-query wall times (ms) go to `samples`.
fn run_round(
    cluster: &InProcessCluster,
    query: &str,
    registrations: &[BatchSqlTable],
    n: usize,
    samples: Option<&Mutex<Vec<f64>>>,
) -> Duration {
    let barrier = Barrier::new(n);
    let round_start = Instant::now();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..n)
            .map(|_| {
                let cluster = cluster.clone();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let start = Instant::now();
                    let batches = cluster
                        .collect_batch_sql_via_coordinator(query, registrations)
                        .expect("all-slots-busy query failed");
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1e3;
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(rows > 0, "q1 returned no rows; data or plan is broken");
                    elapsed_ms
                })
            })
            .collect();
        let times: Vec<f64> = handles
            .into_iter()
            .map(|h| h.join().expect("query worker panicked"))
            .collect();
        if let Some(samples) = samples {
            samples
                .lock()
                .expect("samples mutex poisoned")
                .extend(times);
        }
    });
    round_start.elapsed()
}

fn bench_all_slots_busy(c: &mut Criterion) {
    let Some(mode) = resolve_mode() else { return };

    if std::env::var("KRISHIV_TARGET_PARALLELISM").as_deref() != Ok("1") {
        eprintln!(
            "skipping all_slots_busy: set KRISHIV_TARGET_PARALLELISM=1 so each of the \
             N concurrent queries plans at its per-slot share (slots = cores here, \
             so the share is one partition); without it every query plans at the \
             full core count and the run measures cores^2-way oversubscription, \
             not the capacity contract"
        );
        return;
    }

    let Ok(dir) = std::env::var("KRISHIV_TPCH_DATA_DIR_SF1") else {
        eprintln!("skipping all_slots_busy: KRISHIV_TPCH_DATA_DIR_SF1 not set");
        return;
    };
    let (name, query, tables) = ("q1", tpch::Q1, tpch::Q1_TABLES);
    if !tpch::tables_exist(&dir, tables) {
        eprintln!("skipping all_slots_busy: missing Parquet for {name} under {dir}");
        return;
    }

    let n = cores();
    let capacity = krishiv_common::ExecutorCapacity::detect_cached();
    eprintln!(
        "all_slots_busy: mode={mode} n={n} compute_threads={} capacity: {}",
        krishiv_common::compute_pool::configured_compute_threads(),
        capacity.summary(),
    );
    assert_eq!(
        capacity.slots.get(),
        n,
        "slot override did not take; capacity was read before main set it"
    );

    // One shared cluster for the whole run — the executor whose slots are
    // being saturated. A fresh cluster per query would measure setup, not
    // occupancy (and would give every query its own slot supply).
    let cluster = InProcessCluster::new().expect("in-process cluster creation");
    let registrations: Vec<BatchSqlTable> = tables
        .iter()
        .map(|table| BatchSqlTable {
            table_name: (*table).to_string(),
            path: PathBuf::from(format!("{dir}/{table}.parquet")),
            ..Default::default()
        })
        .collect();

    // Prime sequentially (table registration, parquet footers, page cache),
    // then a couple of unrecorded concurrent warm rounds.
    cluster
        .collect_batch_sql_via_coordinator(query, &registrations)
        .expect("priming query failed");
    for _ in 0..WARM_ROUNDS {
        run_round(&cluster, query, &registrations, n, None);
    }

    let samples: Mutex<Vec<f64>> = Mutex::new(Vec::new());
    let mut group = c.benchmark_group("all_slots_busy");
    group.sample_size(ROUNDS);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(45));
    group.warm_up_time(Duration::from_secs(1));
    group.bench_function(BenchmarkId::new(mode, format!("{name}_x{n}")), |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_round(&cluster, query, &registrations, n, Some(&samples));
            }
            total
        })
    });
    group.finish();

    let mut times = samples.into_inner().expect("samples mutex poisoned");
    times.sort_by(|a, b| a.partial_cmp(b).expect("wall times are finite"));
    let rounds = times.len() / n;
    let min = times.first().copied().unwrap_or(f64::NAN);
    let max = times.last().copied().unwrap_or(f64::NAN);
    let mean = times.iter().sum::<f64>() / times.len().max(1) as f64;
    let (p50, p95, p99) = (
        percentile(&times, 0.50),
        percentile(&times, 0.95),
        percentile(&times, 0.99),
    );
    eprintln!(
        "all_slots_busy per-query wall time: mode={mode} n={n} rounds={rounds} \
         samples={} min={min:.1}ms mean={mean:.1}ms p50={p50:.1}ms p95={p95:.1}ms \
         p99={p99:.1}ms max={max:.1}ms",
        times.len(),
    );
    // The contract line: compare p99 across the serial and pooled runs.
    eprintln!("all_slots_busy mode={mode} n={n} p50={p50:.1} p95={p95:.1} p99={p99:.1}");
}

fn main() {
    // Pin task-slot capacity to the core count BEFORE anything reads
    // `ExecutorCapacity` — it caches on first read, and the first read
    // happens inside the first cluster/query construction below.
    krishiv_common::executor_capacity::set_slots_override(cores());

    let mut criterion = Criterion::default().configure_from_args();
    bench_all_slots_busy(&mut criterion);
    criterion.final_summary();
}
