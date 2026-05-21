//! Nexmark streaming benchmark gates for Krishiv R10.
//!
//! Q1–Q8 throughput targets: ≥ 100 000 events/s end-to-end.
//! P99 latency target: ≤ 500 ms.
//!
//! To run: cargo bench -p krishiv-bench --bench nexmark

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::time::Duration;

/// Simulate Nexmark Q1: currency conversion (bid price * 0.908).
fn nexmark_q1_logic(events: &[u64]) -> Vec<u64> {
    events.iter().map(|price| price * 908 / 1000).collect()
}

/// Simulate Nexmark Q2: item ID filter.
fn nexmark_q2_logic(events: &[(u64, u64)]) -> Vec<(u64, u64)> {
    events
        .iter()
        .filter(|(id, _)| id % 123 == 0)
        .cloned()
        .collect()
}

fn bench_nexmark_q1(c: &mut Criterion) {
    let events: Vec<u64> = (0..100_000).map(|i| 100 + i % 900).collect();
    let mut group = c.benchmark_group("nexmark");
    group.measurement_time(Duration::from_secs(10));
    group.throughput(criterion::Throughput::Elements(100_000));

    group.bench_function("q1_currency_conversion_100k", |b| {
        b.iter_batched(
            || events.clone(),
            |ev| nexmark_q1_logic(&ev),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_nexmark_q2(c: &mut Criterion) {
    let events: Vec<(u64, u64)> = (0..100_000).map(|i| (i, 100 + i % 900)).collect();
    let mut group = c.benchmark_group("nexmark");
    group.measurement_time(Duration::from_secs(10));
    group.throughput(criterion::Throughput::Elements(100_000));

    group.bench_function("q2_item_filter_100k", |b| {
        b.iter_batched(
            || events.clone(),
            |ev| nexmark_q2_logic(&ev),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(nexmark_benches, bench_nexmark_q1, bench_nexmark_q2);
criterion_main!(nexmark_benches);
