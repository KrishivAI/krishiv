//! Nexmark streaming benchmark gates for Krishiv R10 (P2-7).
//!
//! Q1/Q2/Q5/Q8 executed through DataFusion via `krishiv_sql::SqlEngine`.
//!
//! To run: cargo bench -p krishiv-bench --bench nexmark

use std::sync::Arc;

use arrow::array::{Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use krishiv_sql::SqlEngine;
use std::time::Duration;

fn make_bid_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::UInt64, false),
        Field::new("price", DataType::UInt64, false),
    ]));
    let auction: UInt64Array = (0..n as u64).map(|i| i % 10_000).collect();
    let price: UInt64Array = (0..n as u64).map(|i| 100 + (i % 900)).collect();
    RecordBatch::try_new(schema, vec![Arc::new(auction) as _, Arc::new(price) as _]).unwrap()
}

fn make_auction_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("category", DataType::UInt64, false),
    ]));
    let id: UInt64Array = (0..n as u64).collect();
    let category: UInt64Array = (0..n as u64).map(|i| i % 100).collect();
    RecordBatch::try_new(schema, vec![Arc::new(id) as _, Arc::new(category) as _]).unwrap()
}

fn make_person_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("region", DataType::Int64, false),
    ]));
    let id: UInt64Array = (0..n as u64).collect();
    let region: Int64Array = (0..n as i64).map(|i| i % 10).collect();
    RecordBatch::try_new(schema, vec![Arc::new(id) as _, Arc::new(region) as _]).unwrap()
}

async fn register_nexmark_tables(engine: &SqlEngine, n: usize) {
    engine
        .register_record_batches("bid", vec![make_bid_batch(n)])
        .await
        .expect("register bid");
    engine
        .register_record_batches("auction", vec![make_auction_batch(n)])
        .await
        .expect("register auction");
    engine
        .register_record_batches("person", vec![make_person_batch(n)])
        .await
        .expect("register person");
}

fn bench_nexmark_sql(c: &mut Criterion, name: &str, query: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let n = 100_000usize;
    let mut group = c.benchmark_group("nexmark_sql");
    group.measurement_time(Duration::from_secs(10));
    group.throughput(criterion::Throughput::Elements(n as u64));

    group.bench_function(name, |b| {
        b.iter_batched(
            || {
                let engine = SqlEngine::new();
                rt.block_on(register_nexmark_tables(&engine, n));
                engine
            },
            |engine| {
                rt.block_on(async {
                    let df = engine.sql(query).await.expect("sql");
                    let batches = df.collect().await.expect("collect");
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(rows > 0, "expected output rows");
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_nexmark_q1(c: &mut Criterion) {
    bench_nexmark_sql(
        c,
        "q1_currency_conversion_100k",
        "SELECT auction, CAST(price AS DOUBLE) * 0.908 AS dollar FROM bid",
    );
}

fn bench_nexmark_q2(c: &mut Criterion) {
    bench_nexmark_sql(
        c,
        "q2_auction_filter_100k",
        "SELECT auction, price FROM bid WHERE auction % 123 = 0",
    );
}

fn bench_nexmark_q5(c: &mut Criterion) {
    bench_nexmark_sql(
        c,
        "q5_auction_category_100k",
        "SELECT a.id, a.category FROM auction a JOIN bid b ON a.id = b.auction",
    );
}

fn bench_nexmark_q8(c: &mut Criterion) {
    bench_nexmark_sql(
        c,
        "q8_person_region_100k",
        "SELECT p.id, p.region FROM person p WHERE p.region = 3",
    );
}

criterion_group!(
    nexmark_benches,
    bench_nexmark_q1,
    bench_nexmark_q2,
    bench_nexmark_q5,
    bench_nexmark_q8
);
criterion_main!(nexmark_benches);
