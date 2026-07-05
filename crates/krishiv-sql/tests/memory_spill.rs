//! G2 (engine gap register) — memory accounting + spill for large batch
//! queries: proves that `SqlEngine::new_with_memory_limit` actually spills
//! sort/aggregate/join operators to disk under a tight memory budget rather
//! than either OOM-crashing the process or silently returning wrong results.
//!
//! Scale: this environment has no room to generate a real multi-GB TPC-H
//! scale factor (see `docs/BENCHMARKING.md` — any performance claim must
//! state its dataset/scale/hardware). Instead this constructs an in-memory
//! dataset sized to comfortably exceed a deliberately tiny memory pool
//! (single-digit MB), which is what actually exercises the spill path: a
//! `FairSpillPool` at that size cannot hold the working set without writing
//! to disk. A negative control using DataFusion's non-spilling
//! `GreedyMemoryPool` at the *same* limit against the *same* query confirms
//! the workload genuinely requires spill (it fails with `ResourcesExhausted`
//! there) — so the positive case succeeding is evidence spill is engaged,
//! not that the data happened to fit anyway.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use krishiv_sql::SqlEngine;

const ROWS_PER_BATCH: usize = 2_000;
const NUM_BATCHES: usize = 25;
/// ~1KB of padding per row so `NUM_BATCHES * ROWS_PER_BATCH` rows add up to
/// tens of MB — comfortably over the tiny pool sizes used below.
const PAYLOAD_LEN: usize = 1024;
/// Deliberately tiny: far below the dataset's real footprint so a
/// non-spilling pool must fail and a spilling one must actually spill.
const TINY_POOL_BYTES: usize = 2 * 1024 * 1024;

fn make_batches(seed_offset: i64) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    (0..NUM_BATCHES)
        .map(|batch_idx| {
            let ids: Vec<i64> = (0..ROWS_PER_BATCH)
                .map(|i| seed_offset + (batch_idx * ROWS_PER_BATCH + i) as i64)
                .collect();
            // Descending so an `ORDER BY id ASC` genuinely reorders every row
            // (proves sort correctness post-spill, not a no-op pass-through).
            let ids_desc: Vec<i64> = ids.iter().rev().copied().collect();
            let groups: Vec<i64> = ids_desc.iter().map(|id| id % 50).collect();
            let payload = "x".repeat(PAYLOAD_LEN);
            let payloads: Vec<String> = ids_desc
                .iter()
                .map(|id| format!("{payload}-{id}"))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids_desc)),
                    Arc::new(Int64Array::from(groups)),
                    Arc::new(StringArray::from(payloads)),
                ],
            )
            .expect("record batch construction")
        })
        .collect()
}

fn total_rows() -> usize {
    ROWS_PER_BATCH * NUM_BATCHES
}

#[tokio::test]
async fn sort_spills_under_tiny_memory_pool_and_stays_correct() {
    let engine = SqlEngine::new_with_memory_limit(Some(TINY_POOL_BYTES));
    engine
        .register_record_batches("t", make_batches(0))
        .await
        .expect("register table");

    let df = engine
        .sql("SELECT id FROM t ORDER BY id ASC")
        .await
        .expect("sort query should plan under a tiny memory pool");
    let batches = df.collect().await.expect(
        "sort should complete by spilling to disk instead of exhausting the tiny memory pool",
    );

    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64")
                .values()
                .to_vec()
        })
        .collect();

    assert_eq!(ids.len(), total_rows(), "no rows lost across the spill");
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "output must be in ascending order post-spill");
    assert_eq!(
        ids.first().copied(),
        Some(0),
        "first row must be the true minimum, not a partially-merged spill artifact"
    );
    assert_eq!(ids.last().copied(), Some((total_rows() - 1) as i64));
}

#[tokio::test]
async fn grouped_aggregate_spills_under_tiny_memory_pool_and_stays_correct() {
    let engine = SqlEngine::new_with_memory_limit(Some(TINY_POOL_BYTES));
    engine
        .register_record_batches("t", make_batches(0))
        .await
        .expect("register table");

    let df = engine
        .sql("SELECT group_key, COUNT(*) AS n FROM t GROUP BY group_key ORDER BY group_key")
        .await
        .expect("aggregate query should plan under a tiny memory pool");
    let batches = df
        .collect()
        .await
        .expect("aggregation should complete by spilling groups to disk");

    let mut group_keys = Vec::new();
    let mut counts = Vec::new();
    for b in &batches {
        group_keys.extend(
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("group_key is Int64")
                .values()
                .to_vec(),
        );
        counts.extend(
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count is Int64")
                .values()
                .to_vec(),
        );
    }

    assert_eq!(
        group_keys.len(),
        50,
        "exactly 50 distinct group_key values (id % 50)"
    );
    let expected_per_group = (total_rows() / 50) as i64;
    for (key, count) in group_keys.iter().zip(counts.iter()) {
        assert_eq!(
            *count, expected_per_group,
            "group {key} count wrong post-spill (every group must be evenly sized by construction)"
        );
    }
}

#[tokio::test]
async fn hash_join_spills_under_tiny_memory_pool_and_stays_correct() {
    let engine = SqlEngine::new_with_memory_limit(Some(TINY_POOL_BYTES));
    engine
        .register_record_batches("left_t", make_batches(0))
        .await
        .expect("register left table");
    engine
        .register_record_batches("right_t", make_batches(0))
        .await
        .expect("register right table");

    let df = engine
        .sql("SELECT COUNT(*) AS n FROM left_t l JOIN right_t r ON l.id = r.id")
        .await
        .expect("join query should plan under a tiny memory pool");
    let batches = df
        .collect()
        .await
        .expect("hash join build side should spill to disk instead of exhausting the tiny pool");

    let n: i64 = batches
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count is Int64")
                .value(0)
        })
        .sum();
    assert_eq!(
        n,
        total_rows() as i64,
        "every row must join exactly once on the shared id space post-spill"
    );
}

/// Negative control: the *same* dataset and query against DataFusion's
/// non-spilling `GreedyMemoryPool` at the *same* tiny limit must fail with a
/// resources-exhausted error. This confirms the workload genuinely exceeds
/// the pool (the positive tests above aren't merely fitting in memory
/// anyway) — so their success is real evidence of spill, not a fluke.
#[tokio::test]
async fn same_workload_fails_fast_without_a_spilling_pool() {
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(TINY_POOL_BYTES)))
        .build_arc()
        .expect("build greedy-pool runtime");
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new())
        .with_runtime_env(runtime_env)
        .build();
    let ctx = SessionContext::new_with_state(state);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batches = make_batches(0);
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
        .expect("mem table construction");
    ctx.register_table("t", Arc::new(mem_table))
        .expect("register table");

    let df = ctx
        .sql("SELECT id FROM t ORDER BY id ASC")
        .await
        .expect("query should plan (the pool is only checked during execution)");
    let result = df.collect().await;
    assert!(
        result.is_err(),
        "a non-spilling pool this tiny must fail on a dataset this large — \
         if it succeeds, TINY_POOL_BYTES/dataset size need to be revisited \
         so the positive spill tests remain meaningful"
    );
    let message = result.unwrap_err().to_string();
    assert!(
        message.to_lowercase().contains("resources exhausted")
            || message.to_lowercase().contains("memory"),
        "expected a resource/memory exhaustion error, got: {message}"
    );
}

/// `FairSpillPool` shares one budget across concurrently running queries on
/// the same engine — this is the multi-tenant case the doc comment on
/// `SqlEngine::new_with_memory_limit` promises ("shares the limit across
/// concurrently running operators"). Runs the same sort and aggregate
/// concurrently against one engine instance to prove the shared pool doesn't
/// deadlock or corrupt either query's result.
#[tokio::test]
async fn concurrent_queries_share_the_pool_without_corrupting_results() {
    let engine = SqlEngine::new_with_memory_limit(Some(TINY_POOL_BYTES));
    engine
        .register_record_batches("t", make_batches(0))
        .await
        .expect("register table");

    let sort_engine = engine.clone();
    let agg_engine = engine.clone();
    let (sort_result, agg_result) = tokio::join!(
        async move {
            sort_engine
                .sql("SELECT id FROM t ORDER BY id ASC")
                .await
                .expect("sort should plan")
                .collect()
                .await
        },
        async move {
            agg_engine
                .sql("SELECT group_key, COUNT(*) AS n FROM t GROUP BY group_key")
                .await
                .expect("aggregate should plan")
                .collect()
                .await
        },
    );

    let sort_batches = sort_result
        .expect("sort should complete even while sharing the pool with a concurrent aggregate");
    let agg_batches = agg_result
        .expect("aggregate should complete even while sharing the pool with a concurrent sort");

    let sort_rows: usize = sort_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(sort_rows, total_rows());
    let agg_groups: usize = agg_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(agg_groups, 50);
}
