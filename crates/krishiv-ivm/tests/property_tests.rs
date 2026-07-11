//! Property-based IVM correctness tests: verify that incremental output
//! matches batch recompute for the same cumulative input.
//!
//! These tests address the Feldera comparison gap: DBSP has formal proofs
//! that incremental = batch. Krishiv here validates the same invariant
//! empirically.

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;
use std::sync::Arc;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn float_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    let s = schema();
    let keys: Vec<String> = rows.iter().map(|(k, _)| format!("k{k}")).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    RecordBatch::try_new(
        s,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn revenue_batch(rows: &[(i64, f64)]) -> RecordBatch {
    let s = float_schema();
    let regions: Vec<String> = rows
        .iter()
        .map(|(r, _)| format!("region-{}", r % 10))
        .collect();
    let amounts: Vec<f64> = rows.iter().map(|(_, a)| *a).collect();
    RecordBatch::try_new(
        s,
        vec![
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap()
}

async fn batch_query(sql: &str, table: &str, batch: &RecordBatch) -> Vec<RecordBatch> {
    let ctx = datafusion::prelude::SessionContext::new();
    let mem = datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
        .unwrap();
    ctx.register_table(table, Arc::new(mem)).unwrap();
    let df = ctx.sql(sql).await.unwrap();
    df.collect().await.unwrap()
}

fn revenue_view_flow() -> IncrementalFlow {
    let flow = IncrementalFlow::new();
    let spec = IncrementalViewSpec {
        name: "revenue".into(),
        body_sql: "SELECT region, SUM(amount) AS total FROM sales GROUP BY region".into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("total", DataType::Float64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    };
    flow.register_view(spec).unwrap();
    flow
}

#[tokio::test]
async fn group_by_sum_incremental_matches_batch() {
    let flow = revenue_view_flow();
    let mut accumulated: Vec<(i64, f64)> = Vec::new();

    for round in 0..10 {
        let offset = round * 100;
        let rows: Vec<(i64, f64)> = (0..100)
            .map(|i| (i % 10, ((i + offset) % 997) as f64))
            .collect();
        accumulated.extend_from_slice(&rows);
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(revenue_batch(&rows)).unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();

        let full = revenue_batch(&accumulated);
        let batch_result = batch_query(
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
            "sales",
            &full,
        )
        .await;
        let snap = flow.snapshot("revenue").unwrap().unwrap();
        let batch_rows: usize = batch_result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            snap.num_rows(),
            batch_rows,
            "round {round}: incremental={} vs batch={}",
            snap.num_rows(),
            batch_rows
        );
    }
}

#[tokio::test]
async fn ivm_with_updates_matches_batch() {
    let flow = IncrementalFlow::new();
    let spec = IncrementalViewSpec {
        name: "total".into(),
        body_sql: "SELECT SUM(value) AS total FROM src".into(),
        output_schema: Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    };
    flow.register_view(spec).unwrap();

    // Insert 10 rows.
    let rows: Vec<(i64, i64)> = (0..10).map(|i| (i, i * 10)).collect();
    flow.feed("src", DeltaBatch::from_inserts(batch(&rows)).unwrap())
        .unwrap();
    flow.step_datafusion().await.unwrap();

    let snap = flow.snapshot("total").unwrap().unwrap();
    let total = snap
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 450);

    // Update k0: value 0 → 100.
    let delta = DeltaBatch::from_update(&batch(&[(0, 0)]), &batch(&[(0, 100)])).unwrap();
    flow.feed("src", delta).unwrap();
    flow.step_datafusion().await.unwrap();

    let snap = flow.snapshot("total").unwrap().unwrap();
    let total = snap
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 550);

    // Batch recompute verification.
    let all: Vec<(i64, i64)> = vec![(0, 100)]
        .into_iter()
        .chain((1..10).map(|i| (i, i * 10)))
        .collect();
    let full = batch(&all);
    let batch_result = batch_query("SELECT SUM(value) AS total FROM src", "src", &full).await;
    let batch_total = batch_result[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, batch_total, "IVM with updates must match batch");
}

#[tokio::test]
async fn consistent_across_many_small_feeds() {
    let flow = revenue_view_flow();
    let mut total: Vec<(i64, f64)> = Vec::new();

    for idx in 1usize..=5000 {
        let row = vec![((idx % 10) as i64, ((idx % 97) + 1) as f64)];
        total.extend_from_slice(&row);
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(revenue_batch(&row)).unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();

        if idx % 500 == 0 {
            let full = revenue_batch(&total);
            let batch_result = batch_query(
                "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
                "sales",
                &full,
            )
            .await;
            let snap = flow.snapshot("revenue").unwrap().unwrap();
            let batch_rows: usize = batch_result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                snap.num_rows(),
                batch_rows,
                "after {idx} feeds: ivm={} vs batch={}",
                snap.num_rows(),
                batch_rows
            );
        }
    }
}

fn f64_total(snap: &RecordBatch, col: &str) -> f64 {
    snap.column_by_name(col)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

/// AUD-1 regression: a WHERE clause on a single-source aggregate must be
/// honored by the O(Δ) plan. Before the fix, `source_of_plan` peeled the
/// `Filter` node and fed the *raw unfiltered* delta to the aggregate, so this
/// returned 680 (sum of everything) instead of 600 (sum of rows > 100).
#[tokio::test]
async fn where_clause_is_honored_by_incremental_aggregate() {
    let flow = IncrementalFlow::new();
    let spec = IncrementalViewSpec {
        name: "total".into(),
        body_sql: "SELECT SUM(amount) AS total FROM sales WHERE amount > 100".into(),
        output_schema: Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Float64,
            true,
        )])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    };
    flow.register_view(spec).unwrap();

    // Tick 1: [50, 200] → only 200 passes the filter.
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(revenue_batch(&[(0, 50.0), (0, 200.0)])).unwrap(),
    )
    .unwrap();
    let s1 = flow.step_datafusion().await.unwrap();
    assert!(
        !s1.degraded_views.contains(&"total".to_string()),
        "filtered aggregate must stay on the O(Δ) path, not degrade to DiffBased"
    );
    assert_eq!(
        f64_total(&flow.snapshot("total").unwrap().unwrap(), "total"),
        200.0
    );

    // Tick 2: [30, 400] → only 400 passes → cumulative 600 (NOT 680).
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(revenue_batch(&[(0, 30.0), (0, 400.0)])).unwrap(),
    )
    .unwrap();
    let s2 = flow.step_datafusion().await.unwrap();
    assert!(!s2.degraded_views.contains(&"total".to_string()));
    assert_eq!(
        f64_total(&flow.snapshot("total").unwrap().unwrap(), "total"),
        600.0,
        "WHERE amount > 100 must exclude 50 and 30"
    );
}

/// AUD-1: a filtered GROUP BY must match batch recompute of the same query,
/// tick by tick, while staying incremental.
#[tokio::test]
async fn filtered_group_by_matches_batch() {
    let flow = IncrementalFlow::new();
    let spec = IncrementalViewSpec {
        name: "revenue".into(),
        body_sql:
            "SELECT region, SUM(amount) AS total FROM sales WHERE amount > 100 GROUP BY region"
                .into(),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("total", DataType::Float64, true),
        ])),
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    };
    flow.register_view(spec).unwrap();

    let mut accumulated: Vec<(i64, f64)> = Vec::new();
    for round in 0..8 {
        let offset = round * 50;
        // Spread amounts across 30..=279 so every round has some rows on each
        // side of the WHERE amount > 100 threshold (mix of kept and dropped).
        let rows: Vec<(i64, f64)> = (0..50)
            .map(|i| (i % 5, (((i * 7 + offset) % 250) + 30) as f64))
            .collect();
        accumulated.extend_from_slice(&rows);
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(revenue_batch(&rows)).unwrap(),
        )
        .unwrap();
        let summary = flow.step_datafusion().await.unwrap();
        assert!(
            !summary.degraded_views.contains(&"revenue".to_string()),
            "round {round}: filtered GROUP BY degraded to DiffBased"
        );

        let full = revenue_batch(&accumulated);
        let batch_result = batch_query(
            "SELECT region, SUM(amount) AS total FROM sales WHERE amount > 100 GROUP BY region",
            "sales",
            &full,
        )
        .await;
        // An empty filtered view legitimately snapshots to None → 0 rows.
        let snap = flow
            .snapshot("revenue")
            .unwrap()
            .unwrap_or_else(|| RecordBatch::new_empty(revenue_batch(&[]).schema()));
        let batch_rows: usize = batch_result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            snap.num_rows(),
            batch_rows,
            "round {round}: filtered ivm={} vs batch={}",
            snap.num_rows(),
            batch_rows
        );

        // Sum of totals must match too (row count alone is weak).
        let ivm_sum: f64 = (0..snap.num_rows())
            .map(|r| {
                snap.column_by_name("total")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(r)
            })
            .sum();
        let batch_sum: f64 = batch_result
            .iter()
            .flat_map(|b| {
                let c = b
                    .column_by_name("total")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..b.num_rows()).map(|r| c.value(r)).collect::<Vec<_>>()
            })
            .sum();
        assert!(
            (ivm_sum - batch_sum).abs() < 1e-6,
            "round {round}: filtered total sum ivm={ivm_sum} vs batch={batch_sum}"
        );
    }
}

/// #94: per-view insert/retract counters accumulate across ticks and track
/// the multiset weights (a changed aggregate group = retract(old) +
/// insert(new)), and the tick summary carries the same totals.
#[tokio::test]
async fn view_delta_stats_count_inserts_and_retracts() {
    let flow = revenue_view_flow();

    // No output yet -> no stats.
    assert_eq!(flow.view_delta_stats("revenue").unwrap(), None);

    // Tick 1: fresh groups appear - inserts only.
    let rows: Vec<(i64, f64)> = (0..10).map(|i| (i % 2, i as f64)).collect();
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(revenue_batch(&rows)).unwrap(),
    )
    .unwrap();
    let s1 = flow.step_datafusion().await.unwrap();
    let stats1 = flow.view_delta_stats("revenue").unwrap().unwrap();
    assert!(stats1.rows_inserted_total > 0, "aggregate rows inserted");
    assert_eq!(stats1.rows_retracted_total, 0, "no groups changed yet");
    assert_eq!(s1.total_inserted_rows, stats1.rows_inserted_total);
    assert_eq!(s1.total_retracted_rows, 0);

    // Tick 2: both groups change value - each emits retract(old) + insert(new).
    flow.feed(
        "sales",
        DeltaBatch::from_inserts(revenue_batch(&[(0, 100.0), (1, 100.0)])).unwrap(),
    )
    .unwrap();
    let s2 = flow.step_datafusion().await.unwrap();
    let stats2 = flow.view_delta_stats("revenue").unwrap().unwrap();
    assert!(
        stats2.rows_retracted_total > 0,
        "changed groups must retract their old aggregate rows"
    );
    assert!(stats2.rows_inserted_total > stats1.rows_inserted_total);
    assert_eq!(stats2.last_tick_inserts, s2.total_inserted_rows);
    assert_eq!(stats2.last_tick_retracts, s2.total_retracted_rows);
    // Cumulative = tick1 + tick2.
    assert_eq!(
        stats2.rows_inserted_total,
        stats1.rows_inserted_total + stats2.last_tick_inserts
    );
}
