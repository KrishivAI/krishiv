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
