//! A decomposed query must answer exactly what the whole query answers.
//!
//! `krishiv_ivm::decompose` cuts a linear multi-operator query into hops so
//! each one gets an O(delta) plan. That is only worth anything if the chain
//! computes the same relation the original query does, so every case here runs
//! the ORIGINAL SQL through `force_diff_based` — full recompute, the trusted
//! answer by construction — and compares.
//!
//! The comparison is textual (`ArrayFormatter`), not numeric, so a hop that
//! widens a decimal's scale differently from DataFusion is a visible
//! difference rather than an equal-looking one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::panic
)]

use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::prelude::SessionContext;
use krishiv_bench::tpch_fixture::fixture_ddl;
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::{IncrementalFlow, decompose};

async fn fixture() -> SessionContext {
    let ctx = SessionContext::new();
    for ddl in fixture_ddl() {
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    }
    ctx
}

fn canonical(batch: &RecordBatch) -> Vec<Vec<String>> {
    let opts = FormatOptions::default();
    let fmts: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c, &opts).unwrap())
        .collect();
    let mut rows: Vec<Vec<String>> = (0..batch.num_rows())
        .map(|r| fmts.iter().map(|f| f.value(r).to_string()).collect())
        .collect();
    rows.sort();
    rows
}

fn spec(name: &str, sql: &str, out: SchemaRef) -> IncrementalViewSpec {
    IncrementalViewSpec {
        name: name.into(),
        body_sql: sql.into(),
        output_schema: out,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    }
}

/// Decompose `sql`, run the chain, and compare against recomputing `sql` whole.
async fn decomposed_matches_recompute(label: &str, table: &str, sql: &str) {
    let ctx = fixture().await;
    let rows: Vec<RecordBatch> = ctx
        .sql(&format!("SELECT * FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let src_schema: SchemaRef =
        Arc::new(ctx.table(table).await.unwrap().schema().as_arrow().clone());
    let declared: SchemaRef = Arc::new(ctx.sql(sql).await.unwrap().schema().as_arrow().clone());

    let mut schemas: AHashMap<String, SchemaRef> = AHashMap::new();
    schemas.insert(table.to_string(), src_schema);

    let hops = decompose("v", sql, &declared, &schemas)
        .await
        .unwrap_or_else(|| panic!("{label}: refused to decompose"));
    println!("\n== {label} == {} hops", hops.len());
    for h in &hops {
        println!(
            "   {:<16} {}",
            h.name,
            &h.body_sql[..h.body_sql.len().min(120)]
        );
    }
    assert!(hops.len() >= 2, "{label}: a chain needs at least two hops");
    assert_eq!(
        hops.last().unwrap().name,
        "v",
        "the last hop keeps the view's name"
    );

    // Subject: the decomposed chain.
    let subject = IncrementalFlow::new();
    for h in &hops {
        subject
            .register_view(spec(&h.name, &h.body_sql, h.schema.clone()))
            .unwrap();
    }
    // Oracle: the ORIGINAL query, recomputed whole.
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec("v", sql, declared)).unwrap();
    oracle.force_diff_based().unwrap();

    for batch in rows.iter() {
        let d = DeltaBatch::from_inserts(batch.clone()).unwrap();
        subject.feed(table, d.clone()).unwrap();
        oracle.feed(table, d).unwrap();
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(
            s.errored_views.is_empty(),
            "{label}: hop errored: {:?}",
            s.errored_views
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
        );
    }

    // Every hop must be maintaining incrementally — the whole point.
    for h in &hops {
        let (inc, why) = subject
            .view_plan_classification(&h.name)
            .unwrap()
            .expect("registered");
        assert!(inc, "{label}: hop {} fell back: {why}", h.name);
    }

    for h in &hops {
        let snap = subject.snapshot(&h.name).unwrap();
        println!(
            "   snapshot {:<16} rows={:?}",
            h.name,
            snap.map(|b| b.num_rows())
        );
    }
    let got = subject.snapshot("v").unwrap().expect("chain published");
    let want = oracle.snapshot("v").unwrap().expect("oracle published");
    println!(
        "   rows: chain={} recompute={}",
        got.num_rows(),
        want.num_rows()
    );
    assert_eq!(
        canonical(&got),
        canonical(&want),
        "{label}: the decomposed chain disagreed with recomputing the whole query"
    );
}

/// The corpus SQL, not a paraphrase: the claim is that the benchmark's own
/// queries decompose, so the test must read them from where the benchmark
/// does. On the small fixture, q6's filter admits **zero** rows, which is the
/// point — the chain must still answer the one row SQL owes (a NULL sum).
fn corpus_sql(id: &str) -> String {
    TPCH_QUERIES
        .iter()
        .find(|q| q.id == id)
        .unwrap_or_else(|| panic!("{id} not in corpus"))
        .sql_at_scale(1.0)
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_q6_decomposes_and_agrees() {
    decomposed_matches_recompute("q6", "lineitem", &corpus_sql("q6")).await;
}

/// q1 carries the pair `sum(a * (1-d))` and `sum(a * (1-d) * (1+t))` — one
/// hoisted expression a subtree of another — plus three decimal AVGs and an
/// ORDER BY that must ride the final hop. It goes through only if the hoist
/// substitutes whole expressions top-down.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q1_decomposes_and_agrees() {
    decomposed_matches_recompute("q1", "lineitem", &corpus_sql("q1")).await;
}

/// A join is a DAG, not a chain: refused wholesale rather than half-cut.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_is_refused_rather_than_partly_cut() {
    let ctx = fixture().await;
    let mut schemas: AHashMap<String, SchemaRef> = AHashMap::new();
    for t in ["lineitem", "orders"] {
        schemas.insert(
            t.to_string(),
            Arc::new(ctx.table(t).await.unwrap().schema().as_arrow().clone()),
        );
    }
    let sql = "SELECT o_orderkey, sum(l_quantity) AS q FROM orders \
               JOIN lineitem ON o_orderkey = l_orderkey GROUP BY o_orderkey";
    let declared: SchemaRef = Arc::new(ctx.sql(sql).await.unwrap().schema().as_arrow().clone());
    assert!(
        decompose("v", sql, &declared, &schemas).await.is_none(),
        "a partially-cut join is slower than an uncut one; it must be refused"
    );
}

/// The wiring test (DECOMP-2): register the corpus SQL **verbatim** — no
/// library call — and the ENGINE must cut the chain itself at plan time.
/// Asserting the plan description names the chain pins that the values come
/// from the O(Δ) fold, not from a DiffBased fallback agreeing trivially.
async fn verbatim_matches_recompute(label: &str, table: &str, sql: &str) {
    let ctx = fixture().await;
    let rows: Vec<RecordBatch> = ctx
        .sql(&format!("SELECT * FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let declared: SchemaRef = Arc::new(ctx.sql(sql).await.unwrap().schema().as_arrow().clone());

    let subject = IncrementalFlow::new();
    subject
        .register_view(spec("v", sql, declared.clone()))
        .unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec("v", sql, declared)).unwrap();
    oracle.force_diff_based().unwrap();

    for batch in rows.iter() {
        let d = DeltaBatch::from_inserts(batch.clone()).unwrap();
        subject.feed(table, d.clone()).unwrap();
        oracle.feed(table, d).unwrap();
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(
            s.errored_views.is_empty(),
            "{label}: view errored: {:?}",
            s.errored_views
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
        );
    }

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "{label}: fell back to DiffBased: {why}");
    assert!(
        why.contains("chain"),
        "{label}: incremental but not via the chain: {why}"
    );

    let got = subject.snapshot("v").unwrap().expect("subject published");
    let want = oracle.snapshot("v").unwrap().expect("oracle published");
    assert_eq!(
        canonical(&got),
        canonical(&want),
        "{label}: the engine-built chain disagreed with recomputing the whole query"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_q6_registered_verbatim_maintains_incrementally() {
    verbatim_matches_recompute("q6", "lineitem", &corpus_sql("q6")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_q1_registered_verbatim_maintains_incrementally() {
    verbatim_matches_recompute("q1", "lineitem", &corpus_sql("q1")).await;
}

/// DECOMP-4: a chain whose LEAF is a two-source join — `aggregate over
/// (A ⋈ B)`, the TPC-H comma-join idiom. Tables feed on separate ticks so the
/// join's trace must hold the first table's rows to meet the second's.
async fn verbatim_join_matches_recompute(label: &str, tables: &[&str], sql: &str) {
    let ctx = fixture().await;
    let declared: SchemaRef = Arc::new(ctx.sql(sql).await.unwrap().schema().as_arrow().clone());

    let subject = IncrementalFlow::new();
    subject
        .register_view(spec("v", sql, declared.clone()))
        .unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec("v", sql, declared)).unwrap();
    oracle.force_diff_based().unwrap();

    // Tick 1: every table's rows — except the FIRST table, which holds back
    // half of its rows for tick 2. The held-back half must meet the other
    // table's rows in the join's trace, so maintenance (not just first-build)
    // is exercised across the tick boundary.
    let mut held_back: Option<(&str, RecordBatch)> = None;
    for (i, table) in tables.iter().enumerate() {
        let batches: Vec<RecordBatch> = ctx
            .sql(&format!("SELECT * FROM {table}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for (bi, batch) in batches.iter().enumerate() {
            let batch = if i == 0 && bi == 0 && batch.num_rows() >= 2 {
                let half = batch.num_rows() / 2;
                held_back = Some((table, batch.slice(half, batch.num_rows() - half)));
                batch.slice(0, half)
            } else {
                batch.clone()
            };
            let d = DeltaBatch::from_inserts(batch).unwrap();
            subject.feed(*table, d.clone()).unwrap();
            oracle.feed(*table, d).unwrap();
        }
    }
    let s = subject.step_datafusion().await.unwrap();
    oracle.step_datafusion().await.unwrap();
    assert!(
        s.errored_views.is_empty(),
        "{label}: view errored on tick 1: {:?}",
        s.errored_views
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
    if let Some((table, rest)) = held_back {
        let d = DeltaBatch::from_inserts(rest).unwrap();
        subject.feed(table, d.clone()).unwrap();
        oracle.feed(table, d).unwrap();
        let s = subject.step_datafusion().await.unwrap();
        oracle.step_datafusion().await.unwrap();
        assert!(
            s.errored_views.is_empty(),
            "{label}: view errored on tick 2: {:?}",
            s.errored_views
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
        );
    }

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "{label}: fell back to DiffBased: {why}");
    assert!(why.contains("chain"), "{label}: not via the chain: {why}");

    let got = subject.snapshot("v").unwrap().expect("subject published");
    let want = oracle.snapshot("v").unwrap().expect("oracle published");
    assert_eq!(
        canonical(&got),
        canonical(&want),
        "{label}: the join-leaf chain disagreed with recomputing the whole query"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_q12_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q12", &["orders", "lineitem"], &corpus_sql("q12")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_q14_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q14", &["lineitem", "part"], &corpus_sql("q14")).await;
}

/// MJOIN-1: a LEFT-DEEP multi-way comma join — six tables, the WHERE's equi
/// conjuncts distributed across five join levels, an aggregate with a
/// computed argument on top, and an ORDER BY riding the final hop.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q5_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute(
        "q5",
        &[
            "customer", "orders", "lineitem", "supplier", "nation", "region",
        ],
        &corpus_sql("q5"),
    )
    .await;
}
