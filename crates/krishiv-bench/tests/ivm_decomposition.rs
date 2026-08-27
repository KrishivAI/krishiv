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
        let sql = h.body_sql.as_deref().unwrap_or("<no SQL rendering>");
        println!("   {:<16} {}", h.name, &sql[..sql.len().min(120)]);
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
            .register_view(spec(
                &h.name,
                h.body_sql
                    .as_deref()
                    .expect("this corpus decomposes into hops standard SQL can render"),
                h.schema.clone(),
            ))
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

/// The refusal rule is WHOLESALE-OR-NOTHING, and both sides of it are pinned.
/// A joined aggregate — this file's original always-refused fixture — now
/// cuts COMPLETELY (join hop, then aggregate; every hop verified Incremental
/// by the decomposer's own gate), which honours the rule's intent: it forbade
/// PARTIAL cuts, not successful ones. A join with no equality at all still
/// refuses outright — there is nothing to key a trace on, and a half-cut
/// chain would be slower than an uncut query.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_cuts_wholesale_or_not_at_all() {
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
    let hops = decompose("v", sql, &declared, &schemas)
        .await
        .expect("a joined aggregate cuts completely since MJOIN-1");
    assert!(hops.len() >= 2, "join hop plus aggregate at minimum");

    // No equality anywhere: nothing keys a trace; refused, not half-cut.
    let non_equi = "SELECT o_orderkey, sum(l_quantity) AS q FROM orders \
                    JOIN lineitem ON o_orderkey < l_orderkey GROUP BY o_orderkey";
    let declared: SchemaRef =
        Arc::new(ctx.sql(non_equi).await.unwrap().schema().as_arrow().clone());
    assert!(
        decompose("v", non_equi, &declared, &schemas)
            .await
            .is_none(),
        "a join with no equi key must be refused wholesale"
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

/// TOPN-2: `ORDER BY … LIMIT k` above a joined aggregate — the Sort+Limit
/// becomes its own top-N hop over the projection hop, where the aggregate's
/// rename (`sum(…) AS revenue`) is already a plain column.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q3_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q3", &["customer", "orders", "lineitem"], &corpus_sql("q3"))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_q10_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute(
        "q10",
        &["customer", "orders", "lineitem", "nation"],
        &corpus_sql("q10"),
    )
    .await;
}

/// REORDER-1: q9's FROM lists `part, supplier` first, but they meet only
/// THROUGH `lineitem` — the naive left-deep order puts a keyless cross join
/// at the leaf. The join graph relinearizes to a connected order, every level
/// gets its equi key, and the chain includes an EXTRACT(YEAR …) computed
/// group key riding the derived-table map hop.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q9_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute(
        "q9",
        &[
            "part", "supplier", "lineitem", "partsupp", "orders", "nation",
        ],
        &corpus_sql("q9"),
    )
    .await;
}

/// SEMI-2: q4 is `COUNT(*)` grouped above a correlated EXISTS — the
/// decorrelated plan's leaf is a LeftSemi join whose membership side is a
/// filtered projection of `lineitem`, admitted by the decomposer's guard
/// through the same peeling the join builder uses (SEMI-1) and verified on
/// the re-rooted plan itself (PLANHOP-1).
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q4_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q4", &["orders", "lineitem"], &corpus_sql("q4")).await;
}

/// SEMI-2: q16's chain carries a MID-CHAIN LeftAnti join (`NOT IN` over
/// suppliers with complaints) above the partsupp ⋈ part leaf, with a
/// COUNT(DISTINCT) aggregate on top — a membership level whose right side
/// resolves through the same peeling the join builder uses (SEMI-1).
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q16_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q16", &["partsupp", "part", "supplier"], &corpus_sql("q16"))
        .await;
}

/// SIDE-1: q18's membership side is an AGGREGATE — `o_orderkey IN (SELECT
/// l_orderkey … GROUP BY … HAVING sum(l_quantity) > 300)` — maintained as the
/// chain's side fold. The fixture's orders all sum under 300, so the honest
/// proof DRIVES one across the threshold: tick 2 feeds the heaviest order's
/// lineitem rows twenty more times (Z-set weights multiply its sum by 21),
/// which must cross it INTO the HAVING set and surface it through the
/// customer ⋈ orders ⋈ lineitem spine; tick 3 retracts the copies and it
/// must leave again. Every tick is compared against full recompute, and the
/// crossed tick is asserted non-empty so agreement is never vacuous.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q18_registered_verbatim_maintains_incrementally() {
    let ctx = fixture().await;
    let sql = corpus_sql("q18");
    let declared: SchemaRef = Arc::new(ctx.sql(&sql).await.unwrap().schema().as_arrow().clone());

    let subject = IncrementalFlow::new();
    subject
        .register_view(spec("v", &sql, declared.clone()))
        .unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec("v", &sql, declared)).unwrap();
    oracle.force_diff_based().unwrap();

    let feed_sql = |q: String, retract: bool| {
        let ctx = &ctx;
        let subject = &subject;
        let oracle = &oracle;
        async move {
            for batch in ctx.sql(&q).await.unwrap().collect().await.unwrap() {
                let d = if retract {
                    DeltaBatch::from_deletes(batch).unwrap()
                } else {
                    DeltaBatch::from_inserts(batch).unwrap()
                };
                let table = if q.contains("customer") {
                    "customer"
                } else if q.contains("orders") {
                    "orders"
                } else {
                    "lineitem"
                };
                subject.feed(table, d.clone()).unwrap();
                oracle.feed(table, d).unwrap();
            }
        }
    };
    let step_and_compare = |label: &'static str| {
        let subject = &subject;
        let oracle = &oracle;
        async move {
            let s = subject.step_datafusion().await.unwrap();
            oracle.step_datafusion().await.unwrap();
            assert!(s.errored_views.is_empty(), "{label}: {:?}", s.errored_views);
            let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
            let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
            assert_eq!(got, want, "{label}: chain disagreed with recompute");
            got
        }
    };

    // Tick 1: the whole fixture. No order reaches sum(l_quantity) > 300.
    for t in ["customer", "orders", "lineitem"] {
        feed_sql(format!("SELECT * FROM {t}"), false).await;
    }
    let rows = step_and_compare("tick 1").await;
    assert!(rows.is_empty(), "the fixture starts below the threshold");

    // The heaviest order, whose sum a 21x multiplication will push past 300.
    let boost = "SELECT * FROM lineitem WHERE l_orderkey = \
                 (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey \
                  ORDER BY sum(l_quantity) DESC LIMIT 1)";
    for _ in 0..20 {
        feed_sql(boost.to_string(), false).await;
    }
    let rows = step_and_compare("tick 2 (crossing up)").await;
    assert!(
        !rows.is_empty(),
        "the boosted order must cross INTO the HAVING membership"
    );

    for _ in 0..20 {
        feed_sql(boost.to_string(), true).await;
    }
    let rows = step_and_compare("tick 3 (crossing down)").await;
    assert!(
        rows.is_empty(),
        "retracting the copies leaves the set again"
    );

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "q18: fell back to DiffBased: {why}");
    assert!(why.contains("chain"), "q18: not via the chain: {why}");
}

/// SIDE-2 + OUTER-1: q17's correlated scalar `avg` decorrelates to a LEFT
/// OUTER join against a per-partkey aggregate side; the query's own
/// `l_quantity < 0.2 * avg(…)` rejects the padding, the join proves INNER,
/// and the side maintains as the chain's side fold. The fixture holds no
/// Brand#23 / MED BOX part at all, so the honest proof SYNTHESIZES one
/// (fresh key, every value derived from real columns so types match) and
/// drives its lineitem through the threshold: tick 2 lands one row under
/// 0.2×avg (a non-NULL answer must appear), tick 3 retracts the heavy row so
/// the average collapses and the answer must return to NULL. Every tick is
/// compared against full recompute.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q17_registered_verbatim_maintains_incrementally() {
    let ctx = fixture().await;
    let sql = corpus_sql("q17");
    let declared: SchemaRef = Arc::new(ctx.sql(&sql).await.unwrap().schema().as_arrow().clone());

    let subject = IncrementalFlow::new();
    subject
        .register_view(spec("v", &sql, declared.clone()))
        .unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec("v", &sql, declared)).unwrap();
    oracle.force_diff_based().unwrap();

    // The synthesized rows come out of SQL arithmetic with widened types;
    // conform each column back to the SOURCE schema before feeding.
    let conform = |batch: RecordBatch, want: &SchemaRef| -> RecordBatch {
        let cols: Vec<arrow::array::ArrayRef> = batch
            .columns()
            .iter()
            .zip(want.fields())
            .map(|(c, f)| arrow::compute::cast(c, f.data_type()).unwrap())
            .collect();
        RecordBatch::try_new(want.clone(), cols).unwrap()
    };
    let mut src_schemas: AHashMap<String, SchemaRef> = AHashMap::new();
    for t in ["lineitem", "part"] {
        src_schemas.insert(
            t.to_string(),
            Arc::new(ctx.table(t).await.unwrap().schema().as_arrow().clone()),
        );
    }
    let feed_sql = |table: &'static str, q: String, retract: bool| {
        let ctx = &ctx;
        let subject = &subject;
        let oracle = &oracle;
        let src_schemas = &src_schemas;
        let conform = &conform;
        async move {
            for batch in ctx.sql(&q).await.unwrap().collect().await.unwrap() {
                let batch = conform(batch, &src_schemas[table]);
                let d = if retract {
                    DeltaBatch::from_deletes(batch).unwrap()
                } else {
                    DeltaBatch::from_inserts(batch).unwrap()
                };
                subject.feed(table, d.clone()).unwrap();
                oracle.feed(table, d).unwrap();
            }
        }
    };
    let step_and_compare = |label: &'static str| {
        let subject = &subject;
        let oracle = &oracle;
        async move {
            let s = subject.step_datafusion().await.unwrap();
            oracle.step_datafusion().await.unwrap();
            assert!(s.errored_views.is_empty(), "{label}: {:?}", s.errored_views);
            let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
            let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
            assert_eq!(got, want, "{label}: chain disagreed with recompute");
            got
        }
    };

    // Tick 1: the whole fixture — no part qualifies, the global sum is NULL.
    for t in ["lineitem", "part"] {
        feed_sql(t, format!("SELECT * FROM {t}"), false).await;
    }
    let rows = step_and_compare("tick 1").await;
    assert_eq!(rows, vec![vec!["".to_string()]], "fixture answer is NULL");

    // Tick 2: a synthesized Brand#23 / MED BOX part under a fresh key, one
    // light lineitem row (qty 1) and one heavy (qty 100): avg = 50.5, the
    // threshold is 10.1, the light row qualifies — non-NULL answer.
    let part_row = "SELECT p_partkey * 0 + 999999 AS p_partkey, p_name, p_mfgr, \
                    'Brand#23' AS p_brand, p_type, p_size, 'MED BOX' AS p_container, \
                    p_retailprice, p_comment FROM part LIMIT 1";
    let li = |qty: i64| {
        format!(
            "SELECT l_orderkey, l_partkey * 0 + 999999 AS l_partkey, l_suppkey, \
             l_linenumber, l_quantity * 0 + {qty} AS l_quantity, l_extendedprice, \
             l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, \
             l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM lineitem LIMIT 1"
        )
    };
    feed_sql("part", part_row.to_string(), false).await;
    feed_sql("lineitem", li(1), false).await;
    feed_sql("lineitem", li(100), false).await;
    let rows = step_and_compare("tick 2 (crossing in)").await;
    assert_ne!(
        rows,
        vec![vec!["".to_string()]],
        "the light row must land under 0.2 * avg and produce a value"
    );

    // Tick 3: the heavy row retracts, the average collapses to 1, the
    // threshold to 0.2 — nothing qualifies and the answer returns to NULL.
    feed_sql("lineitem", li(100), true).await;
    let rows = step_and_compare("tick 3 (crossing out)").await;
    assert_eq!(rows, vec![vec!["".to_string()]], "back to NULL");

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "q17: fell back to DiffBased: {why}");
    assert!(why.contains("chain"), "q17: not via the chain: {why}");
}

/// SIDE-3: q2's scalar `min(ps_supplycost)` side reads FOUR tables — the
/// side is itself a join run, cut by recursing through the spine's own
/// engine. The spine additionally needs PREFIX relinearization: its FROM
/// order (`part, supplier, …`) is disconnected at the leaf, and the side
/// join atop the run carries a filter, so the old all-or-nothing gate never
/// fired. Two equi keys tie the side in: the correlation key and
/// `ps_supplycost = min(…)` itself (JOIN-2's WHERE-equality → trace key).
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q2_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute(
        "q2",
        &["part", "supplier", "partsupp", "nation", "region"],
        &corpus_sql("q2"),
    )
    .await;
}

/// UNCORR-1 + SIDE-3: q15's `total_revenue = (SELECT max(total_revenue) FROM
/// revenue0)` — an UNCORRELATED scalar subquery no optimizer rule rewrites
/// (DataFusion executes them natively; the delta-batch path cannot). The
/// engine's narrow rewrite cross-joins the one-row global-max side, the
/// equality itself becomes the trace key, and BOTH sides (revenue0, and the
/// max over revenue0) maintain as sub-chains over lineitem. Half of
/// lineitem arrives at the maintenance tick, moving per-supplier revenues
/// AND the global max through the same step.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q15_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q15", &["lineitem", "supplier"], &corpus_sql("q15")).await;
}

/// LEFTAGG-1 + COUNTNULL-1: q13's LEFT OUTER join carries its filter IN THE
/// ON (`… AND o_comment NOT LIKE …`) — a right-side-only predicate that
/// pre-filters orders without touching the padding — and the count
/// distribution above depends on zero-count customers EXISTING, which the
/// aggregate used to GC (a group whose counted column is all NULL read as
/// empty). Half of customer arrives at the maintenance tick.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q13_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q13", &["customer", "orders"], &corpus_sql("q13")).await;
}

/// KEYLESS-1 + SEMI-2 + UNCORR-1 composed: q22's anti join (NOT EXISTS
/// orders) and its uncorrelated `avg(c_acctbal)` side — joined KEYLESS,
/// admissible because the side is a global aggregate (one row by
/// construction). Half of customer arrives at the maintenance tick, moving
/// the average and the anti-join membership through one step.
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q22_registered_verbatim_maintains_incrementally() {
    verbatim_join_matches_recompute("q22", &["customer", "orders"], &corpus_sql("q22")).await;
}

/// ORFACTOR-1: q19's WHERE is a three-arm disjunction, every arm repeating
/// `p_partkey = l_partkey`, the shipmode IN-list and the shipinstruct
/// equality — factored, the equality keys the trace and the arms' remainder
/// filters pairs. The fixture answer is NULL (no row satisfies any arm), so
/// the proof DRIVES one: a synthesized Brand#12 / SM CASE / size-3 part and
/// a qty-5 AIR DELIVER-IN-PERSON lineitem land inside arm 1 (non-NULL
/// asserted), then retract (NULL again).
#[tokio::test(flavor = "multi_thread")]
async fn tpch_q19_registered_verbatim_maintains_incrementally() {
    let ctx = fixture().await;
    let sql = corpus_sql("q19");
    let declared: SchemaRef = Arc::new(ctx.sql(&sql).await.unwrap().schema().as_arrow().clone());

    let subject = IncrementalFlow::new();
    subject
        .register_view(spec("v", &sql, declared.clone()))
        .unwrap();
    let oracle = IncrementalFlow::new();
    oracle.register_view(spec("v", &sql, declared)).unwrap();
    oracle.force_diff_based().unwrap();

    let conform = |batch: RecordBatch, want: &SchemaRef| -> RecordBatch {
        let cols: Vec<arrow::array::ArrayRef> = batch
            .columns()
            .iter()
            .zip(want.fields())
            .map(|(c, f)| arrow::compute::cast(c, f.data_type()).unwrap())
            .collect();
        RecordBatch::try_new(want.clone(), cols).unwrap()
    };
    let mut src_schemas: AHashMap<String, SchemaRef> = AHashMap::new();
    for t in ["lineitem", "part"] {
        src_schemas.insert(
            t.to_string(),
            Arc::new(ctx.table(t).await.unwrap().schema().as_arrow().clone()),
        );
    }
    let feed_sql = |table: &'static str, q: String, retract: bool| {
        let ctx = &ctx;
        let subject = &subject;
        let oracle = &oracle;
        let src_schemas = &src_schemas;
        let conform = &conform;
        async move {
            for batch in ctx.sql(&q).await.unwrap().collect().await.unwrap() {
                let batch = conform(batch, &src_schemas[table]);
                let d = if retract {
                    DeltaBatch::from_deletes(batch).unwrap()
                } else {
                    DeltaBatch::from_inserts(batch).unwrap()
                };
                subject.feed(table, d.clone()).unwrap();
                oracle.feed(table, d).unwrap();
            }
        }
    };
    let step_and_compare = |label: &'static str| {
        let subject = &subject;
        let oracle = &oracle;
        async move {
            let s = subject.step_datafusion().await.unwrap();
            oracle.step_datafusion().await.unwrap();
            assert!(s.errored_views.is_empty(), "{label}: {:?}", s.errored_views);
            let got = canonical(&subject.snapshot("v").unwrap().expect("published"));
            let want = canonical(&oracle.snapshot("v").unwrap().expect("published"));
            assert_eq!(got, want, "{label}: chain disagreed with recompute");
            got
        }
    };

    for t in ["lineitem", "part"] {
        feed_sql(t, format!("SELECT * FROM {t}"), false).await;
    }
    let rows = step_and_compare("tick 1").await;
    assert_eq!(rows, vec![vec!["".to_string()]], "fixture answer is NULL");

    let part_row = "SELECT p_partkey * 0 + 888888 AS p_partkey, p_name, p_mfgr, \
                    'Brand#12' AS p_brand, p_type, p_size * 0 + 3 AS p_size, \
                    'SM CASE' AS p_container, p_retailprice, p_comment FROM part LIMIT 1";
    let li_row = "SELECT l_orderkey, l_partkey * 0 + 888888 AS l_partkey, l_suppkey, \
                  l_linenumber, l_quantity * 0 + 5 AS l_quantity, l_extendedprice, \
                  l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, \
                  l_receiptdate, 'DELIVER IN PERSON' AS l_shipinstruct, 'AIR' AS l_shipmode, \
                  l_comment FROM lineitem LIMIT 1";
    feed_sql("part", part_row.to_string(), false).await;
    feed_sql("lineitem", li_row.to_string(), false).await;
    let rows = step_and_compare("tick 2 (arm 1 satisfied)").await;
    assert_ne!(
        rows,
        vec![vec!["".to_string()]],
        "arm 1 must produce revenue"
    );

    feed_sql("lineitem", li_row.to_string(), true).await;
    let rows = step_and_compare("tick 3 (retracted)").await;
    assert_eq!(rows, vec![vec!["".to_string()]], "back to NULL");

    let (inc, why) = subject
        .view_plan_classification("v")
        .unwrap()
        .expect("registered");
    assert!(inc, "q19: fell back to DiffBased: {why}");
    assert!(why.contains("chain"), "q19: not via the chain: {why}");
}
