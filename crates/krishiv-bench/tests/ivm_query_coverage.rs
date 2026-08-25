//! How many standard-benchmark queries actually maintain on delta-batch.
//!
//! # Why this is a test and not a number in a document
//!
//! The IVM coverage figure has, until now, only ever existed as something an
//! agent measured once and wrote down. That is exactly the kind of claim this
//! repo's audit register keeps catching as wrong: the ORDER-1/TOPN-1 seam
//! defect — every top-N view silently losing its `ORDER BY` — was found by
//! *re-deriving* the count from scratch, not by re-reading the code, and five
//! passing tests had agreed with the bug. A number nobody can re-run is a
//! number nobody can disprove.
//!
//! So the measurement lives here, in CI, against the committed corpora.
//!
//! # What it measures, precisely
//!
//! For each query: register it **verbatim as a single materialized view** and
//! ask whether the planner produces an O(Δ) plan. That is the question the
//! benchmark gate `require_incremental_plan()` asks, and it is the question
//! that decides whether "we benchmarked IVM" means anything.
//!
//! It deliberately does **not** measure the more generous thing — whether a
//! query can be *hand-decomposed* into a DAG of single-hop incremental views.
//! That number is larger (TPC-H q21 alone takes 17 hops) and it is a statement
//! about what a careful human can build, not about what the engine does when
//! handed the query. Both are legitimate; only one is honest to call
//! "coverage" without a footnote, and only one can be gated by a machine.
//!
//! The declared output schema is DataFusion's own logical schema for the
//! query. That matters: IVM-AUD-SCHEMA-1's guard compares the emitted relation
//! against the declared one, so measuring against a schema we invented would
//! measure a contract nobody has.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout)]

use std::sync::Arc;

use ahash::AHashMap;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use krishiv_bench::nexmark::{NexmarkGenerator, SUPPORTED_QUERIES};
use krishiv_bench::tpch_fixture::fixture_ddl;
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_ivm::{ViewPlanKind, build_view_plan};

/// The verdict for one query.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// The planner produced an O(Δ) plan — the query maintains on delta-batch.
    Incremental,
    /// The query plans, but only as full recompute + diff.
    DiffBased,
    /// The SQL does not reach the planner at all (unsupported syntax).
    Unplannable,
}

/// A session with the eight TPC-H tables, and their Arrow schemas.
///
/// The fixture declares money as `DECIMAL(15,2)`, which is the point: a
/// coverage number measured against `Float64` money is measuring a different
/// benchmark than the one whose reference answers are exact decimal.
async fn tpch_env() -> (SessionContext, AHashMap<String, SchemaRef>) {
    let ctx = SessionContext::new();
    for ddl in fixture_ddl() {
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    }
    let mut schemas = AHashMap::new();
    for t in [
        "region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem",
    ] {
        let df = ctx.table(t).await.unwrap();
        schemas.insert(t.to_owned(), Arc::new(df.schema().as_arrow().clone()));
    }
    (ctx, schemas)
}

/// The three NEXMark sources, taking their schemas from the generator the
/// benchmarks actually feed — not from a hand-copied approximation that could
/// drift away from what is measured.
async fn nexmark_env() -> (SessionContext, AHashMap<String, SchemaRef>) {
    let mut g = NexmarkGenerator::new(42, 1_000, 0, 0);
    let bid = g.next_bid_batch(1).unwrap();
    let auction = g.next_auction_batch(1).unwrap();
    let person = g.next_person_batch(1).unwrap();

    let ctx = SessionContext::new();
    let mut schemas = AHashMap::new();
    for (name, batch) in [("bid", bid), ("auction", auction), ("person", person)] {
        let schema = batch.schema();
        ctx.register_batch(name, batch).unwrap();
        schemas.insert(name.to_owned(), schema);
    }
    (ctx, schemas)
}

/// Plan one query as a single view and classify it.
async fn classify(
    ctx: &SessionContext,
    schemas: &AHashMap<String, SchemaRef>,
    sql: &str,
) -> Verdict {
    // DataFusion's own logical schema is the view's declared contract.
    let Ok(df) = ctx.sql(sql).await else {
        return Verdict::Unplannable;
    };
    let declared: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    match build_view_plan(sql, &declared, schemas, &[]).await.kind() {
        ViewPlanKind::Incremental => Verdict::Incremental,
        ViewPlanKind::DiffBased => Verdict::DiffBased,
    }
}

async fn measure(
    label: &str,
    queries: Vec<(String, String)>,
    ctx: &SessionContext,
    schemas: &AHashMap<String, SchemaRef>,
) -> usize {
    let mut incremental = 0;
    println!("\n── {label} ──");
    for (id, sql) in &queries {
        let v = classify(ctx, schemas, sql).await;
        if v == Verdict::Incremental {
            incremental += 1;
        }
        println!("  {id:<28} {v:?}");
    }
    println!("  {label}: {incremental}/{} incremental", queries.len());
    incremental
}

/// The floors are deliberately **exact** on the total rather than a `>=`.
///
/// A `>=` floor hides the failure mode this repo keeps hitting: a change that
/// fixes one query while quietly breaking another nets to zero and reads as
/// "no regression". Exact counts make any movement, in either direction, a
/// test failure that has to be looked at and re-blessed on purpose.
/// **TPC-H is zero, and that is not a bug — it is the shape of the engine.**
/// Every TPC-H query composes several relational operators (join → aggregate →
/// order, and q21 nests seventeen deep). The IVM planner builds **one operator
/// per view**; it does not decompose a multi-operator query into a DAG of
/// internal views. So a TPC-H query handed over verbatim has nothing the
/// planner can match, and falls to full recompute — every time, by design.
///
/// The larger coverage figures quoted elsewhere for TPC-H are a different
/// measurement: what a *human* can build by hand-decomposing each query into
/// single-hop views (166 views for 28 queries). That is a real capability and
/// a fair claim, but it is not this one, and the two must never be quoted as
/// though they were.
const TPCH_INCREMENTAL: usize = 0;
/// All five are NEXMark's stateless queries — projection and filter, the
/// IVM-MAP-1 operator. They are single-operator queries, which is exactly why
/// they are the ones that pass.
const NEXMARK_INCREMENTAL: usize = 5;

#[tokio::test(flavor = "multi_thread")]
async fn standard_benchmark_queries_that_maintain_on_delta_batch() {
    let (ctx, schemas) = tpch_env().await;
    let tpch: Vec<(String, String)> = TPCH_QUERIES
        .iter()
        .map(|q| (q.id.to_owned(), q.sql_at_scale(1.0)))
        .collect();
    let tpch_n = measure("TPC-H (22 queries, single-view)", tpch, &ctx, &schemas).await;

    let (nctx, nschemas) = nexmark_env().await;
    let nexmark: Vec<(String, String)> = SUPPORTED_QUERIES
        .iter()
        .map(|q| (q.name.to_owned(), q.sql.to_owned()))
        .collect();
    let nexmark_n = measure(
        "NEXMark (22 queries, single-view)",
        nexmark,
        &nctx,
        &nschemas,
    )
    .await;

    println!(
        "\nTOTAL: {}/{} standard-benchmark queries maintain incrementally as a single view",
        tpch_n + nexmark_n,
        TPCH_QUERIES.len() + SUPPORTED_QUERIES.len()
    );

    assert_eq!(
        (tpch_n, nexmark_n),
        (TPCH_INCREMENTAL, NEXMARK_INCREMENTAL),
        "IVM single-view coverage moved. This is not automatically a failure — \
         but it must be re-blessed deliberately, with the constants updated in \
         the same commit as the change that moved them."
    );
}
