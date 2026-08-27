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
    // WINDOW-1: registration rewrites `TUMBLE(TABLE …)` into standard SQL, so
    // this measurement applies the same rewrite — it measures what the engine
    // does to a query handed over verbatim, and verbatim registration IS the
    // engine's surface.
    let rewritten = krishiv_ivm::window_rewrite::rewrite_tumble_tvfs(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
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
/// **q1, q3, q4, q5, q6, q9, q10, q12, q14, q16 — via chains.** DECOMP-3
/// wired linear single-table chains (q1, q6); DECOMP-4 admits a two-source
/// join leaf (q12, q14); MJOIN-1 admits LEFT-DEEP multi-way join runs with
/// the WHERE's conjuncts distributed to the lowest covering level (q5);
/// TOPN-2 makes `ORDER BY … LIMIT` its own top-N hop (q3, q10); REORDER-1
/// relinearizes the join GRAPH when the FROM order would put a keyless cross
/// join at some level (q9's `part, supplier` meet only through `lineitem`);
/// SEMI-2 admits membership levels — a semi/anti join whose right side is a
/// filtered projection of one source — so q4's EXISTS becomes a LeftSemi
/// leaf and q16's NOT IN a mid-chain LeftAnti level, each hop verified on
/// its re-rooted plan itself (PLANHOP-1). SIDE-1 admits membership sides
/// that are themselves AGGREGATES, maintained as the chain's SIDE fold —
/// q18's `IN (… GROUP BY … HAVING sum > 300)`. SIDE-2 + OUTER-1 admit
/// EMITTED scalar-aggregate sides: the decorrelated LEFT OUTER join's
/// padding is rejected by the query's own comparison, DataFusion's
/// elimination proves it INNER, and the per-key aggregate maintains as the
/// side fold with its value columns joined into the spine — q17. The
/// remaining 5 mechanisms landed together (batch 1): LEFTAGG-1 pushes a
/// LEFT OUTER's right-side-only ON conjuncts to the right input — padding
/// untouched by definition (q13); ORFACTOR-1 factors conjuncts common to
/// every OR arm so the equality keys the trace (q19); KEYLESS-1 admits an
/// INNER join with NO equi key when the right side is an engine-built
/// GLOBAL-aggregate side — one row by construction, so the cross product is
/// left × 1 (q22). Still refused, measured: q11's uncorrelated side joins
/// ABOVE the mid-chain aggregate (a join whose left is an operator — the
/// chain walk stops at joins); q20's membership side has sides of its OWN;
/// q21's semi level carries a non-equi membership conjunct; self-joins
/// collide bare names (q7, q8).
///
/// The larger coverage figures quoted elsewhere for TPC-H are a different
/// measurement: what a *human* can build by hand-decomposing each query into
/// single-hop views (166 views for 28 queries). That is a real capability and
/// a fair claim, but it is not this one, and the two must never be quoted as
/// though they were.
const TPCH_INCREMENTAL: usize = 17;
/// Five stateless queries (q0, q10, q14, q21, q22), three band joins (q3,
/// q8, q20 — BAND-1), three TUMBLE windows (q1, q2, q7 — WINDOW-1 + UINT-1),
/// and the three statistics queries (q15, q16, q17 — CDIST-1 gives
/// COUNT(DISTINCT col) per-value multiplicity by sharing MIN/MAX's value
/// multiset). The remaining eight: HOP fans out 1:N, SESSION merges
/// statefully, PROCTIME has no delta-batch meaning, q4/q9 window a derived
/// join, q13 needs a side input, q18 needs row_number, q19 a per-window
/// top-N.
const NEXMARK_INCREMENTAL: usize = 14;

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

/// q1, q6 (single-table chains), q12, q14 (join-leaf chains), q5 (MJOIN-1
/// multi-way run), q3, q10 (TOPN-2 top-N hops), q9 (REORDER-1 relinearized
/// graph), q4, q16 (SEMI-2 semi/anti membership levels), q18 (SIDE-1: a
/// HAVING membership set maintained as the chain's side fold), and q17
/// (SIDE-2 + OUTER-1: an emitted scalar-aggregate side on a
/// proven-inner join), and q2 (SIDE-3: a side that is itself a four-table
/// join run, plus prefix relinearization), and q15 (UNCORR-1: an
/// uncorrelated global-max side keyed by its own equality, with the
/// mid-side `revenue0` alias threaded onto the hop scans), plus q13
/// (LEFTAGG-1), q19 (ORFACTOR-1) and q22 (KEYLESS-1).
/// NEXMark's three band joins do NOT decompose —
/// their side tables share the bare column name `id`, which would make every
/// reference above the join hop ambiguous — and they need no chain: BAND-1
/// maintains them whole.
const TPCH_DECOMPOSED: usize = 17;
/// q14 — filter plus computed projection. The other single-table NEXMark
/// queries are single-operator (already incremental whole, nothing to cut),
/// windowed TVFs (unplannable), or joins.
const NEXMARK_DECOMPOSED: usize = 1;

/// How many queries the engine can cut into a chain where EVERY hop maintains
/// incrementally. `decompose` verifies each hop's plan itself and refuses
/// wholesale on any non-incremental hop, so a `Some` here is a machine-checked
/// claim, not a hope — and `ivm_decomposition.rs` proves the chains answer
/// exactly what the whole query answers.
async fn measure_decomposed(
    label: &str,
    queries: Vec<(String, String)>,
    ctx: &SessionContext,
    schemas: &AHashMap<String, SchemaRef>,
) -> usize {
    let mut decomposed = 0;
    println!("\n── {label} ──");
    for (id, sql) in &queries {
        let Ok(df) = ctx.sql(sql).await else {
            println!("  {id:<28} Unplannable");
            continue;
        };
        let declared: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        match krishiv_ivm::decompose("v", sql, &declared, schemas).await {
            Some(hops) => {
                decomposed += 1;
                println!("  {id:<28} Decomposed ({} hops)", hops.len());
            }
            None => println!("  {id:<28} Refused"),
        }
    }
    println!("  {label}: {decomposed}/{} decompose fully", queries.len());
    decomposed
}

#[tokio::test(flavor = "multi_thread")]
async fn standard_benchmark_queries_that_decompose_into_incremental_chains() {
    let (ctx, schemas) = tpch_env().await;
    let tpch: Vec<(String, String)> = TPCH_QUERIES
        .iter()
        .map(|q| (q.id.to_owned(), q.sql_at_scale(1.0)))
        .collect();
    let tpch_n = measure_decomposed("TPC-H (22 queries, decomposed)", tpch, &ctx, &schemas).await;

    let (nctx, nschemas) = nexmark_env().await;
    let nexmark: Vec<(String, String)> = SUPPORTED_QUERIES
        .iter()
        .map(|q| (q.name.to_owned(), q.sql.to_owned()))
        .collect();
    let nexmark_n = measure_decomposed(
        "NEXMark (22 queries, decomposed)",
        nexmark,
        &nctx,
        &nschemas,
    )
    .await;

    println!("\nDECOMPOSED TOTAL: {}/44", tpch_n + nexmark_n);

    assert_eq!(
        (tpch_n, nexmark_n),
        (TPCH_DECOMPOSED, NEXMARK_DECOMPOSED),
        "IVM decomposition coverage moved. Re-bless deliberately, with the \
         constants updated in the same commit as the change that moved them."
    );
}
