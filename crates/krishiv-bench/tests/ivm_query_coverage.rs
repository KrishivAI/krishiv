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
use arrow::array::RecordBatch;
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

    // q13's side input: bounded reference data the STREAMING bench registers
    // once before the stream starts (k UInt64, label Utf8 — the same shape).
    // In the delta-batch world a side input is simply another SOURCE, fed
    // once and never ticked again, so the measurement env registers it too —
    // without it the query referenced a table that did not exist and was
    // counted Unplannable for a reason that had nothing to do with the
    // engine.
    let side = {
        use arrow::array::{StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        let keys: Vec<u64> = (0..1000).collect();
        let labels: Vec<String> = keys.iter().map(|k| format!("cat-{k}")).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::UInt64, false),
                Field::new("label", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(keys)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap()
    };
    let ctx = SessionContext::new();
    let mut schemas = AHashMap::new();
    for (name, batch) in [
        ("bid", bid),
        ("auction", auction),
        ("person", person),
        ("side", side),
    ] {
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
    // WINDOW-1 / HOP-1 / TOPNK-1: registration rewrites the TVF and
    // streaming-ranking dialects into standard SQL, so this measurement
    // applies the same rewrite chain — it measures what the engine does to a
    // query handed over verbatim, and verbatim registration IS the engine's
    // surface.
    let mut owned = sql.to_owned();
    for rewrite in [
        krishiv_ivm::window_rewrite::rewrite_tumble_tvfs,
        krishiv_ivm::window_rewrite::rewrite_hop_tvfs,
        krishiv_ivm::window_rewrite::rewrite_streaming_topn,
    ] {
        if let Some(r) = rewrite(&owned) {
            owned = r;
        }
    }
    let sql = owned.as_str();
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
/// remaining mechanisms landed in two batches. Batch 1: LEFTAGG-1 pushes a
/// LEFT OUTER's right-side-only ON conjuncts to the right input (q13);
/// ORFACTOR-1 factors conjuncts common to every OR arm (q19); KEYLESS-1
/// admits keyless INNER joins against engine-built GLOBAL-aggregate sides
/// (q22). Batch 2: NESTED-1 flattens sides-of-sides into dependency order
/// (q20); SEMI-3 evaluates NON-EQUI membership conditions per left row
/// inside the key group (q21), with resolver-resolved membership sides'
/// qualifiers kept through re-rooting. MIDJOIN-1 descends the chain
/// THROUGH a join whose left is an operator and flips the relation
/// convention (aliased → bare) at the first mid-chain join, so an
/// uncorrelated side can join ABOVE the grouped aggregate (q11).
/// SELFJOIN-1 completes the suite: a plain aliased side whose bare names
/// collide with the accumulated relation RENAMES — its level emits every
/// column as `__alias__col`, references above rewrite to match, and the
/// cross-occurrence residual compiles left-first (q7, q8's second
/// `nation`). All 22 TPC-H queries maintain.
///
/// The larger coverage figures quoted elsewhere for TPC-H are a different
/// measurement: what a *human* can build by hand-decomposing each query into
/// single-hop views (166 views for 28 queries). That is a real capability and
/// a fair claim, but it is not this one, and the two must never be quoted as
/// though they were.
const TPCH_INCREMENTAL: usize = 22;
/// Five stateless queries (q0, q10, q14, q21, q22), three band joins (q3,
/// q8, q20 — BAND-1), three TUMBLE windows (q1, q2, q7 — WINDOW-1 + UINT-1),
/// the HOP window (q5 — HOP-1: the fan-out rewritten to phase-shifted UNION
/// ALL branches, maintained as a stateless FlatMap chain leaf), and the two
/// keyed rankings (q18 keep-last dedup, q19 per-auction top-10 — TOPNK-1:
/// the streaming `GROUP BY … ORDER BY … LIMIT n` idiom rewritten to QUALIFY
/// row_number and maintained by the per-partition ordered index), and the
/// side-input join (q13 — KEYEXPR-1: the computed key `auction % 1000`
/// hoisted into a TRY_CAST projection under the join's left input, the
/// side table registered as the bounded source it is),
/// and the three statistics queries (q15, q16, q17 — CDIST-1 gives
/// COUNT(DISTINCT col) per-value multiplicity by sharing MIN/MAX's value
/// multiset). The remaining eight: HOP fans out 1:N, SESSION merges
/// statefully, PROCTIME has no delta-batch meaning, q4/q9 window a derived
/// join, q13 needs a side input, q18 needs row_number, q19 a per-window
/// top-N.
const NEXMARK_INCREMENTAL: usize = 18;

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
/// (LEFTAGG-1), q19 (ORFACTOR-1), q22 (KEYLESS-1), q20 (NESTED-1) and q21
/// (SEMI-3), q11 (MIDJOIN-1), and q7/q8 (SELFJOIN-1: the second `nation`
/// renamed through the chain).
/// NEXMark's band joins q3/q20 decompose the same way — their aliased
/// person/auction sides collide on bare `id` and rename. q8 still cannot:
/// its own SELECT list emits BOTH `p.id` and `a.id`, so the view's declared
/// output relation repeats a bare name — ambiguous as a flat hop schema no
/// matter what the chain renames internally. BAND-1 maintains it whole.
const TPCH_DECOMPOSED: usize = 22;
/// q14 (filter plus computed projection), the q3/q20 band joins (SELFJOIN-1
/// renames their colliding sides), the six TUMBLE windows (q1, q2, q7, q15,
/// q16, q17 — this gate previously never applied WINDOW-1's registration
/// rewrite, so it counted them Unplannable while the single-view gate
/// counted them Incremental: an under-measurement, corrected when the gate
/// started mirroring the FULL rewrite chain), and q5 (HOP-1's union fans as
/// a FlatMap chain leaf), and q13 (KEYEXPR-1's key-bearing hop under the
/// mid-chain join). q18/q19 maintain WHOLE (the keyed top-N is one
/// operator with its pre/post maps riding it — nothing to cut), and the
/// remaining single-table queries are single-operator or q8 (output repeats
/// `id`).
const NEXMARK_DECOMPOSED: usize = 11;

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
        // The same registration rewrite chain as `classify` — the decomposer
        // sees what registration hands it, not the raw dialect.
        let mut owned = sql.clone();
        for rewrite in [
            krishiv_ivm::window_rewrite::rewrite_tumble_tvfs,
            krishiv_ivm::window_rewrite::rewrite_hop_tvfs,
            krishiv_ivm::window_rewrite::rewrite_streaming_topn,
        ] {
            if let Some(r) = rewrite(&owned) {
                owned = r;
            }
        }
        let sql = &owned;
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
