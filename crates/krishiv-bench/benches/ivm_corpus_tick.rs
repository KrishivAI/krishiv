//! What one delta-batch tick costs, per corpus query, against full recompute.
//!
//! # Why this exists
//!
//! `ivm_vs_full_recompute` measures ONE query — a `GROUP BY SUM` — across
//! accumulated-table sizes. It is the right shape for the headline cost claim
//! and the wrong shape for a coverage corpus: it says nothing about what a HOP
//! fan-out, a keyed top-N, a sessionizer or a QUALIFY chain hop costs per tick.
//! Those operators landed with correctness proofs and coverage gates and **no
//! timed measurement at all**, which is how a mechanism that is correct and
//! ruinously slow survives review.
//!
//! So this walks the committed NEXMark corpus and, for each query, times the
//! same tick twice: once on the incremental plan, once on a `force_diff_based`
//! flow that re-runs the whole view SQL and diffs it. The ratio is reported
//! per query, never as a corpus average, because an average would hide the one
//! query that behaves badly.
//!
//! **Reading the ratio correctly.** A ratio below 1.0 at the default seed is
//! not by itself a defect: the delta is then a large fraction of the state,
//! and incremental maintenance only wins as state/delta grows —
//! `ivm_vs_full_recompute` puts the crossover for a simple aggregate past 1M
//! accumulated rows. What IS a defect is a tick whose cost grows with
//! accumulated state while the delta is held fixed, because that is an O(state)
//! term hiding inside a plan classified O(delta). Scale `SEED` with `DELTA`
//! pinned to tell the two apart — that axis is what found IVM-AUD-PERF-2,
//! where scaling the DELTA instead exposed a quadratic cross term.
//!
//! # What it does not do
//!
//! - **TPC-H is absent, deliberately.** `tpch_fixture` ships DDL only; the
//!   TPC-H benches read Parquet from an external dataset directory. Ticking
//!   those queries needs rows, so a self-contained bench cannot cover them
//!   without inventing data whose distribution nobody has agreed to. Every
//!   operator added since the last benchmark entry is exercised by NEXMark, so
//!   this covers the actual gap; TPC-H tick costs remain unmeasured and are
//!   listed as such in `docs/BENCHMARKING.md`.
//! - **State accumulates across the timed ticks.** Each tick adds
//!   `DELTA_ROWS` and never retracts, so tick *k* sees more state than tick
//!   *k-1*. That is the steady-state shape a live view actually runs in; it is
//!   NOT a fixed-state measurement, and the median over `TICKS` is reported
//!   rather than the mean so one growing outlier cannot carry the number.
//! - **Not criterion.** 21 queries x 2 flows under criterion's sampling would
//!   run for an hour to sharpen numbers whose purpose is spotting a 2x
//!   regression. Medians over a handful of ticks answer that; anything
//!   claiming three significant figures off this harness is over-reading it.
//!
//! To run: `cargo bench -p krishiv-bench --bench ivm_corpus_tick`.
//! Per `docs/BENCHMARKING.md`: record commit, worktree state and hardware
//! alongside any published result — this file does not do that for you.

// Benchmark harness: a panic is the failure signal, and clippy.toml's
// `allow-*-in-tests` does not cover bench targets.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use krishiv_bench::nexmark::{BATCH_DIALECT_EQUIVALENTS, NexmarkGenerator, SUPPORTED_QUERIES};
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_ivm::IncrementalFlow;

/// Rows per source in the untimed seed, before any tick is measured.
const SEED_ROWS_DEFAULT: usize = 20_000;
/// Rows per source fed before each timed tick.
const DELTA_ROWS_DEFAULT: usize = 5_000;
/// Timed ticks per query per flow; the median is reported.
const TICKS_DEFAULT: usize = 5;

/// Overridable so the shape of a cost can be measured, not guessed: holding
/// the seed fixed and scaling only the delta separates a term that is
/// quadratic in the delta from one that is linear in accumulated state.
/// `KRISHIV_BENCH_CORPUS_ONLY` restricts the run to queries whose name
/// contains the given substring.
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn seed_rows() -> usize {
    env_usize("KRISHIV_BENCH_CORPUS_SEED", SEED_ROWS_DEFAULT)
}
fn delta_rows() -> usize {
    env_usize("KRISHIV_BENCH_CORPUS_DELTA", DELTA_ROWS_DEFAULT)
}
fn ticks() -> usize {
    env_usize("KRISHIV_BENCH_CORPUS_TICKS", TICKS_DEFAULT)
}

/// The queries measured: the verbatim corpus plus the two batch-dialect
/// equivalents, which are where the QUALIFY chain hop actually runs.
fn corpus() -> Vec<(&'static str, &'static str)> {
    SUPPORTED_QUERIES
        .iter()
        .chain(BATCH_DIALECT_EQUIVALENTS.iter())
        .map(|q| (q.name, q.sql))
        .collect()
}

/// Registration rewrites the TVF and streaming-ranking dialects into standard
/// SQL, so the measurement applies the same chain — what is timed must be what
/// the engine runs when handed the query verbatim.
fn rewrite(sql: &str) -> String {
    let mut owned = sql.to_owned();
    for r in [
        krishiv_ivm::window_rewrite::rewrite_tumble_tvfs,
        krishiv_ivm::window_rewrite::rewrite_hop_tvfs,
        krishiv_ivm::window_rewrite::rewrite_session_tvfs,
        krishiv_ivm::window_rewrite::rewrite_streaming_topn,
    ] {
        if let Some(next) = r(&owned) {
            owned = next;
        }
    }
    owned
}

/// The bounded reference table q13 joins against — a source fed once, never
/// ticked, exactly as a side input behaves in the delta-batch world.
fn side_batch() -> RecordBatch {
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
}

/// Which streaming sources a query reads, taken from the query's own PLAN.
///
/// The first version of this matched bare tokens, and it was wrong in the way
/// that matters: `auction` is a COLUMN on `bid` (q0, q10, q14, q21, q22), so
/// token-matching fed a source those queries never read. The feed guard
/// (IVM-AUD-CORE-31) rejected it and six queries reported as failures that had
/// nothing wrong with them — the harness's bug presenting as the engine's.
/// Reading the relations off DataFusion's resolved logical plan cannot make
/// that mistake: a column reference is not a table scan.
fn sources_of(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<&'static str> {
    let mut found: Vec<&'static str> = Vec::new();
    // `apply` walks the plan tree only, and a subquery lives inside an
    // EXPRESSION (`EXISTS (…)`, `x IN (SELECT …)`), so a plain walk misses the
    // tables TPC-H q4/q16/q20/q22 read from their subqueries. Those four then
    // went unfed and died at tick time with "table not found" — the harness's
    // blind spot presenting as an engine failure, the same shape as the
    // earlier `bid.auction`-is-not-a-table mistake.
    plan.apply_with_subqueries(|node| {
        if let datafusion::logical_expr::LogicalPlan::TableScan(scan) = node {
            let name = scan.table_name.table();
            for s in [
                "bid", "auction", "person", "region", "nation", "supplier", "customer", "part",
                "partsupp", "orders", "lineitem",
            ] {
                if name.eq_ignore_ascii_case(s) && !found.contains(&s) {
                    found.push(s);
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("plan walk");
    found
}

/// One generator's next `n` rows for a named source.
fn next_batch(g: &mut NexmarkGenerator, source: &str, n: usize) -> RecordBatch {
    match source {
        "bid" => g.next_bid_batch(n).unwrap(),
        "auction" => g.next_auction_batch(n).unwrap(),
        "person" => g.next_person_batch(n).unwrap(),
        other => panic!("unknown source {other}"),
    }
}

/// A DataFusion context holding one row per source, used only to derive each
/// query's declared output schema — the contract IVM-AUD-SCHEMA-1 checks the
/// emitted relation against.
async fn schema_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    let mut g = NexmarkGenerator::new(42, 1_000, 0, 0);
    ctx.register_batch("bid", g.next_bid_batch(1).unwrap())
        .unwrap();
    ctx.register_batch("auction", g.next_auction_batch(1).unwrap())
        .unwrap();
    ctx.register_batch("person", g.next_person_batch(1).unwrap())
        .unwrap();
    ctx.register_batch("side", side_batch()).unwrap();
    ctx
}

/// `None` when there are no samples — reachable via
/// `KRISHIV_BENCH_CORPUS_TICKS=0`, which must report as an error rather than
/// panic or invent a duration.
/// Where a corpus gets its rows. NEXMark synthesises them; TPC-H reads the
/// generated Parquet dataset and hands out successive slices.
///
/// Both corpora go through ONE `measure` path deliberately: a second copy of
/// the seed/tick/compare loop is a second place for the two arms to drift into
/// measuring different things.
enum RowSource {
    Nexmark,
    Tpch(AHashMap<String, RecordBatch>),
}

impl RowSource {
    fn new_cursor(&self) -> RowCursor<'_> {
        match self {
            Self::Nexmark => RowCursor::Nexmark(Box::new(NexmarkGenerator::new(42, 1_000, 0, 0))),
            Self::Tpch(tables) => RowCursor::Tpch {
                tables,
                offsets: AHashMap::new(),
            },
        }
    }
}

enum RowCursor<'a> {
    Nexmark(Box<NexmarkGenerator>),
    Tpch {
        tables: &'a AHashMap<String, RecordBatch>,
        offsets: AHashMap<String, usize>,
    },
}

impl RowCursor<'_> {
    /// The next `n` rows for `source`, or `None` when that table is exhausted.
    /// TPC-H `nation` holds 25 rows total, so it seeds and then has nothing
    /// left — a legitimate empty delta, not a failure.
    fn next(&mut self, source: &str, n: usize) -> Option<RecordBatch> {
        match self {
            Self::Nexmark(g) => Some(next_batch(g, source, n)),
            Self::Tpch { tables, offsets } => {
                let table = tables.get(source)?;
                let off = offsets.entry(source.to_owned()).or_insert(0);
                if *off >= table.num_rows() {
                    return None;
                }
                let take = n.min(table.num_rows() - *off);
                let slice = table.slice(*off, take);
                *off += take;
                Some(slice)
            }
        }
    }
}

/// Load the TPC-H tables from a Parquet dataset directory, capped so the
/// harness holds a bounded amount in memory. `None` when the dataset is absent
/// — the arm then reports itself SKIPPED rather than silently measuring
/// nothing, because "no output" and "no dataset" must not look alike.
async fn tpch_tables(dir: &str) -> Option<AHashMap<String, RecordBatch>> {
    const TABLES: &[&str] = &[
        "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
    ];
    let cap = seed_rows() + delta_rows() * ticks() + 1;
    let ctx = SessionContext::new();
    let mut out = AHashMap::new();
    for t in TABLES {
        let path = format!("{dir}/{t}.parquet");
        if std::fs::metadata(&path).is_err() {
            return None;
        }
        ctx.register_parquet(*t, &path, ParquetReadOptions::default())
            .await
            .ok()?;
        let batches = ctx
            .sql(&format!("SELECT * FROM {t} LIMIT {cap}"))
            .await
            .ok()?
            .collect()
            .await
            .ok()?;
        let schema = batches.first()?.schema();
        let one = arrow::compute::concat_batches(&schema, batches.iter()).ok()?;
        out.insert((*t).to_owned(), one);
    }
    Some(out)
}

fn median(mut xs: Vec<Duration>) -> Option<Duration> {
    xs.sort();
    xs.get(xs.len() / 2).copied()
}

/// Seed, then time `TICKS` ticks. `diff_based` selects the comparison arm.
///
/// Returns `Err(cause)` when anything fails — a tick that errors is not a tick
/// that is fast, and reporting its duration as a measurement would be the
/// exact dishonesty this corpus exists to prevent. The cause is CARRIED, not
/// collapsed to a bare "error": the first version of this harness swallowed
/// six queries' failures behind one word and made them indistinguishable from
/// each other and from an engine defect.
async fn measure(
    sql: &str,
    declared: SchemaRef,
    sources: &[&'static str],
    rows: &RowSource,
    diff_based: bool,
) -> Result<(Duration, bool), String> {
    let flow = IncrementalFlow::new();
    if diff_based {
        flow.force_diff_based()
            .map_err(|e| format!("force_diff: {e}"))?;
    }
    flow.register_view(IncrementalViewSpec {
        name: "v".into(),
        body_sql: sql.to_owned(),
        output_schema: declared,
        is_materialized: true,
        is_recursive: false,
        lateness: Vec::new(),
    })
    .map_err(|e| format!("register: {e}"))?;

    let mut cursor = rows.new_cursor();

    // The side input is fed once, before any timed tick.
    if matches!(rows, RowSource::Nexmark) && sql.to_lowercase().contains("side") {
        let d = DeltaBatch::from_inserts(side_batch()).map_err(|e| format!("side delta: {e}"))?;
        flow.feed("side", d)
            .map_err(|e| format!("feed side: {e}"))?;
    }
    for s in sources {
        let Some(b) = cursor.next(s, seed_rows()) else {
            continue;
        };
        let d = DeltaBatch::from_inserts(b).map_err(|e| format!("seed delta {s}: {e}"))?;
        flow.feed(*s, d)
            .map_err(|e| format!("feed seed {s}: {e}"))?;
    }
    let seed = flow
        .step_datafusion()
        .await
        .map_err(|e| format!("seed step: {e}"))?;
    if !seed.errored_views.is_empty() {
        return Err(format!("seed tick: {:?}", seed.errored_views));
    }

    let mut samples = Vec::with_capacity(ticks());
    for _ in 0..ticks() {
        let mut fed_any = false;
        for s in sources {
            let Some(b) = cursor.next(s, delta_rows()) else {
                continue;
            };
            let d = DeltaBatch::from_inserts(b).map_err(|e| format!("delta {s}: {e}"))?;
            flow.feed(*s, d).map_err(|e| format!("feed {s}: {e}"))?;
            fed_any = true;
        }
        if !fed_any {
            // A tick with nothing fed measures the empty-tick path, not the
            // query — say so rather than reporting a flattering number.
            return Err("source data exhausted before the timed ticks finished; \
                        lower KRISHIV_BENCH_CORPUS_SEED/DELTA"
                .to_string());
        }
        let t0 = Instant::now();
        let summary = flow
            .step_datafusion()
            .await
            .map_err(|e| format!("step: {e}"))?;
        samples.push(t0.elapsed());
        if !summary.errored_views.is_empty() {
            return Err(format!("tick: {:?}", summary.errored_views));
        }
    }
    let incremental = flow
        .view_plan_classification("v")
        .ok()
        .flatten()
        .map(|(inc, _)| inc)
        .unwrap_or(false);
    let median = median(samples)
        .ok_or_else(|| "no timed ticks (KRISHIV_BENCH_CORPUS_TICKS=0?)".to_string())?;
    Ok((median, incremental))
}

/// Time one corpus. Shared by both arms so they cannot drift apart.
async fn run_corpus(
    label: &str,
    queries: Vec<(&'static str, &'static str)>,
    ctx: &SessionContext,
    rows: &RowSource,
    wins: &mut usize,
    losses: &mut usize,
) {
    let borrowed: Vec<(&str, &str)> = queries.iter().map(|(a, b)| (*a, *b)).collect();
    run_corpus_str(label, &borrowed, ctx, rows, wins, losses).await;
}

async fn run_corpus_str(
    label: &str,
    queries: &[(&str, &str)],
    ctx: &SessionContext,
    rows: &RowSource,
    wins: &mut usize,
    losses: &mut usize,
) {
    println!(
        "\n{label} corpus tick — seed {}/source, delta {}/source, median of {} ticks\n",
        seed_rows(),
        delta_rows(),
        ticks()
    );
    println!(
        "{:<32} {:>6} {:>12} {:>14} {:>9}",
        "query", "plan", "incr tick", "diff-based", "speedup"
    );
    println!("{}", "-".repeat(84));

    let only = std::env::var("KRISHIV_BENCH_CORPUS_ONLY").unwrap_or_default();
    for (name, raw) in queries {
        if !only.is_empty() && !name.contains(only.as_str()) {
            continue;
        }
        let sql = rewrite(raw);
        let Ok(df) = ctx.sql(&sql).await else {
            println!("{name:<32} {:>6}", "unplan");
            continue;
        };
        let declared: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let sources = sources_of(df.logical_plan());

        let inc = measure(&sql, declared.clone(), &sources, rows, false).await;
        let dif = measure(&sql, declared, &sources, rows, true).await;
        match (inc, dif) {
            (Ok((i, is_incremental)), Ok((d, _))) => {
                let ratio = d.as_secs_f64() / i.as_secs_f64();
                if is_incremental && ratio >= 1.0 {
                    *wins += 1;
                } else if is_incremental {
                    *losses += 1;
                }
                println!(
                    "{name:<32} {:>6} {:>10.2}ms {:>12.2}ms {:>8.2}x",
                    if is_incremental { "incr" } else { "diff" },
                    i.as_secs_f64() * 1e3,
                    d.as_secs_f64() * 1e3,
                    ratio
                );
            }
            (Err(e), _) | (_, Err(e)) => {
                println!("{name:<32} {:>6}  {e}", "ERROR");
            }
        }
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut wins = 0usize;
        let mut losses = 0usize;

        // ── NEXMark ──
        let ctx = schema_ctx().await;
        run_corpus(
            "NEXMark",
            corpus(),
            &ctx,
            &RowSource::Nexmark,
            &mut wins,
            &mut losses,
        )
        .await;

        // ── TPC-H ──
        // Dataset path comes from the SAME env var the other TPC-H benches
        // read, so there is one place to point at a dataset. Absent dataset =>
        // an explicit SKIPPED line: TPC-H delta-batch tick cost has never been
        // measured, and a silent omission would let that keep being true while
        // looking covered.
        let dir = std::env::var("KRISHIV_TPCH_DATA_DIR_SF1")
            .or_else(|_| std::env::var("KRISHIV_TPCH_DATA_DIR"))
            .unwrap_or_default();
        match if dir.is_empty() {
            None
        } else {
            tpch_tables(&dir).await
        } {
            Some(tables) => {
                let ctx = SessionContext::new();
                for (name, batch) in &tables {
                    ctx.register_batch(name, batch.clone()).unwrap();
                }
                let queries: Vec<(&'static str, String)> = TPCH_QUERIES
                    .iter()
                    .map(|q| (q.id, q.sql_at_scale(1.0)))
                    .collect();
                let borrowed: Vec<(&str, &str)> =
                    queries.iter().map(|(a, b)| (*a, b.as_str())).collect();
                run_corpus_str(
                    "TPC-H (SF1 slices)",
                    &borrowed,
                    &ctx,
                    &RowSource::Tpch(tables),
                    &mut wins,
                    &mut losses,
                )
                .await;
            }
            None => println!(
                "\nTPC-H — SKIPPED: no dataset. Set KRISHIV_TPCH_DATA_DIR_SF1 to a \n\
                 directory of <table>.parquet files (tpchgen-cli parquet -s 1 --output-dir=…).\n\
                 TPC-H delta-batch tick cost is UNMEASURED until this runs."
            ),
        }

        println!("{}", "-".repeat(84));
        println!(
            "incremental plans faster than recompute at this seed/delta: {wins}; \
             slower: {losses}"
        );
        println!(
            "NOTE: slower here is NOT by itself a defect. At the default seed the \n\
             delta is a large fraction of the state, and incremental maintenance \n\
             only wins asymptotically as state/delta grows. The defect signature is \n\
             an incremental tick that GROWS WITH THE SEED while the delta is held \n\
             fixed — measure it with KRISHIV_BENCH_CORPUS_SEED, not with this ratio."
        );
    });
}
