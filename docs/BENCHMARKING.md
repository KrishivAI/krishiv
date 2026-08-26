# Benchmarking and Performance Evidence

Performance claims must be reproducible. A benchmark result without the source
revision, command, hardware, dataset, and configuration is diagnostic data—not
a published project claim.

## Do not benchmark a standard suite against the IVM path without checking the plan

**Measured, and gated: 19 of 44 registered verbatim.** `cargo test -p
krishiv-bench --test ivm_query_coverage -- --nocapture` classifies every query
in the committed TPC-H and NEXMark corpora. As of 2026-08-26:

| suite | verbatim (engine) | how |
|---|---|---|
| TPC-H | **5 / 22** (q1, q6, q12, q14, q5) | chains (DECOMP-3), join-leaf chains (DECOMP-4 + JOIN-2), and left-deep multi-way join runs with WHERE conjuncts distributed per level (MJOIN-1 — q5 is six tables, five join levels) |
| NEXMark | **14 / 22** (q0, q10, q14, q21, q22 + q3, q8, q20 + q1, q2, q7 + q15, q16, q17) | 5 single-operator maps + 3 band joins (BAND-1) + 3 TUMBLE windows (WINDOW-1 + UINT-1) + 3 statistics queries (CDIST-1: COUNT(DISTINCT col) via per-value multiplicity shared with MIN/MAX's multiset) |

Since DECOMP-3 the planner cuts a linear single-table multi-operator query into
a `ViewPlan::Chain` at plan time: every hop must classify `Incremental` or the
whole chain is refused (one DiffBased hop mid-chain forces its upstream to
full-recompute every tick, so a partially cut query is slower than an uncut
one). The chain checkpoints, restores, seeds and attaches to executors through
the one registered view — no generated hop views. `ivm_decomposition.rs`
proves corpus q1 and q6 **registered verbatim** agree exactly with
`force_diff_based` recompute under text canonicalisation, and
`decomposed_chain.rs` pins lossless checkpoint/restore with duplicate rows.

The test asserts those counts **exactly**, not as a floor. A `>=` floor hides
the failure this repo keeps hitting: a change that fixes one query while
breaking another nets to zero and reads as "no regression".

### Why TPC-H is zero, and why that is not a decimal problem

It was a decimal problem until IVM-AUD-DEC-1; it no longer is. `Decimal128`
aggregates are now exact (`i128` accumulation, DataFusion's own result types,
verified against the differential oracle). TPC-H is still zero for a structural
reason: **the IVM planner builds one operator per view.** Every TPC-H query
composes several — join, then aggregate, then order — and q21 nests seventeen
deep. Handed such a query verbatim, the planner finds nothing it can match and
falls to `DiffBased`, which re-runs the whole view SQL each tick and diffs it:
strictly *slower* than simply running the query, and not incremental
maintenance in any sense.

### The number you may have seen quoted, and what it actually means

A larger figure — 28 of 51 — has been quoted for this engine. It measures a
different thing: what a person can reach by **hand-decomposing** each query
into a chain of single-hop views. That capability is real; the flow maintains
view-over-view chains and orders them by dependency. It also took **166
hand-built views to cover 28 queries.**

Both numbers are true. Only one is a statement about the engine:

- *"5 of 44 standard queries maintain incrementally"* — what the engine does
  when handed a query.
- *"28 of 51 can be expressed as DAGs of incremental views"* — what a person
  can build out of the engine's operators, at 166 views.

Never quote the second without the first. The gap between them is a single
missing capability: **automatic decomposition of a multi-operator query into a
DAG of internal views.** That is the highest-value item on the IVM roadmap, and
until it exists, "TPC-H on IVM" means a human wrote 166 views.

**Rule: any benchmark that claims to measure incremental maintenance must assert
its views are actually executing incrementally, and fail if they are not.**
`ivm_vs_full_recompute` does this in `require_incremental_plan()` — call
`view_plan_classification` and panic on `incremental == false`. A benchmark
without that assertion is not evidence about IVM. This is not hypothetical: the
`ivm_vs_full_recompute` header asserted a wrong mechanism and a wrong magnitude
for over a year (IVM-AUD-PERF-1).

## Benchmark suites

- Criterion microbenchmarks: `cargo bench -p krishiv-bench`
- TPC-H batch SQL harness: `just bench-tpch`
- Nexmark streaming harness: `just bench-nexmark`

Use generated data only for development. Published comparisons must state the
dataset generator/version, scale factor, storage format, partitioning, and
whether caches were warm.

## Reproducible run

Create a machine-readable environment record before the run:

```bash
python3 scripts/benchmark_manifest.py --suite criterion \
  --command "cargo bench -p krishiv-bench" \
  --output target/benchmark-manifest.json
cargo bench -p krishiv-bench
```

The manifest records the commit, dirty-worktree state, Rust version, operating
system, CPU, suite, command, and UTC timestamp. Add workload-specific settings
such as scale factor, object-store endpoint, executor count, slots, memory, and
checkpoint interval to the result notes.

## Pull-request policy

- Correctness tests always take precedence over benchmark improvements.
- A statistically credible regression in a critical operator requires an
  explanation or a follow-up issue before merge.
- Do not compare unlike hardware, dependency versions, datasets, or execution
  modes as if they were equivalent.
- Benchmark artifacts are retained by CI for inspection. The nightly
  regression gate (below) is a permanent historical performance database
  for the budgets it tracks; everything else still relies on artifact
  retention only.

## Regression budgets (Phase 66)

`benchmarks/budgets.json` declares a latency budget per tracked benchmark
path; `benchmarks/results.jsonl` is the append-only measurement history
(one JSON object per run: `path`, `value_ms`, `commit`, `date`, `env`).
`scripts/bench_gate.py` (ported from the platform repo's Phase 29 gate —
same semantics, reused rather than reinvented) flags a path whose latest
result exceeds its budget by more than 20%, and fails only on a **sustained**
breach (two consecutive measured runs both over budget) — a single spike on
shared CI hardware warns, it does not fail the build.

`scripts/bench-tier.sh` runs the krishiv-bench targets that need no external
dataset (`streaming_latency`, `ivm_vs_full_recompute`, `nexmark`), reads each
tracked result straight out of criterion's own
`target/criterion/<group>/<id>/new/estimates.json`, and appends it to
`benchmarks/results.jsonl`. `.github/workflows/bench.yml`'s
`regression-gate` job runs this nightly (`workflow_dispatch` also works for
an on-demand run), commits the updated history back to `main` so the
sustained-breach check has real consecutive data to compare (a gap the
platform repo's own equivalent job has today — its history file is never
committed back, so every nightly run there compares against the same stale
baseline instead of the previous night), and opens a tracked `performance`-
labeled issue on a sustained breach.

`scripts/bench-datasets-tier.sh` (the `dataset-tier` job in the same
workflow) covers the two dataset-backed targets that are small enough for
CI to provision itself: `tpch_distributed` over TPC-H SF1 (`tpchgen-cli`)
and `tpcds_smoke` over TPC-DS SF1 (`scripts/bench/gen_tpcds_sf1.py`, DuckDB
`dsdgen`). Both generators are deterministic per version and scale factor,
and the generated data is cached between nights, so day-over-day rows
measure the engine, not the data. Same append → commit-to-main → gate
pipeline as the nightly tier, evaluated with `--tier datasets
--require-fresh 8` against the `"tier": "datasets"` budgets in
`benchmarks/budgets.json`. The tpch_distributed budgets are bootstrap
values (no ci-shared measurement existed when they were declared — each
budget's note records the derivation) and the TPC-DS budgets start at the
60 s `QUERY_TIMEOUT_MS` contract from `krishiv-bench/src/tpcds.rs`; both
are to be re-baselined at ~1.5x the observed medians once the first
committed week of ci-shared history exists.

**The gate has been proven to fail when it should**:
`scripts/plant_regression_demo.sh` is the planted-regression proof. It runs
the smallest gated bench (`streaming_latency_embedded`) for a clean
baseline, re-runs it twice with `KRISHIV_BENCH_PLANT_REGRESSION_MS=25` —
the bench harness's own gate-self-test hook in
`benches/streaming_latency.rs`, which sleeps that long inside every timed
iteration (two planted runs because a single breach only warns; the FAIL
rule requires a sustained, 2-consecutive-runs breach) — and exits 0 only
when `bench_gate.py` flags the plant as a SUSTAINED breach. Entirely
local: the gate runs against a scratch copy of its own layout, and nothing
under `benchmarks/` is touched.

**What this gate does not cover yet**: `tpch_sf10` and `tpch_overhead`
need `KRISHIV_TPCH_DATA_DIR_*` pointing at pre-generated multi-GB data
that CI does not provision — they self-skip (stderr notice) rather than
fail when unset, which is correct behavior for the bench itself but means
declaring a budget for them today would either go permanently "NO DATA
YET" or, worse, permanently fail `--require-fresh` for an infrastructure
reason having nothing to do with performance. Those stay manual (`just
bench-tpch`, `scripts/bench-tpch-tier.sh`) until a runner with the
datasets is wired in — tracked, not silently dropped.

## Publishing comparisons

When comparing Krishiv with Spark, Flink, or another engine, publish all engine
versions, equivalent semantics, configuration files, queries, raw output, and
reproduction commands. Clearly separate batch latency, streaming throughput,
checkpoint cost, recovery time, and resource consumption.

**Status 2026-08-08**: real same-hardware comparisons now exist (they were run
2026-07-26 and 2026-08-04 but this section kept claiming they didn't — that
stale claim cost nothing except making the numbers below invisible). Raw
per-query output lives in `benchmarks/`:

| Setup (TPC-H SF100, 22 queries) | Krishiv | DuckDB | Spark 3.5.3 |
|---|---:|---:|---:|
| Single box, 8 cores (2026-07-26, `tpch-sf100-engine-comparison.json`) | 855.5 s (22/22) | **552.9 s** (22/22) | 1221.0 s (9/22 — not comparable) |
| 3-node k3s cluster (2026-08-04, `tpch-sf100-ab-B-4c122b5f.json` vs `tpch-sf100-spark-2026-08-04.json`) | **4274.8 s** (22/22) | — | 5053.9 s (22/22) |

Read it honestly: on the cluster Krishiv is **1.18×** faster than Spark
(commit `9dbfff0` — the previously-quoted 1.29× rested on a stale Krishiv
number and was corrected the day both sides were re-run together). On a
single box DuckDB is ~1.5× faster than Krishiv; that gap is real and is
what Phase 65/66's kernel work is aimed at. The single-box Spark column is
not a comparison — 13 of 22 queries failed in that harness configuration
and no effort was spent tuning it. Still missing (Phase 66 residuals):
DataFusion-CLI and Sail baselines, ClickBench, and per-engine config files
published alongside the raw JSON.

## Recorded baselines

Later phases must cite deltas against the most recent baseline in this
section. Do not overwrite old entries — append new dated entries so the
history stays comparable.

### 2026-07-11 — Phase 51 yardstick

- **Revision**: engine `a20f2788` plus the bench-harness additions committed
  with this entry (`tpch_overhead` target, IVM 10M ladder point).
- **Hardware**: AMD EPYC (KVM guest), 8 cores, 23 GiB RAM, single local SSD.
  Linux 7.0.0-27-generic, rustc 1.92.0, mold linker, `opt-level=3` +
  thin LTO. Machine otherwise idle during the run.
- **Datasets**: TPC-H Parquet generated by `tpchgen-cli` v3.0.0
  (`tpchgen-cli parquet -s {1,10} --output-dir …`), one file per table:
  SF1 ≈ 345 MB, SF10 ≈ 3.7 GB. Warm page cache (files freshly written).
- **Method**: criterion, 10 samples, 30 s target time (bench files under
  `crates/krishiv-bench/benches/`); full raw output + machine manifest
  archived at `target/bench-results-20260711/` on the run machine. Every
  TPC-H iteration constructs its session/cluster and registers the Parquet
  tables inside the timed region — the numbers are end-to-end
  cold-session latencies, not warm-plan-cache query times.

**TPC-H ladder** (`just bench-tpch`, seconds per iteration, mean ± stddev):

| Query | embedded SF1 | embedded SF10 | coordinated SF1 | coordinated SF10 |
|-------|-------------:|--------------:|----------------:|-----------------:|
| Q1    | 0.52 ± 0.02  | 5.50 ± 0.44   | 0.52 ± 0.08     | 4.93 ± 0.43      |
| Q3    | 0.60 ± 0.05  | 7.22 ± 0.35   | 0.58 ± 0.04     | 6.62 ± 0.31      |
| Q5    | 0.91 ± 0.05  | 12.25 ± 0.99  | 0.84 ± 0.04     | 11.42 ± 1.53     |
| Q6    | 0.58 ± 0.02  | 5.51 ± 0.32   | 0.63 ± 0.06     | 5.96 ± 0.37      |
| Q10   | 0.84 ± 0.06  | 9.39 ± 0.52   | 0.78 ± 0.04     | 8.82 ± 0.60      |
| Q18   | 0.91 ± 0.04  | 13.05 ± 0.64  | 0.91 ± 0.06     | 13.81 ± 0.62     |

**Engine-overhead microbenchmark** (`--bench tpch_overhead`, audit §2b —
same query, same files, three entry points; seconds per iteration):

| Query/SF | raw DataFusion | embedded | coordinated | embedded ÷ raw |
|----------|---------------:|---------:|------------:|---------------:|
| Q1 SF1   | 0.098          | 0.477    | 0.512       | 4.9×           |
| Q1 SF10  | 0.973          | 4.669    | 4.812       | 4.8×           |
| Q6 SF1   | 0.071          | 0.579    | 0.555       | 8.2×           |
| Q6 SF10  | 0.694          | 6.207    | 5.672       | 8.9×           |
| Q3 SF1   | 0.133          | 0.605    | 0.591       | 4.5×           |
| Q3 SF10  | 1.447          | 6.562    | 6.835       | 4.5×           |

Findings tracked from this entry:

1. **Batch engine tax is 4.5–8.9× over raw DataFusion, and it is not fixed
   setup cost — it scales with data.** Root cause: `SqlEngine::new()`
   deliberately defaults DataFusion `target_partitions` to 1
   (`crates/krishiv-sql/src/lib.rs`), while a raw `SessionContext` uses all
   8 cores; the worst ratios (scan-bound Q6) are close to the core count.
   The coordinated path adds almost nothing on top of embedded (−9 % to
   +5 %) — the tax lives in the embedded session defaults, not the
   cluster submission path. This is the tracked budget for the Phase 52
   batch-hot-path work (task #194); the target after that work is
   embedded ÷ raw ≤ 1.2× on this table.
2. **Single-node streaming latency misses its documented target on this
   hardware.** `streaming_latency` (10k-row batch, tumbling window):
   embedded 148 µs/batch (target < 1 ms — met), single-node
   11.7 ms/batch (target < 5 ms — **missed**, 2.3× over), shuffle IPC
   round-trip 79 µs. No distributed-placement latency bench exists yet.
   Both tracked for Phase 55 (task #195).
3. **IVM tick vs full recompute — crossover is now ≈ 0.7 M rows** (was a
   projected ≈ 23 M before the G14 per-flow `SessionContext` reuse fix).
   5 000-row delta feed vs from-scratch recompute of
   `SELECT region, SUM(amount) … GROUP BY region`, ms per tick:

   | Accumulated rows | IVM tick | full recompute |
   |------------------|---------:|---------------:|
   | 50 k             | 11.0     | 3.6            |
   | 200 k            | 12.8     | 7.1            |
   | 500 k            | 15.1     | 12.5           |
   | 1 M              | 15.7     | 28.4           |
   | 10 M             | 140.4    | 297.9          |

   The 10 M tick costs 9× the 1 M tick for the same 5 000-row delta — the
   step still has a state-size-dependent component. Tracked for the
   Phase 57 delta-batch tick mechanics work (task #196).
4. **`bench nexmark` — NOT comparable to Flink/Spark NEXMark, despite the
   name.** Historic figures (Q1 1.61 ms, Q2 4.86 ms, Q5 3.80 ms, Q8 1.66 ms
   per 100 k-row in-memory batch) are retained for internal
   regression-tracking only, and must not be quoted as NEXMark results.

   Two reasons, both structural:
   - It runs the queries as **batch** DataFusion over a fixed in-memory
     table. NEXMark is a *streaming* benchmark; run as a batch query it
     measures the query engine and none of the streaming behaviour —
     watermarks, out-of-order arrival, window closing — that the benchmark
     exists to exercise.
   - Its tables are **not the NEXMark schemas**. `Bid` is `(auction, price)`
     against a spec of `(auction, bidder, price, channel, url, dateTime,
     extra)`, and `Person` is absent entirely.

   For streaming NEXMark use the harness described below, which generates
   the standard entities and reports its coverage honestly.

5. **NEXMark streaming harness (`--bin nexmark_stream`)**: sustainable
   throughput, event-time latency percentiles, and a completeness gate,
   over a faithful generator (standard Person/Auction/Bid schemas, the
   1 : 3 : 46 event mix, seeded and reproducible, with injected
   out-of-orderness).

   **Coverage is 4 of 22 queries** — Q2, Q5, Q7-keyed and Q11 — and the
   harness prints that ratio on every run. The engine's streaming SQL path
   expresses single-column keyed windowed aggregation only: no stateless
   projection (Q0/Q1), no global aggregates (Q7 standard form), no
   composite grouping keys (Q15), no joins (Q3/Q4/Q8). A cross-engine
   comparison on this subset is legitimate **provided the subset is
   stated**; reporting it as "NEXMark" without that is not.

   The completeness gate is load-bearing rather than decorative: the
   run-loop egress buffer drops its OLDEST batches at a cap, so a
   throughput number taken without verifying output would measure how fast
   the engine can discard data, and would improve as it lost more.

```bash
cargo run --release -p krishiv-bench --bin nexmark_stream
```

Reproduce: generate the datasets, then

```bash
export KRISHIV_TPCH_DATA_DIR_SF1=…/tpch/sf1
export KRISHIV_TPCH_DATA_DIR_SF10=…/tpch/sf10
python3 scripts/benchmark_manifest.py --suite criterion \
  --command "just bench-tpch" --output target/benchmark-manifest.json
just bench-tpch                                   # ladder + overhead
cargo bench -p krishiv-bench --bench streaming_latency
cargo bench -p krishiv-bench --bench ivm_vs_full_recompute   # 10M point needs ~2 GB free RAM
cargo bench -p krishiv-bench --bench nexmark
```

### 2026-07-11 — Phase 52 #194 batch hot path (overhead budget closed)

- **Revision**: the Phase 52 Leg 4 commit carrying this entry. Same
  hardware, datasets, and method as the Phase 51 yardstick above;
  `tpch_overhead` re-run at SF1 only (medians below).
- **What changed**: (1) `SqlEngine::with_target_parallelism` was a no-op —
  it set a field the built `SessionContext` never saw, so every caller ran
  DataFusion at `target_partitions = 1`; it now writes through to the live
  session state. (2) `SqlEngine::new()` defaults to available CPU
  parallelism (`KRISHIV_TARGET_PARALLELISM` override); executor task
  engines scale down to their per-slot share. (3) The engine no longer
  forces `parquet.pushdown_filters = true` — attribution measured it at
  ~2.2× on scan-heavy Q6 (268 ms → 121 ms, SF1); parquet options now stay
  at DataFusion defaults, opt in per session via `SET`.

**Engine-overhead microbenchmark** (`--bench tpch_overhead`, SF1 medians,
seconds per iteration):

| Query/SF | raw DataFusion | embedded | coordinated | embedded ÷ raw |
|----------|---------------:|---------:|------------:|---------------:|
| Q1 SF1   | 0.097          | 0.091    | 0.126       | 0.94×          |
| Q6 SF1   | 0.076          | 0.067    | 0.094       | 0.87×          |
| Q3 SF1   | 0.130          | 0.128    | 0.223       | 0.98×          |

Findings tracked from this entry:

1. **The #194 budget (embedded ÷ raw ≤ 1.2×) is met** — embedded now sits
   at 0.87–0.98× raw DataFusion on all three shapes (was 4.5–8.9×).
2. **The coordinated hop is now the visible remainder**: +23 % to +71 %
   over embedded at SF1 (fixed per-job cost — spec build, coordinator
   lifecycle, result collection — that Phase 51 could not see under the
   4.5–8.9× session tax). Tracked as input to the Phase 53 scheduler-v2
   work (task #175/#199).

### 2026-07-21 — Phase 66 #208: post-Phase-57 IVM re-benchmark

- **Revision**: engine `301a3f9e` plus the `benchmarks/`/`scripts/bench-tier.sh`
  regression-gate addition committed with this entry. Same hardware class
  and method as the Phase 51 yardstick (AMD EPYC, 8 cores, KVM guest,
  rustc 1.92.0). `ivm_vs_full_recompute` run twice back-to-back this pass
  (once standalone, once as part of `scripts/bench-tier.sh`'s real run);
  the table below is the **second** run only, kept internally consistent
  rather than mixed — see the variance note.
- **Why this entry exists**: Phase 57 (#179, closed 2026-07-13) shipped
  delta-batch tick mechanics fixes (task #196) whose own exit gate required
  "IVM beats full recompute at the recorded crossover ≤1M rows... result
  published in BENCHMARKING history" — but nobody ever re-ran this bench
  after #196 landed, so that exit-gate claim was never actually checked
  against fresh data. This is the first post-#196 measurement. `just
  bench-tpch`/`tpch_overhead` (TPC-H) were not re-run this pass — only the
  IVM ladder plus what `bench-tier.sh` covers (streaming_latency, nexmark),
  since the IVM ladder is what #196 and Phase 64's entry gate depend on.
- **Run-to-run variance on this shared VM is real and worth stating
  plainly**: the 10 M IVM-tick point read 38.5 ms, then 58.8 ms (mean-CI
  midpoint of the same run), then 64.6 ms on a second full run minutes
  later — all three well under the 2000 ms budget this path is gated on,
  but a ~1.7× spread on an identically-configured back-to-back rerun. Do
  not read single-sample precision into any number here; the qualitative
  findings below (10 M point improved substantially; crossover regressed
  past 1 M) hold across both runs even though the exact figures don't
  repeat.

**IVM tick vs full recompute**, 5 000-row delta feed vs from-scratch
recompute of `SELECT region, SUM(amount) … GROUP BY region`, ms per tick
(criterion median, second run — this is also what's seeded in
`benchmarks/results.jsonl`; Phase 51's 2026-07-11 numbers alongside):

| Accumulated rows | IVM tick (now) | IVM tick (2026-07-11) | full recompute (now) | full recompute (2026-07-11) |
|------------------|---------:|---------:|---------------:|---------------:|
| 50 k             | 11.68    | 11.0     | 6.21           | 3.6            |
| 200 k            | 11.67    | 12.8     | 6.73           | 7.1            |
| 500 k            | 14.41    | 15.1     | 9.54           | 12.5           |
| 1 M              | 13.91    | 15.7     | 11.92          | 28.4           |
| 10 M             | 64.62    | 140.4    | 93.66          | 297.9          |

Findings:

1. **The 10 M point improved substantially** (140.4 ms → 64.6 ms this run,
   or → 38.5 ms on the first run — 2.2×–3.6× depending on which sample)
   — task #196's delta-batch tick mechanics fix genuinely closed (or at
   least significantly narrowed) the state-size-dependent scaling problem
   the Phase 51 entry flagged ("the step still has a state-size-dependent
   component"). This is a real, previously unpublished win, even accounting
   for the run-to-run noise.
2. **The crossover point regressed and is not ≤1M rows today — Phase 57's
   own exit-gate number is not currently met.** At 1 M rows full recompute
   is still faster in both runs (11.92 ms vs 13.91 ms here, 13.07 ms vs
   14.74 ms on the first run — full recompute wins either way); at 10 M
   rows IVM is faster in both runs. The crossover is somewhere in
   (1 M, 10 M] rows, not ≈0.7 M as the Phase 51 entry reported — this
   qualitative conclusion is robust to the run-to-run noise even though the
   exact crossover row count isn't pinned. Root cause is likely **not** an
   IVM regression — `full_recompute` itself got faster at every scale below
   10 M too (plausibly Phase 52's batch-hot-path work, #194, which targeted
   exactly this raw-DataFusion path) — so the IVM side held roughly
   steady-to-improved in absolute terms while the competing baseline it's
   measured against also improved, moving the crossover the wrong way. Not
   root-caused further this pass; needs intermediate samples between 1 M
   and 10 M to pin the actual crossover row count, and a check of whether
   #194's fix touched the `full_recompute` code path directly. Recorded as
   a residual on #179 (Phase 57), not silently corrected in the task's
   "completed" status.
3. **This is also Phase 64's (#193) demand-trigger input.** Current data
   does not show a one-executor tick-latency budget breach at any sampled
   value (64.6 ms at 10 M rows vs the 2000 ms budget in
   `benchmarks/budgets.json`'s `ivm_tick_p50_at_10m_rows`, borrowed from
   the platform repo's `pipeline_tick_p50`) — the trigger does not fire on
   this data, and the ~1.7× run-to-run noise observed is nowhere near
   large enough to change that conclusion. This is the first time that
   question has been answerable at all (see task #193's entry gate).

This measurement (the 10 M point) now also feeds
`benchmarks/results.jsonl` via the nightly regression gate
(`ivm_tick_p50_at_10m_rows`) — see "Regression budgets (Phase 66)" above.

### 2026-07-21 — streaming_latency methodology fix (task #195 residual)

- **Revision**: engine `034187a3` (the `streaming_latency.rs` rewrite,
  `scripts/bench-tier.sh` fix, and the H-14 `emit_open_windows_speculative`
  wiring); `benchmarks/budgets.json`'s note update and the fresh
  `results.jsonl` rows below committed with this entry. Same hardware/method
  as the entries above (AMD EPYC KVM guest, 8 cores, rustc 1.92.0).
- **Why this entry exists**: the Phase 51 yardstick (above) found
  single-node streaming latency missing its 5 ms P99 target at 11.7 ms/batch
  (2.3× over), tracked as task #195/Phase 55. #195's actual functional work
  (early-fire wiring, resident IVM state, etc.) shipped across Phases 55–58
  without ever touching this specific benchmark number, so the finding sat
  undisturbed as a residual. Asked directly to fix it — not just document it
  — this is the root-cause investigation and fix.
- **Root cause was two bugs, not one**:
  1. The original benchmark dispatched through `run_job`, which per-job
     constructs a checkpoint service and (for single-node) opens the RocksDB
     state backend — both one-time costs for a job that then runs
     continuously for its whole lifetime. Measuring them inside a "per
     batch" timed closure charges an entire job's startup cost to a single
     batch.
  2. Fixing (1) by driving `ContinuousWindowExecutor::drain` directly still
     left a second, self-inflicted bug: that first rewrite timed an
     11-batch sequence as one criterion sample and compared the result
     against a per-batch target — and that sequence's timestamps jumped
     100,000 ms per batch against a 10,000 ms tumbling window, so (per
     `tumbling.rs`'s `window_end ≤ new_watermark_ms` close predicate, with
     this spec's default `watermark_lag_ms: 0`) nearly every batch closed
     the *previous* batch's window instead of quietly accumulating into
     one, contradicting the benchmark's own stated design. Caught before
     committing by re-deriving the window-close math from source rather
     than trusting the first rewrite's result.
- **Fix**: `streaming_latency_embedded`/`streaming_latency_single_node` each
  now warm up 9 batches (untimed, in criterion's setup closure) that tile
  `[0, 9_000)` of a single `[0, 10_000)` tumbling window without crossing
  its boundary, then time exactly one more same-window batch — the
  representative steady-state cost of updating already-known per-key state,
  genuinely comparable to the documented P99 targets.

**Streaming latency**, criterion median (Phase 51's differently-shaped
"10k-row batch via `run_job`" alongside for continuity — not a strict
apples-to-apples comparison, given the methodology changed; the real
comparison is against the budget, not the old number):

| Cell | Phase 51 (2026-07-11) | Now (2026-07-21) | Target |
|------|---:|---:|---:|
| embedded (1 batch)     | 148 µs  | 140 µs | < 1 ms (met both times)  |
| single-node (1 batch)  | 11.7 ms | 142 µs | < 5 ms (missed → met)    |
| shuffle IPC round-trip | 79 µs   | 90 µs  | (no budget declared)     |

Findings:

1. **The single-node P99 gap is closed, and it was never a real engine
   regression.** Both root causes were benchmark-methodology bugs
   (job-setup cost, then ladder-vs-single-batch conflation), not slow
   production code. Single-node now measures 142 µs against the 5 ms
   budget, a ~35× margin.
2. **Single-node is barely above embedded (142 µs vs 140 µs), not "a few
   milliseconds higher" as an earlier draft of this benchmark's own doc
   comment predicted.** This is empirical confirmation of
   `operator_runtime.rs`'s `open_state_backend` using `durable_fsync =
   false`: the state backend batches its WAL and only calls `sync()` once
   per checkpoint epoch, so an ordinary drain pays RocksDB's in-process API
   overhead but no synchronous disk flush. Checkpoint-time cost is a
   separate, still-unmeasured cost.
3. **Window-close/emit cost is a distinct, still-unmeasured cost.** Both
   benchmarks now deliberately avoid crossing a window boundary during the
   timed call (that's the steady-state/common case). The cost of the batch
   that actually closes a window — aggregation finalization, output
   `RecordBatch` construction — could legitimately be higher and is not
   covered by this entry. Flagged as a residual, not assumed negligible.
4. **shuffle IPC round-trip's ~90 µs reading is unchanged code, not a
   regression.** This function was not touched this session; its median
   moved 79 µs (Phase 51) → 80.5 µs → 90.1 µs across two more back-to-back
   runs today, from this shared VM's run-to-run scheduling noise (this box
   has separately been observed to swing much larger, up to ~1.7×, on
   heavier benchmarks — see the IVM entry above). Not investigated further;
   no budget is declared for this cell.

`benchmarks/budgets.json`'s `streaming_latency_single_node_p50` note is
updated to reflect the fix; `benchmarks/results.jsonl` gets fresh rows for
both `streaming_latency_*_p50`, tagged to this entry's commit.

### 2026-07-25 — TPC-H enters the committed history (Phase 62 GA gate)

TPC-H numbers previously lived only as one-off entries on this page, not in the
gate-checked `benchmarks/results.jsonl` history every other benchmark uses —
one of Phase 62's open deliverables. `scripts/bench-tpch-tier.sh` closes that:
same criterion → `results.jsonl` → `bench_gate.py` pipeline, same provenance
fields.

First honest run: dev-box (23 GB RAM, otherwise idle — no engine builds
running), embedded `SqlEngine` over the local Parquet dataset, criterion median
of 10 samples per query.

| Query | SF1 | SF10 | SF10/SF1 |
|---|---:|---:|---:|
| Q1 pricing summary | 97.4 ms | 965.6 ms | 9.9x |
| Q3 shipping priority | 156.0 ms | 1599.2 ms | 10.3x |
| Q5 local supplier volume | 233.7 ms | 2414.3 ms | 10.3x |
| Q6 forecasting revenue | 70.8 ms | 709.3 ms | 10.0x |
| Q10 returned item reporting | 164.8 ms | 1901.9 ms | 11.5x |
| Q18 large volume customer | 446.0 ms | 6572.0 ms | **14.7x** |

Five of six queries scale essentially linearly with the 10x data increase,
which is what a scan-bound plan should do. **Q18 is the outlier at 14.7x** and
is worth a look — it is the large-volume-customer query (`IN` subquery over a
grouped aggregate feeding a second grouped aggregate), so a superlinear
hash-aggregate or a spill at SF10 are both plausible. Recorded as an
observation, not diagnosed: nothing here has profiled it.

**Budgets** are declared at ~1.5x each measured median under `"tier": "tpch"`.
They are *machine-specific* — re-baseline on whichever host runs the tier
regularly rather than treating them as portable.

`bench_gate.py` gained `--tier` for this. `--require-fresh` treats an unmeasured
budget as a failure, so putting TPC-H budgets in the nightly set would have
failed CI for an infrastructure reason (no dataset in CI) rather than a
performance one. The nightly job now runs `--tier nightly --require-fresh 8`
and never sees TPC-H; the TPC-H tier runs `--tier tpch --require-fresh 30` on a
host that has the data. Verified both directions: deleting one TPC-H
measurement fails the tpch tier with `STALE` and leaves the nightly tier green.

### 2026-07-25 — TPC-H SF100 on the 3-node cluster (all 22 queries)

Two things changed with this entry: the corpus went from 6 hand-written
queries to **all 22**, and the benchmark moved from a single box to the
3-node k3s cluster with the dataset in object storage.

**Setup**

- **Cluster**: 3 × (4 vCPU, 7 GiB RAM) k3s nodes (s1/s2/s3), Ubuntu 26.04.
  One executor pinned per node (4/3/4 slots); coordinator co-resident. This
  pinning is deliberate — the default `replicas: 3` Deployment had stacked two
  executors on s1 and none on s3, which reads as a 3-node benchmark and is not
  one. See `deploy/k8s/bench/tpch-sf100-executors.yaml`.
- **Data**: TPC-H SF100 Parquet, `tpchgen-cli` v3.0.0, **39 GiB / 68 objects**
  in MinIO (`s3://krishiv-bench/tpch/sf100`). Big tables are split into parts
  (lineitem 32, orders 16, partsupp 8) so a scan has files to spread across
  executors; nation and region are single files.
- **Corpus**: `crates/krishiv-bench/src/tpch_queries.rs`, shared by the
  single-node and cluster runners via `tpch_corpus` so both execute identical
  SQL. All 22 verified executable against SF1 first
  (`cargo run -p krishiv-bench --bin tpch_verify`), with row counts matching
  the canonical answers (q9=175, q11=1048, q16=18314, q18=57, q22=7).
- **Runner**: `scripts/bench/tpch_cluster_run.py` — submits each query to
  `POST /api/v1/batch-sql/submit` and polls to completion.

**Honest characterisation of this operating point.** 21 GiB of cluster RAM
against a 39 GiB dataset means non-trivial queries spill; these are
spill-inclusive numbers, not in-memory ones. All three executors read from a
single MinIO pod, so the object store is a shared bottleneck — this measures
Krishiv-on-a-small-MinIO, not Krishiv against S3-class bandwidth. Both are
properties of this hardware, and neither is hidden by the numbers below.

**What this run found before it produced any timing.** Submitting the corpus
against `s3://` tables surfaced a chain of three defects, each hidden behind
the previous one:

1. The stage builder planned on a DataFusion context with no object store, so
   `register_parquet` on an `s3://` path failed schema inference. The caller
   reads any planning error as "decline to stage", so the job fell back to the
   single-task path — **the whole dataset scanned by one executor while the
   other two idled**, with correct results and no error. Measured cost on q6 at
   SF100: **518 s single-task**. Fixed in `5ce98ead`.
2. With staging fixed the fragment became a `dfplan:` stage, which then failed
   to decode: `No suitable object store found for s3://`. A serialized physical
   plan carries file paths but no way to register their store, and the executor
   only learns which buckets a plan touches *by decoding it* — so resolution has
   to be lazy. Fixed in `fcc48788` with `LazyCloudObjectStoreRegistry`.
3. Executors advertised their pod hostname, which nothing in the cluster
   resolves (no headless Service), so every task launch failed as a "transport
   error" and the job died after five *apparent* executor losses that were
   really DNS. Fixed in the manifest via `POD_IP` (downward API).

The first is the one worth remembering: **a distributed engine silently
degrading to single-node execution, with correct output and a debug-level log
line as the only evidence.** A benchmark is what caught it.

### 2026-08-08 — #179 residual: crossover pinned to (5M, 7.5M] (provisional)

- **Why**: the 2026-07-21 entry left the IVM-vs-full-recompute crossover
  "somewhere in (1M, 10M]" and asked for intermediate samples. This run adds
  them: 1M / 2M / 3.5M / 5M / 7.5M / 10M, via the new
  `KRISHIV_BENCH_IVM_ROWS` override on `ivm_vs_full_recompute` (comma list,
  committed with this entry).
- **Method caveat, stated up front**: same shared box as always, and this
  run had *substantial* concurrent load (a k8s chaos topology, an e2e
  harness, and a cargo build ran alongside). The 2026-07-21 entry measured a
  ~1.7× back-to-back spread on this box when otherwise idle; treat these
  numbers as ordering evidence, not magnitudes.

| Accumulated rows | IVM tick (median ms) | full recompute (median ms) | winner |
|---:|---:|---:|---|
| 1 M   | 8.4  | 5.5  | full |
| 2 M   | 16.8 | 8.6  | full |
| 3.5 M | 16.8 | 12.7 | full |
| 5 M   | 48.9 | 21.6 | full |
| 7.5 M | 20.1 | 42.0 | IVM  |
| 10 M  | 63.2 | 40.0 | full (inversion — see below) |

Findings:

1. **The crossover sits in (5M, 7.5M] on this run** — full recompute wins
   every sampled point through 5M, IVM wins at 7.5M. This sharpens
   2026-07-21's "(1M, 10M]" but is provisional until a quiet-box rerun:
   two points are visibly load-corrupted (IVM@5M > IVM@7.5M is
   non-monotonic; full@10M < full@7.5M likewise), and the 10M row
   contradicts both July runs (which had IVM winning 64.6 vs 93.7). The
   ordering at 1M–5M vs 7.5M held consistently, the corrupted points bound
   it from both sides.
2. **Phase 57's exit number (crossover ≤ 1M rows) remains unmet** — full
   recompute at 1M is ~1.5× faster than the IVM tick here, same direction
   as both July runs. `full_recompute` keeps getting faster
   (11.9 ms → 5.5 ms at 1M since 2026-07-21), consistent with the July
   hypothesis that #194's batch-hot-path work moved the baseline rather
   than IVM regressing. Whether #194 touched this exact path is still
   unverified — that inspection stays open on #179.
3. Next step for a non-provisional pin: rerun
   `KRISHIV_BENCH_IVM_ROWS=5000000,6000000,7000000,7500000` on an idle box.

### 2026-08-11 — Phase 65 oversubscription contract proven (all_slots_busy)

- **Revision**: engine `34346b2` (includes the five in-process concurrency
  fixes this bench flushed out — see below).
- **Hardware**: Intel i7-9750H, 12 threads (6 cores × SMT), 61 GiB RAM,
  Linux 7.0.0-29-generic, rustc 1.92.0, bench profile (opt-level=3 +
  thin LTO). Desktop box, niced run (`nice -n 5`).
- **Dataset**: TPC-H SF1 Parquet (`tpchgen-cli parquet -s 1`), warm cache.
- **Method**: `benches/all_slots_busy.rs` — task slots pinned to the core
  count, `KRISHIV_TARGET_PARALLELISM=1` (so slots × DF-partitions = cores
  exactly), 12 identical q1 queries released together per round through one
  shared `InProcessCluster`'s coordinator job path; 51 rounds → 612
  per-query wall-time samples per mode. Two separate processes because the
  compute pool sizes once per process: `serial` = `KRISHIV_COMPUTE_THREADS=1`,
  `pooled` = default auto-sizing.

**Per-query wall time (ms), every slot busy:**

| mode   | p50    | p95    | p99    | max    |
|--------|-------:|-------:|-------:|-------:|
| serial | 1322.2 | 2000.7 | 2117.6 | 2189.4 |
| pooled | 1340.8 | 1993.3 | 2099.6 | 2161.6 |

**Verdict: contract holds.** Pooled p99 is 0.9% *below* serial p99 (and
p95/max agree); the default pool sizing does not oversubscribe the machine
when every task slot is occupied. p50 +1.4% is within round-to-round noise.

The bench earned its keep before producing a number: getting 12 concurrent
drivers through one in-process cluster exposed five real concurrency bugs,
each fixed with a regression test — executor swept Lost by concurrent
drivers' ticks (`4598acc`), cross-driver row leakage from the shared report
stream (`f6bec6b`), premature job-done declaration (`932938c`), caller input
partitions never reaching tick-launched tasks + the quiescent exit firing
mid-stage-transition (`f34fd9c`), and a waiting driver never draining the
shared inbox, wedging at the stage cap (`34346b2`).

### 2026-08-11 — Phase 66 build-optimization ladder: fat-LTO / PGO / DataFusion-CLI

- **Revision**: engine `f58d203`. Same box as the all_slots_busy entry above
  (i7-9750H, 12 threads, 61 GiB, rustc 1.92.0), same TPC-H SF1 parquet.
- **Method**: `tpch_distributed` (criterion, 10 samples, 30 s target,
  cluster construction + table registration inside the timed region),
  three separate full builds in isolated target dirs, run sequentially on
  an otherwise-idle box: (A) the default bench profile (thin LTO,
  codegen-units=1); (B) `CARGO_PROFILE_BENCH_LTO=fat`; (C) PGO —
  `-Cprofile-generate` → exercise every query ~3 s → `llvm-profdata merge`
  → `-Cprofile-use` rebuild. DataFusion-CLI 54.1.0 (`cargo install`) ran
  the byte-identical SQL against the same files (DDL excluded from its
  timing; krishiv's numbers include session + registration, so the
  comparison slightly favors the CLI).

**Median ms per query (SF1):**

| Query | thin (baseline) | fat LTO | PGO | datafusion-cli 54.1 |
|-------|----------------:|--------:|----:|--------------------:|
| q1    | 112.7 | 115.0 | 118.2 | ~112 |
| q3    | 117.0 | 119.1 | 119.1 | ~113 |
| q5    | 148.6 | 159.9 | 156.1 | ~159 |
| q6    |  80.9 |  75.8 |  77.2 |  ~68 |
| q10   | 138.7 | 144.0 | 147.2 | ~144 |
| q18   | 200.9 | 202.6 | 196.8 | ~298 |

**Verdicts, honestly:**

1. **Fat LTO: not adopted.** −6.3% on scan-bound q6 but +7.6% on
   join-heavy q5 and ≤+4% drift elsewhere — no consistent win to justify
   the ~2× build-time cost. The current thin-LTO + codegen-units=1
   profile stays.
2. **PGO: not adopted at this scale.** Within ±5% of baseline on every
   query (−2% best case, q18). At SF1 the hot paths are already
   well-predicted; re-evaluate at SF10+ with a longer training run before
   writing it off for release artifacts.
3. **DataFusion-CLI baseline: parity.** The coordinator submission path
   costs nothing measurable vs the raw engine on 5 of 6 queries (q6's
   ~16% gap is the per-slot `target_partitions` share — the Phase 65
   elastic-DF-share item); q18 is 31% *faster* through krishiv, likely
   plan-config divergence worth a look rather than a claim.
4. **BOLT: blocked-on-root on this box** — `llvm-bolt` is not installed
   and there is no sudo; documented rather than silently skipped.

### 2026-08-11 — ClickBench first entry (krishiv CLI vs DataFusion-CLI)

- **Revision**: engine `f58d203` release binary (pre-elastic-share — the
  CLI's embedded path plans at full cores either way). Same i7-9750H box.
- **Dataset**: official single-file `hits.parquet` (14 GB, 99,997,497
  rows) from datasets.clickhouse.com; queries are DataFusion's own
  ClickBench variants (43 queries, apache/datafusion @ main).
- **Method**: one process per query invocation for BOTH engines (each
  pays startup + table registration inside the measured wall time),
  3 runs per query interleaved between engines, medians; raw CSV
  committed at `benchmarks/clickbench-2026-08-11.csv`.

**Result over the 36 comparable queries**: krishiv 148.3 s total vs
DataFusion-CLI 54.1 109.3 s — **1.36× overall**, per-query ratio median
1.23× (best 0.88× on q6, worst 1.53× on q28). The gap concentrates in
the string-heavy aggregation queries (q20–q28, q32–q34: 1.3–1.5×);
short scans are near parity.

**q36–q42 fail on BOTH engines with the identical error** (`Cannot cast
string '2013-07-01' to value of UInt16 type` — the official parquet
stores `EventDate` as UInt16 days-since-epoch and DataFusion's
simplify_expressions refuses the string comparison; krishiv inherits
this). Worth knowing: `datafusion-cli` exits 0 on a failed statement, so
a naive harness records those as 40 ms "wins" — this one checks output.
Not counted for either engine.

Honest read: the krishiv CLI carries a governed-session layer over the
same DataFusion 54 core, and on cold single-process invocations that
costs ~20-35% on heavy aggregations. The DuckDB gap (single-box §
2026-08-08) remains the real Phase 65/66 target; this entry exists so
the next optimization round has a committed ClickBench baseline to move.
