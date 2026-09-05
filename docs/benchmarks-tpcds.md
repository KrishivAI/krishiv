# TPC-DS: full 99-query run (derived benchmark)

**Result: 99 of 99 TPC-DS queries execute and return answers identical to
DuckDB over the same dataset.** One query (q17) returns zero rows at SF1 in
both engines; that is a property of the scale factor, not a failure.

This is **not an audited TPC result.** TPC-DS is a trademark of the
Transaction Processing Performance Council; an official result requires the
official kit and an audit. This run uses DuckDB's `dsdgen` implementation and
DuckDB's copy of the 99 query texts. Publish it as *derived from TPC-DS*, never
as a TPC-DS score.

## Provenance

| field | value |
|---|---|
| date | 2026-09-04, timings re-run 2026-09-05 |
| commit | `49249f4` plus the change committed alongside this document |
| scale factor | 1 |
| dataset | 24 tables, Parquet, `target/tpcds-sf1`, 371 MB |
| generator | DuckDB 1.5.5 `INSTALL tpcds; CALL dsdgen(sf=1)` |
| query texts | DuckDB 1.5.5 `tpcds_queries()`, all 99, verbatim |
| engine path | `krishiv sql --local`, release build |
| execution | **embedded, in-process** — one process, one DataFusion context, local disk. No coordinator, no executors, no shuffle, no object store. `--mode` was not passed so it defaulted to `Embedded`; `--local` selects `Session::execute_local`, which runs the in-process `SqlEngine` regardless of mode. These are NOT distributed numbers. |
| oracle | DuckDB 1.5.5 in-process over the identical Parquet files |
| machine | 12-CPU dev box, not otherwise idle |
| caches | cold process per query; OS page cache warm after first pass |

## How correctness is decided

Each query runs twice: once through the engine CLI, once through DuckDB against
the same Parquet files. Both result sets are canonicalised (floats rounded to
two decimals, rows sorted) and compared element by element. A query counts only
on an exact match. A query that errors, times out, or differs is recorded as
such and does not count.

The harness was negative-controlled: an unknown column exits non-zero and is
recorded as an error rather than silently passing.

## Timings

**The first published run was cold-cache and is superseded.** It read the 371 MB
dataset off disk for the first time while timing it, inflating engine numbers by
up to 6x (q72: 17.5 s cold vs 2.8 s warm). Those numbers are kept only as
`benchmarks/tpcds-sf1-99q-2026-09-04.*` for the record. Quote the warm run.

Warm run: every query executed once to warm the page cache, then timed three
times, best of three, for both engines.

| | krishiv | duckdb |
|---|---|---|
| total, 99 queries | 14.4 s | 6.6 s |
| median query | 107 ms | 44 ms |
| median ratio | **2.55x slower** | — |
| slowest | 0.79 s (q64) | 0.40 s (q67) |

The engine is faster on three queries (q22 0.83x, q72, q95) and slower on the
other 96. The spread is 1.2x to 5.1x (q25).

Ten queries account for 30% of total suite time: q64, q4, q67, q11, q23, q51,
q78, q14, q28, q72.

### What changed since the first warm run

Three optimizer changes, each verified to leave all 99 results identical:

| | suite total |
|---|---|
| first warm run | 21.4 s |
| + semi-join pushdown declines a probe that removes no rows | 19.8 s |
| + greedy join reordering (`KRISHIV_JOIN_REORDER`, on by default) | 17.0 s |
| + CTE materialisation (`KRISHIV_CTE_MATERIALIZE`, on by default) | 16.5 s |
| + CTE materialisation reaching subqueries, filters traced to the body | 14.9 s |
| + ROLLUP/CUBE/GROUPING SETS as one aggregate plus re-aggregation | **14.4 s** |

The second is q72 almost entirely. DataFusion 54 has no join-reordering rule, so
join order is `FROM`-clause order; q72 names `catalog_sales JOIN inventory` — a
fact-to-fact join on a non-key column — first, below every selective filter,
building a 15.29 M row intermediate that later joins reduce to 380.9 K. Ordering
the chain smallest-connected-first takes q72 from 2699 ms to 252 ms, past
DuckDB's 305 ms. Measured across all 99 the rule is +15.0% with 7 wins >10%,
4 losses >10% (worst 24 ms) and 88 neutral; outside q72 it is neutral.

Parquet filter pushdown, measured as a third arm, is **not** a suite-wide lever:
enabling `datafusion.execution.parquet.pushdown_filters` globally is worth 1.4%
(19823 ms → 19538 ms) because **51 of 99 queries lose more than 10% and only 10
win**, and the win was almost entirely q72's — which the join reorder now takes
by a different route. An earlier version of this document claimed a 15%
per-query saving; that figure was wrong.

The third: DataFusion inlines every CTE, so `WITH x AS (…)` referenced N times
runs N times — q23 scanned `store_sales` six times. A CTE referenced more than
once is now collected once and read from memory, unless its consumers filter it
(then each inlined copy's pushed-down predicate wins) or its body is a bare
scan (then caching forfeits pushdown). A CTE referenced from inside an
`EXISTS`, `IN` or scalar subquery counts, and a predicate only blocks caching
when it traces to the CTE's body through one alias and could be pushed into
it — a correlation, a join predicate or a filter above a window does not.
q95 4.11x, q14 3.57x, q36 2.05x, q27 1.92x, q23 1.79x, q47 1.75x; across all
99, +15.6% with the worst loss 32 ms. Single-query process only.

The fourth: DataFusion evaluates grouping sets by expanding every input row
once per set — a `ROLLUP` of four columns is five sets. The finest set is now
aggregated once, shared through the CTE cache, and each set re-aggregated from
it (`sum` of sums, `sum` of counts, `min`/`max`, `avg` as sum over count;
decimal averages and anything with `DISTINCT` decline). q22 3.14x, q67 1.29x.

Per-query numbers: `benchmarks/tpcds-sf1-99q-warm-rollup-2026-09-05.csv` and
`.json`, which also carry the timing with neither the cache nor the rewrite
and the per-query result-identity check. Earlier runs are retained as
`benchmarks/tpcds-sf1-99q-warm-ctemat2-2026-09-05.*`,
`benchmarks/tpcds-sf1-99q-warm-ctemat-2026-09-05.*`,
`benchmarks/tpcds-sf1-99q-warm-joinreorder-2026-09-04.*` and
`benchmarks/tpcds-sf1-99q-warm-2026-09-04.*`.

## Reproducing

```
python3 -m venv venv && venv/bin/pip install duckdb
venv/bin/python scripts/bench/gen_tpcds_sf1.py target/tpcds-sf1
cargo build --release -p krishiv --bin krishiv
venv/bin/python scripts/bench/tpcds_99q_verify.py   # correctness, all 99 vs DuckDB
```

## Before publishing

SF1 is a development scale. Rerun at SF10 or SF100 on an idle machine, and
state the scale factor in any claim. A "99 of 99" correctness statement holds
across scales only if it is re-measured at the scale you quote.
