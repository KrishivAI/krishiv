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
| date | 2026-09-04 |
| commit | `1a9451e` plus the join-reorder change committed alongside this document |
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
| total, 99 queries | 17.0 s | 6.6 s |
| median query | 111 ms | 43 ms |
| median ratio | **2.68x slower** | — |
| slowest | 1.05 s (q14) | 0.41 s (q67) |

The engine is faster on one query (q72, 0.83x) and slower on the other 98. The
spread is 1.4x to 5.6x (q27).

Ten queries account for 38% of total suite time: q14, q64, q95, q23, q4, q67,
q22, q47, q27, q11.

### What changed since the first warm run

Two optimizer fixes, each verified to leave all 99 results identical:

| | suite total |
|---|---|
| first warm run | 21.4 s |
| + semi-join pushdown declines a probe that removes no rows | 19.8 s |
| + greedy join reordering (`KRISHIV_JOIN_REORDER`, on by default) | **17.0 s** |

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

Per-query numbers: `benchmarks/tpcds-sf1-99q-warm-joinreorder-2026-09-04.csv`
and `.json`, which also carry the no-reorder timing and the per-query
result-identity check. The previous warm run is retained as
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
