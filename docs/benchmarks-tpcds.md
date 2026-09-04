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
| commit | 93cd52796ff59f00b054ab9e9c85ee5c19485a4c |
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
| total, 99 queries | 21.4 s | 7.2 s |
| median query | 116 ms | 49 ms |
| median ratio | **2.6x slower** | — |
| slowest | 2.80 s (q72) | 0.42 s (q67) |

The engine is slower on all 99 and faster on none. The spread is 1.4x (q87, q35,
q43) to 10.8x (q16, q72, q94).

Ten queries account for 43% of total suite time: q72, q14, q64, q67, q95, q23,
q4, q47, q22, q11.

Parquet filter pushdown, measured as a third arm: enabling it globally is a
wash (21.3 s vs 21.4 s) because it wins on 30 of 99 and loses on the rest.
Choosing it per query would give 18.3 s, a 15% suite-wide saving. Largest single
win is q72 at 2.04 s; largest loss is q64 at 0.46 s.

Per-query numbers: `benchmarks/tpcds-sf1-99q-warm-2026-09-04.csv` and `.json`.

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
