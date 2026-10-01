---
name: benchmarking
description: Use when timing anything in Krishiv — a before/after of one change, an engine comparison (DuckDB, Spark, Sail), a TPC-H/TPC-DS sweep, or a single query that "seems slower" — and before quoting any number as a result.
---

# Benchmarking

A single timing on this machine is not a result. One change here has produced
four contradictory single-sample deltas; only paired, interleaved runs with
answer digests have ever reproduced.

## The run

1. **Idle first.** Wait for no `cargo|rustc` and 1-minute load < 2; sample
   `/proc/loadavg` every 10 s into `load.log` for the whole run so
   contamination is visible afterwards. Nothing compiles while it runs.
2. **Detached** via gating's `gate.sh`; a run that dies with the session
   loses everything that was not checkpointed.
3. **Paired and interleaved.** A/B of a change: both binaries, alternating
   order per query, ≥3 rounds, median per query —
   `python3 skills/benchmarking/ab_krishiv.py OLD NEW corpus.json DATA --rounds 3`
   (the old binary is copied aside *before* rebuilding; `target/` is
   overwritten). Engines: `scripts/bench/tpch_compare_engines.py
   --interleave --repeat 3`.
4. **Checkpoint.** The engine harness writes `<out>.passes.json` after every
   pass; a hand-written loop must print one line per query, flushed.
5. **Digests.** Every result row hashed (`digest_scheme` in the JSON);
   `--compare-to <previous>.json` against the last run. A faster wrong answer
   is a bug, not a win. Krishiv↔Sail agree on all 22 TPC-H; both differ from
   DuckDB on q1 by rounding only.
6. **Record the switches.** `KRISHIV_*` / `SAIL_*` env goes into the JSON; a
   number with a flag flipped is a different measurement.

## Reporting

- Per query, with sample counts; a sum of medians; wins/losses over 10%.
- Memory when it matters: `ru_maxrss` per query (Sail peaked at 30 GB on q21
  and was OOM-killed; that is a result).
- `benchmarks/<name>.md` beside `benchmarks/<name>.json`; the `.md` has the
  table and the conditions, the `.json` has the digests.
- A regression found: quantify old vs new on the affected queries with a
  per-query `timeout`, then fix, then re-measure the same set.

## Known floors

12 cores, 61 GB, `/tmp` is tmpfs (another session's cargo target there once
took 24 GB of RAM). SF100 data `~/krishiv-bench-data/tpch/sf100`; TPC-DS SF1
`target/tpcds-sf1`. Sail: `~/krishiv-bench-data/sail-venv`, `local` mode,
`SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=true`.

## Red flags

| Thought | Reality |
|---|---|
| "One pass is enough to see the direction" | The direction flipped between passes here. Three, interleaved. |
| "I'll compile the fix while the baseline runs" | The baseline is now a build-contended number. Wait. |
| "The timings look the same, skip the digests" | q5 once returned in 21 s with a plan that later took 22 min; digests are what tie a time to an answer. |
