# Performance

This document records how Krishiv is measured, what the current numbers are,
which engine decisions the measurements drove, and the disciplines that keep
a benchmark result meaningful. The numbers are point-in-time; the CSV/JSON
artefacts under `benchmarks/` and `docs/benchmarks-tpcds.md` are the record.

## Benchmarks and harnesses

| Suite | Harness | Purpose |
|---|---|---|
| TPC-DS SF1, 99 queries | `krishiv sql` against Parquet; DuckDB as the correctness oracle and speed reference | embedded query performance; every optimizer change re-verifies 99/99 result-hash match |
| TPC-H SF1–SF100 | `krishiv-bench`, `just bench-tpch`, `deploy/k8s/bench` (Spark comparison in `deploy/bench`) | embedded and distributed batch; the SF100 cluster runs are where the memory model was fixed |
| NEXMark | `just bench-nexmark` | streaming operators |
| IVM corpus | `krishiv-bench/tests` (TPC-H 22 + NEXMark) | O(Δ) plan coverage gate (41/44 verbatim) and tick latency |
| micro | `criterion` benches in individual crates | operator kernels, hash, Z-set consolidation |
| `bench.yml` | nightly against recorded baselines | regression tier |

`docs/BENCHMARKING.md` is the how-to.

## Current embedded result (TPC-DS SF1, 12-core workstation, warm best-of-3)

| | Suite total | vs DuckDB |
|---|---|---|
| before the 2026-09 optimizer work | 21.4 s | 3.2× |
| after (`46cd7d9`) | 13.5 s | 2.03× (DuckDB 6.6 s) |

Two queries (q72, q22) run faster than DuckDB. The remaining gap is parallel
efficiency on the tail — ~270 % CPU on 12 cores — not scan decode, measured
with the in-process phase-timeline probe.

## Decisions the measurements drove

| Decision | Evidence | Doc |
|---|---|---|
| `JoinReorder` (greedy, fires only when the written order is inverted) | q72 2655 → 280 ms; a pure size greedy regressed `store_sales ⋈ store_returns` 4× | `03` |
| CTE materialisation (repeated `SubqueryAlias` → partitioned `MemTable`, subqueries included) | multi-reference CTEs recomputed per reference; gated to the one-shot CLI process | `02` |
| ROLLUP/CUBE/GROUPING SETS rewrite to a finest aggregate + re-aggregation | grouping-set queries ran one aggregate per set | `02` |
| `repartition_file_min_size` 10 MiB → 1 MiB | dimension scans ran single-partition; joins waited on one core | `02` |
| `hash_join_single_partition_threshold` 1 MiB → 8 MiB (1 M rows) | filtered dimensions estimated above 1 MiB (default selectivity) forced repartitioning the fact side; three-way sweep | `02`, `03` |
| `parquet.pushdown_filters` **off** | oracle ceiling 5.6 % with no structural way to pick winners (R3-2 closed with data) | `02` |
| semi-join pushdown guarded by `probe_reduces_rows` and `probe_contains_join`; selective-dimension reduction **off** | 37× regressions when the probe did not reduce | `02` |
| executor memory fractions 0.6 / 0.15 / 0.05, 512 MiB reserve, page-cache eviction | SF100 executors OOM-killed 11/7/3 times with pools reporting headroom; 1.5–1.8 GB was page cache | `05` |
| `write_partition_stream` | one unspillable output partition saturated `FairSpillPool` availability for every consumer (q10, q21 "877 B" failures) | `06` |
| etcd on a dedicated runtime; IVM snapshots chunked | HA chaos gate: every coordinator frozen; 1.57 MiB snapshot rejected | `04` |
| `Trace` key index; chunked `SourceState` | probe cost was O(trace) and append was O(relation) while docs claimed O(Δ) | `09` |

## Measurement discipline

These are rules, because each was learned by publishing a wrong number:

1. **Paired, interleaved A/B.** Run A and B alternately on the same machine
   in the same session; one change once produced four contradictory results
   when measured in separate runs.
2. **Warm best-of-3**, per-query result hashes recorded, load average checked
   before and after; discard a run if another build or test suite overlapped.
3. **Benchmarks measure the binary, not the source.** Rebuild release before
   measuring; a default flip measured unchanged because the binary was stale.
4. **Detach long runs** (`setsid nohup … & disown` with a SUMMARY/ALLDONE
   file) so a tool timeout cannot kill them mid-suite.
5. **Correctness first.** 99/99 against DuckDB after every optimizer change;
   a speedup with a changed row is a bug.
6. **Read the artefact, not the terminal.** Check the CSV/JSON that was
   written; truncated output hid false claims.
7. **Timelines over totals.** Per-operator `ElapsedCompute` and start/end
   timestamps (`explain --analyze`, in-process probes) locate the phase that
   matters; suite totals only say whether to look.

## Capacity guidance

- Embedded: `KRISHIV_TARGET_PARALLELISM` = physical cores; memory is the
  process's; large joins spill.
- Executors: size pods by cgroup limits only — slots, pool, and parallelism
  derive from them (`05`). Adding slots divides the pool; add executors for
  more memory.
- Shuffle partitions: `2 × schedulable slots` bounded to `[2, 512]` (`03`);
  fewer, larger partitions beat many small ones until skew appears, which AQE
  then splits.
- Streaming: `KRISHIV_STREAM_PROFILE=throughput` for batchy sources; the
  default profile minimises latency (`08`).

## Related

- `docs/BENCHMARKING.md`, `docs/benchmarks-tpcds.md`, `benchmarks/`,
  `../engineering-log/crate-audit-register.md` §90–§97 (the per-decision
  measurements in full).
