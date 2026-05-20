# Krishiv R10 Benchmark Targets

These numeric targets are the acceptance gate for R10 GA. They MUST be published and committed before R10 implementation begins. Results that miss a target block the GA tag.

---

## Reference Hardware

All targets are defined against this single-node reference configuration:

| Component | Specification |
|---|---|
| CPU | 8-core x86-64 (e.g., AMD EPYC 7302P or Intel Xeon E-2388G) |
| RAM | 32 GB DDR4 |
| Storage | NVMe SSD, ≥ 1 GB/s sequential read |
| Network | 10 Gbps (irrelevant for single-node embedded benchmarks) |
| OS | Linux kernel ≥ 5.15, ext4 or XFS filesystem |
| Rust toolchain | stable, release profile (`--release`) |
| DataFusion version | pinned in `Cargo.lock` |

Benchmarks run in **embedded mode** (no Kubernetes, no network shuffle) unless otherwise noted.

---

## TPC-H Targets (SF10)

Scale factor: 10 (≈ 10 GB raw data). Queries: all 22 standard TPC-H queries.

| Target | Value |
|---|---|
| Per-query wall time limit | ≤ 60 s each |
| Geometric mean across all 22 queries | ≤ 15 s |
| Total suite wall time | ≤ 600 s |
| Cold-start (first run, no OS cache) | per-query ≤ 90 s |

Queries are run with Parquet-formatted TPC-H data, no pre-partitioning. DataFusion filter and projection pushdown into Parquet row groups is expected.

---

## TPC-DS Targets (SF10)

Scale factor: 10 (≈ 10 GB raw data). Queries: all 99 standard TPC-DS queries.

| Target | Value |
|---|---|
| Per-query wall time limit | ≤ 120 s each |
| Geometric mean across all 99 queries | ≤ 30 s |
| Total suite wall time | ≤ 1800 s |

Queries that require features marked "no" in the SQL compatibility matrix are excluded from the gate and noted as skipped in the report.

---

## Nexmark Targets (Streaming, 1M Events)

Event volume: 1 000 000 events. Mode: embedded streaming (single-node, in-process Kafka harness).

| Target | Value |
|---|---|
| Q1–Q8 minimum throughput | ≥ 100 000 events/s end-to-end for each query |
| Q1–Q8 p99 latency | ≤ 500 ms |
| Q0 (passthrough) minimum throughput | ≥ 500 000 events/s |

Nexmark Q9–Q22 are run for completeness but are not hard-gate targets in R10.

---

## Benchmark Reporting Policy

Every benchmark run produces a report file at `docs/benchmarks/results-<version>.md` with:

- Krishiv version (git tag or commit SHA)
- DataFusion version
- Reference hardware attestation (actual vs. spec)
- Per-query wall time for every TPC-H and TPC-DS query
- Nexmark per-query throughput and p99 latency
- Geometric mean for each suite
- Pass / fail column for each target
- Any skipped queries with reason

Reports are committed to the repository alongside the GA tag. They are not generated during CI on every PR — only on release candidate runs and the final GA run.

---

## Regression Policy

Any query regressing more than **2×** versus the previous committed benchmark report requires:

1. A root-cause comment in the PR that introduced the regression.
2. Either a fix that restores performance within 2× before the GA tag, or an explicit waiver approved in `docs/benchmarks/waivers.md` with justification.

Performance improvements do not require documentation but should update the committed results report.

---

## Scale Factor Note

SF10 is the mandatory baseline for R10 GA. SF100 benchmarks are aspirational; they are run for informational purposes and results are published alongside SF10 results but are not a hard gate condition for v1.0.
