# R4 Shuffle And Batch AQE Implementation Tracker

## Goal

Deliver the first serious Spark bottleneck mitigation layer: an independent shuffle service, distributed joins and aggregations, runtime statistics, partition coalescing, spill hooks, small-file planning, and skew detection.

R4 turns Krishiv's distributed batch execution from simple task distribution into a query engine that can manage expensive data movement intentionally.

## Scope

In scope:

- Independent shuffle service abstraction.
- Shuffle writer and reader APIs.
- Shuffle metadata model.
- Hash partitioning.
- Compression and spill hooks.
- Hash join, sort join, and broadcast join.
- Local pre-aggregation.
- Runtime operator and partition statistics.
- Adaptive partition coalescing.
- Small-file split planning.
- Skew detection baseline.
- Shuffle garbage collection and orphan detection.

Out of scope:

- Full adaptive query re-optimization for every operator.
- Push-based remote shuffle optimization if not needed for baseline.
- Streaming state repartitioning.
- Hot-key splitting for streaming workloads.
- Global cost-based optimizer maturity.

## Dependencies

- R2 distributed task scheduling exists.
- R3 connectors can read/write Parquet and S3/object data.
- Plan and runtime layers can express partitioning requirements.
- Basic job status can expose shuffle and stage metrics.

## Architecture Deliverables

- [ ] Add `crates/krishiv-shuffle`.
- [ ] Add `crates/krishiv-optimizer`.
- [ ] Define optimizer rule trait in `krishiv-optimizer`.
- [ ] Define CBO cost model interface.
- [ ] Define AQE rewrite rule interface.
- [ ] Define stream planning rule interface.
- [ ] Define skew detection rule interface.
- [x] Write `docs/architecture/stage-local-execution.md`: coordinator partitions work into stages; each executor runs a full local DataFusion context for its assigned partitions; shuffle moves data between stages. No custom distributed physical operators needed.
- [ ] Write `docs/architecture/streaming-execution-model.md` (if not already written as part of this release): continuous operator model, watermark protocol, state interaction contract, streaming job lifecycle. Must be approved before R5.1 begins.
- [ ] Define shuffle service deployment mode for single-node and distributed execution.
- [ ] Define shuffle writer API.
- [ ] Define shuffle reader API.
- [ ] Define shuffle metadata model.
- [ ] Define shuffle garbage collection policy for completed, failed, and cancelled jobs.
- [ ] Define orphan shuffle artifact detection model.
- [ ] Define partitioning model.
- [ ] Define spill boundary for memory-heavy operators.
- [ ] Document shuffle recovery expectations.

## API And Interface Deliverables

- [ ] Add plan annotations for partitioning requirements.
- [ ] Add plan annotations for broadcast eligibility.
- [ ] Add runtime stats model for partitions and operators.
- [ ] Add `EXPLAIN` visibility for shuffle boundaries.
- [ ] Add `EXPLAIN` output for optimizer rule decisions (which rules fired and why).
- [ ] Add cost model output to `EXPLAIN` for CBO-selected plans.
- [ ] Add job metrics for shuffle bytes, partitions, spills, and skew.

## Runtime Deliverables

- [ ] Implement hash partitioning.
- [ ] Implement shuffle write path.
- [ ] Implement shuffle read path.
- [ ] Implement shuffle cleanup for completed jobs.
- [ ] Implement shuffle cleanup for failed and cancelled jobs.
- [ ] Implement orphan shuffle artifact detection.
- [ ] Add compression hooks.
- [ ] Add spill hooks.
- [ ] Implement local pre-aggregation.
- [ ] Implement hash join.
- [ ] Implement sort join.
- [ ] Implement broadcast join.
- [ ] Collect runtime statistics.
- [ ] Implement adaptive partition coalescing.
- [ ] Implement small-file split planning.
- [ ] Add skew detection baseline.

## Test Checklist

- [ ] Shuffle writer/reader unit tests pass.
- [ ] Shuffle metadata tests pass.
- [ ] Distributed join correctness tests pass.
- [ ] Distributed aggregation correctness tests pass.
- [ ] Broadcast join tests pass.
- [ ] Spill tests pass.
- [ ] Partition coalescing tests pass.
- [ ] Small-file planning tests pass.
- [ ] Skew simulation identifies hot partitions.
- [ ] Shuffle orphan cleanup tests pass.
- [ ] TPC-H smoke benchmark runs.

## Usable Product Gate

Within R4, before the full acceptance gate, a Usable Product milestone must be reached and demonstrated:

- [ ] Distributed batch SQL on Parquet + S3 runs end-to-end on real executors.
- [ ] TPC-H SF10 runs correctly (correctness, not performance — every query produces the right result).
- [ ] Kafka → Parquet pipeline runs end-to-end.
- [ ] `krishiv submit`, `krishiv jobs`, and `krishiv cancel` work against a live Kubernetes cluster.
- [ ] Published TPC-H SF10 result (raw numbers, not a claim of performance superiority).

After this gate is reached, the project should be made available for external feedback. Early users find bugs and clarify priorities that 6 months of internal testing will not.

## Acceptance Gate

R4 is complete when:

- [ ] Usable Product Gate above passes.
- [ ] Distributed joins and aggregations pass correctness tests.
- [ ] Shuffle boundaries are visible in `EXPLAIN`.
- [ ] Runtime stats show shuffle bytes and partition counts.
- [ ] Spill tests pass for memory-heavy operators.
- [ ] Skew simulation detects at least one hot partition scenario.
- [ ] Orphan shuffle data is detected and cleaned up deterministically.
- [ ] `docs/architecture/streaming-execution-model.md` is written and approved.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Shuffle complexity slows the whole release | Isolate shuffle in `krishiv-shuffle` and keep the baseline narrow |
| Memory-heavy joins crash executors | Add spill hooks before optimizing join performance |
| Runtime stats are inaccurate | Validate stats in deterministic tests before using them for AQE |
| Adaptive coalescing changes query results | Treat AQE as physical-only; preserve logical semantics in golden tests |
| Small-file planning becomes object-store-specific | Keep split planning generic and move provider behavior behind object-store adapters |
| Optimizer rule ordering causes non-determinism | Define a fixed rule application order; log which rules fire for every plan |
| Shuffle artifacts leak after retry/cancel | Add ownership metadata, orphan detection, and deterministic cleanup tests |
