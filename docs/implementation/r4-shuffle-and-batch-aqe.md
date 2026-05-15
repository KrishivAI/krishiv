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
- [ ] Define shuffle service deployment mode for single-node and distributed execution.
- [ ] Define shuffle writer API.
- [ ] Define shuffle reader API.
- [ ] Define shuffle metadata model.
- [ ] Define partitioning model.
- [ ] Define spill boundary for memory-heavy operators.
- [ ] Document shuffle recovery expectations.

## API And Interface Deliverables

- [ ] Add plan annotations for partitioning requirements.
- [ ] Add plan annotations for broadcast eligibility.
- [ ] Add runtime stats model for partitions and operators.
- [ ] Add `EXPLAIN` visibility for shuffle boundaries.
- [ ] Add job metrics for shuffle bytes, partitions, spills, and skew.

## Runtime Deliverables

- [ ] Implement hash partitioning.
- [ ] Implement shuffle write path.
- [ ] Implement shuffle read path.
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
- [ ] TPC-H smoke benchmark runs.

## Acceptance Gate

R4 is complete when:

- [ ] Distributed joins and aggregations pass correctness tests.
- [ ] Shuffle boundaries are visible in `EXPLAIN`.
- [ ] Runtime stats show shuffle bytes and partition counts.
- [ ] Spill tests pass for memory-heavy operators.
- [ ] Skew simulation detects at least one hot partition scenario.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Shuffle complexity slows the whole release | Isolate shuffle in `krishiv-shuffle` and keep the baseline narrow |
| Memory-heavy joins crash executors | Add spill hooks before optimizing join performance |
| Runtime stats are inaccurate | Validate stats in deterministic tests before using them for AQE |
| Adaptive coalescing changes query results | Treat AQE as physical-only; preserve logical semantics in golden tests |
| Small-file planning becomes object-store-specific | Keep split planning generic and move provider behavior behind object-store adapters |
