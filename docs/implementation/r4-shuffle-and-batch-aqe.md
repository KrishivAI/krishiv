# R4 Shuffle And Batch AQE Implementation Tracker

## Goal

Deliver the first serious Spark bottleneck mitigation layer: an independent shuffle service, distributed joins and aggregations, runtime statistics, partition coalescing, spill hooks, small-file planning, and skew detection.

R4 turns Krishiv's distributed batch execution from simple task distribution into a query engine that can manage expensive data movement intentionally.

## Scope

In scope:

- Independent shuffle service abstraction (`ShuffleStore` trait).
- Shuffle writer and reader APIs.
- Shuffle metadata model (partition availability tracking).
- Hash partitioning.
- Compression and spill hooks.
- Hash join, sort join, and broadcast join.
- Local pre-aggregation.
- Runtime operator and partition statistics.
- Adaptive partition coalescing.
- Small-file split planning.
- Skew detection baseline.
- Shuffle garbage collection and orphan detection.
- **Two shuffle durability modes:** `local` (default — local disk, no external dependency) and `object-store` (opt-in — upload to object store for crash resilience on long stages or preemptible nodes).

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
- [x] Write `docs/architecture/data-plane-transport.md`: decide Arrow Flight vs gRPC+Protobuf for shuffle and result data movement. **Decision: Arrow IPC for shuffle writes and Arrow Flight for shuffle reads/result transfer; vanilla gRPC+Protobuf remains for control plane only.** Rationale: gRPC+Protobuf requires full serialization round-trip for every RecordBatch; Arrow Flight transmits Arrow IPC frames directly, eliminating O(rows x columns) CPU overhead per partition.
- [x] Write `docs/architecture/shuffle-deployment-model.md`: define the two-mode shuffle model. **Default (`local`): executors write partitions to local disk and serve reads over Arrow Flight from their own disk; no object store required.** **Optional (`object-store`): executors write to local disk then upload to object store; Stage N+1 reads from object store; executor can die after upload without forcing Stage N re-run.** True hybrid (local reads + object store fallback simultaneously) is not implemented.
- [ ] Define shuffle service deployment mode for single-node and distributed execution per `docs/architecture/shuffle-deployment-model.md`.
- [ ] Define shuffle writer API (local staging write → finalize to configured `ShuffleStore`).
- [ ] Define shuffle reader API (Arrow Flight from executor local disk in `local` mode; Arrow Flight from object store in `object-store` mode).
- [ ] Define shuffle metadata model (partition availability: Pending | Available | Failed, path, size_bytes).
- [ ] Define shuffle garbage collection policy for completed, failed, and cancelled jobs.
- [ ] Define orphan shuffle artifact detection model.
- [ ] Define stage retry lineage policy: on downstream stage failure, retain upstream shuffle output and re-run only the failed stage (Option B); on upstream stage failure, discard partial shuffle output and re-run from source. Document this explicitly.
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
- [ ] Expose Prometheus-compatible `/metrics` endpoint on coordinator (counters: `jobs_submitted_total`, `tasks_assigned_total`, `tasks_failed_total`, `shuffle_bytes_uploaded_total`, `shuffle_partitions_available_total`).
- [ ] Expose Prometheus-compatible `/metrics` endpoint on executor (counters: `tasks_running`, `shuffle_bytes_written_total`, `spill_bytes_total`).
- [ ] Expose `/healthz` and `/readyz` endpoints on coordinator and executor (standard Kubernetes liveness/readiness probes).

## Runtime Deliverables

- [ ] Implement hash partitioning.
- [ ] Implement shuffle write path: write Arrow IPC frames to local staging file per partition.
- [ ] Implement `local` shuffle finalization: atomically rename completed local staging file and mark partition Available in shuffle metadata.
- [ ] Implement optional `object-store` partition upload: on partition complete, upload local file atomically to the configured object store and mark partition Available.
- [ ] Implement shuffle read path: Arrow Flight server serving partition data from local executor disk in `local` mode and from object store in `object-store` mode.
- [ ] Implement shuffle metadata store (partition availability tracking in coordinator MetadataStore).
- [ ] Implement Stage N+1 wait: hold stage start until all required upstream partitions are Available in shuffle metadata.
- [ ] Implement shuffle cleanup for completed jobs (delete local shuffle paths or object-store prefixes for completed stages).
- [ ] Implement shuffle cleanup for failed and cancelled jobs.
- [ ] Implement orphan shuffle artifact detection (scan configured shuffle backend; delete artifacts without active job metadata after TTL).
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
- [ ] Shuffle metadata tests pass (partition transitions: Pending → Available → cleaned).
- [ ] Arrow Flight shuffle read returns correct Arrow RecordBatches from both local and object-store shuffle backends.
- [ ] Distributed join correctness tests pass.
- [ ] Distributed aggregation correctness tests pass.
- [ ] Broadcast join tests pass.
- [ ] Spill tests pass.
- [ ] Partition coalescing tests pass.
- [ ] Small-file planning tests pass.
- [ ] Skew simulation identifies hot partitions.
- [ ] Shuffle orphan cleanup tests pass.
- [ ] Stage retry lineage test: Stage 2 failure → Stage 2 retries reading same Stage 1 shuffle output (no Stage 1 re-run).
- [ ] Stage failure lineage test: Stage 1 failure → full re-run from source (partial shuffle output discarded).
- [ ] Executor crash in `local` mode: downstream stage detects lost partition owner and Stage N re-run recovers correctly.
- [ ] Executor crash mid-upload in `object-store` mode: partition is not marked Available; Stage N re-run recovers correctly.
- [ ] `/metrics` endpoint returns correct counters for coordinator and executor.
- [ ] `/healthz` and `/readyz` return 200 after startup; `/readyz` returns 503 before executor registration is complete.
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
- [ ] Stage retry lineage policy is documented and both retry scenarios pass tests.
- [ ] Executor crash mid-upload produces clean re-run without partial data reaching Stage N+1.
- [ ] `/metrics` and `/healthz` endpoints are live on coordinator and executor.
- [ ] `docs/architecture/data-plane-transport.md` is written and documents the Arrow Flight decision.
- [ ] `docs/architecture/shuffle-deployment-model.md` is written and documents local default plus optional object-store durability.
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
| Object-store upload latency blocks Stage N+1 start | Implement parallel partition uploads; Stage N+1 waits only for its specific required partitions, not all partitions |
| True hybrid shuffle is added prematurely | Do not implement simultaneous local plus object-store writes with fallback reads until benchmarks prove the complexity is needed |
| Arrow Flight adds dependency complexity | Arrow Flight is already a dependency via DataFusion; use the same version; do not introduce a second Flight version |
