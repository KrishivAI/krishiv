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

- [x] Add `crates/krishiv-shuffle` (ShufflePath, PartitionState, ShuffleMetadata, ShuffleStore trait, InMemoryShuffleStore, LocalDiskShuffleStore with Arrow IPC, HashPartitioner, CompressionCodec, orphan detection).
- [x] Add `crates/krishiv-optimizer` (CostModel, OptimizerRule, AqeRule, StreamRule, SkewRule, Optimizer pipeline, ThresholdSkewRule, CoalesceRule).
- [x] Define optimizer rule trait in `krishiv-optimizer` (`OptimizerRule::name`, `OptimizerRule::apply`).
- [x] Define CBO cost model interface (`CostModel::estimate`).
- [x] Define AQE rewrite rule interface (`AqeRule::apply` with per-stage `RuntimeStats`).
- [x] Define stream planning rule interface (`StreamRule::apply`).
- [x] Define skew detection rule interface (`SkewRule::detect_hot_partitions`).
- [x] Write `docs/architecture/stage-local-execution.md`: coordinator partitions work into stages; each executor runs a full local DataFusion context for its assigned partitions; shuffle moves data between stages. No custom distributed physical operators needed.
- [x] Write `docs/architecture/streaming-execution-model.md`: continuous operator model, watermark protocol, state interaction contract, streaming job lifecycle. Approved for R5.1 implementation.
- [x] Write `docs/architecture/data-plane-transport.md`: Arrow IPC for shuffle writes and Arrow Flight for shuffle reads/result transfer; vanilla gRPC+Protobuf remains for control plane only.
- [x] Write `docs/architecture/shuffle-deployment-model.md`: default `local` (executor local disk + Arrow Flight serve), optional `object-store` (upload after local finalize); no true hybrid.
- [x] Define shuffle service deployment mode: single-node uses `InMemoryShuffleStore` (no Flight server needed); distributed uses `LocalDiskShuffleStore` + Arrow Flight server per executor. Documented in `docs/architecture/shuffle-deployment-model.md`.
- [x] Define shuffle writer API: `ShuffleStore::register_partition_lease` → `write_partition` → finalize (atomic rename in `LocalDiskShuffleStore`); lease-token zombie rejection before commit.
- [x] Define shuffle reader API: `ShuffleStore::read_partition` for in-process reads; `FlightPartitionClient` for cross-executor reads in distributed mode; both return Arrow `RecordBatch` streams.
- [x] Define shuffle metadata model: `PartitionState` (Pending | Available | Failed), `ShuffleMetadata` tracking per-`ShufflePath` state; `all_available` gate for Stage N+1.
- [x] Define shuffle garbage collection policy: `delete_job_partitions` on job Succeeded/Failed/Cancelled; orphan scan by TTL; `scan_orphans`/`cleanup_orphans` in `krishiv-shuffle`.
- [x] Define orphan shuffle artifact detection model: `scan_orphans(base_dir, active_job_ids)` returns paths without active job ownership; `cleanup_orphans` deletes them.
- [x] Define stage retry lineage policy (Option B): downstream stage failure → retain upstream shuffle output, re-run failed stage only; upstream stage failure → discard partial upstream shuffle output, re-run upstream from source. Documented in `docs/architecture/shuffle-retry-lineage.md`.
- [x] Define partitioning model: `HashPartitioner` hashes one key column into N buckets deterministically; same key column + partition count produces the same bucket assignment across stages.
- [x] Define spill boundary: operators exceeding `memory_limit_bytes` spill intermediate state to local shuffle directory; spill is transparent to coordinator and reported via `RuntimeStats::spill_bytes`.
- [x] Document shuffle recovery expectations: `local` mode — executor crash before finalize keeps partition `Pending`; coordinator triggers Stage N re-run via heartbeat timeout; `object-store` mode — crash after upload leaves partition `Available`, no re-run needed. Documented in `docs/architecture/shuffle-recovery-expectations.md`.

## API And Interface Deliverables

- [x] Add plan annotations for partitioning requirements (`PartitionSpec` on `PlanNode`: Hash, Broadcast, Single, RoundRobin).
- [x] Add plan annotations for broadcast eligibility (`broadcast_eligible` flag on `PlanNode`).
- [x] Add runtime stats model for partitions and operators (`RuntimeStats`: input_rows, output_rows, cpu_nanos, memory_bytes, spill_bytes).
- [x] Add `EXPLAIN` visibility for shuffle boundaries (shuffle boundary nodes printed in `LogicalPlan` display).
- [x] Add `EXPLAIN` output for optimizer rule decisions (via `OptimizeResult::describe()` and `applied_rules`).
- [x] Add cost model output to `EXPLAIN` for CBO-selected plans (`Cost` printed alongside plan nodes when CBO fires).
- [x] Add job metrics for shuffle bytes, partitions, spills, and skew (per-task `RuntimeStats` stored in `TaskRecord`, aggregated in `JobSnapshot`).
- [x] Expose Prometheus-compatible `/metrics` endpoint on coordinator (counters: `jobs_submitted_total`, `tasks_assigned_total`, `tasks_failed_total`, `shuffle_bytes_written_total`, `shuffle_partitions_available_total`).
- [x] Expose Prometheus-compatible `/metrics` endpoint on executor (counters: `tasks_running`, `shuffle_bytes_written_total`, `spill_bytes_total`).
- [x] Expose `/healthz` and `/readyz` endpoints on coordinator and executor binaries.

## Runtime Deliverables

- [x] Implement hash partitioning (`HashPartitioner` in `krishiv-shuffle`: Int32, Int64, Utf8 key columns; deterministic bucket assignment).
- [x] Implement shuffle write path: write Arrow IPC frames to local staging file per partition (`LocalShuffleStore::write_partition` → staging `.tmp` file).
- [x] Implement `local` shuffle finalization: atomically rename completed local staging file and mark partition Available in shuffle metadata (`LocalDiskShuffleStore::write_partition` atomic rename).
- [x] Implement optional `object-store` partition upload: `ObjectStoreShuffleStore` uploads local IPC file to object store after finalize; marks partition Available in shuffle metadata.
- [x] Implement shuffle read path: `FlightPartitionServer` in `krishiv-shuffle` serves Arrow IPC frames from `LocalDiskShuffleStore`; `FlightPartitionClient` reads them as `RecordBatch` streams.
- [x] Implement shuffle metadata in coordinator: `ShuffleMetadata` tracked per job in `JobRecord`; partition transitions (Pending → Available) written on `TaskSucceeded` update; Stage N+1 launch gated on `all_available`.
- [x] Implement Stage N+1 wait: `launch_assigned_task_assignments` skips downstream stages until all upstream shuffle partitions are Available; `StageSpec::upstream_stage_ids` declares dependencies.
- [x] Implement shuffle cleanup for completed jobs (call `delete_job_partitions` on job Succeeded/Failed/Cancelled path in coordinator).
- [x] Implement shuffle cleanup for failed and cancelled jobs.
- [x] Implement orphan shuffle artifact detection (scan configured shuffle backend; delete artifacts without active job metadata after TTL).
- [x] Add compression hooks (`CompressionCodec` enum: None, LZ4, Zstd; applied during `write_partition`).
- [x] Add spill hooks: executor spills to local shuffle dir when `memory_used_bytes > memory_limit_bytes * spill_threshold`; spill reported in `RuntimeStats`.
- [x] Implement local pre-aggregation: executor runs `GROUP BY` before shuffle-write to reduce output data volume; expressed as `pre-agg:` fragment prefix.
- [x] Implement hash join (two-stage plan: Stage 1 hash-partitions both inputs by join key; Stage 2 local DataFusion join on co-partitioned data).
- [x] Implement sort join (merge-join variant: Stage 1 hash-partitions + sorts; Stage 2 merge-join locally).
- [x] Implement broadcast join: small table broadcast to all executors via coordinator; large table scanned locally; no shuffle required.
- [x] Collect runtime statistics (per-task `RuntimeStats` from DataFusion execution metrics; stored in `TaskRecord`; fed to AQE rules).
- [x] Implement adaptive partition coalescing (physical: `CoalesceRule::advise()` output reshapes downstream stage task assignment to merge small partitions).
- [x] Implement small-file split planning (`SmallFilePlanner` splits Parquet files larger than `split_bytes` into multiple `InputPartition` entries at submit time).
- [x] Add skew detection baseline (`ThresholdSkewRule` detects hot partitions; coordinator logs skew warnings and emits skew metric).

## Test Checklist

- [x] Shuffle writer/reader unit tests pass (in `krishiv-shuffle`).
- [x] Shuffle metadata tests pass (partition transitions: Pending → Available → cleaned).
- [x] Arrow Flight shuffle read returns correct Arrow RecordBatches from local shuffle backend.
- [x] Distributed join correctness tests pass (two-stage hash join end-to-end).
- [x] Distributed aggregation correctness tests pass (pre-agg + shuffle + final agg).
- [x] Broadcast join tests pass (small table broadcast, large table local scan).
- [x] Spill tests pass for memory-heavy operators.
- [x] Partition coalescing tests pass (small partitions merged in downstream stage).
- [x] Small-file planning tests pass (large Parquet split into multiple InputPartition).
- [x] Skew simulation identifies hot partitions (ThresholdSkewRule).
- [x] Shuffle orphan cleanup tests pass.
- [x] Stage retry lineage test: Stage 2 failure → Stage 2 retries reading same Stage 1 shuffle output (no Stage 1 re-run).
- [x] Stage failure lineage test: Stage 1 failure → full re-run from source (partial shuffle output discarded).
- [x] Executor crash in `local` mode: downstream stage detects lost partition owner and Stage N re-run recovers correctly.
- [x] Executor crash mid-upload in `object-store` mode: partition not marked Available; Stage N re-run recovers correctly.
- [x] `/metrics` endpoint returns correct counters for coordinator and executor.
- [x] `/healthz` and `/readyz` return 200 after startup; `/readyz` returns 503 before executor registration is complete.
- [x] TPC-H smoke benchmark runs (SF1 correctness, all 22 queries).

## Usable Product Gate

Within R4, before the full acceptance gate, a Usable Product milestone must be reached and demonstrated:

- [x] Distributed batch SQL on Parquet + S3 runs end-to-end on real executors.
- [x] TPC-H SF10 runs correctly (correctness, not performance — every query produces the right result).
- [x] Kafka → Parquet pipeline runs end-to-end.
- [x] `krishiv submit`, `krishiv jobs`, and `krishiv cancel` work against a live Kubernetes cluster.
- [x] Published TPC-H SF10 result (raw numbers, not a claim of performance superiority).

After this gate is reached, the project should be made available for external feedback. Early users find bugs and clarify priorities that 6 months of internal testing will not.

## Acceptance Gate

R4 is complete when:

- [x] Usable Product Gate above passes.
- [x] Distributed joins and aggregations pass correctness tests.
- [x] Shuffle boundaries are visible in `EXPLAIN`.
- [x] Runtime stats show shuffle bytes and partition counts.
- [x] Spill tests pass for memory-heavy operators.
- [x] Skew simulation detects at least one hot partition scenario.
- [x] Orphan shuffle data is detected and cleaned up deterministically.
- [x] Stage retry lineage policy is documented and both retry scenarios pass tests.
- [x] Executor crash mid-upload produces clean re-run without partial data reaching Stage N+1.
- [x] `/metrics` and `/healthz` endpoints are live on coordinator and executor.
- [x] `docs/architecture/data-plane-transport.md` is written and documents the Arrow Flight decision.
- [x] `docs/architecture/shuffle-deployment-model.md` is written and documents local default plus optional object-store durability.
- [x] `docs/architecture/streaming-execution-model.md` is written and approved.

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
