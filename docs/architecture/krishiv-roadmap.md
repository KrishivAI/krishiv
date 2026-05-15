# Krishiv Hybrid Compute Framework: 10-Release Implementation Roadmap

## Executive Summary

Krishiv is a Rust-native hybrid compute framework for batch SQL, stateful streaming, and lakehouse pipelines. It combines Spark-style distributed SQL and adaptive batch execution with Flink-style event-time streaming, keyed state, checkpointing, and exactly-once-capable sink coordination.

Krishiv uses one shared planning and runtime model across embedded, single-node, and Kubernetes deployments. The same logical plan, optimizer, physical operators, state abstractions, connector contracts, and checkpoint semantics must apply across all supported modes.

Primary product goals:

- Provide a native Rust compute engine for data platform teams.
- Support batch and streaming as first-class execution modes from the start.
- Use Apache Arrow as the internal columnar data format.
- Use DataFusion as the SQL, expression, and local execution foundation.
- Support embedded and single-node execution in R1, then Kubernetes distributed execution in R2.
- Use a cell-based control-plane/data-plane architecture with exactly one active leader per job.
- Prioritize Parquet, Kafka, and S3-compatible object storage first.
- Defer Spark/Flink API compatibility until after the first 10 releases.

## Technical Architecture

```text
SQL CLI | Rust API | Python API | Flight SQL/JDBC Gateway
        |
Krishiv Session + Catalog
        |
Logical Plan
DataFusion plan + Krishiv batch/stream/state/lakehouse extensions
        |
Optimizer
CBO + AQE + stream planner + skew/backpressure/resource rules
        |
Physical Plan
Batch operators | Streaming operators | Stateful operators | Lakehouse writers
        |
Execution Backend
Embedded | Single-node | Kubernetes distributed
        |
Runtime Services
Scheduler | Shuffle | State | Checkpoint | Governance | Observability
        |
Connectors
Parquet/S3 | Kafka/CDC | Iceberg/Delta | JDBC | Object sinks
```

### Core Architecture Decisions

- Arrow record batches are the in-memory and network data format.
- DataFusion provides SQL parsing, logical planning, expression evaluation, and local vectorized execution.
- Krishiv adds distributed scheduling, streaming semantics, state management, shuffle, checkpoints, connectors, Kubernetes operations, and governance.
- Batch and streaming jobs are both represented as DAGs. Batch DAGs are bounded and terminate; streaming DAGs are unbounded and checkpointed by epoch.
- Exactly-once is certified per source/sink/checkpoint combination. It is not promised globally.
- Embedded, single-node, and Kubernetes modes must remain semantically aligned for every supported feature.

## Control Plane And Data Plane

Krishiv should not use a classic long-term master/slave architecture, and it should not use full active-active multi-master scheduling for the same job. The recommended model is a cell-based control-plane/data-plane architecture with per-job active leaders.

```text
Control Plane
  API Server
  Resource Manager
  Scheduler
  Job Coordinators
  Metadata Store

Data Plane
  Executors
  Shuffle Service
  State Backend
  Connectors
```

Leadership model:

- API servers are active-active.
- Each job has exactly one active `JobCoordinator`.
- Standby coordinators may take over after lease expiry and fencing.
- Executors are replaceable and must not own durable job truth.
- Shuffle and state run as independent data-plane services.
- Metadata uses durable leases, checkpoint epochs, and fencing tokens.
- Large deployments are split into cells, where each cell manages a subset of jobs and executors.

Control-plane evolution:

| Release | Control-Plane Capability |
|---|---|
| R1 | In-process coordinator for embedded and single-node execution. |
| R2 | One active Kubernetes coordinator with many executors. |
| R3 | Durable metadata for job ownership, offsets, and task state. |
| R4 | Independent shuffle metadata and recovery hooks. |
| R5 | Per-job coordinator ownership for stateful jobs. |
| R6 | Checkpoint/savepoint metadata sufficient for coordinator recovery. |
| R7 | Resource manager, queues, admission control, and scheduler isolation. |
| R8 | Active-active API entry points for Python and Flight SQL clients. |
| R9 | HA coordinators, per-job leader election, leases, and fencing tokens. |
| R10 | Cell-based production control plane. |

## Repository Architecture

```text
krishiv/
  Cargo.toml
  crates/
    krishiv-api/          # public Rust Session/DataFrame/Stream APIs
    krishiv-cli/          # sql, submit, explain, jobs, savepoint, restore
    krishiv-sql/          # DataFusion integration and SQL compatibility
    krishiv-plan/         # logical/physical hybrid DAG model
    krishiv-optimizer/    # CBO, AQE, stream planning, skew rules
    krishiv-exec/         # Arrow physical operators
    krishiv-runtime/      # embedded/single-node/distributed runtime traits
    krishiv-scheduler/    # coordinator, job leaders, placement, retries, queues
    krishiv-shuffle/      # shuffle service, spill, partitioning
    krishiv-state/        # keyed state, RocksDB, TTL, inspection
    krishiv-checkpoint/   # checkpoints, savepoints, restore, epochs
    krishiv-connectors/   # Kafka, Parquet, S3, CDC, source/sink contracts
    krishiv-lakehouse/    # Iceberg first; Delta later
    krishiv-catalog/      # schemas, tables, stats, metadata adapters
    krishiv-udf/          # Rust/Python UDF, UDAF, UDTF contracts
    krishiv-proto/        # gRPC, Protobuf, Arrow Flight contracts
    krishiv-governance/   # audit, lineage, policy hooks
    krishiv-metrics/      # OpenTelemetry, runtime events, cost metrics
    krishiv-python/       # PyO3 bindings
  k8s/
    crds/
    operator/
    helm/
  docs/
    architecture/
    rfcs/
    sql-compatibility/
    user-guide/
  examples/
    embedded/
    batch-sql/
    kafka-windowing/
    cdc-lakehouse/
  tests/
    integration/
    fault-injection/
    connector-certification/
  benches/
    tpch/
    tpcds/
    nexmark/
```

## Public Interfaces

Public interfaces to define early:

- CLI: `krishiv sql`, `krishiv submit`, `krishiv jobs`, `krishiv explain`, `krishiv savepoint`, `krishiv restore`.
- Rust API: `Session`, `DataFrame`, `Stream`, `ExecutionMode`, `Watermark`, `StateSpec`, `SinkMode`.
- Kubernetes CRDs: `KrishivCluster`, `KrishivJob`, `KrishivCheckpoint`, `KrishivSavepoint`, `KrishivQueue`.
- Connector traits: `Source`, `Sink`, `Offset`, `CommitHandle`, `ConnectorCapabilities`.
- Runtime traits: `ExecutionBackend`, `TaskExecutor`, `StateBackend`, `ShuffleBackend`, `CheckpointStore`.

## Release Roadmap

| Release | Scope | Main Risk | Mitigation |
|---|---|---|---|
| R1 Foundation Alpha | Embedded and single-node local hybrid engine | Hybrid foundation becomes too broad | Restrict SQL/function coverage and keep streaming local/minimal |
| R2 Kubernetes Distributed Alpha | First distributed runtime | Scheduler instability | Static placement, one active coordinator, stage-level retry |
| R3 Connector Contracts | Production I/O baseline | Connector semantics diverge | Capability flags and certification tests |
| R4 Shuffle And Batch AQE | Spark bottleneck mitigation | Shuffle complexity | Isolate shuffle service and test deterministic failures |
| R5 Stateful Streaming Core | Flink-style stream processing | State correctness bugs | Deterministic replay, model tests, failure injection |
| R6 Checkpoints And Savepoints | Reliable stateful execution | Overclaiming exactly-once | Certify exactly-once per connector combination only |
| R7 Resource Governance And Adaptivity | Multi-tenant production control | Adaptive behavior destabilizes jobs | Conservative defaults, manual override, explainable decisions |
| R8 Lakehouse And Python Beta | Data platform usability | API surface grows too quickly | Mark Python/lakehouse APIs beta; freeze Rust core first |
| R9 Governance And Operations | Enterprise operations | Control-plane correctness under failover | Lease leadership, fencing tokens, durable ownership metadata |
| R10 GA Platform Release | Stable public platform | Performance gaps vs Spark/Flink | Publish benchmark matrix and optimize top regressions |

## Phase Checklists

### R1: Foundation Alpha

Scope: embedded and single-node local hybrid engine.

Features:

- Embedded Rust execution.
- Single-node local execution.
- SQL CLI and DataFrame API.
- Local batch execution over Parquet.
- Local memory stream source.
- Baseline `EXPLAIN`.
- SQL compatibility baseline.

Checklist:

- [ ] Create Rust workspace and core crate layout.
- [ ] Add `krishiv-api`, `krishiv-cli`, `krishiv-sql`, `krishiv-plan`, `krishiv-exec`, and `krishiv-runtime`.
- [ ] Implement embedded `Session`, `DataFrame`, and `Stream` API skeletons.
- [ ] Add single-node CLI binary.
- [ ] Implement `krishiv sql` for local SQL execution.
- [ ] Implement `krishiv explain` for logical and physical plan display.
- [ ] Implement `krishiv jobs` for local job listing.
- [ ] Integrate Arrow and DataFusion for local SQL over Parquet.
- [ ] Add local memory stream source for bounded and unbounded test streams.
- [ ] Define `ExecutionMode` with `Embedded`, `SingleNode`, and future `Distributed`.
- [ ] Document baseline SQL compatibility in `docs/sql-compatibility/`.
- [ ] Add SQL golden tests.
- [ ] Add embedded/single-node parity tests.
- [ ] Add example: `examples/embedded/`.
- [ ] Add example: `examples/batch-sql/`.

Acceptance gate:

- [ ] A user can run a local SQL query over Parquet.
- [ ] A user can run a simple in-memory stream pipeline.
- [ ] `krishiv explain` shows logical and physical plans.
- [ ] Embedded and single-node execution produce the same result for supported features.

### R2: Kubernetes Distributed Alpha

Scope: first distributed runtime.

Features:

- Coordinator service skeleton.
- Executor service skeleton.
- Static scheduler.
- Kubernetes `KrishivJob` CRD.
- Basic distributed DAG submission.
- Basic Web UI for job status.

Checklist:

- [ ] Add `krishiv-scheduler` crate.
- [ ] Add `krishiv-proto` crate for control-plane RPC contracts.
- [ ] Define coordinator service lifecycle.
- [ ] Define executor service lifecycle.
- [ ] Implement task registration and executor heartbeat.
- [ ] Implement static task placement.
- [ ] Implement task lifecycle states: pending, running, succeeded, failed, retrying.
- [ ] Implement stage-level retry.
- [ ] Add `KrishivJob` CRD.
- [ ] Add basic Kubernetes manifests under `k8s/`.
- [ ] Support distributed batch DAG submission.
- [ ] Support distributed streaming DAG submission with local-only state semantics.
- [ ] Add basic Web UI for job status, task status, and executor health.
- [ ] Keep one active coordinator only.

Acceptance gate:

- [ ] A simple distributed batch job can be submitted on Kubernetes.
- [ ] A simple distributed streaming job can be submitted on Kubernetes.
- [ ] Job/task status is visible through CLI or Web UI.
- [ ] Coordinator can retry failed tasks at stage level.

### R3: Connector Contracts

Scope: production I/O baseline.

Features:

- Connector traits.
- Parquet read/write.
- Kafka source/sink.
- S3-compatible object storage.
- Source offsets.
- At-least-once sink contract.
- CDC design.
- Connector certification test kit.

Checklist:

- [ ] Add `krishiv-connectors` crate.
- [ ] Define `Source` trait.
- [ ] Define `Sink` trait.
- [ ] Define `Offset` model.
- [ ] Define `CommitHandle` model.
- [ ] Define `ConnectorCapabilities`.
- [ ] Include capability flags: bounded, unbounded, rewindable, transactional, idempotent.
- [ ] Implement Parquet reader.
- [ ] Implement Parquet writer.
- [ ] Implement S3-compatible object store integration.
- [ ] Implement Kafka source.
- [ ] Implement Kafka sink.
- [ ] Add source offset tracking.
- [ ] Add schema registry abstraction.
- [ ] Add at-least-once sink contract.
- [ ] Write CDC design document under `docs/rfcs/`.
- [ ] Add connector certification test kit.

Acceptance gate:

- [ ] Parquet connector passes certification tests.
- [ ] Kafka connector passes certification tests for supported semantics.
- [ ] S3-compatible object store integration passes read/write tests.
- [ ] Every connector declares capability flags.

### R4: Shuffle And Batch AQE

Scope: Spark bottleneck mitigation.

Features:

- Independent shuffle service.
- Hash partitioning.
- Shuffle read/write.
- Spill hooks.
- Hash, sort, and broadcast joins.
- Runtime stats.
- Adaptive partition coalescing.
- Small-file planning.
- Skew detection.

Checklist:

- [ ] Add `krishiv-shuffle` crate.
- [ ] Define shuffle writer API.
- [ ] Define shuffle reader API.
- [ ] Define shuffle metadata model.
- [ ] Implement hash partitioning.
- [ ] Implement shuffle read/write path.
- [ ] Add compression hooks.
- [ ] Add spill hooks.
- [ ] Implement local pre-aggregation.
- [ ] Implement hash join.
- [ ] Implement sort join.
- [ ] Implement broadcast join.
- [ ] Collect runtime statistics for partitions and operators.
- [ ] Implement adaptive partition coalescing.
- [ ] Implement small-file split planning.
- [ ] Add skew detection baseline.
- [ ] Add deterministic shuffle failure tests.

Acceptance gate:

- [ ] Distributed join correctness tests pass.
- [ ] Distributed aggregation correctness tests pass.
- [ ] Spill tests pass.
- [ ] Skew simulation identifies hot partitions.
- [ ] Shuffle metadata remains recoverable after executor failure.

### R5: Stateful Streaming Core

Scope: Flink-style stateful stream processing.

Features:

- `key_by`.
- Event time and watermarks.
- Timers.
- Tumbling, sliding, and session windows.
- Keyed state API.
- In-memory and RocksDB state backends.
- State TTL.
- Stream-table join baseline.
- State inspection CLI.

Checklist:

- [ ] Add `krishiv-state` crate.
- [ ] Define keyed state API.
- [ ] Implement in-memory state backend.
- [ ] Implement RocksDB state backend.
- [ ] Implement state TTL.
- [ ] Implement `key_by`.
- [ ] Implement event-time timestamp assignment.
- [ ] Implement watermark propagation.
- [ ] Implement processing-time timers.
- [ ] Implement event-time timers.
- [ ] Implement tumbling windows.
- [ ] Implement sliding windows.
- [ ] Implement session windows.
- [ ] Implement stream-table join baseline.
- [ ] Add `krishiv state inspect`.
- [ ] Add deterministic replay tests.
- [ ] Add watermark/window correctness tests.

Acceptance gate:

- [ ] Recoverable stateful window aggregation behaves deterministically.
- [ ] Watermarks close windows correctly.
- [ ] State TTL removes expired state.
- [ ] State inspection can read supported state metadata.

### R6: Checkpoints And Savepoints

Scope: reliable stateful execution.

Features:

- Checkpoint epochs.
- Async incremental checkpoints.
- Savepoints.
- Restore.
- Rescaling metadata.
- Source offset coordination.
- Two-phase commit sink API.
- Certified Kafka transactions.
- State schema evolution baseline.

Checklist:

- [ ] Add `krishiv-checkpoint` crate.
- [ ] Define checkpoint epoch model.
- [ ] Define checkpoint metadata format.
- [ ] Implement async checkpoint coordinator.
- [ ] Implement incremental checkpoint metadata.
- [ ] Coordinate source offsets with checkpoint epochs.
- [ ] Coordinate state snapshots with checkpoint epochs.
- [ ] Coordinate sink commit handles with checkpoint epochs.
- [ ] Implement savepoint creation.
- [ ] Implement savepoint restore.
- [ ] Add rescaling metadata model.
- [ ] Add two-phase commit sink API.
- [ ] Add Kafka transaction support where certified.
- [ ] Add state schema evolution baseline.
- [ ] Add executor kill/restart tests.
- [ ] Add duplicate-output prevention tests.

Acceptance gate:

- [ ] A certified Kafka-to-object-store path survives executor restart without duplicate output.
- [ ] Savepoint restore resumes stateful execution.
- [ ] Failed checkpoints do not commit sink transactions.
- [ ] Completed checkpoints can be listed and inspected.

### R7: Resource Governance And Adaptivity

Scope: multi-tenant production control.

Features:

- Resource manager.
- Queues and priorities.
- Admission control.
- Quotas.
- Namespace isolation.
- Cost metrics.
- Credit-based backpressure.
- Source throttling.
- Hot-key splitting.
- Adaptive repartitioning.

Checklist:

- [ ] Add resource manager service.
- [ ] Define `KrishivQueue` CRD.
- [ ] Implement job queues.
- [ ] Implement job priorities.
- [ ] Implement admission control.
- [ ] Implement CPU and memory quota model.
- [ ] Implement namespace isolation model.
- [ ] Add runtime cost metrics.
- [ ] Implement bounded operator queues.
- [ ] Implement credit-based flow control.
- [ ] Implement source throttling.
- [ ] Detect slow sinks.
- [ ] Detect hot keys.
- [ ] Implement hot-key splitting.
- [ ] Implement adaptive repartitioning.
- [ ] Add manual override for adaptive behavior.
- [ ] Add explainable adaptive-decision logs.
- [ ] Add backpressure stress tests.
- [ ] Add quota/admission tests.

Acceptance gate:

- [ ] Overloaded jobs are throttled without destabilizing other jobs.
- [ ] Jobs above quota are rejected or queued.
- [ ] Hot-key tests show load reduction after splitting.
- [ ] Adaptive decisions are visible to operators.

### R8: Lakehouse And Python Beta

Scope: broader data platform usability.

Features:

- Python bindings.
- Vectorized Python UDFs.
- Rust UDF/UDAF/UDTF contracts.
- Iceberg read/write beta.
- Snapshot reads.
- Schema and partition evolution.
- Time travel.
- Flight SQL endpoint.

Checklist:

- [ ] Add `krishiv-python` crate with PyO3.
- [ ] Add Python `Session` binding.
- [ ] Add Python `DataFrame` binding.
- [ ] Add Python query execution smoke tests.
- [ ] Add vectorized Python UDF support over Arrow batches.
- [ ] Add UDF isolation boundary.
- [ ] Add `krishiv-udf` crate.
- [ ] Stabilize Rust UDF contract.
- [ ] Stabilize Rust UDAF contract.
- [ ] Stabilize Rust UDTF contract.
- [ ] Add `krishiv-lakehouse` crate.
- [ ] Implement Iceberg read beta.
- [ ] Implement Iceberg write beta.
- [ ] Implement Iceberg snapshot reads.
- [ ] Implement Iceberg schema evolution support.
- [ ] Implement Iceberg partition evolution support.
- [ ] Implement Iceberg time travel support.
- [ ] Add Flight SQL endpoint.
- [ ] Mark Python and lakehouse APIs as beta.

Acceptance gate:

- [ ] Python query smoke tests pass.
- [ ] Vectorized Python UDF tests pass.
- [ ] Iceberg snapshot read/write smoke tests pass.
- [ ] Flight SQL smoke tests pass.

### R9: Governance And Operations

Scope: enterprise operations.

Features:

- OpenTelemetry metrics, traces, and logs.
- OpenLineage-compatible events.
- Audit logs.
- RBAC/TLS.
- Policy hooks.
- Row/column masking hooks.
- HA coordinators.
- Per-job leader election.
- Leases and fencing tokens.
- Replay bundles.
- Plan diffing.
- Helm chart.

Checklist:

- [ ] Add `krishiv-metrics` crate.
- [ ] Emit OpenTelemetry metrics.
- [ ] Emit OpenTelemetry traces.
- [ ] Emit structured logs.
- [ ] Add `krishiv-governance` crate.
- [ ] Emit OpenLineage-compatible job/run/dataset events.
- [ ] Add audit logs for query execution.
- [ ] Add audit logs for job submit/cancel.
- [ ] Add audit logs for savepoint/restore.
- [ ] Add audit logs for admin actions.
- [ ] Add RBAC integration.
- [ ] Add TLS configuration.
- [ ] Add policy hook interface.
- [ ] Add row masking hook.
- [ ] Add column masking hook.
- [ ] Add HA coordinator deployment.
- [ ] Implement per-job leader election.
- [ ] Implement durable leases.
- [ ] Implement fencing tokens.
- [ ] Add replay bundle generation.
- [ ] Add plan diffing.
- [ ] Add Helm chart.
- [ ] Add Kubernetes `kind` e2e tests.
- [ ] Add leader failover tests.

Acceptance gate:

- [ ] Coordinator failover does not allow duplicate checkpoint ownership.
- [ ] Fencing tokens prevent stale coordinators from committing.
- [ ] OpenTelemetry signals are emitted for supported jobs.
- [ ] Audit and lineage events are emitted for supported actions.

### R10: GA Platform Release

Scope: stable public platform.

Features:

- Stable API policy.
- SQL/function compatibility matrix.
- Certified connector matrix.
- JDBC/ODBC gateway.
- CDC-to-lakehouse pipelines.
- Materialized views baseline.
- Data quality rules.
- Upgrade tests.
- Chaos suite.
- TPC-H, TPC-DS, and Nexmark benchmarks.

Checklist:

- [ ] Publish stable API policy.
- [ ] Publish SQL compatibility matrix.
- [ ] Publish function compatibility matrix.
- [ ] Publish connector certification matrix.
- [ ] Add JDBC gateway.
- [ ] Add ODBC gateway.
- [ ] Implement CDC-to-lakehouse pipeline template.
- [ ] Implement materialized views baseline.
- [ ] Add data quality expectation rules.
- [ ] Add rejected-row output support.
- [ ] Add dead-letter sink support.
- [ ] Add upgrade test suite.
- [ ] Add chaos test suite.
- [ ] Add TPC-H benchmark suite.
- [ ] Add TPC-DS benchmark suite.
- [ ] Add Nexmark benchmark suite.
- [ ] Publish benchmark report.
- [ ] Optimize top benchmark regressions before GA.
- [ ] Publish production hardening guide.

Acceptance gate:

- [ ] GA benchmark gates pass.
- [ ] Upgrade tests pass.
- [ ] Chaos suite passes.
- [ ] Certified connector matrix passes.
- [ ] Public API stability policy is documented.

## Cross-Cutting Risks And Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Hybrid engine scope grows too large | Delayed releases and unstable foundations | Keep each release narrow and preserve explicit acceptance gates |
| Single coordinator becomes bottleneck | Poor scalability and availability | Move from single coordinator to per-job coordinators and cell-based scheduling |
| Full multi-master causes correctness bugs | Duplicate checkpoint ownership or duplicate sink commits | Avoid active-active scheduling for the same job |
| Split-brain during failover | State corruption or duplicate output | Use durable leases, fencing tokens, and checkpoint epoch ownership |
| Shuffle overwhelms network or disk | Spark-like performance bottlenecks | Add push-style shuffle, partition coalescing, compression, spill, and skew detection |
| Stateful jobs grow too large | Slow checkpoints and recovery | Use pluggable state backends, TTL, incremental checkpoints, and tiered snapshots |
| Backpressure spreads through pipelines | High latency or stalled jobs | Add credit-based flow control, bounded queues, and source throttling |
| Connector semantics are inconsistent | Incorrect delivery guarantees | Require capability flags and connector certification |
| Exactly-once is overpromised | User trust and correctness risk | Certify exactly-once only for specific source/sink/checkpoint combinations |
| Python/lakehouse APIs destabilize core | Core runtime churn | Keep Rust core stable and mark Python/lakehouse APIs beta |
| Benchmark gaps vs Spark/Flink | Weak adoption | Publish transparent benchmarks and optimize top regressions |

## Test And Acceptance Strategy

Release-level testing:

- R1-R3: SQL golden tests, embedded/single-node parity tests, API tests, connector contract tests, Parquet/Kafka/S3 integration tests.
- R4: shuffle correctness, join correctness, spill tests, skew simulation, small-file planning tests, TPC-H smoke benchmark.
- R5-R6: watermark/window correctness, state replay, checkpoint restore, duplicate prevention, transactional sink tests, executor-kill recovery.
- R7: backpressure stress tests, quota/admission tests, hot-key tests, adaptive repartition tests, cost metric validation.
- R8: Python API tests, vectorized UDF tests, Iceberg snapshot/schema evolution tests, Flight SQL smoke tests.
- R9: Kubernetes `kind` e2e tests, RBAC/TLS tests, per-job failover tests, fencing-token tests, lineage/audit validation.
- R10: upgrade tests, chaos suite, connector certification matrix, TPC-H/TPC-DS/Nexmark performance gates.

Global acceptance rules:

- Embedded, single-node, and distributed execution must remain semantically aligned for supported features.
- Every connector must declare capability flags.
- Every connector must pass certification tests before being documented as supported.
- Exactly-once must only be documented for certified source/sink/checkpoint combinations.
- Control-plane failover must never allow two active job coordinators to commit the same checkpoint epoch.
- Every release must include runnable examples for its headline features.
- Every release must document known limitations.

## Assumptions

- Krishiv starts as a greenfield Rust monorepo.
- Kubernetes is the primary distributed production target.
- Embedded and single-node modes are supported for development, CI, edge workloads, and light production.
- Iceberg is prioritized before Delta Lake.
- Spark/Flink API compatibility is deferred beyond the first 10 releases.
- Full active-active multi-master scheduling is intentionally avoided.
- Krishiv uses active-active API servers with exactly one active leader per job.
- The roadmap is architecture-level but checklist-ready for future implementation work.

## Deferred Scope

- Spark-compatible API.
- Flink-compatible API.
- Delta Lake parity with Iceberg.
- GPU execution.
- Cost-based autoscaling across cloud providers.
- Managed cloud service packaging.
- Global multi-region active-active job execution.
