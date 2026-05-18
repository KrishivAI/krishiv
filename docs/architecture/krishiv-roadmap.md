# Krishiv Hybrid Compute Framework: 10-Release Implementation Roadmap

## Executive Summary

Krishiv is a Rust-native hybrid compute framework for batch SQL, stateful streaming, and lakehouse pipelines. It combines Spark-style distributed SQL and adaptive batch execution with Flink-style event-time streaming, keyed state, checkpointing, and exactly-once-capable sink coordination.

Krishiv uses one shared planning and runtime model across embedded, single-node, and distributed deployments. The same logical plan, optimizer, physical operators, state abstractions, connector contracts, and checkpoint semantics must apply across all supported modes. Distributed mode supports two deployment targets: **Kubernetes** (primary — operator-managed, CRD-driven) and **bare metal / VM** (secondary — process-managed, static addresses).

Primary product goals:

- Provide a native Rust compute engine for data platform teams.
- Support batch and streaming as first-class execution modes from the start.
- Use Apache Arrow as the internal columnar data format.
- Use DataFusion as the SQL, expression, and local execution foundation.
- Support embedded and single-node execution in R1, then distributed execution in R2 on both Kubernetes and bare metal / VM targets.
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
Embedded | Single-node | Distributed (Kubernetes or bare metal / VM)
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
- Embedded, single-node, and distributed modes must remain semantically aligned for every supported feature.
- Distributed mode supports Kubernetes (primary) and bare metal / VM (secondary); K8s-specific features (operator, CRDs, HA leader election via K8s Lease API, NetworkPolicy) are not available on bare metal.

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
| R3 | Durable metadata for job ownership, task state, task attempts, executor leases, and coordinator restart recovery. |
| R4 | Independent shuffle metadata, recovery hooks, garbage collection, and orphan detection. |
| R5 | Per-job coordinator ownership for stateful jobs plus checkpoint-barrier and watermark protocol design. |
| R6 | Versioned checkpoint/savepoint metadata sufficient for coordinator recovery. |
| R7 | Resource manager, queues, admission control, and scheduler isolation. |
| R8 | Active-active API entry points for Python and Flight SQL clients. |
| R9 | HA coordinators, per-job leader election, leases, and fencing tokens. |
| R10 | Cell-based production control plane. |

### Reliability Pull-Forward Plan

Some stability features must arrive before their full production releases so
later work is not built on a fragile base:

- R3.1 must include task attempt IDs, idempotent task status updates, executor
  leases, coordinator restart recovery, a durable job event log, Kubernetes
  finalizer cleanup, and basic scheduler/executor metrics.
- R4 must include shuffle garbage collection and orphan detection with
  deterministic cleanup tests.
- R5 must define the checkpoint-barrier and watermark interaction protocol
  before durable checkpoint implementation starts in R6.
- R6 must version checkpoint and savepoint metadata from the first format and
  include coordinator/executor/sink chaos tests before any exactly-once claim.
- R9 remains the full HA release, but it must build on the R3 and R6 recovery
  invariants rather than introducing recovery semantics for the first time.

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
    crds/                 # Kubernetes custom resource definitions
    operator/             # Kubernetes operator manifests and packaging
    helm/                 # Helm charts
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

### Architectural Rule: Kubernetes Isolation

**The core runtime must have zero knowledge of how it was deployed.**
- Zero Kubernetes API calls (`kube` crate imports) are allowed in core runtime crates. Kubernetes API access is limited to `krishiv-operator`, Kubernetes packaging under `k8s/`, and narrowly scoped CLI submission/status paths.
- The coordinator has no pod creation or deletion logic; that is strictly the operator's responsibility.
- Features like MetadataStore, LeaderElection, and QueueManager must be hidden behind traits, with Kubernetes-specific implementations living in `krishiv-operator` or Kubernetes packaging under `k8s/`.

This rule ensures that process-mode (bare metal) and future serverless targets (ECS Fargate, Azure Container Apps) remain first-class citizens without requiring core rewrites.

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

- [x] Create Rust workspace and core crate layout.
- [x] Add `krishiv-api`, `krishiv-cli`, `krishiv-sql`, `krishiv-plan`, `krishiv-exec`, and `krishiv-runtime`.
- [x] Implement embedded `Session`, `DataFrame`, and `Stream` API skeletons.
- [x] Add single-node CLI binary.
- [x] Implement `krishiv sql` for local SQL execution.
- [x] Implement `krishiv explain` for logical and physical plan display.
- [x] Implement `krishiv jobs` for local job listing.
- [x] Integrate Arrow and DataFusion for local SQL over Parquet.
- [x] Add local memory stream source for bounded and unbounded test streams.
- [x] Define `ExecutionMode` with `Embedded`, `SingleNode`, and future `Distributed`.
- [x] Document baseline SQL compatibility in `docs/sql-compatibility/`.
- [x] Add SQL golden tests.
- [x] Add embedded/single-node parity tests.
- [x] Add example: `examples/embedded/`.
- [x] Add example: `examples/batch-sql/`.

Acceptance gate:

- [x] A user can run a local SQL query over Parquet.
- [x] A user can run a simple in-memory stream pipeline.
- [x] `krishiv explain` shows logical and physical plans.
- [x] Embedded and single-node execution produce the same result for supported features.

### R2: Distributed Alpha

Scope: first distributed runtime, supporting both Kubernetes and bare metal / VM deployment targets.

**Distributed deployment targets:**

| Target | How processes are managed | Job submission | K8s-specific features |
|---|---|---|---|
| **Kubernetes** (primary) | Operator creates coordinator + executor pods | `KrishivJob` CRD or `krishiv submit` | Operator, CRDs, NetworkPolicy, IRSA |
| **Bare metal / VM** (secondary) | Start coordinator and executor binaries manually (systemd, supervisord, or shell) | `krishiv` CLI connects directly to coordinator address | None — use firewall rules for network isolation |

Bare metal example:
```bash
# Machine A — start coordinator
krishiv-coordinator --listen 0.0.0.0:7070 --data-dir ./meta

# Machine B, C — start executors pointing at coordinator
krishiv-executor --coordinator http://192.168.1.10:7070 --data-dir /var/shuffle

# Any machine — submit and query
krishiv sql --coordinator http://192.168.1.10:7070 "SELECT count(*) FROM ..."
```

Features:

- Coordinator service skeleton.
- Executor service skeleton.
- Static scheduler.
- Kubernetes `KrishivJob` CRD (Kubernetes target only).
- Bare metal static-address coordinator/executor startup.
- Basic distributed DAG submission.
- Basic Web UI for job status.

Checklist:

- [x] Add `krishiv-scheduler` crate.
- [x] Add `krishiv-proto` crate for control-plane RPC contracts.
- [x] Define coordinator service lifecycle.
- [x] Define executor service lifecycle.
- [x] Implement task registration and executor heartbeat.
- [x] Implement static task placement.
- [x] Implement task lifecycle states: pending, running, succeeded, failed, retrying.
- [x] Implement stage-level retry.
- [x] Add `KrishivJob` CRD (Kubernetes target).
- [x] Add basic Kubernetes manifests under `k8s/`.
- [x] Support coordinator and executor binary startup with `--coordinator <addr>` flag (bare metal target).
- [x] Support distributed batch DAG submission on both targets.
- [x] Support distributed streaming DAG submission with local-only state semantics.
- [x] Add basic Web UI for job status, task status, and executor health.
- [x] Keep one active coordinator only.
- [x] Document which features are Kubernetes-only vs available on both targets.

Acceptance gate:

- [x] A simple distributed batch job can be submitted on Kubernetes.
- [x] A simple distributed batch job can be submitted on bare metal (coordinator + executor started as plain binaries).
- [x] Job/task status is visible through CLI or Web UI on both targets.
- [x] Coordinator can retry failed tasks at stage level.

### R3: Connector Contracts

Scope: distributed execution foundation and production I/O baseline. Split into two gated sub-milestones.

**Architecture invariant established in R3:** Stage-Local Execution Model — coordinator partitions work into stages and assigns input partitions to executors; each executor runs a full local DataFusion context for its assigned partitions; shuffle moves data between stages.

#### R3.1: Distributed Execution Foundation

Checklist:

- [x] Add `crates/krishiv-executor` binary crate.
- [x] Add `tonic` gRPC transport to `krishiv-proto`.
- [x] Add tonic-shaped coordinator/executor service boundary in `krishiv-proto`.
- [x] Implement executor registration, heartbeat, and task status over the in-process service adapter.
- [x] Expose executor registration, heartbeat, and task status over a networked gRPC server/client.
- [x] Implement task assignment RPCs.
- [x] Add first executor-side task assignment receiver.
- [x] Add minimal executor task runner skeleton.
- [x] Add versioned coordinator/executor transport contracts in `krishiv-proto`.
- [x] Add task attempt IDs to R3.1 transport task assignments and status updates.
- [x] Reject stale or duplicate task status updates idempotently.
- [x] Add executor lease generation to R3.1 registration, heartbeat, task assignment, and task status contracts.
- [x] Define executor lease model with heartbeat generation and expiry.
- [x] Add durable job event log for job, stage, task, executor, and checkpoint events.
- [x] Add Kubernetes finalizer cleanup for `KrishivJob` delete/cancel paths.
- [x] Add basic scheduler/executor stability metrics: heartbeat age, retry count, task duration, failed assignments.
- [x] Define `MetadataStore` trait with in-memory and durable JSON-file implementations in `krishiv-scheduler`.
- [x] Plug `MetadataStore` into `Coordinator` for durable job/stage/task persistence.
- [x] Replace `PlanNode` string labels with a typed operator enum in `krishiv-plan`.
- [x] Add schema propagation through `LogicalPlan` nodes.
- [x] Add estimated cardinality fields to plan nodes for R4 CBO.
- [x] Write `docs/architecture/stage-local-execution.md`.

Acceptance gate for R3.1:

- [x] Real SQL query completes end-to-end over gRPC (coordinator → executor).
- [x] Executor crash is detected and task is reassigned.
- [x] Coordinator restart recovers job, task, attempt, lease, and event-log state.
- [x] Stale task attempts and duplicate status updates are rejected or ignored safely.
- [x] Operator restart during reconciliation does not create duplicate scheduler jobs.
- [x] Deleting a `KrishivJob` runs finalizer cleanup and leaves no active task assignments.
- [x] Stage-Local Execution Model document is written.
- [x] Stage-Local Execution Model document is reviewed and approved.

#### R3.2: Connector Contracts

Goal: Parquet, Kafka, S3, and catalog — running on real R3.1 executors. Cannot start until R3.1 acceptance gate passes.

Checklist:

- [x] Add `krishiv-connectors` crate.
- [x] Add `krishiv-catalog` crate.
- [x] Define `TableProvider`, `CatalogProvider`, and column statistics model in `krishiv-catalog`.
- [x] Implement in-memory catalog backed by DataFusion `SessionContext` bridge.
- [x] Define `Source` trait.
- [x] Define `Sink` trait.
- [x] Define `Offset` model.
- [x] Define `CommitHandle` model.
- [x] Define `ConnectorCapabilities`.
- [x] Include capability flags: bounded, unbounded, rewindable, transactional, idempotent.
- [x] Implement Parquet reader.
- [x] Implement Parquet writer.
- [x] Implement S3-compatible object store integration (unpartitioned only; partitioned writes depend on R4).
- [x] Implement Kafka source contract and deterministic Kafka-compatible harness; live broker runtime deferred.
- [x] Implement Kafka sink contract and post-write commit protocol; live broker runtime deferred.
- [x] Add source offset tracking.
- [x] Add schema registry abstraction.
- [x] Add at-least-once sink contract.
- [x] Write CDC design document under `docs/rfcs/`.
- [x] Add connector certification test kit.

Acceptance gate for R3.2:

- [x] Parquet connector passes certification tests running on real executors.
- [x] Kafka-compatible connector path passes certification tests for supported semantics; live broker runtime deferred.
- [x] S3-compatible object store integration passes read/write tests.
- [x] Every connector declares capability flags.
- [x] Kafka → Parquet pipeline runs end-to-end on real executors.

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

- [x] Add `docs/architecture/stage-local-execution.md` (Stage-Local Execution Model).
- [x] Add `docs/architecture/streaming-execution-model.md` (continuous operator model, watermark protocol, state interaction, streaming job lifecycle — approved before R5.1 starts).
- [ ] Add `krishiv-shuffle` crate.
- [ ] Add `krishiv-optimizer` crate.
- [ ] Define optimizer rule trait, CBO cost model, AQE rewrite rule, stream planning rule, and skew detection rule interfaces in `krishiv-optimizer`.
- [ ] Define shuffle writer API.
- [ ] Define shuffle reader API.
- [ ] Define shuffle metadata model.
- [ ] Define shuffle garbage collection and orphan detection model.
- [ ] Implement hash partitioning.
- [ ] Implement shuffle read/write path.
- [ ] Implement shuffle cleanup for completed, failed, and cancelled jobs.
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

- [ ] Usable Product Gate passes: distributed batch SQL on Parquet+S3, TPC-H SF10 correctness, Kafka→Parquet pipeline, live Kubernetes CLI, published TPC-H result. Project made public after this gate.
- [ ] Distributed join correctness tests pass.
- [ ] Distributed aggregation correctness tests pass.
- [ ] Spill tests pass.
- [ ] Skew simulation identifies hot partitions.
- [ ] Shuffle metadata remains recoverable after executor failure.
- [ ] Orphan shuffle data is detected and cleaned up deterministically.
- [x] `docs/architecture/streaming-execution-model.md` is written and approved.

### R5: Stateful Streaming Core

Scope: Flink-style stateful stream processing. Split into two gated sub-milestones. R5.1 cannot start until `docs/architecture/streaming-execution-model.md` is approved.

#### R5.1: One Certified Streaming Path

Certified path: Kafka (single partition) → tumbling event-time window → in-memory keyed state → Kafka sink, deterministic replay.

Checklist:

- [ ] Add `krishiv-state` crate.
- [ ] Implement continuous operator execution loop on executor.
- [ ] Implement streaming job lifecycle in coordinator (never Succeeded while running).
- [ ] Define checkpoint-barrier and watermark interaction protocol.
- [ ] Implement in-memory keyed state backend.
- [ ] Implement single-source watermark propagation.
- [ ] Implement tumbling window aggregation.
- [ ] Implement event-time timers.
- [ ] Implement deterministic replay harness.
- [ ] Add `key_by`, tumbling window, and event-time watermark APIs.

Acceptance gate for R5.1:

- [ ] Certified path runs end-to-end on real executors.
- [ ] Watermarks close windows correctly.
- [ ] Deterministic replay produces identical output.
- [ ] Streaming job lifecycle is correctly modeled (never Succeeded while running).
- [ ] Checkpoint-barrier and watermark protocol is documented before R6 implementation starts.
- [ ] R1-R4 batch behavior passes (no regression).

#### R5.2: Streaming Hardening

Checklist:

- [ ] Implement RocksDB keyed state backend (with `spawn_blocking` isolation).
- [ ] Implement state TTL.
- [ ] Implement `key_by`.
- [ ] Implement event-time timestamp assignment.
- [ ] Implement multi-source watermark propagation.
- [ ] Implement processing-time timers.
- [ ] Implement sliding windows.
- [ ] Implement session windows.
- [ ] Implement stream-table join baseline.
- [ ] Add `krishiv state inspect`.
- [ ] Add watermark/window correctness tests.

Acceptance gate for R5.2:

- [ ] Recoverable stateful window aggregation behaves deterministically (RocksDB backend).
- [ ] Multi-source watermarks close windows correctly.
- [ ] State TTL removes expired state.
- [ ] State inspection reads metadata without mutating state.

### R6: Checkpoints And Savepoints

Scope: reliable stateful execution. Exactly-once certified for one specific triple only: Kafka source + in-memory state + S3/Parquet sink. All other combinations are at-least-once in R6. Mandatory chaos test suite required before acceptance gate.

Features:

- Checkpoint epochs.
- Async incremental checkpoints.
- Savepoints.
- Restore.
- Rescaling metadata.
- Source offset coordination.
- Two-phase commit sink API.
- State schema evolution baseline.
- Chaos test suite (coordinator kill, executor kill, sink kill mid-checkpoint).

Checklist:

- [ ] Add `krishiv-checkpoint` crate.
- [ ] Define minimal `FencingToken` type in `krishiv-proto` (monotonic epoch counter).
- [ ] Enforce fencing token checks on checkpoint epoch ownership transitions.
- [ ] Define checkpoint epoch model.
- [ ] Define versioned checkpoint metadata format.
- [ ] Define versioned savepoint metadata format.
- [ ] Implement async checkpoint coordinator.
- [ ] Implement checkpoint metadata version compatibility tests from the first version.
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
- [ ] Checkpoint/savepoint metadata versions are readable across supported upgrades.

### R7: Resource Governance And Adaptivity

Scope: multi-tenant production control. Split into two sub-milestones to contain scope and reduce stall risk.

#### R7.1: Resource Management Foundation

Features:

- Resource manager.
- Queues and priorities.
- Admission control.
- Quotas.
- Namespace isolation.
- Cost metrics.

Checklist:

- [ ] Add resource manager service.
- [ ] Define `KrishivQueue` CRD.
- [ ] Implement job queues.
- [ ] Implement job priorities.
- [ ] Implement admission control.
- [ ] Implement CPU and memory quota model.
- [ ] Implement namespace isolation model.
- [ ] Add runtime cost metrics.
- [ ] Add quota/admission tests.

Acceptance gate for R7.1:

- [ ] Jobs above quota are rejected or queued.
- [ ] Admission control rejects jobs when resources are unavailable.
- [ ] Cost metrics are visible per job in the status API.

#### R7.2: Backpressure And Adaptivity

Features:

- Bounded operator queues.
- Credit-based backpressure.
- Source throttling.
- Slow-sink detection.
- Hot-key detection and splitting.
- Adaptive repartitioning.
- Manual override and explainable decisions.

Checklist:

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
- [ ] Add hot-key simulation tests.

Acceptance gate for R7.2:

- [ ] Overloaded jobs are throttled without destabilizing other jobs.
- [ ] Hot-key tests show load reduction after splitting.
- [ ] Adaptive decisions are visible to operators.
- [ ] Manual override disables adaptive behavior correctly.

### R8: Lakehouse And Python Beta

Scope: broader data platform usability. Split into two sub-milestones to isolate unrelated workstreams and prevent blocking.

#### R8.1: Python Bindings, UDFs, And Flight SQL

Features:

- Python bindings via PyO3.
- Python `Session` and `DataFrame` bindings.
- Vectorized Python UDFs over Arrow batches.
- UDF isolation boundary.
- Stable Rust UDF/UDAF/UDTF contracts.
- Flight SQL endpoint.

Checklist:

- [ ] Add `krishiv-python` crate with PyO3.
- [ ] Add Python `Session` binding.
- [ ] Add Python `DataFrame` binding.
- [ ] Add Python `Stream` binding (bounded collect only; full streaming deferred post-GA).
- [ ] Add `await session.sql_async()` for `asyncio` callers.
- [ ] Add `session.read_parquet()`, `session.read_kafka()`, `session.read_iceberg()` Python connector wrappers.
- [ ] Add Python query execution smoke tests.
- [ ] Add vectorized Python UDF support over Arrow batches.
- [ ] Add UDF isolation boundary.
- [ ] Add `krishiv-udf` crate.
- [ ] Stabilize Rust UDF contract.
- [ ] Stabilize Rust UDAF contract.
- [ ] Stabilize Rust UDTF contract.
- [ ] Implement maturin build pipeline for manylinux wheels.
- [ ] Generate `.pyi` type stub files for all public Python APIs.
- [ ] Add Flight SQL endpoint.
- [ ] Mark Python API as beta.

Acceptance gate for R8.1:

- [ ] Python query smoke tests pass.
- [ ] Vectorized Python UDF tests pass.
- [ ] Flight SQL smoke tests pass.
- [ ] Python API is clearly marked beta.

#### R8.2: Iceberg And Lakehouse Integration

Features:

- Iceberg read/write beta.
- Snapshot reads.
- Schema and partition evolution.
- Time travel.
- Lakehouse catalog integration.

Checklist:

- [ ] Add `krishiv-lakehouse` crate.
- [ ] Implement Iceberg read beta.
- [ ] Implement Iceberg write beta.
- [ ] Implement Iceberg snapshot reads.
- [ ] Implement Iceberg schema evolution support.
- [ ] Implement Iceberg partition evolution support.
- [ ] Implement Iceberg time travel support.
- [ ] Mark lakehouse APIs as beta.

Acceptance gate for R8.2:

- [ ] Iceberg snapshot read/write smoke tests pass.
- [ ] Schema evolution tests pass.
- [ ] Time travel queries return correct historical snapshots.
- [ ] Lakehouse APIs are clearly marked beta.

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
- Metadata schema upgrade tests.
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
- [ ] Add metadata schema upgrade tests for job, event-log, checkpoint, savepoint, connector, and catalog metadata.
- [ ] Add chaos test suite.
- [ ] Add TPC-H benchmark suite.
- [ ] Add TPC-DS benchmark suite.
- [ ] Add Nexmark benchmark suite.
- [ ] Publish benchmark report.
- [ ] Optimize top benchmark regressions before GA.
- [ ] Publish production hardening guide.

Acceptance gate:

- [ ] GA benchmark gates pass with defined numeric thresholds (TPC-H SF100 per-query targets, TPC-DS SF100 targets, Nexmark events/second target; targets must be published before R10 implementation begins).
- [ ] Upgrade tests pass.
- [ ] Metadata schema compatibility tests pass for all GA-supported persisted metadata.
- [ ] Chaos suite passes.
- [ ] Certified connector matrix passes.
- [ ] Public API stability policy is documented.
- [ ] SQL and function compatibility matrix is published.

## Cross-Cutting Risks And Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Hybrid engine scope grows too large | Delayed releases and unstable foundations | Keep each release narrow and preserve explicit acceptance gates |
| Single coordinator becomes bottleneck | Poor scalability and availability | Move from single coordinator to per-job coordinators and cell-based scheduling |
| Full multi-master causes correctness bugs | Duplicate checkpoint ownership or duplicate sink commits | Avoid active-active scheduling for the same job |
| Durable metadata is introduced too late | Restart and recovery semantics become bolted on | Pull `MetadataStore`, task attempts, executor leases, and job event log into R3.1 |
| Duplicate task attempts commit side effects | Incorrect output under retries or executor restarts | Use attempt IDs, idempotent updates, and stale-attempt rejection before connector certification |
| Kubernetes deletes leave runtime state behind | Leaked tasks, shuffle data, or status | Add finalizers and cleanup paths before production connector execution |
| Split-brain during failover | State corruption or duplicate output | Use durable leases, fencing tokens, and checkpoint epoch ownership |
| Shuffle overwhelms network or disk | Spark-like performance bottlenecks | Add push-style shuffle, partition coalescing, compression, spill, and skew detection |
| Shuffle artifacts leak after retries | Disk/object-store growth and incorrect recovery | Add shuffle garbage collection and orphan detection in R4 |
| Stateful jobs grow too large | Slow checkpoints and recovery | Use pluggable state backends, TTL, incremental checkpoints, and tiered snapshots |
| Observability arrives too late | Failures are hard to diagnose during R3-R6 | Add basic scheduler/executor stability metrics in R3 and full OpenTelemetry in R9 |
| Backpressure spreads through pipelines | High latency or stalled jobs | Add credit-based flow control, bounded queues, and source throttling |
| Connector semantics are inconsistent | Incorrect delivery guarantees | Require capability flags and connector certification |
| Exactly-once is overpromised | User trust and correctness risk | Certify exactly-once only for specific source/sink/checkpoint combinations |
| Python/lakehouse APIs destabilize core | Core runtime churn | Keep Rust core stable and mark Python/lakehouse APIs beta |
| Benchmark gaps vs Spark/Flink | Weak adoption | Publish transparent benchmarks and optimize top regressions |

## Test And Acceptance Strategy

Release-level testing:

- R1-R3: SQL golden tests, embedded/single-node parity tests, API tests, connector contract tests, Parquet/Kafka/S3 integration tests, coordinator restart tests, executor lease expiry tests, stale-attempt tests, and operator restart tests.
- R4: shuffle correctness, join correctness, spill tests, skew simulation, small-file planning tests, shuffle orphan cleanup tests, TPC-H smoke benchmark.
- R5-R6: watermark/window correctness, checkpoint-barrier protocol tests, state replay, checkpoint restore, metadata-version compatibility tests, duplicate prevention, transactional sink tests, executor-kill recovery.
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
- **Distributed mode supports two deployment targets:**
  - **Kubernetes** (primary): operator-managed, CRD-driven, NetworkPolicy, IRSA, HA leader election via K8s Lease API.
  - **Bare metal / VM** (secondary): coordinator and executor binaries started as plain processes on any host with TCP connectivity; `krishiv` CLI connects directly to coordinator address. No K8s operator, no CRDs.
- The core coordinator/executor runtime (gRPC transport, task assignment, heartbeat, ShuffleStore, MetadataStore) is identical on both targets. Kubernetes-specific features (operator, CRDs, NetworkPolicy, HA via K8s Lease) are unavailable on bare metal.
- HA coordinator (R9) is Kubernetes-only in the first implementation. Bare metal HA requires external etcd and is deferred post-R9.
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
