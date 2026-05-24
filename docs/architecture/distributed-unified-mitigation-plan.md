# Distributed Unified Batch/Streaming Mitigation Plan

**Status:** Approved implementation plan (architecture decisions locked).  
**Scope:** Close all distributed-mode gaps (GAP-*), remove architectural bottlenecks, and deliver **production-grade unified batch + streaming** on **Kubernetes** and **bare metal/VM** with **no stubs, no placeholder backends, and no “defer to later release” items** inside this plan.  
**Owner:** Architecture + platform engineering.  
**Related:** [unified-execution-model.md](./unified-execution-model.md), [deployment-targets.md](./deployment-targets.md), [r12-maturity-gap-register.md](./r12-maturity-gap-register.md), [architectural-decisions-r12-r20.md](./architectural-decisions-r12-r20.md), [gap-analysis-2026-05-23.md](../engineering/gap-analysis-2026-05-23.md).

---

## 1. Executive summary

Krishiv already has the right **single-engine** shape: one DAG model, `ExecutionRuntime`, stage-local executors, gRPC control plane, Flight data plane. Production gaps come from **(a)** a **cluster-scoped coordinator** bottleneck, **(b)** **fragment-string** execution instead of lowered physical operators, **(c)** **uncertified streaming state/checkpoints**, **(d)** **doc/code drift** on Kubernetes job lifecycle, and **(e)** missing **L5 CI contracts** (multi-process E2E, chaos, certification).

This plan replaces the shared in-operator coordinator with a **two-tier control plane**, completes **physical plan lowering** for batch and streaming, wires **aligned checkpoints + durable state** end-to-end, and ships **target-specific operators** (K8s) and **cluster daemons** (bare metal) that share the same Rust crates and protos.

**Definition of done for this plan:** Every row in [§8 Gap closure matrix](#8-gap-closure-matrix) is implemented, tested at L4+ (binary/CLI/process), and covered by CI gates in [§9 CI and certification contract](#9-ci-and-certification-contract).

---

## 2. Locked architectural decisions (new ADRs)

Record these as **DECIDED** in `architectural-decisions-r12-r20.md` (section “Distributed unified mitigation”).

| ADR | Decision | Rationale |
|-----|----------|-----------|
| **ADR-DIST-01** | **Two-tier control plane:** `ClusterControlPlane` (CCP) + **one `JobCoordinator` (JCP) per job** | Preserves “exactly one active coordinator **per job**”; removes operator single-coordinator bottleneck |
| **ADR-DIST-02** | **K8s:** CCP in `krishiv-operator`; JCP = **JobCoordinator Pod** per `KrishivJob`; executors from **`KrishivExecutorPool`** | Matches Flink JobManager / Spark driver isolation; enables per-job failure domains |
| **ADR-DIST-03** | **Bare metal:** CCP = `krishiv-clusterd`; JCP = **`krishiv-job-coordinator` subprocess** per job | Same semantics without Kubernetes API |
| **ADR-DIST-04** | **Eliminate fragment strings** for product paths: `PhysicalPlan` lowers to typed `PlanOp` → executor `OperatorFactory` | Removes `stream:tw:` / `sql:` hacks (GAP-ST-06); one optimizer output drives batch + streaming |
| **ADR-DIST-05** | **Transport trait** (`CoordinatorExecutorTransport`, `ExecutorTaskTransport`, `BarrierTransport`) shared by gRPC, in-process, and test fakes | Completes ADR-12.4/12.7; no `block_on` in Tokio workers |
| **ADR-DIST-06** | **Async `ExecutionBackend` + `ExecutionRuntime`** (AFIT); remove `block_on` from `DistributedBackend::execute` | ADR-12.7 enforcement |
| **ADR-DIST-07** | **Metadata:** `MetadataStore` with **`sqlite` (bare metal default)**, **`kubernetes-status` (K8s)**, **`etcd` (HA bare metal + global tier)** | GAP-CP-04–06, GAP-K8-01 |
| **ADR-DIST-08** | **Leader election:** CCP only uses `LeaderElection` (K8s Lease or etcd). JCP **does not** compete for cluster lease; JCP HA = **fencing token + metadata epoch** owned by CCP | GAP-CP-01/02 separation |
| **ADR-DIST-09** | **Kafka client:** `rdkafka` only (`features = ["kafka"]`) | ADR-12.1 → DECIDED |
| **ADR-DIST-10** | **LeaderElection:** native `async fn` in trait (AFIT), no `async-trait`, no `block_on` in operator | ADR-12.2 → DECIDED |
| **ADR-DIST-11** | **Checkpoints:** aligned barriers only; **dedicated `BarrierService` gRPC** (ADR-16.3 Option B) | Join/fan-in correctness |
| **ADR-DIST-12** | **Checkpoint storage:** `LocalFsCheckpointStorage` + **`ObjectStoreCheckpointStorage` (S3/GCS/Azure)** required for production distributed | GAP-CK-02; spot/preemptible K8s |
| **ADR-DIST-13** | **Streaming state:** all window/join operators use **`StateBackend`** (RocksDB on executor, namespaces per operator/task) | GAP-ST-01/05 |
| **ADR-DIST-14** | **Shuffle:** production default **`object-store` durability on K8s**; **`local` default on dev bare metal**; spill + partition cap always on | GAP-C6, GAP-SH-01 |
| **ADR-DIST-15** | **Distributed SQL:** Flight SQL for interactive collect (ADR-12.3); **job submission** for long batch/streaming via JCP gRPC `SubmitJob` | Clear latency vs durability split |
| **ADR-DIST-16** | **Session API:** `execute_local` vs `execute_remote` explicit; `with_remote_execution(true)` fails closed (no silent local fallback) | GAP-RT-02 |
| **ADR-DIST-17** | **Idle source detection** for multi-source watermarks (stalling partition policy) | Streaming bottleneck |
| **ADR-DIST-18** | **Rescaling:** savepoint + explicit repartition on restore (ADR-16.2); **implement repartition in restore path** in this plan | No “post-R6” deferral |

---

## 3. Target architecture

### 3.1 Control plane (cluster + per-job)

```text
                    ┌─────────────────────────────────────┐
                    │  Cluster Control Plane (CCP)        │
                    │  - Admission / quotas               │
                    │  - Executor pool registry           │
                    │  - Spawn/stop JobCoordinator (JCP)  │
                    │  - Leader election (1 active CCP)   │
                    │  - MetadataStore (jobs catalog)     │
                    └──────────────┬──────────────────────┘
                                   │ 1..N jobs
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
    ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
    │ JCP (job A) │        │ JCP (job B) │        │ JCP (job C) │
    │ stages/tasks│        │ streaming   │        │ batch SQL   │
    │ checkpoints │        │ barriers    │        │ shuffle meta│
    └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
           │ assign tasks         │                      │
           └──────────────────────┴──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │  Executor pool (replaceable workers) │
                    │  - DataFusion stage fragments        │
                    │  - Streaming operators + StateBackend│
                    │  - Shuffle write/read (Flight)       │
                    └─────────────────────────────────────┘
```

### 3.2 Unified execution path (all modes)

```text
Session API
  → LogicalPlan (+ stream planner for EventTime/Windows)
  → Optimizer (batch + stream rules + CoalesceRule)
  → PhysicalPlan (typed PlanOp graph)
  → ExecutionRuntime (async)
       Embedded/SingleNode → InProcessCluster (mpsc transport)
       Distributed       → JCP SubmitJob + Flight SQL (collect/explain)
  → Executor OperatorRuntime (same krishiv-exec operators everywhere)
```

### 3.3 Data plane (unchanged principle, fully implemented)

- **Control:** tonic gRPC (`CoordinatorExecutor`, `ExecutorTask`, `BarrierService`).
- **Bulk data:** Arrow IPC local shuffle files + Flight reads; object-store-backed shuffle for prod K8s.
- **Results:** Flight SQL to client; streaming continuous jobs use **control RPCs** (`PushStreamInput`, `PollStreamOutput`) + optional Flight for large payloads.

---

## 4. Workstreams (implementation order)

Workstreams are **sequential where noted**; within a workstream, tasks are parallelizable unless marked **depends on**.

### WS-0 — Decision lock, CI skeleton, and doc alignment

| Task | Deliverable | Crates / paths |
|------|-------------|----------------|
| 0.1 | Add ADR-DIST-01..18 to `architectural-decisions-r12-r20.md` | docs |
| 0.2 | Replace `deployment-targets.md` §Kubernetes “per-job pods” with **CCP/JCP/Pool** model | docs |
| 0.3 | Add tracker `docs/implementation/distributed-unified-mitigation.md` (checklist mirror of §4–§8) | docs |
| 0.4 | CI job `distributed-e2e`: build coordinator, executor, operator, clusterd; run in-process + subprocess tests | `.github/workflows/` |
| 0.5 | `cargo test -p krishiv-scheduler --test coordinator_executor_integration` + new `tests/distributed_e2e/` in workspace | tests |

**Gate:** ADRs DECIDED; CI job runs on every PR (may use `services:` for MinIO/Kafka in later WS).

---

### WS-1 — Scheduler decomposition and transport abstraction **(blocks all distributed work)**

**Depends on:** WS-0.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 1.1 | Finish `lib.rs` extraction: `coordinator/`, `job/`, `heartbeat/`, `checkpoint/`, `metadata/`, `grpc/`, `barrier/` | GAP-CP-10 |
| 1.2 | Introduce `CoordinatorExecutorTransport` trait; move tonic adapter behind it | ADR-DIST-05 |
| 1.3 | `Coordinator::new(transport, metadata, config)` — no direct tonic in core | ADR-DIST-05 |
| 1.4 | `JobCoordinator` struct (today’s `Coordinator` job-scoped logic) split from `ClusterControlPlane` | ADR-DIST-01 |
| 1.5 | `ClusterControlPlane`: `submit_job_request`, `spawn_jcp`, `list_executors`, `allocate_executors` | ADR-DIST-01 |
| 1.6 | AFIT `LeaderElection` + migrate `K8sLeaseElection` | GAP-CP-01, ADR-DIST-10 |
| 1.7 | `EtcdLeaderElection` + `EtcdMetadataStore` (using `etcd-client` crate) | bare metal HA |
| 1.8 | Unit tests: transport fake, JCP lifecycle, lease promote/demote | L3→L4 |

**Gate:** `cargo test -p krishiv-scheduler --lib` + decomposition reduces `lib.rs` to &lt; 2500 lines.

---

### WS-2 — Physical plan lowering and async execution runtime

**Depends on:** WS-1.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 2.1 | Expand `krishiv-plan::PlanOp` for all R5/R6 operators (tumbling/sliding/session, join, aggregate, source, sink) | GAP-ST-04, GAP-ST-06 |
| 2.2 | `PhysicalPlanLowering` in `krishiv-plan` or `krishiv-sql`: Logical → Physical typed graph | GAP-ST-06 |
| 2.3 | `krishiv-executor::OperatorFactory::from_plan_op` — **no fragment string** in production path | GAP-ST-06 |
| 2.4 | Keep fragment parser **only** for backward-compatible tests; deprecate in public API | — |
| 2.5 | `ExecutionBackend` → `async fn execute` (AFIT); remove `block_on` from `DistributedBackend` | ADR-DIST-06, GAP-RT-01 |
| 2.6 | `ExecutionRuntime` fully async; sync Session API uses `krishiv-async-util::block_on` only at API edge | ADR-12.7 |
| 2.7 | Wire optimizer (`CoalesceRule`, stream rules) in **`JCP::submit_plan`** before task generation | GAP-I3, GAP-SH-04 |
| 2.8 | Parity tests: same query/window across Embedded, SingleNode, Distributed (in-process JCP) | unified model |

**Gate:** `distributed_backend_submits_plan_over_flight_sql` + new `lowered_plan_window_e2e` tests pass.

---

### WS-3 — Durable metadata, fencing, and coordinator binary hardening

**Depends on:** WS-1.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 3.1 | `krishiv-coordinator` (CCP daemon) and `krishiv-job-coordinator` (JCP daemon) binaries | ADR-DIST-02/03 |
| 3.2 | CCP flags: `--metadata-backend sqlite|etcd|kubernetes`, recovery on startup | GAP-CP-04 |
| 3.3 | `save_job` / task updates **fail-closed** on persist error | GAP-CP-05 |
| 3.4 | `recover_from_store` rebuilds per-job `CheckpointCoordinator` | GAP-CP-06 |
| 3.5 | `validate_fencing_token` on **every** checkpoint write + restore (grep audit in CI) | GAP-CP-03, GAP-CK-01, GAP-CK-04 |
| 3.6 | Fencing token bump on CCP leader acquire; JCP inherits generation via spawn handshake | GAP-CP-02 |
| 3.7 | Ack fencing: reject `!= current` (not only stale) | GAP-CP-11 |
| 3.8 | CCP tick loop: heartbeat + launch assignments (already on operator; **require on standalone CCP**) | GAP-C1 |
| 3.9 | Executor registry idempotent re-register + lease bump (verify/enhance) | GAP-CP-07 |
| 3.10 | `extract_auth_context` on all gRPC handlers → deny unauthenticated when auth enabled | GAP-CP-08 |

**Gate:** `coordinator_restart_recovers_three_jobs` integration test; `fencing_token_audit` CI script.

---

### WS-4 — Executor data plane completion

**Depends on:** WS-2, WS-3.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 4.1 | `GrpcCoordinatorService` connection pool (single lazy channel, reuse) | GAP-C3 |
| 4.2 | Update `lease_generation` from register/heartbeat responses | GAP-C4 |
| 4.3 | `ExecutorTaskRunner` loop mandatory in `krishiv-executor --connect` | GAP-CP-09 |
| 4.4 | Task assignment handles typed `PlanOp` payload (protobuf `PlanFragment`) | GAP-ST-06 |
| 4.5 | `checkpoint_ack` delivery from runner → coordinator | GAP-C8 |
| 4.6 | `BarrierService` server on executor; operator handles aligned barriers | GAP-ST-03, ADR-16.3 |
| 4.7 | Catalog bridge: `MemoryExec` / `ParquetExec` for registered tables in executor SQL | GAP-C9, GAP-C10 |
| 4.8 | Policy: `Session::sql` routes through policy when configured | GAP-RT-05 |
| 4.9 | `collect_with_stats` uses session `TaskContext` | GAP-RT-06 |

**Gate:** `tests/distributed_e2e/batch_sql_over_grpc.rs` and `streaming_window_over_grpc.rs` pass.

---

### WS-5 — Stateful streaming and checkpoints (unified batch/stream correctness)

**Depends on:** WS-4.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 5.1 | `StateBackedWindowOperator` for tumbling/sliding/session on **`RocksDBStateBackend`** | GAP-ST-01 |
| 5.2 | `TimerService` drained on watermark advance in executor loop | GAP-ST-02 |
| 5.3 | Continuous stream registry persists operator state to `StateBackend` | streaming continuous |
| 5.4 | `ObjectStoreCheckpointStorage` (S3/GCS/Azure via `object_store` crate) | GAP-CK-02 |
| 5.5 | Local FS checkpoint: `sync_all` + parent dir fsync | GAP-C5 |
| 5.6 | Epoch monotonicity guard before `write_epoch_metadata` | GAP-C5 |
| 5.7 | Aligned barrier flow: inject → all channels → snapshot → ack | GAP-ST-03, GAP-ST-05 |
| 5.8 | `TtlStateBackend::list_keys` filters expired | GAP-C7 |
| 5.9 | Multi-source watermark + **idle source policy** (advance with idle watermark after timeout) | ADR-DIST-17 |
| 5.10 | Restore repartition implementation (ADR-16.2) | ADR-DIST-18 |
| 5.11 | Certification tests in `tests/certification/streaming_checkpoint.rs` | GAP-CN-03 partial |

**Gate:** chaos test `kill_executor_mid_checkpoint_restores_state`; matrix row for streaming checkpoint updated.

---

### WS-6 — Shuffle production path

**Depends on:** WS-4.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 6.1 | Executor hot path uses `ShuffleCompression` (Lz4 default, Zstd opt-in) | GAP-SH-01 |
| 6.2 | Codec header on all partition files (already designed — enforce in executor writes) | GAP-SH-02 |
| 6.3 | Stable hash (`twox-hash`) in distributed task partitioning | GAP-SH-03 |
| 6.4 | `InMemoryShuffleStore` memory threshold → spill to `LocalDiskShuffleStore` | GAP-C6 |
| 6.5 | Enforce `max_partitions` on every `write_partition` | GAP-C6 |
| 6.6 | `JobSpec` carries shuffle durability mode; JCP configures store per job | ADR-DIST-14 |
| 6.7 | **Krishiv Shuffle Service** (optional DaemonSet / bare metal systemd unit): Flight read of local partitions without pinning to producer executor | data-plane doc |
| 6.8 | Deprecate duplicate shuffle API per GAP-SH-05 | docs + `#[deprecated]` |

**Gate:** TPC-H style multi-stage batch test with shuffle &gt; 1GB spooled; no OOM in CI given limit.

---

### WS-7 — Kubernetes operator v2 (per-job isolation)

**Depends on:** WS-1, WS-3, WS-4.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 7.1 | CRD `KrishivExecutorPool` (replicas, resources, service account) | ADR-DIST-02 |
| 7.2 | CCP Deployment: `krishiv-operator` **only** reconciles pools + spawns JCP; **replicas ≥ 2** with Lease | GAP-C2, GAP-CP-01 |
| 7.3 | JCP Pod template per `KrishivJob`: `krishiv-job-coordinator --job-id …` | ADR-DIST-02 |
| 7.4 | Operator creates JCP Service (gRPC + Flight) per job or shared headless + job label | networking |
| 7.5 | Executors: pool Deployment registers with CCP; JCP assigns tasks to pool members | replaces static-only model |
| 7.6 | Finalizer: delete shuffle prefixes (object store) + cancel JCP + remove JCP pod | R3.1 cleanup |
| 7.7 | Pod failure → `mark_executor_lost` + stage retry (wire existing hooks) | launch failure |
| 7.8 | NetworkPolicy templates under `k8s/networkpolicies/` | deployment-targets |
| 7.9 | Kind E2E **required in CI** (`KRISHIV_KIND_E2E=1` default on main) | GAP-T2, GAP-K8-01 |
| 7.10 | `JobSubmitter` impl: `kubectl`/kube client create `KrishivJob` from CLI | GAP-CP-12 |

**Gate:** Kind test: submit batch + streaming `KrishivJob`, observe JCP pod Running, tasks Succeeded.

---

### WS-8 — Bare metal production stack

**Depends on:** WS-3, WS-4, WS-6.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 8.1 | `krishiv-clusterd` binary (CCP): metadata sqlite path, executor pool registry | ADR-DIST-03 |
| 8.2 | `krishiv job run` spawns `krishiv-job-coordinator` subprocess per job | bare metal jobs |
| 8.3 | systemd unit templates: `krishiv-clusterd.service`, `krishiv-executor@.service` | deployment-targets |
| 8.4 | `krishiv-cluster` CLI: `start|stop|status` for local multi-process dev | Spark-like UX |
| 8.5 | etcd HA profile: document + helm-style sample config for 3-node etcd | ADR-DIST-07 |
| 8.6 | Firewall/port documentation automated (`krishiv-cluster verify-network`) | bare metal ops |
| 8.7 | Bare metal E2E test harness in CI (spawn clusterd + 2 executors + submit) | GAP-T2 |

**Gate:** GitHub Actions job `bare-metal-e2e` green on ubuntu-latest.

---

### WS-9 — Session/API parity and Flight integration

**Depends on:** WS-2, WS-4.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 9.1 | `SessionBuilder::with_coordinator_grpc` + `with_flight_url` separate URLs | ADR-12.3 |
| 9.2 | `execute_remote` / `execute_local` explicit methods; distributed `register_parquet` via Flight catalog protocol | GAP-RT-02 |
| 9.3 | Distributed `collect_bounded` / `submit_stream_job` always pass JCP endpoint | GAP-RT-08 |
| 9.4 | `StateTtlConfig` → `TtlConfig` wired on all modes | GAP-RT-07 |
| 9.5 | Flight SQL: auth + policy both required in production config (default deny) | GAP-GV-03 |
| 9.6 | Remote coordinator CLI fully wired (savepoint/restore/list/inspect) | GAP-RT-04 |
| 9.7 | Python: `connect`, `submit_stream_job`, distributed collect — no `todo!()` | GAP-PY-01 |

**Gate:** `remote_execution_without_fallback_uses_flight_server` + Python maturin tests in CI.

---

### WS-10 — Connectors, observability, autoscale

**Depends on:** WS-5, WS-7.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 10.1 | Kafka: `rdkafka` consumer/producer, watermark lag, offset commit with checkpoint epoch | GAP-CN-02 |
| 10.2 | `ParquetSource::reset()` | GAP-CN-05 |
| 10.3 | Certification suite expansion (Parquet, Kafka, S3 2PC paths) | GAP-CN-03 |
| 10.4 | Prometheus: scheduler metrics on CCP/JCP `/metrics`; executor task metrics | GAP-OB-01, GAP-I6 |
| 10.5 | Structured tracing spans: submit, assign, barrier, checkpoint, shuffle | R14 observability |
| 10.6 | KEDA `ScaledObject` on executor pool keyed by Kafka consumer lag / backlog metric | K8s autoscale |
| 10.7 | **Placement v2:** slot-aware assignment using executor heartbeat resources (CPU/mem slots) | static RR bottleneck |
| 10.8 | Spot/preemptible: drain hook → checkpoint then exit (ADR-19.2 implemented now) | spot recovery |

**Gate:** docker-compose Kafka test in CI; KEDA manifest validates with `kubeconform`.

---

### WS-11 — Federation and multi-region (minimal for single-cluster plan completion)

**Depends on:** WS-3, WS-7.

| Task | Deliverable | Closes |
|------|-------------|--------|
| 11.1 | `RemoteFederationClient` gRPC to global metadata (ADR-19.1 Option C) | GAP-FD-01 |
| 11.2 | Regional CCP registers with global catalog; JCP stays regional | ADR-19.1 |
| 11.3 | Routing policy integration tests (2 mock regions) | federation |

**Gate:** `cargo test -p krishiv-federation` with networked fake global CP.

---

## 5. Kubernetes vs bare metal feature parity

After this plan, both targets expose the **same APIs and semantics**; only **process orchestration** differs.

| Capability | Kubernetes implementation | Bare metal implementation |
|------------|---------------------------|---------------------------|
| Cluster control plane | `krishiv-operator` (CCP) + Lease HA | `krishiv-clusterd` + optional etcd HA |
| Per-job coordinator | JCP Pod | `krishiv-job-coordinator` subprocess |
| Executor pool | `KrishivExecutorPool` Deployment + HPA/KEDA | N × `krishiv-executor` systemd instances |
| Job submit | `KrishivJob` CRD / `krishiv submit` | `krishiv job run` → clusterd |
| Metadata | CRD status + sqlite sidecar **or** CCP sqlite/etcd | sqlite / etcd |
| Network isolation | NetworkPolicy | `krishiv-cluster verify-network` + ufw templates |
| Shuffle durability default | object-store | local (prod profile: object-store via config) |
| Checkpoints | object-store (MinIO/S3) | object-store or local FS |
| TLS/mTLS | cert-manager | manual certs + documented rotation |

---

## 6. Bottleneck elimination map

| Bottleneck (current) | Mitigation (this plan) | Workstream |
|----------------------|------------------------|------------|
| Single shared coordinator in operator | CCP + per-job JCP | WS-1, WS-7, WS-8 |
| Fragment-string execution | `PlanOp` lowering + `OperatorFactory` | WS-2 |
| In-memory streaming state | RocksDB `StateBackend` + checkpoint snapshots | WS-5 |
| No barrier/ack path | `BarrierService` + aligned protocol | WS-4, WS-5 |
| Local-only checkpoints | `ObjectStoreCheckpointStorage` | WS-5 |
| Shuffle OOM / no spill | Spill + caps + compression on hot path | WS-6 |
| Static round-robin placement | Slot-aware placement + pool allocation | WS-10 |
| Multi-source watermark stall | Idle source policy | WS-5 |
| Doc: per-job pods missing | Operator v2 creates JCP pods | WS-7 |
| No distributed CI | kind + bare-metal E2E required | WS-0, WS-7, WS-8 |
| `block_on` in async paths | AFIT backends + transport | WS-1, WS-2 |
| Operator replica=1 SPOF | CCP replicas + Lease; JCP per job | WS-7 |

---

## 7. Crate and ownership map

| Crate | New/changed responsibility |
|-------|----------------------------|
| `krishiv-proto` | `ClusterControlPlane` service, `PlanFragment`, `BarrierService` messages |
| `krishiv-scheduler` | CCP + JCP split, transport traits, metadata backends |
| `krishiv-operator` | CCP reconciliation, JCP pod lifecycle, pools, finalizers |
| `krishiv-executor` | `OperatorFactory`, barrier server, stateful operators |
| `krishiv-plan` | `PlanOp` lowering, stream/batch unified physical graph |
| `krishiv-runtime` | Async `ExecutionRuntime`, no fragment in prod path |
| `krishiv-api` | `execute_local` / `execute_remote`, TTL wiring |
| `krishiv-exec` | State-backed operators only (remove prod in-memory-only path) |
| `krishiv-state` | RocksDB executor backend, TTL list_keys fix |
| `krishiv-checkpoint` | Object store backend, fsync, fencing helpers |
| `krishiv-shuffle` | Spill, caps, shuffle service library |
| `krishiv-connectors` | rdkafka watermark path |
| `krishiv-flight-sql` | Catalog sync + policy default deny |
| `krishiv` (CLI) | `cluster`, `job`, remote coordinator commands |
| **new:** `krishiv-clusterd` | Bare metal CCP binary (thin wrapper over scheduler CCP) |
| **new:** `krishiv-shuffle-svc` | Optional shuffle daemon (WS-6.7) |

---

## 8. Gap closure matrix

Every GAP from [r12-maturity-gap-register.md](./r12-maturity-gap-register.md) and [gap-analysis-2026-05-23.md](../engineering/gap-analysis-2026-05-23.md) maps to a workstream task. **No gap is left “future release”.**

| Gap ID | Workstream task |
|--------|-----------------|
| GAP-CP-01 | WS-1.6, WS-7.2 |
| GAP-CP-02 | WS-3.6 |
| GAP-CP-03 | WS-3.5 |
| GAP-CP-04 | WS-3.2 |
| GAP-CP-05 | WS-3.3 |
| GAP-CP-06 | WS-3.4 |
| GAP-CP-07 | WS-3.9 |
| GAP-CP-08 | WS-3.10 |
| GAP-CP-09 | WS-4.3 |
| GAP-CP-10 | WS-1.1 |
| GAP-CP-11 | WS-3.7 |
| GAP-CP-12 | WS-7.10 |
| GAP-RT-01 | WS-2.5 |
| GAP-RT-02 | WS-9.2 |
| GAP-RT-03 | WS-2.3 (replaces fragments) |
| GAP-RT-04 | WS-9.6 |
| GAP-RT-05 | WS-4.8 |
| GAP-RT-06 | WS-4.9 |
| GAP-RT-07 | WS-9.4 |
| GAP-RT-08 | WS-9.3 |
| GAP-ST-01..06 | WS-2, WS-5 |
| GAP-SH-01..05 | WS-6 |
| GAP-CK-01..04 | WS-3.5, WS-5 |
| GAP-CN-01..07 | WS-10 (01 merge/fix, 02 kafka, 03 cert, 06/07 S3 2PC) |
| GAP-GV-01..05 | WS-9.5 (03), row-level R20 **included as WS-10 follow-on** — implement `PolicyHook` row filters in WS-10.4 |
| GAP-OB-01 | WS-10.4 |
| GAP-FD-01 | WS-11 |
| GAP-K8-01 | WS-7, WS-3 |
| GAP-PY-01 | WS-9.7 |
| GAP-DOC-01 | WS-0.2, acceptance gates |
| GAP-C1..C10 | WS-3, WS-4, WS-5, WS-6 |
| GAP-I1..I6 | WS-2, WS-4, WS-10 |
| GAP-T1..T4 | WS-10, WS-5, WS-0 |
| GAP-B1..B5 | WS-0 CI/toolchain |

---

## 9. CI and certification contract (L5)

| Gate | Command / job | Required evidence |
|------|---------------|-------------------|
| Unit | `cargo test --workspace --lib` | 0 failures |
| Lint | `cargo clippy --workspace -- -D warnings` | 0 warnings |
| Format | `cargo fmt --check` | clean |
| In-process distributed | `cargo test -p krishiv-scheduler --test coordinator_executor_integration` | pass |
| Multi-process E2E | `cargo test --test distributed_e2e` | batch + streaming |
| Bare metal E2E | CI job `bare-metal-e2e` | clusterd + 2 executors |
| Kind E2E | CI job `kind-e2e` | KrishivJob → JCP pod |
| Checkpoint chaos | `cargo test -p krishiv-chaos --features e2e` | restore correctness |
| Kafka | `cargo test -p krishiv-connectors --features kafka` + compose | watermark |
| Fencing audit | `scripts/audit-fencing.sh` | all write paths call validate |
| Certification | `cargo test --test certification` | matrix-aligned |

**L5 definition for this plan:** Kind + bare-metal E2E green; chaos checkpoint test green; fencing audit clean; no `todo!()` / `unimplemented!()` in `krishiv-scheduler`, `krishiv-executor`, `krishiv-operator`, `krishiv-runtime`, `krishiv-api` distributed paths.

---

## 10. Implementation tracker checklist (copy to `docs/implementation/distributed-unified-mitigation.md`)

Use the workstream tables in §4 as the authoritative checklist. Update `docs/implementation/status.md` after each workstream gate.

**Suggested execution order:**

1. WS-0 → WS-1 → WS-2 → WS-3 (foundation)  
2. WS-4 + WS-5 in parallel (executor + state)  
3. WS-6 (shuffle)  
4. WS-7 + WS-8 in parallel (K8s + bare metal)  
5. WS-9 (API parity)  
6. WS-10 → WS-11 (connectors, autoscale, federation)

**Validation after each workstream:** run the §9 gate row(s) for that WS before merging.

---

## 11. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Large scheduler refactor | WS-1 gate: test count unchanged; feature flags `legacy-coordinator` until WS-7 cutover |
| Per-job JCP pod churn | JCP pod reuse pool for short queries; batch interactive still uses Flight-only path (ADR-DIST-15) |
| etcd operational burden | Bare metal HA optional; single-node sqlite supported with documented RPO |
| RocksDB on executor disk | PVC per executor on K8s; documented capacity planning |
| Kind CI flakiness | Retry 2x; namespace per test; kind load docker images in CI cache |

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-24 | Initial plan — full gap coverage, no deferrals |
