# Krishiv Docs

This is the minimal project documentation surface. Treat the Rust workspace as
the source of truth; update this file only when code, crate ownership, commands,
or supported deployment modes change.

## Current Architecture

Krishiv is a Rust-native compute framework for batch SQL, streaming pipelines,
and lakehouse-oriented data work.

Core implementation choices:

- Rust 2024 with Tokio for async runtime work.
- Apache Arrow `RecordBatch` as the in-memory and IPC data model.
- DataFusion for SQL parsing, planning, expressions, and local execution.
- One runtime model across embedded, single-node, and distributed execution.
- Exactly one active job coordinator per job; executors are replaceable workers.
- Shuffle, state, checkpoint, metadata, and connector behavior live behind crate
  APIs rather than being hard-coded into one engine file.
- Checkpoint storage exposes async primitives for Tokio scheduler/executor paths
  plus sync compatibility wrappers for tests and blocking call sites.

## Workspace Map

| Crate | Current responsibility |
|---|---|
| `krishiv` | User-facing facade and CLI binary. |
| `krishiv-common` | Shared utilities used across runtime and engine crates. |
| `krishiv-api` | Session, DataFrame, Stream, and public Rust API surface. |
| `krishiv-sql` | DataFusion integration, SQL execution helpers, SQL policy hooks. |
| `krishiv-plan` | Logical and physical plan structures plus task fragment encoding. |
| `krishiv-runtime` | Embedded, single-node, and remote runtime routing. |
| `krishiv-exec` | Arrow operator runtime, queues, barriers, windows, joins, stateful ops. |
| `krishiv-scheduler` | Coordinator, job/task lifecycle, metadata stores, leadership, gRPC server. |
| `krishiv-executor` | Executor process, task runner, task assignment receiver, shuffle/checkpoint hooks. |
| `krishiv-proto` | Typed IDs and coordinator/executor wire contracts. |
| `krishiv-shuffle` | In-memory, local disk, object-store, and Flight-oriented shuffle support. |
| `krishiv-state` | In-memory, redb-backed, TTL, migration, and incremental state support. |
| `krishiv-checkpoint` | Checkpoint/savepoint metadata, storage, fencing, restore helpers. |
| `krishiv-connectors` | Connector traits and Parquet/Kafka/S3-style integration paths. |
| `krishiv-catalog` | Catalog and table-provider abstractions. |
| `krishiv-optimizer` | Optimizer rule and adaptive-planning support. |
| `krishiv-operator` | Kubernetes CRD and operator integration. |
| `krishiv-ui` | Status API and web UI assets. |
| `krishiv-flight-sql` | Arrow Flight SQL service. |
| `krishiv-python` | PyO3 Python bindings. |
| `krishiv-lakehouse` | Iceberg/Delta/Hudi-oriented lakehouse helpers. |
| `krishiv-metrics` | Metrics, tracing, and debug report structures. |
| `krishiv-governance` | Audit, lineage, and policy support. |
| `krishiv-udf` | UDF contracts and execution limits. |
| `krishiv-ai` | AI/RAG and embedding support. |
| `krishiv-vector-sinks` | Vector sink contracts and implementations. |
| `krishiv-schema-registry` | Schema registry helpers. |
| `krishiv-bench`, `krishiv-chaos`, `krishiv-cep` | Benchmarks, fault testing, and CEP support. |

## Runtime Modes

```text
SQL / API / Flight
  -> Session + catalog
  -> DataFusion + Krishiv plan
  -> ExecutionRuntime
       Embedded + LocalInProcess: in-process cluster
       SingleNode + LocalInProcess: in-process single-host runtime
       SingleNode + SingleNodeDaemon: local Flight/gRPC daemon
       Distributed + RemoteClusterRequired: remote Flight/gRPC cluster
  -> Coordinator
  -> ExecutorTaskRunner
  -> Arrow/DataFusion operators, shuffle, state, checkpoint, connectors
```

`krishiv-runtime` currently exposes a sync `ExecutionRuntime`/`ExecutionBackend`
surface. Remote calls use explicit sync-to-async boundaries internally. Do not
document this as fully async unless the code has actually changed. Checkpoint
storage is async-capable; scheduler gRPC checkpoint acks use the async path.

`RuntimeMode` and `ExecutionPlacement` are intentionally separate. `RuntimeMode`
is the user-visible mode; `ExecutionPlacement` says where data-plane work may
actually run. Distributed sessions require an explicit remote Flight endpoint
and must not silently fall back to in-process execution.

## Deployment Modes

Embedded:

- Runs inside the caller process.
- Best for tests, examples, and local API use.
- Uses in-process runtime paths.

Single-node:

- Runs all core engine pieces on one host.
- May use an in-process cluster or local coordinator/Flight endpoints.
- Uses local filesystem/state by default.

Distributed:

- Uses remote coordinator/executor transport.
- Requires an explicit Flight coordinator URL; local fallback is rejected at
  session build/runtime construction.
- Kubernetes manifests and CRDs live in `k8s/`.
- Bare-metal/VM operation is process-managed: run coordinator and executors
  directly and point clients at the configured endpoints.

## Commands

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --check

cargo run -p krishiv -- sql --query "select 1 as value"
cargo run -p krishiv -- explain --query "select 1 as value"
cargo run -p krishiv -- jobs
```

Use narrower package tests while iterating, for example:

```bash
cargo test -p krishiv-runtime
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-executor --lib
```

## Engineering Rules

- Keep changes scoped to the crate that owns the behavior.
- Prefer typed IDs, typed plans, typed errors, and capability flags over string
  routing at public boundaries.
- Avoid panics in library code except for impossible invariants.
- Do not hide blocking filesystem or database work inside async tasks.
- Add focused tests with behavior changes.
- Update `docs/implementation/status.md` only as a short handoff note; do not
  rebuild large planning documents.

## Current Handoff

Use `docs/implementation/status.md` for the latest durable session note.
