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
- Durability is selected through explicit profiles: `dev-local`,
  `single-node-durable`, and `distributed-durable`.

## Workspace Map

| Crate | Current responsibility |
|---|---|
| `krishiv` | User-facing facade and CLI binary. |
| `krishiv-common` | Shared utilities used across runtime and engine crates. |
| `krishiv-api` | Session, DataFrame, Stream, and public Rust API surface. |
| `krishiv-sql` | DataFusion integration, SQL execution helpers, SQL policy hooks, catalog and table-provider abstractions (`catalog` module). |
| `krishiv-plan` | Logical/physical plans, the versioned public expression/type AST, UDF contracts, governance/audit/policy, CEP pattern matcher, and optimizer rules. |
| `krishiv-runtime` | Embedded, single-node, and remote runtime routing. |
| `krishiv-dataflow` | Arrow operator runtime, queues, barriers, windows, joins, stateful ops. |
| `krishiv-scheduler` | Coordinator, job/task lifecycle, metadata stores, leadership, gRPC server. |
| `krishiv-executor` | Executor process, task runner, task assignment receiver, shuffle/checkpoint hooks. |
| `krishiv-proto` | Typed IDs and coordinator/executor wire contracts. |
| `krishiv-shuffle` | In-memory, local disk, object-store, and Flight-oriented shuffle support. |
| `krishiv-state` | In-memory and RocksDB-backed keyed state, TTL, migration, incremental state, and checkpoint/savepoint storage. |
| `krishiv-connectors` | Source/sink contracts, capability and maturity metadata, Parquet/Kafka/S3 paths, and Iceberg-first lakehouse helpers. Delta/Hudi/vector integrations are optional and experimental. |
| `krishiv-operator` | Kubernetes CRD and operator integration. |
| `krishiv-ui` | Status API and web UI assets. |
| `krishiv-flight-sql` | Arrow Flight SQL service. |
| `krishiv-sql-gateway` | Separately versioned JDBC/ODBC SQL gateway facade. |
| `krishiv-python` | PyO3 Python bindings. |
| `krishiv-metrics` | Metrics, tracing, and debug report structures. |
| `krishiv-chaos` | Cross-crate chaos and fault-injection integration tests. |
| `krishiv-bench` | Benchmarks (on-demand; excluded from default workspace builds). Schema registry helpers live in `krishiv-connectors`'s `schema-registry` feature. |

## Runtime Modes

```text
SQL / API / Flight
  -> Session + catalog
  -> DataFusion + Krishiv plan
  -> ExecutionRuntime
       Embedded + LocalInProcess: in-process cluster
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
- Production coordinator and executor task-control gRPC require bearer-token
  auth via `KRISHIV_COORDINATOR_BEARER_TOKEN` and
  `KRISHIV_EXECUTOR_TASK_BEARER_TOKEN`; anonymous gRPC is for dev-local only.
- Coordinators may accept a startup-time rotation window of comma/newline
  separated server tokens via `KRISHIV_COORDINATOR_BEARER_TOKENS`; clients still
  send the active `KRISHIV_COORDINATOR_BEARER_TOKEN`.
- Long-lived coordinator servers can also reload mounted token files using
  `KRISHIV_COORDINATOR_BEARER_TOKEN_FILE`,
  `KRISHIV_COORDINATOR_BEARER_TOKENS_FILE`, and a positive
  `KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS`.
- Kubernetes manifests and CRDs live in `k8s/`.
- Bare-metal/VM operation is process-managed: run coordinator and executors
  directly and point clients at the configured endpoints.

## Build Feature Matrix

Execution mode is selected at runtime through `RuntimeMode`,
`ExecutionPlacement`, session builders, and environment variables. Cargo
features select compiled capabilities and optional dependency families only.
Because Cargo features are additive, do not use them as mutually exclusive mode
switches.

Rust `krishiv` facade feature presets:

| Feature | Purpose |
|---|---|
| `minimal` | Smallest facade surface; no optional deployment capabilities. |
| `local` | Default developer build; embedded plus single-node capabilities. |
| `embedded` | In-process API use; intentionally has no optional dependencies. |
| `single-node` | Local daemon/in-process cluster support with Flight SQL, shuffle, and RocksDB metadata. |
| `distributed` | Bare remote cluster support with Flight SQL, shuffle, and etcd metadata. |
| `bare-metal` | Alias for distributed process-managed deployments. |
| `cluster` | Compatibility alias for `distributed`. |
| `k8s` | Distributed support plus Kubernetes operator/CRD capability. |
| `full` | Standard compute-engine build: distributed/Kubernetes, Kafka, and primary Iceberg support; excludes AI/vector and secondary lakehouse formats. |

Rust optional integration features:

| Feature | Purpose |
|---|---|
| `flight-sql` | Arrow Flight SQL transport/server support. |
| `shuffle` | Shuffle service/store support. |
| `etcd` | etcd-backed scheduler metadata and coordination. |
| `kafka` | Kafka connector support. |
| `state` | Connector/state integration. |
| `iceberg` | Primary/default lakehouse platform. |
| `delta` | Optional experimental Delta compatibility. |
| `ui` | Operator UI integration. |

Recommended Rust build commands (`just` is the project command runner):

```bash
just check              # verify all four modes compile
just check-embedded
just check-single-node
just check-distributed
just check-k8s

just build-single-node  # debug binary for local dev
just build-bare-metal   # release binary for VMs
just build-k8s          # release binary + operator for Kubernetes

just docker-local       # multi-stage build → load into k3s
just deploy-k8s         # kubectl apply -k k8s/operator
```

Python bindings default to the lean local/remote API surface. Optional native
extension features are enabled only for integration families:

| Python feature | Purpose |
|---|---|
| `kafka` | Kafka sources/connectors. |
| `iceberg` | Iceberg lakehouse bindings. |
| `ai` | Deprecated compatibility feature for optional vector sinks; no RAG/LLM engine functionality. |
| `vector-sinks` | Optional platform-adjacent vector sink compatibility. |
| `qdrant` | Experimental Qdrant vector sink. |
| `pgvector` | Experimental pgvector sink. |

Recommended Python build commands:

```bash
maturin develop --manifest-path crates/krishiv-python/Cargo.toml
maturin develop --manifest-path crates/krishiv-python/Cargo.toml --features iceberg
maturin develop --manifest-path crates/krishiv-python/Cargo.toml --features kafka
```


## Published Engine Contracts

The normative Phase 1 contracts are:

- `docs/contracts/engine-semantics.md` — batch/streaming semantics, delivery
  guarantees, exactly-once matrix, metadata compatibility, operator identity,
  and the Iceberg-first policy.
- `docs/contracts/connectors.md` — source/sink obligations and maturity labels
  for every in-tree connector.
- `docs/implementation/phase-1-engine-contract.md` — implementation resolution,
  completed contract work, and certification follow-ups.
- `docs/implementation/phase-4-user-apis.md` — implemented Rust/Python user API
  surface, compatibility rules, and remaining distributed/protocol work.

Apache Iceberg is the primary lakehouse platform. New lakehouse correctness and
certification work targets Iceberg before Delta Lake or Hudi.

## Durability Profiles

`DurabilityProfile` is shared by shuffle, state, checkpoint, and scheduler
configuration:

- `dev-local`: in-memory metadata/shuffle/state with ephemeral local
  checkpoints; not restart durable.
- `single-node-durable`: local RocksDB metadata, local disk shuffle, local
  RocksDB state, and local filesystem checkpoints; restart durable on one host.
- `distributed-durable`: etcd metadata, tiered (local + object store) shuffle,
  object-store checkpoints, local RocksDB state restored from checkpoints, and
  fenced coordination for multi-node deployments.

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
