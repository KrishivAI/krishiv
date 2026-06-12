# Krishiv Architecture

This document describes the architecture implemented by the current Rust
workspace. It is intentionally descriptive rather than aspirational. Proposed
changes belong in an architecture decision record (ADR) or the public roadmap.

## 1. System boundary

Krishiv is an open-source compute framework for:

- bounded batch SQL and DataFrame workloads;
- unbounded/stateful streaming pipelines;
- Arrow-native data exchange;
- lakehouse-oriented reads and writes, with Apache Iceberg as the primary table
  format; and
- embedded, single-host daemon, and distributed deployments using one execution
  model.

The engine does **not** own collaborative notebooks, workflow orchestration,
billing, enterprise catalog administration, model serving, or other managed data
platform products. Those systems may use Krishiv through its Rust, Python,
Flight SQL, CLI, and control-plane interfaces.

Normative guarantees are defined in `docs/contracts/engine-semantics.md` and
`docs/contracts/connectors.md`. This document explains where those guarantees
are implemented.

## 2. Architectural invariants

1. Batch and streaming use the same plan, coordinator, executor, shuffle,
   connector, and observability layers.
2. Exactly one fenced coordinator owns a job at a time. API replicas may be
   active-active, but scheduling ownership for one job is not.
3. Executors are replaceable data-plane workers. Durable recovery cannot depend
   on the continued existence of one executor process.
4. Apache Arrow `RecordBatch` is the internal columnar and IPC data model.
5. DataFusion owns SQL parsing, expressions, local logical planning, and local
   physical execution unless a Krishiv abstraction explicitly overrides it.
6. Runtime mode and execution placement are separate decisions. Distributed
   mode never silently falls back to local execution.
7. State, shuffle, checkpoint, metadata, and connectors remain behind crate
   APIs and durability profiles.
8. Public boundaries prefer typed IDs, capabilities, versions, and errors over
   string routing.
9. Exactly-once is a property of a certified source/sink/checkpoint/profile
   combination, not a global engine slogan.

## 3. Component map

```text
Rust API / Python / CLI / Arrow Flight SQL
                  |
                  v
        krishiv-api: Session, DataFrame, Stream
                  |
          +-------+-------+
          |               |
          v               v
 krishiv-sql          krishiv-plan
 DataFusion bridge    engine plans, fragments,
 catalog/providers    optimizer, UDF contracts
          |               |
          +-------+-------+
                  v
           krishiv-runtime
     mode + placement + transport routing
                  |
                  v
         krishiv-scheduler coordinator
 job lifecycle, admission, fencing, assignment,
 checkpoints, recovery, metadata, control RPC
                  |
                  v
          krishiv-executor workers
 task attempts, operators, source/sink hooks,
 shuffle/state/checkpoint participation
       /          |           |          \
      v           v           v           v
 dataflow      shuffle      state      connectors
 operators     exchange     keyed      external I/O,
 barriers      and spill    state      offsets, 2PC
                  |
                  v
       metrics / tracing / status UI
```

### Workspace ownership

| Crate | Architectural ownership |
|---|---|
| `krishiv` | CLI and user-facing facade. |
| `krishiv-api` | Public Rust session, batch, stream, expression, and I/O APIs. |
| `krishiv-sql` | DataFusion integration, SQL helpers, catalogs, and table providers. |
| `krishiv-plan` | Logical/physical wrappers, typed task fragments, optimizer and UDF contracts. |
| `krishiv-runtime` | Embedded/daemon/remote placement and execution routing. |
| `krishiv-scheduler` | Coordinator state machine, admission, task assignment, checkpoints, recovery, leadership, and metadata stores. |
| `krishiv-executor` | Replaceable worker process and task-attempt execution. |
| `krishiv-dataflow` | Arrow operators, bounded queues, barriers, watermarks, windows, joins, sorting, and stateful processing. |
| `krishiv-shuffle` | Partitioning, compression, spill, tiered storage, transport, metadata, leases, and cleanup. |
| `krishiv-state` | Keyed state, RocksDB, timers, TTL, snapshots, checkpoints, savepoints, migration, and rescaling. |
| `krishiv-connectors` | Source/sink contracts, offsets, capabilities, maturity, two-phase commit, file/broker/database connectors, and lakehouse integrations. |
| `krishiv-proto` | Typed IDs and versioned coordinator/executor wire contracts. |
| `krishiv-flight-sql` | Arrow Flight SQL service and remote SQL result transport. |
| `krishiv-operator` | Kubernetes CRDs and reconciliation. |
| `krishiv-metrics` | Metrics, tracing, and debug-report structures. |
| `krishiv-ui` | Operational job/executor/checkpoint status surface. |
| `krishiv-python` | PyO3 bindings over public engine APIs. |
| `krishiv-bench` | TPC-H/Nexmark and deployment benchmark programs. |
| `krishiv-common` | Shared durability, async, validation, and fault-injection utilities. |

## 4. Runtime modes and placement

`ExecutionMode`/`RuntimeMode` describes the user-visible mode.
`ExecutionPlacement` describes where data-plane work is allowed to run.

| Mode | Placement | Control/data path | Required endpoint |
|---|---|---|---|
| Embedded | `LocalInProcess` | In-process cluster and DataFusion | None |
| Single-node | `SingleNodeDaemon` | Local Flight/gRPC daemon | Local coordinator/Flight endpoint |
| Distributed | `RemoteClusterRequired` | Remote coordinator and executors | Explicit coordinator/Flight endpoint |

A distributed session with remote execution disabled is invalid. This fail-closed
rule prevents tests or production clients from accidentally reporting a remote
job while executing data locally.

The current `ExecutionRuntime` interface is synchronous. Remote implementations
use explicit sync-to-async boundaries internally. Checkpoint storage separately
provides async operations for scheduler/executor paths.

## 5. Query and job lifecycle

### Batch SQL

1. A caller creates a `Session` and submits SQL or DataFrame transformations.
2. `krishiv-sql` uses DataFusion to parse and prepare local plans.
3. `krishiv-plan` provides engine-owned logical/physical wrappers and versioned
   task-fragment envelopes.
4. `krishiv-runtime` either executes locally or forwards SQL and table
   registrations to the configured remote service.
5. The coordinator admits the job, creates stages/tasks, assigns attempts, and
   records fenced state transitions.
6. Executors run Arrow/DataFusion operators, write shuffle partitions, and
   return result or sink completion.
7. Only the winning task attempt may publish scheduler-visible completion.

### Stateful streaming

Streaming uses the same job and task machinery with additional contracts:

- replayable/checkpointed source positions;
- event-time and processing-time timers;
- watermarks and late-data policy;
- keyed/operator state;
- barrier alignment and checkpoint acknowledgements;
- savepoints and state migration; and
- sink commit/abort coordination where supported.

A retry may replay input. The connector capability matrix determines whether the
observable result is best-effort, at-least-once, effectively-once, or exactly-once.

## 6. Planning and compatibility

Task work crosses scheduler/executor boundaries as a typed fragment envelope.
Durable profiles reject legacy untyped fragments. Unknown future envelope
versions are rejected rather than guessed.

Persisted state identity is based on:

```text
(job_id, stable_operator_id, state_name, key_group)
```

Direct restore requires a matching operator ID, state name, and serializer
version. Renames or byte-format changes require an explicit migration. The
compatibility windows for fragments, checkpoints, and savepoints are published
in `docs/COMPATIBILITY.md`.

## 7. Coordinator and metadata

The scheduler owns:

- job/stage/task/attempt lifecycle;
- executor registration and heartbeat expiry;
- admission and queue policy;
- task assignment and cancellation RPCs;
- checkpoint epochs and acknowledgements;
- recovery after coordinator/executor failure;
- fenced leadership and per-job ownership; and
- status snapshots used by HTTP/UI surfaces.

Metadata implementations are selected by durability profile:

- memory for development;
- local RocksDB-backed metadata for durable single-host deployments; and
- etcd/consensus metadata for distributed durable deployments.

The coordinator launch loop uses notifications rather than a fixed polling-only
model. Distributed ownership requires fencing tokens/leases so stale
coordinators cannot commit task or checkpoint state.

## 8. Executor and data plane

Executors receive typed task assignments and run replaceable task attempts.
Their responsibilities include:

- decoding and validating task fragments;
- constructing batch or streaming operators;
- enforcing attempt identity and cancellation;
- reading sources and writing sinks;
- producing/fetching shuffle partitions;
- snapshotting operator state; and
- reporting heartbeats, task status, checkpoint acknowledgements, and streaming
  progress.

Blocking filesystem/database work must not be hidden on async executor loops;
implementations use explicit blocking boundaries where required.

## 9. Shuffle architecture

`krishiv-shuffle` exposes a backend abstraction with:

- in-memory, local-disk, object-store, spillable, and tiered stores;
- hash, round-robin, broadcast, and range partitioning;
- Arrow IPC serialization and compression;
- partition metadata and leases;
- concurrent partition fetch;
- orphan scanning/cleanup; and
- a standalone shuffle service.

`distributed-durable` uses tiered shuffle: local disk is the fast path and object
storage provides restart/node-loss durability. Object-store-only selection is
rejected when the profile requires a local tier.

## 10. State, checkpoint, and savepoint architecture

State is namespaced by stable operator identity and supports:

- in-memory and RocksDB backends;
- batch get/put/delete and namespace inspection;
- event-time and processing-time timers;
- TTL;
- snapshots and incremental RocksDB checkpoints;
- key groups and rescaling;
- state migration; and
- queryable/inspection helpers.

A checkpoint epoch is valid only after required state and metadata are durable
and its integrity manifest is complete. Checkpoint metadata includes source
offsets, operator snapshot references, fencing information, and optional
connector/table commit metadata. Savepoints are retained user-triggered
checkpoints with versioned metadata.

## 11. Connector architecture

The connector SDK separates:

- `Source`/`DynSource` batch production;
- `CheckpointSource` offset capture and exact restore;
- `Sink`/`DynSink` batch consumption and flush;
- `OffsetCommitter` acknowledgement;
- `TwoPhaseCommitSink` prepare/commit/abort; and
- capability and maturity metadata.

Capabilities are necessary but not sufficient for an end-to-end guarantee. For
example, a transactional sink does not make a non-rewindable source exactly-once.
Connector maturity is `experimental`, `preview`, or `certified`; certification
requires the common external failure matrix described in
`docs/connector-sdk.md`.

Apache Iceberg is the primary lakehouse integration. Delta Lake, Hudi, and vector
sinks are optional compatibility integrations and are excluded from the standard
full-engine preset.

## 12. Durability profiles

| Profile | Metadata | Shuffle | State | Checkpoints | Intended use |
|---|---|---|---|---|---|
| `dev-local` | Memory | Memory | Memory | Ephemeral local | Tests and development; not restart durable |
| `single-node-durable` | Local file/RocksDB | Local disk | Local RocksDB | Local filesystem | One durable host |
| `distributed-durable` | Distributed consensus | Tiered local + object store | Local RocksDB restored from checkpoints | Object store | Multi-node production with fencing |

A profile is a cross-component contract. Components must fail startup when the
configured backend cannot satisfy the selected profile.

## 13. Security boundaries

Production control-plane paths use bearer-token authentication. Coordinator and
executor task-control tokens are separate so client-to-coordinator and
scheduler-to-executor privileges can be rotated independently. Long-lived
coordinators can reload mounted token files.

Security-sensitive boundaries include:

- coordinator/executor gRPC authentication and TLS;
- fencing tokens and lease generations;
- safe identifier/path validation for shuffle and checkpoint data;
- connector credentials and credential rotation;
- UDF execution policy and resource limits; and
- protected status/UI routes in durable profiles.

Vulnerability reporting is documented in `SECURITY.md`.

## 14. Observability

Metrics and tracing cover coordinator, executor, connector, checkpoint, shuffle,
and streaming progress paths. The operational UI exposes jobs, executors,
queues, checkpoints, SQL submission, health, readiness, and metrics endpoints.

The engine UI is operational tooling, not a collaborative data-platform
workspace. Completed-job retention and history-server behavior remain separate
roadmap items.

## 15. Deployment topology

### Embedded

```text
application process
  -> Session
  -> in-process runtime/coordinator/executor
  -> local memory/files
```

### Single host

```text
client
  -> Flight SQL / coordinator daemon
  -> local executor(s)
  -> local RocksDB + disk shuffle + filesystem checkpoints
```

### Distributed / Kubernetes

```text
clients
  -> Flight SQL / coordinator service
  -> fenced coordinator owner per job
  -> executor pool
  -> tiered shuffle + local RocksDB state
  -> object-store checkpoints
  -> etcd metadata/leases
```

Kubernetes supports direct manifests, a CRD/operator path, and a Helm chart.
Bare-metal deployments use process/systemd management with explicit durable
paths and backend flags.

## 16. Extension rules

- Add compute behavior to the crate that owns the abstraction.
- Add new wire fields compatibly and version semantic changes.
- Add stateful operators only with deterministic operator identity and migration
  behavior.
- Add connectors through the common SDK and publish capabilities/maturity.
- Add lakehouse behavior to Iceberg first unless the change is explicitly a
  compatibility fix for another format.
- Do not add platform workflow, notebook, billing, or enterprise-governance
  products to engine crates.
- Record cross-cutting or irreversible decisions as ADRs under `docs/decisions/`.

## 17. Validation strategy

The repository validates architecture at several levels:

- crate unit tests for state machines and operator semantics;
- execution-mode compile matrix for embedded, single-node, bare-metal, and K8s;
- connector and exactly-once integration tests;
- bare-metal and kind smoke tests;
- chaos/fault-injection tests;
- TPC-H and Nexmark benchmarks; and
- compatibility fixtures for durable metadata.

See `CONTRIBUTING.md`, `docs/BENCHMARKING.md`, and `docs/RELEASE.md` for commands
and release gates.
