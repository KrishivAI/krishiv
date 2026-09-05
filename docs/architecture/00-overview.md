# Krishiv architecture — overview

Krishiv is a Rust compute engine that runs **three compute models** — bounded
batch SQL, change-driven incremental view maintenance, and event-time streaming
— on **one runtime**, across **three placements** — embedded in a process, a
single-node daemon, and a distributed coordinator/executor cluster — behind
**one set of surfaces** — Rust, Python, SQL over Arrow Flight, a CLI, HTTP, and
MCP.

This document is the map. Each subsystem has its own document in this
directory; every one of them describes what the code does today, with the
crate and module that does it. Where a capability is preview or experimental
the document says so, because the maturity vocabulary is a contract
(`../contracts/engine-semantics.md`), not a mood.

## The three axes

`krishiv-engine-core` fixes the vocabulary the rest of the workspace uses:

| Axis | Values | Where it is decided |
|---|---|---|
| **Engine** (compute model) | `EngineKind::Batch`, `EngineKind::Incremental`, `EngineKind::Streaming` | on the `CompiledJob` every front-end produces |
| **Placement** | `ExecutionPlacement::LocalInProcess`, `SingleNodeDaemon`, `RemoteClusterRequired` | by `SessionBuilder` / `KRISHIV_MODE`, never by a Cargo feature |
| **Surface** | Rust `Session`/`DataFrame`, Python `krishiv`, SQL (Flight SQL, CLI, HTTP), MCP | whichever the caller uses; all lower to the same plan contract |

The axes are independent. A `Streaming` job can run embedded; a `Batch` job can
run on a cluster; the SQL and Python surfaces compile to the same
`CompiledJob` and are routed by `krishiv_engines::run_job` to the same three
engines. Cargo features (`../architecture/15-configuration.md`) gate optional
dependency families — Kafka, Iceberg, etcd, the Kubernetes operator — and are
never used as mode switches.

## Invariants

These hold across every crate and are enforced by tests, lints, and the
fail-closed checks named in the subsystem documents.

1. **One runtime for batch and streaming.** Plans, coordinator, executors,
   shuffle, connectors, and observability are shared. There is no separate
   streaming engine.
2. **Exactly one fenced coordinator owns a job.** API replicas may be
   active-active; scheduling ownership for one job is not. Stale coordinators
   cannot commit task or checkpoint state (`04-scheduler-and-coordinator.md`).
3. **Executors are replaceable.** Recovery never depends on the survival of one
   executor process (`05-executor-and-data-plane.md`).
4. **Apache Arrow `RecordBatch` is the data model** — in memory, over IPC, and
   in shuffle files. Incremental data is a `DeltaBatch`: a `RecordBatch` with an
   `i64` weight column (`09-incremental-view-maintenance.md`).
5. **DataFusion owns SQL parsing, planning, and local execution** unless a
   Krishiv rule or operator explicitly overrides it. DataFusion types are not
   public API and not wire format (ADR-0002).
6. **Mode and placement are separate decisions**, and distributed mode never
   falls back to local execution (`01-execution-modes.md`).
7. **State, shuffle, checkpoint, metadata, and connectors live behind crate
   APIs and durability profiles** (`14-deployment-and-durability.md`).
8. **Typed IDs, typed fragments, typed errors, and capability flags** at every
   public boundary; no string routing.
9. **Exactly-once is a property of a certified source/sink/checkpoint/profile
   combination**, never a blanket claim (`10-connectors-and-lakehouse.md`).
10. **Every durable envelope carries a format version, and unknown versions are
    rejected**, never treated as the newest known (`18-compatibility-and-versioning.md`).

## Component map

```text
  Rust API · Python · CLI · Arrow Flight SQL · HTTP · MCP          (11)
                          │
                          ▼
             krishiv-api  Session · DataFrame · StreamingDataFrame
                          IncrementalFlow · QueryHandle · Pipeline
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
   krishiv-sql      krishiv-plan     krishiv-engine-core / -engines
   DataFusion seam  engine IR        CompiledJob · EngineKind · run_job
   rewrites,        fragments,       Batch · Incremental · Streaming
   optimizer rules, optimizer, UDF,
   catalogs (2,3)   governance (3)   (0, 8, 9)
          │               │                │
          └───────────────┼────────────────┘
                          ▼
                  krishiv-runtime                                 (1)
        mode × placement × transport routing
        in-process cluster · local daemon · remote Flight/gRPC
                          │
                          ▼
              krishiv-scheduler  coordinator                      (4)
   admission · stages · placement · fencing · leadership
   checkpoint epochs · AQE · metadata · HTTP control plane
                          │
                          ▼
              krishiv-executor  workers                           (5)
   task attempts · fragment kinds · capacity · memory pool
        │           │            │             │
        ▼           ▼            ▼             ▼
   -dataflow    -shuffle      -state      -connectors
   operators    exchange      keyed       sources, sinks, 2PC,
   windows,     spill,        state,      lakehouse (Iceberg
   watermarks,  tiered,       RocksDB,    first), Kafka, S3,
   joins (8)    runtime       checkpoints JDBC, CDC … (10)
                filters (6)   (7)
                          │
                          ▼
        krishiv-metrics · krishiv-ui · krishiv-operator         (12–14)
```

Numbers are the documents in this directory.

### Crate ownership

| Crate | Owns | Document |
|---|---|---|
| `krishiv` | The unified binary: `sql`, `explain`, `stream`, `ivm`, `table`, `doctor`, `capabilities`, `local`, `cluster`, and the daemons (`coordinator`/`clusterd`, `job-coordinator`, `executor`, `flight-server`, `shuffle-svc`, `mcp`) | 11, 14 |
| `krishiv-api` | `Session`/`SessionBuilder`, `DataFrame`, `StreamingDataFrame`, `IncrementalFlow`/`IncrementalDataFrame`, `QueryHandle`, prepared statements, typed expressions, reader/writer builders, the declarative `Pipeline`, the blocking facade, and the connector-backed embedded `EngineRuntime` | 11, 08, 09 |
| `krishiv-engine-core` | `EngineKind`, `CompiledJob`, the `ComputeEngine` contract, runtime services (checkpoint, changelog sink, consolidating and upsert sink wrappers) | 00, 09 |
| `krishiv-engines` | `BatchEngine`, `IncrementalEngine`, `StreamingEngine`, and `run_job` dispatch | 00 |
| `krishiv-sql` | `SqlEngine`: the DataFusion seam — SQL pre-processors, statement intercepts, the CTE cache and grouping-set rewrite, optimizer rules, join and memory policies, distributed plan fragments, Iceberg/REST/Glue/Unity/Postgres catalogs, vector search, Python UDF execution | 02, 03 |
| `krishiv-plan` | Engine-owned logical/physical IR, the versioned expression AST, typed task fragments, the plan-level optimizer and CBO statistics, UDF contracts, CEP pattern matcher, governance interfaces | 03 |
| `krishiv-runtime` | `ExecutionRuntime`: embedded in-process cluster, single-node daemon client, distributed Flight/gRPC client; continuous-stream and IVM job backends | 01 |
| `krishiv-scheduler` | Cluster control plane, per-job coordinators, admission, staged batch planning, AQE, checkpoints and barriers, leadership (single/etcd), metadata stores (memory/RocksDB/etcd), the HTTP control plane | 04 |
| `krishiv-executor` | The executor process: assignment inbox, fragment execution (batch plan, streaming run-loop classes, resident IVM), shuffle write buffers, result spools, barrier service, two-phase transaction registry | 05 |
| `krishiv-dataflow` | Arrow operators: tumbling/sliding/session/count windows, watermarks, interval/temporal/watermark/delta joins, CEP, dedup, process functions, broadcast and connected streams, barrier alignment, the streaming driver kernel | 08 |
| `krishiv-shuffle` | Partitioners, memory/disk/object-store/tiered stores, sort and push shuffle writers, Arrow Flight and HTTP shuffle services, leases, orphan cleanup, cross-stage runtime filters | 06 |
| `krishiv-state` | `StateBackend` (memory, RocksDB, DFS-primary), timers, TTL, key groups and rescaling, migrations, checkpoints (full and incremental), savepoints, queryable state | 07 |
| `krishiv-delta` | `DeltaBatch`, `Trace`, `SourceState`, the incremental operator algebra, lateness, behavior versioning, snapshot index | 09 |
| `krishiv-ivm` | `IncrementalFlow` driver, view decomposition, key-partitioned flows, provenance, spill, window rewrite, vector-sink maintenance | 09 |
| `krishiv-connectors` | Source/sink contracts, capabilities, maturity, driver registry, two-phase commit, Parquet/CSV/JSON/Avro/S3/Kafka/Kinesis/Pulsar/JDBC/Elasticsearch/Cassandra/HBase, Iceberg (native, filesystem, REST), Delta Lake, Hudi, CDC, vector sinks, quality rules | 10 |
| `krishiv-proto` | Typed IDs, job/task/checkpoint/executor wire types, service traits, protobuf conversions | 04, 18 |
| `krishiv-flight-sql` | The Arrow Flight SQL service and per-session hardening | 11, 12 |
| `krishiv-sql-gateway` | In-process SQLSTATE-mapping facade over `BlockingSession` (not a wire server) | 11 |
| `krishiv-mcp` | Model Context Protocol server over `Session` | 11 |
| `krishiv-python` | PyO3 bindings and the `krishiv` Python package | 11 |
| `krishiv-operator` | Kubernetes CRDs (`KrishivJob`, `KrishivQueue`, `KrishivExecutorPool`), reconciler, per-job coordinator pods, admission webhook | 14 |
| `krishiv-ui` | Status API and the embedded web console | 13 |
| `krishiv-metrics` | OpenTelemetry metrics/traces/logs, gRPC trace propagation, log ring, observability report | 13 |
| `krishiv-common` | Durability profiles, env-flag registry, executor capacity, memory budgets, backpressure, validation, production guards, the `block_on` bridge | 01, 05, 12, 15 |
| `krishiv-conformance` | sqllogictest corpus run against all three placements | 17 |
| `krishiv-chaos` | Cross-crate fault-injection tests | 17 |
| `krishiv-bench` | TPC-H, TPC-DS, NEXMark, ClickBench harnesses | 16 |

## One request, end to end

A batch SQL statement submitted through any surface takes this path
(the streaming and incremental paths are in `08` and `09`):

1. **Surface → `Session`.** `krishiv_api::Session::sql` receives the text. In
   the CLI this is `krishiv sql`; over Flight SQL it is `do_get_statement`;
   from Python it is `Session.sql()`; from MCP it is `execute_sql`.
2. **`SqlEngine::sql` — the pre-planning seam** (`02-sql-engine.md`). Krishiv's
   own SQL extensions are rewritten to DataFusion SQL (pipe syntax, Spark
   extensions, PIVOT, streaming TVFs, `AS OF`), introspection and DDL
   statements are intercepted, multi-statement scripts are split, UDFs are
   synced, and the plan cache is consulted.
3. **DataFusion plans it.** Parse → logical plan. Before the optimizer runs,
   two Krishiv rewrites act on the unoptimized plan: a grouping-set aggregate
   becomes one finest-level aggregate plus re-aggregation, and a CTE referenced
   more than once is materialised once (`02`, §"pre-optimizer rewrites").
4. **Optimizer.** DataFusion's rule set plus Krishiv's logical rules
   (join reorder, semi-join reduction, late materialisation, ANN pre-filter)
   and physical rules (spillable join selection, cooperative yielding) run
   (`03-planning-and-optimization.md`).
5. **Routing.** `ExecutionRuntime` decides by placement (`01`): in-process
   DataFusion for embedded; the local daemon over Flight/gRPC for single-node;
   the remote coordinator for distributed. Distributed never falls back.
6. **Coordinator.** The physical plan is cut at repartition boundaries into
   stages; each stage into one task per partition, carried as a
   `dfplan:v1:` typed fragment (ADR-0003). Admission, placement, fencing, and
   — at stage boundaries — adaptive re-optimisation from measured shuffle
   output (`04`).
7. **Executors.** Decode the plan fragment, execute one partition of it,
   write hash-partitioned shuffle output or spool the result; the winning
   attempt alone publishes completion (`05`, `06`).
8. **Result.** Inline Arrow IPC for small results, a disk-backed spool for
   large ones, delivered back through the same surface.

## Data model

- **`RecordBatch`** everywhere. Operators never own a private row format.
- **`DeltaBatch`** (`krishiv-delta`): a `RecordBatch` whose last column is an
  `i64` weight — `+1` insert, `-1` retract, `0` cancelled. Every incremental
  operator is defined over this Z-set algebra and consolidates by summing
  weights on equal rows.
- **Typed task fragments** (`krishiv-plan::TypedTaskFragment`, version 1):
  the single wire carrier for work between coordinator and executor. The body
  kind is a prefix — `dfplan:v1:` (batch), `stream:*` (streaming loop classes),
  `delta:attach|tick|detach` (resident IVM).
- **Operator state address**: `(job_id, stable_operator_id, state_name,
  key_group)`. Restore requires a matching serializer version or a registered
  migration.

## What Krishiv is not

The engine does not own notebooks, workflow orchestration, billing, a managed
SQL warehouse, enterprise catalog administration, dashboards, model serving,
or agent products. Those use Krishiv through the surfaces in `11`. Anything of
that kind belongs outside the engine crates, and the boundary is a governance
invariant (`../GOVERNANCE.md`).

## Reading order

New to the engine: `01` (modes) → `02` (SQL seam) → `04`/`05` (control and
data plane) → `10` (connectors). Working on streaming: `08` → `07` → `06`.
Working on incremental: `09` → `07`. Operating a cluster: `14` → `12` → `13`
→ `15`. Measuring anything: `16` → `17`.
