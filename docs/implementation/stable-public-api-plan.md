# Stable Public API Plan: Rust, Python, and SQL

## Goal

Reach a coherent, production-usable 1.0 public API for Rust, Python, and SQL
without freezing accidental implementation details. This plan implements
[ADR-0002](../decisions/0002-public-api-shape-and-execution-semantics.md).

The target is not exhaustive Spark or Flink compatibility. Spark SQL 4.1.x is
the breadth baseline for sessions, DataFrames, expressions, catalogs,
reader/writer APIs, UDFs, and structured streaming. Flink 2.2's Table/SQL and
DataStream APIs are the semantic baseline for unified bounded/unbounded tables
and lower-level state, timers, watermarks, partitioning, and process functions.

## Definition of stable and complete

A public feature is **stable** only when:

1. its Rust, Python, and SQL names and semantics are documented;
2. embedded, single-node, and distributed behavior is specified;
3. blocking, async, cancellation, timeout, and backpressure behavior is explicit;
4. errors are typed in Rust and mapped to documented Python/SQL errors;
5. plans and expressions have versioned serialization when they cross a process;
6. compatibility, conformance, failure, and recovery tests exist;
7. metrics and progress identify the operation consistently;
8. unsupported combinations fail before execution rather than silently falling
   back; and
9. examples and generated API inventories are checked in CI.

A language binding is **complete** when every stable engine capability is either
exposed with equivalent semantics or listed in a machine-readable parity file
with an approved reason for omission. Identical spelling is not required.

## Target public architecture

```text
                         SQL text
                            |
Rust Expr/DataFrame ----> public AST <---- Python Expr/DataFrame
                            |
                    analyzer + optimizer
                            |
                   versioned executable plan
                            |
                    async QueryHandle API
                     /        |         \
                embedded  single-node  distributed
                     \        |         /
              shared operators/state/shuffle/connectors
```

Two user abstraction levels are supported:

1. **Relational:** `Session`, `DataFrame`, `Expr`, catalog, readers/writers,
   grouped/windowed tables, SQL, and structured streaming.
2. **Process:** `DataStream`, partitioning, process functions, keyed state,
   timers, watermarks, side outputs, connected streams, and async I/O.

Both lower into the same plan/runtime and use the same `QueryHandle`.

## Stable surface target matrix

| Capability | Rust | Python | SQL/Flight | Stable requirement |
|---|---|---|---|---|
| Session/configuration | Typed builder plus runtime config | Sync builder/connect plus async remote connect | Session properties | Same validation, defaults, and redaction |
| Expressions/types | Structured `Expr` AST | `Column`/`Expr`, operators, functions | SQL grammar and bind parameters | Same analyzer, coercion, and normalized AST |
| Relational transformations | Canonical `DataFrame` | One canonical `DataFrame` | `SELECT` query language | Cross-language result/error conformance |
| Catalog/namespaces | Async catalog API | Sync convenience plus async remote API | DDL, `USE`, `SHOW`, `DESCRIBE` | Same identifiers and metadata model |
| Batch execution | Async `QueryHandle`; blocking facade | Blocking plus genuine awaitables | Operation ID over Flight SQL | One submission/cancellation/result path |
| Batch I/O | Async load/save, typed options | Blocking plus async load/save | Table/file DDL and DML | Atomic distributed commit |
| Structured streaming | Relational stream reader/writer | Relational stream reader/writer | Dynamic-table SQL | Same changelog, time, and recovery semantics |
| Query lifecycle | Typed async handle | `Query` with awaitables/async iterators | Operation status/cancel endpoints | Status, progress, stop, timeout, savepoint |
| Process/state API | Rust-first typed process API | Python process API with explicit serialization limits | Not directly exposed | Shared state/timer/operator identity contracts |
| UDFs | Scalar/aggregate/table/async | Scalar/vectorized/aggregate/table/async | Function DDL and invocation | Type, determinism, resource, and failure contract |

## Migration strategy from the current preview API

1. Add inventory and deprecation metadata before renaming or removing anything.
2. Introduce the structured AST and query handle behind existing methods, then
   migrate public signatures; do not rewrite execution twice.
3. Add canonical async Rust methods and the blocking facade in the same release.
4. Convert Python `*_async` methods to awaitables with temporary compatibility
   shims only where call-site detection is reliable; otherwise make the break in
   a documented pre-1.0 minor release.
5. Select one Python `DataFrame` class, preserve import aliases for one minor
   release where possible, and reject ambiguous duplicate registration.
6. Mark old `Relation`, `Stream`, and `StreamingDataFrame` constructors
   deprecated after equivalent canonical paths exist.
7. Remove adapters only after examples, Python type stubs, migration tests, and
   release notes point to replacements.
8. Never migrate durable plan/state formats implicitly; use version checks and
   explicit readers or migration tools.

## Public API ownership

| Contract | Owning crate | Stable facade |
|---|---|---|
| Expressions and relational transformations | `krishiv-api` | `krishiv` and `krishiv-python` |
| SQL grammar, analysis, catalog bridge | `krishiv-sql` | `Session::sql`, Flight SQL, SQL gateway |
| Plan/wire representation | `krishiv-plan`, `krishiv-proto` | Not exposed as implementation types |
| Query submission and lifecycle | `krishiv-runtime` | `QueryHandle` / `StreamingQuery` |
| Low-level process API | `krishiv-dataflow`, `krishiv-api` | `krishiv::stream` |
| Source/sink SDK | `krishiv-connectors` | reader/writer builders and connector SDK |
| State/checkpoint/savepoint | `krishiv-state` | process state and query lifecycle APIs |
| Python binding | `krishiv-python` | `krishiv` Python package |

## Phase A — Freeze the intended surface, not the current accidents

### Work

- Generate inventories for Rust public re-exports, Python classes/methods, SQL
  statements/functions, configuration keys, connector options, and protocols.
- Label every item stable/preview/experimental/internal.
- Define naming, error, null, type coercion, identifier, timezone, decimal, and
  overflow conventions.
- Define a feature parity manifest keyed by engine capability rather than method
  name.
- Mark `Relation`, `Stream`, and `StreamingDataFrame` compatibility status and
  stop adding independent relational features to them.
- Resolve the duplicate Python `DataFrame` identity before adding more methods.

### Deliverables

- `api/rust-public.json`, `api/python-public.json`, `api/sql-public.json`.
- `api/parity.toml`.
- API review checklist and CI baseline-diff job.
- Deprecation annotations and migration page for every duplicate surface.

### Exit gate

No public symbol, Python method, SQL statement, or configuration key can be
added without owner, stability, documentation, and parity metadata.

## Phase B — Canonical expression and type system

### Work

- Replace SQL-string-only expressions with a versioned engine-owned AST.
- Preserve identifiers, literals, functions, casts, aliases, sort/null ordering,
  window specifications, field access, and bind parameters structurally.
- Introduce `DataType`, `ScalarValue`, decimal, timestamp/timezone, interval,
  list, map, struct, and nullability contracts independent of DataFusion.
- Implement Rust operator traits where unambiguous and named methods where Rust
  operator semantics would be surprising.
- Add Python `Column`/`Expr`, operator overloads, `functions`, and `Window`.
- Lower SQL and language expressions through one analyzer and produce identical
  normalized plans for equivalent queries.
- Keep `Expr::raw` and Python `expr()` explicitly preview and validate them at
  analysis time.

### Minimum stable function families

- Comparison, boolean, arithmetic, bitwise, null, conditional, and casts.
- String, binary, date/time, interval, numeric, hash, regex, and JSON.
- Array, map, struct, generator, aggregate, and window functions.
- Determinism, volatility, nullability, and return-type metadata for UDF calls.

### Exit gate

Golden tests prove equivalent Rust, Python, and SQL expressions produce the same
normalized AST and results, including nulls, decimals, timestamps, nested data,
and invalid-type errors.

## Phase C — Canonical DataFrame and catalog API

### Work

- Keep one public `DataFrame` identity per language with boundedness as metadata.
- Complete projection, filter, joins, set operations, ordering, limits, aliases,
  column operations, null handling, deduplication, sampling, statistics, pivot,
  unpivot, grouping sets, cube, rollup, and window operations.
- Add `table`, temporary view, namespace, catalog, function, and metadata APIs.
- Add prepared SQL and typed bind parameters.
- Separate lazy `describe` plans from executing actions such as `show`.
- Implement cache/persist/unpersist and DataFrame checkpoint semantics only after
  memory accounting and distributed storage behavior are specified.

### Required action model

- Rust actions are async-first and return/consume `QueryHandle`.
- Python offers blocking finite actions plus genuine asyncio alternatives.
- SQL/Flight operations expose operation IDs and cancellation.
- `collect`, `first`, `take`, `count`, `is_empty`, and local iteration share one
  execution path and enforce configurable result-size limits.

### Exit gate

A language-neutral relational conformance suite runs the same cases through
Rust DataFrame, Python DataFrame, SQL, embedded, and distributed modes.

## Phase D — Reader, writer, connector, and Iceberg contract

### Work

- Make reader/writer configuration lazy and synchronous; make `load`, `save`,
  table resolution, and commits async at the canonical Rust boundary.
- Add typed per-format options, schema, multiple paths, partition discovery,
  projection/filter pushdown controls, and malformed-record policy.
- Add writer modes, partitioning, sort order, distribution, target file size,
  schema evolution, dynamic overwrite, and table writes.
- Route files, Kafka, JDBC-compatible databases, and Iceberg through common
  source/sink builders and capability negotiation.
- Implement distributed atomic commit and abort; expose no successful write
  before coordinator-approved commit.
- Complete Iceberg-first append, overwrite, row-level delete/update/merge,
  schema/partition evolution, branches/tags, and streaming commits.
- Keep Delta/Hudi optional until they independently pass the same contract.

### Exit gate

Connector certification tests cover discovery, replay, offset restore,
backpressure, schema changes, commit/abort, task retry, coordinator failover,
and duplicate/loss detection for each advertised guarantee.

## Phase E — Query lifecycle and correct sync/async behavior

### Work

- Introduce typed `QueryId`, `QueryHandle`, `QueryStatus`, `QueryProgress`,
  `QueryFailure`, and `QueryResultStream`.
- Support async submit, status refresh, progress subscription, cancellation,
  timeout, result streaming, completion, checkpoint, and savepoint operations.
- Make `collect` and writes convenience consumers of `QueryHandle`.
- Add `krishiv::blocking::{Session, DataFrame, QueryHandle}` backed by one owned,
  reusable runtime; detect and reject unsafe runtime nesting.
- Remove hidden runtime creation/blocking from ordinary Rust library methods.
- Convert Python `*_async` methods to actual asyncio awaitables and async
  iterators; ensure blocking methods release the GIL and propagate interrupts.
- Carry cancellation from Python/Rust client through Flight/gRPC to the fenced
  coordinator and active task attempts.

### Exit gate

Cancellation, timeout, client disconnect, coordinator restart, and partial
result tests pass in all runtime modes without leaked tasks or divergent sync
and async semantics.

## Phase F — Structured streaming API

### Work

- Add `read_stream` and `write_stream` using the same relational expressions and
  schemas as batch.
- Define append/update/complete output modes based on changelog semantics.
- Add continuous/default, processing-time, once, and available-now triggers.
- Add query name, checkpoint location, restart, `foreach_batch`, table sinks,
  and progress metrics.
- Complete event-time attributes, watermarks, late-data policy, deduplication,
  tumbling/sliding/session windows, stream-table joins, stream-stream joins, and
  temporal joins.
- Expose a `StreamingQuery` specialization of `QueryHandle` with
  `await_termination`, `stop`, `recent_progress`, and savepoint operations.
- Make bounded sources use the same API and terminate naturally.

### Exit gate

The same relational pipeline runs against bounded files and unbounded Kafka
without changing transformations, and repeated failure tests verify the
published delivery matrix.

## Phase G — Lower-level stateful processing API

### Work

- Add immutable `DataStream<T>`/Arrow-stream plan nodes for source,
  map/flat-map/filter, process, union, connect, partition, and sink.
- Add explicit operator UID and parallelism/max-parallelism metadata.
- Add typed value/list/map/reducing/aggregating state descriptors.
- Add event-time and processing-time timers, current watermark, side outputs,
  connected/co-process functions, broadcast state, and async I/O.
- Define serialization, closure capture, code deployment, resource, and sandbox
  contracts for Rust and Python user functions.
- Provide explicit Table/DataFrame ↔ DataStream conversion with changelog and
  schema validation.

### Exit gate

Rescaling/savepoint compatibility tests restore keyed state across parallelism
changes, and process-function tests pass under task retry and coordinator
failover.

## Phase H — SQL completeness and gateway stability

### Work

- Publish a versioned grammar and feature matrix.
- Complete catalog/database/table/view/function DDL and `SHOW`/`DESCRIBE`.
- Complete atomic `INSERT`, overwrite, `UPDATE`, `DELETE`, and Iceberg `MERGE`.
- Complete joins, grouping sets, windows, recursive CTE, lateral/unnest, pivot,
  unpivot, temporal queries, and the supported `MATCH_RECOGNIZE` grammar.
- Add prepared statements, bind parameters, SQLSTATE mapping, operation IDs,
  cancellation, timeout, and multi-statement transaction policy.
- Stabilize Flight SQL first; add JDBC/ODBC through a separately versioned SQL
  gateway rather than embedding driver behavior in `krishiv-api`.

### Exit gate

SQL logic tests, DataFusion compatibility tests, Flight SQL protocol tests, and
cross-language result/error conformance pass against the published matrix.

## Phase I — Stability and 1.0 release gate

### Required gates

- **API baseline:** no unreviewed stable Rust/Python/SQL removals or signature
  changes.
- **Parity:** all stable capabilities have Rust, Python, and SQL status recorded.
- **Semantics:** null, type coercion, timestamp, decimal, ordering, and overflow
  suites pass across languages.
- **Modes:** embedded, single-node, and distributed conformance passes.
- **Durability:** checkpoint/savepoint/plan fixtures restore across supported
  release versions.
- **Streaming:** failure loops pass for every certified delivery combination.
- **Documentation:** every stable item has reference docs and a runnable example.
- **Performance:** representative TPC-H/Nexmark workloads have recorded baselines
  and no unexplained release-blocking regression.
- **Packaging:** Rust docs, Python wheels/type stubs, SQL grammar, binaries, SBOM,
  checksums, and provenance are reproducible from the release tag.
- **Deprecation:** every removed preview API has a migration path and release note.

Krishiv 1.0 is not declared while any P0 item below remains.

## Priority backlog

### P0 — architecture blockers

1. Remove duplicate Python `DataFrame` class identity.
2. Choose and document the canonical DataFrame; freeze independent growth of
   `Relation`, `Stream`, and `StreamingDataFrame` compatibility surfaces.
3. Introduce the structured expression/type AST and versioned serialization.
4. Introduce `QueryHandle` and route all execution through it.
5. Make Rust execution async-first and add the explicit blocking facade.
6. Make Python `*_async` APIs genuine awaitables.
7. Implement functional typed reader/writer options and distributed commit.
8. Replace simulated/partial Iceberg DML with correct atomic operations or mark
   it unsupported.
9. Add operation ID, cancellation, timeout, and progress transport.
10. Generate API inventories and enforce compatibility/parity in CI.

### P1 — complete compute-engine API

- Full relational transformation and function families.
- Catalog/namespaces/views/functions and prepared statements.
- Structured streaming reader/writer/query lifecycle.
- Stream joins, watermarks, deduplication, output modes, and triggers.
- Lower-level process functions, state, timers, partitioning, and async I/O.
- Rust/Python scalar, aggregate, table, vectorized, and async UDF contracts.
- Iceberg-first table evolution and maintenance APIs.
- Connector author test kit and certification automation.

### P2 — post-core depth

- Advanced statistics and sketches.
- Cache/storage-level policy and user-visible adaptive execution controls.
- Broader SQL pattern recognition and optimizer extension APIs.
- Additional certified connectors and optional lakehouse formats.
- GPU resource/execution extensions after CPU semantics are stable.

## Explicit non-goals for engine API completion

The 1.0 engine does not require collaborative notebooks, multi-job workflow
orchestration, billing, managed warehouses, enterprise governance UI, dashboard
products, model registry/serving, or AI-agent products. Those are data-platform
layers that may consume the stable engine APIs.

It also does not require Spark RDD/JVM binary compatibility, Spark Connect wire
compatibility, Flink savepoint binary compatibility, MLlib, GraphX, or every
connector shipped by Spark/Flink.

## Implementation order and dependency chain

```text
inventory/stability labels
          |
          v
expression + type AST -----> SQL/DataFrame/Python conformance
          |                              |
          v                              v
canonical DataFrame/API ----------> reader/writer/catalog
          |                              |
          +--------------+---------------+
                         v
                    QueryHandle
                         |
              +----------+----------+
              v                     v
     structured streaming    blocking/async facades
              |
              v
      process/state/timer API
              |
              v
       1.0 compatibility gates
```

Do not implement broad method parity before expression, query-handle, and
identity decisions are complete; otherwise each new method increases migration
cost.

## Executable checklist

The checked/unchecked implementation list is maintained in
[`stable-api-todo.md`](stable-api-todo.md). Machine-readable phase and language
status is maintained in `api/stable-api.toml`; generated public snapshots live
in `api/*-public.json` and are validated by `scripts/check_api_surface.py`.

## Tracking format

Each implementation issue must include:

- capability ID and phase;
- owning crate and public facade;
- Rust/Python/SQL parity impact;
- batch/streaming and execution-mode applicability;
- sync/async and cancellation semantics;
- plan/wire/durable compatibility impact;
- conformance and failure tests;
- documentation and migration requirements; and
- stability level on completion.

## External API baselines

- [Spark SQL/PySpark public API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)
- [Flink Table API and SQL](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/)
- [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream-v2/overview/)

These are comparison baselines, not compatibility promises. Krishiv's normative
contract is its own versioned documentation and conformance suite.
