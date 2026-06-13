# Stable API Implementation TODO

This is the executable checklist for
[`stable-public-api-plan.md`](stable-public-api-plan.md). A checked item means
code, compatibility metadata, tests, and documentation exist; it does not mean
the whole phase is complete. Machine-readable phase and capability status lives
in `api/stable-api.toml`.

## Phase A — Inventory and stability

- [x] Define stable/preview/experimental/internal policy.
- [x] Add machine-readable phase and cross-language capability manifest.
- [x] Generate checked-in Rust, Python, and SQL public inventories.
- [x] Reject duplicate Python class names.
- [x] Add inventory/parity validation to project-hygiene CI.
- [x] Rename the legacy Python unified wrapper from `DataFrame` to `Relation` so
      the module has one canonical `DataFrame` identity.
- [x] Record individual method stability, documentation URL, and deprecation
      replacement in generated inventories.
- [x] Add an approved-baseline diff format that classifies additive, breaking,
      and semantic changes.
- [x] Generate Python type stubs and Rust public API semver reports in CI.

## Phase B — Expression and type AST

- [x] Define versioned engine-owned expression nodes and scalar values.
- [x] Define engine-owned decimal, timestamp/timezone, interval, nested type, and
      nullability semantics.
- [x] Convert Rust expression constructors from SQL strings to AST nodes.
- [x] Add AST serialization/version validation to plan envelopes.
- [x] Lower AST nodes to DataFusion expressions inside `krishiv-sql`.
- [x] Add Python `Column`, operators, functions, and window expressions.
- [x] Add SQL/Rust/Python normalized-AST golden tests.
- [x] Keep raw SQL expressions as an explicit preview escape hatch.

Implementation details and the intentionally preview-only generic/raw nodes are
recorded in [`phase-b-expression-ast.md`](phase-b-expression-ast.md).

## Phase C — Canonical DataFrame and catalog

- [x] Establish one canonical Python `DataFrame` class identity.
- [x] Make boundedness explicit plan metadata on the canonical DataFrame.
- [x] Move unique `Relation`/`Stream`/`StreamingDataFrame` behavior onto the
      canonical DataFrame before deprecation.
- [x] Complete joins, set operations, null handling, deduplication, sampling,
      statistics, grouping sets, cube/rollup, pivot/unpivot, and windows.
- [x] Add typed catalog, namespace, table, view, and function APIs.
- [x] Add prepared statements and typed bind parameters.
- [x] Add cross-language relational conformance tests in every execution mode.

Implementation details are recorded in
[`phase-c-dataframe-catalog.md`](phase-c-dataframe-catalog.md).

## Phase D — I/O, connectors, and Iceberg

- [x] Replace rejected generic options with typed format/connector options.
- [x] Make canonical Rust load/save/table resolution async.
- [x] Add writer modes, partitioning, sort order, distribution, file sizing, and
      schema evolution.
- [x] Route file, Kafka, database, and Iceberg I/O through common builders.
- [x] Implement distributed atomic commit/abort.
- [ ] Implement correct Iceberg append/overwrite/delete/update/merge and
      schema/partition evolution.
- [ ] Pass connector recovery and exactly-once certification suites.

Implementation details and remaining native-driver certification work are recorded in
[`phase-d-io-iceberg.md`](phase-d-io-iceberg.md).

## Phase E — Query lifecycle and async correctness

- [ ] Add typed query ID, handle, status, progress, failure, and result stream.
- [ ] Route collect, writes, and stream submission through one query handle.
- [ ] Add coordinator-backed cancellation, timeout, progress, and completion.
- [ ] Add explicit `krishiv::blocking` facade using one owned runtime.
- [ ] Remove hidden runtime creation/blocking from normal Rust APIs.
- [ ] Convert every Python `*_async` method into a genuine asyncio awaitable.
- [ ] Propagate Python interrupts and client disconnect cancellation.

## Phase F — Structured streaming

- [ ] Add canonical `read_stream` and `write_stream` builders.
- [ ] Add append/update/complete output modes and changelog validation.
- [ ] Add continuous, processing-time, once, and available-now triggers.
- [ ] Add query name, checkpoint location, restart, table sink, and
      `foreach_batch`.
- [ ] Complete watermarks, late-data policy, deduplication, windows,
      stream-table, stream-stream, and temporal joins.
- [ ] Add streaming query lifecycle and repeated failure/recovery tests.

## Phase G — Stateful process API

- [ ] Add distributed map/flat-map/filter/process plan nodes.
- [ ] Add stable operator UID, parallelism, and max-parallelism.
- [ ] Add typed value/list/map/reducing/aggregating state descriptors.
- [ ] Add event-time and processing-time timers.
- [ ] Add side outputs, connected streams, co-process, broadcast state, and
      async I/O.
- [ ] Define Rust/Python user-code serialization and resource limits.
- [ ] Pass savepoint rescaling and task/coordinator failure tests.

## Phase H — SQL and gateway

- [ ] Publish a generated grammar and feature matrix.
- [ ] Complete catalog/database/table/view/function DDL.
- [ ] Complete atomic insert/overwrite/update/delete/Iceberg merge.
- [ ] Complete joins, grouping sets, windows, recursive CTE, lateral/unnest,
      pivot/unpivot, temporal queries, and supported row patterns.
- [ ] Add prepared statements, parameters, SQLSTATE mapping, operation IDs,
      cancellation, and timeouts.
- [ ] Stabilize Flight SQL and add separately versioned JDBC/ODBC gateway tests.

## Phase I — 1.0 release gate

- [ ] Stable API baseline contains no unreviewed breaking changes.
- [ ] Rust/Python/SQL parity manifest has no unexplained stable gaps.
- [ ] Type/null/time/decimal/ordering/overflow conformance passes.
- [ ] Embedded, single-node, and distributed conformance passes.
- [ ] Plan/checkpoint/savepoint fixtures restore across supported versions.
- [ ] Certified streaming delivery combinations pass failure loops.
- [ ] Every stable API has reference docs and runnable examples.
- [ ] TPC-H/Nexmark baselines have no unexplained release blocker.
- [ ] Reproducible binaries, wheels, type stubs, SBOM, checksums, and provenance
      are produced from the release tag.
- [ ] All preview API removals have migrations and release notes.
