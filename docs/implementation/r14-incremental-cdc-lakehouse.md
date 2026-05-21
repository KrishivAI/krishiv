# R14 Incremental Computation & CDC Lakehouse Implementation Tracker

## Goal

Introduce first-class incremental computation to Krishiv: `CREATE LIVE TABLE`
SQL syntax that keeps a materialized view continuously up to date as upstream
CDC events arrive, a Python `live_table()` API, function-level memoization with
content-hash invalidation, multi-table CDC fan-out with full schema evolution
(add/rename/widen/drop columns), and an exactly-once CDC → Iceberg
certification path that aligns Kafka transactional offsets with Iceberg snapshot
IDs inside coordinator checkpoint metadata. R14 is the capstone release of the
core compute roadmap; all work in R12 and R13 must be verified green before
Sprint 1 begins.

## Scope

In scope:

- `CREATE LIVE TABLE <name> AS SELECT …` SQL syntax and planner extension in
  `krishiv-sql`.
- `ks.live_table(name, query)` Python API in `krishiv-python`.
- `@ks.transform(memo=True)` function-level memoization with content-hash key
  and cross-restart persistence.
- Multi-table CDC fan-out: a single `CdcEventSource` drives multiple downstream
  live tables, each with independent schema evolution tracking.
- Schema evolution operations: add column (nullable), rename column, widen
  numeric types, drop column (with deprecation warning).
- `ChangeFeed` output: `(op: insert | update | delete, row: Batch)` streaming
  from any live table.
- Exactly-once CDC → Iceberg: Kafka transactional producer + Iceberg two-phase
  commit, with Kafka committed offset stored in Iceberg snapshot summary and in
  coordinator checkpoint metadata.
- Coordinator barrier alignment: barrier metadata carries `kafka_offset` and
  `iceberg_snapshot_id` so recovery starts from a consistent point.

Out of scope:

- New window or aggregation operator types (shipped in R13).
- New connector types beyond Kafka and Iceberg for exactly-once.
- JVM or Spark compatibility layer.
- Cross-cluster live table federation.

## Dependencies

- R12 acceptance gate is complete: all P0 bugs fixed, Kafka connector live,
  rdkafka with transactional producer available (`features = ["kafka"]`).
- R13 acceptance gate is complete: Python API live, `ks.Session`,
  `ks.read_kafka()`, `ks.sinks.iceberg()` all working.
- `cargo test --workspace` and `pytest python/tests/` both pass clean on the
  R13 baseline.
- Iceberg catalog (REST or Hive Metastore) reachable in the integration test
  environment.

## Architectural Decisions Required

### ADR-R14-01: Incremental View Delta Storage

**Problem**

Live table materialization requires storing per-row deltas (inserts, updates,
deletes) between CDC event ingestion and the next Iceberg snapshot commit.
Three storage strategies have meaningfully different cost and complexity
profiles.

**Options**

- A. Iceberg equality-delete files: deltas are written as Iceberg
  equality-delete files targeting the live table itself. Natural fit — no
  separate storage tier, reads merge base + deletes at scan time. Expensive for
  high-frequency updates (O(deletes) scan overhead per read); not suitable for
  tables with > 10 k updates/second.
- B. Internal Krishiv change log: deltas are appended to a redb log (for
  embedded mode) or a dedicated Kafka topic (for distributed mode) and replayed
  on demand to materialize the current state. Decouples ingestion rate from
  Iceberg commit frequency. Requires a compaction job to prevent unbounded log
  growth.
- C. Delta Lake merge files: write Parquet `_delta_log` entries for each CDC
  batch. Reuses Delta Lake's MVCC merge semantics. Requires `delta-rs` as a new
  dependency and constrains the lakehouse to Delta Lake format, conflicting with
  the existing Iceberg-first strategy.

**Recommendation**

Option B. The internal change log decouples CDC ingestion throughput from
Iceberg commit latency and is consistent with the checkpoint-barrier model
already in place. Use redb for embedded/single-node mode and a Kafka compacted
topic for distributed mode. A background compaction task (configurable interval,
default 60 s) merges the log into the base Iceberg table and truncates the log.

**Risk if deferred**

Without a clear delta storage strategy, Sprint 1 (`CREATE LIVE TABLE` planner)
cannot be completed — the planner must reference the delta store type in the
physical plan.

---

### ADR-R14-02: Memoization Key Design for @ks.transform(memo=True)

**Problem**

`@ks.transform(memo=True)` must produce a content-hash key that covers: (1)
the function's bytecode/code hash (to invalidate when the implementation
changes), (2) all input RecordBatch data (to detect changed inputs), and (3)
the schema of the inputs (to handle schema evolution). The key must survive
coordinator restarts, meaning it must be stored durably.

**Options**

- A. SHA-256 of `(function_code_bytes || schema_json || data_bytes)`: fully
  deterministic, language-agnostic. `function_code_bytes` is computed from
  `inspect.getsource()` on the Python side and hashed before crossing into
  Rust. Durable storage in redb under a `memo:` key namespace.
- B. SHA-256 over Arrow IPC serialization of the batch + a function UUID
  assigned at decoration time: decouples the hash from the function source
  text, which means source code changes do not automatically invalidate the
  cache. Simpler to compute but requires manual cache eviction when logic
  changes.
- C. Redis-backed memoization: use Redis `SETNX` with the SHA-256 key. Survives
  coordinator restarts across machines, supports distributed invalidation. Adds
  a required Redis dependency that is not acceptable for embedded mode.

**Recommendation**

Option A. Covering the function source text in the hash is a correctness
requirement: a change to the transform logic must invalidate prior results.
`inspect.getsource()` is the standard Python mechanism; its output is hashed on
the Python side and passed to Rust as a `[u8; 32]`. Storage in redb (embedded)
or the internal change-log Kafka topic (distributed) keeps the dependency graph
flat (no Redis).

**Risk if deferred**

Without a memoization key design, the `@ks.transform(memo=True)` API cannot
guarantee correctness across restarts. A stale cached result returned after a
logic change would be a silent data-correctness bug.

---

### ADR-R14-03: Schema Evolution in-Flight for Arrow RecordBatches

**Problem**

When a new column appears mid-stream (e.g., the upstream Kafka schema is
widened), in-flight `RecordBatch` objects have a different schema than batches
already written to the Iceberg table or already buffered in the live table delta
log. Sending mismatched schemas to the sink causes Arrow IPC errors.

**Options**

- A. Schema-merge buffer: maintain a `SchemaMerger` struct in the streaming
  operator pipeline that accumulates the union of all observed schemas and
  casts / null-fills each incoming batch to the merged schema before forwarding.
  Implemented in `krishiv-exec` as a `SchemaNormalizeOperator`.
- B. Versioned schema registry per live table: store each observed schema
  version in the coordinator; assign a monotonic version ID. Executors tag
  each batch with its schema version. The sink reads the registry to merge.
  More flexible but adds a round-trip to the coordinator per schema change.
- C. Reject schema changes mid-stream and require a savepoint + restart: the
  simplest correctness guarantee, but unacceptable for a live-table feature
  that promises zero-downtime schema evolution.

**Recommendation**

Option A. The `SchemaNormalizeOperator` is a self-contained Arrow-level
component that can be unit-tested independently of the full streaming stack. It
handles the four supported evolution operations (add nullable column, rename,
widen numeric, drop column) using Arrow `cast` and `null_array` fill. It is
inserted automatically by the planner when a live table's schema version in the
delta log differs from the current batch schema.

**Risk if deferred**

Without schema normalization, any upstream schema change mid-stream will crash
the live table operator with an Arrow schema-mismatch panic. This is a
crash-class risk for the CDC fan-out feature in Sprint 3.

## Sprint 1 — CREATE LIVE TABLE Planner Extension

### S1.1: SQL parser extension — krishiv-sql

- [ ] Extend the DataFusion SQL parser (or a pre-parse rewrite) to recognize
      `CREATE LIVE TABLE <name> AS <query>` and `REFRESH LIVE TABLE <name>`.
- [ ] Introduce `LogicalPlan::CreateLiveTable { name, query, delta_store }` and
      `LogicalPlan::RefreshLiveTable { name }` variants.
- [ ] Add `DROP LIVE TABLE <name>` for completeness.
- [ ] Add parser unit tests for each new statement form, including error cases
      (missing `AS`, unsupported `SELECT *` expansion).

**Validation**: `cargo test -p krishiv-sql -- live_table`

### S1.2: Physical plan lowering — krishiv-sql, krishiv-exec

- [ ] Lower `LogicalPlan::CreateLiveTable` to a physical
      `CreateLiveTableExec` node that:
      (a) registers the target table in the catalog,
      (b) initializes the delta store (ADR-R14-01, Option B — redb for
      embedded, Kafka topic for distributed),
      (c) inserts a `SchemaNormalizeOperator` (ADR-R14-03) between the source
      and the delta writer.
- [ ] Lower `LogicalPlan::RefreshLiveTable` to a `RefreshLiveTableExec` that
      runs the compaction job (merge delta log → Iceberg base table).

**Validation**: `cargo test -p krishiv-sql && cargo test -p krishiv-exec`

### S1.3: ks.live_table() Python API — krishiv-python

- [ ] Implement `ks.live_table(name: str, query: str | Stream) -> LiveTable`
      that submits a `CREATE LIVE TABLE` plan to the session.
- [ ] `LiveTable.refresh()` triggers `REFRESH LIVE TABLE`.
- [ ] `LiveTable.drop()` triggers `DROP LIVE TABLE`.
- [ ] Add `.pyi` stubs and docstrings.

**Validation**: `pytest python/tests/test_live_table.py::test_create_live_table`

### S1.4: Delta store — krishiv-lakehouse

- [ ] Implement `DeltaStore` trait with `append(batch: RecordBatch, op:
      DeltaOp)`, `scan() -> Vec<(DeltaOp, RecordBatch)>`, and `truncate()`.
- [ ] Implement `RedbDeltaStore` for embedded mode.
- [ ] Implement `KafkaDeltaStore` for distributed mode (gated on
      `features = ["kafka"]`).
- [ ] Add unit tests for both implementations covering append, scan, and
      truncate.

**Validation**: `cargo test -p krishiv-lakehouse`

## Sprint 2 — Function-Level Memoization Engine

### S2.1: Content-hash computation — krishiv-python, krishiv-exec

- [ ] On the Python side, compute `SHA-256(source_bytes || schema_json ||
      arrow_ipc_bytes)` in the `@ks.transform(memo=True)` decorator before
      calling into Rust.
- [ ] Pass the 32-byte hash to `MemoCache::lookup` in `krishiv-exec`.
- [ ] Add tests asserting: (a) same inputs + same source → cache hit; (b) same
      inputs + changed source → cache miss; (c) different inputs → cache miss.

**Validation**: `pytest python/tests/test_memo.py::test_cache_invalidation`

### S2.2: MemoCache durable storage — krishiv-exec, krishiv-lakehouse

- [ ] Implement `MemoCache` backed by `RedbDeltaStore` (embedded) or the
      coordinator's Kafka compacted topic (distributed) under the `memo:`
      key namespace.
- [ ] `MemoCache::lookup(key: [u8; 32]) -> Option<RecordBatch>`.
- [ ] `MemoCache::store(key: [u8; 32], batch: RecordBatch)`.
- [ ] Eviction: LRU with a configurable `max_entries` (default 10 000).
- [ ] Add tests for lookup hit/miss, store, and LRU eviction boundary.

**Validation**: `cargo test -p krishiv-exec -- memo`

### S2.3: @ks.transform(memo=True) decorator — krishiv-python

- [ ] Implement the decorator: on call, compute the content hash, call
      `MemoCache::lookup`; on miss, invoke the user function, call
      `MemoCache::store`, return the result.
- [ ] Expose `transform.cache_info()` (hits, misses, size) mirroring Python's
      `functools.lru_cache` API.
- [ ] Add `.pyi` stubs.

**Validation**: `pytest python/tests/test_memo.py`

## Sprint 3 — Multi-Table CDC Fan-Out & Schema Evolution

### S3.1: SchemaNormalizeOperator — krishiv-exec

- [ ] Implement `SchemaNormalizeOperator` that accepts a target `Arc<Schema>`
      and an incoming `RecordBatch` with a potentially different schema.
- [ ] Handle: (a) add nullable column → fill with null array; (b) rename
      column → remap by position if metadata carries old/new name mapping; (c)
      widen numeric type → Arrow `cast`; (d) drop column → silently omit.
- [ ] Emit `ExecError::IncompatibleSchemaEvolution` for unsupported changes
      (e.g., narrowing a type, changing nullability from nullable to non-null).
- [ ] Add tests for each of the four supported evolution operations.

**Validation**: `cargo test -p krishiv-exec -- schema_normalize`

### S3.2: Multi-table CDC routing — krishiv-connectors, krishiv-exec

- [ ] Implement `CdcRouter` that reads from a single `CdcEventSource` and
      routes events to per-table `DeltaStore` instances based on the event's
      `table_name` field.
- [ ] Each routed channel has its own `SchemaNormalizeOperator` initialized from
      the live table's current catalog schema.
- [ ] On schema evolution event (event type `schema_change`), update the
      `SchemaNormalizeOperator` target schema and log the evolution step.

**Validation**: `cargo test -p krishiv-connectors -- cdc_router && cargo test -p krishiv-exec`

### S3.3: ChangeFeed output — krishiv-exec, krishiv-python

- [ ] Add `ChangeFeed` struct: `op: DeltaOp`, `batch: RecordBatch`.
- [ ] Implement `LiveTable.change_feed() -> Stream<ChangeFeed>` in
      `krishiv-python` that subscribes to the live table's delta log.
- [ ] Expose `async for change in table.change_feed()` via the asyncio bridge
      (ADR-R13-01).
- [ ] Add tests that insert, update, and delete rows and assert the change feed
      emits the corresponding `(op, row)` tuples in order.

**Validation**: `pytest python/tests/test_change_feed.py`

## Sprint 4 — Exactly-Once CDC → Iceberg Certification

### S4.1: Kafka transactional producer — krishiv-connectors

- [ ] Enable rdkafka transactional producer: call `init_transactions()` at
      connector startup; call `begin_transaction()` / `commit_transaction()` /
      `abort_transaction()` around each batch.
- [ ] Store the `committed_kafka_offset` per topic-partition in the batch's
      metadata map before forwarding to the Iceberg sink.

**Validation**: `cargo test -p krishiv-connectors --features kafka -- transactional`

### S4.2: Iceberg two-phase commit — krishiv-lakehouse

- [ ] Implement `IcebergTwoPhaseCommit`: `prepare(batch) -> StagedSnapshot` and
      `commit(staged, kafka_offsets: HashMap<TopicPartition, i64>)`.
- [ ] Store `kafka_offsets` in the Iceberg snapshot summary under the key
      `krishiv.kafka.committed_offsets` as a JSON string.
- [ ] On `abort`, call Iceberg `expire_snapshot` on the staged snapshot ID.
- [ ] Add integration tests using a local Iceberg REST catalog (testcontainers).

**Validation**: `cargo test -p krishiv-lakehouse -- two_phase`

### S4.3: Coordinator barrier alignment — krishiv-scheduler, krishiv-proto

- [ ] Extend the `BarrierMetadata` proto message with:
      `kafka_offsets: map<string, int64>` and
      `iceberg_snapshot_id: optional uint64`.
- [ ] In `CheckpointCoordinator::initiate_checkpoint`, populate both fields
      from the current Kafka transactional state and the latest staged Iceberg
      snapshot ID.
- [ ] On recovery, `recover_from_store` reads the barrier metadata and rewinds
      the Kafka consumer to `kafka_offsets` before resuming.
- [ ] Add a test simulating a mid-flight crash (checkpoint initiated, Iceberg
      not committed) and asserting recovery rewinds Kafka to the last consistent
      offset.

**Validation**: `cargo test -p krishiv-scheduler -- barrier_alignment && cargo test -p krishiv-proto`

### S4.4: Exactly-once end-to-end integration test

- [ ] Write an integration test that:
      (1) Produces 10 000 Kafka messages via the transactional producer.
      (2) Runs a live table pipeline that consumes and writes to Iceberg.
      (3) Kills the coordinator mid-run (simulated panic).
      (4) Restarts and resumes from the checkpoint.
      (5) Asserts the Iceberg table contains exactly 10 000 rows (no
          duplicates, no gaps).
- [ ] Gate the test behind `#[cfg(feature = "exactly-once-integration")]`.

**Validation**: `cargo test -p krishiv-lakehouse --features exactly-once-integration -- exactly_once`

## Test Checklist

- [ ] `cargo clippy --workspace -- -D warnings` passes.
- [ ] `cargo test -p krishiv-sql` — `CREATE LIVE TABLE` parser and planner tests.
- [ ] `cargo test -p krishiv-exec` — `SchemaNormalizeOperator`, memo cache, barrier tests.
- [ ] `cargo test -p krishiv-lakehouse` — delta store, two-phase commit, integration tests.
- [ ] `cargo test -p krishiv-connectors --features kafka` — transactional producer, CDC router tests.
- [ ] `cargo test -p krishiv-scheduler` — barrier alignment and recovery tests.
- [ ] `cargo test -p krishiv-proto` — barrier metadata round-trip tests.
- [ ] `pytest python/tests/test_live_table.py` — Python live table API tests.
- [ ] `pytest python/tests/test_memo.py` — memoization tests.
- [ ] `pytest python/tests/test_change_feed.py` — ChangeFeed asyncio tests.
- [ ] `cargo test --workspace` — full Rust suite passes.
- [ ] `pytest python/tests/` — full Python suite passes.

## Acceptance Gate

R14 is complete when:

- [ ] `CREATE LIVE TABLE orders_summary AS SELECT customer_id, SUM(amount) AS
      total FROM orders GROUP BY customer_id` executes without error, creates
      a live table registered in the catalog, and updates when new `orders` CDC
      events arrive.
- [ ] `@ks.transform(memo=True)` returns a cached result on repeated calls with
      the same input batch and source text; returns a fresh result after the
      function source is changed (verified by hash invalidation test).
- [ ] A three-table CDC fan-out (orders, products, customers) runs concurrently
      from one `RdkafkaCdcEventSource` with independent schema tracking for
      each table.
- [ ] A mid-stream `ADD COLUMN discount FLOAT` schema evolution event causes the
      live table to null-fill the new column in all prior rows and accept the
      new column in subsequent rows, with no crash or schema-mismatch error.
- [ ] The exactly-once integration test (S4.4) passes: 10 000 messages →
      exactly 10 000 Iceberg rows after a simulated coordinator crash and
      restart.
- [ ] `async for change in table.change_feed()` emits `(insert, row)`,
      `(update, row)`, and `(delete, row)` tuples in arrival order (verified by
      `test_change_feed.py`).
- [ ] `cargo test --workspace` and `pytest python/tests/` both pass with zero
      failures.
- [ ] `cargo clippy --workspace -- -D warnings` passes.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Iceberg two-phase commit and Kafka transactions have independent failure modes — partial commit leaves the table inconsistent | S4.4 integration test specifically covers the partial-commit failure scenario; recovery is proven by the test before merging |
| redb delta store grows unbounded if the compaction job is never triggered | Compaction is triggered automatically every 60 s by the live table scheduler; the `DeltaStore::truncate` path is covered by unit tests |
| `inspect.getsource()` returns different output on different Python versions (e.g., comment stripping) | Hash is computed after `ast.unparse(ast.parse(source))` normalization to remove formatting differences |
| Schema-merge buffer delays batch delivery when a schema change event arrives out of order | `SchemaNormalizeOperator` buffers at most one batch while waiting for the schema registry update; timeout of 5 s raises `ExecError::SchemaEvolutionTimeout` |
| rdkafka transactional producer requires Kafka broker ≥ 2.5 (EOS v2) | Document minimum broker version in `docs/engineering/standards.md`; integration test container uses Kafka 3.6 |
