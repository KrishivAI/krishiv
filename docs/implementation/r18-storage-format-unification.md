# R18 Storage Format Unification & Time Travel Implementation Tracker

## Goal

Unify Krishiv's lakehouse storage story so that an enterprise team can connect
to any modern lakehouse without format-specific workarounds. R18 delivers
Delta Lake read/write via delta-rs, Apache Hudi snapshot and incremental
reads, Iceberg REST Catalog support for AWS Glue, Tabular, and Nessie, time
travel SQL (`TIMESTAMP AS OF`, `VERSION AS OF`), the `MERGE INTO` DML
statement, Confluent Schema Registry deserialization for Avro/Protobuf/JSON
Kafka topics, and Iceberg partition evolution. All three open table formats must
be accessible under a single Python API and SQL namespace before R19 begins.

## Scope

In scope:

- Delta Lake read and write via the `delta-rs` crate (spawn_blocking integration, see ADR-18.1).
- `session.read_delta(path, version=N)` and `df.write_delta(path, mode, merge_key, schema_evolution)` Python APIs.
- SQL table reference `delta.`s3://...`` resolved by a custom DataFusion `TableProvider`.
- Apache Hudi snapshot query and incremental query (`begin_instant`) via `krishiv-lakehouse`.
- Iceberg REST Catalog client: AWS Glue, Tabular/Nessie, and any compliant REST endpoint via `krishiv-catalog`.
- `ks.catalogs.glue()`, `ks.catalogs.nessie()`, `ks.catalogs.iceberg_rest()` Python API.
- Time travel SQL: `TIMESTAMP AS OF` and `VERSION AS OF` syntax in DataFusion via a planner extension (see ADR-18.3).
- `MERGE INTO` statement: format-specific implementations for Iceberg (native) and Delta (delta-rs) — see ADR-18.2.
- New `krishiv-schema-registry` crate: Avro deserialization via `schema-registry-converter`, Protobuf via `apache-avro`/`prost`.
- `ks.schema_registry.confluent(url, subject, format)` Python API.
- Iceberg partition evolution: add, drop, and replace partition specs without full table rewrite.

Out of scope:

- Delta Lake streaming sink with transactional producer (R19+ concern once federation is stable).
- Hudi write support (read-only for R18; write deferred to R20 if requested by users).
- MERGE INTO as a unified DataFusion LogicalPlan node — that is the R20 target per ADR-18.2.
- Custom Iceberg catalog implementations (only REST-compliant endpoints are supported).
- Schema registry schema evolution notifications (webhooks, push-based) — pull-on-deserialize only.
- Delta Lake Z-Order optimisation and OPTIMIZE command.

## Dependencies

- R12 complete: `spawn_blocking` patterns for async-context blocking I/O are established (P0.3, P0.4 fixes). Any delta-rs integration must follow the same pattern.
- R13 complete: Python API layer (`krishiv-python`) is stable enough to add `read_delta`, `read_hudi`, and catalog constructors without breaking existing `ks.read_*` surface.
- R14 complete: Iceberg snapshot-commit two-phase write is certified exactly-once. `MERGE INTO` for Iceberg uses the same snapshot commit path.
- `krishiv-lakehouse` crate exists and owns Hudi reader logic from R8.2.
- `krishiv-catalog` crate exists and owns Iceberg REST client stubs from R8.2.
- `krishiv-sql` uses DataFusion ≥ 37.0 which exposes the `OptimizerRule` and `TableProviderFactory` extension points needed for time travel and `delta.` prefix resolution.

## Architectural Decisions Required

### ADR-18.1: delta-rs Tokio Runtime Integration

**Problem**

delta-rs exposes an async API but internally creates and owns its own Tokio
runtime. Calling any delta-rs async method from within Krishiv's existing Tokio
multi-thread runtime will panic ("cannot start a runtime from within a runtime"),
the same class of defect fixed in P0.3. The delta-rs crate does not currently
expose a runtime-injection API that would allow sharing Krishiv's handle.

**Options**

- A. Wrap all delta-rs calls in `tokio::task::spawn_blocking` on a dedicated
  thread pool. Safe by construction — the blocking thread has no surrounding
  runtime. Adds one thread-hop latency per delta-rs operation (estimated 0.5–2ms
  round-trip). All checkpoint writes and reads go through this path.
- B. Configure delta-rs to use `Handle::current()` by patching its internal
  runtime initialisation. Requires delta-rs to expose a `RuntimeConfig` API that
  does not exist in any released version. Not feasible for R18 without forking
  delta-rs.
- C. Use delta-rs's synchronous API (where available) in a dedicated thread pool
  managed by `rayon`. Avoids Tokio nesting, but the sync API does not cover the
  full write path (merge, schema evolution) — only reads are fully synchronous in
  the current release.

**Recommendation**

Option A (spawn_blocking). It is the only universally safe option across all
delta-rs operations. Document the thread-hop latency as a known performance
tradeoff in `docs/architecture/checkpoint-storage.md`. Checkpoint writes will
be measurably slower than native async Arrow writes, but correctness is
non-negotiable. Revisit if delta-rs publishes a runtime-injection API by R19.

**Risk if deferred**

Choosing Option B without a working delta-rs API or Option C with incomplete
sync coverage will produce runtime panics in production on the first MERGE INTO
or schema evolution call. This is a crash-class defect identical to P0.3.

---

### ADR-18.2: MERGE INTO SQL Implementation Strategy

**Problem**

DataFusion does not support `MERGE INTO` DML as a built-in plan node.
Implementing a fully general `MERGE INTO` as a DataFusion `LogicalPlan` variant
with a corresponding physical operator is a 2–3 month effort and is not scoped
for R18. However, `MERGE INTO` is the primary write pattern for Delta and
Iceberg upsert workflows; omitting it makes R18 incomplete for enterprise CDC
pipelines.

**Options**

- A. Implement `MERGE INTO` as a new DataFusion `LogicalPlan` node and physical
  operator. Correct semantics, unified SQL behaviour across all formats. Requires
  significant DataFusion fork or upstream contribution work. Not feasible in the
  R18 window.
- B. Rewrite `MERGE INTO` as a sequence of `DELETE` followed by `INSERT`. Fast
  to implement, but semantically incorrect for `UPDATE WHEN MATCHED` clauses —
  the DELETE removes the row before the UPDATE can observe its current value,
  producing wrong results for partial-update patterns.
- C. Implement format-specific merge: route `MERGE INTO` on Delta tables to
  `delta-rs`'s native `DeltaOps::merge` API; route `MERGE INTO` on Iceberg
  tables to Iceberg's native equality-delete + append commit. Parse the
  `MERGE INTO` SQL in Krishiv's parser and dispatch based on the target table
  format before DataFusion sees the statement. Covers 90% of real-world use
  cases. Document that `MERGE INTO` on non-Delta/Iceberg tables is unsupported
  until R20.

**Recommendation**

Option C for R18. Format-specific merge is the pragmatic path that delivers
correct semantics for Delta and Iceberg without a multi-month DataFusion
investment. The tradeoff is that the same SQL syntax has different execution
paths under the hood — document this clearly in the SQL reference. Schedule
Option A (unified DataFusion plan node) as an ADR target for R20.

**Risk if deferred**

Omitting `MERGE INTO` entirely blocks CDC-to-Delta and CDC-to-Iceberg upsert
pipelines that are the primary enterprise use case for R18. Option B's incorrect
semantics would produce data corruption under partial-update merge patterns.

---

### ADR-18.3: Time Travel SQL in DataFusion

**Problem**

SQL time travel syntax (`TIMESTAMP AS OF`, `VERSION AS OF`) requires the query
planner to substitute a historical snapshot for the current table view at scan
time. DataFusion's `TableProvider` trait does not accept a snapshot parameter;
there is no standard extension point for intercepting table scans to inject
historical context.

**Options**

- A. Parse `AS OF` as a Krishiv SQL extension before DataFusion sees the
  statement. Strip the `AS OF` clause, resolve the snapshot ID from the catalog,
  and substitute a snapshot-qualified table name (e.g., `orders@snapshot=8473`)
  into the statement before handing it to DataFusion. Hackish; requires string
  manipulation of SQL before planning.
- B. Encode the snapshot ID in the table URI as a query parameter (e.g.,
  `iceberg.`s3://bucket/orders?snapshot=8473``). DataFusion routes the URI to
  the `TableProviderFactory`, which extracts the snapshot from the URI. Avoids
  custom SQL parser work but produces ugly SQL that users must write manually.
- C. Implement a DataFusion `OptimizerRule` (or `AnalyzerRule`) that detects
  `AS OF` syntax in the parsed AST, resolves the snapshot or timestamp to a
  concrete snapshot ID via the catalog, and rewrites the `TableScan` node to
  use a `SnapshotTableProvider` wrapping the base provider. Clean separation of
  concerns; DataFusion's extension API supports this pattern.

**Recommendation**

Option C. The DataFusion `AnalyzerRule` extension point is designed exactly for
rewriting logical plan nodes after parsing. Implement `AsOfAnalyzerRule` in
`krishiv-sql` that detects `TableScan` nodes with an attached `AsOf` qualifier
(populated by a custom Krishiv SQL dialect extension to the DataFusion SQL
parser), resolves the snapshot ID from the catalog client, and wraps the scan in
a `SnapshotTableProvider`. This keeps time travel logic inside DataFusion's
planning pipeline and is composable with other rules.

**Risk if deferred**

Without time travel, R18 cannot deliver `SELECT * FROM orders TIMESTAMP AS OF
...` or `VERSION AS OF ...`, which are the primary differentiators of the
lakehouse story versus a plain Parquet reader. Deferring makes the Iceberg and
Delta Lake stories weaker than Delta's native Spark support.

---

### ADR-18.4: Schema Registry Deserialization Strategy

**Problem**

Kafka topics encoded with Confluent Schema Registry embed a 5-byte magic header
(1 byte magic, 4 bytes schema ID) before the Avro, Protobuf, or JSON Schema
payload. At runtime, the schema must be fetched from the registry by ID, cached,
and used to deserialize the payload into an Arrow `RecordBatch`. Three options
exist for each format with different trade-offs in maturity, speed, and control.

**Options**

- A. Deserialize into `serde_json::Value` for all formats, then convert to Arrow
  using `arrow_json`. Universal but slow: always heap-allocates an intermediate
  JSON tree regardless of format, and loses Avro/Protobuf type precision
  (e.g., Avro `bytes` becomes JSON string).
- B. Use `apache-avro` crate directly for Avro and `prost` for Protobuf with
  hand-written Arrow conversion. Full control over deserialization and type
  mapping. Requires maintaining Arrow schema inference from Avro/Protobuf schema
  descriptors — significant implementation work.
- C. Use the `schema-registry-converter` crate for Avro (it handles magic byte
  stripping, schema fetch, and `apache-avro` decoding internally) and use Option
  B (`prost`) for Protobuf (schema-registry-converter's Protobuf support is less
  mature). For JSON Schema, use `krishiv`'s existing JSON-to-Arrow path.

**Recommendation**

Option C. The `schema-registry-converter` crate is well-tested across production
Confluent Schema Registry deployments and handles the schema-ID cache, HTTP
retry, and Avro decoding correctly. Using it for Avro avoids reimplementing
magic-byte parsing and schema caching. For Protobuf, `prost` with a hand-written
Arrow converter gives full control over the `FileDescriptorProto`-to-Arrow-schema
mapping, which is format-specific enough to justify the implementation effort.

**Risk if deferred**

Without schema registry support, `ks.read_kafka` cannot handle topics from any
Confluent or Confluent-compatible schema registry, which is the dominant schema
management approach in enterprise Kafka deployments. This blocks R18 from being
usable against real production Kafka clusters.

## Sprint 1 — Delta Lake Read/Write (delta-rs)

### S1.1: delta-rs dependency and spawn_blocking wrapper — krishiv-lakehouse

- [ ] Add `deltalake = { version = "0.17", features = ["s3", "gcs", "azure"] }` to `krishiv-lakehouse/Cargo.toml` behind `features = ["delta"]`.
- [ ] Implement `DeltaTableHandle` wrapping `deltalake::DeltaTable` inside a `spawn_blocking`-guarded async interface per ADR-18.1.
- [ ] Expose `async fn open_delta(path: &str, version: Option<i64>) -> LakehouseResult<DeltaTableHandle>`.
- [ ] Add a unit test that opens a local Delta table (test fixtures) and asserts the correct schema is returned.

**Validation**: `cargo test -p krishiv-lakehouse --features delta`

### S1.2: DeltaTableProvider for DataFusion — krishiv-lakehouse

- [ ] Implement `DeltaTableProvider: TableProvider` that delegates scans to `DeltaTableHandle::scan()` via `spawn_blocking`.
- [ ] Register `DeltaTableProviderFactory` for the `delta` URI scheme in the DataFusion session context in `krishiv-sql`.
- [ ] Add a SQL test: `SELECT count(*) FROM delta.\`/tmp/test-delta\`` against a fixture table.

**Validation**: `cargo test -p krishiv-sql --features delta`

### S1.3: Delta write path — krishiv-lakehouse

- [ ] Implement `async fn write_delta(df: DataFrame, path: &str, mode: WriteMode, merge_key: Option<&str>, schema_evolution: bool) -> LakehouseResult<()>` via `spawn_blocking` wrapping `DeltaOps`.
- [ ] Support `WriteMode::Append`, `WriteMode::Overwrite`, and `WriteMode::Merge` (the merge path dispatches to ADR-18.2 S5 work).
- [ ] Add a round-trip test: write 100k rows to a temp Delta table, read back, assert row count and schema match.

**Validation**: `cargo test -p krishiv-lakehouse --features delta`

### S1.4: Python API for Delta — krishiv (Python bindings)

- [ ] Expose `session.read_delta(path: str, version: Optional[int] = None) -> DataFrame` in `krishiv-python`.
- [ ] Expose `df.write_delta(path: str, mode: str = "append", merge_key: Optional[str] = None, schema_evolution: bool = False)` in `krishiv-python`.
- [ ] Add `.pyi` stub entries for both methods.
- [ ] Add a Python integration test using a local Delta fixture.

**Validation**: `cargo test -p krishiv-python --features delta`

## Sprint 2 — Apache Hudi Read Support

### S2.1: Hudi snapshot query — krishiv-lakehouse

- [ ] Implement `HudiSnapshotReader` in `krishiv-lakehouse` that reads the latest compacted base files for a Hudi Copy-On-Write table from S3/local.
- [ ] Parse the Hudi timeline (`.hoodie/` directory) to determine the latest valid commit instant.
- [ ] Project the file list through DataFusion's `ParquetExec` for columnar Arrow output.
- [ ] Add a test with a pre-generated Hudi CoW fixture (small, committed to `tests/fixtures/`).

**Validation**: `cargo test -p krishiv-lakehouse`

### S2.2: Hudi incremental query — krishiv-lakehouse

- [ ] Extend `HudiSnapshotReader` to accept `query_type: QueryType` and `begin_instant: Option<String>`.
- [ ] For `QueryType::Incremental`, scan only the commit files with instant > `begin_instant` from the timeline.
- [ ] Filter the base file list to those touched in the incremental window; union with the log files for MoR tables (log file reading is best-effort for R18; full MoR support deferred).
- [ ] Add a test that writes two fixture commits and asserts the incremental reader returns only rows from the second commit.

**Validation**: `cargo test -p krishiv-lakehouse`

### S2.3: HudiTableProvider for DataFusion — krishiv-lakehouse, krishiv-sql

- [ ] Implement `HudiTableProvider: TableProvider` wrapping `HudiSnapshotReader`.
- [ ] Register for `hudi` URI scheme in `krishiv-sql`'s session factory.
- [ ] Add SQL test: `SELECT count(*) FROM hudi.\`/tmp/test-hudi\``.

**Validation**: `cargo test -p krishiv-sql`

### S2.4: Python API for Hudi — krishiv (Python bindings)

- [ ] Expose `session.read_hudi(path: str, query_type: str = "snapshot", begin_instant: Optional[str] = None) -> DataFrame`.
- [ ] Add `.pyi` stub entries.
- [ ] Add a Python integration test covering both `snapshot` and `incremental` query types.

**Validation**: `cargo test -p krishiv-python`

## Sprint 3 — Iceberg REST Catalog & Schema Registry

### S3.1: Iceberg REST Catalog client — krishiv-catalog

- [ ] Extend `krishiv-catalog`'s existing Iceberg client to support the Iceberg REST Catalog specification (`/v1/config`, `/v1/{prefix}/namespaces`, `/v1/{prefix}/tables/{namespace}/{table}`).
- [ ] Implement `GlueRestCatalog`, `NessieCatalog`, and `GenericRestCatalog` each implementing a `CatalogClient` trait.
- [ ] Use `aws-sdk-glue` for Glue; use the REST HTTP client for Nessie and generic endpoints.
- [ ] Add unit tests with a mock REST server (`wiremock-rs`) for each catalog type.

**Validation**: `cargo test -p krishiv-catalog`

### S3.2: Python catalog constructors — krishiv (Python bindings)

- [ ] Expose `ks.catalogs.glue(region: str, database: str)`.
- [ ] Expose `ks.catalogs.nessie(uri: str, ref: str = "main")`.
- [ ] Expose `ks.catalogs.iceberg_rest(url: str, warehouse: Optional[str] = None)`.
- [ ] Wire catalog into `ks.Session.connect(catalog=...)` so that table resolution uses the catalog client.
- [ ] Add `.pyi` stub entries.

**Validation**: `cargo test -p krishiv-python`

### S3.3: krishiv-schema-registry crate — new crate

- [ ] Create `crates/krishiv-schema-registry/Cargo.toml` with dependencies: `schema-registry-converter = "3"`, `prost`, `apache-avro`, `arrow`.
- [ ] Implement `SchemaRegistryClient` that fetches and caches schemas by ID with `moka` or `dashmap`.
- [ ] Implement `AvroDeserializer: KafkaDeserializer` using `schema-registry-converter`.
- [ ] Implement `ProtobufDeserializer: KafkaDeserializer` using `prost` with dynamic `FileDescriptorProto` loading.
- [ ] Implement `JsonSchemaDeserializer: KafkaDeserializer` using the existing JSON-to-Arrow path.
- [ ] Add unit tests for each deserializer format against fixture byte payloads.

**Validation**: `cargo test -p krishiv-schema-registry`

### S3.4: Python schema registry API — krishiv (Python bindings)

- [ ] Expose `ks.schema_registry.confluent(url: str, subject: str, format: str)` returning a `SchemaRegistryConfig`.
- [ ] Wire `SchemaRegistryConfig` into `ks.read_kafka(schema=ks.schema_registry.confluent(...))`.
- [ ] Add `.pyi` stub entries.

**Validation**: `cargo test -p krishiv-python`

## Sprint 4 — Time Travel SQL

### S4.1: AS OF SQL parser extension — krishiv-sql

- [ ] Extend Krishiv's DataFusion SQL dialect to parse `TIMESTAMP AS OF <expr>` and `VERSION AS OF <expr>` suffixes on `FROM` table references.
- [ ] Populate a custom `TableScanOptions::as_of: Option<AsOfSpec>` field (where `AsOfSpec` is `Timestamp(i64)` or `Version(i64)`) on the resulting `LogicalPlan::TableScan`.
- [ ] Add parser unit tests for both syntax forms, including `FOR SYSTEM_TIME AS OF TIMESTAMP '...'` alias.

**Validation**: `cargo test -p krishiv-sql`

### S4.2: AsOfAnalyzerRule — krishiv-sql

- [ ] Implement `AsOfAnalyzerRule: AnalyzerRule` per ADR-18.3 that detects `TableScan` nodes with `as_of` populated.
- [ ] Resolve the `AsOfSpec` to a concrete snapshot ID via the table's `CatalogClient` (Iceberg) or `DeltaTableHandle` version number (Delta).
- [ ] Wrap the base `TableProvider` in a `SnapshotTableProvider` that passes the snapshot ID through to the underlying scan.
- [ ] Register `AsOfAnalyzerRule` in `krishiv-sql`'s session configuration.

**Validation**: `cargo test -p krishiv-sql`

### S4.3: SnapshotTableProvider — krishiv-lakehouse

- [ ] Implement `SnapshotTableProvider` wrapping any `TableProvider` with an `AsOfSpec`.
- [ ] For Iceberg: resolve snapshot ID from the catalog's snapshot list and pass to `IcebergTableProvider::scan_at_snapshot`.
- [ ] For Delta: resolve version number and call `DeltaTableHandle::open(version=N)`.
- [ ] For Hudi: map timestamp to a Hudi instant and delegate to `HudiSnapshotReader`.
- [ ] Add integration tests for each format: write two versions, time-travel to version 1, assert the old row counts are returned.

**Validation**: `cargo test -p krishiv-lakehouse`

### S4.4: Python time travel API

- [ ] Expose `session.read_iceberg(table, as_of: Optional[str] = None)` accepting an ISO-8601 timestamp string or snapshot ID integer.
- [ ] Expose `session.read_delta(path, version: Optional[int] = None)` (already added in S1.4; verify it delegates to `SnapshotTableProvider`).
- [ ] Add Python tests for each format.

**Validation**: `cargo test -p krishiv-python`

## Sprint 5 — MERGE INTO Statement

### S5.1: MERGE INTO SQL parser — krishiv-sql

- [ ] Extend the DataFusion SQL dialect to parse full `MERGE INTO target USING source ON condition WHEN ... THEN ...` syntax per the SQL:2003 standard.
- [ ] Produce a Krishiv-specific `MergeStatement` AST node with: target table reference, source table reference, join condition, and a list of `WhenClause` (matched/not-matched, action, optional filter, SET assignments or INSERT column list).
- [ ] Add parser unit tests covering: UPDATE WHEN MATCHED, DELETE WHEN MATCHED, INSERT WHEN NOT MATCHED, and combinations.

**Validation**: `cargo test -p krishiv-sql`

### S5.2: Format-specific MERGE dispatch — krishiv-sql, krishiv-lakehouse

- [ ] In `krishiv-sql`'s statement executor, detect `MergeStatement` and inspect the target table's format.
- [ ] For Delta target: convert `MergeStatement` to `DeltaOps::merge` builder calls via `spawn_blocking` (ADR-18.1). Map `WhenClause::MatchedUpdate` to `.when_matched_update`, `WhenClause::MatchedDelete` to `.when_matched_delete`, `WhenClause::NotMatchedInsert` to `.when_not_matched_insert`.
- [ ] For Iceberg target: convert to Iceberg equality-delete + append via the snapshot commit path from R14.
- [ ] Return `MergeResult { rows_inserted, rows_updated, rows_deleted }` as a single-row Arrow `RecordBatch`.
- [ ] Add integration tests for both formats: round-trip insert, then merge with all three clause types.

**Validation**: `cargo test -p krishiv-sql && cargo test -p krishiv-lakehouse`

### S5.3: Python MERGE INTO support

- [ ] Ensure `session.sql("MERGE INTO ...")` executes the dispatch path in S5.2 transparently.
- [ ] Add a Python integration test that merges a staging DataFrame into a Delta table.
- [ ] Document unsupported scenarios (non-Delta/Iceberg targets) with a clear `MergeTargetUnsupportedError`.

**Validation**: `cargo test -p krishiv-python`

## Sprint 6 — Iceberg Partition Evolution

### S6.1: Partition spec evolution API — krishiv-catalog

- [ ] Implement `IcebergCatalogClient::add_partition_field(table, transform, source_column, partition_name)`.
- [ ] Implement `IcebergCatalogClient::drop_partition_field(table, partition_name)`.
- [ ] Implement `IcebergCatalogClient::replace_partition_spec(table, new_spec)` which creates a new `PartitionSpec` without altering existing data files.
- [ ] Ensure the catalog client commits partition spec changes as a new `TableMetadata` version via the REST Catalog API.
- [ ] Add unit tests with mock catalog for each evolution operation.

**Validation**: `cargo test -p krishiv-catalog`

### S6.2: Partition-transparent scan after evolution — krishiv-lakehouse

- [ ] Extend `IcebergTableProvider::scan` to handle tables with multiple partition spec versions: older files use their original spec; newer files use the current spec.
- [ ] Implement `PartitionSpecResolver` that maps a data file's `spec_id` to the correct `PartitionSpec` version for predicate pushdown.
- [ ] Add a test: create table with spec v1, write data, evolve to spec v2, write more data, scan — assert all rows are returned correctly.

**Validation**: `cargo test -p krishiv-lakehouse`

### S6.3: Python partition evolution API

- [ ] Expose `session.catalog().alter_table(table, add_partition=..., drop_partition=..., replace_partition_spec=...)`.
- [ ] Add `.pyi` stub entries.
- [ ] Add a Python end-to-end test: evolve partition spec mid-stream; verify subsequent reads return all rows.

**Validation**: `cargo test -p krishiv-python`

## Acceptance Gate

R18 is complete when:

- [ ] Delta Lake round-trip: write 1M rows to a temp S3-backed Delta table, read back, verify exact row count and byte-level content checksum.
- [ ] Delta MERGE INTO: upsert 100k rows (mix of insert/update/delete) into a Delta table; verify no duplicates and correct final row count.
- [ ] Hudi snapshot query: read a pre-committed Hudi CoW fixture; assert schema and row count match the fixture manifest.
- [ ] Hudi incremental query: read only rows committed after a given instant; assert no rows from earlier commits are included.
- [ ] Iceberg REST Catalog: connect via `ks.catalogs.nessie()`, list tables, read a table — all three operations succeed against a live Nessie container in CI.
- [ ] Time travel: query an Iceberg table at 3 historical timestamps; each query returns the row count matching the snapshot at that time.
- [ ] `MERGE INTO` on Iceberg: run all three WHEN clause types; verify `rows_inserted + rows_updated + rows_deleted` equals the source row count.
- [ ] Schema registry: Avro-encoded Kafka topic (Confluent magic byte prefix) is deserialized into the correct Arrow schema; column names and types match the registry schema.
- [ ] Partition evolution: add a new partition field to an Iceberg table, write new data, scan the full table — all rows (old and new partitioning) are returned.
- [ ] `cargo test --workspace --features delta` passes with zero failures.
- [ ] `cargo clippy --workspace -- -D warnings` passes.
- [ ] No unconstrained `delta-rs` async calls exist outside `spawn_blocking` wrappers (verified by grep for `await` in `delta`/`deltalake` call sites outside of `spawn_blocking` closures).
