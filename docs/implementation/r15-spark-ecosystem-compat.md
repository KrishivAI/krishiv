# R15 Spark SQL & Ecosystem Compatibility Implementation Tracker

## Goal

Deliver a Spark SQL compatibility layer so PySpark codebases can migrate to Krishiv without rewriting transformations. This includes a SparkSession Python shim, Spark Connect gRPC endpoint on the coordinator, 100+ Spark 3.5 SQL function aliases in DataFusion, dbt and Airflow integrations, a Great Expectations datasource, and a migration analyzer CLI. The compatibility target is declared, not total: a published compatibility matrix governs which Spark operations are certified.

## Scope

In scope:

- `krishiv-spark-compat` Python package: `SparkSession` shim, `col`, `avg`, `sum`, `explode`, and standard DataFrame operations (`filter`, `groupBy`, `agg`, `orderBy`, `join`, `union`).
- `SparkSession.builder.remote("sc://coordinator:7070").getOrCreate()` constructor.
- Spark Connect gRPC endpoint on the coordinator (Spark 3.4+ compatible).
- Spark SQL function coverage targeting Spark 3.5 (date/time, string, array, struct/map, window, ml/stats — 100+ functions).
- Function compatibility test suite with null-handling validation against Spark 3.5 semantics.
- `krishiv-dbt-adapter` Python package using the R8 Flight SQL server.
- `krishiv-airflow` Python package: `KrishivSubmitJobOperator`, `KrishivJobSensor`.
- Great Expectations `KrishivDatasource`.
- Migration analyzer CLI: `krishiv compat analyze <file.py>`.
- Published Spark SQL compatibility matrix (functions and operations).

Out of scope:

- Full Spark 3.5 API parity (RDD, MLlib, GraphX, Spark Streaming legacy API).
- Spark Connect plan coverage beyond TPC-H query patterns in R15.
- Spark Structured Streaming compatibility (deferred to R16/R17).
- JDBC/ODBC Spark Thrift Server emulation.
- Delta Lake writer compatibility.
- Spark UI emulation.

## Dependencies

- R12: advanced connector surface providing stable Kafka and object-store read paths.
- R13: multi-cluster federation providing coordinator gRPC extension points needed for Spark Connect.
- R14: memoization engine and content-hash infrastructure used by the migration analyzer for incremental analysis.
- R8: Flight SQL server (used by dbt adapter as the SQL transport).
- R10: GA SQL compatibility matrix format (reused for Spark function compatibility matrix).

## Architectural Decisions Required

### ADR-R15.1: Spark Connect Protocol Implementation Strategy

**Problem**: The Spark Connect protobuf schema (`spark/connect/relations.proto`, `spark/connect/expressions.proto`) has 200+ message types. A full implementation is a multi-year effort. The subset needed to run TPC-H queries is tractable in one release.

**Options**:
- A: Implement a native Krishiv Spark Connect server that translates all Spark Connect plan trees into DataFusion plans — full control, 6–12 months of engineering.
- B: Adopt the official Apache Spark Connect proto directly and implement only the subset of plan messages needed to pass TPC-H queries, with a published compatibility matrix declaring which message types are supported.
- C: Use the Sail (LakeHQ) approach — transpile Spark Connect plan messages through DataFusion's SQL string representation.

**Recommendation**: Option B with a declared compatibility matrix. Implementing the proto from source gives version-negotiation control. The matrix sets honest expectations. Unsupported plan nodes return a structured `UNIMPLEMENTED` status, not a silent error.

**Risk if deferred**: PySpark clients send plan trees that differ between 3.4 and 3.5; version negotiation is complex. If ADR is deferred past Sprint 1, the Sprint 3 Spark Connect server will be built on an unstable proto subset and require extensive rework.

---

### ADR-R15.2: DataFusion Spark SQL Function Extension Strategy

**Problem**: Adding 100+ Spark SQL functions requires a development strategy. Spark and DataFusion have different null-handling semantics for many equivalent functions. Without a compatibility test suite, aliasing built-in functions is unreliable.

**Options**:
- A: Implement each Spark function as a native DataFusion `ScalarUDF` — correct null semantics, high development cost.
- B: Alias DataFusion built-in functions where semantics match, and implement `ScalarUDF` only for functions with divergent null-handling — faster, targeted.
- C: Generate function stubs that delegate to DataFusion built-ins universally — fastest, highest risk of silent semantic differences.

**Recommendation**: Option B. Build the function compatibility test suite first (Sprint 1), classify functions by semantic equivalence, alias where safe, and implement `ScalarUDF` where semantics diverge. The test suite is the gate that makes aliasing safe.

**Risk if deferred**: Silent null-handling differences surface in user production pipelines after migration, undermining trust in the compatibility layer. The test suite must exist before any function is aliased.

---

### ADR-R15.3: dbt Adapter SQL Transport

**Problem**: dbt adapters communicate via a SQLAlchemy-compatible connection or a custom protocol. Krishiv must expose a SQL endpoint dbt can query. Two candidates exist: the Flight SQL server from R8, or a dedicated dbt protocol server.

**Options**:
- A: Use the R8 Flight SQL server as the SQL transport for the dbt adapter — no new server component, reuses existing auth and session paths.
- B: Implement a dedicated HTTP/JSON SQL server for dbt — more familiar protocol, new operational surface.
- C: Implement a SQLAlchemy dialect directly via the Python DB-API 2.0 interface over Flight SQL — cleanest dbt integration path, requires a `krishiv-sqlalchemy` driver.

**Recommendation**: Option A with Option C layered on top. The dbt adapter uses the Flight SQL Python client as the transport, with a thin SQLAlchemy dialect wrapping it. This routes dbt through the same session, auth, and planner paths as the CLI and avoids a new server component.

**Risk if deferred**: If the adapter uses a separate SQL transport, governance (quota, audit) is bypassed. The ADR must be decided before Sprint 4 begins.

---

## Sprint 1 — DataFusion Spark SQL Function Coverage

### S1.1 Function Compatibility Test Suite
- [ ] Create `crates/krishiv-sql/tests/spark_compat/` test module.
- [ ] Define `SparkFunctionTestCase { input_batches, expected_output, null_handling_note }` harness.
- [ ] Add test cases for date/time functions: `date_add`, `date_sub`, `datediff`, `date_trunc`, `from_unixtime`, `unix_timestamp`, `to_date`, `to_timestamp`, `year`, `month`, `dayofweek`, `hour`, `minute`, `second`, `date_format`.
- [ ] Add test cases for string functions: `concat_ws`, `split`, `regexp_extract`, `regexp_replace`, `initcap`, `lpad`, `rpad`, `repeat`, `instr`, `locate`, `substr`, `substring_index`, `base64`, `unbase64`, `decode`, `encode`.
- [ ] Add test cases for array/struct/map functions: `array_contains`, `array_distinct`, `array_intersect`, `array_union`, `array_except`, `explode`, `posexplode`, `size`, `element_at`, `flatten`, `map_keys`, `map_values`, `struct`.
- [ ] Add test cases for window functions: `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`.
- [ ] Add test cases for statistical/ML functions: `percentile_approx`, `corr`, `covar_pop`, `covar_samp`, `kurtosis`, `skewness`, `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`.
- [ ] Document null-handling classification per function (equivalent, divergent, or unimplemented).

**Validation**: `cargo test -p krishiv-sql -- spark_compat`

### S1.2 Implement Spark SQL Function Aliases and UDFs
- [ ] Add `spark_compat` feature flag to `krishiv-sql`.
- [ ] Register DataFusion aliases for all semantically equivalent Spark functions.
- [ ] Implement `ScalarUDF` for all functions with divergent null-handling semantics (per compatibility test classification).
- [ ] Expose `register_spark_functions(ctx: &SessionContext)` in `krishiv-sql` public API.
- [ ] Publish Spark SQL compatibility matrix as `docs/reference/spark-sql-compat-matrix.md`.

**Validation**: `cargo test -p krishiv-sql` passes all spark_compat test cases; `cargo clippy --workspace -- -D warnings` clean.

---

## Sprint 2 — SparkSession Shim & PySpark API

### S2.1 krishiv-spark-compat Python Package
- [ ] Create `python/krishiv-spark-compat/` package directory.
- [ ] Implement `SparkSession` class with `builder` pattern: `.remote("sc://host:port")`, `.appName()`, `.config()`, `.getOrCreate()`.
- [ ] Implement `DataFrame` class: `filter()`, `where()`, `groupBy()`, `agg()`, `orderBy()`, `sort()`, `join()`, `union()`, `unionAll()`, `select()`, `selectExpr()`, `withColumn()`, `drop()`, `distinct()`, `limit()`, `count()`, `collect()`, `show()`, `printSchema()`, `toPandas()`.
- [ ] Implement column expressions: `col()`, `lit()`, `when().otherwise()`, `isnull()`, `isnotnull()`, arithmetic, comparison, and logical operators.
- [ ] Implement imported functions: `avg`, `sum`, `count`, `min`, `max`, `first`, `last`, `explode`, `posexplode`, `array_contains`, `concat_ws`, `split`, `regexp_extract`, `date_add`, `datediff`, `from_unixtime`, `to_date`, `to_timestamp`, `year`, `month`, `dayofweek`.
- [ ] Map all `DataFrame` operations to Krishiv SQL via Spark Connect client (Sprint 3 provides the server; stub over Flight SQL for Sprint 2 tests).
- [ ] Add `from krishiv.compat.spark import SparkSession, col, avg, sum, explode` import path.
- [ ] Write Python unit tests for all DataFrame operations using a mock Krishiv session.

**Validation**: `pytest python/krishiv-spark-compat/tests/` passes.

### S2.2 SparkSession Remote Connection
- [ ] Implement Spark Connect gRPC client stub in `krishiv-spark-compat` (connects to Sprint 3 server).
- [ ] Implement plan serialization: DataFrame operations → Spark Connect `Relation` proto messages.
- [ ] Implement `ExecutePlan` RPC call with streaming result collection.
- [ ] Implement Arrow IPC result deserialization from `ExecutePlanResponse` batches.
- [ ] Add connection retry and timeout configuration.
- [ ] Write integration test: `SparkSession.builder.remote("sc://localhost:7070").getOrCreate()` connects to a local coordinator stub.

**Validation**: `pytest python/krishiv-spark-compat/tests/test_remote.py` passes against stub server.

---

## Sprint 3 — Spark Connect gRPC Server

### S3.1 Spark Connect Proto Integration
- [ ] Add `spark-connect` proto files (`spark/connect/relations.proto`, `spark/connect/expressions.proto`, `spark/connect/commands.proto`, `spark/connect/base.proto`) to `crates/krishiv-proto/`.
- [ ] Generate Rust types via `tonic-build` in `krishiv-proto/build.rs`.
- [ ] Define `SparkConnectCompatMatrix`: enumerated set of supported `Relation` and `Expression` variant names.
- [ ] Implement version negotiation: `AnalyzePlan` RPC returns server-supported Spark Connect version.

**Validation**: `cargo check -p krishiv-proto` clean.

### S3.2 Spark Connect Plan Translation
- [ ] Add `krishiv-spark-connect` crate (thin adapter, depends on `krishiv-sql` and `krishiv-scheduler`).
- [ ] Implement `SparkRelationTranslator`: translates Spark Connect `Relation` proto → DataFusion `LogicalPlan`.
- [ ] Support relation types: `Read` (named table, parquet, CSV), `Filter`, `Project`, `Aggregate`, `Sort`, `Limit`, `Join` (inner, left, right, outer), `SetOperation` (union, intersect, except), `WithColumns`, `Deduplicate`, `LocalRelation`.
- [ ] Support expression types: `Literal`, `Attribute` (column reference), `Alias`, `Cast`, `UnresolvedFunction`, `Unresolved­Attribute`, arithmetic, comparison, logical, and window expressions.
- [ ] Return `UNIMPLEMENTED` gRPC status for unsupported relation/expression types with a descriptive message referencing the compatibility matrix.
- [ ] Write unit tests for each supported relation translator.

**Validation**: `cargo test -p krishiv-spark-connect`

### S3.3 Spark Connect gRPC Server on Coordinator
- [ ] Add `SparkConnectService` tonic server implementing `spark.connect.SparkConnectService`.
- [ ] Implement `ExecutePlan` RPC: translate relation → DataFusion plan → execute → stream Arrow IPC batches in `ExecutePlanResponse`.
- [ ] Implement `AnalyzePlan` RPC: return schema and explain plan.
- [ ] Implement `Config` RPC: accept and store session config (no-op for unsupported keys, warn for unknown keys).
- [ ] Integrate `SparkConnectService` into `krishiv-scheduler` coordinator startup on configurable port (default 7070).
- [ ] Add `spark_connect_port` field to coordinator configuration.
- [ ] Write integration test: PySpark 3.5 client connects, runs TPC-H Q1, result matches expected.

**Validation**: `cargo test -p krishiv-spark-connect -- integration`; `cargo check --workspace` clean.

---

## Sprint 4 — dbt Adapter & Airflow Operator

### S4.1 krishiv-dbt-adapter Python Package
- [ ] Create `python/krishiv-dbt-adapter/` package directory.
- [ ] Implement dbt adapter class inheriting from `dbt.adapters.base.Adapter`.
- [ ] Implement `profiles.yml` connection type: `type: krishiv`, `flight_sql_host`, `flight_sql_port`, `database`, `schema`.
- [ ] Implement connection using Flight SQL Python client (`flightsql-dbapi`).
- [ ] Implement `execute()` and `get_result_from_cursor()` using Flight SQL `do_get`.
- [ ] Support dbt model types: `table` (CREATE TABLE AS SELECT), `view` (CREATE VIEW AS SELECT), `incremental` (INSERT INTO ... SELECT with predicate merge).
- [ ] Implement `list_relations_without_caching()`, `get_relation()`, `create_schema()`, `drop_relation()`, `truncate_relation()`, `rename_relation()`.
- [ ] Implement dbt `seed` support (upload CSV via Flight SQL `do_put`).
- [ ] Write dbt adapter tests: model compilation, execute, list_relations, incremental merge.
- [ ] Package as `krishiv-dbt-adapter` on PyPI (maturin-independent, pure Python).

**Validation**: `pytest python/krishiv-dbt-adapter/tests/` passes.

### S4.2 krishiv-airflow Python Package
- [ ] Create `python/krishiv-airflow/` package directory.
- [ ] Implement `KrishivSubmitJobOperator(BaseOperator)`: submits a Krishiv job via the coordinator gRPC `SubmitJob` RPC.
- [ ] Implement operator parameters: `coordinator_url`, `job_spec`, `namespace`, `priority`, `cpu_limit`, `memory_limit`, `conn_id`.
- [ ] Implement XCom push of `job_id` on submission.
- [ ] Implement `KrishivJobSensor(BaseSensorOperator)`: polls `GetJobStatus` until terminal state.
- [ ] Implement sensor parameters: `job_id` (from XCom or literal), `coordinator_url`, `success_states`, `failure_states`, `poke_interval`.
- [ ] Add Airflow connection type `krishiv` to connection UI schema.
- [ ] Write unit tests for operator and sensor using mock gRPC stubs.

**Validation**: `pytest python/krishiv-airflow/tests/` passes.

### S4.3 Great Expectations KrishivDatasource
- [ ] Create `python/krishiv-ge/` package directory.
- [ ] Implement `KrishivDatasource` extending GE `Datasource`.
- [ ] Implement `KrishivSQLAlchemyDataConnector` using the SQLAlchemy/Flight SQL dialect from ADR-R15.3.
- [ ] Support `BatchRequest` with `table_name` and `query` batch specs.
- [ ] Write smoke test: connect, get batch, run `expect_column_values_to_not_be_null`.

**Validation**: `pytest python/krishiv-ge/tests/` passes.

---

## Sprint 5 — Migration Tooling & Great Expectations

### S5.1 Migration Analyzer CLI
- [ ] Add `krishiv compat analyze <file.py>` subcommand to `krishiv-cli`.
- [ ] Implement Python AST parser (via `rustpython-parser` or subprocess `ast.dump`) to identify PySpark API call sites.
- [ ] Implement compatibility classifier: map each identified API call to `Supported`, `PartiallySupported { caveats }`, or `Unsupported { reason }` using the compatibility matrix.
- [ ] Implement report generator: output per-API-call compatibility status, total supported/unsupported counts, migration confidence score.
- [ ] Support `--format json` and `--format text` output.
- [ ] Support `--output <file>` for report persistence.
- [ ] Write CLI tests: analyze a sample PySpark script, verify JSON report structure and accuracy.

**Validation**: `cargo test -p krishiv-cli -- compat_analyze`

### S5.2 Compatibility Matrix Publication and E2E Validation
- [ ] Finalize `docs/reference/spark-sql-compat-matrix.md` with all function and operation statuses.
- [ ] Add TPC-H end-to-end test: run all 22 TPC-H queries via PySpark `SparkSession.builder.remote()` against Krishiv coordinator; verify result correctness.
- [ ] Add migration analyzer test: analyze `tpch_pyspark.py` reference script, verify 100% Supported classification for all TPC-H operations.
- [ ] Add dbt adapter end-to-end test: `dbt run` with Krishiv profile, all models build successfully.
- [ ] Add Airflow operator integration test: submit job, sensor detects completion.
- [ ] Publish Python packages to internal registry: `krishiv-spark-compat`, `krishiv-dbt-adapter`, `krishiv-airflow`, `krishiv-ge`.

**Validation**: `cargo test --workspace`; TPC-H E2E test suite passes; `pytest python/` passes.

---

## Acceptance Gate

- [ ] `SparkSession.builder.remote("sc://coordinator:7070").getOrCreate()` connects and executes all 22 TPC-H queries with correct results.
- [ ] `from krishiv.compat.spark import SparkSession, col, avg, sum, explode` import path works.
- [ ] Spark SQL compatibility matrix covers 100+ Spark 3.5 functions with documented null-handling status.
- [ ] All functions in the "Supported" column of the compatibility matrix pass the function compatibility test suite.
- [ ] `dbt run` with `type: krishiv` profile executes table, view, and incremental models successfully.
- [ ] `KrishivSubmitJobOperator` submits a job and `KrishivJobSensor` detects terminal state.
- [ ] `KrishivDatasource` runs a GE expectation suite against a Krishiv table.
- [ ] `krishiv compat analyze` produces a structured report for a PySpark script.
- [ ] Spark Connect server returns `UNIMPLEMENTED` (not a crash) for unsupported plan node types.
- [ ] `cargo test --workspace` passes; `cargo clippy --workspace -- -D warnings` clean.
