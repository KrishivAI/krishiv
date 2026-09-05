# Public interfaces

Every way a user reaches the engine is a thin surface over one core:
`krishiv_api::Session` (Rust) and the coordinator's control APIs. This
document lists the surfaces, what each exposes, and the rule that keeps them
honest — a surface may add ergonomics, never semantics.

## The surface map

```
 SQL text ──► krishiv sql (CLI) ─┐
 Python  ──► krishiv (PyO3) ─────┤
 Rust    ──► krishiv_api ────────┼──► Session ──► ExecutionRuntime (01) ──► engines
 JDBC/ADBC/dbt ─► Flight SQL ────┤
 LLM agents ─► MCP ──────────────┘
 Browser ──► /console ───► /api/v1 (coordinator HTTP) ──► Coordinator (04)
 Kubernetes ─► KrishivJob CRD ─► operator ─► Coordinator
```

## Rust: `krishiv-api`

| Type | Role |
|---|---|
| `SessionBuilder`, `Session` | build (`01`), `sql`, `read_parquet`/`read_csv`/`read_connector`, `register_*`, `ivm_job`, `stream`, `submit_sql_job`, `check_routing`, catalog ops |
| `DataFrame` | the canonical relational type (ADR-0002): lazy transformations (`select`, `filter`, `join`, `group_by().agg()`, `sort`, `limit`, `with_column`, window functions), `collect`/`collect_stream`/`show`, `write_*`, `explain*`, `cache` |
| `Expr` (`expression`) | facade over the versioned `krishiv-plan` AST; `col`, `lit`, operators, functions, `expr("…")` escape hatch |
| `QueryHandle` (`query`) | one lifecycle path for every execution: progress, cancellation, timeout, a true awaitable |
| `PreparedStatement` (`prepared`) | parameterised SQL |
| `StreamingDataFrame`, `DataStreamReader`, `streaming_builder`, `window`, `timers`, `process` | the streaming API (`08`) |
| `IncrementalDataFrame`, `incremental_flow`, `materialized_table` | the incremental API (`09`) |
| `Pipeline` (`pipeline`) | source → operators → sink graphs submitted as one job |
| `SqlJob` / `SubmittedSqlJobStatus` (`sql_job`), `streaming_job` | remote job handles |
| `compute` | `CompiledJob`, `ComputeEngine`, `EngineRuntime`, `run_job` re-exports from `krishiv-engine-core` / `krishiv-engines` |
| `io`, `connector_runtime`, `stream_write` | typed read/write options and sink providers (`ConsolidatingSinkProvider`) |
| `catalog` | database/table/view/function catalog operations |
| `types` | public scalar and schema types |
| `BlockingSession` (`blocking`) | the sync wrapper; the only place `block_on` is sanctioned for the Rust API (`17`) |
| `KrishivError` (`error`) | one error type with stable kinds |
| `pyspark_parity` | the parity metadata that generates `../reference/pyspark-parity.md` |

Rust is async-first; every sync method is documented as a convenience over an
async one. `api/stable-api.toml` is the machine-readable stability inventory
(policy: canonical type `DataFrame`, Rust `async-first`, Python
`sync-convenience-and-asyncio`, SQL `flight-sql-first`) and is validated in
CI; `18-compatibility-and-versioning.md` covers the guarantees.

## Python: `krishiv`

PyO3 bindings over the same `Session`. Public classes: `Session`,
`BlockingSession`, `DataFrame`, `GroupedDataFrame`, `DataFrameStream`,
`StreamingDataFrame`, `DataStreamReader`, `IncrementalDataFrame`, `IvmJob`,
`DeltaBatch`, `StepSummary`, `ViewError`, `Pipeline`, `MemorySink`,
`QueryHandle`, `QueryResult`, `PreparedStatement`, `Column`, `AggExpr`,
`Relation`, `Schema`, `Batch`, `JobStatus`, `EngineJobHandle`, `RunningJob`,
`BroadcastContext`, `ProcessContext` with `ValueState`/`ListState`/`MapState`/
`AggregatingState`, sinks (`ParquetSink`, `ConnectorSink`, `KafkaSink`,
`IcebergSink`, `ElasticsearchSink`, `CassandraSink`, `HBaseSink`, and the
vector sinks), `ConnectorSource`, `RustScalarUdf`, `OperationRegistry`,
`MemoCacheInfo`, and the error hierarchy (`KrishivError` → `QueryError`,
`SchemaError`, `ConnectorError`, `CheckpointError`, `ModeError`,
`AuthorizationError`, `UdfError`). Python UDFs run through the Arrow IPC
bridge (`arrow_fast`) on `spawn_blocking`; a shared multi-thread Tokio
runtime is built at import. PySpark parity is tracked per method in the
generated matrix; the crate is CI-tested through the `test-python` job
(maturin + pytest) rather than `cargo test` (`17`).

## CLI: `krishiv`

| Group | Commands |
|---|---|
| query | `sql`, `explain [--analyze]`, `stream`, `table`, `ivm` |
| jobs | `submit`, `jobs`, `state`, `savepoint`, `restore`, `checkpoints`, `pipeline` |
| daemons | `local start|status|stop`, `cluster`, `clusterd` (= `coordinator`), `job-coordinator`, `executor`, `flight-server`, `shuffle-svc`, `mcp` |
| introspection | `doctor`, `capabilities` |

Execution selectors and mode flags are in `01`. Output is plain text or
`--format json`; exit codes are 0 success, 1 runtime error, 2 usage error.

## Arrow Flight SQL: `krishiv-flight-sql`

The wire protocol for external tools (JDBC via the Arrow Flight SQL driver,
ADBC, dbt). `KrishivFlightSqlService` implements `GetFlightInfo`/`DoGet` for
statements and prepared statements, `DoPut` for parameter binding and table
upload, catalog metadata commands (`GetTables`, `GetDbSchemas`, `GetSqlInfo`,
…), and `BeginTransaction`/`EndTransaction` actions. Sessions are bounded by
`SessionRegistry`; `FlightExecutionHost` binds a service to a `Session`, so a
Flight server can front an embedded engine, a local daemon, or a distributed
coordinator. Auth is a bearer/API key mapped by an `AuthProvider`; a
`PolicyHook` filters table access (`12`). `krishiv-sql-gateway` is an
in-process library (SQLSTATE mapping, connection pool, `GatewaySession`) for
embedders — it is **not** a JDBC/ODBC wire server; drivers connect over
Flight SQL.

## HTTP control plane: `/api/v1`

Served by the coordinator daemon (and the local daemon), JSON, bearer-token
protected (`12`), with `openapi.json` generated from the handlers:

| Path | Purpose |
|---|---|
| `sql`, `batch-sql` | submit SQL; batch-sql returns a job id and results via `jobs/{id}` |
| `jobs`, `jobs/{id}`, `jobs/{id}/cancel`, `jobs/{id}/stages`, `jobs/{id}/diagnose` | unified job lifecycle across batch, streaming, IVM |
| `executors`, `executors/{id}`, `queues` | membership and admission state |
| `events`, `history`, `history/{id}`, `logs`, `metrics-snapshot` | observability (`13`) |
| `bounded-window`, `continuous`, `continuous/{id}/{push,drain,checkpoint,restore}` | streaming drivers |
| `ivm/jobs`, `ivm/jobs/{id}`, `…/step`, `…/restore` | incremental jobs |
| `/healthz`, `/readyz`, `/leaderz`, `/metrics` | probes and Prometheus |

## Web console

`krishiv-ui` serves the TanStack SPA (`console/dist`, embedded into release
binaries; read from disk in debug) under `/console` with SPA fallback. It
is not a second server: it calls the same `/api/v1` routes with the stored
bearer token. The legacy askama pages remain until the console reaches parity.

## MCP: `krishiv-mcp`

A Model Context Protocol frontend over `Session` (stdio or streamable HTTP,
`KRISHIV_MCP_TRANSPORT`; default `127.0.0.1:8765`). Tools: `execute_sql`,
`explain_sql`, `investigate_sql`, `describe_table`, `sample_table`,
`list_tables`, `list_catalogs`, `submit_sql_job`, `get_job_status`,
`get_job_result`, `inspect_job`, `cancel_job`, `list_jobs`, `list_executors`,
`register_source`, `register_sink`, `list_connectors`,
`validate_connector_config`, `create_incremental_view`,
`feed_incremental_view`, `step_incremental_view`, `snapshot_incremental_view`,
`checkpoint_incremental_job`, `restore_incremental_job`,
`enable_incremental_delta_checkpoints`, `create_continuous_stream`,
`feed_continuous_stream`, `drain_continuous_stream`,
`checkpoint_continuous_stream`, `restore_continuous_stream`,
`list_continuous_streams`, `build_streaming_pipeline`,
`submit_streaming_pipeline`, `get_streaming_job_status`,
`deployment_capabilities`, `runtime_info`, `get_metrics_summary`,
`krishiv_health`. Guardrails: `KRISHIV_MCP_MAX_ROWS` (100),
`KRISHIV_MCP_TIMEOUT_MS` (30 000), and writes refused unless
`KRISHIV_MCP_ALLOW_WRITE_SQL` is set.

## Kubernetes: `krishiv-operator`

`KrishivJob` (mode `batch`/`streaming`, image, tasks, parallelism,
restart policy, `dedicatedCoordinator`), `KrishivQueue`, and
`KrishivExecutorPool` CRDs, reconciled into coordinator submissions;
status conditions and phases are written back (`14`).

## The rule

Front-ends lower to one artifact — SQL, Python, and Rust all produce a
`CompiledJob` or a `DataFrame` plan — and one dispatch point routes it
(`run_job`). Per-surface behaviour differences are bugs, and the
cross-language relational conformance suite (`krishiv-conformance`) exists
to find them.

## Related

- `01` (routing), `12` (auth on each surface), `18` (stability labels),
  `../reference/sql-feature-matrix.md`, `../reference/pyspark-parity.md`.
