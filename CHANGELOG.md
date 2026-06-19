# Changelog

All notable changes to Krishiv are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
Semantic Versioning as described in `docs/RELEASE.md`.

## [Unreleased]

### Added

- Public engine contracts, connector maturity, and durable metadata versions.
- Typed Rust/Python DataFrame APIs and Iceberg-first build defaults.
- Phase 5 open-source governance, security, compatibility, benchmarking, and
  release infrastructure.
- Stable API Phase A manifest, per-item metadata, generated Rust/Python/SQL inventories,
  Python type stubs, Rust signature reports, CI change classification, and a unique Python
  `DataFrame` identity.
- Phase B engine-owned expression/type AST shared by Rust, Python, and SQL.
- Phase C canonical DataFrame boundedness, relational operations, typed catalog identifiers,
  and prepared statements.
- Phase D typed I/O contracts, async reader/writer actions, physical file layout controls,
  and coordinator-owned Iceberg atomic commits.
- Phase E typed `QueryHandle`, `BlockingSession` explicit blocking facade, and genuine Python
  asyncio awaitables (`sql_async`, `submit_async`, `collect_async`).
- Phase F `DataStreamReader`/`DataStreamWriter` builders, `StreamingOutputMode`
  (Append/Update/Complete), `StreamingTrigger` variants, stream-table and stream-stream joins,
  deduplication, `foreach_batch`, and `StreamingQuery` lifecycle handle.
- Phase G typed stateful process API: `ProcessFunction`, `CoProcessFunction`,
  `BroadcastProcessFunction`; `ValueState<T>`, `ListState<T>`, `MapState<K,V>`,
  `ReducingState<T>`; event-time and processing-time timers; `OperatorUid`/`OperatorConfig`;
  `ProcessFunctionExecutor` with `snapshot()`/`restore()` for savepoint rescaling.
- Phase H SQL grammar feature matrix (`feature_matrix()`, `features_for_category()`,
  `features_by_status()`); SQLSTATE code mapping (`sqlstate_for()`); `OperationRegistry`
  for thread-safe operation cancellation; `SqlEngine::execute_with_timeout` and
  `SqlEngine::execute_with_operation_id`; `SqlError::OperationCancelled` and
  `SqlError::Timeout` variants.
- Phase I release gate: type/null/time/decimal/ordering/overflow conformance tests;
  embedded and single-node mode conformance tests; streaming delivery certification
  (failure-loop, idempotent re-run, checkpoint round-trip); TPC-H Q1/Q3/Q6/Q10 and
  Nexmark Q1/Q2/Q5/Q8 synthetic baseline gate; parity manifest validation
  (`check_parity_manifest.py`); SBOM and checksum generation (`generate_sbom.py`);
  migration note coverage check (`check_migration_notes.py`); master gate script
  (`check_phase_i_gate.py`); runnable examples (`basic_sql`, `streaming_word_count`).

### Changed

- Rewrote the architecture document against the current workspace.
- `PySession::sql_async` upgraded from `block_in_place` to a genuine asyncio coroutine.
- `QueryHandle` now routes collect, writes, and stream submission through a single typed
  handle; use `DataFrame::submit_async()` to obtain a handle.

### Migration Notes

- **`DataFrame` (Python)**: The legacy `Relation` class (previously exported as the
  unified wrapper) was renamed before Phase A. Use `DataFrame` — `Relation` is a
  deprecated alias that will be removed in 1.0.
- **`sql_async` (Python)**: Now returns a true asyncio coroutine; existing code that
  called `asyncio.run(session.sql_async(...))` continues to work. Code that passed the
  return value to `loop.run_until_complete` without `await` must add `await`.
- **`BlockingSession`**: Callers who used hidden `block_on` internals in the Rust API
  should migrate to `BlockingSession::new(session)` for explicit blocking behaviour.
- **`execute_with_timeout` / `OperationRegistry`**: Replace ad-hoc timeout wrappers
  around `SqlEngine::sql()` with `SqlEngine::execute_with_timeout(sql, timeout_ms)`.

## [0.1.0]

Initial pre-1.0 development release line.
