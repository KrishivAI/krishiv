# Krishiv Implementation Status

## Current Session — Five Stabilization Phases

### Phase 1 — Fix collect_batch_sql arity mismatch (B1)
All call sites in `execution_runtime.rs`, `in_process.rs`, `in_process_cluster.rs`, and
`integration_distributed.rs` updated to pass `is_streaming: bool` (all `false` for batch queries).
`cargo test -p krishiv-runtime` now compiles and all integration tests pass.

### Phase 2 — Tests for previously untested Beta paths
- **CEP/MATCH_RECOGNIZE** (`krishiv-sql/src/cep_sql.rs`): Fixed routing bug where multi-stage
  patterns (A→B→C) could never complete — tracking `(stage_index, start_time_ms)` instead of
  stage_index alone catches both state advances and expiry-then-restart cases.
  Added 3 tests including a 3-stage pattern that now produces output.
- **temporal_join + interval_join** (`krishiv-api/src/streaming_dataframe.rs`): 6 tests covering
  latest-version match, inner/left join semantics, event windows in/out, multiple matches, empty.
- **Circuit-breaker reset HTTP endpoint** (`krishiv-scheduler/src/coordinator_daemon.rs`):
  Test uses `tower::ServiceExt::oneshot` to POST to `/api/v1/executors/{id}/reset` and
  asserts 200 + `{"reset":true}` + counter cleared. Added `tower` to scheduler dev-deps.
- **Continuous stream HTTP push/drain** (`krishiv-scheduler/src/continuous_stream_http.rs`):
  3 tests for register/drain, push, and invalid job-id rejection. Tests register a real executor
  so `SlotAwareScheduler::place` succeeds.
- **Predicate pushdown through Join** (`krishiv-optimizer/src/lib.rs`): 2 tests confirm
  single-side conjuncts are pushed only into the owning scan.

### Phase 3 — Fix B2 and B3
- **B2** (`krishiv-python/src/session.rs`): `Session.connect(url)` now always enables remote
  execution — removed `KRISHIV_REMOTE_EXEC` env-var gate that caused silent local fallback.
- **B3** (`krishiv-runtime/src/execution_runtime.rs`): `RemoteExecutionRuntime::accept_plan`
  for streaming plans now returns `RuntimeError::Unsupported` instead of silently accepting.

### Phase 4 — Durability profile smoke tests
Two inline tests in `execution_runtime.rs`: `dev_local_profile_batch_sql_returns_results`
and `dev_local_profile_continuous_double_drain_does_not_panic` confirm second-drain idempotence.

### Phase 5 — Stubs to clear errors
- `cep_sql.rs` lakehouse stubs: already used `PyRuntimeError::new_err` — verified, no change.
- `session.rs` `read_delta_async`: delegates to `sql_engine.read_delta()` — already errors properly.
- `lib.rs` PanicUdf test: replaced `todo!()` in `input_schema()`/`output_field()` with static stubs.

## Validation

```
cargo check --workspace      # 0 errors
cargo test -p krishiv-runtime              # all pass (integration + lib)
cargo test -p krishiv-scheduler --lib      # 214 passed
cargo test -p krishiv-executor --lib       # all pass
cargo test -p krishiv-optimizer --lib      # 145 passed
cargo test -p krishiv-sql --lib            # 66 passed
cargo test -p krishiv-api --lib            # all pass
```

## Next Steps

- Add tests for the new `execute_match_recognize` path with concurrent-key patterns.
- Add HTTP-level test for the `/api/v1/continuous-push` path using a real IPC-encoded batch.
- Add a smoke test that exercises `RemoteExecutionRuntime::accept_plan` for streaming and
  asserts `Unsupported` error (B3 regression guard).
- Consider renaming `Session.connect()` env-var in docs now that the behavior changed.

---


## Current Session (Completed)

### All 14 audit gaps / bugs fixed

**Fix 1 — InlineIpc 3 MB per-partition size cap** (`krishiv-scheduler/src/batch_sql.rs`)
- Added `MAX_INLINE_PARTITION_BYTES = 3 MB` constant.
- `submit_batch_sql_job` now returns `SchedulerError::InvalidJob` with a clear message
  if any decoded partition exceeds the limit, instead of silently crashing the gRPC channel.
- Decode errors also surface as `InvalidJob` instead of being silently dropped.

**Fix 2 — Continuous streaming coordinator-mediated routing** (`krishiv-scheduler/src/continuous_stream_http.rs`)
- Removed direct executor gRPC calls from `api_continuous_push` and `api_continuous_drain`.
- Push now stores batches as `InlineIpc` partitions via `register_job_input_partitions`.
- Drain now returns results from `take_job_inline_results` (same path as batch SQL).
- Register now uses the `stream:continuous:<job_id>` fragment so the executor reads from
  InlineIpc partitions in its assignment.
- Executor `stream:continuous:` handler (`krishiv-executor/src/fragment/streaming.rs`) now
  falls back to `read_inline_ipc_partitions` when no in-process drainer is available (distributed mode).

**Fix 3 — Circuit-breaker reset HTTP endpoint** (`krishiv-scheduler/src/coordinator_daemon.rs`)
- Added `POST /api/v1/executors/{executor_id}/reset` route.
- Handler calls `coord.executors.reset_task_failures(&executor_id)` (pre-existing method).
- Returns `{"reset": true, "executor_id": "..."}` on success.

**Fix 4 — Optimizer predicate pushdown through join nodes** (`krishiv-optimizer/src/lib.rs`)
- Extended `PredicatePushdownRule.apply()` to also collect scans two hops away through
  `NodeOp::Join` nodes.  Each conjunct is now tested against both join sides independently;
  single-side predicates are pushed into the appropriate scan's `filters` list.

**Fix 5 — Python KafkaSink.write_batches()** (`krishiv-python/src/sinks.rs`)
- Implemented `write_batches(Vec<PyBatch>)` using `KafkaConfig` + `KafkaSink` from
  `krishiv_connectors::kafka` (feature-gated `#[cfg(feature = "kafka")]`).
- Non-kafka builds raise a `RuntimeError` with a clear rebuild instruction.

**Fix 6 — Python IcebergSink.write_batches()** (`krishiv-python/src/sinks.rs`)
- Implemented `write_batches(Vec<PyBatch>)` using `IcebergFsTable::new` + `append` from
  `krishiv_lakehouse` (feature-gated `#[cfg(feature = "iceberg")]`).
- Non-iceberg builds raise a `RuntimeError`.

**Fix 7–9 — Temporal join, interval join, side output in Rust API**
  (`krishiv-api/src/streaming_dataframe.rs`)
- Added `StreamingDataFrame::with_side_output(name, lateness_ms)` — filters late rows
  out of the main pipeline using `SideOutputRouter::is_late`.
- Added free function `temporal_join(stream, table, spec, lookback_ms)` using
  `VersionedTableState::upsert_version` + `lookup_as_of`.
- Added free function `interval_join(left, right, left_col, right_col, spec)` using
  `IntervalJoinState::push_left` / `push_right`.

**Fix 10 — CEP/MATCH_RECOGNIZE wired into SqlEngine** (`krishiv-sql/src/lib.rs`, `cep_sql.rs`)
- `SqlEngine::sql()` now intercepts queries containing `MATCH_RECOGNIZE` before DataFusion.
- Added `execute_match_recognize(stmt, source_batches)` to `cep_sql.rs`: applies
  `SequentialPatternMatcher` per partition key, returns matched-event batches.
- Results are registered into DataFusion as `_krishiv_cep_result` and returned as a normal
  `SqlDataFrame`.

**Fix 11 — OTLP `deployment_target` label** (`krishiv-metrics/src/lib.rs`)
- Added `MetricsConfig::deployment_target: Option<String>` field.
- Added `resolved_deployment_target()` helper: explicit config → `KRISHIV_DEPLOYMENT_TARGET`
  env var → `"unknown"`.
- Both OTLP and Stdout tracer providers now attach `service.name` and `deployment.target`
  as OTel resource attributes via `SdkTracerProvider::builder().with_resource(...)`.

**Fix 12 — Jobs CLI no longer prints misleading "not yet implemented" message**
  (`krishiv/src/cli.rs`)
- Removed the `eprintln!("[local-mode] Jobs are local to this process...")` line.
  In-session jobs were already listed correctly; only the message was wrong.

**Fix 13 — `compat` CLI stubs now return actionable subcommand listing** (`krishiv/src/cli.rs`)
- `krishiv compat <unknown>` now returns a specific error listing `analyze` (available) and
  `convert`/`report` (planned) with instructions, instead of a generic placeholder message.
- `krishiv compat` with no args returns the help text.

**Fix 14 — Remove `hostPath /tmp` from k8s executor manifest** (`k8s/direct/krishiv-distributed.yaml`)
- Removed the `hostPath /tmp` volume and `volumeMounts` from the executor Deployment.
- Data is now delivered via InlineIpc in task assignments; no shared filesystem is needed.

## Validation

```
cargo check --workspace      # 0 errors
cargo test -p krishiv-scheduler --lib   # 210 passed
cargo test -p krishiv-executor --lib    # 163 passed
```

## Pre-existing test failures (not introduced by this session)

`cargo test -p krishiv-runtime` fails on `in_process.rs`, `execution_runtime.rs`,
`in_process_cluster.rs`, and `tests/integration_distributed.rs` with
`E0061: this method takes 3 arguments but 2 were supplied` — these call sites predate
this session and are unrelated to the 14 fixes above (confirmed by stash isolation).

## Next Steps

- Add tests for the new `execute_match_recognize` path with a multi-stage pattern.
- Add tests for `temporal_join` and `interval_join` helper functions.
- Add test for the circuit-breaker reset endpoint via HTTP.
- Fix the pre-existing `collect_batch_sql` arity mismatch in runtime integration tests.
