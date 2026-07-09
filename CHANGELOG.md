# Changelog

All notable changes to Krishiv are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
Semantic Versioning as described in `docs/RELEASE.md`.

## [Unreleased]

### Changed

- **IVM ticks reuse one spill-capable `SessionContext` per flow** (G14,
  2026-07-09): `step_datafusion()` built a fresh context every tick, a
  fixed cost that dominated true O(Δ) work — the 2026-07-05 benchmark
  measured full recompute ~100× *faster* than an IVM tick, with the
  crossover extrapolated at ~23M rows. The flow now caches the context
  (async-mutex-guarded; discarded on tick error) and reconciles the
  table catalog each tick to exactly what a fresh context would hold.
  Re-benchmarked (same workload/hardware): ticks dropped to ~13–18 ms
  nearly flat across 50K–1M-row tables; the crossover is now ~500K rows
  and IVM wins at 1M (17.8 ms vs 24.3 ms). Remaining tick slope is the
  O(n) snapshot apply — tracked as the incremental-state follow-up.

### Fixed

- **Chained DiffBased views no longer read stale upstream output within a
  tick** (2026-07-09): `SessionContext::register_table` errors on duplicate
  names, and the per-view upstream registration swallowed that error with
  `let _ =`, so a downstream DiffBased view executing after its upstream in
  the same tick kept the upstream's previous-tick MemTable. Registration now
  deregisters first (replace semantics).

- **Large batch results no longer OOM the engine pod** (2026-07-09): a
  collected batch result was materialized wholesale at every hop —
  executor `collect` → one giant unary `TaskStatus` gRPC message →
  coordinator in-memory job results → Flight encode — so the 10.2M-row
  NYC-taxi `clean_trips` join (~354 MB) killed the 2 Gi shared engine pod
  (exit 137) on every 15-minute schedule. Executors now stream query
  output and keep results inline only up to
  `KRISHIV_INLINE_RESULT_MAX_BYTES` (default 8 MiB); anything larger is
  written to one Arrow IPC spool file and delivered to the coordinator in
  3 MiB chunks over a new client-streaming `PushTaskResult` RPC before the
  terminal status (which carries `spooled_result_total_bytes`). The
  coordinator spools to disk (`KRISHIV_RESULT_SPOOL_DIR`, capped by
  `KRISHIV_RESULT_SPOOL_MAX_BYTES`, default 8 GiB), verifies size on
  claim (mismatch/missing → job cancelled, never silent missing rows),
  and consumers decode from the file; Flight `do_get` now encodes result
  batches incrementally instead of buffering the full IPC payload.
- **Disk spill now covers all three SQL modes** (2026-07-09): IVM /
  delta-batch ticks (diff-based recompute, plan fallback, `delta:step:`
  fragments) ran on unbounded `SessionContext`s; they now execute on
  `FairSpillPool` contexts sized by `KRISHIV_QUERY_MEMORY_LIMIT_BYTES`.
  When that env is unset, the per-query limit defaults to cgroup
  memory-limit/4 (explicit `0` still disables), so spill is armed by
  default inside memory-limited containers for batch, streaming, and
  delta-batch alike.
- `checkpoint_barrier_integration` test was stale against the
  ack-registry contract (acks are gated on checkpoint completion since
  the phantom-timeout fix) and failed deterministically; it now simulates
  the runner side (drain injector → `complete()`) and asserts the state
  handle round-trip (2026-07-09).

- **Heartbeats no longer stall behind checkpoint work** (2026-07-09):
  coordinator-issued restore/checkpoint commands ran inline in the
  executor's heartbeat loop, so a multi-second restore or checkpoint upload
  delayed the next heartbeat past the coordinator's timeout — evicting the
  healthy, mid-restore executor and triggering another rollback+restore
  livelock. Commands now run on a dedicated ordered worker (restores first
  within a batch) while heartbeats keep flowing.
- **Executor registry no longer grows without bound** (2026-07-09):
  Lost/Removed executor records are retained long enough for zombie fencing
  (40× the heartbeat timeout, ≥30 min) and then pruned; previously every
  k8s pod restart left a corpse the heartbeat tick iterated forever.
- `tick_period_ms` default corrected to the daemon's real 5 s cadence —
  checkpoint interval timers and ack timeouts convert ticks → ms with it and
  ran 5× slow under the old 1 000 default; the heartbeat clock's quiet-path
  per-job walk is also skipped when no executor was lost and debug logging
  is off (2026-07-09).
- **Healthy executors no longer lose their tasks to heartbeat-timeout lease
  churn** (2026-07-08): the default `heartbeat_timeout_ticks` (3 ticks =
  15 s at the daemon's 5 s tick) left one delayed heartbeat between a
  healthy executor and eviction against the executor's 10 s default
  interval; the eviction was silent (no log, no
  `krishiv_executor_lost_total` increment), and running tasks kept
  reporting the lease frozen into their assignment, so after the executor
  re-registered every status RPC was fenced `stale_lease` and the task
  runner aborted healthy work — the recurring "assigned but not running"
  stuck state, ending in a circuit-breaker launch loop. Timeout default is
  now 9 ticks (≈45 s, ≥3× the heartbeat interval), timeout evictions log
  and count like `mark_executor_lost`, and `send_task_status` stamps the
  freshest of the assignment lease and the live shared lease (B10
  precedent), letting a re-registered executor's tasks self-heal.

- **Deployed builds now include `rest-catalog`** (2026-07-08): `just
  build-k8s` / `build-bare-metal` compiled without the feature, so
  `KRISHIV_ICEBERG_REST_URI`/`_TOKEN`/`_WAREHOUSE`/`_NAME` were silently
  ignored (both `register_rest_catalog_from_env` call sites are
  `#[cfg(feature = "rest-catalog")]`) and governed `krishiv.<ns>.<table>`
  SQL failed with "table not found" from every deployed image. Both recipes
  now build with the feature. Known remaining limitation (platform gap
  G15): registration only works on the InProcess Flight host — a
  coordinator-delegated Flight host still warns and skips; run
  `krishiv flight-server` beside the coordinator until coordinator-side
  registration lands.

### Added

- **New benchmark: IVM vs full-recompute**
  (`crates/krishiv-bench/benches/ivm_vs_full_recompute.rs`,
  `cargo bench -p krishiv-bench --bench ivm_vs_full_recompute`). **Finding
  worth flagging, not just a new benchmark**: at table sizes up to 1M rows,
  a full recompute of a `GROUP BY SUM` query is ~100x *faster* in wall-clock
  time than one `IncrementalFlow::step_datafusion()` tick — every production
  call site constructs a fresh `SessionContext` per tick (confirmed via
  `grep -rn step_datafusion` across `krishiv-executor`/`krishiv-api`/
  `krishiv-scheduler` — all of them use the plain convenience method), and
  that fixed setup cost (~650-700ms) dominates the true O(Δ) aggregate
  work at these scales. Extrapolating measured full-recompute scaling, the
  crossover (where a full recompute costs as much as one current IVM tick)
  is ~23M rows. See `docs/implementation/status.md` for the full
  methodology, numbers, and root-cause read of `flow.rs`. Not fixed here —
  reusing a job-scoped `SessionContext` across ticks is the natural
  follow-up and is flagged for the engine team, not attempted this session.
- **G5: restorable checkpoints for continuous windowed jobs, exercised live on
  a cluster.** Every completed continuous cycle now ships the executor's
  post-cycle `stream:loop` operator state back to the coordinator
  (`TaskOutputMetadata.state_snapshot`, wire field 20; captured via
  `ContinuousWindowExecutor::snapshot()` — the `checkpoint()`-first variant, as
  `peek_snapshot_bytes` serializes a backend the live panes were never written
  into), and the coordinator persists it as the job's `ContinuousSnapshot`. As
  a result `POST /api/v1/continuous/{id}/checkpoint` returns real live state
  and `…/restore` rehydrates a recreated job. Verified live on k8s: seed a
  partial window → checkpoint → deregister → re-register + restore → closing
  the window emits the exact pre-kill accumulations.

### Fixed

- **G12 (JDBC/ADBC `?` parameter binding)**: JDBC/ADBC clients bind
  prepared-statement parameters as ordinal `?` marks, but the engine only
  recognized `$N` — every `?`-bound query counted zero parameters and
  failed with a placeholder error. New `normalize_question_mark_params`
  rewrites `?` to `$1, $2, …` (quote-aware — literal `?`s inside strings
  or quoted identifiers are untouched), wired into prepared-statement
  creation. Also fixes a real feature-gating regression found while
  building this: the G3 fix below used `uuid`, gated behind a narrower
  Cargo feature than the file it's in actually compiles under — any build
  enabling `lakehouse` without `iceberg` failed outright. Replaced with a
  std-only nanosecond+atomic-counter tag.
- **G3 (Iceberg concurrent-commit lost updates)**: `IcebergFsTable::append`
  committed metadata via an unconditional tmp-write + rename to a single
  `metadata.json`, so two concurrent committers — even two tasks sharing
  one instance in a single process, not just two separate processes —
  could last-write-win and silently drop one another's commit. Replaced
  with Iceberg-style versioned commits (`metadata-v{N}.json`, created
  atomically via `create_new`/O_EXCL; losers re-read and retry) and removed
  the in-memory state cache entirely — every read now reflects whatever's
  truly latest on disk. New test
  `concurrent_writers_with_independent_table_handles_lose_no_commits`
  proves 8 independently-instanced concurrent writers all survive.
- **G2 (memory-constrained sort spill)**: a `SqlEngine` configured with a
  memory limit under ~15MB had every sort fail immediately with "Not enough
  memory to continue external sort" — DataFusion's `sort_spill_reservation_bytes`
  defaults to a hardcoded 10MB reserved up front for the merge phase,
  regardless of the configured pool size, so the reservation itself didn't
  fit. `build_single_node_session_config` now scales the reservation down
  proportionally (`(limit / 4).clamp(64KB, 10MB)`) when a memory limit is
  set; deployments at or above 40MB are unaffected. New
  `crates/krishiv-sql/tests/memory_spill.rs` proves sort, grouped
  aggregation, and hash join spill correctly under a 2MB pool, with a
  negative control confirming the workload genuinely requires spill.
- Deregistering a continuous job now actually reaches its executor: the
  teardown uses `push_cancel_job` (broadened to cancel *assigned* streaming
  tasks — a `stream:loop` task is only `Running` inside a cycle), and the
  executor retires the job's identity on cancel — drops the stateful window
  executor + buffered inputs, purges the assignment inbox's
  `(job, task, attempt)` dedupe entries (`forget_job`), and clears the task
  tombstone. Without this, a recreated job reusing the same deterministic ids
  (`task-streaming`, attempts from 1) had its first cycle silently swallowed
  as an at-least-once duplicate, wedging the cycle fence so every later push
  409'd forever.
- A `Cancelled` continuous-cycle task now releases the input-cycle fence like
  a `Failed` one (previously only Succeeded/Failed cleared it).
- **Continuous-job recovery after executor loss**, found live via the
  Krishiv Platform executor fault loop (`tests/e2e/pipelines/fault_loop.py
  MODE=executor`): (1) the input-cycle fence (`continuous_input_cycles`)
  used to stay stuck forever if the executor holding the task was lost
  before it ever sent a terminal status update — `advance_heartbeat_tick`
  now releases it once a tick evicts the executor and the task shows no
  assignment; (2) a continuous task's `assigned_executor` is sticky across
  cycles by design, so once its executor was evicted and reset to
  `Pending`, nothing retried placement unless a *new* executor happened to
  register afterward — `reset_running_tasks_for_lost_executor` now treats
  an idle (`Succeeded`, between-cycles) continuous task the same as
  `Running`/`Assigned` for reassignment purposes; (3) a freshly reassigned
  task started with an empty accumulator, silently losing whatever the job
  had accumulated — the coordinator now seeds `pending_continuous_restores`
  from the job's latest persisted `ContinuousSnapshot` at reassignment
  time, the same recovery a manual `/restore` call would give, automatically;
  also, deregister now clears a job's persisted snapshot so a later job
  reusing the same id doesn't silently inherit a stale watermark; (4) the
  generic background task-launch loop doesn't understand continuous jobs
  are driven exclusively by an explicit `continuous-push` — it would
  auto-dispatch a spurious extra cycle the moment reassignment (2) set the
  task `Assigned`, racing the next real push — `should_consider_for_launch`
  now excludes streaming jobs outright. Result: zero data corruption or
  loss across ~40 live executor-kill iterations after all four fixes,
  versus consistent corruption/loss before them.
- Coordinator HTTP `POST /api/v1/continuous-register-sql`: register a continuous
  windowed streaming job from **SQL** (`SELECT key, AGG(col) FROM TUMBLE/HOP/
  SESSION(TABLE src, DESCRIPTOR(ts), <ms>) GROUP BY …`). The coordinator compiles
  the window TVF to a `WindowExecutionSpec` itself (`krishiv_sql::
  streaming_window_plan`), so callers pass SQL and stay decoupled from the
  operator spec type; the response returns the fed source table. Verified live on
  k8s: register → push timestamped Arrow IPC via `continuous-push` → `continuous-
  drain` emits exact per-region tumbling-window `SUM`/`COUNT` as the watermark
  closes each window.
- IVM incremental-operator state (per-group SUM/COUNT/AVG/MIN-MAX accumulators
  and DISTINCT multiplicities) is now serialized by `checkpoint_full` and
  reapplied on `restore_full`, so a maintained view is restored **losslessly**
  after a coordinator restart — including sources with genuinely duplicate rows,
  which the materialized source snapshot (a set, not a multiset) cannot capture.
  Verified live on k8s: `spike_b_ivm_kill.py --recreate` converges over 50
  destroy→rebuild→restore cycles (G6/F4).
- Coordinator HTTP `DELETE /api/v1/continuous/{job_id}`: deregister (cancel and
  tear down) a continuous windowed streaming job by id. Mirrors the IVM
  view-drop endpoint so an external reconciler can converge a windowed streaming
  table by removing it. Verified live on k8s as part of the pipeline reconcile
  Drop path (`streams: []` after drop).

### Changed

### Fixed

- Coordinator `submit_job` now **replaces** a terminal (Cancelled/Failed/
  Succeeded) job that shares the incoming job id instead of rejecting it as a
  `DuplicateJob`. `cancel_job` marks a job GC-ready but keeps it in the registry
  until the next GC tick, so a delete-then-recreate flow (e.g. a reconciler
  Replace: `DELETE /api/v1/continuous/{id}` then re-register the same id) raced
  the GC and hit `409 Conflict`, leaving the replacement job `Cancelled`.
  `submit_job` now evicts the terminal same-id job up front; a still-live same-id
  job is still rejected as a duplicate. Regression test
  `submit_job_replaces_a_terminal_job_with_the_same_id`; verified live on k8s
  (reconcile Replace converges to a `Running` job with the new window spec).

- IVM: a checkpoint-restored flow no longer loses its incremental aggregate
  accumulator, which previously made the second recreate-recovery cycle diverge
  (a non-retracting insertion corrupted the materialized view). Operators are
  restored from serialized state, or seeded from the restored source snapshot as
  a fallback (correct for distinct-row Join sources).
- connectors: panic-free vector point-id derivation (`first_chunk` instead of
  slice+`expect`) and Pinecone namespace injection (`as_object_mut` instead of
  index-assign), clearing `clippy::indexing_slicing`/`expect_used` under the
  workspace lint now that `vector-sinks` is feature-active.

## [0.1.0-rc.1] - 2026-06-26

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
- CI: replaced self-hosted runners with ubuntu-latest, optimized workflow triggers.
- Crate READMEs for all 24 workspace crates.
- Universal `skills/` directory for multi-agent skill sharing.

### Changed

- Rewrote the architecture document against the current workspace.
- `PySession::sql_async` upgraded from `block_in_place` to a genuine asyncio coroutine.
- `QueryHandle` now routes collect, writes, and stream submission through a single typed
  handle; use `DataFrame::submit_async()` to obtain a handle.

### Migration Notes

- **`Session.sql_async` (Python)**: Signature updated to align with the Rust Session API. Use `Session.sql_async (same name, updated signature)`.
- **`Stream._tumbling_window_secs_body` (Python)**: Internal helper renamed/updated. Underscore-prefixed, not part of the stable public API. Use `Stream.tumbling_window (public stable API unchanged)`.
- **`SqlDataFrame` (SQL)**: Derive set changed as part of SQL API surface cleanup. Use `SqlDataFrame (struct retained, derive set updated)`.
- **`DataFrameWriter::option` (Rust)**: Writer option() inventory id changed. Use `DataFrameWriter::option(mut self, key, value)`.
- **`StreamingDataFrame` (Rust)**: Gained `Clone` derive for Python streaming join bindings. Use `StreamingDataFrame (#[derive(Clone)] retained)`.
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
