# Krishiv Implementation Status

## Validated Physical Plan Graph Lowering (2026-06-06)

Completed the physical-plan graph integrity and placeholder-contract production-readiness slice:

- Moved logical-to-physical lowering into `krishiv-plan` as the canonical implementation and re-exported it from `krishiv-exec`.
- Rewrote both node IDs and input references with stable physical IDs, fixing the prior dangling-edge graph.
- Preserved typed operators, partitioning, broadcast eligibility, row estimates, and output schemas during lowering.
- Added shared logical and physical plan validation for blank/whitespace IDs, duplicate IDs, blank/whitespace inputs, dangling references, self-references, cycles, blank plan names, and node-count limits.
- Used iterative topological validation so adversarial deep plans cannot overflow the stack.
- Removed the plan-builder panic at the node-count threshold; limits are now reported as typed validation errors at plan boundaries.
- Validated plans before local acceptance, distributed serialization, coordinator HTTP execution, streaming-spec extraction, and Flight action decode.
- Bound Flight execute-plan envelope name and execution kind to the serialized physical plan, rejecting tampered or inconsistent metadata.
- Removed unused `OperatorKind`/`PhysicalOperator`, runtime `TaskSpec`/`TaskReport`/`TaskExecutor`, and executor placeholder-output contracts.
- Added focused coverage for annotation-preserving lowering, rewritten edges, duplicate/dangling/self/cyclic graphs, forward references, runtime rejection, and Flight envelope tampering.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-plan --tests --offline
cargo check -p krishiv-exec --tests --offline
cargo check -p krishiv-executor --tests --offline
cargo check -p krishiv-runtime --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Empty-node plans remain valid because current runtime APIs intentionally use the physical plan name as the executable SQL or stream descriptor.
- Workspace check passed with the pre-existing executor barrier dead-code warnings and Flight SQL `unused_mut` warning; those remain reserved for the final cleanup slice.

Next useful commands:
```bash
cargo check -p krishiv-plan --tests --offline
cargo check --workspace --tests --offline
```

---

## Coordinator-Owned Bounded Window Sharding Hardening (2026-06-06)

Completed the distributed bounded-window partitioning production-readiness slice:

- Removed the unreachable runtime-side shard branch that treated Flight failover coordinators as executor shards; remote clients now submit one request to the active coordinator, which owns partitioning and placement.
- Added a shared Arrow partitioning abstraction with a versioned, type-tagged SHA-256 routing contract for `Int32`, `Int64`, `Float64`, `Utf8`, and `Boolean` keys.
- Made partitioning fail closed on zero shards, blank/missing keys, null keys, unsupported types, key-type drift, and full Arrow schema drift.
- Replaced per-shard boolean masks with row-index gathers, preserving each source batch's row order without `O(rows * shards)` mask allocation.
- Canonicalized floating NaN payloads so values grouped together by window semantics cannot be routed to different tasks.
- Made the active coordinator cap fanout by schedulable executors and input rows, omit empty hash shards, create one task per non-empty shard, and bind each task to exactly one task-scoped `InlineIpc` partition.
- Added atomic job admission for exact `TaskId -> InputPartition` maps, retained those maps for task retry, and cleaned them on success, failure, cancellation, and completed-job eviction.
- Added process/coordinator-qualified bounded job IDs with checked sequence allocation instead of millisecond-only IDs.
- Cleared partial inline output after failed/cancelled fanout jobs so successful sibling shards cannot leak incomplete results.
- Required executor window assignments to contain exactly one decoded input table whose name matches the validated fragment topic.
- Made bounded retries recompute from complete task input with ephemeral state, preventing failed-attempt state from being double-applied.
- Hardened shared aggregation-key extraction against null and out-of-bounds access.
- Added focused coverage for deterministic/lossless routing, all supported key types, invalid partition contracts, NaN canonicalization, schema drift, exact task/input binding, unsafe topics, executor input-count/topic rejection, and aggregation-key bounds/null handling.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-common --tests --offline
cargo check -p krishiv-exec --tests --offline
cargo check -p krishiv-scheduler --tests --offline
cargo check -p krishiv-executor --tests --offline
cargo check -p krishiv-runtime --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Task-scoped inline inputs remain active-coordinator memory state. This slice does not claim bounded-job recovery across an active-coordinator crash.
- Workspace check passed with the pre-existing executor barrier dead-code warnings and Flight SQL `unused_mut` warning; those remain reserved for the final cleanup slice.

Next useful commands:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo check --workspace --tests --offline
```

---

## Executable Table-UDF Registration Hardening (2026-06-06)

Completed the table-valued UDF registration production-readiness slice:

- Removed schema-only `StubTableUdf` registration and the profile-dependent stub policy; unsupported `LANGUAGE RUST`, `PYTHON`, `WASM`, missing-language, and other non-SQL table-function DDL now fails before registry or DataFusion mutation.
- Kept `LANGUAGE SQL AS '...'` as the executable DDL contract and added a typed `SqlError::InvalidTableFunction` boundary for malformed definitions.
- Replaced the overloaded stub type used by programmatic Rust registration with a real `ClosureTableUdf` that requires a body at construction.
- Validated non-empty function names, non-empty output schemas, unique output columns, non-empty SQL bodies, unique argument/output declarations, and fully consumed DDL input.
- Contained panics from closure-backed UDTFs and from the SQL-body sync/async bridge, returning typed UDF errors instead of unwinding through the query engine.
- Required SQL-body UDTFs to run under an active multi-thread Tokio runtime and converted unsupported runtime contexts into explicit execution errors.
- Enforced declared output column names and data types for both closure-backed and SQL-body UDTFs before creating a DataFusion table provider.
- Added focused coverage for unsupported-language non-registration, incomplete SQL definitions, trailing SQL, duplicate names, invalid closure definitions, closure panic containment, output-schema mismatch, and missing-runtime invocation.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-sql --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Rust, Python, and WASM table-function bodies do not have certified execution runtimes in this workspace; they are now rejected rather than deferred through a placeholder.
- Workspace check passed with pre-existing warnings in `krishiv-executor` and `krishiv-flight-sql`; those remain reserved for the final cleanup slice.

Next useful commands:
```bash
cargo check -p krishiv-sql --tests --offline
cargo check --workspace --tests --offline
```

---

## Continuous Job Execution and Queue Consistency (2026-06-06)

Completed the continuous-job execution and registry-consistency production-readiness slice:

- Replaced lossy compact registration fragments with a versioned, validated JSON `WindowExecutionSpec` payload that preserves all aggregates, output names, watermark settings, TTL, and multi-source configuration.
- Added shared window-spec validation at plan and execution boundaries for empty columns, zero windows/slides/gaps/TTLs, invalid aggregates, duplicate outputs, and incomplete multi-source watermark contracts.
- Registered distributed continuous jobs as typed `stream:loop` tasks and executed each push as one bounded, coordinator-fenced cycle over executor-retained window state.
- Routed remote cycles through normal assignment delivery, rejected undeliverable in-process HTTP targets instead of reporting false success, and rolled task/cycle state back on dispatch failure.
- Kept completed cycle tasks terminal for idempotent status retries while retaining logical job ownership, captured output exactly once per accepted terminal update, and blocked new input until prior output is drained.
- Rejected the obsolete `stream:continuous` executor fragment so unprocessed Inline IPC input can no longer be silently echoed as window output.
- Removed the Flight SQL shadow continuous registry; embedded registration, push, and drain now have one in-process registry owner.
- Hardened the local registry with typed errors, duplicate/blank-ID rejection, exact schema binding, atomic bounded queue admission, serialized drains, and transactional window-state rollback that retains queued input after failures.
- Made session continuous-job IDs take precedence over same-name unbounded SQL tables, preventing input from being routed to the wrong owner.
- Added focused coverage for lossless spec encoding, invalid registration, typed assignment flags, inline distributed execution, legacy-fragment rejection, cycle fencing/rollback, output backpressure, terminal retry idempotence, duplicate registration, schema/capacity enforcement, failed-drain retention, and same-name routing.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-plan --tests --offline
cargo check -p krishiv-exec --tests --offline
cargo check -p krishiv-scheduler --tests --offline
cargo check -p krishiv-executor --tests --offline
cargo check -p krishiv-runtime --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Continuous cycle input, fencing, and undrained output remain coordinator-memory state. This slice does not certify exactly-once behavior across coordinator or executor crashes; that requires source/sink/checkpoint-specific recovery integration.
- Workspace check passed with pre-existing warnings in `krishiv-executor` and `krishiv-flight-sql`; this slice did not address those final-cleanup items.

Next useful commands:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo check --workspace --tests --offline
```

---

## Schema-Bound Unbounded Memory Stream Ingestion (2026-06-06)

Completed the in-memory unbounded stream ingestion production-readiness slice:

- Replaced the data-less `unbounded_memory_stream` placeholder with a schema-bound continuous DataFusion table and a shared typed `ContinuousTableInput`.
- Added bounded synchronous and asynchronous batch submission with explicit schema validation, queue-full, closed-input, and lock-poisoned errors.
- Added idempotent input closure that drops the final sender and propagates end-of-stream consistently through `Session` and cloned `Stream` handles.
- Added configurable channel capacity so callers can select a bounded backpressure budget instead of relying on an implicit unbounded queue.
- Serialized streaming-table registration, rejected empty names and schemas, rejected duplicate providers, and restored a raced catalog entry instead of silently replacing it.
- Made direct construction of an unbounded `Stream` fail closed unless it is attached to a registered input source.
- Replaced the continuous table's second-execution panic with an explicit stream error; the table remains intentionally single-consumer because one Tokio receiver cannot provide replay semantics.
- Added SQL round-trip coverage plus schema mismatch, queue backpressure, close/idempotence, duplicate registration, empty-schema, bounded-stream ingestion, and second-execution failure coverage.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-api --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-executor` and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-api --tests --offline
cargo check --workspace --tests --offline
```

---

## Native Scalar UDF Registration Hardening (2026-06-06)

Completed the native scalar UDF registration production-readiness slice:

- Changed `Session::register_scalar_udf` to return `Result<()>`; durable-profile rejection and SQL synchronization failures can no longer be reported as successful no-ops.
- Added immutable `NativeScalarUdfPolicy` snapshots so durability profile, production-mode, and full-privilege override decisions remain consistent across registry mutation and DataFusion synchronization.
- Native UDF registration now rejects empty names at both the public API and SQL bridge boundaries.
- Registry writes now surface poisoned-lock failures and preserve the previous same-name registration for rollback if DataFusion synchronization fails.
- Added `UdfRegistry::remove_scalar` for transactional rollback and guarded rollback with `Arc::ptr_eq` so a concurrent replacement is not overwritten.
- Updated Rust callers to handle registration results and the Python facade to raise the dedicated Python `UdfError`.
- Upgraded batch integration coverage to plan and execute the registered `double` UDF through DataFusion and verify its Arrow output.
- Added deterministic profile-policy tests, empty-name rejection coverage, registry removal coverage, and removed the duplicate unused full-privilege environment helper.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-api --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-executor` and `krishiv-flight-sql`; the prior `krishiv-udf` dead-code warning was removed in this slice.

Next useful commands:
```bash
cargo check -p krishiv-api --tests --offline
cargo check --workspace --tests --offline
```

---

## Streaming Side-Output Delivery Hardening (2026-06-06)

Completed the streaming late-data side-output production-readiness slice:

- Replaced per-batch watermark reconstruction with an execution-owned `SideOutputRouter::route_batch` contract that retains one monotonic watermark across micro-batches and classifies against the previous batch's watermark.
- Added typed failures for missing event-time columns, non-`Int64` event time, null event-time values, oversized batches, Arrow selection failures, and upstream stream failures instead of silently dropping invalid batches.
- Added `StreamingOutputStreams` and `NamedSideOutputStream`; callers now receive independently consumable main and late-data streams backed by bounded channels.
- Side-output routing now backpressures when either consumer falls behind and cancels the routing task when both receivers are dropped.
- `execute_stream_async` now fails closed when a side output is configured, preventing the former silent loss of late rows; callers must use `execute_stream_with_side_output_async`.
- Windowed side-output execution now extends the window watermark lag by the configured side-output grace period, so rows retained by the router are not subsequently discarded by the window operator.
- Watermark lag and lateness arithmetic now use overflow-safe `i128` calculations for the full `u64` configuration range.
- Added focused coverage for cross-batch routing, grace-period aggregation, dual-stream error propagation, missing/wrong/null event-time inputs, fail-closed API use, and maximum lag/threshold values.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-exec --tests --offline
cargo check -p krishiv-api --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-api --tests --offline
cargo check --workspace --tests --offline
```

---

## Connector Typed Source Checkpoint Restore (2026-06-06)

Completed the connector source checkpoint/restore production-readiness slice:

- Added a typed `CheckpointSource` contract for capturing, encoding, decoding, and restoring exact source read positions.
- Added a typed `ConnectorError::Offset` boundary for malformed, incompatible, non-boundary, and out-of-range offsets.
- Canonical Parquet offsets now reject trailing/truncated encodings and platform-width overflow; the duplicate `sink::ParquetOffset` definition is now a compatibility re-export of the canonical type.
- `ParquetSource` and `S3Source` now advertise checkpoint capability and restore validated `ParquetOffset` positions without accepting offsets past the loaded batch set.
- `InMemoryKafkaSource` now restores validated topic/partition batch-boundary offsets, rejects cross-source and mid-batch offsets, and advances offsets with checked integer conversion/addition.
- Added checkpoint lifecycle certification that restores both initial and intermediate positions, compares replayed Arrow batches exactly, and verifies deterministic resulting offsets.
- Added exactly-once pair capability certification that requires a typed checkpoint source and a checkpoint-coupled 2PC sink.
- Broker-backed Kafka remains intentionally non-checkpoint-capable until partition assignment and seek-based restore implement `CheckpointSource`; runtime guidance no longer claims manual commit alone provides exactly-once.
- Added failure coverage for malformed offset bytes and a connector that advertises checkpoint support but performs a no-op restore.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-connectors --tests --all-features --offline
cargo check -p krishiv-connectors --tests --no-default-features --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-connectors --tests --all-features --offline
cargo check --workspace --tests --offline
```

---

## Connector Two-Phase Commit Contract Hardening (2026-06-06)

Completed the connector two-phase commit production-readiness slice:

- `TwoPhaseCommitSink` now exposes capabilities from the actual sink implementation and requires cloneable handles so coordinator decision retries can be certified.
- Two-phase commit capability declarations now automatically include their transactional and checkpoint prerequisites, and capability validation rejects incoherent declarations.
- Added generic 2PC lifecycle certification covering prepare/abort, repeated abort, prepare/commit, and repeated commit.
- All in-memory, local Parquet, transactional Kafka, and staged Parquet 2PC implementations now declare the complete protocol capability set.
- The staged Parquet sink now uses epoch-qualified final object names, preventing a later epoch from overwriting `part-0.parquet` from an earlier committed epoch.
- Parquet staging allocation now uses create-new semantics, skips existing staged/final handles after restart, detects handle exhaustion, and cleans up incomplete writes.
- Parquet commit and orphan recovery now publish without replacing an existing final file and tolerate retries after an uncertain commit response.
- Added negative certification for dishonest capability declarations, retry lifecycle coverage, cross-epoch Parquet preservation coverage, and upgraded the exactly-once matrix to certify concrete sinks.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-connectors --tests --offline
cargo check -p krishiv-connectors --tests --all-features --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-connectors --tests --all-features --offline
cargo check --workspace --tests --offline
```

---

## Connector Rewindable Source Contract Hardening (2026-06-06)

Completed the connector rewindability production-readiness slice:

- `ParquetSource` now implements rewind through the `Source` trait, so generic connector callers reset the source instead of reaching the trait's default no-op implementation.
- The public Parquet compatibility reset API now delegates to the trait implementation.
- `InMemoryKafkaSource` now retains its configured starting offset and restores both its batch cursor and offset during reset.
- Source certification now validates connector capability invariants, requires exactly one boundedness mode, and requires rewindable sources to expose offsets.
- Added typed rewind lifecycle certification that proves offset advancement, exact reset restoration, replayed batch shape, and deterministic post-replay offsets.
- Added regression coverage for a broken source inheriting the default no-op reset, plus successful generic certification for Parquet and in-memory Kafka sources.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-connectors --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-connectors --tests --offline
cargo check --workspace --tests --offline
```

---

## SQL-Body UDTF Argument Binding (2026-06-06)

Completed the SQL table-function argument production-readiness slice:

- `CREATE FUNCTION ... RETURNS TABLE` parsing now preserves typed formal argument definitions instead of discarding the function signature.
- `LANGUAGE SQL` table functions now bind `$1`, `$2`, and later positional placeholders to runtime literal arguments with SQL-safe string escaping.
- Placeholder scanning preserves quoted strings, quoted identifiers, line comments, nested block comments, and dollar-quoted segments.
- Invalid `$0`, out-of-range placeholders, unterminated quoted/comment segments, wrong invocation arity, non-finite floats, and unsupported binary SQL arguments fail closed with typed UDF errors.
- Malformed placeholder references are rejected during `CREATE FUNCTION` registration rather than being deferred until first invocation.
- DataFusion table-function calls now reject computed/non-literal arguments instead of silently coercing them to `NULL`.
- Added parser, binder, registration, arity, non-literal, and end-to-end SQL execution test coverage.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-sql --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-sql --tests --offline
cargo check --workspace --tests --offline
```

---

## Scheduler Checkpoint Finalization Guard (2026-06-06)

Completed the scheduler checkpoint finalization production-readiness slice:

- Checkpoint finalization now proves the coordinator is still committing the same epoch before transitioning to `Committed`.
- Failed finalization leaves the coordinator in `Committing`, preserves the pending commit, and returns a typed checkpoint error instead of silently committing the requested epoch.
- `CheckpointInner::finalize_ack` now propagates finalization errors, rejects missing jobs, and increments committed metrics only after a successful state transition.
- gRPC and in-process checkpoint ack paths now sync checkpoint-inner state back to the outer coordinator before surfacing finalization errors.
- Restore regression coverage was aligned with the manifest contract: raw invalid rollback metadata remains on disk, but invalid epochs stay excluded from valid-epoch scans.

Validation:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo check --workspace --tests --offline
```

---

## Scheduler Checkpoint Ack Contract Hardening (2026-06-05)

Completed the scheduler checkpoint ack production-readiness slice:

- Checkpoint acks now fail before quorum accounting when the ack `job_id` does not match the owning checkpoint coordinator.
- Checkpoint acks with snapshot paths now must use the canonical checkpoint storage path for the active job/epoch/operator/task.
- Sync and async commit paths now read all declared snapshot files before writing metadata, manifest, or the latest-epoch hint; missing snapshots fail closed instead of sealing an unrestorable epoch.
- Added focused scheduler tests for mismatched ack job IDs, noncanonical snapshot paths, sync missing-snapshot commits, and async missing-snapshot storage commits.

Validation:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo test -p krishiv-scheduler receive_ack_rejects --offline
cargo test -p krishiv-scheduler async_commit_storage_rejects_missing_snapshot --offline
cargo test -p krishiv-scheduler checkpoint --offline
cargo test -p krishiv-scheduler checkpoint_ack --offline
cargo check --workspace --tests --offline
cargo fmt --all
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo test -p krishiv-scheduler restore --offline
cargo test --workspace --no-fail-fast --offline
```

---

## Checkpoint Manifest Contract Hardening (2026-06-05)

Completed the core checkpoint manifest production-readiness slice:

- Active checkpoint epoch validation now requires a manifest that covers `metadata.json`, rejects unsafe manifest-relative paths, validates metadata version and job/epoch identity, and requires manifest coverage for every snapshot referenced by metadata.
- Sync and async `validate_epoch` now share the same metadata/manifest contract, so restart scans and gRPC checkpoint paths do not diverge.
- `write_epoch_metadata` and `write_epoch_metadata_async` now reject incompatible metadata before persisting it.
- Empty manifests, metadata-less manifests, metadata identity mismatches, unmanifested snapshot references, and path-traversal-style manifest entries now fail closed.
- Integration checkpoint fixtures now write snapshot references for the actual storage job ID instead of hardcoded test metadata.

Validation:
```bash
cargo check -p krishiv-checkpoint --tests --offline
cargo test -p krishiv-checkpoint --offline
cargo test -p krishiv-scheduler coordinator_restore --offline
cargo test -p krishiv restore_local_dry_run --offline
cargo check --workspace --tests --offline
cargo fmt --all
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo test -p krishiv-scheduler checkpoint --offline
cargo test --workspace --no-fail-fast --offline
```

---

## Scheduler Restore Metadata Identity Hardening (2026-06-05)

Completed the scheduler restore metadata validation slice:

- Scheduler checkpoint restore now validates `CheckpointMetadata::VERSION` before accepting an epoch.
- Scheduler checkpoint restore now rejects metadata whose embedded `job_id` or `epoch` does not match the requested restore target, even when the metadata bytes match the manifest.
- Restore activation now fails before pruning newer epochs or rewriting the epoch hint when metadata identity is invalid.
- Added scheduler tests for incompatible metadata version, job-id mismatch, and failed activation preserving future epochs plus the latest epoch hint.

Validation:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo test -p krishiv-scheduler coordinator_restore --offline
cargo test -p krishiv-scheduler restore_activation --offline
cargo check --workspace --tests --offline
cargo fmt --all
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo test -p krishiv-scheduler checkpoint --offline
cargo test --workspace --no-fail-fast --offline
```

---

## CLI Restore Dry-Run Integrity Hardening (2026-06-05)

Completed the user-facing restore CLI production-readiness slice:

- Local-mode `krishiv restore` now validates checkpoint metadata version, requested job/epoch identity, and the epoch integrity manifest before printing a dry-run restore plan.
- Parseable but tampered checkpoint metadata now fails closed instead of producing an operator-facing restore plan.
- Added CLI tests for a valid local dry-run and a manifest-mismatch rejection.

Validation:
```bash
cargo check -p krishiv --tests --offline
cargo test -p krishiv restore_local_dry_run --offline
cargo check --workspace --tests --offline
cargo fmt --all
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo test -p krishiv restore --offline
cargo test --workspace --no-fail-fast --offline
```

---

## Scheduler Restore Activation Hardening (2026-06-05)

Completed the scheduler restore production-readiness slice:

- Scheduler checkpoint restore now rejects `validate_epoch == Ok(false)` integrity failures instead of only propagating storage/parse errors.
- Restore fencing validation now treats the live leader-election token supplied by gRPC as authoritative, falling back to the checkpoint coordinator token only when no live token is supplied.
- Added `CheckpointCoordinator::activate_restored_epoch` to clear in-flight checkpoint state, set the restored committed epoch, and carry the active owner fencing token forward for future barrier acks.
- Added `Coordinator::activate_job_restore_from_checkpoint_with_fencing` for mutating restore activation of tracked checkpointed jobs.
- Restore activation now prunes valid active checkpoint epochs newer than the restored epoch and rewrites the epoch hint, preventing restart recovery from resurrecting abandoned future state.
- gRPC `restore_job` now uses the mutating activation path and syncs checkpoint state back into the checkpoint inner lock.
- Governance restore audit events now fire after successful activation instead of during read-only restore validation.
- Added scheduler tests for hash-mismatched checkpoint rejection and rollback activation with future-epoch pruning plus live-token continuation.

Validation:
```bash
cargo check -p krishiv-scheduler --tests --offline
cargo test -p krishiv-scheduler coordinator_restore --offline
cargo check --workspace --tests --offline
cargo fmt --all
git diff --check
```

Notes:
- Workspace check passed with pre-existing warnings in `krishiv-udf`, `krishiv-executor`, and `krishiv-flight-sql`; this slice did not address those unrelated warnings.

Next useful commands:
```bash
cargo test -p krishiv-scheduler checkpoint --offline
cargo test --workspace --no-fail-fast --offline
```

---

## Full Stabilization Waves 1–4 (2026-06-05)

Implemented Waves 1–4 on branch `cursor/full-stabilization-dd55` (PR #59):

### Wave 1 — Shuffle leases & wiring
- Durable shuffle lease sidecars (`.lease` / object-store sidecars) with monotonic validation and restart tests.
- `open_shuffle_backend_from_uri` for `file://`, `s3://`, `memory://`.
- Executor `--shuffle-uri` / `KRISHIV_SHUFFLE_URI` wired for distributed-durable object-store shuffle.
- Profile-aware UDF guards in `krishiv-udf`, `krishiv-sql` (`sync_scalar_udfs` / `sync_aggregate_udfs`), `krishiv-api` session registration, and CREATE FUNCTION stubs.

### Wave 2 — CEP partial state
- `CepOperator::persist_to_state` / `restore_from_state` plus JSON snapshot helpers for checkpoint metadata.

### Wave 3–4 — Observability & profile guards
- `GET /api/v1/jobs/{job_id}/diagnose` returns structured `ObservabilityReport`.
- `inc_checkpoint_committed` metrics on checkpoint quorum (sync) and finalize (async).
- Window operator watermark persistence across tumbling/sliding/session restore paths.
- Flight SQL, UI, and K8s lease simulation guards use durability-profile helpers (not production-only).

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Full Stabilization Wave 0 (2026-06-05)

Implemented Wave 0 P0 fixes on branch `cursor/full-stabilization-dd55`:

### Security & metadata durability
- JCP federation HTTP submit/poll attach coordinator bearer tokens.
- Non-terminal task metadata saves are synchronous under durable profiles.
- `SingleNodeLeader` bumps fencing token only on fresh leadership acquisition.
- Operator controller opens `RedbMetadataStore` from `KRISHIV_METADATA_PATH` with fail-closed writes.
- Metadata store `flush()` waits for in-flight background writes.

### Barriers & checkpoints
- Barrier gRPC auth matches task gRPC (token configured ⇒ required).
- Barrier stream acks deferred until checkpoint completion via `SharedBarrierAckRegistry`.
- Continuous executor gRPC stubs return `Rejected` instead of fake `Accepted`.

### Distributed execution
- `ExecutePlan` routes through coordinator HTTP in proxy mode; streaming uses typed plan nodes.
- `streaming_spec_from_plan` derives window specs from `PhysicalPlan` nodes (no hardcoded test tumbling).
- Flight client attaches bearer auth from `KRISHIV_FLIGHT_API_KEY` / `KRISHIV_API_KEY` / `KRISHIV_API_KEYS`.
- Continuous/bounded Flight fallbacks profile-gated like batch SQL fallback.

### Kafka & state
- SQL `register_kafka_source` respects manual commit under durable profiles.
- Kafka table loop calls `commit_current_offset` when auto-commit is disabled.
- `FjallStateBackend::ephemeral()` forbidden under durable profiles.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Production Stabilization F1–F15 (2026-06-05)

Implemented full F1–F15 stabilization on branch `cursor/f1-f15-stabilization-dd55`:

### F1 — Coordinator auth & restore fencing
- `validate_runtime_security_config` now requires bearer tokens for `single-node-durable` and rejects `--insecure` gRPC on all durable profiles.
- Token file read failures fail startup via `validate_coordinator_bearer_token_sources`.
- Queued jobs rejected in durable/production profiles (fail-closed admission).
- gRPC `restore_job` passes live leader fencing token; durable restores fail without token validation.

### F2 — HTTP client auth
- All `coordinator_http_client` requests attach `Authorization: Bearer` from `KRISHIV_COORDINATOR_BEARER_TOKEN`.

### F3 — Executor gRPC & state
- Barrier gRPC wired with `ExecutorTaskAuthConfig`; durable profiles require task bearer token when task/barrier servers enabled.
- Checkpoint RPC state uses `FjallStateBackend::open_for_profile`; in-memory shuffle omitted outside dev-local.

### F4 — Kafka pipeline
- Durable profiles use `RdkafkaKafkaSource` with `KAFKA_BOOTSTRAP_SERVERS`; simulation connectors dev-only.
- Source throttle token-bucket enforced via `try_consume` (not log-only).

### F5 — Flight SQL routing
- Typed `ContinuousRegister` / `ContinuousPush` / `ContinuousDrain` proxy through coordinator HTTP when configured (matches `BoundedWindow`).

### F6–F8 — Durability guards
- `memory://` checkpoint URIs gated by `allows_memory_checkpoint_uri(profile)`.
- `flight_client::execute_remote_plan` SQL-comment fallback profile-gated.

### F9–F15 — API/SQL/operability
- `SessionBuilder::from_env` rejects embedded mode under durable profiles.
- `SqlEngine::with_in_memory_catalog` rejected in durable/production profiles.
- UDF sandbox production guard (`KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS` escape hatch).
- K8s lease simulation forbidden in production.
- Checkpoint storage commit failures increment `inc_checkpoint_failed` metrics.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-scheduler -p krishiv-runtime -p krishiv-executor -p krishiv-flight-sql -p krishiv-api -p krishiv-udf -p krishiv-checkpoint --lib --no-fail-fast
```

---

## Production Stabilization Sprint A–C + Final Slice (2026-06-05)

Completed end-to-end wiring and production guards on branch `cursor/production-stabilization-dd55` (merged via PR #57):

### Sprint A — Profile-aware fragments & auth
- `validate_job_fragments` wired into scheduler `validate_job()` via `resolve_durability_profile()`.
- Executor hot paths use `task_body_for_profile` / `decode_for_profile` (batch, streaming, execution model).
- `set_allow_anonymous()` returns `Err` when `KRISHIV_PRODUCTION=1`; operator/coordinator call sites updated.
- Executor CLI rejects `memory://` checkpoint URIs for durable profiles (`validate_durable_startup`).
- Removed public `BarrierSimulator` export; production path is `BarrierInjector` + `TaskRunner::handle_initiate_checkpoint`.
- EO certification tests use `TransactionalKafkaSink::new_for_profile(DevLocal, ...)`.

### Sprint B/C — Runtime & API gating
- Remote Flight SQL-comment fallback disabled outside dev-local (`allows_remote_sql_comment_fallback`).
- Alpha APIs gated: `unbounded_memory_stream`, sliding/session windows, multi-source watermark (`allows_alpha_api`).
- `krishiv-plan` exports `validate_job_fragments`, `task_body_for_profile`; added `krishiv-proto` dependency.

### Final slice — workspace quality
- Fixed `block_on` for single-worker multi-thread Tokio runtimes (uses `block_in_place`).
- Fixed `temporal_join` schema assembly and zero-lookback eviction; repaired test batch helpers.
- Flight SQL `run_blocking` uses thread offload on current-thread runtimes.
- Stabilized flaky redb/metrics tests under parallel `--workspace` runs.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
cargo +nightly clippy --workspace --all-targets
```

Blockers: `krishiv-python` tests require system `libpython3.12` (excluded from workspace lib run).

---

## Production Stabilization Waves 0–3 (2026-06-05)

Implemented cross-cutting production hardening across Waves 0–3 (merged via PR #56):

### Wave 0 — Security & data loss
- Added `krishiv-common::production` guards (`KRISHIV_PRODUCTION`, profile fail-closed helpers).
- Coordinator HTTP: bearer auth middleware for durable/production profiles; startup validation when HTTP enabled without tokens.
- `NonBlockingStoreHandle`: fail-closed writes (sync fallback instead of drop) wired from durability profile.
- Executor window fragments: pass `state_dir/<job_id>` into `execute_bounded_window`.
- Flight SQL: auth on handshake, prepared statements, DoAction; production requires `KRISHIV_API_KEYS`.
- UI: production fail-closed when token file unreadable.

### Wave 1 — Correctness & durability
- Typed task fragments: `TypedTaskFragment::decode_for_profile` rejects legacy strings in durable profiles.
- Object-store checkpoint writes: staging key + commit pattern.
- Kafka SQL: manual commit (no auto-commit) in durable/production profiles.
- `TransactionalKafkaSink::new_for_profile` rejects durable profiles.
- `S3Sink`: 1024-batch pending cap.
- `memory://` checkpoint URIs blocked in production mode.

### Wave 2 — Feature completion
- Remote streaming `accept_plan`: registers continuous stream via Flight instead of hard error.
- CEP operator: records `last_barrier_epoch` on barrier.
- SQL: non-SQL UDTF DDL rejected in production mode.
- `FjallStateBackend::open_for_profile` factory.

### Wave 3 — Operability
- Operator HTTP router uses `CoordinatorDaemonConfig::http_sidecar(DistributedDurable)` with auth.
- Re-exported `DurabilityProfile` from `krishiv-common` and `krishiv-scheduler`.

---
