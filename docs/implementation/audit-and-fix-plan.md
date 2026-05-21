# Krishiv Codebase Audit — Master Fix Plan

Generated: 2026-05-21  
Scope: all 17 crates  
Total findings: 164 issues across 5 auditors

Each item is a single durable work unit: one concrete fix + tests + checklist update.  
Work inside this priority ordering. Do not skip tiers unless the user requests it.

---

## P0 — Critical (data loss · panics in library code · security holes)

These must be fixed before any new feature work. Each is a correctness or safety regression.

| # | Work Unit | Crates | Key Location |
|---|-----------|--------|--------------|
| P0.1 | **Fix dual `SqlEngine` split in `SessionBuilder`** — `with_auth`+`with_policy` creates two independent `SessionContext` instances; `sql_as()` can never see tables registered via `register_parquet()`. Share a single `Arc<SqlEngine>` between `Session` and `PolicyEnforcingSqlEngine`. | `krishiv-api`, `krishiv-sql` | `api/lib.rs:228–242` |
| P0.2 | **Fix `SqlDataFrame::collect_with_stats` disconnected context** — creates a fresh `SessionContext::new()` with no registered tables; `create_physical_plan` fails for any table query. Remove the orphaned context or delegate to the engine's own context. | `krishiv-sql` | `sql/lib.rs:304–333` |
| P0.3 | **Fix `block_on_krishiv` runtime creation on every call** — creates a new Tokio runtime per sync API call; panics when called from an existing async runtime. Use `tokio::runtime::Handle::current().block_on()` when inside a runtime, or cache one runtime per thread. | `krishiv-api` | `api/lib.rs:958–966` |
| P0.4 | **Fix blocking filesystem I/O in async shuffle and checkpoint** — `LocalDiskShuffleStore` (write/read/delete) and `LocalFsCheckpointStorage` call `std::fs` inside `async fn`, blocking Tokio executor threads. Wrap all `std::fs` calls in `tokio::task::spawn_blocking`. | `krishiv-shuffle`, `krishiv-checkpoint` | `shuffle/lib.rs:739–812`, `checkpoint/lib.rs:371` |
| P0.5 | **Fix barrier epoch loss in `OperatorQueueReceiver::recv`** — the barrier epoch is silently discarded when data arrives before the barrier is processed; the `let _ = epoch` at line 1417 breaks the exactly-once checkpoint contract. Add an `Option<u64>` pending-barrier slot to re-queue dropped barriers. | `krishiv-exec` | `exec/lib.rs:1407–1425` |
| P0.6 | **Fix silent checkpoint snapshot failure in executor** — `handle_initiate_checkpoint` returns an empty snapshot path on any `StateError` other than `SnapshotUnsupported`; the coordinator records a successful epoch with missing snapshot data. Propagate errors in `CheckpointAckRequest` (add `error: Option<String>` to proto). | `krishiv-executor`, `krishiv-proto` | `executor/lib.rs:577–599` |
| P0.7 | **Fix `RedbStateBackend::load_snapshot` data loss on partial failure** — clears all keys before re-inserting; a mid-operation failure leaves the database empty with no rollback path. Stage all writes in a single redb transaction and abort atomically on error. | `krishiv-state` | `state/lib.rs:594–654` |
| P0.8 | **Fix `unix_now_ms` returns 0 on clock underflow** — a clock behind epoch causes all TTL writes to expire immediately, wiping all state silently. Return `StateError::ClockError` instead of falling back to 0. | `krishiv-state` | `state/lib.rs:790–795` |
| P0.9 | **Fix `decode_if_live` panic on corrupt stored data** — `expect("slice is exactly 8 bytes")` on caller-supplied data panics the process. Return `StateError::CorruptEntry` instead. | `krishiv-state` | `state/lib.rs:830` |
| P0.10 | **Fix `downcast_ref().unwrap()` panics in exec operators** — `format_key_value` (line 136–143) and `LocalAggregator::aggregate` (line 541–546) panic on unexpected Arrow array types. Replace with `ExecError::UnsupportedType`. | `krishiv-exec` | `exec/lib.rs:134–151, 541–546` |
| P0.11 | **Fix `LeaderElection` `block_on` called from async context** — `try_acquire/renew/release` call `Handle::current().block_on()` which panics when invoked from a Tokio worker thread, which is exactly where the controller loop runs. Make the trait methods `async`. | `krishiv-operator` | `operator/lib.rs:1613–1616` |
| P0.12 | **Fix K8s `Merge` patch ignoring `resourceVersion`** — Kubernetes API server ignores `resourceVersion` in Merge patch bodies; two coordinators can simultaneously claim the same lease. Switch to `Patch::Apply` (server-side apply) so `resourceVersion` is enforced by the API server. | `krishiv-operator` | `operator/lib.rs:1451–1476` |
| P0.13 | **Fix flight-sql `check_table_access` never invoked** — `do_get_statement` only calls column masking; table-level deny policy is bypassed entirely. Parse table names from SQL before execution and call `check_table_access` for each. | `krishiv-flight-sql` | `flight-sql/lib.rs:200–243` |
| P0.14 | **Fix `MaskingRule::Redact` schema corruption** — replacing non-string columns with `StringArray` causes `RecordBatch::try_new` schema-mismatch error for INT64/FLOAT64 columns. Use `new_null_array(field.data_type(), batch.num_rows())` for redaction or `cast` to Utf8. | `krishiv-flight-sql`, `krishiv-sql` | `flight-sql/lib.rs:132–134` |
| P0.15 | **Fix non-deterministic hash masking using `DefaultHasher`** — `DefaultHasher` is not cryptographic, not stable across Rust versions/platforms, and not the documented SHA-256. Use `sha2::Sha256` to produce a deterministic hex digest. | `krishiv-governance`, `krishiv-sql`, `krishiv-flight-sql` | `governance/lib.rs:71`, `sql/lib.rs:520–526` |
| P0.16 | **Fix `TtlStateBackend` snapshot portability** — `snapshot()` and `load_snapshot()` don't strip/restore the 8-byte TTL prefix; cross-backend snapshot round-trips double-encode the TTL header. Strip TTL bytes in `snapshot()`, re-encode in `load_snapshot()`. | `krishiv-state` | `state/lib.rs:862–876` |
| P0.17 | **Fix proto wire conversion silent field drops** — `memory_bytes` (TaskRuntimeStats), `throttle_commands`, `memory_used_bytes`, `active_task_count`, `streaming_task_states`, `hot_key_reports` are silently zeroed/omitted in `executor_heartbeat_request_to_wire` and `task_output_metadata_to_wire`. Complete the wire encoding. | `krishiv-proto` | `proto/lib.rs:2817–2865, 3042–3103` |
| P0.18 | **Fix `SlidingWindowOperator::window_starts` potential infinite loop** — no guard on `slide_ms = 0` or `slide_ms << window_size_ms`; loop can run billions of iterations. Validate `0 < slide_ms <= window_size_ms` in the constructor and return `ExecError::InvalidConfig`. | `krishiv-exec` | `exec/lib.rs:922–937` |
| P0.19 | **Fix O(n²) duplicate detection in `check_batch` and `filter_record_batch`** — `rejected_rows.contains()` is a linear scan inside an O(n) outer loop. Replace `Vec<usize>` with `HashSet<usize>` for O(1) lookup. | `krishiv-connectors` | `connectors/lib.rs:858, 886, 700, 1016` |
| P0.20 | **Fix `HttpEmitter::emit` silently ignores 4xx/5xx** — response status is never checked; OpenLineage server errors are swallowed. Add `.error_for_status()?` after `.send().await`. | `krishiv-governance` | `governance/lib.rs:348–358` |
| P0.21 | **Fix `audit_log` emits duplicate events** — `TracingAuditSink::record` and `audit_log` both call `tracing::info!` for the same event. Remove the redundant call from `audit_log`. | `krishiv-governance` | `governance/lib.rs:221–228` |

---

## P1 — High severity (wrong behavior · major stubs · broken contracts)

These break documented guarantees or render major features non-functional.

| # | Work Unit | Crates | Key Location |
|---|-----------|--------|--------------|
| P1.1 | **Fix streaming heartbeat O(jobs×stages×tasks) linear scan** — `apply_streaming_task_state` walks all jobs/stages/tasks on every heartbeat under the coordinator write lock. Add a `HashMap<TaskId, (JobId, StageId)>` index, updated on task assignment/completion. | `krishiv-scheduler` | `scheduler/lib.rs:1543–1554` |
| P1.2 | **Wire gRPC channel pool** — `ExecutorRuntime` and the scheduler each create a new gRPC channel (full TCP+TLS handshake) per RPC call. Add a `ChannelPool: HashMap<endpoint, Channel>` reused across calls. | `krishiv-executor`, `krishiv-scheduler` | `executor/lib.rs:2694–2743`, `scheduler/lib.rs:1936–1980` |
| P1.3 | **Propagate scheduler store errors** — `s.save_job(...).ok()` silently drops persistence failures on job submit and task-state update. Log with `tracing::warn!` at minimum; optionally surface as `SchedulerError::StorageFailure`. | `krishiv-scheduler` | `scheduler/lib.rs:1710–1723, 2067` |
| P1.4 | **Extract partition-loading helper in executor** — ~60 lines of copy-paste partition registration logic duplicated across `execute_batch_fragment`, `execute_shuffle_write_fragment`, and `execute_inmem_shuffle_write`. Extract `load_partitions_into_engine(engine, frag)`. | `krishiv-executor` | `executor/lib.rs:882–919, 1472–1509, 1619–1656` |
| P1.5 | **Wire `EmbeddedBackend` and `SingleNodeBackend` to real execution** — both return `accepted: true` without executing any plan. Wire `EmbeddedBackend::execute` through `SqlEngine::sql()` + `collect()` for local DataFusion execution. | `krishiv-runtime` | `runtime/lib.rs:278–305` |
| P1.6 | **Fix `parse_stream_kafka_partitions` error on empty partition** — empty partition → `Err(InvalidAssignment)` instead of empty batch; breaks zero-lag Kafka catch-up. Return empty `RecordBatch` instead of error. | `krishiv-executor` | `executor/lib.rs:2070–2077` |
| P1.7 | **Fix `S3Sink` full in-memory buffering; `S3Source` full eager load** — `S3Source` reads the entire Parquet file into `Vec<RecordBatch>` before returning; `S3Sink` buffers all output in memory before `put`. Use `put_multipart` for sink; row-group streaming reads for source. | `krishiv-connectors` | `connectors/s3.rs:44–76, 171–186` |
| P1.8 | **Fix checkpoint barrier not forwarded during streaming** — `OperatorMessage::Barrier { epoch }` is silently discarded (`let _ = epoch`) with a "deferred to R8" comment. Forward the barrier through the executor's `checkpoint_ack` path to the `CheckpointCoordinator`. | `krishiv-executor` | `executor/lib.rs:1060–1063` |
| P1.9 | **Fix `StreamTableJoin` restricted to Utf8 join keys** — silently fails with `UnsupportedType` for Int32/Int64 keys, unlike sibling `HashJoin`/`BroadcastJoin`. Use `format_key_value` (already handles Int32/Int64/Utf8) in `process_batch`. | `krishiv-exec` | `exec/lib.rs:1226–1247` |
| P1.10 | **Fix `StreamTableJoin::empty_output` `usize::MAX` column index** — `unwrap_or(usize::MAX)` when join key column is missing produces silent wrong output (all columns pass through). Return `ExecError::ColumnNotFound`. | `krishiv-exec` | `exec/lib.rs:1302–1307` |
| P1.11 | **Fix `S3Source` false `rewindable` capability** — capability flag set but no `rewind()` method exists. Either add `rewind(&mut self)` resetting `cursor` to 0 and add to `Source` trait, or remove the capability flag. | `krishiv-connectors` | `connectors/s3.rs:91–95` |
| P1.12 | **Fix TOCTOU race in `LocalParquetTwoPhaseCommitSink::abort`** — `exists()` then `remove_file` races on the file. Call `remove_file` directly and treat `ErrorKind::NotFound` as success. | `krishiv-connectors` | `connectors/lib.rs:746–754` |
| P1.13 | **Implement `DeadLetterSink` secondary write** — doc says rejected rows are forwarded to a secondary sink but no such field exists. Add `secondary: Box<dyn Sink>` and write rejected rows with error metadata appended. | `krishiv-connectors` | `connectors/lib.rs:980–1025` |
| P1.14 | **Fix `audit_log` hardcoded `AuditOutcome::Allowed`** — denied actions cannot be recorded; `AuditOutcome::Denied` variant exists but is never used. Add `outcome: AuditOutcome` parameter to `audit_log`. | `krishiv-governance` | `governance/lib.rs:219` |
| P1.15 | **Wire `AuditSink` as pluggable dependency** — `audit_log` always uses `TracingAuditSink` directly; the `AuditSink` trait is a dead abstraction. Accept `&dyn AuditSink` as parameter or store on a thread-local. | `krishiv-governance` | `governance/lib.rs:173–175, 221` |
| P1.16 | **Fix `ThresholdSkewRule::median_rows` wrong median for even arrays** — uses lower-median; for `[10, 30]` returns 10 instead of 20. Average the two middle values when `len` is even. | `krishiv-optimizer` | `optimizer/lib.rs:185` |
| P1.17 | **Fix `CoalesceRule::apply` no-op stub** — `AqeRule::apply` unconditionally returns the plan unchanged; coalescing advice is computed but never applied. Implement the plan rewrite or remove the `AqeRule` impl. | `krishiv-optimizer` | `optimizer/lib.rs:273–275` |
| P1.18 | **Fix CDC one-row-per-RecordBatch architecture** — `CdcEvent.before/after` creates a single-row batch per event; schema creation overhead dominates at real CDC throughput. Buffer N events (configurable, default 1000) before building a batch. | `krishiv-connectors` | `connectors/cdc.rs:81–103` |
| P1.19 | **Fix CDC non-deterministic JSON field ordering** — events from the same table with reordered JSON keys produce incompatible schemas. Derive a canonical schema from the schema registry at pipeline creation time. | `krishiv-connectors` | `connectors/cdc.rs:91–99` |
| P1.20 | **Fix flight-sql session created per request** — `make_session()` called on every `do_get_statement` pays full DataFusion context initialization cost per query. Hold `Arc<Session>` on the service struct and reuse. | `krishiv-flight-sql` | `flight-sql/lib.rs:213` |
| P1.21 | **Fix CLI stubs returning exit code 0 for unimplemented features** — `run_savepoint`, `run_restore`, `run_checkpoints_list`, `run_state_inspect` print a message and exit 0; CI scripts cannot detect them as unimplemented. Exit with code 1. | `krishiv-cli` | `cli/lib.rs:299, 699, 729, 812` |
| P1.22 | **Fix K8s lease `holderIdentity: ""` vs `null`** — empty string is not equivalent to null for some lease controllers. Use `serde_json::Value::Null` for the released identity. | `krishiv-operator` | `operator/lib.rs:1587–1589` |
| P1.23 | **Fix `recover_from_store` stale in-memory state** — `or_insert_with` ignores the durable store version when a job already exists in memory. Always overwrite with the store version during recovery. | `krishiv-scheduler` | `scheduler/lib.rs:1627–1632` |
| P1.24 | **Fix `retry_stage` wrong task state assignment** — resets all tasks to `Assigned` regardless of whether they have executors; tasks without executors should be reset to `Pending`. | `krishiv-scheduler` | `scheduler/lib.rs:2908–2918` |
| P1.25 | **Fix `register_partition_lease` / `write_partition` guard asymmetry** — `register` uses `<` (accept ≥ current) but `write` uses `!=` (reject anything but exact match). Make both use `<` for monotonic-token semantics. | `krishiv-shuffle` | `shuffle/lib.rs:594–607, 619–631` |
| P1.26 | **Fix `AggState::Min/Max` sentinel emission** — `Min` initialized to `i64::MAX`, `Max` to `i64::MIN`; if a code path produces a group with zero rows these sentinel values are emitted as real output. Add a `has_value: bool` flag per min/max accumulator. | `krishiv-exec` | `exec/lib.rs:393–406` |
| P1.27 | **Fix `WatchEvent::Delete` silently discarded in operator controller** — force-deleted K8s resources are ignored; jobs are never cleaned up. Handle `WatchEvent::Delete` and cancel the corresponding scheduler job. | `krishiv-operator` | `operator/lib.rs:619` |
| P1.28 | **Fix `RateLimiter` first-call over-refill bug** — `last_refill_ms` initialized to 0; first call with Unix epoch time causes astronomical refill. Initialize `last_refill_ms` to the first observed `now_ms` in `try_consume`. | `krishiv-exec` | `exec/lib.rs:1607–1650` |

---

## P2 — Performance (hot-path allocations · blocking · missing batching)

| # | Work Unit | Crates | Key Location |
|---|-----------|--------|--------------|
| P2.1 | **Add `put_batch`/`get_batch` to state backend** — `RedbStateBackend` opens a new transaction per key; hot-path streaming operators read/write thousands of keys per batch. Add `put_batch(entries)` and `get_batch(keys)` that share one transaction. | `krishiv-state` | `state/lib.rs:475–495` |
| P2.2 | **Fix `HeavyHittersTracker` O(K) linear scan** — `observe()` does `iter().position()` over all K counters per row. Add `HashMap<String, usize>` index mapping value → slot for O(1) lookup. | `krishiv-exec` | `exec/lib.rs:1501–1590` |
| P2.3 | **Fix `HashJoin` one-String-allocation per row** — build phase allocates a `String` key per right-side row. Use `Arc<str>` for string keys; integer key hashing via Arrow `cast` kernel for numeric types. Pre-size with `HashMap::with_capacity(right.num_rows())`. | `krishiv-exec` | `exec/lib.rs:191–209` |
| P2.4 | **Fix `Optimizer::optimize` plan clone per rule** — clones the entire plan before each rule to detect changes; O(rules × plan_size). Have `apply()` return `Option<LogicalPlan>` (None = no change). | `krishiv-optimizer` | `optimizer/lib.rs:144` |
| P2.5 | **Fix `schedulable_executors` descriptor clone per submission** — O(executors) allocation on every `submit_job` call. Return `&[ExecutorDescriptor]` with a scoped lock guard or cache behind `Arc`. | `krishiv-scheduler` | `scheduler/lib.rs:2347–2367` |
| P2.6 | **Consolidate `stability_metrics` to single scan** — walks `self.jobs` six times. Accumulate all counters in one `jobs.values()` pass. | `krishiv-scheduler` | `scheduler/lib.rs:1906–1920` |
| P2.7 | **Consolidate `StageRecord::refresh_state` to single scan** — five separate `.all()`/`.any()` scans of the task list. One pass collecting booleans suffices. | `krishiv-scheduler` | `scheduler/lib.rs:2921–2948` |
| P2.8 | **Compile `DataQualityConfig` regexes once** — `regex::Regex::new()` called inside `find_violations` per batch per rule. Compile regexes into `DataQualityConfig` at construction time. | `krishiv-connectors` | `connectors/lib.rs:947` |
| P2.9 | **Use batch-delete in `ObjectStoreShuffleStore`** — `delete_job_partitions` issues N serial single-object deletes. Use `object_store::ObjectStore::delete_stream` or `delete_objects` for batch deletion. | `krishiv-shuffle` | `shuffle/lib.rs:940–953` |
| P2.10 | **Fix `launch_assigned_task_assignments` O(stages²) upstream check** — `Vec::contains` per stage in a per-stage loop. Use `HashSet<&StageId>` built once before the loop. | `krishiv-scheduler` | `scheduler/lib.rs:2580–2591` |
| P2.11 | **Cache `StatusView` in UI with short TTL** — every HTTP request rebuilds `Vec<JobSummaryView>` + `Vec<ExecutorView>` from scratch. Cache behind `ArcSwap<StatusView>` refreshed by a background task every 500 ms. | `krishiv-ui` | `ui/lib.rs:545–564` |
| P2.12 | **Remove redundant sort in `RedbStateBackend::list_keys`** — redb range scan already returns keys in B-tree order; `keys.sort()` is O(N log N) for nothing. | `krishiv-state` | `state/lib.rs:563–565` |
| P2.13 | **Use `Table::drain` in `clear_namespace`** — current code deletes keys one by one inside a transaction. `Table::drain(range)` removes a range atomically in one operation. | `krishiv-state` | `state/lib.rs:509–534` |
| P2.14 | **Fix `InMemoryTimerService::cancel_timer` O(N) scan** — `retain` on a flat `Vec` is O(N) per cancel. The `BTreeMap<TimerKey, ()>` already has O(log N) removal if the key (including deadline) is known; expose deadline in cancel API. | `krishiv-state` | `state/lib.rs:329–333` |
| P2.15 | **Fix `HashPartitioner::partition` index vec clone** — `UInt32Array::from(indices.clone())` copies a `&[u32]`. Use `UInt32Array::from(indices.as_slice())` to avoid the copy. | `krishiv-shuffle` | `shuffle/lib.rs:429–444` |
| P2.16 | **Fix `scan_orphans` extra stat syscall per entry** — uses `path.is_dir()` (extra `stat`) instead of `entry.file_type()` (returns type from directory entry on Linux at no extra cost). | `krishiv-shuffle` | `shuffle/lib.rs:282–298` |

---

## P3 — Refactoring / cleanup / dead code

| # | Work Unit | Crates | Key Location |
|---|-----------|--------|--------------|
| P3.1 | **Unify `JoinType` enum** — `krishiv-exec` defines `JoinType { Inner }` and `krishiv-plan` defines `JoinType { Inner/Left/Right/Full/Semi/Anti }`. Move the full enum to `krishiv-plan` and re-export from `krishiv-exec`. | `krishiv-exec`, `krishiv-plan` | `exec/lib.rs:113–118` |
| P3.2 | **Unify `ShuffleError` and `StoreError`** — two incompatible error types in the same crate for equivalent operations. Merge into one `ShuffleError` with additional variants. | `krishiv-shuffle` | `shuffle/lib.rs:8–14` |
| P3.3 | **Extract shared `extract_table_hint` utility** — identical multi-byte-unsafe UTF-8 slicing logic duplicated in `krishiv-api` and `krishiv-sql`. Move to a shared helper in `krishiv-sql`, fix the multi-byte UTF-8 bug (use `char_indices` not `find`). | `krishiv-api`, `krishiv-sql` | `api/lib.rs:398–408`, `sql/lib.rs:239–252` |
| P3.4 | **Extract `redb_key`/`redb_prefix` shared helper** — identical prefix-encoding logic copy-pasted across two functions. Extract to one `make_redb_key(op_id, name, key)` free function. | `krishiv-state` | `state/lib.rs:412–435` |
| P3.5 | **Extract shared snapshot deserialization loop** — `InMemoryStateBackend::load_snapshot` and `RedbStateBackend::load_snapshot` share near-identical length-prefixed decode loops. Extract `decode_snapshot_entries(bytes)` iterator. | `krishiv-state` | `state/lib.rs:222–258, 598–654` |
| P3.6 | **Deduplicate `LogicalPlan` / `PhysicalPlan`** — identical structs and method bodies. Use a zero-sized marker type (`Plan<Logical>` / `Plan<Physical>`) or a single `Plan` with a kind enum. | `krishiv-plan` | `plan/lib.rs` |
| P3.7 | **Fix `LocalAggregator` group-key sort** — sorts by `Vec<String>` (lexicographic on string representations); `[1, 2, 10]` sorts as `["1", "10", "2"]`. Sort numerically by maintaining typed group keys or parse before sorting. | `krishiv-exec` | `exec/lib.rs:507–508` |
| P3.8 | **Unify `HashPartitioner` Arrow-type loop** — five near-identical loop bodies for `Int32/Int64/Utf8/Utf8View/LargeUtf8`. Extract `partition_column<A: ArrayAccessor>()` generic helper. | `krishiv-shuffle` | `shuffle/lib.rs:358–449` |
| P3.9 | **Replace `assert_eq!` panics in `CertificationSuite`** — public library code should never panic; replace with `if x != y { return Err(...) }`. | `krishiv-connectors` | `connectors/lib.rs:473–477` |
| P3.10 | **Remove `record`/`upsert` dual API** — `LocalJobRegistry::record` is a pointless alias for `upsert`. Remove `record`, update callers to use `upsert`. | `krishiv-runtime` | `runtime/lib.rs:130–151` |
| P3.11 | **Fix Coordinator 4-constructor fan-out** — `active`, `standby`, `active_with_config`, `standby_with_config` all call a private `build()`. Simplify to two public constructors taking `Option<CoordinatorConfig>`. | `krishiv-scheduler` | `scheduler/lib.rs:1371–1397` |
| P3.12 | **Fix benchmark setup inside iteration loop** — `tpch_sf10.rs` calls `SqlEngine::new()` and `register_parquet()` inside `b.iter()`; benchmarks measure setup overhead. Move setup outside `b.iter()`. | `krishiv-bench` | `benches/tpch_sf10.rs:37–55` |
| P3.13 | **Fix Nexmark benchmarks bypassing the SQL engine** — Q1/Q2 benchmark pure Rust iterator logic; results are not representative. Run through `SqlEngine` as tpch_sf10 does. Add Q3–Q8 coverage. | `krishiv-bench` | `benches/nexmark.rs:12–51` |
| P3.14 | **Enforce `ConnectorCapabilities` mutual exclusion** — `bounded` and `unbounded` can both be `true`. Add `debug_assert!(!self.bounded || !self.unbounded)` or enforce at construction. | `krishiv-connectors` | `connectors/lib.rs:63–175` |
| P3.15 | **Implement `CdcToLakehousePipeline::run()`** — the pipeline struct has `new()` and `validate()` but no execution path. Wire Kafka consumer → `parse_debezium_envelope` → Iceberg sink. | `krishiv-connectors` | `connectors/cdc.rs:122–178` |
| P3.16 | **Wire `AuditSink` into flight-sql** — flight-sql executes queries, authenticates clients, and denies access without emitting any audit events. Add `audit_log()` calls at auth failure, access denied, and query executed. | `krishiv-flight-sql`, `krishiv-governance` | `flight-sql/lib.rs` |
| P3.17 | **Fix `metrics::init()` race on global tracer provider** — parallel test calls to `init()` race on `set_tracer_provider`. Guard with `OnceLock` or accept the replacement semantics and document it. | `krishiv-metrics` | `metrics/lib.rs:127` |
| P3.18 | **Fix `StreamingAqeGuard::plan_is_streaming` shallow check** — only checks top-level plan kind; misses hybrid batch/streaming plans. Recursively walk plan nodes. | `krishiv-optimizer` | `optimizer/lib.rs:370` |
| P3.19 | **Remove `CostModel` and `StreamRule` dead traits** — neither trait has implementations; both are dead abstractions. Remove until needed. | `krishiv-optimizer` | `optimizer/lib.rs:45–48, 72–78` |
| P3.20 | **Fix `LocalFsCheckpointStorage` temp dir leak** — `ephemeral()` uses a naming scheme but never cleans up temp dirs on drop. Wrap in a `TempDir` RAII guard. | `krishiv-checkpoint` | `checkpoint/lib.rs:519–528` |
| P3.21 | **Fix `full_path` partial path-traversal** — strips `..` components but does not reject absolute path components. Add a check that the final path has the base as a prefix. | `krishiv-checkpoint` | `checkpoint/lib.rs:530–537` |
| P3.22 | **Fix `ui_job_detail` double lock acquisition** — acquires coordinator read lock twice per request with a window for state change between. Take lock once, extract both coordinator metadata and job detail, release. | `krishiv-ui` | `ui/lib.rs:531–535` |
| P3.23 | **Fix `krishiv_shuffle_bytes_written_total` always zero** — counter is hardcoded to 0. Wire a shared `AtomicU64` counter incremented by shuffle stores. | `krishiv-ui` | `ui/lib.rs:416` |
| P3.24 | **Fix `event_time_now()` returns Unix epoch seconds string, not ISO 8601** — OpenLineage spec requires ISO 8601; current output fails schema validation. Use `chrono` or `jiff` to format RFC 3339. | `krishiv-governance` | `governance/lib.rs:362–370` |
| P3.25 | **Fix `TPC-H` benchmark skips silently** — no warning when `KRISHIV_TPCH_DATA_DIR` is unset; benchmark silently reports trivially fast. Emit `eprintln!` warning so CI logs show the skip. | `krishiv-bench` | `benches/tpch_sf10.rs:46–50` |

---

## Suggested implementation order

Work through tiers sequentially. Within each tier, prefer items that unblock others:

```
P0.1 → P0.3 (fix API correctness before adding features)
P0.4         (unblock async-safe I/O across shuffle + checkpoint)
P0.7–P0.9   (state backend correctness before any streaming work)
P0.5–P0.6   (barrier + checkpoint ack before R8 checkpoint work)
P0.10–P0.11 (eliminate all panics in library code)
P0.12–P0.13 (operator + flight security before any deployment)
P0.14–P0.16 (masking correctness before governance sign-off)
P0.17       (proto field completeness — needed by P1.3 and P1.8)

P1.1–P1.2   (performance foundations before load testing)
P1.5        (wire real execution into runtime backends)
P1.8        (checkpoint barrier forwarding — R8 dependency)
...

P2 items can be worked in parallel with P1 when relevant crates are touched.
P3 items fold into the same PR as the functional fix that touches the same file.
```

Total P0: 21 items  
Total P1: 28 items  
Total P2: 16 items  
Total P3: 25 items  
**Grand total: 90 work units** (some grouped findings share one fix)
