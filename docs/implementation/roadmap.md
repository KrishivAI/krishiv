# Krishiv Resolution Roadmap

Unified roadmap consolidating all findings from file-level, module-level,
crate-level, architecture, distributed-systems, API/interface, performance,
security, test coverage, dependency cleanup, dead code, refactoring, and
common-code extraction reviews.

---

## Phase 1 — Correctness, Safety, and Production Blockers

| Phase | Priority | Finding Source | File / Module / Crate Area | Issue | Action | Rationale | Expected Impact | Effort | Dependencies |
|---|---|---|---|---|---|---|---|---|---|
| 1 | P0 | File-level | `krishiv-exec/src/aggregate.rs:198,233` | `sum` and `count` aggregation use unchecked `+=` on `i64`/`u64`; silent wraparound in release builds | Replace with `checked_add` returning `ExecError` on overflow | Silent data corruption in windowed aggregation | Correctness | Small | None |
| 1 | P0 | File-level | `krishiv-exec/src/session.rs:246` | Session window does not enforce monotonic watermark; decreasing watermark accepted unconditionally | Add `if new_watermark_ms >= self.prev_watermark_ms` guard matching tumbling/sliding | Late events incorrectly accepted after watermark regression | Correctness | Small | None |
| 1 | P0 | File-level | `krishiv-exec/src/temporal_join.rs:152-162` | Left-outer temporal join silently drops unmatched rows when table version not found | Produce null-filled right-side row for left-outer when no version exists | Incorrect join semantics for left-outer queries | Correctness | Medium | None |
| 1 | P0 | File-level | `krishiv-exec/src/window/tumbling.rs:320-323`, `session.rs:349-353` | `unwrap_or(0)` on key string parse: malformed keys silently become 0, causing key collisions | Return `ExecError` on parse failure or use original string as fallback key | Distinct keys collapse to same output row | Correctness | Small | None |
| 1 | P0 | File-level | `krishiv-proto/src/wire.rs:987-993` | `panic!` in `input_partition_descriptor_to_wire` for `InMemory` variant | Return `Err(WireError)` instead of panicking | Any code path that accidentally serializes `InMemory` crashes the process | Safety | Small | None |
| 1 | P0 | File-level | `krishiv-proto/src/wire.rs` (6 functions) | `TraceContext` silently dropped on all 6 wire types (heartbeat, assignment, status) | Add `trace_context` field to proto messages and wire conversions | Distributed tracing data lost across coordinator-executor boundary | Reliability | Medium | Proto change |
| 1 | P0 | Module-level | `krishiv-connectors/src/parquet.rs:34-67` | `ParquetSource::open` eagerly loads ALL batches into memory; OOM on large files | Add streaming read with configurable batch limit or lazy iterator | Memory exhaustion on production data | Safety/Scalability | Medium | None |
| 1 | P0 | Module-level | `krishiv-connectors/src/s3.rs:44-83` | `S3Source::open` eagerly downloads entire S3 object into memory | Use streaming download with chunked reads | Memory exhaustion on large S3 objects | Safety/Scalability | Medium | None |
| 1 | P0 | File-level | `krishiv-connectors/src/transactional_kafka.rs:126,131` | `.unwrap()` on `Mutex::lock()` in `TransactionalKafkaRegistry` | Use `unwrap_or_else(\|e\| e.into_inner())` or return `ConnectorError` | Mutex poisoning cascades panics to all subsequent callers | Safety | Small | None |
| 1 | P0 | File-level | `krishiv-connectors/src/parquet.rs:197` | `.expect("writer is set above")` in `ParquetSink::write_batch` | Replace with `.ok_or_else(...)` returning `ConnectorError` | Code path change could cause panic in production sink | Safety | Small | None |
| 1 | P0 | Module-level | `krishiv-connectors/src/two_phase.rs:225-248` | `next_handle += 1` without overflow check in `LocalParquetTwoPhaseCommitSink::prepare` | Use `checked_add` matching `TwoPhaseParquetSink` pattern | `u64` overflow causes panic in prepare path | Safety | Small | None |
| 1 | P0 | Module-level | `krishiv-runtime/src/flight_client.rs:795-834` | Standalone `do_action` lacks 64 MiB response cap present in `FlightClientPool::do_action` | Add same response size cap to standalone function | Unbounded memory allocation from malicious/buggy server | Safety | Small | None |
| 1 | P0 | Module-level | `krishiv-runtime/src/flight_client.rs:518` | No per-batch timeout in `execute_sql`/`stream_sql` streaming loop | Add per-batch timeout (e.g., 60s) with `tokio::time::timeout` | Stalled server hangs client indefinitely | Reliability | Small | None |
| 1 | P1 | File-level | `krishiv-exec/src/window/session.rs:141-142` | `unwrap_or(0)` on session state restore: corrupted JSON silently accepted | Return error on missing/corrupt fields | Restored session gets timestamp 0 causing incorrect boundaries | Correctness | Small | None |
| 1 | P1 | File-level | `krishiv-common/src/async_util.rs:15` | `expect()` on Tokio runtime creation in `OnceLock`; panics on OS resource exhaustion | Return `Result` or use static fallback | Process crash on runtime creation failure | Safety | Medium | API change |
| 1 | P1 | File-level | `krishiv-common/src/arrow.rs:14,30,46` | `unwrap()` on `RecordBatch::try_new` in public non-test helper functions | Gate behind `#[cfg(test)]` or return `Result` | Production code calling these helpers panics on schema mismatch | Safety | Small | None |
| 1 | P1 | Module-level | `krishiv-proto/src/task.rs:24` | `debug_assert!(start <= end)` in `KeyGroupRange::new` compiled out in release | Use `try_new` in wire deserialization path or make `new` return `Result` | Invalid key group range silently accepted in release builds | Correctness | Small | None |
| 1 | P1 | Module-level | `krishiv-proto/src/wire.rs` | `TaskOutputMetadata.watermark_ms` silently dropped on wire | Add `watermark_ms` field to proto `TaskOutputMetadata` message | Streaming watermark not propagated to coordinator | Correctness | Small | Proto change |
| 1 | P1 | Module-level | `krishiv-proto/src/wire.rs` | `ExecutorTaskAssignment.requires_reattach` silently dropped on wire | Add `requires_reattach` field to proto message | Re-attach protocol signal lost across wire | Correctness | Small | Proto change |
| 1 | P1 | File-level | `krishiv-common/src/async_util.rs:50-52` | `unix_now_ms()` silently returns 0 on clock error; 20+ callers use unchecked version | Log warning on fallback or promote `_checked` usage | Zero timestamps in TTL, checkpoint timing, observability | Correctness | Medium | Call-site updates |
| 1 | P1 | Module-level | `krishiv-scheduler/src/store.rs:1078-1083` | Busy-wait spin loop in `StoreCommand::Flush` handler | Replace with `Notify` or `watch` channel | CPU consumption during flush waits | Performance | Small | None |
| 1 | P1 | File-level | `krishiv-common/src/production.rs:21-26` | `resolve_durability_profile()` silently falls back to `DevLocal` on invalid env value | Log warning or return `Result` on parse failure | Production deployment misconfigured to least-durable mode without indication | Safety | Small | None |
| 1 | P1 | Module-level | `krishiv-scheduler/src/etcd_metadata.rs:91` | `blocking_lock()` on `tokio::sync::Mutex` panics if called from async context | Use `lock().await` or restructure to avoid async caller | Panic in etcd metadata persistence path | Safety | Small | None |
| 1 | P1 | Module-level | `krishiv-runtime/src/flight_client.rs:239` | `blocking_write()` on `tokio::sync::RwLock` in sync `with_alternate` builder | Document clearly or restructure to async builder | Panic if builder called from async context | Safety | Small | None |
| 1 | P1 | Module-level | `krishiv-runtime/src/flight_client.rs:212` | Silent normalization failure on empty URL stores empty endpoint string | Fail eagerly on empty/whitespace URL | Opaque connection failure later | Correctness | Small | None |
| 1 | P1 | Module-level | `krishiv-flight-sql/src/host.rs:281` | `.expect("run_blocking thread panicked")` propagates thread panics | Use `.join().map_err(...)` to convert to `Status` error | Thread panic in closure crashes calling async task | Safety | Small | None |
| 1 | P1 | Module-level | `krishiv-ui/src/lib.rs:664` | `state.metrics_cache.lock().unwrap()` in `/metrics` handler | Use `unwrap_or_else(\|e\| e.into_inner())` | Mutex poisoning crashes metrics endpoint | Safety | Small | None |
| 1 | P1 | File-level | `krishiv-python/src/stream.rs:213,247,251,262,287` | 5 `lock().unwrap()` calls that cascade-panic on mutex poisoning | Use `unwrap_or_else(\|e\| e.into_inner())` or propagate error | Mutex poisoning crashes Python extension | Safety | Small | None |
| 1 | P1 | File-level | `krishiv-api/src/session.rs:113,121` | `.expect()` in `shared_embedded_runtime()` panics on infrastructure failure | Return `Result` from runtime construction | Process crash on embedded runtime init failure | Safety | Medium | API change |
| 1 | P1 | File-level | `krishiv-governance/src/lib.rs:527,572` | `.expect("reqwest client")` in `HttpEmitter::new()` panics on TLS init failure | Return `Result` from emitter construction | Process crash on HTTP client construction | Safety | Small | API change |
| 1 | P1 | Module-level | `krishiv-connectors/src/cdc.rs:427,553` | CDC pipeline returns `Result<(), String>` instead of `ConnectorError` | Migrate to `ConnectorError` with typed variants | Callers cannot match on error variants | Correctness | Medium | None |
| 1 | P2 | Module-level | `krishiv-proto/src/ids.rs:99` | `AttemptId::next()` uses `saturating_add` on `u32`; silent saturation at `u32::MAX` | Log warning or return `Option` on saturation | Long-lived streaming job with retries silently exhausts attempts | Correctness | Small | None |

---

## Phase 2 — Architecture and Abstraction Cleanup

| Phase | Priority | Finding Source | File / Module / Crate Area | Issue | Action | Rationale | Expected Impact | Effort | Dependencies |
|---|---|---|---|---|---|---|---|---|---|
| 2 | P1 | Architecture | `krishiv-scheduler/src/coordinator/mod.rs:301-327` | Triple-lock acquisition in `advance_heartbeat_tick`: all three write locks held simultaneously | Refactor to acquire locks sequentially with release between; or use message-passing for heartbeat tick | Blocks all readers (heartbeats, health, gRPC status) for entire tick duration | Performance | Large | None |
| 2 | P1 | Architecture | `krishiv-scheduler/src/coordinator_sharded.rs` | Dual-state drift between outer `Coordinator` and inner sharded locks acknowledged in comments | Consolidate to single source of truth or use atomic state snapshots | Inconsistent reads between heartbeat and job lifecycle paths | Correctness | XL | None |
| 2 | P1 | Architecture | `krishiv-runtime/src/flight_client.rs:444-488 vs 795-834` | Two `do_action` code paths with different safety guarantees (pool vs standalone) | Unify standalone functions to use `FlightClientPool` or share size cap/retry | Inconsistent safety properties for same operation | Maintainability | Medium | None |
| 2 | P1 | Architecture | `krishiv-connectors/src/error.rs:49-51` | `ConnectorError::IoStr` migration alias used pervasively; loses structured I/O error info | Migrate all `IoStr` call sites to `ConnectorError::Io(#[from] std::io::Error)` | Structured error info (ErrorKind) lost; callers cannot match on I/O error types | Maintainability | Medium | None |
| 2 | P1 | Architecture | `krishiv-common/src/validate.rs` | `validate_safe_id` uses blocklist while other 4 validators use allowlists; inconsistent approach | Standardize on allowlist approach for all validators | Callers must know which function to use; `..` passes `is_safe_identifier` but blocked by `validate_safe_id` | Security | Medium | None |
| 2 | P1 | Architecture | `krishiv-common/src/error` (multiple files) | Three different error patterns in one crate: public field, private field + accessor, thiserror derive | Standardize on thiserror derive for all error types | Inconsistent error construction across crate | Maintainability | Medium | None |
| 2 | P2 | Architecture | `krishiv-exec/src/operator_runtime.rs:98-159,200-393` | Three nearly identical stream implementations for Tumbling/Sliding/Session windows | Extract common streaming window execution into shared function with window-kind dispatch | Code duplication; changes must be replicated 3 times | Maintainability | Medium | None |
| 2 | P2 | Architecture | `krishiv-proto/src/task.rs` | `HeartbeatHotKeyReport` and `StreamingProgressReport` use raw `String` instead of typed IDs | Migrate to `JobId`, `TaskId`, `ExecutorId` typed IDs | Violates crate's own typed-ID convention | Maintainability | Small | None |
| 2 | P2 | Architecture | `krishiv-proto/src/checkpoint.rs` | `CheckpointSourceOffset.partition_id` and `CheckpointAckRequest.operator_id` are unvalidated `String` | Add typed ID validation | Empty strings accepted; inconsistent with other validated IDs | Correctness | Small | None |
| 2 | P2 | Architecture | `krishiv-proto/src/task.rs:29` | `KeyGroupRange::try_new` returns `Result<Self, String>` instead of `IdError` | Migrate to `IdError` | Inconsistent with crate error conventions | Maintainability | Small | None |
| 2 | P2 | Architecture | `krishiv-runtime/src/in_process.rs` (multiple) | Internal/coordinator errors mapped to `RuntimeError::transport` instead of `InvalidState` | Remap to semantically correct error variants | Callers cannot distinguish transport from state errors | Maintainability | Small | None |
| 2 | P2 | Architecture | `krishiv-connectors` (multiple files) | Blocking filesystem I/O in async-callable methods (`two_phase.rs:193`, `parquet.rs:180`, `feature_store.rs:107`) | Wrap in `spawn_blocking` or document sync-only contract | Violates AGENTS.md: "do not hide blocking filesystem work inside async tasks" | Correctness | Medium | None |
| 2 | P2 | Architecture | `krishiv-connectors/src/cdc.rs:1187-1195` | `block_in_place` + nested `block_on` in `RdkafkaCdcEventSource::poll_records` | Use `block_in_place` alone with direct `await` | Redundant runtime re-entry; wasteful | Performance | Small | None |
| 2 | P2 | Architecture | `krishiv-scheduler/src/coordinator/mod.rs:459-483` | Write lock held during batch task launches (20 jobs per batch) | Move launch dispatch outside write lock; use read lock for collection | Blocks readers for batch launch duration | Performance | Medium | None |
| 2 | P2 | Architecture | `krishiv-scheduler/src/coordinator_daemon.rs:260-261` | 2-second drain window is best-effort heuristic for graceful shutdown | Implement proper barrier waiting for in-flight RPCs | New leader could start while old RPCs still executing | Correctness | Large | None |
| 2 | P2 | Architecture | `krishiv-common/src/production.rs:85-87` | `allow_anonymous_http_override` bypasses production HTTP auth when env var set | Remove override in production mode or add explicit warning | Production deployment with unauthenticated HTTP endpoints | Security | Small | None |
| 2 | P2 | Architecture | `krishiv-connectors/src/sink.rs:67` | Duplicate `ConnectorConfig` struct (also in `config.rs:15`) with different Debug impls | Remove duplicate from `sink.rs`; use `config.rs` version | Two public types with same name but different behavior | Maintainability | Small | None |

---

## Phase 3 — Maintainability, Dead Code, and Refactoring

| Phase | Priority | Finding Source | File / Module / Crate Area | Issue | Action | Rationale | Expected Impact | Effort | Dependencies |
|---|---|---|---|---|---|---|---|---|---|
| 3 | P1 | Module-level | `krishiv-common/src/blocking.rs` | Entire module unused outside its own crate; overlaps with `async_util.rs` | Remove `blocking.rs` module and `run_blocking_safely` export | Dead code; confusing overlap with `block_on` | Maintainability | Small | None |
| 3 | P1 | File-level | `krishiv-common/src/arrow.rs:6-15` | `make_single_int_schema` and `make_single_int_batch` never called externally | Remove or gate behind `#[cfg(test)]` | Dead public API surface | Maintainability | Small | None |
| 3 | P1 | File-level | `krishiv-common/src/arrow.rs:18-23,34-39` | `make_test_user_ts_schema` and `make_test_key_ts_schema` public but only called internally | Change to `pub(crate)` | Unnecessary public exposure | Maintainability | Small | None |
| 3 | P1 | Module-level | `krishiv-common/src/arrow.rs` | Module name `arrow` suggests general utilities but contains only test fixtures | Rename to `test_fixtures` or gate behind `#[cfg(test)]` | Misleading module name | Maintainability | Small | None |
| 3 | P2 | Module-level | `krishiv-proto/src/checkpoint.rs:16,37,45` | `InitiateCheckpointRequest`, `AbortCheckpointRequest`, `CheckpointInitiateResponse` have no wire conversions or proto messages | Remove or implement full wire path | Dead domain types exported from crate | Maintainability | Small | None |
| 3 | P2 | Module-level | `krishiv-proto/src/task.rs:56,82` | `TaskAssignment` and `TaskStatusUpdate` defined but not exported and have no wire conversion | Remove or implement | Dead internal types | Maintainability | Small | None |
| 3 | P2 | Module-level | `krishiv-proto/src/executor.rs:531` | `ExecutorHeartbeat` overlaps with `ExecutorHeartbeatRequest`; no wire conversion | Remove or consolidate | Duplicate concept | Maintainability | Small | None |
| 3 | P2 | Module-level | `krishiv-proto` (barrier.proto) | Entire `barrier.proto` file generates code but has zero domain types or wire conversions | Implement domain types or remove proto | Dead proto contract | Maintainability | Medium | None |
| 3 | P2 | File-level | `krishiv-scheduler/src/grpc.rs:783` | `#[allow(dead_code)]` on `serve_coordinator_executor_grpc` | Remove function or use it | Unused public function | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-scheduler/src/coordinator/task_assignment.rs:764` | `#[allow(dead_code)]` on `get_or_connect_channel` instance method | Remove method | Unused method | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-scheduler/src/store.rs:958,1256` | `#[allow(dead_code)]` on `encode_metadata_snapshot` and `decode_metadata_snapshot` | Remove functions or use them | Unused functions | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-scheduler/src/coordinator/recovery.rs:140-143` | Dead code path: `if !self.job_coordinators.contains_key(job_id)` after `insert` is always false | Remove unreachable branch | Dead code | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-scheduler/src/cluster_control.rs:251` | Duplicate `#[cfg(test)]` annotation | Remove one | Cosmetic redundancy | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-runtime/src/coordinator_http_client.rs:89` | `#[allow(dead_code)]` on `BatchSqlResponseBody::job_id` | Use `_job_id` or drop field | Dead field | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-runtime/src/execution_runtime.rs:585` | `plan_execution_kind` trivially delegates to `plan.kind()` | Remove or document as intentional re-export | Adds no value | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-exec/src/join.rs:616` | Unused `_num_right_cols` variable | Remove | Dead variable | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-exec/src/temporal_join.rs:176` | Unused `_num_stream_cols` variable | Remove | Dead variable | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-exec/src/temporal_join.rs:213` | `format!("<unsupported_type>")` on static string | Use `"<unsupported_type>".to_owned()` | Unnecessary format macro | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-api/src/dataframe.rs:76` | `#[allow(dead_code)]` on `coordinator_url` field | Remove field or use it | Dead field | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-sql/src/policy.rs:39` | `#[allow(dead_code)]` on `PolicyEnforcingSqlEngine::inner()` | Remove or use | Dead method | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-sql/src/window_functions.rs:106` | `#[allow(dead_code)]` on `extract_scalar_i64()` | Remove or use | Dead helper | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-sql/src/udf.rs:18` | `#[allow(dead_code)]` on `sync_scalar_udfs()` | Remove or use | Dead wrapper | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-python/src/schema.rs:19` | `#[allow(dead_code)]` on `PySchema::fields` | Remove field or use | Dead field | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-python/src/batch.rs:128` | `#[allow(dead_code)]` on `make_example_batch()` | Remove or gate behind test | Dead function | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-python/src/stream.rs:130` | `#[allow(dead_code)]` on `_tumbling_window_secs_body()` | Remove or use | Dead function | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-python/src/relation.rs:56` | `#[allow(dead_code)]` on `PyRelation::cached` | Remove field or use | Dead field | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-python/src/lakehouse.rs:124` | `#[allow(dead_code)]` on `PySchemaRegistryConfig::inner` | Remove field or use | Dead field | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-connectors/src/s3.rs:33` | `#[allow(dead_code)]` on `S3Source::store` field | Document purpose or use `Arc<dyn ObjectStore>` differently | Misleading annotation | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-executor/src/runner.rs:1131,1140` | `#[allow(dead_code)]` on two items | Remove or use | Dead code | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-cep/src/matcher.rs:124` | `#[allow(dead_code)]` on `PartitionedCepMatcher` | Implement integration or remove | Dead code pending integration | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-connectors/src/parquet.rs:103-106,133-143` | Duplicate `reset()` methods with different signatures | Consolidate to single implementation | Confusing API | Maintainability | Small | None |
| 3 | P2 | File-level | `krishiv-connectors/src/quality.rs:100-103` | `unwrap_or_default()` on `SystemTime::now()` silently returns 0 on broken clock | Log warning or use `_checked` variant | Silent zero timestamps in quality data | Correctness | Small | None |
| 3 | P2 | Module-level | `krishiv-scheduler/src/store.rs:1130-1188` | Non-fail-closed metadata writes silently dropped when channel full | Log at `error` level or increment metric | Metadata loss without caller awareness | Reliability | Small | None |
| 3 | P2 | Module-level | `krishiv-common/src/async_util.rs:47` | `unix_now_ms_checked()` clamps to `i64::MAX` silently | Log warning on clamping | Silent data corruption for extreme values | Correctness | Small | None |
| 3 | P2 | Module-level | `krishiv-proto/src/wire.rs:736-742` | Deprecated parallel shuffle arrays silently truncate on mismatched lengths via `.zip()` | Add length validation before zip | Silent data loss on mismatched arrays | Correctness | Small | None |
| 3 | P2 | Module-level | `krishiv-proto/src/wire.rs:756-769` | Zero-valued `TaskRuntimeStats` silently dropped (filter `> 0`) | Change to `has_stats = true` always when stats struct present | Valid zero-row tasks lose runtime stats | Correctness | Small | None |
| 3 | P2 | Module-level | `krishiv-common/src/production.rs` | Environment-based guards re-read env vars on each call; not cached | Cache env values at startup or use `OnceLock` | Inconsistent if env changes at runtime | Correctness | Small | None |
| 3 | P3 | Module-level | `krishiv-common/src/async_util.rs:1` | Redundant module-level `#![forbid(unsafe_code)]` (crate-level already set) | Remove redundant attribute | Cosmetic noise | Maintainability | Small | None |
| 3 | P3 | Module-level | `krishiv-common/src/chaos.rs:1` | Same redundant `#![forbid(unsafe_code)]` | Remove redundant attribute | Cosmetic noise | Maintainability | Small | None |
| 3 | P3 | File-level | `krishiv-common/src/hash.rs:32` | `expect("sha256 is at least 8 bytes")` on infallible conversion | Use indexed byte read for zero-cost assurance | Unnecessary panic path (though infallible) | Maintainability | Small | None |
| 3 | P3 | File-level | `krishiv-scheduler/src/coordinator_daemon.rs:700` | `let mut body = body;` shadows itself needlessly | Remove self-shadow | Cosmetic | Maintainability | Small | None |
| 3 | P3 | File-level | `krishiv-scheduler/src/lib.rs:120-122` | Commented-out test import block | Remove dead comment | Cosmetic | Maintainability | Small | None |
| 3 | P3 | Module-level | `krishiv-common/src/chaos.rs:43` | `SeqCst` ordering for simple counter; `Relaxed` suffices | Change to `Relaxed` | Minor unnecessary memory barrier | Performance | Small | None |

---

## Phase 4 — Performance and Scalability

| Phase | Priority | Finding Source | File / Module / Crate Area | Issue | Action | Rationale | Expected Impact | Effort | Dependencies |
|---|---|---|---|---|---|---|---|---|---|
| 4 | P1 | Module-level | `krishiv-exec/src/aggregate.rs:198,233` | Integer `sum`/`count` can silently overflow in release mode | Use `checked_add` or `saturating_add` | Silent wraparound produces wrong aggregation results | Correctness/Performance | Small | None |
| 4 | P1 | Module-level | `krishiv-exec/src/join.rs:383` | Full `broadcast_batch.clone()` in `BroadcastJoin::build()` | Use `Arc<RecordBatch>` instead of cloning | O(broadcast_size) allocation on every build | Performance | Medium | None |
| 4 | P1 | Module-level | `krishiv-exec/src/interval_join.rs:60-62,238-240` | Per-match batch clones in interval join | Use `Arc` wrapping or row-index gathers | O(matches * batch_size) allocations | Performance | Medium | None |
| 4 | P2 | Module-level | `krishiv-exec/src/operator_runtime.rs:98-102,200-204` | Repeated spec field cloning (12x per bounded/streaming call) | Clone spec once and reuse references | Unnecessary string/vector clones | Performance | Small | None |
| 4 | P2 | Module-level | `krishiv-exec/src/window/tumbling.rs:237-253` | `build_output_batch` allocates new Schema/arrays per closed window | Pre-allocate schema; reuse builder pattern | Many small allocations for high-cardinality keys | Performance | Medium | None |
| 4 | P2 | Module-level | `krishiv-exec/src/window/sliding.rs:186` | Key cloned once per overlapping window-start | Use `Rc<str>` or `Arc<str>` for key sharing | N clones per event where N = size/slide | Performance | Small | None |
| 4 | P2 | Module-level | `krishiv-exec/src/window/tumbling.rs:49`, `sliding.rs:41`, `session.rs:43` | Unbounded window state accumulation without TTL | Add configurable max-key eviction or document TTL requirement | Memory exhaustion under high key cardinality | Scalability | Medium | None |
| 4 | P2 | Module-level | `krishiv-exec/src/interval_join.rs:25-28` | Unbounded per-key buffers in interval join | Add configurable capacity limit with backpressure | Memory exhaustion under high-throughput streams | Scalability | Medium | None |
| 4 | P2 | Module-level | `krishiv-exec/src/cep.rs:16` | Unbounded per-key CEP state | Add configurable max-key limit | Memory exhaustion under high key cardinality | Scalability | Small | None |
| 4 | P2 | Module-level | `krishiv-connectors/src/s3.rs:162-167` | `S3Sink` no per-batch byte limit; only batch count limit (1024) | Add total byte budget alongside batch count | Memory exhaustion from large batches | Scalability | Small | None |
| 4 | P2 | Module-level | `krishiv-connectors/src/feature_store.rs:61-104` | All Parquet fragments loaded into memory on startup | Stream fragments lazily | Memory exhaustion on large feature stores | Scalability | Medium | None |
| 4 | P2 | Module-level | `krishiv-connectors/src/feature_store.rs:107-133` | Unbounded in-memory `live` vec without eviction | Add compaction or size limit | Memory exhaustion under continuous writes | Scalability | Medium | None |
| 4 | P2 | Module-level | `krishiv-scheduler/src/coordinator/mod.rs:459-483` | Write lock held during batch task launches | Move launch dispatch outside write lock | Blocks readers for batch duration | Performance | Medium | Phase 2 lock refactor |
| 4 | P3 | Module-level | `krishiv-runtime/src/execution_runtime.rs:335-506` | Multiple sequential `block_on` calls per trait method in `RemoteExecutionRuntime` | Batch async operations into single `block_on` where possible | Multiple runtime re-entries add overhead | Performance | Medium | None |
| 4 | P3 | Module-level | `krishiv-exec/src/continuous.rs:364` | `_rejected_count` from quality hook not logged or exposed | Log or expose via metrics | Lost observability of quality hook effectiveness | Observability | Small | None |

---

## Phase 5 — Testing, Validation, and Hardening

| Phase | Priority | Finding Source | File / Module / Crate Area | Issue | Action | Rationale | Expected Impact | Effort | Dependencies |
|---|---|---|---|---|---|---|---|---|---|
| 5 | P1 | Testing | `krishiv-exec` (window operators) | No property-based tests for window aggregation correctness | Add proptest for arbitrary input sequences, out-of-order events, late data | Edge cases in window boundaries not covered by unit tests | Correctness | Medium | None |
| 5 | P1 | Testing | `krishiv-exec` (window operators) | No fuzz tests for window spec validation | Add `cargo-fuzz` targets for `validate_window_execution_spec` | Adversarial inputs could bypass validation | Safety | Medium | None |
| 5 | P1 | Testing | `krishiv-proto` (wire functions) | No fuzz tests for wire deserialization | Add `cargo-fuzz` targets for `from_wire` functions | Malformed protobuf input could cause panics or OOM | Safety | Medium | None |
| 5 | P1 | Testing | `krishiv-connectors` (Kafka offsets) | No property-based round-trip tests for `KafkaOffset` encode/decode | Add proptest for arbitrary offset values | Edge cases in offset serialization | Correctness | Small | None |
| 5 | P1 | Testing | `krishiv-scheduler` (checkpoint) | No distributed recovery tests simulating coordinator crash during checkpoint | Add integration test for coordinator crash mid-checkpoint | Recovery correctness not validated | Reliability | Large | None |
| 5 | P1 | Testing | `krishiv-scheduler` (leadership) | No test for split-brain scenario with stale fencing tokens | Add test for concurrent coordinator claims | Fencing correctness not validated under adversarial conditions | Reliability | Medium | None |
| 5 | P2 | Testing | `krishiv-exec` (temporal join) | Left-outer temporal join has no test coverage for unmatched rows | Add test for left-outer with missing table versions | Bug from Phase 1 has no regression test | Correctness | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-exec` (session window) | No test for non-monotonic watermark regression | Add test passing decreasing watermark | Bug from Phase 1 has no regression test | Correctness | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-exec` (aggregate) | No test for integer overflow in sum/count | Add test with values near `i64::MAX` | Bug from Phase 1 has no regression test | Correctness | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-runtime` (flight client) | No test for standalone `do_action` response size limit | Add test sending >64 MiB response | Missing size cap not covered by tests | Safety | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-runtime` (flight client) | No test for per-batch timeout in streaming loop | Add test with slow/stalled server | Timeout behavior not validated | Reliability | Medium | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-connectors` (Parquet/S3) | No test for large file OOM behavior | Add test with size-limited streaming read | Memory exhaustion risk not covered | Safety | Medium | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-connectors` (two-phase) | No test for `next_handle` overflow | Add test approaching `u64::MAX` | Overflow panic not covered | Safety | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-common` (production) | No test for silent `DevLocal` fallback on invalid env | Add test with malformed `KRISHIV_DURABILITY_PROFILE` | Silent misconfiguration not covered | Correctness | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-scheduler` (store) | No test for busy-wait spin behavior in flush | Add test measuring CPU during flush wait | Spin loop behavior not validated | Performance | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-python` (stream) | No test for mutex poisoning recovery in stream methods | Add test simulating poisoned lock | Cascade panic behavior not covered | Safety | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-ui` (metrics) | No test for mutex poisoning in `/metrics` handler | Add test simulating poisoned lock | Cascade panic behavior not covered | Safety | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-flight-sql` (host) | No test for thread panic propagation in `run_blocking` | Add test with panicking closure | Panic propagation not covered | Safety | Small | Phase 1 fix |
| 5 | P2 | Testing | `krishiv-connectors` (CDC) | No integration test for `ConnectorError` propagation from CDC pipeline | Add test matching on CDC error variants | Error type not validated | Correctness | Small | Phase 2 fix |
| 5 | P2 | Testing | `krishiv-proto` (wire) | No test for `TraceContext` round-trip | Add test after proto field addition | Trace data loss not covered | Reliability | Small | Phase 1 proto fix |
| 5 | P2 | Testing | `krishiv-proto` (wire) | No test for `watermark_ms` round-trip | Add test after proto field addition | Watermark data loss not covered | Correctness | Small | Phase 1 proto fix |
| 5 | P2 | Testing | `krishiv-proto` (wire) | No test for `requires_reattach` round-trip | Add test after proto field addition | Re-attach signal loss not covered | Correctness | Small | Phase 1 proto fix |
| 5 | P3 | Testing | `krishiv-exec` (interval join) | No test for unbounded buffer memory growth | Add test with high-cardinality keys measuring memory | Memory exhaustion risk not covered | Scalability | Medium | Phase 4 fix |
| 5 | P3 | Testing | `krishiv-exec` (CEP) | No test for unbounded per-key state growth | Add test with many unique keys measuring memory | Memory exhaustion risk not covered | Scalability | Medium | Phase 4 fix |
| 5 | P3 | Testing | `krishiv-connectors` (feature store) | No test for unbounded `live` vec growth | Add test measuring memory under continuous writes | Memory exhaustion risk not covered | Scalability | Medium | Phase 4 fix |
| 5 | P3 | Testing | Workspace-wide | No observability/monitoring validation tests | Add tests for Prometheus metric correctness and completeness | Metrics output not validated | Observability | Large | None |
| 5 | P3 | Testing | Workspace-wide | No regression test suite for bugs fixed in production stabilization waves 0-4 | Create regression test matrix covering all wave fixes | Prior fixes could regress without detection | Reliability | Large | None |

---

## Roadmap Coverage Check

### Critical Findings

- [x] `krishiv-exec` sum/count overflow (Phase 1)
- [x] `krishiv-exec` session window non-monotonic watermark (Phase 1)
- [x] `krishiv-exec` left-outer temporal join drops rows (Phase 1)
- [x] `krishiv-exec` key parse `unwrap_or(0)` data corruption (Phase 1)
- [x] `krishiv-proto` panic in wire serialization (Phase 1)
- [x] `krishiv-proto` TraceContext silently dropped (Phase 1)
- [x] `krishiv-connectors` ParquetSource eager full-file read OOM (Phase 1)
- [x] `krishiv-connectors` S3Source eager full-object download OOM (Phase 1)
- [x] `krishiv-connectors` TransactionalKafkaRegistry mutex unwrap (Phase 1)
- [x] `krishiv-connectors` ParquetSink expect in write_batch (Phase 1)
- [x] `krishiv-connectors` two_phase.rs handle overflow (Phase 1)
- [x] `krishiv-runtime` standalone do_action missing size cap (Phase 1)
- [x] `krishiv-runtime` streaming loop no per-batch timeout (Phase 1)

### High Findings

- [x] `krishiv-scheduler` triple-lock contention in heartbeat tick (Phase 2)
- [x] `krishiv-common` blocking.rs dead module (Phase 3)
- [x] `krishiv-common` arrow.rs unused public functions (Phase 3)
- [x] `krishiv-common` arrow.rs test helpers in public API (Phase 3)
- [x] `krishiv-exec` join.rs broadcast batch clone (Phase 4)
- [x] `krishiv-exec` interval_join per-match clones (Phase 4)

### Medium Findings

- [x] `krishiv-exec` session state restore unwrap_or(0) (Phase 1)
- [x] `krishiv-common` async_util expect on Tokio runtime (Phase 1)
- [x] `krishiv-common` arrow.rs unwrap in public helpers (Phase 1)
- [x] `krishiv-proto` debug_assert in release constructor (Phase 1)
- [x] `krishiv-proto` watermark_ms dropped on wire (Phase 1)
- [x] `krishiv-proto` requires_reattach dropped on wire (Phase 1)
- [x] `krishiv-common` unix_now_ms silent 0 on clock error (Phase 1)
- [x] `krishiv-scheduler` busy-wait spin in flush (Phase 1)
- [x] `krishiv-common` silent DevLocal fallback on invalid env (Phase 1)
- [x] `krishiv-scheduler` etcd_metadata blocking_lock panic (Phase 1)
- [x] `krishiv-runtime` flight_client blocking_write panic (Phase 1)
- [x] `krishiv-runtime` silent empty URL normalization (Phase 1)
- [x] `krishiv-flight-sql` run_blocking thread panic propagation (Phase 1)
- [x] `krishiv-ui` metrics endpoint mutex unwrap (Phase 1)
- [x] `krishiv-python` stream.rs 5 mutex unwraps (Phase 1)
- [x] `krishiv-api` session.rs 2 expects (Phase 1)
- [x] `krishiv-governance` 2 expects on reqwest client (Phase 1)
- [x] `krishiv-connectors` CDC returns String not ConnectorError (Phase 1)
- [x] `krishiv-scheduler` dual-state drift (Phase 2)
- [x] `krishiv-runtime` two do_action paths (Phase 2)
- [x] `krishiv-connectors` IoStr migration alias (Phase 2)
- [x] `krishiv-common` inconsistent validator approaches (Phase 2)
- [x] `krishiv-common` inconsistent error patterns (Phase 2)
- [x] `krishiv-exec` 3x duplicated window stream impls (Phase 2)
- [x] `krishiv-proto` raw string IDs where typed expected (Phase 2)
- [x] `krishiv-runtime` wrong error variant mapping (Phase 2)
- [x] `krishiv-connectors` blocking I/O in async-callable methods (Phase 2)
- [x] `krishiv-connectors` CDC nested block_in_place+block_on (Phase 2)
- [x] `krishiv-scheduler` write lock during batch launches (Phase 2)
- [x] `krishiv-scheduler` 2-second drain window heuristic (Phase 2)
- [x] `krishiv-common` allow_anonymous_http_override bypass (Phase 2)
- [x] `krishiv-connectors` duplicate ConnectorConfig (Phase 2)
- [x] `krishiv-exec` operator_runtime spec cloning (Phase 4)
- [x] `krishiv-exec` window output batch allocation (Phase 4)
- [x] `krishiv-exec` sliding window key cloning (Phase 4)
- [x] `krishiv-exec` unbounded window state (Phase 4)
- [x] `krishiv-exec` unbounded interval join buffers (Phase 4)
- [x] `krishiv-exec` unbounded CEP state (Phase 4)
- [x] `krishiv-connectors` S3Sink no byte limit (Phase 4)
- [x] `krishiv-connectors` feature_store eager fragment load (Phase 4)
- [x] `krishiv-connectors` feature_store unbounded live vec (Phase 4)

### Low Findings

- [x] `krishiv-common` redundant forbid(unsafe_code) attributes (Phase 3)
- [x] `krishiv-common` hash.rs expect on infallible conversion (Phase 3)
- [x] `krishiv-scheduler` self-shadowing let mut body (Phase 3)
- [x] `krishiv-scheduler` commented-out test import (Phase 3)
- [x] `krishiv-common` chaos.rs SeqCst ordering (Phase 3)
- [x] `krishiv-proto` deprecated array zip truncation (Phase 3)
- [x] `krishiv-proto` zero-stats silently dropped (Phase 3)
- [x] `krishiv-common` env guards not cached (Phase 3)
- [x] `krishiv-proto` saturating ID counters (Phase 1)
- [x] `krishiv-connectors` ParquetSource duplicate reset methods (Phase 3)
- [x] `krishiv-connectors` quality.rs clock unwrap_or_default (Phase 3)
- [x] `krishiv-scheduler` non-fail-closed metadata writes (Phase 3)
- [x] `krishiv-common` unix_now_ms_checked i64::MAX clamp (Phase 3)
- [x] `krishiv-exec` join.rs unused _num_right_cols (Phase 3)
- [x] `krishiv-exec` temporal_join.rs unused _num_stream_cols (Phase 3)
- [x] `krishiv-exec` temporal_join.rs format! on static string (Phase 3)
- [x] `krishiv-connectors` two_phase.rs rename POSIX-only (Phase 3)
- [x] `krishiv-connectors` two_phase_parquet_s3.rs hard_link cross-fs (Phase 3)

### Architecture Findings

- [x] Triple-lock contention in scheduler heartbeat (Phase 2)
- [x] Dual-state drift between coordinator layers (Phase 2)
- [x] Two do_action code paths with different safety (Phase 2)
- [x] IoStr migration alias in connectors (Phase 2)
- [x] Inconsistent validator approaches in common (Phase 2)
- [x] Inconsistent error patterns in common (Phase 2)
- [x] Duplicated window stream implementations (Phase 2)
- [x] Blocking I/O in async-callable connectors (Phase 2)
- [x] allow_anonymous_http_override security bypass (Phase 2)
- [x] Duplicate ConnectorConfig in connectors (Phase 2)

### Distributed Systems Findings

- [x] Dual-state drift between coordinator layers (Phase 2)
- [x] 2-second drain window heuristic (Phase 2)
- [x] TraceContext dropped on all wire types (Phase 1)
- [x] watermark_ms dropped on wire (Phase 1)
- [x] requires_reattach dropped on wire (Phase 1)
- [x] Saturating ID counters (Phase 1)
- [x] etcd_metadata blocking_lock panic (Phase 1)
- [x] No split-brain fencing tests (Phase 5)
- [x] No coordinator crash recovery tests (Phase 5)

### Testing Findings

- [x] No property-based tests for window aggregation (Phase 5)
- [x] No fuzz tests for window spec validation (Phase 5)
- [x] No fuzz tests for wire deserialization (Phase 5)
- [x] No property-based round-trip tests for Kafka offsets (Phase 5)
- [x] No distributed recovery tests (Phase 5)
- [x] No split-brain fencing tests (Phase 5)
- [x] No regression tests for Phase 1 bugs (Phase 5)
- [x] No regression tests for production stabilization waves (Phase 5)
- [x] No observability/monitoring validation tests (Phase 5)
- [x] No memory growth tests for unbounded state (Phase 5)

### Cleanup Findings

- [x] 18 `#[allow(dead_code)]` annotations across 9 crates (Phase 3)
- [x] Dead blocking.rs module in krishiv-common (Phase 3)
- [x] Dead proto types without wire conversions (Phase 3)
- [x] Unused barrier.proto (Phase 3)
- [x] Redundant forbid(unsafe_code) attributes (Phase 3)
- [x] Commented-out code blocks (Phase 3)
- [x] Trivial delegation function (Phase 3)
- [x] Unnecessary format! on static strings (Phase 3)

---

## Exclusions

No findings are intentionally excluded from this roadmap. Every finding from
the audit appears in at least one phase. Items marked as "informational" in
the source audits (e.g., `plan_execution_kind` trivial delegation) are included
in Phase 3 as cleanup items.
