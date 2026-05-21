# R16 Advanced Stateful Streaming & Exactly-Once Implementation Tracker

## Goal

Deliver Flink-competitive stateful streaming capabilities: full gRPC checkpoint barrier transport replacing the in-process simulation from R6, complex event processing (CEP) pattern matching, temporal and interval joins, exactly-once delivery certified across all five connector pairs, state rescaling from N to M partitions using key-group redistribution, late-data side outputs, and RocksDB incremental checkpointing. This release completes the streaming execution model that R5 and R6 laid the foundation for.

## Scope

In scope:

- Full gRPC checkpoint barrier transport with barrier injection at sources, forwarding through operators, alignment at multi-input operators, and acknowledgment to coordinator.
- CEP pattern matching: `Pattern().begin().followed_by().where().within()` using a per-key state machine (simple NFA, covering 80% of use cases).
- Temporal joins: stream-table as-of event_time joins, and stream-stream interval joins.
- Exactly-once certification for all five connector pairs: Kafka→Iceberg (verify R14), Kafka→Kafka (Kafka transactions), Kafka→Parquet/S3 (2PC), S3/Parquet→Iceberg, S3/Parquet→Kafka.
- State rescaling: restore an N-partition checkpoint into an M-partition deployment using key-group redistribution.
- Late-data side outputs: `with_side_output("late", lateness_threshold)` routing late records to a named output.
- RocksDB incremental checkpointing: upload only changed SSTables per epoch.
- State schema migration: `@ks.state_migration(from_version=1, to_version=2)` decorator.
- `krishiv-cep` new crate for CEP pattern matching engine.

Out of scope:

- Full NFA with quantifiers (one+, zero-or-more) and complex pattern combinators — deferred to R17/R18.
- CEP over unkeyed streams (CEP is per-key only in R16).
- Cross-partition CEP pattern matching.
- Exactly-once for connector pairs beyond the five certified pairs.
- State rescaling for CEP state (deferred — CEP state is transient per key, rescaling is a key redistribution).
- Flink-compatible savepoint format import.

## Dependencies

- R5: keyed state, watermarks, tumbling windows, and barrier simulation are the baseline this release replaces and extends.
- R6: checkpoint/savepoint architecture, RocksDB state backend trait, and rescaling model (ADR-R6 rescaling decision is the direct predecessor).
- R12: certified Kafka connector providing `rdkafka` as the transactional producer/consumer library.
- R13: coordinator gRPC extension points needed for barrier coordination across executors.
- R14: Iceberg sink with snapshot-based commit semantics, used in the Kafka→Iceberg exactly-once certification.
- R15: no direct dependency; may run in parallel.

## Architectural Decisions Required

### ADR-R16.1: gRPC Checkpoint Barrier Message Format

**Problem**: The checkpoint barrier must flow through every operator in every executor process via gRPC. The message format must carry enough information for sources to inject barriers, operators to forward and align them, and coordinators to track acknowledgments. This must be defined before any operator-level barrier code is written.

**Options**:
- A: Embed barrier messages in the existing `ExecutorHeartbeat` proto — minimal new proto surface, but barriers would be delayed by heartbeat intervals (250ms–1s), which is unacceptable for latency-sensitive streaming jobs.
- B: Define a dedicated `CheckpointBarrier` proto message sent over a separate bidirectional streaming RPC between coordinator and each executor — low latency, clean separation from heartbeat traffic.
- C: Piggyback barriers in the data channel proto alongside `RecordBatch` messages — avoids a separate RPC, but couples data and control plane.

**Recommendation**: Option B. Barriers are control-plane messages and must not share bandwidth or latency characteristics with data or heartbeat channels. A dedicated `BarrierService` bidirectional RPC allows the coordinator to inject barriers and receive acknowledgments independently. The barrier message must carry: `epoch`, `job_id`, `checkpoint_id`, `barrier_kind` (checkpoint or savepoint), and `timestamp_ms`. This ADR must be resolved and proto merged before Sprint 1 subtask S1.2.

**Risk if deferred**: Any operator-level barrier code written before the proto is finalized will require a breaking refactor. Sprint 1 must deliver the proto definition as its first artifact.

---

### ADR-R16.2: CEP Engine Design

**Problem**: Complex event processing requires per-key state tracking of partial pattern matches, time-based expiry, and sequential event matching. A full NFA supporting quantifiers is a 3–4 month investment. A simple state machine covering `begin().followed_by().where().within()` covers 80% of use cases in 4 weeks.

**Options**:
- A: Implement a full NFA from scratch in Rust, supporting quantifiers (one+, zero-or-more), negation (not_followed_by), and branching patterns — full control, 3–4 months.
- B: Port Flink's CEP NFA implementation to Rust — faster, carries Apache 2.0 license obligations and significant translation complexity.
- C: Implement a simple sequential state machine covering `begin().followed_by().where().within()` only — 80% of real-world use cases, 4 weeks, promotes to full NFA in R17/R18.

**Recommendation**: Option C for R16. The `krishiv-cep` crate exposes a `Pattern` builder that compiles to a `SequentialPatternMatcher` per key. The matcher tracks a linear chain of `PatternStage { predicate, max_gap_ms }` states in keyed RocksDB state, advancing on matching events and expiring on `within()` timeout. Unsupported pattern combinators (quantifiers, negation) return a compile-time error with a message pointing to R17/R18.

**Risk if deferred**: Users migrating from Flink with complex CEP patterns will be blocked. The compatibility matrix must clearly document which pattern combinators are supported in R16.

---

### ADR-R16.3: State Rescaling Algorithm

**Problem**: When job parallelism changes from N to M partitions, keyed state stored in RocksDB must be redistributed. The algorithm determines how keys map to new task slots during restore.

**Options**:
- A: Consistent hashing — fast restore, but produces unbalanced shard sizes for small M values.
- B: Key-group hashing (Flink approach) — keys map to a fixed number of key groups (e.g., 32768), and key groups are redistributed evenly among M tasks. Balanced, correct for any N→M change, but requires the `StateBackend` trait to expose `key_group_range()`.
- C: Broadcast all state to all tasks and re-filter on restore — simple implementation, O(N×M) data movement, unacceptable for large state.

**Recommendation**: Option B. Key-group hashing is the correct algorithm for production rescaling. All state backends (`RocksDbStateBackend`, `MemoryStateBackend`) must implement `key_group_range() -> RangeInclusive<u16>` from the start of Sprint 4. Checkpoints must store state partitioned by key group, not by task slot. This ADR must be decided and the `StateBackend` trait updated before Sprint 4 begins, as it affects how Sprint 1–3 write state.

**Risk if deferred**: If state is checkpointed by task slot rather than key group, a full re-implementation of the checkpoint format is required to enable rescaling. The `StateBackend` trait signature must be locked in Sprint 1.

---

### ADR-R16.4: Kafka Exactly-Once — Transactional Producer Strategy

**Problem**: Exactly-once Kafka→Kafka delivery requires a transactional Kafka producer on the sink side. The transaction ID must be deterministic across restarts to allow recovery from zombie transactions.

**Options**:
- A: Use a random transaction ID per execution — simple, but zombie transactions from failed executors cannot be fenced, breaking exactly-once on recovery.
- B: Derive the transaction ID deterministically from `job_id + epoch + partition_id` — allows the coordinator to fence zombie transactions on recovery by completing or aborting them before restarting the epoch.
- C: Use Kafka's idempotent producer only (no transactions) — protects against broker-level duplicates but not against executor crash-restart duplicates.

**Recommendation**: Option B. Transaction IDs are `{job_id}/{partition_id}/{epoch}`. On recovery, the coordinator issues a `fence_zombie_transactions` phase before beginning the next epoch: it attempts to commit or abort any open transactions from the previous epoch. The `rdkafka` `init_transactions()` + `begin_transaction()` + `commit_transaction()` / `abort_transaction()` API supports this. The Kafka source must set `isolation.level=read_committed`.

**Risk if deferred**: Any Kafka→Kafka exactly-once test written without deterministic transaction IDs will pass in happy-path tests but fail on recovery scenarios. The transaction ID scheme must be agreed before Sprint 5.

---

## Sprint 1 — gRPC Checkpoint Barrier Transport

### S1.1 Barrier Proto Definition
- [ ] Add `CheckpointBarrier` message to `krishiv-proto`: fields `epoch: u64`, `job_id: String`, `checkpoint_id: String`, `barrier_kind: BarrierKind` (enum: Checkpoint, Savepoint), `timestamp_ms: i64`.
- [ ] Add `BarrierAck` message: fields `epoch: u64`, `job_id: String`, `task_id: String`, `state_handle: Option<StateHandle>`.
- [ ] Add `BarrierService` gRPC service with `BarrierStream(stream CheckpointBarrier) returns (stream BarrierAck)` bidirectional RPC.
- [ ] Add `StateHandle` message: fields `backend_kind: String`, `checkpoint_uri: String`, `key_group_range_start: u32`, `key_group_range_end: u32`.
- [ ] Regenerate proto bindings: `cargo build -p krishiv-proto`.
- [ ] Add `key_group_range: Option<KeyGroupRange>` to `StateBackend` trait in `krishiv-state` (required by ADR-R16.3).

**Validation**: `cargo check -p krishiv-proto -p krishiv-state` clean.

### S1.2 Barrier Injection at Sources
- [ ] Implement `BarrierInjector` in `krishiv-executor`: receives `CheckpointBarrier` from coordinator via `BarrierService` RPC.
- [ ] Implement barrier injection into source operator output channels: after all in-flight records at the barrier epoch are emitted, inject `OperatorMessage::Barrier { epoch }` into each downstream channel.
- [ ] Implement source-side epoch tracking: source must not emit records for epoch N+1 until the barrier for epoch N has been forwarded.
- [ ] Write unit test: source emits records, barrier is injected, downstream receives barrier after all pre-barrier records.

**Validation**: `cargo test -p krishiv-executor -- barrier_injection`

### S1.3 Barrier Forwarding and Alignment
- [ ] Implement barrier forwarding in single-input operators: on receiving `OperatorMessage::Barrier`, pass it downstream before processing any further data messages.
- [ ] Implement barrier alignment in multi-input operators (join, union): buffer records from faster inputs until the barrier arrives on all inputs, then forward the barrier and release buffered records.
- [ ] Implement alignment timeout: if a barrier has not arrived on all inputs within `barrier_alignment_timeout_ms`, surface a `CheckpointAlignmentTimeout` error to the coordinator.
- [ ] Write unit test: two-input operator receives barrier on input A, buffers A records, receives barrier on input B, forwards barrier, releases buffered records.

**Validation**: `cargo test -p krishiv-executor -- barrier_alignment`

### S1.4 Barrier Acknowledgment to Coordinator
- [ ] Implement `BarrierAck` sender in executor: after all operators have forwarded the barrier and state has been snapshotted, send `BarrierAck` with `state_handle` back to coordinator.
- [ ] Implement barrier tracking in coordinator: `CheckpointTracker { epoch, expected_acks: HashSet<TaskId>, received_acks: HashSet<TaskId> }`.
- [ ] Implement checkpoint completion: when all `expected_acks` are received, coordinator marks checkpoint complete and writes `CheckpointMetadata` to the checkpoint store.
- [ ] Implement checkpoint timeout: coordinator marks checkpoint failed if not all acks received within `checkpoint_timeout_ms`.
- [ ] Write integration test: single-executor job completes a checkpoint cycle end-to-end via gRPC barrier transport.

**Validation**: `cargo test -p krishiv-scheduler -- checkpoint_barrier_integration`

---

## Sprint 2 — CEP Pattern Matching Engine

### S2.1 krishiv-cep Crate and Pattern Builder
- [ ] Create `crates/krishiv-cep/` crate.
- [ ] Define `Pattern` builder: `Pattern::begin("start")`, `.followed_by("next")`, `.where(|event| ...)`, `.within(Duration)`, `.times(n)` (exact count only).
- [ ] Define `PatternStage { name: String, predicate: Box<dyn Fn(&RecordBatch) -> BooleanArray>, max_gap_ms: Option<u64> }`.
- [ ] Define `CompiledPattern { stages: Vec<PatternStage>, window_ms: u64 }`.
- [ ] Return `CepCompileError::UnsupportedCombinator` for quantifiers (one+, zero-or-more) and negation patterns.
- [ ] Write unit tests for pattern builder: valid linear patterns compile; unsupported combinators return error.

**Validation**: `cargo test -p krishiv-cep -- pattern_builder`

### S2.2 Per-Key Sequential Pattern Matcher
- [ ] Implement `SequentialPatternMatcher`: per-key, per-pattern-stage state machine stored in `krishiv-state` keyed state.
- [ ] Implement state: `CepKeyState { current_stage: usize, partial_matches: Vec<PartialMatch>, last_event_ms: i64 }`.
- [ ] Implement `process_event(key, event_batch, event_time_ms)`: advance matching state, expire timed-out partials, emit complete matches.
- [ ] Implement `PartialMatch { stage_index: usize, captured_events: Vec<RecordBatch>, start_time_ms: i64 }`.
- [ ] Implement `within()` expiry: partial matches older than `window_ms` are discarded from `CepKeyState`.
- [ ] Write unit tests: two-stage pattern matches correctly, expired partials are discarded, completed matches are emitted.

**Validation**: `cargo test -p krishiv-cep -- sequential_matcher`

### S2.3 CEP Operator Integration
- [ ] Implement `CepOperator` physical operator in `krishiv-exec`: wraps `SequentialPatternMatcher`, keyed by a configurable key column.
- [ ] Integrate `CepOperator` with `OperatorQueue` barrier protocol (barrier passes through without affecting pattern state).
- [ ] Add `cep_pattern(pattern: CompiledPattern, key_column: &str)` to the streaming DataFrame API in `krishiv-api`.
- [ ] Write integration test: streaming job with two-stage CEP pattern, correct matches emitted, barrier checkpoint survives pattern restart.

**Validation**: `cargo test -p krishiv-exec -- cep_operator`; `cargo test -p krishiv-api -- cep_integration`

---

## Sprint 3 — Temporal Joins & Interval Joins

### S3.1 Stream-Table Temporal Join (As-Of)
- [ ] Define `TemporalJoinSpec { stream_time_col: String, table_version_col: String, join_keys: Vec<String> }` in `krishiv-plan`.
- [ ] Implement `TemporalJoinOperator` in `krishiv-exec`: for each stream record, look up the table version valid at `stream_time_col` using a versioned lookup into keyed state.
- [ ] Implement versioned table state: table updates are ingested as a second stream; the operator stores `BTreeMap<i64, RecordBatch>` per key (keyed by version timestamp), retaining only versions within a configurable lookback window.
- [ ] Implement join semantics: emit stream record joined with the latest table version where `table_version_col <= stream_time_col`.
- [ ] Add `join_temporal(table_stream, spec)` to streaming DataFrame API.
- [ ] Write unit tests: stream record matches correct table version; version before table start returns null (left join) or drops record (inner join).

**Validation**: `cargo test -p krishiv-exec -- temporal_join`

### S3.2 Stream-Stream Interval Join
- [ ] Define `IntervalJoinSpec { left_time_col: String, right_time_col: String, lower_bound_ms: i64, upper_bound_ms: i64, join_keys: Vec<String> }` in `krishiv-plan`.
- [ ] Implement `IntervalJoinOperator` in `krishiv-exec`: buffer records from both streams in per-key state within `[event_time - upper_bound_ms, event_time + upper_bound_ms]`.
- [ ] Implement matching: on receiving a record from either side, probe buffered records from the other side for interval overlap, emit matches.
- [ ] Implement state expiry: records older than `max(lower_bound_ms, upper_bound_ms)` behind the current watermark are evicted.
- [ ] Add `join_interval(right_stream, spec)` to streaming DataFrame API.
- [ ] Write unit tests: overlapping interval emits join, non-overlapping drops, evicted records do not match late arrivals.

**Validation**: `cargo test -p krishiv-exec -- interval_join`

### S3.3 Late Data Side Outputs
- [ ] Define `SideOutput { name: String, lateness_threshold_ms: u64 }` in `krishiv-plan`.
- [ ] Implement late-data routing in watermark-advancing operators: records with `event_time < watermark - lateness_threshold_ms` are emitted to the named side output channel instead of the main output.
- [ ] Implement `with_side_output("late", lateness_ms)` on streaming DataFrame API.
- [ ] Implement `get_side_output("late")` to retrieve the side output as a separate stream.
- [ ] Write unit tests: on-time record goes to main output; late record (beyond threshold) goes to side output.

**Validation**: `cargo test -p krishiv-exec -- side_output`

---

## Sprint 4 — State Rescaling & Key-Group Redistribution

### S4.1 Key-Group State Backend
- [ ] Add `fn key_group_range(&self) -> RangeInclusive<u16>` to `StateBackend` trait in `krishiv-state`.
- [ ] Update `RocksDbStateBackend`: store all state keys prefixed with their key group (`key_group(key) = hash(key) % NUM_KEY_GROUPS`).
- [ ] Update `MemoryStateBackend`: implement `key_group_range()` returning the full range (single-node).
- [ ] Define `NUM_KEY_GROUPS: u16 = 32768` as a workspace constant.
- [ ] Update checkpoint writer: partition state into key-group ranges when writing `StateHandle` to checkpoint metadata.
- [ ] Write unit tests: keys are assigned to correct key groups; `RocksDbStateBackend` stores and retrieves with key-group prefix.

**Validation**: `cargo test -p krishiv-state -- key_group`

### S4.2 Checkpoint Restore with Rescaling
- [ ] Implement `KeyGroupRescaler` in `krishiv-checkpoint`: given an N-task checkpoint and an M-task restore request, compute the mapping of key groups to new task slots.
- [ ] Implement mapping algorithm: divide `[0, 32768)` evenly among M tasks; map each key group to the task slot owning its range.
- [ ] Implement state redistribution on restore: `RocksDbStateBackend::restore_from_checkpoint(handles, key_group_range)` downloads only the SSTable files containing key groups in range.
- [ ] Integrate rescaling into coordinator `RestoreJob` RPC: accept optional `new_parallelism: u32` parameter; if provided, compute new key-group assignment and distribute handles accordingly.
- [ ] Write unit tests: 4→2 rescaling maps key groups correctly; 2→4 rescaling maps key groups correctly; restored state backend contains only keys belonging to its range.

**Validation**: `cargo test -p krishiv-checkpoint -- rescaling`

### S4.3 State Schema Migration
- [ ] Define `StateMigrationFn = Box<dyn Fn(OldStateBytes) -> NewStateBytes + Send + Sync>` in `krishiv-state`.
- [ ] Implement `StateMigrationRegistry { migrations: BTreeMap<(u32, u32), StateMigrationFn> }` — keyed by `(from_version, to_version)`.
- [ ] Implement chained migration: if migrating from version 1 to 3, apply 1→2 then 2→3.
- [ ] Implement migration on restore: `RocksDbStateBackend::restore_from_checkpoint()` checks stored schema version against current and applies registered migrations.
- [ ] Add Python decorator `@ks.state_migration(from_version=1, to_version=2)` in `krishiv-python` that registers a migration function with the active session's `StateMigrationRegistry`.
- [ ] Write unit tests: migration applied on restore; chained migration; missing migration returns `StateMigrationError`.

**Validation**: `cargo test -p krishiv-state -- state_migration`

---

## Sprint 5 — Exactly-Once Certification Matrix

### S5.1 Kafka → Kafka Exactly-Once (Kafka Transactions)
- [ ] Implement `TransactionalKafkaSink` in `krishiv-connectors` using `rdkafka` transactional producer.
- [ ] Derive deterministic transaction ID: `{job_id}/{partition_id}/{epoch}` (per ADR-R16.4).
- [ ] Implement sink commit protocol: `begin_transaction()` at epoch start, `commit_transaction()` on barrier ack, `abort_transaction()` on checkpoint failure.
- [ ] Implement zombie fencing on recovery: coordinator calls `abort_or_commit_open_transaction()` for the previous epoch before starting a new epoch.
- [ ] Implement Kafka source configuration: `isolation.level=read_committed` enforced on all consuming sources in exactly-once sessions.
- [ ] Write certification test: two executors, checkpoint failure mid-epoch, recovery, verify no duplicates or missing records in output topic.

**Validation**: `cargo test -p krishiv-connectors -- kafka_exactly_once`

### S5.2 Kafka → Parquet/S3 Exactly-Once (2PC)
- [ ] Implement `TwoPhaseParquetSink` in `krishiv-connectors`: writes staged Parquet files to a `_staging/` prefix on S3, atomically moves to final path on `commit()`.
- [ ] Implement 2PC protocol: `prepare(epoch)` — write staged files; `commit(epoch)` — atomic rename to final path; `abort(epoch)` — delete staged files.
- [ ] Implement recovery: on restart, coordinator checks for staged files from the previous epoch and either commits or aborts them before beginning the new epoch.
- [ ] Write certification test: crash between `prepare` and `commit`, recovery commits staged files, no data loss or duplication.

**Validation**: `cargo test -p krishiv-connectors -- s3_2pc_exactly_once`

### S5.3 S3/Parquet → Iceberg and S3/Parquet → Kafka Exactly-Once
- [ ] Certify S3/Parquet → Iceberg: source checkpoints S3 file offset (byte range or file list); Iceberg sink uses R14 snapshot commit protocol. Write certification test.
- [ ] Certify S3/Parquet → Kafka: source checkpoints S3 file offset; Kafka sink uses `TransactionalKafkaSink` from S5.1. Write certification test.
- [ ] Certify Kafka → Iceberg: verify R14 certification still passes with R16 gRPC barrier transport (regression test).
- [ ] Update `docs/reference/exactly-once-matrix.md` with all five certified connector pairs and their guarantee conditions.

**Validation**: `cargo test -p krishiv-connectors -- exactly_once_certification`

---

## Sprint 6 — RocksDB Incremental Checkpointing & Late Data

### S6.1 RocksDB Incremental Checkpointing
- [ ] Implement `IncrementalCheckpointWriter` in `krishiv-state`: uses RocksDB's `Checkpoint::export_column_family()` to identify changed SSTables since the last checkpoint.
- [ ] Implement SSTable manifest tracking: store `SstManifest { epoch: u64, sst_files: Vec<SstFileRef> }` per checkpoint; on next checkpoint, diff against previous manifest and upload only new/changed files.
- [ ] Implement checkpoint GC: after N successful checkpoints, delete SSTable files not referenced by any retained checkpoint.
- [ ] Implement full checkpoint fallback: if the previous checkpoint manifest is unavailable, fall back to full upload.
- [ ] Write unit tests: incremental checkpoint uploads only changed SSTables; GC removes unreferenced files; restore from incremental checkpoint is correct.

**Validation**: `cargo test -p krishiv-state -- incremental_checkpoint`

### S6.2 Watermark Propagation and Late Data E2E
- [ ] Audit watermark propagation through all R16 operators (temporal join, interval join, CEP operator, side output): verify watermarks advance correctly and do not regress.
- [ ] Implement watermark hold for multi-input operators: output watermark is `min(input_watermarks)` until all inputs have advanced.
- [ ] Write end-to-end test: streaming job with CEP + interval join + late-data side output, verify watermark advances correctly, late records reach side output, on-time records reach main output.
- [ ] Add `krishiv stream jobs --show-watermarks` CLI output for live watermark inspection.

**Validation**: `cargo test -p krishiv-exec -- watermark_propagation_e2e`

### S6.3 Acceptance Validation
- [ ] Run full workspace test suite.
- [ ] Run `cargo clippy --workspace -- -D warnings`.
- [ ] Verify all five exactly-once certification tests pass.
- [ ] Verify 4→2 and 2→4 rescaling integration tests pass.
- [ ] Verify CEP two-stage pattern matches correctly after checkpoint and restore.
- [ ] Verify incremental checkpoint uploads fewer bytes than full checkpoint for an unchanged state workload.
- [ ] Update `docs/reference/exactly-once-matrix.md` and `docs/reference/streaming-compat-matrix.md`.

**Validation**: `cargo test --workspace`; `cargo clippy --workspace -- -D warnings` clean.

---

## Acceptance Gate

- [ ] gRPC checkpoint barrier transport delivers barriers end-to-end: source → operators → sink → coordinator acknowledgment, verified by integration test.
- [ ] Barrier alignment at multi-input operators is correct: faster input records are buffered until barrier arrives on all inputs.
- [ ] CEP `Pattern().begin().followed_by().where().within()` matches correctly across checkpoint/restore boundaries.
- [ ] Unsupported CEP combinators return `CepCompileError::UnsupportedCombinator` at compile time.
- [ ] Temporal as-of join emits stream records joined with the correct table version.
- [ ] Interval join correctly matches events within the defined time bounds and evicts expired state.
- [ ] Late records (beyond watermark + threshold) are routed to the named side output.
- [ ] State rescaling: restoring a 4-partition checkpoint into a 2-partition deployment produces correct keyed state with no loss or duplication.
- [ ] All five exactly-once connector pairs pass their certification tests: Kafka→Iceberg, Kafka→Kafka, Kafka→Parquet/S3, S3/Parquet→Iceberg, S3/Parquet→Kafka.
- [ ] Incremental RocksDB checkpoints upload only changed SSTables (verified by SSTable upload count assertion).
- [ ] State schema migration is applied on restore when schema version has advanced.
- [ ] `cargo test --workspace` passes; `cargo clippy --workspace -- -D warnings` clean.
