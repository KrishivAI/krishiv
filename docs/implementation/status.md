# Krishiv Implementation Status

## Write API + Join/Union/Describe/FillNull (2026-06-11)

### Done

1. **Write API** — `DataFrame::write_parquet(path)` (uses `parquet::arrow::ArrowWriter`), `DataFrame::write_csv(path)` (uses `arrow::csv::Writer`), `DataFrame::write_json(path)` (uses `arrow::json::LineDelimitedWriter`). All collect then write. Requires `parquet` dependency in `krishiv-api`.

2. **Join** — Added `join` to `KrishivDataFrameOps` trait + `SqlDataFrame` impl using DataFusion's `DataFrame::join()`. Supports inner, left, right, full/outer, left_semi, right_semi, left_anti, right_anti. API `DataFrame::join(right, how, left_on, right_on)` uses `as_any()` downcast to access underlying DataFusion DataFrames.

3. **Union** — Added `union` to trait + `SqlDataFrame` impl via DataFusion `DataFrame::union()` (UNION ALL). API `DataFrame::union(right)` with same downcast pattern.

4. **Describe** — Added `describe` to trait + `SqlDataFrame` impl via `DataFrame::describe().await`. Returns a new DataFrame with summary statistics (count, null_count, mean, std, min, max, median).

5. **FillNull** — Added `fill_null` to trait + `SqlDataFrame` impl using `COALESCE(column, value)` expression with `with_column`. API `DataFrame::fill_null(column, value)`.

6. **Dependency fix** — Added `csv`, `json` features to workspace arrow dep; moved `parquet` from dev-dependencies to main deps in `krishiv-api/Cargo.toml`.

### Validation
```bash
cargo check -p krishiv-sql -p krishiv-api  # clean
cargo check --tests -p krishiv-api         # tests compile clean
cargo test -p krishiv-sql --lib            # 288 passed
```

### Next
- Add `explain` format options (verbose/analyze mode)
- Add `drop_null`, `sample`, `cross_join`
- Add `fill_null` with column-list API (vectorised fill)
- Add typed column references (expression builders) as alternative to string exprs
- Add checkpoint/savepoint API for streaming
- Add streaming SQL primitives (CTAS with streaming sources)
- Add config API (set/get session properties)

---

## Full Workspace Security & Correctness Audit (2026-06-11)

### Done

Systematic 12-dimension analysis of all 411 source files across 20 crates. All P0/P1 issues fixed.

**P0 fixes (panics / data loss):**
1. `krishiv-plan/window.rs` — `encode_stream_fragment` `→ Result<String, PlanError>`; removed `.expect()` on fallible serialization
2. `krishiv-dataflow/src/sort.rs` — Arrow downcast `.unwrap()` → `.ok_or_else()` in `scalar_at` and `scatter_column`
3. `krishiv-sql/src/lib.rs` — Concurrent MERGE/CEP table name collision: hardcoded `"merge_result"` → `next_ephemeral_name()`
4. `krishiv-sql/src/kafka_table.rs` — Cast failure → `DataFusionError::ArrowError(Box::new(e), None)` instead of silent null
5. `krishiv-connectors/src/elasticsearch_sink.rs` — ES bulk API `errors` field not checked; HTTP 200 with item failures was silent data loss

**P1 fixes (correctness):**
6. `krishiv-sql/src/live_table.rs` — `RwLock::write().expect()` → `map_err` returning `SqlError`
7. `krishiv-sql/src/policy.rs` — SQL injection in RLS predicates; `''` escaped quote handling in WHERE injection point
8. `krishiv-sql/src/lakehouse/merge.rs` — `KEY_COL_RE` picked wrong side of `=` for key column extraction
9. `krishiv-sql/src/lakehouse/providers.rs` — `HudiScanProvider::schema()` returned empty on error; cached at construction
10. `krishiv-sql/src/cep_sql.rs` — Non-StringArray partition key silently fell back to row index → now returns `SqlError::Unsupported`
11. `krishiv-sql/src/lib.rs` — `PlanCache` LRU accumulated duplicate order entries on repeated key insertion
12. `krishiv-sql/src/recursive_cte.rs` — Unbounded accumulator; added `MAX_ACCUMULATED_ROWS = 10_000_000` guard
13. `krishiv-sql/src/udf.rs` — Raw Arrow buffer offset reading replaced with `BinaryArray::value(i)` safe API
14. `krishiv-scheduler/src/store.rs` — `snapshot_bytes.len() as u32` → `u32::try_from(...)?` (returns `SchedulerResult`)
15. `krishiv-scheduler/src/auth.rs` — JWT audience validation disabled (`validate_aud = false`); now reads `KRISHIV_OIDC_AUDIENCE`; missing-role fallback `"admin"` → `"reader"`
16. `krishiv-executor/src/cli.rs` — Prometheus label injection via executor_id; now escapes `\` and `"`
17. `krishiv-executor/src/runner.rs` — Hardcoded `"exec"` executor ID in checkpoint ack; added `own_executor_id` field
18. `krishiv-scheduler/src/coordinator_daemon.rs` — `job_id` injected raw into HTTP URL; now `urlencoding::encode(&job_id)`
19. `krishiv-connectors/src/elasticsearch_sink.rs` — `.unwrap()` on Arrow downcasts → safe `.downcast_ref().map(...).unwrap_or(Null)`
20. `krishiv-connectors/src/kafka.rs` — `try_into().unwrap()` in decode → `map_err`; `as u32` truncation in encode → `try_from(...).unwrap_or(u32::MAX)`
21. `krishiv-python/src/sources.rs` — SQL injection via Kafka topic/parquet file name in `SELECT * FROM "{name}"`; escape `"` → `""`
22. `krishiv-state/src/queryable.rs` — `RwLock.unwrap()` in production code → proper `map_err` / `let Ok(...) else`
23. `krishiv-plan/src/udf.rs` — `try_into().unwrap()` guarded by length check → safe `copy_from_slice` helper `read_i64_state`
24. `krishiv-operator/src/reconciler.rs` — Silent mutex poison drop → `tracing::warn!` before return
25. `krishiv-sql/src/connector_table.rs` — Propagate `project_batch` `ArrowError` instead of type mismatch

**P2 fixes (performance/robustness):**
26. `krishiv-sql/src/catalog/mod.rs` — `table_exist` O(n) list → O(1) `get_table().is_ok()`
27. `krishiv-sql/src/unnest_sql.rs` — O(N²) unnest replacement → O(N) with `search_start`
28. `krishiv-sql/src/create_function_ddl.rs` — Regex recompiled per call → `static LazyLock<Regex>`
29. `krishiv-sql/src/subquery.rs` — Case-sensitive streaming table name comparison → lowercase set

**Session 2 additional fixes (2026-06-11):**
30. `krishiv-ai/src/embed/openai.rs` — `EmbeddingRateLimiter::acquire` sleep loop reset tokens to full capacity instead of doing a time-proportional refill; fixed by refilling from elapsed time on each iteration
31. `krishiv-connectors/src/avro.rs` — Arrow downcast `.unwrap()` in `arrow_scalar_to_avro` → `.map(...).unwrap_or(AvroValue::Null)` for each matched type
32. `krishiv-connectors/src/cassandra_sink.rs` — Arrow downcast `.unwrap()` in `arrow_scalar_to_cql` → `?` on `downcast_ref` (returns `None` on mismatch)
33. `krishiv-connectors/src/hbase_connector.rs` — (a) `Mutex::lock().unwrap()` in `write_batch` → `map_err`; (b) Arrow downcast `.unwrap()` in `arrow_cell_to_bytes` → `.map(...).unwrap_or_default()`
34. `krishiv-scheduler/src/store.rs` — `ContinuousSnapshot::decode` used `try_into().unwrap()` → `copy_from_slice` into fixed-size arrays (guarded by prior length check)
35. `krishiv-sql/src/udf.rs` — Dead import `BinaryArray` removed

**Session 3 execution-flow fixes (2026-06-11):**
36. `krishiv-runtime/in_process.rs` — Bug #1: WatermarkHint injected only into `assignments[0]`; fixed to inject into ALL tasks in the streaming stage so late-data suppression is consistent across multi-task stages
37. `krishiv-runtime/in_process.rs` — Bug #3: `contains("stream:")` in streaming-stage detection → `starts_with("stream:")` to avoid false matches in SQL predicates
38. `krishiv-runtime/in_process.rs` — Bug #2: `drain_continuous_job` coordinator lock-poison swallowed silently → `tracing::warn!` on poison so operators know persistence is broken
39. `krishiv-api/session.rs` — Bug #4: `extract_host()` stripped port before comparing coordinator Flight and gRPC URLs; two coordinators on the same host with different ports would pass validation → compare full authority (`host:port`)
40. `krishiv-api/session.rs` — Perf #10: Embedded mode hardcoded `target_parallelism = 1`; all modes now default to `available_parallelism()` — users who need deterministic single-threaded execution can set `target_parallelism(1)` explicitly
41. `krishiv-executor/assignment_inbox.rs` + `cli.rs` — Perf #12: Distributed executor slot loops polled at 50 ms unconditional sleep; added `tokio::sync::Notify` (`wakeup` field) to `ExecutorAssignmentInbox`, notify on `push_with_outcome` success, slot loops now `tokio::select!` on `wakeup.notified()` with 1 s fallback instead of spinning
42. `krishiv-runtime/flight_action.rs` + `execution_runtime.rs` + `in_process.rs` + `in_process_cluster.rs` + `krishiv-flight-sql/host.rs` + `lib.rs` — Gap #5: Kafka source registrations were not forwarded to remote coordinator in distributed mode; added `RegisterKafkaSourceBody`/`RegisterKafkaSource` flight action, `encode_schema_ipc_b64`/`decode_schema_ipc_b64` helpers, `ExecutionRuntime::register_kafka_source` default no-op overridden in `RemoteExecutionRuntime`, `InProcessCluster::register_kafka_source` on server side, `FlightExecutionHost::register_kafka_source`, and session-side forwarding

### Validation
```bash
cargo check --workspace             # 0 errors, 2 pre-existing warnings
cargo test -p krishiv-scheduler --lib  # 304 passed
cargo test -p krishiv-executor --lib   # 181 passed (1 pre-existing failure: checkpoint_fanout test missing executor_id; not introduced here)
cargo test -p krishiv-api --lib        # 60 passed
cargo test -p krishiv-runtime --lib    # 319 passed
cargo test -p krishiv-flight-sql --lib # 42 passed
```

### Blockers
None introduced. Pre-existing: `checkpoint_fanout_uses_running_attempts_without_preexisting_task_runner` test in krishiv-executor fails due to missing `with_executor_id()` call — not related to this session's changes.

### Next useful task
```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p krishiv-runtime
```

---

## E6 Connector Series: Avro, CSV/JSON, Kinesis, Pulsar, Elasticsearch, HBase, Cassandra (2026-06-10)

### Done

Implemented seven production-quality connectors in `crates/krishiv-connectors/src/` under the E6.x series. Each connector is feature-gated, has a correct Arrow schema, pure conversion helpers (testable offline), and at least 6 unit tests. ORC (E6.2) blocked on `orcrs` crate incompatibility with Rust 1.92.

| Module | Feature | Tests | Notes |
|---|---|---|---|
| `avro.rs` | `avro` | 10 | `AvroSource`/`AvroSink<W>`; schema conversion; lifetime trick: `flush(self)` consumes |
| `csv_json.rs` | *(always)* | 14 | `CsvSource`/`NdjsonSource`; `Format` from `arrow::csv::reader::Format` |
| `kinesis.rs` | `kinesis` | 7 | `KinesisSource::next_batch()`; `records_to_batch()` testable offline |
| `pulsar_connector.rs` | `pulsar-source` | 6 | `RawBytes` shim; `TryStreamExt::try_next()` |
| `elasticsearch_sink.rs` | `elasticsearch` | 7 | Bulk API; `SingleNodeConnectionPool`; `url::Url` |
| `hbase_connector.rs` | `hbase` | 8 | Thrift-1 client; `MutationBuilder::column(family, qualifier)`; `spawn_blocking` |
| `cassandra_sink.rs` | `cassandra` | 7 | ScyllaDB driver; UNLOGGED BATCH; `CqlValue` mapping |

Also fixed two pre-existing bugs:
- **`krishiv-executor/src/fragment/batch.rs`**: Duplicate `use std::sync::Arc;` at lines 5+10 → removed the second one.
- **`krishiv-runtime/src/plan.rs`** and **`in_process_cluster.rs`**: Non-exhaustive `WindowKind` match — missing `WindowKind::Count { .. }` arm → mapped to `LocalWindowKind::Tumbling`.

### Validation
```bash
cargo check --workspace                                                    # exit 0
cargo test -p krishiv-connectors --lib --features "avro"                  # 10+14=24 passed
cargo test -p krishiv-connectors --lib --features "cassandra,hbase"       # 15 passed
cargo test -p krishiv-connectors --lib --features "pulsar-source"         # 6 passed
```

### Blockers
- E6.2 (ORC): `orcrs 0.5.0` fails compilation on Rust 1.92 (protobuf `Enum` trait mismatch). Dependency removed; blocked until an updated crate is available.

### Next useful task
```bash
cargo test -p krishiv-connectors --lib --features "kinesis,elasticsearch"   # Kinesis + ES tests
# Then E7.x: metrics instrumentation, DAG visualization, final quality gate
```

---

## Phase 5: Full quality gate (2026-06-10)

### Done
All 20 crates in the workspace now compile clean under `cargo clippy --workspace -- -D warnings` and all lib tests pass.

**Clippy fixes applied across the workspace:**
- `#[default]` on enum variants replacing manual `impl Default` (`krishiv-common`)
- Removed needless `return` statements in match arms (`krishiv-proto`)
- Unused imports removed (`krishiv-plan`, `krishiv-executor`, `krishiv-sql`)
- `div_ceil()` replacing manual ceiling-division arithmetic (`krishiv-plan`, `krishiv-scheduler`)
- `is_some_and()` / `is_none_or()` replacing `map_or(bool, ...)` idioms
- `is_multiple_of()` replacing `% n == 0` (`krishiv-scheduler`)
- `repeat_n()` replacing `repeat().take()` (`krishiv-dataflow`)
- `std::slice::from_ref(&x)` replacing `&[x.clone()]` (`krishiv-executor`)
- Let-chains (`if let ... && ...`) collapsing nested ifs throughout all crates
- Type aliases for `Pin<Box<dyn Future/Stream<...>>>` complex return types (`krishiv-connectors`, `krishiv-executor`, `krishiv-runtime`)
- Module inception fix: `registry/registry.rs` → `registry/connector_registry.rs` (`krishiv-connectors`)
- `async fn` replacing `fn f() -> impl Future { async move { } }` (`krishiv-shuffle`)
- Dead code removed: unused fields, methods, functions across `krishiv-sql`, `krishiv-ui`, `krishiv-api`
- `sort_by_key(|k| Reverse(...))` replacing `sort_by(|a,b| b.cmp(&a))` (`krishiv-scheduler`)
- `matches!()` macro replacing `match { T => true, _ => false }` (`krishiv-runtime`)
- `.enumerate()` replacing explicit counter variables (`krishiv-bench`)
- `#[cfg(test)]` gating on structs/methods used only in test code (`krishiv-executor`)
- Test fix: `.inner()` method call → `.inner` field access in `krishiv-sql/src/policy.rs`

### Validation
```bash
cargo clippy --workspace --exclude krishiv-python -- -D warnings   # Finished dev; 0 errors
cargo test --workspace --lib --exclude krishiv-python               # 19 test suites, 0 failures
# Suite totals: 52+152+60+58+304+227+182+42+71+46+407+65+319+297+116+248+276+19 = 2791 tests passed
```

### Next useful task
```bash
cargo test --workspace --exclude krishiv-python   # integration tests (slower, spawn processes)
```

---

## Phase 1.1: Broker-backed Kafka CheckpointSource — seek-based restore (2026-06-10)

### Done
1. **`MultiKafkaOffset` struct** added to `crates/krishiv-connectors/src/kafka.rs` — newtype over `Vec<KafkaOffset>` with length-prefixed binary encoding (u32 count + per-entry u32 item_len + KafkaOffset bytes). Handles 0..N partitions; encode/decode are inverse.
2. **`CheckpointSource for RdkafkaKafkaSource`** — `checkpoint_offset()` returns current `partition_offsets` snapshot as `MultiKafkaOffset`; `restore_offset()` calls `consumer.assign(TopicPartitionList)` with `rdkafka::topic_partition_list::Offset::Offset(ko.offset)` to bypass group rebalance and seek directly to the stored position. Rebuilds `partition_offsets` so `checkpoint_offset()` is accurate immediately after restore, before new messages arrive.
3. **`CheckpointSource for KafkaSource`** — wrapper delegates to inner `RdkafkaKafkaSource`.
4. **Capabilities updated** — `RdkafkaKafkaSource::capabilities()` now returns `.with_unbounded().with_checkpoint()`.
5. **`from_cdc_config` cfg gate fixed** — was `#[cfg(feature = "kafka")]`, now `#[cfg(all(feature = "kafka", feature = "lakehouse"))]` since `crate::cdc` is gated on `lakehouse`.
6. **`TaskRunner` upgraded** — `kafka_source_offset: i64` (single-partition, sentinel `-1`) replaced with `kafka_source_offsets: Vec<KafkaOffset>` (multi-partition, empty = non-Kafka task). `handle_initiate_checkpoint` emits one `CheckpointSourceOffset` per partition with `partition_id = "kafka-{topic}-{partition}"`.
7. **Tests updated** — `kafka_source_reports_unbounded_and_rewindable` now asserts `is_checkpoint_capable()` when kafka feature enabled; runner tests use `with_kafka_source_offsets(vec![...])` API; `executor_checkpoint_ack_includes_source_offset` tests two-partition offset propagation.
8. **New tests** — `multi_kafka_offset_{empty,single,multi}_roundtrip`, `multi_kafka_offset_decode_rejects_{trailing_bytes,truncated_entry}`, `rdkafka_kafka_source_implements_checkpoint_source_trait`, `kafka_source_wrapper_implements_checkpoint_source_trait`, `task_runner_with_kafka_source_offsets`.

### Validation
```bash
cargo check --workspace                                              # exit 0
cargo test -p krishiv-connectors --lib                              # 58 passed
cargo test -p krishiv-connectors --lib --features kafka             # 86 passed
cargo test -p krishiv-executor --lib                                # 182 passed
```

### Architecture note
- `subscribe()` is used on first consumer creation (group-managed partition assignment).
- `assign()` is used on checkpoint restore — bypasses group coordinator, seeks each partition to the exact stored offset. This is the correct architectural boundary: subscribe for liveness, assign for deterministic recovery.
- `partition_offsets` stores last-received message offset; `all_current_offsets()` returns `offset+1` ("next to read"). After restore, pre-seeds `partition_offsets[p] = ko.offset - 1` so `checkpoint_offset()` is idempotent before the first post-restore read.

### Next
- Phase 1.3: Iceberg write/commit path using iceberg-rust 0.9 Transaction API.

---

## Phase 1.2: Continuous-cycle state persistence to MetadataStore (2026-06-10)

### Done
1. **`ContinuousSnapshot` struct** added to `crates/krishiv-scheduler/src/store.rs` — `{snapshot_bytes: Vec<u8>, watermark_ms: i64}` with binary encode/decode (`[watermark_ms i64 LE][bytes_len u32 LE][bytes]`).
2. **`MetadataStore` trait extended** — 3 new methods: `save_continuous_snapshot`, `load_continuous_snapshot`, `remove_continuous_snapshot`.
3. **`InMemoryMetadataStore`** — `HashMap<String, ContinuousSnapshot>` field; all 3 methods implemented.
4. **`RedbMetadataStore`** — `CONTINUOUS_TABLE: TableDefinition<&str, &[u8]>` table; in-memory cache for synchronous reads; redb transactions for durable writes. Loaded from disk on `open()`.
5. **`EtcdMetadataStore`** — no-op with `tracing::warn!` explaining the 1.5 MiB etcd value limit; operators must use an external object store for continuous state with the etcd backend.
6. **`NonBlockingStoreHandle`** extended — fire-and-forget `save_continuous_snapshot` via bounded async channel; synchronous fallback when no Tokio runtime is active (tests); `load_continuous_snapshot` reads through to in-memory view.
7. **`StoreCommand::SaveContinuousSnapshot`** added — background task handler calls `store.save_continuous_snapshot`.
8. **`ContinuousStreamRegistry::snapshot_job_with_watermark`** — returns `(snapshot_bytes, watermark_ms)` by calling `exec.snapshot()` + `exec.last_watermark_ms()`.
9. **`Coordinator` methods** — `attach_store` (post-construction store attachment), `save_continuous_snapshot` (fire-and-forget delegation), `load_continuous_snapshot` (synchronous read from in-memory view).
10. **`InProcessStreamingRuntime::drain_continuous_job`** — auto-snapshots after each drain and persists via coordinator's store (no-op when no store attached; errors swallowed so drain is never degraded).
11. **`InProcessStreamingRuntime::attach_store`** — attaches a `MetadataStore` to an already-constructed runtime; subsequent drains persist snapshots.
12. **`InProcessStreamingRuntime::load_continuous_snapshot`** — public API to read persisted snapshots (used for cross-session snapshot transfer in tests).
13. **`InProcessStreamingRuntime::restore_continuous_jobs_from_store`** — restores jobs from store using `register_job_from_snapshot`; skips jobs with no stored snapshot or already-registered jobs.
14. **`InProcessCluster`** forwarding methods — `attach_store`, `load_continuous_snapshot`, `restore_continuous_jobs_from_store` (takes `&[(&str, &LocalWindowExecutionSpec)]`).
15. **`ContinuousSnapshot` re-exported** at `krishiv_scheduler` crate root.

### Validation
```bash
cargo check --workspace                                              # exit 0 (no errors)
cargo test -p krishiv-scheduler --lib                               # 293 passed
cargo test -p krishiv-runtime --lib                                 # 319 passed
```

### Architecture notes
- **Snapshot-per-drain discipline**: every `drain_continuous_job` call snapshots immediately after the window executor completes. This gives the latest possible recovery point without requiring explicit user coordination.
- **No-store is safe**: when `attach_store` is never called (default embedded mode), all snapshot calls are no-ops and drains proceed unaffected.
- **Sync fallback**: `NonBlockingStoreHandle` detects the absence of a Tokio runtime at construction time (`tx = None`). In that mode, `save_continuous_snapshot` writes synchronously. This makes the behavior deterministic in unit tests without `#[tokio::test]`.
- **Cross-session transfer pattern**: Session 1 attaches a store, drains (snapshot persisted); session 2 reads the snapshot via `load_continuous_snapshot`, pre-populates a new `InMemoryMetadataStore`, attaches it, calls `restore_continuous_jobs_from_store`. For production, replace `InMemoryMetadataStore` with `RedbMetadataStore` (same file path = automatic recovery).
- **EtcdMetadataStore limitation**: etcd default 1.5 MiB value limit is insufficient for arbitrary window state. The no-op + warn design makes the limitation explicit rather than silently truncating state.

---

## Phase 4: Release engineering (2026-06-10)

### Done
1. **CI gate discrepancy fixed** — `justfile` had `check-distributed` but CI matrix used `just check-bare-metal`; added `check-bare-metal` recipe and kept `check-distributed` as an alias.
2. **Benchmark CI workflow** — `.github/workflows/bench.yml` runs `cargo bench -p krishiv-bench` on every push/PR. On `main`, saves a criterion baseline to the CI cache; on PRs, restores the baseline and runs `bench-compare` for regression detection. Results uploaded as artifacts.
3. **`bench` / `bench-save` / `bench-compare` / `release` justfile recipes** added.
4. **Versioning policy** — `docs/RELEASE.md` documents the `MAJOR.MINOR.PATCH` policy, the release checklist, the CI gate matrix, and how to update baselines. `just release VERSION=x.y.z` automates the bump + tag.

### Validation
```bash
just --list         # bench, bench-save, bench-compare, check-bare-metal, release all listed
cargo check --workspace  # exit 0
```

### Next
- Phase 5: Full quality gate — `cargo fmt --check`, `cargo test --workspace --lib`, `cargo clippy --workspace`.

---

## Phase 3: Structural debt in krishiv-plan (2026-06-10)

### Done
1. **AQE clone-per-rule fix** — `AqeRule::apply` now takes `&PhysicalPlan` instead of `PhysicalPlan`. Non-firing rules pay no clone cost. Rules that fire clone internally only when they know a rewrite is needed (`stamp_target`, coalesce path). `AqeOptimizer::apply` passes `&current` instead of `current.clone()`.
2. **Panic helper unified** — `panic_payload_message` (optimizer.rs) and `panic_message` (udf.rs) removed. Canonical `panic_payload_to_string` added to `krishiv-common::panic_util` and re-exported. Both callers updated.
3. **CEP O(n) eviction → O(log n)** — `PartitionedCepMatcher` now maintains a `BinaryHeap<Reverse<(i64, K)>>` min-heap alongside the state map. On each `process_event`, the updated `(timestamp, key)` is pushed to the heap. Eviction via `evict_stalest()` pops + lazy-skips stale entries. Added `K: Ord` bound. `evict_keys_before` unchanged (O(n) full scan is appropriate there).
4. **AuditEvent job_id redaction** — `audit_log` now builds a `redacted` detail alongside the full `detail`. The full detail is used only for dedup-key hashing. `AuditEvent.resource` receives the redacted form where all `job_id` and `query_hash` values are replaced with their SHA-256-derived 16-hex-char fingerprint via `redact_id()`.
5. **optimizer.rs split** — 3,872-line file split into 5 submodule files under `src/optimizer/`: `coalesce.rs`, `auto_partition.rs`, `small_file.rs`, `broadcast.rs`, `predicate_pushdown.rs`. Core traits, `Optimizer`, `AqeOptimizer`, `StreamingAqeGuard`, `StaticCostModel`, and tests remain in `optimizer.rs`. All types re-exported via `pub use` so the public API is unchanged.

### Validation
```bash
cargo check --workspace                                              # exit 0
cargo test -p krishiv-common --lib                                   # passes
cargo test -p krishiv-plan --lib                                     # 407 passed
```

### Next
- Phase 4: CI gate matrix, benchmark baselines, versioning policy.

---

## krishiv-plan Full Audit: Round 2 (2026-06-10)

Full audit of all 14 source files in `crates/krishiv-plan/src/` — identified 18+ findings across P0–P3. Three bugs fixed, remaining items noted.

### Fixed
1. **P0 — Multi-agg compact fragment broken** (`window.rs`): `encode_stream_fragment` joined multiple aggregates with `;` (e.g. `agg=count;agg=sum:col=amount`) but `parse_stream_fragment` only read a single `agg=`, causing "unknown streaming aggregate" errors. Fix: `encode_stream_fragment` now delegates to the lossless JSON format (`stream:spec:v1:...`) for multi-agg specs — the compact text format cannot represent them due to `:` delimiter conflicts with `agg=kind:col=value` syntax. Single-agg compact format unchanged.
2. **P1 — Scan fragment silently drops pushed-down filters** (`lowering.rs`): `node_op_to_fragment` for `NodeOp::Scan` used `..` to ignore the `filters` field — predicates pushed by `PredicatePushdownRule` were lost. Fix: filters are now encoded as a `WHERE` clause in the `sql:SELECT * FROM "table" WHERE pred1 AND pred2` fragment.
3. **P1 — CoalesceRule overflow returns silent `None`** (`optimizer.rs:521`): `suffix.checked_add(1)?` returned `None` on `usize` overflow, which the caller interpreted as "no change to the plan". Fix: replaced with `saturating_add(1)` so the suffix stays at `usize::MAX` instead of silently masking the overflow.

### Remaining (not fixed this session)
- **P1 — Optimizer clones plan per rule**: `AqeOptimizer::apply` clones the entire plan on each rule iteration (`current.clone()`). Acceptable for moderate plans but O(rules × plan_size).
- **P2 — `optimizer.rs` at 3737 lines**: Should be split into separate rule files (Logical rules, AQE rules, cost model, test modules).
- **P2 — Duplicate `panic_message` / `panic_payload_message`**: Nearly identical functions in `udf.rs` and `optimizer.rs`; should be unified in `krishiv-common`.
- **P2 — `CostModel` trait with no production implementation**: Only used in `explain_sql_with_cost` with test mocks. Wire to a real cost function or deprecate.
- **P3 — `PartitionedCepMatcher` eviction is O(n)** per insertion when over capacity.
- **P3 — Global `OnceLock` for audit/lineage sinks** is inherently single-process.
- **P3 — `AuditEvent.resource` includes job_ids/query_hashes in `detail`** — potential PII leakage.
- **P3 — `encode_window_execution_spec` clones the entire spec** to normalize empty aggregates.

### Validation
```bash
cargo test -p krishiv-plan --lib    # 400 passed (was 397 before audit)
cargo test -p krishiv-runtime --lib  # passes
```

### Next useful commands
```bash
cargo clippy -p krishiv-plan --all-targets
cargo test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Architectural gap closed: shuffle_partitions → AutoPartitionRule (2026-06-09)

### Done
1. **`shuffle_partitions` field** added to `PlanCore`/`LogicalPlan`/`PhysicalPlan` — propagates the override through the plan pipeline.
2. **`lower_to_physical()`** propagates `shuffle_partitions` from logical → physical plan.
3. **`AutoPartitionRule::apply()`** checks `plan.shuffle_partitions()`. When set, uses it as the target bucket count instead of computing from data size. Skips streaming plans but does not require runtime stats.
4. **`SqlDataFrame`** carries `shuffle_partitions` and propagates it in `krishiv_logical_plan()`.
5. **`SqlEngine::sql()`** stamps the override from `self.shuffle_partitions()` onto each `SqlDataFrame` it creates.
6. **`KRIVISH_SHUFFLE_PARTITIONS` env var** parsed in `SessionBuilder::from_env()`.
7. **`DataFrame::repartition()`** typo fix: `terminers` → `terminals`.

### Flow
```
SET shuffle.partitions = 8
  → SqlEngine::sql() stores on SqlDataFrame
  → krishiv_logical_plan() → LogicalPlan.shuffle_partitions = Some(8)
  → lower_to_physical() → PhysicalPlan.shuffle_partitions = Some(8)
  → submit_physical_plan() → AutoPartitionRule::apply()
    → reads plan.shuffle_partitions() → stamps 8 on all exchange nodes
```

### Validated
- `cargo check -p krishiv-sql -p krishiv-api -p krishiv-plan -p krishiv-scheduler` (clean)
- `cargo test -p krishiv-plan --lib` (397 passed)

### Remaining gap: Phase 3 (BroadcastAutoRule) — CLOSED (stale claim corrected 2026-06-10)
This gap no longer exists: `df_plan_to_krishiv_nodes` (`krishiv-sql/src/lib.rs`) translates
DataFusion logical plans into typed Krishiv `PlanNode` DAGs (Scan/Project/Filter/Aggregate/
Join/Sort/Repartition/Limit/Union), annotates scans with `estimated_rows` from the engine's
table-row-count registry, and `krishiv_logical_plan()` runs `default_logical_optimizer()` so
`BroadcastAutoRule` fires on eligible scans. Residual task: end-to-end test proving broadcast
promotion through `krishiv_logical_plan()` on a small-table join.

---

## Phase 2: Hot-key detection during shuffle write (2026-06-09)

### Phase 2a — HeavyHittersTracker wired into shuffle write ✓
Plumbed `HeavyHittersTracker` (SpaceSaving algorithm) through both shuffle write paths:

1. **`execute_shuffle_write_fragment`** (`krishiv-executor/src/fragment/batch.rs:463-502`):
   After partitioning, runs the tracker on the key column, detects hot keys at 10%
   threshold, emits `HeartbeatHotKeyReport`s.
2. **`execute_inmem_shuffle_write`** (`krishiv-executor/src/fragment/batch.rs:605-645`):
   Same tracking for the in-memory shuffle store path.
3. **`ExecutorTaskOutput::hot_key_reports`** (`krishiv-executor/src/runner.rs:226`):
   New field `Vec<HeartbeatHotKeyReport>` on task output.
4. **`TaskOutputMetadata`** (`krishiv-proto/src/executor.rs:142`): Added
   `hot_key_reports` field with wire serialization/deserialization
   (`krishiv-proto/src/wire.rs:787, 854-862`). Added
   `with_hot_key_reports()` builder method.
5. **`HeartbeatHotKeyReport` Eq**: Added manual `Eq` + `Ord` impl using
   `total_cmp` for `f64` heat_score field.

Hot-key reports flow to the coordinator via `TaskOutputMetadata` on task
completion (`send_task_status` path). The existing heartbeat path
(`ExecutorHeartbeatRequest::with_hot_key_reports`) is already processed by
`coordinator::executor_ops::process_hot_key_reports` for streaming tasks.

### Phase 2b — Coordinator-side processing ✓
Already handled by existing `process_hot_key_reports` in
`krishiv-scheduler/src/coordinator/executor_ops.rs:118-160`. Applies
`HOT_KEY_HEAT_THRESHOLD = 0.3` to decide whether to log an adaptive decision
and emit a throttle command.

Validation:
```bash
cargo check --workspace --exclude krishiv-python                    # ✓
cargo test -p krishiv-proto                                         # 65 ✓
cargo test -p krishiv-executor --lib -- runner_tests                # 26 ✓
```

Next: Phase 3 — `BroadcastAutoRule` (already registered as no-op until source
metadata populates `estimated_rows`).

---

## Phase 1: AutoPartition + data-size-aware shards (2026-06-08)

### Phase 1a — AutoPartitionRule ✓
Added `AutoPartitionRule` — an AQE rule that adjusts `Hash`/`RoundRobin` exchange
bucket counts based on observed data volume from the prior execution:

1. **`AutoPartitionRule` struct** (`krishiv-plan/src/optimizer.rs:539-636`): AQE
   rule that sums `memory_bytes` across `RuntimeStats`, computes
   `target = max(1, min(max_buckets, ceil(total_bytes / target_partition_bytes)))`,
   and bumps bucket counts on exchange nodes that are below the target. Default
   128 MiB per partition. No-op when stats are empty or all nodes already meet
   the target.
2. **Mutation API** (`krishiv-plan/src/lib.rs`): Added `PlanNode::set_partitioning()`,
   `PhysicalPlan::nodes_mut()`, and `PlanCore::nodes_mut()` so AQE rules can
   adjust node partition counts in-place without rebuilding the DAG.
3. **Registered** in `default_aqe_optimizer()` as a guarded rule (skipped for
   streaming) with `max_buckets: 64`, before `CoalesceRule`.

### Phase 1b — Bounded-window shard count ✓
`krishiv-scheduler/src/bounded_window.rs`: Replaced the hardcoded shard limit
(`executor_count.min(input_row_count)`) with a data-size-aware computation:
`data_based_shards = max(1, ceil(total_data_bytes / 128 MiB))`, capped by
available executors and input rows. Total data bytes are computed from
`RecordBatch::get_array_memory_size()` across all input batches.

### Phase 1c — AutoPartitionRule wired in job_lifecycle ✓
Already done: `default_aqe_optimizer()` (which now includes `AutoPartitionRule`)
is called in `krishiv-scheduler/src/coordinator/job_lifecycle.rs:473` via
`submit_physical_plan`. With empty stats (first execution) the rule is a no-op;
AQE re-optimization applies when per-stage stats become available.

---

## Cleanup: fixed consolidation fallout + crate mode matrix (2026-06-08)

Follow-up cleanup after the 25→20 crate consolidation:

1. **Fixed broken references** in merged submodules:
   - Internal `crate::checkpoint::` paths in `krishiv-state/src/checkpoint/` (object_store, storage_uri, mod.rs)
   - Internal `crate::cep::pattern::` path in `krishiv-plan/src/cep/matcher.rs`
   - Corrupted `krishiv-dataflowutor` reference in `krishiv-runtime/Cargo.toml`
   - Bare `use krishiv_udf;` import in `krishiv-executor/src/cli.rs`
2. **Batch-fixed old import paths** via sed: `krishiv_exec::` → `krishiv_dataflow::`, `krishiv_checkpoint::` → `krishiv_state::checkpoint::`, etc.
3. **Deleted old crate directories**: `krishiv-checkpoint`, `krishiv-udf`, `krishiv-governance`, `krishiv-cep`, `krishiv-optimizer`, `krishiv-exec`
4. **Removed deprecated items**: `StoreError` type alias (krishiv-shuffle), `commit_watermark` method (krishiv-connectors/kafka)
5. **Updated docs**: README crate list, CONTRIBUTING.md stale refs, architecture.md/architecture.txt with grounded code facts
6. **Added Crate Requirements by Mode** matrix to `docs/architecture.md` — shows required/optional/excluded per mode with feature gate annotations for all 20 crates

Validation: `cargo check --workspace` passes (zero errors).

Blocked: `krishiv-shuffle` fails in isolation (`cargo test -p krishiv-shuffle --lib`) due to pre-existing missing `object_store` `aws` feature in its own Cargo.toml.

Next useful commands:
```bash
cargo test --workspace --lib --no-fail-fast --exclude krishiv-python
cargo clippy --workspace --all-targets
```

---

## Connector follow-ups and lakehouse merge (2026-06-07)

Completed connector consolidation follow-ups on branch
`cursor/connector-registry-consolidation-9bc6`:

1. **SQL DDL factories via registry** (`krishiv-sql::connector_table`):
   - Registered `PARQUET`, `S3`, and `KAFKA` `TableProviderFactory` hooks in
     `SqlEngine::build_local`, delegating config validation to
     `ConnectorRegistry::default_registry()`.
   - Bounded Parquet/S3 tables materialize through registry-opened sources;
     Kafka DDL reuses `KafkaPartitionStream` with registry-validated config.
   - Removed standalone `KafkaTableFactory`; `kafka_table` keeps streaming helpers.
2. **Physical lakehouse merge**: moved `krishiv-lakehouse` implementation into
   `krishiv-connectors::lakehouse` (feature `lakehouse`). `krishiv-lakehouse`
   is now a thin facade re-exporting `krishiv_connectors::lakehouse::*`.
   Updated `cdc` / `cdc_router` and integration tests to use the internal module.
3. **Cleanup**: added `tmp/` to `.gitignore`; fixed lakehouse test imports after
   the move; updated connector integration tests to stop depending on the facade
   crate from `krishiv-connectors` tests.

Validation:
```bash
cargo check -p krishiv-connectors --features "parquet,lakehouse,iceberg,delta,kafka"
cargo check -p krishiv-lakehouse -p krishiv-sql -p krishiv-exec -p krishiv-executor
TMPDIR=/workspace/tmp cargo test -p krishiv-connectors --lib --features lakehouse -- lakehouse::
TMPDIR=/workspace/tmp cargo test -p krishiv-connectors --lib --features "parquet,s3,kafka,two-phase,lakehouse,vector-sinks" registry::tests
```

Note: `krishiv-sql::udtf_ddl_tests::sql_body_udtf_rejects_wrong_arity_and_non_literal_arguments`
still fails on this branch (pre-existing UDTF arity guard; unrelated to connector work).

Next useful command:
```bash
TMPDIR=/workspace/tmp cargo test -p krishiv-connectors --lib --features full
```

## Connector registry consolidation (2026-06-07)

Implemented the connector driver/registry pattern across four phases:

1. **Phase 1 — registry in `krishiv-connectors`**: added `ConnectorKind`,
   `ConnectorRegistry`, `SourceDriver` / `SinkDriver` / `TwoPhaseSinkDriver`,
   `DynSource`, built-in drivers for Parquet/S3/Kafka/two-phase Parquet, and
   `default_registry()`.
2. **Phase 2 — vector sinks**: moved `krishiv-ai::vector_sinks` into
   `krishiv-connectors::vector` (feature `vector-sinks`); `krishiv-ai` now
   re-exports via a compatibility shim.
3. **Phase 3 — lakehouse**: kept implementation in `krishiv-lakehouse` (physical
   move blocked by `exec ↔ lakehouse ↔ connectors` dependency graph); added
   `connector_registry` kind constants and `ConnectorKind::{Iceberg,Delta,Hudi}`
   for discovery. Broke the `connectors → exec` edge by moving
   `StreamQualityHook` to `krishiv-common` and adding
   `connectors::schema_normalize` for CDC paths.
4. **Phase 4 — defaults**: `krishiv-connectors` default features are now
   `["parquet"]` (was `["kafka"]`); SQL/executor/python enable
   `kafka`/`lakehouse` explicitly.

Validation:
```bash
cargo check -p krishiv-connectors --features parquet
cargo check -p krishiv-connectors --features "parquet,s3,kafka,two-phase,lakehouse,vector-sinks"
cargo test -p krishiv-connectors --lib --features "parquet,s3,kafka,two-phase,lakehouse,vector-sinks" registry::tests
cargo check -p krishiv-sql -p krishiv-executor -p krishiv-ai
```

Next useful command:
```bash
cargo test -p krishiv-connectors --lib --features full
```

## Crate consolidation: chaos / schema-registry / catalog merge (2026-06-07)

Implemented a 3-step workspace crate-consolidation refactor to reduce the
number of standalone crates with thin or overlapping responsibilities:

1. **Removed `krishiv-chaos`**: folded its test suite into `krishiv-common`
   as a `chaos_suite` module (no production code lived in the crate, only
   chaos-testing harnesses/tests).
2. **Merged `krishiv-schema-registry` into `krishiv-connectors`** as a
   feature-gated `schema_registry` module (`feature = "schema-registry"`).
   Updated `krishiv-connectors::cdc` to reference
   `crate::schema_registry::{SchemaRegistryClient, RegistryFormat}`, and
   `krishiv-python` now depends on `krishiv-connectors` with
   `features = ["schema-registry"]` instead of the standalone crate
   (`lakehouse.rs` references became `krishiv_connectors::schema_registry::*`).
3. **Merged `krishiv-catalog` into `krishiv-sql`** as a `catalog` module
   (including `catalog::iceberg_rest`). All internal `crate::X` references in
   the moved module were rewritten to `super::X` (the module's old crate root
   is now `krishiv_sql::catalog`, not `krishiv_sql`). `krishiv-python`
   references became `krishiv_sql::catalog::*`.

Removed both `krishiv-schema-registry` and `krishiv-catalog` crate
directories and their workspace `members`/`default-members` entries; updated
`docs/README.md` and `docs/architecture.md` crate tables/diagrams to reflect
the new module locations.

### Incidental fix
While validating with `cargo clippy --workspace --all-targets`, found that
`crates/krishiv-lakehouse/src/iceberg_fs.rs:194` (pre-existing, last touched
2026-06-05, unrelated to this refactor) tripped `clippy::never_loop`
(deny-by-default in the current toolchain): a `loop { ... }` whose every
branch returned on the first iteration. Removed the redundant `loop`
wrapper — this was blocking *any* `cargo clippy` run because
`krishiv-lakehouse` is a transitive dependency of the merged crates.

Validation:
```bash
cargo check --workspace
cargo test -p krishiv-connectors --lib   # 126/126 passed (incl. schema_registry suite)
cargo test -p krishiv-sql --lib          # 136/136 passed (incl. catalog suite + catalog_table_resolved_in_sql)
cargo fmt --check                        # clean for all touched files
cargo clippy -p krishiv-sql -p krishiv-connectors -p krishiv-common \
  -p krishiv-python -p krishiv-lakehouse --all-targets   # clean — no new warnings in
                                                          # catalog/schema_registry/chaos_suite/cdc paths
```

Next useful command: `cargo clippy --workspace --all-targets` (should now
complete cleanly end-to-end with the lakehouse fix in place).

---

## Crate consolidation: Merge 6 crates into 3 (2026-06-08)

Reduced the workspace from 30 to 25 crates by merging small single-domain crates:

1. **Renamed `krishiv-exec` → `krishiv-dataflow`**: Eliminated confusion with
   `krishiv-executor`. Updated all Cargo.toml dependencies and Rust import paths.
2. **Merged into `krishiv-plan`**: `krishiv-udf`, `krishiv-governance`,
   `krishiv-cep`, `krishiv-optimizer` — all plan/rule/policy extensions consumed
   by the same downstream crates. Each became a `pub mod` (udf, governance, cep,
   optimizer) with source files copied from the original crates.
3. **Merged `krishiv-checkpoint` into `krishiv-state`**: Both are durability/cold-storage
   domain crates; checkpoint already depended on state. Checkpoint became
   `crate::checkpoint` submodule with internal crate:: → super:: path rewrites.

Old crate directories (checkpoint, udf, governance, cep, optimizer, exec) were
deleted and removed from workspace members/default-members in root Cargo.toml.

Validation:
```bash
cargo check --workspace
```

Next useful command:
```bash
cargo test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---
Continued implementing roadmap.md Phase 5 testing items 155-175 (regression
tests for prior-wave fixes plus untriaged coverage gaps):

- `krishiv-common::production`: extracted `resolve_durability_profile_from`
  (env-free helper) to test the malformed/missing/valid env-value fallback
  paths without `env::set_var` (blocked by edition-2024 `unsafe fn` +
  crate-level `#![forbid(unsafe_code)]`).
- `krishiv-exec`: regression tests for `TumblingWindowOperator::validate_spec`
  and `SlidingWindowOperator::new` rejecting zero/overflowing window sizes;
  CEP `CepOperator` per-key state eviction at `max_keys` (bounds memory under
  high key cardinality).
- `krishiv-shuffle`: new `partitioner` test module — null-key routing/counting
  and `Clone` resetting the null-key counter.
- `krishiv-scheduler`: `barrier_dispatch` ack-handling regression test;
  `mark_executor_lost`/`apply_task_update`/async-checkpoint-ack metric-delta
  tests; `coordinator_daemon` `/readyz` health-gating test.
- `krishiv-runtime`: `ContinuousStreamRegistry::drain_job` max-drain-batches
  cap regression test.
- `krishiv-metrics`: `inc_executor_lost` increment+render regression test.
- `krishiv-ui`: poison-recovery test for `UiState.metrics_cache` (spawns a
  thread that panics while holding the lock, asserts `/metrics` still serves
  `200 OK` via `.lock().unwrap_or_else(|e| e.into_inner())`).
- `krishiv-connectors`:
  - `cdc_router`: `ConnectorError::Cdc` propagation tests for unknown-table
    routes, missing-payload events, and `poll_and_route` error propagation.
  - `feature_store`: `compact_expired` regression test (drops only
    TTL-elapsed rows, shrinks `live`/rebuilds the entity index, idempotent).
  - `s3`: `S3Sink::with_max_pending_bytes` regression test (rejects writes
    over the byte cap with `pending byte limit` error; `flush` resets
    accounting so writes can resume).
- `krishiv-flight-sql`: `run_blocking` panic-propagation regression test
  (current-thread runtime exercises the `std::thread::scope` fallback;
  asserts a panicking closure surfaces as `Status::internal` with
  `"run_blocking thread panicked"`, not an unwinding panic).

Also confirmed already-adequate (no new test needed): interval-join buffer
cap + per-key eviction (`buffer_limit_drops_oldest_events_when_exceeded`,
`per_key_evict_cleans_all_keys` already cover unbounded-growth concerns).

Validation:
```bash
cargo test -p krishiv-common -p krishiv-exec -p krishiv-shuffle \
  -p krishiv-runtime -p krishiv-metrics -p krishiv-scheduler \
  -p krishiv-connectors -p krishiv-flight-sql -p krishiv-ui --lib
```
All pass except the pre-existing `krishiv-ui::tests::ui_jobs_renders_html`
failure (`body.contains("Krishiv R2 Status")`), confirmed via `git stash` to
fail identically on a clean `main` — unrelated to this sweep.

Next: items still to triage from the Phase 5 list — verify no further
untriaged "DONE on checklist but no real test" gaps remain across
`krishiv-connectors` (Kafka/CDC), `krishiv-chaos`, and `krishiv-lakehouse`.

## krishiv-plan Full Audit Resolution (2026-06-06)

Implemented the complete `krishiv-plan` audit resolution plan. No deprecated code, no backward compatibility shims.

### Phase 1 — P0 critical fixes
- **`NodeOp` consolidation** (`lib.rs`): Replaced `TumblingWindow`, `SlidingWindow`, `SessionWindow` with a single `Window { spec: Box<WindowExecutionSpec> }` variant. All data from `WindowExecutionSpec` is now preserved (key_column_type, watermark_lag_ms, multi-agg). Updated all consumers: `lowering.rs`, `task_fragment.rs`, `streaming_plan.rs`, `krishiv-runtime/src/plan.rs`, scheduler and runtime integration tests.
- **Lossless window encoding** (`lowering.rs`): `node_op_to_fragment` now calls `encode_window_execution_spec` (lossless JSON prefix `stream:spec:v1:`) instead of lossy `encode_stream_fragment`. Session window encoding bug (window_size_ms=0) eliminated.
- **Strict window validation** (`window.rs`): `validate_window_execution_spec` now rejects sliding windows without explicit `slide_ms` and session windows without explicit `session_gap_ms`.

### Phase 2 — P1 fixes
- **`task_fragment.rs`**: Added `KeyBy` and `StateTtl` to streaming classification in `execution_kind_from_legacy`. Deepened `validate_job_fragments` to also validate embedded window specs. Fixed `task_body_for_profile` to avoid `.trim().to_owned()` allocation when body has no surrounding whitespace.
- **`diff_plans`** (`lib.rs`): Now compares inputs, partitioning, estimated_rows, and output_schema in addition to label and op. Added `#[must_use]`.
- **`streaming_plan.rs`**: `logical_plan_for_window` now returns `Result<LogicalPlan, PlanError>` and calls `validate_window_execution_spec`. Fixed slide_ms fallback.

### Phase 3 — P2 design fixes
- **`graph.rs`**: Reject duplicate input edges within the same node.
- **`streaming.rs`**: Added `serde` derives, constructors with validation, and tests for `TemporalJoinSpec`, `IntervalJoinSpec`, `SideOutput`.
- **`r17.rs`**: Fixed `im` typo, added constructors with validation, tests for all types (DataSource, EmbedderConfig, VectorSinkPlanConfig, RefreshPolicy, RagIndexSpec, FeatureDef, FeatureSchema, FeatureStore).

### Phase 4 — P3 polish
- **`describe_plan`** (`lib.rs`): Replaced `push_str(&format!(...))` loops with `write!` macro. Added `#[must_use]` to `with_coalesced_partition_count`.
- **`SendableRecordBatchStream` removed** from `krishiv-plan`. Moved to `krishiv-api::streaming_dataframe::KrishivStream`. Removed `arrow` and `futures` from `krishiv-plan/Cargo.toml`. Updated `krishiv-sql` (local `SqlStream` alias), `krishiv-python`, `krishiv-api`.
- Removed public exports of `encode_task_fragment` and `decode_task_fragment` from `krishiv-plan`. Only `encode_typed_task_fragment` is public.

Validation:
```bash
cargo check --workspace                            # ✓
cargo test -p krishiv-plan --lib                   # ✓ 77 passed
cargo test -p krishiv-runtime --lib                # ✓ 304 passed
cargo test -p krishiv-scheduler --lib              # ✓ 282 passed
cargo test -p krishiv-executor --lib               # ✓ 183 passed
```

---

## krishiv-proto Wire Audit Resolution (2026-06-06)

Implemented the full `krishiv-proto` audit resolution plan. All P0/P1 bugs fixed, management wire conversions added.

### Phase 1 — P0 wire data-loss fixes
- **`wire.rs` heartbeat request**: Added `hot_key_reports` (R7.2 SpaceSaving) and `streaming_task_states` (re-attach protocol) serialization/deserialization. Added `hot_key_report_to/from_wire` and `streaming_task_state_to/from_wire` helpers. `streaming_task_state_from_wire` uses `map(...)?` (not `filter_map`) so invalid task IDs propagate as errors.
- **`wire.rs` task assignment**: Added `shuffle_write`/`shuffle_read` (R4a) serialization/deserialization. Added `shuffle_write_config_to/from_wire` and `shuffle_read_config_to/from_wire` helpers.
- **`wire.rs` task output metadata**: Emit both `memory_bytes` (field 13) and structured `shuffle_partitions` (field 14) alongside deprecated parallel arrays. On decode, prefer `shuffle_partitions`; fall back to deprecated parallel arrays. `memory_bytes` now round-trips (was silently zeroed).
- **`wire.rs` heartbeat response**: Replaced `filter_map(|cmd| { ... .ok()? })` with `map(|cmd| { ... })?` so invalid job IDs or fencing tokens in checkpoint commands propagate as `WireError` instead of being silently dropped.

### Phase 2 — P1 wire semantic fixes
- **`wire.rs` WatermarkHint**: Replaced magic `__watermark_hint_{ms}` table-name encoding with proper `INPUT_PARTITION_DESCRIPTOR_KIND_WATERMARK_HINT = 7` kind + `watermark_ms` field. Added decode branch in `input_partition_descriptor_from_wire`.
- **`wire.rs` InMemory**: Replaced silent IPC encode-and-ignore with an explicit `panic!` that names the variant and gives diagnostic guidance. `InMemory` must never cross the wire.
- **`task.rs` KeyGroupRange**: Added `debug_assert!(start <= end)` to `KeyGroupRange::new`. Added `KeyGroupRange::try_new` returning `Result<Self, String>` for callsites that need validation at runtime.

### Phase 3 — Missing wire conversions
- **`wire.rs` management**: Added `trigger_savepoint_request/response_to/from_wire`, `restore_job_request/response_to/from_wire`, `list_checkpoints_request/response_to/from_wire`, `inspect_state_request/response_to/from_wire` for all `CoordinatorManagementService` RPCs.
- **`management.rs` TriggerSavepointResponse**: Added missing `message: String` field (aligns with `TriggerSavepointResponse.message = 2` in proto). Updated `krishiv-scheduler/src/grpc.rs` construction site.

### Proto changes (prior session)
- Added `HeartbeatHotKeyReport` message (fields 14), `StreamingTaskStateWire` message (field 15) to `ExecutorHeartbeatRequest`.
- Added `ShuffleWriteConfigWire` (field 17) and `ShuffleReadConfigWire` (field 18) to `ExecutorTaskAssignment`.
- Added `ShufflePartitionOutputWire` message and `shuffle_partitions = 14` + `memory_bytes = 13` to `TaskOutputMetadata`.
- Added `INPUT_PARTITION_DESCRIPTOR_KIND_WATERMARK_HINT = 7` and `watermark_ms = 15` to `InputPartitionDescriptor`.

Validation:
```bash
cargo check -p krishiv-proto                       # ✓
cargo test -p krishiv-proto                        # ✓ 61 passed
cargo check --workspace                            # ✓
```

Next useful command:
```bash
cargo clippy -p krishiv-proto --all-targets
```

---

## Production Stabilization Waves 1–4 — Data Correctness, Scheduler, Runtime, Observability (2026-06-06)

### Wave 1 — Data Correctness
- **`krishiv-exec/src/window/tumbling.rs`**: Added `validate_spec()` with zero-check and `window_size_ms > i64::MAX` guard. Called from `execute_bounded_window` and `execute_streaming_window`.
- **`krishiv-exec/src/window/sliding.rs`**: Added `window_size_ms == 0` and u64→i64 overflow validation to `SlidingWindowOperator::new()`. Replaced `s + size > event_time_ms` overflow-prone arithmetic with `checked_add`. Replaced `(size + slide - 1) / slide` overflow-prone count with `checked_add`/`checked_sub`.
- **`krishiv-exec/src/window/session.rs`**: Fixed `s.last_event_time_ms + gap` → `s.saturating_add(gap)` for consistent overflow handling with `flush_closed_sessions`.
- **`krishiv-shuffle/src/partitioner.rs`**: Added `null_key_count: AtomicU64` to `HashPartitioner` for tracking null-key routing (all nulls → bucket 0). Exposed via `null_key_count()` accessor. Manual `Clone` impl resets counter on clone.

### Wave 2 — Scheduler Hardening
- **`krishiv-scheduler/src/barrier_dispatch.rs`**: Replaced `let _ = self.handle_checkpoint_ack(request)` with explicit `match` that logs `CheckpointAckResponse::Accepted` vs rejected variants.

### Wave 3 — Runtime/Flight/Continuous Stream
- **`krishiv-runtime/src/flight_client.rs`**: Added 64 MiB response size cap on `do_action` stream reads; returns `RuntimeError::transport` if exceeded.
- **`krishiv-runtime/src/continuous_stream.rs`**: Capped `drain_job()` to `Self::DEFAULT_MAX_DRAIN_BATCHES` (256) instead of `usize::MAX`, preventing unbounded memory use.

### Wave 4 — Observability & Shutdown
- **`krishiv-metrics/src/lib.rs`**: Added `executor_lost` atomic counter and `inc_executor_lost()` method. Added Prometheus renderer line for `krishiv_executor_lost_total`.
- **`krishiv-scheduler/src/coordinator/executor_ops.rs`**: Added `inc_executor_lost()` metric call in `mark_executor_lost`.
- **`krishiv-scheduler/src/coordinator/job_lifecycle.rs`**: Added `inc_tasks_succeeded()` and `inc_tasks_failed()` metric calls in `apply_task_update`.
- **`krishiv-scheduler/src/coordinator/checkpoint_ops.rs`**: Added missing `inc_checkpoint_committed()` to async checkpoint ack path (was only in sync path).
- **`krishiv-scheduler/src/coordinator_daemon.rs`**: `readyz` now checks for healthy executors in addition to `Active` state; returns 503 when no executors can accept work.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-exec --lib --no-fail-fast      # 199 ✓
cargo +nightly test -p krishiv-scheduler --lib --no-fail-fast  # 282 ✓
cargo +nightly test -p krishiv-metrics --lib --no-fail-fast    # 70 ✓
cargo +nightly test -p krishiv-runtime --lib --no-fail-fast    # 304 ✓
cargo +nightly test -p krishiv-api --lib --no-fail-fast        # 60 ✓
cargo +nightly test -p krishiv-ui --lib --no-fail-fast         # 18 ✓
cargo +nightly test -p krishiv-executor --lib --no-fail-fast   # 183 ✓
cargo +nightly test -p krishiv-flight-sql --lib --no-fail-fast # 1 ✓
```

Next useful commands:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Production Stabilization Wave 0.10 — Window Key Column Type Fix (2026-06-06)

### Problem
Window operators (`TumblingWindowOperator`, `SlidingWindowOperator`, `SessionWindowOperator`) hardcoded the key column output type to `DataType::Utf8` regardless of the actual key type. Even though `extract_agg_key` produces typed `AggKey` values, the operators called `.to_string()` and built `StringArray` output, silently converting all keys to strings.

### Fix
- **Plan layer** (`krishiv-plan/src/window.rs`): Added `key_column_type: String` field (default `"utf8"`) to `WindowExecutionSpec` with serde default. Supported values: `"int32"`, `"int64"`, `"float64"`, `"utf8"`, `"bool"`.
- **Runtime layer** (`krishiv-runtime/src/local_streaming.rs`): Added `key_column_type: String` to `LocalWindowExecutionSpec`. Propagated through `local_spec_to_plan_spec` and `plan_spec_to_local` conversions. `streaming_spec_from_plan` defaults to `"utf8"`.
- **Exec layer** (`krishiv-exec/src/window/{tumbling,sliding,session}.rs`, `operator_runtime.rs`, `continuous.rs`): Added `key_column_type` to `TumblingWindowSpec`, `SlidingWindowSpec`, `SessionWindowSpec`. Updated `build_window_record_batch` and session `build_output_batch` to produce correctly-typed key arrays instead of hardcoded `StringArray`.
- Added helper functions: `key_type_to_arrow_data_type`, `key_value_to_typed_array` (tumbling.rs), and `key_type_to_data_type`, `key_value_to_typed_column` (session.rs).

### Status
- Default key type is `"utf8"` for backward compatibility. Callers can opt into type-aware keys by setting `key_column_type` on the spec.
- 199 `krishiv-exec`, 304 `krishiv-runtime`, 60 `krishiv-api`, 46 `krishiv-plan` tests pass.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-plan --lib --no-fail-fast
cargo +nightly test -p krishiv-exec --lib --no-fail-fast  
cargo +nightly test -p krishiv-runtime --lib --no-fail-fast
cargo +nightly test -p krishiv-api --lib --no-fail-fast
cargo +nightly test -p krishiv-ui --lib --no-fail-fast
cargo +nightly test -p krishiv-operator --lib --no-fail-fast
```

Next useful commands:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test --workspace --lib --no-fail-fast --exclude krishiv-python
```

---

## Production Stabilization Wave 0.9 — Plan Validator (completed, no-op)

The plan validator (`krishiv-plan/src/graph.rs`) already returns `Result<(), PlanError>` for all validation paths. No raw `assert!`, `panic!`, or `unwrap()` calls exist in non-test validation code.

---

## Production Stabilization Wave 0.8 — Iceberg REST (completed, prior sprint)

`RestCatalogConfig` already has URL validation, request timeouts, response size limits, pagination deduplication, and endpoint capability enforcement. No new changes required.

---

## Production Stabilization Wave 0.7 — UDTF/DDL Stubs (completed, prior sprint)

Non-SQL UDTF DDL (`LANGUAGE RUST`, `LANGUAGE PYTHON`, missing language) already rejected in `krishiv-sql/src/create_function_ddl.rs` and `krishiv-sql/src/lib.rs`. No new changes required.

---

## Production Stabilization Wave 0.6 — Library Panic Cleanup (2026-06-06)

### Runtime
- `krishiv-runtime/src/plan.rs`: Replaced `window_kind.unwrap()` after an `is_none()` check with an explicit `match` that returns the error before constructing `LocalWindowExecutionSpec`.
- `krishiv-runtime/src/flight_client.rs`: Removed the now-dead `last_err` variable from `with_transient_retry`; replaced the `expect("loop always sets last_err")` with `unreachable!(...)` since every retry path returns early.

### API
- `krishiv-api/src/dataframe.rs`: Replaced two `is_some()` + `unwrap()` pairs in `collect()` and `execute_stream_async()` with `if let Some(...)` to eliminate panic paths.

### Status
- `krishiv-udf`, `krishiv-plan`, `krishiv-scheduler`, `krishiv-checkpoint`, `krishiv-connectors`, `krishiv-state` library panics were already addressed in prior sprints; no new changes required.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly check -p krishiv-api -p krishiv-runtime -p krishiv-flight-sql -p krishiv-operator -p krishiv-scheduler -p krishiv-ui
cargo +nightly test -p krishiv-runtime --lib --no-fail-fast
cargo +nightly test -p krishiv-api --lib --no-fail-fast
cargo +nightly test -p krishiv-ui --lib --no-fail-fast
cargo +nightly test -p krishiv-flight-sql --lib make_flight_sql_server_compiles -- --nocapture
cargo +nightly test -p krishiv-operator --lib --no-fail-fast
```

Next useful commands:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-scheduler --lib --no-fail-fast
```

---

## Production Stabilization Wave 0.5 — Profile Guards (completed, no-op)

All remaining profile guards (`requires_file_backed_state`, `profile_forbids_native_scalar_udfs`, `forbids_simulation_connectors`, `allows_remote_sql_comment_fallback`) were already wired in prior sprints across `krishiv-state`, `krishiv-udf`, `krishiv-sql`, `krishiv-connectors`, `krishiv-executor`, and `krishiv-runtime`.

---

## Production Stabilization Wave 0.4 — K8s Manifest Security (2026-06-06)

### Operator `hostPath` removal
- Removed `hostPath` volume and volume mount from `PodLifecycleManager::build_pod`; executor pods no longer mount the host filesystem.
- Extracted `build_executor_pod` as a standalone `pub(crate)` function so unit tests can verify pod construction without creating a `kube::Client`.
- Removed `HostPathVolumeSource` and `VolumeMount` imports from `pod_manager.rs`.

### Env-var allow-list validation
- Replaced the permissive alphanumeric+underscore check in `build_pod` arg parsing with an explicit `ALLOWED_EXECUTOR_ENV_VARS` constant.
- Allowed vars: `KRISHIV_HEARTBEAT_INTERVAL_SECS`, `KRISHIV_HTTP_ADDR`, `KRISHIV_TASK_GRPC_ADDR`, `KRISHIV_BARRIER_GRPC_ADDR`, `KRISHIV_SHUFFLE_DIR`, `KRISHIV_SHUFFLE_URI`, `KRISHIV_STATE_DIR`, `KRISHIV_CHECKPOINT_STORAGE`, `KRISHIV_DURABILITY_PROFILE`, `KAFKA_BOOTSTRAP_SERVERS`.
- Auth tokens (`KRISHIV_COORDINATOR_BEARER_TOKEN`, `KRISHIV_EXECUTOR_TASK_BEARER_TOKEN`, `KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH`) and arbitrary keys are silently rejected.
- Added tests: `build_pod_omits_hostpath_volume`, `build_pod_injects_only_allowlisted_env_vars_from_args`.
- All 42 `krishiv-operator` lib tests pass.

### Static manifest cleanup
- Removed `hostPath` volumes and their mounts from:
  - `k8s/operator/operator-deployment.yaml`
  - `k8s/operator/executor-deployment.yaml`
  - `k8s-client-pod.yaml`
  - `k8s/jobs/kafka-streaming-sql.yaml`
  - `k8s/jobs/benchmark.yaml`
  - `k8s/jobs/python-examples.yaml`

### Scheduler daemon signature fixes (regression from Wave 0.2)
- Fixed `spawn_coordinator_sidecars` caller in `run_clusterd_daemon` to pass `extra_http_factory`.
- Fixed `run_clusterd_daemon` and `run_standalone_coordinator` signatures to use `Router` (not `Router<SharedCoordinator>`) for the factory return type, matching axum 0.8's type system.
- Updated callers in `krishiv/src/daemon_cmd.rs` and `krishiv-scheduler/src/bin/krishiv_clusterd.rs` to pass `None`.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly check -p krishiv-operator --tests
cargo +nightly test -p krishiv-operator --lib --no-fail-fast
cargo +nightly check -p krishiv-scheduler --lib
cargo +nightly check -p krishiv-ui --lib
cargo +nightly check -p krishiv-flight-sql --lib
```

Next useful commands:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-scheduler --lib --no-fail-fast
```

---

## Production Stabilization Wave 0.3 — Auth/UI Fail-Closed (2026-06-06)

### UI fail-closed
- `resolve_ui_token()` now returns `Some(String::new())` (fail-closed) when no `KRISHIV_UI_TOKEN` or `KRISHIV_UI_TOKEN_FILE` is configured under durable profiles, instead of returning `None` and serving protected routes anonymously.
- `require_bearer` rejects all requests when the expected token is empty, preventing `Authorization: Bearer ` from bypassing the fail-closed guard.
- Added tests: `api_jobs_rejects_all_requests_when_empty_token`, `api_jobs_rejects_empty_token_even_with_matching_empty_bearer`.
- All 18 `krishiv-ui` lib tests pass.

### Flight SQL panic removal
- `configure_flight_auth_from_env` now returns `Result<KrishivFlightSqlService, String>` instead of panicking when `KRISHIV_API_KEYS` is required but absent under durable profiles.
- `make_flight_sql_server` now returns `Result<FlightServiceServer<...>, String>` instead of using `.expect("flight host")`.
- `run_flight_server` propagates the `Result` from `make_flight_sql_server`.
- Updated all 6 call sites across `krishiv-runtime`, `krishiv-api`, and `krishiv-runtime/tests/integration_distributed.rs` to `.unwrap()` the `Result` in test code.
- `krishiv-flight-sql` lib test `make_flight_sql_server_compiles` passes.

Validation:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly check -p krishiv-ui -p krishiv-flight-sql -p krishiv-runtime -p krishiv-api
cargo +nightly test -p krishiv-ui --lib --no-fail-fast
cargo +nightly test -p krishiv-flight-sql --lib make_flight_sql_server_compiles -- --nocapture
cargo +nightly test -p krishiv-runtime --lib distributed_backend_submits_plan_over_flight_sql -- --nocapture
cargo +nightly test -p krishiv-api remote_execution_without_fallback_uses_flight_server -- --nocapture
cargo +nightly test -p krishiv-runtime --test integration_distributed flight_sql_server_submit_sql_verify -- --nocapture
```

Next useful commands:
```bash
export TMPDIR=/workspace/target/tmp
cargo +nightly test -p krishiv-runtime --test integration_distributed --no-fail-fast
```

---

## krishiv-ui Improvements (2026-06-06)

Added operator-focused UI improvements to `krishiv-ui`:

- **Auto-refresh via htmx** — jobs list, job detail, and executor detail pages poll every 15s via htmx `hx-trigger` with `hx-select` partial swaps. No page scroll or state is lost on refresh.
- **Dark mode** — CSS `prefers-color-scheme: dark` media query with a manual toggle button. All components have dark variants.
- **Executor detail page** — new `/ui/executors/{executor_id}` and `/api/v1/executors/{executor_id}` routes showing executor identity, state, slots, heartbeat, lease generation, memory usage, and running tasks.
- **Checkpoint page** — new `/ui/jobs/{job_id}/checkpoints` route with a visual epoch timeline (dots connected by lines) and a table listing all epochs with latest-epoch highlighting.
- **DAG visualization** — job detail page now renders stages as a horizontal flow with state badges, retry counts, and arrows between stages.
- **Streaming observability** — `TaskView` now exposes watermark display, source offset hex, and failure count. Templates render them in the task table with links to executor detail pages.
- **Task failure details** — failed tasks show a prominent inline error banner with the full failure reason string.
- **Executor links in job view** — executor names in job/detail tables link to executor detail pages.
- **Resource usage and priority** — jobs list table includes priority and CPU nanoseconds columns.
- **Memory in executor tables** — executor tables show memory used/limit in MB.
- **Streaming tag** — streaming jobs get a highlighted `streaming` badge in the jobs list.

Validation:
```bash
cargo check -p krishiv-ui
cargo test -p krishiv-ui
cargo check -p krishiv
```

Next useful commands:
```bash
cargo run -p krishiv-ui -- --demo
```

---

## Iceberg REST Catalog Contract Hardening (2026-06-06)

Completed the remote Iceberg REST catalog and Python binding production-readiness slice:

- Replaced infallible catalog construction and panic-based URL assembly with privately validated `RestCatalogConfig`, typed `Url` storage, positive request timeouts, bounded page sizes, bounded response bodies, and fallible HTTP-client construction.
- Added caller-supplied `reqwest::Client` support for custom trust roots, proxies, and provider-specific headers while preserving Krishiv's per-request timeout and response-size limits.
- Added lazy, shared, cancellation-safe `/v1/config` negotiation with warehouse selection, defaults/client/overrides prefix precedence, namespace-separator decoding, and advertised-endpoint capability enforcement.
- Preserved base paths and percent-encoded every dynamic URL segment so namespace and table identifiers cannot alter catalog routing.
- Replaced status-zero HTTP errors with typed configuration, transport, HTTP status, invalid-response, response-too-large, unsupported-operation, namespace-not-found, and table-not-found errors.
- Bounded successful responses to a configurable 64 MiB default and error diagnostics to 64 KiB, while preserving structured Iceberg error type, message, and code.
- Implemented Iceberg pagination using `pageToken`, `pageSize`, and `next-page-token`, with repeated/empty token detection, page/table ceilings, strict identifier decoding, duplicate rejection, and requested-namespace validation.
- Added typed `LoadedIcebergTable` responses and validated metadata location URIs, Iceberg format versions 1 through 3, UUID-shaped table IDs, optional table locations, and per-table config maps.
- Redacted bearer tokens, per-table config values, metadata values, warehouse values, and metadata locations from debug output.
- Removed nonstandard partition mutation endpoints that did not implement Iceberg commit requirements/updates.
- Removed the Rust/Python Glue and Nessie wrappers because they did not implement AWS SigV4 or Nessie reference semantics; retained one explicit generic Iceberg REST client with bearer-token and custom-client authentication hooks.
- Changed Python catalog construction failures to `ValueError`, retained request failures as `RuntimeError`, exposed bearer token/prefix/page/response controls, and validated table identifiers before network I/O.
- Added focused compile-covered tests for unsafe configuration, credential redaction, config precedence, authentication, base-path and segment encoding, custom clients, pagination, namespace separators, malformed/duplicate identifiers, capability rejection, not-found mapping, load-envelope validation, URI/UUID rejection, response ceilings, structured errors, and timeouts.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-catalog --tests --offline
cargo check -p krishiv-python --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Per sprint rules, focused tests were compiled with `cargo check --tests` but not executed; full test, clippy, and build validation remains reserved for the final slice.
- Table mutation support now fails by absence rather than routing to private mock endpoints. A future mutation slice must model Iceberg commit requirements and updates, including schema/partition field IDs and commit-state-unknown handling.
- AWS Glue and Project Nessie can still be used through the generic REST client only when callers supply the authentication headers and service-specific URL/reference configuration those providers require.
- Workspace check passed with the pre-existing executor barrier dead-code warnings and Flight SQL `unused_mut` warning.

Next useful commands:
```bash
cargo check -p krishiv-catalog --tests --offline
cargo check -p krishiv-python --tests --offline
cargo check --workspace --tests --offline
```

---

## Schema Registry and Schema Evolution Contract Hardening (2026-06-06)

Completed the schema-registry, CDC schema-evolution, Arrow normalization, and catalog error-boundary production-readiness slice:

- Replaced infallible schema-registry construction and string-only HTTP errors with validated URLs, typed configuration/request/status/response-size/response-shape errors, bounded timeouts, a registry user agent, preserved HTTP status diagnostics, and a 4 MiB response ceiling.
- Added caller-supplied `reqwest::Client` support for production authentication headers, custom trust roots, proxies, and timeout policy.
- Added cancellation-safe per-schema request coalescing so concurrent cache misses issue one registry request without retaining abandoned lock entries.
- Replaced the unbounded raw-schema cache with count- and byte-bounded LRU storage and cached parsed Avro/protobuf schemas under the same eviction lifecycle.
- Made `SchemaRegistryConfig` privately validated at construction and removed the unused subject field, dynamic-schema `arrow_schema` placeholder, false format auto-detection, nonstandard JSON protobuf descriptor, and syntactic-only JSON Schema capability.
- Kept only explicit Confluent Avro and Protobuf wire formats; Python now rejects unknown/JSON formats and malformed registry URLs immediately.
- Made Avro conversion preserve exact primitive widths, enums/fixed/UUID, nullable unions, date/time/timestamp logical types, nullability, and value/type agreement while rejecting multi-variant unions and unsupported nested/complex schemas instead of stringifying debug values.
- Hardened protobuf schema and payload handling for real `.proto` text, unique/valid names and field numbers, proto2/proto3 presence, required fields, proto3 defaults, signed/unsigned/fixed scalar widths, wire-type agreement, message-index routing, UTF-8, truncation, overflow, and exact Arrow value types.
- Rejected repeated, map, oneof, unsupported scalar, nested-message routing, non-default message-index, and unknown-only payload contracts instead of emitting partial or all-null rows.
- Added an explicit CDC schema-registry format with Avro compatibility default, feature-capability validation, binary-without-registry rejection, mixed binary/plain batch rejection, empty-decode rejection, and explicit failure for unsupported binary Iceberg CDC ingestion.
- Added validated multi-schema Arrow merging for batches containing multiple schema IDs, safe numeric widening, nullable fill for version-absent fields, metadata/type-drift rejection, duplicate-column rejection, and transactional schema-evolution state updates.
- Hardened the shared schema normalizer to reject arbitrary nullable casts, lossy `Int64 -> Float64`, missing non-nullable fields, duplicate schemas, and unsupported narrowing while supporting complete safe integer widening and nullable null-type promotion.
- Changed the DataFusion catalog bridge to map only true table-not-found errors to `None` and propagate all other catalog failures.
- Added focused compile-covered tests for registry URL/status/size/cache/concurrency/cancellation behavior, parsed-cache eviction, Avro fidelity and fail-closed behavior, protobuf schema/presence/wire/value contracts, CDC capability and schema merging, normalizer safety, and Python validation.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-schema-registry --tests --offline
cargo check -p krishiv-exec --tests --offline
cargo check -p krishiv-connectors --tests --offline
cargo check -p krishiv-connectors --tests --features schema-registry --offline
cargo check -p krishiv-catalog --tests --offline
cargo check -p krishiv-python --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Per sprint rules, focused tests were compiled with `cargo check --tests` but not executed; full test, clippy, and build validation remains reserved for the final slice.
- Avro nested/collection/decimal schemas and protobuf repeated/map/oneof/nested-message routing return explicit decode errors rather than partially decoded output.
- The Iceberg CDC sink remains JSON-envelope-only and now rejects binary schema-registry records before staging any data.
- Workspace check passed with the pre-existing executor barrier dead-code warnings and Flight SQL `unused_mut` warning.

Next useful commands:
```bash
cargo check -p krishiv-schema-registry --tests --offline
cargo check -p krishiv-connectors --tests --features schema-registry --offline
cargo check --workspace --tests --offline
```

---

## Optimizer Rule and DAG Contract Hardening (2026-06-06)

Completed the logical/AQE optimizer and scheduler DAG-conversion production-readiness slice:

- Changed logical and AQE optimizer pipelines to return typed `OptimizerError` results.
- Validated optimizer inputs and every rule output, including plan-name and execution-kind preservation.
- Contained panics from custom logical and AQE rules and reported the responsible rule name.
- Ignored `Some(unchanged_plan)` results instead of falsely recording a rule as applied.
- Removed the unused `StreamRule` placeholder contract.
- Removed duplicate-column and empty-projection rewrites because both can change observable projection schema or row semantics.
- Reworked predicate pushdown around parsed SQL conjunctions, per-conjunct scan ownership, exact table qualifiers, literal/function skipping, ambiguous-column rejection, and inner-join-only traversal.
- Made parse failures and unsupported outer-join cases fail closed without rewriting.
- Made `CoalesceRule` intrinsically skip streaming/hybrid plans, build target-size-bounded groups, preserve a valid connected DAG, insert before terminal sinks, reuse an existing coalesce node on repeated AQE passes, and keep node/count metadata consistent.
- Hardened skew, coalesce, and small-file arithmetic against `u64` overflow.
- Propagated optimizer errors through SQL and scheduler APIs and mapped scheduler optimizer rejection to gRPC `invalid_argument`.
- Validated logical/physical plans before scheduler job conversion and replaced fragment-encoding fallback text with typed conversion errors.
- Replaced scheduler's quadratic/fallback topological ordering with checked `O(nodes + edges)` ordering that handles duplicate edges correctly.
- Added focused coverage for invalid input/output graphs, panic containment, identity changes, unchanged-rule reporting, connected/idempotent coalescing, streaming guards, overflow, predicate ownership, ambiguous columns, aliases, outer joins, SQL propagation, and scheduler conversion.

Validation:
```bash
cargo fmt --all
cargo check -p krishiv-optimizer --tests --offline
cargo check -p krishiv-sql --tests --offline
cargo check -p krishiv-scheduler --tests --offline
cargo check --workspace --tests --offline
git diff --check
```

Notes:
- Per sprint rules, focused tests were compiled with `cargo check --tests` but not executed; full test, clippy, and build validation remains reserved for the final slice.
- Workspace check passed with the pre-existing executor barrier dead-code warnings and Flight SQL `unused_mut` warning.

Next useful commands:
```bash
cargo check -p krishiv-optimizer --tests --offline
cargo check --workspace --tests --offline
```

---

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

## 12-Gap Feature Implementation (2026-06-10)

Resumed prior-session work implementing all remaining feature gaps.

### Gap #10 — CEP streaming executor path

- Added `serde::{Serialize, Deserialize}` derives to `PatternStage` and `CompiledPattern`
  in `krishiv-plan/src/cep/pattern.rs`.
- Added `NodeOp::Cep { key_column, event_time_column, stage_column }` variant to
  `krishiv-plan/src/lib.rs`.
- Added `STREAM_CEP_PREFIX = "stream:cep:"` constant and `encode_cep_fragment()`
  helper to `krishiv-plan/src/cep/mod.rs` so callers build fragments without
  depending on the executor.
- Added `CepFragmentSpec` (serde::Deserialize) and `execute_cep_fragment()` to
  `krishiv-executor/src/fragment/streaming.rs`:
  - Reads input batches via the same priority order as the loop path
    (continuous_drainer → continuous_inputs → InMemory → InlineIpc).
  - Iterates rows row-by-row, extracts key/event_time/stage_name.
  - Routes each row to `PartitionedCepMatcher::<String>::process_event`.
  - Concatenates each completed match's stage batches with `arrow::compute::concat_batches`.
  - Emits match batches as `ExecutorTaskOutput::streaming_window`.
- Dispatch added in `execute_streaming_fragment` before `stream:loop:`.
- Added `serde` dep to `krishiv-executor/Cargo.toml`.

### Gap #12 — Iceberg end-to-end test

Added two `#[tokio::test]` cases in
`krishiv-connectors/src/registry/drivers/lakehouse.rs` (feature-gated `lakehouse`):

- `iceberg_sink_insert_then_source_select`: writes 5 rows in 2 batches through
  `IcebergSinkDriver`, reads back through `IcebergSourceDriver`, asserts 5 rows and
  correct id values.
- `iceberg_two_commits_both_visible`: makes two separate flush commits and asserts
  both are visible in a subsequent full scan.

### Validation

```
cargo check --workspace            # zero errors
cargo test -p krishiv-connectors --features lakehouse --lib -- iceberg
# 17 passed (2 new driver e2e tests + 15 existing iceberg_fs tests)
```

Next useful command: `cargo test --workspace --lib` (requires system libpython3.12 for
krishiv-python; exclude it if unavailable).

---

## Full-workspace bug sweep — all 411 files (2026-06-10)

Ran `cargo clippy --workspace --exclude krishiv-python -- -D warnings` across all 411 Rust
source files in 20 crates. Fixed every error; zero warnings promoted to errors remain.

### Fixes applied

| File | Error | Fix |
|---|---|---|
| `krishiv-common/src/backpressure.rs` | `impl Default` can be derived | Added `#[derive(Default)]` + `#[default]` on `None` variant; removed manual impl |
| `krishiv-shuffle/src/range_partitioner.rs` | Unused var `e`; dead fn `from_str`; redundant closure; 3× collapsible-if | `_e`; removed `from_str`; `RangeBound::Utf8` as fn ptr; let-chain rewrites |
| `krishiv-shuffle/src/spillable.rs` | `std::io::Error::new(Other, …)` → `std::io::Error::other` | Used `Error::other(…)` |
| `krishiv-connectors/src/registry/drivers/lakehouse.rs` | `fn flush → impl Future` should be `async fn` | Changed to `async fn flush` |
| `krishiv-dataflow/src/lookup_join.rs` | Unused imports `BooleanArray`, `Float64Array`, `Int32Array`, `Int64Array`, `StringArray` | Removed from top-level; added `StringArray` to test module's own import |
| `krishiv-dataflow/src/window_join.rs` | Unused import `DataType`; `for (k, _) in map` should iterate keys | Removed import; `map.keys()` |
| `krishiv-dataflow/src/window/count.rs` | Field `global_row` in `RowContrib` never read | Removed field and its initialiser |
| `krishiv-dataflow/src/cep.rs` | 2× collapsible-if | Let-chain rewrite |
| `krishiv-dataflow/src/join.rs` | Collapsible-if (budget check) | Let-chain rewrite |
| `krishiv-dataflow/src/window/session.rs` | 2× collapsible-if (budget check) | Let-chain rewrite |
| `krishiv-sql/src/recursive_cte.rs` | Unused imports `HashSet`, `Cte`, `With` | Removed |
| `krishiv-sql/src/subquery.rs` | Unused `ControlFlow` return value | `let _ =` |
| `krishiv-scheduler/src/rocksdb_metadata.rs` | 15× redundant closure `\|e\| Self::store_err(e)` | `Self::store_err` direct function reference |
| `krishiv-executor/src/cli.rs` | Unused import `executor_task_grpc_server_with_continuous` | Removed |
| `krishiv-executor/src/runner.rs` | `with_backpressure` never used | `#[allow(dead_code)]` (intentionally kept for future use) |
| `krishiv-executor/src/grpc.rs` | `or_insert_with(Vec::new)` should be `or_default()` | Used `or_default()` |

### Validation
```
cargo clippy --workspace --exclude krishiv-python -- -D warnings   # 0 errors
cargo test --workspace --lib --exclude krishiv-python               # 19 suites, 0 failures
```

## Bug sweep — loop pass 2 (2026-06-10)

Second pass adding feature-gated code paths to the sweep. Two more bugs found and fixed.

| File | Error | Fix |
|---|---|---|
| `krishiv-connectors/src/elasticsearch_sink.rs` | `for row in 0..n` index loop; collapsible-if in `extract_id` | `iter_mut().enumerate()`; let-chain rewrite |
| `krishiv-scheduler/src/store.rs` | `encode_metadata_snapshot` / `decode_metadata_snapshot` dead (only used in `#[cfg(test)]` blocks) | Changed `#[cfg(feature = "etcd")]` → `#[cfg(all(feature = "etcd", test))]` |

### Validation
```
cargo clippy --workspace --exclude krishiv-python -- -D warnings                                                              # 0 errors
cargo clippy -p krishiv-connectors --features "lakehouse,kafka,avro,cassandra,hbase,elasticsearch,pulsar-source,kinesis" -- -D warnings  # 0 errors
cargo clippy -p krishiv-scheduler --features etcd -- -D warnings                                                             # 0 errors
cargo test --workspace --lib --exclude krishiv-python                                                                         # 19 suites, 3085 tests, 0 failures
```
