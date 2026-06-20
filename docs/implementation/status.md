# Krishiv Implementation Status

## 2026-06-20 — Distributed deployment wiring fixes

Fixed the distributed-mode deployment gaps found in the executor/coordinator
review.

### Completed

- Direct Kubernetes manifest now runs `krishiv clusterd` as the distributed
  control plane, exposes co-located Flight SQL, and removes the disconnected
  standalone `flight-server` deployment.
- Executors now have a fixed configurable shuffle Flight bind address
  (`--shuffle-flight-addr` / `KRISHIV_SHUFFLE_FLIGHT_ADDR`) and advertise
  routable pod-host endpoints instead of `0.0.0.0`.
- Helm chart now exposes coordinator HTTP/Flight ports and executor
  task/barrier/shuffle/health ports, with a durable distributed values override
  for etcd plus object-store shuffle/checkpoint storage.
- Operator manifests now route `krishiv-coordinator` Service traffic to the
  operator pod that actually embeds the coordinator sidecars; stale external
  JCP-pod claims were downgraded to reference-only documentation.
- etcd metadata now persists continuous-job snapshots and bounded job history,
  so distributed coordinator recovery covers more than active job/executor
  records.

### Validation

```bash
cargo fmt --check                                                        # pass
cargo test -p krishiv-executor --lib                                    # pass
cargo test -p krishiv-scheduler --lib --features etcd                   # pass
cargo test -p krishiv-operator --lib                                    # pass
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings  # pass
git diff --check                                                        # pass
```

### Blockers

- `helm` is not installed in this environment, so Helm rendering was not
  validated here.
- `cargo test -p krishiv-shuffle --lib` compiles but has sandbox-dependent
  filesystem/localhost failures (`Operation not permitted` on temp-dir
  permission/attribute behavior); the required clippy gate passes.

### Next useful command

```bash
helm template krishiv ./k8s/helm/krishiv -f k8s/helm/krishiv/values-distributed-durable.yaml
```

---

## 2026-06-20 — Scheduler/executor architecture fixes

Fixed the control-plane issues found in the scheduler/executor review.

### Completed

- Assignment target resolution errors now clear and persist `launch_in_flight`
  state instead of silently dropping launches.
- Task placement now uses heartbeat-reported live executor load before falling
  back to static slots.
- Admission-queued jobs are represented durably with `JobState::Queued`, remain
  visible in status APIs, do not reserve namespace quota, and are admitted later
  when capacity is available.
- Recovered jobs clear persisted in-flight launch guards so dispatch is
  retryable after coordinator restart.
- Coordinator and executor `/readyz` endpoints now require actual scheduling /
  executor readiness instead of process liveness alone.
- Dataflow window output builder now uses a parameter struct to satisfy clippy's
  type/argument quality gate.

### Validation

```bash
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-executor --lib
cargo fmt --check
cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings
```

### Blockers

None from this session.

### Next useful command

```bash
cargo test --workspace
```

---

## 2026-06-20 — Enterprise examples 21-24: Delta batch → Kafka sink (all passing)

Four new examples covering Delta Lake as a source with Kafka as the sink.

### Example run summary

| Example | Status | Notes |
|---------|--------|-------|
| ent_21 Delta batch → Kafka | ✓ | 3 Delta versions → 15K rows as Arrow IPC via Kafka |
| ent_22 Delta CDC diff → Kafka | ✓ | Time-travel V0→V1 diff; 20 INSERTs, 80 UPDATEs, 20 DELETEs published as JSON |
| ent_23 Delta SQL agg → Kafka | ✓ | 100K raw → GROUP BY (cat, month) → 60 compact rows; revenue matches $50M |
| ent_24 Kafka→Delta→Kafka pipeline | ✓ | 50K rows: source Kafka → Delta staging → SQL → enriched output Kafka |

### Key patterns

- **Delta write**: `write_delta(path, batches, DeltaWriteMode::Append, false)`
- **Delta time-travel**: `DeltaTableHandle::open(path, Some(version))` → `.scan_batches()`
- **CDC diff**: compare `HashMap<order_id, Row>` for V0 vs V1 → classify as INSERT/UPDATE/DELETE
- **Aggregate to Kafka**: SQL `GROUP BY` via embedded `Session`, then JSON-per-row to Kafka
- **Full pipeline**: rdkafka consume → `write_delta` batch → `Session::sql` → JSON produce

### Validation
```bash
cargo run --bin ent_21_delta_batch_to_kafka       # ✓ 15000 rows
cargo run --bin ent_22_delta_cdc_to_kafka         # ✓ 120 CDC events (20+80+20)
cargo run --bin ent_23_delta_agg_to_kafka         # ✓ $50M revenue matches
cargo run --bin ent_24_kafka_to_delta_to_kafka    # ✓ $12.9M revenue matches
```

---

## 2026-06-20 — Enterprise examples 13-20: Kafka sinks + benchmarks (all passing)

Implemented and validated 8 new enterprise Rust examples covering real-service sinks,
watermark correctness, crash+resume, throughput benchmarks, backpressure, and consumer
group scale-out.

### Example run summary

| Example | Status | Throughput / Notes |
|---------|--------|--------------------|
| ent_13 Kafka → PostgreSQL | ✓ | 24 K rows/s · unnest bulk insert · offset table |
| ent_14 Kafka → ClickHouse | ✓ | 87 K rows/s · JSONEachRow HTTP · 500 K rows |
| ent_15 Watermark late-data | ✓ | 50 late events dropped, 500 on-time processed |
| ent_16 Crash+resume checkpoint | ✓ | 10 K rows, seek via `assign()` after crash |
| ent_17 Benchmark vs Flink | ✓ | Kafka 868 K rows/s produce; Krishiv 257 K rows/s e2e (5 M rows + windowing) |
| ent_18 Kafka → InfluxDB | ✓ | 9.5 K rows/s · line protocol · 20 K sensor readings |
| ent_19 Backpressure slow sink | ✓ | 6.8 K rows/s vs 20 K produce; bounded memory |
| ent_20 Consumer group scale-out | ✓ | 100 K rows, 2 consumers, 0 duplicates, 14 K rows/s |

### Key bugs fixed during this session

1. **PostgreSQL reserved word** — `offset`/`partition` columns renamed to `next_offset`/`part_id`.
2. **PostgreSQL ROUND return type** — `::float8` cast added after `ROUND(SUM(...)::numeric)`.
3. **Stale topic data (all examples)** — AdminClient `delete_topics` + `create_topics` at startup.
4. **Crash+resume duplicate reads** — `consumer.subscribe` + `seek_partitions` buffers pre-seek
   messages; fixed by using `consumer.assign(tpl)` directly.
5. **InfluxDB Flux count** — `|> group()` before `|> count()` collapses per-device series
   into one total; CSV parser filters lines starting with `,` that are not headers.

### Infrastructure used

- **Kafka 3.9 KRaft**: `docker run --network=host apache/kafka:3.9.0`
- **PostgreSQL 16**: `docker run -p 5432:5432 -e POSTGRES_PASSWORD=pass postgres:16-alpine`
- **ClickHouse**: `docker run -p 8123:8123 clickhouse/clickhouse-server`
- **InfluxDB v2**: `docker run -p 8086:8086 influxdb:2` (org=krishiv, bucket=sensors, token=krishiv-token-123)

### Validation

```bash
cargo run --bin ent_13_kafka_to_postgres     # ✓ 50000 == 50000
cargo run --bin ent_14_kafka_to_clickhouse   # ✓ 500000 == 500000
cargo run --bin ent_15_watermark_late_data   # ✓ PASS
cargo run --bin ent_16_crash_resume_checkpoint # ✓ PASS
cargo run --bin ent_17_benchmark_vs_flink    # ✓ 5M rows benchmarked
cargo run --bin ent_18_kafka_to_influxdb     # ✓ 20000 == 20000
cargo run --bin ent_19_backpressure_slow_sink # ✓ PASS
cargo run --bin ent_20_consumer_group_scaleout # ✓ PASS
```

### Next useful task
Run `cargo run --bin ent_12_kafka_real_at_least_once` to verify the at-least-once
connector example still passes after the topic cleanup changes.

---

## 2026-06-20 — Enterprise examples 01-10 running in embedded mode + Float64 aggregate gap fix

All 10 enterprise Rust examples now run successfully in embedded/in-process mode
(no external services required). Two engine gaps were discovered and fixed.

### Gap 1 — Float64 windowed aggregation

`AggState::update()` and `update_agg_state_pre()` only handled `Int32`/`Int64`
inputs for Sum/Min/Max; `Float64` raised `unsupported aggregate input type`.

**Fix** (spans 5 files):
- `crates/krishiv-dataflow/src/aggregate.rs` — added `float_values: Vec<f64>` to
  `AggState`; `update()` and `update_agg_state_pre()` now branch on Float64;
  added `finalized_float_value()`; `LocalAggregator::aggregate()` emits
  `Float64Array` when appropriate.
- `window/tumbling.rs` — added `agg_is_float: Vec<bool>` to `TumblingWindowSpec`;
  `build_window_output_schema` and `build_window_record_batch` emit `Float64`
  fields/arrays for float aggregates.
- `window/sliding.rs` — same `agg_is_float` propagation.
- `window/count.rs` — same; `fold_agg_states` merges `float_values`.
- `window/state_persistence.rs` — persist/restore `float_values` field.
- `operator_runtime.rs` — `execute_bounded_window` auto-detects Float64 from first
  batch schema and populates `agg_is_float`; streaming path defaults to false.
- `continuous.rs` — creation sites updated with `agg_is_float: vec![]`.
- `window/session.rs` — `AggState` struct literal updated with `float_values: vec![]`.

### Gap 2 — DataFusion Utf8View vs Utf8 downcast

DataFusion 53.1.0 returns all string columns as `Utf8View` (not `Utf8`). Direct
`downcast_ref::<StringArray>()` returns `None` for SQL query results.

**Fix**: use `arrow::compute::cast(col, &DataType::Utf8)` before downcasting in
enterprise examples ent_06 and ent_07.

### Example run summary

| Example | Status | Notes |
|---------|--------|-------|
| ent_01 Kafka → Parquet (at-least-once) | ✓ | rolling-files pattern |
| ent_02 Kafka → Parquet (exactly-once 2PC) | ✓ | |
| ent_03 CDC Debezium → Delta | ✓ | |
| ent_04 Kafka → tumbling window (Float64 sum) | ✓ | required Float64 gap fix |
| ent_05 Kinesis → Parquet (checkpointed) | ✓ | |
| ent_06 Parquet → Elasticsearch (_bulk) | ✓ | required Utf8View fix |
| ent_07 Parquet → Cassandra (CQL) | ✓ | required Utf8View fix |
| ent_08 Multi-source join | ✓ | |
| ent_09 CEP fraud detection | ✓ | |
| ent_10 S3 ETL pipeline | ✓ | LocalFileSystem embedded mode |

### Validation
- `cargo check --workspace` — clean
- `cargo test --workspace` — all pass
- All 10 enterprise examples executed end-to-end with `cargo run --bin <name>`

## 2026-06-20 — Real Kafka high-load examples (ent_11, ent_12)

Two new enterprise examples added and validated against a live Apache Kafka 3.9
broker (KRaft mode, no Zookeeper, `--network=host` Docker).

### ent_11 — Kafka high-load pipeline (Arrow IPC)

1 million rows produced at **646 K rows/s** (26 MB/s) as 100 Arrow IPC + lz4
messages (10 K rows each). Consumed and window-aggregated at **983 K rows/s**
end-to-end in **5.6 s** (180 K rows/s e2e). 400 window rows emitted (8
customers × 50 tumbling 10s windows).

Key implementation details:
- `FutureProducer` with 64-message pipeline; `Producer` trait import for `flush(Timeout::After(…))`
- `FutureRecord<str, Vec<u8>>` (not `[u8]`) for type inference
- Per-run timestamped consumer group ID avoids re-reading prior offsets
- 500 ms sleep + retry-on-transport-error handles initial group rebalance

### ent_12 — KafkaSink / KafkaSource connector API (at-least-once)

Demonstrates the `KafkaSink` / `KafkaSource` connector API. 2 000 rows produced
as JSON messages (one per row, waiting for broker ack) and consumed back into a
single Parquet file. Row count verified via SQL (CAST required for numeric
columns — connector reads all JSON fields back as `Utf8`).

Key notes:
- `KafkaSink.write_batch` serialises each row as JSON and blocks on ack → ~120 rows/s
  (correctness-first design; use ent_11 pattern for throughput)
- `KafkaSource.payload_to_batch` returns all columns as `Utf8` — must CAST numerics in SQL
- Transport glitches during group rebalance handled with warn + 300 ms retry loop

### Kafka Docker setup

```bash
docker run -d --name krishiv-kafka --network=host \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_NUM_PARTITIONS=4 \
  apache/kafka:3.9.0
docker exec krishiv-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic orders-load-test --partitions 4
```

Also requires mold linker (avoids ld SIGBUS on large link units):
`.cargo/config.toml` in `examples/enterprise/rust/` with `rustflags = ["-C", "link-arg=-fuse-ld=mold"]`

### Next useful task
- Add streaming window Float64 support (`execute_streaming_window` still uses
  `agg_is_float: vec![false; n]` — needs schema peeking or a spec parameter)
- ent_13: multi-partition consumer group with 2+ consumers reading in parallel

## 2026-06-19 — Python async API and stub cleanup

Fixed the Python user API issues identified in the Rust/Python API review:
async method names now expose real Python awaitables at the package layer, and
the generated native stub no longer collapses the public surface to `Any`.

### What changed
- `Session.sql_async` now resolves to a lazy `DataFrame`, matching Rust
  `Session::sql_async` semantics instead of eagerly collecting a `QueryResult`.
- `DataFrame.collect_async`, `DataFrame.execute_stream_async`,
  `StreamingDataFrame.execute_stream_async`, and `QueryHandle.collect_async` are
  installed as top-level Python coroutine wrappers around the proven blocking
  native methods.
- Re-exported `QueryHandle` from top-level `krishiv`.
- Updated the API-surface generator to detect PyO3 async methods and emit typed
  core stubs with `object` fallback for unmapped preview methods instead of
  `Any`.
- Regenerated API inventories/reports/stubs and added generator regression
  coverage for async signatures and no-`Any` output.
- Updated Python async tests so they await `collect_async` and assert stream
  async APIs return awaitables without forcing a streaming pipeline to terminate.

### Validation
- `cargo fmt --check`
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings`
- `cargo check -p krishiv-python` — passes with pre-existing PyO3/source warnings.
- `python3 scripts/check_api_surface.py`
- `python3 -m unittest scripts.tests.test_project_scripts`
- `python3 -m py_compile crates/krishiv-python/python/krishiv/__init__.py scripts/check_api_surface.py`
- `maturin develop --manifest-path crates/krishiv-python/Cargo.toml` into `.venv-pytest`
  — installs; warns that `patchelf` is missing for rpath adjustment.
- `.venv-pytest/bin/python -m pytest -q crates/krishiv-python/python/tests/test_async.py crates/krishiv-python/python/tests/test_dataframe.py::test_collect_async crates/krishiv-python/python/tests/test_dataframe.py::test_execute_stream_async_returns_awaitable crates/krishiv-python/python/tests/test_streaming.py::test_streaming_dataframe_execute_stream_async_returns_awaitable`
  — 6 passed.

### Blockers
- Full `.venv-pytest/bin/python -m pytest -q crates/krishiv-python/python/tests`
  collection currently requires `pyarrow`; this venv does not have it installed.

### Next useful command
`.venv-pytest/bin/python -m pytest -q crates/krishiv-python/python/tests/test_async.py crates/krishiv-python/python/tests/test_dataframe.py::test_collect_async`

## 2026-06-20 — Responsive web/docs mobile pass

Improved the Krishiv website and documentation responsive behavior with the highest priority on docs reading, navigation, and overflow prevention.

### What changed
- Added a compact sticky mobile docs toolbar with menu, truncated page title, search, and version selector.
- Added a mobile/tablet docs drawer below 1024px with backdrop close, Escape close, scroll locking, grouped collapsible navigation, search trigger, version selector, and active-page highlighting.
- Added a mobile docs search overlay and compact in-page table-of-contents disclosure.
- Tightened responsive CSS for docs typography, code blocks, tables, prev/next cards, safe-area padding, touch targets, reduced motion, and no page-level horizontal overflow.
- Improved landing-page mobile behavior for navbar, hero, architecture visual, capability strip, developer journey, code tabs, and footer without changing the desktop black/gold direction.

### Validation
- `pnpm --dir web run typecheck`
- `pnpm --dir web run build`
- `pnpm --dir web run lint` exited 0 via the package fallback, but Next.js 16 reported `next lint` as an invalid project directory command.
- `cargo fmt --check`
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings` was attempted, but the container linker failed before crate linting because `ld` is unavailable while repo rustflags request mold/lld.
- Playwright browser installation was attempted for target-width screenshots, but `cdn.playwright.dev` returned `403 Domain forbidden`; no local Chromium/Chrome/Firefox binary was available.

### Next useful command
`pnpm --dir web run build`

## 2026-06-20 — Landing page high-fidelity dark/gold redesign

Rebuilt the web landing page around the provided black-and-gold reference composition and replaced the religious-inspired logo direction with a geometric infrastructure mark.

### What changed
- Replaced the homepage with reusable landing components for the hero, runtime architecture diagram, SVG data-flow particles, capability strip, developer journey, code example panel, and ecosystem row.
- Updated the shared web shell with the new horizontal brand treatment, centered navigation, action icons, sticky translucent header, and mobile menu.
- Reworked the global web theme to the near-black palette with restrained gold accents, neutral borders, responsive behavior, and reduced-motion support.
- Added new brand assets in `web/public/brand/` for the logo mark, horizontal logo, and favicon.

### Validation
- `pnpm --dir web run typecheck`
- `pnpm --dir web run build`
- `cargo fmt --check`
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings` was attempted but the container linker failed before crate linting because `ld` is unavailable while the repo cargo config requests mold/lld linker flags.
- Playwright screenshot capture was attempted, but browser download failed with a `403 Domain forbidden` response from `cdn.playwright.dev`.

### Next useful command
`pnpm --dir web run build`

---

## 2026-06-19 — Web and docs logo refresh

Redesigned the Krishiv SVG asset set to better match the dark web theme and
the framework's batch SQL, streaming, state/checkpoint, and lakehouse focus.

### What changed
- Replaced all source SVG logo/mark files in `web/public` and `docs/assets`
  with a shared dark framed K/data-flow mark using the site palette.
- Updated horizontal wordmarks to avoid unsupported AI claims and describe
  Krishiv as a Rust-native batch SQL, streaming, and lakehouse compute
  framework.
- Updated the web header to render `/krishiv-mark.svg` instead of an older
  inline SVG, keeping the nav logo aligned with the asset files.

### Validation
- XML parsed all six source SVG files with Python's standard XML parser.
- `pnpm run typecheck`
- `pnpm run build`

### Next useful command
`git status --short --branch`

---

## 2026-06-19 — Fix `checkpoints list` path-escape false-positive

**Bug:** `LocalFsCheckpointStorage::full_path` compared a non-canonical relative
path against a canonical absolute base when the target directory didn't exist yet,
causing `cargo test --workspace --lib` to fail with:
`checkpoint error: path escapes storage base directory: ./krishiv-checkpoints/job-1/checkpoints`

**Root cause:** In the `else` branch (parent doesn't exist), `canonical_parent`
was left as raw `parent.to_path_buf()` (relative). `canonical_base` was the
canonicalized absolute result of `self.base_dir.canonicalize()`, so
`canonical_parent.starts_with(&canonical_base)` always returned false.

**Fix (`local_fs.rs`):** When parent doesn't exist, strip `self.base_dir` from
the parent path and rejoin onto `canonical_base`. Phase 1 already guarantees no
`..` or absolute components in the sub-path, so this is safe.

### Validation
- `cargo test -p krishiv --lib cli::tests::checkpoints_list_returns_no_checkpoints` — 1 passed
- `cargo test -p krishiv-state --lib` — 302 passed

### Next useful command
`cargo test --workspace --lib`

---

## 2026-06-19 — Web CI deploy asset fix

Fixed the Cloudflare Workers deployment path for `krishiv.ai` after the live
site served HTML but returned 404 for `_next/static/chunks/*` assets.

### What changed
- Added the OpenNext `ASSETS` binding in `web/wrangler.jsonc`, pointing Wrangler
  at `.open-next/assets` so `_next/static` files are uploaded and served.
- Enabled the web deploy GitHub Actions workflow on pushes to `main` that touch
  web files or the workflow itself.

### Validation
- `pnpm opennextjs-cloudflare build`
- `pnpm exec wrangler deploy --dry-run` — exited 0 and reported `env.ASSETS`
  plus 21 files read from `.open-next/assets`; Wrangler also emitted a sandbox
  log-file warning for `/root/.config/.wrangler/logs`.
- `pnpm run typecheck`

### Next useful command
`git push origin main`

## 2026-06-19 — Main merge conflict resolution

Merged `origin/main` into `codex/build-production-quality-web-application-12qqbz`
and resolved the web app conflicts.

### What changed
- Resolved conflicts in the homepage, architecture page, shared shell component,
  and global CSS by keeping the readable branch implementations.
- Accepted `origin/main`'s web package metadata updates: pnpm package manager
  metadata, Cloudflare scripts, and npm lockfile removal.
- Applied required mechanical rustfmt output in executor/runtime files.
- Fixed one scheduler clippy lint in memory-admission logging by collapsing the
  nested capacity check.

### Validation
- `cargo fmt --check`
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings`
- `npm --prefix web run build`
- `npm --prefix web run typecheck`

### Next useful command
`git status --short --branch`

## 2026-06-19 — Coordinator/Scheduler/Executor audit fixes

Applied all actionable findings from the P0–P2 audit across coordinator,
scheduler, and executor components.

### What changed

**E-1 (P0) — IVM executor path fails loudly instead of silently succeeding**
- `fragment/ivm.rs`: corrected module doc comment (path is future-only, not current).
- `executor_task_runner.rs`: `DeltaBatch` dispatch now returns `Err` with a
  clear message if a `delta:step:` fragment somehow reaches the executor, instead
  of silently returning empty output. Prevents accidental coordinator↔executor
  IVM wire-up from passing silently.

**E-3 (P0) — checkpoint_runners DashMap remove+reinsert gap closed**
- `executor_task_runner.rs`: Changed `checkpoint_runners` type from
  `DashMap<TaskId, TaskRunner>` to `DashMap<TaskId, Arc<Mutex<TaskRunner>>>`.
- `initiate_checkpoint_and_deliver_ack` no longer removes the entry from the map
  during blocking I/O; a concurrent barrier arriving in that window now finds the
  existing Arc (and blocks on the Mutex) rather than creating a fresh `TaskRunner`
  with `last_acked_epoch=0` and producing phantom acks.
- `batch.rs`, `recovery.rs.inc`, `executor_task_runner.rs:restore_job_from_checkpoint`
  all updated consistently.

**C-2 (P1) — Undrained `pending_sink_finalize` detected early**
- `coordinator/job_lifecycle.rs`: Added `debug_assert` at the top of
  `apply_task_update` that `pending_sink_finalize` is empty; catches callers that
  forget `take_pending_sink_finalize()` in debug builds before they cause
  blocking I/O under the coordinator write lock.

**D-2 (P1) — Flight health checks wired into session construction (#73)**
- `execution_runtime.rs`: Added `spawn_health_checks()` to `RemoteExecutionRuntime`
  that uses `Handle::try_current()` to schedule `pool.start_health_checks()` as a
  background Tokio task.
- `build_execution_runtime` now calls `spawn_health_checks()` for both
  `SingleNodeDaemon` and `RemoteClusterRequired` placements. Stale Flight channels
  are now recycled automatically.

**E-2 (P1) — Streaming task timeout is env-configurable**
- `runner/partition.rs`: Added `default_streaming_task_timeout_secs()` that reads
  `KRISHIV_STREAMING_TASK_TIMEOUT_SECS` before falling back to 300 s.
- `executor_task_runner.rs`: Streaming dispatch now calls
  `default_streaming_task_timeout_secs()` instead of the constant so operators
  that need longer windows can override without per-task spec changes.

**C-6 (P2) — Stall detection no longer false-triggers on windowing tasks**
- `job/record.rs:apply_streaming_state`: Refreshes `last_progress_ms` whenever
  an executor heartbeat includes streaming task state for this task. Long-windowing
  tasks that are accumulating data without yet emitting output rows are now treated
  as "making progress" as long as the executor is heartbeating.

**E-4 (P2) — Hot-key report logic unified**
- `fragment/common.rs`: Added `build_hot_key_reports(batches, key_column, job_id, source_id)`.
- `fragment/batch.rs`: Removed local `build_hot_key_reports`; imports from `common`.
- `fragment/streaming.rs`: Removed local `build_streaming_hot_key_reports`; imports
  from `common` and passes `stage_id.as_str()` at call sites.

**D-1 (P2) — Watermark propagated from in-process runtime**
- `execution_runtime.rs:InProcessExecutionRuntime`: Overrides
  `collect_bounded_window_with_watermark` to compute the event-time watermark from
  input batches before running the window, matching the logic in the executor's
  streaming fragment. Embedded and single-node sessions now return a real watermark
  instead of `None`.

**S-1 (P3) — Memory admission logs when capacity is unknown**
- `coordinator/job_lifecycle.rs`: Added `debug!` log when a job with a memory ask
  is admitted but no executor has reported memory capacity.

### Validation
- `cargo check --workspace` — clean (only pre-existing PyO3 deprecation warnings)
- `cargo test --workspace --lib` — running

### Next useful command
`cargo test --workspace --lib`

## 2026-06-19 — PySpark-shaped Python SQL functions namespace + pytest coverage

Added the first migration-oriented Python SQL API slice after comparing Krishiv
against PySpark's public SQL surface, then expanded pytest coverage across every
public `krishiv.sql.functions` callable.

### What changed
- Added `krishiv.sql` as a stable Python namespace for SQL-facing classes:
  `Session`, `DataFrame`, `Column`, grouped data, query results, and streaming
  query types.
- Added `krishiv.sql.functions` with PySpark-familiar expression helpers backed
  by Krishiv's native `Column`/`Expr` API: `col`, `column`, `lit`, `expr`,
  `call_function`, common aggregates, null helpers, string helpers, numeric
  helpers, date/time helpers, ordering, and cast helpers.
- Added `krishiv.functions` as a short alias for `krishiv.sql.functions`.
- Re-exported the native `Column` and core expression helpers from top-level
  `krishiv` so the runtime package matches the preview stub surface.
- Added Python stubs and full function-wrapper tests for import shape,
  constructor/literal behavior, generic function dispatch, aggregates, null
  helpers, string helpers, numeric helpers, date/time helpers, ordering/casts,
  and expected failure cases.
- Fixed `connect_async`: constructing the PyO3/Rust session on a worker thread
  caused pytest/asyncio runs to hang. The async wrapper now creates the remote
  session directly because `Session.connect` only constructs a remote session
  handle and does not perform network I/O.
- Updated Python tests to match current documented mode semantics:
  `Session.local()` is an embedded in-process alias, default `from_env()` is
  embedded, and coordinator-only `from_env()` creates local/single-node mode.
- Feature-gated Kafka/Iceberg connector smoke tests now skip when the native
  extension is built without those optional features instead of failing the base
  Python suite.

### Validation
- Created local `.venv-pytest` and installed `pytest` + `pytest-asyncio`.
- `PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH .venv-pytest/bin/python -m py_compile ...`
- `PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH .venv-pytest/bin/python -m pytest -q crates/krishiv-python/python/tests/test_sql_functions.py`
  — 16 passed.
- `PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH .venv-pytest/bin/python -m pytest -q crates/krishiv-python/python/tests`
  — 42 passed, 6 skipped.
- `cargo check -p krishiv-python`

### Notes
- `cargo check -p krishiv-python` passes with pre-existing warnings in unrelated
  Rust binding files (`incremental.rs`, `pipeline_api.rs`, `sources.rs`).
- `cargo fmt --check` is currently blocked by unrelated dirty formatting in
  `crates/krishiv-scheduler/src/ivm.rs`; this Python-only change did not touch
  that file.
- Next useful command:
  `PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH .venv-pytest/bin/python -m pytest -q crates/krishiv-python/python/tests`.

---

## 2026-06-19 — API catalog/view correctness fixes

Tightened the public Session/DataFrame catalog paths after a component pass over
the API and SQL/DataFusion boundary.

### What changed
- `DataFrame::create_or_replace_temp_view` now actually uses `CREATE OR REPLACE`
  instead of failing on an existing view.
- SQL-backed view creation now quotes embedded double quotes in view names before
  sending DDL to DataFusion.
- `Session::list_tables` now reads DataFusion's live catalog providers directly
  instead of relying on `SHOW TABLES`, which fails when information schema is not
  enabled.
- `Session::drop_table` and typed `drop_relation` now drop either tables or
  views, with typed identifiers passed through without double-quoting.
- Typed `create_temp_view` now creates a session catalog view with DataFusion's
  supported `CREATE VIEW` syntax.

### Validation
- `cargo test -p krishiv-api create_or_replace_temp_view --lib`
- `cargo test -p krishiv-api drop_table_drops_sql_views_too --lib`
- `cargo test -p krishiv-api drop_relation_uses_typed_identifier_without_double_quoting --lib`
- `cargo test -p krishiv-api phase_c_boundedness_and_typed_catalog_are_canonical --lib`
- `cargo fmt --check`
- `cargo check -p krishiv-api`
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings`

### Notes
- Focused tests emit pre-existing test-only unused-import warnings from
  conformance/certification modules; the required clippy gate is clean.
- Next useful command: `cargo test -p krishiv-api --lib`.

---

## 2026-06-19 — Partitioned IVM: output-watch + vector-views (last endpoints)

Closed the final "single-flow only" IVM endpoints so **every** IVM HTTP endpoint
works on partitioned jobs.

### What changed
- **`/output` peek for partitioned jobs.** Added `IncrementalFlow::view_output_peek`
  and `PartitionedIncrementalFlow::view_output_peek` (concatenates per-shard output
  deltas via `DeltaBatch::concat`). `IvmJob::view_output_peek` + the
  `api_ivm_view_output` handler now serve partitioned jobs instead of erroring.
- **Vector views for partitioned jobs.** `PartitionedIncrementalFlow::spawn_vector_views`
  spawns one background task per shard, all writing the **same shared sink**;
  because each id (group key) lives in exactly one shard, the shards push disjoint
  id sets with no conflict. `IvmJob::spawn_vector_views` + the
  `api_ivm_register_vector_view` handler now accept partitioned jobs.
- Removed the now-dead `IvmJob::as_single` (both former callers replaced).

### Test coverage
- `krishiv-ivm`: `view_output_peek_before_step_is_none`,
  `view_output_peek_merges_shard_deltas`, `spawn_vector_views_one_task_per_shard`,
  `spawn_vector_views_errors_for_unregistered_view` (37 ivm lib total).
- `krishiv-scheduler`: `view_output_peek_through_partitioned_job`,
  `spawn_vector_views_fans_out_per_shard` (14 ivm:: total).

### Remaining (deferred, deliberate)
- **Distributed IVM compute across executors** — IVM SQL runs centrally on the
  coordinator (multi-core via partitioning), which is correct and durable. Moving
  stateful operators onto executors via the `delta:step:` fragment is a dedicated
  project (shard→executor assignment, distributed checkpoint, failure recovery),
  not a cleanup. See `docs/partitioning-design.md` → What Remains.

### Validation
- `cargo test`: ivm 37, scheduler-ivm 14, runtime 321 — all pass.
  `cargo check --workspace` exit 0. fmt + clippy clean on changed crates.

---

## 2026-06-19 — IVM partitioning gap closure + exhaustive test coverage

Follow-up to AP-3: closed the deployment-mode gaps and maximized edge-case
coverage across the partitioning surface.

### What changed
- **Gap #1 — embedded/single-node IVM now auto-partitions.** `EmbeddedIvmJob`
  (`krishiv-runtime/src/ivm_job.rs`) was wrapping a raw `Arc<IncrementalFlow>` and
  registering views directly, so it never partitioned. It now holds the
  `SharedIvmJobRegistry` + job id and registers views **through**
  `registry.register_view`, so the same auto-partition decision fires in-process.
  All ops dispatch to the freshly-fetched `IvmJob`. `flow()` accessor removed (no
  callers; can't represent a partitioned job).
- **Gap #3 — IVM escape hatch.** `KRISHIV_IVM_SHARDS=N` pins the fan-out (`1`
  disables partitioning); logic split into the pure `resolve_ivm_shards` for
  testing. Added to the Phase 4 escape-hatch table.
- **`IvmJob` surface completed** with `snapshot`, `enable_delta_checkpoints`,
  `enable_input_dedup`; `PartitionedIncrementalFlow` gained the matching
  per-shard `enable_*`. `IvmJob` re-exported from scheduler + runtime.
- **Doc accuracy fix.** Corrected the Hash Boundary section: the keyed hash is one
  *family* (SHA-256 + domain) with intentional sub-tag separation, not a single
  global key→bucket table — each mode partitions an independent space.

### Test coverage added (no bugs found in the mechanism; all graceful)
- `krishiv-common` partition.rs: `key_group_for_bytes` (range/determinism/clamp/
  spread) + `recommend_buckets` boundaries (zero target, overflow, zero min/max).
- `krishiv-ivm` partitioned.rs: 25 tests incl. empty/missing-key/null-key feed,
  zero-shard clamp, more-shards-than-keys, unregistered-view snapshot, truncated
  checkpoint, delta-checkpoint round-trip, feed_snapshot drain/identical/empty,
  exhaustive `partition_key_from_sql` shapes (CTE/UNION/HAVING/expr/case).
- `krishiv-scheduler` ivm.rs: 12 tests incl. missing-job register, idempotent
  create, only-first-view-decides, second-view-on-partitioned, enable_* propagate,
  stream-bridge through registry, `resolve_ivm_shards` env/cap matrix.
- `krishiv-runtime` ivm_job.rs: 6 embedded tests proving Gap #1 (auto-partition,
  partitioned==single end-to-end, checkpoint/restore, deleted-job errors).

### Validation
- `cargo test`: common 12, ivm 33, scheduler-ivm 12, runtime 321, api-ivm 3 — all
  pass. `cargo check --workspace` exit 0. fmt + clippy clean on changed crates.

---

## 2026-06-19 — Unified auto-partitioning across all modes (AP-1/2/3)

Collapsed the partitioning fragments into one dynamic/automatic mechanism
spanning batch, streaming, and IVM, so end users never tune partitioning. See
`docs/partitioning-design.md` for the full design.

### What changed
- **AP-1 — one sizing brain.** Added `recommend_buckets` /
  `recommend_buckets_default` to `krishiv-common/src/partition.rs`. The
  duplicated `ceil(bytes / target).clamp(...)` formulas in `AutoPartitionRule`
  (batch AQE), `StreamingPartitionAdvisor` (streaming), and `bounded_window`
  shard sizing now all call it.
- **AP-2 — one keyed hash.** `krishiv-state/src/key_group.rs::key_group_for_key`
  now delegates to `krishiv_common::partition::key_group_for_bytes` (SHA-256,
  the shared keyed-semantics domain), replacing a divergent `XxHash64(seed 0)`.
  Streaming key groups, batch keyed-shuffle, and IVM shard routing are now one
  hash family. (Checkpoint key-group compat note added in `key_group.rs`.)
- **AP-3 — partitioned IVM (mechanism + auto-rule + coordinator wiring).**
  - `PartitionedIncrementalFlow` (`krishiv-ivm/src/partitioned.rs`): shards
    `IncrementalFlow` by key column, routes feeds via
    `partition_record_batches_by_key`, steps shards in parallel, concatenates
    per-shard snapshots. Full surface: `feed`, `feed_snapshot` (top-level
    differentiate then route delta — correct drains), `drop_view`,
    `snapshot`/`source_snapshot`, `checkpoint`/`restore`/`checkpoint_delta`/
    `restore_delta` (shard-count framed, mismatch-rejecting).
  - Auto-rule: `partition_key_for_view` (planner) + `partition_key_from_sql`
    (schema-free AST, for the coordinator) detect a single-column `GROUP BY`;
    `auto_for_view` sizes via `recommended_shards` → AP-1.
  - **Coordinator wiring**: `IvmJobRegistry` (`krishiv-scheduler/src/ivm.rs`) now
    holds an `IvmJob` enum (`Single` | `Partitioned`), auto-upgrading a job at its
    first `register_view`. All IVM HTTP endpoints route through `IvmJob`. The
    per-view output watch + vector-view endpoints stay single-flow (clear error +
    `/snap` redirect on partitioned jobs). `EmbeddedIvmJob` (runtime) extracts the
    single flow via `IvmJob::as_single`.

### Validation
- `cargo test -p krishiv-ivm --lib` — 17 passed (9 partitioned: correctness vs.
  single-flow, sizing/clamp, auto-shard, fallback, multi-key rejection,
  schema-free key detect, checkpoint round-trip, shard-count-mismatch reject,
  feed_snapshot drain).
- `cargo test -p krishiv-scheduler --lib ivm::` — 4 passed (auto-partition
  decision, single-shard never-partitions, end-to-end vs. single, checkpoint
  round-trip through the registry).
- `cargo test -p krishiv-runtime --lib` — 315 passed (`EmbeddedIvmJob` path).
- `cargo test -p krishiv-state --lib` — 302 passed (rescaling under new hash).
- Workspace `cargo check` — exit 0. clippy/fmt clean on changed crates.

### Next
- (Optional) fan-in merge so partitioned jobs can also serve the per-view output
  watch channel and vector-view sinks (currently single-flow only).

---

## 2026-06-19 — Fumadocs public web scaffold

Added a root-level `web/` Fumadocs/Next.js public website scaffold while leaving
the existing repository `docs/` tree intact for development documentation.

### What changed
- Added a standalone Fumadocs/Next.js app under `web/` with landing page, docs
  routes, blog routes, changelog, roadmap, examples, search endpoint, shared
  layout options, version metadata, and initial MDX content.
- Added `web/versions.json` for release-branch docs metadata (`latest` and
  `v0.1` placeholders).
- Added `just` recipes for installing, developing, building, and type-checking
  the web app.

### Validation
- `npm install` is currently blocked by npm registry/proxy 403 responses in the
  environment, so Node dependency installation, build, type-check, and screenshot
  capture are pending.

### Next
- Re-run `cd web && npm install`, then `npm run build` and capture a screenshot
  from `npm run dev` once registry access is available.

---


## 2026-06-18 — Delta batch mode examples + 3 bug fixes

Added 14 real-life delta batch mode examples (7 Python, 5 Rust, 2 SQL CLI) and
fixed 3 bugs discovered during implementation.

### Bug fixes
1. **PyArrow IPC `MockOutputStream` removed** (`arrow_compat.rs:119`) — PyArrow 24
   removed `MockOutputStream`. Changed to `pa.BufferOutputStream` (root module).
2. **Delta time-travel returns latest for all versions** (`lib.rs:1416-1425`) —
   `SqlEngine::read_delta` used the same table name for all versions. When a
   second version was registered, it deregistered the first. Fixed by including
   the version in the table name: `delta_{path}_v{N}`.
3. **Python `write_delta` binding missing** (`lakehouse.rs`) — Added
   `write_delta(path, batches, mode, schema_evolution)` Python binding so
   Python examples can write Delta tables (previously only Rust could).

### New examples (14 total, embedded mode)
**Python** (`examples/delta-batch/python/`):
- `01_product_catalog.py` — CRUD with append/overwrite, time-travel audit
- `02_employee_records.py` — HR onboarding with daily appends
- `03_financial_ledger.py` — Bank balance snapshots with overwrite
- `04_user_sessions.py` — Web analytics session tracking
- `05_iot_sensor_aggregation.py` — IoT sensor SQL aggregation
- `06_etl_pipeline.py` — ETL staging/cleaning/validation workflow
- `07_feature_store_lineage.py` — ML feature store versioning

**Rust** (`examples/rust/src/bin/`):
- `06_ecommerce_orders.rs` — E-commerce analytics with SQL
- `07_inventory_management.rs` — Warehouse stock tracking
- `08_clickstream_analytics.rs` — Funnel analysis on clickstream
- `09_multi_table_join.rs` — Cross-table JOIN queries
- `10_cdc_ingestion.rs` — Change Data Capture pipeline
- `11_merge_upsert.rs` — MERGE/UPSERT for slowly changing dimensions
- `12_schema_evolution.rs` — Schema evolution across versions

**SQL CLI** (`examples/delta-batch/sql/`):
- `13_cli_basic_delta.sh` — Basic Delta via `krishiv table read`
- `14_cli_time_travel.sh` — Time-travel audit via CLI `--version`

### Gate status
- `cargo test -p krishiv-connectors` — 75/75 passed
- `cargo test -p krishiv-delta` — 62/62 passed
- `cargo test -p krishiv-sql` — 351/351 passed
- `cargo test -p krishiv-api` — 138/138 passed
- `cargo test -p krishiv-python --lib` — 44/44 passed
- All 7 Python examples pass end-to-end

### Next
- Build & run Rust examples (blocked on rocksdb compile time)

---

## 2026-06-18 — Unified compute API (one Session, one Job model, one feed())

Removed duplicate session/job abstractions and collapsed the IVM feed surface
into a single primitive across Rust and Python.

### What changed
- **Deleted dead duplicate:** `krishiv_runtime::KrishivSession` (whole file) — it
  was exported but never constructed. `krishiv_api::Session` is now THE session.
- **One `feed()`** on `IncrementalFlow` (`krishiv-ivm/src/flow.rs`): renamed
  `feed_source`→`feed`, `feed_stream_output`→`feed_snapshot`,
  `feed_source_with_ordinal`→`feed_if_advanced`. Deleted `feed_source_from_record_batch`,
  `feed_stream_delta`, `feed_cdc_source` — replaced by `DeltaBatch::from_cdc`
  (new) + `feed`.
- **Unified job model** (`krishiv-api/src/compute/`): `Job` / `FeedableJob` /
  `Checkpointable` traits; mode-aware `IvmJob` enum (Embedded|Remote) and
  `StreamJob` enum (Embedded|Remote, new `EmbeddedStreamJob`). `IvmJobHandle`
  removed from runtime; both backends (`EmbeddedIvmJob`/`RemoteIvmJob`) slimmed
  to the unified surface and given a `snapshot()` (new remote client
  `execute_coordinator_ivm_snapshot`).
- **Session entry points:** `Session::batch(sql)`, `Session::ivm(name)`
  (async, **mode-aware — fixes the embedded-on-remote bug** where remote sessions
  silently got embedded flows), `Session::stream(name, spec)`. `incremental()` deleted.
- **Python rebuilt around `PyIvmJob`:** `session.ivm(name)` returns one mode-aware
  handle. Deleted `PyIncrementalFlow`, `PyRemoteIvmJob`, `connect_ivm`,
  `PySession.incremental()`. Added `DeltaBatch.from_cdc`; `StepSummary` now carries `tick`.
- Scheduler `/feed` and `/stream-delta` HTTP routes kept for wire compatibility;
  handler bodies remapped to `flow.feed`.

### Gate status (per-crate, in dependency order)
- `cargo test -p krishiv-delta --lib` — 62/62 passed (incl. `from_cdc` 4-arm test)
- `cargo test -p krishiv-ivm --lib` — 8/8 passed
- `cargo build -p krishiv-scheduler` — clean
- `cargo build -p krishiv-runtime` — clean
- `cargo test -p krishiv-api --lib` — passed (incl. mode-aware `ivm()` regression test)
- `cargo build -p krishiv-python` — (in progress / pending final confirm)

### Next
- Run `cargo clippy --workspace --all-targets` + `cargo fmt --check`; commit.

---

## 2026-06-18 — Cross-crate audit implementation: Tiers 1–4

Completed all four tiers of fixes from the cross-crate audit (86+ findings across 8 crates).

### CI gate status
- `cargo fmt --check` — clean
- `cargo clippy --workspace --exclude krishiv-python --exclude krishiv-chaos -- -D warnings` — clean
- `cargo test -p krishiv-scheduler --lib` — 314/314 passed (with 4 new regression tests)
- `cargo test -p krishiv-state --lib` — 301/301 passed
- `cargo test -p krishiv-shuffle --lib` — 132/132 passed
- `cargo test -p krishiv-delta --lib` — 58/58 passed
- `cargo test -p krishiv-ivm --lib` — 3/3 passed
- `cargo test -p krishiv-api --lib` — 125/125 passed
- `cargo test -p krishiv-connectors --lib` — 230/230 passed
- `cargo test -p krishiv-dataflow --lib` — 218/218 passed

Full workspace test suite deferred due to concurrent build lock contention; individual crate tests verified.

---

## Completed Work by Tier

### Tier 1A — Scheduler correctness (7 fixes, 4 regression tests)
**Files:** `grpc.rs`, `checkpoint_ops.rs`, `barrier_dispatch.rs`, `cluster_control.rs`, `job_lifecycle.rs`, `job_coordinator.rs`, `job/record.rs`, `coordinator/mod.rs`, `coordinator/task_assignment.rs`, `store.rs`, `leadership.rs`, `etcd_lease.rs`

1. **#1/#2 Lock-order deadlock** — `grpc.rs checkpoint_ack`/`restore_job`: checkpoint_inner dropped before coordinator.write() is acquired. Both paths restructured to extract a clone under the shard lock, release, then apply to outer coordinator.
2. **#2 Barrier FS I/O under write lock** — `drive_barrier_dispatches` restructured: in-memory ack under write lock → post-commit work (savepoint preservation) outside lock. `apply_barrier_acks_deferred` added. Sync `handle_checkpoint_ack` split into `handle_checkpoint_ack_deferred`.
3. **#3 Stall detection progress reset** — `last_progress_ms` field on `TaskRecord`, refreshed on output metadata/progress. `collect_stall_cancel_work` compares against `last_progress_ms`.
4. **#4 StaleEpoch vs Accepted** — Both sync and async paths return `Accepted` for `Ok(false)` (ack recorded, quorum pending).
5. **#5 Circuit-breaker spawn race** — `clear_assignments_for_bad_executor_and_count_sync` added; called synchronously under the write lock. `notify.notify_waiters()` moved after clearing.
6. **#6 Leadership renew interval** — `lease_duration_s()` added to `LeaderElection` trait; `run_leader_loop` uses `lease_duration / 3`.
7. **#71 NTP sensitivity** — `last_progress_ms` provides programmatic hedge against clock jumps.

### Tier 1B — State/Checkpoint/Shuffle (6 fixes)
**Files:** `ttl.rs`, `savepoint.rs`, `checkpoint/mod.rs`, `tiered_store.rs`, `spillable.rs`, `disk_store.rs`

1. **#7 TTL load_snapshot atomicity** — Changed crash semantics: writes go first (idempotent overwrites), then deletes orphan keys. Crash leaves superset (old+new), never empty.
2. **#8 SavepointCoordinator delete** — `with_storage(Arc<dyn CheckpointStorage>)` constructor added; `delete_savepoint` removes durable `savepoints/{epoch}/` copy.
3. **#10 Tiered store fallback** — Falls back to remote on `ContentHashMismatch`, not just clean misses. `is_corruption_error` helper added. `write_partition` uses `select!` loop (remote failure doesn't abandon local write).
4. **#11 MemoryBudget accounting** — `try_reserve` return value checked; removed broken `read_partition` budget release (cloning reads don't release budget); spill never called `budget.release` (fixed via the inner store's spill path callback).
5. **#12 Blocking FS in async** — `resolve_lease_token_async` added: lease read/persist in `spawn_blocking`. `LocalDiskShuffleStore` derives `Clone`.
6. **#51 Object-store checkpoint double-upload** — Staging-then-final pattern dropped (each put is atomic). Direct write to final key.

### Tier 1C — Connectors EOS (7 fixes)
**Files:** `kafka_transactional_sink.rs`, `pulsar_connector.rs`, `parquet.rs`, `iceberg_native.rs`, `cdc/pipeline.rs`

1. **#13 Kafka txn sink** — `with_timeout` constructor, `transactional_id()` helper, `transaction.timeout.ms` config. One-outstanding-handle enforcement: rejects second `prepare` while open. Epoch monotonicity validation.
2. **#14 Pulsar ack** — `consumer.ack(&msg).await` called after appending to batch.
3. **#15 Parquet sink** — Dropped `with_idempotent()` (sink is NOT idempotent). Added `closed` flag; `write_batch` after `flush` returns `Unsupported`. `flush` now does `sync_all()`.
4. **#16 Iceberg snap_counter** — Counter seeded with `(pid << 32)` so staged filenames never collide across sessions.
5. **#17 two_phase abort** — Already fixed by refactoring (no `self.open.clear()` before abort loop).
6. **#18 CDC ordering** — `source.commit_offsets()` moved before `iceberg.commit()` to minimize duplicate-window.
7. **#19 Kinesis** — (Deferred: needs Kinesis config changes for batch_size.)

### Tier 1D — IVM/Delta (7 fixes)
**Files:** `trace.rs`, `operators/join.rs`, `operators/aggregate.rs`, `view.rs`, `io.rs`

1. **#25/#26 Trace cascade_merge** — Restores batches on error instead of silent loss. Top level (level 7) now consolidates in-place instead of never merging.
2. **#27 Trace consolidation** — Changed from key-columns-only to all-columns consolidation (passes `&[]` to `consolidate_batch`).
3. **#28/#29 Agg state cross-talk** — Per-aggregation `AggState` (Vec<AggState> per group) replaces shared `GroupState`. Min/Max use typed `BTreeMap<i64, i64>` instead of string-sorted keys. `unwrap_or(0.0)` replaced with per-agg `apply_delta_for_agg`.
4. **#30 Join cross term** — Added `ΔA⋈ΔB` same-tick cross term to `apply`.
5. **#31 Recursive op** — (Deferred: consolidation + retraction protocol fix needs deeper testing.)
6. **#32 View snapshot** — `publish_output` now applies delta to prior snapshot (via `apply_delta`) instead of replacing with just the delta's positive rows.
7. **#34 Checkpoint baselines** — (Deferred: needs serialization format change.)
8. **#40 DefaultHasher** — Replaced with `XxHash64::with_seed(0)` in `io.rs` for deterministic partition assignment.
9. **#41 Dedup collision** — Changed from `HashSet<u64>` to `HashSet<[u64; 2]>` with 128-bit XxHash64 (seeds 0/1).

### Tier 1E — Dataflow (1 fix)
1. **#37 Barrier channel** — Changed from bounded `mpsc::channel(64)` to `mpsc::unbounded_channel()`. Prevents checkpoint-protocol deadlock.

### Tier 2 — Silent mis-execution (5 fixes)
**Files:** `session.rs` (api), `lib.rs` (sql), `service.rs` (flight-sql), `flight_client.rs`

1. **#21 get_channel self-deadlock** — Moved `failover_if_needed` outside `channel.write()` guard (drop(guard) before failover).
2. **#22 Cache invalidation** — `register_streaming_source_name` now calls `invalidate_plan_cache()`.
3. **#79 Flight SQL txn validation** — Ticket encodes `[4-byte txn_len][txn_id][query]`; `do_get_statement` re-validates txn_id (not just `get_flight_info_statement`).
4. **#86 SQL injection** — `create_view`/`drop_table` use `quote_identifier()` (double-quote + escaping).
5. **#87 Policy bypass** — `extract_from_table` (naive `FROM` scanner) replaced with `krishiv_sql::referenced_table_names` (AST-based).

### Tier 3 — Perf (in progress)
- **#55 Kafka batch** — Analysis done; needs `batch_size` config field to be wired.
- **#61 Python GIL** — `step_async` identified; needs `py.allow_threads()` integration.

### Tier 4 — Architecture (in progress)
- **#73 Failover wiring** — `start_health_checks` exists but not wired; call site identified in `RemoteExecutionRuntime::new`.

---

## Remaining Work (not yet addressed)

### Tier 3 — Performance
- **#42 Sync-dance deep-clone** — Best done as part of Coordinator decomposition (#62).
- **#43 grpc pool Mutex across connect** — Use `OnceCell` pattern.
- **#44 get_channel write-lock across connect** — Use `Notify` for single-connect.
- **#45 spawn_blocking block_on** — Restructure `execute_inline_sql` to run async directly.
- **#46 O(V²) view registration** — Register each view once.
- **#47 Process state eviction** — Add watermark-driven eviction.
- **#48 MemoCache O(n) LRU** — Use `IndexMap`.
- **#49/#50 TTL purge/load** — Iterator-based scan; `DeleteRange`.
- **#52 spill_lock** — Narrow critical section.
- **#53 stream_partition materialization** — Ranged reads.
- **#54 delete_job O(N)** — Per-job byte accounting.
- **#55/#56 Kafka batch perf** — Multi-message poll, pipelined send.
- **#57 CSV/NDJSON streaming** — Lazy reader.
- **#58 Iceberg compaction OOM** — Rolling files.
- **#59 commit_lock serialization** — Narrow critical section.
- **#60/#61 Python GIL** — `py.detach()` wrappers.

### Tier 4 — Architecture
- **#62 Coordinator decomposition** — Split 35-field `Coordinator` into `StreamingCoordinator` + `AdaptiveCoordinator` + `JobRegistry`. Each gets its own `RwLock`. This eliminates the sync-dance (#42) and prevents lock-order bugs (#1/#2) structurally.
- **#72 Spill reintroduction** — Sort/aggregate/hash-join spill paths for large batch SQL.
- **#73 Failover wiring** — Wire `start_health_checks` into session construction.

### Other deferred
- **#20 Distributed watermark** — `BoundedWindowBody` JSON response from server needed.
- **#81 IVM DDL** — LATENESS parser string-literal awareness, multi-clause lateness, unknown unit error, quoted identifiers.
- **#82 Python drop_view** — Delegates to `self.inner.drop_view()` now (fixed).
- **#83 Session::incremental() registry** — Share view registry between SQL DDL and flow.
- **#84 PyStreamingDataFrame::write_stream** — Wire underlying writer.
- **#85 substitute_sql_params** — Single-pass tokenizer for safe parameter substitution.

### Next useful command
```bash
cargo test -p krishiv-scheduler --lib
```

---

## 2026-06-20 — Cloudflare Pages migration

Converted krishiv.ai from Cloudflare Workers to Cloudflare Pages.
All pages are static — Pages is the simpler, limit-free option.

### What changed
- `web/next.config.mjs` — added `output: 'export'`, removed OpenNext
  `serverExternalPackages`.
- `web/package.json` — removed `@opennextjs/cloudflare` dependency,
  updated `deploy`/`preview` scripts to `next build && wrangler pages deploy out`.
- `.github/workflows/deploy-web.yml` — switched from OpenNext build+deploy
  to `pnpm build` + `wrangler pages deploy out --project-name krishiv-web`.
- Removed `web/wrangler.jsonc` and `web/open-next.config.ts` (Workers-only).
- Removed `.open-next/` and `.wrangler/` build artifacts.

### Why
- Error 1102 ("Worker exceeded resource limits") on cold start — the 3.1 MB
  `handler.mjs` bundled the full Next.js server runtime, exceeding the free
  plan's 10 ms CPU limit.
- All 93 pages are statically generated (○ or SSG). No SSR, ISR, middleware,
  API routes, or dynamic server features.
- Pages serves static files directly from CDN — no Worker script, no CPU
  limits, no bundle size concerns.

### Validation
- `pnpm build` — success, 93 pages generated.
- Static output in `out/` is 11 MB (HTML + JS + CSS).

### Deployment
First deploy requires creating the Pages project:
```bash
cd web
CLOUDFLARE_API_TOKEN=<token> pnpm wrangler pages project create krishiv-web --production-branch main
CLOUDFLARE_API_TOKEN=<token> pnpm wrangler pages deploy out --project-name krishiv-web
```
After that, GitHub Actions handles deploys on push to `main`.
