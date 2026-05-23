# Krishiv Codebase Gap-Mitigation Plan

**Generated:** 2026-05-23  
**Scope:** All 35 workspace crates, intensive file-level review  
**Total findings:** ~90 across Critical / High / Medium / Low

---

## How to Read This Document

Issues are grouped by **priority tier**, then **subsystem**. Each item carries:
- **ID** — short stable reference (e.g. `CP-ORCH-1`)
- **Crate(s)** — where the defect lives
- **Finding** — what is wrong
- **Mitigation** — the smallest safe fix
- **Validation** — the command that proves it done

Priority tiers:

| Tier | Meaning | Target |
|---|---|---|
| **P0** | Compilation broken, data loss, or security hole. Cannot ship anything. | Fix immediately on `main` |
| **P1** | Correctness bug or severe reliability gap in a critical subsystem path. | Fix before next release |
| **P2** | Significant quality gap or incomplete integration. | Fix within two releases |
| **P3** | Technical debt, test coverage, low-risk panics. | Backlog |

---

## P0 — Compilation Breaks and Pre-Production Blockers

These issues cause build or test failures, guarantee wrong results for all callers, or represent immediate data-loss/security risks.

### P0-1 · Orphaned source files across core crates

**Crates:** `krishiv-exec`, `krishiv-sql`, `krishiv-plan`  
**Finding:** 12 source files exist on disk but are never compiled because no `mod` declaration includes them:

- `krishiv-exec`: `live_table.rs`, `schema_normalize.rs`, `temporal_join.rs`, `interval_join.rs`, `cep.rs`, `barrier_align.rs`, `side_output.rs`, `memo.rs`, `watermark_e2e.rs`  
- `krishiv-sql`: `live_table.rs`, `spark_compat.rs`, `spark_compat_date.rs`  
- `krishiv-plan`: `streaming.rs`

All R14–R16 streaming operator implementations, the CEP operator, barrier alignment, live tables, and Spark compatibility UDFs are silently absent from every binary and wheel. `cargo test -p krishiv-sql --test spark_compat` fails with `E0432`.

**Mitigation:**

1. For each file, determine whether it is ready to compile: check for missing dependencies, missing enum variants, or import cycles.
2. Add `pub mod <file>;` (or `mod <file>;`) to the crate's `lib.rs` for files that are ready.
3. For files that are not ready: either delete them, gate them with `#[cfg(feature = "unstable")]`, or open a tracking issue with `todo!()` stubs on the public types.
4. Fix the two dependent missing variants first (see P0-2).

**Validation:** `cargo check --workspace 2>&1 | grep "^error"` → zero errors.

---

### P0-2 · Missing enum variants referenced by orphaned files

**Crates:** `krishiv-exec`, `krishiv-plan`  
**Finding:** Two variants are referenced in the orphaned files but not declared:

- `ExecError::IncompatibleSchemaEvolution` — used in `schema_normalize.rs`
- `NodeOp::CreateLiveTable`, `NodeOp::RefreshLiveTable`, `NodeOp::DropLiveTable` — used in `krishiv-sql/src/live_table.rs` and `krishiv-exec/src/live_table.rs`

Wiring P0-1 without these additions causes immediate compile errors.

**Mitigation:** Add the missing variants to `ExecError` (`krishiv-exec/src/lib.rs`) and `NodeOp` (`krishiv-plan/src/lib.rs`) before enabling the modules.

**Validation:** `cargo check -p krishiv-exec -p krishiv-plan 2>&1 | grep "^error"` → zero.

---

### P0-3 · Exactly-once connector test suite does not compile

**Crates:** `krishiv-connectors`  
**Finding:** `tests/exactly_once_certification.rs` imports `krishiv_connectors::transactional_kafka::TransactionalKafkaSink` and `krishiv_connectors::two_phase_parquet_s3::TwoPhaseParquetSink`. Neither module is declared in `src/lib.rs`. Running `cargo test -p krishiv-connectors` fails at the test binary link step.

**Mitigation:** Add `pub mod transactional_kafka;` and `pub mod two_phase_parquet_s3;` to `krishiv-connectors/src/lib.rs`. Also add `cdc_router` (currently orphaned — depends on `krishiv_exec::SchemaNormalizeOperator`; gate it until P0-1 is resolved).

**Validation:** `cargo test -p krishiv-connectors 2>&1 | grep "^error"` → zero.

---

### P0-4 · Coordinator binary never ticks heartbeat or drives task launches

**Crates:** `krishiv-scheduler/src/bin/krishiv_coordinator.rs`, `krishiv-operator/src/main.rs`  
**Finding:** The standalone coordinator binary and the operator's embedded coordinator never call `advance_heartbeat_clock()` or `launch_assigned_task_assignments()`. Consequences:
- Dead/crashed executors are never timed out; their task slots are never reclaimed.
- Jobs submitted via gRPC stay in `Assigned` state forever; no task is ever dispatched to an executor.
- The entire distributed scheduling subsystem is non-functional in a live deployment.

**Mitigation:**

```rust
// In coordinator main loop (after gRPC server start):
tokio::spawn({
    let coord = shared_coord.clone();
    async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            coord.lock().await.advance_heartbeat_clock();
        }
    }
});

tokio::spawn({
    let coord = shared_coord.clone();
    async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let assignments = coord.lock().await.launch_assigned_task_assignments();
            for (executor_addr, task_msg) in assignments {
                // dispatch via GrpcCoordinatorService to executor
                dispatch_task(executor_addr, task_msg).await;
            }
        }
    }
});
```

Apply the same pattern to `krishiv-operator/src/main.rs`.

**Validation:** Submit a job via gRPC; assert task status transitions to `Running` within 5 seconds: `cargo test -p krishiv-scheduler -- task_launch_drives_to_running`.

---

### P0-5 · Kubernetes leader election never wired — split-brain risk

**Crates:** `krishiv-operator/src/lib.rs`, `krishiv-operator/src/main.rs`  
**Finding:** `K8sLeaseElection` is fully implemented but `main.rs` never instantiates it, never runs a renewal loop, and never calls `coordinator.promote_to_active()`. Multiple operator replicas run as permanently "active" with no fencing, causing split-brain job management.

**Mitigation:**

```rust
// In operator main():
let election = K8sLeaseElection::new(kube_client.clone(), namespace, lease_name, identity);
tokio::spawn(async move {
    loop {
        match election.try_acquire().await {
            Ok(true) => coordinator.promote_to_active(),
            _ => {}
        }
        election.renew().await.ok();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
});
```

**Validation:** `cargo test -p krishiv-operator -- leader_election` → leader transitions observed.

---

### P0-6 · K8s finalizer never applied to resource

**Crates:** `krishiv-operator/src/lib.rs`  
**Finding:** `reconcile()` returns `ReconcileAction::FinalizerAdded` when the finalizer is absent but the outer reconciliation loop only patches `.status`. The `metadata.finalizers` array is never updated. Resource deletion cleanup is unreliable.

**Mitigation:** After detecting `FinalizerAdded`, issue a separate `PATCH /apis/.../krishivjobs/{name}` adding `krishiv.io/job-finalizer` to `metadata.finalizers` before proceeding with status patch.

**Validation:** Create and delete a `KrishivJob` CRD in an integration test; assert the finalizer is removed and the job resource is garbage collected.

---

### P0-7 · Executor creates a new TCP connection per gRPC call

**Crates:** `krishiv-executor/src/transport.rs`  
**Finding:** `GrpcCoordinatorService` calls `CoordinatorExecutorClient::connect(endpoint).await` at the start of every method (`task_status`, `executor_heartbeat`, `register_executor`, etc.). A heartbeat every 5 seconds creates 12 new TCP connections per minute per executor. Under load, this will exhaust file descriptors and cause connection storms.

**Mitigation:** Hold the client in a `OnceCell<CoordinatorExecutorClient<Channel>>` or `tokio::sync::RwLock<Option<Channel>>` with lazy initialization and reconnect-on-failure semantics.

```rust
pub struct GrpcCoordinatorService {
    endpoint: String,
    client: tokio::sync::Mutex<Option<CoordinatorExecutorClient<Channel>>>,
}

impl GrpcCoordinatorService {
    async fn client(&self) -> Result<CoordinatorExecutorClient<Channel>> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(CoordinatorExecutorClient::connect(self.endpoint.clone()).await?);
        }
        Ok(guard.as_ref().unwrap().clone())
    }
}
```

**Validation:** Monitor file descriptor count during a 60-second heartbeat run; assert it does not grow.

---

### P0-8 · Executor never updates lease generation from coordinator response

**Crates:** `krishiv-executor/src/transport.rs`, `src/main.rs`  
**Finding:** `ExecutorRuntime.config.lease_generation` is set to `LeaseGeneration::initial()` at construction and never updated. After a coordinator restart (which bumps the generation), all subsequent heartbeats from this executor are rejected as `StaleExecutorLease`, permanently disconnecting the executor.

**Mitigation:** After every successful `register_executor` or `executor_heartbeat` response, extract the new `lease_generation` from the response and store it back into `ExecutorRuntime.config`.

**Validation:** `cargo test -p krishiv-executor -- lease_generation_updated_after_reregister`.

---

### P0-9 · `DataFusionTableBridge::scan` always returns `EmptyExec`

**Crates:** `krishiv-catalog/src/lib.rs`  
**Finding:** Every SQL query resolved through the Krishiv catalog returns zero rows. The `scan()` implementation returns `Arc::new(EmptyExec::new(self.arrow_schema.clone()))` unconditionally.

**Mitigation:** Replace with a real execution plan. For in-memory tables, use DataFusion's `MemoryExec`. For Parquet-backed tables, use `ParquetExec`. The `TableProvider` should hold a reference to the underlying data source and delegate to it.

**Validation:** Register a table with 10 rows, run `SELECT * FROM t`, assert 10 rows returned.

---

### P0-10 · `krishiv-catalog` not a dependency of `krishiv-sql`

**Crates:** `krishiv-catalog`, `krishiv-sql`  
**Finding:** `SqlEngine` does not import `krishiv-catalog`. Table resolution during SQL parsing goes through DataFusion's own `SessionContext` registry, bypassing all catalog-registered tables. Catalog-registered schemas are invisible to SQL queries.

**Mitigation:**

1. Add `krishiv-catalog` as a dependency of `krishiv-sql` in its `Cargo.toml`.
2. In `SqlEngine::new()`, register `DataFusionCatalogBridge` with the DataFusion `SessionContext` via `ctx.register_catalog("krishiv", Arc::new(bridge))`.
3. Fix P0-9 first so the bridge actually returns data.

**Validation:** `cargo test -p krishiv-sql -- catalog_table_resolved_in_sql`.

---

### P0-11 · `WeaviateSink::query_nearest` always returns empty results

**Crates:** `krishiv-vector-sinks/src/weaviate.rs`  
**Finding:** The GraphQL response body is parsed into `_payload: serde_json::Value` (prefixed with `_`, so unused). All Weaviate queries silently return `Vec::new()`.

**Mitigation:** Parse the GraphQL response structure to extract the `data.Get.<ClassName>` array and map each hit to `ScoredChunk { text, chunk_index, score, payload }`.

**Validation:** `cargo test -p krishiv-vector-sinks -- weaviate_query_returns_results` (requires mockito server with a real GraphQL response fixture).

---

### P0-12 · Python wheel is almost entirely dead code

**Crates:** `krishiv-python/src/lib.rs`  
**Finding:** `lib.rs` only declares `mod ai;`. The files `session.rs`, `dataframe.rs`, `stream.rs`, `batch.rs`, `errors.rs`, `sources.rs`, `sinks.rs`, `windows.rs`, `udf.rs`, `agg.rs`, `pipeline.rs`, `migration.rs`, `live_table.rs`, `memo.rs`, `schema.rs`, `job_status.rs`, and `query_result.rs` are never compiled. The compiled Python wheel exposes only the AI submodule and a minimal stub of `PySession` and `PyDataFrame` defined inline in `lib.rs`.

**Mitigation:**

1. For each split module, decide: promote to `lib.rs` or add `mod <name>;`.
2. Resolve the type-definition conflicts (`PySession` and `PyDataFrame` are defined in both `lib.rs` and the split files).
3. Add `mod session; mod dataframe; ...` declarations progressively, fixing compile errors.

**Validation:** `python -c "import krishiv; s = krishiv.Session(); df = s.sql('SELECT 1')"` succeeds.

---

### P0-13 · `rag_query` always returns zero results (new sink per call)

**Crates:** `krishiv-python/src/ai.rs`  
**Finding:** `rag_query()` constructs a new `InMemoryVectorSink` on every call. The sink populated by `rag_index` is a different instance. RAG queries will always return empty results regardless of what was indexed.

**Mitigation:** Share the sink between `rag_index` and `rag_query` via a process-global registry (`once_cell::sync::Lazy<RwLock<VectorSinkRegistry>>`) keyed by pipeline name.

**Validation:** `cargo test -p krishiv-python -- rag_index_then_query_returns_results`.

---

### P0-14 · Spark Connect CAST always emits STRING type

**Crates:** `krishiv-spark-connect/src/translate.rs` line 192  
**Finding:** `CAST({inner} AS STRING)` is emitted regardless of the `DataType` in the proto `Cast` message. Numeric and timestamp casts silently produce string columns. PySpark workloads using `.cast(IntegerType())` will get wrong types with no error.

**Mitigation:** Map the Spark proto `DataType` to SQL type names and use them in the CAST expression:

```rust
fn spark_type_to_sql(dt: &spark_connect::DataType) -> &'static str {
    match dt.kind.as_ref() {
        Some(Kind::Integer(_)) => "INT",
        Some(Kind::Long(_)) => "BIGINT",
        Some(Kind::Double(_)) => "DOUBLE",
        Some(Kind::Timestamp(_)) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}
```

**Validation:** `cargo test -p krishiv-spark-connect -- cast_preserves_type`.

---

### P0-15 · Audit log has zero call sites — no audit records produced

**Crates:** `krishiv-governance`, `krishiv-sql-policy`, `krishiv-api`, `krishiv-flight-sql`, `krishiv-scheduler`  
**Finding:** `audit_log()` is defined and tested only inside `krishiv-governance`. Zero call sites exist anywhere in the execution path. Every SQL execution, every access denial, every job submission produces no audit record, violating compliance requirements.

**Mitigation:**

1. In `krishiv-sql-policy::execute_as()`, call `audit_log(AuditAction::QueryExecuted { ... })` on both allowed and denied paths.
2. In `krishiv-scheduler::handle_submit_job()`, call `audit_log(AuditAction::JobSubmitted { ... })`.
3. In `krishiv-flight-sql`, call `audit_log` in the `do_get` and access-denial paths.
4. Add `krishiv-governance` as a dependency to `krishiv-scheduler` and `krishiv-executor` `Cargo.toml` files.

**Validation:** `cargo test -p krishiv-sql-policy -- audit_events_emitted_on_execute`.

---

### P0-16 · OTel Metrics API entirely absent — `krishiv-metrics` ships no metrics

**Crates:** `krishiv-metrics/src/lib.rs`  
**Finding:** Only distributed tracing (spans) is implemented. `opentelemetry` is declared with `features = ["trace"]` only. There is no `MeterProvider`, no `Counter`, no `Histogram`, and no Prometheus scrape endpoint. The crate name is misleading.

**Mitigation:**

1. Add `features = ["metrics"]` to the `opentelemetry` and `opentelemetry_sdk` dependencies.
2. Add `opentelemetry-prometheus` dependency and expose a `PrometheusExporter` in `MetricsConfig`.
3. Create a `KrishivMetrics` struct with counters and histograms for:
   - `krishiv_tasks_total{status}` (submitted / running / succeeded / failed)
   - `krishiv_task_duration_seconds` (histogram)
   - `krishiv_shuffle_bytes_written_total`
   - `krishiv_job_queue_depth`
4. Export from `krishiv-metrics` and import in `krishiv-scheduler` and `krishiv-executor`.
5. Wire the Prometheus handler into `krishiv-ui`'s `/metrics` route.

**Validation:** Start coordinator; `curl localhost:9090/metrics` returns Prometheus text with `krishiv_tasks_total`.

---

## P1 — Correctness and Reliability

### P1-1 · Checkpoint fencing token uses exact equality — rejects valid future-generation tokens

**Crates:** `krishiv-checkpoint/src/lib.rs:445`  
**Finding:** `validate_fencing_token` checks `stored_token != current_token`. A coordinator that restarts with a higher fencing token cannot validate checkpoints written by the previous valid coordinator, which has a lower but still legitimate token. The correct semantic is: reject if `stored_token < current_token` (stale write) and accept if `stored_token >= current_token`.

**Mitigation:**

```rust
pub fn validate_fencing_token(metadata: &CheckpointMetadata, current: &FencingToken) -> CheckpointResult<()> {
    if metadata.fencing_token.value() < current.value() {
        return Err(CheckpointError::StaleFencingToken { ... });
    }
    Ok(())
}
```

Update the tests that currently assert future-token rejection to assert future-token acceptance.

**Validation:** `cargo test -p krishiv-checkpoint -- fencing_token_accepts_future_generation`.

---

### P1-2 · No epoch monotonicity guard on checkpoint write

**Crates:** `krishiv-checkpoint/src/lib.rs`  
**Finding:** `write_epoch_metadata` accepts any epoch number unconditionally. A stale or replayed gRPC message can overwrite epoch 5 with epoch 3.

**Mitigation:** Before writing, read the current latest epoch from storage and reject if `new_epoch <= latest_committed_epoch`. Store the latest committed epoch in a sidecar file (`latest_epoch.json`) updated atomically on each successful commit.

**Validation:** `cargo test -p krishiv-checkpoint -- stale_epoch_rejected`.

---

### P1-3 · No fsync before rename in checkpoint storage

**Crates:** `krishiv-checkpoint/src/lib.rs`  
**Finding:** `LocalFsCheckpointStorage::write_bytes` writes a temp file and renames it. Without `fsync(tempfile)` and `fsync(parent_dir)`, the rename may not be durable across an OS crash on Linux.

**Mitigation:**

```rust
let tmp_path = path.with_extension("tmp");
let mut file = File::create(&tmp_path)?;
file.write_all(data)?;
file.sync_all()?;  // fsync the data
drop(file);
fs::rename(&tmp_path, path)?;
// fsync parent dir
let parent = path.parent().unwrap();
File::open(parent)?.sync_all()?;
```

**Validation:** `cargo test -p krishiv-checkpoint -- write_survives_simulated_os_crash`.

---

### P1-4 · Shuffle: no spill-to-disk and partition cap unenforced

**Crates:** `krishiv-shuffle/src/lib.rs`  
**Finding:** `InMemoryShuffleStore` has no size limit; large shuffle OOMs instead of spilling. `ShuffleMetadata.max_partitions` is never checked by any store implementation.

**Mitigation:**

1. Add a `max_bytes: Option<usize>` field to `InMemoryShuffleStore`.
2. On write, if total bytes exceed threshold, spill oldest partitions to `LocalDiskShuffleStore`.
3. Add a `max_partitions` check to every `write_partition` call in all three store implementations.

**Validation:** `cargo test -p krishiv-shuffle -- spills_to_disk_at_memory_limit`.

---

### P1-5 · Shuffle: compression disconnected from Parquet and IPC stores

**Crates:** `krishiv-shuffle/src/lib.rs`  
**Finding:** LZ4/Zstd compression is implemented only for `LocalShuffleStore` (raw bytes). `LocalDiskShuffleStore` (Parquet) and `ObjectStoreShuffleStore` (Arrow IPC) write uncompressed data.

**Mitigation:** Pass `ShuffleCompression` through to `LocalDiskShuffleStore::write_partition` and use Parquet `WriterProperties::builder().set_compression()` accordingly. For `ObjectStoreShuffleStore`, use `IpcWriteOptions` with compression.

**Validation:** `cargo test -p krishiv-shuffle -- parquet_store_writes_compressed`.

---

### P1-6 · Shuffle: `ObjectStoreShuffleStore::register_partition_lease` is a no-op

**Crates:** `krishiv-shuffle/src/lib.rs`  
**Finding:** The method body is `Ok(())`. Zombie writers can overwrite committed partitions.

**Mitigation:** Store lease tokens in a `DashMap<PartitionKey, LeaseToken>`. In `write_partition`, check that the caller's token matches the registered lease.

**Validation:** `cargo test -p krishiv-shuffle -- zombie_write_rejected_by_lease`.

---

### P1-7 · State: `TtlStateBackend::list_keys` returns expired keys

**Crates:** `krishiv-state/src/lib.rs`  
**Finding:** `list_keys` and `list_namespaces` delegate to the inner backend without filtering expired entries. Callers see stale keys.

**Mitigation:** Add filtering to both methods:

```rust
fn list_keys(&self, ns: &str) -> StateResult<Vec<Vec<u8>>> {
    let now = unix_now_ms();
    self.inner.list_keys(ns)?.into_iter()
        .filter(|k| !self.is_expired(ns, k, now))
        .collect()
}
```

**Validation:** `cargo test -p krishiv-state -- list_keys_excludes_expired`.

---

### P1-8 · State: no compaction — expired bytes accumulate forever

**Crates:** `krishiv-state/src/lib.rs`  
**Finding:** `RedbStateBackend` has no background sweep; expired key bytes are never physically deleted.

**Mitigation:** Add a `compact(namespace: &str)` method to `StateBackend` that physically deletes keys past their TTL. Expose it from `TtlStateBackend` with a sweep loop that runs every configurable interval via `tokio::time::interval`.

**Validation:** Write 1000 TTL-expired keys; call `compact()`; assert `list_keys()` returns empty and backing file size decreases.

---

### P1-9 · State: no recovery path for corrupt redb file

**Crates:** `krishiv-state/src/lib.rs`  
**Finding:** A corrupt redb file on process restart returns `BackendUnavailable` with no remediation.

**Mitigation:** On `Database::create` failure, attempt `Database::repair(path)` (if redb provides it). If repair fails, rename the corrupt file to `{path}.corrupt.{timestamp}` and start fresh, emitting a `tracing::error!`.

**Validation:** Truncate a redb file; assert open succeeds with an empty store and the corrupt file is renamed.

---

### P1-10 · Iceberg backend is entirely in-memory

**Crates:** `krishiv-lakehouse/src/lib.rs`  
**Finding:** All `LakehouseTable` implementations use `tokio::sync::Mutex<Vec<RecordBatch>>`. The `iceberg` crate is only used for `From<iceberg::Error>`. No Parquet files are read or written.

**Mitigation (phased):**

1. R18 scope: Implement `IcebergFsTable` backed by `object_store` + the `iceberg` crate's `Table` API (read path first: `table.scan().to_arrow().await`).
2. Write path: implement `IcebergWriter` using the iceberg crate's `DataFileWriter` + `AppendFiles` transaction API.
3. Gate behind `features = ["iceberg-fs"]` to keep default builds buildable without Iceberg catalog access.

**Validation:** Write 100 rows to `IcebergFsTable`; restart process; read back 100 rows.

---

### P1-11 · Lakehouse: snapshot commits are not atomic — TOCTOU race

**Crates:** `krishiv-lakehouse/src/lib.rs`  
**Finding:** `check_write_precondition` reads the snapshot ID and `append` increments it as a separate operation. Concurrent appenders can both pass the precondition check and both commit, silently duplicating data.

**Mitigation:** Combine the check and the increment into one atomic operation. For `MemoryLakehouseTable`, hold a single `Mutex` across the entire `check + append` sequence. For the real Iceberg implementation, use optimistic concurrency control via the Iceberg snapshot commit protocol.

**Validation:** `cargo test -p krishiv-lakehouse -- concurrent_append_no_data_duplication`.

---

### P1-12 · Lakehouse: `MemoryLakehouseTable::scan` ignores `snapshot_id`

**Crates:** `krishiv-lakehouse/src/lib.rs`  
**Finding:** `IcebergScanOptions::snapshot_id` is set in tests but never read in `scan()`. Time-travel reads return the full current table.

**Mitigation:** Store batch history keyed by snapshot ID. `scan()` with a `snapshot_id` returns only the batches committed at or before that snapshot.

**Validation:** `cargo test -p krishiv-lakehouse -- time_travel_returns_historical_snapshot`.

---

### P1-13 · Connectors: `FeatureStoreSink` overwrites file on every append

**Crates:** `krishiv-connectors/src/feature_store.rs`  
**Finding:** `flush_parquet` uses `std::fs::File::create` (which truncates), writing only the new batch. All history is lost on process restart.

**Mitigation:** Open the file with append semantics and write each batch as a new Parquet row group, OR maintain a manifest of Parquet fragment files and write each batch to a new fragment. The in-memory `Vec<FeatureRow>` can serve as the read cache.

**Validation:** Append 3 batches; restart process; read back — assert all 3 batches present.

---

### P1-14 · Connectors: Kafka offsets committed before sink write completes

**Crates:** `krishiv-connectors/src/cdc.rs`  
**Finding:** `RdkafkaCdcEventSource::commit_offsets` is called inside `poll_events`, before `on_batch` (the Iceberg write) runs. If the sink write fails, the offset has already been committed and those CDC events are lost, breaking at-least-once.

**Mitigation:** Commit Kafka offsets only after `on_batch` returns `Ok(())`. Pass an `OffsetCommitter` callback to `poll_events` that callers invoke explicitly after successful sink writes.

**Validation:** `cargo test -p krishiv-connectors -- cdc_offset_not_committed_on_sink_failure`.

---

### P1-15 · Connectors: CDC offset tracking is in-memory only

**Crates:** `krishiv-connectors/src/cdc.rs`  
**Finding:** `CdcOffsetTracker` stores offsets in a `HashMap`. On restart, CDC replay starts from the beginning.

**Mitigation:** Persist committed offsets to `RedbStateBackend` under a dedicated namespace `"cdc_offsets"`. Load on startup and resume from the last committed offset.

**Validation:** Commit 5 offsets; restart; assert source resumes from offset 5.

---

### P1-16 · Checkpoint: `checkpoint_ack` fencing token not validated at coordinator

**Crates:** `krishiv-scheduler/src/checkpoint.rs`  
**Finding:** The `fencing_token` in `CheckpointAckRequest` is not compared against the coordinator's current fencing token before accepting the ACK. A superseded coordinator can still commit checkpoint epochs.

**Mitigation:** In `handle_checkpoint_ack`, extract the fencing token and compare it to `self.current_fencing_token()`. Return `Status::failed_precondition` if stale.

**Validation:** `cargo test -p krishiv-scheduler -- stale_ack_rejected`.

---

### P1-17 · Checkpoint ACK never delivered from executor to coordinator

**Crates:** `krishiv-executor/src/runner.rs`, `src/main.rs`  
**Finding:** `TaskRunner::handle_initiate_checkpoint` produces a `CheckpointAckRequest` but `main.rs` has no code path to deliver it via gRPC.

**Mitigation:** After `handle_initiate_checkpoint` returns the ACK request, pass it to `GrpcCoordinatorService::checkpoint_ack()`. Wire this through the `ExecutorRuntime` or a dedicated channel.

**Validation:** `cargo test -p krishiv-executor -- checkpoint_ack_delivered`.

---

### P1-18 · Policy: no row-level security

**Crates:** `krishiv-sql-policy/src/lib.rs`, `krishiv-governance/src/lib.rs`  
**Finding:** The `PolicyHook` trait has no `row_filter()` or `predicate()` method. Column masking is post-execution. Aggregates, row counts, and side-channels can leak data from restricted rows.

**Mitigation:**

1. Add `fn row_predicate(&self, principal: &Principal, table: &str) -> Option<String>` to `PolicyHook`.
2. In `execute_as()`, if a predicate is returned, inject it as a WHERE clause before passing the query to DataFusion: `SELECT * FROM ({original_sql}) AS __t WHERE {predicate}`.
3. Move column masking to an `AnalyzerRule` in DataFusion that rewrites column references to `CASE WHEN policy(col) THEN masked_value ELSE col END` before planning.

**Validation:** `cargo test -p krishiv-sql-policy -- row_level_predicate_applied_before_execution`.

---

### P1-19 · Executor heartbeat never reports running task attempts

**Crates:** `krishiv-executor/src/transport.rs`  
**Finding:** `heartbeat_request()` always sends `running_attempts: Vec::new()`. The coordinator cannot reconcile which tasks are alive during reconnection.

**Mitigation:** Track running tasks in `ExecutorRuntime` via a `DashMap<AttemptId, TaskState>`. Populate `running_attempts` from the map on every heartbeat.

**Validation:** `cargo test -p krishiv-executor -- heartbeat_includes_running_attempts`.

---

### P1-20 · `KafkaSource`/`KafkaSink` return `Unsupported` permanently

**Crates:** `krishiv-connectors/src/kafka.rs`  
**Finding:** The public `KafkaSource` and `KafkaSink` always return `ConnectorError::Unsupported`. The `kafka` feature only enables `RdkafkaKafkaSource`, which is a different type not implementing the public `Source` trait.

**Mitigation:** Either:
- Rename `RdkafkaKafkaSource` to `KafkaSource` (behind `features = ["kafka"]`) so the feature flag gates the entire public type, OR
- Implement the `Source` trait on `RdkafkaKafkaSource` and re-export it as `KafkaSource` when the feature is active.

Remove the permanent-`Unsupported` stubs or make them `#[cfg(not(feature = "kafka"))]` compile-time stubs with a clear error message.

**Validation:** `cargo test -p krishiv-connectors --features kafka -- kafka_source_reads_batches`.

---

### P1-21 · `AggregateUdf` and `TableUdf` not wired to DataFusion SQL

**Crates:** `krishiv-sql/src/udf.rs`, `krishiv-udf/src/lib.rs`  
**Finding:** `sync_scalar_udfs()` bridges `ScalarUdf` to DataFusion. No equivalent exists for `AggregateUdf` or `TableUdf`. UDAFs and UDTFs registered with `UdfRegistry` are silently ignored.

**Mitigation:** Add `sync_aggregate_udfs()` and `sync_table_udfs()` functions in `krishiv-sql/src/udf.rs` following the pattern of `sync_scalar_udfs()`. Call them from `Session::register_aggregate_udf()` and `Session::register_table_udf()`.

**Validation:** `SELECT my_sum(x) FROM t WHERE x > 0` → returns correct aggregate via a registered UDAF.

---

## P2 — Integration Gaps and Quality

### P2-1 · Core plan types lack `Serialize`/`Deserialize`

**Crates:** `krishiv-plan/src/lib.rs`  
**Finding:** `LogicalPlan`, `PhysicalPlan`, `NodeOp`, etc. have no serde derives. Cross-node plan dispatch requires custom encoding.

**Mitigation:** Add `#[derive(Serialize, Deserialize)]` gated on a `serde` feature flag. Audit all fields for serde compatibility (Arc fields need `serde_with` or custom impls).

---

### P2-2 · DataFusion types leak through `SqlDataFrame`

**Crates:** `krishiv-sql/src/lib.rs`  
**Finding:** `SqlDataFrame` holds a `DataFusionDataFrame` field. Long-term public API stability is compromised.

**Mitigation:** Introduce a `KrishivDataFrame` wrapper that hides the DataFusion type behind a `trait KrishivDataFrameInner`. `SqlDataFrame` becomes the concrete impl; the public API only exposes `KrishivDataFrame`.

---

### P2-3 · `krishiv-optimizer` has no production rules

**Crates:** `krishiv-optimizer/src/lib.rs`  
**Finding:** Only test-only `NoOpRule`, `AddNodeRule` exist. No predicate pushdown, projection pruning, or join reordering.

**Mitigation (prioritized order):**
1. `ProjectionPruningRule` — remove unused columns early to reduce shuffle/sort footprint.
2. `PredicatePushdownRule` — push filters below joins and into connectors.
3. `ConstantFoldingRule` — evaluate compile-time expressions.
4. `JoinReorderingRule` — order joins by estimated row counts (requires a `CostModel`).

---

### P2-4 · `CoalescePartitions` never executed

**Crates:** `krishiv-exec`, `krishiv-scheduler`  
**Finding:** `CoalesceRule` annotates the physical plan but no executor operator reads `coalesced_partition_count` or implements partition merging.

**Mitigation:**

1. Add `CoalescePartitionsOperator` to `krishiv-exec` that reads N input Arrow streams and merges them into one output stream.
2. In `krishiv-scheduler`, when dispatching a stage with `coalesced_partition_count < original_partition_count`, assign a coalescing task to one executor.

---

### P2-5 · No object-store `CheckpointStorage` backend

**Crates:** `krishiv-checkpoint`  
**Finding:** Only local FS. Production deployments need S3/GCS/Azure.

**Mitigation:** Implement `ObjectStoreCheckpointStorage<S: ObjectStore>` using the `object_store` crate. Use `put_multipart` for atomic writes. Include a 5-minute write timeout.

---

### P2-6 · `krishiv-testkit` is empty

**Crates:** `krishiv-testkit/src/lib.rs`  
**Finding:** The crate declares `#![forbid(unsafe_code)]` and nothing else. Every crate defines its own local test helpers.

**Mitigation:** Populate with:
- `fn make_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch`
- `fn make_i32_batch(values: &[i32]) -> RecordBatch`
- `struct MockSource` — configurable Arrow batch emitter
- `struct MockSink` — collects batches for assertion
- `struct TestSession` — in-memory session with pre-registered tables
- `fn assert_batches_eq(actual: &[RecordBatch], expected: &[RecordBatch])`

---

### P2-7 · Nexmark benchmarks do not use Arrow or DataFusion

**Crates:** `krishiv-bench/benches/nexmark.rs`  
**Finding:** Benchmarks operate on `Vec<u64>` with plain arithmetic. They measure nothing about the query engine.

**Mitigation:** Rewrite using `Session::sql()` on Arrow record batches with the relevant Nexmark schemas (bid, auction, person). Implement at minimum Q1, Q2, Q5, Q8 (join-heavy).

---

### P2-8 · `PolicyEnforcingSqlEngine` not wired into Flight SQL

**Crates:** `krishiv-flight-sql/src/lib.rs`  
**Finding:** `KrishivFlightSqlService` reimplements table access checks and column masking independently of `PolicyEnforcingSqlEngine`, creating divergence risk.

**Mitigation:** Replace the ad-hoc policy check in `do_get` with a call to `PolicyEnforcingSqlEngine::execute_as()`. Remove the duplicate masking loop.

---

### P2-9 · `LanceDbSink` loses all data on restart

**Crates:** `krishiv-vector-sinks/src/lancedb_sink.rs`  
**Finding:** `open()` initializes an empty in-memory index; existing Parquet fragments are never loaded.

**Mitigation:** On `open()`, scan the `{uri}/{table}/` directory for existing Parquet fragments, read them via `ParquetRecordBatchReader`, and populate the in-memory `InMemoryVectorSink` index.

---

### P2-10 · `QdrantSink::query_nearest` returns empty payload

**Crates:** `krishiv-vector-sinks/src/qdrant.rs`  
**Finding:** `text` and `chunk_index` are stored but not extracted from query results.

**Mitigation:** Parse `scored_point.payload["text"]` and `scored_point.payload["chunk_index"]` from the Qdrant query response and populate `ScoredChunk` fields.

---

### P2-11 · `spark_compat.rs` and `spark_compat_date.rs` orphaned modules have bare `.unwrap()` in UDF closures

**Crates:** `krishiv-sql/src/spark_compat.rs`, `src/spark_compat_date.rs`  
**Finding:** Production UDF closures call `downcast_ref::<Float64Array>().unwrap()` and `.unwrap()` on date arrays. Wiring these modules (P0-1) will introduce panic paths in the query executor.

**Mitigation:** Replace with `downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Execution("expected Float64".into()))?` before P0-1 lands.

---

### P2-12 · `LocalAggregator` serializes group keys to String — Float64 and Timestamp unsupported

**Crates:** `krishiv-exec/src/aggregate.rs`  
**Finding:** `format_key_value` round-trips keys through `String`. Float64 keys fail with `UnsupportedType`. No `AggFunction::Avg` for Float64.

**Mitigation:** Replace the string-keyed map with a typed key enum: `enum AggKey { Int32(i32), Int64(i64), Float64(OrderedFloat<f64>), Utf8(String), Bool(bool) }`. Add `AggFunction::Avg` for numeric types.

---

### P2-13 · Upgrade tests parse JSON to `serde_json::Value`, not typed structs

**Crates:** `krishiv-upgrade-tests`  
**Finding:** Breaking renames in `CheckpointMetadata`, `JobSpec`, etc. are not caught.

**Mitigation:** Deserialize directly to the typed struct: `serde_json::from_str::<CheckpointMetadata>(blob)`. Also add reverse-compat tests (new blob, old reader).

---

### P2-14 · `RedbStateBackend` has no recovery path for corrupt database

**Crates:** `krishiv-state/src/lib.rs`  
**Finding:** `Database::create` on a corrupt redb file fails permanently.

**Mitigation:** On failure, attempt rename to `{path}.corrupt.{ts}` and create a fresh database. Log at `error!` level. Add a metric counter `krishiv_state_backend_corruption_total`.

---

### P2-15 · AI: LSH is O(n²) and produces high collision rate

**Crates:** `krishiv-ai/src/dedup.rs`  
**Finding:** `lsh_candidates` iterates all pairs nested, providing no sublinear speedup. Band signatures (sum of floats) have very high collision rate.

**Mitigation:** Use proper MinHash bands: hash each sub-band to a `u64` using `AHash`, bucket items by band-hash, then only compare within each bucket. This gives O(n) expected candidates per item at the cost of a bounded false-negative rate.

---

### P2-16 · AI: `KeepHighestScore` dedup strategy is broken

**Crates:** `krishiv-ai/src/dedup.rs`  
**Finding:** Both `KeepHighestScore` and `KeepFirst` branches `drop.insert(j)`. No score is available to compare.

**Mitigation:** Change `dedup_indices` signature to accept scores: `fn dedup_indices(embeddings: &[Vec<f32>], scores: &[f32], ...)` and implement `KeepHighestScore` to keep the item with the higher score when similarity exceeds the threshold.

---

### P2-17 · Python: Arrow-to-Python conversion is row-by-row

**Crates:** `krishiv-python/src/lib.rs`  
**Finding:** Result batches are converted to `PyList<PyDict>` row-by-row. For large result sets this is extremely slow.

**Mitigation:** Use `arrow2` or `PyArrow` via the `pyo3-arrow` crate for zero-copy record batch transfer. Call `PyArrow::import_array(batch)` and return `PyArrow` objects that pandas/polars can consume directly.

---

## P3 — Technical Debt and Low-Risk Panics

| ID | Crate | Finding | Fix |
|---|---|---|---|
| P3-1 | `krishiv-shuffle` | `RwLock::write().unwrap()` throughout store impls | Replace with `map_err(|_| ShuffleError::LockPoisoned)` |
| P3-2 | `krishiv-shuffle` | `HashPartitioner` uses `expect()` on `downcast_ref` | Use `ok_or(ExecError::TypeMismatch)` |
| P3-3 | `krishiv-shuffle` | TCP ticket reader has no length cap | Add `max_ticket_len: usize = 65536` cap with hard close |
| P3-4 | `krishiv-shuffle` | `HashPartitioner` does not handle null key columns | Check `arr.is_null(row)` and route nulls to a deterministic partition |
| P3-5 | `krishiv-checkpoint` | `.tmp` concurrent write TOCTOU for same path | Add UUID suffix to temp file name |
| P3-6 | `krishiv-checkpoint` | `list_valid_epochs` swallows I/O errors as invalid | Log at `warn!` and propagate; do not silently exclude |
| P3-7 | `krishiv-state` | `InMemoryProcessingTimeTimerService::cancel` is O(N) | Add identity index `HashMap<TimerId, BTreeKey>` |
| P3-8 | `krishiv-state` | `SharedStateMigrationRegistry` `RwLock::unwrap()` | Propagate poison error as `StateError` |
| P3-9 | `krishiv-ai` | `spawn_blocking(block_on(async))` in `call_one` | Replace with `async fn call_one`; drop `spawn_blocking` |
| P3-10 | `krishiv-ai` | `for_model` mutex `expect()` panics on poison | Use `unwrap_or_else(|p| p.into_inner())` and log at error |
| P3-11 | `krishiv-ai` | `TokenAwareChunker` uses `len/4` proxy | Integrate `tiktoken-rs` behind a feature flag |
| P3-12 | `krishiv-ai` | RAG memo stores only last chunk per document hash | Change memo key to `(content_hash, chunk_index)` |
| P3-13 | `krishiv-ai` | `MemoStore` has no TTL | Add `created_at_ms: u64` to `MemoEntry`; evict on `get` |
| P3-14 | `krishiv-governance` | Audit dedup is `thread_local` — per-thread, not global | Use a `DashMap` for global dedup with TTL expiry |
| P3-15 | `krishiv-governance` | `AuditSink::record` is sync — no async sink possible | Add `async fn record_async` to `AuditSink` with a default impl |
| P3-16 | `krishiv-governance` | `AuditAction` uses `&'a str` — can't cross await points | Change to `String` (owned) |
| P3-17 | `krishiv-metrics` | `MetricsHandle::shutdown()` calls `drop(self)` redundantly | Remove the `drop(self)` call; it is a no-op |
| P3-18 | `krishiv-scheduler` | `generate_id()` uses `.expect()` | Return `KrishivError` instead |
| P3-19 | `krishiv-scheduler` | `CoordinatorState::Standby` has no promotion path | Wire leader election result to `promote_to_active()` (see P0-5) |
| P3-20 | `krishiv-scheduler` | Auth entirely unenforced | Validate Bearer token against `StaticApiKeyAuthProvider` as a minimum |
| P3-21 | `krishiv-ui` | `shuffle_bytes_written_total` hardcoded to 0 | Use `metrics.shuffle_bytes_written` |
| P3-22 | `krishiv-ui` | `/readyz` doesn't check coordinator is Active | Mirror the coordinator binary's `/readyz` logic |
| P3-23 | `krishiv-catalog` | `register_table` silently overwrites | Return `CatalogError::TableAlreadyExists` if present and `if_not_exists=false` |
| P3-24 | `krishiv-catalog` | `FieldType::List` hardcodes `Int64` item type | Add `List(Box<FieldType>)` variant |
| P3-25 | `krishiv-catalog` | `FieldType::Struct` maps to empty struct | Add `Struct(Vec<CatalogField>)` variant |
| P3-26 | `krishiv-federation` | `FederationClient` trait is synchronous | Change to `async fn` trait methods (or `async_trait`) |
| P3-27 | `krishiv-cep` | `SequentialPatternMatcher` has no partitioned-key wrapper | Add `PartitionedCepMatcher<K: Hash+Eq>` wrapping `HashMap<K, SequentialPatternMatcher>` |
| P3-28 | `krishiv-chaos` | `FaultMode::Delay/Drop` never actually executed | Add a `FaultInjector::apply(mode, conn)` method that intercepts real calls |
| P3-29 | `krishiv-upgrade-tests` | `CURRENT_VERSION` is a local constant | Import `SCHEMA_VERSION` from `krishiv-checkpoint` |
| P3-30 | `krishiv-python` | `into_pyobject(py).unwrap()` in UDF conversion | Use `?` and map to `PyRuntimeError` |

---

## Execution Sequence

### Sprint 1 (P0 — fix before any functional testing)

1. P0-1 + P0-2: Wire orphaned modules and add missing enum variants.
2. P0-3: Fix connector `lib.rs` module declarations.
3. P0-4: Wire coordinator/operator heartbeat and task-launch tick loops.
4. P0-7 + P0-8: Fix executor gRPC connection pooling and lease update.
5. P0-9 + P0-10: Fix catalog `EmptyExec` and wire into `krishiv-sql`.
6. P0-12: Wire Python module declarations (fix dead-code crisis).
7. P0-13: Fix `rag_query` shared sink.

Run: `cargo check --workspace && cargo test --workspace`

### Sprint 2 (P0 remaining + P1 high-value)

8. P0-5 + P0-6: Wire K8s leader election and finalizer.
9. P0-11 + P0-14 + P0-15: Fix Weaviate sink, Spark CAST type, and audit call sites.
10. P0-16: Add OTel Metrics API and Prometheus endpoint.
11. P1-1 + P1-2 + P1-3: Checkpoint fencing, epoch monotonicity, fsync.
12. P1-4 + P1-5 + P1-6: Shuffle spill, compression, and lease enforcement.
13. P1-7 + P1-8: State TTL list_keys and compaction.
14. P1-16 + P1-17: Checkpoint ACK fencing and delivery.

### Sprint 3 (P1 remaining + P2 integration)

15. P1-9 + P1-10 + P1-11: Iceberg atomicity, time-travel, and real implementation start.
16. P1-12 + P1-13 + P1-14 + P1-15: Connectors correctness.
17. P1-18: Row-level security.
18. P1-20 + P1-21: Kafka connector and UDF wiring.
19. P2-6: Populate `krishiv-testkit`.
20. P2-3 + P2-4: First optimizer rules and CoalescePartitions execution.
21. Remaining P2 items.

### Sprint 4 (P3 cleanup)

22. All P3 items: Replace `unwrap()` calls, fix O(N) cancel, async AuditSink, federation sync→async.

---

## Quick Reference: Validation Commands

```bash
# All compilation
cargo check --workspace

# Full test suite
cargo test --workspace

# Critical subsystems
cargo test -p krishiv-scheduler -- task_launch_drives_to_running
cargo test -p krishiv-executor  -- heartbeat_includes_running_attempts lease_generation_updated_after_reregister
cargo test -p krishiv-checkpoint -- fencing_token_accepts_future_generation stale_epoch_rejected
cargo test -p krishiv-shuffle   -- spills_to_disk_at_memory_limit zombie_write_rejected_by_lease
cargo test -p krishiv-state     -- list_keys_excludes_expired
cargo test -p krishiv-connectors -- cdc_offset_not_committed_on_sink_failure
cargo test -p krishiv-sql-policy -- row_level_predicate_applied_before_execution audit_events_emitted_on_execute
cargo test -p krishiv-vector-sinks -- weaviate_query_returns_results
cargo test -p krishiv-catalog   -- catalog_table_resolved_in_sql
```
