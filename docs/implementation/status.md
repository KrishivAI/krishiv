# Krishiv Implementation Status

## Current Session (Completed)

### Bug / Gap / Bottleneck Fixes (Post-Audit)

Five fixes applied from the prior session audit, in priority order:

**Fix 1 — `ipc_catalog` per-request scoping** (`crates/krishiv-flight-sql/src/host.rs`)
- Removed shared `ipc_catalog: Arc<DashMap<String, String>>` field from `FlightExecutionHost`.
- Added `collect_ipc_tables(directives)` free function: builds per-call inline table list from
  this request's `RegisterParquetIpc` directives only — not persisted to shared state.
- `execute_sql` now passes per-request IPC tables to `execute_coordinator_batch_sql_inline`
  without writing to any shared DashMap. Concurrent callers cannot observe each other's tables.
- `apply_catalog_directives` now only handles path-based `RegisterParquet` registrations.

**Fix 2 — Rename `window_job_partitions` → `job_input_partitions`**
  (`coordinator/mod.rs`, `coordinator/task_assignment.rs`, `batch_sql.rs`, `bounded_window.rs`)
- Field and method renamed throughout. Old name implied window-only; map holds inline IPC
  inputs for both batch-sql and bounded-window jobs.

**Fix 3 — GC `job_input_partitions` and `batch_sql_job_tables` on terminal job state**
  (`coordinator/job_lifecycle.rs`)
- Both maps `.remove()`'d when a job reaches terminal state (succeeded, failed, cancelled),
  in both the `update_task_status` terminal path and the `cancel_job` path.
- Eliminates the unbounded coordinator memory leak that accumulated input data across all jobs.

**Fix 4 — Remove redundant synchronous `POST /api/v1/batch-sql` endpoint**
  (`batch_sql_http.rs`, `coordinator_daemon.rs`)
- Removed `api_batch_sql`, `BatchSqlResponse`, and the `/api/v1/batch-sql` POST route.
- Only async paths remain: `POST /api/v1/batch-sql/submit` + `GET /api/v1/batch-sql/{job_id}`.

**Fix 5 — `spawn_blocking` for parquet-to-IPC conversion** (`coordinator_http_client.rs`)
- `parquet_to_ipc_b64` blocks on file I/O and CPU encoding.
- Wrapped in `tokio::task::spawn_blocking` so async executor threads are not stalled.

## Validation

```
cargo check --workspace    # 0 errors
```

### Rust examples — all 12 pass (embedded mode)

| Example | Output |
|---------|--------|
| `batch_sql` | London=2, Paris=1 |
| `batch_iot_sensor` | device-1 avg_temp=23.3, device-2=18.75 |
| `batch_ecommerce` | VIP=2 orders $249.9, Standard=1 $45.5 |
| `batch_delta_audit` | Current=3 rows, Historical v0=2 rows |
| `batch_hudi_ingest` | 3 users ingested, snapshot read |
| `batch_log_analytics` | payment-service 50% error rate |
| `memory_stream` | [1, 2, 3] |
| `stream_transaction_count` | Alice/Bob tumbling windows |
| `stream_session_window` | Alice sessions 1000-18000, 20000-30000 |
| `stream_state_ttl` | Alice windows with TTL gap |
| `stream_multi_source` | device-1/device-2 sliding windows |
| `stream_continuous_job` | job submitted, 0 batches polled (expected) |

### Python examples — all 12 pass (embedded mode, `KRISHIV_MODE=embedded`)

| Example | Output |
|---------|--------|
| `batch_sql.py` | London=2, Paris=1 |
| `batch_iot_sensor.py` | device-1 avg_temp=23.3, device-2=18.75 |
| `batch_ecommerce.py` | VIP=2, Standard=1 |
| `batch_delta_audit.py` | Current=3 rows, Historical v0=2 rows |
| `batch_hudi_ingest.py` | 3 users ingested |
| `batch_log_analytics.py` | payment-service 50% error rate |
| `memory_stream.py` | [1, 2, 3] |
| `stream_transaction_count.py` | Alice/Bob windows |
| `stream_session_window.py` | Alice sessions |
| `stream_state_ttl.py` | Alice TTL windows |
| `stream_multi_source.py` | device sliding windows |
| `stream_continuous_job.py` | job submitted, 0 batches polled |

## What was NOT changed (correct as-is)

- `job_inline_results`: NOT GC'd on terminal state — client retrieves async results via
  `take_job_inline_results`; entries are consumed on first read.
- `execute_batch_sql_coordinated` in `batch_sql.rs`: kept for internal callers (tests, CLI).
  Only the HTTP handler was removed.
- `hostPath /tmp` in k8s manifest: safe to remove once fully on InlineIpc, not urgent.
- HTTP long-poll for batch SQL: client-side exponential backoff; server-push is future work.

## Next Steps

- gRPC message size cap for InlineIpc: validate byte count at `register_job_input_partitions`;
  reject submissions exceeding ~3 MB per partition.
- Circuit breaker reset API: `POST /api/v1/executors/{id}/reset` for operator recovery.
- Continuous stream coordinator routing through coordinator for durability.
- `session.deployment_target()` in OTLP metrics labels.
