# Observability

Every Krishiv process exposes the same three signals — metrics, traces,
structured logs — through `krishiv-metrics`, and the coordinator adds job
events, per-stage diagnostics, and a console. This document lists what is
emitted, where to read it, and the instruments the engineering log relies on.

## Initialisation

`krishiv_metrics::init(MetricsConfig { service_name, exporter, log_filter,
otlp_endpoint, deployment_target })` installs the OpenTelemetry meter and
tracer, the `tracing` subscriber (JSON or text via `KRISHIV_LOG_FORMAT`;
filter via `RUST_LOG`/`log_filter`), and the OTLP exporter when an endpoint is
configured; without one the exporter is a no-op and Prometheus text on
`/metrics` is the read path. `current_traceparent()` / `current_tracestate()`
propagate W3C trace context across gRPC (`grpc.rs` interceptors), so one query
is one trace from CLI to executor.

## Metric families

All names are `krishiv_*`; the Prometheus renderer emits exactly one
`HELP`/`TYPE` per family.

| Area | Families |
|---|---|
| tasks and jobs | `tasks_total`, `tasks_running`, `task_attempts_total`, `job_queue_depth`, `executor_slots_used`, `executor_lost_total`, `query_latency_seconds` |
| shuffle | `shuffle_bytes_written_total`, `shuffle_records_written_total`, `shuffle_read_bytes_total`, `shuffle_read_records_total`, `shuffle_partitions`, `shuffle_local_blocks_fetched_total`, `shuffle_remote_blocks_fetched_total`, `shuffle_write_time_us_total`, `shuffle_read_time_us_total`, `shuffle_fetch_wait_time_us_total`, `spill_bytes_total`, `spill_files_total` |
| streaming | `stream_record_latency_seconds` (µs buckets; the run-loop exit-gate instrument), `streaming_rows_emitted_total`, `watermark_ms`, `source_offset_lag`, `source_read_duration_seconds`, `backpressure_duration_us_total`, `output_buffer_flushes_total`, `operator_memory_bytes` |
| checkpoints | `checkpoint_epoch`, `checkpoint_epochs_total`, `checkpoint_alignment_duration_seconds`, `checkpoint_upload_duration_seconds`, `checkpoint_commit_duration_seconds`, `restore_duration_seconds` |
| sinks | `sink_prepare_duration_seconds`, `sink_commit_duration_seconds`, `sink_abort_duration_seconds` |
| state | `state_bytes`, `state_key_count`, `state_cache_hits_total`, `state_cache_misses_total` |
| AQE | `aqe_*` decisions (coalesce, skew split, broadcast) per job |
| sessions and RPC | `active_sessions`, `session_statements_total`, `session_statements_rejected_total`, `grpc_call_duration_seconds`, `object_store_requests_total` |
| process and host (`system.rs`) | `process_cpu_usage`, `process_memory_bytes`, `process_virtual_memory_bytes`, `process_threads`, `system_cpu_usage`, `system_memory_bytes_total`, `system_memory_bytes_available` |

`krishiv_common::memory_budget::cgroup_memory_usage` splits RSS from page
cache so the SF100 OOM class (`05`) is visible without a re-investigation.

## Coordinator-side signals

| Signal | Where | Content |
|---|---|---|
| event log | `/api/v1/events` (in-memory ring, 64 MiB; RocksDB-persisted on single-node) | `JobSubmitted`, `StagePlanned`, `TaskAssigned/Started/Succeeded/Failed`, executor registration and loss |
| job history | `/api/v1/history[/{id}]` | terminal `JobHistoryRecord`s |
| stage detail | `/api/v1/jobs/{id}/stages` | per-stage task counts, shuffle bytes, retries |
| diagnose | `/api/v1/jobs/{id}/diagnose` | a structured explanation of why a job is slow or stuck: placement, lost executors, regeneration, queue wait |
| logs | `/api/v1/logs` | the process log ring (`log_ring.rs`) |
| metrics snapshot | `/api/v1/metrics-snapshot` | the Prometheus families as JSON for the console |
| observability report | `observability_report.rs`, `krishiv doctor` | one document tying config, profile, endpoints, auth state, and recent errors together |
| adaptive decision log | per job | every AQE decision with its measured trigger (`03`) |
| stability metrics | `StabilityMetrics` snapshot | retries, regenerations, lease bumps |
| IVM tick health | `StepSummary`, `TickHealth`, `RetainedState` | degraded/errored views, retained entry counts (`09`) |

## Per-query metrics

`krishiv explain --analyze` executes a query in-process and prints the
DataFusion physical plan annotated with per-operator rows,
`elapsed_compute`, build/probe times, scan and pruning counters, spills, and
peak memory. It is the instrument behind every optimizer decision in
`../engineering-log/crate-audit-register.md` §90–§97 and the reason
`--remote` is refused there: a distributed plan's metrics live on the
executors that ran it. In-process phase timelines (operator start/end
timestamps from `MetricsSet`) are captured through the same mechanism.

## Console and dashboards

The web console (`/console`, `11`) renders jobs, stages, executors, queues,
events, and a SQL editor over the HTTP API. `docs/grafana/krishiv-dashboard.json`
is a Grafana board over the Prometheus families above.

## Health

`/healthz` (liveness, on a dedicated thread so a wedged scheduler is still
distinguishable from a dead pod), `/readyz` (metadata store reachable, leader
or follower), `/leaderz` (leadership for Service routing). `krishiv doctor`
performs the same checks from outside plus environment validation.

## Related

- `04` (events, diagnose), `05` (capacity instruments), `08` (latency
  histogram), `16` (how measurements are taken so they mean something).
