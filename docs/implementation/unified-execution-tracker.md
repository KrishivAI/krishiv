# Unified Execution Tracker

## Completed (2026-05-24)

- [x] `krishiv-plan::window` — `WindowExecutionSpec`, fragment encode/parse (tw/sw/ses, TTL, aggs)
- [x] `krishiv-exec::operator_runtime` — unified bounded window execution (all window kinds)
- [x] `krishiv-runtime::InProcessCluster` — session-scoped coordinator
- [x] `krishiv-runtime::ExecutionRuntime` — Embedded / SingleNode / Distributed routing
- [x] Executor streaming fragment delegates to `execute_bounded_window`
- [x] API `Session` owns runtime; window collect unified across modes
- [x] Python `stream_exec` uses `execution_runtime()`
- [x] `krishiv local start|stop|status`
- [x] `Session::submit_stream_job` for continuous jobs (plan acceptance)
- [x] **Follow-up:** Batch SQL via coordinator (`collect_batch_sql` / `sql:` tasks)
- [x] **Follow-up:** Distributed `Session.sql()` + Flight full result drain
- [x] **Follow-up:** Continuous streaming drain (`stream:continuous:`, `poll_stream_job`)
- [x] **Follow-up:** Multi-source watermark in `execute_bounded_window`
- [x] **Gap closure:** `accept_plan` local fallback; distributed read_parquet/window/memory stream parity
- [ ] **R2+:** True remote execution (disable local fallback, Flight catalog sync, policy via coordinator)

See [unified-execution-mode-gaps.md](unified-execution-mode-gaps.md) for full mode parity analysis.
- [x] Tests: sliding/session windows, coordinator reuse, fragment roundtrip

## Validation

```bash
cargo +stable test -p krishiv-plan -p krishiv-exec -p krishiv-runtime -p krishiv-api -p krishiv-executor --lib
```
