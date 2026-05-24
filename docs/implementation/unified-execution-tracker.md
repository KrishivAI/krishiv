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
- [x] Tests: sliding/session windows, coordinator reuse, fragment roundtrip

## Validation

```bash
cargo +stable test -p krishiv-plan -p krishiv-exec -p krishiv-runtime -p krishiv-api -p krishiv-executor --lib
```
