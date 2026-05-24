# Unified Execution Model

**Status:** Implemented (2026-05-24)  
**Related:** [streaming-execution-model.md](./streaming-execution-model.md), ADR-13.1–13.7 in [architectural-decisions-r12-r20.md](./architectural-decisions-r12-r20.md)

## Summary

Krishiv uses **one execution pipeline** for batch and streaming workloads. Deployment mode only changes the **transport** to the coordinator and executor:

| Mode | Transport | Batch SQL (R1) | Bounded streaming windows |
|------|-----------|----------------|---------------------------|
| **Embedded** | In-process cluster per `Session` | DataFusion via `SqlEngine` | `ExecutionRuntime` → coordinator → executor |
| **SingleNode** | In-process or loopback gRPC + Flight | Same + optional `KRISHIV_COORDINATOR` | Same as embedded |
| **Distributed** | Remote Flight/gRPC | Flight SQL + local fallback | `ExecutionRuntime` with cluster fallback |

## Pipeline

```text
API (sql / window.collect / submit_stream_job)
  → LogicalPlan / WindowExecutionSpec
  → PhysicalPlan (typed NodeOp)
  → ExecutionRuntime::accept_plan
  → Coordinator::submit_job
  → ExecutorTaskRunner (stream:* fragments)
  → krishiv-exec::execute_bounded_window (canonical operators)
```

## Fragment encoding

Streaming tasks use typed fragments:

- `stream:tw:` — tumbling
- `stream:sw:` — sliding (`:slide=`)
- `stream:ses:` — session (`:gap=`)
- Optional `:ttl=` for state TTL on tumbling

Parsed and encoded in `krishiv-plan::window`.

## Local cluster (Spark-like)

```bash
krishiv local start
export KRISHIV_COORDINATOR=http://127.0.0.1:50051
krishiv sql --mode single-node --query 'SELECT 1'
krishiv local stop
```

## Invariants

- Watermark advances per **batch max event time**, then `process_batch(whole batch)`.
- Session holds one `InProcessCluster` for the lifetime of the session.
- Embedded and SingleNode use the same operator path; SingleNode may additionally connect to a daemon cluster.
