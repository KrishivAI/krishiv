# Krishiv Implementation Status

## Current Phase

**R13 COMPLETE (2026-05-23)** — including deferred GAP-RT-01, native asyncio, and connector integration tests.

Release tracker: [`r13-python-streaming-api.md`](r13-python-streaming-api.md)

## R13 Closure (deferred items completed)

| Item | Implementation |
|------|----------------|
| **GAP-RT-01** | `DistributedBackend` submits plans via Arrow Flight SQL (`krishiv-runtime/src/flight_client.rs`) |
| **Native asyncio** | `WindowedStream.__anext__` uses `pyo3-async-runtimes` + `future_into_py` (no `asyncio.to_thread`) |
| **Kafka / Iceberg tests** | `python/tests/test_connectors_integration.py` (local handles + optional live env-gated tests); `read_iceberg()` added |
| **Flight integration** | `distributed_backend_submits_plan_over_flight_sql` spawns in-process Flight SQL server |

### Validation

```
cargo test -p krishiv-runtime --lib
cargo test -p krishiv-python --lib
pytest crates/krishiv-python/python/tests/
```

Optional live connector smoke (CI or laptop):

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export ICEBERG_CATALOG_URI=http://localhost:8181
pytest crates/krishiv-python/python/tests/test_connectors_integration.py -m integration
```

### Next Task

R14 — production observability, executor task-loop tracing, and deeper connector push-down (real Kafka/Iceberg execution in the streaming runtime).
