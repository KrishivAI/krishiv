# Krishiv Implementation Status

## Current Phase

**R13 COMPLETE (2026-05-23).**

Release tracker: [`r13-python-streaming-api.md`](r13-python-streaming-api.md)  
Gap register: [`docs/architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md)

## R13 Python-First Streaming API — Complete

### Delivered

| Area | Summary |
|------|---------|
| **GAP-RT-03** | `WindowedStream::plan_fragment()` for `stream:tw:` executor lowering |
| **GAP-RT-08** | `Stream` carries `coordinator_url` through `collect_bounded` |
| **GAP-OB-01** | Coordinator `/metrics` exposes scheduler hot-path counters |
| **GAP-PY-01** | PyO3 package: `Schema`, transform chain, `Batch.to_arrow`/`to_pandas`, async iteration, sinks helpers, `.pyi` stubs |
| **Deployment modes** | `Session.embedded/local/connect/from_env` with `KRISHIV_COORDINATOR` and `ModeError` guards |
| **CI** | `python-package.yml` (maturin + pytest); `python-wheels.yml` matrix (linux + macOS) |

### Known limits (documented, not R13 blockers)

- `DistributedBackend` accepts plans locally without a full Flight SQL client (GAP-RT-01 → R14).
- `read_kafka` / kafka pipelines require `krishiv[kafka]` build and live brokers for integration tests.
- `WindowedStream` materialization uses SQL aggregation for local bounded paths; executor push-down evolves in R14.

### Validation

```
cargo test --workspace --lib
cargo clippy --workspace -- -D warnings
maturin develop -m crates/krishiv-python --release
pytest crates/krishiv-python/python/tests/
```

### Next Task

R14 — observability hardening, structured tracing on executor task loop, integration tests for remote coordinator + Flight SQL client.

Validation: `cargo test --workspace && cargo clippy --workspace -- -D warnings`
