# Krishiv Implementation Status

## Current Phase

**R13 in progress (2026-05-23).**

Release tracker: [`r13-python-streaming-api.md`](r13-python-streaming-api.md)  
Gap register: [`docs/architecture/r12-maturity-gap-register.md`](../architecture/r12-maturity-gap-register.md)

## R13 Session (2026-05-23)

### Completed

| Area | Change |
|------|--------|
| **GAP-RT-03** | `WindowedStream::plan_fragment()` lowers windowed streams to `stream:tw:` executor fragments |
| **GAP-RT-08** | `Stream` carries `coordinator_url`; `collect_bounded` passes it to `accept_plan_with_backend` |
| **GAP-OB-01** | Coordinator `/metrics` exposes `krishiv_jobs_submitted_total`, checkpoint epochs, tasks assigned |
| **Governance** | Audit dedup uses thread-local keys; dedup test serialized to avoid parallel interference |
| **Python** | `python/krishiv/py.typed`, session-mode pytest smoke tests, CI `python-package.yml` workflow |
| **Python facade** | `connect_async` and factory methods documented in `python/krishiv/__init__.py` |

### Remaining for full R13 acceptance

- Python transformation chain (`Schema`, `key_by`, `with_watermark`, `agg`, PyArrow/Pandas bridges) — partial; extend `krishiv-python` beyond factory methods
- `WindowedStream.__anext__` wired to executor streaming loop (currently stub / SQL materialization follow-up)
- `DistributedBackend` Flight SQL client (GAP-RT-01)
- maturin wheel matrix publish + `.pyi` stubs in tree

### Validation

```
cargo test --workspace --lib    → pass
cargo test -p krishiv-api --lib windowed_stream_plan_fragment_matches_executor_format → pass
```

### Next Task

Wire `WindowedStream.__anext__` to bounded SQL/executor results and land `Schema` + transform chain in `krishiv-python`.

Validation: `maturin develop -m crates/krishiv-python && pytest crates/krishiv-python/python/tests/`
