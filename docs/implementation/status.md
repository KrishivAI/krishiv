# Krishiv Implementation Status

## Current Phase

**R8 COMPLETE (core items).** R7 is complete (commits `0618c61` through `931c824`). R8.1 Python/UDF/FlightSQL and R8.2 Iceberg lakehouse are implemented on branch `claude/plan-r7-implementation-lt3n3`.

## Completed (committed to branch)

| Commit | Content |
|--------|---------|
| `0618c61` | R7.1: Resource governance foundation (quotas, admission, cost metrics) |
| `b5570bb` | R7.2: Backpressure and adaptive governance (SpaceSaving, RateLimiter, barriers) |
| `3dec2a1` | docs: R7 tracker + status updated |
| `8509663` | pre-R8: HashMap job index, auth interceptor skeleton, R7 roadmap sync, R8 ADR |
| `6a1fc17` | pre-R8: TraceContext in proto + OperatorQueue wiring in streaming executor |
| `c867a62` | R8.1 Group A: `krishiv-udf` crate — ScalarUdf, AggregateUdf, TableUdf, UdfRegistry |
| `931c824` | R8.2: `krishiv-lakehouse` — Iceberg read/write beta, snapshot reads, optimistic concurrency |
| `63a8ae2` | R8.1 Group C: `krishiv-flight-sql` — Flight SQL thin adapter over Session |
| `c1af99e` | docs(R8): sync roadmap R8.1 and R8.2 checklists with completed items |
| `27e8c5c` | docs(R8): update status.md and R8 tracker with in-progress items |
| `1611103` | R8.1 Group B: `krishiv-python` PyO3 bindings — Session, DataFrame, Python UDF via spawn_blocking |

## R8 Deferred Items (not blocking R8 acceptance gate)

- Maturin build pipeline for manylinux wheels (deferred within R8.1)
- `.pyi` type stub generation (deferred within R8.1)
- Python `Stream` binding (bounded collect only; deferred within R8.1)
- Python connector wrappers (`read_parquet`, `read_kafka`, `read_iceberg`; deferred within R8.1)
- Python asyncio integration tests (need Python interpreter; deferred within R8.1)
- Iceberg partition evolution support (deferred within R8.2)
- Iceberg time travel SQL syntax (snapshot_id in scan options is the beta form; deferred within R8.2)

## Last Validation (R8 complete)

- `cargo test --workspace --exclude krishiv-python`: 0 failures
- `cargo test --lib -p krishiv-python`: 5 passed, 0 failed
- `cargo clippy --workspace --exclude krishiv-python -- -D warnings`: 0 warnings
- Branch: `claude/plan-r7-implementation-lt3n3`

## Next Steps

1. Begin R9 planning: end-to-end credit propagation, distributed tracing
2. Wire maturin build pipeline for Python wheel distribution (deferred R8.1 item)
3. Generate `.pyi` type stubs for IDE autocomplete (deferred R8.1 item)

## R8.1 Architecture Decisions (locked, see r8-python-flight-sql-adr.md)

- **UDF thread model**: `spawn_blocking` — GIL never held on Tokio worker thread
- **asyncio integration**: embedded Tokio runtime (`LazyLock<Runtime>`) in PyO3 module
- **Flight SQL routing**: thin adapter over `Session::sql_async()` — zero query-path divergence
- **Streaming UDFs**: deferred to post-GA, will use subprocess isolation (Arrow IPC over Unix socket)
- **Beta stability**: all Python/lakehouse public items carry beta annotation

## Architectural Inputs To Preserve

- Distributed mode targets: Kubernetes (primary), bare-metal/VM (secondary). Core runtime crates must stay deploy-target neutral. Kubernetes API in `krishiv-operator`.
- Control-plane: tonic gRPC + Protobuf. Bulk Arrow data uses Arrow IPC/Flight, not Protobuf.
- R4 shuffle: local executor disk, optional object-store. No S3 required for distributed execution.
- Pre-R9 coordinator/executor gRPC has no mTLS. Task specs must not contain credentials.
- R7.2 backpressure: intra-stage only. Cross-stage via `ThrottleCommand`. Full credit propagation deferred to R9.
- Adaptive repartitioning: batch-only, between stages, never mid-stage. Streaming hot-key follows savepoint model.
- `QueueManager.admit()` stateless — coordinator owns reservation state via `NamespaceQuotaSnapshot`.
- `CrdQueueManager` in `krishiv-operator` — no `kube` crate in `krishiv-scheduler`.
- Python UDF thread model: `spawn_blocking` — never hold GIL on Tokio worker.
- Streaming Python UDFs: post-GA, subprocess isolation (Arrow IPC over Unix socket).
- Flight SQL: thin adapter over `Session::sql_async()` — same planner/runtime as CLI.
