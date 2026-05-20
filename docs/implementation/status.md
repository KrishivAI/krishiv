# Krishiv Implementation Status

## Current Phase

**R8 IN PROGRESS.** R7 is complete (commits `0618c61` through `931c824`). R8.1 Python/UDF/FlightSQL and R8.2 Iceberg lakehouse are actively being implemented on branch `claude/plan-r7-implementation-lt3n3`.

## Active Task

**R8.1 + R8.2** — implementing in parallel:

### Completed (committed to branch)

| Commit | Content |
|--------|---------|
| `0618c61` | R7.1: Resource governance foundation (quotas, admission, cost metrics) |
| `b5570bb` | R7.2: Backpressure and adaptive governance (SpaceSaving, RateLimiter, barriers) |
| `3dec2a1` | docs: R7 tracker + status updated |
| `8509663` | pre-R8: HashMap job index, auth interceptor skeleton, R7 roadmap sync, R8 ADR |
| `6a1fc17` | pre-R8: TraceContext in proto + OperatorQueue wiring in streaming executor |
| `c867a62` | R8.1 Group A: `krishiv-udf` crate — ScalarUdf, AggregateUdf, TableUdf, UdfRegistry |
| `931c824` | R8.2: `krishiv-lakehouse` — Iceberg read/write beta, snapshot reads, optimistic concurrency |

### In Progress

- **R8.1 Group B**: `krishiv-python` PyO3 crate — Session/DataFrame bindings, Python UDFs via `spawn_blocking`
- **R8.1 Group C**: `krishiv-flight-sql` — thin adapter over `Session`, routes Flight SQL through existing planner/runtime

## R8.1 Architecture Decisions (locked, see r8-python-flight-sql-adr.md)

- **UDF thread model**: `spawn_blocking` — GIL never held on Tokio worker thread
- **asyncio integration**: embedded Tokio runtime (`LazyLock<Runtime>`) in PyO3 module
- **Flight SQL routing**: thin adapter over `Session::sql_async()` — zero query-path divergence
- **Streaming UDFs**: deferred to post-GA, will use subprocess isolation (Arrow IPC over Unix socket)
- **Beta stability**: all Python/lakehouse public items carry beta annotation

## Next Steps

1. Complete `krishiv-python` + `krishiv-flight-sql` (in-progress)
2. Run full workspace test suite after both agents commit
3. Update R8 tracker checklist with completed items
4. Push to `origin/claude/plan-r7-implementation-lt3n3`
5. Begin R9 planning (end-to-end credit propagation, distributed tracing)

## Known Blockers

- `crates/krishiv-python` was missing (agent was rate-limited) — recreation in progress
- Maturin build pipeline / `.pyi` type stubs not yet implemented (deferred within R8.1)

## Last Validation (R7 complete)

- `cargo test --workspace`: 0 failures (90 scheduler, 52 exec, 510 total workspace)
- `cargo clippy --workspace -- -D warnings`: 0 warnings
- Branch: `claude/plan-r7-implementation-lt3n3`

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
