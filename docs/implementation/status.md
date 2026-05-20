# Krishiv Implementation Status

## Current Phase

**R10 Sprint 1a + Sprint 2a complete.** Auth and policy governance wired into `krishiv-sql` and `krishiv-flight-sql`. Sprint 0 architecture deliverables remain committed on branch `claude/plan-r10-architecture-GnRvo`.

## Active Task

**R10 Sprint 1b/1c/1d** — live K8s Lease API, OTLP integration test, kind failover e2e CI workflow (pre-existing uncommitted changes in krishiv-operator and krishiv-metrics require API fixes before committing).

### Completed (committed to branch)

| Commit | Content |
|--------|---------|
| `0618c61` | R7.1: Resource governance foundation (quotas, admission, cost metrics) |
| `b5570bb` | R7.2: Backpressure and adaptive governance (SpaceSaving, RateLimiter, barriers) |
| `3dec2a1` | docs: R7 tracker + status updated |
| `8509663` | pre-R8: HashMap job index, auth interceptor skeleton, R7 roadmap sync, R8 ADR |
| `6a1fc17` | pre-R8: TraceContext in proto + OperatorQueue wiring in streaming executor |
| `c867a62` | R8.1 Group A: `krishiv-udf` — ScalarUdf, AggregateUdf, TableUdf, UdfRegistry |
| `931c824` | R8.2: `krishiv-lakehouse` — Iceberg read/write beta, snapshot reads, optimistic concurrency |
| `63a8ae2` | R8.1 Group C: `krishiv-flight-sql` — Flight SQL thin adapter over Session |
| `1611103` | R8.1 Group B: `krishiv-python` — PyO3 Session/DataFrame/UDF via spawn_blocking |
| `0105392` | docs(R8): mark R8 complete |
| `ccbda47` | R9.1: `krishiv-governance` — RBAC, audit log, OpenLineage, policy hooks |
| `496504c` | R9.1: `krishiv-metrics` — OTel tracing init, tracing bridge, structured logs |
| `dad69ca` | R9.2: HA leader election, fencing enforcement, replay bundle, plan diff, TLS, Helm |
| `a4d1065` | R7.1 governance: quota-aware QueueManager (QuotaQueueManager, ConfigFileQueueManager) |
| `4ae8a82` | R4a+R5a: typed shuffle wiring (ShuffleWriteConfig/ReadConfig) + redb state backend |
| `6266f8a` | R6a: out-of-band checkpoint barrier (trigger_checkpoint_for_job, checkpoint_ack RPC) |
| `(pending)` | R6c: LocalParquetTwoPhaseCommitSink in krishiv-connectors |
| `(this session)` | R10 Sprint 1a+2a: `PolicyEnforcingSqlEngine` in krishiv-sql + auth/policy wiring in KrishivFlightSqlService |

## R4/R5/R6 Architecture Decisions (locked)

- **Shuffle (R4a)**: `ExecutorTaskRunner::with_inmem_shuffle()` + `execute_inmem_shuffle_write/read`; `ShuffleWriteConfig`/`ShuffleReadConfig` in proto
- **State backend (R5a)**: `RedbStateBackend` (redb 2.x, ACID, pure-Rust); `RocksDbStateBackend` = type alias; in-memory mode for tests
- **Checkpoint barrier (R6a)**: Out-of-band `trigger_checkpoint_for_job()` returns `InitiateCheckpointRequest`; executor acks via `checkpoint_ack()` on `CoordinatorExecutorService`
- **2PC sink (R6c)**: `LocalParquetTwoPhaseCommitSink` — `.tmp` on prepare, atomic rename on commit, delete on abort

## R4/R5/R6 Deferred Items

- R4b: AQE (`StageRuntimeStats` → coordinator fires `CoalesceRule`/`ThresholdSkewRule`)
- R4c: LZ4/Zstd compression in `LocalShuffleStore` (`lz4_flex`)
- R5b/R5c: Watermark operator, tumbling window operator, continuous loop
- R6b full wiring: actual gRPC calls from coordinator to executor task endpoints for barrier (R6a has the logic; wire transport in R10)

## Next Steps

1. Sprint 1b: Replace simulated `K8sLeaseElection` with live async K8s Lease API calls in `krishiv-operator` (has pre-existing Patch::MergePatch API mismatch to fix)
2. Sprint 1c: Add OTLP integration test in `krishiv-metrics` (has pre-existing `SdkTracerProvider`/`with_endpoint` API mismatch to fix)
3. Sprint 1d: Add `.github/workflows/kind-e2e.yml` for `kind` cluster failover CI
4. Sprint 2+: Data quality rules, dead-letter sink, upgrade tests, CDC pipeline, materialized views, benchmarks

## Last Validation

- `cargo test -p krishiv-sql`: 10/10 passed (includes 4 policy engine tests)
- `cargo test -p krishiv-flight-sql`: 13/13 passed (includes 7 auth/governance tests)
- `cargo check -p krishiv-sql -p krishiv-flight-sql -p krishiv-governance`: clean
- `cargo check --workspace`: krishiv-metrics and krishiv-operator fail due to pre-existing API version mismatches (opentelemetry-otlp SdkTracerProvider rename, kube Patch::MergePatch variant removal) — unrelated to Sprint 1a/2a
- Branch: `claude/plan-r10-architecture-GnRvo`
- Sprint 0 docs: `docs/architecture/stability-policy.md`, `compatibility-matrices.md`, `jdbc-odbc-architecture.md`, `cdc-reference.md`, `materialized-views.md`, `data-quality-model.md`, `upgrade-compatibility-policy.md`, `benchmark-targets.md`
- R10 tracker: 12/12 architecture deliverables checked off

## Architectural Inputs To Preserve

- Distributed mode targets: Kubernetes (primary), bare-metal/VM (secondary).
- Control-plane: tonic gRPC + Protobuf. Bulk Arrow data uses Arrow IPC/Flight.
- R7.2 backpressure: intra-stage only. Cross-stage via `ThrottleCommand`. Full credit propagation deferred to R9/R10.
- `LeaderElection` trait in `krishiv-scheduler`; K8s implementation in `krishiv-operator`. Zero K8s API in core runtime.
- Python UDF thread model: `spawn_blocking` — never hold GIL on Tokio worker.
- Flight SQL: thin adapter over `Session::sql_async()` — same planner/runtime as CLI.
- Fencing tokens: every coordinator that writes checkpoint metadata must hold the current leader lease; stale writes rejected by `validate_fencing_token()`.
