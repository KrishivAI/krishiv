# Krishiv Implementation Status

## Current Phase

**R10 Sprint 3 complete.** CDC-to-lakehouse pipeline template (`cdc.rs` in `krishiv-connectors`) and materialized views baseline (`MaterializedViewRegistry` in `krishiv-sql`) delivered. Sprint 0–2 architecture deliverables remain committed on branch `claude/plan-r10-architecture-GnRvo`.

## Active Task

**R10 Sprint 4** — benchmarks (TPC-H, TPC-DS, Nexmark), chaos test suite, and GA API freeze.

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

1. Sprint 4: Add TPC-H, TPC-DS, Nexmark benchmark suites
2. Sprint 4: Add chaos test suite
3. Sprint 4: Freeze GA-supported API and connector surfaces
4. Sprint 4: Implement JDBC/ODBC gateway stubs
5. Sprint 4: Publish SQL/function compatibility matrix

## Last Validation

- `cargo test -p krishiv-connectors`: 47 passed (45 unit + 2 certification) — includes 6 new CDC tests
- `cargo test -p krishiv-sql`: 15 passed — includes 5 new matview tests
- `cargo test -p krishiv-upgrade-tests`: 6 passed
- `cargo check --workspace`: clean (no errors)
- Branch: `claude/plan-r10-architecture-GnRvo`
- Sprint 3 deliverables: `CdcOp`, `CdcEvent`, `parse_debezium_envelope`, `CdcToLakehousePipeline` in `crates/krishiv-connectors/src/cdc.rs`; `MaterializedViewDefinition`, `RefreshPolicy`, `MaterializedViewRegistry` in `crates/krishiv-sql/src/lib.rs`
- R10 tracker: 12/12 architecture, 5/11 API, 6/15 runtime, 6/16 test checklist items checked off

## Architectural Inputs To Preserve

- Distributed mode targets: Kubernetes (primary), bare-metal/VM (secondary).
- Control-plane: tonic gRPC + Protobuf. Bulk Arrow data uses Arrow IPC/Flight.
- R7.2 backpressure: intra-stage only. Cross-stage via `ThrottleCommand`. Full credit propagation deferred to R9/R10.
- `LeaderElection` trait in `krishiv-scheduler`; K8s implementation in `krishiv-operator`. Zero K8s API in core runtime.
- Python UDF thread model: `spawn_blocking` — never hold GIL on Tokio worker.
- Flight SQL: thin adapter over `Session::sql_async()` — same planner/runtime as CLI.
- Fencing tokens: every coordinator that writes checkpoint metadata must hold the current leader lease; stale writes rejected by `validate_fencing_token()`.
