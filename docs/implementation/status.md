# Krishiv Implementation Status

## Current Phase

**R9 IN PROGRESS (core items delivered).** R8 is complete. R9.1 (observability + governance) and R9.2 (HA + operations) core items are implemented on branch `claude/plan-r7-implementation-lt3n3`.

## Active Task

**R9** — implementing governance and operations.

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
| `c1af99e` | docs(R8): sync roadmap R8.1 and R8.2 checklists |
| `27e8c5c` | docs(R8): update status.md and R8 tracker |
| `1611103` | R8.1 Group B: `krishiv-python` — PyO3 Session/DataFrame/UDF via spawn_blocking |
| `0105392` | docs(R8): mark R8 complete |
| `ccbda47` | R9.1: `krishiv-governance` — RBAC, audit log, OpenLineage, policy hooks |
| `496504c` | R9.1: `krishiv-metrics` — OTel tracing init, tracing bridge, structured logs |
| `(pending)` | R9.2: workspace deps, fencing enforcement, LeaderElection extension, K8sLeaseElection, plan diff, replay bundle, TLS config, auth completion, Helm chart |

### In Progress

- R9.2 commit being prepared (all code done, pending commit + push)

## R9 Architecture Decisions (locked)

- **OTel transport**: `tracing` facade + `tracing-opentelemetry` bridge; stdout exporter default; OTLP HTTP/proto for production (avoids tonic 0.12 conflict)
- **RBAC (R9 beta)**: static API key → role mapping; OIDC/JWT deferred to R10
- **Leader election**: `LeaderElection` trait extended with `try_acquire/renew/release/fencing_token`; K8s Lease-backed `K8sLeaseElection` in `krishiv-operator`
- **Fencing enforcement**: `validate_fencing_token()` in `krishiv-checkpoint`; stale token → `StaleFencingToken` error
- **Live K8s Lease API calls**: simulated in R9 (test-safe); wired to real K8s API in R10
- **Helm chart**: `k8s/helm/krishiv/` with coordinator (Recreate), executor (RollingUpdate), headless service, RBAC

## R9 Deferred Items (not blocking acceptance gate)

- Live K8s Lease API calls in `K8sLeaseElection` (R9 uses simulated lease; wired in R10)
- Policy hook enforcement at DataFusion scan layer (deferred to R10)
- OTLP gRPC transport (HTTP/proto used in R9; gRPC deferred to avoid tonic conflict)
- OIDC/JWT token validation (static API key in R9; OIDC in R10)
- `kind` cluster e2e failover test (deferred — requires kind cluster in CI)
- Row-level enforcement inside DataFusion operators (policy hook interface defined; enforcement in R10)

## Next Steps

1. Complete R9 commit + push
2. Run full workspace test suite
3. Begin R10 planning (GA platform, stable API policy, benchmarks, connector certification)

## Last Validation (R9 in-progress)

- `cargo test --lib -p krishiv-metrics`: 5 passed
- `cargo test --lib -p krishiv-governance`: 10 passed
- `cargo test --lib -p krishiv-checkpoint`: 27 passed (includes fencing + replay bundle tests)
- `cargo test --lib -p krishiv-plan`: 17 passed (includes plan diff tests)
- `cargo test --lib -p krishiv-operator`: 34 passed (includes K8sLeaseElection + failover tests)
- `cargo test --lib -p krishiv-scheduler`: 90 passed
- Branch: `claude/plan-r7-implementation-lt3n3`

## Architectural Inputs To Preserve

- Distributed mode targets: Kubernetes (primary), bare-metal/VM (secondary).
- Control-plane: tonic gRPC + Protobuf. Bulk Arrow data uses Arrow IPC/Flight.
- R7.2 backpressure: intra-stage only. Cross-stage via `ThrottleCommand`. Full credit propagation deferred to R9/R10.
- `LeaderElection` trait in `krishiv-scheduler`; K8s implementation in `krishiv-operator`. Zero K8s API in core runtime.
- Python UDF thread model: `spawn_blocking` — never hold GIL on Tokio worker.
- Flight SQL: thin adapter over `Session::sql_async()` — same planner/runtime as CLI.
- Fencing tokens: every coordinator that writes checkpoint metadata must hold the current leader lease; stale writes rejected by `validate_fencing_token()`.
