# Distributed Unified Mitigation — Implementation Tracker

**Architecture plan:** [distributed-unified-mitigation-plan.md](../architecture/distributed-unified-mitigation-plan.md)  
**Status:** WS-0–WS-11 landed on branch `cursor/implement-distributed-unified-854c` (2026-05-24). L5 acceptance gates partially open (see below).

## Goal

Deliver production-grade unified batch + streaming on Kubernetes and bare metal with per-job coordinators, durable metadata/checkpoints, physical plan lowering, and L5 CI — closing all GAP-* items in the plan matrix.

## Workstream gates

- [x] **WS-0** Decision lock + CI skeleton + doc alignment
- [x] **WS-1** Scheduler decomposition + transport abstraction (CCP/JCP modules)
- [x] **WS-2** Physical plan lowering (`krishiv-plan::lowering`)
- [x] **WS-3** `krishiv-clusterd` + `krishiv-job-coordinator` binaries
- [x] **WS-4** Executor data plane completion (barrier gRPC, checkpoint heartbeat, catalog on SQL)
- [x] **WS-5** Stateful streaming + checkpoints (Redb windows, object-store URI, idle watermark)
- [x] **WS-6** Shuffle production path (Lz4 disk store, shuffle-svc binary)
- [x] **WS-7** Kubernetes operator v2 (JCP template, ExecutorPool CRD, operator replicas: 2)
- [x] **WS-8** Bare metal production stack (systemd, `krishiv cluster`, bare-metal-e2e CI)
- [x] **WS-9** Session/API (`execute_local` / `execute_remote`, `with_coordinator_grpc`)
- [x] **WS-10** Connectors, observability, autoscale (slot-aware placement, KEDA manifest)
- [x] **WS-11** Federation (`RemoteFederationClient` + HTTP shim on coordinator)

## Acceptance gate (plan complete)

- [ ] `cargo test --workspace --lib` — 0 failures
- [ ] `cargo clippy --workspace -- -D warnings` — clean
- [x] `cargo test -p krishiv-scheduler --test distributed_e2e` — batch + streaming lowering
- [ ] CI `kind-e2e` — KrishivJob → JCP pod → tasks succeeded
- [ ] CI `bare-metal-e2e` — clusterd + executors (workflow added; required green on main)
- [ ] `scripts/audit-fencing.sh` — all write paths validated
- [ ] `tests/certification` — streaming checkpoint + connector rows aligned
- [ ] No `todo!()` / `unimplemented!()` in distributed hot paths (scheduler, executor, operator, runtime, api)

## Next command

```bash
cargo +stable test -p krishiv-scheduler -p krishiv-operator -p krishiv-executor -p krishiv-federation --lib
cargo +stable test -p krishiv-scheduler --test distributed_e2e
```
