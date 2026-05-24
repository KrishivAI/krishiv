# Distributed Unified Mitigation — Implementation Tracker

**Architecture plan:** [distributed-unified-mitigation-plan.md](../architecture/distributed-unified-mitigation-plan.md)  
**Status:** In progress (WS-0–WS-3, WS-7 partial, WS-9 partial landed 2026-05-24).

## Goal

Deliver production-grade unified batch + streaming on Kubernetes and bare metal with per-job coordinators, durable metadata/checkpoints, physical plan lowering, and L5 CI — closing all GAP-* items in the plan matrix.

## Workstream gates

- [x] **WS-0** Decision lock + CI skeleton + doc alignment
- [x] **WS-1** Scheduler decomposition + transport abstraction (CCP/JCP modules)
- [x] **WS-2** Physical plan lowering (`krishiv-plan::lowering`)
- [x] **WS-3** `krishiv-clusterd` + `krishiv-job-coordinator` binaries
- [ ] **WS-4** Executor data plane completion
- [ ] **WS-5** Stateful streaming + checkpoints
- [ ] **WS-6** Shuffle production path
- [x] **WS-7** Kubernetes operator v2 (in-process JCP loops via `dedicatedCoordinator`)
- [ ] **WS-8** Bare metal production stack
- [x] **WS-9** Session/API (`execute_local` / `execute_remote`, `with_coordinator_grpc`)
- [ ] **WS-10** Connectors, observability, autoscale
- [ ] **WS-11** Federation (global metadata)

## Acceptance gate (plan complete)

- [ ] `cargo test --workspace --lib` — 0 failures
- [ ] `cargo clippy --workspace -- -D warnings` — clean
- [x] `cargo test -p krishiv-scheduler --test distributed_e2e` — batch + streaming lowering
- [ ] CI `kind-e2e` — KrishivJob → JCP pod → tasks succeeded
- [ ] CI `bare-metal-e2e` — clusterd + executors
- [ ] `scripts/audit-fencing.sh` — all write paths validated
- [ ] `tests/certification` — streaming checkpoint + connector rows aligned
- [ ] No `todo!()` / `unimplemented!()` in distributed hot paths (scheduler, executor, operator, runtime, api)

## Next command

```bash
# After WS-0 lands:
cargo test -p krishiv-scheduler --test coordinator_executor_integration
```
