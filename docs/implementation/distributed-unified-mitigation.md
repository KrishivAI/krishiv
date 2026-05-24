# Distributed Unified Mitigation — Implementation Tracker

**Architecture plan:** [distributed-unified-mitigation-plan.md](../architecture/distributed-unified-mitigation-plan.md)  
**Status:** Not started (plan approved 2026-05-24).

## Goal

Deliver production-grade unified batch + streaming on Kubernetes and bare metal with per-job coordinators, durable metadata/checkpoints, physical plan lowering, and L5 CI — closing all GAP-* items in the plan matrix.

## Workstream gates

- [ ] **WS-0** Decision lock + CI skeleton + doc alignment
- [ ] **WS-1** Scheduler decomposition + transport abstraction
- [ ] **WS-2** Physical plan lowering + async execution runtime
- [ ] **WS-3** Durable metadata, fencing, coordinator binaries
- [ ] **WS-4** Executor data plane completion
- [ ] **WS-5** Stateful streaming + checkpoints
- [ ] **WS-6** Shuffle production path
- [ ] **WS-7** Kubernetes operator v2
- [ ] **WS-8** Bare metal production stack
- [ ] **WS-9** Session/API + Flight parity
- [ ] **WS-10** Connectors, observability, autoscale
- [ ] **WS-11** Federation (global metadata)

## Acceptance gate (plan complete)

- [ ] `cargo test --workspace --lib` — 0 failures
- [ ] `cargo clippy --workspace -- -D warnings` — clean
- [ ] `cargo test --test distributed_e2e` — batch + streaming
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
