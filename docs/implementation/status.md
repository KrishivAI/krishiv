# Krishiv Implementation Status

## Current Phase

**Gap mitigation (all sprints) — in progress on PR #36 (2026-05-23)**

Plan: [`docs/engineering/gap-mitigation-plan.md`](../engineering/gap-mitigation-plan.md)

Branch: `cursor/gap-mitigation-7aa2`

## Completed this session (continued)

| Tier | Items |
|------|--------|
| P0 | P0-5/6 leader election + finalizer; P0-11 Weaviate query; P0-14 Spark CAST; P0-15 audit call sites; P0-16 KrishivMetrics + Prometheus text |
| P1 | P1-1/2/3 checkpoint fencing, stale epoch, fsync; P3-6 list_valid_epochs warns/propagates |
| P2 | P2-3 ProjectionPruning, PredicatePushdown, ConstantFolding rules; P2-6 krishiv-testkit helpers |
| P3 | P3-14 DashMap audit dedup; P3-15 record_async; P3-16 owned AuditAction; P3-17 metrics shutdown; P3-21/22 UI shuffle bytes + active readyz; P3-23–25 catalog TableAlreadyExists + List/Struct types |

## Prior commits on branch

Sprint 1 P0 (modules, coordinator ticks, catalog SQL, Python wiring, RAG sink), P1 shuffle/state/lakehouse/connectors/executor, checkpoint ACK, etc.

## Validation

```bash
cargo check --workspace
cargo test -p krishiv-checkpoint -p krishiv-governance -p krishiv-optimizer -p krishiv-catalog -p krishiv-metrics -p krishiv-ui -p krishiv-scheduler -p krishiv-sql-policy -p krishiv-vector-sinks --lib
```

Note: `krishiv-python` / `krishiv-ai` lib tests need `libstdc++` (ONNX). `krishiv-executor` integration test `barrier_injection` needs `barrier_transport` module.

## Next command

Continue remaining P2/P3 items (shuffle lock poison, flight-sql PolicyEnforcingSqlEngine direct wire, coalesce exec, object-store checkpoint, nexmark benches, P3 AI/python items).
