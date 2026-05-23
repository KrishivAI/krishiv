# Krishiv Implementation Status

## Current Phase

**Gap mitigation Sprint 1 (P0) — in progress (2026-05-23)**

Plan: [`docs/engineering/gap-mitigation-plan.md`](../engineering/gap-mitigation-plan.md)

Branch: `cursor/gap-mitigation-7aa2`

## Sprint 1 P0 completed this session

| ID | Fix |
|----|-----|
| P0-1/2 | Wired orphaned `krishiv-exec`, `krishiv-sql`, `krishiv-plan` modules; added `ExecError::IncompatibleSchemaEvolution`, `NodeOp::{Create,Refresh,Drop}LiveTable` |
| P0-3 | Declared `transactional_kafka`, `two_phase_parquet_s3`, `cdc_router` in `krishiv-connectors` |
| P0-4 | `SharedCoordinator::spawn_orchestration_loops()` in coordinator binary + operator |
| P0-7/8 | `GrpcCoordinatorService` gRPC client pooling; executor lease generation updated after register/heartbeat |
| P0-9/10 | Catalog `MemTable` scans via `register_table_with_batches`; `SqlEngine::with_in_memory_catalog` |
| P0-13 | Shared `RAG_VECTOR_SINKS` registry between `rag_index` / `rag_query` |

## Not done (Sprint 1 remainder)

- **P0-12**: Python split modules (`session.rs`, `dataframe.rs`, …) still conflict with inline types in `lib.rs` — needs dedicated refactor.
- **P0-5/6, P0-11, P0-14–16**: K8s leader election, finalizer, Weaviate query, Spark CAST, audit call sites, OTel metrics.

## Validation (this session)

```bash
cargo check --workspace
cargo test -p krishiv-scheduler task_launch_drives_to_running
cargo test -p krishiv-executor --lib lease_generation_updated_after_reregister
cargo test -p krishiv-sql --lib catalog_table_resolved_in_sql
cargo test -p krishiv-catalog --lib catalog_scan_returns_registered_row_count
cargo test -p krishiv-exec -p krishiv-plan -p krishiv-connectors --lib
```

## Next command

Continue Sprint 1: P0-12 Python module wiring, then Sprint 2 items (P0-5/6, P1 checkpoint/shuffle/state).
