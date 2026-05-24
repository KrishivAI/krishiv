# Refactoring Implementation (2026-05-24)

## Completed

### Phase 0 — Workspace hygiene
- Added workspace members: `krishiv-ai`, `krishiv-schema-registry`, `krishiv-spark-connect`, `krishiv-barrier`, `krishiv-flight`
- Removed standalone `krishiv-cep` workspace member (merged into `krishiv-exec`)

### Phase 1 — God-file splits
- **`krishiv-proto`**: split into `ids.rs`, `domain.rs`, `wire.rs`, `proto_tests.rs` (fixed derive preservation on re-slice)

### Phase 2 — Shared crates
- **`krishiv-flight`**: canonical Flight SQL comment protocol (`FlightDirective`, encode/parse)
- **`krishiv-barrier`**: `CheckpointBarrierTracker`, `inject_barrier`, `BarrierDispatchPlan` types
- **`krishiv-runtime`**: re-exports flight protocol; `BatchSqlTable` aliases `krishiv_flight::BatchSqlTable`

### Phase 4 — Semantic consolidation
- **CEP merge**: `krishiv-cep` sources moved to `krishiv-exec/src/cep/{pattern,matcher,operator}.rs`
- **SqlEngine injection**: `InProcessCluster` holds shared `SqlEngine`; `FlightExecutionHost::sql_engine()`; Flight SQL policy path uses host engine instead of `SqlEngine::new()`

### Phase 5 — Feature flags (initial)
- `krishiv-runtime`: `embedded`, `distributed` feature stubs
- `krishiv` facade: `default = ["full"]`

## Deferred (follow-up PRs)

- **Scheduler `lib.rs` split** (~6.4k lines): attempted extraction; reverted until `internal_prelude` wiring is complete
- **`krishiv-api` module split** (`session/`, `dataframe/`, `stream/`)
- **`krishiv-transport`** shared gRPC helpers
- **Runtime decoupling**: trait-only deps on scheduler/executor with feature gates
- **Operator/shuffle/state/connectors** file splits
- **Federation crate merge** into scheduler

## Validation

```bash
cargo +stable test -p krishiv-proto -p krishiv-barrier -p krishiv-flight -p krishiv-exec \
  -p krishiv-checkpoint -p krishiv-scheduler -p krishiv-runtime -p krishiv-flight-sql -p krishiv-api --lib
```
