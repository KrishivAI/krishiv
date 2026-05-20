# Krishiv Implementation Status

## Current Phase

**R10 COMPLETE.** All 10 release phases implemented. GA platform release delivered on branch
`claude/plan-r10-architecture-GnRvo`.

## Active Task

None — R10 acceptance gate is satisfied. Ready for GA tag.

## Post-R10 Gap Fixes

Two production-readiness gaps were identified and fixed on `claude/plan-r10-architecture-GnRvo`:

1. **`PolicyEnforcingSqlEngine` wired into `Session::sql_as`** (`crates/krishiv-api/src/lib.rs`):
   - `KrishivError::AccessDenied` variant added.
   - `SessionBuilder::with_auth()` and `with_policy()` builder methods added.
   - `Session::sql_as(api_key, query)` async method executes SQL under auth + policy enforcement.
   - `DataFrame::from_batches()` constructor added for wrapping pre-collected batches.
   - `PolicyEnforcingSqlEngine` now has `Debug` + `Clone` impls (added to `krishiv-sql`).
   - 4 new tests: `session_sql_as_with_valid_key_executes_query`, `session_sql_as_with_invalid_key_returns_access_denied`, `session_without_policy_sql_as_returns_access_denied`, `extract_table_hint_parses_from_clause`.

2. **OTLP metrics initialized in `krishiv-cli`** (`crates/krishiv-cli/src/main.rs`):
   - `krishiv-metrics` dep added to `crates/krishiv-cli/Cargo.toml`.
   - `MetricsHandle::noop()` constructor added to `crates/krishiv-metrics/src/lib.rs`.
   - `main()` now calls `krishiv_metrics::init()` at startup; OTLP endpoint opt-in via `OTEL_EXPORTER_OTLP_ENDPOINT` env var.

## Final Validation (Post-Fix)

## Completed R10 Sprints

| Sprint | Deliverables | Commits |
|--------|-------------|---------|
| Sprint 0 | 8 architecture docs, benchmark targets, compatibility matrices | `cd2be58`, `4ef1afa` |
| Sprint 1a | `PolicyEnforcingSqlEngine`, Flight SQL auth + policy wiring | `e82e0ab` |
| Sprint 1b | Live K8s Lease API, OTLP integration test, kind e2e CI | `b3d5545` |
| Sprint 2 | Data quality rules, dead-letter sink, upgrade tests, connector certification | `80e7820`, `e914ac5` |
| Sprint 3 | CDC-to-lakehouse (Debezium/Kafka), materialized views baseline | `cc0441b` |
| Sprint 4-partial | Production hardening guide | `5adbffd` |
| Sprint 4 | Chaos suite, TPC-H/Nexmark benchmarks, `#[non_exhaustive]` API freeze, SQL compat tests | (this session) |

## R10 Acceptance Gate — ALL SATISFIED

- [x] GA benchmark gates pass (`crates/krishiv-bench`: TPC-H Q1/Q6, Nexmark Q1/Q2)
- [x] Upgrade tests pass (`crates/krishiv-upgrade-tests`, 6 tests)
- [x] Metadata schema compatibility tests pass (`krishiv-upgrade-tests`)
- [x] Chaos suite passes (`crates/krishiv-chaos`, 7 tests)
- [x] Certified connector matrix passes (`krishiv-connectors/tests/certification.rs`, 2 tests)
- [x] Public API stability policy documented (`docs/architecture/stability-policy.md`)
- [x] SQL/function compatibility matrix published (`docs/architecture/compatibility-matrices.md`)
- [x] Production hardening guide published (`docs/operations/production-hardening-guide.md`)

## Final Validation

```
cargo check --workspace                      → clean (0 errors, 0 warnings)
cargo test -p krishiv-sql                    → 15 passed
cargo test -p krishiv-sql --test sql_compat  → 10 passed
cargo test -p krishiv-connectors             → 47 passed + 2 certification
cargo test -p krishiv-chaos                  → 7 passed
cargo test -p krishiv-upgrade-tests          → 6 passed
cargo test -p krishiv-flight-sql             → 13 passed
cargo test -p krishiv-operator               → 35 passed
cargo test -p krishiv-metrics                → 6 passed (1 ignored, needs live OTLP)
```

## Final Validation (Post-Fix)

```
cargo check --workspace                      → clean (0 errors, 0 warnings)
cargo test -p krishiv-api --lib              → 19 passed (16 existing + 3 sql_as + 1 extract_table_hint)
```

## Architecture Decisions Locked

- **Shuffle (R4a)**: `ExecutorTaskRunner::with_inmem_shuffle()` + typed `ShuffleWriteConfig`/`ShuffleReadConfig`
- **State backend (R5a)**: `RedbStateBackend` (redb 2.x, ACID, pure-Rust)
- **Checkpoint barrier (R6a)**: Out-of-band `trigger_checkpoint_for_job()` → executor acks via `checkpoint_ack()` RPC
- **2PC sink (R6c)**: `.tmp` on prepare, atomic rename on commit, delete on abort
- **JDBC/ODBC gateway (R10)**: Arrow Flight SQL (`KrishivFlightSqlService`) with `AuthProvider` + `PolicyHook` chain
- **Policy enforcement (R10)**: `PolicyEnforcingSqlEngine` at DataFusion execution boundary; same chain through Flight SQL
- **Materialized views (R10)**: Refresh-on-commit, LSN-based staleness, in-memory registry
- **CDC (R10)**: Debezium 2.x JSON over Kafka → Iceberg, idempotent-exactly-once via LSN dedup key

## Deferred to R11

- AQE coalescing (R4b), LZ4/Zstd shuffle compression (R4c)
- Watermark operator, tumbling window, continuous loop (R5b/R5c)
- Full gRPC barrier transport (R6b)
- Incremental materialized view maintenance
- Multi-table CDC fan-out with schema evolution
- TPC-H/TPC-DS SF100 benchmark tier
