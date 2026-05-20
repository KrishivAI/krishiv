# Krishiv Implementation Status

## Current Phase

**R10 COMPLETE + Post-GA Gap Fixes Applied.** All stubs and deferred items resolved.
GA platform release on branch `claude/plan-r10-architecture-GnRvo`.

## Active Task

None — all P0/P1/P2 gaps resolved. Ready for GA tag.

## Post-R10 Gap Fixes (P0 → P2)

### P0: Critical Stubs Replaced

1. **`PolicyEnforcingSqlEngine` wired into `Session::sql_as`** (`crates/krishiv-api`):
   - `KrishivError::AccessDenied` variant, `SessionBuilder::with_auth()` / `with_policy()`
   - `Session::sql_as(api_key, query)` async method — auth + policy enforced execution
   - `DataFrame::from_batches()` constructor; `PolicyEnforcingSqlEngine` gains `Debug`+`Clone`
   - 4 new tests all pass

2. **`checkpoint_ack` wire transport** (`crates/krishiv-executor`, `krishiv-proto`, `krishiv-scheduler`):
   - Added `CheckpointAck` RPC to `coordinator_executor.proto`
   - Wire conversion functions: `checkpoint_ack_request_to/from_wire`, `checkpoint_ack_response_to/from_wire`
   - `NetworkCoordinatorService::checkpoint_ack` now routes over gRPC (replaces `unimplemented!` stub)
   - `ExecutorRuntime::checkpoint_ack_with_grpc_endpoint()` public API
   - Scheduler `CoordinatorExecutorGrpcService::checkpoint_ack` handler added
   - New in-process test: `network_coordinator_service_checkpoint_ack_through_service_boundary`

3. **`DataQualityRule::Regex` real matching** (`crates/krishiv-connectors`):
   - Replaced stub with real `regex::Regex` matching; invalid patterns → `ConnectorError::Config`
   - `LocalParquetTwoPhaseCommitSink` gains `quality_config` field + `with_quality_config()` builder
   - Quality checks run in `prepare()`: `Fail` aborts, `Reject` filters rows via Arrow compute
   - `QualityAction::Warn` uses `tracing::warn!` with structured fields
   - 4 new tests pass

4. **OTLP initialized at CLI startup** (`crates/krishiv-cli`, `crates/krishiv-metrics`):
   - `MetricsHandle::noop()` added for graceful degradation
   - `main()` reads `OTEL_EXPORTER_OTLP_ENDPOINT` and calls `krishiv_metrics::init()` at startup

### P1: HA and Correctness

5. **`MaterializedViewRegistry` wired into `SqlEngine`** (`crates/krishiv-sql`):
   - `SqlEngine::with_view_registry()` builder method
   - `mark_table_committed()` called after `register_parquet` and `register_record_batches`
   - `sql_with_view_cache()` method: cache-hit fast path + cache-fill for `OnCommit` views
   - `extract_simple_view_name()` helper; 2 new tests pass

6. **CDC payload column unpacking** (`crates/krishiv-connectors/src/cdc.rs`):
   - `parse_debezium_envelope` now builds one `Utf8` column per JSON key (replaces single `_payload` column)
   - Test assertion updated to verify column names

### P2: Observability

7. **Structured `AuditEvent` + `AuditSink`** (`crates/krishiv-governance`):
   - `AuditEvent`, `AuditOutcome`, `AuditSink` trait, `TracingAuditSink` added
   - `audit_log()` now constructs an `AuditEvent` and routes through `TracingAuditSink`
   - 2 new tests: `audit_event_constructs_correctly`, `tracing_audit_sink_does_not_panic`

## Completed R10 Sprints

| Sprint | Deliverables | Commits |
|--------|-------------|----------|
| Sprint 0 | 8 architecture docs, benchmark targets, compatibility matrices | `cd2be58`, `4ef1afa` |
| Sprint 1a | `PolicyEnforcingSqlEngine`, Flight SQL auth + policy wiring | `e82e0ab` |
| Sprint 1b | Live K8s Lease API, OTLP integration test, kind e2e CI | `b3d5545` |
| Sprint 2 | Data quality rules, dead-letter sink, upgrade tests, connector certification | `80e7820`, `e914ac5` |
| Sprint 3 | CDC-to-lakehouse (Debezium/Kafka), materialized views baseline | `cc0441b` |
| Sprint 4-partial | Production hardening guide | `5adbffd` |
| Sprint 4 | Chaos suite, TPC-H/Nexmark benchmarks, `#[non_exhaustive]` API freeze, SQL compat tests | `bae2af0` |
| Post-GA P0/P2 | PolicyEngine wiring, checkpoint_ack gRPC, regex quality, OTLP CLI, matview/CDC/audit | `0d32e9f`–`a92c936` |

## R10 Acceptance Gate — ALL SATISFIED

- [x] GA benchmark gates pass (`crates/krishiv-bench`: TPC-H Q1/Q6, Nexmark Q1/Q2)
- [x] Upgrade tests pass (`crates/krishiv-upgrade-tests`, 6 tests)
- [x] Metadata schema compatibility tests pass (`krishiv-upgrade-tests`)
- [x] Chaos suite passes (`crates/krishiv-chaos`, 7 tests)
- [x] Certified connector matrix passes (`krishiv-connectors/tests/certification.rs`, 2 tests)
- [x] Public API stability policy documented (`docs/architecture/stability-policy.md`)
- [x] SQL/function compatibility matrix published (`docs/architecture/compatibility-matrices.md`)
- [x] Production hardening guide published (`docs/operations/production-hardening-guide.md`)

## Final Validation (Post-Fix)

```
cargo check --workspace                           → clean (0 errors, 0 warnings)
cargo test -p krishiv-api --lib                   → 19 passed
cargo test -p krishiv-sql --lib                   → 17 passed
cargo test -p krishiv-sql --test sql_compat       → 10 passed
cargo test -p krishiv-connectors                  → 51 unit + 2 certification = 53 passed
cargo test -p krishiv-governance                  → 12 passed
cargo test -p krishiv-chaos                       → 7 passed
cargo test -p krishiv-upgrade-tests               → 6 passed
cargo test -p krishiv-flight-sql                  → 13 passed
cargo test -p krishiv-executor                    → 46 passed (background, confirmed exit 0)
cargo test -p krishiv-operator                    → 35 passed
cargo test -p krishiv-metrics                     → 6 passed (1 ignored, needs live OTLP)
```

## Architecture Decisions Locked

- **Shuffle (R4a)**: `ExecutorTaskRunner::with_inmem_shuffle()` + typed `ShuffleWriteConfig`/`ShuffleReadConfig`
- **State backend (R5a)**: `RedbStateBackend` (redb 2.x, ACID, pure-Rust)
- **Checkpoint barrier (R6a)**: Out-of-band `trigger_checkpoint_for_job()` → executor acks via `checkpoint_ack()` RPC (now fully wired)
- **2PC sink (R6c)**: `.tmp` on prepare, atomic rename on commit, delete on abort
- **JDBC/ODBC gateway (R10)**: Arrow Flight SQL (`KrishivFlightSqlService`) with `AuthProvider` + `PolicyHook` chain
- **Policy enforcement (R10)**: `PolicyEnforcingSqlEngine` at DataFusion execution boundary; wired through `Session::sql_as` and Flight SQL
- **Materialized views (R10)**: Refresh-on-commit, LSN-based staleness, in-memory registry (wired into `SqlEngine`)
- **CDC (R10)**: Debezium 2.x JSON over Kafka → Iceberg, idempotent-exactly-once via LSN dedup key; column-level unpacking
- **Audit (R10)**: Structured `AuditEvent` + pluggable `AuditSink`; default `TracingAuditSink`

## Deferred to R11

- AQE coalescing (R4b), LZ4/Zstd shuffle compression (R4c)
- Watermark operator, tumbling window, continuous loop (R5b/R5c)
- Full gRPC barrier transport (R6b)
- Incremental materialized view maintenance
- Multi-table CDC fan-out with schema evolution
- TPC-H/TPC-DS SF100 benchmark tier
