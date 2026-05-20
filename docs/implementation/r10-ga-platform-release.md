# R10 GA Platform Release Implementation Tracker

## Goal

Deliver Krishiv's stable public platform release with API stability policy, SQL/function compatibility matrix, certified connectors, JDBC/ODBC access, CDC-to-lakehouse pipelines, materialized views baseline, data quality rules, upgrade tests, chaos tests, and benchmark gates.

R10 establishes Krishiv as a production-ready platform with honest compatibility, performance, and operational guarantees.

## Scope

In scope:

- Stable API policy.
- SQL compatibility matrix.
- Function compatibility matrix.
- Certified connector matrix.
- JDBC gateway.
- ODBC gateway.
- CDC-to-lakehouse pipeline template.
- Materialized views baseline.
- Data quality expectation rules.
- Rejected-row output support.
- Dead-letter sink support.
- Upgrade test suite.
- Metadata schema upgrade and downgrade-readiness tests for job, event-log, checkpoint, savepoint, connector, and catalog metadata.
- Chaos test suite.
- TPC-H benchmark suite.
- TPC-DS benchmark suite.
- Nexmark benchmark suite.
- Published benchmark report.
- Production hardening guide.

Out of scope:

- Spark API compatibility.
- Flink API compatibility.
- Full managed cloud service.
- Global multi-region active-active job execution.
- Complete Delta Lake parity unless explicitly promoted before GA.

## Dependencies

- R1-R9 acceptance gates are complete.
- Public APIs are stable enough to freeze.
- Connector certification suite exists and covers supported guarantees.
- Observability, audit, and HA behavior exist.
- Benchmark infrastructure can run repeatably.

## Architecture Deliverables

- [x] Define GA stability policy.
- [x] Define compatibility matrix format.
- [x] Define connector certification matrix format.
- [x] Define JDBC gateway architecture.
- [x] Define ODBC gateway architecture.
- [x] Define CDC-to-lakehouse reference architecture.
- [x] Define materialized views baseline architecture.
- [x] Define data quality rule model.
- [x] Define upgrade compatibility policy.
- [x] Define metadata schema compatibility policy for every persisted metadata family.
- [x] Define benchmark performance targets (TPC-H SF10 per-query time limits, TPC-DS SF10 limits, Nexmark minimum events/second on reference hardware) before R10 implementation begins.
- [x] Define benchmark reporting policy.

## API And Interface Deliverables

- [ ] Publish stable API policy.
- [ ] Publish SQL compatibility matrix.
- [ ] Publish function compatibility matrix.
- [ ] Publish connector certification matrix.
- [ ] Add JDBC gateway.
- [ ] Add ODBC gateway.
- [ ] Add CDC-to-lakehouse pipeline template.
- [ ] Add materialized view declaration interface.
- [x] Add data quality expectation interface.
- [x] Add rejected-row output configuration.
- [x] Add dead-letter sink configuration.
- [x] Publish production hardening guide.

## Runtime Deliverables

- [ ] Implement JDBC gateway.
- [ ] Implement ODBC gateway.
- [ ] Implement CDC-to-lakehouse template.
- [ ] Implement materialized views baseline.
- [x] Implement data quality expectation rules.
- [x] Implement rejected-row output.
- [x] Implement dead-letter sink support.
- [x] Implement upgrade test suite.
- [ ] Implement metadata schema upgrade tests for job, event-log, checkpoint, savepoint, connector, and catalog metadata.
- [ ] Implement chaos test suite.
- [ ] Implement TPC-H benchmark suite.
- [ ] Implement TPC-DS benchmark suite.
- [ ] Implement Nexmark benchmark suite.
- [ ] Optimize top benchmark regressions before GA.
- [ ] Freeze GA-supported API and connector surfaces.

## Test Checklist

- [ ] API compatibility tests pass.
- [ ] SQL compatibility tests pass.
- [ ] Function compatibility tests pass.
- [ ] Connector certification matrix passes.
- [ ] JDBC smoke tests pass.
- [ ] ODBC smoke tests pass.
- [ ] CDC-to-lakehouse tests pass.
- [ ] Materialized view tests pass.
- [x] Data quality rule tests pass.
- [x] Rejected-row output tests pass.
- [x] Dead-letter sink tests pass.
- [x] Upgrade tests pass.
- [ ] Metadata schema upgrade tests pass for every GA-supported persisted metadata family.
- [ ] Chaos suite passes.
- [ ] TPC-H benchmark gate passes.
- [ ] TPC-DS benchmark gate passes.
- [ ] Nexmark benchmark gate passes.

## Acceptance Gate

R10 is complete when:

- [ ] GA benchmark gates pass against the published numeric performance targets.
- [ ] Upgrade tests pass.
- [ ] Metadata schema compatibility tests pass.
- [ ] Chaos suite passes.
- [ ] Certified connector matrix passes.
- [x] Public API stability policy is documented.
- [ ] SQL/function compatibility matrix is published.
- [ ] Production hardening guide is published.

## Sprint 1b Progress (2026-05-20)

Sprint 1b delivered three deferred R9 items:

- **K8s Lease API (Task 1)**: `K8sLeaseElection` in `crates/krishiv-operator/src/lib.rs` already had the full live K8s Lease API implementation with `k8s_try_acquire`, `k8s_renew`, and `k8s_release` async helpers driven via `block_on`. Fixed compilation errors: added `tracing` dependency, renamed `Patch::MergePatch` → `Patch::Merge` (kube 2.x API), added manual `Debug` impl for `K8sLeaseElection` (kube::Client is not Debug). All 35 operator tests pass including `k8s_lease_simulation_mode_works`.

- **OTLP Integration Test (Task 2)**: Added `otlp_endpoint: Option<String>` to `MetricsConfig`, updated `init()` to return `Result<MetricsHandle, String>`, added `MetricsHandle::shutdown()` method, added `opentelemetry-otlp` dependency, and added `#[ignore]` OTLP integration test. All 6 metrics tests pass; OTLP test is correctly skipped.

- **kind e2e CI (Task 3)**: Created `.github/workflows/kind-e2e.yml` with full kind cluster lifecycle, failover test, and log artifact upload on failure. Triggered on push to main/release/** and PRs touching operator/scheduler/checkpoint.

`cargo check --workspace` passes cleanly.

## Sprint 2 Progress (2026-05-20)

Sprint 2 delivered data quality rules, dead-letter sink, upgrade compatibility tests, and connector certification suite.

- **Data Quality (Task 1)**: Added `DataQualityRule`, `QualityAction`, `DataQualityConfig`, `RejectedRow`, `DataQualityCheckResult`, `check_batch`, `find_violations`, and `DeadLetterSink` to `crates/krishiv-connectors/src/lib.rs`. 4 quality tests pass (notnull reject, range reject, fail flag, dead-letter split).

- **Upgrade Tests (Task 2)**: Created `crates/krishiv-upgrade-tests` crate with 6 integration tests covering v0 offset round-trip, schema_version=1 job/catalog/event blobs, missing-version defaulting, and too-new version rejection.

- **Connector Certification (Task 3)**: Created `crates/krishiv-connectors/tests/certification.rs` and `tests/certification/mod.rs` with 2 certification tests — capability declaration and dead-letter null split.

Validation: `cargo test -p krishiv-connectors` → 43 passed (41 unit + 2 cert); `cargo test -p krishiv-upgrade-tests` → 6 passed; `cargo check --workspace` → clean.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Performance gaps vs Spark/Flink | Publish benchmark matrix and optimize top regressions before GA |
| Benchmark targets are defined too late | Publish numeric targets before R10 implementation starts; use R8 engine measurements as the baseline |
| GA scope keeps expanding | Freeze scope to compatibility, certification, benchmarks, and production hardening |
| Stable API freezes weak designs | Freeze only APIs proven across previous releases |
| JDBC/ODBC bypass governance | Route BI gateways through the same session, auth, audit, and planner paths |
| CDC-to-lakehouse correctness is overclaimed | Certify only supported CDC/source/table combinations |
