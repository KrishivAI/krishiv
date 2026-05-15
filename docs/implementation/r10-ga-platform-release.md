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

- [ ] Define GA stability policy.
- [ ] Define compatibility matrix format.
- [ ] Define connector certification matrix format.
- [ ] Define JDBC gateway architecture.
- [ ] Define ODBC gateway architecture.
- [ ] Define CDC-to-lakehouse reference architecture.
- [ ] Define materialized views baseline architecture.
- [ ] Define data quality rule model.
- [ ] Define upgrade compatibility policy.
- [ ] Define benchmark reporting policy.

## API And Interface Deliverables

- [ ] Publish stable API policy.
- [ ] Publish SQL compatibility matrix.
- [ ] Publish function compatibility matrix.
- [ ] Publish connector certification matrix.
- [ ] Add JDBC gateway.
- [ ] Add ODBC gateway.
- [ ] Add CDC-to-lakehouse pipeline template.
- [ ] Add materialized view declaration interface.
- [ ] Add data quality expectation interface.
- [ ] Add rejected-row output configuration.
- [ ] Add dead-letter sink configuration.
- [ ] Publish production hardening guide.

## Runtime Deliverables

- [ ] Implement JDBC gateway.
- [ ] Implement ODBC gateway.
- [ ] Implement CDC-to-lakehouse template.
- [ ] Implement materialized views baseline.
- [ ] Implement data quality expectation rules.
- [ ] Implement rejected-row output.
- [ ] Implement dead-letter sink support.
- [ ] Implement upgrade test suite.
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
- [ ] Data quality rule tests pass.
- [ ] Rejected-row output tests pass.
- [ ] Dead-letter sink tests pass.
- [ ] Upgrade tests pass.
- [ ] Chaos suite passes.
- [ ] TPC-H benchmark gate passes.
- [ ] TPC-DS benchmark gate passes.
- [ ] Nexmark benchmark gate passes.

## Acceptance Gate

R10 is complete when:

- [ ] GA benchmark gates pass.
- [ ] Upgrade tests pass.
- [ ] Chaos suite passes.
- [ ] Certified connector matrix passes.
- [ ] Public API stability policy is documented.
- [ ] SQL/function compatibility matrix is published.
- [ ] Production hardening guide is published.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Performance gaps vs Spark/Flink | Publish benchmark matrix and optimize top regressions before GA |
| GA scope keeps expanding | Freeze scope to compatibility, certification, benchmarks, and production hardening |
| Stable API freezes weak designs | Freeze only APIs proven across previous releases |
| JDBC/ODBC bypass governance | Route BI gateways through the same session, auth, audit, and planner paths |
| CDC-to-lakehouse correctness is overclaimed | Certify only supported CDC/source/table combinations |
