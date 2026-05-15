# R8 Lakehouse And Python Beta Implementation Tracker

## Goal

Deliver broader data platform usability through Python bindings, vectorized Python UDFs, stabilized Rust UDF/UDAF/UDTF contracts, Iceberg beta support, and Flight SQL.

R8 makes Krishiv usable by data engineers and analysts while keeping core Rust runtime contracts stable.

## Scope

In scope:

- Python bindings through PyO3.
- Python `Session` and `DataFrame` bindings.
- Vectorized Python UDF support over Arrow batches.
- UDF isolation boundary.
- Stable Rust UDF, UDAF, and UDTF contracts.
- Iceberg read/write beta.
- Iceberg snapshot reads.
- Iceberg schema evolution support.
- Iceberg partition evolution support.
- Iceberg time travel support.
- Flight SQL endpoint.
- Beta documentation for Python and lakehouse APIs.

Out of scope:

- Full Delta Lake parity.
- JDBC/ODBC gateway.
- Full SQL warehouse feature set.
- Python streaming API parity if it would destabilize R8.
- Production-grade Iceberg compaction and maintenance services.

## Dependencies

- R3 connectors exist.
- R4 batch execution supports joins, aggregation, and runtime stats.
- R6 checkpoint semantics exist for reliable write paths.
- R7 resource governance can protect Python and lakehouse workloads.
- Public Rust API contracts are stable enough to bind.

## Architecture Deliverables

- [ ] Add `crates/krishiv-python`.
- [ ] Add `crates/krishiv-udf`.
- [ ] Add `crates/krishiv-lakehouse`.
- [ ] Define Python binding boundaries.
- [ ] Define Arrow-based Python data exchange.
- [ ] Define UDF isolation boundary.
- [ ] Define Iceberg catalog/table integration boundary.
- [ ] Define Flight SQL service boundary.
- [ ] Document beta compatibility policy for Python and lakehouse APIs.

## API And Interface Deliverables

- [ ] Add Python `Session` binding.
- [ ] Add Python `DataFrame` binding.
- [ ] Add Python query execution API.
- [ ] Add vectorized Python UDF registration API.
- [ ] Stabilize Rust UDF contract.
- [ ] Stabilize Rust UDAF contract.
- [ ] Stabilize Rust UDTF contract.
- [ ] Add Iceberg table registration API.
- [ ] Add Iceberg snapshot read API.
- [ ] Add Iceberg write API beta.
- [ ] Add Flight SQL endpoint configuration.

## Runtime Deliverables

- [ ] Implement PyO3 crate setup.
- [ ] Implement Arrow batch exchange with Python.
- [ ] Implement vectorized Python UDF execution.
- [ ] Implement UDF error propagation.
- [ ] Implement UDF resource isolation hooks.
- [ ] Implement Iceberg read beta.
- [ ] Implement Iceberg write beta.
- [ ] Implement snapshot reads.
- [ ] Implement schema evolution support.
- [ ] Implement partition evolution support.
- [ ] Implement time travel support.
- [ ] Implement Flight SQL service.

## Test Checklist

- [ ] Python package build test passes.
- [ ] Python `Session` smoke tests pass.
- [ ] Python `DataFrame` smoke tests pass.
- [ ] Vectorized Python UDF tests pass.
- [ ] Rust UDF tests pass.
- [ ] Rust UDAF tests pass.
- [ ] Rust UDTF tests pass.
- [ ] Iceberg snapshot read tests pass.
- [ ] Iceberg write smoke tests pass.
- [ ] Iceberg schema evolution tests pass.
- [ ] Iceberg partition evolution tests pass.
- [ ] Flight SQL smoke tests pass.

## Acceptance Gate

R8 is complete when:

- [ ] Python query smoke tests pass.
- [ ] Vectorized Python UDF tests pass.
- [ ] Iceberg snapshot read/write smoke tests pass.
- [ ] Flight SQL smoke tests pass.
- [ ] Python and lakehouse APIs are clearly marked beta.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| API surface grows too quickly | Mark Python/lakehouse APIs beta and freeze Rust core first |
| Python UDFs hide expensive work | Add isolation hooks, metrics, and clear operator visibility |
| Iceberg writes conflict with checkpoint semantics | Route write certification through R6 checkpoint/sink contracts |
| Flight SQL becomes a separate query path | Route Flight SQL through the same session/planner/runtime APIs |
| Delta pressure distracts from Iceberg | Keep Iceberg first; defer Delta unless requirements change |
