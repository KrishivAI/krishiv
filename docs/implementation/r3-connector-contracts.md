# R3 Connector Contracts Implementation Tracker

## Goal

Deliver Krishiv's production I/O baseline by defining connector contracts and certifying the first source/sink integrations: Parquet, Kafka, and S3-compatible object storage.

R3 makes connector semantics explicit so later checkpointing, exactly-once certification, lakehouse writes, and CDC support can build on stable contracts.

## Scope

In scope:

- Connector traits.
- Connector capability flags.
- Source offset model.
- Sink commit model.
- Parquet reader and writer.
- Kafka source and sink.
- S3-compatible object store integration.
- Schema registry abstraction.
- At-least-once sink contract.
- CDC design document.
- Connector certification test kit.

Out of scope:

- Full exactly-once certification.
- Kafka transaction certification.
- Iceberg/Delta table support.
- JDBC source/sink.
- CDC implementation beyond design.
- Global connector marketplace or plugin loading.

## Dependencies

- R1 local execution and DataFusion integration exist.
- R2 distributed submission and job/task status exist.
- Runtime can pass source/sink configuration into tasks.
- Basic job state can surface connector failures.

## Architecture Deliverables

- [ ] Add `crates/krishiv-connectors`.
- [ ] Define connector module boundaries.
- [ ] Define source lifecycle.
- [ ] Define sink lifecycle.
- [ ] Define offset persistence boundary.
- [ ] Define sink commit boundary.
- [ ] Define connector capability flags.
- [ ] Document connector guarantee vocabulary.

## API And Interface Deliverables

- [ ] Define `Source` trait.
- [ ] Define `Sink` trait.
- [ ] Define `Offset` model.
- [ ] Define `CommitHandle` model.
- [ ] Define `ConnectorCapabilities`.
- [ ] Include capability flags: bounded, unbounded, rewindable, transactional, idempotent.
- [ ] Define schema registry abstraction.
- [ ] Add connector configuration validation errors.
- [ ] Add connector certification test harness interface.

## Runtime Deliverables

- [ ] Implement Parquet reader.
- [ ] Implement Parquet writer.
- [ ] Implement S3-compatible object store reads.
- [ ] Implement S3-compatible object store writes.
- [ ] Implement Kafka source.
- [ ] Implement Kafka sink.
- [ ] Add source offset tracking.
- [ ] Add at-least-once sink contract.
- [ ] Surface connector capabilities in job metadata.
- [ ] Write CDC design document under `docs/rfcs/`.

## Test Checklist

- [ ] Connector trait unit tests pass.
- [ ] Parquet read/write certification tests pass.
- [ ] S3 read/write certification tests pass.
- [ ] Kafka source/sink certification tests pass for supported semantics.
- [ ] Offset serialization tests pass.
- [ ] Connector config validation tests pass.
- [ ] Failure-mode tests document unsupported guarantees.

## Acceptance Gate

R3 is complete when:

- [ ] Parquet, Kafka, and S3 connectors pass certification tests.
- [ ] Every connector declares capability flags.
- [ ] Source offsets are visible in job metadata or logs.
- [ ] At-least-once sink behavior is documented.
- [ ] CDC design is written and linked from the roadmap or implementation docs.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Connector semantics diverge | Require capability flags and certification tests |
| Source offsets become connector-specific strings | Use structured offset models with connector-owned payloads only where needed |
| At-least-once behavior is mistaken for exactly-once | Document delivery guarantees per connector and sink mode |
| S3 behavior differs across providers | Test against S3-compatible contract and document provider-specific limitations |
| CDC scope expands too early | Keep R3 CDC to design only |
