# Phase 1 — Define the Engine Contract

Goal: make every batch, streaming, state, connector, and lakehouse guarantee
specific, versioned, and testable.

## Implementation resolution

- [x] Publish batch and streaming semantic contracts.
- [x] Publish a combination-specific exactly-once matrix.
- [x] Define source/sink capability semantics and expose capabilities through
      concrete and dynamic source/sink interfaces.
- [x] Add a machine-readable delivery guarantee derived conservatively from
      connector capabilities.
- [x] Version typed task-fragment envelopes and reject unknown versions.
- [x] Publish checkpoint metadata writer/restore compatibility (`v2`, reads v1-v2).
- [x] Version savepoint metadata and reject unknown versions.
- [x] Define stable operator identity and direct state compatibility APIs.
- [x] Define connector maturity levels (`experimental`, `preview`, `certified`).
- [x] Label every in-tree connector in the connector inventory.
- [x] Correct AI/vector and lakehouse documentation drift.
- [x] Remove AI/vector integrations from standard `full` build presets.
- [x] Select Apache Iceberg as the default/primary lakehouse platform.

## Follow-up certification work

These items are required before moving connectors or guarantees to a higher
maturity level; they are deliberately not marked complete by API-only work.

- [ ] Add a reusable external connector certification harness.
- [ ] Certify Kafka → Iceberg across executor kill, coordinator kill, broker
      retry, object-store retry, commit retry, and restore.
- [ ] Certify transactional Kafka sink recovery and transactional-ID fencing.
- [ ] Certify two-phase Parquet/S3 cleanup and idempotent commit retry.
- [ ] Add golden compatibility fixtures for checkpoint v1/v2 and savepoint v1.
- [ ] Add explicit operator-ID assignment to every stateful physical operator.
- [ ] Add deployment-time validation that rejects duplicate operator IDs.
- [ ] Add savepoint restore tooling for operator rename mappings and state drops.
- [ ] Promote Iceberg from preview to certified after snapshot, schema evolution,
      partition evolution, concurrent commit, and failure tests pass.

## Exit criteria

Phase 1 APIs and documentation are implemented when focused tests pass. The
engine may only advertise a certified exactly-once combination after the
corresponding follow-up certification tests are complete.
