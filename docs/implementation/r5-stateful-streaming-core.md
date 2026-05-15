# R5 Stateful Streaming Core Implementation Tracker

## Goal

Deliver Krishiv's Flink-style stateful streaming core: keyed streams, event time, watermarks, timers, windows, state TTL, stream-table join baseline, and state inspection.

R5 proves that Krishiv can run stateful streaming pipelines with deterministic behavior before adding full checkpoint/savepoint reliability in R6.

## Scope

In scope:

- Keyed stream API.
- Event-time timestamp assignment.
- Watermark propagation.
- Processing-time and event-time timers.
- Tumbling, sliding, and session windows.
- Keyed state API.
- In-memory state backend.
- RocksDB state backend.
- State TTL.
- Stream-table join baseline.
- State inspection CLI.
- Deterministic replay tests.

Out of scope:

- Exactly-once.
- Durable checkpoint coordination.
- Savepoints.
- State rescaling.
- Tiered remote state.
- Complex Event Processing.
- Production HA coordinator failover.

## Dependencies

- R1 streaming API skeleton exists.
- R2 distributed streaming DAG submission exists.
- R3 Kafka source/sink contracts exist.
- R4 partitioning model can support keyed distribution.

## Architecture Deliverables

- [ ] Add `crates/krishiv-state`.
- [ ] Define keyed state API.
- [ ] Define state namespace model.
- [ ] Define timer service abstraction.
- [ ] Define watermark propagation rules.
- [ ] Define state TTL semantics.
- [ ] Define state inspection safety boundaries.
- [ ] Document R5 streaming semantics and limitations.

## API And Interface Deliverables

- [ ] Add `key_by` to the stream API.
- [ ] Add event-time timestamp assignment API.
- [ ] Add watermark configuration API.
- [ ] Add timer API for internal operators.
- [ ] Add tumbling window API.
- [ ] Add sliding window API.
- [ ] Add session window API.
- [ ] Add state TTL configuration.
- [ ] Add `krishiv state inspect` CLI skeleton.

## Runtime Deliverables

- [ ] Implement in-memory keyed state backend.
- [ ] Implement RocksDB keyed state backend.
- [ ] Implement processing-time timers.
- [ ] Implement event-time timers.
- [ ] Implement watermark propagation.
- [ ] Implement tumbling window aggregation.
- [ ] Implement sliding window aggregation.
- [ ] Implement session window aggregation.
- [ ] Implement state TTL cleanup.
- [ ] Implement stream-table join baseline.
- [ ] Implement safe state metadata inspection.
- [ ] Add deterministic replay harness.

## Test Checklist

- [ ] Keyed state unit tests pass.
- [ ] In-memory state backend tests pass.
- [ ] RocksDB state backend tests pass.
- [ ] Timer tests pass.
- [ ] Watermark propagation tests pass.
- [ ] Tumbling window tests pass.
- [ ] Sliding window tests pass.
- [ ] Session window tests pass.
- [ ] State TTL tests pass.
- [ ] Stream-table join baseline tests pass.
- [ ] Deterministic replay tests pass.

## Acceptance Gate

R5 is complete when:

- [ ] A recoverable stateful window aggregation behaves deterministically under replay.
- [ ] Watermarks close windows correctly.
- [ ] State TTL removes expired state.
- [ ] State inspection can read supported state metadata without mutating state.
- [ ] R1-R4 supported batch behavior still passes.

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| State correctness bugs | Add deterministic replay and model tests before optimizing |
| Watermark semantics are unclear | Document event-time and lateness behavior in R5 docs |
| RocksDB introduces blocking work in async paths | Isolate RocksDB operations away from Tokio worker threads |
| State inspection mutates or corrupts state | Keep inspection read-only and metadata-focused in R5 |
| Streaming APIs overfit early examples | Keep public APIs small and document beta semantics |
