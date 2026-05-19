# R5 Stateful Streaming Core Implementation Tracker

## Goal

Deliver Krishiv's stateful streaming core in two sequential sub-milestones.

**R5.1** delivers exactly one certified streaming path end-to-end: Kafka source → tumbling event-time window → in-memory keyed state → Kafka sink, running deterministically under replay. One correct path proves the streaming execution model is right before adding more window types.

**R5.2** hardens the streaming core with RocksDB, sliding and session windows, multi-source watermarks, state TTL, and stream-table join.

**Rule:** R5.2 cannot begin until R5.1's acceptance gate passes. The streaming execution model must be proven correct on one path before being generalized.

## Scope

In scope:

- Keyed stream API.
- Event-time timestamp assignment.
- Watermark propagation (single source in R5.1; multi-source in R5.2).
- Processing-time and event-time timers.
- Tumbling windows (R5.1).
- Sliding and session windows (R5.2).
- Keyed state API.
- In-memory state backend (R5.1).
- RocksDB state backend (R5.2).
- State TTL (R5.2).
- Stream-table join baseline (R5.2).
- State inspection CLI.
- Deterministic replay tests.
- Checkpoint-barrier and watermark interaction protocol design.

Out of scope:

- Exactly-once (deferred to R6).
- Durable checkpoint coordination (R6).
- Savepoints (R6).
- State rescaling (R6+).
- Tiered remote state.
- Complex Event Processing.
- Production HA coordinator failover (R9).
- Streaming Python UDFs (post-GA via subprocess model; batch UDFs only in R8.1).

## Dependencies

- R1 streaming API skeleton exists.
- R2 distributed streaming DAG submission exists.
- R3.1 real executor and gRPC transport exist.
- R3.2 Kafka source/sink contracts exist.
- R4 partitioning model can support keyed distribution.
- `docs/architecture/streaming-execution-model.md` is written and approved (R4 deliverable; R5.1 must not start until this document exists).
- `docs/architecture/checkpoint-protocol.md` is written and reviewed (R5.1 pre-condition; NOT an R6 deliverable).
- `TaskRunner` enum dispatch separating batch and streaming execution paths exists in `krishiv-executor`.
- `JobRecord::refresh_state()` in `krishiv-scheduler` guards streaming jobs from transitioning to `JobState::Succeeded`.

---

## R5.1: One Certified Streaming Path

### Goal

Prove the streaming execution model is correct on a single end-to-end path before generalizing. The acceptance gate is strict: deterministic replay must produce identical output given identical input.

**Certified path:** Kafka (single partition) → event-time tumbling window → in-memory keyed state → Kafka sink.

### Architecture Deliverables

- [ ] Add `TaskRunner` trait (or equivalent enum dispatch) to `krishiv-executor` separating batch-terminal execution from streaming-continuous execution. R5.1 streaming operators MUST use the streaming runner; R1–R4 batch operators MUST continue to use the batch runner unchanged.
- [ ] Write `docs/architecture/checkpoint-protocol.md` covering aligned checkpoint barrier model, barrier/watermark ordering invariants, and R5.1 simulation requirements. Must exist and be reviewed before R5.1 streaming window implementation starts.
- [ ] Define keyed-distribution stability contract per `docs/architecture/keyed-distribution-stability.md`: `key_by(column)` guarantees same-key → same task for the job lifetime; AQE coalescing is disabled for streaming stages.
- [ ] Add `crates/krishiv-state`.
- [ ] Define keyed state API (read, write, clear per key).
- [ ] Define state namespace model.
- [ ] Define timer service abstraction (event-time only in R5.1).
- [ ] Define single-source watermark propagation rules.
- [ ] Define in-memory state backend interface.
- [ ] Define continuous operator execution loop: how a streaming stage differs from a batch stage in the executor (no terminal completion; produces output continuously).
- [ ] Define streaming job lifecycle in the scheduler (streaming jobs never transition to Succeeded while running).
- [ ] Define streaming job re-attach protocol: on coordinator restart, active streaming executors re-register with current task state (last watermark, last processed Kafka offset); coordinator re-attaches to the running job instead of creating a new one.
- [ ] Define checkpoint-barrier and watermark interaction protocol for R6 checkpoint implementation.
- [ ] Define how barriers flow through single-source tumbling windows without closing windows incorrectly.
- [ ] Define clock skew handling policy: Krishiv trusts the `event_time` field in source records as-is. Late events (event_time < current watermark) are dropped. Clock skew between producers is the operator's responsibility at the source. The `allowed_lateness` window in the watermark configuration is the primary mechanism for tolerating moderate producer clock skew. This policy must be documented in R5.1 release notes.
- [ ] Document R5.1 streaming semantics, limitations, and the exact watermark model used.

### API And Interface Deliverables

- [ ] Add `key_by` to the stream API.
- [ ] Add event-time timestamp assignment API.
- [ ] Add watermark configuration API (single source, fixed lag).
- [ ] Add tumbling window API.
- [ ] Add event-time timer API for internal operators.

### Runtime Deliverables

- [ ] Implement continuous operator execution loop on executor (input RecordBatch loop, no terminal state).
- [ ] Implement streaming job lifecycle in coordinator (no auto-transition to Succeeded).
- [ ] Implement streaming job re-attach: on coordinator restart, accept executor re-registration with current watermark and offset; resume the job from executor-reported state instead of re-submitting a fresh job.
- [ ] Implement in-memory keyed state backend.
- [ ] Implement event-time timers.
- [ ] Implement single-source watermark propagation.
- [ ] Implement tumbling window aggregation.
- [ ] Implement deterministic replay harness (replay the same Kafka input, compare outputs).
- [ ] Implement checkpoint-barrier protocol simulation for the certified path (metadata only; durable checkpoints remain R6).

### Test Checklist

- [ ] Keyed state read/write/clear unit tests pass.
- [ ] In-memory state backend tests pass.
- [ ] Event-time timer fires at correct watermark.
- [ ] Single-source watermark propagation advances correctly.
- [ ] Tumbling window correctness tests pass (windows close at the right watermark).
- [ ] Deterministic replay test: same Kafka input produces identical output on two consecutive runs.
- [ ] Checkpoint-barrier simulation preserves watermark/window ordering.
- [ ] Streaming job remains in Running state in coordinator and does not auto-transition to Succeeded.
- [ ] Re-attach test: coordinator restarts while streaming executors are active; executors re-register with current watermark and offset; job resumes without re-processing already-committed events.
- [ ] R1-R4 batch behavior still passes (no regression).

### Acceptance Gate For R5.1

- [ ] Kafka (single partition) → tumbling window → in-memory state → Kafka sink runs end-to-end on real executors.
- [ ] Watermarks close windows correctly.
- [ ] Deterministic replay produces identical output.
- [ ] Streaming job lifecycle is correctly modeled in the coordinator.
- [ ] Coordinator restart while streaming job runs: job re-attaches from executor-reported state without duplicate reprocessing.
- [ ] Checkpoint-barrier and watermark interaction is documented and validated in simulation before R6 starts.
- [ ] R1-R4 supported batch behavior still passes.
- [ ] `docs/architecture/streaming-execution-model.md` was reviewed and used as the implementation spec.

---

## R5.2: Streaming Hardening

### Goal

Generalize the proven R5.1 streaming model to multiple window types, RocksDB, multi-source watermarks, state TTL, and stream-table join. R5.2 begins only after R5.1's acceptance gate passes.

### Architecture Deliverables

- [ ] Define RocksDB async isolation boundary using `spawn_blocking` (all RocksDB calls must leave the Tokio worker thread).
- [ ] Define RocksDB compaction thread budget (must not starve Tokio workers).
- [ ] Define multi-source watermark reconciliation rules (min watermark across all sources).
- [ ] Define state TTL semantics and cleanup trigger model.
- [ ] Define state inspection safety boundaries (read-only metadata; no mutation from inspection).
- [ ] **Define executor deployment model for stateful streaming:** Executors are Kubernetes `Deployment` pods (not `StatefulSet`). RocksDB on executor local disk is ephemeral. On pod restart, RocksDB state is rebuilt from the last successful checkpoint on S3. This is the only supported model in R5.2. `StatefulSet` with PVC-backed RocksDB is explicitly out of scope and unsupported until proven in a future release.

### API And Interface Deliverables

- [ ] Add sliding window API.
- [ ] Add session window API.
- [ ] Add processing-time timer API.
- [ ] Add multi-source watermark configuration API.
- [ ] Add state TTL configuration.
- [ ] Add `krishiv state inspect` CLI skeleton.

### Runtime Deliverables

- [ ] Implement RocksDB keyed state backend.
- [ ] Implement processing-time timers.
- [ ] Implement multi-source watermark propagation.
- [ ] Implement sliding window aggregation.
- [ ] Implement session window aggregation.
- [ ] Implement state TTL cleanup.
- [ ] Implement stream-table join baseline.
- [ ] Implement safe state metadata inspection.
- [ ] Add RocksDB latency tests vs in-memory backend under load.

### Test Checklist

- [ ] RocksDB state backend tests pass.
- [ ] Processing-time timer tests pass.
- [ ] Multi-source watermark propagation tests pass.
- [ ] Sliding window tests pass.
- [ ] Session window tests pass.
- [ ] State TTL removes expired state.
- [ ] Stream-table join baseline tests pass.
- [ ] RocksDB does not block Tokio worker threads under sustained load.
- [ ] R5.1 certified streaming path still passes with RocksDB backend.

### Acceptance Gate For R5.2

- [ ] A recoverable stateful window aggregation behaves deterministically under replay using RocksDB backend.
- [ ] Multi-source watermarks close windows correctly.
- [ ] State TTL removes expired state.
- [ ] State inspection reads metadata without mutating state.
- [ ] R1-R5.1 supported behavior still passes.

---

## Risks And Mitigations

| Risk | Mitigation |
|---|---|
| R5.1 streaming execution model is wrong; R5.2 would generalize a broken model | Gate R5.2 on R5.1 deterministic replay acceptance; do not generalize until replay proves correctness |
| Watermark semantics are underspecified | `docs/architecture/streaming-execution-model.md` must exist and be approved before R5.1 implementation starts |
| Coordinator restart causes split-brain with running streaming executors | Implement streaming job re-attach in R5.1; test with coordinator restart mid-stream before any R6 work begins |
| Checkpoint barriers conflict with watermarks | Define and simulate barrier/watermark ordering in R5.1 before durable checkpoints arrive in R6 |
| RocksDB introduces blocking work in async paths | Define `spawn_blocking` isolation boundary and compaction thread budget in R5.2 architecture before any RocksDB code is written |
| State inspection mutates or corrupts live state | Keep inspection read-only and metadata-focused; add mutation-detection assertion in tests |
| Streaming APIs overfit the R5.1 example | Keep public APIs minimal; document beta semantics; design `key_by`/window API to generalize to R5.2 window types |
| R5.1 acceptance gate takes too long | Do not relax the gate; adjust R5.2 start date instead |
| Streaming jobs auto-transition to Succeeded when tasks complete | `refresh_state()` streaming guard must be applied and tested before R5.1 streaming task dispatch is wired |
| AQE coalescing invalidates streaming keyed state | `StreamingAqeGuard` rule in optimizer pipeline must skip AQE coalescing for streaming plans |
