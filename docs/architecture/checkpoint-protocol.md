# Krishiv Checkpoint Barrier Protocol

**Status:** Decision — approved for R5.1 simulation and R6 durable implementation.
**Owner:** Architecture team.
**Linked releases:** R5.1 (barrier simulation and watermark ordering validation), R6 (durable checkpoint storage and exactly-once coordination).

---

## Purpose

Checkpoint barriers provide the mechanism for:

- **Fault tolerance:** After a failure, execution resumes from the last committed checkpoint epoch rather than from the beginning of the job.
- **Exactly-once delivery:** Barriers coordinate source offsets, operator state snapshots, and sink commit handles into a single atomic epoch. Exactly-once is certified only for specific source/sink/checkpoint combinations (R6 scope).
- **Coordinator recovery:** The checkpoint coordinator can reconstruct job execution state from committed checkpoint epochs without querying running executors.

Without barriers, stateful streaming operators have no consistent point from which to snapshot state, and source offsets cannot be atomically bound to operator state.

---

## Decision: Aligned Checkpoints

Krishiv uses **aligned checkpoints** for R6. Aligned means: an operator must receive the barrier for epoch E on **all** input channels before it snapshots state and acknowledges the barrier.

**Why aligned over unaligned:**

- Aligned checkpoints are significantly simpler to reason about. State captured at the barrier boundary is consistent with all upstream data processed before the barrier and no data processed after it.
- At R6 scale (single certified path, development cluster), the latency spike from holding back processing on slower input channels is acceptable.
- Unaligned checkpoints (where the operator snapshots in-flight records from faster channels along with state) require capturing and restoring buffered records per channel, which adds substantial implementation complexity and storage cost.

**Unaligned checkpoints** are an explicit future optimization, not a current design choice. They may be introduced post-R6 when SF1000+ benchmarks show barrier alignment latency is a bottleneck.

---

## Barrier Injection Model

The **checkpoint coordinator** (running inside the coordinator process, not a separate service) initiates a checkpoint by injecting barrier control records into every source partition of the job.

- A barrier is a special control record tagged with a **checkpoint epoch ID** (`u64`, monotonically increasing).
- Barriers flow through the DAG alongside data records in the same channel (the same Arrow Flight stream or in-process channel). This preserves FIFO ordering between barriers and data.
- The checkpoint coordinator does not inject barriers into the data plane directly; it signals each source operator instance to emit a barrier after its next batch boundary. Source operators emit the barrier as a control record in their output channel.
- Barrier injection is the only role the checkpoint coordinator plays on the data path. All other checkpoint actions (acknowledgment collection, metadata writes) happen on the control plane.

```text
Checkpoint Coordinator
  │
  │  InitiateCheckpoint(epoch=E)
  ▼
Source Operator (partition 0)    Source Operator (partition 1)
  │  emit Barrier(E)               │  emit Barrier(E)
  ▼                                ▼
[data] [data] [Barrier(E)] ...   [data] [Barrier(E)] ...
  │                                │
  ▼                                ▼
         Downstream Operator(s)
              (wait for Barrier(E) on ALL input channels)
              (snapshot state)
              (ack Barrier(E) to coordinator)
```

---

## Barrier And Watermark Ordering Invariants

Barriers and watermarks are both control records that flow in-band with data. They are independent — a barrier does not carry watermark semantics, and a watermark does not carry checkpoint semantics — but their interaction must be precisely defined.

**Invariant 1: FIFO channel ordering.**
Barriers travel in the same channel as data and watermarks. The channel is FIFO. A barrier for epoch E will never overtake a data record or watermark that was emitted before it in the same channel.

**Invariant 2: Watermarks are independent of barriers.**
A watermark at time T and a barrier for epoch E are separate control records. A watermark can be ahead of or behind a barrier in the channel. Neither implies the other.

**Invariant 3: No watermark advancement past a barrier until acknowledgment.**
An operator must not advance its output watermark past a barrier for epoch E until it has acknowledged the barrier for epoch E to the checkpoint coordinator. This prevents downstream operators from closing windows based on data that has not yet been checkpointed.

Formally: if an operator receives `Watermark(T)` after `Barrier(E)` in the same channel, it must buffer the watermark advancement and apply it only after acknowledging `Barrier(E)`.

**Invariant 4: Window outputs before acknowledgment are part of the checkpoint.**
Window outputs triggered by watermark advancement that occurred before the barrier (i.e., by watermarks received before the barrier in the channel) must be flushed and included in the state snapshot for epoch E. A window that closed before the barrier must not reappear after restore from epoch E.

---

## Operator Checkpoint Protocol

Each operator instance follows this sequence when it has received the barrier for epoch E on all input channels:

1. **Receive barrier on all input channels.** For an operator with multiple input channels (e.g., a join), wait until `Barrier(E)` has arrived on every input channel before proceeding. Data arriving after the barrier on a faster channel is buffered and processed after the snapshot is complete (aligned checkpoint semantics).

2. **Finish processing the current batch.** Complete the in-progress `RecordBatch` that was being processed when the last `Barrier(E)` arrived. Do not start a new batch.

3. **Flush pending window outputs.** Emit all window outputs triggered by watermarks received before the barrier. This ensures that window results which logically belong to epoch E are part of the epoch E snapshot, not deferred to epoch E+1.

4. **Snapshot all owned state namespaces.** Call `snapshot(&self)` on every `StateNamespace` owned by this operator instance. The snapshot must be a point-in-time consistent view of the state as of the barrier boundary.

5. **Write snapshot to checkpoint store.** In R6, write the serialized snapshot to the configured durable object store (S3-compatible), keyed by `(job_id, operator_id, task_id, epoch)`. In R5.1 (simulation mode), this step is omitted; the operator records that it would have written a snapshot.

6. **Acknowledge barrier to checkpoint coordinator.** Send `CheckpointAck(job_id, operator_id, task_id, epoch=E)` to the checkpoint coordinator over the control-plane gRPC channel.

7. **Resume processing.** Release any buffered records from faster input channels and continue the normal processing loop.

---

## Coordinator Checkpoint Protocol

The checkpoint coordinator runs as a component of the coordinator process (not a separate process in R5 or R6). It owns the global checkpoint lifecycle for a job.

1. **Decide to initiate epoch E.** The coordinator initiates a new checkpoint epoch based on a time interval or an explicit savepoint request. The epoch counter is monotonically increasing per job and stored in `MetadataStore`.

2. **Inject barriers into all source partitions.** Send `InitiateCheckpoint(epoch=E)` to every source operator instance via the control-plane gRPC channel. Source operators emit `Barrier(E)` into their output channels after their next batch boundary.

3. **Wait for all operator instances to acknowledge epoch E.** Collect `CheckpointAck(epoch=E)` from every operator instance in the job DAG. A timeout is applied; if acknowledgment is not received within the configured window, the checkpoint attempt for epoch E is aborted. The coordinator logs the failure and will initiate epoch E+1 on the next checkpoint interval.

4. **Write checkpoint metadata atomically.** Once all operator instances have acknowledged epoch E, write the checkpoint metadata record to `MetadataStore`: `CheckpointMetadata { epoch: E, operator_snapshots: [...], source_offsets: [...], timestamp }`. The write is atomic with respect to the metadata store (single document write or transactional update).

5. **Mark epoch E committed.** Update `MetadataStore` to mark epoch E as `Committed`. This is the durable signal that epoch E is safe to restore from.

6. **Clean up epoch E-1.** After epoch E is committed, the coordinator schedules deletion of epoch E-1 snapshot data from the object store. Epoch E-2 and older are already cleaned up (rolling cleanup, one epoch of history retained for safety during the cleanup window).

---

## R5.1 Simulation Requirements

R5.1 implements a **metadata-only barrier simulation** that proves barrier/watermark ordering is correct before R6 adds durable snapshot storage.

The simulation must demonstrate:

- Barriers are injected as control records and flow through the DAG in FIFO order with data and watermarks.
- Each operator instance acknowledges the barrier to the coordinator after flushing pending window outputs.
- The coordinator collects all acknowledgments for epoch E and logs a simulated `CheckpointMetadata` record to `MetadataStore` (without writing snapshot data to object store).
- Watermark advancement is held until after barrier acknowledgment (Invariant 3 above).
- Window outputs triggered before the barrier are flushed before acknowledgment (Invariant 4 above).
- The simulation runs deterministically under the R5.1 deterministic replay harness.

**What R5.1 simulation does NOT implement:**
- Durable snapshot writes to object store (R6).
- Restore from a committed checkpoint epoch (R6).
- Two-phase commit for exactly-once sinks (R6).
- State rescaling (post-R6).

The R5.1 simulation acceptance gate: running the certified path (Kafka single partition → tumbling window → in-memory state → Kafka sink) under the barrier simulation must produce the same output as running without the simulation (no windows closed incorrectly, no windows held open incorrectly, watermarks advance correctly after barrier acknowledgment).

---

## Single-Source Tumbling Window Example

This example traces barrier flow through the simplest certified R5.1 path.

**Pipeline:** Kafka (single partition) → event-time tumbling window [00:00, 00:10) → Kafka sink.

**State:** The window operator holds accumulated keyed aggregates for all open windows.

**Scenario:** Checkpoint epoch E is initiated while window [00:00, 00:10) is open and contains partial results. A watermark at 00:07 has already been emitted.

```text
Time →   ... [data t=00:06] [Watermark(00:07)] [data t=00:08] [Barrier(E)] [data t=00:09] ...

Kafka Source Operator:
  - Emits data records, Watermark(00:07), more data, then Barrier(E) after next batch.
  - Watermark(00:07) does NOT close window [00:00, 00:10) — watermark < window end.
  - Barrier(E) is emitted after data t=00:08 is flushed.

Tumbling Window Operator:
  - Receives: [data t=00:06], [Watermark(00:07)], [data t=00:08], [Barrier(E)]
  - On Watermark(00:07): window [00:00, 00:10) stays open (00:07 < 00:10). No output.
  - On Barrier(E): no more input channels to wait for (single source).
    1. Finish current batch: data t=00:08 is already accumulated into window [00:00, 00:10).
    2. Flush pending window outputs: Watermark(00:07) did not close any window. Nothing to flush.
    3. Snapshot state: snapshot of window [00:00, 00:10) with partial aggregate including t=00:06, t=00:08.
    4. Write snapshot (R6) / log simulated snapshot (R5.1).
    5. Ack Barrier(E) to coordinator.
    6. Resume: process [data t=00:09].
  - On subsequent Watermark(00:10) or later: window [00:00, 00:10) closes and emits output.

Coordinator:
  - Receives Ack(E) from window operator (and sink, which has no state).
  - Writes CheckpointMetadata(epoch=E, source_offset=offset_of_Barrier(E)).
  - Marks epoch E committed.
```

**Incorrect behavior this invariant prevents:**

- Window closed early: if the window operator advanced its watermark past the barrier before acknowledging, a downstream Watermark(00:10) signal could close the window with only partial data (missing records that arrived after the barrier but before the watermark reached 00:10 on restore).
- Window held open after restore: if the state snapshot does not include the partial aggregate for [00:00, 00:10), restore from epoch E would start with an empty window, causing records t=00:06 and t=00:08 to be reprocessed and double-counted (at-least-once, not exactly-once).

---

## Fencing Invariant

A stale checkpoint attempt must not be allowed to commit on behalf of epoch E if a newer attempt has taken ownership of epoch E or later.

**Rule:** Each checkpoint attempt for epoch E carries the coordinator's **fencing token** (a monotonic `u64` lease generation). When the coordinator writes `CheckpointMetadata(epoch=E)`, it also writes the fencing token. If a stale coordinator (old lease generation) attempts to write `CheckpointMetadata(epoch=E)`, the metadata store rejects the write because the fencing token is stale.

This invariant is enforced at the metadata store level (conditional write or compare-and-swap on the fencing token). R5.1 simulation does not require durable fencing (no durable writes); R6 must enforce this before any exactly-once claim.

Formally: let `gen(attempt)` be the coordinator lease generation of a checkpoint attempt. The metadata store only accepts a write for epoch E if `gen(attempt) >= gen(current_committed_epoch_writer)`. If `gen(attempt) < gen(current_committed_epoch_writer)`, the write is rejected and the stale coordinator must stop and yield.

---

## Out Of Scope

The following are explicitly out of scope for R5.1 simulation and R6 initial implementation. They are noted here to prevent scope creep during implementation:

- **Unaligned checkpoints:** Buffering in-flight records from faster channels alongside state. Future optimization; not a current design choice.
- **State rescaling:** Changing the partition count of a stateful streaming stage while preserving state. Requires savepoint + explicit repartition. Post-R6 feature.
- **Incremental checkpoints:** Snapshotting only the delta since the last epoch. R6 implements full snapshots; incremental is a follow-on optimization.
- **Two-phase commit for exactly-once sinks:** The two-phase commit sink API is an R6 deliverable but is not required for the R5.1 simulation.
- **Multi-coordinator checkpoint ownership:** R6 has a single active coordinator per job. Per-job HA coordinator failover with checkpoint epoch fencing is R9.
