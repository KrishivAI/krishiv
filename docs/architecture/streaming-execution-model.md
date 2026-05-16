# Krishiv Streaming Execution Model

**Status:** Draft — must be reviewed and approved before R5.1 implementation begins.
**Owner:** Architecture team.
**Linked releases:** R4 (document deliverable), R5 (implementation spec), R6 (checkpoint extension).

---

## Purpose

This document defines the streaming execution model that all R5–R6 implementation must conform to. Writing code before this document is approved is not permitted for streaming work. Every R5 design decision should trace back to a section here.

---

## 1. Continuous Operator Model

### 1.1 Batch vs Streaming Stages

A **batch stage** is a stage that:
- Receives a bounded set of input `RecordBatch` values.
- Produces a bounded set of output `RecordBatch` values.
- Transitions to `Succeeded` or `Failed` when input is exhausted.

A **streaming stage** is a stage that:
- Receives an unbounded stream of input `RecordBatch` values, each tagged with event time.
- Produces output `RecordBatch` values continuously as watermarks advance.
- **Never transitions to `Succeeded` while running.** It transitions to `Failed` on error or `Stopped` on deliberate cancellation only.

### 1.2 Executor Operator Loop

A streaming operator on an executor runs the following loop:

```
loop:
  batch = receive_next_input_batch()     // blocks until available or source ends
  advance_watermark(batch.max_event_time)
  process_batch(batch)                   // may emit zero or more output batches
  flush_triggered_windows()             // emit windows whose watermark has passed
  send_output_batches(output)
  update_heartbeat()
```

This loop runs on a dedicated Tokio task per streaming operator instance. It must not block the Tokio worker thread pool for longer than one batch processing cycle.

### 1.3 Operator State Contract

Operators that read or write keyed state must:
- Access state only within `process_batch` or `flush_triggered_windows`.
- Never access state from a timer callback on a different Tokio task.
- Treat state as logically owned by the operator instance for its assigned key range.

---

## 2. Watermark Propagation Protocol

### 2.1 Definition

A **watermark** at time `T` for a source partition is a declaration that no future event with event time `< T` will arrive on that partition. Late events (with event time `< current_watermark`) are dropped by default in R5. Late data handling is deferred to post-R6.

### 2.2 Single-Source Watermark (R5.1)

For a single Kafka partition source:
- The watermark advances to `max(event_time_seen) - allowed_lateness`.
- `allowed_lateness` is a configurable fixed lag (e.g., 5 seconds).
- The watermark is emitted as a special control record after every input batch.
- Downstream operators advance their watermark when they receive this control record.

### 2.3 Multi-Source Watermark (R5.2)

When a streaming stage has multiple input sources:
- Each source maintains its own watermark independently.
- The stage's effective watermark is `min(watermark_source_1, watermark_source_2, ...)`.
- A window is closed only when the effective watermark passes the window's end time.
- If one source stalls (no new data), the effective watermark does not advance, and windows do not close. This is correct behavior. Idle source detection is a future enhancement.

### 2.4 Watermark and Checkpoint Barriers

In R6, checkpoint barriers will be injected into the data stream between watermarks. The protocol for aligning barriers with watermarks will be defined in `docs/architecture/checkpoint-protocol.md` (R6 deliverable). R5 implementations must not assume watermark and barrier alignment — leave explicit extension points.

---

## 3. State Interaction Model

### 3.1 Keyed State Access

State is partitioned by key. An operator instance owns the state for exactly the keys in its assigned key range. Operators:
- Read state before or during `process_batch`.
- Write state after computing the new value.
- Clear state when TTL expires (R5.2) or when a window closes and state is no longer needed.

### 3.2 State and Checkpoint Barriers (R6 Extension Point)

In R6, before a checkpoint barrier is processed, the operator must:
1. Finish processing the current batch.
2. Flush all pending window outputs.
3. Take a state snapshot.
4. Acknowledge the barrier to the checkpoint coordinator.

R5 operators must expose a `snapshot(&self) -> StateSnapshot` method even if checkpointing is not yet wired. This prevents a redesign in R6.

### 3.3 State Backend Interface

```rust
// Minimum interface that both in-memory (R5.1) and RocksDB (R5.2) backends must implement.
pub trait StateBackend: Send + Sync {
    fn get(&self, namespace: &str, key: &[u8]) -> StateResult<Option<Bytes>>;
    fn put(&mut self, namespace: &str, key: &[u8], value: Bytes) -> StateResult<()>;
    fn delete(&mut self, namespace: &str, key: &[u8]) -> StateResult<()>;
    fn snapshot(&self) -> StateResult<StateSnapshot>;  // extension point for R6
}
```

---

## 4. Streaming Job Lifecycle In The Coordinator

### 4.1 States

| State | Meaning |
|---|---|
| `Pending` | Job accepted, executors not yet assigned |
| `Running` | All streaming stages are active on executors |
| `Degraded` | One or more stages have failed and are being restarted |
| `Stopped` | Job was deliberately cancelled via CLI or API |
| `Failed` | Job failed and cannot be restarted without operator intervention |

A streaming job **never enters `Succeeded`**. This is a hard invariant enforced by `Coordinator::apply_task_update`.

### 4.2 Stage Restart On Failure

When an executor crashes running a streaming stage:
1. Coordinator detects the heartbeat timeout.
2. Coordinator marks affected tasks as `Failed`.
3. Coordinator transitions the job to `Degraded`.
4. Coordinator re-submits the failed tasks to available executors.
5. Executors restart from the last committed source offset (at-least-once in R5; exactly-once in R6 with checkpoints).
6. When all tasks are `Running` again, job transitions back to `Running`.

### 4.3 Coordinator Status API

The `/api/v1/jobs/{id}` response for a streaming job must include:
- Current job state (`Running`, `Degraded`, `Stopped`, `Failed`).
- Per-stage task state.
- Current watermark per source partition.
- Last committed source offset per partition (added in R6).

---

## 5. Deterministic Replay Contract

For testing correctness in R5.1, every streaming pipeline must be replayable:
- Given the same ordered sequence of input `RecordBatch` values with the same event times.
- The pipeline must produce the same ordered sequence of output `RecordBatch` values.
- This holds for the in-memory state backend. RocksDB adds durability but must not change output.

The deterministic replay harness in `krishiv-state` must be usable by any streaming operator test.

---

## 6. What This Document Does Not Define

The following are out of scope for this document and will be addressed in the referenced documents:

| Topic | Document |
|---|---|
| Checkpoint barrier protocol | `docs/architecture/checkpoint-protocol.md` (R6) |
| Exactly-once certification rules | `docs/implementation/r6-checkpoints-and-savepoints.md` |
| HA coordinator failover with fencing | `docs/implementation/r9-governance-and-operations.md` |
| Streaming Python UDFs via subprocess | Post-GA design document |
| State rescaling on topology change | Post-R6 design document |
