# Krishiv Streaming Execution Model

**Status:** Decision — approved for R5.1 implementation.
**Owner:** Architecture team.
**Linked releases:** R4 (document deliverable), R5 (implementation spec), R6 (checkpoint extension).

Related documents:
- [Krishiv Roadmap](./krishiv-roadmap.md)
- [Stage-Local Execution](./stage-local-execution.md)
- [Shuffle Deployment Model](./shuffle-deployment-model.md)

---

## 1. Continuous Operator Model

Krishiv uses a single unified DAG execution model for both batch and streaming work.  
The difference is not the graph shape — it is the execution kind attached to each node.

### 1.1 Batch vs Streaming Execution Kinds

A **batch** node (`ExecutionKind::Batch`) in `krishiv-plan`:
- Receives a bounded set of `RecordBatch` values.
- Produces a bounded output set.
- Transitions to `Succeeded` or `Failed` when input is exhausted.

A **streaming** node (`ExecutionKind::Streaming`) in `krishiv-plan`:
- Receives an unbounded stream of `RecordBatch` values, each tagged with event time.
- Produces output continuously as watermarks advance.
- **Never transitions to `Succeeded` while running.** It transitions to `Failed` on error or `Stopped` on deliberate cancellation only.

Both execution kinds share the same physical executor loop. Streaming nodes add:
- Watermark tracking and propagation.
- Stateful operator state (keyed, window-scoped).
- Checkpoint barrier injection (R6).

### 1.2 Executor Operator Loop

A streaming operator on an executor runs the following loop on a dedicated Tokio task:

```
loop:
  batch = receive_next_input_batch()         // blocks until available or source ends
  advance_watermark(batch.max_event_time)
  process_batch(batch)                       // may emit zero or more output batches
  flush_triggered_windows()                 // emit windows whose watermark has passed
  send_output_batches(output)
  update_heartbeat()
```

The operator must not block the Tokio worker thread pool for longer than one batch processing cycle. Long-running CPU work must be dispatched with `tokio::task::spawn_blocking`.

### 1.3 Operator State Contract

Operators that read or write keyed state must:
- Access state only within `process_batch` or `flush_triggered_windows`.
- Never access state from a timer callback on a different Tokio task.
- Treat state as logically owned by the operator instance for its assigned key range.
- Expose a `snapshot(&self) -> StateSnapshot` method as an R6 extension point, even if checkpointing is not yet wired.

---

## 2. Watermark Propagation Protocol

### 2.1 Definition

A **watermark** at time `T` for a source partition is a declaration that no future event with event time `< T` will arrive on that partition.

**Watermark monotonicity is enforced per-operator:** an operator must never decrease its watermark. This is a hard invariant — any logic that would do so is a bug.

Late events (event time `< current_watermark`) are dropped by default in R5. Late data handling is a post-R6 enhancement.

### 2.2 Single-Source Watermark (R5.1)

For a single Kafka partition source:
- Watermark advances to `max(event_time_seen) - allowed_lateness`.
- `allowed_lateness` is a configurable fixed lag (e.g., 5 seconds).
- The watermark is emitted as a special control record after every input batch.
- Downstream operators advance their watermark when they receive this control record.

### 2.3 Multi-Source Watermark (R5.2)

When a streaming stage has multiple input sources:
- Each source maintains its own watermark independently.
- The stage's effective watermark is `min(watermark_source_1, watermark_source_2, ...)`.
- A window is closed only when the effective watermark passes the window's end time.
- If one source stalls, the effective watermark does not advance and windows do not close.  
  This is correct behavior; idle source detection is a future enhancement.

### 2.4 Watermark and Checkpoint Barriers

In R6, checkpoint barriers will be injected into the data stream between watermarks. The barrier-watermark alignment protocol will be defined in `docs/architecture/checkpoint-protocol.md` (R6 deliverable). R5 implementations must leave explicit extension points and must not assume alignment.

---

## 3. State Interaction Model

### 3.1 Keyed State Access

State is partitioned by key. An operator instance owns exactly the keys in its assigned key range. Operators:
- Read state before or during `process_batch`.
- Write state after computing the new value.
- Clear state when TTL expires (R5.2) or when a window closes and state is no longer needed.

### 3.2 State and Checkpoint Barriers (R6 Extension Point)

Before processing a checkpoint barrier in R6, the operator must:
1. Finish processing the current batch.
2. Flush all pending window outputs.
3. Take a state snapshot via `snapshot(&self) -> StateSnapshot`.
4. Acknowledge the barrier to the checkpoint coordinator.

For the full protocol, defer to [R6 checkpoints and savepoints](../implementation/r6-checkpoints-and-savepoints.md).

### 3.3 State Backend Interface

The minimum interface that both in-memory (R5.1) and RocksDB (R5.2) backends must implement:

```rust
pub trait StateBackend: Send + Sync {
    fn get(&self, namespace: &str, key: &[u8]) -> StateResult<Option<Bytes>>;
    fn put(&mut self, namespace: &str, key: &[u8], value: Bytes) -> StateResult<()>;
    fn delete(&mut self, namespace: &str, key: &[u8]) -> StateResult<()>;
    fn snapshot(&self) -> StateResult<StateSnapshot>;   // R6 extension point
}
```

---

## 4. Streaming Job Lifecycle

### 4.1 Submit → Plan → Execute → Checkpoint Epochs → Cancel/Drain

```
Client submits JobSpec (ExecutionKind::Streaming)
  └─ Coordinator accepts, transitions job to Pending
      └─ Coordinator assigns streaming tasks to executors → Running
          └─ Executors run continuous operator loops
              └─ Checkpoint coordinator injects barriers (R6) → epoch committed
                  └─ Cancel/drain: coordinator sends Stop signal
                      └─ Operators flush windows, drain in-flight, ACK Stop
                          └─ Job transitions to Stopped
```

### 4.2 Job State Machine

| State | Meaning |
|---|---|
| `Pending` | Job accepted; executors not yet assigned |
| `Running` | All streaming stages active on executors |
| `Degraded` | One or more stages failed and are being restarted |
| `Stopped` | Job cancelled deliberately via CLI or API |
| `Failed` | Job failed; cannot restart without operator intervention |

A streaming job **never enters `Succeeded`**. This is a hard invariant enforced by `Coordinator::apply_task_update`.

### 4.3 Stage Restart On Failure

When an executor crashes:
1. Coordinator detects heartbeat timeout.
2. Coordinator marks affected tasks as `Failed` and the job as `Degraded`.
3. Coordinator re-submits failed tasks to available executors.
4. Executors restart from the last committed source offset.
   - At-least-once in R5; exactly-once in R6 with checkpoints.
5. When all tasks are `Running`, the job transitions back to `Running`.

---

## 5. Shared Batch/Stream Runtime

Both batch and streaming pipelines share the same `krishiv-exec` DAG execution engine. The engine does not distinguish between modes at the operator dispatch level — it simply iterates over `PlanNode` values and dispatches to the appropriate operator.

What changes in streaming mode:
- **Watermarks** — attached to each batch and propagated through the DAG.
- **Stateful operators** — keyed state, windows, TTL cleanup.
- **Unbounded sources** — never signal EOF; inject idle markers to unblock watermarks.
- **Checkpoint barriers** — injected by the coordinator into the data stream (R6).

What stays the same:
- Arrow `RecordBatch` as the universal unit of data.
- The `ExecutionKind` field on `PlanNode` from `krishiv-plan` selects the execution path.
- The shuffle store (`krishiv-shuffle`) is shared by both modes.
- The `krishiv-optimizer` rule pipeline applies to both logical and physical plans.

---

## 6. Key Invariants

| Invariant | Enforcement |
|---|---|
| Streaming plans carry `ExecutionKind::Streaming` | Set on `PlanNode` in `krishiv-plan`; validated by coordinator at submit |
| Watermark monotonicity | Enforced per-operator; decreasing watermark is a bug |
| No `Succeeded` state for streaming jobs | Hard invariant in `Coordinator::apply_task_update` |
| Exactly-once requires certified source + sink + checkpoint | Verified at R6; see [R6 implementation doc](../implementation/r6-checkpoints-and-savepoints.md) |
| Partition never partially served | Shuffle write uses staging → atomic rename; see [Shuffle Deployment Model](./shuffle-deployment-model.md) |

---

## 7. Out Of Scope For This Document

| Topic | Reference |
|---|---|
| Checkpoint barrier protocol | `docs/architecture/checkpoint-protocol.md` (R6 deliverable) |
| Exactly-once certification rules | `docs/implementation/r6-checkpoints-and-savepoints.md` |
| HA coordinator failover with fencing | `docs/implementation/r9-governance-and-operations.md` |
| Streaming Python UDFs | Post-GA design document |
| State rescaling on topology change | Post-R6 design document |
