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

## 6. R5.1 Streaming Re-Attach Protocol

### 6.1 Problem

When the coordinator process restarts while streaming tasks are running on executors, two outcomes are possible:

1. **Stale re-submit (wrong):** The coordinator treats the job as lost and submits it again from scratch, causing duplicate processing and inconsistent output.
2. **Re-attach (correct):** The coordinator recovers the job record from durable state and waits for running executors to re-register. No events are re-processed from before the last committed offset.

R5.1 implements the re-attach path. Durable checkpoints (which enable crash recovery with guaranteed at-least-once from a known offset) are R6.

### 6.2 Re-Attach Sequence

```
1. Coordinator restarts. recover_from_store() loads job records into memory.
   - Grace period starts: streaming executors are not evicted for
     streaming_reattach_grace_ticks heartbeat periods.

2. Executor detects coordinator unavailability (gRPC call fails).
   - Executor continues running the operator loop; does not stop.

3. Executor re-registers with the coordinator.
   - Registration uses the same executor ID and reports the same task IDs
     that were running before the coordinator restart.

4. Executor sends a first heartbeat carrying StreamingTaskState:
   - task_id: the task that was running
   - watermark_ms: current event-time watermark
   - source_offset: last committed Kafka offset (connector-specific encoding)

5. Coordinator applies the streaming task states to the in-memory task records:
   - Updates task.last_watermark_ms and task.last_source_offset.
   - Does NOT submit a new job; does NOT reset task state.

6. Job remains in Running state. Coordinator resumes normal operation.
```

### 6.3 Streaming Task State In Heartbeats

`ExecutorHeartbeatRequest.streaming_task_states` carries per-task state for the re-attach protocol. Executors populate this on their first heartbeat after a coordinator restart. Subsequent heartbeats may also carry it for live watermark tracking.

The coordinator calls `Coordinator::executor_heartbeat` which:
1. Updates the executor registry (lease validation, state, memory metrics).
2. For each `StreamingTaskState` in the heartbeat, calls `apply_streaming_task_state` to update the matching `TaskRecord`.

### 6.4 Re-Attach Limitations In R5.1

- **No exactly-once guarantee.** On coordinator restart, the executor continues processing from wherever it is. Events between the last committed offset and the coordinator restart may be re-processed if the executor also restarts.
- **No durable offset storage.** `last_source_offset` in `TaskRecord` is in-memory. If both coordinator and executor restart simultaneously, the source offset is lost and processing resumes from the beginning or last Kafka consumer group commit.
- **No multi-coordinator failover.** R5.1 is single-coordinator. HA coordinator failover is R9.

Exactly-once with durable checkpoints is R6.

---

## 7. Clock Skew And Late Event Policy

### 7.1 Policy

Krishiv trusts the `event_time` field in source records as-is. The system does not adjust event timestamps for producer clock skew.

A late event is an event with `event_time_ms < current_watermark_ms` (the watermark established by the **previous** batch of events — not the watermark the current event itself would advance). Late events are dropped without error in R5.1.

### 7.2 `prev_watermark_ms` Semantics

In `TumblingWindowOperator`:

- `prev_watermark_ms` tracks the watermark value **from the end of the previous `process_batch` call**.
- When processing a new batch, events are considered late if `event_time_ms < prev_watermark_ms`.
- This means events in the **current** batch are never considered late relative to the watermark they themselves advance — only relative to the watermark established by prior batches.
- After accumulating all non-late events, `prev_watermark_ms` is updated to `new_watermark_ms`.

This matches Apache Flink's behavior: a record advancing the watermark to T is not late relative to T.

### 7.3 `allowed_lateness` As Clock Skew Tolerance

The `WatermarkSpec::fixed_lag_ms` parameter is the primary mechanism for tolerating moderate producer clock skew:

- Watermark = `max(event_time_seen) - lag_ms`
- A `lag_ms` of 5000 means events arriving up to 5 seconds behind the maximum observed event time are still considered on-time.
- Clock skew larger than `lag_ms` will cause late events to be dropped.

Operators are responsible for choosing an appropriate `lag_ms` for their source's expected clock skew distribution.

---

## 8. Key Invariants

| Invariant | Enforcement |
|---|---|
| Streaming plans carry `ExecutionKind::Streaming` | Set on `PlanNode` in `krishiv-plan`; validated by coordinator at submit |
| Watermark monotonicity | Enforced per-operator; decreasing watermark is a bug |
| No `Succeeded` state for streaming jobs | Hard invariant in `Coordinator::apply_task_update` |
| Exactly-once requires certified source + sink + checkpoint | Verified at R6; see [R6 implementation doc](../implementation/r6-checkpoints-and-savepoints.md) |
| Partition never partially served | Shuffle write uses staging → atomic rename; see [Shuffle Deployment Model](./shuffle-deployment-model.md) |

---

## 9. Out Of Scope For This Document

| Topic | Reference |
|---|---|
| Checkpoint barrier protocol | `docs/architecture/checkpoint-protocol.md` (R6 deliverable) |
| Exactly-once certification rules | `docs/implementation/r6-checkpoints-and-savepoints.md` |
| HA coordinator failover with fencing | `docs/implementation/r9-governance-and-operations.md` |
| Streaming Python UDFs | Post-GA design document |
| State rescaling on topology change | Post-R6 design document |
