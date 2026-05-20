# R7.2 Backpressure Protocol

## Scope Boundary Decision

R7.2 backpressure operates **intra-stage only** (within one executor's operator chain).  
Cross-stage throttling uses the coarser `ThrottleCommand` control-plane signal.  
Full end-to-end credit propagation across shuffle boundaries is deferred to R9.

## Backpressure-Barrier Interaction Rule (Risk 3 Mitigation)

The checkpoint barrier protocol (R6) and backpressure (R7.2) could deadlock if
a downstream operator blocks on credit while an upstream operator is waiting for
the barrier to drain.

**Rule:** Barriers travel on a **priority channel that bypasses credit-gating**.
An `OperatorQueue` message is either `Data(RecordBatch)` or `Barrier(epoch)`.
`Barrier` messages are never subject to backpressure blocking — they are always
accepted and processed ahead of queued `Data` messages.  The receiver checks
`Barrier` items after each `Data` item is consumed, so barriers cannot be
indefinitely delayed by a full data queue.

Implementation: `OperatorQueue<T>` is split into:
- `data_tx / data_rx`: bounded `tokio::sync::mpsc` channel — subject to backpressure
- `barrier_tx / barrier_rx`: unbounded `tokio::sync::mpsc` channel — never blocked

The operator loop drains `barrier_rx` after each `data_rx` receive.

## Credit-Based Flow Control Model

Credit token = permission to send one `RecordBatch` downstream.

```
Source ──[credits]──► Operator A ──[credits]──► Operator B ──► Sink
       ◄──[data]────              ◄──[data]────
```

- Each operator maintains a credit budget initialized to `queue_capacity`.
- Downstream sends `grant(n)` when it consumes n items from its input queue.
- Upstream blocks on `credit_rx.recv()` when budget == 0.
- `grant()` messages arrive on an unbounded channel (never blocked).

For R7.2 the credit channel is implicit — Tokio's bounded `mpsc` provides
natural credit through channel capacity. The credit budget is the channel
capacity. Explicit credit tokens are introduced in R9 for cross-stage flow.

## Source Throttling

`ThrottledSource` wraps any `Source` and adds a `rows_per_second` rate limit.
The coordinator sends `ThrottleCommand { job_id, source_id, rows_per_sec }` via
the heartbeat response or a dedicated RPC. The source checks the rate limit
before reading the next batch.

Rate limiting uses a token bucket with 1-second windows. The bucket is
replenished at `rows_per_second` tokens per second. If the bucket is empty,
the source waits (via `tokio::time::sleep`) for the next refill window before
reading.

## Hot-Key Detection Model (Risk 4 Mitigation)

**Plain HashMap is forbidden** for frequency tracking. High-cardinality key
spaces would grow unboundedly in memory.

R7.2 uses the **SpaceSaving** top-K algorithm (Metwally et al. 2005):
- Fixed-size structure of at most K counters (default K=100).
- Each counter tracks `(key, estimated_count, max_error)`.
- When a new key arrives and the structure is full, replace the minimum-count
  entry; the new entry's count = min_count + 1 (overestimate).
- Guarantees: any key appearing more than `1/K` fraction of the time is
  tracked. False positives are bounded by the max_error field.

Reports: `HotKeyReport { key, estimated_count, heat_score: count / total }`.
Heat scores above `heat_threshold` (default 0.05 = 5% of traffic) are reported
in the executor heartbeat as `hot_keys: Vec<HotKeyReport>`.

## Adaptive Repartitioning Scope (Decision 5)

- **Batch jobs only**: the coordinator may increase the next downstream stage's
  partition count based on skew signals from `TaskRuntimeStats`. This is safe
  because the downstream stage has not been launched yet when the upstream
  stage completes.
- **Streaming jobs**: adaptive repartitioning requires a savepoint → restart
  (same as the R6 rescaling model). Attempting live streaming repartition is
  rejected by the coordinator. Streaming hot-key splitting in R7.2 is
  **stateless** (sub-partition key suffix) and follows the savepoint path.
- **Never mid-stage**: partition count is fixed for a stage once tasks are
  assigned. Repartitioning only affects the next unstarted stage.

## Manual Override

`AdaptiveOverrideConfig` in `CoordinatorConfig` can disable any adaptive
behavior globally or per-job:
- `disable_hot_key_splitting: bool`
- `disable_adaptive_repartition: bool`
- `disable_source_throttling: bool`

When an adaptive decision is suppressed by override, it is logged in
`AdaptiveDecisionLog` with `applied: false` and `reason: "manual override"`.

## Explainable Decisions

Every adaptive action produces one `AdaptiveDecisionLog` entry:
```
AdaptiveDecisionLog {
    timestamp_ms: u64,
    kind: AdaptiveDecisionKind,   // HotKeySplit | Repartition | Throttle | SlowSink
    affected_job_id: JobId,
    details: String,              // human-readable explanation
    applied: bool,                // false if suppressed by override
}
```
Accessible via `Coordinator::adaptive_decision_log(&job_id)`.
