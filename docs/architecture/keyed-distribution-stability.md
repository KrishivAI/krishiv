# Krishiv Keyed-Distribution Stability Contract

**Status:** Decision — approved for R5.1 enforcement.
**Owner:** Architecture team.
**Linked releases:** R5 (streaming keyed distribution enforcement), R6 (state rescaling precondition), R7 (AQE adaptivity must respect streaming guard).

---

## Decision

`key_by(column)` in a streaming job guarantees **same-key → same executor task for the job lifetime**. The partition count for a stateful streaming stage is fixed at job submit time and must never be changed by the optimizer or runtime while the job is running.

---

## Batch Partitioning Vs Streaming Keyed Distribution

### Batch Partitioning

In batch jobs, hash partitioning assigns rows to buckets by hashing a column and taking `hash % partition_count`. AQE (Adaptive Query Execution) can coalesce partitions mid-job when runtime statistics show that many partitions are small. This is safe in batch because:

- Each stage terminates. When AQE coalesces partitions for stage N+1, stage N has already completed and its output is fixed on disk.
- No mutable per-key state persists between stage boundaries. The hash routing decision affects only where data lands for a single stage; there is no memory of previous routing decisions.
- The new partition count for stage N+1 applies uniformly to all tasks in that stage.

AQE coalescing is a key performance feature for batch SQL. It is correct and desirable for batch stages.

### Streaming Keyed Distribution

In stateful streaming jobs, `key_by(column)` routes each key to a specific executor task instance. That task instance owns the mutable state for all keys assigned to it. The routing decision is not per-batch or per-stage — it is **permanent for the lifetime of the job**.

If AQE were allowed to coalesce or repartition a streaming stage:

- The partition count would change while the job is running.
- Keys that were previously routed to task instance T would now be routed to a different task instance T'.
- T' has no state for those keys — the state is orphaned in T.
- T still holds state for keys it no longer receives, causing both memory leaks and incorrect aggregations.

This is a **data correctness bug**, not a performance issue. There is no safe way to repartition a stateful streaming stage at runtime without a savepoint.

---

## Hard Rule

**AQE coalescing and repartitioning rules in `krishiv-optimizer` MUST NOT apply to streaming stages.**

A streaming stage is any stage whose plan nodes carry `ExecutionKind::Streaming`. The presence of any streaming node in a stage makes the entire stage AQE-exempt for coalescing and repartitioning rules.

This rule applies for the full job lifetime. It cannot be overridden by job configuration, admission control, or resource pressure.

---

## Enforcement: StreamingAqeGuard

The `Optimizer` pipeline in `krishiv-optimizer` enforces this rule via a dedicated guard rule: `StreamingAqeGuard`.

`StreamingAqeGuard` is added to the optimizer pipeline **before all AQE rules**. It inspects the physical plan and marks any stage containing a `ExecutionKind::Streaming` node as **AQE-exempt**. AQE rules that follow in the pipeline check the exempt flag and skip coalescing and repartitioning rewrites for AQE-exempt stages.

Pipeline order (relevant excerpt):

```text
Optimizer pipeline:
  1. CBO cost rules (apply to all stages)
  2. Stream planner rules (assign ExecutionKind to plan nodes)
  3. StreamingAqeGuard          ← marks streaming stages as AQE-exempt
  4. AQE coalescing rule        ← skips AQE-exempt stages
  5. AQE repartitioning rule    ← skips AQE-exempt stages
  6. Skew detection rule        ← may still apply to batch stages
  7. Small-file planning rule   ← applies to batch stages only
```

`StreamingAqeGuard` does not touch batch stages. Batch stages continue to benefit from AQE coalescing and repartitioning.

---

## Key Contract

The `key_by(column)` streaming API provides the following guarantee:

> For a stateful streaming stage, the same key will route to the same executor task instance for the entire job lifetime.

This guarantee is backed by:

1. **Fixed hash partition function:** `key_by` uses `hash(key, fixed_seed) % partition_count`. The seed and partition count are fixed at job submit time and stored in `MetadataStore` as part of the job plan.
2. **Fixed partition count:** The partition count for a stateful streaming stage is determined at job submit time and stored in the job plan. It is never modified by AQE, resource manager, or any runtime component.
3. **Stable task-to-partition assignment:** Each task instance is assigned a fixed set of partition buckets at job submit time. The assignment is stored in `MetadataStore`. On executor restart or task reassignment, the new task instance receives the same partition bucket assignment and restores state for those buckets from the last checkpoint.

---

## State Rescaling

The only supported way to change the partition count of a stateful streaming stage is:

1. Trigger a savepoint for the running job.
2. Stop the job.
3. Submit a new job from the savepoint with a different partition count, using the state rescaling restore path.

This is a post-R6 feature. State rescaling is not supported in R5 or R6. Any attempt to change the partition count of a running stateful streaming stage is an error.

---

## Out Of Scope

- **State rescaling at runtime:** Not supported until post-R6. The savepoint + restore path is the only supported rescaling mechanism.
- **AQE for streaming batch-hybrid stages:** If a plan contains both batch and streaming nodes in the same stage, the entire stage is treated as streaming (AQE-exempt). Mixing batch and streaming in a single stage is not a supported pattern.
- **Hot-key splitting for streaming stages:** Hot-key detection and splitting (R7.2) applies to batch stages. For stateful streaming stages, hot-key mitigation requires a savepoint + restore with explicit key range repartitioning, which is post-R6.
