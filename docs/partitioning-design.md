# Automatic Partitioning — Design & Implementation Plan

## Problem

Users cannot control partition count in any execution mode. DataFusion
`target_partitions` is hardcoded to 1 (`krishiv-sql/src/lib.rs:214`), bounded
window shard count is `executor_count.min(input_row_count)` with no data-size
awareness (`krishiv-scheduler/src/bounded_window.rs:58-59`), shuffle writes
use whatever `num_partitions` the plan fragment carries, and there is no
`repartition()` API, no `SET shuffle.partitions`, and no SQL hint.

In practice this means:
- Single-node queries never parallelize inside DataFusion (one thread for joins,
  aggregations, file scans).
- Bounded windows create at most `executor_count` shards, even for 10 TiB inputs.
- No hot-key handling: a single skewed partition can balloon to 10× the size of
  its peers with no detection or remediation.
- No broadcast join: small tables are always hash-shuffled.

## Goal

Eliminate the class of `spark.sql.shuffle.partitions = 200` knobs by making
partitioning adaptive, data-size-aware, and skew-resistant — while providing
override escape hatches for the cases auto-mode cannot handle.

---

## Two Partition Domains

These are independent and must be treated separately:

### Domain A — DataFusion internal parallelism (`target_partitions`)

Controls how many threads DataFusion uses for local hash-join build,
aggregation spilling, parquet scan concurrency, and round-robin repartitioning.

**Current:** hardcoded `1` in `build_single_node_session_config()`.

**Target:** derived from `available_parallelism()` × a configurable multiplier.
Single-node daemon mode should auto-detect CPU count; embedded mode stays at 1
(local in-process data, never large enough to benefit from parallelisation).

### Domain B — Krishiv shuffle / exchange partitioning (`Partitioning::Hash { buckets }`)

Controls how data is split across stages, executors, and tasks. This is the
domain users mean when they ask about "partitions" in the Spark sense.

**Current:** bucket count is set per-plan-node, either at plan-construction time
or hardcoded in the bounded-window shard calculation.

**Target:** data-size-aware automatic bucket count, plus hot-key splitting,
plus broadcast-auto for small tables.

---

## Phase Breakdown

### Phase 0 — Dynamic `target_partitions` (DataFusion domain)

Files to change:

| File | Change |
|------|--------|
| `krishiv-sql/src/lib.rs:212-218` | Replace hardcoded `with_target_partitions(1)` with a parameter. Add `with_default_parallelism(SessionConfig, usize)` that sets `target_partitions` and enables `round_robin_repartition`. |
| `krishiv-sql/src/lib.rs` (`SqlEngine`) | Add `parallelism: Option<NonZeroUsize>` field. Add builder method `with_parallelism(n)`. When `None`, embedded → 1, single-node → `available_parallelism()`. |
| `krishiv-api/src/session.rs:158-233` | Wire `KRISHIV_TARGET_PARALLELISM` env var. Add `SessionBuilder::with_target_parallelism(n)`. |
| `krishiv-api/src/types.rs` | Add `ExecutionConfig { target_partitions: u32 }` — currently no per-session config exists. |
| `krishiv-executor/src/cli.rs` | Add `--parallelism` CLI flag and `KRISHIV_TARGET_PARALLELISM` env var override. Default: `available_parallelism()`. |

Design decision: Do NOT expose this as a per-query knob. It's a session-level
resource budget. DataFusion already uses it for hash-partition build-side
parallelism, aggregation spill partitions, and scan concurrency. Setting it
per-query creates unpredictable resource contention.

### Phase 1 — Data-size-aware bucket count (shuffle domain)

The core insight: we don't know the exact data size until the upstream stage
completes. But we CAN estimate from:
- Parquet file metadata (row count × avg row width via footer statistics).
- Inline IPC batch sizes (coordinator has the bytes before dispatching).
- Previous stage runtime stats (`memory_bytes` in `RuntimeStats`).

#### 1a. AQE bucket-count rule

Add a new AQE rule `AutoPartitionRule` in `krishiv-plan/src/optimizer.rs`:

```
Input:  PhysicalPlan with Exchange { partitioning: Hash { keys, buckets: _ } }
                 or RoundRobin { buckets: _ }
Output: Same plan but with buckets recomputed from:
        - estimated_input_bytes from upstream stage stats
        - target_bytes_per_partition (configurable, default 128 MiB)
        - max_buckets = executors * partitions_per_executor (configurable)

Formula: buckets = max(1, min(max_buckets, ceil(estimated_bytes / target_bytes)))
```

`estimated_bytes` comes from:
- `RuntimeStats::memory_bytes` on the upstream stage's output (when available).
- Fallback: `estimated_rows * avg_row_width` from the plan schema for the first
  execution (no stats yet; stats arrive after stage completion for re-optimization).

Register in `default_aqe_optimizer()` alongside `CoalesceRule`.

#### 1b. Bounded window shard calculation

Replace the current heuristic in `krishiv-scheduler/src/bounded_window.rs:58-59`:

```rust
// Current:
executor_count.min(input_row_count.max(1))

// New:
let estimated_bytes: u64 = input_batches.iter()
    .map(|b| b.get_array_memory_size() as u64)
    .sum();
let target_per_shard: u64 = 128 * 1024 * 1024;  // 128 MiB
let ideal = (estimated_bytes / target_per_shard).max(1) as usize;
let capped = ideal.min(executor_count.saturating_mul(2)); // at most 2× executors
let shard_limit = capped.max(1);
```

The `executor_count * 2` ceiling ensures the coordinator doesn't create
thousands of tiny shards when executors are scarce. The `target_per_shard`
should come from `DurabilityProfile` or a coordinator config so operators can
tune it without a recompile.

#### 1c. Shuffle write config in plan construction

When the planner creates a `ShuffleWriteConfig` or a `Partitioning::Hash` node,
the bucket count should be left unset (or set to a sentinel) and resolved by
`AutoPartitionRule` at scheduling time — NOT baked in at plan-construction time.

Files to change:

| File | Change |
|------|--------|
| `krishiv-plan/src/optimizer.rs` | Add `AutoPartitionRule` struct implementing `AqeRule`. |
| `krishiv-plan/src/optimizer.rs` (`default_aqe_optimizer`) | Register `AutoPartitionRule`. |
| `krishiv-scheduler/src/bounded_window.rs:58-59` | Replace shard-limit formula with data-size-aware version. |
| `krishiv-scheduler/src/coordinator/job_lifecycle.rs:468-476` | After `submit_physical_plan`, wire `AutoPartitionRule` with actual input sizes from registered table metadata. |
| `krishiv-common/src/partition.rs` | Add `target_bytes_per_partition()` helper (the 128 MiB constant, indexed by durability profile). |

### Phase 2 — Hot-key detection and splitting (skew domain)

The infra already exists:
- `HeavyHittersTracker` (SpaceSaving) in `krishiv-dataflow/src/adaptive.rs:32`
- `HotKeyReport` in `krishiv-dataflow/src/adaptive.rs:7`
- `AdaptiveDecisionKind::HotKeySplit` in `krishiv-scheduler/src/adaptive.rs:12`

But none of it is wired into the execution path.

#### 2a. Executor-side hot-key tracking

During shuffle write (`execute_shuffle_write_fragment` and
`execute_inmem_shuffle_write` in `krishiv-executor/src/fragment/batch.rs`),
run a `HeavyHittersTracker` on the key column while partitioning. If any
single key exceeds a threshold (e.g. 10% of total rows), include the hot key
in the `TaskRuntimeStats` returned in the heartbeat.

Files to change:

| File | Change |
|------|--------|
| `krishiv-executor/src/fragment/batch.rs:329-471` | Add `HeavyHittersTracker` scanning the key column during `partition()`. Collect `HotKeyReport`s. |
| `krishiv-executor/src/fragment/batch.rs:475-575` | Same for `execute_inmem_shuffle_write`. |
| `krishiv-proto/src/task.rs` | Add `hot_key_reports: Vec<HotKeyReport>` to `TaskRuntimeStats` or heartbeat request. |
| `krishiv-dataflow/src/adaptive.rs` | Promote `HotKeyReport` to `krishiv-common` so both executor and proto can reference it (or serialize as proto). |

#### 2b. Coordinator-side hot-key response

In the heartbeat handler (`krishiv-scheduler/src/coordinator/executor_ops.rs`):
- Parse hot-key reports from executor heartbeats.
- When a key exceeds the threshold, log an `AdaptiveDecisionLog { kind: HotKeySplit, applied: false }`
  and prepare to handle it in the next stage.

For the split itself: when `submit_job_with_task_input_partitions` detects hot
keys, split the offending partition into sub-partitions using a salted hash
(`key || partition_index`). This requires the downstream consumer to support
reading multiple sub-partitions and merging them.

**Design decision:** Hot-key splitting is Phase 2 for a reason — it requires
stage retry or dynamic task creation, neither of which is fully built yet.
In Phase 2, implement detection + reporting + logging only. The actual split
execution is Phase 2b or deferred to a follow-up.

### Phase 3 — Auto-broadcast for small tables

The lowering already has this path:
```rust
// krishiv-plan/src/lowering.rs:16-28
if node.broadcast_eligible() {
    if let Some(Exchange { Hash .. } | Exchange { RoundRobin .. }) = node.op() {
        // promote to Broadcast
    }
}
```

But nothing sets `broadcast_eligible = true` automatically.

Add an optimizer rule (logical, not AQE) that estimates input table sizes from
the plan schema and marks tables below a threshold as broadcast-eligible.

Files to change:

| File | Change |
|------|--------|
| `krishiv-plan/src/optimizer.rs` | Add `BroadcastAutoRule` implementing `OptimizerRule`. Scans for `NodeOp::Scan` nodes, estimates row count from plan metadata, sets `broadcast_eligible` if below threshold. |
| `krishiv-plan/src/optimizer.rs` (`default_logical_optimizer`) | Register `BroadcastAutoRule`. |
| `krishiv-plan/src/lib.rs` | Add `estimated_rows` propagation from source metadata (parquet footer, Kafka partition count × msg size, etc.). Currently `estimated_rows` is `Option<u64>` on `PlanNode` but never actually populated from real metadata. |

Threshold: 100 MiB default, configurable via `DurabilityProfile` or optimizer config.

### Phase 4 — Escape hatches (user overrides)

Despite the above, three cases legitimately need user control:

1. **Sink topology constraint** — "I need exactly 16 output files because my
   downstream consumer expects 16 Kafka partitions." Auto-mode can't read minds.
2. **Known skew that auto-detect hasn't seen yet** — "I know `user_id=42` has
   50% of the data; pre-salt the key before shuffle."
3. **Performance tuning** — "I have 64 cores and want 512 partitions even though
   the data is only 1 GiB, because I know the downstream join explodes."

These justify three escape hatches:

| Hatch | Surface | Scope |
|-------|---------|-------|
| `SET shuffle.partitions = N` | SQL session config (new `SET` parser in `krishiv-sql`) | Overrides `AutoPartitionRule` bucket count for the session. |
| `DataFrame::repartition(n, keys)` | Rust DataFrame API (`krishiv-api/src/dataframe.rs`) | Inserts an `Exchange { Partitioning::Hash { buckets: n } }` node into the logical plan. |
| `KRISHIV_SHUFFLE_PARTITIONS` | Env var / CLI flag (executor or coordinator) | Cluster-wide default. Overrides `target_bytes_per_partition` model. |

Files to change (Phase 4):

| File | Change |
|------|--------|
| `krishiv-sql/src/lib.rs` | Parse `SET shuffle.partitions` → store in `SqlEngine` config. Inject as `Partitioning::Hash { buckets: N }` in plan output. |
| `krishiv-api/src/dataframe.rs` | Add `repartition(columns, num_partitions) -> DataFrame` method. Adds `Exchange` node to logical plan. |
| `krishiv-api/src/session.rs` | Add `SessionBuilder::with_shuffle_partitions(n)`. |
| `krishiv-plan/src/lib.rs` (PlanNode) | Add helper `with_exchange(partitioning)` builder. |

Implementation order: Phase 4 is last. The hatches are not needed for the common
case; they exist for the 10% that auto can't handle. Implement them only after
Phases 0-3 are validated.

---

## Summary of All Changes

| Phase | File | What |
|-------|------|------|
| 0 | `krishiv-sql/src/lib.rs:212-218` | Parameterize `target_partitions`, add `with_parallelism()` |
| 0 | `krishiv-sql/src/lib.rs` (SqlEngine) | Add `parallelism` field, builder method |
| 0 | `krishiv-api/src/session.rs` | Add `with_target_parallelism()`, env var |
| 0 | `krishiv-api/src/types.rs` | Add `ExecutionConfig` struct |
| 0 | `krishiv-executor/src/cli.rs` | Add `--parallelism` flag |
| 1a | `krishiv-plan/src/optimizer.rs` | Add `AutoPartitionRule` (AqeRule) |
| 1a | `krishiv-plan/src/optimizer.rs` | Register in `default_aqe_optimizer()` |
| 1b | `krishiv-scheduler/src/bounded_window.rs:58-59` | Data-size-aware shard count |
| 1b | `krishiv-common/src/partition.rs` | `target_bytes_per_partition()` helper |
| 1c | `krishiv-scheduler/src/coordinator/job_lifecycle.rs:473` | Wire `AutoPartitionRule` with actual sizes |
| 2a | `krishiv-executor/src/fragment/batch.rs:329-575` | `HeavyHittersTracker` in shuffle write |
| 2a | `krishiv-proto/src/task.rs` | `hot_key_reports` in heartbeat |
| 2a | `krishiv-dataflow/src/adaptive.rs` | Promote `HotKeyReport` to `krishiv-common` |
| 2b | `krishiv-scheduler/src/coordinator/executor_ops.rs` | Parse hot-key reports, log decision |
| 3 | `krishiv-plan/src/optimizer.rs` | Add `BroadcastAutoRule` |
| 3 | `krishiv-plan/src/optimizer.rs` | Register in `default_logical_optimizer()` |
| 3 | `krishiv-plan/src/lib.rs` | Populate `estimated_rows` from metadata |
| 4 | `krishiv-sql/src/lib.rs` | Parse `SET shuffle.partitions` |
| 4 | `krishiv-api/src/dataframe.rs` | `repartition(n, keys)` method |
| 4 | `krishiv-api/src/session.rs` | `with_shuffle_partitions()` builder |
| 4 | `krishiv-plan/src/lib.rs` | `with_exchange(partitioning)` helper |

---

## Invariant Checks

- `AutoPartitionRule` must NOT fire on streaming plans (same guard as
  `StreamingAqeGuard` for `CoalesceRule`).
- `AutoPartitionRule` must NOT fire on `Broadcast` partitioning (no-op).
- `target_bytes_per_partition` must be ≥ 1 MiB to prevent creating millions of
  partitions for small data.
- Shard count ceiling should never exceed `executor_count * max_partitions_per_executor`
  (configurable, default 4). Without this cap, a 10 TiB input on 3 executors
  would create 80,000+ shards for 128 MiB target — each executor would get
  ~27,000 tasks, overwhelming the scheduler.
- When `SET shuffle.partitions` or `DataFrame::repartition()` is used,
  `AutoPartitionRule` must skip that plan node (explicit override).

---

## Validation Strategy

| Phase | Validation |
|-------|------------|
| 0 | `target_partitions` = N produces N-way hash join in DataFusion EXPLAIN output. |
| 1a | 1 GiB input with 128 MiB target → 8 buckets. 100 MiB input → 1 bucket. |
| 1b | Bounded window with 500 MiB input, 3 executors → 4 shards (not 3). |
| 2a | Input with 20% single key → `HotKeyReport` in heartbeat stats. |
| 2b | Coordinator logs `HotKeySplit` decision when hot key reported. |
| 3 | 50 MiB table → `broadcast_eligible = true` in lowered plan. |
| 4 | `df.repartition(16, "key")` → plan contains `Exchange { Hash, buckets: 16 }`. |
