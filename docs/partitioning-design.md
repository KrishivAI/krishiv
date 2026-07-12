# Automatic Partitioning — Design & Status

## The Zero-Configuration Guarantee

Krishiv targets the single biggest usability pain point in distributed compute:
**users should never write `spark.sql.shuffle.partitions = 200`**.

From any surface — Python, Rust DataFrame API, SQL — and across every execution
mode — batch, streaming, and incremental view maintenance (IVM) — partitioning is
automatic, data-size driven, skew-resistant, and adaptive. The system self-tunes
from a 100 KB toy query to a 10 TiB production shuffle without any user knob. One
sizing function (`recommend_buckets`) and one keyed hash (SHA-256 key groups)
serve all three modes.

```python
# Python — no partition config needed
session = krishiv.session()
df = session.read_parquet("s3://bucket/events/")
result = df.group_by("user_id").agg(count("*").alias("events"))
result.show()
# Partition count chosen automatically from data size and cluster topology.
```

```sql
-- SQL — no SET statements needed
SELECT user_id, COUNT(*) AS events
FROM events
GROUP BY user_id;
-- AutoPartitionRule fires at AQE time. Hot user_id values trigger
-- skew mitigation automatically.
```

```rust
// Rust — no builder flags needed for the common case
let df = session.read_parquet("s3://bucket/events/").await?;
let result = df.group_by(&["user_id"])?.agg(&[count("*").alias("events")])?;
result.collect().await?;
```

---

## Two Partition Domains

These are independent systems operating at different layers:

### Domain A — DataFusion internal parallelism (`target_partitions`)

Controls thread-level parallelism inside a single executor: hash-join build
side, aggregation spill partitions, Parquet scan concurrency.

**Status: Implemented.**

| Surface | Behaviour |
|---------|-----------|
| Embedded (in-process) | `1` — local data, no benefit from fan-out |
| Single-node daemon / distributed | `available_parallelism()` auto-detected |
| `KRISHIV_TARGET_PARALLELISM=N` env var | Explicit override |
| `SessionBuilder::with_target_parallelism(n)` | Per-session Rust override |

Files: `krishiv-sql/src/lib.rs` (`build_single_node_session_config`),
`krishiv-api/src/session.rs` (`KRISHIV_TARGET_PARALLELISM`).

### Domain B — Shuffle / exchange partitioning (bucket count)

Controls how data is split across stages and executors. This is what users mean
by "partitions" in the Spark sense.

**Status: Fully automatic.** See phases below.

### Domain C — Iceberg storage partitioning (`PARTITIONED BY`)

Controls how table data is laid out in the warehouse — the only domain that is
deliberately **user-declared**, because it encodes query patterns the engine
cannot infer (e.g. "dashboards always filter on `day(event_time)`").

```sql
CREATE TABLE cat.ns.events
PARTITIONED BY (region, day(event_time), bucket(16, user_id))
AS SELECT … ;
```

**Status: Implemented (Phase 52, #191).** Transforms follow the Iceberg spec —
`identity`, `bucket(n, col)`, `truncate(w, col)`, `year|month|day|hour(col)` —
and reuse iceberg-rust's own transform functions, so partition values are
spec-exact (murmur3 bucketing included). The CTAS landing path fans the result
stream out per partition value with a global memory bound (largest buffer
flushes first); every rewrite path (DML copy-on-write, compaction, replace)
preserves the table's partition spec. Compaction (`CALL
system.compact_data_files` or the schedulable `CALL system.maintain_table`)
bin-packs small files within each partition under a snapshot-conflict check.

Files: `krishiv-connectors/src/lakehouse/partitioned_write.rs` (transforms +
fanout), `dml.rs` (CTAS landing, spec-preserving overwrite),
`maintenance.rs` (partition-aware compaction), `krishiv-sql/src/lib.rs`
(`PARTITIONED BY` extraction + routing).

---

## Architecture: How Auto-Partitioning Works End-to-End

```
User query (Python / SQL / Rust)
        │
        ▼
 Logical Optimizer
   BroadcastAutoRule ─── marks small tables broadcast-eligible
        │                (threshold: 1M rows, no shuffle at all)
        ▼
 Physical Planner → Exchange nodes with unresolved bucket count
        │
        ▼
 AQE Optimizer (fires after each stage completes)
   AutoPartitionRule ── sets bucket count from upstream stage byte stats
   CoalesceRule ──────── merges over-sharded output back down
   StreamingAqeGuard ── skips both rules for streaming plans
        │
        ▼
 Coordinator (launch time)
   skew_repartition_overrides ── hot-key override replaces num_partitions
        │                         in ShuffleWriteConfig before dispatch
        ▼
 Executor (shuffle write)
   HashPartitioner (XxHash64) ── routes rows to buckets
   HeavyHittersTracker ────────── scans key column, reports hot keys
   StreamingPartitionAdvisor ──── EMA-based adaptive buckets (streaming)
        │
        ▼
 Coordinator (heartbeat)
   process_hot_key_reports ── heat_score ≥ 0.3 → update skew_repartition_overrides
        │                       for next stage launch
        ▼
 Coalesce (post-shuffle)
   coalesce_partition_batches ── byte-size bin-packing (not count-based)
```

---

## Implementation Status

### Phase 0 — Dynamic `target_partitions` (DataFusion domain)

**Status: Complete.**

`build_single_node_session_config(target_partitions: NonZeroUsize)` in
`krishiv-sql/src/lib.rs:212` accepts the value instead of hardcoding `1`.
`KRISHIV_TARGET_PARALLELISM` env var parsed in `krishiv-api/src/session.rs:232`.
When set `> 1`, round-robin repartitioning is also enabled so DataFusion can
actually parallelize hash-join build and aggregation spill across N threads.

---

### Phase 1 — Data-size-aware bucket count (shuffle domain)

**Status: Complete across all execution modes.**

#### 1a. AQE `AutoPartitionRule`

Location: `krishiv-plan/src/optimizer.rs:537`

Fires after each shuffle stage completes. Reads `RuntimeStats::memory_bytes`
from the upstream stage output. Formula:

```
buckets = clamp(ceil(estimated_bytes / 128 MiB), 1, executor_count × max_per_executor)
```

Registered in `default_aqe_optimizer()` alongside `CoalesceRule`.
`StreamingAqeGuard` prevents both rules from firing on streaming plans.

#### 1b. Bounded window shard calculation

Location: `krishiv-scheduler/src/bounded_window.rs:59-77`

```rust
const TARGET_BYTES_PER_SHARD: u64 = krishiv_common::partition::TARGET_BYTES_PER_PARTITION; // 128 MiB

let data_based_shards = (total_data_bytes + TARGET_BYTES_PER_SHARD - 1)
    / TARGET_BYTES_PER_SHARD;
shard_limit = executor_count.min(data_based_shards.min(input_row_count.max(1)));
```

Measures actual Arrow memory footprint of all input batches, not row count.
A 500 MiB input on 3 executors produces 4 shards — not 3.

#### 1c. Byte-aware coalesce

Location: `krishiv-dataflow/src/coalesce_partitions.rs`

Post-shuffle coalesce uses byte-size bin-packing:

```rust
let target_bytes_per_group = (total_bytes / target as u64).max(1);
// closes a group when it has collected ≥ target_bytes AND more groups remain
```

The old `chunk_size = inputs.len().div_ceil(target)` was pure count-based and
produced wildly uneven groups when input batches were skewed in size.

#### 1d. Shuffle write config propagation (critical bug fix)

Location: `krishiv-scheduler/src/job.rs:394-408`

`ShuffleWriteConfig` and `ShuffleReadConfig` on `TaskSpec` are now propagated
to `ExecutorTaskAssignment` at launch time. Previously these fields were set at
plan-construction time but silently dropped before the assignment reached the
executor — making typed shuffle config effectively dead code.

#### 1e. Streaming: EMA-based `StreamingPartitionAdvisor`

Location: `krishiv-dataflow/src/adaptive.rs:323`

For continuous streaming jobs, `AutoPartitionRule` cannot use AQE
(no stage boundary to re-optimize at). Instead, `StreamingPartitionAdvisor`
maintains an exponential moving average of observed batch byte sizes and
returns a bucket count recommendation on each cycle:

```rust
ema = α × batch_bytes + (1-α) × prev_ema   // α = 0.2 (5-batch lag)
buckets = clamp(ceil(ema / 128 MiB), min_buckets, max_buckets)
```

The EMA seeds on the first observation to avoid a zero-start cold-start bias.
Consumed by streaming executors to adjust their `HashPartitioner` bucket count
each drain cycle without coordinator round-trips.

---

### Phase 2 — Hot-key detection and skew mitigation

**Status: Detection complete. Automatic split mitigation complete for next-stage.**

#### 2a. Executor-side hot-key tracking

**Batch shuffle writes** (`krishiv-executor/src/fragment/batch.rs:472-503`):
`HeavyHittersTracker` (SpaceSaving, 64 slots) scans the key column during
`execute_shuffle_write_fragment` and `execute_inmem_shuffle_write`. Keys with
heat score ≥ 10% produce a `HeartbeatHotKeyReport` attached to
`ExecutorTaskOutput.hot_key_reports`.

**Streaming windows** (`krishiv-executor/src/fragment/streaming.rs`):
The same tracker now runs on all three streaming output paths:
- `execute_loop_fragment` (continuous window drain)
- `execute_streaming_with_batches` (InMemory fast path)
- Default bounded window path (stream-kafka / ASCII)

All paths call `build_streaming_hot_key_reports(batches, key_column, job_id, stage_id)`
and attach reports to the output. This closes the gap where only batch shuffle
jobs signalled hot keys to the coordinator.

#### 2b. Coordinator-side skew response

Location: `krishiv-scheduler/src/coordinator/executor_ops.rs` +
`krishiv-scheduler/src/coordinator/task_assignment.rs` + `job.rs`

`process_hot_key_reports` sets `skew_repartition_overrides[job_id] = buckets`
when any reported key has `heat_score ≥ 0.3`. The coordinator previously set
this override but never consumed it (dead code). It is now read at launch time:

```rust
// task_assignment.rs
let skew_override = self.skew_repartition_overrides.get(job_id).copied();

// job.rs — replaces num_partitions in ShuffleWriteConfig before dispatch
let effective_num_partitions = skew_partition_override
    .map(|n| n as usize)
    .unwrap_or(write_cfg.num_partitions)
    .max(1);
```

This means: if a shuffle stage reports skew, the _next_ stage launch uses a
larger bucket count automatically. No user action required.

**What is not yet done**: sub-partition salted splits within a running stage
(requires stage retry or dynamic task insertion). Currently the system responds
at stage granularity (next stage gets more buckets), not within-stage.

#### 2c. Checkpoint rescaling integrity

Location: `krishiv-state/src/checkpoint/rescaling.rs`

`RescaleChecksum` verifies that checkpoint partition splits do not lose or
duplicate rows. The coordinator captures `{total_rows, column_count, old_parallelism,
new_parallelism}` before the split; the executor calls `verify()` after, which
checks row totals and shard count. Inspired by the Netflix Planner/Splitter
pattern for idempotent splits.

---

### Phase 3 — Auto-broadcast for small tables

**Status: Rule implemented; `estimated_rows` population is partial.**

`BroadcastAutoRule` in `krishiv-plan/src/optimizer.rs:930` scans logical plan
for `NodeOp::Scan` nodes whose `estimated_rows` is ≤ 1,000,000 rows and sets
`broadcast_eligible = true`. The lowering pass then promotes the exchange to
`Broadcast` (no shuffle at all for that table side).

Limitation: `estimated_rows` is only populated when callers explicitly provide
it from Parquet footer statistics. It is not yet auto-populated for all source
types. Tables without a row count estimate are never auto-broadcast.

---

### Phase 4 — Escape hatches

**Status: All three implemented. For the 10% that auto-mode cannot handle.**

| Knob | Surface | Example |
|------|---------|---------|
| `SET shuffle.partitions = N` | SQL session | `SET shuffle.partitions = 512;` |
| `DataFrame::repartition(n, keys)` | Rust API | `df.repartition(16, &["key"])` |
| `SessionBuilder::with_shuffle_partitions(n)` | Rust builder | `SessionBuilder::new().with_shuffle_partitions(64)` |
| `KRISHIV_TARGET_PARALLELISM=N` | Env var | Cluster-wide DataFusion parallelism |
| `KRISHIV_IVM_SHARDS=N` | Env var | IVM partition fan-out (`1` disables partitioning) |

`SET shuffle.partitions` is parsed in `krishiv-sql/src/lib.rs:1161` and stored
per-session. It overrides `AutoPartitionRule` for that session only. It does
not affect the hot-key override path (coordinator always wins on skew).

---

### Phase 5 — Incremental view (IVM) partitioning

**Status: Complete — mechanism, auto-rule, and coordinator wiring.**

`IncrementalFlow` was single-partition — keyed incremental views (`GROUP BY`
aggregates) ran on one core regardless of data size, the one mode the
zero-config guarantee did not cover. `PartitionedIncrementalFlow`
(`krishiv-ivm/src/partitioned.rs`) closes that gap.

It shards an `IncrementalFlow` across `N` partitions by a key column. Feeds are
routed by the **shared keyed hash** (`partition_record_batches_by_key`, SHA-256 —
the same family as streaming key groups), so every key's rows land in exactly one
shard. For a `GROUP BY <key>` view sharded by `<key>`, each group lives entirely
in one shard, so per-shard snapshots concatenate with no cross-shard merge.
Shards step in parallel (`futures::future::try_join_all`), removing the
single-core ceiling.

The **auto-rule** keeps it zero-config:

```rust
PartitionedIncrementalFlow::auto_for_view(ctx, spec, total_bytes_hint, max_shards)
```

- `partition_key_for_view` inspects the view's logical plan: a **single-column
  `GROUP BY` aggregate** over one source is provably shardable and returns its
  key; multi-column `GROUP BY`, joins (two sources keyed independently), and
  diff-based views return `None`.
- Shardable views are sized by `recommended_shards` → `recommend_buckets`
  (Phase 1's sizing brain); everything else falls back to a single flow.

Correctness is locked in by `partitioned_group_by_matches_single_flow` (3 shards
vs. 1 shard, identical per-region totals), `checkpoint_restore_round_trips_across_shards`,
and `feed_snapshot_drains_vanished_keys`.

**Registry wiring — all deployment modes.** The `IvmJobRegistry`
(`krishiv-scheduler/src/ivm.rs`) holds an `IvmJob` enum — `Single(IncrementalFlow)`
or `Partitioned(PartitionedIncrementalFlow)`. The decision is made transparently
at the **first** `register_view`: a single-column `GROUP BY` aggregate (detected
schema-free via `partition_key_from_sql`, since sources aren't registered yet)
upgrades the job to a partitioned flow; anything else stays single. The fan-out
is `KRISHIV_IVM_SHARDS` if set (≥1; `1` disables partitioning), else available
parallelism capped at 8.

This registry backs **every deployment mode**, so embedded, single-node, and
distributed IVM all auto-partition identically:

- **Distributed** — `session.ivm()` → `RemoteIvmJob` → coordinator HTTP `/views`
  → `registry.register_view`.
- **Embedded / single-node** — `session.ivm()` → `EmbeddedIvmJob`, which now holds
  the registry + job id and registers views **through** `registry.register_view`
  (not directly on a flow), so the same upgrade fires in-process.

**Every** IVM HTTP endpoint routes through `IvmJob`, so partitioning is invisible
to clients — feed, stream-delta, stream-bridge, step, snap, output, checkpoint/
restore, checkpoint-delta/restore-delta, drop-view, **vector-views**:

- Partitioned `feed_snapshot` differentiates the whole snapshot once at the top
  level, then routes the delta — insertions *and* retractions — by key, so a key
  whose rows vanish is correctly drained from its shard.
- The per-view `/output` peek merges the per-shard output deltas
  (`view_output_peek` → `DeltaBatch::concat`); for exact materialized state
  prefer `/snap`, since output deltas are tick-relative.
- **Vector views** spawn one background task per shard, all writing the **same
  shared sink**. For a `GROUP BY <key>` view sharded by `<key>`, each id (the
  group key) lives in exactly one shard, so the shards push disjoint id sets with
  no cross-shard conflict.
- Checkpoints are shard-count framed and reject restores with a mismatched shard
  count.

Coverage: `partitioned_job_matches_single_job_end_to_end`,
`register_view_auto_partitions_group_by`, `partitioned_job_checkpoint_restore`,
`feed_snapshot_through_partitioned_registry_job`, `view_output_peek_through_partitioned_job`,
`spawn_vector_views_fans_out_per_shard`, `resolve_ivm_shards_honours_env_and_caps`
(`krishiv-scheduler`); `embedded_group_by_view_auto_partitions`,
`embedded_partitioned_feed_step_snapshot_matches_single` (`krishiv-runtime`); plus
29 `PartitionedIncrementalFlow` edge-case tests (`krishiv-ivm`).

---

## Hash Function Boundary

Two independent hash functions are used and must not be cross-substituted:

| Domain | Hash | File | Purpose |
|--------|------|------|---------|
| Shuffle routing | XxHash64 | `krishiv-shuffle/src/partitioner.rs` | Deterministic bucket assignment across executor nodes. Speed matters; cryptographic properties do not. Nulls route to bucket 0 and are counted. |
| Keyed semantics | SHA-256 + domain prefix `krishiv.partition-key.v1\0` | `krishiv-common/src/partition.rs` | Join keys, state sharding, streaming key groups, checkpoint key groups, IVM shard routing. Domain separation prevents accidental hash collision across use sites. Rejects nulls. |

Using the shuffle router where keyed semantics are required produces incorrect
join results. The module-level doc comment in `partitioner.rs` encodes this
boundary explicitly.

Streaming key groups (`krishiv-state/src/key_group.rs`) now route through the
shared keyed hash via `key_group_for_bytes`, replacing a divergent
`XxHash64(seed 0)`. IVM shard routing (`PartitionedIncrementalFlow`) reuses the
same `partition_record_batches_by_key`. Every keyed-semantics use site is now one
hash **family** — SHA-256 under the `krishiv.partition-key.v1` domain.

Note the family uses **sub-tags** for domain separation, so the numeric group a
key lands in is *not* required to match across use sites: the keyed partitioner
hashes typed values (`i32\0`, `i64\0`, `utf8\0`, …) while `key_group_for_bytes`
hashes serialized key bytes under `keygroup\0`. This is deliberate — each mode
partitions an independent space (streaming uses 32768 rescale key groups; IVM
uses shard count; batch uses bucket count), and they never need to agree on a
group *number*, only to be collision-resistant and deterministic within their own
space. What's unified is the algorithm and domain, not a single global key→bucket
table. (Checkpoints written before this change used the old XxHash64 key-group
assignment; see the migration note in `key_group.rs`.)

---

## Constants

All byte-size targets derive from a single source of truth:

```rust
// krishiv-common/src/partition.rs
pub const TARGET_BYTES_PER_PARTITION: u64 = 128 * 1024 * 1024; // 128 MiB
```

`bounded_window.rs`, `coalesce_partitions.rs`, `adaptive.rs`
(`STREAMING_TARGET_BYTES_PER_PARTITION`), and `AutoPartitionRule` all reference
this constant. To change the target cluster-wide, change one number.

### One sizing function — `recommend_buckets`

All bucket/shard counts derive from a single function, so batch, streaming, and
IVM agree on bytes-per-partition:

```rust
// krishiv-common/src/partition.rs
pub fn recommend_buckets(bytes, min_buckets, max_buckets, target_bytes_per_partition) -> u32;
pub fn recommend_buckets_default(bytes, min_buckets, max_buckets) -> u32; // uses TARGET_BYTES_PER_PARTITION
```

`AutoPartitionRule` (batch AQE), `StreamingPartitionAdvisor` (streaming EMA),
`bounded_window` shard sizing, and `PartitionedIncrementalFlow::recommended_shards`
(IVM) all call it. The old duplicated `ceil(bytes / target).clamp(...)` formulas
are gone — there is one place to change the sizing policy.

---

## Invariants (enforced, not aspirational)

- `AutoPartitionRule` never fires on `ExecutionKind::Streaming` plans
  (`StreamingAqeGuard` wraps it).
- `AutoPartitionRule` never fires on `Broadcast` partitioning (no-op guard).
- `target_bytes_per_partition` has a minimum of 1 byte (guard in
  `coalesce_partition_batches`); in practice the 128 MiB default prevents
  millions of micro-partitions.
- Coalesce output count never exceeds `target_partitions` regardless of input
  batch sizes or byte distribution (last group absorbs remainder).
- Shuffle config (`ShuffleWriteConfig` / `ShuffleReadConfig`) on `TaskSpec`
  always reaches the executor — the propagation gap in `job.rs` that silently
  dropped these is fixed.
- Hot-key override (`skew_repartition_overrides`) is always consumed at the
  next stage launch — the coordinator HashMap no longer accumulates dead entries.

---

## Real-World Test Scenarios

These scenarios exercise the full auto-partition stack from user-facing API
down to the coordinator. None require any partition configuration.

### Scenario 1 — E-commerce Clickstream with Power-User Skew

**What it tests**: hot-key detection + skew override, streaming EMA advisor,
byte-aware coalesce.

**Setup**: Generate a Parquet dataset simulating 30-day clickstream where top
1,000 bot user_ids generate 60% of events (Pareto-distributed, α ≈ 1.1).

```python
import krishiv
import pyarrow as pa
import numpy as np

session = krishiv.session()

# Generate: 10M rows, top 1000 user_ids hold 60% of traffic
n = 10_000_000
bot_ids   = np.random.choice(range(1, 1001),    size=int(n * 0.60))
human_ids = np.random.choice(range(1001, 50001), size=int(n * 0.40))
user_ids  = np.concatenate([bot_ids, human_ids])
np.random.shuffle(user_ids)

table = pa.table({
    "user_id":    pa.array(user_ids, type=pa.int32()),
    "event_time": pa.array(np.random.randint(0, 86400000, n), type=pa.int64()),
    "page":       pa.array(np.random.choice(["home", "pdp", "cart", "checkout"], n)),
    "revenue_usd":pa.array(np.where(np.isin(user_ids, range(1, 1001)), 0.0,
                            np.random.exponential(25.0, n))),
})
session.write_parquet(table, "/tmp/clickstream/")

# Query — no partition hints
result = session.sql("""
    SELECT user_id,
           COUNT(*)                         AS clicks,
           SUM(revenue_usd)                 AS revenue,
           COUNT(DISTINCT page)             AS pages_visited
    FROM   read_parquet('/tmp/clickstream/')
    WHERE  revenue_usd > 0
    GROUP  BY user_id
    ORDER  BY revenue DESC
    LIMIT  100
""")
result.show()
```

**What to verify**:
- Coordinator logs show `HotKeySplit` decision for user_ids 1–1000.
- `skew_repartition_overrides` is set after stage 1; stage 2 launches with
  higher `num_partitions` automatically.
- No manual `SET shuffle.partitions` issued.
- Wall-clock time not dominated by one executor holding the hot partition.

---

### Scenario 2 — IoT Sensor Fleet: Tumbling Window Aggregation

**What it tests**: `StreamingPartitionAdvisor` EMA, streaming hot-key reports,
`execute_loop_fragment` tracking.

**Setup**: 50,000 sensors sending temperature readings every second. Sensor IDs
1–100 are "gateway" sensors that aggregate sub-sensor data — they have 10× the
event volume of regular sensors.

```python
import krishiv, time, random

session = krishiv.session()

stream = session.kafka_stream(
    topic="iot-sensors",
    bootstrap_servers="localhost:9092",
    key_column="sensor_id",
    value_columns=["temp_celsius", "ts_ms"],
)

# 10-second tumbling window, 30-second watermark lag
result = (
    stream
    .tumbling_window("10s", watermark_lag="30s", key="sensor_id")
    .agg(
        avg("temp_celsius").alias("avg_temp"),
        max("temp_celsius").alias("max_temp"),
        count("*").alias("readings"),
    )
)

result.write_stream(krishiv.KafkaSink("iot-aggregated"))
```

**What to verify**:
- After a few drain cycles, `StreamingPartitionAdvisor.current_buckets()` grows
  when sensor_id=1–100 bursts arrive and shrinks during quiet periods.
- Hot-key reports for gateway sensor IDs 1–100 appear in coordinator heartbeat
  logs with `heat_score > 0.1`.
- No user intervention required between burst and quiet periods.

---

### Scenario 3 — Web Server Log Analytics: Multi-Stage Join with Broadcast

**What it tests**: `BroadcastAutoRule` (small dimension), `AutoPartitionRule`
(large fact table), coalesce bin-packing.

**Setup**: 2 TB of Nginx access logs joined to a 5 MB URL classification table.
The URL table should be auto-broadcast; the logs should be auto-partitioned.

```sql
-- No hints. BroadcastAutoRule detects url_classes is small; logs is large.

CREATE EXTERNAL TABLE logs (
    ts          BIGINT,
    client_ip   VARCHAR,
    url         VARCHAR,
    status_code INT,
    bytes_sent  BIGINT
) STORED AS PARQUET LOCATION 's3://prod-logs/nginx/2024/';

CREATE EXTERNAL TABLE url_classes (
    url_pattern VARCHAR,
    category    VARCHAR,
    is_api      BOOLEAN
) STORED AS PARQUET LOCATION 's3://config/url-classes/';   -- 5 MB

SELECT
    date_trunc('hour', to_timestamp(ts / 1000)) AS hour,
    u.category,
    COUNT(*)                                     AS requests,
    SUM(CASE WHEN l.status_code >= 500 THEN 1 ELSE 0 END) AS errors,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY l.bytes_sent) AS p95_bytes
FROM   logs l
JOIN   url_classes u ON l.url LIKE u.url_pattern
GROUP  BY 1, 2
ORDER  BY hour DESC, errors DESC;
```

**What to verify**:
- `EXPLAIN` output shows `url_classes` with `broadcast_eligible = true` and no
  exchange node on its side of the join.
- `logs` side has an `Exchange { Hash }` whose bucket count equals
  `ceil(2TiB / 128MiB)` = 16,384 (capped at `executor_count × max_per_executor`).
- Query completes without OOM on the join executor despite 2 TB input.

---

### Scenario 4 — Financial Transactions: Checkpoint Rescaling Under Load

**What it tests**: `RescaleChecksum`, `KeyGroupRescaler`, hot-key response for
high-value customer IDs.

**Setup**: Streaming fraud-detection job partitioned by `account_id`. Start with
parallelism 4, scale to 8 mid-stream. A handful of institutional accounts
generate 40% of transaction volume (skew that triggers partition override).

```rust
use krishiv::prelude::*;

let session = Session::builder()
    .with_durable_state("/mnt/krishiv-state/fraud")
    .build()
    .await?;

let transactions = session
    .kafka_stream("transactions", &["account_id", "amount_usd", "ts_ms"])
    .await?;

// Session window: groups bursts of activity per account into episodes
let fraud_signals = transactions
    .session_window(
        key:          "account_id",
        gap:          Duration::from_secs(300),  // 5-minute inactivity gap
        watermark_lag: Duration::from_secs(60),
    )
    .agg([
        sum("amount_usd").alias("episode_total"),
        count_distinct("merchant_id").alias("merchant_count"),
        max("ts_ms").minus(min("ts_ms")).alias("episode_duration_ms"),
    ])
    .filter(col("episode_total").gt(lit(10_000.0))
        .or(col("merchant_count").gt(lit(20))))
    .await?;

fraud_signals
    .write_stream(KafkaSink::new("fraud-alerts"))
    .await?;

// Simulate scale-out: coordinator receives rescale request
// RescaleChecksum verifies no transaction is lost or double-counted.
// skew_repartition_overrides fires for institutional account_ids automatically.
```

**What to verify**:
- After scale from 4→8: `RescaleChecksum::verify()` passes (row count preserved).
- `KeyGroupRescaler::task_for_key_group()` correctly remaps all 65,536 key groups
  to 8 tasks with no gaps.
- Institutional `account_id` values trigger `skew_repartition_overrides` with
  larger bucket count on subsequent stages.
- No `SET shuffle.partitions` or manual parallelism tuning required.

---

### Scenario 5 — Regression Baseline: Tiny Data Should Not Over-Shard

**What it tests**: `AutoPartitionRule` lower bound, `coalesce_partition_batches`
zero-byte guard, `StreamingPartitionAdvisor` min_buckets clamp.

```python
import krishiv, pyarrow as pa

session = krishiv.session()

# 10 rows — should produce exactly 1 partition, not 8 or 128
tiny = pa.table({"id": range(10), "val": range(10)})
session.write_parquet(tiny, "/tmp/tiny/")

result = session.sql("SELECT id, val * 2 AS doubled FROM read_parquet('/tmp/tiny/')")
result.show()

# Assert: plan has 1 Exchange bucket, not the default parallelism
plan = session.explain("SELECT id, val * 2 AS doubled FROM read_parquet('/tmp/tiny/')")
assert "buckets: 1" in plan or "Broadcast" in plan, f"Over-sharded tiny table:\n{plan}"
```

This prevents a class of regression where AQE fires with stale stats and
creates hundreds of empty tasks for single-digit row inputs.

---

## What Remains

| Item | Priority | Notes |
|------|----------|-------|
| Distributed IVM compute across executors | Medium | IVM SQL runs centrally on the coordinator (multi-core via partitioning), which is correct and durable today. The `delta:step:` executor fragment (`krishiv-executor/src/fragment/ivm.rs`) is a separate, unwired path. Routing partitioned shards to executors is a **dedicated project**, not a cleanup: it moves stateful IVM operators off the coordinator and requires shard→executor assignment, distributed snapshot/checkpoint coordination, and executor-failure state recovery. Deferred deliberately. |
| Embedded batch shuffle bucket sizing | Low | Embedded batch uses DataFusion thread parallelism only; the AQE `AutoPartitionRule` (data-size bucket count) runs on the coordinator path (single-node/distributed). Embedded is dev-only, so this is intentional. |
| Within-stage hot-key salt split | High | Requires stage retry or dynamic task insertion. Currently mitigation is next-stage only. |
| `estimated_rows` from Parquet footers | Medium | `BroadcastAutoRule` needs row count to work for all source types. Read from footer statistics in `krishiv-connectors`. |
| `KRISHIV_SHUFFLE_PARTITIONS` cluster env var | Low | A cluster-wide shuffle partition floor. Useful for operators; `with_shuffle_partitions()` covers the Rust API path already. |
| Broadcast join for streaming dimension tables | Low | Streaming sources have no row count estimate; broadcast eligibility would need a size hint from the source. |
