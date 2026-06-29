# Krishiv Incremental Computing — Implementation Document

**Status**: In Progress  
**Started**: 2026-06-17  
**Phase coverage**: Phases 1–7 (krishiv-delta crate through Python API)

---

## Table of Contents

1. [Motivation and Goals](#motivation-and-goals)
2. [Architectural Decisions (ADRs)](#architectural-decisions)
3. [Data Model](#data-model)
4. [Crate Map](#crate-map)
5. [Operator Catalog](#operator-catalog)
6. [SQL API](#sql-api)
7. [Rust API](#rust-api)
8. [Python API](#python-api)
9. [Phase TODOs](#phase-todos)
10. [Testing Plan](#testing-plan)
11. [Known Limitations and Future Work](#known-limitations-and-future-work)

---

## Motivation and Goals

Krishiv already handles **batch SQL** (full-scan DataFusion queries) and **streaming** (append-only, time-ordered event streams). The missing mode is **incremental computing**: maintaining query results continuously as source data changes, processing only the delta at each step rather than re-scanning full tables.

Inspirations studied:
- **Feldera/DBSP**: algebraically proven incremental view maintenance via Z-sets and bilinear join; LATENESS-based GC; recursive fixed-point views.
- **CocoIndex**: content-addressed memoization; behavior versioning; tiered change detection; convergent roll-forward commit; per-operator concurrency control.

Krishiv already contains the building blocks:
- `DeltaOp {Insert, Update, Delete}` and `DeltaEntry` in `krishiv-connectors` — already models change operations.
- `Source::current_offset()` — ordinal watermark hook for tiered change detection.
- `krishiv-state` RocksDB backend — durable Trace storage.
- `OperatorUid` and `OperatorConfig` — per-operator identity for behavior versioning.
- `CdcRouter` — Debezium CDC fan-out, feeds `DeltaOp` events.
- `CREATE LIVE TABLE` SQL intercept — pattern for new DDL.
- XxHash64 (workspace dep `twox-hash`) — content fingerprinting.

---

## Architectural Decisions

### ADR-I-01: `ChangeBatch` as Arrow RecordBatch + `_weight` Column

**Decision**: A `ChangeBatch` is an Arrow `RecordBatch` where the last column is always named `_weight` with type `Int64`. Positive weight = insertion, negative weight = retraction.

**Alternatives considered**:
- Separate `Vec<(RecordBatch, i64)>` per-batch weight: loses columnar density.
- Separate `weights: Vec<i64>` parallel vector: requires synchronized iteration, breaks Arrow compute kernels.
- New struct with separate data/weight arrays: breaks DataFusion interop.

**Rationale**: Arrow `RecordBatch` is already the universal type in Krishiv. Adding one `Int64` column maintains full DataFusion interoperability (DataFusion can filter/project/aggregate on it), enables vectorized weight arithmetic via Arrow compute, and requires no new serialization format — IPC already handles it.

**Consequence**: All `ChangeBatch` helpers strip `_weight` before handing `RecordBatch` to DataFusion operators, then re-attach it after.

---

### ADR-I-02: `Trace` is an In-Memory Spine with Optional RocksDB Spill

**Decision**: `Trace` maintains sorted `ChangeBatch` levels in memory (Spine-style: 8 levels, merge when level i > threshold). Durable persistence is via `TraceStateNamespace` in `krishiv-state` (RocksDB), which serializes the consolidated Trace as Arrow IPC batches per operator UID.

**Alternatives considered**:
- LMDB (like CocoIndex): already has RocksDB in workspace; avoid a second embedded KV store.
- Pure in-memory only: no restart durability for join/aggregate state.
- Always-on disk: too slow for hot join probes; in-memory first is correct.

**Rationale**: In-memory gives fast join probing; RocksDB flush on checkpoint gives durability. Existing `krishiv-state` checkpoint protocol handles the flush/restore lifecycle.

---

### ADR-I-03: No Code Generation — Runtime Interpretation via DataFusion Plans

**Decision**: Unlike Feldera (which compiles SQL → Rust source → native binary), Krishiv's incremental views execute via runtime interpretation: the SQL body is compiled to a DataFusion `LogicalPlan`, which is then walked to build an `IncrementalPlan` (a `Vec<Box<dyn IncrementalOp>>`). Operators execute against `ChangeBatch` at runtime.

**Rationale**:
- Avoids 10+ minute compile latency on SQL changes (Feldera's key pain point).
- DataFusion's optimizer already runs (predicate pushdown, projection pruning, etc.) before we convert.
- Krishiv's existing SQL execution path (DataFusion → Arrow → connectors) is reused — no new code-gen pipeline.

**Consequence**: Slightly less optimized than native compiled code (e.g., no inlining across operator boundaries), but correctness and developer velocity are higher priorities at this stage.

---

### ADR-I-04: New Type (`ChangeBatch` / `IncrementalFlow`) — Do Not Extend Existing APIs

**Decision**: Add incremental as an explicit opt-in mode with new types: `ChangeBatch`, `IncrementalFlow`, `IncrementalView`. Existing `DataFrame`, `StreamingDataFrame`, `CreateLiveTable` are unchanged.

**Rationale**:
- No regression risk on existing batch/streaming users.
- The three modes are mathematically related (`Batch[T+1] = Batch[T] + Σ ChangeBatch[T]`) but have different execution contracts.
- Opt-in via `CREATE INCREMENTAL VIEW` (SQL) or `session.incremental()` (Rust/Python).

---

### ADR-I-05: Distributed from Day One via Existing Shuffle/Executor Infrastructure

**Decision**: Incremental computation on partitioned data uses the existing `krishiv-shuffle` and `krishiv-scheduler`/`krishiv-executor` infrastructure. Each partition gets its own `Trace` shard; join probing is partition-local (hash-partitioned by join key).

**Rationale**:
- Feldera is single-node only. CocoIndex is single-node only. Krishiv is designed for distributed from the start.
- Avoids re-designing the distribution layer.
- Key partitioning for join Traces matches existing shuffle key partitioning.

**Consequence**: `IncrementalJoinOperator` must be partition-aware. Cross-partition joins require a shuffle step (same as streaming join in `krishiv-dataflow`).

---

### ADR-I-06: LATENESS Annotation for Watermark-Based State GC

**Decision**: Sources in incremental mode accept a `LATENESS` annotation on timestamp columns. State entries with `timestamp < (max_observed_ts - lateness)` are eligible for GC on each operator's `Trace`.

**Rationale**: Without GC, long-running incremental views over time-series data accumulate unbounded Trace state. LATENESS gives users explicit control: discard late arrivals, free old state. Matches Feldera's design exactly.

**Implementation**: `lateness.rs` in `krishiv-delta` maintains per-view `WatermarkTracker { max_ts: i64, lateness_ms: i64 }`. On each `step()`, computes `watermark = max_ts - lateness_ms`. Calls `trace.gc_below_watermark(ts_col, watermark)` on all stateful operators.

---

### ADR-I-07: Behavior Versioning for Operator-Level Cache Invalidation

**Decision**: `OperatorConfig` gains a `behavior_version: u64` field. `LogicFingerprint` = `XxHash64(uid_bytes || behavior_version.to_le_bytes())`. When `behavior_version` changes, the fingerprint changes, triggering recomputation from scratch for that operator (Trace cleared, running aggregates reset).

**Rationale**: From CocoIndex. Without this, a UDF change silently serves stale memoized results. Explicit versioning forces intentional invalidation.

**Consequence**: Users must bump `behavior_version` when changing UDF logic. Default is `0`; first change → `1`, etc.

---

### ADR-I-08: Convergent Roll-Forward Commit (No Rollback)

**Decision**: The incremental commit protocol never rolls back. If a step partially fails, the next step re-processes the same input (idempotent delta). The "superset invariant" from CocoIndex applies: internal Trace state is always a superset of what has been durably committed to sinks.

**Rationale**: Rollback in a distributed system requires distributed undo, which is complex and slow. Roll-forward is simpler and aligns with Krishiv's existing checkpoint-then-apply pattern.

**Consequence**: Sinks must be idempotent (upsert by key, not append). This is enforced via `merge_key` required on all incremental sinks that are not append-only.

---

### ADR-I-09: CoalescingMap for Rapid-Update Debouncing

**Decision**: When a source emits multiple rapid updates to the same key (e.g., a row updated 10 times in 1 second from CDC), a `CoalescingMap<K, ChangeBatch>` in the ingest layer collapses them into the single latest state before the incremental operators see them.

**Rationale**: Without coalescing, a burst of N updates triggers N evaluations of every downstream operator. Coalescing trades some recency for throughput.

**Implementation**: `CoalescingMap<K, V>` is a `HashMap<K, V>` where `insert(k, v)` always overwrites. Drain produces `(k, latest_v)` pairs in insertion order.

---

### ADR-I-10: Recursive Views via Fixed-Point Iteration

**Decision**: `DECLARE RECURSIVE VIEW v(...)` + `CREATE INCREMENTAL VIEW v AS ...` implements Datalog-style fixed-point iteration. Each step runs the view body iteratively until the output `ChangeBatch` is empty (no new changes). A cycle-limit guard (default 1000 iterations) prevents infinite loops.

**Rationale**: Enables graph reachability, transitive closure, and other recursive queries. Matches Feldera's recursive view support.

**Note**: Recursive views are automatically `DISTINCT` (to prevent infinite weight growth on cycles). This is enforced by the compiler.

---

## Data Model

### `ChangeBatch`

```
ChangeBatch {
    inner: RecordBatch,     // includes _weight: Int64 as last column
    data_schema: SchemaRef, // schema WITHOUT _weight column
    num_data_cols: usize,   // inner.num_columns() - 1
}
```

Z-set algebra operations:
- **add(a, b)** → merge + consolidate (sum weights for matching rows)
- **negate(a)** → flip sign of all weights
- **subtract(a, b)** → add(a, negate(b))
- **filter(a, pred)** → keep rows where pred holds, preserve weights
- **map(a, f)** → apply f to data columns, preserve weights; if f is not injective, matching output rows have summed weights

Weight semantics:
- `+1` = row present (insert)
- `-1` = row absent (delete)
- `+2` = row present twice (multiset; rare in practice)
- `0` = row absent (eliminated by consolidate)

### `Trace`

```
Trace {
    levels: [Vec<ChangeBatch>; 8],  // level i holds batches of approx 2^i rows
    key_columns: Vec<usize>,         // column indices of join/group key
    total_rows: usize,
    lateness_col: Option<usize>,     // column index for LATENESS GC
}
```

Spine merge strategy: when `levels[i].len() > MERGE_THRESHOLD[i]`, merge all batches at level i into one and promote to level i+1. This gives O(log N) amortized merge cost and O(log² N) probe cost.

### `LogicFingerprint`

```
LogicFingerprint(u64) = XxHash64(
    uid.as_bytes(),
    behavior_version.to_le_bytes(),
)
```

Used to key the memo cache in `TraceStateNamespace`. When the fingerprint changes, the existing Trace is discarded and the operator recomputes from scratch.

### `WatermarkTracker`

```
WatermarkTracker {
    max_observed_ts: i64,  // max event timestamp seen
    lateness_ms: i64,      // configured LATENESS bound
}
watermark = max_observed_ts - lateness_ms
```

Records with `ts < watermark` are dropped at ingestion. State entries with `ts < watermark` are GC-eligible.

---

## Crate Map

| Crate | Change | New files |
|---|---|---|
| **`krishiv-delta`** (NEW) | Core incremental engine | All files below |
| **`krishiv-dataflow`** | Add `behavior_version` to `OperatorConfig` | `operator_config.rs` edit |
| **`krishiv-state`** | Add `TraceStateNamespace` for Trace persistence | `incremental_trace.rs` |
| **`krishiv-connectors`** | Bridge `DeltaEntry` → `ChangeBatch` | `change_batch_bridge.rs` |
| **`krishiv-sql`** | SQL intercept for new DDL | `incremental_view.rs`, `lib.rs` edit |
| **`krishiv-scheduler`** | Source watermark tracking, skip-if-unchanged | `source_watermark.rs` |
| **`krishiv-api`** | `session.incremental()`, `IncrementalFlow` | `incremental_flow.rs`, `session.rs` edit |
| **`krishiv-python`** | `PyIncrementalFlow`, `@flow.view` | `incremental.rs`, `lib.rs` edit |

### `krishiv-delta` file layout

```
crates/krishiv-delta/
  Cargo.toml
  src/
    lib.rs                  — public re-exports, crate doc
    error.rs                — DeltaError type
    change_batch.rs         — ChangeBatch, Z-set algebra
    trace.rs                — Spine-style Trace, cursor, GC
    lateness.rs             — WatermarkTracker, LATENESS annotation
    behavior_version.rs     — LogicFingerprint, behavior versioning
    coalesce.rs             — CoalescingMap for update debouncing
    view.rs                 — IncrementalView, IncrementalViewRegistry
    plan.rs                 — IncrementalPlan, plan-walking from DataFusion LogicalPlan
    operators/
      mod.rs
      map.rs                — linear map (apply fn, preserve weights)
      filter.rs             — linear filter (predicate, preserve weights)
      consolidate.rs        — consolidate duplicate keys, sum weights, drop zeros
      join.rs               — bilinear incremental join (two Traces)
      aggregate.rs          — stateful aggregate (sum, count, avg; retraction-aware)
      distinct.rs           — nonlinear distinct (threshold tracking via HashMap)
      recursive.rs          — fixed-point iteration for recursive views
```

---

## Operator Catalog

### Linear Operators (no state, free incrementalization)

| Operator | Module | Cost per tick | Notes |
|---|---|---|---|
| `Map` | `operators/map.rs` | O(|ΔA|) | Apply fn to data cols; preserve `_weight` |
| `Filter` | `operators/filter.rs` | O(|ΔA|) | Boolean mask; preserve weights |
| `Project` | `operators/map.rs` | O(|ΔA|) | Column selection; special case of Map |
| `Union` | `change_batch.rs` | O(|ΔA|+|ΔB|) | Arrow concat; handled in consolidate |
| `Consolidate` | `operators/consolidate.rs` | O(|ΔA| log|ΔA|) | Sort by key, sum weights, drop zeros |

### Bilinear Operators (two Traces, cost ∝ delta size)

| Operator | Module | State | Cost per tick |
|---|---|---|---|
| `InnerJoin` | `operators/join.rs` | `trace_left`, `trace_right` | O(|ΔA|·|matches_B| + |ΔB|·|matches_A|) |
| `SemiJoin` | `operators/join.rs` | `trace_right` only | O(|ΔA|·|key_matches_B|) |
| `AntiJoin` | `operators/join.rs` | `trace_right` | O(|ΔA| + |key_matches_B|) |
| `LeftOuterJoin` | `operators/join.rs` | `trace_left`, `trace_right` | Like inner + null-fill for unmatched |

### Nonlinear Operators (threshold tracking)

| Operator | Module | State | Cost per tick |
|---|---|---|---|
| `Distinct` | `operators/distinct.rs` | `count: AHashMap<RowKey, i64>` | O(|ΔA| · hash) |
| `Aggregate(SUM,COUNT,AVG)` | `operators/aggregate.rs` | `running: AHashMap<GroupKey, AggState>` | O(|ΔA| · hash) |
| `Aggregate(MAX,MIN)` | `operators/aggregate.rs` | `multiset: AHashMap<GroupKey, BTreeMap<Value, i64>>` | O(|ΔA| · log|group|) |
| `Recursive` | `operators/recursive.rs` | inner view state | O(fixpoint_iters · |delta|) |

### Aggregate Retraction Protocol

For any aggregate, when a row changes:
1. Compute old aggregate value → emit `(group_key, old_agg, -1)` as retraction.
2. Update running state with `(row, weight)` from delta.
3. Compute new aggregate value → emit `(group_key, new_agg, +1)` as insertion.

This produces correct output even with negative-weight inputs (deletions and updates from CDC).

### Distinct Threshold Protocol

For each `(row, weight)` in delta:
1. `old_count = count.get(row)` (default 0)
2. `new_count = old_count + weight`
3. `count.insert(row, new_count)`
4. If `old_count <= 0 && new_count > 0`: emit `(row, +1)` — row becomes present.
5. If `old_count > 0 && new_count <= 0`: emit `(row, -1)` — row disappears.
6. Otherwise: no output (row presence unchanged).

---

## SQL API

### New DDL Statements

```sql
-- 1. Source table with LATENESS annotation
CREATE TABLE orders (
    id      BIGINT        NOT NULL,
    amount  DECIMAL(10,2) NOT NULL,
    ts      TIMESTAMP     NOT NULL LATENESS INTERVAL '5' MINUTES,
    PRIMARY KEY (id)
) WITH (
    'connector'   = 'kafka',
    'topic'       = 'orders-cdc',
    'format'      = 'debezium-json',
    'incremental' = 'true'
);

-- 2. Incremental view (maintained as change stream)
CREATE INCREMENTAL VIEW order_totals AS
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id;

-- 3. Recursive view (fixed-point iteration)
DECLARE RECURSIVE VIEW reachable(src BIGINT, dst BIGINT);
CREATE INCREMENTAL VIEW reachable AS
    SELECT src, dst FROM edges
    UNION ALL
    SELECT r.src, e.dst FROM reachable r JOIN edges e ON r.dst = e.src;

-- 4. Materialized view (queryable snapshot)
CREATE MATERIALIZED VIEW order_totals_snap AS
    SELECT * FROM order_totals;

-- 5. Sink: write only changed rows
CREATE SINK order_sink
    FROM order_totals
    INTO ICEBERG TABLE 's3://bucket/orders'
    WITH ('write_mode' = 'merge', 'merge_key' = 'customer_id');

-- 6. Refresh control
REFRESH INCREMENTAL VIEW order_totals;

-- 7. Drop (CASCADE drops dependent views and sinks)
DROP INCREMENTAL VIEW order_totals CASCADE;
```

### SQL Parsing Approach

All new DDL is intercepted in `SqlEngine::sql()` before DataFusion sees it, using the same pattern as `CREATE LIVE TABLE`:

```
1. Trim, uppercase-check for "CREATE INCREMENTAL VIEW" / "DECLARE RECURSIVE VIEW" /
   "CREATE MATERIALIZED VIEW" (when view body references an incremental view) /
   "CREATE SINK ... FROM" / "REFRESH INCREMENTAL VIEW" / "DROP INCREMENTAL VIEW"
2. Use `sqlparser` AST to extract: view name, body SQL, table annotations
3. Compile body SQL to DataFusion LogicalPlan via context.create_logical_plan()
4. Walk LogicalPlan → build IncrementalPlan (Vec<Box<dyn IncrementalOp>>)
5. Register in SqlEngine::incremental_view_registry
6. Return empty RecordBatch (DDL returns no rows)
```

### LATENESS Annotation Parsing

`LATENESS INTERVAL '5' MINUTES` is a column-level attribute extension. Parse at the `CREATE TABLE` level:
- Scan `WITH` options for `'incremental' = 'true'`
- Scan column definitions for `LATENESS INTERVAL ...` suffix
- Build `Vec<LatenessSpec { column: String, duration_ms: i64 }>` per table
- Store in `IncrementalTableSpec` alongside the connector config

---

## Rust API

### Entry Point

```rust
// No breaking changes to existing Session API
let flow = session
    .incremental()                         // → IncrementalFlowBuilder
    .source("orders", kafka_source)
    .with_lateness("ts", Duration::minutes(5))
    .define(|sources| {
        let orders = sources.table("orders");
        let totals = orders
            .filter(col("amount").gt(lit(0)))
            .aggregate(vec!["customer_id"], vec![sum("amount").alias("total")]);
        Ok(vec![("order_totals", totals)])
    })
    .sink("order_totals", iceberg_sink)
    .build()?;                             // → IncrementalFlow

// Manual step
flow.step().await?;
let output: ChangeBatch = flow.view_output("order_totals").await?;

// Continuous
flow.run_until_stopped().await?;

// Query snapshot (materialized views)
let snap: RecordBatch = flow.snapshot("order_totals").await?;
```

### Type Hierarchy

```
Session
  .incremental() → IncrementalFlowBuilder
    .source(name, connector) → Self
    .with_lateness(col, duration) → Self
    .define(fn) → Self
    .sink(view, connector) → Self
    .build() → Result<IncrementalFlow>

IncrementalFlow
  .step() → Result<StepSummary>
  .run_until_stopped() → Result<()>  (async, runs until stop() called)
  .stop()
  .view_output(name) → Result<ChangeBatch>
  .view_output_stream(name) → impl Stream<Item=ChangeBatch>
  .snapshot(name) → Result<RecordBatch>   (materialized views only)
  .view_names() → Vec<String>

StepSummary { input_rows: usize, output_changes: usize, duration_ms: u64 }
```

---

## Python API

```python
import krishiv as ks
import krishiv.incremental as ksi

session = ks.Session.embedded()
flow = session.incremental_flow("my_flow")

# Source table
orders = flow.source_table("orders",
    schema=ks.Schema([...]),
    connector=ks.KafkaSource(...),
    lateness_column="ts",
    lateness="5 minutes")

# Functional view
@flow.view("order_totals", behavior_version=1)
def order_totals(orders):
    return orders.filter("amount > 0").group_by("customer_id").sum("amount")

# SQL view
flow.sql_view("enriched", """
    SELECT o.id, p.name FROM orders o JOIN products p ON o.product_id = p.id
""")

# Sink
flow.sink("order_totals", ks.IcebergSink("s3://...", merge_key=["customer_id"]))

# Run
await flow.step_async()                     # one tick
await flow.run_async()                      # continuous
output = flow.view_output("order_totals")   # pyarrow.RecordBatch with _weight col
snap = flow.snapshot("order_totals")        # pyarrow.RecordBatch (materialized)

# Change stream
async for change_batch in flow.change_stream("order_totals"):
    inserts = change_batch.filter_positive()   # weight > 0
    deletes = change_batch.filter_negative()   # weight < 0
```

---

## Phase TODOs

### Phase 1 — `krishiv-delta` Core  [DONE in this session]

- [x] `Cargo.toml` with workspace deps (arrow, ahash, twox-hash, thiserror)
- [x] `error.rs`: `DeltaError` type
- [x] `change_batch.rs`: `ChangeBatch`, `Weight`, `WEIGHT_COLUMN`, `from_inserts()`, `from_deletes()`, `from_update()`, `negate()`, `concat()`, `consolidate()`, `filter_positive()`, `filter_negative()`, `data_batch()`, `weights()`
- [x] `trace.rs`: `Trace`, 8-level Spine, `insert()`, `probe_by_keys()`, `consolidate()`, `gc_below_watermark()`
- [x] `lateness.rs`: `WatermarkTracker`, `LatenessSpec`, `SourceWatermark`, update/query methods
- [x] `behavior_version.rs`: `LogicFingerprint`, `compute_fingerprint()`, `MemoKey`
- [x] `coalesce.rs`: `CoalescingMap<K,V>`, `insert()`, `drain()`, `len()`
- [x] `operators/map.rs`: `MapOp` (fn + schema transform)
- [x] `operators/filter.rs`: `FilterOp` (predicate on data columns)
- [x] `operators/consolidate.rs`: `ConsolidateOp` (sort → sum weights → drop zeros)
- [x] `operators/join.rs`: `IncrementalJoinOp` (two Traces, bilinear protocol)
- [x] `operators/aggregate.rs`: `IncrementalAggOp` (SUM/COUNT/AVG + retraction; MAX/MIN via BTreeMap)
- [x] `operators/distinct.rs`: `IncrementalDistinctOp` (threshold HashMap)
- [x] `operators/recursive.rs`: `RecursiveOp` (fixed-point loop, cycle guard)
- [x] `view.rs`: `IncrementalView`, `IncrementalViewRegistry`
- [x] `plan.rs`: `IncrementalPlan`, `IncrementalOp` trait, DataFusion plan-walker

### Phase 2 — `krishiv-dataflow` + `krishiv-state`  [DONE in this session]

- [x] `OperatorConfig::behavior_version: u64` + `with_behavior_version()` builder
- [x] `krishiv-state/src/incremental_trace.rs`: `TraceStateNamespace`, serialize/deserialize `ChangeBatch` to Arrow IPC

### Phase 3 — SQL Surface  [DONE in this session]

- [x] `krishiv-sql/src/incremental_view.rs`: `IncrementalViewRegistry`, `IncrementalViewSpec`, DDL execute functions
- [x] `krishiv-sql/src/lib.rs`: intercept `CREATE INCREMENTAL VIEW`, `DECLARE RECURSIVE VIEW`, `REFRESH INCREMENTAL VIEW`, `DROP INCREMENTAL VIEW`, `CREATE SINK ... FROM`
- [x] LATENESS annotation parser in `incremental_view.rs`

### Phase 4 — Source Watermark  [TODO]

- [ ] `krishiv-scheduler/src/source_watermark.rs`: `SourceWatermarkStore`, per-source ordinal tracking in RocksDB
- [ ] Coordinator tick: compare `Source::current_offset()` vs stored watermark; skip task assignment if no advancement
- [ ] `CdcRouter` emits `ChangeBatch` via `DeltaEntry → ChangeBatch` bridge
- [ ] `CoalescingMap` integration in `CdcRouter::poll_and_route`

### Phase 5 — Rust API  [DONE in this session]

- [x] `krishiv-api/src/incremental_flow.rs`: `IncrementalFlowBuilder`, `IncrementalFlow`, `StepSummary`
- [x] `Session::incremental()` entry point

### Phase 6 — Python API  [DONE in this session]

- [x] `krishiv-python/src/incremental.rs`: `PyIncrementalFlow`, `PyChangeBatch`, `@flow.view`, `flow.sql_view()`, `step_async()`, `snapshot()`
- [x] Wire into `krishiv-python/src/lib.rs`

### Phase 7 — Recursive Views  [TODO]

- [ ] `operators/recursive.rs` full fixed-point implementation with cycle guard
- [ ] `DECLARE RECURSIVE VIEW` SQL: forward-declare, parse body, detect self-reference
- [ ] Auto-DISTINCT on recursive views
- [ ] Integration test: transitive closure over 5-node graph

### Phase 8 — Integration + Validation  [TODO]

- [ ] `cargo check --workspace` clean
- [ ] `cargo test -p krishiv-delta --lib` — unit tests for all operators
- [ ] Integration test: Kafka CDC source → incremental join + aggregate → Iceberg MERGE INTO sink
- [ ] Integration test: behavior_version bump → Trace cleared → recompute
- [ ] Integration test: LATENESS GC → old state freed
- [ ] Benchmark: incremental join vs. full re-scan on 10M-row table with 1% delta

### Phase 9 — Source Watermark Scheduler Integration  [TODO]

- [ ] `SourceWatermarkStore` in scheduler
- [ ] Skip-if-unchanged optimization in `coordinator_tick`
- [ ] `Source::ordinal()` protocol formalized in connector trait
- [ ] Tiered detection: ordinal → push (CDC/Kafka offset) → scan (mtime/snapshot_id)

### Phase 10 — Production Hardening  [TODO]

- [ ] Trace spill to disk when in-memory size > configurable limit
- [ ] Per-operator memory budget in `IncrementalFlow` config
- [ ] MAX/MIN aggregate via multiset BTreeMap — correctness + GC tests
- [ ] Distributed Trace sharding (partition by join key, one Trace shard per executor)
- [ ] Prometheus metrics: delta size per tick, operator latency, Trace memory usage
- [ ] `UPDATE TABLE orders SET ... WHERE ...` CDC bridging via `before`/`after` payload
- [ ] `CREATE SINK ... WITH ('emit_retractions' = 'true')` for Kafka sinks that forward negative-weight rows as tombstones

---

## Testing Plan

### Unit Tests (in `krishiv-delta`)

| Test | Covers |
|---|---|
| `change_batch_from_inserts` | weight column all +1 |
| `change_batch_from_deletes` | weight column all -1 |
| `change_batch_from_update` | before -1, after +1 |
| `change_batch_negate` | flip signs |
| `change_batch_consolidate_cancels` | +1 and -1 for same row → empty |
| `change_batch_consolidate_sums` | two +1 for same row → +2 |
| `trace_insert_and_probe` | insert rows, probe by key |
| `trace_gc_removes_old_entries` | watermark GC |
| `map_op_preserves_weights` | map fn doesn't change weights |
| `filter_op_drops_non_matching` | filter removes rows, preserves weights |
| `join_op_bilinear_delta_left` | ΔA ⋈ B_trace produces correct output |
| `join_op_bilinear_delta_right` | A_trace ⋈ ΔB produces correct output |
| `join_op_bilinear_both` | simultaneous deltas |
| `agg_op_sum_insert` | SUM increases on insert |
| `agg_op_sum_delete_retraction` | SUM decreases on delete, emits retraction |
| `agg_op_avg_update` | AVG correct after update (before -1 + after +1) |
| `distinct_op_threshold` | emit +1 on first occurrence, -1 when count drops to 0 |
| `distinct_op_no_output_on_multiset` | weight +2 doesn't re-emit |
| `behavior_version_change_invalidates` | new fingerprint → Trace cleared |
| `coalesce_map_collapses_rapid_updates` | N updates to same key → 1 entry |
| `lateness_drops_old_records` | records below watermark discarded |
| `logic_fingerprint_stable` | same uid + version → same fingerprint |
| `incremental_view_registry_lookup` | register and retrieve view |

### Integration Tests

| Test | Covers |
|---|---|
| `end_to_end_cdc_to_aggregate` | Kafka CDC → aggregate view → Iceberg MERGE |
| `incremental_join_two_sources` | CDC orders + CDC products → enriched view |
| `behavior_version_recomputes` | Bump version → stale state cleared → correct output |
| `lateness_gc_frees_state` | Time advances → old Trace state GC'd → memory reduced |
| `recursive_transitive_closure` | 5-node graph → reachable pairs |
| `multi_tick_watermark_advance` | Ordinal advances → skipped ticks where no change |

---

## Known Limitations and Future Work

### Current Limitations

1. **Trace is in-memory only** (Phase 1): no disk spill. Large join inputs that exceed memory will panic on allocation. Phase 10 adds spill-to-disk via `krishiv-state` RocksDB.

2. **MAX/MIN aggregate is O(|group|)** on each update due to BTreeMap multiset. For high-cardinality groups with frequent MAX/MIN queries, this is expensive. Consider a segment tree or min-heap approach.

3. **No distributed Trace sharding** (Phase 10): the `IncrementalJoinOp` Traces are local to one process. For datasets that exceed single-node memory, distributed sharding is required (planned, uses existing shuffle infrastructure).

4. **Recursive views** are not yet implemented (Phase 7). The `RecursiveOp` stub exists but the fixed-point loop requires forward-declaration resolution.

5. **Source watermark scheduler integration** (Phase 9) is not yet wired. Skip-if-unchanged optimization is planned but not active; all scheduled ticks currently evaluate regardless of source advancement.

6. **Sink `emit_retractions`** for Kafka tombstone forwarding is stubbed but not wired to the `KafkaSink` implementation.

### Future Work

- **Feldera-style SQL-to-DBSP lowering**: formal I/D lifting pass on the DataFusion LogicalPlan for provably correct incrementalization of arbitrary SQL.
- **Speculative emit**: emit partial aggregate results before window closes (requires WATERMARK semantic, not just LATENESS).
- **Streaming + Incremental composition**: allow incremental views to join with streaming sources (hybrid mode).
- **Python `@flow.view` graph analysis**: detect join columns, auto-configure Trace key columns.
- **Iceberg snapshot-as-ordinal**: use Iceberg snapshot IDs as the source ordinal for file-based incremental sources.
- **CDC schema evolution**: when CDC source schema changes, migrate Trace state via the existing `StateMigrationFn` protocol.
- **Prometheus metrics**: `krishiv_delta_trace_rows`, `krishiv_delta_step_duration_ms`, `krishiv_delta_output_changes`, per-view.
