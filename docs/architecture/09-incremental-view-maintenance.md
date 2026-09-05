# Incremental view maintenance

The incremental engine keeps SQL views up to date under inserts and deletes
without recomputing them. It is built on Z-sets (DBSP/Feldera): every row
carries an integer weight, a change is a weighted batch, and operators are
functions on weighted relations. `krishiv-delta` is the algebra;
`krishiv-ivm` is the driver; the coordinator's `IvmJobRegistry` hosts jobs.

## Algebra (`krishiv-delta`)

| Concept | Type | Notes |
|---|---|---|
| weighted batch | `DeltaBatch` (`RecordBatch` + `_weight: Int64`) | `+1` insert, `−1` retract, `0` cancelled; `consolidate_batch` sums weights per row |
| accumulated state | `Trace` | 8-level Spine; batches enter at level 0, a level with 4 batches is merged and promoted; each batch has a lazily built `KeyIndex` so a probe costs O(probe keys · log rows + matches), not O(trace) |
| source state | `SourceState` | the materialised relation (chunked, so an append is O(Δ) not O(n)) **plus** a *deficit*: rows whose net weight is negative. `DELETE 42` before `INSERT 42` ends with 42 absent, as a Z-set says it must |
| lateness | `LatenessSpec`, `WatermarkTracker` | per-source column + bound; late **insertions** are dropped at ingestion and counted; late retractions are always applied (dropping one would strand its insertion forever); watermark GC applies to join traces only — aggregate/distinct state has no time dimension to prove eviction |
| logic versioning | `LogicFingerprint`, `MemoKey` | hash of (operator uid, behaviour version); a bump invalidates cached traces |
| coalescing | `CoalescingMap` | many updates to one key within a tick collapse to the latest |

Operators: `map`/`project`, `filter`, `consolidate` (linear, stateless);
`join` (bilinear over two traces: ΔA ⋈ B + A ⋈ ΔB); `aggregate`, `distinct`,
`topn` (non-linear, stateful). `differentiate` turns a snapshot sequence into
deltas; `IntegrateOp` / `apply_delta` do the reverse. The Z-set laws are
property-tested (`proptest_zset`).

## Planning (`krishiv-ivm::plan`)

`build_view_plan` parses a view's SQL with DataFusion and pattern-matches an
O(Δ) plan:

| Pattern | `ViewPlan` |
|---|---|
| single-source `GROUP BY` aggregate, optional `WHERE` | `Aggregate` (filter applied to the source delta) |
| two-source INNER / LEFT OUTER equi-join, per-side `WHERE` conjuncts, INNER band residuals (`BETWEEN`), a projection above | `Join` |
| single-source `DISTINCT` | `Distinct` |
| `ORDER BY … LIMIT` | `TopN` |
| anything else — subqueries, multi-way joins, window functions, non-equi cross-side predicates, RIGHT/FULL OUTER | `DiffBased`: re-execute the SQL, diff against the previous output |

`decompose.rs` widens what plans incrementally: a multi-operator query over a
**single source** is cut into a linear chain of single-operator hops
(`__ivm_v_h0`, …), each re-rooted *structurally* (the node's input replaced by
a scan of the hop below, aliased to the original table name so qualified
references stay valid) and verified directly by the matchers. Joins are
refused and a chain where any hop fails is discarded whole — a partially cut
query is slower than an uncut one. Every hop carries an explicit projection
(a bare `Filter` unparses to zero columns).

Coverage of the registered corpus is a gated number, not a claim: 41/44
verbatim (TPC-H 22/22, NEXMark 19/22, the remaining 3 semantics-bound), and
`krishiv-bench` re-runs the gate.

## The flow (`krishiv-ivm::flow`)

`IncrementalFlow` holds sources, views in topological order (Kahn's algorithm
over SQL references), per-view plans, and subscribers.

- **feed**: apply lateness, route the delta into `SourceState`, mark the
  source dirty.
- **step**: for each dirty view in order, run its plan on the delta (or the
  DiffBased fallback), difference, publish non-empty deltas to `watch`
  subscribers, update `ViewDeltaStats` (logical inserts/retracts, monotonic).
  Views touching no dirty input are skipped and reuse their snapshot.
  Recursive views iterate to a fixpoint (`MAX_FIXPOINT_ITERS` = 100).
- **StepSummary** reports rows, inserted/retracted counts, active views,
  `degraded_views` (fell back to DiffBased this tick), and `errored_views`
  (skipped with the logged error; the step never panics).
- **Dedup**: opt-in per source, a row-hash set capped at 10 M entries,
  evicting 1 % at a time — never clearing, which silently re-admitted every
  seen row.
- **Checkpoints**: `checkpoint_full` (everything) and delta checkpoints (only
  the slice since the last one); `RetainedState` counts every retained map
  so growth is observable.
- **Memory**: each flow's DataFusion session has a spill-capable budget
  (`spill.rs`, `ivm_memory_limit_bytes`); a partitioned flow *divides* it
  across shards.
- **Streaming bridge**: `feed_snapshot` differentiates micro-batch output
  into deltas, so a windowed stream can feed a view.

## Partitioning

`PartitionedIncrementalFlow` shards a flow by a key column across N
independent flows, routing with the shared keyed hash. It is correct for views
whose output for a key depends only on that key's rows — per-key aggregates,
filters, projections, equi-joins on the shard key — and
`partition_key_from_sql` proves that before the registry chooses it. Shard
count is `min(available_parallelism, 8)` or `KRISHIV_IVM_SHARDS`; it is
core-derived because views are registered before any bytes exist. The routed
key's hash class is pinned per flow so an `Int32`→`Int64` drift cannot split a
group across shards (in-process only; a restart re-arms it — recorded as a
residual).

## Hosting and distribution

`IvmJobRegistry` (`krishiv-scheduler::ivm`) holds each job's flow in the
coordinator — the single source of truth in every mode — and persists it as a
versioned `PersistedIvmJob` (views, shape, `checkpoint_full` as base64,
`pinned_single`, delta-checkpoint flag) to the metadata store (chunked and
zstd-compressed in etcd, `04`).

With live executors, a single-flow job runs **executor-resident**: the flow
ships once at `delta:attach:`, each tick sends only input deltas plus a fence
and receives per-view output deltas, which the coordinator mirrors onto its
own flow (`apply_remote_tick`). Any failure re-feeds pending deltas and
computes centrally. Partitioned jobs always compute centrally (shards are
already parallel in-process). The wire has two dialects (`IVMD1`, `IVMD2`) and
a capability echo so mixed-version rollouts cannot exchange an unreadable
blob (`05`).

## Surfaces

| Surface | Operations |
|---|---|
| SQL | `CREATE INCREMENTAL VIEW … AS SELECT …` (with `LATENESS`), `CREATE MATERIALIZED VIEW` |
| CLI | `krishiv ivm create|feed|step|watch|status|drop` |
| HTTP | `/api/v1/ivm/jobs`, `/…/{job}/views`, `/…/feed`, `/…/step`, `/…/watch`, `/…/snapshot` |
| Rust | `IncrementalDataFrame`, `IncrementalEngine` (a `ComputeEngine`; CDC `ChangelogBatch` → `DeltaBatch` via `delta_from_changelog`), `Session::ivm_job` |
| Python | `IncrementalDataFrame`, `IvmJob`, `DeltaBatch`, `StepSummary`, `ViewError` |

Sinks for incremental output must be retraction-aware
(`ConsolidatingSinkProvider`, or `primary_key` upsert delivery on the
`SinkSpec`); the raw connector sink returns a typed error on the first
retraction.

## Streaming versus incremental

| | Streaming (`08`) | Incremental (this) |
|---|---|---|
| input | append-only events with event time | inserts **and** deletes (CDC, upserts) |
| output | closed windows, emitted once | the current view, as deltas |
| state | per (key, window) | per view: traces and aggregates |
| time | watermarks close windows | lateness bounds ingestion; no windows |
| best fit | time-bucketed aggregates, pattern detection, joins within a time bound | materialised views over changing tables, TPC-H-shaped analytics kept fresh |

## Related

- `../engineering-log/ivm-audit-register.md` — the IVM audit (every finding
  above with an `IVM-AUD-*` tag comes from it).
- `05` (resident protocol), `04` (persistence), `10` (CDC source).
