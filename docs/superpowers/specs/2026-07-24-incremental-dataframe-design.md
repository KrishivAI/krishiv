# IncrementalDataFrame — one DataFrame surface across all three execution modes

- **Date:** 2026-07-24
- **Status:** Design (awaiting review)
- **Area:** `crates/krishiv-python` (client surface) + a thin `crates/krishiv-api` plumbing addition
- **Related:** [[streaming-api-gap-analysis]] (the DataStream→StreamingDataFrame unification this mirrors), Phase 57 (executor-resident IVM state), Phase 61 (unified DataFrame API)

## 1. Goal

Give the delta-batch / incremental-view-maintenance (IVM) mode the **same DataFrame
authoring surface** already used for batch and streaming, so a user builds a query
**once** with the normal DataFrame verbs and then chooses the execution mode by a
conversion — exactly as `df.to_streaming()` already does for streaming today.

Secondary goal: **consolidate the existing delta surfaces** (`IvmJob`, `LiveTable`)
so there is one primary fluent surface per mode, with **zero functionality lost**.

## 2. Non-goals

- No change to the incremental engine's semantics or performance. This is a
  client-surface + thin-plumbing change; the IVM engine (`IvmJob`, `DeltaBatch`,
  executor-resident state) is reused as-is.
- Not touching the **platform/server** "live tables" pipeline feature
  (`kind:pipeline`, CDC→live tables). Retirement here is scoped to the **Python
  client `LiveTable` class** only.
- No new SQL syntax. The view is defined by the DataFrame's existing logical plan.

## 3. Current state (analysis)

The delta-batch mode is imperative and handle-based, with three overlapping layers:

| Layer | Construction | Shape | Verdict |
|---|---|---|---|
| `DeltaBatch` | `from_inserts/deletes/update/cdc/weighted` | Arrow batch + per-row integer weights (Z-set: +1 insert / −1 delete) | Keep — correct primitive |
| `IvmJob` | `s.ivm(name)` | `register_view(sql)` → `feed(src, delta)` → `step()` → `snapshot(view)`; `checkpoint/restore` | Keep as low-level engine handle |
| `LiveTable` | `s.live_table(name, sql)` | `ingest_row(id, op)` / `refresh()` / `change_feed()` / `drop()` | **Retire** (Python client), migrate to new surface |

There is no `DataFrame`-style handle. Batch has `DataFrame`, streaming has the
unified `StreamingDataFrame`, delta has only the imperative `IvmJob` loop — the same
situation streaming was in before it was unified.

Key feasibility facts (verified):
- `DataFrame.to_streaming()` already exists and hands the DataFrame's inner logical
  plan to the streaming builder — the "same plan, pick a mode" pattern already ships.
- The engine works in `krishiv_plan::LogicalPlan` internally (`lower_to_physical`),
  so an IVM view can be defined from a plan; today `register_view` only takes SQL.

## 4. Design

### 4.1 The unified surface (Approach A — "convert" model)

Author once with the normal verbs on `DataFrame`; the mode is a conversion:

```python
df = s.table("orders").group_by("customer_id").agg(total=F.sum("amount"))

df.collect()          # BATCH   — one-shot                → DataFrame          (today)
df.to_streaming()     # STREAM  — continuous / windows    → StreamingDataFrame (today)
df.to_incremental()   # DELTA   — incrementally maintained → IncrementalDataFrame (NEW)
```

The three handles share **one** stateless-verb implementation and **one** plan/spec
source of truth (the Phase-4 anti-drift lesson from streaming), so `select / filter /
withColumn / join / groupBy / agg` behave identically before any conversion.

Because the whole plan is defined on the `DataFrame` **before** `to_incremental()`,
`IncrementalDataFrame` is a **feed/read handle**, not a transform builder (unlike
`StreamingDataFrame`, which re-exposes pre-window stateless verbs). This keeps its
surface small and unambiguous.

**Source binding.** The scan leaves of the DataFrame's plan (`s.table("orders")`,
`s.stream(...)`, registered tables) become the delta-fed **sources**, identified by
name. `to_incremental()` requires the leaves to be named/registered sources that can
receive deltas — analogous to `to_streaming()` requiring a stream source. A plan whose
leaf is a one-shot batch file scan (no name to feed) is rejected with a clear error.

### 4.2 `IncrementalDataFrame` API

```python
iv = df.to_incremental()                # registers the view from df's plan

# ── input (feed changes) ──
iv.apply(DeltaBatch.from_inserts(b))    # explicit Z-set change — full power
iv.apply(delta, source="orders")        # name the source for multi-source plans
iv.insert(batch)                        # convenience: wrap as +1 DeltaBatch
iv.delete(batch)                        # convenience: wrap as −1 DeltaBatch
iv.upsert(batch, keys=["customer_id"])  # convenience: retract-old + insert-new
iv.apply_cdc(cdc_event)                 # convenience over DeltaBatch.from_cdc
with iv.transaction():                  # feed multiple sources → ONE atomic tick
    iv.apply(d1, source="orders"); iv.apply(d2, source="returns")

# ── output (read results) ──
iv.snapshot()                           # full materialized result (Arrow table) — "complete"
async for change in iv.changes():       # change-feed of OUTPUT deltas — "update"
    ...                                  # each item is an output DeltaBatch

# ── durability / lifecycle ──
iv.checkpoint(); iv.restore()           # delegate to IvmJob checkpoint/restore
iv.drop()                               # tear down the view
iv.name                                 # view identity
```

- **Stepping:** `apply/insert/delete/upsert` auto-step by default (feed-and-step).
  `transaction()` buffers feeds across sources and issues one tick on exit — atomic
  multi-source updates.
- **Output modes:** both `snapshot()` (full state) and `changes()` (output deltas)
  are always available — Spark's complete and update modes, exposed as two reads.
- **Async parity:** `changes()` is an async iterator (matches `StreamingDataFrame`);
  `snapshot()` is sync. The output-delta change-feed is what makes views composable
  (one view's `changes()` can feed another view's `apply()` — a live view DAG, a
  natural follow-up capability but not required in v1).

### 4.3 Engine plumbing (the one real addition)

`to_incremental()` must define the IVM view from the DataFrame's plan rather than a
SQL string. Add `IvmJob::register_view_from_plan(name, LogicalPlan)` (krishiv-api)
alongside the existing SQL `register_view`; the SQL path becomes a thin wrapper that
parses to a plan and calls it. `to_incremental()` (pyo3) hands `df`'s inner plan to
this — the same handoff `to_streaming()` already performs. No IVM-semantics change.

### 4.4 Cleanup / consolidation (zero functionality lost)

- **`DeltaBatch`** — unchanged.
- **`IvmJob`** — retained as the low-level engine handle *under* `IncrementalDataFrame`
  (as streaming operators sit under `StreamingDataFrame`). Still public for advanced
  multi-view jobs; the fluent handle delegates to it.
- **`LiveTable` (Python client)** — **retired**, mirroring the DataStream retirement:
  delete the pyo3 class, `Session.live_table()`, its export/stub, and migrate callers.
  Migration mapping (proves no functionality lost):

  | LiveTable | IncrementalDataFrame |
  |---|---|
  | `s.live_table(name, sql)` | `s.sql(sql).to_incremental()` |
  | `.ingest_row(id, "insert")` | `.insert(...)` |
  | `.ingest_row(id, "delete")` | `.delete(...)` |
  | `.ingest_row(id, "update")` | `.upsert(...)` |
  | `.refresh()` | `.step()` (auto with `apply`) |
  | `.change_feed()` | `.changes()` |
  | `.drop()` | `.drop()` |

  `test_live_table.py` / `test_change_feed.py` are rewritten against
  `IncrementalDataFrame` (kept as the no-functionality-lost regression proof).

### 4.5 Distributed + mode inheritance

`to_incremental()` inherits the session's execution mode (embedded vs distributed),
same as the other two conversions. Distributed IVM already exists (Phase 57
executor-resident state), so the distributed path reuses it; `snapshot()`/`changes()`
route to the coordinator in distributed mode.

## 5. Testing strategy

1. **Oracle parity:** feed a delta sequence into `df.to_incremental()`; `snapshot()`
   must equal a full batch recompute of the *same* `df` over the accumulated data.
   Covers agg, groupBy, join, filter, projection.
2. **Change-feed correctness:** the union of emitted output deltas reconstructs the
   snapshot; retractions net correctly on updates.
3. **Transaction atomicity:** multi-source `transaction()` produces one tick with the
   combined effect; partial feeds are not observable mid-transaction.
4. **Migration regression:** every retired `LiveTable`/`change_feed` behavior has an
   equivalent `IncrementalDataFrame` test that passes (no functionality lost).
5. **Cross-mode consistency:** the same `df` yields identical results via `collect()`
   (batch) and via `to_incremental()` snapshot after feeding the same rows as inserts.
6. **Distributed:** validate `to_incremental()` live on the 3-node k8s cluster (feed
   real deltas, compare snapshot to oracle), like the streaming validation.

## 6. Risks & mitigations

- **Plan leaves not feedable** (batch file scan) → detect at `to_incremental()` and
  raise a clear error naming the offending leaf.
- **Drift between the three handles** → single shared stateless-verb impl + single
  plan/spec source of truth (the fix already applied to streaming; enforce with a
  cross-mode consistency test).
- **`LiveTable` retirement breaking external users** → it is a young client class;
  migration is mechanical and mapped 1:1 above. Scope strictly to the Python client
  (server live-tables untouched).
- **IVM full-recompute cost at small scale** (known finding, task #102) is unchanged
  by this work — it is a surface/ergonomics change, not an execution change.

## 7. Rollout (phased)

1. Engine plumbing: `register_view_from_plan` in krishiv-api (+ unit test).
2. pyo3 `IncrementalDataFrame` + `DataFrame.to_incremental()` (feed/read/step, async
   `changes()`), exported top-level + `.pyi`.
3. Retire Python `LiveTable`; migrate examples/tests per the mapping table.
4. Oracle + cross-mode + migration tests green (embedded).
5. Distributed validation on k8s; docs + examples updated to the unified triad.

## 8. Open questions (for review)

- Should v1 include the composable **view-DAG** (one view's `changes()` auto-wired to
  another's `apply()`), or defer it as a follow-up? (Recommendation: defer; keep v1 to
  the single-view surface + cleanup.)
- `transaction()` as a context manager (shown) vs an explicit `begin/commit` pair?
  (Recommendation: context manager — safer, Pythonic.)
