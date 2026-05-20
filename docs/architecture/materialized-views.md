# Materialized Views Baseline Architecture

## Design Decision

Materialized views in R10 use **refresh-on-commit** semantics. A materialized view is a named, persistent query result stored in `RedbStateBackend`. This is the simplest correct model: no incremental maintenance, no streaming refresh, no cross-job consistency; just a cached query result that becomes stale when the backing table advances.

Incremental maintenance and streaming materialized views are deferred to R11.

---

## Interface: `MaterializedViewDefinition`

Defined in `krishiv-sql`:

```rust
#[non_exhaustive]
pub struct MaterializedViewDefinition {
    pub name: String,
    pub query: String,                    // SQL SELECT statement
    pub refresh_policy: RefreshPolicy,
    pub partition_columns: Vec<String>,   // empty = single partition
}

#[non_exhaustive]
pub enum RefreshPolicy {
    OnCommit,   // refresh triggered by upstream table write
    Manual,     // refresh only via explicit REFRESH MATERIALIZED VIEW
}
```

Views are registered via `SqlEngine::create_materialized_view()`. Registration persists the definition to `RedbStateBackend` under the key `matview::<name>::definition`.

---

## Storage

Materialized view data is stored in `RedbStateBackend` keyed by:

```
matview::<view_name>::partition::<partition_key>  →  Arrow IPC batch (bytes)
```

For unpartitioned views (`partition_columns` is empty), the partition key is the constant `__unpartitioned__`.

Each stored entry also carries:
- `schema_version: u32` (current: 1)
- `written_lsn: u64` — the write LSN of the backing table at the time the view was last refreshed

The `RedbStateBackend` ACID guarantees ensure that a partial refresh never replaces a complete prior result: the write is transactional per partition key.

---

## Refresh Trigger

When an upstream table receives a write (Iceberg commit or local Parquet write), the write path emits a `TableCommitEvent{table_name, new_lsn}` to the coordinator.

The coordinator checks whether any registered `OnCommit` materialized view references that table. For each matching view, it schedules a `RefreshMaterializedView{view_name, target_lsn}` task. The task:

1. Executes the view's SQL query via `SqlEngine`.
2. Collects the output as Arrow `RecordBatch` slices.
3. Writes each batch to `RedbStateBackend` under the partition key(s).
4. Updates the `written_lsn` metadata.

Refresh tasks run on a dedicated thread pool to avoid blocking query execution.

---

## Query Rewrite

`SqlEngine` intercepts queries against registered materialized views before planning:

1. Parse the incoming SQL to identify the top-level table reference.
2. Look up whether a materialized view with that name exists and has `RefreshPolicy::OnCommit`.
3. Compare the view's stored `written_lsn` against the current write LSN of the backing table.
   - If equal (fresh): read the stored Arrow IPC batches from `RedbStateBackend` and return them directly. DataFusion is not invoked.
   - If stale (backing LSN advanced): execute the query via DataFusion, store the new result, update `written_lsn`, then return.
4. For `RefreshPolicy::Manual` views, always serve the stored result regardless of staleness (may be stale).

This rewrite is transparent to the caller: the SQL interface is unchanged.

---

## Staleness Contract

- A view is **fresh** until the backing table's write LSN advances past `written_lsn`.
- Staleness is acceptable within a single commit window. Queries may observe a view that is one commit behind the current table state.
- There is no read-your-writes guarantee across a write followed immediately by a view query within the same session. If strict consistency is needed, use `RefreshPolicy::Manual` with an explicit `REFRESH MATERIALIZED VIEW <name>` before the query.

---

## R10 Limitations

| Limitation | Deferred To |
|---|---|
| Multi-table views (joins across multiple source tables) | R11 |
| Incremental maintenance (apply delta, not full recompute) | R11 |
| Streaming materialized views (continuous refresh from a stream) | R11 |
| Cross-job consistency guarantee (view refresh is atomic with the job that wrote the table) | R11 |
| Partial refresh per partition on row-level delete | R11 |

In R10, a refresh always recomputes the entire view. For large tables this is expensive. The `RefreshPolicy::Manual` option allows callers to control refresh timing to avoid unnecessary recomputes.

---

## SQL Surface

```sql
-- Register a materialized view
CREATE MATERIALIZED VIEW orders_summary AS
  SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
  FROM orders
  GROUP BY status;

-- Manual refresh
REFRESH MATERIALIZED VIEW orders_summary;

-- Query (transparent; served from cache if fresh)
SELECT * FROM orders_summary WHERE status = 'pending';

-- Drop
DROP MATERIALIZED VIEW orders_summary;
```

`CREATE MATERIALIZED VIEW` and `DROP MATERIALIZED VIEW` are Krishiv-native SQL extensions handled in `krishiv-sql`'s DDL layer. `REFRESH MATERIALIZED VIEW` is also Krishiv-native. DataFusion does not natively support these DDL forms.
