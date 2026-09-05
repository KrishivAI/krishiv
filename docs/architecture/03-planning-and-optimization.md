# Planning and optimization

Krishiv has two plan layers. DataFusion plans and executes SQL locally;
`krishiv-plan` is the engine-owned intermediate representation that crosses
crate and wire boundaries without exposing DataFusion types. This document
covers both, the optimizer rules on each, cost-based and adaptive execution,
and how a plan becomes distributed tasks.

## Two plan layers

| Layer | Crate | Purpose |
|---|---|---|
| DataFusion `LogicalPlan` / `ExecutionPlan` | `krishiv-sql` | local planning and execution; the substrate for every batch operator |
| Krishiv IR: `LogicalPlan`, `PhysicalPlan`, `NodeOp`, typed expression AST | `krishiv-plan` | the public, versioned plan contract; what SQL, Rust and Python lower to; what task fragments encode |

ADR-0002 fixes the rule: DataFusion remains the implementation, but
DataFusion types are neither public API nor wire format. The Rust `Expr`
(`krishiv-api::expression`) and Python `Column` are facades over
`krishiv-plan::expression`, the versioned AST with structured identifiers,
literals, operators, functions, types, nullability, sort semantics and window
definitions. `Expr::raw` / `expr("…")` is the explicit SQL escape hatch.

## `krishiv-plan`

| Module | Owns |
|---|---|
| `expression` | the versioned public expression and scalar type contract |
| `graph`, `lowering` | structural validation and deterministic logical → physical lowering; `NodeOp` → executor task fragments (ADR-DIST-04) |
| `task_fragment` | `TypedTaskFragment { version, execution_kind, body }`, the single wire carrier for work (ADR-0003) |
| `stream_task`, `window`, `stream_join` | `StreamingTaskSpec::{Window, Join, Pipeline, Stateless}`, `WindowExecutionSpec`, interval-join specs — the streaming plan types |
| `optimizer` | the Krishiv-IR optimizer: rule traits, `Optimizer`, `CostModel`, and the rules below |
| `udf` | UDF extension contracts |
| `cep` | the `MATCH_RECOGNIZE` pattern builder and per-key sequential matcher |
| `governance` | authentication and access-control interfaces (`12-security.md`) |

### Krishiv-IR optimizer rules (`krishiv-plan::optimizer`)

| Rule | Kind | Role |
|---|---|---|
| `predicate_pushdown` | logical | push filters toward scans |
| `constant_folding` | logical | fold constants, eliminate tautologies |
| `join_reorder` | logical | join ordering on the IR |
| `broadcast` (`BroadcastAutoRule`) | logical | mark a small build side for broadcast from the row-count registry |
| `small_file` | planner | small-file scan grouping |
| `stats` | CBO | `StatisticsRegistry` (`TableStatsRegistry`) and an NDV-aware cost model; fed by `ANALYZE TABLE` and by Iceberg CTAS/DELETE row counts |
| `auto_partition` | AQE | data-size-aware partition count |
| `coalesce` | AQE | coalesce small reduce partitions |
| `skew_join` | AQE | split a skewed partition with salting |
| `broadcast_runtime` | AQE | promote/demote broadcast joins from measured sizes |

The IR optimizer's `describe()` is what `EXPLAIN` (as a SQL statement)
reports. The rules that matter most for measured performance today are the
DataFusion-level ones in `krishiv-sql` (`02-sql-engine.md`), because that is
where local and per-task execution is planned.

## DataFusion-level optimization

DataFusion's rule set runs with `max_passes = 3`, stopping early when a pass
changes nothing. Krishiv appends its own logical and physical rules
(`02-sql-engine.md`, "Optimizer rules Krishiv registers"). Two facts about
DataFusion 54 shape several of them and are worth stating plainly:

- **DataFusion has no join-reordering rule.** `EliminateCrossJoin` rewrites a
  cross join plus a predicate in place; nothing else touches join order, so
  join order is `FROM`-clause order. `JoinReorder` exists because TPC-DS q72's
  `catalog_sales JOIN inventory` — a fact-to-fact join on a non-key column —
  sat below every selective filter and built a 15.29 M-row intermediate that
  five joins then reduced to 380 K. Reordering by base-table size took the
  query from 2655 ms to 280 ms with identical rows. The rule only fires when
  the written order is inverted (some relation larger than the anchor),
  because a pure size greedy regressed `store_sales ⋈ store_returns` — a
  near-1:1 fact-to-fact join that *reduces* — by 4x.
- **A `TableSource` exposes no statistics** at the logical level, and
  `ListingTable` does not implement `TableProvider::statistics()`. The row
  counts logical rules use come from Krishiv's own registry, populated at
  registration from the planned scan's `partition_statistics()` — the Parquet
  footers. A rule built over an empty registry (the staged planner) declines,
  by construction.

Physical planning reads `partition_statistics()` where they exist:
`SpillableJoinSelection` and `distributed_plan::redistribute_unsplittable_broadcast_joins`
size build sides from them; `join_estimates` is the one shared reading of a
build side's estimate (rows, bytes, or bytes implied by rows and schema width).

## From plan to tasks (batch)

1. The coordinator plans the statement on a **planning session context**
   (`distributed_plan::planning_session_context_with_options`) whose optimizer
   rules are the same list the embedded engine runs, over an empty row-count
   and vector-index registry — so the two can only differ where engine-resident
   data exists.
2. **Stage cut.** The physical plan is cut at every repartition boundary
   (`krishiv-scheduler::distributed_batch`): the subtree below an exchange is a
   *ShuffleMap* stage whose tasks hash-partition their output; the subtree
   above reads it through a `ShuffleReadExec` extension node. The final stage
   is a *Result* stage.
3. **Stage width** follows the cluster: `resolve_stage_target_partitions`
   targets `2 × schedulable slots`, bounded to `[2, 512]`, so shuffle
   fragments — which grow as partitions² — stay in budget; DataFusion bounds
   the count downward by file groups (`KRISHIV_STAGE_TARGET_PARTITIONS`
   overrides).
4. **One task per output partition.** Each task carries
   `dfplan:v1:<partition>:<base64 physical plan>`; `ShuffleWriteConfig` /
   `ShuffleReadConfig` on the assignment address the shuffle *data*, the body
   addresses the plan partition. Small client-supplied tables travel inline as
   base64 Arrow IPC and are never a data-plane transport.
5. **Stage reuse.** Identical stage plans (by encoded bytes) are deduplicated
   (`KRISHIV_STAGE_REUSE`) so a table scanned twice in one query is computed
   once where the two scans encode identically.
6. **Fallback.** A plan the stage builder cannot split runs as a single
   `sql: <query>` task — remote execution, not scale-out — and is the only
   place that body kind is still emitted.

## Adaptive query execution

At each stage boundary the coordinator re-optimises the *next* stage from the
measured shuffle output of the finished one (`krishiv-scheduler::coordinator::aqe`,
master switch `KRISHIV_AQE`):

| Decision | Trigger | Effect |
|---|---|---|
| partition coalesce (`KRISHIV_AQE_COALESCE`) | small reduce partitions | several partitions become one task, `dfplan:v1:p1,p2,…` multi-partition bodies; target `KRISHIV_AQE_TARGET_PARTITION_BYTES` (default 64 MiB) |
| skew split (`KRISHIV_AQE_SKEW_SPLIT`) | a partition ≥ `KRISHIV_AQE_SKEW_FACTOR` × median and ≥ `KRISHIV_AQE_SKEW_MIN_BYTES` | split into map-task-range sub-tasks `dfplan:v1:p/s0m2-4:…`, gated on a structural split-safety proof of the decoded plan (inner joins, filters, projections only; blocking operators fail closed) |
| broadcast promotion/demotion | measured build-side size | `broadcast_runtime` rule |

Every decision is written to the per-job adaptive decision log and to
`krishiv_aqe_*` metrics, and every decision is result-neutral by
construction and by test.

## Statistics

| Source | Feeds |
|---|---|
| Parquet footers at registration | `table_row_counts` (join reorder, `BroadcastAutoRule`) |
| `ANALYZE TABLE` | `table_row_counts` and `TableStatsRegistry` (per-column NDV/min/max/null count) |
| Iceberg CTAS / DELETE | `TableStatsRegistry` row counts (auto-fed) |
| shuffle output at stage boundaries | AQE |
| DataFusion `partition_statistics()` at physical planning | join build-side sizing |

`hash_join_single_partition_threshold` deserves a note: DataFusion chooses a
partitioned (both-sides-repartitioned) hash join whenever the build side's
*estimated* bytes exceed the threshold, and a `FilterExec` estimates its
output as its input scaled by a default selectivity. A 7.8 MB dimension
filtered to 27 K rows therefore still estimated above DataFusion's 1 MiB and
forced a hash repartition of the 2.88 M-row fact side; `02-sql-engine.md`
records the threshold Krishiv sets and the three-way sweep behind it.

## Explaining a plan

- `krishiv explain -q <sql>` prints the DataFusion logical and physical plans
  the session would run.
- `krishiv explain --analyze` executes and prints the physical plan with
  per-operator metrics (rows, `elapsed_compute`, build/probe times, scan
  metrics, spills, peak memory).
- `EXPLAIN <sql>` as a SQL statement returns Krishiv's IR plan envelope and
  the IR optimizer's decisions, not the DataFusion plan; `EXPLAIN ANALYZE` as
  SQL does not execute. Both are recorded as an open interface item in
  `../engineering-log/crate-audit-register.md` §90.
- `DataFrame::explain_with(...)`, `explain_logical()`, and
  `explain_analyze()` are the API forms.

## Related

- `02-sql-engine.md` — the rules and configuration in `krishiv-sql`.
- `04-scheduler-and-coordinator.md` — staged planning, admission, AQE hosting.
- `16-performance.md` — the measurements behind each rule and setting.
- `../decisions/0003-task-fragment-encoding.md` — the fragment encoding decision.
