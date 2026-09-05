# The SQL engine: `krishiv-sql`

`krishiv-sql` is the seam between Krishiv and DataFusion. Every SQL statement,
on every surface and in every placement, passes through `SqlEngine::sql`.
This document describes that pipeline stage by stage, the SQL dialect it
accepts beyond DataFusion's, the rewrites and optimizer rules Krishiv adds,
the session configuration it fixes, and the catalogs it can attach.

DataFusion owns parsing, expression evaluation, logical and physical planning,
and local execution. Krishiv owns what happens before DataFusion sees the
text, a set of rewrites and optimizer rules DataFusion does not have, the
memory and join policies, the distributed plan encoding, and the catalogs.

## `SqlEngine`

One `SqlEngine` wraps one DataFusion `SessionContext` plus Krishiv's own
registries:

- a **plan cache** keyed by statement text (`KRISHIV_PLAN_CACHE_MAX_ENTRIES`),
  holding DataFusion's *unoptimized* plan so that a cache hit re-runs every
  Krishiv rewrite that depends on the plan;
- a **row-count registry** (`table_row_counts`), filled at table registration
  from the Parquet footers and consumed by the join reorder rule and by the
  broadcast optimizer;
- a **vector-index cache**, shared with the ANN rewrite rule;
- UDF registries (scalar SQL-expression UDFs, Rust scalar/aggregate/table
  UDFs, Python UDFs), synced lazily by version counter;
- the incremental-view, pipeline, live-table and operation registries;
- the process-wide **query memory pool** (`process_query_pool`): one
  `FairSpillPool` shared by every engine in the process, with an
  unspillable-headroom slice (`unspillable_headroom`,
  `KRISHIV_UNSPILLABLE_HEADROOM_PERCENT`) so an operator that cannot spill can
  still make progress when spillable ones are consuming the budget.

`SqlEngine::sql` returns a lazy `SqlDataFrame`; execution happens on
`collect`, `execute_stream`, or `explain_analyze`.

## The `sql()` pipeline

In order:

1. **Pipe syntax.** `FROM t |> WHERE x |> SELECT y` is rewritten to standard
   SQL (`pipe_syntax`). Gated on a leading `FROM` and a `|>`.
2. **Spark SQL extensions.** `LATERAL VIEW`, `TABLESAMPLE`, `DESCRIBE
   EXTENDED` and related constructs DataFusion does not parse are rewritten
   (`spark_sql_ext`); each rewrite is guarded by its own `contains_*` check.
3. **PIVOT / UNPIVOT** macro rewrite (`pivot_sql`).
4. **Streaming table-valued functions.** `TUMBLE`, `HOP`, `SESSION` TVFs are
   rewritten (`streaming_tvf`); `TUMBLE_START`/`TUMBLE_END`/`HOP_START`/
   `HOP_END` are registered as scalar helpers (`window_functions`).
5. **`AS OF` time travel** is pre-processed and the referenced lakehouse
   snapshot/version is applied to the table provider (`lakehouse::as_of`).
6. **Multi-statement scripts.** `;`-separated statements run sequentially in
   this engine; the last statement's result is returned. A failing statement
   aborts the script; already-executed statements are not rolled back. This
   is how setup DDL (`CREATE SOURCE`, a `STORED AS JDBC` pull table) ships
   with the query that reads it to a distributed fragment, which is re-planned
   on a fresh engine.
7. **Plan cache lookup** (simple queries only — never DDL or `AS OF`).
8. **UDF sync** when the registry version changed.
9. **Intercepts** — statements DataFusion does not handle, dispatched before
   it sees them (see "SQL surface" below).
10. **DataFusion** `SessionContext::sql` → unoptimized `LogicalPlan`.
11. **Pre-optimizer rewrites** on that unoptimized plan (below).
12. Return the lazy frame. Optimization and physical planning run at execute
    time through DataFusion, with Krishiv's optimizer rules registered
    (`03-planning-and-optimization.md`).

### Pre-optimizer rewrites

Two rewrites must see the plan *before* DataFusion's optimizer, because the
optimizer specialises identical subtrees to their consumers (projection
pruning) and afterwards they are no longer identical. Both run only in a
process that has called `declare_single_query_process()` — the one-shot CLI —
because they execute part of the query eagerly inside an API whose contract
is lazy (`01-execution-modes.md`).

**Grouping-set rewrite** (`rollup_rewrite`). DataFusion evaluates
`ROLLUP`/`CUBE`/`GROUPING SETS` by expanding every input row once per set
into one hash aggregate; a `ROLLUP` of four columns is five sets, of eight is
nine. The rewrite computes the finest set once and re-aggregates it per set
(`sum` of sums, `sum` of counts, `min`/`max`, `avg` as a `Float64` sum over a
count, `grouping()` as a literal per set), emitting the finest aggregate as
one repeated `SubqueryAlias` that the CTE cache below then collects once.
`__grouping_id` is emitted in DataFusion's own encoding so the schema
round-trips exactly; `DISTINCT`, `FILTER`, ordered or null-treated
aggregates, decimal averages, duplicated group expressions, `CUBE` over six
columns, and more than 64 sets decline.

**CTE materialisation** (`cte_materialize`). DataFusion inlines a CTE once per
reference; a `WITH x AS (…)` referenced N times runs N times. A repeated
`SubqueryAlias` — found through `apply_with_subqueries`, so references inside
`EXISTS`/`IN`/scalar subqueries count — whose body reduces its input and whose
consumers do not filter it is collected once into a partitioned `MemTable`
and every reference is pointed at it. "Consumers do not filter it" is decided
by tracing each predicate's columns through enclosing aliases and projections
to the candidate body and asking whether `PushDownFilter` could have pushed
it beneath the body's top operator: a correlation, a join predicate, a
self-join between two references, and a filter above a window or on a
non-group column do not block; a constant filter on a group key does, because
each inlined copy would have computed only its slice. Bodies that outgrow
`MAX_MATERIALIZED_ROWS`/`MAX_MATERIALIZED_BYTES` are left inlined.

Both rewrites are measured across all 99 TPC-DS queries with per-query result
hashes (`16-performance.md`).

## SQL surface

The grammar is DataFusion's, parsed with the DuckDB dialect (lambdas
`x -> body` and `[...]` array literals), plus the following Krishiv
statements and functions. The complete, engine-generated feature matrix is
`../reference/sql-feature-matrix.md`; the Spark comparison is
`../reference/krishiv-vs-spark-sql.md`.

### Statements Krishiv intercepts

| Statement | Module | What it does |
|---|---|---|
| `DESCRIBE [TABLE\|QUERY\|DATABASE\|SCHEMA\|FUNCTION] [EXTENDED] …`, `SHOW COLUMNS` | `introspection_sql`, `statement_completion` | catalog introspection; `DESCRIBE QUERY` plans without running |
| `EXPLAIN [ANALYZE] <query>` (as SQL) | `introspection_sql` | returns Krishiv's plan envelope; the DataFusion plan with metrics is `krishiv explain [--analyze]` (see `11`) |
| `CREATE [OR REPLACE] EXTERNAL TABLE … STORED AS <connector>` | `connector_table` | a `TableProviderFactory` per registered connector kind (Parquet, CSV, JSON, Kafka, JDBC, Iceberg, Delta, Hudi, …) |
| `CREATE SOURCE` / `CREATE SINK` / `START PIPELINE` | `pipeline_ddl` | declarative pipelines over the connector registry |
| `CREATE [OR REPLACE] STREAMING TABLE <name> AS <select>` | `streaming_table_ddl` | the SQL front door to a continuous streaming job; validated through the streaming planner, run by the streaming coordinator |
| `CREATE [MATERIALIZED] INCREMENTAL VIEW`, `DECLARE RECURSIVE VIEW`, `DROP INCREMENTAL VIEW` | `incremental_view` | IVM views maintained by `krishiv-ivm` (`09`) |
| `CREATE MATERIALIZED VIEW … [REFRESH …]` | `krishiv-api::materialized_table` | Spark-4-style materialized tables with a managed refresh lifecycle |
| `CREATE LIVE TABLE` | `live_table` | parsed, then **rejected** with a typed error — never implemented, and the parser exists so the rejection is precise |
| `CREATE [OR REPLACE] FUNCTION … [RETURNS TABLE]` | `create_function_ddl`, `scalar_udf` | SQL-expression UDFs inlined before planning; table functions |
| `CREATE VECTOR INDEX ON …` | `vector_index` | IVF index over a table's embedding column, persisted in Parquet footer metadata (`vector_footer`) |
| `CREATE PREPARED STATEMENT` / `EXECUTE` | `krishiv-api::prepared` | typed positional parameters (`?` / `$N`) |
| `ANALYZE TABLE <ref> [COMPUTE STATISTICS] [FOR COLUMNS (…)]` | `analyze` | one scan: `COUNT(*)`, per-column approximate NDV, min, max, null count → row-count and `TableStatsRegistry` |
| `CACHE [LAZY] TABLE`, `UNCACHE TABLE`, `CLEAR CACHE` | `SqlEngine` | session-scoped materialisation into a `MemTable`, original provider kept for restore |
| `CALL system.<procedure>(…)` | Iceberg catalog | table maintenance (expire snapshots, rewrite manifests, remove orphans; `lakehouse::maintenance`) |
| `MERGE INTO` | `lakehouse::merge` | Iceberg row-level merge (copy-on-write) |
| `CREATE [OR REPLACE] TABLE … AS SELECT` | `SqlEngine` + Iceberg | durable CTAS landing in Iceberg when a catalog is attached |
| `SET key = value` | DataFusion | any `datafusion.*` or Krishiv option for the session |

### Functions and expressions beyond DataFusion

- Spark-reference scalar functions with exact semantics (`spark_functions`),
  higher-order lambda array functions (`higher_order_functions`), JSON
  functions (`json_functions`).
- `MATCH_RECOGNIZE` complex-event patterns (`cep_sql`, matcher in
  `krishiv-plan::cep`), bounded on streaming input by
  `KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT`.
- Correlated subquery decorrelation for `EXISTS`/`IN`/scalar shapes DataFusion
  rejects (`subquery`), and a guard that refuses a subquery over a streaming
  source before DataFusion's decorrelation would mishandle the unbounded input.
- `LATERAL` / `UNNEST` pre-processing (`unnest_sql`).
- Vector distance built-ins (`vector_functions`, one metric definition shared
  with the IVF index in `vector_metric`) and IVF-accelerated k-NN
  (`vector_search`, scalar quantisation in `vector_quantize`).
- SQLSTATE mapping of every engine error (`sqlstate`), used by Flight SQL and
  the gateway.

### Coverage as a number

`coverage` measures the Spark-reference SQL surface against the engine and
`grammar` publishes the engine-dimensioned feature matrix; both are what the
generated reference documents are blessed from, so a claim in those documents
is a test that passed.

## Optimizer rules Krishiv registers

Registered in `with_krishiv_optimizer_rules_inner`, on top of DataFusion's
default rule set. Each is documented with the measurement that justified it
in its module and in `03-planning-and-optimization.md`; the summary:

| Rule | Level | What it does | Default |
|---|---|---|---|
| `JoinReorder` | logical | reorders a chain of inner joins smallest-connected-first from the row-count registry, only when some relation is larger than the anchor | on (`KRISHIV_JOIN_REORDER`) |
| `SemiJoinReductionThroughAggregate` | logical | a semi-join on an aggregate's own grouping key filters the aggregate's input | on (`KRISHIV_SEMI_JOIN_REDUCTION`) |
| `SemiJoinPushdownThroughInnerJoin` | logical | pushes an existing semi/anti join below inner joins; declines a probe that removes no rows or is itself a join | on (`KRISHIV_SEMI_JOIN_PUSHDOWN`) |
| `SemiJoinReductionFromSelectiveDimension` | logical | reduces a fact stream by a filtered dimension | **off** (`KRISHIV_SEMI_JOIN_DIMENSION`): a full sweep found 18–37x regressions where the dimension is large |
| `LateMaterializeTopKAggregate` | logical | defers wide columns past a bounded top-N aggregate | on (`KRISHIV_LATE_MATERIALIZATION`) |
| `AnnTopKPrefilter` | logical | `ORDER BY distance(col, literal) LIMIT k` over an indexed table gains an exact τ pre-filter | on (`KRISHIV_ANN_AUTO_REWRITE`) |
| `SpillableJoinSelection` | physical | converts a hash join whose build side exceeds its memory share to sort-merge, which can spill; degenerate-broadcast rescue in a single-query process | on (`KRISHIV_SPILL_JOIN_BUILD_BYTES`) |
| `CooperativeAmplifiers` | physical | cooperative yielding for input-amplifying operators so cancellation lands within seconds | on |
| `GraceHashJoinExec` | physical | a spilling hash join, applied on the executor after fragment decode, never in a plan that must be encoded | off (`KRISHIV_GRACE_HASH_JOIN`) |
| cross-stage runtime filter | physical, distributed | a bloom filter from a join's build stage into the probe stage's map tasks (`runtime_filter_exec`, `krishiv-shuffle::runtime_filter`) | on (`KRISHIV_CROSS_STAGE_RUNTIME_FILTER`) |

## Session configuration

`build_single_node_session_config` fixes the DataFusion options every local
engine runs with:

| Option | Value | Why |
|---|---|---|
| `target_partitions` | `available_parallelism()` or `KRISHIV_TARGET_PARALLELISM` | |
| `batch_size` | 8192 (`KRISHIV_BATCH_SIZE`) | |
| `enable_round_robin_repartition` | on when partitions > 1 | |
| dynamic filter pushdown (master, join, top-k, aggregate) | on (`KRISHIV_RUNTIME_FILTERS`) | the master switch alone does not suppress the per-operator options, so `off` clears all four |
| `sql_parser.dialect` | DuckDB | lambdas and array literals |
| `parquet.pushdown_filters` | **off** (DataFusion default) | globally on is 0.871x across TPC-DS; a per-scan choice would be worth at most 5.6% and is not structurally decidable (`16`) |
| `repartition_file_min_size` | 1 MiB (DataFusion: 10 MiB) | every SF1 dimension table is under 10 MiB and was decoded on one thread while eleven waited |
| `hash_join_single_partition_threshold` / `_rows` | 8 MiB / 1 M rows (DataFusion: 1 MiB / 128 K) | a filtered dimension's *estimated* size trips 1 MiB and forces a hash repartition of the fact side; 32 MiB cost q11 71 ms, "always" cost q17 32% |
| `sort_spill_reservation_bytes` | scaled from the memory limit | |
| `collect_statistics` | on | row counts from Parquet footers |

Each value carries the sweep that set it in the source comment; the sweeps
are summarised in `16-performance.md`.

## Distributed plan encoding

`distributed_plan` implements ADR-0003: a physical plan is cut at repartition
boundaries into stages, and each stage subtree is serialised with
`datafusion-proto` (`dfplan:v1:<partition>:<base64>`), with Krishiv's shuffle
read node as a `PhysicalExtensionCodec` extension. Executors decode and run
one partition; they never re-parse SQL. `redistribute_unsplittable_broadcast_joins`
and the `join_estimates` reading of build-side statistics live here too.
Stage construction itself is in the scheduler (`04`).

## Catalogs

`catalog` is the Iceberg-first catalog layer (`KrishivCatalog`), bridged into
DataFusion as a `CatalogProvider`:

| Backend | Module | Feature |
|---|---|---|
| Filesystem (Hadoop-style) Iceberg catalog | `local_catalog` | `local-catalog` |
| Iceberg REST catalog (client + wrapper) | `iceberg_rest`, `rest_catalog_wrapper` | `rest-catalog`; `KRISHIV_ICEBERG_REST_*` |
| Postgres-backed Iceberg catalog | `postgres_catalog` | `postgres-catalog`; advisory lock around migration |
| AWS Glue | `glue_catalog` | `glue-catalog` |
| Unity Catalog | `unity_catalog` | `unity-catalog` |
| Unified facade over the above | `unified` | |

`iceberg_table_provider` is the read path (Iceberg scan → DataFusion Parquet
listing), `object_store_io` the shared-warehouse storage (S3/MinIO),
`object_store_registry` builds cloud stores on first use. DataFusion DML
interception (`INSERT`/`DELETE`/`UPDATE`/`MERGE` into Iceberg) is gated on
`iceberg-datafusion` + `local-catalog`. Delta Lake and Hudi tables register
by URI through `lakehouse::providers`. Lakehouse semantics and commit
protocols are in `10-connectors-and-lakehouse.md`.

## Python UDFs

`python_udf` executes Python scalar and aggregate UDFs registered as pickled
bytes, in a subprocess with `KRISHIV_PYTHON_UDF_TIMEOUT_MS`, so a distributed
fragment can carry them. Full-privilege native UDFs are refused under
restrictive durability profiles unless `KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS`.

## Streaming compilation

The streaming SQL front door compiles to typed specs, not to DataFusion
plans: `streaming_window_plan` (windowed aggregation →
`WindowExecutionSpec`), `streaming_join_plan` (stream-to-stream interval
join), `streaming_pipeline_plan` (a `WITH`-chained banded join feeding
windowed stages), and `stateless_exec` (per-batch SQL without state, shared
by executor, engines and bench). How those specs run is `08-streaming.md`.
