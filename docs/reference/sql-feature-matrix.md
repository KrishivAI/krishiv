# Krishiv SQL feature matrix

_Generated from `krishiv-sql/src/grammar.rs` — do not edit by hand._

Each feature is dimensioned across the three Krishiv execution engines: **batch** (DataFusion + extensions), **streaming** (continuous windows), and **incremental** (IVM). `n/a` means the feature does not apply to that engine.

## SELECT

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `select.projection` | Column projection and aliases | supported | partial | partial |  |
| `select.star` | SELECT * expansion | supported | partial | partial |  |
| `select.distinct` | SELECT DISTINCT deduplication | supported | n/a | partial |  |
| `select.where` | WHERE predicate filtering | supported | partial | partial |  |
| `select.order_by` | ORDER BY with ASC/DESC and NULLS FIRST/LAST | supported | n/a | n/a | streaming/IVM: unbounded ordering is not a maintainable operator |
| `select.limit_offset` | LIMIT / OFFSET pagination | supported | n/a | n/a |  |
| `select.having` | HAVING post-aggregation filter | supported | partial | partial |  |
| `select.case` | CASE WHEN … THEN … ELSE … END expressions | supported | partial | partial |  |
| `select.cast` | CAST(expr AS type) and TRY_CAST | supported | partial | partial |  |
| `select.subquery_scalar` | Scalar subqueries in projection/predicate | supported | n/a | partial |  |
| `select.subquery_exists` | EXISTS / NOT EXISTS correlated subqueries | supported | n/a | partial |  |
| `select.subquery_in` | IN / NOT IN subqueries | supported | n/a | partial |  |
| `select.values` | VALUES clause for inline data | supported | n/a | partial |  |

## GROUP BY

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `groupby.basic` | Basic GROUP BY column list | supported | partial | partial | streaming: only inside a window TVF (windowed aggregation) |
| `groupby.rollup` | ROLLUP grouping sets | supported | n/a | partial |  |
| `groupby.cube` | CUBE grouping sets | supported | n/a | partial |  |
| `groupby.grouping_sets` | Explicit GROUPING SETS | supported | n/a | partial |  |
| `groupby.grouping_function` | GROUPING() function for NULL disambiguation | supported | n/a | partial |  |

## JOIN

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `join.inner` | INNER JOIN (equi and non-equi) | supported | n/a | partial |  |
| `join.left_outer` | LEFT OUTER JOIN | supported | n/a | partial |  |
| `join.right_outer` | RIGHT OUTER JOIN | supported | n/a | partial |  |
| `join.full_outer` | FULL OUTER JOIN | supported | n/a | partial |  |
| `join.cross` | CROSS JOIN | supported | n/a | partial |  |
| `join.natural` | NATURAL JOIN (column-name matching) | supported | n/a | partial |  |
| `join.using` | JOIN … USING (column_list) | supported | n/a | partial |  |
| `join.lateral` | LATERAL JOIN / CROSS JOIN LATERAL | supported | n/a | n/a |  |
| `join.interval` | Streaming interval join on event-time bounds | planned | partial | n/a | DataFrame-only today (audit §9b): the interval-join operator has no SQL planning path, so batch SQL cannot express it (Planned); the streaming operator exists (Partial). Corrected from the prior over-claim of batch Supported. |
| `join.temporal_as_of` | Temporal AS OF point-in-time join | planned | n/a | n/a | no SQL temporal-join planning path: `lakehouse/as_of.rs` is table time-travel (temporal.as_of), not a temporal join. Marked Planned rather than the prior Supported. |
| `join.broadcast_hint` | /*+ BROADCAST(t) */ optimizer hint | partial | n/a | n/a | hint parsed and recorded; broadcast decision is cost-based (see hints.* entries) |

## HINTS

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `hints.join_strategy` | /*+ MERGE|SHUFFLE_HASH|BROADCAST(t) */ join-strategy hints | partial | n/a | n/a | parsed always; honored where the executor supports the strategy (Phase 52/54), recorded either way |
| `hints.repartition` | /*+ REPARTITION(n)|COALESCE(n) */ partitioning hints | partial | n/a | n/a | parsed and recorded; applied where the distributed planner supports it |

## WINDOW

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `window.over` | OVER () window function clauses | supported | n/a | n/a |  |
| `window.partition_by` | PARTITION BY inside OVER | supported | n/a | n/a |  |
| `window.order_by` | ORDER BY inside OVER | supported | n/a | n/a |  |
| `window.rows_range` | ROWS / RANGE frame specification | supported | n/a | n/a |  |
| `window.rank_dense_rank` | RANK(), DENSE_RANK(), ROW_NUMBER() | supported | n/a | n/a |  |
| `window.lead_lag` | LEAD() and LAG() | supported | n/a | n/a |  |
| `window.first_last_value` | FIRST_VALUE() and LAST_VALUE() | supported | n/a | n/a |  |
| `window.nth_value` | NTH_VALUE() | supported | n/a | n/a |  |
| `window.ntile` | NTILE(n) | supported | n/a | n/a |  |
| `window.cume_dist_percent` | CUME_DIST() and PERCENT_RANK() | supported | n/a | n/a |  |
| `window.tumble` | TUMBLE(col, interval) streaming window | supported | supported | partial | batch rewrites the TVF to scalar UDFs (streaming_tvf.rs); streaming compiles it natively |
| `window.hop` | HOP(col, slide, size) sliding window | supported | supported | partial |  |
| `window.session` | Session window on inactivity gap | supported | supported | n/a |  |

## CTE

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `cte.non_recursive` | WITH … AS (…) non-recursive CTEs | supported | partial | partial |  |
| `cte.recursive` | WITH RECURSIVE … (UNION ALL base + recursive) | supported | n/a | n/a |  |
| `cte.multiple` | Multiple CTEs in one WITH clause | supported | partial | partial |  |

## SET

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `set.union_all` | UNION ALL | supported | partial | partial |  |
| `set.union_distinct` | UNION (DISTINCT) | supported | n/a | partial |  |
| `set.intersect` | INTERSECT | supported | n/a | partial |  |
| `set.except` | EXCEPT | supported | n/a | partial |  |

## LATERAL

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `lateral.unnest` | UNNEST(array_col) in FROM clause | supported | n/a | n/a |  |
| `lateral.generate_series` | generate_series() table function | supported | n/a | n/a |  |
| `lateral.cross_join_unnest` | CROSS JOIN UNNEST(…) AS t(col) | supported | n/a | n/a |  |

## PIVOT

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `pivot.pivot` | PIVOT(agg FOR col IN (v1, v2, …)) | supported | n/a | n/a |  |
| `pivot.unpivot` | UNPIVOT(value FOR col IN (c1, c2, …)) | supported | n/a | n/a |  |

## FUNCTIONS

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `functions.json.get_json_object` | get_json_object(json, path) Spark JSONPath extraction | supported | n/a | n/a |  |
| `functions.json.json_array_length` | json_array_length(json) top-level array element count | supported | n/a | n/a |  |
| `functions.json.from_to_json` | from_json / to_json struct⇄JSON conversion | planned | n/a | n/a | requires a typed arrow⇄JSON converter + a Spark-DDL schema parser with Spark's version-specific null-field/timestamp rules; itemized shortfall, not shipped approximate |
| `functions.json.json_tuple` | json_tuple(json, k1, k2, …) multi-key extraction (generator) | planned | n/a | n/a | needs table-generating/LATERAL VIEW machinery; use get_json_object per key today |
| `functions.json.schema_of_json` | schema_of_json(json) infer a DDL schema string | planned | n/a | n/a |  |
| `functions.hof.transform` | transform(array, x -> …) — Spark alias for array_transform | supported | n/a | n/a |  |
| `functions.hof.filter` | filter(array, x -> …) — Spark alias for array_filter | supported | n/a | n/a |  |
| `functions.hof.exists` | exists / any_match(array, x -> …) predicate-any | partial | n/a | n/a | any_match is reachable; the `exists(...)` spelling is shadowed by the EXISTS-subquery keyword in the parser (documented dialect difference) |
| `functions.hof.forall` | forall(array, x -> …) predicate-all (new, exact all-match) | supported | n/a | n/a |  |
| `functions.hof.aggregate_zip_map` | aggregate/reduce, zip_with, map_filter, transform_keys/values | planned | n/a | n/a | require DataFusion's multi-step lambda / map-lambda protocol; itemized shortfall |
| `functions.spark.nvl` | nvl / nvl2 null-coalescing (DataFusion-native, exact) | supported | n/a | n/a |  |
| `functions.spark.substring_index` | substring_index(str, delim, count) (DataFusion-native, exact) | supported | n/a | n/a |  |
| `functions.spark.date_format` | date_format(ts, fmt) with **Spark** pattern letters (yyyy-MM-dd) | supported | n/a | n/a | supported Spark pattern letters translate exactly to chrono; unsupported letters (era/timezone) error clearly rather than emitting wrong output. Differs from DataFusion's chrono-pattern date_format — see honesty page. |
| `functions.spark.crc32` | crc32(expr) IEEE CRC-32 as BIGINT (exact) | supported | n/a | n/a |  |
| `functions.spark.hash_generators` | xxhash64, stack, posexplode, inline | planned | n/a | n/a | xxhash64 needs byte-exact replication of Spark's seed-42 typed hashing; stack/posexplode/inline need generator machinery — itemized shortfall |

## DML

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `dml.copy_to` | COPY (query) TO 'path' (FORMAT …) | supported | n/a | n/a | inherited from DataFusion's native parser/planner; no Krishiv-side code involved |
| `dml.insert_into` | INSERT INTO table SELECT … | supported | n/a | n/a |  |
| `dml.insert_overwrite` | INSERT OVERWRITE (full partition replace) | supported | n/a | n/a |  |
| `dml.delete` | DELETE FROM table WHERE … | partial | n/a | n/a | supported on Iceberg tables; in-memory and Parquet tables require rewrite |
| `dml.update` | UPDATE table SET col = … WHERE … | partial | n/a | n/a | supported on Iceberg tables via MERGE rewrite |
| `dml.merge` | MERGE INTO target USING source ON … WHEN MATCHED … | supported | n/a | n/a |  |
| `dml.iceberg_merge` | Atomic Iceberg MERGE with row-level deletes | supported | n/a | n/a |  |
| `dml.truncate` | TRUNCATE TABLE (Iceberg + memory) | planned | n/a | n/a | itemized shortfall: TRUNCATE is not yet wired for memory/Iceberg session tables |

## DDL

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `ddl.create_external_table` | CREATE EXTERNAL TABLE … STORED AS … | supported | n/a | n/a |  |
| `ddl.create_view` | CREATE VIEW name AS SELECT … | supported | n/a | n/a |  |
| `ddl.create_function` | CREATE FUNCTION … LANGUAGE SQL|PYTHON | supported | n/a | n/a |  |
| `ddl.drop_table` | DROP TABLE [IF EXISTS] | supported | n/a | n/a |  |
| `ddl.drop_view` | DROP VIEW [IF EXISTS] | supported | n/a | n/a |  |
| `ddl.create_table_as` | CREATE TABLE … AS SELECT (CTAS) | supported | n/a | n/a | durable Iceberg landing (G17) when the target resolves to a registered Iceberg catalog; session table otherwise |
| `ddl.partitioned_by` | CREATE TABLE … PARTITIONED BY (col | bucket/truncate/year/month/day/hour(col)) AS SELECT | supported | n/a | n/a | Iceberg catalog tables only; transforms follow the Iceberg partition spec |
| `ddl.alter_table` | ALTER TABLE ADD/DROP COLUMN, RENAME | partial | n/a | n/a | Iceberg schema evolution via ALTER TABLE is supported |
| `ddl.create_schema` | CREATE SCHEMA name | supported | n/a | n/a | inherited from DataFusion's native catalog; no Krishiv-side code involved |
| `ddl.create_materialized_view` | CREATE MATERIALIZED VIEW … AS SELECT → IVM view (REFRESH/DROP) | n/a | n/a | planned | Phase 60 SQL-DDL-for-IVM task; engine primitive under the platform's governed pipelines |
| `ddl.create_streaming_table` | CREATE STREAMING TABLE … AS SELECT → continuous job | n/a | planned | n/a | Phase 60 SQL-DDL-for-streaming task |
| `ddl.live_table` | CREATE / REFRESH / DROP LIVE TABLE via session.sql() | supported | n/a | n/a |  |
| `ddl.connector_source_sink` | CREATE SOURCE/SINK … WITH (connector=…) resolved through the connector registry | partial | n/a | n/a | registry-backed dispatch replacing the parquet-only hardcoded factory (audit §8b); supported kinds come from connector descriptors, unsupported kinds fail loudly |

## SESSION

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `stmt.set_reset` | SET / RESET / SET TIMEZONE session config | supported | n/a | n/a | DataFusion-native session config |
| `stmt.use` | USE [CATALOG|SCHEMA] current-namespace | supported | n/a | n/a | Phase 60: mutates the session default catalog/schema |
| `stmt.cache` | CACHE / UNCACHE / CLEAR CACHE TABLE (session materialization) | planned | n/a | n/a | itemized shortfall: needs a session-scoped materialization + provider swap/restore |

## SHOW

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `show.tables_databases_functions` | SHOW TABLES | DATABASES | SCHEMAS | FUNCTIONS | COLUMNS | partial | n/a | n/a | TABLES/FUNCTIONS/COLUMNS are DataFusion-native; DATABASES/SCHEMAS added in Phase 60 (information_schema.schemata). SHOW PARTITIONS (Iceberg) and SHOW VIEWS remain the gap. |

## DESCRIBE

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `describe.function_database_query` | DESCRIBE FUNCTION | DATABASE | QUERY | planned | n/a | n/a | DESCRIBE <table> is native; FUNCTION/DATABASE/QUERY are the itemized shortfall |

## TEMPORAL

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `temporal.as_of` | AS OF TIMESTAMP point-in-time queries | supported | n/a | n/a |  |
| `temporal.match_recognize` | MATCH_RECOGNIZE pattern matching over ordered rows | partial | partial | n/a | streaming CEP subset: PARTITION BY / ORDER BY / PATTERN (…) / WITHIN <duration>; DEFINE (pattern-variable predicates) and MEASURES (computed output) clauses are the remaining gap vs Oracle/Flink's full grammar |
| `temporal.system_time` | FOR SYSTEM_TIME AS OF (Iceberg time-travel) | partial | n/a | n/a | alias for AS OF on Iceberg tables |

## PREPARED

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `prepared.create` | CREATE PREPARED STATEMENT via Flight SQL action | supported | n/a | n/a |  |
| `prepared.execute` | Execute prepared statement by handle | supported | n/a | n/a |  |
| `prepared.close` | CLOSE PREPARED STATEMENT to release server memory | supported | n/a | n/a |  |
| `prepared.parameters` | Positional parameter binding ($1, $2, …) | supported | n/a | n/a | local PreparedStatement::bind and Flight SQL DoPut parameter batches |
| `prepared.sql_text` | PREPARE name AS …; EXECUTE name(…); DEALLOCATE name | supported | n/a | n/a | inherited from DataFusion's native parser/planner (session-scoped named plans) |

## OPERATION

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `operation.id` | Operation IDs for query tracking | supported | supported | supported |  |
| `operation.cancel` | Cancel a running operation by ID | supported | supported | supported |  |
| `operation.timeout` | Per-query execution timeout | supported | n/a | n/a |  |
| `operation.progress` | Query progress reporting via QueryHandle | supported | supported | supported |  |

## ERROR

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `error.sqlstate` | SQLSTATE codes on error responses | supported | n/a | n/a |  |
| `error.error_position` | Source line/column in error messages | partial | n/a | n/a | DataFusion provides message but not structured position |

## FLIGHT SQL

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `flight.get_flight_info` | GetFlightInfo for statement execution | supported | n/a | n/a |  |
| `flight.do_get` | DoGet streaming result delivery | supported | n/a | n/a |  |
| `flight.prepared_statements` | Prepared statement create/execute/close | supported | n/a | n/a |  |
| `flight.do_action` | DoAction for custom Krishiv operations | supported | n/a | n/a |  |
| `flight.get_sql_info` | GetSqlInfo capability introspection | supported | n/a | n/a |  |
| `flight.auth` | Bearer token authentication | supported | n/a | n/a |  |
| `flight.policy` | Table-level access policy enforcement | supported | n/a | n/a |  |
| `flight.transactions` | BEGIN/COMMIT/ROLLBACK transactions | partial | n/a | n/a | Flight SQL BeginTransaction/EndTransaction actions; SQL BEGIN/COMMIT not routed |
| `flight.schemas` | GetDbSchemas / GetTables catalog introspection | partial | n/a | n/a | tables listed via Krishiv catalog; schema introspection via get_sql_info |

## STREAMING

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `streaming.continuous_select` | Continuous SELECT over unbounded input | n/a | supported | n/a |  |
| `streaming.window_agg` | Windowed aggregations over streaming input | n/a | supported | partial |  |
| `streaming.watermark` | Event-time watermarks for late-data handling | n/a | supported | n/a |  |
| `streaming.interval_join` | Streaming-to-streaming interval join | n/a | supported | n/a |  |
| `streaming.cep` | MATCH_RECOGNIZE CEP over streaming input | n/a | supported | n/a |  |
| `streaming.dedup` | Streaming deduplication (dropDuplicates) | n/a | supported | n/a |  |
| `streaming.sink_modes` | Append / Update / Complete output modes | n/a | supported | partial |  |

## INTROSPECTION

| Feature | Description | Batch | Streaming | Incremental | Notes |
|---|---|---|---|---|---|
| `introspection.describe` | DESCRIBE / DESC / SHOW COLUMNS table schema | supported | n/a | n/a |  |
| `introspection.explain` | EXPLAIN [LOGICAL|PHYSICAL|ANALYZE] query plans | supported | n/a | n/a |  |
| `introspection.information_schema` | information_schema.{tables,columns,views,df_settings,routines,parameters,schemata} | supported | n/a | n/a |  |

