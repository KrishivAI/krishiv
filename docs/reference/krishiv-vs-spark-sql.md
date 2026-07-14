# Krishiv SQL vs Spark SQL

_Generated from `krishiv-sql/src/grammar.rs` — do not edit by hand._

Krishiv targets Spark-SQL reference parity as a **measured** number. This page is the honest ledger: what maps 1:1, what differs semantically, and what is absent, derived from the feature matrix.

## Documented semantic differences

- **`date_format(ts, fmt)` pattern letters.** Krishiv uses **Spark/Java** `DateTimeFormatter` letters (`yyyy-MM-dd`), not chrono/strftime (`%Y-%m-%d`). Supported letters translate exactly; unsupported letters (era `G`, timezone `z`/`X`) raise a clear error instead of emitting wrong output.
- **`exists(array, x -> …)`.** The `exists(` spelling is shadowed by the EXISTS-subquery keyword in the parser; use `any_match(array, x -> …)` (the byte-identical implementation) for the Spark higher-order `exists`.
- **Lambda / array-literal syntax.** The SQL front door parses with a lambda-capable dialect so `transform(arr, x -> …)` and `[1, 2, 3]` work; the array constructor is `make_array(...)` / `[...]` (Spark's `array(...)` maps to these).
- **ANSI mode, integral division, NULL ordering** follow DataFusion semantics, which match Spark ANSI mode for the covered surface; divergences are tracked as matrix notes.

## Maps 1:1 (supported, no semantic caveat)

- `select.projection` — Column projection and aliases
- `select.star` — SELECT * expansion
- `select.distinct` — SELECT DISTINCT deduplication
- `select.where` — WHERE predicate filtering
- `select.limit_offset` — LIMIT / OFFSET pagination
- `select.having` — HAVING post-aggregation filter
- `select.case` — CASE WHEN … THEN … ELSE … END expressions
- `select.cast` — CAST(expr AS type) and TRY_CAST
- `select.subquery_scalar` — Scalar subqueries in projection/predicate
- `select.subquery_exists` — EXISTS / NOT EXISTS correlated subqueries
- `select.subquery_in` — IN / NOT IN subqueries
- `select.values` — VALUES clause for inline data
- `groupby.rollup` — ROLLUP grouping sets
- `groupby.cube` — CUBE grouping sets
- `groupby.grouping_sets` — Explicit GROUPING SETS
- `groupby.grouping_function` — GROUPING() function for NULL disambiguation
- `join.inner` — INNER JOIN (equi and non-equi)
- `join.left_outer` — LEFT OUTER JOIN
- `join.right_outer` — RIGHT OUTER JOIN
- `join.full_outer` — FULL OUTER JOIN
- `join.cross` — CROSS JOIN
- `join.natural` — NATURAL JOIN (column-name matching)
- `join.using` — JOIN … USING (column_list)
- `join.lateral` — LATERAL JOIN / CROSS JOIN LATERAL
- `window.over` — OVER () window function clauses
- `window.partition_by` — PARTITION BY inside OVER
- `window.order_by` — ORDER BY inside OVER
- `window.rows_range` — ROWS / RANGE frame specification
- `window.rank_dense_rank` — RANK(), DENSE_RANK(), ROW_NUMBER()
- `window.lead_lag` — LEAD() and LAG()
- `window.first_last_value` — FIRST_VALUE() and LAST_VALUE()
- `window.nth_value` — NTH_VALUE()
- `window.ntile` — NTILE(n)
- `window.cume_dist_percent` — CUME_DIST() and PERCENT_RANK()
- `window.hop` — HOP(col, slide, size) sliding window
- `window.session` — Session window on inactivity gap
- `cte.non_recursive` — WITH … AS (…) non-recursive CTEs
- `cte.recursive` — WITH RECURSIVE … (UNION ALL base + recursive)
- `cte.multiple` — Multiple CTEs in one WITH clause
- `set.union_all` — UNION ALL
- `set.union_distinct` — UNION (DISTINCT)
- `set.intersect` — INTERSECT
- `set.except` — EXCEPT
- `lateral.unnest` — UNNEST(array_col) in FROM clause
- `lateral.generate_series` — generate_series() table function
- `lateral.cross_join_unnest` — CROSS JOIN UNNEST(…) AS t(col)
- `pivot.pivot` — PIVOT(agg FOR col IN (v1, v2, …))
- `pivot.unpivot` — UNPIVOT(value FOR col IN (c1, c2, …))
- `functions.json.get_json_object` — get_json_object(json, path) Spark JSONPath extraction
- `functions.json.json_array_length` — json_array_length(json) top-level array element count
- `functions.hof.transform` — transform(array, x -> …) — Spark alias for array_transform
- `functions.hof.filter` — filter(array, x -> …) — Spark alias for array_filter
- `functions.hof.forall` — forall(array, x -> …) predicate-all (new, exact all-match)
- `functions.spark.nvl` — nvl / nvl2 null-coalescing (DataFusion-native, exact)
- `functions.spark.substring_index` — substring_index(str, delim, count) (DataFusion-native, exact)
- `functions.spark.crc32` — crc32(expr) IEEE CRC-32 as BIGINT (exact)
- `dml.insert_into` — INSERT INTO table SELECT …
- `dml.insert_overwrite` — INSERT OVERWRITE (full partition replace)
- `dml.merge` — MERGE INTO target USING source ON … WHEN MATCHED …
- `dml.iceberg_merge` — Atomic Iceberg MERGE with row-level deletes
- `ddl.create_external_table` — CREATE EXTERNAL TABLE … STORED AS …
- `ddl.create_view` — CREATE VIEW name AS SELECT …
- `ddl.create_function` — CREATE FUNCTION … LANGUAGE SQL|PYTHON
- `ddl.drop_table` — DROP TABLE [IF EXISTS]
- `ddl.drop_view` — DROP VIEW [IF EXISTS]
- `ddl.live_table` — CREATE / REFRESH / DROP LIVE TABLE via session.sql()
- `temporal.as_of` — AS OF TIMESTAMP point-in-time queries
- `prepared.create` — CREATE PREPARED STATEMENT via Flight SQL action
- `prepared.execute` — Execute prepared statement by handle
- `prepared.close` — CLOSE PREPARED STATEMENT to release server memory
- `operation.id` — Operation IDs for query tracking
- `operation.cancel` — Cancel a running operation by ID
- `operation.timeout` — Per-query execution timeout
- `operation.progress` — Query progress reporting via QueryHandle
- `error.sqlstate` — SQLSTATE codes on error responses
- `flight.get_flight_info` — GetFlightInfo for statement execution
- `flight.do_get` — DoGet streaming result delivery
- `flight.prepared_statements` — Prepared statement create/execute/close
- `flight.do_action` — DoAction for custom Krishiv operations
- `flight.get_sql_info` — GetSqlInfo capability introspection
- `flight.auth` — Bearer token authentication
- `flight.policy` — Table-level access policy enforcement
- `introspection.describe` — DESCRIBE / DESC / SHOW COLUMNS table schema
- `introspection.explain` — EXPLAIN [LOGICAL|PHYSICAL|ANALYZE] query plans
- `introspection.information_schema` — information_schema.{tables,columns,views,df_settings,routines,parameters,schemata}

## Supported with caveats (partial or noted)

- `select.order_by` — ORDER BY with ASC/DESC and NULLS FIRST/LAST _(streaming/IVM: unbounded ordering is not a maintainable operator)_
- `groupby.basic` — Basic GROUP BY column list _(streaming: only inside a window TVF (windowed aggregation))_
- `join.broadcast_hint` — /*+ BROADCAST(t) */ optimizer hint _(hint parsed and recorded; broadcast decision is cost-based (see hints.* entries))_
- `hints.join_strategy` — /*+ MERGE|SHUFFLE_HASH|BROADCAST(t) */ join-strategy hints _(parsed always; honored where the executor supports the strategy (Phase 52/54), recorded either way)_
- `hints.repartition` — /*+ REPARTITION(n)|COALESCE(n) */ partitioning hints _(parsed and recorded; applied where the distributed planner supports it)_
- `window.tumble` — TUMBLE(col, interval) streaming window _(batch rewrites the TVF to scalar UDFs (streaming_tvf.rs); streaming compiles it natively)_
- `functions.hof.exists` — exists / any_match(array, x -> …) predicate-any _(any_match is reachable; the `exists(...)` spelling is shadowed by the EXISTS-subquery keyword in the parser (documented dialect difference))_
- `functions.spark.date_format` — date_format(ts, fmt) with **Spark** pattern letters (yyyy-MM-dd) _(supported Spark pattern letters translate exactly to chrono; unsupported letters (era/timezone) error clearly rather than emitting wrong output. Differs from DataFusion's chrono-pattern date_format — see honesty page.)_
- `dml.copy_to` — COPY (query) TO 'path' (FORMAT …) _(inherited from DataFusion's native parser/planner; no Krishiv-side code involved)_
- `dml.delete` — DELETE FROM table WHERE … _(supported on Iceberg tables; in-memory and Parquet tables require rewrite)_
- `dml.update` — UPDATE table SET col = … WHERE … _(supported on Iceberg tables via MERGE rewrite)_
- `ddl.create_table_as` — CREATE TABLE … AS SELECT (CTAS) _(durable Iceberg landing (G17) when the target resolves to a registered Iceberg catalog; session table otherwise)_
- `ddl.partitioned_by` — CREATE TABLE … PARTITIONED BY (col | bucket/truncate/year/month/day/hour(col)) AS SELECT _(Iceberg catalog tables only; transforms follow the Iceberg partition spec)_
- `ddl.alter_table` — ALTER TABLE ADD/DROP COLUMN, RENAME _(Iceberg schema evolution via ALTER TABLE is supported)_
- `ddl.create_schema` — CREATE SCHEMA name _(inherited from DataFusion's native catalog; no Krishiv-side code involved)_
- `ddl.connector_source_sink` — CREATE SOURCE/SINK … WITH (connector=…) resolved through the connector registry _(registry-backed dispatch replacing the parquet-only hardcoded factory (audit §8b); supported kinds come from connector descriptors, unsupported kinds fail loudly)_
- `stmt.set_reset` — SET / RESET / SET TIMEZONE session config _(DataFusion-native session config)_
- `stmt.use` — USE [CATALOG|SCHEMA] current-namespace _(Phase 60: mutates the session default catalog/schema)_
- `show.tables_databases_functions` — SHOW TABLES | DATABASES | SCHEMAS | FUNCTIONS | COLUMNS _(TABLES/FUNCTIONS/COLUMNS are DataFusion-native; DATABASES/SCHEMAS added in Phase 60 (information_schema.schemata). SHOW PARTITIONS (Iceberg) and SHOW VIEWS remain the gap.)_
- `temporal.match_recognize` — MATCH_RECOGNIZE pattern matching over ordered rows _(streaming CEP subset: PARTITION BY / ORDER BY / PATTERN (…) / WITHIN <duration>; DEFINE (pattern-variable predicates) and MEASURES (computed output) clauses are the remaining gap vs Oracle/Flink's full grammar)_
- `temporal.system_time` — FOR SYSTEM_TIME AS OF (Iceberg time-travel) _(alias for AS OF on Iceberg tables)_
- `prepared.parameters` — Positional parameter binding ($1, $2, …) _(local PreparedStatement::bind and Flight SQL DoPut parameter batches)_
- `prepared.sql_text` — PREPARE name AS …; EXECUTE name(…); DEALLOCATE name _(inherited from DataFusion's native parser/planner (session-scoped named plans))_
- `error.error_position` — Source line/column in error messages _(DataFusion provides message but not structured position)_
- `flight.transactions` — BEGIN/COMMIT/ROLLBACK transactions _(Flight SQL BeginTransaction/EndTransaction actions; SQL BEGIN/COMMIT not routed)_
- `flight.schemas` — GetDbSchemas / GetTables catalog introspection _(tables listed via Krishiv catalog; schema introspection via get_sql_info)_

## Absent (planned — itemized shortfall)

- `join.interval` — Streaming interval join on event-time bounds _(DataFrame-only today (audit §9b): the interval-join operator has no SQL planning path, so batch SQL cannot express it (Planned); the streaming operator exists (Partial). Corrected from the prior over-claim of batch Supported.)_
- `join.temporal_as_of` — Temporal AS OF point-in-time join _(no SQL temporal-join planning path: `lakehouse/as_of.rs` is table time-travel (temporal.as_of), not a temporal join. Marked Planned rather than the prior Supported.)_
- `functions.json.from_to_json` — from_json / to_json struct⇄JSON conversion _(requires a typed arrow⇄JSON converter + a Spark-DDL schema parser with Spark's version-specific null-field/timestamp rules; itemized shortfall, not shipped approximate)_
- `functions.json.json_tuple` — json_tuple(json, k1, k2, …) multi-key extraction (generator) _(needs table-generating/LATERAL VIEW machinery; use get_json_object per key today)_
- `functions.json.schema_of_json` — schema_of_json(json) infer a DDL schema string _(planned)_
- `functions.hof.aggregate_zip_map` — aggregate/reduce, zip_with, map_filter, transform_keys/values _(require DataFusion's multi-step lambda / map-lambda protocol; itemized shortfall)_
- `functions.spark.hash_generators` — xxhash64, stack, posexplode, inline _(xxhash64 needs byte-exact replication of Spark's seed-42 typed hashing; stack/posexplode/inline need generator machinery — itemized shortfall)_
- `dml.truncate` — TRUNCATE TABLE (Iceberg + memory) _(itemized shortfall: TRUNCATE is not yet wired for memory/Iceberg session tables)_
- `stmt.cache` — CACHE / UNCACHE / CLEAR CACHE TABLE (session materialization) _(itemized shortfall: needs a session-scoped materialization + provider swap/restore)_
- `describe.function_database_query` — DESCRIBE FUNCTION | DATABASE | QUERY _(DESCRIBE <table> is native; FUNCTION/DATABASE/QUERY are the itemized shortfall)_

