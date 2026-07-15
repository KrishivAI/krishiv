# Krishiv DataFrame API — PySpark parity

> Generated from `crates/krishiv-api/src/pyspark_parity.rs` — do not edit by hand.
> Regenerate with `KRISHIV_BLESS_PYSPARK_PARITY=1 cargo test -p krishiv-api pyspark_parity`.

**Overall parity: 105/116 = 91%** of the enumerated PySpark surface (Supported or Partial). Each shortfall is itemized below.

## Coverage by namespace

| Namespace | Covered | Total | % |
|---|---|---|---|
| DataFrame | 39 | 47 | 83% |
| Column | 13 | 16 | 81% |
| functions | 29 | 29 | 100% |
| GroupedData | 7 | 7 | 100% |
| Window | 6 | 6 | 100% |
| DataFrameReader | 5 | 5 | 100% |
| DataFrameWriter | 6 | 6 | 100% |

## DataFrame

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `select` | supported | select/select_exprs |  |
| `selectExpr` | supported | select_exprs |  |
| `filter` | supported | filter/filter_expr | alias `where` |
| `where` | supported | filter |  |
| `withColumn` | supported | with_column |  |
| `withColumnRenamed` | supported | rename | rename(existing, new); Spark's silent no-op on a missing column differs (rename validates) |
| `drop` | supported | drop |  |
| `groupBy` | supported | group_by |  |
| `agg` | supported | agg |  |
| `join` | supported | join/join_on |  |
| `crossJoin` | planned | — | no dedicated cross-join method |
| `orderBy` | supported | order_by | alias `sort` |
| `sort` | supported | sort |  |
| `limit` | supported | limit |  |
| `distinct` | supported | distinct |  |
| `dropDuplicates` | planned | — | Phase 61 gap: dedup on a subset of columns (distinct() is all-columns) |
| `union` | supported | union |  |
| `unionAll` | supported | union | deprecated Spark alias of union |
| `unionByName` | supported | union_by_name | name-aligned union; allowMissingColumns (null-fill) is the residual |
| `intersect` | supported | intersect/intersect_distinct |  |
| `exceptAll` | supported | except | except/except_distinct |
| `count` | supported | count |  |
| `collect` | supported | collect |  |
| `show` | supported | show |  |
| `describe` | supported | describe |  |
| `explain` | supported | explain/explain_logical |  |
| `schema` | supported | schema |  |
| `columns` | partial | schema | column names via schema(); no `columns` shortcut |
| `printSchema` | partial | schema | schema() is programmatic; no pretty-print helper |
| `sample` | supported | sample |  |
| `repartition` | supported | repartition |  |
| `coalesce` | planned | repartition | repartition exists; no shrink-only coalesce |
| `cache` | supported | cache/persist |  |
| `persist` | supported | persist |  |
| `unpersist` | supported | unpersist |  |
| `createOrReplaceTempView` | supported | create_or_replace_temp_view |  |
| `unpivot` | supported | unpivot | alias `melt` |
| `melt` | supported | unpivot |  |
| `na` | partial | fill_null/drop_nulls | fill/drop reachable directly; no unified `.na` sub-API (Phase 61 gap) |
| `fillna` | supported | fill_null |  |
| `dropna` | supported | drop_nulls |  |
| `replace` | planned | — | Phase 61 gap: value replacement |
| `withColumnsRenamed` | planned | — | bulk rename (variant-collapse target) |
| `toPandas` | planned | — | Phase 61 gap: zero-copy Arrow → pandas (Python surface) |
| `write` | supported | write | DataFrameWriter |
| `writeStream` | planned | write | Phase 61 keystone: write_stream().to_table(refresh=…) |
| `foreachBatch` | planned | — | Phase 61 gap: micro-batch sink callback |

## Column

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `alias` | supported | Expr::alias |  |
| `cast` | supported | Expr::cast |  |
| `try_cast` | supported | Expr::try_cast |  |
| `asc` | supported | Expr::asc |  |
| `desc` | supported | Expr::desc |  |
| `and` | supported | Expr::and | `&` |
| `or` | supported | Expr::or | `|` |
| `eqNullSafe` | planned | — | null-safe equality (<=>) not exposed |
| `isNull` | supported | Expr::is_null |  |
| `isNotNull` | supported | Expr::is_not_null |  |
| `over` | supported | Expr::over | window spec |
| `between` | planned | — | reachable as (c>=lo)&(c<=hi); no between() sugar |
| `isin` | planned | — | Phase 61 gap: IN-list column predicate |
| `like` | partial | raw | reachable via raw SQL; no typed like() |
| `substr` | partial | function | reachable via function("substr", …) |
| `when_otherwise` | partial | raw | CASE via raw/SQL; no Column.when chain |

## functions

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `col` | supported | col |  |
| `lit` | supported | lit |  |
| `sum` | supported | sum |  |
| `avg` | supported | avg |  |
| `count` | supported | count/count_all |  |
| `min` | supported | min |  |
| `max` | supported | max |  |
| `row_number` | supported | row_number |  |
| `rank` | supported | rank |  |
| `dense_rank` | supported | dense_rank |  |
| `percent_rank` | supported | percent_rank |  |
| `cume_dist` | supported | cume_dist |  |
| `ntile` | supported | ntile |  |
| `lag` | supported | lag |  |
| `lead` | supported | lead |  |
| `first` | supported | first_value |  |
| `last` | supported | last_value |  |
| `nth_value` | supported | nth_value |  |
| `when` | partial | function | reachable via the SQL registry / raw; no typed when() builder (Phase 61 gap) |
| `coalesce` | supported | coalesce | typed F.* helper over the SQL registry |
| `nvl` | supported | nvl | typed helper; exact Spark alias |
| `upper` | supported | upper | typed F.* helper |
| `lower` | supported | lower | typed F.* helper |
| `length` | supported | length | typed F.* helper (character length) |
| `trim` | supported | trim | typed F.* helper |
| `abs` | supported | abs | typed F.* helper |
| `concat` | partial | function | reachable via function("concat", …); no typed helper — Spark returns NULL if any arg is NULL, DataFusion skips nulls, so an exact F.concat is deferred (exact-or-absent rule) |
| `round` | partial | function | reachable via function("round", …); no typed helper — Spark rounds half-up, DataFusion half-even, so an exact F.round is deferred |
| `<sql-registry>` | partial | function | the whole Phase 60 SQL function registry (JSON/HOF/date/hash/…) is reachable via function(name, args); Phase 61 ships typed F.* helpers over it (one registry, all surfaces) |

## GroupedData

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `agg` | supported | group_by(...).agg |  |
| `count` | supported | group_by(...).agg(count) |  |
| `sum` | supported | group_by(...).agg(sum) |  |
| `avg/mean` | supported | group_by(...).agg(avg) |  |
| `min` | supported | group_by(...).agg(min) |  |
| `max` | supported | group_by(...).agg(max) |  |
| `pivot` | partial | DataFrame::pivot | pivot exists on DataFrame; PySpark places it on GroupedData (groupBy().pivot()) |

## Window

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `partitionBy` | supported | Expr::over(partition_by=…) |  |
| `orderBy` | supported | Expr::frame/over order |  |
| `rowsBetween` | supported | Expr::frame (rows) |  |
| `rangeBetween` | supported | Expr::frame (range) |  |
| `unboundedPreceding/Following` | supported | frame bounds |  |
| `currentRow` | supported | frame bounds |  |

## DataFrameReader

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `parquet` | supported | session.read_parquet |  |
| `csv` | supported | session.read_csv |  |
| `json` | partial | session.read_json | reachable; option coverage narrower than Spark |
| `table` | supported | session.sql/table |  |
| `format/load` | partial | read_* methods | typed per-format readers; no generic format(...).load() |

## DataFrameWriter

| PySpark | Status | Krishiv | Notes |
|---|---|---|---|
| `parquet` | supported | write_parquet |  |
| `csv` | supported | write_csv |  |
| `json` | supported | write_json |  |
| `saveAsTable` | partial | write / CTAS | reachable via SQL CTAS; no writer.saveAsTable |
| `mode` | partial | write_parquet_overwrite_partition | overwrite modes exist per-format; no unified .mode() |
| `partitionBy` | partial | write_parquet_with_options | partitioned writes via options / SQL PARTITIONED BY |

