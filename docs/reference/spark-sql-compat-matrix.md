# Spark SQL Compatibility Matrix (R15)

Spark 3.5 function and operation coverage for Krishiv via DataFusion aliases and custom UDFs.

**Total Spark alias registrations:** 191

## Legend

| Status | Meaning |
|--------|---------|
| Supported (alias) | DataFusion builtin aliased; null semantics equivalent |
| Supported (UDF) | Custom ScalarUDF for divergent semantics |
| Partial | Alias registered; argument shapes may differ from Spark |

## SQL Functions

| Spark name | DataFusion builtin | Null handling |
|------------|-------------------|---------------|
| `ifnull` | `coalesce` | equivalent |
| `nvl` | `coalesce` | equivalent |
| `instr` | `strpos` | equivalent |
| `locate` | `strpos` | equivalent |
| `charindex` | `strpos` | equivalent |
| `position` | `strpos` | equivalent |
| `substring` | `substr` | equivalent |
| `substring_index` | `substr` | equivalent |
| `substr` | `substr` | equivalent |
| `left` | `left` | equivalent |
| `right` | `right` | equivalent |
| `upper` | `upper` | equivalent |
| `lower` | `lower` | equivalent |
| `trim` | `trim` | equivalent |
| `btrim` | `trim` | equivalent |
| `ltrim` | `trim` | equivalent |
| `rtrim` | `trim` | equivalent |
| `length` | `length` | equivalent |
| `len` | `length` | equivalent |
| `char_length` | `length` | equivalent |
| `character_length` | `length` | equivalent |
| `concat` | `concat` | equivalent |
| `concat_ws` | `concat_ws` | equivalent |
| `replace` | `replace` | equivalent |
| `translate` | `translate` | equivalent |
| `regexp_replace` | `regexp_replace` | equivalent |
| `regexp_extract` | `regexp_match` | equivalent |
| `regexp_like` | `regexp_match` | equivalent |
| `split_part` | `split_part` | equivalent |
| `split` | `split` | equivalent |
| `initcap` | `initcap` | equivalent |
| `lpad` | `lpad` | equivalent |
| `rpad` | `rpad` | equivalent |
| `repeat` | `repeat` | equivalent |
| `reverse` | `reverse` | equivalent |
| `ascii` | `ascii` | equivalent |
| `chr` | `chr` | equivalent |
| `char` | `chr` | equivalent |
| `base64` | `encode` | equivalent |
| `encode` | `encode` | equivalent |
| `unbase64` | `decode` | equivalent |
| `decode` | `decode` | equivalent |
| `md5` | `md5` | equivalent |
| `sha2` | `sha256` | equivalent |
| `sha256` | `sha256` | equivalent |
| `abs` | `abs` | equivalent |
| `ceil` | `ceil` | equivalent |
| `ceiling` | `ceil` | equivalent |
| `floor` | `floor` | equivalent |
| `round` | `round` | equivalent |
| `bround` | `round` | equivalent |
| `sqrt` | `sqrt` | equivalent |
| `pow` | `power` | equivalent |
| `power` | `power` | equivalent |
| `exp` | `exp` | equivalent |
| `ln` | `ln` | equivalent |
| `log` | `log` | equivalent |
| `log10` | `log` | equivalent |
| `sin` | `sin` | equivalent |
| `cos` | `cos` | equivalent |
| `tan` | `tan` | equivalent |
| `cot` | `cot` | equivalent |
| `asin` | `asin` | equivalent |
| `acos` | `acos` | equivalent |
| `atan` | `atan` | equivalent |
| `atan2` | `atan2` | equivalent |
| `sign` | `sign` | equivalent |
| `signum` | `sign` | equivalent |
| `pmod` | `mod` | equivalent |
| `mod` | `mod` | equivalent |
| `greatest` | `greatest` | equivalent |
| `least` | `least` | equivalent |
| `year` | `date_part` | equivalent |
| `month` | `date_part` | equivalent |
| `day` | `date_part` | equivalent |
| `hour` | `date_part` | equivalent |
| `minute` | `date_part` | equivalent |
| `second` | `date_part` | equivalent |
| `quarter` | `date_part` | equivalent |
| `dayofweek` | `date_part` | equivalent |
| `dayofyear` | `date_part` | equivalent |
| `weekofyear` | `date_part` | equivalent |
| `date_trunc` | `date_trunc` | equivalent |
| `trunc` | `date_trunc` | equivalent |
| `date_add` | `date_add` | equivalent |
| `date_sub` | `date_sub` | equivalent |
| `datediff` | `datediff` | equivalent |
| `date_diff` | `datediff` | equivalent |
| `make_date` | `make_date` | equivalent |
| `to_date` | `to_date` | equivalent |
| `to_timestamp` | `to_timestamp` | equivalent |
| `timestamp` | `to_timestamp` | equivalent |
| `date_format` | `to_char` | equivalent |
| `to_char` | `to_char` | equivalent |
| `from_unixtime` | `from_unixtime` | equivalent |
| `unix_timestamp` | `to_unixtime` | equivalent |
| `to_unixtime` | `to_unixtime` | equivalent |
| `current_timestamp` | `now` | equivalent |
| `now` | `now` | equivalent |
| `localtimestamp` | `now` | equivalent |
| `current_date` | `today` | equivalent |
| `today` | `today` | equivalent |
| `curdate` | `today` | equivalent |
| `array_contains` | `array_has` | equivalent |
| `array_has` | `array_has` | equivalent |
| `array_distinct` | `array_distinct` | equivalent |
| `array_intersect` | `array_intersect` | equivalent |
| `array_union` | `array_union` | equivalent |
| `array_except` | `array_except` | equivalent |
| `size` | `array_length` | equivalent |
| `array_length` | `array_length` | equivalent |
| `element_at` | `array_element` | equivalent |
| `flatten` | `flatten` | equivalent |
| `array_append` | `array_append` | equivalent |
| `array_prepend` | `array_prepend` | equivalent |
| `array_position` | `array_position` | equivalent |
| `array_remove` | `array_remove` | equivalent |
| `array_sort` | `array_sort` | equivalent |
| `slice` | `array_slice` | equivalent |
| `map_keys` | `map_keys` | equivalent |
| `map_values` | `map_values` | equivalent |
| `map_entries` | `map_entries` | equivalent |
| `named_struct` | `struct` | equivalent |
| `struct` | `struct` | equivalent |
| `row_number` | `row_number` | equivalent |
| `rank` | `rank` | equivalent |
| `dense_rank` | `dense_rank` | equivalent |
| `percent_rank` | `percent_rank` | equivalent |
| `cume_dist` | `cume_dist` | equivalent |
| `ntile` | `ntile` | equivalent |
| `lag` | `lag` | equivalent |
| `lead` | `lead` | equivalent |
| `first_value` | `first_value` | equivalent |
| `first` | `first_value` | equivalent |
| `last_value` | `last_value` | equivalent |
| `last` | `last_value` | equivalent |
| `nth_value` | `nth_value` | equivalent |
| `count` | `count` | equivalent |
| `sum` | `sum` | equivalent |
| `avg` | `avg` | equivalent |
| `mean` | `avg` | equivalent |
| `min` | `min` | equivalent |
| `max` | `max` | equivalent |
| `stddev` | `stddev` | equivalent |
| `stddev_samp` | `stddev` | equivalent |
| `stddev_pop` | `stddev_pop` | equivalent |
| `var_samp` | `var_samp` | equivalent |
| `variance` | `var_samp` | equivalent |
| `var_pop` | `var_pop` | equivalent |
| `corr` | `corr` | equivalent |
| `covar_pop` | `covar_pop` | equivalent |
| `covar_samp` | `covar_samp` | equivalent |
| `skewness` | `skewness` | equivalent |
| `kurtosis` | `kurtosis` | equivalent |
| `percentile_approx` | `approx_percentile_cont` | equivalent |
| `approx_percentile` | `approx_percentile_cont` | equivalent |
| `approx_count_distinct` | `approx_distinct` | equivalent |
| `hll_cardinality` | `approx_distinct` | equivalent |
| `bit_and` | `bit_and` | equivalent |
| `bit_or` | `bit_or` | equivalent |
| `bit_xor` | `bit_xor` | equivalent |
| `bool_and` | `bool_and` | equivalent |
| `every` | `bool_and` | equivalent |
| `bool_or` | `bool_or` | equivalent |
| `some` | `bool_or` | equivalent |
| `isnull` | `is_null` | equivalent |
| `isnotnull` | `is_not_null` | equivalent |
| `nullif` | `nullif` | equivalent |
| `try_cast` | `try_cast` | equivalent |
| `uuid` | `uuid` | equivalent |
| `rand` | `random` | equivalent |
| `random` | `random` | equivalent |
| `hash` | `xxhash64` | equivalent |
| `spark_hash` | `xxhash64` | equivalent |
| `xxhash64` | `xxhash64` | equivalent |
| `bin` | `bin` | equivalent |
| `hex` | `hex` | equivalent |
| `octet_length` | `octet_length` | equivalent |
| `bit_length` | `bit_length` | equivalent |
| `overlay` | `overlay` | equivalent |
| `endswith` | `ends_with` | equivalent |
| `ends_with` | `ends_with` | equivalent |
| `startswith` | `starts_with` | equivalent |
| `starts_with` | `starts_with` | equivalent |
| `contains` | `contains` | equivalent |
| `factorial` | `factorial` | equivalent |
| `pi` | `pi` | equivalent |
| `e` | `e` | equivalent |
| `degrees` | `degrees` | equivalent |
| `radians` | `radians` | equivalent |
| `width_bucket` | `width_bucket` | equivalent |

## DataFrame / Spark Connect operations

| Operation | Status | Notes |
|-----------|--------|-------|
| SQL | Supported | Direct SQL relation |
| Read (table/parquet) | Supported | Named table or path |
| Filter, Project, Aggregate | Supported | Translated to SQL |
| Sort, Limit, Join, SetOp | Supported | Inner/outer joins |
| Deduplicate, SubqueryAlias | Supported | |
| LocalRelation (inline) | Partial | Use SQL or registered tables |
| Range, Repartition, streaming | Unsupported | Returns UNIMPLEMENTED |

## PySpark API (migration analyzer)

See `krishiv compat analyze` for per-call-site classification against this matrix.
