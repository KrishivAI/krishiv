# Krishiv Compatibility Matrices

This document defines the format and initial population of Krishiv's three compatibility matrices. These matrices are living documents: update them when coverage changes and before each GA tag.

---

## SQL Compatibility Matrix

**Baseline**: Apache DataFusion. Features listed as "yes" use DataFusion without modification. "Partial" means Krishiv adds translation or restriction on top. "Krishiv-native" means the feature has no DataFusion equivalent and is implemented in `krishiv-sql`.

| SQL Feature | Support | DataFusion Baseline | Krishiv-Specific Notes |
|---|---|---|---|
| `SELECT / FROM / WHERE` | yes | yes | Full projection, filter pushdown |
| `GROUP BY` | yes | yes | Hash aggregate, spill on overflow |
| `ORDER BY` | yes | yes | Sort, limit-sort optimization |
| `LIMIT / OFFSET` | yes | yes | |
| `INNER / LEFT / RIGHT / FULL JOIN` | yes | yes | Hash join, sort-merge join |
| `CROSS JOIN` | yes | yes | |
| Correlated subqueries | yes | yes | Decorrelation via DataFusion |
| Scalar subqueries | yes | yes | |
| `IN / EXISTS` subqueries | yes | yes | Pushed to semi-join |
| CTEs (`WITH`) | yes | yes | Recursive CTEs not in R10 |
| Window functions (`OVER`) | yes | yes | See function matrix |
| `UNION / UNION ALL` | yes | yes | |
| `INTERSECT / EXCEPT` | yes | yes | |
| `DDL: CREATE TABLE AS SELECT` | partial | yes | Persists to Iceberg or local Parquet; not all catalog backends |
| `DDL: CREATE TABLE` (schema only) | partial | yes | Schema registered in catalog; no data placement control in R10 |
| `INSERT INTO` | partial | yes | Supported for Iceberg sinks; not for all connector types |
| `UPDATE / DELETE` | no | no | Deferred; Iceberg merge-on-read workaround via CDC pipeline |
| `EXPLAIN` | yes | yes | `EXPLAIN` and `EXPLAIN ANALYZE` |
| Streaming `SOURCE` clause | Krishiv-native | no | `SELECT ... FROM SOURCE(...)` syntax; routed to `SourceScan` operator |
| Tumbling window (`TUMBLE`) | Krishiv-native | no | `GROUP BY TUMBLE(ts, INTERVAL '1 minute')` |
| Session window (`SESSION`) | no | no | Deferred to R11 |
| Lateral joins | no | partial | Not exposed in R10 |

---

## Function Compatibility Matrix

**Baseline**: DataFusion built-in functions. UDFs add capability via `krishiv-udf`.

### Standard Scalar Functions

| Category | Functions | Notes |
|---|---|---|
| String | `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `substring`, `concat`, `replace`, `regexp_replace`, `split_part`, `char_length`, `starts_with`, `ends_with` | Via DataFusion |
| Math | `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `log`, `log2`, `log10`, `exp`, `sign`, `mod` | Via DataFusion |
| Date/time | `now`, `date_trunc`, `date_part`, `extract`, `to_timestamp`, `to_date`, `date_add`, `date_diff` | Via DataFusion |
| Type cast | `CAST(expr AS type)`, `TRY_CAST` | Via DataFusion |
| Conditional | `CASE/WHEN`, `COALESCE`, `NULLIF`, `IIF` | Via DataFusion |
| Array | `array_length`, `array_contains`, `cardinality` | Via DataFusion; partial |

### Aggregate Functions

| Function | Support | Notes |
|---|---|---|
| `COUNT(*)` | yes | |
| `COUNT(expr)` | yes | Excludes NULLs |
| `COUNT(DISTINCT expr)` | yes | HyperLogLog approximate variant available |
| `SUM` | yes | |
| `AVG` | yes | |
| `MIN / MAX` | yes | |
| `STDDEV / VARIANCE` | yes | Population and sample variants via DataFusion |
| `FIRST_VALUE / LAST_VALUE` | yes | Window and aggregate mode |
| `ARRAY_AGG` | yes | Via DataFusion |
| `STRING_AGG` | yes | Via DataFusion |
| `APPROX_PERCENTILE_CONT` | yes | Via DataFusion t-digest |

### Window Functions

| Function | Support | Notes |
|---|---|---|
| `ROW_NUMBER()` | yes | Via DataFusion |
| `RANK()` | yes | Via DataFusion |
| `DENSE_RANK()` | yes | Via DataFusion |
| `LAG(expr, offset)` | yes | Via DataFusion |
| `LEAD(expr, offset)` | yes | Via DataFusion |
| `NTILE(n)` | yes | Via DataFusion |
| `CUME_DIST()` | yes | Via DataFusion |
| `PERCENT_RANK()` | yes | Via DataFusion |
| `FIRST_VALUE / LAST_VALUE` | yes | Over window frame |
| `NTH_VALUE` | yes | Via DataFusion |

### User-Defined Functions (UDFs)

| UDF Type | Interface | Crate |
|---|---|---|
| Scalar UDF | `ScalarUdf` trait | `krishiv-udf` |
| Aggregate UDF | `AggregateUdf` trait | `krishiv-udf` |
| Table-valued UDF | `TableUdf` trait | `krishiv-udf` |
| Python UDF (scalar) | `@krishiv.udf` decorator, registered via `PyUdfRegistry` | `krishiv-python` |

UDFs are registered at session creation time via `UdfRegistry::register()` and available to all SQL in that session.

---

## Connector Certification Matrix

**Guarantee definitions**:
- **at-most-once**: output records may be lost on failure; no duplicates.
- **at-least-once**: no records lost; duplicates possible on recovery.
- **exactly-once**: no records lost; no duplicates; achieved via 2PC or idempotent write.

**Status definitions**:
- **certified**: connector passes the full connector certification suite; guarantee is enforced in CI.
- **beta**: connector exists and is functionally tested but does not yet pass the full certification suite.
- **experimental**: connector is present in the codebase but not yet recommended for production use.

> **Post-R12 maturity review (2026-05-22):** LocalParquet was previously marked
> **certified** while `tests/certification.rs` only runs capability-invariant checks
> (see **GAP-CN-03** in [`r12-maturity-gap-register.md`](r12-maturity-gap-register.md)).
> Status below reflects **honest CI coverage** until R14 expands the suite.

| Connector | Mode | Guarantee | Status | Notes |
|---|---|---|---|---|
| LocalParquet | sink | exactly-once | beta | 2PC implementation is strong; full certification lifecycle tests land in R14 (GAP-CN-03) |
| Kafka | source | at-least-once | beta | Offset committed after batch ack; no dedup on recovery |
| Iceberg | source | at-least-once | beta | Snapshot-based reads; no row-level dedup |
| Iceberg | sink | at-least-once | beta | Optimistic concurrency; 2PC promotion planned for R11 |
| S3 | sink | at-least-once | beta | Multi-part upload; no atomic transaction |
| FlightSQL gateway | query gateway | at-least-once | beta | Auth + policy wired; read-path only for R10 |
| DebeziumKafka | source (CDC) | at-least-once + idempotent | beta | Combined with Iceberg merge-on-read for idempotent-exactly-once CDC-to-lakehouse |

Connectors must pass the certification suite before status can be upgraded from beta to certified. The certification suite is defined in `crates/krishiv-connectors/tests/certification.rs` (expand in R14 per GAP-CN-03). Until then, do not mark connectors **certified** in this matrix without matching CI jobs.
