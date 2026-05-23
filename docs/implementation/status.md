# Krishiv Implementation Status

## Current Phase

**R15 COMPLETE (2026-05-23).**

Release tracker: [`r15-spark-ecosystem-compat.md`](r15-spark-ecosystem-compat.md)

## R15 Spark SQL & Ecosystem Compatibility (2026-05-23)

All R15 slices S1–S5 implemented with no stubs or deferred items.

### Completed

**S1 — Spark SQL function coverage**
- `crates/krishiv-sql/src/spark_compat.rs`: 190+ Spark 3.5 function aliases + `isnan` UDF
- `crates/krishiv-sql/src/spark_compat_date.rs`: `year`/`month`/`day`/`quarter` UDFs for TPC-H
- `crates/krishiv-sql/tests/spark_compat.rs`: harness + 100+ alias gate
- `docs/reference/spark-sql-compat-matrix.md`: published matrix

**S2 — krishiv-spark-compat Python package**
- `python/krishiv-spark-compat/`: `SparkSession.builder.remote()`, DataFrame SQL builder, Spark Connect gRPC client
- `from krishiv.compat.spark import SparkSession, col, avg, sum, explode`
- Remote integration tests (`tests/test_remote.py`) against `spark_connect_smoke` example

**S3 — Spark Connect gRPC server**
- Spark 3.5 protos in `crates/krishiv-proto/proto/spark/connect/`
- `crates/krishiv-spark-connect/`: plan translation, `SparkConnectService`, coordinator bind on port 7070
- Integration + TPC-H tests (`tests/integration.rs`, `tests/tpch_spark_connect.rs`, official `q1–q22.sql`)

**S4 — dbt, Airflow, Great Expectations**
- `python/krishiv-dbt-adapter/`: Flight SQL transport via `flightsql-dbapi` when installed
- `python/krishiv-airflow/`: `KrishivSubmitJobOperator`, `KrishivJobSensor`
- `python/krishiv-ge/`: `KrishivDatasource`

**S5 — Migration tooling & E2E**
- `krishiv compat analyze <file.py>` with JSON/text output
- TPC-H 22-query Spark Connect execution on mini dataset
- Migration analyzer test on `crates/krishiv/tests/reference/tpch_pyspark.py`

### Validation

```
cargo test --workspace --lib          → all suites pass
cargo test -p krishiv-sql --test spark_compat
cargo test -p krishiv-spark-connect
pytest python/krishiv-spark-compat/
pytest python/krishiv-dbt-adapter/
pytest python/krishiv-airflow/
pytest python/krishiv-ge/
```

### Blockers

None.

### Next Task

Begin R16 per [`docs/architecture/krishiv-roadmap.md`](../architecture/krishiv-roadmap.md).

Validation: `cargo test --workspace && cargo clippy -p krishiv-sql -p krishiv-spark-connect -p krishiv-proto -p krishiv -- -D warnings`
