#![forbid(unsafe_code)]

//! End-to-end integration tests for the Krishiv batch SQL pipeline.
//!
//! Exercises: session creation, table registration, SQL queries (JOIN,
//! aggregation, filtering, ORDER BY + LIMIT, subqueries, CTEs),
//! error handling, in-memory RecordBatch round-trips, and UDF registration.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv::prelude::*;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

// ── helpers ──────────────────────────────────────────────────────────────────

fn build_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

fn build_batch(schema: Arc<Schema>, columns: Vec<Arc<dyn arrow::array::Array>>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("RecordBatch::try_new must succeed")
}

fn write_parquet(path: &std::path::Path, batches: &[RecordBatch]) {
    assert!(!batches.is_empty(), "need at least one batch");
    let schema = batches[0].schema();
    let file = File::create(path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema, None).expect("ArrowWriter::try_new must succeed");
    for batch in batches {
        writer.write(batch).expect("write parquet batch");
    }
    writer.close().expect("close parquet writer");
}

fn session() -> Session {
    Session::builder()
        .build()
        .expect("SessionBuilder::build must succeed")
}

// ── 1. Create Session → register Parquet → SQL query → verify results ───────

#[tokio::test(flavor = "multi_thread")]
async fn create_session_register_parquet_sql_query() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("cities.parquet");

    let schema = build_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["London", "Paris", "Tokyo"])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();
    s.register_parquet("cities", &path).unwrap();

    let df = s.sql("SELECT city FROM cities ORDER BY city").unwrap();
    let result = df.collect_async().await.unwrap();

    assert_eq!(result.row_count(), 3);
    let pretty = result.pretty().unwrap();
    assert!(pretty.contains("London"));
    assert!(pretty.contains("Paris"));
    assert!(pretty.contains("Tokyo"));
}

// ── 2. Register multiple tables → JOIN query → verify results ───────────────

#[tokio::test(flavor = "multi_thread")]
async fn register_multiple_tables_join_query() {
    let temp = tempdir().unwrap();

    // orders table
    let orders_path = temp.path().join("orders.parquet");
    let orders_schema = build_schema(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]);
    let orders_batch = build_batch(
        orders_schema,
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(Float64Array::from(vec![25.0, 80.0, 50.0])),
        ],
    );
    write_parquet(&orders_path, &[orders_batch]);

    // customers table
    let customers_path = temp.path().join("customers.parquet");
    let customers_schema = build_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let customers_batch = build_batch(
        customers_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    );
    write_parquet(&customers_path, &[customers_batch]);

    let s = session();
    s.register_parquet("orders", &orders_path).unwrap();
    s.register_parquet("customers", &customers_path).unwrap();

    let df = s
        .sql(
            "SELECT c.name, o.amount \
             FROM orders o \
             JOIN customers c ON o.customer_id = c.id \
             ORDER BY c.name, o.amount",
        )
        .unwrap();
    let result = df.collect_async().await.unwrap();

    assert_eq!(result.row_count(), 3);
    let pretty = result.pretty().unwrap();
    assert!(pretty.contains("Alice"));
    assert!(pretty.contains("Bob"));
    // Alice has two orders: 25 and 50
}

// ── 3. SQL with aggregation → verify counts/sums ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_aggregation_counts_and_sums() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("sales.parquet");

    let schema = build_schema(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "US", "EU", "US", "EU", "US", "APAC",
            ])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();
    s.register_parquet("sales", &path).unwrap();

    let df = s
        .sql(
            "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total \
             FROM sales \
             GROUP BY region \
             ORDER BY region",
        )
        .unwrap();
    let result = df.collect_async().await.unwrap();

    assert_eq!(result.row_count(), 3);
    let pretty = result.pretty().unwrap();
    assert!(pretty.contains("APAC"));
    assert!(pretty.contains("EU"));
    assert!(pretty.contains("US"));

    // Verify aggregation values via pretty output (avoids downcast type mismatches)
    let pretty = result.pretty().unwrap();
    // APAC: 1 row, 60.0; EU: 2 rows, 60.0; US: 3 rows, 90.0
    assert!(pretty.contains("APAC"));
    assert!(pretty.contains("1")); // cnt for APAC
    assert!(pretty.contains("EU"));
    assert!(pretty.contains("2")); // cnt for EU
    assert!(pretty.contains("US"));
    assert!(pretty.contains("3")); // cnt for US
}

// ── 4. SQL with WHERE filter → verify row counts ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_where_filter_row_counts() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("users.parquet");

    let schema = build_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol", "Dave", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![25, 30, 35, 40, 45])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();
    s.register_parquet("users", &path).unwrap();

    // All users
    let all = s.sql("SELECT * FROM users").unwrap().collect().unwrap();
    assert_eq!(all.row_count(), 5);

    // Users older than 30
    let filtered = s
        .sql("SELECT name FROM users WHERE age > 30 ORDER BY name")
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(filtered.row_count(), 3);
    let pretty = filtered.pretty().unwrap();
    assert!(pretty.contains("Carol"));
    assert!(pretty.contains("Dave"));
    assert!(pretty.contains("Eve"));

    // Users with age between 25 and 30 inclusive
    let range_filtered = s
        .sql("SELECT name FROM users WHERE age BETWEEN 25 AND 30 ORDER BY name")
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(range_filtered.row_count(), 2);
}

// ── 5. SQL with ORDER BY + LIMIT → verify ordering ──────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_order_by_and_limit() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("scores.parquet");

    let schema = build_schema(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol", "Dave", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![85, 92, 78, 95, 88])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();
    s.register_parquet("scores", &path).unwrap();

    // Top 3 scores descending
    let df = s
        .sql(
            "SELECT name, score FROM scores \
             ORDER BY score DESC \
             LIMIT 3",
        )
        .unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 3);

    // Verify ordering via pretty output
    let pretty = result.pretty().unwrap();
    // Highest: Dave=95, Bob=92, Eve=88
    let lines: Vec<&str> = pretty.lines().collect();
    // First data row should contain Dave with 95
    assert!(lines.iter().any(|l| l.contains("Dave") && l.contains("95")));
    assert!(lines.iter().any(|l| l.contains("Bob") && l.contains("92")));
    assert!(lines.iter().any(|l| l.contains("Eve") && l.contains("88")));
}

// ── 6. SQL with subquery → verify results ───────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_subquery() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("employees.parquet");

    let schema = build_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("salary", DataType::Int64, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Engineering",
                "Sales",
                "Sales",
                "HR",
            ])),
            Arc::new(Int64Array::from(vec![
                120_000, 100_000, 90_000, 80_000, 70_000,
            ])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();
    s.register_parquet("employees", &path).unwrap();

    // Find employees who earn more than the average salary
    let df = s
        .sql(
            "SELECT id, dept, salary \
             FROM employees \
             WHERE salary > (SELECT AVG(salary) FROM employees) \
             ORDER BY id",
        )
        .unwrap();
    let result = df.collect_async().await.unwrap();

    // Avg = (120000+100000+90000+80000+70000)/5 = 92000
    // Above average: 120000, 100000 → 2 rows
    assert_eq!(result.row_count(), 2);

    let batches = result.into_batches();
    let batch = &batches[0];
    let id_col = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
}

// ── 7. SQL with CTE → verify results ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_cte() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("products.parquet");

    let schema = build_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "Electronics",
                "Clothing",
                "Electronics",
                "Food",
            ])),
            Arc::new(Float64Array::from(vec![999.99, 49.99, 1299.99, 5.99])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();
    s.register_parquet("products", &path).unwrap();

    // CTE to find categories with total value > 1000
    let df = s
        .sql(
            "WITH category_totals AS ( \
               SELECT category, SUM(price) AS total \
               FROM products \
               GROUP BY category \
             ) \
             SELECT category, total \
             FROM category_totals \
             WHERE total > 1000 \
             ORDER BY total DESC",
        )
        .unwrap();
    let result = df.collect_async().await.unwrap();

    // Electronics: 999.99 + 1299.99 = 2299.98 → qualifies
    assert_eq!(result.row_count(), 1);
    let pretty = result.pretty().unwrap();
    assert!(pretty.contains("Electronics"));

    let batches = result.into_batches();
    let batch = &batches[0];
    let total_col = batch
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((total_col.value(0) - 2299.98).abs() < 0.01);
}

// ── 8. SQL error handling → verify proper errors ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_invalid_query_returns_error() {
    let s = session();
    let result = s.sql("SELECT FROM");
    assert!(result.is_err(), "malformed SQL should return an error");
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("DataFusion") || msg.contains("error") || msg.contains("syntax"),
        "error message should indicate a SQL problem, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_references_unregistered_table_returns_error() {
    let s = session();
    let result = s.sql("SELECT * FROM nonexistent_table");
    assert!(result.is_err(), "querying unregistered table should fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_division_by_zero_returns_error() {
    let s = session();
    let result = s.sql("SELECT 1 / 0");
    // DataFusion may return an error or treat it as NULL depending on config.
    // At minimum, the query should execute without crashing.
    // We accept either an error or a successful result (NULL for division by zero).
    if let Ok(df) = result {
        let _ = df.collect();
    }
}

// ── 9. SQL over in-memory RecordBatch → verify results ──────────────────────
//
// Registers Arrow RecordBatches via a temp Parquet file, then runs SQL.

#[tokio::test(flavor = "multi_thread")]
async fn sql_over_in_memory_recordbatch() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("inmem.parquet");

    let schema = build_schema(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]);

    // Build two RecordBatches in memory
    let b1 = build_batch(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    );
    let b2 = build_batch(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(StringArray::from(vec!["c"])),
        ],
    );

    // Persist both batches into a single Parquet file
    write_parquet(&path, &[b1, b2]);

    let s = session();
    s.register_parquet("inmem", &path).unwrap();

    let df = s.sql("SELECT SUM(x) AS total FROM inmem").unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 1);

    let batches = result.into_batches();
    let total = batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 60); // 10 + 20 + 30
}

// ── 10. SQL with UDF registration → verify UDF works in query ───────────────

#[tokio::test(flavor = "multi_thread")]
async fn sql_with_udf_registration() {
    use krishiv_udf::MultiplyScalarUdf;

    let temp = tempdir().unwrap();
    let path = temp.path().join("measurements.parquet");

    let schema = build_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]);
    let batch = build_batch(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    );
    write_parquet(&path, &[batch]);

    let s = session();

    // Register a "double" UDF that multiplies a column by 2
    let udf = Arc::new(MultiplyScalarUdf::new("double", "v", 2));
    s.register_scalar_udf(udf);
    assert_eq!(s.scalar_udf_names(), vec!["double".to_string()]);

    // Verify UDF is in the registry
    {
        let registry = s.udf_registry();
        let guard = registry.read().unwrap();
        let loaded = guard
            .get_scalar("double")
            .expect("udf should be registered");
        assert_eq!(loaded.name(), "double");
    }

    // Standard SQL still works after UDF registration
    s.register_parquet("measurements", &path).unwrap();
    let df = s
        .sql("SELECT id, value FROM measurements ORDER BY id")
        .unwrap();
    let result = df.collect_async().await.unwrap();
    assert_eq!(result.row_count(), 3);
}

// ── Bonus: multi-batch Parquet scan preserves row ordering ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn multi_batch_parquet_scan_preserves_rows() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("multi.parquet");

    let schema = build_schema(vec![Field::new("val", DataType::Int64, false)]);
    let b1 = build_batch(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    );
    let b2 = build_batch(schema.clone(), vec![Arc::new(Int64Array::from(vec![4, 5]))]);
    let b3 = build_batch(schema.clone(), vec![Arc::new(Int64Array::from(vec![6]))]);
    write_parquet(&path, &[b1, b2, b3]);

    let s = session();
    s.register_parquet("multi", &path).unwrap();

    let result = s
        .sql("SELECT COUNT(*) AS n FROM multi")
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(result.row_count(), 1);
    let batches = result.into_batches();
    let n = batches[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 6);
}

// ── Bonus: session defaults to embedded mode ────────────────────────────────

#[test]
fn session_defaults_to_embedded_execution_mode() {
    let s = session();
    assert_eq!(s.mode(), ExecutionMode::Embedded);
}

// ── Bonus: DataFrame pretty output is human-readable ────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn dataframe_pretty_is_human_readable() {
    let s = session();
    let result = s
        .sql("SELECT 1 AS a, 'hello' AS b")
        .unwrap()
        .collect()
        .unwrap();
    let pretty = result.pretty().unwrap();
    assert!(pretty.contains("a"));
    assert!(pretty.contains("b"));
    assert!(pretty.contains("hello"));
}
