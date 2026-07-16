//! SQL and function compatibility tests — R10 acceptance gate.
//!
//! Verifies the currently supported SQL compatibility surface.

use krishiv_sql::SqlEngine;

async fn run(query: &str) -> Vec<arrow::record_batch::RecordBatch> {
    SqlEngine::new()
        .sql(query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn sql_compat_select_literal() {
    let r = run("SELECT 1 + 1 AS two").await;
    assert_eq!(r[0].num_rows(), 1);
}

#[tokio::test]
async fn sql_compat_group_by_count() {
    let r = run("SELECT n % 2 AS parity, COUNT(*) AS cnt \
         FROM (VALUES (1),(2),(3),(4)) AS t(n) \
         GROUP BY n % 2 ORDER BY parity")
    .await;
    assert_eq!(r[0].num_rows(), 2);
}

#[tokio::test]
async fn sql_compat_cte() {
    let r = run("WITH base AS (SELECT 42 AS v) SELECT v * 2 AS doubled FROM base").await;
    assert_eq!(r[0].num_rows(), 1);
}

#[tokio::test]
async fn sql_compat_limit() {
    let r = run("SELECT n FROM (VALUES (1),(2),(3),(4),(5)) AS t(n) LIMIT 3").await;
    assert_eq!(r[0].num_rows(), 3);
}

#[tokio::test]
async fn sql_compat_order_by() {
    use arrow::array::Int64Array;
    let r = run("SELECT n FROM (VALUES (3),(1),(2)) AS t(n) ORDER BY n ASC").await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 1);
    assert_eq!(col.value(2), 3);
}

#[tokio::test]
async fn sql_compat_string_function_length() {
    use arrow::array::Int32Array;
    let r = run("SELECT length('hello') AS len").await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col.value(0), 5);
}

#[tokio::test]
async fn sql_compat_math_function_abs() {
    use arrow::array::Int64Array;
    let r = run("SELECT abs(-7) AS v").await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 7);
}

#[tokio::test]
async fn sql_compat_aggregate_sum() {
    use arrow::array::Int64Array;
    let r = run("SELECT SUM(n) AS total FROM (VALUES (1),(2),(3)) AS t(n)").await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 6);
}

#[tokio::test]
async fn sql_compat_subquery() {
    let r = run("SELECT v FROM (SELECT 99 AS v) sub WHERE v > 0").await;
    assert_eq!(r[0].num_rows(), 1);
}

#[tokio::test]
async fn sql_compat_union_all() {
    let r = run("SELECT 1 AS n UNION ALL SELECT 2 AS n").await;
    let total_rows: usize = r.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

// ── Multi-statement scripts ──────────────────────────────────────────────
// One SQL body may carry several `;`-separated statements; they execute
// sequentially in the SAME engine context and the last statement's result
// comes back. This is what lets a distributed batch fragment carry its own
// setup DDL (fragments are re-planned on a fresh engine per assignment, so
// session state from a previous call does not exist there).

#[tokio::test]
async fn multi_statement_script_shares_one_context() {
    use arrow::array::Int64Array;
    let r = run(
        "CREATE TABLE ms_t (n INT); \
         INSERT INTO ms_t VALUES (1),(2); \
         SELECT COUNT(*) AS c FROM ms_t",
    )
    .await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 2);
}

#[tokio::test]
async fn multi_statement_semicolon_inside_literal_is_not_a_split() {
    use arrow::array::StringArray;
    let r = run("SELECT 'a;b' AS s").await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "a;b");
}

#[tokio::test]
async fn multi_statement_semicolon_inside_comment_is_not_a_split() {
    use arrow::array::Int64Array;
    let r = run("-- setup; still one comment\nSELECT 1 AS x; SELECT 2 AS y").await;
    let col = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 2); // last statement wins
}

#[tokio::test]
async fn multi_statement_trailing_semicolon_stays_single_statement() {
    let r = run("SELECT 1 AS one;").await;
    assert_eq!(r[0].num_rows(), 1);
}

#[tokio::test]
async fn multi_statement_failure_aborts_the_script() {
    let err = SqlEngine::new()
        .sql("SELECT * FROM ms_missing_table; SELECT 1 AS never_reached")
        .await;
    assert!(err.is_err(), "script must abort on the failing statement");
}
