//! Comprehensive tests for untested features in krishiv-sql.

use arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use krishiv_sql::create_function_ddl::{
    ColumnDef, is_create_function_returns_table, parse_create_function,
};
use krishiv_sql::referenced_table_names;

// ═══════════════════════════════════════════════════════════════════════════════
// CreateFunctionDdl — parse tests
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn parse_scalar_function_returns_table() {
    let sql = "CREATE FUNCTION process_data(input TEXT) RETURNS TABLE (id BIGINT, value DOUBLE)";
    let ddl = parse_create_function(sql).expect("should parse scalar-like UDTF");
    assert_eq!(ddl.function_name, "process_data");
    assert_eq!(ddl.return_columns.len(), 2);
    assert_eq!(
        ddl.return_columns[0],
        ColumnDef {
            name: "id".into(),
            data_type: DataType::Int64
        }
    );
    assert_eq!(
        ddl.return_columns[1],
        ColumnDef {
            name: "value".into(),
            data_type: DataType::Float64
        }
    );
}

#[test]
fn parse_aggregate_style_returns_table() {
    let sql = "CREATE FUNCTION agg_merge(state BYTES) RETURNS TABLE (key INT, result FLOAT)";
    let ddl = parse_create_function(sql).expect("should parse aggregate-style UDTF");
    assert_eq!(ddl.function_name, "agg_merge");
    assert_eq!(
        ddl.return_columns[0],
        ColumnDef {
            name: "key".into(),
            data_type: DataType::Int32
        }
    );
    assert_eq!(
        ddl.return_columns[1],
        ColumnDef {
            name: "result".into(),
            data_type: DataType::Float32
        }
    );
}

#[test]
fn parse_table_function_with_multiple_columns() {
    let sql = "CREATE FUNCTION window_emit(ts TIMESTAMP, payload TEXT) RETURNS TABLE (start_ts TIMESTAMP, end_ts TIMESTAMP, data TEXT) LANGUAGE PYTHON AS 'def window_emit(ts, payload): pass'";
    let ddl = parse_create_function(sql).expect("should parse multi-column UDTF");
    assert_eq!(ddl.function_name, "window_emit");
    assert_eq!(ddl.return_columns.len(), 3);
    assert_eq!(ddl.return_columns[0].name, "start_ts");
    assert_eq!(
        ddl.return_columns[0].data_type,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    );
    assert_eq!(ddl.return_columns[1].name, "end_ts");
    assert_eq!(ddl.return_columns[2].name, "data");
    assert_eq!(ddl.language.as_deref(), Some("python"));
    assert!(ddl.body.is_some());
}

#[test]
fn parse_preserves_function_case() {
    let sql = "CREATE FUNCTION MyCamelCase(x INT) RETURNS TABLE (v BOOLEAN)";
    let ddl = parse_create_function(sql).expect("should parse");
    assert_eq!(ddl.function_name, "MyCamelCase");
}

#[test]
fn parse_body_with_single_quotes() {
    let sql = "CREATE FUNCTION f(x INT) RETURNS TABLE (v TEXT) AS 'SELECT ''hello'' AS v'";
    let ddl = parse_create_function(sql).expect("should parse");
    assert_eq!(ddl.body.as_deref(), Some("SELECT 'hello' AS v"));
}

#[test]
fn parse_no_args_function() {
    let sql = "CREATE FUNCTION now_table() RETURNS TABLE (ts TIMESTAMP)";
    let ddl = parse_create_function(sql).expect("should parse zero-arg UDTF");
    assert_eq!(ddl.function_name, "now_table");
    assert_eq!(ddl.return_columns.len(), 1);
}

#[test]
fn detect_create_or_replace_returns_table() {
    assert!(is_create_function_returns_table(
        "CREATE OR REPLACE FUNCTION my_func(x INT) RETURNS TABLE (y TEXT)"
    ));
}

#[test]
fn reject_returns_scalar_not_table() {
    assert!(!is_create_function_returns_table(
        "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE RUST AS 'a + b'"
    ));
}

#[test]
fn reject_empty_returns_clause() {
    let sql = "CREATE FUNCTION empty(x INT) RETURNS TABLE ()";
    let ddl = parse_create_function(sql).expect("should parse empty column list");
    assert!(ddl.return_columns.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// CreateFunctionDdl — display / derived traits
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn ddl_debug_output_is_human_readable() {
    let sql =
        "CREATE FUNCTION demo(x INT) RETURNS TABLE (a TEXT, b BIGINT) LANGUAGE RUST AS 'body'";
    let ddl = parse_create_function(sql).expect("should parse");
    let debug = format!("{ddl:?}");
    assert!(debug.contains("demo"), "debug should include function name");
    assert!(
        debug.contains("rust"),
        "debug should include lowercased language"
    );
}

#[test]
fn ddl_clone_produces_equal_copy() {
    let sql = "CREATE FUNCTION dup(x INT) RETURNS TABLE (a TEXT)";
    let ddl = parse_create_function(sql).expect("should parse");
    let cloned = ddl.clone();
    assert_eq!(ddl.function_name, cloned.function_name);
    assert_eq!(ddl.return_columns, cloned.return_columns);
    assert_eq!(ddl.language, cloned.language);
    assert_eq!(ddl.body, cloned.body);
}

#[test]
fn column_def_equality() {
    let a = ColumnDef {
        name: "x".into(),
        data_type: DataType::Int64,
    };
    let b = ColumnDef {
        name: "x".into(),
        data_type: DataType::Int64,
    };
    let c = ColumnDef {
        name: "y".into(),
        data_type: DataType::Int64,
    };
    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ═══════════════════════════════════════════════════════════════════════════════
// WindowFunctions — row_number, rank, dense_rank via DataFusion SQL
// ═══════════════════════════════════════════════════════════════════════════════

use arrow::array::cast::AsArray;
use arrow::datatypes::UInt64Type;

async fn setup_events_table(ctx: &SessionContext) {
    ctx.sql(
        "CREATE TABLE scores (student VARCHAR, subject VARCHAR, score INT) AS \
         VALUES ('alice', 'math', 95), ('alice', 'science', 88), \
                ('bob', 'math', 72), ('bob', 'science', 91), \
                ('carol', 'math', 88), ('carol', 'science', 88)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
}

#[tokio::test]
async fn window_row_number_assigns_unique_ranks() {
    let ctx = SessionContext::new();
    setup_events_table(&ctx).await;
    let result = ctx
        .sql(
            "SELECT student, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn \
              FROM scores ORDER BY rn",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = result[0].column(2).as_primitive::<UInt64Type>();
    assert_eq!(col.len(), 6);
    assert_eq!(col.value(0), 1);
    assert_eq!(col.value(5), 6);
}

#[tokio::test]
async fn window_rank_handles_ties() {
    let ctx = SessionContext::new();
    setup_events_table(&ctx).await;
    let result = ctx
        .sql(
            "SELECT student, score, RANK() OVER (ORDER BY score DESC) AS rnk \
              FROM scores ORDER BY score DESC, student",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = result[0].column(2).as_primitive::<UInt64Type>();
    // scores: 95(1), 91(2), 88(3), 88(3), 88(3), 72(6)
    assert_eq!(col.value(0), 1); // 95
    assert_eq!(col.value(1), 2); // 91
    assert_eq!(col.value(2), 3); // 88 tied
    assert_eq!(col.value(3), 3); // 88 tied
    assert_eq!(col.value(4), 3); // 88 tied
    assert_eq!(col.value(5), 6); // 72 — rank 6 (not 4, because 3 ties skip to 6)
}

#[tokio::test]
async fn window_dense_rank_no_gaps() {
    let ctx = SessionContext::new();
    setup_events_table(&ctx).await;
    let result = ctx
        .sql(
            "SELECT student, score, DENSE_RANK() OVER (ORDER BY score DESC) AS dr \
              FROM scores ORDER BY score DESC, student",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = result[0].column(2).as_primitive::<UInt64Type>();
    // scores: 95(1), 91(2), 88(3), 88(3), 88(3), 72(4)
    assert_eq!(col.value(0), 1);
    assert_eq!(col.value(1), 2);
    assert_eq!(col.value(2), 3);
    assert_eq!(col.value(5), 4); // 72 gets dense rank 4, not 6
}

#[tokio::test]
async fn window_row_number_with_partition() {
    let ctx = SessionContext::new();
    setup_events_table(&ctx).await;
    let result = ctx
        .sql(
            "SELECT student, score, \
              ROW_NUMBER() OVER (PARTITION BY student ORDER BY score DESC) AS rn \
              FROM scores ORDER BY student, rn",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rn = result[0].column(2).as_primitive::<UInt64Type>();
    // 6 rows: alice(1,2) bob(1,2) carol(1,2) — each partition starts at 1
    assert_eq!(rn.len(), 6);
    // First row for alice has rank 1
    assert_eq!(rn.value(0), 1);
    // Third row (first for bob) has rank 1
    assert_eq!(rn.value(2), 1);
    // Fifth row (first for carol) has rank 1
    assert_eq!(rn.value(4), 1);
    // Second row in each partition has rank 2
    assert_eq!(rn.value(1), 2);
    assert_eq!(rn.value(3), 2);
    assert_eq!(rn.value(5), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// referenced_table_names — CTEs, subqueries, multiple tables
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn referenced_tables_with_cte() {
    let sql = "WITH regional AS (SELECT id, region FROM sales_region) \
               SELECT r.id, c.name FROM regional r JOIN customers c ON r.id = c.region_id";
    let tables = referenced_table_names(sql).unwrap();
    assert!(tables.contains(&"sales_region".to_string()));
    assert!(tables.contains(&"customers".to_string()));
}

#[test]
fn referenced_tables_with_nested_cte() {
    let sql = "WITH \
                 a AS (SELECT id FROM t1), \
                 b AS (SELECT id FROM t2 JOIN t3 ON t2.x = t3.x) \
               SELECT * FROM a JOIN b ON a.id = b.id";
    let tables = referenced_table_names(sql).unwrap();
    assert!(tables.contains(&"t1".to_string()));
    assert!(tables.contains(&"t2".to_string()));
    assert!(tables.contains(&"t3".to_string()));
}

#[test]
fn referenced_tables_with_subquery_in_where() {
    let sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM vip_customers)";
    let tables = referenced_table_names(sql).unwrap();
    assert!(tables.contains(&"orders".to_string()));
    assert!(tables.contains(&"vip_customers".to_string()));
}

#[test]
fn referenced_tables_with_correlated_subquery() {
    let sql = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.id)";
    let tables = referenced_table_names(sql).unwrap();
    assert!(tables.contains(&"t1".to_string()));
    assert!(tables.contains(&"t2".to_string()));
}

#[test]
fn referenced_tables_triple_join() {
    let sql = "SELECT a.id, b.val, c.label \
               FROM table_a a \
               JOIN table_b b ON a.id = b.a_id \
               JOIN table_c c ON b.id = c.b_id";
    let tables = referenced_table_names(sql).unwrap();
    assert_eq!(tables.len(), 3);
    assert!(tables.contains(&"table_a".to_string()));
    assert!(tables.contains(&"table_b".to_string()));
    assert!(tables.contains(&"table_c".to_string()));
}

#[test]
fn referenced_tables_union_all() {
    let sql = "SELECT id FROM legacy_table UNION ALL SELECT id FROM new_table";
    let tables = referenced_table_names(sql).unwrap();
    assert!(tables.contains(&"legacy_table".to_string()));
    assert!(tables.contains(&"new_table".to_string()));
}

#[test]
fn referenced_tables_cte_with_self_join() {
    let sql = "WITH dupes AS (SELECT id, name FROM users) \
               SELECT u1.id, u2.id FROM users u1 JOIN dupes u2 ON u1.name = u2.name";
    let tables = referenced_table_names(sql).unwrap();
    // users appears both in CTE body and main query; deduplicated
    let count = tables.iter().filter(|t| t.as_str() == "users").count();
    assert_eq!(
        count, 1,
        "users should appear once (deduplicated): {:?}",
        tables
    );
    assert!(tables.contains(&"users".to_string()));
}

#[test]
fn referenced_tables_empty_query_returns_error() {
    let result = referenced_table_names("   ");
    assert!(result.is_err());
}

#[test]
fn referenced_tables_single_table() {
    let sql = "SELECT * FROM events WHERE ts > 1000";
    let tables = referenced_table_names(sql).unwrap();
    assert_eq!(tables, vec!["events"]);
}

#[test]
fn referenced_tables_subquery_in_from() {
    let sql = "SELECT sub.id FROM (SELECT id FROM raw_events WHERE active) AS sub";
    let tables = referenced_table_names(sql).unwrap();
    assert!(tables.contains(&"raw_events".to_string()));
}
