#![forbid(unsafe_code)]
//! Phase 60 coverage harness — the Spark-reference SQL surface as a **measured
//! number**, not a vibe.
//!
//! Three things live here:
//!
//! 1. **The Spark-checklist suite** ([`CHECKLIST`]): one representative case per
//!    batch-claimed feature in the [`crate::grammar`] matrix. Runnable cases are
//!    executed against a fixed in-memory fixture and asserted to succeed;
//!    infra-gated features (Iceberg DML, Flight SQL protocol, the distributed
//!    runtime) point at where they are proven instead.
//! 2. **The matrix-to-test CI rule**: every non-`n/a`, non-`planned` **batch**
//!    cell must have a checklist case — enforced by
//!    [`tests::every_claimed_batch_feature_has_a_checklist_case`].
//! 3. **The published KPI + generated pages**: [`coverage_report`] computes the
//!    pass-rate, and the reference / honesty markdown pages are regenerated from
//!    the matrix and drift-guarded against the checked-in copies.

use crate::grammar::{Engine, FeatureStatus, feature_matrix};

/// Evidence that a batch-claimed feature works.
#[derive(Debug, Clone, Copy)]
pub enum Coverage {
    /// Executable against the in-memory fixture; the suite runs it and asserts
    /// it plans + executes without error.
    Sql(&'static str),
    /// Proven by a named test or integration suite, or infra-gated (Iceberg,
    /// Flight SQL protocol, distributed runtime, streaming engine). Its presence
    /// satisfies the matrix-to-test rule; the reference is the audit trail.
    Elsewhere(&'static str),
}

/// A single checklist case tying a matrix feature id to its evidence.
#[derive(Debug, Clone, Copy)]
pub struct ChecklistCase {
    pub feature_id: &'static str,
    pub coverage: Coverage,
}

const fn sql(feature_id: &'static str, q: &'static str) -> ChecklistCase {
    ChecklistCase {
        feature_id,
        coverage: Coverage::Sql(q),
    }
}
const fn elsewhere(feature_id: &'static str, why: &'static str) -> ChecklistCase {
    ChecklistCase {
        feature_id,
        coverage: Coverage::Elsewhere(why),
    }
}

/// The fixture DDL run before the SQL checklist cases: `t(id, name, ts)` and
/// `u(id, val)` as session tables.
pub const FIXTURE_T: &str = "CREATE TABLE t AS SELECT * FROM (VALUES \
    (1, 'a', TIMESTAMP '2024-01-01 00:00:00'), \
    (2, 'b', TIMESTAMP '2024-01-01 00:01:30'), \
    (3, 'a', TIMESTAMP '2024-01-01 00:03:00')) v(id, name, ts)";
pub const FIXTURE_U: &str = "CREATE TABLE u AS SELECT * FROM (VALUES (1, 10), (2, 20)) v(id, val)";

/// The Spark-reference checklist: one case per batch-claimed matrix feature.
pub static CHECKLIST: &[ChecklistCase] = &[
    // ── SELECT ───────────────────────────────────────────────────────────────
    sql("select.projection", "SELECT id, name AS n FROM t"),
    sql("select.star", "SELECT * FROM t"),
    sql("select.distinct", "SELECT DISTINCT name FROM t"),
    sql("select.where", "SELECT id FROM t WHERE id > 1"),
    sql(
        "select.order_by",
        "SELECT id FROM t ORDER BY id DESC NULLS LAST",
    ),
    sql(
        "select.limit_offset",
        "SELECT id FROM t ORDER BY id LIMIT 1 OFFSET 1",
    ),
    sql(
        "select.having",
        "SELECT name, count(*) c FROM t GROUP BY name HAVING count(*) >= 1",
    ),
    sql(
        "select.case",
        "SELECT CASE WHEN id > 1 THEN 'p' ELSE 'n' END AS c FROM t",
    ),
    sql(
        "select.cast",
        "SELECT CAST(id AS VARCHAR) a, TRY_CAST(name AS INT) b FROM t",
    ),
    sql(
        "select.subquery_scalar",
        "SELECT id, (SELECT max(id) FROM t) m FROM t",
    ),
    sql(
        "select.subquery_exists",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
    ),
    sql(
        "select.subquery_in",
        "SELECT id FROM t WHERE id IN (SELECT id FROM u)",
    ),
    sql("select.values", "SELECT * FROM (VALUES (1), (2)) v(x)"),
    // ── GROUP BY ──────────────────────────────────────────────────────────────
    sql(
        "groupby.basic",
        "SELECT name, count(*) c FROM t GROUP BY name",
    ),
    sql(
        "groupby.rollup",
        "SELECT name, count(*) c FROM t GROUP BY ROLLUP(name)",
    ),
    sql(
        "groupby.cube",
        "SELECT name, count(*) c FROM t GROUP BY CUBE(name)",
    ),
    sql(
        "groupby.grouping_sets",
        "SELECT name, count(*) c FROM t GROUP BY GROUPING SETS ((name), ())",
    ),
    sql(
        "groupby.grouping_function",
        "SELECT name, GROUPING(name) g FROM t GROUP BY ROLLUP(name)",
    ),
    // ── JOIN ──────────────────────────────────────────────────────────────────
    sql("join.inner", "SELECT * FROM t JOIN u ON t.id = u.id"),
    sql(
        "join.left_outer",
        "SELECT * FROM t LEFT JOIN u ON t.id = u.id",
    ),
    sql(
        "join.right_outer",
        "SELECT * FROM t RIGHT JOIN u ON t.id = u.id",
    ),
    sql(
        "join.full_outer",
        "SELECT * FROM t FULL JOIN u ON t.id = u.id",
    ),
    sql("join.cross", "SELECT t.id, u.val FROM t CROSS JOIN u"),
    sql("join.natural", "SELECT * FROM t NATURAL JOIN u"),
    sql("join.using", "SELECT * FROM t JOIN u USING (id)"),
    elsewhere(
        "join.lateral",
        "sql_tests.rs lateral-join coverage; correlation shape varies",
    ),
    sql(
        "join.broadcast_hint",
        "SELECT /*+ BROADCAST(u) */ t.id FROM t JOIN u ON t.id = u.id",
    ),
    // ── HINTS ─────────────────────────────────────────────────────────────────
    sql(
        "hints.join_strategy",
        "SELECT /*+ MERGE(u) */ t.id FROM t JOIN u ON t.id = u.id",
    ),
    sql(
        "hints.repartition",
        "SELECT /*+ REPARTITION(4) */ id FROM t",
    ),
    // ── WINDOW FUNCTIONS ──────────────────────────────────────────────────────
    sql("window.over", "SELECT id, sum(id) OVER () s FROM t"),
    sql(
        "window.partition_by",
        "SELECT id, sum(id) OVER (PARTITION BY name) s FROM t",
    ),
    sql(
        "window.order_by",
        "SELECT id, row_number() OVER (ORDER BY id) r FROM t",
    ),
    sql(
        "window.rows_range",
        "SELECT id, sum(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s FROM t",
    ),
    sql(
        "window.rank_dense_rank",
        "SELECT rank() OVER (ORDER BY id) a, dense_rank() OVER (ORDER BY id) b, row_number() OVER (ORDER BY id) c FROM t",
    ),
    sql(
        "window.lead_lag",
        "SELECT lead(id) OVER (ORDER BY id) a, lag(id) OVER (ORDER BY id) b FROM t",
    ),
    sql(
        "window.first_last_value",
        "SELECT first_value(id) OVER (ORDER BY id) a, last_value(id) OVER (ORDER BY id) b FROM t",
    ),
    sql(
        "window.nth_value",
        "SELECT nth_value(id, 1) OVER (ORDER BY id) a FROM t",
    ),
    sql(
        "window.ntile",
        "SELECT ntile(2) OVER (ORDER BY id) a FROM t",
    ),
    sql(
        "window.cume_dist_percent",
        "SELECT cume_dist() OVER (ORDER BY id) a, percent_rank() OVER (ORDER BY id) b FROM t",
    ),
    elsewhere(
        "window.tumble",
        "streaming_tvf.rs batch TUMBLE (needs an Int64 epoch-ms descriptor column)",
    ),
    elsewhere(
        "window.hop",
        "streaming_tvf.rs + streaming_window_plan.rs HOP coverage",
    ),
    elsewhere(
        "window.session",
        "streaming_window_plan.rs SESSION coverage",
    ),
    // ── CTE ───────────────────────────────────────────────────────────────────
    sql(
        "cte.non_recursive",
        "WITH c AS (SELECT id FROM t) SELECT * FROM c",
    ),
    sql(
        "cte.recursive",
        "WITH RECURSIVE c(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT * FROM c",
    ),
    sql(
        "cte.multiple",
        "WITH a AS (SELECT 1 x), b AS (SELECT 2 y) SELECT * FROM a, b",
    ),
    // ── SET OPS ───────────────────────────────────────────────────────────────
    sql(
        "set.union_all",
        "SELECT id FROM t UNION ALL SELECT id FROM u",
    ),
    sql(
        "set.union_distinct",
        "SELECT id FROM t UNION SELECT id FROM u",
    ),
    sql(
        "set.intersect",
        "SELECT id FROM t INTERSECT SELECT id FROM u",
    ),
    sql("set.except", "SELECT id FROM t EXCEPT SELECT id FROM u"),
    // ── LATERAL / UNNEST ──────────────────────────────────────────────────────
    sql("lateral.unnest", "SELECT unnest([1, 2, 3]) AS e"),
    sql(
        "lateral.generate_series",
        "SELECT * FROM generate_series(1, 3)",
    ),
    elsewhere(
        "lateral.cross_join_unnest",
        "unnest_sql.rs CROSS JOIN UNNEST coverage",
    ),
    // ── PIVOT ─────────────────────────────────────────────────────────────────
    elsewhere("pivot.pivot", "pivot_sql.rs PIVOT rewrite coverage"),
    elsewhere("pivot.unpivot", "pivot_sql.rs UNPIVOT rewrite coverage"),
    // ── FUNCTIONS: JSON ───────────────────────────────────────────────────────
    sql(
        "functions.json.get_json_object",
        "SELECT get_json_object('{\"a\":{\"b\":7}}', '$.a.b') AS v",
    ),
    sql(
        "functions.json.json_array_length",
        "SELECT json_array_length('[1,2,3,4]') AS n",
    ),
    // ── FUNCTIONS: higher-order ───────────────────────────────────────────────
    sql(
        "functions.hof.transform",
        "SELECT transform([1, 2, 3], x -> x * 2) AS r",
    ),
    sql(
        "functions.hof.filter",
        "SELECT filter([1, 2, 3, 4], x -> x % 2 = 0) AS r",
    ),
    sql(
        "functions.hof.exists",
        "SELECT any_match([1, 2, 3], x -> x > 2) AS r",
    ),
    sql(
        "functions.hof.forall",
        "SELECT forall([2, 4, 6], x -> x % 2 = 0) AS r",
    ),
    // ── FUNCTIONS: Spark scalar aliases ───────────────────────────────────────
    sql(
        "functions.spark.nvl",
        "SELECT nvl(NULL, 1) a, nvl2(1, 2, 3) b",
    ),
    sql(
        "functions.spark.substring_index",
        "SELECT substring_index('a.b.c', '.', 2) AS s",
    ),
    sql(
        "functions.spark.date_format",
        "SELECT date_format(TIMESTAMP '2024-03-07 09:05:00', 'yyyy-MM-dd') AS d",
    ),
    sql("functions.spark.crc32", "SELECT crc32('Spark') AS c"),
    // ── DML ───────────────────────────────────────────────────────────────────
    elsewhere("dml.copy_to", "DataFusion-native COPY TO (writes a file)"),
    sql("dml.insert_into", "INSERT INTO u SELECT 9, 90"),
    elsewhere(
        "dml.insert_overwrite",
        "lakehouse Iceberg INSERT OVERWRITE coverage",
    ),
    elsewhere(
        "dml.delete",
        "lakehouse Iceberg DELETE coverage (Iceberg-gated)",
    ),
    elsewhere(
        "dml.update",
        "lakehouse Iceberg UPDATE coverage (Iceberg-gated)",
    ),
    elsewhere("dml.merge", "lakehouse MERGE coverage (Iceberg-gated)"),
    elsewhere(
        "dml.iceberg_merge",
        "lakehouse atomic Iceberg MERGE coverage",
    ),
    // ── DDL ───────────────────────────────────────────────────────────────────
    elsewhere(
        "ddl.create_external_table",
        "sql_tests.rs CREATE EXTERNAL TABLE (needs a file)",
    ),
    sql("ddl.create_view", "CREATE VIEW cov_v AS SELECT 1 AS x"),
    elsewhere(
        "ddl.create_function",
        "create_function_ddl.rs CREATE FUNCTION coverage",
    ),
    sql("ddl.drop_table", "DROP TABLE IF EXISTS cov_absent_table"),
    sql("ddl.drop_view", "DROP VIEW IF EXISTS cov_absent_view"),
    sql(
        "ddl.create_table_as",
        "CREATE TABLE cov_ctas AS SELECT 1 AS x",
    ),
    elsewhere(
        "ddl.partitioned_by",
        "lakehouse PARTITIONED BY writer coverage (Iceberg-gated)",
    ),
    elsewhere(
        "ddl.alter_table",
        "lakehouse ALTER TABLE schema-evolution coverage",
    ),
    sql(
        "ddl.create_schema",
        "CREATE SCHEMA IF NOT EXISTS cov_schema",
    ),
    elsewhere("ddl.live_table", "live_table.rs LIVE TABLE DDL coverage"),
    elsewhere(
        "ddl.connector_source_sink",
        "krishiv-api connector-registry DDL coverage",
    ),
    // ── SESSION ───────────────────────────────────────────────────────────────
    sql(
        "stmt.set_reset",
        "SET datafusion.execution.batch_size = 4096",
    ),
    // `USE public` keeps the default schema where the fixtures live, so it does
    // not pollute later cases (its own unit test proves the schema switch).
    sql("stmt.use", "USE public"),
    // ── SHOW ──────────────────────────────────────────────────────────────────
    sql("show.tables_databases_functions", "SHOW DATABASES"),
    // ── TEMPORAL ──────────────────────────────────────────────────────────────
    elsewhere(
        "temporal.as_of",
        "lakehouse/as_of.rs time-travel coverage (Iceberg-gated)",
    ),
    elsewhere(
        "temporal.match_recognize",
        "cep_sql.rs MATCH_RECOGNIZE coverage",
    ),
    elsewhere(
        "temporal.system_time",
        "lakehouse FOR SYSTEM_TIME AS OF coverage",
    ),
    // ── PREPARED ──────────────────────────────────────────────────────────────
    elsewhere(
        "prepared.create",
        "krishiv-flight-sql prepared-statement protocol coverage",
    ),
    elsewhere(
        "prepared.execute",
        "krishiv-flight-sql prepared-statement protocol coverage",
    ),
    elsewhere(
        "prepared.close",
        "krishiv-flight-sql prepared-statement protocol coverage",
    ),
    elsewhere(
        "prepared.parameters",
        "krishiv-flight-sql parameter-binding coverage",
    ),
    sql("prepared.sql_text", "PREPARE cov_p AS SELECT 1"),
    // ── OPERATION ─────────────────────────────────────────────────────────────
    elsewhere(
        "operation.id",
        "krishiv-runtime operation-tracking coverage",
    ),
    elsewhere("operation.cancel", "krishiv-runtime cancel coverage"),
    elsewhere(
        "operation.timeout",
        "krishiv-runtime per-query timeout coverage",
    ),
    elsewhere("operation.progress", "krishiv-runtime progress coverage"),
    // ── ERROR ─────────────────────────────────────────────────────────────────
    elsewhere("error.sqlstate", "sqlstate.rs SQLSTATE coverage"),
    elsewhere(
        "error.error_position",
        "DataFusion message-only error position",
    ),
    // ── FLIGHT SQL ────────────────────────────────────────────────────────────
    elsewhere(
        "flight.get_flight_info",
        "krishiv-flight-sql service coverage",
    ),
    elsewhere("flight.do_get", "krishiv-flight-sql service coverage"),
    elsewhere(
        "flight.prepared_statements",
        "krishiv-flight-sql service coverage",
    ),
    elsewhere("flight.do_action", "krishiv-flight-sql service coverage"),
    elsewhere("flight.get_sql_info", "krishiv-flight-sql service coverage"),
    elsewhere("flight.auth", "krishiv-flight-sql auth coverage"),
    elsewhere("flight.policy", "krishiv-flight-sql policy coverage"),
    elsewhere(
        "flight.transactions",
        "krishiv-flight-sql transaction coverage",
    ),
    elsewhere(
        "flight.schemas",
        "krishiv-flight-sql catalog-introspection coverage",
    ),
    // ── STREAMING ─────────────────────────────────────────────────────────────
    elsewhere(
        "streaming.continuous_select",
        "streaming.rs continuous-select coverage",
    ),
    elsewhere(
        "streaming.window_agg",
        "streaming_window_plan.rs windowed-agg coverage",
    ),
    elsewhere("streaming.watermark", "streaming engine watermark coverage"),
    elsewhere(
        "streaming.interval_join",
        "streaming interval-join coverage",
    ),
    elsewhere("streaming.cep", "cep_sql.rs streaming CEP coverage"),
    elsewhere("streaming.dedup", "streaming dedup coverage"),
    elsewhere("streaming.sink_modes", "streaming sink-mode coverage"),
    // ── INTROSPECTION ─────────────────────────────────────────────────────────
    sql("introspection.describe", "DESCRIBE t"),
    sql("introspection.explain", "EXPLAIN SELECT 1"),
    sql(
        "introspection.information_schema",
        "SELECT count(*) c FROM information_schema.tables",
    ),
];

/// A published coverage summary (the phase KPI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageReport {
    /// Matrix features with a `Supported` or `Partial` **batch** cell.
    pub batch_claimed: usize,
    /// Of those, how many have a checklist case.
    pub batch_covered: usize,
    /// Checklist cases that are directly executed (`Coverage::Sql`).
    pub executable_cases: usize,
    /// FUNCTIONS-category features supported (batch) out of the total —
    /// the function-parity dimension.
    pub functions_supported: usize,
    pub functions_total: usize,
}

impl CoverageReport {
    /// Batch coverage as a percentage (0–100).
    pub fn batch_coverage_pct(&self) -> f64 {
        if self.batch_claimed == 0 {
            return 100.0;
        }
        (self.batch_covered as f64 / self.batch_claimed as f64) * 100.0
    }
}

/// Compute the coverage KPI from the matrix + checklist.
pub fn coverage_report() -> CoverageReport {
    let mut batch_claimed = 0usize;
    let mut batch_covered = 0usize;
    let mut functions_supported = 0usize;
    let mut functions_total = 0usize;

    for e in feature_matrix() {
        if e.category == "FUNCTIONS" {
            functions_total += 1;
            if e.batch == FeatureStatus::Supported {
                functions_supported += 1;
            }
        }
        if e.status_for(Engine::Batch).is_claimed() {
            batch_claimed += 1;
            if CHECKLIST.iter().any(|c| c.feature_id == e.id) {
                batch_covered += 1;
            }
        }
    }

    let executable_cases = CHECKLIST
        .iter()
        .filter(|c| matches!(c.coverage, Coverage::Sql(_)))
        .count();

    CoverageReport {
        batch_claimed,
        batch_covered,
        executable_cases,
        functions_supported,
        functions_total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checklist_ids_are_valid_and_unique() {
        let matrix_ids: std::collections::HashSet<&str> =
            feature_matrix().iter().map(|e| e.id).collect();
        let mut seen = std::collections::HashSet::new();
        for c in CHECKLIST {
            assert!(
                matrix_ids.contains(c.feature_id),
                "checklist references unknown feature id: {}",
                c.feature_id
            );
            assert!(
                seen.insert(c.feature_id),
                "duplicate checklist case for: {}",
                c.feature_id
            );
        }
    }

    /// The matrix-to-test CI rule: every claimed batch cell has a checklist case.
    #[test]
    fn every_claimed_batch_feature_has_a_checklist_case() {
        for e in feature_matrix() {
            if e.status_for(Engine::Batch).is_claimed() {
                assert!(
                    CHECKLIST.iter().any(|c| c.feature_id == e.id),
                    "batch-claimed feature '{}' has no checklist case (matrix-to-test rule)",
                    e.id
                );
            }
        }
    }

    #[test]
    fn coverage_report_meets_batch_gate() {
        let r = coverage_report();
        // Every claimed batch feature must have a case (100% by construction of
        // the rule above); assert the KPI is computed and the executable subset
        // is substantial.
        assert_eq!(r.batch_covered, r.batch_claimed);
        assert!(
            r.executable_cases >= 45,
            "executable cases: {}",
            r.executable_cases
        );
        assert!(
            r.functions_supported >= 8,
            "functions supported: {}",
            r.functions_supported
        );
    }

    fn workspace_doc(rel: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join(rel)
    }

    /// The public SQL reference page is generated from the matrix, never
    /// hand-written; regenerate with
    /// `KRISHIV_BLESS_SQL_DOCS=1 cargo test -p krishiv-sql coverage`.
    #[test]
    fn committed_reference_page_matches_matrix() {
        let path = workspace_doc("docs/reference/sql-feature-matrix.md");
        let expected = crate::grammar::generate_reference_markdown();
        if std::env::var("KRISHIV_BLESS_SQL_DOCS").is_ok() {
            std::fs::write(&path, &expected).expect("write reference page");
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(
            committed, expected,
            "docs/reference/sql-feature-matrix.md is out of date; regenerate with \
             KRISHIV_BLESS_SQL_DOCS=1 cargo test -p krishiv-sql coverage"
        );
    }

    /// The Krishiv-vs-Spark honesty page is generated from the matrix too.
    #[test]
    fn committed_honesty_page_matches_matrix() {
        let path = workspace_doc("docs/reference/krishiv-vs-spark-sql.md");
        let expected = crate::grammar::generate_honesty_markdown();
        if std::env::var("KRISHIV_BLESS_SQL_DOCS").is_ok() {
            std::fs::write(&path, &expected).expect("write honesty page");
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(
            committed, expected,
            "docs/reference/krishiv-vs-spark-sql.md is out of date; regenerate with \
             KRISHIV_BLESS_SQL_DOCS=1 cargo test -p krishiv-sql coverage"
        );
    }

    /// Execute every `Coverage::Sql` checklist case against the fixture and
    /// assert it plans + runs. This is the Spark-checklist suite proper.
    #[tokio::test]
    async fn spark_checklist_sql_cases_execute() {
        let engine = crate::SqlEngine::new();
        engine
            .sql(FIXTURE_T)
            .await
            .expect("fixture t")
            .collect()
            .await
            .expect("t rows");
        engine
            .sql(FIXTURE_U)
            .await
            .expect("fixture u")
            .collect()
            .await
            .expect("u rows");

        let mut failures: Vec<String> = Vec::new();
        for c in CHECKLIST {
            if let Coverage::Sql(q) = c.coverage {
                match engine.sql(q).await {
                    Ok(df) => {
                        if let Err(e) = df.collect().await {
                            failures.push(format!("[{}] exec: {e}", c.feature_id));
                        }
                    }
                    Err(e) => failures.push(format!("[{}] plan: {e}", c.feature_id)),
                }
            }
        }
        assert!(
            failures.is_empty(),
            "checklist failures:\n{}",
            failures.join("\n")
        );
    }
}
