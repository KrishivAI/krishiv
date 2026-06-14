#![forbid(unsafe_code)]
//! SQL grammar and feature matrix for Krishiv.
//!
//! Provides a machine-readable inventory of which SQL dialect features are
//! supported, partially supported, or planned.  Callers can query the matrix
//! to build documentation, surface feature gaps, or validate queries before
//! submission.

/// Support status for a single SQL feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureStatus {
    /// Fully supported in the current release.
    Supported,
    /// Partially supported; the `note` field explains the gap.
    Partial,
    /// Planned for a future release.
    Planned,
    /// Not applicable to this engine.
    NotApplicable,
}

impl FeatureStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Partial => "partial",
            Self::Planned => "planned",
            Self::NotApplicable => "n/a",
        }
    }
}

impl std::fmt::Display for FeatureStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single entry in the Krishiv SQL feature matrix.
#[derive(Debug, Clone)]
pub struct FeatureEntry {
    /// Stable identifier (e.g. `"select.distinct"`).
    pub id: &'static str,
    /// Broad feature category (e.g. `"SELECT"`, `"JOIN"`, `"DML"`).
    pub category: &'static str,
    /// Human-readable description.
    pub description: &'static str,
    /// Support status.
    pub status: FeatureStatus,
    /// Optional clarifying note (gap description, limitations, workarounds).
    pub note: Option<&'static str>,
}

impl FeatureEntry {
    const fn new(
        id: &'static str,
        category: &'static str,
        description: &'static str,
        status: FeatureStatus,
    ) -> Self {
        Self { id, category, description, status, note: None }
    }

    const fn with_note(mut self, note: &'static str) -> Self {
        self.note = Some(note);
        self
    }
}

impl std::fmt::Display for FeatureEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {} — {}", self.status, self.id, self.description)?;
        if let Some(note) = self.note {
            write!(f, " ({})", note)?;
        }
        Ok(())
    }
}

// ── Feature matrix ────────────────────────────────────────────────────────────

/// Return the complete Krishiv SQL feature matrix.
pub fn feature_matrix() -> &'static [FeatureEntry] {
    FEATURES
}

/// Return only entries matching `category` (case-insensitive prefix match).
pub fn features_for_category(category: &str) -> Vec<&'static FeatureEntry> {
    let cat_upper = category.to_uppercase();
    FEATURES
        .iter()
        .filter(|e| e.category.to_uppercase().starts_with(&cat_upper))
        .collect()
}

/// Return only entries with the given `status`.
pub fn features_by_status(status: FeatureStatus) -> Vec<&'static FeatureEntry> {
    FEATURES.iter().filter(|e| e.status == status).collect()
}

const S: FeatureStatus = FeatureStatus::Supported;
const P: FeatureStatus = FeatureStatus::Partial;
const L: FeatureStatus = FeatureStatus::Planned;

static FEATURES: &[FeatureEntry] = &[
    // ── SELECT ────────────────────────────────────────────────────────────────
    FeatureEntry::new("select.projection", "SELECT", "Column projection and aliases", S),
    FeatureEntry::new("select.star", "SELECT", "SELECT * expansion", S),
    FeatureEntry::new("select.distinct", "SELECT", "SELECT DISTINCT deduplication", S),
    FeatureEntry::new("select.where", "SELECT", "WHERE predicate filtering", S),
    FeatureEntry::new("select.order_by", "SELECT", "ORDER BY with ASC/DESC and NULLS FIRST/LAST", S),
    FeatureEntry::new("select.limit_offset", "SELECT", "LIMIT / OFFSET pagination", S),
    FeatureEntry::new("select.having", "SELECT", "HAVING post-aggregation filter", S),
    FeatureEntry::new("select.case", "SELECT", "CASE WHEN … THEN … ELSE … END expressions", S),
    FeatureEntry::new("select.cast", "SELECT", "CAST(expr AS type) and TRY_CAST", S),
    FeatureEntry::new("select.subquery_scalar", "SELECT", "Scalar subqueries in projection/predicate", S),
    FeatureEntry::new("select.subquery_exists", "SELECT", "EXISTS / NOT EXISTS correlated subqueries", S),
    FeatureEntry::new("select.subquery_in", "SELECT", "IN / NOT IN subqueries", S),
    FeatureEntry::new("select.values", "SELECT", "VALUES clause for inline data", S),

    // ── GROUP BY ─────────────────────────────────────────────────────────────
    FeatureEntry::new("groupby.basic", "GROUP BY", "Basic GROUP BY column list", S),
    FeatureEntry::new("groupby.rollup", "GROUP BY", "ROLLUP grouping sets", S),
    FeatureEntry::new("groupby.cube", "GROUP BY", "CUBE grouping sets", S),
    FeatureEntry::new("groupby.grouping_sets", "GROUP BY", "Explicit GROUPING SETS", S),
    FeatureEntry::new("groupby.grouping_function", "GROUP BY", "GROUPING() function for NULL disambiguation", S),

    // ── JOIN ─────────────────────────────────────────────────────────────────
    FeatureEntry::new("join.inner", "JOIN", "INNER JOIN (equi and non-equi)", S),
    FeatureEntry::new("join.left_outer", "JOIN", "LEFT OUTER JOIN", S),
    FeatureEntry::new("join.right_outer", "JOIN", "RIGHT OUTER JOIN", S),
    FeatureEntry::new("join.full_outer", "JOIN", "FULL OUTER JOIN", S),
    FeatureEntry::new("join.cross", "JOIN", "CROSS JOIN", S),
    FeatureEntry::new("join.natural", "JOIN", "NATURAL JOIN (column-name matching)", S),
    FeatureEntry::new("join.using", "JOIN", "JOIN … USING (column_list)", S),
    FeatureEntry::new("join.lateral", "JOIN", "LATERAL JOIN / CROSS JOIN LATERAL", S),
    FeatureEntry::new("join.interval", "JOIN", "Streaming interval join on event-time bounds", S),
    FeatureEntry::new("join.temporal_as_of", "JOIN", "Temporal AS OF point-in-time join", S),
    FeatureEntry::new("join.broadcast_hint", "JOIN", "/*+ BROADCAST(t) */ optimizer hint", P)
        .with_note("hint parsed; broadcast decision is cost-based, not forced"),

    // ── WINDOW FUNCTIONS ─────────────────────────────────────────────────────
    FeatureEntry::new("window.over", "WINDOW", "OVER () window function clauses", S),
    FeatureEntry::new("window.partition_by", "WINDOW", "PARTITION BY inside OVER", S),
    FeatureEntry::new("window.order_by", "WINDOW", "ORDER BY inside OVER", S),
    FeatureEntry::new("window.rows_range", "WINDOW", "ROWS / RANGE frame specification", S),
    FeatureEntry::new("window.rank_dense_rank", "WINDOW", "RANK(), DENSE_RANK(), ROW_NUMBER()", S),
    FeatureEntry::new("window.lead_lag", "WINDOW", "LEAD() and LAG()", S),
    FeatureEntry::new("window.first_last_value", "WINDOW", "FIRST_VALUE() and LAST_VALUE()", S),
    FeatureEntry::new("window.nth_value", "WINDOW", "NTH_VALUE()", S),
    FeatureEntry::new("window.ntile", "WINDOW", "NTILE(n)", S),
    FeatureEntry::new("window.cume_dist_percent", "WINDOW", "CUME_DIST() and PERCENT_RANK()", S),
    FeatureEntry::new("window.tumble", "WINDOW", "TUMBLE(col, interval) streaming window", S),
    FeatureEntry::new("window.hop", "WINDOW", "HOP(col, slide, size) sliding window", S),
    FeatureEntry::new("window.session", "WINDOW", "Session window on inactivity gap", S),

    // ── CTE ──────────────────────────────────────────────────────────────────
    FeatureEntry::new("cte.non_recursive", "CTE", "WITH … AS (…) non-recursive CTEs", S),
    FeatureEntry::new("cte.recursive", "CTE", "WITH RECURSIVE … (UNION ALL base + recursive)", S),
    FeatureEntry::new("cte.multiple", "CTE", "Multiple CTEs in one WITH clause", S),

    // ── SET OPERATIONS ────────────────────────────────────────────────────────
    FeatureEntry::new("set.union_all", "SET", "UNION ALL", S),
    FeatureEntry::new("set.union_distinct", "SET", "UNION (DISTINCT)", S),
    FeatureEntry::new("set.intersect", "SET", "INTERSECT", S),
    FeatureEntry::new("set.except", "SET", "EXCEPT", S),

    // ── LATERAL / UNNEST ─────────────────────────────────────────────────────
    FeatureEntry::new("lateral.unnest", "LATERAL", "UNNEST(array_col) in FROM clause", S),
    FeatureEntry::new("lateral.generate_series", "LATERAL", "generate_series() table function", S),
    FeatureEntry::new("lateral.cross_join_unnest", "LATERAL", "CROSS JOIN UNNEST(…) AS t(col)", S),

    // ── PIVOT / UNPIVOT ───────────────────────────────────────────────────────
    FeatureEntry::new("pivot.pivot", "PIVOT", "PIVOT(agg FOR col IN (v1, v2, …))", S),
    FeatureEntry::new("pivot.unpivot", "PIVOT", "UNPIVOT(value FOR col IN (c1, c2, …))", S),

    // ── DML ──────────────────────────────────────────────────────────────────
    FeatureEntry::new("dml.insert_into", "DML", "INSERT INTO table SELECT …", S),
    FeatureEntry::new("dml.insert_overwrite", "DML", "INSERT OVERWRITE (full partition replace)", S),
    FeatureEntry::new("dml.delete", "DML", "DELETE FROM table WHERE …", P)
        .with_note("supported on Iceberg tables; in-memory and Parquet tables require rewrite"),
    FeatureEntry::new("dml.update", "DML", "UPDATE table SET col = … WHERE …", P)
        .with_note("supported on Iceberg tables via MERGE rewrite"),
    FeatureEntry::new("dml.merge", "DML", "MERGE INTO target USING source ON … WHEN MATCHED …", S),
    FeatureEntry::new("dml.iceberg_merge", "DML", "Atomic Iceberg MERGE with row-level deletes", S),

    // ── DDL ──────────────────────────────────────────────────────────────────
    FeatureEntry::new("ddl.create_external_table", "DDL", "CREATE EXTERNAL TABLE … STORED AS …", S),
    FeatureEntry::new("ddl.create_view", "DDL", "CREATE VIEW name AS SELECT …", S),
    FeatureEntry::new("ddl.create_function", "DDL", "CREATE FUNCTION … LANGUAGE SQL|PYTHON", S),
    FeatureEntry::new("ddl.drop_table", "DDL", "DROP TABLE [IF EXISTS]", S),
    FeatureEntry::new("ddl.drop_view", "DDL", "DROP VIEW [IF EXISTS]", S),
    FeatureEntry::new("ddl.create_table_as", "DDL", "CREATE TABLE … AS SELECT (CTAS)", P)
        .with_note("supported via INSERT OVERWRITE or external-table pattern"),
    FeatureEntry::new("ddl.alter_table", "DDL", "ALTER TABLE ADD/DROP COLUMN, RENAME", P)
        .with_note("Iceberg schema evolution via ALTER TABLE is supported"),

    // ── TEMPORAL ─────────────────────────────────────────────────────────────
    FeatureEntry::new("temporal.as_of", "TEMPORAL", "AS OF TIMESTAMP point-in-time queries", S),
    FeatureEntry::new("temporal.match_recognize", "TEMPORAL", "MATCH_RECOGNIZE pattern matching over ordered rows", S),
    FeatureEntry::new("temporal.system_time", "TEMPORAL", "FOR SYSTEM_TIME AS OF (Iceberg time-travel)", P)
        .with_note("alias for AS OF on Iceberg tables"),

    // ── PREPARED STATEMENTS ───────────────────────────────────────────────────
    FeatureEntry::new("prepared.create", "PREPARED", "CREATE PREPARED STATEMENT via Flight SQL action", S),
    FeatureEntry::new("prepared.execute", "PREPARED", "Execute prepared statement by handle", S),
    FeatureEntry::new("prepared.close", "PREPARED", "CLOSE PREPARED STATEMENT to release server memory", S),
    FeatureEntry::new("prepared.parameters", "PREPARED", "Positional parameter binding ($1, $2, …)", L),

    // ── OPERATION CONTROL ────────────────────────────────────────────────────
    FeatureEntry::new("operation.id", "OPERATION", "Operation IDs for query tracking", S),
    FeatureEntry::new("operation.cancel", "OPERATION", "Cancel a running operation by ID", S),
    FeatureEntry::new("operation.timeout", "OPERATION", "Per-query execution timeout", S),
    FeatureEntry::new("operation.progress", "OPERATION", "Query progress reporting via QueryHandle", S),

    // ── ERROR HANDLING ────────────────────────────────────────────────────────
    FeatureEntry::new("error.sqlstate", "ERROR", "SQLSTATE codes on error responses", S),
    FeatureEntry::new("error.error_position", "ERROR", "Source line/column in error messages", P)
        .with_note("DataFusion provides message but not structured position"),

    // ── FLIGHT SQL ────────────────────────────────────────────────────────────
    FeatureEntry::new("flight.get_flight_info", "FLIGHT SQL", "GetFlightInfo for statement execution", S),
    FeatureEntry::new("flight.do_get", "FLIGHT SQL", "DoGet streaming result delivery", S),
    FeatureEntry::new("flight.prepared_statements", "FLIGHT SQL", "Prepared statement create/execute/close", S),
    FeatureEntry::new("flight.do_action", "FLIGHT SQL", "DoAction for custom Krishiv operations", S),
    FeatureEntry::new("flight.get_sql_info", "FLIGHT SQL", "GetSqlInfo capability introspection", S),
    FeatureEntry::new("flight.auth", "FLIGHT SQL", "Bearer token authentication", S),
    FeatureEntry::new("flight.policy", "FLIGHT SQL", "Table-level access policy enforcement", S),
    FeatureEntry::new("flight.transactions", "FLIGHT SQL", "BEGIN/COMMIT/ROLLBACK transactions", L),
    FeatureEntry::new("flight.schemas", "FLIGHT SQL", "GetDbSchemas / GetTables catalog introspection", P)
        .with_note("tables listed via Krishiv catalog; schema introspection via get_sql_info"),

    // ── STREAMING SQL ─────────────────────────────────────────────────────────
    FeatureEntry::new("streaming.continuous_select", "STREAMING", "Continuous SELECT over unbounded input", S),
    FeatureEntry::new("streaming.window_agg", "STREAMING", "Windowed aggregations over streaming input", S),
    FeatureEntry::new("streaming.watermark", "STREAMING", "Event-time watermarks for late-data handling", S),
    FeatureEntry::new("streaming.interval_join", "STREAMING", "Streaming-to-streaming interval join", S),
    FeatureEntry::new("streaming.cep", "STREAMING", "MATCH_RECOGNIZE CEP over streaming input", S),
    FeatureEntry::new("streaming.dedup", "STREAMING", "Streaming deduplication (dropDuplicates)", S),
    FeatureEntry::new("streaming.sink_modes", "STREAMING", "Append / Update / Complete output modes", S),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_matrix_is_non_empty() {
        assert!(!feature_matrix().is_empty());
    }

    #[test]
    fn all_ids_are_unique() {
        let ids: Vec<&str> = feature_matrix().iter().map(|e| e.id).collect();
        let mut seen = std::collections::HashSet::new();
        for id in &ids {
            assert!(seen.insert(*id), "duplicate feature id: {id}");
        }
    }

    #[test]
    fn features_for_category_returns_subset() {
        let join_features = features_for_category("JOIN");
        assert!(!join_features.is_empty());
        for f in &join_features {
            assert!(f.category.to_uppercase().starts_with("JOIN"), "{}", f.id);
        }
    }

    #[test]
    fn features_by_status_supported_is_non_empty() {
        let supported = features_by_status(FeatureStatus::Supported);
        assert!(!supported.is_empty());
    }

    #[test]
    fn feature_entry_display_includes_id_and_status() {
        let entry = feature_matrix()
            .iter()
            .find(|e| e.id == "select.distinct")
            .unwrap();
        let s = entry.to_string();
        assert!(s.contains("select.distinct"));
        assert!(s.contains("supported"));
    }

    #[test]
    fn feature_entry_display_with_note() {
        let entry = feature_matrix()
            .iter()
            .find(|e| e.note.is_some())
            .unwrap();
        let s = entry.to_string();
        assert!(s.contains('('));
    }

    #[test]
    fn feature_status_display() {
        assert_eq!(FeatureStatus::Supported.to_string(), "supported");
        assert_eq!(FeatureStatus::Partial.to_string(), "partial");
        assert_eq!(FeatureStatus::Planned.to_string(), "planned");
        assert_eq!(FeatureStatus::NotApplicable.to_string(), "n/a");
    }

    #[test]
    fn flight_sql_features_present() {
        let flight = features_for_category("FLIGHT");
        assert!(flight.iter().any(|e| e.id == "flight.get_flight_info"));
        assert!(flight.iter().any(|e| e.id == "flight.prepared_statements"));
    }

    #[test]
    fn operation_features_present() {
        let ops = features_for_category("OPERATION");
        assert!(ops.iter().any(|e| e.id == "operation.cancel"));
        assert!(ops.iter().any(|e| e.id == "operation.timeout"));
    }
}
