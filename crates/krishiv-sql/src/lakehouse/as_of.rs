//! Time travel SQL preprocessing (R18 S4, ADR-18.3).

use krishiv_connectors::lakehouse::AsOfSpec;
use sqlparser::ast::{
    Expr, Select, SetExpr, Statement, TableFactor, TableVersion, TableWithJoins, Value,
};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;

/// Parsed `AS OF` qualifier attached to a table reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsOfTableRef {
    pub table: String,
    pub spec: AsOfSpec,
}

/// Strip `AS OF` clauses and return rewritten SQL plus qualifiers.
pub fn preprocess_as_of_sql(sql: &str) -> Result<(String, Vec<AsOfTableRef>), String> {
    let dialect = DatabricksDialect {};
    let mut stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {e}"))?;
    if stmts.len() != 1 {
        return Err("expected a single SQL statement".into());
    }
    let mut refs = Vec::new();
    if let Some(stmt) = stmts.first_mut() {
        process_statement(stmt, &mut refs);
    }
    let clean_sql = stmts.first().map(|s| s.to_string()).unwrap_or_default();
    Ok((clean_sql, refs))
}

fn process_statement(stmt: &mut Statement, refs: &mut Vec<AsOfTableRef>) {
    if let Statement::Query(query) = stmt {
        process_query(query, refs);
    }
}

fn process_query(query: &mut sqlparser::ast::Query, refs: &mut Vec<AsOfTableRef>) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            process_query(&mut cte.query, refs);
        }
    }
    process_set_expr(&mut query.body, refs);
}

fn process_set_expr(set_expr: &mut SetExpr, refs: &mut Vec<AsOfTableRef>) {
    match set_expr {
        SetExpr::Select(select) => process_select(select, refs),
        SetExpr::Query(query) => process_query(query, refs),
        SetExpr::SetOperation { left, right, .. } => {
            process_set_expr(left, refs);
            process_set_expr(right, refs);
        }
        _ => {}
    }
}

fn process_select(select: &mut Select, refs: &mut Vec<AsOfTableRef>) {
    for twj in &mut select.from {
        process_table_with_joins(twj, refs);
    }
}

fn process_table_with_joins(twj: &mut TableWithJoins, refs: &mut Vec<AsOfTableRef>) {
    process_table_factor(&mut twj.relation, refs);
    for join in &mut twj.joins {
        process_table_factor(&mut join.relation, refs);
    }
}

fn process_table_factor(tf: &mut TableFactor, refs: &mut Vec<AsOfTableRef>) {
    match tf {
        TableFactor::Table { name, version, .. } => {
            if let Some(ver) = version.take() {
                let table_name = name.to_string();
                if let Some(spec) = table_version_to_spec(ver) {
                    refs.push(AsOfTableRef {
                        table: table_name,
                        spec,
                    });
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            process_query(subquery, refs);
        }
        _ => {}
    }
}

fn table_version_to_spec(ver: TableVersion) -> Option<AsOfSpec> {
    match ver {
        TableVersion::VersionAsOf(Expr::Value(vws)) => match vws.value {
            Value::Number(n, _) => {
                let v = n.parse::<i64>().ok()?;
                Some(AsOfSpec::Version(v))
            }
            Value::SingleQuotedString(s) => {
                let v = s.parse::<i64>().ok()?;
                Some(AsOfSpec::Version(v))
            }
            _ => None,
        },
        TableVersion::TimestampAsOf(Expr::Value(vws)) => match vws.value {
            Value::SingleQuotedString(s) => AsOfSpec::parse(&s).ok(),
            _ => None,
        },
        TableVersion::ForSystemTimeAsOf(Expr::TypedString(ts)) => {
            let s = ts.value.value.into_string()?;
            AsOfSpec::parse(&s).ok()
        }
        TableVersion::ForSystemTimeAsOf(Expr::Value(vws)) => match vws.value {
            Value::SingleQuotedString(s) => AsOfSpec::parse(&s).ok(),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_connectors::lakehouse::AsOfSpec;

    #[test]
    fn parses_version_as_of() {
        let (sql, refs) = preprocess_as_of_sql("SELECT * FROM orders VERSION AS OF 3").unwrap();
        assert!(sql.contains("FROM orders"));
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].spec, AsOfSpec::Version(3));
    }

    #[test]
    fn parses_timestamp_as_of() {
        let (sql, refs) =
            preprocess_as_of_sql("SELECT * FROM events TIMESTAMP AS OF '2024-01-15T10:30:00Z'")
                .unwrap();
        assert!(!sql.contains("TIMESTAMP AS OF"));
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn parses_system_time_as_of() {
        let (sql, refs) = preprocess_as_of_sql(
            "SELECT * FROM tbl FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01T00:00:00Z'",
        )
        .unwrap();
        assert!(!sql.contains("FOR SYSTEM_TIME AS OF"));
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn handles_join_as_of() {
        let (sql, refs) = preprocess_as_of_sql(
            "SELECT * FROM a VERSION AS OF 1 JOIN b VERSION AS OF 2 ON a.id = b.id",
        )
        .unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].spec, AsOfSpec::Version(1));
        assert_eq!(refs[1].spec, AsOfSpec::Version(2));
        assert!(!sql.contains("VERSION AS OF"));
    }

    #[test]
    fn handles_subquery_as_of() {
        let (sql, refs) =
            preprocess_as_of_sql("SELECT * FROM (SELECT * FROM inner_tbl VERSION AS OF 42) AS sub")
                .unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].spec, AsOfSpec::Version(42));
        assert!(!sql.contains("VERSION AS OF"));
    }

    #[test]
    fn handles_cte_as_of() {
        let (sql, refs) = preprocess_as_of_sql(
            "WITH cte AS (SELECT * FROM inner_tbl VERSION AS OF 99) SELECT * FROM cte",
        )
        .unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].spec, AsOfSpec::Version(99));
        assert!(!sql.contains("VERSION AS OF"));
    }

    #[test]
    fn ignores_string_literals() {
        let (sql, refs) =
            preprocess_as_of_sql("SELECT * FROM t WHERE name = 'VERSION AS OF 123'").unwrap();
        assert_eq!(refs.len(), 0);
        assert!(sql.contains("VERSION AS OF 123"));
    }

    #[test]
    fn no_as_of_passes_through() {
        let input = "SELECT id, name FROM users WHERE age > 21";
        let (sql, refs) = preprocess_as_of_sql(input).unwrap();
        assert_eq!(refs.len(), 0);
        assert_eq!(sql, input);
    }

    #[test]
    fn handles_union_as_of() {
        let (sql, refs) = preprocess_as_of_sql(
            "SELECT * FROM a VERSION AS OF 1 UNION ALL SELECT * FROM b VERSION AS OF 2",
        )
        .unwrap();
        assert_eq!(refs.len(), 2);
        assert!(!sql.contains("VERSION AS OF"));
    }
}
