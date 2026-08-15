//! Time travel SQL preprocessing (R18 S4, ADR-18.3).

use krishiv_connectors::lakehouse::AsOfSpec;
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Select, SetExpr, Statement, TableFactor, TableVersion,
    TableWithJoins, Value,
};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;

/// Prefix for the generated names that pinned tables are registered under.
///
/// Deliberately not a legal user identifier prefix in practice, so a rewritten
/// reference cannot collide with a real table.
pub const AS_OF_ALIAS_PREFIX: &str = "__krishiv_as_of_";

/// Parsed `AS OF` qualifier attached to a table reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsOfTableRef {
    /// The table as the user wrote it, e.g. `delta.\`/data/orders\`` or
    /// `iceberg.sales.orders`. Used to resolve the pinned snapshot.
    pub table: String,
    /// The name the *rewritten* SQL now refers to.
    ///
    /// `preprocess_as_of_sql` renames the table reference to this alias, and
    /// whoever resolves the qualifier must register the pinned table under it.
    /// Without the rename the two halves never met: the clause was stripped,
    /// a provider was registered under a mangled name nothing referenced, and
    /// the query was left naming a table DataFusion could not resolve.
    pub alias: String,
    pub spec: AsOfSpec,
}

/// Qualifiers found while walking a statement, plus any that could not be
/// mapped to an [`AsOfSpec`].
#[derive(Default)]
struct AsOfScan {
    refs: Vec<AsOfTableRef>,
    /// Descriptions of `AS OF` qualifiers the mapper did not understand.
    ///
    /// These must not be ignored. The clause is removed from the statement
    /// before DataFusion ever sees it, so an unmapped qualifier that is merely
    /// skipped leaves a query that silently reads the **current** table
    /// version — the same "time travel returned the present" failure already
    /// fixed in `lakehouse/providers`.
    unsupported: Vec<String>,
}

/// Strip `AS OF` clauses and return rewritten SQL plus qualifiers.
///
/// Errors if a qualifier is present but cannot be mapped, rather than dropping
/// it: silently returning the latest snapshot for a time-travel query is worse
/// than refusing the query.
pub fn preprocess_as_of_sql(sql: &str) -> Result<(String, Vec<AsOfTableRef>), String> {
    let dialect = DatabricksDialect {};
    let mut stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {e}"))?;
    if stmts.len() != 1 {
        return Err("expected a single SQL statement".into());
    }
    let mut scan = AsOfScan::default();
    if let Some(stmt) = stmts.first_mut() {
        process_statement(stmt, &mut scan);
    }
    if !scan.unsupported.is_empty() {
        return Err(format!(
            "unsupported AS OF qualifier: {}. Refusing the query rather than dropping the \
             clause, which would read the current table version instead of the requested one.",
            scan.unsupported.join("; ")
        ));
    }
    let clean_sql = stmts.first().map(|s| s.to_string()).unwrap_or_default();
    Ok((clean_sql, scan.refs))
}

fn process_statement(stmt: &mut Statement, scan: &mut AsOfScan) {
    if let Statement::Query(query) = stmt {
        process_query(query, scan);
    }
}

fn process_query(query: &mut sqlparser::ast::Query, scan: &mut AsOfScan) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            process_query(&mut cte.query, scan);
        }
    }
    process_set_expr(&mut query.body, scan);
}

fn process_set_expr(set_expr: &mut SetExpr, scan: &mut AsOfScan) {
    match set_expr {
        SetExpr::Select(select) => process_select(select, scan),
        SetExpr::Query(query) => process_query(query, scan),
        SetExpr::SetOperation { left, right, .. } => {
            process_set_expr(left, scan);
            process_set_expr(right, scan);
        }
        _ => {}
    }
}

fn process_select(select: &mut Select, scan: &mut AsOfScan) {
    for twj in &mut select.from {
        process_table_with_joins(twj, scan);
    }
}

fn process_table_with_joins(twj: &mut TableWithJoins, scan: &mut AsOfScan) {
    process_table_factor(&mut twj.relation, scan);
    for join in &mut twj.joins {
        process_table_factor(&mut join.relation, scan);
    }
}

fn process_table_factor(tf: &mut TableFactor, scan: &mut AsOfScan) {
    match tf {
        TableFactor::Table { name, version, .. } => {
            if let Some(ver) = version.take() {
                let table_name = name.to_string();
                // Describe before consuming: the qualifier is already removed
                // from the statement at this point, so if it cannot be mapped
                // the only remaining record of it is this string.
                let described = format!("{ver:?}");
                match table_version_to_spec(ver) {
                    Some(spec) => {
                        // Rename the reference to a generated alias and hand
                        // that alias back, so the resolver has a name it can
                        // register the pinned snapshot under and the rewritten
                        // SQL actually refers to it.
                        let alias = format!("{AS_OF_ALIAS_PREFIX}{}", scan.refs.len());
                        *name =
                            ObjectName(vec![ObjectNamePart::Identifier(Ident::new(alias.clone()))]);
                        scan.refs.push(AsOfTableRef {
                            table: table_name,
                            alias,
                            spec,
                        });
                    }
                    None => scan
                        .unsupported
                        .push(format!("on table '{table_name}': {described}")),
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            process_query(subquery, scan);
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
        // `TIMESTAMP AS OF TIMESTAMP '…'` — the typed-literal spelling. It was
        // handled for `FOR SYSTEM_TIME AS OF` below but not here, so this form
        // mapped to `None` and the clause was dropped.
        TableVersion::TimestampAsOf(Expr::TypedString(ts)) => {
            let s = ts.value.value.into_string()?;
            AsOfSpec::parse(&s).ok()
        }
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

    /// This used to assert `sql.contains("FROM orders")` — that the table name
    /// survives the rewrite untouched. That was the bug: the clause was
    /// stripped, the name left alone, and the pinned provider registered under
    /// a third name nothing referenced, so no time-travel query could resolve.
    /// The reference must now be renamed to the alias the resolver registers
    /// under, with the original preserved on the ref for snapshot lookup.
    #[test]
    fn parses_version_as_of() {
        let (sql, refs) = preprocess_as_of_sql("SELECT * FROM orders VERSION AS OF 3").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0].table, "orders",
            "the original name is kept on the ref"
        );
        assert_eq!(refs[0].spec, AsOfSpec::Version(3));
        assert!(
            sql.contains(&refs[0].alias),
            "the rewritten SQL must name the alias the pinned table is registered under: {sql}"
        );
        assert!(
            !sql.contains("VERSION AS OF"),
            "the qualifier is consumed: {sql}"
        );
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

    /// The typed-literal spelling must pin a version like the quoted one does.
    /// It previously mapped to `None`, and an unmapped qualifier was dropped —
    /// so this query read the current snapshot.
    #[test]
    fn parses_timestamp_as_of_typed_literal() {
        let (sql, refs) = preprocess_as_of_sql(
            "SELECT * FROM t TIMESTAMP AS OF TIMESTAMP '2024-06-01T00:00:00Z'",
        )
        .unwrap();
        assert_eq!(refs.len(), 1, "the qualifier must be captured, not dropped");
        assert!(!sql.contains("AS OF"));
    }

    /// An `AS OF` the mapper cannot understand must fail the query.
    ///
    /// `version.take()` removes the clause before DataFusion sees the SQL, so
    /// skipping an unmapped qualifier silently returns the *current* table
    /// version — the "time travel returned the present" bug.
    #[test]
    fn unmappable_as_of_is_an_error_not_a_silent_drop() {
        // Parses as a numeric literal, then overflows `i64` — so it reaches the
        // mapper and maps to nothing. (A quoted version is rejected earlier by
        // sqlparser itself, which is why it is not the example here.)
        let error = preprocess_as_of_sql("SELECT * FROM t VERSION AS OF 99999999999999999999999")
            .expect_err("an out-of-range VERSION must not be silently dropped");
        assert!(
            error.contains("unsupported AS OF qualifier"),
            "expected an explicit refusal, got: {error}"
        );
        assert!(error.contains("on table 't'"), "{error}");
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
