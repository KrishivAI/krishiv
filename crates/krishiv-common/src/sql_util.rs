//! SQL string utilities shared across the workspace.
//!
//! Canonical implementations for:
//! - [`quote_identifier`]  — double-quote a SQL identifier, escaping embedded `"`.
//! - [`quote_qualified`]   — quote a schema-qualified name (`schema.table`).
//! - [`split_sql_statements`] — split a SQL string on `;`, respecting quoted strings.
//!
//! Previous copies:
//!   `krishiv-api/src/session.rs`       `fn quote_identifier`
//!   `krishiv-plan/src/expression.rs`   `fn quote_identifier`
//!   `krishiv-sql/src/lib.rs`           `fn quote_identifier`
//!   `krishiv-connectors/src/jdbc.rs`   `fn quote_pg_ident` / `fn quote_pg_relation`
//!   `krishiv/src/query_cli.rs`         `fn split_statements` (buggy — ignored quotes)
//!   `krishiv/src/pipeline_cmd.rs`      `fn split_statements` (buggy — ignored quotes)

/// Quote a SQL identifier with ANSI double-quotes, escaping any embedded `"`
/// as `""`. Prevents SQL injection when interpolating user-supplied names
/// into SQL strings.
///
/// ```
/// use krishiv_common::sql_util::quote_identifier;
/// assert_eq!(quote_identifier("my_table"),       "\"my_table\"");
/// assert_eq!(quote_identifier("bad\"name"),      "\"bad\"\"name\"");
/// assert_eq!(quote_identifier("schema.table"),   "\"schema.table\"");
/// ```
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a schema-qualified SQL name by double-quoting each `.`-separated
/// component individually.
///
/// ```
/// use krishiv_common::sql_util::quote_qualified;
/// assert_eq!(quote_qualified("public.events"),  "\"public\".\"events\"");
/// assert_eq!(quote_qualified("my_table"),       "\"my_table\"");
/// assert_eq!(quote_qualified("a.b.c"),          "\"a\".\"b\".\"c\"");
/// ```
pub fn quote_qualified(name: &str) -> String {
    name.split('.')
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

/// Split a SQL string into individual statements on `;`, respecting
/// single-quoted string literals so that semicolons inside `'…'` are not
/// treated as statement separators.
///
/// SQL's `''` escape (two adjacent single-quotes inside a string literal) is
/// handled correctly by toggling the in-quote flag on every `'` — a pair
/// toggles twice, returning to the same state.
///
/// `--` line comments are stripped before splitting so that a trailing comment
/// like `SELECT 1 -- returns one;` doesn't produce a spurious empty statement.
///
/// ```
/// use krishiv_common::sql_util::split_sql_statements;
///
/// let stmts = split_sql_statements("SELECT 1; SELECT 2");
/// assert_eq!(stmts, ["SELECT 1", "SELECT 2"]);
///
/// // Semicolons inside quoted strings are NOT statement boundaries.
/// let stmts = split_sql_statements("SELECT 'a;b'; SELECT 2");
/// assert_eq!(stmts, ["SELECT 'a;b'", "SELECT 2"]);
///
/// // -- comments are stripped.
/// let stmts = split_sql_statements("SELECT 1 -- comment\n; SELECT 2");
/// assert_eq!(stmts, ["SELECT 1", "SELECT 2"]);
/// ```
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // Strip `--` line comments (only outside quoted strings).
            '-' if !in_single_quote && chars.peek() == Some(&'-') => {
                chars.next(); // consume second '-'
                for c2 in chars.by_ref() {
                    if c2 == '\n' {
                        // Keep the newline so multi-line whitespace collapse
                        // in the trimming step below still works correctly.
                        current.push('\n');
                        break;
                    }
                }
            }
            '\'' => {
                in_single_quote = !in_single_quote;
                current.push(c);
            }
            ';' if !in_single_quote => {
                let stmt = current.split_whitespace().collect::<Vec<_>>().join(" ");
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    // Final statement (no trailing semicolon).
    let stmt = current.split_whitespace().collect::<Vec<_>>().join(" ");
    if !stmt.is_empty() {
        statements.push(stmt);
    }
    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_normal() {
        assert_eq!(quote_identifier("my_table"), "\"my_table\"");
    }

    #[test]
    fn quote_identifier_escapes_double_quote() {
        assert_eq!(quote_identifier("bad\"name"), "\"bad\"\"name\"");
    }

    #[test]
    fn quote_identifier_injection_payload() {
        // An injection attempt like `" DROP TABLE users --` must be safely quoted.
        assert_eq!(
            quote_identifier("\" DROP TABLE users --"),
            "\"\"\" DROP TABLE users --\""
        );
    }

    #[test]
    fn quote_qualified_single_component() {
        assert_eq!(quote_qualified("events"), "\"events\"");
    }

    #[test]
    fn quote_qualified_schema_table() {
        assert_eq!(quote_qualified("public.events"), "\"public\".\"events\"");
    }

    #[test]
    fn quote_qualified_three_parts() {
        assert_eq!(
            quote_qualified("db.schema.tbl"),
            "\"db\".\"schema\".\"tbl\""
        );
    }

    #[test]
    fn split_basic() {
        assert_eq!(
            split_sql_statements("SELECT 1; SELECT 2"),
            ["SELECT 1", "SELECT 2"]
        );
    }

    #[test]
    fn split_trailing_semicolon() {
        assert_eq!(split_sql_statements("SELECT 1;"), ["SELECT 1"]);
    }

    #[test]
    fn split_empty() {
        assert!(split_sql_statements("").is_empty());
        assert!(split_sql_statements("   ").is_empty());
        assert!(split_sql_statements(";; ;").is_empty());
    }

    #[test]
    fn split_ignores_semicolon_in_single_quote() {
        // Regression: naive split(';') tears this statement in half.
        assert_eq!(
            split_sql_statements("SELECT 'a;b'; SELECT 2"),
            ["SELECT 'a;b'", "SELECT 2"]
        );
    }

    #[test]
    fn split_handles_escaped_quote_inside_string() {
        // SQL escapes a single quote by doubling it ('it''s fine').
        assert_eq!(
            split_sql_statements("SELECT 'it''s fine'; SELECT 2"),
            ["SELECT 'it''s fine'", "SELECT 2"]
        );
    }

    #[test]
    fn split_strips_line_comments() {
        assert_eq!(
            split_sql_statements("SELECT 1 -- a comment\n; SELECT 2"),
            ["SELECT 1", "SELECT 2"]
        );
    }

    #[test]
    fn split_normalizes_whitespace() {
        assert_eq!(
            split_sql_statements("SELECT\n  *\nFROM\n  t"),
            ["SELECT * FROM t"]
        );
    }

    #[test]
    fn split_parquet_path_with_semicolon() {
        // Real-world regression: file paths in string literals must not split.
        assert_eq!(
            split_sql_statements("SELECT * FROM parquet('/data/a;b.parquet')"),
            ["SELECT * FROM parquet('/data/a;b.parquet')"]
        );
    }
}
