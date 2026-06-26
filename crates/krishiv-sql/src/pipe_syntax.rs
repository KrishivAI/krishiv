//! P10: SQL Pipe Syntax — `FROM t |> WHERE x |> SELECT y` → standard SQL.
//!
//! Spark 4.0 introduced pipe syntax for SQL readability. This module provides
//! a pre-processor that converts pipe syntax to standard SQL before parsing.
//!
//! # Syntax
//!
//! ```sql
//! -- Pipe syntax
//! FROM orders |> WHERE amount > 100 |> SELECT customer_id, amount
//!
//! -- Equivalent standard SQL
//! SELECT customer_id, amount FROM orders WHERE amount > 100
//! ```
//!
//! # Supported Pipe Operators
//!
//! - `|> WHERE <condition>` — filter rows
//! - `|> SELECT <columns>` — project columns
//! - `|> GROUP BY <columns>` — group rows
//! - `|> ORDER BY <columns>` — sort rows
//! - `|> LIMIT <n>` — limit output rows
//! - `|> JOIN <table> ON <condition>` — join with another table
//! - `|> LEFT JOIN <table> ON <condition>` — left join
//! - `|> RIGHT JOIN <table> ON <condition>` — right join
//! - `|> INNER JOIN <table> ON <condition>` — inner join
//! - `|> CROSS JOIN <table>` — cross join

use std::fmt;

/// Errors that can occur during pipe syntax processing.
#[derive(Debug)]
pub enum PipeSyntaxError {
    /// Invalid pipe syntax.
    InvalidSyntax(String),
    /// Unsupported pipe operator.
    UnsupportedOperator(String),
}

impl fmt::Display for PipeSyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSyntax(msg) => write!(f, "invalid pipe syntax: {msg}"),
            Self::UnsupportedOperator(msg) => write!(f, "unsupported pipe operator: {msg}"),
        }
    }
}

impl std::error::Error for PipeSyntaxError {}

/// Pre-process SQL to convert pipe syntax to standard SQL.
///
/// If the SQL does not contain pipe syntax, it is returned unchanged.
pub fn process_pipe_syntax(sql: &str) -> Result<String, PipeSyntaxError> {
    let trimmed = sql.trim();

    // Check if this is a pipe syntax query (starts with FROM and contains |>)
    if !trimmed.to_uppercase().starts_with("FROM ") || !trimmed.contains("|>") {
        return Ok(trimmed.to_string());
    }

    // Split on pipe operator
    let parts: Vec<&str> = trimmed.split("|>").collect();
    if parts.len() < 2 {
        return Ok(trimmed.to_string());
    }

    // First part is the FROM clause
    let from_clause = parts[0].trim();
    if !from_clause.to_uppercase().starts_with("FROM ") {
        return Err(PipeSyntaxError::InvalidSyntax(
            "pipe syntax must start with FROM".into(),
        ));
    }

    let table_name = from_clause[5..].trim();

    // Process remaining parts
    let mut where_clause = String::new();
    let mut select_clause = String::new();
    let mut group_by_clause = String::new();
    let mut order_by_clause = String::new();
    let mut limit_clause = String::new();
    let mut join_clauses = Vec::new();

    for part in &parts[1..] {
        let part = part.trim();
        let upper = part.to_uppercase();

        if upper.starts_with("WHERE ") {
            where_clause = format!("WHERE {}", &part[6..]);
        } else if upper.starts_with("SELECT ") {
            select_clause = format!("SELECT {}", &part[7..]);
        } else if upper.starts_with("GROUP BY ") {
            group_by_clause = format!("GROUP BY {}", &part[9..]);
        } else if upper.starts_with("ORDER BY ") {
            order_by_clause = format!("ORDER BY {}", &part[9..]);
        } else if upper.starts_with("LIMIT ") {
            limit_clause = format!("LIMIT {}", &part[6..]);
        } else if upper.starts_with("JOIN ") || upper.starts_with("INNER JOIN ") {
            join_clauses.push(part.to_string());
        } else if upper.starts_with("LEFT JOIN ") {
            join_clauses.push(part.to_string());
        } else if upper.starts_with("RIGHT JOIN ") {
            join_clauses.push(part.to_string());
        } else if upper.starts_with("CROSS JOIN ") {
            join_clauses.push(part.to_string());
        } else {
            return Err(PipeSyntaxError::UnsupportedOperator(part.to_string()));
        }
    }

    // Build standard SQL
    let mut sql = String::new();

    if select_clause.is_empty() {
        sql.push_str("SELECT *");
    } else {
        sql.push_str(&select_clause);
    }

    sql.push_str(" FROM ");
    sql.push_str(table_name);

    for join in &join_clauses {
        sql.push(' ');
        sql.push_str(join);
    }

    if !where_clause.is_empty() {
        sql.push(' ');
        sql.push_str(&where_clause);
    }

    if !group_by_clause.is_empty() {
        sql.push(' ');
        sql.push_str(&group_by_clause);
    }

    if !order_by_clause.is_empty() {
        sql.push(' ');
        sql.push_str(&order_by_clause);
    }

    if !limit_clause.is_empty() {
        sql.push(' ');
        sql.push_str(&limit_clause);
    }

    Ok(sql)
}

/// Check if SQL contains pipe syntax.
pub fn has_pipe_syntax(sql: &str) -> bool {
    let trimmed = sql.trim();
    trimmed.to_uppercase().starts_with("FROM ") && trimmed.contains("|>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_pipe_syntax() {
        let sql = "FROM orders |> WHERE amount > 100 |> SELECT customer_id, amount";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(
            result,
            "SELECT customer_id, amount FROM orders WHERE amount > 100"
        );
    }

    #[test]
    fn pipe_syntax_with_group_by() {
        let sql = "FROM orders |> GROUP BY region |> SELECT region, SUM(amount) as total";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(
            result,
            "SELECT region, SUM(amount) as total FROM orders GROUP BY region"
        );
    }

    #[test]
    fn pipe_syntax_with_order_by_and_limit() {
        let sql = "FROM orders |> ORDER BY amount DESC |> LIMIT 10";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(result, "SELECT * FROM orders ORDER BY amount DESC LIMIT 10");
    }

    #[test]
    fn pipe_syntax_with_join() {
        let sql = "FROM orders |> JOIN customers ON orders.customer_id = customers.id |> SELECT *";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
        );
    }

    #[test]
    fn standard_sql_unchanged() {
        let sql = "SELECT * FROM orders WHERE amount > 100";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn has_pipe_syntax_detection() {
        assert!(has_pipe_syntax("FROM t |> SELECT *"));
        assert!(!has_pipe_syntax("SELECT * FROM t"));
        assert!(!has_pipe_syntax("FROM t"));
    }
}
