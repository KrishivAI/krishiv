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

    // Split on pipe operator. `split_first` rather than `parts[0]`/`&parts[1..]`:
    // the crate denies `clippy::indexing_slicing`, which this file never had to
    // satisfy while it was outside the module tree.
    let parts: Vec<&str> = trimmed.split("|>").collect();
    let Some((from_clause, stages)) = parts.split_first() else {
        return Ok(trimmed.to_string());
    };
    if stages.is_empty() {
        return Ok(trimmed.to_string());
    }

    // First part is the FROM clause
    let from_clause = from_clause.trim();
    if !from_clause.to_uppercase().starts_with("FROM ") {
        return Err(PipeSyntaxError::InvalidSyntax(
            "pipe syntax must start with FROM".into(),
        ));
    }

    let table_name = from_clause[5..].trim();

    // Pipe syntax is *sequential*: each stage applies to the result of the one
    // before it. Flattening the stages into one SELECT loses that, so the two
    // places where order is observable are tracked explicitly:
    //
    //  * a repeated stage cannot be expressed in a single flat SELECT, so it is
    //    rejected rather than silently overwriting the earlier one. Assigning
    //    `where_clause = …` per stage meant `|> WHERE a |> WHERE b` dropped
    //    `a` entirely and returned rows that never satisfied it;
    //  * a filter *after* a GROUP BY is a HAVING, not a WHERE. Emitting it as a
    //    WHERE applies it before grouping, which is a different query.
    let mut predicates: Vec<String> = Vec::new();
    let mut having: Vec<String> = Vec::new();
    let mut select_clause: Option<String> = None;
    let mut group_by_clause: Option<String> = None;
    let mut order_by_clause: Option<String> = None;
    let mut limit_clause: Option<String> = None;
    let mut join_clauses = Vec::new();

    /// Reject a second occurrence instead of discarding the first.
    fn set_once(
        slot: &mut Option<String>,
        value: String,
        operator: &str,
    ) -> Result<(), PipeSyntaxError> {
        if slot.is_some() {
            return Err(PipeSyntaxError::UnsupportedOperator(format!(
                "repeated |> {operator}: a pipeline with more than one {operator} stage cannot \
                 be expressed as a single SELECT; write it as a subquery"
            )));
        }
        *slot = Some(value);
        Ok(())
    }

    for part in stages {
        let part = part.trim();
        // ASCII folding so `strip_kw`'s byte offsets into `part` are valid.
        let upper = part.to_ascii_uppercase();

        if let Some(rest) = strip_kw(part, &upper, "WHERE ") {
            // Grouped already? Then this filters groups, i.e. HAVING.
            if group_by_clause.is_some() {
                having.push(rest.to_string());
            } else {
                predicates.push(rest.to_string());
            }
        } else if let Some(rest) = strip_kw(part, &upper, "SELECT ") {
            set_once(&mut select_clause, rest.to_string(), "SELECT")?;
        } else if let Some(rest) = strip_kw(part, &upper, "GROUP BY ") {
            set_once(&mut group_by_clause, rest.to_string(), "GROUP BY")?;
        } else if let Some(rest) = strip_kw(part, &upper, "ORDER BY ") {
            set_once(&mut order_by_clause, rest.to_string(), "ORDER BY")?;
        } else if let Some(rest) = strip_kw(part, &upper, "LIMIT ") {
            set_once(&mut limit_clause, rest.to_string(), "LIMIT")?;
        } else if upper.starts_with("JOIN ")
            || upper.starts_with("INNER JOIN ")
            || upper.starts_with("LEFT JOIN ")
            || upper.starts_with("RIGHT JOIN ")
            || upper.starts_with("CROSS JOIN ")
        {
            join_clauses.push(part.to_string());
        } else {
            return Err(PipeSyntaxError::UnsupportedOperator(part.to_string()));
        }
    }

    // Build standard SQL
    let mut sql = String::new();
    match &select_clause {
        Some(projection) => {
            sql.push_str("SELECT ");
            sql.push_str(projection);
        }
        None => sql.push_str("SELECT *"),
    }

    sql.push_str(" FROM ");
    sql.push_str(table_name);

    for join in &join_clauses {
        sql.push(' ');
        sql.push_str(join);
    }

    // Successive filters compose by conjunction, which is exactly what
    // `filter(filter(t, a), b)` means.
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&join_predicates(&predicates));
    }
    if let Some(group_by) = &group_by_clause {
        sql.push_str(" GROUP BY ");
        sql.push_str(group_by);
    }
    if !having.is_empty() {
        sql.push_str(" HAVING ");
        sql.push_str(&join_predicates(&having));
    }
    if let Some(order_by) = &order_by_clause {
        sql.push_str(" ORDER BY ");
        sql.push_str(order_by);
    }
    if let Some(limit) = &limit_clause {
        sql.push_str(" LIMIT ");
        sql.push_str(limit);
    }

    Ok(sql)
}

/// Strip a leading keyword, matching case-insensitively but returning the
/// original-case remainder. `upper` must be `part.to_uppercase()`; both are
/// ASCII keywords so the byte offsets line up.
fn strip_kw<'a>(part: &'a str, upper: &str, keyword: &str) -> Option<&'a str> {
    upper
        .starts_with(keyword)
        .then(|| part.get(keyword.len()..))
        .flatten()
        .map(str::trim)
        .filter(|rest| !rest.is_empty())
}

/// Combine filters with AND, parenthesising each so `a OR b` composed with `c`
/// does not silently become `a OR (b AND c)`.
fn join_predicates(parts: &[String]) -> String {
    if let [single] = parts {
        return single.clone();
    }
    parts
        .iter()
        .map(|p| format!("({p})"))
        .collect::<Vec<_>>()
        .join(" AND ")
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

    /// Successive filters compose by conjunction. Each stage used to *assign*
    /// `where_clause`, so `|> WHERE a |> WHERE b` dropped `a` and returned rows
    /// that never satisfied it.
    #[test]
    fn successive_where_stages_are_conjoined_not_overwritten() {
        let sql = "FROM orders |> WHERE amount > 100 |> WHERE region = 'US' |> SELECT id";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(
            result,
            "SELECT id FROM orders WHERE (amount > 100) AND (region = 'US')"
        );
    }

    /// Each filter is parenthesised so a disjunction cannot be re-associated.
    #[test]
    fn conjoined_filters_are_parenthesised() {
        let sql = "FROM t |> WHERE a OR b |> WHERE c";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(result, "SELECT * FROM t WHERE (a OR b) AND (c)");
    }

    /// A filter *after* a GROUP BY filters groups — that is HAVING. Emitting it
    /// as a WHERE would apply it before grouping, a different query.
    #[test]
    fn a_filter_after_group_by_becomes_having() {
        let sql = "FROM orders |> GROUP BY region |> WHERE SUM(amount) > 500 \
                   |> SELECT region, SUM(amount)";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(
            result,
            "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 500"
        );
    }

    /// ...and a filter *before* the GROUP BY stays a WHERE.
    #[test]
    fn a_filter_before_group_by_stays_a_where() {
        let sql = "FROM orders |> WHERE amount > 0 |> GROUP BY region";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(result, "SELECT * FROM orders WHERE amount > 0 GROUP BY region");
    }

    /// A repeated stage that cannot be flattened must be refused, not silently
    /// reduced to its last occurrence.
    #[test]
    fn repeated_unflattenable_stages_are_rejected() {
        for sql in [
            "FROM t |> SELECT a |> SELECT b",
            "FROM t |> GROUP BY a |> GROUP BY b",
            "FROM t |> ORDER BY a |> ORDER BY b",
            "FROM t |> LIMIT 1 |> LIMIT 2",
        ] {
            let error = process_pipe_syntax(sql)
                .expect_err("a repeated stage must be refused, not silently dropped");
            assert!(
                error.to_string().contains("repeated |>"),
                "unexpected error for {sql:?}: {error}"
            );
        }
    }

    #[test]
    fn standard_sql_unchanged() {
        let sql = "SELECT * FROM orders WHERE amount > 100";
        let result = process_pipe_syntax(sql).unwrap();
        assert_eq!(result, sql);
    }

    /// The translator has to be reachable from the engine, not merely correct.
    /// The module was never declared in `lib.rs`, so none of this was compiled
    /// and `FROM t |> …` went to DataFusion verbatim and failed to parse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipe_syntax_runs_through_the_engine() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let engine = crate::SqlEngine::new();
        let schema = Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![50_i64, 150, 250]))],
        )
        .unwrap();
        engine
            .register_record_batches("orders", vec![batch])
            .await
            .unwrap();

        let batches = engine
            .sql("FROM orders |> WHERE amount > 100 |> WHERE amount < 200 |> SELECT amount")
            .await
            .expect("piped query should plan")
            .collect()
            .await
            .expect("piped query should execute");

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 1,
            "both filters must apply: only amount=150 is >100 and <200"
        );
    }

    #[test]
    fn has_pipe_syntax_detection() {
        assert!(has_pipe_syntax("FROM t |> SELECT *"));
        assert!(!has_pipe_syntax("SELECT * FROM t"));
        assert!(!has_pipe_syntax("FROM t"));
    }
}
