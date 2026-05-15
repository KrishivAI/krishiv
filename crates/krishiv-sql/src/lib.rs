#![forbid(unsafe_code)]

//! SQL planning seam for Krishiv.
//!
//! DataFusion integration will land in a later R1 slice. This bootstrap crate
//! validates SQL input enough to create a placeholder Krishiv logical plan.

use std::error::Error;
use std::fmt;

use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

/// SQL result alias.
pub type SqlResult<T> = Result<T, SqlError>;

/// SQL-layer errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlError {
    /// Query was empty or whitespace only.
    EmptyQuery,
    /// The requested SQL feature is not available in the bootstrap slice.
    Unsupported { feature: String },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyQuery => f.write_str("SQL query is empty"),
            Self::Unsupported { feature } => write!(f, "unsupported SQL feature: {feature}"),
        }
    }
}

impl Error for SqlError {}

/// SQL planning output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPlan {
    query: String,
    logical_plan: LogicalPlan,
}

impl SqlPlan {
    /// Original query.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Placeholder logical plan.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

/// Create a placeholder logical plan for a SQL query.
pub fn plan_sql(query: impl Into<String>) -> SqlResult<SqlPlan> {
    let query = query.into();
    if query.trim().is_empty() {
        return Err(SqlError::EmptyQuery);
    }

    let logical_plan = LogicalPlan::new("sql-query", ExecutionKind::Batch).with_node(
        PlanNode::new("sql", "sql placeholder", ExecutionKind::Batch),
    );

    Ok(SqlPlan {
        query,
        logical_plan,
    })
}

/// Create bootstrap `EXPLAIN` text for a SQL query.
pub fn explain_sql(query: impl Into<String>) -> SqlResult<String> {
    let plan = plan_sql(query)?;
    Ok(plan.logical_plan().describe())
}

#[cfg(test)]
mod tests {
    use super::{SqlError, explain_sql, plan_sql};

    #[test]
    fn rejects_empty_sql() {
        let error = match plan_sql("   ") {
            Ok(_) => panic!("expected empty query error"),
            Err(error) => error,
        };

        assert_eq!(error, SqlError::EmptyQuery);
    }

    #[test]
    fn explains_non_empty_sql() {
        let explain = match explain_sql("select 1") {
            Ok(explain) => explain,
            Err(error) => panic!("unexpected SQL error: {error}"),
        };

        assert!(explain.contains("logical plan: sql-query"));
    }
}
