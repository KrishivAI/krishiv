#![forbid(unsafe_code)]

//! SQL planning and local execution seam for Krishiv.
//!
//! This crate owns the DataFusion integration for R1 while keeping DataFusion
//! out of the long-term public API exposed by `krishiv-api`.

use std::error::Error;
use std::fmt;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::dataframe::DataFrame as DataFusionDataFrame;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use krishiv_optimizer::{CostModel, Optimizer};
use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

/// SQL result alias.
pub type SqlResult<T> = Result<T, SqlError>;

/// SQL-layer errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlError {
    /// Query was empty or whitespace only.
    EmptyQuery,
    /// A table name was empty.
    EmptyTableName,
    /// The requested SQL feature is not available in R1.
    Unsupported { feature: String },
    /// DataFusion returned an error.
    DataFusion { message: String },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyQuery => f.write_str("SQL query is empty"),
            Self::EmptyTableName => f.write_str("table name is empty"),
            Self::Unsupported { feature } => write!(f, "unsupported SQL feature: {feature}"),
            Self::DataFusion { message } => write!(f, "DataFusion error: {message}"),
        }
    }
}

impl Error for SqlError {}

impl From<datafusion::error::DataFusionError> for SqlError {
    fn from(value: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion {
            message: value.to_string(),
        }
    }
}

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

    /// Krishiv logical plan wrapper.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

/// Local SQL engine backed by DataFusion.
#[derive(Clone)]
pub struct SqlEngine {
    context: SessionContext,
}

impl fmt::Debug for SqlEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlEngine")
            .field("backend", &"datafusion")
            .finish_non_exhaustive()
    }
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Create a local SQL engine.
    pub fn new() -> Self {
        Self {
            context: SessionContext::new(),
        }
    }

    /// Register a local Parquet path as a table.
    pub async fn register_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> SqlResult<()> {
        let table_name = table_name.as_ref();
        if table_name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }

        let path = path.as_ref().to_string_lossy().into_owned();
        self.context
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await?;
        Ok(())
    }

    /// Create a DataFrame by reading a local Parquet path directly.
    pub async fn read_parquet(&self, path: impl AsRef<Path>) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let dataframe = self
            .context
            .read_parquet(path, ParquetReadOptions::default())
            .await?;
        Ok(SqlDataFrame::new("parquet-read", dataframe))
    }

    /// Plan a SQL query with DataFusion.
    pub async fn sql(&self, query: impl AsRef<str>) -> SqlResult<SqlDataFrame> {
        let query = query.as_ref();
        if query.trim().is_empty() {
            return Err(SqlError::EmptyQuery);
        }

        let dataframe = self.context.sql(query).await?;
        Ok(SqlDataFrame::new("sql-query", dataframe))
    }
}

/// Krishiv-owned wrapper around a DataFusion DataFrame.
#[derive(Debug, Clone)]
pub struct SqlDataFrame {
    name: String,
    dataframe: DataFusionDataFrame,
}

impl SqlDataFrame {
    fn new(name: impl Into<String>, dataframe: DataFusionDataFrame) -> Self {
        Self {
            name: name.into(),
            dataframe,
        }
    }

    /// Create a Krishiv logical plan wrapper for this DataFrame.
    pub fn krishiv_logical_plan(&self) -> LogicalPlan {
        let label = self.dataframe.logical_plan().to_string();
        LogicalPlan::new(self.name.clone(), ExecutionKind::Batch).with_node(PlanNode::new(
            "datafusion-logical",
            label,
            ExecutionKind::Batch,
        ))
    }

    /// Explain the logical plan without executing it.
    pub fn explain_logical(&self) -> String {
        self.dataframe.logical_plan().to_string()
    }

    /// Explain logical and physical plan details through DataFusion.
    pub async fn explain(&self) -> SqlResult<String> {
        let batches = self
            .dataframe
            .clone()
            .explain(false, false)?
            .collect()
            .await?;
        pretty_batches(&batches)
    }

    /// Execute and collect this DataFrame.
    pub async fn collect(&self) -> SqlResult<Vec<RecordBatch>> {
        Ok(self.dataframe.clone().collect().await?)
    }
}

/// Create a Krishiv logical plan wrapper for a SQL query without executing it.
pub fn plan_sql(query: impl Into<String>) -> SqlResult<SqlPlan> {
    let query = query.into();
    if query.trim().is_empty() {
        return Err(SqlError::EmptyQuery);
    }

    let logical_plan =
        LogicalPlan::new("sql-query", ExecutionKind::Batch).with_node(PlanNode::new(
            "sql",
            format!("sql: {}", query.trim()),
            ExecutionKind::Batch,
        ));

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

/// Explain a SQL query including optimizer rule decisions.
///
/// Runs the logical plan through `optimizer` and appends the optimizer
/// summary to the plan description.
pub fn explain_sql_optimized(
    query: impl Into<String>,
    optimizer: &Optimizer,
) -> SqlResult<String> {
    let plan = plan_sql(query)?;
    let result = optimizer.optimize(plan.logical_plan().clone());
    let mut output = result.plan.describe();
    let optimizer_line = result.describe();
    output.push('\n');
    output.push_str(&optimizer_line);
    Ok(output)
}

/// Explain a SQL query and append a cost estimate from the provided cost model.
pub fn explain_sql_with_cost(
    query: impl Into<String>,
    cost_model: &dyn CostModel,
) -> SqlResult<String> {
    let plan = plan_sql(query)?;
    let cost = cost_model.estimate(plan.logical_plan());
    let mut output = plan.logical_plan().describe();
    output.push_str(&format!(
        "\ncost: cpu_nanos={}, memory_bytes={}, network_bytes={}",
        cost.cpu_nanos, cost.memory_bytes, cost.network_bytes
    ));
    Ok(output)
}

/// Format Arrow batches for CLI and tests.
pub fn pretty_batches(batches: &[RecordBatch]) -> SqlResult<String> {
    Ok(pretty_format_batches(batches)
        .map_err(|error| SqlError::DataFusion {
            message: error.to_string(),
        })?
        .to_string())
}

#[cfg(test)]
mod tests {
    use krishiv_optimizer::{Cost, CostModel, Optimizer};
    use krishiv_plan::LogicalPlan;

    use super::{SqlEngine, SqlError, explain_sql, explain_sql_optimized, explain_sql_with_cost, plan_sql};

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

    #[test]
    fn explain_sql_optimized_no_op_optimizer_includes_no_rules_message() {
        let optimizer = Optimizer::new();
        let output = explain_sql_optimized("select 1", &optimizer).unwrap();
        assert!(
            output.contains("optimizer: no rules applied"),
            "output did not contain expected optimizer message: {output}"
        );
    }

    #[test]
    fn explain_sql_with_cost_includes_cost_line() {
        struct ZeroCost;
        impl CostModel for ZeroCost {
            fn estimate(&self, _plan: &LogicalPlan) -> Cost {
                Cost::default()
            }
        }

        let output = explain_sql_with_cost("select 1", &ZeroCost).unwrap();
        assert!(
            output.contains("cost:"),
            "output did not contain cost line: {output}"
        );
        assert!(output.contains("cpu_nanos=0"));
        assert!(output.contains("memory_bytes=0"));
        assert!(output.contains("network_bytes=0"));
    }

    #[tokio::test]
    async fn datafusion_sql_collects_rows() {
        let engine = SqlEngine::new();
        let dataframe = match engine.sql("select 1 as value").await {
            Ok(dataframe) => dataframe,
            Err(error) => panic!("unexpected SQL error: {error}"),
        };

        let batches = match dataframe.collect().await {
            Ok(batches) => batches,
            Err(error) => panic!("unexpected collect error: {error}"),
        };

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }
}
