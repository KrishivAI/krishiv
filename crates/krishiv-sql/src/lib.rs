#![forbid(unsafe_code)]

//! SQL planning and local execution seam for Krishiv.
//!
//! This crate owns the DataFusion integration for R1 while keeping DataFusion
//! out of the long-term public API exposed by `krishiv-api`.

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::ops::ControlFlow;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::dataframe::DataFrame as DataFusionDataFrame;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser};

use krishiv_optimizer::{CostModel, Optimizer};
use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

/// SQL result alias.
pub type SqlResult<T> = Result<T, SqlError>;

/// SQL-layer errors.
#[non_exhaustive]
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
    /// Access denied by auth or policy check.
    AccessDenied { reason: String },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyQuery => f.write_str("SQL query is empty"),
            Self::EmptyTableName => f.write_str("table name is empty"),
            Self::Unsupported { feature } => write!(f, "unsupported SQL feature: {feature}"),
            Self::DataFusion { message } => write!(f, "DataFusion error: {message}"),
            Self::AccessDenied { reason } => write!(f, "access denied: {reason}"),
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
    view_registry: Option<std::sync::Arc<std::sync::Mutex<MaterializedViewRegistry>>>,
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
            view_registry: None,
        }
    }

    /// Attach a [`MaterializedViewRegistry`] so the engine tracks view staleness.
    #[must_use]
    pub fn with_view_registry(
        mut self,
        registry: std::sync::Arc<std::sync::Mutex<MaterializedViewRegistry>>,
    ) -> Self {
        self.view_registry = Some(registry);
        self
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
        if let Some(ref reg) = self.view_registry
            && let Ok(mut r) = reg.lock()
        {
            r.mark_table_committed();
        }
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

    /// Register an in-memory table from Arrow record batches.
    ///
    /// The schema is inferred from the first batch. An empty `batches` slice
    /// registers a table with no rows using the provided schema if the batches
    /// are non-empty, or is a no-op if empty.
    pub async fn register_record_batches(
        &self,
        table_name: impl AsRef<str>,
        batches: Vec<RecordBatch>,
    ) -> SqlResult<()> {
        use std::sync::Arc;
        let table_name = table_name.as_ref();
        if table_name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_err(|e| {
                SqlError::DataFusion {
                    message: e.to_string(),
                }
            })?;
        self.context
            .register_table(table_name, Arc::new(mem_table))
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
        if let Some(ref reg) = self.view_registry
            && let Ok(mut r) = reg.lock()
        {
            r.mark_table_committed();
        }
        Ok(())
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

    /// Execute `query` with materialized view cache lookup.
    ///
    /// If the query targets a registered, fresh view, returns cached batches directly.
    /// Otherwise executes normally and caches the result for `OnCommit` views.
    pub async fn sql_with_view_cache(&self, query: impl AsRef<str>) -> SqlResult<Vec<RecordBatch>> {
        let q = query.as_ref().trim();
        let view_name_candidate = extract_simple_view_name(q);

        if let (Some(reg), Some(name)) = (&self.view_registry, &view_name_candidate)
            && let Ok(r) = reg.lock()
            && let Some(cached) = r.get_if_fresh(name)
        {
            return Ok(cached.clone());
        }

        let df = self.sql(q).await?;
        let batches = df.collect().await?;

        if let (Some(reg), Some(name)) = (&self.view_registry, &view_name_candidate)
            && let Ok(mut r) = reg.lock()
            && let Some(def) = r.definition(name).cloned()
            && def.refresh_policy == RefreshPolicy::OnCommit
        {
            r.set_cached(name, batches.clone());
        }

        Ok(batches)
    }
}

fn extract_simple_view_name(query: &str) -> Option<String> {
    let lower = query.to_lowercase();
    if let Some(pos) = lower.find(" from ") {
        let after = query[pos + 6..].trim();
        let end = after
            .find(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ';')
            .unwrap_or(after.len());
        let name = after[..end].trim().to_string();
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
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

    /// Execute and collect this DataFrame, also returning lightweight runtime statistics.
    ///
    /// Collects `output_rows` from DataFusion's execution metrics. `cpu_nanos`
    /// is approximated from `elapsed_compute` when available; other fields default to 0.
    pub async fn collect_with_stats(&self) -> SqlResult<(Vec<RecordBatch>, SqlExecutionStats)> {
        use datafusion::physical_plan::collect as df_collect;

        let ctx = SessionContext::new();
        let physical_plan = ctx
            .state()
            .create_physical_plan(self.dataframe.logical_plan())
            .await?;

        let batches = df_collect(physical_plan.clone(), ctx.task_ctx()).await?;

        let mut output_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut cpu_nanos: u64 = 0;

        if let Some(metrics) = physical_plan.metrics() {
            if let Some(v) = metrics.output_rows() {
                output_rows = v as u64;
            }
            if let Some(t) = metrics.elapsed_compute() {
                cpu_nanos = t as u64;
            }
        }

        Ok((
            batches,
            SqlExecutionStats {
                output_rows,
                cpu_nanos,
            },
        ))
    }
}

/// Lightweight execution statistics collected from a DataFusion physical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlExecutionStats {
    pub output_rows: u64,
    pub cpu_nanos: u64,
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
pub fn explain_sql_optimized(query: impl Into<String>, optimizer: &Optimizer) -> SqlResult<String> {
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

/// Return all base table/relation names referenced by `query`.
///
/// This uses the same SQL parser family as DataFusion, so policy checks cover
/// joins, subqueries, CTE bodies, and other nested relation references instead
/// of relying on a single best-effort `FROM` token.
pub fn referenced_table_names(query: impl AsRef<str>) -> SqlResult<Vec<String>> {
    let query = query.as_ref();
    if query.trim().is_empty() {
        return Err(SqlError::EmptyQuery);
    }

    let statements =
        Parser::parse_sql(&GenericDialect {}, query).map_err(|e| SqlError::DataFusion {
            message: format!("SQL parse error: {e}"),
        })?;
    let mut names = BTreeSet::new();
    let _ = visit_relations(&statements, |relation| {
        names.insert(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    Ok(names.into_iter().collect())
}

/// Format Arrow batches for CLI and tests.
pub fn pretty_batches(batches: &[RecordBatch]) -> SqlResult<String> {
    Ok(pretty_format_batches(batches)
        .map_err(|error| SqlError::DataFusion {
            message: error.to_string(),
        })?
        .to_string())
}

// ─── Materialized Views Baseline ─────────────────────────────────────────────

/// Materialized view refresh policy.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshPolicy {
    /// Refresh whenever the backing table(s) receive a write commit.
    OnCommit,
    /// Only refresh when explicitly triggered by `MaterializedViewRegistry::refresh()`.
    Manual,
}

/// Declaration of a named materialized view.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct MaterializedViewDefinition {
    /// Unique view name.
    pub name: String,
    /// SQL SELECT query that defines the view.
    pub query: String,
    /// Refresh policy.
    pub refresh_policy: RefreshPolicy,
    /// Partition columns for storage keying (empty = unpartitioned).
    pub partition_columns: Vec<String>,
}

impl MaterializedViewDefinition {
    /// Create a new view definition with OnCommit refresh and no partitioning.
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            refresh_policy: RefreshPolicy::OnCommit,
            partition_columns: Vec::new(),
        }
    }

    /// Set the refresh policy.
    #[must_use]
    pub fn with_refresh_policy(mut self, policy: RefreshPolicy) -> Self {
        self.refresh_policy = policy;
        self
    }

    /// Set partition columns.
    #[must_use]
    pub fn with_partition_columns(mut self, cols: Vec<String>) -> Self {
        self.partition_columns = cols;
        self
    }
}

/// In-memory registry for materialized view definitions and their cached results.
///
/// In production, results would be persisted to `RedbStateBackend`. For R10
/// the registry is in-memory and resets on process restart.
#[derive(Debug, Default)]
pub struct MaterializedViewRegistry {
    definitions: HashMap<String, MaterializedViewDefinition>,
    /// Cached results keyed by view name → serialized batch (Arrow IPC).
    cache: HashMap<String, Vec<RecordBatch>>,
    /// Current write LSN — incremented on each `mark_table_committed()` call.
    current_lsn: u64,
    /// LSN at which each view was last refreshed.
    view_lsn: HashMap<String, u64>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a materialized view definition.
    pub fn register(&mut self, def: MaterializedViewDefinition) {
        self.definitions.insert(def.name.clone(), def);
    }

    /// Mark a table as having received a commit. Increments the current LSN.
    /// All OnCommit views are now stale.
    pub fn mark_table_committed(&mut self) {
        self.current_lsn += 1;
    }

    /// Returns true if the view is stale (backing table committed after last refresh,
    /// or the view has never been cached / is not registered).
    pub fn is_stale(&self, view_name: &str) -> bool {
        // Unregistered or never-cached views are always considered stale.
        if !self.view_lsn.contains_key(view_name) {
            return true;
        }
        let last_refresh = self.view_lsn.get(view_name).copied().unwrap_or(0);
        last_refresh < self.current_lsn
    }

    /// Store refreshed results for a view.
    pub fn set_cached(&mut self, view_name: &str, batches: Vec<RecordBatch>) {
        self.cache.insert(view_name.to_string(), batches);
        self.view_lsn
            .insert(view_name.to_string(), self.current_lsn);
    }

    /// Get cached results if the view is fresh.
    pub fn get_if_fresh(&self, view_name: &str) -> Option<&Vec<RecordBatch>> {
        if self.is_stale(view_name) {
            None
        } else {
            self.cache.get(view_name)
        }
    }

    /// Get the view definition, if registered.
    pub fn definition(&self, view_name: &str) -> Option<&MaterializedViewDefinition> {
        self.definitions.get(view_name)
    }
}

#[cfg(test)]
mod matview_tests {
    use super::*;

    #[test]
    fn fresh_view_returns_cached_results() {
        let mut reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
        let batch = vec![]; // empty batch for test
        reg.set_cached("v1", batch.clone());
        assert!(reg.get_if_fresh("v1").is_some());
    }

    #[test]
    fn committed_table_marks_view_stale() {
        let mut reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
        reg.set_cached("v1", vec![]);
        assert!(!reg.is_stale("v1"));
        reg.mark_table_committed();
        assert!(reg.is_stale("v1"));
        assert!(reg.get_if_fresh("v1").is_none());
    }

    #[test]
    fn refresh_after_commit_restores_freshness() {
        let mut reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
        reg.set_cached("v1", vec![]);
        reg.mark_table_committed();
        assert!(reg.is_stale("v1"));
        reg.set_cached("v1", vec![]); // refresh
        assert!(!reg.is_stale("v1"));
    }

    #[test]
    fn unregistered_view_is_stale() {
        let reg = MaterializedViewRegistry::new();
        assert!(reg.is_stale("nonexistent"));
    }

    #[test]
    fn definition_builder_sets_fields() {
        let def = MaterializedViewDefinition::new("sales_summary", "SELECT SUM(amount) FROM sales")
            .with_refresh_policy(RefreshPolicy::Manual)
            .with_partition_columns(vec!["region".into()]);
        assert_eq!(def.name, "sales_summary");
        assert_eq!(def.refresh_policy, RefreshPolicy::Manual);
        assert_eq!(def.partition_columns, vec!["region".to_string()]);
    }
}

#[cfg(test)]
mod view_cache_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn engine_marks_table_committed_after_register() {
        let registry = Arc::new(Mutex::new(MaterializedViewRegistry::new()));
        {
            let mut r = registry.lock().unwrap();
            r.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
            r.set_cached("v1", vec![]);
        }
        assert!(!registry.lock().unwrap().is_stale("v1"));

        let engine = SqlEngine::new().with_view_registry(registry.clone());
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        let col = arrow::array::Int64Array::from(vec![1i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        engine
            .register_record_batches("t1", vec![batch])
            .await
            .unwrap();

        assert!(
            registry.lock().unwrap().is_stale("v1"),
            "commit must mark view stale"
        );
    }

    #[tokio::test]
    async fn sql_with_view_cache_returns_fresh_cache() {
        let registry = Arc::new(Mutex::new(MaterializedViewRegistry::new()));
        let expected_batch = {
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
            ]));
            let col = arrow::array::Int64Array::from(vec![99i64]);
            RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
        };
        {
            let mut r = registry.lock().unwrap();
            r.register(
                MaterializedViewDefinition::new("summary", "SELECT 99 AS v")
                    .with_refresh_policy(RefreshPolicy::OnCommit),
            );
            r.set_cached("summary", vec![expected_batch.clone()]);
        }

        let engine = SqlEngine::new().with_view_registry(registry.clone());
        let batches = engine
            .sql_with_view_cache("SELECT * FROM summary")
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}


#[cfg(test)]
mod tests {
    use krishiv_optimizer::{Cost, CostModel, Optimizer};
    use krishiv_plan::LogicalPlan;

    use super::{
        SqlEngine, SqlError, explain_sql, explain_sql_optimized, explain_sql_with_cost, plan_sql,
        referenced_table_names,
    };

    #[test]
    fn rejects_empty_sql() {
        let error = match plan_sql("   ") {
            Ok(_) => panic!("expected empty query error"),
            Err(error) => error,
        };

        assert_eq!(error, SqlError::EmptyQuery);
    }

    #[test]
    fn referenced_table_names_covers_joins_and_subqueries() {
        let tables = referenced_table_names(
            "SELECT * FROM public JOIN secret ON public.id = secret.id \
             WHERE public.id IN (SELECT id FROM audit)",
        )
        .unwrap();
        assert_eq!(tables, vec!["audit", "public", "secret"]);
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
