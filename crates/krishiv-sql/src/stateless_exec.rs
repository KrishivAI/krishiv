//! Stateless per-batch SQL execution (moved from krishiv-engines in task
//! #147 so the executor, engines, and bench share ONE implementation; the
//! engines crate re-exports it).

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

use crate::{SqlError, SqlResult};

/// Batch-tuned DataFusion context (same knobs the engines crate used).
fn batch_session_context() -> SessionContext {
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let config = SessionConfig::new()
        .with_target_partitions(parallelism)
        .with_batch_size(65_536)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true);
    SessionContext::new_with_config(config)
}

/// Stateless per-batch SQL executor with a cached `SessionContext`.
///
/// The predecessor (`apply_stateless_query`) built a fresh context, planned
/// the SQL, and registered a new MemTable on EVERY batch — full parse+plan on
/// the hot path, the same shape that dominated IVM tick latency before its
/// cached tick context (G14). This reuses one context across batches and
/// replace-registers the input table per batch, which for a single-table
/// stateless query is observationally identical to per-batch construction.
pub struct StatelessBatchExecutor {
    ctx: SessionContext,
    query: String,
    table_name: String,
}

impl StatelessBatchExecutor {
    #[must_use]
    pub fn new(query: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            ctx: batch_session_context(),
            query: query.into(),
            table_name: table_name.into(),
        }
    }

    /// Register a STATIC side table, available to every subsequent
    /// `on_batch` (task #143, NEXMark Q13's side-input join).
    ///
    /// Registered once and never replace-registered: the side input is bounded
    /// reference data by definition, and this method refuses to overwrite an
    /// existing table rather than silently swapping reference data mid-stream.
    pub fn register_side_table(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> SqlResult<()> {
        let schema = batches
            .first()
            .map(arrow::record_batch::RecordBatch::schema)
            .ok_or_else(|| SqlError::DataFusion {
                message: format!("side table '{name}' needs at least one batch"),
            })?;
        let table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
        self.ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Run the query over exactly this batch.
    ///
    /// Output derives from THIS batch alone: the input table is
    /// replace-registered, never appended, so a row from batch N can never
    /// reappear in batch N+1's output.
    pub async fn on_batch(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> SqlResult<Vec<arrow::record_batch::RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let schema = batch.schema();
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;
        // `register_table` errors on a duplicate name rather than overwriting
        // (the IVM replace-register rule), so deregister first. The result is
        // deliberately ignored: on the first batch there is nothing to remove,
        // and that absence is not an error.
        let _ = self.ctx.deregister_table(self.table_name.as_str());
        self.ctx
            .register_table(self.table_name.as_str(), Arc::new(table))?;
        let mut stream = self.ctx.sql(&self.query).await?.execute_stream().await?;
        let mut results = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }
        Ok(results)
    }
}
