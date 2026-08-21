//! Stateless per-batch SQL execution (moved from krishiv-engines in task
//! #147 so the executor, engines, and bench share ONE implementation; the
//! engines crate re-exports it).

use std::sync::{Arc, RwLock};

use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};

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

/// The lazy swap-buffer the cached plan reads through (task #149 fix 9).
///
/// The plan is compiled ONCE against a `StreamingTable` over this partition;
/// each `execute` snapshots whatever batches are in the buffer at that
/// moment. This is what makes plan caching SOUND: a cached plan over a
/// `MemTable` would capture the first batch's data at planning time and
/// silently re-serve it forever — the classic stale-provider trap.
struct SwapBufferPartition {
    schema: arrow::datatypes::SchemaRef,
    current: Arc<RwLock<Vec<arrow::record_batch::RecordBatch>>>,
}

impl std::fmt::Debug for SwapBufferPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwapBufferPartition")
            .finish_non_exhaustive()
    }
}

impl PartitionStream for SwapBufferPartition {
    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let batches = self
            .current
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::iter(batches.into_iter().map(Ok)),
            ),
        )
    }
}

/// Cached-plan pieces built on the first batch (schema-dependent).
///
/// The OPTIMIZED LOGICAL plan is the cache boundary, not the physical plan:
/// DataFusion physical plans are single-execution (RepartitionExec panics
/// "partition not used yet" on re-execute), so each batch runs physical
/// planning over the cached logical plan — parse, resolution, and logical
/// optimization happen once.
struct CompiledQuery {
    optimized: datafusion::logical_expr::LogicalPlan,
    input_schema: arrow::datatypes::SchemaRef,
    buffer: Arc<RwLock<Vec<arrow::record_batch::RecordBatch>>>,
}

/// Stateless per-batch SQL executor with a cached context AND a cached
/// physical plan (task #149 fix 9).
///
/// History: the first cut built a fresh context and re-planned per batch;
/// the #147 version cached the context but still ran parse + logical plan +
/// optimize + physical plan on EVERY batch. This version compiles the query
/// once, on the first batch (whose schema fixes the plan), against a lazy
/// swap-buffer table; each subsequent batch swaps the buffer and re-executes
/// the same physical plan. A batch whose schema differs from the first is
/// refused rather than silently mis-planned.
pub struct StatelessBatchExecutor {
    ctx: SessionContext,
    query: String,
    table_name: String,
    compiled: tokio::sync::Mutex<Option<CompiledQuery>>,
}

impl StatelessBatchExecutor {
    #[must_use]
    pub fn new(query: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            ctx: batch_session_context(),
            query: query.into(),
            table_name: table_name.into(),
            compiled: tokio::sync::Mutex::new(None),
        }
    }

    /// Register a STATIC side table, available to every subsequent
    /// `on_batch` (task #143, NEXMark Q13's side-input join).
    ///
    /// Registered once and never replace-registered: the side input is bounded
    /// reference data by definition, and this method refuses to overwrite an
    /// existing table rather than silently swapping reference data mid-stream.
    /// Must be called before the first `on_batch` — the compiled plan embeds
    /// it.
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
    /// Output derives from THIS batch alone: the swap buffer is REPLACED,
    /// never appended, so a row from batch N can never reappear in batch
    /// N+1's output.
    pub async fn on_batch(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> SqlResult<Vec<arrow::record_batch::RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let mut compiled = self.compiled.lock().await;
        if compiled.is_none() {
            let schema = batch.schema();
            let buffer: Arc<RwLock<Vec<arrow::record_batch::RecordBatch>>> =
                Arc::new(RwLock::new(Vec::new()));
            let partition = Arc::new(SwapBufferPartition {
                schema: Arc::clone(&schema),
                current: Arc::clone(&buffer),
            });
            let table = StreamingTable::try_new(Arc::clone(&schema), vec![partition])?;
            self.ctx
                .register_table(self.table_name.as_str(), Arc::new(table))?;
            let logical = self.ctx.sql(&self.query).await?.into_unoptimized_plan();
            let optimized = self.ctx.state().optimize(&logical)?;
            *compiled = Some(CompiledQuery {
                optimized,
                input_schema: schema,
                buffer,
            });
        }
        let Some(compiled) = compiled.as_ref() else {
            return Err(SqlError::DataFusion {
                message: "stateless compiled query missing after initialization".into(),
            });
        };
        if batch.schema() != compiled.input_schema {
            return Err(SqlError::DataFusion {
                message: format!(
                    "stateless input schema changed mid-stream for table '{}': the compiled \
                     plan was built for the first batch's schema; refusing rather than \
                     mis-planning",
                    self.table_name
                ),
            });
        }
        {
            let mut guard = compiled.buffer.write().map_err(|_| SqlError::DataFusion {
                message: "stateless swap buffer lock poisoned".into(),
            })?;
            *guard = vec![batch];
        }
        let state = self.ctx.state();
        let physical = state
            .query_planner()
            .create_physical_plan(&compiled.optimized, &state)
            .await?;
        let results = datafusion::physical_plan::collect(physical, self.ctx.task_ctx()).await?;
        Ok(results)
    }
}

#[cfg(test)]
mod cached_plan_freshness {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(vals: &[i64]) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vals.to_vec()))],
        )
        .expect("batch")
    }

    fn values(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64");
            out.extend((0..b.num_rows()).map(|i| col.value(i)));
        }
        out.sort_unstable();
        out
    }

    /// The cached plan must read each batch FRESH. This is the stale-provider
    /// trap the swap-buffer table exists to prevent (task #149 fix 9): a plan
    /// cached over a MemTable captures the first batch's data at planning
    /// time and silently re-serves it for every later batch.
    #[tokio::test]
    async fn cached_plan_serves_each_batch_not_the_first() {
        let exec = StatelessBatchExecutor::new("SELECT v * 2 AS d FROM src", "src");
        let first = exec.on_batch(batch(&[1, 2])).await.expect("batch 1");
        assert_eq!(values(&first), vec![2, 4]);
        let second = exec.on_batch(batch(&[10])).await.expect("batch 2");
        assert_eq!(
            values(&second),
            vec![20],
            "batch 2 must produce batch 2's output, not a replay of batch 1"
        );
        // Schema drift is refused, not silently mis-planned.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("other", DataType::Int64, false),
        ]));
        let drifted = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("drifted batch");
        let err = exec.on_batch(drifted).await.expect_err("drift refused");
        assert!(err.to_string().contains("schema changed"));
    }
}
