//! Register delta/hudi URI tables with DataFusion (R18 S1.2, S2.3).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use krishiv_lakehouse::{AsOfSpec, HudiQueryType, HudiSnapshotReader};

use crate::SqlError;
use crate::SqlResult;

/// Register `delta.<path>` as a logical table name `delta_<sanitized>`.
pub async fn register_delta_uri(
    ctx: &SessionContext,
    table_name: &str,
    path: &str,
    version: Option<i64>,
) -> SqlResult<()> {
    let _ = ctx.deregister_table(table_name);
    let handle = krishiv_lakehouse::DeltaTableHandle::open(path, version)
        .await
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    let schema = handle.schema().await.map_err(|e| SqlError::DataFusion {
        message: e.to_string(),
    })?;
    let provider = Arc::new(DeltaScanProvider { handle, schema });
    ctx.register_table(table_name, provider)
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

/// Register `hudi.<path>` table.
pub async fn register_hudi_uri(
    ctx: &SessionContext,
    table_name: &str,
    path: &str,
    query_type: HudiQueryType,
    begin_instant: Option<&str>,
) -> SqlResult<()> {
    let _ = ctx.deregister_table(table_name);
    let reader = {
        let mut r = HudiSnapshotReader::open(path).with_query_type(query_type);
        if let Some(inst) = begin_instant {
            r = r.with_begin_instant(inst);
        }
        r
    };
    let provider = Arc::new(HudiScanProvider { reader });
    ctx.register_table(table_name, provider)
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

#[derive(Debug)]
struct DeltaScanProvider {
    handle: krishiv_lakehouse::DeltaTableHandle,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for DeltaScanProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let batches = self
            .handle
            .scan_batches()
            .await
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;

        let table = MemTable::try_new(schema, vec![batches])?;
        let plan = table.scan(state, projection, filters, limit).await?;
        Ok(plan)
    }
}

#[derive(Debug)]
struct HudiScanProvider {
    reader: HudiSnapshotReader,
}

#[async_trait]
impl TableProvider for HudiScanProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.reader
            .schema()
            .unwrap_or_else(|_| Arc::new(arrow::datatypes::Schema::empty()))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let batches = self
            .reader
            .scan_batches()
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;

        let table = MemTable::try_new(schema, vec![batches])?;
        table.scan(state, projection, filters, limit).await
    }
}

/// Register in-memory batches as a DataFusion table.
pub async fn register_scan_batches(
    ctx: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
) -> SqlResult<()> {
    // Allow overwriting an existing table (used by MERGE write-back).
    let _ = ctx.deregister_table(name);
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    let table = MemTable::try_new(schema, vec![batches]).map_err(|e| SqlError::DataFusion {
        message: e.to_string(),
    })?;
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

/// Apply `AS OF` qualifiers by re-registering pinned delta/hudi tables.
pub async fn apply_as_of_refs(
    ctx: &SessionContext,
    refs: &[super::as_of::AsOfTableRef],
) -> SqlResult<()> {
    for reference in refs {
        if let Some(path) = reference.table.strip_prefix("delta.") {
            let version = match reference.spec {
                AsOfSpec::Version(v) => Some(v),
                _ => None,
            };
            register_delta_uri(ctx, &reference.table.replace('.', "_"), path, version).await?;
        }
    }
    Ok(())
}
