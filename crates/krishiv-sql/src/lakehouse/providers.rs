//! Register delta/hudi URI tables with DataFusion (R18 S1.2, S2.3).

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

use krishiv_connectors::lakehouse::{AsOfSpec, HudiQueryType, HudiSnapshotReader};

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
    let handle = krishiv_connectors::lakehouse::DeltaTableHandle::open(path, version)
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
    // `HudiSnapshotReader::schema` does blocking filesystem/Parquet-decode I/O
    // (unlike the object-store-backed Hudi reader, which is genuinely async).
    // Run it on the blocking pool so registering a Hudi table doesn't stall
    // this async DataFusion/Flight SQL task.
    let reader_for_schema = reader.clone();
    let schema = tokio::task::spawn_blocking(move || reader_for_schema.schema())
        .await
        .map_err(|e| SqlError::DataFusion {
            message: format!("hudi schema read task panicked: {e}"),
        })?
        .map_err(|e| SqlError::DataFusion {
            message: format!("hudi: failed to read table schema: {e}"),
        })?;
    let provider = Arc::new(HudiScanProvider { reader, schema });
    ctx.register_table(table_name, provider)
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

#[derive(Debug)]
struct DeltaScanProvider {
    handle: krishiv_connectors::lakehouse::DeltaTableHandle,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for DeltaScanProvider {
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
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for HudiScanProvider {
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
        // Same blocking-I/O concern as `register_hudi_uri`: offload to the
        // blocking pool instead of running filesystem/Parquet decode inline
        // on the async DataFusion worker thread.
        let reader = self.reader.clone();
        let batches = tokio::task::spawn_blocking(move || reader.scan_batches())
            .await
            .map_err(|e| DataFusionError::External(format!("hudi scan task panicked: {e}").into()))?
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
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

/// Apply `AS OF` qualifiers by re-registering pinned delta tables.
///
/// # Why this refuses instead of skipping
///
/// [`super::as_of::preprocess_as_of_sql`] **removes** the `AS OF` clause from
/// the SQL before DataFusion ever sees it, and collects a ref for *every*
/// table it appears on — not only `delta.`-prefixed ones. So anything this
/// function declines to honour is not merely unsupported: the qualifier has
/// already been erased, the query runs against the current snapshot, and
/// nothing anywhere reports it. A user asking for last quarter's numbers
/// silently receives today's.
///
/// Two shapes used to fall into that hole:
///
///   * any table not named `delta.<path>` — an Iceberg or catalog table with
///     `VERSION AS OF 3` was collected and then dropped on the floor;
///   * `delta.<path>` with a **timestamp**, because the spec was matched with
///     `AsOfSpec::Version(v) => Some(v), _ => None`, and `None` is exactly how
///     [`register_delta_uri`] spells "open the latest version".
///
/// Both now error. Returning wrong data quietly is the one outcome a
/// time-travel query must never produce.
pub async fn apply_as_of_refs(
    ctx: &SessionContext,
    refs: &[super::as_of::AsOfTableRef],
) -> SqlResult<()> {
    for reference in refs.iter() {
        let Some(path) = reference.table.strip_prefix("delta.") else {
            return Err(SqlError::Unsupported {
                feature: format!(
                    "time travel on '{}': AS OF is currently resolved only for Delta tables \
                     referenced as delta.<path>. The qualifier has already been stripped from \
                     the query, so continuing would silently read the current snapshot instead \
                     of the requested one.",
                    reference.table
                ),
            });
        };
        let version = match reference.spec {
            AsOfSpec::Version(v) => v,
            AsOfSpec::Timestamp(ts) => {
                return Err(SqlError::Unsupported {
                    feature: format!(
                        "time travel on '{}' AS OF {ts}: resolving a timestamp to a Delta \
                         version is not implemented (DeltaTableHandle::open takes a version \
                         number). Use VERSION AS OF <n>. Proceeding would read the current \
                         snapshot, not the one at that timestamp.",
                        reference.table
                    ),
                });
            }
        };
        register_delta_uri(
            ctx,
            &reference.table.replace('.', "_"),
            path,
            Some(version),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use super::super::as_of::AsOfTableRef;
    use chrono::TimeZone;

    fn ctx() -> SessionContext {
        SessionContext::new()
    }

    /// No qualifiers, nothing to do — the common path must stay quiet.
    #[tokio::test]
    async fn no_refs_is_a_no_op() {
        assert!(apply_as_of_refs(&ctx(), &[]).await.is_ok());
    }

    /// The silent-wrong-answer case: a time-travel qualifier on a table this
    /// cannot pin. `preprocess_as_of_sql` has already deleted the clause from
    /// the SQL, so skipping it means running against the current snapshot with
    /// no diagnostic anywhere.
    #[tokio::test]
    async fn a_non_delta_table_cannot_be_silently_ignored() {
        let refs = vec![AsOfTableRef {
            table: "orders".into(),
            spec: AsOfSpec::Version(3),
        }];
        let err = apply_as_of_refs(&ctx(), &refs)
            .await
            .expect_err("AS OF on a non-delta table must not be dropped on the floor");
        let msg = err.to_string();
        assert!(msg.contains("orders"), "must name the table: {msg}");
        assert!(
            msg.contains("current snapshot"),
            "must say what the alternative would have done: {msg}"
        );
    }

    /// A timestamp on a Delta table used to become `version = None`, which is
    /// how `register_delta_uri` spells "latest" — so the query silently read
    /// the present.
    #[tokio::test]
    async fn a_timestamp_spec_is_refused_rather_than_read_as_latest() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let refs = vec![AsOfTableRef {
            table: "delta./tmp/does-not-matter".into(),
            spec: AsOfSpec::Timestamp(ts),
        }];
        let err = apply_as_of_refs(&ctx(), &refs)
            .await
            .expect_err("a timestamp must not silently resolve to the latest version");
        assert!(
            err.to_string().contains("VERSION AS OF"),
            "the error should point at the supported spelling: {err}"
        );
    }

    /// Every spec variant must be handled explicitly. If a third one is added,
    /// this fails to compile rather than falling into a `_ => None` arm — which
    /// is precisely how the timestamp case became a silent latest-version read.
    #[test]
    fn every_as_of_spec_variant_is_accounted_for() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        for spec in [AsOfSpec::Version(1), AsOfSpec::Timestamp(ts)] {
            match spec {
                AsOfSpec::Version(_) | AsOfSpec::Timestamp(_) => {}
            }
        }
    }
}
