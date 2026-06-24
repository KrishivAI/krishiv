//! DataFusion `TableProviderFactory` implementations backed by
//! [`krishiv_connectors::ConnectorRegistry`].

use std::any::Any;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::catalog::TableProviderFactory;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::ExecutionPlan;
use krishiv_connectors::{ConnectorConfig, ConnectorError, ConnectorRegistry, default_registry};

use crate::kafka_table::{KafkaPartitionStream, kafka_auto_commit_interval_ms, project_batch};

/// Reject paths that escape the warehouse root via traversal or absolutes.
fn validate_path_under_warehouse(location: &str) -> DataFusionResult<()> {
    let warehouse = std::env::var("KRISHIV_WAREHOUSE_ROOT").unwrap_or_else(|_| ".".to_string());
    let base = PathBuf::from(&warehouse).canonicalize().map_err(|e| {
        DataFusionError::External(Box::new(ConnectorError::Unsupported {
            message: format!("warehouse root '{warehouse}' not accessible: {e}"),
        }))
    })?;
    let candidate = PathBuf::from(location);
    let resolved = if candidate.is_relative() {
        base.join(&candidate)
    } else {
        candidate
    };
    let canonical = resolved.canonicalize().map_err(|e| {
        DataFusionError::External(Box::new(ConnectorError::Unsupported {
            message: format!("path '{location}' not accessible: {e}"),
        }))
    })?;
    if !canonical.starts_with(&base) {
        return Err(DataFusionError::External(Box::new(
            ConnectorError::Unsupported {
                message: format!("path '{location}' escapes warehouse root '{warehouse}'"),
            },
        )));
    }
    Ok(())
}

/// Shared registry instance for SQL DDL table factories.
pub fn shared_connector_registry() -> Arc<ConnectorRegistry> {
    Arc::new(default_registry())
}

/// Register PARQUET, S3, and KAFKA DDL factories on a DataFusion table-factory map.
pub fn register_connector_table_factories(
    table_factories: &mut std::collections::HashMap<String, Arc<dyn TableProviderFactory>>,
    streaming_sources: Arc<RwLock<HashSet<String>>>,
) {
    let registry = shared_connector_registry();
    table_factories.insert(
        "PARQUET".to_string(),
        Arc::new(ConnectorTableFactory::bounded(
            "parquet",
            Arc::clone(&registry),
        )),
    );
    table_factories.insert(
        "S3".to_string(),
        Arc::new(ConnectorTableFactory::bounded("s3", registry)),
    );
    table_factories.insert(
        "KAFKA".to_string(),
        Arc::new(ConnectorTableFactory::streaming(streaming_sources)),
    );
}

/// Build a [`ConnectorConfig`] from a `CREATE EXTERNAL TABLE` command.
pub fn connector_config_from_ddl(
    kind: &str,
    cmd: &CreateExternalTable,
) -> DataFusionResult<ConnectorConfig> {
    let name = cmd.name.table().to_string();
    Ok(match kind {
        "parquet" => {
            if !cmd.location.is_empty() {
                validate_path_under_warehouse(&cmd.location)?;
            }
            ConnectorConfig::new(name, kind).with_property("path", cmd.location.clone())
        }
        "s3" => {
            let mut cfg = ConnectorConfig::new(cmd.name.table(), kind)
                .with_property("object_path", cmd.location.clone());
            for (key, value) in &cmd.options {
                if key == "base_path" {
                    cfg = cfg.with_property("base_path", value.clone());
                }
            }
            cfg
        }
        "kafka" => {
            let mut cfg = ConnectorConfig::new(cmd.name.table(), kind)
                .with_property("topic", cmd.location.clone())
                .with_property("bootstrap.servers", "127.0.0.1:9092".to_string())
                .with_property("group.id", "krishiv-sql".to_string());
            for (key, value) in &cmd.options {
                match key.as_str() {
                    "bootstrap.servers" => {
                        cfg = cfg.with_property("bootstrap.servers", value.clone());
                    }
                    "group.id" => {
                        cfg = cfg.with_property("group.id", value.clone());
                    }
                    other => {
                        cfg = cfg.with_property(other, value.clone());
                    }
                }
            }
            if let Some(ms) = kafka_auto_commit_interval_ms() {
                cfg = cfg.with_property("auto.commit.interval.ms", ms.to_string());
            }
            cfg
        }
        _ => ConnectorConfig::new(name, kind).with_property("path", cmd.location.clone()),
    })
}

fn connector_error(err: ConnectorError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// Factory for bounded connector sources opened through the registry.
pub struct ConnectorTableFactory {
    connector_kind: &'static str,
    registry: Arc<ConnectorRegistry>,
    streaming_sources: Option<Arc<RwLock<HashSet<String>>>>,
}

impl std::fmt::Debug for ConnectorTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorTableFactory")
            .field("connector_kind", &self.connector_kind)
            .finish_non_exhaustive()
    }
}

impl ConnectorTableFactory {
    pub fn bounded(connector_kind: &'static str, registry: Arc<ConnectorRegistry>) -> Self {
        Self {
            connector_kind,
            registry,
            streaming_sources: None,
        }
    }

    pub fn streaming(streaming_sources: Arc<RwLock<HashSet<String>>>) -> Self {
        Self {
            connector_kind: "kafka",
            registry: shared_connector_registry(),
            streaming_sources: Some(streaming_sources),
        }
    }
}

#[async_trait]
impl TableProviderFactory for ConnectorTableFactory {
    async fn create(
        &self,
        _state: &dyn datafusion::catalog::Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let config = connector_config_from_ddl(self.connector_kind, cmd)?;
        self.registry
            .validate_source(&config)
            .map_err(connector_error)?;

        if self.connector_kind == "kafka" {
            return create_kafka_table_provider(cmd, &config, self.streaming_sources.as_ref())
                .await;
        }

        let schema: SchemaRef = cmd.schema.as_ref().inner().clone();
        Ok(Arc::new(BoundedConnectorProvider {
            registry: Arc::clone(&self.registry),
            config,
            schema,
        }))
    }
}

async fn create_kafka_table_provider(
    cmd: &CreateExternalTable,
    config: &ConnectorConfig,
    streaming_sources: Option<&Arc<RwLock<HashSet<String>>>>,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    use krishiv_connectors::kafka::{KafkaConfig, KafkaSource};

    let kafka_config = KafkaConfig::from_config(config).map_err(connector_error)?;
    let schema: SchemaRef = cmd.schema.as_ref().inner().clone();
    let source = KafkaSource::new(kafka_config).map_err(connector_error)?;
    let partition = Arc::new(KafkaPartitionStream::new(schema.clone(), source));
    let table = StreamingTable::try_new(schema, vec![partition])?;

    if let Some(streaming_sources) = streaming_sources {
        let table_name = cmd.name.table().to_string();
        streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(table_name);
    }

    Ok(Arc::new(table))
}

/// Bounded scan provider that materializes all connector batches at scan time.
struct BoundedConnectorProvider {
    registry: Arc<ConnectorRegistry>,
    config: ConnectorConfig,
    schema: SchemaRef,
}

impl std::fmt::Debug for BoundedConnectorProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedConnectorProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for BoundedConnectorProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        use datafusion::common::stats::Precision;
        use datafusion::physical_plan::Statistics;
        let row_count = self.registry.estimated_row_count(&self.config)?;
        Some(Statistics {
            num_rows: Precision::Inexact(row_count as usize),
            ..Statistics::new_unknown(&self.schema)
        })
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut source = self
            .registry
            .open_source(&self.config)
            .await
            .map_err(connector_error)?;

        // T7: apply the user's projection and limit eagerly. The previous
        // implementation drained the entire source into a `MemTable` and
        // deferred the projection and limit to DataFusion's
        // `MemTable::scan`. That is correct but forces the connector to
        // materialise every row and every column before any predicate
        // runs, defeating Parquet column-pruning and file-pruning for any
        // sink that does not have a `DataSourceExec` shim. Eager
        // projection and limit short-circuits here bring the connector's
        // behaviour closer to the `DataSourceExec` path and significantly
        // reduce memory pressure for large bounded sources.
        //
        // Filter pushdown to the connector remains a follow-up: the
        // connector `Source` trait does not yet accept filter
        // expressions, and DataFusion's physical-expression builder is
        // version-sensitive. For now, filters are still applied by
        // DataFusion's downstream `MemTable::scan` so the result is
        // identical — just less memory-efficient than a connector that
        // accepts pushdown filters.
        let projection_columns: Option<Vec<String>> = projection.map(|idxs| {
            idxs.iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect()
        });
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut rows_accumulated: usize = 0;
        let limit_threshold: Option<usize> = limit;
        loop {
            let batch = source.read_batch_dyn().await.map_err(connector_error)?;
            let Some(batch) = batch else { break };
            let batch = project_batch(&batch, &self.schema)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            // Project to the user-requested columns.
            let batch = match &projection_columns {
                Some(cols) => project_to_columns(&batch, cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                None => batch,
            };
            if batch.num_rows() == 0 {
                continue;
            }
            // Honour the limit by truncating the last batch.
            let batch = match limit_threshold {
                Some(threshold) if rows_accumulated + batch.num_rows() > threshold => {
                    let take = threshold.saturating_sub(rows_accumulated);
                    batch.slice(0, take)
                }
                _ => batch,
            };
            rows_accumulated += batch.num_rows();
            batches.push(batch);
            if let Some(threshold) = limit_threshold
                && rows_accumulated >= threshold
            {
                break;
            }
        }

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        table.scan(state, projection, filters, limit).await
    }
}

/// T7: project a batch down to the named columns.
fn project_to_columns(
    batch: &RecordBatch,
    columns: &[String],
) -> arrow::error::Result<RecordBatch> {
    if columns.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let mut cols = Vec::with_capacity(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = batch.schema().index_of(name)?;
        cols.push(batch.column(idx).clone());
        fields.push(batch.schema().field(idx).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn bounded_connector_provider_statistics_returns_none_for_unknown_table() {
        let registry = Arc::new(krishiv_connectors::ConnectorRegistry::new());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let config = krishiv_connectors::ConnectorConfig::new("unknown", "parquet");
        let provider = BoundedConnectorProvider {
            registry,
            config,
            schema,
        };
        assert!(
            provider.statistics().is_none(),
            "no path in config → estimated_row_count returns None → statistics returns None"
        );
    }

    #[test]
    fn extract_create_external_table_name_parses_table_name() {
        assert_eq!(
            super::super::extract_create_external_table_name(
                "CREATE EXTERNAL TABLE my_table STORED AS PARQUET LOCATION 'data.parquet'"
            ),
            Some("my_table".to_string())
        );
        assert_eq!(
            super::super::extract_create_external_table_name("SELECT * FROM foo"),
            None
        );
        assert_eq!(
            super::super::extract_create_external_table_name(
                "CREATE OR REPLACE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 'orders.parquet'"
            ),
            Some("orders".to_string())
        );
    }

    /// T7: `project_to_columns` must keep column order and tolerate an
    /// empty column list (returns an empty projection with the original
    /// schema).
    #[test]
    fn project_to_columns_preserves_order_and_handles_empty() {
        use arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as _,
                Arc::new(Int64Array::from(vec![3, 4])) as _,
                Arc::new(Int64Array::from(vec![5, 6])) as _,
            ],
        )
        .unwrap();
        // Reorder: c, a
        let projected = super::project_to_columns(&batch, &[String::from("c"), String::from("a")])
            .expect("project must succeed");
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "c");
        assert_eq!(projected.schema().field(1).name(), "a");
        // No-op projection.
        let no_op = super::project_to_columns(&batch, &[]).expect("no-op projection must succeed");
        assert_eq!(no_op.num_columns(), 0);
    }
}
