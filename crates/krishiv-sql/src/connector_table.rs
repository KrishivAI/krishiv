//! DataFusion `TableProviderFactory` implementations backed by
//! [`krishiv_connectors::ConnectorRegistry`].

use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::catalog::TableProviderFactory;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::ExecutionPlan;
use krishiv_connectors::{
    ConnectorConfig, ConnectorError, ConnectorRegistry, default_registry,
};

use crate::kafka_table::{KafkaPartitionStream, kafka_auto_commit_interval_ms, project_batch};

/// Shared registry instance for SQL DDL table factories.
pub fn shared_connector_registry() -> Arc<ConnectorRegistry> {
    Arc::new(default_registry())
}

/// Register PARQUET, S3, and KAFKA DDL factories on a DataFusion table-factory map.
pub fn register_connector_table_factories(
    table_factories: &mut std::collections::HashMap<
        String,
        Arc<dyn TableProviderFactory>,
    >,
    streaming_sources: Arc<RwLock<HashSet<String>>>,
) {
    let registry = shared_connector_registry();
    table_factories.insert(
        "PARQUET".to_string(),
        Arc::new(ConnectorTableFactory::bounded("parquet", Arc::clone(&registry))),
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
pub fn connector_config_from_ddl(kind: &str, cmd: &CreateExternalTable) -> ConnectorConfig {
    let name = cmd.name.table().to_string();
    match kind {
        "parquet" => ConnectorConfig::new(name, kind).with_property("path", cmd.location.clone()),
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
    }
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
        let config = connector_config_from_ddl(self.connector_kind, cmd);
        self.registry
            .validate_source(&config)
            .map_err(connector_error)?;

        if self.connector_kind == "kafka" {
            return create_kafka_table_provider(cmd, &config, self.streaming_sources.as_ref()).await;
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

        let mut batches = Vec::new();
        loop {
            let batch = source
                .read_batch_dyn()
                .await
                .map_err(connector_error)?
                .map(|batch| project_batch(&batch, &self.schema));
            match batch {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }

        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        table.scan(state, projection, filters, limit).await
    }
}
