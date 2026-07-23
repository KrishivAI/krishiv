//! DataFusion `TableProviderFactory` implementations backed by
//! [`krishiv_connectors::ConnectorRegistry`].

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::catalog::TableProviderFactory;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::ExecutionPlan;
use krishiv_connectors::{ConnectorConfig, ConnectorError, ConnectorRegistry, default_registry};

use crate::kafka_table::{KafkaPartitionStream, kafka_auto_commit_interval_ms, project_batch};

/// Whether a `CREATE EXTERNAL TABLE` LOCATION is an object-store URL (S3/GCS/
/// Azure) rather than a local filesystem path. Object-store URLs must not be
/// run through local-filesystem canonicalization, and `STORED AS PARQUET`
/// against one is a native DataFusion ListingTable read, not a connector source.
fn is_object_store_url(location: &str) -> bool {
    let l = location.trim_start();
    ["s3://", "s3a://", "gs://", "gcs://", "az://", "azure://", "abfs://", "abfss://"]
        .iter()
        .any(|scheme| l.starts_with(scheme))
}

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
    #[cfg(feature = "jdbc")]
    table_factories.insert(
        "JDBC".to_string(),
        Arc::new(ConnectorTableFactory::bounded(
            "jdbc",
            shared_connector_registry(),
        )),
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
        // JDBC pull source (Phase 31 ingest breadth): LOCATION is the bare
        // Postgres connection URL (no warehouse-path validation — it is not a
        // filesystem path). Options: `table` (required, validated by the
        // registry driver), `cursor.column`/`cursor.after` for incremental
        // keyset pull, `batch_size` for page sizing.
        "jdbc" => {
            let mut cfg =
                ConnectorConfig::new(name, kind).with_property("url", cmd.location.clone());
            for (key, value) in &cmd.options {
                // DataFusion namespaces un-dotted OPTIONS keys under
                // `format.` — accept both spellings of the same option.
                let key = key.strip_prefix("format.").unwrap_or(key);
                match key {
                    "table" | "cursor.column" | "cursor.after" | "batch_size" => {
                        cfg = cfg.with_property(key, value.clone());
                    }
                    other => {
                        return Err(DataFusionError::External(Box::new(
                            ConnectorError::Unsupported {
                                message: format!(
                                    "unknown JDBC option '{other}' (expected table, \
                                     cursor.column, cursor.after, batch_size)"
                                ),
                            },
                        )));
                    }
                }
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
        state: &dyn datafusion::catalog::Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        // `STORED AS PARQUET LOCATION 's3://…'` is a native DataFusion
        // ListingTable read of object storage, not a connector source. The
        // SqlEngine has already registered the backing S3 object store on the
        // runtime env (register_s3_object_store_for_warehouse, invoked before
        // this DDL executes), so delegate to DataFusion's own
        // ListingTableFactory: it looks up the Parquet FileFormat, lists the
        // location to infer the schema, and builds the ListingTable. This
        // bypasses the connector path's local-filesystem `canonicalize`, which
        // cannot resolve an s3:// URL and previously failed the DDL with
        // "path 's3://…' not accessible: No such file or directory"
        // (engine-s3-ddl-gap).
        if self.connector_kind == "parquet" && is_object_store_url(&cmd.location) {
            return datafusion::datasource::listing_table_factory::ListingTableFactory::new()
                .create(state, cmd)
                .await;
        }

        // `connector_config_from_ddl` calls `validate_path_under_warehouse`,
        // which does blocking `Path::canonicalize` syscalls. Run it on the
        // blocking pool so this async `create` never stalls the DataFusion/
        // Flight SQL async worker thread on filesystem I/O.
        let kind = self.connector_kind;
        let cmd_owned = cmd.clone();
        let config =
            tokio::task::spawn_blocking(move || connector_config_from_ddl(kind, &cmd_owned))
                .await
                .map_err(|e| {
                    DataFusionError::External(Box::new(ConnectorError::Unsupported {
                        message: format!("connector config validation task panicked: {e}"),
                    }))
                })??;
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

/// Bounded scan provider that streams connector batches at execution time.
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
        // Zero-materialization scan (Phase 52 #194): the source is opened
        // lazily at execution time and its batches flow straight into the
        // query pipeline. The previous implementation drained the entire
        // source into a `MemTable` at scan time — projection and limit are
        // now applied per batch by `StreamingTableExec` and DataFusion's
        // limit operator, which also cancels the source early by dropping
        // the stream. Filter pushdown to the connector remains a follow-up
        // (the `Source` trait does not accept filter expressions); filters
        // run in DataFusion's downstream `FilterExec` exactly as before.
        let partition = Arc::new(BoundedConnectorPartitionStream {
            registry: Arc::clone(&self.registry),
            config: self.config.clone(),
            schema: Arc::clone(&self.schema),
        });
        let table = StreamingTable::try_new(Arc::clone(&self.schema), vec![partition])?;
        table.scan(state, projection, filters, limit).await
    }
}

/// Lazily streams a bounded connector source, one `read_batch` at a time.
///
/// Each execution opens a fresh source from the registry (sources are
/// single-pass); raw connector batches are normalized to the declared table
/// schema per batch. Zero-row batches are dropped, matching the drained
/// implementation this replaces.
struct BoundedConnectorPartitionStream {
    registry: Arc<ConnectorRegistry>,
    config: ConnectorConfig,
    schema: SchemaRef,
}

impl std::fmt::Debug for BoundedConnectorPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedConnectorPartitionStream")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl datafusion::physical_plan::streaming::PartitionStream for BoundedConnectorPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        use futures::{StreamExt as _, TryStreamExt as _};

        let registry = Arc::clone(&self.registry);
        let config = self.config.clone();
        let schema = Arc::clone(&self.schema);
        let batch_schema = Arc::clone(&self.schema);
        let stream = futures::stream::once(async move {
            let source = registry
                .open_source(&config)
                .await
                .map_err(connector_error)?;
            Ok::<_, DataFusionError>(futures::stream::try_unfold(source, move |mut source| {
                let schema = Arc::clone(&batch_schema);
                async move {
                    loop {
                        match source.read_batch_dyn().await.map_err(connector_error)? {
                            Some(batch) => {
                                let batch = project_batch(&batch, &schema)
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                return Ok(Some((batch, source)));
                            }
                            None => return Ok(None),
                        }
                    }
                }
            }))
        })
        .try_flatten()
        .boxed();
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    /// Phase 31 ingest breadth: `STORED AS JDBC` DDL creates the provider
    /// without touching the database (connection is deferred to scan), and
    /// the option surface is closed — unknown keys and cursor misuse fail at
    /// DDL time, not at first pull.
    #[cfg(feature = "jdbc")]
    #[tokio::test]
    async fn jdbc_ddl_validates_options_without_connecting() {
        let engine = crate::SqlEngine::new();
        engine
            .sql(
                "CREATE EXTERNAL TABLE pg_orders (id BIGINT, amount DOUBLE) \
                 STORED AS JDBC LOCATION 'postgres://u:p@127.0.0.1:1/db' \
                 OPTIONS ('table' 'public.orders', 'cursor.column' 'id', \
                 'cursor.after' '42', 'batch_size' '500')",
            )
            .await
            .expect("jdbc DDL must succeed without a live database");

        let unknown = engine
            .sql(
                "CREATE EXTERNAL TABLE pg_bad (id BIGINT) STORED AS JDBC \
                 LOCATION 'postgres://u:p@127.0.0.1:1/db' \
                 OPTIONS ('table' 't', 'bogus' 'x')",
            )
            .await
            .expect_err("unknown option must be rejected");
        assert!(
            unknown.to_string().contains("unknown JDBC option"),
            "{unknown}"
        );

        let dangling_cursor = engine
            .sql(
                "CREATE EXTERNAL TABLE pg_bad2 (id BIGINT) STORED AS JDBC \
                 LOCATION 'postgres://u:p@127.0.0.1:1/db' \
                 OPTIONS ('table' 't', 'cursor.after' '7')",
            )
            .await
            .expect_err("cursor.after without cursor.column must be rejected");
        assert!(
            dangling_cursor
                .to_string()
                .contains("cursor.after requires cursor.column"),
            "{dangling_cursor}"
        );
    }

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
}
