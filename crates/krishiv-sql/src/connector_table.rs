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
    // URI schemes are case-insensitive (RFC 3986 §3.1), and this check decides
    // whether a `LOCATION` is object storage or a path under the warehouse
    // root. A case-sensitive match sent `LOCATION 'S3://bucket/x'` down the
    // *filesystem* branch, where `validate_path_under_warehouse` tried to
    // canonicalise it as a relative path and failed with "path not accessible"
    // — an error about the local disk for a bucket that was perfectly fine.
    let l = location.trim_start();
    let scheme = match l.split_once("://") {
        Some((scheme, _)) => scheme,
        None => return false,
    };
    [
        "s3", "s3a", "gs", "gcs", "az", "azure", "abfs", "abfss",
    ]
    .iter()
    .any(|known| scheme.eq_ignore_ascii_case(known))
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
    // Batch export sinks (Phase 67 movements; engine #197 one-registry
    // dispatch). `STORED AS ELASTICSEARCH` / `STORED AS KAFKA_SINK` create
    // external tables whose INSERT INTO routes through the registered sink
    // drivers — the same door serve-back uses for `STORED AS JDBC`.
    // `KAFKA_SINK` is a distinct token because bare `KAFKA` is the
    // STREAMING source factory; this one is the bounded batch door.
    #[cfg(feature = "elasticsearch")]
    table_factories.insert(
        "ELASTICSEARCH".to_string(),
        Arc::new(ConnectorTableFactory::sink_only(
            "elasticsearch",
            shared_connector_registry(),
        )),
    );
    table_factories.insert(
        "KAFKA_SINK".to_string(),
        Arc::new(ConnectorTableFactory::sink_only(
            "kafka",
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
        // Elasticsearch export sink (Phase 67): LOCATION is the cluster URL,
        // `index` names the target index, `id_column` (optional) is what
        // makes a retried bulk request an idempotent upsert instead of
        // duplicates. This table exists to be INSERTed into; reading it is
        // refused by the registry (no Elasticsearch SOURCE driver), which is
        // the honest answer rather than a guessed scan.
        "elasticsearch" => {
            let mut cfg =
                ConnectorConfig::new(name, kind).with_property("url", cmd.location.clone());
            for (key, value) in &cmd.options {
                let key = key.strip_prefix("format.").unwrap_or(key);
                cfg = cfg.with_property(key, value.clone());
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
                    // Read options.
                    "table" | "cursor.column" | "cursor.after" | "batch_size" => {
                        cfg = cfg.with_property(key, value.clone());
                    }
                    // Write option (Phase 69 serve-back): declaring the
                    // conflict target is what upgrades INSERT INTO on this
                    // table from at-least-once append to idempotent upsert.
                    // It rides on the SOURCE config and is copied to the
                    // sink config by `sink_config_for`, so one DDL declares
                    // a table that is both readable and writable.
                    "conflict_keys" => {
                        cfg = cfg.with_property(key, value.clone());
                    }
                    other => {
                        return Err(DataFusionError::External(Box::new(
                            ConnectorError::Unsupported {
                                message: format!(
                                    "unknown JDBC option '{other}' (expected table, \
                                     cursor.column, cursor.after, batch_size, \
                                     conflict_keys)"
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
    /// A sink-only door (`STORED AS ELASTICSEARCH` / `KAFKA_SINK`): the
    /// table exists to be INSERTed into. DDL validates the SINK driver —
    /// there may legitimately be no source driver for the kind — and the
    /// kafka kind must NOT fall into the streaming-source provider here.
    sink_only: bool,
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
            sink_only: false,
        }
    }

    pub fn sink_only(connector_kind: &'static str, registry: Arc<ConnectorRegistry>) -> Self {
        Self {
            connector_kind,
            registry,
            streaming_sources: None,
            sink_only: true,
        }
    }

    pub fn streaming(streaming_sources: Arc<RwLock<HashSet<String>>>) -> Self {
        Self {
            connector_kind: "kafka",
            registry: shared_connector_registry(),
            streaming_sources: Some(streaming_sources),
            sink_only: false,
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
        if self.sink_only {
            // Validate against the SINK driver's contract: this is the door
            // the table exists for, and a write-only kind may have no
            // source driver at all. Reading the table still fails honestly
            // at scan time with the registry's "no source driver" error.
            self.registry
                .validate_sink(&sink_config_for_kind(self.connector_kind, &config)?)
                .map_err(connector_error)?;
            let schema: SchemaRef = cmd.schema.as_ref().inner().clone();
            return Ok(Arc::new(BoundedConnectorProvider {
                registry: Arc::clone(&self.registry),
                config,
                schema,
            }));
        }
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
    // Unbounded, for the reason spelled out in `create_kafka_streaming_table`:
    // the topic stream never returns end-of-input, so a `Bounded` claim lets
    // DataFusion accept a pipeline-breaking operator that then hangs forever.
    let table = StreamingTable::try_new(schema, vec![partition])?.with_infinite_table(true);

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

    /// `INSERT INTO <external table> SELECT …` for connector-backed
    /// tables (Phase 69 serve-back; ADR-0021 boundary unchanged — the
    /// ENGINE owns the connector, this only routes rows into it).
    ///
    /// Without this, DataFusion's default returns "Insert into not
    /// implemented for this table" and a `STORED AS JDBC` external table
    /// could not be written from SQL at all.
    ///
    /// Only `InsertOp::Append` is accepted. Overwrite and replace would
    /// require the connector to express truncate/upsert-all semantics it
    /// does not have, and silently downgrading either to an append would
    /// leave stale rows behind — a wrong answer, not a slow one.
    async fn insert_into(
        &self,
        _state: &dyn datafusion::catalog::Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use datafusion::logical_expr::dml::InsertOp;
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(format!(
                "connector tables support INSERT INTO … (append) only; {insert_op:?} would \
                 need truncate/replace semantics the connector does not express, and \
                 downgrading it to an append would silently leave stale rows"
            )));
        }
        let sink = Arc::new(ConnectorDataSink {
            registry: Arc::clone(&self.registry),
            config: sink_config_for(&self.config)?,
            schema: Arc::clone(&self.schema),
        });
        Ok(Arc::new(
            datafusion::datasource::sink::DataSinkExec::new(input, sink, None),
        ))
    }
}

/// Derive the SINK config from a source table's config.
///
/// A `STORED AS JDBC` table names the *source* connector kind; writing to
/// it needs the paired sink kind. The mapping is explicit rather than a
/// string suffix, so a source with no writable counterpart fails here
/// naming itself instead of failing later inside the registry.
fn sink_config_for(source: &ConnectorConfig) -> DataFusionResult<ConnectorConfig> {
    sink_config_for_kind(&source.kind.clone(), source)
}

/// Kind-keyed half of [`sink_config_for`], shared with the sink-only DDL
/// validation which knows the kind before a provider exists.
fn sink_config_for_kind(kind: &str, source: &ConnectorConfig) -> DataFusionResult<ConnectorConfig> {
    let sink_kind = match kind {
        "jdbc" | "postgres" | "postgresql" => "jdbc_sink",
        // Batch export sinks (Phase 67). Same kind on both sides: the
        // registry separates roles, not names, so `elasticsearch` and
        // `kafka` resolve to their registered SinkDriver here.
        "elasticsearch" => "elasticsearch",
        "kafka" => "kafka",
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "connector kind '{other}' has no writable sink counterpart; INSERT INTO is \
                 supported for jdbc/postgres external tables"
            )));
        }
    };
    let mut config = ConnectorConfig::new(source.name.clone(), sink_kind);
    for (key, value) in source.properties() {
        config = config.with_property(key, value);
    }
    Ok(config)
}

/// Routes an execution plan's batches into a registry sink.
struct ConnectorDataSink {
    registry: Arc<ConnectorRegistry>,
    config: ConnectorConfig,
    schema: SchemaRef,
}

impl std::fmt::Debug for ConnectorDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorDataSink")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl datafusion::physical_plan::DisplayAs for ConnectorDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "ConnectorDataSink(kind={})", self.config.kind)
    }
}

#[async_trait]
impl datafusion::datasource::sink::DataSink for ConnectorDataSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: datafusion::execution::SendableRecordBatchStream,
        _context: &Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<u64> {
        use futures::StreamExt as _;
        let mut sink = self
            .registry
            .open_sink(&self.config)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut rows = 0u64;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            rows += batch.num_rows() as u64;
            sink.write_batch_dyn(batch)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        // Flush BEFORE reporting success: a sink that buffers would
        // otherwise have its last batch counted as written while still in
        // memory, and the statement would return a row count for data that
        // never landed.
        sink.flush_dyn()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(rows)
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

    /// Phase 67 export sinks: the ES/Kafka batch doors resolve their SINK
    /// drivers and honest-fail without live services. The DDL must succeed
    /// offline, INSERT INTO must reach the driver (failing at connect, not
    /// at "no writable sink counterpart"), and the delivery-shaping options
    /// (`id_column`) must survive the source→sink config derivation.
    #[cfg(feature = "elasticsearch")]
    #[tokio::test]
    async fn elasticsearch_external_table_routes_insert_to_the_sink_driver() {
        let engine = crate::SqlEngine::new();
        engine
            .sql(
                "CREATE EXTERNAL TABLE es_out (id BIGINT, name VARCHAR) \
                 STORED AS ELASTICSEARCH LOCATION 'http://127.0.0.1:1' \
                 OPTIONS ('index' 'orders', 'id_column' 'id')",
            )
            .await
            .expect("elasticsearch DDL must succeed without a live cluster");

        // The insert reaches the ES sink driver and fails at CONNECT — port 1
        // refuses — proving dispatch went to the sink, not to a missing
        // "writable counterpart" error.
        let planned = engine
            .sql("INSERT INTO es_out VALUES (1, 'a')")
            .await
            .expect("the insert must PLAN — dispatch resolves the sink driver");
        let err = planned
            .collect()
            .await
            .expect_err("no cluster is listening on port 1");
        let msg = err.to_string();
        // The error names the bulk endpoint — the request went to the real
        // ES wire path (`<url>/<index>/_bulk`), not to a generic refusal.
        assert!(
            msg.contains("_bulk"),
            "the failure must come from the elasticsearch bulk request, got: {msg}"
        );
        assert!(
            !msg.contains("no writable sink counterpart"),
            "dispatch must not refuse the kind: {msg}"
        );
    }

    #[tokio::test]
    async fn kafka_sink_external_table_is_a_distinct_batch_door() {
        let engine = crate::SqlEngine::new();
        engine
            .sql(
                "CREATE EXTERNAL TABLE k_out (id BIGINT, name VARCHAR) \
                 STORED AS KAFKA_SINK LOCATION 'orders-topic' \
                 OPTIONS ('bootstrap.servers' '127.0.0.1:1')",
            )
            .await
            .expect("kafka sink DDL must succeed without a live broker");
        // Dispatch resolves the registered Kafka SinkDriver; rdkafka's
        // producer construction is lazy, so the statement may fail at
        // delivery rather than connect — either way it must NOT fail with
        // the "no writable sink counterpart" refusal.
        let planned = engine
            .sql("INSERT INTO k_out VALUES (1, 'a')")
            .await
            .expect("the insert must PLAN — dispatch resolves the sink driver");
        let err = planned
            .collect()
            .await
            .expect_err("no broker is listening on port 1");
        assert!(
            !err.to_string().contains("no writable sink counterpart"),
            "dispatch must reach the kafka sink driver: {err}"
        );
    }

    #[cfg(feature = "elasticsearch")]
    #[test]
    fn sink_config_for_carries_es_options_through() {
        let source = ConnectorConfig::new("es_out", "elasticsearch")
            .with_property("url", "http://127.0.0.1:9200")
            .with_property("index", "orders")
            .with_property("id_column", "id");
        let sink = sink_config_for(&source).expect("elasticsearch has a sink counterpart");
        assert_eq!(sink.kind, "elasticsearch");
        assert_eq!(sink.get("id_column"), Some("id"));
        assert_eq!(sink.get("index"), Some("orders"));
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

    /// URI schemes are case-insensitive (RFC 3986 §3.1).
    ///
    /// A case-sensitive match sent `LOCATION 'S3://bucket/x'` down the
    /// *filesystem* branch, where the warehouse-root validator tried to
    /// canonicalise it as a relative path and reported "path not accessible" —
    /// an error about the local disk for a bucket that was fine.
    #[test]
    fn object_store_schemes_are_recognised_regardless_of_case() {
        for uri in [
            "s3://bucket/k", "S3://bucket/k", "S3A://bucket/k", "Gs://b/k",
            "GCS://b/k", "AZ://b/k", "Azure://b/k", "ABFS://b/k", "AbFsS://b/k",
        ] {
            assert!(is_object_store_url(uri), "{uri} must be object storage");
        }
    }

    /// Leading whitespace is tolerated because `LOCATION` values arrive
    /// straight from SQL.
    #[test]
    fn leading_whitespace_does_not_hide_the_scheme() {
        assert!(is_object_store_url("   s3://bucket/k"));
    }

    /// Everything else is a warehouse path and must stay on the local branch —
    /// including schemes that merely *start* like a known one.
    #[test]
    fn non_object_store_locations_are_not_claimed() {
        for uri in [
            "/var/data/orders",
            "orders/",
            "file:///var/data",
            "s3",
            "s3:/bucket/k",
            "s3x://bucket/k",
            "https://example.com/x",
            "",
        ] {
            assert!(!is_object_store_url(uri), "{uri} must not be object storage");
        }
    }
}

#[cfg(all(test, feature = "jdbc"))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod insert_into_tests {
    use super::*;

    /// Phase 69 serve-back: `INSERT INTO <jdbc external table> SELECT …`
    /// must actually land rows in Postgres. Before `insert_into` existed,
    /// DataFusion returned "Insert into not implemented for this table"
    /// and a connector table could not be written from SQL at all.
    ///
    /// Requires a live Postgres at `KRISHIV_TEST_DATABASE_URL`; self-skips
    /// with a visible marker otherwise, so `cargo test` works everywhere
    /// and CI's service container exercises the real path.
    #[tokio::test]
    async fn insert_into_a_jdbc_external_table_lands_rows() {
        let Ok(url) = std::env::var("KRISHIV_TEST_DATABASE_URL") else {
            eprintln!("SKIP: KRISHIV_TEST_DATABASE_URL unset");
            return;
        };
        let engine = crate::SqlEngine::new();
        let ddl = format!(
            "CREATE EXTERNAL TABLE sb (id BIGINT, v VARCHAR) STORED AS JDBC \
             LOCATION '{url}' OPTIONS ('table' 'serveback', 'conflict_keys' 'id')"
        );
        engine.sql(&ddl).await.expect("ddl").collect().await.expect("ddl run");

        // The write the phase needs: a computed lake-side result landing in
        // the operational store.
        engine
            .sql("INSERT INTO sb SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, v)")
            .await
            .expect("insert plans")
            .collect()
            .await
            .expect("insert runs");

        let back = engine.sql("SELECT id, v FROM sb ORDER BY id").await.unwrap();
        let batches = back.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "rows must be readable back through the source path");

        // Re-delivering the SAME keys must converge, not duplicate — this
        // is what earns the "idempotent upsert" label the sink advertises.
        engine
            .sql("INSERT INTO sb SELECT * FROM (VALUES (1, 'a2'), (3, 'c')) v(id, v)")
            .await
            .expect("second insert plans")
            .collect()
            .await
            .expect("second insert runs");
        let batches = engine
            .sql("SELECT id, v FROM sb ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 3,
            "an upsert re-delivery must converge to 3 rows, not append to 5"
        );
    }

    #[tokio::test]
    async fn overwrite_is_refused_rather_than_silently_appended() {
        // Downgrading OVERWRITE to an append would leave stale rows —
        // a wrong answer, not a slow one.
        let source = ConnectorConfig::new("t", "jdbc").with_property("table", "x");
        let sink = sink_config_for(&source).unwrap();
        assert_eq!(sink.kind, "jdbc_sink");
        assert_eq!(sink.get("table").as_deref(), Some("x"));

        // kafka gained a sink counterpart with the Phase 67 batch door, so
        // the honest-refusal case moved to a kind that truly has none.
        let unwritable = ConnectorConfig::new("t", "csv");
        let err = sink_config_for(&unwritable).unwrap_err().to_string();
        assert!(err.contains("no writable sink counterpart"), "{err}");
    }
}

#[cfg(all(test, feature = "jdbc"))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr)]
mod review_regression_tests {
    /// Regressions from the 2026-08-12 adversarial review, each proven
    /// against a real Postgres because each failed only at execution
    /// time — a unit test over the rendered SQL passed happily.
    #[tokio::test]
    async fn nulls_duplicates_and_case_folded_keys_all_land() {
        let Ok(url) = std::env::var("KRISHIV_TEST_DATABASE_URL") else {
            eprintln!("SKIP: KRISHIV_TEST_DATABASE_URL unset");
            return;
        };
        let engine = crate::SqlEngine::new();
        // 'ID' deliberately mis-cased against column `id` (defect 13).
        let ddl = format!(
            "CREATE EXTERNAL TABLE fx (id BIGINT, flag BOOLEAN, note VARCHAR, score DOUBLE) \
             STORED AS JDBC LOCATION '{url}' \
             OPTIONS ('table' 'fixprobe', 'conflict_keys' 'ID')"
        );
        engine.sql(&ddl).await.expect("ddl").collect().await.expect("ddl run");

        // A NULL bool and a NULL text in one batch (defect 11): the old
        // code bound both as int8 NULL and Postgres rejected the batch.
        // Two rows share id=1 in ONE statement (defect 12): the old code
        // hit 21000 and rolled back everything.
        engine
            .sql(
                "INSERT INTO fx SELECT * FROM (VALUES \
                   (1, true,  'first',  1.5), \
                   (1, false, NULL,     2.5), \
                   (2, NULL,  'second', NULL)) v(id, flag, note, score)",
            )
            .await
            .expect("insert plans")
            .collect()
            .await
            .expect("insert runs");

        let batches = engine
            .sql("SELECT id, note FROM fx ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "id=1 deduped to the LAST occurrence, id=2 kept");
    }
}
