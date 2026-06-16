//! CDC-to-lakehouse pipeline orchestration.

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::ConnectorError;

use super::debezium::{parse_debezium_envelope, parse_debezium_envelope_result, CdcEvent, CdcOp, DebeziumParseError, RawCdcRecord};
#[cfg(feature = "kafka")]
use super::kafka_source::{KafkaCdcConfig, RdkafkaCdcEventSource};

/// A source of raw Debezium JSON strings for a CDC pipeline.
///
/// Implement this trait to plug any Kafka client (or test fixture) into
/// [`CdcToLakehousePipeline::run_with_source`].
pub trait CdcEventSource: Send {
    /// Poll up to `max` raw Debezium JSON event strings.
    ///
    /// Returns an empty `Vec` when the source is exhausted (signals pipeline
    /// shutdown). Returns `Err` on unrecoverable source failures.
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError>;

    /// Poll records with source offset identity.
    ///
    /// Sources that can expose real Kafka offsets should override this method.
    /// The default preserves legacy in-memory behavior with synthetic offsets.
    fn poll_records(&mut self, max: usize) -> Result<Vec<RawCdcRecord>, ConnectorError> {
        Ok(self
            .poll_events(max)?
            .into_iter()
            .enumerate()
            .map(|(i, payload)| RawCdcRecord::new(payload, 0, i as i64))
            .collect())
    }

    /// Whether an empty poll means temporary idleness instead of source exhaustion.
    fn is_live(&self) -> bool {
        false
    }

    /// Commit consumed offsets after a successful downstream write.
    ///
    /// Default implementation is a no-op (stateless / in-memory sources).
    /// Kafka-backed sources must override this to flush consumer offsets only
    /// after the batch has been durably committed downstream (P1-14).
    fn commit_offsets(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// In-memory [`CdcEventSource`] backed by a pre-loaded `Vec<String>`.
///
/// Drains from the front; returns empty when exhausted. Intended for tests
/// and local development without a live Kafka cluster.
pub struct InMemoryCdcEventSource {
    events: std::collections::VecDeque<String>,
}

impl InMemoryCdcEventSource {
    /// Create a source pre-loaded with `events`.
    pub fn new(events: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            events: events.into_iter().map(Into::into).collect(),
        }
    }
}

impl CdcEventSource for InMemoryCdcEventSource {
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
        let n = max.min(self.events.len());
        Ok(self.events.drain(..n).collect())
    }
}

/// Wire format used for raw CDC records decoded through Schema Registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CdcSchemaRegistryFormat {
    /// Confluent-framed Avro. This is the compatibility default.
    #[default]
    Avro,
    /// Confluent-framed Protobuf.
    Protobuf,
}

/// Configuration for a CDC-to-lakehouse pipeline.
#[derive(Debug, Clone)]
pub struct CdcToLakehousePipeline {
    /// Kafka topic carrying Debezium change events.
    pub source_topic: String,
    /// Kafka broker addresses.
    pub kafka_brokers: Vec<String>,
    /// Iceberg catalog identifier.
    pub iceberg_catalog: String,
    /// Target Iceberg table (e.g. "warehouse.orders").
    pub iceberg_table: String,
    /// Primary key columns used for upsert/delete semantics.
    pub primary_key_columns: Vec<String>,
    /// Optional Confluent Schema Registry URL.
    pub schema_registry_url: Option<String>,
    /// Payload format used when `raw_bytes` records are decoded.
    pub schema_registry_format: CdcSchemaRegistryFormat,
    /// Number of CDC events to accumulate before writing a single Arrow batch.
    ///
    /// Defaults to 1000. Higher values reduce write amplification; lower values
    /// reduce end-to-end latency.
    pub batch_size: usize,
    /// Keep output schemas compatible across CDC batches by null-filling new
    /// columns and dropping deprecated columns according to the merged schema.
    pub schema_evolution: bool,
}

impl CdcToLakehousePipeline {
    /// Create a new pipeline configuration.
    pub fn new(
        source_topic: impl Into<String>,
        kafka_brokers: Vec<String>,
        iceberg_catalog: impl Into<String>,
        iceberg_table: impl Into<String>,
        primary_key_columns: Vec<String>,
    ) -> Self {
        #[cfg(not(feature = "state"))]
        tracing::warn!(
            "CdcToLakehousePipeline: the `state` feature is not enabled. \
             Kafka offset tracking is unavailable — this pipeline will restart \
             from the earliest available offset on crash or restart, potentially \
             reprocessing already-committed data. Enable the `state` feature to \
             persist offsets across restarts."
        );
        Self {
            source_topic: source_topic.into(),
            kafka_brokers,
            iceberg_catalog: iceberg_catalog.into(),
            iceberg_table: iceberg_table.into(),
            primary_key_columns,
            schema_registry_url: None,
            schema_registry_format: CdcSchemaRegistryFormat::default(),
            batch_size: 1000,
            schema_evolution: true,
        }
    }

    /// Attach a schema registry URL.
    pub fn with_schema_registry(mut self, url: impl Into<String>) -> Self {
        self.schema_registry_url = Some(url.into());
        self
    }

    /// Select the format used for binary schema-registry records.
    #[must_use]
    pub fn with_schema_registry_format(mut self, format: CdcSchemaRegistryFormat) -> Self {
        self.schema_registry_format = format;
        self
    }

    /// Set the number of CDC events to accumulate before writing a single Arrow batch.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable or disable cross-batch schema evolution normalization.
    #[must_use]
    pub fn with_schema_evolution(mut self, enabled: bool) -> Self {
        self.schema_evolution = enabled;
        self
    }

    /// Validate the pipeline configuration. Returns an error if required fields are missing.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.source_topic.trim().is_empty() {
            return Err(ConnectorError::Cdc("source_topic must not be empty".into()));
        }
        if self.kafka_brokers.is_empty()
            || self
                .kafka_brokers
                .iter()
                .any(|broker| broker.trim().is_empty())
        {
            return Err(ConnectorError::Cdc(
                "kafka_brokers must contain only non-blank addresses".into(),
            ));
        }
        if self.iceberg_catalog.trim().is_empty() {
            return Err(ConnectorError::Cdc(
                "iceberg_catalog must not be empty".into(),
            ));
        }
        if self.iceberg_table.trim().is_empty() {
            return Err(ConnectorError::Cdc(
                "iceberg_table must not be empty".into(),
            ));
        }
        if self.primary_key_columns.is_empty()
            || self
                .primary_key_columns
                .iter()
                .any(|column| column.trim().is_empty())
        {
            return Err(ConnectorError::Cdc(
                "primary_key_columns must not be empty for upsert semantics".into(),
            ));
        }
        let unique_primary_keys = self
            .primary_key_columns
            .iter()
            .map(|column| column.trim())
            .collect::<std::collections::HashSet<_>>();
        if unique_primary_keys.len() != self.primary_key_columns.len() {
            return Err(ConnectorError::Cdc(
                "primary_key_columns must not contain duplicates".into(),
            ));
        }
        if self.batch_size == 0 {
            return Err(ConnectorError::Cdc(
                "batch_size must be greater than zero".into(),
            ));
        }
        if self
            .schema_registry_url
            .as_deref()
            .is_some_and(|url| url.trim().is_empty())
        {
            return Err(ConnectorError::Cdc(
                "schema_registry_url must not be blank".into(),
            ));
        }
        #[cfg(not(feature = "schema-registry"))]
        if self.schema_registry_url.is_some() {
            return Err(ConnectorError::Cdc(
                "schema_registry_url requires the krishiv-connectors schema-registry feature"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Run the pipeline with an external event source.
    ///
    /// Polls `source` for up to `batch_size` raw Debezium JSON strings per
    /// iteration, parses them with [`parse_debezium_envelope`], builds an Arrow
    /// batch with [`build_batch_from_events`], and passes it to `on_batch`.
    /// Stops when `source.poll_records` returns an empty slice (source
    /// exhausted) or the provided `shutdown` channel fires.
    pub async fn run_with_source<S, F>(
        &self,
        mut source: S,
        mut on_batch: F,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), ConnectorError>
    where
        S: CdcEventSource,
        F: FnMut(RecordBatch) -> Result<(), ConnectorError>,
    {
        self.validate()?;
        let mut schema_state = CdcSchemaEvolutionState::default();

        // Build a schema-registry deserializer if a URL is configured.
        // When present, records with raw_bytes are decoded via the registry
        // instead of being parsed as Debezium JSON.
        #[cfg(feature = "schema-registry")]
        let registry_client = self
            .schema_registry_url
            .as_deref()
            .map(crate::schema_registry::SchemaRegistryClient::new)
            .transpose()
            .map_err(|error| {
                ConnectorError::Cdc(format!("invalid schema-registry configuration: {error}"))
            })?;
        #[cfg(not(feature = "schema-registry"))]
        let _ = &self.schema_registry_url; // suppress unused warning

        loop {
            if *shutdown.borrow() {
                break;
            }
            let raw = source.poll_records(self.batch_size)?;
            if raw.is_empty() {
                if source.is_live() {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    continue;
                } else {
                    break;
                }
            }

            let binary_record_count = raw
                .iter()
                .filter(|record| record.raw_bytes.is_some())
                .count();
            if binary_record_count != 0 && binary_record_count != raw.len() {
                return Err(ConnectorError::Cdc(
                    "CDC source returned a mixed batch of binary and plain-text records".into(),
                ));
            }

            // Decode binary records using the explicitly configured registry
            // format; otherwise parse Debezium JSON envelopes.
            #[cfg(feature = "schema-registry")]
            let batch_result: Result<RecordBatch, ConnectorError> = {
                if binary_record_count == raw.len() {
                    let client = registry_client.as_ref().ok_or_else(|| {
                        ConnectorError::Cdc("binary CDC records require schema_registry_url".into())
                    })?;
                    let format = match self.schema_registry_format {
                        CdcSchemaRegistryFormat::Avro => {
                            crate::schema_registry::RegistryFormat::Avro
                        }
                        CdcSchemaRegistryFormat::Protobuf => {
                            crate::schema_registry::RegistryFormat::Protobuf
                        }
                    };
                    let mut batches = Vec::with_capacity(raw.len());
                    for (i, record) in raw.iter().enumerate() {
                        let bytes = record.raw_bytes.as_deref().ok_or_else(|| {
                            ConnectorError::Cdc(
                                "CDC binary batch shape changed during decoding".into(),
                            )
                        })?;
                        let (_schema, decoded) = client
                            .decode_with_format(bytes, format)
                            .await
                            .map_err(|error| {
                                ConnectorError::Cdc(format!(
                                    "schema-registry decode error at index {i}: {error}"
                                ))
                            })?;
                        batches.extend(decoded);
                    }
                    if batches.is_empty() {
                        return Err(ConnectorError::Cdc(
                            "schema-registry decoder produced no rows for a non-empty source batch"
                                .into(),
                        ));
                    }
                    concat_registry_batches(&batches).map_err(ConnectorError::Cdc)
                } else {
                    let events = super::debezium::parse_debezium_records(&raw).map_err(ConnectorError::Cdc)?;
                    if events.is_empty() {
                        continue;
                    }
                    build_batch_from_events(&events)
                        .map_err(|e| ConnectorError::Cdc(format!("batch error: {e}")))
                }
            };
            #[cfg(not(feature = "schema-registry"))]
            let batch_result: Result<RecordBatch, ConnectorError> = {
                if binary_record_count != 0 {
                    return Err(ConnectorError::Cdc(
                        "binary CDC records require the krishiv-connectors schema-registry feature"
                            .into(),
                    ));
                }
                let events = super::debezium::parse_debezium_records(&raw).map_err(ConnectorError::Cdc)?;
                if events.is_empty() {
                    continue;
                }
                build_batch_from_events(&events)
                    .map_err(|e| ConnectorError::Cdc(format!("batch error: {e}")))
            };

            let mut batch = batch_result?;
            if self.schema_evolution {
                batch = schema_state.normalize(batch).map_err(ConnectorError::Cdc)?;
            }
            on_batch(batch)?;
            // P1-14: commit offsets only after the downstream sink write succeeds.
            source.commit_offsets()?;
        }
        Ok(())
    }

    /// Run CDC ingestion into an Iceberg two-phase sink.
    ///
    /// This is the certified in-process commit protocol used by CDC
    /// integration tests: source offsets are committed only after the Iceberg
    /// snapshot commit succeeds, and the committed offsets are written into the
    /// snapshot metadata summary map.
    pub async fn run_with_iceberg_sink<S, I>(
        &self,
        source: S,
        iceberg: &I,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Vec<i64>, ConnectorError>
    where
        S: CdcEventSource,
        I: crate::lakehouse::IcebergTwoPhaseCommit,
    {
        self.run_with_iceberg_sink_inner(source, iceberg, shutdown, None)
            .await
    }

    /// Run CDC ingestion into an Iceberg sink until `max_commits` snapshots commit.
    ///
    /// This is intended for live broker certification tests where a Kafka
    /// source remains idle after the produced certification records are read.
    pub async fn run_with_iceberg_sink_until_commits<S, I>(
        &self,
        source: S,
        iceberg: &I,
        shutdown: tokio::sync::watch::Receiver<bool>,
        max_commits: usize,
    ) -> Result<Vec<i64>, ConnectorError>
    where
        S: CdcEventSource,
        I: crate::lakehouse::IcebergTwoPhaseCommit,
    {
        self.run_with_iceberg_sink_inner(source, iceberg, shutdown, Some(max_commits))
            .await
    }

    async fn run_with_iceberg_sink_inner<S, I>(
        &self,
        mut source: S,
        iceberg: &I,
        shutdown: tokio::sync::watch::Receiver<bool>,
        max_commits: Option<usize>,
    ) -> Result<Vec<i64>, ConnectorError>
    where
        S: CdcEventSource,
        I: crate::lakehouse::IcebergTwoPhaseCommit,
    {
        self.validate()?;
        let mut schema_state = CdcSchemaEvolutionState::default();
        let mut committed_snapshots = Vec::new();

        loop {
            if *shutdown.borrow() {
                break;
            }
            let raw = source.poll_records(self.batch_size)?;
            if raw.is_empty() {
                if source.is_live() {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    continue;
                } else {
                    break;
                }
            }
            if raw.iter().any(|record| record.raw_bytes.is_some()) {
                return Err(ConnectorError::Cdc(
                    "binary schema-registry records are not supported by the Iceberg CDC sink path"
                        .into(),
                ));
            }

            let events = raw
                .iter()
                .enumerate()
                .map(|(i, record)| {
                    parse_debezium_envelope_result(
                        &record.payload,
                        record.partition_id,
                        record.offset,
                    )
                    .map_err(|e| {
                        ConnectorError::Cdc(format!("Debezium parse error at batch index {i}: {e}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if events.is_empty() {
                continue;
            }

            let mut batch = build_batch_from_events(&events)
                .map_err(|e| ConnectorError::Cdc(format!("batch error: {e}")))?;
            if self.schema_evolution {
                batch = schema_state.normalize(batch).map_err(ConnectorError::Cdc)?;
            }
            let offsets = kafka_offsets_for_events(&events);
            let staged = iceberg
                .prepare(vec![batch])
                .await
                .map_err(|e| ConnectorError::Cdc(format!("iceberg prepare failed: {e}")))?;
            let snapshot_id = match iceberg.commit(staged.clone(), offsets).await {
                Ok(snapshot_id) => snapshot_id,
                Err(e) => {
                    let _ = iceberg.abort(staged).await;
                    return Err(ConnectorError::Cdc(format!("iceberg commit failed: {e}")));
                }
            };
            source.commit_offsets()?;
            committed_snapshots.push(snapshot_id);
            if max_commits.is_some_and(|max| committed_snapshots.len() >= max) {
                break;
            }
        }

        Ok(committed_snapshots)
    }

    /// Run the pipeline against a live Kafka cluster.
    ///
    /// This convenience method intentionally fails closed because it has no
    /// durable sink argument. Use [`run_live_kafka_with_iceberg_sink`] or
    /// [`run_with_source`] with a sink callback that durably commits the batch
    /// before returning.
    pub async fn run(&self) -> Result<(), ConnectorError> {
        self.validate()?;
        Err(ConnectorError::Cdc(
            "CdcToLakehousePipeline::run() is disabled because it cannot prove downstream \
             durability; use run_live_kafka_with_iceberg_sink or run_with_source with a durable \
             sink callback"
                .into(),
        ))
    }

    /// Run CDC ingestion from live Kafka into an Iceberg two-phase sink.
    ///
    /// Source offsets are committed only after the Iceberg snapshot commit
    /// succeeds and embeds the consumed offsets in snapshot metadata.
    #[cfg(feature = "kafka")]
    pub async fn run_live_kafka_with_iceberg_sink<I>(
        &self,
        iceberg: &I,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Vec<i64>, ConnectorError>
    where
        I: crate::lakehouse::IcebergTwoPhaseCommit,
    {
        self.validate()?;
        let config = KafkaCdcConfig::new(
            self.kafka_brokers.join(","),
            format!("krishiv-cdc-{}", self.iceberg_table),
            self.source_topic.clone(),
        );
        let source = RdkafkaCdcEventSource::new(&config)?;
        self.run_with_iceberg_sink(source, iceberg, shutdown).await
    }
}

fn kafka_offsets_for_events(events: &[CdcEvent]) -> BTreeMap<String, i64> {
    let mut offsets = BTreeMap::new();
    for event in events {
        let key = format!("{}-{}", event.table, event.partition_id);
        let next_offset = event.offset + 1;
        // H5: take the max offset per partition so non-ascending event order
        // (e.g. from merged multi-table batches) never commits a stale offset.
        offsets
            .entry(key)
            .and_modify(|v: &mut i64| *v = (*v).max(next_offset))
            .or_insert(next_offset);
    }
    offsets
}

#[derive(Debug, Default)]
struct CdcSchemaEvolutionState {
    schema: Option<Arc<Schema>>,
}

impl CdcSchemaEvolutionState {
    fn normalize(&mut self, batch: RecordBatch) -> Result<RecordBatch, String> {
        let merged = match &self.schema {
            Some(existing) => merge_compatible_schemas(existing, &batch.schema())?,
            None => {
                validate_unique_schema_fields(&batch.schema())?;
                batch.schema()
            }
        };
        let normalized = crate::schema_normalize::SchemaNormalizeOperator::new(merged.clone())
            .normalize(&batch)
            .map_err(|e| e.to_string())?;
        self.schema = Some(merged);
        Ok(normalized)
    }
}

#[cfg(any(feature = "schema-registry", test))]
fn concat_registry_batches(batches: &[RecordBatch]) -> Result<RecordBatch, String> {
    let Some(first) = batches.first() else {
        return Err("cannot concatenate an empty schema-registry batch list".into());
    };
    let mut target = first.schema();
    validate_unique_schema_fields(&target)?;
    for batch in &batches[1..] {
        target = merge_compatible_schemas(&target, &batch.schema())?;
    }
    let normalizer = crate::schema_normalize::SchemaNormalizeOperator::new(target.clone());
    let normalized = batches
        .iter()
        .map(|batch| {
            normalizer
                .normalize(batch)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    arrow::compute::concat_batches(&target, &normalized)
        .map_err(|error| format!("schema-registry batch concatenation failed: {error}"))
}

fn merge_compatible_schemas(left: &Schema, right: &Schema) -> Result<Arc<Schema>, String> {
    validate_unique_schema_fields(left)?;
    validate_unique_schema_fields(right)?;

    let right_by_name = right
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.as_ref()))
        .collect::<std::collections::HashMap<_, _>>();
    let left_names = left
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut fields = Vec::with_capacity(left.fields().len() + right.fields().len());

    for left_field in left.fields() {
        let field = if let Some(right_field) = right_by_name.get(left_field.name().as_str()) {
            merge_compatible_field(left_field, right_field)?
        } else {
            arrow::datatypes::Field::new(left_field.name(), left_field.data_type().clone(), true)
                .with_metadata(left_field.metadata().clone())
        };
        fields.push(field);
    }
    for right_field in right.fields() {
        if !left_names.contains(right_field.name().as_str()) {
            fields.push(
                arrow::datatypes::Field::new(
                    right_field.name(),
                    right_field.data_type().clone(),
                    true,
                )
                .with_metadata(right_field.metadata().clone()),
            );
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn validate_unique_schema_fields(schema: &Schema) -> Result<(), String> {
    let mut names = std::collections::HashSet::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if !names.insert(field.name()) {
            return Err(format!("duplicate Arrow field '{}'", field.name()));
        }
    }
    Ok(())
}

fn merge_compatible_field(
    left: &arrow::datatypes::Field,
    right: &arrow::datatypes::Field,
) -> Result<arrow::datatypes::Field, String> {
    if left.metadata() != right.metadata() {
        return Err(format!(
            "field '{}' changed metadata across schema versions",
            left.name()
        ));
    }
    let data_type = compatible_data_type(left.data_type(), right.data_type()).ok_or_else(|| {
        format!(
            "field '{}' changed incompatibly from {:?} to {:?}",
            left.name(),
            left.data_type(),
            right.data_type()
        )
    })?;
    Ok(arrow::datatypes::Field::new(
        left.name(),
        data_type,
        left.is_nullable() || right.is_nullable(),
    )
    .with_metadata(left.metadata().clone()))
}

fn compatible_data_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if left == &DataType::Null {
        return Some(right.clone());
    }
    if right == &DataType::Null {
        return Some(left.clone());
    }
    crate::schema_normalize::SchemaNormalizeOperator::widening_target(left, right)
}

// ---------------------------------------------------------------------------
// Columnar batch builder for multiple CDC events  (P1.18 / P1.19)
// ---------------------------------------------------------------------------

/// An error type for CDC batch building.
#[derive(Debug, thiserror::Error)]
#[error("CDC batch error: {0}")]
pub struct CdcBatchError(pub String);

/// Build a single columnar Arrow `RecordBatch` from a slice of `CdcEvent`s.
///
/// All events must share the same column names in their `after` payload (or
/// `before` payload for deletes). Column names are derived from the union of
/// keys across all events, sorted alphabetically (P1.19), with missing values
/// represented as `null`.
///
/// Returns an error if `events` is empty or if batch construction fails.
pub fn build_batch_from_events(events: &[CdcEvent]) -> Result<RecordBatch, CdcBatchError> {
    if events.is_empty() {
        return Err(CdcBatchError("events slice is empty".into()));
    }

    // Collect the union of all column names from after/before payloads, sorted.
    let mut col_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for event in events {
        let payload = event.after.as_ref().or(event.before.as_ref());
        if let Some(batch) = payload {
            for field in batch.schema().fields() {
                col_names.insert(safe_payload_column(field.name()));
            }
        }
    }

    // Add metadata columns present on every row.
    let meta_cols = ["_op", "_table", "_lsn", "_ts_ms", "_partition", "_offset"];
    for mc in &meta_cols {
        col_names.insert(mc.to_string());
    }

    let col_names: Vec<String> = col_names.into_iter().collect(); // already sorted via BTreeSet

    // Build one string column per name.
    let mut column_data: Vec<Vec<Option<String>>> = vec![Vec::new(); col_names.len()];

    for event in events {
        let payload = event.after.as_ref().or(event.before.as_ref());

        for (col_idx, col_name) in col_names.iter().enumerate() {
            let value: Option<String> = match col_name.as_str() {
                "_op" => Some(format!("{:?}", event.op)),
                "_table" => Some(event.table.clone()),
                "_lsn" => event.source_lsn.map(|v| v.to_string()),
                "_ts_ms" => event.source_ts_ms.map(|v| v.to_string()),
                "_partition" => Some(event.partition_id.to_string()),
                "_offset" => Some(event.offset.to_string()),
                user_col => {
                    // Reverse-map safe name back to original payload field name.
                    let orig_col = if user_col.ends_with("_src")
                        && RESERVED_CDC_COLUMNS.contains(&&user_col[..user_col.len() - 4])
                    {
                        &user_col[..user_col.len() - 4]
                    } else {
                        user_col
                    };
                    match payload {
                        Some(batch) => payload_value_to_string(batch, orig_col)?,
                        None => None,
                    }
                }
            };
            column_data[col_idx].push(value);
        }
    }

    // Build Arrow arrays and schema.
    let fields: Vec<Field> = col_names
        .iter()
        .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let columns: Vec<Arc<dyn arrow::array::Array>> = column_data
        .into_iter()
        .map(|vals| {
            let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
            Arc::new(arr) as Arc<dyn arrow::array::Array>
        })
        .collect();

    RecordBatch::try_new(schema, columns).map_err(|e| CdcBatchError(e.to_string()))
}

fn payload_value_to_string(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Option<String>, CdcBatchError> {
    use arrow::array::Array;
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let Ok(idx) = batch.schema().index_of(column_name) else {
        return Ok(None);
    };
    let col = batch.column(idx);
    if col.is_null(0) {
        return Ok(None);
    }
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(col.as_ref(), &options)
        .map_err(|e| CdcBatchError(e.to_string()))?;
    Ok(Some(formatter.value(0).to_string()))
}

/// Metadata column names injected by `build_batch_from_events`.
///
/// Source payload fields that collide with any of these names are renamed by
/// appending `_src` so that metadata values are never silently overwritten.
const RESERVED_CDC_COLUMNS: &[&str] = &["_op", "_table", "_lsn", "_ts_ms", "_partition", "_offset"];

/// Map a source payload column name to a safe output name.
///
/// If the name collides with a reserved CDC metadata column it is renamed to
/// `{name}_src`; otherwise the name is used as-is.
fn safe_payload_column(name: &str) -> String {
    if RESERVED_CDC_COLUMNS.contains(&name) {
        format!("{name}_src")
    } else {
        name.to_string()
    }
}

// ---------------------------------------------------------------------------
// RdkafkaCdcEventSource  (behind `kafka` feature)
// ---------------------------------------------------------------------------

