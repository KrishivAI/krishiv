#![forbid(unsafe_code)]
//! Iceberg-backed lakehouse capabilities for Krishiv R8.2.
//!
//! This crate provides snapshot reads, schema evolution support, and optimistic
//! concurrency control for multi-writer Iceberg table access.

use arrow::record_batch::RecordBatch;

mod as_of;
pub mod connector_registry;
mod delta;
mod delta_lake;
#[cfg(feature = "iceberg")]
pub mod dml;
mod hudi;
mod iceberg_fs;
mod iceberg_native;
mod local_delta;
#[cfg(feature = "iceberg")]
pub mod maintenance;
mod partition_spec;
mod two_phase;

pub use as_of::AsOfSpec;
#[cfg(feature = "kafka")]
pub use delta::RdkafkaDeltaStore;
pub use delta::{
    DeltaEntry, DeltaOp, DeltaStore, KafkaDeltaStore, MemoryDeltaStore, RedbDeltaStore,
};
pub use delta_lake::DeltaObjectStoreReader;
pub use delta_lake::{
    DeltaTableHandle, DeltaWriteMode, MergeDeltaResult, merge_delta, remove_merge_key_column,
    write_delta,
};
pub use hudi::{
    HudiCowWriter, HudiObjectStoreReader, HudiObjectStoreWriter, HudiQueryType, HudiSnapshotReader,
    HudiStageHandle, HudiTwoPhaseCommitSink, HudiWriteResult, ensure_hoodie_properties,
    read_hoodie_properties, vacuum_hudi_table, write_hudi_cow_append,
    write_hudi_cow_fixture, write_hudi_cow_upsert,
};
pub use iceberg_fs::IcebergFsTable;
#[cfg(feature = "iceberg")]
pub use iceberg_native::IcebergNativeTwoPhaseCommit;
pub use local_delta::{
    DeltaStageHandle, LocalDeltaTwoPhaseCommitSink, read_table_at_timestamp, vacuum_table,
};
pub use partition_spec::{PartitionField, PartitionSpecResolver, PartitionSpecVersion};
pub use two_phase::{
    DistributedIcebergCommitCoordinator, IcebergTwoPhaseCommit, KAFKA_OFFSETS_SUMMARY_KEY,
    MemoryIcebergTwoPhaseCommit, StagedSnapshot, kafka_offsets_json, parse_kafka_offsets_json,
};

// ---------------------------------------------------------------------------
// LakehouseError
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
#[derive(Debug, thiserror::Error)]
pub enum LakehouseError {
    #[doc = "**Beta API**: may change between minor releases."]
    #[error("Iceberg error: {0}")]
    Iceberg(String),
    #[doc = "**Beta API**: may change between minor releases."]
    #[error("Table not found: {table}")]
    NotFound { table: String },
    #[doc = "**Beta API**: may change between minor releases."]
    #[error("Schema conflict: {message}")]
    SchemaConflict { message: String },
    #[doc = "**Beta API**: may change between minor releases."]
    #[error("I/O error: {0}")]
    Io(String),
    #[doc = "**Beta API**: may change between minor releases."]
    #[error("Concurrency conflict: {message}")]
    Concurrency { message: String },
}

/// Convenience result alias for lakehouse operations.
pub type LakehouseResult<T> = Result<T, LakehouseError>;

#[cfg(feature = "iceberg")]
impl From<iceberg::Error> for LakehouseError {
    fn from(e: iceberg::Error) -> Self {
        LakehouseError::Iceberg(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// IcebergTableRef
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IcebergTableRef {
    #[doc = "**Beta API**: may change between minor releases."]
    pub catalog: String,
    #[doc = "**Beta API**: may change between minor releases."]
    pub namespace: String,
    #[doc = "**Beta API**: may change between minor releases."]
    pub name: String,
}

impl IcebergTableRef {
    #[doc = "**Beta API**: may change between minor releases."]
    pub fn new(
        catalog: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    #[doc = "**Beta API**: may change between minor releases."]
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.name)
    }
}

// ---------------------------------------------------------------------------
// IcebergScanOptions
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
#[derive(Debug, Clone, Default)]
pub struct IcebergScanOptions {
    #[doc = "**Beta API**: may change between minor releases."]
    pub snapshot_id: Option<i64>,
    #[doc = "**Beta API**: may change between minor releases."]
    pub columns: Option<Vec<String>>,
    #[doc = "**Beta API**: may change between minor releases."]
    pub row_limit: Option<u64>,
}

impl IcebergScanOptions {
    #[doc = "**Beta API**: may change between minor releases."]
    pub fn new() -> Self {
        Self::default()
    }

    #[doc = "**Beta API**: may change between minor releases."]
    pub fn with_snapshot(mut self, id: i64) -> Self {
        self.snapshot_id = Some(id);
        self
    }

    #[doc = "**Beta API**: may change between minor releases."]
    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.columns = Some(cols);
        self
    }

    #[doc = "**Beta API**: may change between minor releases."]
    pub fn with_row_limit(mut self, limit: u64) -> Self {
        self.row_limit = Some(limit);
        self
    }
}

// ---------------------------------------------------------------------------
// SchemaVersion / SchemaField
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    #[doc = "**Beta API**: may change between minor releases."]
    pub schema_id: i32,
    #[doc = "**Beta API**: may change between minor releases."]
    pub fields: Vec<SchemaField>,
}

#[doc = "**Beta API**: may change between minor releases."]
#[derive(Debug, Clone)]
pub struct SchemaField {
    #[doc = "**Beta API**: may change between minor releases."]
    pub id: i32,
    #[doc = "**Beta API**: may change between minor releases."]
    pub name: String,
    #[doc = "**Beta API**: may change between minor releases."]
    pub required: bool,
    #[doc = "**Beta API**: may change between minor releases."]
    pub data_type: String,
}

/// Iceberg named-reference kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergReferenceKind {
    Branch,
    Tag,
}

/// Named branch or tag pinned to a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergReference {
    pub name: String,
    pub snapshot_id: i64,
    pub kind: IcebergReferenceKind,
}

/// Scalar values supported by row-level Iceberg mutations.
#[derive(Debug, Clone, PartialEq)]
pub enum LakehouseValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Null,
}

/// Equality predicate used by row-level delete and update operations.
#[derive(Debug, Clone, PartialEq)]
pub struct LakehousePredicate {
    pub column: String,
    pub equals: LakehouseValue,
}

/// Constant assignment used by row-level updates.
#[derive(Debug, Clone, PartialEq)]
pub struct LakehouseAssignment {
    pub column: String,
    pub value: LakehouseValue,
}

// ---------------------------------------------------------------------------
// LakehouseTable trait
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
#[async_trait::async_trait]
pub trait LakehouseTable: Send + Sync {
    #[doc = "**Beta API**: may change between minor releases."]
    fn table_ref(&self) -> &IcebergTableRef;

    #[doc = "**Beta API**: may change between minor releases."]
    async fn schema(&self) -> Result<SchemaVersion, LakehouseError>;

    #[doc = "**Beta API**: may change between minor releases."]
    async fn scan(&self, opts: &IcebergScanOptions) -> Result<Vec<RecordBatch>, LakehouseError>;

    #[doc = "**Beta API**: may change between minor releases."]
    async fn append(&self, batches: Vec<RecordBatch>) -> Result<(), LakehouseError>;

    /// Atomically replace the visible table contents with `batches`.
    async fn overwrite(&self, _batches: Vec<RecordBatch>) -> Result<(), LakehouseError> {
        Err(LakehouseError::Iceberg(
            "overwrite is not implemented by this table backend".into(),
        ))
    }

    async fn delete_where(&self, _predicate: &LakehousePredicate) -> Result<(), LakehouseError> {
        Err(LakehouseError::Iceberg(
            "row-level delete is not implemented by this table backend".into(),
        ))
    }

    async fn update_where(
        &self,
        _predicate: &LakehousePredicate,
        _assignments: &[LakehouseAssignment],
    ) -> Result<(), LakehouseError> {
        Err(LakehouseError::Iceberg(
            "row-level update is not implemented by this table backend".into(),
        ))
    }

    async fn merge(
        &self,
        _batches: Vec<RecordBatch>,
        _key_columns: &[String],
    ) -> Result<(), LakehouseError> {
        Err(LakehouseError::Iceberg(
            "merge is not implemented by this table backend".into(),
        ))
    }

    async fn evolve_schema(&self, _schema: SchemaVersion) -> Result<(), LakehouseError> {
        Err(LakehouseError::SchemaConflict {
            message: "schema evolution is not implemented by this table backend".into(),
        })
    }

    #[doc = "**Beta API**: may change between minor releases."]
    async fn current_snapshot_id(&self) -> Result<Option<i64>, LakehouseError>;
}

// ---------------------------------------------------------------------------
// MemoryLakehouseTable
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SnapshotLayer {
    snapshot_id: i64,
    batches: Vec<RecordBatch>,
    replace_all: bool,
}

#[derive(Debug, Default)]
struct MemoryLakehouseTableState {
    /// Batches committed per snapshot, in commit order.
    layers: Vec<SnapshotLayer>,
    /// Last assigned snapshot id; `0` means no snapshots yet.
    last_snapshot_id: i64,
    /// When set, compact the oldest layers into one whenever `layers.len()` would
    /// exceed this limit. Prevents unbounded memory growth in streaming-write workloads.
    max_snapshot_layers: Option<usize>,
}

impl MemoryLakehouseTableState {
    fn current_snapshot_id(&self) -> Option<i64> {
        if self.last_snapshot_id == 0 {
            None
        } else {
            Some(self.last_snapshot_id)
        }
    }

    fn batches_up_to_snapshot(&self, snapshot_id: i64) -> Vec<RecordBatch> {
        let eligible = self
            .layers
            .iter()
            .filter(|layer| layer.snapshot_id <= snapshot_id)
            .collect::<Vec<_>>();
        let start = eligible
            .iter()
            .rposition(|layer| layer.replace_all)
            .unwrap_or(0);
        eligible[start..]
            .iter()
            .flat_map(|layer| layer.batches.iter().cloned())
            .collect()
    }

    fn all_batches(&self) -> Vec<RecordBatch> {
        self.current_snapshot_id()
            .map(|snapshot| self.batches_up_to_snapshot(snapshot))
            .unwrap_or_default()
    }

    fn append_layer(&mut self, batches: Vec<RecordBatch>) -> i64 {
        self.last_snapshot_id += 1;
        let snapshot_id = self.last_snapshot_id;
        self.layers.push(SnapshotLayer {
            snapshot_id,
            batches,
            replace_all: false,
        });
        self.maybe_compact();
        snapshot_id
    }

    fn replace_layer(&mut self, batches: Vec<RecordBatch>) -> i64 {
        self.last_snapshot_id += 1;
        let snapshot_id = self.last_snapshot_id;
        self.layers.push(SnapshotLayer {
            snapshot_id,
            batches,
            replace_all: true,
        });
        self.maybe_compact();
        snapshot_id
    }

    /// Compact oldest snapshot layers into one when `max_snapshot_layers` is set
    /// and the current layer count exceeds it. Preserves all data; only changes
    /// the granularity of snapshot boundaries (time-travel before the merge point
    /// will resolve to the merged snapshot).
    fn maybe_compact(&mut self) {
        let Some(max) = self.max_snapshot_layers else {
            return;
        };
        if self.layers.len() <= max {
            return;
        }
        // Compact the currently visible state into one replacement snapshot.
        // Historical snapshots older than the compaction boundary are intentionally
        // expired, matching Iceberg snapshot expiration semantics.
        let merged_batches = self.all_batches();
        let compacted_snapshot_id = self.last_snapshot_id;
        self.layers.clear();
        self.layers.push(SnapshotLayer {
            snapshot_id: compacted_snapshot_id,
            batches: merged_batches,
            replace_all: true,
        });
    }
}

#[doc = "**Beta API**: may change between minor releases."]
pub struct MemoryLakehouseTable {
    table_ref: IcebergTableRef,
    schema_version: tokio::sync::RwLock<SchemaVersion>,
    state: tokio::sync::Mutex<MemoryLakehouseTableState>,
    /// Active partition spec; mutations via `add_partition_field` / `drop_partition_field`
    /// affect which partition path is computed on the next `append`.
    partition_spec: tokio::sync::Mutex<PartitionSpecResolver>,
    references: tokio::sync::RwLock<std::collections::BTreeMap<String, IcebergReference>>,
}

impl MemoryLakehouseTable {
    #[doc = "**Beta API**: may change between minor releases."]
    pub fn new(table_ref: IcebergTableRef, schema_version: SchemaVersion) -> Self {
        Self::with_compaction_limit(table_ref, schema_version, None)
    }

    /// Construct a [`MemoryLakehouseTable`] with a snapshot-layer compaction limit.
    ///
    /// Streaming-write workloads should pass `Some(max)` to prevent unbounded
    /// memory growth from continuous appends; tests and one-shot batch writes
    /// can pass `None` (same as [`Self::new`]).
    pub fn with_compaction_limit(
        table_ref: IcebergTableRef,
        schema_version: SchemaVersion,
        max_snapshot_layers: Option<usize>,
    ) -> Self {
        let state = MemoryLakehouseTableState {
            max_snapshot_layers,
            ..MemoryLakehouseTableState::default()
        };
        Self {
            table_ref,
            schema_version: tokio::sync::RwLock::new(schema_version),
            state: tokio::sync::Mutex::new(state),
            partition_spec: tokio::sync::Mutex::new(PartitionSpecResolver::new(0)),
            references: tokio::sync::RwLock::new(std::collections::BTreeMap::new()),
        }
    }

    /// Set the maximum number of snapshot layers retained before compaction.
    ///
    /// When the number of layers would exceed `max` after an append, the oldest
    /// layers are merged into one to keep memory bounded. Use this for
    /// streaming-write workloads where continuous appends would otherwise cause
    /// unbounded snapshot accumulation.
    pub async fn with_max_snapshot_layers(self, max: usize) -> Self {
        self.state.lock().await.max_snapshot_layers = Some(max);
        self
    }

    /// Add a partition field to the active spec (spec evolution).
    ///
    /// The change takes effect immediately: the next call to `append` will
    /// compute partition paths using the updated field list.
    #[doc = "**Beta API**: may change between minor releases."]
    pub async fn add_partition_field(&self, field: PartitionField) {
        self.partition_spec.lock().await.add_field(field);
    }

    /// Drop a partition field by name from the active spec (spec evolution).
    ///
    /// The change takes effect immediately: the next call to `append` will
    /// compute partition paths without the dropped field.
    #[doc = "**Beta API**: may change between minor releases."]
    pub async fn drop_partition_field(&self, field_name: &str) {
        self.partition_spec.lock().await.drop_field(field_name);
    }

    /// Return a snapshot of the currently active partition fields.
    #[doc = "**Beta API**: may change between minor releases."]
    pub async fn active_partition_fields(&self) -> Vec<PartitionField> {
        self.partition_spec.lock().await.active_fields().to_vec()
    }

    pub async fn create_reference(
        &self,
        name: impl Into<String>,
        snapshot_id: i64,
        kind: IcebergReferenceKind,
    ) -> Result<(), LakehouseError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(LakehouseError::Iceberg(
                "reference name must be non-empty".into(),
            ));
        }
        if !self
            .state
            .lock()
            .await
            .layers
            .iter()
            .any(|layer| layer.snapshot_id == snapshot_id)
        {
            return Err(LakehouseError::NotFound {
                table: format!("snapshot {snapshot_id}"),
            });
        }
        self.references.write().await.insert(
            name.clone(),
            IcebergReference {
                name,
                snapshot_id,
                kind,
            },
        );
        Ok(())
    }

    pub async fn reference(&self, name: &str) -> Option<IcebergReference> {
        self.references.read().await.get(name).cloned()
    }

    /// Atomically verify the guard's expected snapshot and append batches.
    ///
    /// Holds the table mutex across the precondition check and snapshot commit so
    /// concurrent writers cannot both pass the check (P1-11).
    #[doc = "**Beta API**: may change between minor releases."]
    pub async fn check_and_append(
        &self,
        guard: &MultiWriterGuard,
        batches: Vec<RecordBatch>,
    ) -> Result<i64, LakehouseError> {
        let mut state = self.state.lock().await;
        if state.current_snapshot_id() != guard.expected_snapshot() {
            return Err(LakehouseError::Concurrency {
                message: format!(
                    "writer '{}' expected snapshot {:?} but found {:?}",
                    guard.writer_id(),
                    guard.expected_snapshot(),
                    state.current_snapshot_id(),
                ),
            });
        }
        Ok(state.append_layer(batches))
    }
}

#[async_trait::async_trait]
impl LakehouseTable for MemoryLakehouseTable {
    fn table_ref(&self) -> &IcebergTableRef {
        &self.table_ref
    }

    async fn schema(&self) -> Result<SchemaVersion, LakehouseError> {
        Ok(self.schema_version.read().await.clone())
    }

    async fn scan(&self, opts: &IcebergScanOptions) -> Result<Vec<RecordBatch>, LakehouseError> {
        let state = self.state.lock().await;
        let batches = if let Some(target) = opts.snapshot_id {
            state.batches_up_to_snapshot(target)
        } else {
            state.all_batches()
        };

        // Filter columns if requested
        let filtered: Vec<RecordBatch> = if let Some(cols) = &opts.columns {
            batches
                .iter()
                .map(|batch| {
                    let schema = batch.schema();
                    let indices: Vec<usize> = cols
                        .iter()
                        .filter_map(|col_name| schema.index_of(col_name).ok())
                        .collect();

                    batch
                        .project(&indices)
                        .map_err(|e| LakehouseError::Io(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            batches.clone()
        };

        // Apply row_limit across all batches
        if let Some(limit) = opts.row_limit {
            let limit = limit as usize;
            let mut result = Vec::new();
            let mut remaining = limit;
            for batch in filtered {
                if remaining == 0 {
                    break;
                }
                if batch.num_rows() <= remaining {
                    remaining -= batch.num_rows();
                    result.push(batch);
                } else {
                    result.push(batch.slice(0, remaining));
                    remaining = 0;
                }
            }
            Ok(result)
        } else {
            Ok(filtered)
        }
    }

    /// **Alpha**: Appends record batches to the in-memory delta log.
    ///
    /// **Not transactional**: If the process crashes between mutex acquire and
    /// commit, in-flight data is lost. No WAL or S3 multipart staging is used.
    ///
    /// **Partition path note**: Partition paths are computed and logged at write
    /// time for observability, but are not used to route data to external storage.
    /// In a production implementation this path would target object storage.
    async fn append(&self, batches: Vec<RecordBatch>) -> Result<(), LakehouseError> {
        // Compute the partition path from the active spec before committing.
        // In a full implementation this path would be used when writing data files to
        // object storage.  Here we log it so callers can observe which partition
        // fields are active at write time.
        {
            let spec = self.partition_spec.lock().await;
            let active = spec.active_fields();
            if active.is_empty() {
                tracing::debug!(
                    "table '{}': appending unpartitioned data ({} batch(es))",
                    self.table_ref.full_name(),
                    batches.len(),
                );
            } else {
                let partition_path: String = active
                    .iter()
                    .map(|f| format!("{}={}", f.name, f.transform))
                    .collect::<Vec<_>>()
                    .join("/");
                tracing::debug!(
                    "table '{}': appending to partition path '{}' ({} batch(es))",
                    self.table_ref.full_name(),
                    partition_path,
                    batches.len(),
                );
            }
        }
        let mut state = self.state.lock().await;
        state.append_layer(batches);
        Ok(())
    }

    async fn overwrite(&self, batches: Vec<RecordBatch>) -> Result<(), LakehouseError> {
        self.state.lock().await.replace_layer(batches);
        Ok(())
    }

    async fn delete_where(&self, predicate: &LakehousePredicate) -> Result<(), LakehouseError> {
        let mut guard = self.state.lock().await;
        let current = guard.all_batches();
        let rewritten = current
            .iter()
            .map(|batch| filter_mutation_rows(batch, predicate, false))
            .collect::<Result<Vec<_>, _>>()?;
        guard.replace_layer(rewritten);
        Ok(())
    }

    async fn update_where(
        &self,
        predicate: &LakehousePredicate,
        assignments: &[LakehouseAssignment],
    ) -> Result<(), LakehouseError> {
        let mut guard = self.state.lock().await;
        let current = guard.all_batches();
        let rewritten = current
            .iter()
            .map(|batch| update_mutation_rows(batch, predicate, assignments))
            .collect::<Result<Vec<_>, _>>()?;
        guard.replace_layer(rewritten);
        Ok(())
    }

    async fn merge(
        &self,
        batches: Vec<RecordBatch>,
        key_columns: &[String],
    ) -> Result<(), LakehouseError> {
        if key_columns.is_empty() {
            return Err(LakehouseError::SchemaConflict {
                message: "merge requires at least one key column".into(),
            });
        }
        let incoming_keys = collect_keys(&batches, key_columns)?;
        let mut guard = self.state.lock().await;
        let current = guard.all_batches();
        let mut rewritten = current
            .iter()
            .map(|batch| filter_keys(batch, key_columns, &incoming_keys))
            .collect::<Result<Vec<_>, _>>()?;
        rewritten.extend(batches);
        guard.replace_layer(rewritten);
        Ok(())
    }

    async fn evolve_schema(&self, schema: SchemaVersion) -> Result<(), LakehouseError> {
        let current = self.schema_version.read().await;
        for field in &current.fields {
            let replacement = schema
                .fields
                .iter()
                .find(|candidate| candidate.id == field.id)
                .ok_or_else(|| LakehouseError::SchemaConflict {
                    message: format!("schema evolution cannot remove field '{}'", field.name),
                })?;
            if replacement.name != field.name
                || replacement.data_type != field.data_type
                || (!field.required && replacement.required)
            {
                return Err(LakehouseError::SchemaConflict {
                    message: format!("incompatible evolution for field '{}'", field.name),
                });
            }
        }
        drop(current);
        *self.schema_version.write().await = schema;
        Ok(())
    }

    async fn current_snapshot_id(&self) -> Result<Option<i64>, LakehouseError> {
        Ok(self.state.lock().await.current_snapshot_id())
    }
}

fn predicate_matches(
    batch: &RecordBatch,
    row: usize,
    predicate: &LakehousePredicate,
) -> Result<bool, LakehouseError> {
    use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    let index =
        batch
            .schema()
            .index_of(&predicate.column)
            .map_err(|_| LakehouseError::SchemaConflict {
                message: format!("unknown mutation column '{}'", predicate.column),
            })?;
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(matches!(predicate.equals, LakehouseValue::Null));
    }
    Ok(match &predicate.equals {
        LakehouseValue::Boolean(value) => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .is_some_and(|array| array.value(row) == *value),
        LakehouseValue::Int32(value) => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .is_some_and(|array| array.value(row) == *value),
        LakehouseValue::Int64(value) => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .is_some_and(|array| array.value(row) == *value),
        LakehouseValue::Float64(value) => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .is_some_and(|array| array.value(row) == *value),
        LakehouseValue::Utf8(value) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|array| array.value(row) == value),
        LakehouseValue::Null => false,
    })
}

fn filter_mutation_rows(
    batch: &RecordBatch,
    predicate: &LakehousePredicate,
    retain_matches: bool,
) -> Result<RecordBatch, LakehouseError> {
    let mask = (0..batch.num_rows())
        .map(|row| {
            predicate_matches(batch, row, predicate).map(|matched| matched == retain_matches)
        })
        .collect::<Result<Vec<_>, _>>()?;
    arrow::compute::filter_record_batch(batch, &arrow::array::BooleanArray::from(mask))
        .map_err(|error| LakehouseError::Io(error.to_string()))
}

fn constant_array(
    value: &LakehouseValue,
    data_type: &arrow::datatypes::DataType,
    len: usize,
) -> Result<arrow::array::ArrayRef, LakehouseError> {
    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, new_null_array,
    };
    let array: arrow::array::ArrayRef = match (value, data_type) {
        (LakehouseValue::Null, data_type) => new_null_array(data_type, len),
        (LakehouseValue::Boolean(value), arrow::datatypes::DataType::Boolean) => {
            std::sync::Arc::new(BooleanArray::from(vec![Some(*value); len]))
        }
        (LakehouseValue::Int32(value), arrow::datatypes::DataType::Int32) => {
            std::sync::Arc::new(Int32Array::from(vec![Some(*value); len]))
        }
        (LakehouseValue::Int64(value), arrow::datatypes::DataType::Int64) => {
            std::sync::Arc::new(Int64Array::from(vec![Some(*value); len]))
        }
        (LakehouseValue::Float64(value), arrow::datatypes::DataType::Float64) => {
            std::sync::Arc::new(Float64Array::from(vec![Some(*value); len]))
        }
        (LakehouseValue::Utf8(value), arrow::datatypes::DataType::Utf8) => {
            std::sync::Arc::new(StringArray::from(vec![Some(value.as_str()); len]))
        }
        _ => {
            return Err(LakehouseError::SchemaConflict {
                message: format!("assignment value is incompatible with {data_type}"),
            });
        }
    };
    Ok(array)
}

fn update_mutation_rows(
    batch: &RecordBatch,
    predicate: &LakehousePredicate,
    assignments: &[LakehouseAssignment],
) -> Result<RecordBatch, LakehouseError> {
    let matches = (0..batch.num_rows())
        .map(|row| predicate_matches(batch, row, predicate))
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = batch.columns().to_vec();
    for assignment in assignments {
        let index = batch.schema().index_of(&assignment.column).map_err(|_| {
            LakehouseError::SchemaConflict {
                message: format!("unknown assignment column '{}'", assignment.column),
            }
        })?;
        let replacement = constant_array(
            &assignment.value,
            batch.schema().field(index).data_type(),
            batch.num_rows(),
        )?;
        let original_data = columns[index].to_data();
        let replacement_data = replacement.to_data();
        let mut mutable = arrow::array::MutableArrayData::new(
            vec![&original_data, &replacement_data],
            false,
            batch.num_rows(),
        );
        for (row, matched) in matches.iter().enumerate() {
            mutable.extend(usize::from(*matched), row, row + 1);
        }
        columns[index] = arrow::array::make_array(mutable.freeze());
    }
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|error| LakehouseError::Io(error.to_string()))
}

fn row_key(batch: &RecordBatch, row: usize, columns: &[String]) -> Result<String, LakehouseError> {
    columns
        .iter()
        .map(|column| {
            let index =
                batch
                    .schema()
                    .index_of(column)
                    .map_err(|_| LakehouseError::SchemaConflict {
                        message: format!("unknown merge key column '{column}'"),
                    })?;
            arrow::util::display::array_value_to_string(batch.column(index), row)
                .map_err(|error| LakehouseError::Io(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("\u{1f}"))
}

fn collect_keys(
    batches: &[RecordBatch],
    columns: &[String],
) -> Result<std::collections::BTreeSet<String>, LakehouseError> {
    let mut keys = std::collections::BTreeSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let key = row_key(batch, row, columns)?;
            if !keys.insert(key.clone()) {
                return Err(LakehouseError::Concurrency {
                    message: format!("merge source contains duplicate key '{key}'"),
                });
            }
        }
    }
    Ok(keys)
}

fn filter_keys(
    batch: &RecordBatch,
    columns: &[String],
    excluded: &std::collections::BTreeSet<String>,
) -> Result<RecordBatch, LakehouseError> {
    let mask = (0..batch.num_rows())
        .map(|row| row_key(batch, row, columns).map(|key| !excluded.contains(&key)))
        .collect::<Result<Vec<_>, _>>()?;
    arrow::compute::filter_record_batch(batch, &arrow::array::BooleanArray::from(mask))
        .map_err(|error| LakehouseError::Io(error.to_string()))
}

// ---------------------------------------------------------------------------
// MultiWriterGuard
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
pub struct MultiWriterGuard {
    expected_snapshot: Option<i64>,
    writer_id: String,
}

impl MultiWriterGuard {
    #[doc = "**Beta API**: may change between minor releases."]
    pub fn new(expected_snapshot: Option<i64>, writer_id: impl Into<String>) -> Self {
        Self {
            expected_snapshot,
            writer_id: writer_id.into(),
        }
    }

    #[doc = "**Beta API**: may change between minor releases."]
    pub fn expected_snapshot(&self) -> Option<i64> {
        self.expected_snapshot
    }

    #[doc = "**Beta API**: may change between minor releases."]
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }
}

/// Verify that the table's current snapshot matches the guard's expected snapshot.
/// Returns `Err(LakehouseError::Concurrency)` if they differ.
#[doc = "**Beta API**: may change between minor releases."]
pub async fn check_write_precondition(
    table: &dyn LakehouseTable,
    guard: &MultiWriterGuard,
) -> Result<(), LakehouseError> {
    let current = table.current_snapshot_id().await?;
    if current != guard.expected_snapshot {
        return Err(LakehouseError::Concurrency {
            message: format!(
                "writer '{}' expected snapshot {:?} but found {:?}",
                guard.writer_id(),
                guard.expected_snapshot(),
                current,
            ),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_schema_version() -> SchemaVersion {
        SchemaVersion {
            schema_id: 1,
            fields: vec![SchemaField {
                id: 1,
                name: "x".to_string(),
                required: true,
                data_type: "int64".to_string(),
            }],
        }
    }

    fn make_table_ref() -> IcebergTableRef {
        IcebergTableRef::new("my_catalog", "my_ns", "my_table")
    }

    fn make_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn table_ref_full_name() {
        let r = make_table_ref();
        assert_eq!(r.full_name(), "my_catalog.my_ns.my_table");
    }

    #[test]
    fn scan_options_builder_chain() {
        let opts = IcebergScanOptions::new()
            .with_snapshot(42)
            .with_columns(vec!["a".to_string(), "b".to_string()])
            .with_row_limit(100);
        assert_eq!(opts.snapshot_id, Some(42));
        assert_eq!(opts.columns, Some(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(opts.row_limit, Some(100));
    }

    #[test]
    fn memory_table_append_and_scan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = MemoryLakehouseTable::new(make_table_ref(), make_schema_version());

            let batch1 = make_batch(vec![1, 2, 3]);
            let batch2 = make_batch(vec![4, 5]);
            table.append(vec![batch1]).await.unwrap();
            table.append(vec![batch2]).await.unwrap();

            let opts = IcebergScanOptions::new();
            let result = table.scan(&opts).await.unwrap();
            let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 5);
        });
    }

    #[test]
    fn memory_table_snapshot_id_increments() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = MemoryLakehouseTable::new(make_table_ref(), make_schema_version());

            assert_eq!(table.current_snapshot_id().await.unwrap(), None);

            table.append(vec![make_batch(vec![1])]).await.unwrap();
            let snap1 = table.current_snapshot_id().await.unwrap();

            table.append(vec![make_batch(vec![2])]).await.unwrap();
            let snap2 = table.current_snapshot_id().await.unwrap();

            assert!(snap1.is_some());
            assert!(snap2.is_some());
            assert!(snap2.unwrap() > snap1.unwrap());
        });
    }

    #[test]
    fn memory_table_with_compaction_limit_keeps_data() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Compact on the first layer — every append beyond the first
            // merges the previous layer into one, so all rows are still
            // readable.
            let table = MemoryLakehouseTable::with_compaction_limit(
                make_table_ref(),
                make_schema_version(),
                Some(1),
            );

            for i in 0i64..20 {
                table.append(vec![make_batch(vec![i])]).await.unwrap();
            }

            let result = table.scan(&IcebergScanOptions::new()).await.unwrap();
            let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 20, "compaction must preserve all rows");
        });
    }

    #[test]
    fn scan_with_row_limit() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = MemoryLakehouseTable::new(make_table_ref(), make_schema_version());

            // Append 10 rows across two batches
            table
                .append(vec![make_batch(vec![1, 2, 3, 4, 5])])
                .await
                .unwrap();
            table
                .append(vec![make_batch(vec![6, 7, 8, 9, 10])])
                .await
                .unwrap();

            let opts = IcebergScanOptions::new().with_row_limit(5);
            let result = table.scan(&opts).await.unwrap();
            let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 5);
        });
    }

    #[test]
    fn optimistic_concurrency_conflict() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = Arc::new(MemoryLakehouseTable::new(
                make_table_ref(),
                make_schema_version(),
            ));

            // Both writers observe no prior snapshot
            let guard_a = MultiWriterGuard::new(None, "writer-a");
            let guard_b = MultiWriterGuard::new(None, "writer-b");

            // First writer commits atomically
            table
                .check_and_append(&guard_a, vec![make_batch(vec![1, 2])])
                .await
                .expect("writer-a precondition should pass");

            // Second writer now sees a stale snapshot expectation
            let err = table
                .check_and_append(&guard_b, vec![make_batch(vec![3])])
                .await
                .expect_err("writer-b should detect conflict");
            assert!(
                matches!(err, LakehouseError::Concurrency { .. }),
                "expected Concurrency error, got: {err}"
            );
        });
    }

    #[test]
    fn concurrent_append_no_data_duplication() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = Arc::new(MemoryLakehouseTable::new(
                make_table_ref(),
                make_schema_version(),
            ));
            let guard_a = MultiWriterGuard::new(None, "writer-a");
            let guard_b = MultiWriterGuard::new(None, "writer-b");

            let table_a = Arc::clone(&table);
            let table_b = Arc::clone(&table);

            let handle_a = tokio::spawn(async move {
                table_a
                    .check_and_append(&guard_a, vec![make_batch(vec![1])])
                    .await
            });
            let handle_b = tokio::spawn(async move {
                // Yield so writer-a can acquire the mutex first.
                tokio::task::yield_now().await;
                table_b
                    .check_and_append(&guard_b, vec![make_batch(vec![2])])
                    .await
            });

            let result_a = handle_a.await.unwrap();
            let result_b = handle_b.await.unwrap();
            let successes = [result_a.is_ok(), result_b.is_ok()]
                .into_iter()
                .filter(|ok| *ok)
                .count();
            assert_eq!(successes, 1, "exactly one concurrent writer may commit");

            let opts = IcebergScanOptions::new();
            let scanned = table.scan(&opts).await.unwrap();
            let total_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 1, "only one writer's batch must be visible");
        });
    }

    #[test]
    fn time_travel_returns_historical_snapshot() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = MemoryLakehouseTable::new(make_table_ref(), make_schema_version());

            table.append(vec![make_batch(vec![1, 2])]).await.unwrap();
            let snap1 = table.current_snapshot_id().await.unwrap().unwrap();
            table.append(vec![make_batch(vec![3, 4, 5])]).await.unwrap();
            let snap2 = table.current_snapshot_id().await.unwrap().unwrap();

            let at_snap1 = table
                .scan(&IcebergScanOptions::new().with_snapshot(snap1))
                .await
                .unwrap();
            let rows_snap1: usize = at_snap1.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows_snap1, 2);

            let at_snap2 = table
                .scan(&IcebergScanOptions::new().with_snapshot(snap2))
                .await
                .unwrap();
            let rows_snap2: usize = at_snap2.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows_snap2, 5);
        });
    }

    // -----------------------------------------------------------------------
    // Partition spec evolution tests
    // -----------------------------------------------------------------------

    #[test]
    fn partition_spec_evolution_add_and_drop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let table = MemoryLakehouseTable::new(make_table_ref(), make_schema_version());

            // Initially no partition fields are registered.
            assert!(
                table.active_partition_fields().await.is_empty(),
                "new table should have no active partition fields"
            );

            // Add two partition fields.
            table
                .add_partition_field(PartitionField {
                    name: "year".to_string(),
                    source_column: "event_time".to_string(),
                    transform: "year".to_string(),
                })
                .await;
            table
                .add_partition_field(PartitionField {
                    name: "month".to_string(),
                    source_column: "event_time".to_string(),
                    transform: "month".to_string(),
                })
                .await;

            {
                let fields = table.active_partition_fields().await;
                assert_eq!(fields.len(), 2, "expected two active partition fields");
                assert_eq!(fields[0].name, "year");
                assert_eq!(fields[1].name, "month");
            }

            // Drop the "month" field.
            table.drop_partition_field("month").await;

            {
                let fields = table.active_partition_fields().await;
                assert_eq!(
                    fields.len(),
                    1,
                    "dropping 'month' should leave exactly one field"
                );
                assert_eq!(fields[0].name, "year");
            }

            // Verify that an append still succeeds after evolution.
            table.append(vec![make_batch(vec![10, 20])]).await.unwrap();
            let snap = table.current_snapshot_id().await.unwrap();
            assert!(snap.is_some(), "snapshot should exist after append");
        });
    }

    #[test]
    fn lakehouse_error_display() {
        let variants = vec![
            LakehouseError::Iceberg("ice failure".to_string()),
            LakehouseError::NotFound {
                table: "ns.tbl".to_string(),
            },
            LakehouseError::SchemaConflict {
                message: "field removed".to_string(),
            },
            LakehouseError::Io("disk full".to_string()),
            LakehouseError::Concurrency {
                message: "stale snapshot".to_string(),
            },
        ];
        for v in &variants {
            let s = v.to_string();
            assert!(!s.is_empty(), "Display for {v:?} should not be empty");
        }
    }

    // ── MemoryLakehouseTable compaction ───────────────────────────────────────────

    #[tokio::test]
    async fn memory_iceberg_row_level_dml_and_schema_evolution() {
        let table = MemoryLakehouseTable::new(make_table_ref(), make_schema_version());
        table.append(vec![make_batch(vec![1, 2, 3])]).await.unwrap();
        table
            .update_where(
                &LakehousePredicate {
                    column: "x".into(),
                    equals: LakehouseValue::Int64(2),
                },
                &[LakehouseAssignment {
                    column: "x".into(),
                    value: LakehouseValue::Int64(20),
                }],
            )
            .await
            .unwrap();
        table
            .delete_where(&LakehousePredicate {
                column: "x".into(),
                equals: LakehouseValue::Int64(1),
            })
            .await
            .unwrap();
        table
            .merge(vec![make_batch(vec![20, 4])], &["x".into()])
            .await
            .unwrap();
        let rows: usize = table
            .scan(&IcebergScanOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 3);

        let mut evolved = make_schema_version();
        evolved.schema_id = 2;
        evolved.fields.push(SchemaField {
            id: 2,
            name: "note".into(),
            required: false,
            data_type: "string".into(),
        });
        table.evolve_schema(evolved.clone()).await.unwrap();
        assert_eq!(table.schema().await.unwrap().schema_id, 2);
    }

    #[tokio::test]
    async fn memory_table_compaction_preserves_all_rows() {
        let sv = make_schema_version();
        let tr = make_table_ref();
        let table = MemoryLakehouseTable::new(tr, sv)
            .with_max_snapshot_layers(10)
            .await;

        // Append 50 single-row snapshots — with max=10 this should compact aggressively.
        for i in 0i64..50 {
            table.append(vec![make_batch(vec![i])]).await.unwrap();
        }

        // All 50 rows must still be readable after compaction.
        let rows: usize = table
            .scan(&IcebergScanOptions::new())
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 50, "compaction must not lose any rows");

        // Layer count must be bounded to max.
        let layer_count = table.state.lock().await.layers.len();
        assert!(
            layer_count <= 10,
            "layer count must be ≤ max_snapshot_layers after compaction; got {layer_count}"
        );
    }

    #[tokio::test]
    async fn memory_table_no_compaction_without_max() {
        let sv = make_schema_version();
        let tr = make_table_ref();
        let table = MemoryLakehouseTable::new(tr, sv);

        for i in 0i64..20 {
            table.append(vec![make_batch(vec![i])]).await.unwrap();
        }

        // Without max_snapshot_layers, all 20 layers must exist.
        let layer_count = table.state.lock().await.layers.len();
        assert_eq!(
            layer_count, 20,
            "no compaction should occur without max_snapshot_layers"
        );
    }
}
