#![forbid(unsafe_code)]
//! **Beta crate**: Iceberg-backed lakehouse capabilities for Krishiv R8.2.
//!
//! This crate provides snapshot reads, schema evolution support, and optimistic
//! concurrency control for multi-writer Iceberg table access.

use std::fmt;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

mod as_of;
mod delta;
mod delta_lake;
mod hudi;
mod local_delta;
mod partition_spec;
mod two_phase;

pub use as_of::{AsOfSpec};
pub use delta::{DeltaEntry, DeltaOp, DeltaStore, KafkaDeltaStore, MemoryDeltaStore, RedbDeltaStore};
pub use hudi::{write_hudi_cow_fixture, HudiQueryType, HudiSnapshotReader};
pub use partition_spec::{PartitionSpecResolver, PartitionSpecVersion};
pub use delta_lake::{
    merge_delta, write_delta, DeltaTableHandle, DeltaWriteMode, MergeDeltaResult,
};
pub use two_phase::{
    IcebergTwoPhaseCommit, MemoryIcebergTwoPhaseCommit, StagedSnapshot, KAFKA_OFFSETS_SUMMARY_KEY,
    kafka_offsets_json, parse_kafka_offsets_json,
};
#[cfg(feature = "kafka")]
pub use delta::RdkafkaDeltaStore;

// ---------------------------------------------------------------------------
// LakehouseError
// ---------------------------------------------------------------------------

#[doc = "**Beta API**: may change between minor releases."]
#[derive(Debug)]
pub enum LakehouseError {
    #[doc = "**Beta API**: may change between minor releases."]
    Iceberg(String),
    #[doc = "**Beta API**: may change between minor releases."]
    NotFound { table: String },
    #[doc = "**Beta API**: may change between minor releases."]
    SchemaConflict { message: String },
    #[doc = "**Beta API**: may change between minor releases."]
    Io(String),
    #[doc = "**Beta API**: may change between minor releases."]
    Concurrency { message: String },
}

impl fmt::Display for LakehouseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LakehouseError::Iceberg(msg) => write!(f, "Iceberg error: {msg}"),
            LakehouseError::NotFound { table } => write!(f, "Table not found: {table}"),
            LakehouseError::SchemaConflict { message } => {
                write!(f, "Schema conflict: {message}")
            }
            LakehouseError::Io(msg) => write!(f, "I/O error: {msg}"),
            LakehouseError::Concurrency { message } => {
                write!(f, "Concurrency conflict: {message}")
            }
        }
    }
}

impl std::error::Error for LakehouseError {}

/// Convenience result alias for lakehouse operations.
pub type LakehouseResult<T> = Result<T, LakehouseError>;

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
}

#[derive(Debug, Default)]
struct MemoryLakehouseTableState {
    /// Batches committed per snapshot, in commit order.
    layers: Vec<SnapshotLayer>,
    /// Last assigned snapshot id; `0` means no snapshots yet.
    last_snapshot_id: i64,
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
        self.layers
            .iter()
            .filter(|layer| layer.snapshot_id <= snapshot_id)
            .flat_map(|layer| layer.batches.iter().cloned())
            .collect()
    }

    fn all_batches(&self) -> Vec<RecordBatch> {
        self.layers
            .iter()
            .flat_map(|layer| layer.batches.iter().cloned())
            .collect()
    }

    fn append_layer(&mut self, batches: Vec<RecordBatch>) -> i64 {
        self.last_snapshot_id += 1;
        let snapshot_id = self.last_snapshot_id;
        self.layers.push(SnapshotLayer {
            snapshot_id,
            batches,
        });
        snapshot_id
    }
}

#[doc = "**Beta API**: may change between minor releases."]
pub struct MemoryLakehouseTable {
    table_ref: IcebergTableRef,
    schema_version: SchemaVersion,
    state: tokio::sync::Mutex<MemoryLakehouseTableState>,
}

impl MemoryLakehouseTable {
    #[doc = "**Beta API**: may change between minor releases."]
    pub fn new(table_ref: IcebergTableRef, schema_version: SchemaVersion) -> Self {
        Self {
            table_ref,
            schema_version,
            state: tokio::sync::Mutex::new(MemoryLakehouseTableState::default()),
        }
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
        Ok(self.schema_version.clone())
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

                    let columns: Vec<Arc<dyn Array>> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<Field> =
                        indices.iter().map(|&i| schema.field(i).clone()).collect();
                    let new_schema = Arc::new(Schema::new(fields));

                    RecordBatch::try_new(new_schema, columns)
                        .expect("column selection should always produce a valid batch")
                })
                .collect()
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

    async fn append(&self, batches: Vec<RecordBatch>) -> Result<(), LakehouseError> {
        let mut state = self.state.lock().await;
        state.append_layer(batches);
        Ok(())
    }

    async fn current_snapshot_id(&self) -> Result<Option<i64>, LakehouseError> {
        Ok(self.state.lock().await.current_snapshot_id())
    }
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
}
