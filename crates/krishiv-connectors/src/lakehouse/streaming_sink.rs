//! G7: checkpoint-aligned streaming Iceberg sink.
//!
//! [`IcebergStreamingSink`] implements [`TransactionalSinkParticipant`] over an
//! [`IcebergNativeTwoPhaseCommit`] table, so continuous-cycle output flows
//! through the executor's `TwoPhaseSinkRegistry` and the G5 checkpoint
//! lifecycle:
//!
//! - `stage` — cycle output accumulates in the open transaction buffer;
//! - `pre_commit(epoch)` — the barrier durably stages the buffer as one
//!   Parquet file under `{root}/data/` BEFORE the checkpoint ack;
//! - `commit_through(epoch)` — the checkpoint-complete notification makes
//!   covered epochs visible as Iceberg snapshots;
//! - `abort_after(epoch)` — restore rolls back epochs past the checkpoint
//!   (their staged files are deleted; the rewound source re-delivers).
//!
//! Row-level semantics (`IcebergSinkMode`):
//! - **Append**: each committed epoch `fast_append`s its staged file — zero
//!   rewrite, one snapshot per epoch.
//! - **Upsert**: committed rows replace current rows with equal key columns;
//!   rows whose op column says `delete` remove matching keys. Implemented as
//!   copy-on-write (read current snapshot, filter, overwrite) because
//!   iceberg-rust 0.9.1 exposes no delete-file write API; merge-on-read
//!   equality deletes land with the 0.10 bump (#163).

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};
use crate::lakehouse::iceberg_native::IcebergNativeTwoPhaseCommit;
use crate::lakehouse::{LakehouseError, SchemaVersion};
use crate::two_phase::TransactionalSinkParticipant;

pub use krishiv_proto::IcebergSinkMode;

/// Marker values (case-insensitive) in the op column that delete a key.
const DELETE_OPS: [&str; 3] = ["d", "delete", "-"];

/// Target configuration for a streaming Iceberg sink.
#[derive(Debug, Clone)]
pub struct IcebergSinkTarget {
    /// Local table root directory (contains `data/` + `metadata/`).
    pub root: PathBuf,
    /// Iceberg table name inside the root.
    pub table: String,
    /// Row-level semantics applied at commit.
    pub mode: IcebergSinkMode,
    /// Key columns identifying a logical row (upsert mode).
    pub key_columns: Vec<String>,
    /// Optional column carrying per-row ops (`upsert` default, `delete`).
    pub op_column: Option<String>,
}

/// One epoch staged at a checkpoint barrier, retained until commit or abort.
struct PreparedEpoch {
    /// Staged Parquet paths + their Iceberg descriptors (append path).
    files: Vec<(PathBuf, iceberg::spec::DataFile)>,
    /// The staged rows (upsert path needs them for the merge).
    batches: Vec<RecordBatch>,
    /// Latest source offsets observed before the barrier.
    offsets: BTreeMap<String, i64>,
}

/// Checkpoint-aligned streaming sink writing to a native Iceberg table.
///
/// Owns a dedicated current-thread Tokio runtime for the async Iceberg
/// catalog operations, so every trait method is synchronous and safe to call
/// from `spawn_blocking` contexts. **Do not construct or drive this type from
/// inside an async executor thread** — `open` and the commit paths block.
pub struct IcebergStreamingSink {
    /// Owned current-thread runtime for the async Iceberg catalog calls.
    /// `Option` so `Drop` can take it and shut it down in the background —
    /// a plain `Runtime` drop blocks, which panics when the participant is
    /// dropped from an async context (e.g. job eviction on the executor).
    runtime: Option<tokio::runtime::Runtime>,
    table: IcebergNativeTwoPhaseCommit,
    target: IcebergSinkTarget,
    schema_version: SchemaVersion,
    open: Vec<RecordBatch>,
    pending_offsets: BTreeMap<String, i64>,
    prepared: BTreeMap<u64, PreparedEpoch>,
}

impl Drop for IcebergStreamingSink {
    fn drop(&mut self) {
        if let Some(rt) = self.runtime.take() {
            // Non-blocking shutdown: safe from async and sync contexts alike.
            rt.shutdown_background();
        }
    }
}

fn lake_err(e: LakehouseError) -> ConnectorError {
    ConnectorError::Protocol {
        message: format!("iceberg streaming sink: {e}"),
    }
}

impl IcebergStreamingSink {
    /// Open (or create) the target table and return a ready participant.
    ///
    /// Blocking: call via `spawn_blocking` from async contexts.
    pub fn open(
        target: IcebergSinkTarget,
        schema_version: SchemaVersion,
    ) -> ConnectorResult<Self> {
        if target.mode == IcebergSinkMode::Upsert && target.key_columns.is_empty() {
            return Err(ConnectorError::Protocol {
                message: "iceberg streaming sink: upsert mode requires key columns".into(),
            });
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ConnectorError::Protocol {
                message: format!("iceberg streaming sink: runtime build failed: {e}"),
            })?;
        let table = runtime
            .block_on(IcebergNativeTwoPhaseCommit::open(
                &target.root,
                &target.table,
                &schema_version,
            ))
            .map_err(lake_err)?;
        Ok(Self {
            runtime: Some(runtime),
            table,
            target,
            schema_version,
            open: Vec::new(),
            pending_offsets: BTreeMap::new(),
            prepared: BTreeMap::new(),
        })
    }

    /// Capabilities of this sink (feeds the engine capability metadata that
    /// the platform surfaces as delivery-guarantee labels).
    pub fn sink_capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_transactional()
            .with_checkpoint()
            .with_two_phase_commit()
    }

    // `runtime` is `Some` for the sink's whole life; the `Option` exists only
    // so `Drop` can take it for `shutdown_background()`. Absence is a logic
    // error, not a recoverable condition.
    #[allow(clippy::expect_used)]
    fn rt(&self) -> &tokio::runtime::Runtime {
        self.runtime
            .as_ref()
            .expect("runtime present until drop")
    }

    /// Read the committed table contents (testing/inspection).
    pub fn read_committed(&self) -> ConnectorResult<Vec<RecordBatch>> {
        self.rt().block_on(self.table.read_all()).map_err(lake_err)
    }

    /// Commit one epoch's staged content according to the sink mode.
    fn commit_epoch(&self, entry: &PreparedEpoch) -> ConnectorResult<()> {
        match self.target.mode {
            IcebergSinkMode::Append => {
                let files = entry.files.iter().map(|(_, f)| f.clone()).collect();
                self.rt()
                    .block_on(self.table.append_data_files(files, entry.offsets.clone()))
                    .map_err(lake_err)?;
            }
            IcebergSinkMode::Upsert => {
                let (upserts, delete_keys) = self.split_row_ops(&entry.batches)?;
                let mut keys = delete_keys;
                for batch in &upserts {
                    self.collect_keys(batch, &mut keys)?;
                }
                let current = self.read_committed()?;
                let mut next: Vec<RecordBatch> = Vec::with_capacity(current.len() + upserts.len());
                for batch in &current {
                    let filtered = self.filter_out_keys(batch, &keys)?;
                    if filtered.num_rows() > 0 {
                        next.push(filtered);
                    }
                }
                next.extend(upserts);
                self.rt()
                    .block_on(self.table.overwrite_commit(
                        next,
                        entry.offsets.clone(),
                        &self.schema_version,
                    ))
                    .map_err(lake_err)?;
            }
        }
        Ok(())
    }

    /// Split staged batches into (upsert rows with the op column stripped,
    /// set of keys to delete). Without an op column every row is an upsert.
    fn split_row_ops(
        &self,
        batches: &[RecordBatch],
    ) -> ConnectorResult<(Vec<RecordBatch>, std::collections::HashSet<String>)> {
        let mut upserts = Vec::with_capacity(batches.len());
        let mut delete_keys = std::collections::HashSet::new();
        let Some(op_column) = self.target.op_column.as_deref() else {
            return Ok((batches.to_vec(), delete_keys));
        };
        for batch in batches {
            let Ok(op_idx) = batch.schema().index_of(op_column) else {
                // Batch does not carry the op column — treat as pure upserts.
                upserts.push(batch.clone());
                continue;
            };
            let op_fmt = ArrayFormatter::try_new(
                batch.column(op_idx).as_ref(),
                &FormatOptions::default(),
            )
            .map_err(|e| ConnectorError::Schema {
                message: format!("iceberg streaming sink: op column format: {e}"),
            })?;
            let mut keep = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let op = op_fmt.value(row).to_string().to_lowercase();
                let is_delete = DELETE_OPS.contains(&op.trim());
                keep.push(Some(!is_delete));
                if is_delete {
                    delete_keys.insert(self.row_key(batch, row)?);
                }
            }
            let mask = BooleanArray::from(keep);
            let kept = arrow::compute::filter_record_batch(batch, &mask).map_err(|e| {
                ConnectorError::Schema {
                    message: format!("iceberg streaming sink: op filter: {e}"),
                }
            })?;
            let stripped = strip_column(&kept, op_idx)?;
            if stripped.num_rows() > 0 {
                upserts.push(stripped);
            }
        }
        Ok((upserts, delete_keys))
    }

    /// Format the key-column tuple of `row` as a stable string.
    fn row_key(&self, batch: &RecordBatch, row: usize) -> ConnectorResult<String> {
        let mut parts = Vec::with_capacity(self.target.key_columns.len());
        for col in &self.target.key_columns {
            let idx = batch
                .schema()
                .index_of(col)
                .map_err(|_| ConnectorError::Schema {
                    message: format!("iceberg streaming sink: key column '{col}' missing"),
                })?;
            let fmt =
                ArrayFormatter::try_new(batch.column(idx).as_ref(), &FormatOptions::default())
                    .map_err(|e| ConnectorError::Schema {
                        message: format!("iceberg streaming sink: key format: {e}"),
                    })?;
            parts.push(fmt.value(row).to_string());
        }
        Ok(parts.join("\u{1}"))
    }

    /// Insert every row key of `batch` into `keys`.
    fn collect_keys(
        &self,
        batch: &RecordBatch,
        keys: &mut std::collections::HashSet<String>,
    ) -> ConnectorResult<()> {
        for row in 0..batch.num_rows() {
            keys.insert(self.row_key(batch, row)?);
        }
        Ok(())
    }

    /// Keep only rows of `batch` whose key tuple is NOT in `keys`.
    fn filter_out_keys(
        &self,
        batch: &RecordBatch,
        keys: &std::collections::HashSet<String>,
    ) -> ConnectorResult<RecordBatch> {
        let mut keep = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            keep.push(Some(!keys.contains(&self.row_key(batch, row)?)));
        }
        let mask = BooleanArray::from(keep);
        arrow::compute::filter_record_batch(batch, &mask).map_err(|e| ConnectorError::Schema {
            message: format!("iceberg streaming sink: key filter: {e}"),
        })
    }
}

/// Remove column `idx` from `batch`.
fn strip_column(batch: &RecordBatch, idx: usize) -> ConnectorResult<RecordBatch> {
    let indices: Vec<usize> = (0..batch.num_columns()).filter(|&i| i != idx).collect();
    batch.project(&indices).map_err(|e| ConnectorError::Schema {
        message: format!("iceberg streaming sink: op column strip: {e}"),
    })
}

impl TransactionalSinkParticipant for IcebergStreamingSink {
    fn stage(&mut self, batch: &RecordBatch) -> ConnectorResult<()> {
        if batch.num_rows() > 0 {
            self.open.push(batch.clone());
        }
        Ok(())
    }

    fn stage_source_offsets(&mut self, offsets: &BTreeMap<String, i64>) -> ConnectorResult<()> {
        for (k, v) in offsets {
            self.pending_offsets.insert(k.clone(), *v);
        }
        Ok(())
    }

    fn pre_commit(&mut self, epoch: u64) -> ConnectorResult<()> {
        if let Some((&max_prepared, _)) = self.prepared.iter().next_back()
            && epoch <= max_prepared
        {
            return Err(ConnectorError::Protocol {
                message: format!(
                    "iceberg streaming sink: pre_commit epoch {epoch} is not greater than \
                     already prepared epoch {max_prepared}; checkpoint epochs must be monotonic"
                ),
            });
        }
        if self.open.is_empty() {
            return Ok(());
        }
        let (path, data_file) = self.table.stage_parquet(&self.open).map_err(lake_err)?;
        // Only take the buffer after staging succeeded so a failed pre_commit
        // can be retried at a later barrier without losing rows.
        let batches = std::mem::take(&mut self.open);
        let offsets = std::mem::take(&mut self.pending_offsets);
        self.prepared.insert(
            epoch,
            PreparedEpoch {
                files: vec![(path, data_file)],
                batches,
                offsets,
            },
        );
        Ok(())
    }

    fn commit_through(&mut self, epoch: u64) -> ConnectorResult<usize> {
        let mut later = self.prepared.split_off(&(epoch.saturating_add(1)));
        let to_commit = std::mem::take(&mut self.prepared);
        self.prepared.append(&mut later);

        let mut committed = 0usize;
        let mut iter = to_commit.into_iter();
        while let Some((txn_epoch, entry)) = iter.next() {
            if let Err(error) = self.commit_epoch(&entry) {
                // Re-queue this epoch and the rest for retry — the staged
                // files and batches are still held, so retrying is safe.
                self.prepared.insert(txn_epoch, entry);
                for (e, rest) in iter {
                    self.prepared.insert(e, rest);
                }
                return Err(error);
            }
            committed += entry.files.len().max(1);
        }
        Ok(committed)
    }

    fn abort_after(&mut self, epoch: u64) -> ConnectorResult<usize> {
        self.open.clear();
        self.pending_offsets.clear();
        let to_abort = self.prepared.split_off(&(epoch.saturating_add(1)));
        let mut aborted = 0usize;
        for (txn_epoch, entry) in to_abort {
            for (path, _) in &entry.files {
                if let Err(e) = std::fs::remove_file(path)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        epoch = txn_epoch,
                        path = %path.display(),
                        error = %e,
                        "iceberg streaming sink: failed to remove aborted staged file \
                         (left as orphan for VACUUM)"
                    );
                }
            }
            aborted += entry.files.len().max(1);
        }
        Ok(aborted)
    }

    fn open_rows(&self) -> usize {
        self.open.iter().map(|b| b.num_rows()).sum()
    }

    fn prepared_epochs(&self) -> Vec<u64> {
        self.prepared.keys().copied().collect()
    }
}

/// Derive a `SchemaVersion` for table creation from an Arrow schema,
/// excluding `op_column` (it never reaches the table).
pub fn schema_version_from_arrow(
    schema: &arrow::datatypes::Schema,
    op_column: Option<&str>,
) -> ConnectorResult<SchemaVersion> {
    use crate::lakehouse::SchemaField;
    let mut fields = Vec::new();
    let mut id = 1i32;
    for field in schema.fields() {
        if Some(field.name().as_str()) == op_column {
            continue;
        }
        let data_type = arrow_type_to_iceberg_str(field.data_type()).ok_or_else(|| {
            ConnectorError::Schema {
                message: format!(
                    "iceberg streaming sink: unsupported column type {} for '{}'",
                    field.data_type(),
                    field.name()
                ),
            }
        })?;
        fields.push(SchemaField {
            id,
            name: field.name().clone(),
            required: !field.is_nullable(),
            data_type: data_type.to_owned(),
        });
        id += 1;
    }
    Ok(SchemaVersion {
        schema_id: 1,
        fields,
    })
}

fn arrow_type_to_iceberg_str(dt: &arrow::datatypes::DataType) -> Option<&'static str> {
    use arrow::datatypes::DataType;
    Some(match dt {
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "int",
        DataType::Int64 => "long",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary",
        DataType::Date32 => "date",
        DataType::Timestamp(_, None) => "timestamp",
        DataType::Timestamp(_, Some(_)) => "timestamptz",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;

    fn arrow_schema(with_op: bool) -> Arc<ArrowSchema> {
        let mut fields = vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ];
        if with_op {
            fields.push(Field::new("__op", DataType::Utf8, false));
        }
        Arc::new(ArrowSchema::new(fields))
    }

    fn batch(rows: &[(&str, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema(false),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn op_batch(rows: &[(&str, i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema(true),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(k, _, _)| *k).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, o)| *o).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn committed_rows(sink: &IcebergStreamingSink) -> Vec<(String, i64)> {
        let mut rows = Vec::new();
        for b in sink.read_committed().unwrap() {
            let k = b
                .column(b.schema().index_of("k").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            let v = b
                .column(b.schema().index_of("v").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            for i in 0..b.num_rows() {
                rows.push((k.value(i).to_owned(), v.value(i)));
            }
        }
        rows.sort();
        rows
    }

    fn open_sink(dir: &std::path::Path, mode: IcebergSinkMode) -> IcebergStreamingSink {
        let op_column = (mode == IcebergSinkMode::Upsert).then(|| "__op".to_owned());
        let sv = schema_version_from_arrow(&arrow_schema(false), None).unwrap();
        IcebergStreamingSink::open(
            IcebergSinkTarget {
                root: dir.to_path_buf(),
                table: "t".into(),
                mode,
                key_columns: vec!["k".into()],
                op_column,
            },
            sv,
        )
        .unwrap()
    }

    #[test]
    fn append_mode_commits_only_covered_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);

        sink.stage(&batch(&[("a", 1)])).unwrap();
        sink.pre_commit(1).unwrap();
        sink.stage(&batch(&[("b", 2)])).unwrap();
        sink.pre_commit(2).unwrap();

        // Nothing visible before the completion notification.
        assert!(committed_rows(&sink).is_empty());

        assert_eq!(sink.commit_through(1).unwrap(), 1);
        assert_eq!(committed_rows(&sink), vec![("a".into(), 1)]);

        assert_eq!(sink.commit_through(2).unwrap(), 1);
        assert_eq!(
            committed_rows(&sink),
            vec![("a".into(), 1), ("b".into(), 2)]
        );
    }

    #[test]
    fn restore_commits_covered_and_aborts_later_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);

        sink.stage(&batch(&[("a", 1)])).unwrap();
        sink.pre_commit(1).unwrap();
        sink.stage(&batch(&[("b", 2)])).unwrap();
        sink.pre_commit(2).unwrap();
        sink.stage(&batch(&[("c", 3)])).unwrap(); // open, never prepared

        // Restore to epoch 1: recover-and-commit ≤ 1, recover-and-abort > 1.
        assert_eq!(sink.commit_through(1).unwrap(), 1);
        assert_eq!(sink.abort_after(1).unwrap(), 1);
        assert_eq!(sink.open_rows(), 0);
        assert!(sink.prepared_epochs().is_empty());
        assert_eq!(committed_rows(&sink), vec![("a".into(), 1)]);
    }

    #[test]
    fn committed_snapshots_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);
            sink.stage(&batch(&[("a", 1)])).unwrap();
            sink.pre_commit(1).unwrap();
            sink.commit_through(1).unwrap();
            // Epoch 2 prepared but never committed — simulated crash.
            sink.stage(&batch(&[("lost", 9)])).unwrap();
            sink.pre_commit(2).unwrap();
        }
        let sink = open_sink(dir.path(), IcebergSinkMode::Append);
        // Only the committed epoch is visible after recovery; the prepared
        // epoch's staged file is an invisible orphan.
        assert_eq!(committed_rows(&sink), vec![("a".into(), 1)]);
    }

    #[test]
    fn upsert_mode_replaces_rows_by_key_and_applies_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = open_sink(dir.path(), IcebergSinkMode::Upsert);

        // Epoch 1: initial rows.
        sink.stage(&op_batch(&[("a", 1, "u"), ("b", 2, "u")]))
            .unwrap();
        sink.pre_commit(1).unwrap();
        sink.commit_through(1).unwrap();
        assert_eq!(
            committed_rows(&sink),
            vec![("a".into(), 1), ("b".into(), 2)]
        );

        // Epoch 2: update a, delete b, insert c — row-level ops.
        sink.stage(&op_batch(&[("a", 10, "u"), ("b", 0, "d"), ("c", 3, "u")]))
            .unwrap();
        sink.pre_commit(2).unwrap();
        sink.commit_through(2).unwrap();
        assert_eq!(
            committed_rows(&sink),
            vec![("a".into(), 10), ("c".into(), 3)]
        );
    }

    #[test]
    fn upsert_without_op_column_treats_rows_as_upserts() {
        let dir = tempfile::tempdir().unwrap();
        let sv = schema_version_from_arrow(&arrow_schema(false), None).unwrap();
        let mut sink = IcebergStreamingSink::open(
            IcebergSinkTarget {
                root: dir.path().to_path_buf(),
                table: "t".into(),
                mode: IcebergSinkMode::Upsert,
                key_columns: vec!["k".into()],
                op_column: None,
            },
            sv,
        )
        .unwrap();

        sink.stage(&batch(&[("a", 1)])).unwrap();
        sink.pre_commit(1).unwrap();
        sink.commit_through(1).unwrap();
        sink.stage(&batch(&[("a", 2)])).unwrap();
        sink.pre_commit(2).unwrap();
        sink.commit_through(2).unwrap();
        assert_eq!(committed_rows(&sink), vec![("a".into(), 2)]);
    }

    #[test]
    fn source_offsets_land_in_snapshot_summary() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);

        let mut offsets = BTreeMap::new();
        offsets.insert("orders-0".to_owned(), 41i64);
        sink.stage(&batch(&[("a", 1)])).unwrap();
        sink.stage_source_offsets(&offsets).unwrap();
        sink.pre_commit(1).unwrap();
        sink.commit_through(1).unwrap();

        use iceberg::Catalog as _;
        let table = sink
            .rt()
            .block_on(sink.table.catalog.load_table(&sink.table.ident))
            .unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("krishiv.kafka.offset.orders-0")
                .map(String::as_str),
            Some("41")
        );
    }

    #[test]
    fn pre_commit_rejects_non_monotonic_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);
        sink.stage(&batch(&[("a", 1)])).unwrap();
        sink.pre_commit(5).unwrap();
        sink.stage(&batch(&[("b", 2)])).unwrap();
        assert!(sink.pre_commit(5).is_err());
        assert!(sink.pre_commit(4).is_err());
        // The open buffer survives the rejected barrier.
        assert_eq!(sink.open_rows(), 1);
        sink.pre_commit(6).unwrap();
        assert_eq!(sink.prepared_epochs(), vec![5, 6]);
    }
}
