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
use std::path::{Path, PathBuf};

use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};
use crate::lakehouse::iceberg_native::IcebergNativeTwoPhaseCommit;
use crate::lakehouse::two_phase::{kafka_offsets_json, parse_kafka_offsets_json};
use crate::lakehouse::{LakehouseError, SchemaVersion};
use crate::two_phase::{PreparedSinkRef, TransactionalSinkParticipant};

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

/// The DUR-2 recovery sidecar for a staged data file — a small JSON holding the
/// prepared epoch's source offsets, written next to the Parquet at `pre_commit`
/// so a fresh sink (after an executor crash lost the in-memory `PreparedEpoch`)
/// can reconstruct and idempotently finalize the transaction from its durable
/// path alone.
fn dur2_sidecar_path(data_file: &Path) -> PathBuf {
    let mut name = data_file.as_os_str().to_owned();
    name.push(".dur2.json");
    PathBuf::from(name)
}

impl IcebergStreamingSink {
    /// Open (or create) the target table and return a ready participant.
    ///
    /// Blocking: call via `spawn_blocking` from async contexts.
    pub fn open(target: IcebergSinkTarget, schema_version: SchemaVersion) -> ConnectorResult<Self> {
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
    /// the platform surfaces as delivery-guarantee labels). Delegates to the
    /// shared, feature-independent metadata so the coordinator's advertised
    /// guarantees cannot diverge from the implementation.
    pub fn sink_capabilities() -> ConnectorCapabilities {
        crate::capabilities::iceberg_streaming_sink_capabilities()
    }

    // `runtime` is `Some` for the sink's whole life; the `Option` exists only
    // so `Drop` can take it for `shutdown_background()`. Absence is a logic
    // error, not a recoverable condition.
    #[allow(clippy::expect_used)]
    fn rt(&self) -> &tokio::runtime::Runtime {
        self.runtime.as_ref().expect("runtime present until drop")
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
            let op_fmt =
                ArrayFormatter::try_new(batch.column(op_idx).as_ref(), &FormatOptions::default())
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
        // DUR-2: persist this epoch's source offsets beside the staged Parquet
        // so a fresh sink after an executor crash can reconstruct and
        // idempotently finalize the prepared transaction from its durable path.
        let sidecar = dur2_sidecar_path(&path);
        self.table
            .write_file(&sidecar.to_string_lossy(), &kafka_offsets_json(&offsets))
            .map_err(|e| ConnectorError::Protocol {
                message: format!(
                    "iceberg streaming sink: DUR-2 sidecar write {}: {e}",
                    sidecar.display()
                ),
            })?;
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
            // DUR-2: the epoch is durably committed; its recovery sidecar is no
            // longer needed.
            for (path, _) in &entry.files {
                self.table
                    .remove_file_best_effort(&dur2_sidecar_path(path).to_string_lossy());
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
        for (_txn_epoch, entry) in to_abort {
            for (path, _) in &entry.files {
                // DUR-2: drop the recovery sidecar alongside the staged file.
                // Both are scheme-aware (local FS or object store).
                self.table
                    .remove_file_best_effort(&dur2_sidecar_path(path).to_string_lossy());
                self.table
                    .remove_file_best_effort(&path.to_string_lossy());
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

    fn prepared_refs(&self) -> Vec<PreparedSinkRef> {
        // Report every prepared-but-uncommitted staged file so the coordinator
        // records it in the durable checkpoint (DUR-2). The staged Parquet path
        // is the recovery identity — `finalize_prepared` reconstructs from it.
        let mut refs = Vec::new();
        for (&epoch, entry) in &self.prepared {
            for (path, _) in &entry.files {
                refs.push(PreparedSinkRef {
                    epoch,
                    prepare_path: path.to_string_lossy().into_owned(),
                });
            }
        }
        refs
    }

    fn finalize_prepared(&mut self, prepare_path: &str, commit: bool) -> ConnectorResult<()> {
        let path = PathBuf::from(prepare_path);
        let sidecar = dur2_sidecar_path(&path);
        let sidecar_uri = sidecar.to_string_lossy().into_owned();

        // Read the durable offsets sidecar (scheme-aware: local FS or object
        // store). A missing sidecar means the epoch was already finalized
        // (sidecar removed on commit/abort) or was never durably prepared —
        // either way there is nothing to commit, so drop any stray staging file
        // and return. Idempotent.
        let offsets = match self.table.read_file_opt(&sidecar_uri).map_err(lake_err)? {
            Some(json) => parse_kafka_offsets_json(&json),
            None => {
                self.table.remove_file_best_effort(prepare_path);
                return Ok(());
            }
        };

        // Idempotency gate: if the current committed snapshot already covers
        // this epoch's offsets, it was committed — `fast_append` is not
        // idempotent, so a blind re-append would double-write. Treat as done.
        let committed = self
            .rt()
            .block_on(self.table.committed_kafka_offsets())
            .map_err(lake_err)?;
        let already_committed = !offsets.is_empty()
            && offsets
                .iter()
                .all(|(k, v)| committed.get(k).is_some_and(|c| c >= v));

        if already_committed || !commit {
            // Committed-already, or an explicit abort: discard the orphan
            // staging file + sidecar. For an append-mode commit the staging
            // file has already become the committed data file, so it is only an
            // orphan when we did not (re-)commit here — which is exactly these
            // branches.
            self.table.remove_file_best_effort(prepare_path);
            self.table.remove_file_best_effort(&sidecar_uri);
            return Ok(());
        }

        // Recover-commit: re-read the durable staged rows and commit them
        // through the SAME staging path the live sink uses — a fresh managed
        // Parquet file written by THIS instance. Committing a foreign
        // instance's staged file directly makes iceberg's manifest reference a
        // file its FileIO cannot resolve on read; re-staging avoids that and
        // keeps the recovery path byte-identical to the certified commit path.
        let batches = self.table.read_staged_parquet(prepare_path).map_err(lake_err)?;
        let (fresh_path, fresh_df) = self.table.stage_parquet(&batches).map_err(lake_err)?;
        let entry = PreparedEpoch {
            files: vec![(fresh_path.clone(), fresh_df)],
            batches,
            offsets,
        };
        self.commit_epoch(&entry)?;

        // The original staged file is now superseded by the committed fresh
        // copy. For upsert, commit_epoch rewrote the table into its own new
        // files, so the fresh staged copy is an orphan too.
        self.table.remove_file_best_effort(prepare_path);
        if self.target.mode == IcebergSinkMode::Upsert {
            self.table
                .remove_file_best_effort(&fresh_path.to_string_lossy());
        }
        self.table.remove_file_best_effort(&sidecar_uri);
        Ok(())
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
        let data_type =
            arrow_type_to_iceberg_str(field.data_type()).ok_or_else(|| ConnectorError::Schema {
                message: format!(
                    "iceberg streaming sink: unsupported column type {} for '{}'",
                    field.data_type(),
                    field.name()
                ),
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
        open_sink_root(dir.to_path_buf(), mode)
    }

    /// Open a sink at an arbitrary root — a local dir path or an object-store
    /// URI (`memory://…`) — so the DUR-2 recovery invariants can be proven on
    /// both backends from the same test bodies.
    fn open_sink_root(root: std::path::PathBuf, mode: IcebergSinkMode) -> IcebergStreamingSink {
        let op_column = (mode == IcebergSinkMode::Upsert).then(|| "__op".to_owned());
        let sv = schema_version_from_arrow(&arrow_schema(false), None).unwrap();
        IcebergStreamingSink::open(
            IcebergSinkTarget {
                root,
                table: "t".into(),
                mode,
                key_columns: vec!["k".into()],
                op_column,
            },
            sv,
        )
        .unwrap()
    }

    /// A fresh, unique `memory://` warehouse root. The backing in-process store
    /// is process-global and shared across sink instances (like S3), so a
    /// dropped-then-reopened sink recovers from what the prior instance wrote.
    fn unique_memory_root() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::path::PathBuf::from(format!("memory://dur2-obj-{n}/warehouse"))
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

    // ---- DUR-2: barrier-model prepared-sink recovery ----

    #[test]
    fn dur2_prepared_refs_report_staged_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);
        assert!(sink.prepared_refs().is_empty());

        sink.stage(&batch(&[("a", 1)])).unwrap();
        sink.pre_commit(1).unwrap();
        let refs = sink.prepared_refs();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].epoch, 1);
        assert!(refs[0].prepare_path.ends_with(".parquet"));

        // Committing the epoch clears the ref (nothing left to recover).
        sink.commit_through(1).unwrap();
        assert!(sink.prepared_refs().is_empty());
    }

    #[test]
    fn dur2_recover_commit_append_across_executor_crash() {
        let dir = tempfile::tempdir().unwrap();

        // Pre-crash: stage + pre_commit epoch 1 with source offsets, then the
        // executor dies before commit_through. In the barrier-driven durable
        // model the checkpoint (with its next-offset PAST this epoch) is already
        // persisted, so the source will NOT replay — abort-and-replay would lose
        // this data. DUR-2 recovery must commit it.
        let prepare_path = {
            let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);
            sink.stage(&batch(&[("a", 1)])).unwrap();
            sink.stage_source_offsets(&BTreeMap::from([("p0".to_string(), 5)]))
                .unwrap();
            sink.pre_commit(1).unwrap();
            sink.prepared_refs()[0].prepare_path.clone()
            // sink dropped here = simulated executor crash (prepared log lost).
        };

        // Fresh sink on the same durable root (post-restart executor).
        let mut recovered = open_sink(dir.path(), IcebergSinkMode::Append);
        assert!(
            recovered.prepared_epochs().is_empty(),
            "crash lost the in-memory prepared log"
        );

        // Durable recovery from the checkpoint ref: commit the prepared epoch.
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(committed_rows(&recovered), vec![("a".into(), 1)]);
        let committed = recovered
            .rt()
            .block_on(recovered.table.committed_kafka_offsets())
            .unwrap();
        assert_eq!(committed.get("p0"), Some(&5), "epoch offsets landed");

        // Idempotent: re-running recovery must not double-write (offset gate).
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(committed_rows(&recovered), vec![("a".into(), 1)]);
    }

    #[test]
    fn dur2_recover_abort_discards_uncommitted_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let prepare_path = {
            let mut sink = open_sink(dir.path(), IcebergSinkMode::Append);
            sink.stage(&batch(&[("z", 9)])).unwrap();
            sink.pre_commit(1).unwrap();
            sink.prepared_refs()[0].prepare_path.clone()
        };

        let mut recovered = open_sink(dir.path(), IcebergSinkMode::Append);
        // Restore plan says this epoch is past the restore point → abort.
        recovered.finalize_prepared(&prepare_path, false).unwrap();
        assert!(committed_rows(&recovered).is_empty());
        assert!(
            !std::path::Path::new(&prepare_path).exists(),
            "aborted staging file removed"
        );
        // Idempotent re-abort.
        recovered.finalize_prepared(&prepare_path, false).unwrap();
    }

    #[test]
    fn dur2_recover_commit_upsert_across_executor_crash() {
        let dir = tempfile::tempdir().unwrap();

        // Seed committed epoch 1.
        {
            let mut sink = open_sink(dir.path(), IcebergSinkMode::Upsert);
            sink.stage(&op_batch(&[("a", 1, "u"), ("b", 2, "u")])).unwrap();
            sink.pre_commit(1).unwrap();
            sink.commit_through(1).unwrap();
        }

        // Epoch 2 prepared (update a, delete b, insert c) then crash.
        let prepare_path = {
            let mut sink = open_sink(dir.path(), IcebergSinkMode::Upsert);
            sink.stage(&op_batch(&[("a", 10, "u"), ("b", 0, "d"), ("c", 3, "u")]))
                .unwrap();
            sink.stage_source_offsets(&BTreeMap::from([("p0".to_string(), 9)]))
                .unwrap();
            sink.pre_commit(2).unwrap();
            sink.prepared_refs()[0].prepare_path.clone()
        };

        let mut recovered = open_sink(dir.path(), IcebergSinkMode::Upsert);
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(
            committed_rows(&recovered),
            vec![("a".into(), 10), ("c".into(), 3)],
            "row-level merge replayed from the staged rows"
        );

        // Idempotent: the offset gate prevents a second overwrite.
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(
            committed_rows(&recovered),
            vec![("a".into(), 10), ("c".into(), 3)]
        );
    }

    // ---- DUR-2 on an object-store backend (deterministic, in-process) ----
    //
    // These mirror the local-FS DUR-2 tests above but run the sink on a
    // `memory://` warehouse, exercising the object-store code path in
    // `iceberg_native` (staging, version-hint, reads via `KrishivStorage`) and
    // the cross-instance recover-commit that a real S3/MinIO warehouse enables —
    // with no external dependency.

    #[test]
    fn object_store_round_trip_stage_commit_read() {
        let mut sink = open_sink_root(unique_memory_root(), IcebergSinkMode::Append);
        sink.stage(&batch(&[("a", 1), ("b", 2)])).unwrap();
        sink.pre_commit(1).unwrap();
        assert!(committed_rows(&sink).is_empty(), "nothing visible pre-commit");
        sink.commit_through(1).unwrap();
        assert_eq!(
            committed_rows(&sink),
            vec![("a".into(), 1), ("b".into(), 2)],
            "object-store append round-trips through the memory warehouse"
        );
    }

    #[test]
    fn dur2_recover_commit_append_across_crash_object_store() {
        let root = unique_memory_root();

        // Pre-crash: stage + pre_commit epoch 1, then drop (executor crash).
        let prepare_path = {
            let mut sink = open_sink_root(root.clone(), IcebergSinkMode::Append);
            sink.stage(&batch(&[("a", 1)])).unwrap();
            sink.stage_source_offsets(&BTreeMap::from([("p0".to_string(), 5)]))
                .unwrap();
            sink.pre_commit(1).unwrap();
            let p = sink.prepared_refs()[0].prepare_path.clone();
            assert!(
                p.starts_with("memory://"),
                "staged path is an object-store URI, got {p}"
            );
            p
        };

        // A DIFFERENT sink instance (the restarted executor) recovers from the
        // shared object store — the staged parquet + sidecar the dead instance
        // wrote are visible here, which is the whole point of shared storage.
        let mut recovered = open_sink_root(root, IcebergSinkMode::Append);
        assert!(recovered.prepared_epochs().is_empty());
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(committed_rows(&recovered), vec![("a".into(), 1)]);
        let committed = recovered
            .rt()
            .block_on(recovered.table.committed_kafka_offsets())
            .unwrap();
        assert_eq!(committed.get("p0"), Some(&5), "epoch offsets landed");

        // Idempotent: the offset gate prevents a double append.
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(committed_rows(&recovered), vec![("a".into(), 1)]);
    }

    #[test]
    fn dur2_recover_commit_upsert_across_crash_object_store() {
        let root = unique_memory_root();

        // Seed committed epoch 1.
        {
            let mut sink = open_sink_root(root.clone(), IcebergSinkMode::Upsert);
            sink.stage(&op_batch(&[("a", 1, "u"), ("b", 2, "u")])).unwrap();
            sink.pre_commit(1).unwrap();
            sink.commit_through(1).unwrap();
        }

        // Epoch 2 prepared (update a, delete b, insert c) then crash.
        let prepare_path = {
            let mut sink = open_sink_root(root.clone(), IcebergSinkMode::Upsert);
            sink.stage(&op_batch(&[("a", 10, "u"), ("b", 0, "d"), ("c", 3, "u")]))
                .unwrap();
            sink.stage_source_offsets(&BTreeMap::from([("p0".to_string(), 9)]))
                .unwrap();
            sink.pre_commit(2).unwrap();
            sink.prepared_refs()[0].prepare_path.clone()
        };

        let mut recovered = open_sink_root(root, IcebergSinkMode::Upsert);
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(
            committed_rows(&recovered),
            vec![("a".into(), 10), ("c".into(), 3)],
            "row-level merge replayed from the object-store staged rows"
        );

        // Idempotent: the offset gate prevents a second overwrite.
        recovered.finalize_prepared(&prepare_path, true).unwrap();
        assert_eq!(
            committed_rows(&recovered),
            vec![("a".into(), 10), ("c".into(), 3)]
        );
    }

    #[test]
    fn dur2_recover_abort_discards_uncommitted_epoch_object_store() {
        let root = unique_memory_root();
        let prepare_path = {
            let mut sink = open_sink_root(root.clone(), IcebergSinkMode::Append);
            sink.stage(&batch(&[("z", 9)])).unwrap();
            sink.pre_commit(1).unwrap();
            sink.prepared_refs()[0].prepare_path.clone()
        };

        let mut recovered = open_sink_root(root, IcebergSinkMode::Append);
        recovered.finalize_prepared(&prepare_path, false).unwrap();
        assert!(committed_rows(&recovered).is_empty(), "aborted epoch not committed");
        // Idempotent re-abort.
        recovered.finalize_prepared(&prepare_path, false).unwrap();
        assert!(committed_rows(&recovered).is_empty());
    }
}
