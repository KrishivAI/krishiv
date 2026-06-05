//! Filesystem-backed Iceberg-style table with Parquet data files (P1-10).
//!
//! Persists snapshot layers as Parquet under `{root}/data/` and metadata in
//! `{root}/metadata.json`. Supports restart durability: reopen the same path
//! and scan committed rows.

use arrow::record_batch::RecordBatch;
use futures::stream::Stream;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use crate::{IcebergScanOptions, IcebergTableRef, LakehouseError, LakehouseTable, SchemaVersion};

#[derive(Debug, Serialize, Deserialize)]
struct FsLayerMeta {
    snapshot_id: i64,
    file: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FsTableMetadata {
    last_snapshot_id: i64,
    layers: Vec<FsLayerMeta>,
}

#[derive(Debug)]
struct FsLayer {
    snapshot_id: i64,
    path: PathBuf,
}

/// Parquet-on-disk lakehouse table with snapshot layering (read + append).
#[doc = "**Beta API**: may change between minor releases."]
pub struct IcebergFsTable {
    table_ref: IcebergTableRef,
    schema_version: SchemaVersion,
    root: PathBuf,
    state: tokio::sync::Mutex<Vec<FsLayer>>,
}

impl IcebergFsTable {
    #[doc = "**Beta API**: may change between minor releases."]
    pub fn new(
        root: impl AsRef<Path>,
        table_ref: IcebergTableRef,
        schema_version: SchemaVersion,
    ) -> Result<Self, LakehouseError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("data")).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let layers = Self::load_layers(&root)?;
        Ok(Self {
            table_ref,
            schema_version,
            root,
            state: tokio::sync::Mutex::new(layers),
        })
    }

    fn metadata_path(root: &Path) -> PathBuf {
        root.join("metadata.json")
    }

    fn load_layers(root: &Path) -> Result<Vec<FsLayer>, LakehouseError> {
        let meta_path = Self::metadata_path(root);
        if !meta_path.exists() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(&meta_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let meta: FsTableMetadata =
            serde_json::from_str(&text).map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(meta
            .layers
            .into_iter()
            .map(|l| FsLayer {
                snapshot_id: l.snapshot_id,
                path: root.join("data").join(l.file),
            })
            .collect())
    }

    fn persist_metadata(layers: &[FsLayer], root: &Path) -> Result<(), LakehouseError> {
        let last_snapshot_id = layers.last().map(|l| l.snapshot_id).unwrap_or(0);
        let meta = FsTableMetadata {
            last_snapshot_id,
            layers: layers
                .iter()
                .map(|l| FsLayerMeta {
                    snapshot_id: l.snapshot_id,
                    file: l
                        .path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("part.parquet")
                        .to_string(),
                })
                .collect(),
        };
        let bytes =
            serde_json::to_vec_pretty(&meta).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let tmp = root.join("metadata.json.tmp");
        let final_path = Self::metadata_path(root);
        fs::write(&tmp, &bytes).map_err(|e| LakehouseError::Io(e.to_string()))?;
        // Sync the temp file to ensure the bytes are durable on disk before
        // the rename. Without this, a power loss between `write` and
        // `rename` can leave the renamed file empty or stale.
        if let Ok(f) = fs::OpenOptions::new().write(true).open(&tmp) {
            let _ = f.sync_all();
        }
        fs::rename(&tmp, &final_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        // Sync the parent directory so the rename itself is durable. On
        // Linux this requires opening the parent as a `File`; on platforms
        // where this is unsupported (e.g. Windows) the call is best-effort
        // and a no-op.
        #[cfg(unix)]
        {
            if let Ok(dir) = fs::File::open(root) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    fn read_parquet_file(path: &Path) -> Result<Vec<RecordBatch>, LakehouseError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .build()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        reader
            .map(|b| b.map_err(|e| LakehouseError::Io(e.to_string())))
            .collect()
    }

    fn write_parquet_file(path: &Path, batches: &[RecordBatch]) -> Result<(), LakehouseError> {
        if batches.is_empty() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        let schema = batches[0].schema();
        let file = File::create(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        writer
            .close()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(())
    }

    /// Stream the contents of the table batch-by-batch.
    ///
    /// Returns an async stream of `RecordBatch` items so the caller can
    /// process rows incrementally (e.g. via `for await batch in ...`).
    /// Internally this still materialises the full result set from the
    /// underlying Parquet files before yielding; a future revision can
    /// read each Parquet file in turn to bound peak memory to the size
    /// of the largest single file rather than the sum of all files.
    /// The `row_limit` option still applies: once the cumulative row
    /// count reaches the limit, the stream ends.
    pub async fn scan_stream(
        &self,
        opts: &IcebergScanOptions,
    ) -> Result<
        std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch, LakehouseError>> + Send>>,
        LakehouseError,
    > {
        let batches = self.scan(opts).await?;
        let row_limit = opts.row_limit;
        // Apply row_limit on the way out: yield full batches until the
        // cumulative count would exceed the limit, then emit a partial
        // batch (or stop) and terminate the stream.
        let stream = futures::stream::try_unfold(
            (batches.into_iter(), 0u64),
            move |(mut iter, rows_seen)| {
                let row_limit = row_limit;
                async move {
                    loop {
                        let Some(batch) = iter.next() else {
                            return Ok::<_, LakehouseError>(None);
                        };
                        let n = batch.num_rows() as u64;
                        let Some(limit) = row_limit else {
                            return Ok(Some((batch, (iter, rows_seen + n))));
                        };
                        if rows_seen >= limit {
                            return Ok(None);
                        }
                        let remaining = limit - rows_seen;
                        if n <= remaining {
                            return Ok(Some((batch, (iter, rows_seen + n))));
                        } else {
                            let take = remaining as usize;
                            return Ok(Some((batch.slice(0, take), (iter, limit))));
                        }
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }
}

#[async_trait::async_trait]
impl LakehouseTable for IcebergFsTable {
    fn table_ref(&self) -> &IcebergTableRef {
        &self.table_ref
    }

    async fn schema(&self) -> Result<SchemaVersion, LakehouseError> {
        Ok(self.schema_version.clone())
    }

    async fn scan(&self, opts: &IcebergScanOptions) -> Result<Vec<RecordBatch>, LakehouseError> {
        let layers = self.state.lock().await;
        let selected: Vec<&FsLayer> = if let Some(target) = opts.snapshot_id {
            layers.iter().filter(|l| l.snapshot_id <= target).collect()
        } else {
            layers.iter().collect()
        };
        let mut out = Vec::new();
        for layer in selected {
            out.extend(Self::read_parquet_file(&layer.path)?);
        }
        if let Some(limit) = opts.row_limit {
            let mut trimmed = Vec::new();
            let mut rows = 0u64;
            for batch in out {
                if rows >= limit {
                    break;
                }
                let take = (limit - rows).min(batch.num_rows() as u64) as usize;
                if take < batch.num_rows() {
                    trimmed.push(batch.slice(0, take));
                } else {
                    trimmed.push(batch);
                }
                rows += take as u64;
            }
            return Ok(trimmed);
        }
        Ok(out)
    }

    async fn append(&self, batches: Vec<RecordBatch>) -> Result<(), LakehouseError> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut layers = self.state.lock().await;
        let next_id = layers.last().map(|l| l.snapshot_id + 1).unwrap_or(1);
        let file_name = format!("snap-{next_id:05}.parquet");
        let path = self.root.join("data").join(&file_name);
        let tmp_path = self.root.join("data").join(format!(".{file_name}.tmp"));
        Self::write_parquet_file(&tmp_path, &batches)?;
        layers.push(FsLayer {
            snapshot_id: next_id,
            path: path.clone(),
        });
        Self::persist_metadata(&layers, &self.root)?;
        fs::rename(&tmp_path, &path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(())
    }

    async fn current_snapshot_id(&self) -> Result<Option<i64>, LakehouseError> {
        let layers = self.state.lock().await;
        Ok(layers.last().map(|l| l.snapshot_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema_version() -> SchemaVersion {
        SchemaVersion {
            schema_id: 1,
            fields: vec![crate::SchemaField {
                id: 1,
                name: "x".to_string(),
                required: true,
                data_type: "int64".to_string(),
            }],
        }
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[tokio::test]
    async fn iceberg_fs_append_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let root = dir.path().to_path_buf();

        {
            let table = IcebergFsTable::new(&root, table_ref.clone(), schema_version()).unwrap();
            table.append(vec![batch(vec![1, 2, 3])]).await.unwrap();
            table.append(vec![batch(vec![4, 5])]).await.unwrap();
        }

        let reopened = IcebergFsTable::new(&root, table_ref, schema_version()).unwrap();
        let rows: usize = reopened
            .scan(&IcebergScanOptions::new())
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 5);
        assert_eq!(reopened.current_snapshot_id().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn iceberg_fs_scan_empty_table() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "empty");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        let result = table.scan(&IcebergScanOptions::new()).await.unwrap();
        assert!(result.is_empty());
        assert_eq!(table.current_snapshot_id().await.unwrap(), None);
    }

    #[tokio::test]
    async fn iceberg_fs_scan_stream_yields_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        table.append(vec![batch(vec![1, 2, 3])]).await.unwrap();
        table.append(vec![batch(vec![4, 5])]).await.unwrap();
        // Collect the stream into a Vec<RecordBatch> and verify the total
        // row count matches the expected 5.
        use futures::StreamExt;
        let mut stream = table.scan_stream(&IcebergScanOptions::new()).await.unwrap();
        let mut collected = Vec::new();
        while let Some(b) = stream.next().await {
            collected.push(b.unwrap());
        }
        let rows: usize = collected.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[tokio::test]
    async fn iceberg_fs_scan_stream_honors_row_limit() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        table
            .append(vec![batch(vec![1, 2, 3, 4, 5])])
            .await
            .unwrap();
        use futures::StreamExt;
        let mut stream = table
            .scan_stream(&IcebergScanOptions::new().with_row_limit(2))
            .await
            .unwrap();
        let mut total = 0usize;
        while let Some(b) = stream.next().await {
            total += b.unwrap().num_rows();
        }
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn iceberg_fs_append_empty_batches_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        table.append(vec![]).await.unwrap();
        assert_eq!(table.current_snapshot_id().await.unwrap(), None);
        let result = table.scan(&IcebergScanOptions::new()).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn iceberg_fs_scan_with_row_limit() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        table
            .append(vec![batch(vec![1, 2, 3, 4, 5])])
            .await
            .unwrap();
        let result = table
            .scan(&IcebergScanOptions::new().with_row_limit(2))
            .await
            .unwrap();
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn iceberg_fs_scan_with_snapshot_id() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        table.append(vec![batch(vec![1, 2])]).await.unwrap();
        let snap1 = table.current_snapshot_id().await.unwrap().unwrap();
        table.append(vec![batch(vec![3, 4, 5])]).await.unwrap();

        let at_snap1 = table
            .scan(&IcebergScanOptions::new().with_snapshot(snap1))
            .await
            .unwrap();
        let rows: usize = at_snap1.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn iceberg_fs_snapshot_id_increments() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        assert_eq!(table.current_snapshot_id().await.unwrap(), None);
        table.append(vec![batch(vec![1])]).await.unwrap();
        assert_eq!(table.current_snapshot_id().await.unwrap(), Some(1));
        table.append(vec![batch(vec![2])]).await.unwrap();
        assert_eq!(table.current_snapshot_id().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn iceberg_fs_read_parquet_file_nonexistent_returns_empty() {
        let path = PathBuf::from("/nonexistent/path/file.parquet");
        let result = IcebergFsTable::read_parquet_file(&path).unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn iceberg_fs_table_ref() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("my_cat", "my_ns", "my_table");
        let table = IcebergFsTable::new(dir.path(), table_ref.clone(), schema_version()).unwrap();
        assert_eq!(table.table_ref(), &table_ref);
    }

    #[tokio::test]
    async fn iceberg_fs_schema_returns_version() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let sv = schema_version();
        let table = IcebergFsTable::new(dir.path(), table_ref, sv).unwrap();
        let result = table.schema().await.unwrap();
        assert_eq!(result.schema_id, 1);
        assert_eq!(result.fields[0].name, "x");
    }

    #[tokio::test]
    async fn iceberg_fs_multiple_appends_accumulate() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        for i in 0..5 {
            table.append(vec![batch(vec![i])]).await.unwrap();
        }
        let result = table.scan(&IcebergScanOptions::new()).await.unwrap();
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
        assert_eq!(table.current_snapshot_id().await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn iceberg_fs_data_files_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "t");
        let table = IcebergFsTable::new(dir.path(), table_ref, schema_version()).unwrap();
        table.append(vec![batch(vec![1, 2])]).await.unwrap();
        let data_dir = dir.path().join("data");
        let files: Vec<_> = fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        assert_eq!(files.len(), 1);
        let meta_path = dir.path().join("metadata.json");
        assert!(meta_path.exists());
    }
}
