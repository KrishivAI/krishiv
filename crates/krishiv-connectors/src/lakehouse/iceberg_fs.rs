//! Filesystem-backed Iceberg-style table with Parquet data files (P1-10).
//!
//! Persists snapshot layers as Parquet under `{root}/data/` and metadata as
//! versioned files `{root}/metadata-v{N}.json`. Supports restart durability:
//! reopen the same path and scan committed rows.
//!
//! ## Concurrent commits (G2/G3 gap register)
//!
//! Commits are optimistic-concurrency, Iceberg-style: `append` reads the
//! highest existing `metadata-v{N}.json`, then atomically creates
//! `metadata-v{N+1}.json` via `create_new` (`O_EXCL` on Unix) — the OS
//! guarantees only one writer can win that create. A losing writer (file
//! already exists) re-reads the fresh latest version and retries. This has
//! no unconditional-overwrite step anywhere, so two concurrent committers —
//! whether two tasks in one process or two separate `krishiv` processes
//! pointed at the same directory — can never lose one another's commit; the
//! loser retries instead of clobbering. There is no in-memory state to keep
//! consistent: every read (`scan`, `current_snapshot_id`, `append`) goes
//! straight to whatever `metadata-v{N}.json` is highest on disk right now.

use arrow::record_batch::RecordBatch;
use futures::stream::Stream;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use super::{IcebergScanOptions, IcebergTableRef, LakehouseError, LakehouseTable, SchemaVersion};

/// Bound on commit retries under contention. Each retry is cheap (a
/// directory listing + one `create_new` attempt), so this only matters
/// under pathological contention; exceeding it is treated as a hard error
/// rather than looping forever.
const MAX_COMMIT_ATTEMPTS: u32 = 64;

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

#[derive(Debug, Clone)]
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
        Ok(Self {
            table_ref,
            schema_version,
            root,
        })
    }

    fn metadata_version_path(root: &Path, version: u64) -> PathBuf {
        root.join(format!("metadata-v{version:020}.json"))
    }

    /// Parse the version number out of a `metadata-v{N}.json` file name, if
    /// that's what it is.
    fn parse_metadata_version(file_name: &str) -> Option<u64> {
        file_name
            .strip_prefix("metadata-v")?
            .strip_suffix(".json")?
            .parse()
            .ok()
    }

    /// The highest committed version currently on disk, or `0` if the table
    /// has never been committed to. Always a fresh directory scan — this is
    /// the only source of truth, so concurrent commits from other processes
    /// are visible on the next call.
    fn latest_version(root: &Path) -> Result<u64, LakehouseError> {
        if !root.exists() {
            return Ok(0);
        }
        let mut max_version = 0u64;
        for entry in fs::read_dir(root).map_err(|e| LakehouseError::Io(e.to_string()))? {
            let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Some(v) = Self::parse_metadata_version(&name) {
                max_version = max_version.max(v);
            }
        }
        Ok(max_version)
    }

    /// The current committed layers, read fresh from the highest version on
    /// disk. `(0, vec![])` when the table has never been committed to.
    fn load_latest(root: &Path) -> Result<(u64, Vec<FsLayer>), LakehouseError> {
        let version = Self::latest_version(root)?;
        if version == 0 {
            return Ok((0, Vec::new()));
        }
        let meta_path = Self::metadata_version_path(root, version);
        let text = fs::read_to_string(&meta_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let meta: FsTableMetadata =
            serde_json::from_str(&text).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let layers = meta
            .layers
            .into_iter()
            .map(|l| FsLayer {
                snapshot_id: l.snapshot_id,
                path: root.join("data").join(l.file),
            })
            .collect();
        Ok((version, layers))
    }

    /// Attempt to commit `layers` as version `next_version`. Returns `true`
    /// if this writer won (the version file didn't already exist and was
    /// created atomically); `false` if another writer committed that
    /// version first — the caller must re-read the fresh latest state and
    /// retry with a new `next_version`.
    fn try_commit_version(
        root: &Path,
        next_version: u64,
        layers: &[FsLayer],
    ) -> Result<bool, LakehouseError> {
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
        let path = Self::metadata_version_path(root, next_version);
        // `create_new` opens with O_EXCL on Unix (and CREATE_NEW on
        // Windows): the OS guarantees this fails with `AlreadyExists` if
        // another writer's create won the race, and that at most one of any
        // number of concurrent callers can succeed. This is the whole fix —
        // no unconditional overwrite exists anywhere in the commit path.
        let mut file = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) => return Err(LakehouseError::Io(e.to_string())),
        };
        use std::io::Write;
        file.write_all(&bytes)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        file.sync_all()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        // Sync the parent directory so the new file's directory entry is
        // durable. Best-effort on platforms without this concept.
        #[cfg(unix)]
        {
            if let Ok(dir) = fs::File::open(root) {
                let _ = dir.sync_all();
            }
        }
        Ok(true)
    }

    fn read_parquet_file(path: &Path) -> Result<Vec<RecordBatch>, LakehouseError> {
        if !path.exists() {
            return Err(LakehouseError::Io(format!(
                "committed Iceberg data file is missing: {}",
                path.display()
            )));
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
        let schema = batches
            .first()
            .ok_or_else(|| LakehouseError::Io("empty batches".to_string()))?
            .schema();
        let file = File::create(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        let file = writer
            .into_inner()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        file.sync_all()
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
                        Ok(Some((batch, (iter, rows_seen + n))))
                    } else {
                        let take = remaining as usize;
                        Ok(Some((batch.slice(0, take), (iter, limit))))
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
        let root = self.root.clone();
        let opts = opts.clone();
        tokio::task::spawn_blocking(move || {
            let (_version, layers) = Self::load_latest(&root)?;
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
        })
        .await
        .map_err(|e| LakehouseError::Io(format!("spawn_blocking panicked: {e}")))?
    }

    /// Commit `batches` as a new snapshot layer.
    ///
    /// Optimistic concurrency: reads the latest committed version, writes
    /// the data file under an attempt-unique name (so two racing attempts
    /// with the same candidate `snapshot_id` never clobber each other's
    /// Parquet file), then tries to atomically create the next version's
    /// metadata file. If another writer committed that version first, the
    /// data file this attempt wrote becomes harmless orphaned garbage (a
    /// future vacuum could reclaim it) and the whole attempt retries against
    /// the now-current latest version — up to `MAX_COMMIT_ATTEMPTS`.
    async fn append(&self, batches: Vec<RecordBatch>) -> Result<(), LakehouseError> {
        if batches.is_empty() {
            return Ok(());
        }
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            for _ in 0..MAX_COMMIT_ATTEMPTS {
                let (current_version, current_layers) = Self::load_latest(&root)?;
                let next_id = current_layers
                    .last()
                    .map(|l| l.snapshot_id + 1)
                    .unwrap_or(1);
                // Attempt-unique, not just snapshot-id-unique: two racing
                // attempts can compute the same `next_id` from the same
                // stale read, and must not write the same file path.
                let attempt_tag = uuid::Uuid::new_v4().simple().to_string();
                let file_name = format!("snap-{next_id:05}-{attempt_tag}.parquet");
                let path = root.join("data").join(&file_name);
                Self::write_parquet_file(&path, &batches)?;

                let new_layer = FsLayer {
                    snapshot_id: next_id,
                    path: path.clone(),
                };
                let mut next_layers = current_layers;
                next_layers.push(new_layer);

                if Self::try_commit_version(&root, current_version + 1, &next_layers)? {
                    return Ok(());
                }
                // Lost the race: another writer committed
                // `current_version + 1` first. Our data file is now
                // unreferenced garbage; leave it (harmless, reclaimable
                // later) and retry against the fresh state.
            }
            Err(LakehouseError::Io(format!(
                "commit did not succeed after {MAX_COMMIT_ATTEMPTS} attempts under contention"
            )))
        })
        .await
        .map_err(|e| LakehouseError::Io(format!("spawn_blocking panicked: {e}")))?
    }

    async fn current_snapshot_id(&self) -> Result<Option<i64>, LakehouseError> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let (_version, layers) = Self::load_latest(&root)?;
            Ok(layers.last().map(|l| l.snapshot_id))
        })
        .await
        .map_err(|e| LakehouseError::Io(format!("spawn_blocking panicked: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::lakehouse::{SchemaField, SchemaVersion};

    use super::*;

    fn schema_version() -> SchemaVersion {
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
    async fn iceberg_fs_read_parquet_file_nonexistent_returns_error() {
        let path = PathBuf::from("/nonexistent/path/file.parquet");
        let err = IcebergFsTable::read_parquet_file(&path).unwrap_err();
        assert!(err.to_string().contains("data file is missing"));
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
        let meta_path = IcebergFsTable::metadata_version_path(dir.path(), 1);
        assert!(meta_path.exists());
        let metadata: FsTableMetadata =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(metadata.layers.len(), 1);
        for layer in metadata.layers {
            assert!(
                data_dir.join(layer.file).exists(),
                "metadata must not reference a data file before it exists"
            );
        }
    }

    /// G3 (gap register): proves the "concurrent committers last-write-win
    /// (lost update)" bug is fixed. Each of `N` writers gets its *own*
    /// `IcebergFsTable` instance pointed at the same directory — no shared
    /// `Arc`, no shared in-memory state — the same shape a second `krishiv`
    /// process pointed at the same table would have. All `N` commit
    /// concurrently; if the old unconditional-overwrite scheme were still in
    /// place, the losers' commits would vanish (last write wins) and this
    /// would flake down to fewer than `N` rows / a non-`N` snapshot id.
    #[tokio::test]
    async fn concurrent_writers_with_independent_table_handles_lose_no_commits() {
        const N: i64 = 8;
        let dir = tempfile::tempdir().unwrap();
        let table_ref = IcebergTableRef::new("cat", "ns", "concurrent");
        let root = dir.path().to_path_buf();

        // Ensure the directory structure exists before racing writers start
        // (each `IcebergFsTable::new` would otherwise also race to create
        // it, which is fine — `create_dir_all` is idempotent — but this
        // keeps the test focused on the commit race specifically).
        IcebergFsTable::new(&root, table_ref.clone(), schema_version()).unwrap();

        let writers = (0..N).map(|i| {
            let root = root.clone();
            let table_ref = table_ref.clone();
            tokio::spawn(async move {
                let table = IcebergFsTable::new(&root, table_ref, schema_version()).unwrap();
                table.append(vec![batch(vec![i])]).await.unwrap();
            })
        });
        for w in writers {
            w.await.expect("writer task panicked");
        }

        // Fresh handle, fresh disk read — exactly what a reader in another
        // process would see after all writers finished.
        let reader = IcebergFsTable::new(&root, table_ref, schema_version()).unwrap();
        let result = reader.scan(&IcebergScanOptions::new()).await.unwrap();
        let mut values: Vec<i64> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        values.sort_unstable();
        assert_eq!(
            values,
            (0..N).collect::<Vec<_>>(),
            "every writer's row must survive — a lost update would shrink this below N"
        );
        assert_eq!(
            reader.current_snapshot_id().await.unwrap(),
            Some(N),
            "N successful commits must produce exactly N versions, none skipped/overwritten"
        );
    }
}
