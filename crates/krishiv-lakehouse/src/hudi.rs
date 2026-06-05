//! Apache Hudi Copy-On-Write snapshot, incremental, append, and upsert support (R18 S2).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, UInt32Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{LakehouseError, LakehouseResult};

/// Hudi query type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HudiQueryType {
    #[default]
    Snapshot,
    Incremental,
}

/// Reader for Hudi Copy-On-Write tables (timeline + Parquet base files).
#[derive(Debug, Clone)]
pub struct HudiSnapshotReader {
    table_path: PathBuf,
    query_type: HudiQueryType,
    begin_instant: Option<String>,
}

impl HudiSnapshotReader {
    /// Open a Hudi table directory.
    pub fn open(table_path: impl AsRef<Path>) -> Self {
        Self {
            table_path: table_path.as_ref().to_path_buf(),
            query_type: HudiQueryType::Snapshot,
            begin_instant: None,
        }
    }

    /// Restrict to commits after `instant` (exclusive) for incremental mode.
    pub fn with_begin_instant(mut self, instant: impl Into<String>) -> Self {
        self.begin_instant = Some(instant.into());
        self
    }

    /// Set query type (snapshot or incremental).
    pub fn with_query_type(mut self, query_type: HudiQueryType) -> Self {
        self.query_type = query_type;
        self
    }

    fn hoodie_dir(&self) -> PathBuf {
        self.table_path.join(".hoodie")
    }

    fn list_commits(&self) -> LakehouseResult<Vec<String>> {
        let timeline = self.hoodie_dir().join("timeline");
        if !timeline.exists() {
            return Err(LakehouseError::NotFound {
                table: self.table_path.display().to_string(),
            });
        }
        let mut instants = Vec::new();
        for entry in fs::read_dir(&timeline).map_err(|e| LakehouseError::Io(e.to_string()))? {
            let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".commit") {
                instants.push(name.trim_end_matches(".commit").to_string());
            }
        }
        instants.sort();
        Ok(instants)
    }

    fn commits_for_scan(&self) -> LakehouseResult<Vec<String>> {
        let all = self.list_commits()?;
        match self.query_type {
            HudiQueryType::Snapshot => Ok(all),
            HudiQueryType::Incremental => {
                let begin = self.begin_instant.as_deref().ok_or_else(|| {
                    LakehouseError::Io("incremental query requires begin_instant".into())
                })?;
                Ok(all.into_iter().filter(|c| c.as_str() > begin).collect())
            }
        }
    }

    fn parquet_files_for_commits(&self, commits: &[String]) -> LakehouseResult<Vec<PathBuf>> {
        let commit_metadata = commits
            .iter()
            .map(|commit| HudiCommitMetadata::read(&self.table_path, commit))
            .collect::<LakehouseResult<Vec<_>>>()?;

        if commit_metadata.iter().any(|meta| meta.base_file.is_some()) {
            return match self.query_type {
                HudiQueryType::Snapshot => {
                    let mut files = Vec::new();
                    let mut last_base_idx = None;
                    for (i, meta) in commit_metadata.iter().enumerate().rev() {
                        if meta.base_file.is_some() {
                            last_base_idx = Some(i);
                            break;
                        }
                    }
                    if let Some(idx) = last_base_idx {
                        for meta in &commit_metadata[idx..] {
                            if let Some(base) = &meta.base_file {
                                files.push(self.table_path.join(base));
                            } else if let Some(change) = &meta.change_file {
                                files.push(self.table_path.join(change));
                            }
                        }
                    }
                    Ok(files)
                }
                HudiQueryType::Incremental => Ok(commit_metadata
                    .iter()
                    .filter_map(|meta| meta.change_file.as_ref().or(meta.base_file.as_ref()))
                    .map(|path| self.table_path.join(path))
                    .collect()),
            };
        }

        let mut files = BTreeSet::new();
        for commit in commits {
            let meta = self
                .hoodie_dir()
                .join(format!("{commit}.commit"))
                .join("metadata");
            if meta.exists() {
                let text =
                    fs::read_to_string(&meta).map_err(|e| LakehouseError::Io(e.to_string()))?;
                for line in text.lines() {
                    if let Some(path) = line.strip_prefix("file:") {
                        files.insert(self.table_path.join(path));
                    }
                }
            }
            let data_dir = self.table_path.join(commit);
            if data_dir.is_dir() {
                for entry in
                    fs::read_dir(&data_dir).map_err(|e| LakehouseError::Io(e.to_string()))?
                {
                    let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
                    let p = entry.path();
                    if p.extension().is_some_and(|e| e == "parquet") {
                        files.insert(p);
                    }
                }
            }
        }
        Ok(files.into_iter().collect())
    }

    /// Scan matching Parquet files.
    pub fn scan_batches(&self) -> LakehouseResult<Vec<RecordBatch>> {
        let commits = self.commits_for_scan()?;
        let files = self.parquet_files_for_commits(&commits)?;
        let mut out = Vec::new();
        for path in files {
            let file = fs::File::open(&path).map_err(|e| LakehouseError::Io(e.to_string()))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| LakehouseError::Io(e.to_string()))?
                .build()
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            for batch in reader {
                out.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
            }
        }
        Ok(out)
    }

    /// Infer schema from the first readable batch.
    pub fn schema(&self) -> LakehouseResult<SchemaRef> {
        let batches = self.scan_batches()?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| LakehouseError::Io("hudi table has no readable data".into()))?;
        Ok(schema)
    }
}

/// Result of a Hudi Copy-On-Write write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HudiWriteResult {
    /// Hudi instant written by this operation.
    pub instant: String,
    /// Number of source rows appended to the table.
    pub rows_inserted: u64,
    /// Number of existing rows replaced during upsert.
    pub rows_updated: u64,
    /// Current snapshot row count after the commit.
    pub snapshot_rows: u64,
}

/// Local Apache Hudi Copy-On-Write writer.
///
/// Each commit writes a full base Parquet file for snapshot reads and a change
/// Parquet file for incremental reads. Upserts replace rows with matching typed
/// primary keys and append new keys, then publish the new base file through the
/// `.hoodie/timeline` commit instant.
#[derive(Debug, Clone)]
pub struct HudiCowWriter {
    table_path: PathBuf,
}

impl HudiCowWriter {
    /// Open or create a local Hudi Copy-On-Write table directory.
    pub fn open(table_path: impl AsRef<Path>) -> Self {
        Self {
            table_path: table_path.as_ref().to_path_buf(),
        }
    }

    /// Append rows using a generated Hudi instant.
    pub fn append(&self, batch: RecordBatch) -> LakehouseResult<HudiWriteResult> {
        self.append_at(next_instant(), batch)
    }

    /// Append rows using an explicit Hudi instant.
    pub fn append_at(
        &self,
        instant: impl Into<String>,
        batch: RecordBatch,
    ) -> LakehouseResult<HudiWriteResult> {
        let instant = instant.into();
        validate_instant(&instant)?;
        let current = self.current_snapshot_batch()?;
        let (base_batch, snapshot_rows) = match current {
            Some(existing) if existing.num_rows() > 0 => {
                (None, existing.num_rows() as u64 + batch.num_rows() as u64)
            }
            _ => (Some(&batch), batch.num_rows() as u64),
        };
        let result = self.write_commit(
            &instant,
            "append",
            None,
            base_batch,
            &batch,
            batch.num_rows() as u64,
            0,
            snapshot_rows,
        )?;
        Ok(result)
    }

    /// Upsert rows by primary-key column using a generated Hudi instant.
    pub fn upsert(&self, key_column: &str, batch: RecordBatch) -> LakehouseResult<HudiWriteResult> {
        self.upsert_at(next_instant(), key_column, batch)
    }

    /// Upsert rows by primary-key column using an explicit Hudi instant.
    pub fn upsert_at(
        &self,
        instant: impl Into<String>,
        key_column: &str,
        batch: RecordBatch,
    ) -> LakehouseResult<HudiWriteResult> {
        let instant = instant.into();
        validate_instant(&instant)?;
        let source = deduplicate_by_key_last(&batch, key_column)?;
        let source_key_col = source
            .column_by_name(key_column)
            .ok_or_else(|| LakehouseError::Io(format!("hudi upsert key '{key_column}' missing")))?;
        let source_keys = keys_set(source_key_col.as_ref())?;

        let (merged, rows_updated, rows_inserted) = match self.current_snapshot_batch()? {
            Some(existing) if existing.num_rows() > 0 => {
                ensure_same_schema(existing.schema(), source.schema())?;
                let target_key_col = existing.column_by_name(key_column).ok_or_else(|| {
                    LakehouseError::Io(format!("hudi upsert key '{key_column}' missing in table"))
                })?;
                let target_keys = keys_set(target_key_col.as_ref())?;
                let keep_indices: Vec<u32> = (0..existing.num_rows())
                    .filter_map(|row| {
                        let key = typed_key(target_key_col.as_ref(), row);
                        match key {
                            Ok(key) if source_keys.contains(&key) => None,
                            Ok(_) => Some(Ok(row as u32)),
                            Err(e) => Some(Err(e)),
                        }
                    })
                    .collect::<LakehouseResult<Vec<_>>>()?;
                let updated = source_keys.intersection(&target_keys).count() as u64;
                let inserted = source_keys.difference(&target_keys).count() as u64;
                let merged = if keep_indices.is_empty() {
                    source.clone()
                } else {
                    concat_batches(&[take_rows(&existing, &keep_indices)?, source.clone()])?
                };
                (merged, updated, inserted)
            }
            _ => (source.clone(), 0, source.num_rows() as u64),
        };

        self.write_commit(
            &instant,
            "upsert",
            Some(key_column),
            Some(&merged),
            &source,
            rows_inserted,
            rows_updated,
            merged.num_rows() as u64,
        )
    }

    fn current_snapshot_batch(&self) -> LakehouseResult<Option<RecordBatch>> {
        let reader = HudiSnapshotReader::open(&self.table_path);
        let batches = match reader.scan_batches() {
            Ok(batches) => batches,
            Err(LakehouseError::NotFound { .. }) => Vec::new(),
            Err(e) => return Err(e),
        };
        if batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(concat_batches(&batches)?))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write_commit(
        &self,
        instant: &str,
        action: &str,
        key_column: Option<&str>,
        base_batch: Option<&RecordBatch>,
        change_batch: &RecordBatch,
        rows_inserted: u64,
        rows_updated: u64,
        snapshot_rows: u64,
    ) -> LakehouseResult<HudiWriteResult> {
        fs::create_dir_all(self.table_path.join(".hoodie/timeline"))
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let timeline_marker = self
            .table_path
            .join(".hoodie/timeline")
            .join(format!("{instant}.commit"));
        if timeline_marker.exists() {
            return Err(LakehouseError::Concurrency {
                message: format!("Hudi instant '{instant}' already exists"),
            });
        }
        let commit_dir = self.table_path.join(instant);
        if commit_dir.exists() {
            return Err(LakehouseError::Concurrency {
                message: format!("Hudi commit directory already exists for instant '{instant}'"),
            });
        }
        fs::create_dir_all(&commit_dir).map_err(|e| LakehouseError::Io(e.to_string()))?;

        let change_rel = format!("{instant}/changes-0.parquet");
        let base_rel = base_batch
            .as_ref()
            .map(|_| format!("{instant}/base-0.parquet"));
        if let Some(base) = base_batch
            && let Some(ref base_rel_str) = base_rel
        {
            write_parquet_batch(&self.table_path.join(base_rel_str), base)?;
        }
        write_parquet_batch(&self.table_path.join(&change_rel), change_batch)?;

        let metadata = HudiCommitMetadata {
            instant: instant.to_string(),
            action: action.to_string(),
            key_column: key_column.map(str::to_string),
            base_file: base_rel,
            change_file: Some(change_rel),
            legacy_files: Vec::new(),
        };
        metadata.write(&self.table_path)?;
        // Durability: the timeline marker is the "commit succeeded" signal.
        // A power loss between the metadata write and the marker write could
        // leave the table with a metadata directory but no marker, which
        // Hudi would interpret as a failed/aborted commit. fsync the marker
        // (and on Unix, the parent directory entry) to make the commit
        // atomic and crash-safe.
        let marker_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&timeline_marker)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        marker_file
            .sync_all()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        #[cfg(unix)]
        if let Some(parent) = timeline_marker.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        Ok(HudiWriteResult {
            instant: instant.to_string(),
            rows_inserted,
            rows_updated,
            snapshot_rows,
        })
    }
}

/// Append rows to a local Hudi Copy-On-Write table.
pub fn write_hudi_cow_append(
    table_path: impl AsRef<Path>,
    batch: RecordBatch,
) -> LakehouseResult<HudiWriteResult> {
    HudiCowWriter::open(table_path).append(batch)
}

/// Upsert rows into a local Hudi Copy-On-Write table by key column.
pub fn write_hudi_cow_upsert(
    table_path: impl AsRef<Path>,
    key_column: &str,
    batch: RecordBatch,
) -> LakehouseResult<HudiWriteResult> {
    HudiCowWriter::open(table_path).upsert(key_column, batch)
}

#[derive(Debug, Clone, Default)]
struct HudiCommitMetadata {
    instant: String,
    action: String,
    key_column: Option<String>,
    base_file: Option<String>,
    change_file: Option<String>,
    legacy_files: Vec<String>,
}

impl HudiCommitMetadata {
    fn read(table_path: &Path, instant: &str) -> LakehouseResult<Self> {
        let path = table_path
            .join(".hoodie")
            .join(format!("{instant}.commit"))
            .join("metadata");
        if !path.exists() {
            return Ok(Self {
                instant: instant.to_string(),
                ..Self::default()
            });
        }
        let text = fs::read_to_string(&path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut meta = Self {
            instant: instant.to_string(),
            ..Self::default()
        };
        for line in text.lines() {
            if let Some(value) = line.strip_prefix("action:") {
                meta.action = value.to_string();
            } else if let Some(value) = line.strip_prefix("key:") {
                meta.key_column = Some(value.to_string());
            } else if let Some(value) = line.strip_prefix("base_file:") {
                meta.base_file = Some(value.to_string());
            } else if let Some(value) = line.strip_prefix("change_file:") {
                meta.change_file = Some(value.to_string());
            } else if let Some(value) = line.strip_prefix("file:") {
                meta.legacy_files.push(value.to_string());
            }
        }
        Ok(meta)
    }

    fn write(&self, table_path: &Path) -> LakehouseResult<()> {
        let commit_dir = table_path
            .join(".hoodie")
            .join(format!("{}.commit", self.instant));
        fs::create_dir_all(&commit_dir).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut text = String::new();
        text.push_str(&format!("action:{}\n", self.action));
        if let Some(key) = &self.key_column {
            text.push_str(&format!("key:{key}\n"));
        }
        if let Some(base_file) = &self.base_file {
            text.push_str(&format!("base_file:{base_file}\n"));
        }
        if let Some(change_file) = &self.change_file {
            text.push_str(&format!("change_file:{change_file}\n"));
        }
        let meta_path = commit_dir.join("metadata");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&meta_path)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        use std::io::Write as _;
        file.write_all(text.as_bytes())
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        // Durability: the Hudi commit metadata must be on disk before the
        // timeline marker signals "commit succeeded" (the marker write above
        // is in `write_commit`). Without this fsync, a power loss between
        // the metadata write and the marker write would leave the marker
        // pointing at a missing/stale metadata file.
        file.sync_all()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        Ok(())
    }
}

/// Build a minimal Hudi CoW fixture for tests.
pub fn write_hudi_cow_fixture(
    root: &Path,
    commits: &[(&str, &[(i64, &str)])],
) -> LakehouseResult<()> {
    fs::create_dir_all(root.join(".hoodie/timeline"))
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    for (instant, rows) in commits {
        let commit_dir = root.join(*instant);
        fs::create_dir_all(&commit_dir).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let parquet_path = commit_dir.join("part-0.parquet");
        write_parquet_i64_string(&parquet_path, rows)?;
        let mut meta = String::new();
        meta.push_str(&format!("file:{instant}/part-0.parquet\n"));
        fs::create_dir_all(root.join(".hoodie").join(format!("{instant}.commit")))
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        fs::write(
            root.join(".hoodie")
                .join(format!("{instant}.commit"))
                .join("metadata"),
            meta,
        )
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
        fs::write(
            root.join(".hoodie/timeline")
                .join(format!("{instant}.commit")),
            "",
        )
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    }
    Ok(())
}

// ── ObjectStore-backed Hudi reader/writer ────────────────────────────────────

/// Serialize a `RecordBatch` to Parquet bytes in memory.
fn batch_to_parquet_bytes(batch: &RecordBatch) -> LakehouseResult<bytes::Bytes> {
    use parquet::arrow::ArrowWriter;
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    writer
        .write(batch)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    writer
        .close()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(bytes::Bytes::from(buf))
}

/// Read `RecordBatch`es from Parquet bytes.
fn parquet_bytes_to_batches(data: bytes::Bytes) -> LakehouseResult<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    let reader = ParquetRecordBatchReader::try_new(data, 1024)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LakehouseError::Io(e.to_string()))
}

/// ObjectStore-backed Hudi Copy-On-Write reader.
///
/// Reads commits and Parquet data files from any `ObjectStore` implementation
/// (S3, GCS, Azure, or in-memory for tests). Compatible with tables written
/// by [`HudiObjectStoreWriter`].
pub struct HudiObjectStoreReader {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
    query_type: HudiQueryType,
    begin_instant: Option<String>,
}

impl HudiObjectStoreReader {
    /// Create a reader pointing at `prefix` within `store`.
    pub fn new(store: Arc<dyn object_store::ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            query_type: HudiQueryType::Snapshot,
            begin_instant: None,
        }
    }

    /// Restrict to commits after `instant` for incremental queries.
    #[must_use]
    pub fn with_begin_instant(mut self, instant: impl Into<String>) -> Self {
        self.begin_instant = Some(instant.into());
        self
    }

    /// Set query type.
    #[must_use]
    pub fn with_query_type(mut self, query_type: HudiQueryType) -> Self {
        self.query_type = query_type;
        self
    }

    async fn timeline_prefix(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/.hoodie/timeline", self.prefix))
    }

    async fn list_commits(&self) -> LakehouseResult<Vec<String>> {
        use futures::StreamExt as _;
        let prefix = self.timeline_prefix().await;
        let mut commits = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| LakehouseError::Io(e.to_string()))?;
            let name = meta.location.filename().unwrap_or("").to_string();
            if name.ends_with(".commit") {
                commits.push(name.trim_end_matches(".commit").to_string());
            }
        }
        commits.sort();
        Ok(commits)
    }

    /// Scan all matching Parquet files and return record batches.
    pub async fn scan_batches(&self) -> LakehouseResult<Vec<RecordBatch>> {
        use futures::StreamExt as _;
        let all_commits = self.list_commits().await?;
        let commits: Vec<_> = match self.query_type {
            HudiQueryType::Snapshot => all_commits,
            HudiQueryType::Incremental => {
                let begin = self.begin_instant.as_deref().ok_or_else(|| {
                    LakehouseError::Io("incremental query requires begin_instant".into())
                })?;
                all_commits
                    .into_iter()
                    .filter(|c| c.as_str() > begin)
                    .collect()
            }
        };

        let mut out = Vec::new();
        for commit in &commits {
            // Find Parquet files under <prefix>/<commit>/
            let commit_prefix =
                object_store::path::Path::from(format!("{}/{}", self.prefix, commit));
            let mut stream = self.store.list(Some(&commit_prefix));
            while let Some(meta) = stream.next().await {
                let meta = meta.map_err(|e| LakehouseError::Io(e.to_string()))?;
                let name = meta.location.filename().unwrap_or("").to_string();
                if name.ends_with(".parquet") {
                    let get_result = self
                        .store
                        .get_opts(&meta.location, Default::default())
                        .await
                        .map_err(|e| LakehouseError::Io(e.to_string()))?;
                    let data = get_result
                        .bytes()
                        .await
                        .map_err(|e| LakehouseError::Io(e.to_string()))?;
                    out.extend(parquet_bytes_to_batches(data)?);
                }
            }
        }
        Ok(out)
    }
}

/// ObjectStore-backed Hudi Copy-On-Write writer.
///
/// Each commit writes one Parquet data file and a marker file in the timeline.
pub struct HudiObjectStoreWriter {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
}

impl HudiObjectStoreWriter {
    /// Create a writer targeting `prefix` within `store`.
    pub fn new(store: Arc<dyn object_store::ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// Append rows, generating a new Hudi instant.
    pub async fn append(&self, batch: RecordBatch) -> LakehouseResult<HudiWriteResult> {
        let instant = next_instant();
        let parquet_bytes = batch_to_parquet_bytes(&batch)?;
        let data_path =
            object_store::path::Path::from(format!("{}/{}/part-0.parquet", self.prefix, instant));
        self.store
            .put_opts(&data_path, parquet_bytes.into(), Default::default())
            .await
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        // Write timeline commit marker.
        let commit_path = object_store::path::Path::from(format!(
            "{}/.hoodie/timeline/{}.commit",
            self.prefix, instant
        ));
        self.store
            .put_opts(
                &commit_path,
                bytes::Bytes::from("{}").into(),
                Default::default(),
            )
            .await
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let rows = batch.num_rows() as u64;
        Ok(HudiWriteResult {
            instant,
            rows_inserted: rows,
            rows_updated: 0,
            snapshot_rows: rows,
        })
    }
}

fn write_parquet_i64_string(path: &Path, rows: &[(i64, &str)]) -> LakehouseResult<()> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let names: Vec<&str> = rows.iter().map(|(_, n)| *n).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| LakehouseError::Io(e.to_string()))?;
    let file = fs::File::create(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| LakehouseError::Io(e.to_string()))?;
    writer
        .write(&batch)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    writer
        .close()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(())
}

fn write_parquet_batch(path: &Path, batch: &RecordBatch) -> LakehouseResult<()> {
    use parquet::arrow::ArrowWriter;

    let file = fs::File::create(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    writer
        .write(batch)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    writer
        .close()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(())
}

fn concat_batches(batches: &[RecordBatch]) -> LakehouseResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }
    let schema = batches[0].schema();
    for batch in batches.iter().skip(1) {
        ensure_same_schema(schema.clone(), batch.schema())?;
    }
    let mut columns: Vec<Vec<Arc<dyn Array>>> = vec![Vec::new(); schema.fields().len()];
    for batch in batches {
        for (idx, column) in batch.columns().iter().enumerate() {
            columns[idx].push(column.clone());
        }
    }
    let arrays = columns
        .into_iter()
        .map(|parts| {
            arrow::compute::concat(&parts.iter().map(|p| p.as_ref()).collect::<Vec<_>>())
                .map_err(|e| LakehouseError::Io(e.to_string()))
        })
        .collect::<LakehouseResult<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|e| LakehouseError::Io(e.to_string()))
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> LakehouseResult<RecordBatch> {
    let idx = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            arrow::compute::take(column, &idx, None).map_err(|e| LakehouseError::Io(e.to_string()))
        })
        .collect::<LakehouseResult<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| LakehouseError::Io(e.to_string()))
}

fn deduplicate_by_key_last(batch: &RecordBatch, key_column: &str) -> LakehouseResult<RecordBatch> {
    let key_col = batch
        .column_by_name(key_column)
        .ok_or_else(|| LakehouseError::Io(format!("hudi upsert key '{key_column}' missing")))?;
    let mut last_by_key = BTreeMap::new();
    for row in 0..batch.num_rows() {
        last_by_key.insert(typed_key(key_col.as_ref(), row)?, row as u32);
    }
    let indices = last_by_key.into_values().collect::<Vec<_>>();
    take_rows(batch, &indices)
}

fn keys_set(array: &dyn Array) -> LakehouseResult<HashSet<String>> {
    (0..array.len()).map(|row| typed_key(array, row)).collect()
}

fn typed_key(array: &dyn Array, row: usize) -> LakehouseResult<String> {
    if array.is_null(row) {
        return Err(LakehouseError::Io(
            "hudi upsert key columns must not contain null values".into(),
        ));
    }
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(format!("{}:{}", array.data_type(), formatter.value(row)))
}

fn ensure_same_schema(left: SchemaRef, right: SchemaRef) -> LakehouseResult<()> {
    if left.as_ref() == right.as_ref() {
        Ok(())
    } else {
        Err(LakehouseError::SchemaConflict {
            message: format!("Hudi CoW write schema mismatch: left={left:?}, right={right:?}"),
        })
    }
}

fn validate_instant(instant: &str) -> LakehouseResult<()> {
    // Hudi instant format produced by `next_instant`:
    //   YYYYMMDDHHMMSSfff-PPPPPPPPCCCCCCCC
    //   ^ 17 digits     ^ '-' ^ 16 lowercase hex chars
    // The timestamp prefix is 17 digits (millisecond precision); the
    // separator is `-` (an `_` separator is also accepted for legacy
    // compatibility with timestamps written by older callers); the suffix
    // is 16 lowercase hex characters (PID + counter).
    //
    // Reject anything that does not match. Without this strict check, a
    // Hudi timeline directory containing a malformed file (e.g. a partial
    // commit) could be mistaken for a valid instant and pollute later
    // `list_instant_times` results.
    if instant.len() != 34 {
        return Err(LakehouseError::Io(format!(
            "invalid Hudi instant '{instant}': expected 34 chars (YYYYMMDDHHMMSSfff-<16hex>), got {}",
            instant.len()
        )));
    }
    let bytes = instant.as_bytes();
    if !bytes[..17].iter().all(|b| b.is_ascii_digit()) {
        return Err(LakehouseError::Io(format!(
            "invalid Hudi instant '{instant}': first 17 chars must be digits (timestamp prefix)"
        )));
    }
    if bytes[17] != b'-' && bytes[17] != b'_' {
        return Err(LakehouseError::Io(format!(
            "invalid Hudi instant '{instant}': separator at position 17 must be '-' or '_'"
        )));
    }
    if !bytes[18..]
        .iter()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(LakehouseError::Io(format!(
            "invalid Hudi instant '{instant}': last 16 chars must be lowercase hex (process suffix)"
        )));
    }
    Ok(())
}

fn next_instant() -> String {
    static LAST_INSTANT_MS: AtomicU64 = AtomicU64::new(0);

    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let unique_ms = loop {
        let previous = LAST_INSTANT_MS.load(Ordering::Relaxed);
        let candidate = now_ms.max(previous.saturating_add(1));
        if LAST_INSTANT_MS
            .compare_exchange(previous, candidate, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            break candidate;
        }
    };

    // Append a process-unique suffix to the millisecond timestamp so two
    // executors writing to the same Hudi timeline do not collide on the same
    // instant. The suffix is a 16-char lowercase hex string derived from the
    // process PID and a per-process counter; this keeps the canonical Hudi
    // format (`%Y%m%d%H%M%S%3f-<suffix>`) but guarantees cross-process
    // uniqueness.
    let suffix = process_unique_suffix();
    let ts = chrono::DateTime::from_timestamp_millis(unique_ms as i64)
        .map(|dt| dt.format("%Y%m%d%H%M%S%3f").to_string())
        .unwrap_or_else(|| unique_ms.to_string());
    format!("{ts}-{suffix}")
}

/// Per-process identifier embedded into Hudi instants so that two executors
/// writing to the same object store never produce the same `next_instant()`
/// value. Combined with the monotonically increasing millisecond timestamp,
/// the resulting instant is sortable (timestamp prefix) and unique
/// cross-process (suffix).
fn process_unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() as u64;
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    // 16 lowercase hex chars: 8 from pid, 8 from counter.
    format!("{pid:08x}{counter:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::fs;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn batch(rows: &[(i64, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let names = rows.iter().map(|(_, name)| *name).collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn names(batch: &RecordBatch) -> Vec<String> {
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| names.value(row).to_string())
            .collect()
    }

    #[test]
    fn hudi_incremental_returns_only_later_commit() {
        let dir = tempdir().unwrap();
        write_hudi_cow_fixture(
            dir.path(),
            &[
                ("20240101120000123-0123456789abcdef", &[(1, "a")]),
                ("20240102120000123-0123456789abcdef", &[(2, "b")]),
            ],
        )
        .unwrap();
        let reader = HudiSnapshotReader::open(dir.path())
            .with_query_type(HudiQueryType::Incremental)
            .with_begin_instant("20240101120000123-0123456789abcdef");
        let batches = reader.scan_batches().unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
    }

    #[test]
    fn hudi_cow_append_writes_snapshot_and_incremental_changes() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        writer
            .append_at("20240101120000123-0123456789abcdef", batch(&[(1, "a")]))
            .unwrap();
        let result = writer
            .append_at("20240102120000123-0123456789abcdef", batch(&[(2, "b")]))
            .unwrap();
        assert_eq!(result.rows_inserted, 1);
        assert_eq!(result.snapshot_rows, 2);

        let snapshot = HudiSnapshotReader::open(dir.path()).scan_batches().unwrap();
        assert_eq!(snapshot.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

        let incremental = HudiSnapshotReader::open(dir.path())
            .with_query_type(HudiQueryType::Incremental)
            .with_begin_instant("20240101120000123-0123456789abcdef")
            .scan_batches()
            .unwrap();
        assert_eq!(incremental.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(names(&incremental[0]), vec!["b"]);
    }

    #[test]
    fn hudi_cow_upsert_replaces_existing_keys_and_inserts_new_keys() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        writer
            .append_at(
                "20240101120000123-0123456789abcdef",
                batch(&[(1, "a"), (2, "b")]),
            )
            .unwrap();
        let result = writer
            .upsert_at(
                "20240102120000123-0123456789abcdef",
                "id",
                batch(&[(2, "bb"), (3, "c")]),
            )
            .unwrap();
        assert_eq!(result.rows_updated, 1);
        assert_eq!(result.rows_inserted, 1);
        assert_eq!(result.snapshot_rows, 3);

        let snapshot = HudiSnapshotReader::open(dir.path()).scan_batches().unwrap();
        let current = concat_batches(&snapshot).unwrap();
        assert_eq!(names(&current), vec!["a", "bb", "c"]);

        let incremental = HudiSnapshotReader::open(dir.path())
            .with_query_type(HudiQueryType::Incremental)
            .with_begin_instant("20240101120000123-0123456789abcdef")
            .scan_batches()
            .unwrap();
        assert_eq!(names(&incremental[0]), vec!["bb", "c"]);
    }

    #[test]
    fn hudi_cow_upsert_rejects_missing_key() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        let err = writer.upsert_at(
            "20240101120000123-0123456789abcdef",
            "missing",
            batch(&[(1, "a")]),
        );
        assert!(matches!(err, Err(LakehouseError::Io(_))));
    }

    // ------------------------------------------------------------------
    // HudiQueryType tests
    // ------------------------------------------------------------------

    #[test]
    fn hudi_query_type_default_is_snapshot() {
        let qt = HudiQueryType::default();
        assert_eq!(qt, HudiQueryType::Snapshot);
    }

    #[test]
    fn hudi_query_type_variants_are_distinct() {
        assert_ne!(HudiQueryType::Snapshot, HudiQueryType::Incremental);
    }

    #[test]
    fn hudi_query_type_clone_eq() {
        let qt = HudiQueryType::Incremental;
        let cloned = qt;
        assert_eq!(qt, cloned);
    }

    #[test]
    fn hudi_query_type_debug_format() {
        assert_eq!(format!("{:?}", HudiQueryType::Snapshot), "Snapshot");
        assert_eq!(format!("{:?}", HudiQueryType::Incremental), "Incremental");
    }

    // ------------------------------------------------------------------
    // HudiCowWriter Debug / constructor tests
    // ------------------------------------------------------------------

    #[test]
    fn hudi_cow_writer_debug_format() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        let dbg = format!("{:?}", writer);
        assert!(dbg.contains("HudiCowWriter"));
    }

    #[test]
    fn hudi_cow_writer_open_creates_writer() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        assert!(dir.path().exists());
        let _ = writer;
    }

    // ------------------------------------------------------------------
    // validate_instant tests
    // ------------------------------------------------------------------

    #[test]
    fn validate_instant_rejects_empty() {
        assert!(validate_instant("").is_err());
    }

    #[test]
    fn validate_instant_rejects_special_chars() {
        assert!(validate_instant("2024/01/01").is_err());
        assert!(validate_instant("2024 01 01").is_err());
        assert!(validate_instant("abc@def").is_err());
    }

    #[test]
    fn validate_instant_accepts_valid() {
        // Canonical 34-char form: 17-digit ms timestamp + `-` + 16 hex chars
        assert!(validate_instant("20240101120000123-0123456789abcdef").is_ok());
        // Underscore separator (legacy tolerance)
        assert!(validate_instant("20240101120000123_0123456789abcdef").is_ok());
    }

    #[test]
    fn validate_instant_rejects_wrong_length() {
        assert!(validate_instant("20240101120000").is_err()); // 14 chars
        assert!(validate_instant("commit-123").is_err()); // 11 chars
        assert!(validate_instant("20240101120000123-0123456789abcde").is_err()); // 33 chars
    }

    #[test]
    fn validate_instant_rejects_bad_timestamp_prefix() {
        // Non-digit in timestamp portion
        assert!(validate_instant("2024010X120000123-0123456789abcdef").is_err());
    }

    #[test]
    fn validate_instant_rejects_bad_separator() {
        assert!(validate_instant("20240101120000123.0123456789abcdef").is_err());
        assert!(validate_instant("20240101120000123 0123456789abcdef").is_err());
    }

    #[test]
    fn validate_instant_rejects_non_hex_suffix() {
        // 'g' is not a hex digit
        assert!(validate_instant("20240101120000123-0123456789abcdeg").is_err());
        // uppercase hex is rejected (canonical format is lowercase)
        assert!(validate_instant("20240101120000123-0123456789ABCDEF").is_err());
    }

    // ------------------------------------------------------------------
    // deduplicate_by_key_last tests
    // ------------------------------------------------------------------

    #[test]
    fn deduplicate_by_key_last_keeps_last_occurrence() {
        let batch = batch(&[(1, "first"), (2, "second"), (1, "third")]);
        let deduped = deduplicate_by_key_last(&batch, "id").unwrap();
        assert_eq!(deduped.num_rows(), 2);
        let names = names(&deduped);
        // BTreeMap iterates in key order: key 1 -> "third", key 2 -> "second"
        assert_eq!(names, vec!["third", "second"]);
    }

    #[test]
    fn deduplicate_by_key_last_single_row() {
        let batch = batch(&[(42, "only")]);
        let deduped = deduplicate_by_key_last(&batch, "id").unwrap();
        assert_eq!(deduped.num_rows(), 1);
        assert_eq!(names(&deduped), vec!["only"]);
    }

    #[test]
    fn deduplicate_by_key_last_all_unique() {
        let batch = batch(&[(1, "a"), (2, "b"), (3, "c")]);
        let deduped = deduplicate_by_key_last(&batch, "id").unwrap();
        assert_eq!(deduped.num_rows(), 3);
    }

    #[test]
    fn deduplicate_by_key_last_rejects_missing_column() {
        let batch = batch(&[(1, "a")]);
        let err = deduplicate_by_key_last(&batch, "nonexistent");
        assert!(matches!(err, Err(LakehouseError::Io(_))));
    }

    // ------------------------------------------------------------------
    // HudiWriteResult tests
    // ------------------------------------------------------------------

    #[test]
    fn hudi_write_result_fields() {
        let result = HudiWriteResult {
            instant: "20240101120000123-0123456789abcdef".to_string(),
            rows_inserted: 5,
            rows_updated: 3,
            snapshot_rows: 10,
        };
        assert_eq!(result.instant, "20240101120000123-0123456789abcdef");
        assert_eq!(result.rows_inserted, 5);
        assert_eq!(result.rows_updated, 3);
        assert_eq!(result.snapshot_rows, 10);
    }

    #[test]
    fn hudi_write_result_eq() {
        let a = HudiWriteResult {
            instant: "t".to_string(),
            rows_inserted: 1,
            rows_updated: 2,
            snapshot_rows: 3,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    // ------------------------------------------------------------------
    // HudiSnapshotReader not-found errors
    // ------------------------------------------------------------------

    #[test]
    fn hudi_snapshot_reader_missing_timeline() {
        let dir = tempdir().unwrap();
        let reader = HudiSnapshotReader::open(dir.path());
        let err = reader.scan_batches();
        assert!(matches!(err, Err(LakehouseError::NotFound { .. })));
    }

    #[test]
    fn hudi_snapshot_reader_empty_timeline() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".hoodie/timeline")).unwrap();
        let reader = HudiSnapshotReader::open(dir.path());
        let batches = reader.scan_batches().unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn hudi_incremental_requires_begin_instant() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".hoodie/timeline")).unwrap();
        let reader =
            HudiSnapshotReader::open(dir.path()).with_query_type(HudiQueryType::Incremental);
        let err = reader.scan_batches();
        assert!(matches!(err, Err(LakehouseError::Io(_))));
    }

    #[test]
    fn hudi_cow_append_rejects_duplicate_instant() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        writer
            .append_at("20240101120000123-0123456789abcdef", batch(&[(1, "a")]))
            .unwrap();
        let err = writer.append_at("20240101120000123-0123456789abcdef", batch(&[(2, "b")]));
        assert!(matches!(err, Err(LakehouseError::Concurrency { .. })));
    }

    #[test]
    fn hudi_cow_upsert_rejects_null_key() {
        let dir = tempdir().unwrap();
        let writer = HudiCowWriter::open(dir.path());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let err = writer.upsert_at("20240101120000123-0123456789abcdef", "id", batch);
        assert!(matches!(err, Err(LakehouseError::Io(_))));
    }

    // ── ObjectStore-backed Hudi: round-trip tests ──────────────────────────

    fn make_inmemory_store() -> Arc<dyn object_store::ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    #[tokio::test]
    async fn hudi_object_store_write_then_read() {
        let store = make_inmemory_store();
        let writer = HudiObjectStoreWriter::new(Arc::clone(&store), "test/table");
        let input = batch(&[(1, "alice"), (2, "bob")]);
        let result = writer.append(input).await.unwrap();
        assert_eq!(result.rows_inserted, 2);

        let reader = HudiObjectStoreReader::new(Arc::clone(&store), "test/table");
        let batches = reader.scan_batches().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "object-store Hudi round-trip must return all written rows"
        );
    }

    #[tokio::test]
    async fn hudi_object_store_multiple_commits_readable() {
        let store = make_inmemory_store();
        let writer = HudiObjectStoreWriter::new(Arc::clone(&store), "multi/table");
        writer.append(batch(&[(1, "a")])).await.unwrap();
        writer.append(batch(&[(2, "b"), (3, "c")])).await.unwrap();

        let reader = HudiObjectStoreReader::new(Arc::clone(&store), "multi/table");
        let batches = reader.scan_batches().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "all rows across two commits must be readable"
        );
    }

    #[tokio::test]
    async fn hudi_object_store_empty_store_returns_empty() {
        let store = make_inmemory_store();
        let reader = HudiObjectStoreReader::new(Arc::clone(&store), "empty/table");
        let batches = reader.scan_batches().await.unwrap();
        assert!(
            batches.is_empty(),
            "empty object store must return no batches"
        );
    }

    #[tokio::test]
    async fn hudi_object_store_rapid_commits_are_independent_no_overwrite() {
        // next_instant() uses AtomicU64 compare-exchange for monotonicity, so two
        // rapid appends must produce distinct commit instants and both must be
        // readable — neither overwrites the other.
        let store = make_inmemory_store();
        let writer = HudiObjectStoreWriter::new(Arc::clone(&store), "mono/table");
        let r1 = writer.append(batch(&[(1, "first")])).await.unwrap();
        let r2 = writer.append(batch(&[(2, "second")])).await.unwrap();
        assert_ne!(
            r1.instant, r2.instant,
            "consecutive appends must produce distinct Hudi instants (monotonic clock)"
        );

        let reader = HudiObjectStoreReader::new(Arc::clone(&store), "mono/table");
        let batches = reader.scan_batches().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "both commits must be readable — no overwrite from same-ms instants"
        );
    }
}
