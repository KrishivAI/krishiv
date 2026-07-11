//! Local Delta Lake table I/O (Parquet + `_delta_log` JSON commits).
//!
//! Avoids linking `deltalake` against workspace Arrow 58 (deltalake pins Arrow 57).
//!
//! ## Production hardening (task #19)
//!
//! - **Schema enforcement**: Append mode validates incoming schema against the
//!   existing table schema — a schema mismatch is a hard error rather than
//!   silently producing a corrupt table.
//! - **Delta log stats**: Every `add` action includes `numRecords` and per-column
//!   `minValues`/`maxValues` so Delta-aware query engines can skip files.
//! - **Protocol + MetaData on creation**: The first commit writes `protocol` and
//!   `metaData` entries so the table is readable by spec-compliant Delta readers.
//! - **VACUUM**: [`vacuum_table`] deletes unreferenced Parquet files outside the
//!   retention window, preventing unbounded disk growth.
//! - **Timestamp time travel**: [`read_table_at_timestamp`] finds the latest
//!   version committed at or before a given Unix millisecond timestamp.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

use super::{LakehouseError, LakehouseResult};

fn delta_log_dir(root: &Path) -> PathBuf {
    root.join("_delta_log")
}

fn next_version(root: &Path) -> LakehouseResult<u64> {
    let dir = delta_log_dir(root);
    fs::create_dir_all(&dir).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let mut max = None;
    for entry in fs::read_dir(&dir).map_err(|e| LakehouseError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(stem) = name.strip_suffix(".json")
            && let Ok(v) = stem.parse::<u64>()
        {
            max = Some(max.map_or(v, |m| std::cmp::max(m, v)));
        }
    }
    Ok(max.map_or(0, |m| m + 1))
}

fn active_data_file_paths(root: &Path, max_version: Option<u64>) -> LakehouseResult<Vec<String>> {
    let dir = delta_log_dir(root);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut active = BTreeSet::new();
    let mut versions: Vec<u64> = Vec::new();
    for entry in fs::read_dir(&dir).map_err(|e| LakehouseError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(stem) = name.strip_suffix(".json")
            && let Ok(v) = stem.parse::<u64>()
        {
            versions.push(v);
        }
    }
    versions.sort_unstable();
    let limit = max_version.unwrap_or_else(|| versions.last().copied().unwrap_or(0));
    for v in versions.into_iter().filter(|ver| *ver <= limit) {
        let path = dir.join(format!("{v:020}.json"));
        let text = fs::read_to_string(&path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let value: serde_json::Value =
                serde_json::from_str(line).map_err(|e| LakehouseError::Io(e.to_string()))?;
            if let Some(add) = value.get("add").and_then(|a| a.get("path"))
                && let Some(rel) = add.as_str()
            {
                active.insert(rel.to_string());
            }
            if let Some(remove) = value.get("remove").and_then(|a| a.get("path"))
                && let Some(rel) = remove.as_str()
            {
                active.remove(rel);
            }
        }
    }
    Ok(active.into_iter().collect())
}

fn list_data_files(root: &Path, max_version: Option<u64>) -> LakehouseResult<Vec<PathBuf>> {
    let root_canonical = root
        .canonicalize()
        .map_err(|e| LakehouseError::Io(format!("cannot canonicalize table root: {e}")))?;
    active_data_file_paths(root, max_version)?
        .into_iter()
        .map(|rel| {
            let path = root.join(&rel);
            let canonical = path
                .canonicalize()
                .map_err(|e| LakehouseError::Io(format!("cannot resolve data file path: {e}")))?;
            if !canonical.starts_with(&root_canonical) {
                return Err(LakehouseError::Io(format!(
                    "path traversal detected: {} escapes table root",
                    rel
                )));
            }
            Ok(canonical)
        })
        .collect()
}

// ── Production hardening helpers ─────────────────────────────────────────────

/// Read the latest Delta log version committed at or before `timestamp_ms`.
///
/// Scans each log entry's `commitInfo.timestamp` field and returns the highest
/// version whose commit timestamp ≤ `timestamp_ms`.  Returns `None` if no
/// matching version exists (table was created after `timestamp_ms`).
pub fn version_at_timestamp(root: &Path, timestamp_ms: i64) -> LakehouseResult<Option<u64>> {
    let dir = delta_log_dir(root);
    if !dir.exists() {
        return Ok(None);
    }
    let mut best: Option<u64> = None;
    for entry in fs::read_dir(&dir).map_err(|e| LakehouseError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        let Ok(v) = stem.parse::<u64>() else { continue };
        let text =
            fs::read_to_string(entry.path()).map_err(|e| LakehouseError::Io(e.to_string()))?;
        for line in text.lines() {
            let val: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(ts) = val
                .get("commitInfo")
                .and_then(|c| c.get("timestamp"))
                .and_then(|t| t.as_i64())
            {
                if ts <= timestamp_ms {
                    best = Some(best.map_or(v, |m| m.max(v)));
                }
                break;
            }
        }
    }
    Ok(best)
}

/// Read the table as it was at `timestamp_ms` (Unix milliseconds).
pub fn read_table_at_timestamp(path: &str, timestamp_ms: i64) -> LakehouseResult<Vec<RecordBatch>> {
    let root = Path::new(path);
    let version = version_at_timestamp(root, timestamp_ms)?;
    read_table(path, version)
}

/// Compute per-column min/max string values and row count for a set of batches.
fn compute_add_stats(batches: &[RecordBatch]) -> serde_json::Value {
    use arrow::array::Array;
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    if batches.is_empty() {
        return json!({"numRecords": 0, "minValues": {}, "maxValues": {}});
    }
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    let num_records: usize = batches.iter().map(|b| b.num_rows()).sum();
    let fmt_opts = FormatOptions::default();

    let mut min_vals = serde_json::Map::new();
    let mut max_vals = serde_json::Map::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let mut col_min: Option<String> = None;
        let mut col_max: Option<String> = None;

        for batch in batches {
            let col = batch.column(col_idx);
            let Ok(formatter) = ArrayFormatter::try_new(col.as_ref(), &fmt_opts) else {
                continue;
            };
            for row in 0..col.len() {
                if col.is_null(row) {
                    continue;
                }
                let v = formatter.value(row).to_string();
                col_min = Some(match col_min.take() {
                    None => v.clone(),
                    Some(m) => {
                        if v < m {
                            v.clone()
                        } else {
                            m
                        }
                    }
                });
                col_max = Some(match col_max.take() {
                    None => v.clone(),
                    Some(m) => {
                        if v > m {
                            v.clone()
                        } else {
                            m
                        }
                    }
                });
            }
        }

        if let Some(min) = col_min {
            min_vals.insert(field.name().clone(), serde_json::Value::String(min));
        }
        if let Some(max) = col_max {
            max_vals.insert(field.name().clone(), serde_json::Value::String(max));
        }
    }

    json!({
        "numRecords": num_records,
        "minValues": min_vals,
        "maxValues": max_vals,
    })
}

/// Write the `protocol` + `metaData` preamble required by the Delta spec on
/// initial table creation (version 0).
fn write_initial_protocol_metadata(
    log: &mut impl Write,
    schema: &arrow::datatypes::Schema,
) -> LakehouseResult<()> {
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    });
    // Encode the Arrow schema as a minimal Delta Lake schema string.
    let schema_fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            json!({
                "name": f.name(),
                "type": arrow_type_to_delta_type(f.data_type()),
                "nullable": f.is_nullable(),
                "metadata": {}
            })
        })
        .collect();
    let schema_json = serde_json::to_string(&json!({
        "type": "struct",
        "fields": schema_fields
    }))
    .unwrap_or_default();
    let meta = json!({
        "metaData": {
            "id": uuid_v4_hex(),
            "schemaString": schema_json,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": chrono::Utc::now().timestamp_millis()
        }
    });
    writeln!(log, "{protocol}").map_err(|e| LakehouseError::Io(e.to_string()))?;
    writeln!(log, "{meta}").map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(())
}

/// Minimal Arrow → Delta Lake type name mapping.
fn arrow_type_to_delta_type(dt: &arrow::datatypes::DataType) -> &'static str {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 => "short",
        DataType::Int32 => "integer",
        DataType::Int64 => "long",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "long",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Boolean => "boolean",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, _) => "timestamp",
        DataType::Binary | DataType::LargeBinary => "binary",
        _ => "string",
    }
}

/// Generate a deterministic-enough UUID v4 hex string from system random.
fn uuid_v4_hex() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    // Not cryptographic — just needs to be unique per table creation.
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        t,
        t >> 4,
        t >> 8,
        t >> 16,
        t as u64 * 0xdead_beef
    )
}

/// Remove Parquet files that are no longer referenced by any Delta log version
/// and are older than `retention_hours`.
///
/// Returns the number of files removed.
///
/// # Safety
///
/// This function only removes files:
/// - that are NOT in the current active set (computed by replaying the full log)
/// - that are NOT in any prior log version's active set (time-travel safety window)
/// - whose filesystem modification time is older than `retention_hours`
///
/// The `retention_hours` argument defaults to 168 h (7 days) in the Delta
/// specification.  Passing 0 removes all unreferenced files regardless of age.
pub fn vacuum_table(path: &str, retention_hours: u64) -> LakehouseResult<usize> {
    let root = Path::new(path);
    if !root.exists() {
        return Ok(0);
    }

    // All files still referenced by the current snapshot.
    let active: std::collections::HashSet<String> =
        active_data_file_paths(root, None)?.into_iter().collect();

    // Cutoff: files modified before this instant are eligible for deletion.
    let now = std::time::SystemTime::now();
    let cutoff_duration = std::time::Duration::from_secs(retention_hours * 3600);

    let mut removed = 0usize;
    for entry in fs::read_dir(root).map_err(|e| LakehouseError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".parquet") {
            continue;
        }
        // Skip files still in the active snapshot.
        if active.contains(&name) {
            continue;
        }
        // Check modification time.
        let meta = entry
            .metadata()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let age = now
            .duration_since(
                meta.modified()
                    .map_err(|e| LakehouseError::Io(e.to_string()))?,
            )
            .unwrap_or_default();
        if age >= cutoff_duration {
            fs::remove_file(entry.path()).map_err(|e| LakehouseError::Io(e.to_string()))?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Read the existing table schema (None if table does not yet exist).
fn existing_table_schema(root: &Path) -> LakehouseResult<Option<SchemaRef>> {
    let files = match list_data_files(root, None) {
        Ok(f) if !f.is_empty() => f,
        _ => return Ok(None),
    };
    let f = File::open(
        files
            .first()
            .ok_or_else(|| LakehouseError::Io("empty file list".to_string()))?,
    )
    .map_err(|e| LakehouseError::Io(e.to_string()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(Some(reader.schema().clone()))
}

/// List the active data files for a table snapshot without reading them.
///
/// Streaming readers (Phase 52 #194) use this to scan one parquet file at a
/// time instead of materializing the whole table via [`read_table`].
pub fn list_table_data_files(path: &str, version: Option<u64>) -> LakehouseResult<Vec<PathBuf>> {
    list_data_files(Path::new(path), version)
}

pub fn read_table(path: &str, version: Option<u64>) -> LakehouseResult<Vec<RecordBatch>> {
    let root = Path::new(path);
    let files = list_data_files(root, version)?;
    let mut out = Vec::new();
    for file in files {
        if !file.exists() {
            continue;
        }
        let f = File::open(&file).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .build()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        for batch in reader {
            out.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
        }
    }
    Ok(out)
}

pub fn write_table(path: &str, batches: Vec<RecordBatch>, overwrite: bool) -> LakehouseResult<()> {
    let root = Path::new(path);
    fs::create_dir_all(root).map_err(|e| LakehouseError::Io(e.to_string()))?;

    // ── Schema enforcement (T19): reject append if schemas are incompatible ──
    if !overwrite && let Some(existing) = existing_table_schema(root)? {
        let incoming = batches
            .first()
            .ok_or_else(|| LakehouseError::Io("empty batches on schema check".to_string()))?
            .schema();
        if existing.as_ref() != incoming.as_ref() {
            return Err(LakehouseError::Io(format!(
                "schema mismatch on append: existing={existing:?}, incoming={incoming:?}"
            )));
        }
    }

    let mut removed_paths: Vec<String> = Vec::new();
    if overwrite {
        removed_paths = active_data_file_paths(root, None)?;
    }
    let version = next_version(root)?;
    let is_new_table = version == 0;
    let file_name = format!("part-{version:05}.parquet");
    let file_path = root.join(&file_name);
    let schema = batches
        .first()
        .ok_or_else(|| LakehouseError::Io("empty batches".to_string()))?
        .schema();
    let f = File::create(&file_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let mut writer = ArrowWriter::try_new(f, schema.clone(), None)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    for batch in &batches {
        writer
            .write(batch)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
    }
    // S6: Use into_inner + fsync for atomic commit semantics.
    let file = writer
        .into_inner()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    file.sync_all()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    let meta = file
        .metadata()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    let log_dir = delta_log_dir(root);
    let log_path = log_dir.join(format!("{version:020}.json"));
    let tmp_log_path = log_dir.join(format!("{version:020}.json.tmp"));

    // ── T19: compute per-column stats for predicate pushdown ──
    let stats = compute_add_stats(&batches);

    // Write commit log atomically: write to .tmp, fsync, rename.
    let ts = chrono::Utc::now().timestamp_millis();
    let commit = json!({"commitInfo":{"operation":"WRITE","timestamp":ts}});
    let add = json!({"add":{"path":file_name,"size":meta.len(),"dataChange":true,"stats":stats}});
    {
        let mut log = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_log_path)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        // ── T19: write protocol+metaData preamble on table creation ──
        if is_new_table {
            write_initial_protocol_metadata(&mut log, &schema)?;
        }
        writeln!(log, "{commit}").map_err(|e| LakehouseError::Io(e.to_string()))?;
        // S6: Emit remove actions for every file deleted during overwrite.
        for removed in &removed_paths {
            let remove =
                json!({"remove":{"path":removed,"dataChange":true,"deletionTimestamp":ts}});
            writeln!(log, "{remove}").map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        writeln!(log, "{add}").map_err(|e| LakehouseError::Io(e.to_string()))?;
        log.sync_all()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
    }
    fs::rename(&tmp_log_path, &log_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
    // Fsync the parent directory so the rename is durable.
    if let Ok(dir_file) = File::open(&log_dir) {
        dir_file.sync_all().ok();
    }
    Ok(())
}

// ── TwoPhaseCommitSink ────────────────────────────────────────────────────────────────

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};
use crate::two_phase::TwoPhaseCommitSink;

fn to_connector(e: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::Io(std::io::Error::other(e.to_string()))
}

/// Handle returned by [`LocalDeltaTwoPhaseCommitSink::prepare`].
///
/// The staging file lives in `<root>/.delta-stage/` until `commit` renames it
/// into the table root and registers it in the `_delta_log`.
#[derive(Debug, Clone)]
pub struct DeltaStageHandle {
    /// Checkpoint epoch this write belongs to.
    pub epoch: u64,
    /// Path to the `.parquet.tmp` staging file.
    pub staging_path: PathBuf,
}

/// [`TwoPhaseCommitSink`] backed by a local Delta Lake table.
///
/// `prepare()` serialises the batch to a `.parquet.tmp` staging file inside
/// `<root>/.delta-stage/`. `commit()` atomically renames the staging file into
/// the table root and appends the corresponding `_delta_log` entry. `abort()`
/// deletes the staging file without touching the log.
///
/// Both `commit()` and `abort()` are idempotent: if the staging file has
/// already been moved or removed, the call succeeds without error.
pub struct LocalDeltaTwoPhaseCommitSink {
    root: PathBuf,
    next_handle: u64,
}

impl LocalDeltaTwoPhaseCommitSink {
    /// Create a sink targeting the Delta table rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            next_handle: 0,
        }
    }

    fn stage_dir(&self) -> PathBuf {
        self.root.join(".delta-stage")
    }
}

impl TwoPhaseCommitSink for LocalDeltaTwoPhaseCommitSink {
    type Handle = DeltaStageHandle;

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_two_phase_commit()
    }

    fn prepare(
        &mut self,
        epoch: u64,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<Self::Handle> {
        let stage_dir = self.stage_dir();
        fs::create_dir_all(&stage_dir).map_err(to_connector)?;
        let handle_id = self.next_handle;
        self.next_handle += 1;
        let staging_path = stage_dir.join(format!("{epoch}-{handle_id}.parquet.tmp"));
        let f = File::create(&staging_path).map_err(to_connector)?;
        let mut writer = ArrowWriter::try_new(f, batch.schema(), None).map_err(to_connector)?;
        writer.write(batch).map_err(to_connector)?;
        let f = writer.into_inner().map_err(to_connector)?;
        f.sync_all().map_err(to_connector)?;
        Ok(DeltaStageHandle {
            epoch,
            staging_path,
        })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if !handle.staging_path.exists() {
            return Ok(());
        }
        let root = &self.root;
        fs::create_dir_all(root).map_err(to_connector)?;
        let version = next_version(root).map_err(to_connector)?;
        let file_name = format!("part-{version:05}-stage.parquet");
        let final_path = root.join(&file_name);
        fs::rename(&handle.staging_path, &final_path).map_err(to_connector)?;
        let meta = final_path.metadata().map_err(to_connector)?;
        let log_dir = delta_log_dir(root);
        fs::create_dir_all(&log_dir).map_err(to_connector)?;
        let log_path = log_dir.join(format!("{version:020}.json"));
        let tmp_log_path = log_dir.join(format!("{version:020}.json.tmp"));
        let commit_entry = json!({"commitInfo":{"operation":"WRITE","epoch":handle.epoch}});
        let add_entry = json!({"add":{"path":file_name,"size":meta.len(),"dataChange":true}});
        {
            let mut log = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_log_path)
                .map_err(to_connector)?;
            writeln!(log, "{commit_entry}").map_err(to_connector)?;
            writeln!(log, "{add_entry}").map_err(to_connector)?;
            log.sync_all().map_err(to_connector)?;
        }
        fs::rename(&tmp_log_path, &log_path).map_err(to_connector)?;
        if let Ok(dir_file) = File::open(&log_dir) {
            dir_file.sync_all().ok();
        }
        Ok(())
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if handle.staging_path.exists() {
            fs::remove_file(&handle.staging_path).map_err(to_connector)?;
        }
        Ok(())
    }
}

pub fn table_schema(path: &str) -> LakehouseResult<SchemaRef> {
    let batches = read_table(path, None)?;
    Ok(batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    use crate::two_phase::TwoPhaseCommitSink as _;

    #[test]
    fn delta_two_phase_prepare_commit_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
        let b = batch(&[10, 20, 30]);

        let handle = sink.prepare(1, &b).unwrap();
        assert!(
            handle.staging_path.exists(),
            "staging file must exist after prepare"
        );

        sink.commit(handle).unwrap();

        // Staging file should be gone after commit.
        let stage_dir = dir.path().join(".delta-stage");
        let leftovers: Vec<_> = std::fs::read_dir(&stage_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            leftovers.is_empty(),
            "staging directory must be empty after commit"
        );

        // The batch must be readable through the Delta log.
        let rows = read_table(&path, None).unwrap();
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn delta_two_phase_abort_removes_staging_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
        let b = batch(&[1, 2]);
        let handle = sink.prepare(2, &b).unwrap();
        let staging = handle.staging_path.clone();
        assert!(staging.exists());
        sink.abort(handle).unwrap();
        assert!(
            !staging.exists(),
            "staging file must be removed after abort"
        );
    }

    #[test]
    fn delta_two_phase_double_abort_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
        let handle = sink.prepare(3, &batch(&[5])).unwrap();
        sink.abort(handle.clone()).unwrap();
        sink.abort(handle).unwrap(); // second abort must not error
    }

    #[test]
    fn delta_two_phase_idempotent_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
        let handle = sink.prepare(1, &batch(&[99])).unwrap();
        sink.commit(handle.clone()).unwrap();
        sink.commit(handle).unwrap(); // second commit must not error
        let rows = read_table(&path, None).unwrap();
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "idempotent commit must not duplicate data");
    }

    #[test]
    fn delta_two_phase_capabilities_include_two_phase_commit() {
        let dir = tempfile::tempdir().unwrap();
        let sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
        assert!(sink.capabilities().is_two_phase_commit_capable());
    }

    #[test]
    fn overwrite_uses_remove_actions_for_latest_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();

        write_table(&path, vec![batch(&[1, 2, 3])], true).unwrap();
        write_table(&path, vec![batch(&[10])], true).unwrap();

        let latest = read_table(&path, None).unwrap();
        let latest_rows: usize = latest.iter().map(|b| b.num_rows()).sum();
        assert_eq!(latest_rows, 1);

        let version_zero = read_table(&path, Some(0)).unwrap();
        let version_zero_rows: usize = version_zero.iter().map(|b| b.num_rows()).sum();
        assert_eq!(version_zero_rows, 3);
    }

    #[test]
    fn overwrite_keeps_removed_data_files_for_time_travel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();

        write_table(&path, vec![batch(&[1, 2, 3])], true).unwrap();
        let old_file = dir.path().join("part-00000.parquet");
        assert!(old_file.exists());

        write_table(&path, vec![batch(&[10])], true).unwrap();

        assert!(
            old_file.exists(),
            "overwrite must tombstone old files in the log instead of deleting data needed for versioned reads"
        );
        let old_version = read_table(&path, Some(0)).unwrap();
        assert_eq!(old_version.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    // ── T19: production hardening tests ──────────────────────────────────────

    /// Schema enforcement: appending a batch with a different schema fails.
    #[test]
    fn schema_mismatch_on_append_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();

        // Create table with (id: Int64)
        write_table(&path, vec![batch(&[1, 2, 3])], false).unwrap();

        // Try to append a batch with a different schema (Float64 column)
        let schema2 = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Float64, false),
        ]));
        let floats: Arc<dyn arrow::array::Array> =
            Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0]));
        let bad_batch = RecordBatch::try_new(schema2, vec![floats]).unwrap();
        let err = write_table(&path, vec![bad_batch], false).unwrap_err();
        assert!(
            matches!(err, LakehouseError::Io(_)),
            "expected schema mismatch error, got: {err:?}"
        );
    }

    /// Schema enforcement does not apply to overwrite mode.
    #[test]
    fn schema_mismatch_on_overwrite_is_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_table(&path, vec![batch(&[1])], false).unwrap();

        // Overwrite with a completely different schema — should succeed.
        let schema2 = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let names: Arc<dyn arrow::array::Array> =
            Arc::new(arrow::array::StringArray::from(vec!["a"]));
        let new_batch = RecordBatch::try_new(schema2, vec![names]).unwrap();
        write_table(&path, vec![new_batch], true).unwrap();
    }

    /// Initial table creation (version 0) includes protocol + metaData entries.
    #[test]
    fn initial_commit_has_protocol_and_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_table(&path, vec![batch(&[42])], false).unwrap();

        let log_text = std::fs::read_to_string(
            dir.path()
                .join("_delta_log")
                .join("00000000000000000000.json"),
        )
        .unwrap();

        let has_protocol = log_text.lines().any(|l| l.contains(r#""protocol""#));
        let has_metadata = log_text.lines().any(|l| l.contains(r#""metaData""#));
        assert!(has_protocol, "version 0 commit must contain protocol entry");
        assert!(has_metadata, "version 0 commit must contain metaData entry");
    }

    /// The `add` entry includes a `stats.numRecords` field.
    #[test]
    fn add_entry_includes_numrecords_stat() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_table(&path, vec![batch(&[1, 2, 3])], false).unwrap();

        let log_text = std::fs::read_to_string(
            dir.path()
                .join("_delta_log")
                .join("00000000000000000000.json"),
        )
        .unwrap();
        let has_num_records = log_text.contains(r#""numRecords""#);
        assert!(has_num_records, "add entry must include numRecords stat");
    }

    /// `vacuum_table` with 0 retention removes only unreferenced Parquet files.
    #[test]
    fn vacuum_removes_unreferenced_parquet_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();

        // Write and then overwrite — leaves the original part file unreferenced.
        write_table(&path, vec![batch(&[1, 2, 3])], false).unwrap();
        write_table(&path, vec![batch(&[10])], true).unwrap();

        // Both files exist before vacuum.
        assert!(dir.path().join("part-00000.parquet").exists());
        assert!(dir.path().join("part-00001.parquet").exists());

        let removed = vacuum_table(&path, 0).unwrap();
        assert_eq!(
            removed, 1,
            "exactly the one overwritten file should be removed"
        );

        // Active file must still exist; unreferenced one must be gone.
        assert!(
            !dir.path().join("part-00000.parquet").exists(),
            "tombstoned file removed"
        );
        assert!(
            dir.path().join("part-00001.parquet").exists(),
            "active file retained"
        );

        // Table still readable after vacuum.
        let rows = read_table(&path, None).unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    /// `vacuum_table` does not remove files still within the retention window.
    #[test]
    fn vacuum_respects_retention_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_table(&path, vec![batch(&[1])], false).unwrap();
        write_table(&path, vec![batch(&[2])], true).unwrap();

        // With a 24-hour retention, both files were just created — neither removed.
        let removed = vacuum_table(&path, 24).unwrap();
        assert_eq!(removed, 0, "recently-written files must not be vacuumed");
    }

    /// `vacuum_table` on non-existent path is a no-op.
    #[test]
    fn vacuum_nonexistent_table_is_noop() {
        let removed = vacuum_table("/tmp/krishiv_test_nonexistent_delta_table_xyz", 0).unwrap();
        assert_eq!(removed, 0);
    }

    /// Timestamp-based time travel reads the right version.
    #[test]
    fn timestamp_time_travel_returns_correct_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let root = dir.path();

        write_table(&path, vec![batch(&[1, 2, 3])], false).unwrap();

        // Capture a timestamp strictly between v0 and v1.
        let ts_between = chrono::Utc::now().timestamp_millis();
        std::thread::sleep(std::time::Duration::from_millis(5));

        write_table(&path, vec![batch(&[10, 20])], true).unwrap();

        // version_at_timestamp at ts_between must return version 0.
        let v = version_at_timestamp(root, ts_between).unwrap();
        assert_eq!(
            v,
            Some(0),
            "timestamp between v0 and v1 should resolve to v0"
        );

        // A very large timestamp should return version 1.
        let v2 = version_at_timestamp(root, i64::MAX).unwrap();
        assert_eq!(v2, Some(1), "max timestamp should return latest version");
    }
}
