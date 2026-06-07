//! Local Delta Lake table I/O (Parquet + `_delta_log` JSON commits).
//!
//! Avoids linking `deltalake` against workspace Arrow 58 (deltalake pins Arrow 57).

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
    Ok(active_data_file_paths(root, max_version)?
        .into_iter()
        .map(|rel| root.join(rel))
        .collect())
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
    let mut removed_paths: Vec<String> = Vec::new();
    if overwrite {
        removed_paths = active_data_file_paths(root, None)?;
    }
    let version = next_version(root)?;
    let file_name = format!("part-{version:05}.parquet");
    let file_path = root.join(&file_name);
    let schema = batches[0].schema();
    let f = File::create(&file_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let mut writer =
        ArrowWriter::try_new(f, schema, None).map_err(|e| LakehouseError::Io(e.to_string()))?;
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

    // Write commit log atomically: write to .tmp, fsync, rename.
    let commit = json!({"commitInfo":{"operation":"WRITE","timestamp":chrono::Utc::now().timestamp_millis()}});
    let add = json!({"add":{"path":file_name,"size":meta.len(),"dataChange":true}});
    {
        let mut log = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_log_path)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        writeln!(log, "{commit}").map_err(|e| LakehouseError::Io(e.to_string()))?;
        // S6: Emit remove actions for every file deleted during overwrite.
        for removed in &removed_paths {
            let remove = json!({"remove":{"path":removed,"dataChange":true,"deletionTimestamp":chrono::Utc::now().timestamp_millis()}});
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
}
