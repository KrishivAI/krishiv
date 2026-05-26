//! Local Delta Lake table I/O (Parquet + `_delta_log` JSON commits).
//!
//! Avoids linking `deltalake` against workspace Arrow 58 (deltalake pins Arrow 57).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

use crate::{LakehouseError, LakehouseResult};

fn delta_log_dir(root: &Path) -> PathBuf {
    root.join("_delta_log")
}

fn next_version(root: &Path) -> LakehouseResult<u64> {
    let dir = delta_log_dir(root);
    fs::create_dir_all(&dir).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let mut max = 0u64;
    for entry in fs::read_dir(&dir).map_err(|e| LakehouseError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(stem) = name.strip_suffix(".json")
            && let Ok(v) = stem.parse::<u64>()
        {
            max = max.max(v);
        }
    }
    Ok(max + 1)
}

fn list_data_files(root: &Path, max_version: Option<u64>) -> LakehouseResult<Vec<PathBuf>> {
    let dir = delta_log_dir(root);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
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
                files.push(root.join(rel));
            }
        }
    }
    Ok(files)
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
    if overwrite && root.exists() {
        for entry in fs::read_dir(root).map_err(|e| LakehouseError::Io(e.to_string()))? {
            let entry = entry.map_err(|e| LakehouseError::Io(e.to_string()))?;
            let p = entry.path();
            if p.extension().is_some_and(|e| e == "parquet") {
                fs::remove_file(p).ok();
            }
        }
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
    writer
        .close()
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    let meta = fs::metadata(&file_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
    let log_path = delta_log_dir(root).join(format!("{version:020}.json"));
    let mut log = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&log_path)
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    let commit = json!({"commitInfo":{"operation":"WRITE","timestamp":chrono::Utc::now().timestamp_millis()}});
    writeln!(log, "{commit}").map_err(|e| LakehouseError::Io(e.to_string()))?;
    let add = json!({"add":{"path":file_name,"size":meta.len(),"dataChange":true}});
    writeln!(log, "{add}").map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(())
}

pub fn table_schema(path: &str) -> LakehouseResult<SchemaRef> {
    let batches = read_table(path, None)?;
    Ok(batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty())))
}
