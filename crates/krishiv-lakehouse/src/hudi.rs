//! Apache Hudi snapshot and incremental readers (R18 S2).

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn hudi_incremental_returns_only_later_commit() {
        let dir = tempdir().unwrap();
        write_hudi_cow_fixture(
            dir.path(),
            &[
                ("20240101120000", &[(1, "a")]),
                ("20240102120000", &[(2, "b")]),
            ],
        )
        .unwrap();
        let reader = HudiSnapshotReader::open(dir.path())
            .with_query_type(HudiQueryType::Incremental)
            .with_begin_instant("20240101120000");
        let batches = reader.scan_batches().unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
    }
}
