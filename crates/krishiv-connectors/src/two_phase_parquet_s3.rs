//! Two-phase Parquet commit with staging directory (R16 S5.2).

use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;

use crate::{ConnectorError, ConnectorResult, TwoPhaseCommitSink};

fn io_err(context: &str, e: std::io::Error) -> ConnectorError {
    ConnectorError::IoStr {
        message: format!("{context}: {e}"),
    }
}

/// Handle identifying a staged Parquet object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetStagingHandle {
    id: u64,
    path: PathBuf,
}

/// Stages Parquet under `{base}/_staging/{epoch}/` then commits to `{base}/data/`.
#[derive(Debug)]
pub struct TwoPhaseParquetSink {
    base_dir: PathBuf,
    epoch: u64,
    next_id: u64,
    staged_paths: std::collections::BTreeMap<u64, PathBuf>,
}

impl TwoPhaseParquetSink {
    pub fn new(base_dir: impl AsRef<Path>, epoch: u64) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            epoch,
            next_id: 0,
            staged_paths: std::collections::BTreeMap::new(),
        }
    }

    fn staging_dir(&self) -> PathBuf {
        self.base_dir.join("_staging").join(self.epoch.to_string())
    }

    fn final_dir(&self) -> PathBuf {
        self.base_dir.join("data")
    }

    /// Recovery: commit or abort orphaned staging from a crashed epoch.
    pub fn recover_orphan_staging(
        base_dir: &Path,
        epoch: u64,
        commit: bool,
    ) -> ConnectorResult<()> {
        let staging = base_dir.join("_staging").join(epoch.to_string());
        if !staging.exists() {
            return Ok(());
        }
        if commit {
            std::fs::create_dir_all(base_dir.join("data"))
                .map_err(|e| io_err("create data dir", e))?;
            for entry in std::fs::read_dir(&staging).map_err(|e| io_err("read staging dir", e))? {
                let entry = entry.map_err(|e| io_err("read staging entry", e))?;
                let dest = base_dir.join("data").join(entry.file_name());
                // On POSIX rename() atomically replaces the destination; deleting
                // first creates a window where the file is missing and can lose
                // data if a reader accesses the path during the gap.
                std::fs::rename(entry.path(), dest).map_err(|e| io_err("commit staged file", e))?;
            }
        }
        std::fs::remove_dir_all(&staging).map_err(|e| io_err("remove staging dir", e))?;
        Ok(())
    }
}

impl TwoPhaseCommitSink for TwoPhaseParquetSink {
    type Handle = ParquetStagingHandle;

    fn prepare(&mut self, epoch: u64, batch: &RecordBatch) -> ConnectorResult<Self::Handle> {
        if epoch != self.epoch {
            return Err(ConnectorError::IoStr {
                message: "prepare epoch mismatch".into(),
            });
        }
        std::fs::create_dir_all(self.staging_dir()).map_err(|e| io_err("create staging dir", e))?;
        let id = self.next_id;
        self.next_id += 1;
        let path = self.staging_dir().join(format!("part-{id}.parquet"));
        let file = std::fs::File::create(&path).map_err(|e| io_err("create staged parquet", e))?;
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).map_err(|e| {
                ConnectorError::IoStr {
                    message: format!("parquet writer: {e}"),
                }
            })?;
        writer.write(batch).map_err(|e| ConnectorError::IoStr {
            message: format!("parquet write: {e}"),
        })?;
        writer.close().map_err(|e| ConnectorError::IoStr {
            message: format!("parquet close: {e}"),
        })?;
        self.staged_paths.insert(id, path.clone());
        Ok(ParquetStagingHandle { id, path })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        std::fs::create_dir_all(self.final_dir()).map_err(|e| io_err("create final dir", e))?;
        let name = handle
            .path
            .file_name()
            .ok_or_else(|| ConnectorError::IoStr {
                message: "invalid staged path".into(),
            })?;
        let dest = self.final_dir().join(name);
        // rename is atomic on POSIX — single metadata operation.
        std::fs::rename(&handle.path, &dest).map_err(|e| io_err("commit staged parquet", e))?;
        self.staged_paths.remove(&handle.id);
        Ok(())
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        std::fs::remove_file(&handle.path)
            .map_err(|e| io_err("remove staging file on abort", e))?;
        self.staged_paths.remove(&handle.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap()
    }

    #[test]
    fn s3_2pc_exactly_once_crash_recovery_commits_staged() {
        let dir = tempdir().unwrap();
        let mut sink = TwoPhaseParquetSink::new(dir.path(), 1);
        let h = sink.prepare(1, &batch()).unwrap();
        // simulate crash before commit
        drop(h);
        TwoPhaseParquetSink::recover_orphan_staging(dir.path(), 1, true).unwrap();
        assert!(!dir.path().join("_staging").join("1").exists());
        assert!(std::fs::read_dir(dir.path().join("data")).unwrap().count() >= 1);
    }

    #[test]
    fn s3_2pc_prepare_commit_round_trip() {
        let dir = tempdir().unwrap();
        let mut sink = TwoPhaseParquetSink::new(dir.path(), 1);
        let h = sink.prepare(1, &batch()).unwrap();
        sink.commit(h).unwrap();
        assert!(dir.path().join("data").join("part-0.parquet").exists());
    }
}
