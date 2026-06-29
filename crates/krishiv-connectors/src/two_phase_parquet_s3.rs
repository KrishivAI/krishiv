//! Two-phase Parquet commit with staging directory (R16 S5.2).

use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;

use crate::{ConnectorCapabilities, ConnectorError, ConnectorResult, TwoPhaseCommitSink};

fn io_err(context: &str, e: std::io::Error) -> ConnectorError {
    ConnectorError::Io(std::io::Error::new(e.kind(), format!("{context}: {e}")))
}

fn publish_staged_file(staging_path: &Path, final_path: &Path) -> ConnectorResult<()> {
    // Use `rename` instead of `hard_link` to avoid cross-filesystem failures.
    // `rename` is atomic on the same filesystem; cross-fs moves require explicit
    // copy+delete which is not appropriate for a two-phase commit protocol.
    match std::fs::rename(staging_path, final_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && final_path.exists() => Ok(()),
        Err(error) => Err(io_err("publish staged parquet without replacement", error)),
    }
}

/// Handle identifying a staged Parquet object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetStagingHandle {
    id: u64,
    staging_path: PathBuf,
    final_path: PathBuf,
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
                if !entry
                    .file_type()
                    .map_err(|e| io_err("read staging entry type", e))?
                    .is_file()
                {
                    return Err(ConnectorError::Parquet(format!(
                        "orphan staging contains non-file entry: {:?}",
                        entry.path()
                    )));
                }
                let staging_name = entry.file_name().into_string().map_err(|name| {
                    ConnectorError::Parquet(format!("non-UTF-8 staging file name: {name:?}"))
                })?;
                let final_path = base_dir
                    .join("data")
                    .join(format!("epoch-{epoch}-{staging_name}"));
                publish_staged_file(&entry.path(), &final_path)?;
            }
        }
        std::fs::remove_dir_all(&staging).map_err(|e| io_err("remove staging dir", e))?;
        Ok(())
    }
}

impl TwoPhaseCommitSink for TwoPhaseParquetSink {
    type Handle = ParquetStagingHandle;

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_two_phase_commit()
    }

    fn prepare(&mut self, epoch: u64, batch: &RecordBatch) -> ConnectorResult<Self::Handle> {
        if epoch != self.epoch {
            return Err(ConnectorError::Parquet("prepare epoch mismatch".into()));
        }
        let staging_dir = self.staging_dir();
        std::fs::create_dir_all(&staging_dir).map_err(|e| io_err("create staging dir", e))?;

        let (id, staging_path, final_path, file) = loop {
            let id = self.next_id;
            self.next_id = self.next_id.checked_add(1).ok_or_else(|| {
                ConnectorError::Parquet("Parquet two-phase commit handle space exhausted".into())
            })?;
            let staging_name = format!("part-{id}.parquet");
            let staging_path = staging_dir.join(&staging_name);
            let final_path = self
                .final_dir()
                .join(format!("epoch-{}-{staging_name}", self.epoch));
            if final_path.exists() {
                continue;
            }
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging_path)
            {
                Ok(file) => break (id, staging_path, final_path, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io_err("create staged parquet", error)),
            }
        };

        let write_result = (|| {
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                .map_err(|e| ConnectorError::Parquet(format!("parquet writer: {e}")))?;
            writer
                .write(batch)
                .map_err(|e| ConnectorError::Parquet(format!("parquet write: {e}")))?;
            writer
                .close()
                .map_err(|e| ConnectorError::Parquet(format!("parquet close: {e}")))?;
            Ok(())
        })();
        if let Err(error) = write_result {
            if let Err(cleanup_error) = std::fs::remove_file(&staging_path)
                && cleanup_error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(
                    path = %staging_path.display(),
                    error = %cleanup_error,
                    "failed to clean up incomplete staged Parquet file"
                );
            }
            return Err(error);
        }

        self.staged_paths.insert(id, staging_path.clone());
        Ok(ParquetStagingHandle {
            id,
            staging_path,
            final_path,
        })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        std::fs::create_dir_all(self.final_dir()).map_err(|e| io_err("create final dir", e))?;
        publish_staged_file(&handle.staging_path, &handle.final_path)?;
        self.staged_paths.remove(&handle.id);
        Ok(())
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        match std::fs::remove_file(&handle.staging_path) {
            Ok(()) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && !handle.final_path.exists() => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ConnectorError::Parquet(format!(
                    "cannot abort committed Parquet handle at {:?}",
                    handle.final_path
                )));
            }
            Err(error) => return Err(io_err("remove staging file on abort", error)),
        }
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
        assert!(
            dir.path()
                .join("data")
                .join("epoch-1-part-0.parquet")
                .exists()
        );
    }

    #[test]
    fn parquet_2pc_preserves_commits_across_epochs() {
        let dir = tempdir().unwrap();
        for epoch in [1, 2] {
            let mut sink = TwoPhaseParquetSink::new(dir.path(), epoch);
            let handle = sink.prepare(epoch, &batch()).unwrap();
            sink.commit(handle).unwrap();
        }

        assert!(
            dir.path()
                .join("data")
                .join("epoch-1-part-0.parquet")
                .exists()
        );
        assert!(
            dir.path()
                .join("data")
                .join("epoch-2-part-0.parquet")
                .exists()
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("data")).unwrap().count(),
            2
        );
    }
}
