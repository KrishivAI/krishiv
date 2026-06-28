//! A file-backed [`CheckpointService`] for single-node (and durable) placement.
//!
//! Where [`InMemoryCheckpointService`](crate::mem::InMemoryCheckpointService)
//! holds the latest checkpoint in process memory (lost on restart), this writes
//! each job's latest checkpoint to a file under a directory, so a job's operator
//! state and source offsets survive a process restart — the durability the
//! single-node daemon and distributed placements require. The write is atomic
//! (write-to-temp then rename) so a crash mid-persist never leaves a torn file.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use krishiv_proto::JobId;

use crate::error::{EngineError, EngineResult};
use crate::runtime::{CheckpointPayload, CheckpointService};

/// Persists the latest [`CheckpointPayload`] per job to a directory on disk.
///
/// One file per job (`<dir>/<job_id>.ckpt`), holding the JSON-serialized latest
/// payload. The latest epoch overwrites the previous one (atomically), matching
/// the "latest committed checkpoint" contract of [`CheckpointService`].
#[derive(Debug, Clone)]
pub struct DurableCheckpointService {
    dir: PathBuf,
}

impl DurableCheckpointService {
    /// Create a durable checkpoint service rooted at `dir`, creating the
    /// directory (and parents) if it does not exist.
    pub fn new(dir: impl AsRef<Path>) -> EngineResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| {
            EngineError::Checkpoint(format!(
                "failed to create checkpoint dir '{}': {e}",
                dir.display()
            ))
        })?;
        Ok(Self { dir })
    }

    /// The checkpoint file path for `job`.
    fn path_for(&self, job: &JobId) -> PathBuf {
        self.dir.join(format!("{}.ckpt", job.as_str()))
    }
}

#[async_trait]
impl CheckpointService for DurableCheckpointService {
    async fn persist(&self, job: &JobId, payload: &CheckpointPayload) -> EngineResult<()> {
        let bytes = serde_json::to_vec(payload)
            .map_err(|e| EngineError::Checkpoint(format!("serialize checkpoint: {e}")))?;
        let final_path = self.path_for(job);
        // Atomic publish: write to a temp file in the same dir, then rename.
        let tmp_path = self.dir.join(format!("{}.ckpt.tmp", job.as_str()));
        std::fs::write(&tmp_path, &bytes).map_err(|e| {
            EngineError::Checkpoint(format!(
                "write checkpoint temp '{}': {e}",
                tmp_path.display()
            ))
        })?;
        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            EngineError::Checkpoint(format!(
                "publish checkpoint '{}': {e}",
                final_path.display()
            ))
        })?;
        Ok(())
    }

    async fn restore_latest(&self, job: &JobId) -> EngineResult<Option<CheckpointPayload>> {
        let path = self.path_for(job);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let payload = serde_json::from_slice(&bytes).map_err(|e| {
                    EngineError::Checkpoint(format!(
                        "deserialize checkpoint '{}': {e}",
                        path.display()
                    ))
                })?;
                Ok(Some(payload))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(EngineError::Checkpoint(format!(
                "read checkpoint '{}': {e}",
                path.display()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[tokio::test]
    async fn persists_and_restores_across_instances() {
        let dir = tempfile::tempdir().unwrap();
        let job = JobId::try_new("durable-job").unwrap();

        let svc = DurableCheckpointService::new(dir.path()).unwrap();
        assert!(svc.restore_latest(&job).await.unwrap().is_none());

        let payload = CheckpointPayload {
            epoch: 7,
            operator_state: vec![1, 2, 3, 4],
            source_offsets: vec![("events".to_string(), vec![9, 9])],
        };
        svc.persist(&job, &payload).await.unwrap();

        // A fresh instance over the same dir (simulating a process restart) sees
        // the persisted checkpoint.
        let reopened = DurableCheckpointService::new(dir.path()).unwrap();
        assert_eq!(reopened.restore_latest(&job).await.unwrap(), Some(payload));
    }

    #[tokio::test]
    async fn latest_epoch_overwrites_previous() {
        let dir = tempfile::tempdir().unwrap();
        let job = JobId::try_new("rollover").unwrap();
        let svc = DurableCheckpointService::new(dir.path()).unwrap();

        for epoch in 1..=3 {
            svc.persist(
                &job,
                &CheckpointPayload {
                    epoch,
                    operator_state: vec![epoch as u8],
                    source_offsets: vec![],
                },
            )
            .await
            .unwrap();
        }
        let latest = svc.restore_latest(&job).await.unwrap().unwrap();
        assert_eq!(latest.epoch, 3);
    }
}
