//! A file-backed [`CheckpointService`] for single-node (and durable) placement.
//!
//! Where [`InMemoryCheckpointService`](crate::mem::InMemoryCheckpointService)
//! holds the latest checkpoint in process memory (lost on restart), this writes
//! each job's latest checkpoint to a file under a directory, so a job's operator
//! state and source offsets survive a process restart — the durability the
//! single-node daemon and distributed placements require. The write is
//! crash-durable: write-to-temp, **fsync the temp file**, rename, then fsync the
//! directory — so a crash mid-persist never leaves a torn file and never
//! publishes a name that points at unflushed data. The blocking filesystem work
//! runs on the blocking pool, off the async reactor.

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
        // Unique temp name (pid + nanos) so a stale or concurrent writer for the
        // same job never clobbers another's in-progress temp file before the
        // rename — matching the durability hygiene of the executor-side
        // `checkpoint::ephemeral` storage. (Job ownership is fenced to one
        // coordinator, so this is defense-in-depth against a lingering process.)
        let unique = format!(
            "{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let tmp_path = self.dir.join(format!("{}.ckpt.tmp.{unique}", job.as_str()));
        let dir = self.dir.clone();
        // Blocking filesystem work (write + fsync + rename + dir fsync) runs on
        // the blocking pool, never on the async reactor — a multi-MB checkpoint
        // write must not stall every other task on this worker thread.
        tokio::task::spawn_blocking(move || -> EngineResult<()> {
            use std::io::Write as _;
            // Crash-durable atomic publish. The data must reach stable storage
            // *before* the rename: `std::fs::write` + `rename` alone only orders
            // the bytes against concurrent *readers*; after a power loss the
            // renamed name can point at unflushed (zero/garbage) blocks. So we
            // fsync the temp file's contents first, then rename, then fsync the
            // directory so the rename entry itself survives the crash.
            {
                let mut f = std::fs::File::create(&tmp_path).map_err(|e| {
                    EngineError::Checkpoint(format!(
                        "create checkpoint temp '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
                f.write_all(&bytes).map_err(|e| {
                    EngineError::Checkpoint(format!(
                        "write checkpoint temp '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
                f.sync_all().map_err(|e| {
                    EngineError::Checkpoint(format!(
                        "fsync checkpoint temp '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
            }
            if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
                // Don't leak the (uniquely-named) temp file on a failed publish.
                let _ = std::fs::remove_file(&tmp_path);
                return Err(EngineError::Checkpoint(format!(
                    "publish checkpoint '{}': {e}",
                    final_path.display()
                )));
            }
            // fsync the directory so the rename is durable. Best-effort: some
            // filesystems/platforms don't support directory fsync, and the data
            // fsync above is the critical barrier — so a dir-fsync error is
            // logged-by-return only on the platforms that do support it.
            if let Ok(dir_file) = std::fs::File::open(&dir) {
                let _ = dir_file.sync_all();
            }
            Ok(())
        })
        .await
        .map_err(|e| EngineError::Checkpoint(format!("checkpoint persist task join: {e}")))?
    }

    async fn restore_latest(&self, job: &JobId) -> EngineResult<Option<CheckpointPayload>> {
        let path = self.path_for(job);
        // Read off the reactor too — a restore can be large and is on the
        // recovery critical path, not a place to block other tasks.
        let read = tokio::task::spawn_blocking(move || match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(EngineError::Checkpoint(format!(
                "read checkpoint '{}': {e}",
                path.display()
            ))),
        })
        .await
        .map_err(|e| EngineError::Checkpoint(format!("checkpoint restore task join: {e}")))??;

        match read {
            Some(bytes) => {
                let payload = serde_json::from_slice(&bytes)
                    .map_err(|e| EngineError::Checkpoint(format!("deserialize checkpoint: {e}")))?;
                Ok(Some(payload))
            }
            None => Ok(None),
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
            in_flight: vec![],
            source_in_flight: vec![],
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
                    in_flight: vec![],
                    source_in_flight: vec![],
                },
            )
            .await
            .unwrap();
        }
        let latest = svc.restore_latest(&job).await.unwrap().unwrap();
        assert_eq!(latest.epoch, 3);
    }
}
