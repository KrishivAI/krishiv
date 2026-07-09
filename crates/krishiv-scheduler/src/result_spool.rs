//! Disk-backed task result spools (large-result delivery, Phase 2.10).
//!
//! A batch SQL task whose output exceeds the executor's inline threshold is
//! streamed to the coordinator ahead of its terminal `TaskStatus` via the
//! `PushTaskResult` chunk stream. This module receives that stream: chunks
//! are appended to a spool file (one Arrow IPC stream per task result) so
//! the coordinator's peak memory stays at ~one chunk, and the spool is
//! handed to the job's result consumer once the task reports success.
//!
//! Spool files delete themselves on drop, so results abandoned by a failed
//! or cancelled job cannot leak disk.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt as _;
use krishiv_proto::services::TaskResultChunkStream;
use krishiv_proto::{JobId, TaskId, TaskResultChunk};

/// Environment variable overriding the spool directory (default:
/// `<system temp dir>/krishiv-result-spool`).
pub const RESULT_SPOOL_DIR_ENV: &str = "KRISHIV_RESULT_SPOOL_DIR";

/// Environment variable capping a single spooled result's size in bytes
/// (default 8 GiB). A stream exceeding the cap is rejected, failing the
/// producing task instead of filling the disk.
pub const RESULT_SPOOL_MAX_BYTES_ENV: &str = "KRISHIV_RESULT_SPOOL_MAX_BYTES";

const DEFAULT_RESULT_SPOOL_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Monotonic suffix so concurrent spools for retried attempts never collide.
static SPOOL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Identity of the task attempt that produced a spooled result.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskResultKey {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub attempt_id: u32,
}

/// A fully received task result spool: one Arrow IPC stream on local disk.
///
/// The file is deleted when the spool is dropped.
#[derive(Debug)]
pub struct TaskResultSpool {
    path: PathBuf,
    total_bytes: u64,
}

impl TaskResultSpool {
    /// Path of the spool file (one Arrow IPC stream).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total bytes in the spool file.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Decode the spooled Arrow IPC stream into record batches.
    ///
    /// Reads incrementally from disk; peak memory is the decoded batches
    /// themselves (no intermediate whole-file byte buffer).
    pub fn decode_record_batches(
        &self,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, arrow::error::ArrowError> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            arrow::error::ArrowError::IoError(format!("open result spool: {e}"), e)
        })?;
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::BufReader::new(file), None)?;
        reader.collect()
    }
}

impl Drop for TaskResultSpool {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Directory receiving spool files; created on first use.
pub fn result_spool_dir() -> PathBuf {
    std::env::var(RESULT_SPOOL_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("krishiv-result-spool"))
}

fn result_spool_max_bytes() -> u64 {
    std::env::var(RESULT_SPOOL_MAX_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_RESULT_SPOOL_MAX_BYTES)
}

/// Drain a `PushTaskResult` chunk stream into a spool file.
///
/// Every chunk must carry the same task-attempt identity; the final chunk
/// must set `last` with a `total_bytes` matching the bytes received. On any
/// error the partial file is removed.
pub async fn receive_task_result_spool(
    mut stream: TaskResultChunkStream,
) -> Result<(TaskResultKey, TaskResultSpool), tonic::Status> {
    use tokio::io::AsyncWriteExt as _;

    let dir = result_spool_dir();
    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
        tonic::Status::internal(format!(
            "cannot create result spool dir {}: {e}",
            dir.display()
        ))
    })?;

    let max_bytes = result_spool_max_bytes();
    let mut key: Option<TaskResultKey> = None;
    let mut path: Option<PathBuf> = None;
    let mut file: Option<tokio::io::BufWriter<tokio::fs::File>> = None;
    let mut received: u64 = 0;
    let mut saw_last = false;
    let mut declared_total: u64 = 0;

    // Remove the partial file on any early-return error.
    let cleanup = |path: &Option<PathBuf>| {
        if let Some(p) = path {
            let _ = std::fs::remove_file(p);
        }
    };

    while let Some(chunk) = stream.next().await {
        let chunk: TaskResultChunk = match chunk {
            Ok(c) => c,
            Err(status) => {
                cleanup(&path);
                return Err(status);
            }
        };
        if saw_last {
            cleanup(&path);
            return Err(tonic::Status::invalid_argument(
                "task result chunk received after the final chunk",
            ));
        }

        let chunk_key = TaskResultKey {
            job_id: chunk.job_id().clone(),
            task_id: chunk.task_id().clone(),
            attempt_id: chunk.attempt_id().as_u32(),
        };
        match &key {
            None => {
                let seq = SPOOL_SEQ.fetch_add(1, Ordering::Relaxed);
                let name = format!(
                    "{}-{}-{}-{}-{}.arrow-ipc",
                    chunk_key.job_id.as_str(),
                    chunk_key.task_id.as_str(),
                    chunk_key.attempt_id,
                    std::process::id(),
                    seq
                );
                let p = dir.join(name);
                let f = tokio::fs::File::create(&p).await.map_err(|e| {
                    tonic::Status::internal(format!(
                        "cannot create result spool file {}: {e}",
                        p.display()
                    ))
                })?;
                path = Some(p);
                file = Some(tokio::io::BufWriter::new(f));
                key = Some(chunk_key);
            }
            Some(existing) if *existing != chunk_key => {
                cleanup(&path);
                return Err(tonic::Status::invalid_argument(
                    "task result chunks must all belong to one task attempt",
                ));
            }
            Some(_) => {}
        }

        received = received.saturating_add(chunk.data().len() as u64);
        if received > max_bytes {
            cleanup(&path);
            return Err(tonic::Status::resource_exhausted(format!(
                "spooled task result exceeds {max_bytes} bytes ({RESULT_SPOOL_MAX_BYTES_ENV})"
            )));
        }
        if chunk.last() {
            saw_last = true;
            declared_total = chunk.total_bytes();
        }
        let data = chunk.into_data();
        if let Some(f) = file.as_mut()
            && !data.is_empty()
        {
            f.write_all(&data).await.map_err(|e| {
                cleanup(&path);
                tonic::Status::internal(format!("result spool write failed: {e}"))
            })?;
        }
    }

    let (Some(key), Some(path_buf), Some(mut writer)) = (key, path.clone(), file) else {
        return Err(tonic::Status::invalid_argument(
            "task result stream carried no chunks",
        ));
    };
    if !saw_last {
        cleanup(&Some(path_buf));
        return Err(tonic::Status::invalid_argument(
            "task result stream ended without a final chunk",
        ));
    }
    if declared_total != received {
        cleanup(&Some(path_buf));
        return Err(tonic::Status::data_loss(format!(
            "task result spool incomplete: declared {declared_total} bytes, received {received}"
        )));
    }
    writer.flush().await.map_err(|e| {
        cleanup(&Some(path_buf.clone()));
        tonic::Status::internal(format!("result spool flush failed: {e}"))
    })?;

    Ok((
        key,
        TaskResultSpool {
            path: path_buf,
            total_bytes: received,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::{StageId, TaskAttemptRef};

    fn ids() -> TaskAttemptRef {
        TaskAttemptRef::new(
            JobId::try_new("job-1".to_string()).unwrap(),
            StageId::try_new("stage-0".to_string()).unwrap(),
            TaskId::try_new("task-0".to_string()).unwrap(),
            krishiv_proto::AttemptId::try_new(1).unwrap(),
        )
    }

    fn chunk_stream(chunks: Vec<TaskResultChunk>) -> TaskResultChunkStream {
        Box::pin(futures::stream::iter(chunks.into_iter().map(Ok)))
    }

    #[tokio::test]
    async fn receives_ordered_chunks_into_one_file() {
        let c1 = TaskResultChunk::new(ids(), vec![1, 2, 3]);
        let c2 = TaskResultChunk::new(ids(), vec![4, 5]).with_last(5);
        let (key, spool) = receive_task_result_spool(chunk_stream(vec![c1, c2]))
            .await
            .unwrap();
        assert_eq!(key.job_id.as_str(), "job-1");
        assert_eq!(spool.total_bytes(), 5);
        let bytes = std::fs::read(spool.path()).unwrap();
        assert_eq!(bytes, vec![1, 2, 3, 4, 5]);
        let path = spool.path().to_path_buf();
        drop(spool);
        assert!(!path.exists(), "spool file must delete on drop");
    }

    #[tokio::test]
    async fn rejects_total_mismatch() {
        let c = TaskResultChunk::new(ids(), vec![1, 2, 3]).with_last(99);
        let err = receive_task_result_spool(chunk_stream(vec![c]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::DataLoss);
    }

    #[tokio::test]
    async fn rejects_missing_final_chunk() {
        let c = TaskResultChunk::new(ids(), vec![1, 2, 3]);
        let err = receive_task_result_spool(chunk_stream(vec![c]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn rejects_empty_stream() {
        let err = receive_task_result_spool(chunk_stream(vec![]))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
