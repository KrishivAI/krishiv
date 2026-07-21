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

/// Environment variable overriding how much unsynced data
/// `receive_task_result_spool` writes before forcing an `fdatasync`
/// (default 64 MiB). Large results otherwise arrive as a burst of writes
/// that the kernel buffers as dirty page cache with nothing forcing
/// writeback — under a memory-cgroup limit (Kubernetes `memory.max` counts
/// page cache alongside process RSS), a several-GiB spool can accumulate as
/// dirty cache faster than background writeback reclaims it and get the pod
/// OOMKilled even though the receiving process's own RSS never grows.
/// Syncing periodically bounds how much of any one spool can be dirty at
/// once, independent of the spool's total size.
pub const RESULT_SPOOL_SYNC_INTERVAL_BYTES_ENV: &str = "KRISHIV_RESULT_SPOOL_SYNC_INTERVAL_BYTES";

const DEFAULT_RESULT_SPOOL_SYNC_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

fn result_spool_sync_interval_bytes() -> u64 {
    std::env::var(RESULT_SPOOL_SYNC_INTERVAL_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_RESULT_SPOOL_SYNC_INTERVAL_BYTES)
}

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
    /// Collects every batch into one `Vec` before returning — peak memory is
    /// the whole decoded spool, not just one chunk. Fine for spools known to
    /// be small (or for a caller that needs random access to the batches);
    /// for a large spool that will only be streamed back out one batch at a
    /// time (the Flight SQL `do_get` path), use
    /// [`Self::decode_record_batches_streaming`] instead, which never holds
    /// more than one decoded batch at once.
    pub fn decode_record_batches(
        &self,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, arrow::error::ArrowError> {
        self.decode_record_batches_streaming()?.collect()
    }

    /// Decode the spooled Arrow IPC stream lazily: each `next()` call reads
    /// and decodes exactly one more batch from disk, so a caller that drains
    /// the iterator one batch at a time (rather than collecting it) never
    /// holds more than one decoded batch in memory.
    ///
    /// `#211` residual: [`Self::decode_record_batches`]'s `O(largest spool)`
    /// bound is only as tight as "largest spool" is small — a query that
    /// runs as a single unpartitioned task produces exactly one spool sized
    /// to the *entire* result, at which point `O(largest spool)` and
    /// `O(total result)` are the same bound, and the coordinator OOMs on
    /// exactly the un-LIMITed-SELECT shape #211 was filed against in the
    /// first place. This method is what actually gets a `do_get` consumer to
    /// `O(one batch)` regardless of how many tasks a query happened to run
    /// as; see `krishiv-flight-sql::host::execute_sql_stream`, which uses it
    /// for both the schema-extraction peek and the per-spool batch stream.
    pub fn decode_record_batches_streaming(
        &self,
    ) -> Result<
        arrow::ipc::reader::StreamReader<std::io::BufReader<std::fs::File>>,
        arrow::error::ArrowError,
    > {
        let file = std::fs::File::open(&self.path)
            .map_err(|e| arrow::error::ArrowError::IoError(format!("open result spool: {e}"), e))?;
        arrow::ipc::reader::StreamReader::try_new(std::io::BufReader::new(file), None)
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
    let mut unsynced: u64 = 0;
    let sync_interval = result_spool_sync_interval_bytes();

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
            write_and_maybe_sync(f, &data, &mut unsynced, sync_interval)
                .await
                .map_err(|e| {
                    cleanup(&path);
                    tonic::Status::internal(format!("result spool write failed: {e}"))
                })?;
            // #222: a large transfer that stalls (network contention, or a
            // genuine hang) used to be a total black box until the caller's
            // own timeout fired minutes later, with no trace of how far it
            // got. Log at the same cadence as the fdatasync above (unsynced
            // resets to exactly 0 only when a sync just ran) so a future
            // occurrence has periodic breadcrumbs instead of silence.
            if unsynced == 0
                && let Some(k) = &key
            {
                tracing::debug!(
                    job_id = %k.job_id,
                    task_id = %k.task_id,
                    attempt_id = k.attempt_id,
                    received,
                    "result spool receive progress"
                );
            }
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
    // Sync the tail too — a spool smaller than the sync interval (or the
    // last partial interval of a larger one) would otherwise never get an
    // fdatasync at all, sitting as dirty page cache until the kernel's own
    // background writeback gets to it.
    writer.get_ref().sync_data().await.map_err(|e| {
        cleanup(&Some(path_buf.clone()));
        tonic::Status::internal(format!("result spool final sync failed: {e}"))
    })?;

    Ok((
        key,
        TaskResultSpool {
            path: path_buf,
            total_bytes: received,
        },
    ))
}

/// Write `data` to `writer`, forcing an `fdatasync` (and resetting
/// `*unsynced`) once `*unsynced` reaches `sync_interval`. Factored out of
/// [`receive_task_result_spool`] so the periodic-sync behavior itself is
/// unit-testable without needing an env-var override (which would be
/// process-global and race with other tests) or a giant multi-GiB test
/// fixture — a caller can pass an arbitrarily small `sync_interval`
/// directly.
async fn write_and_maybe_sync(
    writer: &mut tokio::io::BufWriter<tokio::fs::File>,
    data: &[u8],
    unsynced: &mut u64,
    sync_interval: u64,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    writer.write_all(data).await?;
    *unsynced = unsynced.saturating_add(data.len() as u64);
    if *unsynced >= sync_interval {
        writer.flush().await?;
        writer.get_ref().sync_data().await?;
        *unsynced = 0;
    }
    Ok(())
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

    fn int_batch(values: &[i64]) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// A spool file on disk containing a real multi-batch Arrow IPC stream —
    /// `TaskResultSpool`'s fields are private but visible to this same-file
    /// test module, so we can construct one directly around a hand-encoded
    /// file instead of going through the chunk-receiving path above (which
    /// only exercises raw bytes, not Arrow IPC content).
    fn ipc_spool(batches: &[arrow::record_batch::RecordBatch]) -> (TaskResultSpool, tempfile::TempDir) {
        use arrow::ipc::writer::StreamWriter;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.arrows");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = StreamWriter::try_new(file, &batches[0].schema()).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        let total_bytes = std::fs::metadata(&path).unwrap().len();
        (
            TaskResultSpool {
                path,
                total_bytes,
            },
            dir,
        )
    }

    #[test]
    fn decode_record_batches_streaming_yields_every_batch_in_order() {
        let batches = vec![int_batch(&[1, 2, 3]), int_batch(&[4, 5]), int_batch(&[6])];
        let (spool, _dir) = ipc_spool(&batches);

        let decoded: Vec<arrow::record_batch::RecordBatch> = spool
            .decode_record_batches_streaming()
            .expect("open spool for streaming decode")
            .collect::<Result<Vec<_>, _>>()
            .expect("every batch decodes");

        assert_eq!(decoded.len(), 3);
        for (expected, actual) in batches.iter().zip(decoded.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn decode_record_batches_streaming_and_eager_agree() {
        let batches = vec![int_batch(&[10, 20]), int_batch(&[30])];
        let (spool, _dir) = ipc_spool(&batches);

        let eager = spool
            .decode_record_batches()
            .expect("eager decode succeeds");
        let streamed: Vec<_> = spool
            .decode_record_batches_streaming()
            .expect("open spool for streaming decode")
            .collect::<Result<Vec<_>, _>>()
            .expect("every batch decodes");

        assert_eq!(eager, streamed);
    }

    #[test]
    fn decode_record_batches_streaming_schema_is_available_before_any_batch_is_read() {
        let batches = vec![int_batch(&[1])];
        let (spool, _dir) = ipc_spool(&batches);

        let reader = spool
            .decode_record_batches_streaming()
            .expect("open spool for streaming decode");
        // The whole point of using `.schema()` over peeking a batch: it must
        // be readable before `next()` is ever called, so a caller extracting
        // just the schema doesn't have to decode (and hold) a batch to get it.
        assert_eq!(reader.schema(), batches[0].schema());
    }

    #[tokio::test]
    async fn write_and_maybe_sync_preserves_data_across_many_sync_points() {
        use tokio::io::AsyncWriteExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.bin");
        let file = tokio::fs::File::create(&path).await.unwrap();
        let mut writer = tokio::io::BufWriter::new(file);

        // A 3-byte interval against 10 writes of 1 byte each forces a sync
        // roughly every third write, well-exercised, not just once at the
        // very end.
        let mut unsynced = 0u64;
        let mut expected = Vec::new();
        for byte in 0u8..10 {
            write_and_maybe_sync(&mut writer, &[byte], &mut unsynced, 3)
                .await
                .unwrap();
            expected.push(byte);
        }
        writer.flush().await.unwrap();

        let on_disk = tokio::fs::read(&path).await.unwrap();
        assert_eq!(on_disk, expected, "every byte must survive periodic syncing, in order");
    }

    #[tokio::test]
    async fn write_and_maybe_sync_resets_the_counter_only_when_it_actually_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.bin");
        let file = tokio::fs::File::create(&path).await.unwrap();
        let mut writer = tokio::io::BufWriter::new(file);

        let mut unsynced = 0u64;
        write_and_maybe_sync(&mut writer, &[1, 2], &mut unsynced, 100)
            .await
            .unwrap();
        assert_eq!(unsynced, 2, "under the interval: counter just accumulates");

        write_and_maybe_sync(&mut writer, &[3, 4, 5], &mut unsynced, 5)
            .await
            .unwrap();
        assert_eq!(
            unsynced, 0,
            "crossing the interval must sync and reset the counter"
        );
    }
}
