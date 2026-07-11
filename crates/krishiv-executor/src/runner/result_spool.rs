//! Executor-side disk spooling for large task results (Phase 2.10).
//!
//! A batch SQL fragment's output used to be collected into memory, encoded
//! as one Arrow IPC blob, and shipped inside a single `TaskStatus` gRPC
//! message — three full copies of the result in the executor process. For
//! multi-hundred-MB results that is what OOM-killed the shared engine pod.
//!
//! [`drain_stream_with_spool`] consumes the DataFusion record-batch stream
//! incrementally: small results stay in memory and ride the existing inline
//! path (no extra RTT); once the in-memory size crosses the threshold, all
//! batches overflow to a spool file (one Arrow IPC stream) and the executor
//! delivers it to the coordinator in bounded chunks via `PushTaskResult`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use futures::StreamExt as _;
use krishiv_proto::{TaskAttemptRef, TaskResultChunk};

use crate::{ExecutorError, ExecutorResult};

/// Environment variable: maximum bytes of a task result kept in memory and
/// shipped inline in the `TaskStatus` message. Larger results spool to disk
/// and stream to the coordinator in chunks. Default 8 MiB; `0` disables
/// spooling entirely (always inline, pre-2.10 behavior).
pub const INLINE_RESULT_MAX_BYTES_ENV: &str = "KRISHIV_INLINE_RESULT_MAX_BYTES";

const DEFAULT_INLINE_RESULT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Bytes per `PushTaskResult` chunk. Kept well under the 4 MiB tonic default
/// so the chunk stream never depends on raised message-size limits.
const RESULT_CHUNK_BYTES: usize = 3 * 1024 * 1024;

static SPOOL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Programmatic threshold override (`usize::MAX` = unset). Exists because
/// `std::env::set_var` is `unsafe` under edition 2024 and the workspace
/// forbids unsafe code, so tests cannot toggle the env var.
static INLINE_MAX_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(usize::MAX);

/// Test/diagnostic hook: override the inline threshold for this process
/// (`0` disables spooling entirely).
#[doc(hidden)]
pub fn set_inline_result_max_bytes_for_tests(threshold: usize) {
    INLINE_MAX_OVERRIDE.store(threshold, Ordering::Relaxed);
}

/// Resolve the inline threshold; `0`/unparseable → `None` (spooling disabled).
pub(crate) fn inline_result_max_bytes() -> Option<usize> {
    let overridden = INLINE_MAX_OVERRIDE.load(Ordering::Relaxed);
    if overridden != usize::MAX {
        return Some(overridden).filter(|&n| n > 0);
    }
    match std::env::var(INLINE_RESULT_MAX_BYTES_ENV).ok() {
        Some(raw) => raw.trim().parse::<usize>().ok().filter(|&n| n > 0),
        None => Some(DEFAULT_INLINE_RESULT_MAX_BYTES),
    }
}

fn spool_dir() -> PathBuf {
    std::env::var("KRISHIV_RESULT_SPOOL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("krishiv-result-spool"))
}

/// A task result spooled to local disk as one Arrow IPC stream.
///
/// The file deletes itself when the handle drops (after the chunks have been
/// pushed to the coordinator, or on task failure).
#[derive(Debug)]
pub struct SpooledTaskResult {
    path: PathBuf,
    total_bytes: u64,
}

impl SpooledTaskResult {
    /// Path of the spool file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total bytes in the spool file.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl Drop for SpooledTaskResult {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

// The spool is identified by its unique path; equality on (path, size) keeps
// ExecutorTaskOutput's PartialEq derivable.
impl PartialEq for SpooledTaskResult {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.total_bytes == other.total_bytes
    }
}

/// Result of draining a query stream through the spool decision.
#[derive(Debug, PartialEq)]
pub(crate) enum DrainedResult {
    /// Result fits the inline threshold: ships in the `TaskStatus` message.
    Inline(Vec<RecordBatch>),
    /// Result overflowed to disk: delivered via `PushTaskResult` chunks.
    Spooled(SpooledTaskResult),
}

/// Shape summary of a drained result.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DrainedShape {
    pub row_count: usize,
    pub batch_count: usize,
    pub column_count: usize,
}

/// Drain `stream`, keeping at most `threshold` in-memory bytes before
/// overflowing every batch (buffered and future) to a spool file.
///
/// `threshold = None` disables spooling (collect everything, pre-2.10
/// behavior). Peak memory in the spooled case is the buffered prefix plus
/// one in-flight batch.
pub(crate) async fn drain_stream_with_spool(
    mut stream: krishiv_sql::SqlStream,
    threshold: Option<usize>,
) -> ExecutorResult<(DrainedResult, DrainedShape)> {
    let mut shape = DrainedShape::default();
    let mut buffered: Vec<RecordBatch> = Vec::new();
    let mut buffered_bytes: usize = 0;
    let mut writer: Option<arrow::ipc::writer::StreamWriter<std::io::BufWriter<std::fs::File>>> =
        None;
    let mut spool_path: Option<PathBuf> = None;

    let io_err = |context: &str, e: &dyn std::fmt::Display| ExecutorError::LocalExecution {
        message: format!("{context}: {e}"),
    };

    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| ExecutorError::LocalExecution {
            message: e.to_string(),
        })?;
        shape.row_count += batch.num_rows();
        shape.batch_count += 1;
        if shape.column_count == 0 {
            shape.column_count = batch.num_columns();
        }

        if let Some(w) = writer.as_mut() {
            w.write(&batch)
                .map_err(|e| io_err("result spool write", &e))?;
            continue;
        }

        buffered_bytes += batch.get_array_memory_size();
        buffered.push(batch);

        if let Some(limit) = threshold
            && buffered_bytes > limit
        {
            // Overflow: open the spool and move every buffered batch into it.
            let dir = spool_dir();
            std::fs::create_dir_all(&dir).map_err(|e| io_err("create result spool dir", &e))?;
            let seq = SPOOL_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!("executor-{}-{}.arrow-ipc", std::process::id(), seq));
            let file =
                std::fs::File::create(&path).map_err(|e| io_err("create result spool", &e))?;
            let Some(schema) = buffered.first().map(|b| b.schema()) else {
                return Err(ExecutorError::LocalExecution {
                    message: "result spool overflow with no buffered batch".to_string(),
                });
            };
            let mut w =
                arrow::ipc::writer::StreamWriter::try_new(std::io::BufWriter::new(file), &schema)
                    .map_err(|e| io_err("open result spool writer", &e))?;
            for b in buffered.drain(..) {
                w.write(&b).map_err(|e| io_err("result spool write", &e))?;
            }
            buffered_bytes = 0;
            spool_path = Some(path);
            writer = Some(w);
        }
    }

    match (writer, spool_path) {
        (Some(w), Some(path)) => {
            let mut inner = w
                .into_inner()
                .map_err(|e| io_err("finish result spool", &e))?;
            use std::io::Write as _;
            inner
                .flush()
                .map_err(|e| io_err("flush result spool", &e))?;
            drop(inner);
            let total_bytes = std::fs::metadata(&path)
                .map_err(|e| io_err("stat result spool", &e))?
                .len();
            Ok((
                DrainedResult::Spooled(SpooledTaskResult { path, total_bytes }),
                shape,
            ))
        }
        _ => Ok((DrainedResult::Inline(buffered), shape)),
    }
}

/// Build the ordered `PushTaskResult` chunk stream for a spooled result.
///
/// Reads the spool file in `RESULT_CHUNK_BYTES` slices; only one chunk is in
/// memory at a time. The final chunk carries the total byte count.
pub(crate) fn spool_chunk_stream(
    ids: TaskAttemptRef,
    path: PathBuf,
    total_bytes: u64,
) -> krishiv_proto::services::TaskResultChunkStream {
    let stream =
        futures::stream::try_unfold((None::<tokio::fs::File>, 0u64), move |(file, sent)| {
            let ids = ids.clone();
            let path = path.clone();
            async move {
                if sent >= total_bytes {
                    return Ok(None);
                }
                let mut file = match file {
                    Some(f) => f,
                    None => tokio::fs::File::open(&path)
                        .await
                        .map_err(|e| tonic::Status::internal(format!("open result spool: {e}")))?,
                };
                use tokio::io::AsyncReadExt as _;
                let want = std::cmp::min(RESULT_CHUNK_BYTES as u64, total_bytes - sent) as usize;
                let mut buf = vec![0u8; want];
                file.read_exact(&mut buf)
                    .await
                    .map_err(|e| tonic::Status::internal(format!("read result spool: {e}")))?;
                let sent = sent + want as u64;
                let mut chunk = TaskResultChunk::new(ids, buf);
                if sent >= total_bytes {
                    chunk = chunk.with_last(total_bytes);
                }
                Ok(Some((chunk, (Some(file), sent))))
            }
        });
    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(0..n)) as _],
        )
        .unwrap()
    }

    fn stream_of(batches: Vec<RecordBatch>) -> krishiv_sql::SqlStream {
        Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
    }

    #[tokio::test]
    async fn small_result_stays_inline() {
        let (result, shape) =
            drain_stream_with_spool(stream_of(vec![batch(10), batch(5)]), Some(1024 * 1024))
                .await
                .unwrap();
        assert_eq!(shape.row_count, 15);
        assert_eq!(shape.batch_count, 2);
        assert_eq!(shape.column_count, 1);
        match result {
            DrainedResult::Inline(batches) => assert_eq!(batches.len(), 2),
            DrainedResult::Spooled(_) => panic!("small result must not spool"),
        }
    }

    #[tokio::test]
    async fn large_result_spools_and_round_trips() {
        // Tiny threshold forces the spool after the first batch.
        let (result, shape) =
            drain_stream_with_spool(stream_of(vec![batch(1000), batch(1000)]), Some(16))
                .await
                .unwrap();
        assert_eq!(shape.row_count, 2000);
        let spool = match result {
            DrainedResult::Spooled(s) => s,
            DrainedResult::Inline(_) => panic!("must spool past threshold"),
        };
        assert!(spool.total_bytes() > 0);

        // The spool file must decode back to the same rows.
        let file = std::fs::File::open(spool.path()).unwrap();
        let reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::BufReader::new(file), None).unwrap();
        let decoded: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2000);

        let path = spool.path().to_path_buf();
        drop(spool);
        assert!(!path.exists(), "spool must delete on drop");
    }

    #[tokio::test]
    async fn disabled_threshold_never_spools() {
        let (result, _) = drain_stream_with_spool(stream_of(vec![batch(100_000)]), None)
            .await
            .unwrap();
        assert!(matches!(result, DrainedResult::Inline(_)));
    }

    #[tokio::test]
    async fn chunk_stream_covers_file_and_marks_last() {
        use futures::StreamExt as _;

        let dir = std::env::temp_dir().join("krishiv-result-spool");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("chunk-test-{}", std::process::id()));
        let payload: Vec<u8> = (0..10_000_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &payload).unwrap();

        let ids = TaskAttemptRef::new(
            krishiv_proto::JobId::try_new("j".to_string()).unwrap(),
            krishiv_proto::StageId::try_new("s".to_string()).unwrap(),
            krishiv_proto::TaskId::try_new("t".to_string()).unwrap(),
            krishiv_proto::AttemptId::try_new(1).unwrap(),
        );
        let mut stream = spool_chunk_stream(ids, path.clone(), payload.len() as u64);
        let mut got: Vec<u8> = Vec::new();
        let mut last_seen = false;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(!last_seen, "no chunk may follow the final chunk");
            if chunk.last() {
                last_seen = true;
                assert_eq!(chunk.total_bytes(), payload.len() as u64);
            }
            got.extend_from_slice(chunk.data());
        }
        assert!(last_seen);
        assert_eq!(got, payload);
        let _ = std::fs::remove_file(&path);
    }
}
