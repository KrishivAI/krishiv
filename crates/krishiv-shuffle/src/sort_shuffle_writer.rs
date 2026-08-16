//! Spark-style SortShuffleWriter: hash-partition → per-partition sort → single
//! data file with an accompanying binary index file.
//!
//! ## Layout
//!
//! ```text
//! data file  : [partition 0 IPC stream] [partition 1 IPC stream] … [partition N-1 IPC stream]
//! index file : u64-LE[0], u64-LE[1], …, u64-LE[N], u64-LE[N] (N+1 offsets, last = file length)
//! ```
//!
//! `index[p]` is the byte offset in the data file where partition `p` starts.
//! `index[p+1] - index[p]` is the byte length of partition `p`.
//! The ESS (External Shuffle Service, T10) uses these offsets to serve
//! partition-level range reads without materialising the full file.
//!
//! ## Memory management / spill
//!
//! When the total bytes across all in-memory buckets exceed
//! `KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES` (env var, default 512 MiB),
//! the writer spills the largest bucket to a temp IPC file under `output_dir`
//! and reclaims its memory.  At `flush()` time, spilled files are merged with
//! any remaining in-memory data before writing the final data + index files.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::DataType;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;

use crate::error::io_err;
use crate::partitioner::HashPartitioner;
use crate::{ShuffleError, ShuffleResult};

/// Default spill threshold: 512 MiB of in-memory shuffle data.
pub const DEFAULT_SPILL_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;

/// Read the spill threshold from the environment, falling back to the default.
fn spill_threshold_from_env() -> u64 {
    std::env::var("KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SPILL_THRESHOLD_BYTES)
}

/// Sort-shuffle output files produced by [`SortShuffleWriter::flush`].
#[derive(Debug, Clone)]
pub struct SortShuffleFiles {
    /// Concatenated Arrow IPC stream for all partitions.
    pub data_path: PathBuf,
    /// `(num_partitions + 1)` little-endian `u64` byte offsets.
    pub index_path: PathBuf,
    /// Number of partitions.
    pub partition_count: u32,
}

impl SortShuffleFiles {
    /// Read the raw offset table from the index file.
    ///
    /// Returns a `Vec` of length `partition_count + 1`. Entry `p` is the
    /// byte offset in the data file where partition `p` starts; the last
    /// entry is the total data file length.
    pub fn read_offsets(&self) -> ShuffleResult<Vec<u64>> {
        let bytes = std::fs::read(&self.index_path).map_err(|e| {
            io_err(format!(
                "failed to read index file '{}': {e}",
                self.index_path.display()
            ))
        })?;
        let expected = (self.partition_count as usize + 1) * 8;
        if bytes.len() != expected {
            return Err(ShuffleError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "index file '{}' has {} bytes, expected {expected}",
                    self.index_path.display(),
                    bytes.len()
                ),
            )));
        }
        Ok(bytes
            .chunks_exact(8)
            .map(|c| {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(c);
                u64::from_le_bytes(arr)
            })
            .collect())
    }
}

/// Spark-style sort-shuffle writer.
///
/// Call [`push`](SortShuffleWriter::push) for each input batch, then
/// [`flush`](SortShuffleWriter::flush) to write the data file + index file.
///
/// When the total accumulated in-memory bytes exceed the spill threshold,
/// the largest bucket is spilled to a temp IPC file and its memory freed.
/// Spilled files are merged at `flush()` time.
pub struct SortShuffleWriter {
    partitioner: HashPartitioner,
    sort_key: String,
    /// Per-partition accumulated (in-memory) batches.
    buckets: Vec<Vec<RecordBatch>>,
    /// Estimated serialised byte size of each in-memory bucket.
    bucket_bytes: Vec<usize>,
    /// Temp IPC files spilled from each partition (None = not spilled).
    spill_files: Vec<Vec<PathBuf>>,
    output_dir: PathBuf,
    job_id: String,
    stage_id: String,
    /// Spill threshold in bytes. Configurable via env.
    spill_threshold_bytes: u64,
    /// Counter for generating unique spill file names.
    spill_seq: u32,
}

impl SortShuffleWriter {
    /// Create a writer using the spill threshold from `KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES`
    /// (env var, default 512 MiB).
    pub fn new(
        job_id: impl Into<String>,
        stage_id: impl Into<String>,
        partitioner: HashPartitioner,
        sort_key: impl Into<String>,
        output_dir: impl AsRef<Path>,
    ) -> ShuffleResult<Self> {
        Self::new_with_spill_threshold(
            job_id,
            stage_id,
            partitioner,
            sort_key,
            output_dir,
            spill_threshold_from_env(),
        )
    }

    /// Create a writer with an explicit `spill_threshold_bytes`.
    ///
    /// Takes the caller's [`HashPartitioner`] rather than building one, because
    /// building one here silently produced a *different* partitioning than the
    /// store path it runs alongside. This writer feeds the ESS index while the
    /// same map task also writes the shuffle store; when the two disagree about
    /// which partition a row belongs to, a reduce task reading one of them sees
    /// rows for its key placed elsewhere — a wrong answer with no error.
    ///
    /// It diverged on both axes: the store path seeds from the job id
    /// (`with_seed`) while this defaulted to seed 0, and the store path hashes
    /// every declared key column while this hashed only the sort key. Neither is
    /// expressible now — there is one partitioner and both writers share it.
    pub fn new_with_spill_threshold(
        job_id: impl Into<String>,
        stage_id: impl Into<String>,
        partitioner: HashPartitioner,
        sort_key: impl Into<String>,
        output_dir: impl AsRef<Path>,
        spill_threshold_bytes: u64,
    ) -> ShuffleResult<Self> {
        let partition_count = partitioner.partition_count();
        if partition_count == 0 {
            return Err(ShuffleError::InvalidPartitionCount {
                buckets: partition_count,
            });
        }
        let sort_key = sort_key.into();
        let output_dir = output_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&output_dir).map_err(|e| {
            io_err(format!(
                "failed to create sort-shuffle dir '{}': {e}",
                output_dir.display()
            ))
        })?;
        let n = partition_count as usize;
        Ok(Self {
            partitioner,
            sort_key,
            buckets: vec![Vec::new(); n],
            bucket_bytes: vec![0; n],
            spill_files: vec![Vec::new(); n],
            output_dir,
            job_id: job_id.into(),
            stage_id: stage_id.into(),
            spill_threshold_bytes,
            spill_seq: 0,
        })
    }

    /// Hash-partition `batch` and accumulate rows into per-partition buffers.
    ///
    /// If the total in-memory bytes exceed the spill threshold, the largest
    /// partition is spilled to a temp IPC file before returning.
    pub fn push(&mut self, batch: RecordBatch) -> ShuffleResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let partitioned = self.partitioner.partition(&batch)?;
        for (idx, part) in partitioned.into_iter().enumerate() {
            if part.num_rows() > 0 {
                let est = estimated_batch_bytes(&part);
                if let Some(b) = self.bucket_bytes.get_mut(idx) {
                    *b += est;
                }
                if let Some(b) = self.buckets.get_mut(idx) {
                    b.push(part);
                }
            }
        }
        // Spill until back under budget — not once per push.
        //
        // This was a single `if`: one push spilled at most one bucket. With `n`
        // partitions the largest bucket holds roughly `total / n`, so a push
        // that adds more than that leaves the writer *above* the threshold and
        // still growing. At 200 partitions and an 8 MB push the net motion is
        // +5.5 MB per call — the threshold is crossed once and then never
        // enforced again, so "spills at 512 MiB" was a threshold the writer
        // announced and did not hold.
        //
        // `spill_largest_bucket` is a no-op when the largest bucket is already
        // empty, so loop on observed progress rather than on the predicate
        // alone: that terminates even if every bucket is empty while the
        // accounting says otherwise, instead of spinning forever.
        while self.buffered_bytes() as u64 > self.spill_threshold_bytes {
            let before = self.buffered_bytes();
            self.spill_largest_bucket()?;
            if self.buffered_bytes() >= before {
                break;
            }
        }
        Ok(())
    }

    /// Spill the largest in-memory bucket to a temp IPC file.
    fn spill_largest_bucket(&mut self) -> ShuffleResult<()> {
        let largest = self
            .bucket_bytes
            .iter()
            .enumerate()
            .max_by_key(|&(_, b)| b)
            .map(|(i, _)| i);
        let Some(idx) = largest else { return Ok(()) };
        if self.buckets.get(idx).is_none_or(|b| b.is_empty()) {
            return Ok(());
        }
        let seq = self.spill_seq;
        self.spill_seq += 1;
        let spill_path = self.output_dir.join(format!(
            "{}_{}_{idx}_spill{seq}.ipc",
            self.job_id, self.stage_id
        ));

        let batches = self
            .buckets
            .get_mut(idx)
            .map(std::mem::take)
            .unwrap_or_default();
        if let Some(b) = self.bucket_bytes.get_mut(idx) {
            *b = 0;
        }

        let schema = batches
            .first()
            .ok_or_else(|| io_err("empty spill batches".to_string()))?
            .schema();
        let combined = arrow::compute::concat_batches(&schema, batches.iter())
            .map_err(|e| io_err(format!("spill concat failed: {e}")))?;
        let sorted = sort_by_key(&combined, &self.sort_key)?;
        let ipc = encode_ipc(&sorted)?;
        std::fs::write(&spill_path, &ipc).map_err(|e| {
            io_err(format!(
                "failed to write spill file '{}': {e}",
                spill_path.display()
            ))
        })?;
        if let Some(f) = self.spill_files.get_mut(idx) {
            f.push(spill_path);
        }
        Ok(())
    }

    /// Write all accumulated data to `<output_dir>/<job_id>_<stage_id>.data`
    /// and `<output_dir>/<job_id>_<stage_id>.index`.
    ///
    /// Each partition's rows are sorted by the sort key before serialisation.
    pub fn flush(self) -> ShuffleResult<SortShuffleFiles> {
        let n = self.buckets.len() as u32;
        let base = format!("{}_{}", self.job_id, self.stage_id);
        let data_path = self.output_dir.join(format!("{base}.data"));
        let index_path = self.output_dir.join(format!("{base}.index"));

        // Phase 65: the per-bucket gather → concat → sort → IPC-encode is
        // independent CPU-bound work. Run it in parallel on the process-global
        // compute pool, then concatenate the encoded blocks IN ORDER — the data
        // file layout and the (n+1) index offsets stay byte-identical to the
        // serial path (empty buckets encode to a zero-length block, so their
        // offset still points to valid, zero-length data).
        let bucket_refs: Vec<(usize, &Vec<RecordBatch>)> =
            self.buckets.iter().enumerate().collect();
        let encoded_blocks = krishiv_common::compute_pool::par_map(bucket_refs, |(idx, bucket)| {
            encode_bucket(bucket, self.spill_files.get(idx), &self.sort_key)
        });

        let mut data_buf: Vec<u8> = Vec::new();
        let mut offsets: Vec<u64> = Vec::with_capacity(n as usize + 1);
        for block in encoded_blocks {
            offsets.push(data_buf.len() as u64);
            data_buf.extend_from_slice(&block?);
        }
        offsets.push(data_buf.len() as u64);

        // Write data file atomically via a temp file.
        let tmp_data = self.output_dir.join(format!("{base}.data.tmp"));
        std::fs::write(&tmp_data, &data_buf).map_err(|e| {
            io_err(format!(
                "failed to write data file '{}': {e}",
                tmp_data.display()
            ))
        })?;
        std::fs::rename(&tmp_data, &data_path).map_err(|e| {
            io_err(format!(
                "failed to rename data file '{}' → '{}': {e}",
                tmp_data.display(),
                data_path.display()
            ))
        })?;

        // Write index file: (n+1) u64 LE offsets.
        let mut idx_buf = Vec::with_capacity((n as usize + 1) * 8);
        for off in &offsets {
            idx_buf.extend_from_slice(&off.to_le_bytes());
        }
        let tmp_idx = self.output_dir.join(format!("{base}.index.tmp"));
        std::fs::write(&tmp_idx, &idx_buf).map_err(|e| {
            io_err(format!(
                "failed to write index file '{}': {e}",
                tmp_idx.display()
            ))
        })?;
        std::fs::rename(&tmp_idx, &index_path).map_err(|e| {
            io_err(format!(
                "failed to rename index file '{}' → '{}': {e}",
                tmp_idx.display(),
                index_path.display()
            ))
        })?;

        Ok(SortShuffleFiles {
            data_path,
            index_path,
            partition_count: n,
        })
    }

    /// Total number of rows buffered across all partitions.
    pub fn buffered_row_count(&self) -> usize {
        self.buckets
            .iter()
            .flat_map(|b| b.iter())
            .map(|b| b.num_rows())
            .sum()
    }

    /// Total estimated in-memory bytes (excludes spilled data).
    pub fn buffered_bytes(&self) -> usize {
        self.bucket_bytes.iter().sum()
    }
}

/// Estimate the in-memory byte size of a batch.
fn estimated_batch_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/// Gather one shuffle bucket's batches (spilled files first, then in-memory),
/// concatenate, sort by the shuffle key, and IPC-encode into a byte block.
///
/// Returns an empty block when the bucket has no rows (its index offset then
/// points to valid, zero-length data). Independent per bucket, so this is the
/// unit of parallelism in [`SortShuffleWriter::flush`] (Phase 65). Reading and
/// removing the temp spill files happens here too — they are per-bucket
/// independent, and `flush` is a synchronous call (no Tokio reactor to protect).
fn encode_bucket(
    bucket: &[RecordBatch],
    spills: Option<&Vec<PathBuf>>,
    sort_key: &str,
) -> ShuffleResult<Vec<u8>> {
    let has_spills = spills.is_some_and(|f| !f.is_empty());
    if bucket.is_empty() && !has_spills {
        return Ok(Vec::new());
    }

    // Collect all batches: spilled files first, then in-memory.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    if let Some(spill_files) = spills {
        for spill_path in spill_files {
            let mut spilled = read_ipc_file(spill_path)?;
            all_batches.append(&mut spilled);
            // Clean up the temp spill file.
            let _ = std::fs::remove_file(spill_path);
        }
    }
    for b in bucket {
        all_batches.push(b.clone());
    }
    if all_batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = all_batches
        .first()
        .ok_or_else(|| io_err("empty batch list".to_string()))?
        .schema();
    let combined = arrow::compute::concat_batches(&schema, all_batches.iter())
        .map_err(|e| io_err(format!("concat failed: {e}")))?;
    let sorted = sort_by_key(&combined, sort_key)?;
    encode_ipc(&sorted)
}

/// Read a previously-spilled IPC file back as `Vec<RecordBatch>`.
fn read_ipc_file(path: &std::path::Path) -> ShuffleResult<Vec<RecordBatch>> {
    use arrow::ipc::reader::StreamReader;
    let bytes = std::fs::read(path).map_err(|e| {
        io_err(format!(
            "failed to read spill file '{}': {e}",
            path.display()
        ))
    })?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        io_err(format!(
            "IPC reader init for '{}' failed: {e}",
            path.display()
        ))
    })?;
    reader
        .map(|b| b.map_err(|e| io_err(format!("IPC read from '{}' failed: {e}", path.display()))))
        .collect()
}

/// Sort `batch` by the column named `key` in ascending, nulls-last order.
fn sort_by_key(batch: &RecordBatch, key: &str) -> ShuffleResult<RecordBatch> {
    let schema = batch.schema();
    let col_idx = schema
        .index_of(key)
        .map_err(|e| io_err(format!("sort key column '{key}' not found: {e}")))?;
    let col = batch.column(col_idx);

    // Validate key type (same set as HashPartitioner).
    match col.data_type() {
        DataType::Int32
        | DataType::Int64
        | DataType::Utf8
        | DataType::Utf8View
        | DataType::LargeUtf8 => {}
        other => {
            return Err(io_err(format!(
                "unsupported sort key type for column '{key}': {other}"
            )));
        }
    }

    let sort_col = SortColumn {
        values: Arc::clone(col),
        options: Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
    };
    let indices = lexsort_to_indices(&[sort_col], None)
        .map_err(|e| io_err(format!("sort failed for key '{key}': {e}")))?;

    let columns: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .map(|c| {
            take(c.as_ref(), &indices, None)
                .map_err(|e| io_err(format!("take after sort failed: {e}")))
        })
        .collect::<ShuffleResult<_>>()?;

    RecordBatch::try_new(schema, columns)
        .map_err(|e| io_err(format!("RecordBatch rebuild after sort failed: {e}")))
}

/// Encode a single `RecordBatch` as an Arrow IPC stream (no compression).
fn encode_ipc(batch: &RecordBatch) -> ShuffleResult<Vec<u8>> {
    let mut buf = Vec::new();
    let cursor = Cursor::new(&mut buf);
    let opts = IpcWriteOptions::default();
    let mut writer = StreamWriter::try_new_with_options(cursor, batch.schema_ref(), opts)
        .map_err(|e| io_err(format!("IPC writer init failed: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| io_err(format!("IPC write failed: {e}")))?;
    writer
        .finish()
        .map_err(|e| io_err(format!("IPC finish failed: {e}")))?;
    drop(writer);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    fn make_batch(keys: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(keys.iter().copied()))],
        )
        .unwrap()
    }

    #[test]
    fn sort_shuffle_writer_roundtrip_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SortShuffleWriter::new(
            "job1",
            "stage1",
            HashPartitioner::new("k", 4),
            "k",
            dir.path(),
        )
        .unwrap();

        // Push rows across two batches so the concat+sort path is exercised.
        writer.push(make_batch(&[8, 1, 3, 0])).unwrap();
        writer.push(make_batch(&[9, 2, 7, 4])).unwrap();

        let files = writer.flush().unwrap();
        assert_eq!(files.partition_count, 4);
        assert!(files.data_path.exists(), "data file must exist");
        assert!(files.index_path.exists(), "index file must exist");

        let offsets = files.read_offsets().unwrap();
        assert_eq!(
            offsets.len(),
            5,
            "index must have partition_count + 1 entries"
        );
        // Offsets must be monotonically non-decreasing.
        for w in offsets.windows(2) {
            assert!(w[1] >= w[0], "offsets must be non-decreasing");
        }
        // Last offset equals data file length.
        let data_len = std::fs::metadata(&files.data_path).unwrap().len();
        assert_eq!(
            *offsets.last().unwrap(),
            data_len,
            "last index offset must equal data file length"
        );
    }

    #[test]
    fn sort_shuffle_writer_empty_push() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SortShuffleWriter::new(
            "job2",
            "stage1",
            HashPartitioner::new("k", 2),
            "k",
            dir.path(),
        )
        .unwrap();
        // No rows pushed — flush should still produce valid (empty) files.
        writer.push(make_batch(&[])).unwrap();
        let files = writer.flush().unwrap();
        let offsets = files.read_offsets().unwrap();
        assert_eq!(offsets, vec![0u64, 0u64, 0u64]);
        let data_len = std::fs::metadata(&files.data_path).unwrap().len();
        assert_eq!(data_len, 0, "empty data file");
    }

    #[test]
    fn sort_shuffle_writer_rejects_zero_partitions() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            SortShuffleWriter::new("j", "s", HashPartitioner::new("k", 0), "k", dir.path())
                .is_err()
        );
    }

    #[test]
    fn sort_by_key_sorts_ascending() {
        let batch = make_batch(&[5, 2, 8, 1]);
        let sorted = sort_by_key(&batch, "k").unwrap();
        let arr = sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(values, vec![1, 2, 5, 8]);
    }

    /// The spill threshold must actually bound buffered memory.
    ///
    /// With many partitions, one spill per push reclaims only about `1/n` of
    /// what is buffered, so a push that adds more than that leaves the writer
    /// permanently over the threshold. Twelve pushes into sixteen partitions
    /// with a 4 KB budget is enough to separate the two behaviours: the old
    /// single-`if` code ends far above the threshold, the loop ends at or
    /// under it.
    #[test]
    fn the_spill_threshold_actually_bounds_buffered_memory() {
        let dir = tempfile::tempdir().unwrap();
        let threshold = 4096u64;
        let mut writer = SortShuffleWriter::new_with_spill_threshold(
            "job-bound",
            "stage1",
            HashPartitioner::new("k", 16),
            "k",
            dir.path(),
            threshold,
        )
        .unwrap();

        for round in 0..12i64 {
            let keys: Vec<i64> = (0..512).map(|i| round * 512 + i).collect();
            writer.push(make_batch(&keys)).unwrap();
            assert!(
                writer.buffered_bytes() as u64 <= threshold,
                "after push {round} the writer buffers {} bytes against a {threshold}-byte \
                 threshold; spilling one bucket per push reclaims ~1/n and never catches up",
                writer.buffered_bytes()
            );
        }

        // And the data still round-trips through all those spills.
        let files = writer.flush().unwrap();
        let offsets = files.read_offsets().unwrap();
        assert_eq!(offsets.len(), 17);
        let data_len = std::fs::metadata(&files.data_path).unwrap().len();
        assert_eq!(*offsets.last().unwrap(), data_len);
        assert!(data_len > 0, "6144 rows must survive the spill cycle");
    }

    /// Two tasks of the same stage collide on every output path.
    ///
    /// This is a design gap, not a typo, and it is why the sort-shuffle writer
    /// must not be wired as-is. Every filename is built from `job_id` and
    /// `stage_id` only — `{job}_{stage}.data`, `{job}_{stage}.index`, and
    /// `{job}_{stage}_{bucket}_spill{seq}.ipc` — with no map-task identity
    /// anywhere. A stage has one task per partition and this engine runs three
    /// slots per executor, so two tasks of the same stage on one executor
    /// overwrite each other's data and index files and the second `register()`
    /// into `SortShuffleIndex` (keyed `(job_id, stage_id)`) evicts the first.
    /// The reduce side then reads one task's output and reports success.
    ///
    /// Spark's equivalent layout is per map task
    /// (`shuffle_<shuffleId>_<mapId>_0.data`); `mapId` has no counterpart here.
    ///
    /// The test asserts the collision rather than the fix, so it documents the
    /// blocker precisely and will fail the moment someone adds task identity —
    /// at which point it should be inverted into the correctness check.
    #[test]
    fn two_tasks_of_one_stage_overwrite_each_others_output() {
        let dir = tempfile::tempdir().unwrap();

        let mut task_a = SortShuffleWriter::new(
            "job-x",
            "stage-0",
            HashPartitioner::new("k", 2),
            "k",
            dir.path(),
        )
        .unwrap();
        task_a.push(make_batch(&[1, 2, 3, 4])).unwrap();
        let files_a = task_a.flush().unwrap();
        assert_eq!(keys_in(&files_a), vec![1, 2, 3, 4], "task A wrote its rows");

        let mut task_b = SortShuffleWriter::new(
            "job-x",
            "stage-0",
            HashPartitioner::new("k", 2),
            "k",
            dir.path(),
        )
        .unwrap();
        task_b.push(make_batch(&[5, 6, 7, 8])).unwrap();
        let files_b = task_b.flush().unwrap();

        assert_eq!(
            files_a.data_path, files_b.data_path,
            "no map-task identity in the path, so the second task writes the first task's file"
        );
        // Byte length is not a witness here: 4 and 6 Int64 rows pad to the same
        // IPC block size, so only the values show the loss.
        assert_eq!(
            keys_in(&files_a),
            vec![5, 6, 7, 8],
            "task B's rows replaced task A's in the shared file; A's rows are gone \
             and neither writer reported an error"
        );
    }

    /// Keys per partition, in index order, read through the index offsets.
    fn keys_by_partition(files: &SortShuffleFiles) -> Vec<Vec<i64>> {
        use arrow::ipc::reader::StreamReader;
        let data = std::fs::read(&files.data_path).unwrap();
        let offsets = files.read_offsets().unwrap();
        let mut out = Vec::new();
        for w in offsets.windows(2) {
            let (start, end) = (w[0] as usize, w[1] as usize);
            let mut keys = Vec::new();
            if end > start {
                let reader = StreamReader::try_new(Cursor::new(&data[start..end]), None).unwrap();
                for batch in reader {
                    let batch = batch.unwrap();
                    let arr = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    keys.extend((0..arr.len()).map(|i| arr.value(i)));
                }
            }
            keys.sort_unstable();
            out.push(keys);
        }
        out
    }

    /// The writer must place rows exactly where the partitioner it was given
    /// places them.
    ///
    /// It used to build its own `HashPartitioner` from the key column, which
    /// defaults to seed 0. The executor's store path seeds from the job id, so
    /// the ESS index and the shuffle store — written from the *same* batches in
    /// the same map task — disagreed about which partition a row belonged to.
    /// A reduce task reading the ESS side then missed rows for its key and the
    /// query still reported success.
    ///
    /// Asserting against a *seeded* partitioner is what makes this a guard: with
    /// seed 0 the two agree by accident and any test would pass.
    #[test]
    fn writer_places_rows_where_the_given_partitioner_says() {
        let dir = tempfile::tempdir().unwrap();
        let partitioner = HashPartitioner::new("k", 4).with_seed(0x5DEE_CE66_D125_1F13);
        let batch = make_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

        let expected: Vec<Vec<i64>> = partitioner
            .partition(&batch)
            .unwrap()
            .iter()
            .map(|b| {
                let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let mut keys: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
                keys.sort_unstable();
                keys
            })
            .collect();

        // Prove this assertion can actually fail: the partitioner the buggy
        // version built for itself (same key, same bucket count, default seed)
        // must disagree with the seeded one. Without this the test would still
        // pass against the bug if seed 0 happened to agree.
        let unseeded: Vec<Vec<i64>> = HashPartitioner::new("k", 4)
            .partition(&batch)
            .unwrap()
            .iter()
            .map(|b| {
                let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let mut keys: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
                keys.sort_unstable();
                keys
            })
            .collect();
        assert_ne!(
            unseeded, expected,
            "test is not discriminating: pick keys where seed 0 and the job seed differ"
        );

        let mut writer = SortShuffleWriter::new(
            "job-seeded",
            "stage-0",
            partitioner.clone(),
            "k",
            dir.path(),
        )
        .unwrap();
        writer.push(batch).unwrap();
        let files = writer.flush().unwrap();

        assert_eq!(
            keys_by_partition(&files),
            expected,
            "the writer must not re-derive its own partitioner; an unseeded one sends \
             the same key to a different partition than the store path does"
        );
    }

    /// Every key in a sort-shuffle data file, read through its index offsets.
    fn keys_in(files: &SortShuffleFiles) -> Vec<i64> {
        use arrow::ipc::reader::StreamReader;
        let data = std::fs::read(&files.data_path).unwrap();
        let offsets = files.read_offsets().unwrap();
        let mut keys = Vec::new();
        for w in offsets.windows(2) {
            let (start, end) = (w[0] as usize, w[1] as usize);
            if end <= start {
                continue;
            }
            let reader = StreamReader::try_new(Cursor::new(&data[start..end]), None).unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                keys.extend((0..arr.len()).map(|i| arr.value(i)));
            }
        }
        keys.sort_unstable();
        keys
    }

    #[test]
    fn spill_is_transparent_to_final_output() {
        // Use the builder with an explicit spill threshold of 1 byte so that
        // every push triggers a spill without touching process-global env state.
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SortShuffleWriter::new_with_spill_threshold(
            "job3",
            "stage1",
            HashPartitioner::new("k", 2),
            "k",
            dir.path(),
            1,
        )
        .unwrap();

        // Push two batches; each push will trigger a spill due to the tiny threshold.
        writer.push(make_batch(&[4, 2])).unwrap();
        writer.push(make_batch(&[1, 3])).unwrap();

        let files = writer.flush().unwrap();
        assert!(files.data_path.exists());
        let offsets = files.read_offsets().unwrap();
        assert_eq!(offsets.len(), 3, "2 partitions + 1 sentinel");
        // Last offset = data file length.
        let data_len = std::fs::metadata(&files.data_path).unwrap().len();
        assert_eq!(*offsets.last().unwrap(), data_len);

        // All rows (4 total) must be present in output.
        let data = std::fs::read(&files.data_path).unwrap();
        assert!(!data.is_empty(), "data file must be non-empty");
    }
}
