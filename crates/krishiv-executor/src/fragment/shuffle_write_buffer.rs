//! Bounded, spilling accumulator for a map task's hash-partitioned output.
//!
//! # The failure this fixes
//!
//! TPC-H q8 and q9 at SF100 on a 3-node cluster, executors capped at 4500 MiB
//! with a 2392 MiB DataFusion pool. Every kill looked the same:
//!
//! ```text
//! anon-rss 4.59 GiB   file-rss 40 MB   → SIGKILL (exit 137)
//! ```
//!
//! Pure heap, ~2.2 GiB *above* the pool, and no `Resources exhausted` error
//! anywhere — a pool-tracked operator would have failed cleanly the way q18's
//! hash join does. The allocations never went through the pool at all.
//!
//! The killer was always the same stage: `dist-s4` in both queries, which is
//! the raw `lineitem` scan hash-partitioned by `l_partkey` (q8 projects 5
//! columns ≈ 56 B/row, q9 projects 6 ≈ 72 B/row). Its fragment holds no join,
//! so the dynamic filter that would prune it has no build side and never
//! fires: all 600 M rows are shuffled. At 18 tasks that is 33 M rows —
//! 1.9 GiB (q8) to 2.4 GiB (q9) of Arrow data per task. TPC-H q6 scans the
//! same table in the same topology and peaks at 118 MB, because *its* map
//! stage is a partial aggregate: it is the shuffle *output* that is large,
//! not the scan.
//!
//! And the map task held that output whole. The write loop accumulated
//! `Vec<Vec<RecordBatch>>` — every partition of the entire task output —
//! because [`krishiv_shuffle::ShuffleStore::write_partition`] takes a complete
//! partition and has no append. A `MemoryBudget` guard was already wrapped
//! around that buffer, but it is built from
//! `ExecutorTaskAssignment::memory_limit_bytes`, which is only ever populated
//! from the submitted `JobSpec`'s optional namespace quota. Batch queries do
//! not set one, so the guard resolved to `MemoryBudget::unlimited()` and never
//! fired: the code that would have reported the overflow was disarmed on
//! exactly the deployment that overflowed.
//!
//! # The fix
//!
//! Two changes, both structural:
//!
//! 1. **The buffer is a first-class consumer of the task's DataFusion memory
//!    pool.** Bytes are reserved through a [`MemoryReservation`] like any
//!    spilling operator, so the pool that was sized to fit the container now
//!    accounts for 100% of the query heap rather than for everything except
//!    its single largest holder.
//! 2. **It spills.** When the pool refuses to grow — or when the in-memory
//!    total passes the soft ceiling — the largest partition is concatenated,
//!    written to a local Arrow IPC file, and its memory released. Draining
//!    reads the spill files back one partition at a time.
//!
//! Peak memory becomes `soft ceiling + one partition`, independent of input
//! size, instead of the whole map output. When even that cannot be satisfied
//! the pool returns its ordinary `Resources exhausted` error, so the failure
//! mode is a named, retryable task error rather than a SIGKILL that takes
//! every sibling task on the executor down with it.
//!
//! Spill files are fsynced and then dropped from the page cache before the
//! next write. Page cache on shuffle files is charged to the container's
//! cgroup, and trading a heap overrun for a page-cache overrun would just
//! re-open the bug this executor already fixed once.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use krishiv_sql::{MemoryConsumer, MemoryPool, MemoryReservation};

use crate::{ExecutorError, ExecutorResult};

/// Default in-memory ceiling for one map task's shuffle output before it
/// starts spilling, when no pool bound is tighter.
///
/// Matches [`krishiv_shuffle::sort_shuffle_writer::DEFAULT_SPILL_THRESHOLD_BYTES`]
/// — the two writers buffer the same thing for the same reason and there is no
/// case for them disagreeing about how much of it may sit in memory.
pub(crate) const DEFAULT_BUFFER_BYTES: u64 = 512 * 1024 * 1024;

/// Environment override for [`DEFAULT_BUFFER_BYTES`], shared with the
/// sort-shuffle writer.
pub(crate) const BUFFER_BYTES_ENV: &str = "KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES";

/// Resolve the in-memory ceiling from the environment.
pub(crate) fn buffer_bytes_from_env() -> u64 {
    std::env::var(BUFFER_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_BUFFER_BYTES)
}

/// Directory spill files are written to.
///
/// Callers pass the executor's shuffle scratch directory when one is
/// configured, so spill traffic lands on the disk already provisioned for
/// shuffle output rather than on the container's root filesystem.
pub(crate) fn default_spill_dir() -> PathBuf {
    std::env::temp_dir().join("krishiv-shuffle-write-spill")
}

/// Process-unique suffix so two tasks cannot collide on a spill file name.
static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

fn io_err(context: &str, e: &dyn std::fmt::Display) -> ExecutorError {
    ExecutorError::LocalExecution {
        message: format!("{context}: {e}"),
    }
}

/// One spilled run of a single partition. Deletes itself when dropped, so an
/// abandoned task (cancel, error) leaves nothing behind on the scratch disk.
#[derive(Debug)]
struct SpillRun {
    path: PathBuf,
    rows: usize,
}

impl Drop for SpillRun {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A partition's batches handed back to the caller, together with the pool
/// reservation covering them.
///
/// The reservation lives in this struct so it is released exactly when the
/// caller drops the batches — including on the `?` paths through the write
/// loop, which a matched grow/shrink pair would leak.
pub(crate) struct DrainedPartition {
    pub(crate) batches: Vec<RecordBatch>,
    reservation: Option<MemoryReservation>,
}

impl DrainedPartition {
    /// Split into the batches and the reservation covering them.
    ///
    /// Callers must keep the reservation alive for as long as they hold the
    /// batches — binding it to `_reservation` for the rest of the scope is the
    /// intended use, and is what keeps the pool's view honest while the
    /// partition is being written out.
    pub(crate) fn into_parts(self) -> (Vec<RecordBatch>, Option<MemoryReservation>) {
        (self.batches, self.reservation)
    }
}

/// Hash-partitioned map output, bounded in memory and spilled to local disk.
///
/// Push every bucket batch with [`push`](Self::push), then take each partition
/// exactly once with [`drain_partition`](Self::drain_partition).
pub(crate) struct ShuffleWriteBuffer {
    /// In-memory batches per output partition.
    buckets: Vec<Vec<RecordBatch>>,
    /// Accounted in-memory bytes per output partition.
    bucket_bytes: Vec<usize>,
    /// Spilled runs per output partition, oldest first — the read-back order
    /// is the push order, so row order within a partition survives spilling.
    spills: Vec<Vec<SpillRun>>,
    /// Covers `bucket_bytes.iter().sum()`; `None` when no pool is configured.
    reservation: Option<MemoryReservation>,
    /// The pool, kept so each drained partition gets its own reservation.
    pool: Option<Arc<dyn MemoryPool>>,
    /// Soft in-memory ceiling; spilling starts here even if the pool would
    /// still grow, so one map task cannot evict every other operator sharing
    /// the pool just because it happened to ask first.
    soft_limit_bytes: u64,
    spill_dir: PathBuf,
}

impl ShuffleWriteBuffer {
    /// Create a buffer for `num_partitions` output partitions.
    ///
    /// `pool` is the task engine's DataFusion memory pool; `None` (an
    /// unbounded engine, as in unit tests and un-capped local runs) leaves the
    /// soft ceiling as the only bound, which is still bounded.
    pub(crate) fn new(
        num_partitions: usize,
        pool: Option<Arc<dyn MemoryPool>>,
        soft_limit_bytes: u64,
        spill_dir: PathBuf,
    ) -> Self {
        let reservation = pool.as_ref().map(|p| {
            MemoryConsumer::new("ShuffleWriteBuffer")
                .with_can_spill(true)
                .register(p)
        });
        Self {
            buckets: vec![Vec::new(); num_partitions],
            bucket_bytes: vec![0; num_partitions],
            spills: (0..num_partitions).map(|_| Vec::new()).collect(),
            reservation,
            pool,
            soft_limit_bytes,
            spill_dir,
        }
    }

    /// Total in-memory bytes currently held across all partitions.
    pub(crate) fn buffered_bytes(&self) -> usize {
        self.bucket_bytes.iter().sum()
    }

    /// Number of spilled runs across all partitions (diagnostics and tests).
    pub(crate) fn spill_count(&self) -> usize {
        self.spills.iter().map(Vec::len).sum()
    }

    /// Accumulate `batch` into output partition `index`, spilling first if it
    /// would not fit.
    pub(crate) async fn push(&mut self, index: usize, batch: RecordBatch) -> ExecutorResult<()> {
        if batch.num_rows() == 0 || index >= self.buckets.len() {
            return Ok(());
        }
        let bytes = batch.get_array_memory_size();
        self.make_room_for(bytes).await?;
        if let Some(slot) = self.bucket_bytes.get_mut(index) {
            *slot += bytes;
        }
        if let Some(bucket) = self.buckets.get_mut(index) {
            bucket.push(batch);
        }
        Ok(())
    }

    /// Reserve `bytes`, spilling the largest partition as many times as it
    /// takes. Fails only when nothing is left to spill and the pool still
    /// refuses — the honest "this task cannot run in this much memory".
    async fn make_room_for(&mut self, bytes: usize) -> ExecutorResult<()> {
        loop {
            let over_soft_limit =
                (self.buffered_bytes() + bytes) as u64 > self.soft_limit_bytes && bytes > 0;
            if !over_soft_limit {
                match &self.reservation {
                    Some(reservation) => match reservation.try_grow(bytes) {
                        Ok(()) => return Ok(()),
                        Err(pool_error) => {
                            if !self.spill_largest().await? {
                                return Err(ExecutorError::LocalExecution {
                                    message: format!(
                                        "shuffle write buffer could not reserve {bytes} bytes \
                                         from the task memory pool with nothing left to spill: \
                                         {pool_error}"
                                    ),
                                });
                            }
                            continue;
                        }
                    },
                    None => return Ok(()),
                }
            }
            if !self.spill_largest().await? {
                // Nothing in memory to spill and still over the soft ceiling:
                // this single batch is larger than the whole allowance. Let it
                // through rather than failing — one batch has to be resident
                // for the partitioner to have produced it at all, and the pool
                // (checked above on the next iteration) remains the hard bound.
                if let Some(reservation) = &self.reservation {
                    reservation
                        .try_grow(bytes)
                        .map_err(|e| ExecutorError::LocalExecution {
                            message: format!(
                                "shuffle write buffer could not reserve a single {bytes}-byte \
                                 batch from the task memory pool: {e}"
                            ),
                        })?;
                }
                return Ok(());
            }
        }
    }

    /// Spill the largest in-memory partition to a local Arrow IPC file.
    ///
    /// Returns `false` when every partition is already empty, which is the
    /// caller's signal that spilling cannot free anything more.
    async fn spill_largest(&mut self) -> ExecutorResult<bool> {
        let Some((index, _)) = self
            .bucket_bytes
            .iter()
            .enumerate()
            .filter(|&(i, _)| self.buckets.get(i).is_some_and(|b| !b.is_empty()))
            .max_by_key(|&(_, bytes)| bytes)
        else {
            return Ok(false);
        };
        let batches = self
            .buckets
            .get_mut(index)
            .map(std::mem::take)
            .unwrap_or_default();
        if batches.is_empty() {
            return Ok(false);
        }
        let freed = self
            .bucket_bytes
            .get_mut(index)
            .map(|slot| std::mem::replace(slot, 0))
            .unwrap_or(0);

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let seq = SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
        // Two placement rules, both about the sweepers that already run over
        // the shuffle scratch directory:
        //
        //  * flat, not in a subdirectory — `krishiv_shuffle::scan_orphans`
        //    treats every *directory* under the scratch root as a job id and
        //    deletes the artifacts inside any it does not recognise, which
        //    would delete live spill runs mid-task. It skips plain files.
        //  * `.tmp.` in the name — `LocalDiskShuffleStore::cleanup_temp_files`
        //    removes those at store construction, i.e. at executor boot and
        //    never concurrently with a task, so runs orphaned by a SIGKILL are
        //    reclaimed on the next start instead of accumulating.
        let path = self.spill_dir.join(format!(
            "shuffle-write-{}-{seq}-p{index}.tmp.arrow-ipc",
            std::process::id()
        ));
        let dir = self.spill_dir.clone();
        let write_path = path.clone();

        // #223: the IPC write is synchronous `std::fs` work with no await of
        // its own. Run inline it would give task cancellation nothing to
        // interrupt for as long as the write takes; `spawn_blocking` gives the
        // awaiting future a real yield point.
        let task = tokio::task::spawn_blocking(move || -> ExecutorResult<()> {
            std::fs::create_dir_all(&dir).map_err(|e| io_err("create shuffle spill dir", &e))?;
            let schema = match batches.first() {
                Some(batch) => batch.schema(),
                None => return Ok(()),
            };
            let file = std::fs::File::create(&write_path)
                .map_err(|e| io_err("create shuffle spill", &e))?;
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(std::io::BufWriter::new(file), &schema)
                    .map_err(|e| io_err("open shuffle spill writer", &e))?;
            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|e| io_err("shuffle spill write", &e))?;
            }
            writer
                .finish()
                .map_err(|e| io_err("finish shuffle spill", &e))?;
            let file = writer
                .into_inner()
                .map_err(|e| io_err("close shuffle spill writer", &e))?
                .into_inner()
                .map_err(|e| io_err("flush shuffle spill", &e))?;
            // Durable first, then evicted: `DONTNEED` silently skips dirty
            // pages, so evicting before the fsync would leave the container
            // charged for every byte just written.
            file.sync_all()
                .map_err(|e| io_err("fsync shuffle spill", &e))?;
            krishiv_common::page_cache::evict_file_best_effort(&file);
            Ok(())
        });
        task.await.map_err(|e| io_err("shuffle spill join", &e))??;

        if let Some(runs) = self.spills.get_mut(index) {
            runs.push(SpillRun { path, rows });
        }
        if let Some(reservation) = &self.reservation
            && freed > 0
        {
            reservation.shrink(freed.min(reservation.size()));
        }
        tracing::debug!(
            partition = index,
            freed_bytes = freed,
            rows,
            total_spilled_runs = self.spill_count(),
            "spilled shuffle write buffer partition"
        );
        Ok(true)
    }

    /// Take every batch of output partition `index`, in push order.
    ///
    /// Peak memory for the call is that one partition, which is the smallest
    /// unit [`krishiv_shuffle::ShuffleStore::write_partition`] can accept.
    pub(crate) async fn drain_partition(
        &mut self,
        index: usize,
    ) -> ExecutorResult<DrainedPartition> {
        let in_memory = self
            .buckets
            .get_mut(index)
            .map(std::mem::take)
            .unwrap_or_default();
        let in_memory_bytes = self
            .bucket_bytes
            .get_mut(index)
            .map(|slot| std::mem::replace(slot, 0))
            .unwrap_or(0);
        // Hand the bytes over to the drained partition's own reservation
        // rather than double-counting them across both.
        if let Some(reservation) = &self.reservation
            && in_memory_bytes > 0
        {
            reservation.shrink(in_memory_bytes.min(reservation.size()));
        }
        let runs = self
            .spills
            .get_mut(index)
            .map(std::mem::take)
            .unwrap_or_default();
        if !runs.is_empty() {
            tracing::debug!(
                partition = index,
                runs = runs.len(),
                rows = runs.iter().map(|r| r.rows).sum::<usize>(),
                "reading spilled shuffle write runs back for one partition"
            );
        }

        let drained_reservation = self.pool.as_ref().map(|p| {
            MemoryConsumer::new("ShuffleWriteBufferDrain")
                .with_can_spill(false)
                .register(p)
        });
        if let Some(reservation) = &drained_reservation {
            account_unavoidable(reservation, in_memory_bytes, index);
        }

        let mut batches = Vec::new();
        for run in runs {
            let path = run.path.clone();
            let task = tokio::task::spawn_blocking(move || -> ExecutorResult<Vec<RecordBatch>> {
                let file =
                    std::fs::File::open(&path).map_err(|e| io_err("open shuffle spill", &e))?;
                let reader =
                    arrow::ipc::reader::StreamReader::try_new(std::io::BufReader::new(file), None)
                        .map_err(|e| io_err("open shuffle spill reader", &e))?;
                let mut out = Vec::new();
                for batch in reader {
                    out.push(batch.map_err(|e| io_err("read shuffle spill", &e))?);
                }
                Ok(out)
            });
            let restored = task
                .await
                .map_err(|e| io_err("shuffle spill read join", &e))??;
            // The run's file is no longer needed; drop it (which unlinks) and
            // release its page cache before reading the next one.
            evict_and_drop(run);
            if let Some(reservation) = &drained_reservation {
                let bytes: usize = restored
                    .iter()
                    .map(RecordBatch::get_array_memory_size)
                    .sum();
                account_unavoidable(reservation, bytes, index);
            }
            batches.extend(restored);
        }
        batches.extend(in_memory);
        Ok(DrainedPartition {
            batches,
            reservation: drained_reservation,
        })
    }

    /// Build the buffer a map task should use: bounded by the task engine's
    /// own DataFusion pool, spilling into the executor's shuffle scratch
    /// directory when one is configured.
    pub(crate) fn for_task(
        num_partitions: usize,
        engine: &krishiv_sql::SqlEngine,
        spill_dir: Option<PathBuf>,
    ) -> Self {
        let pool = Arc::clone(&engine.session_context().runtime_env().memory_pool);
        Self::new(
            num_partitions,
            Some(pool),
            buffer_bytes_from_env(),
            spill_dir.unwrap_or_else(default_spill_dir),
        )
    }
}

/// Record bytes the drain path has no way to avoid holding.
///
/// One whole output partition has to be resident: it is the smallest unit
/// [`krishiv_shuffle::ShuffleStore::write_partition`] accepts, and there is
/// nothing left to spill once it is being handed over. So the reservation is
/// grown even when the pool would refuse — refusing here would fail a query
/// that can actually run, and *skipping* the accounting is precisely the bug
/// this module exists to fix: an unrecorded allocation is one the pool cannot
/// make anyone else back off for. The overshoot is logged rather than hidden.
fn account_unavoidable(reservation: &MemoryReservation, bytes: usize, partition: usize) {
    if bytes == 0 {
        return;
    }
    if let Err(error) = reservation.try_grow(bytes) {
        tracing::warn!(
            partition,
            bytes,
            %error,
            "shuffle write drain exceeded the task memory pool holding one output \
             partition; recording the allocation so the pool stays honest. Raise the \
             stage's partition count to make each partition smaller."
        );
        reservation.grow(bytes);
    }
}

/// Drop a spilled run's page cache, then the file itself.
fn evict_and_drop(run: SpillRun) {
    evict_path(&run.path);
    drop(run);
}

fn evict_path(path: &Path) {
    krishiv_common::page_cache::evict_path_best_effort(path);
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    /// The same `FairSpillPool` shape a real task engine gets, so these tests
    /// exercise production's pool semantics rather than a simpler stand-in.
    fn pool_of(bytes: usize) -> Arc<dyn MemoryPool> {
        krishiv_sql::EngineMemory::shared_pool(bytes)
    }

    fn batch(start: i64, rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..rows as i64).map(|i| start + i).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn int_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column")
            .values()
            .to_vec()
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "krishiv-shuffle-write-test-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// The regression this whole module exists for: the map-side buffer must
    /// be visible to the task's DataFusion memory pool.
    ///
    /// Before the fix the buffer was a plain `Vec<Vec<RecordBatch>>` guarded
    /// by a `MemoryBudget` that production never armed, so the pool reported
    /// zero while the process walked past its cgroup limit and was
    /// SIGKILLed. Asserting the pool sees *something* is what makes this test
    /// fail against that design — a test that only asserted "stays under the
    /// limit" would have passed vacuously on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffered_bytes_are_reserved_against_the_task_memory_pool() {
        let pool = pool_of(64 * 1024 * 1024);
        let mut buffer = ShuffleWriteBuffer::new(
            4,
            Some(Arc::clone(&pool)),
            64 * 1024 * 1024,
            temp_dir("acct"),
        );
        for i in 0..8 {
            buffer
                .push(i % 4, batch(i as i64 * 1000, 1024))
                .await
                .unwrap();
        }
        assert!(
            pool.reserved() > 0,
            "the shuffle write buffer must reserve its batches against the pool, got {}",
            pool.reserved()
        );
        assert_eq!(
            pool.reserved(),
            buffer.buffered_bytes(),
            "pool accounting must match what the buffer actually holds"
        );
        assert_eq!(
            buffer.spill_count(),
            0,
            "nothing should spill under the cap"
        );
    }

    /// A map output far larger than the pool must spill rather than grow, and
    /// the pool must never be exceeded while it does.
    ///
    /// The soft ceiling is disabled here (`u64::MAX`) so the *pool* is the only
    /// thing that can trigger a spill — that is the bound that matters in
    /// production, where the pool is sized from the container limit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn output_larger_than_the_pool_spills_instead_of_growing() {
        const POOL_BYTES: usize = 512 * 1024;
        let pool = pool_of(POOL_BYTES);
        let mut buffer =
            ShuffleWriteBuffer::new(4, Some(Arc::clone(&pool)), u64::MAX, temp_dir("spill"));
        // 64 batches x 8192 i64 rows = 4 MiB of payload through a 512 KiB pool.
        for i in 0..64 {
            buffer
                .push(i % 4, batch(i as i64 * 8192, 8192))
                .await
                .unwrap();
            assert!(
                pool.reserved() <= POOL_BYTES,
                "pool overrun at push {i}: {} > {POOL_BYTES}",
                pool.reserved()
            );
        }
        assert!(
            buffer.spill_count() > 0,
            "an output 8x the pool must have spilled"
        );
        assert!(
            buffer.buffered_bytes() <= POOL_BYTES,
            "in-memory residue must stay under the ceiling"
        );
    }

    /// Spilling must not lose or reorder a single row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_row_survives_spilling_in_push_order() {
        const POOL_BYTES: usize = 256 * 1024;
        let pool = pool_of(POOL_BYTES);
        let mut buffer =
            ShuffleWriteBuffer::new(3, Some(pool), POOL_BYTES as u64, temp_dir("rows"));
        let mut expected: Vec<Vec<i64>> = vec![Vec::new(); 3];
        for i in 0..48i64 {
            let partition = (i % 3) as usize;
            let b = batch(i * 4096, 4096);
            let column = int_values(&b);
            expected
                .get_mut(partition)
                .expect("partition in range")
                .extend(column);
            buffer.push(partition, b).await.unwrap();
        }
        assert!(
            buffer.spill_count() > 0,
            "test must actually exercise spill"
        );

        for (partition, want) in expected.iter().enumerate() {
            let drained = buffer.drain_partition(partition).await.unwrap();
            let actual: Vec<i64> = drained.batches.iter().flat_map(int_values).collect();
            assert_eq!(
                &actual, want,
                "partition {partition} lost or reordered rows across the spill boundary"
            );
        }
    }

    /// Draining releases everything: no reservation may outlive the batches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn draining_every_partition_returns_the_pool_to_zero() {
        let pool = pool_of(2 * 1024 * 1024);
        let mut buffer =
            ShuffleWriteBuffer::new(2, Some(Arc::clone(&pool)), 1024 * 1024, temp_dir("release"));
        for i in 0..32 {
            buffer
                .push(i % 2, batch(i as i64 * 4096, 4096))
                .await
                .unwrap();
        }
        for partition in 0..2 {
            let drained = buffer.drain_partition(partition).await.unwrap();
            drop(drained);
        }
        drop(buffer);
        assert_eq!(
            pool.reserved(),
            0,
            "every reservation must be released once the batches are gone"
        );
    }

    /// Without a pool the soft ceiling is still a real bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_soft_ceiling_bounds_a_poolless_engine() {
        const CEILING: u64 = 256 * 1024;
        let mut buffer = ShuffleWriteBuffer::new(4, None, CEILING, temp_dir("nopool"));
        for i in 0..32 {
            buffer
                .push(i % 4, batch(i as i64 * 4096, 4096))
                .await
                .unwrap();
            assert!(
                buffer.buffered_bytes() as u64 <= CEILING,
                "unbounded engine still buffered {} bytes past the ceiling",
                buffer.buffered_bytes()
            );
        }
        assert!(buffer.spill_count() > 0);
    }
}
