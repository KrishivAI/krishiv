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
//! size, instead of the whole map output.
//!
//! What it deliberately does **not** do is fail the task when the pool is
//! full. Spilling frees memory only while something is buffered; once every
//! partition is on disk, the batch in hand already exists and dropping it
//! would lose rows. Those allocations are admitted and *recorded* (see
//! [`account_unavoidable`]). The first version refused them instead, and live
//! TPC-H q3 found the hole within one sweep: the process-wide `FairSpillPool`
//! gives each spillable consumer `(pool - unspillable) / num_spill`, so a hash
//! join build side on the same fragment could leave this buffer unable to grow
//! on its FIRST batch, with nothing buffered to spill. The map task then failed
//! deterministically — which the coordinator sees only as a consumer reporting
//! a missing upstream partition, so it regenerated the producer eight times and
//! failed the job on the regeneration budget instead of surfacing the real
//! error. Bounding memory is the point of this type; failing queries that fit
//! is not.
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

/// Byte target for one coalesced shuffle-output batch.
///
/// Large enough that the downstream columnar reader is not paying per-batch
/// overhead on thousands of tiny batches (the reason DB-3 coalesced at all),
/// small enough that building one is not itself a memory event.
pub(crate) const SHUFFLE_COALESCE_TARGET_BYTES: usize = 8 * 1024 * 1024;

/// Coalesce `batches` into groups of roughly [`SHUFFLE_COALESCE_TARGET_BYTES`].
///
/// DB-3 originally concatenated every sub-batch of an output partition into a
/// single `RecordBatch`. That is right for the common case — one small batch
/// per source batch — and wrong for exactly the partitions that hurt: a
/// partition that spilled did so *because* it did not fit, and concatenating
/// it builds a second, full-size copy alongside the first before the old one
/// is dropped. On SF100 that showed up as `ShuffleWriteBufferDrain` allocations
/// of 400–500 MB against a 797 MB shared pool.
///
/// Grouping to a byte target keeps the coalescing benefit and caps the extra
/// copy at one group. Batches already at or above the target pass through
/// untouched rather than being copied for no reason.
///
/// A failed `concat` is not fatal: the group is emitted as its original
/// batches. The partition's contents are identical either way — only the
/// batching differs — so degrading to un-coalesced output is better than
/// failing a task over a layout optimisation.
pub(crate) fn coalesce_shuffle_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    schema: &arrow::datatypes::SchemaRef,
) -> Vec<arrow::record_batch::RecordBatch> {
    use arrow::record_batch::RecordBatch;

    if batches.len() <= 1 {
        return batches;
    }
    let mut out: Vec<RecordBatch> = Vec::new();
    let mut group: Vec<RecordBatch> = Vec::new();
    let mut group_bytes = 0usize;

    // Concatenate one accumulated group, or pass it through if concat fails.
    fn flush(group: Vec<RecordBatch>, schema: &arrow::datatypes::SchemaRef, out: &mut Vec<RecordBatch>) {
        if group.len() <= 1 {
            out.extend(group);
            return;
        }
        match arrow::compute::concat_batches(schema, &group) {
            Ok(batch) => out.push(batch),
            Err(error) => {
                tracing::debug!(%error, "shuffle batch coalesce failed; writing uncoalesced");
                out.extend(group);
            }
        }
    }

    for batch in batches {
        let bytes = batch.get_array_memory_size();
        if bytes >= SHUFFLE_COALESCE_TARGET_BYTES {
            flush(std::mem::take(&mut group), schema, &mut out);
            group_bytes = 0;
            out.push(batch);
            continue;
        }
        if group_bytes + bytes > SHUFFLE_COALESCE_TARGET_BYTES && !group.is_empty() {
            flush(std::mem::take(&mut group), schema, &mut out);
            group_bytes = 0;
        }
        group_bytes += bytes;
        group.push(batch);
    }
    flush(group, schema, &mut out);
    out
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

/// What one drained partition cost, reported back to the call site so it can
/// build its `ShufflePartitionOutput` and metrics without re-walking batches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PartitionWriteStat {
    pub(crate) partition: u32,
    pub(crate) size_bytes: u64,
    pub(crate) rows: u64,
    pub(crate) write_elapsed_us: u64,
}

/// Drain every output partition of `buffer` into `store`, in partition order.
///
/// # The contract this function exists to hold
///
/// A map task must publish its **whole** partition space, including partitions
/// that received no rows. The reduce side treats the two cases differently: a
/// *local* miss reads as empty by convention, but a *remote* fetch of a
/// partition that was never written is an error, which the coordinator answers
/// by invalidating the producer and regenerating it. A producer that
/// deterministically skips an empty partition therefore regenerates into the
/// same gap until the regeneration budget is exhausted and the job fails —
/// TPC-H q3 did exactly that, eight times at ~2.5 s intervals, on one
/// partition.
///
/// The loop is `0..num_partitions`, never "the partitions that got rows", and
/// it lives here rather than being open-coded at each call site so that the
/// three map-write paths cannot drift apart on it.
///
/// `pre_write` sees each partition (schema and batches) immediately before it
/// is handed to the store — that is where the push-shuffle mirror and hot-key
/// accounting hook in.
/// Report a stage whose *produced* batches disagree with its *declared* plan
/// schema, naming both.
///
/// This disagreement is not survivable: `ShuffleReadExec` labels the reduce
/// side's stream with the coordinator's declared schema, so the reduce side
/// concatenates real IPC data against the wrong types and Arrow reports
/// `column types must match schema types, expected X but found Y at column
/// index N` — several operators away from the stage that caused it, with
/// nothing in the message identifying which stage that was. TPC-H q17 and q19
/// at SF100 both die this way, and neither error says so.
///
/// Logging at `warn` rather than failing: the write itself is well-formed and
/// self-consistent (every partition of this task carries the observed schema),
/// and a diagnostic must not be the reason a query that might still succeed
/// does not.
pub(crate) fn warn_on_schema_divergence(
    observed: Option<&arrow::datatypes::SchemaRef>,
    declared: &arrow::datatypes::SchemaRef,
) {
    let Some(observed) = observed else {
        return;
    };
    if observed.as_ref() == declared.as_ref() {
        return;
    }
    let fields = |schema: &arrow::datatypes::Schema| {
        schema
            .fields()
            .iter()
            .map(|f| format!("{}:{}", f.name(), f.data_type()))
            .collect::<Vec<_>>()
            .join(",")
    };
    tracing::warn!(
        declared = %fields(declared),
        produced = %fields(observed),
        "stage output schema disagrees with the plan's declared schema; the reduce side \
         labels this stage's data with the DECLARED schema and will reject it"
    );
}

pub(crate) async fn drain_into_store(
    buffer: &mut ShuffleWriteBuffer,
    store: &krishiv_shuffle::ShuffleBackend,
    id_for: impl Fn(u32) -> krishiv_shuffle::PartitionId,
    fallback_schema: &arrow::datatypes::SchemaRef,
    lease_token: u64,
    mut pre_write: impl FnMut(u32, &arrow::datatypes::SchemaRef, &[RecordBatch]) -> ExecutorResult<()>,
) -> ExecutorResult<Vec<PartitionWriteStat>> {
    use krishiv_shuffle::{ShufflePartition, ShuffleStore as _};

    let num_partitions = buffer.num_partitions();
    let mut stats = Vec::with_capacity(num_partitions);
    // One schema for the whole stage output, latched from the first batch that
    // actually carries data.
    //
    // This used to be decided per partition — a partition with rows took its
    // first batch's schema, an empty one took `fallback_schema` — so a stage
    // could publish partitions that disagreed with each other whenever the two
    // sources disagreed. They do disagree: `fallback_schema` is the plan's
    // *declared* schema, and physical execution re-types expressions.
    // TPC-H q17 aggregates `avg(l_quantity)`, declared `Decimal128(15, 2)` and
    // produced as `Decimal128(30, 15)`, so its empty partitions were labelled
    // one way and its full ones the other, and the reduce side rejected the
    // mixture with "column types must match schema types".
    //
    // A partition's schema must not depend on whether it happened to receive
    // rows. Prefer the observed schema (it is what the bytes are); fall back to
    // the declared one only when this task produced no rows at all, which is
    // the case `fallback_schema` exists for.
    let observed_schema = buffer.pushed_schema();
    warn_on_schema_divergence(observed_schema.as_ref(), fallback_schema);
    for index in 0..num_partitions {
        let partition = u32::try_from(index).map_err(|_| ExecutorError::LocalExecution {
            message: format!("shuffle partition index {index} exceeds u32"),
        })?;
        // `_reservation` must outlive `batches`: it is the pool's view of this
        // partition while it is concatenated and serialised.
        let (batches, _reservation) = buffer.drain_partition(index).await?.into_parts();
        let schema = observed_schema
            .clone()
            .unwrap_or_else(|| Arc::clone(fallback_schema));
        let size_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        // Coalesce the per-source-batch fragments into well-sized batches:
        // better downstream columnar throughput and less per-batch overhead in
        // the store, bounded by a byte target so a partition that spilled is
        // not rebuilt whole in memory to be written out.
        let batches = coalesce_shuffle_batches(batches, &schema);
        pre_write(partition, &schema, &batches)?;
        let write_started = std::time::Instant::now();
        store
            .write_partition(
                ShufflePartition {
                    id: id_for(partition),
                    schema,
                    batches,
                },
                lease_token,
            )
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("shuffle write failed for partition {partition}: {e}"),
            })?;
        stats.push(PartitionWriteStat {
            partition,
            size_bytes,
            rows,
            write_elapsed_us: write_started.elapsed().as_micros() as u64,
        });
    }
    Ok(stats)
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
    /// Schema of the first row-carrying batch this task pushed.
    ///
    /// Latched here, not derived per partition at drain time, because a
    /// partition's schema must not depend on whether it happened to receive
    /// rows — see `drain_into_store`.
    pushed_schema: Option<arrow::datatypes::SchemaRef>,
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
            pushed_schema: None,
        }
    }

    /// Schema of the first row-carrying batch pushed, if any.
    ///
    /// The whole stage output is labelled with this, so that a partition's
    /// schema does not depend on whether it received rows — see
    /// [`drain_into_store`].
    pub(crate) fn pushed_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
        self.pushed_schema.clone()
    }

    /// The output partition space this buffer covers.
    pub(crate) fn num_partitions(&self) -> usize {
        self.buckets.len()
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
        if self.pushed_schema.is_none() {
            self.pushed_schema = Some(batch.schema());
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
    /// takes.
    ///
    /// # Why this never fails
    ///
    /// Spilling frees memory only while there is something buffered to spill.
    /// Once every partition is on disk, the incoming batch has nowhere else to
    /// go: it already exists — the partitioner allocated it before this
    /// function was called — and dropping it would lose rows. So when the pool
    /// refuses and there is nothing left to spill, the batch is admitted and
    /// the allocation is *recorded* rather than refused.
    ///
    /// Refusing was the first version's behaviour and it was wrong in a way
    /// that live TPC-H q3 found immediately. The task engine's pool is the
    /// process-wide `FairSpillPool`; a spillable consumer's share is
    /// `(pool_size - unspillable) / num_spill`, so a hash join build side on
    /// the same fragment can leave this buffer unable to grow on its FIRST
    /// batch, with an empty buffer and therefore nothing to spill. The map
    /// task then failed deterministically — and a producer that always fails
    /// looks to the coordinator like a consumer reporting a missing upstream
    /// partition, so it regenerated the producer eight times and failed the
    /// job on the regeneration budget instead of reporting the real error.
    ///
    /// Bounding memory is the point of this type, but the bound is the soft
    /// ceiling plus one batch — not "fail the query when the pool is busy".
    async fn make_room_for(&mut self, bytes: usize) -> ExecutorResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        loop {
            if (self.buffered_bytes() + bytes) as u64 > self.soft_limit_bytes {
                if self.spill_largest().await? {
                    continue;
                }
                break;
            }
            let Some(reservation) = &self.reservation else {
                return Ok(());
            };
            match reservation.try_grow(bytes) {
                Ok(()) => return Ok(()),
                Err(_) => {
                    if self.spill_largest().await? {
                        continue;
                    }
                    break;
                }
            }
        }
        if let Some(reservation) = &self.reservation {
            account_unavoidable(reservation, bytes, "buffering one map-output batch");
        }
        Ok(())
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
        //  * stamped with this process's owner id rather than its pid, so the
        //    periodic orphan sweep can tell a live spill from one left by a
        //    dead executor. A pid cannot: executors run in a container PID
        //    namespace and a restart is routinely handed the same pid. Before
        //    this, the only thing that ever reclaimed a spill was
        //    `cleanup_temp_files` at store construction — and an executor that
        //    dies on a full disk cannot boot to run it (2026-07-28: 74 GB
        //    stranded on a 145 GB node, whose kubelet then GC'd the engine
        //    image, so the boot that would have freed the space could not
        //    happen).
        let path = self
            .spill_dir
            .join(krishiv_shuffle::spill_file_name(seq, index));
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
            account_unavoidable(
                reservation,
                in_memory_bytes,
                "handing over one output partition",
            );
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
                account_unavoidable(reservation, bytes, "reading back one spilled run");
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

/// Record bytes this buffer has no way to avoid holding.
///
/// Two places qualify. Draining hands over one whole output partition, the
/// smallest unit [`krishiv_shuffle::ShuffleStore::write_partition`] accepts,
/// with nothing left to spill. Buffering admits one batch that the partitioner
/// has already allocated once every partition is on disk. In both the memory
/// is committed before the pool is consulted, so the only question is whether
/// the pool gets to *know* about it.
///
/// It does. Refusing would fail a query that can actually run — that is the
/// TPC-H q3 regression — and skipping the accounting is precisely the bug this
/// module exists to fix: an unrecorded allocation is one the pool cannot make
/// anyone else back off for. So the reservation grows either way and the
/// overshoot is logged rather than hidden.
fn account_unavoidable(reservation: &MemoryReservation, bytes: usize, what: &str) {
    if bytes == 0 {
        return;
    }
    if let Err(error) = reservation.try_grow(bytes) {
        tracing::warn!(
            bytes,
            what,
            %error,
            "shuffle write buffer exceeded the task memory pool on an allocation it \
             cannot avoid; recording it so the pool stays honest. Raise the stage's \
             partition count to make each partition smaller."
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

    /// The q3 regression: a map task must not fail because some OTHER operator
    /// is already holding the pool.
    ///
    /// The task engine's pool is the process-wide `FairSpillPool`, shared with
    /// every operator of every concurrent task — and a spillable consumer's
    /// share is `(pool - unspillable) / num_spill`, so a hash join build side
    /// on the same fragment can leave the shuffle write buffer unable to grow
    /// on its very FIRST batch. With nothing buffered there is nothing to
    /// spill, and the first version of this buffer treated that as fatal.
    ///
    /// It is not fatal: one batch has to be resident for the partitioner to
    /// have produced it. Failing instead killed the producer task
    /// deterministically, which the coordinator sees only as "missing upstream
    /// shuffle partitions" on the consumer — it regenerates the producer, the
    /// producer fails identically, and the job dies when the regeneration
    /// budget runs out. TPC-H q3 did exactly that, 8 times.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pool_held_by_another_operator_does_not_fail_the_map_write() {
        const POOL_BYTES: usize = 256 * 1024;
        let pool = pool_of(POOL_BYTES);
        // Stand in for the fragment's hash join: an unspillable consumer that
        // has already taken the whole pool.
        let squatter = MemoryConsumer::new("PretendHashJoinInput")
            .with_can_spill(false)
            .register(&pool);
        squatter
            .try_grow(POOL_BYTES)
            .expect("precondition: the squatter must be able to take the pool");

        let mut buffer =
            ShuffleWriteBuffer::new(4, Some(Arc::clone(&pool)), u64::MAX, temp_dir("contended"));
        // The very first push, with an empty buffer and a pool that will
        // refuse: nothing to spill, nothing to fall back on.
        let first = buffer.push(0, batch(0, 1024)).await;
        assert!(
            first.is_ok(),
            "a contended pool must not fail the map write: {:?}",
            first.err()
        );
        for i in 1..16 {
            buffer
                .push(i % 4, batch(i as i64 * 1024, 1024))
                .await
                .unwrap_or_else(|e| panic!("push {i} failed under pool contention: {e}"));
        }

        // And every row must still be there.
        let mut total = 0usize;
        for partition in 0..4 {
            let drained = buffer.drain_partition(partition).await.unwrap();
            total += drained.batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total, 16 * 1024, "rows lost under pool contention");
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

#[cfg(test)]
mod coalesce_tests {
    use super::{SHUFFLE_COALESCE_TARGET_BYTES, coalesce_shuffle_batches};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// A coalesced batch has to fit through the shuffle transport.
    ///
    /// It did not. The writer coalesced to 8 MiB while tonic's *default* 4 MiB
    /// decode limit was left in place on both the shuffle Flight client and
    /// server, so every fetch of a coalesced partition failed with
    /// `decoded message length too large: found 5117681 bytes, the limit is:
    /// 4194304 bytes`. The consumer then reported the partition **missing**,
    /// the coordinator regenerated a 5.6 GB producer stage, reproduced the
    /// identical failure and killed the job — TPC-H q10 at SF100, every sweep.
    ///
    /// Two constants in two crates have to stay in a relationship for the
    /// shuffle to work at all, so assert the relationship rather than trusting
    /// that whoever edits one remembers the other.
    #[test]
    fn shuffle_batches_fit_the_wire_limit() {
        let wire = krishiv_shuffle::flight::shuffle_grpc_max_message_bytes();
        assert!(
            SHUFFLE_COALESCE_TARGET_BYTES * 2 <= wire,
            "the shuffle writer coalesces to {SHUFFLE_COALESCE_TARGET_BYTES} bytes but the \
             transport will only carry {wire}; a coalesced partition cannot be fetched, and \
             the failure is reported as a missing partition rather than a transport error"
        );
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]))
    }

    /// `rows` rows of a fixed-width payload, so byte size is predictable.
    fn batch(schema: &Arc<Schema>, rows: usize, tag: i64) -> RecordBatch {
        let n = Int64Array::from(vec![tag; rows]);
        let s = StringArray::from(vec!["x".repeat(64); rows]);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(n), Arc::new(s)]).unwrap()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[test]
    fn coalesces_many_small_batches_into_fewer() {
        // The DB-3 benefit this must not lose: hundreds of tiny batches
        // should not reach the store as hundreds of tiny batches.
        let schema = schema();
        let input: Vec<_> = (0..200).map(|i| batch(&schema, 4, i)).collect();
        let rows_in = total_rows(&input);
        let out = coalesce_shuffle_batches(input, &schema);
        assert!(out.len() < 20, "expected heavy coalescing, got {}", out.len());
        assert_eq!(total_rows(&out), rows_in, "coalescing must not lose rows");
    }

    #[test]
    fn never_builds_a_batch_far_past_the_target() {
        // The regression this exists for: a partition big enough to have
        // spilled must not be concatenated into one batch, because that is a
        // second full-size copy of the thing that already did not fit.
        let schema = schema();
        // ~64 KiB of payload per batch, 600 batches ≈ 40 MiB total.
        let input: Vec<_> = (0..600).map(|i| batch(&schema, 800, i)).collect();
        let rows_in = total_rows(&input);
        let out = coalesce_shuffle_batches(input, &schema);
        assert_eq!(total_rows(&out), rows_in, "coalescing must not lose rows");
        assert!(out.len() > 1, "a 40 MiB partition must not become one batch");
        for b in &out {
            assert!(
                b.get_array_memory_size() < SHUFFLE_COALESCE_TARGET_BYTES.saturating_mul(2),
                "coalesced batch of {} bytes overshoots the {SHUFFLE_COALESCE_TARGET_BYTES} target",
                b.get_array_memory_size(),
            );
        }
    }

    #[test]
    fn passes_an_already_large_batch_through_uncopied() {
        let schema = schema();
        let big = batch(&schema, 200_000, 7);
        assert!(big.get_array_memory_size() >= SHUFFLE_COALESCE_TARGET_BYTES);
        let rows_in = big.num_rows() + 4;
        let out = coalesce_shuffle_batches(vec![big, batch(&schema, 4, 8)], &schema);
        assert_eq!(total_rows(&out), rows_in);
        // The large batch is emitted as-is rather than concatenated with its
        // small neighbour, which would copy 200k rows to append 4.
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn short_inputs_are_returned_unchanged() {
        let schema = schema();
        assert!(coalesce_shuffle_batches(vec![], &schema).is_empty());
        let one = coalesce_shuffle_batches(vec![batch(&schema, 3, 1)], &schema);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].num_rows(), 3);
    }
}

#[cfg(test)]
mod uniform_schema_tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    /// The two schemas q17 ended up mixing: what the plan *declares* for
    /// `avg(l_quantity)` and what execution actually produces.
    fn declared() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(15, 2), true),
        ]))
    }
    fn produced() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Decimal128(30, 15), true),
        ]))
    }

    fn batch(rows: usize) -> RecordBatch {
        let k = Int64Array::from(vec![7i64; rows]);
        let v = Decimal128Array::from(vec![1i128; rows])
            .with_precision_and_scale(30, 15)
            .unwrap();
        RecordBatch::try_new(produced(), vec![Arc::new(k), Arc::new(v)]).unwrap()
    }

    #[tokio::test]
    async fn every_partition_gets_one_schema_even_when_some_are_empty() {
        // The q17 failure: partition 1 receives rows and is labelled
        // Decimal128(30,15); partitions 0 and 2 receive none and used to be
        // labelled Decimal128(15,2) from the declared fallback. The reduce
        // side then saw one stage publishing two different schemas and
        // refused the mixture.
        let mut buffer = ShuffleWriteBuffer::new(3, None, u64::MAX, std::env::temp_dir());
        buffer.push(1, batch(4)).await.unwrap();

        let observed = buffer
            .pushed_schema()
            .expect("a row-carrying push must latch a schema");
        assert_eq!(observed, produced());
        assert_ne!(
            observed,
            declared(),
            "the test is meaningless unless the two schemas differ"
        );

        // Draining every partition must report the same schema for all three.
        let mut schemas = Vec::new();
        for p in 0..3 {
            let (batches, _res) = buffer.drain_partition(p).await.unwrap().into_parts();
            let schema = buffer
                .pushed_schema()
                .unwrap_or_else(|| Arc::clone(&declared()));
            if p == 1 {
                assert!(!batches.is_empty(), "partition 1 had rows");
            } else {
                assert!(batches.is_empty(), "partition {p} had no rows");
            }
            schemas.push(schema);
        }
        assert!(
            schemas.windows(2).all(|w| w[0] == w[1]),
            "a stage must publish one schema, got {schemas:?}"
        );
        assert_eq!(schemas[0], produced());
    }

    #[tokio::test]
    async fn a_task_that_produced_no_rows_has_no_observed_schema() {
        // Nothing was pushed, so there is nothing to latch and the caller must
        // fall back to the declared schema — which is the case the fallback
        // exists for, and the only case it should be used in.
        let buffer = ShuffleWriteBuffer::new(2, None, u64::MAX, std::env::temp_dir());
        assert!(buffer.pushed_schema().is_none());
    }

    #[tokio::test]
    async fn an_empty_batch_does_not_latch_a_schema() {
        // `push` ignores zero-row batches; they must not set the schema
        // either, or an empty first batch would pin the wrong one.
        let mut buffer = ShuffleWriteBuffer::new(2, None, u64::MAX, std::env::temp_dir());
        buffer.push(0, batch(0)).await.unwrap();
        assert!(buffer.pushed_schema().is_none());
        buffer.push(0, batch(3)).await.unwrap();
        assert_eq!(buffer.pushed_schema(), Some(produced()));
    }
}
