//! A hash join that spills.
//!
//! # The gap this closes
//!
//! DataFusion 54's `HashJoinExec` holds its entire build side in memory and has
//! no spill path. `joins/hash_join/exec.rs` carries a comment that reads as if
//! it did:
//!
//! ```text
//! // Decide if we spill or not
//! let batch_size = get_record_batch_memory_size(&batch);
//! state.reservation.try_grow(batch_size)?;
//! ```
//!
//! There is no branch under that comment — the `?` propagates, and it is the
//! SF100 failure verbatim:
//!
//! ```text
//! Resources exhausted: Failed to allocate additional 877.0 B for
//! HashJoinInput - 0.0 B remain available for the total memory pool
//! ```
//!
//! [`crate::spillable_join`] works around this by rewriting oversized hash
//! joins into sort-merge joins, which do spill. That trade is expensive:
//! sort-merge sorts *both* inputs in full even when almost all of the data
//! would have fitted in memory, and it cost q2 a 6.3x slowdown on the cluster.
//!
//! # What this does instead
//!
//! A classic **hybrid grace hash join**:
//!
//! 1. Buffer the build side while it fits a budget. If the whole build side
//!    fits, join in memory and never touch the disk — identical work to
//!    `HashJoinExec`, which is the right algorithm in that case.
//! 2. On overflow, hash-partition *both* inputs into `buckets` spill files on
//!    the join keys.
//! 3. Join bucket by bucket: each bucket's build side is small enough to hold,
//!    so each bucket is an ordinary in-memory hash join.
//!
//! Only the buckets that overflow pay for disk, and nothing is ever sorted.
//!
//! # Why the answer is the same
//!
//! Rows join only to rows with equal join keys, and equal keys hash equal, so
//! co-partitioning both sides on the same expressions with the same seed puts
//! every row and all of its potential matches in the *same* bucket. The union
//! over buckets is therefore the whole join — including the unmatched rows an
//! outer join must emit, because a row's bucket contains every row it could
//! have matched, so "unmatched within the bucket" and "unmatched overall" are
//! the same statement.
//!
//! This is exactly the property the broadcast-join split bug violated: *there*
//! the build side was replicated and the probe side split, so a task could call
//! a row unmatched that another task had matched. Here both sides are
//! partitioned by the same key, which is the safe case.
//!
//! # Delegation, not reimplementation
//!
//! Each bucket is joined by a real `HashJoinExec`, built from the original join
//! with its own builder, so join type, join filter, null equality,
//! null-awareness and the built-in projection are DataFusion's semantics
//! unchanged. This operator only decides *what data goes to which join*; it
//! never reimplements what a join means.
//!
//! Likewise the node delegates `schema()`, `properties()` and the distribution
//! requirements to the join it replaces, so substituting it cannot change
//! anything a parent plan observes.
//!
//! # Known limits
//!
//! - **Skew.** One key larger than the budget lands in one bucket and that
//!   bucket is still an in-memory join. Recursive re-partitioning would need a
//!   second hash seed, which `BatchPartitioner` does not expose. The bucket
//!   count is therefore chosen generously, and an over-budget bucket is warned
//!   about by name in `join_bucket` — with its size and the budget it broke —
//!   rather than being retried or passing silently.
//! - **Disk.** `buckets` temporary files per side stay open while partitioning.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics, Time};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::spill::{SpillManager, get_record_batch_memory_size};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use std::fmt;
use std::sync::Arc;

/// Turn the grace hash join on. Absent or not truthy, the engine keeps the
/// sort-merge conversion.
///
/// Default-off deliberately: the sort-merge path is what the SF100 sweeps have
/// been measured against, and a new join operator earns its place by beating it
/// on the cluster, not by being newer.
pub const GRACE_HASH_JOIN_ENV: &str = "KRISHIV_GRACE_HASH_JOIN";

/// Override the number of hash buckets the build side is partitioned into.
pub const GRACE_HASH_JOIN_BUCKETS_ENV: &str = "KRISHIV_GRACE_HASH_JOIN_BUCKETS";

/// Buckets used when the estimate suggests nothing larger.
///
/// Generous on purpose. The cost of a bucket is one temporary file and one
/// small in-memory join; the cost of too few buckets is a bucket that does not
/// fit, which is the failure this operator exists to prevent.
const DEFAULT_BUCKETS: usize = 32;

/// Bounds on the bucket count, whatever the estimate or the environment says.
const MIN_BUCKETS: usize = 2;
const MAX_BUCKETS: usize = 256;

/// Whether the grace hash join is enabled for this process.
#[must_use]
pub fn enabled() -> bool {
    std::env::var(GRACE_HASH_JOIN_ENV).is_ok_and(|v| {
        let v = v.trim().to_ascii_lowercase();
        v == "1" || v == "true" || v == "yes" || v == "on"
    })
}

/// Buckets to partition into for a build side estimated at `build_bytes`
/// against a per-task `budget`.
///
/// Sized so a bucket is expected to land at about half the budget, which leaves
/// room for the hash table's own overhead on top of the raw rows.
#[must_use]
pub fn bucket_count(build_bytes: u64, budget: u64) -> usize {
    if let Some(override_buckets) = std::env::var(GRACE_HASH_JOIN_BUCKETS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
    {
        return override_buckets.clamp(MIN_BUCKETS, MAX_BUCKETS);
    }
    let target = (budget / 2).max(1);
    let needed = usize::try_from(build_bytes.div_ceil(target)).unwrap_or(MAX_BUCKETS);
    needed.max(DEFAULT_BUCKETS).clamp(MIN_BUCKETS, MAX_BUCKETS)
}

/// A hash join that partitions to disk when its build side does not fit.
///
/// Stands in for the [`HashJoinExec`] it wraps: same children, same join, same
/// output schema and plan properties.
#[derive(Debug)]
pub struct GraceHashJoinExec {
    /// The join being replaced.
    ///
    /// Never executed. It owns the children and the join specification, and
    /// answers every plan-level question on this node's behalf, so that
    /// substituting this operator cannot change what a parent plan sees.
    template: Arc<HashJoinExec>,
    /// Hash buckets to partition both sides into on overflow.
    buckets: usize,
    /// Build-side bytes to buffer before giving up on the in-memory path.
    build_budget: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl GraceHashJoinExec {
    /// Wrap `template`, partitioning into `buckets` when the build side exceeds
    /// `build_budget` bytes.
    ///
    /// # Errors
    ///
    /// When the two sides do not have the same number of partitions.
    ///
    /// This operator joins side by side: output partition `p` reads partition
    /// `p` of *both* children. That is what `PartitionMode::Partitioned` means,
    /// and it is also true of a one-partition `CollectLeft`. It is **not** true
    /// of a wide `CollectLeft`, where a one-partition build side is broadcast to
    /// every probe partition — asking that left child for partition 1 would
    /// either fail outright or, worse, silently read the wrong data.
    ///
    /// Refusing here rather than at execute time means an unsupported shape is
    /// a planning-time decline that leaves the original join in place, not a
    /// query that dies after doing work.
    pub fn try_new(
        template: Arc<HashJoinExec>,
        buckets: usize,
        build_budget: usize,
    ) -> Result<Self> {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let left = template.left().output_partitioning().partition_count();
        let right = template.right().output_partitioning().partition_count();
        if left != right {
            return Err(DataFusionError::Plan(format!(
                "grace hash join needs both sides partitioned alike, got {left} and {right} \
                 (mode {:?})",
                template.partition_mode()
            )));
        }
        Ok(Self {
            template,
            buckets: buckets.clamp(MIN_BUCKETS, MAX_BUCKETS),
            // A zero budget would send a build side of any size to disk,
            // including an empty one, turning every join into a disk round trip.
            build_budget: build_budget.max(1),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The join this node stands in for.
    #[must_use]
    pub fn template(&self) -> &Arc<HashJoinExec> {
        &self.template
    }

    /// Hash buckets used on overflow.
    #[must_use]
    pub fn buckets(&self) -> usize {
        self.buckets
    }

    /// Build-side bytes buffered before partitioning to disk.
    #[must_use]
    pub fn build_budget(&self) -> usize {
        self.build_budget
    }
}

impl DisplayAs for GraceHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => write!(
                f,
                "GraceHashJoinExec: join_type={:?}, buckets={}",
                self.template.join_type(),
                self.buckets
            ),
            DisplayFormatType::Verbose => write!(
                f,
                "GraceHashJoinExec: join_type={:?}, buckets={}, build_budget={}, on={:?}",
                self.template.join_type(),
                self.buckets,
                self.build_budget,
                self.template.on()
            ),
        }
    }
}

impl ExecutionPlan for GraceHashJoinExec {
    fn name(&self) -> &str {
        "GraceHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // Delegated, not recomputed: this node must be indistinguishable from
        // the join it replaces at plan level, or substituting it could change
        // how a parent distributes or orders its input.
        self.template.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.template.left(), self.template.right()]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.template.required_input_distribution()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Partitioning to disk reorders rows within a side, and bucket order is
        // not input order. Claiming otherwise would let a parent skip a sort it
        // actually needs.
        vec![false, false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let template = self
            .template
            .builder()
            .reset_state()
            .with_new_children(children)?
            .build()?;
        Ok(Arc::new(Self::try_new(
            Arc::new(template),
            self.buckets,
            self.build_budget,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.template.schema();
        let template = Arc::clone(&self.template);
        let metrics = self.metrics.clone();
        let buckets = self.buckets;
        let build_budget = self.build_budget;

        // `join` is async because it has to read the build side before it can
        // choose a path. Flattening a one-shot stream over it keeps `execute`
        // synchronous, as the trait requires.
        let started = futures::stream::once(async move {
            join(template, buckets, build_budget, partition, context, metrics).await
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, started)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.template.partition_statistics(partition)
    }
}

/// Run the join for one output partition.
async fn join(
    template: Arc<HashJoinExec>,
    buckets: usize,
    build_budget: usize,
    partition: usize,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let output_schema = template.schema();
    let build_schema = template.left().schema();
    let probe_schema = template.right().schema();

    let mut build_stream = template.left().execute(partition, Arc::clone(&context))?;

    // Pass 1: buffer the build side while it fits.
    //
    // Reserved against the pool as a *spillable* consumer — which is the literal
    // truth, and it matters: `FairSpillPool` caps spillable consumers at a share
    // of the pool while letting unspillable ones take what remains, so declaring
    // this correctly is what stops it from crowding out a neighbouring hash join
    // that genuinely cannot spill.
    let reservation = MemoryConsumer::new(format!("GraceHashJoinBuild[{partition}]"))
        .with_can_spill(true)
        .register(context.memory_pool());
    let mut buffered: Vec<RecordBatch> = Vec::new();
    let mut buffered_bytes: usize = 0;
    let mut overflowed = false;

    while let Some(batch) = build_stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let size = get_record_batch_memory_size(&batch);
        // Either bound ends the in-memory path: our own budget, or the pool
        // refusing. The pool refusing is not an error here — it is precisely the
        // signal this operator exists to act on instead of propagate.
        if buffered_bytes.saturating_add(size) > build_budget || reservation.try_grow(size).is_err()
        {
            overflowed = true;
            buffered.push(batch);
            break;
        }
        buffered_bytes += size;
        buffered.push(batch);
    }

    if !overflowed {
        tracing::debug!(
            partition,
            buffered_bytes,
            batches = buffered.len(),
            "grace-hash-join: build side fits, joining in memory"
        );
        // Hand the batches to the join and stop accounting for them here: the
        // join registers its own reservation over the same rows, and holding
        // both would double-count the build side against the pool.
        drop(reservation);
        let probe = template.right().execute(partition, Arc::clone(&context))?;
        let build_exec = memory_source(vec![buffered], build_schema)?;
        let probe_exec = Arc::new(OnceStreamExec::new(probe_schema, probe));
        return bucket_join(&template, build_exec, probe_exec)?.execute(0, context);
    }

    tracing::info!(
        partition,
        buffered_bytes,
        build_budget,
        buckets,
        "grace-hash-join: build side exceeds the budget, partitioning to disk"
    );

    // Pass 2: co-partition both sides on the join keys.
    let build_keys: Vec<Arc<dyn PhysicalExpr>> =
        template.on().iter().map(|(l, _)| Arc::clone(l)).collect();
    let probe_keys: Vec<Arc<dyn PhysicalExpr>> =
        template.on().iter().map(|(_, r)| Arc::clone(r)).collect();

    let build_spills = SpillManager::new(
        context.runtime_env(),
        SpillMetrics::new(&metrics, partition),
        Arc::clone(&build_schema),
    );
    let probe_spills = SpillManager::new(
        context.runtime_env(),
        SpillMetrics::new(&metrics, partition),
        Arc::clone(&probe_schema),
    );

    let build_files = spill_by_bucket(
        std::mem::take(&mut buffered),
        build_stream,
        build_keys,
        buckets,
        &build_spills,
        "grace hash join build side",
    )
    .await?;
    // Everything buffered has been written through to disk. Held until here
    // rather than released earlier, so the reservation never understates what
    // is actually resident.
    drop(reservation);

    let probe_stream = template.right().execute(partition, Arc::clone(&context))?;
    let probe_files = spill_by_bucket(
        Vec::new(),
        probe_stream,
        probe_keys,
        buckets,
        &probe_spills,
        "grace hash join probe side",
    )
    .await?;

    // Pass 3: one in-memory join per bucket, streamed in turn so that only one
    // bucket's build side is resident at a time.
    let pairs: Vec<(usize, Option<RefCountedTempFile>, Option<RefCountedTempFile>)> = build_files
        .into_iter()
        .zip(probe_files)
        .enumerate()
        .map(|(bucket, (build, probe))| (bucket, build, probe))
        .collect();

    let joined = futures::stream::iter(pairs)
        .map(Ok::<_, DataFusionError>)
        .and_then(move |(bucket, build_file, probe_file)| {
            let template = Arc::clone(&template);
            let context = Arc::clone(&context);
            let build_spills = build_spills.clone();
            let probe_spills = probe_spills.clone();
            let build_schema = Arc::clone(&build_schema);
            let probe_schema = Arc::clone(&probe_schema);
            async move {
                join_bucket(
                    &template,
                    bucket,
                    build_file,
                    probe_file,
                    &build_spills,
                    &probe_spills,
                    build_schema,
                    probe_schema,
                    build_budget,
                    &context,
                )
                .await
            }
        })
        .try_flatten();

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        joined,
    )))
}

/// Join one bucket: its build side read into memory, its probe side streamed.
#[expect(
    clippy::too_many_arguments,
    reason = "one bucket needs both sides' files, spill managers and schemas; \
              bundling them into a struct would only move the list"
)]
async fn join_bucket(
    template: &Arc<HashJoinExec>,
    bucket: usize,
    build_file: Option<RefCountedTempFile>,
    probe_file: Option<RefCountedTempFile>,
    build_spills: &SpillManager,
    probe_spills: &SpillManager,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    build_budget: usize,
    context: &Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    // Both sides empty means no row of either input hashed here: nothing to
    // join, and nothing unmatched to report either.
    if build_file.is_none() && probe_file.is_none() {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            template.schema(),
            futures::stream::empty(),
        )));
    }

    let build: Vec<RecordBatch> = match build_file {
        Some(file) => {
            build_spills
                .read_spill_as_stream(file, None)?
                .try_collect()
                .await?
        }
        // An empty build side is not a shortcut: a right or full outer join
        // still has to emit this bucket's probe rows as unmatched.
        None => Vec::new(),
    };
    let bucket_bytes: usize = build.iter().map(get_record_batch_memory_size).sum();

    // Account for the bucket we just read back.
    //
    // This was unreserved, in the operator whose whole purpose is to stop
    // unaccounted build sides from exhausting the pool. The rows are resident
    // twice over: once in this `Vec` (held by `MemorySourceConfig` for the
    // life of the join) and again in the hash table the inner `HashJoinExec`
    // builds from it, which *is* reserved. So peak residency was about double
    // the bucket with only half of it visible — and on a skewed bucket the
    // invisible half is exactly what pushes the executor over.
    //
    // `can_spill(false)`: these rows have already been through the disk and
    // there is nowhere further to put them. Saying so lets a `FairSpillPool`
    // account for them honestly rather than counting on a spill that cannot
    // happen.
    let reservation = MemoryConsumer::new(format!("GraceHashJoinBucket[{bucket}]"))
        .with_can_spill(false)
        .register(context.memory_pool());
    reservation.try_grow(bucket_bytes)?;

    if bucket_bytes > build_budget {
        // The skew limit named in this module's docs. It used to claim such a
        // bucket "is logged" — it was not, because `build_budget` never
        // reached here and every bucket logged the same line at debug. One
        // key larger than the budget still lands in one bucket; the join will
        // attempt it, and this is the warning that says why the pool is about
        // to be under pressure.
        tracing::warn!(
            bucket,
            bucket_bytes,
            build_budget,
            "grace-hash-join: bucket build side exceeds the per-task budget \
             (key skew); joining it anyway, which may exhaust the pool"
        );
    } else {
        tracing::debug!(bucket, bucket_bytes, "grace-hash-join: joining bucket");
    }

    let probe: SendableRecordBatchStream = match probe_file {
        Some(file) => probe_spills.read_spill_as_stream(file, None)?,
        None => Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&probe_schema),
            futures::stream::empty(),
        )),
    };

    let build_exec = memory_source(vec![build], build_schema)?;
    let probe_exec = Arc::new(OnceStreamExec::new(probe_schema, probe));
    let schema = template.schema();
    let joined = bucket_join(template, build_exec, probe_exec)?.execute(0, Arc::clone(context))?;

    // Carry the reservation with the stream so it is released when this
    // bucket's output is finished or dropped — not when this function returns,
    // which is before a single row has been read.
    let guarded = futures::stream::unfold(
        (joined, reservation),
        |(mut stream, reservation)| async move {
            stream
                .next()
                .await
                .map(|batch| (batch, (stream, reservation)))
        },
    );
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, guarded)))
}

/// A single-partition scan over in-memory batches.
fn memory_source(
    partitions: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let exec =
        datafusion::datasource::memory::MemorySourceConfig::try_new_exec(&partitions, schema, None)?;
    Ok(exec)
}

/// Build the per-bucket join from the original.
///
/// Everything that decides *what the join means* — type, filter, null equality,
/// null-awareness, the built-in projection — is carried over by the builder.
/// Only the children and the partition mode change, and the mode has to: each
/// bucket is a single pair of one-partition inputs, which is what `CollectLeft`
/// describes.
fn bucket_join(
    template: &Arc<HashJoinExec>,
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    template
        .builder()
        // Without this the buckets would share the original's collected build
        // side and dynamic filter — one bucket's data answering another's join.
        .reset_state()
        .with_new_children(vec![build, probe])?
        .with_partition_mode(PartitionMode::CollectLeft)
        .recompute_properties()
        .build_exec()
}

/// Hash-partition `prefix` followed by the rest of `stream` into one spill file
/// per bucket.
///
/// Returns one entry per bucket, `None` where no row hashed to it.
async fn spill_by_bucket(
    prefix: Vec<RecordBatch>,
    stream: SendableRecordBatchStream,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    buckets: usize,
    spills: &SpillManager,
    request: &str,
) -> Result<Vec<Option<RefCountedTempFile>>> {
    let mut partitioner = BatchPartitioner::new_hash_partitioner(keys, buckets, Time::new())?;
    // The type of an in-progress spill file is not nameable outside DataFusion,
    // so it is only ever inferred here.
    let mut files = Vec::with_capacity(buckets);
    for bucket in 0..buckets {
        files.push(spills.create_in_progress_file(&format!("{request} bucket {bucket}"))?);
    }

    // The already-buffered batches and the rest of the stream are the same
    // input; chaining them means one routing loop rather than two that could
    // drift apart.
    let mut all = futures::stream::iter(prefix.into_iter().map(Ok)).chain(stream);
    while let Some(batch) = all.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        partitioner.partition(batch, |bucket, part| {
            if part.num_rows() == 0 {
                return Ok(());
            }
            // The partitioner was built with `buckets` partitions and `files`
            // has one entry per bucket, so this cannot miss — but a silent
            // panic here would surface as a lost task with no explanation.
            let file = files.get_mut(bucket).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "grace hash join routed a batch to bucket {bucket} of {buckets}"
                ))
            })?;
            file.append_batch(&part)?;
            Ok(())
        })?;
    }

    let mut finished = Vec::with_capacity(buckets);
    for mut file in files {
        finished.push(file.finish()?);
    }
    Ok(finished)
}

/// An [`ExecutionPlan`] over one already-created stream.
///
/// The bucket joins need their probe side as a plan node, but the data is a
/// spill-file stream that exists before the plan does. This adapts one to the
/// other. Single partition, single use: `execute` hands the stream out and it is
/// gone, which is exactly how a bucket consumes it.
struct OnceStreamExec {
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for OnceStreamExec {
    // A record-batch stream is not `Debug`, and `ExecutionPlan` requires it of
    // the node. Nothing about a consumed-once stream is worth printing anyway.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OnceStreamExec")
    }
}

impl OnceStreamExec {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            stream: std::sync::Mutex::new(Some(stream)),
            properties,
        }
    }
}

impl DisplayAs for OnceStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OnceStreamExec")
    }
}

impl ExecutionPlan for OnceStreamExec {
    fn name(&self) -> &str {
        "OnceStreamExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OnceStreamExec has one partition, asked for {partition}"
            )));
        }
        self.stream
            .lock()
            .map_err(|_| DataFusionError::Internal("OnceStreamExec mutex poisoned".into()))?
            .take()
            .ok_or_else(|| DataFusionError::Internal("OnceStreamExec was already executed".into()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn probe_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("w", DataType::Int32, true),
        ]))
    }

    fn build_batch(keys: Vec<Option<i32>>, vals: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            build_schema(),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .expect("build batch")
    }

    fn probe_batch(keys: Vec<Option<i32>>, ws: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(
            probe_schema(),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int32Array::from(ws)),
            ],
        )
        .expect("probe batch")
    }

    /// Build side across two batches: duplicate keys, and keys matching nothing.
    fn build_rows() -> Vec<RecordBatch> {
        vec![
            build_batch(vec![Some(1), Some(1), Some(2)], vec![Some("a1"), Some("a2"), Some("b")]),
            build_batch(vec![Some(3), Some(7)], vec![Some("c"), Some("g")]),
        ]
    }

    /// Probe side: a duplicate key, and a key matching nothing.
    fn probe_rows() -> Vec<RecordBatch> {
        vec![
            probe_batch(vec![Some(1), Some(2)], vec![Some(10), Some(20)]),
            probe_batch(vec![Some(2), Some(4)], vec![Some(21), Some(40)]),
        ]
    }

    fn source(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        memory_source(vec![batches], schema).expect("memory source")
    }

    /// The hash join the grace operator stands in for.
    fn hash_join(
        join_type: JoinType,
        null_equality: NullEquality,
        projection: Option<Vec<usize>>,
    ) -> Arc<HashJoinExec> {
        Arc::new(
            HashJoinExec::try_new(
                source(build_schema(), build_rows()),
                source(probe_schema(), probe_rows()),
                vec![(
                    Arc::new(Column::new("k", 0)),
                    Arc::new(Column::new("k", 0)),
                )],
                None,
                &join_type,
                projection,
                PartitionMode::CollectLeft,
                null_equality,
                false,
            )
            .expect("hash join"),
        )
    }

    /// Every cell of every row, sorted. Bucket order is not input order, so only
    /// a set comparison is meaningful — and only cells catch a plan that returns
    /// the right shape with the wrong values.
    async fn cells(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<String> {
        let batches = collect(plan, ctx.task_ctx()).await.expect("collect");
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |r| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c), r)
                                .expect("cell")
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
            })
            .collect();
        rows.sort();
        rows
    }

    /// Number of spill files the operator actually created.
    ///
    /// Every "grace mode" test asserts this is non-zero. Without it a test that
    /// silently took the in-memory path would pass while proving nothing about
    /// the partitioning path it claims to cover.
    /// `MetricsSet::spill_count()`, not `sum_by_name("spill_count")` — the
    /// latter matches only `Count`/`Time`/`Gauge` variants and returns `false`
    /// for `SpillCount`, so it silently reports zero spills forever. It was the
    /// first thing written here, and every grace-mode test "passed" against it.
    fn spill_files(plan: &GraceHashJoinExec) -> usize {
        plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
    }

    /// Grace mode and the hash join it replaces must agree, for every join type.
    ///
    /// Outer joins are the reason this test enumerates: an unmatched row is only
    /// unmatched if *no* row on the other side matched it, so a partitioning
    /// that split a key across buckets would emit spurious unmatched rows. That
    /// is exactly the bug that made a split broadcast `LeftAnti` join return
    /// wrong answers, and it is silent — right shape, right types, wrong data.
    #[tokio::test]
    async fn every_join_type_agrees_with_the_hash_join_it_replaces() {
        let ctx = SessionContext::new();
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            let expected = cells(hash_join(join_type, NullEquality::NullEqualsNothing, None), &ctx).await;

            // Budget of 1 byte: the first batch overflows, so every run of this
            // test takes the partitioning path.
            let grace = Arc::new(
                GraceHashJoinExec::try_new(
                    hash_join(join_type, NullEquality::NullEqualsNothing, None),
                    4,
                    1,
                )
                .expect("grace join"),
            );
            let actual = cells(Arc::clone(&grace) as Arc<dyn ExecutionPlan>, &ctx).await;
            assert!(
                spill_files(&grace) > 0,
                "{join_type:?} took the in-memory path, so this proved nothing"
            );
            assert_eq!(actual, expected, "{join_type:?} disagreed after partitioning");
        }
    }

    /// An anchor: the inner join's rows written out by hand, so the comparison
    /// above cannot pass by both sides being broken the same way.
    #[tokio::test]
    async fn the_inner_join_returns_the_rows_it_should() {
        let ctx = SessionContext::new();
        let grace = Arc::new(
            GraceHashJoinExec::try_new(
                hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, None),
                4,
                1,
            )
            .expect("grace join"),
        );
        assert_eq!(
            cells(grace, &ctx).await,
            vec!["1|a1|1|10", "1|a2|1|10", "2|b|2|20", "2|b|2|21"],
        );
    }

    /// A build side that fits must never touch the disk. This is the whole
    /// reason to prefer this over the sort-merge conversion: the common case
    /// has to stay exactly as fast as an ordinary hash join.
    #[tokio::test]
    async fn a_build_side_that_fits_stays_in_memory() {
        let ctx = SessionContext::new();
        let grace = Arc::new(
            GraceHashJoinExec::try_new(
                hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, None),
                4,
                64 * 1024 * 1024,
            )
            .expect("grace join"),
        );
        let actual = cells(Arc::clone(&grace) as Arc<dyn ExecutionPlan>, &ctx).await;
        assert_eq!(spill_files(&grace), 0, "a fitting build side spilled");
        assert_eq!(
            actual,
            cells(hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, None), &ctx).await
        );
    }

    /// Null keys hash consistently, so they co-locate like any other key. Both
    /// null-equality settings must survive the round trip — under
    /// `NullEqualsNull` the nulls actually join, which only works if they landed
    /// in the same bucket.
    #[tokio::test]
    async fn null_keys_survive_partitioning_under_both_null_equalities() {
        let ctx = SessionContext::new();
        for null_equality in [NullEquality::NullEqualsNothing, NullEquality::NullEqualsNull] {
            let join = || {
                Arc::new(
                    HashJoinExec::try_new(
                        source(
                            build_schema(),
                            vec![build_batch(
                                vec![None, Some(1), None],
                                vec![Some("n1"), Some("a"), Some("n2")],
                            )],
                        ),
                        source(
                            probe_schema(),
                            vec![probe_batch(vec![None, Some(1)], vec![Some(99), Some(10)])],
                        ),
                        vec![(
                            Arc::new(Column::new("k", 0)),
                            Arc::new(Column::new("k", 0)),
                        )],
                        None,
                        &JoinType::Full,
                        None,
                        PartitionMode::CollectLeft,
                        null_equality,
                        false,
                    )
                    .expect("hash join"),
                )
            };
            let expected = cells(join(), &ctx).await;
            let grace =
                Arc::new(GraceHashJoinExec::try_new(join(), 4, 1).expect("grace join"));
            let actual = cells(Arc::clone(&grace) as Arc<dyn ExecutionPlan>, &ctx).await;
            assert!(spill_files(&grace) > 0, "{null_equality:?} stayed in memory");
            assert_eq!(actual, expected, "{null_equality:?} disagreed");
        }
    }

    /// Bucket reservations must be released, not leaked.
    ///
    /// Each bucket's build side is read back from disk into a `Vec` that the
    /// join then holds — memory that went entirely unaccounted until it was
    /// reserved, in the operator whose whole purpose is to keep build sides
    /// from exhausting the pool. Reserving it is only half the job: the
    /// reservation has to be dropped when the bucket's output is finished,
    /// which is long after `join_bucket` returns.
    ///
    /// Running the same plan repeatedly against one bounded pool is the cheap
    /// way to catch a leak: if a bucket's bytes were never given back, a later
    /// run would be refused.
    #[tokio::test]
    async fn bucket_reservations_are_released_after_each_run() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        // Small enough that unreleased buckets would accumulate into a refusal,
        // large enough that one honest run fits.
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(4 * 1024 * 1024)))
            .build_arc()
            .expect("runtime env");
        let ctx = SessionContext::new_with_config_rt(Default::default(), env);

        let mut previous: Option<Vec<String>> = None;
        for run in 0..4 {
            let grace = Arc::new(
                GraceHashJoinExec::try_new(
                    hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, None),
                    4,
                    1,
                )
                .expect("grace join"),
            );
            assert!(
                spill_files(&grace) == 0,
                "fresh node should not report spills before running"
            );
            let rows = cells(Arc::clone(&grace) as Arc<dyn ExecutionPlan>, &ctx).await;
            assert!(!rows.is_empty(), "run {run} produced nothing");
            if let Some(first) = &previous {
                assert_eq!(&rows, first, "run {run} disagreed with the first run");
            }
            previous = Some(rows);
        }
    }

    /// A join carrying a built-in projection keeps it.
    ///
    /// `SortMergeJoinExec` has no projection, which is why the sort-merge
    /// conversion had to rebuild one by hand and broke q7/q8/q9 when it got the
    /// indices wrong. Delegating to a real `HashJoinExec` per bucket means this
    /// operator inherits the projection instead of reconstructing it — this test
    /// pins that it actually does.
    #[tokio::test]
    async fn a_projected_join_keeps_its_projection() {
        let ctx = SessionContext::new();
        // Columns 1 (v) and 3 (w) of the k,v,k,w join schema.
        let projection = Some(vec![1, 3]);
        let expected = cells(
            hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, projection.clone()),
            &ctx,
        )
        .await;

        let grace = Arc::new(
            GraceHashJoinExec::try_new(
                hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, projection),
                4,
                1,
            )
            .expect("grace join"),
        );
        assert_eq!(
            grace.schema().fields().len(),
            2,
            "the projection was lost from the output schema"
        );
        let actual = cells(Arc::clone(&grace) as Arc<dyn ExecutionPlan>, &ctx).await;
        assert!(spill_files(&grace) > 0, "took the in-memory path");
        assert_eq!(actual, expected);
    }

    /// The node must look exactly like the join it replaces, or a parent plan
    /// could distribute or order its input differently around it.
    #[test]
    fn the_node_reports_the_same_schema_and_partitioning_as_its_template() {
        let template = hash_join(JoinType::Inner, NullEquality::NullEqualsNothing, None);
        let grace =
            GraceHashJoinExec::try_new(Arc::clone(&template), 4, 1).expect("grace join");
        assert_eq!(grace.schema(), template.schema());
        assert_eq!(
            format!("{:?}", grace.properties().partitioning),
            format!("{:?}", template.properties().partitioning),
        );
    }

    /// A broadcast join whose sides have different partition counts is refused
    /// at construction, so the caller keeps the original join rather than
    /// discovering the mismatch mid-query.
    #[test]
    fn a_join_whose_sides_differ_in_partition_count_is_refused() {
        let template = Arc::new(
            HashJoinExec::try_new(
                // One build partition, two probe partitions.
                memory_source(vec![build_rows().clone()], build_schema()).unwrap(),
                memory_source(
                    vec![vec![probe_rows()[0].clone()], vec![probe_rows()[1].clone()]],
                    probe_schema(),
                )
                .unwrap(),
                vec![(
                    Arc::new(Column::new("k", 0)),
                    Arc::new(Column::new("k", 0)),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .expect("hash join"),
        );
        let refused = GraceHashJoinExec::try_new(template, 4, 1);
        assert!(
            refused.is_err(),
            "a broadcast join must be refused, not silently mis-executed"
        );
    }

    /// An empty build side is not a shortcut: a right outer join still has to
    /// emit every probe row as unmatched.
    #[tokio::test]
    async fn an_empty_build_side_still_emits_unmatched_probe_rows() {
        let ctx = SessionContext::new();
        let join = || {
            Arc::new(
                HashJoinExec::try_new(
                    source(build_schema(), vec![]),
                    source(probe_schema(), probe_rows()),
                    vec![(
                        Arc::new(Column::new("k", 0)),
                        Arc::new(Column::new("k", 0)),
                    )],
                    None,
                    &JoinType::Right,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .expect("hash join"),
            )
        };
        let expected = cells(join(), &ctx).await;
        let grace = Arc::new(GraceHashJoinExec::try_new(join(), 4, 1).expect("grace join"));
        assert_eq!(cells(grace, &ctx).await, expected);
        assert_eq!(expected.len(), 4, "every probe row should be reported");
    }

    /// More buckets than distinct keys means most buckets are empty; they must
    /// contribute nothing rather than erroring or emitting phantom rows.
    #[tokio::test]
    async fn far_more_buckets_than_keys_changes_nothing() {
        let ctx = SessionContext::new();
        let expected = cells(
            hash_join(JoinType::Full, NullEquality::NullEqualsNothing, None),
            &ctx,
        )
        .await;
        let grace = Arc::new(
            GraceHashJoinExec::try_new(
                hash_join(JoinType::Full, NullEquality::NullEqualsNothing, None),
                256,
                1,
            )
            .expect("grace join"),
        );
        assert_eq!(cells(grace, &ctx).await, expected);
    }

    #[test]
    fn the_bucket_count_grows_with_the_build_side() {
        // Small build side: the floor applies.
        assert_eq!(bucket_count(1024, 1024 * 1024), DEFAULT_BUCKETS);
        // 10 GB against a 256 MB budget wants far more than the floor, and is
        // capped rather than allowed to open unbounded files.
        let big = bucket_count(10 * 1024 * 1024 * 1024, 256 * 1024 * 1024);
        assert!(big > DEFAULT_BUCKETS, "expected more than the floor, got {big}");
        assert!(big <= MAX_BUCKETS);
        // A zero budget must not divide by zero.
        assert!(bucket_count(1, 0) >= MIN_BUCKETS);
    }
}
