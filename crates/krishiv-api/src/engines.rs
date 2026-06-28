//! The three compute engines behind one [`ComputeEngine`] contract.
//!
//! Each engine is driven from a [`CompiledJob`] plus an [`EngineRuntime`]
//! (placement-provided sources/sinks/state). [`run_job`] is the single
//! dispatch point: it routes by the job's explicit [`EngineKind`], so SQL,
//! Python, and Rust front-ends all reach the same three engines the same way.
//!
//! - [`BatchEngine`] — bounded SQL run to completion over DataFusion.
//! - [`IncrementalEngine`] — change-driven incremental view maintenance
//!   (`krishiv-ivm`), emitting the materialized view to sinks.
//! - [`StreamingEngine`] — event-time dataflow streaming. Compiles the
//!   canonical windowed-aggregation SQL shape (`TUMBLE`/`HOP`/`SESSION`) into a
//!   `WindowExecutionSpec` and drives the dataflow `ContinuousWindowExecutor`;
//!   non-windowed queries return a typed [`EngineError::Unsupported`].

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_delta::{DeltaBatch, differentiate};
use krishiv_engine_core::{
    ChangelogBatch, CheckpointPayload, CompiledJob, ComputeEngine, EngineError, EngineKind,
    EngineResult, EngineRuntime, JobHandle, JobStatus, RowKind, SinkSpec, SinkWriter, SourceReader,
};
use krishiv_ivm::{IncrementalFlow, IncrementalViewSpec};
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

fn df_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Runtime(e.to_string())
}

/// Dispatch a compiled job to its engine by the job's explicit [`EngineKind`].
///
/// This is the one place engine selection happens; no front-end forks per
/// engine, and the deployment placement is carried entirely by `rt`.
pub async fn run_job(job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
    match job.engine {
        EngineKind::Batch => BatchEngine.run(job, rt).await,
        EngineKind::Incremental => IncrementalEngine.run(job, rt).await,
        EngineKind::Streaming => StreamingEngine.run(job, rt).await,
    }
}

/// Read every batch a source produces into memory (bounded sources only).
async fn drain_source(
    rt: &EngineRuntime,
    spec: &krishiv_engine_core::SourceSpec,
) -> EngineResult<Vec<arrow::record_batch::RecordBatch>> {
    let mut reader = rt.sources.open(spec).await?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Read every **changelog** a source produces into memory (bounded sources).
///
/// Append-only sources surface their rows as insertions via the default
/// [`SourceReader::next_changelog`]; CDC connectors surface true deletes and
/// updates. The incremental engine drains through this so a delete in the
/// source becomes a retraction in the maintained view.
async fn drain_changelog_source(
    rt: &EngineRuntime,
    spec: &krishiv_engine_core::SourceSpec,
) -> EngineResult<Vec<ChangelogBatch>> {
    let mut reader = rt.sources.open(spec).await?;
    let mut changes = Vec::new();
    while let Some(changelog) = reader.next_changelog().await? {
        changes.push(changelog);
    }
    Ok(changes)
}

/// Convert a [`ChangelogBatch`] into a weighted [`DeltaBatch`] for the
/// incremental engine: each row's [`RowKind`] maps to a DBSP weight (`+1`
/// insert/update-after, `-1` delete/update-before) appended as the `_weight`
/// column. This is the bridge from CDC change semantics to Z-set deltas.
pub(crate) fn delta_from_changelog(changelog: &ChangelogBatch) -> EngineResult<DeltaBatch> {
    let batch = changelog.batch();
    let weights: Vec<i64> = changelog.row_kinds().iter().map(|k| k.weight()).collect();
    let mut fields: Vec<Arc<arrow::datatypes::Field>> =
        batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(arrow::datatypes::Field::new(
        krishiv_delta::WEIGHT_COLUMN,
        arrow::datatypes::DataType::Int64,
        false,
    )));
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(arrow::array::Int64Array::from(weights)));
    let weighted = arrow::record_batch::RecordBatch::try_new(schema, columns).map_err(df_err)?;
    DeltaBatch::from_weighted(weighted).map_err(|e| EngineError::Runtime(e.to_string()))
}

/// Probe the output schema of `query` by registering empty tables for each
/// source schema and planning `... LIMIT 0`.
pub(crate) async fn infer_output_schema(
    sources: &[(String, SchemaRef)],
    query: &str,
) -> EngineResult<SchemaRef> {
    let ctx = SessionContext::new();
    for (name, schema) in sources {
        // One empty partition (not zero) — DataFusion rejects an empty
        // partition list; an empty table is enough for a LIMIT 0 probe.
        let table = MemTable::try_new(schema.clone(), vec![vec![]]).map_err(df_err)?;
        ctx.register_table(name.as_str(), Arc::new(table))
            .map_err(df_err)?;
    }
    let probe = format!("SELECT * FROM ({query}) AS __engine_probe__ LIMIT 0");
    let df = ctx.sql(&probe).await.map_err(df_err)?;
    Ok(Arc::new(df.schema().as_arrow().clone()))
}

// ── Batch ───────────────────────────────────────────────────────────────────

/// Spark-style batch engine: bounded SQL run to completion over DataFusion.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchEngine;

#[async_trait]
impl ComputeEngine for BatchEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Batch
    }

    fn validate(&self, job: &CompiledJob) -> EngineResult<()> {
        job.validate_shape().map_err(EngineError::InvalidJob)?;
        if job.engine != EngineKind::Batch {
            return Err(EngineError::Unsupported {
                engine: EngineKind::Batch,
                reason: format!("job declares the {} engine", job.engine),
            });
        }
        Ok(())
    }

    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
        self.validate(&job)?;

        // Placement seam: if the runtime carries a query executor (single-node or
        // distributed), hand the whole job to it; otherwise run the query
        // in-process over DataFusion. The engine code is identical either way.
        let result = if let Some(executor) = rt.query_executor.clone() {
            executor.execute_batch(&job).await?
        } else {
            let ctx = SessionContext::new();
            for spec in &job.sources {
                let batches = drain_source(&rt, spec).await?;
                let schema = batches.first().map(|b| b.schema()).ok_or_else(|| {
                    EngineError::Source(format!(
                        "source '{}' produced no batches; the batch engine needs a schema",
                        spec.name
                    ))
                })?;
                let table = MemTable::try_new(schema, vec![batches]).map_err(df_err)?;
                ctx.register_table(spec.name.as_str(), Arc::new(table))
                    .map_err(df_err)?;
            }
            let df = ctx.sql(&job.query).await.map_err(df_err)?;
            df.collect().await.map_err(df_err)?
        };

        for spec in &job.sinks {
            let mut writer = rt.sinks.open(spec).await?;
            for batch in &result {
                if batch.num_rows() == 0 {
                    continue;
                }
                writer.write(ChangelogBatch::inserts(batch.clone())).await?;
            }
            writer.flush().await?;
        }
        JobHandle::from_name(&job.name, JobStatus::Completed)
    }
}

// ── Incremental ───────────────────────────────────────────────────────────────

/// Convert a weighted [`DeltaBatch`] (the output of [`differentiate`]) into a
/// [`ChangelogBatch`]: positive weights are insertions, negative weights are
/// deletions, and a row's multiplicity `|weight|` is expanded to that many
/// changelog rows. Returns `None` for an empty delta.
fn changelog_from_delta(delta: &DeltaBatch) -> EngineResult<Option<ChangelogBatch>> {
    if delta.is_empty() {
        return Ok(None);
    }
    let data = delta.data_batch();
    let weights = delta.weights();
    let mut indices: Vec<u32> = Vec::with_capacity(delta.num_rows());
    let mut kinds: Vec<RowKind> = Vec::with_capacity(delta.num_rows());
    for (row, weight) in weights.iter().enumerate() {
        let weight = weight.unwrap_or(0);
        if weight == 0 {
            continue;
        }
        let kind = if weight > 0 {
            RowKind::Insert
        } else {
            RowKind::Delete
        };
        let index = u32::try_from(row)
            .map_err(|_| EngineError::Runtime("changelog row index overflow".to_string()))?;
        for _ in 0..weight.unsigned_abs() {
            indices.push(index);
            kinds.push(kind);
        }
    }
    if indices.is_empty() {
        return Ok(None);
    }
    let index_array = arrow::array::UInt32Array::from(indices);
    let columns = data
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &index_array, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(df_err)?;
    let expanded =
        arrow::record_batch::RecordBatch::try_new(data.schema(), columns).map_err(df_err)?;
    Ok(Some(ChangelogBatch::new(expanded, kinds)?))
}

/// Feldera/DBSP-style incremental engine: maintains the query as a materialized
/// view and emits a **changelog** (insertions and retractions) to the sinks as
/// each input batch lands. The single output relation is registered under the
/// job name.
#[derive(Debug, Clone, Copy, Default)]
pub struct IncrementalEngine;

#[async_trait]
impl ComputeEngine for IncrementalEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Incremental
    }

    fn validate(&self, job: &CompiledJob) -> EngineResult<()> {
        job.validate_shape().map_err(EngineError::InvalidJob)?;
        if job.engine != EngineKind::Incremental {
            return Err(EngineError::Unsupported {
                engine: EngineKind::Incremental,
                reason: format!("job declares the {} engine", job.engine),
            });
        }
        Ok(())
    }

    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
        self.validate(&job)?;

        // Drain every source as a **changelog** stream (CDC-aware) and record its
        // schema for output inference. Append-only sources surface as insertions;
        // CDC sources surface deletes/updates that the view retracts.
        let mut buffered: Vec<(String, Vec<ChangelogBatch>)> = Vec::new();
        let mut source_schemas: Vec<(String, SchemaRef)> = Vec::new();
        for spec in &job.sources {
            let changes = drain_changelog_source(&rt, spec).await?;
            if let Some(first) = changes.first() {
                source_schemas.push((spec.name.clone(), first.batch().schema()));
            }
            buffered.push((spec.name.clone(), changes));
        }

        let output_schema = infer_output_schema(&source_schemas, &job.query).await?;

        // Maintain the query as a materialized view named after the job.
        let flow = IncrementalFlow::new();
        flow.register_view(IncrementalViewSpec {
            name: job.name.clone(),
            body_sql: job.query.clone(),
            output_schema: output_schema.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        })
        .map_err(|e| EngineError::Runtime(e.to_string()))?;

        // Open every sink once; the changelog stream is written incrementally.
        let mut writers: Vec<Box<dyn SinkWriter>> = Vec::with_capacity(job.sinks.len());
        for spec in &job.sinks {
            writers.push(rt.sinks.open(spec).await?);
        }

        // Feed each input batch, recompute the view, and emit the *change* in the
        // materialized view (`differentiate` vs. the previously emitted snapshot)
        // as a changelog — so an aggregate that changes emits a retraction of the
        // old row plus an insertion of the new one, not a fresh full dump.
        let mut prev: Option<arrow::record_batch::RecordBatch> = None;
        for (name, changes) in buffered {
            for changelog in changes {
                if changelog.num_rows() == 0 {
                    continue;
                }
                let delta = delta_from_changelog(&changelog)?;
                flow.feed(&name, delta)
                    .map_err(|e| EngineError::Runtime(e.to_string()))?;
                // SQL-backed views are computed by `step_datafusion`; the sync
                // `step` only drives the manual/closure compute path.
                flow.step_datafusion()
                    .await
                    .map_err(|e| EngineError::Runtime(e.to_string()))?;

                let new = match flow
                    .snapshot(&job.name)
                    .map_err(|e| EngineError::Runtime(e.to_string()))?
                {
                    Some(batch) => batch,
                    // Empty view: match the schema already established by the
                    // previous snapshot (or the inferred output schema).
                    None => {
                        let schema = prev
                            .as_ref()
                            .map(|p| p.schema())
                            .unwrap_or_else(|| output_schema.clone());
                        arrow::record_batch::RecordBatch::new_empty(schema)
                    }
                };
                // Differentiate against the snapshot's *own* schema: the engine's
                // actual output types can differ from the LIMIT-0 probe (e.g. the
                // incremental aggregate promotes SUM to Float64).
                let view_schema = new.schema();
                let view_delta = differentiate(&view_schema, prev.as_ref(), &new)
                    .map_err(|e| EngineError::Runtime(e.to_string()))?;
                if let Some(changelog) = changelog_from_delta(&view_delta)? {
                    for writer in &mut writers {
                        writer.write(changelog.clone()).await?;
                    }
                }
                prev = Some(new);
            }
        }
        for writer in &mut writers {
            writer.flush().await?;
        }
        JobHandle::from_name(&job.name, JobStatus::Running)
    }
}

// ── Streaming ─────────────────────────────────────────────────────────────────

/// Flink-style event-time streaming engine (dataflow windows + watermarks).
///
/// Compiles the canonical windowed-aggregation SQL shape
/// (`SELECT key, AGG(col) FROM TUMBLE/HOP/SESSION(...) GROUP BY ...`) into a
/// `WindowExecutionSpec` via [`compile_streaming_window_sql`] and drives the
/// dataflow `ContinuousWindowExecutor`. Queries that are not a recognised
/// keyed window return a typed [`EngineError::Unsupported`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamingEngine;

fn exec_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Runtime(e.to_string())
}

#[async_trait]
impl ComputeEngine for StreamingEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Streaming
    }

    fn validate(&self, job: &CompiledJob) -> EngineResult<()> {
        job.validate_shape().map_err(EngineError::InvalidJob)?;
        compile_streaming_window_sql(&job.query).map_err(|e| EngineError::Unsupported {
            engine: EngineKind::Streaming,
            reason: e.to_string(),
        })?;
        Ok(())
    }

    /// Bounded run: drains the source to end-of-input once, emits the closed
    /// windows, and persists one checkpoint. This is the right shape for bounded
    /// sources and for tests; unbounded continuous execution uses
    /// [`spawn_streaming_job`].
    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
        let mut setup = streaming_setup(&job, &rt).await?;

        let mut batches = Vec::new();
        while let Some(batch) = setup.reader.next().await? {
            batches.push(batch);
        }
        let source_offset = setup.reader.checkpoint_offset();
        let outputs = setup.executor.drain(batches).map_err(exec_err)?;

        let mut writers = open_writers(&rt, &job.sinks).await?;
        emit_to_writers(&mut writers, &outputs).await?;
        for writer in &mut writers {
            writer.flush().await?;
        }

        persist_streaming_checkpoint(
            &rt,
            &setup.handle,
            &mut setup.executor,
            &setup.source_name,
            source_offset,
            setup.next_epoch,
        )
        .await?;

        Ok(setup.handle)
    }
}

/// How long the continuous loop waits between polls when the source is idle.
const STREAMING_IDLE_TICK_MS: u64 = 5;
/// How many input batches the continuous loop processes between checkpoints.
const STREAMING_CHECKPOINT_EVERY: u32 = 4;

/// Everything the streaming engine needs after setup: a restored window executor
/// wired to a checkpoint-rewound source reader, plus the epoch the next
/// checkpoint should carry.
struct StreamingRun {
    handle: JobHandle,
    reader: Box<dyn SourceReader>,
    executor: ContinuousWindowExecutor,
    source_name: String,
    next_epoch: u64,
}

/// Shared streaming setup for both the bounded run and the continuous loop:
/// compile the window plan, locate the source, restore the latest checkpoint
/// (operator state **and** source offset together), and build a window executor
/// rewound to that checkpoint. The returned reader is already positioned at the
/// restored source offset.
async fn streaming_setup(job: &CompiledJob, rt: &EngineRuntime) -> EngineResult<StreamingRun> {
    let plan = compile_streaming_window_sql(&job.query).map_err(|e| EngineError::Unsupported {
        engine: EngineKind::Streaming,
        reason: e.to_string(),
    })?;

    let source = job
        .sources
        .iter()
        .find(|s| s.name == plan.source)
        .ok_or_else(|| {
            EngineError::InvalidJob(format!(
                "window source '{}' is not among the job's sources",
                plan.source
            ))
        })?;

    // The handle's job id keys the checkpoint store (avoids a krishiv-proto dep).
    let handle = JobHandle::from_name(&job.name, JobStatus::Running)?;
    let restored = rt.checkpoint.restore_latest(handle.job_id()).await?;

    // Open the source and rewind it to the checkpointed offset before reading.
    let mut reader = rt.sources.open(source).await?;
    if let Some(payload) = &restored
        && let Some((_, encoded)) = payload
            .source_offsets
            .iter()
            .find(|(name, _)| name == &source.name)
    {
        reader.restore_offset(encoded)?;
    }

    // Build the window operator. At a placement with a durable state directory
    // (single-node / distributed) the operator is file-backed under a per-job
    // subdir so its window state survives a restart; embedded is in-memory.
    let mut executor = match &rt.state_dir {
        Some(base) => {
            let job_state_dir = base.join(&job.name);
            std::fs::create_dir_all(&job_state_dir).map_err(|e| {
                EngineError::Runtime(format!(
                    "create window state dir '{}': {e}",
                    job_state_dir.display()
                ))
            })?;
            ContinuousWindowExecutor::new_with_state_dir(plan.spec, Some(&job_state_dir))
                .map_err(exec_err)?
        }
        None => ContinuousWindowExecutor::new(plan.spec).map_err(exec_err)?,
    };
    if let Some(payload) = &restored
        && !payload.operator_state.is_empty()
    {
        executor
            .restore_from_snapshot(&payload.operator_state)
            .map_err(exec_err)?;
    }

    let next_epoch = restored.as_ref().map_or(1, |payload| payload.epoch + 1);
    Ok(StreamingRun {
        handle,
        reader,
        executor,
        source_name: source.name.clone(),
        next_epoch,
    })
}

/// Open one writer per sink spec, kept open for the lifetime of a run.
async fn open_writers(
    rt: &EngineRuntime,
    sinks: &[SinkSpec],
) -> EngineResult<Vec<Box<dyn SinkWriter>>> {
    let mut writers = Vec::with_capacity(sinks.len());
    for spec in sinks {
        writers.push(rt.sinks.open(spec).await?);
    }
    Ok(writers)
}

/// Write non-empty output batches as insert-only changelogs to every sink.
async fn emit_to_writers(
    writers: &mut [Box<dyn SinkWriter>],
    outputs: &[arrow::record_batch::RecordBatch],
) -> EngineResult<()> {
    for writer in writers.iter_mut() {
        for batch in outputs {
            if batch.num_rows() == 0 {
                continue;
            }
            writer.write(ChangelogBatch::inserts(batch.clone())).await?;
        }
    }
    Ok(())
}

/// Persist one streaming checkpoint epoch: operator state and the source offset
/// travel together in one payload so a later restore is consistent.
async fn persist_streaming_checkpoint(
    rt: &EngineRuntime,
    handle: &JobHandle,
    executor: &mut ContinuousWindowExecutor,
    source_name: &str,
    source_offset: Option<Vec<u8>>,
    epoch: u64,
) -> EngineResult<()> {
    let operator_state = executor.snapshot().map_err(exec_err)?;
    let source_offsets = source_offset
        .map(|encoded| vec![(source_name.to_string(), encoded)])
        .unwrap_or_default();
    rt.checkpoint
        .persist(
            handle.job_id(),
            &CheckpointPayload {
                epoch,
                operator_state,
                source_offsets,
            },
        )
        .await
}

/// A spawned, continuously-running streaming job with a clean stop control.
///
/// [`spawn_streaming_job`] returns this immediately while the job keeps draining
/// its (unbounded) source on a background task. [`stop`](Self::stop) signals the
/// loop, waits for it to flush and persist a final checkpoint, and returns the
/// terminal [`JobHandle`].
#[derive(Debug)]
pub struct RunningJob {
    handle: JobHandle,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<EngineResult<JobHandle>>,
}

impl RunningJob {
    /// The job handle as of spawn (status [`JobStatus::Running`]).
    pub fn handle(&self) -> &JobHandle {
        &self.handle
    }

    /// Signal the loop to stop, wait for it to drain, and return the terminal
    /// handle (status [`JobStatus::Completed`] on a clean stop).
    pub async fn stop(self) -> EngineResult<JobHandle> {
        // Ignore send errors: a closed receiver means the loop already exited.
        let _ = self.stop.send(true);
        match self.task.await {
            Ok(result) => result,
            Err(join_err) => Err(EngineError::Runtime(format!(
                "streaming job task failed to join: {join_err}"
            ))),
        }
    }
}

/// Spawn an unbounded, continuously-running streaming job.
///
/// Unlike [`run_job`] (which drains a bounded source once and returns), this
/// keeps polling the source on a background task, emitting closed windows and
/// checkpointing periodically, until [`RunningJob::stop`] is called. The window
/// plan is validated synchronously so an unsupported query fails fast here
/// rather than on the background task.
pub fn spawn_streaming_job(job: CompiledJob, rt: EngineRuntime) -> EngineResult<RunningJob> {
    if job.engine != EngineKind::Streaming {
        return Err(EngineError::Unsupported {
            engine: EngineKind::Streaming,
            reason: format!(
                "spawn_streaming_job requires the streaming engine; job declares {}",
                job.engine
            ),
        });
    }
    // Fail fast on a non-windowed query before spawning anything.
    compile_streaming_window_sql(&job.query).map_err(|e| EngineError::Unsupported {
        engine: EngineKind::Streaming,
        reason: e.to_string(),
    })?;

    let handle = JobHandle::from_name(&job.name, JobStatus::Running)?;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(run_streaming_continuous(job, rt, stop_rx));
    Ok(RunningJob {
        handle,
        stop: stop_tx,
        task,
    })
}

/// The continuous streaming loop: drains the source incrementally, emitting
/// closed windows as input arrives and checkpointing every
/// [`STREAMING_CHECKPOINT_EVERY`] batches. When the source is momentarily idle
/// (an unbounded source between arrivals), it waits a short tick or for the stop
/// signal. On stop it flushes and persists a final checkpoint.
async fn run_streaming_continuous(
    job: CompiledJob,
    rt: EngineRuntime,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) -> EngineResult<JobHandle> {
    let mut setup = streaming_setup(&job, &rt).await?;
    let mut writers = open_writers(&rt, &job.sinks).await?;
    let idle_tick = std::time::Duration::from_millis(STREAMING_IDLE_TICK_MS);
    let mut batches_since_checkpoint = 0u32;

    loop {
        if *stop_rx.borrow() {
            break;
        }
        match setup.reader.next().await? {
            Some(batch) => {
                let outputs = setup.executor.drain(vec![batch]).map_err(exec_err)?;
                emit_to_writers(&mut writers, &outputs).await?;
                batches_since_checkpoint = batches_since_checkpoint.saturating_add(1);
                if batches_since_checkpoint >= STREAMING_CHECKPOINT_EVERY {
                    let offset = setup.reader.checkpoint_offset();
                    persist_streaming_checkpoint(
                        &rt,
                        &setup.handle,
                        &mut setup.executor,
                        &setup.source_name,
                        offset,
                        setup.next_epoch,
                    )
                    .await?;
                    setup.next_epoch = setup.next_epoch.saturating_add(1);
                    batches_since_checkpoint = 0;
                }
            }
            None => {
                // Source idle: wait for a stop signal or a short poll tick.
                // A dropped sender (`changed` errors) also means stop.
                tokio::select! {
                    res = stop_rx.changed() => {
                        if res.is_err() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(idle_tick) => {}
                }
            }
        }
    }

    for writer in &mut writers {
        writer.flush().await?;
    }
    let offset = setup.reader.checkpoint_offset();
    persist_streaming_checkpoint(
        &rt,
        &setup.handle,
        &mut setup.executor,
        &setup.source_name,
        offset,
        setup.next_epoch,
    )
    .await?;

    Ok(JobHandle::new(
        setup.handle.job_id().clone(),
        JobStatus::Completed,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_engine_core::mem::{
        InMemoryCheckpointService, InMemorySinkProvider, InMemorySourceProvider,
        InMemoryUpsertSink, embedded_runtime,
    };
    use krishiv_engine_core::{
        CheckpointService, DurableCheckpointService, RowKind, SinkProvider, SinkSpec, SourceSpec,
    };

    use super::*;

    fn kv_batch(keys: &[&str], vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn batch_engine_runs_sql_and_writes_inserts() {
        let sources = InMemorySourceProvider::new();
        sources.insert("t", vec![kv_batch(&["a", "b", "a"], &[1, 2, 3])]);
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "sum-job",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);

        let handle = run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let out = sink.take("out");
        assert_eq!(out.len(), 1);
        let cl = out.first().unwrap();
        assert!(cl.is_append_only());
        let total = cl
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn incremental_engine_materializes_grouped_aggregate() {
        let sources = InMemorySourceProvider::new();
        sources.insert("t", vec![kv_batch(&["a", "b", "a"], &[1, 2, 3])]);
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "agg-job",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);

        let handle = run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Running);

        let out = sink.take("out");
        assert_eq!(out.len(), 1, "one changelog emitted for one input batch");
        let cl = out.first().unwrap();
        // Two groups (a => 4, b => 2); all inserts in the first changelog.
        assert_eq!(cl.num_rows(), 2);
        assert!(cl.row_kinds().iter().all(|k| *k == RowKind::Insert));
    }

    #[tokio::test]
    async fn incremental_engine_emits_retraction_when_aggregate_changes() {
        // Two input batches: {a=1, b=2} then {a=10}. The view SUM(v) GROUP BY k
        // must retract a's old total (1) and insert the new total (11) on the
        // second batch — a real changelog, not a fresh full dump.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "t",
            vec![kv_batch(&["a", "b"], &[1, 2]), kv_batch(&["a"], &[10])],
        );
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "agg",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        run_job(job, rt).await.unwrap();

        let out = sink.take("out");
        let kinds: Vec<RowKind> = out.iter().flat_map(|cl| cl.row_kinds().to_vec()).collect();
        assert!(
            kinds.contains(&RowKind::Delete),
            "a changed aggregate must emit a retraction of the old row"
        );

        // Fold the whole changelog stream through the reference upsert sink and
        // confirm the net materialized view: a -> 11, b -> 2.
        let schema = out
            .first()
            .expect("at least one changelog emitted")
            .batch()
            .schema();
        let upsert = InMemoryUpsertSink::new(vec![0]);
        let mut writer = upsert
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .unwrap();
        for cl in &out {
            writer.write(cl.clone()).await.unwrap();
        }
        let table = upsert.table(&schema).unwrap();
        assert_eq!(table.num_rows(), 2);
        // The incremental aggregate computes SUM as Float64.
        let totals = table
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // Keys sort a < b.
        assert_eq!(totals.value(0), 11.0, "a's net total after the update");
        assert_eq!(totals.value(1), 2.0, "b unchanged");
    }

    #[tokio::test]
    async fn incremental_engine_writes_net_table_through_consolidating_sink() {
        // End-to-end: the incremental engine emits retractions, and a
        // ConsolidatingSinkProvider (what the connector/file-sink path uses) folds
        // them into the net materialized table — one insert-only write, no
        // retraction reaching the append-only sink underneath.
        use krishiv_engine_core::ConsolidatingSinkProvider;

        let sources = InMemorySourceProvider::new();
        sources.insert(
            "t",
            vec![kv_batch(&["a", "b"], &[1, 2]), kv_batch(&["a"], &[10])],
        );
        let collected = InMemorySinkProvider::new();
        let sink = ConsolidatingSinkProvider::new(Arc::new(collected.clone()));
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink));

        let job = CompiledJob::new(
            "agg-consolidated",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        run_job(job, rt).await.unwrap();

        // The consolidating sink wrote the net table exactly once, insert-only.
        let out = collected.take("out");
        assert!(
            out.iter().all(ChangelogBatch::is_append_only),
            "the append-only sink never sees a retraction"
        );
        let net: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(net, 2, "net table is {{a: 11, b: 2}} — two rows");
        let totals: Vec<f64> = out
            .iter()
            .flat_map(|cl| {
                let col = cl
                    .batch()
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap()
                    .clone();
                (0..col.len()).map(move |i| col.value(i))
            })
            .collect();
        let mut sorted = totals.clone();
        sorted.sort_by(|x, y| x.partial_cmp(y).unwrap());
        assert_eq!(sorted, vec![2.0, 11.0], "a updated to 11, b stays 2");
    }

    #[tokio::test]
    async fn incremental_engine_applies_cdc_deletes_from_source() {
        // A true CDC source: batch 1 inserts (a,1),(b,2); batch 2 deletes (a,1).
        // The maintained view SUM(v) GROUP BY k must drop group a entirely,
        // leaving only b — the source delete becomes a view retraction.
        use krishiv_engine_core::mem::InMemoryCdcSourceProvider;

        let cdc = InMemoryCdcSourceProvider::new();
        cdc.insert(
            "t",
            vec![
                ChangelogBatch::inserts(kv_batch(&["a", "b"], &[1, 2])),
                ChangelogBatch::new(kv_batch(&["a"], &[1]), vec![RowKind::Delete]).unwrap(),
            ],
        );
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(cdc), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "cdc-agg",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        run_job(job, rt).await.unwrap();

        let out = sink.take("out");
        assert!(
            out.iter()
                .flat_map(|cl| cl.row_kinds().to_vec())
                .any(|k| k == RowKind::Delete),
            "the CDC delete must surface as a retraction"
        );

        // Fold the whole changelog stream and confirm the net view is just b.
        let schema = out
            .first()
            .expect("at least one changelog emitted")
            .batch()
            .schema();
        let upsert = InMemoryUpsertSink::new(vec![0]);
        let mut writer = upsert
            .open(&SinkSpec::new("out", "memory", ""))
            .await
            .unwrap();
        for cl in &out {
            writer.write(cl.clone()).await.unwrap();
        }
        let table = upsert.table(&schema).unwrap();
        assert_eq!(table.num_rows(), 1, "only group b survives the CDC delete");
        let key = table
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key.value(0), "b");
    }

    fn event_batch(user: &str, ts: i64, amount: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![user])),
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn streaming_engine_runs_tumbling_window() {
        // 10s tumbling window. Events at ts 1000 then 12000 advance the
        // watermark past 10000, closing window [0, 10000) which emits.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![event_batch("a", 1000, 5), event_batch("a", 12000, 7)],
        );
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "win-job",
            "SELECT user_id, SUM(amount) AS total \
             FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 10000) \
             GROUP BY user_id, window_start, window_end",
            vec![SourceSpec::unbounded("events", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming);
        assert_eq!(job.engine, EngineKind::Streaming);

        let handle = run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Running);

        let out = sink.take("out");
        let rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert!(rows > 0, "expected the first closed window to emit a row");
    }

    fn tumbling_job(name: &str) -> CompiledJob {
        CompiledJob::new(
            name,
            "SELECT user_id, SUM(amount) AS total \
             FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 10000) \
             GROUP BY user_id, window_start, window_end",
            vec![SourceSpec::unbounded("events", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming)
    }

    /// Build an embedded runtime that shares one checkpoint service, so a job's
    /// persisted checkpoint is visible to a later run (the restore path).
    fn runtime_with_checkpoint(
        sources: InMemorySourceProvider,
        sink: InMemorySinkProvider,
        checkpoint: InMemoryCheckpointService,
    ) -> EngineRuntime {
        let mut rt = embedded_runtime(Arc::new(sources), Arc::new(sink));
        rt.checkpoint = Arc::new(checkpoint);
        rt
    }

    #[tokio::test]
    async fn streaming_engine_persists_and_restores_checkpoints() {
        let checkpoint = InMemoryCheckpointService::new();

        // First run: drains the source and persists epoch 1 carrying operator
        // state AND the source offset together.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![event_batch("a", 1000, 5), event_batch("a", 12000, 7)],
        );
        let rt = runtime_with_checkpoint(sources, InMemorySinkProvider::new(), checkpoint.clone());
        let handle = run_job(tumbling_job("cp-job"), rt).await.unwrap();

        let payload = checkpoint
            .restore_latest(handle.job_id())
            .await
            .unwrap()
            .expect("a checkpoint must be persisted after the run");
        assert_eq!(payload.epoch, 1);
        assert!(
            !payload.operator_state.is_empty(),
            "window operator state must be captured"
        );
        assert_eq!(
            payload
                .source_offsets
                .first()
                .map(|(name, _)| name.as_str()),
            Some("events"),
            "the source offset travels with the operator state"
        );

        // Second run against the same checkpoint service: it restores the prior
        // epoch (operator state + source offset) and persists the next epoch.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![event_batch("a", 1000, 5), event_batch("a", 12000, 7)],
        );
        let rt = runtime_with_checkpoint(sources, InMemorySinkProvider::new(), checkpoint.clone());
        let handle = run_job(tumbling_job("cp-job"), rt).await.unwrap();

        let payload = checkpoint
            .restore_latest(handle.job_id())
            .await
            .unwrap()
            .expect("checkpoint still present after the second run");
        assert_eq!(payload.epoch, 2, "the epoch advances across runs");
    }

    #[tokio::test]
    async fn single_node_durable_streaming_restores_across_fresh_runtime() {
        // Single-node durability: the checkpoint lands in a *file-backed* store and
        // the window operator state in an on-disk state_dir. The second run uses
        // brand-new, independent service instances over the SAME directories — so a
        // successful restore proves recovery survives a real process restart, not
        // merely a shared in-memory handle (which is what the embedded test covers).
        let ckpt_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();

        fn durable_rt(
            sources: InMemorySourceProvider,
            ckpt_path: &std::path::Path,
            state_path: &std::path::Path,
        ) -> EngineRuntime {
            let mut rt = embedded_runtime(Arc::new(sources), Arc::new(InMemorySinkProvider::new()));
            rt.checkpoint = Arc::new(DurableCheckpointService::new(ckpt_path).unwrap());
            rt.state_dir = Some(state_path.to_path_buf());
            rt
        }

        // First run → persists epoch 1 to disk.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![event_batch("a", 1000, 5), event_batch("a", 12000, 7)],
        );
        let rt = durable_rt(sources, ckpt_dir.path(), state_dir.path());
        let handle = run_job(tumbling_job("durable-cp"), rt).await.unwrap();

        // A brand-new service instance reads the checkpoint back from disk.
        let payload = DurableCheckpointService::new(ckpt_dir.path())
            .unwrap()
            .restore_latest(handle.job_id())
            .await
            .unwrap()
            .expect("checkpoint must be on disk after the run");
        assert_eq!(payload.epoch, 1);
        assert!(
            !payload.operator_state.is_empty(),
            "window operator state is persisted to disk"
        );
        assert!(
            std::fs::read_dir(ckpt_dir.path())
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".ckpt")),
            "a .ckpt file is present on disk"
        );

        // Second run: fresh runtime + fresh services over the same dirs → restores
        // epoch 1 (operator state + source offset from disk) and persists epoch 2.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![event_batch("a", 1000, 5), event_batch("a", 12000, 7)],
        );
        let rt = durable_rt(sources, ckpt_dir.path(), state_dir.path());
        let handle = run_job(tumbling_job("durable-cp"), rt).await.unwrap();

        let payload = DurableCheckpointService::new(ckpt_dir.path())
            .unwrap()
            .restore_latest(handle.job_id())
            .await
            .unwrap()
            .expect("checkpoint still on disk after the second run");
        assert_eq!(
            payload.epoch, 2,
            "the epoch advances across a process-restart restore"
        );
    }

    #[tokio::test]
    async fn streaming_spawn_runs_continuously_until_stopped() {
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![event_batch("a", 1000, 5), event_batch("a", 12000, 7)],
        );
        let sink = InMemorySinkProvider::new();
        let checkpoint = InMemoryCheckpointService::new();
        let rt = runtime_with_checkpoint(sources, sink.clone(), checkpoint.clone());

        let running = spawn_streaming_job(tumbling_job("spawn-job"), rt).unwrap();
        assert_eq!(running.handle().status(), JobStatus::Running);

        // Let the loop drain the two batches and emit the closed [0,10000) window.
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let final_handle = running.stop().await.unwrap();
        assert_eq!(
            final_handle.status(),
            JobStatus::Completed,
            "a cleanly stopped streaming job reports Completed"
        );

        let out = sink.take("out");
        let rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert!(
            rows > 0,
            "the continuous loop should have emitted the closed window before stop"
        );
        assert!(
            checkpoint
                .restore_latest(final_handle.job_id())
                .await
                .unwrap()
                .is_some(),
            "the continuous loop persists a checkpoint"
        );
    }

    #[tokio::test]
    async fn spawn_streaming_job_rejects_non_streaming_engine() {
        let rt = embedded_runtime(
            Arc::new(InMemorySourceProvider::new()),
            Arc::new(InMemorySinkProvider::new()),
        );
        let job = CompiledJob::new(
            "batch-job",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);
        let err = spawn_streaming_job(job, rt).unwrap_err();
        assert!(matches!(err, EngineError::Unsupported { .. }));
    }

    #[tokio::test]
    async fn streaming_engine_rejects_non_windowed_query() {
        let rt = embedded_runtime(
            Arc::new(InMemorySourceProvider::new()),
            Arc::new(InMemorySinkProvider::new()),
        );
        let job = CompiledJob::new(
            "no-win",
            "SELECT k FROM t",
            vec![SourceSpec::unbounded("t", "kafka", "topic")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming);

        let err = run_job(job, rt).await.unwrap_err();
        assert!(matches!(err, EngineError::Unsupported { .. }));
    }
}
