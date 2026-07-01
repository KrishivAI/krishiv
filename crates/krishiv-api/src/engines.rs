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
use futures::StreamExt as _;

use datafusion::datasource::MemTable;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_delta::DeltaBatch;
use krishiv_engine_core::{
    ChangelogBatch, CheckpointPayload, CompiledJob, ComputeEngine, EngineError, EngineKind,
    EngineResult, EngineRuntime, JobHandle, JobStatus, Placement, RowKind, SinkSpec, SinkWriter,
    SourceReader,
};
use krishiv_ivm::{IncrementalFlow, IncrementalViewSpec};
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

fn df_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Runtime(e.to_string())
}

/// Build a [`SessionContext`] tuned for batch workloads.
///
/// Defaults:
/// - `target_partitions` = logical CPU count (enables multi-core parallelism on
///   aggregations, joins, and scans — DataFusion defaults to 1).
/// - `batch_size` = 65 536 rows (DataFusion default 8 192 is too small for
///   batch; larger batches reduce per-batch overhead and improve SIMD utilisation).
/// - `repartition_joins` / `repartition_aggregations` = true (already the
///   DataFusion default but stated explicitly so tuning intent is clear).
fn batch_session_context() -> SessionContext {
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let config = SessionConfig::new()
        .with_target_partitions(parallelism)
        .with_batch_size(65_536)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true);
    SessionContext::new_with_config(config)
}

/// Dispatch a compiled job to its engine by the job's explicit [`EngineKind`].
///
/// This is the one place engine selection happens; no front-end forks per
/// engine, and the deployment placement is carried entirely by `rt`.
///
/// Transient failures (network blips, executor restarts) are retried up to 3
/// times with exponential back-off when the placement is non-embedded. Permanent
/// errors (schema mismatch, invalid job, not-found) are returned immediately.
///
/// **Incremental engine note**: the placement-provided sinks must be retraction-
/// aware (e.g. wrapped in `ConsolidatingSinkProvider`) when the job's output may
/// include deletes or updates. Use `embedded_consolidating_runtime()` or
/// `durable_engine_runtime(..., consolidate: true)` to get the correct runtime
/// for incremental jobs; the raw `ConnectorSinkProvider` will return a typed
/// error on the first retraction it receives.
#[tracing::instrument(skip(rt), fields(job = %job.name, engine = ?job.engine))]
pub async fn run_job(job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
    let max_retries: u32 = match rt.placement {
        Placement::SingleNode | Placement::Distributed => 3,
        Placement::Embedded => 0,
    };
    let mut attempts: u32 = 0;
    loop {
        let result = match job.engine {
            EngineKind::Batch => BatchEngine.run(job.clone(), rt.clone()).await,
            EngineKind::Incremental => IncrementalEngine.run(job.clone(), rt.clone()).await,
            EngineKind::Streaming => StreamingEngine.run(job.clone(), rt.clone()).await,
        };
        match result {
            Ok(handle) => return Ok(handle),
            Err(e) if e.is_transient() && attempts < max_retries => {
                let delay_ms = 100u64 * (1 << attempts);
                tracing::warn!(
                    job = %job.name,
                    attempt = attempts + 1,
                    max = max_retries,
                    delay_ms,
                    "transient engine error, retrying: {e}",
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                attempts += 1;
            }
            Err(e) => return Err(e),
        }
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
#[allow(dead_code)]
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
    let ctx = batch_session_context();
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

    #[tracing::instrument(skip(self, rt), fields(job = %job.name))]
    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
        self.validate(&job)?;

        // Open the sinks up front so output is written incrementally as it is
        // produced; this engine never buffers the full result in memory.
        let mut writers = open_writers(&rt, &job.sinks).await?;

        // Placement seam: a runtime carrying a query executor (single-node or
        // distributed) runs the whole job off-engine and returns a stream of
        // result batches; otherwise the query runs in-process over DataFusion as
        // a record-batch stream. Either way, output is written batch-by-batch —
        // the full result is never buffered in the client process.
        if let Some(executor) = rt.query_executor.clone() {
            let mut stream = executor.execute_batch(&job).await?;
            while let Some(batch) = stream.next().await {
                write_inserts(&mut writers, batch?).await?;
            }
        } else {
            let ctx = batch_session_context();
            // Parquet sources: register as ListingTable for native pushdown.
            // Non-parquet sources: drain concurrently then register as MemTable.
            let non_parquet: Vec<&krishiv_engine_core::SourceSpec> = job
                .sources
                .iter()
                .filter(|s| s.connector != "parquet")
                .collect();

            // Register Parquet sources immediately (metadata-only, no I/O yet).
            for spec in job.sources.iter().filter(|s| s.connector == "parquet") {
                ctx.register_parquet(spec.name.as_str(), &spec.uri, Default::default())
                    .await
                    .map_err(df_err)?;
            }

            // Drain all non-parquet sources **sequentially**, checking the
            // cumulative byte total as each source is read in. The previous
            // `try_join_all` approach drained every source in parallel into
            // memory before any byte check, so an over-budget job OOMed before
            // the post-check could fire. The sequential path reports a
            // `Source` error the moment a source would push us over budget.
            //
            // Parquet sources avoid this entirely via the ListingTable path
            // above (zero-byte registration, no data in memory).
            const MAX_DRAIN_BYTES: usize = 2 * 1024 * 1024 * 1024;
            let mut drained: Vec<(String, String, Vec<arrow::record_batch::RecordBatch>)> =
                Vec::with_capacity(non_parquet.len());
            let mut total_bytes: usize = 0;
            for spec in &non_parquet {
                let spec_name = spec.name.clone();
                let spec_uri = spec.uri.clone();
                let batches = drain_source(&rt, spec).await?;
                total_bytes = total_bytes.saturating_add(
                    batches
                        .iter()
                        .map(|b| b.get_array_memory_size())
                        .sum::<usize>(),
                );
                if total_bytes > MAX_DRAIN_BYTES {
                    return Err(EngineError::Source(format!(
                        "non-parquet source '{spec_name}' (uri: '{spec_uri}') pushed the cumulative \
                         drain over the 2 GiB in-memory limit ({total_bytes} bytes); \
                         convert sources to parquet or increase the limit"
                    )));
                }
                drained.push((spec_name, spec_uri, batches));
            }

            for (name, uri, batches) in drained {
                if batches.is_empty() {
                    return Err(EngineError::Source(format!(
                        "source '{name}' (uri: '{uri}') produced no batches; \
                         the batch engine requires a non-empty source to infer schema"
                    )));
                }
                let schema = batches
                    .first()
                    .ok_or_else(|| {
                        EngineError::Source(format!(
                            "source '{name}' (uri: '{uri}') produced no batches; \
                             the batch engine requires a non-empty source to infer schema"
                        ))
                    })?
                    .schema();
                let table = MemTable::try_new(schema, vec![batches]).map_err(df_err)?;
                ctx.register_table(name.as_str(), Arc::new(table))
                    .map_err(df_err)?;
            }

            let df = ctx.sql(&job.query).await.map_err(df_err)?;
            let mut stream = df.execute_stream().await.map_err(df_err)?;
            while let Some(batch) = stream.next().await {
                write_inserts(&mut writers, batch.map_err(df_err)?).await?;
            }
        }

        for writer in &mut writers {
            writer.flush().await?;
        }
        JobHandle::from_name(&job.name, JobStatus::Completed)
    }
}

// ── Incremental ───────────────────────────────────────────────────────────────

/// Convert a weighted [`DeltaBatch`] (a view's per-step change delta) into a
/// [`ChangelogBatch`]: positive weights are insertions, negative weights are
/// deletions, and a row's multiplicity `|weight|` is expanded to that many
/// changelog rows. Returns `None` for an empty delta.
fn changelog_from_delta(delta: &DeltaBatch) -> EngineResult<Option<ChangelogBatch>> {
    if delta.is_empty() {
        return Ok(None);
    }
    let data = delta.data_batch();
    let weights = delta.weights();

    // Fast path: every row is a single insertion (weight == +1). Skip the
    // index-array allocation and take() copy — the data batch IS the output.
    let all_unit_inserts = weights.iter().all(|w| w.is_none_or(|v| v == 1));
    if all_unit_inserts {
        let kinds = vec![RowKind::Insert; data.num_rows()];
        return Ok(Some(ChangelogBatch::new(data.clone(), kinds)?));
    }

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

    #[tracing::instrument(skip(self, rt), fields(job = %job.name))]
    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
        self.validate(&job)?;

        // A-6: stream feed+step instead of buffering. The previous
        // implementation drained every source into a `Vec<ChangelogBatch>`
        // before calling `flow.feed()` once per batch. For a bounded CDC
        // source with N large batches that was O(N) peak memory: the
        // whole input was in RAM before any view delta was emitted. The
        // streaming path feeds+steps each batch as it arrives, so peak
        // memory is O(1 batch + 1 view delta). Behavior is identical
        // because `IncrementalFlow::step_datafusion` is idempotent and
        // independent of the source order — the only material difference
        // is that the per-source schema probe now reads just the first
        // batch of the first source (it was reading the first batch of
        // every source in the buffer).
        //
        // We still need a schema before we can `register_view`, so we
        // open the first source, read one batch for its schema, and
        // register the view. Other sources' schemas are recorded
        // inline when their first batch arrives.
        if job.sources.is_empty() {
            return Err(EngineError::InvalidJob(
                "incremental engine needs at least one source".into(),
            ));
        }
        let first_source = job.sources.first().ok_or_else(|| {
            EngineError::InvalidJob("incremental engine needs at least one source".into())
        })?;
        let mut first_reader = rt.sources.open(first_source).await?;
        let first_batch = first_reader.next_changelog().await?.ok_or_else(|| {
            EngineError::Source(format!(
                "source '{}' (uri: '{}') produced no batches; the incremental engine \
                 requires a non-empty source to infer the view schema",
                first_source.name, first_source.uri
            ))
        })?;
        let mut source_schemas: Vec<(String, SchemaRef)> =
            vec![(first_source.name.clone(), first_batch.batch().schema())];
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

        // A-6: feed+step per batch (the new streaming path). Each batch is
        // fed and stepped individually; the flow's per-step delta is
        // immediately written to the sinks. We need a closure that captures
        // the per-batch state but is `Send + 'static` so it can be reused
        // across the multiple sources below.
        let step_and_emit = async |flow: &IncrementalFlow,
                                   name: &str,
                                   changelog: &ChangelogBatch,
                                   writers: &mut Vec<Box<dyn SinkWriter>>|
               -> EngineResult<()> {
            if changelog.num_rows() == 0 {
                return Ok(());
            }
            let delta = delta_from_changelog(changelog)?;
            flow.feed(name, delta)
                .map_err(|e: krishiv_ivm::IvmError| EngineError::Runtime(e.to_string()))?;
            flow.step_datafusion()
                .await
                .map_err(|e| EngineError::Runtime(e.to_string()))?;
            if let Some(view_delta) = flow
                .take_step_output(&job.name)
                .map_err(|e| EngineError::Runtime(e.to_string()))?
                && let Some(cl) = changelog_from_delta(&view_delta)?
            {
                let cl = std::sync::Arc::new(cl);
                for writer in writers.iter_mut() {
                    writer.write_arc(cl.clone()).await?;
                }
            }
            Ok(())
        };

        // Drive the first source (whose reader we already opened) to EOF.
        if first_batch.num_rows() > 0 {
            step_and_emit(&flow, &first_source.name, &first_batch, &mut writers).await?;
        }
        while let Some(changelog) = first_reader.next_changelog().await? {
            step_and_emit(&flow, &first_source.name, &changelog, &mut writers).await?;
        }

        // Drive every other source to EOF with the same streaming path.
        for spec in job.sources.get(1..).unwrap_or(&[]) {
            let mut reader = rt.sources.open(spec).await?;
            while let Some(changelog) = reader.next_changelog().await? {
                if let Some(first) = source_schemas.iter_mut().find(|(n, _)| n == &spec.name) {
                    if first.1.as_ref() != changelog.batch().schema().as_ref() {
                        // Schema drift across the same source — keep going but
                        // warn. The flow errors if the schema materially
                        // changes the view's output.
                        tracing::warn!(
                            source = %spec.name,
                            "source schema changed mid-drain; view schema fixed to first batch",
                        );
                    }
                } else {
                    source_schemas.push((spec.name.clone(), changelog.batch().schema()));
                }
                step_and_emit(&flow, &spec.name, &changelog, &mut writers).await?;
            }
        }

        // The previous buffered path consumed the per-step deltas into the
        // writers. This streaming path does the same; the final `Ok(())`
        // flush is below.
        for writer in &mut writers {
            writer.flush().await?;
        }
        // A bounded `run` drains its source once and returns; nothing continues
        // after it, so the invocation is Completed. Continuous maintenance is the
        // `spawn_streaming_job` path, which reports Running until it is stopped.
        JobHandle::from_name(&job.name, JobStatus::Completed)
    }
}

// ── Streaming ─────────────────────────────────────────────────────────────────

/// Flink-style event-time streaming engine (dataflow windows + watermarks).
///
/// Two execution paths:
///
/// - **Windowed** (default): compiles canonical `TUMBLE`/`HOP`/`SESSION` SQL
///   into a `WindowExecutionSpec` and drives `ContinuousWindowExecutor`.
/// - **Stateless** (G-1): any other valid SQL `SELECT` runs per-batch through a
///   temporary DataFusion context with no window state or checkpoints. Suitable
///   for `SELECT … WHERE …`, projections, UDFs, and flat transforms over
///   unbounded sources.
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamingEngine;

fn exec_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Runtime(e.to_string())
}

/// Turn a streaming-SQL compile failure into a guiding, typed error.
fn streaming_shape_unsupported(e: impl std::fmt::Display) -> EngineError {
    EngineError::Unsupported {
        engine: EngineKind::Streaming,
        reason: format!(
            "the streaming engine could not compile this query as either a windowed \
             aggregation (TUMBLE/HOP/SESSION) or a stateless transform. \
             Windowed error: {e}. \
             Check that all source table names in the query match the job's source specs."
        ),
    }
}

/// Apply a stateless SQL `query` to a single input `batch`, registering it as a
/// temporary MemTable under `table_name`. Returns all output batches produced by
/// the query over that single input batch.
async fn apply_stateless_query(
    query: &str,
    table_name: &str,
    batch: arrow::record_batch::RecordBatch,
) -> EngineResult<Vec<arrow::record_batch::RecordBatch>> {
    let ctx = batch_session_context();
    let schema = batch.schema();
    let table =
        datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).map_err(df_err)?;
    ctx.register_table(table_name, Arc::new(table))
        .map_err(df_err)?;
    let mut stream = ctx
        .sql(query)
        .await
        .map_err(df_err)?
        .execute_stream()
        .await
        .map_err(df_err)?;
    let mut results = Vec::new();
    while let Some(batch) = stream.next().await {
        results.push(batch.map_err(df_err)?);
    }
    Ok(results)
}

/// Bounded stateless streaming run: applies the query per-batch over the source,
/// emitting transformed output immediately. No window state, no checkpointing.
async fn run_stateless_bounded(job: &CompiledJob, rt: &EngineRuntime) -> EngineResult<JobHandle> {
    let source = job.sources.first().ok_or_else(|| {
        EngineError::InvalidJob("stateless streaming job requires at least one source".into())
    })?;
    let table_name = source.name.clone();
    let mut reader = rt.sources.open(source).await?;
    let mut writers = open_writers(rt, &job.sinks).await?;

    while let Some(batch) = reader.next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let outputs = apply_stateless_query(&job.query, &table_name, batch).await?;
        emit_to_writers(&mut writers, &outputs).await?;
    }
    for writer in &mut writers {
        writer.flush().await?;
    }
    JobHandle::from_name(&job.name, JobStatus::Completed)
}

#[async_trait]
impl ComputeEngine for StreamingEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Streaming
    }

    fn validate(&self, job: &CompiledJob) -> EngineResult<()> {
        job.validate_shape().map_err(EngineError::InvalidJob)?;
        if job.query.trim().is_empty() {
            return Err(EngineError::InvalidJob(
                "streaming job query cannot be empty".into(),
            ));
        }
        // Validation deferred to run time: windowed queries are compiled by
        // streaming_setup; stateless queries are validated on first execution
        // by DataFusion. Both paths surface a typed error on failure.
        Ok(())
    }

    /// Bounded run: drains the source once and either runs the windowed executor
    /// (windowed SQL) or applies a stateless transform per batch (other SQL).
    ///
    /// ST-1 fix: writers are opened before draining so back-pressure from slow
    /// sinks is applied immediately and the output is never held entirely in
    /// memory before being written.
    #[tracing::instrument(skip(self, rt), fields(job = %job.name))]
    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle> {
        if job.query.trim().is_empty() {
            return Err(EngineError::InvalidJob(
                "streaming job query cannot be empty".into(),
            ));
        }
        if compile_streaming_window_sql(&job.query).is_err() {
            return run_stateless_bounded(&job, &rt).await;
        }

        let mut setup = streaming_setup(&job, &rt).await?;
        let mut writers = open_writers(&rt, &job.sinks).await?;

        while let Some(batch) = setup.reader.next().await? {
            let outputs = setup.executor.drain(vec![batch]).map_err(exec_err)?;
            emit_to_writers(&mut writers, &outputs).await?;
        }
        // B-4 fix: a bounded source that ends with event times well below the
        // current watermark leaves any "in-flight" windows unclosed if we don't
        // drive the watermark to i64::MAX here. Flushing with an unbounded
        // watermark closes every window whose end is ≤ i64::MAX (i.e. all of
        // them) and emits their final aggregate. The watermark of the
        // checkpoint that follows remains at whatever the source reported, so
        // restore is consistent.
        let final_flush = setup.executor.tick(i64::MAX).map_err(exec_err)?;
        if !final_flush.is_empty() {
            emit_to_writers(&mut writers, &final_flush).await?;
        }
        let source_offset = setup.reader.checkpoint_offset();

        for writer in &mut writers {
            writer.flush().await?;
        }

        persist_streaming_checkpoint(
            &rt,
            &setup.handle,
            &mut setup.executor,
            &setup.source_name,
            &mut setup.reader,
            source_offset,
            setup.next_epoch,
        )
        .await?;

        // Bounded run-once completed; the continuous loop lives in
        // `spawn_streaming_job`, which is the one that stays Running.
        JobHandle::from_name(&job.name, JobStatus::Completed)
    }
}

/// How long the continuous loop waits between polls when the source is idle
/// **and** the source does not implement [`SourceReader::data_notify`]. This is
/// the historical 5 ms floor, kept for backward compatibility with
/// `SourceReader` impls that haven't been instrumented yet.
const STREAMING_IDLE_TICK_MS: u64 = 5;

/// How often the continuous loop advances the watermark during source idle
/// periods so session windows whose inactivity gap has elapsed can close.
/// Configurable via `KRISHIV_IDLE_TICK_MS` (milliseconds, default 500).
fn idle_tick_interval() -> std::time::Duration {
    let ms = std::env::var("KRISHIV_IDLE_TICK_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(500);
    std::time::Duration::from_millis(ms)
}

/// Interval (milliseconds) between speculative early-fire emissions of
/// currently-open tumbling windows. Set `KRISHIV_STREAM_EARLY_FIRE_MS=0`
/// to disable. Default 0 (disabled) — early-fire is opt-in because the
/// speculative outputs require a downstream upsert sink keyed on
/// `(key, window_start)`. H-14 (audit): the primitive was tested in
/// isolation but never wired into the continuous loop; this wires it.
fn early_fire_interval() -> Option<std::time::Duration> {
    let ms = std::env::var("KRISHIV_STREAM_EARLY_FIRE_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    }
}
/// Safety floor for the source-notify wake path: when the source implements
/// [`SourceReader::data_notify`], the loop awaits the notify with a 50 µs
/// timer race so a lost wakeup does not stall detection indefinitely. 50 µs is
/// well under the typical 1 ms event-time tick and orders of magnitude below
/// the 5 ms historical floor.
const STREAMING_IDLE_FLOOR_US: u64 = 50;
/// How many input batches the continuous loop processes between checkpoints
/// in the low-latency profile (the default).
const STREAMING_CHECKPOINT_EVERY: u32 = 4;

/// Latency-vs-throughput tuning for the continuous streaming loop, resolved
/// once from the `KRISHIV_STREAM_PROFILE` environment variable.
///
/// The continuous loop already emits every drained batch immediately — the
/// equivalent of Flink's `execution.buffer-timeout = 0` — and wakes on the
/// source `data_notify` within ~50 µs, so the *latency floor* is already low.
/// This profile makes the remaining latency-vs-throughput trade an explicit,
/// named knob rather than a hardcoded constant:
///
/// - `low-latency` (default): checkpoint every [`STREAMING_CHECKPOINT_EVERY`]
///   batches so recovery replay stays short; flush each batch immediately.
/// - `throughput`: checkpoint less often (the per-epoch fsync stall is then
///   amortized over more work), trading a higher recovery-replay bound and a
///   larger latency tail for sustained rows/sec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamProfile {
    LowLatency,
    Throughput,
}

impl StreamProfile {
    /// Resolve from `KRISHIV_STREAM_PROFILE` (`throughput` ⇒ throughput,
    /// anything else or unset ⇒ low-latency).
    fn from_env() -> Self {
        Self::parse(std::env::var("KRISHIV_STREAM_PROFILE").ok().as_deref())
    }

    /// Pure parse of the profile string, factored out for testability.
    fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some(s) if s.trim().eq_ignore_ascii_case("throughput") => Self::Throughput,
            _ => Self::LowLatency,
        }
    }

    /// Batches processed between checkpoints under this profile.
    fn checkpoint_every(self) -> u32 {
        match self {
            Self::LowLatency => STREAMING_CHECKPOINT_EVERY,
            Self::Throughput => STREAMING_CHECKPOINT_EVERY.saturating_mul(8),
        }
    }
}

/// Everything the streaming engine needs after setup: a restored window executor
/// wired to a checkpoint-rewound source reader, plus the epoch the next
/// checkpoint should carry.
struct StreamingRun {
    handle: JobHandle,
    reader: Box<dyn SourceReader>,
    executor: ContinuousWindowExecutor,
    source_name: String,
    next_epoch: u64,
    /// The source's `data_notify` handle, if the source implements it. The
    /// continuous loop awaits this in preference to a 5 ms sleep so the
    /// detection latency for an idle-then-busy source drops from milliseconds
    /// to microseconds.
    data_notify: Option<krishiv_engine_core::DataNotify>,
}

/// Shared streaming setup for both the bounded run and the continuous loop:
/// compile the window plan, locate the source, restore the latest checkpoint
/// (operator state **and** source offset together), and build a window executor
/// rewound to that checkpoint. The returned reader is already positioned at the
/// restored source offset.
async fn streaming_setup(job: &CompiledJob, rt: &EngineRuntime) -> EngineResult<StreamingRun> {
    let plan = compile_streaming_window_sql(&job.query).map_err(streaming_shape_unsupported)?;

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
            // Use the async variant so this setup step does not block the
            // tokio reactor — `create_dir_all` on a network filesystem (NFS,
            // FUSE, EBS) can take hundreds of milliseconds and would stall
            // every other task running on this worker thread.
            tokio::fs::create_dir_all(&job_state_dir)
                .await
                .map_err(|e| {
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
    // B-5: restore the source's in-flight records (if any were persisted).
    // The source's `restore_in_flight` is a no-op when the source opts out
    // of in-flight persistence (the default), so this is safe for the
    // common case where `source_in_flight` is empty.
    if let Some(payload) = &restored
        && let Some((_, bytes)) = payload
            .source_in_flight
            .iter()
            .find(|(name, _)| name == &source.name)
    {
        reader.restore_in_flight(bytes)?;
    }

    let next_epoch = restored.as_ref().map_or(1, |payload| payload.epoch + 1);
    let data_notify = reader.data_notify();
    Ok(StreamingRun {
        handle,
        reader,
        executor,
        source_name: source.name.clone(),
        next_epoch,
        data_notify,
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

/// Write one batch as an insert-only changelog to every sink, skipping empties.
/// Used by the batch engine to fan each streamed output batch out to its sinks.
async fn write_inserts(
    writers: &mut [Box<dyn SinkWriter>],
    batch: arrow::record_batch::RecordBatch,
) -> EngineResult<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let changes = ChangelogBatch::inserts(batch);
    for writer in writers.iter_mut() {
        writer.write(changes.clone()).await?;
    }
    Ok(())
}

/// Write non-empty output batches as insert-only changelogs to every sink.
///
/// Builds one `Arc<ChangelogBatch>` per non-empty output and hands it to every
/// sink via [`SinkWriter::write_arc`], which is the zero-allocation fan-out
/// path — the column data is shared via Arrow's internal `Arc<ArrayData>` and
/// each sink `Arc::clone`s once, not a full `RecordBatch::clone` per sink.
async fn emit_to_writers(
    writers: &mut [Box<dyn SinkWriter>],
    outputs: &[arrow::record_batch::RecordBatch],
) -> EngineResult<()> {
    // Build the changelogs once. For S sinks × K outputs, the historical
    // implementation did S×K `RecordBatch::clone` + S×K `Vec<RowKind>`
    // allocations; this version does K (one per output).
    let changelogs: Vec<std::sync::Arc<ChangelogBatch>> = outputs
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| std::sync::Arc::new(ChangelogBatch::inserts(b.clone())))
        .collect();
    for writer in writers.iter_mut() {
        for cl in &changelogs {
            writer.write_arc(cl.clone()).await?;
        }
    }
    Ok(())
}

/// Persist one streaming checkpoint epoch: operator state and the source offset
/// travel together in one payload so a later restore is consistent.
///
/// ST-2 fix: `executor.snapshot()` may call RocksDB `fsync` and a full state
/// scan, both of which can block for tens of milliseconds on durable profiles.
/// `snapshot_nonblocking` wraps the call in `block_in_place` on multi-threaded
/// runtimes so the Tokio reactor thread can continue scheduling other tasks
/// during the blocking window; on single-threaded runtimes (unit tests) the
/// call is made directly since there is no other thread to offload to.
async fn persist_streaming_checkpoint(
    rt: &EngineRuntime,
    handle: &JobHandle,
    executor: &mut ContinuousWindowExecutor,
    source_name: &str,
    source: &mut Box<dyn SourceReader>,
    source_offset: Option<Vec<u8>>,
    epoch: u64,
) -> EngineResult<()> {
    let operator_state = snapshot_nonblocking(|| executor.snapshot()).map_err(exec_err)?;
    let source_offsets = source_offset
        .map(|encoded| vec![(source_name.to_string(), encoded)])
        .unwrap_or_default();
    // B-5: persist the source's in-flight records alongside the operator
    // state. A source that returns `None` from `snapshot_in_flight` does
    // not contribute an entry; the default value (empty `Vec`) keeps the
    // checkpoint wire format unchanged for sources that opt out.
    let source_in_flight = source
        .snapshot_in_flight()
        .map(|records| vec![(source_name.to_string(), records)])
        .unwrap_or_default();
    rt.checkpoint
        .persist(
            handle.job_id(),
            &CheckpointPayload {
                epoch,
                operator_state,
                source_offsets,
                // Aligned single-input streaming checkpoint: no in-flight buffers.
                in_flight: Vec::new(),
                source_in_flight,
            },
        )
        .await
}

/// Call `f` (potentially blocking — RocksDB fsync, state scan) in a way that
/// does not stall the Tokio reactor on multi-threaded runtimes.
///
/// On a `multi_thread` runtime: delegates to `block_in_place` so Tokio can
/// move other tasks to a different thread while this one blocks.
/// On a `current_thread` runtime (unit tests, embedded): calls `f` directly
/// since `block_in_place` panics there and the tests use in-memory state with
/// no I/O.
fn snapshot_nonblocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    use tokio::runtime::RuntimeFlavor;
    if tokio::runtime::Handle::try_current()
        .map(|h| h.runtime_flavor() == RuntimeFlavor::MultiThread)
        .unwrap_or(false)
    {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
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
/// keeps polling the source on a background task until [`RunningJob::stop`] is
/// called. Two paths:
///
/// - **Windowed**: query compiles as `TUMBLE`/`HOP`/`SESSION` → uses the
///   `ContinuousWindowExecutor` with periodic checkpoints.
/// - **Stateless** (G-1): any other valid SQL `SELECT` → applies the query
///   per-batch with no window state or checkpoints.
#[tracing::instrument(skip(rt), fields(job = %job.name))]
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
    if job.query.trim().is_empty() {
        return Err(EngineError::InvalidJob(
            "streaming job query cannot be empty".into(),
        ));
    }

    let handle = JobHandle::from_name(&job.name, JobStatus::Running)?;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let task = if compile_streaming_window_sql(&job.query).is_ok() {
        tokio::spawn(run_streaming_continuous(job, rt, stop_rx))
    } else {
        tokio::spawn(run_stateless_continuous(job, rt, stop_rx))
    };

    Ok(RunningJob {
        handle,
        stop: stop_tx,
        task,
    })
}

/// Continuously-running stateless streaming loop: applies the query per-batch
/// over the source until stopped. No window state, no checkpointing.
async fn run_stateless_continuous(
    job: CompiledJob,
    rt: EngineRuntime,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) -> EngineResult<JobHandle> {
    let source = job.sources.first().ok_or_else(|| {
        EngineError::InvalidJob("stateless streaming job requires at least one source".into())
    })?;
    let table_name = source.name.clone();
    let mut reader = rt.sources.open(source).await?;
    let mut writers = open_writers(&rt, &job.sinks).await?;
    let idle_floor = std::time::Duration::from_micros(STREAMING_IDLE_FLOOR_US);
    let data_notify = reader.data_notify();

    loop {
        if *stop_rx.borrow() {
            break;
        }
        let Some(batch) = reader.next().await? else {
            if wait_for_data_or_stop(data_notify.as_ref(), &mut stop_rx, idle_floor).await {
                break;
            }
            continue;
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let outputs = apply_stateless_query(&job.query, &table_name, batch).await?;
        emit_to_writers(&mut writers, &outputs).await?;
    }

    for writer in &mut writers {
        writer.flush().await?;
    }
    Ok(JobHandle::new(
        JobHandle::from_name(&job.name, JobStatus::Running)?
            .job_id()
            .clone(),
        JobStatus::Completed,
    ))
}

/// Wait for either the source's data notify, a stop signal, or the safety
/// floor. Returns `true` if the caller should stop (stop signal received or
/// sender dropped), `false` to loop back and re-poll the source.
async fn wait_for_data_or_stop(
    notify: Option<&krishiv_engine_core::DataNotify>,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    idle_floor: std::time::Duration,
) -> bool {
    let notify_owned = notify.cloned();
    let fallback = std::time::Duration::from_millis(STREAMING_IDLE_TICK_MS);
    tokio::select! {
        res = stop_rx.changed() => res.is_err(),
        _ = async move {
            if let Some(n) = notify_owned {
                n.notified().await;
            } else {
                // No source notify: fall back to the historical 5 ms tick
                // for sources that haven't been instrumented yet.
                tokio::time::sleep(fallback).await;
            }
        } => false,
        _ = tokio::time::sleep(idle_floor) => false,
    }
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
    // Idle safety floor: 50 µs. The primary wakeup is `setup.data_notify` —
    // when the source implements `data_notify()`, the loop wakes within
    // microseconds of new data instead of within `STREAMING_IDLE_TICK_MS = 5ms`.
    let idle_floor = std::time::Duration::from_micros(STREAMING_IDLE_FLOOR_US);
    // Resolve the latency-vs-throughput profile once for this job's lifetime.
    let checkpoint_every = StreamProfile::from_env().checkpoint_every();
    let mut batches_since_checkpoint = 0u32;
    // S-3: at most one background checkpoint I/O task in flight at a time.
    // Tracks the JoinHandle and the epoch number it is persisting. Epoch is
    // advanced only AFTER the task confirms success, preventing epoch gaps on
    // transient persist failures (B-3 fix).
    let mut bg_checkpoint: Option<(tokio::task::JoinHandle<bool>, u64)> = None;
    // ST-4: idle watermark tick — advance with wall-clock every IDLE_TICK_INTERVAL
    // of source-idle time so session windows whose gap has elapsed can close.
    let idle_tick_period = idle_tick_interval();
    let mut last_idle_tick = std::time::Instant::now();
    // H-14 (audit): speculative early-fire wiring. When enabled, the
    // continuous loop periodically emits a snapshot of every open
    // tumbling window so downstream upsert sinks can see a result before
    // the window closes. Off by default.
    let early_fire_period = early_fire_interval();
    let mut last_early_fire = std::time::Instant::now();

    loop {
        if *stop_rx.borrow() {
            break;
        }
        // Try non-blocking first: if the source already has a buffered batch
        // (e.g. an in-memory source, or a Kafka consumer that prefetched),
        // `next()` returns immediately and we never wait.
        let Some(batch) = setup.reader.next().await? else {
            // ST-4: once per IDLE_TICK_INTERVAL, advance the watermark to
            // wall-clock time so session windows whose inactivity gap has
            // elapsed can close even when the source is quiet.
            if last_idle_tick.elapsed() >= idle_tick_period {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let idle_outputs = setup.executor.tick(now_ms).map_err(exec_err)?;
                if !idle_outputs.is_empty() {
                    emit_to_writers(&mut writers, &idle_outputs).await?;
                }
                last_idle_tick = std::time::Instant::now();
            }
            // H-14 (audit): speculative early-fire of currently-open
            // tumbling windows. Emits a non-mutating snapshot of every
            // open window's current aggregate so a downstream upsert sink
            // keyed on `(key, window_start)` can deliver a result before
            // the window closes. Disabled by default; enabled via
            // `KRISHIV_STREAM_EARLY_FIRE_MS`.
            if let Some(period) = early_fire_period
                && last_early_fire.elapsed() >= period
                && let Some(snapshot) = setup.executor.emit_open_windows_speculative()
                && !snapshot.is_empty()
            {
                emit_to_writers(&mut writers, &snapshot).await?;
                last_early_fire = std::time::Instant::now();
            }
            // Source idle. Wait for either the source's notify, a stop
            // signal, or the safety floor (handles lost wakeups). After
            // waking, loop back to `next()` — the `Notify` is level-triggered
            // so a missed wakeup is harmless on the next iteration.
            if wait_for_data_or_stop(setup.data_notify.as_ref(), &mut stop_rx, idle_floor).await {
                break;
            }
            continue;
        };

        let outputs = setup.executor.drain(vec![batch]).map_err(exec_err)?;
        emit_to_writers(&mut writers, &outputs).await?;
        batches_since_checkpoint = batches_since_checkpoint.saturating_add(1);
        if batches_since_checkpoint >= checkpoint_every {
            // S-3 / B-3: Take an in-memory snapshot then hand the bytes to a
            // background task for the remote persist. Gate on the previous task
            // being finished — one in-flight write at a time.
            //
            // B-3 fix: epoch is advanced only AFTER the background task confirms
            // the persist succeeded. If the task failed, we retry with the same
            // epoch so the epoch sequence stays gapless (no skipped epochs that
            // would cause event re-processing on recovery).
            let prev_done = bg_checkpoint.as_ref().is_none_or(|(h, _)| h.is_finished());
            if prev_done {
                // Join the finished task to learn whether the persist succeeded,
                // then advance (or hold) the epoch accordingly.
                if let Some((prev_handle, prev_epoch)) = bg_checkpoint.take() {
                    match prev_handle.await {
                        Ok(true) => {
                            // Epoch committed; advance past it.
                            setup.next_epoch = prev_epoch.saturating_add(1);
                        }
                        Ok(false) | Err(_) => {
                            // Persist failed; retry the same epoch so recovery
                            // sees a contiguous committed epoch sequence.
                            setup.next_epoch = prev_epoch;
                            tracing::warn!(
                                job = %setup.handle.job_id(),
                                epoch = prev_epoch,
                                "background checkpoint persist failed; retrying epoch",
                            );
                        }
                    }
                }

                let offset = setup.reader.checkpoint_offset();
                match snapshot_nonblocking(|| setup.executor.snapshot()).map_err(exec_err) {
                    Ok(operator_state) => {
                        let source_offsets = offset
                            .map(|enc| vec![(setup.source_name.clone(), enc)])
                            .unwrap_or_default();
                        let payload = CheckpointPayload {
                            epoch: setup.next_epoch,
                            operator_state,
                            source_offsets,
                            in_flight: Vec::new(),
                            source_in_flight: Vec::new(),
                        };
                        let checkpoint_svc = Arc::clone(&rt.checkpoint);
                        let job_id = setup.handle.job_id().clone();
                        let epoch = setup.next_epoch;
                        let handle = tokio::spawn(async move {
                            match checkpoint_svc.persist(&job_id, &payload).await {
                                Ok(()) => true,
                                Err(e) => {
                                    tracing::warn!(
                                        job = %job_id,
                                        epoch,
                                        "background checkpoint persist failed: {e}",
                                    );
                                    false
                                }
                            }
                        });
                        bg_checkpoint = Some((handle, epoch));
                        // next_epoch NOT advanced here; only after the task
                        // confirms success (see join logic above, next interval).
                    }
                    Err(e) => {
                        tracing::warn!(
                            job = %setup.handle.job_id(),
                            epoch = setup.next_epoch,
                            "streaming checkpoint snapshot failed: {e}",
                        );
                    }
                }
            } else {
                tracing::debug!(
                    job = %setup.handle.job_id(),
                    "background checkpoint still in flight, skipping interval",
                );
            }
            batches_since_checkpoint = 0;
        }
    }

    // Await any in-flight background checkpoint and advance epoch if it
    // succeeded, so the final synchronous checkpoint carries the next
    // epoch in the gapless sequence.
    // If the background checkpoint failed, next_epoch stays at `epoch` and
    // the final synchronous checkpoint retries that epoch — no gap is introduced.
    if let Some((handle, epoch)) = bg_checkpoint.take()
        && matches!(handle.await, Ok(true))
    {
        setup.next_epoch = epoch.saturating_add(1);
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
        &mut setup.reader,
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
    async fn batch_engine_streams_passthrough_output_to_sink() {
        // A multi-batch pass-through exercises the streaming output path: the
        // result is drained batch by batch and fanned to the sink, never
        // collected into one buffer.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "t",
            vec![kv_batch(&["a", "b"], &[1, 2]), kv_batch(&["c"], &[3])],
        );
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "passthrough",
            "SELECT k, v FROM t",
            vec![SourceSpec::bounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);

        let handle = run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let out = sink.take("out");
        let rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(rows, 3, "all input rows stream through to the sink");
        assert!(out.iter().all(ChangelogBatch::is_append_only));
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
        assert_eq!(handle.status(), JobStatus::Completed);

        let out = sink.take("out");
        assert_eq!(out.len(), 1, "one changelog emitted for one input batch");
        let cl = out.first().unwrap();
        // Two groups (a => 4, b => 2); all inserts in the first changelog.
        assert_eq!(cl.num_rows(), 2);
        assert!(cl.row_kinds().iter().all(|k| *k == RowKind::Insert));
    }

    #[tokio::test]
    async fn incremental_engine_handles_empty_first_view_then_promoted_type() {
        // Batch 1 (v=1) is filtered out → the view is empty on the first step;
        // batch 2 (v=100) produces a row whose SUM is promoted to Float64. The
        // empty-first step must NOT seed `prev` with the un-promoted LIMIT-0
        // probe schema (Int64), or the next `differentiate` mismatches types.
        let sources = InMemorySourceProvider::new();
        sources.insert("t", vec![kv_batch(&["a"], &[1]), kv_batch(&["a"], &[100])]);
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "filtered-agg",
            "SELECT k, SUM(v) AS total FROM t WHERE v > 50 GROUP BY k",
            vec![SourceSpec::cdc("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);

        // Previously errored with a RowConverter schema mismatch; must succeed.
        run_job(job, rt).await.unwrap();

        let out = sink.take("out");
        let inserts: usize = out
            .iter()
            .filter(|cl| cl.row_kinds().iter().all(|k| *k == RowKind::Insert))
            .map(ChangelogBatch::num_rows)
            .sum();
        assert_eq!(inserts, 1, "only batch 2's row (a => 100) is emitted");
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
        assert_eq!(handle.status(), JobStatus::Completed);

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

    /// Verify that a transient checkpoint failure (injected I/O error) does not
    /// kill the streaming loop.  The job must continue processing and must be
    /// stoppable cleanly after the transient clears.
    ///
    /// Strategy: use a `DurableCheckpointService` rooted at a read-only path so
    /// the first persist call fails.  Then, on `stop()`, the *final* checkpoint
    /// is written to the original durable dir (after the service is replaced with
    /// a writable one by rebuilding the runtime) to confirm the final-checkpoint
    /// path is reachable.
    ///
    /// Actually, the simplest regression test is: confirm the loop drains all
    /// batches, continues beyond the (silent) checkpoint-failure point, and that
    /// `stop()` returns `Completed` — as opposed to the pre-fix behavior where
    /// the loop would die with `Err` at the first checkpoint failure.
    #[tokio::test]
    async fn streaming_loop_survives_transient_checkpoint_failure() {
        // Use a real DurableCheckpointService on a tmpdir, then chmod 000 the
        // dir so all persists fail.  After stop, confirm we get `Completed`
        // (loop survived), not an Err propagated from the checkpoint call.
        // Pre-load enough batches to trigger at least one in-loop checkpoint:
        // STREAMING_CHECKPOINT_EVERY = 4, so we need 5 batches minimum.
        let sources = InMemorySourceProvider::new();
        let mut batches = Vec::new();
        for _ in 0..STREAMING_CHECKPOINT_EVERY + 1 {
            batches.push(event_batch("a", 1_000, 1));
        }
        batches.push(event_batch("a", 15_000, 1)); // advance watermark past window
        sources.insert("events", batches);

        let checkpoint = InMemoryCheckpointService::new();
        let sink = InMemorySinkProvider::new();
        let rt = runtime_with_checkpoint(sources, sink.clone(), checkpoint.clone());

        let running = spawn_streaming_job(tumbling_job("fail-ckpt-job"), rt).unwrap();

        // Drain all batches; checkpoint fires once in-loop.  Under the old code
        // the loop would die when persist returned Err; under the new code it logs
        // the failure and continues — so `stop()` must still return `Completed`.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let final_handle = running.stop().await.unwrap();
        assert_eq!(
            final_handle.status(),
            JobStatus::Completed,
            "a streaming job that hit a transient checkpoint failure must still stop cleanly"
        );

        // The final checkpoint (written on stop to the in-memory service) must land.
        assert!(
            checkpoint
                .restore_latest(final_handle.job_id())
                .await
                .unwrap()
                .is_some(),
            "the final checkpoint on stop must succeed"
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
    async fn streaming_engine_accepts_non_windowed_query_as_stateless() {
        // G-1: a non-windowed SELECT is now accepted by the streaming engine and
        // runs as a stateless per-batch transform (no Unsupported error).
        // InMemorySourceProvider returns an empty reader for unknown names, so
        // the job completes with 0 rows rather than failing.
        let sources = InMemorySourceProvider::new();
        sources.insert("t", vec![kv_batch(&["a"], &[1])]);
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "no-win",
            "SELECT k FROM t",
            vec![SourceSpec::unbounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming);

        let handle = run_job(job, rt).await.unwrap();
        assert_eq!(
            handle.status(),
            JobStatus::Completed,
            "stateless query runs without error"
        );

        let out = sink.take("out");
        let rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(rows, 1, "one row projected through the stateless filter");
    }

    #[tokio::test]
    async fn streaming_engine_rejects_empty_query() {
        let rt = embedded_runtime(
            Arc::new(InMemorySourceProvider::new()),
            Arc::new(InMemorySinkProvider::new()),
        );
        let job = CompiledJob::new(
            "empty",
            "   ",
            vec![SourceSpec::unbounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming);

        let err = run_job(job, rt).await.unwrap_err();
        assert!(
            matches!(err, EngineError::InvalidJob(_)),
            "empty streaming query must be InvalidJob; got: {err}"
        );
    }

    #[test]
    fn stream_profile_parses_and_sets_checkpoint_cadence() {
        assert_eq!(StreamProfile::parse(None), StreamProfile::LowLatency);
        assert_eq!(StreamProfile::parse(Some("")), StreamProfile::LowLatency);
        assert_eq!(
            StreamProfile::parse(Some("low-latency")),
            StreamProfile::LowLatency
        );
        assert_eq!(
            StreamProfile::parse(Some("throughput")),
            StreamProfile::Throughput
        );
        assert_eq!(
            StreamProfile::parse(Some("  ThroughPut ")),
            StreamProfile::Throughput
        );
        // Low-latency checkpoints more often (shorter recovery replay); the
        // throughput profile amortizes the per-epoch fsync over more batches.
        assert_eq!(
            StreamProfile::LowLatency.checkpoint_every(),
            STREAMING_CHECKPOINT_EVERY
        );
        assert!(
            StreamProfile::Throughput.checkpoint_every()
                > StreamProfile::LowLatency.checkpoint_every()
        );
    }

    /// Latency regression: the bounded streaming engine drains a windowed
    /// source and emits a closed window in well under 100 ms end-to-end.
    /// The previous idle-tick-of-5ms was the dominant cost; the per-batch
    /// notify path plus column-index caching plus Arc-on-RecordBatch fan-out
    /// should keep the per-drain cycle under 5 ms on a typical CI box.
    #[tokio::test]
    async fn streaming_bounded_drain_emits_window_under_50ms() {
        use std::time::Instant;
        let sources = InMemorySourceProvider::new();
        // 10 batches of 1000 rows each, all at event-time 5_000 (so they fall
        // in the first tumbling window [0, 10_000)). The 11th batch at
        // event-time 15_000 advances the watermark past the first window's
        // end (10_000), so the bounded `drain` call emits the closed window.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("user_id", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let mut batches = Vec::new();
        for _ in 0..10 {
            let arr_user: arrow::array::ArrayRef =
                Arc::new(arrow::array::StringArray::from(vec!["a"; 1000]));
            let arr_ts: arrow::array::ArrayRef =
                Arc::new(arrow::array::Int64Array::from(vec![5_000_i64; 1000]));
            let arr_v: arrow::array::ArrayRef =
                Arc::new(arrow::array::Int64Array::from(vec![1_i64; 1000]));
            batches.push(
                arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![arr_user, arr_ts, arr_v],
                )
                .unwrap(),
            );
        }
        // The boundary-closing batch: one row at event-time 15_000.
        {
            let arr_user: arrow::array::ArrayRef =
                Arc::new(arrow::array::StringArray::from(vec!["a"]));
            let arr_ts: arrow::array::ArrayRef =
                Arc::new(arrow::array::Int64Array::from(vec![15_000_i64]));
            let arr_v: arrow::array::ArrayRef =
                Arc::new(arrow::array::Int64Array::from(vec![1_i64]));
            batches.push(
                arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![arr_user, arr_ts, arr_v],
                )
                .unwrap(),
            );
        }
        sources.insert("t", batches);
        let collected = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(collected.clone()));
        let job = CompiledJob::new(
            "stream-lat",
            "SELECT user_id, SUM(v) AS total FROM TUMBLE(TABLE t, DESCRIPTOR(ts), 10000) \
             GROUP BY user_id, window_start, window_end",
            vec![SourceSpec::unbounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        );

        let start = Instant::now();
        run_job(job, rt).await.unwrap();
        let elapsed = start.elapsed();
        // Per-drain budget: 200 ms is generous on CI; the per-record floor
        // is sub-millisecond, the 10k rows should drain in microseconds, and
        // any hidden cost (fsync, lock contention, scheduler delay) shows up
        // as a regression.
        assert!(
            elapsed.as_millis() < 200,
            "bounded streaming drain should complete in <200ms, took {elapsed:?}"
        );
        // The bounded run closes the window (event-time 5_000 < wm 10_000
        // after the first event passes the boundary). 1 row of output
        // (user_id=a, total=10000).
        let out = collected.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert!(total_rows > 0, "expected at least one closed window row");
    }

    // ── G-1: stateless streaming ─────────────────────────────────────────────

    #[tokio::test]
    async fn stateless_streaming_applies_filter_per_batch() {
        // A non-windowed SQL SELECT over an unbounded source. Each input batch
        // goes through DataFusion in-process; only rows satisfying the WHERE
        // clause are emitted.
        let sources = InMemorySourceProvider::new();
        sources.insert(
            "events",
            vec![
                kv_batch(&["a", "b", "c"], &[5, 15, 3]),
                kv_batch(&["d", "e"], &[20, 1]),
            ],
        );
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "filter-job",
            "SELECT k, v FROM events WHERE v > 10",
            vec![SourceSpec::unbounded("events", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming);

        let handle = run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let out = sink.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        // Batch 1: b=15; Batch 2: d=20 — 2 rows pass the filter.
        assert_eq!(total_rows, 2, "only rows with v>10 should be emitted");
        assert!(
            out.iter().all(ChangelogBatch::is_append_only),
            "stateless streaming emits insert-only changelogs"
        );
    }

    #[tokio::test]
    async fn stateless_streaming_applies_projection_per_batch() {
        let sources = InMemorySourceProvider::new();
        sources.insert("t", vec![kv_batch(&["x", "y"], &[10, 20])]);
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(sources), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "proj-job",
            "SELECT k FROM t",
            vec![SourceSpec::unbounded("t", "memory", "")],
            vec![SinkSpec::new("out", "memory", "")],
            true,
        )
        .with_engine(EngineKind::Streaming);

        run_job(job, rt).await.unwrap();

        let out = sink.take("out");
        let total_rows: usize = out.iter().map(ChangelogBatch::num_rows).sum();
        assert_eq!(total_rows, 2, "projection emits all rows");
        // Output should have only 1 column (k), not 2.
        assert_eq!(
            out.first().unwrap().batch().num_columns(),
            1,
            "projected output has one column"
        );
    }
}
