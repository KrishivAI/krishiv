//! Connector-backed [`EngineRuntime`] services for the **embedded** placement.
//!
//! [`Session::submit`](crate::Session::submit) compiles every front-end down to a
//! [`CompiledJob`](crate::CompiledJob) and runs it through
//! [`run_job`](crate::run_job). That dispatch needs placement-provided sources
//! and sinks; this module supplies them by binding the engine-core
//! [`SourceProvider`]/[`SinkProvider`] traits to the real `krishiv-connectors`
//! file connectors.
//!
//! Sources and sinks are connector-backed in every placement. The placement
//! difference is the query executor: embedded runs the batch query in-process
//! (no executor), while [`runtime_backed_engine_runtime`] injects a
//! [`RuntimeQueryExecutor`] so single-node / distributed batch jobs run through
//! the real `ExecutionRuntime` (in-process cluster or remote coordinator). The
//! engine code does not change — only the injected service does.
//!
//! Wired file/object-store connectors: `parquet`, `parquet-directory`, `csv`,
//! `json` (NDJSON), `s3`, and `s3-prefix` for job sources; `parquet`, `csv`,
//! `json` (NDJSON), and `s3` for sinks. Other connector kinds return a typed
//! error pointing at the per-connector follow-up; this mirrors the SQL DDL
//! `connector_factory` path. Off-engine batch execution keeps compute on the
//! configured single-node/distributed runtime: native parquet sources are
//! registered directly, while bounded non-parquet sources are drained locally
//! and spilled to temporary parquet registrations that the runtime ships inline.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use krishiv_connectors::cdc::{CdcEvent, CdcOp, parse_debezium_envelope};
use krishiv_connectors::parquet::{ParquetDirectorySource, ParquetSink, ParquetSource};
use krishiv_connectors::{ConnectorConfig, DynSink, DynSource, default_registry};
use krishiv_engine_core::mem::embedded_runtime;
use krishiv_engine_core::{
    BatchOutputStream, ChangelogBatch, CompiledJob, ConsolidatingSinkProvider,
    DurableCheckpointService, EngineError, EngineResult, EngineRuntime, JobHandle, JobStatus,
    Placement, QueryExecutor, RowKind, SinkProvider, SinkSpec, SinkWriter, SourceProvider,
    SourceReader, SourceSpec, UpsertSinkProvider,
};
use krishiv_runtime::{BatchTableRegistration, ExecutionRuntime};

const MAX_CONNECTOR_DRAIN_BYTES: usize = 2 * 1024 * 1024 * 1024;
const MAX_BATCH_RESULT_BYTES: usize = 2 * 1024 * 1024 * 1024;
static SPILL_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Run a bounded **streaming** job through an [`ExecutionRuntime`]'s continuous
/// seam — the distributed-stateful path behind the unified `Session::submit`.
///
/// I/O stays local (connector source drain + sink write); the windowed
/// computation runs on the runtime: in-process for embedded/single-node, and on
/// the remote coordinator's executors in distributed mode (the
/// `register_continuous_stream` → `push` → `drain` Flight seam). This is the
/// honest run-once shape — drain the bounded source, push one input cycle, drain
/// the closed windows — so it composes with the same `ContinuousWindowExecutor`
/// the dedicated streaming API uses, just reached through `submit`.
pub async fn run_streaming_job_via_runtime(
    runtime: &Arc<dyn ExecutionRuntime>,
    job: &CompiledJob,
) -> EngineResult<JobHandle> {
    use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

    let plan = compile_streaming_window_sql(&job.query).map_err(|e| EngineError::Unsupported {
        engine: krishiv_engine_core::EngineKind::Streaming,
        reason: e.to_string(),
    })?;
    let local_spec = krishiv_runtime::plan_spec_to_local(&plan.spec);

    // Drain the bounded source(s) locally via the file connectors.
    let source_provider = ConnectorSourceProvider;
    let mut input = Vec::new();
    for spec in &job.sources {
        let mut reader = source_provider.open(spec).await?;
        while let Some(batch) = reader.next().await? {
            input.push(batch);
        }
    }

    // Run the window on the runtime: in-process locally, on executors remotely.
    runtime
        .register_continuous_stream(&job.name, &local_spec)
        .map_err(|e| EngineError::Runtime(e.to_string()))?;
    runtime
        .push_continuous_stream_input(&job.name, input)
        .map_err(|e| EngineError::Runtime(e.to_string()))?;
    let outputs = runtime
        .drain_continuous_stream(&job.name)
        .map_err(|e| EngineError::Runtime(e.to_string()))?;

    // Write the closed windows to the job's sink(s). Streaming output is
    // insert-only, so no consolidation is needed.
    let sink_provider = connector_sink_provider(false);
    for spec in &job.sinks {
        let mut writer = sink_provider.open(spec).await?;
        for batch in &outputs {
            if batch.num_rows() > 0 {
                writer.write(ChangelogBatch::inserts(batch.clone())).await?;
            }
        }
        writer.flush().await?;
    }

    // Bounded run-once over the runtime's continuous seam — it returns when the
    // source is drained, so the invocation is Completed.
    JobHandle::from_name(&job.name, JobStatus::Completed)
}

/// Run a bounded **incremental** job through an [`IvmJob`](crate::IvmJob) — the
/// distributed-stateful incremental path behind the unified `Session::submit`.
///
/// `ivm` is mode-aware: embedded in-process, or a remote job on the coordinator
/// in distributed mode (`Session::ivm`). I/O stays local — drain the bounded CDC
/// source via the file connectors, feed each delta and step the view, then write
/// the net materialized snapshot to the sink. The view maintenance runs wherever
/// the `IvmJob` lives, so `submit()` reaches the same engine the dedicated
/// `Session::ivm` API does.
pub async fn run_incremental_job_via_ivm(
    ivm: &crate::IvmJob,
    job: &CompiledJob,
) -> crate::Result<JobHandle> {
    use crate::FeedableJob;
    use krishiv_ivm::IncrementalViewSpec;

    // Drain each source as a CDC changelog via the file connectors.
    let source_provider = ConnectorSourceProvider;
    let mut buffered: Vec<(String, Vec<ChangelogBatch>)> = Vec::new();
    let mut source_schemas: Vec<(String, arrow::datatypes::SchemaRef)> = Vec::new();
    for spec in &job.sources {
        let mut reader = source_provider.open(spec).await?;
        let mut changes = Vec::new();
        while let Some(cl) = reader.next_changelog().await? {
            changes.push(cl);
        }
        if let Some(first) = changes.first() {
            source_schemas.push((spec.name.clone(), first.batch().schema()));
        }
        buffered.push((spec.name.clone(), changes));
    }

    let output_schema = crate::engines::infer_output_schema(&source_schemas, &job.query).await?;

    // Maintain the query as a materialized view named after the job.
    ivm.register_view(IncrementalViewSpec {
        name: job.name.clone(),
        body_sql: job.query.clone(),
        output_schema,
        is_materialized: true,
        is_recursive: false,
        lateness: vec![],
    })
    .await?;

    // Feed each input delta and advance the view.
    for (name, changes) in buffered {
        for cl in changes {
            if cl.num_rows() == 0 {
                continue;
            }
            let delta = crate::engines::delta_from_changelog(&cl)?;
            ivm.feed(&name, &delta).await?;
            ivm.step().await?;
        }
    }

    // Write the net materialized view (insert-only) to the job's sink(s).
    let sink_provider = connector_sink_provider(false);
    for spec in &job.sinks {
        let mut writer = sink_provider.open(spec).await?;
        if let Some(batch) = ivm.snapshot(&job.name).await?
            && batch.num_rows() > 0
        {
            writer.write(ChangelogBatch::inserts(batch)).await?;
        }
        writer.flush().await?;
    }

    // Bounded run-once: the CDC source is drained and the net view written, so
    // the invocation is Completed (continuous distributed-incremental is the
    // dedicated executor-seam follow-up, not this run-once path).
    Ok(JobHandle::from_name(&job.name, JobStatus::Completed)?)
}

/// Build an embedded [`EngineRuntime`] whose sources and sinks are real
/// `krishiv-connectors` file connectors, reusing engine-core's in-memory state,
/// checkpoint, and clock services. No query executor: the batch engine runs the
/// query in-process.
pub fn embedded_connector_runtime() -> EngineRuntime {
    embedded_runtime(
        Arc::new(ConnectorSourceProvider),
        connector_sink_provider(false),
    )
}

/// Like [`embedded_connector_runtime`], but the sink **consolidates** the
/// changelog into its net materialized table before writing. This is the
/// incremental engine's path: append-only file sinks cannot apply retractions,
/// so a [`ConsolidatingSinkProvider`] folds insert/update/delete into the net
/// rows (see its docs). Batch and streaming emit insert-only output and use the
/// plain [`embedded_connector_runtime`].
pub fn embedded_consolidating_runtime() -> EngineRuntime {
    embedded_runtime(
        Arc::new(ConnectorSourceProvider),
        connector_sink_provider(true),
    )
}

/// Build the connector sink provider, optionally wrapped in changelog
/// consolidation for retraction-aware (incremental) output.
fn connector_sink_provider(consolidate: bool) -> Arc<dyn SinkProvider> {
    if consolidate {
        Arc::new(IncrementalSinkProvider {
            base: Arc::new(ConnectorSinkProvider),
        })
    } else {
        Arc::new(ConnectorSinkProvider)
    }
}

/// Sink provider for the **incremental** engine, chosen per sink at `open` time:
/// a primary-key [`UpsertSinkProvider`] when the sink declares one (per-row
/// upsert/delete by key — the merge-on-read connector contract), otherwise a
/// whole-row [`ConsolidatingSinkProvider`]. Both fold the engine's changelog into
/// a net insert-only table the connector can append.
struct IncrementalSinkProvider {
    base: Arc<dyn SinkProvider>,
}

#[async_trait]
impl SinkProvider for IncrementalSinkProvider {
    async fn open(&self, spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        if spec.primary_key.is_some() {
            UpsertSinkProvider::new(Arc::clone(&self.base))
                .open(spec)
                .await
        } else {
            ConsolidatingSinkProvider::new(Arc::clone(&self.base))
                .open(spec)
                .await
        }
    }
}

/// Build a non-embedded [`EngineRuntime`] for `placement`, backed by the given
/// `ExecutionRuntime`. The batch query runs through a [`RuntimeQueryExecutor`]
/// (single-node in-process cluster, or a remote coordinator), while sinks remain
/// connector-backed so output lands the same way as in embedded placement.
pub fn runtime_backed_engine_runtime(
    placement: Placement,
    runtime: Arc<dyn ExecutionRuntime>,
) -> EngineRuntime {
    let mut rt = embedded_runtime(
        Arc::new(ConnectorSourceProvider),
        Arc::new(ConnectorSinkProvider),
    );
    rt.placement = placement;
    rt.query_executor = Some(Arc::new(RuntimeQueryExecutor { runtime }));
    rt
}

/// Build a non-embedded [`EngineRuntime`] for the **stateful** engines
/// (incremental / streaming) at single-node placement.
///
/// These engines do not use the query-executor seam (that is the batch path);
/// they read connector-backed sources and write connector-backed sinks in
/// process, but — unlike the embedded runtime — their checkpoints persist to
/// disk via a [`DurableCheckpointService`] rooted at `checkpoint_dir`, so a
/// job's operator state and source offsets survive a restart. That durability
/// is the single-node daemon's defining difference from embedded; a distributed
/// placement swaps the checkpoint/source/sink services for cluster-backed ones.
///
/// `consolidate` wraps the sink in [`ConsolidatingSinkProvider`] (the incremental
/// engine's retraction-aware path); pass `false` for insert-only streaming output.
pub fn durable_engine_runtime(
    placement: Placement,
    checkpoint_dir: impl AsRef<Path>,
    consolidate: bool,
) -> EngineResult<EngineRuntime> {
    let checkpoint_dir = checkpoint_dir.as_ref();
    let mut rt = embedded_runtime(
        Arc::new(ConnectorSourceProvider),
        connector_sink_provider(consolidate),
    );
    rt.placement = placement;
    rt.checkpoint = Arc::new(DurableCheckpointService::new(checkpoint_dir)?);
    // File-backed window operator state (per-job subdirs) so streaming state
    // survives a restart even between checkpoints — the single-node durable path.
    rt.state_dir = Some(checkpoint_dir.join("window-state"));
    Ok(rt)
}

// ── Query executor (placement seam) ───────────────────────────────────────────

/// A [`QueryExecutor`] that runs the job's query through an
/// [`ExecutionRuntime`] — the single-node / distributed batch path.
///
/// The job's sources are passed to the runtime as path-based table
/// registrations (so a distributed coordinator reads them directly on the
/// cluster) rather than drained into the client process.
pub struct RuntimeQueryExecutor {
    runtime: Arc<dyn ExecutionRuntime>,
}

#[async_trait]
impl QueryExecutor for RuntimeQueryExecutor {
    async fn execute_batch(&self, job: &CompiledJob) -> EngineResult<BatchOutputStream> {
        use futures::StreamExt as _;

        let all_parquet = job.sources.iter().all(|s| s.connector == "parquet");
        if self.runtime.uses_remote_execution() && !all_parquet {
            return self.execute_remote_with_connector_sources(job).await;
        }

        if all_parquet {
            // Fast path: all sources are Parquet — route through the cluster's
            // native ListingTable registration for predicate/projection pushdown.
            // The runtime collects all batches and we wrap them in a stream; a
            // future Flight-streaming upgrade can replace this with a true lazy
            // stream from the coordinator without changing the caller.
            let tables: Vec<BatchTableRegistration> = job
                .sources
                .iter()
                .map(|s| BatchTableRegistration::new(s.name.clone(), PathBuf::from(&s.uri)))
                .collect();
            let batches = self
                .runtime
                .collect_batch_sql_async(&job.query, &tables, false)
                .await
                .map_err(|e| EngineError::Runtime(e.to_string()))?;

            enforce_result_size_limit(&batches, "all-parquet batch query")?;

            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            return Ok(Box::pin(stream));
        }

        // Mixed-connector path: drain non-parquet sources locally via the
        // connector layer, register everything in a local DataFusion context,
        // and run the query as a stream so results flow to sinks batch-by-batch
        // without collecting the full result in memory.
        use datafusion::datasource::MemTable;
        use datafusion::execution::config::SessionConfig;
        use datafusion::prelude::SessionContext;

        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let config = SessionConfig::new()
            .with_target_partitions(parallelism)
            .with_batch_size(65_536)
            .with_repartition_joins(true)
            .with_repartition_aggregations(true);
        let ctx = SessionContext::new_with_config(config);

        for spec in &job.sources {
            if spec.connector == "parquet" {
                ctx.register_parquet(spec.name.as_str(), &spec.uri, Default::default())
                    .await
                    .map_err(|e| {
                        EngineError::Source(format!("register parquet '{}': {e}", spec.name))
                    })?;
            } else {
                let batches = drain_connector_source(spec).await?;
                let schema = batches
                    .first()
                    .ok_or_else(|| {
                        EngineError::Source(format!(
                            "source '{}' (uri: '{}') produced no batches; \
                             the batch engine requires a non-empty source to infer schema",
                            spec.name, spec.connector
                        ))
                    })?
                    .schema();
                let table = MemTable::try_new(schema, vec![batches])
                    .map_err(|e| EngineError::Source(e.to_string()))?;
                ctx.register_table(spec.name.as_str(), Arc::new(table))
                    .map_err(|e| EngineError::Source(e.to_string()))?;
            }
        }

        // Stream the query result: each batch is written to sinks as it arrives
        // without buffering the full output in the client process.
        let df_stream = ctx
            .sql(&job.query)
            .await
            .map_err(|e| EngineError::Runtime(e.to_string()))?
            .execute_stream()
            .await
            .map_err(|e| EngineError::Runtime(e.to_string()))?;
        let mapped = df_stream.map(|r| r.map_err(|e| EngineError::Runtime(e.to_string())));
        Ok(Box::pin(mapped))
    }
}

impl RuntimeQueryExecutor {
    async fn execute_remote_with_connector_sources(
        &self,
        job: &CompiledJob,
    ) -> EngineResult<BatchOutputStream> {
        let mut spilled_tables = Vec::new();
        let mut tables = Vec::new();
        for spec in &job.sources {
            if spec.connector == "parquet" {
                tables.push(BatchTableRegistration::new(
                    spec.name.clone(),
                    PathBuf::from(&spec.uri),
                ));
                continue;
            }

            let batches = drain_connector_source(spec).await?;
            let spilled = spill_source_batches_to_parquet(spec, batches).await?;
            tables.push(spilled.registration.clone());
            spilled_tables.push(spilled);
        }

        let batches = self
            .runtime
            .collect_batch_sql_async(&job.query, &tables, false)
            .await
            .map_err(|e| EngineError::Runtime(e.to_string()))?;
        enforce_result_size_limit(&batches, "remote mixed-connector batch query")?;

        // Keep spilled source directories alive until after the remote runtime
        // has inlined/read the generated parquet registrations.
        drop(spilled_tables);

        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }
}

async fn drain_connector_source(spec: &SourceSpec) -> EngineResult<Vec<RecordBatch>> {
    let provider = ConnectorSourceProvider;
    let mut reader = provider.open(spec).await?;
    let mut batches = Vec::new();
    let mut total_bytes: usize = 0;
    while let Some(batch) = reader.next().await? {
        total_bytes += batch.get_array_memory_size();
        if total_bytes > MAX_CONNECTOR_DRAIN_BYTES {
            return Err(EngineError::Source(format!(
                "source '{}' (connector '{}', uri: '{}') exceeded the 2 GiB in-memory drain \
                 limit; convert to parquet for large datasets or use a connector-native \
                 distributed source provider",
                spec.name, spec.connector, spec.uri
            )));
        }
        batches.push(batch);
    }
    if batches.is_empty() {
        return Err(EngineError::Source(format!(
            "source '{}' (connector '{}', uri: '{}') produced no batches; \
             the batch engine requires a non-empty source to infer schema",
            spec.name, spec.connector, spec.uri
        )));
    }
    Ok(batches)
}

struct SpilledBatchTable {
    registration: BatchTableRegistration,
    dir: PathBuf,
}

impl Drop for SpilledBatchTable {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.dir) {
            tracing::debug!(
                path = %self.dir.display(),
                error = %error,
                "failed to remove temporary connector spill directory"
            );
        }
    }
}

async fn spill_source_batches_to_parquet(
    spec: &SourceSpec,
    batches: Vec<RecordBatch>,
) -> EngineResult<SpilledBatchTable> {
    let source_name = spec.name.clone();
    tokio::task::spawn_blocking(move || {
        spill_source_batches_to_parquet_blocking(source_name, batches)
    })
    .await
    .map_err(|e| EngineError::Source(format!("connector spill task panicked: {e}")))?
}

fn spill_source_batches_to_parquet_blocking(
    source_name: String,
    batches: Vec<RecordBatch>,
) -> EngineResult<SpilledBatchTable> {
    let Some(first) = batches.first() else {
        return Err(EngineError::Source(format!(
            "source '{source_name}' produced no batches to spill"
        )));
    };
    let dir = unique_spill_dir(&source_name)?;
    let path = dir.join("source.parquet");
    let cleanup_dir = dir.clone();
    let result = (|| {
        let file = File::create(&path).map_err(|e| {
            EngineError::Source(format!("create spill parquet '{}': {e}", path.display()))
        })?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, first.schema(), None)
            .map_err(|e| EngineError::Source(format!("create spill parquet writer: {e}")))?;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| EngineError::Source(format!("write spill parquet batch: {e}")))?;
        }
        writer
            .close()
            .map_err(|e| EngineError::Source(format!("close spill parquet writer: {e}")))?;
        Ok(SpilledBatchTable {
            registration: BatchTableRegistration::new(source_name, path),
            dir,
        })
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&cleanup_dir);
    }
    result
}

fn unique_spill_dir(source_name: &str) -> EngineResult<PathBuf> {
    let safe_name = source_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    let counter = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let dir = std::env::temp_dir().join(format!(
        "krishiv-connector-spill-{}-{counter}-{nanos}-{safe_name}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).map_err(|e| {
        EngineError::Source(format!(
            "create connector spill directory '{}': {e}",
            dir.display()
        ))
    })?;
    Ok(dir)
}

fn enforce_result_size_limit(batches: &[RecordBatch], label: &str) -> EngineResult<()> {
    let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if total_bytes > MAX_BATCH_RESULT_BYTES {
        return Err(EngineError::Runtime(format!(
            "{label} result exceeded the 2 GiB in-memory limit ({total_bytes} bytes); \
             use a LIMIT clause, add a WHERE predicate, or write results directly to a sink \
             instead of collecting them"
        )));
    }
    Ok(())
}

// ── Sources ──────────────────────────────────────────────────────────────────

/// Opens connector-backed [`SourceReader`]s from a [`SourceSpec`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectorSourceProvider;

#[async_trait]
impl SourceProvider for ConnectorSourceProvider {
    async fn open(&self, spec: &SourceSpec) -> EngineResult<Box<dyn SourceReader>> {
        match spec.connector.as_str() {
            "parquet" | "parquet-directory" => {
                // `ParquetSource::open` reads the file footer synchronously
                // (blocking `std::fs::File::open` + metadata parse). Offload
                // to the blocking pool so opening a Parquet source doesn't
                // stall the tokio reactor, matching the JSON branch below.
                let spec = spec.clone();
                let connector = spec.connector.clone();
                let inner = tokio::task::spawn_blocking(move || build_dyn_source(&spec))
                    .await
                    .map_err(|e| {
                        EngineError::Source(format!("{connector} source open task panicked: {e}"))
                    })??;
                Ok(Box::new(DynSourceReader { inner }))
            }
            "csv" => {
                // Same concern as Parquet above: `CsvFileSourceReader::open`
                // does a blocking file open + header/schema inference.
                let uri = spec.uri.clone();
                let reader = tokio::task::spawn_blocking(move || CsvFileSourceReader::open(&uri))
                    .await
                    .map_err(|e| {
                        EngineError::Source(format!("csv source open task panicked: {e}"))
                    })??;
                Ok(Box::new(reader))
            }
            "json" | "ndjson" => {
                // `JsonFileSourceReader::open` reads the entire file into
                // memory. Use the async variant so large NDJSON files do
                // not block the tokio reactor on the initial open.
                let bytes = tokio::fs::read(&spec.uri).await.map_err(|e| {
                    EngineError::Source(format!("read json source '{}': {e}", spec.uri))
                })?;
                let inner =
                    krishiv_connectors::csv_json::NdjsonSource::open(bytes, Default::default())
                        .map_err(|e| EngineError::Source(e.to_string()))?;
                Ok(Box::new(JsonFileSourceReader { inner }))
            }
            "s3" | "s3-prefix" => {
                let config = connector_config_from_source_spec(spec);
                let registry = default_registry();
                let inner = registry
                    .open_source(&config)
                    .await
                    .map_err(|e| EngineError::Source(e.to_string()))?;
                Ok(Box::new(DynSourceReader { inner }))
            }
            other => Err(EngineError::Source(format!(
                "connector '{other}' is not available as a job source yet; \
                 supported: parquet, parquet-directory, csv, json, s3, s3-prefix"
            ))),
        }
    }
}

fn build_dyn_source(spec: &SourceSpec) -> EngineResult<Box<dyn DynSource>> {
    match spec.connector.as_str() {
        "parquet" => {
            let source =
                ParquetSource::open(&spec.uri).map_err(|e| EngineError::Source(e.to_string()))?;
            Ok(Box::new(source))
        }
        "parquet-directory" => {
            let recursive = spec
                .options
                .get("recursive")
                .is_some_and(|value| value == "true" || value == "1");
            let source = ParquetDirectorySource::open(&spec.uri, recursive)
                .map_err(|e| EngineError::Source(e.to_string()))?;
            Ok(Box::new(source))
        }
        other => Err(EngineError::Source(format!(
            "connector '{other}' is not available as an embedded job source yet; \
             supported: parquet, parquet-directory"
        ))),
    }
}

/// A bounded CSV file [`SourceReader`] (schema inferred from the file header and
/// first rows). CSV is append-only and offset-free, so it does not checkpoint a
/// source offset (operator state still checkpoints).
struct CsvFileSourceReader {
    inner: krishiv_connectors::csv_json::CsvSource,
}

impl CsvFileSourceReader {
    fn open(uri: &str) -> EngineResult<Self> {
        let file = std::fs::File::open(uri)
            .map_err(|e| EngineError::Source(format!("open csv source '{uri}': {e}")))?;
        let inner = krishiv_connectors::csv_json::CsvSource::open(file, Default::default())
            .map_err(|e| EngineError::Source(e.to_string()))?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl SourceReader for CsvFileSourceReader {
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>> {
        self.inner
            .read_batch()
            .map_err(|e| EngineError::Source(e.to_string()))
    }
}

/// A bounded line-delimited JSON (NDJSON) file [`SourceReader`] (schema inferred).
struct JsonFileSourceReader {
    inner: krishiv_connectors::csv_json::NdjsonSource,
}

#[async_trait]
impl SourceReader for JsonFileSourceReader {
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>> {
        self.inner
            .read_batch()
            .map_err(|e| EngineError::Source(e.to_string()))
    }
}

/// Adapts a `Box<dyn DynSource>` to the engine-core [`SourceReader`] contract.
struct DynSourceReader {
    inner: Box<dyn DynSource>,
}

#[async_trait]
impl SourceReader for DynSourceReader {
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>> {
        self.inner
            .read_batch_dyn()
            .await
            .map_err(|e| EngineError::Source(e.to_string()))
    }

    fn checkpoint_offset(&self) -> Option<Vec<u8>> {
        self.inner.encoded_checkpoint_offset_dyn().ok().flatten()
    }

    fn restore_offset(&mut self, encoded: &[u8]) -> EngineResult<()> {
        self.inner
            .restore_encoded_checkpoint_offset_dyn(encoded)
            .map_err(|e| EngineError::Source(e.to_string()))
    }
}

// ── CDC sources ────────────────────────────────────────────────────────────────

/// A connector-backed **CDC** source: it decodes Debezium JSON change events
/// into [`ChangelogBatch`]es carrying true insert/update/delete semantics, so
/// the incremental engine retracts deleted rows rather than treating every row
/// as an insertion.
///
/// Events are preloaded per source name (the embedded fixture standing in for a
/// live Kafka/Debezium topic); each is parsed with the production
/// [`parse_debezium_envelope`] decoder. A live placement swaps the event store
/// for a real `CdcEventSource` (e.g. the rdkafka-backed source) behind the same
/// [`SourceReader`] contract.
#[derive(Clone, Default)]
pub struct DebeziumCdcSourceProvider {
    events: Arc<Mutex<HashMap<String, Vec<String>>>>,
}

impl DebeziumCdcSourceProvider {
    /// Create an empty CDC provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Preload Debezium JSON envelopes for source `name`, in arrival order.
    pub fn insert(&self, name: impl Into<String>, envelopes: Vec<String>) {
        if let Ok(mut g) = self.events.lock() {
            g.insert(name.into(), envelopes);
        }
    }
}

#[async_trait]
impl SourceProvider for DebeziumCdcSourceProvider {
    async fn open(&self, spec: &SourceSpec) -> EngineResult<Box<dyn SourceReader>> {
        let envelopes = self
            .events
            .lock()
            .map_err(|_| EngineError::Source("cdc event store poisoned".into()))?
            .get(&spec.name)
            .cloned()
            .unwrap_or_default();
        Ok(Box::new(DebeziumCdcSourceReader {
            envelopes,
            cursor: 0,
        }))
    }
}

struct DebeziumCdcSourceReader {
    envelopes: Vec<String>,
    cursor: usize,
}

#[async_trait]
impl SourceReader for DebeziumCdcSourceReader {
    /// The post-image of the next change (`after` for insert/update, empty for
    /// delete), ignoring row kind. CDC consumers use [`next_changelog`](Self::next_changelog).
    async fn next(&mut self) -> EngineResult<Option<RecordBatch>> {
        Ok(self.next_changelog().await?.map(|cl| cl.batch().clone()))
    }

    async fn next_changelog(&mut self) -> EngineResult<Option<ChangelogBatch>> {
        while let Some(payload) = self.envelopes.get(self.cursor) {
            self.cursor = self.cursor.saturating_add(1);
            let event = parse_debezium_envelope(payload, 0, self.cursor as i64)
                .map_err(|e| EngineError::Source(format!("debezium parse error: {e}")))?;
            if let Some(changelog) = changelog_from_cdc_event(&event)? {
                return Ok(Some(changelog));
            }
            // A no-op event (e.g. an empty payload) advances the offset but
            // produces no change — keep scanning for the next real one.
        }
        Ok(None)
    }

    fn checkpoint_offset(&self) -> Option<Vec<u8>> {
        Some((self.cursor as u64).to_le_bytes().to_vec())
    }

    fn restore_offset(&mut self, encoded: &[u8]) -> EngineResult<()> {
        let arr: [u8; 8] = encoded
            .try_into()
            .map_err(|_| EngineError::Source("source offset must be 8 bytes".into()))?;
        self.cursor = usize::try_from(u64::from_le_bytes(arr))
            .map_err(|_| EngineError::Source("source offset exceeds usize".into()))?;
        Ok(())
    }
}

/// Map a parsed [`CdcEvent`] to a [`ChangelogBatch`]:
/// - insert / snapshot-read → `after` rows tagged [`RowKind::Insert`];
/// - delete → `before` rows tagged [`RowKind::Delete`];
/// - update → `before` ([`RowKind::UpdateBefore`]) concatenated with `after`
///   ([`RowKind::UpdateAfter`]) — debezium emits matching schemas for the pair.
///
/// Returns `None` for an event with no usable payload (a no-op).
fn changelog_from_cdc_event(event: &CdcEvent) -> EngineResult<Option<ChangelogBatch>> {
    let tag = |batch: &RecordBatch, kind: RowKind| vec![kind; batch.num_rows()];
    match &event.op {
        CdcOp::Insert | CdcOp::SnapshotRead => match &event.after {
            Some(after) => Ok(Some(ChangelogBatch::new(
                after.clone(),
                tag(after, RowKind::Insert),
            )?)),
            None => Ok(None),
        },
        CdcOp::Delete => match &event.before {
            Some(before) => Ok(Some(ChangelogBatch::new(
                before.clone(),
                tag(before, RowKind::Delete),
            )?)),
            None => Ok(None),
        },
        CdcOp::Update => match (&event.before, &event.after) {
            (Some(before), Some(after)) => {
                let merged = concat_batches(&before.schema(), [before, after])
                    .map_err(|e| EngineError::Source(e.to_string()))?;
                let mut kinds = tag(before, RowKind::UpdateBefore);
                kinds.extend(tag(after, RowKind::UpdateAfter));
                Ok(Some(ChangelogBatch::new(merged, kinds)?))
            }
            (Some(before), None) => Ok(Some(ChangelogBatch::new(
                before.clone(),
                tag(before, RowKind::Delete),
            )?)),
            (None, Some(after)) => Ok(Some(ChangelogBatch::new(
                after.clone(),
                tag(after, RowKind::Insert),
            )?)),
            (None, None) => Ok(None),
        },
        // `CdcOp` is `#[non_exhaustive]`; a new op variant must be handled
        // explicitly rather than silently dropped.
        other => Err(EngineError::Source(format!(
            "unhandled CDC op {other:?}; add changelog mapping for it"
        ))),
    }
}

// ── Sinks ────────────────────────────────────────────────────────────────────

/// Opens connector-backed [`SinkWriter`]s from a [`SinkSpec`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectorSinkProvider;

#[async_trait]
impl SinkProvider for ConnectorSinkProvider {
    async fn open(&self, spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        match spec.connector.as_str() {
            "parquet" => Ok(Box::new(DynSinkWriter {
                inner: build_dyn_sink(spec)?,
                connector: spec.connector.clone(),
            })),
            "csv" => Ok(Box::new(CsvFileSinkWriter::create(&spec.uri)?)),
            "json" | "ndjson" => Ok(Box::new(JsonFileSinkWriter::create(&spec.uri)?)),
            "s3" => {
                let config = connector_config_from_sink_spec(spec);
                let registry = default_registry();
                let inner = registry
                    .open_sink(&config)
                    .await
                    .map_err(|e| EngineError::Sink(e.to_string()))?;
                Ok(Box::new(DynSinkWriter {
                    inner,
                    connector: spec.connector.clone(),
                }))
            }
            other => Err(EngineError::Sink(format!(
                "connector '{other}' is not available as a job sink yet; \
                 supported: parquet, csv, json, s3"
            ))),
        }
    }
}

fn build_dyn_sink(spec: &SinkSpec) -> EngineResult<Box<dyn DynSink>> {
    match spec.connector.as_str() {
        "parquet" => {
            let sink =
                ParquetSink::create(&spec.uri).map_err(|e| EngineError::Sink(e.to_string()))?;
            Ok(Box::new(sink))
        }
        other => Err(EngineError::Sink(format!(
            "connector '{other}' is not available as an embedded job sink yet; \
             supported: parquet"
        ))),
    }
}

fn connector_config_from_source_spec(spec: &SourceSpec) -> ConnectorConfig {
    connector_config_from_spec(&spec.name, &spec.connector, &spec.uri, &spec.options)
}

fn connector_config_from_sink_spec(spec: &SinkSpec) -> ConnectorConfig {
    connector_config_from_spec(&spec.view, &spec.connector, &spec.uri, &spec.options)
}

fn connector_config_from_spec(
    name: &str,
    connector: &str,
    uri: &str,
    options: &BTreeMap<String, String>,
) -> ConnectorConfig {
    let mut config = ConnectorConfig::new(name, connector);
    if !uri.is_empty() {
        let locator_key = match connector {
            "s3" => Some("object_path"),
            "s3-prefix" => Some("prefix"),
            "parquet" | "parquet-directory" | "csv" | "json" | "ndjson" => Some("path"),
            _ => None,
        };
        if let Some(locator_key) = locator_key
            && !options.contains_key(locator_key)
        {
            config = config.with_property(locator_key, uri);
        }
    }
    for (key, value) in options {
        config = config.with_property(key, value);
    }
    config
}

/// Guard: file sinks are append-only, so a changelog carrying retractions is
/// rejected. The incremental engine routes through a consolidating sink, so the
/// changelog reaching here is already insert-only.
fn require_append_only(changes: &ChangelogBatch, kind: &str) -> EngineResult<()> {
    if !changes.is_append_only() {
        return Err(EngineError::Sink(format!(
            "{kind} sink is append-only and received a changelog with retractions \
             (deletes/updates). Route incremental engine output through a \
             ConsolidatingSinkProvider: use `embedded_consolidating_runtime()` for \
             embedded mode or `durable_engine_runtime(..., consolidate: true)` for \
             single-node/distributed."
        )));
    }
    Ok(())
}

/// A CSV file [`SinkWriter`] (header written on the first batch).
struct CsvFileSinkWriter {
    writer: Option<arrow::csv::Writer<std::fs::File>>,
}

impl CsvFileSinkWriter {
    fn create(uri: &str) -> EngineResult<Self> {
        let file = std::fs::File::create(uri)
            .map_err(|e| EngineError::Sink(format!("create csv sink '{uri}': {e}")))?;
        Ok(Self {
            writer: Some(arrow::csv::Writer::new(file)),
        })
    }
}

#[async_trait]
impl SinkWriter for CsvFileSinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        require_append_only(&changes, "csv")?;
        let (batch, _kinds) = changes.into_parts();
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| EngineError::Sink("csv sink already finalized".into()))?;
        writer
            .write(&batch)
            .map_err(|e| EngineError::Sink(e.to_string()))
    }

    async fn flush(&mut self) -> EngineResult<()> {
        use std::io::Write;
        if let Some(writer) = self.writer.take() {
            let mut file = writer.into_inner();
            file.flush().map_err(|e| EngineError::Sink(e.to_string()))?;
        }
        Ok(())
    }
}

/// A line-delimited JSON (NDJSON) file [`SinkWriter`]. `finish()` (on flush)
/// flushes the writer's trailing bytes.
struct JsonFileSinkWriter {
    writer: Option<arrow::json::LineDelimitedWriter<std::fs::File>>,
}

impl JsonFileSinkWriter {
    fn create(uri: &str) -> EngineResult<Self> {
        let file = std::fs::File::create(uri)
            .map_err(|e| EngineError::Sink(format!("create json sink '{uri}': {e}")))?;
        Ok(Self {
            writer: Some(arrow::json::LineDelimitedWriter::new(file)),
        })
    }
}

#[async_trait]
impl SinkWriter for JsonFileSinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        require_append_only(&changes, "json")?;
        let (batch, _kinds) = changes.into_parts();
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| EngineError::Sink("json sink already finalized".into()))?;
        writer
            .write(&batch)
            .map_err(|e| EngineError::Sink(e.to_string()))
    }

    async fn flush(&mut self) -> EngineResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .finish()
                .map_err(|e| EngineError::Sink(e.to_string()))?;
        }
        Ok(())
    }
}

/// Adapts a `Box<dyn DynSink>` to the engine-core [`SinkWriter`] contract.
///
/// File connectors are append-only, so a changelog carrying retractions
/// (`Delete`/`UpdateBefore`) is rejected with a typed error. The incremental
/// engine routes through a [`ConsolidatingSinkProvider`], which folds retractions
/// into the net table so only insert-only output reaches this writer.
struct DynSinkWriter {
    inner: Box<dyn DynSink>,
    connector: String,
}

#[async_trait]
impl SinkWriter for DynSinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        require_append_only(&changes, &self.connector)?;
        let (batch, _kinds) = changes.into_parts();
        self.inner
            .write_batch_dyn(batch)
            .await
            .map_err(|e| EngineError::Sink(e.to_string()))
    }

    async fn flush(&mut self) -> EngineResult<()> {
        self.inner
            .flush_dyn()
            .await
            .map_err(|e| EngineError::Sink(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt as _;
    use krishiv_connectors::parquet::{ParquetSink, ParquetSource};
    use krishiv_connectors::{Sink, Source};
    // The engine-core `JobStatus` is what `JobHandle::status` returns; the
    // crate-root `JobStatus` is `krishiv_runtime`'s, so name it explicitly here.
    use krishiv_engine_core::JobStatus;
    use krishiv_runtime::{
        BatchTableRegistration, ExecutionPlacement, ExecutionReport, ExecutionRuntime,
        LocalWindowExecutionSpec, RuntimeError, RuntimeMode, RuntimeResult,
    };

    use super::{RuntimeQueryExecutor, run_incremental_job_via_ivm, run_streaming_job_via_runtime};
    use crate::{CompiledJob, EngineKind, ExecutionMode, KrishivError, SinkSpec, SourceSpec};

    #[derive(Debug, Clone)]
    struct CapturedRemoteTable {
        table_name: String,
        path: PathBuf,
        rows: usize,
    }

    #[derive(Clone, Default)]
    struct CapturingRemoteRuntime {
        captured_tables: Arc<Mutex<Vec<CapturedRemoteTable>>>,
    }

    impl CapturingRemoteRuntime {
        fn captured_tables(&self) -> Vec<CapturedRemoteTable> {
            self.captured_tables.lock().unwrap().clone()
        }
    }

    impl ExecutionRuntime for CapturingRemoteRuntime {
        fn mode(&self) -> RuntimeMode {
            RuntimeMode::Distributed
        }

        fn placement(&self) -> ExecutionPlacement {
            ExecutionPlacement::RemoteClusterRequired
        }

        fn accept_plan(
            &self,
            _plan: &krishiv_plan::PhysicalPlan,
        ) -> RuntimeResult<ExecutionReport> {
            Err(RuntimeError::unsupported("not used by this test"))
        }

        fn collect_bounded_window(
            &self,
            _topic: &str,
            _input_batches: Vec<RecordBatch>,
            _spec: &LocalWindowExecutionSpec,
        ) -> RuntimeResult<Vec<RecordBatch>> {
            Err(RuntimeError::unsupported("not used by this test"))
        }

        fn collect_batch_sql(
            &self,
            _query: &str,
            _tables: &[BatchTableRegistration],
            _is_streaming: bool,
        ) -> RuntimeResult<Vec<RecordBatch>> {
            Err(RuntimeError::unsupported("use async batch SQL path"))
        }

        fn collect_batch_sql_async<'a>(
            &'a self,
            _query: &'a str,
            tables: &'a [BatchTableRegistration],
            _is_streaming: bool,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = RuntimeResult<Vec<RecordBatch>>> + Send + 'a>,
        > {
            let table_snapshot = tables
                .iter()
                .map(|table| (table.table_name.clone(), table.path.clone()))
                .collect::<Vec<_>>();
            let captured_tables = Arc::clone(&self.captured_tables);
            Box::pin(async move {
                let mut captured = Vec::new();
                for (table_name, path) in table_snapshot {
                    let mut source = ParquetSource::open(&path)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?;
                    let mut rows = 0usize;
                    while let Some(batch) = source
                        .read_batch()
                        .await
                        .map_err(|e| RuntimeError::transport(e.to_string()))?
                    {
                        rows += batch.num_rows();
                    }
                    captured.push(CapturedRemoteTable {
                        table_name,
                        path,
                        rows,
                    });
                }
                *captured_tables.lock().unwrap() = captured;
                Ok(vec![v_batch(&[6])])
            })
        }

        fn explain_sql(&self, _query: &str) -> RuntimeResult<String> {
            Err(RuntimeError::unsupported("not used by this test"))
        }

        fn register_continuous_stream(
            &self,
            _job_id: &str,
            _spec: &LocalWindowExecutionSpec,
        ) -> RuntimeResult<()> {
            Err(RuntimeError::unsupported("not used by this test"))
        }

        fn push_continuous_stream_input(
            &self,
            _job_id: &str,
            _batches: Vec<RecordBatch>,
        ) -> RuntimeResult<()> {
            Err(RuntimeError::unsupported("not used by this test"))
        }

        fn drain_continuous_stream(&self, _job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
            Err(RuntimeError::unsupported("not used by this test"))
        }

        fn flight_url(&self) -> Option<&str> {
            Some("memory://capturing-remote")
        }
    }

    #[tokio::test]
    async fn submit_runs_batch_job_over_csv_connectors() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.csv");
        std::fs::write(&input, "v\n1\n2\n3\n").unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "csv-sum",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "csv", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "csv", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);
        let handle = session.submit(job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let written = std::fs::read_to_string(&output).unwrap();
        assert!(
            written.contains("total"),
            "csv output has the header: {written:?}"
        );
        assert!(written.contains('6'), "SUM(v)=6 in csv output: {written:?}");
    }

    #[tokio::test]
    async fn submit_runs_batch_job_over_json_connectors() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.ndjson");
        let output = dir.path().join("out.ndjson");
        std::fs::write(&input, "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n").unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "json-sum",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "json", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "json", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);
        let handle = session.submit(job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let written = std::fs::read_to_string(&output).unwrap();
        assert!(
            written.contains("\"total\":6"),
            "SUM(v)=6 in ndjson output: {written:?}"
        );
    }

    #[tokio::test]
    async fn submit_runs_batch_job_over_s3_connectors() {
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("object-store");
        std::fs::create_dir_all(&base_path).unwrap();
        let input = base_path.join("input.parquet");
        let output = base_path.join("out.parquet");

        let mut input_sink = ParquetSink::create(&input).unwrap();
        input_sink.write_batch(v_batch(&[1, 2, 3])).await.unwrap();
        input_sink.flush().await.unwrap();

        let source_options = vec![
            ("base_path".to_owned(), base_path.display().to_string()),
            ("object_path".to_owned(), "input.parquet".to_owned()),
        ];
        let sink_options = vec![
            ("base_path".to_owned(), base_path.display().to_string()),
            ("object_path".to_owned(), "out.parquet".to_owned()),
        ];
        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "s3-sum",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "s3", "input.parquet").with_options(source_options)],
            vec![SinkSpec::new("out", "s3", "out.parquet").with_options(sink_options)],
            false,
        );

        let handle = session.submit(job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);
        assert!(output.exists(), "s3 sink should write the object");

        let mut written = ParquetSource::open(&output).unwrap();
        let batch = written.read_batch().await.unwrap().expect("output batch");
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn remote_runtime_spills_csv_sources_to_parquet_registrations() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        std::fs::write(&input, "v\n1\n2\n3\n").unwrap();

        let runtime = CapturingRemoteRuntime::default();
        let executor = RuntimeQueryExecutor {
            runtime: Arc::new(runtime.clone()),
        };
        let job = CompiledJob::new(
            "remote-csv-sum",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "csv", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "json", "")],
            false,
        );

        let mut stream = krishiv_engine_core::QueryExecutor::execute_batch(&executor, &job)
            .await
            .unwrap();
        let mut output_rows = 0usize;
        while let Some(batch) = stream.next().await {
            output_rows += batch.unwrap().num_rows();
        }
        assert_eq!(output_rows, 1);

        let captured = runtime.captured_tables();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].table_name, "t");
        assert_eq!(captured[0].rows, 3);
        assert!(
            !captured[0].path.exists(),
            "temporary spill parquet should be removed after remote dispatch"
        );
    }

    #[tokio::test]
    async fn submit_runs_incremental_over_csv_writes_consolidated_json() {
        // Cross-format: CSV in, NDJSON out, through the incremental engine — the
        // consolidating sink folds the changelog into the net table before the
        // append-only JSON writer sees it.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\nb,2\na,3\n").unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "csv-ivm-json",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "csv", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "json", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        session.submit(job).await.unwrap();

        let written = std::fs::read_to_string(&output).unwrap();
        // a => 1+3 = 4, b => 2. Net table (insert-only) reaches the JSON sink.
        assert!(
            written.contains("\"k\":\"a\""),
            "group a present: {written:?}"
        );
        assert!(
            written.contains("\"k\":\"b\""),
            "group b present: {written:?}"
        );
    }

    #[tokio::test]
    async fn submit_incremental_with_primary_key_upserts_by_key() {
        // Same incremental aggregate, but the sink declares a primary key — so the
        // changelog is applied by key through the UpsertSinkProvider, yielding one
        // net row per key (the merge-on-read / upsert-connector contract).
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\nb,2\na,3\n").unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "csv-ivm-upsert",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "csv", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "json", output.to_str().unwrap()).with_primary_key(["k"])],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        session.submit(job).await.unwrap();

        let written = std::fs::read_to_string(&output).unwrap();
        // Upsert keyed on k: a => 1+3 = 4, b => 2 — exactly one row per key.
        assert!(
            written.contains("\"k\":\"a\"") && written.contains("\"total\":4"),
            "a upserted to 4: {written:?}"
        );
        assert!(
            written.contains("\"k\":\"b\"") && written.contains("\"total\":2"),
            "b is 2: {written:?}"
        );
        let lines = written.lines().filter(|l| !l.trim().is_empty()).count();
        assert_eq!(
            lines, 2,
            "one net row per key, not the intermediate a=1: {written:?}"
        );
    }

    #[tokio::test]
    async fn run_incremental_via_ivm_materializes_through_ivm_job() {
        // The distributed-incremental submit() path: maintain the view through an
        // IvmJob (here the embedded one — the same uniform API the remote/coordinator
        // job exposes), then write the net snapshot to the sink.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\nb,2\na,3\n").unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let ivm = session.ivm("ivm-direct").await.unwrap();
        let job = CompiledJob::new(
            "ivm-direct",
            "SELECT k, SUM(v) AS total FROM t GROUP BY k",
            vec![SourceSpec::cdc("t", "csv", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "json", output.to_str().unwrap())],
            false,
        );
        let handle = run_incremental_job_via_ivm(&ivm, &job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let written = std::fs::read_to_string(&output).unwrap();
        // a => 1+3 = 4, b => 2 — the net materialized view through the IvmJob.
        assert!(
            written.contains("\"k\":\"a\"") && written.contains("\"total\":4"),
            "a=4: {written:?}"
        );
        assert!(
            written.contains("\"k\":\"b\"") && written.contains("\"total\":2"),
            "b=2: {written:?}"
        );
    }

    fn v_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[tokio::test]
    async fn debezium_cdc_source_drives_incremental_retraction() {
        use krishiv_engine_core::RowKind;
        use krishiv_engine_core::mem::{InMemorySinkProvider, embedded_runtime};

        use crate::connector_runtime::DebeziumCdcSourceProvider;

        // A Debezium topic for table "orders": insert id=1, insert id=2,
        // then delete id=1. The incremental view SELECT id FROM orders must
        // retract id=1, leaving id=2.
        let cdc = DebeziumCdcSourceProvider::new();
        cdc.insert(
            "orders",
            vec![
                r#"{"op":"c","before":null,"after":{"id":1},"source":{"table":"orders"}}"#.into(),
                r#"{"op":"c","before":null,"after":{"id":2},"source":{"table":"orders"}}"#.into(),
                r#"{"op":"d","before":{"id":1},"after":null,"source":{"table":"orders"}}"#.into(),
            ],
        );
        let sink = InMemorySinkProvider::new();
        let rt = embedded_runtime(Arc::new(cdc), Arc::new(sink.clone()));

        let job = CompiledJob::new(
            "orders-view",
            "SELECT id FROM orders",
            vec![SourceSpec::cdc("orders", "debezium", "")],
            vec![SinkSpec::new("out", "memory", "")],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        crate::run_job(job, rt).await.unwrap();

        let out = sink.take("out");
        let kinds: Vec<RowKind> = out.iter().flat_map(|cl| cl.row_kinds().to_vec()).collect();
        assert!(
            kinds.contains(&RowKind::Delete),
            "the CDC delete must surface as a view retraction, got {kinds:?}"
        );
    }

    #[tokio::test]
    async fn submit_runs_batch_job_over_parquet_connectors() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");

        // Write the input parquet file the job will read.
        let mut writer = ParquetSink::create(&input).unwrap();
        writer.write_batch(v_batch(&[1, 2, 3])).await.unwrap();
        writer.flush().await.unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "sum-parquet",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "parquet", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "parquet", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);

        let handle = session.submit(job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        // Read the output parquet back and verify the aggregate landed.
        let mut reader = ParquetSource::open(&output).unwrap();
        let out = reader
            .read_batch()
            .await
            .unwrap()
            .expect("one output batch");
        let total = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn submit_runs_incremental_engine_at_single_node() {
        // Single-node placement runs the incremental engine in-process over
        // connector-backed parquet sources/sinks (the batch query path uses the
        // cluster runtime; the stateful engines run locally with durable state).
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.parquet");
        let output = dir.path().join("out.parquet");

        let mut writer = ParquetSink::create(&input).unwrap();
        writer.write_batch(v_batch(&[1, 2, 3])).await.unwrap();
        writer.flush().await.unwrap();

        let session = crate::SessionBuilder::new()
            .with_local_cluster("grpc://127.0.0.1:50051")
            .build()
            .unwrap();
        assert_eq!(session.mode(), ExecutionMode::SingleNode);

        let job = CompiledJob::new(
            "ivm-single-node",
            "SELECT v FROM t",
            vec![SourceSpec::cdc("t", "parquet", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "parquet", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);

        let handle = session.submit(job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        // The first (only) batch is all insertions → append-only view output.
        let mut reader = ParquetSource::open(&output).unwrap();
        let mut rows = 0;
        while let Some(batch) = reader.read_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3, "the materialized view holds all three rows");
    }

    #[tokio::test]
    async fn submit_runs_streaming_engine_at_single_node_with_durable_checkpoint() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let dir = tempfile::tempdir().unwrap();
        let ckpt_dir = dir.path().join("ckpt");
        let input = dir.path().join("events.parquet");
        let output = dir.path().join("win.parquet");

        // Two events: ts 1000 then 12000 close the [0,10000) tumbling window.
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![1000_i64, 12000])),
                Arc::new(Int64Array::from(vec![5_i64, 7])),
            ],
        )
        .unwrap();
        let mut writer = ParquetSink::create(&input).unwrap();
        writer.write_batch(batch).await.unwrap();
        writer.flush().await.unwrap();

        let session = crate::SessionBuilder::new()
            .with_local_cluster("grpc://127.0.0.1:50051")
            .build()
            .unwrap();
        session.set_config("checkpoint_dir", ckpt_dir.to_str().unwrap());

        let job = CompiledJob::new(
            "win-single-node",
            "SELECT user_id, SUM(amount) AS total \
             FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 10000) \
             GROUP BY user_id, window_start, window_end",
            vec![SourceSpec::unbounded(
                "events",
                "parquet",
                input.to_str().unwrap(),
            )],
            vec![SinkSpec::new("out", "parquet", output.to_str().unwrap())],
            true,
        )
        .with_engine(EngineKind::Streaming);
        assert_eq!(job.engine, EngineKind::Streaming);

        let handle = session.submit(job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        // The single-node streaming run persisted a durable checkpoint to disk.
        let ckpt_file = ckpt_dir.join("win-single-node.ckpt");
        assert!(
            ckpt_file.exists(),
            "single-node streaming must persist a durable checkpoint at {ckpt_file:?}"
        );

        // ...and its window operator state is file-backed under a per-job dir,
        // so it survives a restart even between checkpoints (durable state seam).
        let state_dir = ckpt_dir.join("window-state").join("win-single-node");
        assert!(
            state_dir.exists(),
            "single-node streaming window state must be file-backed at {state_dir:?}"
        );
    }

    #[tokio::test]
    async fn submit_surfaces_unsupported_connector_as_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let session = crate::SessionBuilder::new().build().unwrap();
        let job = CompiledJob::new(
            "kafka-src",
            "SELECT v FROM t",
            vec![SourceSpec::bounded("t", "kafka", "topic")],
            vec![SinkSpec::new(
                "out",
                "parquet",
                dir.path().join("out.parquet").to_str().unwrap(),
            )],
            false,
        );

        // The unsupported source connector fails before any sink is opened; the
        // engine Source error maps to `KrishivError::Runtime`.
        let err = session.submit(job).await.unwrap_err();
        assert!(matches!(err, KrishivError::Runtime { .. }));
    }

    #[tokio::test]
    async fn runtime_backed_executor_routes_batch_through_execution_runtime() {
        use krishiv_engine_core::{JobStatus, Placement};

        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("nums.parquet");
        let output = dir.path().join("agg.parquet");

        let mut writer = ParquetSink::create(&input).unwrap();
        writer.write_batch(v_batch(&[4, 5, 6])).await.unwrap();
        writer.flush().await.unwrap();

        // Use an embedded session's real ExecutionRuntime as the executor
        // backend, but drive it through the *placement* seam (SingleNode runtime
        // with a RuntimeQueryExecutor) — proving the batch engine runs unchanged
        // when execution is handed to the runtime instead of in-process DataFusion.
        let session = crate::SessionBuilder::new().build().unwrap();
        let rt = crate::connector_runtime::runtime_backed_engine_runtime(
            Placement::SingleNode,
            session.execution_runtime(),
        );
        assert_eq!(rt.placement, Placement::SingleNode);
        assert!(rt.query_executor.is_some(), "placement injects an executor");

        let job = CompiledJob::new(
            "rt-sum",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "parquet", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "parquet", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);

        let handle = crate::run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let mut reader = ParquetSource::open(&output).unwrap();
        let out = reader
            .read_batch()
            .await
            .unwrap()
            .expect("one output batch");
        let total = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 15);
    }

    #[tokio::test]
    async fn batch_engine_runs_unchanged_at_distributed_placement() {
        use krishiv_engine_core::{JobStatus, Placement};

        // The placement seam: the same batch engine code runs at `Distributed`
        // placement, handing the whole job to the injected query executor. Here
        // the executor is backed by an in-process runtime (standing in for the
        // cluster), proving the engine is placement-agnostic — a remote
        // coordinator swaps only the executor, not the engine. (End-to-end
        // network execution is covered by the daemon-gated integration tests.)
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("d.parquet");
        let output = dir.path().join("d-out.parquet");

        let mut writer = ParquetSink::create(&input).unwrap();
        writer.write_batch(v_batch(&[10, 20, 30])).await.unwrap();
        writer.flush().await.unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let rt = crate::connector_runtime::runtime_backed_engine_runtime(
            Placement::Distributed,
            session.execution_runtime(),
        );
        assert_eq!(rt.placement, Placement::Distributed);
        assert!(
            rt.query_executor.is_some(),
            "distributed placement injects a query executor"
        );

        let job = CompiledJob::new(
            "rt-dist-sum",
            "SELECT SUM(v) AS total FROM t",
            vec![SourceSpec::bounded("t", "parquet", input.to_str().unwrap())],
            vec![SinkSpec::new("out", "parquet", output.to_str().unwrap())],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);

        let handle = crate::run_job(job, rt).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let mut reader = ParquetSource::open(&output).unwrap();
        let out = reader
            .read_batch()
            .await
            .unwrap()
            .expect("one output batch");
        let total = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 60);
    }

    #[tokio::test]
    async fn run_streaming_via_runtime_executes_tumbling_windows() {
        // The distributed-streaming submit() path: drain a bounded source, run the
        // window through the runtime's continuous seam, write closed windows to the
        // sink. Exercised here with an embedded (in-process) runtime — the exact
        // same register/push/drain trait the remote coordinator backend implements,
        // so this validates the orchestration end-to-end without a live cluster.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("events.csv");
        let output = dir.path().join("windows.json");
        // Two windows close once the watermark passes them (the 25000 event):
        //   window [0,10000):  a=10+20=30, b=5
        //   window [10000,20000): a=100, b=200
        std::fs::write(
            &input,
            "user_id,ts,amount\na,1000,10\na,5000,20\nb,6000,5\na,12000,100\nb,13000,200\na,25000,1\n",
        )
        .unwrap();

        let session = crate::SessionBuilder::new().build().unwrap();
        let runtime = session.execution_runtime();
        let job = CompiledJob::new(
            "wins",
            "SELECT user_id, SUM(amount) AS total \
             FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 10000) \
             GROUP BY user_id, window_start, window_end",
            vec![SourceSpec::unbounded(
                "events",
                "csv",
                input.to_str().unwrap(),
            )],
            vec![SinkSpec::new("out", "json", output.to_str().unwrap())],
            true,
        )
        .with_engine(EngineKind::Streaming);

        let handle = run_streaming_job_via_runtime(&runtime, &job).await.unwrap();
        assert_eq!(handle.status(), JobStatus::Completed);

        let written = std::fs::read_to_string(&output).unwrap();
        // Both closed windows landed in the sink with correct per-key sums.
        assert!(written.contains("\"total\":30"), "window0 a=30: {written}");
        assert!(written.contains("\"total\":5"), "window0 b=5: {written}");
        assert!(
            written.contains("\"total\":100"),
            "window1 a=100: {written}"
        );
        assert!(
            written.contains("\"total\":200"),
            "window1 b=200: {written}"
        );
    }
}
