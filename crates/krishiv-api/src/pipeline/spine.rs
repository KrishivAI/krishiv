//! Lowering a declarative [`Pipeline`] onto the engine-core spine.
//!
//! A pipeline is a `source → view → sink` graph; the spine's [`run_job`] takes a
//! single [`CompiledJob`] (one query, source specs, sink specs) plus an
//! [`EngineRuntime`]. A **batch** pipeline with one sink lowers cleanly: the
//! views compose into one query (a multi-view DAG becomes a CTE chain in
//! declaration order), and `Drop` data-quality expectations on the sink's view
//! fold into a trailing `WHERE`. This module detects that case
//! ([`is_spine_lowerable`]) and runs it through the same `run_job` dispatch every
//! other front-end uses ([`run_on_spine`]) — so a declarative batch pipeline
//! reaches the very same [`BatchEngine`](crate::BatchEngine) as a SQL or Rust
//! job, rather than a parallel driver path.
//!
//! What stays on the [`driver`](super::driver): fan-out to several sinks, `Fail`
//! expectations (which must error on violation — not a pure query) or
//! expectations on a view other than the one emitted, and the incremental/stream
//! maintenance loop — semantics the single-query, run-once job model does not
//! express.
//!
//! In particular, **incremental and stream pipelines are deliberately not
//! lowered**, even single-view ones. The driver maintains a named pipeline's IVM
//! job in the session registry so repeated `run()`s feed new input
//! *incrementally* (the documented cross-run persistence contract), whereas the
//! spine's [`IncrementalEngine`](crate::IncrementalEngine) runs a fresh
//! `krishiv_ivm::IncrementalFlow` once per call. Both sit on the **same**
//! `krishiv-ivm` engine core — so engine consistency already holds — but routing
//! a named incremental pipeline through the run-once spine would silently drop
//! its persisted state across runs. Only the stateless batch case (output is a
//! pure function of the current input) lowers without changing semantics.

use std::sync::Arc;

use async_trait::async_trait;
use krishiv_engine_core::mem::{InMemorySourceProvider, embedded_runtime};
use krishiv_engine_core::{
    ChangelogBatch, EngineError, EngineResult, SinkProvider, SinkSpec, SinkWriter, SourceSpec,
};

use super::source::Ingest;
use super::{OnViolation, Pipeline, PipelineMode, ViewDef};
use crate::error::{KrishivError, Result};

/// Whether `pipeline` lowers cleanly onto the spine's single-query job model.
///
/// True for a **batch** pipeline with exactly one sink, whose view (the sink's
/// view) exists, with no CDC sources (CDC ⇒ the incremental maintenance loop the
/// driver owns). A *multi-view* DAG lowers by composing the views as CTEs in
/// declaration order; `Drop` expectations on the sink's view lower by folding
/// their predicates into a `WHERE`. What stays on the driver: streaming/IVM
/// modes, fan-out to several sinks, and `Fail` expectations (which must error on
/// violation — not expressible as a pure query) or expectations on a view other
/// than the one being emitted (the driver applies neither, so lowering them would
/// silently change behavior).
pub(super) fn is_spine_lowerable(pipeline: &Pipeline) -> bool {
    let (Some((sink_view, _)),) = (pipeline.sinks.first(),) else {
        return false;
    };
    let sink_view_exists = pipeline.views.iter().any(|v| v.name == *sink_view);
    let expectations_lowerable = pipeline
        .expectations
        .iter()
        .all(|e| e.on_violation == OnViolation::Drop && e.view == *sink_view);
    pipeline.mode == PipelineMode::Batch
        && pipeline.sinks.len() == 1
        && sink_view_exists
        && expectations_lowerable
        && !pipeline
            .sources
            .iter()
            .any(|(_, ingest)| matches!(ingest, Ingest::Cdc(_)))
}

/// Compose a pipeline's views into a single SQL query that produces `sink_view`'s
/// rows, folding any `Drop` predicates into a trailing `WHERE`.
///
/// A single view with no predicates is emitted verbatim (the original, simplest
/// lowering). Otherwise the views become CTEs in declaration order — so a later
/// view referencing an earlier one by name resolves against its CTE — and the
/// final `SELECT` reads the sink's view, filtered by the conjunction of the
/// `Drop` predicates.
fn compose_query(views: &[ViewDef], sink_view: &str, drop_predicates: &[String]) -> String {
    let where_clause = if drop_predicates.is_empty() {
        String::new()
    } else {
        let conj = drop_predicates
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(" WHERE {conj}")
    };
    if drop_predicates.is_empty()
        && let [single] = views
    {
        return single.sql.clone();
    }
    let ctes = views
        .iter()
        .map(|v| format!("{} AS ({})", v.name, v.sql))
        .collect::<Vec<_>>()
        .join(", ");
    format!("WITH {ctes} SELECT * FROM {sink_view}{where_clause}")
}

/// Run a lowerable pipeline through [`run_job`](crate::run_job).
///
/// Connector sources are drained to batches up front (mirroring the driver's
/// `normalize_sources`), so the whole job runs over an in-memory source provider
/// and the single sink, dispatched by [`CompiledJob`]'s inferred engine.
pub(super) async fn run_on_spine(pipeline: Pipeline) -> Result<()> {
    debug_assert!(is_spine_lowerable(&pipeline));
    let Pipeline {
        name,
        views,
        sources,
        sinks,
        expectations,
        ..
    } = pipeline;

    let (sink_view, egress) = sinks
        .into_iter()
        .next()
        .ok_or_else(|| KrishivError::Runtime {
            message: "spine lowering requires exactly one sink".into(),
        })?;

    // Fold the sink view's `Drop` predicates into the composed query.
    let drop_predicates: Vec<String> = expectations
        .iter()
        .filter(|e| e.view == sink_view && e.on_violation == OnViolation::Drop)
        .map(|e| e.predicate.clone())
        .collect();
    let query = compose_query(&views, &sink_view, &drop_predicates);

    // Drain every source to in-memory batches and register it under its name.
    let provider = InMemorySourceProvider::new();
    let mut source_specs = Vec::with_capacity(sources.len());
    for (source_name, ingest) in sources {
        let batches = match ingest {
            Ingest::Memory(batches) => batches,
            Ingest::Connector(mut source) => {
                let mut batches = Vec::new();
                while let Some(batch) =
                    source
                        .read_batch_dyn()
                        .await
                        .map_err(|e| KrishivError::Runtime {
                            message: format!("pipeline connector source read: {e}"),
                        })?
                {
                    batches.push(batch);
                }
                batches
            }
            Ingest::Cdc(_) => {
                return Err(KrishivError::Runtime {
                    message: "CDC sources are not spine-lowerable (driver owns the IVM loop)"
                        .into(),
                });
            }
        };
        provider.insert(&source_name, batches);
        source_specs.push(SourceSpec::bounded(&source_name, "memory", ""));
    }

    let sink_spec = SinkSpec::new(&sink_view, "memory", "");
    let runtime = embedded_runtime(
        Arc::new(provider),
        Arc::new(PipelineEgressSinkProvider::new(egress)),
    );

    // The composed query (single view verbatim, or a CTE chain with folded
    // expectations) is the job query; bounded memory sources infer the batch
    // engine. Dispatch through the same entry point as every front-end.
    let job = crate::CompiledJob::new(name, query, source_specs, vec![sink_spec], false);
    crate::run_job(job, runtime)
        .await
        .map_err(KrishivError::from)?;
    Ok(())
}

/// A [`SinkProvider`] that forwards the spine's insert-only changelogs to a
/// pipeline [`Egress`](super::sink::Egress). One sink, opened once: the `Egress`
/// is moved into the writer the first time it is opened.
struct PipelineEgressSinkProvider {
    egress: std::sync::Mutex<Option<super::sink::Egress>>,
}

impl PipelineEgressSinkProvider {
    fn new(egress: super::sink::Egress) -> Self {
        Self {
            egress: std::sync::Mutex::new(Some(egress)),
        }
    }
}

#[async_trait]
impl SinkProvider for PipelineEgressSinkProvider {
    async fn open(&self, _spec: &SinkSpec) -> EngineResult<Box<dyn SinkWriter>> {
        let egress = self
            .egress
            .lock()
            .map_err(|_| EngineError::Sink("pipeline egress mutex poisoned".into()))?
            .take()
            .ok_or_else(|| {
                EngineError::Sink("pipeline egress already opened (single-sink lowering)".into())
            })?;
        Ok(Box::new(PipelineEgressSinkWriter { egress }))
    }
}

struct PipelineEgressSinkWriter {
    egress: super::sink::Egress,
}

#[async_trait]
impl SinkWriter for PipelineEgressSinkWriter {
    async fn write(&mut self, changes: ChangelogBatch) -> EngineResult<()> {
        // Batch output is insert-only; reject retractions rather than silently
        // dropping them (a connector/memory egress has no retract semantics here).
        if !changes.is_append_only() {
            return Err(EngineError::Sink(
                "pipeline egress received a retraction; only the batch engine's \
                 insert-only output is spine-lowerable today"
                    .into(),
            ));
        }
        let (batch, _kinds) = changes.into_parts();
        self.egress
            .write(batch)
            .await
            .map_err(|e| EngineError::Sink(e.to_string()))
    }

    async fn flush(&mut self) -> EngineResult<()> {
        self.egress
            .flush()
            .await
            .map_err(|e| EngineError::Sink(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use std::sync::{Arc, Mutex};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::pipeline::RunPolicy;
    use crate::{PipelineMode, SessionBuilder};

    fn kv(keys: &[&str], vals: &[i64]) -> RecordBatch {
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
    async fn single_view_batch_pipeline_runs_through_the_spine() {
        let session = SessionBuilder::new().build().unwrap();
        let collected: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));

        let pipeline = session
            .pipeline("spine-batch")
            .mode(PipelineMode::Batch)
            .source_memory("t", vec![kv(&["a", "b", "a"], &[1, 2, 3])])
            .view(
                "summary",
                "SELECT k, SUM(v) AS total FROM t GROUP BY k",
                true,
            )
            .sink_memory("summary", collected.clone())
            .build();

        // It must take the spine path, not the driver.
        assert!(is_spine_lowerable(&pipeline));
        pipeline.run(RunPolicy::Once).await.unwrap();

        let out = collected.lock().unwrap();
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 2,
            "two groups (a, b) materialized via the batch engine"
        );
    }

    #[tokio::test]
    async fn multi_view_batch_pipeline_lowers_via_ctes() {
        let session = SessionBuilder::new().build().unwrap();
        let collected: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));

        let pipeline = session
            .pipeline("multi")
            .mode(PipelineMode::Batch)
            .source_memory("t", vec![kv(&["a", "b", "a"], &[1, 2, 3])])
            .view("v1", "SELECT k, v FROM t", true)
            .view("v2", "SELECT k, SUM(v) AS total FROM v1 GROUP BY k", true)
            .sink_memory("v2", collected.clone())
            .build();

        // A multi-view DAG now lowers (CTE composition).
        assert!(is_spine_lowerable(&pipeline));
        pipeline.run(RunPolicy::Once).await.unwrap();

        let out = collected.lock().unwrap();
        let total: i64 = out
            .iter()
            .flat_map(|b| {
                let col = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
            })
            .sum();
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 2, "two groups (a=4, b=2) through the chained views");
        assert_eq!(total, 6, "SUM over the chain: a(1+3)=4 + b(2)=2 = 6");
    }

    #[tokio::test]
    async fn drop_expectation_lowers_and_filters_rows() {
        let session = SessionBuilder::new().build().unwrap();
        let collected: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));

        let pipeline = session
            .pipeline("dq")
            .mode(PipelineMode::Batch)
            .source_memory("t", vec![kv(&["a", "b", "c"], &[1, 5, 10])])
            .view("summary", "SELECT k, v FROM t", true)
            .expect("summary", "v_at_least_5", "v >= 5", OnViolation::Drop)
            .sink_memory("summary", collected.clone())
            .build();

        // A single-view pipeline with a Drop expectation lowers (predicate folded).
        assert!(is_spine_lowerable(&pipeline));
        pipeline.run(RunPolicy::Once).await.unwrap();

        let out = collected.lock().unwrap();
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 2,
            "the violating row (v=1) is dropped, leaving b and c"
        );
    }

    #[tokio::test]
    async fn fail_expectation_stays_on_driver() {
        let session = SessionBuilder::new().build().unwrap();
        let pipeline = session
            .pipeline("dq-fail")
            .mode(PipelineMode::Batch)
            .source_memory("t", vec![kv(&["a"], &[1])])
            .view("summary", "SELECT k, v FROM t", true)
            .expect("summary", "must_hold", "v >= 5", OnViolation::Fail)
            .sink_memory("summary", Arc::new(Mutex::new(Vec::new())))
            .build();
        // Fail must error on violation — not expressible as a pure query.
        assert!(!is_spine_lowerable(&pipeline));
    }

    #[tokio::test]
    async fn multi_sink_stays_on_driver() {
        let session = SessionBuilder::new().build().unwrap();
        let pipeline = session
            .pipeline("fanout")
            .mode(PipelineMode::Batch)
            .source_memory("t", vec![kv(&["a"], &[1])])
            .view("v1", "SELECT k, v FROM t", true)
            .view("v2", "SELECT k FROM v1", true)
            .sink_memory("v1", Arc::new(Mutex::new(Vec::new())))
            .sink_memory("v2", Arc::new(Mutex::new(Vec::new())))
            .build();
        // Fan-out to several sinks stays on the driver.
        assert!(!is_spine_lowerable(&pipeline));
    }

    #[test]
    fn compose_query_single_view_is_verbatim() {
        let views = vec![ViewDef {
            name: "v".into(),
            sql: "SELECT 1 AS a".into(),
            materialized: true,
            lateness: vec![],
            is_recursive: false,
        }];
        assert_eq!(compose_query(&views, "v", &[]), "SELECT 1 AS a");
    }

    #[test]
    fn compose_query_chains_ctes_and_folds_predicates() {
        let views = vec![
            ViewDef {
                name: "v1".into(),
                sql: "SELECT k, v FROM t".into(),
                materialized: true,
                lateness: vec![],
                is_recursive: false,
            },
            ViewDef {
                name: "v2".into(),
                sql: "SELECT k, v FROM v1".into(),
                materialized: true,
                lateness: vec![],
                is_recursive: false,
            },
        ];
        let q = compose_query(&views, "v2", &["v >= 5".into()]);
        assert_eq!(
            q,
            "WITH v1 AS (SELECT k, v FROM t), v2 AS (SELECT k, v FROM v1) \
             SELECT * FROM v2 WHERE (v >= 5)"
        );
    }
}
