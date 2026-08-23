//! `IncrementalDataFrame` — the delta/IVM mode of the unified DataFrame surface.
//!
//! Built by [`DataFrame::to_incremental`](crate::DataFrame::to_incremental): the
//! DataFrame's plan becomes an incremental view's `body_sql`, registered on a
//! mode-inherited [`IvmJob`]. The handle then feeds [`DeltaBatch`] changes and
//! reads the materialized `snapshot` or the output change-feed — the same engine
//! as [`Session::ivm`](crate::Session::ivm), wrapped as one fluent conversion so
//! the same DataFrame runs in batch, streaming, or incremental mode.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::ast::{ObjectName, Query, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_runtime::{IvmJobRegistry, SharedIvmJobRegistry};

use super::ivm::IvmJob;
use super::job::{FeedableJob, Job, StepReport};
use crate::{ExecutionMode, KrishivError, Result};

/// Fluent handle to a single incremental view maintained from a DataFrame plan.
pub struct IncrementalDataFrame {
    job: IvmJob,
    view: String,
    sources: Vec<String>,
    output_schema: SchemaRef,
}

impl IncrementalDataFrame {
    /// Register `body_sql` as an incremental view on a mode-inherited job.
    ///
    /// Embedded/single-node sessions get a fresh in-process job (sources are
    /// established by the fed [`DeltaBatch`]es, so no session catalog is needed);
    /// distributed sessions attach to the coordinator's IVM endpoint.
    pub(crate) async fn from_view_sql(
        name: &str,
        body_sql: String,
        output_schema: SchemaRef,
        mode: ExecutionMode,
        coordinator_http: Option<String>,
    ) -> Result<Self> {
        let sources = extract_source_names(&body_sql)?;
        let spec = IncrementalViewSpec {
            name: name.to_string(),
            body_sql,
            output_schema: output_schema.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: Vec::new(),
        };
        let job = match mode {
            ExecutionMode::Distributed => {
                let url = coordinator_http.ok_or_else(|| {
                    KrishivError::unsupported(
                        "distributed to_incremental requires a coordinator URL; \
                         connect the session with with_coordinator()/http_url",
                    )
                })?;
                // Non-partitioned so the coordinator job can host a view-DAG
                // (a derived view reading the base view's full output).
                IvmJob::remote_unpartitioned(&url, name).await?
            }
            ExecutionMode::Embedded | ExecutionMode::SingleNode => {
                // shards=1 disables auto-partitioning: an IncrementalDataFrame job
                // may gain derived views (Session::view view-DAG), and a partitioned
                // job does not cascade to downstream views. Composition correctness
                // outweighs single-view sharding here (in-process anyway).
                //
                // The registry is private to this handle and is kept alive by the
                // returned job: `EmbeddedIvmJob` holds its own `Arc` clone of it.
                // (An extra `_registry` field here used to claim that job — it was
                // inert, and a second owner of an `Arc` proves nothing, API-G7.)
                let registry: SharedIvmJobRegistry =
                    Arc::new(IvmJobRegistry::with_default_shards(1));
                IvmJob::embedded(&registry, name)?
            }
        };
        job.register_view(spec).await?;
        Ok(Self {
            job,
            view: name.to_string(),
            sources,
            output_schema,
        })
    }

    /// Co-register a derived view into an existing base `job` (view-DAG): the
    /// derived view's SQL references the base view by name, so the engine
    /// executes them in topological order and a feed to the base cascades here.
    /// Reached via `Session::view(iv)` + `to_incremental`.
    ///
    /// A feed to the base reaches this view in the **same** tick: the engine
    /// walks the view DAG in topological order, and an incremental base that a
    /// SQL-running dependent reads also produces its full output in that
    /// tick's SQL phase (IVM-AUD-CORE-17). One `step()` is enough. This was
    /// once documented here as a permanent one-tick lag; it was in fact a
    /// permanently *empty* derived view, and it is fixed.
    ///
    /// # Requires a parent known to be single
    ///
    /// A partitioned (auto-sharded) job maintains each shard's flow
    /// independently and never cascades a base view's output into downstream
    /// views, so a view derived on one would sit empty forever. This rejects
    /// any parent not **known** to be single, rather than only rejecting one
    /// known to be partitioned (API-F4): a remote job's shape is unknown to
    /// the client unless this handle created it through `create_unpartitioned`
    /// — which `to_incremental` and `Session::view` do, but the `pub`
    /// `DataFrame::with_ivm_parent` does not, and it accepts any `IvmJob`
    /// including an auto-partitioning one from `Session::ivm`.
    pub(crate) async fn derive_on_job(
        job: IvmJob,
        name: &str,
        body_sql: String,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        match job.is_partitioned()? {
            Some(false) => {}
            Some(true) => {
                return Err(KrishivError::unsupported(format!(
                    "cannot derive view '{name}' on IVM job '{}': the job is key-partitioned, and \
                     a partitioned flow does not cascade a base view's output to downstream \
                     views, so '{name}' would never receive data. Build the base view on an \
                     unpartitioned job (DataFrame::to_incremental pins shards=1 for exactly this \
                     reason).",
                    job.job_id()
                )));
            }
            None => {
                return Err(KrishivError::unsupported(format!(
                    "cannot derive view '{name}' on IVM job '{}': this handle cannot confirm the \
                     job is unpartitioned. A remote job created with auto-partitioning may shard \
                     its first key-shardable view, and a partitioned flow never cascades a base \
                     view's output to downstream views, so '{name}' would sit empty forever. \
                     Create the parent with Session::ivm_unpartitioned / \
                     DataFrame::to_incremental, which pin the coordinator job single.",
                    job.job_id()
                )));
            }
        }
        let sources = extract_source_names(&body_sql)?;
        let spec = IncrementalViewSpec {
            name: name.to_string(),
            body_sql,
            output_schema: output_schema.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: Vec::new(),
        };
        job.register_view(spec).await?;
        Ok(Self {
            job,
            view: name.to_string(),
            sources,
            output_schema,
        })
    }

    /// The underlying IVM job handle — for co-registering derived views into the
    /// same job (`Session::view`).
    pub fn shared_job(&self) -> IvmJob {
        self.job.clone()
    }

    /// The view's declared output schema (used by `Session::view` to register the
    /// base view as a client-side source for planning the derived query).
    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Buffer a change for a source. **Does not advance the tick.**
    ///
    /// The delta sits in the job's pending buffer until [`step`](Self::step)
    /// runs; nothing about the view changes until then, and a snapshot taken in
    /// between still shows the pre-feed state. `source` may be omitted only
    /// when the view reads exactly one source (see [`source_names`]).
    ///
    /// # Not the same call as Python's `apply`
    ///
    /// The Python binding's `IncrementalDataFrame.apply(delta, source=None)`
    /// takes the **delta first** and *also* steps (unless inside a
    /// `transaction()` block). This Rust method takes the **source first** and
    /// never steps. The two are deliberately different verbs on purpose-built
    /// surfaces — Python is a scripting front end, Rust hands you the tick — but
    /// they share a name, so do not port a loop between them without adding or
    /// removing the `step()` (API-D1).
    ///
    /// [`source_names`]: Self::source_names
    pub async fn apply(&self, source: Option<&str>, delta: &DeltaBatch) -> Result<()> {
        let src = self.resolve_source(source)?;
        self.job.feed(&src, delta).await
    }

    /// Advance one IVM tick: drain every buffered input, re-evaluate the views
    /// on this job in topological order, and publish their outputs.
    ///
    /// The returned [`StepReport`] carries the tick counter and the per-tick
    /// output counters — and, in `degraded_views` / `errored_views`, **the only
    /// view-level failure signal there is**. A view whose SQL or operator fails
    /// does not make this call return `Err`: the tick still succeeds, the view
    /// is skipped, and its name appears in `errored_views` (API-F6). Code that
    /// ignores those two fields cannot tell a healthy tick from a broken view.
    ///
    /// Distributed jobs are worse off still: the coordinator's step response
    /// has no health fields, so both vectors arrive empty regardless of what
    /// happened remotely (API-A5, open — see
    /// `docs/implementation/ivm-audit-register.md`).
    pub async fn step(&self) -> Result<StepReport> {
        self.job.step().await
    }

    /// Read the current full materialized snapshot of the view (`None` if the
    /// view has not produced output yet).
    pub async fn snapshot(&self) -> Result<Option<RecordBatch>> {
        self.job.snapshot(&self.view).await
    }

    /// The output change-feed delta from the most recent tick (`None` if the
    /// view has produced none yet). Together with repeated [`step`](Self::step)
    /// this drives an iterator of output deltas.
    ///
    /// Embedded jobs only: a distributed job returns
    /// [`KrishivError::Unsupported`] because there is no client binding for the
    /// coordinator's view-output route, and answering `None` forever made a
    /// remote change-feed loop a silent no-op (INT-F6). The peek is
    /// non-consuming and coalescing — two ticks between two reads means the
    /// first delta is gone, so this is a "latest value", not a stream.
    pub fn last_output(&self) -> Result<Option<DeltaBatch>> {
        self.job.view_output(&self.view)
    }

    /// The view's identifier.
    pub fn name(&self) -> &str {
        &self.view
    }

    /// The source names the view reads (feedable via [`apply`](Self::apply)).
    ///
    /// Parsed from the view body's SQL AST at registration, with identifiers
    /// normalized the way DataFusion resolves them (unquoted lowercased, quoted
    /// kept verbatim) and CTE names excluded, so these are the relations the
    /// body actually reads rather than a guess (API-F7).
    ///
    /// "Relations", not "sources": for a **derived** view the body reads its
    /// upstream *view*, and that view's name is what appears here. Feeding it
    /// pushes rows into the flow under that name, which is not the same thing
    /// as feeding a base source — the upstream view will overwrite them on its
    /// next publication.
    pub fn source_names(&self) -> &[String] {
        &self.sources
    }

    fn resolve_source(&self, source: Option<&str>) -> Result<String> {
        match source {
            Some(s) => Ok(s.to_string()),
            None => match self.sources.as_slice() {
                [only] => Ok(only.clone()),
                _ => Err(KrishivError::unsupported(format!(
                    "view '{}' reads {} sources {:?}; pass source=<name>",
                    self.view,
                    self.sources.len(),
                    self.sources
                ))),
            },
        }
    }
}

/// The base relations a view body reads, in first-appearance order.
///
/// Parsed from the SQL AST (`sqlparser`, the same grammar DataFusion plans
/// with) and **not** scanned for keywords: the previous implementation
/// lowercased the whole body and searched for `from`/`join`, so a `FROM` inside
/// a string literal or a `-- join` comment invented a source, and every CTE
/// name was reported as a base table. That set is a `pub` getter
/// ([`IncrementalDataFrame::source_names`]) and drives single-source
/// defaulting, so a phantom entry turned an unambiguous `apply(None, …)` into a
/// hard error (API-F7).
///
/// Identifiers are normalized the way DataFusion resolves them: unquoted parts
/// are lowercased, quoted parts keep their case, and a qualified name is joined
/// with `.`. CTE names are excluded — they are computed inside the query, not
/// fed. SQL that does not parse is an error, not an empty list: the caller is
/// registering that same text as a view body, and guessing its inputs from
/// unparseable SQL is how a silent no-op feed happens.
fn extract_source_names(sql: &str) -> Result<Vec<String>> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| {
        KrishivError::unsupported(format!(
            "cannot determine the sources of an incremental view: its body SQL does not parse \
             ({e}); body: {sql}"
        ))
    })?;
    let mut collector = SourceCollector::default();
    for statement in &statements {
        // `Visit` never breaks here (the visitor's `Break` type is `()` and it
        // always continues), so the control flow carries no error to inspect.
        let _ = statement.visit(&mut collector);
    }
    let SourceCollector { relations, ctes } = collector;
    Ok(relations
        .into_iter()
        .filter(|name| !ctes.contains(name))
        .collect())
}

/// Collects relation names and the CTE names that shadow them.
///
/// CTEs are gathered from every `WITH` clause in the statement, including
/// nested ones. Scoping is deliberately ignored: a base table that shares its
/// name with a CTE elsewhere in the same body cannot be fed unambiguously
/// anyway, so dropping it is the conservative answer.
#[derive(Default)]
struct SourceCollector {
    relations: Vec<String>,
    ctes: HashSet<String>,
}

impl Visitor for SourceCollector {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.insert(normalize_ident(&cte.alias.name));
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        let name = normalize_object_name(relation);
        if !name.is_empty() && !self.relations.contains(&name) {
            self.relations.push(name);
        }
        ControlFlow::Continue(())
    }
}

/// `schema.Table` → `schema.Table`, `Orders` → `orders`: DataFusion lowercases
/// unquoted identifiers and preserves quoted ones, so a name extracted here
/// matches the name the engine registers the source under.
fn normalize_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| part.as_ident())
        .map(normalize_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn normalize_ident(ident: &datafusion::sql::sqlparser::ast::Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_ascii_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn orders_batch(regions: &[&str], amounts: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(regions.to_vec())),
                Arc::new(Int64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn revenue_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
        ]))
    }

    const REVENUE_SQL: &str = "SELECT region, SUM(amount) AS total FROM orders GROUP BY region";

    async fn embedded_revenue(name: &str) -> IncrementalDataFrame {
        IncrementalDataFrame::from_view_sql(
            name,
            REVENUE_SQL.to_string(),
            revenue_schema(),
            ExecutionMode::Embedded,
            None,
        )
        .await
        .unwrap()
    }

    // ── API-F7: source extraction is parsed, not scanned ──────────────────

    #[test]
    fn extracts_single_source() {
        let s = extract_source_names(REVENUE_SQL).unwrap();
        assert_eq!(s, vec!["orders".to_string()]);
    }

    #[test]
    fn extracts_join_sources_and_dedups() {
        let s = extract_source_names(
            "SELECT o.k FROM orders o JOIN returns r ON o.k = r.k JOIN orders o2 ON o2.k = r.k",
        )
        .unwrap();
        assert_eq!(s, vec!["orders".to_string(), "returns".to_string()]);
    }

    #[test]
    fn quoted_identifiers_keep_their_case_and_unquoted_ones_are_lowered() {
        // DataFusion resolves `Orders` as `orders` but `"Returns"` as `Returns`;
        // a source name that does not match what the engine registered cannot
        // be fed.
        let s =
            extract_source_names("SELECT o.k FROM Orders AS o JOIN \"Returns\" AS r ON o.k = r.k")
                .unwrap();
        assert_eq!(s, vec!["orders".to_string(), "Returns".to_string()]);
    }

    #[test]
    fn collects_base_tables_inside_subqueries() {
        let s = extract_source_names("SELECT * FROM (SELECT * FROM orders) q").unwrap();
        assert_eq!(s, vec!["orders".to_string()]);
    }

    #[test]
    fn qualified_names_keep_their_qualifier() {
        let s = extract_source_names("SELECT * FROM sales.orders").unwrap();
        assert_eq!(s, vec!["sales.orders".to_string()]);
    }

    /// The keyword scanner read `from` inside string literals: this body has
    /// exactly one source, and `'shipped from warehouse'` is a value.
    #[test]
    fn string_literals_do_not_invent_sources() {
        let s = extract_source_names(
            "SELECT region FROM orders WHERE note = 'shipped from warehouse' \
             AND label <> 'join failures'",
        )
        .unwrap();
        assert_eq!(s, vec!["orders".to_string()]);
    }

    /// …and it read `join`/`from` inside comments, too.
    #[test]
    fn comments_do_not_invent_sources() {
        let s = extract_source_names(
            "-- join dimension_table later\n             SELECT region FROM orders /* from staging_table originally */",
        )
        .unwrap();
        assert_eq!(s, vec!["orders".to_string()]);
    }

    /// A CTE is computed inside the query; feeding it is impossible, and
    /// reporting it made `source_names()` ambiguous so `apply(None, …)` failed.
    #[test]
    fn cte_names_are_not_sources() {
        let s = extract_source_names(
            "WITH recent AS (SELECT * FROM orders WHERE ts > 0) \
             SELECT region, SUM(amount) AS total FROM recent GROUP BY region",
        )
        .unwrap();
        assert_eq!(s, vec!["orders".to_string()]);
    }

    #[test]
    fn nested_cte_names_are_not_sources() {
        let s = extract_source_names(
            "WITH a AS (WITH b AS (SELECT * FROM orders) SELECT * FROM b) SELECT * FROM a",
        )
        .unwrap();
        assert_eq!(s, vec!["orders".to_string()]);
    }

    /// Fail closed: unparseable SQL means the source set is unknown, and an
    /// unknown source set silently mis-routes every `apply(None, …)`.
    #[test]
    fn unparseable_body_is_an_error_not_an_empty_source_list() {
        let err = match extract_source_names("SELECT FROM FROM WHERE") {
            Ok(sources) => panic!("unparseable SQL must not yield sources {sources:?}"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("does not parse"), "{err}");
    }

    /// The single-source default depends on the extracted set being exactly
    /// right: one phantom entry turns an unambiguous feed into a hard error.
    #[tokio::test]
    async fn single_source_default_survives_a_string_literal_in_the_body() {
        let iv = IncrementalDataFrame::from_view_sql(
            "flagged",
            "SELECT region, amount FROM orders WHERE note = 'moved from depot'".to_string(),
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
            ])),
            ExecutionMode::Embedded,
            None,
        )
        .await
        .unwrap();
        assert_eq!(iv.source_names(), ["orders".to_string()]);
        let delta = DeltaBatch::from_inserts(orders_batch(&["US"], &[10])).unwrap();
        iv.apply(None, &delta).await.unwrap();
    }

    // ── API-D1 / API-G5: `apply` buffers, `step` ticks ────────────────────

    #[tokio::test]
    async fn apply_buffers_without_ticking_and_step_applies_it() {
        let iv = embedded_revenue("rev_lag").await;
        let delta = DeltaBatch::from_inserts(orders_batch(&["US", "EU"], &[10, 5])).unwrap();
        iv.apply(None, &delta).await.unwrap();
        // Nothing is visible until the tick: Rust `apply` is not Python `apply`.
        assert!(iv.snapshot().await.unwrap().is_none());
        let report = iv.step().await.unwrap();
        assert_eq!(report.tick, 1);
        assert_eq!(iv.snapshot().await.unwrap().unwrap().num_rows(), 2);
    }

    // ── API-G7: the handle owns its registry through the job ──────────────

    #[tokio::test]
    async fn embedded_handle_keeps_its_private_registry_alive() {
        // `from_view_sql` drops the only local `Arc` to the registry before
        // returning; the job's own clone is what keeps it alive.
        let iv = embedded_revenue("rev_reg").await;
        let delta = DeltaBatch::from_inserts(orders_batch(&["US"], &[7])).unwrap();
        iv.apply(None, &delta).await.unwrap();
        iv.step().await.unwrap();
        assert_eq!(iv.snapshot().await.unwrap().unwrap().num_rows(), 1);
    }

    // ── API-F4: a derived view may not be built on a partitioned job ──────

    fn revenue_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "revenue".into(),
            body_sql: REVENUE_SQL.into(),
            output_schema: revenue_schema(),
            is_materialized: true,
            is_recursive: false,
            lateness: Vec::new(),
        }
    }

    #[tokio::test]
    async fn derive_on_partitioned_job_is_rejected() {
        let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job = IvmJob::embedded(&registry, "sharded").unwrap();
        job.register_view(revenue_spec()).await.unwrap();
        assert_eq!(job.is_partitioned().unwrap(), Some(true));

        let err = match IncrementalDataFrame::derive_on_job(
            job,
            "top_regions",
            "SELECT region FROM revenue".to_string(),
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                true,
            )])),
        )
        .await
        {
            Ok(_) => panic!("a derived view on a partitioned job never receives data"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("key-partitioned"), "{err}");
        assert!(err.contains("top_regions"), "{err}");
    }

    #[tokio::test]
    async fn derive_on_unpartitioned_job_is_allowed() {
        let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(1));
        let job = IvmJob::embedded(&registry, "single").unwrap();
        job.register_view(revenue_spec()).await.unwrap();
        assert_eq!(job.is_partitioned().unwrap(), Some(false));

        let derived = IncrementalDataFrame::derive_on_job(
            job,
            "top_regions",
            "SELECT region FROM revenue".to_string(),
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                true,
            )])),
        )
        .await
        .unwrap();
        assert_eq!(derived.name(), "top_regions");
    }

    /// IVM-AUD-API-F4. The guard used to reject only `Some(true)`, and every
    /// remote job answered `None`, so a remote parent was never rejected — and
    /// a partitioned one is reachable: `DataFrame::with_ivm_parent` is `pub`
    /// and takes any `IvmJob`, including an auto-partitioning one from
    /// `Session::ivm`. A partitioned flow never cascades a base view's output,
    /// so the derived view would sit empty forever.
    #[tokio::test]
    async fn deriving_on_a_parent_of_unknown_shape_is_rejected() {
        let unknown = IvmJob::Remote(krishiv_runtime::RemoteIvmJob::from_job_id(
            "http://127.0.0.1:1",
            "unverified-parent",
        ));
        let result = IncrementalDataFrame::derive_on_job(
            unknown,
            "regions",
            "SELECT region FROM revenue".to_string(),
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                true,
            )])),
        )
        .await;
        let err = match result {
            Ok(_) => panic!("a parent whose shape cannot be confirmed must be rejected"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("unverified-parent") && err.contains("cannot confirm"),
            "the error must name the job and say what it could not confirm: {err}"
        );
    }

    // ── API-F5: a derived view sees its base in the SAME tick ────────────

    /// IVM-AUD-CORE-17. This used to be impossible: the tick ran view SQL in an
    /// unlocked phase and applied incremental operators later under the lock,
    /// so a DiffBased view reading an Incremental view found no table at all
    /// and failed planning with "table 'revenue' not found". It then produced
    /// an empty batch, and — because a view is only recomputed when one of its
    /// dependencies is dirty, and the base is not dirty on a tick with no new
    /// input — it never recovered. A derived view was permanently empty, which
    /// an earlier note in this file mistook for a one-tick lag.
    #[tokio::test]
    async fn derived_view_sees_the_base_view_in_the_same_tick() {
        let base = embedded_revenue("revenue").await;
        let derived = IncrementalDataFrame::derive_on_job(
            base.shared_job(),
            "regions",
            "SELECT region FROM revenue".to_string(),
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                true,
            )])),
        )
        .await
        .unwrap();

        let delta = DeltaBatch::from_inserts(orders_batch(&["US", "EU"], &[10, 5])).unwrap();
        base.apply(Some("orders"), &delta).await.unwrap();
        let report = base.step().await.unwrap();

        assert!(
            report.errored_views.is_empty(),
            "the derived view must not fail planning against its base: {:?}",
            report.errored_views
        );
        assert_eq!(
            derived
                .snapshot()
                .await
                .unwrap()
                .map(|b| b.num_rows())
                .unwrap_or(0),
            2,
            "the derived view carries the base's output after the base's first tick"
        );
    }

    // ── API-F6: `step` is Ok even when a view failed ──────────────────────

    #[tokio::test]
    async fn step_reports_a_failing_view_through_errored_views_not_err() {
        let iv = IncrementalDataFrame::from_view_sql(
            "broken",
            "SELECT region, no_such_column FROM orders".to_string(),
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("no_such_column", DataType::Int64, true),
            ])),
            ExecutionMode::Embedded,
            None,
        )
        .await
        .unwrap();
        let delta = DeltaBatch::from_inserts(orders_batch(&["US"], &[1])).unwrap();
        iv.apply(None, &delta).await.unwrap();

        let report = iv.step().await.unwrap();
        assert!(
            !report.errored_views.is_empty(),
            "a view that cannot evaluate must appear in errored_views: {report:?}"
        );
        assert_eq!(report.errored_views[0].view, "broken");
    }
}
