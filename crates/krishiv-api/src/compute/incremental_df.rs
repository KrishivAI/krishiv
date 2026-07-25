//! `IncrementalDataFrame` — the delta/IVM mode of the unified DataFrame surface.
//!
//! Built by [`DataFrame::to_incremental`](crate::DataFrame::to_incremental): the
//! DataFrame's plan becomes an incremental view's `body_sql`, registered on a
//! mode-inherited [`IvmJob`]. The handle then feeds [`DeltaBatch`] changes and
//! reads the materialized `snapshot` or the output change-feed — the same engine
//! as [`Session::ivm`](crate::Session::ivm), wrapped as one fluent conversion so
//! the same DataFrame runs in batch, streaming, or incremental mode.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use krishiv_delta::{DeltaBatch, IncrementalViewSpec};
use krishiv_runtime::{IvmJobRegistry, SharedIvmJobRegistry};

use super::ivm::IvmJob;
use super::job::{FeedableJob, StepReport};
use crate::{ExecutionMode, KrishivError, Result};

/// Fluent handle to a single incremental view maintained from a DataFrame plan.
pub struct IncrementalDataFrame {
    job: IvmJob,
    /// Embedded jobs own a private registry kept alive for the handle's lifetime.
    _registry: Option<SharedIvmJobRegistry>,
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
        let sources = extract_source_names(&body_sql);
        let spec = IncrementalViewSpec {
            name: name.to_string(),
            body_sql,
            output_schema: output_schema.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: Vec::new(),
        };
        let (job, registry) = match mode {
            ExecutionMode::Distributed => {
                let url = coordinator_http.ok_or_else(|| {
                    KrishivError::unsupported(
                        "distributed to_incremental requires a coordinator URL; \
                         connect the session with with_coordinator()/http_url",
                    )
                })?;
                // Non-partitioned so the coordinator job can host a view-DAG
                // (a derived view reading the base view's full output).
                (IvmJob::remote_unpartitioned(&url, name).await?, None)
            }
            ExecutionMode::Embedded | ExecutionMode::SingleNode => {
                // shards=1 disables auto-partitioning: an IncrementalDataFrame job
                // may gain derived views (Session::view view-DAG), and a partitioned
                // job does not cascade to downstream views. Composition correctness
                // outweighs single-view sharding here (in-process anyway).
                let registry: SharedIvmJobRegistry =
                    Arc::new(IvmJobRegistry::with_default_shards(1));
                let job = IvmJob::embedded(&registry, name)?;
                (job, Some(registry))
            }
        };
        job.register_view(spec).await?;
        Ok(Self {
            job,
            _registry: registry,
            view: name.to_string(),
            sources,
            output_schema,
        })
    }

    /// Co-register a derived view into an existing base `job` (view-DAG): the
    /// derived view's SQL references the base view by name, so the engine
    /// executes them in topological order and a feed to the base cascades here.
    /// Reached via `Session::view(iv)` + `to_incremental`.
    pub(crate) async fn derive_on_job(
        job: IvmJob,
        name: &str,
        body_sql: String,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let sources = extract_source_names(&body_sql);
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
            _registry: None,
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

    /// Feed a change to a source. `source` may be omitted only when the view has
    /// exactly one source. Does not advance the tick — call [`step`](Self::step)
    /// (or use [`apply_and_step`](Self::apply_and_step)).
    pub async fn apply(&self, source: Option<&str>, delta: &DeltaBatch) -> Result<()> {
        let src = self.resolve_source(source)?;
        self.job.feed(&src, delta).await
    }

    /// Feed a change and advance one tick.
    pub async fn apply_and_step(
        &self,
        source: Option<&str>,
        delta: &DeltaBatch,
    ) -> Result<StepReport> {
        self.apply(source, delta).await?;
        self.step().await
    }

    /// Advance one IVM tick, returning per-view output counts.
    pub async fn step(&self) -> Result<StepReport> {
        self.job.step().await
    }

    /// Read the current full materialized snapshot of the view (`None` if the
    /// view has not produced output yet).
    pub async fn snapshot(&self) -> Result<Option<RecordBatch>> {
        self.job.snapshot(&self.view).await
    }

    /// The output change-feed delta from the most recent tick (`None` if none).
    /// Embedded jobs only; distributed change-feed is surfaced via the job's HTTP
    /// stats endpoint. Together with repeated [`step`](Self::step) this drives a
    /// streaming iterator of output deltas.
    pub fn last_output(&self) -> Result<Option<DeltaBatch>> {
        self.job.view_output(&self.view)
    }

    /// The view's identifier.
    pub fn name(&self) -> &str {
        &self.view
    }

    /// The source names the view reads (feedable via [`apply`](Self::apply)).
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

/// Best-effort extraction of base-table source names from a view SQL body.
///
/// Collects the identifier following each top-level `FROM`/`JOIN` keyword,
/// skipping subqueries (`FROM (`) and de-duplicating. This drives the
/// single-source default and API validation; feeding by explicit name always
/// works regardless.
fn extract_source_names(sql: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let lowered = sql.to_ascii_lowercase();
    let bytes = lowered.as_bytes();
    for kw in ["from", "join"] {
        let mut search_from = 0usize;
        while let Some(rel) = lowered[search_from..].find(kw) {
            let start = search_from + rel;
            let end = start + kw.len();
            search_from = end;
            // Require the keyword to be whitespace-delimited (not part of a word).
            let before_ok = start
                .checked_sub(1)
                .and_then(|i| bytes.get(i))
                .is_none_or(|b| !b.is_ascii_alphanumeric());
            let after_ok = bytes.get(end).is_none_or(|b| b.is_ascii_whitespace());
            if !before_ok || !after_ok {
                continue;
            }
            // First non-space token after the keyword.
            let rest = lowered[end..].trim_start();
            if rest.starts_with('(') {
                continue; // subquery, not a base table
            }
            // The unparser quotes reserved-word table names (e.g. `"returns"`);
            // read the quoted identifier, otherwise a bare identifier.
            let token: String = if let Some(inner) = rest.strip_prefix('"') {
                inner.chars().take_while(|c| *c != '"').collect()
            } else {
                rest.chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
                    .collect()
            };
            if !token.is_empty() && !out.contains(&token) {
                out.push(token);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_single_source() {
        let s = extract_source_names("SELECT k, SUM(v) AS total FROM orders GROUP BY k");
        assert_eq!(s, vec!["orders".to_string()]);
    }

    #[test]
    fn extracts_join_sources_and_dedups() {
        let s = extract_source_names(
            "SELECT o.k FROM orders o JOIN returns r ON o.k = r.k JOIN orders o2 ON o2.k = r.k",
        );
        assert_eq!(s, vec!["orders".to_string(), "returns".to_string()]);
    }

    #[test]
    fn extracts_quoted_identifiers() {
        // The unparser quotes reserved-word table names.
        let s =
            extract_source_names("SELECT o.k FROM orders AS o JOIN \"returns\" AS r ON o.k = r.k");
        assert_eq!(s, vec!["orders".to_string(), "returns".to_string()]);
    }

    #[test]
    fn skips_subquery_from() {
        // A derived table (FROM ( ... )) has no base name at that position; the
        // inner FROM still contributes its base table.
        let s = extract_source_names("SELECT * FROM (SELECT * FROM orders) q");
        assert_eq!(s, vec!["orders".to_string()]);
    }
}
