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
            output_schema,
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
                (IvmJob::remote(&url, name).await?, None)
            }
            ExecutionMode::Embedded | ExecutionMode::SingleNode => {
                let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::new());
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
        })
    }

    /// Feed a change to a source. `source` may be omitted only when the view has
    /// exactly one source. Does not advance the tick — call [`step`](Self::step)
    /// (or use [`apply_and_step`](Self::apply_and_step)).
    pub async fn apply(&self, source: Option<&str>, delta: &DeltaBatch) -> Result<()> {
        let src = self.resolve_source(source)?;
        self.job.feed(&src, delta).await
    }

    /// Feed a change and advance one tick.
    pub async fn apply_and_step(&self, source: Option<&str>, delta: &DeltaBatch) -> Result<StepReport> {
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
            None if self.sources.len() == 1 => Ok(self.sources[0].clone()),
            None => Err(KrishivError::unsupported(format!(
                "view '{}' reads {} sources {:?}; pass source=<name>",
                self.view,
                self.sources.len(),
                self.sources
            ))),
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
            let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
            let after_ok = end >= bytes.len() || bytes[end].is_ascii_whitespace();
            if !before_ok || !after_ok {
                continue;
            }
            // First non-space token after the keyword.
            let rest = lowered[end..].trim_start();
            if rest.starts_with('(') {
                continue; // subquery, not a base table
            }
            let token: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
                .collect();
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
    fn skips_subquery_from() {
        // A derived table (FROM ( ... )) has no base name at that position; the
        // inner FROM still contributes its base table.
        let s = extract_source_names("SELECT * FROM (SELECT * FROM orders) q");
        assert_eq!(s, vec!["orders".to_string()]);
    }
}
