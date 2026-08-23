//! Mode-agnostic IVM job handle.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use krishiv_delta::DeltaBatch;
use krishiv_ivm::IncrementalViewSpec;
use krishiv_runtime::{EmbeddedIvmJob, RemoteIvmJob, SharedIvmJobRegistry};

use super::job::{Checkpointable, FeedableJob, Job, JobKind, StepReport};
use crate::{KrishivError, Result};

/// A handle to an incremental-view-maintenance job.
///
/// Returned by [`Session::ivm`](crate::Session::ivm). The same handle works in
/// both embedded (in-process) and distributed (coordinator) modes — the session
/// picks the variant from its execution mode, so callers write identical code.
#[derive(Debug, Clone)]
pub enum IvmJob {
    /// In-process execution.
    Embedded(EmbeddedIvmJob),
    /// Remote execution via coordinator HTTP.
    Remote(RemoteIvmJob),
}

impl IvmJob {
    /// Create (or attach to) an embedded IVM job in `registry`.
    pub fn embedded(registry: &SharedIvmJobRegistry, name: &str) -> Result<Self> {
        Ok(Self::Embedded(EmbeddedIvmJob::create(registry, name)?))
    }

    /// Create a remote IVM job on the coordinator at `coordinator_http`.
    pub async fn remote(coordinator_http: &str, name: &str) -> Result<Self> {
        Ok(Self::Remote(
            RemoteIvmJob::create(coordinator_http, Some(name)).await?,
        ))
    }

    /// Like [`remote`](Self::remote), but pins the coordinator job to a single
    /// (non-partitioned) flow so it can host a view-DAG (a derived view reading
    /// the base view's full output). Used by distributed `to_incremental`.
    pub async fn remote_unpartitioned(coordinator_http: &str, name: &str) -> Result<Self> {
        Ok(Self::Remote(
            RemoteIvmJob::create_unpartitioned(coordinator_http, Some(name)).await?,
        ))
    }

    /// Register or update an incremental view on this job.
    pub async fn register_view(&self, spec: IncrementalViewSpec) -> Result<()> {
        match self {
            Self::Embedded(j) => j.register_view(spec)?,
            Self::Remote(j) => j.register_view(&spec).await?,
        }
        Ok(())
    }

    /// Enable delta-checkpoint accumulation so
    /// [`checkpoint_delta`](Checkpointable::checkpoint_delta) captures
    /// every input fed *after* this call.
    ///
    /// **Embedded only.** A remote job returns
    /// [`KrishivError::Unsupported`]: the coordinator IVM HTTP API has no
    /// request field and no handler that turns accumulation on, so its
    /// `/checkpoint-delta` answers an empty (`count = 0`) frame forever. This
    /// used to be a silent `Ok(())` documented as "remote always on", which was
    /// false in both halves — a caller asking for incremental backups got a
    /// success and no backup (API-A6 / DIST-C1).
    pub fn enable_delta_checkpoints(&self) -> Result<()> {
        match self {
            Self::Embedded(j) => Ok(j.enable_delta_checkpoints()?),
            Self::Remote(j) => Err(KrishivError::unsupported(format!(
                "delta-checkpoint accumulation cannot be enabled on remote IVM job '{}': the \
                 coordinator IVM HTTP API exposes no enable-delta-checkpoints endpoint, so \
                 checkpoint_delta() would return an empty frame. Take full checkpoints \
                 (checkpoint()/restore()) distributed, or run the job embedded.",
                j.job_id()
            ))),
        }
    }

    /// Enable content-addressed input dedup (drop re-delivered insertion rows).
    ///
    /// **Embedded only.** A remote job returns
    /// [`KrishivError::Unsupported`] rather than pretending: nothing in the
    /// coordinator IVM HTTP API enables dedup, and `/feed` is at-most-once with
    /// no sequence number or idempotency key, so a retry after a timeout is
    /// applied twice. Silently accepting a request for exactly-once input
    /// semantics exactly where retries happen is the worst possible no-op
    /// (API-A6 / INT-F3).
    pub fn enable_input_dedup(&self) -> Result<()> {
        match self {
            Self::Embedded(j) => Ok(j.enable_input_dedup()?),
            Self::Remote(j) => Err(KrishivError::unsupported(format!(
                "content-addressed input dedup cannot be enabled on remote IVM job '{}': the \
                 coordinator IVM HTTP API exposes no dedup endpoint and its feed routes carry \
                 no idempotency key. Run the job embedded if you need exactly-once feeds.",
                j.job_id()
            ))),
        }
    }

    /// The most recent output delta produced for `view` (the change-feed item
    /// from the last tick).
    ///
    /// **Embedded only.** A remote job returns [`KrishivError::Unsupported`].
    /// The coordinator does own a `/views/{view}/output` route, but
    /// `krishiv-runtime`'s coordinator client has no binding for it, so this
    /// handle has no way to read it; returning `Ok(None)` forever (as this did)
    /// is indistinguishable from "the last tick produced no change" and made a
    /// distributed change-feed loop an infinite no-op (INT-F6). Poll
    /// [`snapshot`](FeedableJob::snapshot) instead until the client binding
    /// exists.
    pub fn view_output(&self, view: &str) -> Result<Option<DeltaBatch>> {
        match self {
            Self::Embedded(j) => Ok(j.view_output(view)?),
            Self::Remote(j) => Err(KrishivError::unsupported(format!(
                "no change feed for remote IVM job '{}' view '{view}': krishiv-runtime has no \
                 client binding for the coordinator's /api/v1/ivm/jobs/{{job}}/views/{{view}}/output \
                 route, so the last output delta cannot be read from this handle. Poll \
                 snapshot() for the materialized view instead.",
                j.job_id()
            ))),
        }
    }

    /// Whether this job auto-partitioned (its first view was key-shardable),
    /// or `None` when the answer is not knowable.
    ///
    /// A remote handle answers `Some(false)` only when it created the job
    /// through `create_unpartitioned` and therefore knows the coordinator
    /// pinned it single. Otherwise it answers `None`: the coordinator's
    /// create/list responses do not report the shape back, so `None` means
    /// "cannot verify", never "no". A caller that must not run on a
    /// partitioned flow — a view-DAG parent, for instance — must therefore
    /// require `Some(false)` rather than merely reject `Some(true)`
    /// (IVM-AUD-API-F4).
    pub fn is_partitioned(&self) -> Result<Option<bool>> {
        match self {
            Self::Embedded(j) => Ok(Some(j.is_partitioned()?)),
            Self::Remote(j) => Ok(if j.is_pinned_single() {
                Some(false)
            } else {
                None
            }),
        }
    }
}

impl Job for IvmJob {
    fn job_id(&self) -> &str {
        match self {
            Self::Embedded(j) => j.job_id(),
            Self::Remote(j) => j.job_id(),
        }
    }

    fn kind(&self) -> JobKind {
        JobKind::Ivm
    }
}

#[async_trait]
impl FeedableJob for IvmJob {
    async fn feed(&self, source: &str, delta: &DeltaBatch) -> Result<()> {
        match self {
            Self::Embedded(j) => j.feed(source, delta.clone())?,
            Self::Remote(j) => j.feed(source, delta).await?,
        }
        Ok(())
    }

    async fn feed_snapshot(&self, source: &str, batches: &[RecordBatch]) -> Result<()> {
        match self {
            Self::Embedded(j) => j.feed_snapshot(source, batches)?,
            Self::Remote(j) => j.feed_snapshot(source, batches).await?,
        }
        Ok(())
    }

    async fn step(&self) -> Result<StepReport> {
        Ok(match self {
            Self::Embedded(j) => {
                let summary = j.step().await?;
                StepReport {
                    active_views: summary.active_views,
                    total_output_rows: summary.total_output_rows,
                    tick: j.tick()?,
                    degraded_views: summary.degraded_views,
                    errored_views: summary
                        .errored_views
                        .into_iter()
                        .map(|e| super::job::ViewError {
                            view: e.view,
                            kind: match e.kind {
                                krishiv_ivm::ViewErrorKind::OperatorApply => {
                                    super::job::ViewErrorKind::OperatorApply
                                }
                                krishiv_ivm::ViewErrorKind::ViewSql => {
                                    super::job::ViewErrorKind::ViewSql
                                }
                                krishiv_ivm::ViewErrorKind::Publish => {
                                    super::job::ViewErrorKind::Publish
                                }
                            },
                            message: e.message,
                        })
                        .collect(),
                }
            }
            Self::Remote(j) => {
                let s = j.step().await?;
                // API-A5 (still OPEN — see docs/implementation/ivm-audit-register.md).
                // The coordinator's /step response carries counters only
                // (`ivm_http::StepResponse` = active_views / total_output_rows /
                // tick), so there is no view-health signal to relay. The two
                // vectors below are empty BECAUSE NOTHING WAS REPORTED, not
                // because every view is healthy, and a caller cannot tell those
                // apart through `StepReport` — distributed IVM currently has no
                // view-level failure channel at all. Closing this needs a health
                // field on the wire (krishiv-scheduler/src/ivm_http.rs +
                // krishiv-runtime/src/coordinator_http_client.rs) *and* an
                // explicitly-unknown representation in `StepReport`
                // (krishiv-api/src/compute/job.rs); all three files are outside
                // this change's ownership, so nothing here may claim otherwise.
                StepReport {
                    active_views: s.active_views,
                    total_output_rows: s.total_output_rows,
                    tick: s.tick,
                    degraded_views: Vec::new(),
                    errored_views: Vec::new(),
                }
            }
        })
    }

    async fn snapshot(&self, view: &str) -> Result<Option<RecordBatch>> {
        Ok(match self {
            Self::Embedded(j) => j.snapshot(view)?,
            Self::Remote(j) => j.snapshot(view).await?,
        })
    }
}

#[async_trait]
impl Checkpointable for IvmJob {
    async fn checkpoint(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Embedded(j) => j.checkpoint()?,
            Self::Remote(j) => j.checkpoint().await?,
        })
    }

    async fn restore(&self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Embedded(j) => j.restore(bytes)?,
            Self::Remote(j) => j.restore(bytes).await?,
        }
        Ok(())
    }

    async fn checkpoint_delta(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Embedded(j) => j.checkpoint_delta()?,
            Self::Remote(j) => j.checkpoint_delta().await?,
        })
    }

    async fn restore_delta(&self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Embedded(j) => j.restore_delta(bytes)?,
            Self::Remote(j) => j.restore_delta(bytes).await?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_runtime::IvmJobRegistry;

    use super::*;

    /// A remote handle needs no live coordinator to be constructed, so the
    /// mode-dependent behaviour of the enum is unit-testable.
    fn remote_job() -> IvmJob {
        IvmJob::Remote(RemoteIvmJob::from_job_id(
            "http://127.0.0.1:1",
            "remote-job",
        ))
    }

    fn revenue_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "revenue".into(),
            body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    /// API-A6: asking a remote job for delta-checkpoint accumulation used to
    /// return `Ok(())` and do nothing. It must name the gap instead.
    #[test]
    fn remote_enable_delta_checkpoints_is_rejected_not_ignored() {
        let err = match remote_job().enable_delta_checkpoints() {
            Ok(()) => panic!("remote enable_delta_checkpoints must not report success"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("remote-job"), "error must name the job: {err}");
        assert!(
            err.contains("delta-checkpoint accumulation"),
            "error must name the capability: {err}"
        );
    }

    /// API-A6 / INT-F3: the exactly-once request is the one that must never be
    /// silently dropped — retries are exactly where dedup would have mattered.
    #[test]
    fn remote_enable_input_dedup_is_rejected_not_ignored() {
        let err = match remote_job().enable_input_dedup() {
            Ok(()) => panic!("remote enable_input_dedup must not report success"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("remote-job"), "error must name the job: {err}");
        assert!(
            err.contains("dedup"),
            "error must name the capability: {err}"
        );
    }

    /// INT-F6: `Ok(None)` forever is indistinguishable from "no change this
    /// tick", so a distributed change-feed loop spun forever in silence.
    #[test]
    fn remote_view_output_reports_no_change_feed() {
        let err = match remote_job().view_output("revenue") {
            Ok(_) => panic!("remote view_output must not answer as if it had a change feed"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("remote-job"), "error must name the job: {err}");
        assert!(err.contains("revenue"), "error must name the view: {err}");
    }

    /// The shape of a remote job is unknown to this handle — not `false`.
    #[test]
    fn remote_is_partitioned_is_unknown() {
        assert_eq!(remote_job().is_partitioned().unwrap(), None);
    }

    #[tokio::test]
    async fn embedded_is_partitioned_reports_the_real_shape() {
        let sharded: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(3));
        let job = IvmJob::embedded(&sharded, "agg").unwrap();
        job.register_view(revenue_spec()).await.unwrap();
        assert_eq!(job.is_partitioned().unwrap(), Some(true));

        let single: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(1));
        let job1 = IvmJob::embedded(&single, "agg").unwrap();
        job1.register_view(revenue_spec()).await.unwrap();
        assert_eq!(job1.is_partitioned().unwrap(), Some(false));
    }

    /// The embedded arm still enables both features for real (the fix above
    /// must not have turned the whole surface into an error).
    #[tokio::test]
    async fn embedded_enable_switches_still_work() {
        let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(1));
        let job = IvmJob::embedded(&registry, "j").unwrap();
        job.enable_delta_checkpoints().unwrap();
        job.enable_input_dedup().unwrap();
        job.register_view(revenue_spec()).await.unwrap();
        // A registered view with no tick yet has no change-feed item — `None`,
        // not an error, is the embedded answer.
        assert!(job.view_output("revenue").unwrap().is_none());
    }
}
