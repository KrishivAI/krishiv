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

    /// Create (or attach to) an embedded IVM job **pinned to a single flow**,
    /// so it can host a view-DAG. The embedded twin of
    /// [`remote_unpartitioned`](Self::remote_unpartitioned) — one mechanism for
    /// "pin single" on both sides (IVM-AUD-API-A4). Errors if a job of that
    /// name already exists and is key-partitioned.
    pub fn embedded_unpartitioned(registry: &SharedIvmJobRegistry, name: &str) -> Result<Self> {
        Ok(Self::Embedded(EmbeddedIvmJob::create_unpartitioned(
            registry, name,
        )?))
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

    /// Delete this job: the flow, its state, and (distributed) its durable
    /// snapshot on the coordinator. Returns `false` when there was no such job
    /// to remove, which is not an error.
    ///
    /// **Must be called explicitly.** IVM-AUD-API-A7: Rust has no async `Drop`,
    /// so dropping the handle cannot do this — and before this method existed a
    /// remote job could not be removed at all through any Rust API, so every
    /// distributed `DataFrame::to_incremental` leaked a coordinator job for the
    /// coordinator's lifetime. An embedded job is dropped from its registry
    /// (the private one a `to_incremental` handle owns is freed with the handle
    /// either way; a `Session::ivm` job is not).
    ///
    /// After this, every other handle to the same job id is stale: embedded
    /// calls fail with "no longer exists", remote calls 404 until something
    /// recreates the job.
    pub async fn close(&self) -> Result<bool> {
        Ok(match self {
            Self::Embedded(j) => j.delete(),
            Self::Remote(j) => j.delete().await?,
        })
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

/// Turn a coordinator step response into a [`StepReport`].
///
/// IVM-AUD-API-A5. This used to be two unconditional `Vec::new()`s, and those
/// two vectors are the *only* view-level failure channel there is (a failing
/// view does not make `step` return `Err`), so a distributed caller could not
/// see a failed view at all — a broken view and a healthy one produced
/// byte-identical reports.
///
/// `RemoteStepSummary::view_health` is `None` exactly when there is no signal:
/// a coordinator that predates the `/step` `view_health` field, or a tick the
/// coordinator dispatched to a resident executor — the resident tick result
/// carries per-view output deltas only, so the coordinator's mirror has nothing
/// to report. Both cases become [`ViewHealth::Unreported`] rather than an empty
/// report, because "nobody looked" is not "nothing failed".
fn remote_step_report(job_id: &str, s: krishiv_runtime::RemoteStepSummary) -> StepReport {
    match s.view_health {
        Some(h) => StepReport {
            active_views: s.active_views,
            total_output_rows: s.total_output_rows,
            tick: s.tick,
            degraded_views: h.degraded_views,
            errored_views: h
                .errored_views
                .into_iter()
                .map(|e| super::job::ViewError {
                    view: e.view,
                    kind: remote_view_error_kind(&e.kind),
                    message: match remote_view_error_kind(&e.kind) {
                        // An unrecognised kind must not be lost: keep the
                        // coordinator's own word for it in the message rather
                        // than dropping it on the floor.
                        super::job::ViewErrorKind::Unrecognized => {
                            format!("[{}] {}", e.kind, e.message)
                        }
                        _ => e.message,
                    },
                })
                .collect(),
            view_health: super::job::ViewHealth::Reported,
        },
        None => StepReport {
            active_views: s.active_views,
            total_output_rows: s.total_output_rows,
            tick: s.tick,
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
            view_health: super::job::ViewHealth::Unreported(format!(
                "the coordinator reported no per-view health for remote IVM job '{job_id}': \
                 either it predates the /step view_health field, or this tick ran on a \
                 resident executor whose result carries output deltas only"
            )),
        },
    }
}

/// Map a coordinator-reported failure-kind name onto this crate's enum.
///
/// A name this binary does not know maps to
/// [`ViewErrorKind::Unrecognized`](super::job::ViewErrorKind::Unrecognized) —
/// never onto one of the known kinds. A newer coordinator can name a failure
/// mode that did not exist when this client was built, and calling it
/// `ViewSql` would be a false diagnosis of a real failure.
fn remote_view_error_kind(name: &str) -> super::job::ViewErrorKind {
    use super::job::ViewErrorKind as K;
    match name {
        "operator_apply" => K::OperatorApply,
        "view_sql" => K::ViewSql,
        "publish" => K::Publish,
        "fixpoint_not_converged" => K::FixpointNotConverged,
        _ => K::Unrecognized,
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
                                krishiv_ivm::ViewErrorKind::FixpointNotConverged => {
                                    super::job::ViewErrorKind::FixpointNotConverged
                                }
                            },
                            message: e.message,
                        })
                        .collect(),
                    view_health: super::job::ViewHealth::Reported,
                }
            }
            Self::Remote(j) => remote_step_report(j.job_id(), j.step().await?),
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

    /// IVM-AUD-API-A5. `degraded_views`/`errored_views` are the only view-level
    /// failure channel there is, and this arm used to fill both with
    /// `Vec::new()` unconditionally, so a distributed caller could not see a
    /// failed view at all. A reported health object must arrive intact.
    #[test]
    fn a_reported_remote_failure_reaches_the_step_report() {
        use krishiv_runtime::{RemoteStepSummary, RemoteViewError, RemoteViewHealth};

        let report = super::remote_step_report(
            "remote-job",
            RemoteStepSummary {
                active_views: 1,
                total_output_rows: 3,
                tick: 9,
                view_health: Some(RemoteViewHealth {
                    degraded_views: vec!["slow".into()],
                    errored_views: vec![RemoteViewError {
                        view: "broken".into(),
                        kind: "view_sql".into(),
                        message: "column 'nope' not found".into(),
                    }],
                    degraded_omitted: 0,
                    errored_omitted: 0,
                }),
            },
        );
        assert_eq!(report.tick, 9);
        assert_eq!(report.degraded_views, vec!["slow".to_string()]);
        assert_eq!(report.errored_views.len(), 1);
        assert_eq!(report.errored_views[0].view, "broken");
        assert_eq!(report.errored_views[0].kind, crate::ViewErrorKind::ViewSql);
        assert_eq!(
            report.errored_views[0].message, "column 'nope' not found",
            "a recognised kind must not have the kind name spliced into its message"
        );
        assert_eq!(report.view_health, crate::ViewHealth::Reported);
    }

    /// IVM-AUD-API-A5, the other half: a tick with no health signal — an older
    /// coordinator, or a tick dispatched to a resident executor whose result
    /// wire carries output deltas only — must be reported as *unknown*. Empty
    /// vectors plus `Reported` would be the original lie in a new place.
    #[test]
    fn an_unreported_remote_tick_says_so_instead_of_claiming_health() {
        use krishiv_runtime::RemoteStepSummary;

        let report = super::remote_step_report(
            "remote-job",
            RemoteStepSummary {
                active_views: 2,
                total_output_rows: 7,
                tick: 4,
                view_health: None,
            },
        );
        assert!(report.errored_views.is_empty());
        match &report.view_health {
            crate::ViewHealth::Reported => {
                panic!("a tick nobody reported health for must not claim to be a health report")
            }
            crate::ViewHealth::Unreported(why) => {
                assert!(why.contains("remote-job"), "must name the job: {why}");
            }
        }
        assert!(!report.view_health.is_reported());
    }

    /// A failure kind this binary does not know must stay unknown. Relabelling
    /// it as one of the kinds we do know would be a false diagnosis of a real
    /// failure, and the coordinator's own word for it is kept in the message.
    #[test]
    fn an_unknown_remote_failure_kind_is_not_relabelled() {
        use krishiv_runtime::{RemoteStepSummary, RemoteViewError, RemoteViewHealth};

        let report = super::remote_step_report(
            "remote-job",
            RemoteStepSummary {
                active_views: 0,
                total_output_rows: 0,
                tick: 1,
                view_health: Some(RemoteViewHealth {
                    degraded_views: Vec::new(),
                    errored_views: vec![RemoteViewError {
                        view: "v".into(),
                        kind: "some_future_kind".into(),
                        message: "boom".into(),
                    }],
                    degraded_omitted: 0,
                    errored_omitted: 0,
                }),
            },
        );
        assert_eq!(
            report.errored_views[0].kind,
            crate::ViewErrorKind::Unrecognized
        );
        assert!(
            report.errored_views[0].message.contains("some_future_kind"),
            "the coordinator's name for the kind must survive: {}",
            report.errored_views[0].message
        );
    }

    /// IVM-AUD-API-A4. "Pin this job to a single flow" had two spellings: the
    /// remote path called `create_unpartitioned`, the embedded path built a
    /// whole registry with `with_default_shards(1)`. The second pins every job
    /// in that registry rather than this one, and it stops working the moment
    /// the registry is shared — which is exactly what INT-F15 required. This
    /// asserts the pin survives a registry that would otherwise shard.
    #[tokio::test]
    async fn embedded_unpartitioned_pins_a_job_in_a_sharding_registry() {
        let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(3));

        let pinned = IvmJob::embedded_unpartitioned(&registry, "pinned").unwrap();
        pinned.register_view(revenue_spec()).await.unwrap();
        assert_eq!(
            pinned.is_partitioned().unwrap(),
            Some(false),
            "a pinned job must stay single even though this registry shards by default"
        );

        // Same registry, same view shape, unpinned: this is what the pin is
        // protecting against, so the contrast is part of the claim.
        let sharded = IvmJob::embedded(&registry, "sharded").unwrap();
        sharded.register_view(revenue_spec()).await.unwrap();
        assert_eq!(sharded.is_partitioned().unwrap(), Some(true));
    }

    /// IVM-AUD-INT-F16, embedded half. `Session::ivm` auto-partitions and
    /// `DataFrame::to_incremental` pins single; they now share the session's
    /// registry, so the same name can be asked for in both shapes. The registry
    /// records the pin and no-ops on an existing job, so asking for single
    /// would have handed back a partitioned flow with the pin merely claimed —
    /// and a partitioned flow never cascades to derived views, so the caller's
    /// view-DAG would sit empty with nothing said.
    #[tokio::test]
    async fn pinning_a_name_already_held_by_a_partitioned_job_is_refused() {
        let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(3));
        let sharded = IvmJob::embedded(&registry, "agg").unwrap();
        sharded.register_view(revenue_spec()).await.unwrap();
        assert_eq!(
            sharded.is_partitioned().unwrap(),
            Some(true),
            "precondition"
        );

        let err = match IvmJob::embedded_unpartitioned(&registry, "agg") {
            Ok(job) => panic!(
                "a partitioned job must not be handed back as pinned single; got partitioned={:?}",
                job.is_partitioned()
            ),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("agg"), "the error must name the job: {err}");
        assert!(
            err.contains("partitioned"),
            "the error must say what the conflict is: {err}"
        );
    }

    /// IVM-AUD-API-A7. Before `close()` existed there was no way to remove an
    /// IVM job through this handle at all — remotely the coordinator job
    /// outlived the process, and embedded a `Session::ivm` job stayed in the
    /// session registry forever. The embedded half is the half a unit test can
    /// prove without a coordinator.
    #[tokio::test]
    async fn close_removes_an_embedded_job_from_its_registry() {
        let registry: SharedIvmJobRegistry = Arc::new(IvmJobRegistry::with_default_shards(1));
        let job = IvmJob::embedded(&registry, "closable").unwrap();
        job.register_view(revenue_spec()).await.unwrap();
        assert!(registry.get("closable").is_some(), "precondition");

        assert!(
            job.close().await.unwrap(),
            "the first close removes the job"
        );
        assert!(
            registry.get("closable").is_none(),
            "the job must be gone from the registry, not merely reported gone"
        );
        assert!(
            !job.close().await.unwrap(),
            "closing an already-closed job is false, not an error"
        );
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
