//! HTTP handlers for coordinated batch SQL (synchronous and async submit/poll).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::SharedCoordinator;
use crate::batch_sql::{BatchSqlInlineTable, BatchSqlTable};

#[derive(Debug, Deserialize)]
pub struct BatchSqlRequest {
    pub query: String,
    /// Input tables as inline Arrow IPC (base64-encoded).
    /// Data travels in-band so executor pods need no shared filesystem.
    #[serde(default)]
    pub tables: Vec<BatchSqlInlineTable>,
    /// Input tables as parquet file paths readable by the coordinator and
    /// every executor (shared filesystem or single-node daemon). Plain
    /// SELECTs over path tables are eligible for partition-parallel staged
    /// execution (Phase 52).
    #[serde(default)]
    pub table_paths: Vec<BatchSqlTable>,
    #[serde(default)]
    pub is_streaming: bool,
}

// ── Async submit / poll ────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSqlSubmitResponse {
    pub job_id: String,
}

/// A job whose id has been handed to a client but whose plan is not built yet.
#[derive(Debug, Clone)]
enum Planning {
    InProgress,
    /// Planning failed. Kept so the client's next poll gets the reason instead
    /// of a 404 it cannot distinguish from a job that never existed.
    Failed(String),
    /// A cancel arrived while the plan was still being built.
    Cancelled,
}

/// Record a cancel for a job that is still being planned.
///
/// Returns `true` if this id was in the planning window, meaning the cancel is
/// answered and the planner will discard its result.
///
/// Without this, an id handed out before planning finishes is un-cancellable:
/// `/api/v1/jobs/{id}/cancel` 404s because the coordinator has no such job yet,
/// and the plan then completes and *starts* a query nobody is waiting for. That
/// is the orphaned-job shape that held slots and scratch disk for a whole day
/// and made three different failures look like skew, OOM, and eviction.
pub(crate) fn cancel_if_planning(job_id: &str) -> bool {
    let Some(mut entry) = PLANNING.get_mut(job_id) else {
        return false;
    };
    match entry.0 {
        // Already terminal from the client's point of view; nothing to stop.
        Planning::Failed(_) | Planning::Cancelled => {}
        Planning::InProgress => {
            entry.0 = Planning::Cancelled;
            entry.1 = std::time::Instant::now();
        }
    }
    true
}

/// Jobs between "id issued" and "spec submitted to the coordinator".
///
/// Entries live for the duration of one plan — seconds normally, a minute or
/// two for the largest SF100 shapes — and are removed the moment the real job
/// exists. A planning *failure* is kept until the client polls for it, or
/// until [`prune_planning`] evicts it, so a crashed client cannot leak an entry
/// forever.
///
/// This lives on the HTTP surface rather than in `Coordinator` deliberately:
/// the coordinator's contract is "jobs it has been given a spec for", and a
/// query still being planned has no spec. Adding a second, spec-less job state
/// to the coordinator would put that distinction in front of every scheduler
/// path that iterates jobs.
static PLANNING: std::sync::LazyLock<dashmap::DashMap<String, (Planning, std::time::Instant)>> =
    std::sync::LazyLock::new(dashmap::DashMap::new);

/// How long a planning entry may sit before it is assumed abandoned.
const PLANNING_ENTRY_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

fn prune_planning() {
    let now = std::time::Instant::now();
    PLANNING.retain(|_, (_, at)| now.duration_since(*at) < PLANNING_ENTRY_TTL);
}

/// `POST /api/v1/batch-sql/submit` — submit a batch SQL job and return
/// immediately with the job id.  Poll `GET /api/v1/batch-sql/{job_id}` for
/// results.  The coordinator's background orchestration loop drives task
/// dispatch; this handler never blocks waiting for the job to complete.
///
/// # Why planning moved off this handler
///
/// The paragraph above was already the documented contract, and it was already
/// false: the handler awaited `submit_batch_sql_job_with_paths`, which *plans*
/// the query — logical plan, optimiser, physical plan, stage cutting, and a
/// proto round-trip rehearsal per stage. That cost scales with plan complexity
/// and input size, not with the network, so the endpoint's latency is a
/// property of the query rather than of the transport.
///
/// At SF100 that bit twice in one sweep. TPC-H q11 and q15 each duplicate a
/// large aggregate inside a scalar subquery, took ~65 s to plan, and blew the
/// client's 60 s socket timeout. Both were recorded as failures while the
/// coordinator went on to plan and could have run them. Worse, a client that
/// retries a submit timeout starts a *second* plan of the same query alongside
/// the first, on the coordinator that was already the reason the first was
/// slow — q15 was once observed with five overlapping plans.
///
/// So the id is minted first and handed back immediately; planning runs in a
/// spawned task and the query is pollable from that moment. A plan that fails
/// surfaces on the poll as a failed job carrying the planning error, which is
/// what a caller can act on — not a socket timeout, which is indistinguishable
/// from an unreachable coordinator.
pub async fn api_batch_sql_submit(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<BatchSqlRequest>,
) -> Result<Json<BatchSqlSubmitResponse>, StatusCode> {
    if body.query.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    prune_planning();
    let job_id = crate::batch_sql::next_batch_sql_job_id().map_err(|e| {
        tracing::error!(error = ?e, "could not mint a batch SQL job id");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let key = job_id.as_str().to_owned();
    let response_id = key.clone();
    PLANNING.insert(
        key.clone(),
        (Planning::InProgress, std::time::Instant::now()),
    );

    tokio::spawn(async move {
        let started = std::time::Instant::now();
        let result = crate::batch_sql::plan_and_submit_batch_sql_job(
            &coordinator,
            job_id.clone(),
            &body.query,
            &body.tables,
            &body.table_paths,
            body.is_streaming,
        )
        .await;
        let planning_ms = started.elapsed().as_millis();
        match result {
            Ok(()) => {
                // A cancel that arrived mid-plan has to be honoured now: the
                // job exists as of the line above, and leaving it would start a
                // query the client has already given up on.
                let cancelled = matches!(
                    PLANNING.get(&key).map(|e| e.0.clone()),
                    Some(Planning::Cancelled)
                );
                // The coordinator owns the job now, so the placeholder must go
                // before any poll can see both and disagree.
                PLANNING.remove(&key);
                if cancelled {
                    tracing::info!(
                        job_id = %key,
                        planning_ms,
                        "batch SQL job was cancelled while planning; cancelling the submitted job"
                    );
                    if let Err(error) = coordinator.cancel_job_and_notify(&job_id).await {
                        tracing::warn!(
                            job_id = %key,
                            %error,
                            "could not cancel a job that was cancelled during planning"
                        );
                    }
                    return;
                }
                tracing::info!(
                    job_id = %key,
                    planning_ms,
                    "batch SQL job planned and submitted"
                );
            }
            Err(e) => {
                tracing::error!(
                    job_id = %key,
                    planning_ms,
                    error = ?e,
                    "batch SQL planning failed"
                );
                PLANNING.insert(
                    key,
                    (Planning::Failed(e.to_string()), std::time::Instant::now()),
                );
            }
        }
    });

    Ok(Json(BatchSqlSubmitResponse {
        job_id: response_id,
    }))
}

#[derive(Debug, Serialize)]
pub struct BatchSqlPollResponse {
    pub job_id: String,
    pub state: String,
    /// Present when state == "Succeeded". Base64 strings on the JSON wire
    /// (see `krishiv_proto::serde_ipc_b64`).
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        with = "krishiv_proto::serde_ipc_b64"
    )]
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
    /// Present when state == "Failed" or "Cancelled".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How many stages the job was planned into.
    ///
    /// This is the client's only way to tell a distributed run from one that
    /// quietly degraded to a single task. Staging is declined for many
    /// legitimate reasons (DDL, unsupported plan shapes) and one illegitimate
    /// one — a planning bug — and in every case the query still returns
    /// correct rows. A caller benchmarking a cluster, or an operator wondering
    /// why one node is hot, needs the execution shape as *data*, not as a log
    /// line they have to know to go looking for.
    pub stage_count: usize,
    /// Total tasks across all stages. `stage_count == 1 && task_count == 1`
    /// means the whole query ran on one executor.
    pub task_count: usize,
}

/// `GET /api/v1/batch-sql/{job_id}` — poll a submitted batch SQL job.
///
/// Returns the current state.  When `state == "Succeeded"` the inline IPC
/// result batches are included and consumed (subsequent calls return empty).
pub async fn api_batch_sql_poll(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id_str): Path<String>,
) -> Result<Json<BatchSqlPollResponse>, StatusCode> {
    use krishiv_proto::JobState;
    let job_id = krishiv_proto::JobId::try_new(&job_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;

    // A job whose id has been issued but whose plan is not built yet is not
    // "unknown" — the client is holding an id this coordinator promised. It
    // used to 404, which a caller cannot distinguish from a job that never
    // existed or one already reaped, so this is checked before the coordinator
    // is consulted.
    if let Some(entry) = PLANNING.get(&job_id_str) {
        let planning = entry.value().0.clone();
        drop(entry);
        return Ok(Json(match planning {
            Planning::InProgress => BatchSqlPollResponse {
                job_id: job_id_str,
                state: "Running".to_string(),
                inline_record_batch_ipc: Vec::new(),
                error: None,
                // Not "one stage" — nothing has been planned yet. Reporting
                // zero keeps a caller that watches the shape (the benchmark
                // harness reads exactly this to tell distributed from
                // single-task) from recording a plan that does not exist.
                stage_count: 0,
                task_count: 0,
            },
            Planning::Failed(error) => BatchSqlPollResponse {
                job_id: job_id_str,
                state: "Failed".to_string(),
                inline_record_batch_ipc: Vec::new(),
                error: Some(error),
                stage_count: 0,
                task_count: 0,
            },
            Planning::Cancelled => BatchSqlPollResponse {
                job_id: job_id_str,
                state: "Cancelled".to_string(),
                inline_record_batch_ipc: Vec::new(),
                error: None,
                stage_count: 0,
                task_count: 0,
            },
        }));
    }

    // Read the execution shape and any task-level failure reason in the same
    // borrow as the state, so the response describes one consistent snapshot.
    let (state, stage_count, task_count, failure_reason) = {
        let coord = coordinator.read().await;
        let snapshot = coord
            .job_snapshot(&job_id)
            .map_err(|_| StatusCode::NOT_FOUND)?;
        // The real reason a job failed lives on the task that failed. Returning
        // a bare "job failed" forced every caller to go read executor logs to
        // learn anything at all — which is how a plain "No suitable object
        // store found for s3://" stayed invisible behind a generic string.
        let failure_reason = coord.job_detail_snapshot(&job_id).ok().and_then(|detail| {
            detail
                .stages()
                .iter()
                .flat_map(|stage| stage.tasks())
                .find_map(|task| task.last_failure_reason().map(str::to_owned))
        });
        (
            snapshot.state(),
            snapshot.stage_count(),
            snapshot.task_count(),
            failure_reason,
        )
    };

    let resp = match state {
        JobState::Succeeded => {
            let (mut batches, spools) = {
                let mut coord = coordinator.write().await;
                (
                    coord.take_job_inline_results(&job_id).unwrap_or_default(),
                    coord.take_job_result_spools(&job_id),
                )
            };
            // Phase 2.10: re-encode disk-spooled results as inline IPC for
            // this HTTP polling surface (the spool file IS one IPC stream).
            for spool in &spools {
                match std::fs::read(spool.path()) {
                    Ok(bytes) => batches.push(bytes),
                    Err(e) => {
                        tracing::error!(error = %e, "cannot read result spool for HTTP poll");
                    }
                }
            }
            BatchSqlPollResponse {
                job_id: job_id_str,
                state: "Succeeded".into(),
                inline_record_batch_ipc: batches,
                error: None,
                stage_count,
                task_count,
            }
        }
        JobState::Failed => BatchSqlPollResponse {
            job_id: job_id_str,
            state: "Failed".into(),
            inline_record_batch_ipc: vec![],
            // Prefer the failing task's own message; fall back to the generic
            // string only when no task recorded a reason.
            error: Some(
                failure_reason
                    .unwrap_or_else(|| String::from("job failed (no task reported a reason)")),
            ),
            stage_count,
            task_count,
        },
        JobState::Cancelled => BatchSqlPollResponse {
            job_id: job_id_str,
            state: "Cancelled".into(),
            inline_record_batch_ipc: vec![],
            error: Some("job was cancelled".into()),
            stage_count,
            task_count,
        },
        s => BatchSqlPollResponse {
            job_id: job_id_str,
            state: format!("{s:?}"),
            inline_record_batch_ipc: vec![],
            error: None,
            stage_count,
            task_count,
        },
    };
    Ok(Json(resp))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod planning_tests {
    use super::*;

    /// Each test uses its own key: `PLANNING` is process-wide, and tests run in
    /// parallel threads of one process.
    fn key(name: &str) -> String {
        format!("batch-sql-planning-test-{name}")
    }

    fn state_of(k: &str) -> Option<Planning> {
        PLANNING.get(k).map(|e| e.0.clone())
    }

    /// A cancel that arrives while a plan is still being built must be
    /// *answered*, not 404'd.
    ///
    /// The submit endpoint hands back an id before the coordinator has a spec,
    /// so there is a window where `/api/v1/jobs/{id}/cancel` finds no such job.
    /// Returning 404 there is not merely unhelpful: the plan then finishes and
    /// starts a query nobody is waiting for, holding slots and scratch disk.
    /// That is the orphaned-job shape that made three separate failures look
    /// like skew, OOM, and eviction for a whole day.
    #[test]
    fn a_cancel_during_planning_is_answered_and_recorded() {
        let k = key("cancel");
        PLANNING.insert(k.clone(), (Planning::InProgress, std::time::Instant::now()));

        assert!(
            cancel_if_planning(&k),
            "a cancel for an id inside the planning window must be answered"
        );
        assert!(
            matches!(state_of(&k), Some(Planning::Cancelled)),
            "the planner must be able to see that its result is unwanted"
        );
        PLANNING.remove(&k);
    }

    /// An id this coordinator never issued must still be unknown — the cancel
    /// route falls through to the coordinator, which is the only thing that can
    /// answer for a real job.
    #[test]
    fn a_cancel_for_an_unknown_id_is_not_claimed() {
        assert!(!cancel_if_planning(&key("never-issued")));
    }

    /// Cancelling twice, or cancelling something already failed, must not
    /// resurrect it into a different state.
    #[test]
    fn cancelling_a_terminal_planning_entry_leaves_it_alone() {
        let k = key("terminal");
        PLANNING.insert(
            k.clone(),
            (Planning::Failed("boom".into()), std::time::Instant::now()),
        );
        assert!(cancel_if_planning(&k), "the id is still ours to answer for");
        assert!(
            matches!(state_of(&k), Some(Planning::Failed(ref e)) if e == "boom"),
            "a failure reason must survive a late cancel — it is what the client polls for"
        );
        PLANNING.remove(&k);
    }

    /// A client that dies between submit and poll must not leak an entry.
    ///
    /// Planning failures are held for the client to collect, so without a TTL
    /// they accumulate for the life of the process.
    #[test]
    fn abandoned_planning_entries_are_pruned_by_age() {
        let fresh = key("prune-fresh");
        let stale = key("prune-stale");
        PLANNING.insert(
            fresh.clone(),
            (Planning::InProgress, std::time::Instant::now()),
        );
        PLANNING.insert(
            stale.clone(),
            (
                Planning::Failed("old".into()),
                std::time::Instant::now()
                    - (PLANNING_ENTRY_TTL + std::time::Duration::from_secs(1)),
            ),
        );

        prune_planning();

        assert!(
            state_of(&fresh).is_some(),
            "an in-flight plan must survive a prune"
        );
        assert!(
            state_of(&stale).is_none(),
            "an entry older than the TTL must be evicted"
        );
        PLANNING.remove(&fresh);
    }
}
