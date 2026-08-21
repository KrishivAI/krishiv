//! gRPC service types for the executor task assignment protocol.

pub const EXECUTOR_TASK_BEARER_TOKEN_ENV: &str = "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN";
pub const REQUIRE_EXECUTOR_TASK_AUTH_ENV: &str = "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH";

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_proto::{
    ExecutorTaskAssignment, ExecutorTaskService, TaskStatusResponse, TransportDisposition,
    TransportVersion, wire,
};

use crate::{AssignmentPushOutcome, ExecutorAssignmentInbox, ExecutorError};

/// Shared map of per-job stateful window executors for continuous streaming.
///
/// Keyed by job-id. Shared between `ExecutorTaskInboxService` (for
/// `push_continuous_input` / `drain_continuous_output`) and `ExecutorTaskRunner`
/// (for `stream:loop:` fragment execution).
pub type SharedLoopExecutors = Arc<DashMap<String, Arc<Mutex<ContinuousWindowExecutor>>>>;

/// Shared per-job input buffer for continuous streaming tasks.
///
/// `push_continuous_input` appends decoded batches here; `drain_continuous_output`
/// drains and processes them through the matching loop executor.
pub type SharedContinuousInputs = Arc<DashMap<String, Vec<RecordBatch>>>;

/// Reserved pseudo-task id carrying the run-loop end-of-stream directive
/// through `push_continuous_input`. Real subtask ids are
/// `task-streaming-<n>`, so this cannot collide with one.
pub const RUN_LOOP_EOS_TASK_ID: &str = "stream-eos";

/// The three class-specific run-loop state maps (task #147): two-source
/// joins, join→agg pipelines, and stateless SQL executors. Bundled into one
/// value so every seam that shares run-loop state shares ALL of it: the
/// cancel path must retire state for every class, and a class whose map is
/// not shared with the gRPC service keeps its loop alive after deregister —
/// the executor slot then leaks forever (the 3-node k3s wedge, 2026-08-21,
/// where each executor ran exactly `slots` jobs and then never another).
#[derive(Clone, Default)]
pub struct SharedClassExecutors {
    /// `stream:rjoin:` two-source join operators, keyed by `rloop_state_key`.
    pub join: Arc<DashMap<String, Arc<Mutex<krishiv_dataflow::WatermarkWindowJoinOperator>>>>,
    /// `stream:rpipe:` join→agg pipelines, keyed by `rloop_state_key`.
    pub pipeline:
        Arc<DashMap<String, Arc<tokio::sync::Mutex<krishiv_dataflow::pipeline::JoinAggPipeline>>>>,
    /// `stream:rbatch:` stateless executors, keyed by `rloop_state_key`.
    pub stateless: Arc<
        DashMap<
            String,
            Arc<tokio::sync::Mutex<krishiv_sql::stateless_exec::StatelessBatchExecutor>>,
        >,
    >,
}

/// Executor-side task assignment service backed by an in-memory inbox.
///
/// `Debug` is hand-written rather than derived: `SharedContinuousConnectorSources`
/// holds `dyn DynSource`, which is not `Debug`.
#[derive(Clone)]
pub struct ExecutorTaskInboxService {
    inbox: ExecutorAssignmentInbox,
    /// Per-job stateful window executors — shared with the task runner.
    pub(crate) loop_executors: SharedLoopExecutors,
    /// Per-job pending input batches for distributed continuous push.
    pub(crate) continuous_inputs: SharedContinuousInputs,
    /// Phase 55: per-job run-loop egress buffers — shared with the task runner.
    pub(crate) continuous_outputs: crate::runner::SharedContinuousOutputs,
    /// Phase 55: per-buffer-key input notifies — shared with the task runner
    /// so a push wakes a blocked run-loop within microseconds.
    pub(crate) input_notify: crate::runner::SharedContinuousNotify,
    /// Connector-source cache — shared with the task runner so a cancelled
    /// job's source READ POSITIONS die with its window state.
    pub(crate) continuous_connector_sources: crate::runner::SharedContinuousConnectorSources,
    /// Class-specific run-loop state (join/pipeline/stateless) — shared with
    /// the task runner so cancel retires EVERY class's state, not just
    /// windows.
    pub(crate) class_executors: SharedClassExecutors,
    /// Per-job egress notifies (task #149 fix 12): staged run-loop output
    /// wakes a long-polling drain instead of the caller busy-polling.
    pub(crate) egress_notify: crate::runner::SharedContinuousNotify,
}

impl std::fmt::Debug for ExecutorTaskInboxService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorTaskInboxService")
            .field("loop_executors", &self.loop_executors.len())
            .field("continuous_inputs", &self.continuous_inputs.len())
            .field("continuous_outputs", &self.continuous_outputs.len())
            .field("input_notify", &self.input_notify.len())
            .field(
                "continuous_connector_sources",
                &self.continuous_connector_sources.len(),
            )
            .field("class_join_executors", &self.class_executors.join.len())
            .finish_non_exhaustive()
    }
}

impl ExecutorTaskInboxService {
    /// Create a task assignment service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inbox,
            loop_executors: Arc::new(DashMap::new()),
            continuous_inputs: Arc::new(DashMap::new()),
            continuous_outputs: Arc::new(DashMap::new()),
            input_notify: Arc::new(DashMap::new()),
            continuous_connector_sources: Arc::new(DashMap::new()),
            class_executors: SharedClassExecutors::default(),
            egress_notify: Arc::new(DashMap::new()),
        }
    }

    /// Create a task assignment service that shares state with an existing runner.
    pub fn new_with_continuous(
        inbox: ExecutorAssignmentInbox,
        loop_executors: SharedLoopExecutors,
        continuous_inputs: SharedContinuousInputs,
    ) -> Self {
        Self {
            inbox,
            loop_executors,
            continuous_inputs,
            continuous_outputs: Arc::new(DashMap::new()),
            input_notify: Arc::new(DashMap::new()),
            continuous_connector_sources: Arc::new(DashMap::new()),
            class_executors: SharedClassExecutors::default(),
            egress_notify: Arc::new(DashMap::new()),
        }
    }

    /// Share the run-loop egress buffers and input notifies with the runner
    /// (Phase 55: push wakes the run-loop; drain serves its egress buffer).
    #[must_use]
    pub fn with_run_loop_state(
        mut self,
        continuous_outputs: crate::runner::SharedContinuousOutputs,
        input_notify: crate::runner::SharedContinuousNotify,
    ) -> Self {
        self.continuous_outputs = continuous_outputs;
        self.input_notify = input_notify;
        self
    }

    /// Share the connector-source cache with the runner — see
    /// [`ExecutorTaskRunner::with_shared_continuous_connector_sources`].
    #[must_use]
    pub fn with_continuous_connector_sources(
        mut self,
        sources: crate::runner::SharedContinuousConnectorSources,
    ) -> Self {
        self.continuous_connector_sources = sources;
        self
    }

    /// Flush every run-loop operator this process hosts for `job_id` into
    /// the job's egress buffer: windows and pipelines emit their open state
    /// as final. Serves the RUN_LOOP_EOS_TASK_ID directive; see the handler.
    async fn flush_run_loop_job(&self, job_id: &str) -> Result<usize, tonic::Status> {
        let prefix = format!("{job_id}#");
        let mut outputs: Vec<RecordBatch> = Vec::new();
        for entry in self.loop_executors.iter() {
            if !(entry.key() == job_id || entry.key().starts_with(&prefix)) {
                continue;
            }
            let mut exec = entry.value().lock().map_err(|_| {
                tonic::Status::internal("run-loop window executor lock poisoned during EOS flush")
            })?;
            outputs.extend(exec.flush_all().map_err(|e| {
                tonic::Status::internal(format!("EOS flush of window state failed: {e}"))
            })?);
        }
        let pipelines: Vec<_> = self
            .class_executors
            .pipeline
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| Arc::clone(e.value()))
            .collect();
        for pipe in pipelines {
            let mut pipe = pipe.lock().await;
            outputs.extend(pipe.flush_all().map_err(|e| {
                tonic::Status::internal(format!("EOS flush of pipeline state failed: {e}"))
            })?);
        }
        // Joins emit matches eagerly and the stateless class holds no window
        // state — nothing to flush for those maps.
        if outputs.is_empty() {
            return Ok(0);
        }
        let flushed = outputs.len();
        // Deliberately NOT capped (task #149 fix 3): the flush MOVES bytes
        // from operator state (freed by flush_all) into the egress buffer, so
        // staging all of it is memory-neutral — while truncating it destroyed
        // computed final output (observed live: q9's whole result kept
        // exactly cap batches). Drain is paged, so the consumer collects the
        // full flush across successive calls without any oversized response.
        self.continuous_outputs
            .entry(job_id.to_owned())
            .or_default()
            .extend(outputs);
        if let Some(notify) = self.egress_notify.get(job_id) {
            notify.notify_waiters();
        }
        Ok(flushed)
    }

    /// Share the per-job egress notify map with the runner, so staged output
    /// wakes long-polling drains (task #149 fix 12).
    #[must_use]
    pub fn with_egress_notify(mut self, notify: crate::runner::SharedContinuousNotify) -> Self {
        self.egress_notify = notify;
        self
    }

    /// Share the class-specific run-loop state maps with the runner — see
    /// [`SharedClassExecutors`]. Without this, cancelling a join/pipeline/
    /// stateless job cannot retire its state or stop its loop.
    #[must_use]
    pub fn with_class_executors(mut self, class_executors: SharedClassExecutors) -> Self {
        self.class_executors = class_executors;
        self
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }
}

/// Authentication settings for the executor task-control gRPC API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskAuthConfig {
    require_auth: bool,
    bearer_token: Option<String>,
}

impl ExecutorTaskAuthConfig {
    /// Build auth config from process environment.
    pub fn from_env() -> Self {
        Self {
            require_auth: parse_bool_env(REQUIRE_EXECUTOR_TASK_AUTH_ENV),
            bearer_token: configured_executor_task_bearer_token(),
        }
    }

    /// Build auth config directly for tests and embedders.
    pub fn new(require_auth: bool, bearer_token: Option<String>) -> Self {
        Self {
            require_auth,
            bearer_token: bearer_token
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty()),
        }
    }

    /// Whether the process must fail closed if no bearer token is configured.
    pub fn require_auth(&self) -> bool {
        self.require_auth
    }

    /// Whether a non-empty bearer token is configured.
    pub fn has_bearer_token(&self) -> bool {
        self.bearer_token.is_some()
    }

    /// The configured bearer token, if any.
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    /// Validate the required-auth startup contract.
    pub fn validate_required(&self) -> crate::ExecutorResult<()> {
        if self.require_auth && self.bearer_token.is_none() {
            return Err(crate::ExecutorError::LocalExecution {
                message: format!(
                    "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
                ),
            });
        }
        Ok(())
    }
}

fn parse_bool_env(name: &str) -> bool {
    krishiv_common::truthy_env(name)
}

fn configured_executor_task_bearer_token() -> Option<String> {
    std::env::var(EXECUTOR_TASK_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

pub fn bearer_token_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<&str> {
    krishiv_common::bearer_token(metadata.get("authorization").and_then(|v| v.to_str().ok()))
}

#[tonic::async_trait]
impl ExecutorTaskService for ExecutorTaskInboxService {
    async fn assign_task(
        &self,
        request: tonic::Request<ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let assignment = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(assignment.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                assignment.version(),
                TransportVersion::CURRENT
            )));
        }

        match self.inbox.push_with_outcome(assignment) {
            Ok(AssignmentPushOutcome::Enqueued) => Ok(tonic::Response::new(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )),
            Ok(AssignmentPushOutcome::Duplicate) => Ok(tonic::Response::new(
                TaskStatusResponse::new(TransportDisposition::Duplicate),
            )),
            Err(ExecutorError::AssignmentQueueFull { current, max }) => {
                // Proper backpressure signal to the coordinator.
                Err(tonic::Status::resource_exhausted(format!(
                    "executor assignment queue full (current={current}, max={max})"
                )))
            }
            Err(other) => Err(krishiv_metrics::grpc::internal_status(
                "handle task assignment",
                &other,
            )),
        }
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<krishiv_proto::task::TaskCancellationRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let request = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(request.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                request.version(),
                TransportVersion::CURRENT
            )));
        }
        let removed = self
            .inbox
            .cancel_task(request.job_id(), request.task_id())
            .map_err(|error| krishiv_metrics::grpc::internal_status("cancel task", &error))?;
        // A cancel for a job with a registered `stream:loop` executor is a
        // continuous-job teardown (the only producer of continuous cancels is
        // job-level deregister/cancel). Retire the whole job identity on this
        // process: drop the stateful window executor and buffered inputs,
        // purge the inbox's dedupe entries, and clear the task tombstone — a
        // later *recreated* job legitimately reuses the same deterministic
        // ids (`task-streaming`, attempts from 1) and must be treated as a
        // fresh incarnation, not swallowed as an at-least-once duplicate or
        // insta-cancelled by the stale tombstone.
        let job_id = request.job_id();
        // Run-loop subtasks key their state by `{job}#…`; a cancel for the
        // job (or any of its subtasks) retires the whole composite family.
        let rloop_prefix = format!("{}#", job_id.as_str());
        let composite_keys: Vec<String> = self
            .loop_executors
            .iter()
            .filter(|e| e.key().starts_with(&rloop_prefix))
            .map(|e| e.key().clone())
            .collect();
        let had_cycle_executor = self.loop_executors.remove(job_id.as_str()).is_some();
        // The classed run-loop families (join/pipeline/stateless) key their
        // state by the SAME `{job}#{subtask}` scheme. Retire all of them: a
        // class left out of this purge keeps its state across incarnations
        // AND keeps its loop alive after deregister (each loop exits by
        // observing its own state entry disappear), leaking the runner slot
        // it occupies forever.
        let mut classed_subtasks = 0usize;
        macro_rules! purge_class_map {
            ($map:expr) => {{
                let keys: Vec<String> = $map
                    .iter()
                    .filter(|e| e.key().starts_with(&rloop_prefix))
                    .map(|e| e.key().clone())
                    .collect();
                classed_subtasks += keys.len();
                for key in &keys {
                    $map.remove(key);
                }
            }};
        }
        purge_class_map!(self.class_executors.join);
        purge_class_map!(self.class_executors.pipeline);
        purge_class_map!(self.class_executors.stateless);
        let had_rloop = !composite_keys.is_empty() || classed_subtasks > 0;
        for key in &composite_keys {
            self.loop_executors.remove(key);
        }
        if had_cycle_executor || had_rloop {
            self.continuous_inputs.remove(job_id.as_str());
            self.continuous_inputs
                .retain(|k, _| !k.starts_with(&rloop_prefix));
            // Undrained egress is computed output that no consumer ever saw.
            // Destroying it is the right call — the job is gone and nothing
            // will ever drain it — but it used to be destroyed *silently*,
            // which is the same shape as the ring's drop-oldest overflow that
            // `continuous_egress_dropped` exists to make visible. Count it on
            // the same meter so teardown loss is not the one path the counter
            // cannot see.
            let undrained = self
                .continuous_outputs
                .remove(job_id.as_str())
                .map(|(_, batches)| batches.len())
                .unwrap_or(0);
            if undrained > 0 {
                tracing::warn!(
                    job_id = %job_id,
                    undrained_batches = undrained,
                    "continuous job cancelled with output still buffered; those batches were \
                     computed but never drained and are now discarded"
                );
            }
            // Retire the source read positions with the window state.
            //
            // Cancel dropped `loop_executors` (the state) but left the
            // connector-source cache alone, and only the RESTORE path ever
            // cleared it. Those entries hold each source's ADVANCED read
            // position, so a same-process re-register of the same job id
            // resumed reading where the dead incarnation stopped — against
            // empty window state. Every event between the last checkpoint and
            // the cancel was skipped, with no error and a job that looked
            // healthy. State and position have to be retired together, or each
            // makes the other lie.
            //
            // Run-loop jobs additionally retire their sink handle and loss
            // counters in their own fragment teardown, which runs after the
            // loop has stopped touching them (see
            // `ExecutorTaskRunner::retire_continuous_job_state`). Doing that
            // here would race a loop that has not yet observed its tombstone.
            let cycle_prefix = format!("{job_id}|");
            let source_prefix = format!("{job_id}#");
            self.continuous_connector_sources
                .retain(|k, _| !k.starts_with(&cycle_prefix) && !k.starts_with(&source_prefix));
            // Wake any run-loop blocked in its idle wait so it observes the
            // cancellation immediately instead of on the fallback tick, then
            // drop the notify entries.
            for entry in self.input_notify.iter() {
                if entry.key() == job_id.as_str() || entry.key().starts_with(&rloop_prefix) {
                    entry.value().notify_waiters();
                }
            }
            self.input_notify
                .retain(|k, _| k != job_id.as_str() && !k.starts_with(&rloop_prefix));
            let purged = self.inbox.forget_job(job_id).map_err(|error| {
                krishiv_metrics::grpc::internal_status("forget cancelled job", &error)
            })?;
            // Run-loop tasks poll `is_task_cancelled` to exit — their
            // tombstone is cleared by the loop itself after it stops, so only
            // cycle-model tombstones are cleared eagerly here.
            if had_cycle_executor && !had_rloop {
                self.inbox
                    .clear_cancelled_task(job_id, request.task_id())
                    .map_err(|error| {
                        krishiv_metrics::grpc::internal_status(
                            "clear cancelled task tombstone",
                            &error,
                        )
                    })?;
            }
            tracing::debug!(
                job_id = %job_id,
                purged_dedupe_entries = purged,
                run_loop_subtasks = composite_keys.len() + classed_subtasks,
                "continuous job cancelled — stateful executors dropped and inbox identity retired"
            );
        }
        let response = if removed {
            TaskStatusResponse::new(TransportDisposition::Accepted)
        } else {
            TaskStatusResponse::new(TransportDisposition::UnknownTask)
                .with_message("task is not queued on this executor")
        };
        Ok(tonic::Response::new(response))
    }

    async fn push_continuous_input(
        &self,
        request: tonic::Request<krishiv_proto::task::PushContinuousInputRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let req = request.into_inner();
        let job_id = req.job_id.as_str().to_owned();

        // Reserved task id: an end-of-stream directive for run-loop jobs
        // (task #147). A bounded producer that has pushed its last batch is
        // the ONLY party that can know the stream is over — the same
        // principle as cycle mode's `stream-eos:` input partition, carried
        // through the same channel a push already reaches this executor by.
        // The flush locks each of the job's class operators (the loops hold
        // the same locks while processing, so this serializes cleanly) and
        // stages every open window into the egress buffer for the next
        // drain. Real subtask ids are `task-streaming-<n>`; "stream-eos"
        // cannot collide.
        if req.task_id.as_str() == RUN_LOOP_EOS_TASK_ID {
            let flushed = self.flush_run_loop_job(&job_id).await?;
            tracing::info!(
                job_id = %job_id,
                flushed_batches = flushed,
                "run-loop end-of-stream flush staged open windows into egress"
            );
            return Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )));
        }

        // Decode Arrow IPC bytes into RecordBatches.
        let batches = decode_ipc_batches(&req.ipc_bytes)?;

        // Phase 55: a push addressed at a registered run-loop subtask buffer
        // (`{job}#{task}` — the keyed-exchange path) lands task-scoped;
        // everything else keeps the per-job buffer (cycle model + external
        // ingest, which any subtask may claim and re-route by key group).
        let task_key = format!("{job_id}#{}", req.task_id.as_str());
        let buffer_key = if self.input_notify.contains_key(&task_key) {
            task_key
        } else {
            job_id.clone()
        };

        // Enforce per-buffer capacity to prevent unbounded memory growth (M1).
        // Dialable (task #149 fix 11): the hardcoded 64 pinned sustained
        // push throughput at drain-rate x 64 with no operator recourse.
        let cap = krishiv_common::streaming_dials::rloop_input_buffer_cap();
        {
            let mut entry = self
                .continuous_inputs
                .entry(buffer_key.clone())
                .or_default();
            if entry.len() + batches.len() > cap {
                return Err(tonic::Status::resource_exhausted(format!(
                    "continuous input buffer for job {job_id} exceeded capacity ({cap}); \
                     slow down the producer, raise KRISHIV_RLOOP_INPUT_BUFFER_CAP, or \
                     increase the drain rate"
                )));
            }
            entry.extend(batches);
        }
        // Wake a blocked run-loop within microseconds of arrival.
        if let Some(notify) = self.input_notify.get(&buffer_key) {
            notify.notify_waiters();
        }
        if buffer_key != job_id
            && let Some(notify) = self.input_notify.get(&job_id)
        {
            notify.notify_waiters();
        }

        Ok(tonic::Response::new(TaskStatusResponse::new(
            TransportDisposition::Accepted,
        )))
    }

    async fn drain_continuous_output(
        &self,
        request: tonic::Request<krishiv_proto::task::DrainContinuousOutputRequest>,
    ) -> Result<tonic::Response<krishiv_proto::task::DrainContinuousOutputResponse>, tonic::Status>
    {
        use krishiv_proto::TransportDisposition;

        let req = request.into_inner();
        let job_id = req.job_id.as_str();

        // Long-poll (task #149 fix 12): an empty egress buffer parks the
        // drain on the job's egress notify up to `wait_ms` instead of
        // returning empty for the caller to busy-poll. The notify is armed
        // BEFORE the emptiness re-check so an append between check and park
        // cannot be missed.
        if req.wait_ms > 0 {
            let empty = self
                .continuous_outputs
                .get(job_id)
                .map(|e| e.is_empty())
                .unwrap_or(true);
            if empty {
                let notify = self
                    .egress_notify
                    .entry(job_id.to_owned())
                    .or_default()
                    .clone();
                let notified = notify.notified();
                tokio::pin!(notified);
                let still_empty = self
                    .continuous_outputs
                    .get(job_id)
                    .map(|e| e.is_empty())
                    .unwrap_or(true);
                if still_empty {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_millis(req.wait_ms.min(60_000)),
                        notified,
                    )
                    .await;
                }
            }
        }

        // Phase 55: run-loop jobs emit into a per-job egress buffer as they
        // run — drain serves (and clears) it without driving any execution.
        if let Some(mut egress) = self.continuous_outputs.get_mut(job_id) {
            // Paged (task #149 fix 3): one drain call returns at most a page
            // and leaves the remainder for the next call, instead of encoding
            // the entire buffer into a single gRPC response. Callers already
            // poll drain in a loop; an EOS flush larger than one page now
            // arrives across successive calls instead of being truncated by
            // the ring cap (or blowing the gRPC message ceiling).
            let page = krishiv_common::streaming_dials::rloop_egress_cap();
            let take = egress.len().min(page);
            let batches: Vec<RecordBatch> = egress.drain(..take).collect();
            drop(egress);
            let ipc_bytes = encode_ipc_batches(&batches).map_err(|e| {
                krishiv_metrics::grpc::internal_status("encode continuous output", &e)
            })?;
            return Ok(tonic::Response::new(
                krishiv_proto::task::DrainContinuousOutputResponse {
                    version: krishiv_proto::TransportVersion::CURRENT,
                    disposition: TransportDisposition::Accepted,
                    ipc_bytes,
                },
            ));
        }

        // Check executor FIRST to avoid losing input batches on early return.
        let executor_entry = match self.loop_executors.get(job_id) {
            Some(e) => e,
            None => {
                return Ok(tonic::Response::new(
                    krishiv_proto::task::DrainContinuousOutputResponse {
                        version: krishiv_proto::TransportVersion::CURRENT,
                        disposition: TransportDisposition::UnknownTask,
                        ipc_bytes: vec![],
                    },
                ));
            }
        };
        let executor_arc = executor_entry.value().clone();
        drop(executor_entry);

        // Now safe to consume pending input batches.
        let input_batches = self
            .continuous_inputs
            .remove(job_id)
            .map(|(_, v)| v)
            .unwrap_or_default();

        let output_batches = {
            let mut exec = executor_arc
                .lock()
                .map_err(|_| tonic::Status::internal("loop executor lock poisoned"))?;
            exec.drain(input_batches).map_err(|e| {
                krishiv_metrics::grpc::internal_status("drain continuous executor", &e)
            })?
        };

        let ipc_bytes = encode_ipc_batches(&output_batches)
            .map_err(|e| krishiv_metrics::grpc::internal_status("encode continuous output", &e))?;

        Ok(tonic::Response::new(
            krishiv_proto::task::DrainContinuousOutputResponse {
                version: krishiv_proto::TransportVersion::CURRENT,
                disposition: TransportDisposition::Accepted,
                ipc_bytes,
            },
        ))
    }
}

/// Maximum IPC payload size accepted from the wire (256 MiB).
const MAX_IPC_BYTES: usize = 256 * 1024 * 1024;

fn decode_ipc_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>, tonic::Status> {
    if ipc_bytes.is_empty() {
        return Ok(vec![]);
    }
    if ipc_bytes.len() > MAX_IPC_BYTES {
        return Err(tonic::Status::resource_exhausted(format!(
            "IPC payload {} bytes exceeds max {} bytes",
            ipc_bytes.len(),
            MAX_IPC_BYTES
        )));
    }
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)
        .map_err(|e| tonic::Status::invalid_argument(format!("IPC decode: {e}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches
            .push(batch.map_err(|e| tonic::Status::invalid_argument(format!("IPC batch: {e}")))?);
    }
    Ok(batches)
}

fn encode_ipc_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, arrow::error::ArrowError> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    use arrow::ipc::writer::StreamWriter;
    let schema = batches
        .first()
        .ok_or_else(|| arrow::error::ArrowError::InvalidArgumentError("empty batches".to_string()))?
        .schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Networked gRPC adapter for executor-side task assignment calls.
#[derive(Debug, Clone)]
pub struct ExecutorTaskGrpcService {
    inner: ExecutorTaskInboxService,
    required_bearer_token: Option<String>,
    auth_misconfiguration: Option<String>,
}

impl ExecutorTaskGrpcService {
    /// Create a networked executor task service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self::with_auth_config(inbox, ExecutorTaskAuthConfig::from_env())
    }

    /// Create a networked executor task service with explicit auth config.
    pub fn with_auth_config(inbox: ExecutorAssignmentInbox, auth: ExecutorTaskAuthConfig) -> Self {
        let auth_misconfiguration = (auth.require_auth() && !auth.has_bearer_token()).then(|| {
            format!(
                "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty \
                 {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
            )
        });
        Self {
            inner: ExecutorTaskInboxService::new(inbox),
            required_bearer_token: auth.bearer_token().map(ToOwned::to_owned),
            auth_misconfiguration,
        }
    }

    /// Require a bearer token for network task-control RPCs.
    #[must_use]
    pub fn with_required_bearer_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.required_bearer_token = (!token.trim().is_empty()).then(|| token.trim().to_owned());
        self.auth_misconfiguration = None;
        self
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        self.inner.inbox()
    }

    fn validate_auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), tonic::Status> {
        if let Some(message) = &self.auth_misconfiguration {
            return Err(tonic::Status::unauthenticated(message.clone()));
        }
        let Some(expected) = &self.required_bearer_token else {
            return Ok(());
        };
        match bearer_token_from_metadata(metadata) {
            Some(actual)
                if constant_time_eq::constant_time_eq(actual.as_bytes(), expected.as_bytes()) =>
            {
                Ok(())
            }
            Some(_) => Err(tonic::Status::unauthenticated(
                "invalid executor task bearer token",
            )),
            None => Err(tonic::Status::unauthenticated(
                "missing executor task bearer token",
            )),
        }
    }
}

#[tonic::async_trait]
impl wire::v1::executor_task_server::ExecutorTask for ExecutorTaskGrpcService {
    async fn assign_task(
        &self,
        request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::executor_task_assignment_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .assign_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<wire::v1::TaskCancellationRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::task_cancellation_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .cancel_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn push_continuous_input(
        &self,
        request: tonic::Request<wire::v1::PushContinuousInputRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::push_continuous_input_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .push_continuous_input(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn drain_continuous_output(
        &self,
        request: tonic::Request<wire::v1::DrainContinuousOutputRequest>,
    ) -> Result<tonic::Response<wire::v1::DrainContinuousOutputResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::drain_continuous_output_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .drain_continuous_output(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::drain_continuous_output_response_to_wire(response),
        ))
    }
}

/// Build the generated tonic server around an executor task inbox.
pub fn executor_task_grpc_server(
    inbox: ExecutorAssignmentInbox,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    let max = krishiv_proto::max_grpc_message_bytes();
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
        .max_decoding_message_size(max)
        .max_encoding_message_size(max)
}

/// Build the generated tonic server sharing continuous-streaming state with a runner.
///
/// The `loop_executors` and `continuous_inputs` maps from
/// `ExecutorTaskRunner::shared_loop_executors()` / `shared_continuous_inputs()`
/// are shared here so that distributed `push_continuous_input` / `drain_continuous_output`
/// RPCs operate on the same state as `execute_loop_fragment`.
///
/// H-19 (audit): callers that wired auth via the builder API had their
/// explicit token silently dropped because this constructor always
/// rebuilt auth from the process environment. The new `auth` parameter
/// takes precedence; pass `None` to keep the env-based default.
pub fn executor_task_grpc_server_with_continuous(
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
    auth: Option<ExecutorTaskAuthConfig>,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    executor_task_grpc_server_with_run_loop(
        inbox,
        loop_executors,
        continuous_inputs,
        Arc::new(DashMap::new()),
        Arc::new(DashMap::new()),
        Arc::new(DashMap::new()),
        SharedClassExecutors::default(),
        Arc::new(DashMap::new()),
        auth,
    )
}

/// Build the generated tonic server sharing the FULL continuous-streaming
/// state with a runner, including the Phase 55 run-loop egress buffers and
/// input notifies (so pushes wake run-loops and drains serve their egress).
#[allow(clippy::too_many_arguments)]
pub fn executor_task_grpc_server_with_run_loop(
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
    continuous_outputs: crate::runner::SharedContinuousOutputs,
    input_notify: crate::runner::SharedContinuousNotify,
    continuous_connector_sources: crate::runner::SharedContinuousConnectorSources,
    class_executors: SharedClassExecutors,
    egress_notify: crate::runner::SharedContinuousNotify,
    auth: Option<ExecutorTaskAuthConfig>,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    let inner =
        ExecutorTaskInboxService::new_with_continuous(inbox, loop_executors, continuous_inputs)
            .with_run_loop_state(continuous_outputs, input_notify)
            .with_continuous_connector_sources(continuous_connector_sources)
            .with_class_executors(class_executors)
            .with_egress_notify(egress_notify);
    let auth = auth.unwrap_or_else(ExecutorTaskAuthConfig::from_env);
    let auth_misconfiguration = (auth.require_auth() && !auth.has_bearer_token()).then(|| {
        format!(
            "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty \
             {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
        )
    });
    let service = ExecutorTaskGrpcService {
        inner,
        required_bearer_token: auth.bearer_token().map(ToOwned::to_owned),
        auth_misconfiguration,
    };
    let max = krishiv_proto::max_grpc_message_bytes();
    wire::v1::executor_task_server::ExecutorTaskServer::new(service)
        .max_decoding_message_size(max)
        .max_encoding_message_size(max)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv_batch(ts: &[i64]) -> RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(vec!["a"; ts.len()])),
                std::sync::Arc::new(Int64Array::from(ts.to_vec())),
            ],
        )
        .expect("batch")
    }

    /// A drain with a wait budget must PARK on an empty egress and return
    /// data that arrives during the wait — the pre-fix behavior returned
    /// empty immediately and every consumer busy-polled (task #149 fix 12).
    #[tokio::test]
    async fn drain_long_poll_returns_data_arriving_mid_wait() {
        use krishiv_proto::task::DrainContinuousOutputRequest;
        use krishiv_proto::{JobId, TaskId, TransportVersion};
        let service = ExecutorTaskInboxService::new(ExecutorAssignmentInbox::new_unbounded());
        let svc = service.clone();
        let drain = tokio::spawn(async move {
            svc.drain_continuous_output(tonic::Request::new(DrainContinuousOutputRequest {
                version: TransportVersion::CURRENT,
                job_id: JobId::try_new("lp-job").expect("job id"),
                task_id: TaskId::try_new("t0").expect("task id"),
                wait_ms: 5_000,
            }))
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        service
            .continuous_outputs
            .entry("lp-job".into())
            .or_default()
            .push(kv_batch(&[1]));
        if let Some(notify) = service.egress_notify.get("lp-job") {
            notify.notify_waiters();
        }
        let started = std::time::Instant::now();
        let response = drain.await.expect("join").expect("drain ok").into_inner();
        assert!(
            !response.ipc_bytes.is_empty(),
            "the parked drain must return the batch that arrived mid-wait"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(4),
            "and must wake on notify, not ride out the full wait"
        );
    }

    /// The EOS flush moves bytes from operator state into egress — memory
    /// neutral — so it must NEVER be truncated by the ring cap. The old
    /// drop-oldest at flush destroyed computed FINAL output (live: NEXMark
    /// q9 kept exactly cap batches of its result). With egress pre-filled to
    /// the cap, flushing one more open window must GROW the buffer, not
    /// evict.
    #[tokio::test]
    async fn eos_flush_is_never_truncated_by_the_egress_cap() {
        let service = ExecutorTaskInboxService::new(ExecutorAssignmentInbox::new_unbounded());
        let spec = krishiv_plan::window::WindowExecutionSpec::tumbling("k", "ts", 60_000);
        let mut exec =
            krishiv_dataflow::ContinuousWindowExecutor::new(spec).expect("executor builds");
        exec.drain(vec![kv_batch(&[1_000])]).expect("window opens");
        service
            .loop_executors
            .insert("capjob#0".into(), Arc::new(Mutex::new(exec)));

        let cap = krishiv_common::streaming_dials::rloop_egress_cap();
        service
            .continuous_outputs
            .entry("capjob".into())
            .or_default()
            .extend(std::iter::repeat_with(|| kv_batch(&[2_000])).take(cap));

        let flushed = service
            .flush_run_loop_job("capjob")
            .await
            .expect("flush succeeds");
        assert!(flushed >= 1, "the open window must flush something");
        let egress_len = service
            .continuous_outputs
            .get("capjob")
            .map(|e| e.len())
            .unwrap_or(0);
        assert_eq!(
            egress_len,
            cap + flushed,
            "flush output must be APPENDED, never traded for evicted batches"
        );
    }

    fn service_with_auth(auth: ExecutorTaskAuthConfig) -> ExecutorTaskGrpcService {
        ExecutorTaskGrpcService::with_auth_config(ExecutorAssignmentInbox::new_unbounded(), auth)
    }

    fn metadata_with_bearer(token: &str) -> tonic::metadata::MetadataMap {
        let mut request = tonic::Request::new(());
        request.metadata_mut().insert(
            "authorization",
            tonic::metadata::MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
        request.metadata().clone()
    }

    #[test]
    fn validate_auth_allows_any_request_when_no_token_is_required() {
        let service = service_with_auth(ExecutorTaskAuthConfig::new(false, None));
        assert!(
            service
                .validate_auth(&tonic::metadata::MetadataMap::new())
                .is_ok()
        );
    }

    #[test]
    fn validate_auth_rejects_a_request_with_no_token_when_required() {
        let service =
            service_with_auth(ExecutorTaskAuthConfig::new(true, Some("secret".to_owned())));
        let err = service
            .validate_auth(&tonic::metadata::MetadataMap::new())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("missing"));
    }

    #[test]
    fn validate_auth_rejects_the_wrong_token() {
        let service =
            service_with_auth(ExecutorTaskAuthConfig::new(true, Some("secret".to_owned())));
        let err = service
            .validate_auth(&metadata_with_bearer("wrong"))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("invalid"));
    }

    #[test]
    fn validate_auth_accepts_the_correct_token() {
        let service =
            service_with_auth(ExecutorTaskAuthConfig::new(true, Some("secret".to_owned())));
        assert!(
            service
                .validate_auth(&metadata_with_bearer("secret"))
                .is_ok()
        );
    }

    /// Security-critical: a deployment that sets `KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH=true`
    /// but forgets to configure a bearer token must fail closed on every
    /// request — never silently fall back to "no auth required". A request
    /// with a token attached must be rejected exactly like one without, since
    /// there is no configured token to validate against.
    #[test]
    fn validate_auth_fails_closed_when_auth_is_required_but_no_token_is_configured() {
        let service = service_with_auth(ExecutorTaskAuthConfig::new(true, None));
        assert!(
            service
                .validate_auth(&tonic::metadata::MetadataMap::new())
                .is_err(),
            "misconfigured auth must reject bare requests"
        );
        assert!(
            service
                .validate_auth(&metadata_with_bearer("anything"))
                .is_err(),
            "misconfigured auth must reject requests even if they carry a token"
        );
    }

    #[tokio::test]
    async fn assign_task_rpc_enforces_auth_before_touching_the_inbox() {
        use krishiv_proto::{
            AttemptId, ExecutorId, JobId, LeaseGeneration, OutputContract, OutputContractKind,
            PlanFragment, StageId, TaskAttemptRef, TaskId,
        };

        let service =
            service_with_auth(ExecutorTaskAuthConfig::new(true, Some("secret".to_owned())));
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-auth").unwrap(),
                StageId::try_new("stage-auth").unwrap(),
                TaskId::try_new("task-auth").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-auth").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        );
        let request =
            tonic::Request::new(wire::executor_task_assignment_to_wire(assignment).unwrap());

        // No authorization header attached — must be rejected before the
        // request ever reaches inbox decoding/insertion.
        let status =
            <ExecutorTaskGrpcService as wire::v1::executor_task_server::ExecutorTask>::assign_task(
                &service, request,
            )
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(
            service.inbox().pop_next().unwrap().is_none(),
            "the rejected assignment must never reach the inbox"
        );
    }

    /// Cancelling a continuous job must retire its source READ POSITIONS along
    /// with its window state.
    ///
    /// It used to drop `loop_executors` (the state) and leave the connector
    /// source cache alone — only the *restore* path ever cleared it. Those
    /// entries hold each source's advanced offset, so re-registering the same
    /// job id in the same process resumed reading where the dead incarnation
    /// stopped, against **empty** window state. Everything between the last
    /// checkpoint and the cancel was skipped, with no error and a job that
    /// looked healthy from every angle.
    /// Minimal `Source` so the cache holds something of the right shape. The
    /// cancel path only keys on the map, never reads the source.
    struct ExhaustedSource;

    impl krishiv_connectors::Source for ExhaustedSource {
        fn capabilities(&self) -> krishiv_connectors::ConnectorCapabilities {
            krishiv_connectors::ConnectorCapabilities::default()
        }
        async fn read_batch(&mut self) -> krishiv_connectors::ConnectorResult<Option<RecordBatch>> {
            Ok(None)
        }
        fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
            None
        }
    }

    #[tokio::test]
    async fn cancelling_a_continuous_job_retires_its_source_read_positions() {
        use krishiv_proto::task::TaskCancellationRequest;

        let inbox = ExecutorAssignmentInbox::new();
        let service = ExecutorTaskInboxService::new(inbox);

        // A registered continuous executor is what marks this cancel as a
        // continuous-job teardown rather than an ordinary task cancel.
        service.loop_executors.insert(
            "job-src".to_owned(),
            Arc::new(Mutex::new(
                ContinuousWindowExecutor::new(krishiv_plan::window::WindowExecutionSpec::tumbling(
                    "k", "ts", 1_000,
                ))
                .unwrap(),
            )),
        );
        // Two source-cache families: cycle (`{job}|…`) and run-loop
        // (`{job}#<subtask>|…`). Both belong to this job and both must go.
        for key in ["job-src|kafka:topic-a", "job-src#0|kafka:topic-a"] {
            service.continuous_connector_sources.insert(
                key.to_owned(),
                Arc::new(tokio::sync::Mutex::new(
                    Box::new(ExhaustedSource) as Box<dyn krishiv_connectors::DynSource>
                )),
            );
        }
        // A different job's entry must survive — the prefix match has to be
        // scoped, not a substring sweep.
        service.continuous_connector_sources.insert(
            "job-src-other|kafka:topic-b".to_owned(),
            Arc::new(tokio::sync::Mutex::new(
                Box::new(ExhaustedSource) as Box<dyn krishiv_connectors::DynSource>
            )),
        );

        let request = TaskCancellationRequest::new(krishiv_proto::TaskAttemptRef::new(
            krishiv_proto::JobId::try_new("job-src").unwrap(),
            krishiv_proto::StageId::try_new("stage-src").unwrap(),
            krishiv_proto::TaskId::try_new("task-src").unwrap(),
            krishiv_proto::AttemptId::initial(),
        ));
        service
            .cancel_task(tonic::Request::new(request))
            .await
            .expect("cancel succeeds");

        assert!(
            !service
                .continuous_connector_sources
                .contains_key("job-src|kafka:topic-a"),
            "the cycle-model source position must die with the job"
        );
        assert!(
            !service
                .continuous_connector_sources
                .contains_key("job-src#0|kafka:topic-a"),
            "the run-loop subtask source position must die with the job"
        );
        assert!(
            service
                .continuous_connector_sources
                .contains_key("job-src-other|kafka:topic-b"),
            "a different job's source must be untouched"
        );
    }
}
