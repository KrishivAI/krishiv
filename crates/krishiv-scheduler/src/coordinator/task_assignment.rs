use super::*;

pub(crate) const MAX_CONCURRENT_ASSIGNMENT_RPCS: usize = 64;
const MAX_ASSIGNMENT_DELIVERY_ATTEMPTS: usize = 3;
const ASSIGNMENT_DELIVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const EXECUTOR_TASK_BEARER_TOKEN_ENV: &str = "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN";

fn configured_executor_task_bearer_token() -> Option<String> {
    std::env::var(EXECUTOR_TASK_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

fn inject_executor_task_request_context(
    req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let mut req = krishiv_metrics::grpc::inject_trace_context(req)?;
    if let Some(token) = configured_executor_task_bearer_token() {
        let header = format!("Bearer {token}");
        let value = tonic::metadata::MetadataValue::try_from(header.as_str()).map_err(|_| {
            tonic::Status::internal(format!(
                "{EXECUTOR_TASK_BEARER_TOKEN_ENV} contains characters that are invalid for gRPC metadata"
            ))
        })?;
        req.metadata_mut().insert("authorization", value);
    }
    Ok(req)
}

pub(crate) fn round_robin_assignment_targets(
    targets: Vec<(String, ExecutorTaskAssignment)>,
) -> Vec<(String, ExecutorTaskAssignment)> {
    let total = targets.len();
    let mut by_endpoint: std::collections::BTreeMap<
        String,
        std::collections::VecDeque<ExecutorTaskAssignment>,
    > = std::collections::BTreeMap::new();
    for (endpoint, assignment) in targets {
        by_endpoint
            .entry(endpoint)
            .or_default()
            .push_back(assignment);
    }

    let mut queues: Vec<_> = by_endpoint.into_iter().collect();
    let mut ordered = Vec::with_capacity(total);
    while ordered.len() < total {
        let mut progressed = false;
        for (endpoint, queue) in &mut queues {
            if let Some(assignment) = queue.pop_front() {
                ordered.push((endpoint.clone(), assignment));
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    ordered
}

pub(crate) async fn collect_bounded_assignment_futures<T, E, Fut, It>(
    futures: It,
) -> Result<Vec<T>, E>
where
    Fut: std::future::Future<Output = Result<T, E>>,
    It: IntoIterator<Item = Fut>,
{
    use futures::StreamExt;

    let mut stream =
        futures::stream::iter(futures).buffer_unordered(MAX_CONCURRENT_ASSIGNMENT_RPCS);
    let mut responses = Vec::new();
    while let Some(result) = stream.next().await {
        responses.push(result?);
    }
    Ok(responses)
}

fn is_retryable_assignment_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    )
}

async fn assignment_retry_backoff(attempt_idx: usize) {
    let backoff_ms = 100u64.saturating_mul(1u64 << attempt_idx.min(4));
    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
}

impl Coordinator {
    /// Re-assign all `Pending` tasks in a job to available executors.
    ///
    /// Called after a stage retry (P1.24) to move tasks from `Pending` back to
    /// `Assigned` so `launch_assigned_tasks` can launch them.
    pub fn assign_pending_tasks(&mut self, job_id: &JobId) -> SchedulerResult<usize> {
        self.ensure_active()?;
        // Collect executor ids first to avoid a simultaneous immutable + mutable borrow.
        let mut executor_ids: Vec<ExecutorId> = self
            .executors
            .schedulable_executors()
            .into_iter()
            .map(|d| d.executor_id().clone())
            .collect();
        executor_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        if executor_ids.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }
        let mut job = self.find_job_mut(job_id)?;
        let pending_task_ids: Vec<TaskId> = job
            .stages
            .iter()
            .flat_map(|s| s.tasks())
            .filter(|t| t.state() == TaskState::Pending)
            .map(|t| t.task_id().clone())
            .collect();
        let count = pending_task_ids.len();
        for (idx, task_id) in pending_task_ids.into_iter().enumerate() {
            let executor_id = executor_ids[idx % executor_ids.len()].clone();
            let assignment = TaskAssignment::new(task_id, executor_id);
            job.apply_assignments(vec![assignment]);
        }
        Ok(count)
    }

    /// Launch all assigned tasks for a job.
    pub fn launch_assigned_tasks(&mut self, job_id: &JobId) -> SchedulerResult<usize> {
        self.launch_assigned_task_assignments(job_id)
            .map(|assignments| assignments.len())
    }

    /// Register batch SQL tables for a job.
    pub fn register_batch_sql_tables(
        &mut self,
        job_id: JobId,
        tables: Vec<crate::batch_sql::BatchSqlTable>,
    ) {
        self.batch_sql_job_tables.insert(job_id, tables);
    }

    /// Register inline input partitions for a batch-sql or bounded-window job.
    pub fn register_job_input_partitions(
        &mut self,
        job_id: JobId,
        partitions: Vec<krishiv_proto::InputPartition>,
    ) {
        self.job_input_partitions.insert(job_id, partitions);
    }

    /// Launch all assigned tasks for a job and return executor transport assignments.
    pub fn launch_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        tracing::debug!(
            job_id = %job_id,
            "launching assigned task assignments (JCP delegation and Notify will be used in two-tier model)"
        );
        self.ensure_active()?;

        // PRR Parallel Execution - Circuit Breaker (IMM-1):
        // Filter out executors that have crossed the failure threshold before
        // even attempting to launch tasks to them.
        let failure_threshold = self.config.circuit_breaker_failure_threshold();
        let bad_executors: std::collections::HashSet<_> = self
            .executors
            .executors_over_failure_threshold(failure_threshold)
            .into_iter()
            .collect();

        let mut executor_leases = self.executors.assignment_leases();
        if !bad_executors.is_empty() {
            executor_leases.retain(|(eid, _)| !bad_executors.contains(eid));
            tracing::warn!(
                job_id = %job_id,
                bad_executor_count = bad_executors.len(),
                "circuit breaker: filtered bad executors from launch candidates"
            );
        }

        let batch_tables = self.batch_sql_job_tables.get(job_id).cloned();
        let window_parts = self.job_input_partitions.get(job_id).cloned();
        let assignments = self
            .find_job_mut(job_id)?
            .launch_assigned_task_assignments(
                &executor_leases,
                batch_tables.as_deref(),
                window_parts.as_deref(),
            )?;
        // GAP-OB-01: Increment tasks_assigned counter.
        TASKS_ASSIGNED_TOTAL.fetch_add(assignments.len() as u64, AtomicOrdering::Relaxed);
        Ok(assignments)
    }

    /// Resolve executor task endpoints for launched assignments.
    pub fn resolve_assignment_targets(
        &self,
        assignments: Vec<ExecutorTaskAssignment>,
    ) -> SchedulerResult<Vec<(String, ExecutorTaskAssignment)>> {
        tracing::debug!(
            assignment_count = assignments.len(),
            "resolving assignment targets for delivery"
        );

        for a in &assignments {
            tracing::trace!(task_id = %a.task_id(), executor = %a.executor_id(), "resolving single assignment target");
        }

        let mut targets = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            let endpoint = self
                .executors
                .find_executor(assignment.executor_id())?
                .descriptor()
                .task_endpoint()
                .ok_or_else(|| SchedulerError::InvalidJob {
                    message: format!(
                        "executor {} has no task endpoint for assignment push",
                        assignment.executor_id()
                    ),
                })?
                .to_owned();
            targets.push((endpoint, assignment));
        }
        Ok(targets)
    }

    /// Push pre-resolved assignments to executor task endpoints.
    pub async fn deliver_assignment_targets(
        &self,
        targets: Vec<(String, ExecutorTaskAssignment)>,
    ) -> SchedulerResult<Vec<(ExecutorTaskAssignment, TaskStatusResponse)>> {
        let channels = self.executor_channels.clone();
        Self::deliver_assignment_targets_with_channels(channels, targets).await
    }

    pub(crate) async fn deliver_assignment_targets_with_channels(
        channels: Arc<DashMap<String, tonic::transport::Channel>>,
        targets: Vec<(String, ExecutorTaskAssignment)>,
    ) -> SchedulerResult<Vec<(ExecutorTaskAssignment, TaskStatusResponse)>> {
        // Inbox-backed in-process targets do not have a gRPC endpoint: they are
        // delivered directly via `InProcessCoordinatorBridge` (see F4 / the
        // `inprocess://` sentinel).  Logging would create noise; the in-process
        // path pushes to the inbox before reaching this function.
        let (in_process, remote): (Vec<_>, Vec<_>) = targets
            .into_iter()
            .partition(|(endpoint, _)| is_in_process_task_endpoint(endpoint));
        if !in_process.is_empty() {
            tracing::debug!(
                count = in_process.len(),
                "skipping gRPC dispatch for in-process task endpoints"
            );
        }

        let futures =
            round_robin_assignment_targets(remote)
                .into_iter()
                .map(|(endpoint, assignment)| {
                    let channels = Arc::clone(&channels);
                    async move {
                        Self::deliver_assignment_target_with_retries(
                            &channels, endpoint, assignment,
                        )
                        .await
                    }
                });

        collect_bounded_assignment_futures(futures).await
    }

    async fn deliver_assignment_target_with_retries(
        channels: &Arc<DashMap<String, tonic::transport::Channel>>,
        endpoint: String,
        assignment: ExecutorTaskAssignment,
    ) -> SchedulerResult<(ExecutorTaskAssignment, TaskStatusResponse)> {
        for attempt_idx in 0..MAX_ASSIGNMENT_DELIVERY_ATTEMPTS {
            let final_attempt = attempt_idx + 1 == MAX_ASSIGNMENT_DELIVERY_ATTEMPTS;
            let channel = match Self::get_or_connect_channel_on_map(channels, &endpoint).await {
                Ok(channel) => channel,
                Err(error) => {
                    if final_attempt {
                        return Err(error);
                    }
                    tracing::warn!(
                        endpoint = %endpoint,
                        task_id = %assignment.task_id(),
                        attempt = attempt_idx + 1,
                        error = %error,
                        "assignment channel connect failed; retrying"
                    );
                    assignment_retry_backoff(attempt_idx).await;
                    continue;
                }
            };

            let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                channel,
                inject_executor_task_request_context
                    as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            );
            let response = tokio::time::timeout(
                ASSIGNMENT_DELIVERY_TIMEOUT,
                client.assign_task(wire::executor_task_assignment_to_wire(assignment.clone())),
            )
            .await;

            match response {
                Ok(Ok(response)) => {
                    let response = response.into_inner();
                    return wire::task_status_response_from_wire(response)
                        .map(|decoded| (assignment, decoded))
                        .map_err(|error| SchedulerError::Transport {
                            message: format!("wire decode from {endpoint}: {error}"),
                        });
                }
                Ok(Err(status)) if is_retryable_assignment_status(&status) && !final_attempt => {
                    channels.remove(&endpoint);
                    tracing::warn!(
                        endpoint = %endpoint,
                        task_id = %assignment.task_id(),
                        attempt = attempt_idx + 1,
                        code = ?status.code(),
                        error = %status,
                        "assign_task rpc failed transiently; retrying"
                    );
                    assignment_retry_backoff(attempt_idx).await;
                }
                Ok(Err(status)) => {
                    return Err(SchedulerError::Transport {
                        message: format!("assign_task to {endpoint}: {status}"),
                    });
                }
                Err(_elapsed) if !final_attempt => {
                    channels.remove(&endpoint);
                    tracing::warn!(
                        endpoint = %endpoint,
                        task_id = %assignment.task_id(),
                        attempt = attempt_idx + 1,
                        "assign_task rpc timed out; retrying"
                    );
                    assignment_retry_backoff(attempt_idx).await;
                }
                Err(_elapsed) => {
                    return Err(SchedulerError::Transport {
                        message: format!(
                            "assign_task to {endpoint} timed out after {}s",
                            ASSIGNMENT_DELIVERY_TIMEOUT.as_secs()
                        ),
                    });
                }
            }
        }

        Err(SchedulerError::Transport {
            message: format!("assign_task to {endpoint}: retry loop exhausted"),
        })
    }

    /// Launch assigned tasks and push them to executor-owned task endpoints.
    ///
    /// # Lock safety (GAP-4)
    ///
    /// This method takes `&mut self` for the sync prepare phase
    /// (`launch_assigned_task_assignments` + `resolve_assignment_targets`), then
    /// clones the channel map and calls the **static** `deliver_assignment_targets_with_channels`
    /// so `self` is NOT borrowed during the async network I/O.
    ///
    /// **Important**: If you call this through a `SharedCoordinator.write()` guard the write
    /// lock is still held for the duration of the await, because the borrow lives for the
    /// entire async function body.  For the production dispatch path use
    /// `JobCoordinator::spawn_job_orchestration_loops`, which explicitly drops the write guard
    /// before awaiting.  This method is intended for tests and CLI tools where no shared lock
    /// is involved.
    pub async fn push_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<TaskStatusResponse>> {
        let assignments = self.launch_assigned_task_assignments(job_id)?;
        let targets = self.resolve_assignment_targets(assignments)?;
        // GAP-4: Clone the channel map BEFORE the await point. Because
        // `deliver_assignment_targets_with_channels` is a static method that owns
        // `channels`, `self` is not borrowed across the network I/O yield points.
        // Callers that hold a `SharedCoordinator.write()` guard should prefer the
        // `JobCoordinator` pattern (acquire lock → collect targets → drop lock → deliver).
        let channels = self.executor_channels.clone();
        let responses =
            match Self::deliver_assignment_targets_with_channels(channels, targets).await {
                Ok(responses) => responses,
                Err(error) => {
                    self.clear_launch_in_flight_for_job(job_id);
                    return Err(error);
                }
            };
        self.apply_assignment_dispatch_responses(job_id, &responses);
        Ok(responses
            .into_iter()
            .map(|(_, response)| response)
            .collect())
    }

    /// Cancel a job and push `CancelTask` RPCs to all executors owning running tasks.
    ///
    /// Partial RPC failures are logged but are not fatal for R3.1 — the
    /// scheduler-side cancel is always applied.
    pub async fn push_cancel_job(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        // Collect (endpoint, TaskCancellationRequest) for each running task.
        let mut targets: Vec<(String, TaskCancellationRequest)> = Vec::new();
        {
            let job = self.find_job(job_id)?;
            for stage in job.stages() {
                for task in stage.tasks() {
                    if task.state() == TaskState::Running
                        && let Some(executor_id) = task.assigned_executor()
                        && let Ok(record) = self.executors.find_executor(executor_id)
                        && let Some(endpoint) = record.descriptor().task_endpoint()
                    {
                        let attempt_id = AttemptId::try_new(task.attempt()).map_err(|e| {
                            SchedulerError::InvalidJob {
                                message: e.to_string(),
                            }
                        })?;
                        let req = TaskCancellationRequest::new(TaskAttemptRef::new(
                            job_id.clone(),
                            stage.stage_id().clone(),
                            task.task_id().clone(),
                            attempt_id,
                        ))
                        .with_reason("job cancelled");
                        targets.push((endpoint.to_owned(), req));
                    }
                }
            }
        }

        // Cancel the job in scheduler state first.
        self.cancel_job(job_id)?;

        // Push cancel RPCs — partial failures are non-fatal.  Re-use the
        // executor channel cache so we do not pay a TCP+TLS handshake per
        // cancel target (F2).  Drive them concurrently.
        let channels = self.executor_channels.clone();
        let mut futures = futures::stream::FuturesUnordered::new();
        for (endpoint, req) in targets {
            if is_in_process_task_endpoint(&endpoint) {
                tracing::debug!(endpoint = %endpoint, "skipping cancel for in-process executor");
                continue;
            }
            let channels = channels.clone();
            futures.push(async move {
                let channel = match Self::get_or_connect_channel_on_map(&channels, &endpoint).await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!(endpoint = %endpoint, error = %err, "push_cancel_job: connect failed");
                        return;
                    }
                };
                let mut client = wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                    channel,
                    inject_executor_task_request_context
                        as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                );
                if let Err(err) = client
                    .cancel_task(wire::task_cancellation_request_to_wire(req))
                    .await
                {
                    tracing::warn!(endpoint = %endpoint, error = %err, "push_cancel_job: cancel_task rpc failed");
                }
            });
        }
        use futures::stream::StreamExt;
        while futures.next().await.is_some() {}
        Ok(())
    }

    pub(crate) fn clear_launch_in_flight_for_job(&mut self, job_id: &JobId) {
        let Some(mut job) = self
            .job_coordinators
            .get(job_id)
            .map(|jc| jc.write_record())
        else {
            return;
        };
        for stage in &mut job.stages {
            for task in stage.tasks_mut() {
                if task.state() == TaskState::Assigned {
                    task.clear_launch_in_flight();
                }
            }
            stage.refresh_state();
        }
        job.refresh_state();
    }

    fn clear_launch_in_flight_for_task(&mut self, job_id: &JobId, task_id: &TaskId) {
        let Some(mut job) = self
            .job_coordinators
            .get(job_id)
            .map(|jc| jc.write_record())
        else {
            return;
        };
        for stage in &mut job.stages {
            let mut changed = false;
            for task in stage.tasks_mut() {
                if task.task_id() == task_id && task.state() == TaskState::Assigned {
                    task.clear_launch_in_flight();
                    changed = true;
                    break;
                }
            }
            if changed {
                stage.refresh_state();
            }
        }
        job.refresh_state();
    }

    pub(crate) fn apply_assignment_dispatch_responses(
        &mut self,
        job_id: &JobId,
        responses: &[(ExecutorTaskAssignment, TaskStatusResponse)],
    ) -> usize {
        tracing::debug!(
            job_id = %job_id,
            response_count = responses.len(),
            "applying launch dispatch responses (JCP delegation may influence future retries)"
        );

        for (assignment, response) in responses {
            tracing::trace!(
                job_id = %job_id,
                task_id = %assignment.task_id(),
                disposition = ?response.disposition(),
                "individual launch response"
            );
        }

        let mut accepted = 0usize;
        for (assignment, response) in responses {
            match response.disposition() {
                krishiv_proto::TransportDisposition::Accepted
                | krishiv_proto::TransportDisposition::Duplicate => {
                    accepted = accepted.saturating_add(1);
                }
                _ => self.clear_launch_in_flight_for_task(job_id, assignment.task_id()),
            }
        }
        accepted
    }

    /// P1.2: Get or create a cached gRPC channel for the given executor endpoint.
    ///
    /// On a cache hit, clones the existing `Channel` (pointer-only cost).
    /// On a miss, establishes a new TCP+TLS connection and stores it for reuse.
    #[allow(dead_code)]
    async fn get_or_connect_channel(
        &self,
        endpoint: &str,
    ) -> SchedulerResult<tonic::transport::Channel> {
        Self::get_or_connect_channel_on_map(&self.executor_channels, endpoint).await
    }

    pub(crate) async fn get_or_connect_channel_on_map(
        channels: &Arc<DashMap<String, tonic::transport::Channel>>,
        endpoint: &str,
    ) -> SchedulerResult<tonic::transport::Channel> {
        // Fast path: check the sharded cache (per-shard lock, dropped
        // immediately) so lookups for different endpoints never contend.
        if let Some(ch) = channels.get(endpoint) {
            return Ok(ch.clone());
        }

        // Slow path: connect outside any lock so a single slow handshake
        // cannot block lookups for other endpoints (M6).
        let parsed =
            tonic::transport::Endpoint::from_shared(endpoint.to_string()).map_err(|e| {
                SchedulerError::InvalidJob {
                    message: e.to_string(),
                }
            })?;
        let ch = parsed
            .connect_timeout(std::time::Duration::from_secs(10))
            .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
            .http2_keep_alive_interval(std::time::Duration::from_secs(15))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .map_err(|e| SchedulerError::ExecutorUnavailable {
                endpoint: endpoint.to_string(),
                reason: e.to_string(),
            })?;

        // Only one shard is locked during the insert.  If another task
        // raced and already installed a channel, prefer the existing one.
        let endpoint_owned = endpoint.to_owned();
        let entry = channels.entry(endpoint_owned);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(existing) => Ok(existing.get().clone()),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(ch.clone());
                Ok(ch)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use krishiv_proto::{
        AttemptId, ExecutorId, JobId, LeaseGeneration, OutputContract, OutputContractKind,
        PlanFragment, StageId, TaskAttemptRef, TaskId,
    };

    use super::*;

    fn test_assignment(task_id: &str, executor_id: &str) -> ExecutorTaskAssignment {
        ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-fair").unwrap(),
                StageId::try_new("stage-fair").unwrap(),
                TaskId::try_new(task_id).unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new(executor_id).unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        )
    }

    #[tokio::test]
    async fn bounded_assignment_collector_limits_concurrency() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let futures = (0..(MAX_CONCURRENT_ASSIGNMENT_RPCS + 8)).map(|_| {
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, SchedulerError>(())
            }
        });

        let responses = collect_bounded_assignment_futures(futures).await.unwrap();

        assert_eq!(responses.len(), MAX_CONCURRENT_ASSIGNMENT_RPCS + 8);
        assert!(
            max_active.load(Ordering::SeqCst) <= MAX_CONCURRENT_ASSIGNMENT_RPCS,
            "assignment dispatch must not exceed the configured concurrency cap"
        );
    }

    #[test]
    fn round_robin_assignment_targets_interleaves_executor_endpoints() {
        let targets = vec![
            (
                "http://exec-a".to_owned(),
                test_assignment("task-a1", "exec-a"),
            ),
            (
                "http://exec-a".to_owned(),
                test_assignment("task-a2", "exec-a"),
            ),
            (
                "http://exec-a".to_owned(),
                test_assignment("task-a3", "exec-a"),
            ),
            (
                "http://exec-b".to_owned(),
                test_assignment("task-b1", "exec-b"),
            ),
            (
                "http://exec-c".to_owned(),
                test_assignment("task-c1", "exec-c"),
            ),
            (
                "http://exec-c".to_owned(),
                test_assignment("task-c2", "exec-c"),
            ),
        ];

        let ordered = round_robin_assignment_targets(targets);
        let endpoint_order: Vec<_> = ordered
            .iter()
            .map(|(endpoint, _)| endpoint.as_str())
            .collect();

        assert_eq!(
            endpoint_order,
            vec![
                "http://exec-a",
                "http://exec-b",
                "http://exec-c",
                "http://exec-a",
                "http://exec-c",
                "http://exec-a",
            ]
        );
    }
}
