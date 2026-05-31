use super::*;

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

    /// Register inline input partitions for a bounded-window job.
    pub fn register_window_partitions(
        &mut self,
        job_id: JobId,
        partitions: Vec<krishiv_proto::InputPartition>,
    ) {
        self.window_job_partitions.insert(job_id, partitions);
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
        let window_parts = self.window_job_partitions.get(job_id).cloned();
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
        use futures::stream::{FuturesUnordered, StreamExt};

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

        let mut futures = FuturesUnordered::new();
        for (endpoint, assignment) in remote {
            let channels = Arc::clone(&channels);
            futures.push(async move {
                let channel = Self::get_or_connect_channel_on_map(&channels, &endpoint).await?;
                let mut client =
                    wire::v1::executor_task_client::ExecutorTaskClient::with_interceptor(
                        channel,
                        krishiv_metrics::grpc::inject_trace_context
                            as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                    );
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    client.assign_task(wire::executor_task_assignment_to_wire(assignment.clone())),
                )
                .await
                .map_err(|_| SchedulerError::Transport {
                    message: format!("assign_task to {endpoint} timed out after 30s"),
                })?
                .map_err(|error| SchedulerError::Transport {
                    message: format!("assign_task to {endpoint}: {error}"),
                })?
                .into_inner();
                wire::task_status_response_from_wire(response)
                    .map(|decoded| (assignment, decoded))
                    .map_err(|error| SchedulerError::Transport {
                        message: format!("wire decode from {endpoint}: {error}"),
                    })
            });
        }

        let mut responses = Vec::new();
        while let Some(result) = futures.next().await {
            responses.push(result?);
        }
        Ok(responses)
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
                    krishiv_metrics::grpc::inject_trace_context
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
