#![forbid(unsafe_code)]

//! R2 in-process scheduler skeleton.
//!
//! This crate owns the distributed control-plane model without introducing
//! Kubernetes clients. R2 keeps one active coordinator and replaceable
//! executors; R3.1 maps coordinator/executor contracts to a networked gRPC
//! service.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};

use krishiv_plan::{ExecutionKind as PlanExecutionKind, LogicalPlan, PhysicalPlan, PlanNode};
use krishiv_proto::{
    AttemptId, CoordinatorExecutorService, CoordinatorId, CoordinatorState, ExecutorDescriptor,
    ExecutorHeartbeat, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId,
    ExecutorState, ExecutorTaskAssignment, InputPartition, JobId, JobKind, JobSpec, JobState,
    LeaseGeneration, OutputContract, OutputContractKind, PlanFragment, RegisterExecutorRequest,
    RegisterExecutorResponse, StageId, StageSpec, StageState, TaskAssignment, TaskAttemptRef,
    TaskId, TaskSpec, TaskState, TaskStatusRequest, TaskStatusResponse, TaskStatusUpdate,
    TransportDisposition, TransportVersion, wire,
};

/// Scheduler result alias.
pub type SchedulerResult<T> = Result<T, SchedulerError>;

/// Result of applying a task status update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskUpdateOutcome {
    /// The update changed scheduler state.
    Applied,
    /// The update was already reflected in scheduler state.
    Duplicate,
}

/// Coordinator behavior knobs for deterministic R2 scheduler tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorConfig {
    max_stage_retries: u32,
    heartbeat_timeout_ticks: u64,
}

impl CoordinatorConfig {
    /// Create a coordinator config.
    pub fn new(max_stage_retries: u32, heartbeat_timeout_ticks: u64) -> Self {
        Self {
            max_stage_retries,
            heartbeat_timeout_ticks: heartbeat_timeout_ticks.max(1),
        }
    }

    /// Maximum number of stage-level retries after an executor reports failure.
    pub fn max_stage_retries(&self) -> u32 {
        self.max_stage_retries
    }

    /// Number of scheduler ticks an executor can miss before it is marked lost.
    pub fn heartbeat_timeout_ticks(&self) -> u64 {
        self.heartbeat_timeout_ticks
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self::new(1, 3)
    }
}

/// Scheduler and coordinator errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    /// The coordinator is not active.
    InactiveCoordinator {
        coordinator_id: CoordinatorId,
        state: CoordinatorState,
    },
    /// Executor already exists.
    DuplicateExecutor { executor_id: ExecutorId },
    /// Executor was not found.
    UnknownExecutor { executor_id: ExecutorId },
    /// Executor used an older or otherwise invalid lease generation.
    StaleExecutorLease {
        executor_id: ExecutorId,
        expected: LeaseGeneration,
        received: LeaseGeneration,
    },
    /// No healthy executors are available for placement.
    NoExecutors,
    /// Job already exists.
    DuplicateJob { job_id: JobId },
    /// Job was not found.
    UnknownJob { job_id: JobId },
    /// Stage was not found.
    UnknownStage { stage_id: StageId },
    /// Task was not found.
    UnknownTask { task_id: TaskId },
    /// Task status referenced an attempt that is no longer current.
    StaleTaskAttempt {
        task_id: TaskId,
        expected: u32,
        received: u32,
    },
    /// Job submission was invalid.
    InvalidJob { message: String },
    /// Distributed DAG conversion failed.
    InvalidPlan { message: String },
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InactiveCoordinator {
                coordinator_id,
                state,
            } => write!(
                f,
                "coordinator {coordinator_id} is {state}; only the active coordinator may mutate state"
            ),
            Self::DuplicateExecutor { executor_id } => {
                write!(f, "executor already registered: {executor_id}")
            }
            Self::UnknownExecutor { executor_id } => write!(f, "unknown executor: {executor_id}"),
            Self::StaleExecutorLease {
                executor_id,
                expected,
                received,
            } => write!(
                f,
                "stale executor lease for {executor_id}: expected generation {expected}, received {received}"
            ),
            Self::NoExecutors => f.write_str("no healthy executors are available"),
            Self::DuplicateJob { job_id } => write!(f, "job already exists: {job_id}"),
            Self::UnknownJob { job_id } => write!(f, "unknown job: {job_id}"),
            Self::UnknownStage { stage_id } => write!(f, "unknown stage: {stage_id}"),
            Self::UnknownTask { task_id } => write!(f, "unknown task: {task_id}"),
            Self::StaleTaskAttempt {
                task_id,
                expected,
                received,
            } => write!(
                f,
                "stale task attempt for {task_id}: expected attempt {expected}, received {received}"
            ),
            Self::InvalidJob { message } => write!(f, "invalid job: {message}"),
            Self::InvalidPlan { message } => write!(f, "invalid plan: {message}"),
        }
    }
}

impl Error for SchedulerError {}

/// R2 coordinator skeleton.
#[derive(Debug, Clone)]
pub struct Coordinator {
    coordinator_id: CoordinatorId,
    state: CoordinatorState,
    config: CoordinatorConfig,
    executors: ExecutorRegistry,
    jobs: Vec<JobRecord>,
}

/// Shared handle to the active coordinator owned by an R2 runtime process.
#[derive(Debug, Clone)]
pub struct SharedCoordinator {
    inner: Arc<RwLock<Coordinator>>,
}

impl SharedCoordinator {
    /// Create a shared coordinator handle.
    pub fn new(coordinator: Coordinator) -> Self {
        Self {
            inner: Arc::new(RwLock::new(coordinator)),
        }
    }

    /// Borrow the coordinator for read-only status snapshots.
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, Coordinator>> {
        self.inner.read()
    }

    /// Borrow the coordinator for scheduler mutations.
    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, Coordinator>> {
        self.inner.write()
    }
}

/// Tonic-shaped adapter that exposes coordinator/executor RPCs over a shared coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorExecutorTonicService {
    coordinator: SharedCoordinator,
}

impl CoordinatorExecutorTonicService {
    /// Create a coordinator/executor service adapter.
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self { coordinator }
    }

    /// Shared coordinator backing this adapter.
    pub fn coordinator(&self) -> &SharedCoordinator {
        &self.coordinator
    }
}

#[tonic::async_trait]
impl CoordinatorExecutorService for CoordinatorExecutorTonicService {
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let descriptor = request.descriptor().clone();
        let executor_id = descriptor.executor_id().clone();
        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator.register_executor(descriptor) {
            Ok(lease_generation) => RegisterExecutorResponse::new(
                executor_id,
                lease_generation,
                TransportDisposition::Accepted,
            ),
            Err(SchedulerError::DuplicateExecutor { executor_id }) => {
                RegisterExecutorResponse::new(
                    executor_id,
                    LeaseGeneration::initial(),
                    TransportDisposition::Duplicate,
                )
                .with_message("executor is already registered")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let heartbeat = ExecutorHeartbeat::new(request.executor_id().clone(), request.state())
            .with_lease_generation(request.lease_generation())
            .with_running_tasks(
                request
                    .running_attempts()
                    .iter()
                    .map(|attempt| attempt.task_id().clone())
                    .collect(),
            );
        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator.executor_heartbeat(heartbeat) {
            Ok(()) => ExecutorHeartbeatResponse::new(
                request.lease_generation(),
                TransportDisposition::Accepted,
            ),
            Err(SchedulerError::UnknownExecutor { .. }) => ExecutorHeartbeatResponse::new(
                request.lease_generation(),
                TransportDisposition::UnknownExecutor,
            )
            .with_message("executor is not registered"),
            Err(SchedulerError::StaleExecutorLease { expected, .. }) => {
                ExecutorHeartbeatResponse::new(expected, TransportDisposition::StaleLease)
                    .with_message("executor lease generation is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut update = TaskStatusUpdate::new(
            request.job_id().clone(),
            request.stage_id().clone(),
            request.task_id().clone(),
            request.executor_id().clone(),
            request.state(),
            request.attempt_id().as_u32(),
        )
        .with_lease_generation(request.lease_generation());
        if let Some(message) = request.message() {
            update = update.with_message(message);
        }

        let mut coordinator = self
            .coordinator
            .write()
            .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))?;

        let response = match coordinator.apply_task_update(update) {
            Ok(TaskUpdateOutcome::Applied) => {
                TaskStatusResponse::new(TransportDisposition::Accepted)
            }
            Ok(TaskUpdateOutcome::Duplicate) => {
                TaskStatusResponse::new(TransportDisposition::Duplicate)
                    .with_message("task status update was already applied")
            }
            Err(SchedulerError::UnknownJob { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownJob)
                    .with_message("job is not registered")
            }
            Err(SchedulerError::UnknownTask { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownTask)
                    .with_message("task is not registered")
            }
            Err(SchedulerError::UnknownExecutor { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownExecutor)
                    .with_message("executor is not registered")
            }
            Err(SchedulerError::StaleExecutorLease { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleLease)
                    .with_message("executor lease generation is stale")
            }
            Err(SchedulerError::StaleTaskAttempt { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleAttempt)
                    .with_message("task attempt is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }
}

/// Networked gRPC adapter for coordinator/executor transport calls.
#[derive(Debug, Clone)]
pub struct CoordinatorExecutorGrpcService {
    inner: CoordinatorExecutorTonicService,
}

impl CoordinatorExecutorGrpcService {
    /// Create a network service from a shared coordinator.
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self {
            inner: CoordinatorExecutorTonicService::new(coordinator),
        }
    }

    /// Shared coordinator backing this service.
    pub fn coordinator(&self) -> &SharedCoordinator {
        self.inner.coordinator()
    }
}

#[tonic::async_trait]
impl wire::v1::coordinator_executor_server::CoordinatorExecutor for CoordinatorExecutorGrpcService {
    async fn register_executor(
        &self,
        request: tonic::Request<wire::v1::RegisterExecutorRequest>,
    ) -> Result<tonic::Response<wire::v1::RegisterExecutorResponse>, tonic::Status> {
        let request = wire::register_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .register_executor(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::register_executor_response_to_wire(response),
        ))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<wire::v1::ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<wire::v1::ExecutorHeartbeatResponse>, tonic::Status> {
        let request = wire::executor_heartbeat_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .executor_heartbeat(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::executor_heartbeat_response_to_wire(response),
        ))
    }

    async fn task_status(
        &self,
        request: tonic::Request<wire::v1::TaskStatusRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::task_status_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .task_status(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }
}

/// Build the generated tonic server around the scheduler-backed gRPC adapter.
pub fn coordinator_executor_grpc_server(
    coordinator: SharedCoordinator,
) -> wire::v1::coordinator_executor_server::CoordinatorExecutorServer<CoordinatorExecutorGrpcService>
{
    wire::v1::coordinator_executor_server::CoordinatorExecutorServer::new(
        CoordinatorExecutorGrpcService::new(coordinator),
    )
}

/// Serve the coordinator/executor gRPC API on a socket address.
pub async fn serve_coordinator_executor_grpc(
    addr: SocketAddr,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(coordinator_executor_grpc_server(coordinator))
        .serve(addr)
        .await
}

/// Serve the coordinator/executor gRPC API on an already-bound listener.
pub async fn serve_coordinator_executor_grpc_with_listener(
    listener: tokio::net::TcpListener,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(coordinator_executor_grpc_server(coordinator))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

fn ensure_transport_version(version: TransportVersion) -> Result<(), tonic::Status> {
    if TransportVersion::CURRENT.is_compatible_with(version) {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(format!(
            "unsupported coordinator/executor transport version {version}; current version is {}",
            TransportVersion::CURRENT
        )))
    }
}

fn status_from_wire_error(error: wire::WireError) -> tonic::Status {
    tonic::Status::invalid_argument(error.to_string())
}

fn status_from_scheduler_error(error: SchedulerError) -> tonic::Status {
    match error {
        SchedulerError::InactiveCoordinator { .. } => {
            tonic::Status::failed_precondition(error.to_string())
        }
        SchedulerError::StaleExecutorLease { .. } | SchedulerError::StaleTaskAttempt { .. } => {
            tonic::Status::failed_precondition(error.to_string())
        }
        SchedulerError::UnknownExecutor { .. }
        | SchedulerError::UnknownJob { .. }
        | SchedulerError::UnknownStage { .. }
        | SchedulerError::UnknownTask { .. } => tonic::Status::not_found(error.to_string()),
        SchedulerError::DuplicateExecutor { .. } | SchedulerError::DuplicateJob { .. } => {
            tonic::Status::already_exists(error.to_string())
        }
        SchedulerError::NoExecutors
        | SchedulerError::InvalidJob { .. }
        | SchedulerError::InvalidPlan { .. } => tonic::Status::invalid_argument(error.to_string()),
    }
}

impl Coordinator {
    /// Create an active R2 coordinator.
    pub fn active(coordinator_id: CoordinatorId) -> Self {
        Self::active_with_config(coordinator_id, CoordinatorConfig::default())
    }

    /// Create an active R2 coordinator with explicit config.
    pub fn active_with_config(coordinator_id: CoordinatorId, config: CoordinatorConfig) -> Self {
        Self {
            coordinator_id,
            state: CoordinatorState::Active,
            config,
            executors: ExecutorRegistry::new(config.heartbeat_timeout_ticks()),
            jobs: Vec::new(),
        }
    }

    /// Create a standby R2 coordinator.
    pub fn standby(coordinator_id: CoordinatorId) -> Self {
        Self::standby_with_config(coordinator_id, CoordinatorConfig::default())
    }

    /// Create a standby R2 coordinator with explicit config.
    pub fn standby_with_config(coordinator_id: CoordinatorId, config: CoordinatorConfig) -> Self {
        Self {
            coordinator_id,
            state: CoordinatorState::Standby,
            config,
            executors: ExecutorRegistry::new(config.heartbeat_timeout_ticks()),
            jobs: Vec::new(),
        }
    }

    /// Coordinator id.
    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    /// Coordinator state.
    pub fn state(&self) -> CoordinatorState {
        self.state
    }

    /// Coordinator config.
    pub fn config(&self) -> CoordinatorConfig {
        self.config
    }

    /// Register an executor with the active coordinator.
    pub fn register_executor(
        &mut self,
        descriptor: ExecutorDescriptor,
    ) -> SchedulerResult<LeaseGeneration> {
        self.ensure_active()?;
        self.executors.register(descriptor)
    }

    /// Apply an executor heartbeat.
    pub fn executor_heartbeat(&mut self, heartbeat: ExecutorHeartbeat) -> SchedulerResult<()> {
        self.ensure_active()?;
        self.executors.heartbeat(heartbeat)
    }

    /// Mark an executor lost, which is the R2 timeout skeleton.
    pub fn mark_executor_lost(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        self.ensure_active()?;
        self.executors.mark_lost(executor_id)
    }

    /// Advance the deterministic heartbeat clock and mark timed-out executors lost.
    pub fn advance_heartbeat_clock(&mut self, ticks: u64) -> SchedulerResult<Vec<ExecutorId>> {
        self.ensure_active()?;
        Ok(self.executors.advance_clock(ticks))
    }

    /// Submit a job and statically assign its tasks.
    pub fn submit_job(&mut self, spec: JobSpec) -> SchedulerResult<()> {
        self.ensure_active()?;
        validate_job(&spec)?;

        if self.jobs.iter().any(|job| job.job_id() == spec.job_id()) {
            return Err(SchedulerError::DuplicateJob {
                job_id: spec.job_id().clone(),
            });
        }

        let executors = self.executors.schedulable_executors();
        let assignments = StaticScheduler::place(&spec, &executors)?;
        let mut record = JobRecord::from_spec(spec, self.config.max_stage_retries());
        record.apply_assignments(assignments);
        self.jobs.push(record);
        Ok(())
    }

    /// Convert and submit a Krishiv logical DAG through the R2 scheduler.
    pub fn submit_logical_plan(
        &mut self,
        job_id: JobId,
        plan: &LogicalPlan,
    ) -> SchedulerResult<()> {
        self.submit_job(job_spec_from_logical_plan(job_id, plan)?)
    }

    /// Convert and submit a Krishiv physical DAG through the R2 scheduler.
    pub fn submit_physical_plan(
        &mut self,
        job_id: JobId,
        plan: &PhysicalPlan,
    ) -> SchedulerResult<()> {
        self.submit_job(job_spec_from_physical_plan(job_id, plan)?)
    }

    /// Launch all assigned tasks for a job.
    pub fn launch_assigned_tasks(&mut self, job_id: &JobId) -> SchedulerResult<usize> {
        self.launch_assigned_task_assignments(job_id)
            .map(|assignments| assignments.len())
    }

    /// Launch all assigned tasks for a job and return executor transport assignments.
    pub fn launch_assigned_task_assignments(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        self.ensure_active()?;
        let executor_leases = self.executors.assignment_leases();
        self.find_job_mut(job_id)?
            .launch_assigned_task_assignments(&executor_leases)
    }

    /// Apply a task update from an executor.
    pub fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        self.ensure_active()?;
        self.executors
            .validate_lease(update.executor_id(), update.lease_generation())?;
        self.find_job_mut(update.job_id())?
            .apply_task_update(update)
    }

    /// Snapshot one job.
    pub fn job_snapshot(&self, job_id: &JobId) -> SchedulerResult<JobSnapshot> {
        self.find_job(job_id).map(JobRecord::snapshot)
    }

    /// Snapshot one job with stage and task detail.
    pub fn job_detail_snapshot(&self, job_id: &JobId) -> SchedulerResult<JobDetailSnapshot> {
        self.find_job(job_id).map(JobRecord::detail_snapshot)
    }

    /// Snapshot all known jobs.
    pub fn job_snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs.iter().map(JobRecord::snapshot).collect()
    }

    /// Snapshot all known executors.
    pub fn executor_snapshots(&self) -> Vec<ExecutorRecord> {
        self.executors.list().to_vec()
    }

    fn ensure_active(&self) -> SchedulerResult<()> {
        if self.state == CoordinatorState::Active {
            Ok(())
        } else {
            Err(SchedulerError::InactiveCoordinator {
                coordinator_id: self.coordinator_id.clone(),
                state: self.state,
            })
        }
    }

    fn find_job(&self, job_id: &JobId) -> SchedulerResult<&JobRecord> {
        self.jobs
            .iter()
            .find(|job| job.job_id() == job_id)
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }

    fn find_job_mut(&mut self, job_id: &JobId) -> SchedulerResult<&mut JobRecord> {
        self.jobs
            .iter_mut()
            .find(|job| job.job_id() == job_id)
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }
}

/// Convert a Krishiv logical plan into an R2 distributed job spec.
pub fn job_spec_from_logical_plan(job_id: JobId, plan: &LogicalPlan) -> SchedulerResult<JobSpec> {
    job_spec_from_plan_parts(job_id, plan.name(), plan.kind(), plan.nodes())
}

/// Convert a Krishiv physical plan into an R2 distributed job spec.
pub fn job_spec_from_physical_plan(job_id: JobId, plan: &PhysicalPlan) -> SchedulerResult<JobSpec> {
    job_spec_from_plan_parts(job_id, plan.name(), plan.kind(), plan.nodes())
}

/// Executor registry skeleton.
#[derive(Debug, Clone)]
pub struct ExecutorRegistry {
    executors: Vec<ExecutorRecord>,
    current_tick: u64,
    heartbeat_timeout_ticks: u64,
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new(CoordinatorConfig::default().heartbeat_timeout_ticks())
    }
}

impl ExecutorRegistry {
    /// Create an executor registry with deterministic heartbeat timeout ticks.
    pub fn new(heartbeat_timeout_ticks: u64) -> Self {
        Self {
            executors: Vec::new(),
            current_tick: 0,
            heartbeat_timeout_ticks: heartbeat_timeout_ticks.max(1),
        }
    }

    /// Register an executor.
    pub fn register(&mut self, descriptor: ExecutorDescriptor) -> SchedulerResult<LeaseGeneration> {
        if let Some(executor) = self
            .executors
            .iter()
            .find(|executor| executor.executor_id() == descriptor.executor_id())
        {
            if executor.state().can_accept_work() || executor.state() == ExecutorState::Draining {
                return Err(SchedulerError::DuplicateExecutor {
                    executor_id: descriptor.executor_id().clone(),
                });
            }
        }

        if let Some(executor) = self
            .executors
            .iter_mut()
            .find(|executor| executor.executor_id() == descriptor.executor_id())
        {
            executor.descriptor = descriptor;
            executor.state = ExecutorState::Registered;
            executor.running_tasks.clear();
            executor.last_heartbeat_tick = self.current_tick;
            return Ok(executor.lease_generation);
        }

        let lease_generation = LeaseGeneration::initial();
        self.executors.push(ExecutorRecord::new(
            descriptor,
            self.current_tick,
            lease_generation,
        ));
        Ok(lease_generation)
    }

    /// Apply a heartbeat.
    pub fn heartbeat(&mut self, heartbeat: ExecutorHeartbeat) -> SchedulerResult<()> {
        let current_tick = self.current_tick;
        let executor = self.find_executor_mut(heartbeat.executor_id())?;
        validate_executor_lease(
            heartbeat.executor_id(),
            executor.lease_generation(),
            heartbeat.lease_generation(),
        )?;

        executor.state = heartbeat.state();
        executor.running_tasks = heartbeat.running_tasks().to_vec();
        executor.last_heartbeat_tick = current_tick;
        Ok(())
    }

    /// Mark an executor lost.
    pub fn mark_lost(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        let executor = self.find_executor_mut(executor_id)?;

        executor.state = ExecutorState::Lost;
        executor.running_tasks.clear();
        executor.lease_generation = executor.lease_generation.next();
        Ok(())
    }

    /// Advance the deterministic heartbeat clock.
    pub fn advance_clock(&mut self, ticks: u64) -> Vec<ExecutorId> {
        self.current_tick = self.current_tick.saturating_add(ticks);
        let mut lost = Vec::new();

        for executor in &mut self.executors {
            if executor.state().can_accept_work()
                && self
                    .current_tick
                    .saturating_sub(executor.last_heartbeat_tick)
                    >= self.heartbeat_timeout_ticks
            {
                executor.state = ExecutorState::Lost;
                executor.running_tasks.clear();
                executor.lease_generation = executor.lease_generation.next();
                lost.push(executor.executor_id().clone());
            }
        }

        lost
    }

    /// List registered executors.
    pub fn list(&self) -> &[ExecutorRecord] {
        &self.executors
    }

    /// Current deterministic heartbeat tick.
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Validate an executor lease generation and return the current generation.
    pub fn validate_lease(
        &self,
        executor_id: &ExecutorId,
        lease_generation: LeaseGeneration,
    ) -> SchedulerResult<LeaseGeneration> {
        let executor = self.find_executor(executor_id)?;
        validate_executor_lease(executor_id, executor.lease_generation(), lease_generation)?;
        Ok(executor.lease_generation())
    }

    fn assignment_leases(&self) -> Vec<(ExecutorId, LeaseGeneration)> {
        self.executors
            .iter()
            .map(|executor| (executor.executor_id().clone(), executor.lease_generation()))
            .collect()
    }

    fn schedulable_executors(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .iter()
            .filter(|executor| {
                executor.state().can_accept_work() && executor.descriptor().slots() > 0
            })
            .map(|executor| executor.descriptor().clone())
            .collect()
    }

    fn find_executor(&self, executor_id: &ExecutorId) -> SchedulerResult<&ExecutorRecord> {
        self.executors
            .iter()
            .find(|executor| executor.executor_id() == executor_id)
            .ok_or_else(|| SchedulerError::UnknownExecutor {
                executor_id: executor_id.clone(),
            })
    }

    fn find_executor_mut(
        &mut self,
        executor_id: &ExecutorId,
    ) -> SchedulerResult<&mut ExecutorRecord> {
        self.executors
            .iter_mut()
            .find(|executor| executor.executor_id() == executor_id)
            .ok_or_else(|| SchedulerError::UnknownExecutor {
                executor_id: executor_id.clone(),
            })
    }
}

fn validate_executor_lease(
    executor_id: &ExecutorId,
    expected: LeaseGeneration,
    received: LeaseGeneration,
) -> SchedulerResult<()> {
    if received == expected {
        Ok(())
    } else {
        Err(SchedulerError::StaleExecutorLease {
            executor_id: executor_id.clone(),
            expected,
            received,
        })
    }
}

/// Executor registry record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRecord {
    descriptor: ExecutorDescriptor,
    lease_generation: LeaseGeneration,
    state: ExecutorState,
    running_tasks: Vec<TaskId>,
    last_heartbeat_tick: u64,
}

impl ExecutorRecord {
    fn new(
        descriptor: ExecutorDescriptor,
        last_heartbeat_tick: u64,
        lease_generation: LeaseGeneration,
    ) -> Self {
        Self {
            descriptor,
            lease_generation,
            state: ExecutorState::Registered,
            running_tasks: Vec::new(),
            last_heartbeat_tick,
        }
    }

    /// Executor descriptor.
    pub fn descriptor(&self) -> &ExecutorDescriptor {
        &self.descriptor
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        self.descriptor.executor_id()
    }

    /// Executor state.
    pub fn state(&self) -> ExecutorState {
        self.state
    }

    /// Current lease generation for this executor.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Running task ids last reported by heartbeat.
    pub fn running_tasks(&self) -> &[TaskId] {
        &self.running_tasks
    }

    /// Last deterministic heartbeat tick.
    pub fn last_heartbeat_tick(&self) -> u64 {
        self.last_heartbeat_tick
    }
}

/// Static R2 task placement.
#[derive(Debug, Clone, Default)]
pub struct StaticScheduler;

impl StaticScheduler {
    /// Place tasks round-robin across schedulable executors.
    pub fn place(
        spec: &JobSpec,
        executors: &[ExecutorDescriptor],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }

        let mut assignments = Vec::with_capacity(spec.task_count());
        for (idx, task) in spec.stages().iter().flat_map(StageSpec::tasks).enumerate() {
            let executor = &executors[idx % executors.len()];
            assignments.push(TaskAssignment::new(
                task.task_id().clone(),
                executor.executor_id().clone(),
            ));
        }

        Ok(assignments)
    }
}

/// Job record owned by the active coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    spec: JobSpec,
    state: JobState,
    max_stage_retries: u32,
    stages: Vec<StageRecord>,
}

impl JobRecord {
    fn from_spec(spec: JobSpec, max_stage_retries: u32) -> Self {
        let stages = spec
            .stages()
            .iter()
            .cloned()
            .map(StageRecord::from_spec)
            .collect();
        Self {
            spec,
            state: JobState::Accepted,
            max_stage_retries,
            stages,
        }
    }

    /// Job id.
    pub fn job_id(&self) -> &JobId {
        self.spec.job_id()
    }

    /// Job state.
    pub fn state(&self) -> JobState {
        self.state
    }

    /// Stage records.
    pub fn stages(&self) -> &[StageRecord] {
        &self.stages
    }

    fn apply_assignments(&mut self, assignments: Vec<TaskAssignment>) {
        self.state = JobState::Running;
        for stage in &mut self.stages {
            stage.state = StageState::Scheduling;
            for task in &mut stage.tasks {
                if let Some(assignment) = assignments
                    .iter()
                    .find(|assignment| assignment.task_id() == task.task_id())
                {
                    task.assigned_executor = Some(assignment.executor_id().clone());
                    task.state = TaskState::Assigned;
                }
            }
        }
    }

    fn launch_assigned_task_assignments(
        &mut self,
        executor_leases: &[(ExecutorId, LeaseGeneration)],
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        let mut assignments = Vec::new();
        self.state = JobState::Running;
        for stage in &mut self.stages {
            let stage_id = stage.stage_id().clone();
            for task in &mut stage.tasks {
                if task.state == TaskState::Assigned {
                    let executor_id = task.assigned_executor.clone().ok_or_else(|| {
                        SchedulerError::InvalidJob {
                            message: format!(
                                "task {} is assigned without an executor",
                                task.task_id()
                            ),
                        }
                    })?;
                    let lease_generation = executor_leases
                        .iter()
                        .find_map(|(known_executor, lease_generation)| {
                            (known_executor == &executor_id).then_some(*lease_generation)
                        })
                        .ok_or_else(|| SchedulerError::UnknownExecutor {
                            executor_id: executor_id.clone(),
                        })?;

                    task.state = TaskState::Running;
                    task.attempt = task.attempt.saturating_add(1);
                    let attempt_id = AttemptId::try_new(task.attempt).map_err(|error| {
                        SchedulerError::InvalidJob {
                            message: error.to_string(),
                        }
                    })?;
                    let task_description = task.spec.description().to_owned();
                    assignments.push(
                        ExecutorTaskAssignment::new(
                            TaskAttemptRef::new(
                                self.spec.job_id().clone(),
                                stage_id.clone(),
                                task.task_id().clone(),
                                attempt_id,
                            ),
                            executor_id,
                            lease_generation,
                            PlanFragment::new(task_description.clone()),
                            OutputContract::new(
                                OutputContractKind::InlineRecordBatches,
                                format!("inline result for {}", task.task_id()),
                            ),
                        )
                        .with_input_partitions(vec![InputPartition::new(
                            task.task_id().as_str(),
                            task_description,
                        )]),
                    );
                }
            }
            if stage
                .tasks
                .iter()
                .any(|task| task.state == TaskState::Running)
            {
                stage.state = StageState::Running;
            }
        }
        Ok(assignments)
    }

    fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        let stage = self
            .stages
            .iter_mut()
            .find(|stage| stage.stage_id() == update.stage_id())
            .ok_or_else(|| SchedulerError::UnknownStage {
                stage_id: update.stage_id().clone(),
            })?;

        let outcome = stage.apply_task_update(update, self.max_stage_retries)?;
        self.refresh_state();
        Ok(outcome)
    }

    fn refresh_state(&mut self) {
        if self
            .stages
            .iter()
            .all(|stage| stage.state == StageState::Succeeded)
        {
            self.state = JobState::Succeeded;
        } else if self
            .stages
            .iter()
            .any(|stage| stage.state == StageState::Failed)
        {
            self.state = JobState::Failed;
        } else {
            self.state = JobState::Running;
        }
    }

    fn snapshot(&self) -> JobSnapshot {
        let mut task_count = 0;
        let mut assigned_task_count = 0;
        let mut running_task_count = 0;
        let mut succeeded_task_count = 0;
        let mut failed_task_count = 0;

        for task in self.stages.iter().flat_map(StageRecord::tasks) {
            task_count += 1;
            match task.state() {
                TaskState::Assigned => assigned_task_count += 1,
                TaskState::Running => running_task_count += 1,
                TaskState::Succeeded => succeeded_task_count += 1,
                TaskState::Failed => failed_task_count += 1,
                TaskState::Pending | TaskState::Retrying | TaskState::Cancelled => {}
            }
        }

        JobSnapshot {
            job_id: self.spec.job_id().clone(),
            kind: self.spec.kind(),
            state: self.state,
            stage_count: self.stages.len(),
            task_count,
            assigned_task_count,
            running_task_count,
            succeeded_task_count,
            failed_task_count,
        }
    }

    fn detail_snapshot(&self) -> JobDetailSnapshot {
        JobDetailSnapshot {
            job: self.snapshot(),
            stages: self.stages.iter().map(StageRecord::snapshot).collect(),
        }
    }
}

/// Stage record owned by a job coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRecord {
    spec: StageSpec,
    state: StageState,
    retry_count: u32,
    tasks: Vec<TaskRecord>,
}

impl StageRecord {
    fn from_spec(spec: StageSpec) -> Self {
        let tasks = spec
            .tasks()
            .iter()
            .cloned()
            .map(TaskRecord::from_spec)
            .collect();
        Self {
            spec,
            state: StageState::Pending,
            retry_count: 0,
            tasks,
        }
    }

    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        self.spec.stage_id()
    }

    /// Stage state.
    pub fn state(&self) -> StageState {
        self.state
    }

    /// Task records.
    pub fn tasks(&self) -> &[TaskRecord] {
        &self.tasks
    }

    /// Number of stage-level retries already scheduled.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
        max_stage_retries: u32,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.task_id() == update.task_id())
            .ok_or_else(|| SchedulerError::UnknownTask {
                task_id: update.task_id().clone(),
            })?;

        let outcome = task.apply_status_update(&update)?;
        if outcome == TaskUpdateOutcome::Duplicate {
            return Ok(outcome);
        }

        if update.state() == TaskState::Failed && self.retry_count < max_stage_retries {
            self.retry_stage();
            return Ok(TaskUpdateOutcome::Applied);
        }
        self.refresh_state();
        Ok(TaskUpdateOutcome::Applied)
    }

    fn retry_stage(&mut self) {
        self.retry_count = self.retry_count.saturating_add(1);
        self.state = StageState::Retrying;

        for task in &mut self.tasks {
            task.state = if task.assigned_executor.is_some() {
                TaskState::Assigned
            } else {
                TaskState::Pending
            };
        }
    }

    fn refresh_state(&mut self) {
        if self
            .tasks
            .iter()
            .all(|task| task.state == TaskState::Succeeded)
        {
            self.state = StageState::Succeeded;
        } else if self
            .tasks
            .iter()
            .any(|task| task.state == TaskState::Failed)
        {
            self.state = StageState::Failed;
        } else if self
            .tasks
            .iter()
            .any(|task| task.state == TaskState::Running)
        {
            self.state = StageState::Running;
        } else if self
            .tasks
            .iter()
            .any(|task| task.state == TaskState::Assigned)
        {
            self.state = StageState::Scheduling;
        } else {
            self.state = StageState::Pending;
        }
    }

    fn snapshot(&self) -> StageSnapshot {
        StageSnapshot {
            stage_id: self.spec.stage_id().clone(),
            state: self.state,
            retry_count: self.retry_count,
            task_count: self.tasks.len(),
            tasks: self.tasks.iter().map(TaskRecord::snapshot).collect(),
        }
    }
}

/// Task record owned by a job coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    spec: TaskSpec,
    state: TaskState,
    assigned_executor: Option<ExecutorId>,
    attempt: u32,
}

impl TaskRecord {
    fn from_spec(spec: TaskSpec) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
            assigned_executor: None,
            attempt: 0,
        }
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        self.spec.task_id()
    }

    /// Task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Assigned executor, if any.
    pub fn assigned_executor(&self) -> Option<&ExecutorId> {
        self.assigned_executor.as_ref()
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    fn apply_status_update(
        &mut self,
        update: &TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        if update.attempt() != self.attempt {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        if self.attempt == 0 {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        if self.assigned_executor.as_ref() != Some(update.executor_id()) {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        if self.state == update.state() {
            return Ok(TaskUpdateOutcome::Duplicate);
        }

        if self.state.is_terminal()
            || (self.state != TaskState::Running && update.state() != TaskState::Running)
        {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        self.state = update.state();
        self.assigned_executor = Some(update.executor_id().clone());
        self.attempt = update.attempt();
        Ok(TaskUpdateOutcome::Applied)
    }

    fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            task_id: self.spec.task_id().clone(),
            state: self.state,
            assigned_executor: self.assigned_executor.clone(),
            attempt: self.attempt,
        }
    }
}

/// Job status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSnapshot {
    job_id: JobId,
    kind: JobKind,
    state: JobState,
    stage_count: usize,
    task_count: usize,
    assigned_task_count: usize,
    running_task_count: usize,
    succeeded_task_count: usize,
    failed_task_count: usize,
}

impl JobSnapshot {
    /// Job id.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Job kind.
    pub fn kind(&self) -> JobKind {
        self.kind
    }

    /// Job state.
    pub fn state(&self) -> JobState {
        self.state
    }

    /// Number of stages.
    pub fn stage_count(&self) -> usize {
        self.stage_count
    }

    /// Number of tasks.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// Number of assigned tasks.
    pub fn assigned_task_count(&self) -> usize {
        self.assigned_task_count
    }

    /// Number of running tasks.
    pub fn running_task_count(&self) -> usize {
        self.running_task_count
    }

    /// Number of succeeded tasks.
    pub fn succeeded_task_count(&self) -> usize {
        self.succeeded_task_count
    }

    /// Number of failed tasks.
    pub fn failed_task_count(&self) -> usize {
        self.failed_task_count
    }
}

/// Detailed job status for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDetailSnapshot {
    job: JobSnapshot,
    stages: Vec<StageSnapshot>,
}

impl JobDetailSnapshot {
    /// Job summary.
    pub fn job(&self) -> &JobSnapshot {
        &self.job
    }

    /// Stage summaries.
    pub fn stages(&self) -> &[StageSnapshot] {
        &self.stages
    }
}

/// Stage status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSnapshot {
    stage_id: StageId,
    state: StageState,
    retry_count: u32,
    task_count: usize,
    tasks: Vec<TaskSnapshot>,
}

impl StageSnapshot {
    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        &self.stage_id
    }

    /// Stage state.
    pub fn state(&self) -> StageState {
        self.state
    }

    /// Number of stage-level retries already scheduled.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Number of tasks in this stage.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// Task summaries.
    pub fn tasks(&self) -> &[TaskSnapshot] {
        &self.tasks
    }
}

/// Task status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    task_id: TaskId,
    state: TaskState,
    assigned_executor: Option<ExecutorId>,
    attempt: u32,
}

impl TaskSnapshot {
    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Assigned executor, if any.
    pub fn assigned_executor(&self) -> Option<&ExecutorId> {
        self.assigned_executor.as_ref()
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

fn validate_job(spec: &JobSpec) -> SchedulerResult<()> {
    if spec.stages().is_empty() {
        return Err(SchedulerError::InvalidJob {
            message: String::from("job must contain at least one stage"),
        });
    }
    if spec.stages().iter().any(|stage| stage.tasks().is_empty()) {
        return Err(SchedulerError::InvalidJob {
            message: String::from("each stage must contain at least one task"),
        });
    }
    Ok(())
}

fn job_spec_from_plan_parts(
    job_id: JobId,
    plan_name: &str,
    kind: PlanExecutionKind,
    nodes: &[PlanNode],
) -> SchedulerResult<JobSpec> {
    let job_kind = match kind {
        PlanExecutionKind::Batch => JobKind::Batch,
        PlanExecutionKind::Streaming => JobKind::Streaming,
    };
    let job_name = if plan_name.trim().is_empty() {
        String::from("unnamed-distributed-dag")
    } else {
        plan_name.to_owned()
    };
    let stage_id = StageId::try_new("stage-1").map_err(|error| SchedulerError::InvalidPlan {
        message: error.to_string(),
    })?;

    let mut stage = StageSpec::new(stage_id, format!("{job_name}-stage"));
    if nodes.is_empty() {
        let task_id = TaskId::try_new("task-1").map_err(|error| SchedulerError::InvalidPlan {
            message: error.to_string(),
        })?;
        stage = stage.with_task(TaskSpec::new(
            task_id,
            format!("{job_kind} plan task for {job_name}"),
        ));
    } else {
        for (idx, node) in nodes.iter().enumerate() {
            let task_id = TaskId::try_new(format!("task-{}", idx + 1)).map_err(|error| {
                SchedulerError::InvalidPlan {
                    message: error.to_string(),
                }
            })?;
            stage = stage.with_task(TaskSpec::new(task_id, plan_node_description(node)));
        }
    }

    Ok(JobSpec::new(job_id, job_name, job_kind).with_stage(stage))
}

fn plan_node_description(node: &PlanNode) -> String {
    if node.inputs().is_empty() {
        format!("{} [{}] {}", node.id(), node.kind(), node.label())
    } else {
        format!(
            "{} [{}] {} <- {}",
            node.id(),
            node.kind(),
            node.label(),
            node.inputs().join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind as PlanExecutionKind, LogicalPlan, PhysicalPlan, PlanNode};
    use krishiv_proto::{
        AttemptId, CoordinatorExecutorService, CoordinatorId, ExecutorDescriptor,
        ExecutorHeartbeat, ExecutorHeartbeatRequest, ExecutorId, ExecutorState, JobId, JobKind,
        JobSpec, JobState, LeaseGeneration, RegisterExecutorRequest, StageId, StageSpec,
        TaskAttemptRef, TaskId, TaskSpec, TaskState, TaskStatusRequest, TaskStatusUpdate,
        TransportDisposition, wire,
    };

    use super::{
        Coordinator, CoordinatorConfig, CoordinatorExecutorTonicService, ExecutorRegistry,
        SchedulerError, SharedCoordinator, StaticScheduler, TaskUpdateOutcome,
        job_spec_from_logical_plan, serve_coordinator_executor_grpc_with_listener,
    };

    #[test]
    fn standby_coordinator_rejects_mutation() {
        let mut coordinator = Coordinator::standby(CoordinatorId::try_new("coord-1").unwrap());
        let executor = ExecutorDescriptor::new(ExecutorId::try_new("exec-1").unwrap(), "pod-a", 1);

        let error = coordinator.register_executor(executor).unwrap_err();

        assert!(matches!(error, SchedulerError::InactiveCoordinator { .. }));
    }

    #[test]
    fn executor_registry_accepts_registration_and_heartbeat() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut registry = ExecutorRegistry::default();
        registry
            .register(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        registry
            .heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        assert_eq!(registry.list().len(), 1);
        assert_eq!(registry.list()[0].state(), ExecutorState::Healthy);
        assert_eq!(registry.list()[0].last_heartbeat_tick(), 0);
    }

    #[test]
    fn heartbeat_timeout_marks_executor_lost() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(1, 2),
        );
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        assert!(coordinator.advance_heartbeat_clock(1).unwrap().is_empty());
        let lost = coordinator.advance_heartbeat_clock(1).unwrap();

        assert_eq!(lost, vec![executor_id]);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );
    }

    #[test]
    fn stale_lease_heartbeat_is_rejected_after_executor_loss() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let lease_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        coordinator.mark_executor_lost(&executor_id).unwrap();
        let current_generation = coordinator.executor_snapshots()[0].lease_generation();
        let error = coordinator
            .executor_heartbeat(
                ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                    .with_lease_generation(lease_generation),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            SchedulerError::StaleExecutorLease {
                expected,
                received,
                ..
            } if expected == current_generation && received == lease_generation
        ));
    }

    #[test]
    fn lost_executor_can_reregister_with_next_lease_generation() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let initial_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        coordinator.mark_executor_lost(&executor_id).unwrap();
        let next_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-b", 2))
            .unwrap();

        assert_eq!(next_generation, initial_generation.next());
        let executor = &coordinator.executor_snapshots()[0];
        assert_eq!(executor.state(), ExecutorState::Registered);
        assert_eq!(executor.descriptor().host(), "pod-b");
        assert_eq!(executor.descriptor().slots(), 2);
        assert_eq!(executor.lease_generation(), next_generation);
    }

    #[test]
    fn shared_coordinator_exposes_same_scheduler_state_to_clones() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let observer = shared.clone();
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
                .unwrap();
            coordinator
                .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
                .unwrap();
        }

        let coordinator = observer.read().unwrap();
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
    }

    #[tokio::test]
    async fn tonic_service_registers_executor_through_shared_coordinator() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        let response = service
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2),
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(response.lease_generation(), LeaseGeneration::initial());
        let coordinator = shared.read().unwrap();
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].executor_id(),
            &executor_id
        );
    }

    #[tokio::test]
    async fn tonic_service_applies_executor_heartbeat_to_shared_coordinator() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let task_id = TaskId::try_new("task-1").unwrap();

        service
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2),
            )))
            .await
            .unwrap();

        let heartbeat = ExecutorHeartbeatRequest::new(
            executor_id.clone(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_running_attempts(vec![TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            task_id.clone(),
            AttemptId::initial(),
        )]);
        let response = service
            .executor_heartbeat(tonic::Request::new(heartbeat))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        let coordinator = shared.read().unwrap();
        let executor = &coordinator.executor_snapshots()[0];
        assert_eq!(executor.state(), ExecutorState::Healthy);
        assert_eq!(executor.running_tasks(), &[task_id]);
    }

    #[tokio::test]
    async fn tonic_service_reports_unknown_executor_heartbeat_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared);

        let response = service
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                ExecutorId::try_new("missing-exec").unwrap(),
                LeaseGeneration::initial(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            response.disposition(),
            TransportDisposition::UnknownExecutor
        );
    }

    #[tokio::test]
    async fn tonic_service_reports_stale_lease_heartbeat_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
                .unwrap();
            coordinator.mark_executor_lost(&executor_id).unwrap();
        }

        let response = service
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                executor_id,
                LeaseGeneration::initial(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::StaleLease);
        assert_eq!(
            response.lease_generation(),
            LeaseGeneration::initial().next()
        );
    }

    #[tokio::test]
    async fn grpc_service_registers_and_heartbeats_over_network() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping networked gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind test gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_shared = shared.clone();
        let server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(listener, server_shared)
                .await
                .unwrap();
        });

        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            format!("http://{addr}"),
        )
        .await
        .unwrap();
        let executor_id = ExecutorId::try_new("exec-network-1").unwrap();
        let registration = client
            .register_executor(wire::register_executor_request_to_wire(
                RegisterExecutorRequest::new(ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-network",
                    2,
                )),
            ))
            .await
            .unwrap()
            .into_inner();
        let registration = wire::register_executor_response_from_wire(registration).unwrap();

        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        assert_eq!(registration.executor_id(), &executor_id);

        let heartbeat = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                ExecutorHeartbeatRequest::new(
                    executor_id.clone(),
                    LeaseGeneration::initial(),
                    ExecutorState::Healthy,
                ),
            ))
            .await
            .unwrap()
            .into_inner();
        let heartbeat = wire::executor_heartbeat_response_from_wire(heartbeat).unwrap();

        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);
        {
            let coordinator = shared.read().unwrap();
            assert_eq!(coordinator.executor_snapshots().len(), 1);
            assert_eq!(
                coordinator.executor_snapshots()[0].state(),
                ExecutorState::Healthy
            );
        }

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();
        {
            let mut coordinator = shared.write().unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let task_status = client
            .task_status(wire::task_status_request_to_wire(TaskStatusRequest::new(
                TaskAttemptRef::new(job_id, stage_id, task_id, AttemptId::initial()),
                executor_id,
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();
        let task_status = wire::task_status_response_from_wire(task_status).unwrap();

        assert_eq!(task_status.disposition(), TransportDisposition::Accepted);

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn tonic_service_routes_task_status_updates() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
                .unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let status = TaskStatusRequest::new(
            TaskAttemptRef::new(job_id.clone(), stage_id, task_id, AttemptId::initial()),
            executor_id,
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        );
        let response = service
            .task_status(tonic::Request::new(status))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(
            shared
                .read()
                .unwrap()
                .job_snapshot(&job_id)
                .unwrap()
                .state(),
            JobState::Running
        );
    }

    #[tokio::test]
    async fn tonic_service_reports_duplicate_task_status_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();
        let ids = TaskAttemptRef::new(
            job_id.clone(),
            stage_id.clone(),
            task_id.clone(),
            AttemptId::initial(),
        );

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
                .unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let accepted = service
            .task_status(tonic::Request::new(TaskStatusRequest::new(
                ids.clone(),
                executor_id.clone(),
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();
        let duplicate = service
            .task_status(tonic::Request::new(TaskStatusRequest::new(
                ids,
                executor_id,
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(accepted.disposition(), TransportDisposition::Accepted);
        assert_eq!(duplicate.disposition(), TransportDisposition::Duplicate);
    }

    #[tokio::test]
    async fn tonic_service_reports_stale_task_attempt_as_domain_response() {
        let shared = SharedCoordinator::new(Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(1, 3),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
                .unwrap();
            coordinator.submit_job(job).unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
            coordinator
                .apply_task_update(TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id.clone(),
                    task_id.clone(),
                    executor_id.clone(),
                    TaskState::Failed,
                    1,
                ))
                .unwrap();
            coordinator.launch_assigned_tasks(&job_id).unwrap();
        }

        let response = service
            .task_status(tonic::Request::new(TaskStatusRequest::new(
                TaskAttemptRef::new(job_id, stage_id, task_id, AttemptId::initial()),
                executor_id,
                LeaseGeneration::initial(),
                TaskState::Succeeded,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::StaleAttempt);
    }

    #[test]
    fn coordinator_rejects_task_status_with_stale_executor_lease() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();
        let stale_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator.mark_executor_lost(&executor_id).unwrap();

        let error = coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id,
                    stage_id,
                    task_id,
                    executor_id,
                    TaskState::Succeeded,
                    1,
                )
                .with_lease_generation(stale_generation),
            )
            .unwrap_err();

        assert!(matches!(error, SchedulerError::StaleExecutorLease { .. }));
    }

    #[test]
    fn duplicate_terminal_task_status_is_idempotent() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();

        let update = TaskStatusUpdate::new(
            job_id.clone(),
            stage_id,
            task_id,
            executor_id,
            TaskState::Succeeded,
            1,
        );
        assert_eq!(
            coordinator.apply_task_update(update.clone()).unwrap(),
            TaskUpdateOutcome::Applied
        );
        assert_eq!(
            coordinator.apply_task_update(update).unwrap(),
            TaskUpdateOutcome::Duplicate
        );
        assert_eq!(
            coordinator
                .job_snapshot(&job_id)
                .unwrap()
                .succeeded_task_count(),
            1
        );
    }

    #[test]
    fn coordinator_launch_returns_executor_task_assignments_with_attempt_and_lease() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let lease_generation = coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        let job = demo_job();
        let job_id = job.job_id().clone();

        coordinator.submit_job(job).unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].job_id(), &job_id);
        assert_eq!(assignments[0].executor_id(), &executor_id);
        assert_eq!(assignments[0].attempt_id(), AttemptId::initial());
        assert_eq!(assignments[0].lease_generation(), lease_generation);
        assert_eq!(
            assignments[0].output_contract().kind(),
            krishiv_proto::OutputContractKind::InlineRecordBatches
        );
        assert!(!assignments[0].input_partitions().is_empty());
        assert!(
            coordinator
                .job_snapshot(&job_id)
                .unwrap()
                .running_task_count()
                > 0
        );
    }

    #[test]
    fn static_scheduler_places_tasks_round_robin() {
        let job = demo_job();
        let executors = vec![
            ExecutorDescriptor::new(ExecutorId::try_new("exec-a").unwrap(), "pod-a", 1),
            ExecutorDescriptor::new(ExecutorId::try_new("exec-b").unwrap(), "pod-b", 1),
        ];

        let assignments = StaticScheduler::place(&job, &executors).unwrap();

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].executor_id().as_str(), "exec-a");
        assert_eq!(assignments[1].executor_id().as_str(), "exec-b");
    }

    #[test]
    fn converts_batch_logical_plan_into_distributed_job_spec() {
        let plan = LogicalPlan::new("batch-dag", PlanExecutionKind::Batch)
            .with_node(PlanNode::new(
                "scan",
                "scan parquet",
                PlanExecutionKind::Batch,
            ))
            .with_node(
                PlanNode::new("aggregate", "count", PlanExecutionKind::Batch).with_inputs(["scan"]),
            );

        let job = job_spec_from_logical_plan(JobId::try_new("job-batch").unwrap(), &plan).unwrap();

        assert_eq!(job.kind(), JobKind::Batch);
        assert_eq!(job.name(), "batch-dag");
        assert_eq!(job.task_count(), 2);
        assert!(job.stages()[0].tasks()[1].description().contains("scan"));
    }

    #[test]
    fn coordinator_routes_batch_logical_plan_through_scheduler() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                2,
            ))
            .unwrap();

        let plan = LogicalPlan::new("batch-dag", PlanExecutionKind::Batch)
            .with_node(PlanNode::new(
                "scan",
                "scan parquet",
                PlanExecutionKind::Batch,
            ))
            .with_node(
                PlanNode::new("project", "project columns", PlanExecutionKind::Batch)
                    .with_inputs(["scan"]),
            );
        let job_id = JobId::try_new("job-batch").unwrap();

        coordinator
            .submit_logical_plan(job_id.clone(), &plan)
            .unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();

        assert_eq!(snapshot.kind(), JobKind::Batch);
        assert_eq!(snapshot.task_count(), 2);
        assert_eq!(snapshot.assigned_task_count(), 2);
        assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
        assert_eq!(
            coordinator
                .job_snapshot(&job_id)
                .unwrap()
                .running_task_count(),
            2
        );
    }

    #[test]
    fn coordinator_routes_streaming_physical_plan_with_local_state_semantics() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-1").unwrap(),
                "pod-a",
                1,
            ))
            .unwrap();

        let plan =
            PhysicalPlan::new("stream-dag", PlanExecutionKind::Streaming).with_node(PlanNode::new(
                "memory-source",
                "local memory stream",
                PlanExecutionKind::Streaming,
            ));
        let job_id = JobId::try_new("job-stream").unwrap();

        coordinator
            .submit_physical_plan(job_id.clone(), &plan)
            .unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();

        assert_eq!(snapshot.kind(), JobKind::Streaming);
        assert_eq!(snapshot.task_count(), 1);
        assert_eq!(snapshot.assigned_task_count(), 1);
    }

    #[test]
    fn empty_plan_routes_as_single_distributed_task() {
        let plan = PhysicalPlan::new("empty-physical", PlanExecutionKind::Batch);

        let job = super::job_spec_from_physical_plan(JobId::try_new("job-empty").unwrap(), &plan)
            .unwrap();

        assert_eq!(job.kind(), JobKind::Batch);
        assert_eq!(job.task_count(), 1);
        assert!(
            job.stages()[0].tasks()[0]
                .description()
                .contains("empty-physical")
        );
    }

    #[test]
    fn coordinator_submits_launches_and_completes_job() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let first_task = job.stages()[0].tasks()[0].task_id().clone();
        let second_task = job.stages()[0].tasks()[1].task_id().clone();

        coordinator.submit_job(job).unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.assigned_task_count(), 2);

        assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.running_task_count(), 2);

        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                first_task,
                executor_id.clone(),
                TaskState::Succeeded,
                1,
            ))
            .unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id,
                second_task,
                executor_id,
                TaskState::Succeeded,
                1,
            ))
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 2);

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages().len(), 1);
        assert_eq!(detail.stages()[0].tasks().len(), 2);
        assert_eq!(coordinator.job_snapshots().len(), 1);
    }

    #[test]
    fn task_failure_marks_stage_and_job_failed() {
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(0, 3),
        );
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let task_id = job.stages()[0].tasks()[0].task_id().clone();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id,
                    task_id,
                    executor_id,
                    TaskState::Failed,
                    1,
                )
                .with_message("executor reported failure"),
            )
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Failed);
        assert_eq!(snapshot.failed_task_count(), 1);
    }

    #[test]
    fn task_failure_retries_entire_stage_before_terminal_failure() {
        let mut coordinator = Coordinator::active_with_config(
            CoordinatorId::try_new("coord-1").unwrap(),
            CoordinatorConfig::new(1, 3),
        );
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();

        let job = demo_job();
        let job_id = job.job_id().clone();
        let stage_id = job.stages()[0].stage_id().clone();
        let first_task = job.stages()[0].tasks()[0].task_id().clone();
        let second_task = job.stages()[0].tasks()[1].task_id().clone();

        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                first_task.clone(),
                executor_id.clone(),
                TaskState::Failed,
                1,
            ))
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Running);
        assert_eq!(snapshot.assigned_task_count(), 2);
        assert_eq!(snapshot.failed_task_count(), 0);

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].retry_count(), 1);
        assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Assigned);
        assert_eq!(detail.stages()[0].tasks()[1].state(), TaskState::Assigned);

        assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].tasks()[0].attempt(), 2);
        assert_eq!(detail.stages()[0].tasks()[1].attempt(), 2);

        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                first_task,
                executor_id.clone(),
                TaskState::Succeeded,
                2,
            ))
            .unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id,
                second_task,
                executor_id,
                TaskState::Succeeded,
                2,
            ))
            .unwrap();

        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 2);
    }

    #[test]
    fn coordinator_marks_executor_lost() {
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();

        coordinator.mark_executor_lost(&executor_id).unwrap();

        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );
    }

    fn demo_job() -> JobSpec {
        JobSpec::new(
            JobId::try_new("job-1").unwrap(),
            "demo batch",
            JobKind::Batch,
        )
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a"))
                .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "scan b")),
        )
    }
}
