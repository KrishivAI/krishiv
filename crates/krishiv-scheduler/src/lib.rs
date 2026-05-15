#![forbid(unsafe_code)]

//! R2 in-process scheduler skeleton.
//!
//! This crate starts the distributed control-plane model without introducing
//! Kubernetes clients or network transports. R2 keeps one active coordinator
//! and replaceable executors; later slices can map these structs to services.

use std::error::Error;
use std::fmt;

use krishiv_proto::{
    CoordinatorId, CoordinatorState, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId,
    ExecutorState, JobId, JobSpec, JobState, StageId, StageSpec, StageState, TaskAssignment,
    TaskId, TaskSpec, TaskState, TaskStatusUpdate,
};

/// Scheduler result alias.
pub type SchedulerResult<T> = Result<T, SchedulerError>;

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
    /// Job submission was invalid.
    InvalidJob { message: String },
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
            Self::NoExecutors => f.write_str("no healthy executors are available"),
            Self::DuplicateJob { job_id } => write!(f, "job already exists: {job_id}"),
            Self::UnknownJob { job_id } => write!(f, "unknown job: {job_id}"),
            Self::UnknownStage { stage_id } => write!(f, "unknown stage: {stage_id}"),
            Self::UnknownTask { task_id } => write!(f, "unknown task: {task_id}"),
            Self::InvalidJob { message } => write!(f, "invalid job: {message}"),
        }
    }
}

impl Error for SchedulerError {}

/// R2 coordinator skeleton.
#[derive(Debug, Clone)]
pub struct Coordinator {
    coordinator_id: CoordinatorId,
    state: CoordinatorState,
    executors: ExecutorRegistry,
    jobs: Vec<JobRecord>,
}

impl Coordinator {
    /// Create an active R2 coordinator.
    pub fn active(coordinator_id: CoordinatorId) -> Self {
        Self {
            coordinator_id,
            state: CoordinatorState::Active,
            executors: ExecutorRegistry::default(),
            jobs: Vec::new(),
        }
    }

    /// Create a standby R2 coordinator.
    pub fn standby(coordinator_id: CoordinatorId) -> Self {
        Self {
            coordinator_id,
            state: CoordinatorState::Standby,
            executors: ExecutorRegistry::default(),
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

    /// Register an executor with the active coordinator.
    pub fn register_executor(&mut self, descriptor: ExecutorDescriptor) -> SchedulerResult<()> {
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
        let mut record = JobRecord::from_spec(spec);
        record.apply_assignments(assignments);
        self.jobs.push(record);
        Ok(())
    }

    /// Launch all assigned tasks for a job.
    pub fn launch_assigned_tasks(&mut self, job_id: &JobId) -> SchedulerResult<usize> {
        self.ensure_active()?;
        self.find_job_mut(job_id)?.launch_assigned_tasks()
    }

    /// Apply a task update from an executor.
    pub fn apply_task_update(&mut self, update: TaskStatusUpdate) -> SchedulerResult<()> {
        self.ensure_active()?;
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

/// Executor registry skeleton.
#[derive(Debug, Clone, Default)]
pub struct ExecutorRegistry {
    executors: Vec<ExecutorRecord>,
}

impl ExecutorRegistry {
    /// Register an executor.
    pub fn register(&mut self, descriptor: ExecutorDescriptor) -> SchedulerResult<()> {
        if self
            .executors
            .iter()
            .any(|executor| executor.executor_id() == descriptor.executor_id())
        {
            return Err(SchedulerError::DuplicateExecutor {
                executor_id: descriptor.executor_id().clone(),
            });
        }

        self.executors.push(ExecutorRecord::new(descriptor));
        Ok(())
    }

    /// Apply a heartbeat.
    pub fn heartbeat(&mut self, heartbeat: ExecutorHeartbeat) -> SchedulerResult<()> {
        let executor = self
            .executors
            .iter_mut()
            .find(|executor| executor.executor_id() == heartbeat.executor_id())
            .ok_or_else(|| SchedulerError::UnknownExecutor {
                executor_id: heartbeat.executor_id().clone(),
            })?;

        executor.state = heartbeat.state();
        executor.running_tasks = heartbeat.running_tasks().to_vec();
        Ok(())
    }

    /// Mark an executor lost.
    pub fn mark_lost(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        let executor = self
            .executors
            .iter_mut()
            .find(|executor| executor.executor_id() == executor_id)
            .ok_or_else(|| SchedulerError::UnknownExecutor {
                executor_id: executor_id.clone(),
            })?;

        executor.state = ExecutorState::Lost;
        executor.running_tasks.clear();
        Ok(())
    }

    /// List registered executors.
    pub fn list(&self) -> &[ExecutorRecord] {
        &self.executors
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
}

/// Executor registry record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRecord {
    descriptor: ExecutorDescriptor,
    state: ExecutorState,
    running_tasks: Vec<TaskId>,
}

impl ExecutorRecord {
    fn new(descriptor: ExecutorDescriptor) -> Self {
        Self {
            descriptor,
            state: ExecutorState::Registered,
            running_tasks: Vec::new(),
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

    /// Running task ids last reported by heartbeat.
    pub fn running_tasks(&self) -> &[TaskId] {
        &self.running_tasks
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
    stages: Vec<StageRecord>,
}

impl JobRecord {
    fn from_spec(spec: JobSpec) -> Self {
        let stages = spec
            .stages()
            .iter()
            .cloned()
            .map(StageRecord::from_spec)
            .collect();
        Self {
            spec,
            state: JobState::Accepted,
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

    fn launch_assigned_tasks(&mut self) -> SchedulerResult<usize> {
        let mut launched = 0;
        self.state = JobState::Running;
        for stage in &mut self.stages {
            for task in &mut stage.tasks {
                if task.state == TaskState::Assigned {
                    task.state = TaskState::Running;
                    launched += 1;
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
        Ok(launched)
    }

    fn apply_task_update(&mut self, update: TaskStatusUpdate) -> SchedulerResult<()> {
        let stage = self
            .stages
            .iter_mut()
            .find(|stage| stage.stage_id() == update.stage_id())
            .ok_or_else(|| SchedulerError::UnknownStage {
                stage_id: update.stage_id().clone(),
            })?;

        stage.apply_task_update(update)?;
        self.refresh_state();
        Ok(())
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

    fn apply_task_update(&mut self, update: TaskStatusUpdate) -> SchedulerResult<()> {
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.task_id() == update.task_id())
            .ok_or_else(|| SchedulerError::UnknownTask {
                task_id: update.task_id().clone(),
            })?;

        task.state = update.state();
        task.assigned_executor = Some(update.executor_id().clone());
        task.attempt = update.attempt();
        self.refresh_state();
        Ok(())
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

#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
        JobKind, JobSpec, JobState, StageId, StageSpec, TaskId, TaskSpec, TaskState,
        TaskStatusUpdate,
    };

    use super::{Coordinator, ExecutorRegistry, SchedulerError, StaticScheduler};

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
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
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
