use krishiv_proto::{ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, LeaseGeneration, TaskId};

use crate::{CoordinatorConfig, SchedulerError, SchedulerResult};

/// Memory and task load snapshot from an executor heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorHealthSnapshot {
    /// Memory used, as reported by the executor.
    pub memory_used_bytes: Option<u64>,
    /// Memory limit, as reported by the executor.
    pub memory_limit_bytes: Option<u64>,
    /// Active task count, as reported by the executor.
    pub active_task_count: Option<u32>,
}

/// Executor registry skeleton.
#[derive(Debug, Clone)]
pub struct ExecutorRegistry {
    pub(crate) executors: Vec<ExecutorRecord>,
    pub(crate) current_tick: u64,
    pub(crate) heartbeat_timeout_ticks: u64,
    pub(crate) memory_threshold_bytes: Option<u64>,
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new(CoordinatorConfig::default().heartbeat_timeout_ticks(), None)
    }
}

impl ExecutorRegistry {
    /// Create an executor registry with deterministic heartbeat timeout ticks.
    pub fn new(heartbeat_timeout_ticks: u64, memory_threshold_bytes: Option<u64>) -> Self {
        Self {
            executors: Vec::new(),
            current_tick: 0,
            heartbeat_timeout_ticks: heartbeat_timeout_ticks.max(1),
            memory_threshold_bytes,
        }
    }

    /// Register an executor.
    ///
    /// GAP-CP-07: Idempotent re-registration with lease bump.  When an executor
    /// re-registers (e.g. after a coordinator restart where state is in memory,
    /// or after a network partition), the lease generation is bumped so all
    /// in-flight heartbeats with the old generation are rejected.  This prevents
    /// zombie executors from submitting stale task updates.
    pub fn register(&mut self, descriptor: ExecutorDescriptor) -> SchedulerResult<LeaseGeneration> {
        if let Some(executor) = self
            .executors
            .iter_mut()
            .find(|e| e.executor_id() == descriptor.executor_id())
        {
            // Idempotent re-registration: bump lease only when the executor was
            // still in a healthy state.  mark_lost / deregister already bump the
            // lease, so re-registering from Lost/Removed keeps the current
            // generation rather than incrementing it a second time.
            let was_alive = executor.state.can_accept_work()
                || matches!(executor.state, ExecutorState::Draining);
            let new_lease = if was_alive {
                executor.lease_generation.next()
            } else {
                executor.lease_generation
            };
            executor.descriptor = descriptor;
            executor.state = ExecutorState::Registered;
            executor.running_tasks.clear();
            executor.last_heartbeat_tick = self.current_tick;
            executor.health_snapshot = None;
            executor.lease_generation = new_lease;
            return Ok(new_lease);
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
        executor.health_snapshot = Some(ExecutorHealthSnapshot {
            memory_used_bytes: heartbeat.memory_used_bytes(),
            memory_limit_bytes: heartbeat.memory_limit_bytes(),
            active_task_count: heartbeat.active_task_count(),
        });
        Ok(())
    }

    /// Deregister an executor through the graceful fast path.
    pub fn deregister(
        &mut self,
        executor_id: &ExecutorId,
        lease_generation: LeaseGeneration,
    ) -> SchedulerResult<LeaseGeneration> {
        let executor = self.find_executor_mut(executor_id)?;
        validate_executor_lease(executor_id, executor.lease_generation(), lease_generation)?;
        executor.state = ExecutorState::Removed;
        executor.running_tasks.clear();
        executor.lease_generation = executor.lease_generation.next();
        Ok(executor.lease_generation)
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

    pub(crate) fn assignment_leases(&self) -> Vec<(ExecutorId, LeaseGeneration)> {
        self.executors
            .iter()
            .map(|executor| (executor.executor_id().clone(), executor.lease_generation()))
            .collect()
    }

    pub(crate) fn heartbeat_ages(&self) -> Vec<ExecutorHeartbeatAge> {
        self.executors
            .iter()
            .map(|executor| ExecutorHeartbeatAge {
                executor_id: executor.executor_id().clone(),
                age_ticks: self
                    .current_tick
                    .saturating_sub(executor.last_heartbeat_tick()),
            })
            .collect()
    }

    /// P2.5: Return borrowed references instead of cloning every descriptor.
    pub(crate) fn schedulable_executors(&self) -> Vec<&ExecutorDescriptor> {
        self.executors
            .iter()
            .filter(|executor| {
                if !executor.state().can_accept_work() || executor.descriptor().slots() == 0 {
                    return false;
                }
                if let Some(threshold) = self.memory_threshold_bytes
                    && let Some(snapshot) = &executor.health_snapshot
                    && let Some(used) = snapshot.memory_used_bytes
                    && used >= threshold
                {
                    return false;
                }
                true
            })
            .map(|executor| executor.descriptor())
            .collect()
    }

    pub(crate) fn find_executor(&self, executor_id: &ExecutorId) -> SchedulerResult<&ExecutorRecord> {
        self.executors
            .iter()
            .find(|executor| executor.executor_id() == executor_id)
            .ok_or_else(|| SchedulerError::UnknownExecutor {
                executor_id: executor_id.clone(),
            })
    }

    pub(crate) fn find_executor_mut(
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

pub(crate) fn validate_executor_lease(
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
    pub(crate) descriptor: ExecutorDescriptor,
    pub(crate) lease_generation: LeaseGeneration,
    pub(crate) state: ExecutorState,
    pub(crate) running_tasks: Vec<TaskId>,
    pub(crate) last_heartbeat_tick: u64,
    pub(crate) health_snapshot: Option<ExecutorHealthSnapshot>,
}

impl ExecutorRecord {
    pub(crate) fn new(
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
            health_snapshot: None,
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

    /// Most recent health snapshot from the executor heartbeat, if any.
    pub fn health_snapshot(&self) -> Option<&ExecutorHealthSnapshot> {
        self.health_snapshot.as_ref()
    }
}

/// Heartbeat age for one executor in deterministic scheduler ticks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorHeartbeatAge {
    pub(crate) executor_id: ExecutorId,
    pub(crate) age_ticks: u64,
}

impl ExecutorHeartbeatAge {
    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Heartbeat age in deterministic scheduler ticks.
    pub fn age_ticks(&self) -> u64 {
        self.age_ticks
    }
}
