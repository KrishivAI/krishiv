use std::collections::HashMap;

use krishiv_proto::{
    ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, LeaseGeneration,
    ResourceProfile, TaskId,
};

use crate::job::ExecutorPlacement;
use crate::{CoordinatorConfig, SchedulerError, SchedulerResult};

/// Memory and task load snapshot from an executor heartbeat.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorHealthSnapshot {
    /// Memory used, as reported by the executor.
    pub memory_used_bytes: Option<u64>,
    /// Memory limit, as reported by the executor.
    pub memory_limit_bytes: Option<u64>,
    /// Active task count, as reported by the executor.
    pub active_task_count: Option<u32>,
    /// Available CPU cores, as reported by the executor.
    pub cpu_cores_used: Option<f64>,
    /// Cumulative network bytes sent, as reported by the executor.
    pub network_bytes_sent: Option<u64>,
    /// Cumulative network bytes received, as reported by the executor.
    pub network_bytes_recv: Option<u64>,
}

/// Executor registry backed by `HashMap` for O(1) lookup on the hot heartbeat path.
#[derive(Debug, Clone)]
pub struct ExecutorRegistry {
    pub(crate) executors: HashMap<ExecutorId, ExecutorRecord>,
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
            executors: HashMap::new(),
            current_tick: 0,
            heartbeat_timeout_ticks: heartbeat_timeout_ticks.max(1),
            memory_threshold_bytes,
        }
    }

    /// Register an executor.
    ///
    /// GAP-CP-07: Idempotent re-registration with lease bump.
    pub fn register(&mut self, descriptor: ExecutorDescriptor) -> SchedulerResult<LeaseGeneration> {
        let executor_id = descriptor.executor_id().clone();
        if let Some(executor) = self.executors.get_mut(&executor_id) {
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
        self.executors.insert(
            executor_id.clone(),
            ExecutorRecord::new(descriptor, self.current_tick, lease_generation),
        );
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
            cpu_cores_used: heartbeat.cpu_cores_used(),
            network_bytes_sent: heartbeat.network_bytes_sent(),
            network_bytes_recv: heartbeat.network_bytes_recv(),
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

    /// Mark an executor as Draining (T13 / SC5).
    ///
    /// `EXECUTOR_DECOMMISSION_SIGNAL` semantics: the executor should finish
    /// its current work, receive no new task assignments, and serve shuffle
    /// fetches for an additional `decom_grace_ticks` so in-flight consumers
    /// can still pull data after the executor's tasks have finished.
    ///
    /// The function is idempotent: calling it on an already-Draining or
    /// Lost executor is a no-op and returns the current `lease_generation`.
    /// The scheduler task-assignment path checks
    /// `ExecutorState::can_accept_work()` and therefore naturally excludes
    /// Draining executors from new launches.
    pub fn drain_executor(&mut self, executor_id: &ExecutorId) -> SchedulerResult<LeaseGeneration> {
        let executor = self.find_executor_mut(executor_id)?;
        match executor.state {
            ExecutorState::Draining | ExecutorState::Removed | ExecutorState::Lost => {
                Ok(executor.lease_generation)
            }
            _ => {
                executor.state = ExecutorState::Draining;
                Ok(executor.lease_generation)
            }
        }
    }

    /// Advance the deterministic heartbeat clock.
    pub fn advance_clock(&mut self, ticks: u64) -> Vec<ExecutorId> {
        // `HashSet::new()` does not allocate until first insertion, so delegating
        // with an empty protected set is free on this hot path.
        self.advance_clock_excluding(ticks, &std::collections::HashSet::new())
    }

    /// Advance the heartbeat clock, but never evict an executor whose id is in
    /// `protected`.
    ///
    /// Recovery uses this to give streaming executors owning running tasks the
    /// re-attach grace window instead of tearing them down immediately (P1.23):
    /// the clock still advances, so a protected executor is evicted on a later
    /// grace-aware tick if it never re-registers.
    pub fn advance_clock_excluding(
        &mut self,
        ticks: u64,
        protected: &std::collections::HashSet<ExecutorId>,
    ) -> Vec<ExecutorId> {
        self.current_tick = self.current_tick.saturating_add(ticks);
        let mut lost = Vec::new();

        // Terminal records (Lost / Removed) are kept for a retention window so
        // zombie-fencing still sees the bumped lease generation, then pruned.
        // Without this the registry grows without bound under executor churn
        // (every k8s pod restart is a new executor id) and every tick iterates
        // the corpses. 40× the heartbeat timeout (≥ 360 ticks ≈ 30 min at the
        // 5 s daemon tick) is far longer than any in-flight RPC can survive.
        let retention_ticks = self.heartbeat_timeout_ticks.saturating_mul(40).max(360);
        let current_tick = self.current_tick;
        self.executors.retain(|_, executor| {
            !(matches!(
                executor.state,
                ExecutorState::Lost | ExecutorState::Removed
            ) && current_tick.saturating_sub(executor.last_heartbeat_tick) >= retention_ticks)
        });

        for executor in self.executors.values_mut() {
            if protected.contains(executor.executor_id()) {
                continue;
            }
            // DIST-2: Also evict Draining executors that stop heartbeating.
            // Previously only Registered|Healthy were checked, so a crashed
            // Draining executor persisted in the registry forever.
            if matches!(
                executor.state(),
                ExecutorState::Registered | ExecutorState::Healthy | ExecutorState::Draining
            ) && self
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

    /// List registered executors as a cloned Vec.
    pub fn list(&self) -> Vec<ExecutorRecord> {
        self.executors.values().cloned().collect()
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
            .map(|(id, executor)| (id.clone(), executor.lease_generation()))
            .collect()
    }

    pub(crate) fn heartbeat_ages(&self) -> Vec<ExecutorHeartbeatAge> {
        self.executors
            .values()
            .map(|executor| ExecutorHeartbeatAge {
                executor_id: executor.executor_id().clone(),
                age_ticks: self
                    .current_tick
                    .saturating_sub(executor.last_heartbeat_tick()),
            })
            .collect()
    }

    pub(crate) fn schedulable_executors(&self) -> Vec<&ExecutorDescriptor> {
        self.executors
            .values()
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

    pub(crate) fn schedulable_executor_placements(&self) -> Vec<ExecutorPlacement> {
        let mut placements: Vec<_> = self
            .executors
            .values()
            .filter(|executor| self.is_schedulable(executor))
            .map(|executor| {
                let active_tasks = executor
                    .health_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.active_task_count)
                    .map(|count| count as usize)
                    .unwrap_or_else(|| executor.running_tasks.len());
                ExecutorPlacement::new(
                    executor.executor_id().clone(),
                    executor.descriptor().slots(),
                    active_tasks,
                )
            })
            .collect();
        placements.sort_by(|a, b| a.executor_id.as_str().cmp(b.executor_id.as_str()));
        placements
    }

    fn is_schedulable(&self, executor: &ExecutorRecord) -> bool {
        self.is_schedulable_for_profile(executor, None)
    }

    /// Returns `true` when `executor` is schedulable AND satisfies the given
    /// SC10 resource profile requirements.
    ///
    /// Memory check: if `profile.task_memory_bytes > 0` and the executor
    /// reports both `memory_limit_bytes` and `memory_used_bytes` in its last
    /// heartbeat, the executor is only considered eligible when
    /// `available_memory >= task_memory_bytes`.  When the executor has not
    /// reported memory capacity (common in unit tests and bare-metal deploys
    /// without the health shim), the check is skipped so assignments still
    /// proceed.
    ///
    /// CPU check (future): `task_cpus` is stored on the profile but not yet
    /// used for placement filtering; it is reserved for a follow-up that adds
    /// per-slot CPU accounting.
    fn is_schedulable_for_profile(
        &self,
        executor: &ExecutorRecord,
        profile: Option<&ResourceProfile>,
    ) -> bool {
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
        // SC10: per-task memory requirement check.
        if let Some(p) = profile
            && p.task_memory_bytes > 0
            && let Some(snapshot) = &executor.health_snapshot
            && let (Some(limit), Some(used)) =
                (snapshot.memory_limit_bytes, snapshot.memory_used_bytes)
        {
            let available = limit.saturating_sub(used);
            if available < p.task_memory_bytes {
                return false;
            }
        }
        true
    }

    /// SC10: return placements filtered by `resource_profile`.
    ///
    /// When `profile` is `None` this is identical to
    /// [`schedulable_executor_placements`].
    pub(crate) fn schedulable_placements_for_profile(
        &self,
        profile: Option<&ResourceProfile>,
    ) -> Vec<ExecutorPlacement> {
        let mut placements: Vec<_> = self
            .executors
            .values()
            .filter(|executor| self.is_schedulable_for_profile(executor, profile))
            .map(|executor| {
                let active_tasks = executor
                    .health_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.active_task_count)
                    .map(|c| c as usize)
                    .unwrap_or_else(|| executor.running_tasks.len());
                ExecutorPlacement::new(
                    executor.executor_id().clone(),
                    executor.descriptor().slots(),
                    active_tasks,
                )
            })
            .collect();
        placements.sort_by(|a, b| a.executor_id.as_str().cmp(b.executor_id.as_str()));
        placements
    }

    /// Sum of available memory across schedulable executors that report
    /// memory capacity in their heartbeats.
    ///
    /// Returns `None` when no schedulable executor reports a memory limit —
    /// callers must treat that as "capacity unknown" and skip memory-based
    /// admission decisions rather than rejecting all work.
    pub(crate) fn cluster_available_memory_bytes(&self) -> Option<u64> {
        let mut total: Option<u64> = None;
        for executor in self.executors.values() {
            if !executor.state().can_accept_work() {
                continue;
            }
            let Some(snapshot) = &executor.health_snapshot else {
                continue;
            };
            let Some(limit) = snapshot.memory_limit_bytes else {
                continue;
            };
            let used = snapshot.memory_used_bytes.unwrap_or(0);
            let available = limit.saturating_sub(used);
            total = Some(total.unwrap_or(0).saturating_add(available));
        }
        total
    }

    pub(crate) fn find_executor(
        &self,
        executor_id: &ExecutorId,
    ) -> SchedulerResult<&ExecutorRecord> {
        self.executors
            .get(executor_id)
            .ok_or_else(|| SchedulerError::UnknownExecutor {
                executor_id: executor_id.clone(),
            })
    }

    pub(crate) fn find_executor_mut(
        &mut self,
        executor_id: &ExecutorId,
    ) -> SchedulerResult<&mut ExecutorRecord> {
        self.executors
            .get_mut(executor_id)
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
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorRecord {
    pub(crate) descriptor: ExecutorDescriptor,
    pub(crate) lease_generation: LeaseGeneration,
    pub(crate) state: ExecutorState,
    pub(crate) running_tasks: Vec<TaskId>,
    pub(crate) last_heartbeat_tick: u64,
    pub(crate) health_snapshot: Option<ExecutorHealthSnapshot>,
    /// Simple consecutive task failure counter. Used as foundation for
    /// Phase 3 circuit breaker (executor-level load shedding).
    pub(crate) consecutive_task_failures: u32,
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
            consecutive_task_failures: 0,
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

    /// Consecutive task failure count (circuit-breaker input).
    pub fn consecutive_task_failures(&self) -> u32 {
        self.consecutive_task_failures
    }

    /// Increment failure counter (called on task failure reports from this executor).
    /// Returns true if the executor has now exceeded the given threshold (circuit break candidate).
    pub fn record_task_failure(&mut self, threshold: u32) -> bool {
        if threshold == 0 {
            return false;
        }
        self.consecutive_task_failures = self.consecutive_task_failures.saturating_add(1);
        self.consecutive_task_failures >= threshold
    }

    /// Reset failure counter (called on successful task or healthy heartbeat).
    pub fn reset_task_failures(&mut self) {
        self.consecutive_task_failures = 0;
    }
}

/// Extension on ExecutorRegistry for circuit breaker logic.
impl ExecutorRegistry {
    /// Record a task failure for a specific executor.
    /// Returns true if this executor has now crossed the given threshold
    /// and should be temporarily avoided for new assignments.
    pub fn record_task_failure(&mut self, executor_id: &ExecutorId, threshold: u32) -> bool {
        self.executors
            .get_mut(executor_id)
            .is_some_and(|record| record.record_task_failure(threshold))
    }

    /// Reset failure count for an executor (e.g. after it reports healthy progress).
    pub fn reset_task_failures(&mut self, executor_id: &ExecutorId) {
        if let Some(record) = self.executors.get_mut(executor_id) {
            record.reset_task_failures();
        }
    }

    /// Return list of executors that currently exceed the failure threshold.
    pub fn executors_over_failure_threshold(&self, threshold: u32) -> Vec<ExecutorId> {
        if threshold == 0 {
            return Vec::new();
        }
        self.executors
            .values()
            .filter(|e| e.consecutive_task_failures >= threshold)
            .map(|e| e.executor_id().clone())
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::{ExecutorId, ExecutorState};

    fn make_descriptor(id: &str) -> ExecutorDescriptor {
        ExecutorDescriptor::new(
            ExecutorId::try_new(id).expect("id"),
            "test-host".to_string(),
            4,
        )
    }

    // ── Executor eviction (worker-failure path) ───────────────────────────────

    /// An executor that misses `heartbeat_timeout_ticks` consecutive ticks is
    /// marked Lost and returned by `advance_clock`. This is the primary
    /// mechanism by which the coordinator detects dead workers.
    #[test]
    fn advance_clock_evicts_stale_executor_after_timeout() {
        let timeout = 3u64;
        let mut registry = ExecutorRegistry::new(timeout, None);
        let id = ExecutorId::try_new("executor-1").expect("id");

        registry
            .register(make_descriptor("executor-1"))
            .expect("register");
        // Transition to Healthy via the first heartbeat (Registered does not
        // satisfy can_accept_work, so the timeout check is skipped).
        registry
            .heartbeat(ExecutorHeartbeat::new(id.clone(), ExecutorState::Healthy))
            .expect("heartbeat");

        // Advance just below timeout — should not evict.
        let lost = registry.advance_clock(timeout - 1);
        assert!(lost.is_empty(), "not yet past timeout: {lost:?}");
        assert_eq!(
            registry.find_executor(&id).unwrap().state(),
            ExecutorState::Healthy,
        );

        // Advance one more tick — should evict.
        let lost = registry.advance_clock(1);
        assert_eq!(lost, vec![id.clone()], "executor must be evicted");
        assert_eq!(
            registry.find_executor(&id).unwrap().state(),
            ExecutorState::Lost,
        );
    }

    /// `advance_clock_excluding` protects a streaming executor in the recovery
    /// grace window from eviction, while still advancing the clock for everyone
    /// else. After the grace window expires (protected set emptied), the same
    /// executor is evicted normally.
    #[test]
    fn advance_clock_excluding_protects_grace_window_executor() {
        let timeout = 2u64;
        let mut registry = ExecutorRegistry::new(timeout, None);
        let protected_id = ExecutorId::try_new("streaming-exec").expect("id");
        let normal_id = ExecutorId::try_new("normal-exec").expect("id");

        registry
            .register(make_descriptor("streaming-exec"))
            .expect("register");
        registry
            .register(make_descriptor("normal-exec"))
            .expect("register");

        // Both transition to Healthy.
        for id in [&protected_id, &normal_id] {
            registry
                .heartbeat(ExecutorHeartbeat::new(id.clone(), ExecutorState::Healthy))
                .expect("heartbeat");
        }

        let mut protected = std::collections::HashSet::new();
        protected.insert(protected_id.clone());

        // Advance past timeout while protected set is active — only normal-exec evicted.
        let lost = registry.advance_clock_excluding(timeout + 1, &protected);
        assert!(
            lost.contains(&normal_id),
            "unprotected executor must be evicted"
        );
        assert!(
            !lost.contains(&protected_id),
            "protected executor must survive grace window"
        );
        assert_eq!(
            registry.find_executor(&protected_id).unwrap().state(),
            ExecutorState::Healthy,
            "protected executor still Healthy inside grace window"
        );

        // Second advance with empty protected set — grace window expired, evicted now.
        let lost = registry.advance_clock_excluding(timeout + 1, &std::collections::HashSet::new());
        assert!(
            lost.contains(&protected_id),
            "executor evicted once grace window expires"
        );
    }

    /// An executor that re-registers after being marked Lost gets a new (bumped)
    /// lease generation, so stale heartbeats from the zombie process are rejected.
    #[test]
    fn reregistration_after_loss_bumps_lease_generation() {
        let mut registry = ExecutorRegistry::new(1, None);
        let id = ExecutorId::try_new("executor-1").expect("id");

        let gen0 = registry
            .register(make_descriptor("executor-1"))
            .expect("register");
        registry
            .heartbeat(ExecutorHeartbeat::new(id.clone(), ExecutorState::Healthy))
            .expect("heartbeat");

        // Force eviction.
        let lost = registry.advance_clock(2);
        assert!(lost.contains(&id));
        let gen_lost = registry.find_executor(&id).unwrap().lease_generation();
        assert!(gen_lost > gen0, "eviction must bump the lease generation");

        // Re-register simulates the executor process restarting.
        let gen_restarted = registry
            .register(make_descriptor("executor-1"))
            .expect("re-register");

        // `validate_lease` with the old lease generation must fail — a stale
        // heartbeat from the zombie process should be rejected.
        let stale_result = registry.validate_lease(&id, gen0);
        assert!(
            stale_result.is_err(),
            "stale lease must be rejected after re-registration"
        );

        // A heartbeat from the new process with the new lease must be accepted.
        registry
            .heartbeat(
                ExecutorHeartbeat::new(id.clone(), ExecutorState::Healthy)
                    .with_lease_generation(gen_restarted),
            )
            .expect("fresh heartbeat must be accepted");
    }

    #[test]
    fn drain_executor_transitions_to_draining_and_is_idempotent() {
        let mut registry = ExecutorRegistry::new(64, None);
        let id = ExecutorId::try_new("executor-1").expect("id");
        let gen0 = registry
            .register(make_descriptor("executor-1"))
            .expect("register");
        // Simulate the heartbeat path: the executor transitions to Healthy
        // after the first heartbeat.
        registry
            .heartbeat(ExecutorHeartbeat::new(id.clone(), ExecutorState::Healthy))
            .expect("heartbeat");
        assert_eq!(
            registry.find_executor(&id).unwrap().state(),
            ExecutorState::Healthy
        );

        // T13: first drain transitions to Draining. The drain is
        // idempotent on the lease — it does not advance the generation
        // because the existing tasks on the executor should keep running
        // under the same lease. Generation advance happens later via the
        // normal `mark_lost` / `register_again` path.
        let gen1 = registry.drain_executor(&id).expect("first drain");
        assert_eq!(gen1, gen0, "drain must NOT advance the lease generation");
        assert_eq!(
            registry.find_executor(&id).unwrap().state(),
            ExecutorState::Draining
        );
        // `can_accept_work` returns false for Draining — task assignment
        // naturally excludes the executor.
        assert!(
            !registry
                .find_executor(&id)
                .unwrap()
                .state()
                .can_accept_work()
        );

        // Second drain is a no-op (returns the current generation).
        let gen2 = registry.drain_executor(&id).expect("second drain is no-op");
        assert_eq!(gen2, gen1);
    }
}
