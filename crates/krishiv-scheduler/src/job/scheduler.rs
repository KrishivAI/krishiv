use krishiv_plan::{
    ExecutionKind as PlanExecutionKind, LogicalPlan, NodeOp, PhysicalPlan, PlanNode,
};
use krishiv_proto::{
    ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec, StageId, StageSpec, TaskAssignment,
    TaskId, TaskSpec,
};

use crate::{SchedulerError, SchedulerResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Job was admitted and is now scheduled.
    Accepted,
    /// Job was held by the admission controller; not yet running.
    ///
    /// `position` is a 0-based index in the admission queue.
    Queued { position: usize },
}

/// Accumulated resource consumption for one job.
///
/// Populated from `TaskRuntimeStats` as tasks complete. Used by the status API
/// and for post-hoc cost attribution. Not used for real-time quota enforcement
/// (admission uses reservation-based accounting from `JobSpec` fields).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceUsage {
    /// Total CPU nanoseconds consumed by all completed tasks.
    pub cpu_nanos: u64,
    /// Peak memory bytes observed across all completed tasks (max across tasks, not sum).
    pub memory_peak_task_bytes: u64,
    /// Sum of memory bytes across all completed tasks.
    pub memory_total_bytes: u64,
    /// Number of completed tasks that have reported stats.
    pub task_count: u32,
}

impl ResourceUsage {
    /// Absorb stats from one completed task.
    pub fn add_task_stats(&mut self, cpu_nanos: u64, memory_bytes: u64) {
        self.cpu_nanos = self.cpu_nanos.saturating_add(cpu_nanos);
        self.memory_peak_task_bytes = self.memory_peak_task_bytes.max(memory_bytes);
        self.memory_total_bytes = self.memory_total_bytes.saturating_add(memory_bytes);
        self.task_count = self.task_count.saturating_add(1);
    }
}

/// Dynamic namespace quota state supplied to `QueueManager::admit` by the
/// coordinator.
///
/// Contains the current reservation totals for the namespace the submitted job
/// belongs to. `QueueManager` implementations compare these against their
/// configured static limits to decide admission.
#[derive(Debug, Clone, Default)]
pub struct NamespaceQuotaSnapshot {
    /// The namespace being queried (`None` = default namespace).
    pub namespace_id: Option<String>,
    /// CPU nanoseconds reserved by active (non-terminal) jobs in this namespace.
    pub cpu_nanos_reserved: u64,
    /// Memory bytes reserved by active (non-terminal) jobs in this namespace.
    pub memory_bytes_reserved: u64,
    /// Number of active (non-terminal) jobs in this namespace.
    pub active_job_count: usize,
}

/// Static R2 task placement.
#[derive(Debug, Clone, Default)]
pub struct StaticScheduler;

impl StaticScheduler {
    /// Place tasks round-robin across schedulable executors.
    pub fn place(
        spec: &JobSpec,
        executors: &[&ExecutorDescriptor],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }

        let mut assignments = Vec::with_capacity(spec.task_count());
        for (idx, task) in spec.stages().iter().flat_map(StageSpec::tasks).enumerate() {
            let executor = executors
                .get(idx % executors.len())
                .ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task.task_id().clone(),
                executor.executor_id().clone(),
            ));
        }

        Ok(assignments)
    }
}

/// Placement v2: prefer executors with the most free slots.
#[derive(Debug, Clone, Default)]
pub struct SlotAwareScheduler;

/// Scheduler input for one executor with live load included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutorPlacement {
    pub(crate) executor_id: ExecutorId,
    slots: usize,
    active_tasks: usize,
    /// T14: node identifier for `NODE_LOCAL` placement. `None` means the
    /// executor has no associated node (treated as a distinct node for
    /// locality purposes).
    pub(crate) node_id: Option<String>,
    /// T14/Phase 53: rack identifier for the `RACK_LOCAL` placement tier.
    pub(crate) rack_id: Option<String>,
}

impl ExecutorPlacement {
    pub(crate) fn new(executor_id: ExecutorId, slots: usize, active_tasks: usize) -> Self {
        Self {
            executor_id,
            slots,
            active_tasks,
            node_id: None,
            rack_id: None,
        }
    }

    /// T14: build a placement with explicit locality tags.
    pub(crate) fn with_locality(
        executor_id: ExecutorId,
        slots: usize,
        active_tasks: usize,
        node_id: Option<String>,
        rack_id: Option<String>,
    ) -> Self {
        Self {
            executor_id,
            slots,
            active_tasks,
            node_id,
            rack_id,
        }
    }

    pub(crate) fn free_slots(&self) -> usize {
        self.slots.saturating_sub(self.active_tasks)
    }

    /// Phase 53 (audit §3b): overlay the coordinator's own view of in-flight
    /// (Assigned + Running) tasks on top of the heartbeat-reported count.
    /// Heartbeats lag dispatch by up to one interval; without this overlay
    /// two assignment rounds in the same window each see full capacity and
    /// over-assign. `max` (not sum) because the heartbeat count already
    /// includes tasks the coordinator also sees as Running.
    pub(crate) fn raise_active_tasks_to(&mut self, coordinator_view: usize) {
        self.active_tasks = self.active_tasks.max(coordinator_view);
    }
}

impl SlotAwareScheduler {
    pub fn place(
        spec: &JobSpec,
        executors: &[&ExecutorDescriptor],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        let executors: Vec<_> = executors
            .iter()
            .map(|executor| {
                ExecutorPlacement::new(executor.executor_id().clone(), executor.slots(), 0)
            })
            .collect();
        Self::place_with_load(spec, &executors)
    }

    pub(crate) fn place_with_load(
        spec: &JobSpec,
        executors: &[ExecutorPlacement],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        let task_ids: Vec<_> = spec
            .stages()
            .iter()
            .flat_map(StageSpec::tasks)
            .map(|task| task.task_id().clone())
            .collect();
        Self::place_task_ids_with_load(&task_ids, executors)
    }

    /// Place `task_ids` on `executors`, most-free-slots first, under a
    /// **strict capacity budget**.
    ///
    /// Phase 53 (audit §3b): this previously reset the per-executor budget to
    /// full capacity whenever all free slots were consumed and kept assigning
    /// — silently oversubscribing every executor under saturation.  Now the
    /// placement stops when the free-slot budget is exhausted: overflow tasks
    /// are simply not assigned and stay `Pending` for the next dispatch tick
    /// (capacity frees on task completion / executor registration, both of
    /// which trigger reassignment).
    pub(crate) fn place_task_ids_with_load(
        task_ids: &[TaskId],
        executors: &[ExecutorPlacement],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }

        let mut slot_budget: Vec<usize> = executors
            .iter()
            .map(ExecutorPlacement::free_slots)
            .collect();
        let mut assignments = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            let Some((idx, _)) = slot_budget
                .iter()
                .enumerate()
                .filter(|(_, slots)| **slots > 0)
                .max_by_key(|(_, slots)| **slots)
            else {
                // Capacity exhausted: remaining tasks stay Pending.
                break;
            };
            if let Some(b) = slot_budget.get_mut(idx) {
                *b = b.saturating_sub(1);
            }
            let executor = executors.get(idx).ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task_id.clone(),
                executor.executor_id.clone(),
            ));
        }
        Ok(assignments)
    }
}

/// Locality preference for one task (Phase 53, promoted from `cfg(test)`).
///
/// `node_id` / `rack_id` name the preferred placement domains (an executor's
/// node identity is its descriptor `host`).  `pending_since_ms` anchors the
/// delay-scheduling budget: a task with a preference waits up to the
/// configured locality wait for a local slot before falling back to ANY
/// (Zaharia et al., EuroSys '10).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalityPreference {
    /// Preferred node (executor `host`); `None` = no node preference.
    pub node_id: Option<String>,
    /// Preferred rack; `None` = no rack preference.
    pub rack_id: Option<String>,
    /// Wall-clock ms when the task first became schedulable with this
    /// preference. `None` = the delay budget starts now.
    pub pending_since_ms: Option<u64>,
}

impl LocalityPreference {
    fn has_preference(&self) -> bool {
        self.node_id.is_some() || self.rack_id.is_some()
    }
}

/// Per-tier assignment counts from one locality placement pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalityTierCounts {
    /// Tasks placed on their preferred node.
    pub node_local: usize,
    /// Tasks placed on their preferred rack (but not node).
    pub rack_local: usize,
    /// Tasks placed with no locality match (or no preference).
    pub any: usize,
    /// Tasks deferred by delay scheduling (kept Pending, waiting for a
    /// local slot within the locality-wait budget).
    pub deferred: usize,
}

/// Result of one tiered locality placement pass.
#[derive(Debug, Clone, Default)]
pub struct LocalityOutcome {
    pub assignments: Vec<TaskAssignment>,
    pub tier_counts: LocalityTierCounts,
}

/// T14 / SC6 / Phase 53: locality-aware placement, live.
///
/// Tier order per task: NODE_LOCAL → RACK_LOCAL → (delay-scheduling wait)
/// → ANY, under the same strict free-slot budget as
/// [`SlotAwareScheduler::place_task_ids_with_load`] (no oversubscription).
/// PROCESS_LOCAL collapses into NODE_LOCAL here: an in-process executor and
/// its data share a host, and the placement key is the host.
pub struct LocalityScheduler;

impl LocalityScheduler {
    /// Tiered placement with delay scheduling.
    ///
    /// `prefs` is aligned with `task_ids`.  A task whose preferred node and
    /// rack have no free slot is **deferred** (not assigned) while
    /// `now_ms - pending_since_ms < locality_wait_ms`; once the wait budget
    /// is exhausted (or `locality_wait_ms == 0`) it falls back to the
    /// slot-greedy ANY tier.  Tasks without a preference always place ANY.
    /// When the whole free-slot budget is exhausted, remaining tasks are
    /// left unassigned (they stay Pending; not counted as deferred).
    pub(crate) fn place_tiered(
        task_ids: &[TaskId],
        executors: &[ExecutorPlacement],
        prefs: &[LocalityPreference],
        now_ms: u64,
        locality_wait_ms: u64,
    ) -> SchedulerResult<LocalityOutcome> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }
        if task_ids.len() != prefs.len() {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "task_ids ({}) and locality preferences ({}) length mismatch",
                    task_ids.len(),
                    prefs.len()
                ),
            });
        }

        // Per-node / per-rack executor indexes for O(1)-ish local lookup.
        let mut node_index: std::collections::HashMap<&str, Vec<usize>> =
            std::collections::HashMap::new();
        let mut rack_index: std::collections::HashMap<&str, Vec<usize>> =
            std::collections::HashMap::new();
        for (idx, exec) in executors.iter().enumerate() {
            if let Some(node) = exec.node_id.as_deref() {
                node_index.entry(node).or_default().push(idx);
            }
            if let Some(rack) = exec.rack_id.as_deref() {
                rack_index.entry(rack).or_default().push(idx);
            }
        }

        let mut slot_budget: Vec<usize> = executors
            .iter()
            .map(ExecutorPlacement::free_slots)
            .collect();
        let mut outcome = LocalityOutcome::default();
        for (task_id, pref) in task_ids.iter().zip(prefs.iter()) {
            if slot_budget.iter().all(|s| *s == 0) {
                // Strict capacity: remaining tasks stay Pending.
                break;
            }
            let find_in = |idxs: Option<&Vec<usize>>, budget: &[usize]| {
                idxs.and_then(|idxs| {
                    idxs.iter()
                        .copied()
                        .filter(|&i| budget.get(i).is_some_and(|s| *s > 0))
                        .max_by_key(|&i| budget.get(i).copied().unwrap_or(0))
                })
            };

            let node_hit = pref
                .node_id
                .as_deref()
                .and_then(|node| find_in(node_index.get(node), &slot_budget));
            let rack_hit = if node_hit.is_none() {
                pref.rack_id
                    .as_deref()
                    .and_then(|rack| find_in(rack_index.get(rack), &slot_budget))
            } else {
                None
            };

            let idx = if let Some(i) = node_hit {
                outcome.tier_counts.node_local += 1;
                i
            } else if let Some(i) = rack_hit {
                outcome.tier_counts.rack_local += 1;
                i
            } else if pref.has_preference() && locality_wait_ms > 0 {
                let waited = now_ms.saturating_sub(pref.pending_since_ms.unwrap_or(now_ms));
                if waited < locality_wait_ms {
                    // Delay scheduling: hold out for a local slot.
                    outcome.tier_counts.deferred += 1;
                    continue;
                }
                outcome.tier_counts.any += 1;
                match slot_budget
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| **s > 0)
                    .max_by_key(|(_, s)| **s)
                {
                    Some((i, _)) => i,
                    None => break,
                }
            } else {
                outcome.tier_counts.any += 1;
                match slot_budget
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| **s > 0)
                    .max_by_key(|(_, s)| **s)
                {
                    Some((i, _)) => i,
                    None => break,
                }
            };
            if let Some(b) = slot_budget.get_mut(idx) {
                *b = b.saturating_sub(1);
            }
            let executor = executors.get(idx).ok_or(SchedulerError::NoExecutors)?;
            outcome.assignments.push(TaskAssignment::new(
                task_id.clone(),
                executor.executor_id.clone(),
            ));
        }
        Ok(outcome)
    }
}

/// A scheduling pool: weight + minimum slot share (SC9 / Phase 53).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSpec {
    /// Proportional share weight; slots beyond min-shares split by weight.
    pub weight: u64,
    /// Slots guaranteed to this pool before weighted distribution.
    pub min_share: u64,
}

impl Default for PoolSpec {
    fn default() -> Self {
        Self {
            weight: 1,
            min_share: 0,
        }
    }
}

/// SC9 / Phase 53: FAIR scheduler, live.
///
/// Splits an available slot budget across pools by `min_share` first, then
/// distributes the remainder proportionally to `weight` (largest-remainder
/// rounding), capped by each pool's demand.  Under saturation the resulting
/// per-pool quotas converge to the weight ratio; excess demand stays
/// unassigned (Pending) instead of oversubscribing executors.
pub struct FairScheduler;

impl FairScheduler {
    /// Compute per-pool slot quotas for one assignment round.
    ///
    /// `demand` is the number of runnable tasks per pool; `pools` supplies
    /// weight/min-share (missing pools default to `PoolSpec::default()`).
    /// The returned quotas sum to at most `total_slots` and never exceed a
    /// pool's demand.
    pub fn compute_pool_quotas(
        total_slots: usize,
        demand: &std::collections::BTreeMap<String, usize>,
        pools: &std::collections::HashMap<String, PoolSpec>,
    ) -> std::collections::BTreeMap<String, usize> {
        let mut quotas: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        if total_slots == 0 || demand.is_empty() {
            return quotas;
        }
        let spec_for = |pool: &str| pools.get(pool).copied().unwrap_or_default();

        // Pass 1: min-shares, capped by demand and remaining budget.
        let mut remaining = total_slots;
        for (pool, &want) in demand {
            let min = usize::try_from(spec_for(pool).min_share).unwrap_or(usize::MAX);
            let grant = min.min(want).min(remaining);
            if grant > 0 {
                quotas.insert(pool.clone(), grant);
                remaining -= grant;
            }
        }

        // Pass 2: weighted distribution of the remainder among pools with
        // unmet demand, iterating until budget or demand is exhausted
        // (iteration handles caps: a small pool's unused share flows to
        // the others by re-normalizing each round).
        loop {
            if remaining == 0 {
                break;
            }
            let unmet: Vec<(&String, usize, u64)> = demand
                .iter()
                .filter_map(|(pool, &want)| {
                    let have = quotas.get(pool).copied().unwrap_or(0);
                    (want > have).then(|| (pool, want - have, spec_for(pool).weight.max(1)))
                })
                .collect();
            if unmet.is_empty() {
                break;
            }
            let total_weight: u64 = unmet.iter().map(|(_, _, w)| w).sum::<u64>().max(1);
            let budget = remaining;
            let mut granted_this_round = 0usize;
            // Weighted proportional grant with floor rounding; the leftover
            // goes one-by-one to the highest-weight pools (largest
            // remainder is approximated by weight order, deterministic).
            struct Grant {
                pool: String,
                unmet_demand: usize,
                weight: u64,
                grant: usize,
            }
            let mut grants: Vec<Grant> = unmet
                .iter()
                .map(|(pool, unmet_demand, w)| {
                    let share = usize::try_from((budget as u64).saturating_mul(*w) / total_weight)
                        .unwrap_or(0);
                    Grant {
                        pool: (*pool).clone(),
                        unmet_demand: *unmet_demand,
                        weight: *w,
                        grant: share.min(*unmet_demand),
                    }
                })
                .collect();
            // Distribute floor-rounding leftovers deterministically by
            // descending weight, then pool name.
            grants.sort_by(|a, b| b.weight.cmp(&a.weight).then_with(|| a.pool.cmp(&b.pool)));
            let mut leftover = budget.saturating_sub(grants.iter().map(|g| g.grant).sum());
            for g in grants.iter_mut() {
                if leftover == 0 {
                    break;
                }
                if g.grant < g.unmet_demand {
                    g.grant += 1;
                    leftover -= 1;
                }
            }
            for g in grants {
                if g.grant > 0 {
                    *quotas.entry(g.pool).or_insert(0) += g.grant;
                    remaining = remaining.saturating_sub(g.grant);
                    granted_this_round += g.grant;
                }
            }
            if granted_this_round == 0 {
                break;
            }
        }
        quotas
    }

    /// Distribute `task_ids` across `executors` fairly by namespace pool.
    ///
    /// `namespace_assignments` is aligned with `task_ids` (`None` = default
    /// pool `""`).  Each pool receives at most its computed quota this
    /// round; tasks beyond the quota are left unassigned (Pending).
    ///
    /// Reference implementation exercised by the 2:1 exit-gate unit test;
    /// the live pool round consumes [`FairScheduler::compute_pool_quotas`]
    /// directly (`assign_pending_tasks_for_schedulable_jobs`).
    #[cfg(test)]
    pub(crate) fn place(
        task_ids: &[TaskId],
        executors: &[ExecutorPlacement],
        namespace_assignments: &[Option<String>],
        pools: &std::collections::HashMap<String, PoolSpec>,
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }
        if task_ids.len() != namespace_assignments.len() {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "task_ids ({}) and namespace_assignments ({}) length mismatch",
                    task_ids.len(),
                    namespace_assignments.len()
                ),
            });
        }

        let mut demand: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for ns in namespace_assignments {
            *demand.entry(ns.clone().unwrap_or_default()).or_insert(0) += 1;
        }
        let total_free: usize = executors.iter().map(ExecutorPlacement::free_slots).sum();
        let mut quotas = Self::compute_pool_quotas(total_free, &demand, pools);

        let mut slot_budget: Vec<usize> = executors
            .iter()
            .map(ExecutorPlacement::free_slots)
            .collect();
        let mut assignments = Vec::with_capacity(task_ids.len());
        for (task_id, ns) in task_ids.iter().zip(namespace_assignments.iter()) {
            let pool = ns.clone().unwrap_or_default();
            let Some(quota) = quotas.get_mut(&pool).filter(|q| **q > 0) else {
                continue; // Pool exhausted its fair share this round.
            };
            let Some((idx, _)) = slot_budget
                .iter()
                .enumerate()
                .filter(|(_, s)| **s > 0)
                .max_by_key(|(_, s)| **s)
            else {
                break;
            };
            *quota -= 1;
            if let Some(b) = slot_budget.get_mut(idx) {
                *b = b.saturating_sub(1);
            }
            let executor = executors.get(idx).ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task_id.clone(),
                executor.executor_id.clone(),
            ));
        }
        Ok(assignments)
    }
}

/// Convert a Krishiv logical plan into an R2 distributed job spec.
pub fn job_spec_from_logical_plan(job_id: JobId, plan: &LogicalPlan) -> SchedulerResult<JobSpec> {
    plan.validate()
        .map_err(|error| SchedulerError::InvalidPlan {
            message: error.to_string(),
        })?;
    job_spec_from_plan_parts(job_id, plan.name(), plan.kind(), plan.nodes(), None)
}

/// Convert a Krishiv physical plan into an R2 distributed job spec.
pub fn job_spec_from_physical_plan(job_id: JobId, plan: &PhysicalPlan) -> SchedulerResult<JobSpec> {
    plan.validate()
        .map_err(|error| SchedulerError::InvalidPlan {
            message: error.to_string(),
        })?;
    job_spec_from_plan_parts(
        job_id,
        plan.name(),
        plan.kind(),
        plan.nodes(),
        plan.coalesced_partition_count(),
    )
}

pub(crate) fn validate_job(spec: &JobSpec) -> SchedulerResult<()> {
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
    let stage_ids: std::collections::HashSet<&StageId> =
        spec.stages().iter().map(|s| s.stage_id()).collect();
    for stage in spec.stages() {
        for upstream_id in stage.upstream_stage_ids() {
            if !stage_ids.contains(upstream_id) {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "stage {} declares upstream dependency on unknown stage {}",
                        stage.stage_id(),
                        upstream_id
                    ),
                });
            }
        }
    }
    // Cycle detection in stage dependency graph using Kahn's algorithm.
    {
        let n = spec.stages().len();
        let stage_id_to_idx: std::collections::HashMap<&StageId, usize> = spec
            .stages()
            .iter()
            .enumerate()
            .map(|(i, s)| (s.stage_id(), i))
            .collect();
        let mut in_degree = vec![0usize; n];
        for stage in spec.stages() {
            let idx = *stage_id_to_idx.get(stage.stage_id()).ok_or_else(|| {
                SchedulerError::InvalidJob {
                    message: format!(
                        "internal error: stage '{}' missing from index during cycle detection",
                        stage.stage_id()
                    ),
                }
            })?;
            if let Some(d) = in_degree.get_mut(idx) {
                *d = d.saturating_add(stage.upstream_stage_ids().len());
            }
        }
        let mut queue: std::collections::VecDeque<usize> = in_degree
            .iter()
            .enumerate()
            .filter_map(|(i, &d)| (d == 0).then_some(i))
            .collect();
        let mut processed = 0usize;
        while let Some(idx) = queue.pop_front() {
            processed += 1;
            let Some(current_stage) = spec.stages().get(idx) else {
                continue;
            };
            let current_id = current_stage.stage_id();
            for (ds_idx, ds_stage) in spec.stages().iter().enumerate() {
                if ds_stage.upstream_stage_ids().contains(current_id)
                    && let Some(d) = in_degree.get_mut(ds_idx)
                {
                    *d = d.saturating_sub(1);
                    if *d == 0 {
                        queue.push_back(ds_idx);
                    }
                }
            }
        }
        if processed != n {
            return Err(SchedulerError::InvalidJob {
                message: String::from("stage dependency graph is cyclic"),
            });
        }
    }
    // O(n) duplicate task-id detection using a HashSet.
    let mut seen_task_ids: std::collections::HashSet<&TaskId> =
        std::collections::HashSet::with_capacity(spec.task_count());
    for stage in spec.stages() {
        for task in stage.tasks() {
            if !seen_task_ids.insert(task.task_id()) {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "duplicate task id {} in job {}",
                        task.task_id(),
                        spec.job_id()
                    ),
                });
            }
        }
    }
    // Field-by-field validation: catch obviously bad inputs before they
    // propagate into the executor or checkpoint store. The error messages
    // include the offending field name so the caller can fix the spec
    // rather than seeing an opaque runtime failure.
    if spec.job_id().as_str().is_empty() {
        return Err(SchedulerError::InvalidJob {
            message: String::from("job_id must not be empty"),
        });
    }
    if let Some(namespace) = spec.namespace_id() {
        if namespace.is_empty() {
            return Err(SchedulerError::InvalidJob {
                message: String::from("namespace_id must not be empty when present"),
            });
        }
        if namespace.len() > 253 {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "namespace_id '{}' exceeds 253 chars (DNS-1123 label limit)",
                    namespace
                ),
            });
        }
    }
    if let Some(interval) = spec.checkpoint_interval_ms()
        && interval == 0
    {
        return Err(SchedulerError::InvalidJob {
            message: String::from(
                "checkpoint_interval_ms must be > 0; use None to disable checkpointing",
            ),
        });
    }
    if let Some(path) = spec.checkpoint_storage_path()
        && path.is_empty()
    {
        return Err(SchedulerError::InvalidJob {
            message: String::from(
                "checkpoint_storage_path must not be empty when checkpoint_interval_ms is set",
            ),
        });
    }
    let profile = krishiv_common::resolve_durability_profile();
    krishiv_plan::validate_job_fragments(spec, profile).map_err(|error| {
        SchedulerError::InvalidJob {
            message: error.to_string(),
        }
    })?;
    Ok(())
}

fn job_spec_from_plan_parts(
    job_id: JobId,
    plan_name: &str,
    kind: PlanExecutionKind,
    nodes: &[PlanNode],
    coalesced_partition_count: Option<usize>,
) -> SchedulerResult<JobSpec> {
    let job_kind = match kind {
        PlanExecutionKind::Batch => JobKind::Batch,
        PlanExecutionKind::Streaming => JobKind::Streaming,
        PlanExecutionKind::DeltaBatch => JobKind::Batch,
    };
    let job_name = if plan_name.trim().is_empty() {
        String::from("unnamed-distributed-dag")
    } else {
        plan_name.to_owned()
    };

    if coalesced_partition_count.is_none() && plan_has_exchange_stages(nodes) {
        return job_spec_from_exchange_stages(job_id, &job_name, job_kind, nodes);
    }

    let stage_id = StageId::try_new("stage-1").map_err(|error| SchedulerError::InvalidPlan {
        message: error.to_string(),
    })?;

    let mut stage = StageSpec::new(stage_id, format!("{job_name}-stage"));

    if let Some(count) = coalesced_partition_count {
        let task_count = count.max(1);
        for i in 0..task_count {
            let task_id = TaskId::try_new(format!("task-{}", i + 1)).map_err(|error| {
                SchedulerError::InvalidPlan {
                    message: error.to_string(),
                }
            })?;
            stage = stage.with_task(TaskSpec::new(
                task_id,
                format!("coalesced-partition-{i}: {job_name}"),
            ));
        }
    } else if nodes.is_empty() {
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
            stage = stage.with_task(TaskSpec::new(task_id, plan_node_description(node)?));
        }
    }

    Ok(JobSpec::new(job_id, job_name, job_kind).with_stage(stage))
}

fn plan_node_description(node: &PlanNode) -> SchedulerResult<String> {
    krishiv_plan::encode_typed_task_fragment(node).map_err(|error| SchedulerError::InvalidPlan {
        message: format!("failed to encode plan node '{}': {error}", node.id()),
    })
}

fn plan_has_exchange_stages(nodes: &[PlanNode]) -> bool {
    nodes
        .iter()
        .any(|node| matches!(node.op(), Some(NodeOp::Exchange { .. })))
}

/// Split a physical plan into multiple stages at [`NodeOp::Exchange`] boundaries.
fn job_spec_from_exchange_stages(
    job_id: JobId,
    job_name: &str,
    job_kind: JobKind,
    nodes: &[PlanNode],
) -> SchedulerResult<JobSpec> {
    let ordered = topo_sort_plan_nodes(nodes)?;
    let mut stage_slices: Vec<Vec<&PlanNode>> = Vec::new();
    let mut current: Vec<&PlanNode> = Vec::new();
    for node in &ordered {
        current.push(node);
        if matches!(node.op(), Some(NodeOp::Exchange { .. })) {
            stage_slices.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        stage_slices.push(current);
    }
    if stage_slices.is_empty() {
        return Err(SchedulerError::InvalidPlan {
            message: String::from("exchange staging produced no stages"),
        });
    }

    let last_idx = stage_slices.len().saturating_sub(1);
    let mut spec = JobSpec::new(job_id, job_name, job_kind);
    let mut prev_stage_id: Option<StageId> = None;
    for (stage_idx, slice) in stage_slices.iter().enumerate() {
        let stage_id = StageId::try_new(format!("stage-{}", stage_idx + 1)).map_err(|error| {
            SchedulerError::InvalidPlan {
                message: error.to_string(),
            }
        })?;
        // The terminal slice has no Exchange node: it reads from shuffle and
        // produces final output → Result.  All preceding slices end with an
        // Exchange node and write to the shuffle store → ShuffleMap.
        let stage_kind = if stage_idx == last_idx {
            krishiv_proto::StageKind::Result
        } else {
            krishiv_proto::StageKind::ShuffleMap
        };
        let mut stage = StageSpec::new(stage_id.clone(), format!("{job_name}-stage-{stage_idx}"))
            .with_kind(stage_kind);
        if let Some(upstream) = prev_stage_id.clone() {
            stage = stage.with_upstream_stage(upstream);
        }
        for (task_idx, node) in slice.iter().enumerate() {
            let task_id = TaskId::try_new(format!("task-{}-{}", stage_idx + 1, task_idx + 1))
                .map_err(|error| SchedulerError::InvalidPlan {
                    message: error.to_string(),
                })?;
            stage = stage.with_task(TaskSpec::new(task_id, plan_node_description(node)?));
        }
        spec = spec.with_stage(stage);
        prev_stage_id = Some(stage_id);
    }
    Ok(spec)
}

/// Topological order of plan nodes using declared `inputs()` edges.
pub(crate) fn topo_sort_plan_nodes(nodes: &[PlanNode]) -> SchedulerResult<Vec<&PlanNode>> {
    use std::collections::{HashMap, VecDeque};

    let indexes = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id(), index))
        .collect::<HashMap<_, _>>();
    let mut in_degrees = vec![0usize; nodes.len()];
    let mut dependents = vec![Vec::new(); nodes.len()];
    for (node_index, node) in nodes.iter().enumerate() {
        for input in node.inputs() {
            let input_index = indexes.get(input.as_str()).copied().ok_or_else(|| {
                SchedulerError::InvalidPlan {
                    message: format!("node '{}' references missing input '{input}'", node.id()),
                }
            })?;
            let deg =
                in_degrees
                    .get_mut(node_index)
                    .ok_or_else(|| SchedulerError::InvalidPlan {
                        message: format!("in_degrees index {node_index} out of range"),
                    })?;
            *deg = deg
                .checked_add(1)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!("node '{}' has too many input edges", node.id()),
                })?;
            dependents
                .get_mut(input_index)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!("dependents index {input_index} out of range"),
                })?
                .push(node_index);
        }
    }
    let mut queue = in_degrees
        .iter()
        .enumerate()
        .filter_map(|(index, in_degree)| (*in_degree == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(node_index) = queue.pop_front() {
        let node = nodes
            .get(node_index)
            .ok_or_else(|| SchedulerError::InvalidPlan {
                message: format!("node index {node_index} out of range"),
            })?;
        ordered.push(node);
        let deps = dependents
            .get(node_index)
            .ok_or_else(|| SchedulerError::InvalidPlan {
                message: format!("dependents index {node_index} out of range"),
            })?;
        for &dependent_index in deps {
            let in_degree = in_degrees.get_mut(dependent_index).ok_or_else(|| {
                SchedulerError::InvalidPlan {
                    message: format!(
                        "topological sort lost in-degree state for node at index {dependent_index}",
                    ),
                }
            })?;
            *in_degree = in_degree
                .checked_sub(1)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!(
                        "topological sort underflowed in-degree for node at index {dependent_index}",
                    ),
                })?;
            if *in_degree == 0 {
                queue.push_back(dependent_index);
            }
        }
    }
    if ordered.len() != nodes.len() {
        return Err(SchedulerError::InvalidPlan {
            message: String::from("plan graph is cyclic and cannot be staged"),
        });
    }
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::ExecutorId;

    fn make_task_id(s: &str) -> TaskId {
        TaskId::try_new(s).expect("valid id")
    }

    fn make_placement(s: &str, slots: usize, active: usize) -> ExecutorPlacement {
        ExecutorPlacement::new(ExecutorId::try_new(s).expect("id"), slots, active)
    }

    fn make_placement_node(s: &str, slots: usize, active: usize, node: &str) -> ExecutorPlacement {
        ExecutorPlacement::with_locality(
            ExecutorId::try_new(s).expect("id"),
            slots,
            active,
            Some(String::from(node)),
            None,
        )
    }

    fn node_prefs(preferred: &[Option<String>]) -> Vec<LocalityPreference> {
        preferred
            .iter()
            .map(|node| LocalityPreference {
                node_id: node.clone(),
                rack_id: None,
                pending_since_ms: None,
            })
            .collect()
    }

    /// T14: when a preferred node has free slots, the locality scheduler
    /// pins the task to that node (even if another node has more free
    /// slots).
    #[test]
    fn locality_prefers_same_node_when_available() {
        let executors = vec![
            make_placement_node("executor-a", 4, 0, "node-1"),
            make_placement_node("executor-b", 4, 0, "node-2"),
            make_placement_node("executor-c", 4, 0, "node-1"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let preferred = vec![Some(String::from("node-1"))];
        let prefs = node_prefs(&preferred);
        let outcome = LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 0, 0)
            .expect("locality placement");
        // Pinned to one of the two executors on node-1; never to node-2.
        let chosen = outcome.assignments[0].executor_id().as_str();
        assert!(
            chosen == "executor-a" || chosen == "executor-c",
            "task must be placed on a node-1 executor; got {chosen}"
        );
    }

    /// T14: when the preferred node is fully consumed, the locality
    /// scheduler falls back to the slot-greedy executor.
    #[test]
    fn locality_falls_back_when_preferred_node_is_full() {
        let executors = vec![
            make_placement_node("executor-a", 2, 2, "node-1"),
            make_placement_node("executor-b", 2, 0, "node-2"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let preferred = vec![Some(String::from("node-1"))];
        let prefs = node_prefs(&preferred);
        let outcome = LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 0, 0)
            .expect("locality placement");
        assert_eq!(
            outcome.assignments[0].executor_id().as_str(),
            "executor-b",
            "preferred node is full, so the task must fall back to the slot-greedy executor"
        );
    }

    /// T14: a `None` preferred location means "no preference" — the
    /// placement uses the standard slot-greedy algorithm.
    #[test]
    fn locality_no_preference_falls_back_to_greedy() {
        let executors = vec![
            make_placement_node("executor-a", 4, 0, "node-1"),
            make_placement_node("executor-b", 8, 0, "node-2"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let preferred = vec![None];
        let prefs = node_prefs(&preferred);
        let outcome = LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 0, 0)
            .expect("locality placement");
        // No preference → greediest node wins (executor-b with 8 slots).
        assert_eq!(outcome.assignments[0].executor_id().as_str(), "executor-b");
    }

    /// T14: a `length` mismatch between tasks and preferences is rejected.
    #[test]
    fn locality_rejects_length_mismatch() {
        let executors = vec![make_placement("executor-a", 4, 0)];
        let tasks = vec![make_task_id("task-1")];
        let prefs: Vec<LocalityPreference> = vec![];
        let result = LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 0, 0);
        assert!(result.is_err(), "length mismatch must return an error");
    }

    fn make_placement_node_rack(
        s: &str,
        slots: usize,
        active: usize,
        node: &str,
        rack: &str,
    ) -> ExecutorPlacement {
        ExecutorPlacement::with_locality(
            ExecutorId::try_new(s).expect("id"),
            slots,
            active,
            Some(String::from(node)),
            Some(String::from(rack)),
        )
    }

    /// Phase 53: when the preferred node is full but an executor on the
    /// preferred rack has slots, the task places RACK_LOCAL.
    #[test]
    fn locality_rack_tier_used_when_node_full() {
        let executors = vec![
            make_placement_node_rack("executor-a", 2, 2, "node-1", "rack-1"),
            make_placement_node_rack("executor-b", 2, 0, "node-2", "rack-1"),
            make_placement_node_rack("executor-c", 8, 0, "node-3", "rack-2"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let prefs = vec![LocalityPreference {
            node_id: Some(String::from("node-1")),
            rack_id: Some(String::from("rack-1")),
            pending_since_ms: None,
        }];
        let outcome =
            LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 0, 0).expect("placement");
        assert_eq!(
            outcome.assignments[0].executor_id().as_str(),
            "executor-b",
            "rack-1 executor must win over the greedier rack-2 executor"
        );
        assert_eq!(outcome.tier_counts.rack_local, 1);
        assert_eq!(outcome.tier_counts.node_local, 0);
    }

    /// Phase 53 delay scheduling: within the locality-wait budget a task with
    /// an unsatisfiable preference is deferred; once the budget is exhausted
    /// it falls back to ANY.
    #[test]
    fn locality_delay_defers_then_falls_back() {
        let executors = vec![
            make_placement_node("executor-a", 2, 2, "node-1"), // preferred, full
            make_placement_node("executor-b", 4, 0, "node-2"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let prefs = vec![LocalityPreference {
            node_id: Some(String::from("node-1")),
            rack_id: None,
            pending_since_ms: Some(1_000),
        }];
        // Within the wait budget (waited 500ms < 3000ms) → deferred.
        let outcome = LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 1_500, 3_000)
            .expect("placement");
        assert!(outcome.assignments.is_empty(), "task must be deferred");
        assert_eq!(outcome.tier_counts.deferred, 1);
        // Budget exhausted (waited 5s ≥ 3s) → ANY fallback.
        let outcome = LocalityScheduler::place_tiered(&tasks, &executors, &prefs, 6_000, 3_000)
            .expect("placement");
        assert_eq!(outcome.assignments.len(), 1);
        assert_eq!(outcome.assignments[0].executor_id().as_str(), "executor-b");
        assert_eq!(outcome.tier_counts.any, 1);
    }
}

#[cfg(test)]
mod fair_scheduler_tests {
    use super::*;
    use krishiv_proto::ExecutorId;
    use std::collections::HashMap;

    fn placement(s: &str, slots: usize) -> ExecutorPlacement {
        ExecutorPlacement::new(ExecutorId::try_new(s).expect("id"), slots, 0)
    }

    fn task(s: &str) -> TaskId {
        TaskId::try_new(s).expect("id")
    }

    /// SC9: with one executor and two namespaces, the FAIR scheduler
    /// assigns all tasks when capacity covers demand.
    #[test]
    fn fair_scheduler_round_robins_across_namespaces() {
        let executors = vec![placement("executor-1", 6)];
        let tasks = vec![
            task("t1"),
            task("t2"),
            task("t3"),
            task("t4"),
            task("t5"),
            task("t6"),
        ];
        let namespaces = vec![
            Some("alpha".to_string()),
            Some("beta".to_string()),
            Some("alpha".to_string()),
            Some("beta".to_string()),
            Some("alpha".to_string()),
            Some("beta".to_string()),
        ];
        let pools = HashMap::new();
        let assignments =
            FairScheduler::place(&tasks, &executors, &namespaces, &pools).expect("fair placement");
        assert_eq!(assignments.len(), 6, "capacity covers demand");
        for a in &assignments {
            assert_eq!(a.executor_id().as_str(), "executor-1");
        }
    }

    /// Phase 53 exit-gate math: two pools with 2:1 weights under saturation
    /// converge to a 2:1 slot split.
    #[test]
    fn fair_scheduler_two_to_one_weights_split_two_to_one_under_saturation() {
        let executors = vec![placement("executor-1", 6)];
        // 12 tasks demanded, 6 slots available: saturation.
        let tasks: Vec<TaskId> = (0..12).map(|i| task(&format!("t{i}"))).collect();
        let namespaces: Vec<Option<String>> = (0..12)
            .map(|i| {
                Some(if i % 2 == 0 {
                    "heavy".to_string()
                } else {
                    "light".to_string()
                })
            })
            .collect();
        let mut pools = HashMap::new();
        pools.insert(
            "heavy".to_string(),
            PoolSpec {
                weight: 2,
                min_share: 0,
            },
        );
        pools.insert(
            "light".to_string(),
            PoolSpec {
                weight: 1,
                min_share: 0,
            },
        );
        let assignments =
            FairScheduler::place(&tasks, &executors, &namespaces, &pools).expect("fair placement");
        assert_eq!(assignments.len(), 6, "no oversubscription");
        let heavy = assignments
            .iter()
            .filter(|a| {
                let idx: usize = a.task_id().as_str()[1..].parse().unwrap();
                idx % 2 == 0
            })
            .count();
        let light = assignments.len() - heavy;
        assert_eq!((heavy, light), (4, 2), "2:1 weights → 4:2 of 6 slots");
    }

    /// Phase 53: min-share is honored before weighted distribution.
    #[test]
    fn fair_scheduler_min_share_honored_before_weights() {
        let mut pools = HashMap::new();
        pools.insert(
            "big".to_string(),
            PoolSpec {
                weight: 10,
                min_share: 0,
            },
        );
        pools.insert(
            "guaranteed".to_string(),
            PoolSpec {
                weight: 1,
                min_share: 3,
            },
        );
        let mut demand = std::collections::BTreeMap::new();
        demand.insert("big".to_string(), 10);
        demand.insert("guaranteed".to_string(), 10);
        let quotas = FairScheduler::compute_pool_quotas(6, &demand, &pools);
        assert!(
            quotas.get("guaranteed").copied().unwrap_or(0) >= 3,
            "min_share=3 must be granted: {quotas:?}"
        );
        assert_eq!(quotas.values().sum::<usize>(), 6, "budget fully used");
    }

    /// Phase 53: a pool with demand below its weighted share donates the
    /// surplus to the other pools.
    #[test]
    fn fair_scheduler_surplus_flows_to_unmet_pools() {
        let pools = HashMap::new(); // equal weights
        let mut demand = std::collections::BTreeMap::new();
        demand.insert("small".to_string(), 1);
        demand.insert("large".to_string(), 20);
        let quotas = FairScheduler::compute_pool_quotas(10, &demand, &pools);
        assert_eq!(quotas.get("small"), Some(&1));
        assert_eq!(quotas.get("large"), Some(&9), "surplus flows: {quotas:?}");
    }

    /// SC1: exchange stages have ShuffleMap kind; the terminal stage has Result kind.
    #[test]
    fn exchange_stages_have_correct_stage_kind() {
        use krishiv_plan::{ExecutionKind, NodeOp, Partitioning, PhysicalPlan, PlanNode};
        use krishiv_proto::{JobId, StageKind};

        let job_id = JobId::try_new("sc1-test").unwrap();
        // Build a two-stage plan: map → exchange → reduce
        let mut plan = PhysicalPlan::new("sc1", ExecutionKind::Batch);
        plan.add_node(PlanNode::new("map", "map", ExecutionKind::Batch).with_op(
            NodeOp::Exchange {
                partitioning: Partitioning::Hash {
                    keys: vec!["k".to_string()],
                    buckets: 4,
                },
            },
        ));
        plan.add_node(
            PlanNode::new("reduce", "reduce", ExecutionKind::Batch)
                .with_inputs(vec!["map".to_string()]),
        );
        let spec = job_spec_from_physical_plan(job_id, &plan).expect("valid plan");
        assert_eq!(spec.stages().len(), 2, "should have 2 stages");
        assert_eq!(
            spec.stages()[0].kind(),
            StageKind::ShuffleMap,
            "stage-0 (map→exchange) must be ShuffleMap"
        );
        assert_eq!(
            spec.stages()[1].kind(),
            StageKind::Result,
            "stage-1 (terminal reduce) must be Result"
        );
    }

    /// SC9: a `length` mismatch is rejected.
    #[test]
    fn fair_scheduler_rejects_length_mismatch() {
        let executors = vec![placement("executor-1", 4)];
        let tasks = vec![task("t1")];
        let namespaces: Vec<Option<String>> = vec![];
        let pools = HashMap::new();
        let result = FairScheduler::place(&tasks, &executors, &namespaces, &pools);
        assert!(result.is_err(), "length mismatch must return an error");
    }

    /// Phase 53 (audit §3b): saturation must not oversubscribe — overflow
    /// tasks stay unassigned instead of resetting the budget to full
    /// capacity and double-booking every executor.
    #[test]
    fn slot_aware_placement_stops_at_capacity() {
        let executors = vec![placement("executor-1", 2), placement("executor-2", 1)];
        let tasks: Vec<TaskId> = (0..10).map(|i| task(&format!("t{i}"))).collect();
        let assignments =
            SlotAwareScheduler::place_task_ids_with_load(&tasks, &executors).expect("placement");
        assert_eq!(
            assignments.len(),
            3,
            "3 free slots → exactly 3 assignments, 7 stay Pending"
        );
    }
}
