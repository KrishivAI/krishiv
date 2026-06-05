use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};

use crate::{ExecutorError, ExecutorResult};
use krishiv_proto::ExecutorTaskAssignment;

/// Result of pushing an assignment into the executor inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentPushOutcome {
    /// The assignment was queued for execution.
    Enqueued,
    /// The same `(job, task, attempt)` was already received.
    Duplicate,
}

/// In-memory receiver queue for task assignments delivered to an executor.
///
/// Supports bounded capacity for backpressure (PRR Phase 1/2).
/// When capacity is reached, `push` returns `ExecutorError::AssignmentQueueFull`.
/// Deduplicates assignments by (JobId, TaskId, AttemptId) — duplicate pushes
/// return Ok(()) without enqueuing to prevent redundant execution.
const DEFAULT_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct ExecutorAssignmentInbox {
    assignments: Arc<RwLock<VecDeque<ExecutorTaskAssignment>>>,
    cancelled_tasks: Arc<RwLock<BTreeSet<krishiv_proto::TaskId>>>,
    /// Tracks (job_id, task_id, attempt_id) tuples already received to prevent
    /// duplicate execution from at-least-once delivery retries.
    seen: Arc<
        RwLock<
            BTreeSet<(
                krishiv_proto::JobId,
                krishiv_proto::TaskId,
                krishiv_proto::AttemptId,
            )>,
        >,
    >,
    /// None = unbounded (legacy / test default). Some(n) = hard limit.
    max_capacity: Option<usize>,
}

impl Default for ExecutorAssignmentInbox {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorAssignmentInbox {
    /// Create a bounded assignment inbox with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create an unbounded inbox (no backpressure — tests and legacy paths).
    pub fn new_unbounded() -> Self {
        Self {
            assignments: Arc::new(RwLock::new(VecDeque::new())),
            cancelled_tasks: Arc::new(RwLock::new(BTreeSet::new())),
            seen: Arc::new(RwLock::new(BTreeSet::new())),
            max_capacity: None,
        }
    }

    /// Create a bounded inbox with the given maximum number of queued assignments.
    /// Pushes beyond this limit will fail with `ExecutorError::AssignmentQueueFull`.
    pub fn with_capacity(max: usize) -> Self {
        Self {
            assignments: Arc::new(RwLock::new(VecDeque::new())),
            cancelled_tasks: Arc::new(RwLock::new(BTreeSet::new())),
            seen: Arc::new(RwLock::new(BTreeSet::new())),
            max_capacity: Some(max),
        }
    }

    /// Current configured capacity (None = unbounded).
    pub fn capacity(&self) -> Option<usize> {
        self.max_capacity
    }

    /// Store one received assignment.
    ///
    /// Deduplicates by (JobId, TaskId, AttemptId) — returns `Ok(())` silently
    /// if this assignment was already received. Returns `Err(AssignmentQueueFull)`
    /// when at capacity — the backpressure signal to the coordinator.
    pub fn push(&self, assignment: ExecutorTaskAssignment) -> ExecutorResult<()> {
        self.push_with_outcome(assignment).map(|_| ())
    }

    /// Store one received assignment and report whether it was newly queued or
    /// already present.
    ///
    /// Lock order: the `seen` set and `assignments` queue are both protected by
    /// their own `RwLock`. We acquire `seen` *first* and hold it across the
    /// capacity check + insert to prevent the TOCTOU race that the prior
    /// implementation had: a duplicate `(job, task, attempt)` could observe
    /// `seen.insert == true` and then be rejected on capacity, leaving the
    /// `seen` set with a stale entry that blocks later legitimate re-pushes.
    pub fn push_with_outcome(
        &self,
        assignment: ExecutorTaskAssignment,
    ) -> ExecutorResult<AssignmentPushOutcome> {
        let key = (
            assignment.job_id().clone(),
            assignment.task_id().clone(),
            assignment.attempt_id(),
        );
        let mut seen = self
            .seen
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        if !seen.insert(key.clone()) {
            return Ok(AssignmentPushOutcome::Duplicate);
        }
        // Hold `seen` while we check capacity and insert. If capacity rejects
        // the push, we MUST undo the `seen.insert` so the coordinator can retry
        // — see the cleanup below.
        let mut q = self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;

        if let Some(max) = self.max_capacity
            && q.len() >= max
        {
            // Roll back the optimistic `seen.insert` so the coordinator can
            // retry once capacity is available. The mutex ordering (seen →
            // assignments → seen) means a concurrent caller cannot observe an
            // inconsistent state where `seen` claims the key but the queue
            // does not.
            seen.remove(&key);
            return Err(ExecutorError::AssignmentQueueFull {
                current: q.len(),
                max,
            });
        }

        q.push_back(assignment);
        Ok(AssignmentPushOutcome::Enqueued)
    }

    /// Remove the next received assignment in FIFO order.
    pub fn pop_next(&self) -> ExecutorResult<Option<ExecutorTaskAssignment>> {
        Ok(self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .pop_front())
    }

    /// Cancel and remove queued assignments for a task id.
    ///
    /// Also marks the task id as cancelled so the runner can skip execution even
    /// if the task has already been popped from the queue.
    ///
    /// Lock order: `seen` → `assignments` → `cancelled_tasks`. This matches the
    /// order used by [`push_with_outcome`] (`seen` → `assignments`), so a
    /// concurrent `push` and `cancel_task` cannot deadlock via AB-BA. We hold
    /// `seen`'s write-lock across the queue mutation so that no `push` can
    /// observe a key already removed from `assignments` while still seeing it
    /// in `seen` (which would block a legitimate re-push).
    pub fn cancel_task(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<bool> {
        let mut seen = self
            .seen
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        let mut assignments = self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        let before = assignments.len();
        let mut removed_keys = Vec::new();
        assignments.retain(|assignment| {
            let remove = assignment.task_id() == task_id;
            if remove {
                removed_keys.push((
                    assignment.job_id().clone(),
                    assignment.task_id().clone(),
                    assignment.attempt_id(),
                ));
            }
            !remove
        });
        for key in &removed_keys {
            seen.remove(key);
        }
        let removed = assignments.len() != before;
        drop(assignments);
        drop(seen);
        self.cancelled_tasks
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .insert(task_id.clone());
        Ok(removed)
    }

    /// Whether a task id has been cancelled.
    pub fn is_task_cancelled(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<bool> {
        Ok(self
            .cancelled_tasks
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .contains(task_id))
    }

    /// Remove a task id from the cancelled set after the runner has handled it.
    pub fn clear_cancelled_task(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<()> {
        self.cancelled_tasks
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .remove(task_id);
        Ok(())
    }

    /// Snapshot all received assignments.
    pub fn assignments(&self) -> ExecutorResult<Vec<ExecutorTaskAssignment>> {
        Ok(self
            .assignments
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .iter()
            .cloned()
            .collect())
    }

    /// Number of assignments received so far.
    pub fn len(&self) -> ExecutorResult<usize> {
        Ok(self
            .assignments
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .len())
    }

    /// Whether the inbox is empty.
    pub fn is_empty(&self) -> ExecutorResult<bool> {
        Ok(self.len()? == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutorError;
    use krishiv_proto::{
        AttemptId, ExecutorId, ExecutorTaskAssignment, JobId, LeaseGeneration, OutputContract,
        OutputContractKind, PlanFragment, StageId, TaskAttemptRef, TaskId,
    };

    fn make_assignment(task_id: &str) -> ExecutorTaskAssignment {
        make_assignment_for_job("job-1", task_id)
    }

    fn make_assignment_for_job(job_id: &str, task_id: &str) -> ExecutorTaskAssignment {
        ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new(job_id).unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new(task_id).unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        )
    }

    #[test]
    fn new_inbox_is_empty() {
        let inbox = ExecutorAssignmentInbox::new();
        assert!(inbox.is_empty().unwrap());
        assert_eq!(inbox.len().unwrap(), 0);
    }

    #[test]
    fn push_increases_length() {
        let inbox = ExecutorAssignmentInbox::new();
        inbox.push(make_assignment("task-1")).unwrap();
        assert_eq!(inbox.len().unwrap(), 1);
        assert!(!inbox.is_empty().unwrap());
    }

    #[test]
    fn pop_next_returns_pushed_assignment() {
        let inbox = ExecutorAssignmentInbox::new();
        let assignment = make_assignment("task-1");
        inbox.push(assignment).unwrap();
        let popped = inbox.pop_next().unwrap().unwrap();
        assert_eq!(popped.task_id().as_str(), "task-1");
        assert!(inbox.is_empty().unwrap());
    }

    #[test]
    fn pop_next_returns_none_on_empty() {
        let inbox = ExecutorAssignmentInbox::new();
        assert!(inbox.pop_next().unwrap().is_none());
    }

    #[test]
    fn pop_next_fifo_order() {
        let inbox = ExecutorAssignmentInbox::new();
        inbox.push(make_assignment("task-1")).unwrap();
        inbox.push(make_assignment("task-2")).unwrap();
        inbox.push(make_assignment("task-3")).unwrap();
        assert_eq!(
            inbox.pop_next().unwrap().unwrap().task_id().as_str(),
            "task-1"
        );
        assert_eq!(
            inbox.pop_next().unwrap().unwrap().task_id().as_str(),
            "task-2"
        );
        assert_eq!(
            inbox.pop_next().unwrap().unwrap().task_id().as_str(),
            "task-3"
        );
        assert!(inbox.pop_next().unwrap().is_none());
    }

    #[test]
    fn cancel_task_removes_from_queue() {
        let inbox = ExecutorAssignmentInbox::new();
        inbox.push(make_assignment("task-1")).unwrap();
        inbox.push(make_assignment("task-2")).unwrap();
        let task_id = TaskId::try_new("task-1").unwrap();
        let removed = inbox.cancel_task(&task_id).unwrap();
        assert!(removed);
        assert_eq!(inbox.len().unwrap(), 1);
        assert_eq!(
            inbox.pop_next().unwrap().unwrap().task_id().as_str(),
            "task-2"
        );
    }

    #[test]
    fn cancel_task_marks_as_cancelled() {
        let inbox = ExecutorAssignmentInbox::new();
        let task_id = TaskId::try_new("task-1").unwrap();
        inbox.cancel_task(&task_id).unwrap();
        assert!(inbox.is_task_cancelled(&task_id).unwrap());
    }

    #[test]
    fn cancel_task_not_in_queue_marks_cancelled() {
        let inbox = ExecutorAssignmentInbox::new();
        let task_id = TaskId::try_new("task-1").unwrap();
        let removed = inbox.cancel_task(&task_id).unwrap();
        assert!(!removed);
        assert!(inbox.is_task_cancelled(&task_id).unwrap());
    }

    #[test]
    fn clear_cancelled_task_removes_from_cancelled_set() {
        let inbox = ExecutorAssignmentInbox::new();
        let task_id = TaskId::try_new("task-1").unwrap();
        inbox.cancel_task(&task_id).unwrap();
        assert!(inbox.is_task_cancelled(&task_id).unwrap());
        inbox.clear_cancelled_task(&task_id).unwrap();
        assert!(!inbox.is_task_cancelled(&task_id).unwrap());
    }

    #[test]
    fn assignments_returns_all() {
        let inbox = ExecutorAssignmentInbox::new();
        inbox.push(make_assignment("task-1")).unwrap();
        inbox.push(make_assignment("task-2")).unwrap();
        let all = inbox.assignments().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].task_id().as_str(), "task-1");
        assert_eq!(all[1].task_id().as_str(), "task-2");
    }

    #[test]
    fn same_task_attempt_in_different_jobs_is_not_duplicate() {
        let inbox = ExecutorAssignmentInbox::new();
        inbox
            .push(make_assignment_for_job("job-1", "task-0"))
            .unwrap();
        inbox
            .push(make_assignment_for_job("job-2", "task-0"))
            .unwrap();

        assert_eq!(inbox.len().unwrap(), 2);
    }

    #[test]
    fn duplicate_task_attempt_reports_duplicate_without_requeue() {
        let inbox = ExecutorAssignmentInbox::new();
        let first = make_assignment("task-dup");
        let duplicate = first.clone();

        assert_eq!(
            inbox.push_with_outcome(first).unwrap(),
            AssignmentPushOutcome::Enqueued
        );
        assert_eq!(
            inbox.push_with_outcome(duplicate).unwrap(),
            AssignmentPushOutcome::Duplicate
        );
        assert_eq!(inbox.len().unwrap(), 1);
    }

    #[test]
    fn cancel_queued_task_allows_same_attempt_to_be_requeued() {
        let inbox = ExecutorAssignmentInbox::new();
        let assignment = make_assignment("task-cancel-retry");
        let task_id = assignment.task_id().clone();
        inbox.push(assignment.clone()).unwrap();

        assert!(inbox.cancel_task(&task_id).unwrap());
        assert_eq!(
            inbox.push_with_outcome(assignment).unwrap(),
            AssignmentPushOutcome::Enqueued
        );
        assert_eq!(inbox.len().unwrap(), 1);
    }

    #[test]
    fn clone_shares_state() {
        let inbox1 = ExecutorAssignmentInbox::new();
        let inbox2 = inbox1.clone();
        inbox1.push(make_assignment("task-1")).unwrap();
        assert_eq!(inbox2.len().unwrap(), 1);
    }

    #[test]
    fn bounded_inbox_rejects_when_full() {
        let inbox = ExecutorAssignmentInbox::with_capacity(2);
        assert_eq!(inbox.capacity(), Some(2));

        inbox.push(make_assignment("t1")).unwrap();
        inbox.push(make_assignment("t2")).unwrap();

        let err = inbox.push(make_assignment("t3")).unwrap_err();
        match err {
            ExecutorError::AssignmentQueueFull { current, max } => {
                assert_eq!(current, 2);
                assert_eq!(max, 2);
            }
            other => panic!("expected AssignmentQueueFull, got {:?}", other),
        }

        // After pop, we should be able to push again
        let _ = inbox.pop_next().unwrap();
        inbox.push(make_assignment("t3")).unwrap();
        assert_eq!(inbox.len().unwrap(), 2);
    }

    #[test]
    fn unbounded_inbox_never_rejects() {
        let inbox = ExecutorAssignmentInbox::new_unbounded(); // unbounded
        assert!(inbox.capacity().is_none());
        for i in 0..5000 {
            inbox.push(make_assignment(&format!("t{}", i))).unwrap();
        }
        assert_eq!(inbox.len().unwrap(), 5000);
    }

    #[test]
    fn cancel_multiple_tasks() {
        let inbox = ExecutorAssignmentInbox::new();
        inbox.push(make_assignment("task-1")).unwrap();
        inbox.push(make_assignment("task-2")).unwrap();
        inbox.push(make_assignment("task-3")).unwrap();
        let t1 = TaskId::try_new("task-1").unwrap();
        let t3 = TaskId::try_new("task-3").unwrap();
        inbox.cancel_task(&t1).unwrap();
        inbox.cancel_task(&t3).unwrap();
        assert_eq!(inbox.len().unwrap(), 1);
        assert_eq!(
            inbox.pop_next().unwrap().unwrap().task_id().as_str(),
            "task-2"
        );
    }
}
