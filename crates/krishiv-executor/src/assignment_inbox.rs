use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};

use crate::{ExecutorError, ExecutorResult};
use krishiv_proto::ExecutorTaskAssignment;

/// In-memory receiver queue for task assignments delivered to an executor.
#[derive(Debug, Clone, Default)]
pub struct ExecutorAssignmentInbox {
    assignments: Arc<RwLock<VecDeque<ExecutorTaskAssignment>>>,
    cancelled_tasks: Arc<RwLock<BTreeSet<krishiv_proto::TaskId>>>,
}

impl ExecutorAssignmentInbox {
    /// Create an empty assignment inbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store one received assignment.
    pub fn push(&self, assignment: ExecutorTaskAssignment) -> ExecutorResult<()> {
        self.assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .push_back(assignment);
        Ok(())
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
    pub fn cancel_task(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<bool> {
        let mut assignments = self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        let before = assignments.len();
        assignments.retain(|assignment| assignment.task_id() != task_id);
        let removed = assignments.len() != before;
        drop(assignments);
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
    use krishiv_proto::{
        AttemptId, ExecutorId, ExecutorTaskAssignment, JobId, LeaseGeneration, OutputContract,
        OutputContractKind, PlanFragment, StageId, TaskAttemptRef, TaskId,
    };

    fn make_assignment(task_id: &str) -> ExecutorTaskAssignment {
        ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-1").unwrap(),
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
    fn clone_shares_state() {
        let inbox1 = ExecutorAssignmentInbox::new();
        let inbox2 = inbox1.clone();
        inbox1.push(make_assignment("task-1")).unwrap();
        assert_eq!(inbox2.len().unwrap(), 1);
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
