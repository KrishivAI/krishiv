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
