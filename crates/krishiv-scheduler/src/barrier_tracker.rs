//! Coordinator-side barrier acknowledgment tracking (R16 S1.4).

use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;

use krishiv_proto::wire::v1::BarrierAck;

/// Tracks expected vs received barrier acks for one checkpoint epoch.
#[derive(Debug)]
pub struct CheckpointBarrierTracker {
    pub epoch: u64,
    pub job_id: String,
    expected_tasks: HashSet<String>,
    received_acks: HashSet<String>,
    ack_details: Vec<BarrierAck>,
    started_at: Instant,
    timeout: Duration,
}

impl CheckpointBarrierTracker {
    pub fn new(
        job_id: impl Into<String>,
        epoch: u64,
        expected_tasks: impl IntoIterator<Item = String>,
        timeout: Duration,
    ) -> Self {
        Self {
            epoch,
            job_id: job_id.into(),
            expected_tasks: expected_tasks.into_iter().collect(),
            received_acks: HashSet::new(),
            ack_details: Vec::new(),
            started_at: Instant::now(),
            timeout,
        }
    }

    pub fn record_ack(&mut self, ack: &BarrierAck) -> bool {
        if ack.epoch != self.epoch || ack.job_id != self.job_id {
            return false;
        }
        if !self.received_acks.insert(ack.task_id.clone()) {
            return false;
        }
        self.ack_details.push(ack.clone());
        self.is_complete()
    }

    pub fn collected_acks(&self) -> &[BarrierAck] {
        &self.ack_details
    }

    pub fn is_complete(&self) -> bool {
        self.expected_tasks.is_subset(&self.received_acks)
    }

    pub fn completed_count(&self) -> usize {
        self.received_acks.len()
    }

    pub fn timed_out(&self) -> bool {
        self.started_at.elapsed() > self.timeout
    }

    pub fn missing_tasks(&self) -> Vec<String> {
        self.expected_tasks
            .difference(&self.received_acks)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::wire::v1::StateHandle;

    #[test]
    fn tracker_completes_when_all_acks_received() {
        let mut tracker = CheckpointBarrierTracker::new(
            "job-1",
            3,
            ["t0".to_string(), "t1".to_string()],
            Duration::from_secs(30),
        );
        assert!(!tracker.is_complete());
        tracker.record_ack(&BarrierAck {
            epoch: 3,
            job_id: "job-1".into(),
            task_id: "t0".into(),
            state_handle: Some(StateHandle {
                backend_kind: "redb".into(),
                checkpoint_uri: "/tmp/cp".into(),
                key_group_range_start: 0,
                key_group_range_end: 32767,
                schema_version: 1,
            }),
        });
        tracker.record_ack(&BarrierAck {
            epoch: 3,
            job_id: "job-1".into(),
            task_id: "t1".into(),
            state_handle: None,
        });
        assert!(tracker.is_complete());
    }
}
