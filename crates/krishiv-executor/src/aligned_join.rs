//! Barrier-aligned windowed-join execution on the executor (B3 distributed
//! checkpoint).
//!
//! This bridges the dataflow continuous barrier-aligned join
//! ([`execute_window_join_aligned`]) to the coordinator's checkpoint-barrier ack
//! protocol. As each checkpoint epoch aligns across the join's two inputs and the
//! operator is snapshotted, the matching `(job_id, epoch)` waiter in the
//! [`SharedBarrierAckRegistry`] is completed — so the coordinator's
//! `send_barrier_and_wait_ack` proceeds only after a consistent two-input
//! snapshot was taken.
//!
//! The caller builds the [`JoinStreamEvent`] stream by interleaving the assigned
//! left/right input with the checkpoint barriers drained from the
//! [`BarrierInjector`](crate::barrier_transport::BarrierInjector); this function
//! owns the alignment, the snapshot, and the ack completion.

use krishiv_dataflow::watermark_join::WatermarkWindowJoinSpec;
use krishiv_dataflow::{AlignedJoinOutput, JoinStreamEvent, execute_window_join_aligned};

use crate::barrier_transport::{BarrierAckCompletion, SharedBarrierAckRegistry};
use crate::error::{ExecutorError, ExecutorResult};

/// Run a windowed join over `events` with barrier-aligned checkpointing, then
/// complete each aligned epoch in `ack_registry`.
///
/// `checkpoint_uri` names where each epoch's snapshot was (or will be) persisted;
/// the returned [`AlignedJoinOutput`] carries the snapshot bytes per epoch for
/// the caller to store and the joined output rows.
pub fn run_aligned_window_join(
    spec: WatermarkWindowJoinSpec,
    job_id: &str,
    events: Vec<JoinStreamEvent>,
    ack_registry: &SharedBarrierAckRegistry,
    checkpoint_uri: impl Fn(u64) -> String,
) -> ExecutorResult<AlignedJoinOutput> {
    let out = execute_window_join_aligned(spec, events, i64::MAX).map_err(|e| {
        ExecutorError::LocalExecution {
            message: format!("barrier-aligned window join: {e}"),
        }
    })?;
    for (epoch, _bytes) in &out.snapshots {
        ack_registry.complete(
            job_id,
            *epoch,
            BarrierAckCompletion {
                checkpoint_uri: checkpoint_uri(*epoch),
                key_group_range_start: 0,
                key_group_range_end: 0,
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn spec() -> WatermarkWindowJoinSpec {
        WatermarkWindowJoinSpec {
            time_column: "ts".into(),
            left_key_column: "id".into(),
            right_key_column: "id".into(),
            window_ms: 500,
        }
    }

    fn jb(id: &str, ts: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn aligned_epoch_completes_the_barrier_ack_waiter() {
        let registry = SharedBarrierAckRegistry::new();
        // A coordinator-side waiter for (job, epoch 1) — completed only after a
        // consistent two-input snapshot is taken.
        let mut waiter = registry.register_wait("job-x", 1);

        // left k@1000; left barrier ep1 (left blocks); left k@1100 buffered;
        // right k@1200 (matches in-epoch); right barrier ep1 → aligns → snapshot.
        let events = vec![
            JoinStreamEvent::Left(jb("k", 1000)),
            JoinStreamEvent::LeftBarrier(1),
            JoinStreamEvent::Left(jb("k", 1100)),
            JoinStreamEvent::Right(jb("k", 1200)),
            JoinStreamEvent::RightBarrier(1),
        ];

        let out = run_aligned_window_join(spec(), "job-x", events, &registry, |e| {
            format!("memory://ckpt/job-x/{e}")
        })
        .expect("aligned join runs");

        assert_eq!(out.snapshots.len(), 1, "one aligned checkpoint at epoch 1");
        // The ack waiter was completed with the checkpoint URI.
        let completion = waiter
            .try_recv()
            .expect("the barrier ack must be completed on alignment");
        assert_eq!(completion.checkpoint_uri, "memory://ckpt/job-x/1");
        let joined_rows: usize = out.joined.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(joined_rows, 2, "in-epoch + replayed match both emitted");
    }

    #[test]
    fn no_barriers_completes_no_acks() {
        let registry = SharedBarrierAckRegistry::new();
        let mut waiter = registry.register_wait("job-y", 1);
        let events = vec![
            JoinStreamEvent::Left(jb("k", 1000)),
            JoinStreamEvent::Right(jb("k", 1200)),
        ];
        let out = run_aligned_window_join(spec(), "job-y", events, &registry, |e| format!("u/{e}"))
            .expect("join runs");
        assert!(out.snapshots.is_empty());
        // No alignment ⇒ the waiter is never completed.
        assert!(waiter.try_recv().is_err());
    }
}
