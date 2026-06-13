//! Aligned multi-input consumption for checkpoint barriers (Phase 3).
//!
//! Multi-input operators (joins, unions) must not mix pre-barrier and
//! post-barrier data of one epoch in their state, or checkpoints stop being
//! consistent cuts.  This module provides:
//!
//! - [`aligned_channel`]: a bounded **in-band** channel where barriers travel
//!   in order with data.  In-band ordering is what makes alignment exact —
//!   the out-of-band barrier channel in [`crate::queue`] lets barriers
//!   overtake queued data (good for source-side checkpoint initiation under
//!   backpressure, wrong for inter-operator epoch cuts).
//! - [`AlignedMultiInput`]: merges N aligned channels.  When input *i*
//!   delivers the barrier for epoch *E*, input *i* stops being polled — its
//!   bounded channel exerts backpressure upstream — until every live input
//!   has delivered the *E* barrier.  This is blocking alignment (Flink's
//!   aligned checkpoints); [`crate::barrier_align::BarrierAligner`] remains
//!   the buffering strategy for callers that cannot stop polling an input.
//! - [`AlignedWindowJoinDriver`]: a barrier-capable two-input
//!   [`WindowJoin`] runtime that snapshots join state at every aligned
//!   barrier and supports restore.

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use crate::window_join::{WindowJoin, WindowJoinSpec};
use crate::{ExecError, ExecResult};

/// One in-band message on an aligned input channel.
#[derive(Debug, Clone)]
pub enum AlignedInputMessage {
    Data(RecordBatch),
    Barrier { epoch: u64 },
}

/// Producer half of an aligned input channel.
#[derive(Debug, Clone)]
pub struct AlignedInputSender {
    tx: mpsc::Sender<AlignedInputMessage>,
}

impl AlignedInputSender {
    /// Send a data batch; awaits while the channel is at capacity
    /// (backpressure).
    pub async fn send_data(&self, batch: RecordBatch) -> ExecResult<()> {
        self.tx
            .send(AlignedInputMessage::Data(batch))
            .await
            .map_err(|_| ExecError::Arrow("aligned input receiver dropped".into()))
    }

    /// Send a checkpoint barrier **in band**: it queues behind any
    /// already-sent data, preserving the epoch cut.  Awaits while the channel
    /// is at capacity — under sustained backpressure the barrier is delayed,
    /// never reordered.
    pub async fn send_barrier(&self, epoch: u64) -> ExecResult<()> {
        self.tx
            .send(AlignedInputMessage::Barrier { epoch })
            .await
            .map_err(|_| ExecError::Arrow("aligned input receiver dropped".into()))
    }
}

/// Consumer half of an aligned input channel.
#[derive(Debug)]
pub struct AlignedInputReceiver {
    rx: mpsc::Receiver<AlignedInputMessage>,
}

/// Create a bounded in-band aligned input channel.
pub fn aligned_channel(capacity: usize) -> (AlignedInputSender, AlignedInputReceiver) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (AlignedInputSender { tx }, AlignedInputReceiver { rx })
}

/// Event produced by [`AlignedMultiInput::recv`].
#[derive(Debug)]
pub enum AlignedEvent {
    /// A data batch from `input`, safe to process under the current epoch.
    Data { input: usize, batch: RecordBatch },
    /// Every live input delivered the barrier for `epoch`: the operator must
    /// snapshot its state now, then the caller forwards the barrier
    /// downstream.
    AlignedBarrier { epoch: u64 },
    /// All inputs closed; no further events.
    InputsClosed,
}

/// Merges N aligned input channels with blocking barrier alignment.
#[derive(Debug)]
pub struct AlignedMultiInput {
    receivers: Vec<Option<AlignedInputReceiver>>,
    /// Inputs blocked since delivering the in-progress epoch's barrier.
    blocked: Vec<bool>,
    /// Epoch currently being aligned, if any.
    aligning_epoch: Option<u64>,
    /// Maximum wall-clock time to wait for slow inputs once alignment starts.
    alignment_timeout: Duration,
}

impl AlignedMultiInput {
    /// Create an aligned merger over `inputs` with an alignment timeout.
    pub fn new(inputs: Vec<AlignedInputReceiver>, alignment_timeout: Duration) -> ExecResult<Self> {
        if inputs.is_empty() {
            return Err(ExecError::Arrow(
                "aligned multi-input requires at least one input".into(),
            ));
        }
        let n = inputs.len();
        Ok(Self {
            receivers: inputs.into_iter().map(Some).collect(),
            blocked: vec![false; n],
            aligning_epoch: None,
            alignment_timeout,
        })
    }

    /// Epoch currently being aligned, if alignment is in progress.
    pub fn aligning_epoch(&self) -> Option<u64> {
        self.aligning_epoch
    }

    fn live_count(&self) -> usize {
        self.receivers.iter().filter(|r| r.is_some()).count()
    }

    /// Alignment completes when every input that is still open has delivered
    /// the barrier.  Closed inputs cannot contribute data and are exempt.
    fn alignment_complete(&self) -> bool {
        self.aligning_epoch.is_some()
            && self
                .receivers
                .iter()
                .zip(&self.blocked)
                .all(|(receiver, blocked)| receiver.is_none() || *blocked)
    }

    fn finish_alignment(&mut self) -> u64 {
        for flag in &mut self.blocked {
            *flag = false;
        }
        self.aligning_epoch
            .take()
            .expect("finish_alignment requires an in-progress epoch")
    }

    /// Receive the next aligned event.
    ///
    /// Cancel-safe with respect to data: the underlying tokio mpsc `recv` is
    /// cancel-safe, and bookkeeping mutations happen only after a message is
    /// returned from a channel.
    pub async fn recv(&mut self) -> ExecResult<AlignedEvent> {
        loop {
            if self.live_count() == 0 {
                return Ok(AlignedEvent::InputsClosed);
            }
            // An alignment can complete via input closure between recv calls.
            if self.alignment_complete() {
                let epoch = self.finish_alignment();
                return Ok(AlignedEvent::AlignedBarrier { epoch });
            }

            // Poll every open, unblocked input concurrently.  Blocked inputs
            // are intentionally not polled: their bounded channels fill and
            // exert backpressure upstream until alignment completes.
            let aligning_epoch = self.aligning_epoch;
            let alignment_timeout = self.alignment_timeout;
            let (input, message) = {
                let blocked = &self.blocked;
                let receivers = &mut self.receivers;
                let polls = receivers
                    .iter_mut()
                    .enumerate()
                    .filter(|(idx, receiver)| receiver.is_some() && !blocked[*idx])
                    .map(|(idx, receiver)| {
                        let rx = receiver.as_mut().expect("filtered to Some");
                        Box::pin(async move { (idx, rx.rx.recv().await) })
                            as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send + '_>>
                    })
                    .collect::<Vec<_>>();
                if polls.is_empty() {
                    // All open inputs are blocked but alignment_complete was
                    // false → impossible by construction.
                    return Err(ExecError::Arrow(
                        "aligned multi-input deadlock: all inputs blocked without completing \
                         alignment"
                            .into(),
                    ));
                }

                let selected = if let Some(epoch) = aligning_epoch {
                    match tokio::time::timeout(
                        alignment_timeout,
                        futures::future::select_all(polls),
                    )
                    .await
                    {
                        Ok(selected) => selected,
                        Err(_) => {
                            return Err(ExecError::Arrow(format!(
                                "barrier alignment for epoch {epoch} timed out after \
                                 {alignment_timeout:?}; a slow or stalled input never \
                                 delivered its barrier"
                            )));
                        }
                    }
                } else {
                    futures::future::select_all(polls).await
                };
                let ((input, message), _, remaining) = selected;
                drop(remaining);
                (input, message)
            };

            match message {
                None => {
                    // Input closed (producer dropped).  Re-evaluate alignment
                    // and termination at the top of the loop.
                    self.receivers[input] = None;
                }
                Some(AlignedInputMessage::Data(batch)) => {
                    return Ok(AlignedEvent::Data { input, batch });
                }
                Some(AlignedInputMessage::Barrier { epoch }) => {
                    match self.aligning_epoch {
                        None => self.aligning_epoch = Some(epoch),
                        Some(current) if current == epoch => {}
                        Some(current) => {
                            return Err(ExecError::Arrow(format!(
                                "barrier epoch mismatch during alignment: input {input} \
                                 delivered epoch {epoch} while aligning epoch {current}"
                            )));
                        }
                    }
                    if self.blocked[input] {
                        return Err(ExecError::Arrow(format!(
                            "duplicate barrier for epoch {epoch} on input {input}"
                        )));
                    }
                    self.blocked[input] = true;
                    if self.alignment_complete() {
                        let epoch = self.finish_alignment();
                        return Ok(AlignedEvent::AlignedBarrier { epoch });
                    }
                }
            }
        }
    }
}

/// Barrier-capable two-input window join runtime.
///
/// Drives a [`WindowJoin`] from two aligned inputs (0 = left, 1 = right).
/// At every aligned barrier the join's buffered state is snapshot and handed
/// to `on_barrier` — the executor persists it and acks the checkpoint.
pub struct AlignedWindowJoinDriver {
    inputs: AlignedMultiInput,
    join: WindowJoin,
}

impl AlignedWindowJoinDriver {
    pub fn new(
        left: AlignedInputReceiver,
        right: AlignedInputReceiver,
        spec: WindowJoinSpec,
        alignment_timeout: Duration,
    ) -> ExecResult<Self> {
        Ok(Self {
            inputs: AlignedMultiInput::new(vec![left, right], alignment_timeout)?,
            join: WindowJoin::new(spec),
        })
    }

    /// Seed the join state from a snapshot produced at an aligned barrier.
    pub fn restore(&mut self, snapshot: &[u8]) -> ExecResult<()> {
        self.join.restore(snapshot)
    }

    /// Run until both inputs close.
    ///
    /// `on_output` receives joined result batches as windows close;
    /// `on_barrier(epoch, snapshot)` runs at each aligned barrier with the
    /// join state captured at the exact epoch cut.  An `on_barrier` error
    /// aborts the run — an unacked checkpoint must not silently continue.
    pub async fn run(
        mut self,
        mut on_output: impl FnMut(RecordBatch) + Send,
        mut on_barrier: impl FnMut(u64, Vec<u8>) -> ExecResult<()> + Send,
    ) -> ExecResult<()> {
        loop {
            match self.inputs.recv().await? {
                AlignedEvent::Data { input, batch } => {
                    match input {
                        0 => self.join.push_left(&batch)?,
                        _ => self.join.push_right(&batch)?,
                    }
                    // Flush windows the advancing watermark closed.
                    for closed in self.join.advance_watermark(i64::MIN)? {
                        on_output(closed);
                    }
                }
                AlignedEvent::AlignedBarrier { epoch } => {
                    let snapshot = self.join.snapshot()?;
                    on_barrier(epoch, snapshot)?;
                }
                AlignedEvent::InputsClosed => {
                    for batch in self.join.flush_all()? {
                        on_output(batch);
                    }
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(keys: &[&str], times: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int64Array::from(times.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn aligned_barrier_fires_only_after_all_inputs_deliver() {
        let (tx_a, rx_a) = aligned_channel(8);
        let (tx_b, rx_b) = aligned_channel(8);
        let mut merged = AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_secs(5)).unwrap();

        tx_a.send_data(batch(&["x"], &[1])).await.unwrap();
        tx_a.send_barrier(1).await.unwrap();
        tx_b.send_data(batch(&["y"], &[2])).await.unwrap();

        // First two events: data from a, then (a blocked) data from b.
        let mut data_seen = 0;
        for _ in 0..2 {
            match merged.recv().await.unwrap() {
                AlignedEvent::Data { .. } => data_seen += 1,
                other => panic!("expected data, got {other:?}"),
            }
        }
        assert_eq!(data_seen, 2);
        assert_eq!(merged.aligning_epoch(), Some(1));

        // b's barrier completes the alignment.
        tx_b.send_barrier(1).await.unwrap();
        match merged.recv().await.unwrap() {
            AlignedEvent::AlignedBarrier { epoch } => assert_eq!(epoch, 1),
            other => panic!("expected aligned barrier, got {other:?}"),
        }
        assert_eq!(merged.aligning_epoch(), None);
    }

    #[tokio::test]
    async fn barriered_input_is_not_polled_until_alignment_completes() {
        let (tx_a, rx_a) = aligned_channel(8);
        let (tx_b, rx_b) = aligned_channel(8);
        let mut merged = AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_secs(5)).unwrap();

        // a: barrier then post-barrier data; b: pre-barrier data.
        tx_a.send_barrier(7).await.unwrap();
        tx_a.send_data(batch(&["post"], &[100])).await.unwrap();
        tx_b.send_data(batch(&["pre"], &[1])).await.unwrap();
        tx_b.send_barrier(7).await.unwrap();

        // Pre-barrier data from b must be delivered BEFORE the aligned
        // barrier; post-barrier data from a must come only after it.
        let first = merged.recv().await.unwrap();
        match first {
            AlignedEvent::Data { input, .. } => assert_eq!(input, 1, "b's pre-barrier data first"),
            other => panic!("expected data, got {other:?}"),
        }
        match merged.recv().await.unwrap() {
            AlignedEvent::AlignedBarrier { epoch } => assert_eq!(epoch, 7),
            other => panic!("expected aligned barrier, got {other:?}"),
        }
        match merged.recv().await.unwrap() {
            AlignedEvent::Data { input, .. } => {
                assert_eq!(input, 0, "a's post-barrier data only after alignment")
            }
            other => panic!("expected data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn epoch_mismatch_during_alignment_is_typed_error() {
        let (tx_a, rx_a) = aligned_channel(8);
        let (tx_b, rx_b) = aligned_channel(8);
        let mut merged = AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_secs(5)).unwrap();

        tx_a.send_barrier(3).await.unwrap();
        tx_b.send_barrier(4).await.unwrap();

        // One barrier starts alignment; the mismatched one errors.
        let mut saw_error = false;
        for _ in 0..2 {
            match merged.recv().await {
                Ok(_) => {}
                Err(e) => {
                    assert!(e.to_string().contains("epoch mismatch"), "{e}");
                    saw_error = true;
                    break;
                }
            }
        }
        assert!(saw_error, "mismatched epochs must surface a typed error");
    }

    #[tokio::test]
    async fn alignment_timeout_produces_typed_error() {
        let (tx_a, rx_a) = aligned_channel(8);
        let (_tx_b, rx_b) = aligned_channel(8);
        let mut merged =
            AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_millis(50)).unwrap();

        tx_a.send_barrier(1).await.unwrap();
        // b never sends its barrier (but stays open).
        let err = loop {
            match merged.recv().await {
                Ok(AlignedEvent::Data { .. }) => continue,
                Ok(other) => panic!("unexpected event {other:?}"),
                Err(e) => break e,
            }
        };
        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn closed_input_is_exempt_from_alignment() {
        let (tx_a, rx_a) = aligned_channel(8);
        let (tx_b, rx_b) = aligned_channel(8);
        let mut merged = AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_secs(5)).unwrap();

        tx_a.send_barrier(2).await.unwrap();
        drop(tx_b); // b closes without ever delivering a barrier

        match merged.recv().await.unwrap() {
            AlignedEvent::AlignedBarrier { epoch } => assert_eq!(epoch, 2),
            other => panic!("expected aligned barrier, got {other:?}"),
        }
        drop(tx_a);
        match merged.recv().await.unwrap() {
            AlignedEvent::InputsClosed => {}
            other => panic!("expected inputs closed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn consecutive_epochs_align_independently() {
        // Barriers for epochs 1 and 2 queued in-band on both inputs: each
        // epoch must align exactly once, in order, with the data between
        // them attributed to the correct epoch side.
        let (tx_a, rx_a) = aligned_channel(8);
        let (tx_b, rx_b) = aligned_channel(8);
        let mut merged = AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_secs(5)).unwrap();

        tx_a.send_barrier(1).await.unwrap();
        tx_a.send_data(batch(&["between"], &[5])).await.unwrap();
        tx_a.send_barrier(2).await.unwrap();
        tx_b.send_barrier(1).await.unwrap();
        tx_b.send_barrier(2).await.unwrap();
        drop(tx_a);
        drop(tx_b);

        let mut events = Vec::new();
        loop {
            match merged.recv().await.unwrap() {
                AlignedEvent::InputsClosed => break,
                AlignedEvent::AlignedBarrier { epoch } => events.push(format!("barrier-{epoch}")),
                AlignedEvent::Data { input, .. } => events.push(format!("data-{input}")),
            }
        }
        assert_eq!(
            events,
            vec!["barrier-1", "data-0", "barrier-2"],
            "epoch cuts must order the in-between data after epoch 1 and before epoch 2"
        );
    }

    #[tokio::test]
    async fn backpressure_blocked_input_fills_bounded_channel() {
        // Capacity-2 channel: once input a is blocked by its barrier, its
        // producer can buffer at most 2 more batches and then must wait —
        // proving alignment exerts backpressure instead of unbounded buffering.
        let (tx_a, rx_a) = aligned_channel(2);
        let (tx_b, rx_b) = aligned_channel(2);
        let mut merged = AlignedMultiInput::new(vec![rx_a, rx_b], Duration::from_secs(5)).unwrap();

        tx_a.send_barrier(1).await.unwrap();
        // Block a; the merger must observe the barrier first.
        // (No data sent yet, so the next recv starts alignment.)
        let recv_task = tokio::spawn(async move {
            // Wait for the aligned barrier only.
            loop {
                match merged.recv().await {
                    Ok(AlignedEvent::AlignedBarrier { epoch }) => return (merged, epoch),
                    Ok(_) => continue,
                    Err(e) => panic!("unexpected error: {e}"),
                }
            }
        });

        // While a is blocked, fill its channel to capacity…
        tx_a.send_data(batch(&["x1"], &[1])).await.unwrap();
        tx_a.send_data(batch(&["x2"], &[2])).await.unwrap();
        // …and verify the third send would block (backpressure).
        let blocked_send = tokio::time::timeout(
            Duration::from_millis(100),
            tx_a.send_data(batch(&["x3"], &[3])),
        )
        .await;
        assert!(
            blocked_send.is_err(),
            "send on a barrier-blocked input at capacity must backpressure"
        );

        // b's barrier releases the alignment.
        tx_b.send_barrier(1).await.unwrap();
        let (_merged, epoch) = recv_task.await.unwrap();
        assert_eq!(epoch, 1);
    }

    #[tokio::test]
    async fn window_join_driver_snapshots_at_aligned_barrier_and_restores() {
        let spec = WindowJoinSpec {
            left_key: "k".into(),
            right_key: "k".into(),
            time_column: "ts".into(),
            window_ms: 10_000,
            watermark_lag_ms: 0,
        };

        // Run 1: push one left row, snapshot at the aligned barrier, then
        // "crash" (drop the driver without closing windows).
        let (tx_l, rx_l) = aligned_channel(8);
        let (tx_r, rx_r) = aligned_channel(8);
        let driver =
            AlignedWindowJoinDriver::new(rx_l, rx_r, spec.clone(), Duration::from_secs(5)).unwrap();

        let snapshots: std::sync::Arc<std::sync::Mutex<Vec<(u64, Vec<u8>)>>> = Default::default();
        let snapshots_in = Arc::clone(&snapshots);
        let run = tokio::spawn(async move {
            driver
                .run(
                    |_batch| {},
                    move |epoch, snap| {
                        snapshots_in.lock().unwrap().push((epoch, snap));
                        Ok(())
                    },
                )
                .await
        });

        tx_l.send_data(batch(&["a"], &[1_000])).await.unwrap();
        tx_l.send_barrier(1).await.unwrap();
        tx_r.send_barrier(1).await.unwrap();
        drop(tx_l);
        drop(tx_r);
        run.await.unwrap().unwrap();

        let captured = snapshots.lock().unwrap().clone();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, 1);

        // Run 2: restore from the snapshot, push the matching right row, and
        // verify the join completes across the restart.
        let (tx_l2, rx_l2) = aligned_channel(8);
        let (tx_r2, rx_r2) = aligned_channel(8);
        let mut driver2 =
            AlignedWindowJoinDriver::new(rx_l2, rx_r2, spec, Duration::from_secs(5)).unwrap();
        driver2.restore(&captured[0].1).unwrap();

        let outputs: std::sync::Arc<std::sync::Mutex<Vec<RecordBatch>>> = Default::default();
        let outputs_in = Arc::clone(&outputs);
        let run2 = tokio::spawn(async move {
            driver2
                .run(
                    move |batch| outputs_in.lock().unwrap().push(batch),
                    |_, _| Ok(()),
                )
                .await
        });

        tx_r2.send_data(batch(&["a"], &[2_000])).await.unwrap();
        drop(tx_l2);
        drop(tx_r2);
        run2.await.unwrap().unwrap();

        let produced = outputs.lock().unwrap();
        let total_rows: usize = produced.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "left row buffered before the crash must join the right row after restore"
        );
    }
}
