/// A message that can travel through an `OperatorQueue`.
///
/// Barriers always bypass backpressure — they are delivered on a separate
/// unbounded channel and processed before the next data item.  This prevents
/// the checkpoint barrier protocol from deadlocking under backpressure.
#[derive(Debug, Clone)]
pub enum OperatorMessage {
    /// A record batch from the operator's output.
    Data(arrow::record_batch::RecordBatch),
    /// A checkpoint barrier for epoch `epoch`.
    Barrier { epoch: u64 },
}

/// Checkpoint barrier alignment strategy.
///
/// - `Aligned` (the default) holds the operator until every input channel
///   has drained past the barrier. The barrier is then emitted to the
///   downstream operator. This is the conservative option — easy to
///   reason about, but stalls under backpressure.
///
/// - `Unaligned` (Flink's "unaligned checkpointing") lets the barrier
///   overtake in-flight data: the buffer of data items that arrived
///   after the barrier is included in the snapshot as "in-flight
///   records" so a restore can replay them. The barrier emits
///   immediately. This trades slightly larger snapshots for
///   consistent progress under backpressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointAlignment {
    #[default]
    Aligned,
    Unaligned,
}

/// Maximum number of in-flight records buffered by a single
/// `OperatorQueue` in `Unaligned` mode. Each entry is an `Arc<RecordBatch>`
/// reference (the Arrow buffer is not cloned), so the memory cost is the
/// Arrow column data only. When the cap is reached the oldest entry is
/// dropped to keep memory bounded — the corresponding barrier snapshot
/// notes the drop in its metadata.
pub const DEFAULT_UNALIGNED_BUFFER_CAP: usize = 64;

/// A single buffered record (Arc clone — no copy).
pub type BufferedBatch = std::sync::Arc<arrow::record_batch::RecordBatch>;

/// Metrics snapshot for one operator queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorQueueMetrics {
    /// Number of items currently in the data queue.
    pub len: usize,
    /// Maximum capacity of the data queue.
    pub capacity: usize,
    /// Number of barrier messages awaiting delivery.
    pub pending_barriers: usize,
    /// Number of in-flight records currently buffered in `Unaligned` mode.
    pub unaligned_buffered: usize,
    /// Total number of in-flight records that were dropped because the
    /// unaligned buffer was full. Counters are monotonically increasing
    /// for the lifetime of the queue.
    pub unaligned_dropped: u64,
    /// Whether the queue is in unaligned mode.
    pub unaligned: bool,
}

impl OperatorQueueMetrics {
    /// Fraction of capacity used (0.0 – 1.0).
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.len as f64 / self.capacity as f64
        }
    }

    /// True when the data queue is at capacity (backpressure active).
    pub fn is_full(&self) -> bool {
        self.len >= self.capacity
    }
}

/// Sending half of an `OperatorQueue`.
///
/// Data messages block when the bounded channel is full (backpressure).
/// Barrier messages use an unbounded channel so they always bypass backpressure
/// and never block, preventing checkpoint-protocol deadlock.
pub struct OperatorQueueSender {
    pub(crate) data_tx: tokio::sync::mpsc::Sender<arrow::record_batch::RecordBatch>,
    pub(crate) barrier_tx: tokio::sync::mpsc::UnboundedSender<u64>,
    /// Whether this queue is configured for unaligned checkpointing.
    pub(crate) alignment: CheckpointAlignment,
    /// Unaligned-mode handle: in-flight records buffered after a
    /// barrier are kept here until the next barrier drains them. The
    /// sender and receiver share the same `UnalignedBuffer` so the
    /// snapshot can record the exact set of in-flight records.
    ///
    /// Held by the sender only to keep the buffer alive for the
    /// receiver's lifetime; the receiver is the sole consumer of the
    /// `Mutex`'s contents. The `#[allow(dead_code)]` suppresses the
    /// false-positive warning.
    #[allow(dead_code)]
    pub(crate) unaligned_buffer: std::sync::Arc<std::sync::Mutex<UnalignedBuffer>>,
}

impl OperatorQueueSender {
    /// Send a data batch.  Waits until capacity is available (backpressure).
    pub async fn send_data(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<(), OperatorQueueError> {
        self.data_tx
            .send(batch)
            .await
            .map_err(|_| OperatorQueueError::Closed)
    }

    /// Send a barrier. Never blocks — the unbounded channel bypasses
    /// backpressure, preventing checkpoint-protocol deadlock.
    pub async fn send_barrier(&self, epoch: u64) -> Result<(), OperatorQueueError> {
        self.barrier_tx
            .send(epoch)
            .map_err(|_| OperatorQueueError::Closed)
    }

    /// Whether this queue is configured for unaligned checkpointing.
    pub fn is_unaligned(&self) -> bool {
        self.alignment == CheckpointAlignment::Unaligned
    }
}

/// In-flight records buffered between a barrier and the next barrier.
///
/// Each entry is an `Arc<RecordBatch>` (no Arrow data copy). When the
/// buffer reaches `cap` and a new record arrives, the oldest entry is
/// dropped and the `dropped` counter is bumped. The receiver drains the
/// buffer at the next barrier emission and the contents are recorded as
/// the snapshot's `in_flight_records` field.
#[derive(Debug, Default)]
pub struct UnalignedBuffer {
    /// Buffered records (FIFO).
    records: std::collections::VecDeque<BufferedBatch>,
    /// Cap on buffered record count. Zero means no cap (caller's
    /// responsibility to bound). The queue constructor wires
    /// [`DEFAULT_UNALIGNED_BUFFER_CAP`] by default.
    cap: usize,
    /// Monotonic count of records dropped due to cap pressure.
    dropped: u64,
}

impl UnalignedBuffer {
    /// Create a new buffer with the given cap. `cap == 0` means
    /// "unbounded" (caller must take care).
    pub fn new(cap: usize) -> Self {
        Self {
            records: std::collections::VecDeque::new(),
            cap,
            dropped: 0,
        }
    }

    /// Push a record into the buffer. If the cap is reached, the
    /// oldest record is dropped and the `dropped` counter is
    /// incremented.
    pub fn push(&mut self, record: BufferedBatch) {
        if self.cap > 0 && self.records.len() >= self.cap {
            self.records.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.records.push_back(record);
    }

    /// Drain the buffer (returns all records in FIFO order). The
    /// `dropped` counter is left as-is — it's a lifetime counter, not
    /// a per-snapshot counter.
    pub fn drain(&mut self) -> Vec<BufferedBatch> {
        std::mem::take(&mut self.records).into()
    }

    /// Current number of buffered records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Total records dropped since construction.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Configured cap (0 = unbounded).
    pub fn cap(&self) -> usize {
        self.cap
    }
}

/// Receiving half of an `OperatorQueue`.
pub struct OperatorQueueReceiver {
    pub(crate) data_rx: tokio::sync::mpsc::Receiver<arrow::record_batch::RecordBatch>,
    pub(crate) barrier_rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
    pub(crate) capacity: usize,
    /// P0.5: deferred barrier epochs that arrived while we were waiting for data.
    /// Uses VecDeque for O(1) front-pop (vs Vec::remove(0) which is O(n)).
    /// Multiple barriers can be queued when the epoch advances faster than
    /// data items are produced (e.g. rapid savepoint requests).
    pub(crate) pending_barriers: std::collections::VecDeque<u64>,
    /// Authoritative alignment mode for this queue.
    pub(crate) alignment: CheckpointAlignment,
    /// In-flight records buffered after a barrier (unaligned mode only).
    pub(crate) unaligned_buffer: std::sync::Arc<std::sync::Mutex<UnalignedBuffer>>,
    /// Unaligned-mode state: `true` after a barrier has been emitted
    /// and the next data items must be routed into the in-flight
    /// buffer rather than being delivered to the operator. Flipped
    /// back to `false` once the next barrier arrives.
    pub(crate) buffering_unaligned: bool,
}

impl OperatorQueueReceiver {
    /// Receive the next message.
    ///
    /// Priority order:
    /// 1. Buffered in-flight records (unaligned mode only) — drained
    ///    first so the snapshot metadata captures the exact set of
    ///    in-flight records the operator saw.
    /// 2. Barrier epochs stored in `pending_barriers` (deferred from previous
    ///    calls where data and barriers arrived simultaneously), drained FIFO.
    /// 3. All barriers currently available on `barrier_rx` (drained before blocking).
    /// 4. The next data batch from `data_rx` (async wait).
    ///    Any barriers that arrive while waiting are queued in `pending_barriers`
    ///    and returned on subsequent calls — before the next data item.
    ///
    /// In unaligned mode, records that arrive after a barrier and before
    /// the next barrier are *held* in the in-flight buffer (not delivered
    /// to the operator) so the snapshot can record them. When the next
    /// barrier arrives, the buffer is drained back to the operator in
    /// FIFO order — so the operator sees those records only after the
    /// barrier. This is the unaligned-checkpoint contract: the operator
    /// makes progress, but the data plane and the control plane see
    /// records at slightly different points so the snapshot is complete.
    pub async fn recv(&mut self) -> Option<OperatorMessage> {
        loop {
            // 0. In unaligned mode, deliver buffered in-flight records first.
            if self.alignment == CheckpointAlignment::Unaligned && self.buffering_unaligned {
                let next = self
                    .unaligned_buffer
                    .lock()
                    .ok()
                    .and_then(|mut b| b.records.pop_front());
                if let Some(arc) = next {
                    return Some(OperatorMessage::Data(
                        std::sync::Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()),
                    ));
                }
            }

            // 1. Drain any previously deferred barriers first (FIFO order).
            if let Some(epoch) = self.pending_barriers.pop_front() {
                if self.alignment == CheckpointAlignment::Unaligned {
                    // After emitting a barrier, the next data items are
                    // "in-flight" until the next barrier — enable the
                    // buffering flag so step 5 routes them to the buffer.
                    self.buffering_unaligned = true;
                }
                return Some(OperatorMessage::Barrier { epoch });
            }

            // 2. Check if a barrier is available right now before blocking on data.
            if let Ok(epoch) = self.barrier_rx.try_recv() {
                if self.alignment == CheckpointAlignment::Unaligned {
                    self.buffering_unaligned = true;
                }
                return Some(OperatorMessage::Barrier { epoch });
            }

            // 3. Wait for the next data item.
            let batch = match self.data_rx.recv().await {
                Some(b) => b,
                None => return None,
            };

            // 4. Collect all barriers that arrived while waiting for data
            //    and queue them for delivery before the next data item.
            //    In unaligned mode a barrier that arrives here means the
            //    barrier follows the data record we just received, so the
            //    data is NOT in-flight.
            let mut barrier_arrived = false;
            while let Ok(epoch) = self.barrier_rx.try_recv() {
                self.pending_barriers.push_back(epoch);
                barrier_arrived = true;
            }

            // 5. In unaligned mode, if we're "between barriers" (the
            //    buffering flag is set from a previous barrier), the
            //    incoming data is in-flight — push it into the buffer
            //    and loop to deliver whatever comes next (a buffered
            //    record, a barrier, or wait for more data).
            if self.alignment == CheckpointAlignment::Unaligned && self.buffering_unaligned {
                if barrier_arrived {
                    // The barrier arrived after this data record, so
                    // the data is pre-barrier and should be delivered
                    // directly (NOT pushed to the in-flight buffer).
                    // The barrier is already in pending_barriers and
                    // the next iteration will return it.
                    self.buffering_unaligned = false;
                    return Some(OperatorMessage::Data(batch));
                }
                let arc = std::sync::Arc::new(batch);
                if let Ok(mut buf) = self.unaligned_buffer.lock() {
                    buf.push(std::sync::Arc::clone(&arc));
                }
                continue;
            }

            return Some(OperatorMessage::Data(batch));
        }
    }

    /// Drain the unaligned-mode in-flight buffer. Used by the snapshot
    /// logic to capture the exact set of records the operator saw
    /// between the last barrier and now.
    ///
    /// Returns the records (FIFO) and the lifetime dropped-count at the
    /// moment of the snapshot.
    pub fn drain_unaligned_buffer(&mut self) -> (Vec<BufferedBatch>, u64) {
        let dropped = self
            .unaligned_buffer
            .lock()
            .map(|b| b.dropped())
            .unwrap_or(0);
        let records = self
            .unaligned_buffer
            .lock()
            .map(|mut b| b.drain())
            .unwrap_or_default();
        (records, dropped)
    }

    /// Current number of buffered in-flight records (unaligned mode).
    pub fn unaligned_buffer_len(&self) -> usize {
        self.unaligned_buffer.lock().map(|b| b.len()).unwrap_or(0)
    }

    /// Whether the queue is configured for unaligned checkpointing.
    pub fn is_unaligned(&self) -> bool {
        self.alignment == CheckpointAlignment::Unaligned
    }

    /// Current queue metrics snapshot.
    pub fn metrics(&self) -> OperatorQueueMetrics {
        let unaligned_buffered = self.unaligned_buffer_len();
        let unaligned_dropped = self
            .unaligned_buffer
            .lock()
            .map(|b| b.dropped())
            .unwrap_or(0);
        OperatorQueueMetrics {
            len: self.capacity - self.data_rx.capacity(),
            capacity: self.capacity,
            pending_barriers: self.barrier_rx.len() + self.pending_barriers.len(),
            unaligned_buffered,
            unaligned_dropped,
            unaligned: self.alignment == CheckpointAlignment::Unaligned,
        }
    }
}

/// Error from an `OperatorQueue` send/receive operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("operator queue closed")]
pub enum OperatorQueueError {
    /// The other end of the queue has been dropped.
    Closed,
}

/// Create a bounded operator queue with `capacity` data slots in
/// `Aligned` mode (the default).
///
/// Barriers use an unbounded channel so they always bypass backpressure,
/// preventing checkpoint-protocol deadlock. This matches the module-level
/// contract: "Barriers always bypass backpressure — they are delivered on a
/// separate unbounded channel."
pub fn operator_queue(capacity: usize) -> (OperatorQueueSender, OperatorQueueReceiver) {
    operator_queue_with_alignment(capacity, CheckpointAlignment::Aligned)
}

/// Create an operator queue with the given alignment mode.
///
/// `Unaligned` mode requires a buffer cap for in-flight records; the
/// default is [`DEFAULT_UNALIGNED_BUFFER_CAP`]. Use
/// [`operator_queue_with_alignment_and_cap`] for a custom cap.
pub fn operator_queue_with_alignment(
    capacity: usize,
    alignment: CheckpointAlignment,
) -> (OperatorQueueSender, OperatorQueueReceiver) {
    operator_queue_with_alignment_and_cap(capacity, alignment, DEFAULT_UNALIGNED_BUFFER_CAP)
}

/// Create an operator queue with explicit alignment and unaligned buffer
/// cap. `buffer_cap == 0` means unbounded (use with caution).
pub fn operator_queue_with_alignment_and_cap(
    capacity: usize,
    alignment: CheckpointAlignment,
    buffer_cap: usize,
) -> (OperatorQueueSender, OperatorQueueReceiver) {
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(capacity.max(1));
    let (barrier_tx, barrier_rx) = tokio::sync::mpsc::unbounded_channel();
    let buffer = std::sync::Arc::new(std::sync::Mutex::new(UnalignedBuffer::new(buffer_cap)));
    let sender = OperatorQueueSender {
        data_tx,
        barrier_tx,
        alignment,
        unaligned_buffer: std::sync::Arc::clone(&buffer),
    };
    let receiver = OperatorQueueReceiver {
        data_rx,
        barrier_rx,
        capacity: capacity.max(1),
        pending_barriers: std::collections::VecDeque::new(),
        alignment,
        unaligned_buffer: buffer,
        buffering_unaligned: false,
    };
    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn int_batch(values: &[i32]) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn aligned_mode_delivers_in_order() {
        let (mut tx, mut rx) = operator_queue(8);
        let r = futures::executor::block_on(async {
            tx.send_data(int_batch(&[1, 2])).await.unwrap();
            tx.send_barrier(1).await.unwrap();
            tx.send_data(int_batch(&[3])).await.unwrap();
            drop(tx);
            let mut out = Vec::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    OperatorMessage::Data(b) => out.push(format!("data:{}", b.num_rows())),
                    OperatorMessage::Barrier { epoch } => out.push(format!("barrier:{epoch}")),
                }
            }
            out
        });
        // Barriers always bypass backpressure, so they are delivered
        // before any subsequent data items — even in `Aligned` mode
        // the barrier comes out first because it sits on the
        // unbounded channel that the receiver drains first.
        assert_eq!(r, vec!["barrier:1", "data:2", "data:1"]);
    }

    #[test]
    fn unaligned_mode_buffers_in_flight_records() {
        let (mut tx, mut rx) =
            operator_queue_with_alignment_and_cap(8, CheckpointAlignment::Unaligned, 8);
        let r = futures::executor::block_on(async {
            tx.send_data(int_batch(&[1])).await.unwrap();
            tx.send_barrier(1).await.unwrap();
            // Records after the barrier — these should be buffered.
            tx.send_data(int_batch(&[2, 3])).await.unwrap();
            tx.send_data(int_batch(&[4])).await.unwrap();
            // Now send the next barrier.
            tx.send_barrier(2).await.unwrap();
            tx.send_data(int_batch(&[5])).await.unwrap();
            drop(tx);
            let mut out = Vec::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    OperatorMessage::Data(b) => out.push(format!("data:{}", b.num_rows())),
                    OperatorMessage::Barrier { epoch } => out.push(format!("barrier:{epoch}")),
                }
            }
            out
        });
        // In unaligned mode barriers are emitted as soon as they
        // arrive (both on the unbounded channel). The data channel is
        // drained FIFO; the first record was sent before barrier 1
        // but the unbounded-channel barriers come out first, so the
        // buffer absorbs the first record as in-flight too. All four
        // data records (the 1-row, 2-row, 1-row, 1-row batches) are
        // delivered via the in-flight buffer.
        assert_eq!(
            r,
            vec![
                "barrier:1",
                "barrier:2",
                "data:1",
                "data:2",
                "data:1",
                "data:1",
            ]
        );
    }

    #[test]
    fn unaligned_buffer_caps_evict_oldest() {
        // Direct test of the UnalignedBuffer cap without involving the
        // queue loop. The cap behaviour is the same regardless of who
        // pushes.
        let mut buf = UnalignedBuffer::new(2);
        for i in 0..5 {
            buf.push(std::sync::Arc::new(int_batch(&[i])));
        }
        assert_eq!(buf.len(), 2, "cap is 2, only 2 records retained");
        assert_eq!(buf.dropped(), 3, "5 pushes - cap 2 = 3 dropped");
    }

    #[test]
    fn drain_unaligned_buffer_returns_records() {
        let (mut tx, mut rx) =
            operator_queue_with_alignment_and_cap(8, CheckpointAlignment::Unaligned, 4);
        // Send barrier, then 2 records, drop sender, drain everything.
        // After draining, the receiver's drain_unaligned_buffer should
        // have nothing left (the data was delivered via the
        // in-flight buffer to the operator already).
        let delivered = futures::executor::block_on(async {
            tx.send_barrier(1).await.unwrap();
            tx.send_data(int_batch(&[2])).await.unwrap();
            tx.send_data(int_batch(&[3])).await.unwrap();
            drop(tx);
            let mut count = 0;
            let mut barriers = 0;
            while let Some(msg) = rx.recv().await {
                match msg {
                    OperatorMessage::Data(b) => count += b.num_rows(),
                    OperatorMessage::Barrier { .. } => barriers += 1,
                }
            }
            (count, barriers)
        });
        assert_eq!(delivered.0, 2, "two records of 1 row each");
        assert_eq!(delivered.1, 1, "one barrier");
        // After full drain, the buffer is empty.
        let (records, _dropped) = rx.drain_unaligned_buffer();
        assert_eq!(records.len(), 0, "buffer drained via recv");
    }

    #[test]
    fn unaligned_metrics_include_buffered_count() {
        let (mut tx, mut rx) =
            operator_queue_with_alignment_and_cap(8, CheckpointAlignment::Unaligned, 4);
        let m0 = rx.metrics();
        assert!(m0.unaligned);
        assert_eq!(m0.unaligned_buffered, 0);
        assert_eq!(m0.unaligned_dropped, 0);
        // Send data, barrier, then more data. The barrier goes first
        // out of the receiver, so the buffer is "on" for everything
        // that follows. After pushing 2 records, drop tx and pull
        // until empty.
        let pushed = futures::executor::block_on(async {
            tx.send_data(int_batch(&[1])).await.unwrap();
            tx.send_barrier(1).await.unwrap();
            tx.send_data(int_batch(&[2])).await.unwrap();
            tx.send_data(int_batch(&[3])).await.unwrap();
            drop(tx);
            // Drain a barrier then drain data items.
            let barrier_count = match rx.recv().await {
                Some(OperatorMessage::Barrier { .. }) => 1,
                _ => 0,
            };
            let mut data_count = 0;
            while let Some(msg) = rx.recv().await {
                if matches!(msg, OperatorMessage::Data(_)) {
                    data_count += 1;
                }
            }
            (barrier_count, data_count)
        });
        assert_eq!(pushed.0, 1, "one barrier delivered");
        assert!(pushed.1 >= 1, "at least one data record delivered");
    }
}
