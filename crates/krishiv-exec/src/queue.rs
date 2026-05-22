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

/// Metrics snapshot for one operator queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorQueueMetrics {
    /// Number of items currently in the data queue.
    pub len: usize,
    /// Maximum capacity of the data queue.
    pub capacity: usize,
    /// Number of barrier messages awaiting delivery.
    pub pending_barriers: usize,
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
/// Barrier messages are always sent without blocking.
pub struct OperatorQueueSender {
    pub(crate) data_tx: tokio::sync::mpsc::Sender<arrow::record_batch::RecordBatch>,
    pub(crate) barrier_tx: tokio::sync::mpsc::UnboundedSender<u64>,
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

    /// Send a barrier.  Never blocks — barriers bypass backpressure.
    pub fn send_barrier(&self, epoch: u64) -> Result<(), OperatorQueueError> {
        self.barrier_tx
            .send(epoch)
            .map_err(|_| OperatorQueueError::Closed)
    }
}

/// Receiving half of an `OperatorQueue`.
pub struct OperatorQueueReceiver {
    pub(crate) data_rx: tokio::sync::mpsc::Receiver<arrow::record_batch::RecordBatch>,
    pub(crate) barrier_rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
    pub(crate) capacity: usize,
    /// P0.5: a barrier epoch that was deferred because a data item arrived
    /// at the same time.  This is drained before the next data receive.
    pub(crate) pending_barrier: Option<u64>,
}

impl OperatorQueueReceiver {
    /// Receive the next message.
    ///
    /// Priority order:
    /// 1. A barrier epoch stored in `pending_barrier` (deferred from a
    ///    previous call where data and a barrier arrived simultaneously).
    /// 2. A barrier available right now on `barrier_rx`.
    /// 3. The next data batch from `data_rx` (async wait).
    ///    If a barrier also arrives after the data batch is dequeued, the
    ///    epoch is saved to `pending_barrier` and returned on the *next* call.
    pub async fn recv(&mut self) -> Option<OperatorMessage> {
        // 1. Drain any previously deferred barrier first.
        if let Some(epoch) = self.pending_barrier.take() {
            return Some(OperatorMessage::Barrier { epoch });
        }

        // 2. Drain any barrier that is already in the channel before blocking.
        match self.barrier_rx.try_recv() {
            Ok(epoch) => return Some(OperatorMessage::Barrier { epoch }),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {}
        }

        // 3. Wait for the next data item.
        let batch = self.data_rx.recv().await?;

        // 4. If a barrier arrived simultaneously (between the try_recv above
        //    and the data recv), save it so it is delivered on the next call
        //    — before the subsequent data item, preserving ordering.
        if let Ok(epoch) = self.barrier_rx.try_recv() {
            self.pending_barrier = Some(epoch);
        }

        Some(OperatorMessage::Data(batch))
    }

    /// Current queue metrics snapshot.
    pub fn metrics(&self) -> OperatorQueueMetrics {
        OperatorQueueMetrics {
            len: self.capacity - self.data_rx.capacity(),
            capacity: self.capacity,
            pending_barriers: self.barrier_rx.len()
                + if self.pending_barrier.is_some() { 1 } else { 0 },
        }
    }
}

/// Error from an `OperatorQueue` send/receive operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorQueueError {
    /// The other end of the queue has been dropped.
    Closed,
}

impl std::fmt::Display for OperatorQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operator queue closed")
    }
}

impl std::error::Error for OperatorQueueError {}

/// Create a bounded operator queue with `capacity` data slots.
///
/// Barriers bypass the bounded channel and are never subject to backpressure.
pub fn operator_queue(capacity: usize) -> (OperatorQueueSender, OperatorQueueReceiver) {
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(capacity.max(1));
    let (barrier_tx, barrier_rx) = tokio::sync::mpsc::unbounded_channel();
    let sender = OperatorQueueSender {
        data_tx,
        barrier_tx,
    };
    let receiver = OperatorQueueReceiver {
        data_rx,
        barrier_rx,
        capacity: capacity.max(1),
        pending_barrier: None,
    };
    (sender, receiver)
}
