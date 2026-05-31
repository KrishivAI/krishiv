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
    pub(crate) barrier_tx: tokio::sync::mpsc::Sender<u64>,
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

    /// Send a barrier. Blocks if the barrier queue is full.
    /// If full, logs a warning and drops the barrier; the coordinator will retry via try_tick.
    pub async fn send_barrier(&self, epoch: u64) -> Result<(), OperatorQueueError> {
        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            self.barrier_tx.send(epoch),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(OperatorQueueError::Closed),
            Err(_) => {
                tracing::warn!(epoch = epoch, "barrier queue full; dropping barrier");
                Ok(())
            }
        }
    }
}

/// Receiving half of an `OperatorQueue`.
pub struct OperatorQueueReceiver {
    pub(crate) data_rx: tokio::sync::mpsc::Receiver<arrow::record_batch::RecordBatch>,
    pub(crate) barrier_rx: tokio::sync::mpsc::Receiver<u64>,
    pub(crate) capacity: usize,
    /// P0.5: deferred barrier epochs that arrived while we were waiting for data.
    /// Uses VecDeque for O(1) front-pop (vs Vec::remove(0) which is O(n)).
    /// Multiple barriers can be queued when the epoch advances faster than
    /// data items are produced (e.g. rapid savepoint requests).
    pub(crate) pending_barriers: std::collections::VecDeque<u64>,
}

impl OperatorQueueReceiver {
    /// Receive the next message.
    ///
    /// Priority order:
    /// 1. Barrier epochs stored in `pending_barriers` (deferred from previous
    ///    calls where data and barriers arrived simultaneously), drained FIFO.
    /// 2. All barriers currently available on `barrier_rx` (drained before blocking).
    /// 3. The next data batch from `data_rx` (async wait).
    ///    Any barriers that arrive while waiting are queued in `pending_barriers`
    ///    and returned on subsequent calls — before the next data item.
    pub async fn recv(&mut self) -> Option<OperatorMessage> {
        // 1. Drain any previously deferred barriers first (FIFO order).
        if let Some(epoch) = self.pending_barriers.pop_front() {
            return Some(OperatorMessage::Barrier { epoch });
        }

        // 2. Check if a barrier is available right now before blocking on data.
        if let Ok(epoch) = self.barrier_rx.try_recv() {
            return Some(OperatorMessage::Barrier { epoch });
        }

        // 3. Wait for the next data item.
        let batch = self.data_rx.recv().await?;

        // 4. Collect all barriers that arrived while waiting for data
        //    and queue them for delivery before the next data item.
        while let Ok(epoch) = self.barrier_rx.try_recv() {
            self.pending_barriers.push_back(epoch);
        }

        Some(OperatorMessage::Data(batch))
    }

    /// Current queue metrics snapshot.
    pub fn metrics(&self) -> OperatorQueueMetrics {
        OperatorQueueMetrics {
            len: self.capacity - self.data_rx.capacity(),
            capacity: self.capacity,
            pending_barriers: self.barrier_rx.len() + self.pending_barriers.len(),
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

/// Create a bounded operator queue with `capacity` data slots.
///
/// Barriers also use a bounded channel (M2: cap=64) to prevent unbounded memory growth
/// from rapid checkpoint storms. Dropping barriers is safe because they are idempotent
/// and the coordinator will retry via try_tick on the next interval.
pub fn operator_queue(capacity: usize) -> (OperatorQueueSender, OperatorQueueReceiver) {
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(capacity.max(1));
    let (barrier_tx, barrier_rx) = tokio::sync::mpsc::channel(64);
    let sender = OperatorQueueSender {
        data_tx,
        barrier_tx,
    };
    let receiver = OperatorQueueReceiver {
        data_rx,
        barrier_rx,
        capacity: capacity.max(1),
        pending_barriers: std::collections::VecDeque::new(),
    };
    (sender, receiver)
}
