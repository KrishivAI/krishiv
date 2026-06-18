//! Pipeline sink adapters — write a view's output to a destination.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_connectors::DynSink;

use crate::{KrishivError, Result};

/// Where a view's output is written.
pub enum Egress {
    /// Collect output batches in memory (testing / embedding).
    Memory(Arc<Mutex<Vec<RecordBatch>>>),
    /// Push output batches to a pull/push connector sink.
    Connector(Box<dyn DynSink>),
}

impl Egress {
    /// Write one output batch to the destination.
    pub(crate) async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            Egress::Memory(handle) => {
                handle
                    .lock()
                    .map_err(|_| KrishivError::Runtime {
                        message: "pipeline memory sink mutex poisoned".into(),
                    })?
                    .push(batch);
                Ok(())
            }
            Egress::Connector(sink) => {
                sink.write_batch_dyn(batch).await.map_err(|e| KrishivError::Runtime {
                    message: format!("pipeline sink write: {e}"),
                })
            }
        }
    }

    /// Flush the destination (no-op for memory sinks).
    pub(crate) async fn flush(&mut self) -> Result<()> {
        match self {
            Egress::Memory(_) => Ok(()),
            Egress::Connector(sink) => {
                sink.flush_dyn().await.map_err(|e| KrishivError::Runtime {
                    message: format!("pipeline sink flush: {e}"),
                })
            }
        }
    }
}

impl std::fmt::Debug for Egress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Egress::Memory(_) => write!(f, "Egress::Memory(..)"),
            Egress::Connector(_) => write!(f, "Egress::Connector(..)"),
        }
    }
}
