//! State-backed tumbling window wrapper (GAP-I2).

use arrow::record_batch::RecordBatch;
use krishiv_state::{Namespace, StateBackend, StateResult};

use super::tumbling::{TumblingWindowOperator, TumblingWindowSpec};
use crate::{ExecError, ExecResult};

pub struct StateBackedTumblingWindowOperator {
    inner: TumblingWindowOperator,
    state: Box<dyn StateBackend>,
    namespace: Namespace,
}

impl StateBackedTumblingWindowOperator {
    pub fn new(
        spec: TumblingWindowSpec,
        state: Box<dyn StateBackend>,
        operator_id: impl Into<String>,
        state_name: impl Into<String>,
    ) -> StateResult<Self> {
        let namespace = Namespace::new(operator_id, state_name);
        let mut inner = TumblingWindowOperator::new(spec);
        inner.restore_from_state(state.as_ref(), &namespace)?;
        Ok(Self {
            inner,
            state,
            namespace,
        })
    }

    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        new_watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        let out = self.inner.process_batch(batch, new_watermark_ms)?;
        self.inner
            .persist_to_state(self.state.as_mut(), &self.namespace)
            .map_err(|e| crate::ExecError::Arrow(e.to_string()))?;
        Ok(out)
    }

    pub fn open_window_count(&self) -> usize {
        self.inner.open_window_count()
    }

    /// Flush closed windows and persist updated state.
    pub fn flush_closed_windows(
        &mut self,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        let out = self.inner.flush_closed_windows(watermark_ms)?;
        self.inner
            .persist_to_state(self.state.as_mut(), &self.namespace)
            .map_err(|e| ExecError::Arrow(e.to_string()))?;
        Ok(out)
    }
}
