//! State-backed window wrappers (GAP-I2).

use arrow::record_batch::RecordBatch;
use krishiv_state::{Namespace, StateBackend, StateResult};

use super::session::{SessionWindowOperator, SessionWindowSpec};
use super::sliding::{SlidingWindowOperator, SlidingWindowSpec};
use super::tumbling::{TumblingWindowOperator, TumblingWindowSpec};
use crate::{ExecError, ExecResult};

macro_rules! state_backed_window_op {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            inner: $inner_ty:ty,
            spec: $spec_ty:ty,
            flush_method: $flush_method:ident,
            count_method: $count_method:ident,
            init: $init_fn:ident,
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            inner: $inner_ty,
            state: Box<dyn StateBackend>,
            namespace: Namespace,
        }

        impl $name {
            pub fn new(
                spec: $spec_ty,
                state: Box<dyn StateBackend>,
                operator_id: impl Into<String>,
                state_name: impl Into<String>,
            ) -> StateResult<Self> {
                let namespace = Namespace::new(operator_id, state_name);
                let inner = $init_fn(spec, state.as_ref(), &namespace)?;
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
                self.inner.process_batch(batch, new_watermark_ms)
            }

            pub fn $count_method(&self) -> usize {
                self.inner.$count_method()
            }

            pub fn $flush_method(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
                self.inner.$flush_method(watermark_ms)
            }

            pub fn checkpoint(&mut self) -> ExecResult<()> {
                self.inner
                    .persist_to_state(self.state.as_mut(), &self.namespace)
                    .map_err(|e| ExecError::Arrow(e.to_string()))?;
                // Force the WAL to disk once per checkpoint epoch. On the hot
                // path the durable backend is opened with `durable_fsync = false`
                // (writes buffered in the WAL), so this single flush is what
                // makes the epoch crash-durable — collapsing what was a
                // per-write fsync into one fsync per checkpoint.
                self.state
                    .sync()
                    .map_err(|e| ExecError::Arrow(e.to_string()))
            }

            pub fn purge_expired(&mut self) -> ExecResult<usize> {
                self.state
                    .purge_expired()
                    .map_err(|e| ExecError::Arrow(e.to_string()))
            }

            pub fn set_watermark(&mut self, watermark_ms: i64) {
                self.state.set_watermark(watermark_ms);
            }

            /// C9: Serialize window state to bytes for cross-session persistence.
            /// The bytes can be stored externally and later loaded via `load_snapshot`.
            pub fn snapshot_state_bytes(&self) -> StateResult<Vec<u8>> {
                self.state.snapshot()
            }

            /// C9: Restore window state from bytes previously returned by
            /// `snapshot_state_bytes`. Called before first `process_batch` on a new executor.
            pub fn load_snapshot_bytes(&mut self, bytes: &[u8]) -> StateResult<()> {
                // Load bytes into the state backend, then refresh the in-memory
                // operator state from it using restore_from_state.
                self.state.load_snapshot(bytes)?;
                self.inner.restore_from_state(self.state.as_ref(), &self.namespace)?;
                Ok(())
            }

            /// Merge a snapshot additively into the current window state.
            ///
            /// Unlike [`load_snapshot_bytes`] (replace-all), existing entries
            /// not present in `bytes` are preserved.  Used when one process
            /// hosts several tasks of a job and must union their restored
            /// per-task snapshots.
            pub fn merge_snapshot_bytes(&mut self, bytes: &[u8]) -> StateResult<()> {
                let entries = krishiv_state::decode_snapshot_entries(bytes)?;
                if !entries.is_empty() {
                    let batch: Vec<(&str, &str, &[u8], &[u8])> = entries
                        .iter()
                        .map(|(op, name, key, value)| {
                            (op.as_str(), name.as_str(), key.as_slice(), value.as_slice())
                        })
                        .collect();
                    self.state.put_batch(&batch)?;
                }
                self.inner.restore_from_state(self.state.as_ref(), &self.namespace)?;
                Ok(())
            }
        }
    };
}

fn init_tumbling(
    spec: TumblingWindowSpec,
    state: &dyn StateBackend,
    ns: &Namespace,
) -> StateResult<TumblingWindowOperator> {
    let mut inner = TumblingWindowOperator::new(spec);
    inner.restore_from_state(state, ns)?;
    Ok(inner)
}

fn init_sliding(
    spec: SlidingWindowSpec,
    state: &dyn StateBackend,
    ns: &Namespace,
) -> StateResult<SlidingWindowOperator> {
    let mut inner =
        SlidingWindowOperator::new(spec).map_err(|e| krishiv_state::StateError::CorruptEntry {
            message: e.to_string(),
        })?;
    inner.restore_from_state(state, ns)?;
    Ok(inner)
}

fn init_session(
    spec: SessionWindowSpec,
    state: &dyn StateBackend,
    ns: &Namespace,
) -> StateResult<SessionWindowOperator> {
    let mut inner = SessionWindowOperator::new(spec);
    inner.restore_from_state(state, ns)?;
    Ok(inner)
}

state_backed_window_op! {
    pub struct StateBackedTumblingWindowOperator {
        inner: TumblingWindowOperator,
        spec: TumblingWindowSpec,
        flush_method: flush_closed_windows,
        count_method: open_window_count,
        init: init_tumbling,
    }
}

state_backed_window_op! {
    pub struct StateBackedSlidingWindowOperator {
        inner: SlidingWindowOperator,
        spec: SlidingWindowSpec,
        flush_method: flush_closed_windows,
        count_method: open_window_count,
        init: init_sliding,
    }
}

state_backed_window_op! {
    pub struct StateBackedSessionWindowOperator {
        inner: SessionWindowOperator,
        spec: SessionWindowSpec,
        flush_method: flush_closed_sessions,
        count_method: open_session_count,
        init: init_session,
    }
}
