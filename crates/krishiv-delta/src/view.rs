#![forbid(unsafe_code)]

//! `IncrementalView` and `IncrementalViewRegistry`.
//!
//! An `IncrementalView` holds the operator pipeline for one SQL incremental
//! view, its current pending output `DeltaBatch`, and its registered sinks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tokio::sync::watch;

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};
use crate::lateness::LatenessSpec;
use crate::operators::stream::differentiate;

/// Specification of one incremental view as registered from SQL DDL.
#[derive(Debug, Clone)]
pub struct IncrementalViewSpec {
    pub name: String,
    pub body_sql: String,
    pub output_schema: SchemaRef,
    pub is_materialized: bool,
    pub is_recursive: bool,
    pub lateness: Vec<LatenessSpec>,
}

/// Runtime state for one incremental view.
pub struct IncrementalView {
    pub spec: IncrementalViewSpec,
    /// Latest output DeltaBatch from the last `step()`. None if never stepped.
    last_output: Arc<Mutex<Option<DeltaBatch>>>,
    /// Watch channel so `view_output_stream` can receive each new output.
    sender: watch::Sender<Option<DeltaBatch>>,
    /// Snapshot accumulation for materialized views.
    snapshot: Arc<Mutex<Option<RecordBatch>>>,
    /// Previous full materialized output used for diff-based IVM.
    /// `differentiate(full_output_prev, new_full)` produces the true delta.
    full_output: Arc<Mutex<Option<RecordBatch>>>,
}

impl IncrementalView {
    pub fn new(spec: IncrementalViewSpec) -> (Self, watch::Receiver<Option<DeltaBatch>>) {
        let (sender, receiver) = watch::channel(None);
        let view = Self {
            spec,
            last_output: Arc::new(Mutex::new(None)),
            sender,
            snapshot: Arc::new(Mutex::new(None)),
            full_output: Arc::new(Mutex::new(None)),
        };
        (view, receiver)
    }

    /// Publish a new output delta (called by the step engine).
    pub fn publish_output(&self, output: DeltaBatch) -> DeltaResult<()> {
        {
            let mut guard = self
                .last_output
                .lock()
                .map_err(|_| DeltaError::Operator("view output lock poisoned".into()))?;
            *guard = Some(output.clone());
        }
        // Update materialized snapshot for materialized views.
        // Apply the delta to the prior full snapshot — don't replace it with
        // just the delta's positive rows (that would lose prior state).
        if self.spec.is_materialized {
            let updated = {
                let mut snap = self
                    .snapshot
                    .lock()
                    .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
                let current = snap.take();
                match crate::operators::stream::apply_delta(current, &output) {
                    Ok(rb) => rb,
                    Err(e) => {
                        tracing::warn!(
                            view = %self.spec.name,
                            error = %e,
                            output_rows = output.num_rows(),
                            "apply_delta failed in publish_output — snapshot not updated"
                        );
                        return Err(e);
                    }
                }
            };
            tracing::debug!(
                view = %self.spec.name,
                rows = updated.num_rows(),
                "snapshot updated"
            );
            {
                let mut snap = self
                    .snapshot
                    .lock()
                    .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
                *snap = Some(updated.clone());
            }
            // Keep the diff baseline in lockstep with the materialized snapshot.
            // The incremental path (O(Δ) operators) never calls `diff_and_update`,
            // so without this `full_output` would stay `None` and a later
            // DiffBased step (e.g. on a remote executor restored from a
            // checkpoint) would treat the entire output as new insertions.
            let mut fo = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            *fo = Some(updated);
        } else {
            tracing::debug!(
                view = %self.spec.name,
                "publish_output: not materialized, snapshot skipped"
            );
        }
        let _ = self.sender.send(Some(output));
        Ok(())
    }

    /// Return the last output, or an empty batch.
    pub fn last_output(&self) -> DeltaResult<Option<DeltaBatch>> {
        self.last_output
            .lock()
            .map(|g| g.clone())
            .map_err(|_| DeltaError::Operator("view lock poisoned".into()))
    }

    /// Return the current materialized snapshot (only for materialized views).
    pub fn snapshot(&self) -> DeltaResult<Option<arrow::array::RecordBatch>> {
        self.snapshot
            .lock()
            .map(|g| g.clone())
            .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))
    }

    /// Return the previous full output used as the diff-based IVM baseline.
    ///
    /// Exposed so a coordinator-authoritative checkpoint can capture view
    /// baselines and ship them to a stateless executor.
    pub fn full_output_baseline(&self) -> DeltaResult<Option<arrow::array::RecordBatch>> {
        self.full_output
            .lock()
            .map(|g| g.clone())
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<DeltaBatch>> {
        self.sender.subscribe()
    }

    /// Compute the delta between the previous full output and `new_full`, store
    /// `new_full` as the new baseline, and return the delta.
    ///
    /// Used by `step_datafusion`: the caller runs the view SQL to get a fresh
    /// full result, then calls this to obtain the true incremental delta.
    pub fn diff_and_update(&self, new_full: RecordBatch) -> DeltaResult<DeltaBatch> {
        let mut guard = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        let delta = differentiate(&self.spec.output_schema, guard.as_ref(), &new_full)?;
        *guard = Some(new_full);
        Ok(delta)
    }

    /// Clear the stored full output so the next `diff_and_update` call treats
    /// all rows as new insertions. Call this when `body_sql` changes
    /// (behavior_version invalidation).
    pub fn reset_full_output(&self) -> DeltaResult<()> {
        let mut guard = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        *guard = None;
        Ok(())
    }

    /// Replace the view's full materialized state with `new_full`.
    ///
    /// Used by coordinator-authoritative IVM to apply a tick computed on a
    /// remote executor: the executor returns the full output, and the
    /// coordinator swaps it in wholesale. This recomputes the output delta
    /// from the prior `full_output` baseline, then updates both `full_output`
    /// and (for materialized views) the `snapshot`, and emits the delta to
    /// subscribers. Replacing — rather than applying a delta — keeps the
    /// baseline and the snapshot in lockstep, so a later central
    /// `diff_and_update` cannot drift.
    pub fn replace_full(&self, new_full: RecordBatch) -> DeltaResult<DeltaBatch> {
        // Diff against the prior baseline and advance it under one lock.
        let delta = {
            let mut guard = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            let delta = differentiate(&self.spec.output_schema, guard.as_ref(), &new_full)?;
            *guard = Some(new_full.clone());
            delta
        };
        if self.spec.is_materialized {
            let mut snap = self
                .snapshot
                .lock()
                .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
            *snap = Some(new_full);
        }
        {
            let mut lo = self
                .last_output
                .lock()
                .map_err(|_| DeltaError::Operator("view output lock poisoned".into()))?;
            *lo = Some(delta.clone());
        }
        let _ = self.sender.send(Some(delta.clone()));
        Ok(delta)
    }

    /// Apply an **output delta** to the view's materialized state (AUD-6).
    ///
    /// Used by the coordinator to mirror a tick computed on a resident
    /// executor: both the diff baseline (`full_output`) and, for materialized
    /// views, the `snapshot` advance by the delta, keeping them in lockstep
    /// with the executor's flow without ever shipping the full output. The
    /// delta is also published to subscribers and stored as `last_output`.
    pub fn apply_output_delta(&self, delta: &DeltaBatch) -> DeltaResult<()> {
        {
            let mut fo = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            let updated = crate::operators::stream::apply_delta(fo.take(), delta)?;
            *fo = Some(updated);
        }
        if self.spec.is_materialized {
            let mut snap = self
                .snapshot
                .lock()
                .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
            let updated = crate::operators::stream::apply_delta(snap.take(), delta)?;
            *snap = Some(updated);
        }
        {
            let mut lo = self
                .last_output
                .lock()
                .map_err(|_| DeltaError::Operator("view output lock poisoned".into()))?;
            *lo = Some(delta.clone());
        }
        let _ = self.sender.send(Some(delta.clone()));
        Ok(())
    }

    /// Restore previously checkpointed view state (`snapshot` + `full_output`).
    ///
    /// Used to seed a transient flow on a remote executor so its single tick
    /// computes correct output deltas. Both fields are optional; `None` resets
    /// that field to its never-stepped state.
    pub fn restore_state(
        &self,
        snapshot: Option<RecordBatch>,
        full_output: Option<RecordBatch>,
    ) -> DeltaResult<()> {
        {
            let mut snap = self
                .snapshot
                .lock()
                .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
            *snap = snapshot;
        }
        let mut fo = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        *fo = full_output;
        Ok(())
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Registry of all incremental views for a session/flow.
pub struct IncrementalViewRegistry {
    views: Mutex<HashMap<String, Arc<IncrementalView>>>,
    receivers: Mutex<HashMap<String, watch::Receiver<Option<DeltaBatch>>>>,
}

impl IncrementalViewRegistry {
    pub fn new() -> Self {
        Self {
            views: Mutex::new(HashMap::new()),
            receivers: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, spec: IncrementalViewSpec) -> DeltaResult<()> {
        let name = spec.name.clone();
        let (view, receiver) = IncrementalView::new(spec);
        {
            let mut views = self
                .views
                .lock()
                .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
            views.insert(name.clone(), Arc::new(view));
        }
        {
            let mut receivers = self
                .receivers
                .lock()
                .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
            receivers.insert(name, receiver);
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> DeltaResult<Arc<IncrementalView>> {
        let views = self
            .views
            .lock()
            .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
        views
            .get(name)
            .cloned()
            .ok_or_else(|| DeltaError::ViewNotFound(name.to_string()))
    }

    pub fn view_names(&self) -> DeltaResult<Vec<String>> {
        let views = self
            .views
            .lock()
            .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
        Ok(views.keys().cloned().collect())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.views
            .lock()
            .map(|v| v.contains_key(name))
            .unwrap_or(false)
    }

    pub fn drop_view(&self, name: &str) -> DeltaResult<bool> {
        let mut views = self
            .views
            .lock()
            .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
        Ok(views.remove(name).is_some())
    }
}

impl Default for IncrementalViewRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_spec(name: &str) -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: name.to_string(),
            body_sql: format!("SELECT 1 AS x -- {name}"),
            output_schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        }
    }

    #[test]
    fn registry_register_and_get() {
        let reg = IncrementalViewRegistry::new();
        reg.register(test_spec("v1")).unwrap();
        let v = reg.get("v1").unwrap();
        assert_eq!(v.spec.name, "v1");
    }

    #[test]
    fn registry_get_missing_returns_error() {
        let reg = IncrementalViewRegistry::new();
        assert!(matches!(
            reg.get("missing"),
            Err(DeltaError::ViewNotFound(_))
        ));
    }

    #[test]
    fn registry_drop_view() {
        let reg = IncrementalViewRegistry::new();
        reg.register(test_spec("v1")).unwrap();
        assert!(reg.drop_view("v1").unwrap());
        assert!(!reg.contains("v1"));
    }
}
