//! Shared continuous streaming job state between session and in-process executor.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_exec::ContinuousWindowExecutor;
use krishiv_plan::window::WindowExecutionSpec;

use crate::{RuntimeError, RuntimeResult};

/// One continuous streaming job registered on the session cluster.
#[derive(Debug)]
struct ContinuousJobEntry {
    spec: WindowExecutionSpec,
    executor: ContinuousWindowExecutor,
    pending_input: VecDeque<RecordBatch>,
    pending_output: VecDeque<RecordBatch>,
}

/// Registry for long-running streaming jobs (session-scoped).
#[derive(Debug, Default)]
pub struct ContinuousStreamRegistry {
    jobs: Mutex<HashMap<String, ContinuousJobEntry>>,
}

impl ContinuousStreamRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a continuous job with its window spec.
    pub fn register_job(
        &self,
        job_id: impl Into<String>,
        spec: WindowExecutionSpec,
    ) -> RuntimeResult<()> {
        let executor = ContinuousWindowExecutor::new(spec.clone())
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| RuntimeError::transport("continuous registry lock poisoned"))?;
        jobs.insert(
            job_id.into(),
            ContinuousJobEntry {
                spec,
                executor,
                pending_input: VecDeque::new(),
                pending_output: VecDeque::new(),
            },
        );
        Ok(())
    }

    /// Enqueue input batches for a continuous job.
    pub fn push_input(&self, job_id: &str, batches: Vec<RecordBatch>) -> RuntimeResult<()> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| RuntimeError::transport("continuous registry lock poisoned"))?;
        let entry = jobs
            .get_mut(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })?;
        entry.pending_input.extend(batches);
        Ok(())
    }

    /// Drain pending input through the window operator and return newly emitted batches.
    pub fn drain_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| RuntimeError::transport("continuous registry lock poisoned"))?;
        let entry = jobs
            .get_mut(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })?;
        let input: Vec<RecordBatch> = entry.pending_input.drain(..).collect();
        if input.is_empty() {
            let output: Vec<RecordBatch> = entry.pending_output.drain(..).collect();
            return Ok(output);
        }
        let emitted = entry
            .executor
            .drain(input)
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        entry.pending_output.extend(emitted.iter().cloned());
        let output: Vec<RecordBatch> = entry.pending_output.drain(..).collect();
        Ok(output)
    }

    /// Borrow the window spec for coordinator fragment encoding.
    pub fn job_spec(&self, job_id: &str) -> RuntimeResult<WindowExecutionSpec> {
        let jobs = self
            .jobs
            .lock()
            .map_err(|_| RuntimeError::transport("continuous registry lock poisoned"))?;
        jobs.get(job_id)
            .map(|entry| entry.spec.clone())
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })
    }

    /// List all registered job ids.
    pub fn list_jobs(&self) -> Vec<String> {
        let jobs = self.jobs.lock().expect("continuous registry lock poisoned");
        jobs.keys().cloned().collect()
    }
}

/// Shared registry handle for session + executor wiring.
pub type SharedContinuousStreamRegistry = Arc<ContinuousStreamRegistry>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_plan::window::WindowExecutionSpec;

    use super::*;

    fn batch(ts: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn continuous_registry_drains_input() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job(
                "job-1",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .expect("register");
        registry
            .push_input("job-1", vec![batch(1_000)])
            .expect("push");
        let out = registry.drain_job("job-1").expect("drain");
        assert!(
            out.is_empty(),
            "no window boundary reached yet, expected empty output"
        );
    }

    #[test]
    fn register_multiple_jobs_and_list() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 5_000))
            .unwrap();
        registry
            .register_job("j2", WindowExecutionSpec::tumbling("k", "ts", 10_000))
            .unwrap();
        let mut jobs = registry.list_jobs();
        jobs.sort();
        assert_eq!(jobs, vec!["j1", "j2"]);
    }

    #[test]
    fn push_input_unknown_job_fails() {
        let registry = ContinuousStreamRegistry::new();
        let err = registry.push_input("no-such-job", vec![]).unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidState { .. }));
    }

    #[test]
    fn drain_unknown_job_fails() {
        let registry = ContinuousStreamRegistry::new();
        let err = registry.drain_job("no-such-job").unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidState { .. }));
    }

    #[test]
    fn drain_empty_input_returns_empty() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 10_000))
            .unwrap();
        let out = registry.drain_job("j1").unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn job_spec_returns_spec_for_registered_job() {
        let registry = ContinuousStreamRegistry::new();
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        registry.register_job("j1", spec.clone()).unwrap();
        let got = registry.job_spec("j1").unwrap();
        assert_eq!(got, spec);
    }

    #[test]
    fn job_spec_unknown_job_fails() {
        let registry = ContinuousStreamRegistry::new();
        let err = registry.job_spec("no-such-job").unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidState { .. }));
    }

    #[test]
    fn drain_across_window_boundary_emits_batch() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();
        registry.push_input("j1", vec![batch(1_000)]).unwrap();
        registry.push_input("j1", vec![batch(11_000)]).unwrap();
        let out = registry.drain_job("j1").unwrap();
        assert!(
            !out.is_empty(),
            "crossing a window boundary should emit output"
        );
    }

    #[test]
    fn register_replaces_existing_job() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 5_000))
            .unwrap();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 20_000))
            .unwrap();
        let spec = registry.job_spec("j1").unwrap();
        assert_eq!(spec.window_size_ms, 20_000);
    }

    #[test]
    fn drain_after_register_no_input() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 10_000))
            .unwrap();
        let out = registry.drain_job("j1").unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn multiple_sequential_drains() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();
        registry.push_input("j1", vec![batch(1_000)]).unwrap();
        let _ = registry.drain_job("j1").unwrap();
        registry.push_input("j1", vec![batch(11_000)]).unwrap();
        let out = registry.drain_job("j1").unwrap();
        assert!(!out.is_empty());
    }

    #[test]
    fn list_jobs_empty() {
        let registry = ContinuousStreamRegistry::new();
        assert!(registry.list_jobs().is_empty());
    }

    #[test]
    fn list_jobs_after_register() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("a", WindowExecutionSpec::tumbling("k", "ts", 1_000))
            .unwrap();
        registry
            .register_job("b", WindowExecutionSpec::tumbling("k", "ts", 2_000))
            .unwrap();
        let mut jobs = registry.list_jobs();
        jobs.sort();
        assert_eq!(jobs, vec!["a", "b"]);
    }

    #[test]
    fn push_multiple_batches_then_drain() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();
        registry
            .push_input("j1", vec![batch(1_000), batch(2_000)])
            .unwrap();
        let out = registry.drain_job("j1").unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn drain_pending_output_accumulates() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();
        registry.push_input("j1", vec![batch(1_000)]).unwrap();
        let _ = registry.drain_job("j1").unwrap();
        registry.push_input("j1", vec![batch(11_000)]).unwrap();
        let out1 = registry.drain_job("j1").unwrap();
        assert!(!out1.is_empty());
    }
}
