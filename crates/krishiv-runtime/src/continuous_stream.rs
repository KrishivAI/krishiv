//! Shared continuous streaming job state between session and in-process executor.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
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

/// Default maximum pending input batches per job before backpressure kicks in.
const DEFAULT_MAX_PENDING_BATCHES: usize = 1024;

/// Registry for long-running streaming jobs (session-scoped).
#[derive(Debug)]
pub struct ContinuousStreamRegistry {
    jobs: DashMap<String, Arc<Mutex<ContinuousJobEntry>>>,
    /// Maximum number of pending (not yet drained) input batches per job.
    /// `push_input` returns [`RuntimeError::InvalidState`] when this limit is
    /// reached so callers can apply backpressure instead of silently filling
    /// memory. Set to `usize::MAX` for unbounded behaviour (tests only).
    max_pending_batches: usize,
}

impl Default for ContinuousStreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ContinuousStreamRegistry {
    pub fn new() -> Self {
        Self {
            jobs: DashMap::new(),
            max_pending_batches: DEFAULT_MAX_PENDING_BATCHES,
        }
    }

    /// Create a registry with a custom per-job input queue depth limit.
    pub fn with_max_pending_batches(max: usize) -> Self {
        Self {
            jobs: DashMap::new(),
            max_pending_batches: max,
        }
    }

    /// Create a registry with no backpressure limit. Use only in tests.
    pub fn new_unbounded() -> Self {
        Self::with_max_pending_batches(usize::MAX)
    }

    /// Register a continuous job with its window spec.
    pub fn register_job(
        &self,
        job_id: impl Into<String>,
        spec: WindowExecutionSpec,
    ) -> RuntimeResult<()> {
        let executor = ContinuousWindowExecutor::new(spec.clone())
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        self.jobs.insert(
            job_id.into(),
            Arc::new(Mutex::new(ContinuousJobEntry {
                spec,
                executor,
                pending_input: VecDeque::new(),
                pending_output: VecDeque::new(),
            })),
        );
        Ok(())
    }

    /// Enqueue input batches for a continuous job.
    ///
    /// Returns [`RuntimeError::InvalidState`] when the job's input queue has
    /// reached `max_pending_batches`. Callers should drain the job before
    /// pushing more data. This prevents unbounded memory accumulation when
    /// producers outrun consumers.
    pub fn push_input(&self, job_id: &str, batches: Vec<RecordBatch>) -> RuntimeResult<()> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })?;
        let mut guard = entry
            .lock()
            .map_err(|_| RuntimeError::transport("continuous entry lock poisoned"))?;
        let current_depth = guard.pending_input.len();
        if current_depth >= self.max_pending_batches {
            return Err(RuntimeError::InvalidState {
                message: format!(
                    "continuous job '{job_id}' input queue is full ({current_depth} batches \
                     pending, limit {limit}); call drain_job before pushing more data",
                    limit = self.max_pending_batches,
                ),
            });
        }
        guard.pending_input.extend(batches);
        Ok(())
    }

    /// Returns the number of pending input batches for a job.
    /// Callers can use this to implement self-rate-limiting before calling `push_input`.
    pub fn pending_batch_depth(&self, job_id: &str) -> RuntimeResult<usize> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })?;
        let guard = entry
            .lock()
            .map_err(|_| RuntimeError::transport("continuous entry lock poisoned"))?;
        Ok(guard.pending_input.len())
    }

    /// Drain pending input through the window operator and return newly emitted batches.
    pub fn drain_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })?;
        let mut guard = entry
            .lock()
            .map_err(|_| RuntimeError::transport("continuous entry lock poisoned"))?;
        let input: Vec<RecordBatch> = guard.pending_input.drain(..).collect();
        if input.is_empty() {
            let output: Vec<RecordBatch> = guard.pending_output.drain(..).collect();
            return Ok(output);
        }
        let emitted = guard
            .executor
            .drain(input)
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        guard.pending_output.extend(emitted.iter().cloned());
        let output: Vec<RecordBatch> = guard.pending_output.drain(..).collect();
        Ok(output)
    }

    /// Borrow the window spec for coordinator fragment encoding.
    pub fn job_spec(&self, job_id: &str) -> RuntimeResult<WindowExecutionSpec> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("unknown continuous stream job '{job_id}'"),
            })?;
        let guard = entry
            .lock()
            .map_err(|_| RuntimeError::transport("continuous entry lock poisoned"))?;
        Ok(guard.spec.clone())
    }

    /// List all registered job ids.
    pub fn list_jobs(&self) -> Vec<String> {
        self.jobs.iter().map(|entry| entry.key().clone()).collect()
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
    fn push_input_returns_error_when_queue_full() {
        // Create a registry with a tiny queue limit of 2 batches per job.
        let registry = ContinuousStreamRegistry::with_max_pending_batches(2);
        registry
            .register_job("j-bp", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();

        // First two pushes succeed.
        registry.push_input("j-bp", vec![batch(1_000)]).unwrap();
        registry.push_input("j-bp", vec![batch(2_000)]).unwrap();

        // Third push must fail: queue is at capacity.
        let err = registry.push_input("j-bp", vec![batch(3_000)]).unwrap_err();
        assert!(
            matches!(err, RuntimeError::InvalidState { .. }),
            "expected InvalidState backpressure error, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("input queue is full"),
            "error message must mention queue full: {msg}"
        );
    }

    #[test]
    fn push_input_after_drain_succeeds() {
        // Verify that draining frees space so subsequent pushes succeed.
        let registry = ContinuousStreamRegistry::with_max_pending_batches(1);
        registry
            .register_job("j-drain-bp", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();

        registry.push_input("j-drain-bp", vec![batch(1_000)]).unwrap();
        // Queue is full — drain first.
        let _ = registry.drain_job("j-drain-bp").unwrap();
        // After drain, pushing again must succeed.
        registry.push_input("j-drain-bp", vec![batch(2_000)]).unwrap();
    }

    #[test]
    fn new_unbounded_has_no_backpressure() {
        let registry = ContinuousStreamRegistry::new_unbounded();
        registry
            .register_job("j-unb", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();
        for i in 0..2000 {
            registry
                .push_input("j-unb", vec![batch(i as i64 * 1_000)])
                .expect("unbounded registry must never return backpressure error");
        }
    }

    #[test]
    fn pending_batch_depth_tracks_pushes_and_drains() {
        let registry = ContinuousStreamRegistry::new_unbounded();
        registry
            .register_job("j-depth", WindowExecutionSpec::tumbling("user_id", "ts", 10_000))
            .unwrap();
        assert_eq!(registry.pending_batch_depth("j-depth").unwrap(), 0);
        registry.push_input("j-depth", vec![batch(1_000), batch(2_000)]).unwrap();
        assert_eq!(registry.pending_batch_depth("j-depth").unwrap(), 2);
        let _ = registry.drain_job("j-depth").unwrap();
        assert_eq!(registry.pending_batch_depth("j-depth").unwrap(), 0);
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
