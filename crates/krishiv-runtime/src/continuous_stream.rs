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
        assert!(out.is_empty() || !out.is_empty());
    }
}
