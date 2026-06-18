#![forbid(unsafe_code)]

//! IVM job registry for the coordinator.
//!
//! Each IVM job is a long-lived `IncrementalFlow` instance held in-process.
//! For single-node mode the coordinator runs `step_datafusion()` inline.
//! For distributed mode, remote executors feed deltas via the HTTP API and
//! the coordinator still runs SQL computation centrally.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use krishiv_ivm::{IncrementalFlow, IvmError};

/// Registry of IVM jobs hosted on this coordinator process.
#[derive(Debug, Default)]
pub struct IvmJobRegistry {
    jobs: Mutex<HashMap<String, Arc<IncrementalFlow>>>,
}

impl IvmJobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new IVM job. Idempotent: returns `Ok` if the job already exists.
    pub fn create(&self, job_id: String) -> Result<(), IvmError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| IvmError::execution("registry lock poisoned"))?;
        jobs.entry(job_id)
            .or_insert_with(|| Arc::new(IncrementalFlow::new()));
        Ok(())
    }

    /// Look up a job. Returns `None` if not found.
    pub fn get(&self, job_id: &str) -> Option<Arc<IncrementalFlow>> {
        self.jobs.lock().ok()?.get(job_id).cloned()
    }

    /// Delete a job. Returns `true` if the job existed.
    pub fn delete(&self, job_id: &str) -> bool {
        self.jobs
            .lock()
            .map(|mut j| j.remove(job_id).is_some())
            .unwrap_or(false)
    }

    /// List all job IDs.
    pub fn job_ids(&self) -> Vec<String> {
        self.jobs
            .lock()
            .map(|j| j.keys().cloned().collect())
            .unwrap_or_default()
    }
}

/// Shared, reference-counted handle to the IVM job registry.
pub type SharedIvmJobRegistry = Arc<IvmJobRegistry>;
