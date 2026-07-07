//! Shared continuous streaming job state between session and in-process executor.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_plan::window::WindowExecutionSpec;

use crate::RuntimeResult;

/// Typed failures for continuous job registration, input, and execution.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContinuousStreamError {
    #[error("continuous job id must not be empty")]
    InvalidJobId,
    #[error("continuous job '{job_id}' is already registered")]
    JobAlreadyExists { job_id: String },
    #[error("continuous job '{job_id}' was not found")]
    JobNotFound { job_id: String },
    #[error(
        "continuous job '{job_id}' input queue is full: current={current}, \
         attempted={attempted}, limit={limit}"
    )]
    QueueFull {
        job_id: String,
        current: usize,
        attempted: usize,
        limit: usize,
    },
    #[error(
        "continuous job '{job_id}' input schema mismatch: expected {expected}, actual {actual}"
    )]
    SchemaMismatch {
        job_id: String,
        expected: String,
        actual: String,
    },
    #[error("continuous job '{job_id}' {component} lock poisoned during {operation}")]
    LockPoisoned {
        job_id: String,
        component: &'static str,
        operation: &'static str,
    },
    #[error("continuous job '{job_id}' execution failed: {message}")]
    Execution { job_id: String, message: String },
}

#[derive(Debug, Default)]
struct ContinuousInputState {
    batches: VecDeque<RecordBatch>,
    schema: Option<SchemaRef>,
}

/// One continuous streaming job registered on the session cluster.
///
/// Input and executor are guarded by independent `Mutex`es so that
/// `push_input` / `pending_batch_depth` only contend with each other and
/// never block while `drain_job` is running the (potentially slow) window
/// computation.
struct ContinuousJobEntry {
    /// Immutable after registration; no lock required.
    spec: WindowExecutionSpec,
    /// Guarded separately so producers can push without waiting for drain.
    input: Mutex<ContinuousInputState>,
    /// Locked only during `drain_job` for the window computation itself.
    executor: Mutex<ContinuousWindowExecutor>,
}

impl std::fmt::Debug for ContinuousJobEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuousJobEntry")
            .field("spec", &self.spec)
            .field(
                "input_queue_len",
                &self
                    .input
                    .lock()
                    .map(|state| state.batches.len())
                    .unwrap_or(0),
            )
            .finish_non_exhaustive()
    }
}

/// Default maximum pending input batches per job before backpressure kicks in.
const DEFAULT_MAX_PENDING_BATCHES: usize = 1024;

fn schemas_structurally_equal(a: &arrow::datatypes::Schema, b: &arrow::datatypes::Schema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields().iter().zip(b.fields().iter()).all(|(af, bf)| {
            af.name() == bf.name()
                && af.data_type() == bf.data_type()
                && af.is_nullable() == bf.is_nullable()
        })
}

/// Registry for long-running streaming jobs (session-scoped).
#[derive(Debug)]
pub struct ContinuousStreamRegistry {
    jobs: DashMap<String, Arc<ContinuousJobEntry>>,
    /// Maximum number of pending (not yet drained) input batches per job.
    /// `push_input` returns [`ContinuousStreamError::QueueFull`] when this limit is
    /// reached so callers can apply backpressure instead of silently filling
    /// memory. Set to `usize::MAX` for unbounded behaviour (tests only).
    max_pending_batches: usize,
}

impl Default for ContinuousStreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn check_drain_output_size(output: &[RecordBatch]) -> RuntimeResult<()> {
    const MAX_DRAIN_OUTPUT_BYTES: usize = 2 * 1024 * 1024 * 1024;
    let total: usize = output.iter().map(|b| b.get_array_memory_size()).sum();
    if total > MAX_DRAIN_OUTPUT_BYTES {
        return Err(ContinuousStreamError::Execution {
            job_id: String::new(),
            message: format!(
                "drain output of {} bytes exceeds the {MAX_DRAIN_OUTPUT_BYTES}-byte limit; \
                 reduce drain batch count or split the job",
                total,
            ),
        }
        .into());
    }
    Ok(())
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
        let job_id = job_id.into();
        if job_id.trim().is_empty() {
            return Err(ContinuousStreamError::InvalidJobId.into());
        }
        let executor = ContinuousWindowExecutor::new(spec.clone()).map_err(|error| {
            ContinuousStreamError::Execution {
                job_id: job_id.clone(),
                message: error.to_string(),
            }
        })?;
        match self.jobs.entry(job_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(ContinuousStreamError::JobAlreadyExists { job_id }.into())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::new(ContinuousJobEntry {
                    spec,
                    input: Mutex::new(ContinuousInputState::default()),
                    executor: Mutex::new(executor),
                }));
                Ok(())
            }
        }
    }

    /// Maximum input batches consumed by a single [`drain_job_up_to`] call.
    ///
    /// Limits the amount of memory allocated by one drain cycle. Callers that
    /// need to drain more than this many batches should call `drain_job_up_to`
    /// in a loop until [`pending_batch_depth`] returns 0.
    pub const DEFAULT_MAX_DRAIN_BATCHES: usize = 256;

    /// Enqueue input batches for a continuous job.
    ///
    /// Returns [`ContinuousStreamError::QueueFull`] when the job's input queue has
    /// reached `max_pending_batches`. Callers should drain the job before
    /// pushing more data. This prevents unbounded memory accumulation when
    /// producers outrun consumers.
    pub fn push_input(&self, job_id: &str, batches: Vec<RecordBatch>) -> RuntimeResult<()> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let [first_batch, rest @ ..] = batches.as_slice() else {
            return Ok(());
        };
        let incoming_schema = first_batch.schema();
        for batch in rest {
            if batch.schema() != incoming_schema {
                return Err(ContinuousStreamError::SchemaMismatch {
                    job_id: job_id.to_owned(),
                    expected: format!("{incoming_schema:?}"),
                    actual: format!("{:?}", batch.schema()),
                }
                .into());
            }
        }

        let mut input = entry
            .input
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "input",
                operation: "push_input",
            })?;
        if let Some(expected_schema) = &input.schema
            && !schemas_structurally_equal(expected_schema, &incoming_schema)
        {
            return Err(ContinuousStreamError::SchemaMismatch {
                job_id: job_id.to_owned(),
                expected: format!("{expected_schema:?}"),
                actual: format!("{incoming_schema:?}"),
            }
            .into());
        }
        let current = input.batches.len();
        let attempted = batches.len();
        let requested =
            current
                .checked_add(attempted)
                .ok_or_else(|| ContinuousStreamError::QueueFull {
                    job_id: job_id.to_owned(),
                    current,
                    attempted,
                    limit: self.max_pending_batches,
                })?;
        if requested > self.max_pending_batches {
            return Err(ContinuousStreamError::QueueFull {
                job_id: job_id.to_owned(),
                current,
                attempted,
                limit: self.max_pending_batches,
            }
            .into());
        }
        if input.schema.is_none() {
            input.schema = Some(incoming_schema);
        }
        input.batches.extend(batches);
        Ok(())
    }

    /// Returns the number of pending input batches for a job.
    /// Callers can use this to implement self-rate-limiting before calling `push_input`.
    pub fn pending_batch_depth(&self, job_id: &str) -> RuntimeResult<usize> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let input = entry
            .input
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "input",
                operation: "pending_batch_depth",
            })?;
        Ok(input.batches.len())
    }

    /// Drain up to `max_input_batches` pending input batches through the window
    /// operator and return newly emitted output batches.
    ///
    /// Limiting the number of input batches consumed per call prevents unbounded
    /// memory spikes when the input queue is large (B6). Call in a loop until
    /// [`pending_batch_depth`] returns 0 to fully drain.
    pub fn drain_job_up_to(
        &self,
        job_id: &str,
        max_input_batches: usize,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;

        // Serialize drains before selecting input so concurrent callers cannot
        // process later batches ahead of earlier batches.
        let mut exec = entry
            .executor
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "executor",
                operation: "drain_job",
            })?;

        // Clone Arrow batch handles while holding the short input lock. Keep
        // the originals queued until the state transaction commits.
        let input: Vec<RecordBatch> = {
            let input = entry
                .input
                .lock()
                .map_err(|_| ContinuousStreamError::LockPoisoned {
                    job_id: job_id.to_owned(),
                    component: "input",
                    operation: "drain_job",
                })?;
            input
                .batches
                .iter()
                .take(max_input_batches)
                .cloned()
                .collect()
        };
        if input.is_empty() {
            return Ok(Vec::new());
        }
        let consumed = input.len();
        let output =
            exec.drain_transactional(input)
                .map_err(|error| ContinuousStreamError::Execution {
                    job_id: job_id.to_owned(),
                    message: error.to_string(),
                })?;

        // BATCH-2: Reject drain outputs that exceed the byte limit to prevent
        // OOM in the caller and Flight/HTTP transport overflows.
        check_drain_output_size(&output)?;

        let mut queued = entry
            .input
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "input",
                operation: "commit drain_job",
            })?;
        for _ in 0..consumed {
            let _ = queued.batches.pop_front();
        }
        Ok(output)
    }

    /// Drain up to [`DEFAULT_MAX_DRAIN_BATCHES`] batches through the operator.
    ///
    /// For draining all pending batches, loop until this returns empty results.
    pub fn drain_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        self.drain_job_up_to(job_id, Self::DEFAULT_MAX_DRAIN_BATCHES)
    }

    /// Borrow the window spec for coordinator fragment encoding.
    pub fn job_spec(&self, job_id: &str) -> RuntimeResult<WindowExecutionSpec> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        Ok(entry.spec.clone())
    }

    /// Returns `true` if a job with this id has been registered.
    pub fn has_job(&self, job_id: &str) -> bool {
        self.jobs.contains_key(job_id)
    }

    /// C9: Serialize a job's window state to bytes AND return the current watermark.
    ///
    /// Returns `(snapshot_bytes, last_watermark_ms)`. Use this when persisting to
    /// a `MetadataStore` so the watermark can be logged for diagnostics; the bytes
    /// alone are sufficient for `register_job_from_snapshot` restore.
    pub fn snapshot_job_with_watermark(&self, job_id: &str) -> RuntimeResult<(Vec<u8>, i64)> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let mut exec = entry
            .executor
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "executor",
                operation: "snapshot_with_watermark",
            })?;
        let watermark_ms = exec.last_watermark_ms();
        let bytes = exec
            .snapshot()
            .map_err(|error| ContinuousStreamError::Execution {
                job_id: job_id.to_owned(),
                message: format!("snapshot failed: {error}"),
            })?;
        Ok((bytes, watermark_ms))
    }

    /// C9: Serialize a job's window state to bytes for cross-session persistence.
    ///
    /// Calls `checkpoint()` on the executor (writes to the in-memory backend),
    /// then extracts the snapshot bytes. Store them externally (file, etcd, object
    /// store) and pass to `restore_job_from_snapshot` on the next session to resume
    /// from where the previous executor left off.
    pub fn snapshot_job(&self, job_id: &str) -> RuntimeResult<Vec<u8>> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let mut exec = entry
            .executor
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "executor",
                operation: "snapshot",
            })?;
        exec.snapshot().map_err(|error| {
            ContinuousStreamError::Execution {
                job_id: job_id.to_owned(),
                message: format!("snapshot failed: {error}"),
            }
            .into()
        })
    }

    /// C9: Register a job and immediately restore its window state from a prior
    /// snapshot.
    ///
    /// Use this instead of `register_job` when resuming a job on a new executor
    /// session. The window state (open partials, committed offsets) is restored
    /// from `snapshot_bytes` before the first `push_input`/`drain_job` call.
    pub fn register_job_from_snapshot(
        &self,
        job_id: impl Into<String>,
        spec: WindowExecutionSpec,
        snapshot_bytes: &[u8],
    ) -> RuntimeResult<()> {
        use krishiv_dataflow::ContinuousWindowExecutor;
        let job_id = job_id.into();
        if job_id.trim().is_empty() {
            return Err(ContinuousStreamError::InvalidJobId.into());
        }
        let mut executor = ContinuousWindowExecutor::new(spec.clone()).map_err(|error| {
            ContinuousStreamError::Execution {
                job_id: job_id.clone(),
                message: error.to_string(),
            }
        })?;
        // Restore window state from the snapshot before accepting new input.
        if !snapshot_bytes.is_empty() {
            executor
                .restore_from_snapshot(snapshot_bytes)
                .map_err(|error| ContinuousStreamError::Execution {
                    job_id: job_id.clone(),
                    message: format!("snapshot restore failed: {error}"),
                })?;
        }
        match self.jobs.entry(job_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(ContinuousStreamError::JobAlreadyExists { job_id }.into())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::new(ContinuousJobEntry {
                    spec,
                    input: Mutex::new(ContinuousInputState::default()),
                    executor: Mutex::new(executor),
                }));
                Ok(())
            }
        }
    }

    /// Replace a registered job's executor state with the supplied snapshot.
    ///
    /// Any queued input is cleared so the restored state becomes the next
    /// processing baseline.
    pub fn restore_job_snapshot(&self, job_id: &str, snapshot_bytes: &[u8]) -> RuntimeResult<()> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let mut exec = entry
            .executor
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "executor",
                operation: "restore_job_snapshot",
            })?;
        exec.restore_from_snapshot(snapshot_bytes)
            .map_err(|error| ContinuousStreamError::Execution {
                job_id: job_id.to_owned(),
                message: format!("snapshot restore failed: {error}"),
            })?;
        let mut input = entry
            .input
            .lock()
            .map_err(|_| ContinuousStreamError::LockPoisoned {
                job_id: job_id.to_owned(),
                component: "input",
                operation: "restore_job_snapshot",
            })?;
        input.batches.clear();
        input.schema = None;
        Ok(())
    }

    /// Remove a registered continuous job and its in-memory state.
    ///
    /// Returns `Ok(())` when the job was found and removed. Returns
    /// [`ContinuousStreamError::JobNotFound`] when no job with `job_id` exists,
    /// so callers can distinguish intentional deregistration from stale references.
    pub fn deregister_job(&self, job_id: &str) -> RuntimeResult<()> {
        if self.jobs.remove(job_id).is_none() {
            return Err(ContinuousStreamError::JobNotFound {
                job_id: job_id.to_owned(),
            }
            .into());
        }
        Ok(())
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
    use crate::RuntimeError;

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

    fn invalid_batch_without_event_time() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"])) as _]).unwrap()
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
        assert!(matches!(
            err,
            RuntimeError::ContinuousStream(ContinuousStreamError::JobNotFound { .. })
        ));
    }

    #[test]
    fn drain_unknown_job_fails() {
        let registry = ContinuousStreamRegistry::new();
        let err = registry.drain_job("no-such-job").unwrap_err();
        assert!(matches!(
            err,
            RuntimeError::ContinuousStream(ContinuousStreamError::JobNotFound { .. })
        ));
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
        assert!(matches!(
            err,
            RuntimeError::ContinuousStream(ContinuousStreamError::JobNotFound { .. })
        ));
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
    fn register_rejects_existing_job_without_resetting_state() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 5_000))
            .unwrap();
        let error = registry
            .register_job("j1", WindowExecutionSpec::tumbling("k", "ts", 20_000))
            .expect_err("duplicate registration must fail");
        assert!(matches!(
            error,
            RuntimeError::ContinuousStream(ContinuousStreamError::JobAlreadyExists { .. })
        ));
        let spec = registry.job_spec("j1").unwrap();
        assert_eq!(spec.window_size_ms, 5_000);
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
            .register_job(
                "j-bp",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();

        // First two pushes succeed.
        registry.push_input("j-bp", vec![batch(1_000)]).unwrap();
        registry.push_input("j-bp", vec![batch(2_000)]).unwrap();

        // Third push must fail: queue is at capacity.
        let err = registry.push_input("j-bp", vec![batch(3_000)]).unwrap_err();
        assert!(
            matches!(
                err,
                RuntimeError::ContinuousStream(ContinuousStreamError::QueueFull { .. })
            ),
            "expected typed backpressure error, got: {err:?}"
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
            .register_job(
                "j-drain-bp",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();

        registry
            .push_input("j-drain-bp", vec![batch(1_000)])
            .unwrap();
        // Queue is full — drain first.
        let _ = registry.drain_job("j-drain-bp").unwrap();
        // After drain, pushing again must succeed.
        registry
            .push_input("j-drain-bp", vec![batch(2_000)])
            .unwrap();
    }

    #[test]
    fn new_unbounded_has_no_backpressure() {
        let registry = ContinuousStreamRegistry::new_unbounded();
        registry
            .register_job(
                "j-unb",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
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
            .register_job(
                "j-depth",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();
        assert_eq!(registry.pending_batch_depth("j-depth").unwrap(), 0);
        registry
            .push_input("j-depth", vec![batch(1_000), batch(2_000)])
            .unwrap();
        assert_eq!(registry.pending_batch_depth("j-depth").unwrap(), 2);
        let _ = registry.drain_job("j-depth").unwrap();
        assert_eq!(registry.pending_batch_depth("j-depth").unwrap(), 0);
    }

    #[test]
    fn drain_job_up_to_respects_max_input_batches() {
        // 10-second tumbling window. First 9 batches are inside [0,10000),
        // batch at 11_000 crosses into the next window and triggers output.
        let registry = ContinuousStreamRegistry::new_unbounded();
        registry
            .register_job(
                "j-up-to",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();
        for i in 0_i64..9 {
            registry
                .push_input("j-up-to", vec![batch(i * 1_000 + 1_000)])
                .unwrap();
        }
        // 10th batch crosses the window boundary.
        registry.push_input("j-up-to", vec![batch(11_000)]).unwrap();
        assert_eq!(registry.pending_batch_depth("j-up-to").unwrap(), 10);
        // Drain at most 3 — the remaining 7 must still be pending; no window
        // closes yet because batch[11_000] is not consumed.
        let partial = registry.drain_job_up_to("j-up-to", 3).unwrap();
        assert_eq!(
            registry.pending_batch_depth("j-up-to").unwrap(),
            7,
            "drain_job_up_to(3) must leave 7 batches pending"
        );
        // No window boundary in first 3 batches (all ts < 10000).
        assert!(partial.is_empty(), "no window closed in first 3 batches");
        // Drain the rest — the window-boundary batch (11_000) is now consumed.
        let final_out = registry.drain_job_up_to("j-up-to", usize::MAX).unwrap();
        assert!(
            !final_out.is_empty(),
            "draining the window-boundary batch must emit output"
        );
        assert_eq!(registry.pending_batch_depth("j-up-to").unwrap(), 0);
    }

    #[test]
    fn drain_job_up_to_usize_max_drains_all_and_emits_output() {
        // Two batches that cross a 5-second window boundary; drain must consume
        // both and return the closed window's output rows.
        let registry = ContinuousStreamRegistry::new_unbounded();
        registry
            .register_job(
                "j-all",
                WindowExecutionSpec::tumbling("user_id", "ts", 5_000),
            )
            .unwrap();
        // batch at 1_000 → inside [0, 5000); batch at 6_000 → closes it.
        registry
            .push_input("j-all", vec![batch(1_000), batch(6_000)])
            .unwrap();
        let out = registry.drain_job_up_to("j-all", usize::MAX).unwrap();
        assert_eq!(
            registry.pending_batch_depth("j-all").unwrap(),
            0,
            "drain_job_up_to(usize::MAX) must drain all batches"
        );
        assert!(!out.is_empty(), "closed window must produce output batches");
    }

    /// Regression (Wave 3 — Runtime/Flight/Continuous Stream): `drain_job`
    /// must delegate to `drain_job_up_to(DEFAULT_MAX_DRAIN_BATCHES)` rather
    /// than draining unboundedly (`usize::MAX`), so a single call cannot be
    /// made to buffer unbounded memory by enqueuing more pending batches than
    /// one drain can consume.
    #[test]
    fn drain_job_caps_consumption_at_default_max_drain_batches() {
        let registry = ContinuousStreamRegistry::new_unbounded();
        registry
            .register_job(
                "j-capped-drain",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();
        let extra = 5;
        let total = ContinuousStreamRegistry::DEFAULT_MAX_DRAIN_BATCHES + extra;
        for i in 0..total {
            registry
                .push_input("j-capped-drain", vec![batch(i as i64 * 100)])
                .unwrap();
        }
        assert_eq!(
            registry.pending_batch_depth("j-capped-drain").unwrap(),
            total
        );

        let _ = registry.drain_job("j-capped-drain").unwrap();
        assert_eq!(
            registry.pending_batch_depth("j-capped-drain").unwrap(),
            extra,
            "drain_job must consume at most DEFAULT_MAX_DRAIN_BATCHES batches per call"
        );
    }

    #[test]
    fn multi_batch_push_is_admitted_atomically() {
        let registry = ContinuousStreamRegistry::with_max_pending_batches(2);
        registry
            .register_job(
                "j-atomic-capacity",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();

        let error = registry
            .push_input(
                "j-atomic-capacity",
                vec![batch(1_000), batch(2_000), batch(3_000)],
            )
            .expect_err("oversized push must fail as one unit");
        assert!(matches!(
            error,
            RuntimeError::ContinuousStream(ContinuousStreamError::QueueFull {
                current: 0,
                attempted: 3,
                limit: 2,
                ..
            })
        ));
        assert_eq!(
            registry.pending_batch_depth("j-atomic-capacity").unwrap(),
            0
        );
    }

    #[test]
    fn input_schema_is_bound_by_first_successful_push() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job(
                "j-schema",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();
        registry.push_input("j-schema", vec![batch(1_000)]).unwrap();

        let error = registry
            .push_input("j-schema", vec![invalid_batch_without_event_time()])
            .expect_err("schema changes must fail before enqueue");
        assert!(matches!(
            error,
            RuntimeError::ContinuousStream(ContinuousStreamError::SchemaMismatch { .. })
        ));
        assert_eq!(registry.pending_batch_depth("j-schema").unwrap(), 1);
    }

    #[test]
    fn failed_drain_keeps_input_queued() {
        let registry = ContinuousStreamRegistry::new();
        registry
            .register_job(
                "j-failed-drain",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .unwrap();
        registry
            .push_input("j-failed-drain", vec![invalid_batch_without_event_time()])
            .unwrap();

        let error = registry
            .drain_job("j-failed-drain")
            .expect_err("invalid input must fail execution");
        assert!(matches!(
            error,
            RuntimeError::ContinuousStream(ContinuousStreamError::Execution { .. })
        ));
        assert_eq!(
            registry.pending_batch_depth("j-failed-drain").unwrap(),
            1,
            "failed cycle must not acknowledge or remove queued input"
        );
    }

    #[test]
    fn empty_job_id_is_rejected() {
        let registry = ContinuousStreamRegistry::new();
        let error = registry
            .register_job(
                "   ",
                WindowExecutionSpec::tumbling("user_id", "ts", 10_000),
            )
            .expect_err("blank job id must fail");
        assert!(matches!(
            error,
            RuntimeError::ContinuousStream(ContinuousStreamError::InvalidJobId)
        ));
    }
}
