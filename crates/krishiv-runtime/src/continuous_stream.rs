//! Shared continuous streaming job state between session and in-process executor.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_exec::ContinuousWindowExecutor;
use krishiv_plan::window::WindowExecutionSpec;

use crate::{RuntimeError, RuntimeResult};

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
    input: Mutex<VecDeque<RecordBatch>>,
    /// Locked only during `drain_job` for the window computation itself.
    executor: Mutex<ContinuousWindowExecutor>,
}

impl std::fmt::Debug for ContinuousJobEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuousJobEntry")
            .field("spec", &self.spec)
            .field(
                "input_queue_len",
                &self.input.lock().map(|q| q.len()).unwrap_or(0),
            )
            .finish_non_exhaustive()
    }
}

/// Default maximum pending input batches per job before backpressure kicks in.
const DEFAULT_MAX_PENDING_BATCHES: usize = 1024;

/// Registry for long-running streaming jobs (session-scoped).
#[derive(Debug)]
pub struct ContinuousStreamRegistry {
    jobs: DashMap<String, Arc<ContinuousJobEntry>>,
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
            Arc::new(ContinuousJobEntry {
                spec,
                input: Mutex::new(VecDeque::new()),
                executor: Mutex::new(executor),
            }),
        );
        Ok(())
    }

    /// Maximum input batches consumed by a single [`drain_job_up_to`] call.
    ///
    /// Limits the amount of memory allocated by one drain cycle. Callers that
    /// need to drain more than this many batches should call `drain_job_up_to`
    /// in a loop until [`pending_batch_depth`] returns 0.
    pub const DEFAULT_MAX_DRAIN_BATCHES: usize = 256;

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
                message: format!("continuous job '{job_id}': not found"),
            })?;
        let mut queue = entry.input.lock().map_err(|_| {
            RuntimeError::transport(format!(
                "continuous job '{job_id}' input lock poisoned during push_input"
            ))
        })?;
        let current_depth = queue.len();
        if current_depth >= self.max_pending_batches {
            return Err(RuntimeError::InvalidState {
                message: format!(
                    "continuous job '{job_id}': input queue is full ({current_depth} batches \
                     pending, limit {limit}); call drain_job before pushing more data",
                    limit = self.max_pending_batches,
                ),
            });
        }
        queue.extend(batches);
        Ok(())
    }

    /// Returns the number of pending input batches for a job.
    /// Callers can use this to implement self-rate-limiting before calling `push_input`.
    pub fn pending_batch_depth(&self, job_id: &str) -> RuntimeResult<usize> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("continuous job '{job_id}': not found"),
            })?;
        let queue = entry.input.lock().map_err(|_| {
            RuntimeError::transport(format!(
                "continuous job '{job_id}' input lock poisoned during pending_batch_depth"
            ))
        })?;
        Ok(queue.len())
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
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("continuous job '{job_id}': not found"),
            })?;
        // Steal at most `max_input_batches` entries with a short critical section,
        // then release the input lock before acquiring the executor lock.
        let input: Vec<RecordBatch> = {
            let mut queue = entry.input.lock().map_err(|_| {
                RuntimeError::transport(format!(
                    "continuous job '{job_id}' input lock poisoned during drain_job"
                ))
            })?;
            let take = queue.len().min(max_input_batches);
            queue.drain(..take).collect()
        };
        if input.is_empty() {
            return Ok(Vec::new());
        }
        // Window computation runs under the executor lock only — producers are
        // not blocked during this (potentially slow) aggregation step.
        let mut exec = entry.executor.lock().map_err(|_| {
            RuntimeError::transport(format!(
                "continuous job '{job_id}' executor lock poisoned during drain_job"
            ))
        })?;
        exec.drain(input)
            .map_err(|e| RuntimeError::transport(e.to_string()))
    }

    /// Drain ALL pending input batches through the window operator.
    ///
    /// Equivalent to calling [`drain_job_up_to`] with `usize::MAX`. For large
    /// input queues, prefer `drain_job_up_to(job_id, DEFAULT_MAX_DRAIN_BATCHES)`
    /// in a loop to avoid memory spikes.
    pub fn drain_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        self.drain_job_up_to(job_id, usize::MAX)
    }

    /// Borrow the window spec for coordinator fragment encoding.
    pub fn job_spec(&self, job_id: &str) -> RuntimeResult<WindowExecutionSpec> {
        let entry = self
            .jobs
            .get(job_id)
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("continuous job '{job_id}': not found"),
            })?;
        Ok(entry.spec.clone())
    }

    /// Returns `true` if a job with this id has been registered.
    pub fn has_job(&self, job_id: &str) -> bool {
        self.jobs.contains_key(job_id)
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
            .ok_or_else(|| RuntimeError::InvalidState {
                message: format!("continuous job '{job_id}': not found"),
            })?;
        let mut exec = entry.executor.lock().map_err(|_| {
            RuntimeError::transport(format!(
                "continuous job '{job_id}' executor lock poisoned during snapshot"
            ))
        })?;
        exec.snapshot()
            .map_err(|e| RuntimeError::transport(format!("snapshot failed: {e}")))
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
        use krishiv_exec::ContinuousWindowExecutor;
        let job_id = job_id.into();
        let mut executor = ContinuousWindowExecutor::new(spec.clone())
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        // Restore window state from the snapshot before accepting new input.
        let wm = executor.last_watermark_ms();
        if !snapshot_bytes.is_empty() {
            executor
                .restore_from_snapshot(snapshot_bytes)
                .map_err(|e| {
                    RuntimeError::transport(format!(
                        "continuous job '{job_id}' snapshot restore failed: {e}"
                    ))
                })?;
        }
        let _ = wm; // watermark is embedded in restored state
        self.jobs.insert(
            job_id,
            std::sync::Arc::new(ContinuousJobEntry {
                spec,
                input: std::sync::Mutex::new(std::collections::VecDeque::new()),
                executor: std::sync::Mutex::new(executor),
            }),
        );
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
}
