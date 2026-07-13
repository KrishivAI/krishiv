/// Execution model inferred from a plan fragment description.
///
/// This is the central dispatch point that separates batch-terminal execution
/// (R1–R4) from streaming-continuous execution (R5+).  Every call site that
/// would otherwise string-match on the fragment prefix should use this enum.
///
/// **Batch**: the runner executes the fragment, collects output, and reports
/// `TaskState::Succeeded` or `TaskState::Failed`.  The task has a finite
/// lifetime. Optional `task_timeout_secs` applies.
///
/// **Streaming**: the runner enters a continuous operator loop and never reports
/// `Succeeded` while the job is running.  The task terminates only on an
/// explicit `Stop` signal from the coordinator or on a fatal error.
/// `task_timeout_secs` is *ignored* for streaming tasks because the duration
/// is unbounded by design.  R5.1 provides the first real streaming runner;
/// until then, submitting a `stream:` fragment returns
/// `ExecutorError::StreamingNotImplemented`.
///
/// **DeltaBatch**: one bounded tick of an IVM job. The executor processes
/// pending source deltas, runs the view SQL, diffs outputs, and returns each
/// view's full output as task output. The executor is **stateless**: each tick
/// runs on a transient flow seeded from a coordinator-shipped state snapshot,
/// so executors remain replaceable workers. The coordinator is the single
/// source of truth (see `submit_resident_ivm_step` in `krishiv-scheduler`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionModel {
    /// Task runs to completion and returns terminal output.
    Batch,
    /// Task runs an unbounded loop until a `Stop` signal or fatal error.
    Streaming,
    /// One bounded IVM tick: apply deltas, run SQL, diff, return output deltas.
    DeltaBatch,
}

impl ExecutionModel {
    /// Infer the execution model from a [`PlanFragment`].
    ///
    /// Prefers the explicit `is_streaming` flag set by the scheduler. Falls back
    /// to description-based detection for fragments produced by older schedulers
    /// that do not populate the flag.
    pub fn from_plan_fragment(
        fragment: &krishiv_proto::PlanFragment,
    ) -> crate::ExecutorResult<Self> {
        if fragment.is_streaming() {
            return Ok(Self::Streaming);
        }
        Self::from_fragment(fragment.description())
    }

    /// Infer the execution model from a plain description string.
    ///
    /// Used as a fallback when the `PlanFragment::is_streaming` flag is absent
    /// (e.g. fragments emitted by older scheduler versions).
    pub fn from_fragment(fragment: &str) -> crate::ExecutorResult<Self> {
        let profile = krishiv_common::resolve_durability_profile();
        let typed = krishiv_plan::TypedTaskFragment::decode_for_profile(fragment, profile)
            .map_err(|error| crate::ExecutorError::InvalidAssignment {
                message: error.to_string(),
            })?;
        Ok(match typed.execution_kind {
            krishiv_plan::ExecutionKind::Streaming => Self::Streaming,
            krishiv_plan::ExecutionKind::Batch => Self::Batch,
            krishiv_plan::ExecutionKind::DeltaBatch => Self::DeltaBatch,
        })
    }
}
