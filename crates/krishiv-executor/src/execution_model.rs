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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionModel {
    /// Task runs to completion and returns terminal output.
    Batch,
    /// Task runs an unbounded loop until a `Stop` signal or fatal error.
    Streaming,
}

impl ExecutionModel {
    /// Infer the execution model from the plan fragment description.
    ///
    /// All `stream:` prefixed fragments use the streaming model.
    /// Everything else is treated as batch (existing behaviour is preserved).
    pub fn from_fragment(fragment: &str) -> crate::ExecutorResult<Self> {
        let profile = krishiv_common::resolve_durability_profile();
        let typed = krishiv_plan::TypedTaskFragment::decode_for_profile(fragment, profile)
            .map_err(|error| crate::ExecutorError::InvalidAssignment {
                message: error.to_string(),
            })?;
        Ok(match typed.execution_kind {
            krishiv_plan::ExecutionKind::Streaming => Self::Streaming,
            krishiv_plan::ExecutionKind::Batch => Self::Batch,
        })
    }
}
