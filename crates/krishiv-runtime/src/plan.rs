//! Physical plan classification helpers (ADR-12.5).

use krishiv_plan::{ExecutionKind, PhysicalPlan};

/// Returns true when the plan must run through the single-node streaming runtime
/// rather than DataFusion batch execution.
pub fn is_streaming_plan(plan: &PhysicalPlan) -> bool {
    if plan.kind() == ExecutionKind::Streaming {
        return true;
    }
    let name = plan.name();
    name.starts_with("stream:")
        || name.contains("krishiv-stream")
        || name.starts_with("stream-kafka:")
}

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

    use super::is_streaming_plan;

    #[test]
    fn streaming_kind_is_streaming_plan() {
        let plan = PhysicalPlan::new("events", ExecutionKind::Streaming);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_kind_with_stream_prefix_is_streaming_plan() {
        let plan = PhysicalPlan::new("stream:tw:key=u", ExecutionKind::Batch);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn ordinary_batch_sql_is_not_streaming() {
        let plan = PhysicalPlan::new("sql-query", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }
}
