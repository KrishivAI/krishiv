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

    #[test]
    fn batch_with_krishiv_stream_in_name() {
        let plan = PhysicalPlan::new("krishiv-stream:events", ExecutionKind::Batch);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_stream_kafka_prefix() {
        let plan = PhysicalPlan::new("stream-kafka:topic:0:0:records", ExecutionKind::Batch);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_partial_stream_name_not_streaming() {
        let plan = PhysicalPlan::new("my-stream-data", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn empty_name_batch_not_streaming() {
        let plan = PhysicalPlan::new("", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn streaming_with_any_name() {
        let plan = PhysicalPlan::new("anything-at-all", ExecutionKind::Streaming);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_name_starting_with_stream_colon() {
        let plan = PhysicalPlan::new("stream:", ExecutionKind::Batch);
        assert!(is_streaming_plan(&plan));
    }

    #[test]
    fn batch_name_contains_krishiv_stream_anywhere() {
        let plan = PhysicalPlan::new("prefix-krishiv-stream-suffix", ExecutionKind::Batch);
        assert!(is_streaming_plan(&plan));
    }
}
