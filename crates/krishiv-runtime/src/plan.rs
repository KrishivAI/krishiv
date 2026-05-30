//! Physical plan classification helpers (ADR-12.5).

use krishiv_plan::{ExecutionKind, PhysicalPlan};

/// Returns true when the plan must run through the single-node streaming runtime
/// rather than DataFusion batch execution.
///
/// Classification is based on the plan's [`ExecutionKind`] — not on string
/// prefix matching.  Prior versions used name-based heuristics which could
/// misclassify user SQL containing the literal text "stream:" or "krishiv-stream".
/// ADR-12.5 established that `ExecutionKind::Streaming` is the sole discriminant.
pub fn is_streaming_plan(plan: &PhysicalPlan) -> bool {
    plan.kind() == ExecutionKind::Streaming
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
    fn batch_kind_with_stream_prefix_is_not_streaming() {
        let plan = PhysicalPlan::new("stream:tw:key=u", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn ordinary_batch_sql_is_not_streaming() {
        let plan = PhysicalPlan::new("sql-query", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_stream_in_name_is_not_streaming() {
        let plan = PhysicalPlan::new("krishiv-stream:events", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_with_stream_kafka_is_not_streaming() {
        let plan = PhysicalPlan::new("stream-kafka:topic:0:0:records", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
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
    fn batch_name_stream_colon_is_not_streaming() {
        let plan = PhysicalPlan::new("stream:", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }

    #[test]
    fn batch_name_krishiv_stream_is_not_streaming() {
        let plan = PhysicalPlan::new("prefix-krishiv-stream-suffix", ExecutionKind::Batch);
        assert!(!is_streaming_plan(&plan));
    }
}
