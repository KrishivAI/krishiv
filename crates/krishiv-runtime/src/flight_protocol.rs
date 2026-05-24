//! Re-exports shared Flight SQL protocol; runtime-specific helpers below.

pub use krishiv_flight::{
    apply_register_directives, catalog_to_batch_tables, encode_batch_sql, encode_continuous_drain,
    encode_explain_sql, has_control_directive, parse_sql, BatchSqlTable, FlightDirective, FlightError,
    FlightResult,
};

use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

/// Encode remote continuous job registration from a local window spec.
pub fn encode_continuous_register(job_id: &str, spec: &LocalWindowExecutionSpec) -> String {
    krishiv_flight::encode_continuous_register(job_id, &spec.to_plan_spec())
}

/// Encode remote bounded window execution from a local window spec.
pub fn encode_bounded_window(
    topic: &str,
    spec: &LocalWindowExecutionSpec,
    input_batches: &[arrow::record_batch::RecordBatch],
) -> RuntimeResult<String> {
    krishiv_flight::encode_bounded_window(topic, &spec.to_plan_spec(), input_batches)
        .map_err(|e| RuntimeError::transport(e.to_string()))
}

/// Encode remote continuous input push.
pub fn encode_continuous_push(
    job_id: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> RuntimeResult<String> {
    krishiv_flight::encode_continuous_push(job_id, batches)
        .map_err(|e| RuntimeError::transport(e.to_string()))
}
