//! R18 lakehouse SQL extensions: delta/hudi providers, AS OF, MERGE INTO.

mod as_of;
mod merge;
mod providers;

pub use as_of::{preprocess_as_of_sql, AsOfTableRef};
pub use merge::{execute_merge_sql, MergeResult, MergeTargetUnsupportedError};
pub use providers::{apply_as_of_refs, register_delta_uri, register_hudi_uri, register_scan_batches};
