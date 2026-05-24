//! R18 lakehouse SQL extensions: delta/hudi providers, AS OF, MERGE INTO.

mod as_of;
mod merge;
mod providers;

pub use as_of::{AsOfTableRef, preprocess_as_of_sql};
pub use merge::{MergeResult, MergeTargetUnsupportedError, execute_merge_sql};
pub use providers::{
    apply_as_of_refs, register_delta_uri, register_hudi_uri, register_scan_batches,
};
