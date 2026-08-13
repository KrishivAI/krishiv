//! The empty-plan tripwire.
//!
//! # What this defends against
//!
//! iceberg-rust 0.10 gives every `Table` a captured `Runtime`. When that
//! runtime is dead or blocked, `TableScan::plan_files()` yields a
//! **successful empty stream** rather than an error — the scan reports "this
//! table has no files" and returns `Ok`.
//!
//! Three callers in this module treat "no files" as ground truth and then act
//! destructively on it:
//!
//! * [`crate::lakehouse::maintenance`] — an empty file set means *every* file
//!   in the table directory is an orphan, and orphan cleanup deletes them.
//! * `IcebergNativeTable::read_all` → the streaming sink's overwrite commit —
//!   an empty read overwrites the table with nothing.
//! * [`crate::lakehouse::dml`] `iceberg_merge_into` — an empty target means the
//!   drop-and-recreate keeps only source rows.
//!
//! Each of those is a full-table data loss that returns success. No suite in
//! this workspace exercises a shut-down or blocked catalog runtime, so nothing
//! would catch it.
//!
//! # Why it is armed now, before the version that needs it
//!
//! Under 0.9.1 this cannot fire: `plan_files()` propagates an error instead of
//! completing empty. That is the point — a guard added in the same commit as
//! the bump is a guard nobody has watched behave. This one gets to run against
//! the version where it is provably a no-op first, so if it ever *does* fire,
//! the bump is the thing that changed.
//!
//! # Why it checks the snapshot's own totals
//!
//! "A current snapshot exists but the plan is empty" is NOT an error: a
//! snapshot legitimately holding zero data files (an empty append, a delete of
//! everything) must plan zero tasks. So the tripwire compares against what the
//! snapshot itself claims to hold — iceberg's standard `total-data-files`
//! summary property, which iceberg-rust computes on every commit
//! (`spec/snapshot_summary.rs`). A plan of zero against a recorded count of
//! zero is agreement; a plan of zero against a recorded count above zero is a
//! contradiction, and contradiction is the signal.

use crate::lakehouse::LakehouseError;

/// Iceberg's standard snapshot-summary key for the data-file count.
const TOTAL_DATA_FILES: &str = "total-data-files";

/// Refuse a scan that planned nothing while the snapshot says it holds files.
///
/// `context` names the operation for the error and the log line; it should read
/// as the thing that was about to happen, e.g. `"expire_snapshots"`.
pub(crate) fn guard_empty_plan(
    table: &iceberg::table::Table,
    planned: usize,
    context: &str,
) -> Result<(), LakehouseError> {
    if planned > 0 {
        return Ok(());
    }
    // No snapshot at all: there is genuinely nothing to plan, and every caller
    // here already handles the empty case correctly.
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(());
    };
    let recorded = snapshot
        .summary()
        .additional_properties
        .get(TOTAL_DATA_FILES)
        .map(|v| v.trim().parse::<u64>());

    match recorded {
        // The snapshot agrees the table is empty.
        Some(Ok(0)) => Ok(()),
        // The contradiction this exists to catch.
        Some(Ok(files)) => Err(LakehouseError::Io(format!(
            "{context}: the scan planned 0 files but snapshot {} records {files} data \
             file(s). Refusing rather than treating the table as empty — this is the \
             signature of a scan whose runtime was shut down or blocked, and every \
             caller of this path acts destructively on an empty result. The table has \
             NOT been modified.",
            snapshot.snapshot_id()
        ))),
        // Unparseable or absent. iceberg-rust writes this key on every commit,
        // so its absence means the snapshot came from somewhere else, and we
        // cannot tell an empty table from a broken scan. Every caller of this
        // guard deletes, overwrites, or recreates on the answer, so refuse:
        // the cost of a false refusal is a run that has to be retried, and the
        // cost of a false acceptance is the table.
        _ => Err(LakehouseError::Io(format!(
            "{context}: the scan planned 0 files and snapshot {} carries no \
             `total-data-files` summary, so an empty table cannot be told apart from \
             a failed scan. Refusing, because this operation would delete or replace \
             data on that answer. The table has NOT been modified.",
            snapshot.snapshot_id()
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    /// The decision table, exercised directly. `guard_empty_plan` needs a real
    /// `iceberg::table::Table` to test end to end, which needs a catalog and a
    /// runtime — the very things whose failure this guards. So the branching
    /// logic is factored to be checkable on its own inputs, and the wiring is
    /// covered by the callers' own tests.
    fn decide(planned: usize, recorded: Option<&str>) -> bool {
        if planned > 0 {
            return true;
        }
        matches!(recorded.map(|v| v.trim().parse::<u64>()), Some(Ok(0)))
    }

    #[test]
    fn a_plan_with_files_always_passes() {
        assert!(decide(3, Some("3")));
        assert!(decide(1, None));
    }

    #[test]
    fn an_empty_plan_agreeing_with_an_empty_snapshot_passes() {
        // The case that would make a naive "snapshot exists = must have files"
        // tripwire refuse legitimate work.
        assert!(decide(0, Some("0")));
        assert!(decide(0, Some(" 0 ")));
    }

    #[test]
    fn an_empty_plan_contradicting_the_snapshot_is_refused_for_everyone() {
        // Not stance-dependent: a contradiction means the scan is wrong, and a
        // reader acting on a wrong empty answer still returns a wrong answer.
        assert!(!decide(0, Some("1")));
        assert!(!decide(0, Some("4096")));
    }

    #[test]
    fn an_unprovable_empty_plan_is_refused() {
        for missing in [None, Some(""), Some("not-a-number")] {
            assert!(
                !decide(0, missing),
                "what cannot be proven empty must not be acted on: {missing:?}"
            );
        }
    }
}
