//! One reading of a join build side's size estimate.
//!
//! # Why this module exists
//!
//! Two independent rules ask "how big is this join's build side?" —
//! [`crate::distributed_plan`]'s broadcast override and
//! [`crate::spillable_join`]'s algorithm choice — and for a while they answered
//! it with two hand-rolled `match`es over the same `Statistics`. They disagreed,
//! and the disagreement was not academic:
//!
//! * TPC-H q21's `NOT EXISTS` becomes a `LeftAnti` self-join over `lineitem`.
//!   DataFusion estimates an anti-join's output as `outer_rows - semi_estimate`
//!   (`joins/utils.rs`), which for a self-join is `593462145 - 593462145` = **0**
//!   rows and 0 bytes. The real intermediate is tens of millions of rows.
//! * A zero is the strongest possible "broadcast me" (`0 < any ceiling`) *and*
//!   the strongest possible "this hash table fits". So one bad number sent both
//!   rules the wrong way at once: q21 was broadcast when it should have been
//!   partitioned, and then — once the broadcast side alone was fixed — died with
//!   `Resources exhausted: HashJoinInput[4] with 806.0 MB already allocated`
//!   out of a 2.6 GB pool, having previously merely been slow.
//!
//! Fixing one rule at a time turned a slow query into a broken one. So the
//! reading of the statistics lives here, once, and the rules consume it.
//!
//! # What is deliberately *not* unified
//!
//! The two rules want genuinely different things from the same numbers, and
//! flattening that difference is what caused a four-query regression
//! (q8 92→375 s, q9 226→576 s, q17 70→339 s, q2 84→104 s):
//!
//! * The **broadcast** override may only fire when the estimate DataFusion
//!   actually acted on is degenerate. `supports_collect_by_thresholds` prefers
//!   `total_byte_size` and falls back to `num_rows`, so a *positive* byte
//!   estimate means DataFusion chose `CollectLeft` on a real number — its call
//!   to make, with the same information. Overriding that is how the four
//!   queries above regressed.
//! * The **spill** choice may be pessimistic on either number, because the cost
//!   of being wrong is asymmetric: a spillable join over a small relation is
//!   nearly free, and a non-spillable join over a large one fails the query.
//!
//! Hence [`BuildSideEstimate::is_wholly_degenerate`] and
//! [`BuildSideEstimate::any_claims_empty`] rather than one predicate.

use datafusion::common::stats::Precision;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A join build side's estimated size, with `Precision::Absent` flattened to
/// `None` so an unknown is never laundered into a confident zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BuildSideEstimate {
    /// Estimated rows; `None` when the planner has no estimate at all.
    pub rows: Option<usize>,
    /// Estimated bytes; `None` when the planner has no estimate at all.
    pub bytes: Option<usize>,
}

/// Read `p`, keeping "absent" distinct from "zero".
fn value(p: &Precision<usize>) -> Option<usize> {
    match p {
        Precision::Exact(v) | Precision::Inexact(v) => Some(*v),
        Precision::Absent => None,
    }
}

impl BuildSideEstimate {
    /// The estimate for `build`, or [`Self::UNKNOWN`] if statistics will not
    /// compute.
    ///
    /// An error computing statistics is not evidence of anything, and both
    /// callers are optimisations — declining to act is always valid.
    pub(crate) fn of(build: &Arc<dyn ExecutionPlan>) -> Self {
        match build.partition_statistics(None) {
            Ok(stats) => Self {
                rows: value(&stats.num_rows),
                bytes: value(&stats.total_byte_size),
            },
            Err(_) => Self::UNKNOWN,
        }
    }

    /// No estimate on either axis.
    pub(crate) const UNKNOWN: Self = Self {
        rows: None,
        bytes: None,
    };

    /// Nothing here is known.
    pub(crate) fn is_unknown(&self) -> bool {
        self.rows.is_none() && self.bytes.is_none()
    }

    /// Either axis explicitly claims the relation is empty.
    ///
    /// For the spill decision: a build side the planner calls empty, on a join
    /// that is being asked to build a hash table over it, is the estimator
    /// giving up rather than a measurement.
    pub(crate) fn any_claims_empty(&self) -> bool {
        self.rows == Some(0) || self.bytes == Some(0)
    }

    /// Every axis is non-positive, *and* at least one of them is an explicit
    /// zero.
    ///
    /// The second clause is what keeps this from firing on a wholly unknown
    /// estimate. It cannot arise from DataFusion's own choice —
    /// `supports_collect_by_thresholds` returns `false` when both statistics are
    /// absent, so it would never pick `CollectLeft` there — but treating
    /// "unknown" as "degenerate" left the broadcast override and the spill rule
    /// disagreeing about the same plan, and a join distributed without being
    /// made spillable is exactly what broke q21.
    pub(crate) fn is_wholly_degenerate(&self) -> bool {
        let non_positive = |v: Option<usize>| !matches!(v, Some(n) if n > 0);
        non_positive(self.rows) && non_positive(self.bytes) && self.any_claims_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn estimate(rows: Option<usize>, bytes: Option<usize>) -> BuildSideEstimate {
        BuildSideEstimate { rows, bytes }
    }

    /// q21's shape: both axes an explicit zero. Both rules must act.
    #[test]
    fn an_explicit_double_zero_is_degenerate_for_both_rules() {
        let q21 = estimate(Some(0), Some(0));
        assert!(q21.is_wholly_degenerate(), "broadcast must override this");
        assert!(q21.any_claims_empty(), "spill must be pessimistic here");
        assert!(!q21.is_unknown(), "a zero is a claim, not an absence");
    }

    /// The regression this pins: a large but *positive* estimate is DataFusion's
    /// call to make. q8/q9/q17 all had `rows≈4000000, bytes=absent` and were
    /// converted by a rule that demanded a ceiling; converting them cost
    /// 4.1x, 2.5x and 4.8x respectively.
    #[test]
    fn a_large_positive_row_estimate_is_never_degenerate() {
        let q8 = estimate(Some(4_000_000), None);
        assert!(
            !q8.is_wholly_degenerate(),
            "a plausible multi-million-row estimate must be left alone; the \
             ceiling is DataFusion's decision, made with these same numbers"
        );
        assert!(!q8.any_claims_empty());
    }

    /// A wholly absent estimate is "no idea", not "empty". Treating it as
    /// degenerate is the latent inconsistency that let one rule distribute a
    /// join the other declined to make spillable.
    #[test]
    fn a_wholly_absent_estimate_is_unknown_not_degenerate() {
        let nothing = estimate(None, None);
        assert!(nothing.is_unknown());
        assert!(
            !nothing.is_wholly_degenerate(),
            "absent must not be read as empty — that is what made the broadcast \
             override and the spill rule disagree about the same plan"
        );
        assert!(!nothing.any_claims_empty());
    }

    /// Zero rows with positive bytes is incoherent, and the two rules are
    /// allowed to read it differently: the spill side is pessimistic because
    /// being wrong is cheap, the broadcast side defers because DataFusion chose
    /// `CollectLeft` on a byte estimate it could see.
    #[test]
    fn zero_rows_with_positive_bytes_splits_the_two_readings() {
        let incoherent = estimate(Some(0), Some(500));
        assert!(
            incoherent.any_claims_empty(),
            "spill must not trust a zero row count"
        );
        assert!(
            !incoherent.is_wholly_degenerate(),
            "broadcast must defer: DataFusion had a positive byte estimate"
        );
    }
}
