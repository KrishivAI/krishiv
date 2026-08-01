//! Per-join selection of a spillable algorithm under a memory cap.
//!
//! # The failure this fixes
//!
//! TPC-H q18 on a 4500 MiB executor:
//!
//! ```text
//! Resources exhausted: Failed to allocate additional 1015.5 KB for
//! HashJoinInput[0] with 732.4 MB already allocated for this reservation -
//! 205.0 KB remain available
//! ```
//!
//! Not a leak, not a mis-sized budget: the pool refused correctly. DataFusion's
//! hash join holds its entire build side in memory with no spill path, so when
//! the pool is exhausted the operator has nowhere to put the overflow and the
//! query fails. Sort-merge join spills.
//!
//! # Why per-join, and why this exists as a rule instead of a config bit
//!
//! The first attempt (8f72a340, reverted) set
//! `datafusion.optimizer.prefer_hash_join = false` for the whole session
//! whenever a cgroup limit existed. Measured on the cluster, q2 — ten stages of
//! joins whose build sides all fit comfortably — went from 189 s to past a
//! 2400 s timeout. Sorting both sides of every join to rescue the one join
//! that overflows is a catastrophic trade.
//!
//! So the decision is made where the information is: at each hash join, from
//! that join's *estimated build size* against the *per-task share* of the
//! query pool. Three deliberately conservative gates, each a direct lesson
//! from the q2 regression:
//!
//! 1. **No cap, no change.** An embedded engine on 23 GB keeps hash joins.
//! 2. **Unknown statistics keep hash join.** A missing estimate is not
//!    evidence of a big build side, and guessing "big" re-creates the blanket
//!    regression. The cost of guessing "small" wrongly is the status quo —
//!    q18 fails as it does today — while the cost of guessing "big" wrongly
//!    is a q2-shaped timeout on healthy queries.
//! 3. **The join mode must be convertible.** `Partitioned` inputs are already
//!    hashed on the join keys — the distribution sort-merge needs — so the
//!    conversion adds per-partition sorts, not exchanges. `CollectLeft`
//!    converts too *when the plan has a single partition*, where sorting alone
//!    satisfies sort-merge. Anything else keeps hash join.
//!
//!    This bullet used to read "CollectLeft build sides are small by
//!    construction". They are not: `CollectLeft` is picked from an estimate
//!    and buffers the whole build side. Worse, a task engine plans with
//!    `target_partitions = cores / slots`, which is **1** on a 3-core, 3-slot
//!    executor — so *every* join was CollectLeft and the rule converted
//!    nothing at all while five SF100 queries died on it.
//!
//! The sorts are inserted explicitly (with partitioning preserved) rather than
//! left to `EnforceSorting`, because appended optimizer rules run *after* the
//! enforcement passes — a requirement declared here would never be satisfied.
//!
//! # Which spillable algorithm
//!
//! Sort-merge is not the only way to make a join spill, and it is the worse
//! one: it sorts *both* inputs in full even when nearly all the data would have
//! fitted, which is what cost q2 6.3x. [`crate::grace_hash_join`] partitions
//! both sides by key and joins bucket by bucket instead — no sorting, and the
//! buckets that fit never reach the disk.
//!
//! So when `grace` is set the rule tries that first and keeps sort-merge as the
//! fallback for shapes it refuses. It is **off by default**: sort-merge is what
//! the SF100 sweeps have actually been measured against, and a newer operator
//! earns the default by beating it on the cluster.

use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Environment override for the build-size threshold, in bytes.
pub const SPILL_JOIN_BUILD_BYTES_ENV: &str = "KRISHIV_SPILL_JOIN_BUILD_BYTES";

/// Share of the per-task memory allowance above which an estimated build side
/// is treated as "will not fit as a hash table".
///
/// A hash table costs more than the raw bytes it holds (buckets, hashes,
/// padding), and the build side is not the task's only consumer, so the
/// threshold sits well below 1.0. Below it, hash join stays — it is the right
/// algorithm when it fits.
const BUILD_FRACTION_OF_TASK_SHARE: f64 = 0.5;

/// Bytes assumed for a column whose type carries no fixed width.
///
/// Varlen columns (`Utf8`, `Binary`, and their `View`/`Large` forms) have no
/// Build-side bytes derived from a row count when `total_byte_size` is absent.
///
/// Returns `None` when the row count is absent too — the one case where the
/// planner genuinely knows nothing and guessing would be a coin flip.
fn estimated_build_bytes_from_rows(
    stats: &datafusion::common::Statistics,
    build_schema: &arrow::datatypes::Schema,
) -> Option<u64> {
    let rows = match stats.num_rows {
        Precision::Exact(rows) | Precision::Inexact(rows) => rows,
        Precision::Absent => return None,
    };
    // One reading of row width, shared with the broadcast rule — see
    // `crate::join_estimates`. Two hand-rolled widths is how the two rules came
    // to disagree about row *counts*, and there is no reason to repeat it.
    let row_width = crate::join_estimates::estimated_row_width(build_schema);
    u64::try_from(rows.saturating_mul(row_width)).ok()
}

/// This join's estimated build-side bytes, or `None` when the planner knows
/// neither a byte size nor a row count for it.
///
/// `total_byte_size` absent does not mean "size unknown" — DataFusion often has
/// a row count when it has no byte size (a shuffle read, a filter over a scan
/// with row stats). Deriving bytes from rows uses information the planner
/// already holds instead of surrendering at the first absent field, which is
/// how q9/SF100 kept a hash join whose build side then took 797.5 MB of a
/// 797.6 MB pool.
///
/// Still conservative: with the row count *also* absent this returns `None` and
/// the caller keeps the hash join, because guessing "big" for every join is the
/// session-wide switch that timed q2 out.
///
/// # The one estimate that is treated as unbounded
///
/// An estimate of **zero bytes and zero rows** on a join that is being asked to
/// build a hash table is not a measurement — it is the estimator giving up, and
/// this rule must not read it as "fits comfortably".
///
/// TPC-H q21 at SF100 is the case. Its `NOT EXISTS` becomes a `LeftAnti`
/// self-join over `lineitem`, which DataFusion estimates as
/// `outer_rows - semi_estimate` = `593462145 - 593462145` = 0
/// (`joins/utils.rs`). The real intermediate is tens of millions of rows. That
/// single zero poisoned **two** independent decisions: the broadcast choice
/// (see `distributed_plan::broadcast_build_estimate_is_empty`) and this one. With
/// the broadcast side fixed so the join is hash-partitioned, each task then had
/// to build its own share as a hash table — and q21 died with
/// `Resources exhausted: HashJoinInput[4] with 806.0 MB already allocated` out
/// of a 2.6 GB pool, having previously merely been slow.
///
/// So a degenerate zero reports `u64::MAX`: assume it does not fit and pick the
/// spillable algorithm. The cost of being wrong is a spillable join over an
/// empty relation, which is free; the cost of trusting it is a failed query.
fn build_bytes_estimate(hash_join: &HashJoinExec) -> Option<u64> {
    // One shared reading of the statistics; see `crate::join_estimates`.
    //
    // An explicit zero is a *claim* that the relation is empty, and it is the
    // claim this function refuses to believe. `Absent` is different — an honest
    // "I do not know" — and keeps the existing policy of leaving the hash join
    // alone, which is what stops this rule from re-creating the q2 timeout.
    let estimate = crate::join_estimates::BuildSideEstimate::of(hash_join.left());
    if estimate.is_unknown() {
        return None;
    }
    if estimate.any_claims_empty() {
        return Some(u64::MAX);
    }
    // An error computing statistics is not evidence of a large build side, and
    // this rule is an optimisation: declining is always a valid answer.
    let stats = hash_join.left().partition_statistics(None).ok()?;
    match stats.total_byte_size {
        Precision::Exact(bytes) | Precision::Inexact(bytes) => u64::try_from(bytes).ok(),
        Precision::Absent => {
            estimated_build_bytes_from_rows(&stats, &hash_join.left().schema())
        }
    }
}

/// Reorder a join filter so that every left-side column precedes every
/// right-side one.
///
/// # The bug this works around
///
/// DataFusion's sort-merge join builds the filter's intermediate batch as
/// **all left columns followed by all right columns**
/// (`joins/sort_merge_join/filter.rs::get_filter_columns`, reached from
/// `materializing_stream.rs`):
///
/// ```text
/// filter_columns.extend(left_columns);   // every Left entry, in order
/// filter_columns.extend(right_columns);  // then every Right entry
/// ```
///
/// But `JoinFilter::schema()` is ordered by `column_indices` **as given**, and
/// `HashJoinExec` builds the batch in that same given order. So a filter whose
/// `column_indices` name a right-side column before a left-side one is correct
/// under hash join and wrong under sort-merge: the batch's columns no longer
/// line up with the schema, and Arrow refuses it.
///
/// That is TPC-H q17 and q19 at SF100, verbatim:
///
/// ```text
/// q17: expected Decimal128(15, 2) but found Decimal128(30, 15) at column index 0
/// q19: expected Decimal128(15, 2) but found Utf8View        at column index 0
/// ```
///
/// Both filters name `l_quantity` (right) first, so column 0 received the left
/// side's first column instead — the `0.2 * avg(l_quantity)` expression in q17,
/// `p_brand` in q19. Neither query is doing anything unusual; any filter that
/// mentions the probe side first hits it.
///
/// (DataFusion's own *other* sort-merge path, `bitwise_stream.rs`'s
/// `evaluate_filter_for_inner_row`, iterates `column_indices` in order and is
/// correct. The two paths disagree with each other, which is what makes this a
/// DataFusion bug rather than a contract we were misreading.)
///
/// # The fix
///
/// Permute `column_indices` and the intermediate schema into the order
/// sort-merge is going to materialise anyway, and rewrite the filter
/// expression's column indices to match. The filter then means exactly what it
/// meant before, expressed in the layout the operator actually builds.
///
/// Returns `None` when the filter cannot be normalised (a `JoinSide::None`
/// entry, or an expression column outside the intermediate schema), in which
/// case the caller keeps the hash join — declining is always safe.
fn left_first_filter(filter: &JoinFilter) -> Option<JoinFilter> {
    use datafusion::common::JoinSide;
    use datafusion::physical_expr::expressions::Column;

    let indices = filter.column_indices();
    let mut order: Vec<usize> = Vec::with_capacity(indices.len());
    order.extend(
        indices
            .iter()
            .enumerate()
            .filter(|(_, ci)| ci.side == JoinSide::Left)
            .map(|(at, _)| at),
    );
    order.extend(
        indices
            .iter()
            .enumerate()
            .filter(|(_, ci)| ci.side == JoinSide::Right)
            .map(|(at, _)| at),
    );
    // A side we do not understand (`JoinSide::None`) would be dropped by the
    // partition above; refuse rather than silently lose a filter column.
    if order.len() != indices.len() {
        return None;
    }
    // Already left-first: hand back the filter untouched so the common case
    // allocates nothing and stays byte-identical.
    if order.iter().enumerate().all(|(to, from)| to == *from) {
        return Some(filter.clone());
    }

    let mut moved_to = vec![0usize; indices.len()];
    for (to, &from) in order.iter().enumerate() {
        *moved_to.get_mut(from)? = to;
    }

    let fields = filter.schema().fields();
    let mut permuted = Vec::with_capacity(order.len());
    for &from in &order {
        permuted.push(fields.get(from)?.as_ref().clone());
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(permuted));
    let column_indices: Vec<_> = order
        .iter()
        .map(|&from| indices.get(from).cloned())
        .collect::<Option<Vec<_>>>()?;

    // The filter expression addresses the intermediate schema positionally, so
    // permuting that schema means re-pointing every column in the expression.
    type Expr = Arc<dyn datafusion::physical_expr::PhysicalExpr>;
    let original: Expr = Arc::clone(filter.expression());
    let expression = original
        .transform(|node: Expr| {
            // `PhysicalExpr: Any` — upcast to downcast, as elsewhere in this file.
            let any = node.as_ref() as &dyn std::any::Any;
            let Some(column) = any.downcast_ref::<Column>() else {
                return Ok(Transformed::no(node));
            };
            let Some(&to) = moved_to.get(column.index()) else {
                return Err(datafusion::error::DataFusionError::Internal(format!(
                    "join filter column {} is outside its {}-column intermediate schema",
                    column.index(),
                    moved_to.len()
                )));
            };
            Ok(Transformed::yes(Arc::new(Column::new(column.name(), to)) as Expr))
        })
        .ok()?
        .data;

    Some(JoinFilter::new(expression, column_indices, schema))
}

/// What the budget needs to know about one hash join in the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JoinFacts {
    /// Estimated build-side bytes, or `None` when the planner knows neither a
    /// byte size nor a row count.
    bytes: Option<u64>,
    /// Whether [`SpillableJoinSelection::convert`] can convert this join's
    /// *mode* at all. A join it will refuse holds its build side whatever the
    /// budget decides, so pretending otherwise mis-spends the budget.
    convertible: bool,
}

impl JoinFacts {
    /// Bytes this join is certain to hold if left alone. Unknown counts as 0 —
    /// the same assumption the per-join gate makes when it keeps a join whose
    /// size it cannot estimate.
    fn retained_bytes(self) -> u64 {
        self.bytes.unwrap_or(0)
    }

    /// Whether the budget is free to choose for this join.
    ///
    /// Only joins that are both convertible and measurable are candidates: an
    /// unknown size keeps its hash join at the per-join gate regardless.
    fn is_candidate(self) -> bool {
        self.convertible && self.bytes.is_some()
    }
}

/// Memory to assume for the joins whose build side cannot be estimated.
///
/// # What this does, and what it deliberately cannot do
///
/// [`JoinFacts::retained_bytes`] reports **zero** for an unmeasurable join, so
/// [`SpillableJoinSelection::conversion_decisions`] valued such joins at
/// nothing when deciding whether aggregate pressure existed. This charges each
/// one `threshold / joins` instead — a refusal to claim the join is free.
///
/// **It only bites on a plan that MIXES measurable and unmeasurable joins**,
/// where it makes the measurable ones convert sooner. Two boundaries make that
/// exact, and both are intentional:
///
/// * With **every** join unmeasurable the term sums to `threshold` (n shares of
///   `threshold / n`), so `total <= threshold` short-circuits and nothing
///   converts. That is not a bug to route around: gate 2 keeps an unmeasurable
///   join's hash join *unconditionally*, and [`JoinFacts::is_candidate`]
///   requires `bytes.is_some()`, so there is no decision left for a budget to
///   make. An all-unknown plan is beyond this rule's reach by construction.
/// * A plan with no unmeasurable joins gets zero, so it behaves exactly as
///   before.
///
/// # Correction: this was written for q21, and q21 was not this
///
/// This function was added believing q21's SF100 failure —
///
/// ```text
/// Resources exhausted: Failed to allocate additional 310.2 MB for
/// HashJoinInput[0] with 0.0 B already allocated for this reservation -
/// 87.9 MB remain available for the total memory pool: fair(pool_size: 2.6 GB)
/// ```
///
/// — was siblings each valued at zero exhausting the pool. It was not. **Every
/// join in every plan was unmeasurable**, because declaring a primary key had
/// silently disabled the table's statistics (fixed in `register_parquet_table`;
/// see its docs). Measured across coordinator and all three executors:
/// `unmeasurable == hash_joins` in **all 414 passes**, **zero** conversions.
/// q21 was therefore the all-unknown boundary above, which this term cannot
/// help — and it duly did not. Restoring statistics fixed q21 and q17.
///
/// It is kept because the mixed case is real and reachable (a shuffle read
/// whose upstream estimate is absent alongside measurable scans), and because
/// valuing an unmeasurable join at zero is indefensible on its own terms. But
/// it has **never been observed to change a decision on the SF100 corpus**, and
/// nobody should cite it as the reason a query stopped failing.
///
/// # Why an even split, and why this does not re-create the q2 regression
///
/// With no byte size and no row count there is genuinely nothing to measure,
/// so any figure is an assumption; the only question is which assumption is
/// defensible. Zero asserts the join is free, which is the assumption that
/// just failed. Treating it as unbounded would convert every join in sight —
/// that is the session-wide `prefer_hash_join = false` switch that took q2
/// from 189 s past a 2400 s timeout.
///
/// The neutral assumption between them is that a shared pool divides evenly
/// among the operators holding it: each unmeasurable join is charged
/// `threshold / joins`. It is not a claim about the join's real size, it is a
/// refusal to claim the join is free.
///
/// **Queries whose joins are all measurable are untouched**: this returns 0 for
/// them, `total` is unchanged, and a plan with no aggregate pressure still
/// short-circuits to the per-join gate exactly as before. The change can only
/// bite where an unmeasurable join exists *and* the pool is under pressure —
/// which is the case it was missing.
fn unknown_build_pressure(facts: &[JoinFacts], threshold: u64) -> u64 {
    let unknown = facts.iter().filter(|f| f.bytes.is_none()).count();
    if unknown == 0 || facts.is_empty() {
        return 0;
    }
    let share = threshold / facts.len() as u64;
    share.saturating_mul(unknown as u64)
}

/// Facts for every hash join in `plan`, **in `transform_up` order**.
///
/// Post-order (children before parent, children left to right) is exactly the
/// order `TreeNode::transform_up` visits nodes, which is what lets the caller
/// pair the Nth fact with the Nth join it is asked to rewrite. `ExecutionPlan`
/// offers no node identity and `transform_up` rebuilds parents as their
/// children change — so pointers are useless here, but position is stable.
fn collect_join_facts(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<JoinFacts>) {
    for child in plan.children() {
        collect_join_facts(child, out);
    }
    let any = plan.as_ref() as &dyn std::any::Any;
    if let Some(hash_join) = any.downcast_ref::<HashJoinExec>() {
        out.push(JoinFacts {
            bytes: build_bytes_estimate(hash_join),
            convertible: convertible_mode(hash_join).is_some(),
        });
    }
}

/// Whether this join's mode can become sort-merge, and with what partitioning.
///
/// `Some(preserve_partitioning)` when convertible, `None` when not. Split out
/// of `convert` so the budget can ask the question without doing the work —
/// counting a join the rule will refuse is how the budget ends up tightening
/// against memory that never gets freed.
fn convertible_mode(hash_join: &HashJoinExec) -> Option<bool> {
    let single_partition = hash_join.left().output_partitioning().partition_count() == 1
        && hash_join.right().output_partitioning().partition_count() == 1;
    match hash_join.partition_mode() {
        PartitionMode::Partitioned => Some(true),
        PartitionMode::CollectLeft if single_partition => Some(false),
        _ => None,
    }
}

/// Convert hash joins whose estimated build side cannot fit the per-task
/// memory share into sort-merge joins, which can spill.
#[derive(Debug)]
pub struct SpillableJoinSelection {
    /// Build-size threshold in bytes; `None` disables the rule entirely
    /// (no memory cap → nothing to protect against).
    threshold_bytes: Option<u64>,
    /// Send oversized joins to [`crate::grace_hash_join`] instead of sort-merge.
    ///
    /// A field rather than an environment read at the point of use: the choice
    /// is then visible in the rule's own state, and a test can exercise both
    /// paths without mutating process-wide environment that every other test in
    /// the binary shares.
    grace: bool,
}

impl SpillableJoinSelection {
    /// Derive the threshold from the process's capacity decision, honouring
    /// [`SPILL_JOIN_BUILD_BYTES_ENV`].
    pub fn from_capacity() -> Self {
        let threshold_bytes = std::env::var(SPILL_JOIN_BUILD_BYTES_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .or_else(|| {
                // `query_memory_share_bytes`, not `min_task_memory_share_bytes`:
                // the per-slot division is real inside an executor and fiction
                // in an embedded process, which runs one query and owns the
                // whole pool. See `declare_single_query_process`.
                let share = krishiv_common::executor_capacity::ExecutorCapacity::detect_cached()
                    .query_memory_share_bytes()?;
                #[expect(
                    clippy::cast_precision_loss,
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "byte counts are far below f64's exact-integer range"
                )]
                Some((share as f64 * BUILD_FRACTION_OF_TASK_SHARE) as u64)
            });
        Self {
            threshold_bytes,
            // NOT grace-aware. `from_capacity` is reached from the coordinator's
            // planning context (`spill_join_build_bytes: None`), and a grace
            // join in a stage plan cannot be encoded, which collapses the whole
            // query to a single task. Grace is opted into explicitly by
            // `for_local_execution`, which only the post-decode executor path
            // calls.
            grace: false,
        }
    }

    /// Same threshold as [`Self::from_capacity`], but allowed to choose the
    /// grace hash join.
    ///
    /// Only for plans that are already decoded and will not be serialized —
    /// see `distributed_plan::apply_local_spill_strategy`.
    #[must_use]
    pub fn for_local_execution() -> Self {
        Self {
            grace: crate::grace_hash_join::enabled(),
            ..Self::from_capacity()
        }
    }

    /// The per-join threshold to actually apply, once the **total** unspillable
    /// build footprint of the plan is taken into account.
    ///
    /// `threshold` describes how much build memory a task can afford. It was
    /// being asked of each join *individually*, which is the wrong question:
    /// a hash join build side cannot spill, every join in a fragment holds its
    /// build side at once, and they all draw on one pool. TPC-H q10 at SF100
    /// planned **8 hash joins and converted 1** — the other 7 each sat under
    /// 250 MB and together exhausted a 2.6 GB pool, after which the next join
    /// was refused 877 bytes and the query died.
    ///
    /// So the budget applies to the sum. The **smallest** joins are retained as
    /// hash joins and everything from the first join that breaks the budget
    /// upward converts — the question is "which joins can we afford to leave
    /// un-spillable", and the cheapest ones are the ones worth keeping.
    ///
    /// (This paragraph said "largest joins convert first" for one revision after
    /// the code stopped doing that. The first implementation walked descending
    /// and returned the largest join's size — routinely *above* the configured
    /// threshold, so the "budget" loosened the rule instead of tightening it.
    /// The walk was fixed to ascending; the prose was not, and described an
    /// algorithm that no longer existed.)
    ///
    /// The decision is **per join**, keyed by position in `transform_up` order.
    ///
    /// An earlier version returned a single tightened threshold instead, which
    /// kept the existing mechanism but could not express two things:
    ///
    /// - **Equal-sized joins became all-or-nothing.** No one threshold can
    ///   retain two of three joins of identical size, so a plan whose joins tie
    ///   converted all of them once the sum broke the budget — over-converting
    ///   to the slower sort-merge plan. Ties are not exotic: sibling joins over
    ///   similarly-sized shuffle inputs estimate identically.
    /// - **The budget counted joins `convert` would refuse.** A join whose mode
    ///   is unconvertible holds its build side regardless, so the threshold
    ///   tightened against memory that was never going to be freed, converting
    ///   smaller joins while the real consumer stayed.
    ///
    /// Both were written off as needing node identity that `ExecutionPlan` does
    /// not offer. It does not offer *pointer* identity — `transform_up` rebuilds
    /// parents as their children change — but post-order **position** is stable,
    /// and that is all this needs.
    ///
    /// Unconvertible and unmeasurable joins are charged to the budget first,
    /// since they are retained no matter what. The remainder is spent on the
    /// candidates smallest-first: the question is "which joins can we afford to
    /// leave un-spillable", and the cheapest ones are the ones worth keeping.
    ///
    /// A strict generalisation: with no aggregate pressure every join is
    /// retained here and the per-join gate decides as it always did, so a plan
    /// that was fine before behaves identically — the q2 regression risk is
    /// unchanged.
    fn conversion_decisions(facts: &[JoinFacts], threshold: u64) -> Vec<bool> {
        let measured = facts
            .iter()
            .map(|f| f.retained_bytes())
            .fold(0u64, u64::saturating_add);
        let unknown_pressure = unknown_build_pressure(facts, threshold);
        let total = measured.saturating_add(unknown_pressure);
        // No aggregate pressure: let the per-join gate decide, as before.
        if total <= threshold {
            return vec![false; facts.len()];
        }

        // Joins the rule cannot convert are retained whatever we decide, so
        // their bytes come off the top rather than pretending they are
        // available to spend. An unmeasurable join is exactly that kind of
        // join — the per-join gate always keeps it — so its assumed share is
        // charged here too.
        let unavoidable = facts
            .iter()
            .filter(|f| !f.is_candidate())
            .map(|f| f.retained_bytes())
            .fold(0u64, u64::saturating_add)
            .saturating_add(unknown_pressure);
        let mut budget = threshold.saturating_sub(unavoidable);

        let mut candidates: Vec<(usize, u64)> = facts
            .iter()
            .enumerate()
            .filter(|(_, f)| f.is_candidate())
            .map(|(at, f)| (at, f.retained_bytes()))
            .collect();
        // Smallest first, and `sort_by_key` is stable, so equal sizes are
        // retained in plan order — deterministic rather than arbitrary.
        candidates.sort_by_key(|(_, bytes)| *bytes);

        let mut convert = vec![false; facts.len()];
        for (at, bytes) in candidates {
            if bytes <= budget {
                budget -= bytes;
            } else if let Some(slot) = convert.get_mut(at) {
                *slot = true;
            }
        }
        convert
    }

    /// Explicit threshold, for tests. Keeps the sort-merge conversion.
    #[must_use]
    pub fn with_threshold(threshold_bytes: Option<u64>) -> Self {
        Self {
            threshold_bytes,
            grace: false,
        }
    }

    /// Explicit threshold and algorithm, for tests.
    #[must_use]
    pub fn with_threshold_and_grace(threshold_bytes: Option<u64>, grace: bool) -> Self {
        Self {
            threshold_bytes,
            grace,
        }
    }


    /// Replace `hash_join` with the spilling grace hash join.
    ///
    /// No projection to restore and no sorts to insert: the operator keeps the
    /// original join whole and joins it a bucket at a time, so its type, filter,
    /// null equality and built-in projection come along unchanged. That is the
    /// whole reason to prefer it — `reapply_projection` exists only because
    /// `SortMergeJoinExec` drops the projection, and getting those indices wrong
    /// is what broke live q7/q8/q9.
    fn grace_join(
        &self,
        hash_join: &HashJoinExec,
        build_bytes: u64,
        threshold: u64,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // `builder()` clones the node; `reset_state()` drops the original's
        // collected build side and dynamic filter so the copy starts clean.
        let template = Arc::new(hash_join.builder().reset_state().build()?);
        let buckets = crate::grace_hash_join::bucket_count(build_bytes, threshold);
        let budget = usize::try_from(threshold).unwrap_or(usize::MAX);
        let grace = crate::grace_hash_join::GraceHashJoinExec::try_new(template, buckets, budget)?;
        tracing::info!(
            build_bytes,
            threshold,
            buckets,
            mode = ?hash_join.partition_mode(),
            join_type = ?hash_join.join_type(),
            "hash join build side exceeds per-task memory share; using grace hash join"
        );
        Ok(Arc::new(grace))
    }

    /// Restore `hash_join`'s built-in projection on top of `converted`.
    ///
    /// A no-op when the join carried none, which is the common case.
    fn reapply_projection(
        converted: Arc<dyn ExecutionPlan>,
        hash_join: &HashJoinExec,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;

        let Some(projection) = hash_join.projection.as_ref() else {
            return Ok(converted);
        };
        let schema = converted.schema();
        let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::with_capacity(projection.len());
        for &index in projection.iter() {
            let Some(field) = schema.fields().get(index) else {
                // The projection does not address this plan's schema after all.
                // Refusing here is safe: the caller keeps the hash join.
                return datafusion::error::Result::Err(
                    datafusion::error::DataFusionError::Plan(format!(
                        "spillable-join: projection index {index} is outside the \
                         converted join's {} columns",
                        schema.fields().len()
                    )),
                );
            };
            exprs.push((
                Arc::new(Column::new(field.name(), index)),
                field.name().clone(),
            ));
        }
        Ok(Arc::new(ProjectionExec::try_new(exprs, converted)?))
    }

    /// Whether this hash join should become a sort-merge join, and if so, the
    /// converted node.
    fn convert(
        &self,
        hash_join: &HashJoinExec,
        threshold: u64,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Gate 3: the join mode must be convertible to sort-merge.
        // `CollectLeft` is convertible exactly when the plan has one partition
        // — which, on this engine, is the *common* case rather than an edge
        // case. A task engine is built with
        // `target_partitions = cores / slots`, and a 3-core executor running 3
        // slots gets **1**. DataFusion never emits `Partitioned` at one
        // partition, so every join in every fragment was `CollectLeft`, and
        // this gate turned all of them away: the rule converted zero joins in
        // three hours across three executors while five SF100 queries died on
        // build sides it was written to rescue.
        //
        // The original premise — "CollectLeft build sides are small by
        // construction" — is false. `CollectLeft` is chosen from an *estimate*
        // and buffers the whole build side, so a wrong estimate makes it the
        // worst mode to be in, not the safest. q9 and q10 each took the entire
        // 797 MB pool this way.
        //
        // Sort-merge needs its inputs sorted on the join keys and co-located.
        // With one partition, "sorted" is the whole requirement — there is no
        // distribution to preserve — so the conversion is *simpler* here than
        // in the partitioned case, not riskier.
        let Some(preserve_partitioning) = convertible_mode(hash_join) else {
            tracing::debug!(
                mode = ?hash_join.partition_mode(),
                threshold,
                "spillable-join: join mode is not convertible"
            );
            return Ok(None);
        };
        // Gate 2: the build side must be *known* to be large. Absent statistics
        // keep hash join — guessing "big" is how the reverted session-wide
        // switch timed out q2.
        //
        // Logged, because a rule that silently declines is indistinguishable
        // from a rule that was never installed. Three SF100 queries died on
        // un-spillable hash joins while this rule sat registered and converted
        // nothing, and the logs could not say which gate turned each one away.
        let Some(build_bytes) = build_bytes_estimate(hash_join) else {
            tracing::debug!(
                threshold,
                "spillable-join: build-side size and row count both unknown, \
                 keeping hash join"
            );
            return Ok(None);
        };
        // No `build_bytes <= threshold` test here any more: whether this join
        // can be afforded is a whole-plan question, decided once in
        // `conversion_decisions` and passed in by the caller. Asking it again
        // per join is what let seven joins each sit under the threshold and
        // together exhaust the pool.

        // The build side is too big. Two ways to make it spill:
        //
        //   grace hash join — partition both sides by key and join bucket by
        //     bucket, each bucket an ordinary in-memory hash join. Nothing is
        //     sorted, and the buckets that would have fitted never touch disk.
        //   sort-merge — sort *both* sides in full, always. Correct, spillable,
        //     and the reason q2 went from 208 s to 1317 s.
        //
        // Grace is strictly the better trade when it applies, so it is tried
        // first; sort-merge remains the fallback for the shapes it refuses
        // (today: a broadcast join, whose sides have different partition
        // counts). Off by default — see `grace_hash_join::enabled`.
        if self.grace {
            match self.grace_join(hash_join, build_bytes, threshold) {
                Ok(converted) => return Ok(Some(converted)),
                Err(error) => tracing::debug!(
                    %error,
                    build_bytes,
                    "spillable-join: grace hash join declined; trying sort-merge"
                ),
            }
        }

        // Sort both sides on the join keys. Partition preservation follows the
        // mode decided above: keep it for `Partitioned` (so no exchange is
        // re-planned), drop it for single-partition `CollectLeft` (where there
        // is nothing to preserve).
        let on = hash_join.on();
        let left_keys: Vec<PhysicalSortExpr> = on
            .iter()
            .map(|(l, _)| PhysicalSortExpr::new_default(Arc::clone(l)))
            .collect();
        let right_keys: Vec<PhysicalSortExpr> = on
            .iter()
            .map(|(_, r)| PhysicalSortExpr::new_default(Arc::clone(r)))
            .collect();
        let (Some(left_ordering), Some(right_ordering)) = (
            LexOrdering::new(left_keys),
            LexOrdering::new(right_keys),
        ) else {
            return Ok(None);
        };
        let sort_options = left_ordering
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect();

        let sorted_left = Arc::new(
            SortExec::new(left_ordering, Arc::clone(hash_join.left()))
                .with_preserve_partitioning(preserve_partitioning),
        );
        let sorted_right = Arc::new(
            SortExec::new(right_ordering, Arc::clone(hash_join.right()))
                .with_preserve_partitioning(preserve_partitioning),
        );

        // Sort-merge materialises the filter's columns left-side-first
        // regardless of the order `column_indices` declares, so a filter that
        // names a right-side column first has to be permuted into that layout
        // or the batch will not match its own schema. See `left_first_filter` —
        // this is q17 and q19 at SF100.
        let filter = match hash_join.filter() {
            Some(filter) => match left_first_filter(filter) {
                Some(normalised) => Some(normalised),
                None => {
                    tracing::debug!(
                        "spillable-join: join filter cannot be reordered for sort-merge, \
                         keeping hash join"
                    );
                    return Ok(None);
                }
            },
            None => None,
        };

        // Let SortMergeJoinExec's own validation decide whether this join
        // shape (type, filter) is supported; on refusal, keep the hash join
        // rather than fail the query.
        match SortMergeJoinExec::try_new(
            sorted_left,
            sorted_right,
            on.to_vec(),
            filter,
            *hash_join.join_type(),
            sort_options,
            hash_join.null_equality(),
        ) {
            Ok(smj) => {
                // `HashJoinExec` has a built-in projection; `SortMergeJoinExec`
                // does not (there is a TODO to that effect in DataFusion's
                // source). Converting a projected join therefore silently
                // widens the output back to the full left++right schema, and
                // the *parent* join's positional `on` columns then point at the
                // wrong fields — live q7/q8/q9 failed with
                // `Missing on the right: Column { name: "o_custkey", index: 3 }`.
                //
                // Reproduce the projection explicitly. The join's projection
                // indices address the same full join schema `SortMergeJoinExec`
                // produces (DataFusion validates them against it with
                // `can_project(&join_schema, ..)`), so selecting those indices
                // off the converted join yields the identical output columns,
                // order and names.
                let converted = Self::reapply_projection(Arc::new(smj), hash_join)?;
                tracing::info!(
                    build_bytes,
                    threshold,
                    mode = ?hash_join.partition_mode(),
                    join_type = ?hash_join.join_type(),
                    projected = hash_join.contains_projection(),
                    "hash join build side exceeds per-task memory share; using sort-merge join"
                );
                Ok(Some(converted))
            }
            Err(error) => {
                tracing::debug!(%error, "sort-merge conversion declined; keeping hash join");
                Ok(None)
            }
        }
    }
}

impl PhysicalOptimizerRule for SpillableJoinSelection {
    fn name(&self) -> &str {
        "spillable_join_selection"
    }

    fn schema_check(&self) -> bool {
        // The conversion preserves the join's output schema exactly; sorts add
        // no columns.
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Gate 1: no cap, no change.
        let Some(configured) = self.threshold_bytes else {
            tracing::debug!("spillable-join: no memory cap configured, rule inactive");
            return Ok(plan);
        };
        // The budget is on the SUM of un-converted build sides, not on each one
        // separately — see `conversion_decisions`. Decisions are indexed by
        // position in `transform_up` order, which `collect_join_facts` mirrors.
        let mut facts = Vec::new();
        collect_join_facts(&plan, &mut facts);
        let decisions = Self::conversion_decisions(&facts, configured);
        let threshold = configured;
        let mut at = 0usize;
        let mut seen = 0usize;
        let mut converted = 0usize;
        let mut declined_on_error = 0usize;
        let out = plan
            .transform_up(|node| {
                // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
                let any = node.as_ref() as &dyn std::any::Any;
                let Some(hash_join) = any.downcast_ref::<HashJoinExec>() else {
                    return Ok(Transformed::no(node));
                };
                let index = at;
                at += 1;
                seen += 1;
                // Not chosen by the budget: leave it a hash join.
                //
                // `unwrap_or(false)` rather than a panic: if the two traversals
                // ever disagreed about how many joins exist, converting nothing
                // is the safe answer — this rule must never be the reason a
                // query fails.
                if !decisions.get(index).copied().unwrap_or(false) {
                    return Ok(Transformed::no(node));
                }
                // A rule that rewrites plans for *memory* reasons must never be
                // the reason a query fails. Live q7/q8/q9 turned an internal
                // refusal ("the left or right side of the join does not have
                // all columns on `on`") into a failed fragment, trading an
                // out-of-memory error for a planning error — strictly worse,
                // because the un-converted plan at least had a chance of
                // fitting. Declining is always available; erroring is not.
                match self.convert(hash_join, threshold) {
                    Ok(Some(plan)) => {
                        converted += 1;
                        Ok(Transformed::yes(plan))
                    }
                    Ok(None) => Ok(Transformed::no(node)),
                    Err(error) => {
                        declined_on_error += 1;
                        tracing::warn!(
                            %error,
                            mode = ?hash_join.partition_mode(),
                            join_type = ?hash_join.join_type(),
                            "spillable-join: conversion errored; keeping hash join"
                        );
                        Ok(Transformed::no(node))
                    }
                }
            })
            .map(|t| t.data)?;
        // One line that distinguishes "no hash joins in this plan", "joins seen
        // and left alone", and "rule not installed" — three states that were
        // previously identical from outside, which is what made three SF100
        // failures take a live investigation to attribute.
        if seen > 0 {
            // At info, not debug: executors run RUST_LOG=info, and the
            // debug-level version of this line was invisible in the only
            // environment that had the bug it was added to diagnose.
            tracing::info!(
                hash_joins = seen,
                converted,
                declined_on_error,
                configured_threshold = configured,
                chosen_by_budget = decisions.iter().filter(|d| **d).count(),
                unconvertible = facts.iter().filter(|f| !f.convertible).count(),
                unmeasurable = facts.iter().filter(|f| f.bytes.is_none()).count(),
                "spillable-join: pass complete"
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A session whose joins plan as `PartitionMode::Partitioned` even at test
    /// sizes — the mode the rule targets. Without forcing the thresholds down,
    /// DataFusion plans tiny joins as CollectLeft and the rule (correctly)
    /// declines, which makes the conversion test pass vacuously.
    fn partitioned_join_ctx() -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().optimizer.hash_join_single_partition_threshold = 0;
        config.options_mut().optimizer.hash_join_single_partition_threshold_rows = 0;
        SessionContext::new_with_config(config)
    }

    async fn joined_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE big AS SELECT v % 1000 AS k, v AS payload FROM (VALUES (1)) t(x), UNNEST(range(0, 20000)) AS u(v)")
            .await.unwrap().collect().await.unwrap();
        ctx.sql("CREATE TABLE small AS SELECT v AS k FROM (VALUES (1)) t(x), UNNEST(range(0, 100)) AS u(v)")
            .await.unwrap().collect().await.unwrap();
        ctx.sql("SELECT b.k, count(*) FROM big b JOIN small s ON b.k = s.k GROUP BY b.k")
            .await.unwrap().create_physical_plan().await.unwrap()
    }

    fn contains(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string()
            .contains(name)
    }

    /// With a threshold below every build side, partitioned hash joins become
    /// sort-merge joins — the conversion mechanics work end to end, and the
    /// converted plan still executes to the same answer.
    #[tokio::test]
    async fn an_oversized_build_side_converts_and_still_answers_correctly() {
        let ctx = partitioned_join_ctx();
        let plan = joined_plan(&ctx).await;
        assert!(contains(&plan, "HashJoinExec"), "precondition: hash join planned");
        assert!(
            contains(&plan, "mode=Partitioned"),
            "precondition: the join must be Partitioned or the rule correctly declines:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );

        let rule = SpillableJoinSelection::with_threshold(Some(1));
        let optimized = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();
        assert!(
            contains(&optimized, "SortMergeJoin"),
            "an over-threshold build side must convert:\n{}",
            datafusion::physical_plan::displayable(optimized.as_ref()).indent(true)
        );

        // A converted plan that returns different rows would be worse than the
        // failure it prevents. Baseline is planned afresh: the optimized tree
        // shares untransformed Arc subtrees with `plan`, and RepartitionExec
        // panics ("partition not used yet") if one instance is executed twice.
        let baseline_plan = ctx
            .sql("SELECT b.k, count(*) FROM big b JOIN small s ON b.k = s.k GROUP BY b.k")
            .await.unwrap().create_physical_plan().await.unwrap();
        let baseline =
            datafusion::physical_plan::collect(baseline_plan, ctx.task_ctx()).await.unwrap();
        let converted =
            datafusion::physical_plan::collect(optimized, ctx.task_ctx()).await.unwrap();
        let count = |bs: &[arrow::record_batch::RecordBatch]| -> usize {
            bs.iter().map(|b| b.num_rows()).sum()
        };
        assert_eq!(count(&baseline), count(&converted));
    }

    /// A build side comfortably under the threshold keeps its hash join. This
    /// is the q2 protection — the reverted session-wide switch failed exactly
    /// this property.
    #[tokio::test]
    async fn a_small_build_side_keeps_its_hash_join() {
        let ctx = partitioned_join_ctx();
        let plan = joined_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(Some(u64::MAX));
        let optimized = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();
        assert!(contains(&optimized, "HashJoinExec"), "under-threshold joins stay hash");
        assert!(!contains(&optimized, "SortMergeJoin"));
    }

    /// No memory cap means no threshold means no change — the embedded engine
    /// on a big machine must keep the fast path untouched.
    #[tokio::test]
    async fn no_cap_leaves_the_plan_alone() {
        let ctx = partitioned_join_ctx();
        let plan = joined_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(None);
        let optimized = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();
        assert!(contains(&optimized, "HashJoinExec"));
        assert!(!contains(&optimized, "SortMergeJoin"));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod row_count_fallback_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ColumnStatistics, Statistics};

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn stats_with(num_rows: Precision<usize>, columns: usize) -> Statistics {
        Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown(); columns],
        }
    }

    #[test]
    fn absent_rows_and_bytes_yields_no_estimate() {
        // The one case where the planner truly knows nothing: keep hash join
        // rather than guess. This is the guard against re-creating the
        // session-wide switch that timed q2 out.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Absent, 1), &s),
            None
        );
    }

    #[test]
    fn a_row_count_gives_an_estimate_when_byte_size_is_absent() {
        // The q9 case: rows known, bytes not. 1M rows x one 8-byte column.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Exact(1_000_000), 1), &s),
            Some(8_000_000)
        );
    }

    #[test]
    fn inexact_row_counts_count_too() {
        // Post-filter estimates are Inexact; refusing them would leave the
        // fallback inert on exactly the plans that need it.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Inexact(1_000), 1), &s),
            Some(8_000)
        );
    }

    #[test]
    fn varlen_columns_get_a_modest_assumed_width() {
        // Utf8 has no fixed width. The estimate must still produce something,
        // and must not be wild: one Int64 + one Utf8 = 8 + 32 per row.
        let s = schema(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let want = 100 * (8 + crate::join_estimates::ASSUMED_VARLEN_COLUMN_BYTES as u64);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Exact(100), 2), &s),
            Some(want)
        );
    }

    #[test]
    fn a_zero_row_build_side_estimates_zero_not_unknown() {
        // Zero rows must not be conflated with "unknown": an empty build side
        // is the strongest possible reason to keep the hash join, and a `None`
        // here would read as "no information" instead.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Exact(0), 1), &s),
            Some(0)
        );
    }

    #[test]
    fn a_huge_row_count_does_not_overflow_into_a_small_estimate() {
        // Saturating arithmetic: an absurd row count must stay absurd rather
        // than wrap around to something that looks like it fits.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        let est = estimated_build_bytes_from_rows(&stats_with(Precision::Exact(usize::MAX), 1), &s)
            .expect("a known row count always yields an estimate");
        assert!(est > u64::from(u32::MAX), "estimate collapsed to {est}");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod collect_left_tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A session that plans exactly the way a task engine does on a saturated
    /// executor: one target partition, because `cores / slots` is 1. This is
    /// the configuration in which every join is `CollectLeft` — the shape the
    /// rule used to skip entirely.
    fn single_partition_ctx() -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1))
    }

    fn shows(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        displayable(plan.as_ref()).indent(true).to_string().contains(name)
    }

    async fn one_partition_join_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE l(k INT, v INT) AS VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE r(k INT, w INT) AS VALUES (1, 100), (2, 200)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_single_partition_plan_really_does_produce_collect_left() {
        // Pins the premise the fix rests on. If DataFusion ever stops choosing
        // CollectLeft at one partition, the tests below stop testing anything
        // and this one says so first.
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        assert!(
            shows(&plan, "CollectLeft"),
            "expected CollectLeft at target_partitions=1, got:\n{}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn a_large_collect_left_join_becomes_sort_merge() {
        // The regression that mattered: with a threshold below the build side,
        // the rule must now convert. Before this fix it returned the plan
        // untouched no matter how large the build side was.
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(Some(1));
        let out = rule.optimize(plan, ctx.copied_config().options()).unwrap();
        assert!(
            shows(&out, "SortMergeJoin"),
            "CollectLeft join was not converted:\n{}",
            displayable(out.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn a_small_collect_left_join_is_left_alone() {
        // Hash join is the right algorithm when it fits; the fix must not
        // convert everything just because it now *can*.
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(Some(64 * 1024 * 1024));
        let out = rule.optimize(plan, ctx.copied_config().options()).unwrap();
        assert!(shows(&out, "HashJoin"), "small join should stay a hash join");
        assert!(!shows(&out, "SortMergeJoin"));
    }

    #[tokio::test]
    async fn the_converted_plan_returns_the_same_rows() {
        // A spillable plan that answers differently is not a fix. Compare the
        // converted plan's output against the hash-join plan's.
        use datafusion::physical_plan::collect;
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();

        let hash_rows = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await.unwrap();
        let converted = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert!(shows(&converted, "SortMergeJoin"));
        let smj_rows = collect(converted, task_ctx).await.unwrap();

        let total = |b: &[arrow::array::RecordBatch]| -> usize {
            b.iter().map(arrow::array::RecordBatch::num_rows).sum()
        };
        assert_eq!(total(&hash_rows), total(&smj_rows), "row count changed");
        assert_eq!(total(&smj_rows), 2, "expected the two matching keys");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod never_fails_the_query_tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A plan whose joins the rule will want to convert.
    async fn joined_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE a(k INT, v INT) AS VALUES (1, 1), (2, 2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE b(k INT, w INT) AS VALUES (1, 9)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE c(k INT, z INT) AS VALUES (1, 5)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Two stacked joins: `transform_up` converts the inner one first, so
        // the outer one is asked about a child the rule already rewrote — the
        // shape that produced the live failure.
        ctx.sql("SELECT a.v, b.w, c.z FROM a JOIN b ON a.k = b.k JOIN c ON a.k = c.k")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn stacked_joins_never_make_the_rule_return_an_error() {
        // The live regression: q7/q8/q9 stopped failing with "Resources
        // exhausted" and started failing with
        // `spillable_join_selection / Error during planning: The left or right
        // side of the join does not have all columns on "on"`. Trading an
        // out-of-memory error for a planning error is strictly worse — the
        // un-converted plan at least had a chance of fitting. This rule is an
        // optimisation and must always be able to decline.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options());
        assert!(
            out.is_ok(),
            "the rule must never fail a plan; got {:?}",
            out.err()
        );
    }

    #[tokio::test]
    async fn a_plan_the_rule_declines_is_returned_unchanged_and_still_runs() {
        use datafusion::physical_plan::collect;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let before = displayable(plan.as_ref()).indent(true).to_string();
        let task_ctx = ctx.task_ctx();

        // Threshold far above anything here: every join declines.
        let out = SpillableJoinSelection::with_threshold(Some(1 << 40))
            .optimize(Arc::clone(&plan), ctx.copied_config().options())
            .unwrap();
        assert_eq!(
            before,
            displayable(out.as_ref()).indent(true).to_string(),
            "declining must leave the plan untouched"
        );
        let rows = collect(out, task_ctx).await.unwrap();
        let total: usize = rows.iter().map(arrow::array::RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "the declined plan must still produce the join result");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod budget_tests {
    use super::*;

    /// Facts as the rule actually sees them.
    ///
    /// Arrow rounds buffer allocations, so a source built to "look like" 200
    /// bytes reports ~296. Asserting on nominal sizes tested the allocator;
    /// these tests measure first and assert the *invariant*.
    fn facts(plan: &Arc<dyn ExecutionPlan>) -> Vec<JoinFacts> {
        let mut out = Vec::new();
        collect_join_facts(plan, &mut out);
        out
    }

    fn sizes_of(facts: &[JoinFacts]) -> Vec<u64> {
        facts.iter().map(|f| f.retained_bytes()).collect()
    }

    /// Bytes left un-converted under `decisions`.
    fn retained(facts: &[JoinFacts], decisions: &[bool]) -> u64 {
        facts
            .iter()
            .zip(decisions)
            .filter(|(_, convert)| !**convert)
            .map(|(f, _)| f.retained_bytes())
            .fold(0, u64::saturating_add)
    }

    /// The threshold is a budget on the SUM, not a per-join allowance.
    ///
    /// q10's SF100 shape: several joins that each fit comfortably and together
    /// do not. Under the old per-join rule every one of these is under the
    /// budget, nothing converts, and the pool is exhausted at run time.
    #[test]
    fn joins_that_each_fit_but_together_do_not_are_converted() {
        let plan = plan_with_build_sizes(&[200; 8]);
        let facts = facts(&plan);
        let sizes = sizes_of(&facts);
        let largest = *sizes.iter().max().expect("fixture has joins");
        let total: u64 = sizes.iter().copied().fold(0, u64::saturating_add);

        // A budget every join fits under individually, that the sum exceeds —
        // exactly the state q10 was in.
        let budget = largest;
        assert!(
            total > budget,
            "fixture must create aggregate pressure: total {total} vs budget {budget}"
        );

        let decisions = SpillableJoinSelection::conversion_decisions(&facts, budget);
        assert!(
            retained(&facts, &decisions) <= budget,
            "the un-converted sum {} exceeds the {budget} budget (sizes {sizes:?})",
            retained(&facts, &decisions)
        );
    }

    /// No aggregate pressure → nothing is chosen for conversion, so a plan that
    /// behaved acceptably before behaves identically. This bounds the q2
    /// regression risk: converting more joins than necessary is what made q2 6x
    /// slower, and this only acts under real pressure.
    #[test]
    fn without_pressure_nothing_is_chosen() {
        let plan = plan_with_build_sizes(&[50, 60]);
        let facts = facts(&plan);
        let total: u64 = sizes_of(&facts).iter().copied().fold(0, u64::saturating_add);
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, total + 1);
        assert!(
            decisions.iter().all(|convert| !convert),
            "a total that fits the budget must convert nothing: {decisions:?}"
        );
    }

    /// One oversized join still converts on its own, as before.
    #[test]
    fn a_single_oversized_join_still_converts() {
        let plan = plan_with_build_sizes(&[900]);
        let facts = facts(&plan);
        let largest = *sizes_of(&facts).iter().max().expect("fixture has a join");
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, largest - 1);
        assert_eq!(decisions, vec![true], "an over-budget join must convert");
    }

    /// **q21 at SF100.** Joins whose build side cannot be estimated used to sum
    /// to zero, so the aggregate check concluded there was no pressure and
    /// converted nothing — then the pool was exhausted at run time by the very
    /// joins it had valued at nothing.
    ///
    /// The measured failure: a join asking for its FIRST 310.2 MB found 87.9 MB
    /// left of a 2.6 GB pool, its siblings already holding the rest.
    #[test]
    fn unmeasurable_joins_still_create_aggregate_pressure() {
        // Four joins whose size the planner cannot estimate at all.
        let facts = vec![
            JoinFacts { bytes: None, convertible: true },
            JoinFacts { bytes: None, convertible: true },
            JoinFacts { bytes: None, convertible: true },
            JoinFacts { bytes: Some(900), convertible: true },
        ];
        let threshold = 1000;
        // Before the fix `total` was 900, which fits 1000, so this returned all
        // false and the query died. The unknowns are now charged a share each.
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, threshold);
        assert!(
            decisions.iter().any(|convert| *convert),
            "unmeasurable joins must count as pressure, or the budget is blind \
             to exactly the joins it exists to bound: {decisions:?}"
        );
    }

    /// The other half of the tension, and the one that must not regress:
    /// **a plan whose joins are all measurable is completely untouched.**
    ///
    /// Guessing "big" for every join is the session-wide `prefer_hash_join =
    /// false` switch that took q2 from 189 s past a 2400 s timeout. The new
    /// pressure term returns zero when nothing is unmeasurable, so such a plan
    /// still short-circuits to the per-join gate exactly as before.
    #[test]
    fn measurable_plans_are_unaffected_by_the_unknown_pressure_term() {
        let facts = vec![
            JoinFacts { bytes: Some(50), convertible: true },
            JoinFacts { bytes: Some(60), convertible: true },
        ];
        assert_eq!(
            super::unknown_build_pressure(&facts, 1000),
            0,
            "no unknowns means no assumed pressure"
        );
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, 1000);
        assert!(
            decisions.iter().all(|convert| !convert),
            "a fully measurable plan under budget must convert nothing: {decisions:?}"
        );
    }

    /// The assumed share is per join, not per plan: one unknown among many
    /// small joins is not enough to force conversion on its own.
    #[test]
    fn a_single_unknown_among_many_does_not_force_conversion() {
        let mut facts: Vec<JoinFacts> = (0..9)
            .map(|_| JoinFacts { bytes: Some(1), convertible: true })
            .collect();
        facts.push(JoinFacts { bytes: None, convertible: true });
        // 10 joins, threshold 1000 -> one unknown is charged 100; 9 + 100 fits.
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, 1000);
        assert!(
            decisions.iter().all(|convert| !convert),
            "one unknown among ten small joins is not aggregate pressure: {decisions:?}"
        );
    }

    /// The boundary the pressure term cannot cross, pinned so nobody "fixes"
    /// it into converting joins the rest of the rule will refuse anyway.
    ///
    /// With every join unmeasurable the term sums to exactly `threshold`
    /// (n shares of `threshold / n`), `total <= threshold` short-circuits, and
    /// no join is chosen. That is correct: gate 2 keeps an unmeasurable join's
    /// hash join unconditionally and `is_candidate` requires `bytes.is_some()`,
    /// so a "convert" decision here would be silently ignored downstream.
    ///
    /// This is the shape TPC-H q21 was in when this term was written *for* it —
    /// all 414 spillable-join passes at SF100 reported
    /// `unmeasurable == hash_joins` because a declared primary key had disabled
    /// the tables' statistics. The term could not have helped, and did not. The
    /// fix belonged at the statistics layer.
    #[test]
    fn an_all_unknown_plan_is_beyond_the_budgets_reach() {
        let facts: Vec<JoinFacts> = (0..4)
            .map(|_| JoinFacts { bytes: None, convertible: true })
            .collect();
        assert_eq!(
            super::unknown_build_pressure(&facts, 1000),
            1000,
            "four unknowns each charged threshold/4 sum to the whole threshold"
        );
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, 1000);
        assert!(
            decisions.iter().all(|convert| !convert),
            "an all-unknown plan has no candidate to convert: {decisions:?}"
        );
    }

    /// A plan with no hash joins must decide nothing, and must not panic on the
    /// empty path.
    #[test]
    fn a_plan_without_joins_is_left_alone() {
        let plan = plan_with_build_sizes(&[]);
        let facts = facts(&plan);
        assert!(facts.is_empty());
        assert!(SpillableJoinSelection::conversion_decisions(&facts, 250).is_empty());
    }

    /// Equal-sized joins are no longer all-or-nothing.
    ///
    /// This is the regression test for a real limitation that was documented
    /// and left in place for one revision: a single tightened threshold cannot
    /// separate joins of identical size, so three equal joins under a budget
    /// that fits two converted **all three** — over-converting to the slower
    /// sort-merge plan. Ties are not exotic; sibling joins over similarly-sized
    /// shuffle inputs estimate identically.
    ///
    /// Deciding per join by post-order position fixes it: exactly the one join
    /// that does not fit converts.
    #[test]
    fn equal_sized_joins_are_decided_individually() {
        let plan = plan_with_build_sizes(&[100, 100, 100]);
        let facts = facts(&plan);
        let sizes = sizes_of(&facts);
        let one = sizes.first().copied().expect("fixture has joins");
        // Room for exactly two of the three.
        let budget = one * 2;

        let decisions = SpillableJoinSelection::conversion_decisions(&facts, budget);
        assert_eq!(
            decisions.iter().filter(|convert| **convert).count(),
            1,
            "exactly one of three equal joins should convert, not all of them: \
             {decisions:?} (sizes {sizes:?}, budget {budget})"
        );
        assert!(
            retained(&facts, &decisions) <= budget,
            "the retained set must still fit the budget"
        );
    }

    /// A join the rule cannot convert must not be counted as spendable.
    ///
    /// Its build side is held whatever the budget decides, so charging it to
    /// the budget first is the difference between converting the joins that
    /// will actually free memory and converting smaller ones while the real
    /// consumer stays put.
    #[test]
    fn unconvertible_joins_are_charged_to_the_budget_first() {
        let big_unconvertible = JoinFacts { bytes: Some(100), convertible: false };
        let small_candidate = JoinFacts { bytes: Some(30), convertible: true };
        let facts = [big_unconvertible, small_candidate];

        // 100 is already spent by the join that cannot convert, so the 30-byte
        // candidate does not fit in the remaining 20 and must convert.
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, 120);
        assert_eq!(
            decisions,
            vec![false, true],
            "the unconvertible join stays (it must), and the candidate converts \
             because the budget it draws on is what is left after it"
        );
    }

    /// A join whose size is unknown keeps its hash join at the per-join gate, so
    /// it is not a candidate and cannot be chosen for conversion.
    #[test]
    fn unmeasurable_joins_are_never_chosen() {
        let facts = [
            JoinFacts { bytes: None, convertible: true },
            JoinFacts { bytes: Some(500), convertible: true },
        ];
        let decisions = SpillableJoinSelection::conversion_decisions(&facts, 10);
        assert_eq!(decisions, vec![false, true]);
    }

    /// Build a plan whose hash joins report the given build-side byte sizes.
    ///
    /// `effective_threshold` reads estimates through `partition_statistics`, so
    /// the sizes have to come from real statistics rather than being injected.
    /// `MemoryExec` over batches of a known width gives that.
    pub(super) fn plan_with_build_sizes(sizes: &[u64]) -> Arc<dyn ExecutionPlan> {
        // Built directly rather than planned from SQL: the point is to control
        // the build estimates exactly, and a planner is free to reorder joins.
        let mut plan: Arc<dyn ExecutionPlan> = sized_source(1);
        for size in sizes {
            plan = Arc::new(
                HashJoinExec::try_new(
                    sized_source(*size),
                    Arc::clone(&plan),
                    vec![(
                        Arc::new(datafusion::physical_expr::expressions::Column::new("k", 0)),
                        Arc::new(datafusion::physical_expr::expressions::Column::new("k", 0)),
                    )],
                    None,
                    &datafusion::common::JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    datafusion::common::NullEquality::NullEqualsNothing,
                    false,
                )
                .expect("hash join"),
            );
        }
        plan
    }

    /// A single-column source whose statistics report `bytes` total.
    pub(super) fn sized_source(bytes: u64) -> Arc<dyn ExecutionPlan> {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        // 4 bytes per Int32 row, so `bytes / 4` rows reports ~`bytes`.
        let rows = usize::try_from(bytes / 4).unwrap_or(1).max(1);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![0; rows]))],
        )
        .expect("batch");
        datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )
        .expect("memory exec")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod projection_tests {
    use super::*;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// Two stacked joins where the inner one projects a subset of its columns.
    /// This is q10's shape: `customer JOIN orders JOIN lineitem`, where the
    /// middle join carries a projection and the outer join's `on` addresses
    /// its output positionally.
    async fn stacked_projected_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        for ddl in [
            "CREATE TABLE c(c_custkey INT, c_name VARCHAR) AS VALUES (1, 'a'), (2, 'b')",
            "CREATE TABLE o(o_orderkey INT, o_custkey INT, o_total INT) AS VALUES (10, 1, 5)",
            "CREATE TABLE l(l_orderkey INT, l_qty INT) AS VALUES (10, 3)",
        ] {
            ctx.sql(ddl).await.unwrap().collect().await.unwrap();
        }
        ctx.sql(
            "SELECT c.c_name, l.l_qty \
             FROM c JOIN o ON c.c_custkey = o.o_custkey \
                    JOIN l ON o.o_orderkey = l.l_orderkey",
        )
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
    }

    /// Number of `SortMergeJoinExec` nodes anywhere in `plan`.
    ///
    /// The precondition every test below depends on. `convert` has six ways to
    /// decline (mode, absent statistics, build side under threshold, no sort
    /// keys, `SortMergeJoinExec::try_new` refusing, projection out of range),
    /// and every one of them returns the plan **unchanged** — which passes an
    /// assertion that the output still matches the input. Without this count, a
    /// rule that silently stopped converting would leave the whole module
    /// green.
    fn sort_merge_join_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
        let any = plan.as_ref() as &dyn std::any::Any;
        let here = usize::from(any.downcast_ref::<SortMergeJoinExec>().is_some());
        here + plan
            .children()
            .iter()
            .map(|c| sort_merge_join_count(c))
            .sum::<usize>()
    }

    /// Whether any `HashJoinExec` in `plan` carries a built-in projection —
    /// the condition `reapply_projection` exists for.
    fn has_projected_hash_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let any = plan.as_ref() as &dyn std::any::Any;
        any.downcast_ref::<HashJoinExec>()
            .is_some_and(HashJoinExec::contains_projection)
            || plan.children().iter().any(|c| has_projected_hash_join(c))
    }

    /// Every cell, row-sorted — not a row count.
    fn cells(batches: &[arrow::array::RecordBatch]) -> Vec<String> {
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |r| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c), r)
                                .expect("cell")
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
            })
            .collect();
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn converting_a_projected_join_keeps_the_output_columns() {
        // The live failure: converting a join that carries a projection widened
        // its output back to the full left++right schema, so the parent join's
        // positional `on` broke with
        // `Missing on the right: Column { name: "o_custkey", index: 3 }`.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = stacked_projected_plan(&ctx).await;
        let before_schema = plan.schema();
        assert!(
            has_projected_hash_join(&plan),
            "fixture must build a hash join carrying a projection, or this tests nothing:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(Arc::clone(&plan), ctx.copied_config().options())
            .expect("the rule must not fail the plan");

        assert!(
            sort_merge_join_count(&out) > 0,
            "the rule declined, so the conversion under test never ran:\n{}",
            displayable(out.as_ref()).indent(true)
        );
        assert_eq!(
            out.schema(),
            before_schema,
            "conversion changed the plan's output schema:\n{}",
            displayable(out.as_ref()).indent(true)
        );
    }

    /// The values, not the shape.
    ///
    /// `reapply_projection` re-indexes the join's projection against the
    /// *converted* join's schema and takes each output column's name from that
    /// schema too. If those indices ever addressed different columns, the names
    /// and types would still line up — they are read from the same place the
    /// data is — so a schema comparison, a row count and a column count would
    /// all agree while every value was wrong. Only comparing cells catches it.
    #[tokio::test]
    async fn the_converted_projected_plan_returns_the_same_values() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = stacked_projected_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        assert!(has_projected_hash_join(&plan), "fixture must project");

        let before = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await.unwrap();
        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert!(
            sort_merge_join_count(&out) > 0,
            "the rule declined, so the conversion under test never ran"
        );
        let after = collect(out, task_ctx).await.unwrap();

        assert_eq!(cells(&before), cells(&after), "converted plan changed the data");
        assert_eq!(cells(&after), vec![String::from("a|3")], "expected the single matching row");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod grace_tests {
    use super::*;
    use crate::grace_hash_join::GraceHashJoinExec;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};

    async fn joined_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE l(k INT, v INT) AS VALUES (1, 10), (2, 20), (3, 30)")
            .await.unwrap().collect().await.unwrap();
        ctx.sql("CREATE TABLE r(k INT, w INT) AS VALUES (1, 100), (2, 200), (2, 201)")
            .await.unwrap().collect().await.unwrap();
        ctx.sql("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k")
            .await.unwrap().create_physical_plan().await.unwrap()
    }

    fn grace_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
        // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
        let any = plan.as_ref() as &dyn std::any::Any;
        usize::from(any.downcast_ref::<GraceHashJoinExec>().is_some())
            + plan.children().iter().map(|c| grace_joins(c)).sum::<usize>()
    }

    /// With the flag on, an oversized build side becomes a grace hash join
    /// rather than a sort-merge join.
    #[tokio::test]
    async fn an_oversized_join_becomes_a_grace_hash_join_when_enabled() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let out = SpillableJoinSelection::with_threshold_and_grace(Some(1), true)
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert_eq!(
            grace_joins(&out),
            1,
            "expected a grace hash join:\n{}",
            displayable(out.as_ref()).indent(true)
        );
        assert!(
            !displayable(out.as_ref()).indent(true).to_string().contains("SortMergeJoin"),
            "grace should have been preferred over sort-merge"
        );
    }

    /// The flag defaults off, so today's deployed behaviour is untouched: the
    /// same plan still converts to sort-merge.
    #[tokio::test]
    async fn with_the_flag_off_the_sort_merge_conversion_is_unchanged() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert_eq!(grace_joins(&out), 0, "the flag is off; no grace join should appear");
        assert!(
            displayable(out.as_ref()).indent(true).to_string().contains("SortMergeJoin"),
            "the sort-merge path must still work"
        );
    }

    /// The substituted plan answers identically. A join that spills but returns
    /// different rows is worse than the failure it prevents.
    #[tokio::test]
    async fn the_grace_plan_returns_the_same_rows() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        let baseline = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await.unwrap();

        let out = SpillableJoinSelection::with_threshold_and_grace(Some(1), true)
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert_eq!(grace_joins(&out), 1, "the rule declined; this proved nothing");
        let converted = collect(out, task_ctx).await.unwrap();

        let cells = |bs: &[arrow::array::RecordBatch]| -> Vec<String> {
            let mut rows: Vec<String> = bs
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(b.column(c), r)
                                    .expect("cell")
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(cells(&baseline), cells(&converted));
        assert_eq!(cells(&converted), vec!["10|100", "20|200", "20|201"]);
    }

    /// A shape the grace join refuses must fall back, not fail the query. The
    /// rule's contract is that it can always decline.
    #[tokio::test]
    async fn a_refused_shape_falls_back_instead_of_failing() {
        // Two partitions on one side and one on the other is the broadcast
        // shape `GraceHashJoinExec::try_new` rejects.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let plan = joined_plan(&ctx).await;
        let out = SpillableJoinSelection::with_threshold_and_grace(Some(1), true)
            .optimize(Arc::clone(&plan), ctx.copied_config().options());
        assert!(out.is_ok(), "a refusal must never fail the plan: {:?}", out.err());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod join_filter_order_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinSide;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_plan::joins::utils::ColumnIndex;

    /// A filter naming the right side first — the shape q17 and q19 produce.
    /// Intermediate schema is `[q: Decimal (right), b: Utf8 (left)]`.
    fn right_first() -> JoinFilter {
        let schema = Arc::new(Schema::new(vec![
            Field::new("q", DataType::Decimal128(15, 2), true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("q", 0)),
            datafusion::logical_expr::Operator::Lt,
            Arc::new(Column::new("b", 1)),
        ));
        JoinFilter::new(
            expression,
            vec![
                ColumnIndex { index: 0, side: JoinSide::Right },
                ColumnIndex { index: 0, side: JoinSide::Left },
            ],
            schema,
        )
    }

    /// Sort-merge materialises `[all left] ++ [all right]`, so the normalised
    /// filter must declare exactly that order.
    #[test]
    fn a_right_first_filter_is_reordered_to_left_first() {
        let out = left_first_filter(&right_first()).expect("normalisable");
        assert_eq!(
            out.column_indices()
                .iter()
                .map(|c| c.side)
                .collect::<Vec<_>>(),
            vec![JoinSide::Left, JoinSide::Right],
        );
        assert_eq!(
            out.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["b".to_string(), "q".to_string()],
            "the intermediate schema must follow the new column order"
        );
    }

    /// Reordering the schema without re-pointing the expression would leave a
    /// filter that reads the wrong columns — the same class of silent wrong
    /// answer, just moved. `q` was at 0 and must now be at 1.
    #[test]
    fn the_expression_is_repointed_at_the_new_positions() {
        let out = left_first_filter(&right_first()).expect("normalisable");
        let rendered = format!("{}", out.expression());
        assert!(
            rendered.contains("q@1") && rendered.contains("b@0"),
            "expression still points at the old positions: {rendered}"
        );
    }

    /// A filter already in left-first order is returned unchanged, so the
    /// common case costs nothing and cannot be perturbed.
    #[test]
    fn an_already_left_first_filter_is_untouched() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("q", DataType::Decimal128(15, 2), true),
        ]));
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 0)),
            datafusion::logical_expr::Operator::Lt,
            Arc::new(Column::new("q", 1)),
        ));
        let filter = JoinFilter::new(
            expression,
            vec![
                ColumnIndex { index: 0, side: JoinSide::Left },
                ColumnIndex { index: 0, side: JoinSide::Right },
            ],
            schema,
        );
        let out = left_first_filter(&filter).expect("normalisable");
        assert_eq!(format!("{}", out.expression()), format!("{}", filter.expression()));
        assert_eq!(out.column_indices(), filter.column_indices());
    }

    /// Interleaved sides keep their relative order within each side — that is
    /// what `get_filter_columns` produces, and anything else would mis-map.
    #[test]
    fn relative_order_within_each_side_is_preserved() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("r0", DataType::Int32, true),
            Field::new("l0", DataType::Int32, true),
            Field::new("r1", DataType::Int32, true),
            Field::new("l1", DataType::Int32, true),
        ]));
        let filter = JoinFilter::new(
            Arc::new(Column::new("l1", 3)),
            vec![
                ColumnIndex { index: 7, side: JoinSide::Right },
                ColumnIndex { index: 5, side: JoinSide::Left },
                ColumnIndex { index: 9, side: JoinSide::Right },
                ColumnIndex { index: 6, side: JoinSide::Left },
            ],
            schema,
        );
        let out = left_first_filter(&filter).expect("normalisable");
        assert_eq!(
            out.column_indices()
                .iter()
                .map(|c| (c.side, c.index))
                .collect::<Vec<_>>(),
            vec![
                (JoinSide::Left, 5),
                (JoinSide::Left, 6),
                (JoinSide::Right, 7),
                (JoinSide::Right, 9),
            ],
        );
        // l1 was the 4th column (index 3) and is now the 2nd (index 1).
        assert_eq!(format!("{}", out.expression()), "l1@1");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod encodability_tests {
    use super::*;
    use crate::grace_hash_join::GraceHashJoinExec;
    use datafusion::prelude::SessionContext;

    fn grace_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let any = plan.as_ref() as &dyn std::any::Any;
        usize::from(any.downcast_ref::<GraceHashJoinExec>().is_some())
            + plan.children().iter().map(|c| grace_joins(c)).sum::<usize>()
    }

    /// The staging planner must never produce a grace hash join.
    ///
    /// `GraceHashJoinExec` is a Krishiv node and `datafusion-proto` cannot
    /// serialize it. A stage plan containing one fails to encode, and the
    /// scheduler's response to an unencodable stage plan is to run the whole
    /// query as a SINGLE TASK — so the flag read as a memory fix while silently
    /// un-distributing q10 and q21 on the cluster:
    ///
    /// ```text
    /// stage plan cannot be encoded and decoded; running this query as a
    /// SINGLE TASK ... Unsupported plan and extension codec failed
    /// ```
    ///
    /// Grace belongs on the executor, after decode
    /// (`distributed_plan::apply_local_spill_strategy`). This pins the
    /// separation: whatever the environment says, the path that plans stages
    /// stays encodable.
    #[tokio::test]
    async fn the_staging_planner_never_emits_an_unencodable_grace_join() {
        // Exactly how `planning_session_context_with_options` builds its rules.
        let rule = SpillableJoinSelection::with_threshold(Some(1));
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(1),
        );
        for ddl in [
            "CREATE TABLE l(k INT, v INT) AS VALUES (1, 10), (2, 20)",
            "CREATE TABLE r(k INT, w INT) AS VALUES (1, 100), (2, 200)",
        ] {
            ctx.sql(ddl).await.unwrap().collect().await.unwrap();
        }
        let plan = ctx
            .sql("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let out = rule.optimize(plan, ctx.copied_config().options()).unwrap();
        assert_eq!(
            grace_joins(&out),
            0,
            "the staging planner produced a grace join, which cannot be encoded:\n{}",
            datafusion::physical_plan::displayable(out.as_ref()).indent(true)
        );
        // And it did convert *something*, so this is not passing because the
        // rule declined everything.
        assert!(
            datafusion::physical_plan::displayable(out.as_ref())
                .indent(true)
                .to_string()
                .contains("SortMergeJoin"),
            "the rule declined entirely, so encodability was never at stake"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod budget_never_loosens_tests {
    use super::*;

    use super::budget_tests::plan_with_build_sizes;

    fn facts_of(plan: &Arc<dyn ExecutionPlan>) -> Vec<JoinFacts> {
        let mut out = Vec::new();
        collect_join_facts(plan, &mut out);
        out
    }

    fn retained(facts: &[JoinFacts], decisions: &[bool]) -> u64 {
        facts
            .iter()
            .zip(decisions)
            .filter(|(_, convert)| !**convert)
            .map(|(f, _)| f.retained_bytes())
            .fold(0, u64::saturating_add)
    }

    /// The budget may only ever convert MORE, never fewer.
    ///
    /// The first implementation returned the largest join's size as a new
    /// threshold — routinely far above the configured one — so it converted
    /// only the single biggest join where the plain per-join rule would have
    /// converted every join over budget. Live on TPC-H q10:
    ///
    /// ```text
    /// threshold=974064839  configured_threshold=250000000  budget_tightened=false
    /// ```
    ///
    /// The budget exists to convert more under aggregate pressure; loosening
    /// inverted it, and is why that fix never moved q10 or q11. Now that the
    /// decision is per join there is no threshold to invert, but the property
    /// it was protecting still has to hold.
    #[test]
    fn everything_over_the_configured_threshold_still_converts() {
        for configured in [1_u64, 1_000, 250_000_000] {
            let plan = plan_with_build_sizes(&[900, 400, 300]);
            let facts = facts_of(&plan);
            let decisions = SpillableJoinSelection::conversion_decisions(&facts, configured);
            let converted = decisions.iter().filter(|convert| **convert).count();
            let would_have = facts
                .iter()
                .filter(|f| f.retained_bytes() > configured)
                .count();
            assert!(
                converted >= would_have,
                "the budget converted {converted} joins where the plain threshold \
                 would have converted {would_have} (configured {configured})"
            );
        }
    }

    /// The point of the budget: what is LEFT as hash joins must fit it.
    ///
    /// This is the property q10 needed and never had. Several joins that each
    /// sit under the threshold, whose sum does not — the retained set has to
    /// come in under budget, or the pool is exhausted at run time exactly as
    /// before.
    #[test]
    fn the_joins_left_as_hash_joins_fit_the_budget() {
        let plan = plan_with_build_sizes(&[200; 8]);
        let facts = facts_of(&plan);
        let total: u64 = facts
            .iter()
            .map(|f| f.retained_bytes())
            .fold(0, u64::saturating_add);
        let largest = facts
            .iter()
            .map(|f| f.retained_bytes())
            .max()
            .expect("fixture has joins");
        // A budget every join clears individually, that the sum does not.
        let budget = largest * 2;
        assert!(total > budget, "fixture must create aggregate pressure");

        let decisions = SpillableJoinSelection::conversion_decisions(&facts, budget);
        assert!(
            retained(&facts, &decisions) <= budget,
            "un-converted joins sum to {}, over the {budget} budget",
            retained(&facts, &decisions)
        );
    }
}
