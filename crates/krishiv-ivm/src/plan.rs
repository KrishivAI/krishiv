#![forbid(unsafe_code)]

//! Incremental execution plan for IVM views.
//!
//! `build_view_plan` parses a view's SQL via DataFusion and attempts to
//! pattern-match an O(Δ) execution strategy. Falls back to `ViewPlan::DiffBased`
//! for any SQL pattern that cannot be lowered to a known incremental operator.
//!
//! # Supported patterns (O(Δ))
//! - Single-source GROUP BY aggregate → `IncrementalAggOp`
//! - Two-source INNER / LEFT OUTER equi-JOIN → `IncrementalJoinOp` (bilinear
//!   probe), including a `WHERE` above the join whose conjuncts each touch
//!   only one side (pushed onto that side's delta; right-side pushdown is
//!   inner-join only — under LEFT OUTER it would change null-padding)
//! - Single-source DISTINCT → `IncrementalDistinctOp`
//!
//! # DiffBased fallback
//! Subqueries, multi-way joins, window functions, non-equi or cross-side
//! join predicates, RIGHT/FULL OUTER joins, and other complex patterns fall
//! through to full SQL re-execution + diff.

use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::TreeNode;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Aggregate, Expr, Join, JoinType, LogicalPlan, Projection, Window};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::prelude::SessionContext;

use arrow::record_batch::RecordBatch;
#[allow(unused_imports)]
use krishiv_delta::operators::consolidate::{};
use krishiv_delta::operators::topn::{IncrementalTopNOp, TopNSortKey};
use krishiv_delta::{
    Aggregation, DeltaBatch, DeltaError, DeltaResult, IncrJoinType, IncrementalAggOp,
    IncrementalDistinctOp, IncrementalJoinOp,
};

// ── ViewPlan enum ─────────────────────────────────────────────────────────────

/// Execution plan for one incremental view.
///
/// Variants other than `DiffBased` are O(Δ): they operate only on the
/// incoming delta and maintain state across ticks.
#[allow(clippy::large_enum_variant)]
pub enum ViewPlan {
    /// Stateful group-by aggregate over one source (or upstream view).
    Aggregate {
        source: String,
        op: IncrementalAggOp,
        /// `WHERE` predicate applied to the source delta before aggregation.
        filter: Option<SourceFilter>,
    },
    /// Bilinear inner join: `ΔA ⋈ B_trace + A_trace ⋈ ΔB`.
    Join {
        left_source: String,
        right_source: String,
        op: IncrementalJoinOp,
        /// Predicate applied to the left source delta before probing.
        left_filter: Option<SourceFilter>,
        /// Predicate applied to the right source delta before probing.
        right_filter: Option<SourceFilter>,
        /// BAND-1: non-equi ON conjuncts (`a.ts BETWEEN p.ts - 10000 AND
        /// p.ts + 10000`), compiled against the joined relation and applied to
        /// the emitted delta after the probe. Filter is linear over Z-sets, so
        /// post-filtering the joined delta is exactly the delta of the
        /// band-joined relation. INNER only: under LEFT OUTER an ON residual
        /// decides *matching* (a band-failing pair still yields a null-padded
        /// left row), which a post-filter cannot express.
        residual: Option<SourceFilter>,
        /// BAND-1: a `Projection` above the join, compiled against the joined
        /// relation (per-side plan qualifiers, right keys dropped) and applied
        /// after the residual — so a projected join emits the DECLARED
        /// relation instead of being refused (the SCHEMA-1 guard then passes
        /// for the right reason).
        post: Option<MapOp>,
    },
    /// Threshold-tracking DISTINCT: emits ±1 only at crossing the 0-threshold.
    Distinct {
        source: String,
        op: IncrementalDistinctOp,
        /// `WHERE` predicate applied to the source delta before de-duplication.
        filter: Option<SourceFilter>,
    },
    /// Stateless projection / derived columns / filter over one source
    /// (IVM-MAP-1). `map` is **linear** over Z-sets — `map(A + B) = map(A) +
    /// map(B)` — so mapping the delta yields exactly the delta of the mapped
    /// relation. That is why this variant carries no operator state: there is
    /// nothing to accumulate, checkpoint, restore, seed or garbage-collect.
    Map { source: String, op: MapOp },
    /// Incremental `ORDER BY … LIMIT k` (IVM-TOPN-1). Unlike a bare `ORDER BY`,
    /// a LIMIT changes *which rows are in the relation*, so this is a real
    /// stateful operator — it holds the whole relation in sort order because a
    /// retraction inside the cut promotes a row from outside it.
    TopN {
        source: String,
        op: IncrementalTopNOp,
    },
    /// A linear pipeline of single-operator hops compiled from one
    /// multi-operator query by [`crate::decompose`] (DECOMP-2). The first hop
    /// reads `source`; each later hop consumes the previous hop's output
    /// delta directly — the hops are not views, have no names of their own,
    /// and checkpoint/restore/seed through this plan like any other. Every
    /// hop is a single-input incremental variant (`Map`, `Aggregate`,
    /// `Distinct`, `TopN`), enforced at build time by the decomposer's
    /// wholesale-refusal rule and at apply time by [`apply_chain_hop`].
    Chain { source: String, hops: Vec<ViewPlan> },
    /// Fallback: full SQL re-execution + diff against previous output (O(state)).
    DiffBased,
}

/// Apply one chain hop to a delta: the fold step for [`ViewPlan::Chain`],
/// shared by the live tick path and snapshot seeding so the two cannot
/// diverge. A hop's own empty-input semantics stay in charge — a map emits
/// nothing, a global aggregate establishes its owed row (GLOBAL-1) — which is
/// what lets EMPTY-2's empty-substitution work through a chain unchanged.
pub(crate) fn apply_chain_hop(
    hop: &mut ViewPlan,
    delta: DeltaBatch,
) -> Result<DeltaBatch, krishiv_delta::DeltaError> {
    match hop {
        ViewPlan::Aggregate { op, filter, .. } => {
            let Some(delta) = apply_side_filter(filter, Some(delta))? else {
                return Err(krishiv_delta::DeltaError::Operator(
                    "side filter dropped a present delta".into(),
                ));
            };
            op.apply(delta)
        }
        ViewPlan::Distinct { op, filter, .. } => {
            let Some(delta) = apply_side_filter(filter, Some(delta))? else {
                return Err(krishiv_delta::DeltaError::Operator(
                    "side filter dropped a present delta".into(),
                ));
            };
            op.apply(delta)
        }
        ViewPlan::Map { op, .. } => op.apply(delta),
        ViewPlan::TopN { op, .. } => op.apply(delta),
        // Unreachable by construction (the decomposer refuses joins and never
        // nests chains), written out so a future variant cannot slide through
        // a `_` arm and silently drop a delta.
        ViewPlan::Join { .. } | ViewPlan::Chain { .. } | ViewPlan::DiffBased => {
            Err(krishiv_delta::DeltaError::Operator(
                "a chain hop must be a single-input incremental operator".into(),
            ))
        }
    }
}

/// Apply a chain's JOIN leaf hop (DECOMP-4): side filters, probe, residual,
/// post — the same sequence as the flow's standalone join arm, shared with
/// snapshot seeding so live and seed paths cannot diverge.
pub(crate) fn apply_chain_join_hop(
    hop: &mut ViewPlan,
    left: Option<DeltaBatch>,
    right: Option<DeltaBatch>,
) -> Result<DeltaBatch, krishiv_delta::DeltaError> {
    let ViewPlan::Join {
        op,
        left_filter,
        right_filter,
        residual,
        post,
        ..
    } = hop
    else {
        return Err(krishiv_delta::DeltaError::Operator(
            "chain leaf is not a join hop".into(),
        ));
    };
    let left = apply_side_filter(left_filter, left)?;
    let right = apply_side_filter(right_filter, right)?;
    let d = op.apply(left, right)?;
    let d = match residual {
        Some(f) => f.apply(d)?,
        None => d,
    };
    match post {
        Some(m) => m.apply(d),
        None => Ok(d),
    }
}

/// Framing magic for a [`ViewPlan::Chain`]'s checkpointed state: per-hop
/// length-prefixed blobs, so one view's checkpoint carries every stateful
/// hop's accumulator.
const CHAIN_STATE_MAGIC: &[u8; 4] = b"CHN1";

/// Compiled projection for a [`ViewPlan::Map`].
///
/// Holds one physical expression per output column plus an optional predicate,
/// compiled once at plan time against the source schema. Applying it to a delta
/// evaluates the expressions over the delta's rows and keeps each row's weight.
#[derive(Clone)]
pub struct MapOp {
    exprs: Vec<(String, Arc<dyn PhysicalExpr>)>,
    output_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl MapOp {
    /// Project (and optionally filter) one delta.
    pub fn apply(&self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        // Filter first: fewer rows to evaluate the projection over, and it
        // matches SQL semantics (WHERE is applied before SELECT).
        let delta = match &self.predicate {
            Some(pred) => {
                let pred = pred.clone();
                krishiv_delta::operators::filter::filter_batch(delta, move |batch| {
                    evaluate_predicate(&pred, batch)
                })?
            }
            None => delta,
        };
        let exprs = self.exprs.clone();
        let schema = self.output_schema.clone();
        krishiv_delta::operators::filter::map_batch(delta, move |batch| {
            let rows = batch.num_rows();
            let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(exprs.len());
            for (name, expr) in &exprs {
                let value = expr.evaluate(batch).map_err(|e| {
                    krishiv_delta::DeltaError::Operator(format!(
                        "map expression for column '{name}' failed: {e}"
                    ))
                })?;
                let array = value.into_array(rows).map_err(|e| {
                    krishiv_delta::DeltaError::Operator(format!(
                        "map expression for column '{name}' produced no array: {e}"
                    ))
                })?;
                // Conform to the planner's type (MAP-TYPE-1). Arrow's own
                // arithmetic can land on a wider type than DataFusion's planner
                // assigned the projection; the planner's answer is what every
                // reader was told to expect, so the column is cast to it rather
                // than the schema being bent to match the kernel.
                let want = schema
                    .field_with_name(name)
                    .map_err(|e| {
                        krishiv_delta::DeltaError::Operator(format!(
                            "map output schema has no column '{name}': {e}"
                        ))
                    })?
                    .data_type();
                let array = if array.data_type() == want {
                    array
                } else {
                    arrow::compute::cast(&array, want).map_err(|e| {
                        krishiv_delta::DeltaError::Operator(format!(
                            "map column '{name}': cannot represent {:?} as the planned {want:?}: {e}",
                            array.data_type()
                        ))
                    })?
                };
                columns.push(array);
            }
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                krishiv_delta::DeltaError::Operator(format!("map projection rebuild failed: {e}"))
            })
        })
    }

    /// The relation this operator emits — used by the IVM-AUD-SCHEMA-1 guard.
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

impl std::fmt::Debug for MapOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapOp")
            .field(
                "columns",
                &self.exprs.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            )
            .field("filtered", &self.predicate.is_some())
            .finish()
    }
}

/// A compiled `WHERE` predicate applied to a source's delta before it reaches
/// an incremental operator.
///
/// Filter is *linear* (`filter(ΔA) = Δ(filter(A))`), so it composes with any
/// O(Δ) operator with no state of its own: apply the predicate to the incoming
/// delta (and to the snapshot replayed during seeding) and the operator sees
/// exactly the rows the view's `WHERE` admits.
///
/// AUD-1: before this, `source_of_plan` peeled `Filter` nodes transparently and
/// the raw *unfiltered* delta was fed to the operator, so any filtered
/// single-source aggregate returned silently wrong results.
#[derive(Clone)]
pub struct SourceFilter {
    predicate: Arc<dyn PhysicalExpr>,
}

impl SourceFilter {
    /// The compiled predicate, for a caller that applies it itself
    /// (`MapOp` filters before projecting rather than in a separate pass).
    fn into_predicate(self) -> Arc<dyn PhysicalExpr> {
        self.predicate
    }

    /// Keep only the delta rows for which the predicate evaluates to `true`.
    pub fn apply(&self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let predicate = self.predicate.clone();
        krishiv_delta::operators::filter::filter_batch(delta, move |batch| {
            evaluate_predicate(&predicate, batch)
        })
    }
}

/// Evaluate a compiled predicate over a batch and return its boolean mask.
///
/// Shared by [`SourceFilter`] and [`MapOp`] so the two cannot drift into
/// different ideas of what a predicate evaluating to non-Boolean means.
fn evaluate_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> DeltaResult<BooleanArray> {
    let n = batch.num_rows();
    let value = predicate
        .evaluate(batch)
        .map_err(|e| DeltaError::Operator(format!("filter predicate eval: {e}")))?;
    let array = value
        .into_array(n)
        .map_err(|e| DeltaError::Operator(format!("filter predicate to_array: {e}")))?;
    let mask = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DeltaError::Operator("filter predicate did not evaluate to Boolean".into())
        })?;
    Ok(mask.clone())
}

/// Apply an optional source filter to an optional delta (helper for both the
/// live apply path and snapshot seeding).
pub fn apply_side_filter(
    filter: &Option<SourceFilter>,
    delta: Option<DeltaBatch>,
) -> DeltaResult<Option<DeltaBatch>> {
    match (filter, delta) {
        (Some(f), Some(d)) => Ok(Some(f.apply(d)?)),
        (_, d) => Ok(d),
    }
}

/// Lightweight discriminant for inter-phase communication without borrowing the
/// operator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewPlanKind {
    Incremental,
    DiffBased,
}

impl ViewPlan {
    pub fn kind(&self) -> ViewPlanKind {
        match self {
            ViewPlan::DiffBased => ViewPlanKind::DiffBased,
            _ => ViewPlanKind::Incremental,
        }
    }

    /// AUD-9 (loud degradation): a short human description of how this view
    /// executes, surfaced on the `debug-info` endpoint so an operator can see —
    /// and act on — a view silently running full-recompute instead of O(Δ).
    pub fn describe(&self) -> &'static str {
        match self {
            ViewPlan::Aggregate { .. } => {
                "incremental aggregate — retract/insert only the changed groups per delta"
            }
            ViewPlan::Distinct { .. } => "incremental DISTINCT — multiset add/remove per delta",
            ViewPlan::Map { .. } => {
                "incremental map — projection/derived columns applied per delta row, stateless"
            }
            ViewPlan::TopN { .. } => {
                "incremental top-N — ordered index over the relation, emits only the change to the window"
            }
            ViewPlan::Join { .. } => {
                "incremental equi-join — symmetric hash trace; probes only the delta rows"
            }
            ViewPlan::Chain { .. } => {
                "incremental chain — a multi-operator query cut into single-operator hops, \
                 each maintained O(Δ), folded per delta (DECOMP-2)"
            }
            ViewPlan::DiffBased => {
                "full recompute (DiffBased) — no O(Δ) plan matched this view shape (needs a \
                 single-source GROUP BY aggregate, a projection/derived-column/filter map, \
                 DISTINCT over the whole source, or an equi-join with supported per-side \
                 filters); the tick re-runs the whole view SQL and diffs the result"
            }
        }
    }

    /// Serialize the operator's internal accumulator state, or `None` when the
    /// operator has none (`DiffBased` is stateless). A caller that gets `None`
    /// falls back to [`seed_from_snapshots`](Self::seed_from_snapshots).
    ///
    /// This is what makes an incremental view survive a coordinator restart
    /// *losslessly*, including sources with genuinely duplicate rows: the
    /// materialized source snapshot is a set (multiplicity dropped by
    /// `filter_positive`), so the accumulator cannot be rebuilt from it — only
    /// the operator itself holds the ground truth (G6/F4). Join traces
    /// serialize their Z-sets via Arrow IPC (#160), which also spares the
    /// distributed `delta:step:` path from rebuilding join hash state from
    /// full source snapshots on every offloaded tick.
    pub fn checkpoint_state(&self) -> Option<Vec<u8>> {
        match self {
            ViewPlan::Aggregate { op, .. } => Some(op.state_bytes()),
            ViewPlan::Distinct { op, .. } => Some(op.state_bytes()),
            // Trace serialization is fallible (IPC); on failure fall back to
            // snapshot seeding rather than failing the whole checkpoint.
            ViewPlan::Join { op, .. } => match op.state_bytes() {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "join trace checkpoint failed; restore will re-seed \
                         from source snapshots (multiplicity-lossy)"
                    );
                    None
                }
            },
            // Stateless by construction (map is linear), so there is nothing to
            // checkpoint. Returning None is correct, not a gap: restore has
            // nothing to rebuild and `seed_from_snapshots` is a no-op too.
            ViewPlan::Map { .. } => None,
            // The ordered index is rebuildable from the source relation, so
            // restore seeds it rather than carrying a second encoding of the
            // same rows through the checkpoint.
            ViewPlan::TopN { .. } => None,
            // Frame every hop's state (stateless hops as an absent slot) so a
            // restore can put each accumulator back where it was. Framing the
            // count makes a chain-shape change detectable at restore: a
            // mismatch errors and the caller re-seeds instead of feeding one
            // hop another hop's bytes.
            ViewPlan::Chain { hops, .. } => {
                let mut out = Vec::new();
                out.extend_from_slice(CHAIN_STATE_MAGIC);
                out.extend_from_slice(&(u32::try_from(hops.len()).ok()?).to_le_bytes());
                for hop in hops {
                    match hop.checkpoint_state() {
                        Some(bytes) => {
                            out.push(1);
                            out.extend_from_slice(
                                &(u32::try_from(bytes.len()).ok()?).to_le_bytes(),
                            );
                            out.extend_from_slice(&bytes);
                        }
                        None => out.push(0),
                    }
                }
                Some(out)
            }
            ViewPlan::DiffBased => None,
        }
    }

    /// Restore operator state produced by [`checkpoint_state`]. Returns `false`
    /// when this plan variant does not carry restorable state (caller should
    /// seed instead); `true` when the state was applied.
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<bool> {
        match self {
            ViewPlan::Aggregate { op, .. } => {
                op.restore_state_bytes(bytes)?;
                Ok(true)
            }
            ViewPlan::Distinct { op, .. } => {
                op.restore_state_bytes(bytes)?;
                Ok(true)
            }
            ViewPlan::Join { op, .. } => {
                op.restore_state_bytes(bytes)?;
                Ok(true)
            }
            ViewPlan::Map { .. } => Ok(false),
            ViewPlan::TopN { .. } => Ok(false),
            ViewPlan::Chain { hops, .. } => {
                let err =
                    |m: &str| krishiv_delta::DeltaError::Operator(format!("chain state: {m}"));
                let rest = bytes
                    .strip_prefix(CHAIN_STATE_MAGIC)
                    .ok_or_else(|| err("bad magic"))?;
                let (count_bytes, mut rest) = rest
                    .split_at_checked(4)
                    .ok_or_else(|| err("truncated count"))?;
                let count =
                    u32::from_le_bytes(count_bytes.try_into().map_err(|_| err("bad count"))?)
                        as usize;
                if count != hops.len() {
                    return Err(err(&format!(
                        "hop count changed: state has {count}, plan has {}",
                        hops.len()
                    )));
                }
                for hop in hops.iter_mut() {
                    let (flag, tail) = rest
                        .split_at_checked(1)
                        .ok_or_else(|| err("truncated flag"))?;
                    rest = tail;
                    if flag == [1] {
                        let (len_bytes, tail) = rest
                            .split_at_checked(4)
                            .ok_or_else(|| err("truncated len"))?;
                        let len =
                            u32::from_le_bytes(len_bytes.try_into().map_err(|_| err("bad len"))?)
                                as usize;
                        let (hop_bytes, tail) = tail
                            .split_at_checked(len)
                            .ok_or_else(|| err("truncated hop"))?;
                        rest = tail;
                        hop.restore_state_bytes(hop_bytes)?;
                    }
                }
                Ok(true)
            }
            ViewPlan::DiffBased => Ok(false),
        }
    }

    /// Seed a freshly built incremental operator's internal state from the
    /// current full snapshot(s) of its source(s).
    ///
    /// This is the **fallback** path: `checkpoint_full` serializes operator
    /// state (aggregates/distinct accumulators and, since #160, join traces),
    /// and restore prefers those bytes. Seeding covers checkpoints written
    /// before join-state serialization existed, a failed state decode, and the
    /// normal first-build case. Without either, the first delta after a
    /// restore is applied against empty state, so the operator emits an
    /// *insertion* for a group that already exists in the restored view
    /// snapshot (no matching retraction), corrupting the materialized output
    /// on the next restore cycle (G6/F4 recreate path). Note seeding replays
    /// the *materialized* snapshot — a set — so duplicate-row multiplicity is
    /// not recoverable on this path; the checkpointed bytes are.
    ///
    /// `lookup(source)` returns the restored full snapshot of a base source or
    /// upstream view (pre-tick, i.e. before this tick's delta). Replaying it as
    /// an insert-only delta reconstructs the exact operator state the original
    /// flow held; the emitted output is discarded (the view snapshot + baseline
    /// were restored separately, in lockstep). A no-op when the source snapshot
    /// is absent or empty — the normal first-build case, where data has not yet
    /// arrived and the operator *should* start empty.
    pub fn seed_from_snapshots(
        &mut self,
        lookup: impl Fn(&str) -> Option<RecordBatch>,
    ) -> DeltaResult<()> {
        let seed_delta = |name: &str| -> DeltaResult<Option<DeltaBatch>> {
            match lookup(name) {
                Some(snap) if snap.num_rows() > 0 => Ok(Some(DeltaBatch::from_inserts(snap)?)),
                _ => Ok(None),
            }
        };
        match self {
            ViewPlan::Aggregate { source, op, filter } => {
                // AUD-1: the replayed snapshot must pass the same WHERE filter,
                // otherwise the seeded state includes rows the view excludes.
                if let Some(delta) = apply_side_filter(filter, seed_delta(source)?)? {
                    let _ = op.apply(delta)?;
                }
            }
            ViewPlan::Distinct { source, op, filter } => {
                if let Some(delta) = apply_side_filter(filter, seed_delta(source)?)? {
                    let _ = op.apply(delta)?;
                }
            }
            // Stateless: there is no accumulator to seed. Replaying the
            // snapshot through it would emit the whole relation as a spurious
            // insert delta, so doing nothing is the correct action, not a gap.
            ViewPlan::Map { .. } => {}
            // Replay the source snapshot through the chain's own fold: each
            // hop's emitted delta IS the relation the next hop reads, so after
            // one pass every stateful hop holds exactly the state it would
            // hold had the source arrived row by row. The final output is
            // discarded like every other seed (the view snapshot + baseline
            // were restored separately, in lockstep). A join leaf (DECOMP-4)
            // seeds both traces AND emits the joined relation as the seed
            // delta for the hops above it — the same fold, two inputs.
            ViewPlan::Chain { source, hops } => {
                if let Some((first, rest)) = hops.split_first_mut() {
                    let seeded = if let ViewPlan::Join {
                        left_source,
                        right_source,
                        ..
                    } = first
                    {
                        let l = seed_delta(&left_source.clone())?;
                        let r = seed_delta(&right_source.clone())?;
                        if l.is_some() || r.is_some() {
                            Some(apply_chain_join_hop(first, l, r)?)
                        } else {
                            None
                        }
                    } else {
                        match seed_delta(source)? {
                            Some(d) => Some(apply_chain_hop(first, d)?),
                            None => None,
                        }
                    };
                    if let Some(mut delta) = seeded {
                        for hop in rest {
                            delta = if let ViewPlan::Join { right_source, .. } = hop {
                                let right = seed_delta(&right_source.clone())?;
                                apply_chain_join_hop(hop, Some(delta), right)?
                            } else {
                                apply_chain_hop(hop, delta)?
                            };
                        }
                    }
                }
            }
            ViewPlan::TopN { source, op } => {
                // Replaying the snapshot rebuilds the ordered index; the emitted
                // delta is discarded because the view's own snapshot already
                // holds the window it describes.
                if let Some(delta) = seed_delta(source)? {
                    let _ = op.apply(delta)?;
                }
            }
            // Traces store raw rows keyed on the equi columns; the BAND-1
            // residual and post-projection shape only the EMITTED delta, so
            // seeding ignores them — the trace a residual-filtered join needs
            // is the same trace an unfiltered one needs.
            ViewPlan::Join {
                left_source,
                right_source,
                op,
                left_filter,
                right_filter,
                ..
            } => {
                let left = apply_side_filter(left_filter, seed_delta(left_source)?)?;
                let right = apply_side_filter(right_filter, seed_delta(right_source)?)?;
                if left.is_some() || right.is_some() {
                    let _ = op.apply(left, right)?;
                }
            }
            ViewPlan::DiffBased => {}
        }
        Ok(())
    }

    /// GC trace state for join operators.
    ///
    /// Each `ViewPlan::Join` is GC'd at the minimum watermark of its own two
    /// sources, not the global minimum across all sources. Using the global
    /// minimum would prevent GC whenever any slow/unwatermarked source exists.
    pub fn gc_watermark(
        &mut self,
        watermarks: &AHashMap<String, i64>,
    ) -> krishiv_delta::DeltaResult<usize> {
        match self {
            ViewPlan::Join {
                left_source,
                right_source,
                op,
                ..
            } => {
                let wm_left = watermarks
                    .get(left_source.as_str())
                    .copied()
                    .unwrap_or(i64::MIN);
                let wm_right = watermarks
                    .get(right_source.as_str())
                    .copied()
                    .unwrap_or(i64::MIN);
                let wm = wm_left.min(wm_right);
                if wm > i64::MIN {
                    op.gc_traces(wm)
                } else {
                    Ok(0)
                }
            }
            ViewPlan::Aggregate { source, op, .. } => {
                let wm = watermarks.get(source.as_str()).copied().unwrap_or(i64::MIN);
                if wm > i64::MIN {
                    op.gc_watermark(wm)
                } else {
                    Ok(0)
                }
            }
            ViewPlan::Distinct { source, op, .. } => {
                let wm = watermarks.get(source.as_str()).copied().unwrap_or(i64::MIN);
                if wm > i64::MIN {
                    op.gc_watermark(wm)
                } else {
                    Ok(0)
                }
            }
            // Hop sources are internal names with no watermark trackers of
            // their own; the chain's base watermark bounds every hop's state
            // (event-time columns pass through map hops unchanged). A join
            // leaf uses the minimum of its OWN two sources, like the
            // standalone join arm, and GCs its traces at it.
            ViewPlan::Chain { source, hops } => {
                // The minimum watermark across EVERY involved source bounds
                // every hop's state: the leaf's side(s) plus each mid-chain
                // join's right table (MJOIN-1).
                // Only REAL sources carry trackers: the leaf join's two
                // sides, and each later join's RIGHT table — a mid-chain
                // join's left is an internal hop name, and treating its
                // missing tracker as MIN would silently disable GC for every
                // multi-join chain.
                let mut wm: Option<i64> = None;
                let mut take = |name: &str| {
                    let w = watermarks.get(name).copied().unwrap_or(i64::MIN);
                    wm = Some(match wm {
                        Some(cur) => cur.min(w),
                        None => w,
                    });
                };
                for (i, hop) in hops.iter().enumerate() {
                    if let ViewPlan::Join {
                        left_source,
                        right_source,
                        ..
                    } = hop
                    {
                        if i == 0 {
                            take(left_source);
                        }
                        take(right_source);
                    }
                }
                let wm = match wm {
                    Some(w) => w,
                    None => watermarks.get(source.as_str()).copied().unwrap_or(i64::MIN),
                };
                if wm == i64::MIN {
                    return Ok(0);
                }
                let mut reclaimed = 0;
                for hop in hops.iter_mut() {
                    reclaimed += match hop {
                        ViewPlan::Aggregate { op, .. } => op.gc_watermark(wm)?,
                        ViewPlan::Distinct { op, .. } => op.gc_watermark(wm)?,
                        ViewPlan::Join { op, .. } => op.gc_traces(wm)?,
                        _ => 0,
                    };
                }
                Ok(reclaimed)
            }
            // No retained state, so nothing can be reclaimed.
            ViewPlan::Map { .. } => Ok(0),
            // The index holds exactly the relation; nothing in it is reclaimable
            // without changing the answer.
            ViewPlan::TopN { .. } => Ok(0),
            ViewPlan::DiffBased => Ok(0),
        }
    }
}

/// One `ORDER BY` term of a view's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderColumn {
    pub name: String,
    pub descending: bool,
    pub nulls_first: bool,
}

/// How a view's materialized output should be presented.
///
/// **Ordering is deliberately NOT part of [`ViewPlan`]** (IVM-ORDER-1). An
/// incremental operator maintains a Z-set, and a Z-set is an unordered
/// multiset — `ORDER BY` says nothing about how the relation changes, only how
/// it is read. Modelling it as a stateful sort operator would put per-tick cost
/// on the maintenance path to serve a property that only matters at snapshot
/// time, and would make every `ViewPlan` match arm carry a delegating case for
/// something that does not affect delta propagation.
///
/// So a `Sort` node is peeled like a projection, the inner plan is maintained
/// exactly as it would be without the clause, and the order is applied when the
/// snapshot is read. Maintenance stays O(Δ) and unchanged; the sort is paid per
/// *read*, which is the operation that actually asked for it.
///
/// `LIMIT` is a different thing entirely and is **not** peeled — see
/// `try_build_from_logical`'s `Sort` arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputOrder {
    pub columns: Vec<OrderColumn>,
}

/// A view's compiled plan plus how its output is presented.
pub struct PlannedView {
    pub plan: ViewPlan,
    pub order: Option<OutputOrder>,
}

/// Read the `ORDER BY` off the root of a view's logical plan.
///
/// Only the root: an inner sort (inside a subquery, under an aggregate) does not
/// describe the view's output and must not be lifted out of it.
fn extract_output_order(plan: &LogicalPlan) -> Option<OutputOrder> {
    // `ORDER BY x` roots at `Sort`; `ORDER BY x LIMIT k` roots at `Limit` with
    // the `Sort` beneath it.
    //
    // The first cut matched only the bare `Sort`, so a top-N view recorded no
    // order and read back **unsorted** — the operator maintained the correct
    // top-k *set* while the presentation order the query asked for was simply
    // dropped. A top-N is ordered by definition; `LIMIT` decides which rows are
    // in the relation and `ORDER BY` decides how they are read, and both apply.
    //
    // This survived its own test: the operator's `BTreeMap` iterates in sort
    // order, so early snapshots looked sorted by accident. It took reading back
    // after several maintenance ticks to expose it.
    let sort = match plan {
        LogicalPlan::Sort(sort) => sort,
        LogicalPlan::Limit(limit) => match limit.input.as_ref() {
            LogicalPlan::Sort(sort) => sort,
            _ => return None,
        },
        _ => return None,
    };
    let mut columns = Vec::with_capacity(sort.expr.len());
    for se in &sort.expr {
        // Only a plain column can be applied at read time. An expression sort
        // (`ORDER BY a + b`) would need the expression evaluated against the
        // snapshot, which is a map's job — express it as a map hop instead.
        let name = expr_col_name(&se.expr)?;
        columns.push(OrderColumn {
            name,
            descending: !se.asc,
            nulls_first: se.nulls_first,
        });
    }
    if columns.is_empty() {
        return None;
    }
    Some(OutputOrder { columns })
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Try to build an O(Δ) `ViewPlan` for a view, falling back to `DiffBased`.
///
/// `available_schemas` maps each known source / upstream view name to its data
/// schema (no `_weight` column). This is needed to construct operators.
///
/// Planning runs against an **ephemeral schema-only context**: the plan is
/// determined by the SQL's structure, never by which sources happen to hold
/// rows this tick. Planning against the tick's data context made an
/// empty/emptied source fail `ctx.sql` and pin the view to DiffBased — fatal
/// after a checkpoint restore, which rebuilds plans lazily (#160).
pub async fn build_view_plan(
    body_sql: &str,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
) -> ViewPlan {
    build_planned_view(body_sql, output_schema, available_schemas, lateness)
        .await
        .plan
}

/// [`build_view_plan`] plus the view's output ordering (IVM-ORDER-1).
pub async fn build_planned_view(
    body_sql: &str,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
) -> PlannedView {
    build_planned_view_impl(body_sql, output_schema, available_schemas, lateness, true).await
}

/// The single-operator matchers only — no chain attempt. This is what the
/// decomposer verifies each hop with: a hop that would need its own chain
/// means the cutting was wrong, and letting a hop re-enter chain-building
/// recurses forever, because DataFusion replans an unparsed aggregate as
/// `Projection(Aggregate)` — two cuts that reproduce themselves — so a
/// refused aggregate hop would decompose into itself without terminating.
pub(crate) async fn build_view_plan_single(
    body_sql: &str,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
) -> ViewPlan {
    build_planned_view_impl(body_sql, output_schema, available_schemas, lateness, false)
        .await
        .plan
}

async fn build_planned_view_impl(
    body_sql: &str,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
    try_chain: bool,
) -> PlannedView {
    use datafusion::datasource::MemTable;
    let ctx = SessionContext::new();
    for (name, schema) in available_schemas {
        let empty = RecordBatch::new_empty(schema.clone());
        if let Ok(table) = MemTable::try_new(schema.clone(), vec![vec![empty]]) {
            let _ = ctx.register_table(name.as_str(), Arc::new(table));
        }
    }
    let df = match ctx.sql(body_sql).await {
        Ok(d) => d,
        Err(_) => {
            return PlannedView {
                plan: ViewPlan::DiffBased,
                order: None,
            };
        }
    };
    let plan = df.logical_plan().clone();
    let order = extract_output_order(&plan);
    let mut built = try_build_from_logical(&plan, output_schema, available_schemas, lateness)
        .unwrap_or(ViewPlan::DiffBased);
    // DECOMP-2: a multi-operator query no single matcher accepts may still cut
    // into a chain of single-operator hops, each O(Δ). Gated on empty lateness
    // because hop plans are built without the view's lateness specs — passing
    // them through per hop is unexamined, and silently dropping them is the
    // AUD-1 mistake. Recursion terminates: a hop's SQL is single-operator, so
    // a nested attempt finds fewer than two cuts and refuses immediately.
    if try_chain
        && matches!(built, ViewPlan::DiffBased)
        && lateness.is_empty()
        && let Some(chain) =
            crate::decompose::decompose_into_chain(body_sql, output_schema, available_schemas).await
    {
        built = chain;
    }
    PlannedView { plan: built, order }
}

// ── Auto-partition key inference ──────────────────────────────────────────────

/// Inspect a view's SQL and report the single column it can be safely sharded
/// by, or `None` if no safe single-key sharding exists.
///
/// # The rule this enforces
///
/// A view is shardable when, for every key value `k`, the view's output rows
/// for `k` depend only on input rows carrying `k` **and** the whole view's
/// output is the concatenation of those per-key results. The only shape this
/// function is willing to prove is a **single-column `GROUP BY` aggregate over
/// exactly one plain table**, with nothing above or beside the aggregation
/// that can see across groups.
///
/// # What it refuses, and why (IVM-AUD-PART-6)
///
/// The predecessor of this function looked at `GROUP BY` and nothing else, so
/// it declared these shardable and got each of them wrong:
///
/// * `LIMIT n` / `FETCH` — the limit is applied *inside every shard*, so an
///   N-shard job returns up to `n × N` rows and, with `ORDER BY`, a top-N over
///   the wrong candidate set.
/// * `ORDER BY` — per-shard results are concatenated, which destroys the
///   ordering the query asked for.
/// * A join, or any second table in `FROM` — sharding by the group key
///   co-locates rows by the *group* key, not the join key, so matching pairs
///   land in different shards and silently disappear (or the dimension source
///   hard-errors at feed time for lacking the key column at all).
/// * Any subquery — a FROM-clause or scalar subquery is evaluated once per
///   shard over that shard's rows, so a `(SELECT SUM(x) FROM t)` denominator
///   becomes the shard's sum instead of the table's.
/// * A projection alias that shadows the key (`SELECT other AS region … GROUP
///   BY region`) — rows are routed by the *input* column named `region`, which
///   is not the column the query groups on.
/// * Window functions, `DISTINCT`, `QUALIFY`, `CLUSTER/DISTRIBUTE/SORT BY`,
///   set operations, CTEs and multi-statement input — each can see across
///   groups or across shards.
///
/// `WHERE`, `HAVING`, and any aggregate function are accepted: they are
/// per-row or per-group, and a group lives entirely inside one shard.
///
/// # Why the SQL text and not a `LogicalPlan`
///
/// The coordinator registers views **before** any data arrives, so source
/// schemas are not yet known and `SessionContext::sql` cannot plan. This
/// parses to a `sqlparser` AST instead. There used to be a second,
/// logical-plan-based detector here that disagreed with this one in both
/// directions (it caught `LIMIT`, because a `Limit` node sits above the
/// `Aggregate`, but happily sharded a join, because it read the `Aggregate`'s
/// group expression without ever looking at its input) — and it was reachable
/// only from a function with no production callers. It is gone: one detector,
/// used everywhere (IVM-AUD-PART-7).
pub fn partition_key_from_sql(sql: &str) -> Option<String> {
    use sqlparser::ast::{Expr as SqlExpr, GroupByExpr, Query, SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(query) = stmts.first()? else {
        return None;
    };

    // One `Query` node in the whole tree. Anything that introduces a second —
    // a CTE, a derived table in FROM, a scalar/IN/EXISTS subquery anywhere —
    // would be evaluated per shard over that shard's rows only.
    if count_query_nodes(query.as_ref()) != 1 {
        return None;
    }

    // Clauses that operate on the *result set* rather than on a group. Each of
    // these would be applied independently inside every shard.
    let Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query.as_ref();
    if with.is_some()
        || order_by.is_some()
        || limit_clause.is_some()
        || fetch.is_some()
        || !locks.is_empty()
        || for_clause.is_some()
        || settings.is_some()
        || format_clause.is_some()
        || !pipe_operators.is_empty()
    {
        return None;
    }

    let SetExpr::Select(select) = body.as_ref() else {
        return None;
    };

    // Select-level modifiers that can see across groups or across shards.
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.exclude.is_some()
        || select.select_modifiers.is_some()
        || !select.optimizer_hints.is_empty()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return None;
    }

    // Exactly one plain table in FROM, no joins, no table-valued function.
    let (table_idents, _) = single_plain_table(select)?;

    // Exactly one plain-column GROUP BY expression, qualified (if at all) by
    // that table or its alias.
    let GroupByExpr::Expressions(exprs, modifiers) = &select.group_by else {
        return None;
    };
    if exprs.len() != 1 || !modifiers.is_empty() {
        return None;
    }
    let key = match exprs.first()? {
        SqlExpr::Identifier(ident) => ident.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => {
            if parts.len() != 2 {
                return None;
            }
            let qualifier = parts.first()?.value.as_str();
            if !table_idents
                .iter()
                .any(|name| name.eq_ignore_ascii_case(qualifier))
            {
                return None;
            }
            parts.last()?.value.clone()
        }
        _ => return None,
    };

    // A window function anywhere in the projection ranks over its own
    // partition, which need not be the shard key.
    if projection_has_window_function(select) {
        return None;
    }

    // A projection alias equal to the key routes rows by an input column the
    // query never groups on.
    if projection_alias_shadows(select, &key) {
        return None;
    }

    Some(key)
}

/// Number of `Query` nodes in the tree, including the root.
fn count_query_nodes(query: &sqlparser::ast::Query) -> usize {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Query, Visit, Visitor};

    struct Counter(usize);
    impl Visitor for Counter {
        type Break = ();
        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.0 += 1;
            ControlFlow::Continue(())
        }
    }
    let mut counter = Counter(0);
    let _ = query.visit(&mut counter);
    counter.0
}

/// The `FROM` clause reduced to the one plain table it must be: returns the
/// names that may qualify a column of it (the table's last identifier and its
/// alias, if any) plus that table's bare name.
fn single_plain_table(select: &sqlparser::ast::Select) -> Option<(Vec<String>, String)> {
    use sqlparser::ast::TableFactor;

    if select.from.len() != 1 {
        return None;
    }
    let from = select.from.first()?;
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        partitions,
        json_path,
        sample,
        ..
    } = &from.relation
    else {
        return None;
    };
    // A table-valued function, a MSSQL hint, a partition selector, a PartiQL
    // path or a TABLESAMPLE all change what "the rows of this table" means.
    if args.is_some()
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
    {
        return None;
    }
    let bare = name.0.last()?.as_ident()?.value.clone();
    let mut idents = vec![bare.clone()];
    if let Some(alias) = alias {
        idents.push(alias.name.value.clone());
    }
    Some((idents, bare))
}

/// Whether any projected expression is a window function (`… OVER (…)`).
fn projection_has_window_function(select: &sqlparser::ast::Select) -> bool {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Expr as SqlExpr, Visit, Visitor};

    struct WindowFinder(bool);
    impl Visitor for WindowFinder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<()> {
            if let SqlExpr::Function(func) = expr
                && func.over.is_some()
            {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut finder = WindowFinder(false);
    let _ = select.projection.visit(&mut finder);
    finder.0
}

/// Whether a projection aliases some *other* expression to the key's name.
///
/// `SELECT customer AS region, SUM(amount) FROM orders GROUP BY region` groups
/// on the alias — i.e. on `customer` — while the router would shard the input
/// on the column literally named `region`. Two different columns, one name.
fn projection_alias_shadows(select: &sqlparser::ast::Select, key: &str) -> bool {
    use sqlparser::ast::{Expr as SqlExpr, SelectItem};

    let names_the_key = |expr: &SqlExpr| match expr {
        SqlExpr::Identifier(ident) => ident.value.eq_ignore_ascii_case(key),
        SqlExpr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|p| p.value.eq_ignore_ascii_case(key)),
        _ => false,
    };
    select.projection.iter().any(|item| match item {
        SelectItem::ExprWithAlias { expr, alias } => {
            alias.value.eq_ignore_ascii_case(key) && !names_the_key(expr)
        }
        SelectItem::ExprWithAliases { expr, aliases } => {
            aliases.iter().any(|a| a.value.eq_ignore_ascii_case(key)) && !names_the_key(expr)
        }
        _ => false,
    })
}

// ── Plan walker ───────────────────────────────────────────────────────────────

fn try_build_from_logical(
    plan: &LogicalPlan,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
) -> Option<ViewPlan> {
    match plan {
        // Peel top-level projections transparently.
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema: proj_schema,
            ..
        }) => {
            // IVM-AUD-CORE-23: a SELECT's aggregate aliases live in this
            // projection, not in the Aggregate below it — the Aggregate's own
            // schema names them `sum(sales.amount)` / `count(*)`. Peeling the
            // projection therefore threw away the only thing that says which
            // aggregate feeds which declared output column, leaving the
            // planner to pair them positionally.
            if let LogicalPlan::Aggregate(agg) = input.as_ref() {
                let aliases = aggregate_output_aliases(expr);
                return build_agg_plan(agg, output_schema, available_schemas, &aliases);
            }
            // BAND-1: a projected join — `SELECT p.name, a.id FROM a JOIN p …`
            // (optionally with a WHERE between) — compiles the projection as a
            // post-map over the joined relation instead of being refused for
            // emitting the wrong one. Tried before the recursive peel because
            // the peel reaches the bare-join arm, which cannot see these
            // projection expressions. On failure this FALLS THROUGH to the
            // peel rather than refusing: a `SELECT *` join carries an identity
            // projection wider than the operator's relation (it repeats the
            // right key), and the peel-then-check path is what has always
            // served that shape.
            let projected_join = match input.as_ref() {
                LogicalPlan::Join(join) => build_join_plan(
                    join,
                    None,
                    output_schema,
                    available_schemas,
                    lateness,
                    Some((expr, proj_schema)),
                ),
                LogicalPlan::Filter(f) => match f.input.as_ref() {
                    LogicalPlan::Join(join) => build_join_plan(
                        join,
                        Some(&f.predicate),
                        output_schema,
                        available_schemas,
                        lateness,
                        Some((expr, proj_schema)),
                    ),
                    _ => None,
                },
                _ => None,
            };
            if projected_join.is_some() {
                return projected_join;
            }
            // IVM-MAP-1: a projection directly over a source (optionally with a
            // WHERE between) is a stateless O(Δ) map. Tried only after the
            // recursive peel fails, so an aggregate/join/distinct underneath
            // still gets its own operator — `resolve_source_with_filters`
            // refuses to peel those nodes, so this cannot claim them anyway.
            try_build_from_logical(input, output_schema, available_schemas, lateness).or_else(
                || build_map_plan(expr, proj_schema, input, output_schema, available_schemas),
            )
        }
        LogicalPlan::Aggregate(agg) => {
            build_agg_plan(agg, output_schema, available_schemas, &AHashMap::new())
        }
        LogicalPlan::Join(join) => {
            // Only 2-source joins (source_of_plan returns None for multi-way joins
            // where one side is itself a Join node with 2 inputs).
            build_join_plan(join, None, output_schema, available_schemas, lateness, None)
        }
        // #160: `WHERE` above a join (`SELECT … FROM a JOIN b ON … WHERE …`)
        // plans as `Filter → Join`. Filter is linear, so conjuncts that touch
        // only one side push onto that side's delta filter; anything
        // cross-side (or right-side under LEFT OUTER, where pushdown changes
        // null-padding semantics) bails to DiffBased inside the builder.
        // Non-join inputs keep the previous behavior (single-source WHERE
        // shapes are resolved inside the aggregate/distinct builders; a bare
        // filtered scan stays DiffBased).
        LogicalPlan::Filter(f) => match f.input.as_ref() {
            LogicalPlan::Join(join) => build_join_plan(
                join,
                Some(&f.predicate),
                output_schema,
                available_schemas,
                lateness,
                None,
            ),
            // IVM-MAP-1: a bare filtered scan is an identity map with a
            // predicate. Documented as DiffBased before this — filter is
            // linear, so there was never a reason it had to be.
            _ => build_map_plan(&[], plan.schema(), plan, output_schema, available_schemas),
        },
        // DISTINCT — the inner plan is the first (and only) input.
        LogicalPlan::Distinct(_) => {
            let inputs = plan.inputs();
            let inner_plan = inputs.first().copied()?;
            let source = source_of_plan(inner_plan)?;
            // IVM-AUD-SCHEMA-1: `IncrementalDistinctOp` dedups whole source
            // rows and emits them unchanged, so it can only serve a view whose
            // output IS the source relation. `SELECT DISTINCT col FROM t`
            // resolved here through the peeled `Projection` and published every
            // source column — a wrong relation, reported healthy.
            if !emits_declared_relation(available_schemas.get(&source)?, output_schema) {
                tracing::warn!(
                    source = %source,
                    "IVM plan degraded to O(state) DiffBased: DISTINCT emits the \
                     whole source relation, which does not match this view's \
                     declared output columns (a projected DISTINCT)"
                );
                return None;
            }
            Some(ViewPlan::Distinct {
                source,
                op: IncrementalDistinctOp::new(),
                // AUD-1: a filtered DISTINCT falls back to DiffBased because
                // `source_of_plan` now refuses to peel `Filter` nodes (returns
                // None → DiffBased). O(Δ) filtered DISTINCT is future work.
                filter: None,
            })
        }
        // IVM-ORDER-1: `ORDER BY` is a read-time property of the output Z-set,
        // so the sort is peeled and the inner plan maintained unchanged. A
        // `Sort` with `fetch` is a top-N — its *answer* depends on the order, so
        // it is deliberately left to DiffBased rather than silently dropping the
        // LIMIT, which would publish more rows than the view promises.
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            try_build_from_logical(&sort.input, output_schema, available_schemas, lateness)
        }
        // IVM-TOPN-1: `ORDER BY … LIMIT k` plans as `Limit -> Sort -> …`, not
        // as a `Sort` carrying `fetch` — verified against the planner rather
        // than assumed. Scoped like the map: the input must resolve to a plain
        // source, so a top-N over an aggregate or a narrowing projection is a
        // two-hop DAG rather than a fused operator.
        LogicalPlan::Limit(limit) => build_topn_plan(limit, output_schema, available_schemas),
        // Window functions (ROW_NUMBER, RANK, rolling aggregates) cannot be
        // computed O(Δ) in general. Fall through to DiffBased explicitly.
        LogicalPlan::Window(Window { .. }) => None,
        // All other patterns (subqueries, set operations, multi-way joins, etc.)
        // fall back to DiffBased full SQL re-execution.
        _ => None,
    }
}

/// Build a stateless O(Δ) map plan for a projection (and/or WHERE) over one
/// source. Returns `None` — meaning DiffBased — for anything else.
///
/// `exprs` empty means "identity projection": every source column, unchanged,
/// which is the bare-filtered-scan shape.
fn build_map_plan(
    exprs: &[Expr],
    node_schema: &DFSchema,
    input: &LogicalPlan,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    // Only a plain source underneath, with optional WHEREs. This deliberately
    // does NOT peel a Join, Aggregate or Distinct: those need their own
    // stateful operator, and a map cannot read a relation that has not been
    // materialized as a source or an upstream view.
    let (source, preds) = resolve_source_with_filters(input)?;
    let source_schema = available_schemas.get(&source)?;

    let df_schema = DFSchema::try_from_qualified_schema(&source, source_schema.as_ref()).ok()?;
    let props = ExecutionProps::new();

    // Identity projection when the node carries no expressions.
    let owned_identity: Vec<Expr>;
    let exprs = if exprs.is_empty() {
        owned_identity = source_schema
            .fields()
            .iter()
            .map(|f| Expr::Column(datafusion::common::Column::new_unqualified(f.name())))
            .collect();
        &owned_identity[..]
    } else {
        exprs
    };

    // IVM-AUD-ALIAS-1: `FROM orders AS a` plans column references qualified by
    // the alias, but `df_schema` above is qualified by the table name —
    // `a.amount` does not resolve against a schema qualified `orders`, the
    // physical compile errors, and the `.ok()?` below turned that qualifier
    // mismatch into a silent DiffBased degrade. The names/types comments in
    // this function warn against a second source of truth for one fact; the
    // qualifier was that same mistake's third instance. One relation underlies
    // a map, so a bare name is unambiguous: strip qualifiers instead of
    // guessing which one the plan used.
    let exprs = unqualify_columns(exprs)?;

    // Output field names come from the plan node's own schema, which is where
    // DataFusion already resolved aliases — re-deriving them here would be a
    // second source of truth for the same fact (the CORE-23 defect's shape).
    let names: Vec<String> = node_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if names.len() != exprs.len() {
        return None;
    }

    let mut compiled: Vec<(String, Arc<dyn PhysicalExpr>)> = Vec::with_capacity(exprs.len());
    let mut fields: Vec<Field> = Vec::with_capacity(exprs.len());
    for ((expr, name), planned) in exprs
        .iter()
        .zip(names.iter())
        .zip(node_schema.fields().iter())
    {
        // Coerce before lowering, for the same reason `compile_source_filter`
        // does: an unoptimized logical expression carries no casts, so a mixed
        // arithmetic expression would fail the Arrow kernel at evaluation.
        let mut coercion = TypeCoercionRewriter::new(&df_schema);
        let coerced = expr.clone().rewrite(&mut coercion).ok()?.data;
        let physical = create_physical_expr(&coerced, &df_schema, &props).ok()?;
        compiled.push((name.clone(), physical));
        // MAP-TYPE-1: the type comes from the plan node's schema, NOT from
        // re-deriving it here. The first cut derived it independently and the
        // two disagreed — `auction % 1000` over a `UInt64` source degraded
        // silently because this function's answer differed from the planner's.
        // That is the same "second source of truth for one fact" mistake the
        // comment above this loop warns about for *names*; it was applied to
        // names and not to types. `MapOp::apply` casts the evaluated column to
        // this type, so the planner's answer is authoritative end to end.
        fields.push(Field::new(
            name,
            planned.data_type().clone(),
            planned.is_nullable(),
        ));
    }
    let emitted: SchemaRef = Arc::new(Schema::new(fields));

    // IVM-AUD-SCHEMA-1: never claim a shape whose relation is not the declared
    // one. A map CAN produce arbitrary projections, so unlike DISTINCT and the
    // join this is a real check rather than a refusal — but it still has to
    // hold, because the declared schema is what every reader trusts.
    if !emits_declared_relation(&emitted, output_schema) {
        // Loud, because this is the one place a map degrades for a reason the
        // author of the view cannot see from the SQL. A silent `None` here is
        // how MAP-TYPE-1 hid.
        tracing::warn!(
            source = %source,
            emitted = ?emitted
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>(),
            declared = ?output_schema
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>(),
            planner_said = ?node_schema
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>(),
            "IVM map plan degraded to DiffBased: the compiled projection does \
             not emit the relation the view declares"
        );
        return None;
    }

    let predicate = match compile_source_filter(&preds, &source, source_schema) {
        Ok(Some(f)) => Some(f.into_predicate()),
        Ok(None) => None,
        // A WHERE that will not compile must never be silently dropped (AUD-1).
        Err(()) => return None,
    };

    Some(ViewPlan::Map {
        source,
        op: MapOp {
            exprs: compiled,
            output_schema: emitted,
            predicate,
        },
    })
}

/// Build an O(Δ) top-N plan for `ORDER BY … LIMIT k` over one source.
fn build_topn_plan(
    limit: &datafusion::logical_expr::Limit,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
) -> Option<ViewPlan> {
    // `OFFSET` shifts the window; the operator emits the first k and has no
    // notion of skipping, so anything but skip=0 bails rather than silently
    // returning the wrong page.
    match limit.skip.as_deref() {
        None => {}
        Some(Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(0)), _)) => {}
        Some(_) => return None,
    }
    let fetch = match limit.fetch.as_deref()? {
        Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(n)), _) if *n >= 0 => {
            usize::try_from(*n).ok()?
        }
        // A non-literal LIMIT is not a fixed window and cannot be maintained.
        _ => return None,
    };
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        return None;
    };
    // `source_of_plan` peels the identity `Projection` the planner inserts, and
    // refuses a `Filter` — a WHERE under the LIMIT changes which rows compete
    // for the window, and the operator has no filter hook, so that must bail
    // rather than limit the unfiltered relation.
    let source = source_of_plan(&sort.input)?;
    let source_schema = available_schemas.get(&source)?;
    if !emits_declared_relation(source_schema, output_schema) {
        return None;
    }
    let mut keys = Vec::with_capacity(sort.expr.len());
    for se in &sort.expr {
        let name = expr_col_name(&se.expr)?;
        let column = source_schema.index_of(&name).ok()?;
        keys.push(TopNSortKey {
            column,
            descending: !se.asc,
            nulls_first: se.nulls_first,
        });
    }
    if keys.is_empty() {
        return None;
    }
    let op = IncrementalTopNOp::new(source_schema.clone(), keys, fetch).ok()?;
    Some(ViewPlan::TopN { source, op })
}

// ── Aggregate plan builder ────────────────────────────────────────────────────

/// Map each projected column's *internal* name to the name the SELECT gives it.
///
/// `SELECT region, SUM(amount) AS total` projects `sum(sales.amount) AS total`,
/// so this yields `{"sum(sales.amount)" -> "total"}`. Un-aliased columns map to
/// themselves, which is what a `SELECT region, …` needs.
fn aggregate_output_aliases(exprs: &[Expr]) -> AHashMap<String, String> {
    let mut out = AHashMap::new();
    for e in exprs {
        // Alias chains nest: `COUNT(*) AS cnt` projects
        // `Alias(Alias(Column("count(Int64(1))"), "count(*)"), "cnt")`, so the
        // outermost name is the user's and the innermost column is the
        // aggregate's internal name. Peel to the base column, keep the outer
        // name.
        let (mut inner, output_name) = match e {
            Expr::Alias(alias) => (alias.expr.as_ref(), alias.name.clone()),
            Expr::Column(col) => (e, col.name.clone()),
            _ => continue,
        };
        while let Expr::Alias(next) = inner {
            inner = next.expr.as_ref();
        }
        if let Expr::Column(col) = inner {
            out.insert(col.name.clone(), output_name);
        }
    }
    out
}

fn build_agg_plan(
    agg: &Aggregate,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    output_aliases: &AHashMap<String, String>,
) -> Option<ViewPlan> {
    // AUD-1: resolve the source *and* any WHERE predicate between the aggregate
    // and it. A clean `Aggregate → [Filter…] → [SubqueryAlias] → Scan` chain
    // keeps O(Δ) with the predicate applied to each delta; a compile failure
    // bails to DiffBased (never silently drops the predicate). Chains the strict
    // resolver can't read (e.g. a projection with computed columns) fall through
    // to `source_of_plan`, which now refuses to peel `Filter` — so a dropped
    // WHERE can never slip through as a plain aggregate.
    let (source, filter) = match resolve_source_with_filters(&agg.input) {
        Some((source, preds)) => {
            let schema = available_schemas.get(&source)?;
            let filter = compile_source_filter(&preds, &source, schema).ok()?;
            (source, filter)
        }
        None => (source_of_plan(&agg.input)?, None),
    };
    let input_schema = available_schemas.get(&source)?;

    // Extract GROUP BY column names.
    let group_by: Vec<String> = agg.group_expr.iter().filter_map(expr_col_name).collect();

    // Aggregate output columns = output_schema columns that are NOT in group_by.
    let agg_output_cols: Vec<String> = output_schema
        .fields()
        .iter()
        .filter(|f| !group_by.contains(f.name()))
        .map(|f| f.name().clone())
        .collect();

    if agg.aggr_expr.len() != agg_output_cols.len() {
        return None;
    }

    // IVM-AUD-CORE-23: pair each aggregate with its declared output column by
    // NAME. This used to zip the two lists positionally — `aggr_expr` in SELECT
    // order against the declared schema's non-group columns in schema order —
    // so a view whose declared schema listed its aggregate columns in a
    // different order than the SELECT list transposed the aggregations
    // (`SUM` computed into the `cnt` column and vice versa) while the arity
    // check above still passed.
    //
    // The plan's own schema is [group fields…, aggregate fields…], so the
    // aggregate at index i is named by field `group_expr.len() + i`.
    let plan_agg_names: Vec<String> = (0..agg.aggr_expr.len())
        .map(|i| {
            let internal = agg.schema.field(agg.group_expr.len() + i).name();
            output_aliases
                .get(internal)
                .cloned()
                .unwrap_or_else(|| internal.to_string())
        })
        .collect();

    let pair_by_name = plan_agg_names
        .iter()
        .all(|n| agg_output_cols.iter().any(|c| c.eq_ignore_ascii_case(n)));

    let mut aggregations: Vec<Aggregation> = Vec::new();
    if pair_by_name {
        for (expr, plan_name) in agg.aggr_expr.iter().zip(plan_agg_names.iter()) {
            let out_col = agg_output_cols
                .iter()
                .find(|c| c.eq_ignore_ascii_case(plan_name))?;
            aggregations.push(expr_to_aggregation(expr, out_col)?);
        }
    } else if agg.aggr_expr.len() == 1 {
        // One aggregate renamed by the view's declared schema: the mapping is
        // unambiguous even though the names differ.
        let out_col = agg_output_cols.first()?;
        let expr = agg.aggr_expr.first()?;
        aggregations.push(expr_to_aggregation(expr, out_col)?);
    } else {
        // Several aggregates whose names do not match the declared schema:
        // there is no way to know which column each one feeds. Degrade to
        // DiffBased (full recompute, right answer) rather than guess.
        return None;
    }

    // AUD-3: honor the view's declared output column types (SUM(Int64)→Int64
    // unless the view declares otherwise) so the incremental snapshot matches
    // the registered contract.
    let op = IncrementalAggOp::new_with_output_schema(
        input_schema,
        group_by,
        aggregations,
        output_schema,
    )
    .ok()?;
    Some(ViewPlan::Aggregate { source, op, filter })
}

// ── Join plan builder ─────────────────────────────────────────────────────────

fn build_join_plan(
    join: &Join,
    outer_filter: Option<&Expr>,
    output_schema: &SchemaRef,
    available_schemas: &AHashMap<String, SchemaRef>,
    lateness: &[krishiv_delta::LatenessSpec],
    projection: Option<(&[Expr], &DFSchema)>,
) -> Option<ViewPlan> {
    let incr_join_type = match join.join_type {
        JoinType::Inner => IncrJoinType::Inner,
        JoinType::Left => IncrJoinType::LeftOuter,
        other => {
            tracing::warn!(
                join_type = ?other,
                "IVM plan degraded to O(state) DiffBased: {:?} join is not \
                 supported by the incremental join operator; only INNER and \
                 LEFT OUTER run in O(Δ) mode",
                other
            );
            return None;
        }
    };

    // AUD-1: resolve each side's source plus any WHERE predicate on that side
    // (e.g. a filtered subquery join input). A predicate that fails to compile
    // bails the whole join to DiffBased rather than dropping the filter.
    let (left_source, left_side_preds) = resolve_source_with_filters(&join.left)
        .or_else(|| source_of_plan(&join.left).map(|s| (s, Vec::new())))?;
    let (right_source, right_side_preds) = resolve_source_with_filters(&join.right)
        .or_else(|| source_of_plan(&join.right).map(|s| (s, Vec::new())))?;
    let left_schema = available_schemas.get(&left_source)?;
    let right_schema = available_schemas.get(&right_source)?;

    let mut left_key_cols: Vec<String> = Vec::new();
    let mut right_key_cols: Vec<String> = Vec::new();

    for (left_expr, right_expr) in &join.on {
        left_key_cols.push(expr_col_name(left_expr)?);
        right_key_cols.push(expr_col_name(right_expr)?);
    }

    // #160: the SQL planner leaves the ON condition in `join.filter` (the
    // optimizer pass that lifts equi-pairs into `join.on` never runs on the
    // unoptimized plan inspected here) — so before this, every SQL-registered
    // join silently degraded to DiffBased. Equi conjuncts become trace keys;
    // BAND-1: anything else (a `BETWEEN` band, an expression comparison) is
    // collected and compiled below as a residual over the joined relation —
    // the trace stays keyed on the equi columns and the residual filters the
    // probe's output, which is linear and therefore delta-correct.
    let mut residual_conjuncts: Vec<Expr> = Vec::new();
    if let Some(filter) = &join.filter {
        for conjunct in datafusion::logical_expr::utils::split_conjunction(filter) {
            let equi = match strip_alias(conjunct) {
                Expr::BinaryExpr(be) if be.op == datafusion::logical_expr::Operator::Eq => {
                    match (strip_alias(&be.left), strip_alias(&be.right)) {
                        (Expr::Column(a), Expr::Column(b)) => {
                            let a_left = join.left.schema().index_of_column(a).is_ok();
                            let b_left = join.left.schema().index_of_column(b).is_ok();
                            match (a_left, b_left) {
                                (true, false) => Some((a.name.clone(), b.name.clone())),
                                (false, true) => Some((b.name.clone(), a.name.clone())),
                                // Same-side equality: a residual, not a key.
                                _ => None,
                            }
                        }
                        _ => None,
                    }
                }
                _ => None,
            };
            match equi {
                Some((l, r)) => {
                    left_key_cols.push(l);
                    right_key_cols.push(r);
                }
                None => residual_conjuncts.push((*conjunct).clone()),
            }
        }
    }
    // #160: decompose a `WHERE` above the join by side. Filter is linear, so
    // a conjunct over one side's columns filters that side's delta before the
    // probe. Cross-side conjuncts cannot be pushed; under LEFT OUTER a
    // right-side conjunct would change null-padding semantics (it makes the
    // join effectively inner) — both bail to DiffBased.
    let mut left_preds = left_side_preds;
    let mut right_preds = right_side_preds;
    if let Some(filter) = outer_filter {
        for conjunct in datafusion::logical_expr::utils::split_conjunction(filter) {
            let cols = conjunct.column_refs();
            if cols.is_empty() {
                return None;
            }
            let all_left = cols
                .iter()
                .all(|c| join.left.schema().index_of_column(c).is_ok());
            let all_right = cols
                .iter()
                .all(|c| join.right.schema().index_of_column(c).is_ok());
            if all_left {
                left_preds.push((*conjunct).clone());
            } else if all_right && incr_join_type == IncrJoinType::Inner {
                right_preds.push((*conjunct).clone());
            } else if incr_join_type == IncrJoinType::Inner {
                // JOIN-2: cross-side conjuncts. For INNER, `WHERE` over the
                // join is the same relation as the same condition in `ON` — a
                // comma join (`FROM a, b WHERE a.k = b.k`) IS an equi-join
                // spelled in the WHERE. A plain cross-side column equality
                // becomes a trace key; anything else joins the BAND-1
                // residual over the joined relation. LEFT OUTER keeps the
                // refusal above: its WHERE is post-padding and neither
                // classification preserves that.
                let equi = match strip_alias(conjunct) {
                    Expr::BinaryExpr(be) if be.op == datafusion::logical_expr::Operator::Eq => {
                        match (strip_alias(&be.left), strip_alias(&be.right)) {
                            (Expr::Column(a), Expr::Column(b)) => {
                                let a_left = join.left.schema().index_of_column(a).is_ok();
                                let b_left = join.left.schema().index_of_column(b).is_ok();
                                match (a_left, b_left) {
                                    (true, false) => Some((a.name.clone(), b.name.clone())),
                                    (false, true) => Some((b.name.clone(), a.name.clone())),
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                };
                match equi {
                    Some((l, r)) => {
                        left_key_cols.push(l);
                        right_key_cols.push(r);
                    }
                    None => residual_conjuncts.push((*conjunct).clone()),
                }
            } else {
                return None;
            }
        }
    }
    // The guards run AFTER both the ON and the WHERE classification loops: a
    // comma join carries its equi keys only in the WHERE, so checking
    // `left_key_cols` before reading the outer filter refused every
    // `FROM a, b WHERE a.k = b.k` outright (JOIN-2's original failure).
    // An ON/WHERE residual under LEFT OUTER decides *matching* — a
    // band-failing pair still owes a null-padded left row — which a
    // post-probe filter cannot express. Refuse rather than change the
    // query's meaning.
    if !residual_conjuncts.is_empty() && incr_join_type != IncrJoinType::Inner {
        return None;
    }
    if left_key_cols.is_empty() {
        return None;
    }

    // Outer-filter columns are qualified by the join-side relation (a table
    // alias, e.g. `t.dist`), which the source-schema compile below cannot
    // resolve — strip qualifiers so they bind by bare name.
    let left_filter =
        compile_source_filter(&unqualify_columns(&left_preds)?, &left_source, left_schema).ok()?;
    let right_filter = compile_source_filter(
        &unqualify_columns(&right_preds)?,
        &right_source,
        right_schema,
    )
    .ok()?;

    // IVM-AUD-CORE-5: build the traces WITH the lateness column when the view
    // declares one that both sides carry. Without it `Trace::gc_below_watermark`
    // early-returns on `lateness_col_idx == None`, so the entire per-tick
    // watermark-GC loop was a no-op and join traces grew unbounded — the exact
    // failure `join.rs` documents ("without calling `with_lateness_column`,
    // `gc_below_watermark` is a universal no-op"). Both sides must carry the
    // column: a trace GC'd on one side only would drop rows that the other
    // side can still legitimately match.
    let lateness_col = lateness
        .iter()
        .map(|l| l.column.as_str())
        .find(|col| left_schema.index_of(col).is_ok() && right_schema.index_of(col).is_ok());
    if lateness_col.is_none() && !lateness.is_empty() {
        tracing::debug!(
            "LATENESS declared but no column is present on both join sides; \
             join traces will not be watermark-GC'd"
        );
    }
    let op = IncrementalJoinOp::new_with_lateness(
        left_schema.clone(),
        right_schema.clone(),
        left_key_cols.clone(),
        right_key_cols.clone(),
        incr_join_type,
        lateness_col,
    )
    .ok()?;

    // BAND-1: the residual and any projection compile against the JOINED
    // relation — the plan's own qualified fields (so `p."dateTime"` and
    // `a."dateTime"` both resolve without rewriting a single expression),
    // minus the right key columns the operator does not emit, so compiled
    // column indices align with the emitted `left ++ right-non-key` batch
    // exactly. A reference to a dropped right key fails the compile and the
    // plan bails to DiffBased rather than guessing.
    let joined_schema = {
        let mut qfields: Vec<(Option<datafusion::common::TableReference>, Arc<Field>)> = Vec::new();
        for (q, f) in join.left.schema().iter() {
            qfields.push((q.cloned(), Arc::clone(f)));
        }
        for (q, f) in join.right.schema().iter() {
            if !right_key_cols.iter().any(|k| k == f.name()) {
                qfields.push((q.cloned(), Arc::clone(f)));
            }
        }
        DFSchema::new_with_metadata(qfields, std::collections::HashMap::new()).ok()?
    };
    // JOIN-2: a reference to a dropped right KEY column rewrites to its
    // paired left key — the two are equal on every inner-joined row, so the
    // rewrite is exact (INNER is guaranteed here for residuals; for the post
    // it also holds because a LEFT OUTER pads the right side with NULL only
    // in non-key... no: a LEFT OUTER unmatched row has a NULL right key while
    // the left key is not NULL, so the rewrite is INNER-only and the compile
    // must refuse otherwise). Without it, `SELECT *` over a join — whose
    // identity projection repeats the right key — could never compile a post.
    let rewrite_right_keys = |e: Expr| -> Option<Expr> {
        use datafusion::common::tree_node::{Transformed, TreeNode as _};
        let rewritten = e
            .transform(|node| {
                if let Expr::Column(c) = &node
                    && joined_schema.index_of_column(c).is_err()
                    && join.right.schema().index_of_column(c).is_ok()
                    && let Some(pos) = right_key_cols.iter().position(|k| k == &c.name)
                    && let Some(col) = left_key_cols.get(pos).and_then(|left_name| {
                        // Qualify by the left side's own qualifier.
                        join.left
                            .schema()
                            .iter()
                            .find(|(_, f)| f.name() == left_name)
                            .map(|(q, f)| datafusion::common::Column::new(q.cloned(), f.name()))
                    })
                {
                    return Ok(Transformed::yes(Expr::Column(col)));
                }
                Ok(Transformed::no(node))
            })
            .ok()?;
        if rewritten.transformed && incr_join_type != IncrJoinType::Inner {
            return None;
        }
        Some(rewritten.data)
    };

    let props = ExecutionProps::new();
    let residual = if residual_conjuncts.is_empty() {
        None
    } else {
        let combined = residual_conjuncts.iter().cloned().reduce(|a, b| a.and(b))?;
        let combined = rewrite_right_keys(combined)?;
        let mut coercion = TypeCoercionRewriter::new(&joined_schema);
        let coerced = combined.rewrite(&mut coercion).ok()?.data;
        let predicate = create_physical_expr(&coerced, &joined_schema, &props).ok()?;
        Some(SourceFilter { predicate })
    };
    let post = match projection {
        None => None,
        Some((exprs, proj_schema)) => {
            if proj_schema.fields().len() != exprs.len() {
                return None;
            }
            let mut compiled: Vec<(String, Arc<dyn PhysicalExpr>)> =
                Vec::with_capacity(exprs.len());
            let mut fields: Vec<Field> = Vec::with_capacity(exprs.len());
            for (expr, planned) in exprs.iter().zip(proj_schema.fields().iter()) {
                let expr = rewrite_right_keys(expr.clone())?;
                let mut coercion = TypeCoercionRewriter::new(&joined_schema);
                let coerced = expr.rewrite(&mut coercion).ok()?.data;
                let physical = create_physical_expr(&coerced, &joined_schema, &props).ok()?;
                compiled.push((planned.name().clone(), physical));
                // Names and types from the planner's own projection schema —
                // never re-derived (CORE-23 / MAP-TYPE-1).
                fields.push(Field::new(
                    planned.name(),
                    planned.data_type().clone(),
                    planned.is_nullable(),
                ));
            }
            Some(MapOp {
                exprs: compiled,
                output_schema: Arc::new(Schema::new(fields)),
                predicate: None,
            })
        }
    };

    // IVM-AUD-SCHEMA-1: the operator emits all left columns plus the right's
    // non-key columns; a post-projection (BAND-1) reshapes that to the
    // projection's own relation. Whichever applies last is what readers see,
    // so that is what must match the declaration — the guard now passes for a
    // projected join because the post genuinely emits the declared relation,
    // not because the check got weaker.
    let emitted: SchemaRef = match &post {
        Some(m) => m.output_schema.clone(),
        None => op.output_schema().clone(),
    };
    if !emits_declared_relation(&emitted, output_schema) {
        tracing::warn!(
            left = %left_source,
            right = %right_source,
            "IVM plan degraded to O(state) DiffBased: the join emits {} which \
             does not match this view's declared output columns",
            if post.is_some() {
                "the compiled post-projection's relation"
            } else {
                "left ++ right-non-key columns"
            }
        );
        return None;
    }

    Some(ViewPlan::Join {
        left_source,
        right_source,
        op,
        left_filter,
        right_filter,
        residual,
        post,
    })
}

/// True when a join side resolves to a plain source (optionally filtered or
/// alias-wrapped) — the only sides [`build_join_plan`] accepts. The decomposer
/// checks this BEFORE synthesising a join-leaf hop: a multi-way join's side is
/// itself a join, and unparsing + replanning that whole tree just to have the
/// hop refused recurses DataFusion's planner deep enough to overflow a
/// default-size thread stack (found by q2's five-way comma join).
pub(crate) fn side_resolves_to_source(plan: &LogicalPlan) -> bool {
    resolve_source_with_filters(plan).is_some() || source_of_plan(plan).is_some()
}

/// Peel `Alias` wrappers off an expression (planners wrap freely).
fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(&alias.expr),
        other => other,
    }
}

/// Rewrite every column reference to its bare (unqualified) name so an
/// expression compiles against the source's data schema regardless of the
/// SQL-side table alias. Sound because every caller compiles against exactly
/// one relation, where a bare name is unambiguous: the join builder for
/// predicates lifted from above the join, and the map builder
/// (IVM-AUD-ALIAS-1) for projection expressions and WHERE predicates, whose
/// plan-side references carry the alias (`a.amount`) while the compilation
/// schema is qualified by the table name.
fn unqualify_columns(preds: &[Expr]) -> Option<Vec<Expr>> {
    use datafusion::common::Column;
    use datafusion::common::tree_node::{Transformed, TreeNode as _};
    preds
        .iter()
        .map(|p| {
            p.clone()
                .transform(|e| {
                    Ok(match e {
                        Expr::Column(c) => {
                            Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))
                        }
                        other => Transformed::no(other),
                    })
                })
                .map(|t| t.data)
                .ok()
        })
        .collect()
}

// ── Source resolution ─────────────────────────────────────────────────────────

/// Walk a plan tree to find the single base table scan, returning its name.
/// Returns `None` for multi-input plans (joins, unions) or unsupported nodes.
///
/// AUD-1: this **refuses to peel `Filter` nodes** (and a `TableScan` carrying
/// pushed-down `filters`). Previously it peeled any single-input node including
/// `Filter`, so the operator was built against a source whose `WHERE` was
/// silently discarded. The filter-aware `resolve_source_with_filters` handles
/// the clean-chain case in O(Δ); anything that reaches a `Filter` here returns
/// `None`, correctly degrading the view to DiffBased full recompute.
/// Does the relation an operator will emit match what the view declared?
///
/// IVM-AUD-SCHEMA-1. Every incremental operator emits its own *natural*
/// relation — `DISTINCT` emits whole source rows, the join emits left ++ right
/// non-key columns — and a `Projection` sitting above it in the logical plan is
/// not part of the operator at all. `source_of_plan`'s catch-all peels any
/// single-input node, projections included, so a projected view resolved to a
/// source and then published the operator's wider relation while reporting
/// `Incremental` with an empty `degraded_views`. Refusing the plan here sends
/// the view to DiffBased, which computes the projection correctly.
///
/// Nullability is deliberately not compared: a declared output schema routinely
/// marks columns nullable where the source does not, and that difference does
/// not change which rows or columns are emitted.
///
/// Nor is the *physical encoding* of a string or binary column. A view's
/// declared schema comes from DataFusion's planned output — which since DF 54
/// is `Utf8View` — while an operator emits whatever the source column
/// physically holds, typically `Utf8`. Those are the same logical column, and
/// requiring them to be byte-identical rejected a correct `GROUP BY` on the
/// resident-executor path (caught by
/// `resident_group_by_aggregate_first_tick_emits_delta`). What this must catch
/// is the *wrong columns* — a different count, different names, or a
/// numerically different type such as Int64 where Float64 was declared, which
/// really would change the values. Encoding is not that.
/// Column *order* is not compared either, and that is not laxness — it is the
/// contract IVM-AUD-CORE-23 established. An aggregate view's operator emits in
/// SELECT order while its declared schema may list the same columns in another
/// order, and the pairing between them is **by name**; readers take the
/// snapshot's columns with `column_by_name`. Comparing positionally rejected
/// the very test that pins that contract
/// (`aggregates_follow_their_names_not_their_positions`, whose entire point is
/// that SELECT order `(total, cnt)` and declared order `(cnt, total)` both
/// work). Matching by name still catches what this guard is for: a projected
/// view emits a different *set* of columns, not the same set reordered.
pub(crate) fn emits_declared_relation(emitted: &SchemaRef, declared: &SchemaRef) -> bool {
    let emitted = emitted.fields();
    if emitted.len() != declared.fields().len() {
        return false;
    }
    // Greedy match against a used-mask rather than a name->type map, so a
    // relation that repeats a column name (a join can) has to bring the same
    // multiplicity on both sides instead of collapsing to one entry.
    let mut used = vec![false; emitted.len()];
    declared.fields().iter().all(|d| {
        let hit = emitted.iter().enumerate().position(|(i, e)| {
            used.get(i) == Some(&false)
                && e.name() == d.name()
                && same_logical_type(e.data_type(), d.data_type())
        });
        match hit {
            Some(i) => {
                if let Some(slot) = used.get_mut(i) {
                    *slot = true;
                }
                true
            }
            None => false,
        }
    })
}

/// Equal, treating the interchangeable Arrow encodings of one logical type as
/// one type. Deliberately narrow: only the string and binary view/large
/// variants, which are representation choices. Numeric widths are NOT unified —
/// Int64 where Float64 was declared is a real difference in the values a caller
/// reads, and is exactly the kind of thing this guard exists to surface.
fn same_logical_type(a: &DataType, b: &DataType) -> bool {
    use DataType::{Binary, BinaryView, LargeBinary, LargeUtf8, Utf8, Utf8View};
    match (a, b) {
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View) => true,
        (Binary | LargeBinary | BinaryView, Binary | LargeBinary | BinaryView) => true,
        _ => a == b,
    }
}

fn source_of_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(ts) if ts.filters.is_empty() => {
            Some(ts.table_name.table().to_string())
        }
        // A scan with pushed-down predicates or a Filter node would mean a
        // dropped WHERE — never resolve through it.
        LogicalPlan::TableScan(_) | LogicalPlan::Filter(_) => None,
        // A relation-level rename changes nothing about the columns.
        LogicalPlan::SubqueryAlias(sa) => source_of_plan(&sa.input),
        // IVM-AUD-RESOLVE-1: a projection may be peeled ONLY if every output
        // name still means the same column underneath. The planner inserts
        // identity projections that must be seen through (IVM-AUD-TOPN-1), but
        // `SELECT amount * 2 AS amount` rebinds the name — and resolving
        // through it makes the operator read the *raw* column while the view
        // promises the computed one.
        LogicalPlan::Projection(p) if projection_preserves_column_meaning(p) => {
            source_of_plan(&p.input)
        }
        // Everything else is refused rather than peeled. The old catch-all
        // peeled ANY single-input node and recursed, which resolved a
        // computing projection, a nested Aggregate and a Distinct alike
        // straight to the base table: the operator then computed against the
        // raw relation, the answer was wrong, and BOTH the plan-time
        // `emits_declared_relation` check and the per-tick
        // `OutputSchemaMismatch` tripwire passed, because the *shape* was
        // right and only the *values* were wrong. Refusing costs a fallback to
        // DiffBased, which is correct and slower.
        _ => None,
    }
}

/// True when a projection re-exposes columns without rebinding any name — the
/// only projection shape [`source_of_plan`] may resolve through.
///
/// Reordering and dropping columns are fine: a name that survives still refers
/// to the same underlying column. Computing (`a * 2 AS a`) and cross-renaming
/// (`other AS amount`) are not.
fn projection_preserves_column_meaning(p: &datafusion::logical_expr::Projection) -> bool {
    p.expr.iter().all(|e| match e {
        Expr::Column(_) => true,
        Expr::Alias(a) => matches!(a.expr.as_ref(), Expr::Column(c) if c.name == a.name),
        _ => false,
    })
}

/// Resolve the single base source under `plan`, collecting the `Filter`
/// predicates between the operator and that source. Only `SubqueryAlias` and
/// `Filter` nodes are peeled; a clean `Scan` (with no pushed-down filters) ends
/// the walk. Any other node (a projection with computed columns, sort, limit,
/// nested aggregate, multi-input) returns `None`, so the caller falls back to
/// `source_of_plan` or DiffBased.
fn resolve_source_with_filters(plan: &LogicalPlan) -> Option<(String, Vec<Expr>)> {
    match plan {
        LogicalPlan::TableScan(ts) if ts.filters.is_empty() => {
            Some((ts.table_name.table().to_string(), Vec::new()))
        }
        LogicalPlan::SubqueryAlias(sa) => resolve_source_with_filters(&sa.input),
        LogicalPlan::Filter(f) => {
            let (src, mut preds) = resolve_source_with_filters(&f.input)?;
            preds.push(f.predicate.clone());
            Some((src, preds))
        }
        _ => None,
    }
}

/// Compile collected predicates (AND-combined) into a [`SourceFilter`] against
/// the source's data schema.
///
/// - `Ok(None)`  — no predicates, no filtering needed.
/// - `Ok(Some)`  — compiled successfully.
/// - `Err(())`   — the predicate could not be compiled; the caller must fall
///   back to DiffBased rather than silently drop it.
fn compile_source_filter(
    preds: &[Expr],
    source: &str,
    source_schema: &SchemaRef,
) -> Result<Option<SourceFilter>, ()> {
    if preds.is_empty() {
        return Ok(None);
    }
    // IVM-AUD-ALIAS-1: a WHERE under `FROM orders AS a` references `a.region`,
    // which cannot resolve against the table-qualified schema built below —
    // same qualifier mismatch as the map's projection expressions, same silent
    // DiffBased degrade. One relation, so bare names are unambiguous.
    let preds = unqualify_columns(preds).ok_or(())?;
    let combined = preds.iter().cloned().reduce(|a, b| a.and(b)).ok_or(())?;
    // Qualify the schema with the source name so predicate column references of
    // either `source.col` or bare `col` resolve to the right column index.
    let df_schema =
        DFSchema::try_from_qualified_schema(source, source_schema.as_ref()).map_err(|_| ())?;
    // The unoptimized logical predicate is not type-coerced, so a `Float64 >
    // Int64` literal comparison would fail the Arrow comparison kernel at eval.
    // Run type coercion against the source schema to insert the needed casts
    // before lowering to a physical expression.
    let mut coercion = TypeCoercionRewriter::new(&df_schema);
    let coerced = combined.rewrite(&mut coercion).map_err(|_| ())?.data;
    let props = ExecutionProps::new();
    let predicate = create_physical_expr(&coerced, &df_schema, &props).map_err(|_| ())?;
    Ok(Some(SourceFilter { predicate }))
}

// ── Expr helpers ─────────────────────────────────────────────────────────────

fn expr_col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::Alias(alias) => expr_col_name(&alias.expr),
        _ => None,
    }
}

fn expr_to_aggregation(expr: &Expr, output_col: &str) -> Option<Aggregation> {
    match expr {
        Expr::Alias(alias) => expr_to_aggregation(&alias.expr, output_col),
        Expr::AggregateFunction(agg_fn) => {
            // IVM-AUD-CORE-22: `AggregateFunctionParams` carries `distinct`,
            // `filter`, `order_by` and `null_treatment`, and NONE of them was
            // read — so `COUNT(DISTINCT user_id)` lowered to a plain
            // incremental COUNT and `SUM(x) FILTER (WHERE y > 0)` to an
            // unfiltered SUM, both silently wrong. This is exactly the
            // MIN_BY/MAX_BY class the code below already guards against; the
            // guard was written for that one case and never generalized.
            //
            // Refusing to build an incremental plan degrades the view to
            // DiffBased (full recompute + diff), which is slower and CORRECT.
            // A wrong answer computed quickly is not a trade worth making.
            if agg_fn.params.distinct {
                // CDIST-1: COUNT(DISTINCT col) over a plain column now HAS the
                // per-value multiplicity a Z-set retraction needs — it shares
                // MIN/MAX's value multiset and counts positive-weight entries.
                // Every other DISTINCT aggregate (SUM/AVG DISTINCT, computed
                // args) still refuses; the multiset holds values, not the
                // arithmetic over them.
                // The early return must not skip the FILTER/ORDER BY guards
                // below — silently dropping a FILTER on a lowered aggregate is
                // the exact CORE-22 shape this function exists to prevent.
                if agg_fn.func.name().to_lowercase() == "count"
                    && agg_fn.params.args.len() == 1
                    && agg_fn.params.filter.is_none()
                    && agg_fn.params.order_by.is_empty()
                    && let Some(input_col) = agg_fn.params.args.first().and_then(expr_col_name)
                {
                    return Some(Aggregation::CountDistinct {
                        input_col,
                        output_col: output_col.to_string(),
                    });
                }
                tracing::warn!(
                    output_col,
                    "IVM plan degraded to O(state) DiffBased: DISTINCT inside a \
                     non-COUNT aggregate has no incremental operator (the value \
                     multiset holds values, not the arithmetic over them)"
                );
                return None;
            }
            if agg_fn.params.filter.is_some() {
                tracing::warn!(
                    output_col,
                    "IVM plan degraded to O(state) DiffBased: FILTER (WHERE …) on \
                     an aggregate is not applied by the incremental operators"
                );
                return None;
            }
            if !agg_fn.params.order_by.is_empty() {
                tracing::warn!(
                    output_col,
                    "IVM plan degraded to O(state) DiffBased: ORDER BY inside an \
                     aggregate is order-sensitive and the incremental operators \
                     are not"
                );
                return None;
            }
            let func_name = agg_fn.func.name().to_lowercase();
            match func_name.as_str() {
                "sum" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Sum {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                "count" => {
                    // IVM-6: COUNT(col) excludes nulls; COUNT(*) counts all rows.
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name);
                    Some(Aggregation::Count {
                        output_col: output_col.to_string(),
                        input_col,
                    })
                }
                "avg" | "mean" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Avg {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                // NOT min_by/max_by: those return the value of arg0 at the
                // extremum of arg1, which plain Min/Max over arg0 silently
                // mis-computes — they must degrade to DiffBased.
                "min" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Min {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                "max" => {
                    let input_col = agg_fn.params.args.first().and_then(expr_col_name)?;
                    Some(Aggregation::Max {
                        input_col,
                        output_col: output_col.to_string(),
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    fn join_ctx_and_schemas() -> (SessionContext, AHashMap<String, SchemaRef>, SchemaRef) {
        let orders_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]));
        let customers_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let orders = RecordBatch::try_new(
            orders_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let customers = RecordBatch::try_new(
            customers_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "orders",
            Arc::new(MemTable::try_new(orders_schema.clone(), vec![vec![orders]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "customers",
            Arc::new(MemTable::try_new(customers_schema.clone(), vec![vec![customers]]).unwrap()),
        )
        .unwrap();
        let mut schemas = AHashMap::new();
        schemas.insert("orders".to_string(), orders_schema);
        schemas.insert("customers".to_string(), customers_schema);
        let out_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        (ctx, schemas, out_schema)
    }

    async fn plan_for(sql: &str) -> ViewPlan {
        let (_ctx, schemas, out_schema) = join_ctx_and_schemas();
        build_view_plan(sql, &out_schema, &schemas, &[]).await
    }

    /// #160 regression pin: the SQL planner leaves the ON condition in
    /// `join.filter`, so this shape must still lower to the incremental
    /// operator. Before the fix every SQL join view silently ran DiffBased.
    #[tokio::test]
    async fn sql_inner_join_lowers_to_incremental() {
        let plan = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id",
        )
        .await;
        assert_eq!(plan.kind(), ViewPlanKind::Incremental);
        let ViewPlan::Join {
            left_source,
            right_source,
            left_filter,
            right_filter,
            ..
        } = plan
        else {
            panic!("expected a join plan");
        };
        assert_eq!(
            (left_source.as_str(), right_source.as_str()),
            ("orders", "customers")
        );
        assert!(left_filter.is_none() && right_filter.is_none());
    }

    /// A WHERE above the join whose conjuncts each touch one side pushes onto
    /// that side's delta filter (O(Δ) preserved).
    #[tokio::test]
    async fn where_above_join_pushes_per_side_filters() {
        let plan = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             WHERE orders.order_id > 10 AND customers.name = 'Alice'",
        )
        .await;
        let ViewPlan::Join {
            left_filter,
            right_filter,
            ..
        } = plan
        else {
            panic!("expected a join plan, got DiffBased");
        };
        assert!(left_filter.is_some(), "left-side WHERE conjunct pushed");
        assert!(right_filter.is_some(), "right-side WHERE conjunct pushed");
    }

    /// Right-side WHERE above a LEFT OUTER join changes null-padding
    /// semantics — must degrade, never push.
    #[tokio::test]
    async fn left_outer_with_right_side_where_degrades() {
        let plan = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders LEFT JOIN customers ON orders.customer_id = customers.customer_id \
             WHERE customers.name = 'Alice'",
        )
        .await;
        assert_eq!(plan.kind(), ViewPlanKind::DiffBased);
    }

    /// Non-equi and cross-side predicates cannot be keyed — degrade.
    #[tokio::test]
    async fn non_equi_and_cross_side_predicates_degrade() {
        let non_equi = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id < customers.customer_id",
        )
        .await;
        assert_eq!(non_equi.kind(), ViewPlanKind::DiffBased);
        // JOIN-2 re-bless: an equi key plus a cross-side WHERE comparison now
        // maintains O(Δ) — the equality keys the trace and the comparison
        // compiles as a residual over the joined relation. The pure non-equi
        // case above stays refused: with no equality there is nothing to key
        // the trace on.
        let cross_side = plan_for(
            "SELECT orders.order_id, orders.customer_id, customers.name \
             FROM orders JOIN customers ON orders.customer_id = customers.customer_id \
             WHERE orders.order_id > customers.customer_id",
        )
        .await;
        assert_eq!(cross_side.kind(), ViewPlanKind::Incremental);
    }

    /// Regression (crate-12 audit, A-class): MIN_BY/MAX_BY return the value of
    /// arg0 at the extremum of arg1 — the previous mapping to plain Min/Max of
    /// arg0 silently computed the wrong answer on the O(Δ) path. They must
    /// degrade to DiffBased.
    #[tokio::test]
    async fn min_by_max_by_degrade_to_diff_based() {
        let (_ctx, schemas, _) = join_ctx_and_schemas();
        let out_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, true),
            Field::new("first_order", DataType::Int32, true),
        ]));
        let plan = build_view_plan(
            "SELECT customer_id, MIN_BY(order_id, order_id) AS first_order \
             FROM orders GROUP BY customer_id",
            &out_schema,
            &schemas,
            &[],
        )
        .await;
        assert_eq!(
            plan.kind(),
            ViewPlanKind::DiffBased,
            "MIN_BY must not lower to the incremental Min operator"
        );
        let plan = build_view_plan(
            "SELECT customer_id, MAX_BY(order_id, order_id) AS last_order \
             FROM orders GROUP BY customer_id",
            &out_schema,
            &schemas,
            &[],
        )
        .await;
        assert_eq!(plan.kind(), ViewPlanKind::DiffBased);
    }
}
