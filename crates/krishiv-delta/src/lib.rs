#![forbid(unsafe_code)]

//! `krishiv-delta` — Incremental computing engine for Krishiv.
//!
//! This crate implements the algebraic incremental view maintenance layer
//! inspired by Feldera/DBSP (Z-set bilinear operators, Spine-style Trace) and
//! CocoIndex (behavior versioning, convergent roll-forward commit, tiered
//! change detection).
//!
//! # Core Concepts
//!
//! * [`DeltaBatch`] — A weighted Arrow `RecordBatch` where each row has an
//!   `i64` weight: `+1` = insertion, `-1` = retraction, `0` = cancelled.
//!
//! * [`Trace`] — A Spine-style sorted merge-tree that accumulates
//!   `DeltaBatch`es across ticks. Used by stateful operators (join, aggregate)
//!   to probe historical state efficiently.
//!
//! * [`LogicFingerprint`] — A 64-bit hash of `(operator_uid, behavior_version)`.
//!   When bumped, it signals that an operator's logic has changed and its
//!   cached Trace state should be discarded and recomputed.
//!
//! * [`WatermarkTracker`] — LATENESS-based watermark. Records arriving below
//!   `max_ts - lateness_ms` are dropped; Trace entries below the watermark
//!   are eligible for GC.
//!
//! * [`CoalescingMap`] — Deduplicates rapid source updates: multiple updates
//!   to the same key within one tick are collapsed to the latest value.
//!
//! # Operator library
//!
//! | Operator | Module | Kind |
//! |---|---|---|
//! | map / project | `operators::map` | Linear (no state) |
//! | filter | `operators::filter` | Linear (no state) |
//! | consolidate | `operators::consolidate` | Linear (no state) |
//! | join | `operators::join` | Bilinear (two Traces) |
//! | aggregate | `operators::aggregate` | Nonlinear (running state) |
//! | distinct | `operators::distinct` | Nonlinear (count map) |
//! | recursive | `operators::recursive` | Fixed-point loop |

pub mod behavior_version;
pub mod coalesce;
pub mod delta_batch;
pub mod error;
pub mod lateness;
pub mod operators;
pub mod trace;
pub mod view;

#[cfg(test)]
mod gap_tests;

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use behavior_version::{LogicFingerprint, MemoKey};
pub use coalesce::CoalescingMap;
pub use delta_batch::{
    DeltaBatch, WEIGHT_COLUMN, Weight, deserialize_delta_batch, serialize_delta_batch,
};
pub use error::{DeltaError, DeltaResult};
pub use lateness::{LatenessSpec, SourceOrdinal, WatermarkTracker};
pub use operators::aggregate::{Aggregation, IncrementalAggOp};
pub use operators::consolidate::{ConsolidateOp, consolidate_batch};
pub use operators::distinct::IncrementalDistinctOp;
pub use operators::filter::{FilterOp, FilterValue, filter_batch};
pub use operators::join::{IncrJoinType, IncrementalJoinOp};
pub use operators::map::{ProjectOp, map_batch, project_batch};
pub use operators::recursive::{DEFAULT_MAX_ITERATIONS, RecursiveOp};
pub use operators::stream::{IntegrateOp, apply_delta, differentiate};
pub use trace::Trace;
pub use view::{IncrementalView, IncrementalViewRegistry, IncrementalViewSpec};
