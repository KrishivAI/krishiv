//! The streaming driver kernel: one place that answers *when* to step.
//!
//! The operator core is shared across every streaming placement in this tree —
//! one [`ContinuousWindowExecutor`](crate::continuous::ContinuousWindowExecutor),
//! one set of window operators. The **loops** that drive it are not shared, and
//! every silent wrong answer this codebase has produced in streaming came from
//! that gap: a decision made in one loop and quietly absent from another.
//!
//! This module does not merge the loops. Their lifecycles genuinely differ — a
//! cycle task exists for one invocation, the run-loop is long-lived across
//! peers and barriers, the seam holds no local operator at all — and a driver
//! that *called* the loops could not survive those differences. Instead each
//! loop **holds** a [`StreamDriver`] and borrows its operator to it per call, so
//! ownership never moves and no loop has to restructure to adopt it.
//!
//! ## What makes this different from a shared helper
//!
//! A shared helper is opt-in: a loop that never calls it is indistinguishable
//! from a loop that does not need it, and nothing in the code records which is
//! which. That is exactly how `coerce_batch_for_window` came to have one caller
//! out of five and `flush_all` two.
//!
//! Here the decision is a **value**, not a call. [`StreamingLoop`] is closed,
//! [`StreamingLoop::policy`] is an exhaustive `match`, and [`DriverPolicy`] has
//! no `Default`, no `#[non_exhaustive]`, and no builder. A sixth loop does not
//! compile until it answers every axis, and an answer that contradicts itself
//! fails the build through [`const_coherence`].
//!
//! ## The honest ceiling
//!
//! This makes axes exhaustive once **named**, not self-detecting. Someone who
//! invents a genuinely new decision and implements it in one loop without adding
//! a `DriverPolicy` field is not caught here — the gate covers adding *loops*,
//! not inventing *decisions*. The counterweight is the cross-loop corpus in
//! [`crate::streaming_corpus`]: a new decision that changes output surfaces as a
//! divergence there, and one that does not change output was arguably not an
//! axis.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use krishiv_plan::window::WindowExecutionSpec;

use crate::continuous::ContinuousWindowExecutor;
use crate::{ExecError, ExecResult};

// ─────────────────────────────────────────────────────────────────────────────
// The axes
// ─────────────────────────────────────────────────────────────────────────────

/// Does this loop advance the watermark on wall-clock time while the source is
/// quiet?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleTick {
    /// No wall clock. Windows close only when an event advances the watermark.
    ///
    /// Correct for a loop that does not own a thread between invocations, and a
    /// genuine functional limit for session windows, which need wall clock to
    /// close on inactivity.
    None,
    /// Advance the watermark to wall-clock time every
    /// [`idle_tick_interval`](krishiv_common::streaming_dials::idle_tick_interval)
    /// of source-idle time.
    WallClock,
}

/// What this loop does with windows still open when it stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndOfStream {
    /// Nothing. Whatever the watermark did not close is not emitted.
    ///
    /// Correct for an unbounded job — its source is never "over", so forcing a
    /// flush would emit a partial aggregate as though it were final. Wrong for a
    /// bounded one, which is the defect this whole effort exists for.
    NoFlush,
    /// Flush every open window when the source is exhausted.
    FlushOnSourceExhausted,
    /// Flush when the control plane says so, because this loop cannot observe
    /// source exhaustion itself.
    FlushOnDirective,
    /// This loop holds no local operator; the flush is the runtime's to perform.
    DelegatedToRuntime,
}

/// Who types the batches this loop hands the operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputTyping {
    /// The driver casts source columns to the window spec's required types.
    CoerceToSpec,
    /// Already coerced before reaching the driver; casting again would be a
    /// wasted pass over every batch on the hot path.
    PreCoerced,
}

/// How long this loop's task lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// One task owns the whole job start to finish.
    OwnsWholeJob,
    /// The task exists for exactly one invocation and is rescheduled per push.
    TransientPerInvocation,
    /// Long-lived, cancelled rather than completed.
    LongLived,
}

/// What this loop does when output accumulates faster than it is consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Egress {
    /// Block until the consumer catches up. No computed window is ever lost.
    Backpressure,
    /// Drop the oldest buffered batches past a cap.
    ///
    /// Lossy by construction: already-computed closed windows are deleted and
    /// never appear in any drain response.
    CappedDropOldest,
}

/// Every decision a driver loop must answer, as one value.
///
/// Deliberately has no `Default`, no `#[non_exhaustive]` and no builder. Each of
/// those would let a new loop inherit an answer instead of giving one, which is
/// the hole this type exists to close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverPolicy {
    /// See [`IdleTick`].
    pub idle_tick: IdleTick,
    /// See [`EndOfStream`].
    pub end_of_stream: EndOfStream,
    /// See [`InputTyping`].
    pub input_typing: InputTyping,
    /// See [`Lifecycle`].
    pub lifecycle: Lifecycle,
    /// See [`Egress`].
    pub egress: Egress,
}

impl DriverPolicy {
    /// Why this combination of answers cannot describe a real loop, if it cannot.
    ///
    /// Checked at compile time for every [`StreamingLoop`] by
    /// [`const_coherence`]. These are not style rules — each one names a
    /// physical impossibility, so a policy that trips one is describing a loop
    /// that cannot exist.
    #[must_use]
    pub const fn incoherence(&self) -> Option<&'static str> {
        if matches!(self.lifecycle, Lifecycle::TransientPerInvocation)
            && matches!(self.idle_tick, IdleTick::WallClock)
        {
            return Some(
                "a transient per-invocation loop owns no thread between invocations, \
                 so nothing can advance a wall clock for it",
            );
        }
        if matches!(self.lifecycle, Lifecycle::LongLived)
            && matches!(self.end_of_stream, EndOfStream::FlushOnSourceExhausted)
        {
            return Some(
                "a long-lived loop's source is never exhausted, so a flush conditioned \
                 on exhaustion can never fire",
            );
        }
        if matches!(self.end_of_stream, EndOfStream::DelegatedToRuntime)
            && matches!(self.lifecycle, Lifecycle::TransientPerInvocation)
        {
            return Some(
                "a transient loop has no runtime handle to delegate a flush to; it is \
                 the thing the runtime schedules",
            );
        }
        None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The closed set of loops
// ─────────────────────────────────────────────────────────────────────────────

/// Every driver loop in this tree.
///
/// Closed on purpose. Adding a variant does not compile until
/// [`StreamingLoop::policy`] and [`StreamingLoop::ordinal`] both answer for it,
/// and [`const_coherence`] rejects an answer that contradicts itself.
///
/// There is intentionally **no** `Test` variant. It would be the escape hatch
/// that makes every one of these answers optional again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingLoop {
    /// `StreamingEngine::run` — embedded, bounded, run-once. Reads its source to
    /// exhaustion and owns the whole job.
    EmbeddedBounded,
    /// `run_streaming_continuous` — embedded, long-lived, stopped by signal.
    EmbeddedContinuous,
    /// `run_streaming_job_via_runtime` — bounded distributed. Pushes through an
    /// `ExecutionRuntime` and holds no local operator.
    RuntimeSeam,
    /// `execute_streaming_fragment` — scheduler-driven cycle (`stream:loop:`).
    /// One invocation per push.
    Cycle,
    /// `execute_run_loop_fragment` — long-lived run-loop (`stream:rloop:`), with
    /// key-group routing across peers and barrier checkpoints.
    RunLoop,
}

impl StreamingLoop {
    /// How many variants exist.
    ///
    /// Manual because `std::mem::variant_count` is unstable.
    ///
    /// **Known limit, stated rather than implied.** Adding a variant produces
    /// three compile errors — [`StreamingLoop::policy`],
    /// [`StreamingLoop::ordinal`] and [`StreamingLoop::name`] are all
    /// exhaustive — so a new loop cannot exist without answering every axis.
    /// But if someone answers all three, gives the variant ordinal 5, and
    /// updates neither `ALL` nor this constant, the coherence check in
    /// [`const_coherence`] never sees it: `ALL.len()` and `VARIANT_COUNT` still
    /// agree at 5. Closing that would need a derive macro; it is not worth one.
    /// The residual exposure is a policy that contradicts itself in a loop
    /// nobody listed, which the cross-loop corpus would still catch the moment
    /// it changed output.
    pub const VARIANT_COUNT: usize = 5;

    /// Every variant, in [`StreamingLoop::ordinal`] order.
    pub const ALL: &'static [StreamingLoop] = &[
        StreamingLoop::EmbeddedBounded,
        StreamingLoop::EmbeddedContinuous,
        StreamingLoop::RuntimeSeam,
        StreamingLoop::Cycle,
        StreamingLoop::RunLoop,
    ];

    /// This loop's answers.
    ///
    /// The exhaustive match here is the gate: a new loop cannot be added to the
    /// enum without deciding, explicitly and in one place, what it does about
    /// idle ticking, end-of-stream, input typing, lifecycle and egress.
    #[must_use]
    pub const fn policy(self) -> DriverPolicy {
        match self {
            // Bounded run-once: reads to exhaustion, then closes what the
            // watermark left open. No wall clock — there is no idle period, the
            // source either yields or is over.
            StreamingLoop::EmbeddedBounded => DriverPolicy {
                idle_tick: IdleTick::None,
                end_of_stream: EndOfStream::FlushOnSourceExhausted,
                input_typing: InputTyping::CoerceToSpec,
                lifecycle: Lifecycle::OwnsWholeJob,
                egress: Egress::Backpressure,
            },
            // Long-lived embedded loop: ticks on idle so session windows can
            // close, and does NOT flush on stop — its source is never over, so a
            // forced flush would publish a partial aggregate as final.
            StreamingLoop::EmbeddedContinuous => DriverPolicy {
                idle_tick: IdleTick::WallClock,
                end_of_stream: EndOfStream::NoFlush,
                input_typing: InputTyping::CoerceToSpec,
                lifecycle: Lifecycle::LongLived,
                egress: Egress::Backpressure,
            },
            // Holds no operator; register/push/drain go through the runtime, and
            // so must the flush.
            StreamingLoop::RuntimeSeam => DriverPolicy {
                idle_tick: IdleTick::None,
                end_of_stream: EndOfStream::DelegatedToRuntime,
                input_typing: InputTyping::CoerceToSpec,
                lifecycle: Lifecycle::OwnsWholeJob,
                egress: Egress::Backpressure,
            },
            // Exists for one invocation, so it cannot observe source exhaustion
            // and cannot own a wall clock. Its flush has to be told to it.
            StreamingLoop::Cycle => DriverPolicy {
                idle_tick: IdleTick::None,
                end_of_stream: EndOfStream::FlushOnDirective,
                input_typing: InputTyping::CoerceToSpec,
                lifecycle: Lifecycle::TransientPerInvocation,
                egress: Egress::Backpressure,
            },
            // Long-lived and cancelled rather than completed. Coerces its own
            // input before routing, so the driver must not cast a second time.
            StreamingLoop::RunLoop => DriverPolicy {
                idle_tick: IdleTick::WallClock,
                end_of_stream: EndOfStream::NoFlush,
                input_typing: InputTyping::PreCoerced,
                lifecycle: Lifecycle::LongLived,
                egress: Egress::CappedDropOldest,
            },
        }
    }

    /// Position in [`StreamingLoop::ALL`].
    ///
    /// A second exhaustive match, which is the point: it means a new variant
    /// fails to compile in two places rather than silently missing from `ALL`.
    #[must_use]
    pub const fn ordinal(self) -> usize {
        match self {
            StreamingLoop::EmbeddedBounded => 0,
            StreamingLoop::EmbeddedContinuous => 1,
            StreamingLoop::RuntimeSeam => 2,
            StreamingLoop::Cycle => 3,
            StreamingLoop::RunLoop => 4,
        }
    }

    /// Human-readable name, used in warnings and assertion messages.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            StreamingLoop::EmbeddedBounded => "embedded-bounded",
            StreamingLoop::EmbeddedContinuous => "embedded-continuous",
            StreamingLoop::RuntimeSeam => "runtime-seam",
            StreamingLoop::Cycle => "cycle",
            StreamingLoop::RunLoop => "run-loop",
        }
    }
}

/// Compile-time proof that every declared loop's policy is internally coherent
/// and that [`StreamingLoop::ALL`] is complete and in order.
///
/// This is the mechanism, so it is worth stating exactly what it catches —
/// both halves were run against rustc rather than assumed:
///
/// - Flip [`StreamingLoop::Cycle`]'s `idle_tick` to [`IdleTick::WallClock`]
///   while its lifecycle stays [`Lifecycle::TransientPerInvocation`], and the
///   build stops with `error[E0080]: evaluation panicked: a StreamingLoop
///   declares a policy that cannot describe a real loop`.
/// - Add a sixth variant and the build stops with three separate
///   `error[E0004]: non-exhaustive patterns` — one each from
///   [`StreamingLoop::policy`], [`StreamingLoop::ordinal`] and
///   [`StreamingLoop::name`].
///
/// What it does **not** catch is documented on [`StreamingLoop::VARIANT_COUNT`].
const _: () = const_coherence();

// `ALL[i]` cannot use `.get()` — `Option::expect` is not available in a const
// context — and the index is bounded by the `i < ALL.len()` loop guard directly
// above it. The lint's hazard does not apply here in the way it usually does:
// this function is only ever evaluated at compile time, so an out-of-bounds
// index would fail the build with E0080 rather than panic in a running process.
#[allow(
    clippy::indexing_slicing,
    reason = "const-evaluated, index bounded by the loop guard; OOB fails the build"
)]
const fn const_coherence() {
    assert!(
        StreamingLoop::ALL.len() == StreamingLoop::VARIANT_COUNT,
        "StreamingLoop::ALL does not list every variant — a loop was added to the enum \
         without being added to ALL, so nothing checks its policy"
    );
    let mut i = 0;
    while i < StreamingLoop::ALL.len() {
        assert!(
            StreamingLoop::ALL[i].ordinal() == i,
            "StreamingLoop::ALL is out of ordinal order"
        );
        assert!(
            StreamingLoop::ALL[i].policy().incoherence().is_none(),
            "a StreamingLoop declares a policy that cannot describe a real loop; \
             see DriverPolicy::incoherence for which rule it trips"
        );
        i += 1;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The operator seam
// ─────────────────────────────────────────────────────────────────────────────

/// The operator surface a driver needs.
///
/// A trait rather than a concrete type so the driver's own tests can drive a
/// stub and assert *which* operator calls a policy produces — the property that
/// distinguishes a real gate from dead code a loop happens to bypass.
pub trait WindowStep {
    /// Feed input and return whatever closed.
    ///
    /// # Errors
    /// Propagates operator failures such as a missing or mistyped column.
    fn step(&mut self, batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>>;

    /// Advance the watermark to a wall-clock instant and return what closed.
    ///
    /// # Errors
    /// Propagates operator failures.
    fn tick(&mut self, wall_clock_ms: i64) -> ExecResult<Vec<RecordBatch>>;

    /// Close every open window because the stream is over.
    ///
    /// # Errors
    /// Propagates operator failures.
    fn flush(&mut self) -> ExecResult<Vec<RecordBatch>>;

    /// Is there state that a flush would emit?
    fn has_open_windows(&self) -> bool;

    /// The window spec this operator validates input against.
    ///
    /// On the trait rather than the driver on purpose: it is the operator's own
    /// spec, so [`InputTyping::CoerceToSpec`] cannot coerce toward something the
    /// operator will then reject.
    fn spec(&self) -> &WindowExecutionSpec;
}

impl WindowStep for ContinuousWindowExecutor {
    fn step(&mut self, batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
        self.drain(batches)
    }

    fn tick(&mut self, wall_clock_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        ContinuousWindowExecutor::tick(self, wall_clock_ms)
    }

    fn flush(&mut self) -> ExecResult<Vec<RecordBatch>> {
        self.flush_all()
    }

    fn has_open_windows(&self) -> bool {
        ContinuousWindowExecutor::has_open_windows(self)
    }

    fn spec(&self) -> &WindowExecutionSpec {
        ContinuousWindowExecutor::spec(self)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Input typing
// ─────────────────────────────────────────────────────────────────────────────

/// Arrow type for a `key_column_type` tag, or `None` for an unknown tag.
fn arrow_type_for_key_tag(tag: &str) -> Option<DataType> {
    match tag.trim().to_ascii_lowercase().as_str() {
        "int32" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),
        "float64" => Some(DataType::Float64),
        "bool" | "boolean" => Some(DataType::Boolean),
        "utf8" | "string" => Some(DataType::Utf8),
        _ => None,
    }
}

/// True for Arrow types the windowed aggregate operators accept directly as a
/// numeric aggregate input (no coercion needed).
fn is_numeric_agg_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Cast a source batch's key, event-time and aggregate-input columns to the
/// types the window operators require.
///
/// A JSON or CSV source with no declared column types delivers everything as
/// `Utf8`. The operators require an `Int64` event-time column, a
/// `key_column_type`-typed key and numeric aggregate inputs, so a raw all-`Utf8`
/// batch fails their type checks and its watermark never advances.
///
/// Only incompatible columns are cast, so an already-typed batch — anything fed
/// through the coordinator push path, for instance — returns a clone with no
/// per-column work. Casts are strict (`safe = false`): a value that cannot be
/// parsed fails loudly rather than nulling silently and dropping the row.
///
/// # `Timestamp` is cast, and used not to be
///
/// This function previously left a `Timestamp(_, _)` event-time column alone, on
/// the reasoning that a timestamp is already a time. But
/// [`crate::watermark_util::max_event_time_ms`] and the tumbling operator both
/// reach for the column with `downcast_ref::<Int64Array>()` and fail with
/// `"<col> must be Int64"` on anything else — so "leave it alone" meant "let it
/// fail one layer down". Timestamps are now cast to Int64 epoch milliseconds.
///
/// # Errors
///
/// Returns [`ExecError::UnsupportedType`] when a strict cast fails, naming the
/// column and both types.
pub fn coerce_batch_for_window(
    batch: &RecordBatch,
    spec: &WindowExecutionSpec,
) -> ExecResult<RecordBatch> {
    use arrow::array::ArrayRef;
    use arrow::compute::{CastOptions, cast_with_options};
    use arrow::datatypes::{Field, Schema, TimeUnit};

    let schema = batch.schema();
    let key_target = arrow_type_for_key_tag(&spec.key_column_type);
    let cast_opts = CastOptions {
        safe: false,
        ..Default::default()
    };

    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut changed = false;

    for (idx, field) in schema.fields().iter().enumerate() {
        let column = batch.column(idx);
        let name = field.name();
        let current = field.data_type();

        let desired: Option<DataType> = if name == &spec.event_time_column {
            match current {
                DataType::Int64 => None,
                // Millisecond timestamps are Int64 millis already, so the cast
                // is a reinterpret. Other units go through Millisecond first so
                // the result is epoch MILLIS, which is the unit every window
                // operator and the watermark comparison assume.
                DataType::Timestamp(TimeUnit::Millisecond, _) => Some(DataType::Int64),
                DataType::Timestamp(_, tz) => {
                    Some(DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))
                }
                _ => Some(DataType::Int64),
            }
        } else if name == &spec.key_column {
            key_target
                .as_ref()
                .filter(|target| *target != current)
                .cloned()
        } else if spec
            .agg_exprs
            .iter()
            .any(|agg| !agg.input_column.is_empty() && &agg.input_column == name)
        {
            if is_numeric_agg_type(current) {
                None
            } else {
                Some(DataType::Float64)
            }
        } else {
            None
        };

        match desired {
            Some(target) => {
                let casted = cast_with_options(column, &target, &cast_opts).map_err(|error| {
                    ExecError::UnsupportedType(format!(
                        "failed to coerce column '{name}' from {current:?} to {target:?} \
                             for the window operator: {error}"
                    ))
                })?;
                // A non-millisecond timestamp needs a second hop to Int64; the
                // first hop only normalised its unit.
                let (casted, target) = if matches!(target, DataType::Timestamp(_, _)) {
                    let millis = cast_with_options(&casted, &DataType::Int64, &cast_opts).map_err(
                        |error| {
                            ExecError::UnsupportedType(format!(
                                "failed to coerce column '{name}' from {target:?} to Int64 \
                                 epoch millis for the window operator: {error}"
                            ))
                        },
                    )?;
                    (millis, DataType::Int64)
                } else {
                    (casted, target)
                };
                new_fields.push(Field::new(name, target, field.is_nullable()));
                new_columns.push(casted);
                changed = true;
            }
            None => {
                new_fields.push(field.as_ref().clone());
                new_columns.push(Arc::clone(column));
            }
        }
    }

    if !changed {
        return Ok(batch.clone());
    }

    RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
        .map_err(|error| ExecError::Arrow(format!("rebuilt coerced batch is invalid: {error}")))
}

// ─────────────────────────────────────────────────────────────────────────────
// Stopping
// ─────────────────────────────────────────────────────────────────────────────

/// Why a loop is stopping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The source is over. A bounded run ends this way.
    SourceExhausted,
    /// Someone cancelled the job. Says nothing about whether the source is over.
    Cancelled,
    /// The control plane told this loop the stream has ended — the only way a
    /// loop that cannot observe its own source learns that.
    CoordinatorDirective,
}

/// What stopping produced.
///
/// `#[must_use]` because the failure this type exists to prevent is a caller
/// that stops, is handed closed windows, and drops them.
#[derive(Debug, Clone, PartialEq)]
#[must_use = "a stop that flushed produced windows that must be written somewhere"]
pub enum StopOutcome {
    /// Windows were closed and must be emitted.
    Flushed(Vec<RecordBatch>),
    /// Nothing was flushed.
    NotFlushed {
        /// Whether state remained that a flush would have emitted. `true` here
        /// means output was genuinely left behind.
        open_windows: bool,
        /// Why, in a form fit for a log line.
        because: &'static str,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// The driver
// ─────────────────────────────────────────────────────────────────────────────

/// The per-loop policy kernel.
///
/// Holds no operator. Every method borrows one for the duration of the call, so
/// adopting the driver never changes who owns the executor — which is what keeps
/// each conversion a local edit instead of a cross-crate refactor.
#[derive(Debug)]
pub struct StreamDriver {
    loop_id: StreamingLoop,
    idle_period: Duration,
    last_tick: Instant,
}

impl StreamDriver {
    /// Build a driver for a named loop.
    ///
    /// The `loop_id` argument is the gate: there is no way to construct a driver
    /// without naming which loop it belongs to, and naming one requires that
    /// loop to already have answered every axis.
    ///
    /// The driver holds no spec: [`InputTyping::CoerceToSpec`] reads it from the
    /// operator through [`WindowStep::spec`], so the driver cannot possibly
    /// coerce toward a different spec than the operator validates against.
    #[must_use]
    pub fn new(loop_id: StreamingLoop) -> Self {
        Self {
            loop_id,
            idle_period: krishiv_common::streaming_dials::idle_tick_interval(),
            last_tick: Instant::now(),
        }
    }

    /// Which loop this driver belongs to.
    #[must_use]
    pub fn loop_id(&self) -> StreamingLoop {
        self.loop_id
    }

    /// This loop's answers.
    #[must_use]
    pub fn policy(&self) -> DriverPolicy {
        self.loop_id.policy()
    }

    /// Feed input to the operator.
    ///
    /// Deliberately does **not** reset the idle clock. That looks wrong at first
    /// glance — input means the source is not idle — but it is what
    /// `run_streaming_continuous` does today, and this step is a refactor that
    /// must move zero semantics. Resetting here would make the loop wait a full
    /// interval after a busy period before its first idle tick, where today it
    /// ticks at the first quiet moment.
    ///
    /// The behaviour is harmless either way (a tick advances the watermark to
    /// wall clock, and a loop that just processed events already has a watermark
    /// near wall clock), which is exactly why it must not be changed silently
    /// as a side effect of factoring the gate out.
    ///
    /// # Errors
    /// Propagates operator failures.
    pub fn on_input<W: WindowStep + ?Sized>(
        &mut self,
        exec: &mut W,
        batches: Vec<RecordBatch>,
    ) -> ExecResult<Vec<RecordBatch>> {
        let batches = match self.policy().input_typing {
            InputTyping::CoerceToSpec => {
                // Scoped so the immutable borrow of `exec` ends before `step`
                // takes it mutably — which is also why the spec is read rather
                // than cloned: no per-batch clone on the hot path.
                let spec = exec.spec();
                batches
                    .iter()
                    .map(|batch| coerce_batch_for_window(batch, spec))
                    .collect::<ExecResult<Vec<_>>>()?
            }
            // The run-loop coerces before routing, because routing reads the key
            // column and must read it at its final type. Casting again here
            // would be a second pass over every batch on the hot path for no
            // change in result.
            InputTyping::PreCoerced => batches,
        };
        exec.step(batches)
    }

    /// Offer the operator a wall-clock tick because the source is quiet.
    ///
    /// Returns an empty vec without touching the operator when this loop's
    /// policy is [`IdleTick::None`], or when the interval has not elapsed. The
    /// interval gate lives here rather than at each call site because that is
    /// the duplication: two loops implemented it, three did not, and nothing
    /// recorded which was intended.
    ///
    /// # Errors
    /// Propagates operator failures.
    pub fn on_idle<W: WindowStep + ?Sized>(
        &mut self,
        exec: &mut W,
        now_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        if self.policy().idle_tick == IdleTick::None {
            return Ok(Vec::new());
        }
        if self.last_tick.elapsed() < self.idle_period {
            return Ok(Vec::new());
        }
        self.last_tick = Instant::now();
        exec.tick(now_ms)
    }

    /// Stop, flushing or not according to this loop's policy.
    ///
    /// The [`StopReason`] matters as much as the policy: a loop that flushes on
    /// exhaustion must NOT flush when merely cancelled, because a cancelled
    /// unbounded job's open windows are partial aggregates, and emitting them as
    /// though final would publish a wrong answer rather than lose a right one.
    ///
    /// # Errors
    /// Propagates operator failures.
    pub fn on_stop<W: WindowStep + ?Sized>(
        &mut self,
        exec: &mut W,
        reason: StopReason,
    ) -> ExecResult<StopOutcome> {
        let flush_now = match (self.policy().end_of_stream, reason) {
            (EndOfStream::FlushOnSourceExhausted, StopReason::SourceExhausted)
            | (EndOfStream::FlushOnDirective, StopReason::CoordinatorDirective) => true,
            (EndOfStream::FlushOnSourceExhausted | EndOfStream::FlushOnDirective, _) => false,
            (EndOfStream::NoFlush | EndOfStream::DelegatedToRuntime, _) => false,
        };
        if flush_now {
            return Ok(StopOutcome::Flushed(exec.flush()?));
        }
        Ok(StopOutcome::NotFlushed {
            open_windows: exec.has_open_windows(),
            because: match self.policy().end_of_stream {
                EndOfStream::NoFlush => {
                    "this loop does not flush on stop; its source is never exhausted"
                }
                EndOfStream::DelegatedToRuntime => {
                    "this loop holds no local operator; the runtime owns the flush"
                }
                EndOfStream::FlushOnSourceExhausted => {
                    "stopped without the source being exhausted, so open windows are partial"
                }
                EndOfStream::FlushOnDirective => {
                    "stopped without an end-of-stream directive from the control plane"
                }
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecError;

    /// A spec the driver can coerce toward. The stub operator ignores batches,
    /// so only the column names matter for the policy tests.
    fn test_spec() -> WindowExecutionSpec {
        WindowExecutionSpec::tumbling("key", "ts", 10_000)
    }

    /// A stub that records which operator calls the driver made.
    ///
    /// The point of driving a stub rather than a real operator: it distinguishes
    /// "the policy gate works" from "the operator happened to return nothing",
    /// which a real executor cannot.
    struct RecordingStep {
        steps: usize,
        ticks: usize,
        flushes: usize,
        open: bool,
        spec: WindowExecutionSpec,
    }

    impl Default for RecordingStep {
        fn default() -> Self {
            Self {
                steps: 0,
                ticks: 0,
                flushes: 0,
                open: false,
                spec: test_spec(),
            }
        }
    }

    impl WindowStep for RecordingStep {
        fn step(&mut self, _batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
            self.steps += 1;
            Ok(Vec::new())
        }
        fn tick(&mut self, _wall_clock_ms: i64) -> ExecResult<Vec<RecordBatch>> {
            self.ticks += 1;
            Ok(Vec::new())
        }
        fn flush(&mut self) -> ExecResult<Vec<RecordBatch>> {
            self.flushes += 1;
            Ok(Vec::new())
        }
        fn has_open_windows(&self) -> bool {
            self.open
        }
        fn spec(&self) -> &WindowExecutionSpec {
            &self.spec
        }
    }

    /// A loop whose policy says "no wall clock" must never reach `tick`.
    ///
    /// Asserts the operator was not called, not merely that the result was
    /// empty — an empty result is what a *called* operator with nothing to close
    /// also returns, so asserting on it would pass whether the gate worked or
    /// not.
    #[test]
    fn a_loop_without_a_wall_clock_never_ticks_the_operator() {
        let mut driver = StreamDriver::new(StreamingLoop::Cycle);
        let mut exec = RecordingStep::default();
        // Force the interval gate open so the ONLY thing that can stop the tick
        // is the policy.
        driver.last_tick = Instant::now() - Duration::from_secs(3600);

        let out = driver.on_idle(&mut exec, 1_000).expect("on_idle");
        assert!(out.is_empty());
        assert_eq!(
            exec.ticks, 0,
            "Cycle's policy is IdleTick::None, so the operator must not be ticked at all"
        );
    }

    /// A loop whose policy says "wall clock" reaches `tick` once the interval
    /// has elapsed, and not before.
    #[test]
    fn a_wall_clock_loop_ticks_only_after_the_interval_elapses() {
        let mut driver = StreamDriver::new(StreamingLoop::RunLoop);
        let mut exec = RecordingStep::default();

        driver.on_idle(&mut exec, 1_000).expect("on_idle");
        assert_eq!(exec.ticks, 0, "no tick before the interval elapses");

        driver.last_tick = Instant::now() - Duration::from_secs(3600);
        driver.on_idle(&mut exec, 2_000).expect("on_idle");
        assert_eq!(exec.ticks, 1, "tick once the interval has elapsed");
    }

    /// Input does NOT defer the next idle tick.
    ///
    /// Pins the behaviour of `run_streaming_continuous`, which tracks its idle
    /// interval independently of input and therefore ticks at the first quiet
    /// moment after a busy period. Making `on_input` reset the clock would be a
    /// defensible design change — and is exactly the kind of change that must
    /// not ride along inside a refactor. If it is ever made deliberately, this
    /// test is the one that has to be edited, which is the point of having it.
    #[test]
    fn input_does_not_defer_the_next_idle_tick() {
        let mut driver = StreamDriver::new(StreamingLoop::RunLoop);
        let mut exec = RecordingStep::default();
        driver.last_tick = Instant::now() - Duration::from_secs(3600);

        driver.on_input(&mut exec, Vec::new()).expect("on_input");
        driver.on_idle(&mut exec, 1_000).expect("on_idle");

        assert_eq!(exec.steps, 1);
        assert_eq!(
            exec.ticks, 1,
            "the idle interval is tracked independently of input, so a source that \
             goes quiet after a busy period ticks at the first opportunity"
        );
    }

    /// The bounded loop flushes when its source is exhausted — and only then.
    ///
    /// This is the pair the whole effort turns on. Reverting the
    /// `FlushOnSourceExhausted`/`SourceExhausted` arm of `on_stop` to return
    /// `false` makes the first half fail here and takes the embedded arm of the
    /// cross-loop corpus down with it.
    #[test]
    fn a_bounded_loop_flushes_on_exhaustion_and_not_on_cancellation() {
        let mut driver = StreamDriver::new(StreamingLoop::EmbeddedBounded);
        let mut exec = RecordingStep {
            open: true,
            ..RecordingStep::default()
        };

        let outcome = driver
            .on_stop(&mut exec, StopReason::SourceExhausted)
            .expect("on_stop");
        assert!(
            matches!(outcome, StopOutcome::Flushed(_)),
            "a bounded source that is over must close its trailing windows"
        );
        assert_eq!(exec.flushes, 1);

        let outcome = driver
            .on_stop(&mut exec, StopReason::Cancelled)
            .expect("on_stop");
        assert!(
            matches!(
                outcome,
                StopOutcome::NotFlushed {
                    open_windows: true,
                    ..
                }
            ),
            "cancellation is not exhaustion; open windows are partial and must not be \
             published as final"
        );
        assert_eq!(exec.flushes, 1, "cancellation must not have flushed");
    }

    /// A long-lived loop never flushes on stop, whatever the reason.
    #[test]
    fn a_long_lived_loop_reports_what_it_left_behind_instead_of_flushing() {
        let mut driver = StreamDriver::new(StreamingLoop::RunLoop);
        let mut exec = RecordingStep {
            open: true,
            ..RecordingStep::default()
        };

        for reason in [
            StopReason::SourceExhausted,
            StopReason::Cancelled,
            StopReason::CoordinatorDirective,
        ] {
            let outcome = driver.on_stop(&mut exec, reason).expect("on_stop");
            assert!(
                matches!(
                    outcome,
                    StopOutcome::NotFlushed {
                        open_windows: true,
                        ..
                    }
                ),
                "the run-loop must not force-flush an unbounded job ({reason:?})"
            );
        }
        assert_eq!(exec.flushes, 0);
    }

    /// The cycle flushes only when told to, because it cannot see its own source.
    #[test]
    fn the_cycle_flushes_on_a_directive_and_not_on_its_own() {
        let mut driver = StreamDriver::new(StreamingLoop::Cycle);
        let mut exec = RecordingStep::default();

        let outcome = driver
            .on_stop(&mut exec, StopReason::SourceExhausted)
            .expect("on_stop");
        assert!(
            matches!(outcome, StopOutcome::NotFlushed { .. }),
            "a cycle invocation cannot observe source exhaustion"
        );

        let outcome = driver
            .on_stop(&mut exec, StopReason::CoordinatorDirective)
            .expect("on_stop");
        assert!(
            matches!(outcome, StopOutcome::Flushed(_)),
            "a cycle told the stream has ended must close its windows"
        );
        assert_eq!(exec.flushes, 1);
    }

    /// Every declared loop is coherent and reachable.
    ///
    /// The const assertion already proves this at build time; this restates it
    /// as a runtime test so the failure is readable when someone is iterating on
    /// a policy, rather than only an `E0080` at the const site.
    #[test]
    fn every_declared_loop_has_a_coherent_policy() {
        assert_eq!(StreamingLoop::ALL.len(), StreamingLoop::VARIANT_COUNT);
        for (idx, loop_id) in StreamingLoop::ALL.iter().enumerate() {
            assert_eq!(loop_id.ordinal(), idx, "{} is misplaced", loop_id.name());
            assert!(
                loop_id.policy().incoherence().is_none(),
                "{} has an incoherent policy: {:?}",
                loop_id.name(),
                loop_id.policy().incoherence()
            );
        }
    }

    /// The coherence rules reject the combinations they claim to reject.
    ///
    /// Without this, `incoherence` could return `None` unconditionally and every
    /// check built on it — including the const assertion — would pass while
    /// enforcing nothing. That is precisely the "guard enforced by nothing"
    /// shape this codebase keeps finding.
    #[test]
    fn the_coherence_rules_actually_reject_something() {
        let transient_with_clock = DriverPolicy {
            idle_tick: IdleTick::WallClock,
            end_of_stream: EndOfStream::FlushOnDirective,
            input_typing: InputTyping::CoerceToSpec,
            lifecycle: Lifecycle::TransientPerInvocation,
            egress: Egress::Backpressure,
        };
        assert!(
            transient_with_clock.incoherence().is_some(),
            "a transient loop owning a wall clock must be rejected"
        );

        let long_lived_exhaustion = DriverPolicy {
            idle_tick: IdleTick::WallClock,
            end_of_stream: EndOfStream::FlushOnSourceExhausted,
            input_typing: InputTyping::PreCoerced,
            lifecycle: Lifecycle::LongLived,
            egress: Egress::CappedDropOldest,
        };
        assert!(
            long_lived_exhaustion.incoherence().is_some(),
            "a long-lived loop flushing on exhaustion must be rejected"
        );
    }

    fn sum_spec() -> WindowExecutionSpec {
        use krishiv_plan::window::{WindowAgg, WindowAggKind};
        let mut spec = WindowExecutionSpec::tumbling("k", "ts", 60_000);
        spec.agg_exprs = vec![WindowAgg {
            kind: WindowAggKind::Sum,
            input_column: "v".to_owned(),
            output_column: "total".to_owned(),
            filter: None,
        }];
        spec
    }

    /// An all-`Utf8` batch — the shape a Kafka JSON source produces when
    /// `decode.columns` is unset — is cast to the types the operators require.
    ///
    /// Reproduces the Kafka case with no broker: `payloads_to_batch` builds
    /// every field as `Utf8`, and both the watermark helper and the tumbling
    /// operator reach for the event-time column with
    /// `downcast_ref::<Int64Array>()`.
    #[test]
    fn an_all_utf8_batch_is_coerced_to_the_types_the_operators_require() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Utf8, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["1000"])),
                Arc::new(StringArray::from(vec!["7"])),
            ],
        )
        .expect("utf8 batch");

        let coerced = coerce_batch_for_window(&batch, &sum_spec()).expect("coerce");
        assert_eq!(coerced.schema().field(1).data_type(), &DataType::Int64);
        assert_eq!(coerced.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(
            coerced.schema().field(0).data_type(),
            &DataType::Utf8,
            "the key column's declared type is utf8, so it must be left alone"
        );
    }

    /// A `Timestamp` event-time column is cast to Int64 epoch millis.
    ///
    /// This is the hole found while moving the helper. It used to return `None`
    /// for `Timestamp(_, _)` — "a timestamp is already a time" — but
    /// `watermark_util::max_event_time_ms` and the tumbling operator both do
    /// `downcast_ref::<Int64Array>()` and fail with `"ts must be Int64"`. So
    /// declining to cast meant failing one layer down instead.
    ///
    /// Revert the `Timestamp` match arms to `None` and this test goes red with
    /// exactly that message coming from the operator.
    #[test]
    fn a_timestamp_event_time_column_becomes_int64_epoch_millis() {
        use arrow::array::{TimestampMillisecondArray, TimestampSecondArray};
        use arrow::datatypes::{Field, Schema, TimeUnit};

        // Millisecond: a straight reinterpret.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(vec![1_500_i64]))],
        )
        .expect("ts batch");
        let coerced = coerce_batch_for_window(&batch, &sum_spec()).expect("coerce millis");
        assert_eq!(coerced.schema().field(0).data_type(), &DataType::Int64);
        let col = coerced
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Int64 after coercion");
        assert_eq!(col.value(0), 1_500);

        // Second: must be rescaled to millis, not merely reinterpreted. A
        // reinterpret would yield 2, three orders of magnitude wrong, and every
        // window boundary with it.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampSecondArray::from(vec![2_i64]))],
        )
        .expect("ts batch");
        let coerced = coerce_batch_for_window(&batch, &sum_spec()).expect("coerce seconds");
        let col = coerced
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Int64 after coercion");
        assert_eq!(
            col.value(0),
            2_000,
            "seconds must be rescaled to epoch millis, which is the unit every \
             window operator and the watermark comparison assume"
        );
    }

    /// An already-typed batch comes back untouched.
    ///
    /// The `changed` short-circuit is what keeps `CoerceToSpec` free for the
    /// coordinator push path, which already delivers typed batches. Without it
    /// this step would put a per-column pass on every batch of every loop.
    #[test]
    fn an_already_typed_batch_is_returned_unchanged() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1_000_i64])),
                Arc::new(Int64Array::from(vec![7_i64])),
            ],
        )
        .expect("typed batch");

        let coerced = coerce_batch_for_window(&batch, &sum_spec()).expect("coerce");
        assert_eq!(coerced.schema(), schema, "schema must be identical");
        assert_eq!(coerced.num_rows(), 1);
    }

    /// A strict cast failure is loud, and names the column.
    ///
    /// `safe = false` is deliberate: a value that cannot be parsed must fail
    /// rather than null silently, because a nulled event-time column drops the
    /// row from its window and produces a quietly short answer.
    #[test]
    fn an_unparseable_value_fails_loudly_rather_than_nulling() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["not-a-timestamp"]))],
        )
        .expect("bad batch");

        let err = coerce_batch_for_window(&batch, &sum_spec()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("'ts'"),
            "the error must name the offending column: {msg}"
        );
    }

    /// The run-loop's `PreCoerced` policy skips the cast entirely.
    ///
    /// Asserts the batch arrives at the operator with its ORIGINAL types, which
    /// is the only way to tell "skipped" from "cast to something identical".
    #[test]
    fn a_precoerced_loop_hands_the_operator_its_input_untouched() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        struct Capturing {
            seen: Option<RecordBatch>,
            spec: WindowExecutionSpec,
        }
        impl WindowStep for Capturing {
            fn step(&mut self, batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
                self.seen = batches.into_iter().next();
                Ok(Vec::new())
            }
            fn tick(&mut self, _w: i64) -> ExecResult<Vec<RecordBatch>> {
                Ok(Vec::new())
            }
            fn flush(&mut self) -> ExecResult<Vec<RecordBatch>> {
                Ok(Vec::new())
            }
            fn has_open_windows(&self) -> bool {
                false
            }
            fn spec(&self) -> &WindowExecutionSpec {
                &self.spec
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["1000"]))])
            .expect("batch");

        let mut exec = Capturing {
            seen: None,
            spec: sum_spec(),
        };
        let mut driver = StreamDriver::new(StreamingLoop::RunLoop);
        assert_eq!(driver.policy().input_typing, InputTyping::PreCoerced);
        driver
            .on_input(&mut exec, vec![batch])
            .expect("run-loop on_input");
        assert_eq!(
            exec.seen
                .expect("operator saw a batch")
                .schema()
                .field(0)
                .data_type(),
            &DataType::Utf8,
            "PreCoerced must not cast; the run-loop already did it before routing"
        );
    }

    /// `ExecError` is reachable from here — pins the error type the driver
    /// propagates so a change to it surfaces as a compile failure in this module
    /// rather than at every call site.
    #[test]
    fn driver_errors_are_operator_errors() {
        struct Failing {
            spec: WindowExecutionSpec,
        }
        impl WindowStep for Failing {
            fn step(&mut self, _b: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
                Err(ExecError::InvalidInput("boom".into()))
            }
            fn tick(&mut self, _w: i64) -> ExecResult<Vec<RecordBatch>> {
                Ok(Vec::new())
            }
            fn flush(&mut self) -> ExecResult<Vec<RecordBatch>> {
                Ok(Vec::new())
            }
            fn has_open_windows(&self) -> bool {
                false
            }
            fn spec(&self) -> &WindowExecutionSpec {
                &self.spec
            }
        }
        let mut driver = StreamDriver::new(StreamingLoop::EmbeddedBounded);
        let err = driver
            .on_input(&mut Failing { spec: test_spec() }, Vec::new())
            .unwrap_err();
        assert!(matches!(err, ExecError::InvalidInput(_)));
    }
}
