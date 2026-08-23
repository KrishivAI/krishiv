//! pyo3 bindings for [`IncrementalDataFrame`] — the delta/IVM mode of the unified
//! DataFrame surface. Built by `DataFrame.to_incremental(name)`.
//!
//! The Rust surface here is the core (feed / step / read / transaction / change
//! cursor); only the thin Z-set conveniences that need no state — `insert`,
//! `delete`, `update`, `apply_cdc` and the `transaction()` context-manager
//! object — are grafted on in pure Python (`_pyspark.py`).
//!
//! Everything that owns state lives here, because state is where the bugs were:
//! `transaction()` buffers its feeds in this struct so an aborted block feeds the
//! engine nothing, and the change cursor lives here so a delta is never handed
//! out twice.

use std::sync::MutexGuard;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use krishiv_api::{IncrementalDataFrame, StepReport};
use krishiv_delta::DeltaBatch;

use crate::batch::PyBatch;
use crate::incremental::{PyDeltaBatch, PyStepSummary};

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Process-wide counter for auto-generated view names.
static VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_view_name() -> String {
    format!("ivm_view_{}", VIEW_SEQ.fetch_add(1, Ordering::Relaxed))
}

/// An open `transaction()` block: feeds land in `buffered` instead of the engine.
///
/// `marks` is the buffer length at each `__enter__`, so a nested block that
/// aborts discards exactly its own feeds and leaves the enclosing block's alone;
/// `marks.len()` is the nesting depth. `owner` pins the block to the thread that
/// opened it — a second thread feeding the same handle mid-block would either be
/// swallowed into someone else's "atomic" tick or race with its commit, so it is
/// rejected instead.
struct Txn {
    owner: ThreadId,
    marks: Vec<usize>,
    buffered: Vec<BufferedFeed>,
}

/// A feed held back by an open block: the source it was addressed to, and the
/// delta itself.
type BufferedFeed = (Option<String>, DeltaBatch);

impl Txn {
    /// Open the outermost block on `owner`'s thread.
    fn open(owner: ThreadId) -> Self {
        Self {
            owner,
            marks: vec![0],
            buffered: Vec::new(),
        }
    }

    /// Open a nested block, remembering how much is already buffered so that
    /// aborting it cannot reach past its own feeds.
    fn enter(&mut self) {
        self.marks.push(self.buffered.len());
    }

    /// Close the innermost block.
    ///
    /// `None` means an enclosing block is still open and owns the buffer;
    /// `Some(feeds)` means this was the outermost block and the whole buffer is
    /// now the caller's to feed. Aborting truncates back to this block's own
    /// mark, so an enclosing block's feeds survive an inner abort untouched.
    fn close(&mut self, commit: bool) -> Option<Vec<BufferedFeed>> {
        let mark = self.marks.pop().unwrap_or(0);
        if !commit {
            self.buffered.truncate(mark);
        }
        if self.marks.is_empty() {
            Some(std::mem::take(&mut self.buffered))
        } else {
            None
        }
    }
}

/// The delta/IVM mode of the unified DataFrame surface.
///
/// Feed `DeltaBatch` changes with :meth:`apply` (which advances one tick unless
/// it is inside a :meth:`transaction`), then read the full :meth:`snapshot` or
/// the per-tick output delta via :meth:`next_change` / :meth:`last_output`.
#[pyclass(name = "IncrementalDataFrame", module = "krishiv")]
pub struct PyIncrementalDataFrame {
    pub(crate) inner: IncrementalDataFrame,
    /// The open `transaction()` block, if any.
    txn: Mutex<Option<Txn>>,
    /// The delta most recently handed out by [`next_change`](Self::next_change),
    /// retained (not just fingerprinted) so the Arc addresses it is compared
    /// against cannot be freed and reused underneath the cursor.
    yielded: Mutex<Option<DeltaBatch>>,
}

impl PyIncrementalDataFrame {
    pub(crate) fn new(inner: IncrementalDataFrame) -> Self {
        Self {
            inner,
            txn: Mutex::new(None),
            yielded: Mutex::new(None),
        }
    }

    /// No Python code runs while either lock is held, so a poisoned lock is
    /// unreachable; recover the guard rather than propagate a panic.
    fn txn_guard(&self) -> MutexGuard<'_, Option<Txn>> {
        self.txn.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn yielded_guard(&self) -> MutexGuard<'_, Option<DeltaBatch>> {
        self.yielded.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Turn a completed tick into a `StepSummary`, failing loudly if *this*
    /// handle's view could not be evaluated.
    ///
    /// `errored_views` is the only per-view failure channel the engine has — a
    /// view whose SQL or operator apply fails is skipped, its snapshot silently
    /// left at the previous value, and the tick still reports success. For the
    /// view this handle is about to be asked for, that is an error: callers of
    /// `apply`/`insert` do not inspect a return value they never asked for, and
    /// "the snapshot stopped changing" is not a diagnosis.
    ///
    /// Other views in the same job are reported, not raised: each derived view
    /// (`Session.view` + `to_incremental`) has its own handle, and a failure in
    /// one must not surface as an exception from another's `apply`. They stay
    /// visible in `StepSummary.errored_views`.
    ///
    /// A derived view reporting "table not found" for its upstream is a real
    /// failure, not an expected lag: since IVM-AUD-CORE-17 a view reads its
    /// upstream's output in the same tick the upstream produces it.
    fn checked_step(&self, report: StepReport) -> PyResult<PyStepSummary> {
        let summary = PyStepSummary::from(report);
        let detail = summary
            .errored_views
            .iter()
            .filter(|e| e.view.eq_ignore_ascii_case(self.inner.name()))
            .map(|e| format!("[{}] {}", e.kind, e.message))
            .collect::<Vec<_>>()
            .join("; ");
        if detail.is_empty() {
            return Ok(summary);
        }
        Err(PyRuntimeError::new_err(format!(
            "incremental view '{}' failed to evaluate and was skipped, so its \
             snapshot did not change: {detail}",
            self.inner.name()
        )))
    }

    fn concurrent_err(&self) -> PyErr {
        PyRuntimeError::new_err(format!(
            "IncrementalDataFrame('{}'): a transaction() block is open on another \
             thread; feeds and transactions on one handle are single-threaded \
             (open the block and feed it from the same thread)",
            self.inner.name()
        ))
    }
}

/// Whether two deltas are the *same publication* — i.e. the second is a clone of
/// the watch value the first came from, not a newly emitted delta.
///
/// The engine's change-feed peek is a coalescing `watch`, so an unchanged value
/// is handed back verbatim: every column is an `Arc` clone of the same array.
/// Comparing addresses (while holding the previous delta alive, see
/// `PyIncrementalDataFrame::yielded`) therefore distinguishes "nothing new was
/// published" from "a new delta was published", which comparing *contents*
/// cannot.
fn same_publication(a: &DeltaBatch, b: &DeltaBatch) -> bool {
    let (a, b) = (a.inner(), b.inner());
    if a.num_columns() == 0 || a.num_columns() != b.num_columns() {
        return false;
    }
    a.columns()
        .iter()
        .zip(b.columns())
        .all(|(l, r)| std::ptr::addr_eq(Arc::as_ptr(l), Arc::as_ptr(r)))
}

#[pymethods]
impl PyIncrementalDataFrame {
    /// The view's identifier.
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// The source names this view reads (feedable via :meth:`apply`).
    #[getter]
    fn source_names(&self) -> Vec<String> {
        self.inner.source_names().to_vec()
    }

    /// An empty batch carrying the view's output schema — used by
    /// ``Session.view()`` to register this view as a client-side planning source
    /// for a downstream (view-DAG) query.
    fn schema_batch(&self) -> PyBatch {
        let empty = arrow::record_batch::RecordBatch::new_empty(self.inner.output_schema());
        PyBatch::from_record_batch(empty)
    }

    /// Feed a change to a source and advance one tick, returning the tick's
    /// :class:`StepSummary`.
    ///
    /// Inside a :meth:`transaction` block the delta is buffered instead — nothing
    /// reaches the engine and ``None`` is returned; the whole block is fed and
    /// ticked once when it exits cleanly.
    ///
    /// `source` may be omitted only when the view has exactly one source.
    /// Raises if *this* view failed to evaluate during the tick; other views in
    /// the same job are reported in :class:`StepSummary.errored_views`.
    #[pyo3(signature = (delta, source=None))]
    fn apply(
        &self,
        py: Python<'_>,
        delta: PyRef<'_, PyDeltaBatch>,
        source: Option<String>,
    ) -> PyResult<Option<PyStepSummary>> {
        let delta = delta.inner.clone();
        // Resolve ambiguity eagerly so a buffered feed reports it at the call
        // site rather than at commit, three lines later in the user's code.
        if source.is_none() && self.inner.source_names().len() != 1 {
            return Err(PyRuntimeError::new_err(format!(
                "view '{}' reads {} sources {:?}; pass source=<name>",
                self.inner.name(),
                self.inner.source_names().len(),
                self.inner.source_names()
            )));
        }
        {
            let mut guard = self.txn_guard();
            if let Some(txn) = guard.as_mut() {
                if txn.owner != std::thread::current().id() {
                    return Err(self.concurrent_err());
                }
                txn.buffered.push((source, delta));
                return Ok(None);
            }
        }
        let report = py
            .detach(move || {
                crate::RUNTIME.block_on(async {
                    self.inner.apply(source.as_deref(), &delta).await?;
                    self.inner.step().await
                })
            })
            .map_err(rt_err)?;
        self.checked_step(report).map(Some)
    }

    /// Advance one IVM tick, returning per-view output counts.
    ///
    /// Raises if *this* view failed to evaluate during the tick; other views in
    /// the same job are reported in :class:`StepSummary.errored_views`.
    fn step(&self, py: Python<'_>) -> PyResult<PyStepSummary> {
        let report = py
            .detach(|| crate::RUNTIME.block_on(self.inner.step()))
            .map_err(rt_err)?;
        self.checked_step(report)
    }

    /// The current full materialized snapshot of the view (`None` if the view
    /// has not produced output yet). "Complete" output mode.
    fn snapshot(&self, py: Python<'_>) -> PyResult<Option<PyBatch>> {
        py.detach(|| crate::RUNTIME.block_on(self.inner.snapshot()))
            .map(|opt| opt.map(PyBatch::from_record_batch))
            .map_err(rt_err)
    }

    /// The next output delta this handle has not returned yet, or ``None``.
    ///
    /// This is the "update" output mode. Its exact contract, because the engine
    /// only offers a *coalescing* peek here:
    ///
    /// - a delta is never returned twice, and a tick that published nothing
    ///   returns ``None`` rather than re-serving the previous delta;
    /// - it is **not** lossless: if several ticks publish output between two
    ///   calls, only the newest delta survives to be returned. The engine has a
    ///   lossless broadcast stream (`IncrementalFlow::view_output_stream`) but it
    ///   is not reachable through `IvmJob`/`IncrementalDataFrame` yet.
    ///
    /// Embedded jobs only; a distributed job returns ``None`` here.
    fn next_change(&self) -> PyResult<Option<PyDeltaBatch>> {
        let Some(delta) = self.inner.last_output().map_err(rt_err)? else {
            return Ok(None);
        };
        let mut yielded = self.yielded_guard();
        if yielded
            .as_ref()
            .is_some_and(|prev| same_publication(prev, &delta))
        {
            return Ok(None);
        }
        *yielded = Some(delta.clone());
        Ok(Some(PyDeltaBatch { inner: delta }))
    }

    /// Peek the view's latest published output delta (`None` if it has never
    /// published one).
    ///
    /// A peek at a coalescing watch, not a feed: repeated calls return the same
    /// delta, and after a tick that published nothing it still returns the
    /// *previous* tick's delta. Use :meth:`next_change` to consume each delta
    /// once. Embedded jobs only; a distributed job returns ``None`` here.
    fn last_output(&self) -> PyResult<Option<PyDeltaBatch>> {
        self.inner
            .last_output()
            .map(|opt| opt.map(|inner| PyDeltaBatch { inner }))
            .map_err(rt_err)
    }

    /// Internal: open a `transaction()` block on this thread (nestable).
    fn _txn_enter(&self) -> PyResult<()> {
        let me = std::thread::current().id();
        let mut guard = self.txn_guard();
        match guard.as_mut() {
            Some(txn) if txn.owner == me => txn.enter(),
            Some(_) => return Err(self.concurrent_err()),
            None => *guard = Some(Txn::open(me)),
        }
        Ok(())
    }

    /// Internal: close a `transaction()` block.
    ///
    /// `commit=False` discards exactly the feeds buffered by this block (a
    /// nested block leaves its parent's untouched) — the engine never saw them,
    /// so nothing is left behind for a later tick to apply. `commit=True` on the
    /// outermost block feeds them all and fires exactly one tick; if the engine
    /// rejects one of them, the feeds it already accepted are retracted before
    /// the error is raised, so no partial write survives.
    fn _txn_exit(&self, py: Python<'_>, commit: bool) -> PyResult<Option<PyStepSummary>> {
        let me = std::thread::current().id();
        let buffered = {
            let mut guard = self.txn_guard();
            let Some(txn) = guard.as_mut() else {
                return Err(PyRuntimeError::new_err(
                    "transaction() exited without a matching enter",
                ));
            };
            if txn.owner != me {
                return Err(self.concurrent_err());
            }
            let Some(feeds) = txn.close(commit) else {
                return Ok(None); // inner block: the outermost one commits
            };
            *guard = None;
            feeds
        };
        if !commit || buffered.is_empty() {
            // Nothing was fed, so nothing needs a tick. (An aborted block that
            // fed nothing must not advance the tick either.)
            return Ok(None);
        }
        let report = py
            .detach(move || {
                crate::RUNTIME.block_on(async move {
                    for (i, (source, delta)) in buffered.iter().enumerate() {
                        let Err(e) = self.inner.apply(source.as_deref(), delta).await else {
                            continue;
                        };
                        let mut problems = vec![format!(
                            "transaction commit rejected feed {}/{}: {e}",
                            i + 1,
                            buffered.len()
                        )];
                        // (`get(..i)` not `[..i]`: no indexing panics in this crate.)
                        problems.extend(self.retract(buffered.get(..i).unwrap_or_default()).await);
                        return Err(problems.join("; "));
                    }
                    self.inner.step().await.map_err(|e| e.to_string())
                })
            })
            .map_err(rt_err)?;
        self.checked_step(report).map(Some)
    }

    fn __repr__(&self) -> String {
        format!(
            "IncrementalDataFrame(view='{}', sources={:?})",
            self.inner.name(),
            self.inner.source_names()
        )
    }
}

impl PyIncrementalDataFrame {
    /// Undo already-accepted feeds by feeding their Z-set negation, newest
    /// first. Nothing has been stepped yet, so `+d` and `-d` consolidate to zero
    /// in `pending` and the failed commit leaves no trace. Returns a description
    /// of every retraction that itself failed, for the caller to report — a
    /// rollback that silently half-worked is the bug this whole path exists to
    /// avoid.
    async fn retract(&self, fed: &[(Option<String>, DeltaBatch)]) -> Vec<String> {
        let mut problems = Vec::new();
        for (source, delta) in fed.iter().rev() {
            match delta.negate() {
                Ok(negated) => {
                    if let Err(e) = self.inner.apply(source.as_deref(), &negated).await {
                        problems.push(format!("ROLLBACK INCOMPLETE (feed not retracted): {e}"));
                    }
                }
                Err(e) => problems.push(format!("ROLLBACK INCOMPLETE (cannot negate feed): {e}")),
            }
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn one_column(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
            .expect("valid batch")
    }

    fn two_columns(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(values.to_vec())),
                Arc::new(Int64Array::from(
                    values.iter().map(|v| i64::from(*v)).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("valid batch")
    }

    fn delta(values: &[i32]) -> DeltaBatch {
        DeltaBatch::from_inserts(one_column(values)).expect("from_inserts")
    }

    fn feed(source: &str, values: &[i32]) -> BufferedFeed {
        (Some(source.to_owned()), delta(values))
    }

    fn sources(feeds: &[BufferedFeed]) -> Vec<&str> {
        feeds
            .iter()
            .map(|(source, _)| source.as_deref().unwrap_or("<none>"))
            .collect()
    }

    // ── same_publication: the change cursor's "is this new?" test ────────────

    #[test]
    fn a_clone_of_a_publication_is_the_same_publication() {
        // What the coalescing watch actually hands back when nothing new was
        // published: the same value, so every column Arc is shared.
        let published = delta(&[1, 2, 3]);
        assert!(same_publication(&published, &published.clone()));
    }

    #[test]
    fn equal_contents_from_a_separate_publication_are_not_the_same_publication() {
        // The whole reason the cursor compares addresses: two ticks can publish
        // byte-identical deltas, and the second one is still new. Comparing
        // contents would swallow it.
        let first = delta(&[1, 2, 3]);
        let second = delta(&[1, 2, 3]);
        assert_eq!(first.inner().num_rows(), second.inner().num_rows());
        assert!(!same_publication(&first, &second));
    }

    #[test]
    fn a_different_column_count_is_not_the_same_publication() {
        let narrow = delta(&[1, 2]);
        let wide = DeltaBatch::from_inserts(two_columns(&[1, 2])).expect("from_inserts");
        assert_ne!(
            narrow.inner().num_columns(),
            wide.inner().num_columns(),
            "the fixtures must actually differ in width"
        );
        assert!(!same_publication(&narrow, &wide));
        assert!(!same_publication(&wide, &narrow));
    }

    // ── Txn: the nesting marks and the abort truncation ──────────────────────

    fn open() -> Txn {
        Txn::open(std::thread::current().id())
    }

    #[test]
    fn the_outermost_block_hands_back_everything_it_buffered() {
        let mut txn = open();
        txn.buffered.push(feed("orders", &[1]));
        txn.buffered.push(feed("returns", &[2]));
        let committed = txn
            .close(true)
            .expect("the outermost block owns the buffer");
        assert_eq!(sources(&committed), ["orders", "returns"]);
        assert!(
            txn.buffered.is_empty(),
            "the buffer is handed over, not copied"
        );
    }

    #[test]
    fn a_block_that_aborts_feeds_nothing() {
        let mut txn = open();
        txn.buffered.push(feed("orders", &[1]));
        let closed = txn.close(false).expect("the outermost block still closes");
        assert!(
            closed.is_empty(),
            "an aborted block must feed the engine nothing"
        );
    }

    #[test]
    fn an_inner_block_leaves_the_buffer_to_the_enclosing_one() {
        let mut txn = open();
        txn.buffered.push(feed("orders", &[1]));
        txn.enter();
        txn.buffered.push(feed("returns", &[2]));
        assert!(
            txn.close(true).is_none(),
            "an inner commit must not fire a tick; the outermost block does"
        );
        let committed = txn.close(true).expect("now the outermost block closes");
        assert_eq!(sources(&committed), ["orders", "returns"]);
    }

    #[test]
    fn an_inner_abort_discards_exactly_its_own_feeds() {
        // The mark is what makes this true: the inner block truncates back to
        // the length the buffer had when it opened, so the enclosing block's
        // feeds are untouched and still commit.
        let mut txn = open();
        txn.buffered.push(feed("outer_first", &[1]));
        txn.enter();
        txn.buffered.push(feed("inner_a", &[2]));
        txn.buffered.push(feed("inner_b", &[3]));
        assert!(
            txn.close(false).is_none(),
            "an enclosing block is still open"
        );
        assert_eq!(sources(&txn.buffered), ["outer_first"]);

        txn.buffered.push(feed("outer_second", &[4]));
        let committed = txn.close(true).expect("the outermost block closes");
        assert_eq!(sources(&committed), ["outer_first", "outer_second"]);
    }

    #[test]
    fn an_outer_abort_discards_a_committed_inner_blocks_feeds_too() {
        // An inner commit is not a write: nothing reached the engine, so the
        // enclosing abort still takes everything with it.
        let mut txn = open();
        txn.enter();
        txn.buffered.push(feed("inner", &[1]));
        assert!(txn.close(true).is_none());
        let closed = txn.close(false).expect("the outermost block closes");
        assert!(
            closed.is_empty(),
            "an outer abort discards the inner block's feeds"
        );
    }

    #[test]
    fn nesting_marks_track_the_depth() {
        let mut txn = open();
        assert_eq!(txn.marks.len(), 1);
        txn.enter();
        txn.enter();
        assert_eq!(txn.marks.len(), 3);
        assert!(txn.close(true).is_none());
        assert!(txn.close(true).is_none());
        assert_eq!(txn.marks.len(), 1);
        assert!(txn.close(true).is_some(), "the last close is the outermost");
    }
}
