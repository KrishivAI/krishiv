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

use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Where [`next_change`](Self::next_change) has read up to: the tick of
    /// the last delta it handed out, and the deltas already fetched after it.
    feed: Mutex<ChangeCursor>,
}

/// The change-feed position of one Python handle.
#[derive(Default)]
struct ChangeCursor {
    /// Tick of the last delta handed out (or resumed from).
    after: u64,
    /// Fetched, not yet handed out, oldest first.
    buffered: std::collections::VecDeque<(u64, DeltaBatch)>,
}

impl PyIncrementalDataFrame {
    pub(crate) fn new(inner: IncrementalDataFrame) -> Self {
        Self {
            inner,
            txn: Mutex::new(None),
            feed: Mutex::new(ChangeCursor::default()),
        }
    }

    /// No Python code runs while either lock is held, so a poisoned lock is
    /// unreachable; recover the guard rather than propagate a panic.
    fn txn_guard(&self) -> MutexGuard<'_, Option<Txn>> {
        self.txn.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn feed_guard(&self) -> MutexGuard<'_, ChangeCursor> {
        self.feed.lock().unwrap_or_else(|e| e.into_inner())
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
    /// This is the "update" output mode, and it is lossless: every delta the
    /// view publishes is returned exactly once, in order, however many ticks
    /// ran between calls, and a tick that published nothing returns ``None``.
    /// It works for an embedded and a distributed job alike.
    ///
    /// The engine retains a bounded amount of output per view. If this handle
    /// falls further behind than that — or the job was restored underneath
    /// it — the deltas in between no longer exist, and this raises
    /// ``RuntimeError`` once rather than hand over a changelog with a hole in
    /// it. Re-read :meth:`snapshot` and carry on; later calls continue from
    /// the point the feed is whole again.
    fn next_change(&self, py: Python<'_>) -> PyResult<Option<PyDeltaBatch>> {
        let after = {
            let mut feed = self.feed_guard();
            if let Some((tick, delta)) = feed.buffered.pop_front() {
                feed.after = tick;
                return Ok(Some(PyDeltaBatch { inner: delta }));
            }
            feed.after
        };
        let since = py
            .detach(|| crate::RUNTIME.block_on(self.inner.changes_since(after)))
            .map_err(rt_err)?;
        let mut feed = self.feed_guard();
        if since.missed {
            // Skip to where the feed is whole; what is retained from there on
            // is still good and is handed out by the following calls.
            feed.after = since.resume_after;
            feed.buffered = since
                .deltas
                .into_iter()
                .filter(|(tick, _)| *tick > since.resume_after)
                .collect();
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "change feed for view '{}' has a gap: output published after tick {after} is no \
                 longer retained (the reader fell behind the retention bound, or the job was \
                 restored). Re-read snapshot() to resynchronise; next_change() continues from \
                 tick {}.",
                self.inner.name(),
                since.resume_after
            )));
        }
        feed.buffered.extend(since.deltas);
        Ok(feed.buffered.pop_front().map(|(tick, delta)| {
            feed.after = tick;
            PyDeltaBatch { inner: delta }
        }))
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
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn one_column(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
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
