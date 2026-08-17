//! Cross-loop conformance for windowed streaming.
//!
//! The claim under test: the same windowed job over the same events produces
//! the same closed windows whichever **driver loop** runs it, because a loop
//! chooses *when* the operator is stepped and nothing else.
//!
//! ## Why this file exists
//!
//! The operator core is shared — one `ContinuousWindowExecutor`, one set of
//! window operators — but the loops that drive it are not. There are four, each
//! owning its own source polling, watermark advance, drain cadence, checkpoint
//! trigger and teardown:
//!
//! | loop | crate | placement |
//! |---|---|---|
//! | `StreamingEngine::run` | `krishiv-engines` | embedded / single-node |
//! | `run_streaming_job_via_runtime` | `krishiv-api` | bounded distributed |
//! | `execute_streaming_fragment` | `krishiv-executor` | cycle (`stream:loop:`) |
//! | `execute_run_loop_fragment` | `krishiv-executor` | run-loop (`stream:rloop:`) |
//!
//! A fix applied to one is not a fix to the others, and nothing said so. That
//! is not hypothetical: commit `dd47d50` fixed a trailing-window drop on the
//! first loop, and `8756b41` had to fix **the identical bug** on the second
//! weeks later — a bounded job whose events all fell in one unclosed window
//! wrote zero rows and reported `Completed`. The two loops sit in different
//! crates, so no test, review, or grep connected them.
//!
//! ## The corpus moved out of this file
//!
//! The fixtures and expectations now live in
//! [`krishiv_dataflow::streaming_corpus`], because `krishiv-api` cannot see
//! `krishiv-executor` and the executor-side arms need the same expectations.
//! `krishiv-dataflow` is the deepest crate both depend on. The executor arms
//! live in `crates/krishiv-executor/tests/streaming_loop_conformance.rs`.
//!
//! ## What the runtime seam actually proves, and what it does not
//!
//! `run_streaming_job_via_runtime` is driven here through **two** runtimes, and
//! the difference between them is the point:
//!
//! - [`via_runtime_seam`] uses the session's in-process runtime, which
//!   implements `flush_continuous_stream`. This is the placement that works.
//! - [`via_runtime_seam_without_flush`] wraps that same runtime in a double
//!   that **omits** `flush_continuous_stream`, exactly mirroring the method set
//!   of `impl ExecutionRuntime for RemoteExecutionRuntime`
//!   (`crates/krishiv-runtime/src/execution_runtime.rs:648`), which overrides
//!   register/push/drain and inherits the trait's error default for flush.
//!
//! Before the double existed, this file drove only the first, and so certified
//! green precisely the one placement where the defect does not appear.
//! `ExecutionMode::Distributed` can only construct a `RemoteExecutionRuntime`
//! (`execution_runtime.rs:1062-1075`), so the second arm is the one that
//! describes production.
//!
//! ## Scope, stated honestly
//!
//! Still not covered here: a **coordinator-backed** host. `FlightSqlHost`
//! routes `Coordinator` drains to `krishiv_scheduler::drain_continuous_stream_coordinated`
//! and never touches `ContinuousStreamRegistry`, and its push path rejects an
//! in-process task endpoint outright, so that arm needs a live coordinator plus
//! a real gRPC executor. It is recorded as open rather than quietly skipped —
//! it is the only arm that observes the full production path end to end.

#[cfg(test)]
mod streaming_conformance_tests {
    use std::sync::Arc;

    use krishiv_dataflow::streaming_corpus::{CORPUS, CorpusEntry, WINDOWED_SQL, render_sorted};
    use krishiv_engine_core::{EngineKind, JobStatus};
    use krishiv_runtime::{
        BatchSqlStreamFuture, BatchTableRegistration, ExecutionPlacement, ExecutionRuntime,
        RuntimeMode,
    };

    use crate::connector_runtime::run_streaming_job_via_runtime;
    use crate::{CompiledJob, SinkSpec, SourceSpec};

    /// A runtime that can do everything except close its open windows.
    ///
    /// Delegates every **required** trait method to a real in-process runtime
    /// and deliberately does not implement `flush_continuous_stream`, so it
    /// inherits the trait default. That is not an approximation of
    /// `RemoteExecutionRuntime` — it is the same omission, and it is the whole
    /// mechanism by which a distributed bounded job loses its trailing window.
    ///
    /// Keeping this as a *double* rather than reaching for a live remote runtime
    /// is deliberate: the defect is a missing method, and a missing method is
    /// observable without a cluster. What a cluster would add is the coordinator
    /// path, which this file does not claim to cover.
    struct FlushlessRuntime {
        inner: Arc<dyn ExecutionRuntime>,
    }

    impl ExecutionRuntime for FlushlessRuntime {
        fn mode(&self) -> RuntimeMode {
            self.inner.mode()
        }

        fn placement(&self) -> ExecutionPlacement {
            self.inner.placement()
        }

        fn accept_plan(
            &self,
            plan: &krishiv_plan::PhysicalPlan,
        ) -> krishiv_runtime::RuntimeResult<krishiv_runtime::ExecutionReport> {
            self.inner.accept_plan(plan)
        }

        fn collect_bounded_window(
            &self,
            topic: &str,
            input_batches: Vec<arrow::record_batch::RecordBatch>,
            spec: &krishiv_runtime::local_streaming::LocalWindowExecutionSpec,
        ) -> krishiv_runtime::RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
            self.inner
                .collect_bounded_window(topic, input_batches, spec)
        }

        fn collect_batch_sql(
            &self,
            query: &str,
            tables: &[BatchTableRegistration],
            is_streaming: bool,
        ) -> krishiv_runtime::RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
            self.inner.collect_batch_sql(query, tables, is_streaming)
        }

        fn collect_batch_sql_async<'a>(
            &'a self,
            query: &'a str,
            tables: &'a [BatchTableRegistration],
            is_streaming: bool,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = krishiv_runtime::RuntimeResult<
                            Vec<arrow::record_batch::RecordBatch>,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            self.inner
                .collect_batch_sql_async(query, tables, is_streaming)
        }

        fn stream_batch_sql<'a>(
            &'a self,
            query: &'a str,
            tables: &'a [BatchTableRegistration],
            is_streaming: bool,
        ) -> BatchSqlStreamFuture<'a> {
            self.inner.stream_batch_sql(query, tables, is_streaming)
        }

        fn explain_sql(&self, query: &str) -> krishiv_runtime::RuntimeResult<String> {
            self.inner.explain_sql(query)
        }

        fn register_continuous_stream(
            &self,
            job_id: &str,
            spec: &krishiv_runtime::local_streaming::LocalWindowExecutionSpec,
        ) -> krishiv_runtime::RuntimeResult<()> {
            self.inner.register_continuous_stream(job_id, spec)
        }

        fn push_continuous_stream_input(
            &self,
            job_id: &str,
            batches: Vec<arrow::record_batch::RecordBatch>,
        ) -> krishiv_runtime::RuntimeResult<()> {
            self.inner.push_continuous_stream_input(job_id, batches)
        }

        fn drain_continuous_stream(
            &self,
            job_id: &str,
        ) -> krishiv_runtime::RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
            self.inner.drain_continuous_stream(job_id)
        }

        /// Declines to flush. This refusal IS the test fixture.
        ///
        /// Until step 4 this method was simply ABSENT here, and the trait's
        /// default supplied exactly this error. That default is now gone — a
        /// runtime cannot answer by saying nothing — so the refusal is written
        /// out, which is precisely the improvement: the decision is at the impl
        /// site where it can be grepped and reviewed.
        ///
        /// The behaviour is unchanged, so the assertions below still hold. What
        /// changed is that `RemoteExecutionRuntime` no longer shares it: it
        /// implements a real flush now, and this double stands in for an
        /// old server that cannot.
        ///
        /// Proven discriminating: make this delegate to `self.inner` and
        /// `a_runtime_without_flush_silently_drops_its_trailing_windows` fails
        /// with `flush-less arm should emit 0 row(s) and lose 2`, printing both
        /// recovered windows.
        fn flush_continuous_stream(
            &self,
            job_id: &str,
        ) -> krishiv_runtime::RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
            Err(krishiv_runtime::RuntimeError::unsupported(format!(
                "this runtime cannot flush job '{job_id}'; a bounded run against it \
                 omits any window the watermark never passed"
            )))
        }
    }

    fn job_for(name: &str, input: &std::path::Path, output: &std::path::Path) -> CompiledJob {
        CompiledJob::new(
            name,
            WINDOWED_SQL,
            vec![SourceSpec::unbounded(
                "events",
                "csv",
                input.to_str().expect("input path"),
            )],
            vec![SinkSpec::new(
                "out",
                "json",
                output.to_str().expect("output path"),
            )],
            true,
        )
        .with_engine(EngineKind::Streaming)
    }

    fn write_fixture(entry: &CorpusEntry, dir: &std::path::Path, arm: &str) -> std::path::PathBuf {
        let input = dir.join(format!("{}-{arm}.csv", entry.name));
        std::fs::write(&input, entry.csv).expect("write fixture");
        input
    }

    /// Drive one corpus entry through `StreamingEngine::run` — the embedded
    /// loop, which owns its own source draining and end-of-stream flush.
    async fn via_embedded_engine(entry: &CorpusEntry, dir: &std::path::Path) -> String {
        let input = write_fixture(entry, dir, "embedded");
        let output = dir.join(format!("{}-embedded.json", entry.name));

        let runtime = crate::connector_runtime::embedded_connector_runtime();
        let handle = krishiv_engines::run_job(job_for(entry.name, &input, &output), runtime)
            .await
            .unwrap_or_else(|e| panic!("[{}] embedded loop failed: {e}", entry.name));
        assert_eq!(
            handle.status(),
            JobStatus::Completed,
            "[{}] embedded loop must complete",
            entry.name
        );
        render_sorted(&std::fs::read_to_string(&output).unwrap_or_default())
    }

    /// Drive the same entry through `run_streaming_job_via_runtime` backed by a
    /// runtime that **can** flush — the placement that works today.
    async fn via_runtime_seam(entry: &CorpusEntry, dir: &std::path::Path) -> String {
        let input = write_fixture(entry, dir, "runtime");
        let output = dir.join(format!("{}-runtime.json", entry.name));

        let session = crate::SessionBuilder::new().build().expect("session");
        let handle = run_streaming_job_via_runtime(
            &session.execution_runtime(),
            &job_for(entry.name, &input, &output),
        )
        .await
        .unwrap_or_else(|e| panic!("[{}] runtime-seam loop failed: {e}", entry.name));
        assert_eq!(
            handle.status(),
            JobStatus::Completed,
            "[{}] runtime-seam loop must complete",
            entry.name
        );
        render_sorted(&std::fs::read_to_string(&output).unwrap_or_default())
    }

    /// Drive the same entry through the same loop, backed by a runtime with no
    /// flush — the method set `RemoteExecutionRuntime` actually has, and
    /// therefore the placement `ExecutionMode::Distributed` actually produces.
    ///
    /// Returns `(sink_contents, status)`: the status matters as much as the
    /// contents, because reporting `Completed` over a short answer is the defect.
    async fn via_runtime_seam_without_flush(
        entry: &CorpusEntry,
        dir: &std::path::Path,
    ) -> (String, JobStatus) {
        let input = write_fixture(entry, dir, "noflush");
        let output = dir.join(format!("{}-noflush.json", entry.name));

        let session = crate::SessionBuilder::new().build().expect("session");
        let flushless: Arc<dyn ExecutionRuntime> = Arc::new(FlushlessRuntime {
            inner: session.execution_runtime(),
        });
        let handle =
            run_streaming_job_via_runtime(&flushless, &job_for(entry.name, &input, &output))
                .await
                .unwrap_or_else(|e| panic!("[{}] flush-less seam loop failed: {e}", entry.name));
        (
            render_sorted(&std::fs::read_to_string(&output).unwrap_or_default()),
            handle.status(),
        )
    }

    /// The gate: every corpus entry must produce the same closed windows on the
    /// loops that are supposed to agree, and must produce the windows it is
    /// supposed to.
    ///
    /// Both halves are load-bearing. Comparing the loops alone would pass if
    /// both were broken the same way — and "both emit nothing" is precisely the
    /// failure this file exists for, since an empty sink plus a `Completed` job
    /// is what silent truncation looks like from the outside. So each entry
    /// also asserts its expected windows independently.
    #[tokio::test]
    async fn every_windowed_job_agrees_across_driver_loops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut divergences = Vec::new();

        for entry in CORPUS {
            let embedded = via_embedded_engine(entry, dir.path()).await;
            let runtime = via_runtime_seam(entry, dir.path()).await;

            if embedded != runtime {
                divergences.push(format!(
                    "[{}] the two loops closed different windows\n  \
                     embedded:\n{embedded}\n  runtime-seam:\n{runtime}",
                    entry.name
                ));
                continue;
            }
            for want in entry.expected_json_fragments() {
                if !embedded.contains(&want) {
                    divergences.push(format!(
                        "[{}] both loops agree but both are WRONG: expected {want} \
                         in\n{embedded}\n(an empty sink with a Completed job is the \
                         silent-truncation signature)",
                        entry.name
                    ));
                }
            }
        }

        assert!(
            divergences.is_empty(),
            "windowed streaming diverged across driver loops:\n{}",
            divergences.join("\n\n")
        );
    }

    /// A runtime with no flush loses exactly the windows the watermark never
    /// closed — and still reports success.
    ///
    /// This asserts **today's broken behaviour on purpose**. Every assertion
    /// carries a `BUG(...)` marker and is flipped by the step that fixes the
    /// defect; until then it is the regression guard that keeps the defect from
    /// being described as anything other than what it is.
    ///
    /// Asserting the broken *values* rather than `flushing != flushless` is what
    /// makes this test self-checking. Inequality would pass vacuously if the
    /// harness were miswired so that neither arm produced anything — the classic
    /// miswiring being to build both arms from `SessionBuilder::new().build()`,
    /// which yields the one runtime where flush works.
    #[tokio::test]
    async fn a_runtime_without_flush_silently_drops_its_trailing_windows() {
        let dir = tempfile::tempdir().expect("tempdir");

        for entry in CORPUS {
            let flushing = via_runtime_seam(entry, dir.path()).await;
            let (flushless, status) = via_runtime_seam_without_flush(entry, dir.path()).await;

            // BUG(eos-flush-missing-on-both-distributed-loops): the job reports
            // success over a short answer. The only trace in production is a
            // tracing::warn! the caller never sees. Flipping this to an error is
            // step 5; until then, pin it.
            assert_eq!(
                status,
                JobStatus::Completed,
                "[{}] a flush-less runtime still reports Completed today",
                entry.name
            );

            // The flushing arm is the reference: it must be correct, or this
            // test is measuring against a broken baseline.
            for want in entry.expected_json_fragments() {
                assert!(
                    flushing.contains(&want),
                    "[{}] the flushing arm is the baseline and must be correct: \
                     expected {want} in\n{flushing}",
                    entry.name
                );
            }

            // BUG(eos-flush-missing-on-both-distributed-loops): exactly the
            // windows the watermark never closed are missing. Entry
            // `closed_window_plus_trailing_window` loses only PART of its
            // output — that partial loss is what distinguishes "no flush" from
            // "the loop never ran", and is why the corpus carries it.
            let survived: Vec<String> = entry
                .expected_without_flush
                .iter()
                .map(|(_, total)| format!("\"total\":{total}"))
                .collect();
            for want in &survived {
                assert!(
                    flushless.contains(want),
                    "[{}] a window the watermark DID close must survive without a flush: \
                     expected {want} in\n{flushless}",
                    entry.name
                );
            }
            let lost = entry.expected.len() - entry.expected_without_flush.len();
            let flushless_rows = flushless.lines().filter(|l| !l.trim().is_empty()).count();
            assert_eq!(
                flushless_rows,
                entry.expected_without_flush.len(),
                "[{}] flush-less arm should emit {} row(s) and lose {lost}; got:\n{flushless}",
                entry.name,
                entry.expected_without_flush.len()
            );
        }
    }

    /// The corpus must contain a fixture where the flush actually matters, or
    /// the arm above proves nothing.
    ///
    /// Guards against a future edit that softens every fixture into one the
    /// watermark closes on its own — which would leave both tests green and the
    /// defect uncovered.
    #[test]
    fn the_corpus_still_exercises_the_flush_divergence() {
        let observable = CORPUS.iter().filter(|e| e.flush_is_observable()).count();
        assert!(
            observable >= 2,
            "the flush-less arm can only detect a defect the corpus exposes; \
             {observable} fixture(s) currently distinguish flushing from not"
        );
    }
}
