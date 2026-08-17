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
//! This harness runs one corpus through both loops and compares. It would have
//! failed the day `dd47d50` landed.
//!
//! ## Scope, stated honestly
//!
//! It covers the two loops that actually diverged, and they are the two
//! reachable from this crate. The two executor fragments need an
//! `ExecutorTaskRunner`, a task assignment, and (for the run-loop) a peer table
//! and a cancellation path; extending the corpus to them is a harness problem,
//! not a corpus problem, and is recorded as open rather than quietly skipped.

#[cfg(test)]
mod streaming_conformance_tests {
    use krishiv_engine_core::{EngineKind, JobStatus};

    use crate::connector_runtime::run_streaming_job_via_runtime;
    use crate::{CompiledJob, SinkSpec, SourceSpec};

    /// One corpus entry: a fixture and the windows it must close.
    ///
    /// Chosen for the shapes where a *loop* difference shows, rather than an
    /// operator difference — the operator is shared, so anything it gets wrong
    /// it gets wrong everywhere and identically. What a loop can get wrong is
    /// *when it stops stepping*: whether it flushes at end-of-stream, whether
    /// it advances the watermark on idle, whether the last batch is processed
    /// before teardown.
    const CORPUS: &[(&str, &str, &[&str])] = &[
        // THE regression. Every event lands in [0,10000) and nothing later
        // arrives, so no watermark ever passes the window end. Only an
        // end-of-stream flush closes it. A loop without one writes nothing and
        // reports success — the exact silent truncation of dd47d50/8756b41.
        (
            "trailing_window_never_closed_by_watermark",
            "user_id,ts,amount\na,1000,10\na,5000,20\nb,6000,5\n",
            &["\"total\":30", "\"total\":5"],
        ),
        // A window the watermark DOES close, plus a trailing one it does not.
        // Catches the opposite error: a loop that flushes eagerly and emits the
        // open window twice, or one that flushes instead of closing normally.
        (
            "closed_window_plus_trailing_window",
            "user_id,ts,amount\na,1000,10\na,5000,20\na,25000,7\n",
            &["\"total\":30", "\"total\":7"],
        ),
        // Single event, single window, nothing else. The degenerate case where
        // "no output" is easiest to mistake for "no input".
        (
            "single_event_single_window",
            "user_id,ts,amount\nz,42,99\n",
            &["\"total\":99"],
        ),
    ];

    const WINDOWED_SQL: &str = "SELECT user_id, SUM(amount) AS total \
         FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 10000) \
         GROUP BY user_id, window_start, window_end";

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

    /// Render a sink's contents as sorted lines, so the comparison is about
    /// *which windows closed*, not the order a loop happened to emit them in.
    /// Emission order is genuinely a loop's own business; window content is not.
    fn render(sink: &std::path::Path) -> String {
        let raw = std::fs::read_to_string(sink).unwrap_or_default();
        let mut lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
        lines.sort_unstable();
        lines.join("\n")
    }

    /// Drive one corpus entry through `StreamingEngine::run` — the embedded
    /// loop, which owns its own source draining and end-of-stream flush.
    async fn via_embedded_engine(name: &str, csv: &str, dir: &std::path::Path) -> String {
        let input = dir.join(format!("{name}-embedded.csv"));
        let output = dir.join(format!("{name}-embedded.json"));
        std::fs::write(&input, csv).expect("write fixture");

        let runtime = crate::connector_runtime::embedded_connector_runtime();
        let handle = krishiv_engines::run_job(job_for(name, &input, &output), runtime)
            .await
            .unwrap_or_else(|e| panic!("[{name}] embedded loop failed: {e}"));
        assert_eq!(
            handle.status(),
            JobStatus::Completed,
            "[{name}] embedded loop must complete"
        );
        render(&output)
    }

    /// Drive the same entry through `run_streaming_job_via_runtime` — the
    /// bounded distributed loop, which pushes through the runtime's
    /// register/push/flush/drain seam instead of stepping the operator itself.
    ///
    /// Run against an in-process runtime: that is the same `ExecutionRuntime`
    /// trait the remote coordinator backend implements, so the orchestration is
    /// exercised without a live cluster. The loop under test is the caller, not
    /// the transport.
    async fn via_runtime_seam(name: &str, csv: &str, dir: &std::path::Path) -> String {
        let input = dir.join(format!("{name}-runtime.csv"));
        let output = dir.join(format!("{name}-runtime.json"));
        std::fs::write(&input, csv).expect("write fixture");

        let session = crate::SessionBuilder::new().build().expect("session");
        let handle = run_streaming_job_via_runtime(
            &session.execution_runtime(),
            &job_for(name, &input, &output),
        )
        .await
        .unwrap_or_else(|e| panic!("[{name}] runtime-seam loop failed: {e}"));
        assert_eq!(
            handle.status(),
            JobStatus::Completed,
            "[{name}] runtime-seam loop must complete"
        );
        render(&output)
    }

    /// The gate: every corpus entry must produce the same closed windows on
    /// both loops, and must produce the windows it is supposed to.
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

        for (name, csv, expected) in CORPUS {
            let embedded = via_embedded_engine(name, csv, dir.path()).await;
            let runtime = via_runtime_seam(name, csv, dir.path()).await;

            if embedded != runtime {
                divergences.push(format!(
                    "[{name}] the two loops closed different windows\n  \
                     embedded:\n{embedded}\n  runtime-seam:\n{runtime}"
                ));
                continue;
            }
            for want in *expected {
                if !embedded.contains(want) {
                    divergences.push(format!(
                        "[{name}] both loops agree but both are WRONG: expected {want} \
                         in\n{embedded}\n(an empty sink with a Completed job is the \
                         silent-truncation signature)"
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
}
