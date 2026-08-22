//! The `write()` terminal on [`StreamingDataFrame`] (task #150 P2).
//!
//! One place configures where a streaming pipeline's output lands — an
//! Iceberg table (checkpoint-aligned two-phase commit), any registered
//! connector sink (at-least-once, per-cycle flush), or no sink at all
//! (output stays drainable through the returned [`StreamingJob`]). The same
//! terminal serves every execution mode the session runs in; combinations a
//! mode cannot honour are refused BY NAME at `start()`, never downgraded.

// Sync-surface boundary module (async-contract): `start()` is the terminal
// of a synchronous builder chain, called from sync tests and the pyo3
// binding thread; the embedded-engine arm drives the query-start future
// through the sanctioned bridge exactly as `session.rs` does.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;

use krishiv_runtime::ContinuousRegisterOptions;
use krishiv_scheduler::continuous_stream_http::ContinuousSinkSpec;

use crate::error::{KrishivError, Result};
use crate::session::Session;
use crate::streaming_dataframe::StreamingDataFrame;
use crate::streaming_job::StreamingJob;

/// Builder returned by [`StreamingDataFrame::write`].
pub struct StreamWriter {
    df: StreamingDataFrame,
    format: Option<String>,
    options: BTreeMap<String, String>,
    output_mode: String,
    trigger: String,
    trigger_interval_ms: u64,
    parallelism: Option<u32>,
    checkpoint_interval_ms: Option<u64>,
    checkpoint_storage_path: Option<String>,
}

impl StreamWriter {
    pub(crate) fn new(df: StreamingDataFrame) -> Self {
        Self {
            df,
            format: None,
            options: BTreeMap::new(),
            output_mode: String::from("append"),
            trigger: String::from("continuous"),
            trigger_interval_ms: 1_000,
            parallelism: None,
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
        }
    }

    /// Sink format: `"iceberg"`, any registered connector kind (`"csv"`,
    /// `"jdbc-sink"`, `"kafka-sink"`, …), or omit for a drain-driven job
    /// whose output the caller collects through the handle.
    #[must_use]
    pub fn format(mut self, name: impl Into<String>) -> Self {
        self.format = Some(name.into());
        self
    }

    /// Sink-specific option (Iceberg `table`/`root`/`catalog`/`namespace`/
    /// `mode`/`key_columns`; connector driver properties).
    #[must_use]
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Output mode: `"append"` (default). `"update"` and `"complete"` are
    /// staged behind task #150 P3/P4 and refused by name until then.
    #[must_use]
    pub fn output_mode(mut self, mode: impl Into<String>) -> Self {
        self.output_mode = mode.into().to_lowercase();
        self
    }

    /// Trigger policy: `"continuous"` (default), `"processing_time"`,
    /// `"once"`, or `"available_now"`. The continuous engine is push-driven,
    /// so `once`/`available_now` mean: push the bounded input, then the
    /// returned handle's `flush()` closes the trailing windows.
    #[must_use]
    pub fn trigger(mut self, trigger: impl Into<String>, interval_ms: u64) -> Self {
        self.trigger = trigger.into().to_lowercase();
        self.trigger_interval_ms = interval_ms;
        self
    }

    /// Run-loop subtask count (requires a coordinator-backed session mode).
    #[must_use]
    pub fn parallelism(mut self, parallelism: u32) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Arm barrier checkpointing (both knobs required).
    #[must_use]
    pub fn checkpoint(mut self, interval_ms: u64, storage_path: impl Into<String>) -> Self {
        self.checkpoint_interval_ms = Some(interval_ms);
        self.checkpoint_storage_path = Some(storage_path.into());
        self
    }

    fn sink_spec(&self) -> Result<Option<ContinuousSinkSpec>> {
        let Some(format) = self
            .format
            .as_deref()
            .map(str::trim)
            .filter(|f| !f.is_empty())
        else {
            return Ok(None);
        };
        if matches!(format, "console" | "memory") {
            // Client-side presentation of drained output, not an engine sink.
            return Ok(None);
        }
        let opt = |key: &str| self.options.get(key).cloned();
        if format == "iceberg" {
            return Ok(Some(ContinuousSinkSpec {
                connector: None,
                options: BTreeMap::new(),
                root: opt("root").unwrap_or_default(),
                table: opt("table").unwrap_or_default(),
                mode: opt("mode").unwrap_or_else(|| String::from("append")),
                key_columns: opt("key_columns")
                    .map(|v| v.split(',').map(|c| c.trim().to_owned()).collect())
                    .unwrap_or_default(),
                op_column: opt("op_column"),
                catalog: opt("catalog"),
                namespace: opt("namespace"),
            }));
        }
        Ok(Some(ContinuousSinkSpec {
            connector: Some(format.to_owned()),
            options: self.options.clone(),
            root: String::new(),
            table: String::new(),
            mode: String::from("append"),
            key_columns: Vec::new(),
            op_column: None,
            catalog: None,
            namespace: None,
        }))
    }

    /// Register the job on `session` (its execution mode decides where it
    /// runs) and return the unified [`StreamingJob`] handle.
    ///
    /// # Errors
    /// Refusals by name: unknown output mode or trigger, `update`/`complete`
    /// before their phases land, sink/parallelism options on a runtime that
    /// cannot honour them (the options seam's existing fail-closed echo).
    pub fn start(self, session: &Session, job_name: impl Into<String>) -> Result<StreamingJob> {
        match self.output_mode.as_str() {
            "append" | "update" | "complete" => {}
            other => {
                return Err(KrishivError::InvalidConfig {
                    message: format!(
                        "unknown output mode '{other}'; expected append, update, or complete"
                    ),
                });
            }
        }
        if !matches!(
            self.trigger.as_str(),
            "continuous" | "processing_time" | "once" | "available_now"
        ) {
            return Err(KrishivError::InvalidConfig {
                message: format!(
                    "unknown trigger '{}'; expected continuous, processing_time, once, or \
                     available_now",
                    self.trigger
                ),
            });
        }

        // A compiled class task from `Session::stream_sql` overrides the
        // builder-derived window spec: the job registers class-routed
        // (pipeline/join/window/stateless) on the coordinator, exactly as the
        // engine's own routing ladder decided. Update/complete modes are a
        // window-operator capability, so non-append is refused BY NAME here
        // rather than registering a job that silently appends.
        // A window-class SQL stream on an EMBEDDED session bridges to the
        // same local spec the builder produces, so `stream_sql` works in
        // every execution mode for the window class — only the join/
        // pipeline/stateless classes need a coordinator (their loops are
        // subtask machinery an embedded session does not host).
        let bridged_window: Option<krishiv_runtime::LocalWindowExecutionSpec> =
            match self.df.task_override() {
                Some(krishiv_plan::stream_task::StreamingTaskSpec::Window(w))
                    if session.mode() == crate::types::ExecutionMode::Embedded =>
                {
                    Some(w.as_ref().into())
                }
                _ => None,
            };
        if let Some(task) = self.df.task_override()
            && bridged_window.is_none()
        {
            if self.output_mode != "append" {
                return Err(KrishivError::InvalidConfig {
                    message: format!(
                        "output mode '{}' is not supported for class-routed streaming SQL \
                         jobs: update/complete are built on the tumbling window operator's \
                         open-window snapshot. Use the builder form (with_event_time/key_by/\
                         tumbling_window) for update or complete mode",
                        self.output_mode
                    ),
                });
            }
            let url = session
                .coordinator_http_url()
                .ok_or_else(|| KrishivError::InvalidConfig {
                    message: "class-routed streaming SQL registers on a coordinator; this \
                              session has no coordinator HTTP URL (embedded sessions use \
                              Session::stream for SQL streams)"
                        .into(),
                })?
                .to_owned();
            let job_name = job_name.into();
            let mut options =
                krishiv_runtime::ContinuousRegisterOptions::run_loop(self.parallelism.unwrap_or(1));
            options.mode = Some(String::from("run-loop"));
            options.checkpoint_interval_ms = self.checkpoint_interval_ms;
            options.checkpoint_storage_path = self.checkpoint_storage_path.clone();
            options.sink = self.sink_spec()?;
            krishiv_common::async_util::block_on(
                krishiv_runtime::execute_coordinator_continuous_register_task(
                    &url, &job_name, task, &options,
                ),
            )
            .map_err(KrishivError::from)?;
            return Ok(StreamingJob::from_session_job(session.clone(), job_name));
        }

        let mut spec = match bridged_window {
            Some(spec) => spec,
            None => self
                .df
                .execution_spec()?
                .ok_or_else(|| KrishivError::InvalidConfig {
                    message: "write() needs a windowed pipeline: set with_event_time/key_by \
                          and a window before start()"
                        .into(),
                })?,
        };
        let tumbling = matches!(spec.window_kind, krishiv_runtime::LocalWindowKind::Tumbling);
        if self.output_mode == "update" && !tumbling {
            // Update mode = speculative re-emission of OPEN windows as
            // provisional upserts keyed on (key, window_start_ms). Only the
            // tumbling operator supports the read-only open-window snapshot;
            // session/sliding shapes are refused BY NAME rather than
            // registering a job that silently behaves like append.
            return Err(KrishivError::InvalidConfig {
                message: "output mode 'update' requires a tumbling window: the \
                          speculative open-window snapshot that update mode is \
                          built on exists only for the tumbling operator"
                    .into(),
            });
        }
        if matches!(self.output_mode.as_str(), "update" | "complete") && tumbling {
            // Early fire keeps the update stream (and a complete view fed by
            // it) fresh while windows are still open. Complete mode WITHOUT
            // tumbling skips this: the sink-layer fold works on any window
            // shape, it just updates only when windows CLOSE — final results
            // only, never provisional ones (task #151 gap closure).
            let interval = if self.trigger == "processing_time" {
                self.trigger_interval_ms.max(1)
            } else {
                1_000
            };
            spec.early_fire_interval_ms = Some(interval);
        }
        let complete_key = spec.key_column.clone();
        let sink = self.sink_spec()?;
        let job_name = job_name.into();

        // Embedded sessions with an ENGINE sink route through the embedded
        // structured-streaming engine, which owns kafka/parquet/iceberg/
        // console/memory delivery in-process — the same pipeline spec, the
        // same handle, a different executor. This is the convergence that
        // lets the legacy write_stream() surface be REMOVED without losing
        // embedded sink delivery (task #150 P6/P7).
        let engine_sink = matches!(
            self.format.as_deref().map(str::trim),
            Some("kafka" | "parquet" | "iceberg" | "console" | "memory")
        );
        if session.mode() == crate::types::ExecutionMode::Embedded && engine_sink {
            use crate::streaming_builder::{
                DataStreamWriter, StreamingOutputMode, StreamingTrigger,
            };
            let mut writer = DataStreamWriter::for_streaming(self.df.clone())
                .output_mode(match self.output_mode.as_str() {
                    "update" => StreamingOutputMode::Update,
                    "complete" => StreamingOutputMode::Complete,
                    _ => StreamingOutputMode::Append,
                })
                .trigger(match self.trigger.as_str() {
                    "once" => StreamingTrigger::Once,
                    "available_now" => StreamingTrigger::AvailableNow,
                    "processing_time" => StreamingTrigger::ProcessingTime(
                        std::time::Duration::from_millis(self.trigger_interval_ms.max(1)),
                    ),
                    _ => StreamingTrigger::Continuous(std::time::Duration::from_millis(
                        self.trigger_interval_ms.max(1),
                    )),
                })
                .query_name(&job_name);
            if let Some(format) = &self.format {
                writer = writer.format(format);
            }
            for (key, value) in &self.options {
                writer = writer.option(key, value.clone());
            }
            let query = krishiv_common::async_util::block_on(writer.start())?;
            return Ok(StreamingJob::from_query(query));
        }

        // Coordinator-backed sessions ALWAYS register the run-loop model:
        // it is the engine every #149/#150 capability lives in (parallel
        // subtasks, keyed exchange, barrier checkpointing, the EOS barrier,
        // update-mode early fire). Registering without a model fell back to
        // the legacy single-subtask cycle path, whose fencing aborted the
        // terminal's plain pushes — the two-model split is exactly what the
        // convergence retires from the user's view.
        let coordinator_backed = session.mode() != crate::types::ExecutionMode::Embedded;
        let needs_options = coordinator_backed
            || sink.is_some()
            || self.parallelism.is_some()
            || self.checkpoint_interval_ms.is_some();
        if needs_options {
            let options = ContinuousRegisterOptions {
                mode: Some(String::from("run-loop")),
                parallelism: Some(self.parallelism.unwrap_or(1)),
                sources: Vec::new(),
                checkpoint_interval_ms: self.checkpoint_interval_ms,
                checkpoint_storage_path: self.checkpoint_storage_path.clone(),
                sink,
            };
            session.submit_stream_job_with_options(&job_name, spec, &options)?;
        } else {
            session.submit_stream_job(&job_name, spec)?;
        }
        let job = StreamingJob::from_session_job(session.clone(), job_name);
        if self.output_mode == "complete" {
            // The view keys on the group column plus the window identity —
            // deltas (provisional early-fires AND final closes) fold by that
            // pair, so each drain returns the maintained full table.
            return Ok(job.with_complete_view(vec![complete_key, String::from("window_start_ms")]));
        }
        Ok(job)
    }
}

impl StreamingDataFrame {
    /// Terminal write configuration for this pipeline — sinks, output mode,
    /// trigger, parallelism, checkpointing — executed by [`StreamWriter::
    /// start`] on whichever session mode you hand it.
    #[must_use]
    pub fn write(&self) -> StreamWriter {
        StreamWriter::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn windowed(session: &Session) -> StreamingDataFrame {
        session
            .sql("SELECT 'seed' AS user_id, CAST(0 AS BIGINT) AS ts")
            .expect("seed df")
            .stream()
            .with_event_time("ts")
            .key_by("user_id")
            .tumbling_window(10_000)
    }

    fn batch(users: &[&str], ts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(users.to_vec())) as _,
                Arc::new(Int64Array::from(ts.to_vec())) as _,
            ],
        )
        .expect("batch")
    }

    /// The ONE terminal drives an embedded end-to-end lifecycle: the write()
    /// builder registers the job and the returned unified handle pushes,
    /// flushes, and stops it.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_start_registers_and_the_handle_round_trips() {
        let session = Session::builder().build().expect("session");
        let job = windowed(&session)
            .write()
            .trigger("available_now", 0)
            .start(&session, "wjob")
            .expect("start");
        job.push(vec![batch(&["a", "a"], &[1_000, 2_000])])
            .await
            .expect("push");
        let flushed = job.flush().await.expect("flush");
        let rows: usize = flushed.iter().map(RecordBatch::num_rows).sum();
        assert!(rows >= 1, "available_now + flush must emit the open window");
        job.stop().await.expect("stop");
    }

    /// complete is staged behind P4 — refused BY NAME, never silently
    /// registered as append. (update graduated in P3 and registers.)
    #[test]
    fn staged_output_modes_are_refused_by_name() {
        let session = Session::builder().build().expect("session");
        let Err(err) = windowed(&session)
            .write()
            .output_mode("complete-typo")
            .start(&session, "m-complete-typo")
        else {
            panic!("unknown mode must refuse");
        };
        assert!(err.to_string().contains("complete-typo"), "{err}");
        let Err(err) = windowed(&session)
            .write()
            .output_mode("bogus")
            .start(&session, "m-bogus")
        else {
            panic!("unknown mode must refuse");
        };
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    /// An embedded session cannot honour a sink (the in-process runtime has
    /// no sink machinery) — the options seam refuses BY NAME instead of
    /// registering a job that silently writes nowhere.
    #[test]
    fn embedded_sink_is_refused_not_silently_dropped() {
        let session = Session::builder().build().expect("session");
        let Err(err) = windowed(&session)
            .write()
            .format("csv")
            .option("path", "/tmp/out.csv")
            .start(&session, "sinkjob")
        else {
            panic!("embedded + sink must refuse");
        };
        let text = err.to_string();
        assert!(
            text.contains("cannot honour") || text.contains("options"),
            "the refusal must name the options seam: {text}"
        );
    }
}

#[cfg(test)]
mod update_mode_tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn batch(users: &[&str], ts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(users.to_vec())) as _,
                Arc::new(Int64Array::from(ts.to_vec())) as _,
            ],
        )
        .expect("batch")
    }

    /// Update mode end to end on the embedded engine: rows land in a window
    /// the watermark NEVER closes (huge lag), and a paced drain still
    /// surfaces them as provisional upserts. Pre-fix behavior (no early-fire
    /// promotion): the drain returns nothing until close or EOS, and this
    /// test goes red on the emptiness assertion.
    #[tokio::test(flavor = "multi_thread")]
    async fn update_mode_surfaces_open_windows_between_closes() {
        let session = Session::builder().build().expect("session");
        let job = session
            .sql("SELECT 'seed' AS user_id, CAST(0 AS BIGINT) AS ts")
            .expect("df")
            .stream()
            .with_event_time("ts")
            .key_by("user_id")
            .tumbling_window(3_600_000)
            .with_watermark_lag(3_600_000)
            .write()
            .output_mode("update")
            .trigger("processing_time", 1)
            .start(&session, "upd-open")
            .expect("update job registers");
        job.push(vec![batch(&["a", "a", "b"], &[1_000, 2_000, 3_000])])
            .await
            .expect("push");
        // First drain applies input; the fire is paced at 1ms, so the next
        // drain (after a beat) must surface the OPEN windows.
        let _ = job.drain().await.expect("drain applies input");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let provisional = job.drain().await.expect("paced drain");
        let rows: usize = provisional.iter().map(RecordBatch::num_rows).sum();
        assert!(
            rows >= 2,
            "open windows for keys a and b must surface as provisional upserts, got {rows}"
        );
        job.stop().await.expect("stop");
    }

    /// Update mode on a session window is refused BY NAME — the speculative
    /// snapshot only exists for the tumbling operator, and registering the
    /// job anyway would silently behave like append.
    #[test]
    fn update_mode_refuses_non_tumbling_windows() {
        let session = Session::builder().build().expect("session");
        let Err(err) = session
            .sql("SELECT 'seed' AS user_id, CAST(0 AS BIGINT) AS ts")
            .expect("df")
            .stream()
            .with_event_time("ts")
            .key_by("user_id")
            .session_window(5_000)
            .write()
            .output_mode("update")
            .start(&session, "upd-session")
        else {
            panic!("session-window update mode must refuse");
        };
        assert!(err.to_string().contains("tumbling"), "{err}");
    }
}

#[cfg(test)]
mod complete_mode_tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn batch(users: &[&str], ts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(users.to_vec())) as _,
                Arc::new(Int64Array::from(ts.to_vec())) as _,
            ],
        )
        .expect("batch")
    }

    fn user_rows(batches: &[RecordBatch]) -> Vec<String> {
        let mut out = Vec::new();
        for b in batches {
            let users = b
                .column_by_name("user_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .expect("user col");
            for i in 0..b.num_rows() {
                out.push(users.value(i).to_owned());
            }
        }
        out.sort();
        out
    }

    /// Complete mode returns the FULL result table on every drain: a key
    /// whose data stopped arriving must STILL appear in later drains. That
    /// stale-key retention is exactly what the fold provides and what raw
    /// delta drains cannot — pre-fix (no view), the second drain only
    /// carries key b and the assertion on key a goes red.
    #[tokio::test(flavor = "multi_thread")]
    async fn complete_mode_retains_stale_keys_across_drains() {
        let session = Session::builder().build().expect("session");
        let job = session
            .sql("SELECT 'seed' AS user_id, CAST(0 AS BIGINT) AS ts")
            .expect("df")
            .stream()
            .with_event_time("ts")
            .key_by("user_id")
            .tumbling_window(1_000)
            .write()
            .output_mode("complete")
            .trigger("processing_time", 1)
            .start(&session, "cmp-stale")
            .expect("complete job registers");

        // a's window [1000,2000) CLOSES when b@5000 advances the watermark:
        // its final row is emitted exactly once and never again — early fire
        // only re-emits OPEN windows. Raw delta drains therefore lose a
        // after that; only the complete-mode fold retains it.
        job.push(vec![batch(&["a"], &[1_000])])
            .await
            .expect("push a");
        job.push(vec![batch(&["b"], &[5_000])])
            .await
            .expect("push b");
        let _ = job.drain().await.expect("drain closes a");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        // This drain's raw deltas are early-fires of b's OPEN window only;
        // the FULL table must still carry the closed a.
        let table = job.drain().await.expect("full table");
        let users = user_rows(&table);
        assert!(
            users.contains(&String::from("a")) && users.contains(&String::from("b")),
            "the complete table must retain the CLOSED key a alongside the open b: {users:?}"
        );
        job.stop().await.expect("stop");
    }

    /// Complete mode is a sink-layer fold of FINAL window closes, which works
    /// for any window shape — only UPDATE mode needs the tumbling operator's
    /// open-window snapshot. A sliding-window complete job must be accepted
    /// (pre-fix the shared tumbling gate refused it) and its fold must retain
    /// closed windows across drains exactly like the tumbling case. (Session
    /// windows are additionally accepted by this gate but the EMBEDDED
    /// registry refuses them for its own wall-clock reason, so the embedded
    /// fixture uses sliding; session-window complete runs on run-loop
    /// registrations.)
    #[tokio::test(flavor = "multi_thread")]
    async fn complete_mode_accepts_sliding_windows_and_folds_final_closes() {
        let session = Session::builder().build().expect("session");
        let job = session
            .sql("SELECT 'seed' AS user_id, CAST(0 AS BIGINT) AS ts")
            .expect("df")
            .stream()
            .with_event_time("ts")
            .key_by("user_id")
            .sliding_window(1_000, 500)
            .write()
            .output_mode("complete")
            .trigger("available_now", 0)
            .start(&session, "cmp-sliding")
            .expect("sliding-window complete job must register, not be refused");

        // a's windows close once b@10_000 advances the watermark far past
        // them; b's close at flush. The full table must carry both keys.
        job.push(vec![batch(&["a"], &[1_000])])
            .await
            .expect("push a");
        job.push(vec![batch(&["b"], &[10_000])])
            .await
            .expect("push b");
        let _ = job.flush().await.expect("flush closes trailing sessions");
        let table = job.drain().await.expect("full table");
        let users = user_rows(&table);
        assert!(
            users.contains(&String::from("a")) && users.contains(&String::from("b")),
            "the complete table must carry both closed sessions: {users:?}"
        );
        job.stop().await.expect("stop");
    }
}

#[cfg(test)]
mod stream_sql_embedded_tests {
    use super::*;

    /// Window-class streaming SQL through `Session::stream_sql` must run on
    /// an EMBEDDED session by bridging to the same local spec the builder
    /// produces (pre-fix, every class-routed job demanded a coordinator URL
    /// and embedded callers were refused BY NAME even for plain windows).
    #[tokio::test(flavor = "multi_thread")]
    async fn window_class_stream_sql_runs_embedded() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let session = Session::builder().build().expect("session");
        let job = session
            .stream_sql(
                "SELECT user_id, COUNT(*) AS c \
                 FROM TUMBLE(TABLE src, DESCRIPTOR(ts), 1000) \
                 GROUP BY user_id, window_start, window_end",
            )
            .expect("compiles")
            .write()
            .trigger("available_now", 0)
            .start(&session, "sqlw-embedded")
            .expect("embedded window-class SQL stream must start");

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
                Arc::new(Int64Array::from(vec![100i64, 200, 300])) as _,
            ],
        )
        .expect("batch");
        job.push(vec![batch]).await.expect("push");
        let flushed = job.flush().await.expect("flush");
        let rows: usize = flushed.iter().map(RecordBatch::num_rows).sum();
        assert!(rows > 0, "the flushed window closes must carry rows");
        job.stop().await.expect("stop");
    }
}

#[cfg(test)]
mod embedded_engine_bridge_tests {
    use super::*;

    /// Embedded + engine sink routes through the structured-streaming
    /// engine and returns a Query-backed handle: output goes to the SINK,
    /// so drain refuses by name instead of half-working; stop() stops the
    /// engine query. Pre-bridge, this combination refused entirely and the
    /// legacy write_stream() surface could not be removed without losing
    /// embedded sink delivery.
    #[tokio::test(flavor = "multi_thread")]
    async fn embedded_engine_sink_routes_through_the_engine() {
        let session = Session::builder().build().expect("session");
        let job = session
            .sql("SELECT 'seed' AS user_id, CAST(0 AS BIGINT) AS ts")
            .expect("df")
            .stream()
            .with_event_time("ts")
            .key_by("user_id")
            .tumbling_window(1_000)
            .write()
            .format("memory")
            .trigger("available_now", 0)
            .start(&session, "bridge-mem")
            .expect("engine-sink job starts");
        assert!(!job.id().is_empty());
        let Err(err) = job.drain().await else {
            panic!("a sink-backed query must refuse drain by name");
        };
        assert!(err.to_string().contains("sink"), "{err}");
        job.stop().await.expect("stop");
    }
}
