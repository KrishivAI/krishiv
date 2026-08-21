//! The `write()` terminal on [`StreamingDataFrame`] (task #150 P2).
//!
//! One place configures where a streaming pipeline's output lands — an
//! Iceberg table (checkpoint-aligned two-phase commit), any registered
//! connector sink (at-least-once, per-cycle flush), or no sink at all
//! (output stays drainable through the returned [`StreamingJob`]). The same
//! terminal serves every execution mode the session runs in; combinations a
//! mode cannot honour are refused BY NAME at `start()`, never downgraded.

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
            "append" => {}
            "update" | "complete" => {
                return Err(KrishivError::InvalidConfig {
                    message: format!(
                        "output mode '{}' is staged behind task #150 P3/P4 and not yet \
                         wired; 'append' is the supported mode",
                        self.output_mode
                    ),
                });
            }
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

        let spec = self
            .df
            .execution_spec()?
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: "write() needs a windowed pipeline: set with_event_time/key_by and a \
                      window before start()"
                    .into(),
            })?;
        let sink = self.sink_spec()?;
        let job_name = job_name.into();

        let needs_options =
            sink.is_some() || self.parallelism.is_some() || self.checkpoint_interval_ms.is_some();
        if needs_options {
            let options = ContinuousRegisterOptions {
                mode: self.parallelism.map(|_| String::from("run-loop")),
                parallelism: self.parallelism,
                sources: Vec::new(),
                checkpoint_interval_ms: self.checkpoint_interval_ms,
                checkpoint_storage_path: self.checkpoint_storage_path.clone(),
                sink,
            };
            session.submit_stream_job_with_options(&job_name, spec, &options)?;
        } else {
            session.submit_stream_job(&job_name, spec)?;
        }
        Ok(StreamingJob::from_session_job(session.clone(), job_name))
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

    /// update/complete are staged behind P3/P4 — refused BY NAME, never
    /// silently registered as append.
    #[test]
    fn staged_output_modes_are_refused_by_name() {
        let session = Session::builder().build().expect("session");
        for mode in ["update", "complete"] {
            let Err(err) = windowed(&session)
                .write()
                .output_mode(mode)
                .start(&session, format!("m-{mode}"))
            else {
                panic!("staged mode '{mode}' must refuse");
            };
            assert!(err.to_string().contains(mode), "{err}");
        }
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
