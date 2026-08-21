//! The ONE streaming lifecycle handle (task #150 P1).
//!
//! Every way of running a stream — the embedded structured-streaming query,
//! a continuous job submitted through a [`Session`] (any execution mode),
//! or a job on a remote coordinator you attach to by id — answers the same
//! surface: identity, liveness, input, output, end-of-stream, and stop.
//! Before this handle existed each path had its own partial vocabulary
//! (`StreamingQuery` had lifecycle but no push/drain; the session job id
//! had push/drain but no stop; `RemoteStreamingJob` had neither state nor
//! stop), and features grew on whichever surface was nearest — the sibling
//! drift this convergence exists to end.

use arrow::record_batch::RecordBatch;

use crate::error::{KrishivError, Result};
use crate::session::Session;
use crate::streaming_builder::StreamingQuery;

/// Where a [`StreamingJob`]'s verbs are served from.
enum Backend {
    /// An embedded structured-streaming query (the `write_stream()` engine
    /// loop). It reads registered sources and delivers to its sink, so
    /// push/drain/flush are refused BY NAME rather than half-working.
    Query(StreamingQuery),
    /// A continuous job registered through a session's execution runtime —
    /// mode-routed: in-process registry on embedded, coordinator on
    /// single-node/distributed. Boxed: `Session` is a wide value and clippy's
    /// variant-size lint is right that the other arms shouldn't pay for it.
    Session {
        session: Box<Session>,
        job_id: String,
    },
    /// A job on a remote coordinator, attached by id without a session.
    Remote(krishiv_runtime::RemoteStreamingJob),
}

/// Unified streaming lifecycle handle. See the module docs.
pub struct StreamingJob {
    backend: Backend,
}

impl StreamingJob {
    /// Wrap an embedded structured-streaming query.
    #[must_use]
    pub fn from_query(query: StreamingQuery) -> Self {
        Self {
            backend: Backend::Query(query),
        }
    }

    /// Handle for a continuous job registered on `session` (any mode).
    #[must_use]
    pub fn from_session_job(session: Session, job_id: impl Into<String>) -> Self {
        Self {
            backend: Backend::Session {
                session: Box::new(session),
                job_id: job_id.into(),
            },
        }
    }

    /// Attach to a job on a remote coordinator by id, without a session.
    #[must_use]
    pub fn attach(coordinator_http: impl Into<String>, job_id: impl Into<String>) -> Self {
        Self {
            backend: Backend::Remote(krishiv_runtime::RemoteStreamingJob::from_job_id(
                coordinator_http,
                job_id,
            )),
        }
    }

    /// The job/query id.
    #[must_use]
    pub fn id(&self) -> String {
        match &self.backend {
            Backend::Query(q) => q.id().to_string(),
            Backend::Session { job_id, .. } => job_id.clone(),
            Backend::Remote(job) => job.job_id().to_owned(),
        }
    }

    /// Push input batches to the job.
    ///
    /// # Errors
    /// Refused by name for embedded structured queries (they read their
    /// registered sources — push to the source table instead); otherwise
    /// propagates the runtime/transport error.
    pub async fn push(&self, batches: Vec<RecordBatch>) -> Result<()> {
        match &self.backend {
            Backend::Query(_) => Err(KrishivError::InvalidConfig {
                message: "this streaming job is an embedded structured query: it reads \
                          its registered sources, so push its source table (e.g. the \
                          memory stream) instead of the job handle"
                    .into(),
            }),
            Backend::Session { session, job_id } => session.push_stream_job_input(job_id, batches),
            Backend::Remote(job) => job.push(&batches).await.map_err(KrishivError::from),
        }
    }

    /// Drain newly emitted output batches.
    ///
    /// # Errors
    /// Refused by name for embedded structured queries (output goes to the
    /// configured sink); otherwise propagates the runtime/transport error.
    pub async fn drain(&self) -> Result<Vec<RecordBatch>> {
        match &self.backend {
            Backend::Query(_) => Err(KrishivError::InvalidConfig {
                message: "this streaming job is an embedded structured query: its \
                          output goes to the configured sink, not a drainable buffer"
                    .into(),
            }),
            Backend::Session { session, job_id } => session.poll_stream_job(job_id).await,
            Backend::Remote(job) => job.drain().await.map_err(KrishivError::from),
        }
    }

    /// Declare end-of-stream: close every window the watermark never reached
    /// and return (session jobs) or stage (remote run-loop jobs, collected by
    /// the next [`drain`](Self::drain)) the flushed output.
    ///
    /// # Errors
    /// Refused by name for embedded structured queries (use the
    /// `available_now` trigger); otherwise propagates the error.
    pub async fn flush(&self) -> Result<Vec<RecordBatch>> {
        match &self.backend {
            Backend::Query(_) => Err(KrishivError::InvalidConfig {
                message: "this streaming job is an embedded structured query: bounded \
                          completion is expressed with the available_now trigger, not \
                          flush()"
                    .into(),
            }),
            Backend::Session { session, job_id } => session.flush_stream_job(job_id),
            Backend::Remote(job) => {
                krishiv_runtime::execute_coordinator_continuous_flush(
                    job.coordinator_http(),
                    job.job_id(),
                )
                .await
                .map_err(KrishivError::from)?;
                Ok(Vec::new())
            }
        }
    }

    /// Stop the job: loops exit, state and undrained egress are discarded,
    /// the id becomes free.
    ///
    /// # Errors
    /// Propagates the runtime/transport error.
    pub async fn stop(&self) -> Result<()> {
        match &self.backend {
            Backend::Query(q) => {
                q.stop();
                Ok(())
            }
            Backend::Session { session, job_id } => session.stop_stream_job(job_id),
            Backend::Remote(job) => krishiv_runtime::execute_coordinator_continuous_deregister(
                job.coordinator_http(),
                job.job_id(),
            )
            .await
            .map_err(KrishivError::from),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind};

    use super::*;

    fn spec() -> LocalWindowExecutionSpec {
        LocalWindowExecutionSpec {
            key_column: "user_id".into(),
            key_column_type: String::from("utf8"),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            early_fire_interval_ms: None,
            window_kind: LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
            window_timezone: None,
            row_filter: None,
        }
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

    /// The ONE handle drives a whole embedded continuous job lifecycle:
    /// push, end-of-stream flush, and stop. Before task #150 P1, stop did
    /// not exist on ANY session surface — a continuous job could only be
    /// abandoned, never stopped, and its state lived for the process
    /// lifetime. Pre-fix behavior for the final assertion: push to a
    /// stopped job SUCCEEDS against the leaked state.
    #[tokio::test(flavor = "multi_thread")]
    async fn handle_drives_push_flush_stop_and_stop_actually_deregisters() {
        let session = Session::builder().build().expect("session");
        let job_id = session
            .submit_stream_job("handle-lifecycle", spec())
            .expect("submit");
        let job = StreamingJob::from_session_job(session.clone(), &job_id);
        assert_eq!(job.id(), job_id);

        job.push(vec![batch(&["a", "a"], &[1_000, 2_000])])
            .await
            .expect("push");
        let flushed = job.flush().await.expect("flush");
        let rows: usize = flushed.iter().map(RecordBatch::num_rows).sum();
        assert!(rows >= 1, "EOS flush must emit the open window");

        job.stop().await.expect("stop");
        let err = job
            .push(vec![batch(&["b"], &[3_000])])
            .await
            .expect_err("push to a stopped job must fail: its state is gone");
        assert!(
            err.to_string().to_lowercase().contains("not found")
                || err.to_string().to_lowercase().contains("unknown"),
            "the error must say the job no longer exists, got: {err}"
        );
    }

    /// The embedded structured-query backend refuses push/drain/flush BY
    /// NAME instead of half-working.
    #[test]
    fn attach_carries_the_id() {
        let job = StreamingJob::attach("http://127.0.0.1:9999", "remote-j");
        assert_eq!(job.id(), "remote-j");
    }
}
