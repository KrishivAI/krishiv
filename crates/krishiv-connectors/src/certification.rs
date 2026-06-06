//! certification.

use crate::error::{ConnectorError, ConnectorResult};
use crate::offset::Offset;
use crate::sink::Sink;
use crate::source::{CheckpointSource, Source};
use crate::two_phase::TwoPhaseCommitSink;

// ---------------------------------------------------------------------------
// CertificationSuite
// ---------------------------------------------------------------------------

/// Reusable connector lifecycle certification harness.
pub struct CertificationSuite {
    /// Human-readable name of the suite being run.
    pub name: String,
}

impl CertificationSuite {
    /// Create a new certification suite with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Verify that a source declares at least one capability.
    pub fn run_source_capabilities_test(source: &impl Source) -> ConnectorResult<()> {
        let caps = source.capabilities();
        caps.validate()?;
        if !caps.has_any() {
            return Err(ConnectorError::Unsupported {
                message: "source must declare at least one capability flag".into(),
            });
        }
        if caps.is_bounded() == caps.is_unbounded() {
            return Err(ConnectorError::CertificationFailed {
                reason: "source must declare exactly one of bounded or unbounded".into(),
            });
        }
        if caps.is_rewindable() && source.current_offset().is_none() {
            return Err(ConnectorError::CertificationFailed {
                reason: "rewindable source must expose a current offset".into(),
            });
        }
        if caps.is_checkpoint_capable() && source.current_offset().is_none() {
            return Err(ConnectorError::CertificationFailed {
                reason: "checkpoint-capable source must expose a current offset".into(),
            });
        }
        Ok(())
    }

    /// Verify that a sink declares at least one capability.
    pub fn run_sink_capabilities_test(sink: &impl Sink) -> ConnectorResult<()> {
        let caps = sink.capabilities();
        caps.validate()?;
        if !caps.has_any() {
            return Err(ConnectorError::Unsupported {
                message: "sink must declare at least one capability flag".into(),
            });
        }
        Ok(())
    }

    /// Verify that a two-phase sink declares the complete checkpointed
    /// transactional capability set required by its protocol.
    pub fn run_two_phase_commit_capabilities_test(
        sink: &impl TwoPhaseCommitSink,
    ) -> ConnectorResult<()> {
        let caps = sink.capabilities();
        caps.validate()?;
        if !caps.is_two_phase_commit_capable()
            || !caps.is_transactional()
            || !caps.is_checkpoint_capable()
        {
            return Err(ConnectorError::CertificationFailed {
                reason: "two-phase sink must declare two-phase commit, transactional, and \
                         checkpoint capabilities"
                    .into(),
            });
        }
        Ok(())
    }

    /// Verify the capability boundary for an exactly-once source/sink pair.
    ///
    /// This does not certify a coordinator implementation by itself; it proves
    /// that the selected source has typed checkpoint restore semantics and the
    /// selected sink has checkpoint-coupled two-phase commit semantics.
    pub fn run_exactly_once_capabilities_test(
        source: &impl CheckpointSource,
        sink: &impl TwoPhaseCommitSink,
    ) -> ConnectorResult<()> {
        Self::run_source_capabilities_test(source)?;
        if !source.capabilities().is_checkpoint_capable() {
            return Err(ConnectorError::CertificationFailed {
                reason: "exactly-once source must declare checkpoint capability".into(),
            });
        }
        Self::run_two_phase_commit_capabilities_test(sink)
    }

    /// Exercise prepare/abort and prepare/commit, including coordinator-style
    /// retries of the final decision.
    pub fn run_two_phase_commit_lifecycle_test(
        sink: &mut impl TwoPhaseCommitSink,
        epoch: u64,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<()> {
        Self::run_two_phase_commit_capabilities_test(sink)?;

        let aborted = sink.prepare(epoch, batch)?;
        sink.abort(aborted.clone())?;
        sink.abort(aborted)?;

        let committed = sink.prepare(epoch, batch)?;
        sink.commit(committed.clone())?;
        sink.commit(committed)?;
        Ok(())
    }

    /// Verify the complete rewind lifecycle for a source with typed offsets.
    ///
    /// The source must:
    /// - advertise `rewindable`,
    /// - expose an offset of type `O`,
    /// - advance its offset after one batch,
    /// - restore the initial offset through [`Source::reset`],
    /// - replay a batch with the same shape,
    /// - reach the same post-read offset after replay.
    pub async fn run_rewind_test<O>(source: &mut impl Source) -> ConnectorResult<()>
    where
        O: PartialEq + std::fmt::Debug + Send + 'static,
    {
        Self::run_source_capabilities_test(source)?;
        if !source.capabilities().is_rewindable() {
            return Err(ConnectorError::Unsupported {
                message: "rewind test requires a rewindable source".into(),
            });
        }

        let initial_offset = Self::typed_source_offset::<O>(source, "before first read")?;
        let first_batch =
            source
                .read_batch()
                .await?
                .ok_or_else(|| ConnectorError::CertificationFailed {
                    reason: "rewindable source produced no batch to replay".into(),
                })?;
        let advanced_offset = Self::typed_source_offset::<O>(source, "after first read")?;
        if advanced_offset == initial_offset {
            return Err(ConnectorError::CertificationFailed {
                reason: format!("source offset did not advance after read: {advanced_offset:?}"),
            });
        }

        Source::reset(source);
        let reset_offset = Self::typed_source_offset::<O>(source, "after reset")?;
        if reset_offset != initial_offset {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "source reset did not restore initial offset: initial={initial_offset:?}, \
                     reset={reset_offset:?}"
                ),
            });
        }

        let replay_batch =
            source
                .read_batch()
                .await?
                .ok_or_else(|| ConnectorError::CertificationFailed {
                    reason: "source produced no batch after reset".into(),
                })?;
        if replay_batch.num_rows() != first_batch.num_rows()
            || replay_batch.num_columns() != first_batch.num_columns()
            || replay_batch.schema().as_ref() != first_batch.schema().as_ref()
        {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "replayed batch shape differs: first={}x{}, replay={}x{}",
                    first_batch.num_rows(),
                    first_batch.num_columns(),
                    replay_batch.num_rows(),
                    replay_batch.num_columns()
                ),
            });
        }

        let replay_offset = Self::typed_source_offset::<O>(source, "after replay")?;
        if replay_offset != advanced_offset {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "replay reached a different offset: first={advanced_offset:?}, \
                     replay={replay_offset:?}"
                ),
            });
        }

        Ok(())
    }

    /// Verify exact typed source checkpoint capture, encoding, and restoration.
    ///
    /// The lifecycle restores both the initial position and the position after
    /// one batch, then compares replayed Arrow data and resulting offsets.
    pub async fn run_checkpoint_restore_test<S>(source: &mut S) -> ConnectorResult<()>
    where
        S: CheckpointSource,
    {
        Self::run_source_capabilities_test(source)?;
        if !source.capabilities().is_checkpoint_capable() {
            return Err(ConnectorError::Unsupported {
                message: "checkpoint restore test requires checkpoint capability".into(),
            });
        }

        let initial = source.checkpoint_offset()?;
        let initial_encoded = source.encoded_checkpoint_offset()?;
        let decoded_initial = S::Offset::decode(&initial_encoded)?;
        if decoded_initial != initial {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "encoded initial offset changed after decode: initial={initial:?}, \
                     decoded={decoded_initial:?}"
                ),
            });
        }

        let first =
            source
                .read_batch()
                .await?
                .ok_or_else(|| ConnectorError::CertificationFailed {
                    reason: "checkpoint-capable source produced no batch to restore".into(),
                })?;
        let after_first = source.checkpoint_offset()?;
        if after_first == initial {
            return Err(ConnectorError::CertificationFailed {
                reason: format!("source offset did not advance after first batch: {after_first:?}"),
            });
        }
        let second = source.read_batch().await?;
        let after_second = source.checkpoint_offset()?;

        source.restore_encoded_offset(&initial_encoded)?;
        let restored_initial = source.checkpoint_offset()?;
        if restored_initial != initial {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "encoded restore did not recover initial offset: expected={initial:?}, \
                     actual={restored_initial:?}"
                ),
            });
        }
        let replay_first =
            source
                .read_batch()
                .await?
                .ok_or_else(|| ConnectorError::CertificationFailed {
                    reason: "source produced no first batch after checkpoint restore".into(),
                })?;
        if replay_first != first {
            return Err(ConnectorError::CertificationFailed {
                reason: "first batch changed after checkpoint restore".into(),
            });
        }
        if source.checkpoint_offset()? != after_first {
            return Err(ConnectorError::CertificationFailed {
                reason: "source reached a different offset after replaying first batch".into(),
            });
        }

        source.restore_offset(&after_first)?;
        if source.checkpoint_offset()? != after_first {
            return Err(ConnectorError::CertificationFailed {
                reason: "typed restore did not recover the intermediate offset".into(),
            });
        }
        let replay_second = source.read_batch().await?;
        if replay_second != second {
            return Err(ConnectorError::CertificationFailed {
                reason: "next batch changed after restoring the intermediate offset".into(),
            });
        }
        if source.checkpoint_offset()? != after_second {
            return Err(ConnectorError::CertificationFailed {
                reason: "source reached a different offset after intermediate replay".into(),
            });
        }

        Ok(())
    }

    fn typed_source_offset<O>(source: &impl Source, phase: &str) -> ConnectorResult<O>
    where
        O: Send + 'static,
    {
        let offset =
            source
                .current_offset()
                .ok_or_else(|| ConnectorError::CertificationFailed {
                    reason: format!("source did not expose an offset {phase}"),
                })?;
        offset.downcast::<O>().map(|value| *value).map_err(|_| {
            ConnectorError::CertificationFailed {
                reason: format!(
                    "source offset {phase} did not have expected type {}",
                    std::any::type_name::<O>()
                ),
            }
        })
    }

    /// Drain a bounded source and verify it returns `None` on exhaustion.
    ///
    /// Returns [`ConnectorError::Unsupported`] if the source is not bounded.
    /// Returns [`ConnectorError::Unsupported`] if the source does not exhaust
    /// within 100,000 batches (guards against infinite sources that
    /// misreport bounded capability).
    pub async fn run_bounded_exhaustion_test(source: &mut impl Source) -> ConnectorResult<()> {
        if !source.capabilities().is_bounded() {
            return Err(ConnectorError::Unsupported {
                message: "exhaustion test requires a bounded source".into(),
            });
        }
        let mut count = 0usize;
        loop {
            match source.read_batch().await? {
                Some(_) => count += 1,
                None => break,
            }
            if count > 100_000 {
                return Err(ConnectorError::Unsupported {
                    message: "source did not exhaust after 100_000 batches".into(),
                });
            }
        }
        Ok(())
    }

    /// Encode then decode `offset` and verify the round-trip produces an equal value.
    ///
    /// `O` must implement both `Offset` and `PartialEq + std::fmt::Debug` so the
    /// failure message can produce a useful description.
    pub fn run_offset_round_trip_test<O>(offset: O) -> ConnectorResult<()>
    where
        O: Offset + PartialEq + std::fmt::Debug,
    {
        let encoded = offset.encode();
        let decoded = O::decode(&encoded)?;
        if offset != decoded {
            return Err(ConnectorError::CertificationFailed {
                reason: format!(
                    "offset round-trip failed: original={offset:?}, decoded={decoded:?}"
                ),
            });
        }
        Ok(())
    }

    /// Write `batches` to `sink`, flush it, and verify the sink declared
    /// idempotent.
    ///
    /// Returns [`ConnectorError::Unsupported`] if the sink is not idempotent.
    pub async fn run_idempotent_sink_test(
        sink: &mut impl Sink,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> ConnectorResult<()> {
        if !sink.capabilities().is_idempotent() {
            return Err(ConnectorError::Unsupported {
                message: "idempotent sink test requires idempotent capability".into(),
            });
        }
        for batch in batches {
            sink.write_batch(batch.clone()).await?;
        }
        sink.flush().await?;
        Ok(())
    }
}
