//! certification.

use crate::error::{ConnectorError, ConnectorResult};
use crate::offset::Offset;
use crate::sink::Sink;
use crate::source::Source;

// ---------------------------------------------------------------------------
// CertificationSuite
// ---------------------------------------------------------------------------

/// Stub test harness for connector certification.
///
/// A real certification suite would drive the connector through its full
/// lifecycle. This stub checks only the capability invariants that every
/// connector must satisfy.
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
        if !caps.has_any() {
            return Err(ConnectorError::Unsupported {
                message: "source must declare at least one capability flag".into(),
            });
        }
        Ok(())
    }

    /// Verify that a sink declares at least one capability.
    pub fn run_sink_capabilities_test(sink: &impl Sink) -> ConnectorResult<()> {
        let caps = sink.capabilities();
        if !caps.has_any() {
            return Err(ConnectorError::Unsupported {
                message: "sink must declare at least one capability flag".into(),
            });
        }
        Ok(())
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
