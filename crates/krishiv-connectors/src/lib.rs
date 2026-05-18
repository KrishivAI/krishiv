#![forbid(unsafe_code)]

//! Connector contracts for Krishiv.
//!
//! This crate defines the `Source` and `Sink` traits, connector capability
//! flags, offset persistence boundary, commit semantics, configuration
//! validation, and a certification test harness stub.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;

pub mod kafka;
pub mod parquet;
pub mod s3;

// ---------------------------------------------------------------------------
// Error and Result
// ---------------------------------------------------------------------------

/// Errors produced by connector operations.
#[derive(Debug)]
pub enum ConnectorError {
    /// Configuration problem (missing required property, bad value, etc.).
    Config { message: String },
    /// I/O error reading from or writing to a source/sink.
    Io { message: String },
    /// Schema mismatch or incompatible field types.
    Schema { message: String },
    /// Operation is not supported by this connector.
    Unsupported { message: String },
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorError::Config { message } => write!(f, "connector config error: {message}"),
            ConnectorError::Io { message } => write!(f, "connector I/O error: {message}"),
            ConnectorError::Schema { message } => write!(f, "connector schema error: {message}"),
            ConnectorError::Unsupported { message } => {
                write!(f, "connector unsupported: {message}")
            }
        }
    }
}

impl std::error::Error for ConnectorError {}

/// Convenience result alias for connector operations.
pub type ConnectorResult<T> = Result<T, ConnectorError>;

// ---------------------------------------------------------------------------
// ConnectorCapabilities
// ---------------------------------------------------------------------------

/// Describes what guarantees and modes a connector supports.
///
/// All flags default to `false`. Use the builder methods to opt-in to
/// capabilities the connector actually provides.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectorCapabilities {
    bounded: bool,
    unbounded: bool,
    rewindable: bool,
    transactional: bool,
    idempotent: bool,
}

impl ConnectorCapabilities {
    /// Create a new capabilities instance with all flags disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the connector as producing a bounded (finite) data stream.
    #[must_use]
    pub fn with_bounded(mut self) -> Self {
        self.bounded = true;
        self
    }

    /// Mark the connector as producing an unbounded (infinite) data stream.
    #[must_use]
    pub fn with_unbounded(mut self) -> Self {
        self.unbounded = true;
        self
    }

    /// Mark the connector as supporting rewind to a previous offset.
    #[must_use]
    pub fn with_rewindable(mut self) -> Self {
        self.rewindable = true;
        self
    }

    /// Mark the connector as supporting transactional commits.
    #[must_use]
    pub fn with_transactional(mut self) -> Self {
        self.transactional = true;
        self
    }

    /// Mark the connector as supporting idempotent writes.
    #[must_use]
    pub fn with_idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }

    /// Returns `true` if the data stream is bounded (finite).
    pub fn is_bounded(&self) -> bool {
        self.bounded
    }

    /// Returns `true` if the data stream is unbounded (infinite).
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
    }

    /// Returns `true` if the connector supports rewind to a previous offset.
    pub fn is_rewindable(&self) -> bool {
        self.rewindable
    }

    /// Returns `true` if the connector supports transactional commits.
    pub fn is_transactional(&self) -> bool {
        self.transactional
    }

    /// Returns `true` if writes are idempotent (safe to replay).
    pub fn is_idempotent(&self) -> bool {
        self.idempotent
    }

    /// Returns `true` if at least one capability flag is set.
    pub fn has_any(&self) -> bool {
        self.bounded || self.unbounded || self.rewindable || self.transactional || self.idempotent
    }
}

// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

/// Serialisable cursor into a connector's data stream.
///
/// Implementors must be able to round-trip through `encode`/`decode` without
/// loss of information.
pub trait Offset {
    /// Serialise this offset to a byte vector.
    fn encode(&self) -> Vec<u8>;

    /// Deserialise an offset from a byte slice.
    fn decode(bytes: &[u8]) -> ConnectorResult<Self>
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// CommitHandle
// ---------------------------------------------------------------------------

/// Handle returned by a sink after a batch is buffered, allowing the caller
/// to drive the two-phase commit boundary.
pub trait CommitHandle {
    /// Durably commit the buffered output.
    fn commit(&self) -> impl Future<Output = ConnectorResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// An async, pull-based data source that emits Arrow [`RecordBatch`] values.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub trait Source {
    /// Return the capabilities this source supports.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Pull the next batch from the source.
    ///
    /// Returns `Ok(None)` when the source is exhausted (bounded sources only).
    fn read_batch(
        &mut self,
    ) -> impl Future<Output = ConnectorResult<Option<arrow::record_batch::RecordBatch>>> + Send;

    /// Return the current read offset, if available.
    ///
    /// The returned value is connector-specific and should be downcast by the
    /// caller if it needs the concrete offset type.
    fn current_offset(&self) -> Option<Box<dyn Any + Send>>;
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// An async, push-based data sink that accepts Arrow [`RecordBatch`] values.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub trait Sink {
    /// Return the capabilities this sink supports.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Write a single batch to the sink.
    fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> impl Future<Output = ConnectorResult<()>> + Send;

    /// Flush any buffered data and close the sink.
    fn flush(&mut self) -> impl Future<Output = ConnectorResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// ConnectorConfig
// ---------------------------------------------------------------------------

/// Key/value configuration bag for connector instantiation.
///
/// Properties are stored in a sorted map to make serialisation deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorConfig {
    /// Logical name for this connector instance.
    pub name: String,
    /// Connector kind identifier (e.g., `"parquet"`, `"kafka"`, `"s3"`).
    pub kind: String,
    properties: BTreeMap<String, String>,
}

impl ConnectorConfig {
    /// Create a new config with the given name and kind, and no properties.
    pub fn new(name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            properties: BTreeMap::new(),
        }
    }

    /// Add a property and return the updated config (builder style).
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Look up a property by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    /// Look up a required property, returning a [`ConnectorError::Config`] if
    /// it is absent.
    pub fn required(&self, key: &str) -> ConnectorResult<&str> {
        self.get(key).ok_or_else(|| ConnectorError::Config {
            message: format!(
                "required property '{key}' is missing from connector '{}'",
                self.name
            ),
        })
    }
}

// ---------------------------------------------------------------------------
// ParquetOffset
// ---------------------------------------------------------------------------

/// Cursor into a Parquet-backed source: index of the next batch to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetOffset {
    pub batch_index: usize,
}

impl Offset for ParquetOffset {
    fn encode(&self) -> Vec<u8> {
        (self.batch_index as u64).to_le_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() < 8 {
            return Err(ConnectorError::Io {
                message: "ParquetOffset decode: expected 8 bytes".into(),
            });
        }
        let n = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        Ok(Self {
            batch_index: n as usize,
        })
    }
}

// ---------------------------------------------------------------------------
// AtLeastOnceSinkContract
// ---------------------------------------------------------------------------

/// Documents the at-least-once sink delivery contract.
///
/// A sink operating under at-least-once semantics guarantees:
/// - Every input batch is written to the downstream store at least once.
/// - On executor reassignment or crash-recovery, batches that were written
///   but not yet acknowledged may be replayed. Idempotent sinks (`is_idempotent()`)
///   handle replays safely. Non-idempotent sinks may produce duplicates.
/// - `flush()` must be called and awaited before an offset is committed to
///   the source. Committing the source offset before `flush()` completes risks
///   data loss on crash.
pub struct AtLeastOnceSinkContract;

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

    /// Encode then decode `offset` and assert the round-trip produces an equal value.
    ///
    /// `O` must implement both `Offset` and `PartialEq + std::fmt::Debug` so the
    /// assertion can produce a useful failure message.
    pub fn run_offset_round_trip_test<O>(offset: O) -> ConnectorResult<()>
    where
        O: Offset + PartialEq + std::fmt::Debug,
    {
        let encoded = offset.encode();
        let decoded = O::decode(&encoded)?;
        assert_eq!(
            offset, decoded,
            "offset round-trip failed: encoded then decoded value differs from original"
        );
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

// ---------------------------------------------------------------------------
// Re-export std::future::Future so trait impls compile without extra imports
// ---------------------------------------------------------------------------

use std::future::Future;

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // ConnectorCapabilities builder
    // -----------------------------------------------------------------------

    #[test]
    fn connector_capabilities_builder_sets_flags() {
        let caps = ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_idempotent();

        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
        assert!(caps.is_idempotent());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
        assert!(caps.has_any());
    }

    #[test]
    fn connector_capabilities_default_all_false() {
        let caps = ConnectorCapabilities::new();
        assert!(!caps.has_any());
    }

    // -----------------------------------------------------------------------
    // ConnectorConfig
    // -----------------------------------------------------------------------

    #[test]
    fn connector_config_required_returns_error_when_missing() {
        let config = ConnectorConfig::new("my-source", "parquet");
        let err = config.required("path").unwrap_err();
        match err {
            ConnectorError::Config { message } => {
                assert!(message.contains("path"), "expected 'path' in: {message}");
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn connector_config_required_returns_value_when_present() {
        let config = ConnectorConfig::new("my-source", "parquet")
            .with_property("path", "/data/file.parquet");
        let value = config.required("path").unwrap();
        assert_eq!(value, "/data/file.parquet");
    }

    // -----------------------------------------------------------------------
    // CertificationSuite: source with no capabilities
    // -----------------------------------------------------------------------

    struct NullSource;

    impl Source for NullSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new() // all false
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    #[test]
    fn certification_suite_rejects_source_with_no_capabilities() {
        let source = NullSource;
        let result = CertificationSuite::run_source_capabilities_test(&source);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // CertificationSuite: bounded exhaustion test
    // -----------------------------------------------------------------------

    struct ThreeBatchSource {
        count: usize,
    }

    impl Source for ThreeBatchSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_bounded()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            if self.count < 3 {
                self.count += 1;
                Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                    std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                )))
            } else {
                Ok(None)
            }
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    struct UnboundedSource;

    impl Source for UnboundedSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_unbounded()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    #[tokio::test]
    async fn certification_exhaustion_test_passes_for_bounded_source() {
        let mut source = ThreeBatchSource { count: 0 };
        let result = CertificationSuite::run_bounded_exhaustion_test(&mut source).await;
        assert!(result.is_ok(), "bounded source exhaustion test should pass");
    }

    // -----------------------------------------------------------------------
    // ParquetOffset + AtLeastOnceSinkContract + CertificationSuite offset test
    // -----------------------------------------------------------------------

    #[test]
    fn parquet_offset_encode_decode_roundtrip() {
        let original = ParquetOffset { batch_index: 42 };
        let encoded = original.encode();
        let decoded = ParquetOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn at_least_once_contract_exists() {
        let _ = AtLeastOnceSinkContract;
    }

    #[test]
    fn certification_offset_round_trip_passes_for_parquet_offset() {
        let offset = ParquetOffset { batch_index: 7 };
        CertificationSuite::run_offset_round_trip_test(offset).unwrap();
    }

    #[tokio::test]
    async fn certification_exhaustion_test_rejects_unbounded_source() {
        let mut source = UnboundedSource;
        let err = CertificationSuite::run_bounded_exhaustion_test(&mut source)
            .await
            .unwrap_err();
        match err {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }
}
