//! Build concrete connector `Source`/`Sink` from a parsed `ConnectorSpec` by
//! resolving **through the shared connector registry** (`default_registry()`).
//!
//! This binds the SQL `CREATE SOURCE … FROM <CONNECTOR>(…)` / `CREATE SINK …
//! INTO <CONNECTOR>(…)` grammar to `krishiv-connectors`. Every connector kind
//! the build supports — parquet/csv/s3/kafka/iceberg/delta/hudi/jdbc/vector/…,
//! keyed by the same [`ConnectorKind`] the programmatic API uses — is reachable
//! from SQL with **zero parser changes**: the set of kinds, required options,
//! and capability validation all come from the connector descriptors, not the
//! grammar (Flink/RisingWave `WITH (connector=…)` model). A kind the build does
//! not include fails loudly, listing the kinds that *are* available for that
//! role, rather than silently supporting only parquet as the old hardcoded
//! factory did.
//!
//! Two entry points, matching how the session uses them:
//! - [`validate_source_spec`] / [`validate_sink_spec`] — **synchronous, no I/O**
//!   config validation for the `validate_pipeline` dry run (kind is available in
//!   this build + required options present). The old factory validated by
//!   *building* (opening) the connector; for kinds that establish a connection
//!   at open time (kafka/s3/jdbc/iceberg) that is a real side effect and can
//!   fail for reasons unrelated to config validity. Registry `validate_*` runs
//!   the driver's declared validation only — no connection, no file.
//! - [`build_source`] / [`build_sink`] — **async** construction that actually
//!   opens the connector, for `START PIPELINE`.

use std::path::{Component, Path};

use krishiv_connectors::registry::ConnectorRole;
use krishiv_connectors::{ConnectorConfig, ConnectorKind, ConnectorRegistry, DynSink, DynSource};

use krishiv_sql::pipeline_ddl::ConnectorSpec;

use crate::{KrishivError, Result};

fn conn_err(e: impl std::fmt::Display) -> KrishivError {
    KrishivError::Runtime {
        message: e.to_string(),
    }
}

/// Translate the parsed SQL `ConnectorSpec` into a registry [`ConnectorConfig`].
/// `name` is the logical connector name (source/sink identifier) used only for
/// diagnostics and sensitive-field redaction.
fn spec_to_config(name: &str, spec: &ConnectorSpec) -> ConnectorConfig {
    let mut config = ConnectorConfig::new(name, spec.kind.clone());
    for (key, value) in &spec.options {
        config = config.with_property(key.clone(), value.clone());
    }
    config
}

/// The kinds registered for `role` in this build, canonical names, sorted.
///
/// Drives the "supported: …" list in the loud unsupported-kind error, so the
/// message always reflects exactly what the running binary can build.
fn supported_kinds(registry: &ConnectorRegistry, role: ConnectorRole) -> Vec<&'static str> {
    let mut kinds: Vec<&'static str> = registry
        .descriptors()
        .into_iter()
        .filter(|descriptor| descriptor.role == role)
        .map(|descriptor| descriptor.kind.as_str())
        .collect();
    kinds.sort_unstable();
    kinds.dedup();
    kinds
}

/// Fail loudly if the spec's kind is unknown, feature-gated out, or has no
/// driver registered for `role` in this build. The error lists the kinds that
/// *are* available for that role so a caller can discover the surface.
fn ensure_kind_available(
    registry: &ConnectorRegistry,
    spec: &ConnectorSpec,
    role: ConnectorRole,
    role_str: &str,
) -> Result<()> {
    let available = ConnectorKind::parse(&spec.kind)
        .map(|kind| registry.has_driver(kind, role))
        .unwrap_or(false);
    if available {
        return Ok(());
    }
    Err(KrishivError::Runtime {
        message: format!(
            "connector kind '{}' is not available as a SQL pipeline {role_str} in this build; \
             supported {role_str} kinds: {}",
            spec.kind,
            supported_kinds(registry, role).join(", ")
        ),
    })
}

/// Reject file paths that contain `..` components, which could let a SQL caller
/// escape an intended data directory (path/directory traversal). Applies to any
/// connector carrying a `path` option (parquet, parquet-directory, csv, …).
/// Absolute paths without `..` are allowed, matching the prior factory.
fn reject_path_traversal(spec: &ConnectorSpec, role: &str) -> Result<()> {
    if let Some(path) = spec.options.get("path") {
        let escapes = Path::new(path)
            .components()
            .any(|component| component == Component::ParentDir);
        if escapes {
            return Err(KrishivError::Runtime {
                message: format!(
                    "path traversal rejected for {role} path '{path}': \
                     '..' components are not allowed in connector file paths"
                ),
            });
        }
    }
    Ok(())
}

/// Validate a SQL source connector spec without opening it (dry run): the kind
/// is available in this build and its required options are present. No I/O.
pub(crate) fn validate_source_spec(name: &str, spec: &ConnectorSpec) -> Result<()> {
    let registry = krishiv_connectors::default_registry();
    reject_path_traversal(spec, "source")?;
    ensure_kind_available(&registry, spec, ConnectorRole::Source, "source")?;
    registry
        .validate_source(&spec_to_config(name, spec))
        .map_err(conn_err)
}

/// Validate a SQL sink connector spec without opening it (dry run). No I/O — in
/// particular this does not create the sink's output, unlike building it would.
pub(crate) fn validate_sink_spec(name: &str, spec: &ConnectorSpec) -> Result<()> {
    let registry = krishiv_connectors::default_registry();
    reject_path_traversal(spec, "sink")?;
    ensure_kind_available(&registry, spec, ConnectorRole::Sink, "sink")?;
    registry
        .validate_sink(&spec_to_config(name, spec))
        .map_err(conn_err)
}

/// Build a `Box<dyn DynSource>` from a connector spec by opening it through the
/// registry.
pub(crate) async fn build_source(name: &str, spec: &ConnectorSpec) -> Result<Box<dyn DynSource>> {
    let registry = krishiv_connectors::default_registry();
    reject_path_traversal(spec, "source")?;
    ensure_kind_available(&registry, spec, ConnectorRole::Source, "source")?;
    registry
        .open_source(&spec_to_config(name, spec))
        .await
        .map_err(conn_err)
}

/// Build a `Box<dyn DynSink>` from a connector spec by opening it through the
/// registry.
pub(crate) async fn build_sink(name: &str, spec: &ConnectorSpec) -> Result<Box<dyn DynSink>> {
    let registry = krishiv_connectors::default_registry();
    reject_path_traversal(spec, "sink")?;
    ensure_kind_available(&registry, spec, ConnectorRole::Sink, "sink")?;
    registry
        .open_sink(&spec_to_config(name, spec))
        .await
        .map_err(conn_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn spec(kind: &str, opts: &[(&str, &str)]) -> ConnectorSpec {
        ConnectorSpec {
            kind: kind.to_string(),
            options: opts
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        }
    }

    #[test]
    fn unknown_source_kind_fails_loudly_listing_supported() {
        let err = validate_source_spec("s", &spec("does-not-exist", &[]))
            .expect_err("bogus kind must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("does-not-exist"), "names the bad kind: {msg}");
        // The list of supported kinds is registry-derived, not a hardcoded
        // "parquet"; parquet and csv are always in the default build.
        assert!(msg.contains("parquet"), "lists parquet: {msg}");
        assert!(msg.contains("csv"), "lists csv: {msg}");
    }

    #[test]
    fn lakehouse_sink_kind_is_reachable_from_sql() {
        // The whole point of #9: kinds beyond parquet are now SQL-reachable.
        // krishiv-api builds krishiv-connectors with the `lakehouse` feature, so
        // iceberg/delta are registered sink kinds — proving the registry, not a
        // parquet-only factory, backs the SQL surface.
        let registry = krishiv_connectors::default_registry();
        let sink_kinds = supported_kinds(&registry, ConnectorRole::Sink);
        assert!(sink_kinds.contains(&"parquet"), "sinks: {sink_kinds:?}");
        assert!(sink_kinds.contains(&"iceberg"), "sinks: {sink_kinds:?}");
        assert!(sink_kinds.contains(&"delta"), "sinks: {sink_kinds:?}");
    }

    #[test]
    fn parquet_source_missing_required_path_errors() {
        let err = validate_source_spec("orders", &spec("parquet", &[]))
            .expect_err("parquet source requires a path");
        assert!(
            err.to_string().contains("path"),
            "error should name the missing option: {err}"
        );
    }

    #[test]
    fn path_traversal_is_rejected_for_sources_and_sinks() {
        let err = validate_source_spec("s", &spec("parquet", &[("path", "../../etc/passwd")]))
            .expect_err("`..` in a source path must be rejected");
        assert!(err.to_string().contains("path traversal"), "{err}");
        let err = validate_sink_spec("k", &spec("parquet", &[("path", "a/../../b.parquet")]))
            .expect_err("`..` in a sink path must be rejected");
        assert!(err.to_string().contains("path traversal"), "{err}");
    }

    #[test]
    fn validate_does_not_open_the_sink() {
        // Dry-run validation must be side-effect free: it runs the driver's
        // declared validation only, never opening the connector (no file, no
        // connection). A parquet sink writes its output lazily on first write,
        // so a validated-but-not-run sink leaves nothing on disk.
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("must-not-exist.parquet");
        validate_sink_spec("out", &spec("parquet", &[("path", out.to_str().unwrap())]))
            .expect("valid parquet sink spec");
        assert!(
            !out.exists(),
            "validation must not open/create the sink output"
        );
    }

    #[tokio::test]
    async fn build_source_and_sink_round_trip_through_registry() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::fs::File;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let in_path = dir.path().join("in.parquet");
        let out_path = dir.path().join("out.parquet");

        // Seed an input parquet file: v = [1, 2, 3].
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _],
            )
            .unwrap();
            let file = File::create(&in_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        // Build a source and a sink purely through the SQL factory → registry.
        let src_spec = spec("parquet", &[("path", in_path.to_str().unwrap())]);
        let sink_spec = spec("parquet", &[("path", out_path.to_str().unwrap())]);
        let mut source: Box<dyn DynSource> = build_source("in", &src_spec).await.unwrap();
        let mut sink: Box<dyn DynSink> = build_sink("out", &sink_spec).await.unwrap();

        // Copy every batch source → sink and flush to finalise the parquet file.
        while let Some(batch) = source.read_batch_dyn().await.unwrap() {
            sink.write_batch_dyn(batch).await.unwrap();
        }
        sink.flush_dyn().await.unwrap();

        // Read the output back through a freshly built registry source: the full
        // three rows must survive the SQL-factory round trip.
        let mut verify: Box<dyn DynSource> = build_source(
            "verify",
            &spec("parquet", &[("path", out_path.to_str().unwrap())]),
        )
        .await
        .unwrap();
        let mut rows = 0;
        while let Some(batch) = verify.read_batch_dyn().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3, "all rows round-tripped through the registry");
    }
}
