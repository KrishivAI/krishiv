#![forbid(unsafe_code)]
//! Connector reachability matrix (#197).
//!
//! `~30 connectors built, but reachability diverges` — the driver registry
//! (`crate::registry`) is one thing; whether a given surface actually *dispatches*
//! to a registered driver is another. This module is a machine-readable inventory
//! of which [`ConnectorKind`]-shaped drivers are reachable from each of Krishiv's
//! connector entry points, mirroring `krishiv-sql::grammar`'s SQL feature matrix
//! pattern exactly: a hand-maintained static table, a generated markdown page
//! (never hand-edited), and a CI drift guard (`tests::committed_page_matches_matrix`)
//! that fails if the checked-in page and the table disagree.
//!
//! # Why hand-maintained, not derived from `default_registry()`
//!
//! Most [`ConnectorKind`] variants are `#[cfg(feature = "...")]`-gated, so a table
//! built by introspecting a live `default_registry()` call would silently shrink
//! or grow depending on which features happen to be enabled for a given `cargo
//! test` invocation — the exact "krishiv-sql lint blindspot" failure mode (a
//! feature-gated code path invisible to whichever build produced the "coverage"
//! number) applied to documentation instead of lints. [`CONNECTORS`] uses plain
//! string kind identifiers instead, so the golden-file test is deterministic
//! regardless of `--features`.
//!
//! # The four surfaces
//!
//! - `sql_ddl`: the registry-backed `CREATE SOURCE`/`CREATE SINK … WITH
//!   (connector=…)` grammar (`krishiv-api::pipeline::connector_factory`, Phase 60).
//!   Fully registry-generic for the `Source`/`Sink` roles — reachable iff a driver
//!   is registered for that role. Does **not** check the `TwoPhaseSink`/
//!   `VectorSink` roles (the factory only calls `ConnectorRole::Source`/`Sink`).
//! - `sql_job`: the ad-hoc SQL job source/sink provider
//!   (`krishiv-api::connector_runtime::{ConnectorSourceProvider,
//!   ConnectorSinkProvider}`). A hardcoded allowlist, not registry-generic:
//!   sources = parquet, parquet-directory, csv, json/ndjson, s3, s3-prefix; sinks =
//!   parquet, csv, json/ndjson, s3 (note: **not** s3-prefix for sinks). `json`/
//!   `ndjson` are not a [`ConnectorKind`] at all — a separate bespoke path, not
//!   part of this matrix's rows.
//! - `distributed_job`: sources are registry-generic (`Arc<ConnectorRegistry>`
//!   injected into the executor task runner — any registered `Source` driver is
//!   reachable). Sinks are **not**: `OutputContractDescriptor`
//!   (`krishiv-proto::task`) is a closed 7-variant enum reaching only Parquet
//!   (`ParquetSink`/`ObjectParquetSink`, the latter still Parquet format written
//!   to an object-store path — not a generic S3 sink), Iceberg (`IcebergSink`,
//!   checkpoint-aligned two-phase commit, G7), and Kafka (`KafkaSink`, same
//!   checkpoint-aligned two-phase commit, Phase 55). This source/sink asymmetry
//!   is the core of the #197 finding.
//! - `python_sink`: `krishiv-python::sinks` — six hand-written pyclasses (Parquet,
//!   Kafka, Iceberg, Cassandra, Elasticsearch, HBase), each `write_batches` a
//!   synchronous `block_on(...)` wrapper, not a checkpointed two-phase-commit
//!   participant like the Rust `IcebergSink`/`KafkaSink`. **Python source
//!   reachability is deliberately not a column here**: the declarative pipeline
//!   API's `Ingest` only carries `Memory`/`Cdc` (`krishiv-python::pipeline_api`),
//!   but Python likely has broader read access through the embedded
//!   DataFrame/session API — that surface was not verified to the standard the
//!   rest of this matrix holds itself to, so it is left out rather than guessed.
//!
//! None of the four surfaces reach the `TwoPhaseSink` role (`two-phase-parquet`)
//! or any `VectorSink`-role kind (`memory-vector`/`qdrant`/`pgvector`/`lancedb`/
//! `weaviate`/`pinecone`) — those roles have registered drivers but no dispatch
//! path in this matrix reaches them.

/// Whether a connector kind's driver is reachable from a given surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// Dispatchable from this surface today.
    Yes,
    /// Not wired on this surface — a real, closeable gap.
    No,
    /// The surface doesn't apply to this row's role at all (e.g. `python_sink`
    /// for a `source`-role kind) — a category mismatch, not a gap.
    NotApplicable,
}

impl Reach {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yes => "yes",
            Self::No => "no",
            Self::NotApplicable => "n/a",
        }
    }

    /// A genuine gap that should carry an explanatory note — `No` only,
    /// `NotApplicable` is a category mismatch, not something to explain.
    pub fn is_claimed_gap(self) -> bool {
        matches!(self, Self::No)
    }
}

impl std::fmt::Display for Reach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One `(connector kind, role)` row. `kind`/`role` are the canonical string
/// identifiers `ConnectorKind::as_str()`/the role's config-string form use, kept
/// as plain strings rather than the live enums for the reason in the module doc.
#[derive(Debug, Clone)]
pub struct ConnectorEntry {
    pub kind: &'static str,
    pub role: &'static str,
    /// `ConnectorKind::default_maturity()` for this kind, as of this writing.
    /// No kind defaults to `certified` today — every row is `preview` or
    /// `experimental`, matching Phase 62's certified-combination-matrix work:
    /// certification is combination-specific and has not been assigned here.
    pub maturity: &'static str,
    pub sql_ddl: Reach,
    pub sql_job: Reach,
    pub distributed_job: Reach,
    pub python_sink: Reach,
    pub note: Option<&'static str>,
}

const fn entry(
    kind: &'static str,
    role: &'static str,
    maturity: &'static str,
    sql_ddl: Reach,
    sql_job: Reach,
    distributed_job: Reach,
    python_sink: Reach,
) -> ConnectorEntry {
    ConnectorEntry {
        kind,
        role,
        maturity,
        sql_ddl,
        sql_job,
        distributed_job,
        python_sink,
        note: None,
    }
}

impl ConnectorEntry {
    const fn with_note(mut self, note: &'static str) -> Self {
        self.note = Some(note);
        self
    }
}

use Reach::{No, NotApplicable, Yes};

/// The connector reachability matrix. Ordered by role-group (source, sink,
/// two-phase-sink, vector-sink) then by first appearance of the kind.
///
/// `python_sink` is always `NotApplicable` for `source`-role rows: the column
/// tracks Python *sink* reachability specifically (see the module doc on why
/// Python source reachability isn't a column at all), so a source row can never
/// have a genuine yes/no there — it's a category mismatch, not a gap.
pub static CONNECTORS: &[ConnectorEntry] = &[
    // ── sources ──────────────────────────────────────────────────────────────
    entry("parquet", "source", "preview", Yes, Yes, Yes, NotApplicable),
    entry("parquet-directory", "source", "preview", Yes, Yes, Yes, NotApplicable),
    entry("csv", "source", "preview", Yes, Yes, Yes, NotApplicable),
    entry("avro", "source", "preview", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("s3", "source", "preview", Yes, Yes, Yes, NotApplicable),
    entry("s3-prefix", "source", "preview", Yes, Yes, Yes, NotApplicable),
    entry("kafka", "source", "preview", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("iceberg", "source", "preview", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("delta", "source", "experimental", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("hudi", "source", "experimental", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("kinesis", "source", "preview", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("pulsar", "source", "preview", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    entry("jdbc", "source", "preview", Yes, No, Yes, NotApplicable)
        .with_note("not wired into the ad-hoc SQL job source allowlist"),
    // ── sinks ────────────────────────────────────────────────────────────────
    entry("parquet", "sink", "preview", Yes, Yes, Yes, Yes),
    entry("csv", "sink", "preview", Yes, Yes, No, No)
        .with_note("not an OutputContractDescriptor variant; no Python CSV sink pyclass"),
    entry("avro", "sink", "preview", Yes, No, No, No)
        .with_note("not wired into the ad-hoc SQL job sink allowlist; no OutputContractDescriptor variant"),
    entry("s3", "sink", "preview", Yes, Yes, Yes, No)
        .with_note(
            "distributed reach is ObjectParquetSink: Parquet format written to an \
             object-store path, not a generic S3 sink of arbitrary format",
        ),
    entry("kafka", "sink", "preview", Yes, No, Yes, Yes)
        .with_note(
            "not wired into the ad-hoc SQL job sink allowlist; distributed reach is the \
             checkpoint-aligned two-phase-commit KafkaSink (Phase 55)",
        ),
    entry("iceberg", "sink", "preview", Yes, No, Yes, Yes).with_note(
        "distributed reach is the checkpoint-aligned two-phase-commit IcebergSink (G7)",
    ),
    entry("delta", "sink", "experimental", Yes, No, No, No)
        .with_note("no OutputContractDescriptor variant; no Python pyclass"),
    entry("hudi", "sink", "experimental", Yes, No, No, No)
        .with_note("no OutputContractDescriptor variant; no Python pyclass"),
    entry("elasticsearch", "sink", "preview", Yes, No, No, Yes)
        .with_note("has both a registry driver and a Python pyclass, but no OutputContractDescriptor variant"),
    entry("cassandra", "sink", "preview", Yes, No, No, Yes)
        .with_note("has both a registry driver and a Python pyclass, but no OutputContractDescriptor variant"),
    entry("hbase", "sink", "preview", Yes, No, No, Yes)
        .with_note("has both a registry driver and a Python pyclass, but no OutputContractDescriptor variant"),
    entry("jdbc-sink", "sink", "preview", Yes, No, No, No)
        .with_note("no OutputContractDescriptor variant; no Python pyclass, despite jdbc being source-reachable everywhere else"),
    // ── two-phase-sink ───────────────────────────────────────────────────────
    entry("two-phase-parquet", "two-phase-sink", "preview", No, No, No, No).with_note(
        "registered in default_registry() as a TwoPhaseSink driver, but none of \
         these four surfaces dispatch to the TwoPhaseSink role at all",
    ),
    // ── unregistered (kind exists, zero driver registration) ───────────────────
    entry("kafka-transactional", "sink", "preview", No, No, No, No).with_note(
        "ConnectorKind::KafkaTransactional exists and parses, but has NO driver \
         registered in default_registry() today under any role — dormant/parked, \
         a different gap class from the other rows above (those have a registered \
         driver some surfaces just don't reach; this one is unreachable everywhere \
         because nothing registers it)",
    ),
    // ── vector-sink ──────────────────────────────────────────────────────────
    // None of these four surfaces dispatch to the VectorSink role at all (see
    // "Roles no surface in this matrix reaches" below) — vector writes go
    // through a separate embedding-pipeline path this matrix doesn't cover.
    entry("memory-vector", "vector-sink", "experimental", No, No, No, No)
        .with_note("VectorSink role — see \"Roles no surface in this matrix reaches\" below"),
    entry("qdrant", "vector-sink", "preview", No, No, No, No)
        .with_note("VectorSink role — see \"Roles no surface in this matrix reaches\" below"),
    entry("pgvector", "vector-sink", "preview", No, No, No, No)
        .with_note("VectorSink role — see \"Roles no surface in this matrix reaches\" below"),
    entry("lancedb", "vector-sink", "experimental", No, No, No, No)
        .with_note("VectorSink role — see \"Roles no surface in this matrix reaches\" below"),
    entry("weaviate", "vector-sink", "experimental", No, No, No, No)
        .with_note("VectorSink role — see \"Roles no surface in this matrix reaches\" below"),
    entry("pinecone", "vector-sink", "experimental", No, No, No, No)
        .with_note("VectorSink role — see \"Roles no surface in this matrix reaches\" below"),
];

/// Render the generated connector reachability reference page. Never
/// hand-written — regenerate with `KRISHIV_BLESS_CONNECTOR_DOCS=1 cargo test -p
/// krishiv-connectors reachability`.
pub fn generate_reference_markdown() -> String {
    let mut out = String::new();
    out.push_str("# Krishiv connector reachability matrix\n\n");
    out.push_str(
        "_Generated from `krishiv-connectors/src/reachability.rs` — do not edit by \
         hand._\n\n\
         Which connector-kind drivers are dispatchable from each of Krishiv's four \
         connector entry points: the registry-backed SQL `CREATE SOURCE`/`CREATE \
         SINK` DDL (`sql_ddl`), the ad-hoc SQL job source/sink provider (`sql_job`), \
         distributed batch/streaming jobs (`distributed_job`), and the Python sink \
         surface (`python_sink`). `yes`/`no` states whether that surface dispatches \
         to the kind's driver today, not whether the driver itself works. See the \
         module doc on `reachability.rs` for exactly what each surface checks and \
         why Python source reachability is not a column here.\n\n",
    );

    out.push_str("| Kind | Role | Maturity | sql_ddl | sql_job | distributed_job | python_sink | Notes |\n");
    out.push_str("|---|---|---|---|---|---|---|---|\n");
    for e in CONNECTORS {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} |\n",
            e.kind,
            e.role,
            e.maturity,
            e.sql_ddl,
            e.sql_job,
            e.distributed_job,
            e.python_sink,
            e.note.unwrap_or(""),
        ));
    }
    out.push('\n');

    out.push_str("## Roles no surface in this matrix reaches\n\n");
    out.push_str(
        "`two-phase-sink` (`two-phase-parquet`) and `vector-sink` \
         (`memory-vector`/`qdrant`/`pgvector`/`lancedb`/`weaviate`/`pinecone`) each \
         have registered drivers in `default_registry()`, but none of `sql_ddl` \
         (only checks `Source`/`Sink` roles), `sql_job`, `distributed_job`, or \
         `python_sink` dispatch to either role. Vector writes happen through a \
         separate embedding-pipeline path not covered by this matrix.\n\n",
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_doc(rel: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join(rel)
    }

    #[test]
    fn committed_page_matches_matrix() {
        let path = workspace_doc("docs/reference/connector-reachability-matrix.md");
        let expected = generate_reference_markdown();
        if std::env::var("KRISHIV_BLESS_CONNECTOR_DOCS").is_ok() {
            std::fs::write(&path, &expected).expect("write reachability page");
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(
            committed, expected,
            "docs/reference/connector-reachability-matrix.md is out of date; \
             regenerate with KRISHIV_BLESS_CONNECTOR_DOCS=1 cargo test -p \
             krishiv-connectors reachability"
        );
    }

    #[test]
    fn every_row_has_a_unique_kind_role_pair() {
        let mut seen = std::collections::HashSet::new();
        for e in CONNECTORS {
            assert!(
                seen.insert((e.kind, e.role)),
                "duplicate (kind, role) row: ({}, {})",
                e.kind,
                e.role
            );
        }
    }

    /// A `no` cell with no explanatory note is either an oversight or a genuine
    /// unexplained gap — every negative cell in this matrix should say why.
    /// `NotApplicable` cells are exempt: a category mismatch isn't a gap.
    #[test]
    fn every_row_with_any_no_cell_has_a_note() {
        for e in CONNECTORS {
            let has_gap = [e.sql_ddl, e.sql_job, e.distributed_job, e.python_sink]
                .iter()
                .any(|r| r.is_claimed_gap());
            assert!(
                !has_gap || e.note.is_some(),
                "({}, {}) has at least one `no` cell but no explanatory note",
                e.kind,
                e.role
            );
        }
    }
}
