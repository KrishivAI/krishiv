//! The compiled job artifact every front-end produces.
//!
//! SQL, Python, and Rust front-ends all lower to a [`CompiledJob`]. The
//! dispatcher then routes it by [`EngineKind`] to a [`ComputeEngine`](crate::ComputeEngine)
//! and by placement to an [`EngineRuntime`](crate::EngineRuntime). One artifact,
//! one dispatch point — no per-API forks.

use serde::{Deserialize, Serialize};

use crate::kind::EngineKind;

/// A source of input batches for a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSpec {
    /// Logical name referenced by the query (the table/relation name).
    pub name: String,
    /// Connector kind (e.g. `"memory"`, `"parquet"`, `"kafka"`, `"iceberg"`).
    pub connector: String,
    /// Connector locator (URI, path, topic, …). Empty for in-memory sources.
    pub uri: String,
    /// Whether the source is bounded (ends) or unbounded (runs forever).
    pub is_bounded: bool,
    /// Whether the source carries change events (CDC/changelog) rather than
    /// plain append records.
    pub is_cdc: bool,
}

impl SourceSpec {
    /// A bounded, append-only source — the batch default.
    pub fn bounded(
        name: impl Into<String>,
        connector: impl Into<String>,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            connector: connector.into(),
            uri: uri.into(),
            is_bounded: true,
            is_cdc: false,
        }
    }

    /// An unbounded, append-only source — drives the streaming engine.
    pub fn unbounded(
        name: impl Into<String>,
        connector: impl Into<String>,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            connector: connector.into(),
            uri: uri.into(),
            is_bounded: false,
            is_cdc: false,
        }
    }

    /// A change-data-capture source — drives the incremental engine.
    pub fn cdc(
        name: impl Into<String>,
        connector: impl Into<String>,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            connector: connector.into(),
            uri: uri.into(),
            is_bounded: false,
            is_cdc: true,
        }
    }
}

/// A sink that consumes one output relation/view of a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkSpec {
    /// The output relation/view this sink consumes.
    pub view: String,
    /// Connector kind (e.g. `"memory"`, `"parquet"`, `"kafka"`, `"iceberg"`, `"jdbc"`).
    pub connector: String,
    /// Connector locator.
    pub uri: String,
    /// Primary-key columns for **upsert** delivery. When set, a stateful engine's
    /// changelog is applied by key — an insert/update writes (replaces) the keyed
    /// row, a delete removes it — so per-row upserts land without carrying the
    /// prior row image. `None` ⇒ append/whole-row-consolidated output.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
}

impl SinkSpec {
    /// Build a sink spec for `view` written through `connector` at `uri`.
    pub fn new(
        view: impl Into<String>,
        connector: impl Into<String>,
        uri: impl Into<String>,
    ) -> Self {
        Self {
            view: view.into(),
            connector: connector.into(),
            uri: uri.into(),
            primary_key: None,
        }
    }

    /// Declare the sink an **upsert** target keyed on `columns` (by name).
    pub fn with_primary_key<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.primary_key = Some(columns.into_iter().map(Into::into).collect());
        self
    }
}

/// The delivery guarantee requested for a job.
///
/// Exactly-once is `certified` only for source/sink/checkpoint combinations
/// that have passed failure certification (see the engine-semantics delivery
/// matrix). Otherwise it is a preview claim and must be labelled as such.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DeliveryContract {
    /// Records may be lost, never duplicated.
    AtMostOnce,
    /// Records are never lost, may be duplicated.
    #[default]
    AtLeastOnce,
    /// Each record affects state exactly once.
    ExactlyOnce {
        /// Whether the source/sink/checkpoint combo is certified for this
        /// claim, rather than a preview.
        certified: bool,
    },
}

/// The state a job maintains, implied by its engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatePolicy {
    /// No state — batch.
    Stateless,
    /// Keyed operator state — streaming windows/joins.
    Keyed,
    /// A maintained materialized view — incremental.
    MaterializedView,
}

impl StatePolicy {
    /// The state policy each engine implies.
    pub fn for_engine(engine: EngineKind) -> Self {
        match engine {
            EngineKind::Batch => Self::Stateless,
            EngineKind::Incremental => Self::MaterializedView,
            EngineKind::Streaming => Self::Keyed,
        }
    }
}

/// A fully compiled job — the single artifact produced by every front-end.
///
/// For Phase 0 the plan is carried as SQL text (all three engines accept SQL);
/// a later phase replaces `query` with a shared logical plan so planning and
/// optimization are unified too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledJob {
    /// Stable job name — this **becomes the job id** at submit time and is the
    /// job's durable identity. For a stateful engine, reusing a name that has a
    /// persisted checkpoint resumes that state (the restart-resume path), so a
    /// name must be unique per logical job.
    pub name: String,
    /// Which engine runs this job.
    pub engine: EngineKind,
    /// The query that defines the job.
    pub query: String,
    /// Input sources.
    pub sources: Vec<SourceSpec>,
    /// Output sinks.
    pub sinks: Vec<SinkSpec>,
    /// Delivery guarantee requested for this job.
    pub delivery: DeliveryContract,
    /// State policy implied by the engine.
    pub state: StatePolicy,
}

impl CompiledJob {
    /// Build a job, inferring the engine and state policy from the sources and
    /// whether the query declares an event-time window.
    ///
    /// Use [`with_engine`](Self::with_engine) to override the inferred engine
    /// (an explicit user choice always wins).
    pub fn new(
        name: impl Into<String>,
        query: impl Into<String>,
        sources: Vec<SourceSpec>,
        sinks: Vec<SinkSpec>,
        event_time_window: bool,
    ) -> Self {
        let engine = EngineKind::infer(&sources, event_time_window);
        Self {
            name: name.into(),
            engine,
            query: query.into(),
            sources,
            sinks,
            delivery: DeliveryContract::default(),
            state: StatePolicy::for_engine(engine),
        }
    }

    /// Override the engine, re-deriving the implied state policy.
    #[must_use]
    pub fn with_engine(mut self, engine: EngineKind) -> Self {
        self.engine = engine;
        self.state = StatePolicy::for_engine(engine);
        self
    }

    /// Set the requested delivery guarantee.
    #[must_use]
    pub fn with_delivery(mut self, delivery: DeliveryContract) -> Self {
        self.delivery = delivery;
        self
    }

    /// Structural validation independent of any engine: the job must be named,
    /// carry a query, and have at least one source and one sink. Engine-specific
    /// checks live in [`ComputeEngine::validate`](crate::ComputeEngine::validate).
    pub fn validate_shape(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("job name is empty".to_string());
        }
        if self.query.trim().is_empty() {
            return Err("job query is empty".to_string());
        }
        if self.sources.is_empty() {
            return Err("job has no sources".to_string());
        }
        // A job with no sink would compute and silently discard its output. A
        // CompiledJob is a source→transform→sink unit; require an output.
        if self.sinks.is_empty() {
            return Err("job has no sinks".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_infers_engine_and_state() {
        let job = CompiledJob::new(
            "j",
            "SELECT 1",
            vec![SourceSpec::bounded("t", "memory", "")],
            vec![],
            false,
        );
        assert_eq!(job.engine, EngineKind::Batch);
        assert_eq!(job.state, StatePolicy::Stateless);
    }

    #[test]
    fn with_engine_redirects_state_policy() {
        let job = CompiledJob::new(
            "j",
            "SELECT 1",
            vec![SourceSpec::bounded("t", "memory", "")],
            vec![],
            false,
        )
        .with_engine(EngineKind::Streaming);
        assert_eq!(job.engine, EngineKind::Streaming);
        assert_eq!(job.state, StatePolicy::Keyed);
    }

    #[test]
    fn cdc_source_infers_incremental_materialized_view() {
        let job = CompiledJob::new(
            "j",
            "SELECT * FROM t",
            vec![SourceSpec::cdc("t", "debezium", "topic")],
            vec![],
            false,
        );
        assert_eq!(job.engine, EngineKind::Incremental);
        assert_eq!(job.state, StatePolicy::MaterializedView);
    }

    #[test]
    fn default_delivery_is_at_least_once() {
        assert_eq!(DeliveryContract::default(), DeliveryContract::AtLeastOnce);
    }

    #[test]
    fn validate_shape_rejects_empty() {
        let bad = CompiledJob::new("", "SELECT 1", vec![], vec![], false);
        assert!(bad.validate_shape().is_err());
    }

    #[test]
    fn validate_shape_rejects_no_sinks() {
        let job = CompiledJob::new(
            "j",
            "SELECT 1",
            vec![SourceSpec::bounded("t", "memory", "")],
            vec![],
            false,
        );
        assert!(
            job.validate_shape().is_err(),
            "a job with no sink must be rejected, not silently discard output"
        );
    }
}
