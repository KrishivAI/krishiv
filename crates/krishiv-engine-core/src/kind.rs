//! Engine selection: which compute model runs a job.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::job::SourceSpec;

/// Which of the three compute engines runs a job.
///
/// Chosen explicitly by the user, or inferred once via [`EngineKind::infer`].
/// The choice is **never** implied by the API surface (SQL/Python/Rust) or by
/// the deployment placement — that conflation is exactly the bug this spine
/// removes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EngineKind {
    /// Bounded input, run-to-completion (Spark-style batch SQL).
    Batch,
    /// Change-driven incremental view maintenance (Feldera/DBSP-style).
    Incremental,
    /// Event-time, watermark-driven streaming with keyed state and
    /// checkpoints (Flink-style).
    Streaming,
}

impl EngineKind {
    /// Infer the engine from the job's sources and whether it declares an
    /// event-time window.
    ///
    /// Rules, in priority order:
    /// 1. any CDC/changelog source => [`EngineKind::Incremental`]
    /// 2. an event-time window, or any unbounded source => [`EngineKind::Streaming`]
    /// 3. otherwise (all bounded, no window) => [`EngineKind::Batch`]
    ///
    /// An explicit user choice always takes precedence; callers use this only
    /// as the fallback when no engine was named. This is the **single**
    /// inference site every front-end shares.
    pub fn infer(sources: &[SourceSpec], event_time_window: bool) -> Self {
        if sources.iter().any(|s| s.is_cdc) {
            return Self::Incremental;
        }
        if event_time_window || sources.iter().any(|s| !s.is_bounded) {
            return Self::Streaming;
        }
        Self::Batch
    }

    /// Stable lowercase token used in SQL hints, config, and the wire format.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Incremental => "incremental",
            Self::Streaming => "streaming",
        }
    }

    /// Whether this engine runs indefinitely (vs. run-to-completion).
    pub fn is_continuous(self) -> bool {
        matches!(self, Self::Incremental | Self::Streaming)
    }

    /// Whether this engine maintains durable keyed/materialized state that
    /// must be checkpointed.
    pub fn is_stateful(self) -> bool {
        matches!(self, Self::Incremental | Self::Streaming)
    }
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when a string does not name a known engine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown engine '{0}': expected one of batch, incremental, streaming")]
pub struct UnknownEngine(pub String);

impl FromStr for EngineKind {
    type Err = UnknownEngine;

    /// Parse an engine name. Accepts the canonical names plus the historical
    /// aliases `ivm`/`delta` (incremental) and `stream` (streaming) so existing
    /// terminology keeps working.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "batch" => Ok(Self::Batch),
            "incremental" | "ivm" | "delta" => Ok(Self::Incremental),
            "streaming" | "stream" => Ok(Self::Streaming),
            other => Err(UnknownEngine(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn bounded() -> SourceSpec {
        SourceSpec::bounded("t", "memory", "")
    }
    fn unbounded() -> SourceSpec {
        SourceSpec::unbounded("t", "kafka", "topic")
    }
    fn cdc() -> SourceSpec {
        SourceSpec::cdc("t", "debezium", "topic")
    }

    #[test]
    fn infer_batch_when_all_bounded_no_window() {
        assert_eq!(EngineKind::infer(&[bounded()], false), EngineKind::Batch);
    }

    #[test]
    fn infer_streaming_when_unbounded() {
        assert_eq!(
            EngineKind::infer(&[unbounded()], false),
            EngineKind::Streaming
        );
    }

    #[test]
    fn infer_streaming_when_bounded_but_windowed() {
        assert_eq!(EngineKind::infer(&[bounded()], true), EngineKind::Streaming);
    }

    #[test]
    fn infer_incremental_takes_priority_over_unbounded() {
        // A CDC source present alongside an unbounded one still selects IVM.
        assert_eq!(
            EngineKind::infer(&[unbounded(), cdc()], true),
            EngineKind::Incremental
        );
    }

    #[test]
    fn from_str_accepts_canonical_and_aliases() {
        assert_eq!("batch".parse::<EngineKind>().unwrap(), EngineKind::Batch);
        assert_eq!(
            "ivm".parse::<EngineKind>().unwrap(),
            EngineKind::Incremental
        );
        assert_eq!(
            "delta".parse::<EngineKind>().unwrap(),
            EngineKind::Incremental
        );
        assert_eq!(
            "STREAM".parse::<EngineKind>().unwrap(),
            EngineKind::Streaming
        );
    }

    #[test]
    fn from_str_rejects_unknown() {
        assert!("flink".parse::<EngineKind>().is_err());
    }

    #[test]
    fn display_roundtrips_through_from_str() {
        for k in [
            EngineKind::Batch,
            EngineKind::Incremental,
            EngineKind::Streaming,
        ] {
            assert_eq!(k.to_string().parse::<EngineKind>().unwrap(), k);
        }
    }

    #[test]
    fn continuous_and_stateful_flags() {
        assert!(!EngineKind::Batch.is_continuous());
        assert!(!EngineKind::Batch.is_stateful());
        assert!(EngineKind::Incremental.is_continuous());
        assert!(EngineKind::Streaming.is_stateful());
    }
}
