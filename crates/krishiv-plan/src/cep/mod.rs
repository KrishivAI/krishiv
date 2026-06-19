#![forbid(unsafe_code)]

//! Complex event processing (CEP) sequential pattern matcher (R16 S2).

mod matcher;
mod pattern;

pub use matcher::{CepKeyState, PartialMatch, PartitionedCepMatcher, SequentialPatternMatcher};
pub use pattern::{CepCompileError, CompiledPattern, Pattern, PatternStage, UnsupportedCombinator};

/// Fragment prefix used by the executor to dispatch CEP tasks.
pub const STREAM_CEP_PREFIX: &str = "stream:cep:";

/// Encode a CEP task fragment that the executor can decode and run.
///
/// The returned string begins with `stream:cep:` followed by a JSON-serialised
/// object containing `key_column`, `event_time_column`, `stage_column`, and
/// `pattern`.
///
/// # Errors
///
/// Returns an error if `pattern` cannot be JSON-serialised (should never happen
/// for a well-formed `CompiledPattern`).
pub fn encode_cep_fragment(
    key_column: &str,
    event_time_column: &str,
    stage_column: &str,
    pattern: &CompiledPattern,
) -> Result<String, serde_json::Error> {
    #[derive(serde::Serialize)]
    struct Spec<'a> {
        key_column: &'a str,
        event_time_column: &'a str,
        stage_column: &'a str,
        pattern: &'a CompiledPattern,
    }
    let json = serde_json::to_string(&Spec {
        key_column,
        event_time_column,
        stage_column,
        pattern,
    })?;
    Ok(format!("{STREAM_CEP_PREFIX}{json}"))
}
