#![forbid(unsafe_code)]

//! Complex event processing (CEP) sequential pattern matcher (R16 S2).

mod matcher;
mod pattern;

pub use matcher::{CepKeyState, PartialMatch, SequentialPatternMatcher};
pub use pattern::{
    CepCompileError, CompiledPattern, Pattern, PatternStage, UnsupportedCombinator,
};
