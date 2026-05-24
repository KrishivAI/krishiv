//! Complex event processing (CEP) sequential pattern matcher (R16 S2).

mod matcher;
mod operator;
mod pattern;

pub use matcher::{CepKeyState, PartitionedCepMatcher, PartialMatch, SequentialPatternMatcher};
pub use operator::CepOperator;
pub use pattern::{CepCompileError, CompiledPattern, Pattern, PatternStage, UnsupportedCombinator};
