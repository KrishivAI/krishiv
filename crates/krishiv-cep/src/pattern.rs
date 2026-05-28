//! CEP pattern builder (R16 S2.1).

use std::time::Duration;

/// Unsupported pattern combinator (quantifiers, negation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsupportedCombinator {
    OneOrMore,
    ZeroOrMore,
    NotFollowedBy,
    Branching,
    ExactCount,
}

/// Pattern compilation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CepCompileError {
    UnsupportedCombinator(UnsupportedCombinator),
    EmptyPattern,
}

impl std::fmt::Display for CepCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedCombinator(c) => write!(
                f,
                "unsupported CEP combinator {:?} — deferred to R17/R18",
                c
            ),
            Self::EmptyPattern => write!(f, "CEP pattern must have at least one stage"),
        }
    }
}

impl std::error::Error for CepCompileError {}

/// One stage in a linear CEP pattern.
#[derive(Debug, Clone)]
pub struct PatternStage {
    pub name: String,
    pub max_gap_ms: Option<u64>,
}

/// Compiled linear pattern.
#[derive(Debug, Clone)]
pub struct CompiledPattern {
    pub stages: Vec<PatternStage>,
    pub window_ms: u64,
}

/// Fluent pattern builder.
#[derive(Debug, Default)]
pub struct Pattern {
    stages: Vec<PatternStage>,
    window_ms: Option<u64>,
}

impl Pattern {
    pub fn begin(name: impl Into<String>) -> Self {
        let mut p = Self::default();
        p.stages.push(PatternStage {
            name: name.into(),
            max_gap_ms: None,
        });
        p
    }

    pub fn followed_by(mut self, name: impl Into<String>) -> Self {
        self.stages.push(PatternStage {
            name: name.into(),
            max_gap_ms: None,
        });
        self
    }

    pub fn within(mut self, duration: Duration) -> Self {
        self.window_ms = Some(duration.as_millis() as u64);
        self
    }

    pub fn times(self, _n: u32) -> Result<Self, CepCompileError> {
        Err(CepCompileError::UnsupportedCombinator(
            UnsupportedCombinator::ExactCount,
        ))
    }

    pub fn compile(self) -> Result<CompiledPattern, CepCompileError> {
        if self.stages.is_empty() {
            return Err(CepCompileError::EmptyPattern);
        }
        Ok(CompiledPattern {
            stages: self.stages,
            window_ms: self.window_ms.unwrap_or(60_000),
        })
    }

    pub fn one_or_more(self) -> Result<Self, CepCompileError> {
        Err(CepCompileError::UnsupportedCombinator(
            UnsupportedCombinator::OneOrMore,
        ))
    }

    pub fn not_followed_by(self) -> Result<Self, CepCompileError> {
        Err(CepCompileError::UnsupportedCombinator(
            UnsupportedCombinator::NotFollowedBy,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_pattern_compiles() {
        let p = Pattern::begin("start")
            .followed_by("next")
            .within(Duration::from_secs(10))
            .compile()
            .unwrap();
        assert_eq!(p.stages.len(), 2);
        assert_eq!(p.window_ms, 10_000);
    }

    #[test]
    fn quantifier_returns_unsupported() {
        let err = Pattern::begin("a").one_or_more().unwrap_err();
        assert!(matches!(
            err,
            CepCompileError::UnsupportedCombinator(UnsupportedCombinator::OneOrMore)
        ));
    }

    #[test]
    fn empty_pattern_rejected() {
        let p = Pattern::default();
        let err = p.compile().unwrap_err();
        assert!(matches!(err, CepCompileError::EmptyPattern));
    }

    #[test]
    fn times_returns_unsupported() {
        let err = Pattern::begin("a").times(3).unwrap_err();
        assert!(matches!(
            err,
            CepCompileError::UnsupportedCombinator(UnsupportedCombinator::ExactCount)
        ));
    }
}
