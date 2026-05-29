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

    pub fn zero_or_more(self) -> Result<Self, CepCompileError> {
        Err(CepCompileError::UnsupportedCombinator(
            UnsupportedCombinator::ZeroOrMore,
        ))
    }

    pub fn branching(self) -> Result<Self, CepCompileError> {
        Err(CepCompileError::UnsupportedCombinator(
            UnsupportedCombinator::Branching,
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

    #[test]
    fn not_followed_by_returns_unsupported() {
        let err = Pattern::begin("a").not_followed_by().unwrap_err();
        assert!(matches!(
            err,
            CepCompileError::UnsupportedCombinator(UnsupportedCombinator::NotFollowedBy)
        ));
    }

    #[test]
    fn zero_or_more_returns_unsupported() {
        let err = Pattern::begin("a").zero_or_more().unwrap_err();
        assert!(matches!(
            err,
            CepCompileError::UnsupportedCombinator(UnsupportedCombinator::ZeroOrMore)
        ));
    }

    #[test]
    fn branching_returns_unsupported() {
        let err = Pattern::begin("a").branching().unwrap_err();
        assert!(matches!(
            err,
            CepCompileError::UnsupportedCombinator(UnsupportedCombinator::Branching)
        ));
    }

    #[test]
    fn display_empty_pattern() {
        let err = CepCompileError::EmptyPattern;
        let msg = format!("{err}");
        assert!(msg.contains("at least one stage"));
    }

    #[test]
    fn display_unsupported_combinator() {
        let err = CepCompileError::UnsupportedCombinator(UnsupportedCombinator::OneOrMore);
        let msg = format!("{err}");
        assert!(msg.contains("OneOrMore"));
    }

    #[test]
    fn single_stage_default_window() {
        let p = Pattern::begin("only").compile().unwrap();
        assert_eq!(p.window_ms, 60_000);
    }

    #[test]
    fn three_stage_pattern() {
        let p = Pattern::begin("a")
            .followed_by("b")
            .followed_by("c")
            .within(Duration::from_secs(30))
            .compile()
            .unwrap();
        assert_eq!(p.stages.len(), 3);
        assert_eq!(p.window_ms, 30_000);
        assert_eq!(p.stages[0].name, "a");
        assert_eq!(p.stages[1].name, "b");
        assert_eq!(p.stages[2].name, "c");
    }

    #[test]
    fn stage_names_preserved() {
        let p = Pattern::begin("login")
            .followed_by("query")
            .followed_by("logout")
            .compile()
            .unwrap();
        let names: Vec<&str> = p.stages.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["login", "query", "logout"]);
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn error_trait_implemented() {
        let err: Box<dyn std::error::Error> = Box::new(CepCompileError::UnsupportedCombinator(
            UnsupportedCombinator::Branching,
        ));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn all_unsupported_combinator_variants_display() {
        let variants = [
            UnsupportedCombinator::OneOrMore,
            UnsupportedCombinator::ZeroOrMore,
            UnsupportedCombinator::NotFollowedBy,
            UnsupportedCombinator::Branching,
            UnsupportedCombinator::ExactCount,
        ];
        for v in &variants {
            let err = CepCompileError::UnsupportedCombinator(v.clone());
            let msg = format!("{err}");
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn unsupported_combinator_debug() {
        let c = UnsupportedCombinator::OneOrMore;
        let debug = format!("{:?}", c);
        assert!(debug.contains("OneOrMore"));
    }

    #[test]
    fn unsupported_combinator_eq() {
        assert_eq!(
            UnsupportedCombinator::OneOrMore,
            UnsupportedCombinator::OneOrMore
        );
        assert_ne!(
            UnsupportedCombinator::OneOrMore,
            UnsupportedCombinator::ZeroOrMore
        );
    }

    #[test]
    fn pattern_default_creates_empty() {
        let p = Pattern::default();
        assert!(p.stages.is_empty());
        assert!(p.window_ms.is_none());
    }

    #[test]
    fn pattern_builder_into_string() {
        let p = Pattern::begin(String::from("dynamic_name"))
            .compile()
            .unwrap();
        assert_eq!(p.stages[0].name, "dynamic_name");
    }

    #[test]
    fn followed_by_chain_builds_correctly() {
        let p = Pattern::begin("a")
            .followed_by("b")
            .followed_by("c")
            .followed_by("d")
            .compile()
            .unwrap();
        assert_eq!(p.stages.len(), 4);
        assert_eq!(p.stages[3].name, "d");
    }

    #[test]
    fn within_sets_window_ms() {
        let p = Pattern::begin("a")
            .within(Duration::from_millis(42))
            .compile()
            .unwrap();
        assert_eq!(p.window_ms, 42);
    }

    #[test]
    fn within_max_duration() {
        let p = Pattern::begin("a")
            .within(Duration::from_millis(u64::MAX))
            .compile()
            .unwrap();
        assert_eq!(p.window_ms, u64::MAX);
    }

    #[test]
    fn within_zero_duration() {
        let p = Pattern::begin("a")
            .within(Duration::from_millis(0))
            .compile()
            .unwrap();
        assert_eq!(p.window_ms, 0);
    }

    #[test]
    fn compiled_pattern_clone() {
        let p = Pattern::begin("x")
            .followed_by("y")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let c = p.clone();
        assert_eq!(c.stages.len(), 2);
        assert_eq!(c.window_ms, 5000);
    }

    #[test]
    fn pattern_stage_clone() {
        let stage = PatternStage {
            name: "test".to_string(),
            max_gap_ms: Some(1000),
        };
        let cloned = stage.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.max_gap_ms, Some(1000));
    }

    #[test]
    fn empty_string_stage_name() {
        let p = Pattern::begin("").compile().unwrap();
        assert_eq!(p.stages[0].name, "");
    }

    #[test]
    fn single_character_stage_name() {
        let p = Pattern::begin("x").compile().unwrap();
        assert_eq!(p.stages[0].name, "x");
    }

    #[test]
    fn long_stage_name() {
        let name = "a".repeat(1000);
        let p = Pattern::begin(name.clone()).compile().unwrap();
        assert_eq!(p.stages[0].name, name);
    }

    #[test]
    fn compile_returns_ok_for_single_stage() {
        assert!(Pattern::begin("only").compile().is_ok());
    }

    #[test]
    fn compile_returns_ok_for_multi_stage() {
        assert!(
            Pattern::begin("a")
                .followed_by("b")
                .followed_by("c")
                .compile()
                .is_ok()
        );
    }
}
