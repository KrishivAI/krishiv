//! Plan-level description of a stream-to-stream interval join.
//!
//! Mirrors the split that [`crate::window`] already makes: the SQL compiler
//! produces this, and `krishiv-dataflow` turns it into the operator. It lives
//! in this crate because `krishiv-sql` cannot depend on `krishiv-dataflow`, and
//! a spec is the only thing the two need to agree on.

use serde::{Deserialize, Serialize};

/// A windowed equi-join between two streams.
///
/// The window is symmetric — a left event matches a right event whose
/// event-time falls within `± window_ms` — because that is what the executing
/// operator implements. Asymmetric bounds are refused by the compiler rather
/// than rounded to something the operator would silently do differently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingJoinSpec {
    pub left_source: String,
    pub right_source: String,
    /// Event-time column, present in both streams under the same name.
    pub time_column: String,
    pub left_key_column: String,
    pub right_key_column: String,
    /// Half-width of the join window in milliseconds.
    pub window_ms: u64,
}

impl StreamingJoinSpec {
    /// Validate what the operator cannot check for itself.
    ///
    /// # Errors
    /// Returns a validation error for an empty column or source name, or a
    /// zero window — a zero-width join matches only exactly-simultaneous
    /// events, which is almost never what was meant and is more likely a unit
    /// mix-up (seconds written where milliseconds were expected).
    pub fn validate(&self) -> Result<(), crate::PlanError> {
        let named = [
            ("left_source", &self.left_source),
            ("right_source", &self.right_source),
            ("time_column", &self.time_column),
            ("left_key_column", &self.left_key_column),
            ("right_key_column", &self.right_key_column),
        ];
        for (field, value) in named {
            if value.trim().is_empty() {
                return Err(crate::PlanError::Validation(format!(
                    "streaming join {field} must not be empty"
                )));
            }
        }
        if self.window_ms == 0 {
            return Err(crate::PlanError::Validation(String::from(
                "streaming join window must be greater than zero: a zero-width window matches \
                 only exactly-simultaneous events, which is more often a unit mistake than an \
                 intent",
            )));
        }
        if self.left_source == self.right_source {
            return Err(crate::PlanError::Validation(format!(
                "streaming join needs two distinct sources; both sides name '{}'. A self-join \
                 needs the same stream registered twice under different names",
                self.left_source
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> StreamingJoinSpec {
        StreamingJoinSpec {
            left_source: "bid".into(),
            right_source: "auction".into(),
            time_column: "ts".into(),
            left_key_column: "auction".into(),
            right_key_column: "id".into(),
            window_ms: 5_000,
        }
    }

    #[test]
    fn a_valid_spec_passes() {
        spec().validate().expect("valid");
    }

    #[test]
    fn a_zero_window_is_refused_with_the_reason() {
        let mut s = spec();
        s.window_ms = 0;
        let err = s.validate().expect_err("zero window must be refused");
        assert!(err.to_string().contains("unit mistake"), "got: {err}");
    }

    #[test]
    fn identical_sources_are_refused() {
        let mut s = spec();
        s.right_source = s.left_source.clone();
        let err = s.validate().expect_err("self-join must be refused");
        assert!(
            err.to_string().contains("two distinct sources"),
            "got: {err}"
        );
    }

    #[test]
    fn an_empty_column_is_refused_and_named() {
        let mut s = spec();
        s.right_key_column = String::new();
        let err = s.validate().expect_err("empty key must be refused");
        assert!(err.to_string().contains("right_key_column"), "got: {err}");
    }
}

/// A streaming pipeline: a banded two-source join feeding a chain of
/// windowed stages (task #146, NEXMark Q4/Q9).
///
/// Stage 0 consumes the join's output; stage N consumes stage N-1's output.
/// Both halves existed before this type (the interval join and the windowed
/// aggregate); this is the pipe between them. The SQL surface is a WITH
/// chain: the first CTE is the join, each later CTE (and the final SELECT)
/// is a windowed query over the previous name.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamingPipelineSpec {
    pub join: StreamingJoinSpec,
    /// Windowed stages in execution order. Never empty — a pipeline with no
    /// stage is just a join and must be planned as one.
    pub stages: Vec<crate::window::WindowExecutionSpec>,
}

impl StreamingPipelineSpec {
    /// Why this pipeline canNOT run at parallelism > 1, or `None` when it
    /// can — the thin gate over [`Self::parallel_plan`], kept for the
    /// registration and executor guards that only need refuse-or-allow.
    #[must_use]
    pub fn parallel_unsafe_reason(&self) -> Option<String> {
        self.parallel_plan().err()
    }

    /// Where the parallel execution of this pipeline must RE-KEY, if
    /// anywhere.
    ///
    /// `Ok(None)`: every stage's group key contains the join key, so
    /// join-key routing co-locates every stage — subtask-local execution is
    /// correct with no further exchange (fix 10's original safe case).
    ///
    /// `Ok(Some((split, key)))`: stages before `split` are join-key
    /// co-located; every stage from `split` on groups by the single plain
    /// column `key`, so ONE exchange of the stage input by `key` at the
    /// split point makes the remainder subtask-local (the NEXMark q4 shape:
    /// MAX per (auction, category) is auction-co-located, AVG per category
    /// re-keys once).
    ///
    /// `Err(reason)`: no single re-key point exists (post-split stages with
    /// differing, composite, or synthetic keys) — refused BY NAME; such a
    /// shape must never silently run parallel.
    ///
    /// # Errors
    /// The refusal reason, for surfacing verbatim at registration.
    pub fn parallel_plan(&self) -> Result<Option<(usize, String)>, String> {
        let join_keys = [
            self.join.left_key_column.clone(),
            format!("left_{}", self.join.left_key_column),
            self.join.right_key_column.clone(),
            format!("right_{}", self.join.right_key_column),
        ];
        // A stage whose group key CONTAINS the join key is co-located under
        // join-key routing: all rows of one group share the join key value,
        // whatever else the group key adds.
        let colocated = |stage: &crate::window::WindowExecutionSpec| {
            !stage.key_is_synthetic
                && (join_keys.contains(&stage.key_column)
                    || stage
                        .key_parts
                        .iter()
                        .any(|part| join_keys.contains(&part.name)))
        };
        let Some(split) = self.stages.iter().position(|s| !colocated(s)) else {
            return Ok(None);
        };
        let Some(split_stage) = self.stages.get(split) else {
            return Ok(None);
        };
        let exchange_key = split_stage.key_column.clone();
        for (index, stage) in self.stages.iter().enumerate().skip(split) {
            if stage.key_is_synthetic || !stage.key_parts.is_empty() {
                return Err(format!(
                    "stage {index} groups by a composite or synthetic key after the                      re-key point (stage {split}); a single exchange cannot co-locate it"
                ));
            }
            if stage.key_column != exchange_key {
                return Err(format!(
                    "stage {index} groups by '{}' but the re-key point (stage {split})                      exchanges by '{exchange_key}'; one exchange cannot serve both",
                    stage.key_column
                ));
            }
        }
        Ok(Some((split, exchange_key)))
    }

    /// # Errors
    /// When the join is invalid, the stage list is empty, or a stage fails
    /// window validation.
    pub fn validate(&self) -> Result<(), crate::PlanError> {
        self.join.validate()?;
        if self.stages.is_empty() {
            return Err(crate::PlanError::Validation(String::from(
                "a streaming pipeline needs at least one windowed stage; a bare join must be \
                 planned as a join",
            )));
        }
        for stage in &self.stages {
            crate::window::validate_window_execution_spec(stage)?;
        }
        // Enforced here so hand-built specs fail closed too, not only the
        // SQL compiler's: join matches arrive up to `window_ms` out of
        // event-time order, and a first stage with less lateness tolerance
        // silently drops them.
        if let Some(first) = self.stages.first()
            && first.watermark_lag_ms < self.join.window_ms.saturating_mul(2)
        {
            return Err(crate::PlanError::Validation(format!(
                "pipeline stage 0 has watermark_lag_ms {} but the join band is {} ms: an \
                 emitted match's event time can trail the watermark by TWICE the band (its \
                 surviving partner by one band, the band itself by another), and a smaller \
                 lag silently drops those matches as late",
                first.watermark_lag_ms, self.join.window_ms
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod parallel_safety {
    use super::*;

    fn base() -> StreamingPipelineSpec {
        let mut stage = crate::window::WindowExecutionSpec::tumbling("left_a", "left_ts", 10_000);
        stage.watermark_lag_ms = 20_000;
        StreamingPipelineSpec {
            join: StreamingJoinSpec {
                left_source: "l".into(),
                right_source: "r".into(),
                time_column: "ts".into(),
                left_key_column: "a".into(),
                right_key_column: "a".into(),
                window_ms: 10_000,
            },
            stages: vec![stage],
        }
    }

    /// The parallel gate (task #149 fix 10, extended): join-keyed stages run
    /// with no re-key; a run of same-keyed non-join stages gets ONE named
    /// re-key point; anything a single exchange cannot co-locate is refused
    /// BY NAME and must never silently run parallel.
    #[test]
    fn parallel_plan_finds_the_single_rekey_point_or_refuses_by_name() {
        // All stages join-keyed: no exchange needed.
        assert_eq!(base().parallel_plan(), Ok(None));

        // One stage keyed on a non-join column: re-key at stage 0.
        let mut single = base();
        single.stages[0].key_column = "category".into();
        assert_eq!(single.parallel_plan(), Ok(Some((0, "category".into()))));
        assert!(single.parallel_unsafe_reason().is_none());

        // The REAL q4 shape: MAX per (auction, category) — co-located, its
        // composite key contains the join key — then AVG per category:
        // re-key at stage 1.
        let mut q4 = base();
        q4.stages[0].key_parts = vec![crate::window::KeyPart {
            name: "category".into(),
            type_tag: "utf8".into(),
        }];
        let mut avg = crate::window::WindowExecutionSpec::tumbling("category", "ts2", 10_000);
        avg.watermark_lag_ms = 20_000;
        q4.stages.push(avg);
        assert_eq!(q4.parallel_plan(), Ok(Some((1, "category".into()))));

        // Composite key AFTER the re-key point: one exchange cannot
        // co-locate it — refused by name.
        let mut composite = base();
        composite.stages[0].key_column = "category".into();
        composite.stages[0].key_parts = vec![crate::window::KeyPart {
            name: "x".into(),
            type_tag: "utf8".into(),
        }];
        assert!(composite.parallel_plan().is_err());
        assert!(composite.parallel_unsafe_reason().is_some());

        // Two post-split stages with DIFFERENT keys: no single exchange
        // serves both — refused, naming both columns.
        let mut differing = base();
        differing.stages[0].key_column = "category".into();
        let mut second = crate::window::WindowExecutionSpec::tumbling("channel", "ts2", 10_000);
        second.watermark_lag_ms = 20_000;
        differing.stages.push(second);
        let reason = differing.parallel_plan().unwrap_err();
        assert!(
            reason.contains("channel") && reason.contains("category"),
            "{reason}"
        );
    }
}
