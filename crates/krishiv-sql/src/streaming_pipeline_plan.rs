//! Compile a WITH-chained streaming pipeline: a banded join feeding windowed
//! stages (task #146, NEXMark Q4/Q9).
//!
//! ```sql
//! WITH joined AS (SELECT ... FROM bid b JOIN auction a ON ... BETWEEN ...),
//!      winning AS (SELECT auction, MAX(price) AS final
//!                  FROM TUMBLE(TABLE joined, DESCRIPTOR(dateTime), 10000)
//!                  GROUP BY auction, window_start, window_end)
//! SELECT category, AVG(final) ... FROM TUMBLE(TABLE winning, ...) ...
//! ```
//!
//! The first CTE MUST be the banded join; every later CTE and the final
//! SELECT must be a windowed query whose source is the PREVIOUS name — a
//! stage reading anything else is refused naming both names, because a
//! silently re-ordered pipeline computes an answer to a different question.
//!
//! Both sub-compilers are the existing ones: the join CTE goes through
//! `compile_streaming_join_sql`, each stage through
//! `compile_streaming_window_sql`. This module owns only the chain.

use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use krishiv_plan::stream_join::StreamingPipelineSpec;

use crate::streaming_join_plan::{compile_streaming_join_sql, looks_like_streaming_join};
use crate::streaming_tvf::rewrite_window_tvfs;
use crate::streaming_window_plan::compile_streaming_window_sql;
use crate::{SqlError, SqlResult};

fn unsupported(feature: impl Into<String>) -> SqlError {
    SqlError::Unsupported {
        feature: feature.into(),
    }
}

/// Compiled pipeline plan.
#[derive(Debug, Clone)]
pub struct StreamingPipelinePlan {
    pub spec: StreamingPipelineSpec,
}

/// Shape predicate: a WITH chain whose FIRST CTE is a banded stream join.
///
/// Shape, not validity — a malformed pipeline must reach THIS compiler so
/// its error describes the pipeline, not an unknown table.
#[must_use]
pub fn looks_like_streaming_pipeline(sql: &str) -> bool {
    let rewritten = rewrite_window_tvfs(sql);
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, &rewritten) else {
        return false;
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return false;
    };
    let Some(with) = &query.with else {
        return false;
    };
    with.cte_tables
        .first()
        .is_some_and(|cte| looks_like_streaming_join(&cte.query.to_string()))
}

/// Compile the WITH pipeline form.
///
/// # Errors
/// Returns [`SqlError::Unsupported`] naming what was refused: a first CTE
/// that is not a banded join, a stage whose source is not the previous
/// name, or anything either sub-compiler refuses.
pub fn compile_streaming_pipeline_sql(sql: &str) -> SqlResult<StreamingPipelinePlan> {
    // TVFs are rewritten over the WHOLE text first (the raw TVF syntax does
    // not parse); each stage is then handed back as SQL text to the existing
    // windowed compiler, whose own rewrite pass finds nothing left to do.
    let rewritten = rewrite_window_tvfs(sql);
    let statements = Parser::parse_sql(&GenericDialect {}, &rewritten)
        .map_err(|e| unsupported(format!("streaming pipeline parse error: {e}")))?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Err(unsupported("streaming pipeline expects a single SELECT"));
    };
    let Some(with) = &query.with else {
        return Err(unsupported(
            "streaming pipeline needs a WITH chain: the first CTE is the join, later CTEs \
             and the final SELECT are windowed stages",
        ));
    };
    let Some((join_cte, stage_ctes)) = with.cte_tables.split_first() else {
        return Err(unsupported("streaming pipeline WITH chain is empty"));
    };

    let join_sql = join_cte.query.to_string();
    if !looks_like_streaming_join(&join_sql) {
        return Err(unsupported(format!(
            "streaming pipeline's first CTE '{}' must be a stream-stream join with an \
             event-time BETWEEN band",
            join_cte.alias.name.value
        )));
    }
    let join = compile_streaming_join_sql(&join_sql)?.spec;

    let mut previous_name = join_cte.alias.name.value.clone();
    let mut stages: Vec<krishiv_plan::window::WindowExecutionSpec> =
        Vec::with_capacity(stage_ctes.len() + 1);
    for cte in stage_ctes {
        let stage = compile_streaming_window_sql(&cte.query.to_string())?;
        if stage.source != previous_name {
            return Err(unsupported(format!(
                "pipeline stage '{}' reads from '{}' but must read from the previous stage \
                 '{previous_name}' — a re-ordered pipeline answers a different question",
                cte.alias.name.value, stage.source
            )));
        }
        stages.push(stage.spec);
        previous_name = cte.alias.name.value.clone();
    }

    // The final SELECT is the last stage. Re-render the query WITHOUT its
    // WITH clause so the windowed compiler sees a plain windowed SELECT.
    let SetExpr::Select(_) = query.body.as_ref() else {
        return Err(unsupported(
            "streaming pipeline's final query must be a plain SELECT",
        ));
    };
    let mut final_query = query.clone();
    final_query.with = None;
    let final_stage = compile_streaming_window_sql(&final_query.to_string())?;
    if final_stage.source != previous_name {
        return Err(unsupported(format!(
            "the final SELECT reads from '{}' but must read from the last stage \
             '{previous_name}'",
            final_stage.source
        )));
    }
    stages.push(final_stage.spec);

    // The join emits a match when its SECOND side arrives, so match event
    // times are out of order by up to the band width relative to arrival.
    // Stage 0 must therefore tolerate at least `window_ms` of lateness or it
    // silently drops every match whose partner arrived late in the band —
    // the fixture that caught this lost one auction of two. Later stages
    // consume closed-window output, which is emitted in window order.
    if let Some(first) = stages.first_mut() {
        first.watermark_lag_ms = first.watermark_lag_ms.max(join.window_ms);
    }
    let spec = StreamingPipelineSpec { join, stages };
    spec.validate()
        .map_err(|e| unsupported(format!("streaming pipeline: {e}")))?;
    Ok(StreamingPipelinePlan { spec })
}
