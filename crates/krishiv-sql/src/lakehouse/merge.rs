//! MERGE INTO dispatch (R18 S5, ADR-18.2).

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use regex::Regex;
use std::sync::LazyLock;

use datafusion::prelude::SessionContext;

use crate::SqlError;
use crate::SqlResult;

/// Match `alias.col = alias.col` in the ON clause, capturing alias and col for both sides.
static KEY_COL_RE: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)((?:\w+|`[^`]+`))\.((?:\w+|`[^`]+`))\s*=\s*((?:\w+|`[^`]+`))\.((?:\w+|`[^`]+`))",
    )
    .ok()
});

/// Groups: 1 target, 2 source table, 3 ON clause, 4 WHEN MATCHED arm,
/// 5 WHEN NOT MATCHED arm.
///
/// Groups 4 and 5 **must stay capturing**. They were written `(?:…)` — the
/// regex then defined only three groups, `caps.get(4)`/`get(5)` were always
/// `None`, so `has_matched`/`has_not_matched` were always false and
/// `execute_merge_sql` rejected *every* MERGE with "requires at least one WHEN
/// MATCHED or WHEN NOT MATCHED clause". No test caught it because they all
/// asserted on `MERGE_RE.is_match` or called `dry_run_merge` directly, never
/// the entry point `SqlEngine::sql` actually invokes.
static MERGE_RE: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(
        r"(?is)^\s*MERGE\s+INTO\s+([`\w.:/-]+)\s+USING\s+([`\w.]+)\s+ON\s+(.+?)(\s+WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+.+?)?(\s+WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*(?:\([^)]*\))?\s*(?:VALUES\s*\([^)]*\)|\*)?)?\s*$",
    )
    .ok()
});

/// MERGE metrics returned as a single-row batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeResult {
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
}

/// Target table format is not Delta or Iceberg.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("MERGE INTO is only supported for delta: and iceberg: targets (got {target})")]
pub struct MergeTargetUnsupportedError {
    pub target: String,
}

/// Parse and execute a MERGE INTO statement when matched.
pub async fn execute_merge_sql(ctx: &SessionContext, sql: &str) -> SqlResult<Vec<RecordBatch>> {
    let caps = MERGE_RE
        .as_ref()
        .ok_or_else(|| SqlError::DataFusion {
            message: "MERGE regex failed to compile".into(),
        })?
        .captures(sql)
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MERGE INTO syntax".into(),
        })?;
    let target = caps[1].trim_matches('`').to_string();
    let source_table = caps[2].trim_matches('`').to_string();
    let on_clause = caps[3].trim();
    let has_matched = caps
        .get(4)
        .and_then(|m| {
            let s = m.as_str().trim();
            if s.is_empty() { None } else { Some(s) }
        })
        .is_some();
    let has_not_matched = caps
        .get(5)
        .and_then(|m| {
            let s = m.as_str().trim();
            if s.is_empty() { None } else { Some(s) }
        })
        .is_some();
    if !has_matched && !has_not_matched {
        return Err(SqlError::Unsupported {
            feature: "MERGE INTO requires at least one WHEN MATCHED or WHEN NOT MATCHED clause"
                .into(),
        });
    }

    let merge_key: String = KEY_COL_RE
        .as_ref()
        .ok_or_else(|| SqlError::DataFusion { message: "KEY_COL regex failed to compile".into() })?
        .captures(on_clause)
        .ok_or_else(|| SqlError::Unsupported {
            feature:
                "MERGE ON clause must contain a qualified column equality (e.g. target.col = source.col)"
                    .into(),
        })
        .map(|caps| {
            let _left_alias = caps[1].trim_matches('`');
            let left_col = caps[2].trim_matches('`');
            let right_alias = caps[3].trim_matches('`');
            let right_col = caps[4].trim_matches('`');
            // Pick the side whose alias does NOT match the source table to get the target col.
            let source_lower = source_table.to_lowercase();
            if right_alias.to_lowercase() == source_lower {
                left_col.to_string()
            } else {
                right_col.to_string()
            }
        })?;
    let merge_key = merge_key.as_str();

    let source_df = ctx
        .table(&source_table)
        .await
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    let source_batches = source_df
        .collect()
        .await
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;

    // Pass the clauses the statement actually carried. These were parsed and
    // then discarded in favour of hardcoded `true, true`, so
    // `WHEN NOT MATCHED THEN INSERT *` — an insert-only merge — also updated
    // every matched row.
    let metrics = if let Some(path) = target
        .strip_prefix("delta:`")
        .and_then(|p| p.strip_suffix('`'))
    {
        krishiv_connectors::lakehouse::merge_delta(
            path,
            source_batches,
            merge_key,
            has_matched,
            has_not_matched,
        )
        .await
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?
    } else if let Some(path) = target.strip_prefix("delta.") {
        krishiv_connectors::lakehouse::merge_delta(
            path,
            source_batches,
            merge_key,
            has_matched,
            has_not_matched,
        )
        .await
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?
    } else if target.starts_with("iceberg:") {
        // Refuse rather than report a merge that did not happen.
        //
        // This branch used to call a *dry run* that counted how many rows would
        // be inserted vs updated and returned those counts as the statement's
        // result. Nothing was ever written back — the helper's own doc said so
        // — but the caller received `rows_inserted`/`rows_updated` and a
        // success, with no way to tell the table was untouched. That is the
        // same false-success class as DUR-1; the engine's rule is that work not
        // done is not reported as done.
        return Err(SqlError::Unsupported {
            feature: format!(
                "MERGE INTO for Iceberg target '{target}': not implemented — the merged rows \
                 are never written back. Only delta: targets have a real merge. Use \
                 INSERT/UPDATE/DELETE or CREATE OR REPLACE TABLE ... AS SELECT instead."
            ),
        });
    } else {
        return Err(SqlError::DataFusion {
            message: MergeTargetUnsupportedError { target }.to_string(),
        });
    };

    Ok(vec![merge_result_batch(metrics)?])
}

fn merge_result_batch(
    result: krishiv_connectors::lakehouse::MergeDeltaResult,
) -> SqlResult<RecordBatch> {
    merge_metrics_batch(
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
    )
}

fn merge_metrics_batch(inserted: u64, updated: u64, deleted: u64) -> SqlResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("rows_inserted", DataType::Int64, false),
        Field::new("rows_updated", DataType::Int64, false),
        Field::new("rows_deleted", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![inserted as i64])),
            Arc::new(Int64Array::from(vec![updated as i64])),
            Arc::new(Int64Array::from(vec![deleted as i64])),
        ],
    )
    .map_err(|e| SqlError::DataFusion {
        message: format!("merge metrics batch: {e}"),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[test]
    fn merge_regex_matches_basic_statement() {
        let sql = "MERGE INTO delta.`/tmp/t` USING staging ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";
        assert!(MERGE_RE.as_ref().unwrap().is_match(sql));
    }

    #[test]
    fn merge_regex_matches_matched_only() {
        let sql = "MERGE INTO delta.`/tmp/t` USING staging ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET *";
        assert!(MERGE_RE.as_ref().unwrap().is_match(sql));
    }

    #[test]
    fn merge_regex_matches_not_matched_only() {
        let sql = "MERGE INTO delta.`/tmp/t` USING staging ON target.id = source.id \
                   WHEN NOT MATCHED THEN INSERT *";
        assert!(MERGE_RE.as_ref().unwrap().is_match(sql));
    }

    #[test]
    fn merge_key_column_extraction() {
        let on = "target.id = source.id";
        let caps = KEY_COL_RE.as_ref().unwrap().captures(on).unwrap();
        // caps: (left_alias, left_col, right_alias, right_col)
        assert_eq!(caps.get(1).map(|m| m.as_str()), Some("target"));
        assert_eq!(caps.get(2).map(|m| m.as_str()), Some("id"));
    }

    #[test]
    fn merge_key_column_extraction_reversed() {
        // ON clause written source.col = target.col — must still extract target col.
        let on = "source.id = target.id";
        let caps = KEY_COL_RE.as_ref().unwrap().captures(on).unwrap();
        assert_eq!(caps.get(1).map(|m| m.as_str()), Some("source"));
        assert_eq!(caps.get(3).map(|m| m.as_str()), Some("target"));
    }

    #[test]
    fn merge_key_extracts_first_column_from_compound() {
        let on = "target.id = source.id AND target.date = source.date";
        let caps = KEY_COL_RE.as_ref().unwrap().captures(on).unwrap();
        assert_eq!(caps.get(2).map(|m| m.as_str()), Some("id"));
    }

    /// The entry point must actually accept a well-formed MERGE.
    ///
    /// Every existing test here checks `MERGE_RE.is_match` or calls
    /// `dry_run_merge` directly — none goes through `execute_merge_sql`, which
    /// is the only thing `SqlEngine::sql` calls.
    #[tokio::test]
    async fn execute_merge_sql_accepts_a_statement_with_both_when_clauses() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        ctx.register_batch(
            "target_t",
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["alice", "bob"])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        ctx.register_batch(
            "staging",
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 3])),
                    Arc::new(StringArray::from(vec!["alice-updated", "charlie"])),
                ],
            )
            .unwrap(),
        )
        .unwrap();

        let error = execute_merge_sql(
            &ctx,
            "MERGE INTO iceberg:target_t USING staging ON target_t.id = staging.id \
             WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        )
        .await
        .expect_err("Iceberg MERGE is not implemented and must say so");

        // The point of the test: it must get *past* the clause check. Before the
        // capture-group fix every MERGE died here regardless of its clauses.
        let message = error.to_string();
        assert!(
            !message.contains("requires at least one WHEN"),
            "the WHEN clauses were parsed but not seen: {message}"
        );
        assert!(
            message.contains("not implemented"),
            "Iceberg MERGE must fail honestly rather than report a merge that did not happen: \
             {message}"
        );
    }

    /// Both WHEN arms must be recognised independently, and must reach
    /// `merge_delta` as its `when_matched_update` / `when_not_matched_insert`
    /// arguments — they were parsed and then thrown away for hardcoded
    /// `true, true`, so an insert-only MERGE also updated every matched row.
    #[test]
    fn when_clauses_are_captured_independently() {
        let re = MERGE_RE.as_ref().unwrap();

        let both = re
            .captures(
                "MERGE INTO delta.`/tmp/t` USING staging ON t.id = staging.id \
                 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            )
            .unwrap();
        assert!(both.get(4).is_some(), "WHEN MATCHED arm must be captured");
        assert!(
            both.get(5).is_some(),
            "WHEN NOT MATCHED arm must be captured"
        );

        let insert_only = re
            .captures(
                "MERGE INTO delta.`/tmp/t` USING staging ON t.id = staging.id \
                 WHEN NOT MATCHED THEN INSERT *",
            )
            .unwrap();
        assert!(
            insert_only.get(4).is_none(),
            "an insert-only MERGE must not report a WHEN MATCHED arm — that is what made it \
             update matched rows"
        );
        assert!(insert_only.get(5).is_some());

        let update_only = re
            .captures(
                "MERGE INTO delta.`/tmp/t` USING staging ON t.id = staging.id \
                 WHEN MATCHED THEN UPDATE SET *",
            )
            .unwrap();
        assert!(update_only.get(4).is_some());
        assert!(
            update_only.get(5).is_none(),
            "an update-only MERGE must not report a WHEN NOT MATCHED arm"
        );
    }
}
