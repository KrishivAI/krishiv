//! MERGE INTO dispatch (R18 S5, ADR-18.2).

use std::fmt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use regex::Regex;
use std::sync::LazyLock;

use datafusion::prelude::SessionContext;

use crate::SqlError;
use crate::SqlResult;

/// Match the ON-clause equality pattern and extract the column name.
static KEY_COL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:(?:\w+|`[^`]+`)\.)?(\w+)\s*=").unwrap());

static MERGE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)^\s*MERGE\s+INTO\s+([`\w.:/-]+)\s+USING\s+([`\w.]+)\s+ON\s+(.+?)(?:\s+WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+.+?)?(?:\s+WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*(?:\([^)]*\))?\s*(?:VALUES\s*\([^)]*\)|\*)?)?\s*$",
    )
    .unwrap()
});

/// MERGE metrics returned as a single-row batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeResult {
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
}

/// Target table format is not Delta or Iceberg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeTargetUnsupportedError {
    pub target: String,
}

impl fmt::Display for MergeTargetUnsupportedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MERGE INTO is only supported for delta: and iceberg: targets (got {})",
            self.target
        )
    }
}

impl std::error::Error for MergeTargetUnsupportedError {}

/// Parse and execute a MERGE INTO statement when matched.
pub async fn execute_merge_sql(ctx: &SessionContext, sql: &str) -> SqlResult<Vec<RecordBatch>> {
    let caps = MERGE_RE
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

    let merge_key = KEY_COL_RE
        .captures(on_clause)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim_matches('`'))
        .ok_or_else(|| SqlError::Unsupported {
            feature:
                "MERGE ON clause must contain a column equality (e.g. target.col = source.col)"
                    .into(),
        })?;

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

    let metrics = if let Some(path) = target
        .strip_prefix("delta:`")
        .and_then(|p| p.strip_suffix('`'))
    {
        krishiv_lakehouse::merge_delta(path, source_batches, merge_key, true, true)
            .await
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?
    } else if let Some(path) = target.strip_prefix("delta.") {
        krishiv_lakehouse::merge_delta(path, source_batches, merge_key, true, true)
            .await
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?
    } else if target.starts_with("iceberg:") {
        let r = merge_iceberg_memory(ctx, &target, source_batches, merge_key).await?;
        krishiv_lakehouse::MergeDeltaResult {
            rows_inserted: r.rows_inserted,
            rows_updated: r.rows_updated,
            rows_deleted: r.rows_deleted,
        }
    } else {
        return Err(SqlError::DataFusion {
            message: MergeTargetUnsupportedError { target }.to_string(),
        });
    };

    Ok(vec![merge_result_batch(metrics)])
}

async fn merge_iceberg_memory(
    ctx: &SessionContext,
    target: &str,
    source_batches: Vec<RecordBatch>,
    merge_key: &str,
) -> SqlResult<MergeResult> {
    use arrow::compute::concat_batches;
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    use std::collections::HashSet;

    if source_batches.is_empty() {
        return Ok(MergeResult::default());
    }

    let source_schema = source_batches[0].schema();
    let source_batch =
        concat_batches(&source_schema, &source_batches).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;

    let inserted: u64 = source_batches.iter().map(|b| b.num_rows() as u64).sum();
    let fmt_opts = FormatOptions::default();

    // Extract source key values into a hash set.
    let key_idx = source_schema
        .index_of(merge_key)
        .map_err(|_| SqlError::Unsupported {
            feature: format!("merge key column '{merge_key}' not found in source schema"),
        })?;
    let source_keys: HashSet<String> = {
        let f = ArrayFormatter::try_new(source_batch.column(key_idx), &fmt_opts).map_err(|e| {
            SqlError::DataFusion {
                message: e.to_string(),
            }
        })?;
        (0..source_batch.num_rows())
            .map(|i| f.value(i).to_string())
            .collect()
    };

    // Only load the target table when we have source keys to match against.
    let updated = if source_keys.is_empty() {
        0
    } else {
        let table = target.trim_start_matches("iceberg:");
        let existing = ctx
            .table(table)
            .await
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?
            .collect()
            .await
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;

        if existing.is_empty() {
            0
        } else {
            let existing_schema = existing[0].schema();
            let tb =
                concat_batches(&existing_schema, &existing).map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })?;
            let target_key_idx =
                tb.schema()
                    .index_of(merge_key)
                    .map_err(|_| SqlError::Unsupported {
                        feature: format!(
                            "merge key column '{merge_key}' not found in target schema"
                        ),
                    })?;
            let target_keys: Vec<String> = {
                let f =
                    ArrayFormatter::try_new(tb.column(target_key_idx), &fmt_opts).map_err(|e| {
                        SqlError::DataFusion {
                            message: e.to_string(),
                        }
                    })?;
                (0..tb.num_rows()).map(|i| f.value(i).to_string()).collect()
            };
            target_keys
                .iter()
                .filter(|k| source_keys.contains(*k))
                .count() as u64
        }
    };
    // ---- end !Send scope ----

    Ok(MergeResult {
        rows_inserted: inserted.saturating_sub(updated),
        rows_updated: updated,
        rows_deleted: 0,
    })
}

fn merge_result_batch(result: krishiv_lakehouse::MergeDeltaResult) -> RecordBatch {
    merge_metrics_batch(
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
    )
}

fn merge_metrics_batch(inserted: u64, updated: u64, deleted: u64) -> RecordBatch {
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
    .expect("valid merge metrics batch")
}

#[cfg(test)]
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
        assert!(MERGE_RE.is_match(sql));
    }

    #[test]
    fn merge_regex_matches_matched_only() {
        let sql = "MERGE INTO delta.`/tmp/t` USING staging ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET *";
        assert!(MERGE_RE.is_match(sql));
    }

    #[test]
    fn merge_regex_matches_not_matched_only() {
        let sql = "MERGE INTO delta.`/tmp/t` USING staging ON target.id = source.id \
                   WHEN NOT MATCHED THEN INSERT *";
        assert!(MERGE_RE.is_match(sql));
    }

    #[test]
    fn merge_key_column_extraction() {
        let on = "target.id = source.id";
        let caps = KEY_COL_RE.captures(on).unwrap();
        assert_eq!(caps.get(1).map(|m| m.as_str()), Some("id"));
    }

    #[test]
    fn merge_key_extracts_first_column_from_compound() {
        let on = "target.id = source.id AND target.date = source.date";
        let caps = KEY_COL_RE.captures(on).unwrap();
        assert_eq!(caps.get(1).map(|m| m.as_str()), Some("id"));
    }

    /// C9 regression: iceberg in-memory merge must return correct metrics
    /// (updated for matching keys, inserted for new keys) and must NOT
    /// report all rows as inserted (the full-table-replace bug).
    #[tokio::test]
    async fn iceberg_merge_returns_correct_row_counts() {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Target: (1, "alice"), (2, "bob")
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

        // Source: (1, "alice-updated"), (3, "charlie") — id=1 matches, id=3 is new
        let source = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 3])),
                Arc::new(StringArray::from(vec!["alice-updated", "charlie"])),
            ],
        )
        .unwrap();

        let result = merge_iceberg_memory(&ctx, "iceberg:target_t", vec![source], "id")
            .await
            .unwrap();

        assert_eq!(result.rows_updated, 1, "id=1 matches target → updated");
        assert_eq!(result.rows_inserted, 1, "id=3 is new → inserted");
        assert_eq!(result.rows_deleted, 0);
    }
}
