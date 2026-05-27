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

static MERGE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)^\s*MERGE\s+INTO\s+([`\w.:/-]+)\s+USING\s+([`\w.]+)\s+ON\s+(.+?)\s+WHEN\s+MATCHED\s+THEN\s+UPDATE\s+.*WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT",
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
    let merge_key = on_clause
        .split('=')
        .next()
        .and_then(|s| s.split('.').next_back())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MERGE ON clause must be equality on a single column".into(),
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

    if source_batches.is_empty() {
        return Ok(MergeResult {
            rows_inserted: 0,
            rows_updated: 0,
            rows_deleted: 0,
        });
    }

    let source_schema = source_batches[0].schema();
    let existing_schema = existing
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| source_schema.clone());

    // Coalesce existing rows into one batch for key matching.
    let target_batch: Option<RecordBatch> = if existing.is_empty() {
        None
    } else {
        Some(
            concat_batches(&existing_schema, &existing).map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?,
        )
    };

    // Coalesce source into one batch for key extraction.
    let source_batch =
        concat_batches(&source_schema, &source_batches).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;

    let fmt_opts = FormatOptions::default();

    let inserted: u64 = source_batches.iter().map(|b| b.num_rows() as u64).sum();

    // ---- Scope for !Send formatters (must not cross .await) ----
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

    // Count how many target rows match a source key, and build the keep set.
    let (updated, merged_batches) = if let Some(ref tb) = target_batch {
        let target_key_idx =
            existing_schema
                .index_of(merge_key)
                .map_err(|_| SqlError::Unsupported {
                    feature: format!("merge key column '{merge_key}' not found in target schema"),
                })?;
        let target_keys: Vec<String> = {
            let f = ArrayFormatter::try_new(tb.column(target_key_idx), &fmt_opts).map_err(|e| {
                SqlError::DataFusion {
                    message: e.to_string(),
                }
            })?;
            (0..tb.num_rows()).map(|i| f.value(i).to_string()).collect()
        };

        let mut up: u64 = 0;
        let mut keep_indices: Vec<u32> = Vec::new();
        for (i, key) in target_keys.iter().enumerate() {
            if source_keys.contains(key) {
                up += 1;
            } else {
                keep_indices.push(i as u32);
            }
        }

        let mut result: Vec<RecordBatch> = Vec::new();
        if !keep_indices.is_empty() {
            let indices_arr = arrow::array::UInt32Array::from(keep_indices);
            let columns: Vec<arrow::array::ArrayRef> = tb
                .columns()
                .iter()
                .map(|c| {
                    arrow::compute::take(c, &indices_arr, None).map_err(|e| SqlError::DataFusion {
                        message: e.to_string(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let kept =
                RecordBatch::try_new(tb.schema(), columns).map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })?;
            result.push(kept);
        }
        result.extend(source_batches);
        (up, result)
    } else {
        (0, source_batches)
    };
    // ---- end !Send scope ----

    super::providers::register_scan_batches(ctx, table, merged_batches).await?;
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
