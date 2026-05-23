//! MERGE INTO dispatch (R18 S5, ADR-18.2).

use std::fmt;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use regex::Regex;
use std::sync::LazyLock;

use datafusion::prelude::SessionContext;

use crate::SqlError;
use crate::SqlResult;

static MERGE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)^\s*MERGE\s+INTO\s+([`\w.:/-]+)\s+USING\s+([`\w.]+)\s+ON\s+([^W]+)WHEN\s+MATCHED\s+THEN\s+UPDATE\s+.*WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT",
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
    } else if target.starts_with("iceberg:") || target.contains('.') {
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
    mut source_batches: Vec<RecordBatch>,
    merge_key: &str,
) -> SqlResult<MergeResult> {
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
    let mut rows = existing;
    rows.append(&mut source_batches);
    super::providers::register_scan_batches(ctx, table, rows).await?;
    Ok(MergeResult {
        rows_inserted: source_batches.iter().map(|b| b.num_rows() as u64).sum(),
        rows_updated: 0,
        rows_deleted: 0,
    })
}

fn merge_result_batch(result: krishiv_lakehouse::MergeDeltaResult) -> RecordBatch {
    merge_metrics_batch(result.rows_inserted, result.rows_updated, result.rows_deleted)
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

    #[test]
    fn merge_regex_matches_basic_statement() {
        let sql = "MERGE INTO delta.`/tmp/t` USING staging ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";
        assert!(MERGE_RE.is_match(sql));
    }
}
