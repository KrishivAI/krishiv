//! Does `AS OF` actually return the pinned version?
//!
//! Every existing test around time travel checks one half in isolation:
//! `as_of.rs` proves the clause is stripped and a qualifier collected, and
//! `providers.rs` proves `apply_as_of_refs` *errors* on the cases it cannot
//! serve. Nothing ran a successful `AS OF` query and looked at the rows.
//!
//! That gap matters more than usual here, because `preprocess_as_of_sql`
//! deletes the clause from the SQL before DataFusion sees it. If the pinned
//! table is registered under a name the rewritten query does not use, the
//! query either fails to resolve or quietly reads the current version — the
//! precise outcome `apply_as_of_refs`'s own doc says a time-travel query must
//! never produce.

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-expect-in-tests` does not reach them. A panic is the failure signal here.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use krishiv_sql::SqlEngine;

/// Build a Delta table with two versions: v0 has 3 rows, v1 has 6.
///
/// The row counts differ so "which version did we read?" has an unambiguous
/// answer.
async fn versioned_delta_table(dir: &std::path::Path) -> String {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let path = dir.join("versioned").to_string_lossy().into_owned();
    for chunk in [vec![1_i64, 2, 3], vec![4, 5, 6]] {
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(chunk))])
                .expect("batch");
        krishiv_connectors::lakehouse::write_delta(
            path.clone(),
            vec![batch],
            krishiv_connectors::lakehouse::DeltaWriteMode::Append,
            false,
        )
        .await
        .expect("write delta");
    }
    path
}

async fn count_rows(engine: &SqlEngine, sql: &str) -> Result<i64, String> {
    let df = engine.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total == 0 {
        return Ok(0);
    }
    let first = batches.first().expect("non-empty checked above");
    let col = first
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "expected Int64 count".to_string())?;
    Ok(col.value(0))
}

/// The whole point of the feature: `VERSION AS OF 0` must return v0's rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_as_of_returns_the_pinned_version_not_the_current_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = versioned_delta_table(dir.path()).await;
    let engine = SqlEngine::new();

    // Baseline via the API path, which generates its own table name. A plain
    // `FROM delta.`<path>`` is not a resolvable reference in this engine —
    // which is part of why the SQL time-travel path went unexercised.
    let current: usize = engine
        .read_delta(&path, None)
        .await
        .expect("read_delta baseline")
        .collect()
        .await
        .expect("collect baseline")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(current, 6, "the current version has both appends");

    let pinned = count_rows(
        &engine,
        &format!("SELECT COUNT(*) FROM delta.`{path}` VERSION AS OF 0"),
    )
    .await
    .expect("VERSION AS OF 0 must resolve");

    assert_eq!(
        pinned, 3,
        "VERSION AS OF 0 returned {pinned} rows — version 0 has 3. Reading the current {current} \
         would mean the pinned registration was never reached and the query ran against \
         the current snapshot, which is the silent-wrong-answer this feature exists to \
         prevent."
    );
}
