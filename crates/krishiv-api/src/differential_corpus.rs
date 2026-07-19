//! Cross-engine differential corpus (Phase 60).
//!
//! For the subset of SQL that every engine shares, one query text executed as a
//! **batch** query and as an **IVM view** (final snapshot) must agree. The
//! oracle is free: IVM's `DiffBased` fallback is a batch recompute, so any
//! divergence between `PipelineMode::Batch` and `PipelineMode::Ivm` over the
//! same input+query is a bug — or a semantic difference that must be
//! *documented*, never silent.
//!
//! The harness runs each corpus query through the *same* pipeline builder in
//! both modes and compares order-independent canonical row sets, so a
//! difference in row values, cardinality, or schema fails loudly and names the
//! query.
//!
//! ## Documented semantic difference — NULL group keys
//!
//! Krishiv's hash-partitioning (`krishiv-common::partition`) rejects NULL
//! partition keys, so the IVM engine rejects a `GROUP BY` whose key column
//! contains NULL, whereas the batch planner groups NULLs into a single NULL
//! group (ANSI SQL). The equivalence corpus therefore uses non-NULL group keys;
//! [`ivm_rejects_null_group_keys_is_a_documented_difference`] pins the
//! divergence explicitly so it can never regress into a silent wrong answer.

use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use crate::{PipelineMode, Result, RunPolicy, Session};

/// Fixture table `t(id BIGINT, cat TEXT, amount BIGINT)` with non-NULL group
/// keys across three categories and a range of amounts, in two batches so IVM
/// maintenance runs across more than one insertion.
fn fixture_t() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("cat", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b", "a", "c"])),
            Arc::new(Int64Array::from(vec![10, 5, 20, 100, 7, 30, 50])),
        ],
    )
    .expect("fixture batch");
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![8, 9])),
            Arc::new(StringArray::from(vec!["c", "a"])),
            Arc::new(Int64Array::from(vec![15, 25])),
        ],
    )
    .expect("fixture batch 2");
    vec![batch, batch2]
}

/// Like [`fixture_t`] but with a NULL in the group-key column, used only to pin
/// the documented NULL-key semantic difference.
fn fixture_with_null_key() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("cat", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            Arc::new(Int64Array::from(vec![10, 5, 7])),
        ],
    )
    .expect("null-key fixture");
    vec![batch]
}

/// The shared-subset corpus: queries every engine is expected to agree on.
/// Restricted to projection / filter / aggregation / GROUP BY / HAVING over a
/// single relation with non-NULL keys — the constructs IVM maintains exactly
/// and the batch planner computes directly.
const SHARED_SUBSET: &[&str] = &[
    "SELECT SUM(amount) AS total FROM t",
    "SELECT COUNT(*) AS n FROM t",
    "SELECT cat, SUM(amount) AS s FROM t GROUP BY cat",
    "SELECT cat, COUNT(*) AS n, SUM(amount) AS s FROM t GROUP BY cat",
    "SELECT cat, MIN(amount) AS lo, MAX(amount) AS hi FROM t GROUP BY cat",
    "SELECT cat, SUM(amount) AS s FROM t WHERE amount > 10 GROUP BY cat",
    "SELECT cat, SUM(amount) AS s FROM t GROUP BY cat HAVING SUM(amount) > 30",
    "SELECT cat, AVG(amount) AS a FROM t GROUP BY cat",
];

/// Canonicalise a result set into an order-independent form: the schema
/// signature plus every row rendered as a `|`-joined string, sorted. Row and
/// batch order are irrelevant to equivalence; values, cardinality, and schema
/// are not.
fn canonicalize(batches: &[RecordBatch]) -> (String, Vec<String>) {
    let mut schema_sig = String::new();
    let mut rows: Vec<String> = Vec::new();
    for batch in batches {
        if schema_sig.is_empty() {
            schema_sig = batch
                .schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
                .join(",");
        }
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|col| {
                    arrow::util::display::array_value_to_string(batch.column(col), row)
                        .unwrap_or_else(|_| "<err>".to_string())
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    (schema_sig, rows)
}

/// Run `query` as a materialized view over `fixture` in `mode`, returning the
/// collected snapshot batches (or the engine error).
async fn run_view_result(
    query: &str,
    mode: PipelineMode,
    fixture: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let out: Arc<Mutex<Vec<RecordBatch>>> = Arc::new(Mutex::new(Vec::new()));
    let session = Session::builder().build().expect("session");
    session
        .pipeline("diff")
        .source_memory("t", fixture)
        .view("v", query, true)
        .sink_memory("v", out.clone())
        .mode(mode)
        .run(RunPolicy::Once)
        .await?;
    let guard = out.lock().expect("sink mutex");
    Ok(guard.clone())
}

/// Run over [`fixture_t`], panicking (with the query named) on engine error.
async fn run_view(query: &str, mode: PipelineMode) -> Vec<RecordBatch> {
    run_view_result(query, mode, fixture_t())
        .await
        .unwrap_or_else(|e| panic!("`{query}` failed in {mode:?} mode: {e}"))
}

#[tokio::test]
async fn batch_equals_ivm_for_shared_subset() {
    for query in SHARED_SUBSET {
        let batch = canonicalize(&run_view(query, PipelineMode::Batch).await);
        let ivm = canonicalize(&run_view(query, PipelineMode::Ivm).await);
        assert_eq!(
            batch, ivm,
            "cross-engine divergence for `{query}`:\n batch={batch:?}\n   ivm={ivm:?}\n\
             every divergence is a bug or a documented semantic difference — never silent"
        );
    }
}

#[tokio::test]
async fn corpus_results_are_non_trivial() {
    // Guard against a false green where both engines return nothing: the grouped
    // query must produce the expected three category groups (a, b, c).
    let ivm = canonicalize(
        &run_view(
            "SELECT cat, SUM(amount) AS s FROM t GROUP BY cat",
            PipelineMode::Ivm,
        )
        .await,
    );
    assert_eq!(ivm.1.len(), 3, "expected 3 category groups: {ivm:?}");
}

#[tokio::test]
async fn ivm_rejects_null_group_keys_is_a_documented_difference() {
    // DOCUMENTED SEMANTIC DIFFERENCE (never silent): the batch planner groups a
    // NULL key into its own group (ANSI SQL); Krishiv's hash-partitioned IVM
    // engine rejects NULL partition keys. Surfaced by the differential corpus.
    let query = "SELECT cat, SUM(amount) AS s FROM t GROUP BY cat";

    // Batch groups the NULL key — succeeds with three groups (a, b, NULL).
    let batch = run_view_result(query, PipelineMode::Batch, fixture_with_null_key())
        .await
        .expect("batch groups NULL keys");
    assert_eq!(
        canonicalize(&batch).1.len(),
        3,
        "batch produces a NULL group alongside a and b"
    );

    // IVM rejects the NULL partition key — loudly, not silently.
    let err = run_view_result(query, PipelineMode::Ivm, fixture_with_null_key())
        .await
        .expect_err("IVM must reject NULL group keys rather than drop or miscount them");
    assert!(
        err.to_string().to_lowercase().contains("null"),
        "the divergence must be a clear NULL-key error: {err}"
    );
}
