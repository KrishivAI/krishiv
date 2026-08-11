#![forbid(unsafe_code)]
//! Vector distance built-ins (Phase 36 leg b(1), gap G19): similarity
//! expressible in plain governed SQL.
//!
//! DataFusion already ships `array_distance` (Euclidean/L2) over
//! `List`/`FixedSizeList`; this module adds what it lacks:
//!
//! - **`cosine_distance(a, b)`** — `1 − dot(a,b) / (‖a‖·‖b‖)`. NULL when
//!   either input is NULL or either magnitude is zero (undefined, not an
//!   error — a zero embedding is data, not a bug in the query).
//! - **`inner_product(a, b)`** — the raw dot product (the scoring
//!   primitive the leg-c quantized re-rank estimates; exposed now so
//!   queries and tests can express exact scores).
//!
//! Both operate row-wise over float lists, error on per-row length
//! mismatches (a mismatched embedding is corrupt data — silently scoring
//! it would rank garbage), and coerce `FixedSizeList` embeddings through
//! DataFusion's normal cast machinery via the `List(Float64)` signature.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Builder, ListArray};
use arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::prelude::SessionContext;

/// Register the vector distance built-ins on `ctx`.
pub fn register_vector_functions(ctx: &SessionContext) {
    ctx.register_udf(make_pairwise("cosine_distance", cosine_of));
    // Spark 4 spells it `cosine_similarity`-adjacent in some engines;
    // the `array_` prefix mirrors DataFusion's own `array_distance`.
    ctx.register_udf(make_pairwise("array_cosine_distance", cosine_of));
    ctx.register_udf(make_pairwise("inner_product", dot_of));
}

fn list_f64() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
}

/// One row's vectors reduced to a score, or `None` for SQL NULL.
type RowScore = fn(&[f64], &[f64]) -> Option<f64>;

fn cosine_of(a: &[f64], b: &[f64]) -> Option<f64> {
    // f64 for the SQL built-in (SQL floats are f64); the IVF index uses the
    // f32 `VectorMetric::Cosine` with identical math, so ordering agrees.
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    (denom > 0.0).then(|| 1.0 - dot / denom)
}

fn dot_of(a: &[f64], b: &[f64]) -> Option<f64> {
    Some(a.iter().zip(b).map(|(x, y)| x * y).sum())
}

fn make_pairwise(name: &'static str, score: RowScore) -> ScalarUDF {
    create_udf(
        name,
        vec![list_f64(), list_f64()],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(move |args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let [left, right] = arrays.as_slice() else {
                return Err(DataFusionError::Internal(format!(
                    "{name}: expected 2 arguments"
                )));
            };
            let (left, right) = (as_list(name, left)?, as_list(name, right)?);
            let mut out = Float64Builder::with_capacity(left.len());
            for i in 0..left.len() {
                if left.is_null(i) || right.is_null(i) {
                    out.append_null();
                    continue;
                }
                let a = row_values(name, left, i)?;
                let b = row_values(name, right, i)?;
                if a.len() != b.len() {
                    return Err(DataFusionError::Execution(format!(
                        "{name}: vector length mismatch at row {i}: {} vs {} — \
                         a mismatched embedding is corrupt data",
                        a.len(),
                        b.len()
                    )));
                }
                match score(&a, &b) {
                    Some(v) => out.append_value(v),
                    None => out.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
        }),
    )
}

fn as_list<'a>(name: &str, array: &'a ArrayRef) -> Result<&'a ListArray, DataFusionError> {
    array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{name}: argument must coerce to List(Float64), got {}",
                array.data_type()
            ))
        })
}

fn row_values(name: &str, list: &ListArray, row: usize) -> Result<Vec<f64>, DataFusionError> {
    use arrow::array::Float64Array;
    let values = list.value(row);
    let floats = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{name}: vector elements must be Float64, got {}",
                values.data_type()
            ))
        })?;
    if floats.null_count() > 0 {
        return Err(DataFusionError::Execution(format!(
            "{name}: vector at row {row} contains NULL elements — score it after \
             cleaning, or the ranking would silently treat NULL as 0"
        )));
    }
    Ok(floats.values().to_vec())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array};

    async fn scalar(sql: &str) -> Option<f64> {
        let engine = crate::SqlEngine::new();
        let batches = engine
            .sql(sql)
            .await
            .expect("query runs")
            .collect()
            .await
            .expect("collect");
        let col = batches[0].column(0);
        let arr = col.as_any().downcast_ref::<Float64Array>().expect("f64");
        arr.is_valid(0).then(|| arr.value(0))
    }

    #[tokio::test]
    async fn cosine_distance_known_values() {
        // Identical direction → 0; orthogonal → 1; opposite → 2.
        let d = scalar("SELECT cosine_distance([1.0, 0.0], [2.0, 0.0])").await;
        assert!(d.unwrap().abs() < 1e-12, "{d:?}");
        let d = scalar("SELECT cosine_distance([1.0, 0.0], [0.0, 1.0])").await;
        assert!((d.unwrap() - 1.0).abs() < 1e-12, "{d:?}");
        let d = scalar("SELECT cosine_distance([1.0, 0.0], [-1.0, 0.0])").await;
        assert!((d.unwrap() - 2.0).abs() < 1e-12, "{d:?}");
        // The array_ alias is byte-identical.
        let d = scalar("SELECT array_cosine_distance([1.0, 0.0], [0.0, 1.0])").await;
        assert!((d.unwrap() - 1.0).abs() < 1e-12, "{d:?}");
    }

    #[tokio::test]
    async fn zero_vector_is_null_not_error() {
        assert_eq!(
            scalar("SELECT cosine_distance([0.0, 0.0], [1.0, 0.0])").await,
            None,
            "zero magnitude is undefined similarity — NULL, not a crash"
        );
    }

    #[tokio::test]
    async fn inner_product_and_fixed_size_list_coercion() {
        let d = scalar("SELECT inner_product([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])").await;
        assert!((d.unwrap() - 32.0).abs() < 1e-12, "{d:?}");
        // FixedSizeList embeddings (the vector-table storage shape) coerce.
        let d = scalar(
            "SELECT cosine_distance(\
               arrow_cast([1.0, 0.0], 'FixedSizeList(2, Float64)'), \
               arrow_cast([0.0, 1.0], 'FixedSizeList(2, Float64)'))",
        )
        .await;
        assert!((d.unwrap() - 1.0).abs() < 1e-12, "{d:?}");
    }

    #[tokio::test]
    async fn length_mismatch_is_an_error() {
        let engine = crate::SqlEngine::new();
        let result = engine
            .sql("SELECT cosine_distance([1.0, 2.0], [1.0, 2.0, 3.0])")
            .await;
        let err = match result {
            Err(e) => e.to_string(),
            Ok(df) => df
                .collect()
                .await
                .expect_err("mismatch must error")
                .to_string(),
        };
        assert!(err.contains("length mismatch"), "{err}");
    }

    #[tokio::test]
    async fn ranks_over_a_table() {
        // The leg-b(1) use shape: ORDER BY distance LIMIT k in governed SQL.
        let engine = crate::SqlEngine::new();
        engine
            .sql(
                "CREATE TABLE docs AS SELECT * FROM (VALUES \
                 ('a', [1.0, 0.0]), ('b', [0.7, 0.7]), ('c', [0.0, 1.0])) v(id, emb)",
            )
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");
        let batches = engine
            .sql(
                "SELECT id FROM docs \
                 ORDER BY cosine_distance(emb, [1.0, 0.0]) LIMIT 2",
            )
            .await
            .expect("rank")
            .collect()
            .await
            .expect("collect");
        use arrow::array::StringArray;
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "a", "exact match ranks first");
        assert_eq!(ids.value(1), "b", "45-degree neighbor second");
    }
}
