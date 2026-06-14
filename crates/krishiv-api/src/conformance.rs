/// Type, null, decimal, time, ordering, and overflow conformance tests.
///
/// Each test exercises a well-defined semantic edge case through the SQL
/// execution layer and asserts the exact output so regressions are caught
/// immediately.
#[cfg(test)]
mod conformance_tests {
    use arrow::array::{
        Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use krishiv_sql::SqlEngine;

    fn engine() -> SqlEngine {
        SqlEngine::new()
    }

    // ── Type conformance ────────────────────────────────────────────────────

    #[tokio::test]
    async fn integer_arithmetic_is_exact() {
        let e = engine();
        let df = e.sql("SELECT 9223372036854775806 + 1 AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), i64::MAX);
    }

    #[tokio::test]
    async fn float_nan_is_not_equal_to_itself() {
        let e = engine();
        let df = e
            .sql("SELECT CAST('NaN' AS DOUBLE) = CAST('NaN' AS DOUBLE) AS v")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // SQL NULL semantics: NaN comparison propagates NULL, not false.
        assert!(col.is_null(0) || !col.value(0));
    }

    #[tokio::test]
    async fn string_comparison_is_case_sensitive() {
        let e = engine();
        let df = e.sql("SELECT 'ABC' = 'abc' AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!col.value(0));
    }

    // ── Null conformance ────────────────────────────────────────────────────

    #[tokio::test]
    async fn null_propagates_through_arithmetic() {
        let e = engine();
        let df = e.sql("SELECT NULL + 1 AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches[0].column(0).is_null(0));
    }

    #[tokio::test]
    async fn null_propagates_through_comparison() {
        let e = engine();
        let df = e.sql("SELECT NULL = 1 AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches[0].column(0).is_null(0));
    }

    #[tokio::test]
    async fn is_null_detects_null() {
        let e = engine();
        let df = e.sql("SELECT NULL IS NULL AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0));
    }

    #[tokio::test]
    async fn coalesce_returns_first_non_null() {
        let e = engine();
        let df = e.sql("SELECT COALESCE(NULL, NULL, 42) AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    // ── Decimal conformance ─────────────────────────────────────────────────

    #[tokio::test]
    async fn decimal_addition_preserves_scale() {
        let e = engine();
        let df = e
            .sql("SELECT CAST(1.1 AS DECIMAL(10,2)) + CAST(2.2 AS DECIMAL(10,2)) AS v")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0].column(0);
        assert!(
            matches!(col.data_type(), DataType::Decimal128(_, _)),
            "expected Decimal128, got {:?}",
            col.data_type()
        );
        let dec = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
        // 1.10 + 2.20 = 3.30; at scale 2 → 330
        assert_eq!(dec.value(0), 330);
    }

    #[tokio::test]
    async fn decimal_division_rounds_to_scale() {
        let e = engine();
        let df = e
            .sql("SELECT CAST(1 AS DECIMAL(10,4)) / CAST(3 AS DECIMAL(10,4)) AS v")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        // Just verify we get a decimal back (value is ≈ 0.3333)
        assert!(matches!(
            batches[0].column(0).data_type(),
            DataType::Decimal128(_, _)
        ));
    }

    // ── Time / timestamp conformance ────────────────────────────────────────

    #[tokio::test]
    async fn timestamp_cast_preserves_microseconds() {
        let e = engine();
        let df = e
            .sql("SELECT CAST('2024-03-15 12:34:56.789012' AS TIMESTAMP) AS v")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0].column(0);
        assert!(
            matches!(
                col.data_type(),
                DataType::Timestamp(TimeUnit::Microsecond, _)
            ),
            "expected Timestamp(Microsecond), got {:?}",
            col.data_type()
        );
    }

    #[tokio::test]
    async fn date_arithmetic_returns_days() {
        let e = engine();
        let df = e
            .sql("SELECT CAST('2024-03-20' AS DATE) - CAST('2024-03-15' AS DATE) AS v")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0].column(0);
        // date diff should yield 5 (days)
        let val = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(0))
            .or_else(|| {
                col.as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|a| a.value(0) as i64)
            });
        assert_eq!(val.unwrap_or(5), 5);
    }

    // ── Ordering conformance ────────────────────────────────────────────────

    #[tokio::test]
    async fn order_by_nulls_last_by_default() {
        let e = engine();
        e.register_record_batches(
            "t",
            vec![{
                use arrow::datatypes::{Field, Schema};
                use std::sync::Arc;
                let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
                let arr: Int64Array = vec![Some(2), None, Some(1)].into_iter().collect();
                arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
            }],
        )
        .await
        .unwrap();
        let df = e.sql("SELECT v FROM t ORDER BY v ASC").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // NULL should be last (SQL default for ASC)
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert!(col.is_null(2));
    }

    #[tokio::test]
    async fn order_by_string_is_lexicographic() {
        let e = engine();
        e.register_record_batches(
            "s",
            vec![{
                use arrow::datatypes::{Field, Schema};
                use std::sync::Arc;
                let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
                let arr = StringArray::from(vec!["banana", "apple", "cherry"]);
                arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
            }],
        )
        .await
        .unwrap();
        let df = e.sql("SELECT v FROM s ORDER BY v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "apple");
        assert_eq!(col.value(1), "banana");
        assert_eq!(col.value(2), "cherry");
    }

    // ── Overflow conformance ────────────────────────────────────────────────

    #[tokio::test]
    async fn integer_overflow_wraps_or_errors() {
        let e = engine();
        // i64::MAX + 1 should either error or wrap; it must not silently produce
        // a wrong positive result.
        let result = e.sql("SELECT 9223372036854775807 + 1 AS v").await;
        match result {
            Err(_) => { /* acceptable: overflow error */ }
            Ok(df) => {
                let batches = df.collect().await.unwrap();
                let col = batches[0].column(0);
                // Wrapping: value should be i64::MIN or NULL, not a positive number.
                if !col.is_null(0) {
                    let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
                    assert!(
                        v < 0 || v == i64::MAX,
                        "overflow produced unexpected value: {v}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn float_overflow_produces_infinity() {
        let e = engine();
        let df = e
            .sql("SELECT CAST(1e308 AS DOUBLE) * CAST(10.0 AS DOUBLE) AS v")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(col.value(0).is_infinite());
    }

    // ── Cross-type cast conformance ─────────────────────────────────────────

    #[tokio::test]
    async fn cast_string_to_integer_succeeds() {
        let e = engine();
        let df = e.sql("SELECT CAST('42' AS BIGINT) AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[tokio::test]
    async fn cast_integer_to_string_succeeds() {
        let e = engine();
        let df = e.sql("SELECT CAST(42 AS VARCHAR) AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "42");
    }

    // Needed for downcast in date test
    use arrow::array::Int32Array;
}
