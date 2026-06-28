/// Embedded and single-node mode conformance tests.
///
/// The same query run through `Embedded` must produce byte-for-byte identical
/// results; mode selection is purely about where data-plane work executes.
/// Single-node tests that require a live coordinator are marked `#[ignore]` so
/// they can be run explicitly against a local daemon.
#[cfg(test)]
mod mode_conformance_tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::session::Session;
    use crate::types::ExecutionMode;

    fn make_batch(vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr: Int64Array = vals.iter().copied().collect();
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn embedded_session() -> Session {
        Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .expect("embedded session")
    }

    fn collect_i64_from_session(session: &Session, sql: &str) -> Vec<i64> {
        let df = session.sql(sql).expect("sql");
        let result = df.collect().expect("collect");
        result
            .into_batches()
            .into_iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // ── Embedded mode conformance ───────────────────────────────────────────

    #[test]
    fn embedded_literal_query_returns_correct_value() {
        let session = embedded_session();
        let vals = collect_i64_from_session(&session, "SELECT 2 + 2 AS v");
        assert_eq!(vals[0], 4);
    }

    #[test]
    fn embedded_session_mode_is_embedded() {
        let session = embedded_session();
        assert_eq!(session.mode(), ExecutionMode::Embedded);
    }

    #[test]
    fn embedded_registered_table_is_queryable() {
        let session = embedded_session();
        session
            .register_record_batches("nums", vec![make_batch(&[10, 20, 30])])
            .unwrap();
        let vals = collect_i64_from_session(&session, "SELECT SUM(v) AS s FROM nums");
        assert_eq!(vals[0], 60);
    }

    #[test]
    fn embedded_filter_and_project() {
        let session = embedded_session();
        session
            .register_record_batches("vals", vec![make_batch(&[1, 2, 3, 4, 5])])
            .unwrap();
        let mut vals =
            collect_i64_from_session(&session, "SELECT v * 2 AS v2 FROM vals WHERE v > 2");
        vals.sort_unstable();
        assert_eq!(vals, vec![6, 8, 10]);
    }

    #[test]
    fn embedded_aggregate_sum_matches_manual() {
        let session = embedded_session();
        session
            .register_record_batches("data", vec![make_batch(&[100, 200, 300])])
            .unwrap();
        let df = session
            .sql("SELECT COUNT(*) AS cnt, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM data")
            .unwrap();
        let result = df.collect().unwrap();
        let batches = result.batches();
        let b = &batches[0];
        let cnt = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let lo = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let hi = b
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 3);
        assert_eq!(s, 600);
        assert_eq!(lo, 100);
        assert_eq!(hi, 300);
    }

    #[test]
    fn embedded_join_produces_cartesian_product_without_condition() {
        let session = embedded_session();
        session
            .register_record_batches("a", vec![make_batch(&[1, 2])])
            .unwrap();
        session
            .register_record_batches("b", vec![make_batch(&[10, 20])])
            .unwrap();
        let df = session
            .sql("SELECT a.v AS av, b.v AS bv FROM a CROSS JOIN b ORDER BY av, bv")
            .unwrap();
        let result = df.collect().unwrap();
        let batches = result.batches();
        let b = &batches[0];
        let avs: Vec<i64> = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let bvs: Vec<i64> = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(avs, vec![1, 1, 2, 2]);
        assert_eq!(bvs, vec![10, 20, 10, 20]);
    }

    // ── Single-node mode conformance (requires live daemon; ignored by default) ──

    #[test]
    #[ignore = "requires local krishiv coordinator on :9090"]
    fn single_node_literal_query_returns_correct_value() {
        let session = Session::builder()
            .with_execution_mode(ExecutionMode::SingleNode)
            .with_coordinator_grpc("http://127.0.0.1:9090")
            .build()
            .expect("single-node session");
        let vals = collect_i64_from_session(&session, "SELECT 2 + 2 AS v");
        assert_eq!(vals[0], 4);
    }

    // ── Determinism: same query, same data → same result ───────────────────

    #[test]
    fn repeated_queries_produce_identical_results() {
        let session = embedded_session();
        session
            .register_record_batches("rep", vec![make_batch(&[5, 3, 1, 4, 2])])
            .unwrap();
        let sql = "SELECT v FROM rep ORDER BY v";
        let v1 = collect_i64_from_session(&session, sql);
        let v2 = collect_i64_from_session(&session, sql);
        assert_eq!(v1, v2);
        assert_eq!(v1, vec![1, 2, 3, 4, 5]);
    }
}
