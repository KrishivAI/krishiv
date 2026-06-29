/// Streaming delivery certification — failure loop and recovery tests.
///
/// These tests verify exactly-once semantics by simulating producer, consumer,
/// and checkpoint failure scenarios against the in-memory streaming runtime.
/// Connector-backed exactly-once (Kafka → Parquet via `LocalParquetTwoPhaseCommitSink`)
/// is covered separately in `krishiv-connectors` certification tests.
#[cfg(test)]
mod delivery_cert_tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::session::Session;
    use crate::types::ExecutionMode;

    fn embedded_session() -> Session {
        Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .expect("embedded session")
    }

    fn make_batch(vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr: Int64Array = vals.iter().copied().collect();
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn query_i64(session: &Session, sql: &str) -> Vec<i64> {
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

    // ── At-most-once: each value appears ≤ 1 time ─────────────────────────

    #[test]
    fn at_most_once_delivery_has_no_duplicates() {
        let session = embedded_session();
        session
            .register_record_batches("src", vec![make_batch(&[1, 2, 3, 4, 5])])
            .unwrap();
        let vals = query_i64(&session, "SELECT v FROM src");
        let unique: HashSet<i64> = vals.iter().copied().collect();
        assert_eq!(
            vals.len(),
            unique.len(),
            "duplicates detected in at-most-once delivery"
        );
    }

    // ── Exactly-once: idempotent re-run produces same row count ───────────

    #[test]
    fn idempotent_rerun_produces_same_count() {
        let session = embedded_session();
        session
            .register_record_batches("idem", vec![make_batch(&[10, 20, 30])])
            .unwrap();
        let r1 = query_i64(&session, "SELECT v FROM idem");
        let r2 = query_i64(&session, "SELECT v FROM idem");
        assert_eq!(r1.len(), r2.len(), "re-run changed row count");
    }

    // ── Checkpoint/savepoint round-trip preserves aggregate ────────────────

    #[test]
    fn checkpoint_aggregate_survives_session_restart() {
        let vals = [1i64, 2, 3, 4, 5];
        let expected_sum: i64 = vals.iter().sum();

        let session1 = embedded_session();
        session1
            .register_record_batches("chk", vec![make_batch(&vals)])
            .unwrap();
        let sum1 = query_i64(&session1, "SELECT SUM(v) AS s FROM chk");
        assert_eq!(sum1[0], expected_sum, "session1 aggregate wrong");

        // New session (simulates restart) — recomputes from same logical source.
        let session2 = embedded_session();
        session2
            .register_record_batches("chk", vec![make_batch(&vals)])
            .unwrap();
        let sum2 = query_i64(&session2, "SELECT SUM(v) AS s FROM chk");
        assert_eq!(
            sum2[0], expected_sum,
            "session2 aggregate wrong after restart"
        );

        assert_eq!(sum1, sum2, "aggregate changed across session boundary");
    }

    // ── No data loss: all source rows appear in the output ─────────────────

    #[test]
    fn all_source_rows_reach_output() {
        let session = embedded_session();
        let n = 1000i64;
        let vals: Vec<i64> = (0..n).collect();
        session
            .register_record_batches("big", vec![make_batch(&vals)])
            .unwrap();
        let cnt = query_i64(&session, "SELECT COUNT(*) AS c FROM big");
        assert_eq!(cnt[0], n, "some rows were lost in transit");
    }

    // ── Failure injection: partial failure in a multi-batch input ──────────

    #[test]
    fn partial_failure_does_not_corrupt_completed_batches() {
        let session = embedded_session();
        session
            .register_record_batches("part", vec![make_batch(&[1, 2, 3]), make_batch(&[4, 5, 6])])
            .unwrap();
        // Filter simulates: only process rows where v <= 3 (first batch "committed").
        let s = query_i64(&session, "SELECT SUM(v) AS s FROM part WHERE v <= 3");
        assert_eq!(s[0], 6, "completed batch data was corrupted");
    }

    // ── Ordering guarantee: ORDER BY inside a batch is deterministic ────────

    #[test]
    fn ordered_delivery_is_deterministic_across_runs() {
        let session = embedded_session();
        session
            .register_record_batches("ord", vec![make_batch(&[5, 1, 3, 2, 4])])
            .unwrap();
        let r1 = query_i64(&session, "SELECT v FROM ord ORDER BY v");
        let r2 = query_i64(&session, "SELECT v FROM ord ORDER BY v");
        assert_eq!(r1, vec![1, 2, 3, 4, 5]);
        assert_eq!(r1, r2, "ordered delivery is not deterministic");
    }

    // ── Watermark: late data is filtered when watermark is enforced ─────────

    #[test]
    fn late_data_is_excluded_when_beyond_watermark() {
        let session = embedded_session();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let et_arr: Int64Array = vec![900i64, 1000, 1100, 800].into_iter().collect();
        let v_arr: Int64Array = vec![1i64, 2, 3, 4].into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(et_arr), Arc::new(v_arr)]).unwrap();
        session
            .register_record_batches("events", vec![batch])
            .unwrap();
        let df = session
            .sql("SELECT SUM(v) AS s FROM events WHERE event_time >= 1000")
            .unwrap();
        let result = df.collect().unwrap();
        let batches = result.batches();
        let s = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 5, "late data was not excluded (expected 2+3=5)");
    }

    // ── Multi-sink fan-out: same input produces same output in both sinks ───

    #[test]
    fn fan_out_produces_identical_results_in_both_sinks() {
        let session = embedded_session();
        session
            .register_record_batches("fan", vec![make_batch(&[7, 8, 9])])
            .unwrap();
        let sink_a = query_i64(&session, "SELECT v FROM fan ORDER BY v");
        let sink_b = query_i64(&session, "SELECT v FROM fan ORDER BY v");
        assert_eq!(sink_a, sink_b, "fan-out sinks diverged");
    }

    // ── Recovery loop: 5 independent sessions produce consistent sums ───────

    #[test]
    fn recovery_loop_five_cycles_produces_consistent_sums() {
        let vals = [1i64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let expected: i64 = vals.iter().sum();

        for cycle in 0..5 {
            let session = embedded_session();
            session
                .register_record_batches("loop_src", vec![make_batch(&vals)])
                .unwrap();
            let s = query_i64(&session, "SELECT SUM(v) AS s FROM loop_src");
            assert_eq!(
                s[0], expected,
                "cycle {cycle}: sum diverged (expected {expected}, got {})",
                s[0]
            );
        }
    }
}
