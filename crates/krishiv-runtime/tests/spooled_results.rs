//! End-to-end test for Phase 2.10 disk-spooled task results.
//!
//! Forces every task result over the inline threshold (1 byte), so the
//! executor spools to disk and delivers via the PushTaskResult chunk stream;
//! the coordinator claims the spool on the terminal status and the runtime
//! decodes it back. Uses the coordinator-path hook — the inline fast path
//! would bypass the runner/transport entirely.

use krishiv_runtime::in_process_cluster::InProcessCluster;

/// The threshold override is process-global; serialize the tests so each
/// one's setting actually governs its own query.
static THRESHOLD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn large_results_round_trip_through_disk_spool() {
    let _guard = THRESHOLD_LOCK.lock().unwrap();
    krishiv_executor::set_inline_result_max_bytes_for_tests(1);

    let cluster = InProcessCluster::new().expect("cluster");
    // Non-trivial result set: 10k rows through the coordinator job path.
    let batches = cluster
        .collect_batch_sql_via_coordinator(
            "SELECT v, v * 2 AS doubled FROM (SELECT unnest(range(0, 10000)) AS v) ORDER BY v",
            &[],
        )
        .expect("spooled batch sql");

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 10_000, "all rows must survive the spool round trip");

    // Verify content integrity end-to-end: sum(v) over 0..10000.
    let mut sum: i64 = 0;
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("v column is Int64");
        sum += col.iter().flatten().sum::<i64>();
    }
    assert_eq!(sum, (0..10_000i64).sum::<i64>());
}

#[test]
fn inline_results_unaffected_when_under_threshold() {
    let _guard = THRESHOLD_LOCK.lock().unwrap();
    // Default threshold (8 MiB): a tiny result must stay on the inline path.
    krishiv_executor::set_inline_result_max_bytes_for_tests(8 * 1024 * 1024);
    let cluster = InProcessCluster::new().expect("cluster");
    let batches = cluster
        .collect_batch_sql_via_coordinator("SELECT 42 AS answer", &[])
        .expect("batch sql");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);
}
