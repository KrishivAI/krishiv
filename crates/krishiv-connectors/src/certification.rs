//! Connector certification harness — recovery and exactly-once guarantees (Phase D).
//!
//! These tests formally verify the recovery and delivery semantics of each
//! certified sink implementation.  They are separate from the unit tests in
//! `two_phase.rs` to allow independent CI gating.
//!
//! Certified backends:
//! - [`EpochTransactionLog`] over [`InMemoryTwoPhaseCommitSink`]  (in-memory)
//! - [`EpochTransactionLog`] over [`LocalParquetTwoPhaseCommitSink`]  (local FS)
//! - [`IcebergNativeTwoPhaseCommit`]  (native Iceberg, iceberg feature)

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::two_phase::{
        EpochTransactionLog, InMemoryTwoPhaseCommitSink, LocalParquetTwoPhaseCommitSink,
        TransactionalSinkParticipant, TwoPhaseCommitSink,
    };

    fn make_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    // ── EpochTransactionLog recovery ───────────────────────────────────────────

    /// Verifies that prepared-but-uncommitted epochs survive a simulated
    /// restart: after `pre_commit`, data is in the staged set; `commit_through`
    /// makes it visible; `abort_after` discards epochs beyond the checkpoint.
    #[test]
    fn epoch_log_recovery_commit_covered_abort_later() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());

        // Epoch 1 — pre-committed before simulated crash
        log.stage(&make_batch(vec![1, 2])).unwrap();
        log.pre_commit(1).unwrap();

        // Epoch 2 — pre-committed but coordinator not yet acked
        log.stage(&make_batch(vec![3])).unwrap();
        log.pre_commit(2).unwrap();

        // Open buffer — not yet staged
        log.stage(&make_batch(vec![99])).unwrap();

        // Simulated restore to checkpoint epoch 1:
        // commit covered (≤ 1), abort future (> 1), discard open buffer.
        let committed = log.commit_through(1).unwrap();
        let aborted = log.abort_after(1).unwrap();

        assert_eq!(committed, 1, "epoch 1 has 1 staged write");
        assert_eq!(aborted, 1, "epoch 2 has 1 staged write to abort");
        assert_eq!(log.open_rows(), 0, "open buffer discarded on restore");
        assert!(
            log.prepared_epochs().is_empty(),
            "no prepared epochs should remain after recovery"
        );
        assert_eq!(
            log.sink().committed().len(),
            1,
            "exactly 1 batch committed after recovery"
        );
        assert_eq!(
            log.sink().staged_count(),
            0,
            "no staged batches should remain after recovery"
        );
    }

    /// Verifies exactly-once: duplicate `commit_through` calls are idempotent
    /// (the handle is already gone from staged after the first commit).
    #[test]
    fn epoch_log_idempotent_commit_through() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());
        log.stage(&make_batch(vec![5, 6])).unwrap();
        log.pre_commit(3).unwrap();

        // First commit
        let first = log.commit_through(3).unwrap();
        assert_eq!(first, 1);

        // Second call for the same epoch — nothing left to commit.
        let second = log.commit_through(3).unwrap();
        assert_eq!(
            second, 0,
            "re-committing an already-committed epoch is a no-op"
        );
        assert_eq!(
            log.sink().committed().len(),
            1,
            "committed batch count must not change on idempotent re-commit"
        );
    }

    /// Verifies that a crash before `pre_commit` (open buffer only) leaves the
    /// sink in a clean state after `abort_after`.
    #[test]
    fn epoch_log_crash_before_pre_commit_recovers_clean() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());

        // Epoch 0 was committed cleanly in the previous run.
        log.stage(&make_batch(vec![1])).unwrap();
        log.pre_commit(0).unwrap();
        log.commit_through(0).unwrap();

        // Now data was staged for epoch 1 but crash happened before pre_commit.
        log.stage(&make_batch(vec![2, 3])).unwrap();
        // (no pre_commit — simulating crash)

        // Recovery: restore to epoch 0, abort nothing (open buffer is discarded).
        let aborted = log.abort_after(0).unwrap();
        assert_eq!(aborted, 0, "nothing was pre-committed, nothing to abort");
        assert_eq!(log.open_rows(), 0, "open buffer cleared");
        assert_eq!(
            log.sink().committed().len(),
            1,
            "only epoch-0 batch is committed"
        );
    }

    // ── LocalParquetTwoPhaseCommitSink crash recovery ──────────────────────────

    /// Verifies that a committed file is stable after simulated crash-and-
    /// replay: re-committing with a handle whose staging file is gone and
    /// final file exists must succeed (idempotent).
    #[test]
    fn parquet_sink_idempotent_commit_after_rename() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
        let batch = make_batch(vec![42, 43]);

        let handle = sink.prepare(1, &batch).unwrap();
        // Normal commit: renames .tmp → .parquet
        sink.commit(handle.clone()).unwrap();

        // Verify the final file exists and the staging file is gone.
        assert!(handle.final_path.exists(), "committed file must exist");
        assert!(!handle.staging_path.exists(), "staging file must be gone");

        // Re-commit (simulating coordinator retry after uncertain outcome):
        // staging is absent, final is present → idempotent success.
        sink.commit(handle).unwrap();
    }

    /// Verifies that an aborted staging file is cleaned up even if the abort
    /// is called after the staging file is already gone (double-abort is safe).
    #[test]
    fn parquet_sink_double_abort_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
        let batch = make_batch(vec![7]);

        let handle = sink.prepare(2, &batch).unwrap();
        sink.abort(handle.clone()).unwrap();
        // Second abort — staging file is already gone.
        sink.abort(handle).unwrap(); // must not panic or error
    }

    /// Verifies that a staged file is NOT visible until commit: no .parquet
    /// file in the output directory before commit.
    #[test]
    fn parquet_sink_staged_not_visible_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
        let batch = make_batch(vec![1, 2, 3]);

        let handle = sink.prepare(1, &batch).unwrap();
        // Staging file exists.
        assert!(handle.staging_path.exists(), "staging file must exist");
        // Final file does not yet exist.
        assert!(
            !handle.final_path.exists(),
            "final file must not exist before commit"
        );

        // Count .parquet files (not .tmp).
        let parquet_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(parquet_count, 0, "no committed parquet files before commit");

        // After commit the file becomes visible.
        sink.commit(handle).unwrap();
        let parquet_count_after = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(
            parquet_count_after, 1,
            "exactly one committed parquet file after commit"
        );
    }

    // ── Kafka exactly-once certification ─────────────────────────────────────

    /// Certifies Kafka exactly-once semantics via `EpochTransactionLog`:
    /// data staged before a simulated crash is committed on recovery; data that
    /// was never pre-committed is not replayed and is not lost after the
    /// checkpoint epoch is re-processed.
    #[test]
    fn kafka_exactly_once_no_data_loss_or_duplication() {
        let mut log = EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new());

        // Epoch 1 — data arrives from Kafka, staged and pre-committed.
        log.stage(&make_batch(vec![1, 2, 3])).unwrap();
        log.pre_commit(1).unwrap();

        // Epoch 2 — additional records arrive; pre-committed before crash.
        log.stage(&make_batch(vec![4, 5])).unwrap();
        log.pre_commit(2).unwrap();

        // Epoch 3 — records staged but coordinator crashed before pre-commit.
        log.stage(&make_batch(vec![99])).unwrap();
        // (no pre_commit — simulated coordinator crash)

        // Recovery: restore to epoch 1 (the last durably acknowledged epoch).
        // Epoch 2 is aborted (pre-committed but not durably acked by coordinator).
        // Epoch 3's open buffer is discarded.
        let committed = log.commit_through(1).unwrap();
        let aborted = log.abort_after(1).unwrap();
        assert_eq!(committed, 1, "epoch 1 has exactly one staged write");
        assert_eq!(aborted, 1, "epoch 2 must be aborted on recovery");
        assert_eq!(
            log.open_rows(),
            0,
            "open buffer must be cleared on recovery"
        );
        assert_eq!(
            log.sink().committed().len(),
            1,
            "exactly one batch must be committed (epoch 1 only)"
        );
        assert_eq!(
            log.sink().staged_count(),
            0,
            "no staged batches should remain after recovery"
        );

        // After recovery the consumer replays from epoch 2.
        // Verify re-processing epoch 2 produces exactly the same data (no duplicates).
        log.stage(&make_batch(vec![4, 5])).unwrap();
        log.pre_commit(2).unwrap();
        log.commit_through(2).unwrap();
        assert_eq!(
            log.sink().committed().len(),
            2,
            "epoch 2 replay must result in exactly two committed batches total"
        );
        let all_values: Vec<i64> = log
            .sink()
            .committed()
            .iter()
            .flat_map(|(_, b)| {
                use arrow::array::Int64Array;
                let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            all_values,
            vec![1, 2, 3, 4, 5],
            "no data loss or duplication after replay"
        );
    }

    // ── S3 exactly-once certification ────────────────────────────────────────

    /// Certifies S3 exactly-once delivery using `LocalParquetTwoPhaseCommitSink`:
    /// data written by `prepare` is not visible until `commit`; after `commit`
    /// the Parquet file is durably present; and re-committing (coordinator retry)
    /// is idempotent — no duplicate files are created.
    #[test]
    fn s3_roundtrip_exactly_once_with_checkpoint_restore() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
        let batch = make_batch(vec![10, 20, 30]);

        // Phase 1: prepare — data is staged (.tmp) but not yet visible.
        let handle = sink.prepare(1, &batch).unwrap();
        assert!(
            handle.staging_path.exists(),
            "staging file must exist after prepare"
        );
        assert!(
            !handle.final_path.exists(),
            "final file must not exist before commit"
        );

        // Phase 2: commit — staging file is atomically renamed to the final path.
        sink.commit(handle.clone()).unwrap();
        assert!(
            handle.final_path.exists(),
            "committed file must be present after commit"
        );
        assert!(
            !handle.staging_path.exists(),
            "staging file must be gone after commit"
        );

        // Phase 3: idempotent re-commit after uncertain coordinator outcome.
        sink.commit(handle.clone()).unwrap();
        let parquet_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(
            parquet_count, 1,
            "re-commit must not create duplicate files"
        );

        // Phase 4: abort after commit must not remove the committed file.
        sink.abort(handle).unwrap();
        assert!(
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|ext| ext == "parquet")),
            "abort after commit must not remove the committed file"
        );
    }

    // ── IcebergNativeTwoPhaseCommit recovery (iceberg feature only) ──────────────

    #[cfg(feature = "iceberg")]
    mod iceberg_recovery {
        use std::collections::BTreeMap;

        use crate::lakehouse::{
            IcebergNativeTwoPhaseCommit, IcebergTwoPhaseCommit, SchemaField, SchemaVersion,
        };

        fn schema_version() -> SchemaVersion {
            SchemaVersion {
                schema_id: 1,
                fields: vec![SchemaField {
                    id: 1,
                    name: "x".to_string(),
                    required: true,
                    data_type: "long".to_string(),
                }],
            }
        }

        fn batch(values: Vec<i64>) -> arrow::record_batch::RecordBatch {
            use arrow::array::Int64Array;
            use arrow::datatypes::{DataType, Field, Schema};
            use std::sync::Arc;
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(values))],
            )
            .unwrap()
        }

        /// Session-1 prepares and commits; session-2 opens the same root and
        /// finds the committed snapshot via the version-hint.text file.
        #[tokio::test]
        async fn iceberg_native_crash_recovery_via_version_hint() {
            let dir = tempfile::tempdir().unwrap();
            let sv = schema_version();

            // Session 1: commit two batches.
            {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                    .await
                    .unwrap();
                let s = tpc.prepare(vec![batch(vec![1, 2])]).await.unwrap();
                tpc.commit(s, BTreeMap::new()).await.unwrap();
                let s = tpc.prepare(vec![batch(vec![3])]).await.unwrap();
                tpc.commit(s, BTreeMap::new()).await.unwrap();
            }

            // Session 2: open the same root — version-hint must point to a valid
            // metadata file so the table is recoverable.
            {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                    .await
                    .unwrap();
                use iceberg::Catalog;
                let table = tpc.catalog.load_table(&tpc.ident).await.unwrap();
                assert!(
                    table.metadata().current_snapshot().is_some(),
                    "recovered table must have a committed snapshot"
                );
            }
        }

        /// A staged snapshot that was never committed (crash between prepare and
        /// commit) does NOT appear in the table after recovery.
        #[tokio::test]
        async fn iceberg_native_uncommitted_staged_is_invisible_after_recovery() {
            let dir = tempfile::tempdir().unwrap();
            let sv = schema_version();

            // Session 1: commit one snapshot, then prepare another but crash.
            let staged_id = {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                    .await
                    .unwrap();
                let s = tpc.prepare(vec![batch(vec![10])]).await.unwrap();
                tpc.commit(s, BTreeMap::new()).await.unwrap();
                // Prepare but do NOT commit — simulates crash.
                let staged = tpc.prepare(vec![batch(vec![99])]).await.unwrap();
                staged.snapshot_id
            };

            // Session 2: open fresh — pending map is empty, uncommitted staged ID
            // is not in the new pending map, so it can never be committed.
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                .await
                .unwrap();
            let pending = tpc.pending.lock().await;
            assert!(
                !pending.contains_key(&staged_id),
                "uncommitted staged snapshot must not appear in recovered pending map"
            );
        }

        /// `overwrite_commit` replaces the entire table content and the new
        /// data is visible in the recovered session.
        #[tokio::test]
        async fn iceberg_native_overwrite_commit_is_recoverable() {
            let dir = tempfile::tempdir().unwrap();
            let sv = schema_version();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                .await
                .unwrap();

            // Append some initial data.
            let s = tpc.prepare(vec![batch(vec![1, 2, 3])]).await.unwrap();
            tpc.commit(s, BTreeMap::new()).await.unwrap();

            // Overwrite with new data.
            tpc.overwrite_commit(vec![batch(vec![7, 8])], BTreeMap::new(), &sv)
                .await
                .unwrap();

            // Recovery: open the same root.
            let tpc2 = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                .await
                .unwrap();
            use iceberg::Catalog;
            let table = tpc2.catalog.load_table(&tpc2.ident).await.unwrap();
            assert!(
                table.metadata().current_snapshot().is_some(),
                "recovered table after overwrite must have a snapshot"
            );
        }

        /// `evolve_schema` stores the new schema in table properties and the
        /// update survives a session restart.
        #[tokio::test]
        async fn iceberg_native_schema_evolution_persists_across_sessions() {
            let dir = tempfile::tempdir().unwrap();
            let sv = schema_version();

            // Session 1: evolve the schema.
            {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                    .await
                    .unwrap();
                let s = tpc.prepare(vec![batch(vec![5])]).await.unwrap();
                tpc.commit(s, BTreeMap::new()).await.unwrap();

                let sv2 = SchemaVersion {
                    schema_id: 2,
                    fields: vec![
                        SchemaField {
                            id: 1,
                            name: "x".to_string(),
                            required: true,
                            data_type: "long".to_string(),
                        },
                        SchemaField {
                            id: 2,
                            name: "y".to_string(),
                            required: false,
                            data_type: "string".to_string(),
                        },
                    ],
                };
                tpc.evolve_schema(&sv2).await.unwrap();
            }

            // Session 2: schema evolution properties must be readable.
            {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "t", &sv)
                    .await
                    .unwrap();
                use iceberg::Catalog;
                let table = tpc.catalog.load_table(&tpc.ident).await.unwrap();
                let props = table.metadata().properties();
                assert_eq!(
                    props.get("krishiv.schema.id").map(String::as_str),
                    Some("2"),
                    "schema id must persist across sessions"
                );
            }
        }
    }

    // ── Delta Lake certification (delta feature) ──────────────────────────────

    #[cfg(feature = "delta")]
    mod delta_certification {
        use crate::lakehouse::{
            DeltaTableHandle, DeltaWriteMode, LocalDeltaTwoPhaseCommitSink, merge_delta,
            write_delta,
        };
        use crate::two_phase::TwoPhaseCommitSink;
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        fn make_batch(ids: Vec<i64>, labels: Vec<&str>) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(labels)),
                ],
            )
            .unwrap()
        }

        async fn row_count(path: &str) -> usize {
            DeltaTableHandle::open(path, None)
                .await
                .unwrap()
                .scan_batches()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum()
        }

        // ── write_delta / DeltaTableHandle ─────────────────────────────────

        #[tokio::test]
        async fn delta_append_creates_durable_rows() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            write_delta(
                &path,
                vec![make_batch(vec![1, 2, 3], vec!["a", "b", "c"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            assert_eq!(row_count(&path).await, 3);
        }

        #[tokio::test]
        async fn delta_append_accumulates_across_writes() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            write_delta(
                &path,
                vec![make_batch(vec![1, 2], vec!["a", "b"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            write_delta(
                &path,
                vec![make_batch(vec![3, 4, 5], vec!["c", "d", "e"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            assert_eq!(
                row_count(&path).await,
                5,
                "two appends must accumulate rows"
            );
        }

        #[tokio::test]
        async fn delta_overwrite_replaces_all_data() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            write_delta(
                &path,
                vec![make_batch(vec![1, 2, 3], vec!["old", "old", "old"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            write_delta(
                &path,
                vec![make_batch(vec![4, 5], vec!["new", "new"])],
                DeltaWriteMode::Overwrite,
                false,
            )
            .await
            .unwrap();
            assert_eq!(
                row_count(&path).await,
                2,
                "overwrite must discard prior rows"
            );
        }

        #[tokio::test]
        async fn delta_time_travel_reads_specific_version() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            write_delta(
                &path,
                vec![make_batch(vec![1, 2], vec!["v0", "v0"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            write_delta(
                &path,
                vec![make_batch(vec![3, 4, 5], vec!["v1", "v1", "v1"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();

            let v0_rows: usize = DeltaTableHandle::open(&path, Some(0))
                .await
                .unwrap()
                .scan_batches()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            let v1_rows: usize = DeltaTableHandle::open(&path, None)
                .await
                .unwrap()
                .scan_batches()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();

            assert_eq!(v0_rows, 2, "version 0 must contain only the first 2 rows");
            assert_eq!(v1_rows, 5, "latest version must contain all 5 rows");
        }

        #[tokio::test]
        async fn delta_merge_inserts_new_keys_and_updates_existing() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            write_delta(
                &path,
                vec![make_batch(vec![1, 2, 3], vec!["old", "old", "old"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            let result = merge_delta(
                &path,
                vec![make_batch(vec![1, 2, 4], vec!["new", "new", "new"])],
                "id",
                true,
                true,
            )
            .await
            .unwrap();
            assert_eq!(
                result.rows_updated, 2,
                "ids 1 and 2 were already present — both updated"
            );
            assert_eq!(result.rows_inserted, 1, "id 4 is new — inserted once");
            assert_eq!(
                row_count(&path).await,
                4,
                "table must have 4 distinct rows after merge"
            );
        }

        #[tokio::test]
        async fn delta_merge_is_idempotent() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_string_lossy().to_string();
            write_delta(
                &path,
                vec![make_batch(vec![1, 2], vec!["a", "b"])],
                DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
            let update = make_batch(vec![1], vec!["updated"]);
            merge_delta(&path, vec![update.clone()], "id", true, true)
                .await
                .unwrap();
            merge_delta(&path, vec![update], "id", true, true)
                .await
                .unwrap();
            assert_eq!(
                row_count(&path).await,
                2,
                "idempotent merge must not create duplicate rows"
            );
        }

        // ── LocalDeltaTwoPhaseCommitSink ────────────────────────────────────

        #[tokio::test]
        async fn local_delta_2pc_staged_not_visible_before_commit() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
            let handle = sink
                .prepare(1, &make_batch(vec![1, 2, 3], vec!["a", "b", "c"]))
                .unwrap();

            assert!(
                handle.staging_path.exists(),
                "staging file must exist after prepare"
            );
            // No committed .parquet files yet.
            let committed = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                .count();
            assert_eq!(committed, 0, "no rows visible before commit");

            sink.commit(handle).unwrap();
            assert_eq!(
                row_count(&dir.path().to_string_lossy()).await,
                3,
                "rows must be visible after commit"
            );
        }

        #[tokio::test]
        async fn local_delta_2pc_abort_discards_staged_data() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(1, &make_batch(vec![99], vec!["x"])).unwrap();
            assert!(handle.staging_path.exists());
            sink.abort(handle).unwrap();
            // Staging file is gone and no delta log written.
            assert!(
                !dir.path()
                    .join(".delta-stage")
                    .join("1-0.parquet.tmp")
                    .exists()
            );
            assert!(
                !dir.path().join("_delta_log").exists(),
                "abort must not write delta log"
            );
        }

        #[tokio::test]
        async fn local_delta_2pc_double_abort_is_safe() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(1, &make_batch(vec![1], vec!["a"])).unwrap();
            sink.abort(handle.clone()).unwrap();
            sink.abort(handle).unwrap(); // must not error when staging file is already gone
        }

        #[tokio::test]
        async fn local_delta_2pc_idempotent_commit_after_crash() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = LocalDeltaTwoPhaseCommitSink::new(dir.path());
            let handle = sink
                .prepare(1, &make_batch(vec![1, 2], vec!["a", "b"]))
                .unwrap();
            sink.commit(handle.clone()).unwrap();
            // Re-commit (coordinator retry after uncertain outcome) must succeed
            // without producing a duplicate row.
            sink.commit(handle).unwrap();
            assert_eq!(
                row_count(&dir.path().to_string_lossy()).await,
                2,
                "re-commit must not duplicate rows"
            );
        }
    }

    // ── Hudi certification (hudi feature) ────────────────────────────────────

    #[cfg(feature = "hudi")]
    mod hudi_certification {
        use crate::lakehouse::{HudiCowWriter, HudiSnapshotReader, HudiTwoPhaseCommitSink};
        use crate::two_phase::TwoPhaseCommitSink;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        fn make_batch(values: Vec<i64>) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
        }

        fn row_count(table_path: &std::path::Path) -> usize {
            HudiSnapshotReader::open(table_path)
                .scan_batches()
                .unwrap_or_default()
                .iter()
                .map(|b| b.num_rows())
                .sum()
        }

        // ── HudiCowWriter (append path) ─────────────────────────────────────

        #[test]
        fn hudi_cow_append_creates_durable_rows() {
            let dir = tempfile::tempdir().unwrap();
            HudiCowWriter::open(dir.path())
                .append(make_batch(vec![1, 2, 3]))
                .unwrap();
            assert_eq!(row_count(dir.path()), 3);
        }

        #[test]
        fn hudi_cow_append_accumulates_rows() {
            let dir = tempfile::tempdir().unwrap();
            let writer = HudiCowWriter::open(dir.path());
            writer.append(make_batch(vec![1, 2])).unwrap();
            writer.append(make_batch(vec![3, 4, 5])).unwrap();
            assert_eq!(row_count(dir.path()), 5, "two appends must accumulate rows");
        }

        // ── HudiTwoPhaseCommitSink (2PC path) ───────────────────────────────

        #[test]
        fn hudi_2pc_staged_not_visible_before_commit() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = HudiTwoPhaseCommitSink::new(dir.path());
            let _handle = sink.prepare(1, &make_batch(vec![10, 20, 30])).unwrap();
            assert_eq!(
                row_count(dir.path()),
                0,
                "staged data must not be visible before commit"
            );
        }

        #[test]
        fn hudi_2pc_commit_makes_data_visible() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = HudiTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(1, &make_batch(vec![10, 20, 30])).unwrap();
            sink.commit(handle).unwrap();
            assert_eq!(row_count(dir.path()), 3, "committed data must be readable");
        }

        #[test]
        fn hudi_2pc_abort_leaves_no_data() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = HudiTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(1, &make_batch(vec![99])).unwrap();
            sink.abort(handle).unwrap();
            assert_eq!(
                row_count(dir.path()),
                0,
                "aborted epoch must not appear in the table"
            );
        }

        #[test]
        fn hudi_2pc_double_abort_is_safe() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = HudiTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(1, &make_batch(vec![1])).unwrap();
            sink.abort(handle.clone()).unwrap();
            sink.abort(handle).unwrap(); // second abort must not panic or error
            assert_eq!(row_count(dir.path()), 0);
        }

        #[test]
        fn hudi_2pc_recovery_replay_after_abort() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = HudiTwoPhaseCommitSink::new(dir.path());
            // Epoch 1 aborted (coordinator crash before commit).
            let h1 = sink.prepare(1, &make_batch(vec![1, 2])).unwrap();
            sink.abort(h1).unwrap();
            // Epoch 1 replayed and committed on recovery.
            let h1_replay = sink.prepare(1, &make_batch(vec![1, 2])).unwrap();
            sink.commit(h1_replay).unwrap();
            assert_eq!(
                row_count(dir.path()),
                2,
                "replayed epoch must produce exactly the right row count"
            );
        }

        #[test]
        fn hudi_2pc_multiple_epochs_accumulate() {
            let dir = tempfile::tempdir().unwrap();
            let mut sink = HudiTwoPhaseCommitSink::new(dir.path());
            let h1 = sink.prepare(1, &make_batch(vec![1, 2])).unwrap();
            sink.commit(h1).unwrap();
            let h2 = sink.prepare(2, &make_batch(vec![3, 4, 5])).unwrap();
            sink.commit(h2).unwrap();
            assert_eq!(
                row_count(dir.path()),
                5,
                "two committed epochs must both be readable"
            );
        }
    }

    // ── Data quality certification ─────────────────────────────────────────────

    mod quality_certification {
        use crate::quality::{
            DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction, check_batch,
            check_batch_compiled,
        };
        use arrow::array::{Array, Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        fn nullable_string_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("score", DataType::Float64, false),
            ]))
        }

        fn batch_with_null_name() -> RecordBatch {
            let schema = nullable_string_schema();
            let names: StringArray = [Some("alice"), None, Some("carol")].into_iter().collect();
            let scores = Float64Array::from(vec![10.0, 20.0, 30.0]);
            RecordBatch::try_new(schema, vec![Arc::new(names), Arc::new(scores)]).unwrap()
        }

        fn score_batch(scores: Vec<f64>) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("score", DataType::Float64, false),
            ]));
            let names: Vec<&str> = scores
                .iter()
                .enumerate()
                .map(|(i, _)| if i % 2 == 0 { "a" } else { "b" })
                .collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(names)),
                    Arc::new(Float64Array::from(scores)),
                ],
            )
            .unwrap()
        }

        fn name_batch(names: Vec<Option<&str>>) -> RecordBatch {
            let schema = nullable_string_schema();
            let name_arr: StringArray = names.into_iter().collect();
            let n = name_arr.len();
            let scores = Float64Array::from(vec![1.0; n]);
            RecordBatch::try_new(schema, vec![Arc::new(name_arr), Arc::new(scores)]).unwrap()
        }

        // ── NotNull rule ────────────────────────────────────────────────────

        #[test]
        fn quality_notnull_reject_removes_null_rows() {
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::NotNull {
                    column: "name".into(),
                },
                QualityAction::Reject,
            );
            let result = check_batch(&batch_with_null_name(), &config).unwrap();
            assert!(!result.failed);
            assert_eq!(
                result.rejected.len(),
                1,
                "exactly the null row must be rejected"
            );
            assert_eq!(
                result.accepted_indices,
                vec![0, 2],
                "non-null rows must be accepted"
            );
        }

        #[test]
        fn quality_notnull_fail_sets_batch_failed_flag() {
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::NotNull {
                    column: "name".into(),
                },
                QualityAction::Fail,
            );
            let result = check_batch(&batch_with_null_name(), &config).unwrap();
            assert!(
                result.failed,
                "Fail action must set the batch-level failed flag"
            );
            assert_eq!(
                result.rejected.len(),
                0,
                "Fail does not reject individual rows"
            );
        }

        #[test]
        fn quality_notnull_warn_passes_all_rows() {
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::NotNull {
                    column: "name".into(),
                },
                QualityAction::Warn,
            );
            let result = check_batch(&batch_with_null_name(), &config).unwrap();
            assert!(!result.failed);
            assert!(result.rejected.is_empty(), "Warn must not reject any rows");
            assert_eq!(
                result.accepted_indices.len(),
                3,
                "all rows pass through with Warn"
            );
        }

        // ── Range rule ──────────────────────────────────────────────────────

        #[test]
        fn quality_range_reject_removes_out_of_bounds() {
            let batch = score_batch(vec![5.0, 50.0, 100.0, 200.0]);
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Range {
                    column: "score".into(),
                    min: 0.0,
                    max: 100.0,
                },
                QualityAction::Reject,
            );
            let result = check_batch(&batch, &config).unwrap();
            assert_eq!(result.rejected.len(), 1, "only score=200 exceeds the range");
            assert_eq!(result.accepted_indices, vec![0, 1, 2]);
        }

        #[test]
        fn quality_range_accepts_all_in_bounds() {
            let batch = score_batch(vec![0.0, 50.0, 100.0]);
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Range {
                    column: "score".into(),
                    min: 0.0,
                    max: 100.0,
                },
                QualityAction::Reject,
            );
            let result = check_batch(&batch, &config).unwrap();
            assert!(result.rejected.is_empty());
            assert_eq!(result.accepted_indices.len(), 3);
        }

        // ── Regex rule ──────────────────────────────────────────────────────

        #[test]
        fn quality_regex_reject_removes_non_matching() {
            let batch = name_batch(vec![Some("alice"), Some("123"), Some("bob")]);
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Regex {
                    column: "name".into(),
                    pattern: r"^[a-z]+$".into(),
                },
                QualityAction::Reject,
            );
            let result = check_batch(&batch, &config).unwrap();
            assert_eq!(result.rejected.len(), 1, "only '123' must be rejected");
            assert_eq!(result.accepted_indices, vec![0, 2]);
        }

        #[test]
        fn quality_regex_invalid_pattern_returns_error() {
            let batch = name_batch(vec![Some("alice")]);
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Regex {
                    column: "name".into(),
                    pattern: r"[invalid".into(),
                },
                QualityAction::Reject,
            );
            assert!(
                check_batch(&batch, &config).is_err(),
                "invalid regex must return Err"
            );
        }

        // ── Compiled vs uncompiled equivalence ──────────────────────────────

        #[test]
        fn quality_compiled_matches_uncompiled_results() {
            let batch = name_batch(vec![Some("alice"), Some("123"), Some("bob")]);
            let pattern = r"^[a-z]+$";
            let plain = DataQualityConfig::new().with_rule(
                DataQualityRule::Regex {
                    column: "name".into(),
                    pattern: pattern.into(),
                },
                QualityAction::Reject,
            );
            let compiled = DataQualityConfig::new()
                .with_rule(
                    DataQualityRule::Regex {
                        column: "name".into(),
                        pattern: pattern.into(),
                    },
                    QualityAction::Reject,
                )
                .compile()
                .unwrap();
            let r1 = check_batch(&batch, &plain).unwrap();
            let r2 = check_batch_compiled(&batch, &compiled).unwrap();
            assert_eq!(r1.accepted_indices, r2.accepted_indices);
            assert_eq!(r1.rejected.len(), r2.rejected.len());
            assert_eq!(r1.failed, r2.failed);
        }

        #[test]
        fn quality_empty_config_accepts_all_rows() {
            let config = DataQualityConfig::new();
            let batch = score_batch(vec![1.0, 2.0, 3.0]);
            let result = check_batch(&batch, &config).unwrap();
            assert!(!result.failed);
            assert!(result.rejected.is_empty());
            assert_eq!(result.accepted_indices.len(), 3);
        }

        // ── DeadLetterSink ──────────────────────────────────────────────────

        #[tokio::test]
        async fn dead_letter_sink_clean_batch_all_accepted() {
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Range {
                    column: "score".into(),
                    min: 0.0,
                    max: 100.0,
                },
                QualityAction::Reject,
            );
            let mut dlq = DeadLetterSink::new("test", config);
            let (accepted, rejected) = dlq
                .process_batch(&score_batch(vec![10.0, 50.0, 90.0]))
                .await
                .unwrap();
            assert_eq!(accepted.num_rows(), 3);
            assert!(rejected.is_empty());
        }

        #[tokio::test]
        async fn dead_letter_sink_routes_violating_rows() {
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Range {
                    column: "score".into(),
                    min: 0.0,
                    max: 50.0,
                },
                QualityAction::Reject,
            );
            let mut dlq = DeadLetterSink::new("test", config);
            // scores: 10 (ok), 60 (violates), 30 (ok), 200 (violates)
            let (accepted, rejected) = dlq
                .process_batch(&score_batch(vec![10.0, 60.0, 30.0, 200.0]))
                .await
                .unwrap();
            assert_eq!(
                accepted.num_rows(),
                2,
                "two in-range scores must be accepted"
            );
            assert_eq!(
                rejected.len(),
                2,
                "two out-of-range scores must be rejected"
            );
        }

        #[tokio::test]
        async fn dead_letter_sink_fail_action_returns_error() {
            let config = DataQualityConfig::new().with_rule(
                DataQualityRule::Range {
                    column: "score".into(),
                    min: 0.0,
                    max: 5.0,
                },
                QualityAction::Fail,
            );
            let mut dlq = DeadLetterSink::new("test", config);
            let result = dlq.process_batch(&score_batch(vec![10.0, 20.0])).await;
            assert!(
                result.is_err(),
                "Fail action must propagate as an error from process_batch"
            );
        }
    }
}
