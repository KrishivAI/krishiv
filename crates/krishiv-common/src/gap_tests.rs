#[cfg(test)]
mod gap_tests {
    use crate::durability::*;
    use crate::production::*;
    use crate::write_commit::*;

    // ── production.rs ──────────────────────────────────────────────────────

    #[test]
    fn durable_window_state_single_node() {
        assert!(profile_requires_durable_window_state(
            DurabilityProfile::SingleNodeDurable
        ));
    }

    #[test]
    fn durable_window_state_distributed() {
        assert!(profile_requires_durable_window_state(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn durable_window_state_dev_local() {
        assert!(!profile_requires_durable_window_state(
            DurabilityProfile::DevLocal
        ));
    }

    #[test]
    fn file_backed_state_dev_local() {
        assert!(!requires_file_backed_state(DurabilityProfile::DevLocal));
    }

    #[test]
    fn file_backed_state_single_node() {
        assert!(requires_file_backed_state(
            DurabilityProfile::SingleNodeDurable
        ));
    }

    #[test]
    fn file_backed_state_distributed() {
        assert!(requires_file_backed_state(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn http_auth_dev_local() {
        assert!(!requires_http_auth(DurabilityProfile::DevLocal));
    }

    #[test]
    fn http_auth_single_node() {
        assert!(requires_http_auth(DurabilityProfile::SingleNodeDurable));
    }

    #[test]
    fn http_auth_distributed() {
        assert!(requires_http_auth(DurabilityProfile::DistributedDurable));
    }

    #[test]
    fn manual_kafka_commit_dev_local() {
        assert!(!requires_manual_kafka_commit(DurabilityProfile::DevLocal));
    }

    #[test]
    fn manual_kafka_commit_single_node() {
        assert!(requires_manual_kafka_commit(
            DurabilityProfile::SingleNodeDurable
        ));
    }

    #[test]
    fn manual_kafka_commit_distributed() {
        assert!(requires_manual_kafka_commit(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn memory_checkpoint_uri_dev_local() {
        assert!(allows_memory_checkpoint_uri(DurabilityProfile::DevLocal));
    }

    #[test]
    fn memory_checkpoint_uri_single_node() {
        assert!(!allows_memory_checkpoint_uri(
            DurabilityProfile::SingleNodeDurable
        ));
    }

    #[test]
    fn memory_checkpoint_uri_distributed() {
        assert!(!allows_memory_checkpoint_uri(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn unbounded_shuffle_store_dev_local() {
        assert!(allows_unbounded_shuffle_store(DurabilityProfile::DevLocal));
    }

    #[test]
    fn unbounded_shuffle_store_single_node() {
        assert!(!allows_unbounded_shuffle_store(
            DurabilityProfile::SingleNodeDurable
        ));
    }

    #[test]
    fn unbounded_shuffle_store_distributed() {
        assert!(!allows_unbounded_shuffle_store(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn alpha_api_dev_local() {
        assert!(allows_alpha_api());
    }

    #[test]
    fn remote_sql_comment_fallback_dev_local() {
        assert!(allows_remote_sql_comment_fallback());
    }

    #[test]
    fn anonymous_http_override_returns_bool() {
        let _ = allow_anonymous_http_override();
    }

    #[test]
    fn native_udf_policy_resolve_dev_local() {
        let policy = NativeScalarUdfPolicy::resolve(DurabilityProfile::DevLocal);
        assert_eq!(policy.profile(), DurabilityProfile::DevLocal);
        assert!(!policy.is_forbidden());
    }

    #[test]
    fn native_udf_policy_resolve_single_node() {
        let policy = NativeScalarUdfPolicy::resolve(DurabilityProfile::SingleNodeDurable);
        assert_eq!(policy.profile(), DurabilityProfile::SingleNodeDurable);
        assert!(policy.is_forbidden());
    }

    #[test]
    fn native_udf_policy_resolve_distributed() {
        let policy = NativeScalarUdfPolicy::resolve(DurabilityProfile::DistributedDurable);
        assert_eq!(policy.profile(), DurabilityProfile::DistributedDurable);
        assert!(policy.is_forbidden());
    }

    #[test]
    fn native_udf_policy_from_decision() {
        let policy = NativeScalarUdfPolicy::from_decision(DurabilityProfile::DevLocal, true);
        assert_eq!(policy.profile(), DurabilityProfile::DevLocal);
        assert!(policy.is_forbidden());
    }

    #[test]
    fn native_udf_policy_clone_copy() {
        let a = NativeScalarUdfPolicy::from_decision(DurabilityProfile::DevLocal, false);
        let b = a;
        assert_eq!(a, b);
    }

    #[test]
    fn native_udf_policy_debug() {
        let policy = NativeScalarUdfPolicy::resolve(DurabilityProfile::DevLocal);
        let _ = format!("{policy:?}");
    }

    #[test]
    fn forbids_native_scalar_udfs_dev_local() {
        assert!(!profile_forbids_native_scalar_udfs(
            DurabilityProfile::DevLocal
        ));
    }

    #[test]
    fn forbids_native_scalar_udfs_single_node() {
        assert!(profile_forbids_native_scalar_udfs(
            DurabilityProfile::SingleNodeDurable
        ));
    }

    #[test]
    fn forbids_native_scalar_udfs_distributed() {
        assert!(profile_forbids_native_scalar_udfs(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn authenticated_flight_delegates_to_http_auth() {
        assert_eq!(
            profile_requires_authenticated_flight(DurabilityProfile::DevLocal),
            requires_http_auth(DurabilityProfile::DevLocal)
        );
        assert_eq!(
            profile_requires_authenticated_flight(DurabilityProfile::SingleNodeDurable),
            requires_http_auth(DurabilityProfile::SingleNodeDurable)
        );
    }

    #[test]
    fn authenticated_ui_delegates_to_http_auth() {
        assert_eq!(
            profile_requires_authenticated_ui(DurabilityProfile::DevLocal),
            requires_http_auth(DurabilityProfile::DevLocal)
        );
        assert_eq!(
            profile_requires_authenticated_ui(DurabilityProfile::SingleNodeDurable),
            requires_http_auth(DurabilityProfile::SingleNodeDurable)
        );
    }

    // ── durability.rs ──────────────────────────────────────────────────────

    #[test]
    fn metadata_durability_traits() {
        let a = MetadataDurability::Memory;
        let b = a;
        assert_eq!(a, b);
        let c = MetadataDurability::LocalFile;
        assert_ne!(a, c);
        let _ = format!("{a:?}");
        let _ = std::hash::Hash::hash(&a, &mut std::collections::hash_map::DefaultHasher::new());
    }

    #[test]
    fn shuffle_durability_traits() {
        let a = ShuffleDurability::Memory;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(a, ShuffleDurability::LocalDisk);
        assert_ne!(a, ShuffleDurability::ObjectStore);
        assert_ne!(a, ShuffleDurability::Tiered);
        let _ = format!("{a:?}");
    }

    #[test]
    fn state_durability_traits() {
        let a = StateDurability::Memory;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(a, StateDurability::LocalRocksDb);
        assert_ne!(a, StateDurability::LocalRocksDbWithCheckpointRestore);
        let _ = format!("{a:?}");
    }

    #[test]
    fn checkpoint_durability_traits() {
        let a = CheckpointDurability::EphemeralLocal;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(a, CheckpointDurability::LocalFilesystem);
        assert_ne!(a, CheckpointDurability::ObjectStore);
        let _ = format!("{a:?}");
    }

    // ── write_commit.rs ────────────────────────────────────────────────────

    #[test]
    fn staged_file_name_basic() {
        assert_eq!(staged_file_name("task-3", 2), "task-3-2.parquet");
    }

    #[test]
    fn staged_file_name_zero_attempt() {
        assert_eq!(staged_file_name("task-0", 0), "task-0-0.parquet");
    }

    #[test]
    fn final_part_file_name_basic() {
        assert_eq!(final_part_file_name(5, "job-abc"), "part-5-job-abc.parquet");
    }

    #[test]
    fn final_part_file_name_zero_index() {
        assert_eq!(final_part_file_name(0, "job-1"), "part-0-job-1.parquet");
    }

    #[test]
    fn hive_partition_slice_fields() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let slice = HivePartitionSlice {
            hive_path: String::from("country=US"),
            batches: vec![batch],
        };
        assert_eq!(slice.hive_path, "country=US");
        assert_eq!(slice.batches.len(), 1);
        assert_eq!(slice.batches[0].num_rows(), 3);
    }

    #[test]
    fn hive_partition_slice_empty_path() {
        let slice = HivePartitionSlice {
            hive_path: String::new(),
            batches: vec![],
        };
        assert!(slice.hive_path.is_empty());
        assert!(slice.batches.is_empty());
    }

    #[test]
    fn hive_partition_slice_clone() {
        let slice = HivePartitionSlice {
            hive_path: String::from("a=1"),
            batches: vec![],
        };
        let cloned = slice.clone();
        assert_eq!(cloned.hive_path, "a=1");
    }

    #[test]
    fn publish_outcome_default() {
        let outcome = PublishOutcome::default();
        assert!(outcome.published.is_empty());
        assert_eq!(outcome.skipped_existing, 0);
        assert!(!outcome.ignored);
    }

    #[test]
    fn publish_outcome_fields() {
        let outcome = PublishOutcome {
            published: vec!["/a/b.parquet".into(), "/a/c.parquet".into()],
            skipped_existing: 3,
            ignored: true,
        };
        assert_eq!(outcome.published.len(), 2);
        assert_eq!(outcome.skipped_existing, 3);
        assert!(outcome.ignored);
    }

    #[test]
    fn publish_outcome_eq() {
        let a = PublishOutcome {
            published: vec![],
            skipped_existing: 1,
            ignored: false,
        };
        let b = PublishOutcome {
            published: vec![],
            skipped_existing: 1,
            ignored: false,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn publish_outcome_clone() {
        let a = PublishOutcome {
            published: vec!["x.parquet".into()],
            skipped_existing: 2,
            ignored: true,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn split_batches_empty_columns() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let slices = split_batches_by_partition_columns(&[batch], &[]).unwrap();
        assert_eq!(slices.len(), 1);
        assert!(slices[0].hive_path.is_empty());
        assert_eq!(slices[0].batches[0].num_rows(), 2);
    }

    #[test]
    fn split_batches_by_string_partition() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("country", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["US", "US", "UK"])),
            ],
        )
        .unwrap();
        let slices =
            split_batches_by_partition_columns(&[batch], &[String::from("country")]).unwrap();
        slices.iter().for_each(|s| {
            let total: usize = s.batches.iter().map(|b| b.num_rows()).sum();
            assert!(total > 0);
            assert!(s.hive_path.starts_with("country="));
        });
        let us = slices.iter().find(|s| s.hive_path == "country=US");
        let uk = slices.iter().find(|s| s.hive_path == "country=UK");
        assert!(us.is_some());
        assert!(uk.is_some());
        assert_eq!(us.unwrap().batches[0].num_rows(), 2);
        assert_eq!(uk.unwrap().batches[0].num_rows(), 1);
    }

    #[test]
    fn split_batches_missing_partition_column() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let result = split_batches_by_partition_columns(&[batch], &[String::from("missing")]);
        assert!(result.is_err());
    }

    #[test]
    fn split_batches_empty_batches() {
        let slices = split_batches_by_partition_columns(&[], &[String::from("col")]).unwrap();
        assert!(slices.is_empty());
    }

    #[test]
    fn split_batches_null_partition_value() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();
        let slices = split_batches_by_partition_columns(&[batch], &[String::from("tag")]).unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].hive_path, format!("tag={HIVE_DEFAULT_PARTITION}"));
    }

    #[test]
    fn split_batches_multiple_partitions() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("country", DataType::Utf8, false),
            Field::new("year", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["US", "US", "UK", "UK"])),
                Arc::new(StringArray::from(vec!["2024", "2025", "2024", "2024"])),
            ],
        )
        .unwrap();
        let slices = split_batches_by_partition_columns(
            &[batch],
            &[String::from("country"), String::from("year")],
        )
        .unwrap();
        assert!(slices.len() >= 3);
        for s in &slices {
            assert!(s.hive_path.contains("country="));
            assert!(s.hive_path.contains("year="));
            assert!(s.hive_path.contains('/'));
        }
    }
}
