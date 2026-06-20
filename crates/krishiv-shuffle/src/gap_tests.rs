use crate::compression::{ShuffleCompression, parquet_writer_properties, partition_memory_bytes};
use crate::error::{MAX_SHUFFLE_TICKET_LEN, io_err, shuffle_read_lock, shuffle_write_lock};
use crate::error::{ShuffleError, ShuffleResult};
use crate::store::{PartitionId, ShufflePartition};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use parquet::schema::types::ColumnPath;

fn column_root() -> ColumnPath {
    ColumnPath::new(vec![])
}

fn make_partition(num_rows: usize) -> ShufflePartition {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let a: Int32Array = (0..num_rows as i32).collect();
    let b: StringArray = (0..num_rows).map(|i| Some(format!("val_{i}"))).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();
    ShufflePartition {
        id: PartitionId {
            job_id: "j".into(),
            stage_id: "s".into(),
            partition: 0,
        },
        schema,
        batches: vec![batch],
    }
}

#[test]
fn partition_memory_bytes_non_zero_for_populated_batch() {
    let partition = make_partition(100);
    let bytes = partition_memory_bytes(&partition);
    assert!(bytes > 0);
}

#[test]
fn partition_memory_bytes_scales_with_row_count() {
    let small = make_partition(10);
    let large = make_partition(1000);
    assert!(partition_memory_bytes(&large) > partition_memory_bytes(&small));
}

#[test]
fn partition_memory_bytes_zero_for_empty_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "j".into(),
            stage_id: "s".into(),
            partition: 0,
        },
        schema,
        batches: vec![],
    };
    assert_eq!(partition_memory_bytes(&partition), 0);
}

#[test]
fn parquet_writer_properties_none_compression() {
    let props = parquet_writer_properties(ShuffleCompression::None);
    assert_eq!(
        props.compression(&column_root()),
        parquet::basic::Compression::UNCOMPRESSED
    );
}

#[test]
fn parquet_writer_properties_lz4() {
    let props = parquet_writer_properties(ShuffleCompression::Lz4);
    assert_eq!(
        props.compression(&column_root()),
        parquet::basic::Compression::LZ4
    );
}

#[test]
fn parquet_writer_properties_zstd() {
    let props = parquet_writer_properties(ShuffleCompression::Zstd);
    assert!(matches!(
        props.compression(&column_root()),
        parquet::basic::Compression::ZSTD(_)
    ));
}

#[test]
fn max_shuffle_ticket_len_value() {
    assert_eq!(MAX_SHUFFLE_TICKET_LEN, 65_536);
}

#[test]
fn io_err_creates_io_variant() {
    let err = io_err("test failure");
    assert!(matches!(err, ShuffleError::Io(_)));
    assert!(err.to_string().contains("test failure"));
}

#[test]
fn shuffle_write_lock_happy_path() {
    let lock = RwLock::new(42u32);
    let guard = shuffle_write_lock(&lock).unwrap();
    assert_eq!(*guard, 42);
}

#[test]
fn shuffle_read_lock_happy_path() {
    let lock = RwLock::new(7u32);
    let guard = shuffle_read_lock(&lock).unwrap();
    assert_eq!(*guard, 7);
}

#[test]
fn shuffle_write_lock_poisoned() {
    let lock = Arc::new(RwLock::new(()));
    let lock_for_panic = lock.clone();
    let handle = std::thread::spawn(move || {
        let _g = lock_for_panic.write().unwrap();
        panic!("poison");
    });
    let _ = handle.join();
    let result = shuffle_write_lock(&lock);
    assert!(matches!(result, Err(ShuffleError::LockPoisoned)));
}

#[test]
fn shuffle_read_lock_poisoned() {
    let lock = Arc::new(RwLock::new(()));
    let lock_for_panic = lock.clone();
    let handle = std::thread::spawn(move || {
        let _g = lock_for_panic.write().unwrap();
        panic!("poison");
    });
    let _ = handle.join();
    let result = shuffle_read_lock(&lock);
    assert!(matches!(result, Err(ShuffleError::LockPoisoned)));
}

#[test]
fn shuffle_error_display_partition_not_found() {
    let err = ShuffleError::PartitionNotFound {
        path: "/tmp/data".into(),
    };
    assert_eq!(err.to_string(), "shuffle partition not found: /tmp/data");
}

#[test]
fn shuffle_error_display_partition_not_available() {
    let err = ShuffleError::PartitionNotAvailable {
        path: "/tmp/data".into(),
    };
    assert_eq!(
        err.to_string(),
        "shuffle partition not available: /tmp/data"
    );
}

#[test]
fn shuffle_error_display_stale_lease_token() {
    let err = ShuffleError::StaleLeaseToken {
        expected: 1,
        actual: 2,
    };
    assert_eq!(
        err.to_string(),
        "stale shuffle lease token: expected 1, actual 2"
    );
}

#[test]
fn shuffle_error_display_not_found() {
    let err = ShuffleError::NotFound {
        path: "/obj/file".into(),
    };
    assert_eq!(err.to_string(), "shuffle path not found: /obj/file");
}

#[test]
fn shuffle_error_display_too_many_partitions() {
    let err = ShuffleError::TooManyPartitions { limit: 100 };
    assert_eq!(
        err.to_string(),
        "shuffle partition limit exceeded: max 100 partitions"
    );
}

#[test]
fn shuffle_error_display_memory_limit_exceeded() {
    let err = ShuffleError::MemoryLimitExceeded {
        max_bytes: 1000,
        current_bytes: 800,
        incoming_bytes: 300,
    };
    let msg = err.to_string();
    assert!(msg.contains("1000"));
    assert!(msg.contains("800"));
    assert!(msg.contains("300"));
}

#[test]
fn shuffle_error_display_lock_poisoned() {
    let err = ShuffleError::LockPoisoned;
    assert_eq!(err.to_string(), "shuffle lock poisoned");
}

#[test]
fn shuffle_error_display_type_mismatch() {
    let err = ShuffleError::TypeMismatch {
        expected: "Int32".into(),
    };
    assert_eq!(err.to_string(), "shuffle type mismatch: expected Int32");
}

#[test]
fn shuffle_error_display_invalid_partition_count() {
    let err = ShuffleError::InvalidPartitionCount { buckets: 0 };
    assert_eq!(err.to_string(), "invalid shuffle partition count: 0");
}

#[test]
fn shuffle_error_display_content_hash_mismatch() {
    let err = ShuffleError::ContentHashMismatch {
        partition: "p0".into(),
        expected: "abc".into(),
        actual: "def".into(),
    };
    assert!(err.to_string().contains("p0"));
}

#[test]
fn shuffle_error_display_disk_full() {
    let source = std::io::Error::new(std::io::ErrorKind::Other, "no space");
    let err = ShuffleError::DiskFull {
        path: "/data".into(),
        source,
    };
    assert!(err.to_string().contains("/data"));
}

#[test]
fn shuffle_error_display_io() {
    let err = io_err("kaboom");
    let msg = err.to_string();
    assert!(msg.contains("shuffle I/O error"));
    assert!(msg.contains("kaboom"));
}

#[test]
fn shuffle_error_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ShuffleError>();
}

#[test]
fn shuffle_error_as_trait_object() {
    let err: Box<dyn std::error::Error> = Box::new(ShuffleError::LockPoisoned);
    assert_eq!(err.to_string(), "shuffle lock poisoned");
}

#[test]
fn shuffle_result_type_alias() {
    let ok: ShuffleResult<i32> = Ok(42);
    assert_eq!(ok.unwrap(), 42);
}

#[test]
fn partition_id_eq_and_hash() {
    let a = PartitionId {
        job_id: "j".into(),
        stage_id: "s".into(),
        partition: 0,
    };
    let b = PartitionId {
        job_id: "j".into(),
        stage_id: "s".into(),
        partition: 0,
    };
    assert_eq!(a, b);
    let mut map = HashMap::new();
    map.insert(a.clone(), 1);
    assert_eq!(map.get(&b), Some(&1));
}
