//! Does `get_array_memory_size()` over-report a hash-partitioned batch?
//!
//! `ShufflePartitionOutput::size_bytes` is computed as the sum of
//! `RecordBatch::get_array_memory_size()` over a partition's batches, and
//! `krishiv-scheduler`'s `aqe.rs` sums those into the per-partition byte map
//! that decides reduce parallelism. So if the number is inflated, AQE does not
//! merely *report* wrong — it *plans* wrong.
//!
//! Suspicion: Arrow reports the size of the whole underlying buffer for an
//! array that shares or slices one. If `HashPartitioner` produces buckets that
//! share the input's buffers, then summing across N buckets counts the same
//! bytes N times — and the inflation grows with bucket count and with how much
//! of the row is variable-length. TPC-H q10 is the only SF100 query whose
//! shuffle carries wide strings (~152 B/row), it is AQE-coalesced to 47
//! partitions, and its observed `shuffle_bytes_written` reached **12.3 TB** for
//! a 100 GB dataset.
//!
//! This test asserts the *current* behaviour so the question is settled by
//! measurement.

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use krishiv_shuffle::HashPartitioner;
use std::sync::Arc;

fn customer_shaped_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        // c_comment averages ~73 bytes in TPC-H; this is the column that makes
        // q10's shuffle wide.
        Field::new("c_comment", DataType::Utf8, false),
    ]));
    let keys = Int64Array::from_iter_values((0..rows as i64).map(|i| i));
    let comments = StringArray::from_iter_values(
        (0..rows).map(|i| format!("{:0>73}", i)),
    );
    RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(comments)]).expect("batch")
}

#[test]
fn summing_partition_memory_size_reports_the_partitioned_batch_faithfully() {
    let rows = 10_000;
    let buckets = 47; // q10's AQE-coalesced partition count.
    let batch = customer_shaped_batch(rows);
    let whole = batch.get_array_memory_size();

    let parts = HashPartitioner::new("c_custkey", buckets)
        .partition(&batch)
        .expect("partition");
    let summed: usize = parts.iter().map(RecordBatch::get_array_memory_size).sum();
    let part_rows: usize = parts.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(part_rows, rows, "partitioning must preserve rows");

    let ratio = summed as f64 / whole as f64;
    println!(
        "whole={whole} B, summed over {buckets} buckets={summed} B, ratio={ratio:.2}x, \
         rows={part_rows}"
    );

    // The honest expectation: partitioning splits rows, so the summed size
    // should be within a small constant of the original (per-array overhead
    // times bucket count, plus offset buffers). A large ratio means the same
    // bytes are counted once per bucket, which is what would make AQE size a
    // stage from a number ~`buckets` times too big.
    assert!(
        ratio < 3.0,
        "summed partition size is {ratio:.2}x the whole batch across {buckets} buckets — \
         `size_bytes` is counting shared buffers repeatedly, and `aqe.rs` plans reduce \
         parallelism from exactly this number"
    );
}
