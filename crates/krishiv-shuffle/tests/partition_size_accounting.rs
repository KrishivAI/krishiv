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

// These builders return `Result` rather than unwrapping internally. Clippy's
// `allow-expect-in-tests` exempts `#[test]` bodies but not plain helper
// functions in the same file, and `unwrap`/`expect`/`panic!` are all denied
// here — so the fallible step belongs at the call sites, which are exempt.
type BatchResult = Result<RecordBatch, arrow::error::ArrowError>;

fn customer_shaped_batch(rows: usize) -> BatchResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        // c_comment averages ~73 bytes in TPC-H; this is the column that makes
        // q10's shuffle wide.
        Field::new("c_comment", DataType::Utf8, false),
    ]));
    let keys = Int64Array::from_iter_values(0..rows as i64);
    let comments = StringArray::from_iter_values((0..rows).map(|i| format!("{i:0>73}")));
    RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(comments)])
}

/// The same batch, but with the string column as `Utf8View` — which is what
/// DataFusion 54 actually produces when it reads Parquet.
fn customer_shaped_view_batch(rows: usize) -> BatchResult {
    use arrow::array::StringViewArray;
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_comment", DataType::Utf8View, false),
    ]));
    let keys = Int64Array::from_iter_values(0..rows as i64);
    let comments = StringViewArray::from_iter_values((0..rows).map(|i| format!("{i:0>73}")));
    RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(comments)])
}

/// `take` on a view array copies the 16-byte *views*, not the data buffers —
/// every output partition keeps a reference to the SAME data buffers, and
/// `get_array_memory_size()` charges each of them the full buffer.
///
/// This is the q10-shaped case: it is the only SF100 query whose shuffle
/// carries wide strings, and it is AQE-coalesced to 47 partitions.
#[test]
fn utf8view_partitions_each_report_the_whole_shared_data_buffer() {
    let rows = 10_000;
    let buckets = 47;
    let batch = customer_shaped_view_batch(rows).expect("batch");
    let whole = krishiv_shuffle::logical_batch_bytes(&batch);

    let parts = HashPartitioner::new("c_custkey", buckets)
        .partition(&batch)
        .expect("partition");
    let arrow_summed: usize = parts.iter().map(RecordBatch::get_array_memory_size).sum();
    let summed: usize = parts
        .iter()
        .map(krishiv_shuffle::logical_batch_bytes)
        .sum();
    let ratio = summed as f64 / whole as f64;
    println!(
        "UTF8VIEW whole={whole} arrow_summed={arrow_summed} ({:.2}x) fixed_summed={summed} \
         ({ratio:.2}x) over {buckets} buckets",
        arrow_summed as f64 / whole as f64
    );

    assert!(
        ratio < 3.0,
        "Utf8View partitions sum to {ratio:.2}x the original: every bucket is charged the \
         whole shared data buffer. `size_bytes` feeds `aqe.rs` partition sizing, so this \
         inflates what AQE thinks a partition weighs by ~the bucket count"
    );
}

#[test]
fn summing_partition_memory_size_reports_the_partitioned_batch_faithfully() {
    let rows = 10_000;
    let buckets = 47; // q10's AQE-coalesced partition count.
    let batch = customer_shaped_batch(rows).expect("batch");
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

/// A bucket must OWN its bytes, not merely reference them.
///
/// This is the property behind every symptom in this file. `take` on a
/// `Utf8View` column copies the 16-byte views and leaves each bucket pointing
/// at the whole shared source data buffer, so the buckets collectively retain
/// N copies' worth of accounted memory for one copy of data. Measuring it with
/// `get_array_memory_size()` — the number the *rest of the engine* uses for
/// memory decisions, not our corrected one — is the point: `logical_batch_bytes`
/// reports the right answer either way, so it cannot detect this.
///
/// The consumers that break when this is false:
///   * `ShuffleWriteBuffer::push` sizes its spill decision from it, so a
///     wide-string map stage spills after ~1/N of the data it should hold;
///   * `DrainTally::observe` reports it as `ShufflePartitionOutput::size_bytes`,
///     which is what made TPC-H q10's `dist-s1` publish 1.74 TB;
///   * `aqe.rs` sums those into reduce parallelism.
#[test]
fn a_utf8view_bucket_owns_its_bytes_rather_than_the_whole_source_buffer() {
    let rows = 100_000;
    let buckets = 18;
    let batch = customer_shaped_view_batch(rows).expect("batch");
    let whole = batch.get_array_memory_size();

    let parts = HashPartitioner::new("c_custkey", buckets)
        .partition(&batch)
        .expect("partition");
    let summed: usize = parts.iter().map(RecordBatch::get_array_memory_size).sum();
    let ratio = summed as f64 / whole as f64;
    println!("ARROW-MEASURED whole={whole} summed={summed} ratio={ratio:.2}x over {buckets} buckets");

    assert!(
        ratio < 1.5,
        "the {buckets} buckets report {ratio:.2}x the source batch's memory: each one still \
         references the whole shared data buffer. Spilling one frees nothing while a sibling \
         holds it, and every consumer that sizes memory from Arrow sees ~{buckets}x the truth"
    );
}

/// Does the sharing cost real bytes, or only accounting?
///
/// This is the question the byte-counting tests cannot answer. Arrow IPC
/// serialises a view array's *data buffers*, so a bucket that still references
/// the whole shared buffer may write the whole shared buffer — once per bucket —
/// into every spill file and every shuffle fetch. If so, the compaction is not
/// a reporting fix at all: it is a reduction in real disk and wire volume, and
/// the multiplier is the bucket count.
///
/// Measured through `StreamWriter`, which is exactly what the write buffer's
/// spill path and the shuffle store both use.
#[test]
fn ipc_bytes_written_per_bucket_do_not_include_the_shared_buffer() {
    fn ipc_len(batch: &RecordBatch) -> usize {
        let mut out: Vec<u8> = Vec::new();
        {
            let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut out, &batch.schema())
                .expect("writer");
            w.write(batch).expect("write");
            w.finish().expect("finish");
        }
        out.len()
    }

    let rows = 100_000;
    let buckets = 18usize;
    let batch = customer_shaped_view_batch(rows).expect("batch");
    let whole = ipc_len(&batch);

    // Both arms from the same row assignment, so the only variable is
    // compaction. Round-robin rather than hashing keeps the split even and the
    // comparison free of skew.
    let mut per_bucket: Vec<Vec<u32>> = vec![Vec::new(); buckets];
    for row in 0..rows {
        per_bucket[row % buckets].push(row as u32);
    }

    let mut shared_total = 0usize;
    let mut compact_total = 0usize;
    for indices in &per_bucket {
        let idx = arrow::array::UInt32Array::from_iter_values(indices.iter().copied());
        let shared: Vec<std::sync::Arc<dyn arrow::array::Array>> = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &idx, None).expect("take"))
            .collect();
        let compact: Vec<std::sync::Arc<dyn arrow::array::Array>> = shared
            .iter()
            .map(|c| krishiv_shuffle::compact_shared_buffers(std::sync::Arc::clone(c)))
            .collect();
        shared_total += ipc_len(&RecordBatch::try_new(batch.schema(), shared).expect("shared"));
        compact_total += ipc_len(&RecordBatch::try_new(batch.schema(), compact).expect("compact"));
    }

    println!(
        "IPC whole={whole} shared={shared_total} ({:.2}x) compacted={compact_total} ({:.2}x) \
         over {buckets} buckets",
        shared_total as f64 / whole as f64,
        compact_total as f64 / whole as f64,
    );

    let ratio = compact_total as f64 / whole as f64;
    assert!(
        ratio < 1.5,
        "compacted buckets still serialise to {ratio:.2}x the source batch ({compact_total} vs \
         {whole} bytes): every spill file and every shuffle fetch would carry the whole shared \
         data buffer once per bucket, which is real disk and wire volume rather than an \
         accounting artefact"
    );
}
