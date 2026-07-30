//! How many bytes does a shuffle partition actually represent?
//!
//! # Why not `get_array_memory_size()`
//!
//! That was the previous answer, and for view arrays it is wrong by roughly the
//! partition count. `take` on a `Utf8View`/`BinaryView` column copies the
//! 16-byte *views* and leaves every output partition referencing the **same**
//! shared data buffers; `get_array_memory_size()` then charges each partition
//! the whole buffer. Measured on a customer-shaped batch split into 47 buckets:
//!
//! ```text
//! Utf8View  summed / whole = 38.32x
//! Utf8      summed / whole =  0.74x
//! ```
//!
//! This is not merely a reporting error. `krishiv-scheduler`'s `aqe.rs` sums
//! these per-partition sizes into the map that decides **reduce parallelism**,
//! so a wide-string stage is sized from a number ~38x too large. TPC-H q10 is
//! the only SF100 query whose shuffle carries wide strings (~152 B/row); its
//! `dist-s3` is AQE-coalesced to 47 partitions where `dist-s0` gets 10, and its
//! reported `shuffle_bytes_written` reached 12.3 TB for a 100 GB dataset.
//!
//! # What this computes instead
//!
//! The partition's *own* bytes. For a view array that is
//! `16 * len` (the views this partition owns) plus the sum of the value
//! lengths it actually references — and arrow encodes a `ByteView`'s length in
//! the low 32 bits of the `u128`, so the sum costs one pass over the views with
//! no string materialisation and no buffer walk.
//!
//! Fixed-width and offset-encoded (Utf8/Binary) arrays are computed exactly
//! from their own slice too. Deferring offset strings to Arrow is what left
//! q10's `dist-s1` reporting **1.74 TB** in the first instrumented run *after*
//! the view fix had landed: `take` allocates, but the map-side write buffer
//! also *slices*, and a sliced offset array shares its values buffer exactly
//! as a view array does.
//!
//! # Known limit
//!
//! Nested types (struct/list) are not descended into and fall back to
//! `get_array_memory_size()`, so a view or string nested inside one can still
//! over-report after a slice. TPC-H does not exercise that shape; a nested
//! schema would need this extended rather than trusted.

use arrow::array::{Array, BinaryViewArray, RecordBatch, StringViewArray};
use arrow::datatypes::DataType;

/// Bytes belonging to one array, not counting buffers it merely shares.
pub fn logical_array_bytes(array: &dyn Array) -> usize {
    match array.data_type() {
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map_or_else(|| array.get_array_memory_size(), view_bytes_of),
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .map_or_else(|| array.get_array_memory_size(), binary_view_bytes_of),
        // Fixed-width types have an exact answer that does not depend on how
        // the array was produced. `get_array_memory_size()` would report the
        // whole shared buffer for a *sliced* primitive — correct after `take`,
        // which allocates, but 16x too high after `slice`, which does not. The
        // production shuffle path gathers with `take`; computing it exactly
        // means this function is right either way.
        dt if dt.primitive_width().is_some() => {
            let width = dt.primitive_width().unwrap_or(0);
            let validity = array.nulls().map_or(0, |_| array.len().div_ceil(8));
            array.len().saturating_mul(width).saturating_add(validity)
        }
        // Offset-encoded strings/binary: the slice's own bytes are the span
        // between its first and last offset. `get_array_memory_size()` reports
        // the whole shared values buffer here exactly as it does for views, so
        // leaving this to Arrow left q10's `dist-s1` reporting **1.74 TB** even
        // after the view fix landed.
        DataType::Utf8 => offset_bytes::<i32>(array),
        DataType::LargeUtf8 => offset_bytes::<i64>(array),
        DataType::Binary => offset_bytes::<i32>(array),
        DataType::LargeBinary => offset_bytes::<i64>(array),
        // Nested types (struct/list) are not descended into and still defer to
        // Arrow, so they remain exact-after-`take` and an over-estimate after
        // `slice`. TPC-H does not exercise that shape.
        _ => array.get_array_memory_size(),
    }
}

/// Bytes an offset-encoded array's own slice references: the value span plus
/// its offsets and validity.
fn offset_bytes<O: arrow::array::OffsetSizeTrait>(array: &dyn Array) -> usize {
    let data = array.to_data();
    let Some(offsets) = data.buffers().first() else {
        return array.get_array_memory_size();
    };
    let slice: &[O] = offsets.typed_data::<O>();
    // `to_data` keeps the parent buffer, so index relative to this array's own
    // offset rather than assuming the slice starts at zero.
    let start = data.offset();
    let end = start + data.len();
    let (Some(first), Some(last)) = (slice.get(start), slice.get(end)) else {
        return array.get_array_memory_size();
    };
    let values = last.as_usize().saturating_sub(first.as_usize());
    let offset_width = std::mem::size_of::<O>();
    let validity = data.nulls().map_or(0, |_| data.len().div_ceil(8));
    values
        .saturating_add(data.len().saturating_add(1).saturating_mul(offset_width))
        .saturating_add(validity)
}

/// A `ByteView` is a `u128` whose low 32 bits are the value length, so the
/// referenced byte total is one pass over the views.
fn view_bytes_of(array: &StringViewArray) -> usize {
    views_bytes(array.views())
}

fn binary_view_bytes_of(array: &BinaryViewArray) -> usize {
    views_bytes(array.views())
}

fn views_bytes(views: &[u128]) -> usize {
    const VIEW_WIDTH: usize = std::mem::size_of::<u128>();
    let referenced: usize = views.iter().map(|v| (*v as u32) as usize).sum();
    views.len().saturating_mul(VIEW_WIDTH).saturating_add(referenced)
}

/// Bytes belonging to one record batch.
pub fn logical_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| logical_array_bytes(c.as_ref()))
        .sum()
}

/// Bytes belonging to a shuffle partition — the number `ShufflePartitionOutput`
/// reports and AQE sizes reduce parallelism from.
pub fn logical_partition_bytes(batches: &[RecordBatch]) -> u64 {
    let total: usize = batches.iter().map(logical_batch_bytes).sum();
    u64::try_from(total).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// The property that matters: splitting a view-array batch must not
    /// multiply its reported size by the number of pieces.
    #[test]
    fn splitting_a_view_batch_does_not_multiply_its_reported_size() {
        let rows = 4_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let keys = Int64Array::from_iter_values(0..rows as i64);
        let strings =
            StringViewArray::from_iter_values((0..rows).map(|i| format!("{i:0>73}")));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(strings)]).unwrap();

        let whole = logical_batch_bytes(&batch);
        let pieces = 16;
        let per = rows / pieces;
        let summed: usize = (0..pieces)
            .map(|i| logical_batch_bytes(&batch.slice(i * per, per)))
            .sum();

        let ratio = summed as f64 / whole as f64;
        assert!(
            (0.9..1.1).contains(&ratio),
            "slicing into {pieces} pieces reported {ratio:.2}x the whole batch; the point of \
             this module is that a piece is charged only for what it references"
        );
    }

    /// Offset strings must be slice-correct too: this is what left q10's
    /// `dist-s1` reporting 1.74 TB after the view fix had already landed.
    #[test]
    fn slicing_a_utf8_batch_does_not_multiply_its_reported_size() {
        use arrow::array::StringArray;
        let rows = 4_000;
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let strings = StringArray::from_iter_values((0..rows).map(|i| format!("{i:0>73}")));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(strings)]).unwrap();
        let whole = logical_batch_bytes(&batch);
        let pieces = 16;
        let per = rows / pieces;
        let summed: usize = (0..pieces)
            .map(|i| logical_batch_bytes(&batch.slice(i * per, per)))
            .sum();
        let ratio = summed as f64 / whole as f64;
        assert!(
            (0.9..1.15).contains(&ratio),
            "Utf8 sliced into {pieces} pieces reported {ratio:.2}x the whole batch"
        );
    }

    /// And it must still count the referenced bytes, not just the views —
    /// otherwise a wide-string stage would look free.
    #[test]
    fn referenced_value_bytes_are_counted_not_just_the_views() {
        let short = StringViewArray::from_iter_values((0..100).map(|_| "x"));
        let long = StringViewArray::from_iter_values((0..100).map(|i| format!("{i:0>200}")));
        let short_bytes = logical_array_bytes(&short);
        let long_bytes = logical_array_bytes(&long);
        assert!(
            long_bytes > short_bytes * 5,
            "200-byte values must cost far more than 1-byte values \
             (short={short_bytes}, long={long_bytes})"
        );
    }
}
