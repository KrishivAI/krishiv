//! Cross-stage runtime filters: a bloom filter that survives the exchange cut.
//!
//! # Why this exists
//!
//! DataFusion builds a *dynamic filter* from a hash join's build side and
//! pushes it into the probe side's scan. That works in a single plan. We cut
//! plans at every exchange, so the probe's scan runs in its own stage **before
//! the join stage exists** — there is no build side to learn from, and every
//! SF100 plan dump shows the placeholder unfilled:
//!
//! ```text
//! lineitem ... predicate=l_returnflag = R AND DynamicFilter [ empty ]
//! ```
//!
//! A runtime filter is the same idea expressed as *data*: the build stage emits
//! a bloom filter over its join keys, and the probe stage applies it during its
//! scan. Being a value rather than a plan reference, it crosses the stage
//! boundary intact.
//!
//! # Why `parquet::bloom_filter::Sbbf` and not a bloom crate
//!
//! It is already in our dependency graph, so it costs no build time. It has a
//! header-free wire format (`write_bitset` / `Sbbf::new`) that round-trips
//! exactly, which is what shipping a filter inside a plan fragment needs. And
//! it hashes with XXHASH over the *Parquet physical encoding* — `i64` little
//! endian, raw bytes for byte arrays — so a filter built here is byte-identical
//! to the ones our Parquet files already carry in their footers. That keeps
//! row-group pruning reachable later; a foreign bloom implementation would
//! close that door for no gain, since both designs are block filters costing
//! one cache line per probe.
//!
//! # The correctness argument
//!
//! A bloom filter is false-positive-only, so a surviving row that should not
//! have survived is merely work: the join still runs and still rejects it. A
//! false *negative* is a wrong answer. Everything below exists to make false
//! negatives impossible rather than unlikely:
//!
//! - **Encoding is canonical and type-checked.** Build and probe must turn the
//!   same logical value into the same bytes. [`RuntimeFilter`] carries the
//!   build side's [`FilterKeyType`]; [`RuntimeFilter::contains`] refuses to
//!   filter when the probe's type disagrees, which degrades to "no filtering"
//!   — slower, never wrong.
//! - **Unsupported types fail open.** [`FilterKeyType::for_data_type`] returns
//!   `None` rather than guessing, and a `None` means the planner does not fire
//!   the rule at all.
//! - **Merging is a bitwise OR**, which can only ever set more bits.
//! - **Nulls are the caller's problem, and the caller is documented.** See
//!   [`RuntimeFilter::contains`].

use crate::error::{ShuffleError, ShuffleResult, io_err};
use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use parquet::bloom_filter::Sbbf;

/// Largest filter we will build or ship, in bytes.
///
/// Past this the broadcast costs more than the scan it saves: the filter goes
/// to every probe task on every node, while the bytes it saves are read once.
/// 16 MiB holds ~13M keys at 1% FPP, which covers every TPC-H build side.
pub const MAX_FILTER_BYTES: usize = 16 * 1024 * 1024;

/// Target false-positive probability. 1% costs ~9.6 bits per key; going lower
/// buys little, since a false positive only means the join rejects the row.
pub const DEFAULT_FPP: f64 = 0.01;

/// The canonical byte encoding used for a join key.
///
/// This is deliberately coarse: every signed integer and date narrows to one
/// variant so that a build side typed `Int32` and a probe side typed `Int64`
/// still agree. Anything not listed is unsupported, and unsupported means the
/// rule does not fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterKeyType {
    /// Signed integers and date types, encoded as `i64` little endian.
    Int,
    /// Unsigned integers, encoded as `u64` little endian.
    UInt,
    /// Strings and binary, encoded as their raw bytes.
    Bytes,
}

impl FilterKeyType {
    /// The encoding for `data_type`, or `None` when we will not filter on it.
    ///
    /// Floats and decimals are excluded on purpose. Equality on a float is not
    /// a safe thing to hash — `-0.0 == 0.0` yet their bytes differ — and a
    /// decimal's bytes depend on a scale the two sides need not share.
    #[must_use]
    pub fn for_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Date64 => Some(Self::Int),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Some(Self::UInt)
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => Some(Self::Bytes),
            _ => None,
        }
    }
}

/// Number of bytes a filter for `ndv` distinct keys wants, clamped to
/// [`MAX_FILTER_BYTES`].
///
/// The planner calls this once and ships the answer, so that every partial
/// filter built in parallel comes out the same length and can be merged.
#[must_use]
pub fn plan_filter_bytes(ndv: u64) -> usize {
    // Mirrors Sbbf's own sizing so the clamp below is applied to the number
    // Sbbf would have picked, rather than to a different one.
    let bits = -8.0 * ndv as f64 / (1.0 - DEFAULT_FPP.powf(1.0 / 8.0)).ln();
    let bytes = if bits.is_finite() && bits > 0.0 {
        (bits / 8.0) as usize
    } else {
        parquet::bloom_filter::BITSET_MIN_LENGTH
    };
    bytes.clamp(parquet::bloom_filter::BITSET_MIN_LENGTH, MAX_FILTER_BYTES)
}

/// Accumulates join keys into a bloom filter of a fixed, pre-planned size.
pub struct RuntimeFilterBuilder {
    sbbf: Sbbf,
    key_type: FilterKeyType,
    keys: u64,
}

impl RuntimeFilterBuilder {
    /// A builder sized to exactly `num_bytes` (rounded up to a power of two by
    /// `Sbbf`, identically for every builder given the same input).
    #[must_use]
    pub fn new(num_bytes: usize, key_type: FilterKeyType) -> Self {
        Self {
            sbbf: Sbbf::new_with_num_of_bytes(num_bytes.min(MAX_FILTER_BYTES)),
            key_type,
            keys: 0,
        }
    }

    /// Insert every non-null value of `array`.
    ///
    /// Nulls are skipped because a null key joins to nothing, so omitting it
    /// cannot cause a matching row to be dropped.
    pub fn insert_array(&mut self, array: &dyn Array) -> ShuffleResult<()> {
        let observed = FilterKeyType::for_data_type(array.data_type()).ok_or_else(|| {
            io_err(format!(
                "runtime filter cannot encode key type {}",
                array.data_type()
            ))
        })?;
        if observed != self.key_type {
            return Err(io_err(format!(
                "runtime filter key type mismatch: built for {:?}, got {observed:?} ({})",
                self.key_type,
                array.data_type()
            )));
        }
        for_each_key(array, &mut |bytes| {
            self.sbbf.insert(bytes);
            self.keys += 1;
        })
    }

    /// Number of non-null keys inserted so far.
    #[must_use]
    pub fn keys_inserted(&self) -> u64 {
        self.keys
    }

    /// Freeze into a shippable filter.
    #[must_use]
    pub fn finish(self) -> RuntimeFilter {
        RuntimeFilter {
            sbbf: self.sbbf,
            key_type: self.key_type,
            keys: self.keys,
        }
    }
}

/// A frozen bloom filter over one join key, ready to ship to a probe stage.
pub struct RuntimeFilter {
    sbbf: Sbbf,
    key_type: FilterKeyType,
    keys: u64,
}

impl std::fmt::Debug for RuntimeFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeFilter")
            .field("key_type", &self.key_type)
            .field("keys", &self.keys)
            .field("bytes", &(self.sbbf.num_blocks() * 32))
            .finish()
    }
}

/// One byte of wire header: which encoding the build side used.
const WIRE_INT: u8 = 1;
const WIRE_UINT: u8 = 2;
const WIRE_BYTES: u8 = 3;

impl RuntimeFilter {
    /// The encoding the build side used for its keys.
    #[must_use]
    pub fn key_type(&self) -> FilterKeyType {
        self.key_type
    }

    /// Number of non-null keys that went in.
    #[must_use]
    pub fn keys_inserted(&self) -> u64 {
        self.keys
    }

    /// Size of the bitset in bytes.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.sbbf.num_blocks() * std::mem::size_of::<u32>() * 8
    }

    /// Serialize as `[key_type byte][8-byte key count LE][raw bitset]`.
    ///
    /// The bitset is `Sbbf`'s own little-endian block layout with no Thrift
    /// header, so [`Self::from_bytes`] reconstructs it exactly.
    pub fn to_bytes(&self) -> ShuffleResult<Vec<u8>> {
        let mut out = Vec::with_capacity(self.byte_len() + 9);
        out.push(match self.key_type {
            FilterKeyType::Int => WIRE_INT,
            FilterKeyType::UInt => WIRE_UINT,
            FilterKeyType::Bytes => WIRE_BYTES,
        });
        out.extend_from_slice(&self.keys.to_le_bytes());
        self.sbbf
            .write_bitset(&mut out)
            .map_err(|e| io_err(format!("runtime filter serialize: {e}")))?;
        Ok(out)
    }

    /// Inverse of [`Self::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> ShuffleResult<Self> {
        let (&tag, rest) = bytes
            .split_first()
            .ok_or_else(|| io_err("runtime filter payload is empty"))?;
        let key_type = match tag {
            WIRE_INT => FilterKeyType::Int,
            WIRE_UINT => FilterKeyType::UInt,
            WIRE_BYTES => FilterKeyType::Bytes,
            other => return Err(io_err(format!("unknown runtime filter key tag {other}"))),
        };
        let (count, bitset) = rest
            .split_at_checked(8)
            .ok_or_else(|| io_err("runtime filter key count truncated"))?;
        let keys = u64::from_le_bytes(
            count
                .try_into()
                .map_err(|_| io_err("runtime filter key count truncated"))?,
        );
        // `Sbbf::new` uses `chunks_exact(32)` and silently drops a partial
        // trailing block, which would turn a corrupted payload into a filter
        // that quietly returns false negatives. Reject it instead.
        if bitset.is_empty() || !bitset.len().is_multiple_of(32) {
            return Err(io_err(format!(
                "runtime filter bitset must be a whole number of 32-byte blocks, got {}",
                bitset.len()
            )));
        }
        Ok(Self {
            sbbf: Sbbf::new(bitset),
            key_type,
            keys,
        })
    }

    /// Merge `other` into `self` by bitwise OR.
    ///
    /// The build stage runs as N parallel tasks, each producing a partial
    /// filter; this is how they become one. `Sbbf` exposes no union, but its
    /// bitset is a flat little-endian `u32` array, so OR-ing the serialized
    /// bytes is exactly OR-ing the blocks.
    ///
    /// Errors if the two were not built at the same size — merging different
    /// sizes would misplace blocks and *lose* set bits, which is the one
    /// failure mode that produces wrong answers.
    pub fn union(&mut self, other: &Self) -> ShuffleResult<()> {
        if self.key_type != other.key_type {
            return Err(io_err(format!(
                "cannot union runtime filters of different key types: {:?} vs {:?}",
                self.key_type, other.key_type
            )));
        }
        let mut lhs = Vec::with_capacity(self.byte_len());
        let mut rhs = Vec::with_capacity(other.byte_len());
        self.sbbf
            .write_bitset(&mut lhs)
            .map_err(|e| io_err(format!("runtime filter union: {e}")))?;
        other
            .sbbf
            .write_bitset(&mut rhs)
            .map_err(|e| io_err(format!("runtime filter union: {e}")))?;
        if lhs.len() != rhs.len() {
            return Err(io_err(format!(
                "cannot union runtime filters of different sizes: {} vs {} bytes",
                lhs.len(),
                rhs.len()
            )));
        }
        for (l, r) in lhs.iter_mut().zip(rhs.iter()) {
            *l |= *r;
        }
        self.sbbf = Sbbf::new(&lhs);
        self.keys = self.keys.saturating_add(other.keys);
        Ok(())
    }

    /// Test one already-encoded key.
    #[must_use]
    pub fn contains_encoded(&self, key: &[u8]) -> bool {
        self.sbbf.check(key)
    }

    /// Evaluate the filter over `array`, returning a mask to keep.
    ///
    /// # Nulls
    ///
    /// A null yields `false` — the row is dropped. That is correct for an
    /// **inner** equijoin, where a null key matches nothing, and wrong for a
    /// join that preserves the probe side (LEFT/FULL outer, and the anti
    /// variants), where an unmatched row must still be emitted. The planner is
    /// responsible for only firing on join types where dropping non-matching
    /// probe rows is sound; this mirrors the constraint recorded for broadcast
    /// join splitting.
    ///
    /// If the probe's type does not agree with the build side's, every row is
    /// kept. Filtering nothing is slow, not wrong.
    pub fn contains(&self, array: &dyn Array) -> ShuffleResult<BooleanArray> {
        let observed = FilterKeyType::for_data_type(array.data_type());
        if observed != Some(self.key_type) {
            return Ok(BooleanArray::from(vec![true; array.len()]));
        }
        let mut mask = Vec::with_capacity(array.len());
        let mut idx = 0usize;
        for_each_key_or_null(array, &mut |key| {
            mask.push(key.is_some_and(|bytes| self.sbbf.check(bytes)));
            idx += 1;
        })?;
        debug_assert_eq!(idx, array.len());
        Ok(BooleanArray::from(mask))
    }
}

/// Call `f` with the canonical encoding of every non-null value.
fn for_each_key(array: &dyn Array, f: &mut impl FnMut(&[u8])) -> ShuffleResult<()> {
    for_each_key_or_null(array, &mut |key| {
        if let Some(bytes) = key {
            f(bytes);
        }
    })
}

/// Call `f` once per slot, with `None` for nulls.
///
/// Integers widen to 8 bytes so that the same logical value encodes the same
/// way regardless of the column's declared width.
fn for_each_key_or_null(array: &dyn Array, f: &mut impl FnMut(Option<&[u8]>)) -> ShuffleResult<()> {
    macro_rules! signed {
        ($ty:ty) => {{
            let typed = downcast::<$ty>(array)?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    f(None);
                } else {
                    f(Some(&i64::from(typed.value(i)).to_le_bytes()));
                }
            }
            return Ok(());
        }};
    }
    macro_rules! unsigned {
        ($ty:ty) => {{
            let typed = downcast::<$ty>(array)?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    f(None);
                } else {
                    f(Some(&u64::from(typed.value(i)).to_le_bytes()));
                }
            }
            return Ok(());
        }};
    }
    macro_rules! bytes_like {
        ($ty:ty, $conv:expr) => {{
            let typed = downcast::<$ty>(array)?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    f(None);
                } else {
                    let v = typed.value(i);
                    f(Some($conv(v)));
                }
            }
            return Ok(());
        }};
    }

    match array.data_type() {
        DataType::Int8 => signed!(Int8Array),
        DataType::Int16 => signed!(Int16Array),
        DataType::Int32 => signed!(Int32Array),
        DataType::Date32 => signed!(Date32Array),
        DataType::UInt8 => unsigned!(UInt8Array),
        DataType::UInt16 => unsigned!(UInt16Array),
        DataType::UInt32 => unsigned!(UInt32Array),
        DataType::Int64 => {
            let typed = downcast::<Int64Array>(array)?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    f(None);
                } else {
                    f(Some(&typed.value(i).to_le_bytes()));
                }
            }
            Ok(())
        }
        DataType::Date64 => {
            let typed = downcast::<Date64Array>(array)?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    f(None);
                } else {
                    f(Some(&typed.value(i).to_le_bytes()));
                }
            }
            Ok(())
        }
        DataType::UInt64 => {
            let typed = downcast::<UInt64Array>(array)?;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    f(None);
                } else {
                    f(Some(&typed.value(i).to_le_bytes()));
                }
            }
            Ok(())
        }
        DataType::Utf8 => bytes_like!(StringArray, str::as_bytes),
        DataType::LargeUtf8 => bytes_like!(LargeStringArray, str::as_bytes),
        DataType::Utf8View => bytes_like!(StringViewArray, str::as_bytes),
        DataType::Binary => bytes_like!(BinaryArray, std::convert::identity),
        DataType::LargeBinary => bytes_like!(LargeBinaryArray, std::convert::identity),
        DataType::BinaryView => bytes_like!(BinaryViewArray, std::convert::identity),
        other => Err(io_err(format!(
            "runtime filter cannot encode key type {other}"
        ))),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> ShuffleResult<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        ShuffleError::Io(std::io::Error::other(format!(
            "runtime filter downcast failed for {}",
            array.data_type()
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn build(values: &[i64], ndv: u64) -> RuntimeFilter {
        let mut b = RuntimeFilterBuilder::new(plan_filter_bytes(ndv), FilterKeyType::Int);
        b.insert_array(&Int64Array::from(values.to_vec())).unwrap();
        b.finish()
    }

    #[test]
    fn every_inserted_key_is_found() {
        let keys: Vec<i64> = (0..50_000).map(|i| i * 7 + 3).collect();
        let filter = build(&keys, 50_000);
        let mask = filter.contains(&Int64Array::from(keys.clone())).unwrap();
        // The whole safety argument: not one false negative, ever.
        assert!(
            (0..mask.len()).all(|i| mask.value(i)),
            "bloom filter reported a false negative"
        );
    }

    #[test]
    fn absent_keys_are_mostly_rejected() {
        let keys: Vec<i64> = (0..20_000).collect();
        let filter = build(&keys, 20_000);
        let probes: Vec<i64> = (1_000_000..1_020_000).collect();
        let mask = filter.contains(&Int64Array::from(probes)).unwrap();
        let kept = (0..mask.len()).filter(|&i| mask.value(i)).count();
        // 1% target FPP; allow generous slack so this cannot flake.
        assert!(kept < 1_000, "kept {kept} of 20000 absent keys");
    }

    #[test]
    fn a_narrower_int_column_encodes_like_a_wide_one() {
        // The build side may be Int64 while the probe scan reads Int32. If
        // these encoded differently every row would be dropped.
        let filter = build(&[1, 2, 3, 999], 4);
        let mask = filter
            .contains(&Int32Array::from(vec![1, 2, 3, 999]))
            .unwrap();
        assert!((0..4).all(|i| mask.value(i)));
    }

    #[test]
    fn nulls_are_dropped_on_the_probe_side_and_skipped_on_the_build_side() {
        let mut b = RuntimeFilterBuilder::new(plan_filter_bytes(8), FilterKeyType::Int);
        b.insert_array(&Int64Array::from(vec![Some(1), None, Some(2)]))
            .unwrap();
        let filter = b.finish();
        assert_eq!(filter.keys_inserted(), 2, "null must not be inserted");
        let mask = filter
            .contains(&Int64Array::from(vec![Some(1), None, Some(2)]))
            .unwrap();
        assert!(mask.value(0));
        assert!(!mask.value(1), "a null key joins to nothing");
        assert!(mask.value(2));
    }

    #[test]
    fn serialization_round_trips_exactly() {
        let keys: Vec<i64> = (0..10_000).map(|i| i * 13).collect();
        let filter = build(&keys, 10_000);
        let bytes = filter.to_bytes().unwrap();
        let restored = RuntimeFilter::from_bytes(&bytes).unwrap();
        assert_eq!(restored.key_type(), FilterKeyType::Int);
        assert_eq!(restored.keys_inserted(), 10_000);
        assert_eq!(restored.byte_len(), filter.byte_len());
        let mask = restored.contains(&Int64Array::from(keys)).unwrap();
        assert!((0..mask.len()).all(|i| mask.value(i)));
    }

    #[test]
    fn union_keeps_every_key_from_both_partials() {
        // This is the parallel build stage: N tasks, N partial filters, one
        // merged result that must contain the union of all their keys.
        let size = plan_filter_bytes(20_000);
        let left: Vec<i64> = (0..10_000).collect();
        let right: Vec<i64> = (500_000..510_000).collect();

        let mut a = RuntimeFilterBuilder::new(size, FilterKeyType::Int);
        a.insert_array(&Int64Array::from(left.clone())).unwrap();
        let mut merged = a.finish();

        let mut b = RuntimeFilterBuilder::new(size, FilterKeyType::Int);
        b.insert_array(&Int64Array::from(right.clone())).unwrap();
        merged.union(&b.finish()).unwrap();

        for keys in [left, right] {
            let mask = merged.contains(&Int64Array::from(keys)).unwrap();
            assert!((0..mask.len()).all(|i| mask.value(i)));
        }
        assert_eq!(merged.keys_inserted(), 20_000);
    }

    #[test]
    fn union_refuses_mismatched_sizes() {
        let mut small = RuntimeFilterBuilder::new(64, FilterKeyType::Int).finish();
        let large = RuntimeFilterBuilder::new(1 << 20, FilterKeyType::Int).finish();
        // Silently OR-ing these would drop set bits => false negatives.
        assert!(small.union(&large).is_err());
    }

    #[test]
    fn union_refuses_mismatched_key_types() {
        let mut ints = RuntimeFilterBuilder::new(1024, FilterKeyType::Int).finish();
        let bytes = RuntimeFilterBuilder::new(1024, FilterKeyType::Bytes).finish();
        assert!(ints.union(&bytes).is_err());
    }

    #[test]
    fn a_type_mismatch_keeps_every_row_rather_than_dropping_them() {
        let filter = build(&[1, 2, 3], 3);
        let probe = StringArray::from(vec!["1", "2", "3"]);
        let mask = filter.contains(&probe).unwrap();
        assert!(
            (0..3).all(|i| mask.value(i)),
            "a type disagreement must fail open, not drop rows"
        );
    }

    #[test]
    fn string_keys_round_trip_across_utf8_and_utf8view() {
        let mut b = RuntimeFilterBuilder::new(plan_filter_bytes(3), FilterKeyType::Bytes);
        b.insert_array(&StringArray::from(vec!["alpha", "beta", "gamma"]))
            .unwrap();
        let filter = b.finish();
        let probe = StringViewArray::from(vec!["alpha", "beta", "gamma", "delta"]);
        let mask = filter.contains(&probe).unwrap();
        assert!(mask.value(0) && mask.value(1) && mask.value(2));
        assert!(!mask.value(3));
    }

    #[test]
    fn unsupported_key_types_are_refused_rather_than_guessed() {
        assert!(FilterKeyType::for_data_type(&DataType::Float64).is_none());
        assert!(FilterKeyType::for_data_type(&DataType::Boolean).is_none());
        assert!(FilterKeyType::for_data_type(&DataType::Decimal128(18, 2)).is_none());
        assert!(FilterKeyType::for_data_type(&DataType::Int64).is_some());
    }

    #[test]
    fn planned_size_is_bounded_and_deterministic() {
        assert_eq!(plan_filter_bytes(5_400_000), plan_filter_bytes(5_400_000));
        assert!(plan_filter_bytes(u64::MAX) <= MAX_FILTER_BYTES);
        assert!(plan_filter_bytes(0) >= parquet::bloom_filter::BITSET_MIN_LENGTH);
        // q10's build side: ~5.4M orders keys must still fit under the cap.
        assert!(plan_filter_bytes(5_400_000) <= MAX_FILTER_BYTES);
    }

    #[test]
    fn a_truncated_payload_is_rejected_not_silently_shortened() {
        let filter = build(&[1, 2, 3], 3);
        let bytes = filter.to_bytes().unwrap();
        // Sbbf::new uses chunks_exact and would drop a partial block, which
        // turns corruption into false negatives.
        assert!(RuntimeFilter::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        assert!(RuntimeFilter::from_bytes(&bytes[..4]).is_err());
        assert!(RuntimeFilter::from_bytes(&[]).is_err());
    }

    #[test]
    fn an_empty_build_side_rejects_everything() {
        // A join whose build side produced no rows must produce no output; the
        // filter should say so rather than pass everything through.
        let filter = RuntimeFilterBuilder::new(plan_filter_bytes(1), FilterKeyType::Int).finish();
        let mask = filter.contains(&Int64Array::from(vec![1, 2, 3])).unwrap();
        assert!((0..3).all(|i| !mask.value(i)));
    }

    #[test]
    fn filters_work_through_an_arc_dyn_array() {
        let filter = build(&[7], 1);
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![7]));
        assert!(filter.contains(array.as_ref()).unwrap().value(0));
    }
}
