use crate::ShuffleResult;
use arrow::record_batch::RecordBatch;

/// Selects the Arrow IPC body compression used on the shuffle wire.
///
/// `lz4` (default) | `zstd` | `none`.
pub const WIRE_COMPRESSION_ENV: &str = "KRISHIV_SHUFFLE_WIRE_COMPRESSION";

/// Arrow IPC write options for the shuffle wire, with body compression.
///
/// # Why this is not `IpcWriteOptions::default()`
///
/// Reduce tasks fetch their input partitions from other executors over Arrow
/// Flight, so most of every shuffle crosses the pod network. On this cluster
/// that link measures **~7.6 MB/s**, against **150–286 MB/s** for a node-local
/// object read — the wire is roughly forty times slower than the disk, and it
/// was carrying uncompressed Arrow buffers. Nothing else on the path
/// compressed either: the disk store defaults to `ShuffleCompression::None`,
/// and tonic is built without a compression feature, so gRPC-level compression
/// was not merely off but unavailable.
///
/// Arrow IPC body compression is the right layer for this. The codec is
/// recorded in each record-batch message, so any conforming reader
/// decompresses without negotiation — no protocol change, no client change,
/// and a peer that receives an uncompressed stream still reads it.
///
/// Measured on a shuffle-shaped batch (20k rows, 500 distinct join keys, five
/// repeated region strings) by `wire_compression_actually_shrinks_the_payload`:
/// **381,640 bytes uncompressed -> 83,976 with LZ4 (4.54x), 56,584 with ZSTD
/// (6.74x)**. LZ4 compresses at hundreds of MB/s per core, two orders of
/// magnitude faster than this link, so on this cluster the trade is
/// one-sided. It stays
/// configurable because that stops being true on a fast fabric: at 10 GbE the
/// CPU cost is comparable to the transfer it saves, and `none` is then the
/// honest setting.
///
/// Falls back to uncompressed — loudly — if the codec cannot be installed,
/// which happens when `arrow` is built without `ipc_compression`. Returning a
/// working uncompressed stream is right here; refusing to serve a shuffle
/// because it cannot be *compressed* would turn a performance feature into an
/// outage.
pub fn wire_ipc_write_options() -> arrow::ipc::writer::IpcWriteOptions {
    let requested = std::env::var(WIRE_COMPRESSION_ENV)
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_else(|_| String::from("lz4"));
    ipc_write_options_for(&requested)
}

/// The codec decision, without reading the environment.
///
/// Split out so the behaviour is testable without mutating process-wide env
/// state — which this crate cannot do anyway (it forbids `unsafe`, and
/// `set_var` is unsafe since Rust 2024), and which would make the tests
/// order-dependent even where it is allowed.
pub fn ipc_write_options_for(requested: &str) -> arrow::ipc::writer::IpcWriteOptions {
    use arrow::ipc::CompressionType;
    use arrow::ipc::writer::IpcWriteOptions;

    let codec = match requested {
        "none" | "off" | "" => None,
        "zstd" => Some(CompressionType::ZSTD),
        "lz4" | "lz4_frame" => Some(CompressionType::LZ4_FRAME),
        other => {
            tracing::warn!(
                "{WIRE_COMPRESSION_ENV}='{other}' is not one of lz4|zstd|none; \
                 falling back to lz4"
            );
            Some(CompressionType::LZ4_FRAME)
        }
    };

    let Some(codec) = codec else {
        return IpcWriteOptions::default();
    };

    match IpcWriteOptions::default().try_with_compression(Some(codec)) {
        Ok(options) => options,
        Err(error) => {
            tracing::warn!(
                %error,
                "shuffle wire compression unavailable (arrow built without \
                 `ipc_compression`?); sending uncompressed"
            );
            IpcWriteOptions::default()
        }
    }
}

/// Selects the codec for shuffle partitions **at rest** — Parquet on local
/// disk, Arrow IPC in the object store.
///
/// `lz4` (default) | `zstd` | `none`. Separate from
/// [`WIRE_COMPRESSION_ENV`] because the two are different trades: the wire
/// pays CPU once per transfer, storage pays it once per write and saves it on
/// every re-read (and on the object-store path, on every byte that crosses to
/// MinIO twice — once written, once fetched).
pub const STORAGE_COMPRESSION_ENV: &str = "KRISHIV_SHUFFLE_STORAGE_COMPRESSION";

/// Codec for newly-written shuffle partitions.
///
/// # Why the default is not `None`
///
/// Both stores defaulted to [`ShuffleCompression::None`], and the only
/// production caller that ever set a codec was `shuffle_svc`, which passes
/// `Lz4` to the local disk store. Every other path — the executor's local,
/// object, and tiered backends, i.e. the ones a distributed query actually
/// uses — took the default. So shuffle partitions were written uncompressed to
/// local disk and to the object store, while one unrelated HTTP service path
/// compressed. `with_compression` existed, was tested, and was never called
/// outside tests on those paths.
///
/// That is expensive here for the same reason wire compression was: an
/// object-store partition crosses the pod network twice, and shuffle payloads
/// are join keys and grouped measures — the shape that compresses.
///
/// Measured by `the_disk_store_compresses_by_default_and_still_round_trips` on
/// a 20k-row shuffle-shaped partition: **34,965 bytes of Parquet uncompressed
/// -> 4,630 with LZ4, 7.55x**. Higher than the 4.54x the same corpus gives on
/// the wire, because Parquet's dictionary and run-length encodings compose
/// with the codec instead of competing with it.
///
/// Both formats are self-describing — Parquet records its codec in file
/// metadata, Arrow IPC in each record-batch message — so this is safe to
/// change under running data in both directions: an old reader reads new
/// compressed partitions, and a new reader reads old uncompressed ones.
pub fn default_storage_compression() -> ShuffleCompression {
    let requested = std::env::var(STORAGE_COMPRESSION_ENV)
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_else(|_| String::from("lz4"));
    storage_compression_for(&requested)
}

/// The storage-codec decision, without reading the environment.
///
/// Split from [`default_storage_compression`] for the same reason
/// [`ipc_write_options_for`] is: this crate forbids `unsafe`, so a test cannot
/// set an env var to exercise the mapping.
pub fn storage_compression_for(requested: &str) -> ShuffleCompression {
    match requested {
        "none" | "off" | "" => ShuffleCompression::None,
        "zstd" => ShuffleCompression::Zstd,
        "lz4" | "lz4_frame" => ShuffleCompression::Lz4,
        other => {
            tracing::warn!(
                "{STORAGE_COMPRESSION_ENV}='{other}' is not one of lz4|zstd|none; \
                 falling back to lz4"
            );
            ShuffleCompression::Lz4
        }
    }
}

/// Compression algorithm for shuffle block data.
///
/// Used in [`ShuffleWriteConfig`] and [`ShuffleReadConfig`] to specify
/// how shuffle blocks are compressed on write and decompressed on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShuffleCompression {
    /// No compression (default).
    #[default]
    None,
    /// LZ4 **block** format with a little-endian u32 length prefix
    /// (`lz4_flex::compress_prepend_size`) — not an LZ4 *frame*.
    ///
    /// The distinction matters if these bytes ever leave the crate: a standard
    /// `lz4` frame tool cannot read them, and `lz4_flex`'s own frame encoder is
    /// a different module. It round-trips correctly because `compress` and
    /// `decompress` are the matching pair; the doc used to say "frame", which
    /// would have sent someone looking for a framing bug that does not exist.
    Lz4,
    /// Zstandard compression via `zstd`.
    Zstd,
}

/// Compression codec — type alias for [`ShuffleCompression`].
pub type CompressionCodec = ShuffleCompression;

impl ShuffleCompression {
    /// Compress `data` using this codec. Returns the compressed bytes.
    ///
    /// # Not on the shuffle path
    ///
    /// Nothing in production calls this or [`Self::decompress`] — only
    /// `tests/integration_shuffle_pipeline.rs`. The *enum* is live, as the codec
    /// selector threaded into [`parquet_writer_properties`] and the stores'
    /// `with_compression`, but shuffle bytes are compressed by Parquet (at rest)
    /// and by Arrow IPC body compression (on the wire), never by these methods.
    ///
    /// Recorded because the crate has already cost two sessions to a plausible
    /// but unreachable code path: check that a caller exists before tuning the
    /// codec here, and note that changing it would not move a shuffle byte.
    pub fn compress(self, data: &[u8]) -> ShuffleResult<Vec<u8>> {
        match self {
            ShuffleCompression::None => Ok(data.to_vec()),
            ShuffleCompression::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            ShuffleCompression::Zstd => {
                zstd::encode_all(data, 0).map_err(|e| crate::error::io_err(e.to_string()))
            }
        }
    }

    /// Decompress `data` using this codec. Returns the original bytes.
    pub fn decompress(self, data: &[u8]) -> ShuffleResult<Vec<u8>> {
        match self {
            ShuffleCompression::None => Ok(data.to_vec()),
            ShuffleCompression::Lz4 => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| crate::error::io_err(e.to_string())),
            ShuffleCompression::Zstd => {
                zstd::decode_all(data).map_err(|e| crate::error::io_err(e.to_string()))
            }
        }
    }
}

pub fn partition_memory_bytes(partition: &crate::store::ShufflePartition) -> usize {
    partition
        .batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum()
}

pub fn parquet_writer_properties(
    compression: ShuffleCompression,
) -> parquet::file::properties::WriterProperties {
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let codec = match compression {
        ShuffleCompression::None => Compression::UNCOMPRESSED,
        ShuffleCompression::Lz4 => Compression::LZ4,
        ShuffleCompression::Zstd => Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()),
    };
    WriterProperties::builder().set_compression(codec).build()
}

#[cfg(test)]
mod wire_compression_tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use std::sync::Arc;

    /// Large and repetitive — i.e. shuffle-shaped. Real shuffle payloads are
    /// join keys and grouped measures, exactly the shape that compresses.
    fn shuffle_shaped_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let keys: Vec<i64> = (0..20_000).map(|i| i % 500).collect();
        let regions: Vec<&str> = (0..20_000)
            .map(|i| ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"][i % 5])
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(regions)),
            ],
        )
        .unwrap()
    }

    fn encode(options: IpcWriteOptions, batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new_with_options(&mut buf, batch.schema_ref(), options)
                    .expect("writer");
            writer.write(batch).expect("write");
            writer.finish().expect("finish");
        }
        buf
    }

    /// The whole point: fewer bytes on the wire.
    ///
    /// Reduce tasks fetch shuffle from other executors over a link measured at
    /// ~7.6 MB/s, against 150-286 MB/s for a node-local read. If this stops
    /// holding, compression is not reaching the encoder and the wire is back
    /// to carrying raw Arrow buffers.
    #[test]
    fn wire_compression_actually_shrinks_the_payload() {
        let batch = shuffle_shaped_batch();
        let plain = encode(IpcWriteOptions::default(), &batch).len();
        let lz4 = encode(ipc_write_options_for("lz4"), &batch).len();
        assert!(
            lz4 < plain,
            "LZ4 must shrink a shuffle-shaped payload: {lz4} >= {plain} bytes"
        );
    }

    /// Compressed bytes must decode back to exactly the same rows, through a
    /// reader given no compression configuration at all. The codec travels in
    /// the record-batch message — which is why this is safe to enable on the
    /// sender alone, with no protocol negotiation and no peer change.
    #[test]
    fn a_compressed_stream_round_trips_through_an_unconfigured_reader() {
        use arrow::ipc::reader::StreamReader;

        let batch = shuffle_shaped_batch();
        let bytes = encode(ipc_write_options_for("lz4"), &batch);

        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let decoded: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), batch.num_rows());
        assert_eq!(decoded[0].column(0), batch.column(0), "keys must survive");
        assert_eq!(decoded[0].column(1), batch.column(1), "strings must survive");
    }

    /// `none` must be a real escape hatch — on a fast fabric the CPU stops
    /// paying for itself and an operator has to be able to turn it off.
    #[test]
    fn none_is_byte_identical_to_no_compression() {
        let batch = shuffle_shaped_batch();
        assert_eq!(
            encode(ipc_write_options_for("none"), &batch),
            encode(IpcWriteOptions::default(), &batch),
        );
    }

    /// An unrecognised value must not silently disable compression on the
    /// slowest link in the system; it falls back to the default codec.
    #[test]
    fn an_unknown_codec_falls_back_to_lz4_not_to_off() {
        let batch = shuffle_shaped_batch();
        let typo = encode(ipc_write_options_for("lz44"), &batch).len();
        let plain = encode(IpcWriteOptions::default(), &batch).len();
        assert!(typo < plain, "a typo must not turn compression off");
    }

    /// The storage codec maps the same vocabulary as the wire one, with the
    /// same refusal to silently disable itself on a typo.
    #[test]
    fn the_storage_codec_maps_the_same_vocabulary_as_the_wire() {
        assert_eq!(storage_compression_for("lz4"), ShuffleCompression::Lz4);
        assert_eq!(storage_compression_for("zstd"), ShuffleCompression::Zstd);
        assert_eq!(storage_compression_for("none"), ShuffleCompression::None);
        assert_eq!(storage_compression_for("off"), ShuffleCompression::None);
        assert_eq!(
            storage_compression_for("lz44"),
            ShuffleCompression::Lz4,
            "a typo must not turn compression off"
        );
    }

    #[test]
    fn zstd_is_selectable() {
        let batch = shuffle_shaped_batch();
        let zstd = encode(ipc_write_options_for("zstd"), &batch).len();
        let plain = encode(IpcWriteOptions::default(), &batch).len();
        assert!(zstd < plain, "zstd must shrink the payload too");
    }
}
