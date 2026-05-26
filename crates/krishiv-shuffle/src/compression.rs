use crate::{ShuffleError, ShuffleResult};
use arrow::record_batch::RecordBatch;

/// Compression algorithm for shuffle block data.
///
/// Used in [`ShuffleWriteConfig`] and [`ShuffleReadConfig`] to specify
/// how shuffle blocks are compressed on write and decompressed on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShuffleCompression {
    /// No compression (default).
    #[default]
    None,
    /// LZ4 frame compression via `lz4_flex`.
    Lz4,
    /// Zstandard compression via `zstd`.
    Zstd,
}

/// Compression codec — type alias for [`ShuffleCompression`].
pub type CompressionCodec = ShuffleCompression;

impl ShuffleCompression {
    /// Compress `data` using this codec. Returns the compressed bytes.
    pub fn compress(self, data: &[u8]) -> ShuffleResult<Vec<u8>> {
        match self {
            ShuffleCompression::None => Ok(data.to_vec()),
            ShuffleCompression::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            ShuffleCompression::Zstd => {
                zstd::encode_all(data, 0).map_err(|e| ShuffleError::Io(e.to_string()))
            }
        }
    }

    /// Decompress `data` using this codec. Returns the original bytes.
    pub fn decompress(self, data: &[u8]) -> ShuffleResult<Vec<u8>> {
        match self {
            ShuffleCompression::None => Ok(data.to_vec()),
            ShuffleCompression::Lz4 => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| ShuffleError::Io(e.to_string())),
            ShuffleCompression::Zstd => {
                zstd::decode_all(data).map_err(|e| ShuffleError::Io(e.to_string()))
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
