use crate::{CompressionCodec, ShuffleCompression, ShuffleError, ShufflePath, ShuffleResult};
use std::path::PathBuf;

/// Local-disk shuffle store.
///
/// Writes each partition to a `.tmp` staging file and then atomically renames
/// it to the final `.ipc` path, matching the invariant from the shuffle
/// deployment model: a partition is either fully available or absent.
#[derive(Debug, Clone)]
pub struct LocalShuffleStore {
    base_dir: PathBuf,
    compression: CompressionCodec,
}

impl LocalShuffleStore {
    /// Create a new store rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            compression: CompressionCodec::None,
        }
    }

    /// Set the compression codec for this store.
    #[must_use]
    pub fn with_compression(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    /// Return the compression codec in use.
    pub fn compression(&self) -> CompressionCodec {
        self.compression
    }

    /// Write `data` to disk for the given partition, applying the configured
    /// compression codec before writing.
    ///
    /// 1. Compresses `data` with the configured codec.
    /// 2. Prepends a 4-byte magic header: `[0x4B, 0x53, 0x48, codec_byte]`
    ///    where `codec_byte` is `0x00` for None, `0x01` for Lz4, `0x02` for Zstd.
    /// 3. Creates `{base_dir}/{staging_name}` (including parent dirs).
    /// 4. Writes the header + compressed bytes.
    /// 5. Atomically renames staging path → final path.
    pub async fn write_partition(&self, path: &ShufflePath, data: &[u8]) -> ShuffleResult<()> {
        let compressed = self.compression.compress(data)?;
        let codec_byte = match self.compression {
            ShuffleCompression::None => 0x00u8,
            ShuffleCompression::Lz4 => 0x01u8,
            ShuffleCompression::Zstd => 0x02u8,
        };
        // Prepend KSH magic header: [0x4B, 0x53, 0x48, codec_byte]
        let mut payload = Vec::with_capacity(4 + compressed.len());
        payload.extend_from_slice(&[0x4B, 0x53, 0x48, codec_byte]);
        payload.extend_from_slice(&compressed);

        let staging = self.base_dir.join(path.staging_name());
        let final_path = self.base_dir.join(path.final_name());

        // Create parent directories.
        if let Some(parent) = staging.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&staging, &payload).await?;
        tokio::fs::rename(&staging, &final_path).await?;
        Ok(())
    }

    /// Read the bytes for a partition, decompressing with the codec indicated
    /// in the file's magic header (not the current store config).
    ///
    /// File format: `[0x4B, 0x53, 0x48, codec_byte] ++ compressed_data`
    /// - Magic bytes `0x4B 0x53 0x48` = "KSH"
    /// - `codec_byte`: `0x00` = None, `0x01` = Lz4, `0x02` = Zstd
    ///
    /// Returns `PartitionNotFound` if the final path does not exist.
    /// Returns `Io` error if the magic bytes are invalid or the codec byte is unknown.
    pub async fn read_partition(&self, path: &ShufflePath) -> ShuffleResult<Vec<u8>> {
        let final_path = self.base_dir.join(path.final_name());
        match tokio::fs::read(&final_path).await {
            Ok(bytes) => {
                // Validate and parse the KSH magic header.
                if bytes.len() < 4 {
                    return Err(ShuffleError::Io(format!(
                        "shuffle file too short to contain header: {}",
                        final_path.display()
                    )));
                }
                if bytes[0] != 0x4B || bytes[1] != 0x53 || bytes[2] != 0x48 {
                    return Err(ShuffleError::Io(format!(
                        "invalid shuffle file magic bytes in: {}",
                        final_path.display()
                    )));
                }
                let codec = match bytes[3] {
                    0x00 => ShuffleCompression::None,
                    0x01 => ShuffleCompression::Lz4,
                    0x02 => ShuffleCompression::Zstd,
                    other => {
                        return Err(ShuffleError::Io(format!(
                            "unknown shuffle codec byte 0x{other:02X} in: {}",
                            final_path.display()
                        )));
                    }
                };
                codec.decompress(&bytes[4..])
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ShuffleError::PartitionNotFound {
                    path: final_path.display().to_string(),
                })
            }
            Err(e) => Err(ShuffleError::Io(e.to_string())),
        }
    }

    /// Delete the entire directory for `job_id`.
    ///
    /// No-ops if the directory does not exist.
    pub async fn delete_job(&self, job_id: &str) -> ShuffleResult<()> {
        let dir = self.base_dir.join(job_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ShuffleError::Io(e.to_string())),
        }
    }
}
