use crate::error::io_err;
use crate::{CompressionCodec, ShuffleCompression, ShuffleError, ShufflePath, ShuffleResult};
use std::path::{Path, PathBuf};

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

    fn hash_path_for(final_path: &Path) -> PathBuf {
        let mut hash_path = final_path.as_os_str().to_owned();
        hash_path.push(".blake3");
        PathBuf::from(hash_path)
    }

    fn encode_hash(hash: &[u8; 32]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in hash {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    fn decode_hash(encoded: &[u8]) -> Option<[u8; 32]> {
        fn nibble(byte: u8) -> Option<u8> {
            match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            }
        }

        let encoded = match encoded {
            [body @ .., b'\n'] | [body @ .., b'\r'] => body,
            body => body,
        };
        if encoded.len() != 64 {
            return None;
        }

        let mut hash = [0u8; 32];
        for (idx, chunk) in encoded.chunks_exact(2).enumerate() {
            let high = nibble(chunk[0])?;
            let low = nibble(chunk[1])?;
            hash[idx] = (high << 4) | low;
        }
        Some(hash)
    }

    fn compute_hash(data: &[u8]) -> [u8; 32] {
        crate::disk_store::blake3_hash(data)
    }

    async fn fsync_dir(path: PathBuf) -> ShuffleResult<()> {
        tokio::task::spawn_blocking(move || {
            let dir = std::fs::File::open(&path)
                .map_err(|e| io_err(format!("failed to open shuffle dir for fsync: {e}")))?;
            dir.sync_all()
                .map_err(|e| io_err(format!("failed to fsync shuffle dir: {e}")))
        })
        .await
        .map_err(|e| io_err(format!("spawn_blocking join error: {e}")))?
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
        crate::validate_safe_id(&path.job_id, "job_id")?;
        crate::validate_safe_id(&path.stage_id, "stage_id")?;
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
        let staging_hash = Self::hash_path_for(&staging);
        let final_hash = Self::hash_path_for(&final_path);
        let hash = Self::compute_hash(&payload);

        // Create parent directories.
        if let Some(parent) = staging.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&staging, &payload).await?;
        tokio::fs::File::open(&staging).await?.sync_all().await?;
        tokio::fs::write(&staging_hash, Self::encode_hash(&hash)).await?;
        tokio::fs::File::open(&staging_hash)
            .await?
            .sync_all()
            .await?;
        tokio::fs::rename(&staging, &final_path).await?;
        tokio::fs::rename(&staging_hash, &final_hash).await?;
        if let Some(parent) = final_path.parent() {
            Self::fsync_dir(parent.to_path_buf()).await?;
        }
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
        crate::validate_safe_id(&path.job_id, "job_id")?;
        crate::validate_safe_id(&path.stage_id, "stage_id")?;
        let final_path = self.base_dir.join(path.final_name());
        let final_hash = Self::hash_path_for(&final_path);
        match tokio::fs::read(&final_path).await {
            Ok(bytes) => {
                let key = (&path.job_id, &path.stage_id, path.partition_id);
                let expected_hash_bytes = tokio::fs::read(&final_hash).await.map_err(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        ShuffleError::ContentHashMismatch {
                            partition: format!("{:?}", key),
                            expected: "persisted blake3 sidecar".to_string(),
                            actual: "missing".to_string(),
                        }
                    } else {
                        io_err(format!(
                            "failed to read shuffle hash sidecar '{}': {e}",
                            final_hash.display()
                        ))
                    }
                })?;
                let expected_hash = Self::decode_hash(&expected_hash_bytes).ok_or_else(|| {
                    ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: "64 lowercase hex blake3 digest".to_string(),
                        actual: String::from_utf8_lossy(&expected_hash_bytes).into_owned(),
                    }
                })?;
                let actual_hash = Self::compute_hash(&bytes);
                if actual_hash != expected_hash {
                    return Err(ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: Self::encode_hash(&expected_hash),
                        actual: Self::encode_hash(&actual_hash),
                    });
                }

                // Validate and parse the KSH magic header.
                if bytes.len() < 4 {
                    return Err(io_err(format!(
                        "shuffle file too short to contain header: {}",
                        final_path.display()
                    )));
                }
                if bytes[0] != 0x4B || bytes[1] != 0x53 || bytes[2] != 0x48 {
                    return Err(io_err(format!(
                        "invalid shuffle file magic bytes in: {}",
                        final_path.display()
                    )));
                }
                let codec = match bytes[3] {
                    0x00 => ShuffleCompression::None,
                    0x01 => ShuffleCompression::Lz4,
                    0x02 => ShuffleCompression::Zstd,
                    other => {
                        return Err(io_err(format!(
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
            Err(e) => Err(crate::error::io_err(e.to_string())),
        }
    }

    /// Delete the entire directory for `job_id`.
    ///
    /// No-ops if the directory does not exist.
    pub async fn delete_job(&self, job_id: &str) -> ShuffleResult<()> {
        crate::validate_safe_id(job_id, "job_id")?;
        let dir = self.base_dir.join(job_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(crate::error::io_err(e.to_string())),
        }
    }
}
