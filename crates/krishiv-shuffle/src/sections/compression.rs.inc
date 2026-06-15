// ── CompressionCodec ──────────────────────────────────────────────────

#[test]
fn compression_codec_default_is_none() {
    assert_eq!(CompressionCodec::default(), CompressionCodec::None);
}

#[test]
fn local_store_default_compression_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    assert_eq!(store.compression(), CompressionCodec::None);
}

#[test]
fn local_store_with_compression_lz4() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
    assert_eq!(store.compression(), CompressionCodec::Lz4);
}

// ── Compression round-trip tests ──────────────────────────────────────

#[test]
fn compression_codec_none_round_trip() {
    let data = b"hello shuffle world";
    let compressed = CompressionCodec::None.compress(data).unwrap();
    let decompressed = CompressionCodec::None.decompress(&compressed).unwrap();
    assert_eq!(&decompressed, data);
}

#[test]
fn compression_codec_lz4_round_trip() {
    let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    let compressed = CompressionCodec::Lz4.compress(&data).unwrap();
    let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
    assert_eq!(decompressed, data, "LZ4 round-trip must be byte-exact");
}

#[test]
fn compression_codec_zstd_round_trip() {
    let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    let compressed = CompressionCodec::Zstd.compress(&data).unwrap();
    let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
    assert_eq!(decompressed, data, "Zstd round-trip must be byte-exact");
}

#[tokio::test]
async fn local_store_lz4_write_read_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
    let path = ShufflePath::new("job-1", "stage-1", 0);
    let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
    store.write_partition(&path, &data).await.unwrap();
    let read_back = store.read_partition(&path).await.unwrap();
    assert_eq!(
        read_back, data,
        "LZ4 write/read round-trip must be byte-exact"
    );
}

#[tokio::test]
async fn local_store_zstd_write_read_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Zstd);
    let path = ShufflePath::new("job-1", "stage-1", 0);
    let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
    store.write_partition(&path, &data).await.unwrap();
    let read_back = store.read_partition(&path).await.unwrap();
    assert_eq!(
        read_back, data,
        "Zstd write/read round-trip must be byte-exact"
    );
}

/// GAP-SH-02: Verify that the header codec byte governs decompression even
/// when the reader store has a different codec configured.
///
/// Write a partition with `CompressionCodec::None` (header byte = 0x00),
/// then read it back through a store configured with `CompressionCodec::Lz4`.
/// The reader must use the None codec recorded in the header and return the
/// original uncompressed bytes without corruption.
#[tokio::test]
async fn shuffle_codec_header_mismatch_detected() {
    let dir = tempfile::tempdir().unwrap();
    let write_store =
        LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::None);
    let read_store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);

    let path = ShufflePath::new("job-mismatch", "stage-0", 0);
    let data: Vec<u8> = (0u8..=127).cycle().take(256).collect();

    // Write with None codec — header byte 0x00 is embedded in the file.
    write_store.write_partition(&path, &data).await.unwrap();

    // Read with a store configured for Lz4 — must use the header's None codec,
    // not Lz4, and return the original bytes without error or corruption.
    let read_back = read_store.read_partition(&path).await.unwrap();
    assert_eq!(
        read_back, data,
        "read_partition must use the codec from the file header, not the store config"
    );
}

