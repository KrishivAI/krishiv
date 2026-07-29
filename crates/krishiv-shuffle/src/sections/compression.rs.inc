// ── CompressionCodec ──────────────────────────────────────────────────

#[test]
fn compression_codec_default_is_none() {
    assert_eq!(CompressionCodec::default(), CompressionCodec::None);
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

/// GAP-SH-02: the codec recorded in the file governs decompression, not the
/// codec the *reader* happens to be configured with.
///
/// This assertion previously ran against `LocalShuffleStore`, a byte-store with
/// its own "KSH" magic-header format that no production path ever wrote or
/// read — so the property was proven for a phantom format and left unproven for
/// the two that ship. It is restated here against `LocalDiskShuffleStore`,
/// where Parquet carries the codec in file metadata. (`compression.rs` proves
/// the same for the object store's Arrow IPC, where the codec rides in each
/// record-batch message.)
///
/// This is the property that makes the storage-compression default safe to
/// change under partitions already on disk: a reader never has to be told.
#[tokio::test]
async fn the_codec_in_the_file_governs_the_read_not_the_readers_config() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let values: Vec<i64> = (0..4_000).map(|i| i % 64).collect();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
    let id = PartitionId {
        job_id: "job-codec-header".to_owned(),
        stage_id: "s0".to_owned(),
        partition: 0,
    };

    let writer = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::Zstd);
    writer
        .write_partition(
            ShufflePartition {
                id: id.clone(),
                schema: schema.clone(),
                batches: vec![batch.clone()],
            },
            1,
        )
        .await
        .unwrap();

    // A reader configured for a *different* codec must still read it.
    let reader = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::None);
    let read = reader.read_partition(&id).await.unwrap().unwrap();

    let actual = arrow::compute::concat_batches(&read.schema, &read.batches).unwrap();
    assert_eq!(
        actual.column(0),
        batch.column(0),
        "the reader must decode using the file's codec, not its own configuration"
    );
}
