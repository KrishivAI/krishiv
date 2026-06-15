// ── CompressionCodec: Arrow IPC roundtrip ────────────────────────────

#[test]
fn compression_lz4_arrow_ipc_roundtrip() {
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .unwrap();

    // Serialize to IPC bytes.
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let compressed = CompressionCodec::Lz4.compress(&ipc_buf).unwrap();
    let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
    assert_eq!(decompressed, ipc_buf);

    // Deserialize and verify values.
    let cursor = std::io::Cursor::new(&decompressed);
    let mut reader = StreamReader::try_new(cursor, None).unwrap();
    let rb = reader.next().unwrap().unwrap();
    assert_eq!(rb.num_rows(), 3);
    let ids = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[test]
fn compression_zstd_arrow_ipc_roundtrip() {
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
        ],
    )
    .unwrap();

    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let compressed = CompressionCodec::Zstd.compress(&ipc_buf).unwrap();
    let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
    assert_eq!(decompressed, ipc_buf);

    let cursor = std::io::Cursor::new(&decompressed);
    let mut reader = StreamReader::try_new(cursor, None).unwrap();
    let rb = reader.next().unwrap().unwrap();
    assert_eq!(rb.num_rows(), 3);
    let names = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "alpha");
    assert_eq!(names.value(1), "beta");
    assert_eq!(names.value(2), "gamma");
}

#[test]
fn compression_large_zstd_roundtrip() {
    let data: Vec<u8> = (0u32..8192).flat_map(|v| v.to_le_bytes()).collect();
    let compressed = CompressionCodec::Zstd.compress(&data).unwrap();
    assert!(
        compressed.len() < data.len(),
        "Zstd should compress 32KB of sequential u32s"
    );
    let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn compression_large_lz4_roundtrip() {
    let data: Vec<u8> = (0u32..8192).flat_map(|v| v.to_le_bytes()).collect();
    let compressed = CompressionCodec::Lz4.compress(&data).unwrap();
    let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

