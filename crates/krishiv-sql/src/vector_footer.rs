#![forbid(unsafe_code)]
//! IVF index persistence in Parquet footer user metadata — Phase 36 G19,
//! the write half of the transparent-acceleration wire-in.
//!
//! The embed tick (or any writer that lands an embedding column) calls
//! [`write_parquet_with_vector_index`] to store the file's rows AND a
//! serialized [`IvfIndex`] over its embedding column in the same file:
//! the index rides in the footer's key/value user metadata under
//! [`vector_index_meta_key`], base64-encoded. Standard Parquet readers
//! ignore unknown footer keys, so the file stays a perfectly ordinary
//! Parquet file — the index costs ~0.3% and can never break a reader.
//!
//! Freshness is by construction, not by bookkeeping: a Parquet file is
//! immutable, so a footer index describes its file forever. New data means
//! a new file (with its own footer index); there is no index-maintenance
//! path to get wrong.
//!
//! [`read_vector_index_footer`] is the read half: `Some(index)` only when
//! the key is present, decodes, and parses. A foreign file, a garbage
//! value, or a truncated payload all degrade to `None` — the caller falls
//! back to a brute-force scan, never a failed query.

use std::path::Path;

use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::FileReader;
use datafusion::parquet::file::serialized_reader::SerializedFileReader;

use crate::vector_index::IvfIndex;
use crate::vector_metric::VectorMetric;

/// Footer user-metadata key carrying the IVF index for `embedding_col`.
/// Versioned in the key (`.v1.`) so an incompatible layout gets a new key
/// and old readers see "absent", not "corrupt".
pub fn vector_index_meta_key(embedding_col: &str) -> String {
    format!("krishiv.vector.ivf.v1.{embedding_col}")
}

/// Write `batches` to a Parquet file at `path` with an IVF index over
/// `embedding_col` embedded in the footer user metadata. Returns the index
/// that was written. `nlist == 0` auto-sizes to `~sqrt(rows)`.
///
/// The data written is byte-identical to a plain Parquet write of the same
/// batches — the index lives only in the footer key/value metadata.
pub fn write_parquet_with_vector_index(
    path: &Path,
    batches: &[RecordBatch],
    embedding_col: &str,
    metric: VectorMetric,
    nlist: usize,
) -> Result<IvfIndex, String> {
    let first = batches
        .first()
        .ok_or_else(|| "vector footer write: no batches".to_string())?;
    let schema = first.schema();
    let col_idx = schema
        .index_of(embedding_col)
        .map_err(|e| format!("vector footer write: column '{embedding_col}': {e}"))?;

    let (rows, dim) =
        crate::vector_search::concat_embedding_batches(batches, col_idx, embedding_col)
            .map_err(|e| format!("vector footer write: {e}"))?;
    if dim == 0 {
        return Err("vector footer write: no rows to index".into());
    }
    let n = rows.len() / dim;
    let nlist = if nlist == 0 {
        (n as f64).sqrt().ceil() as usize
    } else {
        nlist
    };
    let index = IvfIndex::build(&rows, dim, nlist, 12, metric)?;

    let encoded = base64::engine::general_purpose::STANDARD.encode(index.to_bytes());
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue {
            key: vector_index_meta_key(embedding_col),
            value: Some(encoded),
        }]))
        .build();

    let file = std::fs::File::create(path)
        .map_err(|e| format!("vector footer write: create {}: {e}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("vector footer write: open writer: {e}"))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("vector footer write: write batch: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("vector footer write: close: {e}"))?;
    Ok(index)
}

/// Read the IVF index for `embedding_col` from a Parquet file's footer.
///
/// `None` — never an error — when the file is unreadable, carries no index
/// for this column, or the payload fails to decode or parse: every degraded
/// case means "scan without an index", which is always correct.
pub fn read_vector_index_footer(path: &Path, embedding_col: &str) -> Option<IvfIndex> {
    let file = std::fs::File::open(path).ok()?;
    let reader = SerializedFileReader::new(file).ok()?;
    let key = vector_index_meta_key(embedding_col);
    let kv = reader.metadata().file_metadata().key_value_metadata()?;
    let value = kv
        .iter()
        .find(|entry| entry.key == key)
        .and_then(|entry| entry.value.as_deref())?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    IvfIndex::from_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, FixedSizeListBuilder, Float32Builder, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn embedding_batch(n: usize) -> RecordBatch {
        let mut emb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        let mut ids = Vec::new();
        for i in 0..n {
            let base = (i % 6) as f32 * 10.0 + (i / 6) as f32 * 0.1;
            for v in [base, base + 1.0, base - 1.0, base + 0.5] {
                emb.values().append_value(v);
            }
            emb.append(true);
            ids.push(format!("row-{i:03}"));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "emb",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));
        let emb: ArrayRef = Arc::new(emb.finish());
        let ids: ArrayRef = Arc::new(StringArray::from(ids));
        RecordBatch::try_new(schema, vec![ids, emb]).unwrap()
    }

    #[test]
    fn footer_roundtrip_returns_the_written_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let batch = embedding_batch(60);
        let written =
            write_parquet_with_vector_index(&path, &[batch], "emb", VectorMetric::Cosine, 6)
                .unwrap();
        let read = read_vector_index_footer(&path, "emb").expect("index present");
        assert_eq!(written, read, "footer roundtrip is exact");
        assert_eq!(read.len(), 60, "every row indexed");
        // The wrong column name is absent, not an error.
        assert!(read_vector_index_footer(&path, "other").is_none());
    }

    #[test]
    fn plain_parquet_and_garbage_payload_degrade_to_none() {
        let dir = tempfile::tempdir().unwrap();

        // A plain file with no footer key.
        let plain = dir.path().join("plain.parquet");
        let batch = embedding_batch(10);
        let file = std::fs::File::create(&plain).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        assert!(read_vector_index_footer(&plain, "emb").is_none());

        // The right key with a non-index payload (foreign writer).
        let garbage = dir.path().join("garbage.parquet");
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue {
                key: vector_index_meta_key("emb"),
                value: Some("bm90IGFuIGl2ZiBpbmRleA==".into()), // "not an ivf index"
            }]))
            .build();
        let file = std::fs::File::create(&garbage).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        assert!(read_vector_index_footer(&garbage, "emb").is_none());

        // Not even a parquet file.
        let text = dir.path().join("notes.txt");
        std::fs::write(&text, "hello").unwrap();
        assert!(read_vector_index_footer(&text, "emb").is_none());
    }

    #[test]
    fn indexed_file_reads_normally_as_parquet() {
        // The index must never change what a standard reader sees.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let batch = embedding_batch(25);
        write_parquet_with_vector_index(
            &path,
            std::slice::from_ref(&batch),
            "emb",
            VectorMetric::L2,
            0,
        )
        .unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader =
            datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                file,
            )
            .unwrap()
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 25, "footer metadata does not alter the data pages");
    }
}
