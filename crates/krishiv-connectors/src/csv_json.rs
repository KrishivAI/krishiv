//! CSV and NDJSON sources with schema inference.
//! # CSV source
//!
//! ```text
//! CsvSource::open(reader, CsvOptions::default()) → AvroSource-like iterator
//! ```
//!
//! # JSON source (newline-delimited JSON, NDJSON)
//!
//! ```text
//! NdjsonSource::open(reader, NdjsonOptions::default())
//! ```

use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::csv::reader::Format;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::json::reader::ReaderBuilder as JsonReaderBuilder;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};

// ── CSV ───────────────────────────────────────────────────────────────────────

/// Configuration for the CSV reader.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// Whether the first row is a header row. Default: `true`.
    pub has_header: bool,
    /// Field delimiter byte. Default: `b','`.
    pub delimiter: u8,
    /// Number of rows used for schema inference when no explicit schema is
    /// provided. Default: 100.
    pub infer_rows: usize,
    /// Maximum rows per [`RecordBatch`]. Default: 1024.
    pub batch_size: usize,
    /// Optional explicit schema — skips inference when set.
    pub schema: Option<SchemaRef>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            infer_rows: 100,
            batch_size: 1024,
            schema: None,
        }
    }
}

impl CsvOptions {
    pub fn with_has_header(mut self, v: bool) -> Self {
        self.has_header = v;
        self
    }
    pub fn with_delimiter(mut self, v: u8) -> Self {
        self.delimiter = v;
        self
    }
    pub fn with_infer_rows(mut self, v: usize) -> Self {
        self.infer_rows = v;
        self
    }
    pub fn with_batch_size(mut self, v: usize) -> Self {
        self.batch_size = v;
        self
    }
    pub fn with_schema(mut self, s: SchemaRef) -> Self {
        self.schema = Some(s);
        self
    }
}

/// Reads CSV data as Arrow [`RecordBatch`] values.
///
/// Supports schema inference (first *N* rows scanned) or an explicit schema.
/// Backed by `arrow-csv`; all rows are read and buffered on construction.
pub struct CsvSource {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    cursor: usize,
}

impl CsvSource {
    /// Open a CSV source from `reader`.
    ///
    /// If `opts.schema` is `None`, schema is inferred from the first
    /// `opts.infer_rows` rows; the reader is then rewound and parsed fully.
    pub fn open<R: Read + Seek>(mut reader: R, opts: CsvOptions) -> ConnectorResult<Self> {
        let schema = match opts.schema {
            Some(s) => s,
            None => {
                let fmt = Format::default()
                    .with_header(opts.has_header)
                    .with_delimiter(opts.delimiter);
                let (inferred, _) = fmt
                    .infer_schema(&mut reader, Some(opts.infer_rows))
                    .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
                reader
                    .seek(SeekFrom::Start(0))
                    .map_err(ConnectorError::Io)?;
                Arc::new(inferred)
            }
        };

        let reader_built = CsvReaderBuilder::new(schema.clone())
            .with_header(opts.has_header)
            .with_delimiter(opts.delimiter)
            .with_batch_size(opts.batch_size)
            .build(reader)
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        let mut batches = Vec::new();
        for result in reader_built {
            let batch =
                result.map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            batches.push(batch);
        }

        Ok(Self {
            schema,
            batches,
            cursor: 0,
        })
    }

    /// Arrow schema (inferred or explicit).
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Total number of buffered batches.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Total row count across all batches.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Read the next batch.  Returns `Ok(None)` when exhausted.
    pub fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.cursor >= self.batches.len() {
            return Ok(None);
        }
        let batch = self
            .batches
            .get(self.cursor)
            .ok_or_else(|| ConnectorError::Protocol {
                message: "cursor out of range".into(),
            })?
            .clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    /// Reset to the beginning (replayable).
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    /// Connector capabilities.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::default()
            .with_bounded()
            .with_rewindable()
    }
}

// ── Newline-delimited JSON (NDJSON) ───────────────────────────────────────────

/// Configuration for the NDJSON reader.
#[derive(Debug, Clone)]
pub struct NdjsonOptions {
    /// Number of rows used for schema inference. Default: 100.
    pub infer_rows: usize,
    /// Maximum rows per [`RecordBatch`]. Default: 1024.
    pub batch_size: usize,
    /// Optional explicit schema — skips inference when set.
    pub schema: Option<SchemaRef>,
}

impl Default for NdjsonOptions {
    fn default() -> Self {
        Self {
            infer_rows: 100,
            batch_size: 1024,
            schema: None,
        }
    }
}

impl NdjsonOptions {
    pub fn with_infer_rows(mut self, v: usize) -> Self {
        self.infer_rows = v;
        self
    }
    pub fn with_batch_size(mut self, v: usize) -> Self {
        self.batch_size = v;
        self
    }
    pub fn with_schema(mut self, s: SchemaRef) -> Self {
        self.schema = Some(s);
        self
    }
}

/// Reads newline-delimited JSON (NDJSON) as Arrow [`RecordBatch`] values.
///
/// Backed by `arrow-json`. Schema inference reads the first `infer_rows`
/// lines; the source then rewinds and parses the full input.
pub struct NdjsonSource {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    cursor: usize,
}

impl NdjsonSource {
    /// Open an NDJSON source from `data` bytes.
    ///
    /// The bytes are read twice when inference is needed (once to infer schema,
    /// once to parse). Pass an explicit schema to avoid the double read.
    pub fn open(data: Vec<u8>, opts: NdjsonOptions) -> ConnectorResult<Self> {
        let schema = match opts.schema {
            Some(s) => s,
            None => {
                let mut cursor = Cursor::new(&data);
                let buf = BufReader::new(&mut cursor);
                let (inferred, _) = infer_json_schema(buf, Some(opts.infer_rows))
                    .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
                Arc::new(inferred)
            }
        };

        let cursor = Cursor::new(data);
        let buf = BufReader::new(cursor);
        let reader = JsonReaderBuilder::new(schema.clone())
            .with_batch_size(opts.batch_size)
            .build(buf)
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        let mut batches = Vec::new();
        for result in reader {
            let batch =
                result.map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            batches.push(batch);
        }

        Ok(Self {
            schema,
            batches,
            cursor: 0,
        })
    }

    /// Arrow schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Total row count.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Read the next batch.  Returns `Ok(None)` when exhausted.
    pub fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.cursor >= self.batches.len() {
            return Ok(None);
        }
        let batch = self
            .batches
            .get(self.cursor)
            .ok_or_else(|| ConnectorError::Protocol {
                message: "cursor out of range".into(),
            })?
            .clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    /// Reset to the beginning.
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    /// Connector capabilities.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::default()
            .with_bounded()
            .with_rewindable()
    }
}

// ── Infer-only helpers (public API) ───────────────────────────────────────────

/// Infer an Arrow schema from CSV bytes (header row + first `max_rows` rows).
pub fn infer_csv_schema(data: &[u8], has_header: bool, max_rows: usize) -> ConnectorResult<Schema> {
    let fmt = Format::default().with_header(has_header);
    let mut cursor = Cursor::new(data);
    let (schema, _) = fmt
        .infer_schema(&mut cursor, Some(max_rows))
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
    Ok(schema)
}

/// Infer an Arrow schema from NDJSON bytes (first `max_rows` lines).
pub fn infer_ndjson_schema(data: &[u8], max_rows: usize) -> ConnectorResult<Schema> {
    let cursor = Cursor::new(data);
    let buf = BufReader::new(cursor);
    let (schema, _) = infer_json_schema(buf, Some(max_rows))
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
    Ok(schema)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::io::Cursor;

    // ── CSV ───────────────────────────────────────────────────────────────────

    fn csv_bytes() -> &'static [u8] {
        b"id,name,score\n1,alice,9.5\n2,bob,7.0\n3,charlie,8.1\n"
    }

    #[test]
    fn csv_infers_schema_and_reads() {
        let mut src = CsvSource::open(Cursor::new(csv_bytes()), CsvOptions::default()).unwrap();
        let schema = src.schema().clone();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn csv_with_explicit_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let mut src = CsvSource::open(
            Cursor::new(csv_bytes()),
            CsvOptions::default().with_schema(schema.clone()),
        )
        .unwrap();
        assert_eq!(src.schema(), &schema);
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn csv_exhausts_to_none() {
        let mut src = CsvSource::open(Cursor::new(csv_bytes()), CsvOptions::default()).unwrap();
        // Exhaust all batches.
        while src.read_batch().unwrap().is_some() {}
        assert!(src.read_batch().unwrap().is_none());
    }

    #[test]
    fn csv_reset_replays() {
        let mut src = CsvSource::open(Cursor::new(csv_bytes()), CsvOptions::default()).unwrap();
        while src.read_batch().unwrap().is_some() {}
        src.reset();
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn csv_total_rows() {
        let src = CsvSource::open(Cursor::new(csv_bytes()), CsvOptions::default()).unwrap();
        assert_eq!(src.total_rows(), 3);
    }

    #[test]
    fn csv_no_header() {
        let data = b"1,alice\n2,bob\n";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut src = CsvSource::open(
            Cursor::new(data as &[u8]),
            CsvOptions::default()
                .with_has_header(false)
                .with_schema(schema),
        )
        .unwrap();
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn csv_capabilities() {
        let src = CsvSource::open(Cursor::new(csv_bytes()), CsvOptions::default()).unwrap();
        let caps = src.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
    }

    #[test]
    fn infer_csv_schema_fn() {
        let schema = infer_csv_schema(csv_bytes(), true, 100).unwrap();
        assert_eq!(schema.fields().len(), 3);
    }

    // ── NDJSON ────────────────────────────────────────────────────────────────

    fn ndjson_bytes() -> Vec<u8> {
        b"{\"id\":1,\"name\":\"alice\",\"score\":9.5}\n\
          {\"id\":2,\"name\":\"bob\",\"score\":7.0}\n\
          {\"id\":3,\"name\":\"charlie\",\"score\":8.1}\n"
            .to_vec()
    }

    #[test]
    fn ndjson_infers_schema_and_reads() {
        let mut src = NdjsonSource::open(ndjson_bytes(), NdjsonOptions::default()).unwrap();
        let schema = src.schema().clone();
        assert_eq!(schema.fields().len(), 3);
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn ndjson_with_explicit_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let mut src = NdjsonSource::open(
            ndjson_bytes(),
            NdjsonOptions::default().with_schema(schema.clone()),
        )
        .unwrap();
        assert_eq!(src.schema(), &schema);
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn ndjson_reset_replays() {
        let mut src = NdjsonSource::open(ndjson_bytes(), NdjsonOptions::default()).unwrap();
        while src.read_batch().unwrap().is_some() {}
        src.reset();
        assert_eq!(src.read_batch().unwrap().unwrap().num_rows(), 3);
    }

    #[test]
    fn ndjson_total_rows() {
        let src = NdjsonSource::open(ndjson_bytes(), NdjsonOptions::default()).unwrap();
        assert_eq!(src.total_rows(), 3);
    }

    #[test]
    fn ndjson_capabilities() {
        let src = NdjsonSource::open(ndjson_bytes(), NdjsonOptions::default()).unwrap();
        let caps = src.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
    }

    #[test]
    fn infer_ndjson_schema_fn() {
        let schema = infer_ndjson_schema(&ndjson_bytes(), 100).unwrap();
        assert_eq!(schema.fields().len(), 3);
    }
}
