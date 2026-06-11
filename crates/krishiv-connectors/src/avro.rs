//! E6.1 — Avro file source and sink.
//!
//! Reads and writes [Apache Avro] container files as Arrow [`RecordBatch`]
//! streams using the `apache-avro` crate.
//!
//! # Type mapping
//!
//! | Avro type          | Arrow type    |
//! |--------------------|---------------|
//! | `null`             | `Null`        |
//! | `boolean`          | `Boolean`     |
//! | `int`              | `Int32`       |
//! | `long`             | `Int64`       |
//! | `float`            | `Float32`     |
//! | `double`           | `Float64`     |
//! | `bytes` / `fixed`  | `Binary`      |
//! | `string` / `enum`  | `Utf8`        |
//! | `union [null, T]`  | nullable T    |
//!
//! [Apache Avro]: https://avro.apache.org/

use std::io::{Read, Write};
use std::sync::Arc;

use apache_avro::{
    Reader as AvroReader, Schema as AvroSchema, Writer as AvroWriter,
    types::Value as AvroValue,
};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    NullArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};

// ── Schema conversion: Avro → Arrow ───────────────────────────────────────────

/// Convert an Avro top-level record schema to an Arrow [`Schema`].
pub fn avro_schema_to_arrow(schema: &AvroSchema) -> ConnectorResult<Schema> {
    match schema {
        AvroSchema::Record(rec) => {
            let fields: ConnectorResult<Vec<Field>> = rec
                .fields
                .iter()
                .map(|f| avro_field_to_arrow(&f.name, &f.schema))
                .collect();
            Ok(Schema::new(fields?))
        }
        other => Err(ConnectorError::Io(std::io::Error::other(format!(
            "avro: top-level schema must be a record, got {other:?}"
        )))),
    }
}

fn avro_field_to_arrow(name: &str, schema: &AvroSchema) -> ConnectorResult<Field> {
    let (dt, nullable) = avro_schema_to_arrow_type(schema)?;
    Ok(Field::new(name, dt, nullable))
}

fn avro_schema_to_arrow_type(schema: &AvroSchema) -> ConnectorResult<(DataType, bool)> {
    match schema {
        AvroSchema::Null => Ok((DataType::Null, true)),
        AvroSchema::Boolean => Ok((DataType::Boolean, false)),
        AvroSchema::Int => Ok((DataType::Int32, false)),
        AvroSchema::Long => Ok((DataType::Int64, false)),
        AvroSchema::Float => Ok((DataType::Float32, false)),
        AvroSchema::Double => Ok((DataType::Float64, false)),
        AvroSchema::Bytes | AvroSchema::Fixed(_) => Ok((DataType::Binary, false)),
        AvroSchema::String | AvroSchema::Enum(_) => Ok((DataType::Utf8, false)),
        AvroSchema::Union(u) => {
            let variants = u.variants();
            let non_null: Vec<_> =
                variants.iter().filter(|s| !matches!(s, AvroSchema::Null)).collect();
            if non_null.len() == 1 && variants.len() == 2 {
                let (dt, _) = avro_schema_to_arrow_type(non_null[0])?;
                Ok((dt, true))
            } else {
                Ok((DataType::Utf8, true))
            }
        }
        AvroSchema::Record(rec) => {
            let fields: ConnectorResult<Vec<Field>> = rec
                .fields
                .iter()
                .map(|f| avro_field_to_arrow(&f.name, &f.schema))
                .collect();
            Ok((DataType::Struct(fields?.into()), false))
        }
        AvroSchema::Array(arr) => {
            let (item_type, nullable) = avro_schema_to_arrow_type(&arr.items)?;
            Ok((
                DataType::List(Arc::new(Field::new("item", item_type, nullable))),
                false,
            ))
        }
        _ => Ok((DataType::Utf8, true)),
    }
}

// ── Value conversion: Avro → Arrow ────────────────────────────────────────────

/// Convert a slice of top-level Avro record values to an Arrow [`RecordBatch`].
pub fn avro_values_to_batch(
    arrow_schema: &Arc<Schema>,
    records: &[AvroValue],
) -> ConnectorResult<RecordBatch> {
    if records.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema.clone()));
    }

    let n_cols = arrow_schema.fields().len();
    let mut columns: Vec<Vec<&AvroValue>> = vec![Vec::with_capacity(records.len()); n_cols];

    for record in records {
        let AvroValue::Record(fields) = record else {
            return Err(ConnectorError::Io(std::io::Error::other(
                "avro: expected Record at top level",
            )));
        };
        for (i, (_name, val)) in fields.iter().enumerate() {
            if i < n_cols {
                columns[i].push(val);
            }
        }
    }

    let arrays: ConnectorResult<Vec<ArrayRef>> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| build_array(field.data_type(), &columns[i]))
        .collect();

    RecordBatch::try_new(arrow_schema.clone(), arrays?)
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))
}

fn build_array(dt: &DataType, values: &[&AvroValue]) -> ConnectorResult<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Boolean(x) => b.append_value(*x),
                    AvroValue::Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Int(x) => b.append_value(*x),
                    AvroValue::Long(x) => b.append_value(*x as i32),
                    AvroValue::Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Long(x) => b.append_value(*x),
                    AvroValue::Int(x) => b.append_value(*x as i64),
                    AvroValue::Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Float(x) => b.append_value(*x),
                    AvroValue::Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Double(x) => b.append_value(*x),
                    AvroValue::Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for v in values {
                match unwrap_union(v) {
                    AvroValue::String(s) => b.append_value(s),
                    AvroValue::Enum(_, s) => b.append_value(s),
                    AvroValue::Null => b.append_null(),
                    other => b.append_value(format!("{other:?}")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Null => Ok(Arc::new(NullArray::new(values.len()))),
        _ => {
            let mut b = StringBuilder::new();
            for v in values {
                b.append_value(format!("{v:?}"));
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

/// Peel one layer of `Union` wrapping, returning the inner value.
fn unwrap_union(v: &AvroValue) -> &AvroValue {
    match v {
        AvroValue::Union(_, inner) => inner.as_ref(),
        other => other,
    }
}

// ── Schema conversion: Arrow → Avro ───────────────────────────────────────────

/// Convert an Arrow [`Schema`] to an Avro record schema.
///
/// Constructs a JSON schema string and parses it with the official Avro parser.
pub fn arrow_schema_to_avro(schema: &Schema) -> ConnectorResult<AvroSchema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let avro_type = arrow_type_to_avro_json(f.data_type(), f.is_nullable());
        fields.push(serde_json::json!({
            "name": f.name(),
            "type": avro_type,
        }));
    }

    let json_schema = serde_json::json!({
        "type": "record",
        "name": "batch",
        "fields": fields,
    });

    AvroSchema::parse_str(&json_schema.to_string())
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))
}

fn arrow_type_to_avro_json(dt: &DataType, nullable: bool) -> serde_json::Value {
    let base = match dt {
        DataType::Null => serde_json::json!("null"),
        DataType::Boolean => serde_json::json!("boolean"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8
        | DataType::UInt16 => serde_json::json!("int"),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => serde_json::json!("long"),
        DataType::Float32 => serde_json::json!("float"),
        DataType::Float64 => serde_json::json!("double"),
        DataType::Utf8 | DataType::LargeUtf8 => serde_json::json!("string"),
        DataType::Binary | DataType::LargeBinary => serde_json::json!("bytes"),
        _ => serde_json::json!("string"),
    };

    if nullable {
        serde_json::json!(["null", base])
    } else {
        base
    }
}

// ── Value conversion: Arrow → Avro ────────────────────────────────────────────

/// Convert an Arrow [`RecordBatch`] to a `Vec` of `AvroValue::Record` values.
pub fn batch_to_avro_values(batch: &RecordBatch) -> ConnectorResult<Vec<AvroValue>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut fields: Vec<(String, AvroValue)> = Vec::with_capacity(batch.num_columns());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let val = arrow_scalar_to_avro(col.as_ref(), row, field.is_nullable());
            fields.push((field.name().clone(), val));
        }
        rows.push(AvroValue::Record(fields));
    }
    Ok(rows)
}

fn arrow_scalar_to_avro(col: &dyn arrow::array::Array, row: usize, nullable: bool) -> AvroValue {
    use arrow::array::*;

    let is_null = col.is_null(row);

    let val = if is_null {
        AvroValue::Null
    } else {
        match col.data_type() {
            DataType::Null => AvroValue::Null,
            DataType::Boolean => col.as_any().downcast_ref::<BooleanArray>()
                .map(|arr| AvroValue::Boolean(arr.value(row)))
                .unwrap_or(AvroValue::Null),
            DataType::Int8 => col.as_any().downcast_ref::<Int8Array>()
                .map(|arr| AvroValue::Int(arr.value(row) as i32))
                .unwrap_or(AvroValue::Null),
            DataType::Int16 => col.as_any().downcast_ref::<Int16Array>()
                .map(|arr| AvroValue::Int(arr.value(row) as i32))
                .unwrap_or(AvroValue::Null),
            DataType::Int32 => col.as_any().downcast_ref::<Int32Array>()
                .map(|arr| AvroValue::Int(arr.value(row)))
                .unwrap_or(AvroValue::Null),
            DataType::Int64 => col.as_any().downcast_ref::<Int64Array>()
                .map(|arr| AvroValue::Long(arr.value(row)))
                .unwrap_or(AvroValue::Null),
            DataType::UInt8 => col.as_any().downcast_ref::<UInt8Array>()
                .map(|arr| AvroValue::Int(arr.value(row) as i32))
                .unwrap_or(AvroValue::Null),
            DataType::UInt16 => col.as_any().downcast_ref::<UInt16Array>()
                .map(|arr| AvroValue::Int(arr.value(row) as i32))
                .unwrap_or(AvroValue::Null),
            DataType::UInt32 => col.as_any().downcast_ref::<UInt32Array>()
                .map(|arr| AvroValue::Long(arr.value(row) as i64))
                .unwrap_or(AvroValue::Null),
            DataType::UInt64 => col.as_any().downcast_ref::<UInt64Array>()
                .map(|arr| AvroValue::Long(arr.value(row) as i64))
                .unwrap_or(AvroValue::Null),
            DataType::Float32 => col.as_any().downcast_ref::<Float32Array>()
                .map(|arr| AvroValue::Float(arr.value(row)))
                .unwrap_or(AvroValue::Null),
            DataType::Float64 => col.as_any().downcast_ref::<Float64Array>()
                .map(|arr| AvroValue::Double(arr.value(row)))
                .unwrap_or(AvroValue::Null),
            DataType::Utf8 => col.as_any().downcast_ref::<StringArray>()
                .map(|arr| AvroValue::String(arr.value(row).to_owned()))
                .unwrap_or(AvroValue::Null),
            DataType::LargeUtf8 => col.as_any().downcast_ref::<LargeStringArray>()
                .map(|arr| AvroValue::String(arr.value(row).to_owned()))
                .unwrap_or(AvroValue::Null),
            DataType::Binary => col.as_any().downcast_ref::<BinaryArray>()
                .map(|arr| AvroValue::Bytes(arr.value(row).to_vec()))
                .unwrap_or(AvroValue::Null),
            DataType::LargeBinary => col.as_any().downcast_ref::<LargeBinaryArray>()
                .map(|arr| AvroValue::Bytes(arr.value(row).to_vec()))
                .unwrap_or(AvroValue::Null),
            _ => AvroValue::String(format!("{:?}", col.data_type())),
        }
    };

    if nullable {
        // Union index 0 = null branch, 1 = value (matches ["null", T]).
        let idx = if is_null { 0u32 } else { 1u32 };
        AvroValue::Union(idx, Box::new(val))
    } else {
        val
    }
}

// ── AvroSource ────────────────────────────────────────────────────────────────

/// Reads an Avro container file as Arrow [`RecordBatch`] values.
///
/// All records are buffered at construction time. Rows are served in chunks of
/// `batch_size` via [`read_batch`][AvroSource::read_batch].
pub struct AvroSource {
    arrow_schema: Arc<Schema>,
    records: Vec<AvroValue>,
    cursor: usize,
    batch_size: usize,
}

impl AvroSource {
    /// Open an Avro container from `reader` and buffer all records eagerly.
    pub fn open<R: Read>(reader: R, batch_size: usize) -> ConnectorResult<Self> {
        let avro_reader = AvroReader::new(reader)
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        let writer_schema = avro_reader.writer_schema().clone();
        let arrow_schema = Arc::new(avro_schema_to_arrow(&writer_schema)?);

        let records: ConnectorResult<Vec<AvroValue>> = avro_reader
            .map(|r| r.map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string()))))
            .collect();

        Ok(Self {
            arrow_schema,
            records: records?,
            cursor: 0,
            batch_size: batch_size.max(1),
        })
    }

    /// Arrow schema derived from the Avro writer schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    /// Total number of buffered records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` when the source contains no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Read the next batch of up to `batch_size` rows.
    ///
    /// Returns `Ok(None)` once all records are consumed.
    pub fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.cursor >= self.records.len() {
            return Ok(None);
        }
        let end = (self.cursor + self.batch_size).min(self.records.len());
        let batch = avro_values_to_batch(&self.arrow_schema, &self.records[self.cursor..end])?;
        self.cursor = end;
        Ok(Some(batch))
    }

    /// Reset the read cursor to the beginning.
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    /// Connector capabilities: bounded and rewindable.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::default().with_bounded().with_rewindable()
    }
}

// ── AvroSink ──────────────────────────────────────────────────────────────────

/// Writes Arrow [`RecordBatch`] values to an Avro container file.
///
/// Records are buffered until [`flush`][AvroSink::flush] is called, which
/// serializes everything to the underlying writer as a single Avro container.
pub struct AvroSink<W: Write> {
    writer: W,
    avro_schema: AvroSchema,
    buffered: Vec<AvroValue>,
}

impl<W: Write> AvroSink<W> {
    /// Create a new sink.  The Avro schema is derived from `arrow_schema`.
    pub fn new(writer: W, arrow_schema: &Schema) -> ConnectorResult<Self> {
        let avro_schema = arrow_schema_to_avro(arrow_schema)?;
        Ok(Self { writer, avro_schema, buffered: Vec::new() })
    }

    /// Buffer a batch for later writing.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> ConnectorResult<()> {
        let values = batch_to_avro_values(batch)?;
        self.buffered.extend(values);
        Ok(())
    }

    /// Flush all buffered records to the underlying writer.
    ///
    /// Consumes `self` and returns the inner writer.
    pub fn flush(self) -> ConnectorResult<W> {
        let AvroSink { mut writer, avro_schema, buffered } = self;
        {
            let mut avro_writer = AvroWriter::new(&avro_schema, &mut writer);
            for value in buffered {
                avro_writer.append(value).map_err(|e| {
                    ConnectorError::Io(std::io::Error::other(e.to_string()))
                })?;
            }
            avro_writer.flush().map_err(|e| {
                ConnectorError::Io(std::io::Error::other(e.to_string()))
            })?;
        } // avro_writer dropped → borrow of writer released
        Ok(writer)
    }

    /// Number of buffered rows not yet written.
    pub fn buffered_rows(&self) -> usize {
        self.buffered.len()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const SAMPLE_SCHEMA: &str = r#"{
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "id",     "type": "int"},
            {"name": "name",   "type": "string"},
            {"name": "score",  "type": "double"},
            {"name": "active", "type": "boolean"}
        ]
    }"#;

    fn make_avro_bytes(schema_json: &str, rows: &[Vec<(&str, AvroValue)>]) -> Vec<u8> {
        use apache_avro::types::Record;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let mut writer = AvroWriter::new(&schema, Vec::new());
        for row in rows {
            let mut record = Record::new(&schema).expect("schema must be a record");
            for (field, value) in row {
                record.put(field, value.clone());
            }
            writer.append(record).unwrap();
        }
        writer.into_inner().unwrap()
    }

    fn sample_records() -> Vec<Vec<(&'static str, AvroValue)>> {
        vec![
            vec![
                ("id", AvroValue::Int(1)),
                ("name", AvroValue::String("alice".to_owned())),
                ("score", AvroValue::Double(9.5)),
                ("active", AvroValue::Boolean(true)),
            ],
            vec![
                ("id", AvroValue::Int(2)),
                ("name", AvroValue::String("bob".to_owned())),
                ("score", AvroValue::Double(7.0)),
                ("active", AvroValue::Boolean(false)),
            ],
        ]
    }

    #[test]
    fn avro_schema_converts_to_arrow() {
        let s = AvroSchema::parse_str(SAMPLE_SCHEMA).unwrap();
        let arrow = avro_schema_to_arrow(&s).unwrap();
        assert_eq!(arrow.fields().len(), 4);
        assert_eq!(arrow.field_with_name("id").unwrap().data_type(), &DataType::Int32);
        assert_eq!(arrow.field_with_name("name").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(arrow.field_with_name("score").unwrap().data_type(), &DataType::Float64);
        assert_eq!(arrow.field_with_name("active").unwrap().data_type(), &DataType::Boolean);
    }

    #[test]
    fn avro_source_reads_records() {
        let bytes = make_avro_bytes(SAMPLE_SCHEMA, &sample_records());
        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        assert_eq!(src.len(), 2);
        let batch = src.read_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn avro_source_exhausts_to_none() {
        let bytes = make_avro_bytes(SAMPLE_SCHEMA, &sample_records());
        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        src.read_batch().unwrap().unwrap();
        assert!(src.read_batch().unwrap().is_none());
    }

    #[test]
    fn avro_source_respects_batch_size() {
        let recs: Vec<Vec<(&str, AvroValue)>> = (0..10)
            .map(|i| vec![
                ("id", AvroValue::Int(i)),
                ("name", AvroValue::String(format!("u{i}"))),
                ("score", AvroValue::Double(0.0)),
                ("active", AvroValue::Boolean(true)),
            ])
            .collect();
        let bytes = make_avro_bytes(SAMPLE_SCHEMA, &recs);
        let mut src = AvroSource::open(Cursor::new(bytes), 3).unwrap();

        let b1 = src.read_batch().unwrap().unwrap();
        assert_eq!(b1.num_rows(), 3);
        let b2 = src.read_batch().unwrap().unwrap();
        assert_eq!(b2.num_rows(), 3);
    }

    #[test]
    fn avro_source_reset_replays() {
        let bytes = make_avro_bytes(SAMPLE_SCHEMA, &sample_records());
        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        src.read_batch().unwrap().unwrap();
        assert!(src.read_batch().unwrap().is_none());
        src.reset();
        let replayed = src.read_batch().unwrap().unwrap();
        assert_eq!(replayed.num_rows(), 2);
    }

    #[test]
    fn empty_file_returns_none_immediately() {
        let bytes = make_avro_bytes(SAMPLE_SCHEMA, &[]);
        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        assert!(src.is_empty());
        assert!(src.read_batch().unwrap().is_none());
    }

    #[test]
    fn capabilities_bounded_and_rewindable() {
        let bytes = make_avro_bytes(SAMPLE_SCHEMA, &[]);
        let src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        let caps = src.capabilities();
        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
    }

    #[test]
    fn arrow_schema_to_avro_produces_record() {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("val", DataType::Float64, true),
        ]);
        let avro = arrow_schema_to_avro(&schema).unwrap();
        assert!(matches!(avro, AvroSchema::Record(_)));
    }

    #[test]
    fn sink_roundtrip_with_source() {
        use arrow::array::{Float64Array, Int32Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut sink = AvroSink::new(Vec::<u8>::new(), &schema).unwrap();
        sink.write_batch(&batch).unwrap();
        assert_eq!(sink.buffered_rows(), 3);
        let out_bytes = sink.flush().unwrap();

        let mut src = AvroSource::open(Cursor::new(out_bytes), 100).unwrap();
        assert_eq!(src.len(), 3);
        let read_batch = src.read_batch().unwrap().unwrap();
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 3);
    }

    #[test]
    fn nullable_union_reads_correctly() {
        const NULLABLE_SCHEMA: &str = r#"{
            "type": "record",
            "name": "NullableTest",
            "fields": [
                {"name": "id",    "type": "int"},
                {"name": "label", "type": ["null", "string"]}
            ]
        }"#;

        let avro_schema = AvroSchema::parse_str(NULLABLE_SCHEMA).unwrap();
        let arrow_schema = avro_schema_to_arrow(&avro_schema).unwrap();
        assert!(arrow_schema.field_with_name("label").unwrap().is_nullable());

        let records = vec![
            AvroValue::Record(vec![
                ("id".to_owned(), AvroValue::Int(1)),
                (
                    "label".to_owned(),
                    AvroValue::Union(1, Box::new(AvroValue::String("hi".to_owned()))),
                ),
            ]),
            AvroValue::Record(vec![
                ("id".to_owned(), AvroValue::Int(2)),
                ("label".to_owned(), AvroValue::Union(0, Box::new(AvroValue::Null))),
            ]),
        ];

        let arc = Arc::new(arrow_schema);
        let batch = avro_values_to_batch(&arc, &records).unwrap();
        assert_eq!(batch.num_rows(), 2);

        use arrow::array::{Array, StringArray};
        let labels = batch
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.value(0), "hi");
        assert!(labels.is_null(1));
    }
}
