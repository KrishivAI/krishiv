//! Avro file source/sink with bidirectional Arrow-Avro schema and value conversion.
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
//! | `string` / `enum` / `uuid` | `Utf8` |
//! | `date`             | `Date32`      |
//! | `time-millis` / `time-micros` | `Time32(ms)` / `Time64(us)` |
//! | `timestamp-millis` / `timestamp-micros` (incl. local) | `Timestamp(ms/us)` |
//! | `record` / `array` | `Struct` / `List` |
//! | `union [null, T]`  | nullable T    |
//!
//! Anything outside this table (maps, decimals, durations, unions with more
//! than one non-null variant, nanosecond timestamps) is rejected with an error
//! in both directions — never silently coerced.
//!
//! [Apache Avro]: https://avro.apache.org/

use std::io::{Read, Write};
use std::sync::Arc;

use apache_avro::{
    Reader as AvroReader, Schema as AvroSchema, Writer as AvroWriter, types::Value as AvroValue,
};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, ListArray, NullArray, StringBuilder, StructArray,
    Time32MillisecondBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, builder::BooleanBufferBuilder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
        AvroSchema::String | AvroSchema::Enum(_) | AvroSchema::Uuid(_) => {
            Ok((DataType::Utf8, false))
        }
        AvroSchema::Date => Ok((DataType::Date32, false)),
        AvroSchema::TimeMillis => Ok((DataType::Time32(TimeUnit::Millisecond), false)),
        AvroSchema::TimeMicros => Ok((DataType::Time64(TimeUnit::Microsecond), false)),
        AvroSchema::TimestampMillis | AvroSchema::LocalTimestampMillis => {
            Ok((DataType::Timestamp(TimeUnit::Millisecond, None), false))
        }
        AvroSchema::TimestampMicros | AvroSchema::LocalTimestampMicros => {
            Ok((DataType::Timestamp(TimeUnit::Microsecond, None), false))
        }
        AvroSchema::Union(u) => {
            let variants = u.variants();
            let non_null: Vec<_> = variants
                .iter()
                .filter(|s| !matches!(s, AvroSchema::Null))
                .collect();
            if let (Some(inner), 2) = (non_null.first(), variants.len())
                && non_null.len() == 1
            {
                let (dt, _) = avro_schema_to_arrow_type(inner)?;
                Ok((dt, true))
            } else {
                Err(ConnectorError::Io(std::io::Error::other(format!(
                    "avro: unions with more than one non-null variant are not supported \
                     (got {} variants)",
                    variants.len()
                ))))
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
        other => Err(ConnectorError::Io(std::io::Error::other(format!(
            "avro: unsupported avro schema type {other:?}"
        )))),
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
            if let Some(col) = columns.get_mut(i) {
                col.push(val);
            }
        }
    }

    let arrays: ConnectorResult<Vec<ArrayRef>> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            build_array(
                field.data_type(),
                columns.get(i).map_or(&[][..], Vec::as_slice),
            )
        })
        .collect();

    RecordBatch::try_new(arrow_schema.clone(), arrays?)
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))
}

/// A decoded Avro value whose variant does not match the Arrow column type.
///
/// A mismatch is always an error: silently substituting NULL (or a debug
/// string) for a value the producer wrote is data loss the reader cannot see.
/// The only cross-variant conversions accepted are Avro's own schema-resolution
/// promotions (`int → long → float → double`), which are lossless by spec
/// definition (int/long → float/double follow the Avro spec even where the
/// float mantissa rounds).
fn type_mismatch(dt: &DataType, v: &AvroValue) -> ConnectorError {
    ConnectorError::Io(std::io::Error::other(format!(
        "avro: value {v:?} does not match arrow column type {dt:?}"
    )))
}

static AVRO_NULL: AvroValue = AvroValue::Null;

fn build_array(dt: &DataType, values: &[&AvroValue]) -> ConnectorResult<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Boolean(x) => b.append_value(*x),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Int(x) => b.append_value(*x),
                    AvroValue::Null => b.append_null(),
                    // long → int is a narrowing Avro does not sanction; a
                    // truncating `as i32` here silently wrapped values.
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Long(x) => b.append_value(*x),
                    AvroValue::Int(x) => b.append_value(i64::from(*x)),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Float(x) => b.append_value(*x),
                    AvroValue::Int(x) => b.append_value(*x as f32),
                    AvroValue::Long(x) => b.append_value(*x as f32),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Double(x) => b.append_value(*x),
                    AvroValue::Float(x) => b.append_value(f64::from(*x)),
                    AvroValue::Int(x) => b.append_value(f64::from(*x)),
                    AvroValue::Long(x) => b.append_value(*x as f64),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
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
                    AvroValue::Uuid(u) => b.append_value(u.to_string()),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Bytes(bytes) | AvroValue::Fixed(_, bytes) => b.append_value(bytes),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Date(d) => b.append_value(*d),
                    AvroValue::Int(d) => b.append_value(*d),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let mut b = Time32MillisecondBuilder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::TimeMillis(t) => b.append_value(*t),
                    AvroValue::Int(t) => b.append_value(*t),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::TimeMicros(t) => b.append_value(*t),
                    AvroValue::Long(t) => b.append_value(*t),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut b =
                TimestampMillisecondBuilder::with_capacity(values.len()).with_data_type(dt.clone());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::TimestampMillis(t) | AvroValue::LocalTimestampMillis(t) => {
                        b.append_value(*t)
                    }
                    AvroValue::Long(t) => b.append_value(*t),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut b =
                TimestampMicrosecondBuilder::with_capacity(values.len()).with_data_type(dt.clone());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::TimestampMicros(t) | AvroValue::LocalTimestampMicros(t) => {
                        b.append_value(*t)
                    }
                    AvroValue::Long(t) => b.append_value(*t),
                    AvroValue::Null => b.append_null(),
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Struct(fields) => {
            let mut child_cols: Vec<Vec<&AvroValue>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            let mut validity = BooleanBufferBuilder::new(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Record(rec_fields) => {
                        if rec_fields.len() != fields.len() {
                            return Err(ConnectorError::Io(std::io::Error::other(format!(
                                "avro: record has {} fields, arrow struct expects {}",
                                rec_fields.len(),
                                fields.len()
                            ))));
                        }
                        for (i, (_name, fv)) in rec_fields.iter().enumerate() {
                            if let Some(col) = child_cols.get_mut(i) {
                                col.push(fv);
                            }
                        }
                        validity.append(true);
                    }
                    AvroValue::Null => {
                        for col in &mut child_cols {
                            col.push(&AVRO_NULL);
                        }
                        validity.append(false);
                    }
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            let arrays: ConnectorResult<Vec<ArrayRef>> = fields
                .iter()
                .zip(child_cols.iter())
                .map(|(f, col)| build_array(f.data_type(), col))
                .collect();
            Ok(Arc::new(StructArray::new(
                fields.clone(),
                arrays?,
                Some(NullBuffer::new(validity.finish())),
            )))
        }
        DataType::List(item_field) => {
            let mut flat: Vec<&AvroValue> = Vec::new();
            let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
            offsets.push(0);
            let mut validity = BooleanBufferBuilder::new(values.len());
            for v in values {
                match unwrap_union(v) {
                    AvroValue::Array(items) => {
                        flat.extend(items.iter());
                        let end = i32::try_from(flat.len()).map_err(|_| {
                            ConnectorError::Io(std::io::Error::other(
                                "avro: list offsets overflow i32",
                            ))
                        })?;
                        offsets.push(end);
                        validity.append(true);
                    }
                    AvroValue::Null => {
                        let last = offsets.last().copied().unwrap_or(0);
                        offsets.push(last);
                        validity.append(false);
                    }
                    other => return Err(type_mismatch(dt, other)),
                }
            }
            let child = build_array(item_field.data_type(), &flat)?;
            Ok(Arc::new(ListArray::new(
                item_field.clone(),
                OffsetBuffer::new(offsets.into()),
                child,
                Some(NullBuffer::new(validity.finish())),
            )))
        }
        DataType::Null => Ok(Arc::new(NullArray::new(values.len()))),
        other => Err(ConnectorError::Io(std::io::Error::other(format!(
            "avro: unsupported arrow column type {other:?}"
        )))),
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
        let avro_type = arrow_type_to_avro_json(f.data_type(), f.is_nullable(), f.name())?;
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

/// `name` seeds the generated names of nested Avro records (which must be
/// named), keeping them unique within one schema as long as field names are.
fn arrow_type_to_avro_json(
    dt: &DataType,
    nullable: bool,
    name: &str,
) -> ConnectorResult<serde_json::Value> {
    let base = match dt {
        DataType::Null => serde_json::json!("null"),
        DataType::Boolean => serde_json::json!("boolean"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            serde_json::json!("int")
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => serde_json::json!("long"),
        DataType::Float32 => serde_json::json!("float"),
        DataType::Float64 => serde_json::json!("double"),
        DataType::Utf8 | DataType::LargeUtf8 => serde_json::json!("string"),
        DataType::Binary | DataType::LargeBinary => serde_json::json!("bytes"),
        DataType::Date32 => serde_json::json!({"type": "int", "logicalType": "date"}),
        DataType::Time32(TimeUnit::Millisecond) => {
            serde_json::json!({"type": "int", "logicalType": "time-millis"})
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            serde_json::json!({"type": "long", "logicalType": "time-micros"})
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            serde_json::json!({"type": "long", "logicalType": "timestamp-millis"})
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            serde_json::json!({"type": "long", "logicalType": "timestamp-micros"})
        }
        DataType::List(item) => {
            let items = arrow_type_to_avro_json(
                item.data_type(),
                item.is_nullable(),
                &format!("{name}_item"),
            )?;
            serde_json::json!({"type": "array", "items": items})
        }
        DataType::Struct(fields) => {
            let mut rec_fields = Vec::with_capacity(fields.len());
            for f in fields {
                let ft = arrow_type_to_avro_json(
                    f.data_type(),
                    f.is_nullable(),
                    &format!("{name}_{}", f.name()),
                )?;
                rec_fields.push(serde_json::json!({"name": f.name(), "type": ft}));
            }
            serde_json::json!({"type": "record", "name": format!("{name}_rec"), "fields": rec_fields})
        }
        other => {
            return Err(ConnectorError::Io(std::io::Error::other(format!(
                "avro: arrow type {other:?} (column '{name}') has no avro mapping"
            ))));
        }
    };

    Ok(if nullable {
        serde_json::json!(["null", base])
    } else {
        base
    })
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
            let val = arrow_scalar_to_avro(col.as_ref(), row, field.is_nullable())?;
            fields.push((field.name().clone(), val));
        }
        rows.push(AvroValue::Record(fields));
    }
    Ok(rows)
}

/// Downcast a column to its concrete array type; a mismatch is an internal
/// invariant violation (the `data_type` dispatch chose the type) and errors
/// rather than silently degrading to NULL.
fn dc<T: 'static>(col: &dyn arrow::array::Array) -> ConnectorResult<&T> {
    col.as_any().downcast_ref::<T>().ok_or_else(|| {
        ConnectorError::Io(std::io::Error::other(
            "avro: column downcast does not match its declared data type",
        ))
    })
}

fn arrow_scalar_to_avro(
    col: &dyn arrow::array::Array,
    row: usize,
    nullable: bool,
) -> ConnectorResult<AvroValue> {
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
        Time32MillisecondArray, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };

    let is_null = col.is_null(row);

    let val = if is_null {
        AvroValue::Null
    } else {
        match col.data_type() {
            DataType::Null => AvroValue::Null,
            DataType::Boolean => AvroValue::Boolean(dc::<BooleanArray>(col)?.value(row)),
            DataType::Int8 => AvroValue::Int(i32::from(dc::<Int8Array>(col)?.value(row))),
            DataType::Int16 => AvroValue::Int(i32::from(dc::<Int16Array>(col)?.value(row))),
            DataType::Int32 => AvroValue::Int(dc::<Int32Array>(col)?.value(row)),
            DataType::Int64 => AvroValue::Long(dc::<Int64Array>(col)?.value(row)),
            DataType::UInt8 => AvroValue::Int(i32::from(dc::<UInt8Array>(col)?.value(row))),
            DataType::UInt16 => AvroValue::Int(i32::from(dc::<UInt16Array>(col)?.value(row))),
            DataType::UInt32 => AvroValue::Long(i64::from(dc::<UInt32Array>(col)?.value(row))),
            DataType::UInt64 => {
                let v = dc::<UInt64Array>(col)?.value(row);
                let as_long = i64::try_from(v).map_err(|_| {
                    ConnectorError::Io(std::io::Error::other(format!(
                        "avro: uint64 value {v} exceeds the avro long range"
                    )))
                })?;
                AvroValue::Long(as_long)
            }
            DataType::Float32 => AvroValue::Float(dc::<Float32Array>(col)?.value(row)),
            DataType::Float64 => AvroValue::Double(dc::<Float64Array>(col)?.value(row)),
            DataType::Utf8 => AvroValue::String(dc::<StringArray>(col)?.value(row).to_owned()),
            DataType::LargeUtf8 => {
                AvroValue::String(dc::<LargeStringArray>(col)?.value(row).to_owned())
            }
            DataType::Binary => AvroValue::Bytes(dc::<BinaryArray>(col)?.value(row).to_vec()),
            DataType::LargeBinary => {
                AvroValue::Bytes(dc::<LargeBinaryArray>(col)?.value(row).to_vec())
            }
            DataType::Date32 => AvroValue::Date(dc::<Date32Array>(col)?.value(row)),
            DataType::Time32(TimeUnit::Millisecond) => {
                AvroValue::TimeMillis(dc::<Time32MillisecondArray>(col)?.value(row))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                AvroValue::TimeMicros(dc::<Time64MicrosecondArray>(col)?.value(row))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                AvroValue::TimestampMillis(dc::<TimestampMillisecondArray>(col)?.value(row))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                AvroValue::TimestampMicros(dc::<TimestampMicrosecondArray>(col)?.value(row))
            }
            DataType::Struct(fields) => {
                let arr = dc::<StructArray>(col)?;
                let mut rec = Vec::with_capacity(fields.len());
                for (i, f) in fields.iter().enumerate() {
                    let child = arrow_scalar_to_avro(arr.column(i).as_ref(), row, f.is_nullable())?;
                    rec.push((f.name().clone(), child));
                }
                AvroValue::Record(rec)
            }
            DataType::List(item_field) => {
                let arr = dc::<ListArray>(col)?;
                let items = arr.value(row);
                let mut out = Vec::with_capacity(items.len());
                for j in 0..items.len() {
                    out.push(arrow_scalar_to_avro(
                        items.as_ref(),
                        j,
                        item_field.is_nullable(),
                    )?);
                }
                AvroValue::Array(out)
            }
            other => {
                return Err(ConnectorError::Io(std::io::Error::other(format!(
                    "avro: arrow type {other:?} has no avro value mapping"
                ))));
            }
        }
    };

    Ok(if nullable {
        // Union index 0 = null branch, 1 = value (matches ["null", T]).
        let idx = if is_null { 0u32 } else { 1u32 };
        AvroValue::Union(idx, Box::new(val))
    } else {
        val
    })
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
        let window = self.records.get(self.cursor..end).unwrap_or_default();
        let batch = avro_values_to_batch(&self.arrow_schema, window)?;
        self.cursor = end;
        Ok(Some(batch))
    }

    /// Reset the read cursor to the beginning.
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    /// Connector capabilities: bounded and rewindable.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::default()
            .with_bounded()
            .with_rewindable()
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
        Ok(Self {
            writer,
            avro_schema,
            buffered: Vec::new(),
        })
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
        let AvroSink {
            mut writer,
            avro_schema,
            buffered,
        } = self;
        {
            // apache-avro 0.22 made `Writer::new` fallible (it resolves the
            // schema up front instead of at first append), so the error surfaces
            // here rather than on the first row.
            let mut avro_writer = AvroWriter::new(&avro_schema, &mut writer)
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            for value in buffered {
                avro_writer
                    .append_value(value)
                    .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            }
            avro_writer
                .flush()
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
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
        let mut writer = AvroWriter::new(&schema, Vec::new()).unwrap();
        for row in rows {
            let mut record = Record::new(&schema).expect("schema must be a record");
            for (field, value) in row {
                record.put(field, value.clone());
            }
            writer.append_value(record).unwrap();
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
        assert_eq!(
            arrow.field_with_name("id").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            arrow.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            arrow.field_with_name("score").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            arrow.field_with_name("active").unwrap().data_type(),
            &DataType::Boolean
        );
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
            .map(|i| {
                vec![
                    ("id", AvroValue::Int(i)),
                    ("name", AvroValue::String(format!("u{i}"))),
                    ("score", AvroValue::Double(0.0)),
                    ("active", AvroValue::Boolean(true)),
                ]
            })
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
    fn decode_type_mismatch_is_an_error_not_a_silent_null() {
        // A Float64 column receiving a Boolean must error; the pre-fix code
        // silently appended NULL, losing the producer's value.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            true,
        )]));
        let records = vec![AvroValue::Record(vec![(
            "score".to_owned(),
            AvroValue::Boolean(true),
        )])];
        let err = avro_values_to_batch(&schema, &records).unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
    }

    #[test]
    fn long_into_int32_column_errors_instead_of_truncating() {
        // Pre-fix: `Long(2^32 + 1)` became `1` via `as i32`.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let records = vec![AvroValue::Record(vec![(
            "id".to_owned(),
            AvroValue::Long(4_294_967_297),
        )])];
        let err = avro_values_to_batch(&schema, &records).unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
    }

    #[test]
    fn unsupported_sink_type_errors_instead_of_writing_type_names() {
        // Pre-fix: every row of an unmapped column was written as the literal
        // data-type debug string (e.g. "Duration(..)").
        let schema = Schema::new(vec![Field::new(
            "d",
            DataType::Duration(arrow::datatypes::TimeUnit::Millisecond),
            false,
        )]);
        let err = arrow_schema_to_avro(&schema).unwrap_err();
        assert!(err.to_string().contains("no avro mapping"), "{err}");
    }

    #[test]
    fn multi_variant_union_is_rejected() {
        // Pre-fix: ["null","int","string"] mapped to Utf8 and int values were
        // stored as the debug string "Int(5)".
        const MULTI_UNION: &str = r#"{
            "type": "record",
            "name": "U",
            "fields": [{"name": "v", "type": ["null", "int", "string"]}]
        }"#;
        let s = AvroSchema::parse_str(MULTI_UNION).unwrap();
        let err = avro_schema_to_arrow(&s).unwrap_err();
        assert!(err.to_string().contains("non-null variant"), "{err}");
    }

    #[test]
    fn uint64_beyond_long_range_errors_instead_of_wrapping() {
        use arrow::array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef],
        )
        .unwrap();
        let err = batch_to_avro_values(&batch).unwrap_err();
        assert!(
            err.to_string().contains("exceeds the avro long range"),
            "{err}"
        );
    }

    #[test]
    fn binary_round_trips_through_sink_and_source() {
        use arrow::array::BinaryArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![&b"\x00\xffhi"[..], &b""[..]])) as ArrayRef],
        )
        .unwrap();
        let mut sink = AvroSink::new(Vec::<u8>::new(), &schema).unwrap();
        sink.write_batch(&batch).unwrap();
        let bytes = sink.flush().unwrap();

        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        let back = src.read_batch().unwrap().unwrap();
        let col = back
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(col.value(0), b"\x00\xffhi");
        assert_eq!(col.value(1), b"");
    }

    #[test]
    fn timestamp_and_date_round_trip() {
        use arrow::array::{Date32Array, TimestampMicrosecondArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("day", DataType::Date32, false),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Date32Array::from(vec![19_000])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_700_000_000_000_000_i64,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut sink = AvroSink::new(Vec::<u8>::new(), &schema).unwrap();
        sink.write_batch(&batch).unwrap();
        let bytes = sink.flush().unwrap();

        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        assert_eq!(src.schema().field(0).data_type(), &DataType::Date32);
        assert_eq!(
            src.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let back = src.read_batch().unwrap().unwrap();
        use arrow::array::{Date32Array as D, TimestampMicrosecondArray as T};
        assert_eq!(
            back.column(0)
                .as_any()
                .downcast_ref::<D>()
                .unwrap()
                .value(0),
            19_000
        );
        assert_eq!(
            back.column(1)
                .as_any()
                .downcast_ref::<T>()
                .unwrap()
                .value(0),
            1_700_000_000_000_000_i64
        );
    }

    #[test]
    fn struct_and_list_round_trip() {
        use arrow::array::{Array, Int64Builder, ListBuilder, StringBuilder, StructBuilder};
        use arrow::datatypes::Fields;

        let struct_fields = Fields::from(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("zip", DataType::Int64, true),
        ]);
        let list_field = Arc::new(Field::new("item", DataType::Int64, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("addr", DataType::Struct(struct_fields.clone()), true),
            Field::new("nums", DataType::List(list_field), true),
        ]));

        let mut sb = StructBuilder::new(
            struct_fields,
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int64Builder::new()),
            ],
        );
        sb.field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("pune");
        sb.field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(411001);
        sb.append(true);
        sb.field_builder::<StringBuilder>(0).unwrap().append_null();
        sb.field_builder::<Int64Builder>(1).unwrap().append_null();
        sb.append(false);

        let mut lb = ListBuilder::new(Int64Builder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::Int64,
            false,
        )));
        lb.values().append_value(1);
        lb.values().append_value(2);
        lb.append(true);
        lb.append(false);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(sb.finish()) as ArrayRef,
                Arc::new(lb.finish()) as ArrayRef,
            ],
        )
        .unwrap();

        let mut sink = AvroSink::new(Vec::<u8>::new(), &schema).unwrap();
        sink.write_batch(&batch).unwrap();
        let bytes = sink.flush().unwrap();

        let mut src = AvroSource::open(Cursor::new(bytes), 100).unwrap();
        let back = src.read_batch().unwrap().unwrap();
        assert_eq!(back.num_rows(), 2);

        let addr = back
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(addr.is_valid(0) && addr.is_null(1));
        let city = addr
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(city.value(0), "pune");
        let zip = addr
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(zip.value(0), 411001);

        let nums = back.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(nums.is_valid(0) && nums.is_null(1));
        let first = nums.value(0);
        let first = first
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!((first.value(0), first.value(1)), (1, 2));
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
                (
                    "label".to_owned(),
                    AvroValue::Union(0, Box::new(AvroValue::Null)),
                ),
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
