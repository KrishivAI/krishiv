use std::sync::Arc;

use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    Int64Array, NullArray, StringArray, Time32MillisecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use super::client::SchemaRegistryClient;
use super::{KafkaDeserializer, SchemaRegistryConfig, SchemaRegistryError, SchemaRegistryResult};

/// Avro deserializer (Confluent wire format + registry fetch).
#[derive(Clone)]
pub struct AvroDeserializer {
    pub(crate) client: SchemaRegistryClient,
}

impl AvroDeserializer {
    pub fn new(config: &SchemaRegistryConfig) -> SchemaRegistryResult<Self> {
        config.validate()?;
        Ok(Self {
            client: SchemaRegistryClient::new(&config.url)?,
        })
    }
}

#[async_trait]
impl KafkaDeserializer for AvroDeserializer {
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)> {
        if payload.len() < 5 || payload[0] != 0 {
            return Err(SchemaRegistryError::Decode(
                "expected Confluent magic byte 0".into(),
            ));
        }
        let schema_id_bytes: [u8; 4] = payload[1..5]
            .try_into()
            .map_err(|_| SchemaRegistryError::Decode("failed to read schema id bytes".into()))?;
        let schema_id = u32::from_be_bytes(schema_id_bytes);
        let avro_schema = self.client.fetch_avro_schema(schema_id).await?;

        decode_avro_datum_payload(&avro_schema, &payload[5..])
    }
}

pub(crate) fn decode_avro_datum_payload(
    avro_schema: &apache_avro::Schema,
    payload: &[u8],
) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)> {
    if payload.is_empty() {
        return Err(SchemaRegistryError::Decode("empty avro payload".into()));
    }
    let mut cursor = std::io::Cursor::new(payload);
    let record = from_avro_datum(avro_schema, &mut cursor, None)
        .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
    if cursor.position() != payload.len() as u64 {
        return Err(SchemaRegistryError::Decode(format!(
            "trailing bytes after avro datum: {}",
            payload.len() as u64 - cursor.position()
        )));
    }
    let arrow_schema = avro_schema_to_arrow_schema(avro_schema)?;
    let batches = avro_records_to_batches(&[record], &arrow_schema)?;
    Ok((arrow_schema, batches))
}

/// Convert an Avro schema to an Arrow schema.
pub(crate) fn avro_schema_to_arrow_schema(
    avro_schema: &apache_avro::Schema,
) -> SchemaRegistryResult<SchemaRef> {
    match avro_schema {
        apache_avro::Schema::Record(record_schema) => {
            let fields = record_schema
                .fields
                .iter()
                .map(|f| {
                    let (data_type, nullable) = avro_schema_to_data_type(&f.schema)?;
                    Ok(Field::new(&f.name, data_type, nullable))
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Schema::new(fields)))
        }
        other => {
            let (data_type, nullable) = avro_schema_to_data_type(other)?;
            Ok(Arc::new(Schema::new(vec![Field::new(
                "value", data_type, nullable,
            )])))
        }
    }
}

/// Map an Avro schema node to an Arrow DataType.
pub(crate) fn avro_schema_to_data_type(
    schema: &apache_avro::Schema,
) -> SchemaRegistryResult<(DataType, bool)> {
    use apache_avro::Schema;
    let data_type = match schema {
        Schema::Null => return Ok((DataType::Null, true)),
        Schema::Boolean => DataType::Boolean,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Bytes | Schema::Fixed(_) => DataType::Binary,
        Schema::String | Schema::Enum(_) | Schema::Uuid => DataType::Utf8,
        Schema::Date => DataType::Date32,
        Schema::TimeMillis => DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
        Schema::TimeMicros => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        Schema::TimestampMillis | Schema::LocalTimestampMillis => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        Schema::TimestampMicros | Schema::LocalTimestampMicros => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        Schema::TimestampNanos | Schema::LocalTimestampNanos => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        Schema::Union(union) => {
            let mut nullable = false;
            let mut data_variant = None;
            for variant in union.variants() {
                if matches!(variant, Schema::Null) {
                    nullable = true;
                    continue;
                }
                if data_variant.replace(variant).is_some() {
                    return Err(SchemaRegistryError::Decode(
                        "Avro unions with multiple non-null variants are not supported".into(),
                    ));
                }
            }
            let variant = data_variant.ok_or_else(|| {
                SchemaRegistryError::Decode(
                    "Avro union must contain one supported non-null variant".into(),
                )
            })?;
            let (data_type, inner_nullable) = avro_schema_to_data_type(variant)?;
            return Ok((data_type, nullable || inner_nullable));
        }
        unsupported => {
            return Err(SchemaRegistryError::Decode(format!(
                "unsupported Avro schema type: {unsupported:?}"
            )));
        }
    };
    Ok((data_type, false))
}

/// Convert a slice of Avro records into Arrow RecordBatches.
pub(crate) fn avro_records_to_batches(
    records: &[Value],
    arrow_schema: &Schema,
) -> SchemaRegistryResult<Vec<RecordBatch>> {
    let num_fields = arrow_schema.fields().len();
    if num_fields == 0 {
        return Err(SchemaRegistryError::Decode(
            "empty arrow schema from avro payload".into(),
        ));
    }

    let mut columns: Vec<Vec<&Value>> = (0..num_fields)
        .map(|_| Vec::with_capacity(records.len()))
        .collect();

    let name_to_idx: std::collections::HashMap<&str, usize> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    for record in records {
        match record {
            Value::Record(fields) => {
                let orig_len = columns[0].len();
                for (name, val) in fields {
                    if let Some(&col_idx) = name_to_idx.get(name.as_str()) {
                        columns[col_idx].push(val);
                    }
                }
                for col in columns.iter_mut() {
                    if col.len() == orig_len {
                        col.push(&Value::Null);
                    }
                }
            }
            other => {
                if let Some(&col_idx) = name_to_idx.get("value") {
                    columns[col_idx].push(other);
                } else if num_fields == 1 {
                    columns[0].push(other);
                }
            }
        }
    }

    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(i, vals)| avro_values_to_column(vals, arrow_schema.field(i).data_type()))
        .collect::<SchemaRegistryResult<_>>()?;

    let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), arrays)
        .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;

    Ok(vec![batch])
}

/// Unwrap a potential Union wrapper to get the inner value.
fn unwrap_value(value: &Value) -> &Value {
    match value {
        Value::Union(_, inner) => inner.as_ref(),
        other => other,
    }
}

/// Convert a slice of Avro values into a single Arrow column array.
pub(crate) fn avro_values_to_column(
    values: &[&Value],
    data_type: &DataType,
) -> SchemaRegistryResult<ArrayRef> {
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(values.len()))),
        DataType::Boolean => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Boolean(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
        DataType::Int32 => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Int(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Long(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Float32 => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Float(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Float32Array::from(values)))
        }
        DataType::Float64 => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Double(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        DataType::Binary => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Bytes(value) => Ok(Some(value.as_slice())),
                    Value::Fixed(_, value) => Ok(Some(value.as_slice())),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(BinaryArray::from(values)))
        }
        DataType::Utf8 => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::String(value) | Value::Enum(_, value) => Ok(Some(value.clone())),
                    Value::Uuid(value) => Ok(Some(value.to_string())),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::Date32 => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::Date(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Date32Array::from(values)))
        }
        DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::TimeMillis(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Time32MillisecondArray::from(values)))
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::TimeMicros(value) => Ok(Some(*value)),
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Time64MicrosecondArray::from(values)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::TimestampMillis(value) | Value::LocalTimestampMillis(value) => {
                        Ok(Some(*value))
                    }
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(TimestampMillisecondArray::from(values)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::TimestampMicros(value) | Value::LocalTimestampMicros(value) => {
                        Ok(Some(*value))
                    }
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(TimestampMicrosecondArray::from(values)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let values = values
                .iter()
                .map(|value| match unwrap_value(value) {
                    Value::Null => Ok(None),
                    Value::TimestampNanos(value) | Value::LocalTimestampNanos(value) => {
                        Ok(Some(*value))
                    }
                    other => Err(avro_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(TimestampNanosecondArray::from(values)))
        }
        unsupported => Err(SchemaRegistryError::Decode(format!(
            "unsupported Arrow type for Avro conversion: {unsupported}"
        ))),
    }
}

pub(crate) fn avro_value_mismatch(expected: &DataType, actual: &Value) -> SchemaRegistryError {
    SchemaRegistryError::Decode(format!(
        "Avro value {actual:?} does not match Arrow type {expected}"
    ))
}

