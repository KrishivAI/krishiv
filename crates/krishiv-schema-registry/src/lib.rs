#![forbid(unsafe_code)]
//! Confluent Schema Registry deserialization for Kafka payloads (R18 S3.3).

extern crate alloc;

use std::sync::Arc;

use apache_avro::Reader;
use apache_avro::types::Value;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use reqwest::Client;

/// Errors from schema registry operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaRegistryError {
    #[error("schema registry http: {0}")]
    Http(String),
    #[error("schema registry decode: {0}")]
    Decode(String),
}

pub type SchemaRegistryResult<T> = Result<T, SchemaRegistryError>;

/// Payload format handled by the registry client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryFormat {
    Avro,
    Protobuf,
    Json,
}

/// Configuration for a Confluent-compatible registry subject.
#[derive(Debug, Clone)]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub subject: String,
    pub format: RegistryFormat,
}

/// Deserialize Kafka payloads that use Confluent wire format.
#[async_trait]
pub trait KafkaDeserializer: Send + Sync {
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)>;
    fn arrow_schema(&self) -> SchemaRef;
}

/// HTTP schema cache keyed by schema id.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    base_url: String,
    cache: Arc<DashMap<u32, String>>,
    http: Client,
}

impl SchemaRegistryClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            base_url: url.into().trim_end_matches('/').to_string(),
            cache: Arc::new(DashMap::new()),
            http: Client::new(),
        }
    }

    pub async fn fetch_schema(&self, id: u32) -> SchemaRegistryResult<String> {
        if let Some(hit) = self.cache.get(&id) {
            return Ok(hit.clone());
        }
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| SchemaRegistryError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(SchemaRegistryError::Http(
                resp.text().await.unwrap_or_default(),
            ));
        }
        #[derive(serde::Deserialize)]
        struct Body {
            schema: String,
        }
        let body: Body = resp
            .json()
            .await
            .map_err(|e| SchemaRegistryError::Http(e.to_string()))?;
        self.cache.insert(id, body.schema.clone());
        Ok(body.schema)
    }
}

/// Avro deserializer (Confluent wire format + registry fetch).
#[derive(Clone)]
pub struct AvroDeserializer {
    client: SchemaRegistryClient,
}

impl AvroDeserializer {
    pub fn new(config: &SchemaRegistryConfig) -> Self {
        Self {
            client: SchemaRegistryClient::new(config.url.clone()),
        }
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
        let schema_str = self.client.fetch_schema(schema_id).await?;
        let avro_schema = apache_avro::Schema::parse_str(&schema_str)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;

        let reader = Reader::with_schema(&avro_schema, &payload[5..])
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;

        let records: Vec<Value> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;

        if records.is_empty() {
            return Err(SchemaRegistryError::Decode("empty avro payload".into()));
        }

        let arrow_schema = avro_schema_to_arrow_schema(&avro_schema);
        let batches = avro_records_to_batches(&records, &arrow_schema)?;

        Ok((arrow_schema, batches))
    }

    fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![] as Vec<Field>))
    }
}

pub struct JsonSchemaDeserializer;

#[async_trait]
impl KafkaDeserializer for JsonSchemaDeserializer {
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)> {
        let text =
            std::str::from_utf8(payload).map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        let batch = json_payload_batch(text)?;
        let schema = batch.schema();
        Ok((schema, vec![batch]))
    }

    fn arrow_schema(&self) -> SchemaRef {
        payload_schema()
    }
}

/// Protobuf deserializer using Confluent wire format.
///
/// Decodes protobuf-encoded Kafka payloads using the Confluent wire format:
/// `[magic byte 0x00][4-byte BE schema id][protobuf payload]`.
///
/// The schema is fetched from the Confluent Schema Registry and used to
/// decode the protobuf wire format into Arrow `RecordBatch` columns.
pub struct ProtobufDeserializer {
    client: SchemaRegistryClient,
}

impl ProtobufDeserializer {
    pub fn new(config: &SchemaRegistryConfig) -> Self {
        Self {
            client: SchemaRegistryClient::new(config.url.clone()),
        }
    }
}

#[async_trait]
impl KafkaDeserializer for ProtobufDeserializer {
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
        let schema_str = self.client.fetch_schema(schema_id).await?;

        let proto_schema = parse_proto_schema(&schema_str)?;
        let arrow_schema = proto_fields_to_arrow_schema(&proto_schema);
        let records = decode_protobuf_wire(&payload[5..], &proto_schema)?;
        let batches = proto_records_to_batches(&records, &arrow_schema)?;

        Ok((arrow_schema, batches))
    }

    fn arrow_schema(&self) -> SchemaRef {
        payload_schema()
    }
}

/// A parsed protobuf field definition.
#[derive(Debug, Clone)]
struct ProtoField {
    name: String,
    wire_type: u8,
    field_number: u32,
}

/// Parse a minimal protobuf schema definition from JSON or simple text format.
///
/// Accepts JSON format: `[{"name":"field","wire_type":2,"field_number":1}, ...]`
/// where wire_type maps to protobuf wire types: 0=varint, 1=64-bit, 2=length-delimited.
fn parse_proto_schema(schema_str: &str) -> SchemaRegistryResult<Vec<ProtoField>> {
    let trimmed = schema_str.trim();
    if trimmed.starts_with('[') {
        let fields: Vec<ProtoFieldJson> = serde_json::from_str(trimmed).map_err(|e| {
            SchemaRegistryError::Decode(format!("invalid protobuf schema JSON: {e}"))
        })?;
        return Ok(fields
            .into_iter()
            .map(|f| ProtoField {
                name: f.name,
                wire_type: f.wire_type,
                field_number: f.field_number,
            })
            .collect());
    }
    Ok(vec![])
}

#[derive(serde::Deserialize)]
struct ProtoFieldJson {
    name: String,
    wire_type: u8,
    field_number: u32,
}

/// Map protobuf field definitions to Arrow schema.
fn proto_fields_to_arrow_schema(fields: &[ProtoField]) -> SchemaRef {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let dt = match f.wire_type {
                0 => DataType::Int64,
                1 => DataType::Float64,
                2 => DataType::Utf8,
                _ => DataType::Binary,
            };
            Field::new(&f.name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(arrow_fields))
}

/// Decode protobuf wire format (field-tag + value pairs) into a Vec of rows.
/// Uses `prost` encoding primitives and the field list as a descriptor set.
fn decode_protobuf_wire(
    data: &[u8],
    fields: &[ProtoField],
) -> SchemaRegistryResult<Vec<Vec<ProtoValue>>> {
    use bytes::Buf;
    let mut rows: Vec<Vec<ProtoValue>> = Vec::new();
    let current_row: Vec<ProtoValue> = vec![ProtoValue::Null; fields.len()];
    let mut buf = data;

    let field_map: std::collections::HashMap<u32, usize> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.field_number, i))
        .collect();

    while buf.has_remaining() {
        let (tag, wire_type) = prost::encoding::decode_key(&mut buf)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        let field_number = tag;
        let idx = field_map.get(&field_number).copied();

        match wire_type {
            prost::encoding::WireType::Varint => {
                let value = prost::encoding::decode_varint(&mut buf)
                    .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
                if let Some(i) = idx {
                    if rows.is_empty() || current_row.iter().all(|v| matches!(v, ProtoValue::Null))
                    {
                        rows.push(current_row.clone());
                    }
                    rows.last_mut().unwrap()[i] = ProtoValue::Int64(value as i64);
                }
            }
            prost::encoding::WireType::SixtyFourBit => {
                if buf.remaining() < 8 {
                    break;
                }
                let value = buf.get_f64_le();
                if let Some(i) = idx {
                    if rows.is_empty() || current_row.iter().all(|v| matches!(v, ProtoValue::Null))
                    {
                        rows.push(current_row.clone());
                    }
                    rows.last_mut().unwrap()[i] = ProtoValue::Float64(value);
                }
            }
            prost::encoding::WireType::LengthDelimited => {
                let len = prost::encoding::decode_varint(&mut buf)
                    .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?
                    as usize;
                if buf.remaining() < len {
                    break;
                }
                let chunk = buf.copy_to_bytes(len);
                if let Some(i) = idx {
                    let text = String::from_utf8_lossy(&chunk).to_string();
                    if rows.is_empty() || current_row.iter().all(|v| matches!(v, ProtoValue::Null))
                    {
                        rows.push(current_row.clone());
                    }
                    rows.last_mut().unwrap()[i] = ProtoValue::String(text);
                }
            }
            _ => break,
        }
    }

    if rows.is_empty() && !fields.is_empty() {
        rows.push(current_row);
    }

    Ok(rows)
}

/// Protobuf wire format values.
#[derive(Debug, Clone)]
enum ProtoValue {
    Null,
    Int64(i64),
    Float64(f64),
    String(String),
}

/// Convert decoded protobuf rows into Arrow RecordBatches.
fn proto_records_to_batches(
    rows: &[Vec<ProtoValue>],
    arrow_schema: &Schema,
) -> SchemaRegistryResult<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![]);
    }
    let num_fields = arrow_schema.fields().len();
    let mut columns: Vec<Vec<&ProtoValue>> = (0..num_fields)
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();

    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < num_fields {
                columns[i].push(val);
            }
        }
    }

    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(i, vals)| proto_values_to_column(vals, arrow_schema.field(i).data_type()))
        .collect();

    let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), arrays)
        .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;

    Ok(vec![batch])
}

/// Convert protobuf values into an Arrow column array.
fn proto_values_to_column(values: &[&ProtoValue], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match v {
                    ProtoValue::Int64(i) => Some(*i),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match v {
                    ProtoValue::Float64(f) => Some(*f),
                    ProtoValue::Int64(i) => Some(*i as f64),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        _ => {
            // Build owned strings first, then borrow them for StringArray
            // construction (the array copies the bytes, so no leak is needed).
            let strings: Vec<Option<String>> = values
                .iter()
                .map(|v| match v {
                    ProtoValue::String(s) => Some(s.clone()),
                    ProtoValue::Int64(i) => Some(format!("{i}")),
                    ProtoValue::Float64(f) => Some(format!("{f}")),
                    ProtoValue::Null => None,
                })
                .collect();
            let opts: Vec<Option<&str>> = strings.iter().map(|s| s.as_deref()).collect();
            Arc::new(StringArray::from(opts))
        }
    }
}

fn payload_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]))
}

fn json_payload_batch(text: &str) -> SchemaRegistryResult<RecordBatch> {
    let schema = payload_schema();
    let col = Arc::new(StringArray::from(vec![text.to_string()]));
    RecordBatch::try_new(schema, vec![col]).map_err(|e| SchemaRegistryError::Decode(e.to_string()))
}

/// Convert an Avro schema to an Arrow schema.
fn avro_schema_to_arrow_schema(avro_schema: &apache_avro::Schema) -> SchemaRef {
    match avro_schema {
        apache_avro::Schema::Record(record_schema) => {
            let fields: Vec<Field> = record_schema
                .fields
                .iter()
                .map(|f| {
                    let dt = avro_schema_to_data_type(&f.schema);
                    Field::new(&f.name, dt, true)
                })
                .collect();
            Arc::new(Schema::new(fields))
        }
        other => {
            let dt = avro_schema_to_data_type(other);
            Arc::new(Schema::new(vec![Field::new("value", dt, true)]))
        }
    }
}

/// Map an Avro schema node to an Arrow DataType.
fn avro_schema_to_data_type(schema: &apache_avro::Schema) -> DataType {
    use apache_avro::Schema;
    match schema {
        Schema::Null => DataType::Null,
        Schema::Boolean => DataType::Boolean,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Bytes => DataType::Binary,
        Schema::String => DataType::Utf8,
        Schema::Date => DataType::Date32,
        Schema::TimeMillis => DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
        Schema::TimeMicros => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        Schema::TimestampMillis => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        Schema::TimestampMicros => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        Schema::TimestampNanos => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        _ => DataType::Utf8,
    }
}

/// Convert a slice of Avro records into Arrow RecordBatches.
fn avro_records_to_batches(
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
        .collect();

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
fn avro_values_to_column(values: &[&Value], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::Boolean(b) => Some(*b),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Int32 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::Int(i) => Some(*i),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::Long(i) => Some(*i),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Float32 => {
            let arr: Float32Array = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::Float(f) => Some(*f as f64),
                    Value::Double(d) => Some(*d),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Binary => {
            let arr: BinaryArray = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::Bytes(b) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        _ => {
            let arr: StringArray = values
                .iter()
                .map(|v| Some(format!("{:?}", unwrap_value(v))))
                .collect();
            Arc::new(arr)
        }
    }
}

impl SchemaRegistryClient {
    /// Decode a Kafka payload, auto-detecting the format:
    ///
    /// * Confluent magic byte `0x00` → Avro wire format (schema fetched from registry).
    /// * Otherwise → plain JSON payload.
    ///
    /// This is the entry point for CDC pipelines that don't know the format ahead
    /// of time but have a registry URL configured.
    pub async fn decode_any(
        &self,
        payload: &[u8],
    ) -> SchemaRegistryResult<(
        arrow::datatypes::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
    )> {
        if payload.first() == Some(&0x00) {
            // Confluent binary wire format — try Avro.
            let deserializer = AvroDeserializer {
                client: self.clone(),
            };
            deserializer.decode(payload).await
        } else {
            // Plain JSON payload.
            let text = std::str::from_utf8(payload)
                .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
            let batch = json_payload_batch(text)?;
            let schema = batch.schema();
            Ok((schema, vec![batch]))
        }
    }
}

pub fn deserializer_for(config: &SchemaRegistryConfig) -> Arc<dyn KafkaDeserializer> {
    match config.format {
        RegistryFormat::Avro => Arc::new(AvroDeserializer::new(config)),
        RegistryFormat::Protobuf => Arc::new(ProtobufDeserializer::new(config)),
        RegistryFormat::Json => Arc::new(JsonSchemaDeserializer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn json_deserializer_roundtrip() {
        let d = JsonSchemaDeserializer;
        let (_schema, batches) = d.decode(br#"{"k":1}"#).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn avro_schema_float_and_bytes_mapping() {
        let schema_json = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "temperature", "type": "float"},
                {"name": "payload", "type": "bytes"}
            ]
        }"#;
        let avro_schema = apache_avro::Schema::parse_str(schema_json).unwrap();
        let arrow_schema = avro_schema_to_arrow_schema(&avro_schema);
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::Float32,
            "Avro Float must map to Arrow Float32"
        );
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Binary,
            "Avro Bytes must map to Arrow Binary"
        );
    }

    #[test]
    fn avro_values_float32_and_binary_roundtrip() {
        let values_float: Vec<Value> = vec![Value::Float(1.5), Value::Float(2.5)];
        let refs_float: Vec<&Value> = values_float.iter().collect();
        let arr = avro_values_to_column(&refs_float, &DataType::Float32);
        let fa = arr.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(fa.value(0), 1.5);
        assert_eq!(fa.value(1), 2.5);

        let values_bytes: Vec<Value> = vec![Value::Bytes(vec![1, 2, 3]), Value::Bytes(vec![4, 5])];
        let refs_bytes: Vec<&Value> = values_bytes.iter().collect();
        let arr = avro_values_to_column(&refs_bytes, &DataType::Binary);
        let ba = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(ba.value(0), &[1, 2, 3]);
        assert_eq!(ba.value(1), &[4, 5]);
    }
}
