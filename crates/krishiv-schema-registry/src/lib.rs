#![forbid(unsafe_code)]
//! Confluent Schema Registry deserialization for Kafka payloads (R18 S3.3).

use std::sync::Arc;

use apache_avro::Reader;
use apache_avro::types::Value;
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use reqwest::Client;

/// Errors from schema registry operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRegistryError {
    Http(String),
    Decode(String),
}

impl std::fmt::Display for SchemaRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(m) => write!(f, "schema registry http: {m}"),
            Self::Decode(m) => write!(f, "schema registry decode: {m}"),
        }
    }
}

impl std::error::Error for SchemaRegistryError {}

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
        let schema_id = u32::from_be_bytes(payload[1..5].try_into().unwrap());
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

pub struct ProtobufDeserializer;

#[async_trait]
impl KafkaDeserializer for ProtobufDeserializer {
    async fn decode(&self, _payload: &[u8]) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)> {
        Err(SchemaRegistryError::Decode(
            "Protobuf deserialization is not yet implemented; use Avro or JSON format".into(),
        ))
    }

    fn arrow_schema(&self) -> SchemaRef {
        payload_schema()
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
        Schema::Float | Schema::Double => DataType::Float64,
        Schema::Bytes | Schema::String => DataType::Utf8,
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
        _ => {
            let arr: StringArray = values
                .iter()
                .map(|v| Some(format!("{:?}", unwrap_value(v))))
                .collect();
            Arc::new(arr)
        }
    }
}

pub fn deserializer_for(config: &SchemaRegistryConfig) -> Arc<dyn KafkaDeserializer> {
    match config.format {
        RegistryFormat::Avro => Arc::new(AvroDeserializer::new(config)),
        RegistryFormat::Protobuf => Arc::new(ProtobufDeserializer),
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
}
