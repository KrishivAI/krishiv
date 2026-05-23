#![forbid(unsafe_code)]
//! Confluent Schema Registry deserialization for Kafka payloads (R18 S3.3).

use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::Reader;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::datatypes::SchemaRef;
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
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<RecordBatch>;
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
            return Err(SchemaRegistryError::Http(resp.text().await.unwrap_or_default()));
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
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<RecordBatch> {
        if payload.len() < 5 || payload[0] != 0 {
            return Err(SchemaRegistryError::Decode(
                "expected Confluent magic byte 0".into(),
            ));
        }
        let schema_id = u32::from_be_bytes(payload[1..5].try_into().unwrap());
        let schema_str = self.client.fetch_schema(schema_id).await?;
        let schema = apache_avro::Schema::parse_str(&schema_str)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        let mut reader = Reader::with_schema(&schema, &payload[5..])
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        let value = reader
            .next()
            .ok_or_else(|| SchemaRegistryError::Decode("empty avro payload".into()))?
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        avro_value_to_batch(&value)
    }

    fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
    }
}

pub struct JsonSchemaDeserializer;

#[async_trait]
impl KafkaDeserializer for JsonSchemaDeserializer {
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<RecordBatch> {
        let text = std::str::from_utf8(payload)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        json_payload_batch(text)
    }

    fn arrow_schema(&self) -> SchemaRef {
        payload_schema()
    }
}

pub struct ProtobufDeserializer;

#[async_trait]
impl KafkaDeserializer for ProtobufDeserializer {
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<RecordBatch> {
        JsonSchemaDeserializer.decode(payload).await
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

fn avro_value_to_batch(value: &Value) -> SchemaRegistryResult<RecordBatch> {
    match value {
        Value::Record(fields) => {
            let mut names = Vec::new();
            let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
            for (name, v) in fields {
                names.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(vec![format!("{v:?}")])) as Arc<dyn arrow::array::Array>);
            }
            let schema = Arc::new(Schema::new(names));
            RecordBatch::try_new(schema, arrays)
                .map_err(|e| SchemaRegistryError::Decode(e.to_string()))
        }
        Value::Int(i) => {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let col = Arc::new(Int64Array::from(vec![*i as i64]));
            RecordBatch::try_new(schema, vec![col])
                .map_err(|e| SchemaRegistryError::Decode(e.to_string()))
        }
        other => json_payload_batch(&format!("{other:?}")),
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
        let batch = d.decode(br#"{"k":1}"#).await.unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
