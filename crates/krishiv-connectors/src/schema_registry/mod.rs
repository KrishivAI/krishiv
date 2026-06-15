//! Confluent Schema Registry deserialization for Kafka payloads (R18 S3.3).

mod avro;
mod client;
mod protobuf;

pub use avro::AvroDeserializer;
pub use client::SchemaRegistryClient;
pub use protobuf::ProtobufDeserializer;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

/// Errors from schema registry operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaRegistryError {
    #[error("invalid schema registry configuration for {field}: {message}")]
    InvalidConfiguration {
        field: &'static str,
        message: String,
    },
    #[error("schema registry request for schema id {schema_id} failed: {message}")]
    Request { schema_id: u32, message: String },
    #[error("schema registry returned HTTP {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error(
        "schema registry response for schema id {schema_id} (HTTP {status}) exceeded {limit_bytes} bytes"
    )]
    ResponseTooLarge {
        schema_id: u32,
        status: u16,
        limit_bytes: usize,
    },
    #[error("invalid schema registry response: {0}")]
    InvalidResponse(String),
    #[error("schema registry decode: {0}")]
    Decode(String),
}

pub type SchemaRegistryResult<T> = Result<T, SchemaRegistryError>;

/// Payload format handled by the registry client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryFormat {
    Avro,
    Protobuf,
}

/// Configuration for a Confluent-compatible schema registry.
#[derive(Debug, Clone)]
pub struct SchemaRegistryConfig {
    url: String,
    format: RegistryFormat,
}

impl SchemaRegistryConfig {
    /// Create and validate a registry configuration.
    pub fn new(url: impl Into<String>, format: RegistryFormat) -> SchemaRegistryResult<Self> {
        let config = Self {
            url: url.into(),
            format,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate this configuration before constructing a deserializer.
    pub fn validate(&self) -> SchemaRegistryResult<()> {
        SchemaRegistryClient::parse_base_url(&self.url)?;
        Ok(())
    }

    /// Return the validated registry base URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Return the configured wire format.
    pub fn format(&self) -> RegistryFormat {
        self.format
    }
}

/// Deserialize Kafka payloads that use Confluent wire format.
#[async_trait]
pub trait KafkaDeserializer: Send + Sync {
    async fn decode(&self, payload: &[u8]) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)>;
}

pub fn deserializer_for(
    config: &SchemaRegistryConfig,
) -> SchemaRegistryResult<std::sync::Arc<dyn KafkaDeserializer>> {
    match config.format {
        RegistryFormat::Avro => Ok(std::sync::Arc::new(AvroDeserializer::new(config)?)),
        RegistryFormat::Protobuf => Ok(std::sync::Arc::new(ProtobufDeserializer::new(config)?)),
    }
}

impl SchemaRegistryClient {
    /// Decode a Kafka payload using the explicitly configured format.
    pub async fn decode_with_format(
        &self,
        payload: &[u8],
        format: RegistryFormat,
    ) -> SchemaRegistryResult<(SchemaRef, Vec<RecordBatch>)> {
        match format {
            RegistryFormat::Avro => AvroDeserializer { client: self.clone() }.decode(payload).await,
            RegistryFormat::Protobuf => {
                ProtobufDeserializer { client: self.clone() }.decode(payload).await
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use apache_avro::types::Value;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;

    use super::*;
    use super::avro::{
        avro_records_to_batches, avro_schema_to_arrow_schema, avro_values_to_column,
        decode_avro_datum_payload,
    };
    use super::client::{
        SchemaRegistryClient, MAX_CACHED_SCHEMA_BYTES, MAX_CACHED_SCHEMAS,
        MAX_REGISTRY_RESPONSE_BYTES,
    };
    use super::protobuf::{
        decode_protobuf_wire, parse_proto_schema, proto_fields_to_arrow_schema,
        proto_records_to_batches, proto_values_to_column, strip_confluent_protobuf_message_indexes,
        ProtoValue,
    };
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn client_rejects_invalid_base_urls() {
        for url in ["", "registry:8081", "ftp://registry.example", "http://"] {
            let error = SchemaRegistryClient::new(url).err().unwrap();
            assert!(matches!(
                error,
                SchemaRegistryError::InvalidConfiguration { field: "url", .. }
            ));
        }
        assert!(SchemaRegistryClient::new("https://registry.example").is_ok());
        assert!(SchemaRegistryClient::new("https://registry.example/api?tenant=a").is_err());
    }

    #[test]
    fn deserializer_config_requires_valid_url() {
        let error = SchemaRegistryConfig::new("not-a-url", RegistryFormat::Protobuf).unwrap_err();
        assert!(matches!(
            error,
            SchemaRegistryError::InvalidConfiguration { field: "url", .. }
        ));

        let config =
            SchemaRegistryConfig::new("https://registry.example", RegistryFormat::Avro).unwrap();
        assert_eq!(config.url(), "https://registry.example");
        assert_eq!(config.format(), RegistryFormat::Avro);
    }

    #[tokio::test]
    async fn fetch_schema_preserves_status_and_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/7"))
            .respond_with(ResponseTemplate::new(503).set_body_string("registry unavailable"))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(server.uri()).unwrap();

        let error = client.fetch_schema(7).await.unwrap_err();

        assert_eq!(
            error,
            SchemaRegistryError::HttpStatus {
                status: 503,
                body: "registry unavailable".into(),
            }
        );
    }

    #[tokio::test]
    async fn fetch_schema_coalesces_concurrent_cache_misses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/registry/schemas/ids/42"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(25))
                    .set_body_json(serde_json::json!({"schema": "\"string\""})),
            )
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(format!("{}/registry/", server.uri())).unwrap();

        let (first, second) = tokio::join!(client.fetch_schema(42), client.fetch_schema(42));

        assert_eq!(first.unwrap(), "\"string\"");
        assert_eq!(second.unwrap(), "\"string\"");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        assert!(client.fetch_locks.is_empty());
    }

    #[tokio::test]
    async fn parsed_avro_schema_is_cached_with_raw_schema() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/5"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"schema": "\"string\""})),
            )
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(server.uri()).unwrap();

        let first = client.fetch_avro_schema(5).await.unwrap();
        let second = client.fetch_avro_schema(5).await.unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancelled_fetch_releases_request_lock() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/9"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(1))
                    .set_body_json(serde_json::json!({"schema": "\"string\""})),
            )
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(server.uri()).unwrap();
        let task_client = client.clone();
        let task = tokio::spawn(async move { task_client.fetch_schema(9).await });
        tokio::time::sleep(Duration::from_millis(25)).await;

        task.abort();
        let _ = task.await;

        assert!(client.fetch_locks.is_empty());
    }

    #[test]
    fn schema_cache_evicts_least_recently_used_entries() {
        let client = SchemaRegistryClient::new("https://registry.example").unwrap();
        for id in 0..MAX_CACHED_SCHEMAS as u32 {
            client.cache_schema(id, format!("schema-{id}"));
        }
        client.cache_parsed_avro(1, Arc::new(apache_avro::Schema::String));
        assert_eq!(client.cached_schema(0).as_deref(), Some("schema-0"));

        client.cache_schema(MAX_CACHED_SCHEMAS as u32, "new-schema".into());

        assert!(client.cached_schema(1).is_none());
        assert!(!client.avro_cache.contains_key(&1));
        assert_eq!(client.cache.len(), MAX_CACHED_SCHEMAS);
        assert_eq!(
            client.cached_schema(MAX_CACHED_SCHEMAS as u32).as_deref(),
            Some("new-schema")
        );
        assert!(
            client
                .cache_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .total_bytes
                <= MAX_CACHED_SCHEMA_BYTES
        );
    }

    #[tokio::test]
    async fn fetch_schema_rejects_blank_schema_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": " "
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(server.uri()).unwrap();

        let error = client.fetch_schema(3).await.unwrap_err();

        assert!(
            matches!(error, SchemaRegistryError::InvalidResponse(message) if message.contains("blank schema"))
        );
    }

    #[tokio::test]
    async fn fetch_schema_rejects_oversized_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/11"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![
                b'x';
                MAX_REGISTRY_RESPONSE_BYTES
                    + 1
            ]))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(server.uri()).unwrap();

        let error = client.fetch_schema(11).await.unwrap_err();

        assert_eq!(
            error,
            SchemaRegistryError::ResponseTooLarge {
                schema_id: 11,
                status: 200,
                limit_bytes: MAX_REGISTRY_RESPONSE_BYTES,
            }
        );
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
        let arrow_schema = avro_schema_to_arrow_schema(&avro_schema).unwrap();
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
        let arr = avro_values_to_column(&refs_float, &DataType::Float32).unwrap();
        let fa = arr.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(fa.value(0), 1.5);
        assert_eq!(fa.value(1), 2.5);

        let values_bytes: Vec<Value> = vec![Value::Bytes(vec![1, 2, 3]), Value::Bytes(vec![4, 5])];
        let refs_bytes: Vec<&Value> = values_bytes.iter().collect();
        let arr = avro_values_to_column(&refs_bytes, &DataType::Binary).unwrap();
        let ba = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(ba.value(0), &[1, 2, 3]);
        assert_eq!(ba.value(1), &[4, 5]);
    }

    #[test]
    fn avro_nullable_union_preserves_type_and_nullability() {
        let avro_schema = apache_avro::Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "note", "type": ["null", "string"], "default": null}
                ]
            }"#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(0).is_nullable());
    }

    #[test]
    fn avro_nested_structures_fail_closed() {
        let avro_schema = apache_avro::Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                ]
            }"#,
        )
        .unwrap();

        let error = avro_schema_to_arrow_schema(&avro_schema).unwrap_err();

        assert!(
            matches!(error, SchemaRegistryError::Decode(message) if message.contains("unsupported Avro schema type"))
        );
    }

    #[test]
    fn avro_confluent_schemaless_datum_decodes_to_arrow() {
        let schema_json = r#"{
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
            ]
        }"#;
        let avro_schema = apache_avro::Schema::parse_str(schema_json).unwrap();
        let value = Value::Record(vec![
            ("id".to_string(), Value::Long(42)),
            ("name".to_string(), Value::String("alice".to_string())),
            ("active".to_string(), Value::Boolean(true)),
        ]);
        let datum = apache_avro::to_avro_datum(&avro_schema, value).unwrap();

        let (schema, batches) = decode_avro_datum_payload(&avro_schema, &datum).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let id = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let active = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(id.value(0), 42);
        assert_eq!(name.value(0), "alice");
        assert!(active.value(0));
    }

    #[test]
    fn parse_proto_text_schema_extracts_first_message_scalar_fields() {
        let schema = r#"
            syntax = "proto3";
            package krishiv.test;

            message Order {
              string id = 1;
              int64 amount = 2;
              bool active = 3;
              double score = 4;
              bytes raw = 5;

              message Nested {
                string ignored = 1;
              }
            }
        "#;

        let fields = parse_proto_schema(schema).unwrap();
        let arrow = proto_fields_to_arrow_schema(&fields);

        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].field_number, 1);
        assert_eq!(arrow.field(0).data_type(), &DataType::Utf8);
        assert_eq!(arrow.field(1).data_type(), &DataType::Int64);
        assert_eq!(arrow.field(2).data_type(), &DataType::Boolean);
        assert_eq!(arrow.field(3).data_type(), &DataType::Float64);
        assert_eq!(arrow.field(4).data_type(), &DataType::Binary);
    }

    #[test]
    fn protobuf_schema_rejects_duplicate_numbers_and_repeated_fields() {
        let duplicate = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Order {
              string id = 1;
              int64 amount = 1;
            }
            "#,
        )
        .unwrap_err();
        assert!(
            matches!(duplicate, SchemaRegistryError::Decode(message) if message.contains("duplicate protobuf field number"))
        );

        let repeated = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Order {
              repeated string tags = 1;
            }
            "#,
        )
        .unwrap_err();
        assert!(
            matches!(repeated, SchemaRegistryError::Decode(message) if message.contains("repeated fields"))
        );
    }

    #[test]
    fn protobuf_scalar_types_preserve_arrow_signedness_and_width() {
        let fields = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Numbers {
              int32 i32_value = 1;
              uint32 u32_value = 2;
              uint64 u64_value = 3;
              fixed32 fixed32_value = 4;
              sfixed64 sfixed64_value = 5;
            }
            "#,
        )
        .unwrap();
        let arrow = proto_fields_to_arrow_schema(&fields);
        assert_eq!(arrow.field(0).data_type(), &DataType::Int32);
        assert_eq!(arrow.field(1).data_type(), &DataType::UInt32);
        assert_eq!(arrow.field(2).data_type(), &DataType::UInt64);
        assert_eq!(arrow.field(3).data_type(), &DataType::UInt32);
        assert_eq!(arrow.field(4).data_type(), &DataType::Int64);
    }

    #[test]
    fn protobuf_rejects_unknown_only_payload_and_non_default_message() {
        let fields = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Order {
              optional string id = 1;
            }
            "#,
        )
        .unwrap();
        let mut unknown_only = Vec::new();
        push_proto_key(&mut unknown_only, 2, 0);
        push_proto_varint(&mut unknown_only, 7);
        let error = decode_protobuf_wire(&unknown_only, &fields).unwrap_err();
        assert!(
            matches!(error, SchemaRegistryError::Decode(message) if message.contains("did not contain any fields"))
        );

        // Zig-zag encoded path length 1 followed by message index 1.
        let error = strip_confluent_protobuf_message_indexes(&[2, 2, 0]).unwrap_err();
        assert!(
            matches!(error, SchemaRegistryError::Decode(message) if message.contains("only the first top-level message"))
        );
    }

    #[test]
    fn protobuf_presence_semantics_are_enforced() {
        let proto3 = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Defaults {
              int32 count = 1;
              string name = 2;
              optional bool enabled = 3;
            }
            "#,
        )
        .unwrap();
        let rows = decode_protobuf_wire(&[], &proto3).unwrap();
        assert!(matches!(&rows[0][0], ProtoValue::Int32(0)));
        assert!(matches!(&rows[0][1], ProtoValue::String(value) if value.is_empty()));
        assert!(matches!(&rows[0][2], ProtoValue::Null));

        let proto2 = parse_proto_schema(
            r#"
            syntax = "proto2";
            message Required {
              required int64 id = 1;
            }
            "#,
        )
        .unwrap();
        let error = decode_protobuf_wire(&[], &proto2).unwrap_err();
        assert!(
            matches!(error, SchemaRegistryError::Decode(message) if message.contains("omitted required field"))
        );
    }

    #[test]
    fn protobuf_schema_rejects_oneof_fields() {
        let error = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Choice {
              oneof value {
                string text = 1;
                int64 number = 2;
              }
            }
            "#,
        )
        .unwrap_err();
        assert!(
            matches!(error, SchemaRegistryError::Decode(message) if message.contains("oneof fields"))
        );
    }

    #[test]
    fn protobuf_arrow_conversion_rejects_internal_type_mismatch() {
        let value = ProtoValue::Int64(7);
        let error = proto_values_to_column(&[&value], &DataType::Utf8).unwrap_err();
        assert!(
            matches!(error, SchemaRegistryError::Decode(message) if message.contains("does not match Arrow type"))
        );
    }

    #[test]
    fn protobuf_confluent_payload_decodes_scalar_fields() {
        let fields = parse_proto_schema(
            r#"
            syntax = "proto3";
            message Order {
              string id = 1;
              int64 amount = 2;
              bool active = 3;
              double score = 4;
              bytes raw = 5;
            }
            "#,
        )
        .unwrap();
        let arrow_schema = proto_fields_to_arrow_schema(&fields);

        let mut payload = vec![0]; // Confluent optimized message index path for [0].
        push_proto_key(&mut payload, 1, 2);
        push_proto_bytes(&mut payload, b"order-1");
        push_proto_key(&mut payload, 2, 0);
        push_proto_varint(&mut payload, 42);
        push_proto_key(&mut payload, 3, 0);
        push_proto_varint(&mut payload, 1);
        push_proto_key(&mut payload, 4, 1);
        payload.extend_from_slice(&1.25_f64.to_bits().to_le_bytes());
        push_proto_key(&mut payload, 5, 2);
        push_proto_bytes(&mut payload, &[1, 2, 3]);

        let message = strip_confluent_protobuf_message_indexes(&payload).unwrap();
        let rows = decode_protobuf_wire(message, &fields).unwrap();
        let batches = proto_records_to_batches(&rows, &arrow_schema).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let id = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amount = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let active = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let score = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let raw = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        assert_eq!(id.value(0), "order-1");
        assert_eq!(amount.value(0), 42);
        assert!(active.value(0));
        assert_eq!(score.value(0), 1.25);
        assert_eq!(raw.value(0), &[1, 2, 3]);
    }

    fn push_proto_key(buf: &mut Vec<u8>, field_number: u32, wire_type: u8) {
        push_proto_varint(buf, ((field_number as u64) << 3) | u64::from(wire_type));
    }

    fn push_proto_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
        push_proto_varint(buf, bytes.len() as u64);
        buf.extend_from_slice(bytes);
    }

    fn push_proto_varint(buf: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            buf.push((value as u8) | 0x80);
            value >>= 7;
        }
        buf.push(value as u8);
    }
}
