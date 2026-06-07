//! Confluent Schema Registry deserialization for Kafka payloads (R18 S3.3).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};

use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    Int64Array, NullArray, StringArray, Time32MillisecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use reqwest::header::ACCEPT;
use reqwest::{Client, Url};
use tokio::sync::Mutex as AsyncMutex;

const MAX_REGISTRY_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_ERROR_BODY_CHARS: usize = 8 * 1024;
const MAX_CACHED_SCHEMAS: usize = 1_024;
const MAX_CACHED_SCHEMA_BYTES: usize = 32 * 1024 * 1024;

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

/// HTTP schema cache keyed by schema id.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    base_url: Url,
    cache: Arc<DashMap<u32, String>>,
    avro_cache: Arc<DashMap<u32, Arc<apache_avro::Schema>>>,
    protobuf_cache: Arc<DashMap<u32, Arc<Vec<ProtoField>>>>,
    cache_state: Arc<StdMutex<SchemaCacheState>>,
    fetch_locks: Arc<DashMap<u32, Arc<AsyncMutex<()>>>>,
    http: Client,
}

#[derive(Default)]
struct SchemaCacheState {
    order: VecDeque<u32>,
    total_bytes: usize,
}

struct FetchLockLease<'a> {
    locks: &'a DashMap<u32, Arc<AsyncMutex<()>>>,
    id: u32,
    lock: Arc<AsyncMutex<()>>,
}

impl Drop for FetchLockLease<'_> {
    fn drop(&mut self) {
        if let dashmap::mapref::entry::Entry::Occupied(entry) = self.locks.entry(self.id)
            && Arc::ptr_eq(entry.get(), &self.lock)
            && Arc::strong_count(entry.get()) == 2
        {
            entry.remove();
        }
    }
}

impl SchemaRegistryClient {
    /// Create a registry client with bounded request timeouts.
    pub fn new(url: impl AsRef<str>) -> SchemaRegistryResult<Self> {
        let http = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .user_agent(concat!(
                "krishiv-connectors-schema-registry/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .map_err(|error| SchemaRegistryError::InvalidConfiguration {
                field: "http_client",
                message: error.to_string(),
            })?;
        Self::with_http_client(url, http)
    }

    /// Create a registry client with a caller-configured HTTP client.
    ///
    /// This supports custom authentication headers, private certificate roots,
    /// proxies, and timeout policies without embedding secrets in registry URLs.
    pub fn with_http_client(url: impl AsRef<str>, http: Client) -> SchemaRegistryResult<Self> {
        let base_url = Self::parse_base_url(url.as_ref())?;
        Ok(Self {
            base_url,
            cache: Arc::new(DashMap::new()),
            avro_cache: Arc::new(DashMap::new()),
            protobuf_cache: Arc::new(DashMap::new()),
            cache_state: Arc::new(StdMutex::new(SchemaCacheState::default())),
            fetch_locks: Arc::new(DashMap::new()),
            http,
        })
    }

    pub async fn fetch_schema(&self, id: u32) -> SchemaRegistryResult<String> {
        if let Some(hit) = self.cached_schema(id) {
            return Ok(hit);
        }

        let lease = FetchLockLease {
            locks: &self.fetch_locks,
            id,
            lock: self
                .fetch_locks
                .entry(id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone(),
        };
        let guard = lease.lock.lock().await;
        let result = if let Some(hit) = self.cached_schema(id) {
            Ok(hit)
        } else {
            self.fetch_schema_uncached(id).await
        };
        drop(guard);
        result
    }

    async fn fetch_schema_uncached(&self, id: u32) -> SchemaRegistryResult<String> {
        let url = self.schema_url(id)?;
        let resp = self
            .http
            .get(url)
            .header(
                ACCEPT,
                "application/vnd.schemaregistry.v1+json, application/json",
            )
            .send()
            .await
            .map_err(|error| SchemaRegistryError::Request {
                schema_id: id,
                message: error.to_string(),
            })?;
        let status = resp.status();
        let body = read_response_body(resp, id, status.as_u16()).await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&body);
            let body = bounded_error_body(body.trim());
            return Err(SchemaRegistryError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        #[derive(serde::Deserialize)]
        struct Body {
            schema: String,
        }
        let body: Body = serde_json::from_slice(&body).map_err(|error| {
            SchemaRegistryError::InvalidResponse(format!(
                "schema id {id} response was not valid JSON: {error}"
            ))
        })?;
        if body.schema.trim().is_empty() {
            return Err(SchemaRegistryError::InvalidResponse(format!(
                "schema id {id} response contained a blank schema"
            )));
        }
        self.cache_schema(id, body.schema.clone());
        Ok(body.schema)
    }

    fn cached_schema(&self, id: u32) -> Option<String> {
        let mut state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let schema = self.cache.get(&id)?.clone();
        touch_cache_order(&mut state.order, id);
        Some(schema)
    }

    fn touch_cached_schema(&self, id: u32) {
        let mut state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache.contains_key(&id) {
            touch_cache_order(&mut state.order, id);
        }
    }

    fn cache_schema(&self, id: u32, schema: String) {
        let mut state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((_, previous)) = self.cache.remove(&id) {
            state.total_bytes = state.total_bytes.saturating_sub(previous.len());
        }
        self.avro_cache.remove(&id);
        self.protobuf_cache.remove(&id);
        if let Some(index) = state.order.iter().position(|cached_id| *cached_id == id) {
            state.order.remove(index);
        }
        if schema.len() > MAX_CACHED_SCHEMA_BYTES {
            return;
        }
        while state.order.len() >= MAX_CACHED_SCHEMAS
            || state.total_bytes.saturating_add(schema.len()) > MAX_CACHED_SCHEMA_BYTES
        {
            if let Some(evicted_id) = state.order.pop_front() {
                if let Some((_, evicted)) = self.cache.remove(&evicted_id) {
                    state.total_bytes = state.total_bytes.saturating_sub(evicted.len());
                }
                self.avro_cache.remove(&evicted_id);
                self.protobuf_cache.remove(&evicted_id);
            } else {
                state.total_bytes = 0;
                break;
            }
        }
        state.total_bytes += schema.len();
        self.cache.insert(id, schema);
        state.order.push_back(id);
    }

    async fn fetch_avro_schema(&self, id: u32) -> SchemaRegistryResult<Arc<apache_avro::Schema>> {
        if let Some(cached) = self.avro_cache.get(&id) {
            let schema = cached.clone();
            drop(cached);
            self.touch_cached_schema(id);
            return Ok(schema);
        }
        let schema_str = self.fetch_schema(id).await?;
        if let Some(schema) = self.avro_cache.get(&id) {
            return Ok(schema.clone());
        }
        let schema = Arc::new(
            apache_avro::Schema::parse_str(&schema_str)
                .map_err(|error| SchemaRegistryError::Decode(error.to_string()))?,
        );
        self.cache_parsed_avro(id, schema.clone());
        Ok(schema)
    }

    async fn fetch_protobuf_schema(&self, id: u32) -> SchemaRegistryResult<Arc<Vec<ProtoField>>> {
        if let Some(cached) = self.protobuf_cache.get(&id) {
            let schema = cached.clone();
            drop(cached);
            self.touch_cached_schema(id);
            return Ok(schema);
        }
        let schema_str = self.fetch_schema(id).await?;
        if let Some(schema) = self.protobuf_cache.get(&id) {
            return Ok(schema.clone());
        }
        let schema = Arc::new(parse_proto_schema(&schema_str)?);
        self.cache_parsed_protobuf(id, schema.clone());
        Ok(schema)
    }

    fn cache_parsed_avro(&self, id: u32, schema: Arc<apache_avro::Schema>) {
        let _state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache.contains_key(&id) {
            self.avro_cache.insert(id, schema);
        }
    }

    fn cache_parsed_protobuf(&self, id: u32, schema: Arc<Vec<ProtoField>>) {
        let _state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache.contains_key(&id) {
            self.protobuf_cache.insert(id, schema);
        }
    }

    fn parse_base_url(raw_url: &str) -> SchemaRegistryResult<Url> {
        let raw_url = raw_url.trim();
        if raw_url.is_empty() {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "must not be blank".into(),
            });
        }
        let mut url =
            Url::parse(raw_url).map_err(|error| SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: error.to_string(),
            })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "scheme must be http or https".into(),
            });
        }
        if url.cannot_be_a_base() || url.host_str().is_none() {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "must be an absolute hierarchical URL with a host".into(),
            });
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "query parameters and fragments are not allowed".into(),
            });
        }
        let normalized_path = url.path().trim_end_matches('/').to_string();
        url.set_path(if normalized_path.is_empty() {
            "/"
        } else {
            &normalized_path
        });
        Ok(url)
    }

    fn schema_url(&self, id: u32) -> SchemaRegistryResult<Url> {
        let mut url = self.base_url.clone();
        let id = id.to_string();
        url.path_segments_mut()
            .map_err(|_| SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "cannot append registry API path".into(),
            })?
            .pop_if_empty()
            .extend(["schemas", "ids", id.as_str()]);
        Ok(url)
    }
}

fn touch_cache_order(order: &mut VecDeque<u32>, id: u32) {
    if let Some(index) = order.iter().position(|cached_id| *cached_id == id) {
        order.remove(index);
    }
    order.push_back(id);
}

fn bounded_error_body(body: &str) -> String {
    if body.is_empty() {
        return "<empty response body>".into();
    }
    let mut chars = body.chars();
    let mut bounded = chars
        .by_ref()
        .take(MAX_ERROR_BODY_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        bounded.push_str("...");
    }
    bounded
}

async fn read_response_body(
    mut response: reqwest::Response,
    schema_id: u32,
    status: u16,
) -> SchemaRegistryResult<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) =
        response
            .chunk()
            .await
            .map_err(|error| SchemaRegistryError::Request {
                schema_id,
                message: error.to_string(),
            })?
    {
        let next_len =
            body.len()
                .checked_add(chunk.len())
                .ok_or(SchemaRegistryError::ResponseTooLarge {
                    schema_id,
                    status,
                    limit_bytes: MAX_REGISTRY_RESPONSE_BYTES,
                })?;
        if next_len > MAX_REGISTRY_RESPONSE_BYTES {
            return Err(SchemaRegistryError::ResponseTooLarge {
                schema_id,
                status,
                limit_bytes: MAX_REGISTRY_RESPONSE_BYTES,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Avro deserializer (Confluent wire format + registry fetch).
#[derive(Clone)]
pub struct AvroDeserializer {
    client: SchemaRegistryClient,
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
    pub fn new(config: &SchemaRegistryConfig) -> SchemaRegistryResult<Self> {
        config.validate()?;
        Ok(Self {
            client: SchemaRegistryClient::new(&config.url)?,
        })
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
        let proto_schema = self.client.fetch_protobuf_schema(schema_id).await?;
        let arrow_schema = proto_fields_to_arrow_schema(&proto_schema);
        let message_payload = strip_confluent_protobuf_message_indexes(&payload[5..])?;
        let records = decode_protobuf_wire(message_payload, &proto_schema)?;
        let batches = proto_records_to_batches(&records, &arrow_schema)?;

        Ok((arrow_schema, batches))
    }
}

/// A parsed protobuf field definition.
#[derive(Debug, Clone)]
struct ProtoField {
    name: String,
    wire_type: u8,
    field_number: u32,
    field_type: ProtoFieldType,
    presence: ProtoPresence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtoFieldType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    SInt32,
    SInt64,
    Fixed32,
    Fixed64,
    SFixed32,
    SFixed64,
    Bool,
    Float32,
    Float64,
    String,
    Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtoPresence {
    Implicit,
    Optional,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtoSyntax {
    Proto2,
    Proto3,
}

/// Parse scalar fields from the first top-level message in a `.proto` schema.
fn parse_proto_schema(schema_str: &str) -> SchemaRegistryResult<Vec<ProtoField>> {
    let trimmed = schema_str.trim();
    let fields = parse_proto_text_schema(trimmed)?;
    validate_proto_fields(&fields)?;
    Ok(fields)
}

fn validate_proto_fields(fields: &[ProtoField]) -> SchemaRegistryResult<()> {
    if fields.is_empty() {
        return Err(SchemaRegistryError::Decode(
            "protobuf schema did not contain any fields".into(),
        ));
    }
    let mut names = std::collections::HashSet::with_capacity(fields.len());
    let mut numbers = std::collections::HashSet::with_capacity(fields.len());
    for field in fields {
        if !is_proto_identifier(&field.name) {
            return Err(SchemaRegistryError::Decode(format!(
                "invalid protobuf field name '{}'",
                field.name
            )));
        }
        if !names.insert(field.name.as_str()) {
            return Err(SchemaRegistryError::Decode(format!(
                "duplicate protobuf field name '{}'",
                field.name
            )));
        }
        if field.field_number == 0
            || field.field_number > 536_870_911
            || (19_000..=19_999).contains(&field.field_number)
        {
            return Err(SchemaRegistryError::Decode(format!(
                "invalid protobuf field number {} for '{}'",
                field.field_number, field.name
            )));
        }
        if !numbers.insert(field.field_number) {
            return Err(SchemaRegistryError::Decode(format!(
                "duplicate protobuf field number {}",
                field.field_number
            )));
        }
        if !matches!(field.wire_type, 0 | 1 | 2 | 5) {
            return Err(SchemaRegistryError::Decode(format!(
                "unsupported protobuf wire type {} for '{}'",
                field.wire_type, field.name
            )));
        }
    }
    Ok(())
}

fn is_proto_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn parse_proto_text_schema(schema_str: &str) -> SchemaRegistryResult<Vec<ProtoField>> {
    if schema_str.contains("/*") || schema_str.contains("*/") {
        return Err(SchemaRegistryError::Decode(
            "protobuf block comments are not supported by this decoder".into(),
        ));
    }
    let mut fields = Vec::new();
    let mut in_first_message = false;
    let mut depth = 0i32;
    let mut syntax = ProtoSyntax::Proto2;

    for raw_line in schema_str.lines() {
        let line = raw_line
            .split_once("//")
            .map_or(raw_line, |(before, _)| before)
            .trim();
        if line.is_empty() {
            continue;
        }

        let opens = line.chars().filter(|c| *c == '{').count() as i32;
        let closes = line.chars().filter(|c| *c == '}').count() as i32;

        if !in_first_message {
            if line.starts_with("syntax") {
                syntax = if line.contains("\"proto3\"") {
                    ProtoSyntax::Proto3
                } else if line.contains("\"proto2\"") {
                    ProtoSyntax::Proto2
                } else {
                    return Err(SchemaRegistryError::Decode(format!(
                        "unsupported protobuf syntax declaration '{line}'"
                    )));
                };
            }
            if line.starts_with("message ") && line.contains('{') {
                in_first_message = true;
                depth += opens - closes;
            }
            continue;
        }

        if depth == 1 && line.ends_with(';') {
            if let Some(field) = parse_proto_text_field(line, syntax)? {
                fields.push(field);
            }
        }
        if depth == 1 && line.starts_with("oneof ") {
            return Err(SchemaRegistryError::Decode(
                "protobuf oneof fields are not supported by this decoder".into(),
            ));
        }

        depth += opens - closes;
        if in_first_message && depth <= 0 {
            break;
        }
    }

    if fields.is_empty() {
        return Err(SchemaRegistryError::Decode(
            "protobuf schema did not contain supported scalar fields in the first message".into(),
        ));
    }
    Ok(fields)
}

fn parse_proto_text_field(
    line: &str,
    syntax: ProtoSyntax,
) -> SchemaRegistryResult<Option<ProtoField>> {
    let statement = line.trim_end_matches(';').trim();
    let skip_prefixes = [
        "option ",
        "reserved ",
        "extensions ",
        "message ",
        "enum ",
        "service ",
        "rpc ",
    ];
    if skip_prefixes
        .iter()
        .any(|prefix| statement.starts_with(prefix))
    {
        return Ok(None);
    }
    if statement.starts_with("map<") {
        return Err(SchemaRegistryError::Decode(
            "protobuf map fields are not supported by this decoder".into(),
        ));
    }

    let Some((left, right)) = statement.split_once('=') else {
        return Ok(None);
    };
    let field_number_text = right
        .trim()
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or_default();
    let field_number = field_number_text.parse::<u32>().map_err(|e| {
        SchemaRegistryError::Decode(format!("invalid protobuf field number in '{line}': {e}"))
    })?;

    let mut tokens: Vec<&str> = left.split_whitespace().collect();
    if tokens.first().copied() == Some("repeated") {
        return Err(SchemaRegistryError::Decode(
            "protobuf repeated fields are not supported by this decoder".into(),
        ));
    }
    let presence = match tokens.first().copied() {
        Some("optional") => {
            tokens.remove(0);
            ProtoPresence::Optional
        }
        Some("required") => {
            if syntax == ProtoSyntax::Proto3 {
                return Err(SchemaRegistryError::Decode(
                    "protobuf required fields are invalid in proto3".into(),
                ));
            }
            tokens.remove(0);
            ProtoPresence::Required
        }
        _ if syntax == ProtoSyntax::Proto3 => ProtoPresence::Implicit,
        _ => {
            return Err(SchemaRegistryError::Decode(format!(
                "proto2 field declaration requires optional, required, or repeated: '{line}'"
            )));
        }
    };
    if tokens.len() != 2 {
        return Err(SchemaRegistryError::Decode(format!(
            "unsupported protobuf field declaration '{line}'"
        )));
    }
    let proto_type = tokens[0];
    let name = tokens[1].to_string();
    let (wire_type, field_type) = proto_scalar_type(proto_type).ok_or_else(|| {
        SchemaRegistryError::Decode(format!(
            "unsupported protobuf scalar type '{proto_type}' in field '{name}'"
        ))
    })?;

    Ok(Some(ProtoField {
        name,
        wire_type,
        field_number,
        field_type,
        presence,
    }))
}

fn proto_scalar_type(proto_type: &str) -> Option<(u8, ProtoFieldType)> {
    match proto_type {
        "double" => Some((1, ProtoFieldType::Float64)),
        "float" => Some((5, ProtoFieldType::Float32)),
        "int32" => Some((0, ProtoFieldType::Int32)),
        "int64" => Some((0, ProtoFieldType::Int64)),
        "uint32" => Some((0, ProtoFieldType::UInt32)),
        "uint64" => Some((0, ProtoFieldType::UInt64)),
        "fixed32" => Some((5, ProtoFieldType::Fixed32)),
        "sfixed32" => Some((5, ProtoFieldType::SFixed32)),
        "fixed64" => Some((1, ProtoFieldType::Fixed64)),
        "sfixed64" => Some((1, ProtoFieldType::SFixed64)),
        "sint32" => Some((0, ProtoFieldType::SInt32)),
        "sint64" => Some((0, ProtoFieldType::SInt64)),
        "bool" => Some((0, ProtoFieldType::Bool)),
        "string" => Some((2, ProtoFieldType::String)),
        "bytes" => Some((2, ProtoFieldType::Bytes)),
        _ => None,
    }
}

/// Map protobuf field definitions to Arrow schema.
fn proto_fields_to_arrow_schema(fields: &[ProtoField]) -> SchemaRef {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let dt = match f.field_type {
                ProtoFieldType::Int32 | ProtoFieldType::SInt32 | ProtoFieldType::SFixed32 => {
                    DataType::Int32
                }
                ProtoFieldType::Int64 | ProtoFieldType::SInt64 | ProtoFieldType::SFixed64 => {
                    DataType::Int64
                }
                ProtoFieldType::UInt32 | ProtoFieldType::Fixed32 => DataType::UInt32,
                ProtoFieldType::UInt64 | ProtoFieldType::Fixed64 => DataType::UInt64,
                ProtoFieldType::Bool => DataType::Boolean,
                ProtoFieldType::Float32 => DataType::Float32,
                ProtoFieldType::Float64 => DataType::Float64,
                ProtoFieldType::String => DataType::Utf8,
                ProtoFieldType::Bytes => DataType::Binary,
            };
            Field::new(&f.name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(arrow_fields))
}

fn strip_confluent_protobuf_message_indexes(data: &[u8]) -> SchemaRegistryResult<&[u8]> {
    if data.is_empty() {
        return Err(SchemaRegistryError::Decode(
            "protobuf payload missing message indexes".into(),
        ));
    }
    if data[0] == 0 {
        return Ok(&data[1..]);
    }

    let mut buf = data;
    let len_raw = prost::encoding::decode_varint(&mut buf)
        .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
    let len = decode_zigzag_i64(len_raw);
    if len < 0 {
        return Err(SchemaRegistryError::Decode(
            "protobuf message index length cannot be negative".into(),
        ));
    }
    if len > 64 {
        return Err(SchemaRegistryError::Decode(
            "protobuf message index path exceeds 64 entries".into(),
        ));
    }
    let mut indexes = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let raw_index = prost::encoding::decode_varint(&mut buf)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        let index = decode_zigzag_i64(raw_index);
        if index < 0 {
            return Err(SchemaRegistryError::Decode(
                "protobuf message indexes cannot be negative".into(),
            ));
        }
        indexes.push(index);
    }
    if indexes != [0] {
        return Err(SchemaRegistryError::Decode(format!(
            "protobuf message index path {indexes:?} is unsupported; only the first top-level message is supported"
        )));
    }
    Ok(buf)
}

fn decode_zigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn decode_avro_datum_payload(
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

/// Decode protobuf wire format (field-tag + value pairs) into a Vec of rows.
/// Uses `prost` encoding primitives and the field list as a descriptor set.
fn decode_protobuf_wire(
    data: &[u8],
    fields: &[ProtoField],
) -> SchemaRegistryResult<Vec<Vec<ProtoValue>>> {
    use bytes::Buf;
    let mut current_row = fields
        .iter()
        .map(|field| match field.presence {
            ProtoPresence::Implicit => default_proto_value(field.field_type),
            ProtoPresence::Optional | ProtoPresence::Required => ProtoValue::Null,
        })
        .collect::<Vec<_>>();
    let mut saw_field = false;
    let mut saw_wire_field = false;
    let mut buf = data;

    let field_map: std::collections::HashMap<u32, usize> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.field_number, i))
        .collect();

    while buf.has_remaining() {
        let (tag, wire_type) = prost::encoding::decode_key(&mut buf)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
        saw_wire_field = true;
        let field_number = tag;
        let idx = field_map.get(&field_number).copied();
        if let Some(i) = idx
            && Some(fields[i].wire_type) != prost_wire_type_id(wire_type)
        {
            return Err(SchemaRegistryError::Decode(format!(
                "protobuf field '{}' expected wire type {} but payload used {:?}",
                fields[i].name, fields[i].wire_type, wire_type
            )));
        }

        match wire_type {
            prost::encoding::WireType::Varint => {
                let value = prost::encoding::decode_varint(&mut buf)
                    .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
                if let Some(i) = idx {
                    current_row[i] = match fields[i].field_type {
                        ProtoFieldType::Bool => ProtoValue::Bool(value != 0),
                        ProtoFieldType::Int32 => {
                            ProtoValue::Int32(checked_proto_i32(value as i64, &fields[i].name)?)
                        }
                        ProtoFieldType::UInt32 => {
                            ProtoValue::UInt32(u32::try_from(value).map_err(|_| {
                                SchemaRegistryError::Decode(format!(
                                    "protobuf uint32 field '{}' overflowed",
                                    fields[i].name
                                ))
                            })?)
                        }
                        ProtoFieldType::UInt64 => ProtoValue::UInt64(value),
                        ProtoFieldType::SInt32 => ProtoValue::Int32(checked_proto_i32(
                            decode_zigzag_i64(value),
                            &fields[i].name,
                        )?),
                        ProtoFieldType::SInt64 => ProtoValue::Int64(decode_zigzag_i64(value)),
                        _ => ProtoValue::Int64(value as i64),
                    };
                    saw_field = true;
                }
            }
            prost::encoding::WireType::SixtyFourBit => {
                if buf.remaining() < 8 {
                    return Err(SchemaRegistryError::Decode(
                        "truncated protobuf 64-bit field".into(),
                    ));
                }
                let bytes = buf.get_u64_le();
                if let Some(i) = idx {
                    current_row[i] = match fields[i].field_type {
                        ProtoFieldType::Float64 => ProtoValue::Float64(f64::from_bits(bytes)),
                        ProtoFieldType::Fixed64 => ProtoValue::UInt64(bytes),
                        ProtoFieldType::SFixed64 => ProtoValue::Int64(bytes as i64),
                        _ => {
                            return Err(SchemaRegistryError::Decode(format!(
                                "unsupported protobuf 64-bit field type for '{}'",
                                fields[i].name
                            )));
                        }
                    };
                    saw_field = true;
                }
            }
            prost::encoding::WireType::LengthDelimited => {
                let len = prost::encoding::decode_varint(&mut buf)
                    .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?
                    as usize;
                if buf.remaining() < len {
                    return Err(SchemaRegistryError::Decode(
                        "truncated protobuf length-delimited field".into(),
                    ));
                }
                let chunk = buf.copy_to_bytes(len);
                if let Some(i) = idx {
                    current_row[i] = match fields[i].field_type {
                        ProtoFieldType::Bytes => ProtoValue::Bytes(chunk.to_vec()),
                        _ => {
                            ProtoValue::String(String::from_utf8(chunk.to_vec()).map_err(|e| {
                                SchemaRegistryError::Decode(format!(
                                    "protobuf string field is not UTF-8: {e}"
                                ))
                            })?)
                        }
                    };
                    saw_field = true;
                }
            }
            prost::encoding::WireType::ThirtyTwoBit => {
                if buf.remaining() < 4 {
                    return Err(SchemaRegistryError::Decode(
                        "truncated protobuf 32-bit field".into(),
                    ));
                }
                let bytes = buf.get_u32_le();
                if let Some(i) = idx {
                    current_row[i] = match fields[i].field_type {
                        ProtoFieldType::Float32 => ProtoValue::Float32(f32::from_bits(bytes)),
                        ProtoFieldType::Fixed32 => ProtoValue::UInt32(bytes),
                        ProtoFieldType::SFixed32 => ProtoValue::Int32(bytes as i32),
                        _ => {
                            return Err(SchemaRegistryError::Decode(format!(
                                "unsupported protobuf 32-bit field type for '{}'",
                                fields[i].name
                            )));
                        }
                    };
                    saw_field = true;
                }
            }
            _ => {
                prost::encoding::skip_field(
                    wire_type,
                    field_number,
                    &mut buf,
                    prost::encoding::DecodeContext::default(),
                )
                .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
            }
        }
    }

    if !saw_field && saw_wire_field {
        return Err(SchemaRegistryError::Decode(
            "protobuf payload did not contain any fields declared by the schema".into(),
        ));
    }
    for (field, value) in fields.iter().zip(&current_row) {
        if field.presence == ProtoPresence::Required && matches!(value, ProtoValue::Null) {
            return Err(SchemaRegistryError::Decode(format!(
                "protobuf payload omitted required field '{}'",
                field.name
            )));
        }
    }
    Ok(vec![current_row])
}

fn default_proto_value(field_type: ProtoFieldType) -> ProtoValue {
    match field_type {
        ProtoFieldType::Int32 | ProtoFieldType::SInt32 | ProtoFieldType::SFixed32 => {
            ProtoValue::Int32(0)
        }
        ProtoFieldType::Int64 | ProtoFieldType::SInt64 | ProtoFieldType::SFixed64 => {
            ProtoValue::Int64(0)
        }
        ProtoFieldType::UInt32 | ProtoFieldType::Fixed32 => ProtoValue::UInt32(0),
        ProtoFieldType::UInt64 | ProtoFieldType::Fixed64 => ProtoValue::UInt64(0),
        ProtoFieldType::Bool => ProtoValue::Bool(false),
        ProtoFieldType::Float32 => ProtoValue::Float32(0.0),
        ProtoFieldType::Float64 => ProtoValue::Float64(0.0),
        ProtoFieldType::String => ProtoValue::String(String::new()),
        ProtoFieldType::Bytes => ProtoValue::Bytes(Vec::new()),
    }
}

fn checked_proto_i32(value: i64, field_name: &str) -> SchemaRegistryResult<i32> {
    i32::try_from(value).map_err(|_| {
        SchemaRegistryError::Decode(format!("protobuf int32 field '{field_name}' overflowed"))
    })
}

fn prost_wire_type_id(wire_type: prost::encoding::WireType) -> Option<u8> {
    match wire_type {
        prost::encoding::WireType::Varint => Some(0),
        prost::encoding::WireType::SixtyFourBit => Some(1),
        prost::encoding::WireType::LengthDelimited => Some(2),
        prost::encoding::WireType::StartGroup => Some(3),
        prost::encoding::WireType::EndGroup => Some(4),
        prost::encoding::WireType::ThirtyTwoBit => Some(5),
    }
}

/// Protobuf wire format values.
#[derive(Debug, Clone)]
enum ProtoValue {
    Null,
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    UInt64(u64),
    Bool(bool),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
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
        .collect::<SchemaRegistryResult<_>>()?;

    let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), arrays)
        .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;

    Ok(vec![batch])
}

/// Convert protobuf values into an Arrow column array.
fn proto_values_to_column(
    values: &[&ProtoValue],
    data_type: &DataType,
) -> SchemaRegistryResult<ArrayRef> {
    match data_type {
        DataType::Int32 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::Int32(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::Int64(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::UInt32 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::UInt32(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(UInt32Array::from(values)))
        }
        DataType::UInt64 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::UInt64(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(UInt64Array::from(values)))
        }
        DataType::Float64 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::Float64(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        DataType::Float32 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::Float32(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(Float32Array::from(values)))
        }
        DataType::Boolean => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::Bool(value) => Ok(Some(*value)),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
        DataType::Binary => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::Bytes(value) => Ok(Some(value.as_slice())),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(BinaryArray::from(values)))
        }
        DataType::Utf8 => {
            let values = values
                .iter()
                .map(|value| match value {
                    ProtoValue::Null => Ok(None),
                    ProtoValue::String(value) => Ok(Some(value.clone())),
                    other => Err(proto_value_mismatch(data_type, other)),
                })
                .collect::<SchemaRegistryResult<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(values)))
        }
        unsupported => Err(SchemaRegistryError::Decode(format!(
            "unsupported Arrow type for protobuf conversion: {unsupported}"
        ))),
    }
}

fn proto_value_mismatch(expected: &DataType, actual: &ProtoValue) -> SchemaRegistryError {
    SchemaRegistryError::Decode(format!(
        "protobuf value {actual:?} does not match Arrow type {expected}"
    ))
}

/// Convert an Avro schema to an Arrow schema.
fn avro_schema_to_arrow_schema(
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
fn avro_schema_to_data_type(
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
fn avro_values_to_column(
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

fn avro_value_mismatch(expected: &DataType, actual: &Value) -> SchemaRegistryError {
    SchemaRegistryError::Decode(format!(
        "Avro value {actual:?} does not match Arrow type {expected}"
    ))
}

impl SchemaRegistryClient {
    /// Decode a Kafka payload using the explicitly configured format.
    pub async fn decode_with_format(
        &self,
        payload: &[u8],
        format: RegistryFormat,
    ) -> SchemaRegistryResult<(
        arrow::datatypes::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
    )> {
        match format {
            RegistryFormat::Avro => {
                AvroDeserializer {
                    client: self.clone(),
                }
                .decode(payload)
                .await
            }
            RegistryFormat::Protobuf => {
                ProtobufDeserializer {
                    client: self.clone(),
                }
                .decode(payload)
                .await
            }
        }
    }
}

pub fn deserializer_for(
    config: &SchemaRegistryConfig,
) -> SchemaRegistryResult<Arc<dyn KafkaDeserializer>> {
    match config.format {
        RegistryFormat::Avro => Ok(Arc::new(AvroDeserializer::new(config)?)),
        RegistryFormat::Protobuf => Ok(Arc::new(ProtobufDeserializer::new(config)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
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
