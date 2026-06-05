#![forbid(unsafe_code)]
//! Confluent Schema Registry deserialization for Kafka payloads (R18 S3.3).

extern crate alloc;

use std::sync::Arc;

use apache_avro::from_avro_datum;
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
            // Apply explicit connect + per-request timeouts. Without these, a
            // misbehaving registry (or a MITM holding the TCP connection
            // open) can stall every Kafka payload decode indefinitely.
            // The 5s connect / 10s request budget matches the Flight client
            // defaults and is short enough that a caller retrying the batch
            // can fail over to a backup registry within a few seconds.
            http: Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| Client::new()),
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

        decode_avro_datum_payload(&avro_schema, &payload[5..])
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
        let message_payload = strip_confluent_protobuf_message_indexes(&payload[5..])?;
        let records = decode_protobuf_wire(message_payload, &proto_schema)?;
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
    field_type: ProtoFieldType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtoFieldType {
    Int64,
    SInt64,
    Bool,
    Float32,
    Float64,
    String,
    Bytes,
}

/// Parse a minimal protobuf schema definition from JSON or simple text format.
///
/// Accepts JSON format: `[{"name":"field","wire_type":2,"field_number":1}, ...]`
/// where wire_type maps to protobuf wire types: 0=varint, 1=64-bit, 2=length-delimited.
///
/// Also accepts ordinary `.proto` text and extracts scalar fields from the
/// first top-level `message` declaration.
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
                field_type: proto_field_type_from_wire_type(f.wire_type),
            })
            .collect());
    }
    parse_proto_text_schema(trimmed)
}

#[derive(serde::Deserialize)]
struct ProtoFieldJson {
    name: String,
    wire_type: u8,
    field_number: u32,
}

fn proto_field_type_from_wire_type(wire_type: u8) -> ProtoFieldType {
    match wire_type {
        1 => ProtoFieldType::Float64,
        2 => ProtoFieldType::String,
        5 => ProtoFieldType::Float32,
        _ => ProtoFieldType::Int64,
    }
}

fn parse_proto_text_schema(schema_str: &str) -> SchemaRegistryResult<Vec<ProtoField>> {
    let mut fields = Vec::new();
    let mut in_first_message = false;
    let mut depth = 0i32;

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
            if line.starts_with("message ") && line.contains('{') {
                in_first_message = true;
                depth += opens - closes;
            }
            continue;
        }

        if depth == 1 && line.ends_with(';') {
            if let Some(field) = parse_proto_text_field(line)? {
                fields.push(field);
            }
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

fn parse_proto_text_field(line: &str) -> SchemaRegistryResult<Option<ProtoField>> {
    let statement = line.trim_end_matches(';').trim();
    let skip_prefixes = [
        "option ",
        "reserved ",
        "extensions ",
        "oneof ",
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
    if matches!(
        tokens.first().copied(),
        Some("optional" | "required" | "repeated")
    ) {
        tokens.remove(0);
    }
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
    }))
}

fn proto_scalar_type(proto_type: &str) -> Option<(u8, ProtoFieldType)> {
    match proto_type {
        "double" => Some((1, ProtoFieldType::Float64)),
        "float" => Some((5, ProtoFieldType::Float32)),
        "int32" | "int64" | "uint32" | "uint64" => Some((0, ProtoFieldType::Int64)),
        "fixed32" | "sfixed32" => Some((5, ProtoFieldType::Int64)),
        "fixed64" | "sfixed64" => Some((1, ProtoFieldType::Int64)),
        "sint32" | "sint64" => Some((0, ProtoFieldType::SInt64)),
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
                ProtoFieldType::Int64 | ProtoFieldType::SInt64 => DataType::Int64,
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
    for _ in 0..len {
        let _ = prost::encoding::decode_varint(&mut buf)
            .map_err(|e| SchemaRegistryError::Decode(e.to_string()))?;
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
    let arrow_schema = avro_schema_to_arrow_schema(avro_schema);
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
    let mut current_row: Vec<ProtoValue> = vec![ProtoValue::Null; fields.len()];
    let mut saw_field = false;
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
                        _ => ProtoValue::Int64(bytes as i64),
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
                        _ => ProtoValue::Int64(bytes as i64),
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

    if saw_field || !fields.is_empty() {
        Ok(vec![current_row])
    } else {
        Ok(Vec::new())
    }
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
    Int64(i64),
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
                    ProtoValue::Float32(f) => Some(*f as f64),
                    ProtoValue::Int64(i) => Some(*i as f64),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Float32 => {
            let arr: Float32Array = values
                .iter()
                .map(|v| match v {
                    ProtoValue::Float32(f) => Some(*f),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| match v {
                    ProtoValue::Bool(b) => Some(*b),
                    _ => None,
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Binary => {
            let arr: BinaryArray = values
                .iter()
                .map(|v| match v {
                    ProtoValue::Bytes(bytes) => Some(bytes.as_slice()),
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
                    ProtoValue::Bool(b) => Some(format!("{b}")),
                    ProtoValue::Float32(f) => Some(format!("{f}")),
                    ProtoValue::Float64(f) => Some(format!("{f}")),
                    ProtoValue::Bytes(bytes) => Some(format!("{bytes:?}")),
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
        DataType::Utf8 => {
            let strings: Vec<Option<String>> = values
                .iter()
                .map(|v| match unwrap_value(v) {
                    Value::String(s) => Some(s.clone()),
                    Value::Enum(_, s) => Some(s.clone()),
                    Value::Null => None,
                    other => Some(format!("{other:?}")),
                })
                .collect();
            let opts: Vec<Option<&str>> = strings.iter().map(|s| s.as_deref()).collect();
            Arc::new(StringArray::from(opts))
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
