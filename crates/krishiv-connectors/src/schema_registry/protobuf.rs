use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use super::client::SchemaRegistryClient;
use super::{KafkaDeserializer, SchemaRegistryConfig, SchemaRegistryError, SchemaRegistryResult};

/// Protobuf deserializer using Confluent wire format.
///
/// Decodes protobuf-encoded Kafka payloads using the Confluent wire format:
/// `[magic byte 0x00][4-byte BE schema id][protobuf payload]`.
///
/// The schema is fetched from the Confluent Schema Registry and used to
/// decode the protobuf wire format into Arrow `RecordBatch` columns.
pub struct ProtobufDeserializer {
    pub(crate) client: SchemaRegistryClient,
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
pub(crate) struct ProtoField {
    pub(crate) name: String,
    wire_type: u8,
    pub(crate) field_number: u32,
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
pub(crate) fn parse_proto_schema(schema_str: &str) -> SchemaRegistryResult<Vec<ProtoField>> {
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
pub(crate) fn proto_fields_to_arrow_schema(fields: &[ProtoField]) -> SchemaRef {
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

pub(crate) fn strip_confluent_protobuf_message_indexes(data: &[u8]) -> SchemaRegistryResult<&[u8]> {
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

/// Decode protobuf wire format (field-tag + value pairs) into a Vec of rows.
/// Uses `prost` encoding primitives and the field list as a descriptor set.
pub(crate) fn decode_protobuf_wire(
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
pub(crate) enum ProtoValue {
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
pub(crate) fn proto_records_to_batches(
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
pub(crate) fn proto_values_to_column(
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
