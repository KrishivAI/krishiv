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

