use uuid::Uuid;

use crate::{rabin::Rabin, schema::SchemaFingerprint, Schema};

pub trait HeaderBuilder {
    fn build_header(&self) -> Vec<u8>;
}

pub struct RabinFingerprintHeader {
    fingerprint: SchemaFingerprint,
}

impl RabinFingerprintHeader {
    pub fn create_from_schema(schema: &Schema) -> Self {
        let fingerprint = schema.fingerprint::<Rabin>();
        RabinFingerprintHeader { fingerprint }
    }
}

impl HeaderBuilder for RabinFingerprintHeader {
    fn build_header(&self) -> Vec<u8> {
        vec![
            0xC3,
            0x01,
            self.fingerprint.bytes[0],
            self.fingerprint.bytes[1],
            self.fingerprint.bytes[2],
            self.fingerprint.bytes[3],
            self.fingerprint.bytes[4],
            self.fingerprint.bytes[5],
            self.fingerprint.bytes[6],
            self.fingerprint.bytes[7],
        ]
    }
}

pub struct GlueSchemaUuidHeader {
    schema_uuid: Uuid,
}

#[derive(thiserror::Error, Debug)]
pub enum GlueHeaderError {
    #[error("Message is too short to have a valid Glue header")]
    MessageTooShort,
    #[error("Failed to parse a valid UUID from the header: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

impl GlueSchemaUuidHeader {
    pub fn create_from_uuid(schema_uuid: Uuid) -> Self {
        GlueSchemaUuidHeader { schema_uuid }
    }

    pub fn parse_from_raw_avro(message_payload: &[u8]) -> Result<Self, GlueHeaderError> {
        if message_payload.len() < 19 {
            return Err(GlueHeaderError::MessageTooShort);
        }
        let schema_uuid = Uuid::from_slice(&message_payload[2..18])?;
        Ok(GlueSchemaUuidHeader { schema_uuid })
    }

    pub fn schema_uuid(&self) -> Uuid {
        self.schema_uuid
    }
}

impl HeaderBuilder for GlueSchemaUuidHeader {
    fn build_header(&self) -> Vec<u8> {
        let mut output_vec: Vec<u8> = vec![3, 0];
        output_vec.extend_from_slice(self.schema_uuid.as_bytes());
        output_vec
    }
}
