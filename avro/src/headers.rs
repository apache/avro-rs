use uuid::Uuid;

use crate::{rabin::Rabin, schema::SchemaFingerprint, AvroResult, Schema};

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

impl GlueSchemaUuidHeader {
    pub fn create_from_uuid(schema_uuid: Uuid) -> Self {
        GlueSchemaUuidHeader { schema_uuid }
    }

    pub fn parse_from_raw_avro(message_payload: &[u8]) -> AvroResult<Self> {
        if message_payload.len() < 19 {
            return Err(crate::error::Error::HeaderMagic);
        }
        // The only possible error is invalid length, and we just checked the length,
        // so unwrap is safe
        let schema_uuid = Uuid::from_slice(&message_payload[2..18]).unwrap();
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

#[cfg(test)]
mod test {
    use super::*;
    use apache_avro_test_helper::TestResult;

    #[test]
    fn test_rabin_fingerprint_header() -> TestResult {
        let schema_str = r#"
            {
            "type": "record",
            "name": "test",
            "fields": [
                {
                "name": "a",
                "type": "long",
                "default": 42
                },
                {
                "name": "b",
                "type": "string"
                }
            ]
            }
            "#;
        let schema = Schema::parse_str(schema_str)?;
        let header_builder = RabinFingerprintHeader::create_from_schema(&schema);
        let computed_header = header_builder.build_header();
        let expected_header: Vec<u8> = vec![195, 1, 232, 198, 194, 12, 97, 95, 44, 71];
        assert_eq!(computed_header, expected_header);
        Ok(())
    }

    #[test]
    fn test_glue_schema_header() -> TestResult {
        let schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        let header_builder = GlueSchemaUuidHeader::create_from_uuid(schema_uuid);
        let computed_header = header_builder.build_header();
        let expected_header: Vec<u8> = vec![
            3, 0, 178, 241, 207, 0, 4, 52, 1, 62, 67, 154, 18, 94, 184, 72, 90, 95,
        ];
        assert_eq!(computed_header, expected_header);
        Ok(())
    }

    #[test]
    fn test_glue_header_parse() -> TestResult {
        let incoming_message: Vec<u8> = vec![
            3, 0, 178, 241, 207, 0, 4, 52, 1, 62, 67, 154, 18, 94, 184, 72, 90, 95, 65, 65, 65,
        ];
        let header_builder = GlueSchemaUuidHeader::parse_from_raw_avro(&incoming_message)?;
        let expected_schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        assert_eq!(header_builder.schema_uuid(), expected_schema_uuid);
        Ok(())
    }

    #[test]
    fn test_glue_header_parse_err_on_message_too_short() -> TestResult {
        let incoming_message: Vec<u8> = vec![3, 0, 178, 241, 207, 0, 4, 52, 1];
        let header_builder = GlueSchemaUuidHeader::parse_from_raw_avro(&incoming_message);
        assert!(matches!(
            header_builder,
            Err(crate::error::Error::HeaderMagic)
        ));
        Ok(())
    }
}
