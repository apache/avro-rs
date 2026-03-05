// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{io::Write, marker::PhantomData, ops::RangeInclusive};

use serde::Serialize;

use crate::encode::encode_internal;
use crate::serde::ser_schema::SchemaAwareWriteSerializer;
use crate::{
    AvroResult, AvroSchema, Schema,
    error::Details,
    headers::{HeaderBuilder, RabinFingerprintHeader},
    schema::ResolvedOwnedSchema,
    types::Value,
};

/// Writer that encodes messages according to the single object encoding v1 spec
/// Uses an API similar to the current File Writer
/// Writes all object bytes at once, and drains internal buffer
pub struct GenericSingleObjectWriter {
    buffer: Vec<u8>,
    resolved: ResolvedOwnedSchema,
}

impl GenericSingleObjectWriter {
    pub fn new_with_capacity(
        schema: &Schema,
        initial_buffer_cap: usize,
    ) -> AvroResult<GenericSingleObjectWriter> {
        let header_builder = RabinFingerprintHeader::from_schema(schema);
        Self::new_with_capacity_and_header_builder(schema, initial_buffer_cap, header_builder)
    }

    pub fn new_with_capacity_and_header_builder<HB: HeaderBuilder>(
        schema: &Schema,
        initial_buffer_cap: usize,
        header_builder: HB,
    ) -> AvroResult<GenericSingleObjectWriter> {
        let mut buffer = Vec::with_capacity(initial_buffer_cap);
        let header = header_builder.build_header();
        buffer.extend_from_slice(&header);

        Ok(GenericSingleObjectWriter {
            buffer,
            resolved: ResolvedOwnedSchema::try_from(schema.clone())?,
        })
    }

    const HEADER_LENGTH_RANGE: RangeInclusive<usize> = 10_usize..=20_usize;

    /// Write the referenced Value to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value_ref<W: Write>(&mut self, v: &Value, writer: &mut W) -> AvroResult<usize> {
        let original_length = self.buffer.len();
        if !Self::HEADER_LENGTH_RANGE.contains(&original_length) {
            Err(Details::IllegalSingleObjectWriterState.into())
        } else {
            write_value_ref_owned_resolved(&self.resolved, v, &mut self.buffer)?;
            writer
                .write_all(&self.buffer)
                .map_err(Details::WriteBytes)?;
            let len = self.buffer.len();
            self.buffer.truncate(original_length);
            Ok(len)
        }
    }

    /// Write the Value to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value<W: Write>(&mut self, v: Value, writer: &mut W) -> AvroResult<usize> {
        self.write_value_ref(&v, writer)
    }
}

/// Writer that encodes messages according to the single object encoding v1 spec
pub struct SpecificSingleObjectWriter<T>
where
    T: AvroSchema,
{
    resolved: ResolvedOwnedSchema,
    header: Vec<u8>,
    _model: PhantomData<T>,
}

impl<T> SpecificSingleObjectWriter<T>
where
    T: AvroSchema,
{
    pub fn new() -> AvroResult<Self> {
        let schema = T::get_schema();
        let header = RabinFingerprintHeader::from_schema(&schema).build_header();
        let resolved = ResolvedOwnedSchema::new(schema)?;
        // We don't use Self::new_with_header_builder as that would mean calling T::get_schema() twice
        Ok(Self {
            resolved,
            header,
            _model: PhantomData,
        })
    }

    pub fn new_with_header_builder(header_builder: impl HeaderBuilder) -> AvroResult<Self> {
        let header = header_builder.build_header();
        let resolved = ResolvedOwnedSchema::new(T::get_schema())?;
        Ok(Self {
            resolved,
            header,
            _model: PhantomData,
        })
    }

    /// Deprecated. Use [`SpecificSingleObjectWriter::new`] instead.
    #[deprecated(since = "0.22.0", note = "Use new() instead")]
    pub fn with_capacity(_buffer_cap: usize) -> AvroResult<Self> {
        Self::new()
    }
}

impl<T> SpecificSingleObjectWriter<T>
where
    T: AvroSchema + Into<Value>,
{
    /// Write the value to the writer
    ///
    /// Returns the number of bytes written.
    ///
    /// Each call writes a complete single-object encoded message (header + data),
    /// making each message independently decodable.
    pub fn write_value<W: Write>(&self, data: T, writer: &mut W) -> AvroResult<usize> {
        writer
            .write_all(&self.header)
            .map_err(Details::WriteBytes)?;
        let value: Value = data.into();
        let bytes = write_value_ref_owned_resolved(&self.resolved, &value, writer)?;
        Ok(bytes + self.header.len())
    }
}

impl<T> SpecificSingleObjectWriter<T>
where
    T: AvroSchema + Serialize,
{
    /// Write the object to the writer.
    ///
    /// Returns the number of bytes written.
    ///
    /// Each call writes a complete single-object encoded message (header + data),
    /// making each message independently decodable.
    pub fn write_ref<W: Write>(&self, data: &T, writer: &mut W) -> AvroResult<usize> {
        writer
            .write_all(&self.header)
            .map_err(Details::WriteBytes)?;

        let mut serializer = SchemaAwareWriteSerializer::new(
            writer,
            self.resolved.get_root_schema(),
            self.resolved.get_names(),
            None,
        );
        let bytes = data.serialize(&mut serializer)?;

        Ok(bytes + self.header.len())
    }

    /// Write the object to the writer.
    ///
    /// Returns the number of bytes written.
    ///
    /// Each call writes a complete single-object encoded message (header + data),
    /// making each message independently decodable.
    pub fn write<W: Write>(&self, data: T, writer: &mut W) -> AvroResult<usize> {
        self.write_ref(&data, writer)
    }
}

fn write_value_ref_owned_resolved<W: Write>(
    resolved_schema: &ResolvedOwnedSchema,
    value: &Value,
    writer: &mut W,
) -> AvroResult<usize> {
    let root_schema = resolved_schema.get_root_schema();
    if let Some(reason) = value.validate_internal(
        root_schema,
        resolved_schema.get_names(),
        root_schema.namespace(),
    ) {
        return Err(Details::ValidationWithReason {
            value: value.clone(),
            schema: root_schema.clone(),
            reason,
        }
        .into());
    }
    encode_internal(
        value,
        root_schema,
        resolved_schema.get_names(),
        root_schema.namespace(),
        writer,
    )
}

#[cfg(test)]
mod tests {
    use apache_avro_test_helper::TestResult;
    use uuid::Uuid;

    use crate::{encode::encode, headers::GlueSchemaUuidHeader, rabin::Rabin};

    use super::*;

    #[derive(Serialize, Clone)]
    struct TestSingleObjectWriter {
        a: i64,
        b: f64,
        c: Vec<String>,
    }

    impl AvroSchema for TestSingleObjectWriter {
        fn get_schema() -> Schema {
            let schema = r#"
            {
                "type":"record",
                "name":"TestSingleObjectWrtierSerialize",
                "fields":[
                    {
                        "name":"a",
                        "type":"long"
                    },
                    {
                        "name":"b",
                        "type":"double"
                    },
                    {
                        "name":"c",
                        "type":{
                            "type":"array",
                            "items":"string"
                        }
                    }
                ]
            }
            "#;
            Schema::parse_str(schema).unwrap()
        }
    }

    impl From<TestSingleObjectWriter> for Value {
        fn from(obj: TestSingleObjectWriter) -> Value {
            Value::Record(vec![
                ("a".into(), obj.a.into()),
                ("b".into(), obj.b.into()),
                (
                    "c".into(),
                    Value::Array(obj.c.into_iter().map(|s| s.into()).collect()),
                ),
            ])
        }
    }

    #[test]
    fn test_single_object_writer() -> TestResult {
        let mut buf: Vec<u8> = Vec::new();
        let obj = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };
        let mut writer = GenericSingleObjectWriter::new_with_capacity(
            &TestSingleObjectWriter::get_schema(),
            1024,
        )
        .expect("Should resolve schema");
        let value = obj.into();
        let written_bytes = writer
            .write_value_ref(&value, &mut buf)
            .expect("Error serializing properly");

        assert!(buf.len() > 10, "no bytes written");
        assert_eq!(buf.len(), written_bytes);
        assert_eq!(buf[0], 0xC3);
        assert_eq!(buf[1], 0x01);
        assert_eq!(
            &buf[2..10],
            &TestSingleObjectWriter::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..]
        );
        let mut msg_binary = Vec::new();
        encode(
            &value,
            &TestSingleObjectWriter::get_schema(),
            &mut msg_binary,
        )
        .expect("encode should have failed by here as a dependency of any writing");
        assert_eq!(&buf[10..], &msg_binary[..]);

        Ok(())
    }

    #[test]
    fn test_single_object_writer_with_header_builder() -> TestResult {
        let mut buf: Vec<u8> = Vec::new();
        let obj = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };
        let schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        let header_builder = GlueSchemaUuidHeader::from_uuid(schema_uuid);
        let mut writer = GenericSingleObjectWriter::new_with_capacity_and_header_builder(
            &TestSingleObjectWriter::get_schema(),
            1024,
            header_builder,
        )
        .expect("Should resolve schema");
        let value = obj.into();
        writer
            .write_value_ref(&value, &mut buf)
            .expect("Error serializing properly");

        assert_eq!(buf[0], 0x03);
        assert_eq!(buf[1], 0x00);
        assert_eq!(buf[2..18], schema_uuid.into_bytes()[..]);
        Ok(())
    }

    #[test]
    fn test_writer_parity() -> TestResult {
        let obj1 = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };

        let mut buf1: Vec<u8> = Vec::new();
        let mut buf2: Vec<u8> = Vec::new();
        let mut buf3: Vec<u8> = Vec::new();
        let mut buf4: Vec<u8> = Vec::new();

        let mut generic_writer = GenericSingleObjectWriter::new_with_capacity(
            &TestSingleObjectWriter::get_schema(),
            1024,
        )
        .expect("Should resolve schema");
        let specific_writer = SpecificSingleObjectWriter::<TestSingleObjectWriter>::new()
            .expect("Resolved should pass");
        specific_writer
            .write_ref(&obj1, &mut buf1)
            .expect("Serialization expected");
        specific_writer
            .write_ref(&obj1, &mut buf2)
            .expect("Serialization expected");
        specific_writer
            .write_value(obj1.clone(), &mut buf3)
            .expect("Serialization expected");

        generic_writer
            .write_value(obj1.into(), &mut buf4)
            .expect("Serialization expected");

        assert_eq!(buf1, buf2);
        assert_eq!(buf2, buf3);
        assert_eq!(buf3, buf4);

        Ok(())
    }

    #[test]
    fn avro_rs_439_specific_single_object_writer_ref() -> TestResult {
        #[derive(Serialize)]
        struct Recursive {
            field: bool,
            recurse: Option<Box<Recursive>>,
        }

        impl AvroSchema for Recursive {
            fn get_schema() -> Schema {
                Schema::parse_str(
                    r#"{
                    "name": "Recursive",
                    "type": "record",
                    "fields": [
                        { "name": "field", "type": "boolean" },
                        { "name": "recurse", "type": ["null", "Recursive"] }
                    ]
                }"#,
                )
                .unwrap()
            }
        }

        let mut buffer = Vec::new();
        let writer = SpecificSingleObjectWriter::new()?;

        writer.write(
            Recursive {
                field: true,
                recurse: Some(Box::new(Recursive {
                    field: false,
                    recurse: None,
                })),
            },
            &mut buffer,
        )?;
        assert_eq!(
            buffer,
            &[195, 1, 83, 223, 43, 26, 181, 179, 227, 224, 1, 2, 0, 0][..]
        );

        Ok(())
    }
}
