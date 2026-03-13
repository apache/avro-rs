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

use std::{io::Read, marker::PhantomData};

use bon::bon;
use serde::de::DeserializeOwned;

use crate::{
    AvroResult, AvroSchema, Schema,
    decode::decode_internal,
    error::Details,
    headers::{HeaderBuilder, RabinFingerprintHeader},
    schema::ResolvedOwnedSchema,
    serde::deser_schema::{Config, SchemaAwareDeserializer},
    types::Value,
    util::is_human_readable,
};

pub struct GenericSingleObjectReader {
    write_schema: ResolvedOwnedSchema,
    expected_header: Vec<u8>,
    human_readable: bool,
}

#[bon]
impl GenericSingleObjectReader {
    #[builder]
    pub fn new(
        schema: Schema,
        /// The expected header.
        #[builder(default = RabinFingerprintHeader::from_schema(&schema).build_header())]
        header: Vec<u8>,
        /// Was the data serialized with `human_readable`.
        #[builder(default = is_human_readable())]
        human_readable: bool,
    ) -> AvroResult<GenericSingleObjectReader> {
        Ok(Self {
            write_schema: schema.try_into()?,
            expected_header: header,
            human_readable,
        })
    }
}

impl GenericSingleObjectReader {
    pub fn read_value<R: Read>(&self, reader: &mut R) -> AvroResult<Value> {
        let mut header = vec![0; self.expected_header.len()];
        match reader.read_exact(&mut header) {
            Ok(_) => {
                if self.expected_header == header {
                    decode_internal(
                        self.write_schema.get_root_schema(),
                        self.write_schema.get_names(),
                        None,
                        reader,
                    )
                } else {
                    Err(
                        Details::SingleObjectHeaderMismatch(self.expected_header.clone(), header)
                            .into(),
                    )
                }
            }
            Err(io_error) => Err(Details::ReadHeader(io_error).into()),
        }
    }

    pub fn read_deser<T: DeserializeOwned>(&self, reader: &mut impl Read) -> AvroResult<T> {
        let mut header = vec![0; self.expected_header.len()];
        reader
            .read_exact(&mut header)
            .map_err(Details::ReadHeader)?;
        if self.expected_header == header {
            let config = Config {
                names: self.write_schema.get_names(),
                human_readable: self.human_readable,
            };
            T::deserialize(SchemaAwareDeserializer::new(
                reader,
                self.write_schema.get_root_schema(),
                config,
            )?)
        } else {
            Err(Details::SingleObjectHeaderMismatch(self.expected_header.clone(), header).into())
        }
    }
}

pub struct SpecificSingleObjectReader<T>
where
    T: AvroSchema,
{
    inner: GenericSingleObjectReader,
    _model: PhantomData<T>,
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema,
{
    pub fn new() -> AvroResult<SpecificSingleObjectReader<T>> {
        Ok(SpecificSingleObjectReader {
            inner: GenericSingleObjectReader::builder()
                .schema(T::get_schema())
                .build()?,
            _model: PhantomData,
        })
    }
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema + From<Value>,
{
    pub fn read_from_value<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        self.inner.read_value(reader).map(|v| v.into())
    }
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema + DeserializeOwned,
{
    pub fn read<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        self.inner.read_deser(reader)
    }
}

#[cfg(test)]
mod tests {
    use apache_avro_test_helper::TestResult;
    use serde::Deserialize;
    use uuid::Uuid;

    use super::*;
    use crate::{
        AvroSchema, Error, Schema, encode::encode, headers::GlueSchemaUuidHeader, rabin::Rabin,
    };

    #[derive(Deserialize, Clone, PartialEq, Debug)]
    struct TestSingleObjectReader {
        a: i64,
        b: f64,
        c: Vec<String>,
    }

    impl AvroSchema for TestSingleObjectReader {
        fn get_schema() -> Schema {
            let schema = r#"
            {
                "type":"record",
                "name":"TestSingleObjectReader",
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

    impl From<Value> for TestSingleObjectReader {
        fn from(obj: Value) -> TestSingleObjectReader {
            if let Value::Record(fields) = obj {
                let mut a = None;
                let mut b = None;
                let mut c = vec![];
                for (field_name, v) in fields {
                    match (field_name.as_str(), v) {
                        ("a", Value::Long(i)) => a = Some(i),
                        ("b", Value::Double(d)) => b = Some(d),
                        ("c", Value::Array(v)) => {
                            for inner_val in v {
                                if let Value::String(s) = inner_val {
                                    c.push(s);
                                }
                            }
                        }
                        (key, value) => panic!("Unexpected pair: {key:?} -> {value:?}"),
                    }
                }
                TestSingleObjectReader {
                    a: a.unwrap(),
                    b: b.unwrap(),
                    c,
                }
            } else {
                panic!("Expected a Value::Record but was {obj:?}")
            }
        }
    }

    impl From<TestSingleObjectReader> for Value {
        fn from(obj: TestSingleObjectReader) -> Value {
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
    fn test_avro_3507_single_object_reader() -> TestResult {
        let obj = TestSingleObjectReader {
            a: 42,
            b: 3.33,
            c: vec!["cat".into(), "dog".into()],
        };
        let mut to_read = Vec::<u8>::new();
        to_read.extend_from_slice(&[0xC3, 0x01]);
        to_read.extend_from_slice(
            &TestSingleObjectReader::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..],
        );
        encode(
            &obj.clone().into(),
            &TestSingleObjectReader::get_schema(),
            &mut to_read,
        )
        .expect("Encode should succeed");
        let mut to_read = &to_read[..];
        let generic_reader = GenericSingleObjectReader::builder()
            .schema(TestSingleObjectReader::get_schema())
            .build()
            .expect("Schema should resolve");
        let val = generic_reader
            .read_value(&mut to_read)
            .expect("Should read");
        let expected_value: Value = obj.into();
        pretty_assertions::assert_eq!(expected_value, val);

        Ok(())
    }

    #[test]
    fn avro_3642_test_single_object_reader_incomplete_reads() -> TestResult {
        let obj = TestSingleObjectReader {
            a: 42,
            b: 3.33,
            c: vec!["cat".into(), "dog".into()],
        };
        // The two-byte marker, to show that the message uses this single-record format
        let to_read_1 = [0xC3, 0x01];
        let mut to_read_2 = Vec::<u8>::new();
        to_read_2.extend_from_slice(
            &TestSingleObjectReader::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..],
        );
        let mut to_read_3 = Vec::<u8>::new();
        encode(
            &obj.clone().into(),
            &TestSingleObjectReader::get_schema(),
            &mut to_read_3,
        )
        .expect("Encode should succeed");
        let mut to_read = (&to_read_1[..]).chain(&to_read_2[..]).chain(&to_read_3[..]);
        let generic_reader = GenericSingleObjectReader::builder()
            .schema(TestSingleObjectReader::get_schema())
            .build()
            .expect("Schema should resolve");
        let val = generic_reader
            .read_value(&mut to_read)
            .expect("Should read");
        let expected_value: Value = obj.into();
        pretty_assertions::assert_eq!(expected_value, val);

        Ok(())
    }

    #[test]
    fn test_avro_3507_reader_parity() -> TestResult {
        let obj = TestSingleObjectReader {
            a: 42,
            b: 3.33,
            c: vec!["cat".into(), "dog".into()],
        };

        let mut to_read = Vec::<u8>::new();
        to_read.extend_from_slice(&[0xC3, 0x01]);
        to_read.extend_from_slice(
            &TestSingleObjectReader::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..],
        );
        encode(
            &obj.clone().into(),
            &TestSingleObjectReader::get_schema(),
            &mut to_read,
        )
        .expect("Encode should succeed");
        let generic_reader = GenericSingleObjectReader::builder()
            .schema(TestSingleObjectReader::get_schema())
            .build()
            .expect("Schema should resolve");
        let specific_reader = SpecificSingleObjectReader::<TestSingleObjectReader>::new()
            .expect("schema should resolve");
        let mut to_read1 = &to_read[..];
        let mut to_read2 = &to_read[..];
        let mut to_read3 = &to_read[..];

        let val = generic_reader
            .read_value(&mut to_read1)
            .expect("Should read");
        let read_obj1 = specific_reader
            .read_from_value(&mut to_read2)
            .expect("Should read from value");
        let read_obj2 = specific_reader
            .read(&mut to_read3)
            .expect("Should read from deserilize");
        let expected_value: Value = obj.clone().into();
        pretty_assertions::assert_eq!(obj, read_obj1);
        pretty_assertions::assert_eq!(obj, read_obj2);
        pretty_assertions::assert_eq!(val, expected_value);

        Ok(())
    }

    #[test]
    fn avro_rs_164_generic_reader_alternate_header() -> TestResult {
        let schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        let header_builder = GlueSchemaUuidHeader::from_uuid(schema_uuid);
        let generic_reader = GenericSingleObjectReader::builder()
            .schema(TestSingleObjectReader::get_schema())
            .header(header_builder.build_header())
            .build()
            .expect("failed to build reader");
        let data_to_read: Vec<u8> = vec![
            3, 0, 178, 241, 207, 0, 4, 52, 1, 62, 67, 154, 18, 94, 184, 72, 90, 95,
        ];
        let mut to_read = &data_to_read[..];
        let read_result = generic_reader
            .read_value(&mut to_read)
            .map_err(Error::into_details);
        matches!(read_result, Err(Details::ReadBytes(_)));
        Ok(())
    }
}
