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

//! Logic handling reading from Avro format at user level.

pub use crate::state_machines::reading::sync::{
    Reader, from_avro_datum, from_avro_datum_reader_schemata, from_avro_datum_schemata,
};
use crate::{
    AvroResult,
    error::Details,
    from_value,
    headers::{HeaderBuilder, RabinFingerprintHeader},
    schema::{AvroSchema, ResolvedOwnedSchema, Schema},
    types::Value,
};
use futures::AsyncRead;
use serde::de::DeserializeOwned;
use std::{io::Read, marker::PhantomData};

pub mod async_reader {
    #[doc(inline)]
    pub use crate::state_machines::reading::async_impl::{
        Reader, from_avro_datum, from_avro_datum_reader_schemata, from_avro_datum_schemata,
    };
}

/// Reader for Avro objects created using the [single-object encoding].
///
/// [single-object encoding]: https://avro.apache.org/docs/++version++/specification/#single-object-encoding
pub struct GenericSingleObjectReader {
    write_schema: ResolvedOwnedSchema,
    expected_header: Vec<u8>,
}

impl GenericSingleObjectReader {
    /// Create a reader for the given schema.
    ///
    /// This will expect the input to use the [`RabinFingerprintHeader`].
    pub fn new(schema: Schema) -> AvroResult<GenericSingleObjectReader> {
        let header_builder = RabinFingerprintHeader::from_schema(&schema);
        Self::new_with_header_builder(schema, header_builder)
    }

    /// Create a reader for the given schema with a custom fingerprint.
    ///
    /// See [`HeaderBuilder`] for details on how to implement a custom fingerprint.
    pub fn new_with_header_builder<HB: HeaderBuilder>(
        schema: Schema,
        header_builder: HB,
    ) -> AvroResult<GenericSingleObjectReader> {
        let expected_header = header_builder.build_header();
        Ok(GenericSingleObjectReader {
            write_schema: ResolvedOwnedSchema::try_from(schema)?,
            expected_header,
        })
    }

    /// Read a [`Value`] from the reader.
    pub fn read_value<R: Read>(&self, reader: &mut R) -> AvroResult<Value> {
        let mut header = vec![0; self.expected_header.len()];
        match reader.read_exact(&mut header) {
            Ok(_) => {
                if self.expected_header == header {
                    from_avro_datum(self.write_schema.get_root_schema(), reader, None)
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

    pub async fn read_value_async<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> AvroResult<Value> {
        use futures::AsyncReadExt as _;

        let mut header = vec![0; self.expected_header.len()];
        match reader.read_exact(&mut header).await {
            Ok(_) => {
                if self.expected_header == header {
                    async_reader::from_avro_datum(self.write_schema.get_root_schema(), reader, None)
                        .await
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
}

/// Reader for Avro objects created using the [single-object encoding] deserializing directly to `T`.
///
/// [single-object encoding]: https://avro.apache.org/docs/++version++/specification/#single-object-encoding
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
    /// Create the reader from the schema associated with `T`.
    pub fn new() -> AvroResult<SpecificSingleObjectReader<T>> {
        Ok(SpecificSingleObjectReader {
            inner: GenericSingleObjectReader::new(T::get_schema())?,
            _model: PhantomData,
        })
    }
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema + From<Value>,
{
    /// Read a `T` from the reader.
    pub fn read_from_value<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        self.inner.read_value(reader).map(|v| v.into())
    }

    /// Read a `T` from the reader.
    pub async fn read_from_value_async<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> AvroResult<T> {
        self.inner.read_value_async(reader).await.map(|v| v.into())
    }
}

impl<T> SpecificSingleObjectReader<T>
where
    T: AvroSchema + DeserializeOwned,
{
    /// Read a `T` from the reader.
    pub fn read<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        from_value::<T>(&self.inner.read_value(reader)?)
    }

    pub async fn read_async<R: AsyncRead + Unpin>(&self, reader: &mut R) -> AvroResult<T> {
        from_value::<T>(&self.inner.read_value_async(reader).await?)
    }
}

/// Reads the marker bytes from Avro bytes generated earlier by a [`Writer`].
///
/// [`Writer`]: crate::Writer
pub fn read_marker(bytes: &[u8]) -> [u8; 16] {
    assert!(
        bytes.len() > 16,
        "The bytes are too short to read a marker from them"
    );
    let mut marker = [0_u8; 16];
    marker.clone_from_slice(&bytes[(bytes.len() - 16)..]);
    marker
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Error, encode::encode, headers::GlueSchemaUuidHeader, rabin::Rabin, types::Record,
    };
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::{collections::HashMap, io::Cursor};
    use uuid::Uuid;

    const SCHEMA: &str = r#"
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
    const UNION_SCHEMA: &str = r#"["null", "long"]"#;
    const ENCODED: &[u8] = &[
        79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8,
        101u8, 109u8, 97u8, 222u8, 1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8,
        114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8, 44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8,
        58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8, 105u8, 101u8, 108u8,
        100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
        97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8,
        103u8, 34u8, 44u8, 34u8, 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8,
        50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8,
        44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8, 114u8, 105u8,
        110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8,
        100u8, 101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8,
        6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8, 114u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
    ];

    #[test]
    fn test_from_avro_datum() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.into();

        assert_eq!(from_avro_datum(&schema, &mut encoded, None)?, expected);

        Ok(())
    }

    #[test]
    fn test_from_avro_datum_with_union_to_struct() -> TestResult {
        const TEST_RECORD_SCHEMA_3240: &str = r#"
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
        },
        {
            "name": "a_nullable_array",
            "type": ["null", {"type": "array", "items": {"type": "string"}}],
            "default": null
        },
        {
            "name": "a_nullable_boolean",
            "type": ["null", {"type": "boolean"}],
            "default": null
        },
        {
            "name": "a_nullable_string",
            "type": ["null", {"type": "string"}],
            "default": null
        }
      ]
    }
    "#;
        #[derive(Default, Debug, Deserialize, PartialEq, Eq)]
        struct TestRecord3240 {
            a: i64,
            b: String,
            a_nullable_array: Option<Vec<String>>,
            // we are missing the 'a_nullable_boolean' field to simulate missing keys
            // a_nullable_boolean: Option<bool>,
            a_nullable_string: Option<String>,
        }

        let schema = Schema::parse_str(TEST_RECORD_SCHEMA_3240)?;
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        // The schema used to read is not compatible with what is written
        assert!(from_avro_datum(&schema, &mut encoded, None).is_err());

        // let avro_datum = from_avro_datum(&schema, &mut encoded, None)?;

        // let expected_record: TestRecord3240 = TestRecord3240 {
        //     a: 27i64,
        //     b: String::from("foo"),
        //     a_nullable_array: None,
        //     a_nullable_string: None,
        // };

        // let parsed_record: TestRecord3240 = match &avro_datum {
        //     Value::Record(_) => from_value::<TestRecord3240>(&avro_datum)?,
        //     unexpected => {
        //         panic!("could not map avro data to struct, found unexpected: {unexpected:?}")
        //     }
        // };
        //
        // assert_eq!(parsed_record, expected_record);

        Ok(())
    }

    #[test]
    fn test_null_union() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let mut encoded: &'static [u8] = &[2, 0];

        assert_eq!(
            from_avro_datum(&schema, &mut encoded, None)?,
            Value::Union(1, Box::new(Value::Long(0)))
        );

        Ok(())
    }

    #[test]
    fn test_reader_iterator() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let reader = Reader::with_schema(&schema, ENCODED)?;

        let mut record1 = Record::new(&schema).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = [record1.into(), record2.into()];

        for (i, value) in reader.enumerate() {
            assert_eq!(value?, expected[i]);
        }

        Ok(())
    }

    #[test]
    fn test_reader_invalid_header() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let invalid = ENCODED.iter().copied().skip(1).collect::<Vec<u8>>();
        assert!(Reader::with_schema(&schema, &invalid[..]).is_err());

        Ok(())
    }

    #[test]
    fn test_reader_invalid_block() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let invalid = ENCODED
            .iter()
            .copied()
            .rev()
            .skip(19)
            .collect::<Vec<u8>>()
            .into_iter()
            .rev()
            .collect::<Vec<u8>>();
        let mut reader = Reader::with_schema(&schema, &invalid[..])?;

        // The block says it contains 2 values, but only contains one.
        // The first value is successfully decoded
        let _v = reader.next().unwrap().unwrap();
        // The second fails with an unexpected end of file error.
        assert!(reader.next().unwrap().is_err());

        Ok(())
    }

    #[test]
    fn test_reader_empty_buffer() -> TestResult {
        let empty = Cursor::new(Vec::new());
        assert!(Reader::new(empty).is_err());

        Ok(())
    }

    #[test]
    fn test_reader_only_header() -> TestResult {
        let invalid = ENCODED.iter().copied().take(165).collect::<Vec<u8>>();
        let reader = Reader::new(&invalid[..])?;
        for value in reader {
            assert!(value.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_avro_3405_read_user_metadata_success() -> TestResult {
        use crate::writer::Writer;

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new());

        let mut user_meta_data: HashMap<String, Vec<u8>> = HashMap::new();
        user_meta_data.insert("stringKey".to_string(), b"stringValue".to_vec());
        user_meta_data.insert("bytesKey".to_string(), b"bytesValue".to_vec());
        user_meta_data.insert("vecKey".to_string(), vec![1, 2, 3]);

        for (k, v) in user_meta_data.iter() {
            writer.add_user_metadata(k.to_string(), v)?;
        }

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append(record.clone())?;
        writer.append(record.clone())?;
        writer.flush()?;
        let result = writer.into_inner()?;

        let reader = Reader::new(&result[..])?;
        assert_eq!(reader.user_metadata(), &user_meta_data);

        Ok(())
    }

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
        let generic_reader = GenericSingleObjectReader::new(TestSingleObjectReader::get_schema())
            .expect("Schema should resolve");
        let val = generic_reader
            .read_value(&mut to_read)
            .expect("Should read");
        let expected_value: Value = obj.into();
        assert_eq!(expected_value, val);

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
        let generic_reader = GenericSingleObjectReader::new(TestSingleObjectReader::get_schema())
            .expect("Schema should resolve");
        let val = generic_reader
            .read_value(&mut to_read)
            .expect("Should read");
        let expected_value: Value = obj.into();
        assert_eq!(expected_value, val);

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
        let generic_reader = GenericSingleObjectReader::new(TestSingleObjectReader::get_schema())
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
        assert_eq!(obj, read_obj1);
        assert_eq!(obj, read_obj2);
        assert_eq!(val, expected_value);

        Ok(())
    }

    #[test]
    fn avro_rs_164_generic_reader_alternate_header() -> TestResult {
        let schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        let header_builder = GlueSchemaUuidHeader::from_uuid(schema_uuid);
        let generic_reader = GenericSingleObjectReader::new_with_header_builder(
            TestSingleObjectReader::get_schema(),
            header_builder,
        )
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

    #[cfg(not(feature = "snappy"))]
    #[test]
    fn test_avro_3549_read_not_enabled_codec() {
        let snappy_compressed_avro = vec![
            79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 210, 1, 123,
            34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34,
            110, 117, 109, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110,
            103, 34, 125, 93, 44, 34, 110, 97, 109, 101, 34, 58, 34, 101, 118, 101, 110, 116, 34,
            44, 34, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 58, 34, 101, 120, 97, 109, 112,
            108, 101, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 44, 34, 116, 121, 112, 101, 34,
            58, 34, 114, 101, 99, 111, 114, 100, 34, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100,
            101, 99, 12, 115, 110, 97, 112, 112, 121, 0, 213, 209, 241, 208, 200, 110, 164, 47,
            203, 25, 90, 235, 161, 167, 195, 177, 2, 20, 4, 12, 6, 49, 50, 51, 115, 38, 58, 0, 213,
            209, 241, 208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177,
        ];

        if let Err(err) = Reader::new(snappy_compressed_avro.as_slice()) {
            assert_eq!("Codec 'snappy' is not supported/enabled", err.to_string());
        } else {
            panic!("Expected an error in the reading of the codec!");
        }
    }
}
