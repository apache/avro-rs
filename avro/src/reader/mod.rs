pub mod asynch;
pub mod sync;

use crate::{
    AvroResult, AvroSchema, Schema,
    error::Details,
    from_value,
    headers::{HeaderBuilder, RabinFingerprintHeader},
    schema::ResolvedOwnedSchema,
    types::Value,
};
use futures::AsyncRead;
use serde::de::DeserializeOwned;
use std::{io::Read, marker::PhantomData};
pub use sync::*;

// This is for API compatibility with previous versions
pub use sync::*;

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
                    asynch::from_avro_datum(self.write_schema.get_root_schema(), reader, None).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AvroSchema, Error, Schema, encode::encode, error::Details, headers::GlueSchemaUuidHeader,
        rabin::Rabin, types::Value,
    };
    use apache_avro_test_helper::TestResult;
    use serde::Deserialize;
    use uuid::Uuid;

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
        let generic_reader = GenericSingleObjectReader::new(TestSingleObjectReader::get_schema())
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
        pretty_assertions::assert_eq!(obj, read_obj1);
        pretty_assertions::assert_eq!(obj, read_obj2);
        pretty_assertions::assert_eq!(val, expected_value);

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
}
