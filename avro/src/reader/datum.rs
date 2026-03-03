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

use std::io::Read;

use bon::bon;

use crate::{AvroResult, Schema, decode::decode_internal, schema::ResolvedSchema, types::Value};

/// Reader for reading raw Avro data.
///
/// This is most likely not what you need. Most users should use [`Reader`][crate::Reader],
/// [`GenericSingleObjectReader`][crate::GenericSingleObjectReader], or
/// [`SpecificSingleObjectReader`][crate::SpecificSingleObjectReader] instead.
pub struct GenericDatumReader<'s> {
    writer: &'s Schema,
    resolved: ResolvedSchema<'s>,
    reader: Option<(&'s Schema, ResolvedSchema<'s>)>,
}

#[bon]
impl<'s> GenericDatumReader<'s> {
    /// Build a [`DatumReader`].
    ///
    /// This is most likely not what you need. Most users should use [`Reader`][crate::Reader],
    /// [`GenericSingleObjectReader`][crate::GenericSingleObjectReader], or
    /// [`SpecificSingleObjectReader`][crate::SpecificSingleObjectReader] instead.
    #[builder]
    pub fn new(
        /// The schema that was used to write the Avro datum.
        #[builder(start_fn)]
        writer_schema: &'s Schema,
        /// Already resolved schemata that will be used to resolve references in the writer's schema.
        resolved_writer_schemata: Option<ResolvedSchema<'s>>,
        /// The schema that will be used to resolve the value to conform the the new schema.
        reader_schema: Option<&'s Schema>,
        /// Already resolved schemata that will be used to resolve references in the reader's schema.
        resolved_reader_schemata: Option<ResolvedSchema<'s>>,
    ) -> AvroResult<Self> {
        let resolved_writer_schemata = if let Some(resolved) = resolved_writer_schemata {
            resolved
        } else {
            ResolvedSchema::try_from(writer_schema)?
        };

        let reader = if let Some(reader) = reader_schema {
            if let Some(resolved) = resolved_reader_schemata {
                Some((reader, resolved))
            } else {
                Some((reader, ResolvedSchema::try_from(reader)?))
            }
        } else {
            None
        };

        Ok(Self {
            writer: writer_schema,
            resolved: resolved_writer_schemata,
            reader,
        })
    }
}

impl<'s, S: generic_datum_reader_builder::State> GenericDatumReaderBuilder<'s, S> {
    /// Set the schemata that will be used to resolve any references in the writer's schema.
    ///
    /// This is equivalent to `.resolved_writer_schemata(ResolvedSchema::new_with_schemata(schemata)?)`.
    /// If you already have a [`ResolvedSchema`], use that function instead.
    pub fn writer_schemata(
        self,
        schemata: Vec<&'s Schema>,
    ) -> AvroResult<
        GenericDatumReaderBuilder<'s, generic_datum_reader_builder::SetResolvedWriterSchemata<S>>,
    >
    where
        S::ResolvedWriterSchemata: generic_datum_reader_builder::IsUnset,
    {
        let resolved = ResolvedSchema::new_with_schemata(schemata)?;
        Ok(self.resolved_writer_schemata(resolved))
    }

    /// Set the schemata that will be used to resolve any references in the reader's schema.
    ///
    /// This is equivalent to `.resolved_reader_schemata(ResolvedSchema::new_with_schemata(schemata)?)`.
    /// If you already have a [`ResolvedSchema`], use that function instead.
    ///
    /// This function can only be called after the reader schema is set.
    pub fn reader_schemata(
        self,
        schemata: Vec<&'s Schema>,
    ) -> AvroResult<
        GenericDatumReaderBuilder<'s, generic_datum_reader_builder::SetResolvedReaderSchemata<S>>,
    >
    where
        S::ResolvedReaderSchemata: generic_datum_reader_builder::IsUnset,
        S::ReaderSchema: generic_datum_reader_builder::IsSet,
    {
        let resolved = ResolvedSchema::new_with_schemata(schemata)?;
        Ok(self.resolved_reader_schemata(resolved))
    }
}

impl<'s> GenericDatumReader<'s> {
    /// Read a Avro datum from the reader.
    pub fn read_value<R: Read>(&self, reader: &mut R) -> AvroResult<Value> {
        let value = decode_internal(self.writer, self.resolved.get_names(), None, reader)?;
        if let Some((reader, resolved)) = &self.reader {
            value.resolve_internal(reader, resolved.get_names(), None, &None)
        } else {
            Ok(value)
        }
    }
}

/// Deprecated.
///
/// This is equivalent to
/// ```ignore
/// GenericDatumReader::builder(writer_schema)
///    .maybe_reader_schema(reader_schema)
///    .build()?
///    .read_value(reader)
/// ```
///
/// Decode a `Value` encoded in Avro format given its `Schema` and anything implementing `io::Read`
/// to read from.
///
/// In case a reader `Schema` is provided, schema resolution will also be performed.
///
/// **NOTE** This function has a quite small niche of usage and does NOT take care of reading the
/// header and consecutive data blocks; use [`Reader`](struct.Reader.html) if you don't know what
/// you are doing, instead.
#[deprecated(since = "0.22.0", note = "Use `DatumReader` instead")]
pub fn from_avro_datum<R: Read>(
    writer_schema: &Schema,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    GenericDatumReader::builder(writer_schema)
        .maybe_reader_schema(reader_schema)
        .build()?
        .read_value(reader)
}

/// Deprecated.
///
/// This is equivalent to
/// ```ignore
/// GenericDatumReader::builder(writer_schema)
///    .writer_schemata(writer_schemata)?
///    .maybe_reader_schema(reader_schema)
///    .build()?
///    .read_value(reader)
/// ```
///
/// Decode a `Value` from raw Avro data.
///
/// If the writer schema is incomplete, i.e. contains `Schema::Ref`s then it will use the provided
/// schemata to resolve any dependencies.
///
/// When a reader `Schema` is provided, schema resolution will also be performed.
#[deprecated(since = "0.22.0", note = "Use `DatumReader` instead")]
pub fn from_avro_datum_schemata<R: Read>(
    writer_schema: &Schema,
    writer_schemata: Vec<&Schema>,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    GenericDatumReader::builder(writer_schema)
        .writer_schemata(writer_schemata)?
        .maybe_reader_schema(reader_schema)
        .build()?
        .read_value(reader)
}

/// Deprecated.
///
/// This is equivalent to
/// ```ignore
/// GenericDatumReader::builder(writer_schema)
///    .writer_schemata(writer_schemata)?
///    .maybe_reader_schema(reader_schema)
///    .reader_schemata(reader_schemata)?
///    .build()?
///    .read_value(reader)
/// ```
///
/// Decode a `Value` from raw Avro data.
///
/// If the writer schema is incomplete, i.e. contains `Schema::Ref`s then it will use the provided
/// schemata to resolve any dependencies.
///
/// When a reader `Schema` is provided, schema resolution will also be performed.
#[deprecated(since = "0.22.0", note = "Use `DatumReader` instead")]
pub fn from_avro_datum_reader_schemata<R: Read>(
    writer_schema: &Schema,
    writer_schemata: Vec<&Schema>,
    reader: &mut R,
    reader_schema: Option<&Schema>,
    reader_schemata: Vec<&Schema>,
) -> AvroResult<Value> {
    GenericDatumReader::builder(writer_schema)
        .writer_schemata(writer_schemata)?
        .maybe_reader_schema(reader_schema)
        .reader_schemata(reader_schemata)?
        .build()?
        .read_value(reader)
}

#[cfg(test)]
mod tests {
    use apache_avro_test_helper::TestResult;
    use serde::Deserialize;

    use crate::{
        Schema, from_value,
        reader::datum::GenericDatumReader,
        types::{Record, Value},
    };

    #[test]
    fn test_from_avro_datum() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
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
        }"#,
        )?;
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.into();

        let avro_datum = GenericDatumReader::builder(&schema)
            .build()?
            .read_value(&mut encoded)?;

        assert_eq!(avro_datum, expected);

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

        let expected_record: TestRecord3240 = TestRecord3240 {
            a: 27i64,
            b: String::from("foo"),
            a_nullable_array: None,
            a_nullable_string: None,
        };

        let avro_datum = GenericDatumReader::builder(&schema)
            .build()?
            .read_value(&mut encoded)?;
        let parsed_record: TestRecord3240 = match &avro_datum {
            Value::Record(_) => from_value::<TestRecord3240>(&avro_datum)?,
            unexpected => {
                panic!("could not map avro data to struct, found unexpected: {unexpected:?}")
            }
        };

        assert_eq!(parsed_record, expected_record);

        Ok(())
    }

    #[test]
    fn test_null_union() -> TestResult {
        let schema = Schema::parse_str(r#"["null", "long"]"#)?;
        let mut encoded: &'static [u8] = &[2, 0];

        let avro_datum = GenericDatumReader::builder(&schema)
            .build()?
            .read_value(&mut encoded)?;
        assert_eq!(avro_datum, Value::Union(1, Box::new(Value::Long(0))));

        Ok(())
    }
}
