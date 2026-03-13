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
    schema::{ResolvedOwnedSchema, ResolvedSchema},
    serde::deser_schema::{Config, SchemaAwareDeserializer},
    types::Value,
    util::is_human_readable,
};

/// Reader for reading raw Avro data.
///
/// This is most likely not what you need. Most users should use [`Reader`][crate::Reader],
/// [`GenericSingleObjectReader`][crate::GenericSingleObjectReader], or
/// [`SpecificSingleObjectReader`][crate::SpecificSingleObjectReader] instead.
pub struct GenericDatumReader<'s> {
    writer: &'s Schema,
    resolved: ResolvedSchema<'s>,
    reader: Option<(&'s Schema, ResolvedSchema<'s>)>,
    human_readable: bool,
}

#[bon]
impl<'s> GenericDatumReader<'s> {
    /// Build a [`GenericDatumReader`].
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
        /// Was the data serialized with `human_readable`.
        #[builder(default = is_human_readable())]
        human_readable: bool,
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
            human_readable,
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

    /// Read a Avro datum from the reader.
    ///
    /// # Panics
    /// Will panic if a reader schema has been configured, this is a WIP.
    pub fn read_deser<T: DeserializeOwned>(&self, reader: &mut impl Read) -> AvroResult<T> {
        // `reader` is `impl Read` instead of a generic on the function like T so it's easier to
        // specify the type wanted (`read_deser<String>` vs `read_deser<String, _>`)
        if let Some((_, _)) = &self.reader {
            todo!("Schema aware deserialisation does not resolve schemas yet");
        } else {
            T::deserialize(SchemaAwareDeserializer::new(
                reader,
                self.writer,
                Config {
                    names: self.resolved.get_names(),
                    human_readable: self.human_readable,
                },
            )?)
        }
    }
}

/// Reader for reading raw Avro data.
///
/// This is most likely not what you need. Most users should use [`Reader`][crate::Reader],
/// [`GenericSingleObjectReader`][crate::GenericSingleObjectReader], or
/// [`SpecificSingleObjectReader`][crate::SpecificSingleObjectReader] instead.
pub struct SpecificDatumReader<T: AvroSchema> {
    resolved: ResolvedOwnedSchema,
    human_readable: bool,
    phantom: PhantomData<T>,
}

#[bon]
impl<T: AvroSchema> SpecificDatumReader<T> {
    /// Build a [`SpecificDatumReader`].
    ///
    /// This is most likely not what you need. Most users should use [`Reader`][crate::Reader],
    /// [`GenericSingleObjectReader`][crate::GenericSingleObjectReader], or
    /// [`SpecificSingleObjectReader`][crate::SpecificSingleObjectReader] instead.
    #[builder]
    pub fn new(#[builder(default = is_human_readable())] human_readable: bool) -> AvroResult<Self> {
        Ok(Self {
            resolved: T::get_schema().try_into()?,
            human_readable,
            phantom: PhantomData,
        })
    }
}

impl<T: AvroSchema + DeserializeOwned> SpecificDatumReader<T> {
    pub fn read<R: Read>(&self, reader: &mut R) -> AvroResult<T> {
        T::deserialize(SchemaAwareDeserializer::new(
            reader,
            self.resolved.get_root_schema(),
            Config {
                names: self.resolved.get_names(),
                human_readable: self.human_readable,
            },
        )?)
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
#[deprecated(since = "0.22.0", note = "Use `GenericDatumReader` instead")]
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
#[deprecated(since = "0.22.0", note = "Use `GenericDatumReader` instead")]
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
#[deprecated(since = "0.22.0", note = "Use `GenericDatumReader` instead")]
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
        Schema,
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
      "name": "TestRecord3240",
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
        let mut encoded: &[u8] = &[54, 6, 102, 111, 111];

        let error = GenericDatumReader::builder(&schema)
            .build()?
            .read_deser::<TestRecord3240>(&mut encoded)
            .unwrap_err();
        // TODO: Create a version of this test that does schema resolution
        assert_eq!(
            error.to_string(),
            "Failed to read bytes for decoding variable length integer: failed to fill whole buffer"
        );

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
