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

use bon::bon;
use serde::Serialize;
use std::io::Write;

use crate::{
    AvroResult, Schema,
    encode::encode_internal,
    error::Details,
    schema::{NamesRef, ResolvedSchema},
    serde::ser_schema::{Config, SchemaAwareSerializer},
    types::Value,
    util::is_human_readable,
};

/// Writer for writing raw Avro data.
///
/// This is most likely not what you need. Most users should use [`Writer`][crate::Writer],
/// [`GenericSingleObjectWriter`][crate::GenericSingleObjectWriter], or
/// [`SpecificSingleObjectWriter`][crate::SpecificSingleObjectWriter] instead.
pub struct GenericDatumWriter<'s> {
    schema: &'s Schema,
    resolved: ResolvedSchema<'s>,
    validate: bool,
    human_readable: bool,
    target_block_size: Option<usize>,
}

#[bon]
impl<'s> GenericDatumWriter<'s> {
    /// Configure a new writer.
    #[builder]
    pub fn new(
        /// The schema for the data that will be written
        #[builder(start_fn)]
        schema: &'s Schema,
        /// Already resolved schemata that will be used to resolve references in the writer's schema.
        ///
        /// You can also use [`Self::schemata`] instead.
        resolved_schemata: Option<ResolvedSchema<'s>>,
        /// Validate values against the writer schema before writing them.
        ///
        /// Defaults to `true`.
        ///
        /// Setting this to `false` and writing values that don't match the schema will make the
        /// written data unreadable.
        #[builder(default = true)]
        validate: bool,
        /// At what block size to start a new block (for arrays and maps).
        ///
        /// This is a minimum value, the block size will always be larger than this except for the last
        /// block.
        ///
        /// When set to `None` all values will be written in a single block. This can be faster as no
        /// intermediate buffer is used, but seeking through written data will be slower.
        target_block_size: Option<usize>,
        /// Should [`Serialize`] implementations pick a human readable representation.
        ///
        /// It is recommended to set this to `false`.
        #[builder(default = is_human_readable())]
        human_readable: bool,
    ) -> AvroResult<Self> {
        let resolved = if let Some(resolved) = resolved_schemata {
            resolved
        } else {
            ResolvedSchema::try_from(schema)?
        };
        Ok(Self {
            schema,
            resolved,
            validate,
            human_readable,
            target_block_size,
        })
    }
}

impl<'s, S: generic_datum_writer_builder::State> GenericDatumWriterBuilder<'s, S> {
    /// Set the schemata that will be used to resolve any references in the schema.
    ///
    /// This is equivalent to `.resolved_schemata(ResolvedSchema::new_with_schemata(schemata)?)`.
    /// If you already have a [`ResolvedSchema`], use that function instead.
    pub fn schemata(
        self,
        schemata: Vec<&'s Schema>,
    ) -> AvroResult<
        GenericDatumWriterBuilder<'s, generic_datum_writer_builder::SetResolvedSchemata<S>>,
    >
    where
        S::ResolvedSchemata: generic_datum_writer_builder::IsUnset,
    {
        let resolved = ResolvedSchema::new_with_schemata(schemata)?;
        Ok(self.resolved_schemata(resolved))
    }
}

impl GenericDatumWriter<'_> {
    /// Write a value to the writer.
    pub fn write_value<W: Write, V: Into<Value>>(
        &self,
        writer: &mut W,
        value: V,
    ) -> AvroResult<usize> {
        let value = value.into();
        self.write_value_ref(writer, &value)
    }

    /// Write a value to the writer.
    pub fn write_value_ref<W: Write>(&self, writer: &mut W, value: &Value) -> AvroResult<usize> {
        if self.validate
            && value
                .validate_internal(self.schema, self.resolved.get_names(), None)
                .is_some()
        {
            return Err(Details::Validation.into());
        }
        encode_internal(value, self.schema, self.resolved.get_names(), None, writer)
    }

    /// Write a value to a [`Vec`].
    pub fn write_value_to_vec<V: Into<Value>>(&self, value: V) -> AvroResult<Vec<u8>> {
        let mut vec = Vec::new();
        self.write_value(&mut vec, value)?;
        Ok(vec)
    }

    /// Serialize `T` to the writer.
    pub fn write_ser<W: Write, T: Serialize>(
        &self,
        writer: &mut W,
        value: &T,
    ) -> AvroResult<usize> {
        let config = Config {
            names: self.resolved.get_names(),
            target_block_size: self.target_block_size,
            human_readable: self.human_readable,
        };
        value.serialize(SchemaAwareSerializer::new(writer, self.schema, config)?)
    }

    /// Serialize `T` to a [`Vec`].
    pub fn write_ser_to_vec<T: Serialize>(&self, value: &T) -> AvroResult<Vec<u8>> {
        let mut vec = Vec::new();
        self.write_ser(&mut vec, value)?;
        Ok(vec)
    }
}

/// Deprecated. Use [`GenericDatumWriter`] instead.
///
/// This is equivalent to:
/// ```ignore
/// GenericDatumWriter::builder(schema)
///     .build()?
///     .write_value_to_vec(value)
/// ```
///
/// Encode a value into raw Avro data, also performs schema validation.
///
/// **NOTE**: This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`] to be fully Avro-compatible if you don't know what
/// you are doing, instead.
///
/// [`Writer`]: crate::Writer
#[deprecated(since = "0.22.0", note = "Use GenericDatumWriter instead")]
pub fn to_avro_datum<T: Into<Value>>(schema: &Schema, value: T) -> AvroResult<Vec<u8>> {
    GenericDatumWriter::builder(schema)
        .build()?
        .write_value_to_vec(value)
}

/// Write the referenced [Serialize]able object to the provided [Write] object.
///
/// It is recommended to use [`GenericDatumWriter`] instead.
///
/// Returns a result with the number of bytes written.
///
/// **NOTE**: This function has a quite small niche of usage and does **NOT** generate headers and sync
/// markers; use [`Writer::append_ser`] to be fully Avro-compatible
/// if you don't know what you are doing, instead.
///
/// [`Writer::append_ser`]: crate::Writer::append_ser
pub fn write_avro_datum_ref<T: Serialize, W: Write>(
    schema: &Schema,
    names: &NamesRef,
    data: &T,
    writer: &mut W,
) -> AvroResult<usize> {
    let config = Config {
        names,
        target_block_size: None,
        human_readable: is_human_readable(),
    };
    data.serialize(SchemaAwareSerializer::new(writer, schema, config)?)
}

/// Deprecated. Use [`GenericDatumWriter`] instead.
///
/// This is equivalent to:
/// ```ignore
/// GenericDatumWriter::builder(schema)
///     .schemata(schemata)?
///     .build()?
///     .write_value_to_vec(value)
/// ```
///
/// Encode a value into raw Avro data, also performs schema validation.
///
/// If the provided `schema` is incomplete then its dependencies must be
/// provided in `schemata`
#[deprecated(since = "0.22.0", note = "Use GenericDatumWriter instead")]
pub fn to_avro_datum_schemata<T: Into<Value>>(
    schema: &Schema,
    schemata: Vec<&Schema>,
    value: T,
) -> AvroResult<Vec<u8>> {
    GenericDatumWriter::builder(schema)
        .schemata(schemata)?
        .build()?
        .write_value_to_vec(value)
}

#[cfg(test)]
mod tests {
    use apache_avro_test_helper::TestResult;

    use super::*;
    use crate::reader::datum::GenericDatumReader;
    use crate::{
        Days, Decimal, Duration, Millis, Months,
        schema::{DecimalSchema, FixedSchema, InnerDecimalSchema, Name},
        types::Record,
        util::zig_i64,
    };

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

    #[test]
    fn test_to_avro_datum() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let mut expected = Vec::new();
        zig_i64(27, &mut expected)?;
        zig_i64(3, &mut expected)?;
        expected.extend([b'f', b'o', b'o']);

        let written = GenericDatumWriter::builder(&schema)
            .build()?
            .write_value_to_vec(record)?;

        assert_eq!(written, expected);

        Ok(())
    }

    #[test]
    fn avro_rs_193_write_avro_datum_ref() -> TestResult {
        #[derive(Serialize)]
        struct TestStruct {
            a: i64,
            b: String,
        }

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer: Vec<u8> = Vec::new();
        let data = TestStruct {
            a: 27,
            b: "foo".to_string(),
        };

        let mut expected = Vec::new();
        zig_i64(27, &mut expected)?;
        zig_i64(3, &mut expected)?;
        expected.extend([b'f', b'o', b'o']);

        let bytes = GenericDatumWriter::builder(&schema)
            .build()?
            .write_ser(&mut writer, &data)?;

        assert_eq!(bytes, expected.len());
        assert_eq!(writer, expected);

        Ok(())
    }

    #[test]
    fn test_union_not_null() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let union = Value::Union(1, Box::new(Value::Long(3)));

        let mut expected = Vec::new();
        zig_i64(1, &mut expected)?;
        zig_i64(3, &mut expected)?;

        let written = GenericDatumWriter::builder(&schema)
            .build()?
            .write_value_to_vec(union)?;
        assert_eq!(written, expected);

        Ok(())
    }

    #[test]
    fn test_union_null() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let union = Value::Union(0, Box::new(Value::Null));

        let mut expected = Vec::new();
        zig_i64(0, &mut expected)?;

        let written = GenericDatumWriter::builder(&schema)
            .build()?
            .write_value_to_vec(union)?;
        assert_eq!(written, expected);

        Ok(())
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "Value does not implement PartialEq<&Value>"
    )]
    fn logical_type_test<T: Into<Value> + Clone>(
        schema_str: &'static str,

        expected_schema: &Schema,
        value: Value,

        raw_schema: &Schema,
        raw_value: T,
    ) -> TestResult {
        let schema = Schema::parse_str(schema_str)?;
        assert_eq!(&schema, expected_schema);
        // The serialized format should be the same as the schema.
        let ser = GenericDatumWriter::builder(&schema)
            .build()?
            .write_value_to_vec(value.clone())?;
        let raw_ser = GenericDatumWriter::builder(raw_schema)
            .build()?
            .write_value_to_vec(raw_value)?;
        assert_eq!(ser, raw_ser);

        // Should deserialize from the schema into the logical type.
        let mut r = ser.as_slice();
        let de = GenericDatumReader::builder(&schema)
            .build()?
            .read_value(&mut r)?;
        assert_eq!(de, value);
        Ok(())
    }

    #[test]
    fn date() -> TestResult {
        logical_type_test(
            r#"{"type": "int", "logicalType": "date"}"#,
            &Schema::Date,
            Value::Date(1_i32),
            &Schema::Int,
            1_i32,
        )
    }

    #[test]
    fn time_millis() -> TestResult {
        logical_type_test(
            r#"{"type": "int", "logicalType": "time-millis"}"#,
            &Schema::TimeMillis,
            Value::TimeMillis(1_i32),
            &Schema::Int,
            1_i32,
        )
    }

    #[test]
    fn time_micros() -> TestResult {
        logical_type_test(
            r#"{"type": "long", "logicalType": "time-micros"}"#,
            &Schema::TimeMicros,
            Value::TimeMicros(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn timestamp_millis() -> TestResult {
        logical_type_test(
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            &Schema::TimestampMillis,
            Value::TimestampMillis(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn timestamp_micros() -> TestResult {
        logical_type_test(
            r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
            &Schema::TimestampMicros,
            Value::TimestampMicros(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn decimal_fixed() -> TestResult {
        let size = 30;
        let fixed = FixedSchema {
            name: Name::new("decimal")?,
            aliases: None,
            doc: None,
            size,
            attributes: Default::default(),
        };
        let inner = InnerDecimalSchema::Fixed(fixed.clone());
        let value = vec![0u8; size];
        logical_type_test(
            r#"{"type": {"type": "fixed", "size": 30, "name": "decimal"}, "logicalType": "decimal", "precision": 20, "scale": 5}"#,
            &Schema::Decimal(DecimalSchema {
                precision: 20,
                scale: 5,
                inner,
            }),
            Value::Decimal(Decimal::from(value.clone())),
            &Schema::Fixed(fixed),
            Value::Fixed(size, value),
        )
    }

    #[test]
    fn decimal_bytes() -> TestResult {
        let value = vec![0u8; 10];
        logical_type_test(
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 3}"#,
            &Schema::Decimal(DecimalSchema {
                precision: 4,
                scale: 3,
                inner: InnerDecimalSchema::Bytes,
            }),
            Value::Decimal(Decimal::from(value.clone())),
            &Schema::Bytes,
            value,
        )
    }

    #[test]
    fn duration() -> TestResult {
        let inner = Schema::Fixed(FixedSchema {
            name: Name::new("duration")?,
            aliases: None,
            doc: None,
            size: 12,
            attributes: Default::default(),
        });
        let value = Value::Duration(Duration::new(
            Months::new(256),
            Days::new(512),
            Millis::new(1024),
        ));
        logical_type_test(
            r#"{"type": {"type": "fixed", "name": "duration", "size": 12}, "logicalType": "duration"}"#,
            &Schema::Duration(FixedSchema {
                name: Name::try_from("duration").expect("Name is valid"),
                aliases: None,
                doc: None,
                size: 12,
                attributes: Default::default(),
            }),
            value,
            &inner,
            Value::Fixed(12, vec![0, 1, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0]),
        )
    }
}
