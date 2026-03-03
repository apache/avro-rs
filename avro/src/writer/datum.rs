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

use std::io::Write;

use serde::Serialize;

use crate::{
    AvroResult, Schema,
    encode::{encode, encode_internal},
    error::Details,
    schema::{NamesRef, ResolvedOwnedSchema, ResolvedSchema},
    serde::ser_schema::SchemaAwareWriteSerializer,
    types::Value,
};

/// Encode a value into raw Avro data, also performs schema validation.
///
/// **NOTE**: This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`] to be fully Avro-compatible if you don't know what
/// you are doing, instead.
pub fn to_avro_datum<T: Into<Value>>(schema: &Schema, value: T) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    write_avro_datum(schema, value, &mut buffer)?;
    Ok(buffer)
}

/// Write the referenced [Serialize]able object to the provided [Write] object.
///
/// Returns a result with the number of bytes written.
///
/// **NOTE**: This function has a quite small niche of usage and does **NOT** generate headers and sync
/// markers; use [`append_ser`](Writer::append_ser) to be fully Avro-compatible
/// if you don't know what you are doing, instead.
pub fn write_avro_datum_ref<T: Serialize, W: Write>(
    schema: &Schema,
    names: &NamesRef,
    data: &T,
    writer: &mut W,
) -> AvroResult<usize> {
    let mut serializer = SchemaAwareWriteSerializer::new(writer, schema, names, None);
    data.serialize(&mut serializer)
}

/// Encode a value into raw Avro data, also performs schema validation.
///
/// If the provided `schema` is incomplete then its dependencies must be
/// provided in `schemata`
pub fn to_avro_datum_schemata<T: Into<Value>>(
    schema: &Schema,
    schemata: Vec<&Schema>,
    value: T,
) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    write_avro_datum_schemata(schema, schemata, value, &mut buffer)?;
    Ok(buffer)
}

/// Encode a value into raw Avro data, also performs schema validation.
///
/// This is an internal function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
pub(super) fn write_avro_datum<T: Into<Value>, W: Write>(
    schema: &Schema,
    value: T,
    writer: &mut W,
) -> AvroResult<()> {
    let avro = value.into();
    if !avro.validate(schema) {
        return Err(Details::Validation.into());
    }
    encode(&avro, schema, writer)?;
    Ok(())
}

pub(super) fn write_avro_datum_schemata<T: Into<Value>>(
    schema: &Schema,
    schemata: Vec<&Schema>,
    value: T,
    buffer: &mut Vec<u8>,
) -> AvroResult<usize> {
    let avro = value.into();
    let rs = ResolvedSchema::try_from(schemata)?;
    let names = rs.get_names();
    let enclosing_namespace = schema.namespace();
    if let Some(_err) = avro.validate_internal(schema, names, enclosing_namespace) {
        return Err(Details::Validation.into());
    }
    encode_internal(&avro, schema, names, enclosing_namespace, buffer)
}

pub(super) fn write_value_ref_owned_resolved<W: Write>(
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
    use std::collections::HashMap;

    use apache_avro_test_helper::TestResult;

    use crate::{
        Days, Decimal, Duration, Millis, Months,
        schema::{DecimalSchema, FixedSchema, InnerDecimalSchema, Name},
        types::Record,
        util::zig_i64,
    };

    use super::*;

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

        assert_eq!(to_avro_datum(&schema, record)?, expected);

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

        let bytes = write_avro_datum_ref(&schema, &HashMap::new(), &data, &mut writer)?;

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

        assert_eq!(to_avro_datum(&schema, union)?, expected);

        Ok(())
    }

    #[test]
    fn test_union_null() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let union = Value::Union(0, Box::new(Value::Null));

        let mut expected = Vec::new();
        zig_i64(0, &mut expected)?;

        assert_eq!(to_avro_datum(&schema, union)?, expected);

        Ok(())
    }

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
        let ser = to_avro_datum(&schema, value.clone())?;
        let raw_ser = to_avro_datum(raw_schema, raw_value)?;
        assert_eq!(ser, raw_ser);

        // Should deserialize from the schema into the logical type.
        let mut r = ser.as_slice();
        let de = crate::from_avro_datum(&schema, &mut r, None)?;
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
