use crate::error::Details;
use crate::{
    util::{zag_i32, zag_i64}, Error,
    Schema,
};
use serde::de::Visitor;
use serde::{de, forward_to_deserialize_any};
use std::io::Read;
use std::slice::Iter;

pub struct SchemaAwareReadDeserializer<'a, R: Read> {
    reader: &'a mut R,
    root_schema: &'a Schema,
}

impl<'a, R: Read> SchemaAwareReadDeserializer<'a, R> {
    #[allow(dead_code)] // TODO: remove! It is actually used in reader.rs
    pub(crate) fn new(reader: &'a mut R, root_schema: &'a Schema) -> Self {
        Self {
            reader,
            root_schema,
        }
    }
}

impl<'de: 'a, 'a, R: Read> serde::de::Deserializer<'de> for SchemaAwareReadDeserializer<'a, R> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Implement the deserialization logic here
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let schema = self.root_schema;
        let mut this = self;
        this.deserialize_bool_with_schema(visitor, schema)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i32(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i32(visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let schema = self.root_schema;
        let mut this = self;
        this.deserialize_i32_with_schema(visitor, schema)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let schema = self.root_schema;
        let mut this = self;
        this.deserialize_i64_with_schema(visitor, schema)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i32(visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i32(visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!("Implement deserialization for str")
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.root_schema {
            Schema::String => {
                match zag_i64(self.reader)
                    .map_err(Error::into_details) {
                    Ok(len) => {
                        dbg!(len);
                        let mut buf = vec![0; usize::try_from(len)
                            .map_err(|e| Details::ConvertI64ToUsize(e, len))?];
                        dbg!(&buf);
                        self.reader.read_exact(&mut buf).map_err(|e| {
                            Details::ReadBytes(e)
                        })?;
                        let string = String::from_utf8(buf)
                            .map_err(|e| Details::ConvertToUtf8(e))?;
                        visitor.visit_string(string)
                    }
                    Err(details) => {
                        Err(de::Error::custom(format!(
                            "Cannot read the length of the string schema {details:?}",
                        )))
                    }
                }

            }
            not_implemented => {
                Err(de::Error::custom(format!(
                    "Expected a String schema, but got {:?}",
                    not_implemented
                )))
            }
        }
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.root_schema {
            Schema::Null => visitor.visit_none(),
            Schema::Union(union_schema) => {
                match zag_i64(self.reader).map_err(Error::into_details) {
                    Ok(index) => {
                        let variants = union_schema.variants();
                        let variant = variants
                            .get(usize::try_from(index).map_err(|e| Details::ConvertI64ToUsize(e, index))?)
                            .ok_or(Details::GetUnionVariant {
                                index,
                                num_variants: variants.len(),
                            })?;
                        dbg!(&variant);
                        match variant {
                            Schema::Null => visitor.visit_none(),
                            _ => visitor.visit_some(
                                SchemaAwareReadDeserializer::new(self.reader, variant),
                            ),
                        }
                    }
                    Err(details) => Err(de::Error::custom(format!(
                        "Cannot read the index of the union schema variant {details:?}",
                    ))),
                }
            }
            _ => Err(de::Error::custom(format!(
                    "Expected a Union, but got {:?}",
                    self.root_schema
                ))),
            }
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let schema = self.root_schema;
        let mut this = self;
        this.deserialize_struct_with_schema(name, fields, visitor, schema)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

impl<'de: 'a, 'a, R: Read> SchemaAwareReadDeserializer<'a, R> {
    fn deserialize_bool_with_schema<V>(
        &mut self,
        visitor: V,
        schema: &Schema,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let create_error = |cause: &str| {
            Details::DeserializeValueWithSchema {
                value_type: "bool",
                value: format!("Cause: {cause}"),
                schema: schema.clone(),
            }
            .into()
        };

        match schema {
            Schema::Boolean => {
                let mut buf = [0; 1];
                self.reader
                    .read_exact(&mut buf) // Read a single byte
                    .map_err(|e| create_error(&format!("Failed to read: {e}")))?;
                let value = buf[0] != 0;
                visitor.visit_bool(value)
            }
            Schema::Union(union_schema) => {
                for variant_schema in union_schema.schemas.iter() {
                    match variant_schema {
                        Schema::Boolean => {
                            return self.deserialize_bool_with_schema(visitor, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(&format!(
                    "The union schema must have a Boolean variant: {schema:?}"
                )))
            }
            unexpected => Err(create_error(&format!(
                "Expected a boolean schema, found: {unexpected:?}"
            ))),
        }
    }

    fn deserialize_i32_with_schema<V>(
        &mut self,
        visitor: V,
        schema: &Schema,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let create_error = |cause: &str| {
            Error::new(Details::DeserializeValueWithSchema {
                value_type: "i32",
                value: format!("Cause: {cause}"),
                schema: schema.clone(),
            })
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int = zag_i32(self.reader)?;
                visitor.visit_i32(int)
            }
            Schema::Union(union_schema) => {
                for variant_schema in union_schema.schemas.iter() {
                    match variant_schema {
                        Schema::Int | Schema::TimeMillis | Schema::Date => {
                            return self.deserialize_i32_with_schema(visitor, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(&format!(
                    "The union schema must have an Int[-like] variant: {schema:?}"
                )))
            }
            unexpected => Err(create_error(&format!(
                "Expected an Int[-like] schema, found: {unexpected:?}"
            ))),
        }
    }

    fn deserialize_i64_with_schema<V>(
        &mut self,
        visitor: V,
        schema: &Schema,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let create_error = |cause: &str| {
            Details::DeserializeValueWithSchema {
                value_type: "i64",
                value: format!("Cause: {cause}"),
                schema: schema.clone(),
            }
            .into()
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let long = zag_i64(self.reader)?;
                let int = i32::try_from(long)
                    .map_err(|cause| create_error(cause.to_string().as_str()))?;
                visitor.visit_i32(int)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                let long = zag_i64(self.reader)?;
                visitor.visit_i64(long)
            }
            Schema::Union(union_schema) => {
                for variant_schema in union_schema.schemas.iter() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos => {
                            return self.deserialize_i64_with_schema(visitor, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(&format!(
                    "The union schema must have a Long[-like] variant: {schema:?}"
                )))
            }
            unexpected => Err(create_error(&format!(
                "Expected a Long[-like] schema, found: {unexpected:?}"
            ))),
        }
    }

    fn deserialize_struct_with_schema<V>(
        &'a mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
        schema: &'a Schema,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let create_error = |cause: &str| {
            Details::DeserializeValueWithSchema {
                value_type: "struct",
                value: format!("Cause: {cause}"),
                schema: schema.clone(),
            }
            .into()
        };
        dbg!(name, fields);
        match schema {
            Schema::Record(record_schema) => {
                visitor.visit_map(RecordSchemaAwareReadDeserializerStruct::new(
                    self,
                    name,
                    fields.iter(),
                    record_schema,
                ))
            }
            Schema::Union(union_schema) => {
                for variant_schema in union_schema.schemas.iter() {
                    match variant_schema {
                        Schema::Int
                        | Schema::TimeMillis
                        | Schema::Date
                        | Schema::Long
                        | Schema::TimeMicros
                        | Schema::TimestampMillis
                        | Schema::TimestampMicros
                        | Schema::TimestampNanos
                        | Schema::LocalTimestampMillis
                        | Schema::LocalTimestampMicros
                        | Schema::LocalTimestampNanos => {
                            return self.deserialize_i64_with_schema(visitor, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(&format!(
                    "The union schema must have a Long[-like] variant: {schema:?}"
                )))
            }
            unexpected => Err(create_error(&format!(
                "Expected a Long[-like] schema, found: {unexpected:?}"
            ))),
        }
    }
}

struct RecordSchemaAwareReadDeserializerStruct<'a, R: Read> {
    deser: &'a mut SchemaAwareReadDeserializer<'a, R>,
    _schema_name: &'static str,
    fields: Iter<'a, &'static str>,
    current_field: Option<&'static str>,
    record_schema: &'a crate::schema::RecordSchema,
}

impl<'a, R: Read> RecordSchemaAwareReadDeserializerStruct<'a, R> {
    fn new(
        deser: &'a mut SchemaAwareReadDeserializer<'a, R>,
        _schema_name: &'static str,
        fields: Iter<'a, &'static str>,
        record_schema: &'a crate::schema::RecordSchema,
    ) -> Self {
        Self {
            deser,
            _schema_name,
            fields,
            current_field: None,
            record_schema,
        }
    }
}

impl<'de: 'a, 'a, R: Read> de::MapAccess<'de>
    for RecordSchemaAwareReadDeserializerStruct<'a, R>
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        match self.fields.next() {
            Some(&field_name) => {
                self.current_field = Some(field_name);
                seed
                    .deserialize(StringDeserializer { input: field_name })
                    .map(Some)
            },
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match self.current_field.take() {
            Some(field_name) => {
                let schema = self.record_schema;
                let record_field = schema.lookup.get(field_name)
                    .and_then(|idx| schema.fields.get(*idx))
                    .ok_or_else(|| {
                    Error::new(Details::DeserializeValueWithSchema {
                        value_type: "struct",
                        value: format!("Field '{field_name}' not found in record schema"),
                        schema: Schema::Record(schema.clone()),
                    })
                })?;
                let field_schema = &record_field.schema;
                seed.deserialize(SchemaAwareReadDeserializer::new(
                    self.deser.reader,
                    field_schema,
                ))
            }
            None => Err(de::Error::custom("should not happen - too many values")),
        }
    }
}

// struct RecordSchemaAwareReadDeserializer<'s, R: Read> {
//     deser: &'s mut SchemaAwareReadDeserializer<'s, R>,
//     schema_name: &'static str,
//     fields: Iter<'s, &'static str>,
//     record_schema: &'s crate::schema::RecordSchema,
// }
//
// impl<'s, R: Read> RecordSchemaAwareReadDeserializer<'s, R> {
//     fn new(
//         deser: &'s mut SchemaAwareReadDeserializer<'s, R>,
//         schema_name: &'static str,
//         fields: Iter<'s, &'static str>,
//         record_schema: &'s crate::schema::RecordSchema,
//     ) -> Self {
//         Self {
//             deser,
//             schema_name,
//             fields,
//             record_schema,
//         }
//     }
// }

// impl<'de, R: Read> de::MapAccess<'de> for RecordSchemaAwareReadDeserializer<'de, R> {
//     type Error = Error;
//
//     fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
//     where
//         K: de::DeserializeSeed<'de>,
//     {
//         match self.fields.next() {
//             Some(&field_name) => seed
//                 .deserialize(StringDeserializer { input: field_name })
//                 .map(Some),
//             None => Ok(None),
//         }
//     }
//
//     fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
//     where
//         V: de::DeserializeSeed<'de>,
//     {
//         match self.fields.next() {
//             Some(&field_name) => {
//                 let field_idx = self.record_schema.lookup.get(field_name).ok_or_else(|| {
//                     return Error::new(Details::DeserializeValueWithSchema {
//                         value_type: "field",
//                         value: format!("Field '{field_name}' not found in record schema"),
//                         schema: Schema::Record(self.record_schema.clone()),
//                     });
//                 })?;
//                 let record_field = self.record_schema.fields.get(*field_idx).ok_or_else(|| {
//                     return Error::new(Details::DeserializeValueWithSchema {
//                         value_type: "field",
//                         value: format!("Field index {field_idx} out of bounds"),
//                         schema: Schema::Record(self.record_schema.clone()),
//                     });
//                 })?;
//                 let field_schema = &record_field.schema;
//                 seed.deserialize(SchemaAwareReadDeserializer::new(
//                     self.deser.reader,
//                     field_schema,
//                 ))
//             }
//             None => Err(de::Error::custom("should not happen - too many values")),
//         }
//     }
// }

#[derive(Clone)]
struct StringDeserializer<'de> {
    input: &'de str,
}

impl<'de> de::Deserializer<'de> for StringDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.input)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Details;
    use crate::{
        reader::read_avro_datum_ref,
        schema::{Schema, UnionSchema},
        util::{zig_i32, zig_i64},
    };
    use apache_avro_test_helper::TestResult;
    use std::io::Cursor;

    #[test]
    fn avro_rs_226_deserialize_bool_boolean_schema() -> TestResult {
        let schema = Schema::Boolean;

        for (byte, expected) in [(0, false), (1, true)] {
            let mut reader: &[u8] = &[byte];
            let read: bool = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, expected);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_bool_union_boolean_schema() -> TestResult {
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Boolean])?);

        for (byte, expected) in [(0, false), (1, true)] {
            let mut reader: &[u8] = &[byte];
            let read: bool = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, expected);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_bool_invalid_schema() -> TestResult {
        let schema = Schema::Long; // Using a non-boolean schema

        let mut reader: &[u8] = &[0, 1, 2];
        match read_avro_datum_ref::<bool, &[u8]>(&schema, &mut reader).map_err(Error::into_details)
        {
            Err(Details::DeserializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "bool");
                assert!(value.contains("Cause: Expected a boolean schema"));
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid schema"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_bool_union_invalid_schema() -> TestResult {
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Long])?);

        let mut reader: &[u8] = &[1, 2, 3];
        match read_avro_datum_ref::<bool, &[u8]>(&schema, &mut reader).map_err(Error::into_details)
        {
            Err(Details::DeserializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "bool");
                assert!(value.contains("The union schema must have a Boolean variant"));
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid union schema"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_int32_int_schema() -> TestResult {
        let schema = Schema::Int;

        for value in [123_i32, -1024_i32] {
            let mut writer = vec![];
            zig_i32(value, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: i32 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_int32_union_int_schema() -> TestResult {
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?);

        for value in [123_i32, -1024_i32] {
            let mut writer = vec![];
            zig_i32(value, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: i32 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_i32_invalid_schema() -> TestResult {
        let schema = Schema::Long; // Using a non-Int schema

        let mut reader: &[u8] = &[0, 1, 2];
        match read_avro_datum_ref::<i32, &[u8]>(&schema, &mut reader).map_err(Error::into_details) {
            Err(Details::DeserializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "i32");
                assert!(
                    value.contains("Cause: Expected an Int[-like] schema"),
                    "Got: {value}",
                );
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid schema"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_i32_union_invalid_schema() -> TestResult {
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Long])?);

        let mut reader: &[u8] = &[1, 2, 3];
        match read_avro_datum_ref::<i32, &[u8]>(&schema, &mut reader).map_err(Error::into_details) {
            Err(Details::DeserializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "i32");
                assert!(
                    value.contains("The union schema must have an Int[-like] variant"),
                    "Got: {value}",
                );
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid union schema"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_int64_int_schema() -> TestResult {
        let schema = Schema::TimeMillis;

        for value in [i32::MAX, -i32::MAX] {
            let mut writer = vec![];
            zig_i64(value as i64, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: i64 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value as i64);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_int64_long_schema() -> TestResult {
        let schema = Schema::TimestampMicros;

        for value in [i64::MAX, -i64::MAX] {
            let mut writer = vec![];
            zig_i64(value, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: i64 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_int64_union_int_schema() -> TestResult {
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::TimeMicros])?);

        for value in [123_i64, -1024_i64] {
            let mut writer = vec![];
            zig_i64(value, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: i64 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_i64_invalid_schema() -> TestResult {
        let schema = Schema::Uuid; // Using a non-Long schema

        let mut reader: &[u8] = &[0, 1, 2];
        match read_avro_datum_ref::<i64, &[u8]>(&schema, &mut reader).map_err(Error::into_details) {
            Err(Details::DeserializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "i64");
                assert!(
                    value.contains("Cause: Expected a Long[-like] schema"),
                    "Got: {value}",
                );
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid schema"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_i64_union_invalid_schema() -> TestResult {
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::String])?);

        let mut reader: &[u8] = &[1, 2, 3];
        match read_avro_datum_ref::<i64, &[u8]>(&schema, &mut reader).map_err(Error::into_details) {
            Err(Details::DeserializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "i64");
                assert!(
                    value.contains("The union schema must have a Long[-like] variant"),
                    "Got: {value}",
                );
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid union schema"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_u8_int_schema() -> TestResult {
        let schema = Schema::TimeMillis;

        for value in [u8::MAX, 0] {
            let mut writer = vec![];
            zig_i32(value as i32, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: u8 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_u16_int_schema() -> TestResult {
        let schema = Schema::TimeMillis;

        for value in [u16::MAX, 0] {
            let mut writer = vec![];
            zig_i32(value as i32, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: u16 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_u32_long_schema() -> TestResult {
        let schema = Schema::TimeMicros;

        for value in [u32::MAX, 0] {
            let mut writer = vec![];
            zig_i64(value as i64, &mut writer)?;
            let mut reader = Cursor::new(&writer);
            let read: u32 = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, value);
        }
        Ok(())
    }
}
