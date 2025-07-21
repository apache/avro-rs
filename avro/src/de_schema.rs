use crate::error::Details;
use crate::schema::{NamesRef, Namespace};
use crate::util::zag_i32;
use crate::{Error, Schema};
use serde::de::Visitor;
use std::io::Read;

pub struct SchemaAwareReadDeserializer<'s, R: Read> {
    reader: &'s mut R,
    root_schema: &'s Schema,
    _names: &'s NamesRef<'s>,
    _enclosing_namespace: Namespace,
}

impl<'s, R: Read> SchemaAwareReadDeserializer<'s, R> {
    pub(crate) fn new(
        reader: &'s mut R,
        root_schema: &'s Schema,
        _names: &'s NamesRef<'s>,
        _enclosing_namespace: Namespace,
    ) -> Self {
        Self {
            reader,
            root_schema,
            _names,
            _enclosing_namespace,
        }
    }
}

impl<'de, R: Read> serde::de::Deserializer<'de> for SchemaAwareReadDeserializer<'de, R> {
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
        (&mut this).deserialize_bool_with_schema(visitor, schema)
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
        (&mut this).deserialize_i32_with_schema(visitor, schema)
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
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
        todo!()
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
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

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
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
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
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

impl<'s, R: Read> SchemaAwareReadDeserializer<'s, R> {
    fn deserialize_bool_with_schema<'de, V>(
        &mut self,
        visitor: V,
        schema: &Schema,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let create_error = |cause: &str| {
            Details::SerializeValueWithSchema {
                // TODO: DeserializeValueWithSchema
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
                for (_, variant_schema) in union_schema.schemas.iter().enumerate() {
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

    fn deserialize_i32_with_schema<'de, V>(
        &mut self,
        visitor: V,
        schema: &Schema,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let create_error = |cause: &str| Error::SerializeValueWithSchema {
            // TODO: DeserializeValueWithSchema
            value_type: "i32",
            value: format!("Cause: {cause}"),
            schema: Box::new(schema.clone()),
        };

        match schema {
            Schema::Int => {
                let i = zag_i32(&mut self.reader)?;
                visitor.visit_i32(i)
            }
            Schema::Union(union_schema) => {
                for (_, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Int => {
                            return self.deserialize_i32_with_schema(visitor, variant_schema);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(&format!(
                    "The union schema must have an Int variant: {schema:?}"
                )))
            }
            unexpected => Err(create_error(&format!(
                "Expected an Int schema, found: {unexpected:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Details;
    use crate::reader::read_avro_datum_ref;
    use crate::schema::{Schema, UnionSchema};
    use crate::util::zig_i32;
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
        match read_avro_datum_ref::<bool, &[u8]>(&schema, &mut reader) {
            Err(Error(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            })) => {
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
        match read_avro_datum_ref::<bool, &[u8]>(&schema, &mut reader) {
            Err(Error(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            })) => {
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
        let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Boolean])?);

        for (byte, expected) in [(0, false), (1, true)] {
            let mut reader: &[u8] = &[byte];
            let read: bool = read_avro_datum_ref(&schema, &mut reader)?;
            assert_eq!(read, expected);
        }
        Ok(())
    }

    #[test]
    fn avro_rs_226_deserialize_i32_invalid_schema() -> TestResult {
        let schema = Schema::Long; // Using a non-boolean schema

        let mut reader: &[u8] = &[0, 1, 2];
        match read_avro_datum_ref::<i32, &[u8]>(&schema, &mut reader) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "i32");
                assert!(
                    value.contains("Cause: Expected an Int schema"),
                    "Got: {}",
                    value
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
        match read_avro_datum_ref::<i32, &[u8]>(&schema, &mut reader) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "i32");
                assert!(
                    value.contains("The union schema must have an Int variant"),
                    "Got: {}",
                    value
                );
                assert_eq!(schema.to_string(), schema.to_string());
            }
            _ => panic!("Expected an error for invalid union schema"),
        }

        Ok(())
    }
}
