use std::borrow::Borrow;
use serde::de::Visitor;
use serde::Deserializer;
use serde_json::Value;
use crate::{Error, Schema};
use crate::error::Details;
use crate::schema::{UnionSchema, UuidSchema};
use crate::serde::deser_schema::{Config, DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS};
use crate::util::zag_i32;

pub struct DefaultDeserializer<'s, S: Borrow<Schema>> {
    default: &'s Value,
    schema: &'s Schema,
    config: &'s Config<'s, S>,
}

impl<'s, S: Borrow<Schema>> DefaultDeserializer<'s, S> {
    pub fn new(default: &'s Value, schema: &'s Schema, config: &'s Config<'s, S>) -> Result<Self, Error> {
        if let Schema::Ref { name } = schema {
            let schema = config
                .names
                .get(name)
                .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?
                .borrow();
            Self::new(default, schema, config)
        } else {
            Ok(Self {
                default,
                schema,
                config,
            })
        }
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::DeserializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: self.schema.clone(),
        })
    }

    /// Create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the schema if it is a reference.
    fn with_different_schema(mut self, schema: &'s Schema) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema {
            self.config
                .names
                .get(name)
                .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?
                .borrow()
        } else {
            schema
        };
        self.schema = schema;
        Ok(self)
    }

    /// Read the union and create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the read schema if it is a reference.
    fn with_union(self, schema: &'s UnionSchema) -> Result<Self, Error> {
        let index = zag_i32(self.reader)?;
        let variant =
            schema
                .variants()
                .get(index as usize)
                .ok_or_else(|| Details::GetUnionVariant {
                    index: index as i64,
                    num_variants: schema.variants().len(),
                })?;
        self.with_different_schema(variant)
    }
}

impl<'de, 's, S: Borrow<Schema>> Deserializer<'de> for DefaultDeserializer<'s, S> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.schema {
            Schema::Null => self.deserialize_unit(visitor),
            Schema::Boolean => self.deserialize_bool(visitor),
            Schema::Int | Schema::Date | Schema::TimeMillis => self.deserialize_i32(visitor),
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => self.deserialize_i64(visitor),
            Schema::Float => self.deserialize_f32(visitor),
            Schema::Double => self.deserialize_f64(visitor),
            Schema::Bytes
            | Schema::Fixed(_)
            | Schema::Decimal(_)
            | Schema::BigDecimal
            | Schema::Uuid(UuidSchema::Fixed(_))
            | Schema::Duration(_) => self.deserialize_byte_buf(visitor),
            Schema::String | Schema::Uuid(UuidSchema::String) => self.deserialize_string(visitor),
            Schema::Array(_) => self.deserialize_seq(visitor),
            Schema::Map(_) => self.deserialize_map(visitor),
            Schema::Union(union) => self.with_union(union)?.deserialize_any(visitor),
            Schema::Record(schema) => {
                if schema.attributes.get("org.apache.avro.rust.tuple")
                    == Some(&serde_json::Value::Bool(true))
                {
                    // This is needed because we can't tell the difference between a tuple and struct.
                    // And a tuple needs to be deserialized as a sequence
                    self.deserialize_tuple(schema.fields.len(), visitor)
                } else {
                    self.deserialize_struct(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
                }
            }
            Schema::Enum(_) => {
                self.deserialize_enum(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
            }
            Schema::Ref { .. } => unreachable!("References are resolved on deserializer creation"),
            Schema::Uuid(UuidSchema::Bytes) => panic!("Unsupported"),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn is_human_readable(&self) -> bool {
        todo!()
    }
}
