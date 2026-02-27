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

use std::fmt::{Debug, Formatter};
use std::io::Read;

use serde::de::Visitor;

use super::{Config, DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, SchemaAwareDeserializer};
use crate::error::Details;
use crate::schema::{SchemaKind, UuidSchema};
use crate::util::zag_i32;
use crate::{Error, Schema, schema::UnionSchema};

pub struct UnionDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s UnionSchema,
    config: Config<'s>,
    variant: &'s Schema,
}

impl<'s, 'r, R: Read> Debug for UnionDeserializer<'s, 'r, R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionDeserializer")
            .field("schema", self.schema)
            .field("variant", &self.variant)
            .finish()
    }
}

impl<'s, 'r, R: Read> UnionDeserializer<'s, 'r, R> {
    pub fn new(
        reader: &'r mut R,
        schema: &'s UnionSchema,
        config: Config<'s>,
    ) -> Result<Self, Error> {
        let index = zag_i32(reader)?;
        let variant =
            schema
                .variants()
                .get(index as usize)
                .ok_or_else(|| Details::GetUnionVariant {
                    index: index as i64,
                    num_variants: schema.variants().len(),
                })?;
        Ok(Self {
            reader,
            schema,
            config,
            variant,
        })
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::DeserializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: Schema::Union(self.schema.clone()),
        })
    }
}

impl<'de, 's, 'r, R: Read> serde::Deserializer<'de> for UnionDeserializer<'s, 'r, R> {
    type Error = Error;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_any: {self:?}");
        match self.variant {
            Schema::Null => visitor.visit_unit(),
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
            Schema::Record(_) => {
                self.deserialize_struct(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
            }
            Schema::Enum(_) => {
                self.deserialize_enum(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
            }
            Schema::Ref { name } => {
                let schema = self
                    .config
                    .names
                    .get(name)
                    .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?;
                self.variant = schema;
                self.deserialize_any(visitor)
            }
            Schema::Union(_) => Err(self.error("any", "Nested unions are not supported")),
            Schema::Uuid(UuidSchema::Bytes) => panic!("Unsupported"),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_bool: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_bool(visitor)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_i8: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_i8(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_i16: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_i16(visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_i32: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_i32(visitor)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_i64: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_i64(visitor)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_i128: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_i128(visitor)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_u8: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_u8(visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_u16: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_u16(visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_u32: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_u32(visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_u64: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_u64(visitor)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_u128: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_u128(visitor)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_f32: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_f32(visitor)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_f64: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_f64(visitor)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_char: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_char(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_str: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_str(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_string: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_string(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_bytes: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_bytes(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_byte_buf: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_byte_buf(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_option: {self:?}");
        if self.schema.variants().len() == 2 && self.schema.index().get(&SchemaKind::Null).is_some()
        {
            match self.variant {
                Schema::Null => visitor.visit_none(),
                schema => visitor.visit_some(SchemaAwareDeserializer::new(
                    self.reader,
                    schema,
                    self.config,
                )?),
            }
        } else {
            Err(self.error(
                "option",
                "Expected Schema::Union(variants.contains(Schema::Null), self.variants.len() == 2)",
            ))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_unit: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_unit(visitor)
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_unit_struct: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_unit_struct(name, visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_newtype_struct: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_newtype_struct(name, visitor)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_seq: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_seq(visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_tuple: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_tuple_struct: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_map: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_map(visitor)
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
        println!("deserialize_struct: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_struct(name, fields, visitor)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_enum: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_enum(name, variants, visitor)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("deserialize_identifier: {self:?}");
        SchemaAwareDeserializer::new(self.reader, self.variant, self.config)?
            .deserialize_identifier(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // TODO: We can do something more efficient here
        println!("deserialize_ignored_any: {self:?}");
        self.deserialize_any(visitor)
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}
