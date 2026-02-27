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

mod array;
mod enums;
mod map;
mod record;
mod tuple;
mod union;

use std::fmt::{Debug, Formatter};
use std::io::Read;

use serde::Deserializer;

use crate::serde::deser_schema::enums::PlainEnumAccess;
use crate::serde::deser_schema::tuple::{
    ManyTupleDeserializer, OneTupleDeserializer, UnitTupleDeserializer,
};
use crate::serde::deser_schema::union::UnionDeserializer;
use crate::{
    Error, Schema,
    decode::decode_len,
    error::Details,
    schema::{DecimalSchema, InnerDecimalSchema, Names, SchemaKind, UuidSchema},
    serde::deser_schema::{
        array::ArrayDeserializer, map::MapDeserializer, record::RecordDeserializer,
    },
    util::{zag_i32, zag_i64},
};

#[derive(Debug, Clone, Copy)]
pub struct Config<'s> {
    /// All names that can be referenced in the schema being used for serialisation.
    pub names: &'s Names,
    /// Should `Serialize` implementations pick a human-readable format.
    ///
    /// It is recommended to set this to `false` as it results in compacter output.
    pub human_readable: bool,
}

pub struct SchemaAwareDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s>,
}

impl<'s, 'r, R: Read> Debug for SchemaAwareDeserializer<'s, 'r, R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaAwareDeserializer")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<'s, 'r, R: Read> SchemaAwareDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s Schema, config: Config<'s>) -> Result<Self, Error> {
        if let Schema::Ref { name } = schema {
            let schema = config
                .names
                .get(name)
                .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?;
            Self::new(reader, schema, config)
        } else {
            Ok(Self {
                reader,
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
        } else {
            schema
        };
        self.schema = schema;
        Ok(self)
    }

    fn read_int(&mut self, original_ty: &'static str) -> Result<i32, Error> {
        match self.schema {
            Schema::Int | Schema::Date | Schema::TimeMillis => zag_i32(self.reader),
            _ => Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis",
            )),
        }
    }

    fn read_long(&mut self, original_ty: &'static str) -> Result<i64, Error> {
        match self.schema {
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
            | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => zag_i64(self.reader),
            _ => Err(self.error(
                original_ty,
                "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}",
            )),
        }
    }

    fn read_string(&mut self) -> Result<String, Error> {
        let bytes = self.read_bytes_with_len()?;
        Ok(String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?)
    }

    fn read_bytes_with_len(&mut self) -> Result<Vec<u8>, Error> {
        let length = decode_len(self.reader)?;
        self.read_bytes(length)
    }

    fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; length];
        self.reader
            .read_exact(&mut buf)
            .map_err(Details::ReadBytes)?;
        Ok(buf)
    }
}

const DESERIALIZE_ANY: &str = "This value is compared by pointer value";
const DESERIALIZE_ANY_FIELDS: &[&str] = &[];

impl<'de, 's, 'r, R: Read> Deserializer<'de> for SchemaAwareDeserializer<'s, 'r, R> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
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
            Schema::Union(union) => {
                UnionDeserializer::new(self.reader, union, self.config)?.deserialize_any(visitor)
            }
            Schema::Record(_) => {
                self.deserialize_struct(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
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
        V: serde::de::Visitor<'de>,
    {
        match self.schema {
            Schema::Boolean => {
                let mut buf = [0];
                self.reader
                    .read_exact(&mut buf)
                    .map_err(Details::ReadBytes)?;
                // TODO: The TryFrom implementation wasn't working??
                let boolean = match buf[0] {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(
                            self.error("bool", format!("{} is not a valid boolean", buf[0]))
                        );
                    }
                };
                visitor.visit_bool(boolean)
            }
            _ => Err(self.error("bool", "Expected a Schema::Boolean")),
        }
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_int("i8")?;
        let value = i8::try_from(int)
            .map_err(|_| self.error("i8", format!("Could not convert int ({int}) to an i8")))?;
        visitor.visit_i8(value)
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_int("i16")?;
        let value = i16::try_from(int)
            .map_err(|_| self.error("i16", format!("Could not convert int ({int}) to an i16")))?;
        visitor.visit_i16(value)
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_int("i32")?;
        let value = i32::try_from(int)
            .map_err(|_| self.error("i32", format!("Could not convert int ({int}) to an i32")))?;
        visitor.visit_i32(value)
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_long("i64")?;
        let value = i64::try_from(int)
            .map_err(|_| self.error("i64", format!("Could not convert int ({int}) to an i64")))?;
        visitor.visit_i64(value)
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_int("u8")?;
        let value = u8::try_from(int)
            .map_err(|_| self.error("u8", format!("Could not convert int ({int}) to an u8")))?;
        visitor.visit_u8(value)
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_int("u16")?;
        let value = u16::try_from(int)
            .map_err(|_| self.error("u16", format!("Could not convert int ({int}) to an u16")))?;
        visitor.visit_u16(value)
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let int = self.read_long("u32")?;
        let value = u32::try_from(int)
            .map_err(|_| self.error("u32", format!("Could not convert int ({int}) to an u32")))?;
        visitor.visit_u32(value)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Fixed(fixed) = self.schema
            && fixed.size == 8
            && fixed.name.name() == "u64"
        {
            let mut buf = [0; 8];
            self.reader
                .read_exact(&mut buf)
                .map_err(Details::ReadBytes)?;
            visitor.visit_u64(u64::from_le_bytes(buf))
        } else {
            Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8)"#))
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Float = self.schema {
            let mut buf = [0; 4];
            self.reader
                .read_exact(&mut buf)
                .map_err(Details::ReadBytes)?;
            visitor.visit_f32(f32::from_le_bytes(buf))
        } else {
            Err(self.error("f32", r#"Expected Schema::Float)"#))
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Double = self.schema {
            let mut buf = [0; 8];
            self.reader
                .read_exact(&mut buf)
                .map_err(Details::ReadBytes)?;
            visitor.visit_f64(f64::from_le_bytes(buf))
        } else {
            Err(self.error("f64", r#"Expected Schema::Double)"#))
        }
    }

    fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::String = self.schema {
            let string = self.read_string()?;
            let mut chars = string.chars();
            if let Some(character) = chars.next() {
                if chars.next().is_some() {
                    Err(self.error("char", "String contains more than one character"))
                } else {
                    visitor.visit_char(character)
                }
            } else {
                Err(self.error("char", "String is empty"))
            }
        } else {
            Err(self.error("char", "Expected Schema::String"))
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::String = self.schema {
            let string = self.read_string()?;
            visitor.visit_string(string)
        } else {
            Err(self.error("string", "Expected Schema::String"))
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.schema {
            Schema::Bytes | Schema::BigDecimal | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, .. }) => {
                let bytes = self.read_bytes_with_len()?;
                visitor.visit_byte_buf(bytes)
            }
            Schema::Fixed(fixed) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(fixed), ..}) | Schema::Uuid(UuidSchema::Fixed(fixed)) | Schema::Duration(fixed) => {
                let bytes = self.read_bytes(fixed.size)?;
                visitor.visit_byte_buf(bytes)
            }
            _ => Err(self.error("bytes", "Expected Schema::Bytes | Schema::Fixed | Schema::BigDecimal | Schema::Decimal | Schema::Uuid(Fixed) | Schema::Duration"))
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema
            && union.variants().len() == 2
            && let Some(null_index) = union.index().get(&SchemaKind::Null).copied()
        {
            let index = zag_i32(self.reader)?;
            if index < 0 || index > 1 {
                return Err(self.error("option", format!("Invalid union index {index}")));
            }
            let index = index as usize;
            if index == null_index {
                visitor.visit_none()
            } else {
                visitor.visit_some(self.with_different_schema(&union.variants()[index])?)
            }
        } else {
            Err(self.error("option", "Expected Schema::Union([Null, _])"))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Null = self.schema {
            visitor.visit_unit()
        } else {
            Err(self.error("unit", "Expected Schema::Null"))
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Record(record) = self.schema
            && record.fields.len() == 0
            && record.name.name() == name
        {
            visitor.visit_unit()
        } else {
            Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == 0)"),
            ))
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Record(record) = self.schema
            && record.fields.len() == 1
            && record.name.name() == name
        {
            visitor.visit_newtype_struct(self.with_different_schema(&record.fields[0].schema)?)
        } else {
            Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == 1)"),
            ))
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Array(array) = self.schema {
            visitor.visit_seq(ArrayDeserializer::new(self.reader, array, self.config))
        } else {
            Err(self.error("array", "Expected Schema::Array"))
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        println!("deserialize_tuple(len: {len}): {self:?}");
        match self.schema {
            Schema::Null if len == 0 => visitor.visit_seq(UnitTupleDeserializer),
            schema if len == 1 => {
                visitor.visit_seq(OneTupleDeserializer::new(self.reader, schema, self.config))
            }
            Schema::Record(record) if record.fields.len() == len => {
                visitor.visit_seq(ManyTupleDeserializer::new(self.reader, record, self.config))
            }
            _ if len == 0 => Err(self.error("tuple", "Expected Schema::Null for unit tuple")),
            _ => Err(self.error("tuple", format!("Expected Schema::Record for {len}-tuple"))),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Record(record) = self.schema
            && record.name.name() == name
            && record.fields.len() == len
        {
            visitor.visit_map(RecordDeserializer::new(self.reader, record, self.config))
        } else {
            Err(self.error(
                "tuple struct",
                format!("Expected Schema::Record(fields.len() == {len})"),
            ))
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Map(map) = self.schema {
            visitor.visit_map(MapDeserializer::new(self.reader, map, self.config))
        } else {
            Err(self.error("map", "Expected Schema::Map"))
        }
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        println!("deserialize_struct(name: {name}, fields = {_fields:?}): {self:?}");
        if let Schema::Record(record) = self.schema
            && record.name.name() == name
        {
            visitor.visit_map(RecordDeserializer::new(self.reader, record, self.config))
        } else {
            Err(self.error("struct", format!("Expected Schema::Record(name: {name})")))
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        println!("deserialize_enum(name: {name}, variants: {variants:?}): {self:?}");
        match self.schema {
            Schema::Enum(schema) => visitor.visit_enum(PlainEnumAccess::new(self.reader, schema)),
            _ => panic!(
                "deserializing enum, name: {name}, variants: {variants:#?}, {:?}",
                self.schema
            ),
        }
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Fixed(fixed) = self.schema
            && fixed.size == 16
            && fixed.name.name() == "i128"
        {
            let mut buf = [0; 16];
            self.reader
                .read_exact(&mut buf)
                .map_err(Details::ReadBytes)?;
            visitor.visit_i128(i128::from_le_bytes(buf))
        } else {
            Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16)"#))
        }
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Schema::Fixed(fixed) = self.schema
            && fixed.size == 16
            && fixed.name.name() == "u128"
        {
            let mut buf = [0; 16];
            self.reader
                .read_exact(&mut buf)
                .map_err(Details::ReadBytes)?;
            visitor.visit_u128(u128::from_le_bytes(buf))
        } else {
            Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16)"#))
        }
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        panic!("deserialize_identifier: {:?}", self.schema);
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // TODO: We can probably do something more efficient, but that might need the Seek trait bound
        self.deserialize_any(visitor)
    }
}
