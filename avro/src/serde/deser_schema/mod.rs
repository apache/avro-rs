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

use std::{borrow::Borrow, collections::HashMap, io::Read};

use serde::de::{Deserializer, Visitor};

use crate::{
    Error, Schema,
    decode::decode_len,
    error::Details,
    schema::{DecimalSchema, InnerDecimalSchema, Name, UnionSchema, UuidSchema},
    util::{zag_i32, zag_i64},
};

mod array;
mod enums;
mod identifier;
mod map;
mod record;
mod tuple;

use array::ArrayDeserializer;
use enums::PlainEnumDeserializer;
use map::MapDeserializer;
use record::RecordDeserializer;
use tuple::{ManyTupleDeserializer, OneTupleDeserializer};

use crate::serde::deser_schema::enums::UnionEnumDeserializer;

/// Configure the deserializer.
#[derive(Debug)]
pub struct Config<'s, S: Borrow<Schema>> {
    /// Any references in the schema will be resolved using this map.
    ///
    /// This map is not allowed to contain any [`Schema::Ref`], the deserializer is allowed to panic
    /// in that case.
    pub names: &'s HashMap<Name, S>,
    /// Was the data serialized with `human_readable`.
    pub human_readable: bool,
}

impl<'s, S: Borrow<Schema>> Config<'s, S> {
    /// Get the schema for this name.
    fn get_schema(&self, name: &Name) -> Result<&'s Schema, Error> {
        self.names
            .get(name)
            .map(Borrow::borrow)
            .ok_or_else(|| Details::SchemaResolutionError(name.clone()).into())
    }
}

// This needs to be implemented manually as the derive puts a bound on `S`
// which is unnecessary as a reference is always Copy.
impl<'s, S: Borrow<Schema>> Copy for Config<'s, S> {}
impl<'s, S: Borrow<Schema>> Clone for Config<'s, S> {
    fn clone(&self) -> Self {
        *self
    }
}

/// A deserializer that deserializes directly from raw Avro datum.
pub struct SchemaAwareDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    /// The schema of the data being deserialized.
    ///
    /// This schema is guaranteed to not be a [`Schema::Ref`].
    schema: &'s Schema,
    config: Config<'s, S>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> SchemaAwareDeserializer<'s, 'r, R, S> {
    /// Create a new deserializer for this schema.
    ///
    /// This will resolve a [`Schema::Ref`] to its actual schema.
    pub fn new(
        reader: &'r mut R,
        schema: &'s Schema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        if let Schema::Ref { name } = schema {
            let schema = config.get_schema(name)?;
            Ok(Self {
                reader,
                schema,
                config,
            })
        } else {
            Ok(Self {
                reader,
                schema,
                config,
            })
        }
    }

    /// Create an error for the current type being deserialized with the given message.
    ///
    /// This will also include the current schema.
    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::DeserializeSchemaAware {
            value_type: ty,
            value: error.into(),
            schema: self.schema.clone(),
        })
    }

    /// Create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the schema if it is a reference.
    fn with_different_schema(mut self, schema: &'s Schema) -> Result<Self, Error> {
        self.schema = if let Schema::Ref { name } = schema {
            self.config.get_schema(name)?
        } else {
            schema
        };
        Ok(self)
    }

    /// Read the union and create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the read schema if it is a reference.
    fn with_union(self, schema: &'s UnionSchema) -> Result<Self, Error> {
        let index = zag_i32(self.reader)?;
        let variant = schema.get_variant(index as usize)?;
        self.with_different_schema(variant)
    }

    /// Read an integer from the reader.
    ///
    /// This will check that the current schema is [`Schema::Int`] or a logical type based on that.
    /// It does not read [`Schema::Union`]s.
    fn checked_read_int(&mut self, original_ty: &'static str) -> Result<i32, Error> {
        match self.schema {
            Schema::Int | Schema::Date | Schema::TimeMillis => zag_i32(self.reader),
            _ => Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis",
            )),
        }
    }

    /// Read a long from the reader.
    ///
    /// This will check that the current schema is [`Schema::Long`] or a logical type based on that.
    /// It does not read [`Schema::Union`]s.
    fn checked_read_long(&mut self, original_ty: &'static str) -> Result<i64, Error> {
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

    /// Read a string from the reader.
    ///
    /// This does not check the current schema.
    fn read_string(&mut self) -> Result<String, Error> {
        let bytes = self.read_bytes_with_len()?;
        Ok(String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?)
    }

    /// Read a bytes from the reader.
    ///
    /// This does not check the current schema.
    fn read_bytes_with_len(&mut self) -> Result<Vec<u8>, Error> {
        let length = decode_len(self.reader)?;
        self.read_bytes(length)
    }

    /// Read `n` bytes from the reader.
    ///
    /// This does not check the current schema.
    fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; length];
        self.reader
            .read_exact(&mut buf)
            .map_err(Details::ReadBytes)?;
        Ok(buf)
    }

    /// Read `n` bytes from the reader.
    ///
    /// This does not check the current schema.
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let mut buf = [0; N];
        self.reader
            .read_exact(&mut buf)
            .map_err(Details::ReadBytes)?;
        Ok(buf)
    }
}

/// A static string that will bypass name checks in `deserialize_*` functions.
///
/// This is used so that `deserialize_any` can use the `deserialize_*` implementation which take
/// a static string.
///
/// We don't want users to abuse this feature so this value is compared by pointer address, therefore
/// a user providing the string below will not be able to skip name validation.
static DESERIALIZE_ANY: &str = "This value is compared by pointer value";
/// A static array so that `deserialize_any` can call `deserialize_*` functions.
static DESERIALIZE_ANY_FIELDS: &[&str] = &[];

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> Deserializer<'de>
    for SchemaAwareDeserializer<'s, 'r, R, S>
{
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
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
            | Schema::Uuid(UuidSchema::Fixed(_) | UuidSchema::Bytes)
            | Schema::Duration(_) => self.deserialize_byte_buf(visitor),
            Schema::String | Schema::Uuid(UuidSchema::String) => self.deserialize_string(visitor),
            Schema::Array(_) => self.deserialize_seq(visitor),
            Schema::Map(_) => self.deserialize_map(visitor),
            Schema::Union(union) => self.with_union(union)?.deserialize_any(visitor),
            Schema::Record(schema) => {
                if schema.attributes.get("org.apache.avro.rust.tuple")
                    == Some(&serde_json::Value::Bool(true))
                {
                    // This attribute is needed because we can't tell the difference between a tuple
                    // and struct, but a tuple needs to be deserialized as a sequence instead of a map.
                    self.deserialize_tuple(schema.fields.len(), visitor)
                } else {
                    self.deserialize_struct(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
                }
            }
            Schema::Enum(_) => {
                self.deserialize_enum(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
            }
            Schema::Ref { .. } => unreachable!("References are resolved on deserializer creation"),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Boolean => {
                let mut buf = [0xFF];
                self.reader
                    .read_exact(&mut buf)
                    .map_err(Details::ReadBytes)?;
                match buf[0] {
                    0 => visitor.visit_bool(false),
                    1 => visitor.visit_bool(true),
                    _ => Err(self.error("bool", format!("{} is not a valid boolean", buf[0]))),
                }
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_bool(visitor),
            _ => Err(self.error("bool", "Expected Schema::Boolean")),
        }
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_i8(visitor)
        } else {
            let int = self.checked_read_int("i8")?;
            let value = i8::try_from(int)
                .map_err(|_| self.error("i8", format!("Could not convert int ({int}) to an i8")))?;
            visitor.visit_i8(value)
        }
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_i16(visitor)
        } else {
            let int = self.checked_read_int("i16")?;
            let value = i16::try_from(int).map_err(|_| {
                self.error("i16", format!("Could not convert int ({int}) to an i16"))
            })?;
            visitor.visit_i16(value)
        }
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_i32(visitor)
        } else {
            visitor.visit_i32(self.checked_read_int("i32")?)
        }
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_i64(visitor)
        } else {
            visitor.visit_i64(self.checked_read_long("i64")?)
        }
    }

    fn deserialize_i128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "i128" => {
                visitor.visit_i128(i128::from_le_bytes(self.read_array()?))
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_i128(visitor),
            _ => Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16)"#)),
        }
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_u8(visitor)
        } else {
            let int = self.checked_read_int("u8")?;
            let value = u8::try_from(int)
                .map_err(|_| self.error("u8", format!("Could not convert int ({int}) to an u8")))?;
            visitor.visit_u8(value)
        }
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_u16(visitor)
        } else {
            let int = self.checked_read_int("u16")?;
            let value = u16::try_from(int).map_err(|_| {
                self.error("u16", format!("Could not convert int ({int}) to an u16"))
            })?;
            visitor.visit_u16(value)
        }
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema {
            self.with_union(union)?.deserialize_u32(visitor)
        } else {
            let long = self.checked_read_long("u32")?;
            let value = u32::try_from(long).map_err(|_| {
                self.error("u32", format!("Could not convert long ({long}) to an u32"))
            })?;
            visitor.visit_u32(value)
        }
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u64" => {
                visitor.visit_u64(u64::from_le_bytes(self.read_array()?))
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_u64(visitor),
            _ => Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8)"#)),
        }
    }

    fn deserialize_u128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "u128" => {
                visitor.visit_u128(u128::from_le_bytes(self.read_array()?))
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_i128(visitor),
            _ => Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16)"#)),
        }
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Float => visitor.visit_f32(f32::from_le_bytes(self.read_array()?)),
            Schema::Union(union) => self.with_union(union)?.deserialize_f32(visitor),
            _ => Err(self.error("f32", "Expected Schema::Float")),
        }
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Double => visitor.visit_f64(f64::from_le_bytes(self.read_array()?)),
            Schema::Union(union) => self.with_union(union)?.deserialize_f64(visitor),
            _ => Err(self.error("f64", "Expected Schema::Double")),
        }
    }

    fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            // A char cannot be deserialized using Schema::Uuid(UuidSchema::String) as that is at least
            // 32 characters.
            Schema::String => {
                let string = self.read_string()?;
                let mut chars = string.chars();
                let char = chars
                    .next()
                    .ok_or_else(|| self.error("char", "String is empty"))?;
                // We can't just check the string length, as that is in bytes not characters
                if chars.next().is_some() {
                    Err(self.error(
                        "char",
                        format!(r#"Read more than one character: "{string}""#),
                    ))
                } else {
                    visitor.visit_char(char)
                }
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_char(visitor),
            _ => Err(self.error("char", "Expected Schema::String")),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::String | Schema::Uuid(UuidSchema::String) => {
                visitor.visit_string(self.read_string()?)
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_string(visitor),
            _ => Err(self.error("string", "Expected Schema::String | Schema::Uuid(String)")),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Bytes | Schema::BigDecimal | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | Schema::Uuid(UuidSchema::Bytes) => {
                visitor.visit_byte_buf(self.read_bytes_with_len()?)
            }
            Schema::Fixed(fixed) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(fixed), ..}) | Schema::Uuid(UuidSchema::Fixed(fixed)) | Schema::Duration(fixed) => {
                visitor.visit_byte_buf(self.read_bytes(fixed.size)?)
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_byte_buf(visitor),
            _ => Err(self.error("bytes", "Expected Schema::Bytes | Schema::Fixed | Schema::BigDecimal | Schema::Decimal | Schema::Uuid(Fixed | Bytes) | Schema::Duration")),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Schema::Union(union) = self.schema
            && union.variants().len() == 2
            && union.is_nullable()
        {
            let index = zag_i32(self.reader)?;
            let schema = union.get_variant(index as usize)?;
            if let Schema::Null = schema {
                visitor.visit_none()
            } else {
                visitor.visit_some(self.with_different_schema(schema)?)
            }
        } else {
            Err(self.error("option", "Expected Schema::Union([Schema::Null, _])"))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Null => visitor.visit_unit(),
            Schema::Union(union) => self.with_union(union)?.deserialize_unit(visitor),
            _ => Err(self.error("unit", "Expected Schema::Null")),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Record(record) if record.fields.is_empty() && record.name.name() == name => {
                visitor.visit_unit()
            }
            Schema::Union(union) => self
                .with_union(union)?
                .deserialize_unit_struct(name, visitor),
            _ => Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == 0)"),
            )),
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Record(record) if record.fields.len() == 1 && record.name.name() == name => {
                visitor.visit_newtype_struct(self.with_different_schema(&record.fields[0].schema)?)
            }
            Schema::Union(union) => self
                .with_union(union)?
                .deserialize_newtype_struct(name, visitor),
            _ => Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == 1)"),
            )),
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Array(array) => {
                visitor.visit_seq(ArrayDeserializer::new(self.reader, array, self.config)?)
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_seq(visitor),
            _ => Err(self.error("seq", "Expected Schema::Array")),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            // `len == 0` is not possible for derived Deserialize implementations but users might use it.
            // The derived Deserialize implementations use `deserialize_unit` instead
            Schema::Null if len == 0 => visitor.visit_unit(),
            schema if len == 1 => {
                visitor.visit_seq(OneTupleDeserializer::new(self.reader, schema, self.config)?)
            }
            Schema::Record(record) if record.fields.len() == len => {
                visitor.visit_seq(ManyTupleDeserializer::new(self.reader, record, self.config))
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_tuple(len, visitor),
            _ if len == 0 => Err(self.error("tuple", "Expected Schema::Null for unit tuple")),
            _ => Err(self.error(
                "tuple",
                format!("Expected Schema::Record(fields.len() == {len}) for {len}-tuple"),
            )),
        }
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
        match self.schema {
            Schema::Record(record) if record.name.name() == name && record.fields.len() == len => {
                visitor.visit_map(RecordDeserializer::new(self.reader, record, self.config))
            }
            Schema::Union(union) => self
                .with_union(union)?
                .deserialize_tuple_struct(name, len, visitor),
            _ => Err(self.error(
                "tuple struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len})"),
            )),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Map(map) => {
                visitor.visit_map(MapDeserializer::new(self.reader, map, self.config)?)
            }
            Schema::Record(record) => {
                // Needed for flattened structs which are (de)serialized as maps
                visitor.visit_map(RecordDeserializer::new(self.reader, record, self.config))
            }
            Schema::Union(union) => self.with_union(union)?.deserialize_map(visitor),
            _ => Err(self.error("map", "Expected Schema::Map")),
        }
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
        match self.schema {
            // Checking that the amount of fields match does not work because of `skip_deserializing`
            Schema::Record(record)
                if record.name.name() == name || name.as_ptr() == DESERIALIZE_ANY.as_ptr() =>
            {
                visitor.visit_map(RecordDeserializer::new(self.reader, record, self.config))
            }
            Schema::Union(union) => self
                .with_union(union)?
                .deserialize_struct(name, fields, visitor),
            _ => Err(self.error("struct", format!("Expected Schema::Record(name: {name})"))),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        // This also includes aliases, so can't be used to check the amount of symbols
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::Enum(schema) => {
                visitor.visit_enum(PlainEnumDeserializer::new(self.reader, schema))
            }
            Schema::Union(union) => {
                visitor.visit_enum(UnionEnumDeserializer::new(self.reader, union, self.config))
            }
            _ => Err(self.error("enum", "Expected Schema::Enum | Schema::Union")),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.schema {
            Schema::String => self.deserialize_string(visitor),
            _ => Err(self.error("identifier", "Expected Schema::String")),
        }
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // TODO: Add `Seek` bound and skip over ignored data
        self.deserialize_any(visitor)
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}
