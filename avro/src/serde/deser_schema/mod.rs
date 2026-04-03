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

mod block;
mod enums;
mod identifier;
mod record;
mod tuple;

use block::BlockDeserializer;
use enums::PlainEnumDeserializer;
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
        let index = usize::try_from(index).map_err(|e| Details::ConvertI32ToUsize(e, index))?;
        let variant = schema.get_variant(index)?;
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
            Schema::Union(union) => self.with_union(union)?.deserialize_u128(visitor),
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
            let index = usize::try_from(index).map_err(|e| Details::ConvertI32ToUsize(e, index))?;
            let schema = union.get_variant(index)?;
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
                visitor.visit_seq(BlockDeserializer::array(self.reader, array, self.config)?)
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
                visitor.visit_seq(ManyTupleDeserializer::new(self.reader, record, self.config))
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
                visitor.visit_map(BlockDeserializer::map(self.reader, map, self.config)?)
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

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use apache_avro_test_helper::TestResult;
    use bigdecimal::BigDecimal;
    use num_bigint::BigInt;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize, de::DeserializeOwned};
    use serde_bytes::ByteBuf;
    use uuid::Uuid;

    use super::*;
    use crate::{
        AvroResult, AvroSchema, Decimal, reader::datum::GenericDatumReader,
        writer::datum::GenericDatumWriter,
    };

    #[expect(
        clippy::needless_pass_by_value,
        reason = "Significantly complicates the trait bounds"
    )]
    #[track_caller]
    fn assert_roundtrip<T>(value: T, schema: &Schema, schemata: Vec<&Schema>) -> AvroResult<()>
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug + Clone,
    {
        let buf = GenericDatumWriter::builder(schema)
            .schemata(schemata.clone())?
            .build()?
            .write_ser_to_vec(&value)?;

        let decoded_value: T = GenericDatumReader::builder(schema)
            .writer_schemata(schemata)?
            .build()?
            .read_deser(&mut &buf[..])?;

        assert_eq!(decoded_value, value);

        Ok(())
    }

    #[test]
    fn avro_3955_decode_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum SourceType {
            Sozu,
            Haproxy,
            HaproxyTcp,
        }
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct AccessLog {
            source: SourceType,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "AccessLog",
            "namespace": "com.clevercloud.accesslogs.common.avro",
            "type": "record",
            "fields": [{
                "name": "source",
                "type": {
                    "type": "enum",
                    "name": "SourceType",
                    "items": "string",
                    "symbols": ["SOZU", "HAPROXY", "HAPROXY_TCP"]
                }
            }]
        }"#,
        )?;

        let data = AccessLog {
            source: SourceType::Sozu,
        };

        assert_roundtrip(data, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_decode_enum_invalid_data() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum SourceType {
            Sozu,
            Haproxy,
            HaproxyTcp,
        }
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct AccessLog {
            source: SourceType,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "AccessLog",
            "namespace": "com.clevercloud.accesslogs.common.avro",
            "type": "record",
            "fields": [{
                "name": "source",
                "type": {
                    "type": "enum",
                    "name": "SourceType",
                    "items": "string",
                    "symbols": ["SOZU", "HAPROXY", "HAPROXY_TCP"]
                }
            }]
        }"#,
        )?;

        // Contains index 3 (4th symbol)
        let data_with_unknown_index = &[6u8];

        let error = GenericDatumReader::builder(&schema)
            .build()?
            .read_deser::<AccessLog>(&mut &data_with_unknown_index[..])
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Enum symbol index out of bounds: got 3 but there are only 3 variants"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_512_nested_struct() -> TestResult {
        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
        struct Test {
            a: i64,
            b: String,
            c: Decimal,
        }

        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
        struct TestInner {
            a: Test,
            b: i32,
        }

        let schemas = Schema::parse_list([
            r#"{
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": "long"
                },
                {
                    "name": "b",
                    "type": "string"
                },
                {
                    "name": "c",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2
                    }
                }
            ]
        }"#,
            r#"{
            "name": "TestInner",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": "Test"
                },
                {
                    "name": "b",
                    "type": "int"
                }
            ]
        }"#,
        ])?;

        let test = Test {
            a: 27,
            b: "foo".to_string(),
            c: Decimal::from(vec![1, 24]),
        };

        assert_roundtrip(test.clone(), &schemas[0], Vec::new())?;

        let test_inner = TestInner { a: test, b: 35 };

        assert_roundtrip(test_inner, &schemas[1], vec![&schemas[0]])?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_external_unit_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        pub enum UnitExternalEnum {
            Val1,
            Val2,
        }
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestUnitExternalEnum {
            a: UnitExternalEnum,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestUnitExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": {
                    "type": "enum",
                    "name": "UnitExternalEnum",
                    "items": "string",
                    "symbols": ["Val1", "Val2"]
                }
            }]
        }"#,
        )?;

        let alt_schema = Schema::parse_str(
            r#"{
            "name": "TestUnitExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": [
                    {
                        "name": "Val1",
                        "type": "record",
                        "fields": []
                    },
                    {
                        "name": "Val2",
                        "type": "record",
                        "fields": []
                    }
                ]
            }]
        }"#,
        )?;

        let value = TestUnitExternalEnum {
            a: UnitExternalEnum::Val1,
        };
        assert_roundtrip(value.clone(), &schema, Vec::new())?;
        assert_roundtrip(value, &alt_schema, Vec::new())?;

        let value = TestUnitExternalEnum {
            a: UnitExternalEnum::Val2,
        };
        assert_roundtrip(value.clone(), &alt_schema, Vec::new())?;
        assert_roundtrip(value, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_internal_unit_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        #[serde(tag = "t")]
        pub enum UnitInternalEnum {
            Val1,
            Val2,
        }
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestUnitInternalEnum {
            a: UnitInternalEnum,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestUnitInternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": {
                    "type": "record",
                    "name": "UnitInternalEnum",
                    "fields": [{
                        "name": "t",
                        "type": "string"
                    }]
                }
            }]
        }"#,
        )?;

        let value = TestUnitInternalEnum {
            a: UnitInternalEnum::Val1,
        };
        assert_roundtrip(value, &schema, Vec::new())?;

        let value = TestUnitInternalEnum {
            a: UnitInternalEnum::Val2,
        };
        assert_roundtrip(value, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_adjacent_unit_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        #[serde(tag = "t", content = "v")]
        pub enum UnitAdjacentEnum {
            Val1,
            Val2,
        }
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestUnitAdjacentEnum {
            a: UnitAdjacentEnum,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestUnitAdjacentEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": {
                    "type": "record",
                    "name": "UnitAdjacentEnum",
                    "fields": [
                        {
                            "name": "t",
                            "type": {
                                "type": "enum",
                                "name": "t",
                                "symbols": ["Val1", "Val2"]
                            }
                        },
                        {
                            "name": "v",
                            "default": null,
                            "type": ["null"]
                        }
                    ]
                }
            }]
        }"#,
        )?;

        let value = TestUnitAdjacentEnum {
            a: UnitAdjacentEnum::Val1,
        };
        assert_roundtrip(value, &schema, Vec::new())?;

        let value = TestUnitAdjacentEnum {
            a: UnitAdjacentEnum::Val2,
        };
        assert_roundtrip(value, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_untagged_unit_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        #[serde(untagged)]
        pub enum UnitUntaggedEnum {
            Val1,
            Val2,
        }
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestUnitUntaggedEnum {
            a: UnitUntaggedEnum,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestUnitUntaggedEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": ["null"]
            }]
        }"#,
        )?;

        let value1 = TestUnitUntaggedEnum {
            a: UnitUntaggedEnum::Val1,
        };
        assert_roundtrip(value1.clone(), &schema, Vec::new())?;

        let value2 = TestUnitUntaggedEnum {
            a: UnitUntaggedEnum::Val2,
        };
        let buf = GenericDatumWriter::builder(&schema)
            .build()?
            .write_ser_to_vec(&value1)?;

        let decoded_value: TestUnitUntaggedEnum = GenericDatumReader::builder(&schema)
            .build()?
            .read_deser(&mut &buf[..])?;

        // Val2 cannot troundtrip. All unit variants are serialized to the same null.
        // This also doesn't roundtrip in serde_json.
        assert_ne!(value2, decoded_value);
        assert_eq!(decoded_value, value1);

        Ok(())
    }

    #[test]
    fn avro_rs_512_mixed_enum() -> TestResult {
        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
        struct TestNullExternalEnum {
            a: NullExternalEnum,
        }

        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
        enum NullExternalEnum {
            Val1,
            Val2(),
            Val3(()),
            Val4(u64),
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestNullExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": [
                    {
                        "name": "Val1",
                        "type": "record",
                        "fields": []
                    },
                    {
                        "name": "Val2",
                        "type": "record",
                        "fields": []
                    },
                    {
                        "name": "Val3",
                        "type": "record",
                        "org.apache.avro.rust.union_of_records": true,
                        "fields": [{
                            "name": "field_0",
                            "type": "null"
                        }]
                    },
                    {
                        "name": "Val4",
                        "type": "record",
                        "org.apache.avro.rust.union_of_records": true,
                        "fields": [{
                            "name": "field_0",
                            "type": {
                                "type": "fixed",
                                "name": "u64",
                                "size": 8
                            }
                        }]
                    }
                ]
            }]
        }"#,
        )?;

        let data = [
            TestNullExternalEnum {
                a: NullExternalEnum::Val1,
            },
            TestNullExternalEnum {
                a: NullExternalEnum::Val2(),
            },
            TestNullExternalEnum {
                a: NullExternalEnum::Val2(),
            },
            TestNullExternalEnum {
                a: NullExternalEnum::Val3(()),
            },
            TestNullExternalEnum {
                a: NullExternalEnum::Val4(123),
            },
        ];

        for value in data {
            assert_roundtrip(value, &schema, Vec::new())?;
        }

        Ok(())
    }

    #[test]
    fn avro_rs_512_single_value_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestSingleValueExternalEnum {
            a: SingleValueExternalEnum,
        }

        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        enum SingleValueExternalEnum {
            Double(f64),
            String(String),
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestSingleValueExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": [
                    {
                        "name": "Double",
                        "type": "record",
                        "org.apache.avro.rust.union_of_records": true,
                        "fields": [{
                            "name": "field_0",
                            "type": "double"
                        }]
                    },
                    {
                        "name": "String",
                        "type": "record",
                        "org.apache.avro.rust.union_of_records": true,
                        "fields": [{
                            "name": "field_0",
                            "type": "string"
                        }]
                    }
                ]
            }]
        }"#,
        )?;

        let alt_schema = Schema::parse_str(
            r#"{
            "name": "TestSingleValueExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": ["double", "string"]
            }]
        }"#,
        )?;

        let double = TestSingleValueExternalEnum {
            a: SingleValueExternalEnum::Double(64.0),
        };
        assert_roundtrip(double.clone(), &schema, Vec::new())?;
        assert_roundtrip(double, &alt_schema, Vec::new())?;

        let string = TestSingleValueExternalEnum {
            a: SingleValueExternalEnum::String("test".to_string()),
        };
        assert_roundtrip(string.clone(), &schema, Vec::new())?;
        assert_roundtrip(string, &alt_schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_struct_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestStructExternalEnum {
            a: StructExternalEnum,
        }

        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        enum StructExternalEnum {
            Val1 { x: f32, y: f32 },
            Val2 { x: f32, y: f32 },
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestStructExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": [
                    {
                        "name": "Val1",
                        "type": "record",
                        "fields": [
                            {
                                "name": "x",
                                "type": "float"
                            },
                            {
                                "name": "y",
                                "type": "float"
                            }
                        ]
                    },
                    {
                        "name": "Val2",
                        "type": "record",
                        "fields": [
                            {
                                "name": "x",
                                "type": "float"
                            },
                            {
                                "name": "y",
                                "type": "float"
                            }
                        ]
                    }
                ]
            }]
        }"#,
        )?;

        let value1 = TestStructExternalEnum {
            a: StructExternalEnum::Val1 { x: 1.0, y: 2.0 },
        };

        assert_roundtrip(value1, &schema, Vec::new())?;

        let value2 = TestStructExternalEnum {
            a: StructExternalEnum::Val2 { x: 2.0, y: 1.0 },
        };

        assert_roundtrip(value2, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_struct_flatten() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct S1 {
            f1: String,
            #[serde(flatten)]
            inner: S2,
        }

        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct S2 {
            f2: String,
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "S1",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "string"
                },
                {
                    "name": "f2",
                    "type": "string"
                }
            ]
        }"#,
        )?;

        let value = S1 {
            f1: "Hello".to_owned(),
            inner: S2 {
                f2: "World".to_owned(),
            },
        };

        assert_roundtrip(value, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_tuple_enum() -> TestResult {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        struct TestTupleExternalEnum {
            a: TupleExternalEnum,
        }

        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
        enum TupleExternalEnum {
            Val1(f32, f32),
            Val2(f32, f32, f32),
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "TestTupleExternalEnum",
            "type": "record",
            "fields": [{
                "name": "a",
                "type": [
                    {
                        "name": "Val1",
                        "type": "record",
                        "fields": [
                            {
                                "name": "field_0",
                                "type": "float"
                            },
                            {
                                "name": "field_1",
                                "type": "float"
                            }
                        ]
                    },
                    {
                        "name": "Val2",
                        "type": "record",
                        "fields": [
                            {
                                "name": "field_0",
                                "type": "float"
                            },
                            {
                                "name": "field_1",
                                "type": "float"
                            },
                            {
                                "name": "field_2",
                                "type": "float"
                            }
                        ]
                    }
                ]
            }]
        }"#,
        )?;

        let value1 = TestTupleExternalEnum {
            a: TupleExternalEnum::Val1(1.0, 2.0),
        };

        assert_roundtrip(value1, &schema, Vec::new())?;

        let value2 = TestTupleExternalEnum {
            a: TupleExternalEnum::Val2(3.0, 2.0, 1.0),
        };

        assert_roundtrip(value2, &schema, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_bool() -> TestResult {
        let schema = Schema::Boolean;
        assert_roundtrip(true, &schema, Vec::new())?;
        assert_roundtrip(false, &Schema::union(vec![schema.clone()])?, Vec::new())?;
        assert_roundtrip(true, &schema, Vec::new())?;
        assert_roundtrip(false, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_i8() -> TestResult {
        let schema = Schema::Int;
        assert_roundtrip(1i8, &schema, Vec::new())?;
        assert_roundtrip(1i8, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_i16() -> TestResult {
        let schema = Schema::Int;
        assert_roundtrip(1i16, &schema, Vec::new())?;
        assert_roundtrip(1i16, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_i32() -> TestResult {
        let schema = Schema::Int;
        assert_roundtrip(1i32, &schema, Vec::new())?;
        assert_roundtrip(1i32, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_i64() -> TestResult {
        let schema = Schema::Long;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_i128() -> TestResult {
        let schema = i128::get_schema();
        assert_roundtrip(1i128, &schema, Vec::new())?;
        assert_roundtrip(1i128, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_u8() -> TestResult {
        let schema = Schema::Int;
        assert_roundtrip(1u8, &schema, Vec::new())?;
        assert_roundtrip(1u8, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_u16() -> TestResult {
        let schema = Schema::Int;
        assert_roundtrip(1u16, &schema, Vec::new())?;
        assert_roundtrip(1u16, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_u32() -> TestResult {
        let schema = Schema::Long;
        assert_roundtrip(1u32, &schema, Vec::new())?;
        assert_roundtrip(1u32, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_u64() -> TestResult {
        let schema = u64::get_schema();
        assert_roundtrip(1u64, &schema, Vec::new())?;
        assert_roundtrip(1u64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_u128() -> TestResult {
        let schema = u128::get_schema();
        assert_roundtrip(1u128, &schema, Vec::new())?;
        assert_roundtrip(1u128, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_f32() -> TestResult {
        let schema = Schema::Float;
        assert_roundtrip(1.0f32, &schema, Vec::new())?;
        assert_roundtrip(1.0f32, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_f64() -> TestResult {
        let schema = Schema::Double;
        assert_roundtrip(1.0f64, &schema, Vec::new())?;
        assert_roundtrip(1.0f64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_string() -> TestResult {
        let schema = Schema::String;
        assert_roundtrip(String::from("avro"), &schema, Vec::new())?;
        assert_roundtrip(
            String::from("avro"),
            &Schema::union(vec![schema])?,
            Vec::new(),
        )?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_bytes() -> TestResult {
        let schema = Schema::Bytes;

        let bytes = ByteBuf::from(vec![b'a', b'v', b'r', b'o']);
        assert_roundtrip(bytes.clone(), &schema, Vec::new())?;
        assert_roundtrip(bytes.clone(), &Schema::union(vec![schema])?, Vec::new())?;

        let schema = Schema::fixed("four".parse()?, 4).build();
        assert_roundtrip(bytes.clone(), &schema, Vec::new())?;
        assert_roundtrip(bytes.clone(), &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_unit() -> TestResult {
        let schema = Schema::Null;
        assert_roundtrip((), &schema, Vec::new())?;
        assert_roundtrip((), &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_date() -> TestResult {
        let schema = Schema::Date;
        assert_roundtrip(1i32, &schema, Vec::new())?;
        assert_roundtrip(1i32, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_time_millis() -> TestResult {
        let schema = Schema::TimeMillis;
        assert_roundtrip(1i32, &schema, Vec::new())?;
        assert_roundtrip(1i32, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_time_micros() -> TestResult {
        let schema = Schema::TimeMicros;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_timestamp_millis() -> TestResult {
        let schema = Schema::TimestampMillis;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_timestamp_micros() -> TestResult {
        let schema = Schema::TimestampMicros;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_3916_timestamp_nanos() -> TestResult {
        let schema = Schema::TimestampNanos;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_3853_local_timestamp_millis() -> TestResult {
        let schema = Schema::LocalTimestampMillis;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_3853_local_timestamp_micros() -> TestResult {
        let schema = Schema::LocalTimestampMicros;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_3916_local_timestamp_nanos() -> TestResult {
        let schema = Schema::LocalTimestampNanos;
        assert_roundtrip(1i64, &schema, Vec::new())?;
        assert_roundtrip(1i64, &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_512_uuid() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "logicalType": "uuid",
            "size": 16,
            "name": "uuid"
        }"#,
        )?;

        let alt_schema = Schema::Uuid(UuidSchema::String);

        let uuid = Uuid::parse_str("9ec535ff-3e2a-45bd-91d3-0a01321b5a49")?;

        assert_roundtrip(uuid, &schema, Vec::new())?;
        assert_roundtrip(uuid, &Schema::union(vec![schema])?, Vec::new())?;

        let buf = GenericDatumWriter::builder(&alt_schema)
            .human_readable(true)
            .build()?
            .write_ser_to_vec(&uuid)?;

        let decoded_value: Uuid = GenericDatumReader::builder(&alt_schema)
            .human_readable(true)
            .build()?
            .read_deser(&mut &buf[..])?;

        assert_eq!(decoded_value, uuid);

        Ok(())
    }

    #[test]
    fn avro_3892_deserialize_bytes_from_decimal() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 4,
            "scale": 2
        }"#,
        )?;
        let schema_option = Schema::parse_str(
            r#"[
            "null",
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 4,
                "scale": 2
            }
        ]"#,
        )?;

        let expected_bytes = BigInt::from(123456789).to_signed_bytes_be();
        let value = Decimal::from(&expected_bytes);

        assert_roundtrip(value.clone(), &schema, Vec::new())?;
        assert_roundtrip(value.clone(), &Schema::union(vec![schema])?, Vec::new())?;
        assert_roundtrip(Some(value), &schema_option, Vec::new())?;
        assert_roundtrip(None::<BigDecimal>, &schema_option, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_414_deserialize_char_from_string() -> TestResult {
        let schema = Schema::String;

        assert_roundtrip('a', &schema, Vec::new())?;
        assert_roundtrip('👹', &schema, Vec::new())?;
        assert_roundtrip('a', &Schema::union(vec![schema.clone()])?, Vec::new())?;
        assert_roundtrip('👹', &Schema::union(vec![schema])?, Vec::new())?;

        Ok(())
    }

    #[test]
    fn avro_rs_414_deserialize_char_from_long_string() -> TestResult {
        let schema = Schema::String;
        let buf = GenericDatumWriter::builder(&schema)
            .build()?
            .write_ser_to_vec(&"avro")?;

        let error = GenericDatumReader::builder(&schema)
            .build()?
            .read_deser::<char>(&mut &buf[..])
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            r#"Failed to deserialize value of type char using schema String: Read more than one character: "avro""#
        );

        Ok(())
    }
}
