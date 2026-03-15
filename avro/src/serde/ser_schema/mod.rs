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

//! Logic for serde-compatible schema-aware serialization which writes directly to a writer.

mod block;
mod record;
mod tuple;
mod union;

use std::{borrow::Borrow, collections::HashMap, io::Write};

use block::BlockSerializer;
use record::RecordSerializer;
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value::Bool;
use tuple::{ManyTupleSerializer, TupleSerializer};
use union::UnionSerializer;

use crate::{
    Error, Schema,
    error::Details,
    schema::{
        DecimalSchema, InnerDecimalSchema, MapSchema, Name, RecordSchema, SchemaKind, UnionSchema,
        UuidSchema,
    },
    util::{zig_i32, zig_i64},
};

pub struct Config<'s, S: Borrow<Schema>> {
    /// Any references in the schema will be resolved using this map.
    ///
    /// This map is not allowed to contain any [`Schema::Ref`], the serializer is allowed to panic
    /// in that case.
    pub names: &'s HashMap<Name, S>,
    /// At what block size to start a new block (for arrays and maps).
    ///
    /// This is a minimum value, the block size will always be larger than this except for the last
    /// block.
    ///
    /// When set to `None` all values will be written in a single block. This can be faster as no
    /// intermediate buffer is used, but seeking through written data will be slower.
    pub target_block_size: Option<usize>,
    /// Should `Serialize` implementations pick a human-readable format.
    ///
    /// It is recommended to set this to `false` as it results in compacter output.
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

pub struct SchemaAwareSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    /// The schema of the data being serialized.
    ///
    /// This schema is guaranteed to not be a [`Schema::Ref`].
    schema: &'s Schema,
    config: Config<'s, S>,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SchemaAwareSerializer<'s, 'w, W, S> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s Schema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema {
            config.get_schema(name)?
        } else {
            schema
        };
        Ok(Self {
            writer,
            schema,
            config,
        })
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::SerializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: self.schema.clone(),
        })
    }

    /// Create a new serializer with the existing writer and config.
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

    /// Get the schema at the given index of the union, resolving references.
    fn get_resolved_union_variant(
        &self,
        union: &'s UnionSchema,
        index: u32,
    ) -> Result<&'s Schema, Error> {
        match union.get_variant(index as usize)? {
            Schema::Ref { name } => self.config.get_schema(name),
            schema => Ok(schema),
        }
    }

    /// Write an integer to the writer.
    ///
    /// This will check that the current schema is [`Schema::Int`] or a logical type based on that.
    /// This also handles [`Schema::Union`].
    fn checked_write_int(self, original_ty: &'static str, v: i32) -> Result<usize, Error> {
        match self.schema {
            Schema::Int | Schema::Date | Schema::TimeMillis => zig_i32(v, self.writer),
            Schema::Union(union) => UnionSerializer::new(self.writer, union, self.config)
                .checked_write_int(original_ty, v),
            _ => Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis",
            )),
        }
    }

    /// Write a long to the writer.
    ///
    /// This will check that the current schema is [`Schema::Long`] or a logical type based on that.
    /// This also handles [`Schema::Union`].
    fn checked_write_long(self, original_ty: &'static str, v: i64) -> Result<usize, Error> {
        match self.schema {
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
            | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                zig_i64(v, self.writer)
            }
            Schema::Union(union) => UnionSerializer::new(self.writer, union, self.config).checked_write_long(original_ty, v),
            _ => {
                Err(self.error(original_ty, "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}"))
            }
        }
    }

    /// Write bytes to the writer with preceding length header.
    ///
    /// This does not check the current schema.
    fn write_bytes_with_len(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written = 0;
        bytes_written += zig_i64(bytes.len() as i64, &mut *self.writer)?;
        bytes_written += self.write_bytes(bytes)?;
        Ok(bytes_written)
    }

    /// Write bytes to the writer.
    ///
    /// This does not check the current schema.
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        self.writer.write_all(bytes).map_err(Details::WriteBytes)?;
        Ok(bytes.len())
    }

    /// Write an array of `n` bytes to the writer.
    ///
    /// This does not check the current schema.
    fn write_array<const N: usize>(&mut self, bytes: [u8; N]) -> Result<usize, Error> {
        self.write_bytes(&bytes)?;
        Ok(N)
    }
}

/// Indicate to the serializer that a record field default is being serialized.
///
/// This is needed because the serializer takes a `&'static str` for the enum name and variant name.
/// When this value is encountered, the serializer will blindly trust the variant index.
///
/// To prevent users from abusing this fact, the string is compared by pointer value. Because the static
/// is not public, there is no way for a user to obtain that value.
static SERIALIZING_SCHEMA_DEFAULT: &str = "This value is compared by pointer value";

impl<'s, 'w, W: Write, S: Borrow<Schema>> Serializer for SchemaAwareSerializer<'s, 'w, W, S> {
    /// The amount of bytes written.
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = BlockSerializer<'s, 'w, W, S>;
    type SerializeTuple = TupleSerializer<'s, 'w, W, S>;
    type SerializeTupleStruct = ManyTupleSerializer<'s, 'w, W, S>;
    type SerializeTupleVariant = ManyTupleSerializer<'s, 'w, W, S>;
    type SerializeMap = MapOrRecordSerializer<'s, 'w, W, S>;
    type SerializeStruct = RecordSerializer<'s, 'w, W, S>;
    type SerializeStructVariant = RecordSerializer<'s, 'w, W, S>;

    fn serialize_bool(mut self, v: bool) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Boolean => self.write_array([v as u8]),
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_bool(v)
            }
            _ => Err(self.error("bool", "Expected Schema::Boolean")),
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i8", i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i16", i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i32", v)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.checked_write_long("i64", v)
    }

    fn serialize_i128(mut self, v: i128) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "i128" => {
                self.write_array(v.to_le_bytes())
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_i128(v)
            }
            _ => Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16)"#)),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("u8", i32::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("u16", i32::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.checked_write_long("u32", i64::from(v))
    }

    fn serialize_u64(mut self, v: u64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u64" => {
                self.write_array(v.to_le_bytes())
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_u64(v)
            }
            _ => Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8)"#)),
        }
    }

    fn serialize_u128(mut self, v: u128) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "u128" => {
                self.write_array(v.to_le_bytes())
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_u128(v)
            }
            _ => Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16)"#)),
        }
    }

    fn serialize_f32(mut self, v: f32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Float => self.write_array(v.to_le_bytes()),
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_f32(v)
            }
            _ => Err(self.error("f32", "Expected Schema::Float")),
        }
    }

    fn serialize_f64(mut self, v: f64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Double => self.write_array(v.to_le_bytes()),
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_f64(v)
            }
            _ => Err(self.error("f64", "Expected Schema::Double")),
        }
    }

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            // Convert the UTF-32 character to UTF-8
            Schema::String => self.write_bytes_with_len(v.to_string().as_bytes()),
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_char(v)
            }
            _ => Err(self.error("char", "Expected Schema::String")),
        }
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::String | Schema::Uuid(UuidSchema::String) => {
                self.write_bytes_with_len(v.as_bytes())
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_str(v)
            }
            _ => Err(self.error("str", "Expected Schema::String | Schema::Uuid(String)")),
        }
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Bytes | Schema::BigDecimal | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | Schema::Uuid(UuidSchema::Bytes) => {
                self.write_bytes_with_len(v)
            }
            Schema::Fixed(fixed) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(fixed), .. }) | Schema::Uuid(UuidSchema::Fixed(fixed)) | Schema::Duration(fixed) => {
                if fixed.size != v.len() {
                    Err(self.error("bytes", format!("Fixed size ({}) does not match bytes length ({})", fixed.size, v.len())))
                } else {
                    self.write_bytes(v)
                }
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_bytes(v)
            }
            _ => Err(self.error("bytes", "Expected Schema::Bytes | Schema::Fixed | Schema::BigDecimal | Schema::Decimal | Schema::Uuid(Bytes | Fixed) | Schema::Duration")),
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        if let Schema::Union(union) = self.schema
            && union.variants().len() == 2
            && let Some(null_index) = union.index_of_schema_kind(SchemaKind::Null)
        {
            zig_i32(null_index as i32, &mut *self.writer)
        } else {
            Err(self.error("none", "Expected Schema::Union([Schema::Null, _])"))
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if let Schema::Union(union) = self.schema
            && union.variants().len() == 2
            && let Some(null_index) = union.index_of_schema_kind(SchemaKind::Null)
        {
            let some_index = (null_index + 1) & 1;
            let mut bytes_written = zig_i32(some_index as i32, &mut *self.writer)?;
            bytes_written +=
                value.serialize(self.with_different_schema(&union.variants()[some_index])?)?;
            Ok(bytes_written)
        } else {
            Err(self.error("some", "Expected Schema::Union([Schema::Null, _])"))
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Null => Ok(0),
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_unit()
            }
            _ => Err(self.error("unit", "Expected Schema::Null")),
        }
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Record(record) if record.fields.is_empty() && record.name.name() == name => {
                Ok(0)
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_unit_struct(name)
            }
            _ => Err(self.error(
                "unit struct",
                format!(r#"Expected Schema::Record(name: "{name}", fields: [])"#),
            )),
        }
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Enum(enum_schema) => {
                // Plain enum
                if variant.as_ptr() == SERIALIZING_SCHEMA_DEFAULT.as_ptr() || enum_schema.symbols[variant_index as usize] == variant {
                    zig_i32(variant_index as i32, &mut *self.writer)
                } else {
                    Err(self.error("unit variant", format!(r#"Expected symbol "{variant}" at index {variant_index} in enum"#)))
                }
            }
            Schema::Union(union) => match self.get_resolved_union_variant(union, variant_index)? {
                // Bare union
                Schema::Null => zig_i32(variant_index as i32, &mut *self.writer),
                Schema::Record(record) if record.fields.is_empty() && record.name.name() == variant => {
                    // Union of records
                    zig_i32(variant_index as i32, &mut *self.writer)
                }
                _ => Err(self.error("unit variant", format!("Expected Schema::Null | Schema::Record(name: {variant}, fields: []) at index {variant_index} in the union"))),
            }
            _ => Err(self.error("unit variant", format!("Expected Schema::Enum(symbols[{variant_index}] == {variant}) | Schema::Union(variants[{variant_index}] == Schema::Null | Schema::Record(name: {variant}, fields: []))"))),
        }
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self.schema {
            Schema::Record(record) if record.fields.len() == 1 && record.name.name() == name => {
                let schema = &record.fields[0].schema;
                value.serialize(self.with_different_schema(schema)?)
            }
            Schema::Union(union) => UnionSerializer::new(self.writer, union, self.config)
                .serialize_newtype_struct(name, value),
            _ => Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields: [_])"),
            )),
        }
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self.schema {
            Schema::Union(union) => match self.get_resolved_union_variant(union, variant_index)? {
                Schema::Record(record)
                    if record.fields.len() == 1
                        && record.name.name() == variant
                        && record
                            .attributes
                            .get("org.apache.avro.rust.union_of_records")
                            == Some(&Bool(true)) =>
                {
                    // Union of records
                    let mut bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
                    let schema = &record.fields[0].schema;
                    bytes_written += value.serialize(self.with_different_schema(schema)?)?;
                    Ok(bytes_written)
                }
                schema => {
                    let mut bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
                    bytes_written += value.serialize(self.with_different_schema(schema)?)?;
                    Ok(bytes_written)
                }
            },
            _ => Err(self.error("newtype variant", "Expected Schema::Union")),
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match self.schema {
            Schema::Array(array) => {
                BlockSerializer::array(self.writer, array, self.config, len, None)
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_seq(len)
            }
            _ => Err(self.error("seq", "Expected Schema::Array")),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        match self.schema {
            Schema::Union(union) => {
                // This needs to be matched first, otherwise the `schema if len == 1` will also
                // match unions
                UnionSerializer::new(self.writer, union, self.config).serialize_tuple(len)
            }
            // `len == 0` is not possible for derived Serialize implementations but users might use it.
            // The derived Serialize implementations use `serialize_unit` instead
            Schema::Null if len == 0 => Ok(TupleSerializer::unit(None)),
            schema if len == 1 => Ok(TupleSerializer::one(self.writer, schema, self.config, None)),
            Schema::Record(record) if record.fields.len() == len => Ok(TupleSerializer::many(
                self.writer,
                record,
                self.config,
                None,
            )),
            // This error case can only happen for len > 1
            _ => Err(self.error(
                "tuple",
                format!("Expected Schema::Record(fields.len() == {len})"),
            )),
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        match self.schema {
            Schema::Record(record) if record.fields.len() == len && record.name.name() == name => {
                Ok(ManyTupleSerializer::new(
                    self.writer,
                    record,
                    self.config,
                    None,
                ))
            }
            Schema::Union(union) => UnionSerializer::new(self.writer, union, self.config)
                .serialize_tuple_struct(name, len),
            _ => Err(self.error(
                "tuple struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len})"),
            )),
        }
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        if let Schema::Union(union) = self.schema
            && let Schema::Record(record) = self.get_resolved_union_variant(union, variant_index)?
            && record.fields.len() == len
            && record.name.name() == variant
        {
            let bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
            Ok(ManyTupleSerializer::new(
                self.writer,
                record,
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error("tuple variant", format!("Expected Schema::Union(variants[{variant_index}] == Schema::Record(name: {variant}, fields.len() == {len}))")))
        }
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match self.schema {
            Schema::Map(map) => Ok(MapOrRecordSerializer::map(
                self.writer,
                map,
                self.config,
                len,
                None,
            )?),
            Schema::Record(record) => {
                // Structs with flattened fields are serialized as a map
                Ok(MapOrRecordSerializer::record(
                    self.writer,
                    record,
                    self.config,
                    len,
                ))
            }
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_map(len)
            }
            _ => Err(self.error(
                "map",
                "Expected Schema::Map | Schema::Record for structs with flattened fields",
            )),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        match self.schema {
            // Serde is inconsistent with the `name` and `len` provided. When using internally tagged
            // enums the name can be the name of the inner type of a newtype variant. The length can
            // also change based on `serialize_if`.
            Schema::Record(record) => Ok(RecordSerializer::new(
                self.writer,
                record,
                self.config,
                None,
            )),
            Schema::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_struct(name, len)
            }
            _ => Err(self.error("struct", "Expected Schema::Record")),
        }
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        if let Schema::Union(union) = self.schema
            && let Schema::Record(record) = self.get_resolved_union_variant(union, variant_index)?
            && record.fields.len() == len
            && record.name.name() == variant
        {
            let bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
            Ok(RecordSerializer::new(
                self.writer,
                record,
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error("struct variant", format!("Expected Schema::Union(variants[{variant_index}] == Schema::Record(name: {variant}, fields.len() == {len}))")))
        }
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}

pub enum MapOrRecordSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    Map(BlockSerializer<'s, 'w, W, S>),
    Record(RecordSerializer<'s, 'w, W, S>),
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> MapOrRecordSerializer<'s, 'w, W, S> {
    pub fn record(
        writer: &'w mut W,
        schema: &'s RecordSchema,
        config: Config<'s, S>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self::Record(RecordSerializer::new(writer, schema, config, bytes_written))
    }

    pub fn map(
        writer: &'w mut W,
        schema: &'s MapSchema,
        config: Config<'s, S>,
        len: Option<usize>,
        bytes_written: Option<usize>,
    ) -> Result<Self, Error> {
        Ok(Self::Map(BlockSerializer::map(
            writer,
            schema,
            config,
            len,
            bytes_written,
        )?))
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeMap for MapOrRecordSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            MapOrRecordSerializer::Map(map) => map.serialize_key(key),
            MapOrRecordSerializer::Record(record) => record.serialize_key(key),
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            MapOrRecordSerializer::Map(map) => map.serialize_value(value),
            MapOrRecordSerializer::Record(record) => record.serialize_value(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            MapOrRecordSerializer::Map(map) => map.end(),
            MapOrRecordSerializer::Record(record) => record.end(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FixedSchema;
    use crate::{
        Days, Duration, Millis, Months, Reader, Writer, decimal::Decimal, error::Details,
        from_value, schema::ResolvedSchema,
    };
    use apache_avro_test_helper::TestResult;
    use bigdecimal::BigDecimal;
    use num_bigint::{BigInt, Sign};
    use serde::{Deserialize, Serialize};
    use serde_bytes::{ByteArray, Bytes};
    use std::{
        collections::{BTreeMap, HashMap},
        marker::PhantomData,
    };
    use uuid::Uuid;

    #[test]
    fn test_serialize_null() -> TestResult {
        let schema = Schema::Null;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        None::<()>.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        None::<i32>.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        None::<String>.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), Vec::<u8>::new().as_slice());

        Ok(())
    }

    #[test]
    fn test_serialize_bool() -> TestResult {
        let schema = Schema::Boolean;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        true.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        false.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), &[1, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_int() -> TestResult {
        let schema = Schema::Int;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        4u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        31u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        13u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        7i8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        (-57i16).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        129i32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), &[8, 62, 26, 14, 113, 130, 2]);

        Ok(())
    }

    #[test]
    fn test_serialize_long() -> TestResult {
        let schema = Schema::Long;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        4u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        31u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        13u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        291u64.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        7i8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        (-57i16).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        129i32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        (-432i64).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(
            buffer.as_slice(),
            &[8, 62, 26, 198, 4, 14, 113, 130, 2, 223, 6]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_float() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        4.7f32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        (-14.1f64).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_double() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        4.7f32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        (-14.1f64).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_bytes() -> TestResult {
        let schema = Schema::Bytes;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        "test".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(SchemaAwareSerializer::new(
            &mut buffer,
            &schema,
            config,
        )?)?;
        assert!(
            ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            PhantomData::<String>
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(
            buffer.as_slice(),
            &[2, b'a', 8, b't', b'e', b's', b't', 10, 12, 3, 7, 91, 4]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_string() -> TestResult {
        let schema = Schema::String;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        "test".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(SchemaAwareSerializer::new(
            &mut buffer,
            &schema,
            config,
        )?)?;
        assert!(
            ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            PhantomData::<String>
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(
            buffer.as_slice(),
            &[2, b'a', 8, b't', b'e', b's', b't', 10, 12, 3, 7, 91, 4]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"}
            ]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct GoodTestRecord {
            string_field: String,
            int_field: i32,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct BadTestRecord {
            foo_string_field: String,
            bar_int_field: i32,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let good_record = GoodTestRecord {
            string_field: String::from("test"),
            int_field: 10,
        };
        good_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        let bad_record = BadTestRecord {
            foo_string_field: String::from("test"),
            bar_int_field: 10,
        };
        assert!(
            bad_record
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert!(
            "".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            Some("")
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), &[8, b't', b'e', b's', b't', 20]);

        Ok(())
    }

    #[test]
    fn test_serialize_empty_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "EmptyRecord",
            "fields": []
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        #[derive(Serialize)]
        struct EmptyRecord;
        EmptyRecord.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        #[derive(Serialize)]
        struct NonEmptyRecord {
            foo: String,
        }
        let record = NonEmptyRecord {
            foo: "bar".to_string(),
        };
        match record
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::FieldName(field_name)) if field_name == "foo" => (),
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        match ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none"); // serialize_unit() delegates to serialize_none()
                assert_eq!(value, "None. Cause: Expected: Record. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.len(), 0);

        Ok(())
    }

    #[test]
    fn test_serialize_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "enum",
            "name": "Suit",
            "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        }"#,
        )?;

        #[derive(Serialize)]
        enum Suit {
            Spades,
            Hearts,
            Diamonds,
            Clubs,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        Suit::Spades.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Suit::Hearts.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Suit::Diamonds.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Suit::Clubs.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        match None::<()>
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None. Cause: Expected: Enum. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[0, 2, 4, 6]);

        Ok(())
    }

    #[test]
    fn test_serialize_array() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "array",
            "items": "long"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let arr: Vec<i64> = vec![10, 5, 400];
        arr.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match vec![1_f32]
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "1. Cause: Expected: Long. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[6, 20, 10, 160, 6, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_map() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "map",
            "values": "long"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let mut map: BTreeMap<String, i64> = BTreeMap::new();
        map.insert(String::from("item1"), 10);
        map.insert(String::from("item2"), 400);

        map.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        let mut map: BTreeMap<String, &str> = BTreeMap::new();
        map.insert(String::from("item1"), "value1");
        match map
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "string");
                assert_eq!(value, "value1. Cause: Expected: Long. Got: String");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                4, 10, b'i', b't', b'e', b'm', b'1', 20, 10, b'i', b't', b'e', b'm', b'2', 160, 6,
                0
            ]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_nullable_union() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": ["null", "long"]
        }"#,
        )?;

        #[derive(Serialize)]
        enum NullableLong {
            Null,
            Long(i64),
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        Some(10i64).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        None::<i64>.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        NullableLong::Long(400).serialize(SchemaAwareSerializer::new(
            &mut buffer,
            &schema,
            config,
        )?)?;
        NullableLong::Null.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match "invalid"
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "string");
                assert_eq!(
                    value,
                    "invalid. Cause: Expected one of the union variants [Null, Long]. Got: String"
                );
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[2, 20, 0, 2, 160, 6, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_union() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": ["null", "long", "string"]
        }"#,
        )?;

        #[derive(Serialize)]
        enum LongOrString {
            Null,
            Long(i64),
            Str(String),
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        LongOrString::Null.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        LongOrString::Long(400).serialize(SchemaAwareSerializer::new(
            &mut buffer,
            &schema,
            config,
        )?)?;
        LongOrString::Str(String::from("test")).serialize(SchemaAwareSerializer::new(
            &mut buffer,
            &schema,
            config,
        )?)?;

        match 1_f64
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f64");
                assert_eq!(
                    value,
                    "1. Cause: Cannot find a Double schema in [Null, Long, String]"
                );
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[0, 2, 160, 6, 4, 8, b't', b'e', b's', b't']
        );

        Ok(())
    }

    #[test]
    fn test_serialize_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "size": 8,
            "name": "LongVal"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        Bytes::new(&[10, 124, 31, 97, 14, 201, 3, 88]).serialize(SchemaAwareSerializer::new(
            &mut buffer,
            &schema,
            config,
        )?)?;

        // non-8 size
        match Bytes::new(&[123])
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "bytes");
                assert_eq!(
                    value,
                    "7b. Cause: Fixed schema size (8) does not match the value length (1)"
                ); // Bytes represents its values as hexadecimals: '7b' is 123
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // array
        match [1; 8]
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=8). Cause: Expected: Fixed. Got: Array");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // slice
        match &[1, 2, 3, 4, 5, 6, 7, 8]
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(*value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=8). Cause: Expected: Fixed. Got: Array");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[10, 124, 31, 97, 14, 201, 3, 88]);

        Ok(())
    }

    #[test]
    fn test_serialize_decimal_bytes() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let val = Decimal::from(&[251, 155]);
        val.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None. Cause: Expected: Decimal. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[4, 251, 155]);

        Ok(())
    }

    #[test]
    fn test_serialize_decimal_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "name": "FixedDecimal",
            "size": 8,
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let val = Decimal::from(&[0, 0, 0, 0, 0, 0, 251, 155]);
        val.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?).map_err(Error::into_details) {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None. Cause: Expected: Decimal. Got: Null");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[0, 0, 0, 0, 0, 0, 251, 155]);

        Ok(())
    }

    #[test]
    fn test_serialize_bigdecimal() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "big-decimal"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let val = BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2);
        val.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[10, 6, 0, 195, 104, 4]);

        Ok(())
    }

    #[test]
    fn test_serialize_uuid() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "size": 16,
            "logicalType": "uuid",
            "name": "FixedUuid"
        }"#,
        )?;

        assert!(!crate::util::is_human_readable());
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        "8c28da81-238c-4326-bddd-4e3d00cc5099"
            .parse::<Uuid>()?
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match 1_u8
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "u8");
                assert_eq!(value, "1. Cause: Expected: Uuid. Got: Int");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                140, 40, 218, 129, 35, 140, 67, 38, 189, 221, 78, 61, 0, 204, 80, 153
            ]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_date() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "int",
            "logicalType": "date"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        100_u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_i16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_i32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match 10000_f32
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000. Cause: Expected: Date. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_time_millis() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "int",
            "logicalType": "time-millis"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        100_u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_i16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_i32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match 10000_f32
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000. Cause: Expected: TimeMillis. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_time_micros() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "long",
            "logicalType": "time-micros"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        100_u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_i16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_i32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        10000_i64.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match 10000_f32
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000. Cause: Expected: TimeMicros. Got: Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1, 160, 156, 1
            ]
        );

        Ok(())
    }

    #[test]
    fn test_serialize_timestamp() -> TestResult {
        for precision in ["millis", "micros", "nanos"] {
            let schema = Schema::parse_str(&format!(
                r#"{{
                "type": "long",
                "logicalType": "timestamp-{precision}"
            }}"#
            ))?;

            let mut buffer: Vec<u8> = Vec::new();
            let names = HashMap::new();
            let config = Config::<'_, Schema> {
                names: &names,
                target_block_size: None,
                human_readable: false,
            };
            100_u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
            1000_u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
            10000_u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
            1000_i16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
            10000_i32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
            10000_i64.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

            match 10000_f64
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .map_err(Error::into_details)
            {
                Err(Details::SerializeValueWithSchema {
                    value_type,
                    value,
                    schema,
                }) => {
                    let mut capital_precision = precision.to_string();
                    if let Some(c) = capital_precision.chars().next() {
                        capital_precision.replace_range(..1, &c.to_uppercase().to_string());
                    }
                    assert_eq!(value_type, "f64");
                    assert_eq!(
                        value,
                        format!(
                            "10000. Cause: Expected: Timestamp{capital_precision}. Got: Double"
                        )
                    );
                    assert_eq!(schema, schema);
                }
                unexpected => panic!("Expected an error. Got: {unexpected:?}"),
            }

            assert_eq!(
                buffer.as_slice(),
                &[
                    200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1, 160, 156, 1
                ]
            );
        }

        Ok(())
    }

    #[test]
    fn test_serialize_duration() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "size": 12,
            "name": "duration",
            "logicalType": "duration"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let duration_bytes =
            ByteArray::new(Duration::new(Months::new(3), Days::new(2), Millis::new(1200)).into());
        duration_bytes.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        match [1; 12]
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
            .map_err(Error::into_details)
        {
            Err(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(
                    value,
                    "tuple (len=12). Cause: Expected: Duration. Got: Array"
                );
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[3, 0, 0, 0, 2, 0, 0, 0, 176, 4, 0, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_recursive_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"},
                {"name": "bigDecimalField", "type": {"type": "bytes", "logicalType": "big-decimal"}},
                {"name": "uuidField", "type": {"name": "uuid", "type": "fixed", "size": 16, "logicalType": "uuid"}},
                {"name": "innerRecord", "type": ["null", "TestRecord"]}
            ]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            string_field: String,
            int_field: i32,
            big_decimal_field: BigDecimal,
            uuid_field: Uuid,
            // #[serde(skip_serializing_if = "Option::is_none")] => Never ignore None!
            inner_record: Option<Box<TestRecord>>,
        }

        assert!(!crate::util::is_human_readable());
        let mut buffer: Vec<u8> = Vec::new();
        let rs = ResolvedSchema::try_from(&schema)?;
        let config = Config {
            names: rs.get_names(),
            target_block_size: None,
            human_readable: false,
        };

        let good_record = TestRecord {
            string_field: String::from("test"),
            int_field: 10,
            big_decimal_field: BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2),
            uuid_field: "8c28da81-238c-4326-bddd-4e3d00cc5098".parse::<Uuid>()?,
            inner_record: Some(Box::new(TestRecord {
                string_field: String::from("inner_test"),
                int_field: 100,
                big_decimal_field: BigDecimal::new(BigInt::new(Sign::Plus, vec![20038]), 2),
                uuid_field: "8c28da81-238c-4326-bddd-4e3d00cc5099".parse::<Uuid>()?,
                inner_record: None,
            })),
        };
        good_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(
            buffer.as_slice(),
            &[
                8, 116, 101, 115, 116, 20, 10, 6, 0, 195, 104, 4, 140, 40, 218, 129, 35, 140, 67,
                38, 189, 221, 78, 61, 0, 204, 80, 152, 2, 20, 105, 110, 110, 101, 114, 95, 116,
                101, 115, 116, 200, 1, 8, 4, 78, 70, 4, 140, 40, 218, 129, 35, 140, 67, 38, 189,
                221, 78, 61, 0, 204, 80, 153, 0
            ]
        );

        Ok(())
    }

    #[test]
    fn avro_rs_337_serialize_union_record_variant() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [{
                "name": "innerUnion", "type": [
                    {"type": "record", "name": "innerRecordFoo", "fields": [
                        {"name": "foo", "type": "string"}
                    ]},
                    {"type": "record", "name": "innerRecordBar", "fields": [
                        {"name": "bar", "type": "string"}
                    ]},
                    {"name": "intField", "type": "int"},
                    {"name": "stringField", "type": "string"}
                ]
            }]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            inner_union: InnerUnion,
        }

        #[derive(Serialize)]
        #[serde(untagged)]
        enum InnerUnion {
            InnerVariantFoo(InnerRecordFoo),
            InnerVariantBar(InnerRecordBar),
            IntField(i32),
            StringField(String),
        }

        #[derive(Serialize)]
        #[serde(rename = "innerRecordFoo")]
        struct InnerRecordFoo {
            foo: String,
        }

        #[derive(Serialize)]
        #[serde(rename = "innerRecordBar")]
        struct InnerRecordBar {
            bar: String,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let rs = ResolvedSchema::try_from(&schema)?;
        let config = Config {
            names: rs.get_names(),
            target_block_size: None,
            human_readable: false,
        };

        let foo_record = TestRecord {
            inner_union: InnerUnion::InnerVariantFoo(InnerRecordFoo {
                foo: String::from("foo"),
            }),
        };
        foo_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let bar_record = TestRecord {
            inner_union: InnerUnion::InnerVariantBar(InnerRecordBar {
                bar: String::from("bar"),
            }),
        };
        bar_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let int_record = TestRecord {
            inner_union: InnerUnion::IntField(1),
        };
        int_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let string_record = TestRecord {
            inner_union: InnerUnion::StringField(String::from("string")),
        };
        string_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Ok(())
    }

    #[test]
    fn avro_rs_337_serialize_option_union_record_variant() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [{
                "name": "innerUnion", "type": [
                    "null",
                    {"type": "record", "name": "innerRecordFoo", "fields": [
                        {"name": "foo", "type": "string"}
                    ]},
                    {"type": "record", "name": "innerRecordBar", "fields": [
                        {"name": "bar", "type": "string"}
                    ]},
                    {"name": "intField", "type": "int"},
                    {"name": "stringField", "type": "string"}
                ]
            }]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            inner_union: Option<InnerUnion>,
        }

        #[derive(Serialize)]
        #[serde(untagged)]
        enum InnerUnion {
            InnerVariantFoo(InnerRecordFoo),
            InnerVariantBar(InnerRecordBar),
            IntField(i32),
            StringField(String),
        }

        #[derive(Serialize)]
        #[serde(rename = "innerRecordFoo")]
        struct InnerRecordFoo {
            foo: String,
        }

        #[derive(Serialize)]
        #[serde(rename = "innerRecordBar")]
        struct InnerRecordBar {
            bar: String,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let rs = ResolvedSchema::try_from(&schema)?;
        let config = Config {
            names: rs.get_names(),
            target_block_size: None,
            human_readable: false,
        };

        let null_record = TestRecord { inner_union: None };
        null_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let foo_record = TestRecord {
            inner_union: Some(InnerUnion::InnerVariantFoo(InnerRecordFoo {
                foo: String::from("foo"),
            })),
        };
        foo_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let bar_record = TestRecord {
            inner_union: Some(InnerUnion::InnerVariantBar(InnerRecordBar {
                bar: String::from("bar"),
            })),
        };
        bar_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let int_record = TestRecord {
            inner_union: Some(InnerUnion::IntField(1)),
        };
        int_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        let string_record = TestRecord {
            inner_union: Some(InnerUnion::StringField(String::from("string"))),
        };
        string_record.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        Ok(())
    }

    #[test]
    fn avro_rs_351_different_field_order_serde_vs_schema() -> TestResult {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Foo {
            a: String,
            b: String,
            c: usize,
            d: f64,
            e: usize,
        }
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"b",
                    "type":"string"
                },
                {
                    "name":"a",
                    "type":"string"
                },
                {
                    "name":"d",
                    "type":"double"
                },
                {
                    "name":"e",
                    "type":"long"
                },
                {
                    "name":"c",
                    "type":"long"
                }
            ]
        }
        "#,
        )?;

        let mut writer = Writer::new(&schema, Vec::new())?;
        writer.append_ser(Foo {
            a: "Hello".into(),
            b: "World".into(),
            c: 42,
            d: std::f64::consts::PI,
            e: 5,
        })?;
        let encoded = writer.into_inner()?;
        let mut reader = Reader::builder(&encoded[..])
            .reader_schema(&schema)
            .build()?;
        let decoded = from_value::<Foo>(&reader.next().unwrap()?)?;
        assert_eq!(
            decoded,
            Foo {
                a: "Hello".into(),
                b: "World".into(),
                c: 42,
                d: std::f64::consts::PI,
                e: 5
            }
        );
        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_string() -> TestResult {
        let schema = Schema::String;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[2, b'a']);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_bytes() -> TestResult {
        let schema = Schema::Bytes;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[2, b'a']);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_fixed() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("char")?,
            aliases: None,
            doc: None,
            size: 4,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[b'a', 0, 0, 0]);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_emoji_char_as_string() -> TestResult {
        let schema = Schema::String;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        '👹'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[8, 240, 159, 145, 185]);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_emoji_char_as_bytes() -> TestResult {
        let schema = Schema::Bytes;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        '👹'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[8, 240, 159, 145, 185]);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_emoji_char_as_fixed() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("char")?,
            aliases: None,
            doc: None,
            size: 4,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        '👹'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        // This is a different byte value than the tests above. This is because by creating a String
        // the unicode value is normalized by Rust
        assert_eq!(buffer.as_slice(), &[121, 244, 1, 0]);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_fixed_wrong_name() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("characters")?,
            aliases: None,
            doc: None,
            size: 4,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        assert!(matches!(
            'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .unwrap_err()
                .details(),
            Details::SerializeValueWithSchema { .. }
        ));

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_fixed_wrong_size() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("char")?,
            aliases: None,
            doc: None,
            size: 1,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        assert!(matches!(
            'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .unwrap_err()
                .details(),
            Details::SerializeValueWithSchema { .. }
        ));

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_i128_as_fixed() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("i128")?,
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let bytes_written =
            i128::MAX.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert_eq!(bytes_written, 16);

        assert_eq!(
            buffer.as_slice(),
            &[
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0x7F
            ]
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_i128_as_fixed_wrong_name() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("onehundredtwentyeight")?,
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        assert!(matches!(
            i128::MAX
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .unwrap_err()
                .details(),
            Details::SerializeValueWithSchema { .. }
        ));

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_i128_as_fixed_wrong_size() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("i128")?,
            aliases: None,
            doc: None,
            size: 8,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        assert!(matches!(
            i128::MAX
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .unwrap_err()
                .details(),
            Details::SerializeValueWithSchema { .. }
        ));

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_u128_as_fixed() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("u128")?,
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        let bytes_written =
            u128::MAX.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert_eq!(bytes_written, 16);

        assert_eq!(
            buffer.as_slice(),
            &[
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF
            ]
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_u128_as_fixed_wrong_name() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("onehundredtwentyeight")?,
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        assert!(matches!(
            u128::MAX
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .unwrap_err()
                .details(),
            Details::SerializeValueWithSchema { .. }
        ));

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_u128_as_fixed_wrong_size() -> TestResult {
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("u128")?,
            aliases: None,
            doc: None,
            size: 8,
            attributes: Default::default(),
        });

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };

        assert!(matches!(
            u128::MAX
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .unwrap_err()
                .details(),
            Details::SerializeValueWithSchema { .. }
        ));

        Ok(())
    }

    #[test]
    fn avro_rs_421_serialize_bytes_union_of_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"[
            { "name": "fixed4", "type": "fixed", "size": 4 },
            { "name": "fixed8", "type": "fixed", "size": 8 }
        ]"#,
        )
        .unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let config = Config::<'_, Schema> {
            names: &names,
            target_block_size: None,
            human_readable: false,
        };
        let bytes_written = crate::serde::fixed::serialize(
            &[0, 1, 2, 3],
            SchemaAwareSerializer::new(&mut buffer, &schema, config)?,
        )?;
        assert_eq!(bytes_written, 4);
        let bytes_written = crate::serde::fixed::serialize(
            &[4, 5, 6, 7, 8, 9, 10, 11],
            SchemaAwareSerializer::new(&mut buffer, &schema, config)?,
        )?;
        assert_eq!(bytes_written, 8);

        assert_eq!(buffer, &[0, 0, 1, 2, 3, 2, 4, 5, 6, 7, 8, 9, 10, 11][..]);

        Ok(())
    }
}
