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

mod array;
mod map;
mod record;
mod tuple;
mod union;

use crate::encode::{encode_int, encode_long};
use crate::error::Details;
use crate::schema::{DecimalSchema, InnerDecimalSchema, NamesRef, SchemaKind, UuidSchema};
use crate::serde::with::{BytesType, SER_BYTES_TYPE};
use crate::{Error, Schema};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::fmt::{Debug, Formatter};
use std::io::Write;

use crate::serde::ser_schema::tuple::{OneTupleSerializer, TupleSerializer, UnitTupleSerializer};
use array::ArraySerializer;
use map::MapSerializer;
use record::RecordSerializer;
use tuple::ManyTupleSerializer;
use union::UnionAwareSerializer;

/// Indicate to the serializer that a record field default is being serialized.
///
/// This is needed because the serializer takes a `&'static str` for the enum name and variant name.
/// When this value is encountered, the serializer will blindly trust the variant index.
///
/// To prevent users from abusing this fact, the string is compared by pointer value. Because the static
/// is not public, there is no way for a user to obtain that value.
static SERIALIZING_SCHEMA_DEFAULT: &str = "The pointer value is used, not the string itself";

pub enum MapOrRecordSerializer<'s, 'w, W: Write> {
    Map(MapSerializer<'s, 'w, W>),
    Record(RecordSerializer<'s, 'w, W>),
}

impl<'s, 'w, W: Write> SerializeMap for MapOrRecordSerializer<'s, 'w, W> {
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

#[derive(Clone, Copy)]
pub struct Config<'s> {
    /// All names that can be referenced in the schema being used for serialisation.
    pub names: &'s NamesRef<'s>,
    /// At what block size to start a new block.
    ///
    /// This is a minimum value, the block size will always be larger than this except for the last
    /// block.
    ///
    /// When set to `None` all values will be written in a single block. This can be faster as no
    /// intermediate buffer is used, but deserialization can be slower as the block size is not written.
    pub target_block_size: Option<usize>,
    /// Should `Serialize` implementations pick a human-readable format.
    ///
    /// It is recommended to set this to `false` as it results in compacter output.
    pub human_readable: bool,
}

pub struct SchemaAwareSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    schema: &'s Schema,
    config: Config<'s>,
}

impl<'s, 'w, W: Write> Debug for SchemaAwareSerializer<'s, 'w, W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaAwareSerializer")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<'s, 'w, W: Write> SchemaAwareSerializer<'s, 'w, W> {
    pub fn new(writer: &'w mut W, schema: &'s Schema, config: Config<'s>) -> Result<Self, Error> {
        if let Schema::Ref { name } = schema {
            let schema = config
                .names
                .get(name)
                .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?;
            Self::new(writer, schema, config)
        } else {
            Ok(Self {
                writer,
                schema,
                config,
            })
        }
    }

    /// Create a new serializer with the existing writer and config.
    ///
    /// This will resolve the schema if it is a reference.
    fn with_different_schema(mut self, schema: &'s Schema) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema {
            self.config
                .names
                .get(name)
                .copied()
                .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?
        } else {
            schema
        };
        self.schema = schema;
        Ok(self)
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::SerializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: self.schema.clone(),
        })
    }

    fn serialize_int(self, original_ty: &'static str, v: i32) -> Result<usize, Error> {
        match self.schema {
            Schema::Int | Schema::Date | Schema::TimeMillis => encode_int(v, &mut *self.writer),
            Schema::Union(union) => UnionAwareSerializer::new(self.writer, union, self.config)
                .serialize_int(original_ty, v),
            _ => Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis",
            )),
        }
    }

    fn serialize_long(self, original_ty: &'static str, v: i64) -> Result<usize, Error> {
        match self.schema {
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
            | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                encode_long(v, &mut *self.writer)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_long(original_ty, v)
            }
            _ => {
                Err(self.error(original_ty, "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}"))
            }
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        self.writer.write_all(bytes).map_err(Details::WriteBytes)?;

        Ok(bytes.len())
    }

    fn write_bytes_with_len(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written: usize = 0;

        bytes_written += encode_long(bytes.len() as i64, &mut *self.writer)?;
        bytes_written += self.write_bytes(bytes)?;

        Ok(bytes_written)
    }
}

impl<'s, 'w, W: Write> Serializer for SchemaAwareSerializer<'s, 'w, W> {
    /// Amount of bytes written
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = ArraySerializer<'s, 'w, W>;
    type SerializeTuple = TupleSerializer<'s, 'w, W>;
    type SerializeTupleStruct = ManyTupleSerializer<'s, 'w, W>;
    type SerializeTupleVariant = ManyTupleSerializer<'s, 'w, W>;
    type SerializeMap = MapOrRecordSerializer<'s, 'w, W>;
    type SerializeStruct = RecordSerializer<'s, 'w, W>;
    type SerializeStructVariant = RecordSerializer<'s, 'w, W>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        println!("serialize_bool({v}): {self:?}");
        match self.schema {
            Schema::Boolean => {
                self.writer
                    .write_all(&[v as u8])
                    .map_err(Details::WriteBytes)?;
                Ok(1)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_bool(v)
            }
            _ => Err(self.error("bool", "Expected Schema::Boolean")),
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        println!("serialize_i8({v}): {self:?}");
        self.serialize_int("i8", i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        println!("serialize_i16({v}): {self:?}");
        self.serialize_int("i16", i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        println!("serialize_i32({v}): {self:?}");
        self.serialize_int("i32", v)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        println!("serialize_i64({v}): {self:?}");
        self.serialize_long("i64", v)
    }

    fn serialize_i128(mut self, v: i128) -> Result<Self::Ok, Self::Error> {
        println!("serialize_i128({v}): {self:?}");
        match self.schema {
            Schema::Fixed(fixed) if fixed.name.name() == "i128" && fixed.size == 16 => {
                let bytes = v.to_le_bytes();
                self.write_bytes(&bytes)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_i128(v)
            }
            _ => Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16)"#)),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        println!("serialize_u8({v}): {self:?}");
        self.serialize_int("u8", i32::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        println!("serialize_u16({v}): {self:?}");
        self.serialize_int("u16", i32::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        println!("serialize_u32({v}): {self:?}");
        self.serialize_long("u32", i64::from(v))
    }

    fn serialize_u64(mut self, v: u64) -> Result<Self::Ok, Self::Error> {
        println!("serialize_u64({v}): {self:?}");
        match self.schema {
            Schema::Fixed(fixed) if fixed.name.name() == "u64" && fixed.size == 8 => {
                let bytes = v.to_le_bytes();
                self.write_bytes(&bytes)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_u64(v)
            }
            _ => Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8)"#)),
        }
    }

    fn serialize_u128(mut self, v: u128) -> Result<Self::Ok, Self::Error> {
        println!("serialize_u128({v}): {self:?}");
        match self.schema {
            Schema::Fixed(fixed) if fixed.name.name() == "u128" && fixed.size == 16 => {
                let bytes = v.to_le_bytes();
                self.write_bytes(&bytes)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_u128(v)
            }
            _ => Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16)"#)),
        }
    }

    fn serialize_f32(mut self, v: f32) -> Result<Self::Ok, Self::Error> {
        println!("serialize_f32({v}): {self:?}");
        match self.schema {
            Schema::Float => {
                let bytes = v.to_le_bytes();
                self.write_bytes(&bytes)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_f32(v)
            }
            _ => Err(self.error("f32", r"Expected Schema::Float")),
        }
    }

    fn serialize_f64(mut self, v: f64) -> Result<Self::Ok, Self::Error> {
        println!("serialize_f64({v}): {self:?}");
        match self.schema {
            Schema::Float => {
                let bytes = v.to_le_bytes();
                self.write_bytes(&bytes)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_f64(v)
            }
            _ => Err(self.error("f64", r"Expected Schema::Double")),
        }
    }

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        println!("serialize_char({v}): {self:?}");
        match self.schema {
            Schema::String => {
                let bytes = v.to_string().into_bytes();
                self.write_bytes_with_len(&bytes)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_char(v)
            }
            _ => Err(self.error("char", r"Expected Schema::String")),
        }
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        println!("serialize_str({v}): {self:?}");
        match self.schema {
            Schema::String | Schema::Uuid(UuidSchema::String) => {
                self.write_bytes_with_len(v.as_bytes())
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_str(v)
            }
            _ => Err(self.error("str", "Expected Schema::String | Schema::Uuid(String)")),
        }
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        println!("serialize_bytes({v:?}): {self:?}");
        match (SER_BYTES_TYPE.get(), self.schema) {
            (BytesType::Unset | BytesType::Bytes, Schema::Bytes | Schema::BigDecimal | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, .. })) => {
                self.write_bytes_with_len(v)
            }
            (BytesType::Unset | BytesType::Fixed, Schema::Fixed(fixed) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(fixed), ..}) | Schema::Uuid(UuidSchema::Fixed(fixed)) | Schema::Duration(fixed)) => {
                if fixed.size != v.len() {
                    Err(self.error("bytes", format!("Fixed size ({}) does not match value length ({})", fixed.size, v.len())))
                } else {
                    self.write_bytes(v)
                }
            }
            (_, Schema::Union(union)) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_bytes(v)
            }
            _ => Err(self.error("bytes", "Expected Schema::Bytes | Schema::BigDecimal | Schema::Decimal(Bytes | Fixed) | Schema::Fixed | Schema::Uuid(Fixed) | Schema::Duration")),
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        println!("serialize_none: {self:?}");
        if let Schema::Union(union) = self.schema
            && union.variants().len() == 2
            && let Some(index) = union.index().get(&SchemaKind::Null).copied()
        {
            encode_int(index as i32, &mut *self.writer)
        } else {
            Err(self.error(
                "None",
                "Expected Schema::Union(variants.len() == 2 && variants.contains(Schema::Null))",
            ))
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        println!("serialize_some: {self:?}");
        if let Schema::Union(union) = self.schema
            && union.variants().len() == 2
            && let Some(index) = union.index().get(&SchemaKind::Null).copied()
        {
            // Convert the index of null to the other index
            let index = (index + 1) & 1;
            let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
            let ser = self.with_different_schema(&union.variants()[index])?;
            bytes_written += value.serialize(ser)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                "None",
                "Expected Schema::Union(variants.len() == 2 && variants.contains(Schema::Null))",
            ))
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        println!("serialize_unit: {self:?}");
        match self.schema {
            Schema::Null => Ok(0),
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_unit()
            }
            _ => Err(self.error("unit", "Expected Schema::Null")),
        }
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        println!("serialize_unit_struct(name: {name}): {self:?}");
        match self.schema {
            Schema::Record(record) if record.name.name() == name && record.fields.is_empty() => {
                Ok(0)
            }
            Schema::Union(union) => UnionAwareSerializer::new(self.writer, union, self.config)
                .serialize_unit_struct(name),
            _ => Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields: [])"),
            )),
        }
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        println!(
            "serialize_struct_variant(name: {name}, index: {variant_index}, variant: {variant}): {self:?}"
        );
        match self.schema {
            Schema::Enum(enum_schema) => {
                if name.as_ptr() == SERIALIZING_SCHEMA_DEFAULT.as_ptr() {
                    encode_int(variant_index as i32, &mut *self.writer)
                } else {
                    if enum_schema.symbols[variant_index as usize] != variant {
                        return Err(self.error(
                            "unit variant",
                            format!(
                                "Enum variant ({variant}) is not at index {variant_index} in symbols"
                            ),
                        ));
                    }
                    encode_int(variant_index as i32, &mut *self.writer)
                }
            }
            Schema::Union(union) => {
                if let Some(index) = union.index().get(&SchemaKind::Null).copied() {
                    // Bare union
                    encode_int(index as i32, &mut *self.writer)
                } else {
                    // Union of records
                    let Some(Schema::Record(record)) = union.variants().get(variant_index as usize)
                    else {
                        return Err(self.error("unit variant", format!("Union does not contain null and variant at index {variant_index} is not a record")));
                    };
                    if record.name.name() != variant {
                        return Err(self.error("unit variant", format!("Union does not contain null and record at index {variant_index} is not named {variant}")));
                    }
                    if !record.fields.is_empty() {
                        return Err(self.error("unit variant", format!("Union does not contain null and record at index {variant_index} is not empty")));
                    }
                    encode_int(variant_index as i32, &mut *self.writer)
                }
            }
            _ => Err(self.error("unit variant", "Expected Schema::Enum | Schema::Union")),
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
        println!("serialize_newtype_struct(name: {name}): {self:?}");
        match self.schema {
            Schema::Record(record) if record.name.name() == name && record.fields.len() == 1 => {
                let schema = &record.fields[0].schema;
                let ser = self.with_different_schema(schema)?;
                value.serialize(ser)
            }
            Schema::Union(union) => UnionAwareSerializer::new(self.writer, union, self.config)
                .serialize_newtype_struct(name, value),
            _ => Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == 1)"),
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
        println!(
            "serialize_newtype_variant(name: {_name}, index: {variant_index}, variant: {variant}): {self:?}"
        );
        match self.schema {
            Schema::Union(union) => {
                if let Some(Schema::Record(record)) = union.variants().get(variant_index as usize)
                    && record.name.name() == variant
                    && record.fields.len() == 1
                {
                    // Union of records
                    let mut bytes_written = encode_int(variant_index as i32, &mut *self.writer)?;

                    let schema = &record.fields[0].schema;
                    let ser = self.with_different_schema(schema)?;
                    bytes_written += value.serialize(ser)?;
                    Ok(bytes_written)
                } else {
                    // Bare union, UnionAwareSerializer will write the correct index
                    let ser = UnionAwareSerializer::new(self.writer, union, self.config);
                    value.serialize(ser)
                }
            }
            _ => Err(self.error("newtype variant", "Expected Schema::Union")),
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        println!("serialize_seq(len: {len:?}): {self:?}");
        match self.schema {
            Schema::Array(array) => {
                ArraySerializer::new(self.writer, array, self.config, len, None)
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_seq(len)
            }
            _ => Err(self.error("seq", "Expected Schema::Array")),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        println!("serialize_tuple(len: {len}): {self:?}");
        match self.schema {
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_tuple(len)
            }
            Schema::Null if len == 0 => Ok(TupleSerializer::Unit(UnitTupleSerializer::new(None))),
            schema if len == 1 => Ok(TupleSerializer::One(OneTupleSerializer::new(
                self.writer,
                schema,
                self.config,
            ))),
            Schema::Record(record) if record.fields.len() == len => Ok(TupleSerializer::Many(
                ManyTupleSerializer::new(self.writer, record, self.config, None),
            )),
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
        println!("serialize_tuple_struct(name: {name}, len: {len}): {self:?}");
        match self.schema {
            Schema::Record(record) if record.name.name() == name && record.fields.len() == len => {
                Ok(ManyTupleSerializer::new(
                    self.writer,
                    record,
                    self.config,
                    None,
                ))
            }
            Schema::Union(union) => UnionAwareSerializer::new(self.writer, union, self.config)
                .serialize_tuple_struct(name, len),
            _ => Err(self.error(
                "tuple",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len})"),
            )),
        }
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        println!(
            "serialize_tuple_variant(name: {_name}, index: {variant_index}, variant: {variant}, len: {len}): {self:?}"
        );
        match self.schema {
            Schema::Union(union) => {
                if let Some(Schema::Record(record)) = union.variants().get(variant_index as usize)
                    && record.name.name() == variant
                    && record.fields.len() == len
                {
                    // Union of records
                    let bytes_written = encode_int(variant_index as i32, &mut *self.writer)?;

                    Ok(ManyTupleSerializer::new(
                        self.writer,
                        record,
                        self.config,
                        Some(bytes_written),
                    ))
                } else if let Some((index, Schema::Record(record))) = union
                    .variants()
                    .iter()
                    .enumerate()
                    .find(|(_i, s)| s.name().is_some_and(|n| n.name() == variant))
                    && record.fields.len() == len
                {
                    // Bare union
                    let bytes_written = encode_int(index as i32, &mut *self.writer)?;

                    Ok(ManyTupleSerializer::new(
                        self.writer,
                        record,
                        self.config,
                        Some(bytes_written),
                    ))
                } else {
                    Err(self.error(
                        "tuple variant",
                        format!("Expected Schema::Union(variants.contains(Schema::Record(name: {variant}, fields.len() == {len})))"),
                    ))
                }
            }
            _ => Err(self.error("tuple variant", "Expected Schema::Union")),
        }
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        println!("serialize_map(len: {len:?}): {self:?}");
        match self.schema {
            Schema::Map(map) => Ok(MapOrRecordSerializer::Map(MapSerializer::new(
                self.writer,
                map,
                self.config,
                len,
                None,
            )?)),
            Schema::Record(record) => {
                // Structs with flattened fields will be serialized as a map
                Ok(MapOrRecordSerializer::Record(RecordSerializer::new(
                    self.writer,
                    record,
                    self.config,
                    len,
                )))
            }
            Schema::Union(union) => {
                UnionAwareSerializer::new(self.writer, union, self.config).serialize_map(len)
            }
            _ => Err(self.error("map", "Expected Schema::Map")),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        println!("serialize_struct(name: {name}, len: {len}): {self:?}");
        match self.schema {
            // For unit variants with tag,content `len` will be 1 but we expect 2.
            Schema::Record(record)
                if record.name.name() == name && record.fields.len() == len
                    || record.fields.len() == 2 =>
            {
                Ok(RecordSerializer::new(
                    self.writer,
                    record,
                    self.config,
                    None,
                ))
            }
            Schema::Union(union) => UnionAwareSerializer::new(self.writer, union, self.config)
                .serialize_struct(name, len),
            _ => Err(self.error(
                "struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len})"),
            )),
        }
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        println!(
            "serialize_struct_variant(name: {_name}, index: {variant_index}, variant: {variant}, len: {len}): {self:?}"
        );
        match self.schema {
            Schema::Union(union) => {
                if let Some(Schema::Record(record)) = union.variants().get(variant_index as usize)
                    && record.name.name() == variant
                    && record.fields.len() == len
                {
                    // Union of records
                    let bytes_written = encode_int(variant_index as i32, &mut *self.writer)?;

                    Ok(RecordSerializer::new(
                        self.writer,
                        record,
                        self.config,
                        Some(bytes_written),
                    ))
                } else if let Some((index, Schema::Record(record))) = union
                    .variants()
                    .iter()
                    .enumerate()
                    .find(|(_i, s)| s.name().is_some_and(|n| n.name() == variant))
                    && record.fields.len() == len
                {
                    // Bare union
                    let bytes_written = encode_int(index as i32, &mut *self.writer)?;

                    Ok(RecordSerializer::new(
                        self.writer,
                        record,
                        self.config,
                        Some(bytes_written),
                    ))
                } else {
                    Err(self.error(
                        "struct variant",
                        format!("Expected Schema::Union(variants.contains(Schema::Record(name: {variant}, fields.len() == {len})))"),
                    ))
                }
            }
            _ => Err(self.error("struct variant", "Expected Schema::Union")),
        }
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, SchemaAwareSerializer};
    use crate::schema::{FixedSchema, Name};
    use crate::{
        Days, Duration, Error, Millis, Months, Reader, Schema, Writer, decimal::Decimal,
        error::Details, from_value, schema::ResolvedSchema,
    };
    use apache_avro_test_helper::TestResult;
    use bigdecimal::BigDecimal;
    use num_bigint::{BigInt, Sign};
    use serde::{Deserialize, Serialize};
    use serde_bytes::{ByteArray, Bytes};
    use std::collections::HashMap;
    use std::{collections::BTreeMap, marker::PhantomData};
    use uuid::Uuid;

    #[test]
    fn test_serialize_null() -> TestResult {
        let schema = Schema::Null;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            None::<()>
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            None::<i32>
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            None::<String>
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

        assert!(buffer.is_empty());

        Ok(())
    }

    #[test]
    fn test_serialize_bool() -> TestResult {
        let schema = Schema::Boolean;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        4u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        31u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            13u32
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
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

        assert_eq!(buffer.as_slice(), &[8, 62, 14, 113, 130, 2]);

        Ok(())
    }

    #[test]
    fn test_serialize_long() -> TestResult {
        let schema = Schema::Long;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        assert!(
            4u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            31u16
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        13u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            291u64
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            7i8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            (-57i16)
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            129i32
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
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

        assert_eq!(buffer.as_slice(), &[26, 223, 6]);

        Ok(())
    }

    #[test]
    fn test_serialize_float() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        4.7f32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            (-14.1f64)
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

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64]);

        Ok(())
    }

    #[test]
    fn test_serialize_double() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        4.7f32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        (-14.1f32).serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            (-14.1f64)
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

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_bytes() -> TestResult {
        let schema = Schema::Bytes;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        assert!(
            'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            "test"
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
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

        assert_eq!(buffer.as_slice(), &[10, 12, 3, 7, 91, 4]);

        Ok(())
    }

    #[test]
    fn test_serialize_string() -> TestResult {
        let schema = Schema::String;
        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        "test".serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            Bytes::new(&[12, 3, 7, 91, 4])
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config,)?)
                .is_err()
        );
        assert!(
            ().serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            PhantomData::<String>
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );

        assert_eq!(buffer.as_slice(), &[2, b'a', 8, b't', b'e', b's', b't']);

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
        #[serde(rename_all = "camelCase", rename = "TestRecord")]
        struct GoodTestRecord {
            string_field: String,
            int_field: i32,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase", rename = "TestRecord")]
        struct BadTestRecord {
            foo_string_field: String,
            bar_int_field: i32,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        #[derive(Serialize)]
        struct EmptyRecord;
        EmptyRecord.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        #[derive(Serialize)]
        #[serde(rename = "EmptyRecord")]
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
                assert_eq!(value_type, "unit");
                assert_eq!(value, "Expected Schema::Null");
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
        #[serde(rename_all = "UPPERCASE")]
        enum Suit {
            Spades,
            Hearts,
            Diamonds,
            Clubs,
        }

        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value_type, "None");
                assert_eq!(value, "Expected Schema::Union([null, _])");
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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value, "Expected Schema::Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // The 2 at the end is because the DirectArraySerializer immediately writes the length and doesn't
        // know the values will be invalid yet
        assert_eq!(buffer.as_slice(), &[6, 20, 10, 160, 6, 0, 2]);

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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value_type, "str");
                assert_eq!(value, "Expected Schema::String | Schema::Uuid(String)");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // The last 7 bytes are written because the DirectMapSerializer will write immediately and doesn't
        // know yet that the value is not a long
        assert_eq!(
            buffer.as_slice(),
            &[
                4, 10, b'i', b't', b'e', b'm', b'1', 20, 10, b'i', b't', b'e', b'm', b'2', 160, 6,
                0, 2, 10, 105, 116, 101, 109, 49
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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value_type, "str");
                assert_eq!(value, "Expected Schema::String | Schema::Uuid(String)");
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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value, "Expected Schema::Double");
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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value, "Fixed size (8) does not match value length (1)");
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
                // Arrays are serialized as a tuple by Serde
                assert_eq!(value_type, "tuple");
                assert_eq!(value, "Expected Schema::Record(fields.len() == 8)");
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
                // This is a tuple as Serde serializes array as a tuple
                assert_eq!(*value_type, "tuple");
                assert_eq!(value, "Expected Schema::Record(fields.len() == 8)");
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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value_type, "unit");
                assert_eq!(value, "Expected Schema::Null");
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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(value_type, "unit");
                assert_eq!(value, "Expected Schema::Null");
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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        let val = BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2);
        crate::serde::bigdecimal::serialize(
            &val,
            SchemaAwareSerializer::new(&mut buffer, &schema, config)?,
        )?;

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
        let config = Config {
            names: &HashMap::new(),
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
                assert_eq!(
                    value,
                    "Expected Schema::Int | Schema::Date | Schema::TimeMillis"
                );
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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        100_u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            10000_u32
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
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
                assert_eq!(value, "Expected Schema::Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[200, 1, 208, 15, 208, 15, 160, 156, 1]);

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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        100_u8.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        1000_u16.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            10000_u32
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
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
                assert_eq!(value, "Expected Schema::Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[200, 1, 208, 15, 208, 15, 160, 156, 1]);

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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        assert!(
            100_u8
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            1000_u16
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        10000_u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert!(
            1000_i16
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
        assert!(
            10000_i32
                .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                .is_err()
        );
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
                assert_eq!(value, "Expected Schema::Float");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[160, 156, 1, 160, 156, 1,]);

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
            let config = Config {
                names: &HashMap::new(),
                target_block_size: None,
                human_readable: false,
            };

            assert!(
                100_u8
                    .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                    .is_err()
            );
            assert!(
                1000_u16
                    .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                    .is_err()
            );
            10000_u32.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
            assert!(
                1000_i16
                    .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                    .is_err()
            );
            assert!(
                10000_i32
                    .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)
                    .is_err()
            );
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
                    assert_eq!(value, format!("Expected Schema::Double"));
                    assert_eq!(schema, schema);
                }
                unexpected => panic!("Expected an error. Got: {unexpected:?}"),
            }

            assert_eq!(buffer.as_slice(), &[160, 156, 1, 160, 156, 1,]);
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
        let config = Config {
            names: &HashMap::new(),
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
                // This is a tuple because Serde serializes arrays [T; N] as tuples
                assert_eq!(value_type, "tuple");
                assert_eq!(value, "Expected Schema::Record(fields.len() == 12)");
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
            #[serde(with = "crate::serde::bigdecimal")]
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

    // TODO: Figure out what to do with Option<Enum> mapping to Union([Null, ..])
    #[test]
    #[ignore]
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

    // TODO: Figure out what to do with Option<Enum> mapping to Union([Null, ..])
    #[test]
    #[ignore]
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
                ],
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
            c: i64,
            d: f64,
            e: i64,
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
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        'a'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[2, b'a']);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_emoji_char_as_string() -> TestResult {
        let schema = Schema::String;

        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        '👹'.serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;

        assert_eq!(buffer.as_slice(), &[8, 240, 159, 145, 185]);

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
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
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
        let config = Config {
            names: &HashMap::new(),
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
        )?;

        #[derive(Serialize)]
        enum UnionOfFixed {
            #[serde(with = "crate::serde::fixed")]
            Four([u8; 4]),
            #[serde(with = "crate::serde::fixed")]
            Eight([u8; 8]),
        }

        let mut buffer: Vec<u8> = Vec::new();
        let config = Config {
            names: &HashMap::new(),
            target_block_size: None,
            human_readable: false,
        };

        let bytes_written = UnionOfFixed::Four([0, 1, 2, 3])
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert_eq!(bytes_written, 5);

        let bytes_written = UnionOfFixed::Eight([4, 5, 6, 7, 8, 9, 10, 11])
            .serialize(SchemaAwareSerializer::new(&mut buffer, &schema, config)?)?;
        assert_eq!(bytes_written, 9);

        assert_eq!(buffer, &[0, 0, 1, 2, 3, 2, 4, 5, 6, 7, 8, 9, 10, 11][..]);

        Ok(())
    }
}
