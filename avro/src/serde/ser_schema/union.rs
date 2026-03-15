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

use std::{borrow::Borrow, io::Write};

use serde::{Serialize, Serializer};

use super::{Config, MapOrRecordSerializer, SchemaAwareSerializer};
use crate::{
    Error, Schema,
    error::Details,
    schema::{FixedSchema, SchemaKind, UnionSchema},
    serde::{
        ser_schema::{
            block::BlockSerializer,
            record::RecordSerializer,
            tuple::{ManyTupleSerializer, TupleSerializer},
        },
        with::{BytesType, SER_BYTES_TYPE},
    },
    util::{zig_i32, zig_i64},
};

pub struct UnionSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    union: &'s UnionSchema,
    config: Config<'s, S>,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> UnionSerializer<'s, 'w, W, S> {
    pub fn new(writer: &'w mut W, union: &'s UnionSchema, config: Config<'s, S>) -> Self {
        UnionSerializer {
            writer,
            union,
            config,
        }
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::SerializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: Schema::Union(self.union.clone()),
        })
    }

    /// Write an integer to the writer.
    ///
    /// This will check that the current schema is [`Schema::Int`] or a logical type based on that.
    /// This will write the union index.
    pub(super) fn checked_write_int(
        &mut self,
        original_ty: &'static str,
        v: i32,
    ) -> Result<usize, Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Int) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += zig_i32(v, &mut *self.writer)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis in variants",
            ))?
        }
    }

    /// Write a long to the writer.
    ///
    /// This will check that the current schema is [`Schema::Long`] or a logical type based on that.
    /// This will write the union index.
    pub(super) fn checked_write_long(
        &mut self,
        original_ty: &'static str,
        v: i64,
    ) -> Result<usize, Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Long) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += zig_i64(v, &mut *self.writer)?;
            Ok(bytes_written)
        } else {
            Err(self.error(original_ty, "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos} in variants"))
        }
    }

    /// Write bytes to the writer with preceding length header.
    ///
    /// This does not check the current schema and does not write the union index.
    fn write_bytes_with_len(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written = 0;
        bytes_written += zig_i64(bytes.len() as i64, &mut *self.writer)?;
        bytes_written += self.write_bytes(bytes)?;
        Ok(bytes_written)
    }

    /// Write bytes to the writer.
    ///
    /// This does not check the current schema and does not write the union index.
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        self.writer.write_all(bytes).map_err(Details::WriteBytes)?;
        Ok(bytes.len())
    }

    /// Write an array of `n` bytes to the writer.
    ///
    /// This does not check the current schema and does not write the union index.
    fn write_array<const N: usize>(&mut self, bytes: [u8; N]) -> Result<usize, Error> {
        self.write_bytes(&bytes)?;
        Ok(N)
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> Serializer for UnionSerializer<'s, 'w, W, S> {
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
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Boolean) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += self.write_array([u8::from(v)])?;
            Ok(bytes_written)
        } else {
            Err(self.error("bool", "Expected Schema::Boolean in variants"))?
        }
    }

    fn serialize_i8(mut self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i8", i32::from(v))
    }

    fn serialize_i16(mut self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i16", i32::from(v))
    }

    fn serialize_i32(mut self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i32", v)
    }

    fn serialize_i64(mut self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.checked_write_long("i64", v)
    }

    fn serialize_i128(mut self, v: i128) -> Result<Self::Ok, Self::Error> {
        match self.union.find_named_schema("i128", self.config.names)? {
            Some((index, Schema::Fixed(FixedSchema { size: 16, .. }))) => {
                let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                bytes_written += self.write_array(v.to_le_bytes())?;
                Ok(bytes_written)
            }
            _ => Err(self.error(
                "i128",
                r#"Expected Schema::Fixed(name: "i128", size: 16) in variants"#,
            )),
        }
    }

    fn serialize_u8(mut self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("u8", i32::from(v))
    }

    fn serialize_u16(mut self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("u16", i32::from(v))
    }

    fn serialize_u32(mut self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.checked_write_long("u32", i64::from(v))
    }

    fn serialize_u64(mut self, v: u64) -> Result<Self::Ok, Self::Error> {
        match self.union.find_named_schema("u64", self.config.names)? {
            Some((index, Schema::Fixed(FixedSchema { size: 16, .. }))) => {
                let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                bytes_written += self.write_array(v.to_le_bytes())?;
                Ok(bytes_written)
            }
            _ => Err(self.error(
                "u64",
                r#"Expected Schema::Fixed(name: "u64", size: 8) in variants"#,
            )),
        }
    }

    fn serialize_u128(mut self, v: u128) -> Result<Self::Ok, Self::Error> {
        match self.union.find_named_schema("u128", self.config.names)? {
            Some((index, Schema::Fixed(FixedSchema { size: 16, .. }))) => {
                let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                bytes_written += self.write_array(v.to_le_bytes())?;
                Ok(bytes_written)
            }
            _ => Err(self.error(
                "u128",
                r#"Expected Schema::Fixed(name: "u128", size: 16) in variants"#,
            )),
        }
    }

    fn serialize_f32(mut self, v: f32) -> Result<Self::Ok, Self::Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Float) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += self.write_array(v.to_le_bytes())?;
            Ok(bytes_written)
        } else {
            Err(self.error("f32", "Expected Schema::Float in variants"))?
        }
    }

    fn serialize_f64(mut self, v: f64) -> Result<Self::Ok, Self::Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Double) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += self.write_array(v.to_le_bytes())?;
            Ok(bytes_written)
        } else {
            Err(self.error("f64", "Expected Schema::Double in variants"))?
        }
    }

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::String) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += self.write_bytes_with_len(v.to_string().as_bytes())?;
            Ok(bytes_written)
        } else {
            Err(self.error("char", "Expected Schema::String in variants"))?
        }
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::String) {
            let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            bytes_written += self.write_bytes_with_len(v.as_bytes())?;
            Ok(bytes_written)
        } else {
            Err(self.error("str", "Expected Schema::String in variants"))?
        }
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let (index, with_len) = match SER_BYTES_TYPE.get() {
            BytesType::Bytes => {
                if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Bytes) {
                    (index, true)
                } else {
                    return Err(self.error("bytes", "Expected Schema::Bytes in variants"));
                }
            }
            BytesType::Fixed => {
                if let Some((index, _)) = self
                    .union
                    .find_fixed_of_size_n(v.len(), self.config.names)?
                {
                    (index, false)
                } else {
                    return Err(self.error(
                        "bytes",
                        format!("Expected Schema::Fixed(size: {}) in variants", v.len()),
                    ));
                }
            }
            BytesType::Unset => {
                let bytes_index = self.union.index_of_schema_kind(SchemaKind::Bytes);
                let fixed_index = self
                    .union
                    .find_fixed_of_size_n(v.len(), self.config.names)?;
                // Find the first variant that matches the bytes or fixed
                match (bytes_index, fixed_index) {
                    (Some(bytes_index), Some((fixed_index, _))) => {
                        (bytes_index.min(fixed_index), bytes_index < fixed_index)
                    }
                    (Some(bytes_index), None) => (bytes_index, true),
                    (None, Some((fixed_index, _))) => (fixed_index, false),
                    (None, None) => {
                        return Err(self.error(
                            "bytes",
                            format!(
                                "Expected Schema::Bytes | Schema::Fixed(size: {}) in variants",
                                v.len()
                            ),
                        ));
                    }
                }
            }
        };
        let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
        if with_len {
            bytes_written += self.write_bytes_with_len(v)?;
        } else {
            bytes_written += self.write_bytes(v)?;
        }
        Ok(bytes_written)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(self.error("none", "Nested unions are not supported"))
    }

    fn serialize_some<T>(self, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(self.error("some", "Nested unions are not supported"))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Null) {
            zig_i32(index as i32, &mut *self.writer)
        } else {
            Err(self.error("unit", "Expected Schema::Null in variants"))
        }
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        match self.union.find_named_schema(name, self.config.names)? {
            Some((index, Schema::Record(record))) if record.fields.is_empty() => {
                zig_i32(index as i32, &mut *self.writer)
            }
            _ => Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields: []) in variants"),
            )),
        }
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        match self.union.find_named_schema(name, self.config.names)? {
            Some((index, Schema::Enum(schema))) if schema.symbols[variant_index as usize] == variant => {
                let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                bytes_written += zig_i32(variant_index as i32, &mut *self.writer)?;
                Ok(bytes_written)
            }
            _ => Err(self.error("unit variant", format!("Expected Schema::Enum(name: {name}, symbols[{variant_index}] == {variant}) in variants"))),
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
        match self.union.find_named_schema(name, self.config.names)? {
            Some((index, Schema::Record(record))) if record.fields.len() == 1 => {
                let mut bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                bytes_written += value.serialize(SchemaAwareSerializer::new(
                    self.writer,
                    &record.fields[0].schema,
                    self.config,
                )?)?;
                Ok(bytes_written)
            }
            _ => Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields: [_]) in variants"),
            )),
        }
    }

    fn serialize_newtype_variant<T>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(self.error("newtype variant", "Nested unions are not supported"))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Array)
            && let Schema::Array(array) = &self.union.variants()[index]
        {
            let bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            BlockSerializer::array(self.writer, array, self.config, len, Some(bytes_written))
        } else {
            Err(self.error("array", "Expected Schema::Array in variants"))
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        if len == 0 {
            if let Some(index) = self.union.index_of_schema_kind(SchemaKind::Null) {
                let bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                Ok(TupleSerializer::unit(Some(bytes_written)))
            } else {
                Err(self.error("tuple", "Expected Schema::Null in variants for 0-tuple"))
            }
        } else if len == 1 {
            Ok(TupleSerializer::one_union(
                self.writer,
                self.union,
                self.config,
            ))
        } else if let Some((index, record)) = self
            .union
            .find_record_with_n_fields(len, self.config.names)?
        {
            let bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            Ok(TupleSerializer::many(
                self.writer,
                record,
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error(
                "tuple",
                format!(
                    "Expected Schema::Record(fields.len() == {len}) in variants for {len}-tuple"
                ),
            ))
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        match self.union.find_named_schema(name, self.config.names)? {
            Some((index, Schema::Record(record))) if record.fields.len() == len => {
                let bytes_written = zig_i32(index as i32, &mut *self.writer)?;
                Ok(ManyTupleSerializer::new(
                    self.writer,
                    record,
                    self.config,
                    Some(bytes_written),
                ))
            }
            _ => Err(self.error(
                "tuple struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len}) in variants"),
            )),
        }
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(self.error("tuple variant", "Nested unions are not supported"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let map_index = self.union.index_of_schema_kind(SchemaKind::Map).map(|i| {
            if let Schema::Map(map) = &self.union.variants()[i] {
                (i, map)
            } else {
                unreachable!("SchemaKind is Map so Schema must also be a Map")
            }
        });
        let record_index = if let Some(len) = len {
            self.union
                .find_record_with_n_fields(len, self.config.names)?
        } else {
            None
        };
        match (map_index, record_index) {
            (Some((map_index, map)), Some((record_index, record))) => {
                let bytes_written = zig_i32(map_index.min(record_index) as i32, &mut *self.writer)?;
                if map_index < record_index {
                    MapOrRecordSerializer::map(
                        self.writer,
                        map,
                        self.config,
                        len,
                        Some(bytes_written),
                    )
                } else {
                    Ok(MapOrRecordSerializer::record(
                        self.writer,
                        record,
                        self.config,
                        Some(bytes_written),
                    ))
                }
            }
            (Some((map_index, map)), None) => {
                let bytes_written = zig_i32(map_index as i32, &mut *self.writer)?;
                MapOrRecordSerializer::map(self.writer, map, self.config, len, Some(bytes_written))
            }
            (None, Some((record_index, record))) => {
                let bytes_written = zig_i32(record_index as i32, &mut *self.writer)?;
                Ok(MapOrRecordSerializer::record(
                    self.writer,
                    record,
                    self.config,
                    Some(bytes_written),
                ))
            }
            (None, None) => Err(self.error(
                "map",
                "Expected Schema::Map or Schema::Record for structs with flattened fields in variants",
            )),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        if let Some((index, Schema::Record(record))) =
            self.union.find_named_schema(name, self.config.names)?
        {
            let bytes_written = zig_i32(index as i32, &mut *self.writer)?;
            Ok(RecordSerializer::new(
                self.writer,
                record,
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error(
                "struct",
                format!("Expected Schema::Record(name: {name}) in variants"),
            ))
        }
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(self.error("struct variant", "Nested unions are not supported"))
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}
