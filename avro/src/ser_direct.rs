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

//! Logic for serde-compatible serialization which writes directly to a `Write` stream

use std::{borrow::Cow, io::Write};

use serde::ser;

use crate::{
    encode::{encode_int, encode_long},
    error::Error,
    schema::{Name, NamesRef, Namespace, RecordSchema, Schema},
};

const RECORD_FIELD_INIT_BUFFER_SIZE: usize = 64;
const COLLECTION_SERIALIZER_ITEM_LIMIT: usize = 1024;
const COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY: usize = 32;
const SINGLE_VALUE_INIT_BUFFER_SIZE: usize = 128;

/// The sequence serializer for `DirectSerializer`.  `DirectSerializeSeq` may break large arrays up
/// into multiple blocks to avoid having to obtain the length of the entire array before being able
/// to write any data to the underlying [`std::fmt::Write`] stream.  (See the [Data Seralization and
/// Deserialization](https://avro.apache.org/docs/1.12.0/specification/#data-serialization-and-deserialization)
/// section of the Avro spec for more info.)
pub struct DirectSerializeSeq<'a, 's, W: Write> {
    ser: &'a mut DirectSerializer<'s, W>,
    item_schema: &'s Schema,
    item_buffer_size: usize,
    item_buffers: Vec<Vec<u8>>,
    bytes_written: usize,
}

impl<'a, 's, W: Write> DirectSerializeSeq<'a, 's, W> {
    fn new(
        ser: &'a mut DirectSerializer<'s, W>,
        item_schema: &'s Schema,
        len: Option<usize>,
    ) -> DirectSerializeSeq<'a, 's, W> {
        DirectSerializeSeq {
            ser,
            item_schema,
            item_buffer_size: SINGLE_VALUE_INIT_BUFFER_SIZE,
            item_buffers: Vec::with_capacity(
                len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY),
            ),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if !self.item_buffers.is_empty() {
            self.bytes_written +=
                encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
            for item in self.item_buffers.drain(..) {
                self.bytes_written += self
                    .ser
                    .writer
                    .write(item.as_slice())
                    .map_err(Error::WriteBytes)?;
            }
        }

        Ok(())
    }

    fn serialize_element<T: ser::Serialize>(&mut self, value: &T) -> Result<(), Error> {
        let mut item_buffer: Vec<u8> = Vec::with_capacity(self.item_buffer_size);
        let mut item_ser = DirectSerializer::new(
            &mut item_buffer,
            self.item_schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        value.serialize(&mut item_ser)?;

        self.item_buffer_size = std::cmp::max(self.item_buffer_size, item_buffer.len() + 16);

        self.item_buffers.push(item_buffer);

        if self.item_buffers.len() > COLLECTION_SERIALIZER_ITEM_LIMIT {
            self.write_buffered_items()?;
        }

        Ok(())
    }

    fn end(mut self) -> Result<usize, Error> {
        self.write_buffered_items()?;
        self.bytes_written += self.ser.writer.write(&[0u8]).map_err(Error::WriteBytes)?;

        Ok(self.bytes_written)
    }
}

impl<W: Write> ser::SerializeSeq for DirectSerializeSeq<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_element(&value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<W: Write> ser::SerializeTuple for DirectSerializeSeq<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

/// The map serializer for `DirectSerializer`.  `DirectSerializeMap` may break large maps up
/// into multiple blocks to avoid having to obtain the length of the entire array before being able
/// to write any data to the underlying [`std::fmt::Write`] stream.  (See the [Data Seralization and
/// Deserialization](https://avro.apache.org/docs/1.12.0/specification/#data-serialization-and-deserialization)
/// section of the Avro spec for more info.)
pub struct DirectSerializeMap<'a, 's, W: Write> {
    ser: &'a mut DirectSerializer<'s, W>,
    item_schema: &'s Schema,
    item_buffer_size: usize,
    item_buffers: Vec<Vec<u8>>,
    bytes_written: usize,
}

impl<'a, 's, W: Write> DirectSerializeMap<'a, 's, W> {
    fn new(
        ser: &'a mut DirectSerializer<'s, W>,
        item_schema: &'s Schema,
        len: Option<usize>,
    ) -> DirectSerializeMap<'a, 's, W> {
        DirectSerializeMap {
            ser,
            item_schema,
            item_buffer_size: SINGLE_VALUE_INIT_BUFFER_SIZE,
            item_buffers: Vec::with_capacity(
                len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY),
            ),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if !self.item_buffers.is_empty() {
            self.bytes_written +=
                encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
            for item in self.item_buffers.drain(..) {
                self.bytes_written += self
                    .ser
                    .writer
                    .write(item.as_slice())
                    .map_err(Error::WriteBytes)?;
            }
        }

        Ok(())
    }
}

impl<W: Write> ser::SerializeMap for DirectSerializeMap<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let mut element_buffer: Vec<u8> = Vec::with_capacity(self.item_buffer_size);
        let string_schema = Schema::String;
        let mut key_ser = DirectSerializer::new(
            &mut element_buffer,
            &string_schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        key.serialize(&mut key_ser)?;

        self.item_buffers.push(element_buffer);

        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let last_index = self.item_buffers.len() - 1;
        let element_buffer = &mut self.item_buffers[last_index];
        let mut val_ser = DirectSerializer::new(
            element_buffer,
            self.item_schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        value.serialize(&mut val_ser)?;

        self.item_buffer_size = std::cmp::max(self.item_buffer_size, element_buffer.len() + 16);

        if self.item_buffers.len() > COLLECTION_SERIALIZER_ITEM_LIMIT {
            self.write_buffered_items()?;
        }

        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.write_buffered_items()?;
        self.bytes_written += self.ser.writer.write(&[0u8]).map_err(Error::WriteBytes)?;

        Ok(self.bytes_written)
    }
}

/// The struct serializer for `DirectSerializer`, which can serialize Avro records.  `DirectSerializeStruct`
/// can accept fields out of order, but doing so incurs a performance penalty, since it requires
/// `DirectSerializeStruct` to buffer serialized values in order to write them to the stream in order.
pub struct DirectSerializeStruct<'a, 's, W: Write> {
    ser: &'a mut DirectSerializer<'s, W>,
    record_schema: &'s RecordSchema,
    item_count: usize,
    buffered_fields: Vec<Option<Vec<u8>>>,
    bytes_written: usize,
}

impl<'a, 's, W: Write> DirectSerializeStruct<'a, 's, W> {
    fn new(
        ser: &'a mut DirectSerializer<'s, W>,
        record_schema: &'s RecordSchema,
        len: usize,
    ) -> DirectSerializeStruct<'a, 's, W> {
        DirectSerializeStruct {
            ser,
            record_schema,
            item_count: 0,
            buffered_fields: vec![None; len],
            bytes_written: 0,
        }
    }

    fn serialize_next_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let next_field = self.record_schema.fields.get(self.item_count).expect(
            "Validity of the next field index was expected to have been checked by the caller",
        );

        // If we receive fields in order, write them directly to the main writer
        let mut value_ser = DirectSerializer::new(
            &mut *self.ser.writer,
            &next_field.schema,
            self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        self.bytes_written += value.serialize(&mut value_ser)?;

        self.item_count += 1;

        // Write any buffered data to the stream that has now become next in line
        while let Some(buffer) = self
            .buffered_fields
            .get_mut(self.item_count)
            .and_then(|b| b.take())
        {
            self.bytes_written += self
                .ser
                .writer
                .write(buffer.as_slice())
                .map_err(Error::WriteBytes)?;
            self.item_count += 1;
        }

        Ok(())
    }

    fn end(self) -> Result<usize, Error> {
        if self.item_count != self.record_schema.fields.len() {
            Err(Error::GetField(
                self.record_schema.fields[self.item_count].name.clone(),
            ))
        } else {
            Ok(self.bytes_written)
        }
    }
}

impl<W: Write> ser::SerializeStruct for DirectSerializeStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        if self.item_count >= self.record_schema.fields.len() {
            return Err(Error::FieldName(String::from(key)));
        }

        let next_field = &self.record_schema.fields[self.item_count];
        let next_field_matches = match &next_field.aliases {
            Some(aliases) => {
                key == next_field.name.as_str() || aliases.iter().any(|a| key == a.as_str())
            }
            None => key == next_field.name.as_str(),
        };

        if next_field_matches {
            self.serialize_next_field(&value).map_err(|e| {
                Error::SerializeRecordFieldWithSchema {
                    field_name: key,
                    record_schema: Schema::Record(self.record_schema.clone()),
                    error: Box::new(e),
                }
            })?;
            Ok(())
        } else {
            if self.item_count < self.record_schema.fields.len() {
                for i in self.item_count..self.record_schema.fields.len() {
                    let field = &self.record_schema.fields[i];
                    let field_matches = match &field.aliases {
                        Some(aliases) => {
                            key == field.name.as_str() || aliases.iter().any(|a| key == a.as_str())
                        }
                        None => key == field.name.as_str(),
                    };

                    if field_matches {
                        let mut buffer: Vec<u8> = Vec::with_capacity(RECORD_FIELD_INIT_BUFFER_SIZE);
                        let mut value_ser = DirectSerializer::new(
                            &mut buffer,
                            &field.schema,
                            self.ser.names,
                            self.ser.enclosing_namespace.clone(),
                        );
                        value.serialize(&mut value_ser).map_err(|e| {
                            Error::SerializeRecordFieldWithSchema {
                                field_name: key,
                                record_schema: Schema::Record(self.record_schema.clone()),
                                error: Box::new(e),
                            }
                        })?;

                        self.buffered_fields[i] = Some(buffer);

                        return Ok(());
                    }
                }
            }

            Err(Error::FieldName(String::from(key)))
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<W: Write> ser::SerializeStructVariant for DirectSerializeStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        ser::SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeStruct::end(self)
    }
}

/// The tuple struct serializer for `DirectSerializer`.  `DirectSerializeTupleStruct` can serialize to an Avro
/// array or record.  When serializing to a record, fields must be provided in the correct order, since no
/// names names are provided.
pub enum DirectSerializeTupleStruct<'a, 's, W: Write> {
    Record(DirectSerializeStruct<'a, 's, W>),
    Array(DirectSerializeSeq<'a, 's, W>),
}

impl<W: Write> DirectSerializeTupleStruct<'_, '_, W> {
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        use DirectSerializeTupleStruct::*;
        match self {
            Record(record_ser) => record_ser.serialize_next_field(&value),
            Array(array_ser) => array_ser.serialize_element(&value),
        }
    }

    fn end(self) -> Result<usize, Error> {
        use DirectSerializeTupleStruct::*;
        match self {
            Record(record_ser) => record_ser.end(),
            Array(array_ser) => array_ser.end(),
        }
    }
}

impl<W: Write> ser::SerializeTupleStruct for DirectSerializeTupleStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_field(&value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<W: Write> ser::SerializeTupleVariant for DirectSerializeTupleStruct<'_, '_, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_field(&value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

/// A `serde::se::Serializer` implementation that serializes directly to a [`std::fmt::Write`] using the provided
/// schema.  If `DirectSerializer` isn't able to match the incoming data with its schema, it will return an error.
///
/// A `DirectSerializer` instance can be re-used to serialize multiple values matching the schema to its
/// [`std::fmt::Write`] stream.
pub struct DirectSerializer<'s, W: Write> {
    writer: &'s mut W,
    root_schema: &'s Schema,
    names: &'s NamesRef<'s>,
    enclosing_namespace: Namespace,
    schema_stack: Vec<&'s Schema>,
}

impl<'s, W: Write> DirectSerializer<'s, W> {
    /// Create a new `DirectSerializer`.
    ///
    /// `writer` is the [`std::fmt::Write`] stream to be written to.
    ///
    /// `schema` is the schema of the value to be written.
    ///
    /// `names` is the mapping of schema names to schemas, to be used for type reference lookups
    ///
    /// `enclosing_namespace` is the enclosing namespace to be used for type reference lookups
    pub fn new(
        writer: &'s mut W,
        schema: &'s Schema,
        names: &'s NamesRef<'s>,
        enclosing_namespace: Namespace,
    ) -> DirectSerializer<'s, W> {
        DirectSerializer {
            writer,
            root_schema: schema,
            names,
            enclosing_namespace,
            schema_stack: Vec::new(),
        }
    }

    fn get_ref_schema(&self, name: &'s Name) -> Result<&'s Schema, Error> {
        let full_name = match name.namespace {
            Some(_) => Cow::Borrowed(name),
            None => Cow::Owned(Name {
                name: name.name.clone(),
                namespace: self.enclosing_namespace.clone(),
            }),
        };

        let ref_schema = self.names.get(full_name.as_ref()).copied();

        ref_schema.ok_or_else(|| Error::SchemaResolutionError(full_name.as_ref().clone()))
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written: usize = 0;

        bytes_written += encode_long(bytes.len() as i64, &mut self.writer)?;
        bytes_written += self.writer.write(bytes).map_err(Error::WriteBytes)?;

        Ok(bytes_written)
    }
}

impl<'a, 's, W: Write> ser::Serializer for &'a mut DirectSerializer<'s, W> {
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = DirectSerializeSeq<'a, 's, W>;
    type SerializeTuple = DirectSerializeSeq<'a, 's, W>;
    type SerializeTupleStruct = DirectSerializeTupleStruct<'a, 's, W>;
    type SerializeTupleVariant = DirectSerializeTupleStruct<'a, 's, W>;
    type SerializeMap = DirectSerializeMap<'a, 's, W>;
    type SerializeStruct = DirectSerializeStruct<'a, 's, W>;
    type SerializeStructVariant = DirectSerializeStruct<'a, 's, W>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "bool",
            value: format!("{v}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Boolean => self.writer.write(&[u8::from(v)]).map_err(Error::WriteBytes),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Boolean => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_bool(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i32(v as i32)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "int (i8 | i16 | i32)",
            value: format!("{v}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => encode_int(v, &mut self.writer),
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
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
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_i32(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = |cause: Option<String>| {
            let cause = cause
                .map(|c| format!(". Cause: {}", c))
                .unwrap_or("".to_string());
            Error::SerializeValueWithSchema {
                value_type: "i64",
                value: format!("{v}{cause}"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value =
                    i32::try_from(v).map_err(|cause| create_error(Some(cause.to_string())))?;
                encode_int(int_value, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(v, &mut self.writer),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
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
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_i64(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(None))
            }
            _ => Err(create_error(None)),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "u8",
            value: format!("{v}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                encode_int(v as i32, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            Schema::Bytes => self.write_bytes(&[v]),
            Schema::Union(sch) => {
                for (i, variant_sch) in sch.schemas.iter().enumerate() {
                    match variant_sch {
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
                        | Schema::LocalTimestampNanos
                        | Schema::Bytes => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_sch);
                            return self.serialize_u8(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(v as u32)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = |cause: Option<String>| {
            let cause = cause
                .map(|c| format!(". Cause: {}", c))
                .unwrap_or("".to_string());
            Error::SerializeValueWithSchema {
                value_type: "unsigned int (u16 | u32)",
                value: format!("{v}{cause}"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value =
                    i32::try_from(v).map_err(|cause| create_error(Some(cause.to_string())))?;
                encode_int(int_value, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            Schema::Union(sch) => {
                for (i, variant_sch) in sch.schemas.iter().enumerate() {
                    match variant_sch {
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
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_sch);
                            return self.serialize_u32(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(None))
            }
            _ => Err(create_error(None)),
        }
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = |cause: Option<String>| {
            let cause = cause
                .map(|c| format!(". Cause: {}", c))
                .unwrap_or("".to_string());
            Error::SerializeValueWithSchema {
                value_type: "u64",
                value: format!("{v}{cause}"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value =
                    i32::try_from(v).map_err(|cause| create_error(Some(cause.to_string())))?;
                encode_int(int_value, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                let long_value =
                    i64::try_from(v).map_err(|cause| create_error(Some(cause.to_string())))?;
                encode_long(long_value, &mut self.writer)
            }
            Schema::Union(sch) => {
                for (i, variant_sch) in sch.schemas.iter().enumerate() {
                    match variant_sch {
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
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_sch);
                            return self.serialize_u64(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(None))
            }
            _ => Err(create_error(None)),
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "f32",
            value: format!("{v}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Float => self
                .writer
                .write(&v.to_le_bytes())
                .map_err(Error::WriteBytes),
            Schema::Double => self
                .writer
                .write(&(v as f64).to_le_bytes())
                .map_err(Error::WriteBytes),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Float | Schema::Double => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_f32(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "f64",
            value: format!("{v}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Float => self
                .writer
                .write(&(v as f32).to_le_bytes())
                .map_err(Error::WriteBytes),
            Schema::Double => self
                .writer
                .write(&v.to_le_bytes())
                .map_err(Error::WriteBytes),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Float | Schema::Double => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_f64(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "char",
            value: String::from(v),
            schema: schema.clone(),
        };

        match schema {
            Schema::String | Schema::Bytes => self.write_bytes(String::from(v).as_bytes()),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::String | Schema::Bytes => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_char(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "string",
            value: String::from(v),
            schema: schema.clone(),
        };

        match schema {
            Schema::String | Schema::Bytes | Schema::Uuid | Schema::BigDecimal => {
                self.write_bytes(v.as_bytes())
            }
            Schema::Fixed(fixed_schema) => {
                if v.len() == fixed_schema.size {
                    self.writer.write(v.as_bytes()).map_err(Error::WriteBytes)
                } else {
                    Err(create_error())
                }
            }
            Schema::Ref { name } => {
                let ref_schema = self.get_ref_schema(name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_str(v)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::String
                        | Schema::Bytes
                        | Schema::Uuid
                        | Schema::Fixed(_)
                        | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_str(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || {
            use std::fmt::Write;

            let mut v_str = String::with_capacity(v.len());
            for b in v {
                if write!(&mut v_str, "{:x}", b).is_err() {
                    v_str.push_str("??");
                }
            }
            Error::SerializeValueWithSchema {
                value_type: "bytes",
                value: v_str,
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::String | Schema::Bytes | Schema::Uuid | Schema::BigDecimal => {
                self.write_bytes(v)
            }
            Schema::Fixed(fixed_schema) => {
                if v.len() == fixed_schema.size {
                    self.writer.write(v).map_err(Error::WriteBytes)
                } else {
                    Err(create_error())
                }
            }
            Schema::Duration => {
                if v.len() == 12 {
                    self.writer.write(v).map_err(Error::WriteBytes)
                } else {
                    Err(create_error())
                }
            }
            Schema::Decimal(sch) => match sch.inner.as_ref() {
                Schema::Bytes => self.write_bytes(v),
                Schema::Fixed(fixed_schema) => match fixed_schema.size.checked_sub(v.len()) {
                    Some(pad) => {
                        let pad_val = match v.len() {
                            0 => 0,
                            _ => v[0],
                        };
                        let padding = vec![pad_val; pad];
                        self.writer
                            .write(padding.as_slice())
                            .map_err(Error::WriteBytes)?;
                        self.writer.write(v).map_err(Error::WriteBytes)
                    }
                    None => Err(Error::CompareFixedSizes {
                        size: fixed_schema.size,
                        n: v.len(),
                    }),
                },
                _ => Err(create_error()),
            },
            Schema::Ref { name } => {
                let ref_schema = self.get_ref_schema(name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_bytes(v)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::String
                        | Schema::Bytes
                        | Schema::Uuid
                        | Schema::BigDecimal
                        | Schema::Fixed(_)
                        | Schema::Duration
                        | Schema::Decimal(_)
                        | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_bytes(v);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "none",
            value: String::from("None"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Null => Ok(0),
            Schema::Ref { name: _ } => Ok(0),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Null => {
                            return encode_int(i as i32, &mut *self.writer);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "some",
            value: String::from("Some(?)"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Null => { /* skip */ }
                        _ => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return value.serialize(self);
                        }
                    }
                }
                Err(create_error())
            }
            _ => value.serialize(self),
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = |cause: Option<String>| {
            let cause = cause
                .map(|c| format!(". Cause: {}", c))
                .unwrap_or("".to_string());
            Error::SerializeValueWithSchema {
                value_type: "unit struct",
                value: format!("{name}{cause}"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Record(sch) => match sch.fields.len() {
                0 => Ok(0),
                too_many => Err(create_error(Some(format!(
                    "Too many fields: {}. Expected: 0",
                    too_many
                )))),
            },
            Schema::Null => Ok(0),
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_unit_struct(name)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Record(record_schema) if record_schema.fields.is_empty() => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_unit_struct(name);
                        }
                        Schema::Null | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_unit_struct(name);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error(None))
            }
            _ => Err(create_error(None)),
        }
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = |cause: Option<String>| {
            let cause = cause
                .map(|c| format!(". Cause: {}", c))
                .unwrap_or("".to_string());
            Error::SerializeValueWithSchema {
                value_type: "unit variant",
                value: format!("{name}::{variant} (index={variant_index}){cause}"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Enum(enum_schema) => {
                if variant_index as usize >= enum_schema.symbols.len() {
                    return Err(create_error(Some(format!(
                        "Variant index out of bounds: {}. The Enum schema has '{}' symbols",
                        variant_index,
                        enum_schema.symbols.len()
                    ))));
                }

                encode_int(variant_index as i32, &mut self.writer)
            }
            Schema::Union(union_schema) => {
                if variant_index as usize >= union_schema.schemas.len() {
                    return Err(create_error(Some(format!(
                        "Variant index out of bounds: {}. The union schema has '{}' schemas",
                        variant_index,
                        union_schema.schemas.len()
                    ))));
                }

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack
                    .push(&union_schema.schemas[variant_index as usize]);
                self.serialize_unit_struct(name)
            }
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_unit_variant(name, variant_index, variant)
            }
            unsupported => Err(create_error(Some(format!(
                "Unsupported schema: {:?}. Expected: Enum, Union or Ref",
                unsupported
            )))),
        }
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        // Treat any newtype struct as a transparent wrapper around the contained type
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "newtype variant",
            value: format!("{name}::{variant}(?) (index={variant_index})"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Union(sch) => {
                let variant_schema = sch
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(create_error)?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(variant_schema);
                self.serialize_newtype_struct(variant, value)
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || {
            let len_str = len
                .map(|l| format!("{l}"))
                .unwrap_or_else(|| String::from("?"));

            Error::SerializeValueWithSchema {
                value_type: "sequence",
                value: format!("sequence (len={len_str})"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Array(array_schema) => Ok(DirectSerializeSeq::new(
                self,
                array_schema.items.as_ref(),
                len,
            )),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Array(_) => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_seq(len);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "tuple",
            value: format!("tuple (len={len})"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Array(array_schema) => Ok(DirectSerializeSeq::new(
                self,
                array_schema.items.as_ref(),
                Some(len),
            )),
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Array(_) => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_tuple(len);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "tuple struct",
            value: format!("{name}({})", vec!["?"; len].as_slice().join(",")),
            schema: schema.clone(),
        };

        match schema {
            Schema::Array(sch) => Ok(DirectSerializeTupleStruct::Array(DirectSerializeSeq::new(
                self,
                &sch.items,
                Some(len),
            ))),
            Schema::Record(sch) => Ok(DirectSerializeTupleStruct::Record(
                DirectSerializeStruct::new(self, sch, len),
            )),
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_tuple_struct(name, len)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Record(inner) => {
                            if inner.fields.len() == len {
                                encode_int(i as i32, &mut *self.writer)?;
                                self.schema_stack.push(variant_schema);
                                return self.serialize_tuple_struct(name, len);
                            }
                        }
                        Schema::Array(_) | Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_tuple_struct(name, len);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "tuple variant",
            value: format!(
                "{name}::{variant}({}) (index={variant_index})",
                vec!["?"; len].as_slice().join(",")
            ),
            schema: schema.clone(),
        };

        match schema {
            Schema::Union(union_schema) => {
                let variant_schema = union_schema
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(create_error)?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(variant_schema);
                self.serialize_tuple_struct(variant, len)
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || {
            let len_str = len
                .map(|l| format!("{}", l))
                .unwrap_or_else(|| String::from("?"));

            Error::SerializeValueWithSchema {
                value_type: "map",
                value: format!("map (size={len_str})"),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::Map(map_schema) => Ok(DirectSerializeMap::new(
                self,
                map_schema.types.as_ref(),
                len,
            )),
            Schema::Union(union_schema) => {
                for (i, variant_sch) in union_schema.schemas.iter().enumerate() {
                    match variant_sch {
                        Schema::Map(_) => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_sch);
                            return self.serialize_map(len);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "struct",
            value: format!("{name}{{ ... }}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Record(record_schema) => {
                Ok(DirectSerializeStruct::new(self, record_schema, len))
            }
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_struct(name, len)
            }
            Schema::Union(union_schema) => {
                for (i, variant_schema) in union_schema.schemas.iter().enumerate() {
                    match variant_schema {
                        Schema::Record(inner)
                            if inner.fields.len() == len && inner.name.name == name =>
                        {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_struct(name, len);
                        }
                        Schema::Ref { name: _ } => {
                            encode_int(i as i32, &mut *self.writer)?;
                            self.schema_stack.push(variant_schema);
                            return self.serialize_struct(name, len);
                        }
                        _ => { /* skip */ }
                    }
                }
                Err(create_error())
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "struct variant",
            value: format!("{name}::{variant}{{ ... }} (size={len}"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Union(union_schema) => {
                let variant_schema = union_schema
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(create_error)?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(variant_schema);
                self.serialize_struct(variant, len)
            }
            _ => Err(create_error()),
        }
    }

    fn is_human_readable(&self) -> bool {
        crate::util::is_human_readable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decimal::Decimal, schema::ResolvedSchema, Days, Duration, Millis, Months};
    use apache_avro_test_helper::TestResult;
    use bigdecimal::BigDecimal;
    use num_bigint::{BigInt, Sign};
    use serde::Serialize;
    use serde_bytes::{ByteArray, Bytes};
    use serial_test::serial;
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        ().serialize(&mut serializer)?;
        None::<()>.serialize(&mut serializer)?;
        None::<i32>.serialize(&mut serializer)?;
        None::<String>.serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), Vec::<u8>::new().as_slice());

        Ok(())
    }

    #[test]
    fn test_serialize_bool() -> TestResult {
        let schema = Schema::Boolean;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        true.serialize(&mut serializer)?;
        false.serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[1, 0]);

        Ok(())
    }

    #[test]
    fn test_serialize_int() -> TestResult {
        let schema = Schema::Int;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4u8.serialize(&mut serializer)?;
        31u16.serialize(&mut serializer)?;
        13u32.serialize(&mut serializer)?;
        7i8.serialize(&mut serializer)?;
        (-57i16).serialize(&mut serializer)?;
        129i32.serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[8, 62, 26, 14, 113, 130, 2]);

        Ok(())
    }

    #[test]
    fn test_serialize_long() -> TestResult {
        let schema = Schema::Long;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4u8.serialize(&mut serializer)?;
        31u16.serialize(&mut serializer)?;
        13u32.serialize(&mut serializer)?;
        291u64.serialize(&mut serializer)?;
        7i8.serialize(&mut serializer)?;
        (-57i16).serialize(&mut serializer)?;
        129i32.serialize(&mut serializer)?;
        (-432i64).serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4.7f32.serialize(&mut serializer)?;
        (-14.1f64).serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_double() -> TestResult {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4.7f32.serialize(&mut serializer)?;
        (-14.1f64).serialize(&mut serializer)?;
        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);

        Ok(())
    }

    #[test]
    fn test_serialize_bytes() -> TestResult {
        let schema = Schema::Bytes;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        'a'.serialize(&mut serializer)?;
        "test".serialize(&mut serializer)?;
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(&mut serializer)?;
        assert!(().serialize(&mut serializer).is_err());
        assert!(PhantomData::<String>.serialize(&mut serializer).is_err());

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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        'a'.serialize(&mut serializer)?;
        "test".serialize(&mut serializer)?;
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(&mut serializer)?;
        assert!(().serialize(&mut serializer).is_err());
        assert!(PhantomData::<String>.serialize(&mut serializer).is_err());

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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let good_record = GoodTestRecord {
            string_field: String::from("test"),
            int_field: 10,
        };
        good_record.serialize(&mut serializer)?;

        let bad_record = BadTestRecord {
            foo_string_field: String::from("test"),
            bar_int_field: 10,
        };
        assert!(bad_record.serialize(&mut serializer).is_err());

        assert!("".serialize(&mut serializer).is_err());
        assert!(Some("").serialize(&mut serializer).is_err());

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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        #[derive(Serialize)]
        struct EmptyRecord;
        EmptyRecord.serialize(&mut serializer)?;

        #[derive(Serialize)]
        struct NonEmptyRecord {
            foo: String,
        }
        let record = NonEmptyRecord {
            foo: "bar".to_string(),
        };
        match record.serialize(&mut serializer) {
            Err(Error::FieldName(field_name)) if field_name == "foo" => (),
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        match ().serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none"); // serialize_unit() delegates to serialize_none()
                assert_eq!(value, "None");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        Suit::Spades.serialize(&mut serializer)?;
        Suit::Hearts.serialize(&mut serializer)?;
        Suit::Diamonds.serialize(&mut serializer)?;
        Suit::Clubs.serialize(&mut serializer)?;
        match None::<()>.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let arr: Vec<i64> = vec![10, 5, 400];
        arr.serialize(&mut serializer)?;

        match vec![1_f32].serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "1");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let mut map: BTreeMap<String, i64> = BTreeMap::new();
        map.insert(String::from("item1"), 10);
        map.insert(String::from("item2"), 400);

        map.serialize(&mut serializer)?;

        let mut map: BTreeMap<String, &str> = BTreeMap::new();
        map.insert(String::from("item1"), "value1");
        match map.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "string");
                assert_eq!(value, "value1");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        Some(10i64).serialize(&mut serializer)?;
        None::<i64>.serialize(&mut serializer)?;
        NullableLong::Long(400).serialize(&mut serializer)?;
        NullableLong::Null.serialize(&mut serializer)?;

        match "invalid".serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "string");
                assert_eq!(value, "invalid");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        LongOrString::Null.serialize(&mut serializer)?;
        LongOrString::Long(400).serialize(&mut serializer)?;
        LongOrString::Str(String::from("test")).serialize(&mut serializer)?;

        match 1_f64.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f64");
                assert_eq!(value, "1");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        Bytes::new(&[10, 124, 31, 97, 14, 201, 3, 88]).serialize(&mut serializer)?;

        // non-8 size
        match Bytes::new(&[123]).serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "bytes");
                assert_eq!(value, "7b"); // Bytes represents its values as hexadecimals: '7b' is 123
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // array
        match [1; 8].serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=8)");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        // slice
        match &[1, 2, 3, 4, 5, 6, 7, 8].serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(*value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=8)");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let val = Decimal::from(&[251, 155]);
        val.serialize(&mut serializer)?;

        match ().serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let val = Decimal::from(&[0, 0, 0, 0, 0, 0, 251, 155]);
        val.serialize(&mut serializer)?;

        match ().serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "none");
                assert_eq!(value, "None");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[0, 0, 0, 0, 0, 0, 251, 155]);

        Ok(())
    }

    #[test]
    #[serial(serde_is_human_readable)]
    fn test_serialize_bigdecimal() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "big-decimal"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let val = BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2);
        val.serialize(&mut serializer)?;

        assert_eq!(buffer.as_slice(), &[12, 53, 48, 48, 46, 50, 52]);

        Ok(())
    }

    #[test]
    #[serial(serde_is_human_readable)]
    fn test_serialize_uuid() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "string",
            "logicalType": "uuid"
        }"#,
        )?;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        "8c28da81-238c-4326-bddd-4e3d00cc5099"
            .parse::<Uuid>()?
            .serialize(&mut serializer)?;

        match 1_u8.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "u8");
                assert_eq!(value, "1");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[
                72, b'8', b'c', b'2', b'8', b'd', b'a', b'8', b'1', b'-', b'2', b'3', b'8', b'c',
                b'-', b'4', b'3', b'2', b'6', b'-', b'b', b'd', b'd', b'd', b'-', b'4', b'e', b'3',
                b'd', b'0', b'0', b'c', b'c', b'5', b'0', b'9', b'9'
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        100_u8.serialize(&mut serializer)?;
        1000_u16.serialize(&mut serializer)?;
        10000_u32.serialize(&mut serializer)?;
        1000_i16.serialize(&mut serializer)?;
        10000_i32.serialize(&mut serializer)?;

        match 10000_f32.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        100_u8.serialize(&mut serializer)?;
        1000_u16.serialize(&mut serializer)?;
        10000_u32.serialize(&mut serializer)?;
        1000_i16.serialize(&mut serializer)?;
        10000_i32.serialize(&mut serializer)?;

        match 10000_f32.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000");
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        100_u8.serialize(&mut serializer)?;
        1000_u16.serialize(&mut serializer)?;
        10000_u32.serialize(&mut serializer)?;
        1000_i16.serialize(&mut serializer)?;
        10000_i32.serialize(&mut serializer)?;
        10000_i64.serialize(&mut serializer)?;

        match 10000_f32.serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "f32");
                assert_eq!(value, "10000");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(
            buffer.as_slice(),
            &[200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1, 160, 156, 1]
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
            let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

            100_u8.serialize(&mut serializer)?;
            1000_u16.serialize(&mut serializer)?;
            10000_u32.serialize(&mut serializer)?;
            1000_i16.serialize(&mut serializer)?;
            10000_i32.serialize(&mut serializer)?;
            10000_i64.serialize(&mut serializer)?;

            match 10000_f64.serialize(&mut serializer) {
                Err(Error::SerializeValueWithSchema {
                    value_type,
                    value,
                    schema,
                }) => {
                    assert_eq!(value_type, "f64");
                    assert_eq!(value, "10000");
                    assert_eq!(schema, schema);
                }
                unexpected => panic!("Expected an error. Got: {unexpected:?}"),
            }

            assert_eq!(
                buffer.as_slice(),
                &[200, 1, 208, 15, 160, 156, 1, 208, 15, 160, 156, 1, 160, 156, 1]
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
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let duration_bytes =
            ByteArray::new(Duration::new(Months::new(3), Days::new(2), Millis::new(1200)).into());
        duration_bytes.serialize(&mut serializer)?;

        match [1; 12].serialize(&mut serializer) {
            Err(Error::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }) => {
                assert_eq!(value_type, "tuple"); // TODO: why is this 'tuple' ?!
                assert_eq!(value, "tuple (len=12)");
                assert_eq!(schema, schema);
            }
            unexpected => panic!("Expected an error. Got: {unexpected:?}"),
        }

        assert_eq!(buffer.as_slice(), &[3, 0, 0, 0, 2, 0, 0, 0, 176, 4, 0, 0]);

        Ok(())
    }

    #[test]
    #[serial(serde_is_human_readable)] // for BigDecimal and Uuid
    fn test_serialize_recursive_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"},
                {"name": "bigDecimalField", "type": {"type": "bytes", "logicalType": "big-decimal"}},
                {"name": "uuidField", "type": "fixed", "size": 16, "logicalType": "uuid"},
                {"name": "innerRecord", "type": "TestRecord"}
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

        let mut buffer: Vec<u8> = Vec::new();
        let rs = ResolvedSchema::try_from(&schema)?;
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, rs.get_names(), None);

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
        good_record.serialize(&mut serializer)?;

        assert_eq!(
            buffer.as_slice(),
            &[
                8, 116, 101, 115, 116, 20, 12, 53, 48, 48, 46, 50, 52, 72, 56, 99, 50, 56, 100, 97,
                56, 49, 45, 50, 51, 56, 99, 45, 52, 51, 50, 54, 45, 98, 100, 100, 100, 45, 52, 101,
                51, 100, 48, 48, 99, 99, 53, 48, 57, 56, 20, 105, 110, 110, 101, 114, 95, 116, 101,
                115, 116, 200, 1, 12, 50, 48, 48, 46, 51, 56, 72, 56, 99, 50, 56, 100, 97, 56, 49,
                45, 50, 51, 56, 99, 45, 52, 51, 50, 54, 45, 98, 100, 100, 100, 45, 52, 101, 51,
                100, 48, 48, 99, 99, 53, 48, 57, 57
            ]
        );

        Ok(())
    }
}
