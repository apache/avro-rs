use std::collections::HashMap;
use std::fmt;
use std::io::Write;

use serde::ser;

use crate::encode::encode_int;
use crate::encode::encode_long;
use crate::error::Error;
use crate::schema::NamesRef;
use crate::schema::{Namespace, RecordSchema, Schema};
use crate::types::Value;

const RECORD_FIELD_INIT_BUFFER_SIZE: usize = 64;
const COLLECTION_SERIALIZER_ITEM_LIMIT: usize = 1024;
const COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY: usize = 32;
const SINGLE_VALUE_INIT_BUFFER_SIZE: usize = 128;

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
            item_buffers: Vec::with_capacity(len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY)),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if self.item_buffers.len() > 0 {
            self.bytes_written += encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
            for item in self.item_buffers.drain(..) {
                self.bytes_written += self
                    .ser
                    .writer
                    .write(item.as_slice())
                    .map_err(|e| Error::WriteBytes(e))?;
            }
        }

        Ok(())
    }

    fn serialize_element<T: ser::Serialize>(&mut self, value: &T) -> Result<(), Error> {
        let mut item_buffer: Vec<u8> = Vec::with_capacity(self.item_buffer_size);
        let mut item_ser = DirectSerializer::new(
            &mut item_buffer,
            &self.item_schema,
            &self.ser.names,
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
        self.bytes_written += self
            .ser
            .writer
            .write(&[0u8])
            .map_err(|e| Error::WriteBytes(e))?;

        Ok(self.bytes_written)
    }
}

impl<'a, 's, W: Write> ser::SerializeSeq for DirectSerializeSeq<'a, 's, W> {
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

impl<'a, 's, W: Write> ser::SerializeTuple for DirectSerializeSeq<'a, 's, W> {
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
            item_buffers: Vec::with_capacity(len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY)),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if self.item_buffers.len() > 0 {
            self.bytes_written += encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
            for item in self.item_buffers.drain(..) {
                self.bytes_written += self
                    .ser
                    .writer
                    .write(item.as_slice())
                    .map_err(|e| Error::WriteBytes(e))?;
            }
        }

        Ok(())
    }
}

impl<'a, 's, W: Write> ser::SerializeMap for DirectSerializeMap<'a, 's, W> {
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
            &self.ser.names,
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
        let last_index = self.item_buffers.len()-1;
        let element_buffer = &mut self.item_buffers[last_index];
        let mut val_ser = DirectSerializer::new(
            element_buffer,
            self.item_schema,
            &self.ser.names,
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
        self.bytes_written += self
            .ser
            .writer
            .write(&[0u8])
            .map_err(|e| Error::WriteBytes(e))?;

        Ok(self.bytes_written)
    }
}

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
            &self.ser.names,
            self.ser.enclosing_namespace.clone(),
        );
        self.bytes_written += value.serialize(&mut value_ser)?;

        self.item_count += 1;

        // Write any buffered data to the stream that has now become next in line
        while let Some(buffer) = self.buffered_fields.get_mut(self.item_count).and_then(|b| b.take()) {
            self.bytes_written += self
                .ser
                .writer
                .write(buffer.as_slice())
                .map_err(|e| Error::WriteBytes(e))?;
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

impl<'a, 's, W: Write> ser::SerializeStruct
    for DirectSerializeStruct<'a, 's, W>
{
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
            Some(aliases) => key == next_field.name.as_str() || aliases.iter().any(|a| key == a.as_str()),
            None => key == next_field.name.as_str()
        };

        if next_field_matches {
            self.serialize_next_field(&value)?;
            return Ok(());
        } else {
            if self.item_count < self.record_schema.fields.len() {
                for i in self.item_count..self.record_schema.fields.len() {
                    let field = &self.record_schema.fields[i];
                    let field_matches = match &field.aliases {
                        Some(aliases) => key == field.name.as_str() || aliases.iter().any(|a| key == a.as_str()),
                        None => key == field.name.as_str()
                    };
    
                    if field_matches {
                        let mut buffer: Vec<u8> = Vec::with_capacity(RECORD_FIELD_INIT_BUFFER_SIZE);
                        let mut value_ser = DirectSerializer::new(
                            &mut buffer,
                            &field.schema,
                            &self.ser.names,
                            self.ser.enclosing_namespace.clone(),
                        );
                        value.serialize(&mut value_ser)?;
    
                        self.buffered_fields[i] = Some(buffer);
    
                        return Ok(());
                    }
                }
            }

            return Err(Error::FieldName(String::from(key)));
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<'a, 's, W: Write> ser::SerializeStructVariant
    for DirectSerializeStruct<'a, 's, W>
{
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

pub enum DirectSerializeTupleStruct<'a, 's, W: Write> {
    Record(DirectSerializeStruct<'a, 's, W>),
    Array(DirectSerializeSeq<'a, 's, W>),
}

impl<'a, 's, W: Write> DirectSerializeTupleStruct<'a, 's, W> {
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

impl<'a, 's, W: Write> ser::SerializeTupleStruct
    for DirectSerializeTupleStruct<'a, 's, W>
{
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

impl<'a, 's, W: Write> ser::SerializeTupleVariant
    for DirectSerializeTupleStruct<'a, 's, W>
{
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

pub struct DirectSerializer<'s, W: Write> {
    writer: &'s mut W,
    root_schema: &'s Schema,
    names: &'s NamesRef<'s>,
    enclosing_namespace: Namespace,
    schema_stack: Vec<&'s Schema>,
}

impl<'s, W: Write> DirectSerializer<'s,  W> {
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

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written: usize = 0;

        bytes_written += encode_long(bytes.len() as i64, &mut self.writer)?;
        bytes_written += self.writer.write(bytes).map_err(|e| Error::WriteBytes(e))?;

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
        self.writer
            .write(&[u8::from(v)])
            .map_err(|e| Error::WriteBytes(e))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

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
            | Schema::LocalTimestampNanos => {
                encode_long(v as i64, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Int(v as i32),
                    schema: schema.clone(),
                    reason: String::from("i8 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

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
            | Schema::LocalTimestampNanos => {
                encode_long(v as i64, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Int(v as i32),
                    schema: schema.clone(),
                    reason: String::from("i16 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                encode_int(v, &mut self.writer)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                encode_long(v as i64, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Int(v),
                    schema: schema.clone(),
                    reason: String::from("i32 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value = i32::try_from(v).map_err(|_| Error::Validation)?;
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
                encode_long(v, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Long(v),
                    schema: schema.clone(),
                    reason: String::from("i16 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

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
            | Schema::LocalTimestampNanos => {
                encode_long(v as i64, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Int(v as i32),
                    schema: schema.clone(),
                    reason: String::from("u8 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

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
            | Schema::LocalTimestampNanos => {
                encode_long(v as i64, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Int(v as i32),
                    schema: schema.clone(),
                    reason: String::from("u16 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value = i32::try_from(v).map_err(|_| Error::Validation)?;
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
                encode_long(v as i64, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Long(v as i64),
                    schema: schema.clone(),
                    reason: String::from("u32 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value = i32::try_from(v).map_err(|_| Error::Validation)?;
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
                let long_value = i64::try_from(v).map_err(|_| Error::Validation)?;
                encode_long(long_value, &mut self.writer)
            }
            _ => {
                Err(Error::ValidationWithReason {
                    value: Value::Int(v as i32),
                    schema: schema.clone(),
                    reason: String::from("i16 cannot be converted to the expected schema type")
                })
            }
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Float => self
                .writer
                .write(&v.to_le_bytes())
                .map_err(|e| Error::WriteBytes(e)),
            Schema::Double => self
                .writer
                .write(&(v as f64).to_le_bytes())
                .map_err(|e| Error::WriteBytes(e)),
            _ => Err(Error::ValidationWithReason {
                value: Value::Float(v),
                schema: schema.clone(),
                reason: String::from("f32 cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Float => self
                .writer
                .write(&(v as f32).to_le_bytes())
                .map_err(|e| Error::WriteBytes(e)),
            Schema::Double => self
                .writer
                .write(&v.to_le_bytes())
                .map_err(|e| Error::WriteBytes(e)),
            _ => Err(Error::ValidationWithReason {
                value: Value::Double(v),
                schema: schema.clone(),
                reason: String::from("f32 cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::String | Schema::Bytes => self.write_bytes(String::from(v).as_bytes()),
            _ => Err(Error::ValidationWithReason {
                value: Value::String(String::from(v)),
                schema: schema.clone(),
                reason: String::from("char cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::String | Schema::Bytes | Schema::Uuid => self.write_bytes(v.as_bytes()),
            Schema::Fixed(sch) => {
                if v.as_bytes().len() == sch.size {
                    self.writer
                        .write(v.as_bytes())
                        .map_err(|e| Error::WriteBytes(e))
                } else {
                    Err(Error::Validation)
                }
            }
            _ => Err(Error::ValidationWithReason {
                value: Value::String(String::from(v)),
                schema: schema.clone(),
                reason: String::from("&str cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::String | Schema::Bytes => self.write_bytes(v),
            _ => Err(Error::ValidationWithReason {
                value: Value::Bytes(Vec::from(v)),
                schema: schema.clone(),
                reason: String::from("&[u8] cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Null => Ok(0),
            Schema::Union(sch) => {
                if sch.schemas.len() == 2 {
                    for i in 0..2 {
                        if let Schema::Null = sch.schemas[i] {
                            return encode_int(i as i32, &mut self.writer);
                        }
                    }
                }

                Err(Error::ValidationWithReason {
                    value: Value::Null,
                    schema: schema.clone(),
                    reason: String::from("None value is only compatible with union schema when 'null' is one of two variants")
                })
            }
            _ => Err(Error::ValidationWithReason {
                value: Value::Null,
                schema: schema.clone(),
                reason: String::from("None cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Union(sch) => {
                if sch.schemas.len() == 2 {
                    let mut some_index = 0;
                    if let Schema::Null = sch.schemas[0] {
                        some_index = 1;
                    }
                    encode_int(some_index as i32, &mut self.writer)?;
                    self.schema_stack.push(&sch.schemas[some_index]);
                    return value.serialize(self);
                }

                Err(Error::ValidationWithReason {
                    value: Value::Null,
                    schema: schema.clone(),
                    reason: String::from("Some(...) value is only compatible with union schema when 'null' is one of two variants")
                })
            }
            _ => Err(Error::ValidationWithReason {
                value: Value::Null,
                schema: schema.clone(),
                reason: String::from("Some(...) cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Record(sch) => match sch.fields.len() {
                0 => Ok(0),
                _ => Err(Error::ValidationWithReason {
                    value: Value::Record(Vec::new()),
                    schema: schema.clone(),
                    reason: String::from(
                        "Unit struct is only compatible with a record schema with no fields",
                    ),
                }),
            },
            _ => Err(Error::ValidationWithReason {
                value: Value::Record(Vec::new()),
                schema: schema.clone(),
                reason: String::from("Unit struct cannot be converted to the expected schema type"),
            }),
        }
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Enum(sch) => {
                if variant_index as usize >= sch.symbols.len() {
                    return Err(Error::ValidationWithReason {
                        value: Value::Enum(variant_index, String::from(variant)),
                        schema: schema.clone(),
                        reason: String::from("Enum variant index is out of bounds"),
                    });
                }

                encode_int(variant_index as i32, &mut self.writer)
            }
            Schema::Union(sch) => {
                if variant_index as usize >= sch.schemas.len() {
                    return Err(Error::ValidationWithReason {
                        value: Value::Union(variant_index, Box::new(Value::Record(Vec::new()))),
                        schema: schema.clone(),
                        reason: String::from("Union variant index is out of bounds"),
                    });
                }

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(&sch.schemas[variant_index as usize]);
                self.serialize_unit_struct(name)
            }
            _ => Err(Error::ValidationWithReason {
                value: Value::Enum(variant_index, String::from(variant)),
                schema: schema.clone(),
                reason: String::from(""),
            }),
        }
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
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

        match schema {
            Schema::Union(sch) => {
                let variant_schema = sch
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(|| Error::FindUnionVariant)?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(variant_schema);
                self.serialize_newtype_struct(variant, value)
            }
            _ => Err(Error::Validation),
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Array(sch) => Ok(DirectSerializeSeq::new(self, sch.items.as_ref(), len)),
            _ => Err(Error::Validation),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Array(sch) => Ok(DirectSerializeSeq::new(self, sch.items.as_ref(), Some(len))),
            _ => Err(Error::Validation),
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Array(sch) => Ok(DirectSerializeTupleStruct::Array(DirectSerializeSeq::new(
                self, &sch.items, Some(len)
            ))),
            Schema::Record(sch) => Ok(DirectSerializeTupleStruct::Record(
                DirectSerializeStruct::new(self, &sch, len),
            )),
            _ => Err(Error::Validation),
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

        match schema {
            Schema::Union(sch) => {
                let variant_schema = sch
                    .schemas
                    .get(variant_index as usize)
                    .ok_or_else(|| Error::FindUnionVariant)?;

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(variant_schema);
                self.serialize_tuple_struct(variant, len)
            }
            _ => Err(Error::Validation),
        }
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Map(sch) => Ok(DirectSerializeMap::new(self, sch.types.as_ref(), len)),
            _ => Err(Error::Validation),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Record(sch) => Ok(DirectSerializeStruct::new(self, sch, len)),
            _ => Err(Error::Validation),
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

        match schema {
            Schema::Union(sch) => {
                let variant_schema = &sch.schemas[variant_index as usize];

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(variant_schema);
                self.serialize_struct(variant, len)
            }
            _ => Err(Error::Validation),
        }
    }
}
