use std::borrow::Cow;
use std::io::Write;

use serde::ser;

use crate::encode::encode_int;
use crate::encode::encode_long;
use crate::error::Error;
use crate::schema::Name;
use crate::schema::NamesRef;
use crate::schema::{Namespace, RecordSchema, Schema};

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
            item_buffers: Vec::with_capacity(
                len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY),
            ),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if self.item_buffers.len() > 0 {
            self.bytes_written +=
                encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
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
            item_buffers: Vec::with_capacity(
                len.unwrap_or(COLLECTION_SERIALIZER_DEFAULT_INIT_ITEM_CAPACITY),
            ),
            bytes_written: 0,
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if self.item_buffers.len() > 0 {
            self.bytes_written +=
                encode_long(self.item_buffers.len() as i64, &mut self.ser.writer)?;
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
        let last_index = self.item_buffers.len() - 1;
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
        while let Some(buffer) = self
            .buffered_fields
            .get_mut(self.item_count)
            .and_then(|b| b.take())
        {
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

impl<'a, 's, W: Write> ser::SerializeStruct for DirectSerializeStruct<'a, 's, W> {
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
            self.serialize_next_field(&value)?;
            return Ok(());
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

impl<'a, 's, W: Write> ser::SerializeStructVariant for DirectSerializeStruct<'a, 's, W> {
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

impl<'a, 's, W: Write> ser::SerializeTupleStruct for DirectSerializeTupleStruct<'a, 's, W> {
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

impl<'a, 's, W: Write> ser::SerializeTupleVariant for DirectSerializeTupleStruct<'a, 's, W> {
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

impl<'s, W: Write> DirectSerializer<'s, W> {
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

        let ref_schema = self.names.get(full_name.as_ref()).map(|s| *s);

        ref_schema.ok_or_else(|| Error::SchemaResolutionError(full_name.as_ref().clone()))
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
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Boolean => self
                .writer
                .write(&[u8::from(v)])
                .map_err(|e| Error::WriteBytes(e)),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "bool",
                value: format!("{v}"),
                schema: schema.clone(),
            }),
        }
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
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "i8",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
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
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "i16",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
        }
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

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
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "i32",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
        }
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value = i32::try_from(v).map_err(|_| Error::SerializeValueWithSchema {
                    value_type: "i64",
                    value: format!("{}", v),
                    schema: schema.clone(),
                })?;
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
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "i64",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
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
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            Schema::Bytes => self.write_bytes(&[v]),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "u8",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
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
            | Schema::LocalTimestampNanos => encode_long(v as i64, &mut self.writer),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "u16",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
        }
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value = i32::try_from(v).map_err(|_| Error::SerializeValueWithSchema {
                    value_type: "u32",
                    value: format!("{}", v),
                    schema: schema.clone(),
                })?;
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
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "u32",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
        }
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "u64",
            value: format!("{}", v),
            schema: schema.clone(),
        };

        match schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value = i32::try_from(v).map_err(|_| create_error())?;
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
                let long_value = i64::try_from(v).map_err(|_| create_error())?;
                encode_long(long_value, &mut self.writer)
            }
            _ => Err(create_error()),
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
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "f32",
                value: format!("{}", v),
                schema: schema.clone(),
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
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "f64",
                value: format!("{}", v),
                schema: schema.clone(),
            }),
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::String | Schema::Bytes => self.write_bytes(String::from(v).as_bytes()),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "char",
                value: String::from(v),
                schema: schema.clone(),
            }),
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
            Schema::String | Schema::Bytes | Schema::Uuid => self.write_bytes(v.as_bytes()),
            Schema::Fixed(sch) => {
                if v.as_bytes().len() == sch.size {
                    self.writer
                        .write(v.as_bytes())
                        .map_err(|e| Error::WriteBytes(e))
                } else {
                    Err(create_error())
                }
            }
            Schema::Ref { name } => {
                let ref_schema = self.get_ref_schema(name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_str(v)
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
                if let Err(_) = write!(&mut v_str, "{:x}", b) {
                    v_str.push_str("??");
                }
            }
            Error::SerializeValueWithSchema {
                value_type: "bytes",
                value: format!("{}", v_str),
                schema: schema.clone(),
            }
        };

        match schema {
            Schema::String | Schema::Bytes | Schema::Uuid | Schema::BigDecimal => self.write_bytes(v),
            Schema::Fixed(sch) => {
                if v.len() == sch.size {
                    self.writer.write(v).map_err(|e| Error::WriteBytes(e))
                } else {
                    Err(create_error())
                }
            }
            Schema::Decimal(_) => self.write_bytes(v),
            Schema::Ref { name } => {
                let ref_schema = self.get_ref_schema(name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_bytes(v)
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
            Schema::Union(sch) => {
                if sch.schemas.len() == 2 {
                    for i in 0..2 {
                        if let Schema::Null = sch.schemas[i] {
                            return encode_int(i as i32, &mut self.writer);
                        }
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
            Schema::Union(sch) => {
                if sch.schemas.len() == 2 {
                    let mut some_index_opt: Option<usize> = None;
                    if let Schema::Null = sch.schemas[0] {
                        some_index_opt = Some(1);
                    } else if let Schema::Null = sch.schemas[1] {
                        some_index_opt = Some(0);
                    }

                    match some_index_opt {
                        Some(some_index) => {
                            encode_int(some_index as i32, &mut self.writer)?;
                            self.schema_stack.push(&sch.schemas[some_index]);
                            value.serialize(self)
                        }
                        None => Err(create_error()),
                    }
                } else {
                    Err(create_error())
                }
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "unit struct",
            value: format!("{}", name),
            schema: schema.clone(),
        };

        match schema {
            Schema::Record(sch) => match sch.fields.len() {
                0 => Ok(0),
                _ => Err(create_error()),
            },
            Schema::Null => Ok(0),
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_unit_struct(name)
            }
            _ => Err(create_error()),
        }
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        let create_error = || Error::SerializeValueWithSchema {
            value_type: "unit variant",
            value: format!("{name}::{variant} (index={variant_index})"),
            schema: schema.clone(),
        };

        match schema {
            Schema::Enum(sch) => {
                if variant_index as usize >= sch.symbols.len() {
                    return Err(create_error());
                }

                encode_int(variant_index as i32, &mut self.writer)
            }
            Schema::Union(sch) => {
                if variant_index as usize >= sch.schemas.len() {
                    return Err(create_error());
                }

                encode_int(variant_index as i32, &mut self.writer)?;
                self.schema_stack.push(&sch.schemas[variant_index as usize]);
                self.serialize_unit_struct(name)
            }
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_unit_variant(name, variant_index, variant)
            }
            _ => Err(create_error()),
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

        match schema {
            Schema::Array(sch) => Ok(DirectSerializeSeq::new(self, sch.items.as_ref(), len)),
            _ => {
                let len_str = len
                    .map(|l| format!("{}", l))
                    .unwrap_or_else(|| String::from("?"));
                Err(Error::SerializeValueWithSchema {
                    value_type: "sequence",
                    value: format!("sequence (len={len_str})"),
                    schema: schema.clone(),
                })
            }
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let schema = self.schema_stack.pop().unwrap_or(self.root_schema);

        match schema {
            Schema::Array(sch) => Ok(DirectSerializeSeq::new(self, sch.items.as_ref(), Some(len))),
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "tuple",
                value: format!("tuple (len={len})"),
                schema: schema.clone(),
            }),
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
                self,
                &sch.items,
                Some(len),
            ))),
            Schema::Record(sch) => Ok(DirectSerializeTupleStruct::Record(
                DirectSerializeStruct::new(self, &sch, len),
            )),
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_tuple_struct(name, len)
            }
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "tuple struct",
                value: format!("{name}({})", vec!["?"; len].as_slice().join(",")),
                schema: schema.clone(),
            }),
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
            Schema::Union(sch) => {
                let variant_schema = sch
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

        match schema {
            Schema::Map(sch) => Ok(DirectSerializeMap::new(self, sch.types.as_ref(), len)),
            _ => {
                let len_str = len
                    .map(|l| format!("{}", l))
                    .unwrap_or_else(|| String::from("?"));
                Err(Error::SerializeValueWithSchema {
                    value_type: "map",
                    value: format!("map (size={len_str})"),
                    schema: schema.clone(),
                })
            }
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
            Schema::Ref { name: ref_name } => {
                let ref_schema = self.get_ref_schema(ref_name)?;
                self.schema_stack.push(ref_schema);
                self.serialize_struct(name, len)
            }
            _ => Err(Error::SerializeValueWithSchema {
                value_type: "struct",
                value: format!("{name}{{ ... }}"),
                schema: schema.clone(),
            }),
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
            Schema::Union(sch) => {
                let variant_schema = sch
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};
    use serde::Serialize;
    use serde_bytes::{Bytes, ByteBuf};
    use num_bigint::{BigInt, Sign};
    use bigdecimal::BigDecimal;

    use crate::decimal::Decimal;
    use crate::bigdecimal::big_decimal_as_bytes;

    #[test]
    fn test_serialize_null() {
        let schema = Schema::Null;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        ().serialize(&mut serializer).unwrap();
        (None as Option<()>).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), Vec::<u8>::new().as_slice());
    }

    #[test]
    fn test_serialize_bool() {
        let schema = Schema::Boolean;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        true.serialize(&mut serializer).unwrap();
        false.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[1, 0]);
    }

    #[test]
    fn test_serialize_int() {
        let schema = Schema::Int;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4u8.serialize(&mut serializer).unwrap();
        31u16.serialize(&mut serializer).unwrap();
        13u32.serialize(&mut serializer).unwrap();
        7i8.serialize(&mut serializer).unwrap();
        (-57i16).serialize(&mut serializer).unwrap();
        129i32.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[8, 62, 26, 14, 113, 130, 2]);
    }


    #[test]
    fn test_serialize_long() {
        let schema = Schema::Long;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4u8.serialize(&mut serializer).unwrap();
        31u16.serialize(&mut serializer).unwrap();
        13u32.serialize(&mut serializer).unwrap();
        291u64.serialize(&mut serializer).unwrap();
        7i8.serialize(&mut serializer).unwrap();
        (-57i16).serialize(&mut serializer).unwrap();
        129i32.serialize(&mut serializer).unwrap();
        (-432i64).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[8, 62, 26, 198, 4, 14, 113, 130, 2, 223, 6]);
    }

    #[test]
    fn test_serialize_float() {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4.7f32.serialize(&mut serializer).unwrap();
        (-14.1f64).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);
    }

    #[test]
    fn test_serialize_double() {
        let schema = Schema::Float;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        4.7f32.serialize(&mut serializer).unwrap();
        (-14.1f64).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[102, 102, 150, 64, 154, 153, 97, 193]);
    }

    #[test]
    fn test_serialize_bytes() {
        let schema = Schema::Bytes;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        'a'.serialize(&mut serializer).unwrap();
        "test".serialize(&mut serializer).unwrap();
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[2, b'a', 8, b't', b'e', b's', b't', 10, 12, 3, 7, 91, 4]);
    }

    #[test]
    fn test_serialize_string() {
        let schema = Schema::String;
        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        'a'.serialize(&mut serializer).unwrap();
        "test".serialize(&mut serializer).unwrap();
        Bytes::new(&[12, 3, 7, 91, 4]).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[2, b'a', 8, b't', b'e', b's', b't', 10, 12, 3, 7, 91, 4]);
    }

    #[test]
    fn test_serialize_record() {
        let schema = Schema::parse_str(r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"}
            ]
        }"#).unwrap();

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            pub string_field: String,
            pub int_field: i32
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let record = TestRecord { string_field: String::from("test"), int_field: 10 };
        record.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[8, b't', b'e', b's', b't', 20]);
    }

    #[test]
    fn test_serialize_empty_record() {
        let schema = Schema::parse_str(r#"{
            "type": "record",
            "name": "EmptyRecord",
            "fields": []
        }"#).unwrap();

        #[derive(Serialize)]
        struct EmptyRecord;

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        EmptyRecord.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_serialize_enum() {
        let schema = Schema::parse_str(r#"{
            "type": "enum",
            "name": "Suit",
            "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        }"#).unwrap();

        #[derive(Serialize)]
        enum Suit {
            Spades,
            Hearts,
            Diamonds,
            Clubs
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        Suit::Spades.serialize(&mut serializer).unwrap();
        Suit::Hearts.serialize(&mut serializer).unwrap();
        Suit::Diamonds.serialize(&mut serializer).unwrap();
        Suit::Clubs.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[0, 2, 4, 6]);
    }

    #[test]
    fn test_serialize_array() {
        let schema = Schema::parse_str(r#"{
            "type": "array",
            "items": "long"
        }"#).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let arr: Vec<i64> = vec![10, 5, 400];
        arr.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[6, 20, 10, 160, 6, 0]);
    }

    #[test]
    fn test_serialize_map() {
        let schema = Schema::parse_str(r#"{
            "type": "map",
            "values": "long"
        }"#).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let mut map: BTreeMap<String, i64> = BTreeMap::new();
        map.insert(String::from("item1"), 10);
        map.insert(String::from("item2"), 400);

        map.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[4, 10, b'i', b't', b'e', b'm', b'1', 20, 10, b'i', b't', b'e', b'm', b'2', 160, 6, 0]);
    }

    #[test]
    fn test_serialize_nullable() {
        let schema = Schema::parse_str(r#"{
            "type": ["null", "long"]
        }"#).unwrap();

        #[derive(Serialize)]
        enum NullableLong {
            Null,
            Long(i64)
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        Some(10i64).serialize(&mut serializer).unwrap();
        (None as Option<i64>).serialize(&mut serializer).unwrap();
        NullableLong::Long(400).serialize(&mut serializer).unwrap();
        NullableLong::Null.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[2, 20, 0, 2, 160, 6, 0]);
    }

    #[test]
    fn test_serialize_union() {
        let schema = Schema::parse_str(r#"{
            "type": ["null", "long", "string"]
        }"#).unwrap();

        #[derive(Serialize)]
        enum LongOrString {
            Null,
            Long(i64),
            Str(String)
        }

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        LongOrString::Null.serialize(&mut serializer).unwrap();
        LongOrString::Long(400).serialize(&mut serializer).unwrap();
        LongOrString::Str(String::from("test")).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[0, 2, 160, 6, 4, 8, b't', b'e', b's', b't']);
    }

    #[test]
    fn test_serialize_fixed() {
        let schema = Schema::parse_str(r#"{
            "type": "fixed",
            "size": 8,
            "name": "LongVal"
        }"#).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        Bytes::new(&[10, 124, 31, 97, 14, 201, 3, 88]).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[10, 124, 31, 97, 14, 201, 3, 88]);
    }

    #[test]
    fn test_serialize_decimal() {
        let schema = Schema::parse_str(r#"{
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let val = Decimal::from(&[251, 155]);
        val.serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[4, 251, 155]);
    }

    #[test]
    fn test_serialize_bigdecimal() {
        let schema = Schema::parse_str(r#"{
            "type": "bytes",
            "logicalType": "big-decimal"
        }"#).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        let val = BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2);
        ByteBuf::from(big_decimal_as_bytes(&val)).serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[10, 6, 0, 195, 104, 4]);
    }

    #[test]
    fn test_serialize_uuid() {
        let schema = Schema::parse_str(r#"{
            "type": "string",
            "logicalType": "uuid"
        }"#).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let names = HashMap::new();
        let mut serializer = DirectSerializer::new(&mut buffer, &schema, &names, None);

        "8c28da81-238c-4326-bddd-4e3d00cc5099".serialize(&mut serializer).unwrap();

        assert_eq!(buffer.as_slice(), &[72, b'8', b'c', b'2', b'8', b'd', b'a', b'8', b'1', b'-', b'2', b'3', b'8', b'c', b'-',
            b'4', b'3', b'2', b'6', b'-', b'b', b'd', b'd', b'd', b'-', b'4', b'e', b'3', b'd', b'0', b'0', b'c', b'c', b'5', b'0', b'9', b'9']);
    }


}
