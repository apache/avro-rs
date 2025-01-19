use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::io::Write;

use serde::ser;

use crate::encode::encode_int;
use crate::encode::encode_long;
use crate::error::Error;
use crate::schema::{EnumSchema, Name, Namespace, RecordField, RecordSchema, Schema};
use crate::types::Value;

#[derive(Clone)]
enum SerItem<'s> {
    BeginItem(Cow<'s, Schema>),
    RecordField(Cow<'s, RecordField>),
    EndRecord,
    Array(Cow<'s, Schema>),
    Map(Cow<'s, Schema>)
}

const COLLECTION_SERIALIZER_INIT_BUFFER_SIZE: usize = 1024;
const COLLECTION_SERIALIZER_BUFFER_SOFT_LIMIT: usize = 768;
const SINGLE_VALUE_INIT_BUFFER_SIZE: usize = 128;

pub struct DirectSerializeSeq<'a, 's, W: Write, S: Borrow<Schema>> {
    ser: &'a mut DirectSerializer<'s, W, S>,
    item_schema: &'s Schema,
    item_count: i64,
    data_buffer: Vec<u8>,
}

impl <'a, 's, W: Write, S: Borrow<Schema>> DirectSerializeSeq<'a, 's, W, S> {
    fn new(ser: &'a mut DirectSerializer<'s, W, S>, item_schema: &'s Schema) -> DirectSerializeSeq<'a, 's, W, S> {
        DirectSerializeSeq {
            ser,
            item_schema,
            item_count: 0,
            data_buffer: Vec::with_capacity(COLLECTION_SERIALIZER_INIT_BUFFER_SIZE)
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if self.item_count > 0 {
            encode_long(-self.item_count, &mut self.ser.writer)?;
            encode_long(self.data_buffer.len() as i64, &mut self.ser.writer)?;
            self.ser.writer.write(self.data_buffer.as_slice()).map_err(|e| Error::WriteBytes(e))?;

            self.data_buffer.clear();
            self.item_count = 0;
        }

        Ok(())
    }
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeSeq for DirectSerializeSeq<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        let mut item_ser = DirectSerializer::new(&mut self.data_buffer, &self.item_schema, &self.ser.names, self.ser.enclosing_namespace.clone());
        value.serialize(&mut item_ser)?;

        self.item_count += 1;
        
        if self.data_buffer.len() > COLLECTION_SERIALIZER_BUFFER_SOFT_LIMIT {
            self.write_buffered_items()?;
        }
        
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.write_buffered_items()?;
        self.ser.writer.write(&[0u8]).map_err(|e| Error::WriteBytes(e))?;

        Ok(())
    }
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeTuple for DirectSerializeSeq<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize {
            ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeTupleStruct for DirectSerializeSeq<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeTupleVariant for DirectSerializeSeq<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

pub struct DirectSerializeMap<'a, 's, W: Write, S: Borrow<Schema>> {
    ser: &'a mut DirectSerializer<'s, W, S>,
    item_schema: &'s Schema,
    item_count: i64,
    data_buffer: Vec<u8>,
}

impl <'a, 's, W: Write, S: Borrow<Schema>> DirectSerializeMap<'a, 's, W, S> {
    fn new(ser: &'a mut DirectSerializer<'s, W, S>, item_schema: &'s Schema) -> DirectSerializeMap<'a, 's, W, S> {
        DirectSerializeMap {
            ser,
            item_schema,
            item_count: 0,
            data_buffer: Vec::with_capacity(COLLECTION_SERIALIZER_INIT_BUFFER_SIZE)
        }
    }

    fn write_buffered_items(&mut self) -> Result<(), Error> {
        if self.item_count > 0 {
            encode_long(-self.item_count, &mut self.ser.writer)?;
            encode_long(self.data_buffer.len() as i64, &mut self.ser.writer)?;
            self.ser.writer.write(self.data_buffer.as_slice()).map_err(|e| Error::WriteBytes(e))?;
        }

        Ok(())
    }
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeMap for DirectSerializeMap<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        let string_schema = Schema::String;
        let mut key_ser = DirectSerializer::new(&mut self.data_buffer, &string_schema, &self.ser.names, self.ser.enclosing_namespace.clone());
        key.serialize(&mut key_ser)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        let mut val_ser = DirectSerializer::new(&mut self.data_buffer, self.item_schema, &self.ser.names, self.ser.enclosing_namespace.clone());
        value.serialize(&mut val_ser)?;

        self.item_count += 1;

        if self.data_buffer.len() > COLLECTION_SERIALIZER_BUFFER_SOFT_LIMIT {
            self.write_buffered_items()?;
        }

        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.write_buffered_items()?;
        self.ser.writer.write(&[0u8]).map_err(|e| Error::WriteBytes(e))?;

        Ok(())
    }
}

pub struct DirectSerializeStruct<'a, 's, W: Write, S: Borrow<Schema>> {
    ser: &'a mut DirectSerializer<'s, W, S>,
    record_schema: &'s RecordSchema,
    item_count: usize,
    buffered_fields: HashMap<usize, Vec<u8>>
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeStruct for DirectSerializeStruct<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        if self.item_count >= self.record_schema.fields.len() {
            return Err(Error::FieldName(String::from(key)));
        }

        let next_field = &self.record_schema.fields[self.item_count];
        if key == next_field.name.as_str() {
            // If we receive fields in order, write them directly to the main writer
            let mut value_ser = DirectSerializer::new(&mut self.ser.writer, &next_field.schema, &self.ser.names, self.ser.enclosing_namespace.clone());
            value.serialize(&mut value_ser)?;

            self.item_count += 1;
        } else {
            for (i, field) in self.record_schema.fields.iter().enumerate() {
                if field.name.as_str() == key {
                    if i < self.item_count {
                        return Err(Error::FieldNameDuplicate(String::from(key)));
                    }

                    let mut buffer: Vec<u8> = Vec::with_capacity(SINGLE_VALUE_INIT_BUFFER_SIZE);
                    let mut value_ser = DirectSerializer::new(&mut buffer, &field.schema, &self.ser.names, self.ser.enclosing_namespace.clone());
                    value.serialize(&mut value_ser)?;

                    self.buffered_fields.insert(i, buffer);

                    return Ok(());
                }
            }

            return Err(Error::FieldName(String::from(key)));
        }

        // Write any buffered data to the stream that has now become next in line
        while let Some(buffer) = self.buffered_fields.remove(&self.item_count) {
            self.ser.writer.write(buffer.as_slice()).map_err(|e| Error::WriteBytes(e))?;
            self.item_count += 1;
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if self.item_count != self.record_schema.fields.len() {
            Err(Error::GetField(self.record_schema.fields[self.item_count].name.clone()))
        } else {
            Ok(())
        }
    }
}

impl  <'a, 's, W: Write, S: Borrow<Schema>> ser::SerializeStructVariant for DirectSerializeStruct<'a, 's, W, S> {
    type Ok = ();
    type Error = Error;
    
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        ser::SerializeStruct::serialize_field(self, key, value)
    }
    
    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeStruct::end(self)
    }
}

pub struct DirectSerializer<'s, W: Write, S: Borrow<Schema>> {
    writer: W,
    schema: &'s Schema,
    names: &'s HashMap<String, S>,
    enclosing_namespace: Namespace,
    item_stack: Vec<SerItem<'s>>,
}

impl <'s, W: Write, S: Borrow<Schema>> DirectSerializer<'s, W, S> {
    pub fn new(writer: W, schema: &'s Schema, names: &'s HashMap<String, S>, enclosing_namespace: Namespace) -> DirectSerializer<'s, W, S> {
        DirectSerializer {
            writer,
            schema,
            names,
            enclosing_namespace,
            item_stack: vec![SerItem::BeginItem(Cow::Borrowed(schema))],
        }
    }

    fn write_int<T>(&mut self, v: T, int_type: &'static str) -> Result<(), Error>
        where
        T: TryInto<i32> + TryInto<i64>,
        <T as TryInto<i32>>::Error: fmt::Display,
        <T as TryInto<i64>>::Error: fmt::Display
    {
        match self.schema {
            Schema::Int | Schema::TimeMillis | Schema::Date => {
                let int_value: i32 = v.try_into().map_err(|e: <T as TryInto<i32>>::Error| Error::SerializeValue(e.to_string()))?;
                encode_int(int_value, &mut self.writer)
            },
            Schema::Long 
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                let long_value: i64 = v.try_into().map_err(|e: <T as TryInto<i64>>::Error| Error::SerializeValue(e.to_string()))?;
                encode_long(long_value, &mut self.writer)
            },
            _ => {
                let long_value: i64 = v.try_into().map_err(|e: <T as TryInto<i64>>::Error| Error::SerializeValue(e.to_string()))?;

                Err(Error::ValidationWithReason {
                    value: Value::Long(long_value),
                    schema: self.schema.clone(),
                    reason: format!("{} cannot be converted to the expected schema type", int_type)
                })
            }
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        encode_long(bytes.len() as i64, &mut self.writer)?;
        self.writer.write(bytes).map_err(|e| Error::WriteBytes(e))?;

        Ok(())
    }

    fn write_union_variant<T:  ser::Serialize>(&mut self, variant_index: i32, schema: &'s Schema, value: &T) -> Result<(), Error> {
        encode_int(variant_index, &mut self.writer)?;

        let mut variant_ser = DirectSerializer::new(&mut self.writer, schema, self.names, self.enclosing_namespace.clone());
        value.serialize(&mut variant_ser)?;

        Ok(())
    }
}

impl <'a, 's, W: Write, S: Borrow<Schema>> ser::Serializer for &'a mut DirectSerializer<'s, W, S> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = DirectSerializeSeq<'a, 's, W, S>;
    type SerializeTuple = DirectSerializeSeq<'a, 's, W, S>;
    type SerializeTupleStruct = DirectSerializeSeq<'a, 's, W, S>;
    type SerializeTupleVariant = DirectSerializeSeq<'a, 's, W, S>;
    type SerializeMap = DirectSerializeMap<'a, 's, W, S>;
    type SerializeStruct = DirectSerializeStruct<'a, 's, W, S>;
    type SerializeStructVariant = DirectSerializeStruct<'a, 's, W, S>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.writer.write(&[u8::from(v)]).map_err(|e| Error::WriteBytes(e))?;
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "i8")
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "i16")
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "i32")
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "i64")
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "u8")
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "u16")
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "u32")
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.write_int(v, "u64")
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Float => self.writer.write(&v.to_le_bytes()).map_err(|e| Error::WriteBytes(e))?,
            Schema::Double => self.writer.write(&(v as f64).to_le_bytes()).map_err(|e| Error::WriteBytes(e))?,
            _ => {
                return Err(Error::ValidationWithReason {
                    value: Value::Float(v),
                    schema: self.schema.clone(),
                    reason: String::from("f32 cannot be converted to the expected schema type")
                });
            }
        };

        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Float => self.writer.write(&(v as f32).to_le_bytes()).map_err(|e| Error::WriteBytes(e))?,
            Schema::Double => self.writer.write(&v.to_le_bytes()).map_err(|e| Error::WriteBytes(e))?,
            _ => {
                return Err(Error::ValidationWithReason {
                    value: Value::Double(v),
                    schema: self.schema.clone(),
                    reason: String::from("f32 cannot be converted to the expected schema type")
                });
            }
        };

        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::String | Schema::Bytes => self.write_bytes(String::from(v).as_bytes()),
            _ => Err(Error::ValidationWithReason {
                value: Value::String(String::from(v)),
                schema: self.schema.clone(),
                reason: String::from("char cannot be converted to the expected schema type")
            })
        }
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::String | Schema::Bytes => self.write_bytes(v.as_bytes()),
            _ => Err(Error::ValidationWithReason {
                value: Value::String(String::from(v)),
                schema: self.schema.clone(),
                reason: String::from("&str cannot be converted to the expected schema type")
            })
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::String | Schema::Bytes => self.write_bytes(v),
            _ => Err(Error::ValidationWithReason {
                value: Value::Bytes(Vec::from(v)),
                schema: self.schema.clone(),
                reason: String::from("&[u8] cannot be converted to the expected schema type")
            })
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Null => Ok(()),
            Schema::Union(sch) => {
                if sch.schemas.len() == 2 {
                    for i in 0..2 {
                        if let Schema::Null = sch.schemas[i] {
                            return self.write_union_variant::<Option<()>>(i as i32, &sch.schemas[i], &None);
                        }
                    }
                }

                Err(Error::ValidationWithReason {
                    value: Value::Null,
                    schema: self.schema.clone(),
                    reason: String::from("None value is only compatible with union schema when 'null' is one of two variants")
                })
            },
            _ => Err(Error::ValidationWithReason {
                value: Value::Null,
                schema: self.schema.clone(),
                reason: String::from("None cannot be converted to the expected schema type")
            })
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize
    {
        match self.schema {
            Schema::Union(sch) => {
                if sch.schemas.len() == 2 {
                    for i in 0..2 {
                        match &sch.schemas[i] {
                            Schema::Null => { continue; },
                            _ => {
                                if let Schema::Null = &sch.schemas[i + 1 % 2] {
                                    return self.write_union_variant(i as i32, &sch.schemas[i], &value);
                                }
                            }
                        }
                    }
                }

                Err(Error::ValidationWithReason {
                    value: Value::Null,
                    schema: self.schema.clone(),
                    reason: String::from("Some(...) value is only compatible with union schema when 'null' is one of two variants")
                })
            },
            _ => Err(Error::ValidationWithReason {
                value: Value::Null,
                schema: self.schema.clone(),
                reason: String::from("Some(...) cannot be converted to the expected schema type")
            })
        }
        
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            Schema::Record(sch) => match sch.fields.len() {
                0 => Ok(()),
                _ => Err(Error::ValidationWithReason {
                    value: Value::Record(Vec::new()),
                    schema: self.schema.clone(),
                    reason: String::from("Unit struct is only compatible with a record schema with no fields")
                })
            },
            _ => Err(Error::ValidationWithReason {
                value: Value::Record(Vec::new()),
                schema: self.schema.clone(),
                reason: String::from("Unit struct cannot be converted to the expected schema type")
            })
        }
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        // Stand-in struct for serializing union variants that are unit structs.  It doesn't matter what type the
        // struct is, since record names don't get serialized in the data
        #[derive(serde::Serialize)]
        struct UnionVariantUnitStruct;

        match self.schema {
            Schema::Enum(sch) => {
                if variant_index as usize >= sch.symbols.len() {
                    return Err(Error::ValidationWithReason {
                        value: Value::Enum(variant_index, String::from(variant)),
                        schema: self.schema.clone(),
                        reason: String::from("Enum variant index is out of bounds")
                    });
                }

                let variant_index_int = i32::try_from(variant_index)
                    .map_err(|_| Error::SerializeValue(String::from("Enum variant index does not fit into an integer value")))?;

                encode_int(variant_index_int, &mut self.writer)
            },
            Schema::Union(sch) => {
                if variant_index as usize >= sch.schemas.len() {
                    return Err(Error::ValidationWithReason {
                        value: Value::Union(variant_index, Box::new(Value::Record(Vec::new()))),
                        schema: self.schema.clone(),
                        reason: String::from("Union variant index is out of bounds")
                    });
                }

                let variant_index_int = i32::try_from(variant_index)
                    .map_err(|_| Error::SerializeValue(String::from("Enum variant index does not fit into an integer value")))?;

                self.write_union_variant(variant_index_int, &sch.schemas[variant_index as usize], &UnionVariantUnitStruct)
            },
            _ => Err(Error::ValidationWithReason {
                value: Value::Enum(variant_index, String::from(variant)),
                schema: self.schema.clone(),
                reason: String::from("")
            })
        }
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize {
        todo!()
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize {
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}
