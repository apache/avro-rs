use std::io::Write;

use super::{Config, SchemaAwareSerializer};
use crate::error::Details;
use crate::schema::UnionSchema;
use crate::serde::ser_schema::union::UnionAwareSerializer;
use crate::{Error, Schema, schema::RecordSchema};
use serde::Serialize;
use serde::ser::{SerializeTuple, SerializeTupleStruct, SerializeTupleVariant};

pub enum TupleSerializer<'s, 'w, W: Write> {
    Unit(UnitTupleSerializer),
    One(OneTupleSerializer<'s, 'w, W>),
    OneUnion(OneUnionTupleSerializer<'s, 'w, W>),
    Many(ManyTupleSerializer<'s, 'w, W>),
}

impl<'s, 'w, W: Write> SerializeTuple for TupleSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            TupleSerializer::Unit(tuple) => tuple.serialize_element(value),
            TupleSerializer::One(tuple) => tuple.serialize_element(value),
            TupleSerializer::OneUnion(tuple) => tuple.serialize_element(value),
            TupleSerializer::Many(tuple) => tuple.serialize_element(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            TupleSerializer::Unit(tuple) => tuple.end(),
            TupleSerializer::One(tuple) => tuple.end(),
            TupleSerializer::OneUnion(tuple) => tuple.end(),
            TupleSerializer::Many(tuple) => SerializeTuple::end(tuple),
        }
    }
}

pub struct ManyTupleSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    schema: &'s RecordSchema,
    config: Config<'s>,
    field_position: usize,
    bytes_written: usize,
}

impl<'s, 'w, W: Write> ManyTupleSerializer<'s, 'w, W> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s RecordSchema,
        config: Config<'s>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self {
            writer,
            schema,
            config,
            field_position: 0,
            bytes_written: bytes_written.unwrap_or(0),
        }
    }
}

impl<'s, 'w, W: Write> SerializeTuple for ManyTupleSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        let schema = &self.schema.fields[self.field_position].schema;
        let ser = SchemaAwareSerializer::new(&mut *self.writer, schema, self.config)?;
        self.bytes_written += value.serialize(ser)?;
        self.field_position += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        assert_eq!(self.field_position, self.schema.fields.len());
        Ok(self.bytes_written)
    }
}

impl<'s, 'w, W: Write> SerializeTupleStruct for ManyTupleSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.serialize_element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeTuple::end(self)
    }
}

impl<'s, 'w, W: Write> SerializeTupleVariant for ManyTupleSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.serialize_element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeTuple::end(self)
    }
}

pub struct OneTupleSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    schema: &'s Schema,
    config: Config<'s>,
    written_field: bool,
    bytes_written: usize,
}

impl<'s, 'w, W: Write> OneTupleSerializer<'s, 'w, W> {
    pub fn new(writer: &'w mut W, schema: &'s Schema, config: Config<'s>) -> Self {
        Self {
            writer,
            schema,
            config,
            written_field: false,
            bytes_written: 0,
        }
    }
}

impl<'s, 'w, W: Write> SerializeTuple for OneTupleSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if self.written_field {
            panic!(
                "Invalid serialize implementation, serialize_element was called more than once for tuple with one element"
            )
        }
        match self.schema {
            Schema::Union(union) => {
                self.bytes_written +=
                    value.serialize(UnionAwareSerializer::new(self.writer, union, self.config))?;
            }
            schema => {
                self.bytes_written += value.serialize(SchemaAwareSerializer::new(
                    self.writer,
                    schema,
                    self.config,
                )?)?;
            }
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if !self.written_field {
            panic!(
                "Invalid serialize implementation, serialize_element was never called for tuple with one element"
            )
        }
        Ok(self.bytes_written)
    }
}

pub struct OneUnionTupleSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    schema: &'s UnionSchema,
    config: Config<'s>,
    written_field: bool,
    bytes_written: usize,
}

impl<'s, 'w, W: Write> OneUnionTupleSerializer<'s, 'w, W> {
    pub fn new(writer: &'w mut W, schema: &'s UnionSchema, config: Config<'s>) -> Self {
        Self {
            writer,
            schema,
            config,
            written_field: false,
            bytes_written: 0,
        }
    }
}

impl<'s, 'w, W: Write> SerializeTuple for OneUnionTupleSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if self.written_field {
            panic!(
                "Invalid serialize implementation, serialize_element was called more than once for tuple with one element"
            )
        }
        self.bytes_written += value.serialize(UnionAwareSerializer::new(
            self.writer,
            self.schema,
            self.config,
        ))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if !self.written_field {
            panic!(
                "Invalid serialize implementation, serialize_element was never called for tuple with one element"
            )
        }
        Ok(self.bytes_written)
    }
}

pub struct UnitTupleSerializer {
    bytes_written: usize,
}

impl UnitTupleSerializer {
    pub fn new(bytes_written: Option<usize>) -> Self {
        Self {
            bytes_written: bytes_written.unwrap_or(0),
        }
    }
}

impl SerializeTuple for UnitTupleSerializer {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::new(Details::SerializeValueWithSchema {
            value_type: "tuple",
            value: "Expected no elements for the unit tuple".into(),
            schema: Schema::Null,
        }))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.bytes_written)
    }
}
