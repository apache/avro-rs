use crate::{Error, error::Details};
use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

/// Serialize a `T: Serialize` as a `String`.
///
/// An error will be returned if any other function than [`Serializer::serialize_str`] is called.
pub struct StringSerializer;

impl Serializer for StringSerializer {
    type Ok = String;
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeSeq for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeTuple for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeTupleStruct for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeTupleVariant for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeMap for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeStruct for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}

impl SerializeStructVariant for StringSerializer {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Details::MapFieldExpectedString.into())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Details::MapFieldExpectedString.into())
    }
}
