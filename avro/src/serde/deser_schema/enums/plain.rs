use crate::error::Details;
use crate::schema::EnumSchema;
use crate::util::zag_i32;
use crate::{Error, Schema};
use serde::Deserializer;
use serde::de::{DeserializeSeed, EnumAccess, Unexpected, VariantAccess, Visitor};
use std::io::Read;

pub struct PlainEnumAccess<'s, 'r, R: Read> {
    schema: &'s EnumSchema,
    reader: &'r mut R,
}

impl<'s, 'r, R: Read> PlainEnumAccess<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s EnumSchema) -> Self {
        Self { schema, reader }
    }
}

impl<'de, 's, 'r, R: Read> EnumAccess<'de> for PlainEnumAccess<'s, 'r, R> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let deserializer = EnumIdentifierDeserializer {
            reader: self.reader,
            schema: self.schema,
        };
        Ok((seed.deserialize(deserializer)?, self))
    }
}

impl<'de, 's, 'r, R: Read> VariantAccess<'de> for PlainEnumAccess<'s, 'r, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let unexp = Unexpected::UnitVariant;
        Err(serde::de::Error::invalid_type(unexp, &"newtype variant"))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let unexp = Unexpected::UnitVariant;
        Err(serde::de::Error::invalid_type(unexp, &"newtype variant"))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let unexp = Unexpected::UnitVariant;
        Err(serde::de::Error::invalid_type(unexp, &"newtype variant"))
    }
}

struct EnumIdentifierDeserializer<'s, 'r, R: Read> {
    schema: &'s EnumSchema,
    reader: &'r mut R,
}

impl<'s, 'r, R: Read> EnumIdentifierDeserializer<'s, 'r, R> {
    fn error(&self, error: impl Into<String>) -> Error {
        Error::new(Details::DeserializeValueWithSchema {
            value_type: "enum",
            value: error.into(),
            schema: Schema::Enum(self.schema.clone()),
        })
    }
}

impl<'de, 's, 'r, R: Read> Deserializer<'de> for EnumIdentifierDeserializer<'s, 'r, R> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_any, expected deserialize_identifier"))
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_bool, expected deserialize_identifier"))
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_i8, expected deserialize_identifier"))
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_i16, expected deserialize_identifier"))
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_i32, expected deserialize_identifier"))
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_i64, expected deserialize_identifier"))
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_u8, expected deserialize_identifier"))
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_u16, expected deserialize_identifier"))
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_u32, expected deserialize_identifier"))
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_u64, expected deserialize_identifier"))
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_f32, expected deserialize_identifier"))
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_f64, expected deserialize_identifier"))
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_char, expected deserialize_identifier"))
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_str, expected deserialize_identifier"))
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_string, expected deserialize_identifier"))
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_bytes, expected deserialize_identifier"))
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_byte_buf, expected deserialize_identifier"))
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_option, expected deserialize_identifier"))
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_unit, expected deserialize_identifier"))
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_unit_struct, expected deserialize_identifier"))
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_newtype_struct, expected deserialize_identifier"))
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_seq, expected deserialize_identifier"))
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_tuple, expected deserialize_identifier"))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_tuple_struct, expected deserialize_identifier"))
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_map, expected deserialize_identifier"))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_struct, expected deserialize_identifier"))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(self.error("Unexpected deserialize_enum, expected deserialize_identifier"))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let index = zag_i32(self.reader)?;
        let symbol =
            self.schema
                .symbols
                .get(index as usize)
                .ok_or_else(|| Details::EnumSymbolIndex {
                    index: index as usize,
                    num_variants: self.schema.symbols.len(),
                })?;
        visitor.visit_str(&symbol)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}
