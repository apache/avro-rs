use crate::Error;
use crate::error::Details;
use crate::schema::EnumSchema;
use crate::serde::deser_schema::identifier::IdentifierDeserializer;
use crate::util::zag_i32;
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
        let orig_index = zag_i32(self.reader)?;
        let index =
            usize::try_from(orig_index).map_err(|e| Details::ConvertI32ToUsize(e, orig_index))?;
        let symbol = self
            .schema
            .symbols
            .get(index)
            .ok_or(Details::EnumSymbolIndex {
                index: index as usize,
                num_variants: self.schema.symbols.len(),
            })?;
        Ok((
            seed.deserialize(IdentifierDeserializer::string(symbol))?,
            self,
        ))
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
