use std::fmt::Formatter;
use serde::de::{EnumAccess, Error, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer};

pub struct AnyVisitor;

impl<'de> Visitor<'de> for AnyVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "anything")
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_i8<E>(self, _: i8) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_i16<E>(self, _: i16) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_i32<E>(self, _: i32) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_i128<E>(self, _: i128) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_u8<E>(self, _: u8) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_u16<E>(self, _: u16) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_u32<E>(self, _: u32) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_u128<E>(self, _: u128) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_f32<E>(self, _: f32) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_char<E>(self, _: char) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_borrowed_str<E>(self, _: &'de str) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_bytes<E>(self, _: &[u8]) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_borrowed_bytes<E>(self, _: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_byte_buf<E>(self, _: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_any(AnyVisitor)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error
    {
        Ok(())
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_any(AnyVisitor)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>
    {
        while let Some(_) = seq.next_element::<AnyDeserialize>()? {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>
    {
        while let Some(_) = map.next_entry::<AnyDeserialize, AnyDeserialize>()? {}
        Ok(())
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: EnumAccess<'de>
    {
        let (_, variant) = data.variant::<AnyDeserialize>()?;
        variant.unit_variant()?;
        Ok(())
    }
}

pub struct AnyDeserialize;

impl<'de> Deserialize<'de> for AnyDeserialize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_any(AnyVisitor).map(|()| Self)
    }
}

