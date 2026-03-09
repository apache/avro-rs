use std::{borrow::Borrow, io::Read};

use serde::{
    Deserializer,
    de::{EnumAccess, Unexpected, VariantAccess},
};

use crate::{
    Error, Schema,
    error::Details,
    schema::UnionSchema,
    serde::deser_schema::{
        Config, DESERIALIZE_ANY, SchemaAwareDeserializer, identifier::IdentifierDeserializer,
    },
    util::zag_i32,
};

pub struct UnionEnumAccess<'s, 'r, R: Read, S: Borrow<Schema>> {
    schema: &'s UnionSchema,
    reader: &'r mut R,
    config: Config<'s, S>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> UnionEnumAccess<'s, 'r, R, S> {
    pub fn new(schema: &'s UnionSchema, reader: &'r mut R, config: Config<'s, S>) -> Self {
        Self {
            schema,
            reader,
            config,
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> EnumAccess<'de> for UnionEnumAccess<'s, 'r, R, S> {
    type Error = Error;

    type Variant = UnionVariantAccess<'s, 'r, R, S>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let orig_index = zag_i32(self.reader)?;
        let index =
            usize::try_from(orig_index).map_err(|e| Details::ConvertI32ToUsize(e, orig_index))?;

        let schema = self
            .schema
            .variants()
            .get(index)
            .ok_or(Details::GetUnionVariant {
                index: orig_index as i64,
                num_variants: self.schema.variants().len(),
            })?;

        Ok((
            seed.deserialize(IdentifierDeserializer::index(index as u32))?,
            UnionVariantAccess::new(schema, self.reader, self.config),
        ))
    }
}

pub struct UnionVariantAccess<'s, 'r, R: Read, S: Borrow<Schema>> {
    schema: &'s Schema,
    reader: &'r mut R,
    config: Config<'s, S>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> UnionVariantAccess<'s, 'r, R, S> {
    fn new(schema: &'s Schema, reader: &'r mut R, config: Config<'s, S>) -> Self {
        Self {
            schema,
            reader,
            config,
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> VariantAccess<'de>
    for UnionVariantAccess<'s, 'r, R, S>
{
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        if let Schema::Null = self.schema {
            Ok(())
        } else if let Schema::Record(record) = self.schema
            && record.fields.is_empty()
        {
            Ok(())
        } else {
            let unexp = Unexpected::UnitVariant;
            Err(serde::de::Error::invalid_type(unexp, &"other variant"))
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if let Schema::Record(record) = self.schema
            && record.fields.len() == 1
            && record
                .attributes
                .get("org.apache.avro.rust.union_of_records")
                == Some(&serde_json::Value::Bool(true))
        {
            // Most likely a Union of Records
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                &record.fields[0].schema,
                self.config,
            )?)
        } else {
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                self.schema,
                self.config,
            )?)
        }
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        SchemaAwareDeserializer::new(self.reader, self.schema, self.config)?
            .deserialize_tuple(len, visitor)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        SchemaAwareDeserializer::new(self.reader, self.schema, self.config)?.deserialize_struct(
            DESERIALIZE_ANY,
            fields,
            visitor,
        )
    }
}
