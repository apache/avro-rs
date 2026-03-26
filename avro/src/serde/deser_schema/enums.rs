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

use serde::{
    Deserializer,
    de::{DeserializeSeed, EnumAccess, Unexpected, VariantAccess, Visitor},
};
use std::{borrow::{Borrow, Cow}, io::Read};

use super::{Config, DESERIALIZE_ANY, SchemaAwareDeserializer, identifier::IdentifierDeserializer};
use crate::{
    Error, Schema,
    error::Details,
    schema::EnumSchema,
    util::zag_i32,
};

/// Deserializer for plain enums.
pub struct PlainEnumDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    symbols: &'s [String],
}

impl<'s, 'r, R: Read> PlainEnumDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s EnumSchema) -> Self {
        Self {
            symbols: &schema.symbols,
            reader,
        }
    }
}

impl<'de, 's, 'r, R: Read> EnumAccess<'de> for PlainEnumDeserializer<'s, 'r, R> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let index = zag_i32(self.reader)?;
        let index = usize::try_from(index).map_err(|e| Details::ConvertI32ToUsize(e, index))?;
        let symbol = self.symbols.get(index).ok_or(Details::EnumSymbolIndex {
            index,
            num_variants: self.symbols.len(),
        })?;
        Ok((
            seed.deserialize(IdentifierDeserializer::string(symbol))?,
            self,
        ))
    }
}

impl<'de, 's, 'r, R: Read> VariantAccess<'de> for PlainEnumDeserializer<'s, 'r, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let unexp = Unexpected::UnitVariant;
        Err(serde::de::Error::invalid_type(unexp, &"newtype variant"))
    }

    fn tuple_variant<V>(self, _: usize, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let unexp = Unexpected::UnitVariant;
        Err(serde::de::Error::invalid_type(unexp, &"tuple variant"))
    }

    fn struct_variant<V>(self, _: &'static [&'static str], _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let unexp = Unexpected::UnitVariant;
        Err(serde::de::Error::invalid_type(unexp, &"struct variant"))
    }
}

pub struct UnionEnumDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s, S>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> UnionEnumDeserializer<'s, 'r, R, S> {
    pub fn new(
        reader: &'r mut R,
        schema: &'s Schema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        Ok(Self {
            reader,
            schema,
            config,
        })
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> EnumAccess<'de>
    for UnionEnumDeserializer<'s, 'r, R, S>
{
    type Error = Error;
    type Variant = UnionVariantAccess<'s, 'r, R, S>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let name = match self.schema.name() {
            Some(name) => Cow::Borrowed(name.name()),
            None => Cow::Owned(self.schema.to_string()),
        };
        Ok((
            seed.deserialize(IdentifierDeserializer::string(&name))?,
            UnionVariantAccess::new(self.schema, self.reader, self.config)?,
        ))
    }
}
pub struct UnionVariantAccess<'s, 'r, R: Read, S: Borrow<Schema>> {
    schema: &'s Schema,
    reader: &'r mut R,
    config: Config<'s, S>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> UnionVariantAccess<'s, 'r, R, S> {
    pub fn new(
        schema: &'s Schema,
        reader: &'r mut R,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema {
            config.get_schema(name)?
        } else {
            schema
        };
        Ok(Self {
            schema,
            reader,
            config,
        })
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::DeserializeSchemaAware {
            value_type: ty,
            value: error.into(),
            schema: self.schema.clone(),
        })
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> VariantAccess<'de>
    for UnionVariantAccess<'s, 'r, R, S>
{
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.schema {
            Schema::Null => Ok(()),
            Schema::Record(record) if record.fields.is_empty() => Ok(()),
            _ => Err(self.error(
                "unit variant",
                "Expected Schema::Null | Schema::Record(fields.len() == 0)",
            )),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.schema {
            Schema::Record(record)
                if record.fields.len() == 1
                    && record
                        .attributes
                        .get("org.apache.avro.rust.union_of_records")
                        == Some(&serde_json::Value::Bool(true)) =>
            {
                seed.deserialize(SchemaAwareDeserializer::new(
                    self.reader,
                    &record.fields[0].schema,
                    self.config,
                )?)
            }
            _ => seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                self.schema,
                self.config,
            )?),
        }
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
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
        V: Visitor<'de>,
    {
        SchemaAwareDeserializer::new(self.reader, self.schema, self.config)?.deserialize_struct(
            DESERIALIZE_ANY,
            fields,
            visitor,
        )
    }
}
