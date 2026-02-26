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

use std::io::Read;

use serde::de::{EnumAccess, VariantAccess};

use crate::{Error, schema::UnionSchema};

use super::Config;

pub struct UnionDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s UnionSchema,
    config: Config<'s>,
}

impl<'s, 'r, R: Read> UnionDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s UnionSchema, config: Config<'s>) -> Self {
        Self {
            reader,
            schema,
            config,
        }
    }
}

impl<'s, 'r, 'de, R: Read> EnumAccess<'de> for UnionDeserializer<'s, 'r, R> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de> {
            seed.deserialize(deserializer)
    }
}

impl<'s, 'r, 'de, R: Read> VariantAccess<'de> for UnionDeserializer<'s, 'r, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        todo!()
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de> {
        todo!()
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        todo!()
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        todo!()
    }
}
