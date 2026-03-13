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

use std::{borrow::Borrow, io::Read};

use serde::de::{DeserializeSeed, MapAccess};

use crate::{
    Error, Schema,
    schema::MapSchema,
    serde::deser_schema::{Config, SchemaAwareDeserializer},
    util::{zag_i32, zag_i64},
};

pub struct MapDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s, S>,
    remaining: Option<u32>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> MapDeserializer<'s, 'r, R, S> {
    pub fn new(
        reader: &'r mut R,
        schema: &'s MapSchema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema.types.as_ref() {
            config.get_schema(name)?
        } else {
            &schema.types
        };
        let remaining = Self::read_block_header(reader)?;
        Ok(Self {
            schema,
            reader,
            config,
            remaining,
        })
    }

    fn read_block_header(reader: &mut R) -> Result<Option<u32>, Error> {
        let remaining = zag_i32(reader)?;
        if remaining < 0 {
            // If the block size is negative the next number is the size of the block in bytes
            let _bytes = zag_i64(reader)?;
        }
        if remaining == 0 {
            // If the block size is zero the array is finished
            Ok(None)
        } else {
            Ok(Some(remaining.unsigned_abs()))
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> MapAccess<'de> for MapDeserializer<'s, 'r, R, S> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.remaining.is_some() {
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                &Schema::String,
                self.config,
            )?)
            .map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let mut remaining = self
            .remaining
            .expect("next_key returned None, next_value should not have been called");
        let value = seed.deserialize(SchemaAwareDeserializer::new(
            self.reader,
            self.schema,
            self.config,
        )?)?;

        remaining -= 1;
        if remaining == 0 {
            self.remaining = Self::read_block_header(self.reader)?;
        } else {
            self.remaining = Some(remaining);
        }
        Ok(value)
    }

    fn next_entry_seed<K, V>(
        &mut self,
        kseed: K,
        vseed: V,
    ) -> Result<Option<(K::Value, V::Value)>, Self::Error>
    where
        K: DeserializeSeed<'de>,
        V: DeserializeSeed<'de>,
    {
        if let Some(mut remaining) = self.remaining {
            let key = kseed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                &Schema::String,
                self.config,
            )?)?;
            let value = vseed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                self.schema,
                self.config,
            )?)?;

            remaining -= 1;
            if remaining == 0 {
                self.remaining = Self::read_block_header(self.reader)?;
            } else {
                self.remaining = Some(remaining);
            }

            Ok(Some((key, value)))
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        self.remaining
            .map(|x| usize::try_from(x).unwrap_or(usize::MAX))
    }
}
