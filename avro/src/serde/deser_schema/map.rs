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

use serde::de::MapAccess;

use super::Config;
use crate::serde::deser_schema::union::UnionDeserializer;
use crate::{
    Error, Schema, schema::MapSchema, serde::deser_schema::SchemaAwareDeserializer, util::zag_i32,
};

pub struct MapDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s MapSchema,
    config: Config<'s>,
    state: State,
}

impl<'s, 'r, R: Read> MapDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s MapSchema, config: Config<'s>) -> Self {
        Self {
            reader,
            schema,
            config,
            state: State::EndOfBlock,
        }
    }
}

enum State {
    EndOfBlock,
    ReadingKey(u32),
    ReadingValue(u32),
    Finished,
}

impl<'de, 's, 'r, R: Read> MapAccess<'de> for MapDeserializer<'s, 'r, R> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        match self.state {
            State::EndOfBlock => {
                let remaining = zag_i32(self.reader)?;
                if remaining < 0 {
                    let _bytes = zag_i32(self.reader)?;
                }
                if remaining == 0 {
                    self.state = State::Finished
                } else {
                    self.state = State::ReadingKey(remaining.abs() as u32)
                }
                self.next_key_seed(seed)
            }
            State::ReadingKey(remaining) => {
                let v = seed.deserialize(SchemaAwareDeserializer::new(
                    self.reader,
                    &Schema::String,
                    self.config,
                )?)?;

                self.state = State::ReadingValue(remaining);

                Ok(Some(v))
            }
            State::Finished => Ok(None),
            State::ReadingValue(_) => {
                panic!("`next_key_seed` and `next_value_seed` where called in the wrong error")
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let State::ReadingValue(mut remaining) = self.state else {
            panic!("`next_key_seed` and `next_value_seed` where called in the wrong error")
        };
        let v = match self.schema.types.as_ref() {
            Schema::Union(union) => {
                seed.deserialize(UnionDeserializer::new(self.reader, union, self.config)?)?
            }
            schema => seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema,
                self.config,
            )?)?,
        };

        remaining -= 1;
        if remaining == 0 {
            self.state = State::EndOfBlock;
        } else {
            self.state = State::ReadingKey(remaining);
        }

        Ok(v)
    }
}
