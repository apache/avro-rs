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

use serde::de::SeqAccess;

use super::Config;
use crate::serde::deser_schema::union::UnionDeserializer;
use crate::{
    Error, Schema, schema::ArraySchema, serde::deser_schema::SchemaAwareDeserializer, util::zag_i32,
};

pub struct ArrayDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s ArraySchema,
    config: Config<'s>,
    state: State,
}

impl<'s, 'r, R: Read> ArrayDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s ArraySchema, config: Config<'s>) -> Self {
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
    ReadingValue(u32),
    Finished,
}

impl<'de, 's, 'r, R: Read> SeqAccess<'de> for ArrayDeserializer<'s, 'r, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
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
                    self.state = State::ReadingValue(remaining.abs() as u32)
                }
                self.next_element_seed(seed)
            }
            State::ReadingValue(remaining) => {
                let v = match self.schema.items.as_ref() {
                    Schema::Union(union) => {
                        seed.deserialize(UnionDeserializer::new(self.reader, union, self.config)?)?
                    }
                    schema => seed.deserialize(SchemaAwareDeserializer::new(
                        self.reader,
                        schema,
                        self.config,
                    )?)?,
                };

                self.state = State::ReadingValue(remaining);

                Ok(Some(v))
            }
            State::Finished => Ok(None),
        }
    }
}
