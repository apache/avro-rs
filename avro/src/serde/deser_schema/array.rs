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

use serde::de::{DeserializeSeed, SeqAccess};

use super::{Config, SchemaAwareDeserializer};
use crate::{
    Error, Schema,
    schema::ArraySchema,
    util::{zag_i32, zag_i64},
};

/// Deserialize sequences from an Avro array.
pub struct ArrayDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s, S>,
    /// Track where we are in reading the array
    remaining: Option<u32>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> ArrayDeserializer<'s, 'r, R, S> {
    pub fn new(
        reader: &'r mut R,
        schema: &'s ArraySchema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema.items.as_ref() {
            config.get_schema(name)?
        } else {
            &schema.items
        };
        let remaining = Self::read_block_header(reader)?;
        Ok(Self {
            reader,
            schema,
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

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> SeqAccess<'de> for ArrayDeserializer<'s, 'r, R, S> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if let Some(mut remaining) = self.remaining {
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
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        self.remaining
            .map(|x| usize::try_from(x).unwrap_or(usize::MAX))
    }
}
