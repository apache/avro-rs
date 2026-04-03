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

use crate::{
    Error, Schema,
    schema::RecordSchema,
    serde::deser_schema::{Config, SchemaAwareDeserializer},
};

pub struct OneTupleDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s, S>,
    field_read: bool,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> OneTupleDeserializer<'s, 'r, R, S> {
    pub fn new(
        reader: &'r mut R,
        schema: &'s Schema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema {
            config.get_schema(name)?
        } else {
            schema
        };
        Ok(Self {
            reader,
            schema,
            config,
            field_read: false,
        })
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> SeqAccess<'de>
    for OneTupleDeserializer<'s, 'r, R, S>
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.field_read {
            Ok(None)
        } else {
            let val = seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                self.schema,
                self.config,
            )?)?;
            self.field_read = true;
            Ok(Some(val))
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(1 - usize::from(self.field_read))
    }
}

pub struct ManyTupleDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s RecordSchema,
    config: Config<'s, S>,
    current_field: usize,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> ManyTupleDeserializer<'s, 'r, R, S> {
    pub fn new(reader: &'r mut R, schema: &'s RecordSchema, config: Config<'s, S>) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: 0,
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> SeqAccess<'de>
    for ManyTupleDeserializer<'s, 'r, R, S>
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.current_field < self.schema.fields.len() {
            let schema = &self.schema.fields[self.current_field].schema;
            let val = seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema,
                self.config,
            )?)?;
            self.current_field += 1;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.schema.fields.len() - self.current_field)
    }
}
